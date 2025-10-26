from flask import Flask, jsonify, send_from_directory, render_template, request, session
import happybase
import os
import json
import time
import traceback
import xgboost as xgb
import numpy as np
import joblib
import google.generativeai as genai
import pandas as pd
from datetime import datetime, timedelta
from newsapi import NewsApiClient
import requests
import trafilatura
import uuid

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "your-secret-key-change-this")

# Global chat history storage
# Structure: {session_id: [{"timestamp": datetime, "question": str, "answer": str}, ...]}
chat_history = {}

try:
    genai.configure(api_key=os.getenv("GOOGLE_AI_API_KEY"))
    model = genai.GenerativeModel("gemini-2.5-flash-lite")
except Exception as e:
    print(f"CẢNH BÁO: Không thể cấu hình Gemini. Lỗi: {e}")
    model = None

try:
    newsapi = NewsApiClient(api_key=os.getenv("NEWS_API_KEY"))
except Exception as e:
    print(f"CẢNH BÁO: Không thể cấu hình NewsAPI. Lỗi: {e}")
    newsapi = None

CONTEXT_DOCUMENT_ID = "latest_market_context"


def get_or_create_session_id():
    """Get or create a session ID for the current user"""
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
    return session['session_id']


def add_to_chat_history(session_id, question, answer):
    """Add a question-answer pair to the chat history"""
    if session_id not in chat_history:
        chat_history[session_id] = []
    
    chat_history[session_id].append({
        "timestamp": datetime.now().isoformat(),
        "question": question,
        "answer": answer
    })
    
    # Keep only the last 10 conversations to prevent memory bloat
    if len(chat_history[session_id]) > 10:
        chat_history[session_id] = chat_history[session_id][-10:]


def get_chat_history_summary(session_id):
    """Get a formatted summary of recent chat history"""
    if session_id not in chat_history or not chat_history[session_id]:
        return "No previous conversation history."
    
    history = chat_history[session_id]
    summary_parts = []
    
    for i, entry in enumerate(history[-5:], 1):  # Last 5 conversations
        summary_parts.append(f"Q{i}: {entry['question']}")
        summary_parts.append(f"A{i}: {entry['answer'][:200]}...")  # Truncate long answers
    
    return "\n".join(summary_parts)


def is_greeting(question):
    """Check if the question is a simple greeting"""
    greeting_words = [
        "hello", "hi", "hey", "good morning", "good afternoon", "good evening",
        "greetings", "howdy", "what's up", "how are you", "how do you do"
    ]
    
    question_lower = question.lower().strip()
    
    # Check for exact matches or questions that are just greetings
    for greeting in greeting_words:
        if question_lower == greeting or question_lower.startswith(greeting + " "):
            return True
    
    # Check for greeting + "who are you" patterns
    if any(greeting in question_lower for greeting in greeting_words) and "who are you" in question_lower:
        return True
        
    return False


def is_crypto_related(question, chat_history_summary=""):
    """Check if the question is related to cryptocurrency or trading"""
    crypto_keywords = [
        # Cryptocurrency names
        "bitcoin", "btc", "ethereum", "eth", "binance", "bnb", "solana", "sol",
        "ripple", "xrp", "cardano", "ada", "dogecoin", "doge", "chainlink", "link",
        "polkadot", "dot", "litecoin", "ltc", "usdt", "usdc", "tether",
        
        # Trading terms
        "crypto", "cryptocurrency", "trading", "buy", "sell", "price", "market",
        "bullish", "bearish", "pump", "dump", "hodl", "moon", "lambo",
        "altcoin", "defi", "nft", "blockchain", "mining", "wallet", "exchange",
        
        # Financial terms in crypto context
        "portfolio", "profit", "loss", "gains", "returns",
        "volatility", "liquidity", "market cap", "volume", "resistance", "support",
        
        # Technical analysis
        "rsi", "macd", "bollinger", "fibonacci", "candlestick", "chart",
        "technical analysis", "fundamental analysis", "trend", "breakout"
    ]
    
    question_lower = question.lower()
    
    # Check if any crypto keyword is mentioned in the current question
    for keyword in crypto_keywords:
        if keyword in question_lower:
            return True
    
    # Check for currency symbols and common crypto patterns
    crypto_patterns = [
        r'\$[a-z]{3,6}',  # $BTC, $ETH, etc.
        r'[a-z]{3,6}usdt',  # BTCUSDT, ETHUSDT, etc.
        r'[a-z]{3,6}/usdt',  # BTC/USDT, ETH/USDT, etc.
    ]
    
    import re
    for pattern in crypto_patterns:
        if re.search(pattern, question_lower):
            return True
    
    # Check if the question is a follow-up to a crypto conversation
    if chat_history_summary and chat_history_summary != "No previous conversation history.":
        # Look for crypto keywords in recent conversation history
        history_lower = chat_history_summary.lower()
        for keyword in crypto_keywords:
            if keyword in history_lower:
                # If the current question contains investment-related terms and there's crypto context
                investment_terms = ["worth", "invest", "investing", "buy", "sell", "should i", "recommend", "advice", "good", "bad", "safe", "risky"]
                if any(term in question_lower for term in investment_terms):
                    return True
    
    return False


def is_irrelevant_question(question):
    """Check if the question is completely irrelevant to crypto/trading"""
    irrelevant_topics = [
        # Math and science
        "calculate", "solve", "equation", "formula", "mathematics", "math",
        "physics", "chemistry", "biology", "science", "experiment", "problem",
        "2+2", "addition", "subtraction", "multiplication", "division",
        
        # Programming
        "code", "programming", "python", "javascript", "java", "c++", "html", "css",
        "algorithm", "function", "variable", "debug", "compile", "syntax",
        "write code", "program", "software", "development",
        
        # Literature and arts
        "poem", "poetry", "novel", "book", "author", "writer", "literature",
        "art", "painting", "music", "song", "movie", "film", "actor",
        "shakespeare", "shakespeare", "poetry", "novel", "story",
        
        # General knowledge
        "history", "geography", "politics", "sports", "cooking", "recipe",
        "travel", "weather", "health", "medicine", "psychology",
        "pasta", "food", "restaurant", "hotel", "vacation",
        
        # Personal questions
        "your name", "your age", "your favorite", "your opinion on",
        "what do you think about", "do you like", "are you",
        "how are you", "what's your", "tell me about yourself"
    ]
    
    question_lower = question.lower()
    
    # Check if question contains irrelevant topics
    has_irrelevant = any(topic in question_lower for topic in irrelevant_topics)
    
    # If it has irrelevant topics, it's irrelevant (regardless of crypto context)
    # This prevents the AI from trying to relate everything back to crypto
    return has_irrelevant


def extract_crypto_from_context(chat_history_summary):
    """Extract cryptocurrency information from conversation context"""
    if not chat_history_summary or chat_history_summary == "No previous conversation history.":
        return None, None
    
    all_symbols = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt", "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"]
    
    coin_name_map = {
        "btc": "bitcoin", "eth": "ethereum", "bnb": "binance coin",
        "sol": "solana", "xrp": "ripple", "ada": "cardano",
        "doge": "dogecoin", "link": "chainlink", "dot": "polkadot",
        "ltc": "litecoin"
    }
    
    history_lower = chat_history_summary.lower()
    
    # Look for cryptocurrency mentions in the conversation history
    for s_with_usdt in all_symbols:
        s_without_usdt = s_with_usdt.replace("usdt", "")
        
        # Check for symbol mentions (BTC, ETH, DOGE, etc.)
        if s_without_usdt.upper() in history_lower.upper():
            return s_with_usdt, coin_name_map.get(s_without_usdt, s_without_usdt)
        
        # Check for full name mentions (bitcoin, ethereum, dogecoin, etc.)
        coin_name = coin_name_map.get(s_without_usdt)
        if coin_name and coin_name in history_lower:
            return s_with_usdt, coin_name
    
    return None, None


def ensure_table_exists(connection, table_name):
    tables = [t.decode() for t in connection.tables()]
    if table_name not in tables:
        print(f"[Flask] Creating table '{table_name}'...")
        connection.create_table(
            table_name,
            {'data': dict(), 'meta': dict()}
        )
        time.sleep(3)

def get_historical_data_from_hbase(symbol, limit=200):
    try:
        connection = happybase.Connection(host=os.getenv("HBASE_THRIFT_HOST", "hbase"), port=int(os.getenv("HBASE_THRIFT_PORT", "9090")))
        connection.open()
        table = connection.table("crypto_prices")
        row_prefix = f"{symbol.lower()}_".encode("utf-8")
        
        rows = table.scan(row_prefix=row_prefix, limit=limit)
        
        data = []
        for key, raw_data in rows:
            record = {'timestamp': int(key.decode().split('_')[1])}
            for k, v in raw_data.items():
                col_name = k.decode().split(':')[-1]
                if col_name == 'price':
                    record['close'] = float(v.decode())
                if col_name == 'high_price':
                    record['high'] = float(v.decode())
                if col_name == 'low_price':
                    record['low'] = float(v.decode())
            if 'close' in record:
                data.append(record)
        
        connection.close()
        
        if not data:
            return None
        
        data.sort(key=lambda x: x['timestamp'])
        return pd.DataFrame(data)

    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu HBase: {e}")
        return None

def get_and_scrape_news(query="crypto OR bitcoin OR ethereum", days=1, articles_to_fetch=5):
    if not newsapi:
        return {
            "full_text": "Dịch vụ tin tức chưa được cấu hình.",
            "sources_markdown": ""
        }

    try:
        from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        response = newsapi.get_everything(
            q=query, from_param=from_date, sort_by="relevancy", language="en", page_size=articles_to_fetch
        )
        
        articles = response.get("articles", [])
        if not articles:
            print(f"[SCRAPER] Không tìm thấy bài báo nào cho query: {query}")
            return { "full_text": "Không tìm thấy bài báo liên quan.", "sources_markdown": "" }

    except Exception as e:
        print(f"[SCRAPER] Lỗi khi gọi NewsAPI: {e}")
        return { "full_text": f"Lỗi khi lấy tin tức: {e}", "sources_markdown": "" }

    full_content_list = []
    sources_markdown_list = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for article in articles:
        title = article.get('title', 'No Title')
        url = article.get('url')
        published_at_str = article.get('publishedAt')
        
        # Chuyển đổi và định dạng ngày tháng
        published_date = ""
        if published_at_str:
            try:
                # Chuyển chuỗi ISO 8601 "2023-10-27T10:00:00Z" thành đối tượng datetime
                dt_object = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
                # Định dạng lại thành "YYYY-MM-DD"
                published_date = dt_object.strftime("%Y-%m-%d")
            except ValueError:
                # Nếu định dạng ngày tháng không đúng, bỏ qua
                published_date = ""
        if not url:
            continue

        try:
            downloaded = requests.get(url, headers=headers, timeout=15)
            downloaded.raise_for_status()

            content = trafilatura.extract(downloaded.text, favor_precision=True, include_comments=False)

            if content:
                full_content_list.append(f"--- ARTICLE START ---\nTITLE: {title}\nURL: {url}\nDATE: {published_date}\nCONTENT:\n{content}\n--- ARTICLE END ---")
                
                sources_markdown_list.append(f"- [{title}]({url}) ({published_date})")
            else:
                 print(f"[SCRAPER] Bỏ qua (không trích xuất được nội dung): {url}")

        except Exception as e:
            print(f"[SCRAPER] Lỗi khi crawl trang {url}: {e}")
            continue

    if not full_content_list:
        return { "full_text": "Không crawl được nội dung từ các bài báo đã tìm thấy.", "sources_markdown": "" }

    return {
        "full_text": "\n\n".join(full_content_list),
        "sources_markdown": "\n".join(sources_markdown_list)
    }

def calculate_rsi(df, period=14):

    delta = df['close'].diff()

    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()

    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)

    rsi = 100 - (100 / (1 + rs))
    
    return rsi


@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/crypto/<string:symbol>", methods=["GET"])
def get_crypto(symbol):
    try:
        connection = happybase.Connection(
            host=os.getenv("HBASE_THRIFT_HOST", "hbase"),
            port=int(os.getenv("HBASE_THRIFT_PORT", "9090"))
        )
        connection.open()
        ensure_table_exists(connection, "crypto_prices")

        table = connection.table("crypto_prices")
        row_prefix = f"{symbol.strip().lower()}_".encode("utf-8")
        rows = table.scan(row_prefix=row_prefix)

        result = []
        for key, data in rows:
            record = {k.decode().replace(':', '_'): v.decode() for k, v in data.items()}
            record["row_key"] = key.decode()
            result.append(record)

        connection.close()
        return jsonify(result) if result else jsonify({"message": f"No data for {symbol}"})

    except Exception as e:
        print("[ERROR]", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)})

@app.route("/api/crypto/predictions/<string:symbol>", methods=["GET"])
def predict_realtime(symbol):
    try:
        connection = happybase.Connection(
            host=os.getenv("HBASE_THRIFT_HOST", "hbase"),
            port=int(os.getenv("HBASE_THRIFT_PORT", "9090"))
        )
        connection.open()
        table = connection.table("crypto_prices")

        # Prefix: ví dụ "btcusdt_"
        row_prefix = f"{symbol.lower()}_".encode("utf-8")

        # Quét dữ liệu mới nhất (ví dụ 200 dòng)
        rows = table.scan(row_prefix=row_prefix, limit=200)

        data = []
        for key, raw in rows:
            try:
                parsed = {
                    k.decode().split(":")[-1]: float(v.decode())
                    for k, v in raw.items()
                    if v.decode().replace('.', '', 1).isdigit()
                }
                data.append(parsed)
            except Exception:
                continue

        connection.close()

        if not data:
            return jsonify({"error": "No recent data found in HBase"}), 400

        # Sắp xếp tăng dần theo timestamp
        data = sorted(data, key=lambda x: x.get("timestamp", 0))

        latest_row = data[-1]
        required_keys = ["open_price", "high_price", "low_price", "volume_token"]

        if not all(k in latest_row for k in required_keys):
            return jsonify({"error": "Missing necessary features in latest record"}), 400

        X_input = np.array([[
            latest_row["open_price"],
            latest_row["high_price"],
            latest_row["low_price"],
            latest_row["volume_token"]
        ]])

        model_path = f"models/xgboost_{symbol.lower()}.pkl"

        # Nếu model chưa có local, có thể lấy từ HDFS (tuỳ cấu hình)
        if not os.path.exists(model_path):
            hdfs_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol.lower()}.pkl"
            os.system(f"hadoop fs -get -f {hdfs_path} {model_path}")

        if not os.path.exists(model_path):
            return jsonify({"error": f"Model not found for {symbol}"}), 404

        model = joblib.load(model_path)

        predicted_price = float(model.predict(X_input)[0])
        print(f"[{symbol.upper()}] Predicted = {predicted_price:.4f}", flush=True)

        return jsonify({
            "symbol": symbol.upper(),
            "predicted_price": round(predicted_price, 4),
            "features_used": {
                "open": latest_row["open_price"],
                "high": latest_row["high_price"],
                "low": latest_row["low_price"],
                "volume": latest_row["volume_token"]
            }
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/api/chatbot/ask", methods=["POST"])
def ask_chatbot():
    if not model:
        return jsonify({"error": "Chatbot is not configured."}), 500

    user_question = request.json.get("question")
    if not user_question:
        return jsonify({"error": "Question is required."}), 400
    
    # Get or create session ID for this user
    session_id = get_or_create_session_id()
    
    # Get chat history for context
    chat_history_summary = get_chat_history_summary(session_id)
    
    # 1. CHECK FOR SIMPLE GREETINGS
    if is_greeting(user_question):
        if "who are you" in user_question.lower():
            answer = """Hello! I'm a specialized cryptocurrency market analyst AI. I can help you with:

• **Crypto Market Analysis** - Current prices, trends, and market conditions
• **Technical Analysis** - RSI, moving averages, support/resistance levels
• **News Analysis** - Latest crypto news and market sentiment
• **Trading Insights** - Buy/sell recommendations based on data
• **Portfolio Guidance** - Investment strategies and risk assessment

What would you like to know about the crypto market today?"""
        else:
            answer = "Hi! I'm your crypto market analyst. What can I help you with today? I can analyze market trends, provide technical insights, or discuss the latest crypto news."
        
        # Store the conversation
        add_to_chat_history(session_id, user_question, answer)
        return jsonify({"answer": answer})
    
    # 2. CHECK FOR IRRELEVANT QUESTIONS
    if is_irrelevant_question(user_question):
        answer = "I'm a specialized cryptocurrency market analyst AI. I can only help with crypto-related questions, market analysis, and trading insights. Please ask me about cryptocurrency markets, trading, or blockchain topics."
        
        # Store the conversation
        add_to_chat_history(session_id, user_question, answer)
        return jsonify({"answer": answer})
    
    # 3. CHECK IF QUESTION IS CRYPTO-RELATED (with context)
    if not is_crypto_related(user_question, chat_history_summary):
        answer = "I'm a specialized cryptocurrency market analyst AI. Your question doesn't seem to be related to cryptocurrency, trading, or blockchain topics. I can help you with crypto market analysis, technical indicators, trading strategies, or the latest crypto news. What would you like to know about the crypto market?"
        
        # Store the conversation
        add_to_chat_history(session_id, user_question, answer)
        return jsonify({"answer": answer})

    all_symbols = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt", "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"]
    
    # 4. PHÂN TÍCH CÂU HỎI ĐỂ TÌM COIN MỤC TIÊU (CHỈ KHI LÀ CRYPTO-RELATED)
    target_symbol = None
    target_coin_name = None
    
    coin_name_map = {
        "btc": "bitcoin", "eth": "ethereum", "bnb": "binance coin",
        "sol": "solana", "xrp": "ripple", "ada": "cardano",
        "doge": "dogecoin", "link": "chainlink", "dot": "polkadot",
        "ltc": "litecoin"
    }

    # First, try to find crypto mentioned in the current question
    for s_with_usdt in all_symbols:
        s_without_usdt = s_with_usdt.replace("usdt", "")
        if s_without_usdt.upper() in user_question.upper() or \
           (coin_name_map.get(s_without_usdt) and coin_name_map.get(s_without_usdt).upper() in user_question.upper()):
            target_symbol = s_with_usdt
            target_coin_name = coin_name_map.get(s_without_usdt, s_without_usdt)
            break
    
    # If no crypto found in current question, try to extract from conversation context
    if not target_symbol:
        context_symbol, context_coin_name = extract_crypto_from_context(chat_history_summary)
        if context_symbol:
            target_symbol = context_symbol
            target_coin_name = context_coin_name
            print(f"[CHATBOT] Using crypto from context: {target_symbol} ({target_coin_name})")

    if target_coin_name:
        news_query = f'"{target_coin_name}" OR "{target_symbol.replace("usdt", "").upper()}"'
        print(f"[CHATBOT] Crawling news with specific query: {news_query}")
        news_data = get_and_scrape_news(query=news_query, articles_to_fetch=3) # Crawl 3 bài liên quan nhất
    else:
        print("[CHATBOT] Crawling news with generic query.")
        news_data = get_and_scrape_news(articles_to_fetch=3)

    full_article_content = news_data.get("full_text")
    sources_list_markdown = news_data.get("sources_markdown")

    has_relevant_news = bool(sources_list_markdown) 

    # XÂY DỰNG BẢN TÓM TẮT THỊ TRƯỜNG TỔNG QUAN TỪ HBASE
    market_snapshot_data = []
    try:
        connection = happybase.Connection(host=os.getenv("HBASE_THRIFT_HOST", "hbase"), port=int(os.getenv("HBASE_THRIFT_PORT", "9090")))
        connection.open()
        table = connection.table("crypto_prices")
        
        for symbol in all_symbols:
            row_prefix = f"{symbol.lower()}_".encode("utf-8")
            latest_row = next(table.scan(row_prefix=row_prefix, reversed=True, limit=1), None)
            if latest_row:
                key, data = latest_row
                price = float(data.get(b'data:price', b'0'))
                change_pct = float(data.get(b'data:price_change_pct', b'0'))
                market_snapshot_data.append(f"- **{symbol.upper()}**: Price=${price:.4f}, 24h Change={change_pct:.2f}%")

        connection.close()
        formatted_data_summary = "\n".join(market_snapshot_data)
        if not formatted_data_summary:
            formatted_data_summary = "Could not retrieve market snapshot from HBase."
            
    except Exception as e:
        formatted_data_summary = f"Error retrieving market snapshot: {e}"


    # Phân tích câu hỏi của người dùng để tìm symbol mục tiêu
    target_symbol = None
    for s_with_usdt in all_symbols:
        s_without_usdt = s_with_usdt.replace("usdt", "")
        if s_without_usdt.upper() in user_question.upper():
            target_symbol = s_with_usdt
            break
            
    # Nếu có symbol, thực hiện phân tích kỹ thuật
    technical_analysis_summary = "No specific coin mentioned for technical analysis."
    if target_symbol:
        df_history = get_historical_data_from_hbase(target_symbol, limit=50)
        if df_history is not None and not df_history.empty and len(df_history) > 20:
            df_history['RSI_14'] = calculate_rsi(df_history, period=14)
            df_history['SMA_20'] = df_history['close'].rolling(window=20).mean()
            
            latest_indicators = df_history.iloc[-1]
            if not pd.isna(latest_indicators.get('RSI_14')) and not pd.isna(latest_indicators.get('SMA_20')):
                current_price = latest_indicators['close']
                rsi_14 = latest_indicators.get('RSI_14')
                sma_20 = latest_indicators.get('SMA_20')
                
                technical_analysis_summary = f"""
**Technical Analysis for {target_symbol.upper()}:**
- **Current Price:** ${current_price:.4f}
- **RSI (14 periods):** {rsi_14:.2f} (A value > 70 suggests overbought, < 30 suggests oversold).
- **Simple Moving Average (20 periods):** ${sma_20:.4f} (If price is above SMA, it's a bullish signal; if below, it's bearish).
- **Recent High (Resistance):** ${df_history['high'].max():.4f}
- **Recent Low (Support):** ${df_history['low'].min():.4f}
"""

    # Tạo Prompt cho Gemini
    if has_relevant_news:
        # Nếu CÓ tin-tức, dùng prompt đầy-đủ
        print("[CHATBOT] Found relevant news. Using full prompt.")
        prompt = f"""
        You are an expert crypto market analyst. Your analysis must be technical and data-driven.
        Based on the following news, overall market snapshot, and specific technical indicators (if available), answer the user's question.
        Present the data and analysis objectively and finally may give direct financial advice (e.g., "you should buy/sell") if justified by the data.
        When you use information from a news article, you MUST note the article at the end of your response.
        Answer in the same language as the user's question.

    ### INSTRUCTIONS ###
        1.  **Context Awareness:** You have access to the previous conversation history. Use this context to provide more relevant and personalized responses.
        2.  **Analyze Full Content:** Read through all the full article content provided below. Pay attention to the publication DATE of each article to understand the timeline of events.
        3.  **Synthesize and Summarize:** Based on this content, synthesize the key market-moving information, trends, and sentiments.
        4.  **Combine with Data:** Integrate your findings from the news with the provided "Overall Market Snapshot" and "Specific Technical Analysis".
        5.  **Answer the Question:** Formulate a comprehensive answer to the "User's Question".
        6.  **Cite Sources:** When you use information from an article, you MUST cite it using a number like `[1]`. At the end of your entire response, create a `### Sources` section and list all the sources corresponding to the numbers, using the provided "Available News Sources". The sources already include the publication date.
        **Example of the final output format:**
        Here is some analysis about the market [1]. Further data suggests another trend [2]. The first point is confirmed by this article again [1].

        ### Sources
        1. [Publication Date][Full Title of Article 1](http://...)
        2. [Publication Date][Full Title of Article 2](http://...)
        ---
        
        --- Previous Conversation History ---
        {chat_history_summary}
        --- End of Chat History ---
        
        --- Full News Content (for your analysis) ---
        {full_article_content}

        **Overall Market Snapshot:**
        {formatted_data_summary}
        --- End of General Context ---

        --- Specific Technical Analysis ---
        {technical_analysis_summary}
        --- End of Specific Analysis ---

        --- Available News Sources (for citation) ---
        {sources_list_markdown}
        --- End of Sources ---

        **User's Question:** {user_question}

        **Your Expert Answer:**
        """
    else:
        # Nếu KHÔNG có tin-tức, dùng prompt rút-gọn
        print("[CHATBOT] No relevant news found. Using technical-only prompt.")
        prompt = f"""
        You are an expert crypto market analyst. Your analysis must be technical and data-driven.
        Based on the overall market snapshot, and specific technical indicators (if available), answer the user's question.
        Present the data and analysis objectively and finally may give direct financial advice (e.g., "you should buy/sell") if justified by the data.
        Answer in the same language as the user's question.

        ### INSTRUCTIONS ###
        1.  **Context Awareness:** You have access to the previous conversation history. Use this context to provide more relevant and personalized responses.
        2.  Analyze the "Overall Market Snapshot" and "Specific Technical Analysis" provided below.
        3.  Formulate a comprehensive answer to the "User's Question" based ONLY on this data.
        4.  **Crucially, start your response by clearly stating that no specific recent news was found for the query, so the analysis is purely based on technical data.**

        --- Previous Conversation History ---
        {chat_history_summary}
        --- End of Chat History ---
        
        ---
        ### Overall Market Snapshot
        {formatted_data_summary}
        ---
        ### Specific Technical Analysis
        {technical_analysis_summary}
        ---

        **User's Question:** {user_question}
        **Your Expert Answer:**
        """
    # ==============================================
    # 6. Gọi Gemini API và trả lời
    try:
        response = model.generate_content(prompt)
        answer = response.text
        
        # Store the conversation in chat history
        add_to_chat_history(session_id, user_question, answer)
        
        return jsonify({"answer": answer})
    except Exception as e:
        print(f"Lỗi khi gọi Gemini API: {e}")
        return jsonify({"error": f"Error generating response from AI: {e}"}), 500


@app.route("/api/chatbot/history", methods=["GET"])
def get_chat_history():
    """Get the chat history for the current session"""
    session_id = get_or_create_session_id()
    
    if session_id not in chat_history:
        return jsonify({"history": []})
    
    return jsonify({"history": chat_history[session_id]})


@app.route("/api/chatbot/clear", methods=["POST"])
def clear_chat_history():
    """Clear the chat history for the current session"""
    session_id = get_or_create_session_id()
    
    if session_id in chat_history:
        chat_history[session_id] = []
    
    return jsonify({"message": "Chat history cleared successfully"})


@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory("static", filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)








