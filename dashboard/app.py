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


def add_to_chat_history(session_id, question, answer, mood="neutral"):
    """Add a question-answer pair and mood to the chat history"""
    if session_id not in chat_history:
        chat_history[session_id] = []
    
    chat_history[session_id].append({
        "timestamp": datetime.now().isoformat(),
        "question": question,
        "answer": answer,
        "mood": mood
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

def get_intent_from_prompt(user_question, chat_summary):
    if not model:
        # Fallback for when AI is not configured
        return {
            "intent": "unknown",
            "crypto_topic": "General",
            "mood": "neutral",
            "language": "English",
            "crypto_question_parts": user_question,
            "is_calculation_or_logic": False
        }

    prompt = f"""
        You are an AI assistant that analyzes user queries for a cryptocurrency chatbot. Your task is to deeply understand the user's intent, identify key topics, detect their mood, and determine the primary language.

        Analyze the following information and return a single, minified JSON object with the following keys:
        - "intent": (string) Classify the user's primary intent. Possible values: "greeting", "crypto_question", "general_question", "contextual_question", "irrelevant_question".
        - "crypto_topic": (string) If the intent is "crypto_question", identify the specific crypto (e.g., "Bitcoin", "Dogecoin"). If general, use "General". If not a crypto question, use "None".
        - "mood": (string) Analyze the user's likely mood (e.g., "curious", "happy", "frustrated", "neutral").
        - "language": (string) Identify the dominant language of the "User's Question". You must support ALL languages. If multiple languages are present, choose the one with the most words. (e.g., "Vietnamese", "English", "Spanish", "Japanese").
        - "crypto_question_parts": (string) Extract ONLY the parts of the user's question that are related to cryptocurrency. If the whole question is about crypto, return the whole question.
        - "is_calculation_or_logic": (boolean) Set to true if the crypto-related part is a math/logic problem (e.g., "If I buy BTC at 30k and it goes to 36k, what is my profit?", "Is a 20% portfolio gain reasonable if BTC rose 10%?").

        ### Rules:
        - For a mixed question like "What is BTC price and what is the capital of France?", the "intent" should be "crypto_question", and "crypto_question_parts" should ONLY be "What is BTC price?".
        - If the question is purely non-crypto, the "intent" is "irrelevant_question" and "crypto_question_parts" should be empty.
        - The "is_calculation_or_logic" flag is crucial for routing the question correctly.

        ---
        ### Previous Conversation History
        {chat_summary}
        ---
        ### User's Question
        {user_question}
        ---

        JSON response:
    """
    try:
        response = model.generate_content(prompt)
        # Clean up the response to ensure it's valid JSON
        text_response = response.text
        start_index = text_response.find('{')
        end_index = text_response.rfind('}') + 1
        if start_index != -1 and end_index != -1:
            json_str = text_response[start_index:end_index]
            result = json.loads(json_str)
            return result
        else:
            raise ValueError("No JSON object found in response")
    except Exception as e:
        print(f"Error parsing intent from AI: {e}")
        return {
            "intent": "crypto_question",
            "crypto_topic": "General",
            "mood": "neutral",
            "language": "English",
            "crypto_question_parts": user_question,
            "is_calculation_or_logic": False
        }

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
    
    session_id = get_or_create_session_id()
    chat_history_summary = get_chat_history_summary(session_id)
    
    # 1. NEW: Get intent and entities from the AI model
    intent_data = get_intent_from_prompt(user_question, chat_history_summary)
    intent = intent_data.get("intent")
    crypto_topic = intent_data.get("crypto_topic")
    user_mood = intent_data.get("mood", "neutral")
    user_language = intent_data.get("language", "English")
    crypto_question_part = intent_data.get("crypto_question_parts") or user_question
    is_calculation = intent_data.get("is_calculation_or_logic", False)
    
    print(f"[CHATBOT] Intent: {intent}, Topic: {crypto_topic}, Lang: {user_language}, IsCalc: {is_calculation}")
    print(f"[CHATBOT] Relevant Question Part: '{crypto_question_part}'")

    if intent == "crypto_question" and is_calculation:
        print("[CHATBOT] Handling a crypto calculation/logic question.")
        # PROMPT ĐÃ ĐƯỢC NÂNG CẤP ĐỂ YÊU CẦU GIẢI THÍCH
        calc_prompt = f"""
            You are a helpful and clear-thinking AI assistant with expertise in finance and mathematics.
            The user has a question that requires calculation or logical reasoning related to cryptocurrency.
            Your task is to answer the user's question clearly and provide a simple, step-by-step explanation of how you arrived at the answer.

            ### INSTRUCTIONS ###
            1.  First, state the final answer clearly and directly.
            2.  Then, in a new paragraph, explain the formula or the logic you used in simple terms.
            3.  Finally, show the actual numbers from the user's question being used in the calculation.
            4.  You MUST formulate your entire response in {user_language}.

            --- Conversation History ---
            {chat_history_summary}
            ---
            User's Question: "{user_question}"
            ---
            Your clear answer with a step-by-step explanation in {user_language}:
        """
        try:
            response = model.generate_content(calc_prompt)
            answer = response.text
            add_to_chat_history(session_id, user_question, answer, user_mood)
            return jsonify({"answer": answer})
        except Exception as e:
            print(f"Error calling Gemini for calculation question: {e}")
            answer = "I'm sorry, I encountered an error while processing your calculation."
            add_to_chat_history(session_id, user_question, answer, user_mood)
            return jsonify({"answer": answer})

    # 2. Handle non-crypto intents directly
    if intent in ["greeting", "general_question", "irrelevant_question", "contextual_question"]:
        answer = ""
        # Create a simple prompt for these cases
        print(f"[CHATBOT] Handling simple intent '{intent}' in {user_language}.")
        simple_prompt = f"""
            You are a helpful crypto analyst AI. Your task is to provide a simple, direct response based on the user's intent.
            You MUST formulate your entire response in {user_language}.
            Do not add any extra formatting, sources, or financial advice.

            ### Instructions ###
            Based on the provided intent, generate the appropriate response:
            - If the intent is "greeting": Provide a friendly, brief greeting welcoming the user.
            - If the intent is "irrelevant_question": Politely explain that you are an AI specialized in cryptocurrency and cannot answer topics outside of this domain.
            - If the intent is "general_question" (e.g., "who are you?") or "contextual_question" (e.g., "what did I ask before?"): Answer the user's question concisely based on your role and the conversation history.

            --- Conversation History ---
            {chat_history_summary}
            ---
            Detected Intent: "{intent}"
            User's Question: "{user_question}"
            ---
            Your concise response in {user_language}:
        """
        try:
            response = model.generate_content(simple_prompt)
            answer = response.text
        except Exception as e:
            print(f"Error calling Gemini for simple question: {e}")
            answer = "I'm sorry, I encountered an error. Please try again."

        add_to_chat_history(session_id, user_question, answer, user_mood)
        return jsonify({"answer": answer})


    # 3. For crypto questions, proceed with data gathering
    all_symbols = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt", "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"]
    coin_name_map = {
        "bitcoin": "btcusdt", "ethereum": "ethusdt", "binance coin": "bnbusdt",
        "solana": "solusdt", "ripple": "xrpusdt", "cardano": "adausdt",
        "dogecoin": "dogeusdt", "chainlink": "linkusdt", "polkadot": "dotusdt",
        "litecoin": "ltcusdt"
    }

    target_symbol = None
    if crypto_topic and crypto_topic.lower() in coin_name_map:
        target_symbol = coin_name_map[crypto_topic.lower()]
    
    # Fetch news based on the detected topic
    if crypto_topic and crypto_topic != "General" and crypto_topic != "None":
        news_query = f'"{crypto_topic}"'
        print(f"[CHATBOT] Crawling news with specific query: {news_query}")
        news_data = get_and_scrape_news(query=news_query, articles_to_fetch=3)
    else:
        print("[CHATBOT] Crawling news with generic query.")
        news_data = get_and_scrape_news(articles_to_fetch=3)
        
    full_article_content = news_data.get("full_text")
    sources_list_markdown = news_data.get("sources_markdown")
    has_relevant_news = bool(sources_list_markdown)

    # 4. Build Market Snapshot
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

    # 5. Perform Technical Analysis if a specific coin is targeted
    technical_analysis_summary = "No specific coin was targeted for technical analysis."
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

    # 6. Generate the Final Prompt for the AI
    prompt = ""
    language_instruction = f"You MUST formulate your entire response in {user_language}."

    additional_instruction = ""
    if intent == "crypto_question" and crypto_question_part.strip() != user_question.strip():
        additional_instruction = f"""
        ---
        IMPORTANT FINAL INSTRUCTION: The user's original question was "{user_question}". You have correctly focused on analyzing the crypto-related part. After providing your full, comprehensive analysis, you MUST add a concluding sentence at the very end. This sentence should politely state that the other topics in their original question are outside your expertise as a crypto analyst.
        """

    # Tạo Prompt cho Gemini
    if has_relevant_news:
        # Nếu CÓ tin-tức, dùng prompt đầy-đủ
        print("[CHATBOT] Found relevant news. Using full prompt.")
        prompt = f"""
        You are an expert crypto market analyst. Your analysis must be technical and data-driven.
        {language_instruction}
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

        Note : If there is no relevant news, do NOT create any ### Sources section or add links.
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

        **User's Question:** {crypto_question_part}

        {additional_instruction}

        **Your Expert Answer:**
        """
    else:
        # Nếu KHÔNG có tin-tức, dùng prompt rút-gọn
        print("[CHATBOT] No relevant news found. Using technical-only prompt.")
        prompt = f"""
        You are an expert crypto market analyst. Your analysis must be technical and data-driven.
        {language_instruction}
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

        **User's Question:** {crypto_question_part}

        {additional_instruction}

        **Your Expert Answer:**
        """

    # 7. Call Gemini API and return the answer
    try:
        response = model.generate_content(prompt)
        final_answer = response.text
        
        add_to_chat_history(session_id, user_question, final_answer, user_mood)
        
        return jsonify({"answer": final_answer})
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
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








