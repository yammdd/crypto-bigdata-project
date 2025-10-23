from flask import Flask, jsonify, send_from_directory, render_template, request
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

app = Flask(__name__)

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

def get_latest_crypto_news(query="crypto OR bitcoin OR ethereum", days=1, page_size=5): # Giảm xuống 5 tin để prompt gọn hơn
    if not newsapi:
        return {
            "summary": "News service is not configured.",
            "sources_markdown": ""
        }
    try:
        from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        response = newsapi.get_everything(
            q=query, from_param=from_date, sort_by="relevancy", language="en", page_size=page_size
        )
        
        summary_lines = []
        sources_markdown = []
        
        for article in response.get("articles", []):
            title = article.get('title', 'No Title')
            url = article.get('url', '#')
            # Tóm tắt chỉ chứa tiêu đề cho ngắn gọn
            summary_lines.append(f"- {title}")
            # Nguồn chứa cả tiêu đề và link dạng Markdown
            sources_markdown.append(f"- [{title}]({url})")
            
        return {
            "summary": "\n".join(summary_lines),
            "sources_markdown": "\n".join(sources_markdown)
        }
    except Exception as e:
        return {
            "summary": f"Error fetching news: {e}",
            "sources_markdown": ""
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

    # LẤY BỐI CẢNH TIN TỨC TRỰC TIẾP
    news_context = get_latest_crypto_news()
    news_summary = news_context.get("summary")
    sources_list_markdown = news_context.get("sources_markdown")

    # XÂY DỰNG BẢN TÓM TẮT THỊ TRƯỜNG TỔNG QUAN TỪ HBASE
    all_symbols = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt", "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"]
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
    prompt = f"""
    You are an expert crypto market analyst. Your analysis must be technical and data-driven.
    Based on the following news, overall market snapshot, and specific technical indicators (if available), answer the user's question.
    Present the data and analysis objectively and finally may give direct financial advice (e.g., "you should buy/sell") if justified by the data.
    When you use information from a news article, you MUST end the sentence with a citation that includes the article's title in markdown link format, like this: "This happened recently [cite: Title of The Article](url_to_article).".
    Answer in the same language as the user's question.
    
    --- General Market Context ---
    **Recent News:**
    {news_summary}

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

    # 6. Gọi Gemini API và trả lời
    try:
        response = model.generate_content(prompt)
        return jsonify({"answer": response.text})
    except Exception as e:
        print(f"Lỗi khi gọi Gemini API: {e}")
        return jsonify({"error": f"Error generating response from AI: {e}"}), 500


@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory("static", filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)









