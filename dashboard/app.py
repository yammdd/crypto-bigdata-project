from flask import Flask, jsonify, send_from_directory, render_template, request
import happybase
import os
import json
import time
import traceback
import xgboost as xgb
import numpy as np
import joblib
from pymongo import MongoClient
import google.generativeai as genai
import pandas as pd

app = Flask(__name__)

try:
    genai.configure(api_key=os.getenv("GOOGLE_AI_API_KEY"))
    model = genai.GenerativeModel("gemini-2.5-flash-lite")
except Exception as e:
    print(f"CẢNH BÁO: Không thể cấu hình Gemini. Lỗi: {e}")
    model = None

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

def calculate_rsi(df, period=14):
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
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

    # Lấy bối cảnh chung từ MongoDB
    try:
        client = MongoClient(os.getenv("MONGO_HOST", "mongodb"), 27017)
        db = client['crypto_db']
        collection = db['market_context']
        context = collection.find_one({"_id": CONTEXT_DOCUMENT_ID})
        client.close()
    except Exception as e:
        return jsonify({"error": f"Could not connect to context database: {e}"}), 500
        
    if not context or not context.get('real_time_data'):
        return jsonify({"answer": "I don't have enough market context yet. Please wait a moment and try again."})

    # Định dạng bối cảnh chung
    formatted_data_summary = "Here is the latest data for each symbol:\n"
    all_symbols = []
    for item in context.get('real_time_data', []):
        symbol = item.get('symbol', 'N/A').upper()
        all_symbols.append(symbol)
        price = item.get('price', 0)
        change_pct = item.get('price_change_pct', 0)
        formatted_data_summary += f"- **{symbol}**: Price=${price:.4f}, 24h Change={change_pct:.2f}%\n"

    # Phân tích câu hỏi của người dùng để tìm symbol
    target_symbol = None
    for s in all_symbols:
        if s.replace("USDT", "") in user_question.upper():
            target_symbol = s.replace("USDT", "").lower() + "usdt"
            break
            
    # Lấy dữ liệu lịch sử và tính chỉ báo
    technical_analysis_summary = "No specific technical analysis performed."
    if target_symbol:
        df_history = get_historical_data_from_hbase(target_symbol, limit=50)
        if df_history is not None and not df_history.empty and len(df_history) > 14:
            df_history['RSI_14'] = calculate_rsi(df_history, period=14)
            df_history['SMA_20'] = df_history['close'].rolling(window=20).mean()
            
            latest_indicators = df_history.iloc[-1]
            current_price = latest_indicators['close']
            rsi_14 = latest_indicators.get('RSI_14')
            sma_20 = latest_indicators.get('SMA_20')
            
            technical_analysis_summary = f"""
**Technical Analysis for {target_symbol.upper()}:**
- **Current Price:** ${current_price:.4f}
- **RSI (14 periods):** {rsi_14:.2f} (A value > 70 suggests overbought, < 30 suggests oversold).
- **Simple Moving Average (20 periods):** ${sma_20:.4f} (If price is above SMA, it's a bullish signal; if below, it's bearish).
- **Recent High:** ${df_history['high'].max():.4f} (Potential resistance level).
- **Recent Low:** ${df_history['low'].min():.4f} (Potential support level).
"""

    prompt = f"""
    You are an expert crypto market analyst. Your analysis must be technical and data-driven.
    Based on the following real-time market context, news, and specific technical indicators, answer the user's question.
    Present the data and analysis objectively and finally may give direct financial advice (e.g., "you should buy/sell") if justified by the data.

    --- General Market Context ---
    **Recent News:**
    {context.get('news_summary', 'No news available.')}

    **Overall Market Snapshot:**
    {formatted_data_summary}
    --- End of General Context ---

    --- Specific Technical Analysis ---
    {technical_analysis_summary}
    --- End of Specific Analysis ---

    **User's Question:** {user_question}

    **Your Expert Answer:**
    """
    # Gửi prompt đến mô hình Gemini
    try:
        response = model.generate_content(prompt)
        return jsonify({"answer": response.text})
    except Exception as e:
        return jsonify({"error": f"Error generating response from AI: {e}"}), 500

@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory("static", filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)









