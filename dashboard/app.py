from flask import Flask, jsonify, send_from_directory, render_template
import happybase
import os
import json
import time
import traceback
import xgboost as xgb
import numpy as np
import joblib

app = Flask(__name__)

def ensure_table_exists(connection, table_name):
    tables = [t.decode() for t in connection.tables()]
    if table_name not in tables:
        print(f"[Flask] Creating table '{table_name}'...")
        connection.create_table(
            table_name,
            {'data': dict(), 'meta': dict()}
        )
        time.sleep(3)

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

@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory("static", filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)









