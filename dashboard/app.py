# ===============================
# 📁 dashboard/app.py
# ===============================
from flask import Flask, jsonify, send_from_directory, render_template
import happybase
import os
import json
import time
import traceback

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
def get_prediction(symbol):
    try:
        file_path = f"models/xgboost_{symbol.lower()}.json"
        if not os.path.exists(file_path):
            return jsonify({"error": "Prediction file not found"}), 404

        with open(file_path, "r") as f:
            data = json.load(f)
        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory("static", filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)