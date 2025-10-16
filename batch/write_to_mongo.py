# batch/write_to_mongo.py

import os
import json
from pymongo import MongoClient

symbols = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
    "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"
]

mongo_uri = "mongodb://mongodb:27017"
client = MongoClient(mongo_uri)
db = client["crypto_batch"]
collection = db["predictions"]

model_dir = "/opt/spark/work-dir/models"

for symbol in symbols:
    try:
        local_path = os.path.join(model_dir, f"xgboost_{symbol}_prediction.json")
        if not os.path.exists(local_path):
            print(f"[WARN] File not found: {local_path}")
            continue

        with open(local_path, "r") as f:
            data = json.load(f)
            data["_id"] = symbol
            collection.replace_one({"_id": symbol}, data, upsert=True)
            print(f"[OK] Inserted prediction for {symbol}")

    except Exception as e:
        print(f"[ERROR] {symbol}: {e}")
