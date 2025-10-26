import os, json
from pymongo import MongoClient

symbols = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
    "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"
]

mongo_uri = "mongodb://mongodb:27017"
client = MongoClient(mongo_uri)
db = client["crypto_batch"]
collection = db["predictions"]

base_dir = "/opt/spark/work-dir/models"

for sym in symbols:
    file_path = os.path.join(base_dir, f"xgboost_{sym}_prediction.json")
    if not os.path.exists(file_path):
        print(f"[WARN] Missing: {file_path}")
        continue
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            data["_id"] = sym
            collection.replace_one({"_id": sym}, data, upsert=True)
            print(f"[OK] Updated MongoDB for {sym}")
    except Exception as e:
        print(f"[ERROR] {sym}: {e}")
