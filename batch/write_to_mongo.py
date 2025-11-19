import os, json
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col  
import pandas as pd 

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

spark_hist = SparkSession.builder.appName("HistoricalDataToMongo").getOrCreate()
hist_collection = db["historical_prices"]
hdfs_base = "hdfs://hdfs-namenode:8020/crypto/yahoo"

for sym in symbols:
    try:
        path = f"{hdfs_base}/{sym}"
        df = spark_hist.read.parquet(path).dropna()
        
        if df.count() == 0:
            print(f"[WARN] No historical data for {sym} in HDFS")
            continue

        df = df.withColumn("datetime_str", col("datetime").cast("string"))
        df_no_dt = df.drop("datetime") 
        
        pdf = df_no_dt.toPandas()
        pdf['datetime'] = pd.to_datetime(pdf['datetime_str'], utc=True).dt.tz_localize(None)
        pdf = pdf.drop('datetime_str', axis=1)
        
        for _, row in pdf.iterrows():
            dt_str = row['datetime'].isoformat() if hasattr(row['datetime'], 'isoformat') else str(row['datetime'])
            doc = {
                "_id": f"{sym}_{dt_str[:19].replace(':', '-').replace(' ', 'T')}",  # Composite key an toàn (YYYY-MM-DDTHH-MM-SS)
                "symbol": sym,
                "datetime": dt_str,
                "open": float(row['open']),
                "high": float(row['high']),
                "low": float(row['low']),
                "close": float(row['close']),
                "volume": float(row['volume'])
            }
            hist_collection.replace_one(
                {"_id": doc["_id"]}, 
                doc, 
                upsert=True
            )
        
        print(f"[OK] Updated historical data to MongoDB for {sym} ({len(pdf)} records)")
        
    except Exception as e:
        print(f"[ERROR] Historical {sym}: {e}")

spark_hist.stop()