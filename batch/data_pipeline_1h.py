# MODIFIED: Removed unnecessary imports for Yahoo Finance fetching (yfinance, dateutil.relativedelta, timedelta)
# MODIFIED: Added imports for Spark Kafka integration and JSON schema
from datetime import datetime  # KEEP: Still needed minimally, though not used much now
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp  # NEW: For Kafka data parsing
from pyspark.sql.types import StructType, StructField, StringType, DoubleType  # NEW: For defining JSON schema
from pyspark.ml.feature import VectorAssembler  # KEEP: Original, used for feature assembly
import pandas as pd  # KEEP: Original
import xgboost as xgb  # KEEP: Original
import joblib  # KEEP: Original, for model saving
import os  # KEEP: Original, for directories and HDFS upload

# KEEP: Original symbol_map
symbol_map = {
    "btcusdt": "BTC-USD",
    "ethusdt": "ETH-USD",
    "solusdt": "SOL-USD",
    "bnbusdt": "BNB-USD",
    "xrpusdt": "XRP-USD",
    "adausdt": "ADA-USD",
    "dogeusdt": "DOGE-USD",
    "linkusdt": "LINK-USD",
    "dotusdt": "DOT-USD",
    "ltcusdt": "LTC-USD"
}

# MODIFIED: Removed end_date and start_date as no longer needed for monthly batch fetching

# NEW: Define schema for Kafka JSON messages (matching producer output)
kafka_schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("symbol_binance", StringType(), True),
    StructField("interval", StringType(), True)
])

# KEEP: Original Spark init
spark = SparkSession.builder.appName("YahooFinance_XGBoost").getOrCreate()

# KEEP: Original models directory creation
os.makedirs("./models", exist_ok=True)

# MODIFIED: Replaced entire Yahoo fetching and Spark DF creation with Kafka consumption
# NEW: Read from Kafka topic (using "crypto_prices_yahoo_hourly" for 1h interval)
kafka_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto_prices_yahoo_hourly") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Parse JSON value and convert datetime to timestamp
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data")
).select("data.*")

# Convert datetime string to timestamp
parsed_df = parsed_df.withColumn("datetime", to_timestamp(col("datetime")))

# MODIFIED: Moved the per-symbol processing loop here (no more fetching inside loop)
for symbol_binance, symbol_yahoo in symbol_map.items():  # KEEP: Iterate over symbols
    print(f"\n=== Processing {symbol_yahoo} ({symbol_binance}) ===")
    
    # NEW: Filter Kafka data for this symbol
    try:
        symbol_sdf = parsed_df.filter(col("symbol_binance") == symbol_binance)
        
        if symbol_sdf.count() == 0:
            print(f"[ERROR] No valid data for {symbol_binance}. Skipping.")
            continue

        # Sort by datetime to ensure order  # NEW: Added for consistency with original sorted data
        symbol_sdf = symbol_sdf.orderBy("datetime")
        
        # KEEP: Original Spark transformations (select, rename close to label)
        sdf = symbol_sdf.select("datetime", "open", "high", "low", "close", "volume")
        sdf = sdf.withColumnRenamed("close", "label")

        # KEEP: Original VectorAssembler
        assembler = VectorAssembler(
            inputCols=["open", "high", "low", "volume"],
            outputCol="features"
        )
        sdf = assembler.transform(sdf).select("datetime", "open", "high", "low", "volume", "label", "features")

        # KEEP: Original pandas conversion methods (no changes)
        try:
            # Method 1: Convert datetime to string first
            sdf_str = sdf.withColumn("datetime_str", sdf["datetime"].cast("string"))
            sdf_no_dt = sdf_str.drop("datetime")
            pdf = sdf_no_dt.toPandas()
            pdf["datetime"] = pd.to_datetime(pdf["datetime_str"])
            pdf = pdf.drop(columns=["datetime_str"])
            pdf = pdf.sort_values("datetime").reset_index(drop=True)

        except Exception as e1:
            print(f"[WARN] {symbol_binance.upper()}: Pandas conversion method 1 failed: {e1}")
            try:
                # Method 2: Drop datetime column entirely
                sdf_no_dt = sdf.drop("datetime")
                pdf = sdf_no_dt.toPandas()
                pdf["datetime"] = pd.date_range(start="2020-01-01", periods=len(pdf), freq="H")
            except Exception as e2:
                print(f"[ERROR] {symbol_binance.upper()}: Pandas conversion failed entirely: {e2}")
                continue

        # KEEP: Original data sufficiency check
        if len(pdf) < 500:
            print(f"[WARN] {symbol_binance.upper()}: Not enough data ({len(pdf)} rows)")
            continue

        # KEEP: Original feature and label extraction (from Spark ML Vectors)
        X = pdf["features"].apply(lambda x: x.toArray()).tolist()
        y = pdf["label"].tolist()

        # KEEP: Original split
        split_idx = int(0.8 * len(X))
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        # KEEP: Original XGBoost model and training
        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            random_state=42
        )
        model.fit(X_train, y_train)

        # KEEP: Original evaluation (RMSE calculation)
        preds = model.predict(X_test)
        rmse = ((sum((p - y) ** 2 for p, y in zip(preds, y_test)) / len(y_test)) ** 0.5)
        print(f"[RESULT] RMSE for {symbol_binance.upper()}: {rmse:.4f}")

        # KEEP: Original local model save with joblib
        local_model_path = f"./models/xgboost_{symbol_binance}.pkl"
        joblib.dump(model, local_model_path)
        print(f"[LOCAL] Model saved at {local_model_path}")

        # KEEP: Original HDFS upload (mkdir and put)
        hdfs_model_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol_binance}.pkl"

        # Upload to HDFS (overwrite if exists)
        try:
            os.system(f"hadoop fs -mkdir -p hdfs://hdfs-namenode:8020/models")
            os.system(f"hadoop fs -put -f {local_model_path} {hdfs_model_path}")
            print(f"[HDFS] Model uploaded to {hdfs_model_path}")
        except Exception as e:
            print(f"[ERROR] Failed to upload model to HDFS: {e}")

    except Exception as e:
        print(f"[ERROR] Failed to process {symbol_binance}: {e}")
        continue

# KEEP: Original final Spark stop and print
spark.stop()
print("\nAll models trained and saved locally + on HDFS.")
