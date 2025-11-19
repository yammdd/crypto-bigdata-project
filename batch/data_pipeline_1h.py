from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType  
from pyspark.ml.feature import VectorAssembler  
import pandas as pd  
import xgboost as xgb  
import joblib  
import os  

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

spark = SparkSession.builder.appName("YahooFinance_XGBoost").getOrCreate()

os.makedirs("./models", exist_ok=True)

kafka_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto_prices_yahoo_hourly") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn("datetime", to_timestamp(col("datetime")))

for symbol_binance, symbol_yahoo in symbol_map.items():  
    print(f"\nProcessing {symbol_yahoo} ({symbol_binance})")
    
    try:
        symbol_sdf = parsed_df.filter(col("symbol_binance") == symbol_binance)
        
        if symbol_sdf.count() == 0:
            print(f"[ERROR] No valid data for {symbol_binance}. Skipping.")
            continue

        symbol_sdf = symbol_sdf.orderBy("datetime")
        
        sdf = symbol_sdf.select("datetime", "open", "high", "low", "close", "volume")
        sdf = sdf.withColumnRenamed("close", "label")

        assembler = VectorAssembler(
            inputCols=["open", "high", "low", "volume"],
            outputCol="features"
        )
        sdf = assembler.transform(sdf).select("datetime", "open", "high", "low", "volume", "label", "features")

        try:
            sdf_str = sdf.withColumn("datetime_str", sdf["datetime"].cast("string"))
            sdf_no_dt = sdf_str.drop("datetime")
            pdf = sdf_no_dt.toPandas()
            pdf["datetime"] = pd.to_datetime(pdf["datetime_str"])
            pdf = pdf.drop(columns=["datetime_str"])
            pdf = pdf.sort_values("datetime").reset_index(drop=True)

        except Exception as e1:
            print(f"[WARN] {symbol_binance.upper()}: Pandas conversion method 1 failed: {e1}")
            try:
                sdf_no_dt = sdf.drop("datetime")
                pdf = sdf_no_dt.toPandas()
                pdf["datetime"] = pd.date_range(start="2020-01-01", periods=len(pdf), freq="H")
            except Exception as e2:
                print(f"[ERROR] {symbol_binance.upper()}: Pandas conversion failed entirely: {e2}")
                continue

        if len(pdf) < 500:
            print(f"[WARN] {symbol_binance.upper()}: Not enough data ({len(pdf)} rows)")
            continue

        X = pdf["features"].apply(lambda x: x.toArray()).tolist()
        y = pdf["label"].tolist()

        split_idx = int(0.8 * len(X))
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            random_state=42
        )
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        rmse = ((sum((p - y) ** 2 for p, y in zip(preds, y_test)) / len(y_test)) ** 0.5)
        print(f"[RESULT] RMSE for {symbol_binance.upper()}: {rmse:.4f}")

        local_model_path = f"./models/xgboost_{symbol_binance}.pkl"
        joblib.dump(model, local_model_path)
        print(f"[LOCAL] Model saved at {local_model_path}")

        hdfs_model_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol_binance}.pkl"

        try:
            os.system(f"hadoop fs -mkdir -p hdfs://hdfs-namenode:8020/models")
            os.system(f"hadoop fs -put -f {local_model_path} {hdfs_model_path}")
            print(f"[HDFS] Model uploaded to {hdfs_model_path}")
        except Exception as e:
            print(f"[ERROR] Failed to upload model to HDFS: {e}")

    except Exception as e:
        print(f"[ERROR] Failed to process {symbol_binance}: {e}")
        continue

spark.stop()