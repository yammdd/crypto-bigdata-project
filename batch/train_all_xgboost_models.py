from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
import xgboost as xgb
import os

from pyspark.ml.linalg import Vectors
from pyspark.ml.functions import vector_to_array

symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
           "adausdt", "dogeusdt", "linkusdt", "dotusdt", "maticusdt"]

spark = SparkSession.builder.appName("XGBoostMultiCrypto").getOrCreate()

for symbol in symbols:
    print(f"\n=== Training model for {symbol.upper()} ===")
    
    try:
        path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol}"
        df = spark.read.parquet(path).dropna()

        df = df.select("datetime", "open", "high", "low", "volume", "close")
        df = df.withColumnRenamed("close", "label")

        assembler = VectorAssembler(
            inputCols=["open", "high", "low", "volume"],
            outputCol="features"
        )
        df = assembler.transform(df).select("features", "label")

        # Convert to Pandas
        pandas_df = df.toPandas()
        X = pandas_df["features"].apply(lambda x: x.toArray()).tolist()
        y = pandas_df["label"].tolist()

        # Train/test split
        split_idx = int(0.8 * len(X))
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        # XGBoost training
        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1
        )
        model.fit(X_train, y_train)

        # Evaluate
        preds = model.predict(X_test)
        rmse = ((sum((p - y)**2 for p, y in zip(preds, y_test)) / len(y_test))**0.5)
        print(f"[{symbol.upper()}] RMSE: {rmse:.4f}")

        # Save model to local path first
        local_path = f"/opt/spark/work-dir/models/xgboost_{symbol}"
        model.save_model(f"{local_path}.json")
        print(f"[{symbol.upper()}] Model saved locally to {local_path}.json")

        # Upload to HDFS
        hdfs_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol}.json"
        os.system(f"hadoop fs -put -f {local_path}.json {hdfs_path}")
        print(f"[{symbol.upper()}] Model uploaded to HDFS: {hdfs_path}")

    except Exception as e:
        print(f"[ERROR] Failed training {symbol.upper()}: {str(e)}")

spark.stop()
