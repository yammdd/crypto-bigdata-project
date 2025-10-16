from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import xgboost as xgb
import os
import json
import joblib

symbols = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
    "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"
]

spark = SparkSession.builder.appName("XGBoostMultiCrypto").getOrCreate()

for symbol in symbols:
    print(f"\n=== Training model for {symbol.upper()} ===")
    try:
        # Load from HDFS
        path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol}"
        df = spark.read.parquet(path).dropna()

        df = df.select("datetime", "open", "high", "low", "volume", "close")
        df = df.withColumnRenamed("close", "label")

        assembler = VectorAssembler(
            inputCols=["open", "high", "low", "volume"],
            outputCol="features"
        )
        df = assembler.transform(df).select("datetime", "open", "high", "low", "volume", "label", "features")

        pandas_df = df.toPandas()
        X = pandas_df["features"].apply(lambda x: x.toArray()).tolist()
        y = pandas_df["label"].tolist()

        # Last row info
        last_row = pandas_df.iloc[-1]
        latest_features = [X[-1]]
        last_open = float(last_row["open"])
        last_close = float(last_row["label"])
        last_high = float(last_row["high"])
        last_low = float(last_row["low"])
        last_volume = float(last_row["volume"])

        change_24h = last_close - last_open

        # Train/Test Split
        split_idx = int(0.8 * len(X))
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        # Train model
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

        # Save model
        local_model_path = f"/opt/spark/work-dir/models/xgboost_{symbol}.pkl"
        joblib.dump(model, local_model_path)
        print(f"[{symbol.upper()}] Model saved to {local_model_path}")

        # Predict next
        next_pred = float(model.predict(latest_features)[0])

        # Save prediction info
        prediction_result = {
            "symbol": symbol,
            "predicted_price": next_pred,
            "last_price": last_close,
            "change_24h": change_24h,
            "volume": last_volume,
            "high": last_high,
            "low": last_low,
            "open": last_open,
            "rmse": rmse
        }

        local_pred_path = f"/opt/spark/work-dir/models/xgboost_{symbol}_prediction.json"
        with open(local_pred_path, "w") as f:
            json.dump(prediction_result, f)

        print(f"[{symbol.upper()}] Prediction saved to {local_pred_path}")

        # Upload to HDFS
        hdfs_model_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol}.pkl"
        hdfs_pred_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol}_prediction.json"

        os.system(f"hadoop fs -put -f {local_model_path} {hdfs_model_path}")
        os.system(f"hadoop fs -put -f {local_pred_path} {hdfs_pred_path}")
        print(f"[{symbol.upper()}] Uploaded model & prediction to HDFS")

    except Exception as e:
        print(f"[ERROR] Failed training {symbol.upper()}: {e}")

spark.stop()
