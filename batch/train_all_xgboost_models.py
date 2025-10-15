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
        # Đường dẫn dữ liệu trên HDFS
        path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol}"
        df = spark.read.parquet(path).dropna()

        # Chọn cột và rename
        df = df.select("datetime", "open", "high", "low", "volume", "close")
        df = df.withColumnRenamed("close", "label")

        # Assemble features
        assembler = VectorAssembler(
            inputCols=["open", "high", "low", "volume"],
            outputCol="features"
        )
        df = assembler.transform(df).select("features", "label")

        # Convert to pandas
        pandas_df = df.toPandas()
        X = pandas_df["features"].apply(lambda x: x.toArray()).tolist()
        y = pandas_df["label"].tolist()

        # Train/test split
        split_idx = int(0.8 * len(X))
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        # Train model
        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1
        )
        model.fit(X_train, y_train)

        # Evaluate
        preds = model.predict(X_test)
        rmse = ((sum((p - y) ** 2 for p, y in zip(preds, y_test)) / len(y_test)) ** 0.5)
        print(f"[{symbol.upper()}] RMSE: {rmse:.4f}")

        local_model_path = f"/opt/spark/work-dir/models/xgboost_{symbol}.pkl"
        joblib.dump(model, local_model_path)
        print(f"[{symbol.upper()}] Model saved to {local_model_path}")

        latest_features = [X[-1]]  # predict next close
        next_pred = float(model.predict(latest_features)[0])
        prediction_result = {
            "symbol": symbol,
            "predicted_price": next_pred,
            "rmse": rmse
        }

        local_pred_path = f"/opt/spark/work-dir/models/xgboost_{symbol}_prediction.json"
        with open(local_pred_path, "w") as f:
            json.dump(prediction_result, f)

        print(f"[{symbol.upper()}] Prediction saved to {local_pred_path}")

        hdfs_model_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol}.pkl"
        hdfs_pred_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol}_prediction.json"

        os.system(f"hadoop fs -put -f {local_model_path} {hdfs_model_path}")
        os.system(f"hadoop fs -put -f {local_pred_path} {hdfs_pred_path}")

        print(f"[{symbol.upper()}] Uploaded .pkl model + prediction to HDFS")

    except Exception as e:
        print(f"[ERROR] Failed training {symbol.upper()}: {str(e)}")

spark.stop()
