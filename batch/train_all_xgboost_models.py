from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import xgboost as xgb
import os
import json
import joblib
import pandas as pd

symbols = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
    "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"
]

spark = SparkSession.builder.appName("XGBoostMultiCrypto").getOrCreate()

for symbol in symbols:
    print(f"\n=== Training model for {symbol.upper()} ===")
    try:
        # Đọc dữ liệu từ HDFS
        path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol}"
        df = spark.read.parquet(path).dropna()

        df = df.select("datetime", "open", "high", "low", "volume", "close")
        df = df.withColumnRenamed("close", "label")

        assembler = VectorAssembler(
            inputCols=["open", "high", "low", "volume"],
            outputCol="features"
        )
        df = assembler.transform(df)

        # ✅ KHÔNG LẤY CỘT DATETIME để tránh lỗi chuyển kiểu
        pdf = df.select("open", "high", "low", "volume", "label", "features").toPandas()

        X = [x.toArray() for x in pdf["features"]]
        y = pdf["label"].tolist()

        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42
        )
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        rmse = ((sum((p - y) ** 2 for p, y in zip(preds, y_test)) / len(y_test)) ** 0.5)
        print(f"[{symbol.upper()}] RMSE: {rmse:.4f}")

        last_row = pdf.iloc[-1]
        latest_features = [X[-1]]
        next_pred = float(model.predict(latest_features)[0])

        prediction_result = {
            "symbol": symbol,
            "predicted_price": next_pred,
            "last_price": float(last_row["label"]),
            "open": float(last_row["open"]),
            "high": float(last_row["high"]),
            "low": float(last_row["low"]),
            "volume": float(last_row["volume"]),
            "ma_6h": float(pdf["label"].tail(6).mean()),
            "ma_24h": float(pdf["label"].tail(24).mean()),
            "ma_72h": float(pdf["label"].tail(72).mean()),
            "volatility": float(pdf["label"].tail(24).std()),
            "price_change_pct": float((last_row["label"] - pdf["label"].iloc[-2]) / pdf["label"].iloc[-2]),
            "volume_change_pct": float((last_row["volume"] - pdf["volume"].iloc[-2]) / pdf["volume"].iloc[-2]),
            "high_low_ratio": float(last_row["high"] / last_row["low"]),
            "high_close_diff": float(last_row["high"] - last_row["label"]),
            "low_close_diff": float(last_row["label"] - last_row["low"]),
            "rmse": float(rmse)
        }

        # Lưu local
        local_model_path = f"/opt/spark/work-dir/models/xgboost_{symbol}.pkl"
        joblib.dump(model, local_model_path)

        local_pred_path = f"/opt/spark/work-dir/models/xgboost_{symbol}_prediction.json"
        with open(local_pred_path, "w") as f:
            json.dump(prediction_result, f, indent=2)

        print(f"[{symbol.upper()}] Model & prediction saved to local")

        # Upload lên HDFS
        hdfs_model_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol}.pkl"
        hdfs_pred_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol}_prediction.json"

        os.system(f"hadoop fs -put -f {local_model_path} {hdfs_model_path}")
        os.system(f"hadoop fs -put -f {local_pred_path} {hdfs_pred_path}")

        print(f"[{symbol.upper()}] Uploaded to HDFS successfully")

    except Exception as e:
        print(f"[ERROR] {symbol.upper()}: {e}")

print("Training finished for all symbols.")
spark.stop()
