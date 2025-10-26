from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import yfinance as yf
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

end_date = datetime.now()
start_date = end_date - timedelta(days=365 * 2)

spark = SparkSession.builder.appName("YahooFinance_XGBoost").getOrCreate()

# Directory to store models
os.makedirs("./models", exist_ok=True)

for symbol_binance, symbol_yahoo in symbol_map.items():
    print(f"\n=== Processing {symbol_yahoo} ({symbol_binance}) ===")
    monthly_dfs = []
    current = start_date

    while current < end_date:
        start = current
        end = current + relativedelta(months=1)
        print(f"[INFO] Fetching {symbol_yahoo}: {start.date()} -> {end.date()}")

        try:
            df = yf.download(
                symbol_yahoo,
                start=start,
                end=end,
                interval="1h",
                progress=False,
            )
            if df.empty:
                print(f"[WARN] No data for {symbol_yahoo} in {start.strftime('%Y-%m')}")
                current += relativedelta(months=1)
                continue

            df.reset_index(inplace=True)
            monthly_dfs.append(df)
        except Exception as e:
            print(f"[ERROR] Failed to fetch {symbol_yahoo} in {start.strftime('%Y-%m')}: {e}")

        current += relativedelta(months=1)

    if not monthly_dfs:
        print(f"[ERROR] No valid data for {symbol_binance}. Skipping.")
        continue

    # Combine all data
    all_df = pd.concat(monthly_dfs)
    all_df.columns = [c[0].lower().replace(' ', '_') if isinstance(c, tuple)
                      else c.lower().replace(' ', '_') for c in all_df.columns]

    sdf = spark.createDataFrame(all_df)
    sdf = sdf.select("datetime", "open", "high", "low", "close", "volume")
    sdf = sdf.withColumnRenamed("close", "label")

    assembler = VectorAssembler(
        inputCols=["open", "high", "low", "volume"],
        outputCol="features"
    )
    sdf = assembler.transform(sdf).select("datetime", "open", "high", "low", "volume", "label", "features")

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

    # HDFS path
    hdfs_model_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol_binance}.pkl"

    # Upload to HDFS (overwrite if exists)
    try:
        os.system(f"hadoop fs -mkdir -p hdfs://hdfs-namenode:8020/models")
        os.system(f"hadoop fs -put -f {local_model_path} {hdfs_model_path}")
        print(f"[HDFS] Model uploaded to {hdfs_model_path}")
    except Exception as e:
        print(f"[ERROR] Failed to upload model to HDFS: {e}")

spark.stop()
print("\nAll models trained and saved locally + on HDFS.")
