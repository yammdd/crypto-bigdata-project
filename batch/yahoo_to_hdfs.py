from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
import yfinance as yf
import pandas as pd
import os

# Map Binance symbols to Yahoo Finance symbols
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

# Output HDFS path
output_dir = "hdfs://namenode:9000/crypto/yahoo"

# Range to fetch: from Oct 2023 to Oct 2025 (you can change this)
start_date = datetime(2023, 10, 1)
end_date = datetime(2025, 10, 1)

# Initialize Spark
spark = SparkSession.builder.appName("YahooFinanceBatch").getOrCreate()

for symbol_binance, symbol_yahoo in symbol_map.items():
    print(f"[INFO] Start fetching {symbol_yahoo}...")
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
        print(f"[ERROR] No valid data for {symbol_binance} over all months.")
        continue

    # Combine all monthly data
    all_df = pd.concat(monthly_dfs)
    #all_df.columns = [c.lower().replace(' ', '_') for c in all_df.columns]
    all_df.columns = [c[0].lower().replace(' ', '_') if isinstance(c, tuple) else c.lower().replace(' ', '_') for c in all_df.columns]

    sdf = spark.createDataFrame(all_df)
    save_path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol_binance.lower()}"
    sdf.write.mode("overwrite").parquet(save_path)

    print(f"[INFO] Saving {symbol_binance} to {save_path}")
    print(f"[SUCCESS] {symbol_binance} written to HDFS")
    #print("DEBUG: all_df.columns =", all_df.columns)
    #print("DEBUG: type(all_df.columns[0]) =", type(all_df.columns[0]))

spark.stop()
