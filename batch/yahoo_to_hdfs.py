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

end_date = datetime.now()
start_date = end_date - timedelta(days=365*8)  # 8 years of data

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
                interval="1d",  # Daily intervals for long-term analysis
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
    all_df.columns = [c[0].lower().replace(' ', '_') if isinstance(c, tuple) else c.lower().replace(' ', '_') for c in all_df.columns]
    
    # Rename 'date' column to 'datetime' for consistency
    if 'date' in all_df.columns:
        all_df = all_df.rename(columns={'date': 'datetime'})
    
    # Ensure datetime column is properly formatted
    all_df['datetime'] = pd.to_datetime(all_df['datetime'])

    sdf = spark.createDataFrame(all_df)
    save_path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol_binance.lower()}"
    sdf.write.mode("overwrite").parquet(save_path)

    print(f"[INFO] Saving {symbol_binance} to {save_path}")
    print(f"[SUCCESS] {symbol_binance} written to HDFS")

spark.stop()