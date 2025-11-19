from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import yfinance as yf
import pandas as pd
import json
from kafka import KafkaProducer
import time

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
    "ltcusdt": "LTC-USD",
}

producer = KafkaProducer(
    bootstrap_servers="kafka:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_and_produce_yahoo_data(interval="1d", topic_name="crypto_prices_yahoo_daily"):
    print(f"\nStarting Yahoo Finance Producer for {interval} data")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 2)  # 2 years back, change if you want more/less data, be careful with rate limits

    for symbol_binance, symbol_yahoo in symbol_map.items():
        all_dfs = []

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
                    interval=interval,
                    progress=False,
                )
                if df.empty:
                    print(f"[WARNING] No data for {symbol_yahoo} from {start.date()} to {end.date()}")
                    current += relativedelta(months=1)
                    continue

                df = df.reset_index()
                all_dfs.append(df)

            except Exception as e:
                print(f"[ERROR] Failed to fetch data for {symbol_yahoo}: {e}")

            current += relativedelta(months=1)

        if not all_dfs:
            print(f"[WARNING] No data fetched for {symbol_yahoo}. Skipping...")
            continue

        all_data = pd.concat(all_dfs, ignore_index=True)

        all_data.columns = [
            (c[0].lower().replace(" ", "_") if isinstance(c, tuple) else c.lower().replace(" ", "_"))
            for c in all_data.columns
        ]

        if "date" in all_data.columns:
            all_data = all_data.rename(columns={"date": "datetime"})

        for _, row in all_data.iterrows():
            output = {
                "datetime": str(row["datetime"]),   # "2025-11-07 03:00:00"
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "symbol_binance": symbol_binance,
                "interval": interval
            }

            producer.send(topic_name, output)

    producer.flush()
    print(f"Yahoo producer for {interval} sent all messages to {topic_name}.")

if __name__ == "__main__":
    fetch_and_produce_yahoo_data("1h", "crypto_prices_yahoo_hourly")
    time.sleep(3)
    fetch_and_produce_yahoo_data("1d", "crypto_prices_yahoo_daily")