from pyspark.sql import SparkSession
import json
import requests
from datetime import datetime, timedelta

def get_current_market_prices():
    """Get current market prices from CoinGecko"""
    symbols = {
        "btcusdt": "bitcoin",
        "ethusdt": "ethereum", 
        "solusdt": "solana",
        "bnbusdt": "binancecoin",
        "xrpusdt": "ripple",
        "adausdt": "cardano",
        "dogeusdt": "dogecoin",
        "linkusdt": "chainlink",
        "dotusdt": "polkadot",
        "ltcusdt": "litecoin"
    }
    
    try:
        ids = ",".join(symbols.values())
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        current_prices = {}
        for binance_symbol, coingecko_id in symbols.items():
            if coingecko_id in data:
                current_prices[binance_symbol] = data[coingecko_id]["usd"]
        
        return current_prices
    except Exception as e:
        print(f"Error fetching current prices: {e}")
        return {}

def check_data_freshness():
    """Check how fresh the data is"""
    print(" Data Freshness Check ")
    
    spark = SparkSession.builder.appName("DataFreshnessCheck").getOrCreate()
    
    symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt"]
    
    for symbol in symbols:
        print(f"\n {symbol.upper()} ")
        try:
            path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol}"
            df = spark.read.parquet(path)
            
            # Get the most recent date
            if 'datetime' in df.columns:
                date_col = 'datetime'
            elif 'date' in df.columns:
                date_col = 'date'
            else:
                print(f"No datetime column found")
                continue
            
            max_date = df.select(date_col).rdd.max()[0]
            min_date = df.select(date_col).rdd.min()[0]
            
            print(f"Date range: {min_date} to {max_date}")
            
            # Check how recent the data is
            if isinstance(max_date, str):
                max_date = datetime.strptime(max_date.split()[0], '%Y-%m-%d')
            
            days_old = (datetime.now() - max_date).days
            print(f"Data is {days_old} days old")
            
            if days_old > 7:
                print(f"WARNING: Data is more than a week old!")
            elif days_old > 1:
                print(f"WARNING: Data is more than a day old!")
            else:
                print(f"Data is fresh")
            
            # Get the last price from data
            last_row = df.orderBy(df[date_col].desc()).limit(1).collect()[0]
            last_price = last_row['close']
            print(f"Last price in data: ${last_price:,.2f}")
            
        except Exception as e:
            print(f"Error: {e}")
    
    spark.stop()

def check_model_predictions():
    """Check model predictions vs current market"""
    print("\n Model Prediction Analysis ")
    current_prices = get_current_market_prices()
    
    symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt"]
    
    for symbol in symbols:
        print(f"\n {symbol.upper()} ")
        try:
            # Read prediction file
            with open(f"/opt/spark/work-dir/models/xgboost_{symbol}_prediction.json", "r") as f:
                pred_data = json.load(f)
            
            predicted_price = pred_data['predicted_price']
            last_price = pred_data['last_price']
            current_price = current_prices.get(symbol, None)
            
            print(f"Predicted Price: ${predicted_price:,.2f}")
            print(f"Last Price (in data): ${last_price:,.2f}")
            if current_price:
                print(f"Current Market Price: ${current_price:,.2f}")
                
                # Calculate errors
                pred_error = abs(predicted_price - current_price) / current_price * 100
                last_error = abs(last_price - current_price) / current_price * 100
                
                print(f"Prediction Error: {pred_error:.2f}%")
                print(f"Last Price Error: {last_error:.2f}%")
                
                if pred_error < last_error:
                    print(f"Model prediction is better than last price")
                else:
                    print(f"Model prediction is worse than last price")
            
            # Check prediction vs last price difference
            pred_diff = abs(predicted_price - last_price) / last_price * 100
            print(f"Prediction vs Last Price: {pred_diff:.2f}% difference")
            
            if pred_diff > 20:
                print(f"WARNING: Large difference between prediction and last price!")
            
            # Model performance
            rmse = pred_data.get('rmse', 0)
            mape = pred_data.get('mape', 0)
            print(f"Model RMSE: {rmse:.4f}")
            print(f"Model MAPE: {mape:.2f}%")
            
        except FileNotFoundError:
            print(f"Prediction file not found")
        except Exception as e:
            print(f"Error: {e}")

def main():
    print("Crypto Prediction Diagnostic Tool")
    print(f"Check Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    check_data_freshness()
    check_model_predictions()
    
    print("\n Recommendations ")
    print("1. If data is old (>1 day), re-run yahoo_to_hdfs.py")
    print("2. If predictions are very different from last price, check model training")
    print("3. If MAPE > 10%, consider retraining with more recent data")
    print("4. If prediction errors are high, check feature engineering")

if __name__ == "__main__":
    main()