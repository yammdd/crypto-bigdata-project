from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
import yfinance as yf
import pandas as pd
import os
from pyspark.ml.feature import VectorAssembler
import xgboost as xgb
import json
import joblib
import numpy as np
from sklearn.preprocessing import StandardScaler

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
start_date = end_date - timedelta(days=365*2)  

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

symbols = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
    "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"
]

def calculate_technical_indicators(df):
    """Calculate comprehensive technical indicators for daily data"""
    df = df.copy()
    
    # Price-based features
    df['price_change'] = df['close'].pct_change()
    df['high_low_ratio'] = df['high'] / df['low']
    df['close_open_ratio'] = df['close'] / df['open']
    df['volume_price_ratio'] = df['volume'] / df['close']
    
    # Moving averages (for daily data)
    df['ma_7d'] = df['close'].rolling(window=7).mean()
    df['ma_14d'] = df['close'].rolling(window=14).mean()
    df['ma_30d'] = df['close'].rolling(window=30).mean()
    df['ma_90d'] = df['close'].rolling(window=90).mean()
    df['ma_200d'] = df['close'].rolling(window=200).mean()
    
    # Moving average ratios
    df['ma_7_14_ratio'] = df['ma_7d'] / df['ma_14d']
    df['ma_14_30_ratio'] = df['ma_14d'] / df['ma_30d']
    df['ma_30_90_ratio'] = df['ma_30d'] / df['ma_90d']
    df['price_ma_200_ratio'] = df['close'] / df['ma_200d']
    
    # Volatility indicators
    df['volatility_7d'] = df['close'].rolling(window=7).std()
    df['volatility_14d'] = df['close'].rolling(window=14).std()
    df['volatility_30d'] = df['close'].rolling(window=30).std()
    
    # Volume indicators
    df['volume_ma_7d'] = df['volume'].rolling(window=7).mean()
    df['volume_ma_14d'] = df['volume'].rolling(window=14).mean()
    df['volume_ratio_7d'] = df['volume'] / df['volume_ma_7d']
    df['volume_ratio_14d'] = df['volume'] / df['volume_ma_14d']
    
    # RSI (Relative Strength Index)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # MACD
    exp1 = df['close'].ewm(span=12).mean()
    exp2 = df['close'].ewm(span=26).mean()
    df['macd'] = exp1 - exp2
    df['macd_signal'] = df['macd'].ewm(span=9).mean()
    df['macd_histogram'] = df['macd'] - df['macd_signal']
    
    # Bollinger Bands
    df['bb_middle'] = df['close'].rolling(window=20).mean()
    bb_std = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
    df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
    df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']
    df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
    
    # Price momentum
    df['momentum_5d'] = df['close'] / df['close'].shift(5) - 1
    df['momentum_10d'] = df['close'] / df['close'].shift(10) - 1
    df['momentum_20d'] = df['close'] / df['close'].shift(20) - 1
    
    # Support and resistance levels
    df['support_level'] = df['low'].rolling(window=20).min()
    df['resistance_level'] = df['high'].rolling(window=20).max()
    df['support_distance'] = (df['close'] - df['support_level']) / df['close']
    df['resistance_distance'] = (df['resistance_level'] - df['close']) / df['close']
    
    return df

spark = SparkSession.builder.appName("XGBoostMultiCrypto").getOrCreate()

for symbol in symbols:
    try:
        # Read data from HDFS
        path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol}"
        df = spark.read.parquet(path).dropna()
        
        # Show the most recent data instead of oldest
        datetime_col = 'datetime' if 'datetime' in df.columns else 'date'
        
        try:
            datetime_col = None
            if 'datetime' in df.columns:
                datetime_col = 'datetime'
            elif 'date' in df.columns:
                datetime_col = 'date'
            else:
                raise Exception("No datetime or date column found")
            
            # Convert datetime to string first, then to pandas
            df_with_string_date = df.withColumn("datetime_str", df[datetime_col].cast("string"))
            df_no_datetime = df_with_string_date.drop(datetime_col)
            
            pdf = df_no_datetime.toPandas()
            pdf = pdf.reset_index(drop=True)
            
            # Convert string back to datetime in pandas
            pdf['datetime'] = pd.to_datetime(pdf['datetime_str'])
            pdf = pdf.drop('datetime_str', axis=1)
            pdf = pdf.sort_values('datetime').reset_index(drop=True)
              
        except Exception as pandas_error:
            print(f"[WARN] {symbol.upper()}: Method 1 failed, trying method 2: {pandas_error}")
            try:
                # Method 2: Drop datetime/date column entirely and work with index
                datetime_col = 'datetime' if 'datetime' in df.columns else 'date'
                df_no_datetime = df.drop(datetime_col)
                pdf = df_no_datetime.toPandas()
                pdf = pdf.reset_index(drop=True)
                
                # Add a dummy datetime column for compatibility
                pdf['datetime'] = pd.date_range(start='2020-01-01', periods=len(pdf), freq='D')
                                
            except Exception as pandas_error2:
                print(f"[ERROR] {symbol.upper()}: Method 2 failed, trying method 3: {pandas_error2}")
                try:
                    # Method 3: Use Spark SQL to convert datetime/date to string, then pandas
                    df.createOrReplaceTempView("temp_df")
                    
                    # Determine which datetime column exists
                    datetime_col = 'datetime' if 'datetime' in df.columns else 'date'
                    
                    df_converted = spark.sql(f"""
                        SELECT 
                            date_format({datetime_col}, 'yyyy-MM-dd HH:mm:ss') as datetime_str,
                            open, high, low, close, volume
                        FROM temp_df
                    """)
                    
                    pdf = df_converted.toPandas()
                    pdf['datetime'] = pd.to_datetime(pdf['datetime_str'])
                    pdf = pdf.drop('datetime_str', axis=1)
                    pdf = pdf.sort_values('datetime').reset_index(drop=True)
                                     
                except Exception as pandas_error3:
                    print(f"[ERROR] {symbol.upper()}: All pandas conversion methods failed: {pandas_error3}")
                    continue
        
        # Calculate comprehensive technical indicators
        pdf = calculate_technical_indicators(pdf)
        
        # Drop rows with NaN values (from rolling calculations)
        pdf = pdf.dropna()
        
        if len(pdf) < 500:  # Need sufficient data for training
            print(f"[WARN] {symbol.upper()}: Insufficient data ({len(pdf)} rows)")
            continue
        
        # Define feature columns (excluding datetime and target)
        feature_cols = [
            'open', 'high', 'low', 'volume', 'close',
            'price_change', 'high_low_ratio', 'close_open_ratio', 'volume_price_ratio',
            'ma_7d', 'ma_14d', 'ma_30d', 'ma_90d', 'ma_200d',
            'ma_7_14_ratio', 'ma_14_30_ratio', 'ma_30_90_ratio', 'price_ma_200_ratio',
            'volatility_7d', 'volatility_14d', 'volatility_30d',
            'volume_ma_7d', 'volume_ma_14d', 'volume_ratio_7d', 'volume_ratio_14d',
            'rsi', 'macd', 'macd_signal', 'macd_histogram',
            'bb_width', 'bb_position',
            'momentum_5d', 'momentum_10d', 'momentum_20d',
            'support_distance', 'resistance_distance'
        ]
        
        # Prepare features and target
        X = pdf[feature_cols].values
        y = pdf['close'].values
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Split data (80% train, 20% test)
        split_idx = int(len(X_scaled) * 0.8)
        X_train, X_test = X_scaled[:split_idx], X_scaled[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        # Improved XGBoost model with better hyperparameters
        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            n_estimators=200,
            max_depth=8,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,
            reg_lambda=0.1,
            random_state=42,
            n_jobs=-1
        )
        
        model.fit(X_train, y_train)

        # Evaluate model
        preds = model.predict(X_test)
        rmse = np.sqrt(np.mean((preds - y_test) ** 2))
        mae = np.mean(np.abs(preds - y_test))
        mape = np.mean(np.abs((y_test - preds) / y_test)) * 100
        
        print(f"[{symbol.upper()}] RMSE: {rmse:.4f}, MAE: {mae:.4f}, MAPE: {mape:.2f}%")
        
        # Calculate additional metrics for prediction
        last_price = float(pdf['close'].iloc[-1])
        price_change_pct = float((last_price - pdf['close'].iloc[-2]) / pdf['close'].iloc[-2] * 100)
        volume_change_pct = float((pdf['volume'].iloc[-1] - pdf['volume'].iloc[-2]) / pdf['volume'].iloc[-2] * 100)

        # Get latest technical indicators
        latest_row = pdf.iloc[-1]
        
        # Generate prediction for next day
        latest_features = X_scaled[-1:].reshape(1, -1)
        next_pred = float(model.predict(latest_features)[0])
        
        # Calculate prediction confidence based on multiple factors
        recent_volatility = float(latest_row["volatility_7d"])
        price_volatility_ratio = recent_volatility / last_price if last_price > 0 else 0
        
        # Calculate additional confidence factors
        rsi = float(latest_row["rsi"])
        bb_position = float(latest_row["bb_position"])
        
        # RSI-based confidence (extreme RSI = lower confidence)
        rsi_confidence = 1.0 - abs(rsi - 50) / 50  # Closer to 50 = higher confidence
        
        # Bollinger Band position confidence (middle = higher confidence)
        bb_confidence = 1.0 - abs(bb_position - 0.5) * 2  # Closer to 0.5 = higher confidence
        
        # Overall confidence score (0-1)
        overall_confidence = (rsi_confidence + bb_confidence + (1 - min(price_volatility_ratio * 10, 1))) / 3
        
        # Determine confidence level
        if overall_confidence > 0.7:
            confidence_level = "high"
        elif overall_confidence > 0.4:
            confidence_level = "medium"
        else:
            confidence_level = "low"
        
        print(f"[{symbol.upper()}] Confidence factors - Volatility: {price_volatility_ratio:.2%}, RSI: {rsi:.1f}, BB: {bb_position:.2f}, Overall: {overall_confidence:.2f} ({confidence_level})")
        
        # Adjust prediction to be more conservative if volatility is high
        if price_volatility_ratio > 0.05:  # If volatility > 5%
            # Use moving average as baseline instead of raw prediction
            ma_7d = float(latest_row["ma_7d"])
            ma_14d = float(latest_row["ma_14d"])
            
            # Weighted average: 70% moving average, 30% model prediction
            conservative_pred = (ma_7d * 0.4 + ma_14d * 0.3 + next_pred * 0.3)
            next_pred = conservative_pred
            
            print(f"[{symbol.upper()}] High volatility detected ({price_volatility_ratio:.2%}), using conservative prediction")
        
        # Ensure prediction is within reasonable bounds (not more than 50% change)
        max_change = 0.5  # 50% max change
        min_pred = last_price * (1 - max_change)
        max_pred = last_price * (1 + max_change)
        next_pred = max(min_pred, min(next_pred, max_pred))

        prediction_result = {
            "symbol": symbol,
            "predicted_price": next_pred,
            "prediction_type": "1-day ahead forecast",
            "prediction_confidence": confidence_level,
            "confidence_score": float(overall_confidence),
            "last_price": last_price,
            "prediction_change_pct": float((next_pred - last_price) / last_price * 100),
            "volatility_ratio": float(price_volatility_ratio),
            "open": float(latest_row["open"]),
            "high": float(latest_row["high"]),
            "low": float(latest_row["low"]),
            "volume": float(latest_row["volume"]),
            
            # Moving averages (daily)
            "ma_7d": float(latest_row["ma_7d"]),
            "ma_14d": float(latest_row["ma_14d"]),
            "ma_30d": float(latest_row["ma_30d"]),
            "ma_90d": float(latest_row["ma_90d"]),
            "ma_200d": float(latest_row["ma_200d"]),
            
            # Technical indicators
            "rsi": float(latest_row["rsi"]),
            "macd": float(latest_row["macd"]),
            "macd_signal": float(latest_row["macd_signal"]),
            "bb_position": float(latest_row["bb_position"]),
            "bb_width": float(latest_row["bb_width"]),
            
            # Volatility
            "volatility_7d": float(latest_row["volatility_7d"]),
            "volatility_14d": float(latest_row["volatility_14d"]),
            "volatility_30d": float(latest_row["volatility_30d"]),
            
            # Volume indicators
            "volume_ratio_7d": float(latest_row["volume_ratio_7d"]),
            "volume_ratio_14d": float(latest_row["volume_ratio_14d"]),
            
            # Price ratios and momentum
            "high_low_ratio": float(latest_row["high_low_ratio"]),
            "close_open_ratio": float(latest_row["close_open_ratio"]),
            "price_ma_200_ratio": float(latest_row["price_ma_200_ratio"]),
            "momentum_5d": float(latest_row["momentum_5d"]),
            "momentum_10d": float(latest_row["momentum_10d"]),
            "momentum_20d": float(latest_row["momentum_20d"]),
            
            # Support/Resistance
            "support_distance": float(latest_row["support_distance"]),
            "resistance_distance": float(latest_row["resistance_distance"]),
            
            # Price changes
            "price_change_pct": price_change_pct,
            "volume_change_pct": volume_change_pct,
            
            # Model performance
            "rmse": float(rmse),
            "mae": float(mae),
            "mape": float(mape),
            
            # Data quality
            "total_data_points": len(pdf),
            "training_data_points": len(X_train),
            "test_data_points": len(X_test)
        }

        local_pred_path = f"/opt/spark/work-dir/models/xgboost_{symbol}_prediction.json"
        
        with open(local_pred_path, "w") as f:
            json.dump(prediction_result, f, indent=2)

        print(f"[{symbol.upper()}] Model, scaler & prediction saved to local")

        hdfs_pred_path = f"hdfs://hdfs-namenode:8020/models/xgboost_{symbol}_prediction.json"

        os.system(f"hadoop fs -put -f {local_pred_path} {hdfs_pred_path}")

        print(f"[{symbol.upper()}] Uploaded to HDFS successfully")

    except Exception as e:
        print(f"[ERROR] {symbol.upper()}: {e}")
        import traceback
        print(f"[ERROR] {symbol.upper()}: Full traceback:")
        traceback.print_exc()

spark.stop()