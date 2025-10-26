#!/usr/bin/env python3
"""
Test script to validate the improved crypto prediction system
"""

import requests
import json
from datetime import datetime

def get_current_prices():
    """Get current crypto prices from CoinGecko API"""
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
        # Get current prices from CoinGecko
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

def test_prediction_accuracy():
    """Test the accuracy of predictions vs current market prices"""
    print("=== Crypto Prediction Accuracy Test ===")
    print(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Get current market prices
    current_prices = get_current_prices()
    
    if not current_prices:
        print("❌ Could not fetch current market prices")
        return
    
    # Read prediction results (assuming they exist)
    try:
        with open("/opt/spark/work-dir/models/xgboost_btcusdt_prediction.json", "r") as f:
            btc_pred = json.load(f)
        
        print("📊 BTC Prediction Analysis:")
        print(f"   Predicted Price: ${btc_pred['predicted_price']:,.2f}")
        print(f"   Last Price: ${btc_pred['last_price']:,.2f}")
        print(f"   Current Market Price: ${current_prices.get('btcusdt', 'N/A'):,.2f}")
        
        if 'btcusdt' in current_prices:
            current_price = current_prices['btcusdt']
            predicted_price = btc_pred['predicted_price']
            last_price = btc_pred['last_price']
            
            pred_error = abs(predicted_price - current_price) / current_price * 100
            last_error = abs(last_price - current_price) / current_price * 100
            
            print(f"   Prediction Error: {pred_error:.2f}%")
            print(f"   Last Price Error: {last_error:.2f}%")
            
            if pred_error < last_error:
                print("   ✅ Prediction is closer to current price than last price!")
            else:
                print("   ⚠️  Prediction is further from current price than last price")
        
        print()
        
        # Show model performance metrics
        print("📈 Model Performance:")
        print(f"   RMSE: {btc_pred['rmse']:.4f}")
        print(f"   MAE: {btc_pred['mae']:.4f}")
        print(f"   MAPE: {btc_pred['mape']:.2f}%")
        print(f"   Data Points: {btc_pred['total_data_points']}")
        print(f"   Training Points: {btc_pred['training_data_points']}")
        print()
        
        # Show technical indicators
        print("🔍 Technical Indicators:")
        print(f"   RSI: {btc_pred['rsi']:.2f}")
        print(f"   MACD: {btc_pred['macd']:.4f}")
        print(f"   BB Position: {btc_pred['bb_position']:.2f}")
        print(f"   Price vs MA200: {btc_pred['price_ma_200_ratio']:.2f}")
        print(f"   5-day Momentum: {btc_pred['momentum_5d']:.2%}")
        print(f"   10-day Momentum: {btc_pred['momentum_10d']:.2%}")
        print(f"   20-day Momentum: {btc_pred['momentum_20d']:.2%}")
        
    except FileNotFoundError:
        print("❌ Prediction file not found. Run train_all_xgboost_models.py first.")
    except Exception as e:
        print(f"❌ Error reading prediction file: {e}")

if __name__ == "__main__":
    test_prediction_accuracy()
