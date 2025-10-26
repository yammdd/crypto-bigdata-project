#!/usr/bin/env python3
"""
Troubleshooting script to debug MongoDB update issues
"""

import os
import json
from pymongo import MongoClient
from datetime import datetime

def check_prediction_files():
    """Check if prediction files exist and their content"""
    print("=== Prediction Files Check ===")
    
    symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt"]
    base_dir = "/opt/spark/work-dir/models"
    
    for symbol in symbols:
        file_path = os.path.join(base_dir, f"xgboost_{symbol}_prediction.json")
        print(f"\n--- {symbol.upper()} ---")
        
        if os.path.exists(file_path):
            print(f"   ✅ File exists: {file_path}")
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                
                print(f"   📊 Predicted Price: ${data.get('predicted_price', 'N/A'):,.2f}")
                print(f"   📈 Last Price: ${data.get('last_price', 'N/A'):,.2f}")
                print(f"   📅 File modified: {datetime.fromtimestamp(os.path.getmtime(file_path))}")
                
            except Exception as e:
                print(f"   ❌ Error reading file: {e}")
        else:
            print(f"   ❌ File not found: {file_path}")

def check_mongodb_connection():
    """Check MongoDB connection and current data"""
    print("\n=== MongoDB Connection Check ===")
    
    try:
        mongo_uri = "mongodb://mongodb:27017"
        client = MongoClient(mongo_uri)
        db = client["crypto_batch"]
        collection = db["predictions"]
        
        print("   ✅ MongoDB connection successful")
        
        # Check current data in MongoDB
        symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt"]
        
        for symbol in symbols:
            print(f"\n--- {symbol.upper()} in MongoDB ---")
            doc = collection.find_one({"_id": symbol})
            
            if doc:
                print(f"   ✅ Document found")
                print(f"   📊 Predicted Price: ${doc.get('predicted_price', 'N/A'):,.2f}")
                print(f"   📈 Last Price: ${doc.get('last_price', 'N/A'):,.2f}")
                print(f"   📅 Document ID: {doc.get('_id', 'N/A')}")
            else:
                print(f"   ❌ Document not found")
        
        client.close()
        
    except Exception as e:
        print(f"   ❌ MongoDB connection failed: {e}")

def test_mongodb_update():
    """Test updating MongoDB with current prediction files"""
    print("\n=== MongoDB Update Test ===")
    
    try:
        mongo_uri = "mongodb://mongodb:27017"
        client = MongoClient(mongo_uri)
        db = client["crypto_batch"]
        collection = db["predictions"]
        
        symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt"]
        base_dir = "/opt/spark/work-dir/models"
        
        for symbol in symbols:
            print(f"\n--- Updating {symbol.upper()} ---")
            file_path = os.path.join(base_dir, f"xgboost_{symbol}_prediction.json")
            
            if not os.path.exists(file_path):
                print(f"   ⚠️  File not found: {file_path}")
                continue
            
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                    data["_id"] = symbol
                    data["updated_at"] = datetime.now().isoformat()
                
                # Update MongoDB
                result = collection.replace_one({"_id": symbol}, data, upsert=True)
                
                if result.acknowledged:
                    print(f"   ✅ Successfully updated MongoDB")
                    print(f"   📊 Predicted Price: ${data.get('predicted_price', 'N/A'):,.2f}")
                    print(f"   📈 Last Price: ${data.get('last_price', 'N/A'):,.2f}")
                else:
                    print(f"   ❌ MongoDB update failed")
                    
            except Exception as e:
                print(f"   ❌ Error updating {symbol}: {e}")
        
        client.close()
        
    except Exception as e:
        print(f"   ❌ MongoDB update test failed: {e}")

def check_streamlit_cache():
    """Check if Streamlit is caching data"""
    print("\n=== Streamlit Cache Check ===")
    print("   💡 Streamlit caches data by default")
    print("   💡 Try refreshing the browser page (Ctrl+F5)")
    print("   💡 Or restart the Streamlit container:")
    print("      docker compose restart streamlit")

def main():
    print("🔧 MongoDB Update Troubleshooting Tool")
    print(f"📅 Check Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    check_prediction_files()
    check_mongodb_connection()
    test_mongodb_update()
    check_streamlit_cache()
    
    print("\n=== Troubleshooting Steps ===")
    print("1. If files exist but MongoDB is empty: Check MongoDB connection")
    print("2. If MongoDB has old data: Run write_to_mongo.py again")
    print("3. If Streamlit shows old data: Refresh browser or restart container")
    print("4. If still issues: Check container networking")

if __name__ == "__main__":
    main()
