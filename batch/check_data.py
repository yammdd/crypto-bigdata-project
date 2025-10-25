#!/usr/bin/env python3
"""
Script to check if data was fetched correctly from HDFS
"""

from pyspark.sql import SparkSession

def check_data_quality():
    """Check the quality and structure of data in HDFS"""
    print("=== Data Quality Check ===")
    
    spark = SparkSession.builder.appName("DataQualityCheck").getOrCreate()
    
    symbols = [
        "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
        "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"
    ]
    
    for symbol in symbols:
        print(f"\n--- Checking {symbol.upper()} ---")
        try:
            # Read data from HDFS
            path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol}"
            df = spark.read.parquet(path)
            
            print(f"✅ Data found for {symbol.upper()}")
            print(f"   Schema:")
            df.printSchema()
            
            print(f"   Row count: {df.count()}")
            
            print(f"   Sample data:")
            df.show(5)
            
            # Check for missing values
            print(f"   Missing values per column:")
            for col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                print(f"     {col}: {null_count}")
            
            # Check date range
            if 'datetime' in df.columns:
                date_col = 'datetime'
            elif 'date' in df.columns:
                date_col = 'date'
            else:
                print(f"   ⚠️  No datetime/date column found!")
                continue
            
            min_date = df.select(date_col).rdd.min()[0]
            max_date = df.select(date_col).rdd.max()[0]
            print(f"   Date range: {min_date} to {max_date}")
            
            # Check for reasonable price values
            price_cols = ['open', 'high', 'low', 'close']
            for col in price_cols:
                if col in df.columns:
                    min_price = df.select(col).rdd.min()[0]
                    max_price = df.select(col).rdd.max()[0]
                    print(f"   {col}: ${min_price:.2f} - ${max_price:.2f}")
            
        except Exception as e:
            print(f"❌ Error reading {symbol.upper()}: {e}")
    
    spark.stop()
    print("\n=== Data Quality Check Complete ===")

if __name__ == "__main__":
    check_data_quality()
