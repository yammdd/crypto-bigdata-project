from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, LongType

# Tạo Spark session
spark = SparkSession.builder \
    .appName("CryptoPriceStream") \
    .getOrCreate()

# Schema JSON cho dữ liệu Kafka
schema = StructType() \
    .add("source", StringType()) \
    .add("category", StringType()) \
    .add("symbol", StringType()) \
    .add("price", FloatType()) \
    .add("price_change", FloatType()) \
    .add("price_change_pct", FloatType()) \
    .add("high_price", FloatType()) \
    .add("low_price", FloatType()) \
    .add("open_price", FloatType()) \
    .add("volume_token", FloatType()) \
    .add("volume_quote", FloatType()) \
    .add("best_bid_price", FloatType()) \
    .add("best_ask_price", FloatType()) \
    .add("timestamp", LongType())

# Đọc từ Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON từ value
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# In kết quả ra console (có thể thay bằng ghi HBase sau này)
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
