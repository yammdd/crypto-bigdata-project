from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, LongType
from streaming.utils.hbase_writer import write_to_hbase

spark = SparkSession.builder \
    .appName("CryptoPriceStream") \
    .getOrCreate()

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

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

query = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_hbase) \
    .start()

query.awaitTermination()