from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, max, min, collect_set, round
from pyspark.sql.types import StructType, StringType, FloatType, LongType
from streaming.utils.hbase_writer import write_to_hbase
from streaming.utils.market_analyzer import get_latest_crypto_news, get_market_analysis

def process_batch_and_analyze(df, epoch_id):
    if df.isEmpty():
        print(f"Batch {epoch_id} rỗng, bỏ qua.")
        return

    print(f"\n--- Xử lý Batch ID: {epoch_id} ---")
    df.cache()

    # Tóm tắt dữ liệu thời gian thực từ DataFrame của batch
    print("1. Đang tóm tắt dữ liệu thời gian thực...")
    symbols_in_batch = df.select(collect_set("symbol")).first()[0]
    summary_stats = df.select(
        round(avg("price"), 2).alias("avg_price"),
        round(max("price"), 2).alias("max_price"),
        round(min("price"), 2).alias("min_price")
    ).first()

    real_time_summary = (
        f"Symbols processed: {', '.join(symbols_in_batch)}\n"
        f"Average price across symbols: ${summary_stats['avg_price']}\n"
        f"Price range: ${summary_stats['min_price']} to ${summary_stats['max_price']}"
    )
    print(real_time_summary)

    # Lấy tin tức và thực hiện phân tích
    print("2. Đang lấy tin tức mới nhất...")
    latest_news = get_latest_crypto_news()
    news_summary = "\n".join(latest_news)
    
    print("3. Đang gửi dữ liệu đến Gemini để phân tích...")
    analysis_result = get_market_analysis(news_summary, real_time_summary)

    print("\n" + "="*25 + " PHÂN TÍCH THỊ TRƯỜNG " + "="*25)
    print(analysis_result)
    print("="*72 + "\n")

    # Ghi dữ liệu gốc của batch vào HBase (giữ nguyên chức năng cũ)
    print("4. Đang ghi dữ liệu batch vào HBase...")
    write_to_hbase(df, epoch_id)
    print(f"--- Hoàn thành xử lý Batch ID: {epoch_id} ---")
    
    df.unpersist()


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
    .foreachBatch(process_batch_and_analyze) \
    .start()

query.awaitTermination()