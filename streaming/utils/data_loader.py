from pyspark.sql import SparkSession

def load_crypto_df(spark: SparkSession, symbol: str):
    path = f"hdfs://hdfs-namenode:8020/crypto/yahoo/{symbol}"
    df = spark.read.parquet(path)
    return df
