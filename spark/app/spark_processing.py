import logging
import os
from typing import Optional  # keep once

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(name)s:%(funcName)s:%(levelname)s:%(message)s"
)
logger = logging.getLogger("spark_structured_streaming")


def initialize_spark_session_with_keys(app_name: str,
                                       access_key: str,
                                       secret_key: str) -> SparkSession:
    try:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception:
        logger.exception("Spark session initialization failed")
        raise


def get_streaming_dataframe(spark: SparkSession, brokers: str, topic: str) -> SparkSession:
    """Define a Kafka streaming DataFrame (unbounded table)."""
    # Prefer fail-fast: let caller handle exceptions.
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def transform_streaming_data(df):
    """Cast Kafka value→STRING, parse JSON to typed columns."""
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("nation", StringType(), True),
        StructField("zip", StringType(), True),  # keep as STRING
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("email", StringType(), True),
    ])
    return (
        df.selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), schema).alias("data"))
          .select("data.*")
    )


def initiate_streaming_to_bucket(df, path: str, checkpoint_location: str):
    """Start Parquet streaming sink with checkpointing and block."""
    logger.info("Starting streaming sink (Parquet)...")
    query = (
        df.writeStream
          .format("parquet")
          .outputMode("append")
          .option("path", path)
          .option("checkpointLocation", checkpoint_location)
          .start()
    )
    query.awaitTermination()


def main():
    """End-to-end: Spark init → Kafka read → JSON parse → Parquet sink (AWS S3)."""
    app_name = "spark_streaming"

    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.environ.get("KAFKA_TOPIC", "names_topic")

    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET is required")

    output_prefix = os.environ.get("S3_OUTPUT_PREFIX", "names").strip("/")
    checkpoint_prefix = (os.environ.get("S3_CHECKPOINT_PREFIX") or f"checkpoints/{output_prefix}").strip("/")
    path = f"s3a://{bucket}/{output_prefix}"
    checkpoint_location = f"s3a://{bucket}/{checkpoint_prefix}"

    s3_region = os.environ.get("S3_REGION", "us-east-1")

    s3_access_key = os.environ.get("S3_ACCESS_KEY")
    s3_secret_key = os.environ.get("S3_SECRET_KEY")

    if s3_access_key and s3_secret_key:
        spark = initialize_spark_session_with_keys(app_name, s3_access_key, s3_secret_key)
    else:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.region", s3_region)  # optional
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

    try:
        df = get_streaming_dataframe(spark, brokers, topic)
        transformed_df = transform_streaming_data(df)
        initiate_streaming_to_bucket(transformed_df, path, checkpoint_location)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
