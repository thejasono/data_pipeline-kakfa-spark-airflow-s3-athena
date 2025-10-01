import logging
import os
from typing import Optional, Tuple
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(name)s:%(funcName)s:%(levelname)s:%(message)s"
)
logger = logging.getLogger("spark_structured_streaming")


def _configure_s3_credentials(builder: SparkSession.Builder,
                              access_key: str,
                              secret_key: str,
                              session_token: Optional[str] = None) -> SparkSession.Builder:
    """Apply static AWS credentials to a Spark builder."""
    provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    if session_token:
        provider = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"

    builder = (
        builder
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    )

    if session_token:
        builder = builder.config("spark.hadoop.fs.s3a.session.token", session_token)

    return builder


def _normalize_s3_endpoint(raw: Optional[str]) -> Tuple[str, Optional[bool]]:
    """Split an S3 endpoint into host[:port] and SSL preference."""
    if raw is None:
        raise ValueError("S3 endpoint is required when normalization is requested")

    value = raw.strip()
    if not value:
        raise ValueError("S3 endpoint cannot be empty or whitespace")

    if "://" not in value:
        if any(ch in value for ch in "/?#"):
            raise ValueError("S3 endpoint without scheme must not contain paths or queries")
        return value, None

    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"Unsupported S3 endpoint scheme: {parsed.scheme}")
    if not parsed.hostname:
        raise ValueError("S3 endpoint must include a hostname")
    if parsed.path not in {"", None}:
        raise ValueError("S3 endpoint must not include a path component")
    if parsed.params or parsed.query or parsed.fragment:
        raise ValueError("S3 endpoint must not include params, query or fragment")

    host = parsed.netloc
    ssl_enabled = parsed.scheme == "https"
    return host, ssl_enabled


def initialize_spark_session(app_name: str,
                             *,
                             region: str,
                             access_key: Optional[str] = None,
                             secret_key: Optional[str] = None,
                             session_token: Optional[str] = None,
                             endpoint: Optional[str] = None,
                             path_style: Optional[bool] = None,
                             ssl_enabled: Optional[bool] = None) -> SparkSession:
    """Create a SparkSession with optional static AWS credentials."""
    builder = SparkSession.builder.appName(app_name)

    if access_key and secret_key:
        builder = _configure_s3_credentials(builder, access_key, secret_key, session_token)
    else:
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )

    builder = builder.config("spark.hadoop.fs.s3a.region", region)

    if endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)

        if path_style is None:
            path_style = "amazonaws.com" not in endpoint.lower()

    if path_style is not None:
        builder = builder.config(
            "spark.hadoop.fs.s3a.path.style.access",
            str(bool(path_style)).lower(),
        )

    if ssl_enabled is not None:
        builder = builder.config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "true" if ssl_enabled else "false",
        )

    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception:
        logger.exception("Spark session initialization failed")
        raise


def get_streaming_dataframe(spark: SparkSession, brokers: str, topic: str) -> DataFrame:
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
    """Start JSON streaming sink with checkpointing and block."""
    logger.info("Starting streaming sink (JSON)...")
    query = (
        df.writeStream
          .format("json")
          .outputMode("append")
          .option("path", path)
          .option("checkpointLocation", checkpoint_location)
          .start()
    )
    query.awaitTermination()


def main():
    """End-to-end: Spark init → Kafka read → JSON parse → JSON sink (AWS S3)."""
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

    s3_region = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION") or "eu-west-2"

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN")


    endpoint_host = None
    ssl_pref = None
    endpoint_raw = os.environ.get("S3_ENDPOINT")
    if endpoint_raw:
        endpoint_host, ssl_pref = _normalize_s3_endpoint(endpoint_raw)

    path_style_env = os.environ.get("S3_PATH_STYLE_ACCESS")
    path_style = None
    if path_style_env is not None:
        path_style = path_style_env.lower() in {"1", "true", "yes", "on"}

    spark = initialize_spark_session(
        app_name,
        region=s3_region,
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
        endpoint=endpoint_host,
        path_style=path_style,
        ssl_enabled=ssl_pref,
    )

    try:
        df = get_streaming_dataframe(spark, brokers, topic)
        transformed_df = transform_streaming_data(df)
        initiate_streaming_to_bucket(transformed_df, path, checkpoint_location)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
