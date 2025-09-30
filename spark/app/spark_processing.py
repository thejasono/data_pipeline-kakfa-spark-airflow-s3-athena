import logging
import os
from typing import Optional
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
    # Keep zip as STRING (MD5→int may exceed 64-bit).
)

# Basic logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(name)s:%(funcName)s:%(levelname)s:%(message)s"
)
logger = logging.getLogger("spark_structured_streaming")


def _normalize_s3_endpoint(raw_endpoint: str) -> tuple[str, Optional[bool]]:
    """Normalize S3 endpoint string → (host[:port], ssl_enabled?)."""
    if raw_endpoint is None:
        raise ValueError("S3 endpoint cannot be None")

    candidate = raw_endpoint.strip()
    if not candidate:
        raise ValueError("S3 endpoint is empty")

    ssl_enabled = None

    # If a scheme is present, parse and validate (no path/query allowed).
    if "://" in candidate:
        parsed = urlparse(candidate, allow_fragments=False)
        host = parsed.netloc or parsed.path
        if not host:
            raise ValueError("S3 endpoint is missing a host")
        if parsed.path:
            raise ValueError("S3 endpoint must not include path segments")
        if parsed.query or parsed.params:
            raise ValueError("S3 endpoint must not include query/params")
        ssl_enabled = parsed.scheme.lower() != "http"
    else:
        host = candidate

    if "/" in host:
        raise ValueError("S3 endpoint must be host[:port] only")

    if not host:
        raise ValueError("S3 endpoint resolved to an empty host")

    return host, ssl_enabled


def initialize_spark_session(
    app_name: str,
    s3_access_key: str = None,
    s3_secret_key: str = None,
    s3_endpoint: str = None,
    s3_region: str = "us-east-1",
    s3_endpoint_ssl: Optional[bool] = None,
) -> SparkSession:
    """Create a SparkSession configured for Kafka + S3A (AWS or MinIO)."""
    try:
        builder = SparkSession.builder.appName(app_name)

        # Credentials provider: explicit keys → Simple; else → Default chain.
        if s3_access_key and s3_secret_key:
            builder = builder.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        else:
            builder = builder.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )

        # Endpoint (MinIO/self-hosted). For AWS, omit.
        ssl_enabled = True
        if s3_endpoint:
            endpoint_for_conf = s3_endpoint
            inferred_ssl = None
            if any(token in s3_endpoint for token in ("://", "/")):
                endpoint_for_conf, inferred_ssl = _normalize_s3_endpoint(s3_endpoint)
            if s3_endpoint_ssl is not None:
                ssl_enabled = s3_endpoint_ssl
            elif inferred_ssl is not None:
                ssl_enabled = inferred_ssl

            builder = (
                builder
                .config("spark.hadoop.fs.s3a.endpoint", endpoint_for_conf)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")  # Needed for most MinIO.
            )

        # S3A FS + TLS + region
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if ssl_enabled else "false")
            .config("spark.hadoop.fs.s3a.region", s3_region)
        )

        # Explicit keys only when using Simple provider (dev/MinIO).
        if s3_access_key and s3_secret_key:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
                .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
            )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session initialized successfully")
        return spark

    except Exception:
        logger.exception("Spark session initialization failed")
        return None


def get_streaming_dataframe(spark: SparkSession, brokers: str, topic: str):
    """Define a Kafka streaming DataFrame (unbounded table)."""
    try:
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        logger.info("Streaming DataFrame (Kafka source) defined successfully")
        return df
    except Exception:
        logger.exception("Failed to define Kafka streaming DataFrame")
        return None


def transform_streaming_data(df):
    """Cast Kafka value→STRING, parse JSON to typed columns."""
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("nation", StringType(), True),
        StructField("zip", StringType(), True),       # keep as STRING
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("email", StringType(), True),
    ])

    transformed_df = (
        df.selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), schema).alias("data"))
          .select("data.*")
    )
    return transformed_df


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
    """End-to-end: Spark init → Kafka read → JSON parse → Parquet sink."""
    app_name = "spark_streaming"

    # S3/MinIO auth + endpoint (prefer roles on AWS; explicit on MinIO/dev).
    s3_access_key = os.environ.get("S3_ACCESS_KEY")
    s3_secret_key = os.environ.get("S3_SECRET_KEY")
    raw_s3_endpoint = os.environ.get("S3_ENDPOINT") or None
    if raw_s3_endpoint is not None:
        logger.info("Observed raw S3_ENDPOINT: %r", raw_s3_endpoint)

    # Validate endpoint (no paths/trailing slash).
    if raw_s3_endpoint:
        endpoint_candidate = raw_s3_endpoint.strip()
        parsed_endpoint = None
        if "://" in endpoint_candidate:
            parsed_endpoint = urlparse(endpoint_candidate, allow_fragments=False)
            extra_path = parsed_endpoint.path or ""
            if extra_path:
                logger.error("Invalid S3_ENDPOINT '%s': remove path segment '%s'.", raw_s3_endpoint, extra_path)
                return
        host_hint = (parsed_endpoint.netloc or parsed_endpoint.path) if parsed_endpoint else endpoint_candidate
        if host_hint.endswith("/") or "/" in host_hint:
            logger.error("Invalid S3_ENDPOINT '%s': omit slashes/paths.", raw_s3_endpoint)
            return

    # Normalize endpoint for Spark config.
    normalized_endpoint = None
    endpoint_ssl = None
    if raw_s3_endpoint:
        try:
            normalized_endpoint, endpoint_ssl = _normalize_s3_endpoint(raw_s3_endpoint)
            if normalized_endpoint != raw_s3_endpoint:
                logger.info("Normalized S3 endpoint from %s to %s", raw_s3_endpoint, normalized_endpoint)
        except ValueError as exc:
            logger.error("Invalid S3_ENDPOINT '%s': %s", raw_s3_endpoint, exc)
            return
    s3_endpoint = normalized_endpoint
    s3_region = os.environ.get("S3_REGION", "us-east-1")

    # Kafka source
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.environ.get("KAFKA_TOPIC", "names_topic")

    # Output paths (S3A URIs)
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        logger.error("S3_BUCKET is required.")
        return

    output_prefix = os.environ.get("S3_OUTPUT_PREFIX", "names").strip("/")
    checkpoint_prefix = (os.environ.get("S3_CHECKPOINT_PREFIX") or f"checkpoints/{output_prefix}").strip("/")
    path = f"s3a://{bucket}/{output_prefix}/"
    checkpoint_location = f"s3a://{bucket}/{checkpoint_prefix}/"

    logger.info(
        "Target: bucket=%s data_prefix=%s checkpoint_prefix=%s endpoint=%s region=%s ssl=%s",
        bucket, output_prefix, checkpoint_prefix, s3_endpoint or "<aws-default>", s3_region,
        "auto" if endpoint_ssl is None else ("enabled" if endpoint_ssl else "disabled"),
    )

    # Stage 1: Spark
    logger.info("Stage 1: initializing Spark session...")
    spark = initialize_spark_session(
        app_name=app_name,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_endpoint_ssl=endpoint_ssl,
    )
    if spark is None:
        logger.error("Aborting: Spark session failed to initialize.")
        return

    try:
        # Stage 2: Kafka source
        logger.info("Stage 2: creating Kafka streaming DataFrame...")
        df = get_streaming_dataframe(spark, brokers, topic)
        if df is None:
            logger.error("Aborting: failed to create Kafka streaming DataFrame.")
            return

        # Stage 3: Transform
        logger.info("Stage 3: applying schema and parsing JSON...")
        transformed_df = transform_streaming_data(df)

        # Stage 4: Sink
        logger.info("Stage 4: starting Parquet sink to S3A with checkpointing...")
        initiate_streaming_to_bucket(transformed_df, path, checkpoint_location)

    finally:
        logger.info("Stopping Spark session...")
        spark.stop()


if __name__ == "__main__":
    main()

# Logging: stage boundaries, failures, and sink start are logged to aid debugging.
