import logging
import os
from typing import Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(name)s:%(funcName)s:%(levelname)s:%(message)s",
)
logger = logging.getLogger("spark_structured_streaming")


def _configure_s3_credentials(
    builder: SparkSession.Builder,
    access_key: str,
    secret_key: str,
    session_token: Optional[str] = None,
) -> SparkSession.Builder:
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
    """Split an S3 endpoint into host[:port] and SSL preference.

    Examples:
    - 's3.eu-west-2.amazonaws.com'     -> ('s3.eu-west-2.amazonaws.com', None)
    - 'http://minio:9000'              -> ('minio:9000', False)
    - 'https://s3.eu-west-2.amazonaws.com' -> ('s3.eu-west-2.amazonaws.com', True)
    """
    if raw is None:
        raise ValueError("S3 endpoint is required when normalization is requested")

    value = raw.strip()
    if not value:
        raise ValueError("S3 endpoint cannot be empty or whitespace")

    # No scheme → treat as host[:port]
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


def initialize_spark_session(
    app_name: str,
    *,
    region: str,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    session_token: Optional[str] = None,
    endpoint: Optional[str] = None,
    path_style: Optional[bool] = None,
    ssl_enabled: Optional[bool] = None,
) -> SparkSession:
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

        # Default path-style for non-AWS endpoints (e.g. MinIO)
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
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def transform_streaming_data(df: DataFrame) -> DataFrame:
    """Cast Kafka value→STRING, parse JSON to typed columns."""
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("nation", StringType(), True),
            StructField("zip", StringType(), True),  # keep as STRING
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("email", StringType(), True),
        ]
    )
    return (
        df.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )


def s3_healthcheck_write(spark: SparkSession, path: str) -> None:
    """Perform a small test write to S3A to confirm credentials/endpoint work.

    Writes a single-row JSON file under:
    {path}/_healthcheck/{timestamp}/part-...json
    """
    # Example: 2025-11-28T19-52-30Z
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    health_path = f"{path.rstrip('/')}/_healthcheck/{ts}"

    logger.info("Running S3 healthcheck write to %s", health_path)

    try:
        df = spark.createDataFrame([(ts,)], ["healthcheck_timestamp"])
        (
            df.write
            .mode("append")
            .format("json")
            .save(health_path)
        )
        logger.info("S3 healthcheck write succeeded at %s", health_path)
    except Exception:
        logger.exception("S3 healthcheck write FAILED at %s", health_path)
        raise


def initiate_streaming_to_bucket(df: DataFrame, path: str, checkpoint_location: str) -> None:
    """Start JSON streaming sink with checkpointing and per-batch logging."""
    logger.info(
        "Starting streaming sink (JSON) to path=%s with checkpoint=%s",
        path,
        checkpoint_location,
    )

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        ts = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Batch %s: about to write %s record(s) to %s at %s",
            batch_id,
            count,
            path,
            ts,
        )

        (
            batch_df.write
            .mode("append")
            .format("json")
            .save(path)
        )

        logger.info(
            "Batch %s: successfully wrote %s record(s) to %s at %s",
            batch_id,
            count,
            path,
            ts,
        )

    query = (
        df.writeStream
        .outputMode("append")
        .foreachBatch(_write_batch)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    logger.info("Streaming query started; awaiting termination.")
    query.awaitTermination()


def main() -> None:
    """End-to-end: Spark init → Kafka read → JSON parse → JSON sink (AWS S3)."""
    app_name = "spark_streaming"

    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.environ.get("KAFKA_TOPIC", "names_topic")

    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET is required")

    output_prefix = os.environ.get("S3_OUTPUT_PREFIX", "names").strip("/")
    checkpoint_prefix = (
        os.environ.get("S3_CHECKPOINT_PREFIX") or f"checkpoints/{output_prefix}"
    ).strip("/")

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

    logger.info(
        "Spark streaming config: app_name=%s brokers=%s topic=%s bucket=%s "
        "output_prefix=%s checkpoint_prefix=%s region=%s endpoint=%s "
        "path_style=%s ssl_enabled=%s",
        app_name,
        brokers,
        topic,
        bucket,
        output_prefix,
        checkpoint_prefix,
        s3_region,
        endpoint_host,
        path_style,
        ssl_pref,
    )

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
        # Optional explicit S3 connectivity check (can be removed if undesired)
        s3_healthcheck_write(spark, path)

        df = get_streaming_dataframe(spark, brokers, topic)
        transformed_df = transform_streaming_data(df)
        initiate_streaming_to_bucket(transformed_df, path, checkpoint_location)
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    main()
