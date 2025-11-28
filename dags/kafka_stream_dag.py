# -----------------------------------------------------------------------------
# File: dags/kafka_stream_dag.py
# Purpose: Run a short-lived Kafka streaming job on a schedule via Airflow.
# -----------------------------------------------------------------------------

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator  # for Option A (spark-submit inside Airflow)
from airflow.providers.docker.operators.docker import DockerOperator  # for Option B (Spark container)
from producer.kafka_streaming_service import ensure_topic, KAFKA_BOOTSTRAP, KAFKA_TOPIC
# -------------------------------------------------------------------------
# Callables used by tasks
# -------------------------------------------------------------------------


def _trigger_stream() -> None:
    """Import and run the Kafka streaming helper at task runtime.

    Importing inside the callable keeps the DAG importable even if optional
    runtime dependencies (e.g., ``confluent_kafka``) are missing when the
    scheduler parses the file.
    """
    from producer.kafka_streaming_service import initiate_stream
    initiate_stream()


def _check_kafka() -> None:
    """Ensure Kafka broker is reachable and the topic exists."""
    # This will no-op if the topic already exists
    ensure_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC, num_partitions=1, replication_factor=1)


def _check_bucket() -> None:
    """Ensure AWS S3 bucket exists; create it if missing."""
    import os
    import boto3
    from botocore.exceptions import ClientError

    bucket = "names-bucket"
    region = os.getenv("AWS_REGION", "eu-west-2")

    s3 = boto3.client("s3", region_name=region)

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        # AWS returns string codes ("404", "NoSuchBucket", etc.)
        if error_code in ("404", "NoSuchBucket"):
            if region == "us-east-1":
                s3.create_bucket(Bucket=bucket)
            else:
                s3.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": region},
                )
        else:
            # Any other error → fail the task
            raise

# -------------------------------------------------------------------------
# DAG default args and config
# -------------------------------------------------------------------------

LOCAL_TZ = pendulum.timezone("Europe/London")
DAG_START_DATE = pendulum.datetime(2024, 1, 1, 0, 0, tz=LOCAL_TZ)

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": DAG_START_DATE,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

# -------------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------------

with DAG(
    dag_id="name_stream_dag",
    description="Stream random names to Kafka and land them in S3 via Spark",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "streaming", "demo"],
) as dag:

    # 1) Kafka health check
    kafka_health_check = PythonOperator(
        task_id="kafka_health_check",
        python_callable=_check_kafka,
    )

    # 2) S3 / MinIO bucket health check
    bucket_health_check = PythonOperator(
        task_id="s3_bucket_health_check",
        python_callable=_check_bucket,
    )

    # 3) Kafka producer (existing logic)
    kafka_stream_task = PythonOperator(
        task_id="stream_to_kafka_task",
        python_callable=_trigger_stream,
    )

    # 4) Spark consumer: choose ONE of the following implementations

    # -----------------------------------------------------------------
    #  DockerOperator (Spark runs in its own container)
    # Uncomment this block *and* the import at the top, then delete the
    # BashOperator spark_stream_task above.
    # -----------------------------------------------------------------
    spark_stream_task = DockerOperator(
        task_id="spark_stream_to_s3",
        image="custom-spark",
        command=(
            "/opt/bitnami/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/app/spark_processing.py"
        ),
        network_mode="docker_streaming",
        env_file=[".env.aws"],    # reuse same credentials
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
            "KAFKA_TOPIC": "names_topic",
            "S3_BUCKET": "names-bucket",
            "S3_OUTPUT_PREFIX": "names",
            "S3_CHECKPOINT_PREFIX": "checkpoints/names",

            # Let Spark pick default AWS endpoint based on region/SDK config:
            "S3_REGION": "eu-west-2",
            # Drop these MinIO-specific settings:
            # "S3_ENDPOINT": "http://minio:9000",
            # "S3_PATH_STYLE_ACCESS": "true",
            # And do NOT override AWS creds here,
            # they come from .env.aws
        },
        auto_remove=True,
    )


    # 5) Wiring: health checks → producer → Spark consumer
    kafka_health_check >> bucket_health_check >> kafka_stream_task >> spark_stream_task
