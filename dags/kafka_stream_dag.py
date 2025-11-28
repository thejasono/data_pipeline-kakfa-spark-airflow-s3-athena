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
    """Fail fast if Kafka broker or topic is not ready."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": "kafka:19092"})
    md = admin.list_topics(timeout=5)

    if "names_topic" not in md.topics:
        raise RuntimeError("Kafka topic 'names_topic' is missing")


def _check_bucket() -> None:
    """Ensure MinIO/S3 bucket exists; create it if missing."""
    from minio import Minio

    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="adminadmin",
        secure=False,
    )
    bucket = "names-bucket"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


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
         image="custom-spark",  # same image as spark-master / spark_streaming
         command=(
             "/opt/bitnami/spark/bin/spark-submit "
             "--master spark://spark-master:7077 "
             "/opt/spark/app/spark_processing.py"
         ),
         network_mode="docker_streaming",  # must see kafka + minio + spark-master
         environment={
             "KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
             "KAFKA_TOPIC": "names_topic",
             "S3_BUCKET": "names-bucket",
             "S3_OUTPUT_PREFIX": "names",
             "S3_CHECKPOINT_PREFIX": "checkpoints/names",
             "S3_REGION": "eu-west-2",
             "S3_ENDPOINT": "http://minio:9000",
             "S3_PATH_STYLE_ACCESS": "true",
             "AWS_ACCESS_KEY_ID": "admin",
             "AWS_SECRET_ACCESS_KEY": "adminadmin",
         },
         auto_remove=True,
     )

    # 5) Wiring: health checks → producer → Spark consumer
    kafka_health_check >> bucket_health_check >> kafka_stream_task >> spark_stream_task
