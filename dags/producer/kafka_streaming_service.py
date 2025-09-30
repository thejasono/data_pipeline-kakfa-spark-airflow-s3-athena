# dags/kafka_stream_dag.py
# Airflow 2.x DAG: runs a short-lived Kafka producer on a schedule.

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def _trigger_stream() -> None:
    """Import and run the streaming function at task runtime."""
    from producer.kafka_streaming_service import initiate_stream
    initiate_stream()


# Timezone-aware start date
LOCAL_TZ = pendulum.timezone("Europe/London")
DAG_START_DATE = pendulum.datetime(2024, 1, 1, 0, 0, tz=LOCAL_TZ)

# Default task args
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": DAG_START_DATE,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

# DAG: runs every 5 minutes; no backfill; only one active run at a time
with DAG(
    dag_id="name_stream_dag",
    description="Stream random names to Kafka (short-lived producer)",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "streaming", "demo"],
) as dag:

    # Single task: execute Python callable
    kafka_stream_task = PythonOperator(
        task_id="stream_to_kafka_task",
        python_callable=_trigger_stream,
        # op_kwargs={"topic": "names_topic", "duration": 120, "pause": 10},  # example
    )

    # No dependencies (only one task)
