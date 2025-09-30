# -----------------------------------------------------------------------------
# File: dags/kafka_stream_dag.py
# Purpose: Run a short-lived Kafka streaming job on a schedule via Airflow.
#
# Notes for Future-You:
# - This DAG is written for Airflow 2.x.
# - It expects your "kafka_streaming_service.py" module (with initiate_stream())
#   to be importable by the Airflow container. Easiest: place it inside /opt/airflow/dags.
#   Alternatively, bake it into a custom image or adjust PYTHONPATH.
# -----------------------------------------------------------------------------

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

def _trigger_stream() -> None:
    """Import and run the Kafka streaming helper at task runtime.

    Importing inside the callable keeps the DAG importable even if optional
    runtime dependencies (e.g., ``confluent_kafka``) are missing when the
    scheduler parses the file. Airflow can now list the DAG, and the task will
    surface a clear error at execution time if the dependency is still absent.
    """

    from producer.kafka_streaming_service import initiate_stream

    initiate_stream()
# Use a timezone-aware start_date (Airflow recommends pendulum).
LOCAL_TZ = pendulum.timezone("Europe/London")
DAG_START_DATE = pendulum.datetime(2024, 1, 1, 0, 0, tz=LOCAL_TZ)

# Default task arguments applied to all tasks in this DAG unless overridden.
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": DAG_START_DATE,     # Anchor time for the DAG’s schedule (see Q&A below)
    "retries": 1,                     # If the task fails, try once more
    "retry_delay": timedelta(seconds=5),
}

# -----------------------------------------------------------------------------
# DAG DEFINITION
# -----------------------------------------------------------------------------
# - dag_id: unique identifier in the Airflow UI
# - schedule: cron-like string. "0 1 * * *" = run at 01:00 every day
# - catchup: False means “don’t backfill old runs between start_date and now”
# - max_active_runs: only allow one active run at a time for this DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="name_stream_dag",
    description="Stream random names to Kafka topic (short-lived producer job)",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "streaming", "demo"],
) as dag:

    # -------------------------------------------------------------------------
    # TASK: Run the Python streaming function.
    # - python_callable: the Python function to execute inside the task process.
    # - If you need params (e.g., different topic/duration), pass via op_args/op_kwargs.
    # -------------------------------------------------------------------------
    kafka_stream_task = PythonOperator(
        task_id="stream_to_kafka_task",
        python_callable=_trigger_stream,     # imports dependency at execution time
        # Example of passing arguments:
        # op_kwargs={"topic": "names_topic", "duration": 120, "pause": 10}
    )

    # This DAG only has one task, so no dependencies to set (e.g., a >> b).


# =============================================================================
# Q&A — READ ME LATER (EXPLANATIONS)
# =============================================================================
# Q1) “Go over the syntax for a DAG”
#     A DAG file is regular Python that defines:
#       - Imports: airflow core + any operators you need.
#       - DEFAULT_ARGS: common task configs (owner, retries, start_date, etc.).
#       - A DAG context manager:
#           with DAG(dag_id=..., schedule=..., start_date=..., catchup=...) as dag:
#         Inside it you instantiate tasks (operators).
#       - Task dependencies if you have multiple tasks (a >> b >> c).
#
#     Key DAG parameters:
#       - dag_id:       Unique name shown in the UI.
#       - schedule:     When runs are *scheduled* (cron, e.g., "0 1 * * *" or presets like @daily).
#       - start_date:   The logical anchor time used by the scheduler to decide when the first run occurs.
#                       Airflow will only schedule runs whose execution_date ≥ start_date.
#                       Make it timezone-aware (pendulum) to avoid surprises.
#       - catchup:      Whether to backfill past runs between start_date and “now”.
#       - max_active_runs: Limit concurrent active runs of *this* DAG.
#
# Q2) “What does catchup mean?”
#     - catchup=True (default): Airflow *backfills* all missing scheduled intervals from start_date up to now.
#       Useful for ETL jobs that must process historical partitions.
#     - catchup=False: Only schedule *future* runs; do not backfill the past.
#       Useful for jobs where backfilling is unnecessary or would be wasteful.
#
# Q3) “What does python_callable mean?”
#     - For PythonOperator, python_callable is the actual Python function object Airflow will call
#       inside the task process when this task runs. It must be importable in the worker’s environment.
#       You can pass arguments via op_args / op_kwargs, and you’ll see logs in the Airflow UI.
#
# Q4) “Is this DAG a Python operator, and how does that differ from a Bash operator?”
#     - The DAG is just the container of tasks; the *task* uses PythonOperator here.
#     - PythonOperator: executes a Python function (your code runs in-process).
#       Good for calling libraries, APIs, Kafka clients, database code, etc.
#     - BashOperator: executes a shell command (bash -c "...").
#       Good for CLI tools, shell scripts, Spark-submit, dbt CLI, etc.
#     - You can mix multiple operator types in the same DAG.
#
# Q5) “How does it work under the hood?”
#     - The Scheduler parses this file, builds a graph of tasks, and (based on schedule)
#       decides when a run should exist (execution_date / data interval).
#     - For each run, task instances are queued and executed by the executor (LocalExecutor here),
#       which forks subprocesses to run tasks.
#     - When your task executes, Airflow imports kafka_streaming_service, calls initiate_stream(),
#       and captures logs/state in the metadata DB (Postgres) and UI.
#
# STREAMING NOTE:
#     Airflow is batch-oriented; tasks should be *finite*. Your initiate_stream() function should
#     run for a bounded duration and exit. Avoid infinite loops in Airflow tasks.
#     For truly continuous streaming services, run them as long-lived services (systemd/K8s/Compose),
#     and let Airflow orchestrate/control them (start/stop/health checks), rather than run them forever.
#
# IMPORT VISIBILITY:
#     If Airflow cannot import kafka_streaming_service, place that file inside the DAGs folder
#     (/opt/airflow/dags in the container) or build a custom image that installs it into PYTHONPATH.
#
# PASSING PARAMETERS (OPTIONAL):
#     You can parameterize the task without editing code:
#       kafka_stream_task = PythonOperator(
#           task_id="stream_to_kafka_task",
#           python_callable=initiate_stream,
#           op_kwargs={"topic": "names_topic", "duration": 120, "pause": 10},
#       )
#     Then define initiate_stream(topic: str = "names_topic", duration: int = 120, pause: int = 10) -> None.
#
# TASKFLOW API (OPTIONAL, CLEANER STYLE):
#     from airflow.decorators import dag, task
#     @dag(schedule="0 1 * * *", start_date=DAG_START_DATE, catchup=False)
#     def name_stream_dag():
#         @task
#         def stream():
#             initiate_stream()
#         stream()
#     dag = name_stream_dag()
# =============================================================================
