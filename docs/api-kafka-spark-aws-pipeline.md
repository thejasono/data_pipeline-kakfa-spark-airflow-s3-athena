An Event-Driven Data Pipeline Using APIs, Kafka, Spark, and AWS
==============================================================

Welcome back to the DataEngineering101 series! If this is your first stop, this collection breaks down foundational data engineering concepts one production-ready brick at a time. Previously, we built an end-to-end pipeline with Docker, Airflow, dbt, and Postgres. This time, we are shifting gears to the streaming world—designing a system that ingests live API data, moves it through Apache Kafka, processes it with Apache Spark, and lands the outputs in AWS for durable storage and downstream analytics.

Just like the last article, this one is intentionally thorough. Use the section headers to hop directly to the parts that matter most to you.

Workflow Overview
-----------------

Here’s the high-level journey our data will take:

* **Ingest:** Continuously poll an external API and publish the responses to a Kafka topic.
* **Buffer:** Use Kafka as the durable, replayable event log that decouples producers from consumers.
* **Process:** Run structured streaming jobs in Spark to clean, enrich, and aggregate the Kafka events.
* **Persist:** Write curated batch and streaming outputs into AWS S3 and catalog the results in AWS Glue/Athena.
* **Orchestrate:** Coordinate infrastructure and jobs with AWS-native services like Managed Streaming for Kafka (MSK), EMR, and Step Functions (or Airflow on AWS).

At a glance, that’s a lot of moving parts—APIs, Kafka clusters, Spark jobs, cloud storage, metadata catalogs, orchestration. The real value lies in how each piece hands off to the next with minimal coupling. Our go-to ally for building and testing all of this locally? Docker.

Kafka 101: The Event Log for Everything
---------------------------------------

If you have never touched Kafka before, picture it as a *commit log with superpowers*. At its core, Kafka is a distributed system made up of:

* **Brokers:** Servers that store and serve the data. A production cluster typically has 3–5 brokers for fault tolerance. Locally, we often run a single broker container to keep things simple.
* **Topics:** Named categories of data (for example, `orders`). Each topic is split into **partitions**, which are ordered, append-only logs that can be spread across brokers for scalability.
* **Producers:** Clients that write messages to a topic. Each record is a key/value pair plus headers and a timestamp.
* **Consumers:** Clients that read messages. Consumers join a **consumer group** so Kafka can assign partitions to group members automatically.

### Topic anatomy for first-time users

Topics can feel abstract, so let’s walk through what is physically stored on disk. Suppose you create a topic called `orders` with three partitions. On each broker you will see folders such as:

```
/kafka-logs/orders-0/00000000000000000000.log
/kafka-logs/orders-1/00000000000000000000.log
/kafka-logs/orders-2/00000000000000000000.log
```

Each log file is a partition. Inside the file, Kafka stores records as binary batches. Every record is stamped with a sequential **offset**. Offsets are zero-based and only increase; Kafka never edits the middle of the log. When you hear someone say “replay from offset 42,” it literally means “start reading the file from the 43rd record.”

### Why partitions matter

Partitions unlock parallelism. Imagine we have three partitions (`0`, `1`, `2`). When a producer sends an event with key `customer_42`, Kafka hashes the key and always writes it to the same partition. That guarantees ordering for that customer while allowing different customers to be processed simultaneously by different consumers.

Each partition keeps track of a monotonically increasing **offset** (0, 1, 2, …). Consumers store the last processed offset in Kafka (or an external store). If a consumer dies, another one in the group resumes from the latest committed offset—no duplicates, no gaps.

### Message lifecycle (produce → store → consume)

1. **Produce:** The client serializes a Python dictionary (or Java object) into bytes, wraps it in a record that includes headers, the key, and the value, and sends it over TCP to the broker.
2. **Persist:** The broker appends the bytes to the correct partition log, immediately fsyncs to disk (depending on `acks`), and replicates the batch to follower brokers if replication is enabled.
3. **Consume:** Consumers fetch batches of bytes, deserialize them back into structured objects, and commit the last processed offset. That offset commit is just another small record written to Kafka’s internal `__consumer_offsets` topic.

### Durability and retention

Kafka writes messages to disk and replicates them across brokers. With a replication factor of 3, every partition has one leader and two followers. Producers only get an acknowledgment when the leader (and optionally the followers) confirm the write, so data survives broker failures. Retention policies decide how long messages live (by time, size, or compaction). For ETL-style pipelines we often keep 7–30 days so we can reprocess history when downstream logic changes.

Retention is defined per topic. For example, to keep seven days of history you can run:

```bash
docker exec kafka kafka-configs --alter --entity-type topics \
  --entity-name orders \
  --add-config retention.ms=$((7*24*60*60*1000))
```

Kafka will automatically delete older segments when they expire, but offsets stay continuous—consumers simply find that older offsets are no longer available.

### Working with Kafka on the command line

Kafka ships with CLI tools that help you explore the cluster. If you have Docker Compose running from this project, you can open a new terminal and experiment:

```bash
# 1. Create a topic with 3 partitions and replication factor 1 (good enough for local dev)
docker exec kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic orders \
  --partitions 3 \
  --replication-factor 1

# 2. Describe topic metadata
docker exec kafka kafka-topics --describe --topic orders --bootstrap-server kafka:9092

# 3. Produce a message interactively
docker exec -it kafka kafka-console-producer --topic orders --bootstrap-server kafka:9092
>{"order_id": "1", "customer_id": "alice", "net_amount": 17.25}

# 4. Peek at messages (good for debugging)
docker exec kafka kafka-console-consumer \
  --topic orders \
  --from-beginning \
  --bootstrap-server kafka:9092
```

> **Tip for beginners:** If the console consumer prints binary gibberish, remember that Kafka stores bytes. When using Avro or Protobuf you must pass a deserializer. For plain JSON payloads like the quick test above, the consumer prints readable text.

Here is what the main flags mean:

* `--bootstrap-server`: the host:port pair the CLI connects to (matches the broker’s advertised listener).
* `--topic`: the logical channel you are operating on.
* `--partitions`: how many logs to create for the topic. Start with the number of concurrent consumers you expect.
* `--from-beginning`: tells the consumer to start at offset `0` rather than the latest offset.

Once you are comfortable with the CLI, moving to higher-level clients (Python, Java, Go) feels less mysterious.

Spark 101: Distributed DataFrames and Lazy Execution
----------------------------------------------------

Apache Spark is a distributed analytics engine. When people say “Spark job,” they mean a program that consists of:

* A **driver** process that defines the transformations (usually via the `SparkSession`).
* Multiple **executors** that perform the work in parallel across a cluster.
* A **cluster manager** (like Spark Standalone, YARN, Kubernetes, or on AWS, EMR) that provisions and monitors those executors.

If you are brand new to Spark, map the architecture to something familiar:

| Spark component | Analogy | What it actually does |
|-----------------|---------|------------------------|
| Driver | The “main” Python script | Parses your code, builds logical plans, and tells executors what to run |
| Executor | Worker process | Executes the tasks (map, filter, aggregate) on data partitions |
| Task | Function call | Smallest unit of work shipped from the driver to executors |
| Job | Batch of stages | Triggered by an action such as `count()` or `write()` |

Spark works with resilient distributed datasets (RDDs) under the hood, but in modern Spark you will mostly use **DataFrames** (tables with named columns). DataFrames are lazily evaluated: when you call `.select()` or `.groupBy()`, Spark builds a logical plan. The plan only executes when you perform an **action** (`show()`, `write`, `count`, etc.). Spark’s optimizer (Catalyst) rewrites the plan for efficiency before execution.

You can inspect that plan with `.explain()`—a useful trick when you are learning:

```python
totals.explain()
```

The output shows the logical plan, the optimized plan, and the physical plan that actually runs on the executors.

### Batch vs. Structured Streaming

Structured Streaming lets you express streaming jobs using the same DataFrame API you already know from batch processing. Instead of returning a static DataFrame, `readStream` produces a never-ending table where each micro-batch adds new rows. Spark keeps track of progress using **checkpoints** (stored on disk/S3) so it can recover from failures and exactly-once semantics are achievable.

### Running your first Spark job locally

Follow these steps using the Docker Compose stack in this repo:

1. **Open a shell inside the Spark master:** `docker exec -it spark-master bash`.
2. **Launch `pyspark`:** `/opt/spark/bin/pyspark --master spark://spark-master:7077`.
3. **Paste the minimal example below** and observe the output.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("intro-example")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

data = spark.createDataFrame([
    ("customer_1", 125.40),
    ("customer_2", 83.10),
    ("customer_1", 64.99),
], ["customer_id", "amount"])

totals = data.groupBy("customer_id").sum("amount")

totals.show()
```

> **What just happened?** The driver (your `pyspark` REPL) split the DataFrame into partitions, sent them to executor containers, waited for results, and printed them. Because the data set is tiny, the whole job finishes instantly, but the same code scales to millions of rows.

When we graduate to Structured Streaming, the API stays consistent—you still build DataFrames and call `.writeStream` to publish the results.

Understanding these concepts up front means the rest of this article can focus on wiring the pieces together rather than unraveling jargon along the way.

Why Docker Still Matters
------------------------

Even though the target platform is AWS, it’s impractical (and expensive) to test every iteration in the cloud. Docker lets us assemble a representative sandbox: a Kafka broker, a schema registry, a Spark cluster, and an API simulator—each in its own container. This mirrors the production topology while preserving developer velocity.

Containerizing the stack brings the same advantages we highlighted before:

* **Isolation:** Kafka, Spark, and ancillary services each carry their own dependencies without conflicting.
* **Reproducibility:** `docker compose up` recreates the environment on any teammate’s machine or CI agent.
* **Portability:** Whether you are on macOS, Windows, or Linux, the containers behave identically.

With that foundation, we can experiment locally, then translate configurations to AWS services when we are ready for scale.

1. Docker Compose – Building the Streaming Sandbox
-----------------------------------------------

Our local `docker-compose.yml` describes four main services: the API generator, Kafka (plus ZooKeeper or KRaft), a Spark cluster, and optional observability tooling (like Kafka UI or Prometheus).

A stripped-down service definition looks like this:

```
services:
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"
    depends_on:
      zookeeper:
        condition: service_healthy
```

Every entry follows the same template: choose an image, declare environment variables, expose ports, mount volumes if needed, and express startup dependencies. Once you understand this pattern, scaling the Compose file to include schema registries, Kafka Connect, or Spark history servers is straightforward.

The Full Compose File
---------------------

Our real configuration adds more detail:

* A mock API container that emits JSON payloads at a configurable rate.
* ZooKeeper (or KRaft) backing Kafka, with health checks to ensure the broker waits for coordination before starting.
* Spark master and worker containers, with shared volumes for job submissions and configuration.
* Utility containers like `kafdrop` for inspecting topics or `spark-submit` helpers.

The Compose file also seeds a local S3-compatible object store (e.g., MinIO) so we can test AWS-style storage semantics before deploying to the cloud. By the time `docker compose up` finishes, you have a realistic playground for the end-to-end flow.

How to Read a Service Definition
--------------------------------

Each service block answers a few core questions:

* **Hostname:** The service key doubles as the DNS entry Docker networks provide (e.g., other containers reach Kafka at `kafka:9092`).
* **Image or Build:** Some components (like Kafka) use public images; others (like the API simulator) are built from local Dockerfiles to bake in custom logic.
* **Environment:** Configuration values—bootstrap servers, topic names, schema registry URLs—often come from an `.env` file so we can swap between local and cloud-friendly settings.
* **Volumes:** Spark jobs mount the `/opt/spark-apps` directory so they can access the same job artifacts the driver submits.
* **Healthchecks & depends_on:** Guarantee that Kafka only starts after ZooKeeper is ready, and Spark waits for Kafka before beginning structured streaming.

Once the local environment mirrors production behavior, we can implement each pipeline stage with confidence.

2. Ingest: Publishing API Events into Kafka
-------------------------------------------

Our data source is an HTTP API that emits transactional events—think e-commerce orders, IoT telemetry, or payment updates. A lightweight Python service polls the endpoint, converts responses into well-structured JSON messages, and pushes them to a Kafka topic.

Key responsibilities of the producer:

1. **Watermarking:** Track the last successful poll (via `updated_at` timestamps or incremental IDs) to avoid duplicate ingestion.
2. **Batching & Rate Limiting:** Respect API quotas by batching requests and sleeping between polls.
3. **Schema Enforcement:** Serialize payloads using Avro or JSON Schema so downstream consumers know the data contracts.
4. **Error Handling:** Log failures, raise metrics, and optionally push problematic payloads to a dead-letter topic.

Under the hood, the Python client (using `confluent-kafka` or `kafka-python`) opens a TCP connection to the broker, negotiates the protocol version, and starts an asynchronous send loop. You can tune delivery guarantees with producer configs:

* `acks=all` waits for the leader *and* replicas to confirm writes—safer than the default `acks=1`.
* `enable.idempotence=true` prevents duplicate records during retries.
* `linger.ms` and `batch.size` let the client coalesce multiple API responses into a single network batch for throughput.

In code, this might look like:

```python
import requests
from confluent_kafka import SerializingProducer

producer = SerializingProducer({...})

def publish_orders():
    params = {"updated_since": watermark()}
    response = requests.get(API_URL, headers=AUTH_HEADER, params=params, timeout=10)
    response.raise_for_status()

    for record in response.json()["data"]:
        key = record["order_id"]
        value = order_schema.encode(record)
        producer.produce(topic="orders", key=key, value=value)
    producer.flush()
```

We package this script into the API container and schedule it with cron, Airflow, or a simple loop with backoff logic. The moment a payload lands on the `orders` topic, the streaming machinery takes over.

To round out the picture, here is a bare-bones consumer that prints the same events:

```python
from confluent_kafka import DeserializingConsumer

consumer = DeserializingConsumer({
    "bootstrap.servers": "kafka:9092",
    "key.deserializer": str,
    "value.deserializer": order_schema.decode,
    "group.id": "orders-debug",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["orders"])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    print(msg.key(), msg.value())
    consumer.commit(msg)
```

By experimenting with these snippets locally, beginners see how Kafka’s offset management and delivery semantics work in practice.

#### Producer configuration breakdown

Replace the `{...}` placeholder in the `SerializingProducer` with a dictionary like this:

```python
producer = SerializingProducer({
    "bootstrap.servers": "kafka:9092",    # matches docker-compose service name and port
    "security.protocol": "PLAINTEXT",     # switch to SASL_SSL in production
    "key.serializer": str.encode,          # converts Python strings into bytes
    "value.serializer": order_schema.encode,
    "enable.idempotence": True,            # guarantees exactly-once publishes when retries happen
    "acks": "all",                         # wait for leader + replicas
    "linger.ms": 50,                       # batch small API responses together
})
```

The consumer receives a similar configuration but with deserializers (`str` or custom functions) so it can turn bytes back into Python objects.

> **Why call `consumer.commit(msg)` manually?** Automatic commits (`enable.auto.commit=True`) work for simple use cases, but explicitly committing after processing ensures you only advance the offset when your business logic succeeds.

3. Kafka Fundamentals: Topics, Partitions, and Retention
--------------------------------------------------------

Kafka is the backbone of the pipeline. Three concepts matter most:

* **Topics & Partitions:** Each topic is sharded into partitions to parallelize reads/writes. For ordered processing, key events consistently so that related records land in the same partition.
* **Retention:** Configure how long Kafka keeps data. For near-real-time ETL, a 7–30 day retention window often strikes a balance between replay capability and storage cost.
* **Consumer Groups:** Spark streaming jobs (and any other consumers) join a group; Kafka ensures each partition is consumed by only one member in a group. This pattern allows scaling Spark executors horizontally.

We also set up:

* **Schema Registry:** Enforce schemas for producers/consumers, enabling compatibility checks when fields evolve.
* **Monitoring:** JMX metrics piped to Prometheus/Grafana to alert on lag, throughput, and broker health.
* **Kafka Connect:** Optional but powerful—prebuilt connectors can stream data between Kafka and S3, databases, or Elasticsearch without writing custom code. For example, you can mirror curated Spark outputs back into Kafka for downstream microservices.

> **Try it yourself:** Once the Docker cluster is up, run `docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group orders-debug` to inspect lag and offsets. Understanding these numbers helps you size partitions and tune consumer concurrency when workloads spike.

4. Process: Streaming Transformations with Spark Structured Streaming
---------------------------------------------------------------------

Once messages accumulate in Kafka, Apache Spark Structured Streaming ingests them in micro-batches (or continuous processing mode), applies transformations, and writes results out to AWS targets.

A typical job does the following:

1. **Read from Kafka:** Use the built-in Kafka source to read from topics, specifying starting offsets (earliest/latest) and checkpoint locations for fault tolerance.
2. **Parse Payloads:** Deserialize Avro/JSON into Spark DataFrame columns using the registered schema.
3. **Enrich:** Join against dimension tables or reference data (either in-memory broadcast joins or external systems like DynamoDB or RDS).
4. **Aggregate:** Compute rolling metrics (e.g., hourly order volume, per-customer spend) using window functions.
5. **Write:** Persist results to S3 in partitioned Parquet files and optionally push a summary back to Kafka or a REST endpoint.

Here’s a simplified Spark job:

```python
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("status", StringType()),
    StructField("net_amount", DoubleType()),
])

spark = (
    SparkSession.builder
    .appName("orders-stream")
    .getOrCreate()
)

raw_orders = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.environ["KAFKA_BROKERS"])
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)

orders = (
    raw_orders.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), order_schema).alias("order"))
    .select("order.*")
)

agg = (
    orders
    .withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "5 minutes"), col("customer_id"))
    .sum("net_amount")
)

query = (
    agg.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", "s3a://my-bucket/checkpoints/orders")
    .option("path", "s3a://my-bucket/curated/orders_5min")
    .start()
)

query.awaitTermination()
```

Key Spark features we lean on:

* **Checkpointing:** Preserves offsets and aggregation state so the job can recover after failures without reprocessing.
* **Watermarking:** Drops late events beyond a threshold to control state growth.
* **Exactly-once semantics:** With Kafka + Spark + S3, we can achieve effectively-once processing when using idempotent writes or merge-on-read tables (e.g., Apache Hudi or Delta Lake).

Let’s unpack a few lines from the script so new Spark users can follow the execution:

1. `spark.readStream.format("kafka")` tells Spark to instantiate the Kafka source; under the covers it uses the Kafka consumer API and manages offsets in the checkpoint directory you specify later.
2. `.selectExpr("CAST(value AS STRING)")` converts the Kafka byte payload into a JSON string so we can parse it. The `from_json` function then maps the JSON to columns defined in `order_schema` (a `StructType`).
3. `.withWatermark("event_time", "10 minutes")` signals how long Spark should wait for late events with older timestamps. Combined with `window`, Spark knows when it can safely emit aggregates.
4. `.writeStream.format("parquet")` leverages the built-in Parquet sink. The `s3a://` prefix works because we include the `hadoop-aws` jar and configure AWS credentials via environment variables or IAM roles.

#### Understanding the write options

* `outputMode("append")` — emits a row only once the window is complete. Switch to `update` if you want intermediate aggregates.
* `option("checkpointLocation", ...)` — directory where Spark saves offsets, schema information, and aggregation state. Losing this folder forces a replay from the earliest retained offsets.
* `option("path", ...)` — the S3 prefix for new Parquet files. Spark automatically creates partition folders (e.g., `window=2024-05-01 12:00:00`) when the query involves windows or partitioned columns.
* `.start()` — launches the continuous query and returns a `StreamingQuery` object. You can monitor `query.status` or `query.lastProgress` to debug throughput and latency.

To run this job against the containers, we submit it with `spark-submit` from the master node:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.access.key=minio \
  --conf spark.hadoop.fs.s3a.secret.key=minio123 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  /opt/spark-apps/orders_stream.py
```

The package coordinates pull in the Kafka source connector and S3 client libraries. When you deploy to AWS EMR, those dependencies are pre-installed, and credentials come from IAM roles instead of static keys.

5. Persist & Catalog: Landing Data in AWS
-----------------------------------------

Once transformations complete, we deliver the outputs to AWS services:

* **S3 Buckets:** Store raw, refined, and aggregated zones as partitioned Parquet files. Use lifecycle policies to transition cold data to Glacier if needed.
* **AWS Glue Data Catalog:** Maintain table metadata so Athena (and other query engines) know the schema and location of datasets.
* **Athena & QuickSight:** Analysts query curated tables directly or build dashboards without hitting production clusters.
* **Optional Warehousing:** For near-real-time serving, replicate curated data into Redshift, Snowflake, or Lakehouse tables via Apache Hudi, Delta Lake, or Iceberg running on EMR.

A sample Glue crawler configuration points at `s3://my-bucket/curated/` and updates table definitions whenever new partitions land. Athena queries then become as simple as:

```sql
SELECT date_trunc('hour', event_time) AS hour, SUM(net_amount)
FROM curated.orders_5min
WHERE event_time >= current_timestamp - interval '7' day
GROUP BY 1
ORDER BY 1;
```

6. Orchestration: AWS-Native or Airflow on AWS
---------------------------------------------

Coordinating the producers, Spark jobs, and downstream catalog updates can be handled in several ways:

* **AWS Step Functions:** Model the workflow as a state machine—start the API producer (perhaps an ECS task or Lambda), trigger the EMR Serverless Spark job, then kick off a Glue crawler.
* **Managed Airflow (MWAA):** Reuse familiar DAG patterns to start/stop EMR clusters, submit Spark jobs, and run health checks.
* **EventBridge:** Use rule-based triggers so that when new data lands in S3, automated jobs (like Glue ETL or Lambda functions) respond immediately.

No matter the orchestrator, the DAG/state machine should handle retries, alerting (via CloudWatch Alarms or SNS), and parameterization (e.g., dynamic output paths based on execution timestamps).

7. From Local to AWS: Deployment Considerations
----------------------------------------------

With the pipeline proven in Docker, migrating to AWS involves a few mapping decisions:

* **Kafka:** Replace the containerized broker with Amazon MSK (or self-managed EC2). Configure security groups, IAM, and encryption-at-rest.
* **Spark:** Use EMR (clusters or EMR Serverless) or Glue Streaming jobs. Package PySpark code in an S3 location and submit via the AWS CLI or SDK.
* **API Producer:** Run as an ECS Fargate task, Lambda (for lightweight workloads), or a Step Functions activity.
* **Storage:** Point Spark’s `s3a://` paths to real S3 buckets with IAM roles granting least-privilege access.
* **Monitoring:** Integrate with CloudWatch, AWS X-Ray, and MSK metrics. Forward Spark logs to CloudWatch Logs or an ELK stack.

CI/CD pipelines (GitHub Actions, CodePipeline) can build Docker images, deploy infrastructure with Terraform/CloudFormation, and roll out Spark job updates with blue/green strategies.

### Beginner runbook: from zero to first event

1. **Clone the repo** and copy `.env.example` to `.env`, filling in API credentials.
2. **Start the local stack:** `docker compose up --build`. Wait until Kafka, Spark, and the mock API report healthy.
3. **Create a topic:** run the CLI commands above to create the `orders` topic.
4. **Publish sample data:** use the API producer container (`docker exec api-producer python publish_orders.py --one-shot`) or the console producer to send a test event.
5. **Tail the consumer:** run the console consumer or the Python snippet to ensure the event appears.
6. **Submit the Spark job:** `spark-submit` the `orders_stream.py` script and watch the logs show micro-batch progress.
7. **Inspect S3/MinIO:** open the MinIO UI (usually at `http://localhost:9001`) and verify Parquet files are written under the configured prefix.
8. **Query the results:** use Athena (in AWS) or `aws athena start-query-execution` locally to validate the curated dataset.

Treat this checklist as muscle memory; repeating it a few times cements how the components interact.

8. Observability & Data Quality in Motion
-----------------------------------------

Streaming pipelines demand continuous visibility:

* **Lag Monitoring:** Track Kafka consumer lag per topic/partition to ensure Spark keeps pace with producers.
* **Data Quality:** Use tools like Deequ or Great Expectations integrated into Spark to validate schemas and value ranges on the fly.
* **Dead-Letter Queues:** Capture malformed messages or processing failures for offline inspection.
* **Alerting:** Wire CloudWatch alarms or Prometheus alerts to Slack, PagerDuty, or email so on-call engineers respond quickly to anomalies.

Automated unit tests around schema evolution (e.g., adding optional fields) prevent incompatible changes from breaking consumers. Contract testing between the API producer and Spark ensures both sides agree on payload structures.

9. Putting It All Together
--------------------------

Let’s recap the lifecycle:

1. A producer polls the external API, packages the payloads, and publishes them to Kafka with schema validation and watermarking.
2. Kafka stores the stream durably, enabling multiple consumers (Spark, monitoring tools, ML feature pipelines) to read at their own pace.
3. Spark Structured Streaming reads from Kafka, enriches and aggregates the events, and continuously writes curated outputs to S3 with checkpointing.
4. AWS Glue catalogs the new data so Athena, QuickSight, or downstream warehouses can consume it with minimal friction.
5. AWS Step Functions or Managed Airflow orchestrates the jobs, manages retries, and notifies the team if something breaks.

The result is an event-driven architecture capable of handling surges in traffic, replaying data if downstream systems fail, and scaling independently at each stage.

Conclusion
----------

By fusing APIs, Kafka, Spark, and AWS services, we constructed a resilient streaming pipeline that captures real-time events and transforms them into analytics-ready datasets. Docker provided the playground to iterate quickly; Kafka decoupled producers and consumers; Spark delivered scalable transformations; AWS supplied durable storage, cataloging, and orchestration.

Whether you are extending an existing batch pipeline into real-time territory or building greenfield streaming infrastructure, this blueprint covers the critical pieces: local reproducibility, cloud deployment, data quality, and observability. From here, you can add ML feature stores, anomaly detection, or hybrid batch/stream processing without rewriting the foundation.

Happy streaming!
