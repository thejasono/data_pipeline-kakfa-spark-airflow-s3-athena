# API-Kafka-Spark-Airflow-S3-Athena Pipeline
## **Introduction: Building a Dynamic Data Engineering Project**

In our rapidly evolving digital age, data engineering has emerged as the backbone of the modern data-driven world. We're surrounded by an ever-increasing volume of data, and the ability to process and analyze this data in real-time is becoming a necessity rather than a luxury. In this guide, we'll delve deep into constructing a robust data pipeline, leveraging a combination of Kafka for data streaming, Spark for processing, Airflow for orchestration, Docker for containerization, S3 for storage, and Python as our primary scripting language.

To illustrate this process, we'll employ the Random Name API, a versatile tool that generates fresh random data every time it's triggered. It offers a practical representation of the real-time data many businesses deal with daily. The first step in our journey involves a Python script, designed meticulously to fetch data from this API. To emulate the streaming nature of data, we'll execute this script at regular intervals. But that's not all — this very script will also serve as our bridge to Kafka, writing the fetched data directly to a Kafka topic.

As we progress, Airflow's Directed Acyclic Graphs (DAGs) play a pivotal role. Orchestrating our processes, the Airflow DAG script ensures our Python script runs like clockwork, consistently streaming data and feeding it into our pipeline. Once our data makes its way to the Kafka producer, Spark Structured Streaming takes the baton. It consumes this data, processes it, and then seamlessly writes the modified data to S3, ensuring it's ready for any subsequent analytical processes.

An essential aspect of our project is its modular architecture. Each service, be it Kafka, Spark, or Airflow, runs in its isolated environment, thanks to Docker containers. This not only ensures smooth interoperability but also simplifies scalability and debugging.

## **Getting Started: Prerequisites and Setup**

For this project, we are leveraging a GitHub repository that hosts our entire setup, making it easy for anyone to get started.

**a. Docker:**
Docker will be our primary tool to orchestrate and run various services.

- **Installation:** Visit Docker's official website to download and install Docker Desktop for your OS.
- **Verification:** Open a terminal or command prompt and execute `docker --version` to ensure a successful installation.

**b. Object storage (AWS S3):**
The pipeline writes JSON micro-batches directly to Amazon S3. Create a bucket in your preferred region and update the environment files with the bucket name, region, and (if necessary) temporary credentials. When the stack starts, the Spark streaming container uses those settings to persist data without any additional bootstrap services.

Populate **both** environment files before you start the stack:

- `.env` already ships with sane defaults for service configuration (Airflow, Kafka, Spark). Update the S3 bucket/region entries to match your environment.
- `.env.aws` contains the AWS credentials that the Spark containers load via the Compose `env_file` directive. The repository includes placeholder values so `docker compose` can start, but you must overwrite them with working credentials (for example, paste the output from `aws configure export-credentials --format env`). The Windows helper script `run_spark.ps1` automates this by logging in with AWS SSO, generating a fresh `.env.aws`, and running Compose with those short-lived credentials.

#### Enabling Amazon S3 connectivity

> 📝 **S3 setup checklist** – before you start Docker, work through the following quick list so the streaming job can talk to your bucket:
>
> 1. Create or identify an S3 bucket in your preferred AWS region.
> 2. Update `.env` with your bucket name, output prefixes, and region (`S3_BUCKET`, `S3_OUTPUT_PREFIX`, `S3_CHECKPOINT_PREFIX`, `S3_REGION`).
> 3. Decide how the containers should authenticate:
>    - Paste long-lived access keys into `.env.aws` (or `.env`) as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and, when required, `AWS_SESSION_TOKEN`/`AWS_REGION`; **or**
>    - Leave those values blank and mount your host `~/.aws` directory with an active AWS CLI profile. Export `AWS_PROFILE` if you need a non-default profile inside the containers.
> 4. Only set `S3_ENDPOINT` and `S3_PATH_STYLE_ACCESS` in `.env` when you target a private gateway or S3-compatible appliance that needs path-style URLs.
> 5. Run `docker compose up -d --build spark_streaming` (or restart the full stack) after editing the environment files so the Spark container reloads its configuration.

1. **Confirm AWS-side access.** Double-check that the IAM user or role you plan to use can call `s3:PutObject`, `s3:ListBucket`, and `s3:GetBucketLocation` on the target bucket/prefix. Verifying the bucket name and region now prevents confusing Spark errors later.
2. **Surface credentials to the containers.** Choose one of the following approaches so Spark can read credentials from the default provider chain:
   - **Environment variables:** Add `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and (if required) `AWS_SESSION_TOKEN` to `.env` or `.env.aws`. These keys are injected directly into the `spark_streaming` service via Compose.
   - **IAM Identity Center/SSO or roles:** Leave the credential entries blank, uncomment the `~/.aws` bind mount in `docker-compose.yaml`, and (optionally) export `AWS_PROFILE` so the container uses your cached CLI login.
3. **Set bucket and endpoint configuration.** Adjust `S3_BUCKET`, `S3_REGION`, and `S3_OUTPUT_PREFIX` in `.env`. Only populate `S3_ENDPOINT` and `S3_PATH_STYLE_ACCESS` when you target a private gateway or an S3-compatible appliance that requires path-style URLs.
4. **Restart the streaming service to pick up changes.** Rebuild just the Spark streaming container after updating credentials or environment variables:

   ```bash
   docker compose up -d --build spark_streaming
   ```

5. **Post-change verification.** Before and after the restart, run a quick network sanity check (for example, `curl https://s3.<region>.amazonaws.com --head`) to ensure the Docker host can reach the endpoint. Once the stack is running, tail the streaming logs and watch your bucket for new JSON objects under the configured prefix:

   ```bash
   docker compose logs -f spark_streaming
   ```

   If your VPC endpoint only supports path-style access, remember to set `S3_PATH_STYLE_ACCESS=true` so Spark forms the correct URLs.


**c. Setting Up the Project:**

- **Clone the Repository:** First, you'll need to clone the project from its GitHub repository using the following command:

```
git clone https://github.com/<your-username>/api-kafka-spark-airflow-s3-athena.git
```

Navigate to the project directory:

```
cd api-kafka-spark-airflow-s3-athena
```

- **Deploy Services using `docker-compose`:** Within the project directory, you'll find a `docker-compose.yml` file. This file describes all the services and their

```
docker network create docker_streaming  # create once, safe to rerun
docker compose up -d --build
```

Running the stack in detached mode (`-d`) lets Docker start every container in the background, and the `--build` flag ensures the images are rebuilt if you have changed any of the source files since your last run. The command orchestrates the start-up of all necessary services like Kafka, Spark, Airflow, etc., in Docker containers. If you only need to restart the stack without rebuilding the images, you can omit `--build` on subsequent runs.

> 🗒️ **PostgreSQL client access** – the `airflow_db` service now mounts `postgres/conf/pg_hba.conf` so that other containers on the Docker network can authenticate with SCRAM-SHA-256. If you initialised the database with an older version of the configuration, remove the local `./airflow_pgdata` directory before running `docker compose up` so Postgres picks up the updated access rules.

> ❓ **Why did `airflow-init` complain that `airflow_db` does not exist?** The official Postgres image only provisions the database named in `POSTGRES_DB` the very first time it bootstraps an *empty* data directory. Because this project persists Postgres state to `./airflow_pgdata`, any subsequent container restarts reuse the existing files. If that directory was created before `POSTGRES_DB=airflow_db` was set—or if the metadata database was dropped manually—Postgres will come back up without recreating it and Airflow's health check fails with `FATAL: database "airflow_db" does not exist`. The bundled `postgres/init/001-create-airflow-db.sh` script now runs on every start-up to create the database when it is missing, but if you want Postgres to re-run all init scripts from scratch you can still delete `./airflow_pgdata` (or `docker compose down -v`) and start the stack again.

> 🔐 **One-time secret key setup** – update `AIRFLOW_SECRET_KEY` in `.env` with a long random value before starting the stack. Every Airflow container reads this value so served logs can be fetched without the 403/"secret_key" mismatch warning that appears when each component autogenerates its own key.

> 🆕 **Automatic end-to-end pipeline** — once the containers are healthy the following pieces cooperate without manual intervention:
> * `airflow_init` seeds the metadata DB and admin user, then the scheduler/webserver start normally.
> * `name_stream_dag` is scheduled every five minutes and runs the API → Kafka producer for two minutes per cycle.
> * `spark_streaming` (a long-running service) submits `spark_processing.py` to the Spark master so Kafka records are continuously written to Amazon S3 as newline-delimited JSON objects.
>
> That means `docker compose up -d --build` is now enough to demonstrate “API → Kafka → Spark → S3” for your portfolio—no manual `docker exec`, `curl`, or `spark-submit` steps.

### Custom Airflow image & Python dependencies

The Compose file builds a lightweight wrapper image defined in `Dockerfile.airflow` on top of the official `apache/airflow:2.9.3-python3.11` base. During the build we install the pinned contents of `requirements.txt` with the matching Apache Airflow constraints file. That combination gives you:

* **Reproducible dependency resolution** – every Python package (for example, `confluent-kafka==2.4.0`) is baked into the image with explicit versions so a fresh deployment runs the same code you validated.
* **Faster container start-up** – no `pip install` happens on container boot; workers, the webserver, and the scheduler all reuse the same pre-built image.
* **A production-friendly workflow** – pushing this image to an internal registry lets you promote the exact artifact you tested in staging, instead of re-installing requirements at runtime.

If you add new Python dependencies, update `requirements.txt`, rebuild the image (`docker compose build airflow_webserver`), and redeploy. For more advanced scenarios you can supply a different constraints URL via the `AIRFLOW_CONSTRAINTS_URL` build argument to keep Airflow and your packages in sync.

### Spark ↔ S3 defaults

The Spark submitter container (`spark_streaming`) starts with the AWS connector JARs required for Structured Streaming and the `s3a://` filesystem. Supply an AWS region and bucket in `.env`, then either rely on the Default AWS Credentials Provider Chain or pass explicit credentials through environment variables. If you use interface endpoints or custom gateways, set `S3_ENDPOINT` accordingly so the job targets the correct hostname.

After the containers have started, use the following quick checks to make sure everything is healthy before proceeding:

1. `docker compose ps` & `docker compose logs --tail=50` – confirm each service is in the `running` state and that there are no obvious startup errors.
2. Airflow Webserver (`http://localhost:8080`) – log in with the admin account you create in the next section to confirm the UI loads and the `name_stream_dag` DAG appears.
3. Kafka UI (`http://localhost:8888`) – ensure the cluster is reachable and that the `names_topic` topic exists once you create it.
4. Spark Master UI (`http://localhost:8085`) – verify the master and both workers are listed as `ALIVE` before submitting streaming jobs.
5. Amazon S3 – check your bucket (for example with `aws s3 ls s3://$S3_BUCKET/$S3_OUTPUT_PREFIX/`) and confirm newline-delimited JSON files are arriving as the Spark job processes Kafka records.

> 📈 **Validating the full flow** – Trigger `name_stream_dag` in Airflow or wait for the scheduled run, then watch `spark_streaming` container logs (`docker compose logs -f spark_streaming`) and your Amazon S3 bucket. You should see Structured Streaming batches completing and fresh JSON files landing in the `names/` prefix without any manual `spark-submit` commands.

### Guided end-to-end verification

If you would rather click through the interfaces instead of tailing logs, follow this walkthrough once the containers are healthy:

1. **Airflow** – Open `http://localhost:8080`, log in, and toggle the **`name_stream_dag`** switch to "On". Either trigger a manual run or wait for the next five-minute schedule; the DAG runs the producer for two minutes per execution.
2. **Kafka UI** – Head to `http://localhost:8888`, choose the Kafka cluster, and inspect the **`names_topic`** topic. You should see the message rate steadily increase while the DAG run is active.
3. **Spark UI** – Visit the Spark master at `http://localhost:8085` and open the **`spark_streaming`** application link. Under the Structured Streaming tab you can confirm that new micro-batches are processed and checkpoints are advancing.
4. **Amazon S3** – Inspect your bucket using the AWS CLI, Console, or SDK of choice. New newline-delimited JSON files should appear under the configured prefix as the Spark streaming job processes Kafka records.


Completing the four steps above proves the full path "API → Kafka → Spark → S3" is functioning without digging into container logs.


> ℹ️ Both the `airflow_webserver` and `airflow_scheduler` services run with the same user ID/group ID mapping derived from the `AIRFLOW_UID`/`AIRFLOW_GID` values in `.env`. This prevents the scheduler from failing with permission errors when the mounted `dags/`, `logs/`, or `plugins/` directories are owned by `root`, and ensures that the Airflow components come up cleanly together. The only Airflow port published to the host is `8080`, and every other service in the stack binds to a distinct host port, so there are no container port conflicts when you run the full compose file.

## **Breaking Down the projects Files**

### 1)  ****`docker-compose.yml`**

The heart of our project setup lies in the **`docker-compose.yml`** file. It orchestrates our services, ensuring smooth communication and initialization. Here's a breakdown:

**1. Version**

We're using Docker Compose file format version '3.7', ensuring compatibility with our services.

**2. Services**

Our project encompasses several services:

- **Airflow:**
- **Database (`airflow_db`):** Uses PostgreSQL[1](https://github.com/simardeep1792/Data-Engineering-Streaming-Project#:~:text=%E3%80%9059%E2%80%A0.env%E3%80%91%0A%0A%E3%80%9060%E2%80%A0README.md%E3%80%91%0A%0A%E3%80%9061%E2%80%A0airflow.sh%E3%80%91%0A%0A%E3%80%9062%E2%80%A0docker).
- **Web Server (`airflow_webserver`):** Initiates the database and sets up an admin user.
- **Kafka:**
- **Zookeeper (`kafka_zookeeper`):** Manages broker metadata.
- **Brokers:** Three instances (**`kafka_broker_1`**, **`2`**, and **`3`**).
- **Base Configuration (`kafka_base`):** Common settings for brokers.
- **Kafka Connect (`kafka_connect`):** Facilitates stream processing.
- **Schema Registry (`kafka_schema_registry`):** Manages Kafka schemas.
- **User Interface (`kafka_ui`):** Visual interface for Kafka insights.
- **Spark:**
- **Master Node (`spark_master`):** The central control node for Apache Spark.
- **Streaming submitter (`spark_streaming`):** A helper container that continuously submits the Structured Streaming job.
  

**3. Volumes**

We utilize a persistent volume, **`spark_data`**, ensuring data consistency for Spark.

**4. Networks**

Two networks anchor our services:

- **Kafka Network (`kafka_network`):** Dedicated to Kafka.
- **Default Network (`default`):** Externally named as **`docker_streaming`**.

### 3) **Published Service Ports**

Each service that exposes a user-facing port is mapped to a unique host port so that the stack can run without conflicts on a single Docker host. The current bindings from `docker-compose.yaml` are summarised below:

| Service | Host Port | Container Port | Notes |
| --- | --- | --- | --- |
| Airflow Webserver | 8080 | 8080 | Airflow UI (`http://localhost:8080`). |
| Kafka Broker (external listener) | 9092 | 9092 | External client access to Kafka. |
| Kafka Connect REST | 8083 | 8083 | Manage connectors via REST. |
| Schema Registry | 8081 | 8081 | Avro/JSON schema management. |
| Kafka UI | 8888 | 8080 | Provectus Kafka UI (`http://localhost:8888`). |
| Spark Master UI | 8085 | 8080 | Remapped to avoid clashing with Airflow. |
| Spark Worker 1 UI | 8086 | 8081 | Worker 1 monitoring UI. |
| Spark Worker 2 UI | 8087 | 8081 | Worker 2 monitoring UI. |

> ✅ **No host port conflicts** – every published port is distinct, so you can run the entire stack simultaneously without manual remapping. If you introduce new services, continue assigning unused host ports to maintain this guarantee.

### 2)  **`kafka_stream_dag.py`**

This file primarily defines an Airflow Directed Acyclic Graph (DAG) that handles the streaming of data to a Kafka topic.

**1. Imports**

Essential modules and functions are imported, notably the Airflow DAG and PythonOperator, as well as a custom **`initiate_stream`** function from **`kafka_streaming_service`**.

**2. Configuration**

- **DAG Start Date (`DAG_START_DATE`):** Sets when the DAG begins its execution.
- **Default Arguments (`DAG_DEFAULT_ARGS`):** Configures the DAG's basic parameters, such as owner, start date, and retry settings.

**3. DAG Definition**

  A new DAG is created with the name **`name_stream_dag`**, configured to run every five minutes. It's designed not to run for any missed intervals (with **`catchup=False`**) and allows only one active run at a time.

**4. Tasks**

A single task, **`kafka_stream_task`**, is defined using the PythonOperator. This task calls the **`initiate_stream`** function, effectively streaming data to Kafka when the DAG runs.

### 3)  **`kafka_streaming_service.py`**

**1. Imports & Configuration**

Essential libraries are imported, and constants are set, such as the API endpoint, Kafka bootstrap servers, topic name, and streaming interval details.

**2. User Data Retrieval**

The **`retrieve_user_data`** function fetches random user details from the specified API endpoint.

**3. Data Transformation**

The **`transform_user_data`** function formats the raw user data for Kafka streaming, while **`encrypt_zip`** hashes the zip code to maintain user privacy.

**4. Kafka Configuration & Publishing**

- **`configure_kafka`** sets up a Kafka producer.
- **`publish_to_kafka`** sends transformed user data to a Kafka topic.
- **`delivery_status`** provides feedback on whether data was successfully sent to Kafka.

**5. Main Streaming Function**

**`initiate_stream`** orchestrates the entire process, retrieving, transforming, and publishing user data to Kafka at regular intervals.

**6. Execution**

When the script is run directly, the **`initiate_stream`** function is executed, streaming data for the duration specified by **`STREAMING_DURATION`**.

### 3)  **`spark_processing.py`**

**1. Imports & Logging Initialization**

The necessary libraries are imported, and a logging setup is created for better debugging and monitoring.

**2. Spark Session Initialization**

**`initialize_spark_session`**: This function sets up the Spark session with configurations required to access data from S3.

**3. Data Retrieval & Transformation**

- **`get_streaming_dataframe`**: Fetches a streaming dataframe from Kafka with specified brokers and topic details.
- **`transform_streaming_data`**: Transforms the raw Kafka data into a desired structured format.

**4. Streaming to S3**

**`initiate_streaming_to_bucket`**: This function streams the transformed data to an S3 bucket in parquet format. It uses a checkpoint mechanism to ensure data integrity during streaming.

**5. Main Execution**

The **`main`** function orchestrates the entire process: initializing the Spark session, fetching data from Kafka, transforming it, and streaming it to S3.

**6. Script Execution**

If the script is the main module being run, it will execute the **`main`** function, initiating the entire streaming process.

## **Building the Data Pipeline: Step-by-Step**

### 1. Start (or restart) the stack

```bash
docker network create docker_streaming  # create once
docker compose up -d --build
```

The compose bundle handles dependency installation, Airflow database migrations, user creation, and service start-up automatically. No additional `pip install` or manual `airflow users create` commands are required unless you change the default credentials in `.env`.

### 2. What the stack does for you

- **Airflow**: `airflow-init` runs database migrations and provisions the admin account defined in `.env`. The scheduler and webserver containers then start, unpause `name_stream_dag`, and trigger the producer every five minutes.
- **Kafka**: the producer task calls `ensure_topic(...)`, so the `names_topic` topic is created on-demand—there is no need to pre-create it in the UI for local demos.
- **Spark**: the `spark_streaming` service submits `spark_processing.py` to the Spark master as soon as Kafka is healthy. Required connector JARs are baked into the custom image, so Structured Streaming can immediately sink newline-delimited JSON data to Amazon S3.

Once all services report healthy, the system continuously demonstrates the end-to-end path (Random User API → Kafka → Spark → S3) without extra intervention.

### 3. Optional manual commands (for debugging or learning)

- **Inspect Airflow from the CLI**

  ```bash
  docker compose exec airflow_scheduler airflow dags list
  docker compose exec airflow_scheduler airflow tasks list name_stream_dag
  ```

- **Tail logs**

  ```bash
  docker compose logs -f spark_streaming
  docker compose logs -f airflow_scheduler
  ```

- **Submit ad-hoc Spark jobs** – if you want to experiment outside the managed service, open a shell inside the master and run `spark-submit` manually:

  ```bash
  docker compose exec -it spark-master /bin/bash
  cd /opt/bitnami/spark
  ./bin/spark-submit --master spark://spark-master:7077 /opt/spark/app/spark_processing.py
  ```

  The custom image already contains the Kafka and S3 connector jars, so additional downloads are unnecessary.

### 4. Verify data flow

- Watch the Kafka topic in the UI (`http://localhost:8888`) while the DAG run is active.
- Follow Structured Streaming progress in the Spark master UI (`http://localhost:8085`).
- Inspect the Amazon S3 bucket configured in `.env` and confirm newline-delimited JSON files appear under the `names/` prefix.

## C**hallenges and Troubleshooting**

1. **Configuration Challenges**: Ensuring environment variables and configurations (like in the **`docker-compose.yaml`** file) are correctly set can be tricky. An incorrect setting might prevent services from starting or communicating.
2. **Service Dependencies**: Services like Kafka or Airflow have dependencies on other services (e.g., Zookeeper for Kafka). Ensuring the correct order of service initialization is crucial.
3. **Airflow DAG Errors**: Syntax or logical errors in the DAG file (**`kafka_stream_dag.py`**) can prevent Airflow from recognizing or executing the DAG correctly.
4. **Data Transformation Issues**: The data transformation logic in the Python script might not always produce expected results, especially when handling various data inputs from the Random Name API.
5. **Spark Dependencies**: Ensuring all required JARs are available and compatible is essential for Spark's streaming job. Missing or incompatible JARs can lead to job failures.
6. **Kafka Topic Management**: Creating topics with the correct configuration (like replication factor) is essential for data durability and fault tolerance.
7. **Networking Challenges**: Docker networking, as set up in the **`docker-compose.yaml`**, must correctly facilitate communication between services, especially for Kafka brokers and Zookeeper.
8. **S3 Bucket Permissions**: Ensuring correct permissions when writing to S3 is crucial. Misconfigured permissions can prevent Spark from saving data to the bucket.
9. **Deprecation Warnings**: The provided logs show deprecation warnings, indicating that some methods or configurations used might become obsolete in future versions.
10. **PostgreSQL Crash Recovery Delays**: When the `airflow_db` container is restarted after an abrupt shutdown, PostgreSQL can spend a minute or more replaying its write-ahead log. The compose file now gives the database up to 90 seconds before health checks start and increases the retry budget, but if you still see `the database system is starting up` errors you may need to wait a little longer or remove the `airflow_pgdata` directory to let Postgres initialize from scratch.

## Additional Troubleshooting Notes

- **Schema Registry cannot reach Kafka:** The Schema Registry container connects via the `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` value. The compose file now reads the value from `.env`, so double-check that the variable includes the protocol prefix (e.g. `PLAINTEXT://kafka:19092`) and that the `docker_streaming` network exists before starting the stack. If the Schema Registry still refuses connections, confirm the broker is healthy with `docker compose logs kafka`.
- **Kafka logs `Invalid receive (size = 1195725856 ...)`:** This warning means something (often a browser tab or `curl` command) is speaking HTTP to the Kafka listener. The four bytes in the log decode to `GET `, so the broker is just rejecting an HTTP request on a binary port. Remove HTTP-based health checks or curls against `localhost:9092` and use a real Kafka client instead (for example `docker compose exec kafka kafka-broker-api-versions --bootstrap-server kafka:19092`).
- **Spark Native Hadoop warning:** Bitnami's Spark image ships without the native Hadoop bindings, so you will see `WARN NativeCodeLoader: Unable to load native-hadoop library for your platform`. This is expected and Spark falls back to the built-in Java implementation—no action is required unless you specifically need native Hadoop features.

## **Conclusion:**

Throughout this journey, we delved deep into the intricacies of real-world data engineering, progressing from raw, unprocessed data to actionable insights. Beginning with collecting random user data, we harnessed the capabilities of Kafka, Spark, and Airflow to manage, process, and automate the streaming of this data. Docker streamlined the deployment, ensuring a consistent environment, while other tools like S3 and Python played pivotal roles.

This endeavor was more than just constructing a pipeline; it was about understanding the synergy between tools. I encourage all readers to experiment further, adapting and enhancing this pipeline to cater to unique requirements and uncover even more profound insights. Dive in, explore, and innovate!
