# Validating JSON File Integrity with Amazon Athena

## Overview
Amazon Athena is a serverless, interactive query service that lets you analyze data directly in Amazon Simple Storage Service (Amazon S3) using standard SQL. Because Athena is serverless, there is nothing to provision or manage, and you are billed only for the queries that you run. This makes it an excellent option for quickly verifying the integrity of flat files—such as JSON payloads—stored in S3 without needing to spin up a dedicated cluster or build complex ETL pipelines.

This guide walks through the process of querying JSON files with Athena to ensure they conform to the expected schema and contain valid data. The steps include creating a database, defining an external table that references the JSON objects through the OpenX JSON SerDe, and running validation queries to spot malformed records or data quality issues.

## Prerequisites
- An AWS account with permissions to use Amazon Athena and access to the target S3 bucket.
- JSON files uploaded to `s3://namegeneratorbucket/names/` that follow the schema described below.
- The AWS Glue Data Catalog enabled for Athena, which stores database and table metadata.

## Step 1 – Create the Athena Database
Create a logical namespace for the table definitions that will point to the JSON data. Databases in Athena map directly to entries in the AWS Glue Data Catalog.

```sql
CREATE DATABASE IF NOT EXISTS streaming_demo;
```

If the database already exists, the `IF NOT EXISTS` clause prevents the statement from failing.

## Step 2 – Create an External Table with JSON SerDe
Use an external table to describe the structure of the JSON payloads. Athena relies on a serializer/deserializer (SerDe) to translate between the JSON documents in S3 and relational rows that SQL can query. The `org.openx.data.jsonserde.JsonSerDe` class is a widely used SerDe implementation that supports flexible JSON structures and optional properties while ignoring malformed records when configured appropriately.

```sql
CREATE EXTERNAL TABLE streaming_demo.names_stream (
  name        string,
  gender      string,
  address     string,
  city        string,
  nation      string,
  zip         string,
  latitude    double,
  longitude   double,
  email       string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
LOCATION 's3://namegeneratorbucket/names/';
```

Key points:
- **Schema definition**: Matches each expected field in the JSON payload to a column and data type. When a JSON property is missing, Athena will return `NULL` for that column.
- **SerDe configuration**: The OpenX JSON SerDe understands nested and sparse JSON. Setting `ignore.malformed.json` to `true` skips invalid JSON objects so a single bad record will not fail the entire query. You can remove this property to surface parsing errors when validating new feeds.
- **External table**: Because the table is external, dropping it does not delete the underlying S3 data.

## Step 3 – Run Validation Queries
Run SQL queries to sample and inspect the data. For example, this query orders the first twenty records alphabetically by name so that you can spot anomalies quickly.

```sql
SELECT * FROM streaming_demo.names_stream ORDER BY name LIMIT 20;
```

## Additional Integrity Checks
Beyond the initial sample, use these queries to assess data quality:

- **Row count**: `SELECT COUNT(*) FROM streaming_demo.names_stream;` confirms that Athena can read every JSON object in the location.
- **Malformed records**: Temporarily remove `ignore.malformed.json` or query the CloudWatch logs for warnings emitted by the JSON SerDe to identify files that fail to parse.
- **Null/blank field detection**: `SELECT * FROM streaming_demo.names_stream WHERE name IS NULL OR name = '';` ensures required fields are populated.
- **Duplicate detection**: `SELECT email, COUNT(*) FROM streaming_demo.names_stream GROUP BY email HAVING COUNT(*) > 1;` helps flag duplicate records when a column should be unique.
- **Geolocation validation**: `SELECT * FROM streaming_demo.names_stream WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180;` verifies coordinate ranges.

## Operational Tips
- Partition the table (for example, by ingestion date) if you ingest large volumes of JSON files. Partition pruning speeds up validation queries and reduces costs.
- Use AWS Glue crawlers to auto-discover schema changes and update the table definition when the JSON structure evolves.
- Store query results in a dedicated S3 bucket configured in the Athena console so that audit logs and validation snapshots are preserved.
- Integrate Athena queries into automated data quality checks (e.g., AWS Step Functions or Lambda) to continuously monitor incoming files.

By following these steps, you can leverage Athena's serverless SQL engine and JSON SerDe support to validate the integrity of JSON payloads stored in Amazon S3 quickly and cost-effectively.
