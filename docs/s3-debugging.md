# S3 Delivery Debugging Guide

If the Spark streaming job is running but no files land in the target S3 bucket, work through the following checklist.

## 1. Validate AWS credentials (Root Cause)
The `spark_streaming` container only loads credentials from the `.env.aws` file. Leaving this file empty while populating `.env` prevents Spark from authenticating with S3. To fix this:

1. Copy your AWS access key ID and secret access key into `.env.aws` using the standard AWS environment variable names:
   ```
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...
   AWS_DEFAULT_REGION=...
   ```
2. Recreate or restart the `spark_streaming` container so the environment variables are picked up. With `docker-compose` you can run `docker compose up -d spark_streaming --force-recreate`.
3. Confirm the credentials by opening a shell inside the container and running `aws sts get-caller-identity`. A successful response confirms the fix.

## 2. Bucket/Prefix configuration
Ensure the bucket name and prefix that Spark uses match what you expect:

- Check the `TARGET_S3_BUCKET` and `TARGET_S3_PREFIX` variables in `.env`.
- Verify the Airflow DAG and Spark job configuration files reference the same values.

## 3. Review logs
Inspect logs to ensure messages are being written and consumed:

- Airflow task logs for the name generator DAG confirm records are being produced to Kafka.
- Spark driver logs (`docker compose logs spark_streaming`) show any authentication or write failures.

## 4. Bucket permissions
If credentials are set but writes still fail, confirm the IAM user has the following permissions on the bucket/prefix:

- `s3:PutObject`
- `s3:ListBucket`
- `s3:GetBucketLocation`

Use AWS CLI or the console to review the IAM policy and bucket ACLs.

## 5. Network connectivity
For on-prem or restricted networks, ensure outbound connectivity to S3 endpoints is allowed. Use a simple command like `curl https://s3.<region>.amazonaws.com` from inside the container to verify.

Following this sequence (Credentials → Configuration → Logs → Permissions → Networking) should resolve the missing output objects in S3.
