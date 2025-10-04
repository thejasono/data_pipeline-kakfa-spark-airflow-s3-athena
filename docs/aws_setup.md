# AWS S3 Configuration Guide

Follow these steps to configure the demo stack so it writes streaming output to your AWS S3 bucket.

## 1. Prepare AWS resources
1. **Create or pick an S3 bucket**. Note the bucket name and region (for example `us-east-1`).
2. **Provision credentials**:
   - *Recommended*: attach an IAM role to the compute environment that runs Docker (EC2 instance profile, ECS task role, or EKS IRSA). The Spark job will then rely on the Default AWS Credentials Provider Chain.
   - *Alternative*: create an IAM user with programmatic access and grant it permission to write to the bucket (for example the `AmazonS3FullAccess` policy while testing).

## 2. Update the `.env` file
Edit the root `.env` file so the Spark container receives the correct values:

```dotenv
S3_BUCKET=<your-bucket-name>
S3_REGION=<bucket-region>
S3_ENDPOINT=https://s3.<bucket-region>.amazonaws.com
S3_OUTPUT_PREFIX=names        # or any folder/prefix you prefer
S3_CHECKPOINT_PREFIX=checkpoints/names

# Only required when you use long-lived IAM access keys.
AWS_ACCESS_KEY_ID=<aws_access_key_id>
AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
# Optional when using temporary credentials.
AWS_SESSION_TOKEN=<aws_session_token>

# Alternatively, set the standard AWS variables:
AWS_ACCESS_KEY_ID=<aws_access_key_id>
AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
# Optional when using temporary credentials.
AWS_SESSION_TOKEN=<aws_session_token>

# Leave this unset (or set it to "false") when targeting AWS so Spark uses virtual-host style URLs.
S3_PATH_STYLE_ACCESS=false
```

If you deploy with IAM roles and temporary credentials, leave the access key entries blank so that `spark_processing.py` falls back to the default provider chain. The Spark helper recognises the conventional AWS variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`) and uses them automatically when present. 【F:spark/app/spark_processing.py†L68-L112】

### Using AWS IAM Identity Center (AWS SSO)

When your organisation issues credentials through AWS IAM Identity Center, you do **not** copy the start URL or region into `.env`. Instead, configure the AWS CLI once on your workstation:

1. Run `aws configure sso` and answer the prompts with your Identity Center start URL (for example `https://d-9c675e8e1c.awsapps.com/start/#`) and home region (for example `eu-west-2`).
2. Execute `aws sso login --profile <profile-name>` whenever the cached credentials expire.
3. Mount your local `~/.aws` directory and point the container at that profile by uncommenting `AWS_PROFILE` and the `~/.aws` volume in `docker-compose.yaml`. 【F:docker-compose.yaml†L350-L377】

The Spark job will continue to rely on the Default AWS Credentials Provider Chain, so once the profile is logged in the container picks up the temporary credentials automatically—no additional environment variables are required. 【F:spark/app/spark_processing.py†L82-L102】【F:docker-compose.yaml†L350-L377】

## 3. Restart the stack
Rebuild and restart the containers so the new environment variables are available:

```bash
docker compose down
docker compose up -d --build spark_streaming
```

The `spark_streaming` service will automatically pick up the new settings the next time it submits the job. 【F:docker-compose.yaml†L337-L360】【F:spark/app/spark_processing.py†L165-L214】

## 4. Verify the pipeline
1. Use the Spark UI (`http://localhost:8085`) to confirm the Structured Streaming query is running.
2. Inspect your S3 bucket for newly created newline-delimited JSON files under the configured prefix.
3. Check the container logs if no data arrives. Common causes include missing IAM permissions or a typo in the bucket name.
