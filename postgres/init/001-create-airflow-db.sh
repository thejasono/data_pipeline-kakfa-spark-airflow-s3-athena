#!/bin/bash
set -euo pipefail

# Ensure the metadata database exists for Airflow. This script is idempotent
# so it can run on every container start without failing when the database is
# already present.

DB_NAME=${POSTGRES_DB:-airflow}
DB_USER=${POSTGRES_USER:-postgres}
DEFAULT_DB=postgres

EXISTS=$(psql -U "$DB_USER" -d "$DEFAULT_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'")
if [[ "$EXISTS" != "1" ]]; then
  echo "Creating database '${DB_NAME}' for Airflow metadata"
  psql -U "$DB_USER" -d "$DEFAULT_DB" -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${DB_NAME}\""
else
  echo "Database '${DB_NAME}' already exists"
fi
