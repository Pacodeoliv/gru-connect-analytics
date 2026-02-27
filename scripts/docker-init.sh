#!/usr/bin/env bash
# docker-init.sh — Bootstrap script for Airflow inside Docker Compose.
# Runs during the airflow-init service: migrates DB, creates admin user,
# and sets up the spark_default connection for Cosmos/dbt.
set -euo pipefail

echo "[docker-init] Migrating Airflow database..."
airflow db migrate

echo "[docker-init] Creating admin user..."
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com || true

echo "[docker-init] Setting up spark_default connection..."
airflow connections delete spark_default 2>/dev/null || true
airflow connections add spark_default \
  --conn-type spark \
  --conn-host "spark://spark-master" \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode": "client"}'

echo "[docker-init] Done."
