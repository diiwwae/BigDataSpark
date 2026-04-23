#!/usr/bin/env bash
set -euo pipefail

docker compose exec spark /opt/spark/bin/spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/postgresql-42.7.4.jar,/opt/spark/jars/clickhouse-jdbc-0.6.3.jar \
  /workspace/project/spark/jobs/etl_to_star.py
