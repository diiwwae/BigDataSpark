#!/usr/bin/env bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
TRUNCATE TABLE raw.mock_data;
SQL

find /data/mock_data -maxdepth 1 -name '*.csv' -print0 | sort -z | while IFS= read -r -d '' file; do
  echo "Loading $file into raw.mock_data"
  escaped_file=${file//\'/\'\'}
  psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -c "COPY raw.mock_data FROM '$escaped_file' WITH (FORMAT csv, HEADER true, QUOTE '\"')"
done
