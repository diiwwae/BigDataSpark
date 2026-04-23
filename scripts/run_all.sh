#!/usr/bin/env bash
set -euo pipefail

./scripts/run_star.sh
./scripts/run_clickhouse.sh
