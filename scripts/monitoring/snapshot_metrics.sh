#!/usr/bin/env bash
# Snapshot YARN/Impala metrics during integrated test
set -euo pipefail

OUT_DIR="${1:-docs/results/metrics_snapshot}"
RM_URL="${YARN_RM_URL:-http://kdap-base-m1.example.com:8088}"
mkdir -p "${OUT_DIR}"

ts=$(date +%Y%m%d_%H%M%S)

curl -sf "${RM_URL}/ws/v1/cluster/metrics" -o "${OUT_DIR}/yarn_metrics_${ts}.json" 2>/dev/null || \
  echo "WARN: YARN metrics unavailable" > "${OUT_DIR}/yarn_metrics_${ts}.txt"

curl -sf "${RM_URL}/ws/v1/cluster/scheduler" -o "${OUT_DIR}/yarn_scheduler_${ts}.json" 2>/dev/null || true

impala-shell -k -q "SELECT pool, total_admitted, total_rejected, mem_limit
  FROM sys.impala_resource_pool_usage;" -o csv \
  > "${OUT_DIR}/impala_pools_${ts}.csv" 2>/dev/null || true

echo "Metrics snapshot: ${OUT_DIR}"
