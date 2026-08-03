#!/usr/bin/env bash
# Scenario 2: Validate Flink 5-min window output vs batch results
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMPALA_SHELL="${IMPALA_SHELL:-impala-shell -k -B --quiet}"
WINDOW_START="${WINDOW_START:-2025-12-01 12:45:00}"
ETL_DATE="${ETL_DATE:-20251201}"
OUTPUT="${1:-docs/results/flink_validation.csv}"

mkdir -p "$(dirname "${OUTPUT}")"

compare_counts() {
  local label="$1"
  local batch_sql="$2"
  local flink_sql="$3"
  local batch_cnt flink_cnt
  batch_cnt=$(${IMPALA_SHELL} -q "${batch_sql}" 2>/dev/null | tail -1)
  flink_cnt=$(${IMPALA_SHELL} -q "${flink_sql}" 2>/dev/null | tail -1)
  local diff="N/A"
  if [[ "${batch_cnt}" =~ ^[0-9]+$ && "${flink_cnt}" =~ ^[0-9]+$ ]]; then
    diff=$((flink_cnt - batch_cnt))
  fi
  echo "${label},${batch_cnt},${flink_cnt},${diff}"
}

{
  echo "job,batch_count,flink_count,diff"
  compare_counts "ltas" \
    "SELECT COUNT(*) FROM kdap.sum_ltas_cdr_tm WHERE etl_date='${ETL_DATE}'" \
    "SELECT SUM(record_cnt) FROM kdap.ltas_5min_flink WHERE window_start='${WINDOW_START}'"
  compare_counts "sgi" \
    "SELECT COUNT(*) FROM kdap.tfm_s1ap_sgi_use_dd_v1 WHERE etl_date='${ETL_DATE}'" \
    "SELECT SUM(record_cnt) FROM kdap.sgi_5min_flink WHERE window_start='${WINDOW_START}'"
  compare_counts "mdt" \
    "SELECT COUNT(*) FROM kdap.cnv_mdt_merge_stg WHERE base_date='${ETL_DATE}'" \
    "SELECT SUM(record_cnt) FROM kdap.mdt_5min_flink WHERE window_start='${WINDOW_START}'"
} | tee "${OUTPUT}"

echo "Flink validation written to ${OUTPUT}"
