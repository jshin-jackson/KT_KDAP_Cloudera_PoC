#!/usr/bin/env bash
# Validate row counts after data migration
set -euo pipefail

IMPALA_SHELL="${IMPALA_SHELL:-impala-shell -k -B --quiet}"
ETL_DATE="${ETL_DATE:-20251201}"
BASE_DATE="${BASE_DATE:-20251201}"
OUTPUT="${1:-docs/results/validation_row_counts.txt}"

mkdir -p "$(dirname "${OUTPUT}")"

queries=(
  "SELECT 'cdr_sgi' AS tbl, COUNT(*) FROM kdap.cdr_sgi WHERE etl_date='${ETL_DATE}'"
  "SELECT 'cdr_mdt_smsng' AS tbl, COUNT(*) FROM kdap.cdr_mdt_smsng WHERE base_date='${BASE_DATE}'"
  "SELECT 'bts_master' AS tbl, COUNT(*) FROM kdap.bts_master"
  "SELECT 'call_quality_kudu' AS tbl, COUNT(*) FROM kdap.call_quality_kudu"
  "SELECT 'call_quality_iceberg' AS tbl, COUNT(*) FROM kdap.call_quality_iceberg WHERE base_date='${BASE_DATE}'"
)

{
  echo "# Row count validation $(date -Iseconds)"
  echo "# ETL_DATE=${ETL_DATE} BASE_DATE=${BASE_DATE}"
  echo "table|count|expected|status"
  for q in "${queries[@]}"; do
    tbl=$(echo "$q" | sed "s/.*'\\([^']*\\)'.*/\\1/")
    cnt=$(${IMPALA_SHELL} -q "${q}" 2>/dev/null | tail -1 || echo "ERROR")
    echo "${tbl}|${cnt}|TBD|pending"
  done
} | tee "${OUTPUT}"

echo "Validation written to ${OUTPUT}"
