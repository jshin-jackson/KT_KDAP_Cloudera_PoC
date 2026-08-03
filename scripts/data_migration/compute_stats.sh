#!/usr/bin/env bash
# Compute Impala stats for all kdap PoC tables
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

IMPALA_SHELL="${IMPALA_SHELL:-impala-shell -k --quiet}"
ETL_DATE="${ETL_DATE:-20251201}"
BASE_DATE="${BASE_DATE:-20251201}"

TABLES=(
  "kdap.cdr_sgi"
  "kdap.cdr_sgi_raw"
  "kdap.cdr_mdt_smsng"
  "kdap.cdr_mdt_smsng_raw"
  "kdap.bts_master"
  "kdap.tbm_rpot_m"
  "kdap.tbm_gpot25"
  "kdap.tbm_bdri"
  "kdap.lte_call_fact"
  "kdap.contract_dim"
  "kdap.product_dim"
  "kdap.call_quality_iceberg"
  "kdap.call_quality_kudu"
)

echo "==> Refreshing Iceberg metadata"
for t in "${TABLES[@]}"; do
  echo "  REFRESH ${t}"
  ${IMPALA_SHELL} -q "REFRESH ${t};" 2>/dev/null || true
done

echo "==> COMPUTE STATS"
for t in "${TABLES[@]}"; do
  echo "  COMPUTE STATS ${t}"
  ${IMPALA_SHELL} -q "COMPUTE STATS ${t};" || echo "WARN: stats failed for ${t}"
done

echo "==> Incremental stats on partitioned facts (sample partition)"
${IMPALA_SHELL} -q "
  COMPUTE INCREMENTAL STATS kdap.cdr_sgi PARTITION (etl_date='${ETL_DATE}');
  COMPUTE INCREMENTAL STATS kdap.cdr_mdt_smsng PARTITION (base_date='${BASE_DATE}');
"

echo "==> Done. Verify with: SHOW TABLE STATS kdap.cdr_sgi;"
