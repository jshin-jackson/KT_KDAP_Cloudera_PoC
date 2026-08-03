#!/usr/bin/env bash
# Distcp + Iceberg migration helper for KT PoC data (15-20 TB)
set -euo pipefail

SOURCE_NN="${SOURCE_NN:?Set SOURCE_NN e.g. hdfs://kt-source:8020}"
TARGET_WAREHOUSE="${TARGET_WAREHOUSE:-hdfs://kdap/user/kdap/staging}"
ETL_DATE="${ETL_DATE:-20251201}"
BASE_DATE="${BASE_DATE:-20251201}"
PARALLEL="${PARALLEL:-16}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

distcp_table() {
  local src_path="$1"
  local dst_path="$2"
  log "distcp ${src_path} -> ${dst_path}"
  hadoop distcp -Dmapreduce.job.queuename=root.kdap_migration \
    -m "${PARALLEL}" -bandwidth 100 \
    "${SOURCE_NN}${src_path}" "${TARGET_WAREHOUSE}${dst_path}"
}

# Fact tables — update paths from data-intake-checklist
distcp_table "/data/cdr_sgi/etl_date=${ETL_DATE}" "/cdr_sgi/etl_date=${ETL_DATE}"
distcp_table "/data/cdr_mdt/base_date=${BASE_DATE}" "/cdr_mdt/base_date=${BASE_DATE}"
distcp_table "/data/dim/bts_master" "/dim/bts_master"

log "Post-distcp: run Iceberg CTAS or spark write from staging"
log "  impala-shell -f sql/ddl/01_iceberg_fact_tables.sql"
log "  ./scripts/data_migration/compute_stats.sh"
