#!/usr/bin/env bash
# Scenario 1: Large-scale batch — single or concurrent execution
# Usage: ./run_scenario1_batch.sh [single|concurrent]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

MODE="${1:-single}"
init_summary

BATCH_QUERIES=(
  "s1_ltas:sql/batch/WF_N02960_TMP_SUM_LTAS_CDR_TM.sql"
  "s1_sgi_v1:sql/batch/WF_M07046_TFM_S1AP_SGI_USE_DD_v1.sql"
  "s1_sgi_v2:sql/batch/WF_M07046_TFM_S1AP_SGI_USE_DD_v2.sql"
  "s1_mdt:sql/batch/WF_N05923_CNV_MDT_MERGE.sql"
)

RESOURCE_POOLS=(
  "pool_ltas_batch"
  "pool_sgi_batch_1"
  "pool_sgi_batch_2"
  "pool_mdt_batch"
)

run_single() {
  for entry in "${BATCH_QUERIES[@]}"; do
    label="${entry%%:*}"
    sql="${REPO_ROOT}/${entry#*:}"
    run_query_timed "${label}" "${sql}" "${IMPALA_SHELL}"
  done
}

run_concurrent() {
  local pids=()
  local i=0
  for entry in "${BATCH_QUERIES[@]}"; do
    label="${entry%%:*}"
    sql="${REPO_ROOT}/${entry#*:}"
    pool="${RESOURCE_POOLS[$i]}"
    (
      export IMPALA_SHELL="${IMPALA_SHELL} --var=REQUEST_POOL=${pool}"
      run_query_timed "${label}_concurrent" "${sql}" "${IMPALA_SHELL}"
    ) &
    pids+=($!)
    ((i++)) || true
  done
  for pid in "${pids[@]}"; do
    wait "${pid}" || log "WARN: job ${pid} failed"
  done
}

case "${MODE}" in
  single) run_single ;;
  concurrent) run_concurrent ;;
  *)
    echo "Usage: $0 [single|concurrent]"
    exit 1
    ;;
esac

log "Scenario 1 complete. Results: ${RUN_DIR}/summary.csv"
log "Collect Query Profiles from Impala UI or: scripts/profile/collect_profiles.sh ${RUN_DIR}"
