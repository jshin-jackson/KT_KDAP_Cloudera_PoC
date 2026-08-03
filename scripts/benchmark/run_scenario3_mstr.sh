#!/usr/bin/env bash
# Scenario 3: MSTR queries on Base Impala or CDW Impala
# Usage: ./run_scenario3_mstr.sh --target [base|cdw] [--mode single|concurrent]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

TARGET="base"
MODE="single"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [[ "${TARGET}" == "cdw" ]]; then
  SHELL="${IMPALA_SHELL_CDW}"
  POOL_PREFIX="pool_mstr_cdw"
else
  SHELL="${IMPALA_SHELL}"
  POOL_PREFIX="pool_mstr_base"
fi

init_summary

MSTR_QUERIES=(
  "mstr_rsrp:sql/mstr/mstr_lte_rsrp_bad_rate.sql"
  "mstr_volte:sql/mstr/mstr_volte_drop_rate.sql"
  "mstr_q03:sql/mstr/mstr_query_03.sql"
  "mstr_q04:sql/mstr/mstr_query_04.sql"
)

run_mstr() {
  local suffix="$1"
  for entry in "${MSTR_QUERIES[@]}"; do
    label="${entry%%:*}${suffix}"
    sql="${REPO_ROOT}/${entry#*:}"
    QUERY_ID="${label}"
    run_query_timed "${label}" "${sql}" "${SHELL}"
  done
}

if [[ "${MODE}" == "concurrent" ]]; then
  pids=()
  i=0
  for entry in "${MSTR_QUERIES[@]}"; do
    label="${entry%%:*}_concurrent"
    sql="${REPO_ROOT}/${entry#*:}"
    (
      export IMPALA_SHELL="${SHELL} --var=REQUEST_POOL=${POOL_PREFIX}"
      run_query_timed "${label}" "${sql}" "${SHELL}"
    ) &
    pids+=($!)
    ((i++)) || true
  done
  for pid in "${pids[@]}"; do wait "${pid}" || true; done
else
  run_mstr ""
fi

log "Scenario 3 (${TARGET}/${MODE}) complete: ${RUN_DIR}/summary.csv"
