#!/usr/bin/env bash
# Shared benchmark utilities for KT KDAP PoC
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_BASE="${RESULTS_BASE:-${REPO_ROOT}/docs/results}"
RUN_ID="${RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}"
RUN_DIR="${RESULTS_BASE}/${RUN_ID}"

ETL_DATE="${ETL_DATE:-20251201}"
BASE_DATE="${BASE_DATE:-20251201}"
BASE_DATE_PREFIX="${BASE_DATE_PREFIX:-202507}"

IMPALA_SHELL="${IMPALA_SHELL:-impala-shell -k --quiet}"
IMPALA_SHELL_CDW="${IMPALA_SHELL_CDW:-impala-shell -k --quiet -i cdw-vw-impala.example.com:21050}"

mkdir -p "${RUN_DIR}/profiles" "${RUN_DIR}/logs"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${RUN_DIR}/benchmark.log"; }

substitute_vars() {
  local file="$1"
  sed -e "s/\${ETL_DATE}/${ETL_DATE}/g" \
      -e "s/\${BASE_DATE}/${BASE_DATE}/g" \
      -e "s/\${BASE_DATE_PREFIX}/${BASE_DATE_PREFIX}/g" \
      -e "s/\${LOOKUP_KEY}/${LOOKUP_KEY:-SAMPLE_KEY}/g" \
      -e "s/\${QUERY_ID}/${QUERY_ID:-q}/g" \
      "${file}"
}

run_query_timed() {
  local label="$1"
  local sql_file="$2"
  local shell="${3:-${IMPALA_SHELL}}"
  local out_sql="${RUN_DIR}/logs/${label}.sql"
  local out_time="${RUN_DIR}/${label}_timing.txt"

  substitute_vars "${sql_file}" > "${out_sql}"
  log "START ${label} ($(basename "${sql_file}"))"

  local start end elapsed
  start=$(date +%s.%N)
  ${shell} -f "${out_sql}" > "${RUN_DIR}/logs/${label}_output.txt" 2>&1 || {
    log "FAIL ${label}"
    echo "${label}|FAILED|0" >> "${RUN_DIR}/summary.csv"
    return 1
  }
  end=$(date +%s.%N)
  elapsed=$(echo "${end} - ${start}" | bc)

  log "END ${label}: ${elapsed}s"
  echo "${label}|SUCCESS|${elapsed}" >> "${RUN_DIR}/summary.csv"
  echo "${label},${elapsed},$(date -Iseconds)" >> "${out_time}"
}

collect_profile() {
  local label="$1"
  local query_id="$2"
  local shell="${3:-${IMPALA_SHELL}}"
  log "Collecting profile for ${label} query_id=${query_id}"
  ${shell} -q "PROFILE ${query_id}" > "${RUN_DIR}/profiles/${label}.profile.txt" 2>&1 || true
}

init_summary() {
  echo "label|status|elapsed_sec" > "${RUN_DIR}/summary.csv"
  log "Run directory: ${RUN_DIR}"
}

export REPO_ROOT RESULTS_BASE RUN_ID RUN_DIR ETL_DATE BASE_DATE BASE_DATE_PREFIX
export IMPALA_SHELL IMPALA_SHELL_CDW
