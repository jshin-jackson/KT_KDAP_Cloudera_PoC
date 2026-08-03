#!/usr/bin/env bash
# Scenario 4: Kudu vs Iceberg key lookup A/B benchmark
# Target: 2-3 sec per lookup (p95)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

ITERATIONS="${ITERATIONS:-100}"
CONCURRENCY="${CONCURRENCY:-1}"
LOOKUP_KEYS_FILE="${LOOKUP_KEYS_FILE:-${REPO_ROOT}/config/sample_lookup_keys.txt}"

init_summary

if [[ ! -f "${LOOKUP_KEYS_FILE}" ]]; then
  log "Generating sample lookup keys file"
  mkdir -p "$(dirname "${LOOKUP_KEYS_FILE}")"
  for i in $(seq 1 "${ITERATIONS}"); do
    printf "LOOKUP_KEY_%05d\n" "$i"
  done > "${LOOKUP_KEYS_FILE}"
fi

run_lookup_ab() {
  local engine="$1"
  local sql_file="$2"
  local times_file="${RUN_DIR}/${engine}_lookup_times.csv"
  echo "iteration,elapsed_sec,key" > "${times_file}"

  local n=0
  while IFS= read -r key && [[ $n -lt ${ITERATIONS} ]]; do
    export LOOKUP_KEY="${key}"
    local out_sql="${RUN_DIR}/logs/${engine}_${n}.sql"
    substitute_vars "${sql_file}" > "${out_sql}"
    local start end elapsed
    start=$(date +%s.%N)
    ${IMPALA_SHELL} -f "${out_sql}" > /dev/null 2>&1 || elapsed=-1
    end=$(date +%s.%N)
    if [[ "${elapsed:-}" != "-1" ]]; then
      elapsed=$(echo "${end} - ${start}" | bc)
    fi
    echo "${n},${elapsed},${key}" >> "${times_file}"
    ((n++)) || true
  done < "${LOOKUP_KEYS_FILE}"

  # Summary stats
  awk -F, 'NR>1 && $2>=0 {sum+=$2; cnt++; if($2>max||max=="")max=$2} END {
    if(cnt>0) printf "%s avg=%.3fs max=%.3fs n=%d\n", "'"${engine}"'", sum/cnt, max, cnt
  }' "${times_file}" | tee -a "${RUN_DIR}/summary.csv"
}

log "A/B lookup test: ${ITERATIONS} iterations, concurrency=${CONCURRENCY}"
run_lookup_ab "kudu" "${REPO_ROOT}/sql/lookup/lookup_call_quality_kudu.sql"
run_lookup_ab "iceberg" "${REPO_ROOT}/sql/lookup/lookup_call_quality_iceberg.sql"

log "Scenario 4 complete. Compare ${RUN_DIR}/kudu_lookup_times.csv vs iceberg_lookup_times.csv"
log "Target: p95 <= 3.0 sec"
