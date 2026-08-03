#!/usr/bin/env bash
# Collect Impala query profiles for a benchmark run directory
# Usage: ./collect_profiles.sh [run_dir]
set -euo pipefail

RUN_DIR="${1:-docs/results/latest}"
IMPALA_SHELL="${IMPALA_SHELL:-impala-shell -k --quiet}"
PROFILE_DIR="${RUN_DIR}/profiles"
mkdir -p "${PROFILE_DIR}"

echo "Fetching recent queries from Impala..."
${IMPALA_SHELL} -q "SELECT query_id, stmt, start_time, end_time, duration_ms/1000 AS duration_sec
  FROM sys.impala_queries
  WHERE start_time > hours_sub(now(), 24)
  ORDER BY start_time DESC
  LIMIT 50;" -o csv > "${PROFILE_DIR}/recent_queries.csv" 2>/dev/null || true

if [[ -f "${PROFILE_DIR}/recent_queries.csv" ]]; then
  tail -n +2 "${PROFILE_DIR}/recent_queries.csv" | while IFS=, read -r qid _rest; do
    safe_id=$(echo "${qid}" | tr -cd 'a-zA-Z0-9_')
    [[ -z "${safe_id}" ]] && continue
    echo "Profile: ${safe_id}"
    ${IMPALA_SHELL} -q "PROFILE '${qid}'" > "${PROFILE_DIR}/profile_${safe_id}.txt" 2>&1 || true
  done
fi

echo "Profiles saved to ${PROFILE_DIR}"
echo "Analysis checklist:"
echo "  - Scan bytes/rows, partition pruning"
echo "  - Join type (broadcast/hash), cardinality"
echo "  - Memory peak, spill to disk"
echo "  - HDFS throughput / Kudu scan mode"
