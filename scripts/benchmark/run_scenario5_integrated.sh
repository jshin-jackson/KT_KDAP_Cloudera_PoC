#!/usr/bin/env bash
# Scenario 5: Integrated workload — S1 batch + S2 Flink + S3 MSTR concurrent
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

init_summary
log "Starting integrated workload test (Scenario 5)"

# Start Flink jobs (if not already running)
FLINK_JOBS=(
  "flink/ltas_5min.sql"
  "flink/sgi_5min_v1.sql"
  "flink/mdt_5min.sql"
)

for job in "${FLINK_JOBS[@]}"; do
  log "Submit Flink job: ${job}"
  "${REPO_ROOT}/flink/bin/submit_flink_sql.sh" "${job}" || true
  echo "flink_job_submitted:${job}" >> "${RUN_DIR}/summary.csv"
done

# Concurrent batch on Base (Scenario 1)
(
  export RUN_ID="${RUN_ID}_s1"
  "${SCRIPT_DIR}/run_scenario1_batch.sh" concurrent
) &
PID_S1=$!

# Concurrent MSTR on CDW (Scenario 3)
(
  export RUN_ID="${RUN_ID}_s3"
  "${SCRIPT_DIR}/run_scenario3_mstr.sh" --target cdw --mode concurrent
) &
PID_S3=$!

# Optional: MSTR on Base simultaneously (Architecture req #5)
(
  export RUN_ID="${RUN_ID}_s3_base"
  "${SCRIPT_DIR}/run_scenario3_mstr.sh" --target base --mode concurrent
) &
PID_S3B=$!

log "Waiting for integrated run (batch + MSTR CDW + MSTR Base)..."
wait "${PID_S1}" || log "WARN: S1 failed"
wait "${PID_S3}" || log "WARN: S3 CDW failed"
wait "${PID_S3B}" || log "WARN: S3 Base failed"

# Collect cluster metrics snapshot
"${REPO_ROOT}/scripts/monitoring/snapshot_metrics.sh" "${RUN_DIR}/metrics"

log "Scenario 5 complete: ${RUN_DIR}"
