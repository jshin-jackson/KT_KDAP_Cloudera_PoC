#!/usr/bin/env bash
# Submit Flink SQL via Cloudera SQL Stream Builder (SSB) REST API.
#
# Usage (from repo root, after kinit):
#   cp .env.example .env   # set SSB_API_BASE from SSB Web UI → API Explorer
#   ./flink/bin/submit_flink_sql.sh flink/ltas_5min.sql
#   ./flink/run_ltas_5min.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <flink-sql-file> [extra-sql-file ...]" >&2
  exit 1
fi

SQL_ARGS=()
for arg in "$@"; do
  if [[ -f "$arg" ]]; then
    SQL_ARGS+=("$arg")
  elif [[ -f "${REPO_ROOT}/${arg}" ]]; then
    SQL_ARGS+=("${REPO_ROOT}/${arg}")
  else
    echo "ERROR: SQL file not found: $arg" >&2
    exit 1
  fi
done

exec python3.11 "${REPO_ROOT}/scripts/ssb_submit_sql.py" "${SQL_ARGS[@]}"
