#!/usr/bin/env bash
# Ensure YARN session, then submit Catalog + Flink SQL job file.
#
# Usage (from repo root):
#   ./flink/bin/submit_flink_sql.sh flink/ltas_5min.sql
#   ./flink/bin/submit_flink_sql.sh flink/conf/00_catalog_setup_jshin.sql   # catalog only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <flink-sql-file> [extra-sql-file ...]" >&2
  exit 1
fi

SQL_FILES=()
for arg in "$@"; do
  if [[ -f "$arg" ]]; then
    SQL_FILES+=("$arg")
  elif [[ -f "${REPO_ROOT}/${arg}" ]]; then
    SQL_FILES+=("${REPO_ROOT}/${arg}")
  else
    echo "ERROR: SQL file not found: $arg" >&2
    exit 1
  fi
done

if ! flink_yarn_session_running; then
  echo "Flink YARN session not found — starting..."
  "${SCRIPT_DIR}/start_yarn_session.sh"
fi

cd "${REPO_ROOT}"

CMD=("${FLINK_BIN}/flink-sql-client" embedded "${SSL_OPTS[@]}")
if [[ "$(basename "${SQL_FILES[0]}")" != "00_catalog_setup_jshin.sql" && "$(basename "${SQL_FILES[0]}")" != "00_catalog_setup.sql" ]]; then
  CMD+=(-f "${CATALOG_SQL}")
fi
for sql in "${SQL_FILES[@]}"; do
  CMD+=(-f "$sql")
done

echo "Submitting: ${CMD[*]}"
exec "${CMD[@]}"
