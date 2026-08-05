#!/usr/bin/env bash
# Submit Catalog + Flink SQL via CSA-supported backends.
#
# Backends (FLINK_SUBMIT_BACKEND):
#   auto       try sql-client (parcel/vendor), else SSB REST API (default)
#   sql-client Apache Flink SQL Client (-f SQL files)
#   ssb        SSB REST API (Web UI와 동일)
#
# Usage (from repo root):
#   kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
#   ./flink/bin/submit_flink_sql.sh flink/ltas_5min.sql

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"
# shellcheck source=resolve_sql_client.sh
source "${SCRIPT_DIR}/resolve_sql_client.sh"

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

BACKEND="${FLINK_SUBMIT_BACKEND:-auto}"

if [[ "$BACKEND" == "auto" ]]; then
  if sql_client_ready; then
    BACKEND="sql-client"
    echo "Backend: sql-client (${SQL_CLIENT_SOURCE})"
  else
    BACKEND="ssb"
    echo "Backend: ssb (sql-client not bootstrapped — copy sql-client.sh + jar to CSA_FLINK/lib/flink)"
  fi
fi

if [[ "$BACKEND" == "ssb" ]]; then
  exec python3.11 "${REPO_ROOT}/scripts/ssb_submit_sql.py" "${SQL_ARGS[@]}"
fi

if [[ "$BACKEND" == "sql-client" ]]; then
  if ! sql_client_ready; then
    echo "ERROR: sql-client not available." >&2
    echo "  Bootstrap: cp flink-1.20.1/bin/sql-client.sh \$CSA_FLINK/bin/" >&2
    echo "             cp flink-1.20.1/opt/flink-sql-client-*.jar \$CSA_FLINK/lib/" >&2
    echo "             cp flink-1.20.1/opt/flink-sql-gateway-*.jar \$CSA_FLINK/lib/" >&2
    echo "  Or script: ./scripts/bootstrap_flink_sql_client.sh /path/to/flink-${FLINK_VERSION:-1.20.1} --target parcel" >&2
    echo "  Or use SSB:  FLINK_SUBMIT_BACKEND=ssb $0 ..." >&2
    exit 1
  fi

  cd "${REPO_ROOT}"
  CMD=("${SQL_CLIENT_BIN}" embedded "${SSL_OPTS[@]}")
  if [[ "$(basename "${SQL_ARGS[0]}")" != "00_catalog_setup_jshin.sql" && "$(basename "${SQL_ARGS[0]}")" != "00_catalog_setup.sql" ]]; then
    CMD+=(-f "${CATALOG_SQL}")
  fi
  for sql in "${SQL_ARGS[@]}"; do
    CMD+=(-f "$sql")
  done
  echo "Submitting (${SQL_CLIENT_SOURCE}): ${CMD[*]}"
  exec "${CMD[@]}"
fi

echo "ERROR: unknown FLINK_SUBMIT_BACKEND=${BACKEND} (use auto, sql-client, or ssb)" >&2
exit 1
