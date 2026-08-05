#!/usr/bin/env bash
# Submit Catalog + Flink SQL via CSA-supported backends.
#
# Backends (FLINK_SUBMIT_BACKEND):
#   auto       sql-client when bootstrapped (default); else SSB if SSB_API_BASE set
#   sql-client Apache Flink SQL Client — requires flink-sql-connector-hive in CSA lib/
#   ssb        SSB REST API (optional; only if SQL Stream Builder is installed)
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

load_repo_dotenv
BACKEND="${FLINK_SUBMIT_BACKEND:-auto}"

if [[ "$BACKEND" == "auto" ]]; then
  if sql_client_ready; then
    BACKEND="sql-client"
    echo "Backend: sql-client (${SQL_CLIENT_SOURCE})"
  elif ssb_configured; then
    BACKEND="ssb"
    echo "Backend: ssb (sql-client not ready; using SSB_API_BASE)"
  else
    echo "ERROR: no submit backend available." >&2
    while IFS= read -r line; do
      echo "  ${line}" >&2
    done < <(sql_client_bootstrap_status)
    exit 1
  fi
fi

if [[ "$BACKEND" == "ssb" ]]; then
  exec python3.11 "${REPO_ROOT}/scripts/ssb_submit_sql.py" "${SQL_ARGS[@]}"
fi

if [[ "$BACKEND" == "sql-client" ]]; then
  if ! sql_client_ready; then
    echo "ERROR: sql-client not available." >&2
    echo "  Bootstrap: ./scripts/bootstrap_flink_sql_client.sh /path/to/flink-${FLINK_VERSION:-1.20.1} --target parcel" >&2
    exit 1
  fi

  if ! hive_connector_in_lib; then
    echo "ERROR: flink-sql-connector-hive missing in ${CSA_FLINK_LIB}/lib/" >&2
    echo "  Iceberg HiveCatalog needs this jar (embedded gateway ignores CDH -j)." >&2
    echo "  Install:" >&2
    echo "    cp ~/flink-1.20.1/opt/flink-sql-connector-hive-*.jar ${CSA_FLINK_LIB}/lib/" >&2
    echo "    chown flink:flink ${CSA_FLINK_LIB}/lib/flink-sql-connector-hive-*.jar" >&2
    echo "  submit uses -Dclassloader.resolve-order=parent-first to avoid INSERT Calcite conflict." >&2
    exit 1
  fi

  cd "${REPO_ROOT}"
  CMD=("${SQL_CLIENT_BIN}" embedded "${FLINK_JVM_OPTS[@]}" "${SSL_OPTS[@]}")
  echo "Hive catalog: flink-sql-connector-hive in lib/ (classloader.resolve-order=parent-first)"

  catalog_sql=""
  job_files=()
  for sql in "${SQL_ARGS[@]}"; do
    base="$(basename "$sql")"
    if [[ "$base" == "00_catalog_setup_jshin.sql" || "$base" == "00_catalog_setup.sql" ]]; then
      catalog_sql="$sql"
    else
      job_files+=("$sql")
    fi
  done
  if [[ -z "$catalog_sql" && ${#job_files[@]} -gt 0 ]]; then
    catalog_sql="${CATALOG_SQL}"
  fi
  if [[ -n "$catalog_sql" && ${#job_files[@]} -gt 0 ]]; then
    CMD+=(-i "$catalog_sql")
    for sql in "${job_files[@]}"; do
      CMD+=(-f "$sql")
    done
  elif [[ -n "$catalog_sql" ]]; then
    CMD+=(-f "$catalog_sql")
  else
    for sql in "${job_files[@]}"; do
      CMD+=(-f "$sql")
    done
  fi
  echo "Submitting (${SQL_CLIENT_SOURCE}): ${CMD[*]}"
  exec "${CMD[@]}"
fi

echo "ERROR: unknown FLINK_SUBMIT_BACKEND=${BACKEND} (use auto, sql-client, or ssb)" >&2
exit 1
