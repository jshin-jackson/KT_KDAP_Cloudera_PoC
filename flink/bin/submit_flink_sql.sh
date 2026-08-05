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
    echo "Backend: ssb (sql-client incomplete on this host — bootstrap or use SSB)"
    while IFS= read -r line; do
      echo "  ${line}"
    done < <(sql_client_bootstrap_status)
  fi
fi

if [[ "$BACKEND" == "ssb" ]]; then
  exec python3.11 "${REPO_ROOT}/scripts/ssb_submit_sql.py" "${SQL_ARGS[@]}"
fi

if [[ "$BACKEND" == "sql-client" ]]; then
  if ! sql_client_ready; then
    echo "ERROR: sql-client not available." >&2
    echo "  Bootstrap:" >&2
    echo "    cp flink-1.20.1/bin/sql-client.sh \$CSA_FLINK/bin/" >&2
    echo "    cp flink-1.20.1/opt/flink-sql-client-*.jar \$CSA_FLINK/lib/" >&2
    echo "    cp flink-1.20.1/opt/flink-sql-gateway-*.jar \$CSA_FLINK/lib/" >&2
    echo "    cp iceberg-flink-runtime-1.20-*.jar \$CSA_FLINK/lib/" >&2
    echo "  Do NOT put flink-sql-connector-hive in lib/ (Calcite conflict). Use HIVE_HOME + HADOOP_CLASSPATH." >&2
    echo "  Or script: ./scripts/bootstrap_flink_sql_client.sh /path/to/flink-${FLINK_VERSION:-1.20.1} --target parcel" >&2
    echo "  Or use SSB:  FLINK_SUBMIT_BACKEND=ssb $0 ..." >&2
    exit 1
  fi

  cd "${REPO_ROOT}"
  collect_hive_metastore_jars
  # Flink SQL Client accepts only ONE -f file; multiple -f flags ignore all but the first.
  # Catalog + job: -i (init) for catalog DDL, -f for job SQL. Catalog-only: -f alone.
  CMD=("${SQL_CLIENT_BIN}" embedded "${SSL_OPTS[@]}")
  if [[ ${#HIVE_METASTORE_JARS[@]} -gt 0 ]]; then
    for jar in "${HIVE_METASTORE_JARS[@]}"; do
      if [[ ! -f "$jar" ]]; then
        echo "ERROR: Hive jar not found: $jar" >&2
        exit 1
      fi
      CMD+=(-j "$jar")
    done
    echo "Hive jars (${#HIVE_METASTORE_JARS[@]}): ${HIVE_METASTORE_JARS[*]}"
  else
    echo "ERROR: no CDH Hive jars found for Iceberg HiveCatalog." >&2
    echo "  HIVE_HOME=${HIVE_HOME}" >&2
    echo "  searched:" >&2
    for dir in "${HIVE_JAR_SEARCH_DIRS[@]}"; do
      echo "    - ${dir}" >&2
    done
    echo "  hint: ls /opt/cloudera/parcels/CDH/jars/hive-*.jar" >&2
    echo "  or:  FLINK_SUBMIT_BACKEND=ssb $0 ..." >&2
    exit 1
  fi
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
