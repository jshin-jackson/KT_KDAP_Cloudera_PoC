#!/usr/bin/env bash
# Resolve CDP Flink parcel and run flink-sql-client (jshin) or sql-client.sh.
# Usage (from repo root):
#   ./scripts/flink_sql_client.sh embedded -Djavax.net.ssl.trustStore=... -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
#   ./scripts/flink_sql_client.sh flink list

set -euo pipefail

find_sql_client() {
  local home="${FLINK_HOME:-/opt/cloudera/parcels/FLINK}"
  local candidates=(
    "${home}/bin/flink-sql-client"
    "${home}/bin/sql-client.sh"
    /opt/cloudera/parcels/FLINK/bin/flink-sql-client
    /opt/cloudera/parcels/FLINK/bin/sql-client.sh
    /opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh
  )
  local path
  for path in "${candidates[@]}"; do
    if [[ -x "$path" ]]; then
      echo "$path"
      return 0
    fi
  done

  find /opt/cloudera/parcels -maxdepth 4 \( -name flink-sql-client -o -name sql-client.sh \) -type f 2>/dev/null | head -1
}

SQL_CLIENT="$(find_sql_client || true)"
if [[ -z "$SQL_CLIENT" || ! -x "$SQL_CLIENT" ]]; then
  echo "ERROR: flink-sql-client / sql-client.sh not found under /opt/cloudera/parcels." >&2
  echo "Run: ls /opt/cloudera/parcels/FLINK/bin" >&2
  echo "Set FLINK_HOME=/opt/cloudera/parcels/FLINK (jshin confirmed)." >&2
  exit 1
fi

BIN_DIR="$(dirname "$SQL_CLIENT")"
if [[ "${1:-}" == "flink" ]]; then
  shift
  exec "${BIN_DIR}/flink" "$@"
fi

exec "$SQL_CLIENT" "$@"
