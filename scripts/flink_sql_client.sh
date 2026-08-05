#!/usr/bin/env bash
# Run Flink SQL Client on CDP CSA.
# Prefers parcel lib/flink/bin/sql-client.sh over bin/flink-sql-client wrapper
# (wrapper fails if lib/flink/bin/sql-client.sh is missing in the parcel).
#
# Usage (from repo root):
#   ./scripts/flink_sql_client.sh embedded -Djavax.net.ssl.trustStore=... \
#     -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
#   ./scripts/flink_sql_client.sh flink list

set -euo pipefail

find_flink_bin_dir() {
  local home="${FLINK_HOME:-/opt/cloudera/parcels/FLINK}"
  if [[ -d "${home}/bin" ]]; then
    echo "${home}/bin"
    return 0
  fi
  local linked
  linked="$(readlink -f /opt/cloudera/parcels/FLINK 2>/dev/null || true)"
  if [[ -n "$linked" && -d "${linked}/bin" ]]; then
    echo "${linked}/bin"
    return 0
  fi
  return 1
}

find_sql_client() {
  # 1) Real sql-client.sh inside CSA parcel (preferred)
  local path
  path="$(find /opt/cloudera/parcels/FLINK-* -path '*/lib/flink/bin/sql-client.sh' -type f 2>/dev/null | head -1)"
  if [[ -n "$path" && -x "$path" ]]; then
    echo "$path"
    return 0
  fi

  path="$(find /opt/cloudera/parcels/FLINK-* -name sql-client.sh -type f 2>/dev/null | head -1)"
  if [[ -n "$path" && -x "$path" ]]; then
    echo "$path"
    return 0
  fi

  local home="${FLINK_HOME:-/opt/cloudera/parcels/FLINK}"
  if [[ -x "${home}/lib/flink/bin/sql-client.sh" ]]; then
    echo "${home}/lib/flink/bin/sql-client.sh"
    return 0
  fi
  if [[ -x "${home}/bin/sql-client.sh" ]]; then
    echo "${home}/bin/sql-client.sh"
    return 0
  fi

  # 2) Last resort: bin/flink-sql-client (wrapper; needs lib/flink/bin/sql-client.sh)
  if [[ -x "${home}/bin/flink-sql-client" ]]; then
    echo "${home}/bin/flink-sql-client"
    return 0
  fi

  return 1
}

print_troubleshoot() {
  echo "ERROR: Flink SQL Client not usable on this host." >&2
  echo "" >&2
  echo "Diagnose:" >&2
  echo "  readlink -f /opt/cloudera/parcels/FLINK" >&2
  echo "  ls -la /opt/cloudera/parcels/FLINK/lib/flink/bin 2>/dev/null || echo 'missing lib/flink/bin'" >&2
  echo "  find /opt/cloudera/parcels/FLINK-* -name sql-client.sh" >&2
  echo "" >&2
  echo "If lib/flink/bin/sql-client.sh is missing, Flink parcel is incomplete." >&2
  echo "Fix: Cloudera Manager → Parcels → Flink → Distribute → Activate (or restart Flink service)." >&2
}

SQL_CLIENT="$(find_sql_client || true)"
if [[ -z "$SQL_CLIENT" || ! -x "$SQL_CLIENT" ]]; then
  print_troubleshoot
  exit 1
fi

BIN_DIR="$(find_flink_bin_dir || dirname "$SQL_CLIENT")"
if [[ "${1:-}" == "flink" ]]; then
  shift
  exec "${BIN_DIR}/flink" "$@"
fi

if [[ "$(basename "$SQL_CLIENT")" == "flink-sql-client" ]]; then
  parcel_root="$(readlink -f /opt/cloudera/parcels/FLINK 2>/dev/null || echo /opt/cloudera/parcels/FLINK)"
  if [[ ! -x "${parcel_root}/lib/flink/bin/sql-client.sh" ]]; then
    echo "ERROR: flink-sql-client wrapper requires ${parcel_root}/lib/flink/bin/sql-client.sh" >&2
    print_troubleshoot
    exit 1
  fi
fi

exec "$SQL_CLIENT" "$@"
