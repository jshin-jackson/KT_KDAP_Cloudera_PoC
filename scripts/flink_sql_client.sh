#!/usr/bin/env bash
# Run Flink SQL Client on CDP CSA (parcel or Apache vendor copy).
#
# Bootstrap first (once):
#   ./scripts/bootstrap_flink_sql_client.sh /path/to/flink-1.20.1
#
# Usage (from repo root):
#   ./scripts/flink_sql_client.sh embedded -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
#   ./scripts/flink_sql_client.sh flink list

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../flink/bin/common.sh
source "${REPO_ROOT}/flink/bin/common.sh"
# shellcheck source=../flink/bin/resolve_sql_client.sh
source "${REPO_ROOT}/flink/bin/resolve_sql_client.sh"

print_troubleshoot() {
  echo "ERROR: Flink SQL Client not usable on this host." >&2
  echo "" >&2
  echo "Bootstrap from Apache Flink ${FLINK_VERSION:-1.20.1} (must match CSA parcel):" >&2
  echo "  CSA_FLINK=/opt/cloudera/parcels/FLINK/lib/flink" >&2
  echo "  cp ~/flink-${FLINK_VERSION:-1.20.1}/bin/sql-client.sh \$CSA_FLINK/bin/" >&2
  echo "  cp ~/flink-${FLINK_VERSION:-1.20.1}/opt/flink-sql-client-*.jar \$CSA_FLINK/lib/" >&2
  echo "  cp ~/flink-${FLINK_VERSION:-1.20.1}/opt/flink-sql-gateway-*.jar \$CSA_FLINK/lib/" >&2
  echo "  Or: ./scripts/bootstrap_flink_sql_client.sh flink-${FLINK_VERSION:-1.20.1} --target parcel" >&2
  echo "" >&2
  echo "Or use SSB REST API:" >&2
  echo "  FLINK_SUBMIT_BACKEND=ssb ./flink/run_ltas_5min.sh" >&2
}

if [[ "${1:-}" == "flink" ]]; then
  shift
  exec "${FLINK_BIN}/flink" "$@"
fi

if ! sql_client_ready; then
  print_troubleshoot
  exit 1
fi

echo "Using sql-client (${SQL_CLIENT_SOURCE}): ${SQL_CLIENT_BIN}" >&2
exec "${SQL_CLIENT_BIN}" "$@"
