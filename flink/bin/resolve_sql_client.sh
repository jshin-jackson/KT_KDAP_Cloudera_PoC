# Resolve Flink SQL Client on CSA (parcel or Apache vendor copy).
# shellcheck shell=bash

FLINK_VERSION="${FLINK_VERSION:-1.20.1}"
SQL_CLIENT_BIN=""
SQL_CLIENT_SOURCE=""

parcel_has_sql_client_jar() {
  local flink_lib="$1"
  local has_client=false has_gateway=false

  if compgen -G "${flink_lib}/lib/flink-sql-client-"*.jar >/dev/null \
    || compgen -G "${flink_lib}/opt/flink-sql-client-"*.jar >/dev/null; then
    has_client=true
  fi
  if compgen -G "${flink_lib}/lib/flink-sql-gateway-"*.jar >/dev/null \
    || compgen -G "${flink_lib}/opt/flink-sql-gateway-"*.jar >/dev/null; then
    has_gateway=true
  fi
  [[ "$has_client" == true && "$has_gateway" == true ]]
}

resolve_sql_client() {
  local parcel_root flink_lib vendor_home
  parcel_root="$(readlink -f /opt/cloudera/parcels/FLINK 2>/dev/null || echo /opt/cloudera/parcels/FLINK)"
  flink_lib="${parcel_root}/lib/flink"
  vendor_home="${REPO_ROOT}/flink/vendor/apache-flink-${FLINK_VERSION}"

  if [[ -x "${flink_lib}/bin/sql-client.sh" ]] && parcel_has_sql_client_jar "${flink_lib}"; then
    SQL_CLIENT_BIN="${flink_lib}/bin/sql-client.sh"
    SQL_CLIENT_SOURCE="csa-parcel"
    return 0
  fi

  if [[ -x "${vendor_home}/bin/sql-client.sh" ]] \
    && compgen -G "${vendor_home}/opt/flink-sql-client-"*.jar >/dev/null \
    && compgen -G "${vendor_home}/opt/flink-sql-gateway-"*.jar >/dev/null; then
    SQL_CLIENT_BIN="${vendor_home}/bin/sql-client.sh"
    SQL_CLIENT_SOURCE="apache-vendor"
    export FLINK_OPT_DIR="${vendor_home}/opt"
    if [[ -f "${parcel_root}/bin/flink-exec-env.sh" ]]; then
      # shellcheck disable=SC1090
      source "${parcel_root}/bin/flink-exec-env.sh"
    fi
    return 0
  fi

  SQL_CLIENT_BIN=""
  SQL_CLIENT_SOURCE=""
  return 1
}

sql_client_ready() {
  resolve_sql_client
}
