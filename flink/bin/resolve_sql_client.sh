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

parcel_has_iceberg_jar() {
  local flink_lib="$1"
  compgen -G "${flink_lib}/lib/iceberg-flink-runtime-"*.jar >/dev/null \
    || compgen -G "${flink_lib}/opt/iceberg-flink-runtime-"*.jar >/dev/null
}

parcel_has_hive_jar() {
  # Intentionally unused for sql-client readiness: flink-sql-connector-hive in lib/
  # bundles Calcite and breaks INSERT planning (NoSuchFieldError: operands).
  # Iceberg Hive catalog uses CDH Hive client via HIVE_HOME / HADOOP_CLASSPATH.
  return 1
}

resolve_sql_client() {
  local parcel_root flink_lib vendor_home
  parcel_root="$(readlink -f /opt/cloudera/parcels/FLINK 2>/dev/null || echo /opt/cloudera/parcels/FLINK)"
  flink_lib="${parcel_root}/lib/flink"
  vendor_home="${REPO_ROOT}/flink/vendor/apache-flink-${FLINK_VERSION}"

  if [[ -x "${flink_lib}/bin/sql-client.sh" ]] \
    && parcel_has_sql_client_jar "${flink_lib}" \
    && parcel_has_iceberg_jar "${flink_lib}"; then
    SQL_CLIENT_BIN="${FLINK_BIN}/flink-sql-client"
    SQL_CLIENT_SOURCE="csa-parcel"
    return 0
  fi

  if [[ -x "${vendor_home}/bin/sql-client.sh" ]] \
    && compgen -G "${vendor_home}/opt/flink-sql-client-"*.jar >/dev/null \
    && compgen -G "${vendor_home}/opt/flink-sql-gateway-"*.jar >/dev/null \
    && compgen -G "${vendor_home}/opt/iceberg-flink-runtime-"*.jar >/dev/null; then
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

sql_client_bootstrap_status() {
  local parcel_root flink_lib
  parcel_root="$(readlink -f /opt/cloudera/parcels/FLINK 2>/dev/null || echo /opt/cloudera/parcels/FLINK)"
  flink_lib="${parcel_root}/lib/flink"

  if [[ ! -x "${flink_lib}/bin/sql-client.sh" ]]; then
    echo "missing: sql-client.sh"
  fi
  if ! parcel_has_sql_client_jar "${flink_lib}"; then
    echo "missing: flink-sql-client + flink-sql-gateway jars"
  fi
  if ! parcel_has_iceberg_jar "${flink_lib}"; then
    echo "missing: iceberg-flink-runtime jar in ${flink_lib}/lib/ (required for CREATE CATALOG iceberg)"
  fi
  if compgen -G "${flink_lib}/lib/flink-sql-connector-hive-"*.jar >/dev/null 2>&1; then
    echo "remove: flink-sql-connector-hive from ${flink_lib}/lib/ (Calcite conflict on INSERT — use HIVE_HOME + HADOOP_CLASSPATH)"
  fi
}
