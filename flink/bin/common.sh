# Shared env for Flink SQL Client (jshin CDP 7.3.2)
# shellcheck shell=bash

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export HIVE_HOME="${HIVE_HOME:-/opt/cloudera/parcels/CDH/lib/hive}"
export HIVE_CONF_DIR="${HIVE_CONF_DIR:-${HIVE_HOME}/conf}"

FLINK_BIN="${FLINK_BIN:-/opt/cloudera/parcels/FLINK/bin}"
CSA_FLINK_LIB="${CSA_FLINK_LIB:-/opt/cloudera/parcels/FLINK/lib/flink}"

# Iceberg Hive catalog + HDFS access for embedded SQL Gateway (Flink 1.16+ loads catalog factories from lib/)
if command -v hadoop >/dev/null 2>&1 && [[ -z "${HADOOP_CLASSPATH:-}" ]]; then
  export HADOOP_CLASSPATH="$(hadoop classpath 2>/dev/null || true)"
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CATALOG_SQL="${CATALOG_SQL:-${REPO_ROOT}/flink/conf/00_catalog_setup_jshin.sql}"

SSL_OPTS=(
  -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks
  -Djavax.net.ssl.trustStorePassword=changeit
  -Djavax.net.ssl.trustStoreType=JKS
)

# CDH Hive jars for Iceberg HiveCatalog (embedded SQL Gateway classpath).
# Do NOT use flink-sql-connector-hive in CSA lib/ — it bundles Calcite and breaks INSERT.
collect_hive_metastore_jars() {
  local hive_lib="${HIVE_HOME}/lib"
  local patterns=(hive-exec-*.jar hive-metastore-*.jar hive-common-*.jar hive-serde-*.jar)
  local pattern jar
  HIVE_METASTORE_JARS=()
  if [[ ! -d "$hive_lib" ]]; then
    return 0
  fi
  shopt -s nullglob
  for pattern in "${patterns[@]}"; do
    for jar in "${hive_lib}/${pattern}"; do
      HIVE_METASTORE_JARS+=("$jar")
    done
  done
  shopt -u nullglob
}
collect_hive_metastore_jars
