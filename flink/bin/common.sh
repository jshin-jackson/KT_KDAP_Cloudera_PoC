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
hive_jar_search_dirs() {
  local -a dirs=()
  local cdh_root cdh_real

  if [[ -d "${HIVE_HOME}/lib" ]]; then
    dirs+=("${HIVE_HOME}/lib")
  fi

  # CDH layout: HIVE_HOME=.../CDH/lib/hive, jars=.../CDH/jars
  cdh_root="$(cd "${HIVE_HOME}/../.." 2>/dev/null && pwd || true)"
  if [[ -n "$cdh_root" && -d "${cdh_root}/jars" ]]; then
    dirs+=("${cdh_root}/jars")
  fi

  if [[ -d /opt/cloudera/parcels/CDH/jars ]]; then
    dirs+=("/opt/cloudera/parcels/CDH/jars")
  fi

  cdh_real="$(readlink -f /opt/cloudera/parcels/CDH 2>/dev/null || true)"
  if [[ -n "$cdh_real" && -d "${cdh_real}/jars" ]]; then
    dirs+=("${cdh_real}/jars")
  fi

  printf '%s\n' "${dirs[@]}" | awk '!seen[$0]++'
}

first_hive_jar() {
  local prefix="$1"
  local dir jar
  while IFS= read -r dir; do
    [[ -n "$dir" && -d "$dir" ]] || continue
    jar="$(find "$dir" -maxdepth 1 -type f -name "${prefix}-*.jar" 2>/dev/null | sort | head -n1)"
    if [[ -n "$jar" && -f "$jar" ]]; then
      printf '%s' "$jar"
      return 0
    fi
  done < <(hive_jar_search_dirs)
  return 1
}

collect_hive_metastore_jars() {
  local prefix jar
  local -a prefixes=(hive-metastore hive-common hive-serde hive-exec)
  HIVE_METASTORE_JARS=()
  HIVE_JAR_SEARCH_DIRS=()
  while IFS= read -r dir; do
    [[ -n "$dir" ]] && HIVE_JAR_SEARCH_DIRS+=("$dir")
  done < <(hive_jar_search_dirs)

  for prefix in "${prefixes[@]}"; do
    jar="$(first_hive_jar "$prefix")" || true
    if [[ -n "$jar" && -f "$jar" ]]; then
      HIVE_METASTORE_JARS+=("$jar")
    fi
  done
}
