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

# Hive connector in lib/ bundles Calcite; parent-first keeps Flink's planner Calcite.
FLINK_JVM_OPTS=(
  -Dclassloader.resolve-order=parent-first
)

hive_connector_in_lib() {
  compgen -G "${CSA_FLINK_LIB}/lib/flink-sql-connector-hive-"*.jar >/dev/null 2>&1
}

HIVE_METASTORE_API_CLASS="org/apache/hadoop/hive/metastore/api/NoSuchObjectException.class"

load_repo_dotenv() {
  local env_file="${REPO_ROOT}/.env"
  [[ -f "$env_file" ]] || return 0
  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -n "$line" && "$line" == *"="* ]] || continue
    key="${line%%=*}"
    value="${line#*=}"
    value="${value%\"}"
    value="${value#\"}"
    value="${value%\'}"
    value="${value#\'}"
    if [[ -z "${!key:-}" ]]; then
      export "$key=$value"
    fi
  done < "$env_file"
}

ssb_configured() {
  [[ -n "${SSB_API_BASE:-}" ]]
}

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

jar_contains_class() {
  local jar="$1" class_path="$2"
  [[ -f "$jar" ]] || return 1
  jar tf "$jar" 2>/dev/null | grep -Fq "$class_path"
}

append_unique_jar() {
  local jar="$1" existing
  [[ -f "$jar" ]] || return 0
  for existing in "${HIVE_METASTORE_JARS[@]}"; do
    [[ "$existing" == "$jar" ]] && return 0
  done
  HIVE_METASTORE_JARS+=("$jar")
}

first_hive_jar() {
  local prefix="$1"
  local dir jar
  while IFS= read -r dir; do
    [[ -n "$dir" && -d "$dir" ]] || continue
    if [[ "$prefix" == "hive-exec" ]]; then
      jar="$(find "$dir" -maxdepth 1 -type f -name 'hive-exec-*.jar' \
        ! -name '*-core.jar' ! -name '*-tests.jar' 2>/dev/null | sort | head -n1)"
      if [[ -z "$jar" ]]; then
        jar="$(find "$dir" -maxdepth 1 -type f -name 'hive-exec-*-core.jar' \
          ! -name '*-tests.jar' 2>/dev/null | sort | head -n1)"
      fi
    else
      jar="$(find "$dir" -maxdepth 1 -type f -name "${prefix}-*.jar" \
        ! -name '*-tests.jar' 2>/dev/null | sort | head -n1)"
    fi
    if [[ -n "$jar" && -f "$jar" ]]; then
      printf '%s' "$jar"
      return 0
    fi
  done < <(hive_jar_search_dirs)
  return 1
}

find_jars_with_class() {
  local class_path="$1"
  local name_pattern="${2:-*.jar}"
  local dir jar
  while IFS= read -r dir; do
    [[ -n "$dir" && -d "$dir" ]] || continue
    while IFS= read -r jar; do
      if jar_contains_class "$jar" "$class_path"; then
        printf '%s\n' "$jar"
      fi
    done < <(find "$dir" -maxdepth 1 -type f -name "$name_pattern" 2>/dev/null)
  done < <(hive_jar_search_dirs)
}

hive_metastore_api_present() {
  local jar
  for jar in "${HIVE_METASTORE_JARS[@]}"; do
    if jar_contains_class "$jar" "$HIVE_METASTORE_API_CLASS"; then
      return 0
    fi
  done
  return 1
}

collect_hive_metastore_jars() {
  local prefix jar
  local -a prefixes=(
    hive-standalone-metastore
    hive-service-rpc
    hive-metastore
    hive-common
    hive-serde
    libthrift
    libfb303
  )
  HIVE_METASTORE_JARS=()
  HIVE_JAR_SEARCH_DIRS=()
  while IFS= read -r dir; do
    [[ -n "$dir" ]] && HIVE_JAR_SEARCH_DIRS+=("$dir")
  done < <(hive_jar_search_dirs)

  for prefix in "${prefixes[@]}"; do
    jar="$(first_hive_jar "$prefix")" || true
    [[ -n "$jar" ]] && append_unique_jar "$jar"
  done

  jar="$(first_hive_jar "hive-exec")" || true
  [[ -n "$jar" ]] && append_unique_jar "$jar"

  if ! hive_metastore_api_present; then
    while IFS= read -r jar; do
      append_unique_jar "$jar"
    done < <(find_jars_with_class "$HIVE_METASTORE_API_CLASS" 'hive-*.jar')
  fi
}
