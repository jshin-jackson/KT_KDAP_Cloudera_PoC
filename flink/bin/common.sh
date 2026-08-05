# Shared env for Flink YARN session + SQL Client (jshin CDP 7.3.2)
# shellcheck shell=bash

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"

FLINK_BIN="${FLINK_BIN:-/opt/cloudera/parcels/FLINK/bin}"
YARN_SLOTS="${YARN_SLOTS:-2}"
YARN_TM_MEMORY="${YARN_TM_MEMORY:-2048}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CATALOG_SQL="${CATALOG_SQL:-${REPO_ROOT}/flink/conf/00_catalog_setup_jshin.sql}"

SSL_OPTS=(
  -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks
  -Djavax.net.ssl.trustStorePassword=changeit
  -Djavax.net.ssl.trustStoreType=JKS
)

flink_yarn_properties_file() {
  local user props
  user="$(whoami)"
  props="${REPO_ROOT}/.yarn-properties-${user}"
  if [[ -f "$props" ]]; then
    echo "$props"
    return 0
  fi
  find /tmp -maxdepth 1 -name ".yarn-properties-${user}" 2>/dev/null | head -1
}

flink_yarn_session_running() {
  local props
  props="$(flink_yarn_properties_file || true)"
  if [[ -n "$props" && -f "$props" ]]; then
    return 0
  fi
  yarn application -list -appStates RUNNING 2>/dev/null | grep -qiE 'Flink session|Apache Flink'
}
