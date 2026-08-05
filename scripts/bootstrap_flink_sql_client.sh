#!/usr/bin/env bash
# Copy Apache Flink SQL Client files for CSA (missing sql-client.sh in parcel).
#
# jshin Edge Node (수동 적용 — parcel lib/ 경로):
#   CSA_FLINK=/opt/cloudera/parcels/FLINK-1.20.1-csa1.17.1.0-81475796/lib/flink
#   cp ~/flink-1.20.1/bin/sql-client.sh $CSA_FLINK/bin/
#   chmod +x $CSA_FLINK/bin/sql-client.sh
#   chown flink:flink $CSA_FLINK/bin/sql-client.sh
#   cp ~/flink-1.20.1/opt/flink-sql-client-1.20.1.jar $CSA_FLINK/lib/
#   cp ~/flink-1.20.1/opt/flink-sql-gateway-1.20.1.jar $CSA_FLINK/lib/
#   curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.20.1/flink-sql-connector-hive-3.1.3_2.12-1.20.1.jar
#   cp flink-sql-connector-hive-3.1.3_2.12-1.20.1.jar $CSA_FLINK/lib/   # HiveCatalog (Maven — tarball에 없음)
#   cp ~/iceberg-flink-runtime-1.20-*.jar $CSA_FLINK/lib/                   # Iceberg (Flink 1.20용)
#   chown flink:flink $CSA_FLINK/lib/flink-sql-*.jar $CSA_FLINK/lib/iceberg-flink-runtime-*.jar $CSA_FLINK/lib/flink-sql-connector-hive-*.jar
#
# Or use this script:
#   ./scripts/bootstrap_flink_sql_client.sh ~/flink-1.20.1 --target parcel

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLINK_VERSION="${FLINK_VERSION:-1.20.1}"
TARGET="vendor"
APACHE_HOME=""

usage() {
  cat <<EOF
Usage: $0 <apache-flink-${FLINK_VERSION}-dir> [--target vendor|parcel]

  vendor  copy to ${REPO_ROOT}/flink/vendor/apache-flink-${FLINK_VERSION}/
  parcel  copy to CSA parcel lib/flink/{bin,lib}/ (jshin layout)

Download Apache Flink ${FLINK_VERSION}:
  curl -LO https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz
  tar xzf flink-${FLINK_VERSION}-bin-scala_2.12.tgz
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET="${2:?}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "$APACHE_HOME" ]]; then
        APACHE_HOME="$1"
      else
        echo "ERROR: unexpected argument: $1" >&2
        usage
        exit 1
      fi
      shift
      ;;
  esac
done

if [[ -z "$APACHE_HOME" ]]; then
  usage
  exit 1
fi

APACHE_HOME="$(cd "$APACHE_HOME" && pwd)"

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "ERROR: missing required file: $1" >&2
    exit 1
  fi
}

require_file "${APACHE_HOME}/bin/sql-client.sh"
require_file "${APACHE_HOME}/opt/flink-sql-client-"*.jar
require_file "${APACHE_HOME}/opt/flink-sql-gateway-"*.jar

copy_optional_jar_glob() {
  local pattern="$1"
  local dest_lib="$2"
  local label="$3"
  local prefix="${pattern##*/}"
  prefix="${prefix%%\**}"
  if compgen -G "${dest_lib}/${prefix}"*.jar >/dev/null; then
    return 0
  fi
  if compgen -G "${pattern}" >/dev/null; then
    cp -f ${pattern} "${dest_lib}/"
    echo "Copied ${label}: ${dest_lib}/$(basename ${pattern})"
    return 0
  fi
  echo "WARN: ${label} not found (${pattern})" >&2
  return 1
}

find_iceberg_jar() {
  local candidate=""
  if [[ -n "${ICEBERG_JAR:-}" && -f "${ICEBERG_JAR}" ]]; then
    echo "${ICEBERG_JAR}"
    return 0
  fi
  candidate="$(find /opt/cloudera/parcels -name 'iceberg-flink-runtime-1.20-*.jar' 2>/dev/null | head -1 || true)"
  if [[ -n "$candidate" && -f "$candidate" ]]; then
    echo "$candidate"
    return 0
  fi
  candidate="$(find /opt/cloudera/parcels -name 'iceberg-flink-runtime-1.18-*.jar' 2>/dev/null | head -1 || true)"
  if [[ -n "$candidate" && -f "$candidate" ]]; then
    echo "WARN: using ${candidate} — prefer iceberg-flink-runtime-1.20-* for Flink ${FLINK_VERSION}" >&2
    echo "$candidate"
    return 0
  fi
  return 1
}

copy_iceberg_jar() {
  local dest_lib="$1"
  local iceberg_jar
  if compgen -G "${dest_lib}/iceberg-flink-runtime-"*.jar >/dev/null; then
    echo "Iceberg runtime already present in ${dest_lib}"
    return 0
  fi
  if ! iceberg_jar="$(find_iceberg_jar)"; then
    echo "WARN: iceberg-flink-runtime-1.20-*.jar not found on this host." >&2
    echo "  Search: find /opt/cloudera/parcels -name 'iceberg-flink-runtime-1.20-*.jar'" >&2
    echo "  Or download (example): curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.7.2/iceberg-flink-runtime-1.20-1.7.2.jar" >&2
    echo "  Then: ICEBERG_JAR=/path/to/iceberg-flink-runtime-1.20-*.jar $0 ... --target parcel" >&2
    return 1
  fi
  cp -f "$iceberg_jar" "${dest_lib}/"
  echo "Copied Iceberg runtime: ${dest_lib}/$(basename "$iceberg_jar")"
}

copy_hive_connector_jar() {
  # Do NOT install flink-sql-connector-hive into CSA lib/ — it bundles Calcite and
  # breaks Flink 1.20 INSERT planning (NoSuchFieldError: operands). Iceberg Hive
  # catalog uses CDH Hive client via HIVE_HOME + HADOOP_CLASSPATH instead.
  echo "Skipping flink-sql-connector-hive (use HIVE_HOME + HADOOP_CLASSPATH, not lib/)"
  return 0
}

remove_hive_connector_from_lib() {
  local dest_lib="$1"
  local jar
  shopt -s nullglob
  for jar in "${dest_lib}"/flink-sql-connector-hive-*.jar; do
    echo "Removing Calcite-conflicting jar: ${jar}"
    rm -f "${jar}"
  done
  shopt -u nullglob
}

install_vendor() {
  local dest="${REPO_ROOT}/flink/vendor/apache-flink-${FLINK_VERSION}"
  mkdir -p "${dest}/bin" "${dest}/opt"
  cp -f "${APACHE_HOME}/bin/sql-client.sh" "${dest}/bin/"
  cp -f "${APACHE_HOME}/opt/flink-sql-client-"*.jar "${dest}/opt/"
  cp -f "${APACHE_HOME}/opt/flink-sql-gateway-"*.jar "${dest}/opt/"
  copy_iceberg_jar "${dest}/opt" || true
  copy_hive_connector_jar "${dest}/opt" || true
  chmod +x "${dest}/bin/sql-client.sh"
  echo "Installed vendor SQL Client:"
  echo "  ${dest}/bin/sql-client.sh"
  ls -la "${dest}/opt/"
}

install_parcel() {
  local parcel_root dest_bin dest_lib
  parcel_root="$(readlink -f /opt/cloudera/parcels/FLINK 2>/dev/null || echo /opt/cloudera/parcels/FLINK)"
  dest_bin="${parcel_root}/lib/flink/bin"
  dest_lib="${parcel_root}/lib/flink/lib"
  mkdir -p "${dest_bin}" "${dest_lib}"
  cp -f "${APACHE_HOME}/bin/sql-client.sh" "${dest_bin}/"
  cp -f "${APACHE_HOME}/opt/flink-sql-client-"*.jar "${dest_lib}/"
  cp -f "${APACHE_HOME}/opt/flink-sql-gateway-"*.jar "${dest_lib}/"
  copy_iceberg_jar "${dest_lib}" || true
  remove_hive_connector_from_lib "${dest_lib}"
  chmod +x "${dest_bin}/sql-client.sh"
  if id flink &>/dev/null; then
    chown flink:flink "${dest_bin}/sql-client.sh" \
      "${dest_lib}/flink-sql-client-"*.jar \
      "${dest_lib}/flink-sql-gateway-"*.jar \
      "${dest_lib}/iceberg-flink-runtime-"*.jar 2>/dev/null || true
  fi
  echo "Installed into CSA parcel (jshin layout):"
  echo "  CSA_FLINK=${parcel_root}/lib/flink"
  echo "  ${dest_bin}/sql-client.sh"
  ls -la "${dest_lib}/flink-sql-client-"*.jar "${dest_lib}/flink-sql-gateway-"*.jar 2>/dev/null || true
  ls -la "${dest_lib}/iceberg-flink-runtime-"*.jar 2>/dev/null \
    || echo "  (iceberg runtime missing — CREATE CATALOG iceberg may fail)"
  echo "  Hive: use HIVE_HOME + HADOOP_CLASSPATH (do not add flink-sql-connector-hive to lib/)"
  echo ""
  echo "Test:"
  echo "  /opt/cloudera/parcels/FLINK/bin/flink-sql-client --help"
}

case "$TARGET" in
  vendor)
    install_vendor
    ;;
  parcel)
    install_parcel
    ;;
  *)
    echo "ERROR: --target must be vendor or parcel" >&2
    exit 1
    ;;
esac

echo ""
echo "Run SQL job:"
echo "  export HADOOP_CONF_DIR=/etc/hadoop/conf"
echo "  kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM"
echo "  FLINK_SUBMIT_BACKEND=sql-client ${REPO_ROOT}/flink/run_ltas_5min.sh"
