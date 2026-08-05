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
#   chown flink:flink $CSA_FLINK/lib/flink-sql-client-*.jar $CSA_FLINK/lib/flink-sql-gateway-*.jar
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

install_vendor() {
  local dest="${REPO_ROOT}/flink/vendor/apache-flink-${FLINK_VERSION}"
  mkdir -p "${dest}/bin" "${dest}/opt"
  cp -f "${APACHE_HOME}/bin/sql-client.sh" "${dest}/bin/"
  cp -f "${APACHE_HOME}/opt/flink-sql-client-"*.jar "${dest}/opt/"
  cp -f "${APACHE_HOME}/opt/flink-sql-gateway-"*.jar "${dest}/opt/"
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
  chmod +x "${dest_bin}/sql-client.sh"
  if id flink &>/dev/null; then
    chown flink:flink "${dest_bin}/sql-client.sh" \
      "${dest_lib}/flink-sql-client-"*.jar \
      "${dest_lib}/flink-sql-gateway-"*.jar 2>/dev/null || true
  fi
  echo "Installed into CSA parcel (jshin layout):"
  echo "  CSA_FLINK=${parcel_root}/lib/flink"
  echo "  ${dest_bin}/sql-client.sh"
  ls -la "${dest_lib}/flink-sql-client-"*.jar "${dest_lib}/flink-sql-gateway-"*.jar
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
