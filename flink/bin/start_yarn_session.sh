#!/usr/bin/env bash
# Start Flink YARN session (once per cluster session).
#
# Usage (from repo root):
#   export HADOOP_CONF_DIR=/etc/hadoop/conf
#   kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
#   ./flink/bin/start_yarn_session.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

if flink_yarn_session_running; then
  echo "Flink YARN session is already running."
  yarn application -list -appStates RUNNING 2>/dev/null | grep -i flink || true
  flink_yarn_properties_file || true
  exit 0
fi

echo "Starting Flink YARN session (-s ${YARN_SLOTS} -tm ${YARN_TM_MEMORY})"
cd "${REPO_ROOT}"
"${FLINK_BIN}/flink-yarn-session" -d -s "${YARN_SLOTS}" -tm "${YARN_TM_MEMORY}"

echo ""
echo "Verify RUNNING (default name: Flink session cluster):"
yarn application -list -appStates RUNNING 2>/dev/null | grep -i flink || true
echo ""
echo "YARN properties file:"
flink_yarn_properties_file || true
