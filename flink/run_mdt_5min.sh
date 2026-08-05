#!/usr/bin/env bash
exec "$(dirname "$0")/bin/submit_flink_sql.sh" "$(dirname "$0")/mdt_5min.sql"
