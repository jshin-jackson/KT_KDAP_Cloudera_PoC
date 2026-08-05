#!/usr/bin/env bash
exec "$(dirname "$0")/bin/submit_flink_sql.sh" "$(dirname "$0")/sgi_5min_v1.sql"
