#!/usr/bin/env bash
exec "$(dirname "$0")/bin/submit_flink_sql.sh" "$(dirname "$0")/conf/00_catalog_setup_jshin.sql"
