-- Scenario 2: SGi real-time 5-min TUMBLE (Variant 1 — maps to WF_M07046 v1)
--
-- Run (jshin, repo root):
--   export HADOOP_CONF_DIR=/etc/hadoop/conf
--   kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
--   ./flink/run_sgi_5min_v1.sh
--
-- Manual (flink-sql-client):
--   /opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded \
--     -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks \
--     -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS \
--     -i flink/conf/00_catalog_setup_jshin.sql -f flink/sgi_5min_v1.sql
-- Fallback (SSB):
--   FLINK_SUBMIT_BACKEND=ssb ./flink/run_sgi_5min_v1.sh

SET 'execution.runtime-mode' = 'streaming';

USE CATALOG default_catalog;
USE default_database;

CREATE TABLE cdr_sgi_stream (
  sgi_id       STRING,
  cell_id      STRING,
  alt_cell_id  STRING,
  rqt_st_dt    STRING,
  use_flag     STRING,
  signal_type  STRING,
  val          BIGINT,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_hive_catalog',
  'catalog-database' = 'kdap',
  'catalog-table' = 'cdr_sgi_raw',
  'streaming' = 'true',
  'monitor-interval' = '30s'
);

CREATE TABLE bts_dim (
  cell_id      STRING,
  alt_cell_id  STRING,
  bts_alt_key  STRING,
  region_cd    STRING
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_hive_catalog',
  'catalog-database' = 'kdap',
  'catalog-table' = 'bts_master',
  'streaming' = 'false'
);

CREATE TABLE sgi_5min_sink (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  bts_alt_key  STRING,
  region_cd    STRING,
  total_val    BIGINT,
  record_cnt   BIGINT
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_hive_catalog',
  'catalog-database' = 'kdap',
  'catalog-table' = 'sgi_5min_flink'
);

CREATE TEMPORARY VIEW sgi_joined_v1 AS
SELECT s.use_flag, s.val, s.event_time, b.bts_alt_key, b.region_cd
FROM cdr_sgi_stream s
LEFT JOIN bts_dim FOR SYSTEM_TIME AS OF s.event_time AS b
  ON s.cell_id = b.cell_id OR s.alt_cell_id = b.alt_cell_id
WHERE SUBSTRING(s.rqt_st_dt, 11, 2) = '45'
  AND s.signal_type IN ('S1AP', 'SGI');

INSERT INTO sgi_5min_sink
SELECT
  window_start,
  window_end,
  bts_alt_key,
  region_cd,
  SUM(CASE WHEN use_flag = 'Y' THEN val ELSE 0 END) AS total_val,
  COUNT(*) AS record_cnt
FROM TABLE(
  TUMBLE(TABLE sgi_joined_v1, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, bts_alt_key, region_cd;
