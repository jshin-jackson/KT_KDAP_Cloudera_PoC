-- Scenario 2: SGi real-time 5-min TUMBLE (Variant 2 — maps to WF_M07046 v2)
-- Sink: sgi_5min_flink_v2 (v1 과 별도 테이블)
--
-- Run (jshin, repo root):
--   export HADOOP_CONF_DIR=/etc/hadoop/conf
--   kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
--   ./flink/run_sgi_5min_v2.sh
--
-- Manual (flink-sql-client):
--   /opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded \
--     -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks \
--     -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS \
--     -f flink/conf/00_catalog_setup_jshin.sql -f flink/sgi_5min_v2.sql
-- Fallback (SSB):
--   FLINK_SUBMIT_BACKEND=ssb ./flink/run_sgi_5min_v2.sh

SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.state.ttl' = '1 h';

CREATE TABLE cdr_sgi_stream (
  sgi_id       STRING,
  cell_id      STRING,
  rqt_st_dt    STRING,
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

CREATE TABLE bts_dim_v2 (
  cell_id      STRING,
  bts_alt_key  STRING
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_hive_catalog',
  'catalog-database' = 'kdap',
  'catalog-table' = 'bts_master',
  'streaming' = 'false'
);

CREATE TABLE sgi_5min_sink_v2 (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  bts_alt_key  STRING,
  total_val    BIGINT,
  record_cnt   BIGINT
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_hive_catalog',
  'catalog-database' = 'kdap',
  'catalog-table' = 'sgi_5min_flink_v2'
);

INSERT INTO sgi_5min_sink_v2
SELECT
  window_start,
  window_end,
  bts_alt_key,
  SUM(val) AS total_val,
  COUNT(*) AS record_cnt
FROM TABLE(
  TUMBLE(
    TABLE (
      SELECT
        COALESCE(b.bts_alt_key, s.cell_id) AS bts_alt_key,
        s.val,
        s.event_time
      FROM cdr_sgi_stream s
      LEFT JOIN bts_dim_v2 FOR SYSTEM_TIME AS OF s.event_time AS b
        ON s.cell_id = b.cell_id
      WHERE SUBSTRING(s.rqt_st_dt, 11, 2) = '45'
    ),
    DESCRIPTOR(event_time),
    INTERVAL '5' MINUTE
  )
)
GROUP BY window_start, window_end, bts_alt_key;
