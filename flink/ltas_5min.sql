-- Scenario 2: LTAS real-time 5-min TUMBLE aggregation (Flink SQL)
-- Source: CDR_DW.TRD_CDR_SGI streaming via Iceberg
-- Real-time filter: SUBSTR(rqt_st_dt, 11, 2) = '45'
--
-- Run (jshin, repo root):
--   export HADOOP_CONF_DIR=/etc/hadoop/conf
--   kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
--   ./flink/run_ltas_5min.sh
--
-- Manual (YARN session + SQL):
--   /opt/cloudera/parcels/FLINK/bin/flink-yarn-session -d -s 2 -tm 2048
--   /opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded \
--     -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks \
--     -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS \
--     -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql

SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.state.ttl' = '1 h';

CREATE TABLE cdr_sgi_stream (
  sgi_id       STRING,
  cell_id      STRING,
  rqt_st_dt    STRING,
  val          BIGINT,
  etl_date     STRING,
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
  bts_alt_key  STRING,
  region_cd    STRING
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_hive_catalog',
  'catalog-database' = 'kdap',
  'catalog-table' = 'bts_master',
  'streaming' = 'false'
);

CREATE TABLE ltas_5min_sink (
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
  'catalog-table' = 'ltas_5min_flink'
);

INSERT INTO ltas_5min_sink
SELECT
  window_start,
  window_end,
  bts_alt_key,
  region_cd,
  SUM(val) AS total_val,
  COUNT(*) AS record_cnt
FROM TABLE(
  TUMBLE(
    TABLE (
      SELECT
        s.val,
        s.event_time,
        b.bts_alt_key,
        b.region_cd
      FROM cdr_sgi_stream s
      LEFT JOIN bts_dim FOR SYSTEM_TIME AS OF s.event_time AS b
        ON s.cell_id = b.cell_id
      WHERE SUBSTRING(s.rqt_st_dt, 11, 2) = '45'
    ),
    DESCRIPTOR(event_time),
    INTERVAL '5' MINUTE
  )
)
GROUP BY window_start, window_end, bts_alt_key, region_cd;
