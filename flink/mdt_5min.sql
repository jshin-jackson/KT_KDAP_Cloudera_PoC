-- Scenario 2: MDT real-time 5-min TUMBLE
-- Source: MDT_DW.TRD_CDR_MDT_SMSNG_M1 via Iceberg streaming
--
-- Run (jshin, repo root):
--   export HADOOP_CONF_DIR=/etc/hadoop/conf
--   kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
--   ./flink/run_mdt_5min.sh
--
-- Manual:
--   /opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded \
--     -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks \
--     -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS \
--     -f flink/conf/00_catalog_setup_jshin.sql -f flink/mdt_5min.sql

SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE cdr_mdt_stream (
  mdt_id       STRING,
  gnss_utmkx   DOUBLE,
  gnss_utmky   DOUBLE,
  rqt_st_dt    STRING,
  bts_market_nm STRING,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_hive_catalog',
  'catalog-database' = 'kdap',
  'catalog-table' = 'cdr_mdt_smsng_raw',
  'streaming' = 'true',
  'monitor-interval' = '30s'
);

CREATE TABLE bts_dim (
  bts_market_nm STRING,
  bts_alt_key   STRING
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_hive_catalog',
  'catalog-database' = 'kdap',
  'catalog-table' = 'bts_master',
  'streaming' = 'false'
);

CREATE TABLE mdt_5min_sink (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  bts_alt_key  STRING,
  record_cnt   BIGINT
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_hive_catalog',
  'catalog-database' = 'kdap',
  'catalog-table' = 'mdt_5min_flink'
);

INSERT INTO mdt_5min_sink
SELECT
  window_start,
  window_end,
  bts_alt_key,
  COUNT(*) AS record_cnt
FROM TABLE(
  TUMBLE(
    TABLE (
      SELECT m.event_time, b.bts_alt_key
      FROM cdr_mdt_stream m
      LEFT JOIN bts_dim FOR SYSTEM_TIME AS OF m.event_time AS b
        ON m.bts_market_nm = b.bts_market_nm
      WHERE SUBSTRING(m.rqt_st_dt, 11, 2) = '45'
    ),
    DESCRIPTOR(event_time),
    INTERVAL '5' MINUTE
  )
)
GROUP BY window_start, window_end, bts_alt_key;
