-- KT KDAP PoC: Iceberg fact tables
-- Partition strategy: ETL_DATE / BASE_DATE (finalize after full dataset receipt)
-- Run after: impala-shell -k -f sql/ddl/00_create_database.sql

USE kdap;

-- CDR_SGI: primary fact for LTAS and SGi scenarios (~130억 - 254억 rows)
-- Replace column list with actual KT schema when received
CREATE TABLE IF NOT EXISTS cdr_sgi (
  sgi_id           STRING,
  cell_id          STRING,
  alt_cell_id      STRING,
  rqt_st_dt        STRING,
  etl_date         STRING,
  base_date        STRING,
  val              BIGINT,
  signal_type      STRING,
  use_flag         STRING,
  subscriber_id    STRING,
  -- add remaining columns from KT DDL
  event_time       TIMESTAMP
)
PARTITIONED BY SPEC (
  etl_date
)
STORED AS ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'format-version' = '2'
);

-- Real-time streaming source mirror (CDR_DW.TRD_CDR_SGI)
CREATE TABLE IF NOT EXISTS cdr_sgi_raw (
  sgi_id           STRING,
  cell_id          STRING,
  rqt_st_dt        STRING,
  etl_date         STRING,
  val              BIGINT,
  event_time       TIMESTAMP
)
PARTITIONED BY SPEC (
  days(event_time)
)
STORED AS ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'format-version' = '2'
);

-- MDT Samsung fact (180억 rows)
CREATE TABLE IF NOT EXISTS cdr_mdt_smsng (
  mdt_id           STRING,
  gnss_utmkx       DOUBLE,
  gnss_utmky       DOUBLE,
  gnss_metric_id   STRING,
  bts_market_nm    STRING,
  trigr_ev         STRING,
  hndset_pet_nm    STRING,
  dlearfcn_cd      STRING,
  rsrp             INT,
  base_date        STRING,
  etl_date         STRING,
  event_time       TIMESTAMP
)
PARTITIONED BY SPEC (
  base_date
)
STORED AS ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'format-version' = '2'
);

-- Real-time MDT streaming source (MDT_DW.TRD_CDR_MDT_SMSNG_M1)
CREATE TABLE IF NOT EXISTS cdr_mdt_smsng_raw (
  mdt_id           STRING,
  gnss_utmkx       DOUBLE,
  gnss_utmky       DOUBLE,
  rqt_st_dt        STRING,
  base_date        STRING,
  event_time       TIMESTAMP
)
PARTITIONED BY SPEC (
  days(event_time)
)
STORED AS ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'format-version' = '2'
);

-- Batch output: LTAS aggregation target
CREATE TABLE IF NOT EXISTS sum_ltas_cdr_tm (
  etl_date         STRING,
  base_hour        STRING,
  bts_alt_key      STRING,
  region_cd        STRING,
  total_val        BIGINT,
  record_cnt       BIGINT,
  created_at       TIMESTAMP
)
PARTITIONED BY SPEC (
  etl_date
)
STORED AS ICEBERG;

-- Batch output: SGi use DD (2 variants)
CREATE TABLE IF NOT EXISTS tfm_s1ap_sgi_use_dd_v1 (
  etl_date         STRING,
  cell_id          STRING,
  use_cnt          BIGINT,
  created_at       TIMESTAMP
)
PARTITIONED BY SPEC (etl_date)
STORED AS ICEBERG;

CREATE TABLE IF NOT EXISTS tfm_s1ap_sgi_use_dd_v2 (
  etl_date         STRING,
  cell_id          STRING,
  use_cnt          BIGINT,
  created_at       TIMESTAMP
)
PARTITIONED BY SPEC (etl_date)
STORED AS ICEBERG;

-- Batch output: MDT merge staging
CREATE TABLE IF NOT EXISTS cnv_mdt_merge_stg (
  mdt_id           STRING,
  gnss_utmkx       DOUBLE,
  gnss_utmky       DOUBLE,
  bts_alt_key      STRING,
  distance_m       DOUBLE,
  base_date        STRING,
  created_at       TIMESTAMP
)
PARTITIONED BY SPEC (base_date)
STORED AS ICEBERG;

-- Flink 5-min sink tables
CREATE TABLE IF NOT EXISTS ltas_5min_flink (
  window_start     TIMESTAMP,
  window_end       TIMESTAMP,
  bts_alt_key      STRING,
  region_cd        STRING,
  total_val        BIGINT,
  record_cnt       BIGINT
)
PARTITIONED BY SPEC (days(window_start))
STORED AS ICEBERG;

CREATE TABLE IF NOT EXISTS sgi_5min_flink (
  window_start     TIMESTAMP,
  window_end       TIMESTAMP,
  bts_alt_key      STRING,
  region_cd        STRING,
  total_val        BIGINT,
  record_cnt       BIGINT
)
PARTITIONED BY SPEC (days(window_start))
STORED AS ICEBERG;

CREATE TABLE IF NOT EXISTS mdt_5min_flink (
  window_start     TIMESTAMP,
  window_end       TIMESTAMP,
  bts_alt_key      STRING,
  record_cnt       BIGINT
)
PARTITIONED BY SPEC (days(window_start))
STORED AS ICEBERG;

-- Iceberg variant for HBase lookup A/B test
CREATE TABLE IF NOT EXISTS call_quality_iceberg (
  lookup_key       STRING,
  call_id          STRING,
  rsrp             INT,
  rsrq             INT,
  drop_flag        STRING,
  base_date        STRING,
  detail_json      STRING
)
PARTITIONED BY SPEC (base_date)
STORED AS ICEBERG
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'write.parquet.row-group-size-bytes' = '134217728'
);

-- Refresh metadata after external writes
-- ALTER TABLE cdr_sgi REFRESH;
