-- CDP 내부 Flink 테스트용 최소 테이블 (Impala에서 실행)
-- KT 전체 데이터 없이 Flink 5분 윈도우 동작만 검증합니다.

CREATE DATABASE IF NOT EXISTS kdap;
USE kdap;

-- 기지국 (차원)
CREATE TABLE IF NOT EXISTS bts_master (
  cell_id       STRING,
  alt_cell_id   STRING,
  bts_alt_key   STRING,
  bts_market_nm STRING,
  region_cd     STRING
)
STORED AS ICEBERG
TBLPROPERTIES ('format-version' = '2');

-- SGi 스트리밍 소스 (CDR_DW.TRD_CDR_SGI 대응)
CREATE TABLE IF NOT EXISTS cdr_sgi_raw (
  sgi_id     STRING,
  cell_id    STRING,
  alt_cell_id STRING,
  rqt_st_dt  STRING,
  etl_date   STRING,
  val        BIGINT,
  use_flag   STRING,
  signal_type STRING,
  event_time TIMESTAMP
)
PARTITIONED BY SPEC (days(event_time))
STORED AS ICEBERG
TBLPROPERTIES ('format-version' = '2');

-- MDT 스트리밍 소스 (MDT_DW.TRD_CDR_MDT_SMSNG_M1 대응)
CREATE TABLE IF NOT EXISTS cdr_mdt_smsng_raw (
  mdt_id        STRING,
  gnss_utmkx    DOUBLE,
  gnss_utmky    DOUBLE,
  rqt_st_dt     STRING,
  bts_market_nm STRING,
  event_time    TIMESTAMP
)
PARTITIONED BY SPEC (days(event_time))
STORED AS ICEBERG
TBLPROPERTIES ('format-version' = '2');

-- Flink 적재 대상 (LTAS)
CREATE TABLE IF NOT EXISTS ltas_5min_flink (
  window_start TIMESTAMP,
  window_end   TIMESTAMP,
  bts_alt_key  STRING,
  region_cd    STRING,
  total_val    BIGINT,
  record_cnt   BIGINT
)
PARTITIONED BY SPEC (days(window_start))
STORED AS ICEBERG;

-- Flink 적재 대상 (SGi v1)
CREATE TABLE IF NOT EXISTS sgi_5min_flink (
  window_start TIMESTAMP,
  window_end   TIMESTAMP,
  bts_alt_key  STRING,
  region_cd    STRING,
  total_val    BIGINT,
  record_cnt   BIGINT
)
PARTITIONED BY SPEC (days(window_start))
STORED AS ICEBERG;

-- Flink 적재 대상 (SGi v2)
CREATE TABLE IF NOT EXISTS sgi_5min_flink_v2 (
  window_start TIMESTAMP,
  window_end   TIMESTAMP,
  bts_alt_key  STRING,
  total_val    BIGINT,
  record_cnt   BIGINT
)
PARTITIONED BY SPEC (days(window_start))
STORED AS ICEBERG;

-- Flink 적재 대상 (MDT)
CREATE TABLE IF NOT EXISTS mdt_5min_flink (
  window_start TIMESTAMP,
  window_end   TIMESTAMP,
  bts_alt_key  STRING,
  record_cnt   BIGINT
)
PARTITIONED BY SPEC (days(window_start))
STORED AS ICEBERG;
