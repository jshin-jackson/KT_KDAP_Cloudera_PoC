-- KT KDAP PoC: Iceberg dimension / reference tables
USE kdap;

-- Base station master (기지국)
CREATE TABLE IF NOT EXISTS bts_master (
  cell_id          STRING,
  alt_cell_id      STRING,
  bts_alt_key      STRING,
  bts_market_nm    STRING,
  lat              DOUBLE,
  lon              DOUBLE,
  region_cd        STRING,
  rpot_geom        STRING,
  rpot_line_cnt    INT
)
STORED AS ICEBERG
TBLPROPERTIES ('format-version' = '2');

-- MDT report points (TBM_RPOT_M equivalent)
CREATE TABLE IF NOT EXISTS tbm_rpot_m (
  rpot_seq         INT,
  rpot_geom        STRING,
  rpot_line_cnt    INT,
  bts_market_nm    STRING
)
STORED AS ICEBERG;

-- Administrative geography (MSTR RSRP)
CREATE TABLE IF NOT EXISTS tbm_gpot25 (
  gpot25_x         INT,
  gpot25_y         INT,
  bdri_cd          STRING
)
STORED AS ICEBERG;

CREATE TABLE IF NOT EXISTS tbm_bdri (
  bdri_cd          STRING,
  sido_name        STRING,
  gungu_name       STRING,
  bemd_name        STRING,
  bdri_name        STRING
)
STORED AS ICEBERG;

-- MSTR: LTE call / contract / product (VoLTE drop rate)
CREATE TABLE IF NOT EXISTS lte_call_fact (
  call_id          STRING,
  contract_id      STRING,
  product_id       STRING,
  call_start_dt    STRING,
  drop_flag        STRING,
  volte_flag       STRING,
  base_date        STRING
)
PARTITIONED BY SPEC (base_date)
STORED AS ICEBERG;

CREATE TABLE IF NOT EXISTS contract_dim (
  contract_id      STRING,
  subscriber_id    STRING,
  plan_type        STRING
)
STORED AS ICEBERG;

CREATE TABLE IF NOT EXISTS product_dim (
  product_id       STRING,
  product_name     STRING,
  volte_eligible   STRING
)
STORED AS ICEBERG;

-- MSTR mart output staging
CREATE TABLE IF NOT EXISTS mstr_rsrp_bad_rate_result (
  run_id           STRING,
  base_date        STRING,
  sido_name        STRING,
  gungu_name       STRING,
  rsrp_tot         BIGINT,
  rsrp_u110        BIGINT,
  computed_at      TIMESTAMP
)
PARTITIONED BY SPEC (base_date)
STORED AS ICEBERG;

CREATE TABLE IF NOT EXISTS mstr_volte_drop_result (
  run_id           STRING,
  window_start     TIMESTAMP,
  drop_rate        DOUBLE,
  total_calls      BIGINT,
  dropped_calls    BIGINT,
  computed_at      TIMESTAMP
)
PARTITIONED BY SPEC (days(window_start))
STORED AS ICEBERG;
