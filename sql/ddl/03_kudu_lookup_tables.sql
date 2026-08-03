-- KT KDAP PoC: Kudu tables for HBase replacement (Scenario 4)
-- Run: impala-shell -k -f sql/ddl/03_kudu_lookup_tables.sql

USE kdap;

-- Primary key lookup table (call quality single-row fetch)
-- Hash partitions for even distribution; adjust # buckets after row count known
CREATE TABLE IF NOT EXISTS call_quality_kudu (
  lookup_key       STRING,
  call_id          STRING,
  rsrp             INT,
  rsrq             INT,
  drop_flag        STRING,
  base_date        STRING,
  detail_json      STRING,
  PRIMARY KEY (lookup_key)
)
PARTITION BY HASH (lookup_key) PARTITIONS 32
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = 'kdap-base-m1.example.com:7051,kdap-base-m2.example.com:7051,kdap-base-m3.example.com:7051'
);

-- Secondary index pattern: lookup by call_id + date range
CREATE TABLE IF NOT EXISTS call_quality_kudu_by_call (
  call_id          STRING,
  base_date        STRING,
  lookup_key       STRING,
  rsrp             INT,
  drop_flag        STRING,
  PRIMARY KEY (call_id, base_date)
)
PARTITION BY HASH (call_id) PARTITIONS 16
STORED AS KUDU;

-- Note: Update kudu.master_addresses to actual CM-generated value
-- impala-shell -q "SHOW CREATE TABLE call_quality_kudu"
