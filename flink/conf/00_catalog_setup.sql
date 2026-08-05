-- Iceberg Hive Catalog (generic template)
-- <HMS_HOST> 를 Cloudera Manager → Hive → Hive Metastore Host 로 바꾸세요.
-- jshin 클러스터는 00_catalog_setup_jshin.sql 사용.
--
-- Run (repo root):
--   cp .env.example .env
--   ./flink/run_catalog_setup.sh

CREATE CATALOG IF NOT EXISTS iceberg_hive_catalog WITH (
  'type'         = 'iceberg',
  'catalog-type' = 'hive',
  'uri'          = 'thrift://<HMS_HOST>:9083',
  'warehouse'    = 'hdfs:///user/hive/warehouse',
  'clients'      = '5',
  'property-version' = '1'
);

USE CATALOG iceberg_hive_catalog;
USE kdap;
