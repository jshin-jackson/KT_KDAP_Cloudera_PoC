-- Flink SQL Client: Iceberg Hive Catalog 설정 (generic template)
-- <HMS_HOST> 를 Cloudera Manager → Hive → Hive Metastore Host 로 바꾸세요.
-- jshin 클러스터는 00_catalog_setup_jshin.sql 사용.
--
-- Run (jshin, repo root):
--   ./flink/run_catalog_setup.sh
--
-- Manual (YARN session + SQL):
--   /opt/cloudera/parcels/FLINK/bin/flink-yarn-session -d -s 2 -tm 2048
--   /opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded \
--     -Djavax.net.ssl.trustStore=... -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS \
--     -f flink/conf/00_catalog_setup.sql

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
