-- Flink SQL Client: Iceberg Hive Catalog 설정
-- flink-sql-client 실행 후 맨 먼저 이 파일을 적용합니다.
-- <HMS_HOST> 를 Cloudera Manager → Hive → Hive Metastore Host 로 바꾸세요.

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
