-- Flink SQL Client: Iceberg Hive Catalog (jshin CDP 7.3.2)
-- HMS 호스트: CM → Hive → Hive Metastore Host 와 다르면 uri 만 수정

CREATE CATALOG IF NOT EXISTS iceberg_hive_catalog WITH (
  'type'           = 'iceberg',
  'catalog-type'   = 'hive',
  'uri'            = 'thrift://ccycloud-5.jshin.root.comops.site:9083',
  'warehouse'      = 'hdfs:///user/hive/warehouse',
  'clients'        = '5',
  'property-version' = '1'
);

USE CATALOG iceberg_hive_catalog;
USE kdap;
