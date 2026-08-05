-- Flink SQL Client: Iceberg Hive Catalog (jshin CDP 7.3.2)
-- HDFS HA: warehouse = hdfs://ns1/...
-- HMS: CM → Hive → Hive Metastore Host 와 다르면 uri 만 수정

CREATE CATALOG IF NOT EXISTS iceberg_hive_catalog WITH (
  'type'             = 'iceberg',
  'catalog-type'     = 'hive',
  'uri'              = 'thrift://ccycloud-1.jshin.root.comops.site:9083,thrift://ccycloud-3.jshin.root.comops.site:9083',
  'warehouse'        = 'hdfs://ns1/user/hive/warehouse',
  'clients'          = '5',
  'property-version' = '1'
);

USE CATALOG iceberg_hive_catalog;
USE kdap;
