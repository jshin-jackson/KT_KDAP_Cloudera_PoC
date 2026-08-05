-- Flink SQL Client: Iceberg Hive Catalog (jshin CDP 7.3.2)
-- HDFS HA: warehouse = hdfs://ns1/...
-- HMS: CM → Hive → Hive Metastore Host 와 다르면 uri 만 수정
--
-- Run (jshin, repo root):
--   export HADOOP_CONF_DIR=/etc/hadoop/conf
--   kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
--   ./flink/run_catalog_setup.sh
--
-- Manual:
--   /opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded \
--     -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks \
--     -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS \
--     -f flink/conf/00_catalog_setup_jshin.sql

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
