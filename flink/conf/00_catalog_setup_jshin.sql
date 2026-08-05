-- Flink SQL Client: Iceberg Hive Catalog (jshin CDP 7.3.2)
-- HDFS HA: warehouse = hdfs://ns1/...
-- HMS: CM → Hive → Hive Metastore Host 와 다르면 uri 만 수정
--
-- Run (jshin, repo root):
--   export HADOOP_CONF_DIR=/etc/hadoop/conf
--   kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
--   ./flink/run_catalog_setup.sh
--
-- Manual (flink-sql-client):
--   /opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded \
--     -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks \
--     -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS \
--     -f flink/conf/00_catalog_setup_jshin.sql
-- Fallback (SSB):
--   FLINK_SUBMIT_BACKEND=ssb ./flink/run_catalog_setup.sh

CREATE CATALOG IF NOT EXISTS iceberg_hive_catalog WITH (
  'type'             = 'iceberg',
  'catalog-type'     = 'hive',
  'uri'              = 'thrift://ccycloud-1.jshin.root.comops.site:9083,thrift://ccycloud-3.jshin.root.comops.site:9083',
  'warehouse'        = 'hdfs://ns1/user/hive/warehouse',
  'hive-conf-dir'    = '/opt/cloudera/parcels/CDH/lib/hive/conf',
  'hadoop-conf-dir'  = '/etc/hadoop/conf',
  'clients'          = '5',
  'property-version' = '1'
);

USE CATALOG iceberg_hive_catalog;
USE kdap;
-- Job SQL (-f) 은 default_catalog 로 전환 후 connector 테이블을 생성합니다.
-- iceberg catalog 안에서 'connector'='iceberg' DDL 은 허용되지 않습니다.
