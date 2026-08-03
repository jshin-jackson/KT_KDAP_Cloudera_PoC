# Flink — KT KDAP PoC (시나리오 2)

5분 TUMBLE 윈도우 기반 실시간 집계 Flink SQL Job (CDP 7.3.2 / Flink 1.20.1)

## 환경 설정

```
cp .env.example .env
export HADOOP_CONF_DIR=/etc/hadoop/conf
```

→ [docs/runbooks/env-setup.md](../docs/runbooks/env-setup.md) · [config/cluster/jshin-cdp732.md](../config/cluster/jshin-cdp732.md)

## SDV 대용량 합성 데이터

```
python3.11 sdv/generate_flink_data.py --scale bts=5000 --scale sgi=1000000 --scale mdt=1000000
hdfs dfs -put -f sdv/output/cdr_sgi_raw.parquet hdfs://ns1/user/kdap/staging/sdv/
```

→ [sdv/README.md](../sdv/README.md) · [docs/runbooks/flink-sdv-data-load.md](../docs/runbooks/flink-sdv-data-load.md)

## jshin 클러스터 — 빠른 시작

→ [cdp-test/COMMANDS.md](cdp-test/COMMANDS.md)

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_01_create_tables.sql
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
```

## Job 파일

| 파일 | 설명 |
|------|------|
| `conf/00_catalog_setup_jshin.sql` | Iceberg Catalog (`hdfs://ns1`, HMS) |
| `conf/00_catalog_setup.sql` | 일반 클러스터용 placeholder |
| `ltas_5min.sql` | LTAS Job |
| `sgi_5min_v1.sql` | SGi v1 Job |
| `sgi_5min_v2.sql` | SGi v2 Job |
| `mdt_5min.sql` | MDT Job |

## jshin 클러스터 요약

| 항목 | 값 |
|------|-----|
| HDFS | `hdfs://ns1` · `HADOOP_CONF_DIR=/etc/hadoop/conf` |
| Impala | `ccycloud-5.jshin.root.comops.site:25003` + `--ssl` |
| Python | **python3.11** (SDV) |
| Kerberos | `kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM` |
| Truststore | `/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks` |
| Iceberg warehouse | `hdfs://ns1/user/hive/warehouse` |
| Target DB | `kdap` |
