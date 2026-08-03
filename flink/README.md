# Flink — KT KDAP PoC (시나리오 2)

5분 TUMBLE 윈도우 기반 실시간 집계 Flink SQL Job (CDP 7.3.2 / Flink 1.20.1)

## jshin 내부 클러스터 — 바로 시작

→ **[cdp-test/COMMANDS.md](cdp-test/COMMANDS.md)** (명령어 전체)  
→ **[../config/cluster/jshin-cdp732.md](../config/cluster/jshin-cdp732.md)** (연결 정보)

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_01_create_tables.sql
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_02_sample_data.sql
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
```

## Job 파일

| 파일 | 설명 |
|------|------|
| `conf/00_catalog_setup_jshin.sql` | **jshin** Iceberg Catalog (HMS + TLS) |
| `conf/00_catalog_setup.sql` | 일반 클러스터용 (HMS placeholder) |
| `ltas_5min.sql` | LTAS Job |
| `sgi_5min_v1.sql` | SGi v1 Job |
| `sgi_5min_v2.sql` | SGi v2 Job |
| `mdt_5min.sql` | MDT Job |

## CDP 테스트 Impala SQL

| 파일 | 용도 |
|------|------|
| `cdp-test/impala_01_create_tables.sql` | DDL |
| `cdp-test/impala_02_sample_data.sql` | 샘플 INSERT |
| `cdp-test/impala_03_validate.sql` | 결과 확인 |

## jshin 클러스터 요약

| 항목 | 값 |
|------|-----|
| Impala | `ccycloud-5.jshin.root.comops.site:25003` + `--ssl` |
| Kerberos | `kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM` |
| Truststore | `/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks` |
