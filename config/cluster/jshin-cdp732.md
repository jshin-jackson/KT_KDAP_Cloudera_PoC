# Cloudera 내부 클러스터 — jshin CDP 7.3.2

KT KDAP Flink PoC 테스트용 **고정 클러스터 정보**입니다.

| 항목 | 값 |
|------|-----|
| CDP Version | 7.3.2 |
| Flink Version | 1.20.1 |
| Python | 3.11 |
| Kerberos | 활성화 |
| Auto-TLS | 활성화 |
| Impala Host | `ccycloud-5.jshin.root.comops.site:25003` |
| Kerberos Principal | `systest@QE-INFRA-AD.CLOUDERA.COM` |
| Keytab | `/cdep/keytabs/systest.keytab` |

## Impala 접속 (매 명령 앞에 사용)

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem
```

파일 실행 예:

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_01_create_tables.sql
```

쿼리 한 줄 실행 예:

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SHOW DATABASES"
```

## Kerberos (keytab)

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
klist
```

## TLS Truststore (Flink / Java 클라이언트)

| 항목 | 값 |
|------|-----|
| TRUSTSTORE_PATH | `/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks` |
| TRUSTSTORE_PASSWORD | `changeit` |
| TRUSTSTORE_TYPE | `JKS` |

## Hive Metastore (Flink Iceberg Catalog)

CM → Hive → **Hive Metastore Host** 확인.  
현재 Impala 와 동일 노드로 가정:

```
thrift://ccycloud-5.jshin.root.comops.site:9083
```

HMS 호스트가 다르면 `flink/conf/00_catalog_setup_jshin.sql` 의 URI 만 수정하세요.

## Flink 바이너리 경로 (CDP 7.3.2 / Flink 1.20.1)

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh
```

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/flink
```

Parcel 경로 확인:

```
ls /opt/cloudera/parcels/FLINK/lib/flink/bin/
```

## Spark/YARN Principal (Flink on YARN 시)

| 항목 | 값 |
|------|-----|
| SPARK_YARN_PRINCIPAL | `systest@QE-INFRA-AD.CLOUDERA.COM` |
| SPARK_YARN_KEYTAB | `/cdep/keytabs/systest.keytab` |

## 빠른 연결 테스트

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT 1"
```

```
hdfs dfs -ls /
```
