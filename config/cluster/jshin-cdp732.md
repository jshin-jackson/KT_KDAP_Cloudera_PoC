# Cloudera 내부 클러스터 — jshin CDP 7.3.2

KT KDAP Flink PoC 테스트용 **클러스터 연결 정보**입니다.  
환경 변수 템플릿: [`.env.example`](../../.env.example) → `cp .env.example .env`

## 클러스터 요약

| 항목 | 값 |
|------|-----|
| CDP Version | 7.3.2 |
| Flink Version | 1.20.1 |
| Python (사용) | **python3.11** (3.11.11) — `python`/`python3`(3.8) 사용 금지 |
| Kerberos | 활성화 |
| Auto-TLS | 활성화 |
| HDFS | **HA nameservice `ns1`** → `hdfs://ns1` |
| HADOOP_CONF_DIR | `/etc/hadoop/conf` |
| YARN Queue | `default` |
| Target DB | `kdap` |

## Kerberos (keytab)

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
klist
```

## HDFS (ns1 nameservice)

**`hdfs dfs` 실행 전** (standby NN 오류 방지):

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
```

```
hdfs dfs -ls hdfs://ns1/
```

스테이징 경로:

```
hdfs://ns1/user/kdap/staging/sdv/
```

## Impala (TLS + Kerberos)

| 항목 | 값 |
|------|-----|
| Host | `ccycloud-5.jshin.root.comops.site:25003` |
| CA Cert | `/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem` |

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT 1"
```

## TLS Truststore (Flink / Spark / Java)

| 항목 | 값 |
|------|-----|
| TRUSTSTORE_PATH | `/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks` |
| TRUSTSTORE_PASSWORD | `changeit` |
| TRUSTSTORE_TYPE | `JKS` |

Flink SQL Client JVM 옵션:

```
-Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS
```

## Hive Metastore / Iceberg

| 항목 | 값 |
|------|-----|
| HMS Hosts | `ccycloud-1.jshin.root.comops.site`, `ccycloud-3.jshin.root.comops.site` (port 9083) |
| HMS URI (HA) | `thrift://ccycloud-1.jshin.root.comops.site:9083,thrift://ccycloud-3.jshin.root.comops.site:9083` |
| Impala (별도) | `ccycloud-5.jshin.root.comops.site:25003` — **HMS 아님** |
| Warehouse | `hdfs://ns1/user/hive/warehouse` |
| Spark Catalog | `spark_catalog` (Hive-type Iceberg) |
| Flink Catalog | `iceberg_hive_catalog` |

HMS URI 확인 (`HADOOP_CONF_DIR` 설정 후):

```
grep hive.metastore.uris /etc/hadoop/conf/hive-site.xml
```

Flink catalog SQL: [flink/conf/00_catalog_setup_jshin.sql](../../flink/conf/00_catalog_setup_jshin.sql)

## PySpark (Parquet → Iceberg 적재)

| 용도 | 명령 |
|------|------|
| 스크립트 일괄 적재 | `spark-submit sdv/load_sdv_to_iceberg.py` |
| 대화형 | `pyspark` + Iceberg `--conf` (runbook 참고) |

사전: `export HADOOP_CONF_DIR=/etc/hadoop/conf` + `kinit`  
Iceberg `spark.sql.extensions` 등은 Session 시작 전 `--conf`로만 설정 가능.

## Flink (CDP 7.3.2 / 1.20.1)

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh
```

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/flink list
```

## SDV 데이터 생성 (Edge 노드)

```
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r sdv/requirements.txt
python3.11 sdv/generate_flink_data.py --scale bts=5000 --scale sgi=1000000 --scale mdt=1000000
```

## 빠른 연결 테스트 (전체)

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
hdfs dfs -ls hdfs://ns1/user/
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SHOW DATABASES"
```

## 관련 Runbook

- Flink 테스트: [flink/cdp-test/COMMANDS.md](../../flink/cdp-test/COMMANDS.md)
- SDV 적재: [docs/runbooks/flink-sdv-data-load.md](../../docs/runbooks/flink-sdv-data-load.md)
- 환경 설정: [docs/runbooks/env-setup.md](../../docs/runbooks/env-setup.md)
