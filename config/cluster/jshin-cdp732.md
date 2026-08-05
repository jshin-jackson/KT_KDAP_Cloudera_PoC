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

## Flink (CDP 7.3.2 / 1.20.1 / CSA)

jshin 확인 경로:

| 항목 | 경로 |
|------|------|
| FLINK_HOME | `/opt/cloudera/parcels/FLINK` |
| CSA_FLINK (lib/flink) | `/opt/cloudera/parcels/FLINK/lib/flink` |
| flink-sql-client | `/opt/cloudera/parcels/FLINK/bin/flink-sql-client` |
| SSB Web UI | CM → SQL Stream Builder (port **8082**) |
| SSB REST API | SSB UI → **API Explorer** → Base URL |

### CSA SQL Client bootstrap (jshin Edge Node — 적용 완료)

CSA parcel에 Apache Flink 1.20.1 SQL Client 파일 복사:

```
CSA_FLINK=/opt/cloudera/parcels/FLINK-1.20.1-csa1.17.1.0-81475796/lib/flink
cp ~/flink-1.20.1/bin/sql-client.sh $CSA_FLINK/bin/
chmod +x $CSA_FLINK/bin/sql-client.sh
chown flink:flink $CSA_FLINK/bin/sql-client.sh
cp ~/flink-1.20.1/opt/flink-sql-client-1.20.1.jar $CSA_FLINK/lib/
cp ~/flink-1.20.1/opt/flink-sql-gateway-1.20.1.jar $CSA_FLINK/lib/
# flink-sql-connector-hive 는 lib/ 에 넣지 않음 (INSERT Calcite 충돌)
# HiveCatalog 클래스는 run_*.sh 가 CDH/jars 의 hive-metastore 등 실제 경로를 -j 로 전달
rm -f $CSA_FLINK/lib/flink-sql-connector-hive-*.jar
chown flink:flink $CSA_FLINK/lib/flink-sql-client-*.jar $CSA_FLINK/lib/flink-sql-gateway-*.jar $CSA_FLINK/lib/iceberg-flink-runtime-*.jar
```

| Apache Flink 원본 | parcel 대상 |
|-------------------|-------------|
| `bin/sql-client.sh` | `$CSA_FLINK/bin/sql-client.sh` |
| `opt/flink-sql-client-*.jar` | `$CSA_FLINK/lib/flink-sql-client-*.jar` |
| `opt/flink-sql-gateway-*.jar` | `$CSA_FLINK/lib/flink-sql-gateway-*.jar` |
| `iceberg-flink-runtime-1.20-*.jar` | `$CSA_FLINK/lib/iceberg-flink-runtime-*.jar` |

확인: `/opt/cloudera/parcels/FLINK/bin/flink-sql-client --help`

### PoC 실행 순서 (SSB — **권장**, Iceberg Hive catalog)

```
cp .env.example .env   # SSB_API_BASE — 호스트명은 API Explorer 값으로 확인
export HADOOP_CONF_DIR=/etc/hadoop/conf
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
cd ~/KT_KDAP_Cloudera_PoC
./flink/run_ltas_5min.sh
```

> sql-client embedded 모드는 CSA에서 Iceberg `HiveCatalog` classpath(HMS Thrift API)가 불안정합니다.  
> `flink-sql-connector-hive`를 lib/에 두면 catalog는 되지만 INSERT Calcite 충돌. CDH `-j`만으로는 `NoSuchObjectException` 지속.

### PoC 실행 순서 (sql-client — 실험용)

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/opt/cloudera/parcels/CDH/lib/hive/conf
export HIVE_HOME=/opt/cloudera/parcels/CDH/lib/hive
export HADOOP_CLASSPATH=$(hadoop classpath)
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
cd ~/KT_KDAP_Cloudera_PoC
FLINK_SUBMIT_BACKEND=sql-client ./flink/run_ltas_5min.sh
```

직접 호출 (`-i` catalog, `-f` job — `-f` 두 번 쓰면 첫 파일만 실행됨):

```
/opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded \
  -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks \
  -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS \
  -i flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
```

**자동 선택 (`FLINK_SUBMIT_BACKEND=auto`):** `.env`에 `SSB_API_BASE` 있으면 SSB, 없으면 sql-client(bootstrap 시) → SSB 권장.

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
