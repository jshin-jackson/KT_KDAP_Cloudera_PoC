# Flink SDV 합성 데이터 — CDP 적재 가이드 (jshin / ns1)

SDV Parquet → `kdap.*` Iceberg 테이블 적재.  
환경 변수: [`.env.example`](../../.env.example) · [env-setup.md](env-setup.md)

**사전:** `python3.11 sdv/generate_flink_data.py` 완료 → `sdv/output/*.parquet`

---

## 0. 세션 초기화 (매번)

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
```

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

---

## 1. Impala — kdap 테이블 생성 (최초 1회)

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_01_create_tables.sql
```

---

## 2. HDFS 스테이징 업로드 (ns1)

```
hdfs dfs -mkdir -p hdfs://ns1/user/kdap/staging/sdv
```

```
hdfs dfs -put -f sdv/output/bts_master.parquet hdfs://ns1/user/kdap/staging/sdv/
```

```
hdfs dfs -put -f sdv/output/cdr_sgi_raw.parquet hdfs://ns1/user/kdap/staging/sdv/
```

```
hdfs dfs -put -f sdv/output/cdr_mdt_smsng_raw.parquet hdfs://ns1/user/kdap/staging/sdv/
```

```
hdfs dfs -ls hdfs://ns1/user/kdap/staging/sdv/
```

---

## 3. PySpark — Parquet → Iceberg (권장)

> **참고:** `-k`는 Impala 전용입니다. PySpark **스크립트는 `spark-submit`**, 대화형은 **`pyspark`** (jshin 클러스터 확인됨).  
> `HADOOP_CONF_DIR` 미설정 시 standby NameNode WARN이 날 수 있습니다 — **0단계를 먼저** 실행하세요.  
> `spark.sql.extensions` 등 Iceberg 설정은 **SparkSession 시작 전**(`--conf` 또는 스크립트)에만 지정 가능합니다.  
> **HMS**는 `ccycloud-1` / `ccycloud-3` (9083). `ccycloud-5`는 Impala 전용 — metastore URI로 쓰면 Connection refused.

HMS URI 확인:

```
grep hive.metastore.uris /etc/hadoop/conf/hive-site.xml
```

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
spark-submit --driver-memory 4g --executor-memory 8g --num-executors 4 sdv/load_sdv_to_iceberg.py
```

(`HADOOP_CONF_DIR` 설정 시 스크립트는 `hive-site.xml`의 metastore URI를 자동 사용)

스크립트: [`sdv/load_sdv_to_iceberg.py`](../../sdv/load_sdv_to_iceberg.py)

대화형으로 실행하려면 Iceberg `--conf`를 **pyspark 시작 시** 넘긴 뒤 적재 코드만 붙여넣습니다  
(catalog 이름 `spark_catalog` — `iceberg.kdap.*` 아님, **`uri` `--conf` 생략** → hive-site.xml 사용):

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
pyspark --driver-memory 4g --executor-memory 8g --num-executors 4 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.spark_catalog.warehouse=hdfs://ns1/user/hive/warehouse \
  --conf spark.hadoop.fs.defaultFS=hdfs://ns1
```

hive-site.xml이 없거나 URI를 명시해야 할 때만 추가:

```
  --conf spark.sql.catalog.spark_catalog.uri=thrift://ccycloud-1.jshin.root.comops.site:9083,thrift://ccycloud-3.jshin.root.comops.site:9083 \
```

```python
staging = "hdfs://ns1/user/kdap/staging/sdv"
spark.read.parquet(f"{staging}/bts_master.parquet").writeTo("spark_catalog.kdap.bts_master").overwritePartitions()
spark.read.parquet(f"{staging}/cdr_sgi_raw.parquet").writeTo("spark_catalog.kdap.cdr_sgi_raw").append()
spark.read.parquet(f"{staging}/cdr_mdt_smsng_raw.parquet").writeTo("spark_catalog.kdap.cdr_mdt_smsng_raw").append()
```

Impala 메타데이터 갱신:

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "REFRESH kdap.bts_master; REFRESH kdap.cdr_sgi_raw; REFRESH kdap.cdr_mdt_smsng_raw; COMPUTE STATS kdap.bts_master; COMPUTE STATS kdap.cdr_sgi_raw; COMPUTE STATS kdap.cdr_mdt_smsng_raw"
```

---

## 4. 적재 검증

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT COUNT(*) AS sgi_cnt FROM kdap.cdr_sgi_raw"
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT COUNT(*) AS sgi_min45 FROM kdap.cdr_sgi_raw WHERE SUBSTR(rqt_st_dt, 11, 2) = '45'"
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT MIN(event_time), MAX(event_time) FROM kdap.cdr_sgi_raw"
```

---

## 5. Flink Job 실행

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
```

---

## SDV 생성 (python3.11)

```
python3.11 -m venv .venv && source .venv/bin/activate && pip install -r sdv/requirements.txt
```

| 프리셋 | 명령 |
|--------|------|
| Smoke | `python3.11 sdv/generate_flink_data.py --scale bts=100 --scale sgi=10000 --scale mdt=10000 --output sdv/output/smoke` |
| Medium | `python3.11 sdv/generate_flink_data.py --scale bts=5000 --scale sgi=1000000 --scale mdt=1000000` |
| Large | `python3.11 sdv/generate_flink_data.py --scale bts=10000 --scale sgi=10000000 --scale mdt=10000000 --output sdv/output/large` |
