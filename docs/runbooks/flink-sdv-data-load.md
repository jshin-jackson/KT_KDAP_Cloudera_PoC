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

> **참고:** `-k`는 Impala 전용입니다. **0단계 `kinit` 이후** `pyspark3`로 실행하세요.

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
pyspark3 --driver-memory 4g --executor-memory 8g --num-executors 4 sdv/load_sdv_to_iceberg.py
```

스크립트: [`sdv/load_sdv_to_iceberg.py`](../../sdv/load_sdv_to_iceberg.py)

대화형으로 실행하려면 `pyspark3`만 실행한 뒤 아래를 붙여넣습니다:

```python
staging = "hdfs://ns1/user/kdap/staging/sdv"
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.spark_catalog.type", "hive")
spark.conf.set("spark.hadoop.fs.defaultFS", "hdfs://ns1")
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.read.parquet(f"{staging}/bts_master.parquet").writeTo("iceberg.kdap.bts_master").overwritePartitions()
spark.read.parquet(f"{staging}/cdr_sgi_raw.parquet").writeTo("iceberg.kdap.cdr_sgi_raw").append()
spark.read.parquet(f"{staging}/cdr_mdt_smsng_raw.parquet").writeTo("iceberg.kdap.cdr_mdt_smsng_raw").append()
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
