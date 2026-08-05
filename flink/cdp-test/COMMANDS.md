# Flink CDP 테스트 — jshin 클러스터 (CDP 7.3.2 / ns1)

**Cluster:** `ccycloud-5.jshin.root.comops.site` · **HDFS:** `hdfs://ns1`  
환경 설정: [docs/runbooks/env-setup.md](../../docs/runbooks/env-setup.md) · `cp .env.example .env`

---

## 0. 세션 초기화

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
```

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
klist
```

---

## 1. 연결 테스트

```
hdfs dfs -ls hdfs://ns1/user/
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT 1"
```

---

## 2. 프로젝트 폴더

```
cd /path/to/KT_KDAP_Cloudera_PoC
```

---

## 3. Impala — kdap 테이블 생성

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_01_create_tables.sql
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SHOW TABLES IN kdap"
```

---

## 4. 샘플 또는 SDV 데이터

**소량 샘플:**

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_02_sample_data.sql
```

**SDV 대용량 (python3.11):**

```
python3.11 -m venv .venv && source .venv/bin/activate && pip install -r sdv/requirements.txt
```

```
python3.11 sdv/generate_flink_data.py --scale bts=5000 --scale sgi=1000000 --scale mdt=1000000
```

→ SDV 적재: [docs/runbooks/flink-sdv-data-load.md](../../docs/runbooks/flink-sdv-data-load.md)

---

## 5. Flink SQL Job 제출

**세션 초기화 (매 터미널):**

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
cd /path/to/KT_KDAP_Cloudera_PoC
```

### 방법 A — Job별 스크립트 (권장)

```
./flink/run_ltas_5min.sh
```

```
./flink/run_sgi_5min_v1.sh
```

```
./flink/run_sgi_5min_v2.sh
```

```
./flink/run_mdt_5min.sh
```

### 방법 B — 수동 (flink-sql-client)

**F-LTAS:**

```
/opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
```

**F-SGi-1 / F-SGi-2 / F-MDT (kinit 후 SQL 추가):**

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
/opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/sgi_5min_v1.sql
```

```
/opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/sgi_5min_v2.sql
```

```
/opt/cloudera/parcels/FLINK/bin/flink-sql-client embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/mdt_5min.sql
```

---

## 6. 결과 확인 (5~6분 후)

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_03_validate.sql
```

---

## 7. Job 상태

```
/opt/cloudera/parcels/FLINK/bin/flink list
```

```
/opt/cloudera/parcels/FLINK/bin/flink cancel <JOB_ID>
```

---

클러스터 상세: [config/cluster/jshin-cdp732.md](../../config/cluster/jshin-cdp732.md)
