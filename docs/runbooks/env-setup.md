# CDP Edge 환경 설정 (jshin 클러스터)

KT KDAP PoC를 Cloudera Edge 노드에서 실행하기 위한 초기 설정입니다.

## 1. 환경 변수 파일

```
cd /path/to/KT_KDAP_Cloudera_PoC
cp .env.example .env
```

`.env` 는 git에 커밋하지 않습니다 (`.gitignore`).

## 2. HDFS HA (필수)

`hdfs dfs` / Spark / distcp 실행 **전** 매 세션:

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
```

standby NameNode `host:8020` 오류를 방지하려면 nameservice **`ns1`** 을 사용합니다:

```
hdfs dfs -ls hdfs://ns1/user/
```

## 3. Kerberos

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
klist
```

## 4. Python (SDV)

CDP Edge의 `python3`(3.8)이 아닌 **python3.11** 사용:

```
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r sdv/requirements.txt
```

```
python3.11 sdv/generate_flink_data.py --scale bts=5000 --scale sgi=1000000 --scale mdt=1000000
```

## 5. Impala 연결 확인

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT 1"
```

## 6. PySpark Iceberg (선택 — SDV Parquet 적재)

> **참고:** `-k`는 Impala 전용입니다. **3단계 `kinit` 이후** `pyspark3`로 실행하세요.

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
pyspark3 sdv/load_sdv_to_iceberg.py
```

대화형 확인:

```python
spark.sql("SHOW TABLES IN kdap").show()
```

## 7. Flink TLS

Flink SQL Client 실행 시 truststore JVM 옵션 포함 (Auto-TLS):

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
```

## 다음 단계

- Flink 테스트: [flink/cdp-test/COMMANDS.md](../../flink/cdp-test/COMMANDS.md)
- SDV 생성 + 적재: [flink-sdv-data-load.md](flink-sdv-data-load.md)
- 클러스터 상세: [config/cluster/jshin-cdp732.md](../../config/cluster/jshin-cdp732.md)
