# Flink CDP 테스트 — jshin 클러스터 명령어 (CDP 7.3.2)

**Cluster:** `ccycloud-5.jshin.root.comops.site`  
**Kerberos + Auto-TLS** 환경. 위에서부터 순서대로 복사 실행하세요.

---

## 1. Kerberos 로그인 (keytab)

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
klist
```

---

## 2. 연결 테스트

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT 1"
```

---

## 3. 프로젝트 폴더 이동

```
cd /path/to/KT_KDAP_Cloudera_PoC
```

---

## 4. Impala — kdap 테스트 테이블 생성

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_01_create_tables.sql
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SHOW TABLES IN kdap"
```

---

## 5. Impala — 샘플 데이터 적재

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_02_sample_data.sql
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT COUNT(*) FROM kdap.cdr_sgi_raw"
```

---

## 6. Flink — Kerberos 재확인 (Job 제출 직전)

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
klist
```

---

## 7. Flink Job — LTAS (5분 TUMBLE)

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
```

---

## 8. Flink Job — SGi v1 (새 터미널, kinit 후 실행)

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/sgi_5min_v1.sql
```

---

## 9. Flink Job — SGi v2 (새 터미널)

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/sgi_5min_v2.sql
```

---

## 10. Flink Job — MDT (새 터미널)

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/mdt_5min.sql
```

---

## 11. 5~6분 대기 후 Impala 결과 확인

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_03_validate.sql
```

개별 확인:

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "REFRESH kdap.ltas_5min_flink"
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SELECT * FROM kdap.ltas_5min_flink ORDER BY window_start DESC LIMIT 5"
```

---

## 12. (선택) Job 실행 중 스트리밍 데이터 추가

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "INSERT INTO kdap.cdr_sgi_raw (sgi_id, cell_id, alt_cell_id, rqt_st_dt, etl_date, val, use_flag, signal_type, event_time) VALUES ('SGI007', 'CELL_A', 'CELL_A_ALT', '20260803124555', '20260803', 110, 'Y', 'SGI', NOW())"
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "REFRESH kdap.cdr_sgi_raw"
```

---

## 13. Flink Job 상태 확인

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/flink list
```

YARN 에서 확인:

```
yarn application -list -appStates RUNNING
```

---

## 14. Flink Job 중지

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/flink cancel <JOB_ID>
```

---

## 참고

- 클러스터 상세: [config/cluster/jshin-cdp732.md](../../config/cluster/jshin-cdp732.md)
- 전체 설명: [docs/runbooks/flink-scenario-cdp-test.md](../../docs/runbooks/flink-scenario-cdp-test.md)
