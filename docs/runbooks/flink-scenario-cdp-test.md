# Flink 시나리오 2 — CDP 클러스터 테스트 가이드

KT KDAP PoC **시나리오 2 (Flink 5분 TUMBLE 실시간 집계)** 를 Cloudera 내부 CDP 클러스터에서 검증하는 절차입니다.

**전제:** Shell/Python 스크립트 없이 **명령어만** 복사해서 실행합니다.

---

## ★ jshin 내부 클러스터 (CDP 7.3.2) — 바로 실행

| 항목 | 값 |
|------|-----|
| CDP | 7.3.2 / Flink 1.20.1 |
| HDFS | **ns1** → `hdfs://ns1` · `HADOOP_CONF_DIR=/etc/hadoop/conf` |
| Impala | `ccycloud-5.jshin.root.comops.site:25003` (TLS) |
| Python | **python3.11** (SDV — `python3` 3.8 parcel 사용 금지) |
| Kerberos | `systest@QE-INFRA-AD.CLOUDERA.COM` + keytab |
| Keytab | `/cdep/keytabs/systest.keytab` |
| Iceberg warehouse | `hdfs://ns1/user/hive/warehouse` |

**환경 설정:** [docs/runbooks/env-setup.md](../../docs/runbooks/env-setup.md) · `cp .env.example .env`  
**명령어 전체:** [flink/cdp-test/COMMANDS.md](../../flink/cdp-test/COMMANDS.md)  
**클러스터 상세:** [config/cluster/jshin-cdp732.md](../../config/cluster/jshin-cdp732.md)  
**Catalog SQL:** [flink/conf/00_catalog_setup_jshin.sql](../../flink/conf/00_catalog_setup_jshin.sql)

### 최소 실행

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
```

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_01_create_tables.sql
```

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
```

---

## 0. 시나리오 개요

| Job | 소스 테이블 | Sink 테이블 | 배치 대응 SQL |
|-----|-------------|-------------|---------------|
| F-LTAS | `kdap.cdr_sgi_raw` | `kdap.ltas_5min_flink` | WF_N02960 |
| F-SGi-1 | `kdap.cdr_sgi_raw` | `kdap.sgi_5min_flink` | WF_M07046 v1 |
| F-SGi-2 | `kdap.cdr_sgi_raw` | `kdap.sgi_5min_flink_v2` | WF_M07046 v2 |
| F-MDT | `kdap.cdr_mdt_smsng_raw` | `kdap.mdt_5min_flink` | WF_N05923 |

**실시간 필터 조건:** `SUBSTR(rqt_st_dt, 11, 2) = '45'` (45분대 데이터만 집계)

**윈도우:** 5분 TUMBLE + WATERMARK 10초

---

## 1. 사전 확인 (Cloudera Manager)

Cloudera Manager 웹 UI에서 아래 서비스가 **Started / Green** 인지 확인합니다.

- HDFS
- Hive Metastore (HMS)
- Impala
- **Flink** (또는 Flink on YARN)
- **SQL Stream Builder (SSB)** — 선택 (UI로도 제출 가능)

Flink Gateway 노드 호스트명을 메모합니다. (예: `flink-gateway.example.com`)

---

## 2. Kerberos 로그인

### jshin 클러스터 (keytab)

```
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

```
klist
```

### 일반 클러스터 (패스워드)

```
kinit <본인계정>@<KERBEROS_REALM>
```

```
klist
```

---

## 3. Impala — 테스트 테이블 생성

프로젝트 디렉터리로 이동 (경로는 환경에 맞게 수정):

```
cd /path/to/KT_KDAP_Cloudera_PoC
```

테이블 DDL 실행:

**jshin 클러스터:**

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_01_create_tables.sql
```

**일반 클러스터:**

```
impala-shell -k -f flink/cdp-test/impala_01_create_tables.sql
```

성공 확인:

```
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -q "SHOW TABLES IN kdap"
```

---

## 4. Impala — 샘플 데이터 적재

```
impala-shell -k -f flink/cdp-test/impala_02_sample_data.sql
```

건수 확인:

```
impala-shell -k -q "SELECT COUNT(*) FROM kdap.cdr_sgi_raw"
```

```
impala-shell -k -q "SELECT COUNT(*) FROM kdap.cdr_mdt_smsng_raw"
```

```
impala-shell -k -q "SELECT COUNT(*) FROM kdap.bts_master"
```

---

## 5. Flink SQL Client 접속

Flink Gateway 노드에서 SQL Client 실행:

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded
```

> Parcel 경로가 다르면 CM → Flink → Configuration → **FLINK_HOME** 확인 후 해당 `bin/sql-client.sh` 사용

---

## 6. Flink — Iceberg Catalog 설정

**jshin 클러스터**는 `flink/conf/00_catalog_setup_jshin.sql` 사용 (HMS: `ccycloud-5.jshin.root.comops.site:9083`).

**방법 A — 파일 적용 (권장, jshin + TLS)**

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql
```

**방법 B — 일반 클러스터**

`flink/conf/00_catalog_setup.sql` 에서 `<HMS_HOST>` 수정 후:

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -f flink/conf/00_catalog_setup.sql
```

---

## 7. Flink Job 제출 — 시나리오별 명령

각 Job은 **별도 터미널**에서 SQL Client를 띄우거나, `-f` 옵션으로 파일 제출합니다.

### 7-1. F-LTAS (LTAS 5분 집계) — jshin

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -Djavax.net.ssl.trustStore=/var/lib/cloudera-scm-agent/agent/cert/cm-auto-global_cacerts.jks -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS -f flink/conf/00_catalog_setup_jshin.sql -f flink/ltas_5min.sql
```

### 7-2. F-SGi-1 (SGi 5분 집계 v1)

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -f flink/conf/00_catalog_setup.sql -f flink/sgi_5min_v1.sql
```

### 7-3. F-SGi-2 (SGi 5분 집계 v2)

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -f flink/conf/00_catalog_setup.sql -f flink/sgi_5min_v2.sql
```

### 7-4. F-MDT (MDT 5분 집계)

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/sql-client.sh embedded -f flink/conf/00_catalog_setup.sql -f flink/mdt_5min.sql
```

> Job이 `RUNNING` 상태로 유지되면 정상입니다. INSERT INTO streaming job 은 종료하지 않습니다.

---

## 8. 스트리밍 데이터 추가 (Job 실행 중)

Flink Job이 **RUNNING** 인 상태에서, **새 터미널**을 열고 Impala로 추가 데이터를 넣습니다.
(Iceberg streaming source는 새 snapshot/commit 을 감지합니다.)

```
impala-shell -k -q "INSERT INTO kdap.cdr_sgi_raw (sgi_id, cell_id, alt_cell_id, rqt_st_dt, etl_date, val, use_flag, signal_type, event_time) VALUES ('SGI007', 'CELL_A', 'CELL_A_ALT', '20260803124555', '20260803', 110, 'Y', 'SGI', NOW())"
```

```
impala-shell -k -q "REFRESH kdap.cdr_sgi_raw"
```

MDT 추가:

```
impala-shell -k -q "INSERT INTO kdap.cdr_mdt_smsng_raw (mdt_id, gnss_utmkx, gnss_utmky, rqt_st_dt, bts_market_nm, event_time) VALUES ('MDT005', 965000.0, 1950000.0, '20260803124550', 'SAMSUNG', NOW())"
```

```
impala-shell -k -q "REFRESH kdap.cdr_mdt_smsng_raw"
```

> **주의:** `rqt_st_dt` 11~12번째 문자가 `'45'` 이어야 Flink WHERE 조건을 통과합니다.  
> 예: `'20260803124500'` → 11~12번째 = `45`

---

## 9. 결과 확인 (Impala)

5분 윈도우가 닫힌 후 (Job 제출 후 **5~6분** 대기):

```
impala-shell -k -f flink/cdp-test/impala_03_validate.sql
```

개별 확인 예시:

```
impala-shell -k -q "REFRESH kdap.ltas_5min_flink"
```

```
impala-shell -k -q "SELECT * FROM kdap.ltas_5min_flink ORDER BY window_start DESC LIMIT 5"
```

```
impala-shell -k -q "SELECT * FROM kdap.sgi_5min_flink ORDER BY window_start DESC LIMIT 5"
```

```
impala-shell -k -q "SELECT * FROM kdap.mdt_5min_flink ORDER BY window_start DESC LIMIT 5"
```

---

## 10. Flink Job 상태 확인

### Flink Web UI

CM → Flink → **Web UI** 링크 클릭 → **Running Jobs** 확인

또는 Gateway 노드에서:

```
curl -s http://<FLINK_JOBMANAGER_HOST>:8081/jobs/overview
```

### YARN Application (Flink on YARN 인 경우)

```
yarn application -list -appStates RUNNING
```

특정 Application 상세:

```
yarn application -status <application_id>
```

---

## 11. Flink Job 중지

Flink Web UI → Job 선택 → **Cancel**

또는 CLI:

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/flink list
```

```
/opt/cloudera/parcels/FLINK/lib/flink/bin/flink cancel <JOB_ID>
```

---

## 12. SQL Stream Builder (SSB) 로 제출 — UI 대안

명령어 대신 UI를 선호하면:

1. CM → SQL Stream Builder → **Open**
2. Project 생성: `kdap-poc-flink`
3. `flink/conf/00_catalog_setup.sql` 내용 붙여넣기 → Execute
4. `flink/ltas_5min.sql` (또는 sgi/mdt) 붙여넣기 → **Deploy**
5. SSB Dashboard에서 Running / Checkpoint 상태 확인

---

## 13. 검증 체크리스트

| # | 확인 항목 | 명령 / 방법 | 기대 결과 |
|---|-----------|-------------|-----------|
| 1 | 소스 데이터 | `impala-shell -k -q "SELECT COUNT(*) FROM kdap.cdr_sgi_raw"` | ≥ 1 |
| 2 | Job Running | Flink Web UI | State = RUNNING |
| 3 | Checkpoint | Flink Web UI → Checkpoints | SUCCESS |
| 4 | Sink 적재 | `SELECT COUNT(*) FROM kdap.ltas_5min_flink` | 윈도우 후 ≥ 1 |
| 5 | 분 필터 | rqt_st_dt 11~12 = '45' 아닌 행 | sink 에 미포함 |
| 6 | 5분 윈도우 | window_end - window_start | 5 minutes |

---

## 14. KT PoC 본番 전환 (내부 테스트 이후)

내부 CDP 테스트 성공 후 KT 판교 환경에서는:

1. `flink/cdp-test/` 대신 실제 데이터 경로로 `cdr_sgi_raw`, `cdr_mdt_smsng_raw` 적재
2. `flink/conf/00_catalog_setup.sql` 의 `<HMS_HOST>` 를 KT 클러스터 HMS 로 변경
3. 동일 Flink SQL 파일(`flink/ltas_5min.sql` 등) 그대로 `-f` 로 제출
4. 배치 결과(`sum_ltas_cdr_tm` 등)와 Flink sink 건수 비교

배치 대비 비교 쿼리:

```
impala-shell -k -q "SELECT COUNT(*) AS batch_cnt FROM kdap.sum_ltas_cdr_tm WHERE etl_date='20251201'"
```

```
impala-shell -k -q "SELECT SUM(record_cnt) AS flink_cnt FROM kdap.ltas_5min_flink"
```

---

## 15. 자주 발생하는 문제

| 증상 | 원인 | 해결 명령 |
|------|------|-----------|
| Catalog not found | catalog 미생성 | 6단계 `CREATE CATALOG` 재실행 |
| Sink 항상 0건 | rqt_st_dt 분 필터 | 11~12번째 = `'45'` 인 데이터 확인 |
| Sink 항상 0건 | WATERMARK 지연 | `event_time` 을 `NOW()` 근처로 INSERT |
| Permission denied | Kerberos 만료 | `kinit` 재실행 |
| Table not found | Impala 메타 stale | `REFRESH kdap.<테이블명>` |
| Job FAILED | HMS 연결 | `<HMS_HOST>:9083` 연결 및 방화벽 확인 |

---

## 16. 관련 파일

| 파일 | 용도 |
|------|------|
| `flink/conf/00_catalog_setup.sql` | Iceberg Hive Catalog |
| `flink/ltas_5min.sql` | LTAS Flink Job |
| `flink/sgi_5min_v1.sql` | SGi v1 Flink Job |
| `flink/sgi_5min_v2.sql` | SGi v2 Flink Job |
| `flink/mdt_5min.sql` | MDT Flink Job |
| `flink/cdp-test/impala_01_create_tables.sql` | CDP 테스트 DDL |
| `flink/cdp-test/impala_02_sample_data.sql` | 샘플 데이터 |
| `flink/cdp-test/impala_03_validate.sql` | 결과 검증 |

문의: Flink PS **신정훈** / 튜닝 **박소희, 이지환**
