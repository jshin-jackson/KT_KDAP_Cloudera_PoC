# KT KDAP 운영 Runbook

Daily operations guide for post-PoC production handoff.

## 1. Daily Batch (Scenario 1)

```bash
export ETL_DATE=$(date +%Y%m%d)
export BASE_DATE=${ETL_DATE}

# Sequential or concurrent per SLA
./scripts/benchmark/run_scenario1_batch.sh concurrent

# Post-run
./scripts/data_migration/compute_stats.sh
./scripts/profile/collect_profiles.sh docs/results/latest
```

**Monitoring:** CM -> Impala Queries; check pool queue depth for `pool_sgi_batch_1`.

**Failure recovery:** Re-run individual SQL from `sql/batch/` with same date vars.

## 2. Real-time Flink (Scenario 2)

| Job | SQL | SSB Project |
|-----|-----|-------------|
| LTAS | `flink/ltas_5min.sql` | kdap-ltas |
| SGi v1 | `flink/sgi_5min_v1.sql` | kdap-sgi |
| MDT | `flink/mdt_5min.sql` | kdap-mdt |

**Health checks:**
- Flink Dashboard: checkpoint success, no sustained backpressure
- `./scripts/benchmark/validate_scenario2_flink.sh` (daily reconciliation)

**Restart procedure:**
1. Save savepoint
2. Cancel job via SSB/Flink UI
3. Redeploy from savepoint

## 3. MSTR / CDW (Scenario 3)

- JDBC endpoint: see `config/cdw/virtual-warehouse.yaml`
- Pre-warm VW before business hours: `SELECT 1`
- Concurrent limit: 4 queries — scale VW if queue > 2 min

## 4. Call Quality Lookup (Scenario 4)

- Production table: `call_quality_kudu` or `call_quality_iceberg` (per PoC outcome)
- SLA alert: p95 > 3 sec for 5 consecutive minutes
- Load incremental CDR via nightly Spark/Kudu upsert job

## 5. Integrated Operations (Scenario 5)

**Schedule overlap matrix:**

| Window | Batch | Flink | MSTR |
|--------|-------|-------|------|
| 00:00-06:00 | Primary | Running | Low |
| 08:00-18:00 | Avoid peak | Running | Primary |

**Escalation:**
- YARN RM queue saturation → reduce batch concurrency
- CDW rejections → scale VW executors
- HDFS disk > 80% → compaction + archive old partitions

## 6. Contacts

| Issue | Contact |
|-------|---------|
| Cluster / CM | 허종진, 김재현 |
| Query tuning | 박소희, 이지환 |
| Flink | 신정훈 |
| KT data | 고태헌 |

## 7. Useful Commands

```bash
# Kerberos
kinit kdap_svc@REALM

# Table freshness
impala-shell -q "REFRESH kdap.cdr_sgi; SHOW FILES IN kdap.cdr_sgi PARTITION (etl_date='20251201');"

# Pool usage
impala-shell -q "SELECT * FROM sys.impala_resource_pool_usage;"

# YARN queues
yarn queue -status root.kdap_batch.pool_ltas_batch
```
