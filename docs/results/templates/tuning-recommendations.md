# KT KDAP 튜닝 권고안 (Template)

## Iceberg / Storage

1. **Partition keys:** Use `etl_date` / `base_date` identity partitions on facts; `days(event_time)` on streaming tables.
2. **File sizing:** Target 512MB–1GB Parquet files; run compaction after bulk load.
3. **Sort order:** `WRITE ORDER BY` join keys (`cell_id`, `lookup_key`) before batch benchmarks.
4. **Stats:** Schedule `COMPUTE INCREMENTAL STATS` after daily partition landing.

## Impala (Base)

1. **Admission pools:** Separate pools per batch job (see `config/resource_pools/impala-admission-pools.yaml`).
2. **Memory:** SGi v1 query — allocate ≥120GB `mem_limit`; enable spill monitoring.
3. **OR joins:** Evaluate materialized intermediate Iceberg table for BTS OR-join path.
4. **MDT spatial:** Pre-filter with bounding box before `ST_DISTANCE`.

## CDW (MSTR)

1. **VW sizing:** 12 executors × 64GB for 4-way MSTR concurrency.
2. **Always-on VW:** Prevent cold-start latency in production BI.
3. **Query timeout:** Set ≥7200s for VoLTE-style queries until optimized.

## Flink

1. **Checkpoint interval:** 5 min aligned with TUMBLE window.
2. **State TTL:** 1h for late-arriving CDR events.
3. **Dim join:** Use `FOR SYSTEM_TIME AS OF` temporal join for BTS.

## Kudu vs Iceberg (Lookup)

Document benchmark outcome:

| Criterion | Kudu | Iceberg |
|-----------|------|---------|
| p95 lookup | | |
| Ops complexity | Medium | Low |
| **Recommendation** | | |

## Resource Pools (Production)

```
root.kdap_batch.{pool_ltas, pool_sgi_1, pool_sgi_2, pool_mdt}
root.kdap_flink.pool_flink_tm
root.kdap_mstr.pool_mstr_base
CDW VW (isolated)
```

Adjust weights when SGi v2 consumes v1 output (3 vs 4 pools).
