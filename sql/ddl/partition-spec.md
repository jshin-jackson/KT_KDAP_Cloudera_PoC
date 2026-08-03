# Iceberg partition specification (draft — finalize after full PoC dataset)

## Principles

1. Align partitions with **WHERE clauses** used in PoC:
   - Batch: `ETL_DATE`, `BASE_DATE`
   - Real-time: `days(event_time)` + minute filter on `RQT_ST_DT`
2. Avoid over-partitioning small dimension tables (BTS, contract, product).
3. Run `COMPUTE INCREMENTAL STATS` after each partition load.

## Fact Tables

| Table | Partition Field | Transform | Rationale |
|-------|-----------------|-----------|-----------|
| `cdr_sgi` | `etl_date` | identity (STRING YYYYMMDD) | Daily batch filter |
| `cdr_sgi_raw` | `event_time` | `days` | Streaming micro-batch |
| `cdr_mdt_smsng` | `base_date` | identity | MDT Wing/MSTR filters |
| `cdr_mdt_smsng_raw` | `event_time` | `days` | Flink streaming |
| `lte_call_fact` | `base_date` | identity | VoLTE MSTR |
| `call_quality_iceberg` | `base_date` | identity | Lookup + prune |

## Sort Order (Iceberg)

Recommended `WRITE ORDER BY` for join performance:

```sql
ALTER TABLE kdap.cdr_sgi WRITE ORDER BY cell_id;
ALTER TABLE kdap.cdr_mdt_smsng WRITE ORDER BY gnss_metric_id;
ALTER TABLE kdap.call_quality_iceberg WRITE ORDER BY lookup_key;
```

## Kudu (Scenario 4)

| Table | PK | Hash Partitions |
|-------|-----|-----------------|
| `call_quality_kudu` | `lookup_key` | 32 (tune to ~1-2GB/tablet) |
| `call_quality_kudu_by_call` | `(call_id, base_date)` | 16 |

## Post-Load Commands

```bash
./scripts/data_migration/compute_stats.sh
impala-shell -q "SHOW PARTITIONS kdap.cdr_sgi;"
impala-shell -q "SHOW TABLE STATS kdap.cdr_sgi;"
```

## Open Items

- [ ] Confirm single-day vs multi-day PoC slice
- [ ] Measure partition file sizes (target 512MB-1GB per file)
- [ ] Evaluate Z-order on `lookup_key` for Iceberg lookup path
