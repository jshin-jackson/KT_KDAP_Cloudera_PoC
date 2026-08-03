# Phase 1: Environment Setup & Data Load (판교)

**Schedule:** 2026-08-03 ~ 2026-08-06  
**Owners:** 김익환, 김재현 (data) / 허종진 (DS/Flink)

## Day 1 (08-03): Access & Data Services

- [ ] Kerberos `kinit` for `kdap_svc` on all edge nodes
- [ ] CM: all services GREEN after 판교 cutover
- [ ] Install/configure CDW on ECS (see `config/cdw/virtual-warehouse.yaml`)
- [ ] Deploy Flink + SSB on Base Master #3
- [ ] Create VW `kdap-mstr-vw` with 12 executor pods
- [ ] Apply Ranger policies (`config/ranger/kdap-policies.yaml`)

```bash
impala-shell -k -f sql/ddl/00_create_database.sql
impala-shell -k -f sql/ddl/01_iceberg_fact_tables.sql
impala-shell -k -f sql/ddl/02_iceberg_dim_tables.sql
impala-shell -k -f sql/ddl/03_kudu_lookup_tables.sql
```

## Day 2-3 (08-04 ~ 08-05): Data Migration (15-20 TB)

```bash
export SOURCE_NN=hdfs://kt-source:8020
export ETL_DATE=20251201
export BASE_DATE=20251201
./scripts/data_migration/distcp_facts.sh
./scripts/data_migration/validate_row_counts.sh
./scripts/data_migration/compute_stats.sh
```

- [ ] Row counts match KT source inventory
- [ ] Iceberg metadata refreshed
- [ ] Kudu lookup table loaded (Scenario 4)

## Day 4 (08-06): Smoke Tests

| Component | Test | Command |
|-----------|------|---------|
| Impala | Simple select | `impala-shell -q "SELECT COUNT(*) FROM kdap.bts_master"` |
| Iceberg | Partition prune | `EXPLAIN SELECT ... WHERE etl_date='...'` |
| Kudu | Point lookup | `sql/lookup/lookup_call_quality_kudu.sql` |
| Flink | Job submit | SSB deploy `flink/ltas_5min.sql` |
| CDW | MSTR pre-warm | `impala-shell -i cdw-vw ... -q "SELECT 1"` |

- [ ] Apply YARN fair-scheduler (`config/resource_pools/fair-scheduler.xml`)
- [ ] Configure Impala admission pools (`config/resource_pools/impala-admission-pools.yaml`)
- [ ] Sign off Phase 1 → begin Scenario benchmarks 08-07

## Exit Criteria

Ready for `./scripts/benchmark/run_scenario1_batch.sh single` on 08-07.
