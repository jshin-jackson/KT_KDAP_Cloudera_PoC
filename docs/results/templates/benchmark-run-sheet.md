# PoC Benchmark Run Sheet Template

**Run ID:** `run_YYYYMMDD_HHMMSS`  
**Date:**  
**Operator:**  
**Cluster:** KT KDAP PoC (판교)

## Environment

| Parameter | Value |
|-----------|-------|
| ETL_DATE | |
| BASE_DATE | |
| CDP Version | |
| Impala Version | |
| Flink Version | |

## Scenario 1: Batch (Impala + Iceberg)

| Query | Mode | Duration (sec) | Rows Out | Pool | Profile File |
|-------|------|----------------|----------|------|--------------|
| WF_N02960 LTAS | single / concurrent | | | pool_ltas_batch | |
| WF_M07046 SGi v1 | single / concurrent | | | pool_sgi_batch_1 | |
| WF_M07046 SGi v2 | single / concurrent | | | pool_sgi_batch_2 | |
| WF_N05923 MDT | single / concurrent | | | pool_mdt_batch | |

**Profile analysis notes:**
- Partition pruning: Y/N
- Join strategy:
- Spill to disk: Y/N

## Scenario 2: Flink Real-time

| Job | Window | E2E Latency | Checkpoint OK | Batch Parity Diff |
|-----|--------|-------------|---------------|-------------------|
| ltas_5min | 5 min | | | |
| sgi_5min_v1 | 5 min | | | |
| sgi_5min_v2 | 5 min | | | |
| mdt_5min | 5 min | | | |

## Scenario 3: MSTR

| Query | Engine | Mode | Duration (sec) | Presto Baseline | Profile |
|-------|--------|------|----------------|-----------------|---------|
| RSRP 불량률 | Base / CDW | single / concurrent | | ~30 min | |
| VoLTE 절단율 | Base / CDW | single / concurrent | | ~60 min | |
| Query 03 | Base / CDW | | | | |
| Query 04 | Base / CDW | | | | |

## Scenario 4: HBase Replacement

| Engine | Avg (sec) | p95 (sec) | p99 (sec) | Target ≤3s |
|--------|-----------|-----------|-----------|------------|
| Kudu | | | | |
| Iceberg | | | | |

**Recommendation:** Kudu / Iceberg

## Scenario 5: Integrated

| Workload | Running | Degraded SLA | Pool Contention |
|----------|---------|--------------|-----------------|
| 4 batch | | | |
| Flink x3 | | | |
| MSTR x4 CDW | | | |

## Sign-off

| Role | Name | Date |
|------|------|------|
| PoC Lead | | |
| Tuning | 박소희 | |
| KT Review | | |
