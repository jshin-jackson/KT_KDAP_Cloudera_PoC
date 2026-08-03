# KT KDAP PoC 결과 보고서 (Template)

**Document Version:** 1.0  
**PoC Period:** 2026-08-03 ~ 2026-08-14  
**Author:** Cloudera PoC Team

---

## 1. Executive Summary

| Scenario | Objective | Result | Recommendation |
|----------|-----------|--------|----------------|
| S1 Batch | 4 concurrent Impala batch | | |
| S2 Flink | 5-min streaming aggregation | | |
| S3 MSTR | CDW Impala vs Presto | | |
| S4 Lookup | Kudu vs Iceberg ≤3s | | |
| S5 Integrated | Mixed workload isolation | | |

## 2. Environment

- Cluster: 23 nodes (see `docs/architecture/node-service-matrix.md`)
- Data volume loaded: ___ TB
- CDP / Runtime version: ___

## 3. Scenario Results

### 3.1 Scenario 1 — Large-scale Batch

Insert tables from `docs/results/run_*/summary.csv`.

| Query | Single (sec) | Concurrent (sec) | Hive Baseline | Improvement |
|-------|--------------|------------------|---------------|-------------|

Key Query Profile findings:
- 

### 3.2 Scenario 2 — Flink Real-time

Validation: `scripts/benchmark/validate_scenario2_flink.sh` output.

### 3.3 Scenario 3 — MSTR / CDW

| Matrix | Base Single | Base Concurrent | CDW Single | CDW Concurrent | Base+CDW Mixed |
|--------|-------------|-----------------|------------|----------------|----------------|

### 3.4 Scenario 4 — HBase Replacement

Winner: ___ (Kudu / Iceberg)  
p95 lookup latency: ___ sec

### 3.5 Scenario 5 — Integrated Operations

Resource pool effectiveness:
- 

## 4. Tuning Applied

| Area | Change | Impact |
|------|--------|--------|
| Iceberg partition | | |
| Impala mem_limit | | |
| Join hint / broadcast | | |
| Kudu bucket count | | |

## 5. Risks & Production Recommendations

1. 
2. 
3. 

## 6. Appendix

- Raw profiles: `docs/results/run_*/profiles/`
- WBS: Customer KT_PoC Documents
- Architecture PDF: 20260727-KT KDAP-PoC Architecture.pdf
