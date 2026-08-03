# KT Data Intake Checklist

**KT Contact:** 고태헌 선임 (010-4295-4568)  
**Cloudera Contact:** 박소희 상무 / 김용재 이사  
**Target Volume:** 15-20 TB

## Required Deliverables from KT

### 1. Source Data Paths

| Dataset | Expected Table | Source Path | Format | Est. Rows | Est. Size |
|---------|----------------|-------------|--------|-----------|-----------|
| LTAS/SGi CDR | `CDR_SGI` | _TBD_ | Parquet/ORC | 130억+ | _TBD_ |
| SGi Signal #1 | `CDR_SGI` (signal) | _TBD_ | Parquet/ORC | 254억 | _TBD_ |
| SGi Signal #2 | `CDR_SGI` (signal) | _TBD_ | Parquet/ORC | 14억 | _TBD_ |
| MDT Samsung | `TRD_CDR_MDT_SMSNG_M1` | _TBD_ | Parquet/ORC | 180억 | _TBD_ |
| Real-time SGi | `CDR_DW.TRD_CDR_SGI` | _TBD_ | _TBD_ | streaming | _TBD_ |
| Real-time MDT | `MDT_DW.TRD_CDR_MDT_SMSNG_M1` | _TBD_ | _TBD_ | streaming | _TBD_ |

- [ ] Source HDFS/Hive paths documented
- [ ] Network path for distcp validated (bandwidth test)
- [ ] Kerberos credentials or trust for cross-cluster distcp

### 2. Dimension / Reference Tables

| Table | Purpose | Join Key |
|-------|---------|----------|
| BTS / 기지국 master | LTAS, SGi, MDT joins | `cell_id`, `bts_alt_key` |
| TBM_RPOT_M | MDT report points | geometry join |
| STG_WING.WING_ICE_DAY | MDT GNSS | `BASE_DATE` |
| LTE 통화 / 계약 / 상품 | MSTR VoLTE | contract/product keys |
| BAS.TBM_GPOT25, TBM_BDRI | MSTR RSRP geo | grid codes |

- [ ] Schema DDL for all dimension tables received
- [ ] Row counts and sample data (100 rows) per table

### 3. SQL Scripts (7 categories)

| # | File | Scenario | Status |
|---|------|----------|--------|
| 1 | `WF_N02960_TMP_SUM_LTAS_CDR_TM.sql` | S1 Batch LTAS | [ ] Received |
| 2 | `WF_M07046_TFM_S1AP_SGI_USE_DD.sql` (v1) | S1 Batch SGi #1 | [ ] Received |
| 3 | `WF_M07046_TFM_S1AP_SGI_USE_DD.sql` (v2) | S1 Batch SGi #2 | [ ] Received |
| 4 | `WF_N05923_CNV_MDT_MERGE.sql` | S1 Batch MDT | [ ] Received |
| 5 | `mstr_lte_rsrp_bad_rate.sql` | S3 MSTR | [ ] Received |
| 6 | `mstr_volte_drop_rate.sql` | S3 MSTR | [ ] Received |
| 7 | `mstr_query_03.sql` | S3 MSTR | [ ] Pending (KT update) |
| 8 | `mstr_query_04.sql` | S3 MSTR | [ ] Pending (KT update) |
| 9 | `lookup_call_quality.sql` | S4 HBase replacement | [ ] Received |

Place received SQL in `sql/batch/`, `sql/mstr/`, `sql/lookup/` respectively.

### 4. Key Columns for Partitioning

Confirm sample values for:

| Column | Tables | Sample Values | Notes |
|--------|--------|---------------|-------|
| `ETL_DATE` | CDR fact | `YYYYMMDD` | Batch filter |
| `BASE_DATE` | MDT, Wing | `YYYYMMDD` | Batch filter |
| `RQT_ST_DT` | Real-time CDR | timestamp string | Minute filter: `SUBSTR(...,11,2)='45'` |

- [ ] Date range of PoC dataset confirmed (single day vs multi-day)
- [ ] Cardinality of partition keys estimated
- [ ] Encryption columns identified (exclude from WHERE per agreement)

### 5. HBase Lookup Key Definition

- [ ] Primary lookup key column name (e.g., `call_id`, `cdr_key`)
- [ ] Expected QPS for concurrent lookup test (10/20/50)
- [ ] Current HBase table schema or equivalent CDR query

## Validation After Load

Run after data migration (Phase 1):

```bash
# Row count validation script
./scripts/data_migration/validate_row_counts.sh

# Schema diff
impala-shell -f sql/ddl/00_create_database.sql
impala-shell -q "SHOW TABLES IN kdap"
```

## Sign-off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| KT Data Owner | | | |
| Cloudera PoC Lead | 김익환 | | |
| Cloudera Architect | 박소희 | | |
