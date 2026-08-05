# KT KDAP PoC

Cloudera Data Platform PoC for KT KDAP (Korea Telecom Data Analytics Platform).

Validates migration from Hive/Presto/HBase to **Impala + Iceberg + Kudu + Flink + CDW** for telecom analytics workloads (LTAS, SGi, MDT).

## Scenarios

| # | Area | Engine | KPI |
|---|------|--------|-----|
| 1 | Large-scale batch | Base Impala + Iceberg | 4 concurrent batch queries |
| 2 | Real-time aggregation | Flink 5-min TUMBLE | Streaming vs batch parity |
| 3 | MSTR BI queries | CDW Impala | Presto comparison, 4 queries |
| 4 | HBase replacement | Kudu vs Iceberg | Key lookup ≤ 2-3 sec |
| 5 | Integrated ops | Base + CDW | Resource pool isolation |

## Repository Layout

```
docs/           Runbooks, architecture, result templates
sql/            Batch, MSTR, lookup SQL + DDL
flink/          Flink SQL streaming jobs (5-min window)
scripts/        Benchmark, profile, migration, monitoring
config/         Resource pools, CDW VW, Ranger, firewall
```

## Environment (jshin CDP 7.3.2)

```
cp .env.example .env
export HADOOP_CONF_DIR=/etc/hadoop/conf
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

→ [docs/runbooks/env-setup.md](docs/runbooks/env-setup.md) · [config/cluster/jshin-cdp732.md](config/cluster/jshin-cdp732.md)

## Flink (시나리오 2) — jshin CDP 7.3.2 / HDFS ns1

**명령어 목록:** [flink/cdp-test/COMMANDS.md](flink/cdp-test/COMMANDS.md)

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
kinit -kt /cdep/keytabs/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
impala-shell -i ccycloud-5.jshin.root.comops.site:25003 --protocol=beeswax -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -f flink/cdp-test/impala_01_create_tables.sql
./flink/run_ltas_5min.sh
```

## Quick Start

### 1. Environment Setup

Follow [docs/runbooks/phase0-infrastructure.md](docs/runbooks/phase0-infrastructure.md) for cluster install.

### 2. Data Intake

Complete [docs/runbooks/data-intake-checklist.md](docs/runbooks/data-intake-checklist.md) with KT.

### 3. Create Database & Tables

```bash
export IMPALA_SHELL="impala-shell -k --quiet"
$IMPALA_SHELL -f sql/ddl/00_create_database.sql
$IMPALA_SHELL -f sql/ddl/01_iceberg_fact_tables.sql
$IMPALA_SHELL -f sql/ddl/02_iceberg_dim_tables.sql
$IMPALA_SHELL -f sql/ddl/03_kudu_lookup_tables.sql
./scripts/data_migration/compute_stats.sh
```

### 4. Run Benchmarks

```bash
# Scenario 1: single then concurrent batch
./scripts/benchmark/run_scenario1_batch.sh single
./scripts/benchmark/run_scenario1_batch.sh concurrent

# Scenario 3: MSTR on Base and CDW
./scripts/benchmark/run_scenario3_mstr.sh --target base
./scripts/benchmark/run_scenario3_mstr.sh --target cdw

# Scenario 4: Kudu vs Iceberg lookup
./scripts/benchmark/run_scenario4_lookup.sh

# Scenario 5: integrated workload
./scripts/benchmark/run_scenario5_integrated.sh
```

### 5. Collect Results

Results written to `docs/results/run_<timestamp>/`. Use templates in `docs/results/templates/`.

## Configuration

- Resource pools: `config/resource_pools/`
- CDW VW spec: `config/cdw/virtual-warehouse.yaml`
- Firewall: `config/firewall-ports.md`

## Key Contacts

| Role | Name |
|------|------|
| Infra | 허종진, 김재현 |
| PoC execution | 김익환, 김재현 |
| Tuning | 박소희, 이지환 |
| Flink PS | 신정훈 |
| KT network | 고태현 |
| KT data | 고태헌 (010-4295-4568) |

## References

- Architecture: `~/Cloudera/Customer/KT_PoC/Documents/20260727-KT KDAP-PoC Architecture.pdf`
- WBS: `~/Cloudera/Customer/KT_PoC/Documents/WBS_R^0R_KTPOC1.0.xlsx`
