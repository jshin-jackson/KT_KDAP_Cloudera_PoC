# KT KDAP PoC Node & Service Matrix

Source: [20260727-KT KDAP-PoC Architecture.pdf](file:///Users/jackson/Cloudera/Customer/KT_PoC/Documents/20260727-KT%20KDAP-PoC%20Architecture.pdf)

## Cluster Summary

| Metric | Value |
|--------|-------|
| Total nodes | 23 |
| Total vCore | 736 |
| Total memory | 11,776 GB |
| Storage (HDFS/Kudu per worker) | 8 TB × 14 workers |
| Expected PoC data | 15-20 TB |

## Node Inventory

| Host Role | Qty | vCore | RAM | Hostname Pattern |
|-----------|-----|-------|-----|------------------|
| Base Master | 3 | 32 | 512 GB | `kdap-base-m{1,2,3}.example.com` |
| Base Worker | 14 | 32 | 512 GB | `kdap-base-w{01..14}.example.com` |
| ECS Master | 1 | 32 | 512 GB | `kdap-ecs-m1.example.com` |
| ECS Worker | 4 | 32 | 512 GB | `kdap-ecs-w{1..4}.example.com` |
| IPA Server | 1 | 32 | 512 GB | `kdap-ipa1.example.com` |

## Service Placement

### Base Master #1
- NameNode (Active), FailoverController, JournalNode, ZooKeeper
- YARN ResourceManager (Active)
- Kudu Master
- Cloudera Manager Server, CM DB (PostgreSQL)
- Ranger Admin

### Base Master #2
- NameNode (Standby), FailoverController, JournalNode, ZooKeeper
- YARN ResourceManager (Standby)
- Kudu Master
- Hive Metastore
- Impala Catalog Server, Impala StateStore

### Base Master #3
- JournalNode, ZooKeeper
- YARN JobHistory Server
- Spark History Server
- HiveServer2, Hive, Oozie
- Flink Dashboard, SSB Dashboard, Flink Gateway

### Base Worker #1-14
- HDFS DataNode, YARN NodeManager
- Impalad (coordinator + executor)
- Kudu Tablet Server
- CM Agent
- 8 TB data disk (HDFS + Kudu WAL/data)

### ECS Master #1
- Kubernetes Control Plane, etcd
- Longhorn
- CDP Control Plane
- CM Agent

### ECS Worker #1-4
- Kubernetes Worker Node
- Longhorn Storage Agent
- Impala Executor Pod (3 pods/node = 12 total)
- CDW Pod

### IPA #1
- FreeIPA (LDAP, Kerberos, DNS)

## CDW Virtual Warehouse Sizing

| Parameter | Recommended Value |
|-----------|-------------------|
| Executor pods | 12 (3 × 4 ECS workers) |
| Memory per executor | 64-128 GB (tune based on MSTR query mem) |
| Concurrency target | 4 simultaneous MSTR queries |
| Catalog | Shared Hive Metastore (Iceberg tables) |

## Storage Layout

```
/user/kdap/
├── iceberg/          # Iceberg warehouse (HDFS)
│   ├── cdr_sgi/
│   ├── cdr_mdt/
│   ├── bts_master/
│   └── mstr_mart/
├── kudu/             # Kudu table data (managed by Kudu)
└── staging/          # distcp landing zone
```

## Workload Mapping

| Scenario | Compute | Storage |
|----------|---------|---------|
| S1 Batch | Base Impala (14 impalads) | Iceberg on HDFS |
| S2 Flink | Flink on YARN (Gateway on Master #3) | Iceberg streaming sink |
| S3 MSTR | CDW Impala (12 pods) | Iceberg on HDFS (shared) |
| S4 Lookup | Base Impala | Kudu + Iceberg A/B |
| S5 Integrated | Base + CDW concurrent | All |
