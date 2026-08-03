# KT KDAP PoC Firewall Port Matrix

Reference for KT network team (고태현선임). Adjust hostnames to actual FQDNs.

## Inbound to Cluster

| Port | Protocol | Service | Nodes |
|------|----------|---------|-------|
| 22 | TCP | SSH | All |
| 88 | TCP/UDP | Kerberos | IPA, All |
| 464 | TCP/UDP | Kerberos change/set password | IPA, All |
| 389 | TCP | LDAP | IPA |
| 636 | TCP | LDAPS | IPA |
| 53 | TCP/UDP | DNS | IPA |
| 7180 | TCP | Cloudera Manager UI | Base Master #1 |
| 7183 | TCP | Cloudera Manager API | Base Master #1 |
| 7182 | TCP | Cloudera Manager Agents | All (agent → server) |
| 8020 | TCP | HDFS NN RPC | Base Masters |
| 9870 | TCP | HDFS NN HTTP | Base Masters |
| 9864 | TCP | DataNode data transfer | Workers |
| 9866 | TCP | DataNode HTTP | Workers |
| 8032 | TCP | YARN RM | Base Masters |
| 8088 | TCP | YARN RM UI | Base Masters |
| 8050 | TCP | YARN NM | Workers |
| 21050 | TCP | Impala HS2 | Base Master #3, Workers |
| 25000 | TCP | Impalad | Workers |
| 25020 | TCP | Impala statestore | Base Master #2 |
| 7051 | TCP | Kudu Master | Base Masters |
| 7050 | TCP | Kudu Tablet Server | Workers |
| 8081 | TCP | Flink JobManager REST | Base Master #3 |
| 8082 | TCP | SSB Dashboard | Base Master #3 |
| 443 | TCP | CDW / ECS Console | ECS Master |
| 6443 | TCP | Kubernetes API | ECS Master |

## Inter-Cluster (All-to-All within PoC VLAN)

- ZooKeeper: 2181, 3181, 4181
- JournalNode: 8485
- Impala: 22000, 23000, 24000 (inter-Impala)
- Kudu internal: 7050-7054

## Data Migration (KT Source → HDFS)

| Port | Protocol | Service | Direction |
|------|----------|---------|-----------|
| 22 | TCP | SFTP/SSH distcp source | KT → Workers |
| 9864 | TCP | HDFS write | KT distcp client → DataNodes |
| 1004 | TCP | WebHDFS (if used) | KT → NN |

## Notes

- PoC data volume: 15-20 TB; ensure no proxy timeout < 24h for bulk transfer
- Kerberos requires UDP 88 bidirectional
- CDW Impala executors run inside ECS; no direct inbound to executor pods required
