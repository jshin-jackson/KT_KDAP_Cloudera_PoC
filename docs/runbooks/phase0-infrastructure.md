# Phase 0: Infrastructure Setup Runbook

**Location:** Cheonan (천안)  
**Schedule:** 2026-07-29 ~ 2026-08-02  
**Owners:** 허종진, 김재현 (Cloudera) / 고태현 (KT)

## Prerequisites Checklist

- [ ] 23 nodes provisioned per spec (736 vCore, 11,776 GB RAM total)
- [ ] Network bonding configured on all nodes
- [ ] DNS entries for all cluster hosts (IPA-managed)
- [ ] NTP synchronized across cluster
- [ ] Root/sudo access for Cloudera install team
- [ ] KT firewall port matrix approved (see `config/firewall-ports.md`)

## Day 1 (07-29): OS Installation

| Node Group | Count | OS | Disk Layout |
|------------|-------|-----|-------------|
| Base Master | 3 | RHEL 8.x / Rocky 8.x | OS: 200GB, Data: per CM spec |
| Base Worker | 14 | RHEL 8.x / Rocky 8.x | OS: 200GB, HDFS/Kudu: 8TB |
| ECS Master | 1 | RHEL 8.x / Rocky 8.x | OS + K8s/etcd storage |
| ECS Worker | 4 | RHEL 8.x / Rocky 8.x | OS + Longhorn storage |
| IPA Server | 1 | RHEL 8.x / Rocky 8.x | OS + LDAP/Kerberos DB |

### OS Hardening Steps

```bash
# Run on all nodes
hostnamectl set-hostname <fqdn>
timedatectl set-timezone Asia/Seoul

# Disable SELinux (or configure per CM requirements)
setenforce 0
sed -i 's/SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config

# Disable transparent hugepages (required for Impala/Kudu)
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag

# Verify
ulimit -n   # expect >= 65535 after CM agent install
free -h     # expect 512GB on each node
nproc       # expect 32 vCore
```

- [ ] All 23 nodes OS installed and reachable via SSH
- [ ] `/etc/hosts` or DNS resolves all FQDNs
- [ ] THP disabled on all worker nodes

## Day 2 (07-30): CDP Base + ECS Installation

### Cloudera Manager Server (Base Master #1)

1. Install CM Server + embedded PostgreSQL (or external DB)
2. Prepare cluster host template with 18 Base nodes (3 master + 14 worker + 1 IPA if co-located)
3. Install parcels: CDP Runtime, Iceberg, Flink
4. Deploy services per architecture diagram:

**Base Master #1:** NN (Active), ZKFC, JournalNode, ZK, YARN RM (Active), Kudu Master, CM Server, Ranger Admin  
**Base Master #2:** NN (Standby), HMS, Impala Catalog, Impala StateStore, YARN RM (Standby), Kudu Master  
**Base Master #3:** JournalNode, ZK, YARN JobHistory, Spark History, HiveServer2, Oozie, Flink Dashboard, SSB Dashboard

**Base Workers #1-14:** DataNode, NodeManager, Impalad, Kudu Tablet Server, CM Agent

5. Enable Kerberos via FreeIPA integration
6. Configure HDFS replication factor = 3
7. Upload ECS Docker images to ECS Master

### ECS / Data Services

1. Install ECS on 1 Master + 4 Workers
2. Verify Kubernetes control plane healthy
3. Deploy CDP Control Plane on ECS Master
4. Pre-stage CDW Impala VW template (12 executor pods)

### Validation

```bash
# CM API health
curl -k -u admin:password "https://<cm-host>:7183/api/v40/clusters"

# HDFS
hdfs dfs -mkdir -p /user/kdap
hdfs dfs -chmod 777 /user/kdap

# Kerberos
kinit kdap_svc@REALM
klist
```

- [ ] CM shows all Base services GREEN
- [ ] HDFS HA failover test passed
- [ ] Kerberos ticket obtained for service account
- [ ] ECS cluster nodes Ready
- [ ] Docker images uploaded

## Day 3 (07-31): Firewall + Prerequisites

**KT Owner:** 고태현선임

Required ports (minimum):

| Source | Destination | Port | Service |
|--------|-------------|------|---------|
| Cloudera team | All nodes | 22 | SSH |
| All cluster | All cluster | 88, 464 | Kerberos |
| All cluster | All cluster | 389, 636 | LDAP |
| Clients | CM Server | 7180, 7183 | Cloudera Manager |
| Clients | Impala | 21050, 25000 | Impala HS2/thrift |
| Clients | ECS | 443 | CDW Console |
| Workers | Workers | 9864, 9866 | Iceberg/Impala IO |

- [ ] Firewall rules applied and verified
- [ ] Cloudera team VPN/access to 판교 from Cheonan staging complete
- [ ] Data migration path confirmed with KT (고태헌 010-4295-4568)

## Day 4-5 (08-01 ~ 08-02): Data Migration Prep

- [ ] Receive LTAS/SGi/MDT source paths and schemas from KT
- [ ] Validate sample data row counts
- [ ] Prepare distcp / Iceberg migration scripts (`scripts/data_migration/`)
- [ ] Stage data transfer bandwidth test (target: 15-20 TB within 2 days)

## Exit Criteria

All items checked before Phase 1 start (08-03, Pangyo):

1. 23 nodes healthy, CM green
2. Kerberos authentication working
3. ECS + CDW control plane ready
4. Firewall open, network latency < 1ms intra-rack
5. Data migration info documented in `docs/runbooks/data-intake-checklist.md`

## Rollback / Escalation

| Issue | Contact | Action |
|-------|---------|--------|
| CM install failure | 허종진 | Check CM logs `/var/log/cloudera-scm-server/` |
| Kerberos auth failure | IPA admin | Verify realm, keytab, DNS |
| ECS pod scheduling | 허종진 | Check Longhorn storage, node labels |
| Firewall blocked | 고태현 (KT) | Port matrix escalation |
