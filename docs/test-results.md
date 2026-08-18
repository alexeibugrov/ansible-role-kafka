# Test results

Every result below was produced by executing the role against a real three-node
cluster. Nothing here is projected from reading the code.

**Environment.** Three `t3.small` EC2 instances (2 vCPU, 1907 MiB usable RAM, 30 GiB
gp3, encrypted), Ubuntu 24.04.4 LTS, `eu-central-1a`, one VPC / one subnet. Apache
Kafka 4.3.1 on OpenJDK 21.0.11, KRaft, all three nodes `broker,controller`.
Cluster ID `gJWSNi_40i6HwOaWMnMEGg`. The environment is ephemeral and is torn down
with `make clean`.

| Node | Private IP | node.id |
|---|---|---|
| kafka-1 | 10.42.1.44 | 1 |
| kafka-2 | 10.42.1.154 | 2 |
| kafka-3 | 10.42.1.85 | 3 |

## Deployment

Final `ansible-playbook playbooks/kafka.yml` on a converged cluster:

```text
kafka-1 : ok=48  changed=0  unreachable=0  failed=0  skipped=23
kafka-2 : ok=38  changed=0  unreachable=0  failed=0  skipped=17
kafka-3 : ok=38  changed=0  unreachable=0  failed=0  skipped=17
```

A first run against empty hosts reports `changed=30` on kafka-1 and `changed=28` on
the other two.

## Idempotency

Second run with identical configuration: **`changed=0`, `failed=0` on all three
nodes**, and zero `changed:` lines in the output.

Checked specifically:

| Property | Result |
|---|---|
| Kafka not re-downloaded / re-extracted | `get_url` checksum match, `unarchive` `creates:` guard |
| `current` symlink unchanged | no change |
| Cluster ID not regenerated | supplied from `.local/cluster-identity.yml`, never minted in-role |
| Controller bootstrap metadata unchanged | `--initial-controllers` derived from the same file |
| **KRaft storage not reformatted** | `meta.properties` present, format task skipped |
| TLS private keys not regenerated | `openssl_privatekey` `regenerate: full_idempotence` |
| **TLS certificates not regenerated** | serials identical, see below |
| SCRAM credentials not rewritten | existence check matched all three principals |
| `server.properties` unchanged | no change |
| systemd unit unchanged | no change, no `daemon-reload` |
| **Kafka not restarted** | unit start timestamps predate the run |
| JMX Exporter + config unchanged | checksum match |
| Topic not recreated or altered | no change |

### Certificate idempotency

Serial numbers captured before and after an idempotent re-run:

| Node | Serial (before == after) |
|---|---|
| kafka-1 | `45D86C46577B692DCD3FECE2E370A1D480CAD033` |
| kafka-2 | `6E698C2DD9BF86BEB3513A72D3C390B1015A943A` |
| kafka-3 | `60BB46B0DC5AE3482A7AF3EC5A6E28264777C165` |

SHA-256 fingerprints were likewise identical. No private key material is reproduced
here. This works because `community.crypto.x509_certificate` defaults
`ignore_timestamps: true`, so a relative `ownca_not_after` does not make an existing
certificate look different on each run.

### Service continuity

`systemctl show -p ActiveEnterTimestamp` after the idempotent run — all three
timestamps predate it, so no broker was restarted:

```text
kafka-1  Tue 2026-08-18 06:50:19 UTC
kafka-2  Tue 2026-08-18 06:50:45 UTC
kafka-3  Tue 2026-08-18 06:51:14 UTC
```

These are the timestamps of the initial rolling restart. They were unchanged by three
subsequent converge runs, so no broker was restarted by any of them.

## Verification (`playbooks/verify.yml`)

Exit code 0, `failed=0` on all three nodes.

| Check | Result |
|---|---|
| Service active and enabled | pass, 3/3 |
| BROKER / CONTROLLER / metrics bound to the private address only | pass, 3/3 |
| Insecure JMX remoting disabled, no RMI port | pass, 3/3 |
| Certificate SANs, expiry, issuer | pass — e.g. kafka-1: `DNS:kafka-1, IP:10.42.1.44, DNS:localhost, IP:127.0.0.1, DNS:ip-10-42-1-44.eu-central-1.compute.internal`, valid to 2028-11-20 |
| TLS chain verifies against the internal CA | pass, 3/3 (`openssl s_client -verify_return_error` → `Verification: OK`) |
| Client not trusting the CA is rejected | pass, 3/3 |
| Valid SCRAM credentials accepted | pass, 3/3 |
| Invalid SCRAM credentials rejected | pass, 3/3 (`SaslAuthenticationException`) |
| No PLAINTEXT listener answers | pass, 3/3 |
| journald carries Kafka logs | pass, 3/3 (200 recent lines) |
| `/metrics` exposes Kafka + KRaft + JVM families | pass, 3/3 (~458 KB per node) |
| KRaft quorum healthy, leader present, 3 voters | pass |
| All three brokers registered | pass |
| Topic shape and replica spread | pass |
| Produce/consume 3 JSON events over SASL_SSL | pass |

Metric families confirmed present (not merely HTTP 200):
`kafka_server_brokertopicmetrics_bytesinpersec_total`,
`kafka_server_replicamanager_underreplicatedpartitions`,
`kafka_controller_activecontrollercount`, `kafka_raft_current_epoch`,
`kafka_broker_metadata_last_applied_record_lag_ms`,
`jvm_memory_heapmemoryusage_used_bytes`, `jvm_gc_collection_seconds_count`.

## Topic management

Starting state: `analytics.events`, 6 partitions, RF 3, `min.insync.replicas=2`,
`retention.ms=604800000`.

| Step | Expected | Observed |
|---|---|---|
| 1. Request 7 partitions | increase | `changed=1`, `PartitionCount: 7` |
| 2. Re-run at 7 | no change | `changed=0` on all nodes |
| 3. Request 6 (a decrease) | fail safely, topic untouched | playbook exited 2 with the message below; topic **still 7 partitions** |
| 4. Explicit delete, then re-run | recreated at declared shape | `PartitionCount: 6, ReplicationFactor: 3, min.insync.replicas=2, retention.ms=604800000` |

The decrease failure message:

> Topic 'analytics.events' has 7 partitions but 6 were requested. Kafka cannot
> decrease a partition count. Achieving it would mean deleting and recreating the
> topic, which destroys its data and re-hashes every keyed message onto a different
> partition, breaking per-key ordering for every consumer. Either restore partitions
> to 7 or more in kafka_topics, or migrate to a new topic deliberately.

Step 4 required an explicit `kafka-topics.sh --delete` by hand. The role never deletes
a topic, including when one is removed from `kafka_topics`.

## Public exposure

From the Ansible control machine against each public IP. SSH is included as a
positive control, so a dead probe path cannot be mistaken for a blocked port:

| Port | kafka-1 | kafka-2 | kafka-3 | Expected |
|---|---|---|---|---|
| 22 (control) | OPEN | OPEN | OPEN | SSH ingress, key-only auth |
| 9092 | closed/filtered | closed/filtered | closed/filtered | not publicly reachable |
| 9093 | closed/filtered | closed/filtered | closed/filtered | not publicly reachable |
| 9404 | closed/filtered | closed/filtered | closed/filtered | not publicly reachable |

Kafka's ports are admitted only by security-group self-reference, so they are
reachable between cluster members and from nowhere else. SSH is reachable from
any address (`ssh_ingress_cidr` defaults to `0.0.0.0/0`) because the machine running
Ansible has a rotating public address; authentication is key-only ED25519.

## Terraform

`terraform fmt -check -recursive` and `terraform validate` both pass. The plan creates
15 resources, exactly 3 of them `aws_instance`. SSH is the only ingress rule
carrying `0.0.0.0/0`; every Kafka port is admitted solely by security-group
self-reference. `terraform.tfstate` contains no `BEGIN … PRIVATE KEY` block — only the
public key and its fingerprint, because the keypair is generated by `ssh-keygen`
outside Terraform.

## End-to-end harness

`scripts/e2e.sh` was run to completion against this environment: **exit code 0**,
all eleven phases green — dependency check, local material, Terraform apply,
inventory render, Galaxy collection install, SSH wait, deploy, verify, idempotency,
and the public-port probe.

Interactive produce/consume was confirmed separately from the verify playbook's own
round-trip. Five JSON events were produced on **kafka-1** with `acks=all` and read
back through **kafka-3's** bootstrap — a different broker than accepted the writes,
so the path exercises replication rather than a local read. All five returned intact,
with the quorum reporting `LeaderId: 1` and all six partitions at full `Isr: 1,2,3`.

One flakiness worth recording: on an earlier invocation the idempotency stage failed
with `Connection refused` on SSH across all three hosts, seconds before they answered
normally again. A refusal rather than a timeout points at sshd's `MaxStartups`
throttle, triggered by three playbooks running back-to-back at `forks=10` — not the
security group, and not the role. Re-running the stage passed cleanly. The harness
would benefit from backoff around its SSH bursts.

## Defects found and fixed during testing

These are the reason for running the thing rather than reasoning about it.

**1. Insecure JMX remoting enabled by default, with an RMI port on all interfaces.**
`bin/kafka-run-class.sh` substitutes its own `KAFKA_JMX_OPTS` when the variable is
unset:

```text
-Dcom.sun.management.jmxremote=true
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false
```

`ss -lntp` showed an ephemeral port bound to `*` that answered a JRMI `ProtocolAck`
(`0x4e`) — an RMI listener with authentication and TLS both off. Setting
`KAFKA_JMX_OPTS=-Dcom.sun.management.jmxremote=false` **did not fix it**: the JVM
enables JMX when the property is merely *present*, whatever its value. The fix is to
set `KAFKA_JMX_OPTS` to an inert property that names no `jmxremote` setting at all,
keeping it non-empty so Kafka's default is never substituted. Confirmed by
re-checking: only 9092, 9093 and 9404 remain bound. `verify.yml` now asserts this
against the running process's command line rather than trusting the unit file.

**2. `kafka-configs.sh` stored quote characters as part of the password.** The role
passed `--add-config 'SCRAM-SHA-512=[…,password="secret"]'`. `kafka-storage.sh
--add-scram` *does* expect quoted values, but `kafka-configs.sh` does not strip them,
so the stored password gained a leading and trailing `"`. Applied to `kafka_admin`,
this locked the admin principal out of its own cluster on the next task. Recovered by
authenticating as `kafka_broker` — whose credential was written by `kafka-storage.sh`
at format time and never touched — and resetting `kafka_admin`. The two CLIs genuinely
differ; the role now documents this at the call site.

**3. The SCRAM existence check never matched.** `kafka-configs.sh --describe` prints
`SCRAM credential configs for user-principal 'kafka_admin' are …`, not `name=…`. The
original condition looked for `name=<principal>`, so it never matched and rewrote
every credential on every run — a permanent `changed=1` and, for the admin principal,
the lockout above. Now matched against the real output format.

**4. Cross-host variable resolution.** `kafka_advertised_address` is a role *default*,
so it is absent from other hosts' `hostvars`. Deriving the quorum with
`map(attribute='kafka_advertised_address')` failed with `'dict object' has no
attribute`. The derivation now falls back through variables that genuinely exist
per-host: explicit override, then the inventory's `kafka_private_ip`, then the host's
`ansible_default_ipv4.address`.

**5. Kafka's stock log4j2 hides controller logs from journald.** The shipped
`config/log4j2.yaml` attaches the controller, authorizer, request and log-cleaner
loggers to their own rolling files with `additivity: false`, so those records never
reach stdout. `journalctl -u kafka` would have been missing exactly the lines a KRaft
or auth incident needs. The role ships a console-only configuration instead.

**6. JMX Exporter is not on Maven Central past 1.0.1.** Maven Central's
`maven-metadata.xml` still advertises 1.0.1 as the current release; 1.1.0 onward is
published as a GitHub release asset with a `.sha256` sidecar. A Maven URL for 1.6.0
returns 404.

**7. A catch-all `- pattern: ".*"` is not a drop rule.** It is the exporter's
collect-everything idiom. The rule list now simply ends, since an unmatched MBean is
already not exported.

**8. `current-state` is a string MBean** and cannot be scraped as a gauge; it is
converted to a constant-`1` series with the state as a label.

**9. GC metrics from a `java.lang<type=GarbageCollector>` rule emit nothing** — the
agent's built-in JVM collectors already publish `jvm_gc_collection_seconds_*` and
shadow it. The redundant rules were removed.

## Not tested

Stated plainly rather than implied:

- **Failure/recovery behaviour.** No broker was killed to observe quorum re-election,
  under-replicated partition recovery, or producer behaviour at `min.insync.replicas`.
- **Performance.** No throughput or latency measurement was taken; `t3.small` with a
  512 MiB heap would not produce a meaningful number anyway.
- **Upgrades.** The versioned-install and symlink layout is designed for it, but no
  version-to-version upgrade was executed.
- **Certificate rotation / expiry renewal.**
- **ClickHouse.** The `clickhouse` principal exists and is authenticated, but no
  ClickHouse instance consumed the topic — deploying one was out of scope.
- **ACL authorization.** Authentication is enforced; authorization is not configured,
  so any authenticated principal has full access. See the README's production section.
