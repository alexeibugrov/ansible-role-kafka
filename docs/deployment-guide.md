# Deployment and testing guide

How to stand this cluster up, confirm it actually works, and take it down again.

`README.md` explains *why* the role is shaped the way it is; `docs/architecture.md`
covers the internals. This page is the operational path: the commands, in order, with
what each one should print.

Two routes are supported and they share every step except the machines:

- **Route A — ephemeral AWS test environment.** Terraform builds three EC2 instances
  and writes the inventory for you. This is how the role was validated.
- **Route B — your own hosts.** Any three Linux boxes, any provider or bare metal.
  You write a five-line inventory and skip Terraform entirely.

---

## 1. Prerequisites

**Control machine** (where Ansible runs):

| Tool | Needed for |
|---|---|
| `ansible-core` (≥ 2.16) | everything |
| `python3` | inventory rendering |
| `ssh-keygen`, `curl` | key generation, downloads |
| `terraform` (≥ 1.6) | Route A only |
| AWS credentials | Route A only |

**Target hosts**: Ubuntu 24.04 (or any systemd distro with an `apt`/`dnf` Java 21
package), reachable over SSH with `become` rights, and able to reach the internet for
the Apache Kafka tarball and the JMX Exporter jar.

**Network between the nodes** must allow `9092` (clients + inter-broker), `9093`
(KRaft controller quorum) and `9404` (metrics) *between cluster members*. None of the
three should be reachable from the public internet.

Install the collection dependencies once:

```bash
make deps
```

---

## 2. Generate the local material

Three things must be minted **once** and then never change. All land in `.local/`,
which is gitignored.

```bash
make ssh-key    # Route A only — ED25519 keypair, never enters Terraform state
make secrets    # SCRAM passwords for kafka_admin, kafka_broker, clickhouse
make identity   # cluster ID + one controller directory UUID per node
```

`make identity` is the one that matters most. The cluster ID and the per-node
directory UUIDs are written into each node's KRaft metadata at format time and are
**immutable afterwards**. They are generated outside the role deliberately: generated
inside, each host would invent a different value and the quorum would never form.

Re-running any of these three reuses what already exists. That is intentional — SCRAM
passwords must stay stable across runs, and regenerating a cluster ID would orphan
every node's storage.

> In production these values come from your secret manager, not from `.local/`. Point
> the playbook's `vars_files` at whatever supplies them.

---

## 3a. Route A — build the AWS test environment

```bash
make tf-init
make tf-apply
make inventory
```

`make tf-apply` creates 15 resources: a VPC, a subnet, an internet gateway, a route
table and association, a security group with four ingress rules and one egress rule, a
key pair, and three `t3.small` instances.

`make inventory` turns the Terraform outputs into `inventory/generated.yml`. Note what
it does with addresses: Ansible connects over the **public** IP, while Kafka is told to
bind and advertise the **private** one, so no public address ever reaches
`advertised.listeners`.

Wait for SSH to answer before deploying — instances accept connections slightly after
Terraform returns:

```bash
ansible -i inventory/generated.yml kafka -m ansible.builtin.ping
```

## 3b. Route B — use your own hosts

Skip Terraform. Write `inventory/generated.yml` (or pass `-i` to point elsewhere)
modelled on `examples/inventory.yml`:

```yaml
all:
  children:
    kafka:
      hosts:
        kafka-1: {ansible_host: 203.0.113.11, kafka_node_id: 1, kafka_private_ip: 10.0.10.11}
        kafka-2: {ansible_host: 203.0.113.12, kafka_node_id: 2, kafka_private_ip: 10.0.10.12}
        kafka-3: {ansible_host: 203.0.113.13, kafka_node_id: 3, kafka_private_ip: 10.0.10.13}
      vars:
        ansible_user: ubuntu
        ansible_ssh_private_key_file: ~/.ssh/id_ed25519
```

The role needs exactly three facts per host — `ansible_host`, `kafka_node_id`,
`kafka_private_ip`. Everything else has a default. `kafka_node_id` must be unique and
permanent; it is the KRaft `node.id`.

Tunables live in `roles/kafka/defaults/main.yml`; override them per group in
`group_vars/`, using `examples/group_vars/kafka.yml` as the reference.

---

## 4. Deploy

```bash
make deploy
```

This runs two plays, and the split is deliberate:

1. **Converge, all hosts in parallel.** Bootstrap *must* be parallel — one node of
   three cannot elect a quorum leader, so a `serial: 1` first run would block forever
   waiting for a broker that cannot start.
2. **Apply restarts, `serial: 1`.** Updates *must* be serialised — with
   `min.insync.replicas=2`, one broker down still accepts `acks=all` writes and two
   down stops them.

The restart handler does not restart. It records that a restart is owed by touching a
marker file, and the second play pays those back one node at a time, clearing each
marker only after that node proves healthy.

Expect `failed=0` and `unreachable=0` on all three. First run reports roughly
`changed=30` on the first node and `changed=28` on the others.

---

## 5. Confirm the cluster is live

```bash
make verify
```

Exit code 0 means all of the following passed, on every node, as assertions rather
than eyeballed output:

- service active and enabled
- `BROKER` / `CONTROLLER` / metrics bound to the private address only
- insecure JMX remoting disabled, no RMI port listening
- certificate SANs, expiry and issuer correct; chain verifies against the internal CA
- a client that does not trust the CA is **rejected**
- valid SCRAM credentials accepted, invalid ones **rejected**
- no PLAINTEXT listener answers
- KRaft quorum has a leader and three voters; all three brokers registered
- `analytics.events` has the declared shape
- three JSON events produced and consumed back over `SASL_SSL`
- `/metrics` exposes Kafka, KRaft and JVM families
- journald carries Kafka's logs

---

## 6. Produce and consume by hand

`make verify` already does a round-trip, but to convince yourself interactively, use
the admin credentials the role installs at `/etc/kafka/admin.properties`. Nothing is
exposed publicly, so run this **on a node**.

Produce five events on **kafka-1**:

```bash
ssh ubuntu@<kafka-1-public-ip>
sudo -i
B=/opt/kafka/current/bin; C=/etc/kafka/admin.properties
for i in 1 2 3 4 5; do
  printf '{"event":"page_view","seq":%d}\n' "$i"
done | $B/kafka-console-producer.sh \
    --bootstrap-server <kafka-1-private-ip>:9092 \
    --command-config $C \
    --topic analytics.events \
    --request-required-acks all
```

Consume them back from **kafka-3**, i.e. a different broker than accepted the writes.
This is the useful version of the test: it exercises replication rather than a local
read.

```bash
ssh ubuntu@<kafka-3-public-ip>
sudo -i
B=/opt/kafka/current/bin; C=/etc/kafka/admin.properties
$B/kafka-console-consumer.sh \
    --bootstrap-server <kafka-3-private-ip>:9092 \
    --command-config $C \
    --topic analytics.events \
    --from-beginning --timeout-ms 20000
```

Check cluster state while you are there:

```bash
$B/kafka-metadata-quorum.sh --bootstrap-server <private-ip>:9092 \
    --command-config $C describe --status
$B/kafka-topics.sh --bootstrap-server <private-ip>:9092 \
    --command-config $C --describe --topic analytics.events
```

A healthy cluster shows a `LeaderId`, three `CurrentVoters`, and `Isr: 1,2,3` on every
partition. Partition leadership should be spread across all three brokers, not parked
on one.

---

## 7. Prove idempotency

```bash
make idempotence
```

Re-runs the deploy and fails the target unless **every** node reports `changed=0` and
`failed=0`. Parsing the recap is what turns "it looked fine" into a check.

To go further than the recap, confirm nothing on disk moved:

```bash
ansible -i inventory/generated.yml kafka -b -m ansible.builtin.shell \
  -a "openssl x509 -in /etc/kafka/tls/broker.pem -noout -serial;
      sha256sum /etc/kafka/tls/broker.pem /etc/kafka/server.properties;
      systemctl show kafka -p ActiveEnterTimestamp --value"
```

Run it before and after. Certificate serials, config hashes and — most importantly —
broker start timestamps must be identical. An unchanged start timestamp is the proof
that no broker was restarted.

---

## 8. The whole chain in one command

```bash
make test-e2e
```

`scripts/e2e.sh` runs everything above in order: dependency check, local material,
Terraform, inventory, collections, SSH wait, deploy, verify, idempotency, and a
public-port probe that includes SSH as a **positive control** — without it, a broken
probe path is indistinguishable from a correctly firewalled port.

On failure it deliberately leaves the infrastructure running and prints the destroy
command. A destroyed cluster cannot be debugged, and the failure is the interesting
part.

---

## 9. Day-2 operations

**Add a topic.** Add it to `kafka_topics` and re-run `make deploy`. Partition
*increases* are applied; partition *decreases* and replication-factor changes fail
loudly rather than silently deleting and recreating the topic. Removing a topic from
the list does **not** delete it — the role has no delete path at all.

**Add a SCRAM user.** Add it to `kafka_scram_users` and re-run. New principals are
created over `SASL_SSL` against the running cluster. Only `kafka_admin` and
`kafka_broker` are bootstrapped at format time, because inter-broker replication needs
credentials before any broker starts.

**Restart the fleet safely.**

```bash
make rolling-restart
```

One broker at a time, with a health gate between each. "Healthy" means the broker port
accepts connections, an authenticated API call succeeds over `SASL_SSL`, *and* the
quorum reports a leader — not merely that `systemctl` went green, which happens long
before the broker is serving.

---

## 10. Teardown

```bash
make destroy     # Terraform only
make clean       # destroy, then shred .local and the generated inventory
```

Verify independently rather than trusting the state file:

```bash
aws ec2 describe-instances --region eu-central-1 \
  --filters "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].[Tags[?Key==`Name`].Value|[0]]' --output text
```

Route B has no teardown — the role does not uninstall itself.

---

## 11. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Deploy hangs on the health gate, first run | A single node cannot elect a leader | Ensure all three hosts are in the play. Never bootstrap with `--limit` or `serial: 1` |
| `Failed to connect ... Connection refused` on SSH, mid-run | sshd `MaxStartups` throttling from back-to-back plays at `forks=10` | Transient. Re-run the stage; lower `forks` if persistent |
| SSH times out (not refused) | Firewall or security group | A timeout is the network denying you; a refusal means the host answered |
| `has KRaft storage initialised with cluster ID X, but this play expects Y` | Host holds another cluster's data | Investigate before acting. The role refuses to reformat because that destroys data |
| Quorum never forms | `kafka_controller_directory_ids` differs between hosts | All nodes must receive byte-identical identity. Regenerate once, redeploy |
| ExternalSecret-style auth failures after changing a password | SCRAM changed underneath running clients | Rotate deliberately; restart consumers |
| `terraform apply` fails on credentials | No AWS credentials on the control machine | Export them or configure `~/.aws/credentials` |

For what has and has not been tested, see `docs/test-results.md` — including an
explicit list of what was never exercised.
