# Research: existing Kafka Ansible roles and Kafka 4.3 behaviour

Four public roles were cloned and read before any code was written. They are
*inspiration only*. Where a role and the current Apache Kafka documentation
disagree, **the documentation wins** — several of these roles predate KRaft's
dynamic quorum and would have led the implementation somewhere wrong.

## Reference implementations

| Implementation | Useful pattern | Potential issue / outdated pattern | Decision for our role |
|---|---|---|---|
| **confluentinc/cp-ansible** (`confluent.platform` 8.3.0) | Thin template + property registry: `server.properties.j2` is a 5-line `\|dictsort` loop over a computed dict, so no conditionals live in the template and output is byte-stable. Health check *is* the rolling-restart gate — controller waits on `kafka-metadata-quorum describe --replication` until every voter is caught up. Green/brownfield split: `service_facts` decides parallel vs `serial: 1`. | Confluent-specific packages and `confluent.platform` collection throughout. Requires `hash_behaviour=merge` globally, asserted at role start. **Static `controller.quorum.voters` only — no KIP-853 support at all.** Node IDs derived from inventory *position*, which shifts if the inventory is reordered. | **Adopted:** health-check-as-restart-gate, `meta: flush_handlers` before the terminal start+health step, restart logic in its own task file so the handler and an operator playbook share one implementation. **Rejected:** Confluent packages, `hash_behaviour`, static quorum, positional node IDs (ours are explicit in the inventory). |
| **idealista/kafka_role** | KRaft is the default and correctly uses the **bootstrap.servers** path, not `quorum.voters`. Layered config merge (`kafka_cfg_default` / `_base` / `_extra`). Defaults split across `defaults/main/` files. `creates:` guard on the storage format step. | Its default version does not download: `kafka_version: 4.2.0` is fetched from `downloads.apache.org`, which only ever carries the *current* release. `kafka_mirror` (the archive) is declared but referenced nowhere — dead code. `kafka_initial_controllers` is used by the bootstrap command but defined only in a molecule fixture. | **Adopted:** the KIP-853 dynamic-quorum identity scheme and the `creates:` format guard. **Adapted:** the download/archive split, actually wired up — we fall back to `archive.apache.org` when a pinned version has aged out of `downloads.apache.org`. |
| **s3pweb/ansible-kafka-kraft** | Apache SHA-512 fetch-and-normalise done correctly (see below). Cluster ID generated exactly once and persisted, rather than re-derived per run. Two-tier namespace: public `kafka_*` in defaults, computed `kafka__*` in vars. | **Quorum voters derived from `ansible_play_hosts`** — running with `--limit one-node` silently rewrites the voter list to a single node. Cluster-ID lock file lives under the version symlink, so a version bump orphans it. Multi-log-dir format check only inspects `results[0]`. | **Adopted:** the checksum normalisation, generate-identity-once. **Rejected:** deriving quorum membership from the current run's hosts — ours comes from the inventory group, so `--limit` cannot reshape the cluster. |
| **sleighzy/ansible-kafka** | Versioned install dir + stable symlink. Split `defaults/main/` directory. `stat`-guard before download/unarchive. Passthrough dict at the tail of the properties template. | **ZooKeeper-only, zero KRaft support** — fatal on Kafka 4.x, which removed ZooKeeper entirely. `broker.id` instead of `node.id`. Templates `zookeeper.properties` unconditionally. **No storage formatting step at all**, so a KRaft data dir would never be initialised. | **Adopted:** versioned install + symlink layout, `creates:`-guarded unarchive, the `kafka_extra_config` passthrough. **Rejected:** everything ZooKeeper-era. |

## Where the Kafka 4.3 documentation overrides the reference roles

Two of the four roles build a **static** `controller.quorum.voters` string. The Kafka
4.3 configuration reference is explicit that this is the legacy mechanism:

> `controller.quorum.voters` — *"Map of id/endpoint information for the set of voters
> … This is the old way of defining membership for controller quorums and should NOT
> be set if using dynamic quorums. Instead, `controller.quorum.bootstrap.servers`
> should be set, and the voter set is determined by the `--standalone` or
> `--initial-controllers` flags when formatting."*

So this role sets `controller.quorum.bootstrap.servers` and never writes
`controller.quorum.voters` anywhere. The voter set is fixed once, at format time, by
`kafka-storage.sh format --initial-controllers`.

## Key Kafka 4.3 facts established from primary sources

Verified directly against `kafka.apache.org` and the `apache/kafka` tree at tag
`4.3.1`, not taken from the reference roles.

- **Version.** 4.3.1, released 2026-06-25. Binary artefact `kafka_2.13-4.3.1.tgz`.
- **Checksum format.** Apache publishes the digest as
  `kafka_2.13-4.3.1.tgz: C7D7B231 8CB51AA0 …` — filename-prefixed, upper-case, and
  space-grouped across several lines. This is accepted by neither `sha512sum -c` nor
  Ansible's `get_url checksum=`. It must be stripped of the prefix and all
  whitespace, then lower-cased. This single detail is the usual reason artefact
  verification quietly gets dropped from a Kafka role; `s3pweb` is the one reference
  implementation that handles it correctly.
- **Storage format.** `kafka-storage.sh format --cluster-id <id> --initial-controllers
  '<id>@<host>:<port>:<directory-uuid>,…' --config <file> --add-scram '…'`. Every node
  must be given a byte-identical `--initial-controllers` string, and each directory
  UUID must match the one written into that node's own metadata.
- **SCRAM bootstrap.** *"Credentials for inter-broker communication must be created
  before Kafka brokers are started. `kafka-storage.sh` can format storage with initial
  credentials."* Hence `--add-scram` at format time for the broker and admin
  principals, and `kafka-configs.sh` afterwards for application principals.
- **The two SCRAM CLIs quote differently.** `kafka-storage.sh --add-scram` expects
  `[name="user",password="secret"]`; `kafka-configs.sh --add-config` expects
  `[password=secret]` **unquoted** and stores the quote characters literally if you
  supply them. Getting this wrong on the admin principal locks the admin out of its
  own cluster — see `docs/test-results.md`, where exactly that happened.
- **TLS can be PEM.** `ssl.keystore.type=PEM` is supported natively: *"Default SSL
  engine factory supports only PEM format with a list of X.509 certificates"*. No
  keytool, no JKS, no PKCS#12. This matters for idempotency — `keytool` rewrites a
  keystore on every invocation, so roles built on it either restart Kafka every run or
  hide it behind `changed_when: false`.
- **Logging is log4j2.** Kafka 4.3 ships `config/log4j2.yaml`. Its stock configuration
  attaches the controller, authorizer, request and log-cleaner loggers to their own
  file appenders with `additivity: false`, so those records **never reach stdout** —
  meaning `journalctl -u kafka` would be missing precisely the controller lines an
  incident needs.

### Controller listener security

The one design question the documentation does not answer outright. `sasl.mechanism.controller.protocol`
exists and the docs show `CONTROLLER:SASL_SSL` in an example, so SASL on the controller
listener is configurable. But SCRAM specifically has a bootstrap problem: *"The default
implementation of SASL/SCRAM in Kafka stores SCRAM credentials in the metadata log"* —
and the metadata log is the thing the controller quorum exists to replicate. A
controller cannot resolve a SCRAM credential until the quorum it is trying to join is
already serving.

**Decision: the controller listener uses SSL with mutual TLS** (`ssl.client.auth=required`).
The trust anchor is a file on disk before the process starts, so there is no circular
dependency. Both listeners remain encrypted and authenticated; neither is PLAINTEXT.

## Observability research

- **JMX Exporter pinned to 1.6.0.** Maven Central carries this artefact only up to
  **1.0.1** — its `maven-metadata.xml` still advertises 1.0.1 as current. From 1.1.0
  onward the project publishes the agent jar as a **GitHub release asset** with a
  `.sha256` sidecar. Pointing a role at Maven Central for a 1.x version yields a 404
  that reads like a typo'd version number.
- **Do not append a catch-all `- pattern: ".*"`.** That is the exporter's
  *collect-everything* idiom (see upstream `examples/standalone_sample_config.yml`),
  not a drop rule. An unmatched MBean is already not exported; adding the catch-all
  exports every remaining attribute on the broker.
- **`current-state` is a string**, not a number, so it cannot be scraped as a gauge.
  Upstream's `kafka-kraft` example converts it to a constant-`1` series carrying the
  state as a label; this role does the same.
- **GC/thread/class metrics come from the agent's built-in collectors**
  (`jvm_gc_collection_seconds_*`), which shadow any `java.lang<type=GarbageCollector>`
  rule you write — verified here, where such a rule emitted nothing at all.
- **VictoriaLogs** ingests via `/insert/jsonline` with `_msg_field` / `_time_field` /
  `_stream_fields` query parameters; Fluent Bit's `systemd` input reads the unit's
  journal directly, so Kafka needs no log files on disk.

## Ansible/PKI research

`community.crypto` 3.3.0 (a major bump over the 2.x most tutorials describe):

- `openssl_privatekey` defaults to `regenerate: full_idempotence` — an existing usable
  key is left alone.
- `x509_certificate` has **no** `regenerate` option; it compares the existing
  certificate against the CSR. Critically, `ignore_timestamps` defaults to `true`, so a
  *relative* `ownca_not_after` such as `+825d` does **not** make the certificate look
  different on each run. That default is what keeps serial numbers stable.
- Kafka's PEM support means `community.general.java_keystore` is not needed at all,
  which also keeps the collection dependency list to two entries.

## Conclusions → design decisions

| Decision | Lands in |
|---|---|
| Fetch + normalise the Apache SHA-512, or use a pinned digest; fall back to the archive mirror | `tasks/install.yml` |
| Versioned install dir + `current` symlink; `creates:`-guarded unarchive | `tasks/install.yml` |
| PEM keystore/truststore, CA private key kept on the controller, broker keys never leave their node | `tasks/pki.yml` |
| `controller.quorum.bootstrap.servers`; never `controller.quorum.voters` | `templates/server.properties.j2` |
| Cluster ID + controller directory UUIDs minted once *outside* the role | `scripts/generate-cluster-identity.sh` |
| Format guarded by `meta.properties`; cluster-ID mismatch fails rather than reformats | `tasks/kraft.yml` |
| `--add-scram` for broker/admin at format time; `kafka-configs.sh` for the rest | `tasks/kraft.yml`, `tasks/security.yml` |
| BROKER = SASL_SSL/SCRAM-SHA-512, CONTROLLER = SSL/mTLS | `templates/server.properties.j2` |
| Console-only log4j2 so journald is complete | `templates/log4j2.yaml.j2` |
| Pinned JMX agent from GitHub releases, bound to the private address, no JMX remoting | `tasks/observability.yml`, `templates/kafka.service.j2` |
| Restart deferred to a `serial: 1` play so bootstrap can be parallel | `handlers/main.yml`, `playbooks/kafka.yml` |

## Sources

- <https://kafka.apache.org/43/generated/kafka_config.html> — broker configuration reference
- <https://github.com/apache/kafka/blob/4.3.1/docs/security/authentication-using-sasl.md>
- <https://github.com/apache/kafka/blob/4.3.1/docs/security/listener-configuration.md>
- <https://github.com/apache/kafka/blob/4.3.1/config/log4j2.yaml>
- <https://downloads.apache.org/kafka/4.3.1/> — artefact and `.sha512`
- <https://github.com/prometheus/jmx_exporter> — README, `examples/`, releases
- <https://docs.victoriametrics.com/victorialogs/data-ingestion/>
- <https://github.com/confluentinc/cp-ansible>, <https://github.com/idealista/kafka_role>,
  <https://github.com/s3pweb/ansible-kafka-kraft>, <https://github.com/sleighzy/ansible-kafka>
