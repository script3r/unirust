# Unirust

<div align="center">
  <img src="unirust.png" alt="Unirust Logo" width="350" height="350">
</div>

A high-performance temporal entity resolution engine in Rust.

## What is Entity Resolution?

Entity resolution (also known as record linkage or data matching) is the process of identifying records that refer to the same real-world entity across different data sources. Unirust adds **temporal awareness** - it understands that entity attributes change over time and handles conflicts intelligently.

**Example**: Three records from different systems all referring to "John Doe":
- CRM: `name="John Doe", email="john@old.com"` (valid 2020-2022)
- ERP: `name="John Doe", email="john@new.com"` (valid 2022-present)
- Web: `name="John Doe", phone="555-1234"` (valid 2021-present)

Unirust will:
1. Cluster these as the same entity based on identity keys (name)
2. Detect the email conflict during the overlapping 2022 period
3. Produce a golden record for any point in time

## Features

- **Temporal Awareness**: All data has validity intervals—merges and conflicts are evaluated per-time-period
- **Conflict Detection**: Automatic detection of attribute conflicts within clusters
- **Distributed**: Router + multi-shard architecture for horizontal scaling
- **Persistent**: RocksDB storage with crash recovery
- **Measured Performance**: Release baselines use persistent shards and include full entity resolution; see the benchmark below

## Quick Start

### Installation

```bash
git clone https://github.com/script3r/unirust.git
cd unirust
cargo build --release
```

### Single-Shard Mode (Development)

```bash
# Start a single shard
./target/release/unirust_shard --listen 127.0.0.1:50061 --shard-id 0 --ephemeral

# In another terminal, start the router
./target/release/unirust_router --listen 127.0.0.1:50060 --shards 127.0.0.1:50061
```

### Local Multi-Shard Cluster

```bash
# Use the cluster script
SHARDS=5 ONTOLOGY=/etc/unirust/ontology.json ./scripts/cluster.sh start

# Restart the same persistent cluster without deleting records
SHARDS=5 ONTOLOGY=/etc/unirust/ontology.json ./scripts/cluster.sh restart

# Destructive reset is separate and requires explicit confirmation
UNIRUST_CONFIRM_RESET=1 ./scripts/cluster.sh reset

# Or start manually:
./target/release/unirust_shard --listen 127.0.0.1:50061 --shard-id 0 --data-dir /data/shard0 --backup-dir /backup/shard0
./target/release/unirust_shard --listen 127.0.0.1:50062 --shard-id 1 --data-dir /data/shard1 --backup-dir /backup/shard1
./target/release/unirust_shard --listen 127.0.0.1:50063 --shard-id 2 --data-dir /data/shard2 --backup-dir /backup/shard2
./target/release/unirust_shard --listen 127.0.0.1:50064 --shard-id 3 --data-dir /data/shard3 --backup-dir /backup/shard3
./target/release/unirust_shard --listen 127.0.0.1:50065 --shard-id 4 --data-dir /data/shard4 --backup-dir /backup/shard4

./target/release/unirust_router --listen 127.0.0.1:50060 \
  --shards 127.0.0.1:50061,127.0.0.1:50062,127.0.0.1:50063,127.0.0.1:50064,127.0.0.1:50065
```

### Using the Library

```rust
use unirust_rs::{Unirust, PersistentStore, StreamingTuning, TuningProfile};
use unirust_rs::ontology::{Ontology, IdentityKey, StrongIdentifier};

// Create ontology (matching rules)
let mut ontology = Ontology::new();
ontology.add_identity_key(IdentityKey::new(
    vec![name_attr, email_attr],
    "name_email".to_string()
));
ontology.add_strong_identifier(StrongIdentifier::new(
    ssn_attr,
    "ssn_unique".to_string()
));

// Open persistent store
let store = PersistentStore::open("/path/to/data")?;

// Create engine with tuning profile
let tuning = StreamingTuning::from_profile(TuningProfile::HighThroughput);
let mut engine = Unirust::with_store_and_tuning(ontology, store, tuning);

// Ingest records
let result = engine.ingest(records)?;
println!("Assigned {} records to {} clusters",
    result.assignments.len(),
    result.cluster_count);
println!("Detected {} conflicts", result.conflicts.len());

// Query entities
let matches = engine.query(&descriptors, interval)?;
```

## Configuration

Unirust uses a layered configuration system: **CLI args > Environment variables > Config file > Defaults**

### Config File (TOML)

```toml
# unirust.toml
profile = "high-throughput"

[shard]
listen = "0.0.0.0:50061"
id = 0
data_dir = "/var/lib/unirust/shard-0"
backup_dir = "/var/backups/unirust/shard-0"
tls_cert = "/etc/unirust/tls/shard-0.crt"
tls_key = "/etc/unirust/tls/shard-0.key"
tls_client_ca = "/etc/unirust/tls/clients-ca.crt"

[router]
listen = "0.0.0.0:50060"
shards = ["https://shard-0:50061", "https://shard-1:50061", "https://shard-2:50061"]
tls_cert = "/etc/unirust/tls/router.crt"
tls_key = "/etc/unirust/tls/router.key"
tls_client_ca = "/etc/unirust/tls/clients-ca.crt"
shard_tls_ca = "/etc/unirust/tls/shards-ca.crt"
shard_tls_cert = "/etc/unirust/tls/router-client.crt"
shard_tls_key = "/etc/unirust/tls/router-client.key"

[storage]
block_cache_mb = 1024
write_buffer_mb = 256
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `UNIRUST_CONFIG` | Path to config file |
| `UNIRUST_PROFILE` | Tuning profile |
| `UNIRUST_SHARD_LISTEN` | Shard listen address |
| `UNIRUST_SHARD_ID` | Shard ID |
| `UNIRUST_SHARD_DATA_DIR` | Persistent shard data directory |
| `UNIRUST_SHARD_BACKUP_DIR` | External checkpoint root |
| `UNIRUST_SHARD_TLS_CERT` | Shard server certificate |
| `UNIRUST_SHARD_TLS_KEY` | Shard server private key |
| `UNIRUST_SHARD_TLS_CLIENT_CA` | CA for required shard client certificates |
| `UNIRUST_SHARD_REPLICA` | Passive replica endpoint for this primary |
| `UNIRUST_SHARD_REPLICA_MODE` | Run as a passive replica |
| `UNIRUST_SHARD_REPLICATION_TOKEN_FILE` | Shared secret file for one replica pair |
| `UNIRUST_SHARD_ALLOW_INSECURE_REPLICATION` | Permit plaintext replication for isolated development |
| `UNIRUST_SHARD_REPLICA_CONNECT_TIMEOUT_SECS` | Replica connection timeout |
| `UNIRUST_SHARD_REPLICA_REQUEST_TIMEOUT_SECS` | Per-RPC replica timeout |
| `UNIRUST_SHARD_REPLICA_TCP_KEEPALIVE_SECS` | Replica TCP keepalive interval |
| `UNIRUST_SHARD_REPLICA_TLS_CA` | CA used to verify the replica |
| `UNIRUST_SHARD_REPLICA_TLS_CERT` | Primary certificate presented to the replica |
| `UNIRUST_SHARD_REPLICA_TLS_KEY` | Primary client private key |
| `UNIRUST_ROUTER_SHARDS` | Comma-separated shard addresses |
| `UNIRUST_ROUTER_CHECKPOINT_INTERVAL_SECS` | Coordinated checkpoint interval (`0` disables) |
| `UNIRUST_ROUTER_SHARD_CONNECT_TIMEOUT_SECS` | Shard connection timeout |
| `UNIRUST_ROUTER_SHARD_REQUEST_TIMEOUT_SECS` | Per-RPC shard timeout |
| `UNIRUST_ROUTER_TLS_CERT` | Router server certificate |
| `UNIRUST_ROUTER_TLS_KEY` | Router server private key |
| `UNIRUST_ROUTER_TLS_CLIENT_CA` | CA for required router client certificates |
| `UNIRUST_ROUTER_SHARD_TLS_CA` | CA used to verify shard certificates |
| `UNIRUST_ROUTER_SHARD_TLS_CERT` | Router certificate presented to shards |
| `UNIRUST_ROUTER_SHARD_TLS_KEY` | Router client private key |

### Tuning Profiles

| Profile | Use Case |
|---------|----------|
| `balanced` | General purpose (default for library) |
| `low-latency` | Interactive queries, fast responses |
| `high-throughput` | Batch processing (default for binaries) |
| `bulk-ingest` | Large initial loads with lower candidate caps; entity resolution remains enabled |
| `memory-saver` | Constrained environments |
| `billion-scale` | Disk-backed DSU/index; see the recovery and memory limits below |

## API Reference

### gRPC Services

**Router Service** (client-facing):
- `IngestRecords` - Ingest a batch of records
- `QueryEntities` - Query entities by descriptors and time range
- `ListConflicts` - List detected conflicts
- `GetStats` - Get cluster statistics
- `Reconcile` - Trigger cross-shard reconciliation

**Shard Service** (internal):
- Same as router, plus boundary tracking RPCs

### Library API

```rust
// Core operations
engine.ingest(records) -> IngestResult
engine.query(descriptors, interval) -> QueryOutcome
engine.clusters() -> Clusters
engine.graph() -> KnowledgeGraph

// Persistence
engine.checkpoint() -> Result<()>

// Metrics
engine.stats() -> Stats
```

## Architecture

See [DESIGN.md](DESIGN.md) for detailed architecture documentation, including:
- Entity resolution algorithm (4-phase streaming linker)
- Conflict detection algorithms (sweep-line vs atomic intervals)
- Distributed architecture (router + shards)
- Cross-shard reconciliation protocol
- Storage layer (RocksDB column families)
- Performance optimizations

## Examples

The `examples/` directory demonstrates the supported sharded deployment model:

- `cluster.rs` - Full 3-shard distributed cluster with router
- `unirust.toml` - Persistent router and shard configuration

Run examples:
```bash
# Distributed cluster (requires the persistent cluster running first)
SHARDS=3 ./scripts/cluster.sh start
cargo run --example cluster
./scripts/cluster.sh stop
```

## Durability

Persistent shards use two recovery layers for every ingest request:

1. The request is written to a versioned, checksummed binary ingest WAL. The
   file and its parent directory are synced before entity resolution begins.
2. Records, indexes, cluster assignments, and all other state produced by the
   request are written to RocksDB, then its WAL is synced to stable storage.
3. Only after that sync succeeds is the ingest WAL removed and the request
   acknowledged. Its directory is synced again after removal.

On restart, a remaining ingest WAL is replayed idempotently. A pending WAL is
never overwritten by a later request; a failed ingest therefore requires shard
restart and replay before more traffic is accepted. A truncated or corrupt WAL
is preserved with a `.corrupt.*` suffix and shard startup fails with a data-loss
error instead of accepting traffic with an unknown recovery gap. Cross-shard
merge redirects are also persisted before acknowledgement and reloaded when the
streaming linker is reconstructed.

The tuple `(entity_type, perspective, uid)` identifies one immutable source
record. Retrying that identity with the same descriptors is idempotent, including
when descriptor order differs. Reusing it with different values or validity
intervals is rejected instead of acknowledging data that would be discarded.
Temporal revisions must use a new source-record UID; entity resolution links the
snapshots through their configured identity keys.

In a multi-shard cluster, the router first durably reserves that source identity
on a shard selected only from the source tuple. The reservation stores a
canonical binary payload digest and the original ingest target before the record
enters the normal shard entity-resolution path. This prevents a changed
identity-key value from routing the same source UID to another shard. If target
ingest fails after reservation, retry the exact request; the reservation remains
valid and no record has bypassed entity resolution.

Router startup performs a crash-resumable one-time reservation backfill for
records written by an earlier release. Each target shard is marked complete only
after every exported record has been reserved durably. Conflicting legacy
duplicates make startup fail closed for operator repair. The internal router and
shard protocol is versioned, and mixed versions are rejected, so upgrades must
replace the full cluster with one coordinated Unirust version before restarting
the router. Shard gRPC ports are internal APIs; client ingest must use the router.
The persisted reservation directory is also bound to the configured shard count.
Router startup rejects shard-count changes because the current import API cannot
atomically move a record, update its reservation, and delete the old copy.
Cross-shard copy imports therefore fail closed; online scale-out and scale-in
require a future transactional relocation protocol or an offline rebuild.

Cross-shard redirects are durably applied to every shard and are idempotent. If
any shard fails, or the initiating request is cancelled, while a reconciliation
result is being applied, the router latches the cluster closed for ingest, query,
and administrative traffic rather than serving a partially updated global view.
Retrying `Reconcile` repairs the retained dirty keys in place. After a router or
full-cluster restart, router startup performs that repair before returning a
serviceable node and clears the dirty generation only after every shard
converges.

Cluster-wide ontology replacement has the same fail-closed cancellation
semantics. Readiness stays failed after an ambiguous partial update until
`SetOntology` is retried with the intended configuration or every shard is
recovered offline to one configuration. Router startup also rejects mismatched
shard ontologies.

The shard reconstructs all derived linker state before opening its gRPC
listener. Recovery scans persisted records in ordered, bounded batches through
the normal entity-resolution path, so recovery time remains O(record count).
This is a correctness-first crash-recovery path, not a bounded recovery-time
guarantee. Measure restart time at the intended dataset size and set
orchestration startup probes accordingly.

Global cluster IDs are anchored to durable record IDs so replay order cannot
change cross-shard identity. On the first startup of a database created before
this scheme marker existed, the shard atomically removes allocation-order
redirects that cannot be trusted after replay; router startup reconstructs them
from authoritative records before becoming ready. Upgrade every shard and the
router together for this transition rather than mixing versions.

`LinkerStateConfig` cache capacities are not enforced because the current LRU
backend has no durable spill/read-through path. Evicting cluster IDs, strong-ID
summaries, or record perspectives would change entity-resolution results.
Persistent profiles therefore retain this correctness-critical working set in
memory even though their DSU and identity index are disk-backed. The
`billion-scale` profile must not be treated as proof that a billion-record
deployment fits a given memory or recovery-time budget.

`scripts/cluster.sh start` and `restart` preserve `DATA_DIR`. Only the explicit
`reset` action deletes shard data, and it requires `UNIRUST_CONFIRM_RESET=1`.
The script defaults to `examples/loadtest-ontology.json`; set `ONTOLOGY` to the
same immutable configuration on every shard and router for another deployment.
Router startup compares the complete ontology reported by every shard and fails
closed on a mismatch.

The destructive gRPC `Reset` method is disabled by default because a sequential
multi-shard reset cannot be atomic. The supported production reset is the
confirmed offline script action. Test or isolated admin deployments can opt in
with the shard flag `--allow-destructive-admin`.

The shard and router binaries handle SIGINT/SIGTERM with graceful gRPC shutdown.
The shard drains active mutations and flushes dirty DSU and authoritative store
state before exit. Acknowledged ingests are additionally covered by an
executable-level SIGKILL, restart, SIGTERM, and second-restart integration test.

Use `unirust_healthcheck --shard <URI>` and
`unirust_healthcheck --router <URI>` for readiness probes. These call the gRPC
health methods rather than checking only for an open socket. Shard readiness
requires completed WAL recovery and a healthy persistent store; router
readiness additionally requires a consistent reconciliation state and a
successful health response from every configured shard. The provided Compose
deployment and cluster script use these semantic probes.

Router-to-shard calls are bounded by configurable transport settings under
`[router]`: `shard_connect_timeout_secs` (10 seconds by default),
`shard_request_timeout_secs` (120 seconds), and
`shard_tcp_keepalive_secs` (30 seconds). A deadline error or client cancellation
is not proof that a mutation was rolled back; an admitted ingest continues to a
durable outcome after its caller disconnects. Retry ingest with the same
immutable source-record identity and payload so the operation is resolved
idempotently.

### Synchronous Replication

A logical shard can run as one primary and one passive replica on distinct
persistent volumes and failure domains. Both processes use the same shard ID,
ontology, config version, and replication token. Bootstrap both volumes from
the same committed checkpoint, or start with two empty volumes. Primary startup
computes a SHA-256 digest over every logical RocksDB key/value pair on both
nodes and refuses traffic unless their complete durable states match. Pairing
startup is O(database size), so measure it against the production dataset.
Set `UNIRUST_SHARD_REPLICA_REQUEST_TIMEOUT_SECS` above the measured worst-case
pairing digest time.

Generate a separate secret for each pair, store at least 32 random bytes in a
file readable only by the service account, and mount the same content on both
nodes. Start the passive replica first:

```bash
unirust_shard \
  --shard-id 0 \
  --data-dir /var/lib/unirust/shard-0-replica \
  --backup-dir /var/backups/unirust/shard-0-replica \
  --replica-mode \
  --replication-token-file /etc/unirust/replication/shard-0.token \
  --tls-cert /etc/unirust/tls/shard-0-replica.crt \
  --tls-key /etc/unirust/tls/shard-0-replica.key \
  --tls-client-ca /etc/unirust/tls/primaries-ca.crt
```

Then start the primary with its normal shard server credentials plus:

```bash
unirust_shard \
  --shard-id 0 \
  --data-dir /var/lib/unirust/shard-0-primary \
  --backup-dir /var/backups/unirust/shard-0-primary \
  --replica https://shard-0-replica:50061 \
  --replication-token-file /etc/unirust/replication/shard-0.token \
  --replica-tls-ca /etc/unirust/tls/replicas-ca.crt \
  --replica-tls-cert /etc/unirust/tls/shard-0-primary.crt \
  --replica-tls-key /etc/unirust/tls/shard-0-primary.key
```

Every durable mutation is applied to the replica first, then locally, under one
per-pair serialization gate. The primary acknowledges only after both results
match. An unavailable or ambiguous replica result latches the primary
unhealthy and blocks reads and writes until operators reconcile the pair.
Replication therefore protects acknowledged writes from one volume loss, but
adds replica latency and requires both nodes to be available for writes.

Failover is manual because Unirust does not implement leader election or
quorum fencing:

1. Prove the old primary is stopped or isolated from clients and the replica.
2. Stop the passive process and restart its volume without `--replica-mode`.
3. Point the router at the promoted endpoint and restart the router.
4. Rebootstrap the old primary from a checkpoint of the promoted node before
   attaching it as a new passive replica.

Never serve the old primary and promoted replica simultaneously. Doing so can
create split brain. Online reset is disabled while a primary has a replica;
stop and rebootstrap both members together. Keep verified off-host backups for
correlated failures, operator mistakes, and storage corruption.

### External Backups

Configure each shard with a unique checkpoint root on storage outside its data
directory and, for real volume-loss protection, in a separate failure domain:

```toml
[shard]
data_dir = "/var/lib/unirust/shard-0"
backup_dir = "/var/backups/unirust/shard-0"

[router]
# Select this from the deployment RPO. Zero disables automatic checkpoints.
checkpoint_interval_secs = 3600
```

Trigger checkpoints through the router so router-mediated mutations remain
blocked for the complete cluster snapshot. A supplied name is created beneath
every shard's configured backup root:

```bash
grpcurl -plaintext \
  -d '{"path":"backup-2026-07-24T1300Z"}' \
  127.0.0.1:50060 unirust.RouterService/Checkpoint
```

Checkpoint creation uses a two-phase prepare/commit protocol. Every shard first
flushes its in-memory linker state and creates a RocksDB snapshot. Only after
all shards prepare successfully does the router write a binary commit marker to
every snapshot. The response includes the shared `generation` and
`committed: true`. A failed generation remains uncommitted and cannot be
restored; retrying the same name is idempotent and completes any missing
prepare or commit steps. Do not call the shard checkpoint RPC directly for a
production backup.

The router scheduler waits one configured interval before its first checkpoint.
If a shard fails during prepare or commit, the scheduler retains and retries the
same immutable generation instead of creating a stream of unrelated partial
snapshots. Successful and failed generations are emitted to structured logs.
The container deployment enables hourly checkpoints by default; set
`UNIRUST_CHECKPOINT_INTERVAL_SECS` explicitly to choose another RPO or `0` to
disable them.

To recover from lost data volumes, stop the complete cluster and restore every
shard from the same checkpoint generation into an empty replacement directory:

```bash
unirust_shard \
  --shard-id 0 \
  --data-dir /replacement/shard-0 \
  --backup-dir /var/backups/unirust/shard-0 \
  --restore-from /var/backups/unirust/shard-0/backup-2026-07-24T1300Z \
  --ontology /etc/unirust/ontology.json
```

Restore refuses a non-RocksDB source, symlinks, a nonempty destination, and an
existing partial staging directory. It also requires matching binary
prepare/commit markers, verifies the checkpoint belongs to the requested shard,
and opens both the source and staged copy read-only with RocksDB paranoid checks.
It copies and syncs into a sibling staging directory before publishing the
replacement with one rename. Restore the whole cluster together; restoring
only one older shard beside newer peers can violate the cluster snapshot
boundary. Each shard retains the committed checkpoint provenance in its
replacement data directory and refuses a manifest for another shard. Router
startup requires every shard to be either unrestored or restored from the same
generation and shard count, then verifies the topology, ontology, and protocol
versions before accepting traffic. Mixed restored/unrestored volumes and
different committed generations fail closed.

Export a complete committed generation to storage mounted from another failure
domain. All shard checkpoint paths must be accessible to the export process:

```bash
unirust_backup export \
  --destination /mnt/off-host/unirust/backup-2026-07-24T1300Z \
  --checkpoint /var/backups/unirust/shard-0/backup-2026-07-24T1300Z \
  --checkpoint /var/backups/unirust/shard-1/backup-2026-07-24T1300Z \
  --checkpoint /var/backups/unirust/shard-2/backup-2026-07-24T1300Z

unirust_backup verify \
  --backup /mnt/off-host/unirust/backup-2026-07-24T1300Z
```

Export validates that every shard is present exactly once and belongs to the
same generation and topology. It copies into a sibling staging directory,
records every file length and SHA-256 digest in a binary manifest, opens every
copied RocksDB checkpoint read-only with paranoid checks, syncs the tree, and
publishes it with one rename. Verification rejects modified, missing, extra, or
symlinked content. Restore from the exported `shard-0`, `shard-1`, and
`shard-2` directories, not from the deleted local checkpoint roots.

Retention only removes generations after every entry in its root verifies:

```bash
unirust_backup prune --root /mnt/off-host/unirust --retain 14
```

The built-in scheduler creates coordinated source checkpoints; it does not
automatically run the export command. Schedule export and verification after
checkpoint completion, monitor both, and run periodic restore drills. The
destination filesystem must provide independent storage and encryption at rest.
Without an enabled synchronous replica, the recovery point for a lost volume
remains the last successfully exported generation and acknowledged writes
after it can be lost. Process crashes and ordinary restarts remain covered
independently by the synced ingest and RocksDB WALs.

## Deployment Security

Router and shard binaries support mutually authenticated TLS. Each server
requires its certificate, private key, and client CA as an all-or-none group.
The router-to-shard client likewise requires a CA, client certificate, and
private key together, and secured shard addresses must use `https://`. Startup
fails closed for partial certificate groups, unreadable or empty PEM files,
plaintext endpoints paired with TLS configuration, or HTTPS endpoints without
explicit trust configuration. Certificate SANs must match the endpoint host.

Client tools use `--tls-ca`, `--tls-cert`, and `--tls-key`; the semantic health
probe uses `--ca-cert`, `--client-cert`, and `--client-key`. All three options
are required together. Certificate rotation currently requires a process
restart. Production deployments must enable native mTLS on the router and every
shard, or enforce equivalent authenticated TLS through a service mesh. Keep
shard ports private and never expose plaintext gRPC to an untrusted network.
The supplied Compose file is a local plaintext example and binds its router
port to loopback only.

Replication additionally uses a per-pair shared token. The shard binary rejects
plaintext replication by default; `--allow-insecure-replication` exists only
for isolated development. Protect token files as credentials, rotate them by
stopping and restarting both members, and use different tokens for every pair.

The shard binary requires a persistent `--data-dir` and a non-overlapping
`--backup-dir`. Mount them from independent storage: separate directory names
alone do not protect against volume loss. An in-memory shard can only be started
with the explicit `--ephemeral` flag and loses all records when the process
exits. `--allow-colocated-checkpoints` is a development-only escape hatch and
does not provide volume-loss recovery.

Router and shard servers cap each encoded or decoded gRPC message at 4 MiB,
limit each connection to 128 concurrent requests, and shed excess load. Use the
streaming ingest, import, and export RPCs for larger transfers. Bound connection
counts and request rates at the load balancer as well.

## Performance

Release verification on an Apple M5 with 32 GB RAM, five persistent shards,
16 concurrent streams, 5,000-record batches, and 10% overlap:

| Records | Records/sec | Stream Errors |
|---------|-------------|---------------|
| 10,000,000 | 50,598 | 0 |

This is an end-to-end power-loss-durability measurement: records and cluster
assignments are persisted and the RocksDB WAL is synchronously flushed before
acknowledgement, and every record goes through entity resolution. Results depend
on storage hardware and ontology complexity; rerun the command below on the
release target rather than treating this number as a service-level guarantee.

## Development

```bash
# Fast correctness gate (unit and integration tests)
cargo test

# Compile examples, binaries, and benchmarks without executing benchmarks
cargo check --all-targets --all-features

# Run quick benchmarks (~30s)
cargo bench --bench bench_quick

# Run load test (start cluster first: SHARDS=5 ./scripts/cluster.sh start)
./target/release/unirust_loadtest \
  --router http://127.0.0.1:50060 \
  --count 10000000 \
  --streams 16 \
  --batch 5000

# Format and lint
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
```

`cargo test --all-targets` executes Criterion benchmark binaries on some Cargo
versions and is intentionally not the default correctness gate. Run benchmarks
explicitly with `cargo bench --bench <name>`.

### Test Strategy

- Unit tests cover temporal algebra, matching, DSU behavior, indexes, and focused
  persistence failure modes.
- Integration tests exercise router and shard RPCs with temporary
  `PersistentStore` databases, including restart, WAL, reconciliation, streaming,
  reset, and rebalance behavior.
- `cargo check --all-targets --all-features` compiles examples and benchmarks
  without mixing performance workloads into the correctness suite.
- `bench_quick` is the local performance smoke test. The distributed load test is
  the release benchmark and must be run against persistent shards.

## Container Deployment

```bash
# Build image
podman build -t unirust -f Containerfile .

# Run a single shard
podman run --rm -p 50061:50061 -v unirust-data:/data unirust shard --shard-id 0

# Run router
podman run --rm -p 50060:50060 unirust router --shards host.containers.internal:50061
```

### Cluster with Compose

Deploy a 3-shard cluster:
```bash
# Start cluster
podman-compose up -d

# Check status
podman-compose ps

# View router logs
podman-compose logs -f router

# Run loadtest
podman-compose run --rm loadtest

# Stop and clean up
podman-compose down

# Explicitly delete all persistent shard volumes
podman-compose down -v
```

## License

MIT. See [LICENSE](LICENSE).
