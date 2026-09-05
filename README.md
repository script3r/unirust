# Unirust

<div align="center">
  <img src="unirust.png" alt="Unirust Logo" width="350" height="350">
</div>

A temporal entity resolution engine in Rust, with RocksDB persistence and a
gRPC router/shard deployment model.

The current release is [v0.2.0](https://github.com/script3r/unirust/releases/tag/v0.2.0).
The subsequent [CI fix](https://github.com/script3r/unirust/pull/8) authenticates
`protoc` downloads; it is not a new runtime release. Documentation and example
updates on `main` may be newer than the release tag. See [CHANGELOG.md](CHANGELOG.md)
for release scope and compatibility notes.

## Entity Resolution and Time

Unirust links source records using configured identity keys. Descriptors carry
integer, half-open validity intervals `[start, end)`, with `start < end`. Choose
one time unit for the dataset; the engine does not convert timestamps or infer
matching rules from attribute names.

For example, two records that share an email identity key during `[0, 10)` can
form one entity. A role of `analyst` during `[0, 5)` and `manager` during `[5, 10)`
is a change over time, not an overlap. If different role values overlap, golden
output omits those values for the conflicting interval rather than choosing a
preferred source automatically.

Configured strong identifiers guard merges when components contain different
values of the same strong attribute **in the same perspective during overlapping
time**. They do not impose a blanket ban on different values across sources.
Cross-shard reconciliation applies these guards to whole connected components,
including transitive merge candidates.

Queries combine descriptors with AND over their simultaneous validity. For a
reconciled entity, the router combines matching descriptors and golden fields
across all contributing shards. Cross-shard reconciliation is asynchronous;
call `Reconcile` when a workflow needs pending boundary work processed before a
query. Its scheduling thresholds are not a guaranteed freshness deadline.

Every new record goes through entity resolution. That does not imply exhaustive
matching: identity-key extraction limits each attribute to eight coalesced
value/interval alternatives, and some processing paths and tuning profiles cap
or sample candidates. Validate matching results with representative high
cardinality data. See [DESIGN.md](DESIGN.md) for algorithm and path-specific
limits.

## Quick Start

### Build Prerequisites

Use Rust **1.88 or newer**, a C/C++ toolchain, CMake, libclang, and `protoc` on
`PATH`. RocksDB and its native dependencies are compiled during the build;
`build.rs` also generates Rust bindings from the protobuf schema.

On Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake libclang-dev protobuf-compiler
```

On macOS, install Xcode Command Line Tools and the native dependencies, for
example with Homebrew:

```bash
xcode-select --install
brew install cmake protobuf llvm
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
```

Build the release tag, including the optional load-test tool:

```bash
git clone --branch v0.2.0 --depth 1 https://github.com/script3r/unirust.git
cd unirust
cargo build --release --locked --bins --features test-support
```

### Persistent Three-Shard Local Demo

Run this from the repository root with ports **50060–50063 available** and no
`UNIRUST_CONFIG` or `UNIRUST_SHARD_*` / `UNIRUST_ROUTER_*` environment overrides.
It creates an explicit email-matching ontology, starts three persistent shards
and a router, waits for readiness, then runs a sample ingest and query client.
The script stops its processes on exit and preserves the printed data/log
directory for inspection.

```bash
bash <<'SH'
set -e
demo_dir="$(mktemp -d "${TMPDIR:-/tmp}/unirust-demo.XXXXXX")"
shard_pids=()
router_pid=""
cleanup() {
  if [ -n "$router_pid" ]; then
    kill -TERM "$router_pid" 2>/dev/null || true
    wait "$router_pid" 2>/dev/null || true
  fi
  for pid in "${shard_pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  for pid in "${shard_pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT
trap 'exit 130' INT TERM

cat > "$demo_dir/ontology.json" <<'JSON'
{
  "identity_keys": [{"name": "email_key", "attributes": ["email"]}],
  "strong_identifiers": [],
  "constraints": []
}
JSON

wait_ready() {
  for attempt in $(seq 1 120); do
    if ./target/release/unirust_healthcheck "$1" "$2" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  cat "$demo_dir"/*.log >&2
  return 1
}

for shard in 0 1 2; do
  port=$((50061 + shard))
  ./target/release/unirust_shard \
    --listen "127.0.0.1:$port" --shard-id "$shard" \
    --data-dir "$demo_dir/shard-$shard" --allow-colocated-checkpoints \
    --ontology "$demo_dir/ontology.json" \
    > "$demo_dir/shard-$shard.log" 2>&1 &
  shard_pids+=("$!")
done
for port in 50061 50062 50063; do
  wait_ready --shard "http://127.0.0.1:$port"
done

./target/release/unirust_router \
  --listen 127.0.0.1:50060 \
  --shards 127.0.0.1:50061,127.0.0.1:50062,127.0.0.1:50063 \
  --ontology "$demo_dir/ontology.json" \
  > "$demo_dir/router.log" 2>&1 &
router_pid="$!"
wait_ready --router http://127.0.0.1:50060

./target/release/unirust_client \
  --router http://127.0.0.1:50060 --ontology "$demo_dir/ontology.json"
printf 'Persistent demo data and logs: %s\n' "$demo_dir"
SH
```

The client prints assignments, an Alice email match, and a conflict response
for the shared `role=admin` query. This is a plaintext loopback demo. Its
`--allow-colocated-checkpoints` flag permits checkpoints beneath each shard's
data directory; it provides no independent copy if that volume is lost.
Production requires authenticated transport and checkpoint/replica storage in
independent failure domains. Different directory names or container volumes do
not, by themselves, establish that independence.

### Local Cluster Script

For a cluster that stays running, [scripts/cluster.sh](scripts/cluster.sh)
defaults to three persistent shards and `examples/loadtest-ontology.json`:

```bash
./scripts/cluster.sh start
./scripts/cluster.sh status
./scripts/cluster.sh restart
./scripts/cluster.sh stop
```

`start` and `restart` preserve data; `status` checks process IDs, while startup
waits for gRPC readiness. The defaults are `cluster_data`, `cluster_backups`, and
`cluster_logs` beneath the repository, with automatic checkpoints disabled.
These local paths are not independent storage. Set `SHARDS`, `ONTOLOGY`,
`DATA_DIR`, `BACKUP_DIR`, `LOG_DIR`, and `CHECKPOINT_INTERVAL_SECS` consistently
for a different local deployment. The script is not a production TLS or
multi-host orchestrator. It builds the server and healthcheck binaries, but
not the load-test or sample client binaries.

After stopping the local cluster, an explicit destructive reset is available:

```bash
UNIRUST_CONFIRM_RESET=1 ./scripts/cluster.sh reset
```

This deletes the configured `DATA_DIR`, not `BACKUP_DIR`. Keep shard count and
ontology fixed for an existing dataset; changing `SHARDS` is not an online
rebalance operation.

### Using the Library

The library can run a local engine against a persistent database. This example
is separate from the distributed deployment above; applications using the
cluster should send requests through the router.

Add these dependencies to a Rust binary crate:

```toml
[dependencies]
unirust-rs = "0.2.0"
anyhow = "1"
```

A complete `src/main.rs` (with the same native build prerequisites):

```rust
use unirust_rs::ontology::{IdentityKey, StrongIdentifier};
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, QueryDescriptor, Record,
    RecordId, RecordIdentity, StreamingTuning, Unirust,
};

fn main() -> anyhow::Result<()> {
    let store = PersistentStore::open("library-data")?;
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email_key"));
    ontology.add_strong_identifier(StrongIdentifier::from_name("ssn", "ssn_guard"));
    let mut engine =
        Unirust::with_store_and_tuning(ontology, store, StreamingTuning::balanced());

    let email = engine.intern_attr("email");
    let alice = engine.intern_value("alice@example.com");
    let interval = Interval::new(0, 10)?;
    let records = vec![
        Record::new(
            RecordId(0),
            RecordIdentity::new("person".into(), "crm".into(), "alice-v1".into()),
            vec![Descriptor::new(email, alice, interval)],
        ),
        Record::new(
            RecordId(1),
            RecordIdentity::new("person".into(), "erp".into(), "alice-v1".into()),
            vec![Descriptor::new(email, alice, interval)],
        ),
    ];
    let result = engine.ingest(records)?;
    println!(
        "Assigned {} records; {} clusters, {} conflicts",
        result.assignments.len(), result.cluster_count, result.conflicts.len()
    );
    let outcome = engine.query(&[QueryDescriptor { attr: email, value: alice }], interval)?;
    println!("{outcome:?}");
    engine.checkpoint()?;
    Ok(())
}
```

`checkpoint()` flushes this engine's state; it does not create a coordinated
external cluster backup. See the [library API](https://docs.rs/unirust-rs/0.2.0/unirust_rs/)
and [examples/cluster.rs](examples/cluster.rs) for the gRPC client model. The
cluster example connects to already-running servers and installs its own
ontology, so use it with an empty cluster or one already using those exact
rules. It cannot replace the ontology of an existing load-test dataset.

## Configuration

For supported shared settings, precedence is **CLI > environment > TOML >
defaults**. A TOML file is read only when selected with `--config PATH` or
`UNIRUST_CONFIG`; merely creating `unirust.toml` does not load it. CLI switches
such as `--ephemeral`, `--allow-colocated-checkpoints`, and
`--allow-destructive-admin` are not TOML settings.

Supply the same ontology and config version on every shard and router. Omitting
`--ontology` / its configuration setting loads an empty rule set, which does
not provide identity-key matching. Ontology JSON is external configuration;
record storage, ingest WALs, and backup manifests use binary formats.

This is a deployment template: provision the named directories, ontology,
certificates, and reachable shard hosts before running it, and give each shard
its own ID and paths.

```toml
profile = "high-throughput"

[shard]
listen = "0.0.0.0:50061"
id = 0
data_dir = "/var/lib/unirust/shard-0"
backup_dir = "/var/backups/unirust/shard-0"
ontology = "/etc/unirust/ontology.json"
tls_cert = "/etc/unirust/tls/shard-0.crt"
tls_key = "/etc/unirust/tls/shard-0.key"
tls_client_ca = "/etc/unirust/tls/clients-ca.crt"

[router]
listen = "0.0.0.0:50060"
shards = ["https://shard-0:50061", "https://shard-1:50061", "https://shard-2:50061"]
ontology = "/etc/unirust/ontology.json"
tls_cert = "/etc/unirust/tls/router.crt"
tls_key = "/etc/unirust/tls/router.key"
tls_client_ca = "/etc/unirust/tls/clients-ca.crt"
shard_tls_ca = "/etc/unirust/tls/shards-ca.crt"
shard_tls_cert = "/etc/unirust/tls/router-client.crt"
shard_tls_key = "/etc/unirust/tls/router-client.key"
checkpoint_interval_secs = 3600
```

For example, after saving an adapted file as `/etc/unirust/unirust.toml`, start
each service on its designated host with `unirust_shard --config
/etc/unirust/unirust.toml` or `unirust_router --config /etc/unirust/unirust.toml`.
The examples below assume the installed binaries are on `PATH`; source builds
place them in `target/release/`.

### Environment Variables

The full mapping is in [src/config/mod.rs](src/config/mod.rs). Common settings
include:

| Variable | Meaning |
|----------|---------|
| `UNIRUST_CONFIG` | Explicit TOML path |
| `UNIRUST_PROFILE` | Linker tuning profile |
| `UNIRUST_SHARD_LISTEN`, `UNIRUST_SHARD_ID` | Shard endpoint and logical ID |
| `UNIRUST_SHARD_DATA_DIR`, `UNIRUST_SHARD_BACKUP_DIR` | Data and checkpoint roots |
| `UNIRUST_SHARD_ONTOLOGY`, `UNIRUST_ROUTER_ONTOLOGY` | Matching-rule JSON paths |
| `UNIRUST_ROUTER_LISTEN`, `UNIRUST_ROUTER_SHARDS` | Router endpoint and comma-separated shard addresses |
| `UNIRUST_ROUTER_CHECKPOINT_INTERVAL_SECS` | Automatic checkpoint interval; `0` disables |
| `UNIRUST_ROUTER_SHARD_CONNECT_TIMEOUT_SECS` | Shard connection timeout; default 10 seconds |
| `UNIRUST_ROUTER_SHARD_REQUEST_TIMEOUT_SECS` | Per-shard RPC timeout; default 120 seconds |
| `UNIRUST_ROUTER_SHARD_TCP_KEEPALIVE_SECS` | TCP keepalive interval; default 30 seconds |
| `UNIRUST_SHARD_REPLICA`, `UNIRUST_SHARD_REPLICA_MODE` | Primary's replica endpoint, or passive mode |
| `UNIRUST_SHARD_REPLICATION_TOKEN_FILE` | Shared secret file for one replica pair |
| `UNIRUST_SHARD_REPLICA_REQUEST_TIMEOUT_SECS` | Replica RPC timeout, including pairing digest |

Server TLS settings map to `UNIRUST_SHARD_TLS_CERT`, `_TLS_KEY`, and
`_TLS_CLIENT_CA`, or the corresponding `UNIRUST_ROUTER_*` names. Outgoing shard
TLS uses `UNIRUST_ROUTER_SHARD_TLS_CA`, `_TLS_CERT`, and `_TLS_KEY`; outgoing
replica TLS uses `UNIRUST_SHARD_REPLICA_TLS_CA`, `_TLS_CERT`, and `_TLS_KEY`.
See each binary's `--help` for supported command-line options.

### Effective RocksDB Tuning

In v0.2.0, `[storage]` TOML fields and `UNIRUST_STORAGE_*` variables are parsed
by shared configuration but are **not applied when the shard opens RocksDB**.
`PersistentStore` instead reads these process environment variables directly:

| Variable | Actual default | Effect |
|----------|---------------:|--------|
| `UNIRUST_BLOCK_CACHE_MB` | 512 | Block cache size in MiB |
| `UNIRUST_WRITE_BUFFER_MB` | 128 | Write buffer size in MiB |
| `UNIRUST_MAX_WRITE_BUFFERS` | 4 | Maximum write buffers |
| `UNIRUST_COMPACTION_THREADS` | 1 | Background compaction threads |
| `UNIRUST_FLUSH_THREADS` | 2 | Background flush threads |
| `UNIRUST_RATE_LIMIT_MBPS` | 20 | RocksDB background I/O rate limit in MiB/s; `0` disables the limiter |

These are per-process settings, not a total cluster memory budget. See
[src/persistence.rs](src/persistence.rs) for the remaining storage knobs.

### Tuning Profiles

| Profile | Intended use |
|---------|--------------|
| `balanced` | General purpose; library default |
| `low-latency` | Lower latency tuning |
| `high-throughput` | Batch workloads; binary default |
| `bulk-ingest` | Initial loads with lower candidate caps |
| `memory-saver` | Smaller caches and candidate budgets |
| `billion-scale` | Disk-backed DSU/index with tighter memory settings |
| `billion-scale-high-performance` | Larger caches than `billion-scale` |

Profile names are tuning presets, not latency, capacity, or matching-completeness
guarantees. Entity resolution stays enabled in every profile. The recovery and
memory limits below still apply.

## API and Architecture

The exact gRPC contract is [proto/unirust.proto](proto/unirust.proto).
`RouterService` offers batch ingest, entity queries, conflict listing,
statistics/metrics, health, ontology configuration, reconciliation, coordinated
checkpoints, and administrative record export/import. `IngestRecordsFromUrl`
is declared but returns `UNIMPLEMENTED`. Import is not an online shard-movement
API. `Reset` is disabled by default on shards.

`ListConflicts` rebuilds an in-memory view from all exported records when its
cache is invalidated; it can be expensive after ingest. Router `GetStats` sums
local cluster counts, so `cluster_count` is not a count of unique reconciled
global entities. Use the semantic health RPC for readiness.

`ShardService` is internal. It additionally exposes source-identity reservations,
entity-fragment queries, boundary metadata, merge application, and streaming
ingest. The router has no streaming-ingest RPC: clients split ingest into bounded
`IngestRecords` batches. Router record export/import also have streaming forms;
each message remains subject to size limits.

Core library methods return:

| Method | Return type |
|--------|-------------|
| `engine.ingest(records)` | `anyhow::Result<IngestResult>` |
| `engine.query(&descriptors, interval)` | `anyhow::Result<QueryOutcome>` |
| `engine.clusters()` | `anyhow::Result<Clusters>` |
| `engine.graph()` | `anyhow::Result<KnowledgeGraph>` |
| `engine.checkpoint()` | `anyhow::Result<()>` |
| `engine.stats()` | `Stats` |

See [DESIGN.md](DESIGN.md) for matching, temporal guards, candidate indexes,
cross-shard reconciliation, persistence, and known implementation limits.

## Durability

Persistent shard ingest uses an application WAL and the RocksDB WAL:

1. The request is written to a versioned, checksummed binary ingest WAL. The
   file and its parent directory are synced before entity resolution begins.
2. Records, indexes, and cluster assignments are written to RocksDB, then its
   WAL is synced before acknowledgement. Derived linker state can be rebuilt
   from the records during recovery.
3. Only after that sync succeeds is the ingest WAL removed and the request
   acknowledged. Its directory is synced again after removal.

On restart, a remaining ingest WAL is replayed idempotently. A pending WAL is
never overwritten by a later request. A mutation failure that leaves pending
WAL or uncertain store state requires shard restart and recovery before traffic
resumes; a validation error rejected before mutation does not imply this state. A truncated or corrupt WAL
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

The current live protocol is **6**. Protocol 5 routers, shards, and replicas must
be stopped and upgraded together: older coordinators do not implement the
component guards or canonical entity queries required by this release. This
handshake change does not change WAL or checkpoint format version 1, record
snapshot encoding, or durable source-reservation format version 5. Existing
volumes, pending WAL batches, and coordinated checkpoints remain readable;
valid version 5 reservation markers retain their original shard-count binding
without another backfill. Internal ingest/import callers must send the current
live protocol, while public router clients continue leaving that field unset.

The persisted reservation directory is also bound to the configured shard count.
Router startup rejects shard-count changes because the current import API cannot
atomically move a record, update its reservation, and delete the old copy.
Cross-shard copy imports therefore fail closed; online scale-out and scale-in
require a future transactional relocation protocol or an offline rebuild.

Cross-shard redirects are durably applied to every shard and are idempotent. If
any shard fails, or the initiating request is cancelled, while a reconciliation
result is being applied, the router blocks ingest, entity queries, and readiness
while its global view may be partially updated. Recovery RPCs remain available.
Retrying `Reconcile` repairs the retained dirty keys in place. After a router or
full-cluster restart, router startup performs that repair before returning a
serviceable node and clears the dirty generation only after every shard
converges.

Changing ontology is allowed only on empty stores (reapplying the same rules
is idempotent). A cluster-wide replacement has fail-closed cancellation
semantics. Readiness stays failed after an ambiguous partial update until
`SetOntology` is retried with the intended configuration or every shard is
recovered offline to one configuration. Router startup also rejects mismatched
shard ontologies.

The shard reconstructs all derived linker state before opening its gRPC
listener. Recovery scans persisted records in ordered, bounded batches through
the normal entity-resolution path. It reads the full dataset and performs the
associated matching work; recovery has no fixed duration guarantee. Measure restart time at the intended dataset size and set
orchestration startup probes accordingly.

Global cluster IDs use durable record anchors instead of allocation-order
local cluster IDs, avoiding allocation-order redirect drift during replay. On the first startup of a database created before
this scheme marker existed, the shard atomically removes allocation-order
redirects that cannot be trusted after replay; router startup reconstructs them
from authoritative records before becoming ready. Upgrade every shard and the
router together for this transition rather than mixing versions.

`LinkerStateConfig` cache capacities are not enforced because the current LRU
backend has no durable spill/read-through path. Evicting cluster IDs, strong-ID
summaries, or record perspectives would change entity-resolution results.
This correctness-critical working set remains in memory even when the selected
profile uses a disk-backed DSU and identity index. The
`billion-scale` profile must not be treated as proof that a billion-record
deployment fits a given memory or recovery-time budget.

The destructive gRPC `Reset` method is disabled by default because a sequential
multi-shard reset cannot be atomic. Reset an entire deployment offline, with
all writers and shards stopped. The confirmed script action above is available
for script-managed local data. Test or isolated admin deployments can opt in
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
ontology, config version, live protocol, and replication token. Configure the
router with primary endpoints only; it rejects passive replicas. Bootstrap
both volumes from the same committed checkpoint, or start with two empty
volumes. Primary startup
compares SHA-256 digests over the explicit `DURABLE_STATE_COLUMN_FAMILIES`
list in `src/persistence.rs`, including records, reservations, interning, indexes,
and stored linker metadata. It rejects a mismatch in that covered state; this
is not a byte-for-byte comparison of database files or every dynamically created
column family. Pairing reads all covered key/value pairs, so measure startup
against the production dataset.
Set `UNIRUST_SHARD_REPLICA_REQUEST_TIMEOUT_SECS` above the measured worst-case
pairing digest time.

Generate a separate secret for each pair, store at least 32 random bytes in a
file readable only by the service account, and mount exactly the same file
content on both nodes. For example, `openssl rand -hex 32` produces a suitable
secret; create its destination with restrictive permissions. The token loader
hashes the complete file contents, including any trailing newline.

The following are deployment templates, requiring the named storage, ontology,
DNS hosts, and certificates to be provisioned. Start the passive replica first:

```bash
unirust_shard \
  --listen 0.0.0.0:50061 --shard-id 0 \
  --ontology /etc/unirust/ontology.json \
  --data-dir /var/lib/unirust/shard-0-replica \
  --backup-dir /var/backups/unirust/shard-0-replica \
  --replica-mode \
  --replication-token-file /etc/unirust/replication/shard-0.token \
  --tls-cert /etc/unirust/tls/shard-0-replica.crt \
  --tls-key /etc/unirust/tls/shard-0-replica.key \
  --tls-client-ca /etc/unirust/tls/primaries-ca.crt
```

Then start the primary, including its server credentials and its outgoing
replica credentials:

```bash
unirust_shard \
  --listen 0.0.0.0:50061 --shard-id 0 \
  --ontology /etc/unirust/ontology.json \
  --data-dir /var/lib/unirust/shard-0-primary \
  --backup-dir /var/backups/unirust/shard-0-primary \
  --tls-cert /etc/unirust/tls/shard-0-primary.crt \
  --tls-key /etc/unirust/tls/shard-0-primary.key \
  --tls-client-ca /etc/unirust/tls/routers-ca.crt \
  --replica https://shard-0-replica:50061 \
  --replication-token-file /etc/unirust/replication/shard-0.token \
  --replica-tls-ca /etc/unirust/tls/replicas-ca.crt \
  --replica-tls-cert /etc/unirust/tls/shard-0-primary.crt \
  --replica-tls-key /etc/unirust/tls/shard-0-primary.key
```

Replicated mutations are applied to the replica first, then locally, under one
per-pair serialization gate. The primary acknowledges only after both results
match. Replica errors or ambiguous results fail primary readiness and block ingest
and entity queries until operators reconcile the pair.
Replication therefore protects acknowledged writes from one volume loss, but
adds replica latency and requires both nodes to be available for writes.

Failover is manual because Unirust does not implement leader election or
quorum fencing:

1. Prove the old primary is stopped or isolated from clients and the replica.
2. Stop the passive process and restart its volume with replica mode disabled
   in CLI, environment, and TOML. Preserve its ontology and provision server TLS
   trust for router clients; the replica template above trusts primaries only.
3. Point the router at the promoted endpoint, configure trust for that server
   certificate, and restart the router.
4. Before adding a passive replica again, ensure both pair members have identical
   durable state and restore provenance. A checkpoint-based rebootstrap requires
   quiescing writes, creating a fresh consistent cluster checkpoint, stopping the
   cluster, and restoring every active shard and replica from that generation.
   Restore both members of each pair from the same per-shard checkpoint. Restoring
   only the old primary from a new generation fails provenance checks.

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

Trigger checkpoints through the router. Its mutation gate blocks operations
through that router for the duration of each checkpoint call; direct shard
writes or another coordinator bypass that gate. A supplied name is created
beneath every shard's configured backup root. From the repository root, using
`grpcurl` against a local plaintext router:

```bash
grpcurl -plaintext -import-path proto -proto unirust.proto \
  -d '{"path":"backup-2026-07-24T1300Z"}' \
  127.0.0.1:50060 unirust.RouterService/Checkpoint
```

The servers do not enable gRPC reflection, so supply the schema as shown. For
a secured router, replace `-plaintext` with `-cacert`, `-cert`, and `-key` options
pointing to your provisioned credentials.

Checkpoint creation uses a two-phase prepare/commit protocol. Every shard first
flushes its in-memory linker state and creates a RocksDB snapshot. Only after
all shards prepare successfully does the router write a binary commit marker to
every snapshot. The response includes the shared `generation` and
`committed: true`. A failed call can leave some snapshots prepared and some
committed. Retry the same name to complete missing prepare/commit steps, and
accept a backup only when every shard has a valid committed snapshot from the
same generation. Do not call the shard checkpoint RPC directly for a production
cluster backup.

**Retry limitation:** the router releases its mutation gate between failed
checkpoint calls. Already-prepared snapshots are reused, so writes between
attempts can make a completed generation span different points in time. Quiesce
application writes before checkpointing and keep them paused through retries
when a consistent cluster snapshot is required. If writes resumed after a
partial prepare, discard that generation as a consistency candidate and create
a fresh one while quiesced. Generation markers and checksums cannot establish
that no writes occurred between prepare attempts.

The router scheduler waits one configured interval before its first checkpoint.
If a shard fails during prepare or commit, the scheduler retains and retries the
same immutable generation instead of creating a stream of unrelated partial
snapshots. This scheduler does not keep writers quiesced between attempts.
Successful and failed generations are emitted to structured logs.
The container deployment enables hourly checkpoints by default; set
`UNIRUST_CHECKPOINT_INTERVAL_SECS` explicitly to choose another RPO or `0` to
disable them.

To recover from lost data volumes, stop the complete cluster and restore every
shard from the same checkpoint generation into an empty replacement directory:

```bash
unirust_shard \
  --listen 127.0.0.1:50061 --shard-id 0 \
  --data-dir /replacement/shard-0 \
  --backup-dir /var/backups/unirust/shard-0 \
  --restore-from /var/backups/unirust/shard-0/backup-2026-07-24T1300Z \
  --ontology /etc/unirust/ontology.json
```

Restore refuses a non-RocksDB source, symlinked sources or entries, a nonempty
destination, and an
existing partial staging directory. It also requires matching binary
prepare/commit markers, verifies the checkpoint belongs to the requested shard,
and opens both the source and staged copy read-only with RocksDB paranoid checks.
It copies and syncs into a sibling staging directory before publishing the
replacement with one rename. The restore command above uses loopback; apply
your normal endpoint and TLS settings when restoring each production shard.
Restore the whole cluster together; restoring only one older shard beside newer
peers can violate the cluster snapshot
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
symlinked content. To recover using an export, pass its `shard-0`, `shard-1`,
and `shard-2` directories to each corresponding shard's `--restore-from`. Export does not
delete the local source checkpoints.

Retention only removes generations after every entry in its root verifies:

```bash
unirust_backup prune --root /mnt/off-host/unirust --retain 14
```

The built-in scheduler creates coordinated source checkpoints; it does not
automatically run the export command. Schedule export and verification after
checkpoint completion, monitor both, and run periodic restore drills. Provide
independent destination storage and encryption at rest at the
infrastructure layer; the export tool does not configure either. Without a
synchronous replica, volume-loss recovery is limited to the last intact,
consistent cluster checkpoint on surviving storage, typically the last verified
off-host export. Acknowledged writes after that recovery point can be lost.
Process crashes and ordinary restarts remain covered
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
mTLS authenticates certificate holders; it does not provide per-user or
per-method authorization. Restrict administrative RPC access at a trusted
proxy/network boundary. The supplied Compose file is a local plaintext example
and binds its router port to loopback only.

Replication additionally uses a per-pair shared token. The shard binary rejects
plaintext replication by default; `--allow-insecure-replication` exists only
for isolated development. Protect token files as credentials, rotate them by
stopping and restarting both members, and use different tokens for every pair.

The shard binary requires a persistent `--data-dir` and a non-overlapping
`--backup-dir` unless the local-development allowance is set. The path checks
reject containment, including canonical-path overlap, but do not verify separate
filesystems or physical devices. Mount them from independent storage: separate
directory names alone do not protect against volume loss. An in-memory shard
can only be started
with the explicit `--ephemeral` flag and loses all records when the process
exits. `--allow-colocated-checkpoints` is a development-only escape hatch and
does not provide volume-loss recovery.

Router and shard servers cap each encoded or decoded gRPC message at 4 MiB,
limit each connection to 128 concurrent requests, and shed excess load. Use the
router's bounded batch ingest and chunked record import/export for larger
transfers; streaming does not remove the per-message limit. Bound connection
counts and request rates at the load balancer as well.

## Performance and Development

Measure the actual release, dataset, ontology, persistent storage, and topology
you intend to deploy. A record-throughput result is not evidence of query
latency, crash-recovery time, match completeness, or survival of physical volume
loss. This README does not claim a portable throughput or capacity guarantee.

A reproducible local load-test setup uses a fresh directory, five persistent
shards, 16 concurrent client streams, 5,000-record batches, and 10% generated
overlap. The script-managed backups are still local demo storage. With ports
50060–50065 available:

```bash
bash <<'SH'
set -e
cargo build --release --locked --bin unirust_loadtest --features test-support
perf_dir="$(mktemp -d "${TMPDIR:-/tmp}/unirust-perf.XXXXXX")"
export SHARDS=5
export DATA_DIR="$perf_dir/data" BACKUP_DIR="$perf_dir/backups"
export LOG_DIR="$perf_dir/logs" RUN_DIR="$perf_dir/run"
export ONTOLOGY="$PWD/examples/loadtest-ontology.json"
trap './scripts/cluster.sh stop' EXIT
./scripts/cluster.sh start
./target/release/unirust_loadtest \
  --router http://127.0.0.1:50060 --count 1000000 \
  --streams 16 --batch 5000 --overlap 0.1 --headless
printf 'Persistent performance data and logs: %s\n' "$perf_dir"
SH
```

Use the same settings for comparisons; report acknowledged records, errors,
hardware, and storage configuration alongside throughput. Run benchmark suites
explicitly; their duration depends on the machine:

```bash
cargo bench --locked --bench bench_quick
cargo bench --locked --bench bench_distributed
```

The CI correctness and packaging checks are:

```bash
cargo test --locked --all-features
cargo check --locked --all-targets --all-features
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
cargo package --locked
```

Integration tests use temporary persistent stores for distributed, restart,
WAL, replication, reconciliation, query, import, and backup regressions. Unit
tests may use in-memory stores. `cargo test --all-targets` can execute Criterion
benchmark binaries and is intentionally not the default correctness command.
Process-kill recovery tests do not simulate every hardware or filesystem failure.

## Container Deployment

[Containerfile](Containerfile) builds the binaries with native dependencies and
runs them as a non-root user. [compose.yaml](compose.yaml) supplies a local
three-shard plaintext cluster with the load-test ontology and separate named
data/checkpoint volumes. Those volumes can reside on the same host disk;
production still needs independent storage and authenticated transport.

With Podman and a compatible Compose provider installed, from the repository
root:

```bash
podman-compose up --build -d
podman-compose ps
podman-compose logs -f router
```

After the router's semantic healthcheck passes, run the optional client workload:

```bash
podman-compose run --rm loadtest
```

The Compose load-test service supplies its own router address and workload
arguments. It uses the `tools` profile, and explicit `run loadtest` selects that
service. The router port is published only on host loopback. Compose enables
hourly checkpoint attempts by default; set `UNIRUST_CHECKPOINT_INTERVAL_SECS`
before startup to select another interval or `0` to disable. This is a Compose
substitution variable, distinct from the binary's
`UNIRUST_ROUTER_CHECKPOINT_INTERVAL_SECS`. Automatic attempts have the retry
consistency limitation described above and do not export backups off-host.

Stop while preserving volumes:

```bash
podman-compose down
```

To deliberately delete **both shard data and checkpoint volumes**:

```bash
podman-compose down -v
```

## License

MIT. See [LICENSE](LICENSE).
