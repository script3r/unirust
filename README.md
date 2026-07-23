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
git clone https://github.com/unirust/unirust.git
cd unirust
cargo build --release
```

### Single-Shard Mode (Development)

```bash
# Start a single shard
./target/release/unirust_shard --listen 127.0.0.1:50061 --shard-id 0

# In another terminal, start the router
./target/release/unirust_router --listen 127.0.0.1:50060 --shards 127.0.0.1:50061
```

### Multi-Shard Cluster (Production)

```bash
# Use the cluster script
SHARDS=5 ONTOLOGY=/etc/unirust/ontology.json ./scripts/cluster.sh start

# Restart the same persistent cluster without deleting records
SHARDS=5 ONTOLOGY=/etc/unirust/ontology.json ./scripts/cluster.sh restart

# Destructive reset is separate and requires explicit confirmation
UNIRUST_CONFIRM_RESET=1 ./scripts/cluster.sh reset

# Or start manually:
./target/release/unirust_shard --listen 127.0.0.1:50061 --shard-id 0 --data-dir /data/shard0
./target/release/unirust_shard --listen 127.0.0.1:50062 --shard-id 1 --data-dir /data/shard1
./target/release/unirust_shard --listen 127.0.0.1:50063 --shard-id 2 --data-dir /data/shard2
./target/release/unirust_shard --listen 127.0.0.1:50064 --shard-id 3 --data-dir /data/shard3
./target/release/unirust_shard --listen 127.0.0.1:50065 --shard-id 4 --data-dir /data/shard4

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

[router]
listen = "0.0.0.0:50060"
shards = ["shard-0:50061", "shard-1:50061", "shard-2:50061"]

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
| `UNIRUST_ROUTER_SHARDS` | Comma-separated shard addresses |

### Tuning Profiles

| Profile | Use Case |
|---------|----------|
| `balanced` | General purpose (default for library) |
| `low-latency` | Interactive queries, fast responses |
| `high-throughput` | Batch processing (default for binaries) |
| `bulk-ingest` | Large initial loads with lower candidate caps; entity resolution remains enabled |
| `memory-saver` | Constrained environments |
| `billion-scale` | Persistent DSU for huge datasets |

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

On restart, a remaining ingest WAL is replayed idempotently. A truncated or
corrupt WAL is preserved with a `.corrupt.*` suffix and shard startup fails with
a data-loss error instead of accepting traffic with an unknown recovery gap.
Cross-shard merge redirects are also persisted before acknowledgement and
reloaded when the streaming linker is reconstructed.

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

The shard and router binaries handle SIGINT/SIGTERM with graceful gRPC shutdown;
acknowledged ingests are additionally covered by an executable-level process
kill and restart integration test.

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
