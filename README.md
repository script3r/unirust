# Unirust

<div align="center">
  <img src="unirust.png" alt="Unirust Logo" width="350" height="350">
</div>

A high-performance temporal entity resolution engine in Rust.

## Overview

Unirust is a distributed entity resolution system that clusters records from multiple sources into unified entities while respecting temporal constraints and detecting conflicts. It is designed for production workloads requiring:

- **Correctness**: Temporal guards ensure merges only happen when constraints allow
- **Scale**: Persistent DSU and tiered indexing support billions of records
- **Performance**: Lock-free atomics, SIMD hashing, adaptive candidate selection
- **Distribution**: Router and multi-shard architecture with cross-shard reconciliation

## Core Concepts

### Entity Resolution Pipeline

```
Records (from multiple sources)
    |
    v
Store (in-memory or RocksDB)
    |
    v
Streaming Linker
  - Phase 1: Extract identity key values (parallel)
  - Phase 2: Find candidates in identity index
  - Phase 3: Merge clusters via DSU with temporal guards
  - Phase 4: Finalize assignments (parallel)
    |
    v
Cluster Assignments (RecordId -> ClusterId)
    |
    v
Conflict Detection -> Observations -> Knowledge Graph
```

### Ontology

Define entity matching rules:

- **Identity Keys**: Attributes that must match for records to be considered the same entity (e.g., name+email)
- **Strong Identifiers**: Attributes that cannot conflict within a cluster (e.g., SSN)
- **Constraints**: Uniqueness rules that prevent invalid merges

### Temporal Model

All data has temporal validity intervals `[start, end)`. Entity resolution respects these intervals, only merging records when their values agree during overlapping time periods.

## Quick Start

### Distributed Mode (Recommended)

Start a 5-shard cluster:

```bash
# Terminal 1: Start router
./target/release/unirust_router --port 50060 --shards 5

# Terminal 2-6: Start shards
./target/release/unirust_shard --port 50061 --shard-id 0 --data-dir /tmp/shard0
./target/release/unirust_shard --port 50062 --shard-id 1 --data-dir /tmp/shard1
./target/release/unirust_shard --port 50063 --shard-id 2 --data-dir /tmp/shard2
./target/release/unirust_shard --port 50064 --shard-id 3 --data-dir /tmp/shard3
./target/release/unirust_shard --port 50065 --shard-id 4 --data-dir /tmp/shard4
```

Ingest records via gRPC:

```bash
./target/release/unirust_client --router http://127.0.0.1:50060 --ontology ontology.json
```

### Load Testing

```bash
./target/release/unirust_loadtest \
  -r http://127.0.0.1:50060 \
  -c 10000000 \
  --streams 16 \
  --batch 5000
```

### Library Usage

```rust
use unirust_rs::{Unirust, PersistentStore, StreamingTuning, TuningProfile};
use unirust_rs::ontology::{Ontology, IdentityKey, StrongIdentifier, Constraint};

// Create ontology
let mut ontology = Ontology::new();
ontology.add_identity_key(IdentityKey::new(vec![name_attr, email_attr], "name_email".to_string()));
ontology.add_strong_identifier(StrongIdentifier::new(ssn_attr, "ssn".to_string()));
ontology.add_constraint(Constraint::unique(email_attr, "unique_email".to_string()));

// Open persistent store
let store = PersistentStore::open("/path/to/data")?;

// Create unirust instance with tuning
let tuning = StreamingTuning::from_profile(TuningProfile::HighThroughput);
let mut unirust = Unirust::with_store_and_tuning(ontology, store, tuning);

// Stream records
let results = unirust.stream_records(records)?;

// Query entities
let matches = unirust.query_master_entities(&descriptors, interval)?;
```

## Tuning Profiles

| Profile | Use Case |
|---------|----------|
| `Balanced` | General purpose (default) |
| `LowLatency` | Fast response times |
| `HighThroughput` | Bulk processing |
| `BulkIngest` | Maximum ingest speed |
| `BillionScale` | Persistent DSU for billion-entity datasets |
| `MemorySaver` | Reduced memory footprint |

## Architecture

See [DESIGN.md](DESIGN.md) for detailed architecture documentation.

## Development

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench --bench bench_quick   # Fast CI feedback (~30s)
cargo bench --bench bench_micro   # Component benchmarks

# Format and lint
cargo fmt
cargo clippy --all-targets
```

## License

MIT. See `LICENSE`.
