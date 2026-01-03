# Storage and Persistence (RocksDB)

## Overview

Unirust uses RocksDB for persistent storage with column families for different data types.
The persistence layer supports both simple restart-safe storage and billion-scale datasets.

## Column Families

| Column Family | Purpose |
|---------------|---------|
| `default` | General metadata |
| `records` | `record_id -> Record` (bincode) |
| `interner` | String interner state |
| `metadata` | `next_record_id`, ontology config |
| `dsu_parent` | DSU parent pointers |
| `dsu_rank` | DSU rank values |
| `dsu_guards` | Temporal merge guards |
| `index_hot` | Hot tier identity-key postings |
| `index_warm` | Warm tier identity-key postings |
| `index_cold` | Cold tier identity-key postings |
| `linker_cluster_ids` | Record to cluster ID mappings |
| `linker_global_ids` | Record to global cluster ID mappings |
| `linker_metadata` | Linker state (next_cluster_id, etc.) |

## Access Patterns

- **Ingest**: Append-heavy, per-record updates, batched writes via WriteBatch
- **Query**: Indexed lookups by attribute/value + temporal overlap
- **DSU operations**: find() uses cached parents with disk fallback, merge() batches writes
- **Index lookups**: Hot tier in memory, warm/cold read from disk with LRU promotion

## Persistent DSU

The DSU backend supports two modes:

1. **In-memory**: All parent/rank data in HashMap (default for small datasets)
2. **Persistent**: RocksDB-backed with configurable LRU cache

Configuration via `PersistentDSUConfig`:
```rust
PersistentDSUConfig {
    cache_capacity: 1_000_000,  // LRU cache size
    write_batch_size: 10_000,   // Batch writes
}
```

## Tiered Index

Identity-key index uses three tiers:

- **Hot**: In-memory LRU, fastest access
- **Warm**: On-disk, moderate access frequency
- **Cold**: On-disk, infrequent access

Configuration via `TierConfig`:
```rust
TierConfig {
    hot_capacity: 100_000,
    warm_capacity: 1_000_000,
    promotion_threshold: 3,  // Accesses before promotion
}
```

## Tuning Profiles

Storage behavior varies by tuning profile:

| Profile | DSU Backend | Index Tiers | Cache Sizes |
|---------|-------------|-------------|-------------|
| Balanced | In-memory | Hot only | Default |
| BillionScale | Persistent | Hot/Warm/Cold | Large |
| BillionScaleHighPerformance | Persistent | Hot/Warm/Cold | Very large |

## Environment Variables

RocksDB tuning via environment:

- `UNIRUST_BLOCK_CACHE_MB` (default 512)
- `UNIRUST_WRITE_BUFFER_MB` (default 128)
- `UNIRUST_MAX_WRITE_BUFFERS` (default 4)
- `UNIRUST_TARGET_FILE_MB` (default 128)
- `UNIRUST_LEVEL_BASE_MB` (default 512)
- `UNIRUST_BLOOM_BITS_PER_KEY` (default 10.0)
- `UNIRUST_RATE_LIMIT_MBPS` (default 0, disabled)

## Usage

```rust
use unirust_rs::{PersistentStore, Unirust, StreamingTuning, TuningProfile};

// Open persistent store
let store = PersistentStore::open("/var/lib/unirust/data")?;

// Use BillionScale tuning for large datasets
let tuning = StreamingTuning::from_profile(TuningProfile::BillionScale);
let mut unirust = Unirust::with_store_and_tuning(ontology, store, tuning);
```

## Operational Notes

- WAL is enabled by default for durability
- Periodic snapshots recommended for backup
- Monitor `running_compactions` and `running_flushes` via metrics
- Reserve 20% free disk space to avoid compaction stalls
