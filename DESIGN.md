# Unirust Architecture

## System Overview

Unirust is a temporal entity resolution engine that clusters records from multiple source systems into unified master entities. The system operates in distributed mode with a router coordinating multiple shards for horizontal scalability.

```
                    ┌─────────────────┐
                    │   Clients       │
                    │   (gRPC)        │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │     Router      │
                    │  (hash-based    │
                    │   routing)      │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼───────┐   ┌───────▼───────┐   ┌───────▼───────┐
│   Shard 0     │   │   Shard 1     │   │   Shard N     │
│ (RocksDB)     │   │ (RocksDB)     │   │ (RocksDB)     │
└───────────────┘   └───────────────┘   └───────────────┘
```

## Core Components

### 1. Store (`store.rs`, `persistence.rs`)

The store manages record storage with two implementations:

**In-Memory Store**: For unit testing and small datasets
- Fast but non-durable
- Uses FxHashMap for O(1) lookups

**Persistent Store**: Production use with RocksDB
- Durable storage with column families:
  - `records`: Serialized Record objects
  - `metadata`: Manifest, counts, configuration
  - `interner`: String interning for attributes/values
  - `index_*`: Various indexes for fast lookup
  - `dsu_*`: DSU state for cluster management

### 2. Streaming Linker (`linker.rs`)

The core entity resolution engine using a four-phase approach:

```
Phase 1 (Parallel): Extract identity key values from records
Phase 2 (Sequential): Query identity index for candidates
Phase 3 (Sequential): Merge clusters via DSU with temporal guards
Phase 4 (Parallel): Finalize cluster assignments
```

**Key optimizations**:
- Parallel processing for batches >= 100 records
- Hot key optimization (exits early if >50K candidates)
- SmallVec for inline candidate storage (avoids heap for <32 candidates)
- Adaptive candidate caps based on workload

### 3. DSU - Disjoint Set Union (`dsu.rs`)

Two implementations for different scale requirements:

**TemporalDSU** (in-memory):
- Parent/rank arrays with path compression
- Root cache (16K entries) for recently found roots
- Used for datasets up to millions of records

**PersistentTemporalDSU** (RocksDB-backed):
- For billion-scale datasets
- LRU cache for hot entries
- Lazy parent/rank lookups from DB

**Temporal Guards**:
- Validate merge legality based on time intervals
- Store reason for allowed merges
- Track conflicts when merges are blocked

### 4. Ontology (`ontology.rs`)

Defines entity matching rules:

**Identity Keys**: Attributes that must match for same-entity determination
```rust
IdentityKey::new(vec![name_attr, email_attr], "name_email")
```

**Strong Identifiers**: Attributes that cannot conflict within a cluster
```rust
StrongIdentifier::new(ssn_attr, "ssn_unique")
```

**Constraints**: Uniqueness rules
```rust
Constraint::unique(email_attr, "unique_email")
Constraint::unique_within_perspective(id_attr, "source_unique")
```

### 5. Conflict Detection (`conflicts.rs`)

Two-phase algorithm with auto-selection:

**Sweep-Line Algorithm** (O(n log n)):
- Best for diverse time boundaries
- Processes interval events in sorted order

**Atomic Intervals Algorithm** (O(atoms × n)):
- Best for high overlap scenarios
- Groups intervals by overlapping regions

**Auto-selection heuristic**: Uses sweep-line if >50% unique boundaries, otherwise atomic intervals.

### 6. Distributed Layer (`distributed.rs`, `sharding.rs`)

**Router**:
- Hash-based routing using identity key values
- Single-shard fast path for single-shard deployments
- Parallel record building for batches >500 records

**Shards**:
- Independent unirust instances with local RocksDB
- Boundary tracking for cross-shard reconciliation
- Global cluster IDs: `(shard_id << 48) | (version << 32) | local_id`

**Cross-Shard Merging**:
- Boundary Index tracks identity keys crossing shard boundaries
- Bloom filters (16MB) for fast negative lookups
- Incremental reconciliation without full rebuild

### 7. Partitioned Processing (`partitioned.rs`)

For maximum ingest throughput:

```
Records partitioned by hash(identity_key)
    |
    v
32 parallel partitions (no locks)
    |
    v
Each partition: local DSU + linker
    |
    v
Cross-partition merge queue
```

## Data Flow

### Ingest Path

```
1. Client sends RecordInput via gRPC
2. Router hashes identity key, routes to shard
3. Shard builds Record from proto
4. Store stages record (dedup check)
5. Linker extracts identity key values
6. Identity index queried for candidates
7. DSU merge with temporal guards
8. Cluster assignment returned
```

### Query Path

```
1. Client sends QueryEntitiesRequest
2. Router fans out to all shards
3. Each shard:
   a. Looks up descriptors in index
   b. Filters by temporal interval
   c. Returns matching clusters
4. Router aggregates results
5. Returns golden records (conflict-free)
```

## Persistence Schema

### Column Families

| CF Name | Key | Value |
|---------|-----|-------|
| `records` | RecordId (4 bytes) | Record (bincode) |
| `index_identity` | identity_hash | RecordId list |
| `index_attr_value` | attr_id:value_id | RecordId list |
| `dsu_parent` | RecordId | Parent RecordId |
| `dsu_rank` | RecordId | Rank (u32) |
| `dsu_guards` | RecordId pair | TemporalGuard |
| `cluster_assignments` | RecordId | ClusterId |
| `interner` | string | InternedId |

### Write-Ahead Log

- Async WAL with write coalescing
- Binary format (protobuf serialization)
- Configurable write buffering (default 128MB)

## Performance Characteristics

### Throughput

| Configuration | Records/sec |
|--------------|-------------|
| Single shard, in-memory | ~300K |
| Single shard, persistent | ~150K |
| 5 shards, persistent | ~550K |

### Memory Usage

- Base: ~50MB per shard
- Per million records: ~200MB (with indexes)
- DSU: ~12 bytes per record (in-memory), ~4 bytes (persistent)

### Tuning Profiles

| Profile | Candidate Cap | Use Case |
|---------|--------------|----------|
| Balanced | 2000 | General purpose |
| LowLatency | 1000 | Fast responses |
| HighThroughput | 4000 | Batch processing |
| BulkIngest | 10000 | Maximum speed |
| BillionScale | 2000 + persistent DSU | Huge datasets |

## Module Dependency Graph

```
lib.rs (Unirust API)
├── linker.rs (StreamingLinker)
│   ├── dsu.rs (TemporalDSU)
│   └── index.rs (TieredIndex)
├── store.rs (Store)
│   └── persistence.rs (PersistentStore)
├── ontology.rs (Ontology)
├── conflicts.rs (ConflictDetector)
└── graph.rs (KnowledgeGraph)

distributed.rs (gRPC services)
├── sharding.rs (BoundaryIndex)
└── partitioned.rs (ParallelPartitionedUnirust)
```

## Error Handling

All operations return `Result<T, UniError>` where `UniError` covers:
- `StoreError`: Storage operations
- `LinkError`: Entity resolution failures
- `OntologyError`: Invalid ontology configuration
- `TemporalError`: Invalid intervals

## Concurrency Model

- **Read operations**: Lock-free where possible
- **Write operations**: Per-partition locking (32 partitions)
- **DSU operations**: Atomic compare-and-swap for parent updates
- **Index updates**: Batch writes with single DB transaction
