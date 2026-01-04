# Unirust Architecture & Design

This document describes the internal architecture, algorithms, and design decisions of Unirust. It serves as the authoritative reference for understanding how the system works.

## Table of Contents

1. [System Overview](#system-overview)
2. [Core Concepts](#core-concepts)
3. [Entity Resolution Algorithm](#entity-resolution-algorithm)
4. [Conflict Detection](#conflict-detection)
5. [Distributed Architecture](#distributed-architecture)
6. [Cross-Shard Reconciliation](#cross-shard-reconciliation)
7. [Storage Layer](#storage-layer)
8. [Performance Optimizations](#performance-optimizations)
9. [Data Flow](#data-flow)

---

## System Overview

Unirust is a temporal entity resolution engine that clusters records from multiple source systems into unified master entities. The system supports both single-node and distributed deployments.

```
                      ┌─────────────────┐
                      │     Clients     │
                      │   (gRPC/API)    │
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
  │  ┌─────────┐  │   │  ┌─────────┐  │   │  ┌─────────┐  │
  │  │ Linker  │  │   │  │ Linker  │  │   │  │ Linker  │  │
  │  │   DSU   │  │   │  │   DSU   │  │   │  │   DSU   │  │
  │  │  Index  │  │   │  │  Index  │  │   │  │  Index  │  │
  │  │ RocksDB │  │   │  │ RocksDB │  │   │  │ RocksDB │  │
  │  └─────────┘  │   │  └─────────┘  │   │  └─────────┘  │
  └───────────────┘   └───────────────┘   └───────────────┘
```

---

## Core Concepts

### Records

A **Record** represents a single observation from a source system:

```rust
struct Record {
    id: RecordId,           // Unique within shard
    identity: RecordIdentity,  // Entity type + perspective + UID
    descriptors: Vec<Descriptor>,  // Attribute-value pairs with time intervals
}

struct RecordIdentity {
    entity_type: String,    // e.g., "person", "company"
    perspective: String,    // Source system, e.g., "crm", "erp"
    uid: String,            // Source-unique identifier
}

struct Descriptor {
    attr: AttrId,           // Interned attribute name
    value: ValueId,         // Interned value
    interval: Interval,     // [start, end) validity period
}
```

### Ontology

The **Ontology** defines matching rules:

1. **Identity Keys**: Attribute combinations that identify the same entity
   ```rust
   // Records with matching (name, email) are considered the same entity
   IdentityKey::new(vec![name_attr, email_attr], "name_email")
   ```

2. **Strong Identifiers**: Attributes that cannot conflict within a cluster
   ```rust
   // SSN must be unique - conflicting SSNs block merges
   StrongIdentifier::new(ssn_attr, "ssn_unique")
   ```

3. **Constraints**: Validation rules
   ```rust
   Constraint::unique(email_attr, "unique_email")
   Constraint::unique_within_perspective(account_id, "source_unique")
   ```

### Temporal Model

All data has temporal validity. An **Interval** `[start, end)` defines when a descriptor is valid. Entity resolution respects these intervals:

- Records only merge if their identity key values match during **overlapping time periods**
- Conflicts are detected per-interval, not globally
- Golden records are computed for each unique time period

---

## Entity Resolution Algorithm

### Streaming Linker

The core algorithm processes records in four phases:

```
Phase 1 (Parallel): Extract identity key values from records
    │
    ▼
Phase 2 (Sequential): Query identity index for candidate clusters
    │
    ▼
Phase 3 (Sequential): Merge clusters via DSU with temporal guards
    │
    ▼
Phase 4 (Parallel): Finalize cluster assignments
```

#### Phase 1: Key Extraction

For each record, extract values for all identity keys defined in the ontology:

```rust
fn extract_key_values(record: &Record, ontology: &Ontology) -> Vec<KeyValue> {
    ontology.identity_keys()
        .iter()
        .filter_map(|key| {
            // Collect all attribute values for this identity key
            let values: Vec<_> = key.attributes()
                .iter()
                .filter_map(|attr| record.value_for(*attr))
                .collect();

            // Only complete keys (all attributes present) form valid key values
            if values.len() == key.attributes().len() {
                Some(KeyValue::new(key.name(), values))
            } else {
                None
            }
        })
        .collect()
}
```

#### Phase 2: Candidate Discovery

For each key value, query the identity index:

```rust
fn find_candidates(key_value: &KeyValue, index: &IdentityIndex) -> Vec<RecordId> {
    let signature = hash_key_value(key_value);
    index.get(&signature).unwrap_or_default()
}
```

The identity index maps key value signatures to record IDs. This is the hot path, optimized with:
- Bloom filters for fast negative lookups (16MB filter, <1% false positive rate)
- Sharded caching to avoid lock contention
- SIMD-accelerated hashing

#### Phase 3: Cluster Merging

The Disjoint Set Union (DSU) data structure tracks cluster membership. Merging follows these rules:

```rust
fn try_merge(dsu: &mut TemporalDSU, a: RecordId, b: RecordId,
             store: &Store, ontology: &Ontology) -> MergeResult {
    let root_a = dsu.find(a);
    let root_b = dsu.find(b);

    if root_a == root_b {
        return MergeResult::AlreadySame;
    }

    // Check temporal guards
    let guard = compute_temporal_guard(root_a, root_b, store, ontology);

    match guard {
        TemporalGuard::Allowed { reason, interval } => {
            dsu.union_with_guard(root_a, root_b, guard);
            MergeResult::Merged { interval }
        }
        TemporalGuard::Blocked { reason } => {
            MergeResult::Conflict { reason }
        }
    }
}
```

**Temporal Guards** validate merges:

1. **Overlapping Intervals**: Records must have overlapping validity periods
2. **Strong Identifier Agreement**: Strong identifiers must match during overlap
3. **Constraint Satisfaction**: Uniqueness constraints must be satisfied

#### Phase 4: Assignment Finalization

Each record receives its final cluster ID:

```rust
fn finalize_assignment(record_id: RecordId, dsu: &TemporalDSU) -> ClusterId {
    ClusterId(dsu.find(record_id).0)
}
```

### Adaptive Candidate Capping

To prevent pathological cases (e.g., very common names), candidate discovery uses adaptive capping:

| Candidate Count | Cap Applied |
|-----------------|-------------|
| < 2,000 | No cap |
| 2,000 - 10,000 | Cap at 1,000 |
| > 10,000 | Cap at 500 |
| > 50,000 | Early exit (hot key) |

Additionally, **stochastic sampling** maintains match quality: when candidates exceed the threshold, random sampling weighted by temporal overlap preserves expected accuracy.

---

## Conflict Detection

Conflicts occur when records in the same cluster have incompatible values for the same attribute during overlapping time periods.

### Detection Algorithms

Unirust implements two conflict detection algorithms with automatic selection:

#### 1. Sweep-Line Algorithm (O(n log n))

Best for clusters with diverse time boundaries.

```
Events: [(t1, START, r1), (t2, END, r1), (t3, START, r2), ...]
        sorted by time

Active set: records currently "open"

For each event:
  if START: add to active set, check conflicts with all active
  if END: remove from active set
```

#### 2. Atomic Intervals Algorithm (O(atoms × n))

Best for clusters with high overlap (many records share same intervals).

```
1. Collect all unique time boundaries: {t1, t2, t3, ...}
2. Create atomic intervals: [t1,t2), [t2,t3), [t3,t4), ...
3. For each atomic interval:
   - Find all records active during this interval
   - Group by attribute
   - If multiple values for same attribute → conflict
```

#### Auto-Selection Heuristic

```rust
fn select_algorithm(unique_boundaries: usize, total_descriptors: usize) -> Algorithm {
    let max_boundaries = total_descriptors * 2;  // Each descriptor has start + end
    let ratio = unique_boundaries as f64 / max_boundaries as f64;

    if ratio < 0.5 {
        // High overlap → atomic intervals is faster
        Algorithm::AtomicIntervals
    } else {
        // Low overlap → sweep line is faster
        Algorithm::SweepLine
    }
}
```

### Conflict Types

1. **Direct Conflict**: Same attribute has different values in overlapping intervals
   ```
   Record A: email = "john@foo.com" [100, 200)
   Record B: email = "john@bar.com" [150, 250)
   → Conflict in [150, 200)
   ```

2. **Indirect Conflict**: Strong identifier violation
   ```
   Record A: ssn = "123-45-6789" [100, 200)
   Record B: ssn = "987-65-4321" [150, 250)
   → Blocked merge (strong identifier conflict)
   ```

---

## Distributed Architecture

### Router

The router provides the external API and routes requests to shards:

```rust
impl Router {
    async fn ingest(&self, records: Vec<Record>) -> Vec<Assignment> {
        // Group records by target shard
        let mut shard_batches: HashMap<ShardId, Vec<Record>> = HashMap::new();

        for record in records {
            let shard_id = self.route(&record);
            shard_batches.entry(shard_id).or_default().push(record);
        }

        // Fan out to shards in parallel
        let futures: Vec<_> = shard_batches
            .into_iter()
            .map(|(shard_id, batch)| {
                self.shards[shard_id].ingest(batch)
            })
            .collect();

        // Collect results
        join_all(futures).await.into_iter().flatten().collect()
    }
}
```

### Routing Strategy

Records are routed by hashing their identity key values:

```rust
fn route(record: &Record, num_shards: usize) -> ShardId {
    let key_values = extract_key_values(record);
    let hash = hash_key_values(&key_values);
    ShardId(hash % num_shards as u64)
}
```

This ensures records that might need to merge are routed to the same shard.

### Global Cluster IDs

Each cluster has a globally unique ID:

```
┌──────────────────────────────────────────────────────────────┐
│                    GlobalClusterId (64 bits)                 │
├──────────────┬────────────────────┬──────────────────────────┤
│  shard_id    │      version       │       local_id           │
│  (16 bits)   │     (16 bits)      │      (32 bits)           │
└──────────────┴────────────────────┴──────────────────────────┘
```

- **shard_id**: Owning shard (0-65535)
- **version**: Merge version for conflict detection
- **local_id**: Cluster ID within the shard

---

## Cross-Shard Reconciliation

### The Problem

When records that should merge are routed to different shards, we need cross-shard reconciliation:

```
Shard 0                          Shard 1
┌────────────┐                   ┌────────────┐
│ Record A   │                   │ Record B   │
│ name=John  │   Should merge    │ name=John  │
│ email=j@x  │ ◄───────────────► │ email=j@x  │
│ Cluster 0  │                   │ Cluster 5  │
└────────────┘                   └────────────┘
```

### Boundary Tracking

Each shard maintains a **Cluster Boundary Index** tracking identity keys that appear:

```rust
struct ClusterBoundaryIndex {
    // Maps identity key signature → boundary entries
    boundaries: HashMap<IdentityKeySignature, Vec<BoundaryEntry>>,
    // Bloom filter for fast negative lookups
    bloom: BloomFilter,
    // Keys modified since last reconciliation
    dirty_keys: HashSet<IdentityKeySignature>,
}

struct BoundaryEntry {
    cluster_id: GlobalClusterId,
    interval: Interval,
    shard_id: ShardId,
}
```

### Reconciliation Algorithm

1. **Dirty Key Collection**: Each shard tracks keys modified since last reconciliation

2. **Boundary Exchange**: Router collects dirty boundaries from all shards

3. **Merge Detection**: For each key appearing on multiple shards:
   ```rust
   fn detect_cross_shard_merges(key: &IdentityKeySignature,
                                 entries: &[BoundaryEntry]) -> Vec<ClusterMerge> {
       let mut merges = Vec::new();

       // Group by overlapping intervals
       for (a, b) in entries.iter().tuple_combinations() {
           if a.shard_id != b.shard_id && a.interval.overlaps(&b.interval) {
               merges.push(ClusterMerge {
                   primary: a.cluster_id,
                   secondary: b.cluster_id,
               });
           }
       }

       merges
   }
   ```

4. **Merge Application**: Each shard applies merges to its local DSU:
   ```rust
   fn apply_cross_shard_merge(&mut self, primary: GlobalClusterId,
                               secondary: GlobalClusterId) -> usize {
       // Update all records in secondary cluster to point to primary
       let mut updated = 0;
       for record_id in self.records_in_cluster(secondary) {
           self.cluster_map.insert(record_id, primary);
           updated += 1;
       }
       updated
   }
   ```

5. **Key Clearing**: Successfully reconciled keys are removed from dirty set

### Consistency Guarantees

- **Eventual Consistency**: Cross-shard clusters converge after reconciliation
- **No Data Loss**: Failed reconciliation retries on next cycle
- **Conflict Preservation**: Cross-shard conflicts are detected and reported

---

## Storage Layer

### In-Memory Store

For testing and small datasets:

```rust
struct Store {
    records: FxHashMap<RecordId, Record>,
    by_entity_type: FxHashMap<String, Vec<RecordId>>,
    attr_interner: StringInterner,
    value_interner: StringInterner,
}
```

### Persistent Store (RocksDB)

Production storage with column families:

| Column Family | Key | Value | Purpose |
|---------------|-----|-------|---------|
| `records` | RecordId (4B) | Record (bincode) | Record storage |
| `index_identity` | hash (8B) | RecordId list | Identity key index |
| `index_attr_value` | attr:value | RecordId list | Attribute lookup |
| `dsu_parent` | RecordId | Parent RecordId | DSU parent links |
| `dsu_rank` | RecordId | u32 | DSU rank for balancing |
| `dsu_guards` | (RecordId, RecordId) | TemporalGuard | Merge guards |
| `cluster_assignments` | RecordId | ClusterId | Cluster membership |
| `interner` | String | InternedId | String interning |
| `metadata` | key | value | Manifest, counters |

### Tuning Parameters

```toml
[storage]
block_cache_mb = 512          # Read cache
write_buffer_mb = 128         # Write buffer before flush
max_background_jobs = 4       # Compaction threads
rate_limit_mbps = 0           # I/O rate limiting (0 = unlimited)
```

---

## Performance Optimizations

### Lock-Free Structures

1. **Atomic DSU**: Lock-free parent updates using CAS operations
   ```rust
   fn find(&self, x: RecordId) -> RecordId {
       let mut current = x;
       loop {
           let parent = self.parent[current].load(Ordering::Acquire);
           if parent == current {
               return current;
           }
           // Path compression with CAS
           let grandparent = self.parent[parent].load(Ordering::Acquire);
           let _ = self.parent[current].compare_exchange(
               parent, grandparent, Ordering::Release, Ordering::Relaxed
           );
           current = parent;
       }
   }
   ```

2. **Sharded Caching**: 256 shards to minimize contention
   ```rust
   fn get_shard(&self, key: &K) -> &RwLock<LruCache<K, V>> {
       let hash = hash(key);
       &self.shards[hash as usize % 256]
   }
   ```

### SIMD Hashing

Identity key hashing uses SIMD for throughput:

```rust
fn simd_hash(data: &[u8]) -> u64 {
    // Process 32 bytes at a time using AVX2
    let mut state = _mm256_set1_epi64x(SEED);
    for chunk in data.chunks_exact(32) {
        let block = _mm256_loadu_si256(chunk.as_ptr() as *const _);
        state = _mm256_xor_si256(state, block);
        state = _mm256_mul_epi32(state, MULTIPLIER);
    }
    // Horizontal reduction
    reduce_256_to_64(state)
}
```

### Async WAL

Write-ahead logging with coalescing:

```rust
struct AsyncWal {
    tx: Sender<WalEntry>,      // Submit writes
    writer_thread: JoinHandle, // Background writer
}

impl AsyncWal {
    fn submit(&self, data: Vec<u8>) -> WalTicket {
        let ticket = WalTicket::new();
        self.tx.send(WalEntry { data, ticket: ticket.clone() });
        ticket  // Caller can wait on ticket
    }
}

// Writer thread coalesces multiple writes into single fsync
fn wal_writer_loop(rx: Receiver<WalEntry>, config: WalConfig) {
    let mut buffer = Vec::new();
    loop {
        match rx.recv_timeout(config.max_coalesce_delay) {
            Ok(entry) => buffer.push(entry),
            Err(Timeout) => {
                if !buffer.is_empty() {
                    flush_buffer(&mut buffer);  // Single fsync
                }
            }
        }
        if buffer.len() >= config.max_coalesce_records {
            flush_buffer(&mut buffer);
        }
    }
}
```

### Partitioned Processing

For maximum throughput, records are partitioned for parallel processing:

```
                    ┌──────────────────────┐
                    │   Incoming Records   │
                    └──────────┬───────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
   ┌──────▼──────┐     ┌──────▼──────┐     ┌──────▼──────┐
   │ Partition 0 │     │ Partition 1 │     │ Partition N │
   │ Local DSU   │     │ Local DSU   │     │ Local DSU   │
   │ Local Index │     │ Local Index │     │ Local Index │
   └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
          │                    │                    │
          └────────────────────┼────────────────────┘
                               │
                    ┌──────────▼───────────┐
                    │ Cross-Partition      │
                    │ Merge Queue          │
                    └──────────────────────┘
```

---

## Data Flow

### Ingest Path

```
1. Client sends RecordInput via gRPC
2. Router hashes identity key → target shard
3. Shard receives record batch
4. Store stages records (dedup check by identity)
5. For each record:
   a. Extract identity key values
   b. Query identity index for candidates
   c. For each candidate: attempt DSU merge with temporal guards
   d. Update identity index with new record
6. Flush staged records to RocksDB
7. Update boundary index for cross-shard tracking
8. Return cluster assignments
```

### Query Path

```
1. Client sends QueryEntitiesRequest
2. Router fans out to all shards (parallel)
3. Each shard:
   a. Looks up descriptors in attribute index
   b. Filters by temporal interval
   c. Resolves cluster IDs via DSU
   d. Computes golden records (conflict-free values)
4. Router aggregates results
5. Returns QueryOutcome:
   - If single cluster matches: QueryMatches
   - If multiple clusters claim same identity: QueryConflict
```

### Reconciliation Cycle

```
1. Router triggers reconciliation (periodic or on-demand)
2. Collect dirty boundary keys from all shards
3. For each key appearing on multiple shards:
   a. Check for overlapping intervals
   b. If overlap found: create merge candidates
4. Validate merges against temporal guards
5. Apply merges to each shard
6. Clear dirty keys
7. Report reconciliation stats
```

---

## Appendix: Tuning Profiles

| Profile | Candidate Cap | Hot Key Threshold | Use Case |
|---------|---------------|-------------------|----------|
| Balanced | 2,000 | 50,000 | General purpose |
| LowLatency | 1,000 | 20,000 | Fast responses |
| HighThroughput | 4,000 | 100,000 | Batch processing |
| BulkIngest | 500 | 10,000 | Maximum speed |
| MemorySaver | 500 | 5,000 | Reduced memory |
| BillionScale | 2,000 + persistent DSU | 100,000 | Huge datasets |

---

## Appendix: Performance Characteristics

### Throughput (5-shard cluster)

| Workload | Records/sec |
|----------|-------------|
| Pure ingest, no conflicts | ~500K |
| 10% overlap probability | ~400K |
| 50% overlap probability | ~250K |
| With reconciliation enabled | ~350K |

### Memory Usage

| Component | Memory per Million Records |
|-----------|---------------------------|
| In-memory DSU | ~12 MB |
| Persistent DSU | ~4 MB (cached) |
| Identity Index | ~50 MB |
| Record Storage | ~100 MB (compressed) |

### Latency

| Operation | P50 | P99 |
|-----------|-----|-----|
| Single record ingest | 0.5ms | 2ms |
| Batch ingest (1000 records) | 10ms | 50ms |
| Point query | 0.2ms | 1ms |
| Range query (1000 results) | 5ms | 20ms |
