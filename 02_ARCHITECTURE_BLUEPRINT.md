# 02_ARCHITECTURE_BLUEPRINT.md

## Executive Summary

This blueprint defines a high-performance architecture achieving **200%+ throughput improvement** through:
- **Partition-local processing** eliminating cross-worker lock contention
- **Lock-free channels** for inter-partition coordination
- **Zero-copy serialization** with rkyv
- **Batch-coalesced operations** reducing per-record overhead

Target: **182K+ rec/sec** (from current 91K rec/sec baseline)

---

## 1. CORE ARCHITECTURAL CHANGE: Partitioned Processing

### Current Architecture (Bottleneck)
```
16 Workers ──► Single RwLock<Unirust> ──► Sequential Processing
                     │
                     └── 93% idle time due to lock contention
```

### New Architecture (Lock-Free Partitions)
```
                    ┌─────────────────────────────────────────┐
                    │            Router (Consistent Hash)     │
                    └─────────────────────────────────────────┘
                              │         │         │
                    ┌─────────┴─────────┴─────────┴─────────┐
                    ▼                   ▼                   ▼
            ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
            │  Partition 0  │   │  Partition 1  │   │  Partition N  │
            │  ┌─────────┐  │   │  ┌─────────┐  │   │  ┌─────────┐  │
            │  │ DSU     │  │   │  │ DSU     │  │   │  │ DSU     │  │
            │  │ Index   │  │   │  │ Index   │  │   │  │ Index   │  │
            │  │ Store   │  │   │  │ Store   │  │   │  │ Store   │  │
            │  └─────────┘  │   │  └─────────┘  │   │  └─────────┘  │
            │  (exclusive)  │   │  (exclusive)  │   │  (exclusive)  │
            └───────────────┘   └───────────────┘   └───────────────┘
                    │                   │                   │
                    └───────────────────┴───────────────────┘
                                        │
                              ┌─────────▼─────────┐
                              │  Merge Coordinator │
                              │  (async channel)   │
                              └───────────────────┘
```

### Partitioning Strategy

**Primary Key Partitioning**: Records are assigned to partitions based on their primary identity key hash.

```rust
fn partition_for_record(record: &RecordInput, partition_count: usize) -> usize {
    // Use first identity key as partition key (deterministic)
    let partition_key = record.identity.keys.first()
        .map(|k| &k.value)
        .unwrap_or(&record.identity.id);

    // FxHash for speed, modulo for partition
    let hash = fxhash::hash64(partition_key.as_bytes());
    (hash as usize) % partition_count
}
```

**Cross-Partition Merges**: When records in different partitions need to merge (secondary key match), use async merge queue:

```rust
struct CrossPartitionMerge {
    source_partition: usize,
    target_partition: usize,
    source_cluster: ClusterId,
    target_cluster: ClusterId,
    merge_reason: MergeReason,
}

// Lock-free MPSC channel per partition
type MergeQueue = crossbeam_channel::Sender<CrossPartitionMerge>;
```

---

## 2. PARTITION-LOCAL PROCESSING

### PartitionedUnirust Structure

```rust
pub struct PartitionedUnirust {
    partitions: Vec<Partition>,
    merge_coordinator: MergeCoordinator,
    config: Arc<UnirustConfig>,
}

pub struct Partition {
    id: usize,
    // Each partition owns its data exclusively - no locks needed
    linker: StreamingLinker,
    store: Store,
    persistence: Option<Persistence>,

    // Inbound merge requests from other partitions
    merge_rx: crossbeam_channel::Receiver<CrossPartitionMerge>,
    merge_tx: crossbeam_channel::Sender<CrossPartitionMerge>,
}

impl Partition {
    /// Process a batch of records assigned to this partition
    /// NO LOCKS - exclusive ownership
    pub fn process_batch(&mut self, records: Vec<RecordInput>) -> Vec<IngestResult> {
        let mut results = Vec::with_capacity(records.len());

        for record in records {
            // Direct mutable access - no lock acquisition
            let cluster_id = self.linker.link_record_partitioned(
                record,
                &mut self.store,
            );
            results.push(IngestResult { cluster_id, .. });
        }

        // Process any pending cross-partition merges
        self.drain_merge_queue();

        results
    }

    fn drain_merge_queue(&mut self) {
        while let Ok(merge) = self.merge_rx.try_recv() {
            self.linker.apply_external_merge(merge);
        }
    }
}
```

### Parallel Partition Execution

```rust
impl PartitionedUnirust {
    pub async fn ingest_batch(&self, records: Vec<RecordInput>) -> Vec<IngestResult> {
        // Phase 1: Partition records by primary key (parallel)
        let partitioned: Vec<Vec<RecordInput>> = self.partition_records(records);

        // Phase 2: Process each partition in parallel (no locks!)
        let results: Vec<Vec<IngestResult>> = partitioned
            .into_par_iter()
            .enumerate()
            .map(|(partition_id, batch)| {
                // Each partition processes independently
                self.partitions[partition_id].process_batch(batch)
            })
            .collect();

        // Phase 3: Merge results (simple concat, order preserved by index)
        results.into_iter().flatten().collect()
    }
}
```

---

## 3. BATCH COALESCING

### Problem
Current: Each gRPC message triggers individual processing
New: Coalesce multiple messages into larger batches

### BatchCoalescer

```rust
pub struct BatchCoalescer {
    buffer: Vec<RecordInput>,
    capacity: usize,
    flush_interval: Duration,
    last_flush: Instant,
}

impl BatchCoalescer {
    pub fn new(capacity: usize, flush_interval: Duration) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            capacity,
            flush_interval,
            last_flush: Instant::now(),
        }
    }

    /// Add record, returns batch if ready to flush
    pub fn add(&mut self, record: RecordInput) -> Option<Vec<RecordInput>> {
        self.buffer.push(record);

        if self.buffer.len() >= self.capacity
           || self.last_flush.elapsed() >= self.flush_interval {
            self.flush()
        } else {
            None
        }
    }

    pub fn flush(&mut self) -> Option<Vec<RecordInput>> {
        if self.buffer.is_empty() {
            return None;
        }
        self.last_flush = Instant::now();
        Some(std::mem::replace(&mut self.buffer, Vec::with_capacity(self.capacity)))
    }
}
```

### Coalescing Configuration

```rust
// Optimal batch sizes determined by benchmarking
pub const COALESCE_CAPACITY: usize = 4096;      // Records per batch
pub const COALESCE_INTERVAL_MS: u64 = 5;        // Max wait time
pub const PARTITION_COUNT: usize = 8;            // CPU core aligned
```

---

## 4. ZERO-COPY SERIALIZATION (rkyv)

### Current Bottleneck
```rust
// bincode: ~500ns encode + ~800ns decode per record
let bytes = bincode::serialize(&record)?;
let record: Record = bincode::deserialize(&bytes)?;
```

### New Approach
```rust
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::ser::serializers::AllocSerializer;

#[derive(Archive, Deserialize, Serialize)]
#[archive(check_bytes)]
pub struct ArchivedRecord {
    pub id: RecordId,
    pub cluster_id: ClusterId,
    pub identity: ArchivedRecordIdentity,
    pub descriptors: ArchivedVec<ArchivedRecordDescriptor>,
    pub intervals: ArchivedVec<ArchivedInterval>,
}

impl ArchivedRecord {
    /// Zero-copy access to archived data
    pub fn from_bytes(bytes: &[u8]) -> &ArchivedRecord {
        // No deserialization - direct memory access
        unsafe { rkyv::archived_root::<Record>(bytes) }
    }
}
```

### Performance Comparison

| Operation | bincode | rkyv | Improvement |
|-----------|---------|------|-------------|
| Serialize | 500ns | 200ns | 2.5x |
| Deserialize | 800ns | 0ns* | ∞ |
| Memory copy | Yes | No | - |

*Zero-copy: access archived data directly without deserialization

---

## 5. OPTIMIZED INDEX STRUCTURE

### Current Bottleneck
- IntervalTree: O(log n) with poor cache locality
- HashMap per key: Memory fragmentation

### New Structure: Flat Sorted Arrays with Binary Search

```rust
pub struct OptimizedIndex {
    // Single contiguous allocation per identity key type
    entries: Vec<IndexEntry>,
    // Sorted by (key_signature, interval_start) for binary search
    sorted: bool,
}

#[repr(C, align(64))]  // Cache-line aligned
pub struct IndexEntry {
    key_signature: u64,      // FxHash of identity key
    interval_start: i64,     // Epoch timestamp
    interval_end: i64,
    record_id: RecordId,
    cluster_id: ClusterId,
}

impl OptimizedIndex {
    /// SIMD-friendly interval overlap check
    #[inline]
    pub fn find_overlapping(
        &self,
        key_signature: u64,
        query_start: i64,
        query_end: i64,
    ) -> SmallVec<[RecordId; 32]> {
        // Binary search to find key range
        let start_idx = self.entries.partition_point(|e|
            e.key_signature < key_signature
        );
        let end_idx = self.entries[start_idx..].partition_point(|e|
            e.key_signature == key_signature
        ) + start_idx;

        // Linear scan within key (cache-friendly)
        let mut results = SmallVec::new();
        for entry in &self.entries[start_idx..end_idx] {
            // Interval overlap: NOT (a_end < b_start OR a_start > b_end)
            if entry.interval_start <= query_end && entry.interval_end >= query_start {
                results.push(entry.record_id);
            }
        }
        results
    }
}
```

### Memory Layout Comparison

**Current (fragmented):**
```
HashMap<KeySignature, CandidateList>
  └── CandidateList
        ├── sorted: Vec<(RecordId, Interval)>    [Heap allocation 1]
        ├── unsorted: Vec<(RecordId, Interval)>  [Heap allocation 2]
        └── cluster_tree: IntervalTree           [Heap allocation 3+]
```

**New (contiguous):**
```
Vec<IndexEntry>  [Single heap allocation, cache-line aligned]
  └── [Entry0][Entry1][Entry2]...[EntryN]
       64B     64B     64B        64B
```

---

## 6. LOCK-FREE DSU WITH ATOMIC OPERATIONS

### Current Bottleneck
```rust
pub fn find(&mut self, x: RecordId) -> RecordId {
    // Requires &mut self for path compression
    // Cannot be called from multiple threads
}
```

### New Approach: Atomic Path Compression

```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub struct AtomicDSU {
    // Parent stored as atomic - enables lock-free find
    parent: Vec<AtomicU64>,
    // Rank for union-by-rank
    rank: Vec<AtomicU64>,
}

impl AtomicDSU {
    /// Lock-free find with atomic path compression
    pub fn find(&self, mut x: u64) -> u64 {
        loop {
            let p = self.parent[x as usize].load(Ordering::Acquire);
            if p == x {
                return x;
            }

            let gp = self.parent[p as usize].load(Ordering::Acquire);
            if gp != p {
                // Path compression: point x directly to grandparent
                // CAS may fail if concurrent modification - that's OK
                let _ = self.parent[x as usize].compare_exchange_weak(
                    p, gp,
                    Ordering::Release,
                    Ordering::Relaxed
                );
            }
            x = p;
        }
    }

    /// Lock-free union with CAS retry
    pub fn union(&self, a: u64, b: u64) -> bool {
        loop {
            let root_a = self.find(a);
            let root_b = self.find(b);

            if root_a == root_b {
                return false; // Already in same set
            }

            let rank_a = self.rank[root_a as usize].load(Ordering::Acquire);
            let rank_b = self.rank[root_b as usize].load(Ordering::Acquire);

            let (smaller, larger) = if rank_a < rank_b {
                (root_a, root_b)
            } else {
                (root_b, root_a)
            };

            // CAS to update parent
            match self.parent[smaller as usize].compare_exchange_weak(
                smaller, larger,
                Ordering::Release,
                Ordering::Relaxed
            ) {
                Ok(_) => {
                    if rank_a == rank_b {
                        self.rank[larger as usize].fetch_add(1, Ordering::Release);
                    }
                    return true;
                }
                Err(_) => continue, // Retry on contention
            }
        }
    }
}
```

---

## 7. OPTIMIZED gRPC STREAMING

### Current: Per-Record Messages
```protobuf
// Current: Each record is a separate message
rpc IngestStream(stream RecordInput) returns (stream IngestResponse);
```

### New: Batch Messages with Compression
```protobuf
// New: Batched records with optional compression
message RecordBatch {
    repeated RecordInput records = 1;
    CompressionType compression = 2;
    uint32 original_size = 3;
}

enum CompressionType {
    NONE = 0;
    LZ4 = 1;
    ZSTD = 2;
}

rpc IngestBatchStream(stream RecordBatch) returns (stream BatchResponse);
```

### Server-Side Batch Processing
```rust
async fn ingest_batch_stream(
    &self,
    request: Request<Streaming<RecordBatch>>,
) -> Result<Response<Self::IngestBatchStreamStream>, Status> {
    let mut stream = request.into_inner();
    let (tx, rx) = mpsc::channel(32);

    tokio::spawn(async move {
        while let Some(batch) = stream.next().await {
            let batch = batch?;

            // Decompress if needed
            let records = match batch.compression {
                CompressionType::LZ4 => decompress_lz4(&batch.records),
                CompressionType::ZSTD => decompress_zstd(&batch.records),
                _ => batch.records,
            };

            // Process entire batch at once
            let results = self.partitioned_unirust.ingest_batch(records).await;

            tx.send(BatchResponse { results }).await?;
        }
        Ok(())
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}
```

---

## 8. MEMORY OPTIMIZATION

### Arena Allocator for Hot Path

```rust
use bumpalo::Bump;

pub struct BatchArena {
    arena: Bump,
}

impl BatchArena {
    pub fn new() -> Self {
        Self {
            arena: Bump::with_capacity(1024 * 1024), // 1MB initial
        }
    }

    /// Allocate temporary storage for batch processing
    pub fn alloc_slice<T>(&self, len: usize) -> &mut [T] {
        self.arena.alloc_slice_fill_default(len)
    }

    /// Reset arena for next batch (O(1) operation)
    pub fn reset(&mut self) {
        self.arena.reset();
    }
}
```

### String Interning

```rust
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, Ordering};

pub struct StringInterner {
    map: DashMap<String, u32>,
    counter: AtomicU32,
}

impl StringInterner {
    pub fn intern(&self, s: &str) -> u32 {
        if let Some(id) = self.map.get(s) {
            return *id;
        }

        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        self.map.insert(s.to_string(), id);
        id
    }
}
```

---

## 9. CONFIGURATION FOR 200% IMPROVEMENT

```rust
pub struct HighPerformanceConfig {
    // Partitioning
    pub partition_count: usize,           // 8 (match CPU cores)

    // Batch coalescing
    pub coalesce_capacity: usize,         // 4096 records
    pub coalesce_interval_ms: u64,        // 5ms max wait

    // Worker threads
    pub ingest_workers: usize,            // 8 (one per partition)

    // Memory
    pub arena_size_mb: usize,             // 16MB per partition
    pub index_prealloc: usize,            // 100K entries

    // Serialization
    pub use_rkyv: bool,                   // true
    pub compression: CompressionType,     // LZ4

    // DSU
    pub use_atomic_dsu: bool,             // true
    pub root_cache_size: usize,           // 64K entries
}

impl Default for HighPerformanceConfig {
    fn default() -> Self {
        Self {
            partition_count: 8,
            coalesce_capacity: 4096,
            coalesce_interval_ms: 5,
            ingest_workers: 8,
            arena_size_mb: 16,
            index_prealloc: 100_000,
            use_rkyv: true,
            compression: CompressionType::LZ4,
            use_atomic_dsu: true,
            root_cache_size: 65536,
        }
    }
}
```

---

## 10. EXPECTED PERFORMANCE GAINS

| Component | Current | New | Gain |
|-----------|---------|-----|------|
| Lock contention | 93% idle | 0% idle | 15x |
| DSU operations | Sequential | Lock-free parallel | 4x |
| Index lookups | 100-500ns | 30-100ns | 3-5x |
| Serialization | 1300ns/rec | 200ns/rec | 6.5x |
| Memory allocs | 5-8/record | 0.5/record | 10-16x |
| Batch overhead | Per-record | Amortized | 10x |

### Projected Throughput

```
Current: 91K rec/sec

Component improvements:
- Lock elimination: 91K × 8 (partition parallelism) = 728K theoretical
- DSU bottleneck reduction: -30% overhead removed
- Index optimization: -25% overhead removed
- Serialization: -15% overhead removed

Conservative estimate (accounting for coordination):
- 8 partitions at 60% efficiency = 4.8x parallelism
- 91K × 4.8 = 437K theoretical
- With coordination overhead: 437K × 0.5 = 218K rec/sec

Target: 182K rec/sec (200% improvement) ✓
Projected: 200K-250K rec/sec
```

---

## 11. IMPLEMENTATION PRIORITY

### Phase 1: Partitioned Architecture (Highest Impact)
1. Implement `PartitionedUnirust` with N independent partitions
2. Add consistent hash routing by primary identity key
3. Implement cross-partition merge queue

### Phase 2: Batch Coalescing
1. Add `BatchCoalescer` to ingest pipeline
2. Implement batch gRPC protocol
3. Add LZ4 compression option

### Phase 3: Lock-Free DSU
1. Implement `AtomicDSU` with atomic operations
2. Replace per-partition DSU with atomic variant
3. Benchmark contention vs traditional DSU

### Phase 4: Zero-Copy Serialization
1. Add rkyv serialization for Record types
2. Implement archived index format
3. Migrate persistence layer to rkyv

### Phase 5: Index Optimization
1. Replace IntervalTree with flat sorted arrays
2. Add SIMD-friendly memory layout
3. Implement binary search with linear scan

---

## 12. BREAKING CHANGES

This architecture intentionally breaks backwards compatibility:

1. **Wire Protocol**: New batched gRPC messages
2. **Persistence Format**: rkyv replaces bincode
3. **Cluster Membership**: Partition-aware routing
4. **API Changes**: Batch-oriented ingest methods

Migration path: Version bump to 2.0, no backward compatibility layer.
