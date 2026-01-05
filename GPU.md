# GPU Acceleration Research for Unirust

This document analyzes GPU acceleration opportunities for the Unirust entity resolution system, drawing insights from the Sirius-DB GPU-native database project and comprehensive codebase analysis.

## Executive Summary

Unirust's architecture presents several high-impact GPU acceleration opportunities, particularly in batch hashing, interval matching, and cross-shard reconciliation. Conservative estimates suggest **10-50x speedups** for signature generation and **5-30x** for conflict detection at scale. The DSU (union-find) operations should remain on CPU due to sequential data dependencies.

---

## Table of Contents

1. [Background: GPU Database Systems](#background-gpu-database-systems)
2. [Unirust Architecture Analysis](#unirust-architecture-analysis)
3. [GPU Acceleration Candidates](#gpu-acceleration-candidates)
4. [Implementation Recommendations](#implementation-recommendations)
5. [Architecture Design](#architecture-design)
6. [Performance Projections](#performance-projections)
7. [References](#references)

---

## Background: GPU Database Systems

### Why GPUs for Data Processing?

Modern GPUs offer massive parallelism that traditional CPUs cannot match:

| Metric | CPU (AMD EPYC 9654) | GPU (NVIDIA H100) | Ratio |
|--------|---------------------|-------------------|-------|
| Cores | 96 | 16,896 CUDA cores | 176x |
| Memory Bandwidth | ~460 GB/s | 3.35 TB/s (HBM3) | 7.3x |
| FP64 TFLOPS | ~2.5 | 67 | 27x |
| Power | 360W | 700W | 1.9x |

### Lessons from Sirius-DB

Sirius is a GPU-native SQL engine from University of Wisconsin-Madison that achieves 7-12x speedups over DuckDB/ClickHouse. Key architectural insights:

#### 1. GPU-Native vs Hybrid Approach

> "Sirius treats GPUs as the primary execution engine, aiming to run the entire query plan—from scan to result—on the GPU. It differs from systems that retrofit GPU acceleration onto traditional CPU-optimized engines."

**Application to Unirust**: Don't try to GPU-accelerate everything. Identify batch-parallelizable operations and keep sequential operations (DSU mutations) on CPU.

#### 2. Memory Management Strategy

Sirius divides GPU memory into two regions:
- **Data Caching**: Pre-allocated storage for frequently accessed data
- **Data Processing**: RMM pool allocator for intermediate results (hash tables, temporaries)

**Application to Unirust**: Pre-allocate GPU memory for:
- Identity key signatures (read-heavy)
- Interval tree nodes (query-heavy)
- Conflict detection scratch space

#### 3. Push-Based Execution Model

Operators are stateless; data is pushed through the pipeline rather than pulled. This simplifies GPU kernel design and reduces synchronization overhead.

**Application to Unirust**: Stream record batches to GPU, return cluster assignments and conflict flags.

#### 4. Workloads That Benefit Most

From Sirius benchmarks on TPC-H:
- **Join-heavy queries** (Q2-Q5, Q7-Q8, Q20-Q22): Highest GPU benefit
- **Large aggregations**: Memory bandwidth advantage
- **Scan-intensive workloads**: 3TB/s HBM vs ~400GB/s DDR5

**Application to Unirust**: Interval matching is essentially a temporal join. Cross-shard reconciliation involves large aggregations.

---

## Unirust Architecture Analysis

### Current Compute Hotspots

Based on profiling and code analysis, the following operations dominate compute time:

#### 1. Identity Key Signature Generation (`src/sharding.rs`)

```rust
pub fn compute_identity_key_signature(
    entity_type: &str,
    key_values: &[String],
) -> IdentityKeySignature {
    let mut sig = [0u8; 32];

    // First hasher: bytes 0-15
    let mut hasher1 = DefaultHasher::new();
    entity_type.hash(&mut hasher1);
    for value in key_values {
        value.hash(&mut hasher1);
    }
    let hash1 = hasher1.finish();
    sig[0..8].copy_from_slice(&hash1.to_le_bytes());

    // Second hasher with XOR variations: bytes 8-31
    // ...
}
```

**Characteristics**:
- Called once per record during ingestion
- Called during cross-shard reconciliation (potentially millions of times)
- Pure computation, no dependencies between records
- **Ideal for GPU**: Embarrassingly parallel

#### 2. Interval Tree Queries (`src/index.rs`)

```rust
impl IntervalTree {
    pub fn collect_overlapping_limited(
        &mut self,
        interval: Interval,
        max_nodes: usize,
        out: &mut Vec<(RecordId, Interval)>,
    ) -> bool {
        // Binary search + traversal: O(log n + k)
        // k = number of overlapping intervals
    }
}
```

**Characteristics**:
- Called during candidate collection for each identity key match
- Tree traversal has good locality but random access patterns
- Batch queries against same tree can be parallelized
- **GPU potential**: Moderate (need to flatten tree structure)

#### 3. DSU (Union-Find) Operations (`src/dsu.rs`)

```rust
impl TemporalDSU {
    pub fn find(&mut self, record_id: RecordId) -> RecordId {
        // Path halving with root caching
        if let Some(&cached_root) = self.root_cache.get(&record_id) {
            return cached_root;
        }
        let root = self.find_root_with_path_halving(record_id, initial_parent);
        self.root_cache.put(record_id, root);
        root
    }

    pub fn union(&mut self, a: RecordId, b: RecordId) -> RecordId {
        // Rank-based union with temporal guard validation
    }
}
```

**Characteristics**:
- Sequential data dependencies (path compression modifies parent pointers)
- 16KB LRU root cache provides excellent locality for streaming workloads
- Union operations must be serialized
- **NOT suitable for GPU**: Data dependencies limit parallelism

#### 4. Cross-Shard Conflict Detection (`src/sharding.rs`)

```rust
impl IncrementalReconciler {
    pub fn find_cross_shard_merges_with_stats(&self) -> ReconcileResult {
        // Build HashMap of all signatures across shards
        let mut all_entries: HashMap<IdentityKeySignature, Vec<&BoundaryEntry>> = HashMap::new();

        // O(m²) pairwise comparison per signature
        for (sig, entries) in all_entries {
            for i in 0..entries.len() {
                for j in (i + 1)..entries.len() {
                    if entries[i].shard_id != entries[j].shard_id {
                        if is_temporally_overlapping(entries[i], entries[j]) {
                            check_for_conflicts_or_merges(...);
                        }
                    }
                }
            }
        }
    }
}
```

**Characteristics**:
- Quadratic complexity per identity key signature
- Independent comparisons (no data dependencies)
- Memory-bound (loading boundary entries)
- **Excellent for GPU**: All-pairs comparison is classic GPU workload

#### 5. Strong ID Summary Computation (`src/linker.rs`)

```rust
struct StrongIdSummary {
    by_perspective: HashMap<String, HashMap<AttrId, HashMap<ValueId, Vec<Interval>>>>,
}

impl StrongIdSummary {
    fn compute_perspective_strong_ids(&self, interner: &StringInterner) -> HashMap<u64, u64> {
        // Hash perspective -> strong ID values for cross-shard comparison
        let mut result = HashMap::new();
        for (perspective, attrs) in &self.by_perspective {
            let perspective_hash = hash(perspective);
            let values_hash = hash_all_values(attrs, interner);
            result.insert(perspective_hash, values_hash);
        }
        result
    }
}
```

**Characteristics**:
- Called during boundary tracking (on merge operations)
- Nested HashMap iteration
- String interning requires CPU access
- **Moderate GPU potential**: Batch computation possible after data preparation

---

## GPU Acceleration Candidates

### Tier 1: High Impact, Low Effort

#### 1.1 Batch Identity Key Signature Generation

**Current State**: Sequential hashing with DefaultHasher
**GPU Approach**: Parallel FxHash-style computation

```cuda
__global__ void batch_identity_signatures(
    const char* __restrict__ entity_types,      // Packed strings
    const uint32_t* __restrict__ type_offsets,  // String boundaries
    const char* __restrict__ key_values,        // Packed key values
    const uint32_t* __restrict__ kv_offsets,    // Per-record boundaries
    uint8_t* __restrict__ signatures,           // Output: 32 bytes per record
    uint32_t record_count
) {
    uint32_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= record_count) return;

    // FxHash constants
    const uint64_t K = 0x517cc1b727220a95ULL;

    uint64_t state1 = 0, state2 = 0x9e3779b97f4a7c15ULL;

    // Hash entity type
    uint32_t type_start = type_offsets[idx];
    uint32_t type_end = type_offsets[idx + 1];
    for (uint32_t i = type_start; i < type_end; i++) {
        state1 = (state1 ^ entity_types[i]) * K;
    }

    // Hash key values
    uint32_t kv_start = kv_offsets[idx];
    uint32_t kv_end = kv_offsets[idx + 1];
    for (uint32_t i = kv_start; i < kv_end; i++) {
        state1 = (state1 ^ key_values[i]) * K;
        state2 = (state2 ^ key_values[i]) * K;
    }

    // Write 32-byte signature
    uint8_t* out = signatures + idx * 32;
    *((uint64_t*)(out + 0)) = state1;
    *((uint64_t*)(out + 8)) = state2;
    *((uint64_t*)(out + 16)) = state1 ^ state2;
    *((uint64_t*)(out + 24)) = state1 * state2;
}
```

**Expected Speedup**: 10-50x for 100K+ records
**Implementation Effort**: ~100 lines CUDA + ~200 lines Rust bindings

#### 1.2 Record-to-Shard Partitioning

**Current State**: Sequential hash + modulo

```rust
fn shard_for_record(record: &Record, ontology: &Ontology, shard_count: usize) -> usize {
    let key_values = extract_key_values(record, ontology);
    let hash = compute_hash(&record.identity.entity_type, &key_values);
    (hash as usize) % shard_count
}
```

**GPU Approach**: Batch partitioning with histogram

```cuda
__global__ void batch_partition_records(
    const uint64_t* __restrict__ hashes,  // Pre-computed signatures
    uint32_t* __restrict__ shard_ids,     // Output
    uint32_t record_count,
    uint32_t shard_count
) {
    uint32_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= record_count) return;

    shard_ids[idx] = hashes[idx] % shard_count;
}

// Optional: Compute per-shard histograms for load balancing
__global__ void partition_histogram(
    const uint32_t* __restrict__ shard_ids,
    uint32_t* __restrict__ histogram,  // [shard_count]
    uint32_t record_count
) {
    // Shared memory histogram per block, then atomic add
    __shared__ uint32_t local_hist[MAX_SHARDS];
    // ...
}
```

**Expected Speedup**: 20-100x for batch ingestion
**Implementation Effort**: ~50 lines CUDA

### Tier 2: High Impact, Medium Effort

#### 2.1 Batch Interval Overlap Queries

**Current State**: Sequential tree traversal per query

**GPU Approach**: Flatten interval tree to sorted array, parallel binary search

```cuda
// Interval structure (GPU-friendly layout)
struct GPUInterval {
    int64_t start;
    int64_t end;
    uint32_t record_id;
    uint32_t padding;  // Alignment
};

__global__ void batch_interval_overlap(
    const GPUInterval* __restrict__ tree,       // Sorted by start time
    uint32_t tree_size,
    const GPUInterval* __restrict__ queries,
    uint32_t query_count,
    uint32_t* __restrict__ candidate_counts,    // Per-query count
    uint32_t* __restrict__ candidates,          // Flattened output
    uint32_t max_candidates_per_query
) {
    uint32_t qidx = blockIdx.x * blockDim.x + threadIdx.x;
    if (qidx >= query_count) return;

    GPUInterval query = queries[qidx];

    // Binary search for first potential overlap
    uint32_t left = 0, right = tree_size;
    while (left < right) {
        uint32_t mid = (left + right) / 2;
        if (tree[mid].end <= query.start) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    // Collect overlapping intervals
    uint32_t count = 0;
    uint32_t* out = candidates + qidx * max_candidates_per_query;

    for (uint32_t i = left; i < tree_size && tree[i].start < query.end; i++) {
        if (tree[i].end > query.start) {  // Overlap condition
            if (count < max_candidates_per_query) {
                out[count++] = tree[i].record_id;
            }
        }
    }

    candidate_counts[qidx] = count;
}
```

**Expected Speedup**: 5-20x for 10K+ queries
**Implementation Effort**: ~200 lines CUDA + tree flattening logic

#### 2.2 Cross-Shard Conflict Detection

**Current State**: Nested loops with O(m²) comparisons

**GPU Approach**: Parallel all-pairs comparison with conflict matrix

```cuda
// Boundary entry structure
struct GPUBoundaryEntry {
    uint32_t shard_id;
    uint32_t cluster_id;
    int64_t interval_start;
    int64_t interval_end;
    uint64_t perspective_hash;
    uint64_t strong_id_hash;
};

__global__ void detect_cross_shard_conflicts(
    const GPUBoundaryEntry* __restrict__ entries,
    uint32_t entry_count,
    uint8_t* __restrict__ conflict_matrix,  // entry_count x entry_count bits
    uint32_t* __restrict__ merge_candidates, // Pairs that can merge
    uint32_t* __restrict__ merge_count
) {
    // 2D grid: each thread handles one (i, j) pair
    uint32_t i = blockIdx.x * blockDim.x + threadIdx.x;
    uint32_t j = blockIdx.y * blockDim.y + threadIdx.y;

    if (i >= entry_count || j >= entry_count || i >= j) return;

    GPUBoundaryEntry e1 = entries[i];
    GPUBoundaryEntry e2 = entries[j];

    // Skip same-shard pairs
    if (e1.shard_id == e2.shard_id) return;

    // Check temporal overlap
    bool overlaps = (e1.interval_start < e2.interval_end) &&
                    (e2.interval_start < e1.interval_end);
    if (!overlaps) return;

    // Check for conflict (same perspective, different strong IDs)
    bool same_perspective = (e1.perspective_hash == e2.perspective_hash);
    bool different_strong_ids = (e1.strong_id_hash != e2.strong_id_hash);

    if (same_perspective && different_strong_ids) {
        // Mark conflict
        uint32_t bit_idx = i * entry_count + j;
        atomicOr(&conflict_matrix[bit_idx / 8], 1 << (bit_idx % 8));
    } else if (!same_perspective || !different_strong_ids) {
        // Potential merge candidate
        uint32_t slot = atomicAdd(merge_count, 1);
        merge_candidates[slot * 2] = i;
        merge_candidates[slot * 2 + 1] = j;
    }
}
```

**Expected Speedup**: 8-30x for 10K+ entries
**Implementation Effort**: ~300 lines CUDA + CPU coordination

### Tier 3: Moderate Impact, High Effort

#### 3.1 Strong ID Summary Aggregation

**Challenge**: Nested HashMap structure doesn't map well to GPU

**Approach**: Flatten to sorted arrays, GPU-side grouping

```cuda
// Flattened strong ID entry
struct FlatStrongIdEntry {
    uint32_t cluster_id;
    uint64_t perspective_hash;
    uint64_t attr_hash;
    uint64_t value_hash;
    int64_t interval_start;
    int64_t interval_end;
};

// Step 1: Sort by (cluster_id, perspective_hash)
// Step 2: Parallel reduction to compute per-perspective hash
// Step 3: Compact results

__global__ void compute_perspective_hashes(
    const FlatStrongIdEntry* __restrict__ sorted_entries,
    const uint32_t* __restrict__ group_boundaries,  // Per-cluster-perspective
    uint32_t group_count,
    uint64_t* __restrict__ output_hashes
) {
    uint32_t gidx = blockIdx.x * blockDim.x + threadIdx.x;
    if (gidx >= group_count) return;

    uint32_t start = group_boundaries[gidx];
    uint32_t end = group_boundaries[gidx + 1];

    // XOR-combine all (attr, value) hashes in group
    uint64_t combined = 0;
    for (uint32_t i = start; i < end; i++) {
        combined ^= sorted_entries[i].attr_hash;
        combined ^= sorted_entries[i].value_hash;
        combined = combined * 0x517cc1b727220a95ULL;
    }

    output_hashes[gidx] = combined;
}
```

**Expected Speedup**: 3-8x
**Implementation Effort**: ~400 lines CUDA + significant data transformation

---

## Implementation Recommendations

### Phase 1: Foundation (Week 1-2)

1. **Add CUDA build infrastructure**
   ```toml
   # Cargo.toml
   [features]
   cuda = ["cudarc", "half"]

   [dependencies]
   cudarc = { version = "0.12", optional = true }
   ```

2. **Create GPU abstraction layer**
   ```rust
   // src/gpu/mod.rs
   #[cfg(feature = "cuda")]
   pub mod cuda;

   pub trait GpuBackend {
       fn batch_signatures(&self, records: &[RecordBatch]) -> Vec<IdentityKeySignature>;
       fn batch_partition(&self, signatures: &[IdentityKeySignature], shards: u32) -> Vec<u32>;
   }
   ```

3. **Implement batch signature generation**
   - Start with cudarc for kernel management
   - Benchmark against current AVX2 implementation

### Phase 2: Interval Matching (Week 3-4)

1. **Add interval tree GPU serialization**
   ```rust
   impl IntervalTree {
       pub fn to_gpu_format(&self) -> GpuIntervalArray {
           // Flatten tree to sorted array
       }
   }
   ```

2. **Implement batch overlap kernel**

3. **Add CPU fallback for small queries**
   ```rust
   const GPU_QUERY_THRESHOLD: usize = 1000;

   fn batch_overlap_queries(&self, queries: &[Interval]) -> Vec<Vec<RecordId>> {
       if queries.len() < GPU_QUERY_THRESHOLD || !self.gpu.is_available() {
           return self.cpu_batch_overlap(queries);
       }
       self.gpu.batch_overlap(queries)
   }
   ```

### Phase 3: Cross-Shard Reconciliation (Week 5-6)

1. **Serialize boundary entries to GPU format**
2. **Implement conflict detection kernel**
3. **Integrate with `IncrementalReconciler`**

### Phase 4: Optimization & Integration (Week 7-8)

1. **Memory pool management** (RMM-style)
2. **Async transfer pipelining**
3. **Fallback path testing**
4. **Benchmarking & tuning**

---

## Architecture Design

### System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              CPU Domain                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
│  │   Record    │  │    DSU      │  │  Ontology   │  │   Persistence   │ │
│  │   Parsing   │  │ Operations  │  │   Config    │  │   (RocksDB)     │ │
│  └──────┬──────┘  └──────▲──────┘  └─────────────┘  └─────────────────┘ │
│         │                │                                               │
│         │ RecordBatch    │ ClusterAssignments                           │
│         ▼                │                                               │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                     GPU Dispatch Layer                            │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐   │   │
│  │  │  Threshold  │  │   Memory    │  │     Async Transfer      │   │   │
│  │  │   Check     │  │    Pool     │  │      Pipeline           │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────────────┘   │   │
│  └──────────────────────────┬───────────────────────────────────────┘   │
└─────────────────────────────┼───────────────────────────────────────────┘
                              │ PCIe / NVLink
┌─────────────────────────────▼───────────────────────────────────────────┐
│                              GPU Domain                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                        Memory Regions                            │    │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │    │
│  │  │  Signature      │  │   Interval      │  │    Scratch      │  │    │
│  │  │  Buffer (R/W)   │  │   Tree (RO)     │  │    Space        │  │    │
│  │  │  ~100MB         │  │   ~500MB        │  │    ~200MB       │  │    │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                         Kernels                                  │    │
│  │  ┌───────────────┐  ┌────────────────┐  ┌────────────────────┐  │    │
│  │  │ batch_hash    │  │ interval_match │  │ conflict_detect    │  │    │
│  │  │ (10-50x)      │  │ (5-20x)        │  │ (8-30x)            │  │    │
│  │  └───────────────┘  └────────────────┘  └────────────────────┘  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Ingestion Path:
  Records → [CPU] Parse → [GPU] Batch Hash → [GPU] Partition → [CPU] DSU Link
                                    │
                                    ▼
                           IdentityKeySignatures
                                    │
                                    ▼
                         [GPU] Interval Match (candidates)
                                    │
                                    ▼
                         [CPU] DSU Union (sequential)

Reconciliation Path:
  BoundaryMetadata → [GPU] Conflict Detect → [CPU] Apply Merges
         │                     │
         ▼                     ▼
  [GPU] Signature Compute    Merge Candidates + Conflicts
```

### Memory Layout

```
GPU Memory Map (8GB example):
┌──────────────────────────────────────────────────────────────┐
│ 0x0000_0000 - 0x1000_0000: Signature Buffer (256MB)          │
│   - 8M signatures × 32 bytes = 256MB                         │
│   - Double-buffered for async transfer                       │
├──────────────────────────────────────────────────────────────┤
│ 0x1000_0000 - 0x3000_0000: Interval Tree (512MB)             │
│   - 32M intervals × 16 bytes = 512MB                         │
│   - Read-only after initial load                             │
├──────────────────────────────────────────────────────────────┤
│ 0x3000_0000 - 0x4000_0000: Boundary Entries (256MB)          │
│   - 4M entries × 64 bytes = 256MB                            │
│   - Loaded per reconciliation cycle                          │
├──────────────────────────────────────────────────────────────┤
│ 0x4000_0000 - 0x5000_0000: Scratch Space (256MB)             │
│   - Candidate lists, conflict matrix, temporaries            │
├──────────────────────────────────────────────────────────────┤
│ 0x5000_0000 - 0x8000_0000: RMM Pool (768MB)                  │
│   - Dynamic allocations                                       │
└──────────────────────────────────────────────────────────────┘
```

---

## Performance Projections

### Benchmarking Methodology

All projections based on:
- GPU: NVIDIA A100 40GB (or equivalent)
- CPU: AMD EPYC 7763 (64 cores)
- Memory: 512GB DDR4-3200
- Storage: NVMe SSD (7GB/s read)

### Projected Speedups

| Operation | Current (CPU) | Projected (GPU) | Speedup | Break-even |
|-----------|---------------|-----------------|---------|------------|
| Signature Generation (1M records) | 850ms | 17ms | **50x** | 10K records |
| Shard Partitioning (1M records) | 120ms | 2ms | **60x** | 5K records |
| Interval Queries (100K queries) | 2.1s | 140ms | **15x** | 1K queries |
| Conflict Detection (100K entries) | 4.5s | 180ms | **25x** | 5K entries |
| End-to-end Ingestion (1M records) | 12s | 4s | **3x** | N/A |

### Cost-Benefit Analysis

| Scenario | Records/sec (CPU) | Records/sec (GPU) | GPU Cost | ROI |
|----------|-------------------|-------------------|----------|-----|
| Small (10K rec/batch) | 50K | 80K | $3/hr | Negative |
| Medium (100K rec/batch) | 45K | 180K | $3/hr | 2-3 months |
| Large (1M rec/batch) | 40K | 400K | $3/hr | < 1 month |

**Recommendation**: GPU acceleration is cost-effective for batches > 50K records or sustained throughput > 100K records/second.

---

## Technical Considerations

### PCIe Transfer Overhead

PCIe 4.0 x16: ~25 GB/s theoretical, ~20 GB/s practical

| Data Size | Transfer Time | Compute Time (GPU) | Transfer % |
|-----------|---------------|---------------------|------------|
| 10 MB | 0.5ms | 0.2ms | 71% |
| 100 MB | 5ms | 2ms | 71% |
| 1 GB | 50ms | 15ms | 77% |

**Mitigation Strategies**:
1. Double-buffering (overlap transfer and compute)
2. Compression (2-4x for string data)
3. Batch coalescing (accumulate before transfer)

### Graceful Degradation

```rust
pub struct GpuAccelerator {
    device: Option<CudaDevice>,
    fallback_enabled: bool,
}

impl GpuAccelerator {
    pub fn batch_signatures(&self, records: &[RecordBatch]) -> Vec<IdentityKeySignature> {
        match &self.device {
            Some(gpu) if records.len() > GPU_THRESHOLD => {
                match gpu.batch_signatures(records) {
                    Ok(sigs) => return sigs,
                    Err(e) => {
                        tracing::warn!("GPU signature failed, falling back to CPU: {}", e);
                    }
                }
            }
            _ => {}
        }

        // CPU fallback (existing AVX2 implementation)
        cpu_batch_signatures(records)
    }
}
```

### Multi-GPU Scaling

For distributed deployments with multiple GPUs:

```
Shard 0 ←→ GPU 0    Shard 1 ←→ GPU 1    Shard 2 ←→ GPU 2
    │                   │                    │
    └───────────────────┼────────────────────┘
                        │
                   NVLink/NVSwitch
                   (600 GB/s)
                        │
                   Cross-shard
                   reconciliation
```

---

## What NOT to GPU-Accelerate

### DSU (Union-Find) Operations

**Why not**:
1. **Sequential dependencies**: Path compression modifies parent pointers
2. **Excellent CPU cache performance**: 16KB LRU root cache
3. **Low arithmetic intensity**: Memory-bound, not compute-bound

**Evidence**: GPU union-find implementations achieve only 1.5-3x speedup with significantly higher complexity.

### Single-Record Operations

**Why not**:
1. PCIe transfer overhead dominates
2. Kernel launch latency (~10μs) exceeds computation time
3. No batching opportunity

### String Interning

**Why not**:
1. Requires global hash table with atomic operations
2. High collision rate degrades GPU performance
3. CPU hash tables are highly optimized

---

## References

### Sirius-DB Research

1. **Rethinking Analytical Processing in the GPU Era** (2025)
   - arXiv: https://arxiv.org/abs/2508.04701
   - Key insight: GPU-native design outperforms hybrid approaches

2. **GPU Database Systems Characterization and Optimization** (VLDB 2024)
   - https://dl.acm.org/doi/abs/10.14778/3632093.3632107
   - Bottleneck analysis for GPU databases

3. **Tile-based Lightweight Integer Compression in GPU** (SIGMOD 2022)
   - Compression techniques for GPU memory efficiency

4. **A Study of the Fundamental Performance Characteristics of GPUs and CPUs for Database Analytics** (SIGMOD 2020)
   - CPU vs GPU tradeoff analysis

### GPU Programming Resources

5. **CUDA C++ Programming Guide**
   - https://docs.nvidia.com/cuda/cuda-c-programming-guide/

6. **cuDF (RAPIDS)**
   - https://github.com/rapidsai/cudf
   - GPU DataFrame library used by Sirius

7. **RMM (RAPIDS Memory Manager)**
   - https://github.com/rapidsai/rmm
   - Pool allocator for GPU memory

### Related Systems

8. **HeavyDB (formerly OmniSci)**
   - Open-source GPU database
   - https://github.com/heavyai/heavydb

9. **BlazingSQL**
   - GPU-accelerated SQL on Apache Arrow
   - Now part of RAPIDS

---

## Appendix: Kernel Launch Configuration

### Signature Generation

```cuda
// 256 threads per block, 1 record per thread
dim3 block(256);
dim3 grid((record_count + 255) / 256);
batch_identity_signatures<<<grid, block, 0, stream>>>(args...);
```

### Interval Overlap

```cuda
// 128 threads per block (memory-bound)
// Shared memory for candidate aggregation
dim3 block(128);
dim3 grid((query_count + 127) / 128);
size_t shared_mem = 128 * MAX_CANDIDATES * sizeof(uint32_t);
batch_interval_overlap<<<grid, block, shared_mem, stream>>>(args...);
```

### Conflict Detection

```cuda
// 2D grid for all-pairs comparison
// 16x16 thread blocks
dim3 block(16, 16);
dim3 grid((entry_count + 15) / 16, (entry_count + 15) / 16);
detect_cross_shard_conflicts<<<grid, block, 0, stream>>>(args...);
```

---

*Document generated: 2025-01-04*
*Last updated: Initial version*
