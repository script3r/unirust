# AGENTS.md

Instructions for AI agents working on this codebase.

## Project Overview

Unirust is a distributed temporal entity resolution engine. The primary function is to ingest records from multiple source systems and cluster them into unified entities while respecting temporal constraints.

## Critical Invariants

### Entity Resolution Must Always Happen

Every ingested record MUST go through entity resolution. Never skip or bypass:
- `linker.link_records_batch_parallel()` - batch links with parallel extraction
- `partitioned.process_batch_optimized()` - optimized partition processing
- `partitioned.ingest_batch()` - distributed batch processing

Any optimization that skips entity resolution is incorrect and breaks the core value proposition.

### Persistence Mode for Production

- **Unit tests**: May use in-memory `Store::new()`
- **Integration tests**: Must use `PersistentStore`
- **Examples**: Must demonstrate sharded/distributed mode
- **Benchmarks**: Should test both modes but focus on persistent

### Binary Format Only

- No JSON for data storage or WAL
- Use protobuf/bincode for serialization
- JSON is only acceptable for:
  - Ontology configuration files (external input)
  - Graph visualization exports (external output)

## File Organization

```
src/
├── lib.rs              # Public API (Unirust struct)
├── linker.rs           # Core entity resolution
├── dsu.rs              # Disjoint Set Union
├── store.rs            # In-memory store
├── persistence.rs      # RocksDB store
├── distributed.rs      # gRPC services
├── partitioned.rs      # Parallel processing
├── ontology.rs         # Matching rules
├── conflicts.rs        # Conflict detection
└── bin/
    ├── unirust_router.rs   # Router binary
    ├── unirust_shard.rs    # Shard binary
    └── unirust_loadtest.rs # Load testing
```

## Key Entry Points

### Ingest Flow
1. `distributed.rs:ShardNode::ingest_records()` - gRPC entry
2. `distributed.rs:dispatch_ingest_partitioned()` - routes to partitioned processing
3. `partitioned.rs:ParallelPartitionedUnirust::ingest_batch_with_partitions()` - parallel partition dispatch
4. `partitioned.rs:Partition::process_batch_optimized()` - **hot path**: batch insert → parallel extract → sequential link
5. `linker.rs:link_records_batch_parallel()` - parallel key extraction, sequential DSU merges

### Query Flow
1. `distributed.rs:RouterService::query_entities()` - gRPC entry
2. `lib.rs:Unirust::query_master_entities()` - query execution
3. `query.rs` - query planning and execution

## Testing Strategy

### Unit Tests
- Located in each source file as `#[cfg(test)]` modules
- May use in-memory stores
- Fast, isolated tests

### Integration Tests
- Located in `tests/` directory
- Must use `PersistentStore` with `tempfile`
- Test distributed scenarios (router + shards)

### Load Testing
- Use `unirust_loadtest` binary
- Standard command: `./target/release/unirust_loadtest -r http://127.0.0.1:50060 -c 10000000 --streams 16 --batch 5000`
- Baseline with 5 shards, 10% overlap: **~410K rec/sec, ~12ms batch latency**

## Performance Considerations

### Do Not Regress
After any change, verify performance with loadtest. Current baseline with 5 shards:
- **~410K records/second** (10% overlap)
- **~12ms batch latency**

### Hot Paths
- `partitioned.rs:process_batch_optimized()` - batch insert + parallel extract + sequential link
- `linker.rs:link_records_batch_parallel()` - parallel extraction, sequential DSU
- `linker.rs:link_extracted_record()` - DSU merges with temporal guards
- `dsu.rs:find()` - path compression with root cache

### Avoid
- Unnecessary cloning of large structures
- Lock contention in hot paths
- JSON serialization in data path
- Unbounded allocations

## Common Tasks

### Adding a New Feature
1. Update ontology if new matching rules needed
2. Add to `lib.rs` public API
3. Add unit tests
4. Add integration test in `tests/`
5. Run `cargo test`, `cargo clippy`, `cargo fmt`

### Modifying Entity Resolution
1. Changes to `linker.rs` require careful review
2. Must maintain temporal guard semantics
3. Must not break cluster correctness
4. Add regression tests for edge cases

### Adding gRPC Endpoints
1. Update `proto/unirust.proto`
2. Regenerate with `cargo build`
3. Implement in `distributed.rs`
4. Add integration test

## Commands Reference

```bash
# Development
cargo test                          # Run all tests
cargo clippy --all-targets          # Lint
cargo fmt                           # Format

# Benchmarks
cargo bench --bench bench_quick     # Fast (~30s)
cargo bench --bench bench_micro     # Component benchmarks

# Start cluster (recommended)
SHARDS=5 ./scripts/cluster.sh start

# Load test (requires running cluster)
./target/release/unirust_loadtest \
  --router http://127.0.0.1:50060 \
  --count 10000000 \
  --streams 16 \
  --batch 5000

# Stop cluster
./scripts/cluster.sh stop
```

## Style Guidelines

- Use `Result<T, UniError>` for fallible operations
- Prefer `&str` over `String` for parameters
- Use `#[inline]` for small hot functions
- Avoid `unwrap()` in library code
- Comments explain "why", code explains "what"
