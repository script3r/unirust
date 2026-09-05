# AGENTS.md

Instructions for AI agents working on this codebase.

## Project Overview

Unirust is a distributed temporal entity resolution engine. The primary function is to ingest records from multiple source systems and cluster them into unified entities while respecting temporal constraints.

## Critical Invariants

### Entity Resolution Must Always Happen

Every newly accepted record MUST complete entity resolution before its record data
commits. Valid retries of an already resolved source identity remain idempotent.
Preserve the linker calls in every ingest path, including:
- `Unirust::stream_records()` — persistent shard ingestion
- `StreamingLinker::link_records_batch_parallel_with_interner()` — parallel extraction and sequential linking
- `Partition::process_batch_optimized()` — the separate in-memory partition path

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

### Persistent Ingest Flow
1. `distributed.rs:RouterService::ingest_records()` — placement and source-identity reservations
2. `distributed.rs:ShardNode::ingest_records()` — shard RPC and ingest WAL
3. `distributed.rs:dispatch_ingest_records()` — worker dispatch
4. `lib.rs:Unirust::stream_records()` — stage records, link, commit and flush
5. `linker.rs:StreamingLinker` — key extraction, temporal guards and DSU merges

Persistent shards disable partitioned ingestion. `dispatch_ingest_partitioned()`
and `ParallelPartitionedUnirust` use in-memory partition stores and must never
replace the persistent path.

### Query Flow
1. `distributed.rs:RouterService::query_entities()` — concurrent candidate discovery and canonical fragment hydration
2. `lib.rs:Unirust::query_master_entities()` — local candidates and golden descriptors
3. `query.rs` and `graph.rs` — temporal conjunction and mastering

See [DESIGN.md](DESIGN.md) for recovery, reconciliation and matching limits.

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

Build the load generator explicitly with `--features test-support` (see below).
Use fresh persistent directories and a fixed ontology, topology, seed and workload
for comparisons. Do not change the shard count of an existing dataset to run a
benchmark; durable reservations bind it to its topology.

## Performance Considerations

### Do Not Regress

For runtime changes, compare persistent load tests before and after the change,
including acknowledgement counts, failures, throughput and measured RPC latency.
Use focused diagnostics for query or recovery changes. Documentation-only edits
require checking their examples and claims rather than repeating throughput runs.

The September audit measured approximately 43,415 versus 43,939 records/second
on one local five-shard, one-million-record comparison (10% overlap); average RPC
latency was approximately 1,585 versus 1,657 ms. These are observations, not
performance guarantees. The old 410K records/second and 12 ms figures do not
establish a durable production baseline. Workload details and limitations are in
[the audit report](docs/critical-audit-2026-09-05.md).

### Hot Paths
- `lib.rs:stream_records()` - persistent staging, resolution and commit
- `partitioned.rs:process_batch_optimized()` - separate in-memory partition processing
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
cargo test --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo fmt --check
cargo test --doc --locked --all-features

# Benchmarks
cargo bench --bench bench_quick     # Focused benchmark suite
cargo bench --bench bench_micro     # Component benchmarks

# Build the load generator; run the cluster on a fresh benchmark dataset
cargo build --release --locked --features test-support --bin unirust_loadtest
SHARDS=5 ./scripts/cluster.sh start

# Load test (requires running cluster)
./target/release/unirust_loadtest \
  --router http://127.0.0.1:50060 \
  --count 10000000 \
  --streams 16 \
  --batch 5000 --overlap 0.1 --seed 42

# Stop cluster
./scripts/cluster.sh stop
```

## Style Guidelines

- Follow existing error types: `anyhow::Result` for library operations and `tonic::Status` at gRPC boundaries
- Prefer `&str` over `String` for parameters
- Use `#[inline]` for small hot functions
- Avoid `unwrap()` in library code
- Comments explain "why", code explains "what"
