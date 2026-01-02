# Productionalization TODO

This is the working checklist to take Unirust from functional to production-ready at scale.
Focus: correctness, durability, and performance with 32GB RAM per shard and 100M+ entities.

## Phase 0: Safety and Integrity (now)

- [x] Add storage manifest with format version + app version
- [x] Validate manifest on startup (fail fast on mismatched format)
- [x] Ensure RocksDB flush on shutdown (drop-safe)
- [x] Add periodic checkpoint API (manual trigger is enough for now)
- [x] Add startup corruption checks / repair option

## Phase 1: Memory + Indexing (scale)

- [x] Refactor `RecordStore` to return owned records or iterators
- [x] Eliminate full in-memory record scan dependency in query/linker/conflicts
- [x] Implement RocksDB-native indexes for attr/value, entity_type, perspective
- [x] Implement temporal bucketed index (time-windowed keys)
- [x] Add bounded LRU cache for hot records

## Phase 2: Ingest Throughput

- [x] Batch write record + index updates via RocksDB `WriteBatch`
- [x] Async ingest pipeline with bounded queue + backpressure
- [x] Compaction tuning (memtable size, level sizes, compression)
- [x] Incremental interner persistence (keyed entries, not full snapshot)

## Phase 3: Query and Conflict Performance

- [x] Query planner to choose the smallest index scan
- [x] Incremental conflict detection for affected clusters only
- [x] Precompute cluster keys and golden summaries (cacheable)

## Phase 4: Distributed Reliability

- [x] Shard discovery (static file or registry)
- [x] Routing config versioning (fail fast if router/shards disagree)
- [x] Rebalancing plan + tooling
- [x] Replica/backup strategy (snapshot shipping)

Rebalancing plan (v1):
- Use consistent hashing ring with virtual nodes.
- Provide a tooling command to list record ID ranges per shard and to export/import those ranges.
- Move records via batch export/import RPCs (future: incremental streaming).
- Freeze writes during move or apply dual-write with cutover (future).

Replica/backup strategy (v1):
- Use `Checkpoint` RPC per shard to create on-disk snapshots.
- Ship snapshot directories to object storage on a schedule (external job).
- Restore by replaying snapshots + rehydrating interner state.

## Phase 5: Production Operations

- [x] Metrics/tracing (ingest latency, query latency, compactions, qps)
- [x] Alerting + runbooks
- [x] SLOs and capacity planning
