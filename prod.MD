# Productionalization TODO

This is the final checklist to take Unirust to billions of entities on 32GB RAM per shard.
Focus: correctness, durability, and sustained throughput at scale.

## Billion-scale requirements (final)

- [x] Avoid full record loads on startup; access records lazily from RocksDB only.
- [x] Persist identity -> record_id index in RocksDB for idempotent ingest without RAM maps.
- [x] Maintain exact record_count metadata (avoid relying on next_record_id).
- [x] Persist incremental cluster state (no full rebuild for queries/stats).
- [x] Persist incremental conflict materialization per affected cluster only.
- [x] Replace stats paths that build full graph with lightweight counters.
- [x] Shard-local parallel ingest workers with key-based partitioning.
- [x] Hot-key mitigation + shard auto-rebalance with dual-write cutover.
- [x] Disk-backed or compacted interner/value dictionary for high-cardinality values.
- [x] Scale benchmarks: 100M+ ingest harness and p50/p95/p99 regression gates.
- [x] Failure/chaos tests for WAL replay and partial ingest.
- [x] Snapshot restore validation at scale (automated correctness checks).
