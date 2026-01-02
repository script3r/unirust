# Storage and Persistence (RocksDB)

This document captures the current persistence strategy, why it exists, and the plan to scale it.
It is intentionally critical so we can see the gaps before they hurt us in production.

## Goals

- Preserve shard state across restarts (records + interned strings + ontology).
- Keep ingest fast and predictable.
- Preserve deterministic reconciliation within a shard.
- Support tens to hundreds of millions of records per shard without redesigning the entire system.

## Current Reality (Before Persistence)

- All shard state lived in memory.
- Restarting a shard loses records, indices, and interner state.
- Streaming reconcile assumes a consistent interner; without persistence this is lost.

## Access Patterns

- **Ingest**: append-heavy, per-record updates, low contention, mostly sequential.
- **Query**: indexed lookups by attribute/value + temporal overlap; must be fast.
- **Conflicts**: derived from clusters; often full or partial scans.
- **Graph export**: full scan of the shard’s records.

## Persistence Model (v1)

We persist per shard using RocksDB with column families:

- `records`: `record_id -> Record` (bincode)
- `interner`: `interner -> StringInterner` (bincode)
- `metadata`: `next_record_id`, `ontology_config`

On startup:
- Load interner + metadata.
- Load all records into an in-memory `Store` (and rebuild indices).
- Rebuild ontology from the stored or CLI config.

Usage (per shard):
```
unirust_shard --listen 0.0.0.0:50061 --shard-id 0 --data-dir /var/lib/unirust/shard0
```

This guarantees correct behavior across restarts with minimal impact on the rest of the codebase.

### Critical limitations of v1

- **Memory bound**: all records are still loaded into memory.
- **Full scans**: building clusters and conflicts still require full-store scans.
- **Interner persistence is coarse**: we snapshot the entire interner on each ingest batch.
- **No durability for derived state**: clusters and graphs are recomputed on demand.

This is correct and simple, but **not yet sufficient** for hundreds of millions of entities.

## Scaling Strategy (v2+)

To support 100M+ entities per shard, we must move away from “full in-memory store”:

1) **RecordStore API evolution**
   - Return owned records, iterators, or streaming cursors instead of `&Record`.
   - Make query and linker components operate on iterators instead of full scans.

2) **RocksDB-native secondary indexes**
   - `attr/value -> posting list of record ids`
   - `entity_type -> record ids`
   - `perspective -> record ids`
   - `temporal buckets -> record ids`

3) **Incremental interner persistence**
   - Store mappings as separate key/value entries instead of full snapshots.

4) **Background compaction + TTL strategies**
   - Use time-partitioned keys for temporal data.
   - Apply compaction filters for expired intervals.

5) **Operational safety**
   - Periodic snapshots.
   - Write-ahead log (WAL) enabled.
   - Validation pass on startup (detect corruption, partial writes).

## Implementation Plan (v1)

- Add `PersistentStore` (RocksDB-backed) that delegates to the existing `Store`.
- Persist records + interner + metadata on each write.
- Load from disk on startup so shards survive restart.
- Persist ontology config so shards restore deterministically.
- Keep API stable so all existing tests and demos keep working.

## Plan Critique

What this plan does well:
- Preserves correctness and determinism.
- Minimal change risk.
- Explicit, reversible, and testable.

What it does poorly:
- Does not address memory growth at all.
- Doesn’t improve query scalability beyond current in-memory indexes.
- Expensive interner snapshots on each ingest.

## Plan Refinement (v1.5)

Before moving to a full v2 refactor, the following pragmatic upgrades will help:

- Persist interner entries incrementally (append-only CF).
- Add a lightweight record-id index by attribute/value directly in RocksDB.
- Add a startup option to skip full in-memory load for “cold start” query-only tasks.

## Summary

The current implementation is correct and restart-safe, but it is **not yet scale-safe**.
We should view v1 as the durable foundation and build toward v2 indexing and streaming
so we can confidently handle hundreds of millions of entities per shard.
