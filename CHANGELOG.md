# Changelog

All notable changes to this project are documented here. This project follows
Semantic Versioning.

## [Unreleased]

### Performance

- Avoid global-ID/boundary scans when a local merge retains its canonical ID,
  and incrementally merge normalized strong-ID histories while preserving the
  legacy behavior of malformed public intervals.
- Replace quadratic temporal candidate discovery with an exact cross-shard sweep
  and dense-overlap path, retaining all candidates and component conflict guards.
- Sweep golden-value boundaries and intersect normalized interval sets with two
  pointers for queries and constraint overlap reporting.
- Add persistent differential/recovery regressions and reproducible timing
  diagnostics; see `docs/performance-analysis-2026-09-05.md` for measured scope.

### Fixed

- Authenticate protoc setup requests in CI to avoid unauthenticated GitHub API
  rate limits (merged after the v0.2.0 tag).

### Documentation

- Align deployment, architecture, configuration and Rust API examples with the
  persistent runtime; describe matching, memory and recovery limits explicitly.
- Correct load-test diagnostic guidance: `UNIRUST_PROFILE` selects a tuning
  preset and does not enable a profiler.
- Build API documentation in CI with warnings treated as errors; compile the
  crate quick-start example as a doctest.
- Remove unsupported GPU projections and performance guarantees, and distinguish
  measured local diagnostics from production capacity claims.

## [0.2.0] - 2026-09-05

### Added

- Persistent distributed shards, streaming ingest, cross-shard reconciliation,
  coordinated checkpoints, verified off-host backups, and synchronous replication.
- Mutual TLS transport, semantic readiness checks, and fail-closed recovery for
  partial reconciliation, interrupted ingestion, and inconsistent restores.
- Enabled persistent regressions for the September correctness audit and query
  equivalence across bridge merges, cache invalidation, and restart.

### Fixed

- Transitive cross-shard merges now compare component-wide temporal strong IDs,
  including observations from previous reconciliation rounds.
- Distributed queries assemble complete canonical entities before temporal
  conjunction and golden attribute mastering.
- Occupied record IDs cannot overwrite durable records; rejected batches discard
  staged records and partial resolution state before another request can commit.
- Bounded interner caches preserve durable IDs after restart. Corrupt query
  lookups fail explicitly instead of returning empty matches.
- Repeated common keys and the memory-saver profile continue entity resolution.
  Tiered index updates preserve complete buckets through eviction and recovery,
  respect capacity settings, and propagate storage failures.
- Persistent DSU cluster enumeration, deferred membership updates, and restored
  query-label cache invalidation now use authoritative cluster state.
- Unbounded temporal intervals no longer overflow duration calculations or
  require enumerating every temporal bucket; Allen relations preserve orientation.
- Rust 1.98 Clippy failures are resolved. Release publishing now depends on full
  CI validation and a matching tag/package version, including package and image
  builds. Shell validation checks every deployment script.

- Ingest acknowledgement now waits for a synchronous RocksDB WAL flush. The
  external ingest WAL is removed only after records and entity-resolution state
  have reached stable storage, closing a power-loss window for acknowledged data.
- Ingest WAL files now carry a version, payload length, and checksum. WAL rename,
  removal, and quarantine operations sync the parent directory; corrupt or
  truncated WALs fail shard startup and are preserved for recovery.
- Cross-shard merge redirects now persist before their RPC reports success and
  reload when persistent linker state is reconstructed.
- Persistent reset now clears DSU, tiered-index, and linker state together with
  records and assignments in one RocksDB write batch, preventing stale clusters
  from reappearing after reset or restart.
- Explicit checkpoints sync the RocksDB WAL before creating a checkpoint.
- Persistent shards no longer send large batches to in-memory partition stores.
  Acknowledged records and their entity-resolution assignments now survive a
  shard restart regardless of batch size.
- Duplicate source identities within one staged persistent batch are now
  idempotent instead of creating multiple records.
- Ingest APIs now return persistence failures instead of logging them and
  returning a successful acknowledgement.
- Large partitioned requests now reject invalid records consistently instead of
  silently dropping them during parallel conversion.
- Partitioned linking errors are propagated instead of producing cluster zero
  assignments.
- Persistent shard reset now releases the active RocksDB handle before reopening
  the database, avoiding self-deadlock on the database lock.
- Interner persistence watermarks now advance only after a successful RocksDB
  write, so retries cannot omit attribute or value mappings.
- Load-test sent and acknowledged counters no longer double-count generated
  records or classify failed requests as successful. Reported latency is now
  measured across successful RPCs instead of inferred from aggregate throughput,
  and incomplete runs exit unsuccessfully after writing their diagnostic report.

### Removed

- Removed the partitioned `ultra_fast` API, which skipped both storage and entity
  resolution and violated the engine's ingest contract.
- Removed standalone in-memory and single-node examples. The maintained example
  now demonstrates the persistent distributed deployment model.

### Changed

- Adopted Rust 2024 with a documented MSRV of Rust 1.88.
- Updated the active gRPC, terminal UI, cache, and compression dependencies and
  removed unused direct HTTP, TOML, and protobuf-types dependencies.
- The live distributed protocol is now 6. Upgrade routers, shards, and replicas
  together; WAL/checkpoint version 1 and durable reservation format 5 remain
  readable. Public router clients continue using the existing request contract.
- Selective queries reuse resolved membership and master only candidate entities;
  composite labels retain global prefix collision semantics. Shard requests run
  concurrently with bounded fan-out and entity hydration batches.
- Persistent ingestion lends staged records directly to parallel extraction,
  avoiding record cloning and correctly resolving batches above 100,000 records.
- The application lockfile is tracked for reproducible binary and container
  builds.
- Distributed integration tests now use temporary persistent shard stores; the
  in-memory store remains limited to unit tests.
- Replaced the non-durable historical performance claim with a verified
  five-shard measurement reported at release preparation: 50,598 records/second
  for a 10-million-record workload using synchronous durable ingestion. This
  historical result is not a capacity guarantee or a physical power-cut test;
  the later comparative audit used a separate one-million-record workload.
