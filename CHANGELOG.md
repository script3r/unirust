# Changelog

All notable changes to this project are documented here. This project follows
Semantic Versioning.

## [Unreleased]

### Fixed

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
- The package version is now `0.2.0` in preparation for the next release.
- The application lockfile is tracked for reproducible binary and container
  builds.
- Distributed integration tests now use temporary persistent shard stores; the
  in-memory store remains limited to unit tests.
- Replaced the non-durable historical performance claim with a verified
  five-shard, power-loss-durable baseline of 50,598 records/second for the
  documented 10-million-record workload.
