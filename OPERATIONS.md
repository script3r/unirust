# Operations Guide

This document captures the production operations baseline for Unirust.

## Metrics

Unirust exposes gRPC metrics via `GetMetrics` on both `ShardService` and `RouterService`.

Key fields:
- `ingest_requests`, `ingest_records`, `query_requests`
- `ingest_latency`, `query_latency` (count/total/max in micros)
- `store.running_compactions`, `store.running_flushes`
- `shards_reporting` (router only)

Example (router):

```
grpcurl -plaintext -import-path proto -proto proto/unirust.proto \
  -d '{}' 127.0.0.1:50060 unirust.RouterService/GetMetrics
```

Example (shard):

```
grpcurl -plaintext -import-path proto -proto proto/unirust.proto \
  -d '{}' 127.0.0.1:50061 unirust.ShardService/GetMetrics
```

Notes:
- Router metrics aggregate shard metrics (counts and latencies).
- Use `uptime_seconds` with counts to compute average QPS.

## Alerting + Runbooks

Suggested alerts:
- Query latency max exceeds 1s for > 5m.
- Ingest latency max exceeds 2s for > 5m.
- Running compactions stuck > 0 for > 10m (possible write stall).
- Shard unavailable / router health check fails.

Runbook quick checks:
1) Verify shard health: `HealthCheck` on each shard.
2) Check `GetMetrics` for compaction pressure and latency growth.
3) If using RocksDB: verify disk space and file descriptor limits.
4) If ingest latency spikes: reduce ingest concurrency or pause ingestion.
5) For sustained query latency: increase shard count or scale up CPUs.

## SLOs + Capacity Planning

Baseline (per shard, 32GB RAM):
- Target p95 query latency: <= 200ms under steady load.
- Target p95 ingest latency: <= 500ms (batch size <= 1000).
- Availability: 99.9% per shard, 99.5% end-to-end.

Capacity hints:
- Scale shard count by record volume and query concurrency.
- Reserve ~30% RAM for RocksDB cache and memtables.
- Avoid compaction stalls by maintaining 20% free disk space.
- Use periodic checkpoints + snapshots for restore drills.

## Storage Tuning (RocksDB)

Environment overrides (values are in MB unless noted):
- `UNIRUST_BLOCK_CACHE_MB` (default 512)
- `UNIRUST_WRITE_BUFFER_MB` (default 128)
- `UNIRUST_MAX_WRITE_BUFFERS` (default 4)
- `UNIRUST_TARGET_FILE_MB` (default 128)
- `UNIRUST_LEVEL_BASE_MB` (default 512)
- `UNIRUST_BLOOM_BITS_PER_KEY` (default 10.0)
- `UNIRUST_MEMTABLE_PREFIX_BLOOM_RATIO` (default 0.1)
- `UNIRUST_RATE_LIMIT_MBPS` (default 0, disabled)
