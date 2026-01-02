# Benchmarks

## Overview

Benchmarks live in `benches/entity_benchmark.rs` and focus on streaming throughput and
overlap-heavy workloads. The suites are split into standard streaming, large-scale runs,
and sharded ingest.

## Running benchmarks

```bash
cargo bench --bench entity_benchmark -- entity_resolution
cargo bench --bench entity_benchmark -- entity_resolution_large
cargo bench --bench entity_benchmark -- entity_resolution_sharded
```

## Profiling hot paths

The lightweight profiler is feature-gated. Enable it for high-level timing summaries.

```bash
UNIRUST_PROFILE=1 cargo bench --bench entity_benchmark --features profiling -- profile_5000
UNIRUST_PROFILE=1 cargo bench --bench entity_benchmark --features profiling -- profile_100k
```

## Notes

- High-overlap runs are intentionally heavy because they stress candidate scans.
- Sharded benchmarks replay records in deterministic order during reconciliation.
- Tune streaming performance with `StreamingTuning` (see `README.md`).
