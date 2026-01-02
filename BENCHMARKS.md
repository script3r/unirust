# Benchmarks

## Overview

Benchmarks live in `benches/entity_benchmark.rs` (macro-scale streaming throughput) and
`benches/perf_suite.rs` (fast feedback loop for ingest, query, and persistence). Use
`perf_suite` during tight optimization cycles and `entity_benchmark` for full runs.

## Running benchmarks

Fast perf loop (smoke-level scale):

```bash
cargo bench --bench perf_suite -- ingest_smoke
cargo bench --bench perf_suite -- query_smoke
cargo bench --bench perf_suite -- ingest_persistent_smoke
```

Or run the bundled script:

```bash
./scripts/bench_smoke.sh
```

Scale ingest harness (configurable):

```bash
UNIRUST_SCALE_COUNT=100000000 UNIRUST_SCALE_BATCH=100000 UNIRUST_SCALE_OVERLAP=0.01 \
  cargo bench --bench scale_benchmark -- scale_ingest
```

Persistent scale runner (RocksDB-backed, 32GB RAM defaults):

```bash
UNIRUST_SCALE_COUNT=10000000 UNIRUST_SCALE_BATCH=100000 UNIRUST_SCALE_OVERLAP=0.01 \
  UNIRUST_SCALE_DATA_DIR=./scale_data UNIRUST_SCALE_RESET=1 ./scripts/scale_persistent.sh
```

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
- `perf_suite` uses smaller datasets to keep iteration under a few seconds.
