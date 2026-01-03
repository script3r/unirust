# Benchmarks

## Overview

Four benchmark tiers organized by purpose and runtime:

| Benchmark | Runtime | Purpose |
|-----------|---------|---------|
| `bench_quick` | ~30s | CI and development feedback |
| `bench_scale` | ~3-5min | Pre-merge validation |
| `bench_micro` | ~1min | Component internals |
| `bench_diagnostic` | One-shot | Deep bottleneck analysis |

## Quick Benchmarks (~30s)

Fast feedback for development cycles. Tests core ingest, query, and graph operations.

```bash
cargo bench --bench bench_quick
```

Includes:
- `quick/ingest/memory` - In-memory store ingest (2K records)
- `quick/ingest/persistent` - Persistent store ingest (2K records)
- `quick/query/by_email` - Query latency
- `quick/graph/stream_with_graph` - Graph update performance

## Scale Benchmarks (~3-5min)

Pre-merge validation at configurable scale. Uses Criterion for statistical analysis.

```bash
cargo bench --bench bench_scale
```

Configure via environment variables:
- `UNIRUST_SCALE_COUNT` - Total records (default: 100,000)
- `UNIRUST_SCALE_BATCH` - Batch size (default: 10,000)
- `UNIRUST_SCALE_OVERLAP` - Overlap probability (default: 0.01)

Examples:
```bash
# Default 100K records
cargo bench --bench bench_scale

# 500K records with 5% overlap
UNIRUST_SCALE_COUNT=500000 UNIRUST_SCALE_OVERLAP=0.05 cargo bench --bench bench_scale

# 1M records
UNIRUST_SCALE_COUNT=1000000 cargo bench --bench bench_scale
```

Includes:
- `scale/memory/ingest` - In-memory throughput at scale
- `scale/persistent/ingest` - Persistent store throughput (BillionScale tuning)
- `scale/overlap/*` - Performance across overlap probabilities (1%, 10%, 25%)

## Microbenchmarks (~1min)

Component-level benchmarks for internal hot paths.

```bash
cargo bench --bench bench_micro
```

Includes:
- `dsu/*` - DSU find, merge, get_clusters operations
- `temporal/*` - Interval operations, coalescing
- `store/*` - Record lookups
- `conflicts/*` - Detection algorithms

## Diagnostic Tool (One-shot)

Human-readable diagnostic output for deep analysis. Not Criterion-based.

```bash
cargo bench --bench bench_diagnostic 2>&1 | tee diagnostic.log
```

Diagnostics:
- DSU scaling: find/merge ops at 10K-1M scale
- Index scaling via full pipeline
- Full pipeline throughput and degradation
- Memory pressure effects on batch timing

## Profiling

Enable the lightweight profiler for hot-path timings:

```bash
UNIRUST_PROFILE=1 cargo bench --bench bench_scale --features profiling
```

## Notes

- High-overlap runs stress candidate scans intentionally
- BillionScale tuning uses persistent DSU for large datasets
- Tune streaming performance with `StreamingTuning` profiles
