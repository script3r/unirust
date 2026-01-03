# Testing

## Fast loop

```bash
cargo test
```

## Distributed regression suite

```bash
cargo test --test distributed_e2e
cargo test --test distributed_rebalance
cargo test --test distributed_rebalance_stream
cargo test --test distributed_router_admin
cargo test --test distributed_ingest_stream
cargo test --test distributed_ingest_batch_url
cargo test --test distributed_metrics
cargo test --test distributed_apply_ontology
cargo test --test distributed_conflicts_presets
cargo test --test distributed_wal_replay
```

## Performance regression loop

```bash
cargo bench --bench bench_quick
```

## Latency regression gates (optional)

```bash
UNIRUST_ENABLE_LATENCY_GATES=1 UNIRUST_GATE_RECORDS=50000 UNIRUST_GATE_QUERIES=1000 \
  UNIRUST_P95_US=500 UNIRUST_P99_US=1000 cargo test --test query_latency_regression
```

## Full benchmark runs

```bash
cargo bench --bench bench_quick       # ~30s  - CI/dev feedback
cargo bench --bench bench_scale       # ~3-5m - Pre-merge validation
cargo bench --bench bench_micro       # ~1m   - Component internals
cargo bench --bench bench_diagnostic  # One-shot deep analysis
```

## Scale benchmarks with custom parameters

```bash
UNIRUST_SCALE_COUNT=500000 UNIRUST_SCALE_OVERLAP=0.05 cargo bench --bench bench_scale
```

## Profiling hot paths

```bash
UNIRUST_PROFILE=1 cargo bench --bench bench_scale --features profiling
```
