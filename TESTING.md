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

## Performance regression loop (fast)

```bash
cargo bench --bench perf_suite -- ingest_smoke
cargo bench --bench perf_suite -- query_smoke
cargo bench --bench perf_suite -- ingest_persistent_smoke
```

## Latency regression gates (optional)

```bash
UNIRUST_ENABLE_LATENCY_GATES=1 UNIRUST_GATE_RECORDS=50000 UNIRUST_GATE_QUERIES=1000 \
  UNIRUST_P95_US=500 UNIRUST_P99_US=1000 cargo test --test query_latency_regression
```

## Full benchmark runs

```bash
cargo bench --bench entity_benchmark -- entity_resolution
cargo bench --bench entity_benchmark -- entity_resolution_sharded
cargo bench --bench entity_benchmark -- entity_resolution_large
```

## Profiling hot paths

```bash
UNIRUST_PROFILE=1 cargo bench --bench entity_benchmark --features profiling -- profile_5000
UNIRUST_PROFILE=1 cargo bench --bench entity_benchmark --features profiling -- profile_100k
```
