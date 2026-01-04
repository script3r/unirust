# Agent Guidelines

This document provides guidance for AI agents working on the Unirust codebase.

## Testing

### Unit Tests
- Use in-memory shards for fast, isolated testing.

### Integration Tests and Benchmarks
- Always use persistent (on-disk) shards—never in-memory shards.
- This ensures tests reflect real-world storage behavior and performance characteristics.

### Benchmark Configuration
- Use 5 shards with a batch size of 5000.

### Running the Primary Benchmark

The most important benchmark for evaluating performance:

1. Launch the cluster:
   ```bash
   ./scripts/cluster.sh
   ```

2. Run the load test:
   ```bash
   cargo run --release --bin unirust_loadtest --features test-support -- --count 10000000 --router http://127.0.0.1:50060 --headless
   ```

3. Evaluate the outputted log file for performance numbers.

## Before Completing a Task

Always run and fix any issues from the following before declaring a task complete:

1. `cargo fmt` — Fix all formatting issues.
2. `cargo clippy` — Resolve all linter warnings.
3. `cargo test` — Ensure all tests pass.
