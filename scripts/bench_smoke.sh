#!/usr/bin/env bash
set -euo pipefail

cargo bench --bench perf_suite -- ingest_smoke
cargo bench --bench perf_suite -- query_smoke
cargo bench --bench perf_suite -- ingest_persistent_smoke
