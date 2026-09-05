Audit and repairs based on merged commit `3d99fe0d27d7344b08b3bd5b23bb05c75479f1bd`, September 5, 2026.

Three parallel reviews covered entity resolution, persistence/recovery, and distributed correctness. All eight original defects were reproduced with persistent storage and have enabled regression coverage. Every accepted record still completes entity resolution before its record data commits.

| Defect and original trigger | Implemented repair | Regression suite |
| --- | --- | --- |
| A—B—C cross-shard identity chain merged incompatible strong IDs on A and C. | Check accumulated component observations before every union, across metadata chunks and prior reconciliation rounds; hydrate authoritative remote fragments. | `distributed_entity_regressions` |
| Reusing occupied `RecordId(42)` overwrote another durable record. | Reject occupied IDs in ordinary, explicit, batch, and staged storage APIs while preserving valid source-identity retries. | `durable_ingest_regressions` |
| A rejected batch left an orphan staged; a later request committed it without resolving it. | Discard aborted staging and rebuild partial derived state. Commit record data only after linking succeeds. Recover partial assignment writes from resolved durable records. | `durable_ingest_regressions` |
| Bounded interner caches changed durable values to `unknown` after restart. | Hydrate durable descriptor IDs before preparation; query lookup uses durable reverse indexes without allocating strings in the store. | `durable_ingest_regressions` |
| 300 identical observations permanently stopped later matching. | Avoid redundant interval-tree nodes; exhausted scan budgets use exact candidate fallback rather than permanently tainting a key. | `resolution_capacity_regressions` |
| Memory-saver left two matching source systems unmerged. | Respect disabled adaptive limits and use a positive fallback cap. | `resolution_capacity_regressions` |
| Cross-shard conjunction missed entities and returned incomplete golden data. | Discover candidate IDs, hydrate all fragments, then evaluate temporal conjunction and master conflicting attribute values globally. | `distributed_entity_regressions` |
| Cached-key insertion ignored hot/warm capacity limits. | Run amortized tier maintenance, persist complete buckets before eviction, promote complete buckets before updates, and propagate I/O/decode errors. | `resolution_capacity_regressions` |

Additional regression coverage includes all four ingest APIs, a 100,001-record batch, an aborted first ingest that already flushed persistent DSU nodes, partial durable assignment failure, tier eviction/update/restart, corrupt cold buckets, deferred merge membership, temporal adjacency, and local query equivalence before/after restart. Authoritative membership also fixes the previously unsupported cluster enumeration with persistent DSU.

Query execution reuses the linker's authoritative membership and masters golden data only for indexed candidate entities. Single-attribute labels are candidate-local. Composite labels retain a separate global key cache because their shortest unique prefixes depend on other entities; rebuilding that metadata after ingestion still requires a global pass. An unopened streaming engine retains the existing recovery/cache path.

Router queries dispatch up to 16 shard requests concurrently and hydrate at most 1,000 entity IDs per request. A barrier-based regression proves that both query phases dispatch concurrently. Strong-ID-only reconciliation hydration skips golden data and label construction. Persistent staging lends records directly to parallel extraction, avoiding an extra record clone and the old bounded-cache cutoff.

Baseline release-mode selective-query measurements used `PersistentStore`, batches of 1,000 distinct emails, and one matching entity:

| Stored records | First query before repair | Warm query before repair | After one unrelated insert before repair |
| ---: | ---: | ---: | ---: |
| 5,000 | 19.454 ms | 0.002 ms | 15.732 ms |
| 20,000 | 66.230 ms | 0.006 ms | 61.903 ms |
| 80,000 | 306.473 ms | 0.003 ms | 300.151 ms |

After repair, the same release-mode diagnostic measured:

| Stored records | First query after repair | Warm query after repair | After one unrelated insert after repair |
| ---: | ---: | ---: | ---: |
| 5,000 | 0.055 ms | 0.006 ms | 0.010 ms |
| 20,000 | 0.022 ms | 0.005 ms | 0.007 ms |
| 80,000 | 0.029 ms | 0.005 ms | 0.008 ms |

These timings are local diagnostics, not a universal latency guarantee; warm-cache microsecond differences are below meaningful precision for this single-run measurement. The standard historical 410K records/second figure was not reproduced on this machine; comparative throughput uses the same five persistent shards and workload for both revisions.

The comparative ingest run used five persistent shards with the high-throughput profile, 1,000,000 records, 16 streams, batches of 5,000, 10% overlap, and seed 42:

| Revision | Acknowledged records | Throughput | Average RPC latency | Failed batches |
| --- | ---: | ---: | ---: | ---: |
| Merged baseline | 1,000,000 | 43,415 records/s | 1,585 ms | 0 |
| Audit repairs | 1,000,000 | 43,939 records/s | 1,657 ms | 0 |

Ingest throughput was effectively unchanged in this single comparison; the small throughput and latency differences do not establish a statistically significant change. This workload measures durable ingest, not a universal cross-shard query or reconciliation latency. The selective-query improvement is measured separately above.

CI now passes the Rust 1.98 lints that failed on main (`chunks_exact_to_as_chunks` and `manual_slice_fill`). PR validation explicitly installs native build dependencies and verifies the release package. Release publishing depends on the reusable complete CI workflow, including MSRV, tests, lint, manifests, and production image, and rejects tags that do not match the package version. Workflow syntax is checked with actionlint.

Validation commands:

```sh
cargo test --locked --all-features
cargo +1.98.0 clippy --locked --all-targets --all-features -- -D warnings
cargo +1.88.0 check --locked --all-targets --all-features
cargo fmt --check
cargo package --locked
cargo test --release --test audit_performance -- --ignored --nocapture
```

The correctness suites run by default. Only the manual timing diagnostic is ignored; it has no machine-dependent timing assertion. Distributed benchmarks now include persistent shard ingestion rather than treating the in-memory partition path as evidence of production throughput.
