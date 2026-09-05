# Algorithmic performance analysis — September 2026

This analysis compares the persistent runtime at `35b902b` with the algorithmic
changes at `8dc532e`. The changes remove repeated work while preserving entity
resolution, temporal guards, canonical membership and synchronous durable
acknowledgements. No candidate cap was lowered, no sampling was added, and no
persistence synchronization was removed.

## Findings and implemented changes

| Operation | Previous cost | Implemented algorithm and cost |
| --- | --- | --- |
| Local merge whose canonical global ID does not change | Scans all global-ID mappings and boundary entries on every merge | Prove the redirect set contains only the canonical ID and omit those identity assignments and impossible boundary updates. Constant work in unrelated cluster/boundary counts; real ID-changing bridges keep the existing reconciliation path. |
| Strong-ID history accumulation | Re-coalesces every stored value of a touched attribute after each merge; growing chronological histories accumulate quadratic work | Merge only incoming values. Chronological intervals append/coalesce at the tail; arbitrary sorted histories use a linear merge. Single out-of-order intervals use binary search but can still shift a vector suffix. |
| Cross-shard temporal candidate discovery | Tests every boundary-entry pair, including disjoint times and same-shard entries | Sweep starts, expire ended intervals and group active entries by shard: `O(B log B + P)` for valid intervals, where `P` is the actual overlapping cross-shard pair count. A common temporal intersection permits direct shard-group pair enumeration. |
| Golden attribute mastering | For each distinct value, copies and subtracts every competing value's intervals | Sweep temporal boundaries once per attribute and emit spans with exactly one distinct active value: `O(I log I + output)` after collecting/coalescing inputs. |
| Query temporal conjunction | Cartesian interval intersection, followed by coalescing | Normalize interval sets, then intersect with two pointers. `O(a+b)` for already normalized inputs; otherwise sorting/coalescing costs apply. |
| Constraint overlap times | Cartesian descriptor-interval intersection for every value pair | Reuse the normalized interval-set intersection. Existing value-pair and participant-reporting semantics remain unchanged. |

Here, `B` counts boundary entries for one identity key, `I` counts temporal
intervals, and `a`/`b` count the two interval lists. These bounds describe the
changed operations, not complete ingest/query latency. Storage, descriptor
hydration, output construction and component conflict guards still have costs.

### Why the shortcuts preserve results

For unchanged canonical IDs, every matching global-map assignment was `id := id`.
The old boundary predicate required both membership in the one-element canonical
set and inequality with that canonical ID, so it could never select an entry.
The optimization retains anchor updates, redirect cleanup and the new root's
mapping, then returns before scanning unrelated state.

The internal strong-ID summary records whether its intervals satisfy the sorted,
valid-interval invariant. Validation visits only each incoming record's summary;
it does not rescan accumulated history. Public `StrongIdSummary::merge` remains
general. Empty or reversed public intervals retain the legacy merge path through
private validity metadata, which is recomputed during recovery and is not stored
in the durable format.

The boundary sweep changes pair enumeration, not the matching predicate or
component guards. It retains pair orientation, candidate multiplicity, conflict
metrics and canonical merge ordering. Conflict vectors already had unspecified
ordering through hash-set traversal; differential tests compare their exact
multisets. Malformed public intervals use the original pairwise predicate path.
When all valid intervals share a common intersection, every cross-shard pair
must overlap, which justifies the dense fast path without dropping any pair.

Golden mastering first coalesces intervals for each distinct value. The sweep
then applies all events at a timestamp before examining the following half-open
span. A span is retained exactly when one value is active. No endpoint arithmetic
is required, including at the extreme `i64` endpoints. Malformed public inputs
retain legacy subtraction behavior. Query/constraint intersection normalizes
unordered inputs and preserves the union of their pairwise intersections.

## Evidence from profiling

The initial diagnostic used five persistent shards on an Apple M5 with ten
logical CPUs and 32 GiB RAM. A separate macOS `sample` run captured the router
and one shard during ingestion. Unchanged-global-ID reconciliation and its
boundary-map iterator/filter frames were prominent active Rust frames in the
shard sample, corroborating the source-level full-state scan. Router samples
also showed SHA-256 work, allocation/copying and boundary reconciliation.

Sampling was performed in a separate run and excluded from comparative
throughput measurements. The sample includes waiting threads, so its raw sample
counts are not presented as percentages of CPU utilization or time saved.

## Focused persistent measurements

All times below are milliseconds. Persistence remains enabled. Query diagnostics
time queries after ingestion; ingest diagnostics include the persistent ingest
operation. These measurements were taken during implementation to isolate each
changed path, rather than treating every intermediate diagnostic as a benchmark
of the final packaged release.

| Workload | Size | Before | After | Ratio |
| --- | ---: | ---: | ---: | ---: |
| Duplicate merges with boundary tracking | 6,000 entities | 228.044 | 50.722 | 4.50× |
| Duplicate merges with boundary tracking | 12,000 entities | 811.066 | 105.187 | 7.71× |
| One strong-ID value with disjoint chronological observations | 6,000 records | 84.753 | 42.354 | 2.00× |
| One strong-ID value with disjoint chronological observations | 12,000 records | 262.874 | 68.746 | 3.82× |
| Query one attribute's distinct historical values | 1,000 values | 36.541 | 0.574 | 63.66× |
| Query one attribute's distinct historical values | 2,000 values | 145.139 | 1.104 | 131.47× |
| Query one attribute's distinct historical values | 4,000 values | 571.773 | 2.191 | 260.96× |
| Disjoint query conjunction | 2,000 intervals per descriptor | 3.842 | 2.421 | 1.59× |
| Disjoint query conjunction | 4,000 intervals per descriptor | 9.482 | 3.288 | 2.88× |
| Disjoint query conjunction | 8,000 intervals per descriptor | 29.287 | 5.717 | 5.12× |
| Disjoint constraint histories | 1,000 intervals per value | 0.607 | 0.213 | 2.85× |
| Disjoint constraint histories | 2,000 intervals per value | 1.896 | 0.440 | 4.31× |
| Disjoint constraint histories | 4,000 intervals per value | 6.509 | 0.835 | 7.80× |

Query values are medians of three repetitions; constraint values are medians of
five. Linker diagnostics time one fresh persistent workload per reported size.
The strong-ID history diagnostic disables boundary tracking to isolate summary
maintenance; the duplicate-merge diagnostic enables tracking. The large query
ratio applies to the deliberately long-history case, not all queries.

The old duplicate-merge workload took 3.56 times as long when size doubled; the
replacement takes 2.07 times as long. The historical-value query's previous
roughly fourfold cost per doubling is removed. Timings at these sizes support
the source-level complexity findings without establishing universal capacity or
latency guarantees.

## Reconciliation candidate enumeration

This diagnostic compares the retained quadratic reference and optimized pair
enumerator within one release-mode binary. Values are the best of three runs,
not confidence intervals. Both paths must emit the same candidate count.
These are component measurements; they exclude RPCs and full component guards.

| Case | Entries | Candidate pairs | Pairwise ms | Optimized ms |
| --- | ---: | ---: | ---: | ---: |
| Sparse temporal windows | 1,000 | 500 | 0.778 | 0.110 |
| Sparse temporal windows | 4,000 | 2,000 | 8.617 | 0.222 |
| Sparse temporal windows | 16,000 | 8,000 | 98.985 | 0.572 |
| Common temporal overlap | 1,000 | 333,333 | 0.225 | 0.095 |
| Common temporal overlap | 4,000 | 5,333,333 | 5.807 | 1.364 |
| Dense, partially overlapping windows | 1,000 | 249,833 | 0.230 | 0.221 |
| Dense, partially overlapping windows | 4,000 | 3,999,333 | 5.915 | 2.747 |
| Entries from one physical shard | 16,000 | 0 | 73.337 | 0.009 |

A first implementation made the 1,000-entry common-overlap case about 20% slower.
The exact common-intersection path removed that measured regression. Tiny dense
cases were also checked, but sub-microsecond rounded results do not establish a
meaningful speed ratio. Dense candidate output is still quadratic when the data
contains quadratically many overlapping pairs.

## End-to-end results

Frozen baseline and optimized binaries were run in alternating order on fresh
five-shard persistent stores. Each one-million-record condition has three runs
per revision. The two-million-record condition has one run per revision, so its
result is a scaling diagnostic rather than a repeated estimate. All use 16
streams, batches of 5,000, seed 42 and the same generated dataset per condition.

| Workload | Before records/s | After records/s | Throughput change | Before average RPC ms | After average RPC ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1M records, 10% generated overlap (medians) | 43,573 | 63,526 | +45.8% | 1,621.14 | 1,006.10 |
| 1M records, 0% generated overlap (medians) | 47,638 | 69,991 | +46.9% | 1,451.32 | 1,003.51 |
| 2M records, 10% generated overlap (one run) | 18,490 | 34,150 | +84.7% | 4,047.91 | 2,163.94 |

All 16 million records across these comparative runs were acknowledged; every
run reported zero failed batches. The three optimized throughput samples exceed
all three baseline samples in each one-million-record condition. These small
samples are not statistical confidence intervals. The 0% setting controls the
generator's overlap probability; it does not assert that the ontology can never
find a shared identity in the generated data.

The larger run improves by about 85%, but throughput still falls as the dataset
grows in both revisions. The whole engine is not claimed to scale linearly.
The post-change sample no longer has unchanged-ID boundary scans among its
leading frames; persistent I/O and RocksDB operations are prominent. A separate
investigation is needed to attribute the larger workload's remaining slowdown
between storage/cache behavior and other data-dependent costs.

[All raw run summaries](performance-results-2026-09-05.tsv) include the initial
exploratory baseline runs and separate sampled diagnostics as well as the paired
and scaling comparisons. Sampled runs are excluded from the table above. No
Cargo builds or other agent benchmarks ran during these measurements.

## Cluster workload and reproduction

The end-to-end runner is [scripts/benchmark_persistent.py](../scripts/benchmark_persistent.py).
It starts already-built binaries on fresh persistent directories and free
loopback ports, uses `examples/loadtest-ontology.json`, fixes the high-throughput
profile and seed, disables automatic checkpoints, and validates successful exit
and complete acknowledgements. It removes `UNIRUST_*` environment overrides and
records binary/ontology hashes plus relevant runtime settings. All five shards,
the router and client run on the same machine; this is not a multi-host capacity
claim. Local colocated checkpoints are a test convenience, not volume-loss
protection.

Build each revision separately and copy its four binaries to an immutable
directory before measuring. Do not run Cargo builds or other benchmarks during
a measured run:

```sh
cargo build --release --locked --features test-support \
  --bin unirust_shard --bin unirust_router \
  --bin unirust_loadtest --bin unirust_healthcheck

python3 scripts/benchmark_persistent.py \
  --bin-dir /path/to/frozen-binaries \
  --output-dir /path/to/new-results-directory \
  --runs 3 --count 1000000 --shards 5 --streams 16 --batch 5000 \
  --overlap 0.1 --seed 42 --discard-data
```

The output directory must not exist. `--discard-data` removes only generated
shard databases after a successful run; logs, configuration and `summary.tsv`
remain. Without that flag, databases remain available for inspection. Optional
`--sample-seconds 5` requires macOS `sample`; keep those runs separate from
throughput comparisons.

Repeat frozen before/after binaries in alternating order. Compare both the
standard 10% overlap workload and a 0% generated-overlap control. Record all
runs and acknowledgement failures, not only the fastest result. A larger
workload checks whether speedup survives growth beyond the focused fixtures.

## Correctness and remaining limits

Enabled regressions cover persistent DSU/tiered-index and in-memory DSU/index
backends over persistent records; out-of-order and malformed strong-ID histories;
unchanged and bridge canonical IDs; boundary metadata and recovery; exact golden
values and intervals; duplicate, nested, adjacent and extreme endpoints; query
intersection against a pairwise oracle; and constraint intervals/participants
before and after restart. A three-shard persistent fixture exercises 64 temporal
windows, temporal strong-ID conflicts and canonical queries. The fixture uses
one perspective to avoid conflating reconciliation with the existing local
cross-perspective tainted-key policy.

The focused timing tests are ignored in normal CI and have no machine-dependent
speed assertions. Correctness regressions run by default. Commands:

```sh
cargo test --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo fmt --check
RUSTDOCFLAGS='-D warnings' cargo doc --locked --all-features --no-deps

cargo test --release --locked --test streaming_merge_performance_regressions \
  -- --ignored --nocapture --test-threads=1
cargo test --release --locked --test temporal_mastering_regressions \
  --test constraint_interval_performance -- --ignored --nocapture --test-threads=1
cargo test --release --locked --lib boundary_pair_sweep_benchmark \
  -- --ignored --nocapture
```

Remaining targets require additional design or evidence:

- Chronologically increasing starts can still degenerate the identity interval
  tree. Balancing it changes candidate traversal order, which can affect
  order-sensitive matching. A replacement needs an explicit ordering contract
  and differential resolution proof.
- Distributed boundary updates still export/sort a cluster's strong-ID history.
  Long-history distributed ingestion can therefore retain repeated work even
  after local summary maintenance improves.
- Composite display labels still require a global collision-cache rebuild after
  writes. Query-result size, golden output and full-history recovery remain
  data dependent.
- Exact component guards and dense candidate output can still be expensive.
  Hashing, serialization, persistent writes and RPC coordination remain linear
  work or storage/network costs; their presence does not justify weakening
  durability or changing signatures.
- Authoritative membership and conflict metadata still grow in memory. These
  changes do not establish a billion-record memory bound or exhaustive matching
  beyond the existing candidate extraction/sampling policies.
