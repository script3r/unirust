# Unirust

<div align="center">
  <img src="unirust.png" alt="Unirust Logo" width="350" height="350">
</div>

Temporal-first entity mastering and conflict resolution in Rust.

## Why Unirust

- Time-aware entity resolution across sources and perspectives.
- Conflict detection with explainable observations.
- Conflict-free golden records per cluster.
- Streaming ingest with deterministic clustering.

## How the basic example flows

The `examples/basic_example.rs` program streams five records, defines identity rules (name+email) and a strong identifier (SSN), resolves clusters, and reports conflicts.

```mermaid
flowchart LR
  subgraph Sources
    CRM[CRM record]
    CRM2[CRM record]
    ERP[ERP record]
    WEB[Web record]
    MOB[Mobile record]
  end

  CRM --> R1[Record + descriptors + interval]
  CRM2 --> R3[Record + descriptors + interval]
  ERP --> R2[Record + descriptors + interval]
  WEB --> R4[Record + descriptors + interval]
  MOB --> R5[Record + descriptors + interval]

  R1 --> Store[Store]
  R2 --> Store
  R3 --> Store
  R4 --> Store
  R5 --> Store

  Ontology[Ontology\nIdentity key: name+email\nStrong ID: SSN\nConstraint: unique email]
  Store --> Linker[Streaming linker\nlink_record]
  Ontology --> Linker

  Linker --> Clusters[Clusters]
  Clusters --> Conflicts[Conflict detection]
  Store --> Conflicts
  Ontology --> Conflicts

  Conflicts --> Observations[Observations]
  Observations --> Graph[Incremental knowledge graph]
  Graph --> Outputs[JSONL / DOT / PNG-SVG / text summary]
```

## Example output (derived entities)

```mermaid
flowchart LR
  subgraph Cluster0["Cluster 0"]
    R3["R3 (crm:crm_002)\nJane Smith\njane@example.com\n555-5678\n987-65-4321"]
  end

  subgraph Cluster1["Cluster 1"]
    R1["R1 (crm:crm_001)\nJohn Doe\njohn@example.com\n555-1234\n123-45-6789"]
    R2["R2 (erp:erp_001)\nJohn Doe\njohn@example.com\n555-9999\n123-45-6789"]
    R4["R4 (web:web_001)\nJohn Doe\njohn@example.com\n555-0000\n123-45-6789"]
  end

  subgraph Cluster2["Cluster 2"]
    R5["R5 (mobile:mobile_001)\nJohn Doe\njohn.doe@example.com\n555-0000\n123-45-6789"]
  end

  R1 ---|SAME_AS all_time| R2
  R1 ---|SAME_AS all_time| R4
  R2 ---|SAME_AS all_time| R4

  R1 -. "CONFLICTS attr:2 150-200" .- R2
  R1 -. "CONFLICTS attr:2 180-200" .- R4
  R2 -. "CONFLICTS attr:2 180-250" .- R4
```

## Quick start

```bash
cargo run --example basic_example
```

## Tuning

Streaming performance is controlled via `StreamingTuning`.

```rust
use unirust_rs::{StreamingTuning, TuningProfile, Unirust, Store};

let tuning = StreamingTuning::from_profile(TuningProfile::HighThroughput);
// Or customize individual fields:
// let tuning = StreamingTuning { candidate_cap: 2000, ..StreamingTuning::default() };

let mut unirust = Unirust::with_store_and_tuning(ontology, Store::new(), tuning);
```

Available profiles: `Balanced` (default), `LowLatency`, `HighThroughput`, `BulkIngest`, `MemorySaver`.

## Query master entities

```rust
use unirust_rs::{QueryDescriptor, QueryOutcome, Interval};

match unirust.query_master_entities(
    &[
        QueryDescriptor { attr: org_attr, value: org_value },
        QueryDescriptor { attr: role_attr, value: role_admin },
    ],
    Interval::new(0, 30)?,
)? {
    QueryOutcome::Matches(matches) => {
        // Each interval is guaranteed to map to a single master entity.
        // matches[i].golden includes the conflict-free golden descriptors for that cluster.
        // matches[i].cluster_key provides a human-friendly stable key for the cluster.
    }
    QueryOutcome::Conflict(conflict) => {
        // Overlapping clusters; conflict.descriptors includes overlap intervals.
    }
}
```

## Add to your project

```toml
[dependencies]
unirust-rs = "0.1.0"
```

## Development

```bash
cargo test
```

### Benchmarks and profiling

Benchmarks live in `benches/entity_benchmark.rs` and `benches/perf_suite.rs`.

```bash
cargo bench --bench entity_benchmark -- entity_resolution
cargo bench --bench entity_benchmark -- entity_resolution_large
cargo bench --bench entity_benchmark -- entity_resolution_sharded
```

Enable the lightweight profiler (feature-gated) to print hot-path timings:

```bash
UNIRUST_PROFILE=1 cargo bench --bench entity_benchmark --features profiling -- profile_5000
UNIRUST_PROFILE=1 cargo bench --bench entity_benchmark --features profiling -- profile_100k
```

Benchmark notes: see `BENCHMARKS.md`. Testing notes: see `TESTING.md`. Architecture notes: see `DESIGN.md`.

## License

MIT. See `LICENSE`.
