# Unirust

<div align="center">
  <img src="unirust.png" alt="Unirust Logo" width="350" height="350">
</div>

A temporal-first entity mastering and conflict-resolution engine in Rust.

## What it does

- Model records with explicit validity intervals.
- Resolve entities across multiple sources and perspectives.
- Detect direct and indirect conflicts.
- Export a knowledge graph for auditing and visualization.

## How the basic example flows

The `examples/basic_example.rs` program creates five person records from CRM/ERP/Web/Mobile, defines identity rules (name+email) and a strong identifier (SSN), resolves clusters, detects conflicts, and exports a graph.

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
  Store --> Linker[Entity resolution\nbuild_clusters_optimized]
  Ontology --> Linker

  Linker --> Clusters[Clusters]
  Clusters --> Conflicts[Conflict detection]
  Store --> Conflicts
  Ontology --> Conflicts

  Conflicts --> Observations[Observations]
  Observations --> Graph[Knowledge graph]
  Graph --> Outputs[JSONL / DOT / PNG-SVG / text summary]
```

## Quick start

```bash
cargo run --example basic_example
```

## Add to your project

```toml
[dependencies]
unirust = "0.1.0"
```

## Development

```bash
cargo test
```

## License

MIT. See `LICENSE`.
