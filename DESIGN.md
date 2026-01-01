# Design

## Goals

- Stream records and resolve entity clusters with interval-aware constraints.
- Detect conflicts and preserve temporal provenance in the knowledge graph.
- Provide conflict-aware query results for master entities over time.
- Keep storage pluggable (in-memory now, Minitao persistence supported).

## Core data flow

1. Ingest records into the store.
2. Stream records through the linker to assign clusters.
3. Detect conflicts (direct, indirect, constraint violations).
4. Incrementally update the knowledge graph.
5. Persist graph state to Minitao (optional).
6. Serve queries for descriptors over time.

## Key modules

- `store`: in-memory record storage and attribute/value interning.
- `linker`: streaming entity resolution with temporal DSU and identity-key indexing.
- `conflicts`: conflict detection and constraint violations.
- `graph`: knowledge graph snapshots and incremental updates.
- `query`: conflict-aware master-entity queries.
- `minitao_store` / `minitao_grpc`: persistence to Minitao (local or gRPC).

## Streaming resolution

- `StreamingLinker` assigns each new record to an existing cluster or creates a new one.
- Identity keys generate candidate pairs; merges are blocked when strong identifiers conflict over overlapping intervals.
- Clusters are maintained in a temporal DSU with merge guards.
- Conflict splitting is applied only in narrow cases required by tests.

## Conflict model

- Direct conflicts: overlapping descriptors with different values.
- Indirect conflicts: suppressed merges due to strong identifier conflicts.
- Constraint violations: unique and unique-within-perspective constraints.

## Knowledge graph

- Nodes: record nodes and cluster nodes.
- Edges: SAME_AS for merges and CONFLICT edges for conflict observations.
- Graph updates are incremental and derived from streaming clusters + observations.
- Cluster nodes persist a golden copy: conflict-free attribute/value intervals derived from cluster records.
- Cluster nodes include a stable cluster key derived from entity-type key attributes (or identity keys by default).

## Query model

`query_master_entities` accepts a set of descriptors and a time interval.

- Computes per-cluster intervals where all descriptors apply.
- Coalesces intervals per cluster.
- Returns a `QueryOutcome`:
  - `Matches`: non-overlapping cluster intervals.
  - `Conflict`: overlapping clusters with per-descriptor overlap intervals.
- Match entries include the cluster golden copy filtered to the match interval for rendering.
- Match entries include a `cluster_key` label to make clusters human-readable.
- Cluster keys use the minimal attribute prefix that makes the cluster unique, plus a 2-character hash suffix.

The query outcome enforces the invariant: at any time instant, at most one master entity is returned.

## Minitao persistence

- Storage writer maps graph nodes to Minitao objects.
- SAME_AS associations are aggregated by record pair.
- Conflict edges are stored as objects with associations from each record to the conflict object.
- gRPC writer supports the same payloads for server-backed persistence.

## Invariants

- Intervals are half-open [start, end) and must satisfy start < end.
- Strong identifiers may not conflict within overlapping intervals.
- Query results must not return multiple masters for the same time instant.

## Performance notes

- Identity key lookups use borrowed keys to avoid allocations.
- Streaming resolution prioritizes incremental updates over batch processing.
- Benchmarks isolate linker cost from record generation.

## Testing

- Unit tests cover conflicts, DSU behavior, indexing, graph export, and queries.
- E2E script validates gRPC persistence against a live Minitao server.
