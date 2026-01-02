# Distributed Processing Plan

This document describes the concrete steps to make Unirust distributable across multiple servers
using gRPC (tonic), and how we will test it end to end.

## Phase 1: Minimal distributed ingest + query (this implementation)

1. **Define the gRPC contract**
   - Add `proto/unirust.proto` with `ShardService` (ingest + query) and `RouterService` (ingest + query).
   - Records use string attributes/values; shard nodes intern them locally to avoid interner coupling.
   - Queries return matches and conflicts with cluster keys + golden descriptors (not raw record IDs).

2. **Introduce distributed ontology config**
   - Add `DistributedOntologyConfig` (string-based identity keys, strong identifiers, constraints).
   - Provide a builder that converts config → `Ontology` via a shard-local `Store` interner.
   - Use the same config in the router to hash records to shards by identity keys.

3. **Shard node service**
   - Each shard runs its own `Unirust` instance, `Store`, and tuning profile.
   - `IngestRecords` converts inputs to `Record`, streams via `stream_record_update_graph`,
     and returns cluster assignments + cluster keys.
   - `QueryEntities` runs `query_master_entities` and returns golden descriptors and cluster keys.

4. **Router/coordinator service**
   - Router hashes each record to a shard, batches per-shard RPCs, and merges results in request order.
   - Router fans out queries to all shards, merges matches, and detects cross-shard overlap conflicts.
   - If any shard reports a conflict, router returns that conflict; otherwise it checks overlaps
     between matches from different shards and returns a conflict if needed.

5. **CLI binaries**
   - `unirust_shard`: runs a shard gRPC server (accepts a tuning profile flag).
   - `unirust_router`: runs a router gRPC server and connects to shard nodes.
   - Both accept a JSON ontology config file; otherwise they start with an empty ontology and must receive `SetOntology` over gRPC.

6. **End-to-end tests**
   - Integration test starts two shard servers + router in-process.
   - Client streams records through the router.
   - Test queries verify: (a) results are returned, (b) overlaps are correctly marked as conflicts,
     (c) matches include golden descriptors + cluster keys.

## Ontology config (JSON)

```json
{
  "identity_keys": [
    { "name": "email", "attributes": ["email"] }
  ],
  "strong_identifiers": ["ssn"],
  "constraints": [
    { "name": "unique_email", "attribute": "email", "kind": "unique" }
  ]
}
```

## Podman demo

```bash
scripts/podman_cluster.sh start
scripts/podman_demo.sh
scripts/podman_cluster.sh stop
```

## Phase 2: Production hardening (next steps)

1. **Durable ingestion**
   - Add a stream log (Kafka/Redpanda) so router produces and shards consume.
   - Ensure idempotent ingestion with record UUIDs.

2. **Cross-shard reconciliation**
   - Add a background reconciliation service to re-hash and re-link on schema changes or hot keys.
   - Store versioned cluster snapshots.

3. **Query acceleration**
   - Maintain shard-local indexes for query descriptors.
   - Add a coordinator cache for recent query intervals.

4. **Fault tolerance**
   - Retry with backoff on shard RPC failure.
   - Add health checks and shard drain/rebalance.

5. **Observability**
   - Structured logging and metrics for stream/merge/query times.
   - Tracing across router ↔ shard RPCs.

## Testing strategy

- **Unit tests** for hashing, config-to-ontology conversion, and query merge logic.
- **Integration test** to bring up shards + router and validate:
  - Record routing is stable based on identity key values.
  - Query matches never overlap unless returned as a conflict.
  - Golden descriptors are non-conflicting and time-filtered.
