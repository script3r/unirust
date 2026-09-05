# GPU acceleration: unimplemented feasibility notes

Unirust v0.2.0 has no GPU backend, GPU feature flag or GPU build dependency.
The production ingestion and query paths execute on the CPU. This document
records possible experiments; it does not describe shipped functionality.
No GPU throughput, latency, speedup or cost measurements are available for
Unirust. Earlier numerical projections in this file were unsupported and have
been removed.

## Current execution path

Persistent shards call `Unirust::stream_records`. Large batches use Rayon for
identity-key extraction and strong-ID summary preparation, followed by
sequential linking and DSU mutation. Durable commit follows resolution. See
[the implementation design](DESIGN.md), [the linker](src/linker.rs) and
[the shard service](src/distributed.rs).

[IdentityKeySignature](src/sharding.rs) is a 32-byte SHA-256 digest, not an
FNV hash or a GPU-specific value. Its numeric and text constructors use separate
versioned domain prefixes and length-prefixed fields. Numeric interner IDs are
local to a store; distributed agreement must use the appropriate text-based
representation. Any accelerated signature implementation would need to match
these bytes exactly, including ordering and encoding.

The helpers under [src/perf](src/perf/mod.rs) are separate building blocks.
Their names do not establish that production ingestion uses SIMD hashing,
lock-free union-find, asynchronous WAL acknowledgement or GPU execution.
The authoritative production WAL and durability path are in
[distributed.rs](src/distributed.rs) and [persistence.rs](src/persistence.rs).

## Experiments that could be evaluated

| Candidate | Work that might be batched | Required boundary |
| --- | --- | --- |
| Key preparation | Encoding or hashing many independent keys | Preserve exact signature bytes and complete temporal key tuples |
| Interval candidate filtering | Comparing immutable interval arrays | Preserve half-open interval semantics, extreme endpoints and all required candidates |
| Reconciliation preparation | Grouping boundary signatures | Leave authoritative component-wide strong-ID checks and merge application intact |

These are hypotheses about parallel work, not recommendations to add a particular
GPU library or buy hardware. Profiling must first establish whether each operation
materially contributes to end-to-end time. Faster key hashing would not by itself
remove durable writes, RPC latency, record hydration or sequential merge work.

A prototype must return enough information for the existing resolution path to
perform its checks. It cannot acknowledge ingestion before durable commit, skip
resolution, or replace component-wide guards with pairwise checks. Current
candidate pruning and sampling limits are described in [DESIGN.md](DESIGN.md);
acceleration must not silently introduce further omissions.

## Evidence required before adoption

1. Measure the CPU reference on persistent shards with a recorded commit,
   hardware, profile, ontology, topology, dataset size, overlap and seed. Include
   repeated runs, failure counts, acknowledged records and RPC latency.
2. Separate key preparation, transfer, kernel, synchronization, linking and
   persistence costs. Include initialization and small batches as well as large
   batches; report both memory use and end-to-end results.
3. Compare cluster membership, strong-ID rejections and query results against
   the CPU reference. Cover transitive merges, duplicate source identities,
   different perspectives, adjacent and unbounded intervals, hot keys and
   cross-shard reconciliation.
4. Exercise allocation and device failures, partial batches and restart. Verify
   that fallback preserves the existing staging, error propagation and durability
   contract without resolving or committing a record twice.
5. Keep a CPU-only build and deployment path. Add hardware-specific CI only once
   a concrete implementation and test environment exist.

An implementation decision should follow those measurements. No speedup target,
batch-size threshold or memory-capacity guarantee is established by this note.
