//! Distributed/sharded/streaming microbenchmarks (~2-4 minutes).
//!
//! Run with:
//! ```
//! cargo bench --bench bench_distributed
//! ```
//!
//! Environment knobs:
//! - UNIRUST_DIST_RECORDS: default record count per batch (default: 50000)
//! - UNIRUST_DIST_OVERLAP: overlap probability (default: 0.05)
//! - UNIRUST_DIST_PARTITIONS: partition count (default: 8)
//! - UNIRUST_DIST_SHARDS: shard count for sharding benches (default: 4)
//! - UNIRUST_DIST_STREAM_CHUNK: streaming chunk size (default: 512)
//! - UNIRUST_DIST_STREAM_TOTAL: total streaming records (default: 20000)
//! - UNIRUST_DIST_RECON_KEYS: reconciliation key count (default: 1000)
//! - UNIRUST_DIST_RECON_ENTRIES: boundary entries per key (default: 4)

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tonic::Request;
use unirust_rs::distributed::proto::shard_service_server::ShardService;
use unirust_rs::distributed::{
    proto, ClusterLocalityIndex, ConstraintConfig, ConstraintKind, DistributedOntologyConfig,
    IdentityKeyConfig, ShardNode,
};
use unirust_rs::model::{GlobalClusterId, KeyValue, RecordId};
use unirust_rs::ontology::Ontology;
use unirust_rs::partitioned::{ParallelPartitionedUnirust, PartitionConfig};
use unirust_rs::perf::ConcurrentInterner;
use unirust_rs::sharding::{ClusterBoundaryIndex, IdentityKeySignature, IncrementalReconciler};
use unirust_rs::{Descriptor, Interval, Record, RecordIdentity, StreamingTuning, TuningProfile};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn default_dist_config() -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: vec![IdentityKeyConfig {
            name: "name_email".to_string(),
            attributes: vec!["name".to_string(), "email".to_string()],
        }],
        strong_identifiers: vec!["ssn".to_string()],
        constraints: vec![ConstraintConfig {
            name: "unique_ssn".to_string(),
            attribute: "ssn".to_string(),
            kind: ConstraintKind::Unique,
        }],
    }
}

fn generate_proto_batch(
    start_index: u32,
    count: u32,
    overlap_probability: f64,
    seed: u64,
) -> Vec<proto::RecordInput> {
    let mut rng = StdRng::seed_from_u64(seed ^ start_index as u64);
    let mut records = Vec::with_capacity(count as usize);

    let name_value = "John Doe";
    let email_value = "john@example.com";
    let phone_value = "555-1234";
    let ssn_value = "123-45-6789";
    let perspectives = ["crm", "erp", "web", "mobile", "api"];

    for offset in 0..count {
        let index = start_index + offset;
        let perspective = perspectives[rng.random_range(0..perspectives.len())];
        let uid = format!("{perspective}_{index:09}");
        let identity = proto::RecordIdentity {
            entity_type: "person".to_string(),
            perspective: perspective.to_string(),
            uid,
        };

        let start = rng.random_range(0..1000);
        let end = start + rng.random_range(1..100);

        let mut descriptors = Vec::new();

        if rng.random_bool(overlap_probability) {
            descriptors.push(proto::RecordDescriptor {
                attr: "name".to_string(),
                value: name_value.to_string(),
                start,
                end,
            });
            descriptors.push(proto::RecordDescriptor {
                attr: "email".to_string(),
                value: email_value.to_string(),
                start,
                end,
            });
        } else {
            let unique_name = format!("Person_{index:09}");
            let unique_email = format!("person_{index:09}@example.com");
            descriptors.push(proto::RecordDescriptor {
                attr: "name".to_string(),
                value: unique_name,
                start,
                end,
            });
            descriptors.push(proto::RecordDescriptor {
                attr: "email".to_string(),
                value: unique_email,
                start,
                end,
            });
        }

        if rng.random_bool(0.5) {
            descriptors.push(proto::RecordDescriptor {
                attr: "phone".to_string(),
                value: phone_value.to_string(),
                start,
                end,
            });
        } else {
            let phone = format!("555-{:04}", rng.random_range(1000..9999));
            descriptors.push(proto::RecordDescriptor {
                attr: "phone".to_string(),
                value: phone,
                start,
                end,
            });
        }

        if rng.random_bool(0.8) {
            descriptors.push(proto::RecordDescriptor {
                attr: "ssn".to_string(),
                value: ssn_value.to_string(),
                start,
                end,
            });
        } else {
            let ssn = format!(
                "{:03}-{:02}-{:04}",
                rng.random_range(100..999),
                rng.random_range(10..99),
                rng.random_range(1000..9999)
            );
            descriptors.push(proto::RecordDescriptor {
                attr: "ssn".to_string(),
                value: ssn,
                start,
                end,
            });
        }

        records.push(proto::RecordInput {
            index,
            identity: Some(identity),
            descriptors,
        });
    }

    records
}

fn build_record_from_proto(interner: &ConcurrentInterner, input: &proto::RecordInput) -> Record {
    let identity = input
        .identity
        .as_ref()
        .expect("record identity is required");

    let descriptors = input
        .descriptors
        .iter()
        .map(|desc| {
            let attr = interner.intern_attr(&desc.attr);
            let value = interner.intern_value(&desc.value);
            let interval = Interval::new(desc.start, desc.end).expect("valid interval");
            Descriptor::new(attr, value, interval)
        })
        .collect::<Vec<_>>();

    Record::new(
        RecordId(0),
        RecordIdentity::new(
            identity.entity_type.clone(),
            identity.perspective.clone(),
            identity.uid.clone(),
        ),
        descriptors,
    )
}

fn compute_partition_id_for_record(
    record: &Record,
    ontology: &Ontology,
    interner: &ConcurrentInterner,
    partition_count: usize,
) -> usize {
    use rustc_hash::FxHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = FxHasher::default();
    let identity_keys = ontology.identity_keys_for_type(&record.identity.entity_type);

    if !identity_keys.is_empty() {
        let first_key = &identity_keys[0];
        let attr_ids: Vec<_> = first_key
            .attribute_names
            .iter()
            .filter_map(|name| interner.get_attr_id(name))
            .collect();

        let key_value_ids: Vec<_> = record
            .descriptors
            .iter()
            .filter(|d| attr_ids.contains(&d.attr))
            .map(|d| &d.value)
            .collect();

        if !key_value_ids.is_empty() {
            record.identity.entity_type.hash(&mut hasher);
            for value_id in key_value_ids {
                value_id.hash(&mut hasher);
            }
        } else {
            record.identity.uid.hash(&mut hasher);
        }
    } else {
        record.identity.uid.hash(&mut hasher);
    }

    (hasher.finish() as usize) % partition_count
}

fn build_partitioned(
    partition_count: usize,
    interner: &ConcurrentInterner,
    config: &DistributedOntologyConfig,
) -> (ParallelPartitionedUnirust, Ontology) {
    let ontology = config.build_ontology_with_interner(interner);
    let tuning = StreamingTuning::from_profile(TuningProfile::Balanced);
    let partition_config = PartitionConfig::for_cores(partition_count);
    let partitioned =
        ParallelPartitionedUnirust::new(partition_config, Arc::new(ontology.clone()), tuning)
            .expect("partitioned");
    (partitioned, ontology)
}

// =============================================================================
// DISTRIBUTED BUILD + PARTITION BENCHMARKS
// =============================================================================

fn bench_distributed_record_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed/build_record");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));

    let count = env_u32("UNIRUST_DIST_RECORDS", 50_000);
    let overlap = env_f64("UNIRUST_DIST_OVERLAP", 0.05);
    let proto_records = generate_proto_batch(1, count, overlap, 42);

    group.throughput(Throughput::Elements(count as u64));
    group.bench_function(BenchmarkId::new("batch", count), |b| {
        b.iter_batched(
            ConcurrentInterner::new,
            |interner| {
                let mut built = Vec::with_capacity(proto_records.len());
                for record in &proto_records {
                    built.push(build_record_from_proto(&interner, record));
                }
                black_box(built);
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_distributed_partition_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed/partition_id");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));

    let count = env_u32("UNIRUST_DIST_RECORDS", 50_000);
    let overlap = env_f64("UNIRUST_DIST_OVERLAP", 0.05);
    let partition_count = env_usize("UNIRUST_DIST_PARTITIONS", 8);
    let config = default_dist_config();

    group.throughput(Throughput::Elements(count as u64));
    group.bench_function(BenchmarkId::new("batch", count), |b| {
        b.iter_batched(
            || {
                let interner = ConcurrentInterner::new();
                let proto_records = generate_proto_batch(1, count, overlap, 43);
                let records = proto_records
                    .iter()
                    .map(|record| build_record_from_proto(&interner, record))
                    .collect::<Vec<_>>();
                let ontology = config.build_ontology_with_interner(&interner);
                (interner, records, ontology)
            },
            |(interner, records, ontology)| {
                let mut partitions = Vec::with_capacity(records.len());
                for record in &records {
                    partitions.push(compute_partition_id_for_record(
                        record,
                        &ontology,
                        &interner,
                        partition_count,
                    ));
                }
                black_box(partitions);
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

// =============================================================================
// PARTITIONED INGEST BENCHMARKS
// =============================================================================

fn bench_partitioned_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed/partitioned_ingest");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(6));

    let count = env_u32("UNIRUST_DIST_RECORDS", 50_000);
    let overlap = env_f64("UNIRUST_DIST_OVERLAP", 0.05);
    let partition_count = env_usize("UNIRUST_DIST_PARTITIONS", 8);
    let config = default_dist_config();

    group.throughput(Throughput::Elements(count as u64));
    group.bench_function(BenchmarkId::new("prepartitioned", partition_count), |b| {
        b.iter_batched(
            || {
                let interner = ConcurrentInterner::new();
                let proto_records = generate_proto_batch(1, count, overlap, 44);
                let (partitioned, ontology) =
                    build_partitioned(partition_count, &interner, &config);
                let mut records = Vec::with_capacity(proto_records.len());
                for record in &proto_records {
                    records.push(build_record_from_proto(&interner, record));
                }
                let indexed = records
                    .into_iter()
                    .enumerate()
                    .map(|(idx, record)| {
                        let partition_id = compute_partition_id_for_record(
                            &record,
                            &ontology,
                            &interner,
                            partition_count,
                        );
                        (partition_id, idx as u32, record)
                    })
                    .collect::<Vec<_>>();
                (partitioned, indexed)
            },
            |(partitioned, indexed)| {
                let results = partitioned.ingest_batch_with_partitions(indexed);
                black_box(results);
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

// =============================================================================
// SHARD NODE (DISTRIBUTED INGEST) BENCHMARKS
// =============================================================================

fn bench_shardnode_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed/shard_ingest");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(6));

    std::env::set_var("UNIRUST_PARTITIONED", "1");
    let partition_count = env_usize("UNIRUST_DIST_PARTITIONS", 8);
    std::env::set_var("UNIRUST_PARTITION_COUNT", partition_count.to_string());

    let count = env_u32("UNIRUST_DIST_RECORDS", 50_000);
    let overlap = env_f64("UNIRUST_DIST_OVERLAP", 0.05);
    let config = default_dist_config();

    group.throughput(Throughput::Elements(count as u64));
    group.bench_function(BenchmarkId::new("batch", count), |b| {
        b.iter_batched(
            || {
                let rt = Runtime::new().expect("runtime");
                let shard = rt.block_on(async {
                    ShardNode::new(0, config.clone(), StreamingTuning::default()).expect("shard")
                });
                let records = generate_proto_batch(1, count, overlap, 45);
                (rt, shard, records)
            },
            |(rt, shard, records)| {
                let response = rt.block_on(async {
                    shard
                        .ingest_records(Request::new(proto::IngestRecordsRequest { records }))
                        .await
                        .expect("ingest")
                });
                black_box(response);
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_shardnode_streaming_simulated(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed/shard_stream");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(6));

    std::env::set_var("UNIRUST_PARTITIONED", "1");
    let partition_count = env_usize("UNIRUST_DIST_PARTITIONS", 8);
    std::env::set_var("UNIRUST_PARTITION_COUNT", partition_count.to_string());

    let total = env_u32("UNIRUST_DIST_STREAM_TOTAL", 20_000);
    let chunk = env_u32("UNIRUST_DIST_STREAM_CHUNK", 512).max(1);
    let overlap = env_f64("UNIRUST_DIST_OVERLAP", 0.05);
    let config = default_dist_config();

    group.throughput(Throughput::Elements(total as u64));
    group.bench_function(BenchmarkId::new("chunked", chunk), |b| {
        b.iter_batched(
            || {
                let rt = Runtime::new().expect("runtime");
                let shard = rt.block_on(async {
                    ShardNode::new(0, config.clone(), StreamingTuning::default()).expect("shard")
                });
                let records = generate_proto_batch(1, total, overlap, 46);
                (rt, shard, records)
            },
            |(rt, shard, records)| {
                let response = rt.block_on(async {
                    let mut offset = 0usize;
                    let mut assignments = Vec::new();
                    while offset < records.len() {
                        let end = (offset + chunk as usize).min(records.len());
                        let batch = records[offset..end].to_vec();
                        let resp = shard
                            .ingest_records(Request::new(proto::IngestRecordsRequest {
                                records: batch,
                            }))
                            .await
                            .expect("ingest");
                        assignments.extend(resp.into_inner().assignments);
                        offset = end;
                    }
                    assignments
                });
                black_box(response);
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

// =============================================================================
// SHARDED ROUTING + RECONCILIATION BENCHMARKS
// =============================================================================

fn bench_cluster_locality_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("sharded/locality_index");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));

    let record_count = env_usize("UNIRUST_DIST_RECORDS", 50_000) as u32;
    let shard_count = env_usize("UNIRUST_DIST_SHARDS", 4).max(1);
    let interner = ConcurrentInterner::new();

    let signatures = (0..record_count)
        .map(|i| {
            let name = interner.intern_attr("name");
            let email = interner.intern_attr("email");
            let name_val = interner.intern_value(&format!("Person_{i:09}"));
            let email_val = interner.intern_value(&format!("person_{i:09}@example.com"));
            let key_values = vec![
                KeyValue::new(name, name_val),
                KeyValue::new(email, email_val),
            ];
            IdentityKeySignature::from_key_values("person", &key_values)
        })
        .collect::<Vec<_>>();

    group.throughput(Throughput::Elements(record_count as u64));
    group.bench_function("lookup_hits", |b| {
        b.iter_batched(
            || {
                let mut index = ClusterLocalityIndex::new();
                for (i, sig) in signatures.iter().enumerate() {
                    let shard_id = (i % shard_count) as u16;
                    index.register(
                        *sig,
                        shard_id,
                        GlobalClusterId::new(shard_id, i as u32, 0),
                        i as u64,
                    );
                }
                (index, signatures.clone())
            },
            |(mut index, signatures)| {
                let mut hits = 0usize;
                for sig in &signatures {
                    if index.get_locality(sig).is_some() {
                        hits += 1;
                    }
                }
                black_box(hits);
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("lookup_misses", |b| {
        b.iter_batched(
            || ClusterLocalityIndex::new(),
            |mut index| {
                let mut misses = 0usize;
                for sig in &signatures {
                    if index.get_locality(sig).is_none() {
                        misses += 1;
                    }
                }
                black_box(misses);
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_reconciliation(c: &mut Criterion) {
    let mut group = c.benchmark_group("sharded/reconcile");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(6));

    let key_count = env_usize("UNIRUST_DIST_RECON_KEYS", 1_000).max(1);
    let entries_per_key = env_usize("UNIRUST_DIST_RECON_ENTRIES", 4).max(1);
    let shard_count = env_usize("UNIRUST_DIST_SHARDS", 4).max(1);
    let interner = ConcurrentInterner::new();
    let name = interner.intern_attr("name");
    let email = interner.intern_attr("email");

    let signatures = (0..key_count)
        .map(|i| {
            let name_val = interner.intern_value(&format!("Person_{i:06}"));
            let email_val = interner.intern_value(&format!("person_{i:06}@example.com"));
            let key_values = vec![
                KeyValue::new(name, name_val),
                KeyValue::new(email, email_val),
            ];
            IdentityKeySignature::from_key_values("person", &key_values)
        })
        .collect::<Vec<_>>();

    group.throughput(Throughput::Elements(key_count as u64));
    group.bench_function(
        BenchmarkId::new("keys", format!("{key_count}_entries_{entries_per_key}")),
        |b| {
            b.iter_batched(
                || {
                    let mut boundaries = Vec::with_capacity(shard_count);
                    for shard in 0..shard_count {
                        boundaries.push(ClusterBoundaryIndex::new_small(shard as u16));
                    }

                    let mut idx = 0usize;
                    for sig in &signatures {
                        for entry_idx in 0..entries_per_key {
                            let shard = (idx + entry_idx) % shard_count;
                            let cluster_id =
                                GlobalClusterId::new(shard as u16, (idx + entry_idx) as u32, 0);
                            let interval =
                                Interval::new(0, 100 + entry_idx as i64).expect("valid interval");
                            boundaries[shard].register_boundary_key(*sig, cluster_id, interval);
                        }
                        idx += 1;
                    }

                    let key_set = signatures
                        .iter()
                        .copied()
                        .collect::<std::collections::HashSet<_>>();
                    (boundaries, key_set)
                },
                |(boundaries, key_set)| {
                    let mut reconciler = IncrementalReconciler::new();
                    for boundary in boundaries {
                        reconciler.add_shard_boundary(boundary);
                    }
                    let result = reconciler.reconcile_keys(&key_set);
                    black_box(result);
                },
                BatchSize::LargeInput,
            )
        },
    );

    group.finish();
}

criterion_group!(
    distributed_benches,
    bench_distributed_record_build,
    bench_distributed_partition_id,
    bench_partitioned_ingest,
    bench_shardnode_ingest,
    bench_shardnode_streaming_simulated,
    bench_cluster_locality_index,
    bench_reconciliation,
);
criterion_main!(distributed_benches);
