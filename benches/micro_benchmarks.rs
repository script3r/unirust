//! Microbenchmarks for hot paths in the unirust codebase.
//!
//! These benchmarks target specific internal operations that are frequently
//! called in tight loops or have significant algorithmic complexity.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;
use unirust_rs::dsu::{Clusters, TemporalDSU, TemporalGuard};
use unirust_rs::model::{AttrId, ClusterId, Descriptor, Record, RecordId, RecordIdentity, ValueId};
use unirust_rs::ontology::{Ontology, StrongIdentifier};
use unirust_rs::temporal::Interval;
use unirust_rs::Store;

// =============================================================================
// DSU BENCHMARKS - Union-Find Operations
// =============================================================================

/// Benchmark DSU find() with path compression under various cluster depths.
/// Tests: O(α(n)) amortized complexity and cache behavior.
fn bench_dsu_find(c: &mut Criterion) {
    let mut group = c.benchmark_group("dsu_find");
    group.sample_size(50);
    group.warm_up_time(Duration::from_millis(500));

    // Test different cluster sizes
    for &record_count in &[1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(
            BenchmarkId::new("find_all", record_count),
            &record_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let mut dsu = TemporalDSU::new();
                        // Create records and merge them into chains
                        for i in 0..count {
                            dsu.add_record(RecordId(i));
                        }
                        // Create a few large clusters by merging consecutive records
                        let cluster_size = 100.min(count / 10);
                        for cluster_start in (0..count).step_by(cluster_size as usize) {
                            for i in 1..cluster_size.min(count - cluster_start) {
                                let guard = TemporalGuard::new(
                                    Interval::new(0, 100).unwrap(),
                                    "bench".to_string(),
                                );
                                let _ = dsu.try_merge(
                                    RecordId(cluster_start),
                                    RecordId(cluster_start + i),
                                    guard,
                                );
                            }
                        }
                        dsu
                    },
                    |mut dsu| {
                        // Find all records (exercises path compression)
                        for i in 0..count {
                            black_box(dsu.find(RecordId(i)));
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark DSU merge operations with temporal guard validation.
/// Tests: Guard conflict checking O(G²) per merge.
fn bench_dsu_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("dsu_merge");
    group.sample_size(30);

    // Test with varying guard counts per record
    for &guards_per_record in &[1, 5, 10, 20] {
        let merge_count = 1000;
        group.throughput(Throughput::Elements(merge_count as u64));
        group.bench_with_input(
            BenchmarkId::new("with_guards", guards_per_record),
            &guards_per_record,
            |b, &guard_count| {
                b.iter_batched(
                    || {
                        let mut dsu = TemporalDSU::new();
                        // Pre-populate with records
                        for i in 0..(merge_count * 2) {
                            dsu.add_record(RecordId(i));
                        }
                        (dsu, guard_count)
                    },
                    |(mut dsu, guard_count)| {
                        for i in 0..merge_count {
                            let guard = TemporalGuard::new(
                                Interval::new(i as i64 * 10, (i as i64 + 1) * 10).unwrap(),
                                format!("guard_{}", i % guard_count),
                            );
                            black_box(dsu.try_merge(RecordId(i * 2), RecordId(i * 2 + 1), guard));
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark cluster materialization from DSU.
/// Tests: O(N) cluster building with HashMap allocation.
fn bench_dsu_get_clusters(c: &mut Criterion) {
    let mut group = c.benchmark_group("dsu_get_clusters");
    group.sample_size(20);

    for &record_count in &[1_000, 10_000, 50_000] {
        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(
            BenchmarkId::new("materialize", record_count),
            &record_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let mut dsu = TemporalDSU::new();
                        for i in 0..count {
                            dsu.add_record(RecordId(i));
                        }
                        // Create clusters of size ~10
                        for i in (0..count).step_by(10) {
                            for j in 1..10.min(count - i) {
                                let guard = TemporalGuard::new(
                                    Interval::new(0, 100).unwrap(),
                                    "bench".to_string(),
                                );
                                let _ = dsu.try_merge(RecordId(i), RecordId(i + j), guard);
                            }
                        }
                        dsu
                    },
                    |mut dsu| {
                        black_box(dsu.get_clusters());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// =============================================================================
// TEMPORAL BENCHMARKS - Interval Operations
// =============================================================================

/// Benchmark Allen relation computation in tight loops.
/// Tests: O(1) but with 8 comparisons, frequently called.
fn bench_allen_relation(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_allen");
    group.sample_size(100);

    let comparison_count = 100_000;
    group.throughput(Throughput::Elements(comparison_count as u64));

    // Pre-generate intervals for consistent benchmarking
    let intervals: Vec<Interval> = (0..1000)
        .map(|i| Interval::new(i * 10, i * 10 + 15).unwrap())
        .collect();

    group.bench_function("relation_100k", |b| {
        b.iter(|| {
            let mut count = 0u32;
            for _ in 0..(comparison_count / 1000) {
                for i in 0..intervals.len() {
                    for j in 0..intervals.len().min(i + 10) {
                        if unirust_rs::temporal::is_overlapping(&intervals[i], &intervals[j]) {
                            count += 1;
                        }
                    }
                }
            }
            black_box(count)
        })
    });

    group.finish();
}

/// Benchmark interval intersection computation.
fn bench_interval_intersect(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_intersect");
    group.sample_size(100);

    let ops = 100_000;
    group.throughput(Throughput::Elements(ops as u64));

    let intervals_a: Vec<Interval> = (0..1000)
        .map(|i| Interval::new(i * 10, i * 10 + 20).unwrap())
        .collect();
    let intervals_b: Vec<Interval> = (0..1000)
        .map(|i| Interval::new(i * 10 + 5, i * 10 + 25).unwrap())
        .collect();

    group.bench_function("intersect_100k", |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for _ in 0..(ops / 1000) {
                for i in 0..intervals_a.len() {
                    if let Some(intersection) =
                        unirust_rs::temporal::intersect(&intervals_a[i], &intervals_b[i])
                    {
                        sum += intersection.end - intersection.start;
                    }
                }
            }
            black_box(sum)
        })
    });

    group.finish();
}

/// Benchmark interval coalescing with varying fragmentation.
fn bench_interval_coalesce(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_coalesce");
    group.sample_size(30);

    for &interval_count in &[100, 500, 1000, 5000] {
        group.throughput(Throughput::Elements(interval_count as u64));

        // Create intervals with ~30% overlap (adjacent/overlapping)
        let intervals: Vec<(Interval, ())> = (0..interval_count)
            .map(|i| {
                let start = (i as i64) * 7; // 70% spacing, 30% overlap
                let end = start + 10;
                (Interval::new(start, end).unwrap(), ())
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("adjacent_overlap", interval_count),
            &intervals,
            |b, intervals| {
                b.iter(|| black_box(unirust_rs::temporal::coalesce_same_value(intervals)))
            },
        );
    }

    group.finish();
}

// =============================================================================
// INDEX BENCHMARKS - Store Lookups
// =============================================================================

/// Benchmark record lookups with varying store sizes.
fn bench_store_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_lookup");
    group.sample_size(30);

    for &record_count in &[1_000, 10_000, 50_000] {
        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::new("get_record", record_count),
            &record_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let mut store = Store::new();
                        let attr = store.interner_mut().intern_attr("name");

                        for i in 0..count {
                            let value = store.interner_mut().intern_value(&format!("value_{}", i));
                            let record = Record::new(
                                RecordId(i),
                                RecordIdentity::new(
                                    "entity".to_string(),
                                    "source".to_string(),
                                    format!("uid_{}", i),
                                ),
                                vec![Descriptor::new(attr, value, Interval::new(0, 100).unwrap())],
                            );
                            store.add_record(record).unwrap();
                        }
                        store
                    },
                    |store| {
                        let mut sum = 0u32;
                        for i in 0..count {
                            if let Some(record) = store.get_record(RecordId(i)) {
                                sum += record.descriptors.len() as u32;
                            }
                        }
                        black_box(sum)
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// =============================================================================
// CONFLICT DETECTION BENCHMARKS
// =============================================================================

/// Benchmark conflict detection with varying cluster sizes.
fn bench_conflict_detection(c: &mut Criterion) {
    let mut group = c.benchmark_group("conflicts_detect");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &cluster_size in &[10, 50, 100, 500, 1000, 5000] {
        let cluster_count = 100;
        let total_records = cluster_size * cluster_count;
        group.throughput(Throughput::Elements(total_records as u64));

        group.bench_with_input(
            BenchmarkId::new("cluster_size", cluster_size),
            &cluster_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut store = Store::new();
                        let attr = store.interner_mut().intern_attr("name");
                        let strong_attr = store.interner_mut().intern_attr("ssn");

                        // Build clusters
                        let mut clusters_vec = Vec::new();
                        for cluster_idx in 0..cluster_count {
                            let base_id = cluster_idx * size;
                            let mut records = Vec::new();

                            for i in 0..size {
                                let record_id = RecordId(base_id + i);
                                let value = store
                                    .interner_mut()
                                    .intern_value(&format!("name_{}_{}", cluster_idx, i));
                                let strong_val = store
                                    .interner_mut()
                                    .intern_value(&format!("ssn_{}", base_id + i));

                                let record = Record::new(
                                    record_id,
                                    RecordIdentity::new(
                                        "person".to_string(),
                                        "source".to_string(),
                                        format!("uid_{}_{}", cluster_idx, i),
                                    ),
                                    vec![
                                        Descriptor::new(
                                            attr,
                                            value,
                                            Interval::new(0, 100).unwrap(),
                                        ),
                                        Descriptor::new(
                                            strong_attr,
                                            strong_val,
                                            Interval::new(0, 100).unwrap(),
                                        ),
                                    ],
                                );
                                store.add_record(record).unwrap();
                                records.push(record_id);
                            }

                            clusters_vec.push(unirust_rs::dsu::Cluster::new(
                                ClusterId(cluster_idx),
                                RecordId(base_id),
                                records,
                            ));
                        }

                        let clusters = Clusters {
                            clusters: clusters_vec,
                        };

                        let mut ontology = Ontology::new();
                        ontology.add_strong_identifier(StrongIdentifier::new(
                            strong_attr,
                            "ssn".to_string(),
                        ));

                        (store, clusters, ontology)
                    },
                    |(store, clusters, ontology)| {
                        black_box(
                            unirust_rs::conflicts::detect_conflicts(&store, &clusters, &ontology)
                                .unwrap(),
                        );
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// =============================================================================
// GOLDEN RECORD BENCHMARKS
// =============================================================================

/// Benchmark golden record construction with varying complexity.
fn bench_golden_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_golden");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));

    for &records_per_cluster in &[5, 20, 50, 100] {
        for &attrs_per_record in &[5, 10, 20] {
            let label = format!("{}recs_{}attrs", records_per_cluster, attrs_per_record);
            let cluster_count = 50;
            group.throughput(Throughput::Elements(cluster_count as u64));

            group.bench_function(BenchmarkId::new("build", &label), |b| {
                b.iter_batched(
                    || {
                        let mut store = Store::new();
                        let attrs: Vec<AttrId> = (0..attrs_per_record)
                            .map(|i| store.interner_mut().intern_attr(&format!("attr_{}", i)))
                            .collect();

                        let mut clusters_vec = Vec::new();
                        for cluster_idx in 0..cluster_count {
                            let base_id = (cluster_idx * records_per_cluster) as u32;
                            let mut records = Vec::new();

                            for i in 0..records_per_cluster {
                                let record_id = RecordId(base_id + i as u32);
                                let mut descriptors = Vec::new();

                                for (attr_idx, &attr) in attrs.iter().enumerate() {
                                    let value = store
                                        .interner_mut()
                                        .intern_value(&format!("val_{}_{}", cluster_idx, attr_idx));
                                    descriptors.push(Descriptor::new(
                                        attr,
                                        value,
                                        Interval::new((i as i64) * 10, (i as i64 + 1) * 10 + 5)
                                            .unwrap(),
                                    ));
                                }

                                let record = Record::new(
                                    record_id,
                                    RecordIdentity::new(
                                        "entity".to_string(),
                                        format!("source_{}", i % 3),
                                        format!("uid_{}_{}", cluster_idx, i),
                                    ),
                                    descriptors,
                                );
                                store.add_record(record).unwrap();
                                records.push(record_id);
                            }

                            clusters_vec.push(unirust_rs::dsu::Cluster::new(
                                ClusterId(cluster_idx as u32),
                                RecordId(base_id),
                                records,
                            ));
                        }

                        let clusters = Clusters {
                            clusters: clusters_vec,
                        };
                        let ontology = Ontology::new();

                        (store, clusters, ontology)
                    },
                    |(store, clusters, _ontology)| {
                        for cluster in &clusters.clusters {
                            black_box(unirust_rs::graph::golden_for_cluster(&store, cluster));
                        }
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }

    group.finish();
}

// =============================================================================
// HASH OPERATION BENCHMARKS
// =============================================================================

/// Benchmark HashMap operations simulating identity key lookups.
fn bench_hashmap_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_lookup");
    group.sample_size(50);

    for &key_count in &[1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(key_count as u64));

        group.bench_with_input(
            BenchmarkId::new("string_keys", key_count),
            &key_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let map: HashMap<String, u32> = (0..count)
                            .map(|i| (format!("key_{}_{}", i, i * 17 % 1000), i))
                            .collect();
                        let lookup_keys: Vec<String> = (0..count)
                            .map(|i| format!("key_{}_{}", i, i * 17 % 1000))
                            .collect();
                        (map, lookup_keys)
                    },
                    |(map, keys)| {
                        let mut sum = 0u64;
                        for key in &keys {
                            if let Some(&v) = map.get(key) {
                                sum += v as u64;
                            }
                        }
                        black_box(sum)
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark record ID based lookups (integer keys).
fn bench_record_id_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_record_id");
    group.sample_size(50);

    for &key_count in &[1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(key_count as u64));

        group.bench_with_input(
            BenchmarkId::new("record_id_keys", key_count),
            &key_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let map: HashMap<RecordId, ClusterId> = (0..count)
                            .map(|i| (RecordId(i), ClusterId(i / 10)))
                            .collect();
                        map
                    },
                    |map| {
                        let mut sum = 0u32;
                        for i in 0..count {
                            if let Some(cluster) = map.get(&RecordId(i)) {
                                sum += cluster.0;
                            }
                        }
                        black_box(sum)
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// =============================================================================
// SERIALIZATION BENCHMARKS
// =============================================================================

/// Benchmark record serialization/deserialization.
fn bench_record_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("serde_record");
    group.sample_size(30);

    for &descriptor_count in &[5, 20, 50] {
        group.throughput(Throughput::Elements(1000));

        group.bench_function(BenchmarkId::new("serialize", descriptor_count), |b| {
            b.iter_batched(
                || {
                    (0..1000)
                        .map(|i| {
                            let descriptors: Vec<Descriptor> = (0..descriptor_count)
                                .map(|d| {
                                    Descriptor::new(
                                        AttrId(d),
                                        ValueId(i * 100 + d),
                                        Interval::new(d as i64 * 10, (d as i64 + 1) * 10).unwrap(),
                                    )
                                })
                                .collect();
                            Record::new(
                                RecordId(i),
                                RecordIdentity::new(
                                    "entity".to_string(),
                                    "source".to_string(),
                                    format!("uid_{}", i),
                                ),
                                descriptors,
                            )
                        })
                        .collect::<Vec<_>>()
                },
                |records| {
                    let mut total_bytes = 0usize;
                    for record in &records {
                        let bytes = bincode::serialize(record).unwrap();
                        total_bytes += bytes.len();
                    }
                    black_box(total_bytes)
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_function(BenchmarkId::new("deserialize", descriptor_count), |b| {
            b.iter_batched(
                || {
                    (0..1000)
                        .map(|i| {
                            let descriptors: Vec<Descriptor> = (0..descriptor_count)
                                .map(|d| {
                                    Descriptor::new(
                                        AttrId(d),
                                        ValueId(i * 100 + d),
                                        Interval::new(d as i64 * 10, (d as i64 + 1) * 10).unwrap(),
                                    )
                                })
                                .collect();
                            let record = Record::new(
                                RecordId(i),
                                RecordIdentity::new(
                                    "entity".to_string(),
                                    "source".to_string(),
                                    format!("uid_{}", i),
                                ),
                                descriptors,
                            );
                            bincode::serialize(&record).unwrap()
                        })
                        .collect::<Vec<_>>()
                },
                |serialized| {
                    let mut count = 0u32;
                    for bytes in &serialized {
                        let record: Record = bincode::deserialize(bytes).unwrap();
                        count += record.descriptors.len() as u32;
                    }
                    black_box(count)
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// =============================================================================
// CRITERION CONFIGURATION
// =============================================================================

criterion_group!(
    dsu_benches,
    bench_dsu_find,
    bench_dsu_merge,
    bench_dsu_get_clusters
);

criterion_group!(
    temporal_benches,
    bench_allen_relation,
    bench_interval_intersect,
    bench_interval_coalesce
);

criterion_group!(index_benches, bench_store_lookup);

criterion_group!(conflict_benches, bench_conflict_detection);

criterion_group!(graph_benches, bench_golden_record);

criterion_group!(hash_benches, bench_hashmap_lookup, bench_record_id_lookup);

criterion_group!(serde_benches, bench_record_serde);

criterion_main!(
    dsu_benches,
    temporal_benches,
    index_benches,
    conflict_benches,
    graph_benches,
    hash_benches,
    serde_benches
);
