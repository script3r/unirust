//! Quick benchmarks for CI and development feedback (~30 seconds).
//!
//! Run with:
//! ```
//! cargo bench --bench bench_quick
//! ```
//!
//! These benchmarks provide fast feedback on core performance characteristics.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::time::Duration;
use tempfile::TempDir;
use unirust_rs::test_support::{default_ontology, generate_dataset};
use unirust_rs::{
    Interval, PersistentStore, QueryDescriptor, Store, StreamingTuning, TuningProfile, Unirust,
};

// =============================================================================
// INGEST BENCHMARKS
// =============================================================================

/// Quick ingest benchmark with in-memory store.
fn bench_ingest_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick/ingest");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    // Small dataset for quick feedback
    let entity_count = 2_000u32;
    let overlap_prob = 0.10;

    let mut base_store = Store::new();
    let dataset = generate_dataset(&mut base_store, entity_count, overlap_prob, 42);
    let ontology = default_ontology(&dataset.schema);
    let records = dataset.records;

    group.throughput(Throughput::Elements(entity_count as u64));
    group.bench_with_input(
        BenchmarkId::new("memory", format!("{entity_count}_records")),
        &(base_store, ontology, records),
        |b, (store, ontology, records)| {
            b.iter_batched(
                || (store.clone(), ontology.clone(), records.clone()),
                |(store, ontology, records)| {
                    let mut unirust = Unirust::with_store(ontology, store);
                    black_box(unirust.stream_records(records).unwrap())
                },
                BatchSize::SmallInput,
            )
        },
    );

    group.finish();
}

/// Quick ingest benchmark with persistent store.
fn bench_ingest_persistent(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick/ingest");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    let entity_count = 2_000u32;
    let overlap_prob = 0.10;

    group.throughput(Throughput::Elements(entity_count as u64));
    group.bench_function(
        BenchmarkId::new("persistent", format!("{entity_count}_records")),
        |b| {
            b.iter_batched(
                || {
                    let temp = TempDir::new().expect("temp dir");
                    let mut store = PersistentStore::open(temp.path()).expect("open store");
                    let dataset =
                        generate_dataset(store.inner_mut(), entity_count, overlap_prob, 42);
                    let ontology = default_ontology(&dataset.schema);
                    (temp, store, ontology, dataset.records)
                },
                |(_temp, store, ontology, records)| {
                    let mut unirust = Unirust::with_store_and_tuning(
                        ontology,
                        store,
                        StreamingTuning::from_profile(TuningProfile::Balanced),
                    );
                    black_box(unirust.stream_records(records).unwrap())
                },
                BatchSize::SmallInput,
            )
        },
    );

    group.finish();
}

// =============================================================================
// QUERY BENCHMARKS
// =============================================================================

/// Quick query benchmark.
fn bench_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick/query");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    // Setup: ingest 5K records first
    let entity_count = 5_000u32;
    let overlap_prob = 0.10;
    let mut store = Store::new();
    let dataset = generate_dataset(&mut store, entity_count, overlap_prob, 42);
    let ontology = default_ontology(&dataset.schema);

    let mut unirust = Unirust::with_store(ontology, store);
    unirust.stream_records(dataset.records).unwrap();

    let descriptors = vec![QueryDescriptor {
        attr: dataset.schema.email_attr,
        value: dataset.schema.email_value,
    }];
    let interval = Interval::new(0, 1000).expect("valid interval");

    group.throughput(Throughput::Elements(1));
    group.bench_function("by_email", |b| {
        b.iter(|| {
            black_box(
                unirust
                    .query_master_entities(&descriptors, interval)
                    .unwrap(),
            )
        })
    });

    group.finish();
}

// =============================================================================
// GRAPH BENCHMARKS
// =============================================================================

/// Quick graph update benchmark.
fn bench_graph_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick/graph");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    let entity_count = 200u32;
    let overlap_prob = 0.10;

    let mut base_store = Store::new();
    let base_dataset = generate_dataset(&mut base_store, entity_count, overlap_prob, 42);
    let ontology = default_ontology(&base_dataset.schema);

    group.throughput(Throughput::Elements(entity_count as u64));
    group.bench_with_input(
        BenchmarkId::new("stream_with_graph", format!("{entity_count}_records")),
        &(base_store, ontology, overlap_prob),
        |b, (store, ontology, overlap_prob)| {
            b.iter_batched(
                || {
                    let mut store = store.clone();
                    let dataset = generate_dataset(&mut store, entity_count, *overlap_prob, 43);
                    (store, ontology.clone(), dataset.records)
                },
                |(store, ontology, records)| {
                    let mut unirust = Unirust::with_store(ontology, store);
                    black_box(unirust.stream_records_update_graph(records).unwrap())
                },
                BatchSize::SmallInput,
            )
        },
    );

    group.finish();
}

criterion_group!(
    quick_benches,
    bench_ingest_memory,
    bench_ingest_persistent,
    bench_query,
    bench_graph_update,
);
criterion_main!(quick_benches);
