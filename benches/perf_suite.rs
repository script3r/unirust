use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use std::hint::black_box;
use std::time::Duration;
use tempfile::TempDir;
use unirust_rs::{QueryDescriptor, StreamingTuning, TuningProfile, Unirust};
use unirust_rs::{PersistentStore, Store};
use unirust_rs::temporal::Interval;

#[path = "../src/test_support.rs"]
mod test_support;
use test_support::{default_ontology, generate_dataset};

fn bench_ingest_smoke(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_smoke");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    let entity_count = 2_000;
    let graph_count = 200;
    let overlap_prob = 0.10;
    let mut base_store = Store::new();
    let dataset = generate_dataset(&mut base_store, entity_count, overlap_prob, 42);
    let ontology = default_ontology(&dataset.schema);
    let records = dataset.records;

    group.throughput(Throughput::Elements(entity_count as u64));
    group.bench_with_input(
        BenchmarkId::new("stream_records", "2k"),
        &(base_store.clone(), ontology.clone(), records.clone()),
        |b, input| {
            let (store, ontology, records) = input;
            b.iter_batched(
                || (store.clone(), ontology.clone(), records.clone()),
                |(store, ontology, records)| {
                    let mut unirust = Unirust::with_store(ontology, store);
                    black_box(unirust.stream_records(records).unwrap());
                },
                BatchSize::SmallInput,
            )
        },
    );

    group.bench_with_input(
        BenchmarkId::new("stream_records_update_graph", "200"),
        &(base_store, ontology, graph_count, overlap_prob),
        |b, input| {
            let (store, ontology, graph_count, overlap_prob) = input;
            b.iter_batched(
                || {
                    let mut store = store.clone();
                    let dataset = generate_dataset(
                        &mut store,
                        *graph_count,
                        *overlap_prob,
                        43,
                    );
                    (store, ontology.clone(), dataset.records)
                },
                |(store, ontology, records)| {
                    let mut unirust = Unirust::with_store(ontology, store);
                    black_box(unirust.stream_records_update_graph(records).unwrap());
                },
                BatchSize::SmallInput,
            )
        },
    );

    group.finish();
}

fn bench_query_smoke(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_smoke");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    let entity_count = 5_000;
    let overlap_prob = 0.10;
    let mut store = Store::new();
    let dataset = generate_dataset(&mut store, entity_count, overlap_prob, 42);
    let ontology = default_ontology(&dataset.schema);
    let records = dataset.records;

    let mut unirust = Unirust::with_store(ontology, store);
    unirust.stream_records(records).unwrap();

    let descriptors = vec![QueryDescriptor {
        attr: dataset.schema.email_attr,
        value: dataset.schema.email_value,
    }];
    let interval = Interval::new(0, 1000).expect("valid interval");

    group.throughput(Throughput::Elements(1));
    group.bench_function("query_email", |b| {
        b.iter(|| {
            black_box(
                unirust
                    .query_master_entities(&descriptors, interval)
                    .unwrap(),
            );
        })
    });

    group.finish();
}

fn bench_ingest_persistent_smoke(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_persistent_smoke");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    let entity_count = 2_000;
    let overlap_prob = 0.10;
    group.throughput(Throughput::Elements(entity_count as u64));

    group.bench_function("persistent_stream_records", |b| {
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
                black_box(unirust.stream_records(records).unwrap());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    perf_suite,
    bench_ingest_smoke,
    bench_query_smoke,
    bench_ingest_persistent_smoke
);
criterion_main!(perf_suite);
