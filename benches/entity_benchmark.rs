use criterion::{
    criterion_group, criterion_main, AxisScale, BatchSize, BenchmarkId, Criterion,
    PlotConfiguration, SamplingMode, Throughput,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Once;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use unirust_rs::model::{Descriptor, Record, RecordId, RecordIdentity};
use unirust_rs::ontology::{IdentityKey, Ontology, StrongIdentifier};
use unirust_rs::sharding::ShardedStreamEngine;
use unirust_rs::temporal::Interval;
use unirust_rs::*;

/// Generate test records with controlled overlap patterns
fn generate_test_records(store: &mut Store, count: u32, overlap_probability: f64) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut records = Vec::with_capacity(count as usize);

    // Create attributes
    let name_attr = store.interner_mut().intern_attr("name");
    let email_attr = store.interner_mut().intern_attr("email");
    let phone_attr = store.interner_mut().intern_attr("phone");
    let ssn_attr = store.interner_mut().intern_attr("ssn");

    // Create shared values for clustering
    let name_value1 = store.interner_mut().intern_value("John Doe");
    let email_value1 = store.interner_mut().intern_value("john@example.com");
    let phone_value1 = store.interner_mut().intern_value("555-1234");
    let ssn_value1 = store.interner_mut().intern_value("123-45-6789");

    for i in 1..=count {
        let perspectives = ["crm", "erp", "web", "mobile", "api"];
        let perspective = perspectives[rng.random_range(0..perspectives.len())];
        let uid = format!("{}_{:06}", perspective, i);
        let identity = RecordIdentity::new("person".to_string(), perspective.to_string(), uid);

        let start = rng.random_range(0..1000);
        let end = start + rng.random_range(1..100);
        let interval = Interval::new(start, end).unwrap();

        let mut descriptors = Vec::new();

        // Use shared values based on overlap probability
        if rng.random_bool(overlap_probability) {
            descriptors.push(Descriptor::new(name_attr, name_value1, interval));
            descriptors.push(Descriptor::new(email_attr, email_value1, interval));
        } else {
            let unique_name = format!("Person_{:06}", i);
            let unique_email = format!("person_{:06}@example.com", i);
            let name_value = store.interner_mut().intern_value(&unique_name);
            let email_value = store.interner_mut().intern_value(&unique_email);
            descriptors.push(Descriptor::new(name_attr, name_value, interval));
            descriptors.push(Descriptor::new(email_attr, email_value, interval));
        }

        // Phone - some shared, some unique
        if rng.random_bool(0.5) {
            descriptors.push(Descriptor::new(phone_attr, phone_value1, interval));
        } else {
            let phone = format!("555-{:04}", rng.random_range(1000..9999));
            let phone_value = store.interner_mut().intern_value(&phone);
            descriptors.push(Descriptor::new(phone_attr, phone_value, interval));
        }

        // SSN - mostly shared for strong clustering
        if rng.random_bool(0.8) {
            descriptors.push(Descriptor::new(ssn_attr, ssn_value1, interval));
        } else {
            let ssn = format!(
                "{:03}-{:02}-{:04}",
                rng.random_range(100..999),
                rng.random_range(10..99),
                rng.random_range(1000..9999)
            );
            let ssn_value = store.interner_mut().intern_value(&ssn);
            descriptors.push(Descriptor::new(ssn_attr, ssn_value, interval));
        }

        records.push(Record::new(RecordId(i), identity, descriptors));
    }

    records
}

/// Create a simple ontology for testing
fn create_test_ontology(store: &mut Store) -> Ontology {
    let mut ontology = Ontology::new();

    // Create attributes
    let name_attr = store.interner_mut().intern_attr("name");
    let email_attr = store.interner_mut().intern_attr("email");
    let ssn_attr = store.interner_mut().intern_attr("ssn");

    // Identity key: name + email
    let identity_key = IdentityKey::new(vec![name_attr, email_attr], "name_email".to_string());
    ontology.add_identity_key(identity_key);

    // Strong identifier: SSN
    let ssn_strong_id = StrongIdentifier::new(ssn_attr, "ssn".to_string());
    ontology.add_strong_identifier(ssn_strong_id);

    ontology
}

/// Benchmark entity resolution with different entity counts and overlap probabilities
fn benchmark_entity_resolution(c: &mut Criterion) {
    let mut group = c.benchmark_group("entity_resolution");

    // Set longer measurement time to ensure we can complete 10 samples
    group.measurement_time(Duration::from_secs(10));

    // Test configurations: (entity_count, overlap_probability)
    let test_configs = vec![
        (1000, 0.01),      // 1000 entities, 1% overlap
        (1000, 0.10),      // 1000 entities, 10% overlap
        (1000, 0.30),      // 1000 entities, 30% overlap
        (5000, 0.01),      // 5000 entities, 1% overlap
        (5000, 0.10),      // 5000 entities, 10% overlap
        (5000, 0.30),      // 5000 entities, 30% overlap
        (1_000_000, 0.01), // 1M entities, 1% overlap
    ];

    for (entity_count, overlap_prob) in test_configs {
        group.throughput(Throughput::Elements(entity_count as u64));
        group.sample_size(10);
        if entity_count >= 100_000 {
            group.warm_up_time(Duration::from_secs(2));
            group.measurement_time(Duration::from_secs(45));
        } else {
            group.warm_up_time(Duration::from_secs(3));
            group.measurement_time(Duration::from_secs(10));
        }

        // Streaming algorithm benchmark
        let mut base_store = Store::new();
        let ontology = create_test_ontology(&mut base_store);
        let records = generate_test_records(&mut base_store, entity_count, overlap_prob);
        group.bench_with_input(
            BenchmarkId::new(
                "streaming",
                format!(
                    "{}_entities_{}%_overlap",
                    entity_count,
                    (overlap_prob * 100.0) as u32
                ),
            ),
            &(base_store, ontology, records),
            |b, input| {
                let (base_store, ontology, records) = input;
                b.iter_batched(
                    || (base_store.clone(), ontology.clone(), records.clone()),
                    |(store, ontology, records)| {
                        let mut unirust = Unirust::with_store(ontology, store);
                        let result = black_box(unirust.stream_records(records).unwrap());
                        maybe_print_profile();
                        result
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark entity resolution for very large entity counts.
fn benchmark_entity_resolution_large(c: &mut Criterion) {
    static LARGE_CACHE: OnceLock<Mutex<HashMap<String, Duration>>> = OnceLock::new();

    let mut group = c.benchmark_group("entity_resolution_large");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3600));

    let test_configs = vec![(10_000_000, 0.01)];

    for (entity_count, overlap_prob) in test_configs {
        group.throughput(Throughput::Elements(entity_count as u64));
        group.bench_with_input(
            BenchmarkId::new(
                "streaming",
                format!(
                    "{}_entities_{}%_overlap",
                    entity_count,
                    (overlap_prob * 100.0) as u32
                ),
            ),
            &(entity_count, overlap_prob),
            |b, (entity_count, overlap_prob)| {
                b.iter_custom(|iterations| {
                    let cache = LARGE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
                    let key = format!("{}_{}", entity_count, overlap_prob);
                    if let Some(duration) = cache.lock().unwrap().get(&key).copied() {
                        return duration * iterations as u32;
                    }

                    let mut store = Store::new();
                    let ontology = create_test_ontology(&mut store);
                    let records = generate_test_records(&mut store, *entity_count, *overlap_prob);
                    let mut unirust = Unirust::with_store(ontology, store);
                    let start = std::time::Instant::now();
                    black_box(unirust.stream_records(records).unwrap());
                    let duration = start.elapsed();
                    cache.lock().unwrap().insert(key, duration);
                    duration * iterations as u32
                })
            },
        );
    }

    group.finish();
}

fn benchmark_entity_resolution_sharded(c: &mut Criterion) {
    static SHARDED_CACHE: OnceLock<Mutex<HashMap<String, Duration>>> = OnceLock::new();

    let mut group = c.benchmark_group("entity_resolution_sharded");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.sampling_mode(SamplingMode::Flat);
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Linear));

    let configs = vec![
        (1_000_000, 0.01, 16usize),
        (1_000_000, 0.10, 16usize),
        (1_000_000, 0.30, 16usize),
    ];

    for (entity_count, overlap_prob, shard_count) in configs {
        group.throughput(Throughput::Elements(entity_count as u64));

        group.bench_with_input(
            BenchmarkId::new(
                "sharded_streaming",
                format!(
                    "{}_entities_{}%_overlap_{}shards",
                    entity_count,
                    (overlap_prob * 100.0) as u32,
                    shard_count
                ),
            ),
            &(entity_count, overlap_prob, shard_count),
            |b, (entity_count, overlap_prob, shard_count)| {
                b.iter_custom(|_iterations| {
                    let cache = SHARDED_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
                    let key = format!("{}_{}_{}", entity_count, overlap_prob, shard_count);
                    if let Some(duration) = cache.lock().unwrap().get(&key).copied() {
                        return duration;
                    }

                    let mut base_store = Store::new();
                    let ontology = create_test_ontology(&mut base_store);
                    let records =
                        generate_test_records(&mut base_store, *entity_count, *overlap_prob);
                    let mut engine = ShardedStreamEngine::new(ontology, *shard_count).unwrap();
                    engine.seed_interners(base_store.interner());

                    let start = std::time::Instant::now();
                    black_box(engine.stream_records(records).unwrap());
                    let duration = start.elapsed();
                    cache.lock().unwrap().insert(key, duration);
                    duration
                })
            },
        );
    }

    group.finish();
}

fn benchmark_entity_resolution_profile_5000(c: &mut Criterion) {
    let mut group = c.benchmark_group("profile_5000");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(1));

    let entity_count = 5000;
    let overlap_prob = 0.10;

    let mut base_store = Store::new();
    let ontology = create_test_ontology(&mut base_store);
    let records = generate_test_records(&mut base_store, entity_count, overlap_prob);

    group.bench_with_input(
        BenchmarkId::new("streaming", "5000_entities_10%_overlap"),
        &(base_store, ontology, records),
        |b, input| {
            let (base_store, ontology, records) = input;
            b.iter_custom(|iterations| {
                let mut total = Duration::from_secs(0);
                for _ in 0..iterations {
                    let mut unirust = Unirust::with_store(ontology.clone(), base_store.clone());
                    let start = std::time::Instant::now();
                    black_box(unirust.stream_records(records.clone()).unwrap());
                    total += start.elapsed();
                }

                if std::env::var("UNIRUST_PROFILE").is_ok() {
                    unirust_rs::profile::print_report();
                }

                total
            })
        },
    );

    group.finish();
}

fn benchmark_entity_resolution_profile_100k(c: &mut Criterion) {
    let mut group = c.benchmark_group("profile_100k");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(1));

    let entity_count = 100_000;
    let overlap_prob = 0.10;

    let mut base_store = Store::new();
    let ontology = create_test_ontology(&mut base_store);
    let records = generate_test_records(&mut base_store, entity_count, overlap_prob);

    group.bench_with_input(
        BenchmarkId::new("streaming", "100000_entities_10%_overlap"),
        &(base_store, ontology, records),
        |b, input| {
            let (base_store, ontology, records) = input;
            b.iter_custom(|iterations| {
                let mut total = Duration::from_secs(0);
                for _ in 0..iterations {
                    let mut unirust = Unirust::with_store(ontology.clone(), base_store.clone());
                    let start = std::time::Instant::now();
                    black_box(unirust.stream_records(records.clone()).unwrap());
                    total += start.elapsed();
                }

                if std::env::var("UNIRUST_PROFILE").is_ok() {
                    unirust_rs::profile::print_report();
                }

                total
            })
        },
    );

    group.finish();
}

fn maybe_print_profile() {
    if std::env::var("UNIRUST_PROFILE").is_err() {
        return;
    }
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        unirust_rs::profile::print_report();
    });
}

criterion_group!(benches, benchmark_entity_resolution);
criterion_group! {
    name = large_benches;
    config = Criterion::default().without_plots();
    targets = benchmark_entity_resolution_large
}
criterion_group!(profile_benches, benchmark_entity_resolution_profile_5000);
criterion_group!(
    profile_100k_benches,
    benchmark_entity_resolution_profile_100k
);
criterion_group! {
    name = sharded_benches;
    config = Criterion::default().without_plots();
    targets = benchmark_entity_resolution_sharded
}
criterion_main!(
    benches,
    large_benches,
    profile_benches,
    profile_100k_benches,
    sharded_benches
);
