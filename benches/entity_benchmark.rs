use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::time::Duration;
use unirust::linker::build_clusters_optimized;
use unirust::model::{Descriptor, Record, RecordId, RecordIdentity};
use unirust::ontology::{IdentityKey, Ontology, StrongIdentifier};
use unirust::temporal::Interval;
use unirust::*;

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
        (1000, 0.01), // 1000 entities, 1% overlap
        (1000, 0.10), // 1000 entities, 10% overlap
        (1000, 0.30), // 1000 entities, 30% overlap
        (5000, 0.01), // 5000 entities, 1% overlap
        (5000, 0.10), // 5000 entities, 10% overlap
        (5000, 0.30), // 5000 entities, 30% overlap
    ];

    for (entity_count, overlap_prob) in test_configs {
        group.throughput(Throughput::Elements(entity_count as u64));
        group.sample_size(10);

        // Optimized algorithm benchmark
        group.bench_with_input(
            BenchmarkId::new(
                "optimized",
                format!(
                    "{}_entities_{}%_overlap",
                    entity_count,
                    (overlap_prob * 100.0) as u32
                ),
            ),
            &(entity_count, overlap_prob),
            |b, &(entity_count, overlap_prob)| {
                b.iter(|| {
                    let mut store = Store::new();
                    let ontology = create_test_ontology(&mut store);
                    let records = generate_test_records(&mut store, entity_count, overlap_prob);
                    store.add_records(records).unwrap();

                    black_box(build_clusters_optimized(&store, &ontology).unwrap())
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_entity_resolution);
criterion_main!(benches);
