use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use std::hint::black_box;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use unirust_rs::Store;
use unirust_rs::{StreamingTuning, TuningProfile, Unirust};

#[path = "../src/test_support.rs"]
mod test_support;
use test_support::{default_ontology, generate_batch};

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn benchmark_scale_ingest(c: &mut Criterion) {
    static SCALE_CACHE: OnceLock<Mutex<std::collections::HashMap<String, Duration>>> =
        OnceLock::new();

    let mut group = c.benchmark_group("scale_ingest");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(3600));

    let total = env_u32("UNIRUST_SCALE_COUNT", 1_000_000);
    let batch = env_u32("UNIRUST_SCALE_BATCH", 100_000).max(1);
    let overlap = env_f64("UNIRUST_SCALE_OVERLAP", 0.01);

    group.throughput(Throughput::Elements(total as u64));
    group.bench_with_input(
        BenchmarkId::new("stream_records", format!("{total}_records")),
        &(total, batch, overlap),
        |b, (total, batch, overlap)| {
            b.iter_custom(|_iterations| {
                let cache =
                    SCALE_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
                let key = format!("{}_{}_{}", total, batch, overlap);
                if let Some(duration) = cache.lock().unwrap().get(&key).copied() {
                    return duration;
                }

                let mut store = Store::new();
                let schema = test_support::generate_dataset(&mut store, 1, *overlap, 42).schema;
                let ontology = default_ontology(&schema);
                let mut unirust = Unirust::with_store_and_tuning(
                    ontology,
                    store,
                    StreamingTuning::from_profile(TuningProfile::Balanced),
                );

                let start = std::time::Instant::now();
                let mut offset = 1;
                while offset <= *total {
                    let remaining = total - offset + 1;
                    let count = (*batch).min(remaining);
                    let records = generate_batch(unirust.store_mut(), offset, count, *overlap, 42);
                    black_box(unirust.stream_records(records).unwrap());
                    offset += count;
                }
                let duration = start.elapsed();
                let duration = if duration.is_zero() {
                    Duration::from_nanos(1)
                } else {
                    duration
                };
                cache.lock().unwrap().insert(key, duration);
                duration
            })
        },
    );

    group.finish();
}

criterion_group!(scale_benches, benchmark_scale_ingest);
criterion_main!(scale_benches);
