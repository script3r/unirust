//! Scale benchmarks for pre-merge validation (~3-5 minutes).
//!
//! Run with:
//! ```
//! cargo bench --bench bench_scale
//! ```
//!
//! Configure via environment variables:
//! - UNIRUST_SCALE_COUNT: Total records (default: 100000)
//! - UNIRUST_SCALE_BATCH: Batch size (default: 10000)
//! - UNIRUST_SCALE_OVERLAP: Overlap probability (default: 0.01)
//!
//! Examples:
//! ```
//! # Default 100K records
//! cargo bench --bench bench_scale
//!
//! # 500K records with 5% overlap
//! UNIRUST_SCALE_COUNT=500000 UNIRUST_SCALE_OVERLAP=0.05 cargo bench --bench bench_scale
//!
//! # 1M records (longer run)
//! UNIRUST_SCALE_COUNT=1000000 cargo bench --bench bench_scale
//! ```

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use std::hint::black_box;
use std::time::Duration;
use tempfile::tempdir;
use unirust_rs::persistence::PersistentOpenOptions;
use unirust_rs::test_support::{default_ontology, generate_batch};
use unirust_rs::{PersistentStore, Store, StreamingTuning, TuningProfile, Unirust};

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

// =============================================================================
// IN-MEMORY SCALE BENCHMARKS
// =============================================================================

/// Scale benchmark with in-memory store.
fn bench_scale_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("scale/memory");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(60));

    let total = env_u32("UNIRUST_SCALE_COUNT", 100_000);
    let batch = env_u32("UNIRUST_SCALE_BATCH", 10_000).max(1);
    let overlap = env_f64("UNIRUST_SCALE_OVERLAP", 0.01);

    group.throughput(Throughput::Elements(total as u64));
    group.bench_with_input(
        BenchmarkId::new("ingest", format!("{total}_records")),
        &(total, batch, overlap),
        |b, (total, batch, overlap)| {
            b.iter_custom(|iterations| {
                let mut total_duration = Duration::ZERO;

                for _ in 0..iterations {
                    let mut store = Store::new();
                    let schema =
                        unirust_rs::test_support::generate_dataset(&mut store, 1, *overlap, 42)
                            .schema;
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
                        let records =
                            generate_batch(unirust.store_mut(), offset, count, *overlap, 42);
                        black_box(unirust.stream_records(records).unwrap());
                        offset += count;
                    }
                    total_duration += start.elapsed();
                }

                total_duration.max(Duration::from_nanos(1))
            })
        },
    );

    group.finish();
}

// =============================================================================
// PERSISTENT SCALE BENCHMARKS
// =============================================================================

/// Scale benchmark with persistent store (BillionScale tuning).
fn bench_scale_persistent(c: &mut Criterion) {
    let mut group = c.benchmark_group("scale/persistent");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(60));

    let total = env_u32("UNIRUST_SCALE_COUNT", 100_000);
    let batch = env_u32("UNIRUST_SCALE_BATCH", 10_000).max(1);
    let overlap = env_f64("UNIRUST_SCALE_OVERLAP", 0.01);

    group.throughput(Throughput::Elements(total as u64));
    group.bench_with_input(
        BenchmarkId::new("ingest", format!("{total}_records")),
        &(total, batch, overlap),
        |b, (total, batch, overlap)| {
            b.iter_custom(|iterations| {
                let mut total_duration = Duration::ZERO;

                for _ in 0..iterations {
                    let temp_dir = tempdir().unwrap();
                    let mut store = PersistentStore::open_with_options(
                        temp_dir.path(),
                        PersistentOpenOptions::default(),
                    )
                    .unwrap();

                    let schema = unirust_rs::test_support::generate_dataset(
                        store.inner_mut(),
                        1,
                        *overlap,
                        42,
                    )
                    .schema;
                    let ontology = default_ontology(&schema);
                    let tuning = StreamingTuning::from_profile(TuningProfile::BillionScale);
                    let mut unirust = Unirust::with_store_and_tuning(ontology, store, tuning);

                    let start = std::time::Instant::now();
                    let mut offset = 1;
                    while offset <= *total {
                        let remaining = total - offset + 1;
                        let count = (*batch).min(remaining);
                        let records =
                            generate_batch(unirust.store_mut(), offset, count, *overlap, 42);
                        black_box(unirust.stream_records(records).unwrap());
                        offset += count;
                    }
                    total_duration += start.elapsed();
                }

                total_duration.max(Duration::from_nanos(1))
            })
        },
    );

    group.finish();
}

// =============================================================================
// OVERLAP COMPARISON BENCHMARKS
// =============================================================================

/// Compare performance across different overlap probabilities.
fn bench_scale_overlap_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("scale/overlap");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(30));

    // Fixed smaller scale for comparison
    let total = 50_000u32;
    let batch = 10_000u32;
    let overlaps = [0.01, 0.10, 0.25];

    for overlap in overlaps {
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::new("memory", format!("{:.0}%_overlap", overlap * 100.0)),
            &(total, batch, overlap),
            |b, (total, batch, overlap)| {
                b.iter_custom(|iterations| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iterations {
                        let mut store = Store::new();
                        let schema =
                            unirust_rs::test_support::generate_dataset(&mut store, 1, *overlap, 42)
                                .schema;
                        let ontology = default_ontology(&schema);
                        let mut unirust = Unirust::with_store(ontology, store);

                        let start = std::time::Instant::now();
                        let mut offset = 1;
                        while offset <= *total {
                            let remaining = total - offset + 1;
                            let count = (*batch).min(remaining);
                            let records =
                                generate_batch(unirust.store_mut(), offset, count, *overlap, 42);
                            black_box(unirust.stream_records(records).unwrap());
                            offset += count;
                        }
                        total_duration += start.elapsed();
                    }

                    total_duration.max(Duration::from_nanos(1))
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    scale_benches,
    bench_scale_memory,
    bench_scale_persistent,
    bench_scale_overlap_comparison,
);
criterion_main!(scale_benches);
