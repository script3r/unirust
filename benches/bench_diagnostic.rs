//! Diagnostic tool for billion-scale bottleneck identification.
//!
//! This is a one-shot diagnostic (not Criterion) that prints human-readable output.
//!
//! Run with:
//! ```
//! cargo bench --bench bench_diagnostic 2>&1 | tee diagnostic.log
//! ```
//!
//! Diagnostics included:
//! - DSU scaling: find/merge ops at 10K-1M scale
//! - Full pipeline: throughput and degradation at scale
//! - Memory pressure: batch timing analysis

use std::hint::black_box;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use unirust_rs::advanced::{DsuBackend, PersistentDSUConfig, TemporalGuard};
use unirust_rs::persistence::PersistentOpenOptions;
use unirust_rs::test_support::{default_ontology, generate_batch};
use unirust_rs::{Interval, PersistentStore, RecordId, StreamingTuning, TuningProfile, Unirust};

fn main() {
    println!("=== Critical Path Diagnostic ===\n");

    // Run each diagnostic
    diagnose_dsu_scaling();
    diagnose_index_scaling();
    diagnose_full_pipeline();
    diagnose_memory_pressure();
}

/// Test DSU find() and merge() scaling
fn diagnose_dsu_scaling() {
    println!("--- DSU Scaling Diagnostic ---");

    for scale in [10_000u32, 100_000, 500_000, 1_000_000] {
        let temp_dir = tempdir().unwrap();

        // Use PersistentStore to get a properly configured DB with column families
        let store =
            PersistentStore::open_with_options(temp_dir.path(), PersistentOpenOptions::default())
                .unwrap();

        let db = store.db_shared();
        let config = PersistentDSUConfig::default();
        let mut dsu = DsuBackend::persistent(db, config).unwrap();

        // Measure add_record
        let start = Instant::now();
        for i in 0..scale {
            dsu.add_record(RecordId(i)).unwrap();
        }
        let add_time = start.elapsed();

        // Measure find() operations
        let start = Instant::now();
        for i in 0..scale {
            let _ = black_box(dsu.find(RecordId(i)));
        }
        let find_time = start.elapsed();

        // Measure merge operations (every 10th record)
        let merge_count = scale / 10;
        let start = Instant::now();
        for i in 0..merge_count {
            let a = RecordId(i * 10);
            let b = RecordId(i * 10 + 1);
            let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "merge".to_string());
            let _ = dsu.try_merge(a, b, guard);
        }
        let merge_time = start.elapsed();

        println!(
            "  {scale:>10} records: add={:>8.2}ms, find={:>8.2}ms ({:.0}/s), merge={:>8.2}ms ({:.0}/s)",
            add_time.as_secs_f64() * 1000.0,
            find_time.as_secs_f64() * 1000.0,
            scale as f64 / find_time.as_secs_f64(),
            merge_time.as_secs_f64() * 1000.0,
            merge_count as f64 / merge_time.as_secs_f64(),
        );
    }
    println!();
}

/// Test index via full pipeline at different scales
fn diagnose_index_scaling() {
    println!("--- Index Scaling via Pipeline Diagnostic ---");
    println!("  (Index performance is measured via full pipeline - see diagnose_full_pipeline for details)");
    println!();
}

/// Test full pipeline at increasing scales
fn diagnose_full_pipeline() {
    println!("--- Full Pipeline Scaling Diagnostic ---");

    for scale in [10_000u32, 50_000, 100_000, 250_000, 500_000] {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path();

        let mut store =
            PersistentStore::open_with_options(path, PersistentOpenOptions::default()).unwrap();

        let schema =
            unirust_rs::test_support::generate_dataset(store.inner_mut(), 1, 0.01, 42).schema;
        let ontology = default_ontology(&schema);

        let tuning = StreamingTuning::from_profile(TuningProfile::BillionScale);
        let mut unirust = Unirust::with_store_and_tuning(ontology, store, tuning);

        let batch_size = 10_000u32;
        let mut offset = 1u32;
        let mut total_time = Duration::ZERO;
        let mut batch_times = Vec::new();

        while offset <= scale {
            let remaining = scale - offset + 1;
            let count = batch_size.min(remaining);
            let records = generate_batch(unirust.store_mut(), offset, count, 0.01, 42);

            let start = Instant::now();
            black_box(unirust.stream_records(records).unwrap());
            let batch_time = start.elapsed();

            batch_times.push((offset, batch_time));
            total_time += batch_time;
            offset += count;
        }

        let rate = scale as f64 / total_time.as_secs_f64();
        let cluster_count = unirust.streaming_cluster_count().unwrap_or(0);

        // Check for degradation within the run
        let first_batch = batch_times
            .first()
            .map(|(_, t)| t.as_secs_f64() * 1000.0)
            .unwrap_or(0.0);
        let last_batch = batch_times
            .last()
            .map(|(_, t)| t.as_secs_f64() * 1000.0)
            .unwrap_or(0.0);
        let degradation = if first_batch > 0.0 {
            (last_batch / first_batch - 1.0) * 100.0
        } else {
            0.0
        };

        println!(
            "  {scale:>10} records: {:.0}/s, clusters={}, first_batch={:.1}ms, last_batch={:.1}ms, degradation={:+.1}%",
            rate, cluster_count, first_batch, last_batch, degradation
        );
    }
    println!();
}

/// Test memory pressure effects
fn diagnose_memory_pressure() {
    println!("--- Memory Pressure Diagnostic ---");

    let scale = 500_000u32;
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();

    let mut store =
        PersistentStore::open_with_options(path, PersistentOpenOptions::default()).unwrap();

    let schema = unirust_rs::test_support::generate_dataset(store.inner_mut(), 1, 0.01, 42).schema;
    let ontology = default_ontology(&schema);

    let tuning = StreamingTuning::from_profile(TuningProfile::BillionScale);
    let mut unirust = Unirust::with_store_and_tuning(ontology, store, tuning);

    let batch_size = 10_000u32;
    let mut offset = 1u32;

    println!("  Batch ingestion timing (batch_size={batch_size}):");
    while offset <= scale {
        let remaining = scale - offset + 1;
        let count = batch_size.min(remaining);
        let records = generate_batch(unirust.store_mut(), offset, count, 0.01, 42);

        let start = Instant::now();
        black_box(unirust.stream_records(records).unwrap());
        let batch_time = start.elapsed();

        if offset % 100_000 == 1 || offset == 1 {
            let rate = count as f64 / batch_time.as_secs_f64();
            println!(
                "    offset={offset:>7}: {:.1}ms ({:.0}/s)",
                batch_time.as_secs_f64() * 1000.0,
                rate
            );
        }

        offset += count;
    }

    println!("\n  Final stats:");
    println!(
        "    clusters: {}",
        unirust.streaming_cluster_count().unwrap_or(0)
    );
    println!("    boundaries: {}", unirust.boundary_count());
    println!(
        "    cross_shard_merges: {}",
        unirust.cross_shard_merge_count()
    );
}
