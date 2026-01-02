use std::time::Duration;

#[path = "../src/test_support.rs"]
mod test_support;

use test_support::{default_ontology, generate_dataset};
use unirust_rs::{QueryDescriptor, Store, Unirust};
use unirust_rs::temporal::Interval;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn percentile(mut values: Vec<u128>, p: f64) -> u128 {
    values.sort_unstable();
    if values.is_empty() {
        return 0;
    }
    let idx = ((values.len() as f64 - 1.0) * p).round() as usize;
    values[idx]
}

#[test]
fn query_latency_regression_gate() -> anyhow::Result<()> {
    if std::env::var("UNIRUST_ENABLE_LATENCY_GATES").is_err() {
        return Ok(());
    }

    let entity_count = env_u64("UNIRUST_GATE_RECORDS", 50_000) as u32;
    let overlap = std::env::var("UNIRUST_GATE_OVERLAP")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.10);
    let iterations = env_usize("UNIRUST_GATE_QUERIES", 1_000);

    let mut store = Store::new();
    let dataset = generate_dataset(&mut store, entity_count, overlap, 42);
    let ontology = default_ontology(&dataset.schema);
    let records = dataset.records;

    let mut unirust = Unirust::with_store(ontology, store);
    unirust.stream_records(records)?;

    let descriptor = QueryDescriptor {
        attr: dataset.schema.email_attr,
        value: dataset.schema.email_value,
    };
    let interval = Interval::new(0, 1000)?;

    let mut timings = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = std::time::Instant::now();
        let _ = unirust.query_master_entities(&[descriptor], interval)?;
        timings.push(start.elapsed().as_micros());
    }

    let p50 = percentile(timings.clone(), 0.50);
    let p95 = percentile(timings.clone(), 0.95);
    let p99 = percentile(timings.clone(), 0.99);

    if let Ok(value) = std::env::var("UNIRUST_P50_US") {
        if let Ok(limit) = value.parse::<u128>() {
            assert!(p50 <= limit, "p50 {}us > {}us", p50, limit);
        }
    }
    if let Ok(value) = std::env::var("UNIRUST_P95_US") {
        if let Ok(limit) = value.parse::<u128>() {
            assert!(p95 <= limit, "p95 {}us > {}us", p95, limit);
        }
    }
    if let Ok(value) = std::env::var("UNIRUST_P99_US") {
        if let Ok(limit) = value.parse::<u128>() {
            assert!(p99 <= limit, "p99 {}us > {}us", p99, limit);
        }
    }

    let _ = Duration::from_micros(p99 as u64);
    Ok(())
}
