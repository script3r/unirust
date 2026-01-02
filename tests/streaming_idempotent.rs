#[path = "../src/test_support.rs"]
mod test_support;

use test_support::{default_ontology, generate_dataset};
use unirust_rs::{Store, Unirust};

#[test]
fn streaming_ingest_is_idempotent() -> anyhow::Result<()> {
    let mut store = Store::new();
    let dataset = generate_dataset(&mut store, 500, 0.25, 7);
    let ontology = default_ontology(&dataset.schema);

    let mut unirust = Unirust::with_store(ontology, store);
    let first = unirust.stream_records(dataset.records.clone())?;
    let count_after_first = unirust.record_count();
    let second = unirust.stream_records(dataset.records)?;

    assert_eq!(count_after_first, unirust.record_count());
    assert_eq!(first.len(), second.len());
    for (left, right) in first.iter().zip(second.iter()) {
        assert_eq!(left.record_id, right.record_id);
        assert_eq!(left.cluster_id, right.cluster_id);
    }

    Ok(())
}
