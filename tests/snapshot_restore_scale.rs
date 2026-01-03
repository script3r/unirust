#[path = "../src/test_support.rs"]
mod test_support;

use tempfile::tempdir;
use unirust_rs::{PersistentStore, RecordStore};

#[test]
fn snapshot_restore_preserves_scale_state() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let data_dir = dir.path().join("store");
    std::fs::create_dir_all(&data_dir)?;

    let mut store = PersistentStore::open(&data_dir)?;
    let dataset = test_support::generate_dataset(&mut store, 1000, 0.2, 42);
    for record in dataset.records.clone() {
        store.add_record(record)?;
    }

    let checkpoint_dir = dir.path().join("checkpoint");
    store.checkpoint(&checkpoint_dir)?;
    drop(store);

    let restored = PersistentStore::open(&checkpoint_dir)?;
    assert_eq!(restored.len(), dataset.records.len());
    let sample = &dataset.records[0];
    assert!(restored
        .get_record_id_by_identity(&sample.identity)
        .is_some());
    assert!(restored.resolve_attr(sample.descriptors[0].attr).is_some());

    Ok(())
}
