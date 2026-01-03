//! Integration test for linker state persistence and recovery.
//!
//! This test verifies that cluster ID mappings are preserved across restarts
//! when using PersistentStore with linker state persistence.

use std::collections::HashMap;
use tempfile::tempdir;
use unirust_rs::advanced::{ClusterId, GlobalClusterId, LinkerStateConfig};
use unirust_rs::persistence::linker_encoding;
use unirust_rs::test_support::{default_ontology, generate_dataset};
use unirust_rs::{PersistentStore, RecordId, StreamingTuning, Unirust};

#[test]
fn linker_state_persists_across_restart() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let data_dir = dir.path().join("store");
    std::fs::create_dir_all(&data_dir)?;

    // Phase 1: Ingest records and checkpoint
    let mut cluster_ids_before: HashMap<RecordId, ClusterId> = HashMap::new();
    let next_cluster_id_before: u32;
    {
        let mut store = PersistentStore::open(&data_dir)?;
        let dataset = generate_dataset(&mut store, 100, 0.25, 42);
        let ontology = default_ontology(&dataset.schema);

        let mut unirust = Unirust::with_store(ontology, store);

        // Stream records to create clusters (this initializes streaming)
        let assignments = unirust.stream_records(dataset.records)?;

        // Get cluster IDs before checkpoint
        for assignment in &assignments {
            cluster_ids_before.insert(assignment.record_id, assignment.cluster_id);
        }

        // Get next_cluster_id from linker
        next_cluster_id_before = unirust.next_cluster_id().unwrap_or(0);

        // Checkpoint linker state
        unirust.checkpoint_linker_state()?;

        // Verify we have some data
        assert!(
            !cluster_ids_before.is_empty(),
            "Expected some record->cluster mappings"
        );
        assert!(
            next_cluster_id_before > 0,
            "Expected next_cluster_id > 0 after ingestion"
        );
    }

    // Phase 2: Reopen and verify state is recovered
    {
        let mut store = PersistentStore::open(&data_dir)?;
        let dataset = generate_dataset(&mut store, 0, 0.0, 42); // Just for schema
        let ontology = default_ontology(&dataset.schema);

        let mut unirust = Unirust::with_store(ontology, store);

        // Need to initialize streaming first with a dummy call
        let empty_records: Vec<unirust_rs::Record> = vec![];
        let _ = unirust.stream_records(empty_records)?;

        // Restore linker state
        let restored_count = unirust.restore_linker_state()?;
        assert!(
            restored_count > 0,
            "Expected some cluster IDs to be restored"
        );

        // Verify next_cluster_id was restored
        let next_cluster_id_after = unirust.next_cluster_id().unwrap_or(0);
        assert_eq!(
            next_cluster_id_after, next_cluster_id_before,
            "next_cluster_id should be preserved across restart"
        );

        // Add a new record with unique data - create it using the same store
        // Generate unique record data that won't match existing records
        use unirust_rs::model::{Descriptor, Record, RecordIdentity};
        use unirust_rs::temporal::Interval;

        let name_attr = unirust.intern_attr("name");
        let email_attr = unirust.intern_attr("email");
        let name_value = unirust.intern_value("New Person 999");
        let email_value = unirust.intern_value("newperson999@unique.com");

        let new_record = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "test".to_string(), "999".to_string()),
            vec![
                Descriptor::new(name_attr, name_value, Interval::new(0, 100).unwrap()),
                Descriptor::new(email_attr, email_value, Interval::new(0, 100).unwrap()),
            ],
        );

        let assignments = unirust.stream_records(vec![new_record])?;
        let assignment = assignments.into_iter().next().unwrap();

        // The new record should get a cluster ID >= next_cluster_id_before
        // (no collision with pre-restart cluster IDs)
        assert!(
            assignment.cluster_id.0 >= next_cluster_id_before,
            "New cluster ID {} should be >= next_cluster_id_before {}",
            assignment.cluster_id.0,
            next_cluster_id_before
        );
    }

    Ok(())
}

#[test]
fn linker_state_with_lru_config_persists() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let data_dir = dir.path().join("store");
    std::fs::create_dir_all(&data_dir)?;

    // Use LRU-bounded linker state
    let mut tuning = StreamingTuning::balanced();
    tuning.linker_state_config = Some(LinkerStateConfig {
        cluster_ids_capacity: 100,
        global_ids_capacity: 50,
        summaries_capacity: 50,
        perspectives_capacity: 100,
        dirty_buffer_size: 10,
    });

    let next_cluster_id_before: u32;
    {
        let mut store = PersistentStore::open(&data_dir)?;
        let dataset = generate_dataset(&mut store, 50, 0.1, 42);
        let ontology = default_ontology(&dataset.schema);

        let mut unirust = Unirust::with_store_and_tuning(ontology, store, tuning.clone());

        // Stream records
        unirust.stream_records(dataset.records)?;

        next_cluster_id_before = unirust.next_cluster_id().unwrap_or(0);

        unirust.checkpoint_linker_state()?;
    }

    // Reopen and verify
    {
        let mut store = PersistentStore::open(&data_dir)?;
        let dataset = generate_dataset(&mut store, 0, 0.0, 42);
        let ontology = default_ontology(&dataset.schema);

        let mut unirust = Unirust::with_store_and_tuning(ontology, store, tuning);

        // Initialize streaming
        let empty_records: Vec<unirust_rs::Record> = vec![];
        let _ = unirust.stream_records(empty_records)?;

        let restored = unirust.restore_linker_state()?;
        assert!(restored > 0, "Expected some cluster IDs to be restored");

        let next_cluster_id_after = unirust.next_cluster_id().unwrap_or(0);
        assert_eq!(next_cluster_id_after, next_cluster_id_before);
    }

    Ok(())
}

#[test]
fn linker_persistence_round_trip_encoding() {
    // Test the encoding/decoding helpers directly

    // Test RecordId encoding
    let record_id = RecordId(12345678);
    let encoded = linker_encoding::encode_record_key(record_id);
    let decoded = linker_encoding::decode_record_key(&encoded);
    assert_eq!(decoded, Some(record_id));

    // Test ClusterId encoding
    let cluster_id = ClusterId(98765432);
    let encoded = linker_encoding::encode_cluster_id(cluster_id);
    let decoded = linker_encoding::decode_cluster_id(&encoded);
    assert_eq!(decoded, Some(cluster_id));

    // Test GlobalClusterId encoding
    let global_id = GlobalClusterId {
        shard_id: 42,
        local_id: 12345,
        version: 7,
    };
    let encoded = linker_encoding::encode_global_cluster_id(global_id);
    let decoded = linker_encoding::decode_global_cluster_id(&encoded);
    assert_eq!(decoded, Some(global_id));

    // Test next_cluster_id encoding
    let next_id: u32 = 999999;
    let encoded = linker_encoding::encode_next_cluster_id(next_id);
    let decoded = linker_encoding::decode_next_cluster_id(&encoded);
    assert_eq!(decoded, Some(next_id));
}

#[test]
fn linker_persistence_encoding_edge_cases() {
    // Test edge cases for encoding

    // Zero values
    let record_id = RecordId(0);
    let encoded = linker_encoding::encode_record_key(record_id);
    let decoded = linker_encoding::decode_record_key(&encoded);
    assert_eq!(decoded, Some(record_id));

    // Max values
    let cluster_id = ClusterId(u32::MAX);
    let encoded = linker_encoding::encode_cluster_id(cluster_id);
    let decoded = linker_encoding::decode_cluster_id(&encoded);
    assert_eq!(decoded, Some(cluster_id));

    // GlobalClusterId with max values
    let global_id = GlobalClusterId {
        shard_id: u16::MAX,
        local_id: u32::MAX,
        version: u16::MAX,
    };
    let encoded = linker_encoding::encode_global_cluster_id(global_id);
    let decoded = linker_encoding::decode_global_cluster_id(&encoded);
    assert_eq!(decoded, Some(global_id));

    // Test decoding with insufficient bytes returns None
    let short_bytes = [0u8; 2];
    assert_eq!(linker_encoding::decode_record_key(&short_bytes), None);
    assert_eq!(linker_encoding::decode_cluster_id(&short_bytes), None);
    assert_eq!(
        linker_encoding::decode_global_cluster_id(&short_bytes),
        None
    );
    assert_eq!(linker_encoding::decode_next_cluster_id(&short_bytes), None);
}
