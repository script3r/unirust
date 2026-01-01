use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use minitao::storage::{sqlite::SqliteStorage, StorageEngine};
use unirust_rs::minitao_store::{
    record_object_id, MinitaoGraphWriter, ASSOC_TYPE_SAME_AS, OBJECT_TYPE_RECORD,
};
use unirust_rs::ontology::{IdentityKey, Ontology};
use unirust_rs::{Descriptor, Interval, Record, RecordId, RecordIdentity, Store, Unirust};

fn temp_db_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = std::env::temp_dir();
    path.push(format!("unirust_{prefix}_{nanos}.db"));
    path
}

#[tokio::test]
async fn test_persist_graph_to_minitao_sqlite() -> anyhow::Result<()> {
    let mut ontology = Ontology::new();
    let mut store = Store::new();

    let name_attr = store.interner_mut().intern_attr("name");
    let identity_key = IdentityKey::new(vec![name_attr], "name_key".to_string());
    ontology.add_identity_key(identity_key);

    let name_value = store.interner_mut().intern_value("Ada Lovelace");
    let record1 = Record::new(
        RecordId(1),
        RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "crm_001".to_string(),
        ),
        vec![Descriptor::new(
            name_attr,
            name_value,
            Interval::new(0, 10)?,
        )],
    );
    let record2 = Record::new(
        RecordId(2),
        RecordIdentity::new(
            "person".to_string(),
            "erp".to_string(),
            "erp_001".to_string(),
        ),
        vec![Descriptor::new(
            name_attr,
            name_value,
            Interval::new(0, 10)?,
        )],
    );

    let mut unirust = Unirust::with_store(ontology, store);
    unirust.stream_record_update_graph(record1)?;
    let update = unirust.stream_record_update_graph(record2)?;

    let db_path = temp_db_path("minitao");
    let storage = Arc::new(SqliteStorage::new(&db_path)?);
    let writer = MinitaoGraphWriter::new(storage.clone());

    writer.apply_graph(&update.graph).await?;

    let obj1 = storage
        .get_object(record_object_id(RecordId(1)))
        .await?
        .expect("record object missing");
    let obj2 = storage
        .get_object(record_object_id(RecordId(2)))
        .await?
        .expect("record object missing");

    assert_eq!(obj1.otype, OBJECT_TYPE_RECORD);
    assert_eq!(obj2.otype, OBJECT_TYPE_RECORD);

    let assoc = storage
        .get_assoc(
            record_object_id(RecordId(1)),
            ASSOC_TYPE_SAME_AS,
            record_object_id(RecordId(2)),
        )
        .await?;
    assert!(assoc.is_some(), "expected same_as association");

    let _ = std::fs::remove_file(&db_path);
    Ok(())
}
