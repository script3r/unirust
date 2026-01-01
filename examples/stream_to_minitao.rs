use std::sync::Arc;

use minitao::storage::{sqlite::SqliteStorage, StorageEngine};
use unirust_rs::minitao_store::{
    record_object_id, MinitaoGraphWriter, ASSOC_TYPE_CONFLICT_EDGE, ASSOC_TYPE_SAME_AS,
};
use unirust_rs::ontology::{IdentityKey, Ontology};
use unirust_rs::{Descriptor, Interval, Record, RecordId, RecordIdentity, Store, Unirust};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Unirust streaming -> Minitao example ===");

    let mut ontology = Ontology::new();
    let mut store = Store::new();

    let name_attr = store.interner_mut().intern_attr("name");
    let email_attr = store.interner_mut().intern_attr("email");
    let identity_key = IdentityKey::new(vec![name_attr, email_attr], "name_email".to_string());
    ontology.add_identity_key(identity_key);

    let name_value = store.interner_mut().intern_value("Ada Lovelace");
    let email_value = store.interner_mut().intern_value("ada@example.com");

    let record1 = Record::new(
        RecordId(1),
        RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "crm_001".to_string(),
        ),
        vec![
            Descriptor::new(name_attr, name_value, Interval::new(0, 10)?),
            Descriptor::new(email_attr, email_value, Interval::new(0, 10)?),
        ],
    );
    let record2 = Record::new(
        RecordId(2),
        RecordIdentity::new(
            "person".to_string(),
            "erp".to_string(),
            "erp_001".to_string(),
        ),
        vec![
            Descriptor::new(name_attr, name_value, Interval::new(5, 15)?),
            Descriptor::new(email_attr, email_value, Interval::new(5, 15)?),
        ],
    );

    let mut unirust = Unirust::with_store(ontology, store);

    let storage = Arc::new(SqliteStorage::new("unirust_graph.db")?);
    let writer = MinitaoGraphWriter::new(storage.clone());

    for record in vec![record1, record2] {
        let update = unirust.stream_record_update_graph(record)?;
        writer.apply_graph(&update.graph).await?;

        println!(
            "Stored graph after record {} -> cluster {}",
            update.assignment.record_id.0,
            update.assignment.cluster_id.0
        );

        let record_object = record_object_id(update.assignment.record_id);
        let same_as = storage
            .get_assoc_range(record_object, ASSOC_TYPE_SAME_AS, 0, 10)
            .await?;
        let conflicts = storage
            .get_assoc_range(record_object, ASSOC_TYPE_CONFLICT_EDGE, 0, 10)
            .await?;

        println!("  SAME_AS assocs: {}", same_as.len());
        for assoc in &same_as {
            println!("    SAME_AS -> id2={} time={}", assoc.id2, assoc.time);
        }

        println!("  CONFLICT assocs: {}", conflicts.len());
        for assoc in &conflicts {
            let conflict_obj = storage.get_object(assoc.id2).await?;
            println!(
                "    CONFLICT -> id2={} time={} payload={}",
                assoc.id2,
                assoc.time,
                conflict_obj
                    .as_ref()
                    .and_then(|obj| obj.data.get("payload_json"))
                    .cloned()
                    .unwrap_or_else(|| "<missing>".to_string())
            );
        }
    }

    println!("Minitao SQLite file: unirust_graph.db");
    Ok(())
}
