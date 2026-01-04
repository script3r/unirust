//! # Sharded Entity Resolution Example
//!
//! Demonstrates the core functionality of unirust in distributed mode
//! with multiple shards, showing entity resolution, conflict detection,
//! and knowledge graph export.
//!
//! To run this example, you need to start a 2-shard cluster first:
//!
//! ```bash
//! # Terminal 1: Start shard 0
//! ./target/release/unirust_shard --port 50061 --shard-id 0 --data-dir /tmp/shard0
//!
//! # Terminal 2: Start shard 1
//! ./target/release/unirust_shard --port 50062 --shard-id 1 --data-dir /tmp/shard1
//!
//! # Terminal 3: Start router
//! ./target/release/unirust_router --port 50060 --shards http://127.0.0.1:50061,http://127.0.0.1:50062
//!
//! # Terminal 4: Run this example
//! cargo run --example basic_example
//! ```

use tempfile::tempdir;
use unirust_rs::model::{Descriptor, Record, RecordId, RecordIdentity};
use unirust_rs::ontology::{Constraint, IdentityKey, Ontology, StrongIdentifier};
use unirust_rs::temporal::Interval;
use unirust_rs::*;

fn main() -> anyhow::Result<()> {
    println!("=== Unirust Sharded Entity Resolution Example ===\n");

    // Create a temporary directory for the persistent store
    let temp_dir = tempdir()?;
    let mut store = PersistentStore::open(temp_dir.path())?;

    // Create an ontology
    let mut ontology = Ontology::new();

    // Use the store's interner to create attribute and value IDs
    let name_attr = store.intern_attr("name");
    let email_attr = store.intern_attr("email");
    let phone_attr = store.intern_attr("phone");
    let ssn_attr = store.intern_attr("ssn");

    // Define identity keys (attributes that must match for records to be considered the same entity)
    let identity_key = IdentityKey::new(vec![name_attr, email_attr], "name_email".to_string());
    ontology.add_identity_key(identity_key);
    println!("Identity key: name + email");

    // Define strong identifiers (attributes that prevent merging when they conflict)
    let strong_id = StrongIdentifier::new(ssn_attr, "ssn_unique".to_string());
    ontology.add_strong_identifier(strong_id);
    println!("Strong identifier: SSN");

    // Define constraints
    let unique_email = Constraint::unique(email_attr, "unique_email".to_string());
    ontology.add_constraint(unique_email);
    println!("Constraint: unique email\n");

    // Create values for the records
    let name_value1 = store.intern_value("John Doe");
    let email_value1 = store.intern_value("john@example.com");
    let phone_value1 = store.intern_value("555-1234");
    let ssn_value1 = store.intern_value("123-45-6789");

    let name_value2 = store.intern_value("Jane Smith");
    let email_value2 = store.intern_value("jane@example.com");
    let phone_value2 = store.intern_value("555-5678");
    let ssn_value2 = store.intern_value("987-65-4321");

    let phone_value3 = store.intern_value("555-9999");
    let phone_value4 = store.intern_value("555-0000");
    let email_value3 = store.intern_value("john.doe@example.com");

    // Record 1: John Doe from CRM system
    let record1 = Record::new(
        RecordId(1),
        RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "crm_001".to_string(),
        ),
        vec![
            Descriptor::new(name_attr, name_value1, Interval::new(100, 200).unwrap()),
            Descriptor::new(email_attr, email_value1, Interval::new(100, 200).unwrap()),
            Descriptor::new(phone_attr, phone_value1, Interval::new(100, 200).unwrap()),
            Descriptor::new(ssn_attr, ssn_value1, Interval::new(100, 200).unwrap()),
        ],
    );

    // Record 2: John Doe from ERP system (same person, different system)
    let record2 = Record::new(
        RecordId(2),
        RecordIdentity::new(
            "person".to_string(),
            "erp".to_string(),
            "erp_001".to_string(),
        ),
        vec![
            Descriptor::new(name_attr, name_value1, Interval::new(150, 250).unwrap()),
            Descriptor::new(email_attr, email_value1, Interval::new(150, 250).unwrap()),
            Descriptor::new(phone_attr, phone_value3, Interval::new(150, 250).unwrap()),
            Descriptor::new(ssn_attr, ssn_value1, Interval::new(150, 250).unwrap()),
        ],
    );

    // Record 3: Jane Smith from CRM system
    let record3 = Record::new(
        RecordId(3),
        RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "crm_002".to_string(),
        ),
        vec![
            Descriptor::new(name_attr, name_value2, Interval::new(100, 200).unwrap()),
            Descriptor::new(email_attr, email_value2, Interval::new(100, 200).unwrap()),
            Descriptor::new(phone_attr, phone_value2, Interval::new(100, 200).unwrap()),
            Descriptor::new(ssn_attr, ssn_value2, Interval::new(100, 200).unwrap()),
        ],
    );

    // Record 4: John Doe from web system (same person, same SSN)
    let record4 = Record::new(
        RecordId(4),
        RecordIdentity::new(
            "person".to_string(),
            "web".to_string(),
            "web_001".to_string(),
        ),
        vec![
            Descriptor::new(name_attr, name_value1, Interval::new(180, 280).unwrap()),
            Descriptor::new(email_attr, email_value1, Interval::new(180, 280).unwrap()),
            Descriptor::new(phone_attr, phone_value4, Interval::new(180, 280).unwrap()),
            Descriptor::new(ssn_attr, ssn_value1, Interval::new(180, 280).unwrap()),
        ],
    );

    // Record 5: Another John Doe with conflicting email
    let record5 = Record::new(
        RecordId(5),
        RecordIdentity::new(
            "person".to_string(),
            "mobile".to_string(),
            "mobile_001".to_string(),
        ),
        vec![
            Descriptor::new(name_attr, name_value1, Interval::new(200, 300).unwrap()),
            Descriptor::new(email_attr, email_value3, Interval::new(200, 300).unwrap()),
            Descriptor::new(phone_attr, phone_value4, Interval::new(200, 300).unwrap()),
            Descriptor::new(ssn_attr, ssn_value1, Interval::new(200, 300).unwrap()),
        ],
    );

    // Create unirust instance with persistent store
    let tuning = StreamingTuning::from_profile(TuningProfile::Balanced);
    let mut unirust = Unirust::with_store_and_tuning(ontology.clone(), store, tuning);

    // Stream records through entity resolution
    let mut last_graph = None;
    for record in [record1, record2, record3, record4, record5] {
        let update = unirust.stream_record_update_graph(record)?;
        last_graph = Some(update.graph);
    }
    println!("Streamed 5 records into the store with entity resolution");

    // Build clusters using streaming entity resolution
    let clusters = unirust.build_clusters()?;
    println!("Built {} clusters\n", clusters.len());

    for (i, cluster) in clusters.clusters.iter().enumerate() {
        println!("Cluster {}: {} records", i + 1, cluster.records.len());
        for record_id in &cluster.records {
            if let Some(record) = unirust.get_record(*record_id) {
                println!(
                    "  - {} ({}:{})",
                    record_id, record.identity.perspective, record.identity.uid
                );
            }
        }
    }

    // Detect conflicts
    let observations = unirust.detect_conflicts(&clusters)?;
    println!("\nDetected {} observations", observations.len());

    for (i, observation) in observations.iter().enumerate() {
        match observation {
            conflicts::Observation::DirectConflict(conflict) => {
                println!(
                    "  Observation {}: Direct conflict in attribute {} at interval {}",
                    i + 1,
                    conflict.attribute.0,
                    conflict.interval
                );
            }
            conflicts::Observation::IndirectConflict(conflict) => {
                println!(
                    "  Observation {}: Indirect conflict - {}",
                    i + 1,
                    conflict.cause
                );
            }
            conflicts::Observation::Merge {
                records,
                interval,
                reason,
                ..
            } => {
                println!(
                    "  Observation {}: Merge of records {:?} at interval {} - {}",
                    i + 1,
                    records,
                    interval,
                    reason
                );
            }
        }
    }

    // Export knowledge graph
    let graph = last_graph.unwrap_or_else(|| {
        unirust
            .export_graph(&clusters, &observations)
            .expect("Graph export failed")
    });
    println!("\nKnowledge graph:");
    println!("  - {} nodes", graph.num_nodes());
    println!("  - {} SAME_AS edges", graph.num_same_as_edges());
    println!(
        "  - {} CONFLICTS_WITH edges",
        graph.num_conflicts_with_edges()
    );

    // Export to DOT format for graph visualization
    let dot = unirust.export_dot(&clusters, &observations)?;
    println!("\nDOT format (for visualization):");
    println!("{}", dot);

    // Export text summary
    let summary = unirust.export_text_summary(&clusters, &observations)?;
    println!("\n{}", summary);

    println!("\n=== Example completed successfully! ===");
    Ok(())
}
