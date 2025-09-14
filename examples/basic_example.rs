//! # Basic Example
//! 
//! Demonstrates the core functionality of unirust with a simple example
//! showing entity resolution, conflict detection, and knowledge graph export.

use unirust::*;
use unirust::temporal::Interval;
use unirust::model::{Record, RecordIdentity, Descriptor, RecordId};
use unirust::ontology::{Ontology, IdentityKey, StrongIdentifier, Constraint};
use unirust::linker::build_clusters_optimized;
use unirust::utils;


fn main() -> anyhow::Result<()> {
    println!("=== Unirust Basic Example ===\n");

    // Create an ontology
    let mut ontology = Ontology::new();
    
    // Create a store first to get the interner
    let mut store = Store::new();
    
    // Use the store's interner to create attribute and value IDs
    let name_attr = store.interner_mut().intern_attr("name");
    let email_attr = store.interner_mut().intern_attr("email");
    let phone_attr = store.interner_mut().intern_attr("phone");
    let ssn_attr = store.interner_mut().intern_attr("ssn");
    
    // Define identity keys (attributes that must match for records to be considered the same entity)
    let identity_key = IdentityKey::new(vec![name_attr, email_attr], "name_email".to_string());
    ontology.add_identity_key(identity_key);
    println!("Identity key attributes: {:?}", vec![name_attr, email_attr]);
    
    // Define strong identifiers (attributes that prevent merging when they conflict)
    let strong_id = StrongIdentifier::new(ssn_attr, "ssn_unique".to_string());
    ontology.add_strong_identifier(strong_id);
    
    // Define constraints
    let unique_email = Constraint::unique(email_attr, "unique_email".to_string());
    ontology.add_constraint(unique_email);
    
    println!("Created ontology with identity keys, strong identifiers, and constraints");

    // Create values for the records
    let name_value1 = store.interner_mut().intern_value("John Doe");
    let email_value1 = store.interner_mut().intern_value("john@example.com");
    let phone_value1 = store.interner_mut().intern_value("555-1234");
    let ssn_value1 = store.interner_mut().intern_value("123-45-6789");
    
    let name_value2 = store.interner_mut().intern_value("Jane Smith");
    let email_value2 = store.interner_mut().intern_value("jane@example.com");
    let phone_value2 = store.interner_mut().intern_value("555-5678");
    let ssn_value2 = store.interner_mut().intern_value("987-65-4321");
    
    let phone_value3 = store.interner_mut().intern_value("555-9999");
    let phone_value4 = store.interner_mut().intern_value("555-0000");
    let email_value3 = store.interner_mut().intern_value("john.doe@example.com");
    
    // Record 1: John Doe from CRM system
    let record1 = Record::new(
        RecordId(1),
        RecordIdentity::new("person".to_string(), "crm".to_string(), "crm_001".to_string()),
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
        RecordIdentity::new("person".to_string(), "erp".to_string(), "erp_001".to_string()),
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
        RecordIdentity::new("person".to_string(), "crm".to_string(), "crm_002".to_string()),
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
        RecordIdentity::new("person".to_string(), "web".to_string(), "web_001".to_string()),
        vec![
            Descriptor::new(name_attr, name_value1, Interval::new(180, 280).unwrap()),
            Descriptor::new(email_attr, email_value1, Interval::new(180, 280).unwrap()),
            Descriptor::new(phone_attr, phone_value4, Interval::new(180, 280).unwrap()),
            Descriptor::new(ssn_attr, ssn_value1, Interval::new(180, 280).unwrap()), // Same SSN as records 1 and 2
        ],
    );
    
    // Record 5: Another John Doe with conflicting email
    let record5 = Record::new(
        RecordId(5),
        RecordIdentity::new("person".to_string(), "mobile".to_string(), "mobile_001".to_string()),
        vec![
            Descriptor::new(name_attr, name_value1, Interval::new(200, 300).unwrap()),
            Descriptor::new(email_attr, email_value3, Interval::new(200, 300).unwrap()), // Different email!
            Descriptor::new(phone_attr, phone_value4, Interval::new(200, 300).unwrap()),
            Descriptor::new(ssn_attr, ssn_value1, Interval::new(200, 300).unwrap()),
        ],
    );
    
    store.add_records(vec![record1, record2, record3, record4, record5])?;
    println!("Added 5 records to the store");

    // Build clusters using entity resolution (optimized)
    let clusters = build_clusters_optimized(&store, &ontology)?;
    println!("Built {} clusters", clusters.len());
    
    for (i, cluster) in clusters.clusters.iter().enumerate() {
        println!("  Cluster {}: {} records", i + 1, cluster.records.len());
        for record_id in &cluster.records {
            if let Some(record) = store.get_record(*record_id) {
                println!("    - {} ({}:{})", record_id, record.identity.perspective, record.identity.uid);
            }
        }
    }

    // Detect conflicts
    let observations = conflicts::detect_conflicts(&store, &clusters, &ontology)?;
    println!("\nDetected {} observations", observations.len());
    
    for (i, observation) in observations.iter().enumerate() {
        match observation {
            conflicts::Observation::DirectConflict(conflict) => {
                println!("  Observation {}: Direct conflict in attribute {} at interval {}", 
                    i + 1, conflict.attribute.0, conflict.interval);
            }
            conflicts::Observation::IndirectConflict(conflict) => {
                println!("  Observation {}: Indirect conflict - {}", 
                    i + 1, conflict.cause);
            }
            conflicts::Observation::Merge { records, interval, reason, .. } => {
                println!("  Observation {}: Merge of records {:?} at interval {} - {}", 
                    i + 1, records, interval, reason);
            }
        }
    }

    // Export knowledge graph
    let graph = graph::export_graph(&store, &clusters, &observations, &ontology)?;
    println!("\nExported knowledge graph:");
    println!("  - {} nodes", graph.num_nodes());
    println!("  - {} SAME_AS edges", graph.num_same_as_edges());
    println!("  - {} CONFLICTS_WITH edges", graph.num_conflicts_with_edges());

    // Export to JSONL
    let jsonl = graph.to_jsonl()?;
    println!("\nKnowledge graph exported to JSONL format:");
    println!("{}", jsonl);

    // Export to DOT format for graph visualization
    let dot = utils::export_to_dot(&store, &clusters, &observations, &ontology)?;
    println!("\nKnowledge graph exported to DOT format:");
    println!("{}", dot);

    // Generate graph visualizations
    println!("\nGenerating graph visualizations...");
    utils::generate_graph_visualizations(&store, &clusters, &observations, &ontology, "knowledge_graph")?;

    // Export text summary
    let summary = utils::export_to_text_summary(&store, &clusters, &observations)?;
    println!("\n{}", summary);

    println!("\n=== Example completed successfully! ===");
    Ok(())
}

