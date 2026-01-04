//! # In-Memory Entity Resolution Example
//!
//! This example demonstrates unirust's core entity resolution capability
//! using only in-memory storage. Perfect for learning, testing, and
//! small datasets.
//!
//! ## What This Example Shows
//!
//! 1. Creating an ontology (matching rules)
//! 2. Ingesting records from multiple source systems
//! 3. Automatic entity clustering based on identity keys
//! 4. Conflict detection when records disagree
//! 5. Querying the unified view
//!
//! ## Run It
//!
//! ```bash
//! cargo run --example in_memory
//! ```

use unirust_rs::model::{Descriptor, Record, RecordId, RecordIdentity};
use unirust_rs::ontology::{IdentityKey, Ontology, StrongIdentifier};
use unirust_rs::temporal::Interval;
use unirust_rs::{QueryDescriptor, Unirust};

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║           Unirust In-Memory Entity Resolution                ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // =========================================================================
    // Step 1: Create an Ontology
    // =========================================================================
    //
    // The ontology defines HOW records are matched:
    // - Identity Keys: Attributes that, when matching, indicate same entity
    // - Strong Identifiers: Unique attributes that prevent false merges
    //
    // Think of identity keys as "these records MIGHT be the same entity"
    // and strong identifiers as "these records are DEFINITELY different if this differs"

    let mut ontology = Ontology::new();
    let mut engine = Unirust::new(ontology.clone());

    // Create attribute IDs (interned strings for efficiency)
    let name_attr = engine.intern_attr("name");
    let email_attr = engine.intern_attr("email");
    let _phone_attr = engine.intern_attr("phone");
    let ssn_attr = engine.intern_attr("ssn");

    // Identity Key: Records with same (name + email) are candidates for merging
    ontology.add_identity_key(IdentityKey::new(
        vec![name_attr, email_attr],
        "name_email".to_string(),
    ));

    // Strong Identifier: SSN uniquely identifies a person
    // If two records have different SSNs, they CANNOT be merged
    ontology.add_strong_identifier(StrongIdentifier::new(ssn_attr, "ssn_unique".to_string()));

    println!("Ontology configured:");
    println!("  - Identity Key: name + email (records match if both are equal)");
    println!("  - Strong ID: SSN (prevents merging if different)\n");

    // Recreate engine with the configured ontology
    let mut engine = Unirust::new(ontology);

    // Re-intern attributes (required after creating new engine)
    let name_attr = engine.intern_attr("name");
    let email_attr = engine.intern_attr("email");
    let phone_attr = engine.intern_attr("phone"); // Used in records below
    let ssn_attr = engine.intern_attr("ssn");

    // =========================================================================
    // Step 2: Create Records from Multiple Source Systems
    // =========================================================================
    //
    // Imagine we have data from three systems: CRM, ERP, and a Web app.
    // Each record has:
    // - RecordId: Unique identifier within unirust
    // - RecordIdentity: Source system info (entity_type, perspective, uid)
    // - Descriptors: Attribute values with temporal validity intervals

    // Intern the values we'll use
    let john_name = engine.intern_value("John Doe");
    let john_email = engine.intern_value("john@example.com");
    let ssn_123 = engine.intern_value("123-45-6789");
    let phone_1 = engine.intern_value("555-1234");
    let phone_2 = engine.intern_value("555-5678"); // Different phone - will cause conflict!

    let jane_name = engine.intern_value("Jane Smith");
    let jane_email = engine.intern_value("jane@example.com");
    let ssn_987 = engine.intern_value("987-65-4321");
    let phone_3 = engine.intern_value("555-9999");

    // Time intervals: [start, end) representing when data is valid
    let interval_2022 = Interval::new(202201, 202301).unwrap();
    let interval_2023 = Interval::new(202301, 202401).unwrap();

    println!("Creating records from multiple source systems...\n");

    // Record 1: John from CRM (2022)
    let john_crm = Record::new(
        RecordId(1),
        RecordIdentity::new("person".into(), "crm".into(), "CRM-001".into()),
        vec![
            Descriptor::new(name_attr, john_name, interval_2022),
            Descriptor::new(email_attr, john_email, interval_2022),
            Descriptor::new(phone_attr, phone_1, interval_2022),
            Descriptor::new(ssn_attr, ssn_123, interval_2022),
        ],
    );

    // Record 2: John from ERP (2023) - Same person, different phone!
    let john_erp = Record::new(
        RecordId(2),
        RecordIdentity::new("person".into(), "erp".into(), "ERP-001".into()),
        vec![
            Descriptor::new(name_attr, john_name, interval_2023),
            Descriptor::new(email_attr, john_email, interval_2023),
            Descriptor::new(phone_attr, phone_2, interval_2023), // <-- Different phone
            Descriptor::new(ssn_attr, ssn_123, interval_2023),
        ],
    );

    // Record 3: Jane from CRM - Different person entirely
    let jane_crm = Record::new(
        RecordId(3),
        RecordIdentity::new("person".into(), "crm".into(), "CRM-002".into()),
        vec![
            Descriptor::new(name_attr, jane_name, interval_2022),
            Descriptor::new(email_attr, jane_email, interval_2022),
            Descriptor::new(phone_attr, phone_3, interval_2022),
            Descriptor::new(ssn_attr, ssn_987, interval_2022),
        ],
    );

    // =========================================================================
    // Step 3: Ingest Records
    // =========================================================================

    let result = engine.ingest(vec![john_crm, john_erp, jane_crm])?;

    println!("Ingestion Results:");
    println!("  - Records processed: {}", result.assignments.len());
    println!("  - Clusters formed: {}", result.cluster_count);
    println!("  - Conflicts detected: {}", result.conflicts.len());
    println!();

    // Show cluster assignments
    println!("Cluster Assignments:");
    for assignment in &result.assignments {
        println!(
            "  Record {} -> Cluster {}",
            assignment.record_id.0, assignment.cluster_id.0
        );
    }
    println!();

    // =========================================================================
    // Step 4: Examine Conflicts
    // =========================================================================

    if !result.conflicts.is_empty() {
        println!("Conflicts Detected:");
        for conflict in &result.conflicts {
            let attr_name = conflict.attribute.as_deref().unwrap_or("unknown");
            let cause = conflict.cause.as_deref().unwrap_or("value mismatch");
            println!(
                "  - {} conflict at {}: {}",
                attr_name, conflict.interval, cause
            );
            println!("    Records involved: {:?}", conflict.records);
        }
        println!();
    }

    // =========================================================================
    // Step 5: Query the Unified View
    // =========================================================================

    println!("Querying for 'John Doe' with email 'john@example.com'...");

    let query_name = engine.intern_value("John Doe");
    let query_email = engine.intern_value("john@example.com");

    let query_result = engine.query(
        &[
            QueryDescriptor {
                attr: name_attr,
                value: query_name,
            },
            QueryDescriptor {
                attr: email_attr,
                value: query_email,
            },
        ],
        Interval::new(202201, 202401).unwrap(),
    )?;

    match query_result {
        unirust_rs::QueryOutcome::Matches(matches) => {
            if matches.is_empty() {
                println!("  No matches found");
            } else {
                println!("  Found {} matching cluster(s):", matches.len());
                for m in &matches {
                    println!(
                        "    Cluster {} at interval {} with {} golden attributes",
                        m.cluster_id.0,
                        m.interval,
                        m.golden.len()
                    );
                }
            }
        }
        unirust_rs::QueryOutcome::Conflict(conflict) => {
            println!("  Query resulted in conflict: {:?}", conflict);
        }
    }

    // =========================================================================
    // Step 6: Get Statistics
    // =========================================================================

    println!("\nEngine Statistics:");
    let stats = engine.stats();
    println!("  - Total records: {}", stats.record_count);
    println!("  - Total clusters: {}", stats.cluster_count);
    println!("  - Merges performed: {}", stats.merges_performed);

    println!("\n✓ Example completed successfully!");
    println!("\nNext steps:");
    println!("  - Try 'cargo run --example persistent_shard' for persistence");
    println!("  - Try 'cargo run --example cluster' for distributed mode");

    Ok(())
}
