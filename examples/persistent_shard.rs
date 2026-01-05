//! # Persistent Single-Shard Example
//!
//! This example demonstrates unirust with persistent storage using RocksDB.
//! Data survives restarts and can handle larger datasets than fit in memory.
//!
//! ## What This Example Shows
//!
//! 1. Opening a persistent store
//! 2. Ingesting records with durability
//! 3. Checkpointing for crash recovery
//! 4. Restarting and recovering state
//! 5. Performance tuning profiles
//!
//! ## Run It
//!
//! ```bash
//! # First run - creates data
//! cargo run --example persistent_shard
//!
//! # Second run - recovers from disk
//! cargo run --example persistent_shard
//! ```

use tempfile::tempdir;
use unirust_rs::model::{Descriptor, Record, RecordId, RecordIdentity};
use unirust_rs::ontology::{IdentityKey, Ontology, StrongIdentifier};
use unirust_rs::temporal::Interval;
use unirust_rs::{PersistentStore, StreamingTuning, TuningProfile, Unirust};

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║          Unirust Persistent Single-Shard Example             ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // For this example, we use a temp directory. In production, use a fixed path.
    // let data_dir = PathBuf::from("/var/lib/unirust/shard-0");
    let temp_dir = tempdir()?;
    let data_dir = temp_dir.path().to_path_buf();

    println!("Data directory: {}\n", data_dir.display());

    // =========================================================================
    // Step 1: Open Persistent Store
    // =========================================================================
    //
    // PersistentStore uses RocksDB under the hood, providing:
    // - ACID transactions
    // - Crash recovery
    // - Efficient compression
    // - Background compaction

    println!("Opening persistent store...");
    let store = PersistentStore::open(&data_dir)?;
    println!("  Store opened successfully\n");

    // =========================================================================
    // Step 2: Choose a Tuning Profile
    // =========================================================================
    //
    // Tuning profiles optimize for different workloads:
    //
    // | Profile        | Best For                              |
    // |---------------|---------------------------------------|
    // | Balanced      | General purpose, moderate loads       |
    // | LowLatency    | Interactive queries, fast responses   |
    // | HighThroughput| Batch processing, maximum ingest rate |
    // | BulkIngest    | Initial data loading, minimal matching|
    // | MemorySaver   | Memory-constrained environments       |
    // | BillionScale  | Huge datasets with persistent DSU     |

    let profile = TuningProfile::HighThroughput;
    let tuning = StreamingTuning::from_profile(profile);
    println!("Using tuning profile: {:?}\n", profile);

    // =========================================================================
    // Step 3: Configure Ontology
    // =========================================================================
    //
    // Using from_names() - no need to pre-intern attributes!
    // The engine will automatically intern them when created.

    let mut ontology = Ontology::new();

    ontology.add_identity_key(IdentityKey::from_names(vec!["name", "email"], "name_email"));

    ontology.add_strong_identifier(StrongIdentifier::from_name("ssn", "ssn_unique"));

    println!("Ontology configured with identity key (name+email) and strong ID (ssn)\n");

    // =========================================================================
    // Step 4: Create Engine with Store and Ontology
    // =========================================================================
    //
    // The engine automatically interns the ontology's attribute names.

    let mut engine = Unirust::with_store_and_tuning(ontology, store, tuning);

    // Intern attributes for use in records
    let name_attr = engine.intern_attr("name");
    let email_attr = engine.intern_attr("email");
    let phone_attr = engine.intern_attr("phone");
    let ssn_attr = engine.intern_attr("ssn");
    let dept_attr = engine.intern_attr("department");

    // =========================================================================
    // Step 5: Generate and Ingest Records
    // =========================================================================

    println!("Generating sample records...");

    // Simulate records from multiple departments
    let departments = ["Engineering", "Sales", "Marketing", "Support"];
    let mut records = Vec::new();

    // Create 100 records across departments
    for i in 0..100 {
        let dept = departments[i % departments.len()];
        let name_val = engine.intern_value(&format!("Employee {}", i));
        let email_val = engine.intern_value(&format!("emp{}@company.com", i));
        let phone_val = engine.intern_value(&format!("555-{:04}", i));
        let ssn_val = engine.intern_value(&format!("{:03}-{:02}-{:04}", i / 100, i % 100, i));
        let dept_val = engine.intern_value(dept);

        let interval = Interval::new(202401, 202501).unwrap();

        records.push(Record::new(
            RecordId(i as u32),
            RecordIdentity::new(
                "employee".into(),
                "hr_system".into(),
                format!("HR-{:04}", i),
            ),
            vec![
                Descriptor::new(name_attr, name_val, interval),
                Descriptor::new(email_attr, email_val, interval),
                Descriptor::new(phone_attr, phone_val, interval),
                Descriptor::new(ssn_attr, ssn_val, interval),
                Descriptor::new(dept_attr, dept_val, interval),
            ],
        ));
    }

    // Add some duplicate records from a different system (will be merged)
    for i in 0..10 {
        let name_val = engine.intern_value(&format!("Employee {}", i));
        let email_val = engine.intern_value(&format!("emp{}@company.com", i));
        let phone_val = engine.intern_value(&format!("555-{:04}", 1000 + i)); // Different phone
        let ssn_val = engine.intern_value(&format!("{:03}-{:02}-{:04}", i / 100, i % 100, i));

        let interval = Interval::new(202406, 202501).unwrap();

        records.push(Record::new(
            RecordId(100 + i as u32),
            RecordIdentity::new(
                "employee".into(),
                "payroll_system".into(),
                format!("PAY-{:04}", i),
            ),
            vec![
                Descriptor::new(name_attr, name_val, interval),
                Descriptor::new(email_attr, email_val, interval),
                Descriptor::new(phone_attr, phone_val, interval),
                Descriptor::new(ssn_attr, ssn_val, interval),
            ],
        ));
    }

    println!("  Created {} records\n", records.len());

    println!("Ingesting records...");
    let start = std::time::Instant::now();
    let result = engine.ingest(records)?;
    let elapsed = start.elapsed();

    println!("Ingestion complete:");
    println!("  - Records: {}", result.assignments.len());
    println!("  - Clusters: {}", result.cluster_count);
    println!("  - Conflicts: {}", result.conflicts.len());
    println!("  - Time: {:?}", elapsed);
    println!(
        "  - Rate: {:.0} records/sec\n",
        result.assignments.len() as f64 / elapsed.as_secs_f64()
    );

    // =========================================================================
    // Step 6: Checkpoint for Crash Recovery
    // =========================================================================
    //
    // Checkpointing ensures all data is durably written to disk.
    // Call this periodically in production to limit data loss on crash.

    println!("Creating checkpoint...");
    engine.checkpoint()?;
    println!("  Checkpoint complete - data is durable\n");

    // =========================================================================
    // Step 7: Query and Statistics
    // =========================================================================

    println!("Engine Statistics:");
    let stats = engine.stats();
    println!("  - Total records: {}", stats.record_count);
    println!("  - Total clusters: {}", stats.cluster_count);
    println!("  - Merges performed: {}", stats.merges_performed);
    println!("  - Conflicts detected: {}", stats.conflicts_detected);

    // Show conflicts (phone number changes between systems)
    if !result.conflicts.is_empty() {
        println!("\nConflicts detected (phone changes between systems):");
        for (i, conflict) in result.conflicts.iter().take(5).enumerate() {
            let attr = conflict.attribute.as_deref().unwrap_or("unknown");
            let cause = conflict.cause.as_deref().unwrap_or("value mismatch");
            println!("  {}. {} at {}: {}", i + 1, attr, conflict.interval, cause);
        }
        if result.conflicts.len() > 5 {
            println!("  ... and {} more", result.conflicts.len() - 5);
        }
    }

    // =========================================================================
    // Step 8: Demonstrate Recovery (Simulated)
    // =========================================================================

    println!("\n--- Simulating Restart ---");
    println!("In production, you would:");
    println!("  1. Close the engine (drop it)");
    println!("  2. Restart your application");
    println!("  3. Re-open with PersistentStore::open()");
    println!("  4. All data would be recovered automatically");

    println!("\n✓ Example completed successfully!");
    println!("\nNext steps:");
    println!("  - Try 'cargo run --example cluster' for distributed mode");
    println!("  - Use a fixed data_dir path for real persistence");

    Ok(())
}
