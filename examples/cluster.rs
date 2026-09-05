//! # Distributed Cluster Example
//!
//! This example demonstrates unirust in distributed mode with a router
//! and multiple shards. This is the production deployment model for
//! large-scale entity resolution.
//!
//! ## Architecture
//!
//! ```text
//!                     ┌─────────────┐
//!                     │   Router    │ :50060
//!                     └──────┬──────┘
//!                ┌───────────┼───────────┐
//!                ▼           ▼           ▼
//!          ┌─────────┐ ┌─────────┐ ┌─────────┐
//!          │ Shard 0 │ │ Shard 1 │ │ Shard 2 │
//!          │ :50061  │ │ :50062  │ │ :50063  │
//!          └─────────┘ └─────────┘ └─────────┘
//! ```
//!
//! ## Prerequisites
//!
//! Use a dedicated, fresh persistent cluster: this example replaces its ontology
//! and ingests sample records. Follow the local demonstration in README.md to
//! start three shards and a router. Do not run against an existing dataset.
//!
//! ## Run It
//!
//! ```bash
//! cargo run --locked --example cluster -- http://127.0.0.1:50060
//! ```
//!
//! The router URI is optional and defaults to the address above. The integer
//! interval endpoints below are an application convention, not parsed dates.

use unirust_rs::distributed::proto::query_entities_response::Outcome;
use unirust_rs::distributed::proto::router_service_client::RouterServiceClient;
use unirust_rs::distributed::proto::{
    ApplyOntologyRequest, ConstraintConfig, ConstraintKind, IdentityKeyConfig,
    IngestRecordsRequest, OntologyConfig, QueryDescriptor, QueryEntitiesRequest, ReconcileRequest,
    RecordDescriptor, RecordIdentity, RecordInput, StatsRequest,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║           Unirust Distributed Cluster Example                ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // =========================================================================
    // Step 1: Connect to the Router
    // =========================================================================

    let router_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:50060".to_string());
    println!("Connecting to router at {}...", router_addr);

    let mut client = match RouterServiceClient::connect(router_addr.clone()).await {
        Ok(c) => {
            println!("  Connected successfully!\n");
            c
        }
        Err(e) => {
            eprintln!("\nError: Could not connect to router at {}", router_addr);
            eprintln!("  {}\n", e);
            eprintln!("Start a fresh persistent demo cluster using the README instructions.");
            return Err(e.into());
        }
    };

    // =========================================================================
    // Step 2: Configure Ontology
    // =========================================================================
    //
    // In distributed mode, ontology is sent to the router, which propagates
    // it to all shards. This ensures consistent matching rules across the cluster.

    println!("Configuring ontology...");

    let ontology = OntologyConfig {
        identity_keys: vec![
            IdentityKeyConfig {
                name: "name_email".to_string(),
                attributes: vec!["name".to_string(), "email".to_string()],
            },
            IdentityKeyConfig {
                name: "phone_only".to_string(),
                attributes: vec!["phone".to_string()],
            },
        ],
        strong_identifiers: vec!["ssn".to_string()],
        constraints: vec![ConstraintConfig {
            name: "unique_email".to_string(),
            attribute: "email".to_string(),
            kind: ConstraintKind::Unique.into(),
        }],
    };

    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(ontology),
        })
        .await?;

    println!("  Ontology applied to cluster\n");

    // =========================================================================
    // Step 3: Ingest Records
    // =========================================================================
    //
    // Records are automatically distributed across shards based on their
    // identity key signatures. Placement favors local matches, while explicit
    // reconciliation below resolves identities that span multiple shards.

    println!("Creating sample records...");

    let mut records = Vec::new();

    // Create 50 people from different source systems
    for i in 0..50 {
        // Main record from CRM
        records.push(RecordInput {
            index: i * 2,
            identity: Some(RecordIdentity {
                entity_type: "person".to_string(),
                perspective: "crm".to_string(),
                uid: format!("CRM-{:04}", i),
            }),
            descriptors: vec![
                RecordDescriptor {
                    attr: "name".to_string(),
                    value: format!("Person {}", i),
                    start: 202401,
                    end: 202501,
                },
                RecordDescriptor {
                    attr: "email".to_string(),
                    value: format!("person{}@example.com", i),
                    start: 202401,
                    end: 202501,
                },
                RecordDescriptor {
                    attr: "phone".to_string(),
                    value: format!("555-{:04}", i),
                    start: 202401,
                    end: 202501,
                },
                RecordDescriptor {
                    attr: "ssn".to_string(),
                    value: format!("{:03}-{:02}-{:04}", i / 100, i % 100, i),
                    start: 202401,
                    end: 202501,
                },
            ],
        });

        // Every 5th person has a duplicate from ERP (will be merged)
        if i % 5 == 0 {
            records.push(RecordInput {
                index: i * 2 + 1,
                identity: Some(RecordIdentity {
                    entity_type: "person".to_string(),
                    perspective: "erp".to_string(),
                    uid: format!("ERP-{:04}", i),
                }),
                descriptors: vec![
                    RecordDescriptor {
                        attr: "name".to_string(),
                        value: format!("Person {}", i), // Same name
                        start: 202406,
                        end: 202501,
                    },
                    RecordDescriptor {
                        attr: "email".to_string(),
                        value: format!("person{}@example.com", i), // Same email -> match!
                        start: 202406,
                        end: 202501,
                    },
                    RecordDescriptor {
                        attr: "phone".to_string(),
                        value: format!("555-{:04}", i + 1000), // Different phone observation
                        start: 202406,
                        end: 202501,
                    },
                    RecordDescriptor {
                        attr: "ssn".to_string(),
                        value: format!("{:03}-{:02}-{:04}", i / 100, i % 100, i), // Same SSN
                        start: 202406,
                        end: 202501,
                    },
                ],
            });
        }
    }

    println!("  Created {} records\n", records.len());

    println!("Ingesting records into cluster...");
    let start = std::time::Instant::now();

    let response = client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 0,
            records,
        })
        .await?
        .into_inner();

    let elapsed = start.elapsed();

    println!("Ingestion complete:");
    println!("  - Records assigned: {}", response.assignments.len());
    println!("  - Time: {:?}", elapsed);
    println!(
        "  - Rate: {:.0} records/sec\n",
        response.assignments.len() as f64 / elapsed.as_secs_f64()
    );

    // Show how records are distributed across shards
    let mut shard_counts = std::collections::HashMap::new();
    for assignment in &response.assignments {
        *shard_counts.entry(assignment.shard_id).or_insert(0u32) += 1;
    }

    println!("Distribution across shards:");
    for (shard, count) in &shard_counts {
        println!("  - Shard {}: {} records", shard, count);
    }
    println!();

    // =========================================================================
    // Step 4: Query Entities
    // =========================================================================

    // Ingestion resolves locally. Reconcile before expecting cross-shard
    // fragments to appear as one canonical entity in the router query.
    client
        .reconcile(ReconcileRequest {
            shard_metadata: Vec::new(),
        })
        .await?;

    println!("Querying for Person 0...");

    let query_response = client
        .query_entities(QueryEntitiesRequest {
            descriptors: vec![
                QueryDescriptor {
                    attr: "name".to_string(),
                    value: "Person 0".to_string(),
                },
                QueryDescriptor {
                    attr: "email".to_string(),
                    value: "person0@example.com".to_string(),
                },
            ],
            start: 202401,
            end: 202501,
        })
        .await?
        .into_inner();

    println!("Query result:");
    match query_response.outcome {
        Some(Outcome::Matches(matches)) => {
            if matches.matches.is_empty() {
                println!("  No matches found");
            } else {
                for (i, m) in matches.matches.iter().enumerate() {
                    println!(
                        "  Match {}: Shard {} Cluster {} ({} golden attrs)",
                        i + 1,
                        m.shard_id,
                        m.cluster_id,
                        m.golden.len()
                    );
                }
            }
        }
        Some(Outcome::Conflict(conflict)) => {
            println!(
                "  Conflict detected: {} clusters claim the same identity",
                conflict.clusters.len()
            );
        }
        None => {
            println!("  No matches found");
        }
    }
    println!();

    // =========================================================================
    // Step 5: Get Cluster Statistics
    // =========================================================================

    println!("Cluster statistics:");

    let stats_response = client.get_stats(StatsRequest {}).await?.into_inner();

    println!("  - Total records: {}", stats_response.record_count);
    println!(
        "  - Sum of shard-local clusters: {}",
        stats_response.cluster_count
    );
    println!("  - Conflicts: {}", stats_response.conflict_count);
    println!("  - Graph nodes: {}", stats_response.graph_node_count);
    println!("  - Graph edges: {}", stats_response.graph_edge_count);
    println!(
        "  - Cross-shard merges: {}",
        stats_response.cross_shard_merges
    );
    println!(
        "  - Boundary keys tracked: {}",
        stats_response.boundary_keys_tracked
    );

    println!("\n✓ Example completed successfully!");
    println!("Use the demo shell's cleanup instructions to stop its processes.");
    println!("See README.md for benchmarking on a separate fresh dataset.");

    Ok(())
}
