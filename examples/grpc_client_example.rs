//! # gRPC Client Example
//!
//! Demonstrates the core functionality of unirust gRPC API with the same workflow
//! as basic_example.rs but using the gRPC client interface.

use unirust::grpc_server::unirust::unirust_service_client::UnirustServiceClient;
use unirust::grpc_server::unirust::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Unirust gRPC Client Example ===\n");

    // Connect to the gRPC server
    let mut client = UnirustServiceClient::connect("http://127.0.0.1:50051").await?;

    // Health check
    let health_response = client.health_check(HealthCheckRequest {}).await?;
    println!(
        "Server health: {} (version: {})",
        health_response.get_ref().status,
        health_response.get_ref().version
    );

    // Create an ontology
    let ontology = Ontology {
        identity_keys: vec![IdentityKey {
            attributes: vec!["name".to_string(), "email".to_string()],
            name: "name_email".to_string(),
        }],
        strong_identifiers: vec![StrongIdentifier {
            attribute: "ssn".to_string(),
            name: "ssn_unique".to_string(),
        }],
        crosswalks: vec![],
        constraints: vec![Constraint {
            constraint_type: Some(constraint::ConstraintType::Unique(UniqueConstraint {
                attribute: "email".to_string(),
                name: "unique_email".to_string(),
            })),
        }],
        entity_types: vec![],
        perspective_weights: std::collections::HashMap::from([
            ("crm".to_string(), 100),
            ("erp".to_string(), 90),
            ("web".to_string(), 80),
            ("mobile".to_string(), 70),
        ]),
        perspective_permanent_attributes: std::collections::HashMap::new(),
    };

    // Create session with ontology
    let create_response = client
        .create_ontology(CreateOntologyRequest {
            ontology: Some(ontology),
        })
        .await?;

    let session_id = create_response.get_ref().session_id.clone();
    println!("Created session: {}", session_id);

    // Create test records (same as basic_example.rs)
    let records = vec![
        // Record 1: John Doe from CRM system
        Record {
            id: 1,
            identity: Some(RecordIdentity {
                entity_type: "person".to_string(),
                perspective: "crm".to_string(),
                uid: "crm_001".to_string(),
            }),
            descriptors: vec![
                Descriptor {
                    attribute: "name".to_string(),
                    value: "John Doe".to_string(),
                    interval: Some(Interval { start: 100, end: 200 }),
                },
                Descriptor {
                    attribute: "email".to_string(),
                    value: "john@example.com".to_string(),
                    interval: Some(Interval { start: 100, end: 200 }),
                },
                Descriptor {
                    attribute: "phone".to_string(),
                    value: "555-1234".to_string(),
                    interval: Some(Interval { start: 100, end: 200 }),
                },
                Descriptor {
                    attribute: "ssn".to_string(),
                    value: "123-45-6789".to_string(),
                    interval: Some(Interval { start: 100, end: 200 }),
                },
            ],
        },
        // Record 2: John Doe from ERP system (same person, different system)
        Record {
            id: 2,
            identity: Some(RecordIdentity {
                entity_type: "person".to_string(),
                perspective: "erp".to_string(),
                uid: "erp_001".to_string(),
            }),
            descriptors: vec![
                Descriptor {
                    attribute: "name".to_string(),
                    value: "John Doe".to_string(),
                    interval: Some(Interval { start: 150, end: 250 }),
                },
                Descriptor {
                    attribute: "email".to_string(),
                    value: "john@example.com".to_string(),
                    interval: Some(Interval { start: 150, end: 250 }),
                },
                Descriptor {
                    attribute: "phone".to_string(),
                    value: "555-9999".to_string(),
                    interval: Some(Interval { start: 150, end: 250 }),
                },
                Descriptor {
                    attribute: "ssn".to_string(),
                    value: "123-45-6789".to_string(),
                    interval: Some(Interval { start: 150, end: 250 }),
                },
            ],
        },
        // Record 3: Jane Smith from CRM system
        Record {
            id: 3,
            identity: Some(RecordIdentity {
                entity_type: "person".to_string(),
                perspective: "crm".to_string(),
                uid: "crm_002".to_string(),
            }),
            descriptors: vec![
                Descriptor {
                    attribute: "name".to_string(),
                    value: "Jane Smith".to_string(),
                    interval: Some(Interval { start: 100, end: 200 }),
                },
                Descriptor {
                    attribute: "email".to_string(),
                    value: "jane@example.com".to_string(),
                    interval: Some(Interval { start: 100, end: 200 }),
                },
                Descriptor {
                    attribute: "phone".to_string(),
                    value: "555-5678".to_string(),
                    interval: Some(Interval { start: 100, end: 200 }),
                },
                Descriptor {
                    attribute: "ssn".to_string(),
                    value: "987-65-4321".to_string(),
                    interval: Some(Interval { start: 100, end: 200 }),
                },
            ],
        },
        // Record 4: John Doe from web system (same person, same SSN)
        Record {
            id: 4,
            identity: Some(RecordIdentity {
                entity_type: "person".to_string(),
                perspective: "web".to_string(),
                uid: "web_001".to_string(),
            }),
            descriptors: vec![
                Descriptor {
                    attribute: "name".to_string(),
                    value: "John Doe".to_string(),
                    interval: Some(Interval { start: 180, end: 280 }),
                },
                Descriptor {
                    attribute: "email".to_string(),
                    value: "john@example.com".to_string(),
                    interval: Some(Interval { start: 180, end: 280 }),
                },
                Descriptor {
                    attribute: "phone".to_string(),
                    value: "555-0000".to_string(),
                    interval: Some(Interval { start: 180, end: 280 }),
                },
                Descriptor {
                    attribute: "ssn".to_string(),
                    value: "123-45-6789".to_string(),
                    interval: Some(Interval { start: 180, end: 280 }),
                },
            ],
        },
        // Record 5: Another John Doe with conflicting email
        Record {
            id: 5,
            identity: Some(RecordIdentity {
                entity_type: "person".to_string(),
                perspective: "mobile".to_string(),
                uid: "mobile_001".to_string(),
            }),
            descriptors: vec![
                Descriptor {
                    attribute: "name".to_string(),
                    value: "John Doe".to_string(),
                    interval: Some(Interval { start: 200, end: 300 }),
                },
                Descriptor {
                    attribute: "email".to_string(),
                    value: "john.doe@example.com".to_string(), // Different email!
                    interval: Some(Interval { start: 200, end: 300 }),
                },
                Descriptor {
                    attribute: "phone".to_string(),
                    value: "555-0000".to_string(),
                    interval: Some(Interval { start: 200, end: 300 }),
                },
                Descriptor {
                    attribute: "ssn".to_string(),
                    value: "123-45-6789".to_string(),
                    interval: Some(Interval { start: 200, end: 300 }),
                },
            ],
        },
    ];

    // Ingest records
    let ingest_response = client
        .ingest_records(IngestRecordsRequest {
            session_id: session_id.clone(),
            records,
        })
        .await?;
    println!("Ingested {} records", ingest_response.get_ref().records_ingested);

    // Get session info
    let session_info = client
        .get_session_info(GetSessionInfoRequest {
            session_id: session_id.clone(),
        })
        .await?;
    let info = session_info.get_ref();
    println!("Session info: {} records, {} attributes, {} perspectives", 
             info.record_count, 
             info.available_attributes.len(),
             info.available_perspectives.len());

    // Build clusters using entity resolution (optimized)
    let clusters_response = client
        .build_clusters(BuildClustersRequest {
            session_id: session_id.clone(),
            use_optimized: true,
        })
        .await?;

    let clusters = clusters_response.get_ref().clusters.as_ref().unwrap();
    println!("Built {} clusters", clusters.clusters.len());

    for (i, cluster) in clusters.clusters.iter().enumerate() {
        println!("  Cluster {}: {} records", i + 1, cluster.records.len());
        for record_id in &cluster.records {
            println!("    - R{}", record_id);
        }
    }

    // Detect conflicts
    let conflicts_response = client
        .detect_conflicts(DetectConflictsRequest {
            session_id: session_id.clone(),
            clusters: Some(clusters.clone()),
        })
        .await?;

    let observations = &conflicts_response.get_ref().observations;
    println!("\nDetected {} observations", observations.len());

    for (i, observation) in observations.iter().enumerate() {
        match &observation.observation_type {
            Some(observation::ObservationType::DirectConflict(conflict)) => {
                println!(
                    "  Observation {}: Direct conflict in attribute {} at interval [{}, {}]",
                    i + 1,
                    conflict.attribute,
                    conflict.interval.as_ref().map(|i| i.start).unwrap_or(0),
                    conflict.interval.as_ref().map(|i| i.end).unwrap_or(0)
                );
            }
            Some(observation::ObservationType::IndirectConflict(conflict)) => {
                println!(
                    "  Observation {}: Indirect conflict - {}",
                    i + 1,
                    conflict.cause
                );
            }
            Some(observation::ObservationType::Merge(merge)) => {
                println!(
                    "  Observation {}: Merge of records {:?} at interval [{}, {}] - {}",
                    i + 1,
                    merge.records,
                    merge.interval.as_ref().map(|i| i.start).unwrap_or(0),
                    merge.interval.as_ref().map(|i| i.end).unwrap_or(0),
                    merge.reason
                );
            }
            None => {
                println!("  Observation {}: Unknown observation type", i + 1);
            }
        }
    }

    // Export knowledge graph
    let kg_response = client
        .export_knowledge_graph(ExportKnowledgeGraphRequest {
            session_id: session_id.clone(),
            clusters: Some(clusters.clone()),
            observations: observations.clone(),
        })
        .await?;

    if kg_response.get_ref().success {
        println!("\nKnowledge graph exported successfully");
        // In a real implementation, you'd process the graph data
    }

    // Export formats
    let export_response = client
        .export_formats(ExportFormatsRequest {
            session_id: session_id.clone(),
            clusters: Some(clusters.clone()),
            observations: observations.clone(),
            formats: vec![
                "jsonl".to_string(),
                "dot".to_string(),
                "text_summary".to_string(),
            ],
        })
        .await?;

    if export_response.get_ref().success {
        println!("\nExported formats:");
        for (format, content) in &export_response.get_ref().exports {
            println!("  {}: {} characters", format, content.len());
            if format == "text_summary" {
                println!("Text Summary:");
                println!("{}", content);
            }
        }
    }

    // Clean up - delete session
    let delete_response = client
        .delete_session(DeleteSessionRequest { session_id })
        .await?;

    if delete_response.get_ref().success {
        println!("\nSession deleted successfully");
    }

    println!("\n=== gRPC Client Example completed successfully! ===");
    Ok(())
}