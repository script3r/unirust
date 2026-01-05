//! Tests for entity temporal evolution.
//!
//! These tests verify that entities can be ingested incrementally over time
//! and produce the same results as batch ingestion. The key properties tested:
//!
//! 1. Incremental vs batch equivalence - ingesting descriptors one at a time
//!    should produce the same cluster state as ingesting all at once
//! 2. Order independence - ingestion order should not affect final state
//! 3. Temporal consistency - descriptors with different time ranges merge correctly
//! 4. Golden record consistency - merged golden records match regardless of ingestion path

#![allow(clippy::useless_vec)]

use std::collections::HashSet;
use std::net::SocketAddr;

#[allow(unused_imports)]
use tempfile::tempdir;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, ApplyOntologyRequest, IngestRecordsRequest,
    QueryDescriptor as ProtoQueryDescriptor, QueryEntitiesRequest, RecordDescriptor,
    RecordIdentity as ProtoRecordIdentity, RecordInput, StatsRequest,
};
use unirust_rs::distributed::{DistributedOntologyConfig, RouterNode, ShardNode};
use unirust_rs::{StreamingTuning, TuningProfile};

mod support;

async fn spawn_shard(
    shard_id: u32,
    config: DistributedOntologyConfig,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shard = ShardNode::new(
        shard_id,
        config,
        StreamingTuning::from_profile(TuningProfile::Balanced),
    )?;
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::shard_service_server::ShardServiceServer::new(shard))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("shard server");
    });
    Ok((addr, handle))
}

async fn spawn_router(
    shard_addrs: Vec<SocketAddr>,
    config: DistributedOntologyConfig,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shard_urls = shard_addrs
        .into_iter()
        .map(|addr| format!("http://{}", addr))
        .collect::<Vec<_>>();
    let router = RouterNode::connect(shard_urls, config)
        .await
        .expect("router connect");

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::router_service_server::RouterServiceServer::new(
                router,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("router server");
    });
    Ok((addr, handle))
}

fn record_input(
    index: u32,
    entity_type: &str,
    perspective: &str,
    uid: &str,
    descriptors: Vec<(&str, &str, i64, i64)>,
) -> RecordInput {
    RecordInput {
        index,
        identity: Some(ProtoRecordIdentity {
            entity_type: entity_type.to_string(),
            perspective: perspective.to_string(),
            uid: uid.to_string(),
        }),
        descriptors: descriptors
            .into_iter()
            .map(|(attr, value, start, end)| RecordDescriptor {
                attr: attr.to_string(),
                value: value.to_string(),
                start,
                end,
            })
            .collect(),
    }
}

/// Extract golden descriptors from query response as a sorted set for comparison.
fn extract_golden_set(
    response: &proto::QueryEntitiesResponse,
) -> HashSet<(String, String, i64, i64)> {
    match &response.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => matches
            .matches
            .iter()
            .flat_map(|m| &m.golden)
            .map(|g| (g.attr.clone(), g.value.clone(), g.start, g.end))
            .collect(),
        _ => HashSet::new(),
    }
}

/// Get cluster ID from query response.
fn extract_cluster_id(response: &proto::QueryEntitiesResponse) -> Option<(u32, u32)> {
    match &response.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => {
            matches.matches.first().map(|m| (m.shard_id, m.cluster_id))
        }
        _ => None,
    }
}

/// Count total matches.
fn count_matches(response: &proto::QueryEntitiesResponse) -> usize {
    match &response.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => matches.matches.len(),
        _ => 0,
    }
}

async fn setup_cluster() -> anyhow::Result<(RouterServiceClient<tonic::transport::Channel>,)> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _) = spawn_router(vec![shard0_addr, shard1_addr], empty_config).await?;

    let mut client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    Ok((client,))
}

/// Test that ingesting a single entity incrementally (one descriptor at a time)
/// produces the same cluster state as ingesting all descriptors at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_ingestion_equals_batch_ingestion() -> anyhow::Result<()> {
    // Setup: Two separate clusters, one for incremental, one for batch
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    // Cluster 1: Incremental ingestion
    let (shard0_addr_inc, _) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr_inc, _) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr_inc, _) =
        spawn_router(vec![shard0_addr_inc, shard1_addr_inc], empty_config.clone()).await?;
    let mut client_inc =
        RouterServiceClient::connect(format!("http://{}", router_addr_inc)).await?;
    client_inc
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    // Cluster 2: Batch ingestion
    let (shard0_addr_batch, _) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr_batch, _) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr_batch, _) =
        spawn_router(vec![shard0_addr_batch, shard1_addr_batch], empty_config).await?;
    let mut client_batch =
        RouterServiceClient::connect(format!("http://{}", router_addr_batch)).await?;
    client_batch
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    // Incremental: Ingest 3 different source records that share the same email (identity key).
    // Each is from a DIFFERENT source (different uid) so they're distinct records that merge.
    let record_c1 = record_input(
        0,
        "person",
        "crm",
        "record_001", // First source record
        vec![("email", "alice@example.com", 0, 100)],
    );
    let record_c2 = record_input(
        1,
        "person",
        "crm",
        "record_002", // Second source record - different uid
        vec![
            ("email", "alice@example.com", 0, 100),
            ("phone", "555-1234", 0, 100),
        ],
    );
    let record_c3 = record_input(
        2,
        "person",
        "crm",
        "record_003", // Third source record - different uid
        vec![
            ("email", "alice@example.com", 0, 100),
            ("name", "Alice Smith", 0, 100),
        ],
    );

    // Ingest incrementally
    client_inc
        .ingest_records(IngestRecordsRequest {
            records: vec![record_c1.clone()],
        })
        .await?;
    client_inc
        .ingest_records(IngestRecordsRequest {
            records: vec![record_c2.clone()],
        })
        .await?;
    client_inc
        .ingest_records(IngestRecordsRequest {
            records: vec![record_c3.clone()],
        })
        .await?;

    // Batch: Ingest all 3 at once
    client_batch
        .ingest_records(IngestRecordsRequest {
            records: vec![record_c1, record_c2, record_c3],
        })
        .await?;

    // Query both clusters for the entity by email
    let query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "alice@example.com".to_string(),
        }],
        start: 0,
        end: 100,
    };

    let response_inc = client_inc.query_entities(query.clone()).await?.into_inner();
    let response_batch = client_batch.query_entities(query).await?.into_inner();

    // Both should return exactly one match
    assert_eq!(count_matches(&response_inc), 1, "incremental: one match");
    assert_eq!(count_matches(&response_batch), 1, "batch: one match");

    // Golden records should contain the same descriptors
    let golden_inc = extract_golden_set(&response_inc);
    let golden_batch = extract_golden_set(&response_batch);

    assert_eq!(
        golden_inc, golden_batch,
        "golden records should match:\nincremental: {:?}\nbatch: {:?}",
        golden_inc, golden_batch
    );

    // Verify all three descriptors are present
    assert!(
        golden_inc
            .iter()
            .any(|(attr, value, _, _)| attr == "email" && value == "alice@example.com"),
        "email descriptor present"
    );
    assert!(
        golden_inc
            .iter()
            .any(|(attr, value, _, _)| attr == "phone" && value == "555-1234"),
        "phone descriptor present"
    );
    assert!(
        golden_inc
            .iter()
            .any(|(attr, value, _, _)| attr == "name" && value == "Alice Smith"),
        "name descriptor present"
    );

    Ok(())
}

/// Test that ingestion order does not affect the final entity state.
/// Ingesting C1→C2→C3 should produce same result as C3→C2→C1 or C2→C1→C3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingestion_order_independence() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    // Setup three clusters with different ingestion orders
    let mut clients = Vec::new();
    let orders = vec![
        vec![0, 1, 2], // C1, C2, C3
        vec![2, 1, 0], // C3, C2, C1
        vec![1, 0, 2], // C2, C1, C3
    ];

    for _ in 0..3 {
        let (shard0_addr, _) = spawn_shard(0, empty_config.clone()).await?;
        let (shard1_addr, _) = spawn_shard(1, empty_config.clone()).await?;
        let (router_addr, _) =
            spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;
        let mut client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
        client
            .set_ontology(ApplyOntologyRequest {
                config: Some(support::to_proto_config(&config)),
            })
            .await?;
        clients.push(client);
    }

    // Each record is from a different source (different uid) but shares the identity key (email).
    let records = vec![
        record_input(
            0,
            "person",
            "erp",
            "emp_100", // First source record
            vec![("email", "bob@corp.com", 0, 100)],
        ),
        record_input(
            1,
            "person",
            "erp",
            "emp_101", // Different source - but same email so they merge
            vec![
                ("email", "bob@corp.com", 0, 100),
                ("department", "Engineering", 0, 100),
            ],
        ),
        record_input(
            2,
            "person",
            "erp",
            "emp_102", // Different source - but same email so they merge
            vec![
                ("email", "bob@corp.com", 0, 100),
                ("title", "Senior Developer", 0, 100),
            ],
        ),
    ];

    // Ingest in different orders
    for (client, order) in clients.iter_mut().zip(&orders) {
        for &idx in order {
            client
                .ingest_records(IngestRecordsRequest {
                    records: vec![records[idx].clone()],
                })
                .await?;
        }
    }

    // Query all clusters
    let query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "bob@corp.com".to_string(),
        }],
        start: 0,
        end: 100,
    };

    let mut golden_sets = Vec::new();
    for client in &mut clients {
        let response = client.query_entities(query.clone()).await?.into_inner();
        assert_eq!(count_matches(&response), 1, "each cluster has one match");
        golden_sets.push(extract_golden_set(&response));
    }

    // All golden sets should be identical
    assert_eq!(
        golden_sets[0], golden_sets[1],
        "order 0-1-2 should match order 2-1-0"
    );
    assert_eq!(
        golden_sets[1], golden_sets[2],
        "order 2-1-0 should match order 1-0-2"
    );

    Ok(())
}

/// Test that descriptors with different temporal ranges are tracked correctly.
/// Entity has email in month 1, phone in month 2, address in month 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporal_descriptor_evolution() -> anyhow::Result<()> {
    let (mut client,) = setup_cluster().await?;

    // Entity evolves over time with different descriptors from different source records.
    // Each record is from a different source (different uid) but shares the identity key (email).
    let jan = record_input(
        0,
        "person",
        "crm",
        "cust_001", // First source
        vec![("email", "customer@example.com", 202401, 202404)],
    );
    let feb = record_input(
        1,
        "person",
        "crm",
        "cust_002", // Different source
        vec![
            ("email", "customer@example.com", 202401, 202404),
            ("phone", "555-0001", 202402, 202403),
        ],
    );
    let mar = record_input(
        2,
        "person",
        "crm",
        "cust_003", // Different source
        vec![
            ("email", "customer@example.com", 202401, 202404),
            ("address", "123 Main St", 202403, 202404),
        ],
    );

    // Ingest over time
    client
        .ingest_records(IngestRecordsRequest { records: vec![jan] })
        .await?;
    client
        .ingest_records(IngestRecordsRequest { records: vec![feb] })
        .await?;
    client
        .ingest_records(IngestRecordsRequest { records: vec![mar] })
        .await?;

    // Query by email - should find the entity
    let email_query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "customer@example.com".to_string(),
        }],
        start: 202401,
        end: 202404,
    };
    let email_response = client.query_entities(email_query).await?.into_inner();
    assert_eq!(count_matches(&email_response), 1, "found entity by email");

    // Query in February by phone - should find same entity
    let feb_query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "phone".to_string(),
            value: "555-0001".to_string(),
        }],
        start: 202402,
        end: 202403,
    };
    let feb_response = client.query_entities(feb_query).await?.into_inner();
    assert_eq!(count_matches(&feb_response), 1, "found entity by phone");

    // Query in March by address - should find same entity
    let mar_query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "address".to_string(),
            value: "123 Main St".to_string(),
        }],
        start: 202403,
        end: 202404,
    };
    let mar_response = client.query_entities(mar_query).await?.into_inner();
    assert_eq!(count_matches(&mar_response), 1, "found entity by address");

    // All queries should return the same cluster
    let email_cluster = extract_cluster_id(&email_response);
    let feb_cluster = extract_cluster_id(&feb_response);
    let mar_cluster = extract_cluster_id(&mar_response);

    assert_eq!(
        email_cluster, feb_cluster,
        "same entity cluster when queried by email vs phone"
    );
    assert_eq!(
        feb_cluster, mar_cluster,
        "same entity cluster when queried by phone vs address"
    );

    // Query spanning full range should show all descriptors in golden record
    let full_query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "customer@example.com".to_string(),
        }],
        start: 202401,
        end: 202404,
    };
    let full_response = client.query_entities(full_query).await?.into_inner();
    let golden = extract_golden_set(&full_response);

    // All three descriptors should be in the golden record
    assert!(
        golden.iter().any(|(attr, _, _, _)| attr == "email"),
        "email in golden"
    );
    assert!(
        golden.iter().any(|(attr, _, _, _)| attr == "phone"),
        "phone in golden"
    );
    assert!(
        golden.iter().any(|(attr, _, _, _)| attr == "address"),
        "address in golden"
    );

    Ok(())
}

/// Test that an entity can accumulate new attributes over time from different sources.
/// Entity gets additional contact info from a second source record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attribute_value_changes_over_time() -> anyhow::Result<()> {
    let (mut client,) = setup_cluster().await?;

    // Two source records that share an identity key (email) and merge.
    // The first source has the email, the second adds an alt_email.
    let initial = record_input(
        0,
        "person",
        "crm",
        "user_001", // First source
        vec![("email", "primary@example.com", 202301, 202501)],
    );
    let with_secondary = record_input(
        1,
        "person",
        "crm",
        "user_002", // Different source
        vec![
            ("email", "primary@example.com", 202301, 202501),
            ("alt_email", "secondary@example.com", 202401, 202501),
        ],
    );

    client
        .ingest_records(IngestRecordsRequest {
            records: vec![initial, with_secondary],
        })
        .await?;

    // Query with primary email - should find across all time
    let query_primary = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "primary@example.com".to_string(),
        }],
        start: 202301,
        end: 202501,
    };
    let response_primary = client.query_entities(query_primary).await?.into_inner();
    assert_eq!(
        count_matches(&response_primary),
        1,
        "found by primary email"
    );

    // Query with alt email in 2024 - should find same entity
    let query_alt = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "alt_email".to_string(),
            value: "secondary@example.com".to_string(),
        }],
        start: 202401,
        end: 202501,
    };
    let response_alt = client.query_entities(query_alt).await?.into_inner();
    assert_eq!(
        count_matches(&response_alt),
        1,
        "found by alt email in 2024"
    );

    // Both should return the same cluster
    assert_eq!(
        extract_cluster_id(&response_primary),
        extract_cluster_id(&response_alt),
        "same entity for both emails"
    );

    // Alt email should NOT match in 2023 (before it existed)
    let query_alt_2023 = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "alt_email".to_string(),
            value: "secondary@example.com".to_string(),
        }],
        start: 202301,
        end: 202401,
    };
    let response_alt_2023 = client.query_entities(query_alt_2023).await?.into_inner();
    assert_eq!(
        count_matches(&response_alt_2023),
        0,
        "alt email should not match in 2023"
    );

    Ok(())
}

/// Test that multiple records from different perspectives merge correctly
/// and can be queried incrementally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_perspective_incremental_merge() -> anyhow::Result<()> {
    let (mut client,) = setup_cluster().await?;

    // CRM perspective first
    let crm_record = record_input(
        0,
        "person",
        "crm",
        "crm_001",
        vec![("email", "shared@example.com", 0, 100)],
    );
    client
        .ingest_records(IngestRecordsRequest {
            records: vec![crm_record],
        })
        .await?;

    // Query - should find one cluster
    let query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "shared@example.com".to_string(),
        }],
        start: 0,
        end: 100,
    };
    let response1 = client.query_entities(query.clone()).await?.into_inner();
    assert_eq!(count_matches(&response1), 1);
    let cluster1 = extract_cluster_id(&response1);

    // ERP perspective with same email - should merge
    let erp_record = record_input(
        1,
        "person",
        "erp",
        "emp_001",
        vec![
            ("email", "shared@example.com", 0, 100),
            ("department", "Sales", 0, 100),
        ],
    );
    client
        .ingest_records(IngestRecordsRequest {
            records: vec![erp_record],
        })
        .await?;

    // Query again - still one cluster, but now with merged data
    let response2 = client.query_entities(query.clone()).await?.into_inner();
    assert_eq!(
        count_matches(&response2),
        1,
        "still one cluster after merge"
    );
    let cluster2 = extract_cluster_id(&response2);
    assert_eq!(cluster1, cluster2, "same cluster after merge");

    // Golden record should now include department from ERP
    let golden = extract_golden_set(&response2);
    assert!(
        golden
            .iter()
            .any(|(attr, value, _, _)| attr == "department" && value == "Sales"),
        "department from ERP in golden record"
    );

    // Third perspective with more data
    let hr_record = record_input(
        2,
        "person",
        "hr",
        "hr_001",
        vec![
            ("email", "shared@example.com", 0, 100),
            ("salary_band", "L5", 0, 100),
        ],
    );
    client
        .ingest_records(IngestRecordsRequest {
            records: vec![hr_record],
        })
        .await?;

    let response3 = client.query_entities(query).await?.into_inner();
    assert_eq!(
        count_matches(&response3),
        1,
        "still one cluster after 3 perspectives"
    );

    let golden = extract_golden_set(&response3);
    assert!(
        golden
            .iter()
            .any(|(attr, value, _, _)| attr == "salary_band" && value == "L5"),
        "salary_band from HR in golden record"
    );

    // Verify stats show expected counts
    let stats = client.get_stats(StatsRequest {}).await?.into_inner();
    assert_eq!(stats.record_count, 3, "three records total");
    assert_eq!(stats.cluster_count, 1, "all merged into one cluster");

    Ok(())
}

/// Test idempotency: re-ingesting the same record should not change state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_re_ingestion() -> anyhow::Result<()> {
    let (mut client,) = setup_cluster().await?;

    let record = record_input(
        0,
        "person",
        "crm",
        "idem_001",
        vec![
            ("email", "idem@example.com", 0, 100),
            ("name", "Test User", 0, 100),
        ],
    );

    // First ingestion
    let resp1 = client
        .ingest_records(IngestRecordsRequest {
            records: vec![record.clone()],
        })
        .await?
        .into_inner();
    let assign1 = &resp1.assignments[0];

    // Second ingestion (identical)
    let resp2 = client
        .ingest_records(IngestRecordsRequest {
            records: vec![record.clone()],
        })
        .await?
        .into_inner();
    let assign2 = &resp2.assignments[0];

    // Should return same cluster
    assert_eq!(
        assign1.cluster_id, assign2.cluster_id,
        "same cluster on re-ingestion"
    );

    // Third ingestion
    let resp3 = client
        .ingest_records(IngestRecordsRequest {
            records: vec![record],
        })
        .await?
        .into_inner();
    let assign3 = &resp3.assignments[0];
    assert_eq!(
        assign2.cluster_id, assign3.cluster_id,
        "same cluster on third ingestion"
    );

    // Query should return one match
    let query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "idem@example.com".to_string(),
        }],
        start: 0,
        end: 100,
    };
    let response = client.query_entities(query).await?.into_inner();
    assert_eq!(count_matches(&response), 1);

    Ok(())
}

/// Test that partial descriptor overlap correctly extends entity validity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_temporal_ranges_extend_validity() -> anyhow::Result<()> {
    let (mut client,) = setup_cluster().await?;

    // First record: email valid Jan-Mar
    let jan_mar = record_input(
        0,
        "person",
        "crm",
        "overlap_001",
        vec![("email", "overlap@example.com", 202401, 202404)],
    );

    // Second record: same entity, email valid Feb-Apr (overlapping)
    let feb_apr = record_input(
        1,
        "person",
        "crm",
        "overlap_001",
        vec![("email", "overlap@example.com", 202402, 202405)],
    );

    client
        .ingest_records(IngestRecordsRequest {
            records: vec![jan_mar],
        })
        .await?;
    client
        .ingest_records(IngestRecordsRequest {
            records: vec![feb_apr],
        })
        .await?;

    // Query spanning Jan-Apr should find the entity
    let query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "overlap@example.com".to_string(),
        }],
        start: 202401,
        end: 202405,
    };
    let response = client.query_entities(query).await?.into_inner();
    assert_eq!(
        count_matches(&response),
        1,
        "entity found across full range"
    );

    Ok(())
}

/// Test that descriptors can have temporal gaps while the entity remains merged.
/// Entity email spans full range, but phone has a gap (only in Q1 and Q3).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporal_gap_between_records() -> anyhow::Result<()> {
    let (mut client,) = setup_cluster().await?;

    // Two source records with the same email (spanning full range for identity key overlap).
    // Phone attribute has a gap - only present in Q1 and Q3.
    let q1 = record_input(
        0,
        "person",
        "crm",
        "gap_001", // First source
        vec![
            ("email", "gap@example.com", 202401, 202410), // Email spans full range
            ("phone", "555-GAP1", 202401, 202404),        // Phone only in Q1
        ],
    );

    // Different source record with same email, adds phone in Q3
    let q3 = record_input(
        1,
        "person",
        "crm",
        "gap_002", // Different source
        vec![
            ("email", "gap@example.com", 202401, 202410), // Same email, full range
            ("phone", "555-GAP3", 202407, 202410),        // Phone only in Q3
        ],
    );

    client
        .ingest_records(IngestRecordsRequest {
            records: vec![q1, q3],
        })
        .await?;

    // Query by email spanning full range
    let email_query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "gap@example.com".to_string(),
        }],
        start: 202401,
        end: 202410,
    };
    let email_response = client.query_entities(email_query).await?.into_inner();
    assert_eq!(count_matches(&email_response), 1, "found by email");

    // Query by phone in Q1 - should find
    let q1_query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "phone".to_string(),
            value: "555-GAP1".to_string(),
        }],
        start: 202401,
        end: 202404,
    };
    let q1_response = client.query_entities(q1_query).await?.into_inner();
    assert_eq!(count_matches(&q1_response), 1, "found by Q1 phone");

    // Query by phone in Q2 (gap) - should NOT find either phone
    let q2_query_gap1 = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "phone".to_string(),
            value: "555-GAP1".to_string(),
        }],
        start: 202404,
        end: 202407,
    };
    let q2_response = client.query_entities(q2_query_gap1).await?.into_inner();
    assert_eq!(
        count_matches(&q2_response),
        0,
        "Q1 phone not found in Q2 gap"
    );

    // Query by phone in Q3 - should find
    let q3_query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "phone".to_string(),
            value: "555-GAP3".to_string(),
        }],
        start: 202407,
        end: 202410,
    };
    let q3_response = client.query_entities(q3_query).await?.into_inner();
    assert_eq!(count_matches(&q3_response), 1, "found by Q3 phone");

    // All queries should return the same cluster
    assert_eq!(
        extract_cluster_id(&email_response),
        extract_cluster_id(&q1_response),
        "same cluster for email and Q1 phone"
    );
    assert_eq!(
        extract_cluster_id(&q1_response),
        extract_cluster_id(&q3_response),
        "same cluster for Q1 and Q3 phones"
    );

    // Golden record should contain both phone values
    let golden = extract_golden_set(&email_response);
    assert!(
        golden
            .iter()
            .any(|(attr, value, _, _)| attr == "phone" && value == "555-GAP1"),
        "Q1 phone in golden"
    );
    assert!(
        golden
            .iter()
            .any(|(attr, value, _, _)| attr == "phone" && value == "555-GAP3"),
        "Q3 phone in golden"
    );

    Ok(())
}

// =============================================================================
// CLUSTER ASSIGNMENT CONSISTENCY TESTS
// =============================================================================
// These tests verify that records arrive at the same cluster regardless of
// insertion order - a key property for distributed entity resolution.

/// Test that all records sharing an identity key end up in the same cluster,
/// regardless of which record is ingested first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_cluster_regardless_of_insertion_order() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    // Create 3 clusters with different insertion orders
    let orders = vec![
        vec![0, 1, 2], // A, B, C
        vec![2, 1, 0], // C, B, A
        vec![1, 2, 0], // B, C, A
    ];

    let records = [record_input(
            0,
            "person",
            "crm",
            "rec_A",
            vec![("email", "shared@example.com", 0, 100)],
        ),
        record_input(
            1,
            "person",
            "erp",
            "rec_B",
            vec![("email", "shared@example.com", 0, 100)],
        ),
        record_input(
            2,
            "person",
            "hr",
            "rec_C",
            vec![("email", "shared@example.com", 0, 100)],
        )];

    let mut all_record_clusters: Vec<Vec<u32>> = Vec::new();

    for order in &orders {
        // Fresh cluster for each order
        let (shard0_addr, _) = spawn_shard(0, empty_config.clone()).await?;
        let (shard1_addr, _) = spawn_shard(1, empty_config.clone()).await?;
        let (router_addr, _) =
            spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;
        let mut client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
        client
            .set_ontology(ApplyOntologyRequest {
                config: Some(support::to_proto_config(&config)),
            })
            .await?;

        // Ingest in specified order
        let mut cluster_ids = vec![0u32; 3];
        for &idx in order {
            let resp = client
                .ingest_records(IngestRecordsRequest {
                    records: vec![records[idx].clone()],
                })
                .await?
                .into_inner();
            cluster_ids[idx] = resp.assignments[0].cluster_id;
        }

        all_record_clusters.push(cluster_ids);
    }

    // All records should be in the SAME cluster within each run
    for (i, clusters) in all_record_clusters.iter().enumerate() {
        assert_eq!(
            clusters[0], clusters[1],
            "order {:?}: records A and B should be same cluster",
            orders[i]
        );
        assert_eq!(
            clusters[1], clusters[2],
            "order {:?}: records B and C should be same cluster",
            orders[i]
        );
    }

    Ok(())
}

/// Test that batch ingestion maintains consistent cluster assignment.
/// All records in a batch with same identity key go to same cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_ingestion_consistent_cluster() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    // Same 3 records, ingested in a single batch vs one at a time
    let records = vec![
        record_input(
            0,
            "person",
            "crm",
            "batch_A",
            vec![("email", "batch@example.com", 0, 100)],
        ),
        record_input(
            1,
            "person",
            "erp",
            "batch_B",
            vec![("email", "batch@example.com", 0, 100)],
        ),
        record_input(
            2,
            "person",
            "hr",
            "batch_C",
            vec![("email", "batch@example.com", 0, 100)],
        ),
    ];

    // Cluster 1: Batch ingestion
    let (shard0_addr, _) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;
    let mut batch_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    batch_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    let batch_resp = batch_client
        .ingest_records(IngestRecordsRequest {
            records: records.clone(),
        })
        .await?
        .into_inner();

    // All should be in same cluster
    let batch_cluster = batch_resp.assignments[0].cluster_id;
    assert_eq!(batch_resp.assignments[1].cluster_id, batch_cluster);
    assert_eq!(batch_resp.assignments[2].cluster_id, batch_cluster);

    // Cluster 2: Sequential ingestion
    let (shard0_addr, _) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;
    let mut seq_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    seq_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    let mut seq_clusters = Vec::new();
    for record in &records {
        let resp = seq_client
            .ingest_records(IngestRecordsRequest {
                records: vec![record.clone()],
            })
            .await?
            .into_inner();
        seq_clusters.push(resp.assignments[0].cluster_id);
    }

    // All should be in same cluster
    assert_eq!(seq_clusters[0], seq_clusters[1], "A and B same cluster");
    assert_eq!(seq_clusters[1], seq_clusters[2], "B and C same cluster");

    // Both approaches should result in 1 cluster total
    let batch_stats = batch_client.get_stats(StatsRequest {}).await?.into_inner();
    let seq_stats = seq_client.get_stats(StatsRequest {}).await?.into_inner();

    assert_eq!(batch_stats.cluster_count, 1, "batch: one cluster");
    assert_eq!(seq_stats.cluster_count, 1, "sequential: one cluster");

    Ok(())
}

/// Test that multiple independent entities maintain separate clusters
/// regardless of interleaved insertion order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_entities_stay_separate_any_order() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    // Two independent entities: Alice and Bob (different emails)
    let alice1 = record_input(
        0,
        "person",
        "crm",
        "alice_1",
        vec![("email", "alice@example.com", 0, 100)],
    );
    let alice2 = record_input(
        1,
        "person",
        "erp",
        "alice_2",
        vec![("email", "alice@example.com", 0, 100)],
    );
    let bob1 = record_input(
        2,
        "person",
        "crm",
        "bob_1",
        vec![("email", "bob@example.com", 0, 100)],
    );
    let bob2 = record_input(
        3,
        "person",
        "erp",
        "bob_2",
        vec![("email", "bob@example.com", 0, 100)],
    );

    // Interleave insertions in different orders
    let orders = vec![
        vec![0, 2, 1, 3], // alice1, bob1, alice2, bob2
        vec![2, 0, 3, 1], // bob1, alice1, bob2, alice2
        vec![3, 2, 1, 0], // bob2, bob1, alice2, alice1
        vec![1, 3, 0, 2], // alice2, bob2, alice1, bob1
    ];

    for order in &orders {
        let (shard0_addr, _) = spawn_shard(0, empty_config.clone()).await?;
        let (shard1_addr, _) = spawn_shard(1, empty_config.clone()).await?;
        let (router_addr, _) =
            spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;
        let mut client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
        client
            .set_ontology(ApplyOntologyRequest {
                config: Some(support::to_proto_config(&config)),
            })
            .await?;

        let records = [alice1.clone(), alice2.clone(), bob1.clone(), bob2.clone()];

        for &idx in order {
            client
                .ingest_records(IngestRecordsRequest {
                    records: vec![records[idx].clone()],
                })
                .await?;
        }

        // Query for Alice and Bob
        let alice_query = QueryEntitiesRequest {
            descriptors: vec![ProtoQueryDescriptor {
                attr: "email".to_string(),
                value: "alice@example.com".to_string(),
            }],
            start: 0,
            end: 100,
        };
        let bob_query = QueryEntitiesRequest {
            descriptors: vec![ProtoQueryDescriptor {
                attr: "email".to_string(),
                value: "bob@example.com".to_string(),
            }],
            start: 0,
            end: 100,
        };

        let alice_resp = client.query_entities(alice_query).await?.into_inner();
        let bob_resp = client.query_entities(bob_query).await?.into_inner();

        let alice_cluster = extract_cluster_id(&alice_resp);
        let bob_cluster = extract_cluster_id(&bob_resp);

        // Alice records should be in one cluster
        assert_eq!(
            count_matches(&alice_resp),
            1,
            "order {:?}: Alice is one cluster",
            order
        );
        // Bob records should be in one cluster
        assert_eq!(
            count_matches(&bob_resp),
            1,
            "order {:?}: Bob is one cluster",
            order
        );
        // But they should be DIFFERENT clusters
        assert_ne!(
            alice_cluster, bob_cluster,
            "order {:?}: Alice and Bob should be in different clusters",
            order
        );

        // Verify stats show 2 clusters
        let stats = client.get_stats(StatsRequest {}).await?.into_inner();
        assert_eq!(
            stats.cluster_count, 2,
            "order {:?}: should have exactly 2 clusters",
            order
        );
    }

    Ok(())
}

/// Test late-arriving record merges into existing cluster correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_arrival_merges_into_existing_cluster() -> anyhow::Result<()> {
    let (mut client,) = setup_cluster().await?;

    // First, ingest two records that form a cluster
    let rec1 = record_input(
        0,
        "person",
        "crm",
        "late_1",
        vec![("email", "late@example.com", 0, 100)],
    );
    let rec2 = record_input(
        1,
        "person",
        "erp",
        "late_2",
        vec![("email", "late@example.com", 0, 100)],
    );

    let resp1 = client
        .ingest_records(IngestRecordsRequest {
            records: vec![rec1, rec2],
        })
        .await?
        .into_inner();

    // Both should be in same cluster
    assert_eq!(
        resp1.assignments[0].cluster_id, resp1.assignments[1].cluster_id,
        "initial records in same cluster"
    );
    let original_cluster = resp1.assignments[0].cluster_id;

    // Now a late-arriving record with same email
    let late_rec = record_input(
        2,
        "person",
        "hr",
        "late_3",
        vec![
            ("email", "late@example.com", 0, 100),
            ("department", "Sales", 0, 100),
        ],
    );

    let resp2 = client
        .ingest_records(IngestRecordsRequest {
            records: vec![late_rec],
        })
        .await?
        .into_inner();

    // Late record should join the existing cluster
    assert_eq!(
        resp2.assignments[0].cluster_id, original_cluster,
        "late-arriving record joins existing cluster"
    );

    // Query should show all three records merged with the new department
    let query = QueryEntitiesRequest {
        descriptors: vec![ProtoQueryDescriptor {
            attr: "email".to_string(),
            value: "late@example.com".to_string(),
        }],
        start: 0,
        end: 100,
    };
    let resp = client.query_entities(query).await?.into_inner();
    assert_eq!(count_matches(&resp), 1, "still one cluster");

    let golden = extract_golden_set(&resp);
    assert!(
        golden
            .iter()
            .any(|(attr, value, _, _)| attr == "department" && value == "Sales"),
        "late-arriving department in golden record"
    );

    Ok(())
}
