//! End-to-end tests for cross-shard reconciliation and conflict detection.
//!
//! These tests verify that the full gRPC pipeline correctly:
//! 1. Serializes perspective_strong_ids in boundary metadata
//! 2. Detects cross-shard conflicts during reconciliation
//! 3. Propagates conflicts to affected shards
//! 4. Makes conflicts queryable via list_conflicts
//!
//! This catches bugs that unit tests miss because they bypass the proto serialization layer.

use std::net::SocketAddr;

use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, shard_service_client::ShardServiceClient,
    ApplyOntologyRequest, ConstraintConfig as ProtoConstraintConfig,
    ConstraintKind as ProtoConstraintKind, IdentityKeyConfig as ProtoIdentityKeyConfig,
    IngestRecordsRequest, OntologyConfig, RecordDescriptor, RecordIdentity, RecordInput,
};
use unirust_rs::distributed::{
    ConstraintConfig, ConstraintKind, DistributedOntologyConfig, IdentityKeyConfig, RouterNode,
    ShardNode,
};
use unirust_rs::{StreamingTuning, TuningProfile};

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
        identity: Some(RecordIdentity {
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

fn to_proto_config(config: &DistributedOntologyConfig) -> OntologyConfig {
    OntologyConfig {
        identity_keys: config
            .identity_keys
            .iter()
            .map(|entry| ProtoIdentityKeyConfig {
                name: entry.name.clone(),
                attributes: entry.attributes.clone(),
            })
            .collect(),
        strong_identifiers: config.strong_identifiers.clone(),
        constraints: config
            .constraints
            .iter()
            .map(|entry| ProtoConstraintConfig {
                name: entry.name.clone(),
                attribute: entry.attribute.clone(),
                kind: match entry.kind {
                    ConstraintKind::Unique => ProtoConstraintKind::Unique.into(),
                    ConstraintKind::UniqueWithinPerspective => {
                        ProtoConstraintKind::UniqueWithinPerspective.into()
                    }
                },
            })
            .collect(),
    }
}

/// Build an ontology config that uses (isin, country) as identity key
/// and ts_code as a strong identifier (unique within perspective).
fn build_instrument_config() -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: vec![IdentityKeyConfig {
            name: "isin_country".to_string(),
            attributes: vec!["isin".to_string(), "country".to_string()],
        }],
        strong_identifiers: vec!["ts_code".to_string()],
        constraints: vec![ConstraintConfig {
            name: "unique_ts_code".to_string(),
            attribute: "ts_code".to_string(),
            kind: ConstraintKind::UniqueWithinPerspective,
        }],
    }
}

/// Test that cross-shard conflicts are detected when records with the same
/// identity key have conflicting strong identifiers in the same perspective.
///
/// This is the "indirect conflict" scenario from dominus:
/// - Record 1 (msci): ISIN=I, country=C, ts_code=TS1
/// - Record 2 (msci): ISIN=I, country=C, ts_code=TS2 (CONFLICT!)
///
/// Both records share the identity key (ISIN, country) but have different
/// ts_code values in the same "msci" perspective.
///
/// We ingest directly to shards (bypassing router routing) to force records
/// onto different shards. Each shard needs 2+ records with the same identity
/// key to trigger a merge, which activates boundary tracking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_shard_conflict_detected_via_reconcile() -> anyhow::Result<()> {
    let config = build_instrument_config();
    let empty_config = DistributedOntologyConfig::empty();

    // Spawn 2 shards and router
    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1_client = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;

    // Apply ontology to all
    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    // Wait for ontology to propagate
    sleep(Duration::from_millis(100)).await;

    // Shard 0: Two records with same identity key, ts_code = "TS1"
    // Having 2 records triggers a merge, which activates boundary tracking.
    let shard0_rec1 = record_input(
        0,
        "instrument",
        "msci",
        "msci_001",
        vec![
            ("isin", "ISIN001", 0, 100),
            ("country", "US", 0, 100),
            ("ts_code", "TS1", 0, 100), // Strong ID value 1
        ],
    );
    let shard0_rec2 = record_input(
        1,
        "instrument",
        "msci",
        "msci_001b", // Different UID to create second record
        vec![
            ("isin", "ISIN001", 0, 100), // Same identity key
            ("country", "US", 0, 100),   // Same identity key
            ("ts_code", "TS1", 0, 100),  // Same strong ID (no local conflict)
        ],
    );

    shard0_client
        .ingest_records(IngestRecordsRequest {
            records: vec![shard0_rec1, shard0_rec2],
        })
        .await?;

    // Shard 1: Two records with same identity key, ts_code = "TS2" (DIFFERENT!)
    let shard1_rec1 = record_input(
        2,
        "instrument",
        "msci",
        "msci_002",
        vec![
            ("isin", "ISIN001", 0, 100), // Same identity key as shard0
            ("country", "US", 0, 100),   // Same identity key
            ("ts_code", "TS2", 0, 100),  // DIFFERENT strong ID - should conflict!
        ],
    );
    let shard1_rec2 = record_input(
        3,
        "instrument",
        "msci",
        "msci_002b",
        vec![
            ("isin", "ISIN001", 0, 100),
            ("country", "US", 0, 100),
            ("ts_code", "TS2", 0, 100),
        ],
    );

    shard1_client
        .ingest_records(IngestRecordsRequest {
            records: vec![shard1_rec1, shard1_rec2],
        })
        .await?;

    // Trigger explicit reconciliation
    let reconcile_response = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    // Wait a moment for conflict propagation
    sleep(Duration::from_millis(100)).await;

    // Query conflicts from router
    let conflicts_response = router_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 100,
            attribute: String::new(),
        })
        .await?
        .into_inner();

    // We should have at least one conflict (ts_code mismatch across shards)
    assert!(
        !conflicts_response.conflicts.is_empty() || reconcile_response.conflicts_blocked > 0,
        "Expected conflicts to be detected. Records have same identity key but different ts_code across shards. \
         Reconcile stats: merges={}, conflicts_blocked={}",
        reconcile_response.merges_performed,
        reconcile_response.conflicts_blocked
    );

    // If conflicts_blocked > 0, that's also success - conflict was detected
    // during merge attempt and blocked
    if !conflicts_response.conflicts.is_empty() {
        // Verify the conflict is about ts_code or is a cross-shard conflict
        let has_ts_code_conflict = conflicts_response.conflicts.iter().any(|c| {
            c.attribute == "ts_code"
                || c.kind.contains("cross_shard")
                || c.kind.contains("indirect")
        });

        assert!(
            has_ts_code_conflict,
            "Expected a ts_code conflict or cross-shard conflict, got: {:?}",
            conflicts_response.conflicts
        );
    }

    Ok(())
}

/// Test that cross-shard merges proceed when there's no conflict.
///
/// Records with the same identity key but from different perspectives
/// (or with matching strong IDs) should merge successfully.
/// We ingest directly to shards to force cross-shard scenario.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_shard_merge_succeeds_without_conflict() -> anyhow::Result<()> {
    let config = build_instrument_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1_client = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Shard 0: Two records with same identity key, from "msci" perspective
    let shard0_rec1 = record_input(
        0,
        "instrument",
        "msci",
        "msci_001",
        vec![
            ("isin", "ISIN002", 0, 100),
            ("country", "UK", 0, 100),
            ("ts_code", "TS_MSCI", 0, 100),
        ],
    );
    let shard0_rec2 = record_input(
        1,
        "instrument",
        "msci",
        "msci_001b",
        vec![
            ("isin", "ISIN002", 0, 100),
            ("country", "UK", 0, 100),
            ("ts_code", "TS_MSCI", 0, 100),
        ],
    );

    shard0_client
        .ingest_records(IngestRecordsRequest {
            records: vec![shard0_rec1, shard0_rec2],
        })
        .await?;

    // Shard 1: Two records with same identity key, from "axioma" perspective
    // Different perspective means no strong ID conflict
    let shard1_rec1 = record_input(
        2,
        "instrument",
        "axioma", // Different perspective - no conflict expected
        "axioma_001",
        vec![
            ("isin", "ISIN002", 0, 100), // Same identity key
            ("country", "UK", 0, 100),   // Same identity key
            ("axioma_id", "AX001", 0, 100),
        ],
    );
    let shard1_rec2 = record_input(
        3,
        "instrument",
        "axioma",
        "axioma_001b",
        vec![
            ("isin", "ISIN002", 0, 100),
            ("country", "UK", 0, 100),
            ("axioma_id", "AX001", 0, 100),
        ],
    );

    shard1_client
        .ingest_records(IngestRecordsRequest {
            records: vec![shard1_rec1, shard1_rec2],
        })
        .await?;

    // Trigger reconciliation
    let reconcile_response = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    // Should have no conflicts blocked (different perspectives don't conflict)
    // Note: conflicts_blocked only counts cross-shard conflicts

    sleep(Duration::from_millis(100)).await;

    // Query conflicts - should have no ts_code conflicts
    let conflicts_response = router_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 100,
            attribute: "ts_code".to_string(),
        })
        .await?
        .into_inner();

    assert!(
        conflicts_response.conflicts.is_empty(),
        "Expected no ts_code conflicts for different perspectives, got: {:?}. \
         Reconcile stats: merges={}, conflicts_blocked={}",
        conflicts_response.conflicts,
        reconcile_response.merges_performed,
        reconcile_response.conflicts_blocked
    );

    Ok(())
}

/// Test that conflicts propagated from router are queryable on individual shards.
/// We ingest directly to shards to force cross-shard scenario.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_shard_conflicts_propagated_to_shards() -> anyhow::Result<()> {
    let config = build_instrument_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1_client = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Shard 0: Two records with same identity key, ts_code = "TS_A"
    let shard0_rec1 = record_input(
        0,
        "instrument",
        "msci",
        "msci_003",
        vec![
            ("isin", "ISIN003", 0, 100),
            ("country", "JP", 0, 100),
            ("ts_code", "TS_A", 0, 100),
        ],
    );
    let shard0_rec2 = record_input(
        1,
        "instrument",
        "msci",
        "msci_003b",
        vec![
            ("isin", "ISIN003", 0, 100),
            ("country", "JP", 0, 100),
            ("ts_code", "TS_A", 0, 100),
        ],
    );

    shard0_client
        .ingest_records(IngestRecordsRequest {
            records: vec![shard0_rec1, shard0_rec2],
        })
        .await?;

    // Shard 1: Two records with same identity key, ts_code = "TS_B" (conflict!)
    let shard1_rec1 = record_input(
        2,
        "instrument",
        "msci",
        "msci_004",
        vec![
            ("isin", "ISIN003", 0, 100),
            ("country", "JP", 0, 100),
            ("ts_code", "TS_B", 0, 100), // Different ts_code - conflict!
        ],
    );
    let shard1_rec2 = record_input(
        3,
        "instrument",
        "msci",
        "msci_004b",
        vec![
            ("isin", "ISIN003", 0, 100),
            ("country", "JP", 0, 100),
            ("ts_code", "TS_B", 0, 100),
        ],
    );

    shard1_client
        .ingest_records(IngestRecordsRequest {
            records: vec![shard1_rec1, shard1_rec2],
        })
        .await?;

    // Trigger reconciliation to detect and propagate conflicts
    let _reconcile_response = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?;

    // Wait for conflict propagation
    sleep(Duration::from_millis(200)).await;

    // Query conflicts from both shards
    let shard0_conflicts = shard0_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 100,
            attribute: String::new(),
        })
        .await?
        .into_inner();

    let shard1_conflicts = shard1_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 100,
            attribute: String::new(),
        })
        .await?
        .into_inner();

    // At least one shard should have the conflict
    // (the shard that owns the records or received the propagated conflict)
    let total_conflicts = shard0_conflicts.conflicts.len() + shard1_conflicts.conflicts.len();

    assert!(
        total_conflicts > 0,
        "Expected conflicts to be stored on at least one shard. \
         Shard0 conflicts: {:?}, Shard1 conflicts: {:?}",
        shard0_conflicts.conflicts,
        shard1_conflicts.conflicts
    );

    Ok(())
}

/// Test that boundary metadata includes perspective_strong_ids.
/// This directly tests the serialization fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boundary_metadata_includes_perspective_strong_ids() -> anyhow::Result<()> {
    let config = build_instrument_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Ingest TWO records with the same identity key (isin+country) to trigger a merge.
    // Boundary tracking only happens on merge, so we need multiple records.
    // Both from same perspective with same strong ID value to avoid conflict.
    let record1 = record_input(
        0,
        "instrument",
        "msci",
        "msci_005",
        vec![
            ("isin", "ISIN005", 0, 100),
            ("country", "DE", 0, 100),
            ("ts_code", "TS_DE_001", 0, 100), // Strong identifier
        ],
    );

    let record2 = record_input(
        1,
        "instrument",
        "msci",
        "msci_006", // Different UID, same perspective
        vec![
            ("isin", "ISIN005", 0, 100),      // Same identity key
            ("country", "DE", 0, 100),        // Same identity key
            ("ts_code", "TS_DE_001", 0, 100), // Same strong ID value
        ],
    );

    router_client
        .ingest_records(IngestRecordsRequest {
            records: vec![record1, record2],
        })
        .await?;

    // Get boundary metadata from shard
    let metadata_response = shard_client
        .get_boundary_metadata(proto::GetBoundaryMetadataRequest { since_version: 0 })
        .await?
        .into_inner();

    let metadata = metadata_response.metadata.expect("metadata should exist");

    // Find entries and check for perspective_strong_ids
    let has_strong_ids = metadata.entries.iter().any(|key_entries| {
        key_entries
            .entries
            .iter()
            .any(|entry| !entry.perspective_strong_ids.is_empty())
    });

    assert!(
        has_strong_ids,
        "Expected boundary entries to have perspective_strong_ids populated. \
         This indicates the proto serialization is working correctly. \
         Metadata entries: {:?}",
        metadata.entries
    );

    Ok(())
}

/// Test dirty boundary keys also include perspective_strong_ids.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dirty_boundary_keys_include_perspective_strong_ids() -> anyhow::Result<()> {
    let config = build_instrument_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Ingest TWO records with the same identity key to trigger a merge.
    // Boundary tracking only happens on merge.
    let record1 = record_input(
        0,
        "instrument",
        "bloomberg",
        "bb_001",
        vec![
            ("isin", "ISIN006", 0, 100),
            ("country", "FR", 0, 100),
            ("ts_code", "TS_FR_001", 0, 100),
        ],
    );

    let record2 = record_input(
        1,
        "instrument",
        "bloomberg",
        "bb_002", // Different UID, same perspective
        vec![
            ("isin", "ISIN006", 0, 100),      // Same identity key
            ("country", "FR", 0, 100),        // Same identity key
            ("ts_code", "TS_FR_001", 0, 100), // Same strong ID value
        ],
    );

    router_client
        .ingest_records(IngestRecordsRequest {
            records: vec![record1, record2],
        })
        .await?;

    // Get dirty boundary keys from shard
    let dirty_keys_response = shard_client
        .get_dirty_boundary_keys(proto::GetDirtyBoundaryKeysRequest {})
        .await?
        .into_inner();

    // Check if any dirty key entries have perspective_strong_ids
    let has_strong_ids = dirty_keys_response.dirty_keys.iter().any(|dirty_key| {
        dirty_key
            .entries
            .iter()
            .any(|entry| !entry.perspective_strong_ids.is_empty())
    });

    assert!(
        has_strong_ids,
        "Expected dirty boundary key entries to have perspective_strong_ids populated. \
         Dirty keys: {:?}",
        dirty_keys_response.dirty_keys
    );

    Ok(())
}

// =============================================================================
// ADVANCED DISTRIBUTED CONFLICT SCENARIOS
// =============================================================================
// These tests cover conflicts that are easy to detect in-memory but become
// challenging in a sharded/distributed setup. Inspired by dominus data quality
// checks and real-world entity resolution edge cases.

/// Build an ontology with two identity keys for testing transitive relationships.
/// - key1: (isin, country) - primary identity key
/// - key2: (sedol, country) - secondary identity key for transitive linking
fn build_multi_key_config() -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: vec![
            IdentityKeyConfig {
                name: "isin_country".to_string(),
                attributes: vec!["isin".to_string(), "country".to_string()],
            },
            IdentityKeyConfig {
                name: "sedol_country".to_string(),
                attributes: vec!["sedol".to_string(), "country".to_string()],
            },
        ],
        strong_identifiers: vec!["ts_code".to_string()],
        constraints: vec![ConstraintConfig {
            name: "unique_ts_code".to_string(),
            attribute: "ts_code".to_string(),
            kind: ConstraintKind::UniqueWithinPerspective,
        }],
    }
}

/// Test: TRANSITIVE CROSS-SHARD CONFLICT (Indirect Conflict via Shared Key)
///
/// Scenario (dominus: canHandleIndirectConflict):
/// - Entity A on shard0: ISIN=I1, SEDOL=S1, ts_code=TS_A
/// - Entity B on shard1: SEDOL=S1, ISIN=I2, ts_code=TS_B (shares SEDOL with A)
/// - Entity C on shard2: ISIN=I2, ts_code=TS_C (shares ISIN with B)
///
/// In-memory: A=B via SEDOL, B=C via ISIN, therefore A=C. But A.ts_code ≠ C.ts_code = CONFLICT
/// Sharded: Each shard only sees local connections. The transitive path A↔B↔C spans 3 shards.
///
/// This tests whether reconciliation can detect conflicts through transitive relationships.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transitive_cross_shard_conflict_detected() -> anyhow::Result<()> {
    let config = build_multi_key_config();
    let empty_config = DistributedOntologyConfig::empty();

    // Spawn 3 shards for A, B, C
    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;
    let (shard2_addr, _shard2_handle) = spawn_shard(2, empty_config.clone()).await?;
    let (router_addr, _router_handle) = spawn_router(
        vec![shard0_addr, shard1_addr, shard2_addr],
        empty_config.clone(),
    )
    .await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1_client = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;
    let mut shard2_client = ShardServiceClient::connect(format!("http://{}", shard2_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Shard 0: Entity A - ISIN=I1, SEDOL=S1, ts_code=TS_A
    // Need 2 records to trigger merge and boundary tracking
    let a1 = record_input(
        0,
        "instrument",
        "msci",
        "a1",
        vec![
            ("isin", "ISIN_A", 0, 100),
            ("sedol", "SEDOL_SHARED", 0, 100),
            ("country", "US", 0, 100),
            ("ts_code", "TS_A", 0, 100),
        ],
    );
    let a2 = record_input(
        1,
        "instrument",
        "msci",
        "a2",
        vec![
            ("isin", "ISIN_A", 0, 100),
            ("sedol", "SEDOL_SHARED", 0, 100),
            ("country", "US", 0, 100),
            ("ts_code", "TS_A", 0, 100),
        ],
    );
    shard0_client
        .ingest_records(IngestRecordsRequest {
            records: vec![a1, a2],
        })
        .await?;

    // Shard 1: Entity B - SEDOL=S1 (links to A), ISIN=I2, ts_code=TS_B
    let b1 = record_input(
        2,
        "instrument",
        "msci",
        "b1",
        vec![
            ("isin", "ISIN_B", 0, 100),
            ("sedol", "SEDOL_SHARED", 0, 100), // Links to A via sedol_country key
            ("country", "US", 0, 100),
            ("ts_code", "TS_B", 0, 100), // Different from TS_A - potential conflict!
        ],
    );
    let b2 = record_input(
        3,
        "instrument",
        "msci",
        "b2",
        vec![
            ("isin", "ISIN_B", 0, 100),
            ("sedol", "SEDOL_SHARED", 0, 100),
            ("country", "US", 0, 100),
            ("ts_code", "TS_B", 0, 100),
        ],
    );
    shard1_client
        .ingest_records(IngestRecordsRequest {
            records: vec![b1, b2],
        })
        .await?;

    // Shard 2: Entity C - ISIN=I2 (links to B), ts_code=TS_C
    let c1 = record_input(
        4,
        "instrument",
        "msci",
        "c1",
        vec![
            ("isin", "ISIN_B", 0, 100), // Links to B via isin_country key
            ("country", "US", 0, 100),
            ("ts_code", "TS_C", 0, 100), // Different from TS_A and TS_B!
        ],
    );
    let c2 = record_input(
        5,
        "instrument",
        "msci",
        "c2",
        vec![
            ("isin", "ISIN_B", 0, 100),
            ("country", "US", 0, 100),
            ("ts_code", "TS_C", 0, 100),
        ],
    );
    shard2_client
        .ingest_records(IngestRecordsRequest {
            records: vec![c1, c2],
        })
        .await?;

    // First reconciliation - should detect A↔B via sedol_country key
    let reconcile1 = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    // Second reconciliation - needed for transitive detection (B↔C via isin_country)
    sleep(Duration::from_millis(100)).await;
    let reconcile2 = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    sleep(Duration::from_millis(100)).await;

    // Check for conflicts
    let conflicts = router_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 100,
            attribute: String::new(),
        })
        .await?
        .into_inner();

    // We expect conflicts to be detected through the transitive chain
    // Either directly blocked during merge, or reported as conflicts
    let total_conflicts_blocked = reconcile1.conflicts_blocked + reconcile2.conflicts_blocked;

    assert!(
        !conflicts.conflicts.is_empty() || total_conflicts_blocked > 0,
        "Expected transitive conflict to be detected. A→B→C chain should reveal \
         that A.ts_code=TS_A conflicts with C.ts_code=TS_C via shared B. \
         Reconcile1: merges={}, blocked={}; Reconcile2: merges={}, blocked={}; \
         Conflicts: {:?}",
        reconcile1.merges_performed,
        reconcile1.conflicts_blocked,
        reconcile2.merges_performed,
        reconcile2.conflicts_blocked,
        conflicts.conflicts
    );

    Ok(())
}

/// Test: PEIC - Perspective Entity Identity Conflict (Many-Perm-To-One-Identifier)
///
/// Scenario (dominus: canDetectPEIC2):
/// - Entity A on shard0: UID=a, perspective=msci, sedol=SEDOL_UNIQUE
/// - Entity B on shard1: UID=b, perspective=msci, sedol=SEDOL_UNIQUE (SAME!)
///
/// These are TWO DISTINCT entities (different UIDs in same perspective) that
/// both claim the same unique identifier. This violates uniqueness constraints.
///
/// In-memory: Immediately detected as constraint violation.
/// Sharded: Each shard thinks its entity legitimately owns the identifier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peic_many_entities_claim_same_identifier_across_shards() -> anyhow::Result<()> {
    // Use UNIQUE constraint (not UniqueWithinPerspective) for global uniqueness
    let config = DistributedOntologyConfig {
        identity_keys: vec![IdentityKeyConfig {
            name: "sedol_country".to_string(),
            attributes: vec!["sedol".to_string(), "country".to_string()],
        }],
        strong_identifiers: vec!["axioma_id".to_string()],
        constraints: vec![ConstraintConfig {
            name: "unique_sedol".to_string(),
            attribute: "sedol".to_string(),
            kind: ConstraintKind::Unique, // Globally unique, not just per-perspective
        }],
    };
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1_client = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Shard 0: Entity A claims SEDOL=SEDOL_UNIQUE with axioma_id=AX1
    let a1 = record_input(
        0,
        "instrument",
        "axioma",
        "entity_a",
        vec![
            ("sedol", "SEDOL_UNIQUE", 0, 100),
            ("country", "US", 0, 100),
            ("axioma_id", "AX_001", 0, 100),
        ],
    );
    let a2 = record_input(
        1,
        "instrument",
        "axioma",
        "entity_a2",
        vec![
            ("sedol", "SEDOL_UNIQUE", 0, 100),
            ("country", "US", 0, 100),
            ("axioma_id", "AX_001", 0, 100),
        ],
    );
    shard0_client
        .ingest_records(IngestRecordsRequest {
            records: vec![a1, a2],
        })
        .await?;

    // Shard 1: Entity B also claims SEDOL=SEDOL_UNIQUE but with DIFFERENT axioma_id!
    // This is a PEIC - two distinct entities claiming the same unique identifier
    let b1 = record_input(
        2,
        "instrument",
        "axioma",
        "entity_b",
        vec![
            ("sedol", "SEDOL_UNIQUE", 0, 100), // SAME sedol!
            ("country", "US", 0, 100),
            ("axioma_id", "AX_002", 0, 100), // DIFFERENT axioma_id = conflict!
        ],
    );
    let b2 = record_input(
        3,
        "instrument",
        "axioma",
        "entity_b2",
        vec![
            ("sedol", "SEDOL_UNIQUE", 0, 100),
            ("country", "US", 0, 100),
            ("axioma_id", "AX_002", 0, 100),
        ],
    );
    shard1_client
        .ingest_records(IngestRecordsRequest {
            records: vec![b1, b2],
        })
        .await?;

    // Reconcile to detect the conflict
    let reconcile = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    sleep(Duration::from_millis(100)).await;

    let conflicts = router_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 100,
            attribute: String::new(),
        })
        .await?
        .into_inner();

    // PEIC should be detected: two entities with same identity key but different strong IDs
    assert!(
        !conflicts.conflicts.is_empty() || reconcile.conflicts_blocked > 0,
        "Expected PEIC conflict: Entity A (axioma_id=AX_001) and Entity B (axioma_id=AX_002) \
         both claim sedol=SEDOL_UNIQUE. This violates uniqueness. \
         Reconcile: merges={}, blocked={}; Conflicts: {:?}",
        reconcile.merges_performed,
        reconcile.conflicts_blocked,
        conflicts.conflicts
    );

    Ok(())
}

/// Test: TEMPORAL OVERLAP CONFLICT
///
/// Scenario (dominus: canDetectAndFixPEIC1):
/// - Entity A on shard0: ts_code=TS_A for days 1-10
/// - Entity B on shard1: ts_code=TS_B for days 5-15 (OVERLAPS days 5-10!)
///
/// Same identity key, same perspective, overlapping time ranges with different
/// strong identifier values. The overlap period (days 5-10) has conflicting ts_codes.
///
/// In-memory: Interval overlap detection catches this immediately.
/// Sharded: Each shard only knows about its own time ranges.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporal_overlap_conflict_across_shards() -> anyhow::Result<()> {
    let config = build_instrument_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1_client = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Shard 0: Records with ts_code=TS_A valid for days 1-10
    let a1 = record_input(
        0,
        "instrument",
        "msci",
        "rec_a1",
        vec![
            ("isin", "ISIN_OVERLAP", 1, 10), // Days 1-10
            ("country", "US", 1, 10),
            ("ts_code", "TS_EARLY", 1, 10), // ts_code A for early period
        ],
    );
    let a2 = record_input(
        1,
        "instrument",
        "msci",
        "rec_a2",
        vec![
            ("isin", "ISIN_OVERLAP", 1, 10),
            ("country", "US", 1, 10),
            ("ts_code", "TS_EARLY", 1, 10),
        ],
    );
    shard0_client
        .ingest_records(IngestRecordsRequest {
            records: vec![a1, a2],
        })
        .await?;

    // Shard 1: Records with ts_code=TS_B valid for days 5-15 (OVERLAPS 5-10!)
    let b1 = record_input(
        2,
        "instrument",
        "msci",
        "rec_b1",
        vec![
            ("isin", "ISIN_OVERLAP", 5, 15), // Days 5-15 - overlaps with shard0!
            ("country", "US", 5, 15),
            ("ts_code", "TS_LATE", 5, 15), // DIFFERENT ts_code in overlapping period!
        ],
    );
    let b2 = record_input(
        3,
        "instrument",
        "msci",
        "rec_b2",
        vec![
            ("isin", "ISIN_OVERLAP", 5, 15),
            ("country", "US", 5, 15),
            ("ts_code", "TS_LATE", 5, 15),
        ],
    );
    shard1_client
        .ingest_records(IngestRecordsRequest {
            records: vec![b1, b2],
        })
        .await?;

    let reconcile = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    sleep(Duration::from_millis(100)).await;

    let conflicts = router_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 20,
            attribute: String::new(),
        })
        .await?
        .into_inner();

    // The overlap period (days 5-10) should show a conflict
    assert!(
        !conflicts.conflicts.is_empty() || reconcile.conflicts_blocked > 0,
        "Expected temporal overlap conflict. Days 5-10 have both ts_code=TS_EARLY and \
         ts_code=TS_LATE for the same identity key. \
         Reconcile: merges={}, blocked={}; Conflicts: {:?}",
        reconcile.merges_performed,
        reconcile.conflicts_blocked,
        conflicts.conflicts
    );

    Ok(())
}

/// Test: LATE-ARRIVING DATA CONFLICT
///
/// Scenario:
/// - Initial state: Cluster exists on shard0, reconciliation completed
/// - Later: New conflicting record arrives on shard1
/// - The new record should conflict with the existing cluster
///
/// In-memory: New record immediately checked against existing cluster.
/// Sharded: Requires re-reconciliation to detect the new conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_arriving_data_triggers_conflict() -> anyhow::Result<()> {
    let config = build_instrument_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1_client = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Phase 1: Initial data on shard0 - establish a cluster
    let initial1 = record_input(
        0,
        "instrument",
        "msci",
        "initial_1",
        vec![
            ("isin", "ISIN_LATE", 0, 100),
            ("country", "GB", 0, 100),
            ("ts_code", "TS_ORIGINAL", 0, 100),
        ],
    );
    let initial2 = record_input(
        1,
        "instrument",
        "msci",
        "initial_2",
        vec![
            ("isin", "ISIN_LATE", 0, 100),
            ("country", "GB", 0, 100),
            ("ts_code", "TS_ORIGINAL", 0, 100),
        ],
    );
    shard0_client
        .ingest_records(IngestRecordsRequest {
            records: vec![initial1, initial2],
        })
        .await?;

    // Initial reconciliation - everything is clean
    let reconcile1 = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    assert_eq!(
        reconcile1.conflicts_blocked, 0,
        "Initial state should have no conflicts"
    );

    // Phase 2: Late-arriving conflicting data on shard1
    sleep(Duration::from_millis(100)).await;

    let late1 = record_input(
        2,
        "instrument",
        "msci",
        "late_1",
        vec![
            ("isin", "ISIN_LATE", 0, 100), // Same identity key
            ("country", "GB", 0, 100),
            ("ts_code", "TS_CONFLICTING", 0, 100), // DIFFERENT ts_code!
        ],
    );
    let late2 = record_input(
        3,
        "instrument",
        "msci",
        "late_2",
        vec![
            ("isin", "ISIN_LATE", 0, 100),
            ("country", "GB", 0, 100),
            ("ts_code", "TS_CONFLICTING", 0, 100),
        ],
    );
    shard1_client
        .ingest_records(IngestRecordsRequest {
            records: vec![late1, late2],
        })
        .await?;

    // Re-reconcile to detect the new conflict
    let reconcile2 = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    sleep(Duration::from_millis(100)).await;

    let conflicts = router_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 100,
            attribute: String::new(),
        })
        .await?
        .into_inner();

    assert!(
        !conflicts.conflicts.is_empty() || reconcile2.conflicts_blocked > 0,
        "Late-arriving data should trigger conflict detection. \
         Original cluster has ts_code=TS_ORIGINAL, new data has ts_code=TS_CONFLICTING. \
         Reconcile2: merges={}, blocked={}; Conflicts: {:?}",
        reconcile2.merges_performed,
        reconcile2.conflicts_blocked,
        conflicts.conflicts
    );

    Ok(())
}

/// Test: MULTI-HOP CHAIN CONFLICT (A=B=C=D with A≠D)
///
/// Scenario (extended from dominus cascading resolution):
/// - A on shard0: linked to B via key1
/// - B on shard1: linked to A via key1, linked to C via key2
/// - C on shard2: linked to B via key2, linked to D via key3
/// - D on shard3: linked to C via key3
/// - A.ts_code ≠ D.ts_code → CONFLICT!
///
/// In-memory: DSU finds A=B=C=D, transitively detects A≠D conflict.
/// Sharded: Requires multiple reconciliation rounds to propagate the chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_hop_chain_conflict_across_four_shards() -> anyhow::Result<()> {
    // Ontology with 3 identity keys for the chain
    let config = DistributedOntologyConfig {
        identity_keys: vec![
            IdentityKeyConfig {
                name: "key1".to_string(),
                attributes: vec!["attr1".to_string()],
            },
            IdentityKeyConfig {
                name: "key2".to_string(),
                attributes: vec!["attr2".to_string()],
            },
            IdentityKeyConfig {
                name: "key3".to_string(),
                attributes: vec!["attr3".to_string()],
            },
        ],
        strong_identifiers: vec!["ts_code".to_string()],
        constraints: vec![ConstraintConfig {
            name: "unique_ts_code".to_string(),
            attribute: "ts_code".to_string(),
            kind: ConstraintKind::UniqueWithinPerspective,
        }],
    };
    let empty_config = DistributedOntologyConfig::empty();

    // Spawn 4 shards
    let (shard0_addr, _h0) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _h1) = spawn_shard(1, empty_config.clone()).await?;
    let (shard2_addr, _h2) = spawn_shard(2, empty_config.clone()).await?;
    let (shard3_addr, _h3) = spawn_shard(3, empty_config.clone()).await?;
    let (router_addr, _hr) = spawn_router(
        vec![shard0_addr, shard1_addr, shard2_addr, shard3_addr],
        empty_config.clone(),
    )
    .await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0 = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1 = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;
    let mut shard2 = ShardServiceClient::connect(format!("http://{}", shard2_addr)).await?;
    let mut shard3 = ShardServiceClient::connect(format!("http://{}", shard3_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Shard 0: Entity A - attr1=V1, ts_code=TS_A
    let a1 = record_input(
        0,
        "instrument",
        "msci",
        "a1",
        vec![
            ("attr1", "CHAIN_V1", 0, 100),   // Links to B
            ("ts_code", "TS_START", 0, 100), // Start of chain
        ],
    );
    let a2 = record_input(
        1,
        "instrument",
        "msci",
        "a2",
        vec![
            ("attr1", "CHAIN_V1", 0, 100),
            ("ts_code", "TS_START", 0, 100),
        ],
    );
    shard0
        .ingest_records(IngestRecordsRequest {
            records: vec![a1, a2],
        })
        .await?;

    // Shard 1: Entity B - attr1=V1 (links to A), attr2=V2 (links to C), ts_code=TS_B
    let b1 = record_input(
        2,
        "instrument",
        "msci",
        "b1",
        vec![
            ("attr1", "CHAIN_V1", 0, 100), // Links to A
            ("attr2", "CHAIN_V2", 0, 100), // Links to C
            ("ts_code", "TS_MID1", 0, 100),
        ],
    );
    let b2 = record_input(
        3,
        "instrument",
        "msci",
        "b2",
        vec![
            ("attr1", "CHAIN_V1", 0, 100),
            ("attr2", "CHAIN_V2", 0, 100),
            ("ts_code", "TS_MID1", 0, 100),
        ],
    );
    shard1
        .ingest_records(IngestRecordsRequest {
            records: vec![b1, b2],
        })
        .await?;

    // Shard 2: Entity C - attr2=V2 (links to B), attr3=V3 (links to D), ts_code=TS_C
    let c1 = record_input(
        4,
        "instrument",
        "msci",
        "c1",
        vec![
            ("attr2", "CHAIN_V2", 0, 100), // Links to B
            ("attr3", "CHAIN_V3", 0, 100), // Links to D
            ("ts_code", "TS_MID2", 0, 100),
        ],
    );
    let c2 = record_input(
        5,
        "instrument",
        "msci",
        "c2",
        vec![
            ("attr2", "CHAIN_V2", 0, 100),
            ("attr3", "CHAIN_V3", 0, 100),
            ("ts_code", "TS_MID2", 0, 100),
        ],
    );
    shard2
        .ingest_records(IngestRecordsRequest {
            records: vec![c1, c2],
        })
        .await?;

    // Shard 3: Entity D - attr3=V3 (links to C), ts_code=TS_D (CONFLICTS with TS_A!)
    let d1 = record_input(
        6,
        "instrument",
        "msci",
        "d1",
        vec![
            ("attr3", "CHAIN_V3", 0, 100), // Links to C
            ("ts_code", "TS_END", 0, 100), // End of chain - CONFLICTS with TS_START!
        ],
    );
    let d2 = record_input(
        7,
        "instrument",
        "msci",
        "d2",
        vec![("attr3", "CHAIN_V3", 0, 100), ("ts_code", "TS_END", 0, 100)],
    );
    shard3
        .ingest_records(IngestRecordsRequest {
            records: vec![d1, d2],
        })
        .await?;

    // Multiple reconciliation rounds to propagate the chain
    let mut total_blocked = 0;
    for i in 0..4 {
        sleep(Duration::from_millis(50)).await;
        let reconcile = router_client
            .reconcile(proto::ReconcileRequest {
                shard_metadata: vec![],
            })
            .await?
            .into_inner();
        total_blocked += reconcile.conflicts_blocked;

        if reconcile.merges_performed == 0 && i > 0 {
            break; // No more progress possible
        }
    }

    sleep(Duration::from_millis(100)).await;

    let conflicts = router_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 100,
            attribute: String::new(),
        })
        .await?
        .into_inner();

    // The chain A=B=C=D should eventually reveal that A.ts_code ≠ D.ts_code
    assert!(
        !conflicts.conflicts.is_empty() || total_blocked > 0,
        "Multi-hop chain A=B=C=D should detect conflict between A.ts_code=TS_START and \
         D.ts_code=TS_END through transitive relationship. \
         Total conflicts blocked: {}; Conflicts: {:?}",
        total_blocked,
        conflicts.conflicts
    );

    Ok(())
}

/// Test: DIFFERENT PERSPECTIVES SHOULD NOT CONFLICT
///
/// Negative test to ensure we don't false-positive:
/// - Entity A on shard0: perspective=msci, ts_code=TS_MSCI
/// - Entity B on shard1: perspective=axioma, ts_code=TS_AXIOMA (different!)
///
/// These should NOT conflict because strong identifiers are per-perspective.
/// The UniqueWithinPerspective constraint means different perspectives can
/// have different values for the same attribute.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn different_perspectives_no_false_positive_conflict() -> anyhow::Result<()> {
    let config = build_instrument_config(); // Uses UniqueWithinPerspective
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0_client = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1_client = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    // Shard 0: MSCI perspective with ts_code=TS_MSCI
    let msci1 = record_input(
        0,
        "instrument",
        "msci",
        "msci_1",
        vec![
            ("isin", "ISIN_SHARED", 0, 100),
            ("country", "DE", 0, 100),
            ("ts_code", "TS_MSCI_VALUE", 0, 100),
        ],
    );
    let msci2 = record_input(
        1,
        "instrument",
        "msci",
        "msci_2",
        vec![
            ("isin", "ISIN_SHARED", 0, 100),
            ("country", "DE", 0, 100),
            ("ts_code", "TS_MSCI_VALUE", 0, 100),
        ],
    );
    shard0_client
        .ingest_records(IngestRecordsRequest {
            records: vec![msci1, msci2],
        })
        .await?;

    // Shard 1: AXIOMA perspective with DIFFERENT ts_code - should NOT conflict
    let axioma1 = record_input(
        2,
        "instrument",
        "axioma",
        "axioma_1",
        vec![
            ("isin", "ISIN_SHARED", 0, 100), // Same identity key
            ("country", "DE", 0, 100),
            ("ts_code", "TS_AXIOMA_VALUE", 0, 100), // Different ts_code, different perspective = OK
        ],
    );
    let axioma2 = record_input(
        3,
        "instrument",
        "axioma",
        "axioma_2",
        vec![
            ("isin", "ISIN_SHARED", 0, 100),
            ("country", "DE", 0, 100),
            ("ts_code", "TS_AXIOMA_VALUE", 0, 100),
        ],
    );
    shard1_client
        .ingest_records(IngestRecordsRequest {
            records: vec![axioma1, axioma2],
        })
        .await?;

    let reconcile = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    sleep(Duration::from_millis(100)).await;

    let conflicts = router_client
        .list_conflicts(proto::ListConflictsRequest {
            start: 0,
            end: 100,
            attribute: "ts_code".to_string(),
        })
        .await?
        .into_inner();

    // Should have NO ts_code conflicts - different perspectives are allowed different values
    assert!(
        conflicts.conflicts.is_empty() && reconcile.conflicts_blocked == 0,
        "Different perspectives should NOT conflict on ts_code. \
         MSCI has TS_MSCI_VALUE, AXIOMA has TS_AXIOMA_VALUE - this is allowed. \
         Reconcile: blocked={}; Conflicts: {:?}",
        reconcile.conflicts_blocked,
        conflicts.conflicts
    );

    Ok(())
}
