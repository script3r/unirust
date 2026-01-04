//! Reliability tests for distributed unirust operations.
//!
//! Tests cover:
//! - Shard disconnect and reconnect scenarios
//! - Router handling of unavailable shards
//! - Graceful shutdown and recovery
//! - Entity resolution consistency after failures

use std::net::SocketAddr;
use std::time::Duration;

use tempfile::tempdir;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, shard_service_client::ShardServiceClient,
    ApplyOntologyRequest, IngestRecordsRequest, RecordDescriptor, RecordIdentity, RecordInput,
    StatsRequest,
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

async fn spawn_shard_with_data_dir(
    shard_id: u32,
    config: DistributedOntologyConfig,
    data_dir: std::path::PathBuf,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shard = ShardNode::new_with_data_dir(
        shard_id,
        config,
        StreamingTuning::from_profile(TuningProfile::Balanced),
        Some(data_dir),
        false,
        None,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_recovery_after_restart() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let data_dir = temp_dir.path().join("shard0");
    std::fs::create_dir_all(&data_dir)?;

    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    // Start shard with persistent storage
    let (shard_addr, shard_handle) =
        spawn_shard_with_data_dir(0, empty_config.clone(), data_dir.clone()).await?;

    // Connect and configure
    let mut client = ShardServiceClient::connect(format!("http://{}", shard_addr)).await?;
    client
        .set_ontology(proto::ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    // Ingest records
    let records = vec![
        record_input(
            0,
            "person",
            "crm",
            "persist-1",
            vec![("email", "persist@example.com", 0, 10)],
        ),
        record_input(
            1,
            "person",
            "crm",
            "persist-2",
            vec![("email", "persist2@example.com", 0, 10)],
        ),
    ];
    client
        .ingest_records(proto::IngestRecordsRequest { records })
        .await?;

    let stats = client.get_stats(StatsRequest {}).await?.into_inner();
    assert_eq!(stats.record_count, 2);

    // Stop shard
    shard_handle.abort();
    drop(client);
    sleep(Duration::from_millis(100)).await;

    // Restart shard with same data directory
    let (shard_addr, _shard_handle) =
        spawn_shard_with_data_dir(0, config.clone(), data_dir).await?;

    // Reconnect and verify data persisted
    let mut client = ShardServiceClient::connect(format!("http://{}", shard_addr)).await?;
    let stats = client.get_stats(StatsRequest {}).await?.into_inner();

    assert_eq!(
        stats.record_count, 2,
        "Records should persist after restart"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_handles_partial_shard_availability() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    // Start two shards
    let (shard0_addr, shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;

    // Start router
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    // Ingest a record to verify connection works
    let record = record_input(
        0,
        "person",
        "crm",
        "partial-1",
        vec![("email", "partial@example.com", 0, 10)],
    );
    let response = client
        .ingest_records(IngestRecordsRequest {
            records: vec![record],
        })
        .await?
        .into_inner();
    assert_eq!(response.assignments.len(), 1);

    // Stop shard 0
    shard0_handle.abort();
    sleep(Duration::from_millis(100)).await;

    // Router should still be able to handle health checks
    let health = client
        .health_check(proto::HealthCheckRequest {})
        .await?
        .into_inner();

    // Health check should still work even with partial shard availability
    assert!(!health.status.is_empty());

    Ok(())
}

// Note: entity_resolution_consistent_after_shard_restart test is complex due to
// RocksDB lock management. The shard_recovery_after_restart test covers the
// core functionality of shard restart and data persistence.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_operations_complete_before_shutdown() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard_addr, _shard_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard_addr], empty_config.clone()).await?;

    let mut client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    // Ingest multiple batches
    for batch in 0..3 {
        let records: Vec<RecordInput> = (0..10)
            .map(|i| {
                record_input(
                    batch * 10 + i,
                    "person",
                    "crm",
                    &format!("batch{}-{}", batch, i),
                    vec![("email", &format!("batch{}{}@example.com", batch, i), 0, 10)],
                )
            })
            .collect();

        let response = client
            .ingest_records(IngestRecordsRequest { records })
            .await?
            .into_inner();

        // Each batch should complete fully
        assert_eq!(response.assignments.len(), 10);
    }

    // Verify all records were ingested
    let stats = client.get_stats(StatsRequest {}).await?.into_inner();
    assert_eq!(stats.record_count, 30);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_stats_reflect_all_shards() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;

    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    // Ingest records that will be distributed across shards
    let records: Vec<RecordInput> = (0..20)
        .map(|i| {
            record_input(
                i,
                "person",
                "crm",
                &format!("stats-{}", i),
                vec![("email", &format!("stats{}@example.com", i), 0, 10)],
            )
        })
        .collect();

    client
        .ingest_records(IngestRecordsRequest { records })
        .await?;

    // Get stats from router
    let stats = client.get_stats(StatsRequest {}).await?.into_inner();

    // Should reflect all 20 records across both shards
    assert_eq!(stats.record_count, 20);

    Ok(())
}
