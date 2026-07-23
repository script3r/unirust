//! Persistent-store tests for cross-shard reconciliation.

use std::net::SocketAddr;
use std::path::PathBuf;

use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, shard_service_client::ShardServiceClient,
    ApplyOntologyRequest, IngestRecordsRequest, RecordDescriptor, RecordIdentity, RecordInput,
};
use unirust_rs::distributed::{
    DistributedOntologyConfig, IdentityKeyConfig, RouterNode, ShardNode,
};
use unirust_rs::{StreamingTuning, TuningProfile};

mod support;

struct SpawnedShard {
    addr: SocketAddr,
    _handle: JoinHandle<()>,
    _data_dir: TempDir,
}

async fn spawn_shard_persistent(
    shard_id: u32,
    config: DistributedOntologyConfig,
) -> anyhow::Result<SpawnedShard> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let data_dir = TempDir::new()?;
    let data_path: PathBuf = data_dir.path().join(format!("shard-{shard_id}"));
    std::fs::create_dir_all(&data_path)?;

    let shard = ShardNode::new_with_data_dir(
        shard_id,
        config,
        StreamingTuning::from_profile(TuningProfile::Balanced),
        Some(data_path),
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
    Ok(SpawnedShard {
        addr,
        _handle: handle,
        _data_dir: data_dir,
    })
}

async fn spawn_shard_at(
    shard_id: u32,
    config: DistributedOntologyConfig,
    data_path: PathBuf,
) -> anyhow::Result<(SocketAddr, ShardNode, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shard = ShardNode::new_with_data_dir(
        shard_id,
        config,
        StreamingTuning::from_profile(TuningProfile::Balanced),
        Some(data_path),
        false,
        None,
    )?;
    let service_shard = shard.clone();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::shard_service_server::ShardServiceServer::new(
                service_shard,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("shard server");
    });
    Ok((addr, shard, handle))
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

fn build_email_config() -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: vec![IdentityKeyConfig {
            name: "email_key".to_string(),
            attributes: vec!["email".to_string()],
        }],
        strong_identifiers: vec!["ssn".to_string()],
        constraints: Vec::new(),
    }
}

/// Ensure cross-shard merges occur with persistent stores.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_shard_merge_persistent_store() -> anyhow::Result<()> {
    let config = build_email_config();
    let empty_config = DistributedOntologyConfig::empty();

    let shard0 = spawn_shard_persistent(0, empty_config.clone()).await?;
    let shard1 = spawn_shard_persistent(1, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0.addr, shard1.addr], empty_config.clone()).await?;

    let mut router_client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let mut shard0_client = ShardServiceClient::connect(format!("http://{}", shard0.addr)).await?;
    let mut shard1_client = ShardServiceClient::connect(format!("http://{}", shard1.addr)).await?;

    router_client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    sleep(Duration::from_millis(100)).await;

    let shard0_rec1 = record_input(
        0,
        "person",
        "hr",
        "hr_001",
        vec![
            ("email", "alice@example.com", 0, 100),
            ("ssn", "1234", 0, 100),
        ],
    );
    let shard0_rec2 = record_input(
        1,
        "person",
        "hr",
        "hr_001b",
        vec![
            ("email", "alice@example.com", 0, 100),
            ("ssn", "1234", 0, 100),
        ],
    );
    shard0_client
        .ingest_records(IngestRecordsRequest {
            records: vec![shard0_rec1, shard0_rec2],
        })
        .await?;

    let shard1_rec1 = record_input(
        2,
        "person",
        "hr",
        "hr_002",
        vec![
            ("email", "alice@example.com", 0, 100),
            ("ssn", "1234", 0, 100),
        ],
    );
    let shard1_rec2 = record_input(
        3,
        "person",
        "hr",
        "hr_002b",
        vec![
            ("email", "alice@example.com", 0, 100),
            ("ssn", "1234", 0, 100),
        ],
    );
    shard1_client
        .ingest_records(IngestRecordsRequest {
            records: vec![shard1_rec1, shard1_rec2],
        })
        .await?;

    let reconcile_response = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: vec![],
        })
        .await?
        .into_inner();

    assert!(
        reconcile_response.merges_performed > 0,
        "Expected cross-shard merges, got: {:?}",
        reconcile_response
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_shard_singleton_merge_survives_full_restart() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let config = build_email_config();
    let data_paths = (0..3)
        .map(|shard_id| root.path().join(format!("shard-{shard_id}")))
        .collect::<Vec<_>>();
    for path in &data_paths {
        std::fs::create_dir_all(path)?;
    }

    let mut shard_addrs = Vec::new();
    let mut shard_nodes = Vec::new();
    let mut shard_handles = Vec::new();
    for (shard_id, path) in data_paths.iter().enumerate() {
        let (addr, shard, handle) =
            spawn_shard_at(shard_id as u32, config.clone(), path.clone()).await?;
        shard_addrs.push(addr);
        shard_nodes.push(shard);
        shard_handles.push(handle);
    }
    let (router_addr, router_handle) = spawn_router(shard_addrs.clone(), config.clone()).await?;
    let mut router_client = RouterServiceClient::connect(format!("http://{router_addr}")).await?;

    for (shard_id, shard_addr) in shard_addrs.iter().enumerate() {
        let mut shard_client = ShardServiceClient::connect(format!("http://{shard_addr}")).await?;
        shard_client
            .ingest_records(IngestRecordsRequest {
                records: vec![record_input(
                    shard_id as u32,
                    "person",
                    "hr",
                    &format!("person-{shard_id}"),
                    vec![
                        ("email", "restart@example.com", 0, 100),
                        ("ssn", "1234", 0, 100),
                    ],
                )],
            })
            .await?;
    }

    let reconciliation = router_client
        .reconcile(proto::ReconcileRequest {
            shard_metadata: Vec::new(),
        })
        .await?
        .into_inner();
    assert_eq!(reconciliation.merges_performed, 2);
    assert_eq!(query_match_count(&mut router_client).await?, 1);

    drop(router_client);
    router_handle.abort();
    let _ = router_handle.await;
    for shard in &shard_nodes {
        shard.shutdown().await?;
    }
    for handle in shard_handles {
        handle.abort();
        let _ = handle.await;
    }
    drop(shard_nodes);
    sleep(Duration::from_millis(100)).await;

    let mut restarted_addrs = Vec::new();
    let mut _restarted_nodes = Vec::new();
    let mut _restarted_handles = Vec::new();
    for (shard_id, path) in data_paths.iter().enumerate() {
        let (addr, shard, handle) =
            spawn_shard_at(shard_id as u32, config.clone(), path.clone()).await?;
        restarted_addrs.push(addr);
        _restarted_nodes.push(shard);
        _restarted_handles.push(handle);
    }
    let (router_addr, _router_handle) = spawn_router(restarted_addrs, config).await?;
    let mut router_client = RouterServiceClient::connect(format!("http://{router_addr}")).await?;
    assert_eq!(query_match_count(&mut router_client).await?, 1);

    Ok(())
}

async fn query_match_count(
    client: &mut RouterServiceClient<tonic::transport::Channel>,
) -> anyhow::Result<usize> {
    let response = client
        .query_entities(proto::QueryEntitiesRequest {
            descriptors: vec![proto::QueryDescriptor {
                attr: "email".to_string(),
                value: "restart@example.com".to_string(),
            }],
            start: 0,
            end: 100,
        })
        .await?
        .into_inner();
    match response.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => {
            Ok(matches.matches.len())
        }
        other => anyhow::bail!("expected reconciled query matches, got {other:?}"),
    }
}
