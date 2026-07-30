//! Persistent-store tests for cross-shard reconciliation.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::{http, Service};
use tonic::server::NamedService;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, shard_service_client::ShardServiceClient,
    ApplyOntologyRequest, IngestRecordsRequest, RecordDescriptor, RecordIdentity, RecordInput,
};
use unirust_rs::distributed::{
    hash_record_to_shard, hash_source_identity_to_shard, DistributedOntologyConfig,
    IdentityKeyConfig, RouterNode, ShardNode, DISTRIBUTED_PROTOCOL_VERSION,
};
use unirust_rs::{StreamingTuning, TuningProfile};

mod support;

// Each case opens multiple RocksDB instances with many column families. Running
// them concurrently exceeds the default macOS per-process file descriptor limit.
static PERSISTENT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct SpawnedShard {
    addr: SocketAddr,
    _handle: JoinHandle<()>,
    _data_dir: TempDir,
}

#[derive(Clone)]
struct FailIngest<S> {
    inner: S,
}

impl<S, B> Service<http::Request<B>> for FailIngest<S>
where
    S: Service<
            http::Request<B>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Send,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() == "/unirust.ShardService/IngestRecords" {
            return Box::pin(async {
                Ok(tonic::Status::unavailable("injected target ingest failure").into_http())
            });
        }
        Box::pin(self.inner.call(request))
    }
}

impl<S> NamedService for FailIngest<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

#[derive(Clone)]
struct FailFirstApplyMerge<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
}

impl<S, B> Service<http::Request<B>> for FailFirstApplyMerge<S>
where
    S: Service<
            http::Request<B>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Send,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() == "/unirust.ShardService/ApplyMerges"
            && self.fail_next.swap(false, Ordering::AcqRel)
        {
            return Box::pin(async {
                Ok(tonic::Status::unavailable("injected ApplyMerges failure").into_http())
            });
        }
        Box::pin(self.inner.call(request))
    }
}

impl<S> NamedService for FailFirstApplyMerge<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
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

async fn spawn_apply_merge_failing_shard_at(
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
    let service = proto::shard_service_server::ShardServiceServer::new(shard.clone());
    let service = FailFirstApplyMerge {
        inner: service,
        fail_next: Arc::new(AtomicBool::new(true)),
    };
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("shard server");
    });
    Ok((addr, shard, handle))
}

async fn spawn_apply_merge_stalling_shard_at(
    shard_id: u32,
    config: DistributedOntologyConfig,
    data_path: PathBuf,
) -> anyhow::Result<(
    SocketAddr,
    ShardNode,
    JoinHandle<()>,
    support::StallControls,
)> {
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
    let (service, controls) = support::stall_response(
        proto::shard_service_server::ShardServiceServer::new(shard.clone()),
        "/unirust.ShardService/ApplyMerges",
        true,
    );
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("shard server");
    });
    Ok((addr, shard, handle, controls))
}

async fn spawn_ingest_failing_shard_at(
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
    let service = proto::shard_service_server::ShardServiceServer::new(service_shard);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(FailIngest { inner: service })
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
    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
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
            internal_protocol_version: 5,
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
            internal_protocol_version: 5,
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
    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
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
                internal_protocol_version: 5,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_identity_reservation_survives_routing_change_and_restart() -> anyhow::Result<()> {
    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let root = tempfile::tempdir()?;
    let config = build_email_config();
    let data_paths = (0..2)
        .map(|shard_id| root.path().join(format!("source-reservation-{shard_id}")))
        .collect::<Vec<_>>();
    for path in &data_paths {
        std::fs::create_dir_all(path)?;
    }

    let mut original = record_input(
        0,
        "person",
        "crm",
        "immutable-source",
        vec![("email", "route-original@example.com", 0, 100)],
    );
    let original_target = hash_record_to_shard(&config, &original, 2);
    let mut changed = None;
    for candidate in 0..1_000 {
        let record = record_input(
            0,
            "person",
            "crm",
            "immutable-source",
            vec![(
                "email",
                &format!("route-changed-{candidate}@example.com"),
                0,
                100,
            )],
        );
        if hash_record_to_shard(&config, &record, 2) != original_target {
            changed = Some(record);
            break;
        }
    }
    let changed = changed.expect("test must find a payload routed to the other shard");

    let mut shard_nodes = Vec::new();
    let mut shard_handles = Vec::new();
    let mut shard_addrs = Vec::new();
    for (shard_id, path) in data_paths.iter().enumerate() {
        let (addr, shard, handle) =
            spawn_shard_at(shard_id as u32, config.clone(), path.clone()).await?;
        shard_addrs.push(addr);
        shard_nodes.push(shard);
        shard_handles.push(handle);
    }
    let (router_addr, router_handle) = spawn_router(shard_addrs.clone(), config.clone()).await?;
    let mut router_client = RouterServiceClient::connect(format!("http://{router_addr}")).await?;

    let response = router_client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![original.clone()],
        })
        .await?
        .into_inner();
    assert_eq!(response.assignments.len(), 1);
    assert_eq!(response.assignments[0].shard_id as usize, original_target);

    let error = router_client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![changed.clone()],
        })
        .await
        .expect_err("a routing change must not evade immutable source identity");
    assert_eq!(error.code(), tonic::Code::AlreadyExists);
    assert_eq!(cluster_record_count(&shard_addrs).await?, 1);

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

    let mut restarted_nodes = Vec::new();
    let mut restarted_handles = Vec::new();
    let mut restarted_addrs = Vec::new();
    for (shard_id, path) in data_paths.iter().enumerate() {
        let (addr, shard, handle) =
            spawn_shard_at(shard_id as u32, config.clone(), path.clone()).await?;
        restarted_addrs.push(addr);
        restarted_nodes.push(shard);
        restarted_handles.push(handle);
    }
    let (router_addr, _router_handle) =
        spawn_router(restarted_addrs.clone(), config.clone()).await?;
    let mut router_client = RouterServiceClient::connect(format!("http://{router_addr}")).await?;

    let error = router_client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![changed],
        })
        .await
        .expect_err("the source reservation must survive a full cluster restart");
    assert_eq!(error.code(), tonic::Code::AlreadyExists);

    original.index = 1;
    let retry = router_client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![original],
        })
        .await?
        .into_inner();
    assert_eq!(retry.assignments.len(), 1);
    assert_eq!(retry.assignments[0].shard_id as usize, original_target);
    assert_eq!(cluster_record_count(&restarted_addrs).await?, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_backfills_legacy_records_before_serving_ingest() -> anyhow::Result<()> {
    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let root = tempfile::tempdir()?;
    let config = build_email_config();
    let data_paths = (0..2)
        .map(|shard_id| root.path().join(format!("legacy-reservation-{shard_id}")))
        .collect::<Vec<_>>();
    for path in &data_paths {
        std::fs::create_dir_all(path)?;
    }

    let original = record_input(
        0,
        "person",
        "crm",
        "legacy-source",
        vec![("email", "legacy-original@example.com", 0, 100)],
    );
    let original_target = hash_record_to_shard(&config, &original, 2);
    let mut changed = None;
    for candidate in 0..1_000 {
        let record = record_input(
            0,
            "person",
            "crm",
            "legacy-source",
            vec![(
                "email",
                &format!("legacy-changed-{candidate}@example.com"),
                0,
                100,
            )],
        );
        if hash_record_to_shard(&config, &record, 2) != original_target {
            changed = Some(record);
            break;
        }
    }
    let changed = changed.expect("test must find a changed payload routed elsewhere");

    let mut shard_addrs = Vec::new();
    let mut _shard_nodes = Vec::new();
    let mut _shard_handles = Vec::new();
    for (shard_id, path) in data_paths.iter().enumerate() {
        let (addr, shard, handle) =
            spawn_shard_at(shard_id as u32, config.clone(), path.clone()).await?;
        shard_addrs.push(addr);
        _shard_nodes.push(shard);
        _shard_handles.push(handle);
    }

    let mut original_target_client =
        ShardServiceClient::connect(format!("http://{}", shard_addrs[original_target])).await?;
    let status_before = original_target_client
        .get_config_version(proto::ConfigVersionRequest {
            include_durable_state_digest: false,
        })
        .await?
        .into_inner();
    assert_eq!(status_before.source_reservation_backfill_version, 0);
    original_target_client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![original],
        })
        .await?;

    let (router_addr, _router_handle) = spawn_router(shard_addrs.clone(), config.clone()).await?;
    for shard_addr in &shard_addrs {
        let mut shard_client = ShardServiceClient::connect(format!("http://{shard_addr}")).await?;
        let status = shard_client
            .get_config_version(proto::ConfigVersionRequest {
                include_durable_state_digest: false,
            })
            .await?
            .into_inner();
        assert_eq!(
            status.source_reservation_backfill_version,
            DISTRIBUTED_PROTOCOL_VERSION
        );
        assert_eq!(status.source_reservation_shard_count, 2);
    }

    let mut router_client = RouterServiceClient::connect(format!("http://{router_addr}")).await?;
    let error = router_client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![changed],
        })
        .await
        .expect_err("backfilled legacy source identity must reject a changed payload");
    assert_eq!(error.code(), tonic::Code::AlreadyExists);
    assert_eq!(cluster_record_count(&shard_addrs).await?, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserved_ingest_retries_after_target_failure_and_full_restart() -> anyhow::Result<()> {
    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let root = tempfile::tempdir()?;
    let config = build_email_config();
    let data_paths = (0..2)
        .map(|shard_id| root.path().join(format!("partial-reservation-{shard_id}")))
        .collect::<Vec<_>>();
    for path in &data_paths {
        std::fs::create_dir_all(path)?;
    }

    let mut selected = None;
    for candidate in 0..1_000 {
        let record = record_input(
            0,
            "person",
            "crm",
            &format!("partial-source-{candidate}"),
            vec![(
                "email",
                &format!("partial-route-{candidate}@example.com"),
                0,
                100,
            )],
        );
        let identity = record.identity.as_ref().unwrap();
        let owner = hash_source_identity_to_shard(identity, 2);
        let target = hash_record_to_shard(&config, &record, 2);
        if owner != target {
            selected = Some((record, owner, target));
            break;
        }
    }
    let (mut record, _owner, target) =
        selected.expect("test must find distinct reservation owner and ingest target");

    let mut shard_nodes = Vec::new();
    let mut shard_handles = Vec::new();
    let mut shard_addrs = Vec::new();
    for (shard_id, path) in data_paths.iter().enumerate() {
        let (addr, shard, handle) = if shard_id == target {
            spawn_ingest_failing_shard_at(shard_id as u32, config.clone(), path.clone()).await?
        } else {
            spawn_shard_at(shard_id as u32, config.clone(), path.clone()).await?
        };
        shard_addrs.push(addr);
        shard_nodes.push(shard);
        shard_handles.push(handle);
    }
    let (router_addr, router_handle) = spawn_router(shard_addrs, config.clone()).await?;
    let mut router_client = RouterServiceClient::connect(format!("http://{router_addr}")).await?;

    let error = router_client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![record.clone()],
        })
        .await
        .expect_err("ingest must fail while its target shard is unavailable");
    assert_eq!(error.code(), tonic::Code::Unavailable);

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

    let mut restarted_nodes = Vec::new();
    let mut restarted_handles = Vec::new();
    let mut restarted_addrs = Vec::new();
    for (shard_id, path) in data_paths.iter().enumerate() {
        let (addr, shard, handle) =
            spawn_shard_at(shard_id as u32, config.clone(), path.clone()).await?;
        restarted_addrs.push(addr);
        restarted_nodes.push(shard);
        restarted_handles.push(handle);
    }
    let (router_addr, _router_handle) =
        spawn_router(restarted_addrs.clone(), config.clone()).await?;
    let mut router_client = RouterServiceClient::connect(format!("http://{router_addr}")).await?;
    assert_eq!(cluster_record_count(&restarted_addrs).await?, 0);

    record.index = 1;
    let retry = router_client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![record.clone()],
        })
        .await?
        .into_inner();
    assert_eq!(retry.assignments.len(), 1);
    assert_eq!(retry.assignments[0].shard_id as usize, target);
    assert_eq!(cluster_record_count(&restarted_addrs).await?, 1);

    let mut changed = record;
    changed.descriptors[0].value = "changed-after-partial-failure@example.com".to_string();
    let error = router_client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![changed],
        })
        .await
        .expect_err("the durable pre-ingest reservation must reject a changed payload");
    assert_eq!(error.code(), tonic::Code::AlreadyExists);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_reconciliation_blocks_traffic_and_recovers_after_full_restart(
) -> anyhow::Result<()> {
    use proto::router_service_server::RouterService;

    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let root = tempfile::tempdir()?;
    let config = build_email_config();
    let path0 = root.path().join("reconcile-shard-0");
    let path1 = root.path().join("reconcile-shard-1");
    let (addr0, shard0, handle0) = spawn_shard_at(0, config.clone(), path0.clone()).await?;
    let (addr1, shard1, handle1) =
        spawn_apply_merge_failing_shard_at(1, config.clone(), path1.clone()).await?;
    let router = RouterNode::connect(
        vec![format!("http://{addr0}"), format!("http://{addr1}")],
        config.clone(),
    )
    .await?;

    let mut client0 = ShardServiceClient::connect(format!("http://{addr0}")).await?;
    let mut client1 = ShardServiceClient::connect(format!("http://{addr1}")).await?;
    client0
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![record_input(
                0,
                "person",
                "hr",
                "partial-merge-0",
                vec![
                    ("email", "partial-merge@example.com", 0, 100),
                    ("ssn", "1234", 0, 100),
                ],
            )],
        })
        .await?;
    client1
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 5,
            records: vec![record_input(
                1,
                "person",
                "hr",
                "partial-merge-1",
                vec![
                    ("email", "partial-merge@example.com", 0, 100),
                    ("ssn", "1234", 0, 100),
                ],
            )],
        })
        .await?;

    let error = RouterService::reconcile(
        router.as_ref(),
        tonic::Request::new(proto::ReconcileRequest {
            shard_metadata: Vec::new(),
        }),
    )
    .await
    .expect_err("second shard must fail the first merge application");
    assert_eq!(error.code(), tonic::Code::Unavailable);

    let blocked = RouterService::ingest_records(
        router.as_ref(),
        tonic::Request::new(IngestRecordsRequest {
            internal_protocol_version: 0,
            records: vec![record_input(
                2,
                "person",
                "hr",
                "blocked-during-partial-merge",
                vec![("email", "blocked@example.com", 0, 100)],
            )],
        }),
    )
    .await
    .expect_err("traffic must fail closed after a partial reconciliation apply");
    assert_eq!(blocked.code(), tonic::Code::FailedPrecondition);

    drop(client0);
    drop(client1);
    drop(router);
    shard0.shutdown().await?;
    shard1.shutdown().await?;
    handle0.abort();
    handle1.abort();
    let _ = handle0.await;
    let _ = handle1.await;
    drop(shard0);
    drop(shard1);
    sleep(Duration::from_millis(100)).await;

    let (restarted_addr0, restarted_shard0, restarted_handle0) =
        spawn_shard_at(0, config.clone(), path0).await?;
    let (restarted_addr1, restarted_shard1, restarted_handle1) =
        spawn_shard_at(1, config.clone(), path1).await?;
    let restarted_router = RouterNode::connect(
        vec![
            format!("http://{restarted_addr0}"),
            format!("http://{restarted_addr1}"),
        ],
        config,
    )
    .await?;
    let query = RouterService::query_entities(
        restarted_router.as_ref(),
        tonic::Request::new(proto::QueryEntitiesRequest {
            descriptors: vec![proto::QueryDescriptor {
                attr: "email".to_string(),
                value: "partial-merge@example.com".to_string(),
            }],
            start: 0,
            end: 100,
        }),
    )
    .await?
    .into_inner();
    match query.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => {
            assert_eq!(matches.matches.len(), 1);
        }
        other => anyhow::bail!("expected one reconciled entity after restart, got {other:?}"),
    }

    drop(restarted_router);
    restarted_shard0.shutdown().await?;
    restarted_shard1.shutdown().await?;
    restarted_handle0.abort();
    restarted_handle1.abort();
    let _ = restarted_handle0.await;
    let _ = restarted_handle1.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_partial_reconciliation_latches_router_closed() -> anyhow::Result<()> {
    use proto::router_service_server::RouterService;

    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let root = tempfile::tempdir()?;
    let config = build_email_config();
    let (addr0, shard0, handle0) =
        spawn_shard_at(0, config.clone(), root.path().join("cancel-shard-0")).await?;
    let (addr1, shard1, handle1, controls) =
        spawn_apply_merge_stalling_shard_at(1, config.clone(), root.path().join("cancel-shard-1"))
            .await?;
    let router = RouterNode::connect(
        vec![format!("http://{addr0}"), format!("http://{addr1}")],
        config,
    )
    .await?;

    for (shard_addr, uid) in [(addr0, "cancel-merge-0"), (addr1, "cancel-merge-1")] {
        let mut client = ShardServiceClient::connect(format!("http://{shard_addr}")).await?;
        client
            .ingest_records(IngestRecordsRequest {
                internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
                records: vec![record_input(
                    0,
                    "person",
                    "hr",
                    uid,
                    vec![
                        ("email", "cancel-merge@example.com", 0, 100),
                        ("ssn", "1234", 0, 100),
                    ],
                )],
            })
            .await?;
    }

    let reconcile_router = router.clone();
    let reconcile_task = tokio::spawn(async move {
        RouterService::reconcile(
            reconcile_router.as_ref(),
            tonic::Request::new(proto::ReconcileRequest {
                shard_metadata: Vec::new(),
            }),
        )
        .await
    });
    controls
        .wait_until_committed(Duration::from_secs(10))
        .await?;
    reconcile_task.abort();
    assert!(reconcile_task
        .await
        .expect_err("reconciliation task must be cancelled")
        .is_cancelled());
    controls.release();

    let blocked = RouterService::health_check(
        router.as_ref(),
        tonic::Request::new(proto::HealthCheckRequest {}),
    )
    .await
    .expect_err("router must fail closed after cancellation during reconciliation apply");
    assert_eq!(blocked.code(), tonic::Code::FailedPrecondition);

    RouterService::reconcile(
        router.as_ref(),
        tonic::Request::new(proto::ReconcileRequest {
            shard_metadata: Vec::new(),
        }),
    )
    .await?;
    RouterService::health_check(
        router.as_ref(),
        tonic::Request::new(proto::HealthCheckRequest {}),
    )
    .await?;

    drop(router);
    shard0.shutdown().await?;
    shard1.shutdown().await?;
    handle0.abort();
    handle1.abort();
    let _ = handle0.await;
    let _ = handle1.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_reconciliation_can_be_retried_in_place() -> anyhow::Result<()> {
    use proto::router_service_server::RouterService;

    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let root = tempfile::tempdir()?;
    let config = build_email_config();
    let (addr0, shard0, handle0) =
        spawn_shard_at(0, config.clone(), root.path().join("retry-shard-0")).await?;
    let (addr1, shard1, handle1) =
        spawn_apply_merge_failing_shard_at(1, config.clone(), root.path().join("retry-shard-1"))
            .await?;
    let router = RouterNode::connect(
        vec![format!("http://{addr0}"), format!("http://{addr1}")],
        config,
    )
    .await?;
    for (shard_addr, uid) in [(addr0, "retry-merge-0"), (addr1, "retry-merge-1")] {
        let mut client = ShardServiceClient::connect(format!("http://{shard_addr}")).await?;
        client
            .ingest_records(IngestRecordsRequest {
                internal_protocol_version: 5,
                records: vec![record_input(
                    0,
                    "person",
                    "hr",
                    uid,
                    vec![
                        ("email", "retry-merge@example.com", 0, 100),
                        ("ssn", "1234", 0, 100),
                    ],
                )],
            })
            .await?;
    }

    let request = proto::ReconcileRequest {
        shard_metadata: Vec::new(),
    };
    RouterService::reconcile(router.as_ref(), tonic::Request::new(request.clone()))
        .await
        .expect_err("first merge application must fail");
    let retried = RouterService::reconcile(router.as_ref(), tonic::Request::new(request))
        .await?
        .into_inner();
    assert_eq!(retried.merges_performed, 1);

    let query = RouterService::query_entities(
        router.as_ref(),
        tonic::Request::new(proto::QueryEntitiesRequest {
            descriptors: vec![proto::QueryDescriptor {
                attr: "email".to_string(),
                value: "retry-merge@example.com".to_string(),
            }],
            start: 0,
            end: 100,
        }),
    )
    .await?
    .into_inner();
    match query.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => {
            assert_eq!(matches.matches.len(), 1);
        }
        other => anyhow::bail!("expected one reconciled entity after retry, got {other:?}"),
    }

    drop(router);
    shard0.shutdown().await?;
    shard1.shutdown().await?;
    handle0.abort();
    handle1.abort();
    let _ = handle0.await;
    let _ = handle1.await;
    Ok(())
}

async fn cluster_record_count(shard_addrs: &[SocketAddr]) -> anyhow::Result<u64> {
    let mut total = 0;
    for shard_addr in shard_addrs {
        let mut client = ShardServiceClient::connect(format!("http://{shard_addr}")).await?;
        total += client
            .get_stats(proto::StatsRequest {})
            .await?
            .into_inner()
            .record_count;
    }
    Ok(total)
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
