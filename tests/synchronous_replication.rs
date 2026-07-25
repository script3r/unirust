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
use tonic::Request;
use unirust_rs::distributed::proto::{
    self, shard_service_client::ShardServiceClient, ConfigVersionRequest, IngestRecordsRequest,
    RecordDescriptor, RecordIdentity, RecordInput, StatsRequest,
};
use unirust_rs::distributed::{
    DistributedOntologyConfig, IdentityKeyConfig, RouterNode, ShardNode,
    DISTRIBUTED_PROTOCOL_VERSION,
};
use unirust_rs::{StreamingTuning, TuningProfile};

static PERSISTENT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const REPLICATION_TOKEN: &str = "42fd59df99d782a1e9d614e5f2f9d3a425da036468fceb91c0dd3bcc6bf3d729";
const WRONG_REPLICATION_TOKEN: &str =
    "f12307f3555950adee23c22dc0ee37c597451f44418595d6f0774258313a649e";

#[derive(Clone)]
struct FailIngest<S> {
    inner: S,
    fail: Arc<AtomicBool>,
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
        if self.fail.load(Ordering::Acquire)
            && request.uri().path() == "/unirust.ShardService/IngestRecords"
        {
            return Box::pin(async {
                Ok(tonic::Status::unavailable("injected replica outage").into_http())
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

async fn spawn_node(node: ShardNode) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let service_node = node.clone();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::shard_service_server::ShardServiceServer::new(
                service_node,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("shard server");
    });
    Ok((addr, handle))
}

async fn spawn_failable_node(
    node: ShardNode,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>, Arc<AtomicBool>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let fail = Arc::new(AtomicBool::new(false));
    let service = FailIngest {
        inner: proto::shard_service_server::ShardServiceServer::new(node),
        fail: fail.clone(),
    };
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("shard server");
    });
    Ok((addr, handle, fail))
}

async fn stop_node(node: ShardNode, handle: JoinHandle<()>) -> anyhow::Result<()> {
    node.shutdown().await?;
    handle.abort();
    let _ = handle.await;
    drop(node);
    sleep(Duration::from_millis(50)).await;
    Ok(())
}

fn config() -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: vec![IdentityKeyConfig {
            name: "email".to_string(),
            attributes: vec!["email".to_string()],
        }],
        strong_identifiers: Vec::new(),
        constraints: Vec::new(),
    }
}

fn node(data_dir: PathBuf, backup_dir: PathBuf) -> anyhow::Result<ShardNode> {
    ShardNode::new_with_storage_paths(
        0,
        config(),
        StreamingTuning::from_profile(TuningProfile::Balanced),
        Some(data_dir),
        Some(backup_dir),
        false,
        None,
    )
}

fn authenticated_request<T>(payload: T, token: &str) -> Request<T> {
    let mut request = Request::new(payload);
    request
        .metadata_mut()
        .insert("x-unirust-replication-token", token.parse().unwrap());
    request
}

fn record(index: u32) -> RecordInput {
    RecordInput {
        index,
        identity: Some(RecordIdentity {
            entity_type: "person".to_string(),
            perspective: "crm".to_string(),
            uid: "replicated-source".to_string(),
        }),
        descriptors: vec![RecordDescriptor {
            attr: "email".to_string(),
            value: "replicated@example.com".to_string(),
            start: 0,
            end: 100,
        }],
    }
}

fn distinct_record(index: u32, source: u32) -> RecordInput {
    RecordInput {
        index,
        identity: Some(RecordIdentity {
            entity_type: "person".to_string(),
            perspective: "crm".to_string(),
            uid: format!("replicated-source-{source}"),
        }),
        descriptors: vec![RecordDescriptor {
            attr: "email".to_string(),
            value: format!("replicated-{source}@example.com"),
            start: 0,
            end: 100,
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_ingest_survives_primary_volume_loss_and_manual_failover() -> anyhow::Result<()>
{
    use proto::router_service_server::RouterService;

    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let volumes = TempDir::new()?;
    let primary_data = volumes.path().join("primary-data");
    let replica_data = volumes.path().join("replica-data");
    let primary_backup = volumes.path().join("primary-backup");
    let replica_backup = volumes.path().join("replica-backup");

    let replica_node = node(replica_data.clone(), replica_backup.clone())?
        .into_replica(REPLICATION_TOKEN.into())?;
    let (replica_addr, replica_handle) = spawn_node(replica_node.clone()).await?;
    let mut replica_client = ShardServiceClient::connect(format!("http://{replica_addr}")).await?;

    let direct_error = replica_client
        .ingest_records(IngestRecordsRequest {
            records: vec![record(0)],
            internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
        })
        .await
        .expect_err("a passive replica must reject direct mutations");
    assert_eq!(direct_error.code(), tonic::Code::PermissionDenied);

    let primary_node = node(primary_data.clone(), primary_backup)?
        .with_replica(replica_client.clone(), REPLICATION_TOKEN.into())
        .await?;
    let (primary_addr, primary_handle) = spawn_node(primary_node.clone()).await?;
    let router = RouterNode::connect(vec![format!("http://{primary_addr}")], config()).await?;
    let response = RouterService::ingest_records(
        router.as_ref(),
        Request::new(IngestRecordsRequest {
            records: (0..128)
                .map(|source| distinct_record(source, source))
                .collect(),
            internal_protocol_version: 0,
        }),
    )
    .await?
    .into_inner();
    assert_eq!(response.assignments.len(), 128);

    let mut primary_client = ShardServiceClient::connect(format!("http://{primary_addr}")).await?;
    let primary_stats = primary_client
        .get_stats(StatsRequest {})
        .await?
        .into_inner();
    let replica_stats = replica_client
        .get_stats(StatsRequest {})
        .await?
        .into_inner();
    assert_eq!(primary_stats.record_count, 128);
    assert_eq!(primary_stats, replica_stats);

    let primary_config = primary_client
        .get_config_version(ConfigVersionRequest {
            include_durable_state_digest: true,
        })
        .await?
        .into_inner();
    let replica_config = replica_client
        .get_config_version(authenticated_request(
            ConfigVersionRequest {
                include_durable_state_digest: true,
            },
            REPLICATION_TOKEN,
        ))
        .await?
        .into_inner();
    assert_eq!(
        proto::ShardRole::try_from(primary_config.shard_role)?,
        proto::ShardRole::Primary
    );
    assert_eq!(
        proto::ShardRole::try_from(replica_config.shard_role)?,
        proto::ShardRole::Replica
    );
    assert_eq!(
        primary_config.durable_state_digest,
        replica_config.durable_state_digest
    );

    let replica_router_error =
        RouterNode::connect(vec![format!("http://{replica_addr}")], config())
            .await
            .err()
            .expect("routers must reject passive replicas");
    assert_eq!(replica_router_error.code(), tonic::Code::FailedPrecondition);

    drop(router);
    drop(primary_client);
    drop(replica_client);
    stop_node(primary_node, primary_handle).await?;
    stop_node(replica_node, replica_handle).await?;

    let restarted_replica = node(replica_data.clone(), replica_backup.clone())?
        .into_replica(REPLICATION_TOKEN.into())?;
    let (restarted_replica_addr, restarted_replica_handle) =
        spawn_node(restarted_replica.clone()).await?;
    let restarted_replica_client =
        ShardServiceClient::connect(format!("http://{restarted_replica_addr}")).await?;
    let restarted_primary = node(primary_data.clone(), volumes.path().join("primary-backup"))?
        .with_replica(restarted_replica_client, REPLICATION_TOKEN.into())
        .await?;
    let (restarted_primary_addr, restarted_primary_handle) =
        spawn_node(restarted_primary.clone()).await?;
    let mut restarted_primary_client =
        ShardServiceClient::connect(format!("http://{restarted_primary_addr}")).await?;
    assert_eq!(
        restarted_primary_client
            .health_check(proto::HealthCheckRequest {})
            .await?
            .into_inner()
            .status,
        "ok"
    );
    drop(restarted_primary_client);
    stop_node(restarted_primary, restarted_primary_handle).await?;
    stop_node(restarted_replica, restarted_replica_handle).await?;

    std::fs::remove_dir_all(&primary_data)?;

    let promoted_node = node(replica_data, replica_backup)?;
    let (promoted_addr, promoted_handle) = spawn_node(promoted_node.clone()).await?;
    let promoted_router =
        RouterNode::connect(vec![format!("http://{promoted_addr}")], config()).await?;
    let retry = RouterService::ingest_records(
        promoted_router.as_ref(),
        Request::new(IngestRecordsRequest {
            records: vec![distinct_record(999, 0)],
            internal_protocol_version: 0,
        }),
    )
    .await?
    .into_inner();
    assert_eq!(retry.assignments.len(), 1);
    let mut promoted_client =
        ShardServiceClient::connect(format!("http://{promoted_addr}")).await?;
    let promoted_stats = promoted_client
        .get_stats(StatsRequest {})
        .await?
        .into_inner();
    assert_eq!(promoted_stats.record_count, 128);

    drop(promoted_router);
    drop(promoted_client);
    stop_node(promoted_node, promoted_handle).await?;
    assert!(!primary_data.exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_outage_prevents_local_commit_and_latches_primary_closed() -> anyhow::Result<()> {
    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let volumes = TempDir::new()?;
    let primary_data = volumes.path().join("primary-data");
    let replica_data = volumes.path().join("replica-data");
    let replica_node = node(replica_data, volumes.path().join("replica-backup"))?
        .into_replica(REPLICATION_TOKEN.into())?;
    let (replica_addr, replica_handle, fail_replica) =
        spawn_failable_node(replica_node.clone()).await?;
    let replica_client = ShardServiceClient::connect(format!("http://{replica_addr}")).await?;
    let primary_node = node(primary_data, volumes.path().join("primary-backup"))?
        .with_replica(replica_client, REPLICATION_TOKEN.into())
        .await?;
    let (primary_addr, primary_handle) = spawn_node(primary_node.clone()).await?;
    let mut primary_client = ShardServiceClient::connect(format!("http://{primary_addr}")).await?;

    fail_replica.store(true, Ordering::Release);
    let error = primary_client
        .ingest_records(IngestRecordsRequest {
            records: vec![record(0)],
            internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
        })
        .await
        .expect_err("primary must not commit while its replica is unavailable");
    assert_eq!(error.code(), tonic::Code::Aborted);

    let blocked = primary_client
        .ingest_records(IngestRecordsRequest {
            records: vec![record(0)],
            internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
        })
        .await
        .expect_err("an ambiguous replication failure must latch the primary closed");
    assert_eq!(blocked.code(), tonic::Code::FailedPrecondition);
    let health = primary_client
        .health_check(proto::HealthCheckRequest {})
        .await
        .expect_err("latched primary must fail readiness");
    assert_eq!(health.code(), tonic::Code::FailedPrecondition);

    drop(primary_client);
    stop_node(primary_node, primary_handle).await?;
    stop_node(replica_node, replica_handle).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_pairing_rejects_wrong_credentials_and_mismatched_state() -> anyhow::Result<()> {
    use proto::shard_service_server::ShardService;

    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let volumes = TempDir::new()?;
    let replica_node = node(
        volumes.path().join("replica-data"),
        volumes.path().join("replica-backup"),
    )?
    .into_replica(REPLICATION_TOKEN.into())?;
    let (replica_addr, replica_handle) = spawn_node(replica_node.clone()).await?;
    let replica_client = ShardServiceClient::connect(format!("http://{replica_addr}")).await?;

    let wrong_token_error = node(
        volumes.path().join("wrong-token-primary-data"),
        volumes.path().join("wrong-token-primary-backup"),
    )?
    .with_replica(replica_client.clone(), WRONG_REPLICATION_TOKEN.into())
    .await
    .err()
    .expect("replica pairing must authenticate the primary");
    assert_eq!(wrong_token_error.code(), tonic::Code::PermissionDenied);

    let mismatched_primary = node(
        volumes.path().join("mismatched-primary-data"),
        volumes.path().join("mismatched-primary-backup"),
    )?;
    ShardService::ingest_records(
        &mismatched_primary,
        Request::new(IngestRecordsRequest {
            records: vec![record(0)],
            internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
        }),
    )
    .await?;
    let mismatch_error = mismatched_primary
        .with_replica(replica_client, REPLICATION_TOKEN.into())
        .await
        .err()
        .expect("replica pairing must prove exact durable state equality");
    assert_eq!(mismatch_error.code(), tonic::Code::FailedPrecondition);
    assert!(
        mismatch_error.message().contains("durable state"),
        "{}",
        mismatch_error.message()
    );

    stop_node(replica_node, replica_handle).await?;
    Ok(())
}
