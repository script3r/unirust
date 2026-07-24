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
    self, shard_service_client::ShardServiceClient, CheckpointRequest, IngestRecordsRequest,
    RecordDescriptor, RecordIdentity, RecordInput, StatsRequest,
};
use unirust_rs::distributed::{
    hash_record_to_shard, hash_source_identity_to_shard, DistributedOntologyConfig,
    IdentityKeyConfig, RouterNode, ShardNode,
};
use unirust_rs::{
    read_cluster_checkpoint_manifest, restore_checkpoint, restore_checkpoint_for_shard,
    StreamingTuning, TuningProfile,
};

static PERSISTENT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone)]
struct FailFirstCheckpoint<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
}

impl<S, B> Service<http::Request<B>> for FailFirstCheckpoint<S>
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
        if request.uri().path() == "/unirust.ShardService/Checkpoint"
            && self.fail_next.swap(false, Ordering::AcqRel)
        {
            return Box::pin(async {
                Ok(tonic::Status::unavailable("injected checkpoint failure").into_http())
            });
        }
        Box::pin(self.inner.call(request))
    }
}

impl<S> NamedService for FailFirstCheckpoint<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

async fn spawn_shard(
    shard_id: u32,
    data_dir: PathBuf,
    backup_dir: PathBuf,
    config: DistributedOntologyConfig,
) -> anyhow::Result<(SocketAddr, ShardNode, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shard = ShardNode::new_with_storage_paths(
        shard_id,
        config,
        StreamingTuning::from_profile(TuningProfile::Balanced),
        Some(data_dir),
        Some(backup_dir),
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

async fn spawn_fail_first_checkpoint_shard(
    shard_id: u32,
    data_dir: PathBuf,
    backup_dir: PathBuf,
    config: DistributedOntologyConfig,
) -> anyhow::Result<(SocketAddr, ShardNode, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shard = ShardNode::new_with_storage_paths(
        shard_id,
        config,
        StreamingTuning::from_profile(TuningProfile::Balanced),
        Some(data_dir),
        Some(backup_dir),
        false,
        None,
    )?;
    let service = proto::shard_service_server::ShardServiceServer::new(shard.clone());
    let service = FailFirstCheckpoint {
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

fn config() -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: vec![IdentityKeyConfig {
            name: "email_key".to_string(),
            attributes: vec!["email".to_string()],
        }],
        strong_identifiers: Vec::new(),
        constraints: Vec::new(),
    }
}

fn record(index: u32, email: &str) -> RecordInput {
    RecordInput {
        index,
        identity: Some(RecordIdentity {
            entity_type: "person".to_string(),
            perspective: "crm".to_string(),
            uid: "backup-source".to_string(),
        }),
        descriptors: vec![RecordDescriptor {
            attr: "email".to_string(),
            value: email.to_string(),
            start: 0,
            end: 100,
        }],
    }
}

async fn stop_shard(shard: ShardNode, handle: JoinHandle<()>) -> anyhow::Result<()> {
    shard.shutdown().await?;
    handle.abort();
    let _ = handle.await;
    drop(shard);
    sleep(Duration::from_millis(50)).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_checkpoint_restores_a_lost_shard_volume() -> anyhow::Result<()> {
    use proto::router_service_server::RouterService;

    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let data_volume = TempDir::new()?;
    let backup_volume = TempDir::new()?;
    let original_data = data_volume.path().join("original-shard");
    let replacement_data = data_volume.path().join("replacement-shard");
    let backup_root = backup_volume.path().join("shard-0");

    let (addr, shard, handle) =
        spawn_shard(0, original_data.clone(), backup_root.clone(), config()).await?;
    let mut client = ShardServiceClient::connect(format!("http://{addr}")).await?;
    client
        .ingest_records(IngestRecordsRequest {
            records: vec![record(0, "backup@example.com")],
            internal_protocol_version: 2,
        })
        .await?;
    let before = client.get_stats(StatsRequest {}).await?.into_inner();
    assert_eq!(before.record_count, 1);
    assert_eq!(before.cluster_count, 1);

    let router = RouterNode::connect(vec![format!("http://{addr}")], config()).await?;
    let checkpoint = RouterService::checkpoint(
        router.as_ref(),
        Request::new(CheckpointRequest {
            path: "snapshot-a".to_string(),
            checkpoint_protocol_version: 0,
            shard_count: 0,
            finalize: false,
        }),
    )
    .await?
    .into_inner();
    assert_eq!(checkpoint.paths.len(), 1);
    assert!(checkpoint.committed);
    assert_eq!(checkpoint.generation, "snapshot-a");
    let checkpoint_path = PathBuf::from(&checkpoint.paths[0]);
    assert!(checkpoint_path.starts_with(backup_root.canonicalize()?));
    assert!(checkpoint_path.join("CURRENT").is_file());

    drop(client);
    drop(router);
    stop_shard(shard, handle).await?;
    std::fs::remove_dir_all(&original_data)?;
    restore_checkpoint(&checkpoint_path, &replacement_data)?;

    let (addr, restored_shard, restored_handle) =
        spawn_shard(0, replacement_data, backup_root, config()).await?;
    let mut client = ShardServiceClient::connect(format!("http://{addr}")).await?;
    let after = client.get_stats(StatsRequest {}).await?.into_inner();
    assert_eq!(after.record_count, 1);
    assert_eq!(after.cluster_count, 1);

    client
        .ingest_records(IngestRecordsRequest {
            records: vec![record(1, "backup@example.com")],
            internal_protocol_version: 2,
        })
        .await?;
    let retry_stats = client.get_stats(StatsRequest {}).await?.into_inner();
    assert_eq!(retry_stats.record_count, 1);

    let error = client
        .ingest_records(IngestRecordsRequest {
            records: vec![record(2, "changed@example.com")],
            internal_protocol_version: 2,
        })
        .await
        .expect_err("restored immutable source identity must reject a changed payload");
    assert_eq!(error.code(), tonic::Code::AlreadyExists);

    drop(client);
    stop_shard(restored_shard, restored_handle).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinated_checkpoint_restores_source_reservations_across_shards() -> anyhow::Result<()> {
    use proto::router_service_server::RouterService;

    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let data_volume = TempDir::new()?;
    let backup_volume = TempDir::new()?;
    let cluster_config = config();
    let original_paths = (0..2)
        .map(|shard_id| data_volume.path().join(format!("original-{shard_id}")))
        .collect::<Vec<_>>();
    let replacement_paths = (0..2)
        .map(|shard_id| data_volume.path().join(format!("replacement-{shard_id}")))
        .collect::<Vec<_>>();
    let backup_roots = (0..2)
        .map(|shard_id| backup_volume.path().join(format!("shard-{shard_id}")))
        .collect::<Vec<_>>();

    let mut source_record = None;
    for candidate in 0..1_000 {
        let mut candidate_record = record(0, &format!("cluster-backup-{candidate}@example.com"));
        candidate_record.identity.as_mut().unwrap().uid = format!("cluster-source-{candidate}");
        let identity = candidate_record.identity.as_ref().unwrap();
        if hash_source_identity_to_shard(identity, 2)
            != hash_record_to_shard(&cluster_config, &candidate_record, 2)
        {
            source_record = Some(candidate_record);
            break;
        }
    }
    let source_record =
        source_record.expect("test must find distinct reservation owner and target shards");

    let mut shard_addrs = Vec::new();
    let mut shard_nodes = Vec::new();
    let mut shard_handles = Vec::new();
    for (shard_id, (data_path, backup_root)) in original_paths.iter().zip(&backup_roots).enumerate()
    {
        let (addr, shard, handle) = spawn_shard(
            shard_id as u32,
            data_path.clone(),
            backup_root.clone(),
            cluster_config.clone(),
        )
        .await?;
        shard_addrs.push(addr);
        shard_nodes.push(shard);
        shard_handles.push(handle);
    }
    let router = RouterNode::connect(
        shard_addrs
            .iter()
            .map(|addr| format!("http://{addr}"))
            .collect(),
        cluster_config.clone(),
    )
    .await?;

    RouterService::ingest_records(
        router.as_ref(),
        Request::new(IngestRecordsRequest {
            records: vec![source_record.clone()],
            internal_protocol_version: 0,
        }),
    )
    .await?;
    let checkpoint = RouterService::checkpoint(
        router.as_ref(),
        Request::new(CheckpointRequest {
            path: "coordinated".to_string(),
            checkpoint_protocol_version: 0,
            shard_count: 0,
            finalize: false,
        }),
    )
    .await?
    .into_inner();
    assert_eq!(checkpoint.paths.len(), 2);

    drop(router);
    for (shard, handle) in shard_nodes.into_iter().zip(shard_handles) {
        stop_shard(shard, handle).await?;
    }
    for path in &original_paths {
        std::fs::remove_dir_all(path)?;
    }
    for (checkpoint_path, replacement_path) in checkpoint.paths.iter().zip(&replacement_paths) {
        restore_checkpoint(std::path::Path::new(checkpoint_path), replacement_path)?;
    }

    let mut restarted_addrs = Vec::new();
    let mut restarted_nodes = Vec::new();
    let mut restarted_handles = Vec::new();
    for (shard_id, (replacement_path, backup_root)) in
        replacement_paths.iter().zip(&backup_roots).enumerate()
    {
        let (addr, shard, handle) = spawn_shard(
            shard_id as u32,
            replacement_path.clone(),
            backup_root.clone(),
            cluster_config.clone(),
        )
        .await?;
        restarted_addrs.push(addr);
        restarted_nodes.push(shard);
        restarted_handles.push(handle);
    }
    let router = RouterNode::connect(
        restarted_addrs
            .iter()
            .map(|addr| format!("http://{addr}"))
            .collect(),
        cluster_config,
    )
    .await?;
    let stats = RouterService::get_stats(router.as_ref(), Request::new(StatsRequest {}))
        .await?
        .into_inner();
    assert_eq!(stats.record_count, 1);

    let mut retry = source_record.clone();
    retry.index = 1;
    RouterService::ingest_records(
        router.as_ref(),
        Request::new(IngestRecordsRequest {
            records: vec![retry],
            internal_protocol_version: 0,
        }),
    )
    .await?;
    let stats = RouterService::get_stats(router.as_ref(), Request::new(StatsRequest {}))
        .await?
        .into_inner();
    assert_eq!(stats.record_count, 1);

    let mut changed = source_record;
    changed.index = 2;
    changed.descriptors[0].value = "changed-after-cluster-restore@example.com".to_string();
    let error = RouterService::ingest_records(
        router.as_ref(),
        Request::new(IngestRecordsRequest {
            records: vec![changed],
            internal_protocol_version: 0,
        }),
    )
    .await
    .expect_err("restored cluster reservation must reject a changed payload");
    assert_eq!(error.code(), tonic::Code::AlreadyExists);

    drop(router);
    for (shard, handle) in restarted_nodes.into_iter().zip(restarted_handles) {
        stop_shard(shard, handle).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_checkpoint_is_not_restorable_and_same_generation_can_retry() -> anyhow::Result<()>
{
    use proto::router_service_server::RouterService;

    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let data_volume = TempDir::new()?;
    let backup_volume = TempDir::new()?;
    let backup_roots = (0..2)
        .map(|shard_id| backup_volume.path().join(format!("shard-{shard_id}")))
        .collect::<Vec<_>>();

    let (addr0, shard0, handle0) = spawn_shard(
        0,
        data_volume.path().join("shard-0"),
        backup_roots[0].clone(),
        config(),
    )
    .await?;
    let (addr1, shard1, handle1) = spawn_fail_first_checkpoint_shard(
        1,
        data_volume.path().join("shard-1"),
        backup_roots[1].clone(),
        config(),
    )
    .await?;
    let router = RouterNode::connect(
        vec![format!("http://{addr0}"), format!("http://{addr1}")],
        config(),
    )
    .await?;
    let request = CheckpointRequest {
        path: "retryable-generation".to_string(),
        checkpoint_protocol_version: 0,
        shard_count: 0,
        finalize: false,
    };

    let error = RouterService::checkpoint(router.as_ref(), Request::new(request.clone()))
        .await
        .expect_err("injected second-shard failure must fail the cluster checkpoint");
    assert_eq!(error.code(), tonic::Code::Unavailable);

    let partial = backup_roots[0].join("retryable-generation");
    assert!(partial.join("CURRENT").is_file());
    assert!(read_cluster_checkpoint_manifest(&partial).is_err());
    let partial_restore = data_volume.path().join("partial-restore");
    restore_checkpoint(&partial, &partial_restore)
        .expect_err("an uncommitted shard checkpoint must not be restorable");
    assert!(!partial_restore.exists());

    let completed = RouterService::checkpoint(router.as_ref(), Request::new(request))
        .await?
        .into_inner();
    assert!(completed.committed);
    assert_eq!(completed.generation, "retryable-generation");
    assert_eq!(completed.paths.len(), 2);

    for (shard_id, checkpoint_path) in completed.paths.iter().enumerate() {
        let checkpoint_path = std::path::Path::new(checkpoint_path);
        let manifest = read_cluster_checkpoint_manifest(checkpoint_path)?;
        assert_eq!(manifest.generation(), "retryable-generation");
        assert_eq!(manifest.shard_id(), shard_id as u32);
        assert_eq!(manifest.shard_count(), 2);
        restore_checkpoint_for_shard(
            checkpoint_path,
            &data_volume.path().join(format!("restored-{shard_id}")),
            Some(shard_id as u32),
        )?;
    }

    drop(router);
    stop_shard(shard0, handle0).await?;
    stop_shard(shard1, handle1).await?;
    Ok(())
}
