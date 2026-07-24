use std::net::SocketAddr;
use std::path::PathBuf;

use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::TcpListenerStream;
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
use unirust_rs::{restore_checkpoint, StreamingTuning, TuningProfile};

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

    let checkpoint = client
        .checkpoint(CheckpointRequest {
            path: "snapshot-a".to_string(),
        })
        .await?
        .into_inner();
    assert_eq!(checkpoint.paths.len(), 1);
    let checkpoint_path = PathBuf::from(&checkpoint.paths[0]);
    assert!(checkpoint_path.starts_with(backup_root.canonicalize()?));
    assert!(checkpoint_path.join("CURRENT").is_file());

    drop(client);
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
