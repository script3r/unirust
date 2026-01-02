use std::net::SocketAddr;

use serde_json::json;
use tempfile::tempdir;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{self, shard_service_client::ShardServiceClient, StatsRequest};
use unirust_rs::distributed::ShardNode;
use unirust_rs::{StreamingTuning, TuningProfile};

mod support;

async fn spawn_shard_with_data_dir(
    shard_id: u32,
    data_dir: std::path::PathBuf,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shard = ShardNode::new_with_data_dir(
        shard_id,
        support::build_iam_config(),
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

#[tokio::test]
async fn shard_replays_ingest_wal_on_start() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("ingest_wal.json");
    let payload = json!([{
        "index": 0,
        "identity": {
            "entity_type": "person",
            "perspective": "crm",
            "uid": "wal-1"
        },
        "descriptors": [
            { "attr": "email", "value": "wal@example.com", "start": 0, "end": 10 },
            { "attr": "ssn", "value": "123-45-6789", "start": 0, "end": 10 }
        ]
    }]);
    std::fs::write(&wal_path, serde_json::to_vec(&payload)?)?;

    let (addr, handle) = spawn_shard_with_data_dir(0, dir.path().to_path_buf()).await?;
    let mut client = ShardServiceClient::connect(format!("http://{}", addr)).await?;
    let stats = client
        .get_stats(StatsRequest {})
        .await?
        .into_inner();

    assert_eq!(stats.record_count, 1);
    assert!(!wal_path.exists());

    handle.abort();
    Ok(())
}
