use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tempfile::tempdir;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, shard_service_client::ShardServiceClient, StatsRequest,
};
use unirust_rs::distributed::ShardNode;
use unirust_rs::{
    Descriptor, Interval, PersistentStore, Record, RecordId, RecordIdentity, RecordStore,
    StreamingTuning, TuningProfile,
};

mod support;

/// WAL record format for binary serialization (matches distributed.rs).
#[derive(Debug, Deserialize, Serialize)]
struct WalRecordIdentity {
    entity_type: String,
    perspective: String,
    uid: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct WalRecordDescriptor {
    attr: String,
    value: String,
    start: i64,
    end: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct WalRecordInput {
    index: u32,
    identity: WalRecordIdentity,
    descriptors: Vec<WalRecordDescriptor>,
}

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

fn build_record(
    store: &mut PersistentStore,
    entity_type: &str,
    perspective: &str,
    uid: &str,
    email: &str,
    ssn: &str,
) -> Record {
    let email_attr = store.intern_attr("email");
    let ssn_attr = store.intern_attr("ssn");
    let email_value = store.intern_value(email);
    let ssn_value = store.intern_value(ssn);
    let interval = Interval::new(0, 10).expect("interval");
    let identity = RecordIdentity::new(
        entity_type.to_string(),
        perspective.to_string(),
        uid.to_string(),
    );
    let descriptors = vec![
        Descriptor::new(email_attr, email_value, interval),
        Descriptor::new(ssn_attr, ssn_value, interval),
    ];
    Record::new(RecordId(0), identity, descriptors)
}

#[tokio::test]
async fn wal_replay_skips_duplicate_records() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    let mut store = PersistentStore::open(&data_dir)?;
    let first = build_record(
        &mut store,
        "person",
        "crm",
        "wal-1",
        "wal@example.com",
        "123-45-6789",
    );
    store.add_record(first)?;
    drop(store);

    let wal_path = data_dir.join("ingest_wal.bin");
    let payload: Vec<WalRecordInput> = vec![
        WalRecordInput {
            index: 0,
            identity: WalRecordIdentity {
                entity_type: "person".to_string(),
                perspective: "crm".to_string(),
                uid: "wal-1".to_string(),
            },
            descriptors: vec![
                WalRecordDescriptor {
                    attr: "email".to_string(),
                    value: "wal@example.com".to_string(),
                    start: 0,
                    end: 10,
                },
                WalRecordDescriptor {
                    attr: "ssn".to_string(),
                    value: "123-45-6789".to_string(),
                    start: 0,
                    end: 10,
                },
            ],
        },
        WalRecordInput {
            index: 1,
            identity: WalRecordIdentity {
                entity_type: "person".to_string(),
                perspective: "crm".to_string(),
                uid: "wal-2".to_string(),
            },
            descriptors: vec![
                WalRecordDescriptor {
                    attr: "email".to_string(),
                    value: "wal2@example.com".to_string(),
                    start: 0,
                    end: 10,
                },
                WalRecordDescriptor {
                    attr: "ssn".to_string(),
                    value: "987-65-4321".to_string(),
                    start: 0,
                    end: 10,
                },
            ],
        },
    ];
    std::fs::write(&wal_path, bincode::serialize(&payload)?)?;

    let (addr, handle) = spawn_shard_with_data_dir(0, data_dir).await?;
    let mut client = ShardServiceClient::connect(format!("http://{}", addr)).await?;
    let stats = client.get_stats(StatsRequest {}).await?.into_inner();

    assert_eq!(stats.record_count, 2);
    assert!(!wal_path.exists());

    handle.abort();
    Ok(())
}

#[tokio::test]
async fn wal_replay_quarantines_corrupt_file() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir)?;

    let wal_path = data_dir.join("ingest_wal.bin");
    std::fs::write(&wal_path, b"\x00\xff\x00corrupt")?;

    let (addr, handle) = spawn_shard_with_data_dir(0, data_dir.clone()).await?;
    let mut client = ShardServiceClient::connect(format!("http://{}", addr)).await?;
    let stats = client.get_stats(StatsRequest {}).await?.into_inner();

    assert_eq!(stats.record_count, 0);
    assert!(!wal_path.exists());

    let corrupt_found = std::fs::read_dir(&data_dir)?
        .filter_map(Result::ok)
        .any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("ingest_wal.bin.corrupt")
        });
    assert!(corrupt_found);

    handle.abort();
    Ok(())
}
