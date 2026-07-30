use std::time::Duration;

use tempfile::tempdir;
use tonic::Request;
use unirust_rs::distributed::proto::shard_service_server::ShardService;
use unirust_rs::distributed::proto::{
    HealthCheckRequest, IngestRecordsRequest, RecordDescriptor, RecordIdentity, RecordInput,
    StatsRequest,
};
use unirust_rs::distributed::{DistributedOntologyConfig, ShardNode, DISTRIBUTED_PROTOCOL_VERSION};
use unirust_rs::StreamingTuning;

fn records(count: u32) -> Vec<RecordInput> {
    (0..count)
        .map(|index| RecordInput {
            index,
            identity: Some(RecordIdentity {
                entity_type: "person".to_string(),
                perspective: "crm".to_string(),
                uid: format!("cancel-{index}"),
            }),
            descriptors: vec![RecordDescriptor {
                attr: "email".to_string(),
                value: format!("cancel-{index}@example.com"),
                start: 0,
                end: 10,
            }],
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_ingest_finishes_commit_and_survives_restart() -> anyhow::Result<()> {
    const RECORD_COUNT: u32 = 5_000;

    let temp_dir = tempdir()?;
    let shard = ShardNode::new_with_data_dir(
        0,
        DistributedOntologyConfig::empty(),
        StreamingTuning::balanced(),
        Some(temp_dir.path().to_path_buf()),
        false,
        None,
    )?;
    let request_records = records(RECORD_COUNT);
    let service = shard.clone();
    let request_task = tokio::spawn({
        let request_records = request_records.clone();
        async move {
            ShardService::ingest_records(
                &service,
                Request::new(IngestRecordsRequest {
                    internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
                    records: request_records,
                }),
            )
            .await
        }
    });

    let wal_path = temp_dir.path().join("ingest_wal.bin");
    tokio::time::timeout(Duration::from_secs(10), async {
        while !wal_path.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    request_task.abort();
    assert!(request_task
        .await
        .expect_err("request task must be cancelled")
        .is_cancelled());

    tokio::time::timeout(Duration::from_secs(30), async {
        while wal_path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    let retry = ShardService::ingest_records(
        &shard,
        Request::new(IngestRecordsRequest {
            internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
            records: request_records,
        }),
    )
    .await?
    .into_inner();
    assert_eq!(retry.assignments.len(), RECORD_COUNT as usize);
    let stats = ShardService::get_stats(&shard, Request::new(StatsRequest {}))
        .await?
        .into_inner();
    assert_eq!(stats.record_count, u64::from(RECORD_COUNT));
    ShardService::health_check(&shard, Request::new(HealthCheckRequest {})).await?;

    shard.shutdown().await?;
    drop(shard);

    let restarted = ShardNode::new_with_data_dir(
        0,
        DistributedOntologyConfig::empty(),
        StreamingTuning::balanced(),
        Some(temp_dir.path().to_path_buf()),
        false,
        None,
    )?;
    let stats = ShardService::get_stats(&restarted, Request::new(StatsRequest {}))
        .await?
        .into_inner();
    assert_eq!(stats.record_count, u64::from(RECORD_COUNT));
    ShardService::health_check(&restarted, Request::new(HealthCheckRequest {})).await?;
    restarted.shutdown().await?;
    Ok(())
}
