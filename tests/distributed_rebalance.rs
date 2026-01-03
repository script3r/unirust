use std::net::SocketAddr;

use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, shard_service_client::ShardServiceClient, ApplyOntologyRequest, ExportRecordsRequest,
    ImportRecordsRequest, IngestRecordsRequest, RecordDescriptor, RecordIdRangeRequest,
    RecordIdentity as ProtoRecordIdentity, RecordInput,
};
use unirust_rs::distributed::{DistributedOntologyConfig, ShardNode};
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

fn record_input(
    index: u32,
    entity_type: &str,
    perspective: &str,
    uid: &str,
    descriptors: Vec<(&str, &str, i64, i64)>,
) -> RecordInput {
    RecordInput {
        index,
        identity: Some(ProtoRecordIdentity {
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
async fn distributed_export_import_range() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;

    let mut shard0 = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    let mut shard1 = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;

    shard0
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;
    shard1
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    let records = vec![
        record_input(
            0,
            "person",
            "crm",
            "crm_001",
            vec![("email", "alice@example.com", 0, 10)],
        ),
        record_input(
            1,
            "person",
            "crm",
            "crm_002",
            vec![("email", "bob@example.com", 0, 10)],
        ),
        record_input(
            2,
            "person",
            "crm",
            "crm_003",
            vec![("email", "carol@example.com", 0, 10)],
        ),
    ];

    let ingest_response = shard0
        .ingest_records(IngestRecordsRequest { records })
        .await?
        .into_inner();
    assert_eq!(ingest_response.assignments.len(), 3);

    let record_ids: Vec<u32> = ingest_response
        .assignments
        .iter()
        .map(|assignment| assignment.record_id)
        .collect();

    let range = shard0
        .get_record_id_range(RecordIdRangeRequest {})
        .await?
        .into_inner();
    assert!(!range.empty);
    assert_eq!(range.record_count, 3);

    let mut all_records = Vec::new();
    let mut next_start = 0u32;
    loop {
        let response = shard0
            .export_records(ExportRecordsRequest {
                start_id: next_start,
                end_id: 0,
                limit: 2,
            })
            .await?
            .into_inner();
        all_records.extend(response.records);
        if !response.has_more {
            break;
        }
        next_start = response.next_start_id;
        assert!(next_start > 0);
    }

    assert_eq!(all_records.len(), 3);
    for record_id in &record_ids {
        assert!(all_records
            .iter()
            .any(|record| record.record_id == *record_id));
    }

    shard1
        .import_records(ImportRecordsRequest {
            records: all_records,
        })
        .await?;

    let range = shard1
        .get_record_id_range(RecordIdRangeRequest {})
        .await?
        .into_inner();
    assert_eq!(range.record_count, 3);

    let exported = shard1
        .export_records(ExportRecordsRequest {
            start_id: 0,
            end_id: 0,
            limit: 10,
        })
        .await?
        .into_inner();
    let exported_ids: Vec<u32> = exported
        .records
        .iter()
        .map(|record| record.record_id)
        .collect();
    for record_id in record_ids {
        assert!(exported_ids.contains(&record_id));
    }

    Ok(())
}
