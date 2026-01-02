use std::net::SocketAddr;

use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, shard_service_client::ShardServiceClient, ApplyOntologyRequest, IngestRecordsChunk,
    QueryDescriptor, QueryEntitiesRequest, RecordDescriptor, RecordIdentity as ProtoRecordIdentity,
    RecordInput,
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
async fn shard_stream_ingest_accepts_chunks() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();
    let (shard_addr, _handle) = spawn_shard(0, empty_config.clone()).await?;

    let mut client = ShardServiceClient::connect(format!("http://{}", shard_addr)).await?;
    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    let (tx, rx) = tokio::sync::mpsc::channel::<IngestRecordsChunk>(4);
    let mut ingest_client = client.clone();
    let ingest_task = tokio::spawn(async move {
        ingest_client
            .ingest_records_stream(ReceiverStream::new(rx))
            .await
            .map(|response| response.into_inner())
            .map_err(|err| anyhow::anyhow!(err.to_string()))
    });

    tx.send(IngestRecordsChunk {
        records: vec![record_input(
            0,
            "person",
            "crm",
            "crm_001",
            vec![("email", "alice@example.com", 0, 10)],
        )],
    })
    .await?;
    tx.send(IngestRecordsChunk {
        records: vec![record_input(
            1,
            "person",
            "crm",
            "crm_002",
            vec![("email", "bob@example.com", 0, 10)],
        )],
    })
    .await?;
    drop(tx);

    let response = ingest_task.await??;
    assert_eq!(response.assignments.len(), 2);

    let query = QueryEntitiesRequest {
        descriptors: vec![QueryDescriptor {
            attr: "email".to_string(),
            value: "alice@example.com".to_string(),
        }],
        start: 0,
        end: 10,
    };
    let result = client.query_entities(query).await?.into_inner();
    assert!(result.outcome.is_some());

    Ok(())
}
