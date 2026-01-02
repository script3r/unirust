use std::net::SocketAddr;

use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient,
    shard_service_client::ShardServiceClient, ApplyOntologyRequest, IngestRecordsRequest,
    RecordDescriptor, RecordIdentity as ProtoRecordIdentity, RecordInput,
    RouterExportRecordsRequest, RouterImportRecordsRequest,
};
use unirust_rs::distributed::{DistributedOntologyConfig, RouterNode, ShardNode};
use unirust_rs::{StreamingTuning, TuningProfile};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

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
async fn distributed_rebalance_stream_via_router() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;

    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut shard0 = ShardServiceClient::connect(format!("http://{}", shard0_addr)).await?;
    shard0
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;
    let mut shard1 = ShardServiceClient::connect(format!("http://{}", shard1_addr)).await?;
    shard1
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    shard0
        .ingest_records(IngestRecordsRequest {
            records: vec![
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
            ],
        })
        .await?;

    let mut router = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    let (tx, rx) = tokio::sync::mpsc::channel::<RouterImportRecordsRequest>(4);
    let import_task = tokio::spawn(async move {
        router
            .import_records_stream(ReceiverStream::new(rx))
            .await
            .map(|response| response.into_inner())
            .map_err(|err| anyhow::anyhow!(err.to_string()))
    });

    let mut export_stream = RouterServiceClient::connect(format!("http://{}", router_addr))
        .await?
        .export_records_stream(RouterExportRecordsRequest {
            shard_id: 0,
            start_id: 0,
            end_id: 0,
            limit: 1,
        })
        .await?
        .into_inner();

    while let Some(chunk) = export_stream.next().await {
        let chunk = chunk?;
        if chunk.records.is_empty() {
            continue;
        }
        tx.send(RouterImportRecordsRequest {
            shard_id: 1,
            records: chunk.records,
        })
        .await
        .map_err(|_| anyhow::anyhow!("import stream closed"))?;
    }
    drop(tx);

    let response = import_task.await??;
    assert_eq!(response.imported, 2);

    let range = shard1
        .get_record_id_range(proto::RecordIdRangeRequest {})
        .await?
        .into_inner();
    assert_eq!(range.record_count, 2);

    Ok(())
}
