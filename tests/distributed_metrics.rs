use std::net::SocketAddr;

use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient,
    shard_service_client::ShardServiceClient, ApplyOntologyRequest, IngestRecordsRequest,
    MetricsRequest, QueryDescriptor, QueryEntitiesRequest, RecordDescriptor,
    RecordIdentity as ProtoRecordIdentity, RecordInput,
};
use unirust_rs::distributed::{DistributedOntologyConfig, RouterNode, ShardNode};
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
async fn metrics_report_requests() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard_addr, _shard_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard_addr], empty_config.clone()).await?;

    let mut router = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    router
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    router
        .ingest_records(IngestRecordsRequest {
            records: vec![record_input(
                0,
                "person",
                "crm",
                "crm_001",
                vec![("email", "alice@example.com", 0, 10)],
            )],
        })
        .await?;

    router
        .query_entities(QueryEntitiesRequest {
            descriptors: vec![QueryDescriptor {
                attr: "email".to_string(),
                value: "alice@example.com".to_string(),
            }],
            start: 0,
            end: 10,
        })
        .await?;

    let router_metrics = router
        .get_metrics(MetricsRequest {})
        .await?
        .into_inner();
    assert!(router_metrics.ingest_requests >= 1);
    assert!(router_metrics.query_requests >= 1);
    assert_eq!(router_metrics.shards_reporting, 1);

    let mut shard = ShardServiceClient::connect(format!("http://{}", shard_addr)).await?;
    let shard_metrics = shard
        .get_metrics(MetricsRequest {})
        .await?
        .into_inner();
    assert!(shard_metrics.ingest_requests >= 1);
    assert!(shard_metrics.query_requests >= 1);
    assert_eq!(shard_metrics.shards_reporting, 1);

    Ok(())
}
