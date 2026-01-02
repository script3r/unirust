use std::net::SocketAddr;

use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, ApplyOntologyRequest, IngestRecordsRequest,
    QueryDescriptor, QueryEntitiesRequest, RecordDescriptor,
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
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_ontology_enables_queries_distributed() -> anyhow::Result<()> {
    let empty = DistributedOntologyConfig::empty();
    let config = support::build_iam_config();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty.clone()).await?;
    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty.clone()).await?;

    let mut client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;

    let query = QueryEntitiesRequest {
        descriptors: vec![QueryDescriptor {
            attr: "email".to_string(),
            value: "alice@example.com".to_string(),
        }],
        start: 0,
        end: 10,
    };
    let response = client.query_entities(query.clone()).await?.into_inner();
    match response.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => {
            assert!(matches.matches.is_empty());
        }
        _ => anyhow::bail!("expected empty matches before apply"),
    }

    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    let record = RecordInput {
        index: 0,
        identity: Some(ProtoRecordIdentity {
            entity_type: "person".to_string(),
            perspective: "crm".to_string(),
            uid: "crm_001".to_string(),
        }),
        descriptors: vec![RecordDescriptor {
            attr: "email".to_string(),
            value: "alice@example.com".to_string(),
            start: 0,
            end: 10,
        }],
    };

    client
        .ingest_records(IngestRecordsRequest {
            records: vec![record],
        })
        .await?;

    let response = client.query_entities(query).await?.into_inner();
    match response.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => {
            assert_eq!(matches.matches.len(), 1);
        }
        _ => anyhow::bail!("expected match after apply"),
    }

    Ok(())
}
