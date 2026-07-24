use std::net::SocketAddr;

use tempfile::tempdir;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, ApplyOntologyRequest, IngestRecordsRequest,
    RecordDescriptor, RecordIdentity as ProtoRecordIdentity, RecordInput,
    RouterExportRecordsRequest, RouterImportRecordsRequest, RouterRecordIdRangeRequest,
};
use unirust_rs::distributed::{
    hash_record_to_shard, DistributedOntologyConfig, RouterNode, ShardNode,
};
use unirust_rs::{StreamingTuning, TuningProfile};

mod support;

async fn spawn_shard(
    shard_id: u32,
    config: DistributedOntologyConfig,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let data_dir = tempdir()?;
    let shard = ShardNode::new_with_data_dir(
        shard_id,
        config,
        StreamingTuning::from_profile(TuningProfile::Balanced),
        Some(data_dir.path().to_path_buf()),
        false,
        None,
    )?;
    let handle = tokio::spawn(async move {
        let _data_dir = data_dir;
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
async fn router_admin_rejects_non_atomic_cross_shard_copy() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;

    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut router = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    router
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    let mut records = Vec::new();
    for candidate in 0..1_000 {
        let record = record_input(
            candidate,
            "person",
            "crm",
            &format!("admin-copy-{candidate}"),
            vec![(
                "email",
                &format!("admin-copy-{candidate}@example.com"),
                0,
                10,
            )],
        );
        if hash_record_to_shard(&config, &record, 2) == 0 {
            records.push(record);
        }
        if records.len() == 2 {
            break;
        }
    }
    assert_eq!(records.len(), 2);
    router
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 3,
            records,
        })
        .await?;

    let range = router
        .get_record_id_range(RouterRecordIdRangeRequest { shard_id: 0 })
        .await?
        .into_inner();
    assert!(!range.empty);
    assert_eq!(range.record_count, 2);

    let export = router
        .export_records(RouterExportRecordsRequest {
            shard_id: 0,
            start_id: 0,
            end_id: 0,
            limit: 10,
        })
        .await?
        .into_inner();
    assert_eq!(export.records.len(), 2);

    let error = router
        .import_records(RouterImportRecordsRequest {
            shard_id: 1,
            records: export.records,
        })
        .await
        .expect_err("copying a reserved source identity to another shard must fail");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let range = router
        .get_record_id_range(RouterRecordIdRangeRequest { shard_id: 1 })
        .await?
        .into_inner();
    assert!(range.empty);
    assert_eq!(range.record_count, 0);

    Ok(())
}
