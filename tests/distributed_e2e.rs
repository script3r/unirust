use std::net::SocketAddr;

use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, ApplyOntologyRequest, IngestRecordsRequest,
    RecordDescriptor, RecordIdentity as ProtoRecordIdentity, RecordInput,
    StatsRequest,
};
use unirust_rs::distributed::{
    hash_record_to_shard, DistributedOntologyConfig, RouterNode, ShardNode,
};
use unirust_rs::{
    QueryDescriptor, QueryOutcome, Record, RecordId, RecordIdentity, StreamingTuning,
    TuningProfile, Unirust,
};

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

fn build_local_record(unirust: &mut Unirust, input: &RecordInput) -> anyhow::Result<Record> {
    let identity = input.identity.as_ref().expect("identity");
    let descriptors = input
        .descriptors
        .iter()
        .map(|descriptor| {
            let attr = unirust.intern_attr(&descriptor.attr);
            let value = unirust.intern_value(&descriptor.value);
            let interval = unirust_rs::Interval::new(descriptor.start, descriptor.end)?;
            Ok(unirust_rs::Descriptor::new(attr, value, interval))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(Record::new(
        RecordId(0),
        RecordIdentity::new(
            identity.entity_type.clone(),
            identity.perspective.clone(),
            identity.uid.clone(),
        ),
        descriptors,
    ))
}

fn normalize_local_query(outcome: QueryOutcome) -> (&'static str, usize, (i64, i64)) {
    match outcome {
        QueryOutcome::Matches(matches) => {
            let interval = matches
                .first()
                .map(|entry| (entry.interval.start, entry.interval.end))
                .unwrap_or((0, 0));
            ("matches", matches.len(), interval)
        }
        QueryOutcome::Conflict(conflict) => (
            "conflict",
            conflict.clusters.len(),
            (conflict.interval.start, conflict.interval.end),
        ),
    }
}

fn normalize_remote_query(
    response: proto::QueryEntitiesResponse,
) -> (&'static str, usize, (i64, i64)) {
    match response.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => {
            let interval = matches
                .matches
                .first()
                .map(|entry| (entry.start, entry.end))
                .unwrap_or((0, 0));
            ("matches", matches.matches.len(), interval)
        }
        Some(proto::query_entities_response::Outcome::Conflict(conflict)) => (
            "conflict",
            conflict.clusters.len(),
            (conflict.start, conflict.end),
        ),
        None => ("matches", 0, (0, 0)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_stream_and_query() -> anyhow::Result<()> {
    let config = support::build_iam_config();
    let empty_config = DistributedOntologyConfig::empty();

    let (shard0_addr, _shard0_handle) = spawn_shard(0, empty_config.clone()).await?;
    let (shard1_addr, _shard1_handle) = spawn_shard(1, empty_config.clone()).await?;

    let (router_addr, _router_handle) =
        spawn_router(vec![shard0_addr, shard1_addr], empty_config.clone()).await?;

    let mut client = RouterServiceClient::connect(format!("http://{}", router_addr)).await?;
    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(support::to_proto_config(&config)),
        })
        .await?;

    let health = client
        .health_check(proto::HealthCheckRequest {})
        .await?
        .into_inner();
    assert_eq!(health.status, "ok");

    let record_a = record_input(
        0,
        "person",
        "crm",
        "crm_001",
        vec![
            ("email", "alice@example.com", 0, 10),
            ("role", "admin", 0, 10),
        ],
    );
    let record_b = record_input(
        1,
        "person",
        "crm",
        "crm_002",
        vec![
            ("email", "bob@example.com", 0, 10),
            ("role", "admin", 0, 10),
        ],
    );

    let expected_shard_a = hash_record_to_shard(&config, &record_a, 2) as u32;
    let expected_shard_b = hash_record_to_shard(&config, &record_b, 2) as u32;

    let response = client
        .ingest_records(IngestRecordsRequest {
            records: vec![record_a.clone(), record_b.clone()],
        })
        .await?
        .into_inner();

    assert_eq!(response.assignments.len(), 2);
    let shard_ids: Vec<u32> = response.assignments.iter().map(|a| a.shard_id).collect();
    assert!(shard_ids.contains(&expected_shard_a));
    assert!(shard_ids.contains(&expected_shard_b));

    let query = proto::QueryEntitiesRequest {
        descriptors: vec![proto::QueryDescriptor {
            attr: "role".to_string(),
            value: "admin".to_string(),
        }],
        start: 0,
        end: 10,
    };
    let query_response = client.query_entities(query.clone()).await?.into_inner();
    let remote_summary = normalize_remote_query(query_response);
    assert_eq!(remote_summary.0, "conflict");
    assert_eq!(remote_summary.1, 2);
    assert_eq!(remote_summary.2, (0, 10));

    let query = proto::QueryEntitiesRequest {
        descriptors: vec![proto::QueryDescriptor {
            attr: "email".to_string(),
            value: "alice@example.com".to_string(),
        }],
        start: 0,
        end: 10,
    };
    let query_response = client.query_entities(query.clone()).await?.into_inner();
    let remote_summary = normalize_remote_query(query_response.clone());
    assert_eq!(remote_summary.0, "matches");
    assert_eq!(remote_summary.1, 1);
    assert_eq!(remote_summary.2, (0, 10));

    let mut local_store = unirust_rs::Store::new();
    let local_ontology = config.build_ontology(&mut local_store);
    let mut local = Unirust::with_store_and_tuning(
        local_ontology,
        local_store,
        StreamingTuning::from_profile(TuningProfile::Balanced),
    );

    let local_record_a = build_local_record(&mut local, &record_a)?;
    let local_record_b = build_local_record(&mut local, &record_b)?;
    local.stream_record_update_graph(local_record_a)?;
    local.stream_record_update_graph(local_record_b)?;

    let role_attr = local.intern_attr("role");
    let role_admin = local.intern_value("admin");
    let role_outcome = local.query_master_entities(
        &[QueryDescriptor {
            attr: role_attr,
            value: role_admin,
        }],
        unirust_rs::Interval::new(0, 10)?,
    )?;
    let local_summary = normalize_local_query(role_outcome);
    assert_eq!(local_summary, ("conflict", 2, (0, 10)));

    let email_attr = local.intern_attr("email");
    let email_value = local.intern_value("alice@example.com");
    let email_outcome = local.query_master_entities(
        &[QueryDescriptor {
            attr: email_attr,
            value: email_value,
        }],
        unirust_rs::Interval::new(0, 10)?,
    )?;
    let local_summary = normalize_local_query(email_outcome);
    assert_eq!(local_summary, ("matches", 1, (0, 10)));

    let remote_golden = match query_response.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => {
            matches.matches[0].golden.clone()
        }
        _ => Vec::new(),
    };
    let local_golden = match local.query_master_entities(
        &[QueryDescriptor {
            attr: email_attr,
            value: email_value,
        }],
        unirust_rs::Interval::new(0, 10)?,
    )? {
        QueryOutcome::Matches(matches) => matches[0]
            .golden
            .iter()
            .map(|descriptor| {
                (
                    descriptor.attr.clone(),
                    descriptor.value.clone(),
                    descriptor.interval.start,
                    descriptor.interval.end,
                )
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };

    let mut remote_golden_flat = remote_golden
        .into_iter()
        .map(|descriptor| {
            (
                descriptor.attr,
                descriptor.value,
                descriptor.start,
                descriptor.end,
            )
        })
        .collect::<Vec<_>>();
    remote_golden_flat.sort();
    let mut local_golden_flat = local_golden;
    local_golden_flat.sort();
    assert_eq!(remote_golden_flat, local_golden_flat);

    let stats = client.get_stats(StatsRequest {}).await?.into_inner();
    assert!(stats.record_count >= 2);
    assert!(stats.cluster_count >= 2);

    Ok(())
}
