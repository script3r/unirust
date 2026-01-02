use std::collections::BTreeSet;
use std::net::SocketAddr;

use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, ApplyOntologyRequest,
    IngestRecordsRequest, RecordDescriptor, RecordIdentity, RecordInput,
};
use unirust_rs::distributed::{
    DistributedOntologyConfig, IdentityKeyConfig, RouterNode, ShardNode,
};
use unirust_rs::{
    Record, RecordId, RecordIdentity as LocalRecordIdentity, StreamingTuning, TuningProfile,
    Unirust,
};

mod support;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Preset {
    id: String,
    identity_keys: Vec<String>,
    conflict_attrs: Vec<String>,
    records: Vec<PresetRecord>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PresetRecord {
    uid: String,
    entity_type: String,
    perspective: String,
    descriptors: Vec<PresetDescriptor>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PresetDescriptor {
    attr: String,
    value: String,
    start: i64,
    end: i64,
}

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

fn build_record_inputs(records: &[PresetRecord]) -> Vec<RecordInput> {
    records
        .iter()
        .enumerate()
        .map(|(idx, record)| RecordInput {
            index: idx as u32,
            identity: Some(RecordIdentity {
                entity_type: record.entity_type.clone(),
                perspective: record.perspective.clone(),
                uid: record.uid.clone(),
            }),
            descriptors: record
                .descriptors
                .iter()
                .map(|descriptor| RecordDescriptor {
                    attr: descriptor.attr.clone(),
                    value: descriptor.value.clone(),
                    start: descriptor.start,
                    end: descriptor.end,
                })
                .collect(),
        })
        .collect()
}

fn build_local_record(unirust: &mut Unirust, record: &PresetRecord) -> anyhow::Result<Record> {
    let descriptors = record
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
        LocalRecordIdentity::new(
            record.entity_type.clone(),
            record.perspective.clone(),
            record.uid.clone(),
        ),
        descriptors,
    ))
}

fn build_config_from_presets(presets: &[Preset]) -> DistributedOntologyConfig {
    let mut identity_sets = BTreeSet::new();
    let mut conflict_attrs = BTreeSet::new();

    for preset in presets {
        if !preset.identity_keys.is_empty() {
            identity_sets.insert(preset.identity_keys.clone());
        }
        for attr in &preset.conflict_attrs {
            if !attr.trim().is_empty() {
                conflict_attrs.insert(attr.clone());
            }
        }
    }

    let identity_keys = identity_sets
        .into_iter()
        .map(|attrs| IdentityKeyConfig {
            name: attrs.join("_"),
            attributes: attrs,
        })
        .collect();

    let constraints = conflict_attrs
        .into_iter()
        .map(|attr| unirust_rs::distributed::ConstraintConfig {
            name: format!("unique_{attr}"),
            attribute: attr,
            kind: unirust_rs::distributed::ConstraintKind::Unique,
        })
        .collect();

    DistributedOntologyConfig {
        identity_keys,
        strong_identifiers: Vec::new(),
        constraints,
    }
}

fn normalize_local_conflicts(unirust: &Unirust) -> Vec<String> {
    let clusters = unirust.build_clusters().expect("clusters");
    let observations = unirust.detect_conflicts(&clusters).expect("conflicts");
    let summaries = unirust.summarize_conflicts(&observations);
    summaries
        .into_iter()
        .map(|summary| {
            let mut records = summary
                .records
                .iter()
                .map(|record| format!("{}:{}", record.perspective, record.uid))
                .collect::<Vec<_>>();
            records.sort();
            format!(
                "{}|{}|{}|{}|{}|{}",
                summary.kind,
                summary.attribute.unwrap_or_default(),
                summary.interval.start,
                summary.interval.end,
                summary.cause.unwrap_or_default(),
                records.join(",")
            )
        })
        .collect()
}

fn normalize_remote_conflicts(response: proto::ListConflictsResponse) -> Vec<String> {
    response
        .conflicts
        .into_iter()
        .map(|summary| {
            let mut records = summary
                .records
                .iter()
                .map(|record| format!("{}:{}", record.perspective, record.uid))
                .collect::<Vec<_>>();
            records.sort();
            format!(
                "{}|{}|{}|{}|{}|{}",
                summary.kind,
                summary.attribute,
                summary.start,
                summary.end,
                summary.cause,
                records.join(",")
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_conflict_presets_match_local() -> anyhow::Result<()> {
    let contents = std::fs::read_to_string("web/presets.json")?;
    let presets: Vec<Preset> = serde_json::from_str(&contents)?;
    let config = build_config_from_presets(&presets);
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

    for preset in presets {
        client
            .reset(proto::Empty {})
            .await
            .expect("reset");

        let mut local_store = unirust_rs::Store::new();
        let local_ontology = config.clone().build_ontology(&mut local_store);
        let mut local = Unirust::with_store_and_tuning(
            local_ontology,
            local_store,
            StreamingTuning::from_profile(TuningProfile::Balanced),
        );

        let inputs = build_record_inputs(&preset.records);
        if !inputs.is_empty() {
            client
                .ingest_records(IngestRecordsRequest {
                    records: inputs.clone(),
                })
                .await
                .expect("stream records");
        }

        for record in &preset.records {
            let local_record = build_local_record(&mut local, record)?;
            local.stream_record_update_graph(local_record)?;
        }

        let remote = client
            .list_conflicts(proto::ListConflictsRequest {
                start: 0,
                end: 0,
                attribute: String::new(),
            })
            .await
            .expect("get_conflicts")
            .into_inner();

        let mut remote_norm = normalize_remote_conflicts(remote);
        remote_norm.sort();
        let mut local_norm = normalize_local_conflicts(&local);
        local_norm.sort();

        assert_eq!(local_norm, remote_norm, "preset {} mismatch", preset.id);
    }

    Ok(())
}
