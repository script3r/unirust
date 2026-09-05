//! Persistent equivalence checks for cross-shard boundary interval sweeps.

use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request};
use unirust_rs::distributed::proto::router_service_server::RouterService;
use unirust_rs::distributed::{
    hash_record_to_shard, proto, DistributedOntologyConfig, IdentityKeyConfig, RouterNode,
    ShardNode,
};
use unirust_rs::ontology::{IdentityKey, StrongIdentifier};
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, QueryDescriptor, QueryOutcome, Record,
    RecordId, RecordIdentity, StreamingTuning, Unirust,
};

struct Cluster {
    router: Arc<RouterNode>,
    shards: Vec<ShardNode>,
    servers: Vec<JoinHandle<()>>,
    _directory: TempDir,
}

impl Cluster {
    async fn new(config: &DistributedOntologyConfig) -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let mut addresses = Vec::new();
        let mut shards = Vec::new();
        let mut servers = Vec::new();
        for shard_id in 0..3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            addresses.push(format!("http://{}", listener.local_addr()?));
            let shard = ShardNode::new_with_data_dir(
                shard_id,
                config.clone(),
                StreamingTuning::balanced(),
                Some(directory.path().join(format!("shard-{shard_id}"))),
                false,
                None,
            )?;
            let service = proto::shard_service_server::ShardServiceServer::new(shard.clone());
            servers.push(tokio::spawn(async move {
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .expect("temporal sweep shard server");
            }));
            shards.push(shard);
        }
        let router = RouterNode::connect(addresses, config.clone()).await?;
        Ok(Self {
            router,
            shards,
            servers,
            _directory: directory,
        })
    }

    async fn query(&self, label: &str) -> anyhow::Result<proto::QueryMatch> {
        let response = RouterService::query_entities(
            self.router.as_ref(),
            Request::new(proto::QueryEntitiesRequest {
                descriptors: vec![proto::QueryDescriptor {
                    attr: "label".into(),
                    value: label.into(),
                }],
                start: -1,
                end: 2_000,
            }),
        )
        .await?
        .into_inner();
        match response.outcome {
            Some(proto::query_entities_response::Outcome::Matches(mut matches)) => {
                assert_eq!(matches.matches.len(), 1, "label {label}");
                Ok(matches.matches.remove(0))
            }
            other => anyhow::bail!("unexpected label query outcome: {other:?}"),
        }
    }

    async fn stop(self) -> anyhow::Result<()> {
        for shard in &self.shards {
            shard.shutdown().await?;
        }
        for server in &self.servers {
            server.abort();
        }
        Ok(())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

fn routed_record(
    config: &DistributedOntologyConfig,
    index: u32,
    shard: usize,
    label: String,
    interval: Interval,
    strong_value: Option<String>,
) -> proto::RecordInput {
    let descriptor = |attr: &str, value: String| proto::RecordDescriptor {
        attr: attr.into(),
        value,
        start: interval.start,
        end: interval.end,
    };
    let mut record = proto::RecordInput {
        index,
        identity: Some(proto::RecordIdentity {
            entity_type: "person".into(),
            // Use one perspective so local tainted-key suppression does not
            // introduce a separate cross-perspective matching policy difference.
            perspective: "source".into(),
            uid: label.clone(),
        }),
        descriptors: vec![
            descriptor("email", String::new()),
            descriptor("phone", "shared-history".into()),
            descriptor("label", label),
        ],
    };
    if let Some(value) = strong_value {
        record.descriptors.push(descriptor("ssn", value));
    }
    for candidate in 0..1_000 {
        record.descriptors[0].value = format!("record-{index}-{candidate}@example.com");
        if hash_record_to_shard(config, &record, 3) == shard {
            return record;
        }
    }
    panic!("could not find an email routing to shard {shard}");
}

fn local_query(engine: &mut Unirust, label: &str) -> anyhow::Result<RecordId> {
    let query = QueryDescriptor {
        attr: engine.intern_attr("label"),
        value: engine.intern_value(label),
    };
    match engine.query(&[query], Interval::new(-1, 2_000)?)? {
        QueryOutcome::Matches(matches) => {
            assert_eq!(matches.len(), 1, "local label {label}");
            Ok(matches[0].root_record_id)
        }
        other => anyhow::bail!("unexpected local query: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_temporal_reconciliation_matches_persistent_local_resolution() -> anyhow::Result<()>
{
    let config = DistributedOntologyConfig {
        identity_keys: ["email", "phone"]
            .into_iter()
            .map(|attr| IdentityKeyConfig {
                name: format!("{attr}_key"),
                attributes: vec![attr.into()],
            })
            .collect(),
        strong_identifiers: vec!["ssn".into()],
        constraints: Vec::new(),
    };
    let mut records = Vec::new();
    let windows = 64u32;
    for window in 0..windows {
        let start = i64::from(window) * 20;
        for (offset, shard, suffix, begin, end, strong) in [
            (0, 0, "a", start, start + 10, Some(format!("id-{window}"))),
            (1, 1, "b", start + 2, start + 8, None),
            (
                2,
                2,
                "c",
                start + 8,
                start + 15,
                Some(if window.is_multiple_of(2) {
                    format!("id-{window}")
                } else {
                    format!("conflict-{window}")
                }),
            ),
            (3, 2, "d", start + 15, start + 20, None),
        ] {
            records.push(routed_record(
                &config,
                window * 4 + offset,
                shard,
                format!("{window}-{suffix}"),
                Interval::new(begin, end)?,
                strong,
            ));
        }
    }

    let local_directory = tempfile::tempdir()?;
    let mut ontology = Ontology::new();
    for attr in ["email", "phone"] {
        ontology.add_identity_key(IdentityKey::from_names(vec![attr], format!("{attr}_key")));
    }
    ontology.add_strong_identifier(StrongIdentifier::from_name("ssn", "ssn"));
    let mut local = Unirust::with_store(ontology, PersistentStore::open(local_directory.path())?);
    let local_records = records
        .iter()
        .map(|input| {
            let identity = input.identity.as_ref().unwrap();
            let descriptors = input
                .descriptors
                .iter()
                .map(|descriptor| {
                    Descriptor::new(
                        local.intern_attr(&descriptor.attr),
                        local.intern_value(&descriptor.value),
                        Interval::new(descriptor.start, descriptor.end).unwrap(),
                    )
                })
                .collect();
            Record::new(
                RecordId(0),
                RecordIdentity::new(
                    identity.entity_type.clone(),
                    identity.perspective.clone(),
                    identity.uid.clone(),
                ),
                descriptors,
            )
        })
        .collect();
    local.stream_records(local_records)?;

    let cluster = Cluster::new(&config).await?;
    RouterService::ingest_records(
        cluster.router.as_ref(),
        Request::new(proto::IngestRecordsRequest {
            records,
            internal_protocol_version: 0,
        }),
    )
    .await?;
    let reconciled = RouterService::reconcile(
        cluster.router.as_ref(),
        Request::new(proto::ReconcileRequest {
            shard_metadata: Vec::new(),
        }),
    )
    .await?
    .into_inner();
    assert_eq!(reconciled.merges_performed, windows + windows / 2);

    let mut distinct_windows = HashSet::new();
    for window in 0..windows {
        let mut global_ids = Vec::new();
        let mut local_ids = Vec::new();
        for suffix in ["a", "b", "c", "d"] {
            let label = format!("{window}-{suffix}");
            let result = cluster.query(&label).await?;
            assert!(result
                .golden
                .iter()
                .all(|entry| entry.start >= result.start && entry.end <= result.end));
            global_ids.push((result.shard_id, result.cluster_id));
            local_ids.push(local_query(&mut local, &label)?);
        }
        assert!(distinct_windows.insert(global_ids[0]));
        for left in 0..4 {
            for right in left + 1..4 {
                assert_eq!(
                    global_ids[left] == global_ids[right],
                    local_ids[left] == local_ids[right],
                    "window {window}, records {left} and {right}; global {global_ids:?}, local {local_ids:?}"
                );
            }
        }
        assert_eq!(global_ids[0], global_ids[1]);
        assert_eq!(global_ids[0] == global_ids[2], window.is_multiple_of(2));
        assert_ne!(
            global_ids[2], global_ids[3],
            "adjacent intervals must not merge"
        );
    }
    let repeated = RouterService::reconcile(
        cluster.router.as_ref(),
        Request::new(proto::ReconcileRequest {
            shard_metadata: Vec::new(),
        }),
    )
    .await?
    .into_inner();
    assert_eq!(repeated.merges_performed, 0);
    cluster.stop().await
}
