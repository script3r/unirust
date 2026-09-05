//! Persistent distributed regressions for canonical entity queries and guards.

use proto::router_service_server::RouterService;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::{http, Service};
use tonic::server::NamedService;
use tonic::{transport::Server, Request};
use unirust_rs::distributed::{
    hash_record_to_shard, proto, DistributedOntologyConfig, IdentityKeyConfig, RouterNode,
    ShardNode,
};
use unirust_rs::{StreamingTuning, TuningProfile};

static AUDIT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone)]
struct SynchronizeQueries<S> {
    inner: S,
    barrier: Option<Arc<tokio::sync::Barrier>>,
}

impl<S, B> Service<http::Request<B>> for SynchronizeQueries<S>
where
    S: Service<
            http::Request<B>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Send,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let barrier = (request.uri().path() == "/unirust.ShardService/QueryEntityFragments")
            .then(|| self.barrier.clone())
            .flatten();
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await?;
            if let Some(barrier) = barrier {
                if tokio::time::timeout(std::time::Duration::from_secs(2), barrier.wait())
                    .await
                    .is_err()
                {
                    return Ok(tonic::Status::deadline_exceeded(
                        "router did not dispatch peer query concurrently",
                    )
                    .into_http());
                }
            }
            Ok(response)
        })
    }
}

impl<S: NamedService> NamedService for SynchronizeQueries<S> {
    const NAME: &'static str = S::NAME;
}

struct Cluster {
    router: Arc<RouterNode>,
    shards: Vec<ShardNode>,
    servers: Vec<JoinHandle<()>>,
    _directory: TempDir,
}

impl Cluster {
    async fn stop(self) -> anyhow::Result<()> {
        for shard in &self.shards {
            shard.shutdown().await?;
        }
        for server in self.servers {
            server.abort();
            let _ = server.await;
        }
        Ok(())
    }

    async fn query(&self, descriptors: &[(&str, &str)]) -> anyhow::Result<Vec<proto::QueryMatch>> {
        let response = RouterService::query_entities(
            self.router.as_ref(),
            Request::new(proto::QueryEntitiesRequest {
                descriptors: descriptors
                    .iter()
                    .map(|(attr, value)| proto::QueryDescriptor {
                        attr: (*attr).into(),
                        value: (*value).into(),
                    })
                    .collect(),
                start: 0,
                end: 100,
            }),
        )
        .await?
        .into_inner();
        match response.outcome {
            Some(proto::query_entities_response::Outcome::Matches(matches)) => Ok(matches.matches),
            other => anyhow::bail!("expected query matches, got {other:?}"),
        }
    }
}

fn record(index: u32, email: &str, extra: (&str, &str)) -> proto::RecordInput {
    proto::RecordInput {
        index,
        identity: Some(proto::RecordIdentity {
            entity_type: "person".into(),
            perspective: format!("source-{index}"),
            uid: format!("record-{index}"),
        }),
        descriptors: [("email", email), ("phone", "shared-phone"), extra]
            .into_iter()
            .map(|(attr, value)| proto::RecordDescriptor {
                attr: attr.into(),
                value: value.into(),
                start: 0,
                end: 100,
            })
            .collect(),
    }
}

async fn empty_cluster(config: &DistributedOntologyConfig, count: u32) -> anyhow::Result<Cluster> {
    empty_cluster_with_barrier(config, count, None).await
}

async fn empty_cluster_with_barrier(
    config: &DistributedOntologyConfig,
    count: u32,
    barrier: Option<Arc<tokio::sync::Barrier>>,
) -> anyhow::Result<Cluster> {
    let directory = tempfile::tempdir()?;
    let mut urls = Vec::new();
    let mut shards = Vec::new();
    let mut servers = Vec::new();
    for shard_id in 0..count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        urls.push(format!("http://{}", listener.local_addr()?));
        let shard = ShardNode::new_with_data_dir(
            shard_id,
            config.clone(),
            StreamingTuning::from_profile(TuningProfile::Balanced),
            Some(directory.path().join(format!("shard-{shard_id}"))),
            false,
            None,
        )?;
        let service_shard = shard.clone();
        let barrier = barrier.clone();
        servers.push(tokio::spawn(async move {
            Server::builder()
                .add_service(SynchronizeQueries {
                    inner: proto::shard_service_server::ShardServiceServer::new(service_shard),
                    barrier,
                })
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("audit shard server");
        }));
        shards.push(shard);
    }
    let router = RouterNode::connect(urls, config.clone()).await?;
    Ok(Cluster {
        router,
        shards,
        servers,
        _directory: directory,
    })
}

async fn reconciled_cluster() -> anyhow::Result<Cluster> {
    let config = DistributedOntologyConfig {
        identity_keys: ["email", "phone"]
            .into_iter()
            .map(|attr| IdentityKeyConfig {
                name: format!("{attr}_key"),
                attributes: vec![attr.into()],
            })
            .collect(),
        strong_identifiers: vec![],
        constraints: vec![],
    };
    let cluster = empty_cluster(&config, 2).await?;
    let router = &cluster.router;
    let first = record(0, "first@example.com", ("name", "Alice"));
    let first_target = hash_record_to_shard(&config, &first, 2);
    let second = (0..1000)
        .map(|candidate| {
            record(
                1,
                &format!("second-{candidate}@example.com"),
                ("city", "London"),
            )
        })
        .find(|candidate| hash_record_to_shard(&config, candidate, 2) != first_target)
        .expect("find email routed to other shard");
    let response = RouterService::ingest_records(
        router.as_ref(),
        Request::new(proto::IngestRecordsRequest {
            records: vec![first, second],
            internal_protocol_version: 0,
        }),
    )
    .await?
    .into_inner();
    assert_ne!(
        response.assignments[0].shard_id,
        response.assignments[1].shard_id
    );
    let reconciliation = RouterService::reconcile(
        router.as_ref(),
        Request::new(proto::ReconcileRequest {
            shard_metadata: vec![],
        }),
    )
    .await?
    .into_inner();
    assert_eq!(reconciliation.merges_performed, 1);
    assert_eq!(
        cluster.query(&[("phone", "shared-phone")]).await?.len(),
        1,
        "shared identity must resolve to one entity"
    );
    Ok(cluster)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_conjunction_across_reconciled_shards() -> anyhow::Result<()> {
    let _lock = AUDIT_LOCK.lock().await;
    let cluster = reconciled_cluster().await?;
    let matches = cluster
        .query(&[("name", "Alice"), ("city", "London")])
        .await?;
    cluster.stop().await?;
    assert_eq!(
        matches.len(),
        1,
        "the reconciled entity contains both descriptors over [0,100)"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_golden_across_reconciled_shards() -> anyhow::Result<()> {
    let _lock = AUDIT_LOCK.lock().await;
    let cluster = reconciled_cluster().await?;
    let matches = cluster.query(&[("name", "Alice")]).await?;
    cluster.stop().await?;
    assert_eq!(matches.len(), 1);
    assert!(
        matches[0]
            .golden
            .iter()
            .any(|descriptor| descriptor.attr == "city" && descriptor.value == "London"),
        "entity golden must include city from its reconciled fragment, got {:?}",
        matches[0].golden
    );
    Ok(())
}

fn config_for_keys(keys: &[&str], strong: &[&str]) -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: keys
            .iter()
            .map(|attr| IdentityKeyConfig {
                name: format!("{attr}_key"),
                attributes: vec![(*attr).into()],
            })
            .collect(),
        strong_identifiers: strong.iter().map(|attr| (*attr).into()).collect(),
        constraints: Vec::new(),
    }
}

fn record_on_shard(
    config: &DistributedOntologyConfig,
    target: u32,
    count: usize,
    extra: (&str, &str),
) -> proto::RecordInput {
    (0..1000)
        .map(|candidate| {
            record(
                target,
                &format!("target-{target}-{candidate}@example.com"),
                extra,
            )
        })
        .find(|record| hash_record_to_shard(config, record, count) == target as usize)
        .expect("find routed record")
}

async fn ingest(cluster: &Cluster, records: Vec<proto::RecordInput>) -> anyhow::Result<()> {
    RouterService::ingest_records(
        cluster.router.as_ref(),
        Request::new(proto::IngestRecordsRequest {
            records,
            internal_protocol_version: 0,
        }),
    )
    .await?;
    Ok(())
}

async fn reconcile(cluster: &Cluster) -> anyhow::Result<proto::ReconcileResponse> {
    Ok(RouterService::reconcile(
        cluster.router.as_ref(),
        Request::new(proto::ReconcileRequest {
            shard_metadata: Vec::new(),
        }),
    )
    .await?
    .into_inner())
}

fn descriptor(attr: &str, value: &str, start: i64, end: i64) -> proto::RecordDescriptor {
    proto::RecordDescriptor {
        attr: attr.into(),
        value: value.into(),
        start,
        end,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_query_preserves_intersections_and_adjacent_intervals() -> anyhow::Result<()> {
    let _lock = AUDIT_LOCK.lock().await;
    let config = config_for_keys(&["email", "phone"], &[]);
    let cluster = empty_cluster(&config, 2).await?;
    let mut left = record_on_shard(&config, 0, 2, ("name", "Alice"));
    left.descriptors
        .iter_mut()
        .find(|entry| entry.attr == "name")
        .unwrap()
        .end = 60;
    left.descriptors.extend([
        descriptor("left", "yes", 0, 40),
        descriptor("status", "active", 0, 50),
    ]);
    let mut right = record_on_shard(&config, 1, 2, ("city", "London"));
    right
        .descriptors
        .iter_mut()
        .find(|entry| entry.attr == "city")
        .unwrap()
        .start = 40;
    right
        .descriptors
        .push(descriptor("status", "active", 50, 100));
    ingest(&cluster, vec![left, right]).await?;
    assert_eq!(reconcile(&cluster).await?.merges_performed, 1);
    let overlap = cluster
        .query(&[("name", "Alice"), ("city", "London")])
        .await?;
    assert_eq!(
        overlap
            .iter()
            .map(|entry| (entry.start, entry.end))
            .collect::<Vec<_>>(),
        vec![(40, 60)]
    );
    assert!(overlap[0]
        .golden
        .iter()
        .all(|entry| entry.start >= 40 && entry.end <= 60));
    assert!(
        cluster
            .query(&[("left", "yes"), ("city", "London")])
            .await?
            .is_empty(),
        "adjacent intervals have no conjunction overlap"
    );
    let adjacent = cluster.query(&[("status", "active")]).await?;
    assert_eq!(
        adjacent
            .iter()
            .map(|entry| (entry.start, entry.end))
            .collect::<Vec<_>>(),
        vec![(0, 100)]
    );
    assert_eq!(
        adjacent[0]
            .golden
            .iter()
            .filter(|entry| entry.attr == "status")
            .map(|entry| (entry.start, entry.end))
            .collect::<Vec<_>>(),
        vec![(0, 100)]
    );
    assert!(cluster.query(&[("unknown", "value")]).await?.is_empty());
    cluster.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn golden_conflict_trimming_uses_all_raw_entity_fragments() -> anyhow::Result<()> {
    let _lock = AUDIT_LOCK.lock().await;
    let config = config_for_keys(&["email", "phone"], &[]);
    let cluster = empty_cluster(&config, 2).await?;
    let left = record_on_shard(&config, 0, 2, ("name", "Alice"));
    let right = record_on_shard(&config, 1, 2, ("name", "Alice"));
    let mut conflict = left.clone();
    conflict.index = 2;
    conflict.identity.as_mut().unwrap().uid = "conflicting-name".into();
    let name = conflict
        .descriptors
        .iter_mut()
        .find(|entry| entry.attr == "name")
        .unwrap();
    name.value = "Bob".into();
    name.start = 20;
    name.end = 40;
    ingest(&cluster, vec![left, right, conflict]).await?;
    reconcile(&cluster).await?;
    let matches = cluster.query(&[("phone", "shared-phone")]).await?;
    assert_eq!(matches.len(), 1);
    let names = matches[0]
        .golden
        .iter()
        .filter(|entry| entry.attr == "name")
        .map(|entry| (entry.value.as_str(), entry.start, entry.end))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![("Alice", 0, 20), ("Alice", 40, 100)],
        "a remote Alice observation must not restore the locally conflicted interval"
    );
    assert!(
        !matches[0].golden.iter().any(|entry| entry.attr == "email"),
        "conflicting emails on separate fragments must be removed globally"
    );
    cluster.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_dispatches_candidate_and_hydration_queries_concurrently() -> anyhow::Result<()> {
    let _lock = AUDIT_LOCK.lock().await;
    let config = config_for_keys(&["email", "phone"], &[]);
    let cluster =
        empty_cluster_with_barrier(&config, 2, Some(Arc::new(tokio::sync::Barrier::new(2))))
            .await?;
    let left = record_on_shard(&config, 0, 2, ("name", "Alice"));
    let right = record_on_shard(&config, 1, 2, ("city", "London"));
    ingest(&cluster, vec![left, right]).await?;
    reconcile(&cluster).await?;
    assert_eq!(
        cluster
            .query(&[("name", "Alice"), ("city", "London")])
            .await?
            .len(),
        1
    );
    cluster.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_upgrade_retains_v5_reservation_topology_binding() -> anyhow::Result<()> {
    let _lock = AUDIT_LOCK.lock().await;
    let directory = tempfile::tempdir()?;
    {
        let mut store = unirust_rs::PersistentStore::open(directory.path())?;
        unirust_rs::store::RecordStore::mark_source_reservation_backfill(&mut store, 5, 2)?;
    }
    let config = DistributedOntologyConfig::empty();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let shard = ShardNode::new_with_data_dir(
        0,
        config.clone(),
        StreamingTuning::balanced(),
        Some(directory.path().to_path_buf()),
        false,
        None,
    )?;
    let service_shard = shard.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::shard_service_server::ShardServiceServer::new(
                service_shard,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("upgrade shard");
    });
    let error = RouterNode::connect(vec![format!("http://{address}")], config)
        .await
        .err()
        .expect("upgrading the wire protocol must not permit changing the old two-shard topology");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("topology mismatch"));
    shard.shutdown().await?;
    server.abort();
    let _ = server.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prior_reconciliation_preserves_remote_component_strong_ids() -> anyhow::Result<()> {
    let _lock = AUDIT_LOCK.lock().await;
    let config = config_for_keys(&["email", "key1", "key2"], &["ssn"]);
    let cluster = empty_cluster(&config, 3).await?;
    let mut records = Vec::new();
    for target in 0..3 {
        let mut record = record_on_shard(&config, target, 3, ("group", "audit"));
        record.descriptors.retain(|entry| entry.attr != "phone");
        record.identity.as_mut().unwrap().perspective =
            if target == 1 { "bridge" } else { "hr" }.into();
        match target {
            0 => record.descriptors.extend([
                descriptor("key1", "AB", 0, 100),
                descriptor("ssn", "111", 0, 100),
            ]),
            1 => record.descriptors.extend([
                descriptor("key1", "AB", 0, 100),
                descriptor("key2", "BC", 0, 100),
            ]),
            _ => record.descriptors.extend([
                descriptor("key2", "BC", 0, 100),
                descriptor("ssn", "222", 0, 100),
            ]),
        }
        records.push(record);
    }
    let last = records.pop().unwrap();
    ingest(&cluster, records).await?;
    assert_eq!(reconcile(&cluster).await?.merges_performed, 1);
    ingest(&cluster, vec![last]).await?;
    let result = reconcile(&cluster).await?;
    assert_eq!(
        result.merges_performed, 0,
        "clean key1 must still contribute hr SSN111 to the canonical component"
    );
    assert!(result.conflicts_blocked > 0);
    let response = RouterService::query_entities(
        cluster.router.as_ref(),
        Request::new(proto::QueryEntitiesRequest {
            descriptors: vec![proto::QueryDescriptor {
                attr: "group".into(),
                value: "audit".into(),
            }],
            start: 0,
            end: 100,
        }),
    )
    .await?
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(proto::query_entities_response::Outcome::Conflict(_))
    ));
    cluster.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transitive_cross_shard_merge_preserves_strong_id_guard() -> anyhow::Result<()> {
    let _lock = AUDIT_LOCK.lock().await;
    let config = DistributedOntologyConfig {
        identity_keys: ["email", "key1", "key2"]
            .into_iter()
            .map(|attr| IdentityKeyConfig {
                name: format!("{attr}_key"),
                attributes: vec![attr.into()],
            })
            .collect(),
        strong_identifiers: vec!["ssn".into()],
        constraints: vec![],
    };
    let cluster = empty_cluster(&config, 3).await?;
    let mut records = Vec::new();
    for target in 0..3 {
        let extras = match target {
            0 => vec![("key1", "AB"), ("ssn", "111")],
            1 => vec![("key1", "AB"), ("key2", "BC")],
            _ => vec![("key2", "BC"), ("ssn", "222")],
        };
        let candidate = (0..1000)
            .map(|candidate| {
                let mut input = record(
                    target,
                    &format!("target-{target}-{candidate}@example.com"),
                    ("group", "audit"),
                );
                input.identity.as_mut().unwrap().perspective =
                    if target == 1 { "bridge" } else { "hr" }.into();
                input
                    .descriptors
                    .retain(|descriptor| descriptor.attr != "phone");
                input.descriptors.extend(extras.iter().map(|(attr, value)| {
                    proto::RecordDescriptor {
                        attr: (*attr).into(),
                        value: (*value).into(),
                        start: 0,
                        end: 100,
                    }
                }));
                input
            })
            .find(|input| hash_record_to_shard(&config, input, 3) == target as usize)
            .expect("find target shard");
        records.push(candidate);
    }
    {
        use unirust_rs::ontology::{IdentityKey, StrongIdentifier};
        use unirust_rs::{
            Descriptor, Interval, Ontology, PersistentStore, Record, RecordId, RecordIdentity,
            Unirust,
        };
        let store = PersistentStore::open(cluster._directory.path().join("local-control"))?;
        let mut ontology = Ontology::new();
        for attr in ["email", "key1", "key2"] {
            ontology.add_identity_key(IdentityKey::from_names(vec![attr], format!("{attr}_key")));
        }
        ontology.add_strong_identifier(StrongIdentifier::from_name("ssn", "ssn"));
        let mut engine =
            Unirust::with_store_and_tuning(ontology, store, StreamingTuning::balanced());
        let mut local_records = Vec::new();
        for input in &records {
            let identity = input.identity.as_ref().expect("audit input identity");
            let mut descriptors = Vec::new();
            for descriptor in &input.descriptors {
                descriptors.push(Descriptor::new(
                    engine.intern_attr(&descriptor.attr),
                    engine.intern_value(&descriptor.value),
                    Interval::new(descriptor.start, descriptor.end)?,
                ));
            }
            local_records.push(Record::new(
                RecordId(0),
                RecordIdentity::new(
                    identity.entity_type.clone(),
                    identity.perspective.clone(),
                    identity.uid.clone(),
                ),
                descriptors,
            ));
        }
        engine.stream_records(local_records)?;
        assert_eq!(
            engine.streaming_cluster_count(),
            Some(2),
            "local persistent engine must preserve the strong-ID guard"
        );
        let descriptor = unirust_rs::query::QueryDescriptor {
            attr: engine.intern_attr("group"),
            value: engine.intern_value("audit"),
        };
        let local_query = engine.query_master_entities(&[descriptor], Interval::new(0, 100)?)?;
        assert!(
            matches!(local_query, unirust_rs::query::QueryOutcome::Conflict(_)),
            "local persistent query should expose distinct overlapping entities: {local_query:?}"
        );
        println!("local PersistentStore control: 2 clusters, query reports Conflict");
    }
    RouterService::ingest_records(
        cluster.router.as_ref(),
        Request::new(proto::IngestRecordsRequest {
            records,
            internal_protocol_version: 0,
        }),
    )
    .await?;
    let reconciliation = RouterService::reconcile(
        cluster.router.as_ref(),
        Request::new(proto::ReconcileRequest {
            shard_metadata: vec![],
        }),
    )
    .await?
    .into_inner();
    let response = RouterService::query_entities(
        cluster.router.as_ref(),
        Request::new(proto::QueryEntitiesRequest {
            descriptors: vec![proto::QueryDescriptor {
                attr: "group".into(),
                value: "audit".into(),
            }],
            start: 0,
            end: 100,
        }),
    )
    .await?
    .into_inner();
    cluster.stop().await?;
    assert_eq!(reconciliation.merges_performed, 1,
        "cannot merge A(hr,ssn=111)—B(bridge,no ssn)—C(hr,ssn=222) at [0,100): reconciliation={reconciliation:?}; query={response:?}");
    assert!(reconciliation.conflicts_blocked > 0);
    assert!(matches!(
        response.outcome,
        Some(proto::query_entities_response::Outcome::Conflict(_))
    ));
    Ok(())
}
