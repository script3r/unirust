use crate::conflicts::ConflictSummary;
use crate::graph::GoldenDescriptor;
use crate::model::{AttrId, Record, RecordId, RecordIdentity};
use crate::ontology::{Constraint, IdentityKey, Ontology, StrongIdentifier};
use crate::query::{QueryDescriptor, QueryOutcome};
use crate::temporal::Interval;
use crate::persistence::PersistentOpenOptions;
use crate::{PersistentStore, StoreMetrics, StreamingTuning, Unirust};
use anyhow::Result as AnyResult;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{fs};
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use std::pin::Pin;

#[derive(Debug, Deserialize)]
struct JsonRecordIdentity {
    entity_type: String,
    perspective: String,
    uid: String,
}

#[derive(Debug, Deserialize)]
struct JsonRecordDescriptor {
    attr: String,
    value: String,
    start: i64,
    end: i64,
}

#[derive(Debug, Deserialize)]
struct JsonRecordInput {
    index: u32,
    identity: JsonRecordIdentity,
    descriptors: Vec<JsonRecordDescriptor>,
}

pub mod proto {
    tonic::include_proto!("unirust");
}

#[derive(Debug, Default)]
struct LatencyCounters {
    count: AtomicU64,
    total_micros: AtomicU64,
    max_micros: AtomicU64,
}

impl LatencyCounters {
    fn record(&self, micros: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_micros.fetch_add(micros, Ordering::Relaxed);
        let mut current = self.max_micros.load(Ordering::Relaxed);
        while micros > current {
            match self.max_micros.compare_exchange(
                current,
                micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    fn snapshot(&self) -> proto::LatencyMetrics {
        proto::LatencyMetrics {
            count: self.count.load(Ordering::Relaxed),
            total_micros: self.total_micros.load(Ordering::Relaxed),
            max_micros: self.max_micros.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct Metrics {
    start: Instant,
    ingest_requests: AtomicU64,
    ingest_records: AtomicU64,
    query_requests: AtomicU64,
    ingest_latency: LatencyCounters,
    query_latency: LatencyCounters,
}

impl Metrics {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            ingest_requests: AtomicU64::new(0),
            ingest_records: AtomicU64::new(0),
            query_requests: AtomicU64::new(0),
            ingest_latency: LatencyCounters::default(),
            query_latency: LatencyCounters::default(),
        }
    }

    fn record_ingest(&self, record_count: usize, micros: u64) {
        self.ingest_requests.fetch_add(1, Ordering::Relaxed);
        self.ingest_records
            .fetch_add(record_count as u64, Ordering::Relaxed);
        self.ingest_latency.record(micros);
    }

    fn record_query(&self, micros: u64) {
        self.query_requests.fetch_add(1, Ordering::Relaxed);
        self.query_latency.record(micros);
    }

    fn uptime_seconds(&self) -> u64 {
        self.start.elapsed().as_secs()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityKeyConfig {
    pub name: String,
    pub attributes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintKind {
    Unique,
    UniqueWithinPerspective,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintConfig {
    pub name: String,
    pub attribute: String,
    pub kind: ConstraintKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedOntologyConfig {
    pub identity_keys: Vec<IdentityKeyConfig>,
    pub strong_identifiers: Vec<String>,
    pub constraints: Vec<ConstraintConfig>,
}

impl DistributedOntologyConfig {
    pub fn empty() -> Self {
        Self {
            identity_keys: Vec::new(),
            strong_identifiers: Vec::new(),
            constraints: Vec::new(),
        }
    }

    pub fn build_ontology(&self, store: &mut crate::Store) -> Ontology {
        let mut ontology = Ontology::new();

        for key in &self.identity_keys {
            let attrs: Vec<AttrId> = key
                .attributes
                .iter()
                .map(|attr| store.interner_mut().intern_attr(attr))
                .collect();
            ontology.add_identity_key(IdentityKey::new(attrs, key.name.clone()));
        }

        for attr in &self.strong_identifiers {
            let attr_id = store.interner_mut().intern_attr(attr);
            ontology
                .add_strong_identifier(StrongIdentifier::new(attr_id, format!("{attr}_strong")));
        }

        for constraint in &self.constraints {
            let attr_id = store.interner_mut().intern_attr(&constraint.attribute);
            let constraint = match constraint.kind {
                ConstraintKind::Unique => Constraint::unique(attr_id, constraint.name.clone()),
                ConstraintKind::UniqueWithinPerspective => {
                    Constraint::unique_within_perspective(attr_id, constraint.name.clone())
                }
            };
            ontology.add_constraint(constraint);
        }

        ontology
    }
}

fn map_proto_config(config: &proto::OntologyConfig) -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: config
            .identity_keys
            .iter()
            .map(|entry| IdentityKeyConfig {
                name: entry.name.clone(),
                attributes: entry.attributes.clone(),
            })
            .collect(),
        strong_identifiers: config.strong_identifiers.clone(),
        constraints: config
            .constraints
            .iter()
            .map(|entry| ConstraintConfig {
                name: entry.name.clone(),
                attribute: entry.attribute.clone(),
                kind: match proto::ConstraintKind::try_from(entry.kind)
                    .unwrap_or(proto::ConstraintKind::Unique)
                {
                    proto::ConstraintKind::Unique => ConstraintKind::Unique,
                    proto::ConstraintKind::UniqueWithinPerspective => {
                        ConstraintKind::UniqueWithinPerspective
                    }
                    proto::ConstraintKind::Unspecified => ConstraintKind::Unique,
                },
            })
            .collect(),
    }
}

async fn fetch_record_batch_from_url(url: &str) -> Result<proto::IngestRecordsRequest, Status> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(Status::invalid_argument("url must be http or https"));
    }

    let response = reqwest::get(url)
        .await
        .map_err(|err| Status::unavailable(err.to_string()))?;
    if !response.status().is_success() {
        return Err(Status::unavailable(format!(
            "failed to fetch batch: {}",
            response.status()
        )));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|err| Status::unavailable(err.to_string()))?;
    let inputs: Vec<JsonRecordInput> = serde_json::from_slice(&bytes)
        .map_err(|err| Status::invalid_argument(err.to_string()))?;

    let records = inputs
        .into_iter()
        .map(|input| proto::RecordInput {
            index: input.index,
            identity: Some(proto::RecordIdentity {
                entity_type: input.identity.entity_type,
                perspective: input.identity.perspective,
                uid: input.identity.uid,
            }),
            descriptors: input
                .descriptors
                .into_iter()
                .map(|descriptor| proto::RecordDescriptor {
                    attr: descriptor.attr,
                    value: descriptor.value,
                    start: descriptor.start,
                    end: descriptor.end,
                })
                .collect(),
        })
        .collect();

    Ok(proto::IngestRecordsRequest { records })
}

pub fn hash_record_to_shard(
    config: &DistributedOntologyConfig,
    record: &proto::RecordInput,
    shard_count: usize,
) -> usize {
    let identity = record.identity.as_ref();
    let mut descriptors_by_attr: HashMap<&str, &str> = HashMap::new();
    for descriptor in &record.descriptors {
        descriptors_by_attr.insert(descriptor.attr.as_str(), descriptor.value.as_str());
    }

    for key in &config.identity_keys {
        let mut values = Vec::new();
        let mut has_all = true;
        for attr in &key.attributes {
            if let Some(value) = descriptors_by_attr.get(attr.as_str()) {
                values.push(*value);
            } else {
                has_all = false;
                break;
            }
        }
        if has_all {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            if let Some(identity) = identity {
                identity.entity_type.hash(&mut state);
            }
            key.name.hash(&mut state);
            for value in values {
                value.hash(&mut state);
            }
            return (state.finish() as usize) % shard_count;
        }
    }

    for constraint in &config.constraints {
        if let Some(value) = descriptors_by_attr.get(constraint.attribute.as_str()) {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            if let Some(identity) = identity {
                identity.entity_type.hash(&mut state);
                if matches!(constraint.kind, ConstraintKind::UniqueWithinPerspective) {
                    identity.perspective.hash(&mut state);
                }
            }
            constraint.name.hash(&mut state);
            constraint.attribute.hash(&mut state);
            value.hash(&mut state);
            return (state.finish() as usize) % shard_count;
        }
    }

    let mut state = std::collections::hash_map::DefaultHasher::new();
    if let Some(identity) = identity {
        identity.entity_type.hash(&mut state);
        identity.perspective.hash(&mut state);
        identity.uid.hash(&mut state);
    }
    (state.finish() as usize) % shard_count
}

#[derive(Clone)]
pub struct ShardNode {
    shard_id: u32,
    unirust: Arc<Mutex<Unirust>>,
    tuning: StreamingTuning,
    ontology_config: Arc<Mutex<DistributedOntologyConfig>>,
    data_dir: Option<PathBuf>,
    ingest_tx: tokio::sync::mpsc::Sender<IngestJob>,
    config_version: String,
    metrics: Arc<Metrics>,
}

const INGEST_QUEUE_CAPACITY: usize = 128;
const EXPORT_DEFAULT_LIMIT: usize = 1000;

struct IngestJob {
    records: Vec<proto::RecordInput>,
    respond_to: oneshot::Sender<Result<Vec<proto::IngestAssignment>, Status>>,
}

impl ShardNode {
    pub fn new(
        shard_id: u32,
        ontology_config: DistributedOntologyConfig,
        tuning: StreamingTuning,
    ) -> AnyResult<Self> {
        Self::new_with_data_dir(shard_id, ontology_config, tuning, None, false, None)
    }

    pub fn new_with_data_dir(
        shard_id: u32,
        ontology_config: DistributedOntologyConfig,
        tuning: StreamingTuning,
        data_dir: Option<PathBuf>,
        repair_on_start: bool,
        config_version: Option<String>,
    ) -> AnyResult<Self> {
        let config_version = config_version.unwrap_or_else(|| "unversioned".to_string());
        if let Some(path) = data_dir.clone() {
            let (store, config, ontology) =
                load_persistent_state(&path, ontology_config, repair_on_start)?;
            let unirust = Arc::new(Mutex::new(Unirust::with_store_and_tuning(
                ontology,
                store,
                tuning.clone(),
            )));
            let ingest_tx = spawn_ingest_worker(unirust.clone(), shard_id);
            return Ok(Self {
                shard_id,
                unirust,
                tuning,
                ontology_config: Arc::new(Mutex::new(config)),
                data_dir: Some(path),
                ingest_tx,
                config_version,
                metrics: Arc::new(Metrics::new()),
            });
        }

        let mut store = crate::Store::new();
        let ontology = ontology_config.clone().build_ontology(&mut store);
        let unirust = Arc::new(Mutex::new(Unirust::with_store_and_tuning(
            ontology,
            store,
            tuning.clone(),
        )));
        let ingest_tx = spawn_ingest_worker(unirust.clone(), shard_id);
        Ok(Self {
            shard_id,
            unirust,
            tuning,
            ontology_config: Arc::new(Mutex::new(ontology_config)),
            data_dir: None,
            ingest_tx,
            config_version,
            metrics: Arc::new(Metrics::new()),
        })
    }

    #[allow(clippy::result_large_err)]
    fn build_record(unirust: &mut Unirust, input: &proto::RecordInput) -> Result<Record, Status> {
        let identity = input
            .identity
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("record identity is required"))?;

        let descriptors = input
            .descriptors
            .iter()
            .map(|desc| {
                let attr = unirust.intern_attr(&desc.attr);
                let value = unirust.intern_value(&desc.value);
                let interval = Interval::new(desc.start, desc.end)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;
                Ok(crate::Descriptor::new(attr, value, interval))
            })
            .collect::<Result<Vec<_>, Status>>()?;

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

    #[allow(clippy::result_large_err)]
    fn build_record_with_id(
        unirust: &mut Unirust,
        record_id: u32,
        identity: &proto::RecordIdentity,
        descriptors: &[proto::RecordDescriptor],
    ) -> Result<Record, Status> {
        let descriptors = descriptors
            .iter()
            .map(|desc| {
                let attr = unirust.intern_attr(&desc.attr);
                let value = unirust.intern_value(&desc.value);
                let interval = Interval::new(desc.start, desc.end)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;
                Ok(crate::Descriptor::new(attr, value, interval))
            })
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Record::new(
            RecordId(record_id),
            RecordIdentity::new(
                identity.entity_type.clone(),
                identity.perspective.clone(),
                identity.uid.clone(),
            ),
            descriptors,
        ))
    }

    fn record_to_snapshot(unirust: &Unirust, record: &Record) -> proto::RecordSnapshot {
        proto::RecordSnapshot {
            record_id: record.id.0,
            identity: Some(proto::RecordIdentity {
                entity_type: record.identity.entity_type.clone(),
                perspective: record.identity.perspective.clone(),
                uid: record.identity.uid.clone(),
            }),
            descriptors: record
                .descriptors
                .iter()
                .map(|descriptor| proto::RecordDescriptor {
                    attr: unirust
                        .resolve_attr(descriptor.attr)
                        .unwrap_or_default(),
                    value: unirust
                        .resolve_value(descriptor.value)
                        .unwrap_or_default(),
                    start: descriptor.interval.start,
                    end: descriptor.interval.end,
                })
                .collect(),
        }
    }

    fn cluster_key_from_graph(
        cluster_id: crate::ClusterId,
        graph: &crate::graph::KnowledgeGraph,
    ) -> String {
        graph
            .nodes
            .iter()
            .find(|node| node.cluster_id == Some(cluster_id))
            .and_then(|node| node.properties.get("cluster_key"))
            .cloned()
            .unwrap_or_default()
    }

    fn to_proto_match(
        shard_id: u32,
        cluster_id: crate::ClusterId,
        interval: Interval,
        golden: &[GoldenDescriptor],
        cluster_key: Option<String>,
        cluster_key_identity: Option<String>,
    ) -> proto::QueryMatch {
        proto::QueryMatch {
            shard_id,
            cluster_id: cluster_id.0,
            start: interval.start,
            end: interval.end,
            cluster_key: cluster_key.unwrap_or_default(),
            cluster_key_identity: cluster_key_identity.unwrap_or_default(),
            golden: golden
                .iter()
                .map(|descriptor| proto::GoldenDescriptor {
                    attr: descriptor.attr.clone(),
                    value: descriptor.value.clone(),
                    start: descriptor.interval.start,
                    end: descriptor.interval.end,
                })
                .collect(),
        }
    }
}

fn resolve_checkpoint_path(data_dir: &Path, requested: &str) -> Result<PathBuf, Status> {
    if requested.is_empty() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| Status::internal(err.to_string()))?
            .as_secs();
        return Ok(data_dir.join("checkpoints").join(format!("{timestamp}")));
    }
    let candidate = PathBuf::from(requested);
    if candidate.is_absolute() {
        Ok(candidate)
    } else {
        Ok(data_dir.join(candidate))
    }
}

fn spawn_ingest_worker(
    unirust: Arc<Mutex<Unirust>>,
    shard_id: u32,
) -> mpsc::Sender<IngestJob> {
    let (tx, mut rx) = mpsc::channel::<IngestJob>(INGEST_QUEUE_CAPACITY);
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let result = {
                let mut guard = unirust.lock().await;
                process_ingest_batch(&mut guard, shard_id, &job.records)
            };
            let _ = job.respond_to.send(result);
        }
    });
    tx
}

fn process_ingest_batch(
    unirust: &mut Unirust,
    shard_id: u32,
    records: &[proto::RecordInput],
) -> Result<Vec<proto::IngestAssignment>, Status> {
    let mut assignments = Vec::new();
    for record in records {
        let record_input = ShardNode::build_record(unirust, record)?;
        let update = unirust
            .stream_record_update_graph(record_input)
            .map_err(|err| Status::internal(err.to_string()))?;
        let cluster_key = ShardNode::cluster_key_from_graph(update.assignment.cluster_id, &update.graph);

        assignments.push(proto::IngestAssignment {
            index: record.index,
            shard_id,
            record_id: update.assignment.record_id.0,
            cluster_id: update.assignment.cluster_id.0,
            cluster_key,
        });
    }
    Ok(assignments)
}

fn load_persistent_state(
    path: &Path,
    fallback_config: DistributedOntologyConfig,
    repair_on_start: bool,
) -> AnyResult<(PersistentStore, DistributedOntologyConfig, Ontology)> {
    let mut store = PersistentStore::open_with_options(
        path,
        PersistentOpenOptions {
            repair: repair_on_start,
        },
    )?;
    let stored_config = store
        .load_ontology_config()?
        .map(|payload| serde_json::from_slice(&payload))
        .transpose()?;
    let config = if let Some(config) = stored_config {
        config
    } else {
        store.save_ontology_config(&serde_json::to_vec(&fallback_config)?)?;
        fallback_config
    };
    let ontology = config.build_ontology(store.inner_mut());
    store.persist_state()?;
    Ok((store, config, ontology))
}

#[tonic::async_trait]
impl proto::shard_service_server::ShardService for ShardNode {
    type ExportRecordsStreamStream =
        Pin<Box<dyn Stream<Item = Result<proto::ExportRecordsChunk, Status>> + Send + 'static>>;

    async fn set_ontology(
        &self,
        request: Request<proto::ApplyOntologyRequest>,
    ) -> Result<Response<proto::ApplyOntologyResponse>, Status> {
        let config = request
            .into_inner()
            .config
            .ok_or_else(|| Status::invalid_argument("ontology config is required"))?;

        let config = map_proto_config(&config);
        let mut config_guard = self.ontology_config.lock().await;
        *config_guard = config.clone();

        if let Some(path) = &self.data_dir {
            let mut store =
                PersistentStore::open(path).map_err(|err| Status::internal(err.to_string()))?;
            store
                .reset_data()
                .map_err(|err| Status::internal(err.to_string()))?;
            store
                .save_ontology_config(&serde_json::to_vec(&config).map_err(|err| {
                    Status::internal(format!("failed to encode ontology config: {err}"))
                })?)
                .map_err(|err| Status::internal(err.to_string()))?;
            let ontology = config.build_ontology(store.inner_mut());
            store
                .persist_state()
                .map_err(|err| Status::internal(err.to_string()))?;
            let mut guard = self.unirust.lock().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        } else {
            let mut store = crate::Store::new();
            let ontology = config.build_ontology(&mut store);
            let mut guard = self.unirust.lock().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        }

        Ok(Response::new(proto::ApplyOntologyResponse {}))
    }

    async fn ingest_records(
        &self,
        request: Request<proto::IngestRecordsRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let start = Instant::now();
        let records = request.into_inner().records;
        let record_count = records.len();
        let (tx, rx) = oneshot::channel();
        let job = IngestJob {
            records,
            respond_to: tx,
        };
        self.ingest_tx
            .send(job)
            .await
            .map_err(|_| Status::unavailable("ingest queue unavailable"))?;
        let assignments = rx
            .await
            .map_err(|_| Status::internal("ingest worker dropped"))??;

        self.metrics
            .record_ingest(record_count, start.elapsed().as_micros() as u64);
        Ok(Response::new(proto::IngestRecordsResponse { assignments }))
    }

    async fn ingest_records_stream(
        &self,
        request: Request<tonic::Streaming<proto::IngestRecordsChunk>>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let start = Instant::now();
        let mut stream = request.into_inner();
        let mut assignments = Vec::new();
        let mut record_count = 0usize;

        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(err.to_string()))?
        {
            if chunk.records.is_empty() {
                continue;
            }
            record_count += chunk.records.len();
            let (tx, rx) = oneshot::channel();
            let job = IngestJob {
                records: chunk.records,
                respond_to: tx,
            };
            self.ingest_tx
                .send(job)
                .await
                .map_err(|_| Status::unavailable("ingest queue unavailable"))?;
            let batch_assignments = rx
                .await
                .map_err(|_| Status::internal("ingest worker dropped"))??;
            assignments.extend(batch_assignments);
        }

        assignments.sort_by_key(|assignment| assignment.index);
        self.metrics
            .record_ingest(record_count, start.elapsed().as_micros() as u64);
        Ok(Response::new(proto::IngestRecordsResponse { assignments }))
    }

    async fn ingest_records_from_url(
        &self,
        request: Request<proto::IngestRecordsFromUrlRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let batch = fetch_record_batch_from_url(&request.into_inner().url).await?;
        self.ingest_records(Request::new(batch)).await
    }

    async fn query_entities(
        &self,
        request: Request<proto::QueryEntitiesRequest>,
    ) -> Result<Response<proto::QueryEntitiesResponse>, Status> {
        let start = Instant::now();
        let mut unirust = self.unirust.lock().await;
        let request = request.into_inner();
        let interval = Interval::new(request.start, request.end)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;

        let descriptors = request
            .descriptors
            .iter()
            .map(|descriptor| QueryDescriptor {
                attr: unirust.intern_attr(&descriptor.attr),
                value: unirust.intern_value(&descriptor.value),
            })
            .collect::<Vec<_>>();

        let outcome = unirust
            .query_master_entities(&descriptors, interval)
            .map_err(|err| Status::internal(err.to_string()))?;

        let response = match outcome {
            QueryOutcome::Matches(matches) => proto::QueryEntitiesResponse {
                outcome: Some(proto::query_entities_response::Outcome::Matches(
                    proto::QueryMatches {
                        matches: matches
                            .into_iter()
                            .map(|entry| {
                                Self::to_proto_match(
                                    self.shard_id,
                                    entry.cluster_id,
                                    entry.interval,
                                    &entry.golden,
                                    entry.cluster_key,
                                    entry.cluster_key_identity,
                                )
                            })
                            .collect(),
                    },
                )),
            },
            QueryOutcome::Conflict(conflict) => {
                let descriptors = conflict
                    .descriptors
                    .into_iter()
                    .map(|descriptor| proto::QueryDescriptorOverlap {
                        descriptor: Some(proto::QueryDescriptor {
                            attr: unirust
                                .resolve_attr(descriptor.descriptor.attr)
                                .unwrap_or_default(),
                            value: unirust
                                .resolve_value(descriptor.descriptor.value)
                                .unwrap_or_default(),
                        }),
                        start: descriptor.interval.start,
                        end: descriptor.interval.end,
                    })
                    .collect();

                let clusters = conflict
                    .clusters
                    .into_iter()
                    .map(|cluster_id| proto::QueryMatch {
                        shard_id: self.shard_id,
                        cluster_id: cluster_id.0,
                        start: conflict.interval.start,
                        end: conflict.interval.end,
                        cluster_key: String::new(),
                        cluster_key_identity: String::new(),
                        golden: Vec::new(),
                    })
                    .collect();

                proto::QueryEntitiesResponse {
                    outcome: Some(proto::query_entities_response::Outcome::Conflict(
                        proto::QueryConflict {
                            start: conflict.interval.start,
                            end: conflict.interval.end,
                            clusters,
                            descriptors,
                        },
                    )),
                }
            }
        };

        self.metrics
            .record_query(start.elapsed().as_micros() as u64);
        Ok(Response::new(response))
    }

    async fn get_stats(
        &self,
        _request: Request<proto::StatsRequest>,
    ) -> Result<Response<proto::StatsResponse>, Status> {
        let unirust = self.unirust.lock().await;
        let clusters = unirust
            .build_clusters()
            .map_err(|err| Status::internal(err.to_string()))?;
        let observations = unirust
            .detect_conflicts(&clusters)
            .map_err(|err| Status::internal(err.to_string()))?;
        let graph = unirust
            .export_graph(&clusters, &observations)
            .map_err(|err| Status::internal(err.to_string()))?;
        let record_count = graph
            .metadata
            .get("num_records")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);

        Ok(Response::new(proto::StatsResponse {
            record_count,
            cluster_count: clusters.len() as u64,
            conflict_count: observations.len() as u64,
            graph_node_count: graph.num_nodes() as u64,
            graph_edge_count: (graph.num_same_as_edges() + graph.num_conflicts_with_edges())
                as u64,
        }))
    }

    async fn health_check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        Ok(Response::new(proto::HealthCheckResponse {
            status: "ok".to_string(),
        }))
    }

    async fn get_config_version(
        &self,
        _request: Request<proto::ConfigVersionRequest>,
    ) -> Result<Response<proto::ConfigVersionResponse>, Status> {
        Ok(Response::new(proto::ConfigVersionResponse {
            version: self.config_version.clone(),
        }))
    }

    async fn get_metrics(
        &self,
        _request: Request<proto::MetricsRequest>,
    ) -> Result<Response<proto::MetricsResponse>, Status> {
        let store_metrics = {
            let unirust = self.unirust.lock().await;
            unirust.store_metrics()
        };
        let response = proto::MetricsResponse {
            uptime_seconds: self.metrics.uptime_seconds(),
            ingest_requests: self.metrics.ingest_requests.load(Ordering::Relaxed),
            ingest_records: self.metrics.ingest_records.load(Ordering::Relaxed),
            query_requests: self.metrics.query_requests.load(Ordering::Relaxed),
            ingest_latency: Some(self.metrics.ingest_latency.snapshot()),
            query_latency: Some(self.metrics.query_latency.snapshot()),
            store: Some(store_metrics_to_proto(store_metrics)),
            shards_reporting: 1,
        };
        Ok(Response::new(response))
    }


    async fn checkpoint(
        &self,
        request: Request<proto::CheckpointRequest>,
    ) -> Result<Response<proto::CheckpointResponse>, Status> {
        let data_dir = self
            .data_dir
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("checkpoint requires --data-dir"))?;
        let target = resolve_checkpoint_path(data_dir, &request.into_inner().path)?;
        fs::create_dir_all(
            target
                .parent()
                .ok_or_else(|| Status::internal("invalid checkpoint path"))?,
        )
        .map_err(|err| Status::internal(err.to_string()))?;
        let unirust = self.unirust.lock().await;
        unirust
            .checkpoint(&target)
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(proto::CheckpointResponse {
            paths: vec![target.to_string_lossy().to_string()],
        }))
    }

    async fn get_record_id_range(
        &self,
        _request: Request<proto::RecordIdRangeRequest>,
    ) -> Result<Response<proto::RecordIdRangeResponse>, Status> {
        let unirust = self.unirust.lock().await;
        let record_count = unirust.record_count() as u64;
        let response = match unirust.record_id_bounds() {
            Some((min_id, max_id)) => proto::RecordIdRangeResponse {
                empty: false,
                min_id: min_id.0,
                max_id: max_id.0,
                record_count,
            },
            None => proto::RecordIdRangeResponse {
                empty: true,
                min_id: 0,
                max_id: 0,
                record_count: 0,
            },
        };
        Ok(Response::new(response))
    }

    async fn export_records(
        &self,
        request: Request<proto::ExportRecordsRequest>,
    ) -> Result<Response<proto::ExportRecordsResponse>, Status> {
        let request = request.into_inner();
        let limit = if request.limit == 0 {
            EXPORT_DEFAULT_LIMIT
        } else {
            request.limit as usize
        };
        let start_id = RecordId(request.start_id);
        let end_id = if request.end_id == 0 {
            RecordId(u32::MAX)
        } else {
            RecordId(request.end_id)
        };
        if start_id >= end_id {
            return Err(Status::invalid_argument("start_id must be < end_id"));
        }

        let unirust = self.unirust.lock().await;
        let mut records = unirust.records_in_id_range(start_id, end_id, limit + 1);
        let has_more = records.len() > limit;
        if has_more {
            records.truncate(limit);
        }
        let next_start_id = if has_more {
            records
                .last()
                .map(|record| record.id.0.saturating_add(1))
                .unwrap_or(request.start_id)
        } else {
            0
        };
        let response = proto::ExportRecordsResponse {
            records: records
                .iter()
                .map(|record| Self::record_to_snapshot(&unirust, record))
                .collect(),
            has_more,
            next_start_id,
        };
        Ok(Response::new(response))
    }

    async fn export_records_stream(
        &self,
        request: Request<proto::ExportRecordsRequest>,
    ) -> Result<Response<Self::ExportRecordsStreamStream>, Status> {
        let request = request.into_inner();
        let limit = if request.limit == 0 {
            EXPORT_DEFAULT_LIMIT
        } else {
            request.limit as usize
        };
        let mut start_id = request.start_id;
        let end_id = if request.end_id == 0 {
            u32::MAX
        } else {
            request.end_id
        };
        if start_id >= end_id {
            return Err(Status::invalid_argument("start_id must be < end_id"));
        }

        let unirust = self.unirust.clone();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            loop {
                let (records, has_more, next_start_id) = {
                    let unirust_guard = unirust.lock().await;
                    let mut records =
                        unirust_guard.records_in_id_range(RecordId(start_id), RecordId(end_id), limit + 1);
                    let has_more = records.len() > limit;
                    if has_more {
                        records.truncate(limit);
                    }
                    let next_start_id = if has_more {
                        records
                            .last()
                            .map(|record| record.id.0.saturating_add(1))
                            .unwrap_or(start_id)
                    } else {
                        0
                    };
                    let snapshots = records
                        .iter()
                        .map(|record| ShardNode::record_to_snapshot(&unirust_guard, record))
                        .collect::<Vec<_>>();
                    (snapshots, has_more, next_start_id)
                };

                if records.is_empty() {
                    break;
                }

                if tx
                    .send(Ok(proto::ExportRecordsChunk {
                        records,
                        has_more,
                        next_start_id,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }

                if !has_more {
                    break;
                }
                if next_start_id == 0 {
                    break;
                }
                start_id = next_start_id;
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn import_records(
        &self,
        request: Request<proto::ImportRecordsRequest>,
    ) -> Result<Response<proto::ImportRecordsResponse>, Status> {
        let request = request.into_inner();
        if request.records.is_empty() {
            return Ok(Response::new(proto::ImportRecordsResponse { imported: 0 }));
        }
        let mut unirust = self.unirust.lock().await;
        let mut records = Vec::with_capacity(request.records.len());
        for snapshot in &request.records {
            let identity = snapshot
                .identity
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("record identity is required"))?;
            records.push(Self::build_record_with_id(
                &mut unirust,
                snapshot.record_id,
                identity,
                &snapshot.descriptors,
            )?);
        }
        unirust
            .ingest(records)
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(proto::ImportRecordsResponse {
            imported: request.records.len() as u64,
        }))
    }

    async fn import_records_stream(
        &self,
        request: Request<tonic::Streaming<proto::ImportRecordsChunk>>,
    ) -> Result<Response<proto::ImportRecordsResponse>, Status> {
        let mut stream = request.into_inner();
        let mut imported = 0u64;
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(err.to_string()))?
        {
            if chunk.records.is_empty() {
                continue;
            }
            let mut unirust = self.unirust.lock().await;
            let mut records = Vec::with_capacity(chunk.records.len());
            for snapshot in &chunk.records {
                let identity = snapshot
                    .identity
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("record identity is required"))?;
                records.push(Self::build_record_with_id(
                    &mut unirust,
                    snapshot.record_id,
                    identity,
                    &snapshot.descriptors,
                )?);
            }
            unirust
                .ingest(records)
                .map_err(|err| Status::internal(err.to_string()))?;
            imported += chunk.records.len() as u64;
        }
        Ok(Response::new(proto::ImportRecordsResponse { imported }))
    }

    async fn list_conflicts(
        &self,
        request: Request<proto::ListConflictsRequest>,
    ) -> Result<Response<proto::ListConflictsResponse>, Status> {
        let unirust = self.unirust.lock().await;
        let request = request.into_inner();
        let clusters = unirust
            .build_clusters()
            .map_err(|err| Status::internal(err.to_string()))?;
        let observations = unirust
            .detect_conflicts(&clusters)
            .map_err(|err| Status::internal(err.to_string()))?;
        let mut summaries = unirust.summarize_conflicts(&observations);

        if !request.attribute.is_empty() {
            summaries.retain(|summary| summary.attribute.as_deref() == Some(&request.attribute));
        }

        if request.end > request.start {
            let filter = Interval::new(request.start, request.end)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
            summaries.retain(|summary| crate::temporal::is_overlapping(&summary.interval, &filter));
        }
        let response = proto::ListConflictsResponse {
            conflicts: summaries
                .into_iter()
                .map(|summary| to_proto_conflict_summary(summary))
                .collect(),
        };
        Ok(Response::new(response))
    }

    async fn reset(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        let config = self.ontology_config.lock().await.clone();
        if let Some(path) = &self.data_dir {
            let mut store =
                PersistentStore::open(path).map_err(|err| Status::internal(err.to_string()))?;
            store
                .reset_data()
                .map_err(|err| Status::internal(err.to_string()))?;
            store
                .save_ontology_config(&serde_json::to_vec(&config).map_err(|err| {
                    Status::internal(format!("failed to encode ontology config: {err}"))
                })?)
                .map_err(|err| Status::internal(err.to_string()))?;
            let ontology = config.build_ontology(store.inner_mut());
            store
                .persist_state()
                .map_err(|err| Status::internal(err.to_string()))?;
            let mut guard = self.unirust.lock().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        } else {
            let mut store = crate::Store::new();
            let ontology = config.build_ontology(&mut store);
            let mut guard = self.unirust.lock().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        }
        Ok(Response::new(proto::Empty {}))
    }
}

#[derive(Clone)]
pub struct RouterNode {
    shard_clients: Vec<proto::shard_service_client::ShardServiceClient<tonic::transport::Channel>>,
    ontology_config: Arc<Mutex<DistributedOntologyConfig>>,
    config_version: String,
    metrics: Arc<Metrics>,
}

impl RouterNode {
    pub async fn connect(
        shard_addrs: Vec<String>,
        ontology_config: DistributedOntologyConfig,
    ) -> Result<Self, Status> {
        Self::connect_with_version(shard_addrs, ontology_config, None).await
    }

    pub async fn connect_with_version(
        shard_addrs: Vec<String>,
        ontology_config: DistributedOntologyConfig,
        config_version: Option<String>,
    ) -> Result<Self, Status> {
        let mut shard_clients = Vec::with_capacity(shard_addrs.len());
        for addr in shard_addrs {
            let client = proto::shard_service_client::ShardServiceClient::connect(addr)
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
            shard_clients.push(client);
        }
        let config_version = config_version.unwrap_or_else(|| "unversioned".to_string());
        for client in &shard_clients {
            let mut client = client.clone();
            let version = client
                .get_config_version(Request::new(proto::ConfigVersionRequest {}))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner()
                .version;
            if version != config_version {
                return Err(Status::failed_precondition(format!(
                    "config version mismatch: router {}, shard {}",
                    config_version, version
                )));
            }
        }
        Ok(Self {
            shard_clients,
            ontology_config: Arc::new(Mutex::new(ontology_config)),
            config_version,
            metrics: Arc::new(Metrics::new()),
        })
    }

    pub async fn connect_from_file(
        path: impl AsRef<Path>,
        ontology_config: DistributedOntologyConfig,
        config_version: Option<String>,
    ) -> Result<Self, Status> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let shard_addrs = content
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    None
                } else if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
                    Some(trimmed.to_string())
                } else {
                    Some(format!("http://{}", trimmed))
                }
            })
            .collect::<Vec<_>>();
        if shard_addrs.is_empty() {
            return Err(Status::invalid_argument("no shard addresses found"));
        }
        Self::connect_with_version(shard_addrs, ontology_config, config_version).await
    }

    fn shard_client(
        &self,
        shard_id: u32,
    ) -> Result<proto::shard_service_client::ShardServiceClient<tonic::transport::Channel>, Status>
    {
        let idx = shard_id as usize;
        if idx >= self.shard_clients.len() {
            return Err(Status::invalid_argument(format!(
                "shard_id {} out of range",
                shard_id
            )));
        }
        Ok(self.shard_clients[idx].clone())
    }

    fn merge_query_responses(
        &self,
        descriptors: &[proto::QueryDescriptor],
        responses: Vec<proto::QueryEntitiesResponse>,
    ) -> proto::QueryEntitiesResponse {
        let mut matches = Vec::new();
        for response in responses {
            match response.outcome {
                Some(proto::query_entities_response::Outcome::Conflict(conflict)) => {
                    return proto::QueryEntitiesResponse {
                        outcome: Some(proto::query_entities_response::Outcome::Conflict(conflict)),
                    };
                }
                Some(proto::query_entities_response::Outcome::Matches(found)) => {
                    matches.extend(found.matches);
                }
                None => {}
            }
        }

        if matches.len() <= 1 {
            return proto::QueryEntitiesResponse {
                outcome: Some(proto::query_entities_response::Outcome::Matches(
                    proto::QueryMatches { matches },
                )),
            };
        }

        matches.sort_by(|a, b| a.start.cmp(&b.start));

        for window in matches.windows(2) {
            let current = &window[0];
            let next = &window[1];
            if current.shard_id == next.shard_id && current.cluster_id == next.cluster_id {
                continue;
            }
            if current.start < next.end && next.start < current.end {
                let overlap_start = current.start.max(next.start);
                let overlap_end = current.end.min(next.end);
                let descriptors = descriptors
                    .iter()
                    .map(|descriptor| proto::QueryDescriptorOverlap {
                        descriptor: Some(descriptor.clone()),
                        start: overlap_start,
                        end: overlap_end,
                    })
                    .collect();

                return proto::QueryEntitiesResponse {
                    outcome: Some(proto::query_entities_response::Outcome::Conflict(
                        proto::QueryConflict {
                            start: overlap_start,
                            end: overlap_end,
                            clusters: vec![current.clone(), next.clone()],
                            descriptors,
                        },
                    )),
                };
            }
        }

        proto::QueryEntitiesResponse {
            outcome: Some(proto::query_entities_response::Outcome::Matches(
                proto::QueryMatches { matches },
            )),
        }
    }
}

#[tonic::async_trait]
impl proto::router_service_server::RouterService for RouterNode {
    type ExportRecordsStreamStream =
        Pin<Box<dyn Stream<Item = Result<proto::ExportRecordsChunk, Status>> + Send + 'static>>;

    async fn set_ontology(
        &self,
        request: Request<proto::ApplyOntologyRequest>,
    ) -> Result<Response<proto::ApplyOntologyResponse>, Status> {
        let payload = request.into_inner();
        let config = payload
            .config
            .clone()
            .ok_or_else(|| Status::invalid_argument("ontology config is required"))?;
        let mapped = map_proto_config(&config);
        *self.ontology_config.lock().await = mapped;

        for client in &self.shard_clients {
            let mut client = client.clone();
            client
                .set_ontology(Request::new(payload.clone()))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
        }

        Ok(Response::new(proto::ApplyOntologyResponse {}))
    }

    async fn ingest_records(
        &self,
        request: Request<proto::IngestRecordsRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let start = Instant::now();
        let batch = request.into_inner();
        let record_count = batch.records.len();
        let shard_count = self.shard_clients.len();
        let config = self.ontology_config.lock().await.clone();
        let mut shard_batches: Vec<Vec<proto::RecordInput>> = vec![Vec::new(); shard_count];

        for record in batch.records {
            let shard_idx = hash_record_to_shard(&config, &record, shard_count);
            shard_batches[shard_idx].push(record);
        }

        let mut results: Vec<proto::IngestAssignment> = Vec::new();
        for (idx, records) in shard_batches.into_iter().enumerate() {
            if records.is_empty() {
                continue;
            }
            let mut client = self.shard_clients[idx].clone();
            let response = client
                .ingest_records(Request::new(proto::IngestRecordsRequest { records }))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
            results.extend(response.into_inner().assignments);
        }

        results.sort_by_key(|assignment| assignment.index);
        self.metrics
            .record_ingest(record_count, start.elapsed().as_micros() as u64);
        Ok(Response::new(proto::IngestRecordsResponse {
            assignments: results,
        }))
    }

    async fn ingest_records_from_url(
        &self,
        request: Request<proto::IngestRecordsFromUrlRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let batch = fetch_record_batch_from_url(&request.into_inner().url).await?;
        self.ingest_records(Request::new(batch)).await
    }

    async fn query_entities(
        &self,
        request: Request<proto::QueryEntitiesRequest>,
    ) -> Result<Response<proto::QueryEntitiesResponse>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        let mut responses = Vec::with_capacity(self.shard_clients.len());
        for client in &self.shard_clients {
            let mut client = client.clone();
            let response = client
                .query_entities(Request::new(request.clone()))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
            responses.push(response.into_inner());
        }

        let merged = self.merge_query_responses(&request.descriptors, responses);
        self.metrics
            .record_query(start.elapsed().as_micros() as u64);
        Ok(Response::new(merged))
    }

    async fn get_stats(
        &self,
        _request: Request<proto::StatsRequest>,
    ) -> Result<Response<proto::StatsResponse>, Status> {
        let mut totals = proto::StatsResponse {
            record_count: 0,
            cluster_count: 0,
            conflict_count: 0,
            graph_node_count: 0,
            graph_edge_count: 0,
        };

        for client in &self.shard_clients {
            let mut client = client.clone();
            let response = client
                .get_stats(Request::new(proto::StatsRequest {}))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            totals.record_count += response.record_count;
            totals.cluster_count += response.cluster_count;
            totals.conflict_count += response.conflict_count;
            totals.graph_node_count += response.graph_node_count;
            totals.graph_edge_count += response.graph_edge_count;
        }

        Ok(Response::new(totals))
    }

    async fn health_check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        for client in &self.shard_clients {
            let mut client = client.clone();
            client
                .health_check(Request::new(proto::HealthCheckRequest {}))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
        }

        Ok(Response::new(proto::HealthCheckResponse {
            status: "ok".to_string(),
        }))
    }

    async fn get_config_version(
        &self,
        _request: Request<proto::ConfigVersionRequest>,
    ) -> Result<Response<proto::ConfigVersionResponse>, Status> {
        Ok(Response::new(proto::ConfigVersionResponse {
            version: self.config_version.clone(),
        }))
    }

    async fn get_metrics(
        &self,
        _request: Request<proto::MetricsRequest>,
    ) -> Result<Response<proto::MetricsResponse>, Status> {
        let mut ingest_latency = empty_latency();
        let mut query_latency = empty_latency();
        let mut ingest_requests = 0u64;
        let mut ingest_records = 0u64;
        let mut query_requests = 0u64;
        let mut running_compactions = 0u64;
        let mut running_flushes = 0u64;
        let mut persistent = false;
        let mut shards_reporting = 0u32;

        for client in &self.shard_clients {
            let mut client = client.clone();
            let metrics = client
                .get_metrics(Request::new(proto::MetricsRequest {}))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            ingest_requests += metrics.ingest_requests;
            ingest_records += metrics.ingest_records;
            query_requests += metrics.query_requests;
            merge_latency(&mut ingest_latency, metrics.ingest_latency);
            merge_latency(&mut query_latency, metrics.query_latency);
            if let Some(store) = metrics.store {
                persistent |= store.persistent;
                running_compactions += store.running_compactions;
                running_flushes += store.running_flushes;
            }
            shards_reporting += 1;
        }

        if shards_reporting == 0 {
            ingest_requests = self.metrics.ingest_requests.load(Ordering::Relaxed);
            ingest_records = self.metrics.ingest_records.load(Ordering::Relaxed);
            query_requests = self.metrics.query_requests.load(Ordering::Relaxed);
            ingest_latency = self.metrics.ingest_latency.snapshot();
            query_latency = self.metrics.query_latency.snapshot();
        }

        let response = proto::MetricsResponse {
            uptime_seconds: self.metrics.uptime_seconds(),
            ingest_requests,
            ingest_records,
            query_requests,
            ingest_latency: Some(ingest_latency),
            query_latency: Some(query_latency),
            store: Some(proto::StoreMetrics {
                persistent,
                running_compactions,
                running_flushes,
            }),
            shards_reporting,
        };
        Ok(Response::new(response))
    }

    async fn get_record_id_range(
        &self,
        request: Request<proto::RouterRecordIdRangeRequest>,
    ) -> Result<Response<proto::RecordIdRangeResponse>, Status> {
        let request = request.into_inner();
        let mut client = self.shard_client(request.shard_id)?;
        let response = client
            .get_record_id_range(Request::new(proto::RecordIdRangeRequest {}))
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?
            .into_inner();
        Ok(Response::new(response))
    }

    async fn export_records(
        &self,
        request: Request<proto::RouterExportRecordsRequest>,
    ) -> Result<Response<proto::ExportRecordsResponse>, Status> {
        let request = request.into_inner();
        let mut client = self.shard_client(request.shard_id)?;
        let response = client
            .export_records(Request::new(proto::ExportRecordsRequest {
                start_id: request.start_id,
                end_id: request.end_id,
                limit: request.limit,
            }))
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?
            .into_inner();
        Ok(Response::new(response))
    }

    async fn export_records_stream(
        &self,
        request: Request<proto::RouterExportRecordsRequest>,
    ) -> Result<Response<Self::ExportRecordsStreamStream>, Status> {
        let request = request.into_inner();
        let mut client = self.shard_client(request.shard_id)?;
        let response = client
            .export_records_stream(Request::new(proto::ExportRecordsRequest {
                start_id: request.start_id,
                end_id: request.end_id,
                limit: request.limit,
            }))
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?;
        let stream = response
            .into_inner()
            .map(|item| item.map_err(|err| Status::unavailable(err.to_string())));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn import_records(
        &self,
        request: Request<proto::RouterImportRecordsRequest>,
    ) -> Result<Response<proto::ImportRecordsResponse>, Status> {
        let request = request.into_inner();
        let mut client = self.shard_client(request.shard_id)?;
        let response = client
            .import_records(Request::new(proto::ImportRecordsRequest {
                records: request.records,
            }))
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?
            .into_inner();
        Ok(Response::new(response))
    }

    async fn import_records_stream(
        &self,
        request: Request<tonic::Streaming<proto::RouterImportRecordsRequest>>,
    ) -> Result<Response<proto::ImportRecordsResponse>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let Some(first) = first else {
            return Ok(Response::new(proto::ImportRecordsResponse { imported: 0 }));
        };

        let shard_id = first.shard_id;
        let mut client = self.shard_client(shard_id)?;
        let (tx, rx) = mpsc::channel(4);
        let (err_tx, err_rx) = oneshot::channel::<Result<(), Status>>();

        tokio::spawn(async move {
            if tx
                .send(proto::ImportRecordsChunk {
                    records: first.records,
                })
                .await
                .is_err()
            {
                let _ = err_tx.send(Err(Status::unavailable("import channel closed")));
                return;
            }
            loop {
                match inbound.message().await {
                    Ok(Some(chunk)) => {
                        if chunk.shard_id != shard_id {
                            let _ = err_tx.send(Err(Status::invalid_argument(
                                "shard_id must be consistent for stream",
                            )));
                            return;
                        }
                        if tx
                            .send(proto::ImportRecordsChunk {
                                records: chunk.records,
                            })
                            .await
                            .is_err()
                        {
                            let _ = err_tx.send(Err(Status::unavailable(
                                "import channel closed",
                            )));
                            return;
                        }
                    }
                    Ok(None) => {
                        let _ = err_tx.send(Ok(()));
                        return;
                    }
                    Err(err) => {
                        let _ = err_tx.send(Err(Status::invalid_argument(err.to_string())));
                        return;
                    }
                }
            }
        });

        let response = client
            .import_records_stream(Request::new(ReceiverStream::new(rx)))
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?;
        match err_rx.await {
            Ok(Ok(())) => Ok(response),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(Status::unavailable("import stream dropped")),
        }
    }


    async fn checkpoint(
        &self,
        request: Request<proto::CheckpointRequest>,
    ) -> Result<Response<proto::CheckpointResponse>, Status> {
        let payload = request.into_inner();
        let mut paths = Vec::new();
        for client in &self.shard_clients {
            let mut client = client.clone();
            let response = client
                .checkpoint(Request::new(payload.clone()))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            paths.extend(response.paths);
        }
        Ok(Response::new(proto::CheckpointResponse { paths }))
    }

    async fn list_conflicts(
        &self,
        request: Request<proto::ListConflictsRequest>,
    ) -> Result<Response<proto::ListConflictsResponse>, Status> {
        let payload = request.into_inner();
        let mut summaries = Vec::new();
        for client in &self.shard_clients {
            let mut client = client.clone();
            let response = client
                .list_conflicts(Request::new(payload.clone()))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            summaries.extend(response.conflicts);
        }

        summaries.sort_by(|a, b| {
            (
                a.kind.clone(),
                a.attribute.clone(),
                a.start,
                a.end,
                a.cause.clone(),
                a.records
                    .iter()
                    .map(|record| format!("{}:{}", record.perspective, record.uid))
                    .collect::<Vec<_>>(),
            )
                .cmp(&(
                    b.kind.clone(),
                    b.attribute.clone(),
                    b.start,
                    b.end,
                    b.cause.clone(),
                    b.records
                        .iter()
                        .map(|record| format!("{}:{}", record.perspective, record.uid))
                        .collect::<Vec<_>>(),
                ))
        });

        Ok(Response::new(proto::ListConflictsResponse {
            conflicts: summaries,
        }))
    }

    async fn reset(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        for client in &self.shard_clients {
            let mut client = client.clone();
            client
                .reset(Request::new(proto::Empty {}))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
        }
        Ok(Response::new(proto::Empty {}))
    }
}

fn to_proto_conflict_summary(summary: ConflictSummary) -> proto::ConflictSummary {
    proto::ConflictSummary {
        kind: summary.kind,
        attribute: summary.attribute.unwrap_or_default(),
        start: summary.interval.start,
        end: summary.interval.end,
        records: summary
            .records
            .into_iter()
            .map(|record| proto::RecordRef {
                perspective: record.perspective,
                uid: record.uid,
            })
            .collect(),
        cause: summary.cause.unwrap_or_default(),
    }
}

fn store_metrics_to_proto(metrics: Option<StoreMetrics>) -> proto::StoreMetrics {
    if let Some(metrics) = metrics {
        proto::StoreMetrics {
            persistent: metrics.persistent,
            running_compactions: metrics.running_compactions,
            running_flushes: metrics.running_flushes,
        }
    } else {
        proto::StoreMetrics {
            persistent: false,
            running_compactions: 0,
            running_flushes: 0,
        }
    }
}

fn empty_latency() -> proto::LatencyMetrics {
    proto::LatencyMetrics {
        count: 0,
        total_micros: 0,
        max_micros: 0,
    }
}

fn merge_latency(acc: &mut proto::LatencyMetrics, other: Option<proto::LatencyMetrics>) {
    if let Some(other) = other {
        acc.count += other.count;
        acc.total_micros += other.total_micros;
        acc.max_micros = acc.max_micros.max(other.max_micros);
    }
}
