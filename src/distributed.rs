use crate::conflicts::ConflictSummary;
use crate::graph::GoldenDescriptor;
use crate::model::{AttrId, GlobalClusterId, Record, RecordId, RecordIdentity};
use crate::ontology::{Constraint, IdentityKey, Ontology, StrongIdentifier};
use crate::persistence::PersistentOpenOptions;
use crate::query::{QueryDescriptor, QueryOutcome};
use crate::sharding::{BloomFilter, IdentityKeySignature};
use crate::store::StoreMetrics;
use crate::temporal::Interval;
use crate::{PersistentStore, StreamingTuning, Unirust};
use anyhow::Result as AnyResult;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::{Mutex, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

#[derive(Debug, Deserialize, Serialize)]
struct JsonRecordIdentity {
    entity_type: String,
    perspective: String,
    uid: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct JsonRecordDescriptor {
    attr: String,
    value: String,
    start: i64,
    end: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct JsonRecordInput {
    index: u32,
    identity: JsonRecordIdentity,
    descriptors: Vec<JsonRecordDescriptor>,
}

struct IngestWal {
    path: PathBuf,
    temp_path: PathBuf,
}

#[allow(clippy::result_large_err)]
impl IngestWal {
    fn new(data_dir: &Path) -> Self {
        let path = data_dir.join("ingest_wal.json");
        let temp_path = data_dir.join("ingest_wal.json.tmp");
        Self { path, temp_path }
    }

    fn write_batch(&self, records: &[proto::RecordInput]) -> Result<(), Status> {
        if records.is_empty() {
            return self.clear();
        }
        let inputs: Vec<JsonRecordInput> = records
            .iter()
            .map(json_from_proto_input)
            .collect::<Result<_, Status>>()?;
        let payload =
            serde_json::to_vec(&inputs).map_err(|err| Status::internal(err.to_string()))?;
        let mut file =
            fs::File::create(&self.temp_path).map_err(|err| Status::internal(err.to_string()))?;
        file.write_all(&payload)
            .map_err(|err| Status::internal(err.to_string()))?;
        file.sync_all()
            .map_err(|err| Status::internal(err.to_string()))?;
        fs::rename(&self.temp_path, &self.path).map_err(|err| Status::internal(err.to_string()))?;
        Ok(())
    }

    fn load_batch(&self) -> Result<Option<Vec<proto::RecordInput>>, Status> {
        let source = if self.path.exists() {
            Some(self.path.as_path())
        } else if self.temp_path.exists() {
            Some(self.temp_path.as_path())
        } else {
            None
        };
        let Some(path) = source else {
            return Ok(None);
        };
        let bytes = fs::read(path).map_err(|err| Status::internal(err.to_string()))?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let inputs: Vec<JsonRecordInput> = match serde_json::from_slice(&bytes) {
            Ok(inputs) => inputs,
            Err(_) => {
                self.quarantine_corrupt()?;
                return Ok(None);
            }
        };
        let records = inputs.into_iter().map(proto_from_json_input).collect();
        Ok(Some(records))
    }

    fn replay(&self, unirust: &mut Unirust, shard_id: u32) -> Result<(), Status> {
        if let Some(records) = self.load_batch()? {
            process_ingest_batch(unirust, shard_id, &records)?;
            self.clear()?;
        }
        Ok(())
    }

    fn clear(&self) -> Result<(), Status> {
        if self.path.exists() {
            fs::remove_file(&self.path).map_err(|err| Status::internal(err.to_string()))?;
        }
        if self.temp_path.exists() {
            fs::remove_file(&self.temp_path).map_err(|err| Status::internal(err.to_string()))?;
        }
        Ok(())
    }

    fn quarantine_corrupt(&self) -> Result<(), Status> {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| Status::internal(err.to_string()))?
            .as_secs();
        let corrupt_path = self.path.with_extension(format!("json.corrupt.{}", suffix));
        if self.path.exists() {
            if let Err(err) = fs::rename(&self.path, &corrupt_path) {
                if self.path.exists() {
                    fs::remove_file(&self.path).map_err(|err| Status::internal(err.to_string()))?;
                }
                return Err(Status::internal(err.to_string()));
            }
        }
        if self.temp_path.exists() {
            fs::remove_file(&self.temp_path).map_err(|err| Status::internal(err.to_string()))?;
        }
        Ok(())
    }
}

pub mod proto {
    tonic::include_proto!("unirust");
}

/// Locality information for a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterLocality {
    /// The shard that owns this cluster.
    pub shard_id: u16,
    /// The global cluster ID.
    pub cluster_id: GlobalClusterId,
    /// When this locality was last updated.
    pub last_updated: u64,
}

/// Index for cluster-aware routing.
///
/// Tracks which shards own which identity key signatures,
/// enabling routing of related records to the same shard.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ClusterLocalityIndex {
    /// Map from identity key signature to cluster locality.
    key_to_shard: HashMap<[u8; 32], ClusterLocality>,
    /// Bloom filter for fast negative lookups.
    bloom: BloomFilter,
    /// Number of entries in the index.
    entry_count: usize,
    /// Maximum entries before cleanup.
    max_entries: usize,
}

#[allow(dead_code)]
impl ClusterLocalityIndex {
    /// Create a new locality index with default settings.
    pub fn new() -> Self {
        Self {
            key_to_shard: HashMap::new(),
            bloom: BloomFilter::new_1mb(),
            entry_count: 0,
            max_entries: 1_000_000, // 1M entries by default
        }
    }

    /// Create a locality index with custom capacity.
    pub fn with_capacity(max_entries: usize) -> Self {
        Self {
            key_to_shard: HashMap::with_capacity(max_entries / 10),
            bloom: BloomFilter::new_1mb(),
            entry_count: 0,
            max_entries,
        }
    }

    /// Register a cluster's identity key signature with a shard.
    pub fn register(
        &mut self,
        signature: IdentityKeySignature,
        shard_id: u16,
        cluster_id: GlobalClusterId,
        timestamp: u64,
    ) {
        self.bloom.insert(&signature);

        let locality = ClusterLocality {
            shard_id,
            cluster_id,
            last_updated: timestamp,
        };

        if let std::collections::hash_map::Entry::Vacant(e) = self.key_to_shard.entry(signature.0) {
            e.insert(locality);
            self.entry_count += 1;
        } else {
            // Update existing entry if newer
            if let Some(existing) = self.key_to_shard.get_mut(&signature.0) {
                if timestamp > existing.last_updated {
                    *existing = locality;
                }
            }
        }

        // Cleanup if too many entries
        if self.entry_count > self.max_entries {
            self.cleanup_stale_entries(timestamp);
        }
    }

    /// Check if a signature might be in the index (fast bloom check).
    pub fn may_contain(&self, signature: &IdentityKeySignature) -> bool {
        self.bloom.may_contain(signature)
    }

    /// Get the locality for a signature if known.
    pub fn get_locality(&self, signature: &IdentityKeySignature) -> Option<&ClusterLocality> {
        if !self.may_contain(signature) {
            return None;
        }
        self.key_to_shard.get(&signature.0)
    }

    /// Route a record to an existing cluster's shard if possible.
    ///
    /// Returns (primary_shard, optional_secondary_shard) tuple.
    /// If the signature is known, routes to the known shard.
    /// Otherwise returns None for both, indicating fallback to hash-based routing.
    pub fn route_to_cluster(&self, signature: &IdentityKeySignature) -> Option<u16> {
        self.get_locality(signature).map(|loc| loc.shard_id)
    }

    /// Remove entries older than the given threshold.
    fn cleanup_stale_entries(&mut self, current_time: u64) {
        let threshold = current_time.saturating_sub(3600); // 1 hour threshold
        self.key_to_shard
            .retain(|_, locality| locality.last_updated > threshold);
        self.entry_count = self.key_to_shard.len();

        // Note: We don't rebuild the bloom filter on cleanup
        // This may lead to some false positives, but they're harmless
    }

    /// Get the number of entries in the index.
    pub fn len(&self) -> usize {
        self.entry_count
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.key_to_shard.clear();
        self.bloom.clear();
        self.entry_count = 0;
    }

    /// Update the cluster ID for a signature after a merge.
    pub fn update_cluster_id(
        &mut self,
        signature: &IdentityKeySignature,
        new_cluster_id: GlobalClusterId,
        timestamp: u64,
    ) {
        if let Some(locality) = self.key_to_shard.get_mut(&signature.0) {
            locality.cluster_id = new_cluster_id;
            locality.last_updated = timestamp;
        }
    }
}

impl Default for ClusterLocalityIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to create an IdentityKeySignature from a proto record.
#[allow(dead_code)]
fn signature_from_proto_record(
    config: &DistributedOntologyConfig,
    record: &proto::RecordInput,
) -> Option<IdentityKeySignature> {
    let identity = record.identity.as_ref()?;
    let mut descriptors_by_attr: HashMap<&str, &str> = HashMap::new();
    for descriptor in &record.descriptors {
        descriptors_by_attr.insert(descriptor.attr.as_str(), descriptor.value.as_str());
    }

    // Try to find a matching identity key
    for key in &config.identity_keys {
        let mut has_all = true;
        let mut key_values = Vec::new();

        for attr in &key.attributes {
            if let Some(value) = descriptors_by_attr.get(attr.as_str()) {
                // Create a pseudo KeyValue for hashing
                // We use the string values directly for hashing
                key_values.push((attr.as_str(), *value));
            } else {
                has_all = false;
                break;
            }
        }

        if has_all {
            // Hash the entity type and key values
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut hasher1 = DefaultHasher::new();
            let mut hasher2 = DefaultHasher::new();

            identity.entity_type.hash(&mut hasher1);
            identity.entity_type.hash(&mut hasher2);

            for (i, (attr, value)) in key_values.iter().enumerate() {
                attr.hash(&mut hasher1);
                value.hash(&mut hasher1);
                (attr, value, i).hash(&mut hasher2);
            }

            let h1 = hasher1.finish().to_le_bytes();
            let h2 = hasher2.finish().to_le_bytes();

            let mut bytes = [0u8; 32];
            bytes[0..8].copy_from_slice(&h1);
            bytes[8..16].copy_from_slice(&h2);
            for i in 0..8 {
                bytes[16 + i] = h1[i] ^ h2[7 - i];
                bytes[24 + i] = h1[7 - i].wrapping_add(h2[i]);
            }

            return Some(IdentityKeySignature(bytes));
        }
    }

    None
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

#[allow(clippy::result_large_err)]
fn json_from_proto_input(input: &proto::RecordInput) -> Result<JsonRecordInput, Status> {
    let identity = input
        .identity
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("record identity is required"))?;
    let descriptors = input
        .descriptors
        .iter()
        .map(|descriptor| {
            Interval::new(descriptor.start, descriptor.end)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
            Ok(JsonRecordDescriptor {
                attr: descriptor.attr.clone(),
                value: descriptor.value.clone(),
                start: descriptor.start,
                end: descriptor.end,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(JsonRecordInput {
        index: input.index,
        identity: JsonRecordIdentity {
            entity_type: identity.entity_type.clone(),
            perspective: identity.perspective.clone(),
            uid: identity.uid.clone(),
        },
        descriptors,
    })
}

fn proto_from_json_input(input: JsonRecordInput) -> proto::RecordInput {
    proto::RecordInput {
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
    let inputs: Vec<JsonRecordInput> =
        serde_json::from_slice(&bytes).map_err(|err| Status::invalid_argument(err.to_string()))?;

    let records = inputs.into_iter().map(proto_from_json_input).collect();

    Ok(proto::IngestRecordsRequest { records })
}

fn hash_record_to_u64(config: &DistributedOntologyConfig, record: &proto::RecordInput) -> u64 {
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
            return state.finish();
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
            return state.finish();
        }
    }

    let mut state = std::collections::hash_map::DefaultHasher::new();
    if let Some(identity) = identity {
        identity.entity_type.hash(&mut state);
        identity.perspective.hash(&mut state);
        identity.uid.hash(&mut state);
    }
    state.finish()
}

/// Mix hash bits for better distribution with small modulo values.
/// Uses fibonacci hashing to reduce clustering.
#[inline]
fn mix_hash(hash: u64) -> u64 {
    // Fibonacci hashing: multiply by golden ratio and take high bits
    // This provides excellent distribution for small modulo values
    const GOLDEN_RATIO: u64 = 0x9E3779B97F4A7C15;
    hash.wrapping_mul(GOLDEN_RATIO)
}

pub fn hash_record_to_shard(
    config: &DistributedOntologyConfig,
    record: &proto::RecordInput,
    shard_count: usize,
) -> usize {
    let hash = hash_record_to_u64(config, record);
    let mixed = mix_hash(hash);
    // Use high bits which have better distribution after mixing
    ((mixed >> 32) as usize) % shard_count
}

fn hash_record_to_bucket(
    config: &DistributedOntologyConfig,
    record: &proto::RecordInput,
    bucket_count: usize,
) -> usize {
    let hash = hash_record_to_u64(config, record);
    let mixed = mix_hash(hash);
    ((mixed >> 32) as usize) % bucket_count
}

struct BucketAssignment {
    primary: usize,
    secondary: Option<usize>,
    dual_write_until: Option<Instant>,
    cutover_at: Option<Instant>,
}

struct ShardBalancer {
    bucket_count: usize,
    shard_count: usize,
    assignments: StdRwLock<Vec<BucketAssignment>>,
    bucket_counts: StdMutex<Vec<u64>>,
    rebalance_every: u64,
    rebalance_skew: f64,
    dual_write_for: Duration,
    cutover_after: Option<Duration>,
    ingest_counter: AtomicU64,
}

impl ShardBalancer {
    fn new(shard_count: usize) -> Option<Self> {
        let bucket_count = env_usize("UNIRUST_REBALANCE_BUCKETS", 0);
        if bucket_count == 0 || shard_count == 0 {
            return None;
        }
        let rebalance_every = env_u64("UNIRUST_REBALANCE_EVERY", 0);
        if rebalance_every == 0 {
            return None;
        }
        let rebalance_skew = env_f64("UNIRUST_REBALANCE_SKEW", 1.25);
        let dual_write_secs = env_u64("UNIRUST_REBALANCE_DUAL_WRITE_SECS", 300);
        let cutover_secs = env_u64("UNIRUST_REBALANCE_CUTOVER_SECS", dual_write_secs);
        let cutover_after = if cutover_secs == 0 {
            None
        } else {
            Some(Duration::from_secs(cutover_secs))
        };

        let mut assignments = Vec::with_capacity(bucket_count);
        for bucket in 0..bucket_count {
            assignments.push(BucketAssignment {
                primary: bucket % shard_count,
                secondary: None,
                dual_write_until: None,
                cutover_at: None,
            });
        }

        Some(Self {
            bucket_count,
            shard_count,
            assignments: StdRwLock::new(assignments),
            bucket_counts: StdMutex::new(vec![0; bucket_count]),
            rebalance_every,
            rebalance_skew,
            dual_write_for: Duration::from_secs(dual_write_secs),
            cutover_after,
            ingest_counter: AtomicU64::new(0),
        })
    }

    fn route_record(
        &self,
        config: &DistributedOntologyConfig,
        record: &proto::RecordInput,
    ) -> (usize, Option<usize>) {
        let bucket = hash_record_to_bucket(config, record, self.bucket_count);
        if let Ok(mut counts) = self.bucket_counts.lock() {
            if let Some(count) = counts.get_mut(bucket) {
                *count = count.saturating_add(1);
            }
        }

        self.maybe_rebalance();

        let now = Instant::now();
        if let Ok(mut assignments) = self.assignments.write() {
            let assignment = &mut assignments[bucket];
            if let Some(cutover_at) = assignment.cutover_at {
                if now >= cutover_at {
                    if let Some(secondary) = assignment.secondary.take() {
                        assignment.primary = secondary;
                    }
                    assignment.dual_write_until = None;
                    assignment.cutover_at = None;
                }
            }
            if assignment.cutover_at.is_none() {
                if let Some(until) = assignment.dual_write_until {
                    if now >= until {
                        assignment.secondary = None;
                        assignment.dual_write_until = None;
                    }
                }
            }
            return (assignment.primary, assignment.secondary);
        }
        (bucket % self.shard_count, None)
    }

    fn maybe_rebalance(&self) {
        if self.rebalance_every == 0 || self.bucket_count == 0 {
            return;
        }
        let count = self.ingest_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if !count.is_multiple_of(self.rebalance_every) {
            return;
        }

        let assignments = match self.assignments.read() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let counts = match self.bucket_counts.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };

        let mut shard_loads = vec![0u64; self.shard_count];
        for (bucket, assignment) in assignments.iter().enumerate() {
            if let Some(count) = counts.get(bucket) {
                shard_loads[assignment.primary] =
                    shard_loads[assignment.primary].saturating_add(*count);
            }
        }

        let (max_shard, max_load) = shard_loads
            .iter()
            .enumerate()
            .max_by_key(|(_, load)| *load)
            .map(|(idx, load)| (idx, *load))
            .unwrap_or((0, 0));
        let (min_shard, min_load) = shard_loads
            .iter()
            .enumerate()
            .min_by_key(|(_, load)| *load)
            .map(|(idx, load)| (idx, *load))
            .unwrap_or((0, 0));

        if max_shard == min_shard {
            return;
        }
        if min_load > 0 && (max_load as f64) <= (min_load as f64 * self.rebalance_skew) {
            return;
        }

        let mut candidate_bucket = None;
        let mut candidate_load = 0u64;
        for (bucket, assignment) in assignments.iter().enumerate() {
            if assignment.primary != max_shard || assignment.secondary.is_some() {
                continue;
            }
            let load = counts.get(bucket).copied().unwrap_or(0);
            if load > candidate_load {
                candidate_load = load;
                candidate_bucket = Some(bucket);
            }
        }
        drop(assignments);
        drop(counts);

        let Some(bucket) = candidate_bucket else {
            return;
        };
        let mut assignments = match self.assignments.write() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let assignment = &mut assignments[bucket];
        if assignment.primary != max_shard || assignment.secondary.is_some() {
            return;
        }
        let now = Instant::now();
        assignment.secondary = Some(min_shard);
        assignment.dual_write_until = Some(now + self.dual_write_for);
        assignment.cutover_at = self.cutover_after.map(|duration| now + duration);
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

#[derive(Clone)]
pub struct ShardNode {
    shard_id: u32,
    unirust: Arc<RwLock<Unirust>>,
    tuning: StreamingTuning,
    ontology_config: Arc<Mutex<DistributedOntologyConfig>>,
    data_dir: Option<PathBuf>,
    ingest_wal: Option<Arc<IngestWal>>,
    ingest_txs: Vec<tokio::sync::mpsc::Sender<IngestJob>>,
    config_version: String,
    metrics: Arc<Metrics>,
}

const INGEST_QUEUE_CAPACITY: usize = 128;
const DEFAULT_INGEST_WORKERS: usize = 4;
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
        // Set shard_id in tuning for boundary tracking
        let tuning = tuning.with_shard_id(shard_id as u16);
        let config_version = config_version.unwrap_or_else(|| "unversioned".to_string());
        let worker_count = ingest_worker_count();
        if let Some(path) = data_dir.clone() {
            let (store, config, ontology) =
                load_persistent_state(&path, ontology_config, repair_on_start)?;
            let ingest_wal = Some(Arc::new(IngestWal::new(&path)));
            let mut unirust = Unirust::with_store_and_tuning(ontology, store, tuning.clone());
            if let Some(wal) = ingest_wal.as_ref() {
                wal.replay(&mut unirust, shard_id)
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            }
            let unirust = Arc::new(RwLock::new(unirust));
            let ingest_txs = spawn_ingest_workers(unirust.clone(), shard_id, worker_count);
            return Ok(Self {
                shard_id,
                unirust,
                tuning,
                ontology_config: Arc::new(Mutex::new(config)),
                data_dir: Some(path),
                ingest_wal,
                ingest_txs,
                config_version,
                metrics: Arc::new(Metrics::new()),
            });
        }

        let mut store = crate::Store::new();
        let ontology = ontology_config.clone().build_ontology(&mut store);
        let ingest_wal = None;
        let unirust = Arc::new(RwLock::new(Unirust::with_store_and_tuning(
            ontology,
            store,
            tuning.clone(),
        )));
        let ingest_txs = spawn_ingest_workers(unirust.clone(), shard_id, worker_count);
        Ok(Self {
            shard_id,
            unirust,
            tuning,
            ontology_config: Arc::new(Mutex::new(ontology_config)),
            data_dir: None,
            ingest_wal,
            ingest_txs,
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
                    attr: unirust.resolve_attr(descriptor.attr).unwrap_or_default(),
                    value: unirust.resolve_value(descriptor.value).unwrap_or_default(),
                    start: descriptor.interval.start,
                    end: descriptor.interval.end,
                })
                .collect(),
        }
    }

    fn to_proto_match(
        shard_id: u32,
        cluster_id: crate::model::ClusterId,
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

#[allow(clippy::result_large_err)]
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

fn ingest_worker_count() -> usize {
    std::env::var("UNIRUST_INGEST_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_INGEST_WORKERS)
}

fn ingest_worker_index(record: &proto::RecordInput, worker_count: usize) -> usize {
    let identity = record.identity.as_ref();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    if let Some(identity) = identity {
        identity.entity_type.hash(&mut hasher);
        identity.perspective.hash(&mut hasher);
        identity.uid.hash(&mut hasher);
    } else {
        record.index.hash(&mut hasher);
    }
    (hasher.finish() as usize) % worker_count
}

fn spawn_ingest_workers(
    unirust: Arc<RwLock<Unirust>>,
    shard_id: u32,
    worker_count: usize,
) -> Vec<mpsc::Sender<IngestJob>> {
    let mut senders = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (tx, mut rx) = mpsc::channel::<IngestJob>(INGEST_QUEUE_CAPACITY);
        let worker_unirust = unirust.clone();
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                let result = {
                    let mut guard = worker_unirust.write().await;
                    process_ingest_batch(&mut guard, shard_id, &job.records)
                };
                let _ = job.respond_to.send(result);
            }
        });
        senders.push(tx);
    }
    senders
}

async fn dispatch_ingest_records(
    ingest_txs: &[mpsc::Sender<IngestJob>],
    ingest_wal: Option<&IngestWal>,
    records: Vec<proto::RecordInput>,
) -> Result<Vec<proto::IngestAssignment>, Status> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    if let Some(wal) = ingest_wal {
        wal.write_batch(&records)?;
    }

    let worker_count = ingest_txs.len().max(1);
    let mut batches = vec![Vec::new(); worker_count];
    for record in records {
        let idx = ingest_worker_index(&record, worker_count);
        batches[idx].push(record);
    }

    let mut receivers = Vec::new();
    for (idx, batch) in batches.into_iter().enumerate() {
        if batch.is_empty() {
            continue;
        }
        let (tx, rx) = oneshot::channel();
        let job = IngestJob {
            records: batch,
            respond_to: tx,
        };
        ingest_txs[idx]
            .send(job)
            .await
            .map_err(|_| Status::unavailable("ingest queue unavailable"))?;
        receivers.push(rx);
    }

    let mut assignments = Vec::new();
    for rx in receivers {
        let batch_assignments = rx
            .await
            .map_err(|_| Status::internal("ingest worker dropped"))??;
        assignments.extend(batch_assignments);
    }

    if let Some(wal) = ingest_wal {
        wal.clear()?;
    }

    assignments.sort_by_key(|assignment| assignment.index);
    Ok(assignments)
}

#[allow(clippy::result_large_err)]
fn process_ingest_batch(
    unirust: &mut Unirust,
    shard_id: u32,
    records: &[proto::RecordInput],
) -> Result<Vec<proto::IngestAssignment>, Status> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    // Build all records first, preserving original indices
    let mut record_inputs = Vec::with_capacity(records.len());
    let mut indices = Vec::with_capacity(records.len());
    for record in records {
        let record_input = ShardNode::build_record(unirust, record)?;
        record_inputs.push(record_input);
        indices.push(record.index);
    }

    // Fast path: stream_records skips graph updates and conflict detection
    // This is 10x+ faster than stream_records_update_graph
    let cluster_assignments = unirust
        .stream_records(record_inputs)
        .map_err(|err| Status::internal(err.to_string()))?;

    // Build assignments from batch results (cluster_key derived on query, not ingest)
    let mut assignments = Vec::with_capacity(cluster_assignments.len());
    for (assignment, index) in cluster_assignments.into_iter().zip(indices) {
        assignments.push(proto::IngestAssignment {
            index,
            shard_id,
            record_id: assignment.record_id.0,
            cluster_id: assignment.cluster_id.0,
            cluster_key: String::new(), // Computed on-demand at query time
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
            // Must acquire write lock and drop old Unirust BEFORE opening new store
            // to release the RocksDB file lock
            let mut guard = self.unirust.write().await;

            // Create a temporary in-memory Unirust to replace the persistent one,
            // which closes the RocksDB when dropped
            let temp_store = crate::Store::new();
            let temp_ontology = crate::Ontology::new();
            let old = std::mem::replace(
                &mut *guard,
                Unirust::with_store_and_tuning(
                    temp_ontology,
                    temp_store,
                    self.tuning.clone(),
                ),
            );
            drop(old); // Explicitly drop to close RocksDB

            // Now safe to open a new store at the same path
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
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        } else {
            let mut store = crate::Store::new();
            let ontology = config.build_ontology(&mut store);
            let mut guard = self.unirust.write().await;
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
        let assignments =
            dispatch_ingest_records(&self.ingest_txs, self.ingest_wal.as_deref(), records).await?;

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
            let batch_assignments = dispatch_ingest_records(
                &self.ingest_txs,
                self.ingest_wal.as_deref(),
                chunk.records,
            )
            .await?;
            assignments.extend(batch_assignments);
        }

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
        let mut unirust = self.unirust.write().await;
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
        let unirust = self.unirust.read().await;
        let record_count = unirust.record_count() as u64;
        let cluster_count = unirust.streaming_cluster_count().unwrap_or(0) as u64;
        let conflict_count = unirust.conflict_summary_count().unwrap_or(0) as u64;
        let (graph_node_count, graph_edge_count) = unirust.graph_counts().unwrap_or((0, 0));

        Ok(Response::new(proto::StatsResponse {
            record_count,
            cluster_count,
            conflict_count,
            graph_node_count,
            graph_edge_count,
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
            let unirust = self.unirust.read().await;
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
        let unirust = self.unirust.read().await;
        unirust
            .checkpoint_to_path(&target)
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(proto::CheckpointResponse {
            paths: vec![target.to_string_lossy().to_string()],
        }))
    }

    async fn get_record_id_range(
        &self,
        _request: Request<proto::RecordIdRangeRequest>,
    ) -> Result<Response<proto::RecordIdRangeResponse>, Status> {
        let unirust = self.unirust.read().await;
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

        let unirust = self.unirust.read().await;
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
                    let unirust_guard = unirust.read().await;
                    let mut records = unirust_guard.records_in_id_range(
                        RecordId(start_id),
                        RecordId(end_id),
                        limit + 1,
                    );
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
        let mut unirust = self.unirust.write().await;
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
            let mut unirust = self.unirust.write().await;
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
        let request = request.into_inner();
        let mut summaries = if let Some(cache) = {
            let guard = self.unirust.read().await;
            guard.cached_conflict_summaries()
        } {
            cache
        } else if let Some(persisted) = {
            let guard = self.unirust.read().await;
            guard.load_conflict_summaries()
        } {
            let mut unirust = self.unirust.write().await;
            unirust.set_conflict_cache(persisted.clone());
            persisted
        } else {
            let mut unirust = self.unirust.write().await;
            let clusters = unirust
                .build_clusters()
                .map_err(|err| Status::internal(err.to_string()))?;
            let observations = unirust
                .detect_conflicts(&clusters)
                .map_err(|err| Status::internal(err.to_string()))?;
            let summaries = unirust.summarize_conflicts(&observations);
            unirust.set_conflict_cache(summaries.clone());
            summaries
        };

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
                .map(to_proto_conflict_summary)
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
            let mut guard = self.unirust.write().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        } else {
            let mut store = crate::Store::new();
            let ontology = config.build_ontology(&mut store);
            let mut guard = self.unirust.write().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn get_boundary_metadata(
        &self,
        request: Request<proto::GetBoundaryMetadataRequest>,
    ) -> Result<Response<proto::GetBoundaryMetadataResponse>, Status> {
        let since_version = request.into_inner().since_version;

        // Export boundary metadata from the streaming linker
        let unirust = self.unirust.read().await;
        let boundary_index = unirust.export_boundary_index();

        let metadata = match boundary_index {
            Some(index) => {
                let exported = index.export_metadata();
                // Only return if version is newer than requested
                if exported.version <= since_version {
                    proto::BoundaryMetadata {
                        shard_id: self.shard_id,
                        version: exported.version,
                        entries: Vec::new(),
                    }
                } else {
                    proto::BoundaryMetadata {
                        shard_id: self.shard_id,
                        version: exported.version,
                        entries: exported
                            .entries
                            .into_iter()
                            .map(|(sig, entries)| proto::BoundaryKeyEntries {
                                signature: Some(proto::IdentityKeySignature {
                                    signature: sig.0.to_vec(),
                                }),
                                entries: entries
                                    .into_iter()
                                    .map(|e| proto::ClusterBoundaryEntry {
                                        cluster_id: Some(proto::GlobalClusterId {
                                            shard_id: e.cluster_id.shard_id as u32,
                                            local_id: e.cluster_id.local_id,
                                            version: e.cluster_id.version as u32,
                                        }),
                                        interval_start: e.interval.start,
                                        interval_end: e.interval.end,
                                        shard_id: e.shard_id as u32,
                                    })
                                    .collect(),
                            })
                            .collect(),
                    }
                }
            }
            None => proto::BoundaryMetadata {
                shard_id: self.shard_id,
                version: 0,
                entries: Vec::new(),
            },
        };

        Ok(Response::new(proto::GetBoundaryMetadataResponse {
            metadata: Some(metadata),
        }))
    }

    async fn apply_merge(
        &self,
        request: Request<proto::ApplyMergeRequest>,
    ) -> Result<Response<proto::ApplyMergeResponse>, Status> {
        let req = request.into_inner();

        let primary = req
            .primary
            .ok_or_else(|| Status::invalid_argument("primary cluster ID is required"))?;
        let secondary = req
            .secondary
            .ok_or_else(|| Status::invalid_argument("secondary cluster ID is required"))?;

        let primary_id = GlobalClusterId::new(
            primary.shard_id as u16,
            primary.local_id,
            primary.version as u16,
        );
        let secondary_id = GlobalClusterId::new(
            secondary.shard_id as u16,
            secondary.local_id,
            secondary.version as u16,
        );

        // Only apply if we own one of the clusters
        if primary_id.shard_id != self.shard_id as u16
            && secondary_id.shard_id != self.shard_id as u16
        {
            return Ok(Response::new(proto::ApplyMergeResponse {
                success: true,
                records_updated: 0,
                error: String::new(),
            }));
        }

        // Get write lock on unirust
        let mut unirust = self.unirust.write().await;

        // Apply the merge via the streaming linker's DSU
        let records_updated = match unirust.apply_cross_shard_merge(primary_id, secondary_id) {
            Ok(count) => count,
            Err(err) => {
                return Ok(Response::new(proto::ApplyMergeResponse {
                    success: false,
                    records_updated: 0,
                    error: err.to_string(),
                }));
            }
        };

        Ok(Response::new(proto::ApplyMergeResponse {
            success: true,
            records_updated: records_updated as u32,
            error: String::new(),
        }))
    }
}

#[derive(Clone)]
pub struct RouterNode {
    shard_clients: Vec<proto::shard_service_client::ShardServiceClient<tonic::transport::Channel>>,
    /// RwLock for ontology config - read-heavy, written only during set_ontology
    ontology_config: Arc<RwLock<DistributedOntologyConfig>>,
    config_version: String,
    metrics: Arc<Metrics>,
    balancer: Option<Arc<ShardBalancer>>,
    /// Cluster locality index for cluster-aware routing.
    locality_index: Arc<StdRwLock<ClusterLocalityIndex>>,
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
        let shard_count = shard_addrs.len();
        let mut shard_clients = Vec::with_capacity(shard_count);
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
            ontology_config: Arc::new(RwLock::new(ontology_config)),
            config_version,
            metrics: Arc::new(Metrics::new()),
            balancer: ShardBalancer::new(shard_count).map(Arc::new),
            locality_index: Arc::new(StdRwLock::new(ClusterLocalityIndex::new())),
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

    #[allow(clippy::result_large_err)]
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

    /// Get read access to the cluster locality index.
    pub fn locality_index(&self) -> std::sync::RwLockReadGuard<'_, ClusterLocalityIndex> {
        self.locality_index
            .read()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Register a cluster's identity key signature with a shard.
    pub fn register_cluster_locality(
        &self,
        signature: IdentityKeySignature,
        shard_id: u16,
        cluster_id: GlobalClusterId,
    ) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        if let Ok(mut index) = self.locality_index.write() {
            index.register(signature, shard_id, cluster_id, timestamp);
        }
    }

    /// Update locality index from ingestion assignments.
    fn update_locality_from_assignments(
        &self,
        config: &DistributedOntologyConfig,
        records: &[proto::RecordInput],
        assignments: &[proto::IngestAssignment],
    ) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut index = match self.locality_index.write() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };

        // Build a map of record index to assignment
        let assignment_map: HashMap<u32, &proto::IngestAssignment> =
            assignments.iter().map(|a| (a.index, a)).collect();

        for record in records {
            if let Some(sig) = signature_from_proto_record(config, record) {
                if let Some(assignment) = assignment_map.get(&record.index) {
                    let global_id = GlobalClusterId::new(
                        assignment.shard_id as u16,
                        assignment.cluster_id,
                        0, // version 0 for initial assignments
                    );
                    index.register(sig, assignment.shard_id as u16, global_id, timestamp);
                }
            }
        }
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
        *self.ontology_config.write().await = mapped;

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
        let all_records = batch.records.clone(); // Keep for locality update
        let record_count = all_records.len();
        let shard_count = self.shard_clients.len();
        let config = self.ontology_config.read().await.clone();
        let mut shard_batches: Vec<Vec<proto::RecordInput>> = vec![Vec::new(); shard_count];
        let mut shadow_batches: Vec<Vec<proto::RecordInput>> = vec![Vec::new(); shard_count];

        // Route all records synchronously before any await
        // Use a scope to ensure the lock is released before await
        {
            let locality_index = self
                .locality_index
                .read()
                .unwrap_or_else(|e| e.into_inner());

            for record in batch.records {
                // First, try cluster-aware routing via the locality index
                let cluster_aware_shard = signature_from_proto_record(&config, &record)
                    .and_then(|sig| locality_index.route_to_cluster(&sig))
                    .map(|shard_id| shard_id as usize)
                    .filter(|&idx| idx < shard_count);

                if let Some(shard_idx) = cluster_aware_shard {
                    // Route to the known cluster's shard
                    shard_batches[shard_idx].push(record);
                } else if let Some(balancer) = &self.balancer {
                    // Fall back to load-balanced routing
                    let (primary, secondary) = balancer.route_record(&config, &record);
                    if let Some(secondary) = secondary {
                        shard_batches[primary].push(record.clone());
                        shadow_batches[secondary].push(record);
                    } else {
                        shard_batches[primary].push(record);
                    }
                } else {
                    // Fall back to hash-based routing
                    let shard_idx = hash_record_to_shard(&config, &record, shard_count);
                    shard_batches[shard_idx].push(record);
                }
            }
        } // locality_index lock released here

        // Parallel shard ingest - spawn all shard requests concurrently
        let shard_futures: Vec<_> = shard_batches
            .into_iter()
            .enumerate()
            .filter(|(_, records)| !records.is_empty())
            .map(|(idx, records)| {
                let mut client = self.shard_clients[idx].clone();
                async move {
                    client
                        .ingest_records(Request::new(proto::IngestRecordsRequest { records }))
                        .await
                }
            })
            .collect();

        let shard_responses = futures::future::join_all(shard_futures).await;
        let mut results: Vec<proto::IngestAssignment> = Vec::new();
        for response in shard_responses {
            match response {
                Ok(resp) => results.extend(resp.into_inner().assignments),
                Err(err) => return Err(Status::unavailable(err.to_string())),
            }
        }

        // Update locality index with the new assignments
        self.update_locality_from_assignments(&config, &all_records, &results);

        // Shadow writes in parallel (fire-and-forget style, but await completion)
        let shadow_futures: Vec<_> = shadow_batches
            .into_iter()
            .enumerate()
            .filter(|(_, records)| !records.is_empty())
            .map(|(idx, records)| {
                let mut client = self.shard_clients[idx].clone();
                async move {
                    let _ = client
                        .ingest_records(Request::new(proto::IngestRecordsRequest { records }))
                        .await;
                }
            })
            .collect();
        futures::future::join_all(shadow_futures).await;

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
        let mut block_cache_capacity_bytes = 0u64;
        let mut block_cache_usage_bytes = 0u64;
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
                block_cache_capacity_bytes += store.block_cache_capacity_bytes;
                block_cache_usage_bytes += store.block_cache_usage_bytes;
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
                block_cache_capacity_bytes,
                block_cache_usage_bytes,
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
                            let _ = err_tx.send(Err(Status::unavailable("import channel closed")));
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

    async fn reconcile(
        &self,
        request: Request<proto::ReconcileRequest>,
    ) -> Result<Response<proto::ReconcileResponse>, Status> {
        let req = request.into_inner();

        // Collect boundary metadata from all shards if not provided
        let shard_metadata = if req.shard_metadata.is_empty() {
            let mut metadata = Vec::new();
            for client in &self.shard_clients {
                let mut client = client.clone();
                if let Ok(response) = client
                    .get_boundary_metadata(Request::new(proto::GetBoundaryMetadataRequest {
                        since_version: 0,
                    }))
                    .await
                {
                    if let Some(m) = response.into_inner().metadata {
                        metadata.push(m);
                    }
                }
            }
            metadata
        } else {
            req.shard_metadata
        };

        // Use IncrementalReconciler to find cross-shard merges
        let mut reconciler = crate::sharding::IncrementalReconciler::new();

        for metadata in &shard_metadata {
            let mut boundary_index =
                crate::sharding::ClusterBoundaryIndex::new_small(metadata.shard_id as u16);

            for key_entries in &metadata.entries {
                if let Some(sig_proto) = &key_entries.signature {
                    if sig_proto.signature.len() == 32 {
                        let mut sig_bytes = [0u8; 32];
                        sig_bytes.copy_from_slice(&sig_proto.signature);
                        let sig = IdentityKeySignature::from_bytes(sig_bytes);

                        for entry in &key_entries.entries {
                            if let Some(cluster_id_proto) = &entry.cluster_id {
                                let cluster_id = GlobalClusterId::new(
                                    cluster_id_proto.shard_id as u16,
                                    cluster_id_proto.local_id,
                                    cluster_id_proto.version as u16,
                                );
                                if let Ok(interval) = crate::temporal::Interval::new(
                                    entry.interval_start,
                                    entry.interval_end,
                                ) {
                                    boundary_index.register_boundary_key(sig, cluster_id, interval);
                                }
                            }
                        }
                    }
                }
            }

            reconciler.add_shard_boundary(boundary_index);
        }

        let result = reconciler.reconcile();

        // Apply merges to affected shards
        let mut total_records_updated = 0u32;
        for (primary, secondary) in &result.merged_clusters {
            // Apply to primary's shard
            if let Ok(mut client) = self.shard_client(primary.shard_id as u32) {
                let _ = client
                    .apply_merge(Request::new(proto::ApplyMergeRequest {
                        primary: Some(proto::GlobalClusterId {
                            shard_id: primary.shard_id as u32,
                            local_id: primary.local_id,
                            version: primary.version as u32,
                        }),
                        secondary: Some(proto::GlobalClusterId {
                            shard_id: secondary.shard_id as u32,
                            local_id: secondary.local_id,
                            version: secondary.version as u32,
                        }),
                    }))
                    .await
                    .map(|resp| total_records_updated += resp.into_inner().records_updated);
            }

            // Apply to secondary's shard if different
            if secondary.shard_id != primary.shard_id {
                if let Ok(mut client) = self.shard_client(secondary.shard_id as u32) {
                    let _ = client
                        .apply_merge(Request::new(proto::ApplyMergeRequest {
                            primary: Some(proto::GlobalClusterId {
                                shard_id: primary.shard_id as u32,
                                local_id: primary.local_id,
                                version: primary.version as u32,
                            }),
                            secondary: Some(proto::GlobalClusterId {
                                shard_id: secondary.shard_id as u32,
                                local_id: secondary.local_id,
                                version: secondary.version as u32,
                            }),
                        }))
                        .await
                        .map(|resp| total_records_updated += resp.into_inner().records_updated);
                }
            }
        }

        let merges = result
            .merged_clusters
            .iter()
            .map(|(primary, secondary)| proto::ClusterMerge {
                primary: Some(proto::GlobalClusterId {
                    shard_id: primary.shard_id as u32,
                    local_id: primary.local_id,
                    version: primary.version as u32,
                }),
                secondary: Some(proto::GlobalClusterId {
                    shard_id: secondary.shard_id as u32,
                    local_id: secondary.local_id,
                    version: secondary.version as u32,
                }),
            })
            .collect();

        Ok(Response::new(proto::ReconcileResponse {
            merges_performed: result.merges_performed as u32,
            keys_checked: result.keys_checked as u32,
            keys_matched: result.keys_matched as u32,
            merges,
        }))
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
            block_cache_capacity_bytes: metrics.block_cache_capacity_bytes,
            block_cache_usage_bytes: metrics.block_cache_usage_bytes,
        }
    } else {
        proto::StoreMetrics {
            persistent: false,
            running_compactions: 0,
            running_flushes: 0,
            block_cache_capacity_bytes: 0,
            block_cache_usage_bytes: 0,
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
