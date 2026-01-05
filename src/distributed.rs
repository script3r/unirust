use crate::conflicts::ConflictSummary;
use crate::graph::GoldenDescriptor;
use crate::model::{AttrId, GlobalClusterId, Record, RecordId, RecordIdentity};
use crate::ontology::{Constraint, IdentityKey, Ontology, StrongIdentifier};
use crate::partitioned::{ParallelPartitionedUnirust, PartitionConfig};
use crate::perf::ConcurrentInterner;
use crate::persistence::PersistentOpenOptions;
use crate::query::{QueryDescriptor, QueryOutcome};
use crate::sharding::{BloomFilter, IdentityKeySignature};
use crate::store::StoreMetrics;
use crate::temporal::Interval;
use crate::{PersistentStore, StreamingTuning, Unirust};
use anyhow::Result as AnyResult;
use lru::LruCache;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::{Mutex, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

/// WAL record format for binary serialization.
#[derive(Debug, Deserialize, Serialize)]
struct WalRecordIdentity {
    entity_type: String,
    perspective: String,
    uid: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct WalRecordDescriptor {
    attr: String,
    value: String,
    start: i64,
    end: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct WalRecordInput {
    index: u32,
    identity: WalRecordIdentity,
    descriptors: Vec<WalRecordDescriptor>,
}

struct IngestWal {
    path: PathBuf,
    temp_path: PathBuf,
}

#[allow(clippy::result_large_err)]
impl IngestWal {
    fn new(data_dir: &Path) -> Self {
        let path = data_dir.join("ingest_wal.bin");
        let temp_path = data_dir.join("ingest_wal.bin.tmp");
        Self { path, temp_path }
    }

    fn write_batch(&self, records: &[proto::RecordInput]) -> Result<(), Status> {
        if records.is_empty() {
            return self.clear();
        }
        let inputs: Vec<WalRecordInput> = records
            .iter()
            .map(wal_from_proto_input)
            .collect::<Result<_, Status>>()?;
        let payload =
            bincode::serialize(&inputs).map_err(|err| Status::internal(err.to_string()))?;
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
        let inputs: Vec<WalRecordInput> = match bincode::deserialize(&bytes) {
            Ok(inputs) => inputs,
            Err(_) => {
                self.quarantine_corrupt()?;
                return Ok(None);
            }
        };
        let records = inputs.into_iter().map(proto_from_wal_input).collect();
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
        let corrupt_path = self.path.with_extension(format!("bin.corrupt.{}", suffix));
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
pub struct ClusterLocalityIndex {
    /// Map from identity key signature to cluster locality.
    key_to_shard: LruCache<[u8; 32], ClusterLocality>,
    /// Bloom filter for fast negative lookups.
    bloom: BloomFilter,
}

impl ClusterLocalityIndex {
    /// Create a new locality index with default settings.
    pub fn new() -> Self {
        Self {
            key_to_shard: LruCache::new(
                NonZeroUsize::new(1_000_000).expect("locality cache capacity"),
            ),
            bloom: BloomFilter::new_1mb(),
        }
    }

    /// Create a locality index with custom capacity.
    pub fn with_capacity(max_entries: usize) -> Self {
        let capacity = NonZeroUsize::new(max_entries.max(1)).expect("locality cache capacity");
        Self {
            key_to_shard: LruCache::new(capacity),
            bloom: BloomFilter::new_1mb(),
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

        if let Some(existing) = self.key_to_shard.get_mut(&signature.0) {
            if timestamp > existing.last_updated {
                *existing = locality;
            }
        } else {
            self.key_to_shard.put(signature.0, locality);
        }
    }

    /// Check if a signature might be in the index (fast bloom check).
    pub fn may_contain(&self, signature: &IdentityKeySignature) -> bool {
        self.bloom.may_contain(signature)
    }

    /// Get the locality for a signature if known.
    pub fn get_locality(&mut self, signature: &IdentityKeySignature) -> Option<ClusterLocality> {
        if !self.may_contain(signature) {
            return None;
        }
        self.key_to_shard.get(&signature.0).copied()
    }

    /// Route a record to an existing cluster's shard if possible.
    ///
    /// Returns (primary_shard, optional_secondary_shard) tuple.
    /// If the signature is known, routes to the known shard.
    /// Otherwise returns None for both, indicating fallback to hash-based routing.
    pub fn route_to_cluster(&mut self, signature: &IdentityKeySignature) -> Option<u16> {
        self.get_locality(signature).map(|loc| loc.shard_id)
    }

    /// Get the number of entries in the index.
    pub fn len(&self) -> usize {
        self.key_to_shard.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.key_to_shard.is_empty()
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.key_to_shard.clear();
        self.bloom.clear();
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

/// Cache-line aligned latency counters to prevent false sharing.
/// Each counter is on its own 64-byte cache line for maximum throughput.
#[repr(C, align(64))]
#[derive(Debug)]
struct AlignedLatencyCounters {
    count: AtomicU64,
    _pad1: [u8; 56],
    total_micros: AtomicU64,
    _pad2: [u8; 56],
    max_micros: AtomicU64,
    _pad3: [u8; 56],
}

impl Default for AlignedLatencyCounters {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            _pad1: [0u8; 56],
            total_micros: AtomicU64::new(0),
            _pad2: [0u8; 56],
            max_micros: AtomicU64::new(0),
            _pad3: [0u8; 56],
        }
    }
}

impl AlignedLatencyCounters {
    #[inline]
    fn record(&self, micros: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_micros.fetch_add(micros, Ordering::Relaxed);
        let mut current = self.max_micros.load(Ordering::Relaxed);
        while micros > current {
            match self.max_micros.compare_exchange_weak(
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

/// high-throughput-optimized metrics with cache-line aligned counters.
/// Eliminates false sharing when multiple threads update metrics concurrently.
#[repr(C)]
#[derive(Debug)]
struct PerfMetrics {
    start: Instant,
    // Each counter on its own cache line
    ingest_requests: crate::perf::AlignedCounter,
    ingest_records: crate::perf::AlignedCounter,
    query_requests: crate::perf::AlignedCounter,
    ingest_latency: AlignedLatencyCounters,
    query_latency: AlignedLatencyCounters,
    // Cross-shard reconciliation stats (tracked at router level)
    cross_shard_conflicts: crate::perf::AlignedCounter,
}

impl PerfMetrics {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            ingest_requests: crate::perf::AlignedCounter::new(),
            ingest_records: crate::perf::AlignedCounter::new(),
            query_requests: crate::perf::AlignedCounter::new(),
            ingest_latency: AlignedLatencyCounters::default(),
            query_latency: AlignedLatencyCounters::default(),
            cross_shard_conflicts: crate::perf::AlignedCounter::new(),
        }
    }

    #[inline]
    fn record_ingest(&self, record_count: usize, micros: u64) {
        self.ingest_requests.increment();
        self.ingest_records
            .fetch_add(record_count as u64, Ordering::Relaxed);
        self.ingest_latency.record(micros);
    }

    #[inline]
    fn record_query(&self, micros: u64) {
        self.query_requests.increment();
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
            // Store both AttrIds and string names for partitioning support
            ontology.add_identity_key(IdentityKey::with_attribute_names(
                attrs,
                key.attributes.clone(),
                key.name.clone(),
            ));
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

    /// Build ontology using a ConcurrentInterner for partitioned mode.
    /// This ensures AttrIds match between ontology and records built with the same interner.
    pub fn build_ontology_with_interner(&self, interner: &ConcurrentInterner) -> Ontology {
        let mut ontology = Ontology::new();

        for key in &self.identity_keys {
            let attrs: Vec<AttrId> = key
                .attributes
                .iter()
                .map(|attr| interner.intern_attr(attr))
                .collect();
            // Store both AttrIds and string names for partitioning support
            ontology.add_identity_key(IdentityKey::with_attribute_names(
                attrs,
                key.attributes.clone(),
                key.name.clone(),
            ));
        }

        for attr in &self.strong_identifiers {
            let attr_id = interner.intern_attr(attr);
            ontology
                .add_strong_identifier(StrongIdentifier::new(attr_id, format!("{attr}_strong")));
        }

        for constraint in &self.constraints {
            let attr_id = interner.intern_attr(&constraint.attribute);
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
fn wal_from_proto_input(input: &proto::RecordInput) -> Result<WalRecordInput, Status> {
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
            Ok(WalRecordDescriptor {
                attr: descriptor.attr.clone(),
                value: descriptor.value.clone(),
                start: descriptor.start,
                end: descriptor.end,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(WalRecordInput {
        index: input.index,
        identity: WalRecordIdentity {
            entity_type: identity.entity_type.clone(),
            perspective: identity.perspective.clone(),
            uid: identity.uid.clone(),
        },
        descriptors,
    })
}

fn proto_from_wal_input(input: WalRecordInput) -> proto::RecordInput {
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

fn hash_record_to_u64(config: &DistributedOntologyConfig, record: &proto::RecordInput) -> u64 {
    let identity = record.identity.as_ref();
    let mut descriptors_by_attr: HashMap<&str, &str> = HashMap::new();
    for descriptor in &record.descriptors {
        descriptors_by_attr
            .entry(descriptor.attr.as_str())
            .or_insert(descriptor.value.as_str());
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

#[derive(Clone)]
pub struct ShardNode {
    shard_id: u32,
    /// Uses parking_lot RwLock (faster for short critical sections than tokio's async RwLock)
    unirust: Arc<parking_lot::RwLock<Unirust>>,
    /// Partitioned Unirust for high-throughput parallel processing (optional)
    /// Uses per-partition locks for TRUE parallel processing - no global lock!
    /// Wrapped in RwLock to allow rebuilding when ontology changes
    /// Inner Arc allows cloning for use across await points
    partitioned: Arc<parking_lot::RwLock<Option<Arc<ParallelPartitionedUnirust>>>>,
    /// Concurrent interner for lock-free record building
    concurrent_interner: Arc<ConcurrentInterner>,
    tuning: StreamingTuning,
    ontology_config: Arc<Mutex<DistributedOntologyConfig>>,
    data_dir: Option<PathBuf>,
    ingest_wal: Option<Arc<IngestWal>>,
    ingest_wal_lock: Arc<Mutex<()>>,
    ingest_txs: Vec<tokio::sync::mpsc::Sender<IngestJob>>,
    config_version: String,
    metrics: Arc<PerfMetrics>,
    /// Cross-shard conflicts detected during reconciliation.
    /// These are indirect conflicts where clusters on different shards share
    /// an identity key but have conflicting strong identifiers.
    cross_shard_conflicts: Arc<parking_lot::RwLock<Vec<crate::sharding::CrossShardConflict>>>,
}

const INGEST_QUEUE_CAPACITY: usize = 1024; // Increased from 128 for high-throughput
const DEFAULT_INGEST_WORKERS: usize = 32; // Increased from 16 for high-throughput
const EXPORT_DEFAULT_LIMIT: usize = 1000;

/// Check if partitioned processing is enabled (default: true)
/// Set UNIRUST_PARTITIONED=0 to disable
fn is_partitioned_enabled() -> bool {
    std::env::var("UNIRUST_PARTITIONED")
        .map(|v| v != "0" && v.to_lowercase() != "false")
        .unwrap_or(true)
}

/// Get the number of partitions (defaults to CPU count or 8)
fn partition_count() -> usize {
    std::env::var("UNIRUST_PARTITION_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(8)
        })
}

// Helper macros for lock acquisition (parking_lot RwLock - synchronous)
macro_rules! write_unirust {
    ($self:expr) => {
        $self.unirust.write()
    };
}

macro_rules! read_unirust {
    ($self:expr) => {
        $self.unirust.read()
    };
}

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
        // Set shard_id in tuning and enable boundary tracking for cross-shard reconciliation.
        // Boundary tracking is REQUIRED for distributed mode - it enables cross-shard conflict
        // detection. Without it, conflicts between records on different shards go undetected.
        let tuning = tuning
            .with_shard_id(shard_id as u16)
            .with_boundary_tracking(true);

        // Defensive check: fail fast if boundary tracking is somehow disabled.
        // This should never happen since we just enabled it, but guards against
        // future refactoring that might accidentally break this invariant.
        if !tuning.enable_boundary_tracking {
            anyhow::bail!(
                "ShardNode requires enable_boundary_tracking=true for cross-shard conflict detection. \
                 Distributed mode cannot safely operate without boundary tracking enabled."
            );
        }

        let config_version = config_version.unwrap_or_else(|| "unversioned".to_string());
        let worker_count = ingest_worker_count();
        let use_partitioned = is_partitioned_enabled();
        let num_partitions = partition_count();

        if use_partitioned {
            tracing::info!(shard_id, num_partitions, "Partitioned processing enabled");
        }

        if let Some(path) = data_dir.clone() {
            let (store, config, ontology) =
                load_persistent_state(&path, ontology_config, repair_on_start)?;
            let ingest_wal = Some(Arc::new(IngestWal::new(&path)));
            let mut unirust =
                Unirust::with_store_and_tuning(ontology.clone(), store, tuning.clone());
            if let Some(wal) = ingest_wal.as_ref() {
                wal.replay(&mut unirust, shard_id)
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            }
            let unirust = Arc::new(parking_lot::RwLock::new(unirust));

            // Create partitioned processor if enabled - no RwLock needed!
            // ParallelPartitionedUnirust has per-partition Mutexes for true parallelism
            let partitioned = if use_partitioned {
                let partition_config = PartitionConfig::for_cores(num_partitions);
                let partitioned_unirust = ParallelPartitionedUnirust::new(
                    partition_config,
                    Arc::new(ontology),
                    tuning.clone(),
                )?;
                Some(Arc::new(partitioned_unirust))
            } else {
                None
            };

            let ingest_txs = spawn_ingest_workers(unirust.clone(), shard_id, worker_count);
            return Ok(Self {
                shard_id,
                unirust,
                partitioned: Arc::new(parking_lot::RwLock::new(partitioned)),
                concurrent_interner: Arc::new(ConcurrentInterner::new()),
                tuning,
                ontology_config: Arc::new(Mutex::new(config)),
                data_dir: Some(path),
                ingest_wal,
                ingest_wal_lock: Arc::new(Mutex::new(())),
                ingest_txs,
                config_version,
                metrics: Arc::new(PerfMetrics::new()),
                cross_shard_conflicts: Arc::new(parking_lot::RwLock::new(Vec::new())),
            });
        }

        // Create concurrent interner FIRST - used for both ontology and records
        let concurrent_interner = Arc::new(ConcurrentInterner::new());

        // Build ontology using the concurrent interner for partitioned mode
        // This ensures AttrIds match between ontology and records
        let ontology = if use_partitioned {
            ontology_config
                .clone()
                .build_ontology_with_interner(&concurrent_interner)
        } else {
            let mut store = crate::Store::new();
            ontology_config.clone().build_ontology(&mut store)
        };

        let store = crate::Store::new();
        let ingest_wal = None;
        let unirust = Arc::new(parking_lot::RwLock::new(Unirust::with_store_and_tuning(
            ontology.clone(),
            store,
            tuning.clone(),
        )));

        // Create partitioned processor if enabled - no RwLock needed!
        // ParallelPartitionedUnirust has per-partition Mutexes for true parallelism
        let partitioned = if use_partitioned {
            let partition_config = PartitionConfig::for_cores(num_partitions);
            let partitioned_unirust = ParallelPartitionedUnirust::new(
                partition_config,
                Arc::new(ontology),
                tuning.clone(),
            )?;
            Some(Arc::new(partitioned_unirust))
        } else {
            None
        };

        let ingest_txs = spawn_ingest_workers(unirust.clone(), shard_id, worker_count);
        Ok(Self {
            shard_id,
            unirust,
            partitioned: Arc::new(parking_lot::RwLock::new(partitioned)),
            concurrent_interner, // Use the same interner used for ontology
            tuning,
            ontology_config: Arc::new(Mutex::new(ontology_config)),
            data_dir: None,
            ingest_wal,
            ingest_wal_lock: Arc::new(Mutex::new(())),
            ingest_txs,
            config_version,
            metrics: Arc::new(PerfMetrics::new()),
            cross_shard_conflicts: Arc::new(parking_lot::RwLock::new(Vec::new())),
        })
    }

    /// Build a record using the concurrent interner - NO LOCK REQUIRED!
    #[allow(clippy::result_large_err)]
    fn build_record_concurrent(
        interner: &ConcurrentInterner,
        input: &proto::RecordInput,
    ) -> Result<Record, Status> {
        let identity = input
            .identity
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("record identity is required"))?;

        let descriptors = input
            .descriptors
            .iter()
            .map(|desc| {
                let attr = interner.intern_attr(&desc.attr);
                let value = interner.intern_value(&desc.value);
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

/// Spawn ingest workers using parking_lot::RwLock for faster locking.
fn spawn_ingest_workers(
    unirust: Arc<parking_lot::RwLock<Unirust>>,
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
                    let mut guard = worker_unirust.write();
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

/// Compute partition ID for a record using the interner and ontology's identity keys.
/// This ensures records with the same identity key values end up in the same partition.
fn compute_partition_id_for_record(
    record: &Record,
    ontology: &crate::Ontology,
    interner: &ConcurrentInterner,
    partition_count: usize,
) -> usize {
    use rustc_hash::FxHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = FxHasher::default();
    let identity_keys = ontology.identity_keys_for_type(&record.identity.entity_type);

    if !identity_keys.is_empty() {
        let first_key = &identity_keys[0];

        // Use attribute_names from ontology to find matching descriptors
        // Intern the attribute names to get AttrIds that match the record's descriptors
        let attr_ids: Vec<_> = first_key
            .attribute_names
            .iter()
            .filter_map(|name| interner.get_attr_id(name))
            .collect();

        // Find descriptor values that match the identity key attributes
        let key_value_ids: Vec<_> = record
            .descriptors
            .iter()
            .filter(|d| attr_ids.contains(&d.attr))
            .map(|d| &d.value)
            .collect();

        if !key_value_ids.is_empty() {
            record.identity.entity_type.hash(&mut hasher);
            for value_id in key_value_ids {
                value_id.hash(&mut hasher);
            }
        } else {
            record.identity.uid.hash(&mut hasher);
        }
    } else {
        record.identity.uid.hash(&mut hasher);
    }

    (hasher.finish() as usize) % partition_count
}

/// Dispatch records using the partitioned architecture for maximum throughput.
/// This bypasses the worker queue entirely and processes partitions in parallel.
///
/// Performance architecture:
/// High-performance dispatch with REAL entity resolution.
/// Uses parallel partitioned processing for maximum throughput while
/// maintaining full entity resolution correctness.
async fn dispatch_ingest_partitioned(
    partitioned: &Arc<ParallelPartitionedUnirust>,
    interner: &Arc<ConcurrentInterner>,
    ingest_wal: Option<&IngestWal>,
    shard_id: u32,
    records: Vec<proto::RecordInput>,
) -> Result<Vec<proto::IngestAssignment>, Status> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    // Optional WAL write
    if let Some(wal) = ingest_wal {
        wal.write_batch(&records)?;
    }

    let partition_count = partitioned.partition_count();
    let ontology = partitioned.ontology();

    // Phase 1: Build records AND compute partition IDs using the same interner
    // This ensures records with same identity key values go to the same partition
    let indexed_records: Vec<(usize, u32, Record)> = if records.len() > 500 {
        records
            .par_iter()
            .filter_map(|record| {
                ShardNode::build_record_concurrent(interner, record)
                    .ok()
                    .map(|r| {
                        let partition_id = compute_partition_id_for_record(
                            &r,
                            ontology,
                            interner,
                            partition_count,
                        );
                        (partition_id, record.index, r)
                    })
            })
            .collect()
    } else {
        let mut indexed_records = Vec::with_capacity(records.len());
        for record in &records {
            let record_input = ShardNode::build_record_concurrent(interner, record)?;
            let partition_id =
                compute_partition_id_for_record(&record_input, ontology, interner, partition_count);
            indexed_records.push((partition_id, record.index, record_input));
        }
        indexed_records
    };

    // Phase 2: REAL entity resolution via partitioned processing with pre-computed partition IDs
    let partition_results = partitioned.ingest_batch_with_partitions(indexed_records);

    // Phase 3: Convert to proto assignments
    let mut assignments = Vec::with_capacity(partition_results.len());
    for result in partition_results {
        assignments.push(proto::IngestAssignment {
            index: result.index,
            shard_id,
            record_id: 0,
            cluster_id: result.cluster_id.0,
            cluster_key: String::new(),
        });
    }

    if let Some(wal) = ingest_wal {
        wal.clear()?;
    }

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
        .map(|payload| bincode::deserialize(&payload))
        .transpose()?;
    let config = if let Some(config) = stored_config {
        config
    } else {
        store.save_ontology_config(&bincode::serialize(&fallback_config)?)?;
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
            let mut guard = write_unirust!(self);

            // Create a temporary in-memory Unirust to replace the persistent one,
            // which closes the RocksDB when dropped
            let temp_store = crate::Store::new();
            let temp_ontology = crate::Ontology::new();
            let old = std::mem::replace(
                &mut *guard,
                Unirust::with_store_and_tuning(temp_ontology, temp_store, self.tuning.clone()),
            );
            drop(old); // Explicitly drop to close RocksDB

            // Now safe to open a new store at the same path
            let mut store =
                PersistentStore::open(path).map_err(|err| Status::internal(err.to_string()))?;
            store
                .reset_data()
                .map_err(|err| Status::internal(err.to_string()))?;
            store
                .save_ontology_config(&bincode::serialize(&config).map_err(|err| {
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
            let mut guard = write_unirust!(self);
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        }

        // Rebuild partitioned processor with the new ontology if enabled
        // CRITICAL: The partitioned processor needs the ontology with strong identifiers
        // for conflict detection to work!
        if is_partitioned_enabled() {
            let num_partitions = partition_count();
            let partition_config = PartitionConfig::for_cores(num_partitions);
            // Build ontology using concurrent interner to ensure AttrIds match records
            let ontology = config.build_ontology_with_interner(&self.concurrent_interner);
            let new_partitioned = ParallelPartitionedUnirust::new(
                partition_config,
                Arc::new(ontology),
                self.tuning.clone(),
            )
            .map_err(|err| Status::internal(err.to_string()))?;

            let mut partitioned_guard = self.partitioned.write();
            *partitioned_guard = Some(Arc::new(new_partitioned));
            tracing::info!("Partitioned processor rebuilt with updated ontology");
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
        let _wal_guard = if self.ingest_wal.is_some() {
            Some(self.ingest_wal_lock.lock().await)
        } else {
            None
        };

        // Use partitioned processing for large batches (high-throughput mode)
        // Small batches use sequential path for correctness (query support)
        // Clone the Arc and release the lock before awaiting (guards aren't Send)
        let partitioned_arc = self.partitioned.read().clone();
        let use_partitioned = partitioned_arc.is_some() && records.len() >= 100;
        let assignments = if use_partitioned {
            dispatch_ingest_partitioned(
                partitioned_arc.as_ref().unwrap(),
                &self.concurrent_interner,
                self.ingest_wal.as_deref(),
                self.shard_id,
                records,
            )
            .await?
        } else {
            dispatch_ingest_records(&self.ingest_txs, self.ingest_wal.as_deref(), records).await?
        };

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
            let _wal_guard = if self.ingest_wal.is_some() {
                Some(self.ingest_wal_lock.lock().await)
            } else {
                None
            };

            // Use partitioned processing for large batches (high-throughput mode)
            // Small batches use sequential path for correctness
            // Clone the Arc and release the lock before awaiting (guards aren't Send)
            let partitioned_arc = self.partitioned.read().clone();
            let use_partitioned = partitioned_arc.is_some() && chunk.records.len() >= 100;
            let batch_assignments = if use_partitioned {
                dispatch_ingest_partitioned(
                    partitioned_arc.as_ref().unwrap(),
                    &self.concurrent_interner,
                    self.ingest_wal.as_deref(),
                    self.shard_id,
                    chunk.records,
                )
                .await?
            } else {
                dispatch_ingest_records(&self.ingest_txs, self.ingest_wal.as_deref(), chunk.records)
                    .await?
            };
            assignments.extend(batch_assignments);
        }

        self.metrics
            .record_ingest(record_count, start.elapsed().as_micros() as u64);
        Ok(Response::new(proto::IngestRecordsResponse { assignments }))
    }

    async fn ingest_records_from_url(
        &self,
        _request: Request<proto::IngestRecordsFromUrlRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        Err(Status::unimplemented(
            "URL-based ingestion is deprecated. Use gRPC ingest_records instead.",
        ))
    }

    async fn query_entities(
        &self,
        request: Request<proto::QueryEntitiesRequest>,
    ) -> Result<Response<proto::QueryEntitiesResponse>, Status> {
        let start = Instant::now();
        let mut unirust = write_unirust!(self);
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
        let unirust = read_unirust!(self);
        let partitioned_guard = self.partitioned.read();

        // Sum stats from both partitioned path (large batches) and worker path (small batches)
        let (record_count, cluster_count, partitioned_conflicts) =
            if let Some(partitioned) = partitioned_guard.as_ref() {
                (
                    partitioned.total_records() + unirust.record_count() as u64,
                    partitioned.total_cluster_count() as u64
                        + unirust.streaming_cluster_count().unwrap_or(0) as u64,
                    partitioned.total_conflicts(),
                )
            } else {
                (
                    unirust.record_count() as u64,
                    unirust.streaming_cluster_count().unwrap_or(0) as u64,
                    0,
                )
            };

        // Include conflicts from:
        // 1. Partitioned path (large batches)
        // 2. Worker path streaming linker (small batches)
        // 3. Stored conflict summaries (historical)
        let worker_conflicts = unirust.streaming_conflicts_detected();
        let stored_conflicts = unirust.conflict_summary_count().unwrap_or(0) as u64;
        let conflict_count = partitioned_conflicts + worker_conflicts + stored_conflicts;
        let (graph_node_count, graph_edge_count) = unirust.graph_counts().unwrap_or((0, 0));

        // Get cross-shard stats from both partitioned and non-partitioned paths
        let (boundary_keys, cross_shard_merges) =
            if let Some(partitioned) = partitioned_guard.as_ref() {
                (
                    partitioned.total_boundary_count() as u64 + unirust.boundary_count() as u64,
                    partitioned.total_cross_shard_merge_count() as u64
                        + unirust.cross_shard_merge_count() as u64,
                )
            } else {
                (
                    unirust.boundary_count() as u64,
                    unirust.cross_shard_merge_count() as u64,
                )
            };

        Ok(Response::new(proto::StatsResponse {
            record_count,
            cluster_count,
            conflict_count,
            graph_node_count,
            graph_edge_count,
            cross_shard_merges,
            cross_shard_conflicts: 0, // Tracked at router level during reconcile
            boundary_keys_tracked: boundary_keys,
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
            let unirust = read_unirust!(self);
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
        let unirust = read_unirust!(self);
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
        let unirust = read_unirust!(self);
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

        let unirust = read_unirust!(self);
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

        let read_records = move |unirust: Arc<parking_lot::RwLock<Unirust>>,
                                 start: u32,
                                 end: u32,
                                 lim: usize|
              -> Pin<Box<dyn std::future::Future<Output = _> + Send>> {
            Box::pin(async move {
                let guard = unirust.read();
                let mut records =
                    guard.records_in_id_range(RecordId(start), RecordId(end), lim + 1);
                let has_more = records.len() > lim;
                if has_more {
                    records.truncate(lim);
                }
                let next_start_id = if has_more {
                    records
                        .last()
                        .map(|r| r.id.0.saturating_add(1))
                        .unwrap_or(start)
                } else {
                    0
                };
                let snapshots = records
                    .iter()
                    .map(|r| ShardNode::record_to_snapshot(&guard, r))
                    .collect::<Vec<_>>();
                (snapshots, has_more, next_start_id)
            })
        };

        tokio::spawn(async move {
            loop {
                let (records, has_more, next_start_id) =
                    read_records(unirust.clone(), start_id, end_id, limit).await;

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
        let mut unirust = write_unirust!(self);
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
            let mut unirust = write_unirust!(self);
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
            let guard = read_unirust!(self);
            guard.cached_conflict_summaries()
        } {
            cache
        } else if let Some(persisted) = {
            let guard = read_unirust!(self);
            guard.load_conflict_summaries()
        } {
            let mut unirust = write_unirust!(self);
            unirust.set_conflict_cache(persisted.clone());
            persisted
        } else {
            let mut unirust = write_unirust!(self);
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

        // Add cross-shard conflicts (indirect conflicts from reconciliation)
        let cross_shard_conflicts = self.cross_shard_conflicts.read();
        for conflict in cross_shard_conflicts.iter() {
            // Apply same filters
            if request.end > request.start {
                let filter = Interval::new(request.start, request.end)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;
                if !crate::temporal::is_overlapping(&conflict.interval, &filter) {
                    continue;
                }
            }

            // Convert to ConflictSummary
            summaries.push(ConflictSummary {
                kind: "indirect_cross_shard".to_string(),
                attribute: None, // We only have hash, not the actual attribute name
                interval: conflict.interval,
                records: vec![],
                cause: Some(format!(
                    "Cross-shard conflict: clusters {}:{} and {}:{} share identity key but have different strong IDs in same perspective",
                    conflict.cluster1.shard_id, conflict.cluster1.local_id,
                    conflict.cluster2.shard_id, conflict.cluster2.local_id
                )),
            });
        }
        drop(cross_shard_conflicts);

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
                .save_ontology_config(&bincode::serialize(&config).map_err(|err| {
                    Status::internal(format!("failed to encode ontology config: {err}"))
                })?)
                .map_err(|err| Status::internal(err.to_string()))?;
            let ontology = config.build_ontology(store.inner_mut());
            store
                .persist_state()
                .map_err(|err| Status::internal(err.to_string()))?;
            let mut guard = write_unirust!(self);
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        } else {
            let mut store = crate::Store::new();
            let ontology = config.build_ontology(&mut store);
            let mut guard = write_unirust!(self);
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
        let unirust = read_unirust!(self);
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
                                        perspective_strong_ids: e.perspective_strong_ids.clone(),
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
        let mut unirust = write_unirust!(self);

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

    async fn get_dirty_boundary_keys(
        &self,
        _request: Request<proto::GetDirtyBoundaryKeysRequest>,
    ) -> Result<Response<proto::GetDirtyBoundaryKeysResponse>, Status> {
        let unirust = read_unirust!(self);
        let partitioned_guard = self.partitioned.read();

        // Get dirty keys from both partitioned and non-partitioned paths
        let mut all_dirty_keys = unirust.get_dirty_boundary_keys().unwrap_or_default();
        let mut combined_index = unirust.export_boundary_index();

        // Add dirty keys from partitioned processor
        if let Some(partitioned) = partitioned_guard.as_ref() {
            all_dirty_keys.extend(partitioned.get_all_dirty_boundary_keys());
            let partitioned_index = partitioned.export_boundary_index();
            if let Some(ref mut index) = combined_index {
                index.merge_from(&partitioned_index);
            } else {
                combined_index = Some(partitioned_index);
            }
        }

        let mut dirty_key_entries = Vec::new();
        if let Some(index) = combined_index {
            for sig in &all_dirty_keys {
                if let Some(entries) = index.get_boundaries(sig) {
                    let proto_entries: Vec<proto::ClusterBoundaryEntry> = entries
                        .iter()
                        .map(|e| proto::ClusterBoundaryEntry {
                            cluster_id: Some(proto::GlobalClusterId {
                                shard_id: e.cluster_id.shard_id as u32,
                                local_id: e.cluster_id.local_id,
                                version: e.cluster_id.version as u32,
                            }),
                            interval_start: e.interval.start,
                            interval_end: e.interval.end,
                            shard_id: e.shard_id as u32,
                            perspective_strong_ids: e.perspective_strong_ids.clone(),
                        })
                        .collect();

                    dirty_key_entries.push(proto::DirtyBoundaryKey {
                        signature: Some(proto::IdentityKeySignature {
                            signature: sig.to_bytes().to_vec(),
                        }),
                        entries: proto_entries,
                    });
                }
            }
        }

        Ok(Response::new(proto::GetDirtyBoundaryKeysResponse {
            dirty_keys: dirty_key_entries,
            shard_id: self.shard_id,
        }))
    }

    async fn clear_dirty_keys(
        &self,
        request: Request<proto::ClearDirtyKeysRequest>,
    ) -> Result<Response<proto::ClearDirtyKeysResponse>, Status> {
        let req = request.into_inner();
        let mut unirust = write_unirust!(self);
        let partitioned_guard = self.partitioned.read();

        let keys: Vec<crate::sharding::IdentityKeySignature> = req
            .keys
            .iter()
            .filter_map(|sig| {
                if sig.signature.len() == 32 {
                    let mut bytes = [0u8; 32];
                    bytes.copy_from_slice(&sig.signature);
                    Some(crate::sharding::IdentityKeySignature::from_bytes(bytes))
                } else {
                    None
                }
            })
            .collect();

        let keys_cleared = keys.len() as u32;
        unirust.clear_dirty_boundary_keys(&keys);

        // Also clear on partitioned processor
        if let Some(partitioned) = partitioned_guard.as_ref() {
            partitioned.clear_dirty_boundary_keys(&keys);
        }

        Ok(Response::new(proto::ClearDirtyKeysResponse {
            keys_cleared,
        }))
    }

    async fn store_cross_shard_conflicts(
        &self,
        request: Request<proto::StoreCrossShardConflictsRequest>,
    ) -> Result<Response<proto::StoreCrossShardConflictsResponse>, Status> {
        let req = request.into_inner();
        let conflicts: Vec<crate::sharding::CrossShardConflict> = req
            .conflicts
            .into_iter()
            .filter_map(|c| {
                let sig = c.identity_key_signature.as_ref()?;
                if sig.signature.len() != 32 {
                    return None;
                }
                let mut sig_bytes = [0u8; 32];
                sig_bytes.copy_from_slice(&sig.signature);

                let cluster1 = c.cluster1.as_ref()?;
                let cluster2 = c.cluster2.as_ref()?;

                let interval = Interval::new(c.interval_start, c.interval_end).ok()?;

                Some(crate::sharding::CrossShardConflict {
                    identity_key_signature: crate::sharding::IdentityKeySignature::from_bytes(
                        sig_bytes,
                    ),
                    cluster1: GlobalClusterId::new(
                        cluster1.shard_id as u16,
                        cluster1.local_id,
                        cluster1.version as u16,
                    ),
                    cluster2: GlobalClusterId::new(
                        cluster2.shard_id as u16,
                        cluster2.local_id,
                        cluster2.version as u16,
                    ),
                    interval,
                    perspective_hash: c.perspective_hash,
                    strong_id_hash1: c.strong_id_hash1,
                    strong_id_hash2: c.strong_id_hash2,
                })
            })
            .collect();

        let stored_count = conflicts.len() as u32;

        // Store conflicts - keep only those relevant to this shard
        let shard_id = self.shard_id as u16;
        let mut conflict_storage = self.cross_shard_conflicts.write();
        for conflict in conflicts {
            // Store if either cluster belongs to this shard
            if conflict.cluster1.shard_id == shard_id || conflict.cluster2.shard_id == shard_id {
                // Avoid duplicates - check if already stored
                if !conflict_storage.iter().any(|c| {
                    c.cluster1 == conflict.cluster1
                        && c.cluster2 == conflict.cluster2
                        && c.interval == conflict.interval
                }) {
                    conflict_storage.push(conflict);
                }
            }
        }

        Ok(Response::new(proto::StoreCrossShardConflictsResponse {
            stored_count,
        }))
    }
}

// =============================================================================
// ADAPTIVE RECONCILIATION
// =============================================================================

/// Configuration for adaptive reconciliation scheduling.
#[derive(Debug, Clone)]
pub struct AdaptiveReconciliationConfig {
    /// Number of dirty keys before triggering reconciliation.
    pub key_count_threshold: usize,
    /// Maximum time a key can remain dirty before triggering reconciliation.
    pub max_staleness: Duration,
    /// Ingest rate (records/sec) below which system is considered idle.
    pub idle_ingest_rate: f64,
    /// Minimum interval between reconciliation runs.
    pub min_reconcile_interval: Duration,
}

impl Default for AdaptiveReconciliationConfig {
    fn default() -> Self {
        Self {
            key_count_threshold: 1000,
            max_staleness: Duration::from_secs(60),
            idle_ingest_rate: 1000.0,
            min_reconcile_interval: Duration::from_secs(5),
        }
    }
}

/// State for a dirty boundary key.
#[derive(Debug)]
struct DirtyKeyState {
    /// Which shards have this key.
    shards: std::collections::HashSet<u16>,
}

/// Coordinator for incremental cross-shard reconciliation.
/// Accumulates dirty boundary keys from shards and triggers reconciliation
/// based on adaptive conditions.
struct ReconciliationCoordinator {
    /// Keys that changed since last reconcile, deduplicated.
    dirty_keys: std::collections::HashMap<crate::sharding::IdentityKeySignature, DirtyKeyState>,
    /// When the oldest dirty key was first seen.
    oldest_dirty: Option<Instant>,
    /// Last time reconciliation was performed.
    last_reconcile: Instant,
    /// Configuration.
    config: AdaptiveReconciliationConfig,
}

impl ReconciliationCoordinator {
    fn new(config: AdaptiveReconciliationConfig) -> Self {
        Self {
            dirty_keys: std::collections::HashMap::new(),
            oldest_dirty: None,
            last_reconcile: Instant::now(),
            config,
        }
    }

    /// Add dirty keys from a shard response.
    fn add_dirty_keys_from_shard(
        &mut self,
        shard_id: u16,
        keys: Vec<crate::sharding::IdentityKeySignature>,
    ) {
        for sig in keys {
            let state = self.dirty_keys.entry(sig).or_insert_with(|| {
                if self.oldest_dirty.is_none() {
                    self.oldest_dirty = Some(Instant::now());
                }
                DirtyKeyState {
                    shards: std::collections::HashSet::new(),
                }
            });
            state.shards.insert(shard_id);
        }
    }

    /// Get the age of the oldest dirty key.
    fn oldest_dirty_age(&self) -> Option<Duration> {
        self.oldest_dirty.map(|t| t.elapsed())
    }

    /// Take and clear dirty keys for reconciliation.
    fn take_dirty_keys(&mut self) -> Vec<crate::sharding::IdentityKeySignature> {
        self.oldest_dirty = None;
        self.last_reconcile = Instant::now();
        self.dirty_keys.drain().map(|(k, _)| k).collect()
    }

    /// Check if we should reconcile based on adaptive conditions.
    fn should_reconcile(&self, current_ingest_rate: f64) -> bool {
        let dirty_count = self.dirty_keys.len();

        if dirty_count == 0 {
            return false;
        }

        // Don't reconcile too frequently
        if self.last_reconcile.elapsed() < self.config.min_reconcile_interval {
            return false;
        }

        // Condition 1: Enough dirty keys accumulated
        if dirty_count >= self.config.key_count_threshold {
            return true;
        }

        // Condition 2: Keys have been dirty too long
        if let Some(age) = self.oldest_dirty_age() {
            if age > self.config.max_staleness {
                return true;
            }
        }

        // Condition 3: System is idle, might as well reconcile
        if current_ingest_rate < self.config.idle_ingest_rate {
            return true;
        }

        false
    }
}

#[derive(Clone)]
pub struct RouterNode {
    shard_clients: Vec<proto::shard_service_client::ShardServiceClient<tonic::transport::Channel>>,
    /// RwLock for ontology config - read-heavy, written only during set_ontology
    ontology_config: Arc<RwLock<DistributedOntologyConfig>>,
    config_version: String,
    metrics: Arc<PerfMetrics>,
    /// Cluster locality index for cluster-aware routing
    locality_index: Arc<StdRwLock<ClusterLocalityIndex>>,
    /// Reconciliation coordinator (wrapped in mutex for interior mutability)
    reconciliation_coordinator: Arc<tokio::sync::Mutex<ReconciliationCoordinator>>,
}

impl RouterNode {
    pub async fn connect(
        shard_addrs: Vec<String>,
        ontology_config: DistributedOntologyConfig,
    ) -> Result<Arc<Self>, Status> {
        Self::connect_with_version(shard_addrs, ontology_config, None).await
    }

    pub async fn connect_with_version(
        shard_addrs: Vec<String>,
        ontology_config: DistributedOntologyConfig,
        config_version: Option<String>,
    ) -> Result<Arc<Self>, Status> {
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
        let router = Arc::new(Self {
            shard_clients,
            ontology_config: Arc::new(RwLock::new(ontology_config)),
            config_version,
            metrics: Arc::new(PerfMetrics::new()),
            locality_index: Arc::new(StdRwLock::new(ClusterLocalityIndex::new())),
            reconciliation_coordinator: Arc::new(tokio::sync::Mutex::new(
                ReconciliationCoordinator::new(AdaptiveReconciliationConfig::default()),
            )),
        });

        // Start adaptive reconciliation background task automatically
        let reconcile_router = router.clone();
        tokio::spawn(async move {
            reconcile_router.run_adaptive_reconciliation_loop().await;
        });

        Ok(router)
    }

    pub async fn connect_from_file(
        path: impl AsRef<Path>,
        ontology_config: DistributedOntologyConfig,
        config_version: Option<String>,
    ) -> Result<Arc<Self>, Status> {
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

    /// Start the adaptive reconciliation background task.
    /// This task polls shards for dirty boundary keys and triggers reconciliation
    /// based on adaptive conditions (key count, staleness, idle system).
    pub fn start_adaptive_reconciliation(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let router = self.clone();
        tokio::spawn(async move {
            router.run_adaptive_reconciliation_loop().await;
        })
    }

    /// Run the adaptive reconciliation loop.
    async fn run_adaptive_reconciliation_loop(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(500));

        loop {
            interval.tick().await;

            // 1. Poll all shards for dirty boundary keys
            let mut all_dirty_keys: std::collections::HashMap<
                crate::sharding::IdentityKeySignature,
                Vec<(u16, proto::DirtyBoundaryKey)>,
            > = std::collections::HashMap::new();

            for (idx, client) in self.shard_clients.iter().enumerate() {
                let mut client = client.clone();
                if let Ok(response) = client
                    .get_dirty_boundary_keys(Request::new(proto::GetDirtyBoundaryKeysRequest {}))
                    .await
                {
                    let resp = response.into_inner();
                    let shard_id = resp.shard_id as u16;

                    for dirty_key in resp.dirty_keys {
                        if let Some(sig_proto) = &dirty_key.signature {
                            if sig_proto.signature.len() == 32 {
                                let mut sig_bytes = [0u8; 32];
                                sig_bytes.copy_from_slice(&sig_proto.signature);
                                let sig = IdentityKeySignature::from_bytes(sig_bytes);

                                all_dirty_keys
                                    .entry(sig)
                                    .or_default()
                                    .push((shard_id, dirty_key.clone()));

                                // Add to coordinator
                                let mut coordinator = self.reconciliation_coordinator.lock().await;
                                coordinator.add_dirty_keys_from_shard(shard_id, vec![sig]);
                            }
                        }
                    }
                }
                let _ = idx; // suppress unused warning
            }

            // 2. Check if we should reconcile
            let should_run = {
                let coordinator = self.reconciliation_coordinator.lock().await;
                let ingest_rate = self.current_ingest_rate();
                coordinator.should_reconcile(ingest_rate)
            };

            if should_run {
                // 3. Take dirty keys and perform targeted reconciliation
                let dirty_keys = {
                    let mut coordinator = self.reconciliation_coordinator.lock().await;
                    coordinator.take_dirty_keys()
                };

                if !dirty_keys.is_empty() {
                    let result = self
                        .reconcile_dirty_keys(&dirty_keys, &all_dirty_keys)
                        .await;

                    // Update metrics
                    if result.conflicts_blocked > 0 {
                        self.metrics
                            .cross_shard_conflicts
                            .fetch_add(result.conflicts_blocked as u64, Ordering::Relaxed);
                    }

                    // 4. Clear dirty keys on shards
                    let proto_keys: Vec<proto::IdentityKeySignature> = dirty_keys
                        .iter()
                        .map(|sig| proto::IdentityKeySignature {
                            signature: sig.to_bytes().to_vec(),
                        })
                        .collect();

                    for client in &self.shard_clients {
                        let mut client = client.clone();
                        let _ = client
                            .clear_dirty_keys(Request::new(proto::ClearDirtyKeysRequest {
                                keys: proto_keys.clone(),
                            }))
                            .await;
                    }
                }
            }
        }
    }

    /// Perform targeted reconciliation for specific dirty keys.
    async fn reconcile_dirty_keys(
        &self,
        keys: &[crate::sharding::IdentityKeySignature],
        dirty_key_data: &std::collections::HashMap<
            crate::sharding::IdentityKeySignature,
            Vec<(u16, proto::DirtyBoundaryKey)>,
        >,
    ) -> crate::sharding::ReconciliationResult {
        use crate::sharding::{ClusterBoundaryIndex, IncrementalReconciler};

        // Build boundary indices for each shard from dirty key data
        let mut shard_boundaries: std::collections::HashMap<u16, ClusterBoundaryIndex> =
            std::collections::HashMap::new();

        for sig in keys {
            if let Some(entries) = dirty_key_data.get(sig) {
                for (shard_id, dirty_key) in entries {
                    let boundary = shard_boundaries
                        .entry(*shard_id)
                        .or_insert_with(|| ClusterBoundaryIndex::new_small(*shard_id));

                    for entry in &dirty_key.entries {
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
                                boundary.register_boundary_key_with_strong_ids(
                                    *sig,
                                    cluster_id,
                                    interval,
                                    entry.perspective_strong_ids.clone(),
                                );
                            }
                        }
                    }
                }
            }
        }

        // Run targeted reconciliation
        let mut reconciler = IncrementalReconciler::new();
        for (_shard_id, boundary) in shard_boundaries {
            reconciler.add_shard_boundary(boundary);
        }

        let key_set: std::collections::HashSet<_> = keys.iter().copied().collect();
        let result = reconciler.reconcile_keys(&key_set);

        // Apply merges to affected shards
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
                    .await;
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
                        .await;
                }
            }
        }

        // Propagate detected conflicts to affected shards
        if !result.detected_conflicts.is_empty() {
            // Group conflicts by shard
            let mut conflicts_by_shard: std::collections::HashMap<
                u16,
                Vec<proto::CrossShardConflict>,
            > = std::collections::HashMap::new();

            for conflict in &result.detected_conflicts {
                let proto_conflict = proto::CrossShardConflict {
                    identity_key_signature: Some(proto::IdentityKeySignature {
                        signature: conflict.identity_key_signature.to_bytes().to_vec(),
                    }),
                    cluster1: Some(proto::GlobalClusterId {
                        shard_id: conflict.cluster1.shard_id as u32,
                        local_id: conflict.cluster1.local_id,
                        version: conflict.cluster1.version as u32,
                    }),
                    cluster2: Some(proto::GlobalClusterId {
                        shard_id: conflict.cluster2.shard_id as u32,
                        local_id: conflict.cluster2.local_id,
                        version: conflict.cluster2.version as u32,
                    }),
                    interval_start: conflict.interval.start,
                    interval_end: conflict.interval.end,
                    perspective_hash: conflict.perspective_hash,
                    strong_id_hash1: conflict.strong_id_hash1,
                    strong_id_hash2: conflict.strong_id_hash2,
                };

                // Send to both shards involved
                conflicts_by_shard
                    .entry(conflict.cluster1.shard_id)
                    .or_default()
                    .push(proto_conflict.clone());
                if conflict.cluster2.shard_id != conflict.cluster1.shard_id {
                    conflicts_by_shard
                        .entry(conflict.cluster2.shard_id)
                        .or_default()
                        .push(proto_conflict);
                }
            }

            // Send conflicts to each shard
            for (shard_id, conflicts) in conflicts_by_shard {
                if let Ok(mut client) = self.shard_client(shard_id as u32) {
                    let _ = client
                        .store_cross_shard_conflicts(Request::new(
                            proto::StoreCrossShardConflictsRequest { conflicts },
                        ))
                        .await;
                }
            }
        }

        result
    }

    /// Get current ingest rate (records per second) from metrics.
    fn current_ingest_rate(&self) -> f64 {
        let elapsed = self.metrics.start.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.metrics.ingest_records.load(Ordering::Relaxed) as f64 / elapsed
        } else {
            0.0
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
        let record_count = batch.records.len();
        let shard_count = self.shard_clients.len();

        // ULTRA-FAST single-shard path: skip all hashing and sorting
        if shard_count == 1 {
            let mut client = self.shard_clients[0].clone();
            let response = client
                .ingest_records(Request::new(proto::IngestRecordsRequest {
                    records: batch.records,
                }))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;

            self.metrics
                .record_ingest(record_count, start.elapsed().as_micros() as u64);
            return Ok(response);
        }

        // Multi-shard path: hash-based routing
        let config = self.ontology_config.read().await.clone();
        let mut shard_batches: Vec<Vec<proto::RecordInput>> = vec![Vec::new(); shard_count];

        for record in batch.records {
            let shard_idx = hash_record_to_shard(&config, &record, shard_count);
            shard_batches[shard_idx].push(record);
        }

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

        results.sort_by_key(|assignment| assignment.index);
        self.metrics
            .record_ingest(record_count, start.elapsed().as_micros() as u64);
        Ok(Response::new(proto::IngestRecordsResponse {
            assignments: results,
        }))
    }

    async fn ingest_records_from_url(
        &self,
        _request: Request<proto::IngestRecordsFromUrlRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        Err(Status::unimplemented(
            "URL-based ingestion is deprecated. Use gRPC ingest_records instead.",
        ))
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
            cross_shard_merges: 0,
            cross_shard_conflicts: 0,
            boundary_keys_tracked: 0,
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
            totals.cross_shard_merges += response.cross_shard_merges;
            // cross_shard_conflicts from shards is 0, we track it at router level
            totals.boundary_keys_tracked += response.boundary_keys_tracked;
        }

        // Add router-level cross-shard conflicts (tracked during reconcile)
        totals.cross_shard_conflicts = self
            .metrics
            .cross_shard_conflicts
            .load(std::sync::atomic::Ordering::Relaxed);

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
                                    boundary_index.register_boundary_key_with_strong_ids(
                                        sig,
                                        cluster_id,
                                        interval,
                                        entry.perspective_strong_ids.clone(),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            reconciler.add_shard_boundary(boundary_index);
        }

        let result = reconciler.reconcile();

        // Track cross-shard conflicts in router metrics
        if result.conflicts_blocked > 0 {
            self.metrics.cross_shard_conflicts.fetch_add(
                result.conflicts_blocked as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

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

        // Propagate detected conflicts to affected shards
        if !result.detected_conflicts.is_empty() {
            // Group conflicts by shard
            let mut conflicts_by_shard: std::collections::HashMap<
                u16,
                Vec<proto::CrossShardConflict>,
            > = std::collections::HashMap::new();

            for conflict in &result.detected_conflicts {
                let proto_conflict = proto::CrossShardConflict {
                    identity_key_signature: Some(proto::IdentityKeySignature {
                        signature: conflict.identity_key_signature.to_bytes().to_vec(),
                    }),
                    cluster1: Some(proto::GlobalClusterId {
                        shard_id: conflict.cluster1.shard_id as u32,
                        local_id: conflict.cluster1.local_id,
                        version: conflict.cluster1.version as u32,
                    }),
                    cluster2: Some(proto::GlobalClusterId {
                        shard_id: conflict.cluster2.shard_id as u32,
                        local_id: conflict.cluster2.local_id,
                        version: conflict.cluster2.version as u32,
                    }),
                    interval_start: conflict.interval.start,
                    interval_end: conflict.interval.end,
                    perspective_hash: conflict.perspective_hash,
                    strong_id_hash1: conflict.strong_id_hash1,
                    strong_id_hash2: conflict.strong_id_hash2,
                };

                // Send to both shards involved
                conflicts_by_shard
                    .entry(conflict.cluster1.shard_id)
                    .or_default()
                    .push(proto_conflict.clone());
                if conflict.cluster2.shard_id != conflict.cluster1.shard_id {
                    conflicts_by_shard
                        .entry(conflict.cluster2.shard_id)
                        .or_default()
                        .push(proto_conflict);
                }
            }

            // Send conflicts to each shard
            for (shard_id, conflicts) in conflicts_by_shard {
                if let Ok(mut client) = self.shard_client(shard_id as u32) {
                    let _ = client
                        .store_cross_shard_conflicts(Request::new(
                            proto::StoreCrossShardConflictsRequest { conflicts },
                        ))
                        .await;
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
            merge_candidates: result.merge_candidates as u32,
            conflicts_blocked: result.conflicts_blocked as u32,
        }))
    }
}

/// RouterService implementation for Arc<RouterNode> to allow use with gRPC server
/// while returning Arc from connect methods for background task spawning.
#[tonic::async_trait]
impl proto::router_service_server::RouterService for Arc<RouterNode> {
    type ExportRecordsStreamStream =
        <RouterNode as proto::router_service_server::RouterService>::ExportRecordsStreamStream;

    async fn set_ontology(
        &self,
        request: Request<proto::ApplyOntologyRequest>,
    ) -> Result<Response<proto::ApplyOntologyResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::set_ontology(self, request)
            .await
    }

    async fn ingest_records(
        &self,
        request: Request<proto::IngestRecordsRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::ingest_records(self, request)
            .await
    }

    async fn ingest_records_from_url(
        &self,
        request: Request<proto::IngestRecordsFromUrlRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::ingest_records_from_url(
            self, request,
        )
        .await
    }

    async fn query_entities(
        &self,
        request: Request<proto::QueryEntitiesRequest>,
    ) -> Result<Response<proto::QueryEntitiesResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::query_entities(self, request)
            .await
    }

    async fn get_stats(
        &self,
        request: Request<proto::StatsRequest>,
    ) -> Result<Response<proto::StatsResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::get_stats(self, request).await
    }

    async fn health_check(
        &self,
        request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::health_check(self, request)
            .await
    }

    async fn get_config_version(
        &self,
        request: Request<proto::ConfigVersionRequest>,
    ) -> Result<Response<proto::ConfigVersionResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::get_config_version(
            self, request,
        )
        .await
    }

    async fn get_metrics(
        &self,
        request: Request<proto::MetricsRequest>,
    ) -> Result<Response<proto::MetricsResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::get_metrics(self, request)
            .await
    }

    async fn get_record_id_range(
        &self,
        request: Request<proto::RouterRecordIdRangeRequest>,
    ) -> Result<Response<proto::RecordIdRangeResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::get_record_id_range(
            self, request,
        )
        .await
    }

    async fn export_records(
        &self,
        request: Request<proto::RouterExportRecordsRequest>,
    ) -> Result<Response<proto::ExportRecordsResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::export_records(self, request)
            .await
    }

    async fn export_records_stream(
        &self,
        request: Request<proto::RouterExportRecordsRequest>,
    ) -> Result<Response<Self::ExportRecordsStreamStream>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::export_records_stream(
            self, request,
        )
        .await
    }

    async fn import_records(
        &self,
        request: Request<proto::RouterImportRecordsRequest>,
    ) -> Result<Response<proto::ImportRecordsResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::import_records(self, request)
            .await
    }

    async fn import_records_stream(
        &self,
        request: Request<tonic::Streaming<proto::RouterImportRecordsRequest>>,
    ) -> Result<Response<proto::ImportRecordsResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::import_records_stream(
            self, request,
        )
        .await
    }

    async fn checkpoint(
        &self,
        request: Request<proto::CheckpointRequest>,
    ) -> Result<Response<proto::CheckpointResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::checkpoint(self, request).await
    }

    async fn list_conflicts(
        &self,
        request: Request<proto::ListConflictsRequest>,
    ) -> Result<Response<proto::ListConflictsResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::list_conflicts(self, request)
            .await
    }

    async fn reset(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::reset(self, request).await
    }

    async fn reconcile(
        &self,
        request: Request<proto::ReconcileRequest>,
    ) -> Result<Response<proto::ReconcileResponse>, Status> {
        <RouterNode as proto::router_service_server::RouterService>::reconcile(self, request).await
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
