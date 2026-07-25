use crate::conflicts::ConflictSummary;
use crate::graph::GoldenDescriptor;
use crate::model::{GlobalClusterId, Record, RecordId, RecordIdentity};
use crate::ontology::{Constraint, IdentityKey, Ontology, StrongIdentifier};
use crate::partitioned::{ParallelPartitionedUnirust, PartitionConfig};
use crate::perf::ConcurrentInterner;
use crate::persistence::{
    commit_cluster_checkpoint, prepare_cluster_checkpoint, read_restored_checkpoint_manifest,
    validate_prepared_cluster_checkpoint, ClusterCheckpointManifest, PersistentOpenOptions,
    CHECKPOINT_PROTOCOL_VERSION,
};
use crate::query::{QueryDescriptor, QueryOutcome};
use crate::sharding::{BloomFilter, IdentityKeySignature};
use crate::store::{SourceRecordReservation, SourceReservationError, StoreMetrics};
use crate::temporal::Interval;
use crate::{PersistentStore, StreamingTuning, Unirust};
use anyhow::Result as AnyResult;
use lru::LruCache;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::{Mutex, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Channel;
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

const INGEST_WAL_MAGIC: &[u8; 8] = b"UNIRWAL\0";
const INGEST_WAL_VERSION: u32 = 1;
const INGEST_WAL_HEADER_LEN: usize = 24;
pub const DISTRIBUTED_PROTOCOL_VERSION: u32 = 5;
const REPLICATION_TOKEN_HEADER: &str = "x-unirust-replication-token";

#[allow(clippy::result_large_err)]
fn validate_distributed_protocol(protocol_version: u32) -> Result<(), Status> {
    if protocol_version == DISTRIBUTED_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(Status::failed_precondition(format!(
            "distributed protocol mismatch: router {}, shard {}; deploy one coordinated Unirust \
             version across the complete cluster",
            DISTRIBUTED_PROTOCOL_VERSION, protocol_version
        )))
    }
}

#[allow(clippy::result_large_err)]
fn validate_checkpoint_protocol(protocol_version: u32) -> Result<(), Status> {
    if protocol_version == CHECKPOINT_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(Status::failed_precondition(format!(
            "checkpoint protocol mismatch: router {}, shard {}; deploy one coordinated Unirust \
             version across the complete cluster",
            CHECKPOINT_PROTOCOL_VERSION, protocol_version
        )))
    }
}

fn encode_wal_batch(inputs: &[WalRecordInput]) -> Result<Vec<u8>, Status> {
    let body = bincode::serialize(inputs).map_err(|err| Status::internal(err.to_string()))?;
    let body_len = u64::try_from(body.len())
        .map_err(|_| Status::resource_exhausted("ingest WAL batch is too large"))?;
    let mut encoded = Vec::with_capacity(INGEST_WAL_HEADER_LEN + body.len());
    encoded.extend_from_slice(INGEST_WAL_MAGIC);
    encoded.extend_from_slice(&INGEST_WAL_VERSION.to_be_bytes());
    encoded.extend_from_slice(&body_len.to_be_bytes());
    encoded.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

fn decode_wal_batch(bytes: &[u8]) -> Result<Vec<WalRecordInput>, String> {
    // Accept the pre-0.2 unframed format so an upgrade can replay an existing WAL.
    if !bytes.starts_with(INGEST_WAL_MAGIC) {
        return bincode::deserialize(bytes).map_err(|err| err.to_string());
    }
    if bytes.len() < INGEST_WAL_HEADER_LEN {
        return Err("truncated ingest WAL header".to_string());
    }

    let version = u32::from_be_bytes(
        bytes[8..12]
            .try_into()
            .map_err(|_| "invalid ingest WAL version field".to_string())?,
    );
    if version != INGEST_WAL_VERSION {
        return Err(format!("unsupported ingest WAL version {version}"));
    }
    let declared_len = u64::from_be_bytes(
        bytes[12..20]
            .try_into()
            .map_err(|_| "invalid ingest WAL payload length field".to_string())?,
    );
    let declared_len = usize::try_from(declared_len)
        .map_err(|_| "ingest WAL payload length exceeds this platform".to_string())?;
    let expected_len = INGEST_WAL_HEADER_LEN
        .checked_add(declared_len)
        .ok_or_else(|| "ingest WAL payload length overflow".to_string())?;
    if bytes.len() != expected_len {
        return Err(format!(
            "ingest WAL length mismatch: expected {expected_len} bytes, found {}",
            bytes.len()
        ));
    }

    let expected_checksum = u32::from_be_bytes(
        bytes[20..24]
            .try_into()
            .map_err(|_| "invalid ingest WAL checksum field".to_string())?,
    );
    let body = &bytes[INGEST_WAL_HEADER_LEN..];
    let actual_checksum = crc32fast::hash(body);
    if actual_checksum != expected_checksum {
        return Err("ingest WAL checksum mismatch".to_string());
    }
    bincode::deserialize(body).map_err(|err| err.to_string())
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), Status> {
    let parent = path
        .parent()
        .ok_or_else(|| Status::internal("ingest WAL path has no parent directory"))?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| Status::internal(format!("failed to sync WAL directory: {err}")))
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), Status> {
    Ok(())
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
            return Ok(());
        }
        let inputs: Vec<WalRecordInput> = records
            .iter()
            .map(wal_from_proto_input)
            .collect::<Result<_, Status>>()?;
        let payload = encode_wal_batch(&inputs)?;
        if self.path.exists() || self.temp_path.exists() {
            return Err(Status::failed_precondition(
                "an earlier ingest WAL is still pending; restart the shard to replay it",
            ));
        }
        let mut file =
            fs::File::create(&self.temp_path).map_err(|err| Status::internal(err.to_string()))?;
        file.write_all(&payload)
            .map_err(|err| Status::internal(err.to_string()))?;
        file.sync_all()
            .map_err(|err| Status::internal(err.to_string()))?;
        drop(file);
        fs::rename(&self.temp_path, &self.path).map_err(|err| Status::internal(err.to_string()))?;
        sync_parent_directory(&self.path)?;
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
        let inputs = match decode_wal_batch(&bytes) {
            Ok(inputs) => inputs,
            Err(reason) => {
                let quarantined = self.quarantine_corrupt()?;
                return Err(Status::data_loss(format!(
                    "ingest WAL is corrupt ({reason}); preserved as {}",
                    quarantined
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        };
        let records = inputs.into_iter().map(proto_from_wal_input).collect();
        Ok(Some(records))
    }

    fn has_pending(&self) -> bool {
        self.path.exists() || self.temp_path.exists()
    }

    fn replay(&self, unirust: &mut Unirust, shard_id: u32) -> Result<(), Status> {
        if let Some(records) = self.load_batch()? {
            process_ingest_batch(unirust, shard_id, &records)?;
            self.clear()?;
        }
        Ok(())
    }

    fn clear(&self) -> Result<(), Status> {
        let mut removed = false;
        if self.path.exists() {
            fs::remove_file(&self.path).map_err(|err| Status::internal(err.to_string()))?;
            removed = true;
        }
        if self.temp_path.exists() {
            fs::remove_file(&self.temp_path).map_err(|err| Status::internal(err.to_string()))?;
            removed = true;
        }
        if removed {
            sync_parent_directory(&self.path)?;
        }
        Ok(())
    }

    fn quarantine_corrupt(&self) -> Result<Vec<PathBuf>, Status> {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| Status::internal(err.to_string()))?
            .as_nanos();
        let mut quarantined = Vec::new();
        for source in [&self.path, &self.temp_path] {
            if !source.exists() {
                continue;
            }
            let file_name = source
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| Status::internal("ingest WAL filename is not valid UTF-8"))?;
            let destination = source.with_file_name(format!("{file_name}.corrupt.{suffix}"));
            fs::rename(source, &destination).map_err(|err| Status::internal(err.to_string()))?;
            quarantined.push(destination);
        }
        if !quarantined.is_empty() {
            sync_parent_directory(&self.path)?;
        }
        Ok(quarantined)
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityKeyConfig {
    pub name: String,
    pub attributes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintKind {
    Unique,
    UniqueWithinPerspective,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintConfig {
    pub name: String,
    pub attribute: String,
    pub kind: ConstraintKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    fn ontology_template(&self) -> Ontology {
        let mut ontology = Ontology::new();

        for key in &self.identity_keys {
            ontology.add_identity_key(IdentityKey::from_names(
                key.attributes.iter().map(String::as_str).collect(),
                key.name.clone(),
            ));
        }

        for attr in &self.strong_identifiers {
            ontology
                .add_strong_identifier(StrongIdentifier::from_name(attr, format!("{attr}_strong")));
        }

        for constraint in &self.constraints {
            let constraint = match constraint.kind {
                ConstraintKind::Unique => {
                    Constraint::unique_from_name(&constraint.attribute, constraint.name.clone())
                }
                ConstraintKind::UniqueWithinPerspective => {
                    Constraint::unique_within_perspective_from_name(
                        &constraint.attribute,
                        constraint.name.clone(),
                    )
                }
            };
            ontology.add_constraint(constraint);
        }

        ontology
    }

    pub fn build_ontology(&self, store: &mut crate::Store) -> Ontology {
        let mut ontology = self.ontology_template();
        ontology.intern_attributes(|name| store.interner_mut().intern_attr(name));
        ontology
    }

    /// Build ontology using a ConcurrentInterner for partitioned mode.
    /// This ensures AttrIds match between ontology and records built with the same interner.
    pub fn build_ontology_with_interner(&self, interner: &ConcurrentInterner) -> Ontology {
        let mut ontology = self.ontology_template();
        ontology.intern_attributes(|name| interner.intern_attr(name));
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

fn to_proto_config(config: &DistributedOntologyConfig) -> proto::OntologyConfig {
    proto::OntologyConfig {
        identity_keys: config
            .identity_keys
            .iter()
            .map(|entry| proto::IdentityKeyConfig {
                name: entry.name.clone(),
                attributes: entry.attributes.clone(),
            })
            .collect(),
        strong_identifiers: config.strong_identifiers.clone(),
        constraints: config
            .constraints
            .iter()
            .map(|entry| proto::ConstraintConfig {
                name: entry.name.clone(),
                attribute: entry.attribute.clone(),
                kind: match entry.kind {
                    ConstraintKind::Unique => proto::ConstraintKind::Unique.into(),
                    ConstraintKind::UniqueWithinPerspective => {
                        proto::ConstraintKind::UniqueWithinPerspective.into()
                    }
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

type CanonicalDescriptor = (String, String, i64, i64);

fn validate_record_inputs(
    records: &[proto::RecordInput],
) -> Result<HashMap<RecordIdentity, Vec<CanonicalDescriptor>>, Status> {
    let mut unique_records = HashMap::with_capacity(records.len());
    for record in records {
        let identity = record
            .identity
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("record identity is required"))?;
        if identity.entity_type.is_empty()
            || identity.perspective.is_empty()
            || identity.uid.is_empty()
        {
            return Err(Status::invalid_argument(
                "record identity fields must not be empty",
            ));
        }

        let mut descriptors = Vec::with_capacity(record.descriptors.len());
        for descriptor in &record.descriptors {
            Interval::new(descriptor.start, descriptor.end)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
            descriptors.push((
                descriptor.attr.clone(),
                descriptor.value.clone(),
                descriptor.start,
                descriptor.end,
            ));
        }
        descriptors.sort_unstable();

        let identity = RecordIdentity::new(
            identity.entity_type.clone(),
            identity.perspective.clone(),
            identity.uid.clone(),
        );
        if let Some(previous) = unique_records.insert(identity, descriptors.clone()) {
            if previous != descriptors {
                return Err(Status::invalid_argument(
                    "source record identity appears more than once with different payloads",
                ));
            }
        }
    }
    Ok(unique_records)
}

fn hash_route_component(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn source_identity_hash(identity: &proto::RecordIdentity) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"unirust.source-owner.v1");
    hash_route_component(&mut hasher, &identity.entity_type);
    hash_route_component(&mut hasher, &identity.perspective);
    hash_route_component(&mut hasher, &identity.uid);
    let digest = hasher.finalize();
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 digest contains eight routing bytes"),
    )
}

#[doc(hidden)]
pub fn hash_source_identity_to_shard(
    identity: &proto::RecordIdentity,
    shard_count: usize,
) -> usize {
    (source_identity_hash(identity) % shard_count as u64) as usize
}

fn canonical_record_payload_digest(record: &proto::RecordInput) -> Result<[u8; 32], Status> {
    let identity = record
        .identity
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("record identity is required"))?;
    let mut descriptors = record
        .descriptors
        .iter()
        .map(|descriptor| {
            (
                descriptor.attr.as_str(),
                descriptor.value.as_str(),
                descriptor.start,
                descriptor.end,
            )
        })
        .collect::<Vec<_>>();
    descriptors.sort_unstable();

    let mut hasher = Sha256::new();
    hasher.update(b"unirust.source-payload.v1");
    hash_route_component(&mut hasher, &identity.entity_type);
    hash_route_component(&mut hasher, &identity.perspective);
    hash_route_component(&mut hasher, &identity.uid);
    hasher.update((descriptors.len() as u64).to_be_bytes());
    for (attribute, value, start, end) in descriptors {
        hash_route_component(&mut hasher, attribute);
        hash_route_component(&mut hasher, value);
        hasher.update(start.to_be_bytes());
        hasher.update(end.to_be_bytes());
    }
    Ok(hasher.finalize().into())
}

fn hash_record_to_u64(config: &DistributedOntologyConfig, record: &proto::RecordInput) -> u64 {
    let identity = record.identity.as_ref();
    let mut descriptors_by_attr: HashMap<&str, Vec<&str>> = HashMap::new();
    for descriptor in &record.descriptors {
        descriptors_by_attr
            .entry(descriptor.attr.as_str())
            .or_default()
            .push(descriptor.value.as_str());
    }
    for values in descriptors_by_attr.values_mut() {
        values.sort_unstable();
        values.dedup();
    }

    let mut hasher = Sha256::new();
    for key in &config.identity_keys {
        let values = key
            .attributes
            .iter()
            .map(|attribute| {
                descriptors_by_attr
                    .get(attribute.as_str())
                    .and_then(|values| values.first())
                    .copied()
            })
            .collect::<Option<Vec<_>>>();
        if let Some(values) = values {
            hasher.update(b"unirust.identity-route.v1");
            if let Some(identity) = identity {
                hash_route_component(&mut hasher, &identity.entity_type);
            }
            hash_route_component(&mut hasher, &key.name);
            for (attribute, value) in key.attributes.iter().zip(values) {
                hash_route_component(&mut hasher, attribute);
                hash_route_component(&mut hasher, value);
            }
            let digest = hasher.finalize();
            return u64::from_be_bytes(
                digest[..8]
                    .try_into()
                    .expect("SHA-256 digest contains eight routing bytes"),
            );
        }
    }

    for constraint in &config.constraints {
        if let Some(value) = descriptors_by_attr
            .get(constraint.attribute.as_str())
            .and_then(|values| values.first())
        {
            hasher.update(b"unirust.constraint-route.v1");
            if let Some(identity) = identity {
                hash_route_component(&mut hasher, &identity.entity_type);
                if matches!(constraint.kind, ConstraintKind::UniqueWithinPerspective) {
                    hash_route_component(&mut hasher, &identity.perspective);
                }
            }
            hash_route_component(&mut hasher, &constraint.name);
            hash_route_component(&mut hasher, &constraint.attribute);
            hash_route_component(&mut hasher, value);
            let digest = hasher.finalize();
            return u64::from_be_bytes(
                digest[..8]
                    .try_into()
                    .expect("SHA-256 digest contains eight routing bytes"),
            );
        }
    }

    hasher.update(b"unirust.source-route.v1");
    if let Some(identity) = identity {
        hash_route_component(&mut hasher, &identity.entity_type);
        hash_route_component(&mut hasher, &identity.perspective);
        hash_route_component(&mut hasher, &identity.uid);
    } else {
        hasher.update(record.index.to_be_bytes());
    }
    let digest = hasher.finalize();
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 digest contains eight routing bytes"),
    )
}

pub fn hash_record_to_shard(
    config: &DistributedOntologyConfig,
    record: &proto::RecordInput,
    shard_count: usize,
) -> usize {
    let hash = hash_record_to_u64(config, record);
    (hash % shard_count as u64) as usize
}

fn global_cluster_id_to_proto(id: GlobalClusterId) -> proto::GlobalClusterId {
    proto::GlobalClusterId {
        shard_id: u32::from(id.shard_id),
        local_id: id.local_id,
        version: u32::from(id.version),
    }
}

#[allow(clippy::result_large_err)]
fn global_cluster_id_from_proto(
    id: &proto::GlobalClusterId,
    field: &str,
) -> Result<GlobalClusterId, Status> {
    Ok(GlobalClusterId::new(
        u16::try_from(id.shard_id)
            .map_err(|_| Status::invalid_argument(format!("{field} shard_id exceeds u16")))?,
        id.local_id,
        u16::try_from(id.version)
            .map_err(|_| Status::invalid_argument(format!("{field} version exceeds u16")))?,
    ))
}

fn cross_shard_conflict_to_proto(
    conflict: &crate::sharding::CrossShardConflict,
) -> proto::CrossShardConflict {
    proto::CrossShardConflict {
        identity_key_signature: Some(proto::IdentityKeySignature {
            signature: conflict.identity_key_signature.to_bytes().to_vec(),
        }),
        cluster1: Some(global_cluster_id_to_proto(conflict.cluster1)),
        cluster2: Some(global_cluster_id_to_proto(conflict.cluster2)),
        interval_start: conflict.interval.start,
        interval_end: conflict.interval.end,
        perspective_hash: conflict.perspective_hash,
        strong_id_hash1: conflict.strong_id_hash1,
        strong_id_hash2: conflict.strong_id_hash2,
    }
}

fn boundary_strong_id_to_proto(
    strong_id: crate::sharding::BoundaryStrongId,
) -> proto::BoundaryStrongId {
    proto::BoundaryStrongId {
        perspective: strong_id.perspective,
        attribute: strong_id.attribute,
        value: strong_id.value,
        interval_start: strong_id.interval.start,
        interval_end: strong_id.interval.end,
    }
}

#[allow(clippy::result_large_err)]
fn boundary_index_from_metadata(
    metadata: &proto::BoundaryMetadata,
) -> Result<crate::sharding::ClusterBoundaryIndex, Status> {
    let shard_id = u16::try_from(metadata.shard_id)
        .map_err(|_| Status::invalid_argument("boundary shard_id exceeds u16"))?;
    let mut boundary_index = crate::sharding::ClusterBoundaryIndex::new_small(shard_id);

    for key_entries in &metadata.entries {
        let signature = key_entries
            .signature
            .as_ref()
            .ok_or_else(|| Status::data_loss("boundary entry is missing its signature"))?;
        let signature_bytes: [u8; 32] = signature
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| Status::data_loss("boundary signature must be 32 bytes"))?;
        let signature = IdentityKeySignature::from_bytes(signature_bytes);

        for entry in &key_entries.entries {
            if entry.shard_id != metadata.shard_id {
                return Err(Status::data_loss(format!(
                    "boundary entry shard_id {} does not match metadata shard_id {}",
                    entry.shard_id, metadata.shard_id
                )));
            }
            let cluster = entry
                .cluster_id
                .as_ref()
                .ok_or_else(|| Status::data_loss("boundary entry is missing its cluster ID"))?;
            let cluster_shard = u16::try_from(cluster.shard_id)
                .map_err(|_| Status::data_loss("boundary cluster shard_id exceeds u16"))?;
            let cluster_version = u16::try_from(cluster.version)
                .map_err(|_| Status::data_loss("boundary cluster version exceeds u16"))?;
            let interval = Interval::new(entry.interval_start, entry.interval_end)
                .map_err(|err| Status::data_loss(err.to_string()))?;
            let strong_ids = entry
                .strong_ids
                .iter()
                .map(|strong_id| {
                    if strong_id.perspective.is_empty()
                        || strong_id.attribute.is_empty()
                        || strong_id.value.is_empty()
                    {
                        return Err(Status::data_loss(
                            "boundary strong-ID fields must not be empty",
                        ));
                    }
                    Ok(crate::sharding::BoundaryStrongId {
                        perspective: strong_id.perspective.clone(),
                        attribute: strong_id.attribute.clone(),
                        value: strong_id.value.clone(),
                        interval: Interval::new(strong_id.interval_start, strong_id.interval_end)
                            .map_err(|err| Status::data_loss(err.to_string()))?,
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
            boundary_index.register_boundary_key_with_conflict_data(
                signature,
                GlobalClusterId::new(cluster_shard, cluster.local_id, cluster_version),
                interval,
                entry.perspective_strong_ids.clone(),
                strong_ids,
            );
        }
    }

    Ok(boundary_index)
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
    restored_checkpoint: Option<ClusterCheckpointManifest>,
    checkpoint_root: Option<PathBuf>,
    ingest_wal: Option<Arc<IngestWal>>,
    ingest_wal_lock: Arc<Mutex<()>>,
    mutation_gate: Arc<tokio::sync::RwLock<()>>,
    ingest_txs: Vec<tokio::sync::mpsc::Sender<IngestJob>>,
    ingest_worker_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    config_version: String,
    metrics: Arc<PerfMetrics>,
    shard_role: proto::ShardRole,
    replica_client: Option<proto::shard_service_client::ShardServiceClient<Channel>>,
    replication_token: Option<String>,
    replication_consistent: Arc<AtomicBool>,
    mutation_consistent: Arc<AtomicBool>,
    replication_gate: Arc<Mutex<()>>,
    /// Destructive RPCs are disabled unless explicitly enabled by the embedding process.
    allow_destructive_admin: bool,
    /// Cross-shard conflicts detected during reconciliation.
    /// These are indirect conflicts where clusters on different shards share
    /// an identity key but have conflicting strong identifiers.
    cross_shard_conflicts: Arc<parking_lot::RwLock<Vec<crate::sharding::CrossShardConflict>>>,
}

const INGEST_QUEUE_CAPACITY: usize = 1024; // Increased from 128 for high-throughput
const DEFAULT_INGEST_WORKERS: usize = 32; // Increased from 16 for high-throughput
const EXPORT_DEFAULT_LIMIT: usize = 1000;
const DIRTY_KEY_PAGE_LIMIT: usize = 50_000;
const RECONCILIATION_KEY_CHUNK: usize = 10_000;
const MERGE_APPLICATION_CHUNK: usize = 50_000;
const CONFLICT_APPLICATION_CHUNK: usize = 10_000;

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

struct ReplicationAttempt {
    consistency: Arc<AtomicBool>,
    armed: bool,
}

struct DurableMutationAttempt {
    consistency: Arc<AtomicBool>,
    armed: bool,
}

impl DurableMutationAttempt {
    fn new(consistency: Arc<AtomicBool>, armed: bool) -> Self {
        Self { consistency, armed }
    }

    fn finish(mut self) {
        self.armed = false;
    }
}

impl Drop for DurableMutationAttempt {
    fn drop(&mut self) {
        if self.armed {
            self.consistency.store(false, Ordering::Release);
        }
    }
}

impl ReplicationAttempt {
    fn new(consistency: Arc<AtomicBool>, armed: bool) -> Self {
        Self { consistency, armed }
    }

    fn finish(mut self, responses_match: bool, operation: &str) -> Result<(), Status> {
        if !responses_match {
            return Err(Status::data_loss(format!(
                "primary and replica returned different results for {operation}; traffic is \
                 blocked"
            )));
        }
        self.armed = false;
        Ok(())
    }
}

impl Drop for ReplicationAttempt {
    fn drop(&mut self) {
        if self.armed {
            self.consistency.store(false, Ordering::Release);
        }
    }
}

fn replication_error_is_ambiguous(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Aborted
            | tonic::Code::Internal
            | tonic::Code::Unavailable
            | tonic::Code::DataLoss
    )
}

fn authenticated_replica_request<T>(
    payload: T,
    replication_token: &str,
) -> Result<Request<T>, Status> {
    let token = tonic::metadata::MetadataValue::try_from(replication_token)
        .map_err(|_| Status::internal("replication token metadata is invalid"))?;
    let mut request = Request::new(payload);
    request
        .metadata_mut()
        .insert(REPLICATION_TOKEN_HEADER, token);
    Ok(request)
}

macro_rules! replicate_to_replica {
    ($self:expr, $method:ident, $payload:expr) => {{
        $self.ensure_replication_consistent()?;
        let replication_serial = if $self.replica_client.is_some() {
            Some($self.replication_gate.lock().await)
        } else {
            None
        };
        $self.ensure_replication_consistent()?;
        let replica_response = if let Some(mut replica) = $self.replica_client.clone() {
            let token = $self.replication_token.as_deref().ok_or_else(|| {
                Status::failed_precondition("primary replication token is missing")
            })?;
            let replica_request = authenticated_replica_request(($payload).clone(), token)?;
            match replica.$method(replica_request).await {
                Ok(response) => Some(response.into_inner()),
                Err(error) => {
                    if replication_error_is_ambiguous(error.code()) {
                        $self.replication_consistent.store(false, Ordering::Release);
                        return Err(Status::aborted(format!(
                            "replica {operation} failed and may have committed; traffic is \
                             blocked: {error}",
                            operation = stringify!($method)
                        )));
                    }
                    $self.replication_consistent.store(false, Ordering::Release);
                    return Err(Status::new(
                        error.code(),
                        format!(
                            "replica rejected {operation}: {}",
                            error.message(),
                            operation = stringify!($method)
                        ),
                    ));
                }
            }
        } else {
            None
        };
        let replication_attempt = ReplicationAttempt::new(
            $self.replication_consistent.clone(),
            replica_response.is_some(),
        );
        (replication_serial, replica_response, replication_attempt)
    }};
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
        Self::new_with_storage_paths(
            shard_id,
            ontology_config,
            tuning,
            data_dir,
            None,
            repair_on_start,
            config_version,
        )
    }

    pub fn new_with_storage_paths(
        shard_id: u32,
        ontology_config: DistributedOntologyConfig,
        tuning: StreamingTuning,
        data_dir: Option<PathBuf>,
        checkpoint_root: Option<PathBuf>,
        repair_on_start: bool,
        config_version: Option<String>,
    ) -> AnyResult<Self> {
        if data_dir.is_none() && checkpoint_root.is_some() {
            anyhow::bail!("checkpoint root requires persistent shard storage");
        }
        let shard_id_u16 = u16::try_from(shard_id)
            .map_err(|_| anyhow::anyhow!("shard_id {shard_id} exceeds u16"))?;
        // Set shard_id in tuning and enable boundary tracking for cross-shard reconciliation.
        // Boundary tracking is REQUIRED for distributed mode - it enables cross-shard conflict
        // detection. Without it, conflicts between records on different shards go undetected.
        let tuning = tuning
            .with_shard_id(shard_id_u16)
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
        let partitioned_requested = is_partitioned_enabled();
        // Partition stores are currently in-memory. Persistent shards must keep every
        // ingest on the PersistentStore path until partitions have a durable backend.
        let use_partitioned = partitioned_requested && data_dir.is_none();
        let num_partitions = partition_count();

        if use_partitioned {
            tracing::info!(shard_id, num_partitions, "Partitioned processing enabled");
        } else if partitioned_requested && data_dir.is_some() {
            tracing::info!(
                shard_id,
                "Partitioned processing disabled for persistent shard"
            );
        }

        // Create concurrent interner FIRST - used for both ontology and records
        let concurrent_interner = Arc::new(ConcurrentInterner::new());

        if let Some(path) = data_dir.clone() {
            let checkpoint_root = if let Some(checkpoint_root) = checkpoint_root {
                fs::create_dir_all(&path)?;
                fs::create_dir_all(&checkpoint_root)?;
                let data_path = path.canonicalize()?;
                let backup_path = checkpoint_root.canonicalize()?;
                if backup_path.starts_with(&data_path) || data_path.starts_with(&backup_path) {
                    anyhow::bail!(
                        "external checkpoint root {} and data directory {} must be disjoint",
                        backup_path.display(),
                        data_path.display()
                    );
                }
                backup_path
            } else {
                let checkpoint_root = path.join("checkpoints");
                fs::create_dir_all(&checkpoint_root)?;
                checkpoint_root.canonicalize()?
            };
            let restored_checkpoint = read_restored_checkpoint_manifest(&path)?;
            if let Some(manifest) = &restored_checkpoint {
                if manifest.shard_id() != shard_id {
                    anyhow::bail!(
                        "restored checkpoint belongs to shard {}, not configured shard {}",
                        manifest.shard_id(),
                        shard_id
                    );
                }
            }
            let (store, config, ontology) =
                load_persistent_state(&path, ontology_config, repair_on_start)?;
            let ingest_wal = Some(Arc::new(IngestWal::new(&path)));
            let mut unirust =
                Unirust::with_store_and_tuning(ontology.clone(), store, tuning.clone());
            let recovery_started = Instant::now();
            unirust.initialize_streaming()?;
            if let Some(wal) = ingest_wal.as_ref() {
                wal.replay(&mut unirust, shard_id)
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            }
            tracing::info!(
                shard_id,
                records = unirust.record_count(),
                elapsed_ms = recovery_started.elapsed().as_millis(),
                "persistent linker recovery completed"
            );
            let recovered_cross_shard_conflicts = unirust.load_cross_shard_conflicts()?;
            let unirust = Arc::new(parking_lot::RwLock::new(unirust));

            // Create partitioned processor if enabled - no RwLock needed!
            // ParallelPartitionedUnirust has per-partition Mutexes for true parallelism
            let partitioned = if use_partitioned {
                let partition_config = PartitionConfig::for_cores(num_partitions);
                let partitioned_unirust = ParallelPartitionedUnirust::new_with_interner(
                    partition_config,
                    Arc::new(ontology),
                    tuning.clone(),
                    concurrent_interner.clone(),
                )?;
                Some(Arc::new(partitioned_unirust))
            } else {
                None
            };

            let (ingest_txs, ingest_worker_handles) =
                spawn_ingest_workers(unirust.clone(), shard_id, worker_count);
            return Ok(Self {
                shard_id,
                unirust,
                partitioned: Arc::new(parking_lot::RwLock::new(partitioned)),
                concurrent_interner,
                tuning,
                ontology_config: Arc::new(Mutex::new(config)),
                data_dir: Some(path),
                restored_checkpoint,
                checkpoint_root: Some(checkpoint_root),
                ingest_wal,
                ingest_wal_lock: Arc::new(Mutex::new(())),
                mutation_gate: Arc::new(tokio::sync::RwLock::new(())),
                ingest_txs,
                ingest_worker_handles: Arc::new(Mutex::new(ingest_worker_handles)),
                config_version,
                metrics: Arc::new(PerfMetrics::new()),
                shard_role: proto::ShardRole::Standalone,
                replica_client: None,
                replication_token: None,
                replication_consistent: Arc::new(AtomicBool::new(true)),
                mutation_consistent: Arc::new(AtomicBool::new(true)),
                replication_gate: Arc::new(Mutex::new(())),
                allow_destructive_admin: false,
                cross_shard_conflicts: Arc::new(parking_lot::RwLock::new(
                    recovered_cross_shard_conflicts,
                )),
            });
        }

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
            let partitioned_unirust = ParallelPartitionedUnirust::new_with_interner(
                partition_config,
                Arc::new(ontology),
                tuning.clone(),
                concurrent_interner.clone(),
            )?;
            Some(Arc::new(partitioned_unirust))
        } else {
            None
        };

        let (ingest_txs, ingest_worker_handles) =
            spawn_ingest_workers(unirust.clone(), shard_id, worker_count);
        Ok(Self {
            shard_id,
            unirust,
            partitioned: Arc::new(parking_lot::RwLock::new(partitioned)),
            concurrent_interner, // Use the same interner used for ontology
            tuning,
            ontology_config: Arc::new(Mutex::new(ontology_config)),
            data_dir: None,
            restored_checkpoint: None,
            checkpoint_root: None,
            ingest_wal,
            ingest_wal_lock: Arc::new(Mutex::new(())),
            mutation_gate: Arc::new(tokio::sync::RwLock::new(())),
            ingest_txs,
            ingest_worker_handles: Arc::new(Mutex::new(ingest_worker_handles)),
            config_version,
            metrics: Arc::new(PerfMetrics::new()),
            shard_role: proto::ShardRole::Standalone,
            replica_client: None,
            replication_token: None,
            replication_consistent: Arc::new(AtomicBool::new(true)),
            mutation_consistent: Arc::new(AtomicBool::new(true)),
            replication_gate: Arc::new(Mutex::new(())),
            allow_destructive_admin: false,
            cross_shard_conflicts: Arc::new(parking_lot::RwLock::new(Vec::new())),
        })
    }

    /// Explicitly enable destructive admin RPCs such as `Reset`.
    ///
    /// Production binaries leave these disabled by default. Prefer an offline
    /// reset of stopped shard data directories whenever possible.
    pub fn with_destructive_admin(mut self, allow: bool) -> Self {
        self.allow_destructive_admin = allow;
        self
    }

    /// Configure this persistent node as a passive replica. A replica can only
    /// be targeted by a primary and is rejected as a router shard endpoint.
    pub fn into_replica(mut self, replication_token: String) -> AnyResult<Self> {
        if self.data_dir.is_none() {
            anyhow::bail!("replica mode requires persistent shard storage");
        }
        if self.replica_client.is_some() {
            anyhow::bail!("a replica cannot have another replica");
        }
        if replication_token.is_empty() {
            anyhow::bail!("replica mode requires a nonempty replication token");
        }
        self.shard_role = proto::ShardRole::Replica;
        self.replication_token = Some(replication_token);
        Ok(self)
    }

    /// Attach a passive replica after proving that its topology, configuration,
    /// restore provenance, and complete durable key/value state match.
    pub async fn with_replica(
        mut self,
        mut replica: proto::shard_service_client::ShardServiceClient<Channel>,
        replication_token: String,
    ) -> Result<Self, Status> {
        if self.data_dir.is_none() {
            return Err(Status::failed_precondition(
                "synchronous replication requires persistent primary storage",
            ));
        }
        if self.shard_role != proto::ShardRole::Standalone || self.replica_client.is_some() {
            return Err(Status::failed_precondition(
                "replication can only be attached to a standalone shard",
            ));
        }
        if replication_token.is_empty() {
            return Err(Status::invalid_argument(
                "replication requires a nonempty shared token",
            ));
        }
        let health = replica
            .health_check(Request::new(proto::HealthCheckRequest {}))
            .await
            .map_err(|error| Status::unavailable(format!("replica health check failed: {error}")))?
            .into_inner();
        if health.status != "ok" {
            return Err(Status::unavailable(format!(
                "replica reported unhealthy status {}",
                health.status
            )));
        }
        let request = authenticated_replica_request(
            proto::ConfigVersionRequest {
                include_durable_state_digest: true,
            },
            &replication_token,
        )?;
        let response = replica
            .get_config_version(request)
            .await
            .map_err(|error| {
                Status::new(
                    error.code(),
                    format!("replica configuration check failed: {}", error.message()),
                )
            })?
            .into_inner();
        validate_distributed_protocol(response.protocol_version)?;
        validate_checkpoint_protocol(response.checkpoint_protocol_version)?;
        if proto::ShardRole::try_from(response.shard_role) != Ok(proto::ShardRole::Replica) {
            return Err(Status::failed_precondition(
                "configured replication target is not running in replica mode",
            ));
        }
        if response.version != self.config_version {
            return Err(Status::failed_precondition(format!(
                "replica config version mismatch: primary {}, replica {}",
                self.config_version, response.version
            )));
        }
        let replica_ontology = response.ontology_config.ok_or_else(|| {
            Status::failed_precondition("replica did not report its ontology configuration")
        })?;
        if map_proto_config(&replica_ontology) != *self.ontology_config.lock().await {
            return Err(Status::failed_precondition(
                "replica ontology does not match the primary",
            ));
        }
        let local_restore = self
            .restored_checkpoint
            .as_ref()
            .map(|manifest| (manifest.generation(), manifest.shard_count()));
        let replica_restore = match (
            response.restore_generation.is_empty(),
            response.restore_shard_count,
        ) {
            (true, 0) => None,
            (false, shard_count) if shard_count > 0 => {
                Some((response.restore_generation.as_str(), shard_count))
            }
            _ => {
                return Err(Status::data_loss(
                    "replica reported incomplete restore checkpoint provenance",
                ));
            }
        };
        if local_restore != replica_restore {
            return Err(Status::failed_precondition(
                "primary and replica restore provenance do not match",
            ));
        }
        let local_digest = self
            .durable_state_digest()?
            .ok_or_else(|| Status::failed_precondition("primary does not expose durable state"))?;
        if response.durable_state_digest.as_slice() != local_digest {
            return Err(Status::failed_precondition(
                "replica durable state does not match the primary; bootstrap both from the same \
                 checkpoint before enabling replication",
            ));
        }
        let metadata = replica
            .get_boundary_metadata(Request::new(proto::GetBoundaryMetadataRequest {
                since_version: u64::MAX,
                signatures: Vec::new(),
            }))
            .await
            .map_err(|error| {
                Status::unavailable(format!("replica shard identity check failed: {error}"))
            })?
            .into_inner()
            .metadata
            .ok_or_else(|| Status::data_loss("replica returned no boundary metadata"))?;
        if metadata.shard_id != self.shard_id {
            return Err(Status::failed_precondition(format!(
                "replica reports shard {}, primary is shard {}",
                metadata.shard_id, self.shard_id
            )));
        }
        self.replica_client = Some(replica);
        self.replication_token = Some(replication_token);
        self.shard_role = proto::ShardRole::Primary;
        Ok(self)
    }

    fn durable_state_digest(&self) -> Result<Option<[u8; 32]>, Status> {
        let db = read_unirust!(self).store().shared_db();
        db.map(|db| {
            crate::persistence::durable_state_digest(&db)
                .map_err(|error| Status::internal(error.to_string()))
        })
        .transpose()
    }

    fn ensure_replication_consistent(&self) -> Result<(), Status> {
        if !self.mutation_consistent.load(Ordering::Acquire) {
            return Err(Status::failed_precondition(
                "a durable mutation failed after it may have changed local state; traffic is \
                 blocked until the shard restarts and recovers",
            ));
        }
        if !self.replication_consistent.load(Ordering::Acquire) {
            return Err(Status::failed_precondition(
                "primary and replica may have diverged; traffic is blocked until their complete \
                 durable states are reconciled",
            ));
        }
        Ok(())
    }

    fn begin_durable_mutation(&self) -> DurableMutationAttempt {
        DurableMutationAttempt::new(self.mutation_consistent.clone(), self.data_dir.is_some())
    }

    fn authorize_mutation<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let supplied = request
            .metadata()
            .get(REPLICATION_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok());
        match self.shard_role {
            proto::ShardRole::Replica => {
                if supplied == self.replication_token.as_deref() {
                    Ok(())
                } else {
                    Err(Status::permission_denied(
                        "passive replicas only accept mutations forwarded by their primary",
                    ))
                }
            }
            proto::ShardRole::Standalone | proto::ShardRole::Primary => {
                if supplied.is_some() {
                    Err(Status::permission_denied(
                        "replicated mutation metadata is only valid on a passive replica",
                    ))
                } else {
                    Ok(())
                }
            }
            proto::ShardRole::Unspecified => {
                Err(Status::failed_precondition("shard role is not configured"))
            }
        }
    }

    /// Drain mutations and durably flush derived state before process exit.
    pub async fn shutdown(&self) -> AnyResult<()> {
        let _mutation_guard = self.mutation_gate.write().await;
        let _wal_guard = self.ingest_wal_lock.lock().await;
        if self.data_dir.is_some() {
            let mut unirust = write_unirust!(self);
            unirust.checkpoint_for_shutdown()?;
            if self
                .ingest_wal
                .as_ref()
                .is_some_and(|wal| wal.has_pending())
            {
                tracing::warn!(
                    shard_id = self.shard_id,
                    "shutdown completed with a pending ingest WAL; it will replay on restart"
                );
            }
        }

        let worker_handles = {
            let mut handles = self.ingest_worker_handles.lock().await;
            std::mem::take(&mut *handles)
        };
        for handle in worker_handles {
            handle.abort();
            let _ = handle.await;
        }
        Ok(())
    }

    fn ensure_no_pending_ingest(&self) -> Result<(), Status> {
        self.ensure_replication_consistent()?;
        if self
            .ingest_wal
            .as_ref()
            .is_some_and(|wal| wal.has_pending())
        {
            return if self.ingest_wal_lock.try_lock().is_ok() {
                Err(Status::failed_precondition(
                    "ingest recovery is pending; restart the shard before serving more traffic",
                ))
            } else {
                Err(Status::unavailable("an ingest commit is in progress"))
            };
        }
        let unirust = read_unirust!(self);
        unirust.ensure_store_healthy().map_err(|err| {
            Status::failed_precondition(format!(
                "persistent store is unhealthy; restart the shard: {err}"
            ))
        })
    }

    fn ensure_recovery_healthy(&self) -> Result<(), Status> {
        self.ensure_replication_consistent()?;
        if self
            .ingest_wal
            .as_ref()
            .is_some_and(|wal| wal.has_pending())
            && self.ingest_wal_lock.try_lock().is_ok()
        {
            return Err(Status::failed_precondition(
                "ingest recovery is pending; restart the shard",
            ));
        }
        let unirust = read_unirust!(self);
        unirust.ensure_store_healthy().map_err(|err| {
            Status::failed_precondition(format!(
                "persistent store is unhealthy; restart the shard: {err}"
            ))
        })
    }

    fn ensure_store_healthy(&self) -> Result<(), Status> {
        self.ensure_replication_consistent()?;
        let unirust = read_unirust!(self);
        unirust.ensure_store_healthy().map_err(|err| {
            Status::failed_precondition(format!(
                "persistent store is unhealthy; restart the shard: {err}"
            ))
        })
    }

    fn ensure_ingest_payloads_idempotent(
        &self,
        records: &[proto::RecordInput],
    ) -> Result<(), Status> {
        let unique_records = validate_record_inputs(records)?;
        let unirust = read_unirust!(self);
        for (identity, incoming_descriptors) in unique_records {
            let Some(existing) = unirust
                .get_record_by_identity(&identity)
                .map_err(|err| Status::failed_precondition(err.to_string()))?
            else {
                continue;
            };

            let mut existing_descriptors = Vec::with_capacity(existing.descriptors.len());
            for descriptor in &existing.descriptors {
                let attr = unirust.resolve_attr(descriptor.attr).ok_or_else(|| {
                    Status::data_loss("stored record references an unknown attribute")
                })?;
                let value = unirust.resolve_value(descriptor.value).ok_or_else(|| {
                    Status::data_loss("stored record references an unknown value")
                })?;
                existing_descriptors.push((
                    attr,
                    value,
                    descriptor.interval.start,
                    descriptor.interval.end,
                ));
            }
            existing_descriptors.sort_unstable();
            if existing_descriptors != incoming_descriptors {
                return Err(Status::already_exists(
                    "source record identity already exists with a different payload",
                ));
            }
        }
        Ok(())
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

    #[allow(clippy::result_large_err)]
    fn validate_import_records(
        unirust: &Unirust,
        snapshots: &[proto::RecordSnapshot],
    ) -> Result<Vec<usize>, Status> {
        let mut records_to_build = Vec::with_capacity(snapshots.len());
        let mut batch_ids: HashMap<u32, &proto::RecordSnapshot> = HashMap::new();
        let mut batch_identities: HashMap<(String, String, String), u32> = HashMap::new();

        for (index, snapshot) in snapshots.iter().enumerate() {
            let identity = snapshot
                .identity
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("record identity is required"))?;
            if identity.entity_type.is_empty()
                || identity.perspective.is_empty()
                || identity.uid.is_empty()
            {
                return Err(Status::invalid_argument(
                    "record identity fields must not be empty",
                ));
            }
            for descriptor in &snapshot.descriptors {
                Interval::new(descriptor.start, descriptor.end)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;
            }
            if let Some(previous) = batch_ids.get(&snapshot.record_id) {
                if **previous != *snapshot {
                    return Err(Status::already_exists(format!(
                        "import batch contains conflicting content for record ID {}",
                        snapshot.record_id
                    )));
                }
                continue;
            }
            batch_ids.insert(snapshot.record_id, snapshot);

            let identity_key = (
                identity.entity_type.clone(),
                identity.perspective.clone(),
                identity.uid.clone(),
            );
            if let Some(previous_id) =
                batch_identities.insert(identity_key.clone(), snapshot.record_id)
            {
                if previous_id != snapshot.record_id {
                    return Err(Status::already_exists(format!(
                        "import batch maps one record identity to IDs {previous_id} and {}",
                        snapshot.record_id
                    )));
                }
            }

            if let Some(existing) = unirust.get_record(RecordId(snapshot.record_id)) {
                if Self::record_to_snapshot(unirust, &existing) != *snapshot {
                    return Err(Status::already_exists(format!(
                        "record ID {} already exists with different content",
                        snapshot.record_id
                    )));
                }
                continue;
            }

            let record_identity =
                RecordIdentity::new(identity_key.0, identity_key.1, identity_key.2);
            if let Some(existing_id) = unirust.store().get_record_id_by_identity(&record_identity) {
                return Err(Status::already_exists(format!(
                    "record identity already exists under ID {} instead of requested ID {}",
                    existing_id.0, snapshot.record_id
                )));
            }
            records_to_build.push(index);
        }
        unirust
            .ensure_store_healthy()
            .map_err(|err| Status::failed_precondition(err.to_string()))?;
        Ok(records_to_build)
    }

    #[allow(clippy::result_large_err)]
    fn build_import_records(
        unirust: &mut Unirust,
        snapshots: &[proto::RecordSnapshot],
        records_to_build: &[usize],
    ) -> Result<Vec<Record>, Status> {
        let mut records = Vec::with_capacity(records_to_build.len());
        for &index in records_to_build {
            let snapshot = &snapshots[index];
            let identity = snapshot
                .identity
                .as_ref()
                .ok_or_else(|| Status::internal("validated record identity is missing"))?;
            records.push(Self::build_record_with_id(
                unirust,
                snapshot.record_id,
                identity,
                &snapshot.descriptors,
            )?);
        }
        Ok(records)
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
fn resolve_checkpoint_path(checkpoint_root: &Path, requested: &str) -> Result<PathBuf, Status> {
    if requested.is_empty() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| Status::internal(err.to_string()))?
            .as_nanos();
        return Ok(checkpoint_root.join(format!("{timestamp}")));
    }
    let candidate = PathBuf::from(requested);
    if candidate.is_absolute()
        || candidate
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(Status::invalid_argument(
            "checkpoint path must be relative and remain within the shard checkpoint directory",
        ));
    }
    Ok(checkpoint_root.join(candidate))
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
) -> (
    Vec<mpsc::Sender<IngestJob>>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let mut senders = Vec::with_capacity(worker_count);
    let mut handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (tx, mut rx) = mpsc::channel::<IngestJob>(INGEST_QUEUE_CAPACITY);
        let worker_unirust = unirust.clone();
        let handle = tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                let result = {
                    let mut guard = worker_unirust.write();
                    process_ingest_batch(&mut guard, shard_id, &job.records)
                };
                let _ = job.respond_to.send(result);
            }
        });
        senders.push(tx);
        handles.push(handle);
    }
    (senders, handles)
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
    let worker_index = ingest_worker_index(&records[0], worker_count);
    let (tx, rx) = oneshot::channel();
    ingest_txs[worker_index]
        .send(IngestJob {
            records,
            respond_to: tx,
        })
        .await
        .map_err(|_| Status::unavailable("ingest queue unavailable"))?;
    let mut assignments = rx
        .await
        .map_err(|_| Status::internal("ingest worker dropped"))??;

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
            .map(|record| {
                let built = ShardNode::build_record_concurrent(interner, record)?;
                let partition_id =
                    compute_partition_id_for_record(&built, ontology, interner, partition_count);
                Ok((partition_id, record.index, built))
            })
            .collect::<Result<Vec<_>, Status>>()?
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
    let partition_results = partitioned
        .ingest_batch_with_partitions(indexed_records)
        .map_err(|err| Status::internal(err.to_string()))?;

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

fn build_global_conflict_summaries(
    config: DistributedOntologyConfig,
    snapshots: Vec<proto::RecordSnapshot>,
) -> AnyResult<Vec<proto::ConflictSummary>> {
    let mut store = crate::Store::new();
    let ontology = config.build_ontology(&mut store);
    let mut unirust = Unirust::with_store_and_tuning(
        ontology,
        store,
        StreamingTuning::from_profile(crate::TuningProfile::Balanced),
    );
    let mut batch = Vec::with_capacity(1_000);

    for snapshot in snapshots {
        let identity = snapshot
            .identity
            .ok_or_else(|| anyhow::anyhow!("exported record is missing its identity"))?;
        let mut descriptors = Vec::with_capacity(snapshot.descriptors.len());
        for descriptor in snapshot.descriptors {
            descriptors.push(crate::Descriptor::new(
                unirust.intern_attr(&descriptor.attr),
                unirust.intern_value(&descriptor.value),
                Interval::new(descriptor.start, descriptor.end)?,
            ));
        }
        batch.push(Record::new(
            RecordId(0),
            RecordIdentity::new(identity.entity_type, identity.perspective, identity.uid),
            descriptors,
        ));
        if batch.len() == 1_000 {
            unirust.stream_records(std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        unirust.stream_records(batch)?;
    }

    let clusters = unirust.build_clusters()?;
    let observations = unirust.detect_conflicts(&clusters)?;
    let summaries = unirust
        .summarize_conflicts(&observations)
        .into_iter()
        .map(to_proto_conflict_summary)
        .collect::<Vec<_>>();
    Ok(summaries)
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

    async fn reserve_source_records(
        &self,
        request: Request<proto::ReserveSourceRecordsRequest>,
    ) -> Result<Response<proto::ReserveSourceRecordsResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_store_healthy()?;
        let request = request.into_inner();
        let replication_request = request.clone();
        let mut reservations = Vec::with_capacity(request.reservations.len());
        let mut indices = Vec::with_capacity(request.reservations.len());

        for reservation in request.reservations {
            let identity = reservation
                .identity
                .ok_or_else(|| Status::invalid_argument("reservation identity is required"))?;
            if identity.entity_type.is_empty()
                || identity.perspective.is_empty()
                || identity.uid.is_empty()
            {
                return Err(Status::invalid_argument(
                    "reservation identity fields must not be empty",
                ));
            }
            let payload_digest: [u8; 32] = reservation
                .payload_digest
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("payload digest must be 32 bytes"))?;
            indices.push(reservation.index);
            reservations.push(SourceRecordReservation {
                identity: RecordIdentity::new(
                    identity.entity_type,
                    identity.perspective,
                    identity.uid,
                ),
                payload_digest,
                target_shard_id: reservation.target_shard_id,
            });
        }

        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, reserve_source_records, replication_request);
        let targets = {
            let mut unirust = write_unirust!(self);
            unirust
                .store_mut()
                .reserve_source_records(&reservations)
                .map_err(|err| {
                    if let Some(conflict) = err.downcast_ref::<SourceReservationError>() {
                        match conflict {
                            SourceReservationError::PayloadConflict => {
                                Status::already_exists(conflict.to_string())
                            }
                            SourceReservationError::TargetConflict { .. } => {
                                Status::failed_precondition(conflict.to_string())
                            }
                        }
                    } else {
                        Status::internal(err.to_string())
                    }
                })?
        };
        let reservations = indices
            .into_iter()
            .zip(targets)
            .map(
                |(index, target_shard_id)| proto::SourceRecordReservationResult {
                    index,
                    target_shard_id,
                },
            )
            .collect();
        let response = proto::ReserveSourceRecordsResponse { reservations };
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "source reservation",
        )?;
        Ok(Response::new(response))
    }

    async fn mark_source_reservations_backfilled(
        &self,
        request: Request<proto::MarkSourceReservationsBackfilledRequest>,
    ) -> Result<Response<proto::MarkSourceReservationsBackfilledResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_store_healthy()?;
        let request = request.into_inner();
        if request.protocol_version != DISTRIBUTED_PROTOCOL_VERSION {
            return Err(Status::failed_precondition(format!(
                "cannot mark source reservations for protocol {}; shard requires {}",
                request.protocol_version, DISTRIBUTED_PROTOCOL_VERSION
            )));
        }
        if request.shard_count == 0 {
            return Err(Status::invalid_argument(
                "source reservation shard count must be greater than zero",
            ));
        }

        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, mark_source_reservations_backfilled, request);
        let mut unirust = write_unirust!(self);
        unirust
            .store_mut()
            .mark_source_reservation_backfill(request.protocol_version, request.shard_count)
            .map_err(|err| Status::internal(err.to_string()))?;
        let response = proto::MarkSourceReservationsBackfilledResponse {};
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "source reservation migration marker",
        )?;
        Ok(Response::new(response))
    }

    async fn set_ontology(
        &self,
        request: Request<proto::ApplyOntologyRequest>,
    ) -> Result<Response<proto::ApplyOntologyResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.write().await;
        self.ensure_no_pending_ingest()?;
        let request = request.into_inner();
        let replication_request = request.clone();
        let config = request
            .config
            .ok_or_else(|| Status::invalid_argument("ontology config is required"))?;

        let config = map_proto_config(&config);
        let mut config_guard = self.ontology_config.lock().await;
        if *config_guard == config {
            return Ok(Response::new(proto::ApplyOntologyResponse {}));
        }

        let record_count = {
            let guard = read_unirust!(self);
            guard.record_count()
        };
        if record_count != 0 {
            return Err(Status::failed_precondition(format!(
                "refusing to replace ontology on a nonempty shard ({record_count} records); \
                 reset the cluster explicitly before changing ontology"
            )));
        }

        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, set_ontology, replication_request);
        let local_attempt = self.begin_durable_mutation();
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
        self.cross_shard_conflicts.write().clear();

        // Rebuild partitioned processor with the new ontology if enabled
        // CRITICAL: The partitioned processor needs the ontology with strong identifiers
        // for conflict detection to work!
        if is_partitioned_enabled() && self.data_dir.is_none() {
            let num_partitions = partition_count();
            let partition_config = PartitionConfig::for_cores(num_partitions);
            // Build ontology using concurrent interner to ensure AttrIds match records
            let ontology = config.build_ontology_with_interner(&self.concurrent_interner);
            let new_partitioned = ParallelPartitionedUnirust::new_with_interner(
                partition_config,
                Arc::new(ontology),
                self.tuning.clone(),
                self.concurrent_interner.clone(),
            )
            .map_err(|err| Status::internal(err.to_string()))?;

            let mut partitioned_guard = self.partitioned.write();
            *partitioned_guard = Some(Arc::new(new_partitioned));
            tracing::info!("Partitioned processor rebuilt with updated ontology");
        } else {
            *self.partitioned.write() = None;
        }

        *config_guard = config;
        local_attempt.finish();
        let response = proto::ApplyOntologyResponse {};
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "ontology update",
        )?;
        Ok(Response::new(response))
    }

    async fn ingest_records(
        &self,
        request: Request<proto::IngestRecordsRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
        let start = Instant::now();
        let request = request.into_inner();
        validate_distributed_protocol(request.internal_protocol_version)?;
        let records = request.records;
        let record_count = records.len();
        let _wal_guard = if self.ingest_wal.is_some() {
            Some(self.ingest_wal_lock.lock().await)
        } else {
            None
        };
        self.ensure_store_healthy()?;
        self.ensure_ingest_payloads_idempotent(&records)?;

        let replication_request = proto::IngestRecordsRequest {
            records: records.clone(),
            internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
        };
        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, ingest_records, replication_request);
        let local_attempt = self.begin_durable_mutation();
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
        local_attempt.finish();
        let response = proto::IngestRecordsResponse { assignments };
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "record ingest",
        )?;
        Ok(Response::new(response))
    }

    async fn ingest_records_stream(
        &self,
        request: Request<tonic::Streaming<proto::IngestRecordsChunk>>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
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
            validate_distributed_protocol(chunk.internal_protocol_version)?;
            record_count += chunk.records.len();
            let _wal_guard = if self.ingest_wal.is_some() {
                Some(self.ingest_wal_lock.lock().await)
            } else {
                None
            };
            self.ensure_store_healthy()?;
            self.ensure_ingest_payloads_idempotent(&chunk.records)?;

            let replication_request = proto::IngestRecordsRequest {
                records: chunk.records.clone(),
                internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
            };
            let (_replication_serial, replica_response, replication_attempt) =
                replicate_to_replica!(self, ingest_records, replication_request);
            let local_attempt = self.begin_durable_mutation();
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
            replication_attempt.finish(
                replica_response
                    .as_ref()
                    .is_none_or(|replica| replica.assignments == batch_assignments),
                "streamed record ingest",
            )?;
            local_attempt.finish();
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
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
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
                                let global_id = unirust
                                    .global_cluster_id_for_record(entry.root_record_id)
                                    .map_err(|err| Status::internal(err.to_string()))?;
                                Ok(Self::to_proto_match(
                                    u32::from(global_id.shard_id),
                                    crate::model::ClusterId(global_id.local_id),
                                    entry.interval,
                                    &entry.golden,
                                    entry.cluster_key,
                                    entry.cluster_key_identity,
                                ))
                            })
                            .collect::<Result<Vec<_>, Status>>()?,
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
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
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
            cross_shard_conflicts: self.cross_shard_conflicts.read().len() as u64,
            boundary_keys_tracked: boundary_keys,
        }))
    }

    async fn health_check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        self.ensure_recovery_healthy()?;
        if let Some(mut replica) = self.replica_client.clone() {
            let response = replica
                .health_check(Request::new(proto::HealthCheckRequest {}))
                .await
                .map_err(|error| {
                    Status::unavailable(format!("replica health check failed: {error}"))
                })?
                .into_inner();
            if response.status != "ok" {
                return Err(Status::unavailable(format!(
                    "replica reported unhealthy status {}",
                    response.status
                )));
            }
        }
        Ok(Response::new(proto::HealthCheckResponse {
            status: "ok".to_string(),
        }))
    }

    async fn get_config_version(
        &self,
        request: Request<proto::ConfigVersionRequest>,
    ) -> Result<Response<proto::ConfigVersionResponse>, Status> {
        let include_durable_state_digest = request.get_ref().include_durable_state_digest;
        if include_durable_state_digest && self.shard_role == proto::ShardRole::Replica {
            self.authorize_mutation(&request)?;
        }
        let _mutation_guard = if include_durable_state_digest {
            Some(self.mutation_gate.write().await)
        } else {
            None
        };
        let ontology_config = self.ontology_config.lock().await;
        let source_reservation_backfill = read_unirust!(self)
            .store()
            .source_reservation_backfill()
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(proto::ConfigVersionResponse {
            version: self.config_version.clone(),
            ontology_config: Some(to_proto_config(&ontology_config)),
            protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
            source_reservation_backfill_version: source_reservation_backfill
                .map(|value| value.0)
                .unwrap_or_default(),
            source_reservation_shard_count: source_reservation_backfill
                .map(|value| value.1)
                .unwrap_or_default(),
            checkpoint_protocol_version: CHECKPOINT_PROTOCOL_VERSION,
            restore_generation: self
                .restored_checkpoint
                .as_ref()
                .map(|manifest| manifest.generation().to_string())
                .unwrap_or_default(),
            restore_shard_count: self
                .restored_checkpoint
                .as_ref()
                .map(ClusterCheckpointManifest::shard_count)
                .unwrap_or_default(),
            shard_role: self.shard_role as i32,
            durable_state_digest: if include_durable_state_digest {
                self.durable_state_digest()?
                    .map(|digest| digest.to_vec())
                    .unwrap_or_default()
            } else {
                Vec::new()
            },
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
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.write().await;
        self.ensure_no_pending_ingest()?;
        let request = request.into_inner();
        validate_checkpoint_protocol(request.checkpoint_protocol_version)?;
        if request.shard_count == 0 || self.shard_id >= request.shard_count {
            return Err(Status::invalid_argument(format!(
                "checkpoint shard {} is outside cluster shard count {}",
                self.shard_id, request.shard_count
            )));
        }
        let checkpoint_root = self
            .checkpoint_root
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("checkpoint requires persistent storage"))?;
        let target = resolve_checkpoint_path(checkpoint_root, &request.path)?;
        let parent = target
            .parent()
            .ok_or_else(|| Status::internal("invalid checkpoint path"))?;
        fs::create_dir_all(parent).map_err(|err| Status::internal(err.to_string()))?;
        let parent = parent
            .canonicalize()
            .map_err(|err| Status::internal(err.to_string()))?;
        if !parent.starts_with(checkpoint_root) {
            return Err(Status::invalid_argument(
                "checkpoint path traverses outside the shard checkpoint directory",
            ));
        }
        if target
            .symlink_metadata()
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            return Err(Status::invalid_argument(
                "checkpoint target must not be a symbolic link",
            ));
        }

        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, checkpoint, request);
        if request.finalize {
            commit_cluster_checkpoint(&target, &request.path, self.shard_id, request.shard_count)
                .map_err(|err| Status::failed_precondition(err.to_string()))?;
        } else if target.exists() {
            validate_prepared_cluster_checkpoint(
                &target,
                &request.path,
                self.shard_id,
                request.shard_count,
            )
            .map_err(|err| Status::already_exists(err.to_string()))?;
        } else {
            let mut unirust = write_unirust!(self);
            unirust
                .checkpoint_for_shutdown()
                .map_err(|err| Status::internal(err.to_string()))?;
            unirust
                .checkpoint_to_path(&target)
                .map_err(|err| Status::internal(err.to_string()))?;
            prepare_cluster_checkpoint(&target, &request.path, self.shard_id, request.shard_count)
                .map_err(|err| Status::internal(err.to_string()))?;
        }
        let response = proto::CheckpointResponse {
            paths: vec![target.to_string_lossy().to_string()],
            generation: request.path,
            committed: request.finalize,
        };
        replication_attempt.finish(
            replica_response.as_ref().is_none_or(|replica| {
                replica.generation == response.generation
                    && replica.committed == response.committed
                    && replica.paths.len() == 1
            }),
            "checkpoint",
        )?;
        Ok(Response::new(response))
    }

    async fn get_record_id_range(
        &self,
        _request: Request<proto::RecordIdRangeRequest>,
    ) -> Result<Response<proto::RecordIdRangeResponse>, Status> {
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
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
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
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
        let mutation_guard = self.mutation_gate.clone().read_owned().await;
        self.ensure_no_pending_ingest()?;
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
            let _mutation_guard = mutation_guard;
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
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
        let request = request.into_inner();
        validate_distributed_protocol(request.internal_protocol_version)?;
        if request.records.is_empty() {
            return Ok(Response::new(proto::ImportRecordsResponse { imported: 0 }));
        }
        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, import_records, request);
        let mut unirust = write_unirust!(self);
        let records_to_build = Self::validate_import_records(&unirust, &request.records)?;
        let local_attempt = self.begin_durable_mutation();
        let records =
            Self::build_import_records(&mut unirust, &request.records, &records_to_build)?;
        unirust
            .ingest_with_explicit_ids(records)
            .map_err(|err| Status::internal(err.to_string()))?;
        let response = proto::ImportRecordsResponse {
            imported: request.records.len() as u64,
        };
        local_attempt.finish();
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "record import",
        )?;
        Ok(Response::new(response))
    }

    async fn import_records_stream(
        &self,
        request: Request<tonic::Streaming<proto::ImportRecordsChunk>>,
    ) -> Result<Response<proto::ImportRecordsResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
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
            validate_distributed_protocol(chunk.internal_protocol_version)?;
            let replication_request = proto::ImportRecordsRequest {
                records: chunk.records.clone(),
                internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
            };
            let (_replication_serial, replica_response, replication_attempt) =
                replicate_to_replica!(self, import_records, replication_request);
            let mut unirust = write_unirust!(self);
            let records_to_build = Self::validate_import_records(&unirust, &chunk.records)?;
            let local_attempt = self.begin_durable_mutation();
            let records =
                Self::build_import_records(&mut unirust, &chunk.records, &records_to_build)?;
            unirust
                .ingest_with_explicit_ids(records)
                .map_err(|err| Status::internal(err.to_string()))?;
            let response = proto::ImportRecordsResponse {
                imported: chunk.records.len() as u64,
            };
            local_attempt.finish();
            replication_attempt.finish(
                replica_response
                    .as_ref()
                    .is_none_or(|replica| replica == &response),
                "streamed record import",
            )?;
            imported += chunk.records.len() as u64;
        }
        Ok(Response::new(proto::ImportRecordsResponse { imported }))
    }

    async fn list_conflicts(
        &self,
        request: Request<proto::ListConflictsRequest>,
    ) -> Result<Response<proto::ListConflictsResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
        let request = request.into_inner();
        let replication_request = request.clone();
        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, list_conflicts, replication_request);
        let local_attempt = self.begin_durable_mutation();
        let mut summaries = if let Some(cache) = {
            let guard = read_unirust!(self);
            guard.cached_conflict_summaries()
        } {
            cache
        } else if let Some(persisted) = {
            let guard = read_unirust!(self);
            guard
                .load_conflict_summaries()
                .map_err(|err| Status::failed_precondition(err.to_string()))?
        } {
            let mut unirust = write_unirust!(self);
            unirust
                .set_conflict_cache(persisted.clone())
                .map_err(|err| Status::internal(err.to_string()))?;
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
            unirust
                .set_conflict_cache(summaries.clone())
                .map_err(|err| Status::internal(err.to_string()))?;
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
        local_attempt.finish();
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "conflict materialization",
        )?;
        Ok(Response::new(response))
    }

    async fn reset(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        self.authorize_mutation(&request)?;
        if !self.allow_destructive_admin {
            return Err(Status::permission_denied(
                "destructive reset RPC is disabled; stop the cluster and use the confirmed \
                 offline reset procedure",
            ));
        }
        let _mutation_guard = self.mutation_gate.write().await;
        self.ensure_no_pending_ingest()?;
        if self.replica_client.is_some() {
            return Err(Status::failed_precondition(
                "online reset is disabled while replication is configured; stop both nodes and \
                 reset or rebootstrap them together",
            ));
        }
        let local_attempt = self.begin_durable_mutation();
        let config = self.ontology_config.lock().await.clone();
        {
            let mut guard = write_unirust!(self);
            guard
                .reset_with_ontology(config.ontology_template())
                .map_err(|err| Status::internal(err.to_string()))?;
        }

        if is_partitioned_enabled() && self.data_dir.is_none() {
            let partition_config = PartitionConfig::for_cores(partition_count());
            let ontology = config.build_ontology_with_interner(&self.concurrent_interner);
            let partitioned = ParallelPartitionedUnirust::new_with_interner(
                partition_config,
                Arc::new(ontology),
                self.tuning.clone(),
                self.concurrent_interner.clone(),
            )
            .map_err(|err| Status::internal(err.to_string()))?;
            *self.partitioned.write() = Some(Arc::new(partitioned));
        } else {
            *self.partitioned.write() = None;
        }
        self.cross_shard_conflicts.write().clear();
        local_attempt.finish();
        Ok(Response::new(proto::Empty {}))
    }

    async fn get_boundary_metadata(
        &self,
        request: Request<proto::GetBoundaryMetadataRequest>,
    ) -> Result<Response<proto::GetBoundaryMetadataResponse>, Status> {
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
        let request = request.into_inner();
        let mut signatures = std::collections::HashSet::with_capacity(request.signatures.len());
        for signature in request.signatures {
            let bytes = <[u8; 32]>::try_from(signature.signature.as_slice())
                .map_err(|_| Status::invalid_argument("boundary signature must be 32 bytes"))?;
            signatures.insert(IdentityKeySignature::from_bytes(bytes));
        }

        let unirust = read_unirust!(self);
        let boundary_index = if signatures.is_empty() {
            None
        } else {
            unirust.export_boundary_index_for_signatures(&signatures)
        };

        let metadata = match boundary_index {
            Some(index) => {
                let exported = index.export_metadata();
                if exported.version <= request.since_version {
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
                                        strong_ids: e
                                            .strong_ids
                                            .into_iter()
                                            .map(boundary_strong_id_to_proto)
                                            .collect(),
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
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
        let req = request.into_inner();
        let replication_request = req;

        let primary = req
            .primary
            .ok_or_else(|| Status::invalid_argument("primary cluster ID is required"))?;
        let secondary = req
            .secondary
            .ok_or_else(|| Status::invalid_argument("secondary cluster ID is required"))?;

        let primary_id = global_cluster_id_from_proto(&primary, "primary")?;
        let secondary_id = global_cluster_id_from_proto(&secondary, "secondary")?;
        if primary_id == secondary_id {
            return Err(Status::invalid_argument(
                "primary and secondary cluster IDs must differ",
            ));
        }
        if primary_id.to_u64() >= secondary_id.to_u64() {
            return Err(Status::invalid_argument(
                "cross-shard merge must redirect the higher cluster ID to the lower canonical ID",
            ));
        }

        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, apply_merge, replication_request);
        let local_attempt = self.begin_durable_mutation();
        // Get write lock on unirust
        let mut unirust = write_unirust!(self);

        // Apply the merge via the streaming linker's DSU
        let response = match unirust.apply_cross_shard_merge(primary_id, secondary_id) {
            Ok(records_updated) => proto::ApplyMergeResponse {
                success: true,
                records_updated: records_updated as u32,
                error: String::new(),
            },
            Err(err) => proto::ApplyMergeResponse {
                success: false,
                records_updated: 0,
                error: err.to_string(),
            },
        };
        if response.success {
            local_attempt.finish();
        }
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "cross-shard merge",
        )?;
        Ok(Response::new(response))
    }

    async fn apply_merges(
        &self,
        request: Request<proto::ApplyMergesRequest>,
    ) -> Result<Response<proto::ApplyMergesResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
        let req = request.into_inner();
        let replication_request = req.clone();
        let mut merges = Vec::with_capacity(req.merges.len());
        let mut primaries_by_secondary = std::collections::HashMap::with_capacity(req.merges.len());
        for merge in &req.merges {
            let primary = merge
                .primary
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("merge primary is required"))?;
            let secondary = merge
                .secondary
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("merge secondary is required"))?;
            let primary = global_cluster_id_from_proto(primary, "primary")?;
            let secondary = global_cluster_id_from_proto(secondary, "secondary")?;
            if primary == secondary {
                return Err(Status::invalid_argument(
                    "primary and secondary cluster IDs must differ",
                ));
            }
            if primary.to_u64() >= secondary.to_u64() {
                return Err(Status::invalid_argument(
                    "cross-shard merge must redirect the higher cluster ID to the lower canonical ID",
                ));
            }
            if let Some(existing) = primaries_by_secondary.insert(secondary, primary) {
                if existing != primary {
                    return Err(Status::invalid_argument(
                        "cross-shard merge batch redirects one secondary to multiple primaries",
                    ));
                }
                continue;
            }
            merges.push((primary, secondary));
        }

        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, apply_merges, replication_request);
        let local_attempt = self.begin_durable_mutation();
        let mut unirust = write_unirust!(self);
        let response = match unirust.apply_cross_shard_merges(&merges) {
            Ok(records_updated) => proto::ApplyMergesResponse {
                success: true,
                records_updated: records_updated as u64,
                error: String::new(),
            },
            Err(err) => proto::ApplyMergesResponse {
                success: false,
                records_updated: 0,
                error: err.to_string(),
            },
        };
        if response.success {
            local_attempt.finish();
        }
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "cross-shard merge batch",
        )?;
        Ok(Response::new(response))
    }

    async fn get_dirty_boundary_keys(
        &self,
        request: Request<proto::GetDirtyBoundaryKeysRequest>,
    ) -> Result<Response<proto::GetDirtyBoundaryKeysResponse>, Status> {
        let _mutation_guard = self.mutation_gate.read().await;
        let _wal_guard = if self.ingest_wal.is_some() {
            Some(self.ingest_wal_lock.lock().await)
        } else {
            None
        };
        if self
            .ingest_wal
            .as_ref()
            .is_some_and(|wal| wal.has_pending())
        {
            return Err(Status::failed_precondition(
                "ingest recovery is pending; restart the shard",
            ));
        }
        self.ensure_store_healthy()?;
        let request = request.into_inner();
        let after_signature = if request.after_signature.is_empty() {
            None
        } else {
            Some(
                <[u8; 32]>::try_from(request.after_signature.as_slice())
                    .map(IdentityKeySignature::from_bytes)
                    .map_err(|_| Status::invalid_argument("dirty-key cursor must be 32 bytes"))?,
            )
        };
        let limit = if request.limit == 0 {
            DIRTY_KEY_PAGE_LIMIT
        } else {
            usize::try_from(request.limit)
                .map_err(|_| Status::invalid_argument("dirty-key limit exceeds usize"))?
        };
        if limit > DIRTY_KEY_PAGE_LIMIT {
            return Err(Status::invalid_argument(format!(
                "dirty-key limit must not exceed {DIRTY_KEY_PAGE_LIMIT}"
            )));
        }
        let unirust = read_unirust!(self);
        let partitioned_guard = self.partitioned.read();

        let candidate_limit = limit.saturating_add(1);
        let mut all_dirty_keys = unirust
            .dirty_boundary_key_candidates(after_signature, candidate_limit)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        if let Some(partitioned) = partitioned_guard.as_ref() {
            all_dirty_keys.extend(
                partitioned.dirty_boundary_key_candidates(after_signature, candidate_limit),
            );
        }
        let mut page = all_dirty_keys
            .into_iter()
            .take(candidate_limit)
            .collect::<Vec<_>>();
        let has_more = page.len() > limit;
        if has_more {
            page.pop();
        }
        let next_after_signature = if has_more {
            page.last()
                .map_or_else(Vec::new, |signature| signature.to_bytes().to_vec())
        } else {
            Vec::new()
        };
        let dirty_keys = page
            .into_iter()
            .map(|signature| proto::DirtyBoundaryKey {
                signature: Some(proto::IdentityKeySignature {
                    signature: signature.to_bytes().to_vec(),
                }),
                entries: Vec::new(),
            })
            .collect();

        Ok(Response::new(proto::GetDirtyBoundaryKeysResponse {
            dirty_keys,
            shard_id: self.shard_id,
            next_after_signature,
            has_more,
        }))
    }

    async fn clear_dirty_keys(
        &self,
        request: Request<proto::ClearDirtyKeysRequest>,
    ) -> Result<Response<proto::ClearDirtyKeysResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_no_pending_ingest()?;
        let req = request.into_inner();
        let replication_request = req.clone();

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

        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, clear_dirty_keys, replication_request);
        let mut unirust = write_unirust!(self);
        let partitioned_guard = self.partitioned.read();
        let keys_cleared = keys.len() as u32;
        unirust.clear_dirty_boundary_keys(&keys);

        // Also clear on partitioned processor
        if let Some(partitioned) = partitioned_guard.as_ref() {
            partitioned.clear_dirty_boundary_keys(&keys);
        }

        let response = proto::ClearDirtyKeysResponse { keys_cleared };
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "dirty boundary key clear",
        )?;
        Ok(Response::new(response))
    }

    async fn store_cross_shard_conflicts(
        &self,
        request: Request<proto::StoreCrossShardConflictsRequest>,
    ) -> Result<Response<proto::StoreCrossShardConflictsResponse>, Status> {
        self.authorize_mutation(&request)?;
        let _mutation_guard = self.mutation_gate.write().await;
        self.ensure_no_pending_ingest()?;
        let req = request.into_inner();
        let replication_request = req.clone();
        let conflicts: Vec<crate::sharding::CrossShardConflict> = req
            .conflicts
            .into_iter()
            .map(|c| {
                let sig = c.identity_key_signature.ok_or_else(|| {
                    Status::invalid_argument("cross-shard conflict signature is required")
                })?;
                if sig.signature.len() != 32 {
                    return Err(Status::invalid_argument(
                        "cross-shard conflict signature must be 32 bytes",
                    ));
                }
                let mut sig_bytes = [0u8; 32];
                sig_bytes.copy_from_slice(&sig.signature);

                let cluster1 = c.cluster1.ok_or_else(|| {
                    Status::invalid_argument("cross-shard conflict cluster1 is required")
                })?;
                let cluster2 = c.cluster2.ok_or_else(|| {
                    Status::invalid_argument("cross-shard conflict cluster2 is required")
                })?;
                let cluster1_shard = u16::try_from(cluster1.shard_id)
                    .map_err(|_| Status::invalid_argument("cluster1 shard_id exceeds u16"))?;
                let cluster1_version = u16::try_from(cluster1.version)
                    .map_err(|_| Status::invalid_argument("cluster1 version exceeds u16"))?;
                let cluster2_shard = u16::try_from(cluster2.shard_id)
                    .map_err(|_| Status::invalid_argument("cluster2 shard_id exceeds u16"))?;
                let cluster2_version = u16::try_from(cluster2.version)
                    .map_err(|_| Status::invalid_argument("cluster2 version exceeds u16"))?;
                let interval = Interval::new(c.interval_start, c.interval_end)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;

                Ok(crate::sharding::CrossShardConflict {
                    identity_key_signature: crate::sharding::IdentityKeySignature::from_bytes(
                        sig_bytes,
                    ),
                    cluster1: GlobalClusterId::new(
                        cluster1_shard,
                        cluster1.local_id,
                        cluster1_version,
                    ),
                    cluster2: GlobalClusterId::new(
                        cluster2_shard,
                        cluster2.local_id,
                        cluster2_version,
                    ),
                    interval,
                    perspective_hash: c.perspective_hash,
                    strong_id_hash1: c.strong_id_hash1,
                    strong_id_hash2: c.strong_id_hash2,
                })
            })
            .collect::<Result<_, Status>>()?;

        let (_replication_serial, replica_response, replication_attempt) =
            replicate_to_replica!(self, store_cross_shard_conflicts, replication_request);
        let local_attempt = self.begin_durable_mutation();
        // Store conflicts - keep only those relevant to this shard
        let shard_id = self.shard_id as u16;
        let mut conflict_storage = self.cross_shard_conflicts.write();
        let mut updated = conflict_storage.clone();
        let mut stored_count = 0u32;
        for conflict in conflicts {
            if conflict.cluster1.shard_id == shard_id || conflict.cluster2.shard_id == shard_id {
                if !updated.contains(&conflict) {
                    updated.push(conflict);
                    stored_count = stored_count.saturating_add(1);
                }
            }
        }
        if stored_count != 0 {
            let mut unirust = write_unirust!(self);
            unirust
                .persist_cross_shard_conflicts(&updated)
                .map_err(|err| Status::internal(err.to_string()))?;
            *conflict_storage = updated;
        }

        let response = proto::StoreCrossShardConflictsResponse { stored_count };
        local_attempt.finish();
        replication_attempt.finish(
            replica_response
                .as_ref()
                .is_none_or(|replica| replica == &response),
            "cross-shard conflict storage",
        )?;
        Ok(Response::new(response))
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

/// Failure bounds for router-to-shard transport and RPC calls.
#[derive(Clone)]
pub struct RouterRpcConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub tcp_keepalive: Duration,
    pub shard_mtls: Option<tonic::transport::ClientTlsConfig>,
}

impl std::fmt::Debug for RouterRpcConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterRpcConfig")
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("tcp_keepalive", &self.tcp_keepalive)
            .field("shard_mtls_configured", &self.shard_mtls.is_some())
            .finish()
    }
}

impl Default for RouterRpcConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            tcp_keepalive: Duration::from_secs(30),
            shard_mtls: None,
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
        self.dirty_keys.drain().map(|(k, _)| k).collect()
    }

    fn mark_reconciled(&mut self) {
        self.last_reconcile = Instant::now();
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
    /// Serializes destructive cluster changes against router-mediated mutations.
    mutation_gate: Arc<tokio::sync::RwLock<()>>,
    /// Latches closed if a distributed ontology mutation cannot be rolled back.
    cluster_consistent: Arc<AtomicBool>,
    /// Latches closed while a partially applied reconciliation needs repair.
    reconciliation_consistent: Arc<AtomicBool>,
    /// Shared base checkpoint generation when the complete cluster was restored.
    restore_generation: Option<String>,
    /// Authoritative conflict view rebuilt from a consistent all-shard record scan.
    global_conflict_cache: Arc<parking_lot::RwLock<Option<Vec<proto::ConflictSummary>>>>,
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
        Self::connect_with_version_and_reconciliation(
            shard_addrs,
            ontology_config,
            config_version,
            AdaptiveReconciliationConfig::default(),
        )
        .await
    }

    pub async fn connect_with_version_and_reconciliation(
        shard_addrs: Vec<String>,
        ontology_config: DistributedOntologyConfig,
        config_version: Option<String>,
        reconciliation_config: AdaptiveReconciliationConfig,
    ) -> Result<Arc<Self>, Status> {
        Self::connect_with_runtime_config(
            shard_addrs,
            ontology_config,
            config_version,
            reconciliation_config,
            RouterRpcConfig::default(),
        )
        .await
    }

    pub async fn connect_with_runtime_config(
        shard_addrs: Vec<String>,
        ontology_config: DistributedOntologyConfig,
        config_version: Option<String>,
        reconciliation_config: AdaptiveReconciliationConfig,
        rpc_config: RouterRpcConfig,
    ) -> Result<Arc<Self>, Status> {
        if reconciliation_config.key_count_threshold == 0 {
            return Err(Status::invalid_argument(
                "reconciliation key_count_threshold must be greater than zero",
            ));
        }
        if !reconciliation_config.idle_ingest_rate.is_finite()
            || reconciliation_config.idle_ingest_rate < 0.0
        {
            return Err(Status::invalid_argument(
                "reconciliation idle_ingest_rate must be finite and nonnegative",
            ));
        }
        if rpc_config.connect_timeout.is_zero()
            || rpc_config.request_timeout.is_zero()
            || rpc_config.tcp_keepalive.is_zero()
        {
            return Err(Status::invalid_argument(
                "router shard RPC timeouts and TCP keepalive must be greater than zero",
            ));
        }
        let shard_count = shard_addrs.len();
        if shard_count == 0 {
            return Err(Status::invalid_argument(
                "at least one shard address is required",
            ));
        }
        if shard_count > usize::from(u16::MAX) + 1 {
            return Err(Status::invalid_argument(format!(
                "shard count {shard_count} exceeds the global cluster ID capacity"
            )));
        }
        let mut shard_clients = Vec::with_capacity(shard_count);
        for addr in shard_addrs {
            let uses_https = addr.starts_with("https://");
            let endpoint = tonic::transport::Endpoint::from_shared(addr)
                .map_err(|err| Status::invalid_argument(err.to_string()))?
                .connect_timeout(rpc_config.connect_timeout)
                .timeout(rpc_config.request_timeout)
                .tcp_keepalive(Some(rpc_config.tcp_keepalive));
            let endpoint = match (&rpc_config.shard_mtls, uses_https) {
                (Some(tls), true) => endpoint
                    .tls_config(tls.clone())
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
                (Some(_), false) => {
                    return Err(Status::invalid_argument(
                        "router-to-shard mTLS requires https:// shard addresses",
                    ));
                }
                (None, true) => {
                    return Err(Status::invalid_argument(
                        "https:// shard addresses require explicit router-to-shard mTLS \
                         certificate configuration",
                    ));
                }
                (None, false) => endpoint,
            };
            let channel = endpoint
                .connect()
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
            let client = proto::shard_service_client::ShardServiceClient::new(channel);
            shard_clients.push(client);
        }
        let config_version = config_version.unwrap_or_else(|| "unversioned".to_string());
        let expected_reservation_shard_count = u32::try_from(shard_count)
            .map_err(|_| Status::invalid_argument("shard count exceeds u32"))?;
        let mut source_reservation_backfill_required = Vec::with_capacity(shard_count);
        let mut cluster_restore_state: Option<Option<(String, u32)>> = None;
        for (expected_shard_id, client) in shard_clients.iter().enumerate() {
            let mut client = client.clone();
            let response = client
                .get_config_version(Request::new(proto::ConfigVersionRequest {
                    include_durable_state_digest: false,
                }))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            validate_distributed_protocol(response.protocol_version)?;
            validate_checkpoint_protocol(response.checkpoint_protocol_version)?;
            match proto::ShardRole::try_from(response.shard_role) {
                Ok(proto::ShardRole::Standalone | proto::ShardRole::Primary) => {}
                Ok(proto::ShardRole::Replica) => {
                    return Err(Status::failed_precondition(format!(
                        "shard endpoint at index {expected_shard_id} is a passive replica; \
                         promote it before routing traffic"
                    )));
                }
                _ => {
                    return Err(Status::data_loss(format!(
                        "shard endpoint at index {expected_shard_id} reported an invalid role"
                    )));
                }
            }
            if !response.durable_state_digest.is_empty()
                && response.durable_state_digest.len() != 32
            {
                return Err(Status::data_loss(format!(
                    "shard endpoint at index {expected_shard_id} reported an invalid durable \
                     state digest"
                )));
            }
            let shard_restore_state = match (
                response.restore_generation.is_empty(),
                response.restore_shard_count,
            ) {
                (true, 0) => None,
                (false, count) if count > 0 => {
                    if count != expected_reservation_shard_count {
                        return Err(Status::failed_precondition(format!(
                            "restored checkpoint generation {} was created for {} shards, \
                                 but the router has {}",
                            response.restore_generation, count, expected_reservation_shard_count
                        )));
                    }
                    Some((response.restore_generation.clone(), count))
                }
                _ => {
                    return Err(Status::data_loss(
                        "shard reported incomplete restore checkpoint provenance",
                    ));
                }
            };
            if let Some(expected) = &cluster_restore_state {
                if expected != &shard_restore_state {
                    let message = match (expected, &shard_restore_state) {
                        (Some((expected, _)), Some((actual, _))) => format!(
                            "shards were restored from different checkpoint generations \
                             ({expected} and {actual}); restore the complete cluster from one \
                             coordinated generation"
                        ),
                        _ => "cluster mixes restored and unrestored shard volumes; restore the \
                              complete cluster from one coordinated generation"
                            .to_string(),
                    };
                    return Err(Status::failed_precondition(message));
                }
            } else {
                cluster_restore_state = Some(shard_restore_state);
            }
            if response.version != config_version {
                return Err(Status::failed_precondition(format!(
                    "config version mismatch: router {}, shard {}",
                    config_version, response.version
                )));
            }
            let shard_ontology = response.ontology_config.ok_or_else(|| {
                Status::failed_precondition("shard did not report its ontology configuration")
            })?;
            if map_proto_config(&shard_ontology) != ontology_config {
                return Err(Status::failed_precondition(
                    "ontology mismatch between router and shard; restart the router with the \
                     same ontology used by every shard",
                ));
            }
            if response.source_reservation_backfill_version == DISTRIBUTED_PROTOCOL_VERSION
                && response.source_reservation_shard_count != 0
                && response.source_reservation_shard_count != expected_reservation_shard_count
            {
                return Err(Status::failed_precondition(format!(
                    "shard topology mismatch: shard was initialized for {} shards, router has {}; \
                     online shard-count changes are unsupported without an atomic relocation \
                     protocol",
                    response.source_reservation_shard_count, expected_reservation_shard_count
                )));
            }
            source_reservation_backfill_required.push(
                response.source_reservation_backfill_version != DISTRIBUTED_PROTOCOL_VERSION
                    || response.source_reservation_shard_count != expected_reservation_shard_count,
            );
            let metadata = client
                .get_boundary_metadata(Request::new(proto::GetBoundaryMetadataRequest {
                    since_version: u64::MAX,
                    signatures: Vec::new(),
                }))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner()
                .metadata
                .ok_or_else(|| Status::data_loss("shard returned no boundary metadata"))?;
            let expected_shard_id = u32::try_from(expected_shard_id)
                .map_err(|_| Status::invalid_argument("shard index exceeds u32"))?;
            if metadata.shard_id != expected_shard_id {
                return Err(Status::failed_precondition(format!(
                    "shard endpoint at index {expected_shard_id} reports shard_id {}; shard \
                     addresses must be ordered by contiguous shard ID",
                    metadata.shard_id
                )));
            }
        }
        let restore_generation = cluster_restore_state
            .flatten()
            .map(|(generation, _)| generation);
        let router = Arc::new(Self {
            shard_clients,
            ontology_config: Arc::new(RwLock::new(ontology_config)),
            config_version,
            metrics: Arc::new(PerfMetrics::new()),
            locality_index: Arc::new(StdRwLock::new(ClusterLocalityIndex::new())),
            reconciliation_coordinator: Arc::new(tokio::sync::Mutex::new(
                ReconciliationCoordinator::new(reconciliation_config),
            )),
            mutation_gate: Arc::new(tokio::sync::RwLock::new(())),
            cluster_consistent: Arc::new(AtomicBool::new(true)),
            reconciliation_consistent: Arc::new(AtomicBool::new(true)),
            restore_generation,
            global_conflict_cache: Arc::new(parking_lot::RwLock::new(None)),
        });

        router
            .backfill_source_reservations(source_reservation_backfill_required)
            .await?;
        router.recover_dirty_reconciliation_on_startup().await?;

        // Start adaptive reconciliation without keeping the router alive forever.
        router.clone().start_adaptive_reconciliation();

        Ok(router)
    }

    pub async fn connect_from_file(
        path: impl AsRef<Path>,
        ontology_config: DistributedOntologyConfig,
        config_version: Option<String>,
    ) -> Result<Arc<Self>, Status> {
        Self::connect_from_file_with_reconciliation(
            path,
            ontology_config,
            config_version,
            AdaptiveReconciliationConfig::default(),
        )
        .await
    }

    pub async fn connect_from_file_with_reconciliation(
        path: impl AsRef<Path>,
        ontology_config: DistributedOntologyConfig,
        config_version: Option<String>,
        reconciliation_config: AdaptiveReconciliationConfig,
    ) -> Result<Arc<Self>, Status> {
        Self::connect_from_file_with_runtime_config(
            path,
            ontology_config,
            config_version,
            reconciliation_config,
            RouterRpcConfig::default(),
        )
        .await
    }

    pub async fn connect_from_file_with_runtime_config(
        path: impl AsRef<Path>,
        ontology_config: DistributedOntologyConfig,
        config_version: Option<String>,
        reconciliation_config: AdaptiveReconciliationConfig,
        rpc_config: RouterRpcConfig,
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
        Self::connect_with_runtime_config(
            shard_addrs,
            ontology_config,
            config_version,
            reconciliation_config,
            rpc_config,
        )
        .await
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

    #[allow(clippy::result_large_err)]
    fn ensure_ontology_consistent(&self) -> Result<(), Status> {
        if self.cluster_consistent.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "cluster is blocked after an incomplete ontology change; retry SetOntology with \
                 the intended configuration or recover the shard configurations offline",
            ))
        }
    }

    #[allow(clippy::result_large_err)]
    fn ensure_cluster_consistent(&self) -> Result<(), Status> {
        self.ensure_ontology_consistent()?;
        if self.reconciliation_consistent.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "cluster is blocked after a partially applied cross-shard reconciliation; retry \
                 Reconcile or restart the router to repair retained dirty keys before serving \
                 traffic",
            ))
        }
    }

    fn invalidate_global_conflict_cache(&self) {
        *self.global_conflict_cache.write() = None;
    }

    async fn recover_dirty_reconciliation_on_startup(&self) -> Result<(), Status> {
        let dirty_keys = self.fetch_dirty_boundary_keys(true).await?;
        if dirty_keys.is_empty() {
            return Ok(());
        }

        tracing::warn!(
            dirty_keys = dirty_keys.len(),
            "router startup is repairing retained cross-shard reconciliation work"
        );
        self.reconcile_dirty_keys(&dirty_keys).await?;
        self.clear_dirty_boundary_keys(&dirty_keys).await?;
        Ok(())
    }

    async fn backfill_source_reservations(&self, required: Vec<bool>) -> Result<(), Status> {
        let shard_count = self.shard_clients.len();
        let shard_count_u32 = u32::try_from(shard_count)
            .map_err(|_| Status::invalid_argument("shard count exceeds u32"))?;

        for (target_shard_id, requires_backfill) in required.into_iter().enumerate() {
            if !requires_backfill {
                continue;
            }
            let target_shard_id_u32 = u32::try_from(target_shard_id)
                .map_err(|_| Status::invalid_argument("target shard exceeds u32"))?;
            let mut target_client = self.shard_clients[target_shard_id].clone();
            let mut start_id = 0;
            let mut migrated_records = 0u64;

            loop {
                let response = target_client
                    .export_records(Request::new(proto::ExportRecordsRequest {
                        start_id,
                        end_id: 0,
                        limit: 10_000,
                    }))
                    .await
                    .map_err(|err| Status::unavailable(err.to_string()))?
                    .into_inner();
                let mut owner_batches = vec![Vec::new(); shard_count];
                let mut expected_targets = Vec::with_capacity(response.records.len());

                for (position, snapshot) in response.records.into_iter().enumerate() {
                    let identity = snapshot.identity.ok_or_else(|| {
                        Status::data_loss("exported record is missing its source identity")
                    })?;
                    if identity.entity_type.is_empty()
                        || identity.perspective.is_empty()
                        || identity.uid.is_empty()
                    {
                        return Err(Status::data_loss(
                            "exported record has an empty source identity field",
                        ));
                    }
                    let reservation_index = u32::try_from(position).map_err(|_| {
                        Status::resource_exhausted("reservation migration page exceeds u32 records")
                    })?;
                    let record = proto::RecordInput {
                        index: reservation_index,
                        identity: Some(identity.clone()),
                        descriptors: snapshot.descriptors,
                    };
                    validate_record_inputs(std::slice::from_ref(&record)).map_err(|err| {
                        Status::data_loss(format!(
                            "stored record is invalid during source reservation migration: {}",
                            err.message()
                        ))
                    })?;
                    let owner_shard_id = hash_source_identity_to_shard(&identity, shard_count);
                    owner_batches[owner_shard_id].push(proto::SourceRecordReservation {
                        index: reservation_index,
                        identity: Some(identity),
                        payload_digest: canonical_record_payload_digest(&record)?.to_vec(),
                        target_shard_id: target_shard_id_u32,
                    });
                    expected_targets.push(target_shard_id_u32);
                }

                self.dispatch_source_reservations(owner_batches, &expected_targets)
                    .await?;
                migrated_records = migrated_records.saturating_add(expected_targets.len() as u64);

                if !response.has_more {
                    break;
                }
                if response.next_start_id == 0 || response.next_start_id <= start_id {
                    return Err(Status::data_loss(format!(
                        "shard {target_shard_id} returned a non-progressing reservation migration \
                         cursor"
                    )));
                }
                start_id = response.next_start_id;
            }

            target_client
                .mark_source_reservations_backfilled(Request::new(
                    proto::MarkSourceReservationsBackfilledRequest {
                        protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
                        shard_count: shard_count_u32,
                    },
                ))
                .await?;
            tracing::info!(
                target_shard_id,
                migrated_records,
                "source reservation migration completed"
            );
        }
        Ok(())
    }

    async fn reserve_and_partition_source_records(
        &self,
        config: &DistributedOntologyConfig,
        records: Vec<proto::RecordInput>,
    ) -> Result<Vec<Vec<proto::RecordInput>>, Status> {
        let shard_count = self.shard_clients.len();
        let mut owner_batches = vec![Vec::new(); shard_count];
        let mut expected_targets = Vec::with_capacity(records.len());

        for (position, record) in records.iter().enumerate() {
            let identity = record
                .identity
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("record identity is required"))?;
            let reservation_index = u32::try_from(position)
                .map_err(|_| Status::resource_exhausted("ingest batch exceeds u32 records"))?;
            let target_shard_id = hash_record_to_shard(config, record, shard_count);
            let target_shard_id = u32::try_from(target_shard_id)
                .map_err(|_| Status::resource_exhausted("target shard exceeds u32"))?;
            let owner_shard_id = hash_source_identity_to_shard(identity, shard_count);
            owner_batches[owner_shard_id].push(proto::SourceRecordReservation {
                index: reservation_index,
                identity: Some(identity.clone()),
                payload_digest: canonical_record_payload_digest(record)?.to_vec(),
                target_shard_id,
            });
            expected_targets.push(target_shard_id);
        }

        let confirmed_targets = self
            .dispatch_source_reservations(owner_batches, &expected_targets)
            .await?;

        let mut shard_batches = vec![Vec::new(); shard_count];
        for (record, target_shard_id) in records.into_iter().zip(confirmed_targets) {
            let target_shard_id = target_shard_id as usize;
            shard_batches[target_shard_id].push(record);
        }
        Ok(shard_batches)
    }

    async fn reserve_source_snapshots_for_target(
        &self,
        target_shard_id: u32,
        snapshots: &[proto::RecordSnapshot],
    ) -> Result<(), Status> {
        let shard_count = self.shard_clients.len();
        if target_shard_id as usize >= shard_count {
            return Err(Status::invalid_argument("target shard is out of range"));
        }
        let mut owner_batches = vec![Vec::new(); shard_count];
        let mut expected_targets = Vec::with_capacity(snapshots.len());

        for (position, snapshot) in snapshots.iter().enumerate() {
            let identity = snapshot
                .identity
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("imported record identity is required"))?;
            let reservation_index = u32::try_from(position)
                .map_err(|_| Status::resource_exhausted("import batch exceeds u32 records"))?;
            let record = proto::RecordInput {
                index: reservation_index,
                identity: Some(identity.clone()),
                descriptors: snapshot.descriptors.clone(),
            };
            validate_record_inputs(std::slice::from_ref(&record))?;
            let owner_shard_id = hash_source_identity_to_shard(identity, shard_count);
            owner_batches[owner_shard_id].push(proto::SourceRecordReservation {
                index: reservation_index,
                identity: Some(identity.clone()),
                payload_digest: canonical_record_payload_digest(&record)?.to_vec(),
                target_shard_id,
            });
            expected_targets.push(target_shard_id);
        }
        self.dispatch_source_reservations(owner_batches, &expected_targets)
            .await?;
        Ok(())
    }

    async fn dispatch_source_reservations(
        &self,
        owner_batches: Vec<Vec<proto::SourceRecordReservation>>,
        expected_targets: &[u32],
    ) -> Result<Vec<u32>, Status> {
        let reservation_futures = owner_batches
            .into_iter()
            .enumerate()
            .filter(|(_, reservations)| !reservations.is_empty())
            .map(|(owner_shard_id, reservations)| {
                let mut client = self.shard_clients[owner_shard_id].clone();
                async move {
                    client
                        .reserve_source_records(Request::new(proto::ReserveSourceRecordsRequest {
                            reservations,
                        }))
                        .await
                }
            })
            .collect::<Vec<_>>();
        let responses = futures::future::join_all(reservation_futures).await;
        let mut confirmed_targets = vec![None; expected_targets.len()];
        for response in responses {
            let response = response?.into_inner();
            for reservation in response.reservations {
                let position = reservation.index as usize;
                let expected_target = expected_targets.get(position).ok_or_else(|| {
                    Status::data_loss("source reservation response index is out of range")
                })?;
                if reservation.target_shard_id != *expected_target {
                    return Err(Status::data_loss(format!(
                        "source reservation target mismatch at index {}: expected {}, received {}",
                        reservation.index, expected_target, reservation.target_shard_id
                    )));
                }
                let confirmed = confirmed_targets.get_mut(position).ok_or_else(|| {
                    Status::data_loss("source reservation response index is out of range")
                })?;
                if confirmed.replace(reservation.target_shard_id).is_some() {
                    return Err(Status::data_loss(
                        "source reservation response contains a duplicate index",
                    ));
                }
            }
        }
        if confirmed_targets.iter().any(Option::is_none) {
            return Err(Status::data_loss(
                "source reservation response omitted one or more records",
            ));
        }
        confirmed_targets
            .into_iter()
            .map(|target| {
                target.ok_or_else(|| {
                    Status::data_loss("source reservation response omitted a verified target")
                })
            })
            .collect()
    }

    async fn fetch_all_record_snapshots(&self) -> Result<Vec<proto::RecordSnapshot>, Status> {
        let mut snapshots = Vec::new();
        for (shard_id, client) in self.shard_clients.iter().enumerate() {
            let mut client = client.clone();
            let range = client
                .get_record_id_range(Request::new(proto::RecordIdRangeRequest {}))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            if range.empty {
                continue;
            }
            if range.max_id == u32::MAX {
                return Err(Status::data_loss(format!(
                    "shard {shard_id} contains reserved record ID {}",
                    u32::MAX
                )));
            }

            let mut start_id = 0;
            loop {
                let response = client
                    .export_records(Request::new(proto::ExportRecordsRequest {
                        start_id,
                        end_id: 0,
                        limit: 10_000,
                    }))
                    .await
                    .map_err(|err| Status::unavailable(err.to_string()))?
                    .into_inner();
                snapshots.extend(response.records);
                if !response.has_more {
                    break;
                }
                if response.next_start_id == 0 || response.next_start_id <= start_id {
                    return Err(Status::data_loss(format!(
                        "shard {shard_id} returned a non-progressing record export cursor"
                    )));
                }
                start_id = response.next_start_id;
            }
        }
        Ok(snapshots)
    }

    async fn authoritative_conflict_summaries(
        &self,
    ) -> Result<Vec<proto::ConflictSummary>, Status> {
        if let Some(cached) = self.global_conflict_cache.read().clone() {
            return Ok(cached);
        }

        let snapshots = self.fetch_all_record_snapshots().await?;
        let config = self.ontology_config.read().await.clone();
        let summaries =
            tokio::task::spawn_blocking(move || build_global_conflict_summaries(config, snapshots))
                .await
                .map_err(|err| Status::internal(format!("global conflict scan panicked: {err}")))?
                .map_err(|err| Status::internal(err.to_string()))?;
        *self.global_conflict_cache.write() = Some(summaries.clone());
        Ok(summaries)
    }

    async fn fetch_boundary_metadata(
        &self,
        keys: &[IdentityKeySignature],
    ) -> Result<Vec<proto::BoundaryMetadata>, Status> {
        let signatures = keys
            .iter()
            .map(|signature| proto::IdentityKeySignature {
                signature: signature.to_bytes().to_vec(),
            })
            .collect::<Vec<_>>();
        let requests = self.shard_clients.iter().cloned().enumerate().map(
            |(expected_shard_id, mut client)| {
                let signatures = signatures.clone();
                async move {
                    let response = client
                        .get_boundary_metadata(Request::new(proto::GetBoundaryMetadataRequest {
                            since_version: 0,
                            signatures,
                        }))
                        .await
                        .map_err(|err| Status::unavailable(err.to_string()))?
                        .into_inner();
                    Ok::<_, Status>((expected_shard_id, response))
                }
            },
        );
        let responses = futures::future::join_all(requests).await;
        let mut metadata = Vec::with_capacity(self.shard_clients.len());
        for response in responses {
            let (expected_shard_id, response) = response?;
            let shard_metadata = response
                .metadata
                .ok_or_else(|| Status::data_loss("shard returned no boundary metadata"))?;
            let expected_shard_id = u32::try_from(expected_shard_id)
                .map_err(|_| Status::internal("shard index exceeds u32"))?;
            if shard_metadata.shard_id != expected_shard_id {
                return Err(Status::data_loss(format!(
                    "shard endpoint at index {expected_shard_id} reports shard_id {}",
                    shard_metadata.shard_id
                )));
            }
            metadata.push(shard_metadata);
        }
        Ok(metadata)
    }

    async fn fetch_dirty_boundary_keys(
        &self,
        fetch_all_pages: bool,
    ) -> Result<Vec<IdentityKeySignature>, Status> {
        let requests = self.shard_clients.iter().cloned().enumerate().map(
            |(expected_shard_id, mut client)| async move {
                let mut shard_keys = Vec::new();
                let mut after_signature = Vec::new();
                loop {
                    let response = client
                        .get_dirty_boundary_keys(Request::new(proto::GetDirtyBoundaryKeysRequest {
                            after_signature: after_signature.clone(),
                            limit: DIRTY_KEY_PAGE_LIMIT as u32,
                        }))
                        .await
                        .map_err(|err| Status::unavailable(err.to_string()))?
                        .into_inner();
                    if response.shard_id as usize != expected_shard_id {
                        return Err(Status::data_loss(format!(
                            "dirty-key response from endpoint {expected_shard_id} reports shard {}",
                            response.shard_id
                        )));
                    }
                    for dirty_key in response.dirty_keys {
                        if !dirty_key.entries.is_empty() {
                            return Err(Status::data_loss(
                                "dirty-key response unexpectedly included boundary metadata",
                            ));
                        }
                        let signature = dirty_key.signature.ok_or_else(|| {
                            Status::data_loss("dirty-key response is missing a signature")
                        })?;
                        let signature = <[u8; 32]>::try_from(signature.signature.as_slice())
                            .map_err(|_| {
                                Status::data_loss("dirty-key signature must be 32 bytes")
                            })?;
                        shard_keys.push(IdentityKeySignature::from_bytes(signature));
                    }
                    if !fetch_all_pages || !response.has_more {
                        break;
                    }
                    if response.next_after_signature.len() != 32
                        || response.next_after_signature <= after_signature
                    {
                        return Err(Status::data_loss(
                            "shard returned a non-progressing dirty-key cursor",
                        ));
                    }
                    after_signature = response.next_after_signature;
                }
                Ok::<_, Status>(shard_keys)
            },
        );
        let responses = futures::future::join_all(requests).await;
        let mut dirty_keys = std::collections::HashSet::new();
        for shard_keys in responses {
            dirty_keys.extend(shard_keys?);
        }
        let mut dirty_keys = dirty_keys.into_iter().collect::<Vec<_>>();
        dirty_keys.sort_by_key(|signature| *signature.to_bytes());
        Ok(dirty_keys)
    }

    async fn clear_dirty_boundary_keys(&self, keys: &[IdentityKeySignature]) -> Result<(), Status> {
        for chunk in keys.chunks(RECONCILIATION_KEY_CHUNK) {
            let keys = chunk
                .iter()
                .map(|signature| proto::IdentityKeySignature {
                    signature: signature.to_bytes().to_vec(),
                })
                .collect::<Vec<_>>();
            for (shard_id, client) in self.shard_clients.iter().enumerate() {
                let mut client = client.clone();
                client
                    .clear_dirty_keys(Request::new(proto::ClearDirtyKeysRequest {
                        keys: keys.clone(),
                    }))
                    .await
                    .map_err(|err| {
                        Status::unavailable(format!(
                            "failed to clear reconciled dirty keys on shard {shard_id}: {err}"
                        ))
                    })?;
            }
        }
        Ok(())
    }

    async fn apply_merges_to_shard(
        &self,
        shard_id: u16,
        merges: &[(GlobalClusterId, GlobalClusterId)],
    ) -> Result<u64, Status> {
        let mut client = self.shard_client(u32::from(shard_id))?;
        let response = client
            .apply_merges(Request::new(proto::ApplyMergesRequest {
                merges: merges
                    .iter()
                    .map(|(primary, secondary)| proto::ClusterMerge {
                        primary: Some(global_cluster_id_to_proto(*primary)),
                        secondary: Some(global_cluster_id_to_proto(*secondary)),
                    })
                    .collect(),
            }))
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?
            .into_inner();
        if !response.success {
            return Err(Status::aborted(format!(
                "shard {shard_id} rejected cross-shard merge batch: {}",
                response.error
            )));
        }
        Ok(response.records_updated)
    }

    async fn store_conflicts_on_shard(
        &self,
        shard_id: u16,
        conflicts: Vec<proto::CrossShardConflict>,
    ) -> Result<u32, Status> {
        let mut client = self.shard_client(u32::from(shard_id))?;
        let response = client
            .store_cross_shard_conflicts(Request::new(proto::StoreCrossShardConflictsRequest {
                conflicts,
            }))
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?
            .into_inner();
        Ok(response.stored_count)
    }

    async fn apply_reconciliation_result(
        &self,
        result: &crate::sharding::ReconciliationResult,
    ) -> Result<(), Status> {
        let outcome = self.apply_reconciliation_result_inner(result).await;
        self.reconciliation_consistent
            .store(outcome.is_ok(), Ordering::Release);
        outcome
    }

    async fn apply_reconciliation_result_inner(
        &self,
        result: &crate::sharding::ReconciliationResult,
    ) -> Result<(), Status> {
        for merges in result.merged_clusters.chunks(MERGE_APPLICATION_CHUNK) {
            for shard_id in 0..self.shard_clients.len() {
                let shard_id = u16::try_from(shard_id)
                    .map_err(|_| Status::internal("shard index exceeds u16"))?;
                self.apply_merges_to_shard(shard_id, merges).await?;
            }
        }

        let mut conflicts_by_shard: std::collections::HashMap<u16, Vec<proto::CrossShardConflict>> =
            std::collections::HashMap::new();
        for conflict in &result.detected_conflicts {
            let proto_conflict = cross_shard_conflict_to_proto(conflict);
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
        for (shard_id, conflicts) in conflicts_by_shard {
            for chunk in conflicts.chunks(CONFLICT_APPLICATION_CHUNK) {
                self.store_conflicts_on_shard(shard_id, chunk.to_vec())
                    .await?;
            }
        }
        Ok(())
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
        let router = Arc::downgrade(&self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                let Some(router) = router.upgrade() else {
                    return;
                };
                router.run_adaptive_reconciliation_once().await;
            }
        })
    }

    /// Start periodic coordinated checkpoints. A failed two-phase generation is
    /// retried with the same immutable name until it commits successfully.
    #[allow(clippy::result_large_err)]
    pub fn start_checkpoint_scheduler(
        self: Arc<Self>,
        checkpoint_interval: Duration,
    ) -> Result<tokio::task::JoinHandle<()>, Status> {
        if checkpoint_interval.is_zero() {
            return Err(Status::invalid_argument(
                "checkpoint interval must be greater than zero",
            ));
        }
        let start = tokio::time::Instant::now()
            .checked_add(checkpoint_interval)
            .ok_or_else(|| Status::invalid_argument("checkpoint interval is too large"))?;
        let router = Arc::downgrade(&self);
        Ok(tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(start, checkpoint_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut pending_generation = None;

            loop {
                ticker.tick().await;
                let Some(router) = router.upgrade() else {
                    return;
                };
                let generation = match pending_generation.clone() {
                    Some(generation) => generation,
                    None => {
                        let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
                            Ok(timestamp) => timestamp.as_nanos(),
                            Err(err) => {
                                tracing::error!(
                                    error = %err,
                                    "automatic checkpoint skipped because the system clock is \
                                     before the Unix epoch"
                                );
                                continue;
                            }
                        };
                        let generation = format!("scheduled-{timestamp}");
                        pending_generation = Some(generation.clone());
                        generation
                    }
                };

                let result =
                    <RouterNode as proto::router_service_server::RouterService>::checkpoint(
                        router.as_ref(),
                        Request::new(proto::CheckpointRequest {
                            path: generation.clone(),
                            checkpoint_protocol_version: 0,
                            shard_count: 0,
                            finalize: false,
                        }),
                    )
                    .await;
                match result {
                    Ok(response) => {
                        let response = response.into_inner();
                        tracing::info!(
                            generation = response.generation,
                            shard_paths = response.paths.len(),
                            "automatic cluster checkpoint committed"
                        );
                        pending_generation = None;
                    }
                    Err(err) => {
                        tracing::error!(
                            error = %err,
                            generation,
                            "automatic cluster checkpoint failed; the same generation will retry"
                        );
                    }
                }
            }
        }))
    }

    async fn run_adaptive_reconciliation_once(&self) {
        let dirty_keys = {
            let _mutation_guard = self.mutation_gate.read().await;
            if self.ensure_ontology_consistent().is_err() {
                return;
            }
            match self.fetch_dirty_boundary_keys(false).await {
                Ok(dirty_keys) => dirty_keys,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to poll shard dirty keys");
                    return;
                }
            }
        };
        if !dirty_keys.is_empty() {
            let mut coordinator = self.reconciliation_coordinator.lock().await;
            coordinator.add_dirty_keys_from_shard(0, dirty_keys);
        }

        let should_run = {
            let coordinator = self.reconciliation_coordinator.lock().await;
            let ingest_rate = self.current_ingest_rate();
            !self.reconciliation_consistent.load(Ordering::Acquire)
                || coordinator.should_reconcile(ingest_rate)
        };

        if should_run {
            // Pause router-mediated ingest while refetching metadata, applying merges,
            // and clearing exactly the reconciled dirty-key generation.
            let _mutation_guard = self.mutation_gate.write().await;
            let reconciliation_started = Instant::now();
            if self.ensure_ontology_consistent().is_err() {
                return;
            }
            {
                let mut coordinator = self.reconciliation_coordinator.lock().await;
                coordinator.take_dirty_keys();
            }
            let dirty_keys = match self.fetch_dirty_boundary_keys(true).await {
                Ok(dirty_keys) => dirty_keys,
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        "adaptive reconciliation failed to refetch authoritative dirty keys"
                    );
                    return;
                }
            };
            let result = match self.reconcile_dirty_keys(&dirty_keys).await {
                Ok(result) => result,
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        "adaptive reconciliation failed; shard dirty keys retained"
                    );
                    return;
                }
            };
            if let Err(err) = self.clear_dirty_boundary_keys(&dirty_keys).await {
                tracing::error!(
                    error = %err,
                    "adaptive reconciliation failed to clear dirty keys; retry is safe"
                );
                return;
            }
            tracing::info!(
                dirty_keys = dirty_keys.len(),
                merges = result.merges_performed,
                conflicts_blocked = result.conflicts_blocked,
                elapsed_ms = reconciliation_started.elapsed().as_millis(),
                "adaptive cross-shard reconciliation completed"
            );
            self.reconciliation_coordinator
                .lock()
                .await
                .mark_reconciled();
            if result.conflicts_blocked > 0 {
                self.metrics
                    .cross_shard_conflicts
                    .fetch_add(result.conflicts_blocked as u64, Ordering::Relaxed);
            }
        }
    }

    /// Perform targeted reconciliation for specific dirty keys.
    async fn reconcile_dirty_keys(
        &self,
        keys: &[crate::sharding::IdentityKeySignature],
    ) -> Result<crate::sharding::ReconciliationResult, Status> {
        use crate::sharding::IncrementalReconciler;

        let metadata_started = Instant::now();
        let mut reconciler = IncrementalReconciler::new();
        for chunk in keys.chunks(RECONCILIATION_KEY_CHUNK) {
            for metadata in self.fetch_boundary_metadata(chunk).await? {
                reconciler.add_shard_boundary(boundary_index_from_metadata(&metadata)?);
            }
        }
        let metadata_elapsed = metadata_started.elapsed();
        let key_set = keys.iter().copied().collect();
        let reconcile_started = Instant::now();
        let result = reconciler.reconcile_keys(&key_set);
        let reconcile_elapsed = reconcile_started.elapsed();
        let apply_started = Instant::now();
        self.apply_reconciliation_result(&result).await?;
        tracing::info!(
            dirty_keys = keys.len(),
            merges = result.merges_performed,
            metadata_ms = metadata_elapsed.as_millis(),
            reconcile_ms = reconcile_elapsed.as_millis(),
            apply_ms = apply_started.elapsed().as_millis(),
            "cross-shard reconciliation phase timings"
        );
        Ok(result)
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

        matches.sort_by_key(|query_match| {
            (
                query_match.shard_id,
                query_match.cluster_id,
                query_match.start,
                query_match.end,
            )
        });
        let mut consolidated: Vec<proto::QueryMatch> = Vec::with_capacity(matches.len());
        for mut query_match in matches {
            if let Some(previous) = consolidated.last_mut() {
                if previous.shard_id == query_match.shard_id
                    && previous.cluster_id == query_match.cluster_id
                    && query_match.start <= previous.end
                {
                    previous.start = previous.start.min(query_match.start);
                    previous.end = previous.end.max(query_match.end);
                    for golden in query_match.golden.drain(..) {
                        if !previous.golden.contains(&golden) {
                            previous.golden.push(golden);
                        }
                    }
                    previous.golden.sort_by(|left, right| {
                        (&left.attr, &left.value, left.start, left.end).cmp(&(
                            &right.attr,
                            &right.value,
                            right.start,
                            right.end,
                        ))
                    });
                    if previous.cluster_key.is_empty() {
                        previous.cluster_key = query_match.cluster_key;
                    }
                    if previous.cluster_key_identity.is_empty() {
                        previous.cluster_key_identity = query_match.cluster_key_identity;
                    }
                    continue;
                }
            }
            consolidated.push(query_match);
        }
        let mut matches = consolidated;

        if matches.len() <= 1 {
            return proto::QueryEntitiesResponse {
                outcome: Some(proto::query_entities_response::Outcome::Matches(
                    proto::QueryMatches { matches },
                )),
            };
        }

        matches.sort_by_key(|query_match| query_match.start);

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
        let _mutation_guard = self.mutation_gate.write().await;
        let payload = request.into_inner();
        let config = payload
            .config
            .clone()
            .ok_or_else(|| Status::invalid_argument("ontology config is required"))?;
        let mapped = map_proto_config(&config);
        let previous = self.ontology_config.read().await.clone();
        if previous != mapped {
            for client in &self.shard_clients {
                let mut client = client.clone();
                let stats = client
                    .get_stats(Request::new(proto::StatsRequest {}))
                    .await?
                    .into_inner();
                if stats.record_count != 0 {
                    return Err(Status::failed_precondition(format!(
                        "refusing to replace ontology while the cluster contains records \
                         (shard reports {} records); reset the cluster explicitly first",
                        stats.record_count
                    )));
                }
            }
        }
        self.invalidate_global_conflict_cache();

        for client in &self.shard_clients {
            let mut client = client.clone();
            if let Err(apply_error) = client.set_ontology(Request::new(payload.clone())).await {
                let rollback = proto::ApplyOntologyRequest {
                    config: Some(to_proto_config(&previous)),
                };
                let mut rollback_complete = true;
                for rollback_client in &self.shard_clients {
                    let mut rollback_client = rollback_client.clone();
                    if rollback_client
                        .set_ontology(Request::new(rollback.clone()))
                        .await
                        .is_err()
                    {
                        rollback_complete = false;
                    }
                }
                if !rollback_complete {
                    self.cluster_consistent.store(false, Ordering::Release);
                    return Err(Status::aborted(format!(
                        "ontology update failed and rollback was incomplete; cluster is blocked: \
                         {apply_error}"
                    )));
                }
                return Err(apply_error);
            }
        }
        *self.ontology_config.write().await = mapped;
        self.cluster_consistent.store(true, Ordering::Release);

        Ok(Response::new(proto::ApplyOntologyResponse {}))
    }

    async fn ingest_records(
        &self,
        request: Request<proto::IngestRecordsRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_cluster_consistent()?;
        let start = Instant::now();
        let batch = request.into_inner();
        let record_count = batch.records.len();
        let shard_count = self.shard_clients.len();
        validate_record_inputs(&batch.records)?;
        self.invalidate_global_conflict_cache();

        // ULTRA-FAST single-shard path: skip all hashing and sorting
        if shard_count == 1 {
            let mut client = self.shard_clients[0].clone();
            let response = client
                .ingest_records(Request::new(proto::IngestRecordsRequest {
                    records: batch.records,
                    internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
                }))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;

            self.metrics
                .record_ingest(record_count, start.elapsed().as_micros() as u64);
            return Ok(response);
        }

        // Multi-shard path: durably bind each immutable source identity to its
        // canonical payload and routing destination before entity resolution.
        let config = self.ontology_config.read().await.clone();
        let shard_batches = self
            .reserve_and_partition_source_records(&config, batch.records)
            .await?;

        // Parallel shard ingest - spawn all shard requests concurrently
        let shard_futures: Vec<_> = shard_batches
            .into_iter()
            .enumerate()
            .filter(|(_, records)| !records.is_empty())
            .map(|(idx, records)| {
                let mut client = self.shard_clients[idx].clone();
                async move {
                    client
                        .ingest_records(Request::new(proto::IngestRecordsRequest {
                            records,
                            internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
                        }))
                        .await
                }
            })
            .collect();

        let shard_responses = futures::future::join_all(shard_futures).await;
        let mut results: Vec<proto::IngestAssignment> = Vec::new();
        for response in shard_responses {
            match response {
                Ok(resp) => results.extend(resp.into_inner().assignments),
                Err(err) => return Err(err),
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
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_cluster_consistent()?;
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
        let _mutation_guard = self.mutation_gate.read().await;
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
            totals.cross_shard_merges = totals.cross_shard_merges.max(response.cross_shard_merges);
            totals.cross_shard_conflicts += response.cross_shard_conflicts;
            totals.boundary_keys_tracked += response.boundary_keys_tracked;
        }

        // Each cross-shard conflict is durably stored on both participating shards.
        totals.cross_shard_conflicts = totals.cross_shard_conflicts.div_ceil(2);

        Ok(Response::new(totals))
    }

    async fn health_check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_cluster_consistent()?;
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
        let ontology_config = self.ontology_config.read().await;
        Ok(Response::new(proto::ConfigVersionResponse {
            version: self.config_version.clone(),
            ontology_config: Some(to_proto_config(&ontology_config)),
            protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
            source_reservation_backfill_version: DISTRIBUTED_PROTOCOL_VERSION,
            source_reservation_shard_count: self.shard_clients.len() as u32,
            checkpoint_protocol_version: CHECKPOINT_PROTOCOL_VERSION,
            restore_generation: self.restore_generation.clone().unwrap_or_default(),
            restore_shard_count: self
                .restore_generation
                .as_ref()
                .map(|_| self.shard_clients.len() as u32)
                .unwrap_or_default(),
            shard_role: proto::ShardRole::Unspecified as i32,
            durable_state_digest: Vec::new(),
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
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_cluster_consistent()?;
        let request = request.into_inner();
        self.invalidate_global_conflict_cache();
        self.reserve_source_snapshots_for_target(request.shard_id, &request.records)
            .await?;
        let mut client = self.shard_client(request.shard_id)?;
        let response = client
            .import_records(Request::new(proto::ImportRecordsRequest {
                records: request.records,
                internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
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
        let _mutation_guard = self.mutation_gate.read().await;
        self.ensure_cluster_consistent()?;
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let Some(first) = first else {
            return Ok(Response::new(proto::ImportRecordsResponse { imported: 0 }));
        };

        self.invalidate_global_conflict_cache();
        let shard_id = first.shard_id;
        self.reserve_source_snapshots_for_target(shard_id, &first.records)
            .await?;
        let mut client = self.shard_client(shard_id)?;
        let (tx, rx) = mpsc::channel(4);
        let (err_tx, err_rx) = oneshot::channel::<Result<(), Status>>();
        let router = self.clone();

        tokio::spawn(async move {
            if tx
                .send(proto::ImportRecordsChunk {
                    records: first.records,
                    internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
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
                        if let Err(err) = router
                            .reserve_source_snapshots_for_target(shard_id, &chunk.records)
                            .await
                        {
                            let _ = err_tx.send(Err(err));
                            return;
                        }
                        if tx
                            .send(proto::ImportRecordsChunk {
                                records: chunk.records,
                                internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
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
        let _mutation_guard = self.mutation_gate.write().await;
        let mut payload = request.into_inner();
        if payload.path.is_empty() {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|err| Status::internal(err.to_string()))?
                .as_nanos();
            payload.path = format!("cluster-{timestamp}");
        }
        let shard_count = u32::try_from(self.shard_clients.len())
            .map_err(|_| Status::resource_exhausted("shard count exceeds u32"))?;
        payload.checkpoint_protocol_version = CHECKPOINT_PROTOCOL_VERSION;
        payload.shard_count = shard_count;
        payload.finalize = false;

        let mut paths = Vec::new();
        for (shard_id, client) in self.shard_clients.iter().enumerate() {
            let mut client = client.clone();
            let response = client
                .checkpoint(Request::new(payload.clone()))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            if response.committed
                || response.generation != payload.path
                || response.paths.len() != 1
            {
                return Err(Status::data_loss(format!(
                    "shard {shard_id} returned an invalid checkpoint prepare response"
                )));
            }
            paths.extend(response.paths);
        }

        payload.finalize = true;
        for (shard_id, client) in self.shard_clients.iter().enumerate() {
            let mut client = client.clone();
            let response = client
                .checkpoint(Request::new(payload.clone()))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            if !response.committed
                || response.generation != payload.path
                || response.paths.len() != 1
            {
                return Err(Status::data_loss(format!(
                    "shard {shard_id} returned an invalid checkpoint commit response"
                )));
            }
        }

        Ok(Response::new(proto::CheckpointResponse {
            paths,
            generation: payload.path,
            committed: true,
        }))
    }

    async fn list_conflicts(
        &self,
        request: Request<proto::ListConflictsRequest>,
    ) -> Result<Response<proto::ListConflictsResponse>, Status> {
        let _mutation_guard = self.mutation_gate.write().await;
        self.ensure_cluster_consistent()?;
        let payload = request.into_inner();
        let mut summaries = self.authoritative_conflict_summaries().await?;
        if !payload.attribute.is_empty() {
            summaries.retain(|summary| summary.attribute == payload.attribute);
        }
        if payload.end > payload.start {
            let filter = Interval::new(payload.start, payload.end)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
            summaries.retain(|summary| {
                Interval::new(summary.start, summary.end)
                    .is_ok_and(|interval| crate::temporal::is_overlapping(&interval, &filter))
            });
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
        summaries.dedup();

        Ok(Response::new(proto::ListConflictsResponse {
            conflicts: summaries,
        }))
    }

    async fn reset(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        let _mutation_guard = self.mutation_gate.write().await;
        self.invalidate_global_conflict_cache();
        for client in &self.shard_clients {
            let mut client = client.clone();
            client.reset(Request::new(proto::Empty {})).await?;
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn reconcile(
        &self,
        request: Request<proto::ReconcileRequest>,
    ) -> Result<Response<proto::ReconcileResponse>, Status> {
        let _mutation_guard = self.mutation_gate.write().await;
        self.ensure_ontology_consistent()?;
        let req = request.into_inner();
        if !req.shard_metadata.is_empty() {
            return Err(Status::invalid_argument(
                "caller-supplied shard metadata is not accepted; the router fetches authoritative \
                 metadata from its configured shards",
            ));
        }
        let dirty_keys = self.fetch_dirty_boundary_keys(true).await?;
        let result = self.reconcile_dirty_keys(&dirty_keys).await?;
        self.clear_dirty_boundary_keys(&dirty_keys).await?;

        if result.conflicts_blocked > 0 {
            self.metrics.cross_shard_conflicts.fetch_add(
                result.conflicts_blocked as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        let merges = result
            .merged_clusters
            .iter()
            .map(|(primary, secondary)| proto::ClusterMerge {
                primary: Some(global_cluster_id_to_proto(*primary)),
                secondary: Some(global_cluster_id_to_proto(*secondary)),
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

#[cfg(test)]
mod wal_tests {
    use super::*;

    fn sample_wal_record() -> WalRecordInput {
        WalRecordInput {
            index: 7,
            identity: WalRecordIdentity {
                entity_type: "person".to_string(),
                perspective: "crm".to_string(),
                uid: "wal-checksum".to_string(),
            },
            descriptors: vec![WalRecordDescriptor {
                attr: "email".to_string(),
                value: "checksum@example.com".to_string(),
                start: 0,
                end: 10,
            }],
        }
    }

    #[test]
    fn framed_wal_round_trips_and_detects_corruption() {
        let records = vec![sample_wal_record()];
        let mut encoded = encode_wal_batch(&records).expect("encode WAL");
        let decoded = decode_wal_batch(&encoded).expect("decode WAL");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].identity.uid, "wal-checksum");

        let last = encoded.last_mut().expect("WAL payload byte");
        *last ^= 0x01;
        assert!(decode_wal_batch(&encoded)
            .expect_err("checksum mutation must fail")
            .contains("checksum mismatch"));
    }

    #[test]
    fn legacy_unframed_wal_remains_replayable() {
        let encoded = bincode::serialize(&vec![sample_wal_record()]).expect("encode legacy WAL");
        let decoded = decode_wal_batch(&encoded).expect("decode legacy WAL");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].index, 7);
    }

    #[test]
    fn source_routing_hash_is_stable_and_payload_independent() {
        let config = DistributedOntologyConfig::empty();
        let original = proto_from_wal_input(sample_wal_record());
        assert_eq!(
            hash_record_to_u64(&config, &original),
            15_034_823_070_914_786_251
        );

        let mut changed_payload = original.clone();
        changed_payload.descriptors.reverse();
        changed_payload.descriptors[0].value = "changed@example.com".to_string();
        assert_eq!(
            hash_record_to_u64(&config, &original),
            hash_record_to_u64(&config, &changed_payload),
            "an immutable source identity must always route to the same shard"
        );

        changed_payload.identity.as_mut().expect("identity").uid = "different-source".to_string();
        assert_ne!(
            hash_record_to_u64(&config, &original),
            hash_record_to_u64(&config, &changed_payload)
        );
    }

    #[test]
    fn mixed_distributed_protocol_versions_fail_closed() {
        validate_distributed_protocol(DISTRIBUTED_PROTOCOL_VERSION)
            .expect("matching protocol must be accepted");
        let error = validate_distributed_protocol(DISTRIBUTED_PROTOCOL_VERSION - 1)
            .expect_err("mixed router and shard protocols must be rejected");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("coordinated Unirust version"));
    }

    #[test]
    fn pending_wal_cannot_be_overwritten() {
        let temp_dir = tempfile::tempdir().expect("temporary WAL directory");
        let wal = IngestWal::new(temp_dir.path());
        let first = proto_from_wal_input(sample_wal_record());
        wal.write_batch(std::slice::from_ref(&first))
            .expect("write first WAL");

        let mut second = first;
        second.index = 99;
        second.identity.as_mut().expect("identity").uid = "replacement".to_string();
        let error = wal
            .write_batch(&[second])
            .expect_err("pending WAL must block replacement");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);

        let recovered = wal
            .load_batch()
            .expect("load pending WAL")
            .expect("pending batch");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].index, 7);
        assert_eq!(
            recovered[0].identity.as_ref().expect("identity").uid,
            "wal-checksum"
        );
    }

    #[tokio::test]
    async fn pending_wal_fails_health_and_reads_closed() {
        use proto::shard_service_server::ShardService;

        let temp_dir = tempfile::tempdir().expect("temporary shard directory");
        let shard = ShardNode::new_with_data_dir(
            0,
            DistributedOntologyConfig::empty(),
            StreamingTuning::balanced(),
            Some(temp_dir.path().to_path_buf()),
            false,
            None,
        )
        .expect("persistent shard");
        let wal = shard.ingest_wal.as_ref().expect("ingest WAL");
        wal.write_batch(&[proto_from_wal_input(sample_wal_record())])
            .expect("write pending WAL");

        let health_error =
            ShardService::health_check(&shard, Request::new(proto::HealthCheckRequest {}))
                .await
                .expect_err("pending recovery must fail health");
        assert_eq!(health_error.code(), tonic::Code::FailedPrecondition);

        let stats_error = ShardService::get_stats(&shard, Request::new(proto::StatsRequest {}))
            .await
            .expect_err("pending recovery must block state reads");
        assert_eq!(stats_error.code(), tonic::Code::FailedPrecondition);

        wal.clear().expect("clear test WAL");
        ShardService::health_check(&shard, Request::new(proto::HealthCheckRequest {}))
            .await
            .expect("health recovers after WAL clears");
    }

    #[tokio::test]
    async fn conflicting_source_identity_is_rejected_before_wal_creation() {
        use proto::shard_service_server::ShardService;

        let temp_dir = tempfile::tempdir().expect("temporary shard directory");
        let shard = ShardNode::new_with_data_dir(
            0,
            DistributedOntologyConfig::empty(),
            StreamingTuning::balanced(),
            Some(temp_dir.path().to_path_buf()),
            false,
            None,
        )
        .expect("persistent shard");
        let original = proto_from_wal_input(sample_wal_record());
        ShardService::ingest_records(
            &shard,
            Request::new(proto::IngestRecordsRequest {
                internal_protocol_version: 5,
                records: vec![original.clone()],
            }),
        )
        .await
        .expect("initial ingest");

        let mut conflicting = original;
        conflicting.descriptors[0].value = "different@example.com".to_string();
        let error = ShardService::ingest_records(
            &shard,
            Request::new(proto::IngestRecordsRequest {
                internal_protocol_version: 5,
                records: vec![conflicting],
            }),
        )
        .await
        .expect_err("changed payload must be rejected");
        assert_eq!(error.code(), tonic::Code::AlreadyExists);
        assert!(
            !shard.ingest_wal.as_ref().expect("ingest WAL").has_pending(),
            "a deterministic validation error must not leave a recovery WAL"
        );

        let stats = ShardService::get_stats(&shard, Request::new(proto::StatsRequest {}))
            .await
            .expect("stats")
            .into_inner();
        assert_eq!(stats.record_count, 1);
        shard.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn invalid_persistent_import_does_not_latch_shard_closed() {
        use proto::shard_service_server::ShardService;

        let temp_dir = tempfile::tempdir().expect("temporary shard directory");
        let shard = ShardNode::new_with_data_dir(
            0,
            DistributedOntologyConfig::empty(),
            StreamingTuning::balanced(),
            Some(temp_dir.path().to_path_buf()),
            false,
            None,
        )
        .expect("persistent shard");
        let error = ShardService::import_records(
            &shard,
            Request::new(proto::ImportRecordsRequest {
                records: vec![proto::RecordSnapshot {
                    record_id: 7,
                    identity: Some(proto::RecordIdentity {
                        entity_type: "person".to_string(),
                        perspective: "crm".to_string(),
                        uid: "invalid-import".to_string(),
                    }),
                    descriptors: vec![proto::RecordDescriptor {
                        attr: "email".to_string(),
                        value: "invalid@example.com".to_string(),
                        start: 10,
                        end: 10,
                    }],
                }],
                internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
            }),
        )
        .await
        .expect_err("invalid import must fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        ShardService::health_check(&shard, Request::new(proto::HealthCheckRequest {}))
            .await
            .expect("fully validated client errors must not poison readiness");
        shard.shutdown().await.expect("shutdown");
    }

    #[test]
    fn incomplete_durable_mutation_latches_consistency() {
        let consistency = Arc::new(AtomicBool::new(true));
        drop(DurableMutationAttempt::new(consistency.clone(), true));
        assert!(!consistency.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn shard_rejects_ingest_from_an_older_router_protocol() {
        use proto::shard_service_server::ShardService;

        let shard = ShardNode::new(
            0,
            DistributedOntologyConfig::empty(),
            StreamingTuning::balanced(),
        )
        .expect("shard");
        let error = ShardService::ingest_records(
            &shard,
            Request::new(proto::IngestRecordsRequest {
                records: vec![proto_from_wal_input(sample_wal_record())],
                internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION - 1,
            }),
        )
        .await
        .expect_err("a new shard must reject ingest from an old router");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(shard.unirust.read().record_count(), 0);
    }
}
