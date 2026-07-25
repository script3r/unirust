use crate::model::{GlobalClusterId, Record, RecordId, RecordIdentity, StringInterner};
use crate::store::{
    records_have_same_payload, RecordStore, SourceRecordReservation, SourceReservationError, Store,
    StoreMetrics,
};
use anyhow::{anyhow, Result};
use lru::LruCache;
use rocksdb::{
    checkpoint::Checkpoint, BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType,
    Direction, IteratorMode, Options, SliceTransform, WriteBatch, WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub const CHECKPOINT_PROTOCOL_VERSION: u32 = 1;
const CHECKPOINT_MANIFEST_FORMAT_VERSION: u32 = 1;
const CHECKPOINT_MANIFEST_FILE: &str = "UNIRUST_CHECKPOINT_MANIFEST";
const CHECKPOINT_COMMITTED_FILE: &str = "UNIRUST_CHECKPOINT_COMMITTED";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterCheckpointManifest {
    format_version: u32,
    checkpoint_protocol_version: u32,
    generation: String,
    shard_id: u32,
    shard_count: u32,
}

impl ClusterCheckpointManifest {
    pub fn generation(&self) -> &str {
        &self.generation
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    fn new(generation: &str, shard_id: u32, shard_count: u32) -> Result<Self> {
        if generation.is_empty() {
            anyhow::bail!("checkpoint generation must not be empty");
        }
        if shard_count == 0 || shard_id >= shard_count {
            anyhow::bail!(
                "checkpoint shard {} is outside cluster shard count {}",
                shard_id,
                shard_count
            );
        }
        Ok(Self {
            format_version: CHECKPOINT_MANIFEST_FORMAT_VERSION,
            checkpoint_protocol_version: CHECKPOINT_PROTOCOL_VERSION,
            generation: generation.to_string(),
            shard_id,
            shard_count,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.format_version != CHECKPOINT_MANIFEST_FORMAT_VERSION {
            anyhow::bail!(
                "unsupported checkpoint manifest format version {}",
                self.format_version
            );
        }
        if self.checkpoint_protocol_version != CHECKPOINT_PROTOCOL_VERSION {
            anyhow::bail!(
                "unsupported checkpoint protocol version {}",
                self.checkpoint_protocol_version
            );
        }
        if self.generation.is_empty() {
            anyhow::bail!("checkpoint manifest generation is empty");
        }
        if self.shard_count == 0 || self.shard_id >= self.shard_count {
            anyhow::bail!(
                "checkpoint manifest shard {} is outside cluster shard count {}",
                self.shard_id,
                self.shard_count
            );
        }
        Ok(())
    }
}

const CF_RECORDS: &str = "records";
const CF_METADATA: &str = "metadata";
const CF_INTERNER: &str = "interner";
const CF_INDEX_ATTR_VALUE: &str = "index_attr_value";
const CF_INDEX_ENTITY_TYPE: &str = "index_entity_type";
const CF_INDEX_PERSPECTIVE: &str = "index_perspective";
const CF_INDEX_TEMPORAL_BUCKET: &str = "index_temporal_bucket";
const CF_INDEX_IDENTITY: &str = "index_identity";
const CF_CONFLICT_SUMMARIES: &str = "conflict_summaries";
const CF_CLUSTER_ASSIGNMENTS: &str = "cluster_assignments";
const CF_SOURCE_RESERVATIONS: &str = "source_reservations";

thread_local! {
    static RECORD_SER_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn with_record_bytes<R, F>(record: &Record, f: F) -> Result<R>
where
    F: FnOnce(&[u8]) -> Result<R>,
{
    RECORD_SER_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        record.encode_into(&mut buf).map_err(|err| anyhow!(err))?;
        f(&buf)
    })
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn copy_checkpoint_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_checkpoint_tree(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), &target)?;
            fs::File::open(&target)?.sync_all()?;
        } else {
            anyhow::bail!(
                "checkpoint contains unsupported non-file entry {}",
                entry.path().display()
            );
        }
    }
    sync_directory(destination)?;
    Ok(())
}

fn validate_rocksdb_checkpoint(path: &Path) -> Result<()> {
    let mut options = Options::default();
    options.set_paranoid_checks(true);
    let mut column_families = DB::list_cf(&options, path)?;
    column_families.retain(|name| name != "default");
    let db = DB::open_cf_for_read_only(&options, path, column_families, false)?;
    drop(db);
    Ok(())
}

fn read_manifest_file(path: &Path) -> Result<(ClusterCheckpointManifest, Vec<u8>)> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        anyhow::bail!("checkpoint marker {} must be a real file", path.display());
    }
    let bytes = fs::read(path)?;
    let manifest: ClusterCheckpointManifest = bincode::deserialize(&bytes)?;
    manifest.validate()?;
    Ok((manifest, bytes))
}

fn write_marker_once(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.exists() {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            anyhow::bail!("checkpoint marker {} must be a real file", path.display());
        }
        if fs::read(path)? == bytes {
            return Ok(());
        }
        anyhow::bail!("checkpoint marker {} does not match", path.display());
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("checkpoint marker name must be UTF-8"))?;
    let staging = path.with_file_name(format!(".{file_name}.tmp"));
    if staging.exists() {
        fs::remove_file(&staging)?;
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&staging, path)?;
    sync_directory(
        path.parent()
            .ok_or_else(|| anyhow!("checkpoint marker has no parent directory"))?,
    )?;
    Ok(())
}

pub(crate) fn prepare_cluster_checkpoint(
    checkpoint: &Path,
    generation: &str,
    shard_id: u32,
    shard_count: u32,
) -> Result<()> {
    if !checkpoint.is_dir() || !checkpoint.join("CURRENT").is_file() {
        anyhow::bail!(
            "prepared checkpoint {} is not a RocksDB checkpoint",
            checkpoint.display()
        );
    }
    let expected = ClusterCheckpointManifest::new(generation, shard_id, shard_count)?;
    let bytes = bincode::serialize(&expected)?;
    write_marker_once(&checkpoint.join(CHECKPOINT_MANIFEST_FILE), &bytes)
}

pub(crate) fn validate_prepared_cluster_checkpoint(
    checkpoint: &Path,
    generation: &str,
    shard_id: u32,
    shard_count: u32,
) -> Result<()> {
    if !checkpoint.is_dir() || !checkpoint.join("CURRENT").is_file() {
        anyhow::bail!(
            "prepared checkpoint {} is not a RocksDB checkpoint",
            checkpoint.display()
        );
    }
    let expected = ClusterCheckpointManifest::new(generation, shard_id, shard_count)?;
    let (prepared, _) = read_manifest_file(&checkpoint.join(CHECKPOINT_MANIFEST_FILE))?;
    if prepared != expected {
        anyhow::bail!(
            "prepared checkpoint {} does not match generation {} shard {}/{}",
            checkpoint.display(),
            generation,
            shard_id,
            shard_count
        );
    }
    Ok(())
}

pub(crate) fn commit_cluster_checkpoint(
    checkpoint: &Path,
    generation: &str,
    shard_id: u32,
    shard_count: u32,
) -> Result<()> {
    let expected = ClusterCheckpointManifest::new(generation, shard_id, shard_count)?;
    let (prepared, bytes) = read_manifest_file(&checkpoint.join(CHECKPOINT_MANIFEST_FILE))?;
    if prepared != expected {
        anyhow::bail!(
            "prepared checkpoint {} does not match generation {} shard {}/{}",
            checkpoint.display(),
            generation,
            shard_id,
            shard_count
        );
    }
    write_marker_once(&checkpoint.join(CHECKPOINT_COMMITTED_FILE), &bytes)
}

/// Read and validate a committed cluster checkpoint manifest.
pub fn read_cluster_checkpoint_manifest(checkpoint: &Path) -> Result<ClusterCheckpointManifest> {
    let metadata = fs::symlink_metadata(checkpoint)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!("checkpoint source must be a real directory");
    }
    let checkpoint = checkpoint.canonicalize()?;
    if !checkpoint.join("CURRENT").is_file() {
        anyhow::bail!(
            "checkpoint source {} is not a RocksDB checkpoint",
            checkpoint.display()
        );
    }
    let (manifest, prepared_bytes) =
        read_manifest_file(&checkpoint.join(CHECKPOINT_MANIFEST_FILE))?;
    let (_committed, committed_bytes) =
        read_manifest_file(&checkpoint.join(CHECKPOINT_COMMITTED_FILE))?;
    if committed_bytes != prepared_bytes {
        anyhow::bail!(
            "checkpoint {} has mismatched prepare and commit markers",
            checkpoint.display()
        );
    }
    Ok(manifest)
}

/// Validate a committed cluster checkpoint and open its RocksDB contents
/// read-only with paranoid checks.
pub fn verify_cluster_checkpoint(checkpoint: &Path) -> Result<ClusterCheckpointManifest> {
    let manifest = read_cluster_checkpoint_manifest(checkpoint)?;
    let checkpoint = checkpoint.canonicalize()?;
    validate_rocksdb_checkpoint(&checkpoint)?;
    Ok(manifest)
}

/// Return the committed checkpoint provenance copied into a restored RocksDB
/// data directory. A lone or corrupt marker is a recovery integrity failure,
/// not an unrestored directory.
pub(crate) fn read_restored_checkpoint_manifest(
    data_dir: &Path,
) -> Result<Option<ClusterCheckpointManifest>> {
    let prepared = data_dir.join(CHECKPOINT_MANIFEST_FILE);
    let committed = data_dir.join(CHECKPOINT_COMMITTED_FILE);
    let marker_exists = |path: &Path| match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    };
    match (marker_exists(&prepared)?, marker_exists(&committed)?) {
        (false, false) => Ok(None),
        (true, true) => read_cluster_checkpoint_manifest(data_dir).map(Some),
        _ => anyhow::bail!(
            "restored data directory {} contains incomplete checkpoint provenance",
            data_dir.display()
        ),
    }
}

/// Restore a RocksDB checkpoint into an empty replacement data directory.
///
/// Copying occurs in a sibling staging directory and is made visible with one
/// rename only after every file and directory has been synchronized. Only
/// checkpoints committed by the router's cluster-wide two-phase protocol are
/// accepted.
pub fn restore_checkpoint(source: &Path, destination: &Path) -> Result<()> {
    restore_checkpoint_for_shard(source, destination, None)
}

/// Restore a committed cluster checkpoint and verify its shard identity.
pub fn restore_checkpoint_for_shard(
    source: &Path,
    destination: &Path,
    expected_shard_id: Option<u32>,
) -> Result<()> {
    let manifest = verify_cluster_checkpoint(source)?;
    if let Some(expected_shard_id) =
        expected_shard_id.filter(|expected| *expected != manifest.shard_id)
    {
        anyhow::bail!(
            "checkpoint belongs to shard {}, not requested shard {}",
            manifest.shard_id,
            expected_shard_id
        );
    }

    let source = source.canonicalize()?;

    if destination.exists() {
        let metadata = fs::symlink_metadata(destination)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            anyhow::bail!("restore destination must be an empty directory");
        }
        if fs::read_dir(destination)?.next().transpose()?.is_some() {
            anyhow::bail!(
                "refusing to restore over nonempty destination {}",
                destination.display()
            );
        }
    }

    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("restore destination must have a UTF-8 directory name"))?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let parent = parent.canonicalize()?;
    if parent.starts_with(&source) {
        anyhow::bail!("restore destination must not be inside the checkpoint");
    }
    let destination = parent.join(name);
    let staging = parent.join(format!(".{name}.restore.tmp"));
    if staging.exists() {
        anyhow::bail!(
            "restore staging directory {} already exists; inspect and remove it before retrying",
            staging.display()
        );
    }

    copy_checkpoint_tree(&source, &staging)?;
    validate_rocksdb_checkpoint(&staging)?;
    if destination.exists() {
        fs::remove_dir(&destination)?;
    }
    fs::rename(&staging, &destination)?;
    sync_directory(&parent)?;
    Ok(())
}

// DSU persistence column families
const CF_DSU_PARENT: &str = "dsu_parent";
const CF_DSU_RANK: &str = "dsu_rank";
const CF_DSU_GUARDS: &str = "dsu_guards";
const CF_DSU_METADATA: &str = "dsu_metadata";

// Tiered index column families
const CF_INDEX_IDENTITY_KEYS: &str = "index_identity_keys"; // Cold tier storage
const CF_INDEX_KEY_STATS: &str = "index_key_stats"; // Access statistics

// Linker state column families (for restart recovery)
const CF_LINKER_CLUSTER_IDS: &str = "linker_cluster_ids";
const CF_LINKER_GLOBAL_IDS: &str = "linker_global_ids";
const CF_LINKER_METADATA: &str = "linker_metadata";

const DURABLE_STATE_COLUMN_FAMILIES: &[&str] = &[
    CF_RECORDS,
    CF_METADATA,
    CF_INTERNER,
    CF_INDEX_ATTR_VALUE,
    CF_INDEX_ENTITY_TYPE,
    CF_INDEX_PERSPECTIVE,
    CF_INDEX_TEMPORAL_BUCKET,
    CF_INDEX_IDENTITY,
    CF_CONFLICT_SUMMARIES,
    CF_CLUSTER_ASSIGNMENTS,
    CF_SOURCE_RESERVATIONS,
    CF_DSU_PARENT,
    CF_DSU_RANK,
    CF_DSU_GUARDS,
    CF_DSU_METADATA,
    CF_INDEX_IDENTITY_KEYS,
    CF_INDEX_KEY_STATS,
    CF_LINKER_CLUSTER_IDS,
    CF_LINKER_GLOBAL_IDS,
    CF_LINKER_METADATA,
];

const RESET_DATA_CFS: &[&str] = &[
    CF_RECORDS,
    CF_INTERNER,
    CF_INDEX_ATTR_VALUE,
    CF_INDEX_ENTITY_TYPE,
    CF_INDEX_PERSPECTIVE,
    CF_INDEX_TEMPORAL_BUCKET,
    CF_INDEX_IDENTITY,
    CF_CONFLICT_SUMMARIES,
    CF_CLUSTER_ASSIGNMENTS,
    CF_SOURCE_RESERVATIONS,
    CF_DSU_PARENT,
    CF_DSU_RANK,
    CF_DSU_GUARDS,
    CF_DSU_METADATA,
    CF_INDEX_IDENTITY_KEYS,
    CF_INDEX_KEY_STATS,
    CF_LINKER_CLUSTER_IDS,
    CF_LINKER_GLOBAL_IDS,
    CF_LINKER_METADATA,
];

const KEY_NEXT_RECORD_ID: &[u8] = b"next_record_id";
const KEY_INTERNER: &[u8] = b"interner";
const KEY_ONTOLOGY_CONFIG: &[u8] = b"ontology_config";
const KEY_MANIFEST: &[u8] = b"manifest";
const KEY_INDEX_VERSION: &[u8] = b"index_version";
const KEY_NEXT_ATTR_ID: &[u8] = b"next_attr_id";
const KEY_NEXT_VALUE_ID: &[u8] = b"next_value_id";
const KEY_RECORD_COUNT: &[u8] = b"record_count";
const KEY_CLUSTER_COUNT: &[u8] = b"cluster_count";
const KEY_CONFLICT_SUMMARY_COUNT: &[u8] = b"conflict_summary_count";
const KEY_CROSS_SHARD_CONFLICTS: &[u8] = b"cross_shard_conflicts";
const KEY_SOURCE_RESERVATION_BACKFILL: &[u8] = b"source_reservation_backfill";

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct SourceReservationValue {
    payload_digest: [u8; 32],
    target_shard_id: u32,
}

// DSU metadata keys
const KEY_DSU_NEXT_CLUSTER_ID: &[u8] = b"dsu_next_cluster_id";
const KEY_DSU_CLUSTER_COUNT: &[u8] = b"dsu_cluster_count";

const STORAGE_FORMAT_VERSION: u32 = 1;
const INDEX_FORMAT_VERSION: u32 = 2;
const TEMPORAL_BUCKET_SECONDS: i64 = 86400;
const DEFAULT_CACHE_CAPACITY: usize = 100_000;
const DEFAULT_BLOCK_CACHE_MB: u64 = 512;
const DEFAULT_WRITE_BUFFER_MB: u64 = 128;
const DEFAULT_MAX_WRITE_BUFFERS: i32 = 4;
const DEFAULT_TARGET_FILE_MB: u64 = 128;
const DEFAULT_LEVEL_BASE_MB: u64 = 512;
const DEFAULT_BLOOM_BITS_PER_KEY: f64 = 10.0;
const DEFAULT_MEMTABLE_PREFIX_BLOOM_RATIO: f64 = 0.1;
// Aggressive compaction deferral: favor ingest throughput over compaction
// Default 20 MB/s rate limit ensures compaction doesn't starve ingest
const DEFAULT_RATE_LIMIT_MBPS: u64 = 20;
// Reduce compaction threads to minimize CPU contention with ingest
const DEFAULT_COMPACTION_THREADS: i32 = 1;
// Keep flush threads higher to avoid write stalls
const DEFAULT_FLUSH_THREADS: i32 = 2;
// Disable auto compaction by default for maximum ingest throughput
// Compaction runs during quiet periods or can be triggered manually
const DEFAULT_DISABLE_AUTO_COMPACTION: bool = false;
// Soft limit before slowing writes (4GB default - very permissive)
const DEFAULT_SOFT_PENDING_COMPACTION_GB: u64 = 4;

const ENV_BLOCK_CACHE_MB: &str = "UNIRUST_BLOCK_CACHE_MB";
const ENV_WRITE_BUFFER_MB: &str = "UNIRUST_WRITE_BUFFER_MB";
const ENV_MAX_WRITE_BUFFERS: &str = "UNIRUST_MAX_WRITE_BUFFERS";
const ENV_TARGET_FILE_MB: &str = "UNIRUST_TARGET_FILE_MB";
const ENV_LEVEL_BASE_MB: &str = "UNIRUST_LEVEL_BASE_MB";
const ENV_BLOOM_BITS_PER_KEY: &str = "UNIRUST_BLOOM_BITS_PER_KEY";
const ENV_MEMTABLE_PREFIX_BLOOM_RATIO: &str = "UNIRUST_MEMTABLE_PREFIX_BLOOM_RATIO";
const ENV_RATE_LIMIT_MBPS: &str = "UNIRUST_RATE_LIMIT_MBPS";
const ENV_COMPACTION_THREADS: &str = "UNIRUST_COMPACTION_THREADS";
const ENV_FLUSH_THREADS: &str = "UNIRUST_FLUSH_THREADS";
const ENV_DISABLE_AUTO_COMPACTION: &str = "UNIRUST_DISABLE_AUTO_COMPACTION";
const ENV_SOFT_PENDING_COMPACTION_GB: &str = "UNIRUST_SOFT_PENDING_COMPACTION_GB";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StorageManifest {
    format_version: u32,
    app_version: String,
}

pub struct PersistentStore {
    inner: Store,
    db: Arc<DB>,
    cache: Mutex<LruCache<RecordId, Record>>,
    staged_records: Mutex<Vec<Record>>,
    staged_identities: Mutex<HashMap<RecordIdentity, RecordId>>,
    persisted_attr_id: u32,
    persisted_value_id: u32,
    record_count: u64,
    cluster_count: u64,
    conflict_summary_count: u64,
    read_fault: AtomicBool,
}

/// Create WriteOptions for grouped writes. A high-level ingest calls
/// `RecordStore::sync` once after all related batches have been written.
fn fast_write_opts() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(false);
    opts.disable_wal(false); // Keep WAL for crash recovery
    opts
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PersistentOpenOptions {
    pub repair: bool,
}

impl PersistentStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, PersistentOpenOptions::default())
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: PersistentOpenOptions,
    ) -> Result<Self> {
        if options.repair {
            repair_db(path.as_ref())?;
        }
        let db = open_db(path)?;
        validate_or_init_manifest(&db)?;

        let (interner, persisted_attr_id, persisted_value_id) = load_interner_state(&db)?;
        let (record_count, should_persist_count) = load_record_count(&db)?;
        let cluster_count = load_metadata::<u64>(&db, KEY_CLUSTER_COUNT)?.unwrap_or(0);
        let conflict_summary_count =
            load_metadata::<u64>(&db, KEY_CONFLICT_SUMMARY_COUNT)?.unwrap_or(0);
        let mut store = Store::with_interner(interner, 0);
        if let Some(next_id) = load_metadata::<u32>(&db, KEY_NEXT_RECORD_ID)? {
            store.set_next_record_id(next_id);
        }

        let mut instance = Self {
            inner: store,
            db: Arc::new(db),
            cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).expect("cache capacity"),
            )),
            staged_records: Mutex::new(Vec::new()),
            staged_identities: Mutex::new(HashMap::new()),
            persisted_attr_id,
            persisted_value_id,
            record_count,
            cluster_count,
            conflict_summary_count,
            read_fault: AtomicBool::new(false),
        };
        instance.rebuild_indexes_if_needed()?;
        if should_persist_count {
            instance.persist_record_count()?;
        }
        Ok(instance)
    }

    pub fn inner(&self) -> &Store {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut Store {
        &mut self.inner
    }

    fn mark_read_fault(&self) {
        self.read_fault.store(true, Ordering::Release);
    }

    fn append_interner_to_batch(&self, batch: &mut WriteBatch) -> Result<(u32, u32)> {
        let interner_cf = self
            .db
            .cf_handle(CF_INTERNER)
            .ok_or_else(|| anyhow!("missing interner column family"))?;
        let interner = self.inner.interner();
        let next_attr = interner.next_attr_id();
        let next_value = interner.next_value_id();

        for id in self.persisted_attr_id..next_attr {
            let attr_id = crate::model::AttrId(id);
            if let Some(attr) = interner.get_attr(attr_id) {
                let key = encode_interner_key(b'a', id);
                batch.put_cf(interner_cf, key, attr.as_bytes());
                let lookup_key = encode_interner_lookup_key(b'A', attr);
                batch.put_cf(interner_cf, lookup_key, id.to_be_bytes());
            }
        }

        for id in self.persisted_value_id..next_value {
            let value_id = crate::model::ValueId(id);
            if let Some(value) = interner.get_value(value_id) {
                let key = encode_interner_key(b'v', id);
                batch.put_cf(interner_cf, key, value.as_bytes());
                let lookup_key = encode_interner_lookup_key(b'V', value);
                batch.put_cf(interner_cf, lookup_key, id.to_be_bytes());
            }
        }

        batch.put_cf(
            self.db
                .cf_handle(CF_METADATA)
                .ok_or_else(|| anyhow!("missing metadata column family"))?,
            KEY_NEXT_ATTR_ID,
            bincode::serialize(&next_attr)?,
        );
        batch.put_cf(
            self.db
                .cf_handle(CF_METADATA)
                .ok_or_else(|| anyhow!("missing metadata column family"))?,
            KEY_NEXT_VALUE_ID,
            bincode::serialize(&next_value)?,
        );
        Ok((next_attr, next_value))
    }

    fn commit_interner_watermark(&mut self, watermark: (u32, u32)) {
        self.persisted_attr_id = watermark.0;
        self.persisted_value_id = watermark.1;
    }

    pub fn persist_metadata(&self, batch: &mut WriteBatch) -> Result<()> {
        self.persist_metadata_with_count(batch, self.record_count)
    }

    fn persist_metadata_with_count(&self, batch: &mut WriteBatch, record_count: u64) -> Result<()> {
        let metadata_cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        let bytes = bincode::serialize(&self.inner.next_record_id())?;
        batch.put_cf(metadata_cf, KEY_NEXT_RECORD_ID, bytes);
        let count_bytes = bincode::serialize(&record_count)?;
        batch.put_cf(metadata_cf, KEY_RECORD_COUNT, count_bytes);
        let cluster_bytes = bincode::serialize(&self.cluster_count)?;
        batch.put_cf(metadata_cf, KEY_CLUSTER_COUNT, cluster_bytes);
        let conflict_bytes = bincode::serialize(&self.conflict_summary_count)?;
        batch.put_cf(metadata_cf, KEY_CONFLICT_SUMMARY_COUNT, conflict_bytes);
        Ok(())
    }

    fn index_record(&self, record: &Record) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.index_record_with_batch(record, &mut batch)?;
        self.db.write(batch)?;
        Ok(())
    }

    pub fn save_ontology_config(&self, payload: &[u8]) -> Result<()> {
        let metadata_cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        self.db.put_cf(metadata_cf, KEY_ONTOLOGY_CONFIG, payload)?;
        Ok(())
    }

    pub fn load_ontology_config(&self) -> Result<Option<Vec<u8>>> {
        let metadata_cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        Ok(self.db.get_cf(metadata_cf, KEY_ONTOLOGY_CONFIG)?)
    }

    pub fn reset_data(&mut self) -> Result<()> {
        let mut batch = WriteBatch::default();
        for &cf_name in RESET_DATA_CFS {
            clear_cf_in_batch(&self.db, cf_name, &mut batch)?;
        }
        let metadata_cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        batch.delete_cf(metadata_cf, KEY_INDEX_VERSION);
        batch.delete_cf(metadata_cf, KEY_CROSS_SHARD_CONFLICTS);
        batch.delete_cf(metadata_cf, KEY_SOURCE_RESERVATION_BACKFILL);
        batch.put_cf(metadata_cf, KEY_NEXT_RECORD_ID, bincode::serialize(&0u32)?);
        batch.put_cf(metadata_cf, KEY_RECORD_COUNT, bincode::serialize(&0u64)?);
        batch.put_cf(metadata_cf, KEY_CLUSTER_COUNT, bincode::serialize(&0u64)?);
        batch.put_cf(
            metadata_cf,
            KEY_CONFLICT_SUMMARY_COUNT,
            bincode::serialize(&0u64)?,
        );
        batch.put_cf(metadata_cf, KEY_NEXT_ATTR_ID, bincode::serialize(&0u32)?);
        batch.put_cf(metadata_cf, KEY_NEXT_VALUE_ID, bincode::serialize(&0u32)?);
        self.db.write(batch)?;
        self.db.flush_wal(true)?;

        // Only publish the empty live view after the durable reset commits.
        self.inner = Store::new();
        self.persisted_attr_id = 0;
        self.persisted_value_id = 0;
        self.record_count = 0;
        self.cluster_count = 0;
        self.conflict_summary_count = 0;
        self.read_fault.store(false, Ordering::Release);
        self.staged_records
            .get_mut()
            .map_err(|_| anyhow!("staged records lock poisoned"))?
            .clear();
        self.staged_identities
            .get_mut()
            .map_err(|_| anyhow!("staged identities lock poisoned"))?
            .clear();
        self.cache
            .get_mut()
            .map_err(|_| anyhow!("record cache lock poisoned"))?
            .clear();
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush_wal(true)?;
        self.db.flush()?;
        Ok(())
    }

    pub fn checkpoint(&self, path: impl AsRef<Path>) -> Result<()> {
        self.db.flush_wal(true)?;
        let checkpoint = Checkpoint::new(&self.db)?;
        checkpoint.create_checkpoint(path)?;
        Ok(())
    }

    pub fn persist_state(&mut self) -> Result<()> {
        let mut batch = WriteBatch::default();
        let watermark = self.append_interner_to_batch(&mut batch)?;
        self.persist_metadata(&mut batch)?;
        self.db.write(batch)?;
        self.db.flush_wal(true)?;
        self.commit_interner_watermark(watermark);
        Ok(())
    }

    /// Stage a record for later batch write. Returns (record_id, inserted).
    /// The record is added to cache immediately so it's readable, but not yet persisted to DB.
    pub fn stage_record_if_absent(&mut self, mut record: Record) -> Result<(RecordId, bool)> {
        <Self as RecordStore>::ensure_healthy(self)?;
        if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
            self.ensure_idempotent_record(existing, &record)?;
            return Ok((existing, false));
        }
        <Self as RecordStore>::ensure_healthy(self)?;

        if let Some(existing) = self
            .staged_identities
            .lock()
            .map_err(|_| anyhow!("staged identities lock poisoned"))?
            .get(&record.identity)
            .copied()
        {
            self.ensure_idempotent_record(existing, &record)?;
            return Ok((existing, false));
        }

        let record_id = self.inner.prepare_record(&mut record)?;
        let identity = record.identity.clone();

        // Add to cache immediately so it's readable
        self.cache
            .lock()
            .map_err(|_| anyhow!("record cache lock poisoned"))?
            .put(record_id, record.clone());

        // Stage for later batch write
        self.staged_records
            .lock()
            .map_err(|_| anyhow!("staged records lock poisoned"))?
            .push(record);
        self.staged_identities
            .lock()
            .map_err(|_| anyhow!("staged identities lock poisoned"))?
            .insert(identity, record_id);

        Ok((record_id, true))
    }

    pub fn stage_record_with_explicit_id_if_absent(
        &mut self,
        mut record: Record,
    ) -> Result<(RecordId, bool)> {
        <Self as RecordStore>::ensure_healthy(self)?;
        if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
            self.ensure_idempotent_record(existing, &record)?;
            return Ok((existing, false));
        }
        <Self as RecordStore>::ensure_healthy(self)?;
        if let Some(existing) = self
            .staged_identities
            .lock()
            .map_err(|_| anyhow!("staged identities lock poisoned"))?
            .get(&record.identity)
            .copied()
        {
            self.ensure_idempotent_record(existing, &record)?;
            return Ok((existing, false));
        }
        if self.get_record(record.id).is_some() {
            anyhow::bail!("record ID {} already exists", record.id.0);
        }
        <Self as RecordStore>::ensure_healthy(self)?;

        let record_id = self.inner.prepare_record_with_explicit_id(&mut record)?;
        let identity = record.identity.clone();
        self.cache
            .lock()
            .map_err(|_| anyhow!("record cache lock poisoned"))?
            .put(record_id, record.clone());
        self.staged_records
            .lock()
            .map_err(|_| anyhow!("staged records lock poisoned"))?
            .push(record);
        self.staged_identities
            .lock()
            .map_err(|_| anyhow!("staged identities lock poisoned"))?
            .insert(identity, record_id);
        Ok((record_id, true))
    }

    fn ensure_idempotent_record(&self, existing_id: RecordId, incoming: &Record) -> Result<()> {
        let existing = self
            .get_record(existing_id)
            .ok_or_else(|| anyhow!("identity index references a missing record"))?;
        <Self as RecordStore>::ensure_healthy(self)?;
        if records_have_same_payload(&existing, incoming) {
            Ok(())
        } else {
            anyhow::bail!("source record identity already exists with a different payload")
        }
    }

    /// Flush all staged records to the database in a single batch write.
    pub fn flush_staged_records(&mut self) -> Result<usize> {
        <Self as RecordStore>::ensure_healthy(self)?;
        let records = {
            let mut staged = self
                .staged_records
                .lock()
                .map_err(|_| anyhow!("staged records lock poisoned"))?;
            std::mem::take(&mut *staged)
        };

        if records.is_empty() {
            return Ok(0);
        }

        let count = records.len();
        let next_count = self.record_count.saturating_add(count as u64);
        let write_result = (|| -> Result<(u32, u32)> {
            let mut batch = WriteBatch::default();
            let watermark = self.append_interner_to_batch(&mut batch)?;
            self.persist_metadata_with_count(&mut batch, next_count)?;
            let records_cf = self
                .db
                .cf_handle(CF_RECORDS)
                .ok_or_else(|| anyhow!("missing records column family"))?;
            for record in &records {
                let key = record.id.0.to_be_bytes();
                with_record_bytes(record, |bytes| {
                    batch.put_cf(records_cf, key, bytes);
                    Ok(())
                })?;
                self.index_record_with_batch(record, &mut batch)?;
            }
            self.db.write_opt(batch, &fast_write_opts())?;
            Ok(watermark)
        })();

        let watermark = match write_result {
            Ok(watermark) => watermark,
            Err(error) => {
                *self
                    .staged_records
                    .lock()
                    .map_err(|_| anyhow!("staged records lock poisoned"))? = records;
                return Err(error);
            }
        };
        self.commit_interner_watermark(watermark);
        self.record_count = next_count;
        self.staged_identities
            .lock()
            .map_err(|_| anyhow!("staged identities lock poisoned"))?
            .clear();

        Ok(count)
    }

    fn persist_record_count(&self) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.persist_metadata(&mut batch)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn lookup_interner_id(&self, prefix: u8, value: &str) -> Option<u32> {
        let interner_cf = match self.db.cf_handle(CF_INTERNER) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return None;
            }
        };
        let key = encode_interner_lookup_key(prefix, value);
        let bytes = match self.db.get_cf(interner_cf, key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return None,
            Err(_) => {
                self.mark_read_fault();
                return None;
            }
        };
        if bytes.len() != 4 {
            self.mark_read_fault();
            return None;
        }
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&bytes);
        Some(u32::from_be_bytes(buf))
    }

    fn lookup_interner_value(&self, prefix: u8, id: u32) -> Option<String> {
        let interner_cf = match self.db.cf_handle(CF_INTERNER) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return None;
            }
        };
        let key = encode_interner_key(prefix, id);
        let bytes = match self.db.get_cf(interner_cf, key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return None,
            Err(_) => {
                self.mark_read_fault();
                return None;
            }
        };
        match String::from_utf8(bytes) {
            Ok(value) => Some(value),
            Err(_) => {
                self.mark_read_fault();
                None
            }
        }
    }
}

impl RecordStore for PersistentStore {
    fn ensure_healthy(&self) -> Result<()> {
        if self.read_fault.load(Ordering::Acquire) {
            anyhow::bail!(
                "persistent store observed an I/O or decode failure; restart before continuing"
            );
        }
        Ok(())
    }

    fn reset_data(&mut self) -> Result<()> {
        PersistentStore::reset_data(self)
    }

    fn add_record(&mut self, record: Record) -> Result<RecordId> {
        self.ensure_healthy()?;
        let mut record = record;
        let record_id = self.inner.prepare_record(&mut record)?;
        let next_count = self.record_count.saturating_add(1);
        let mut batch = WriteBatch::default();
        let records_cf = self
            .db
            .cf_handle(CF_RECORDS)
            .ok_or_else(|| anyhow!("missing records column family"))?;
        let key = record_id.0.to_be_bytes();
        with_record_bytes(&record, |bytes| {
            batch.put_cf(records_cf, key, bytes);
            Ok(())
        })?;
        self.index_record_with_batch(&record, &mut batch)?;
        let watermark = self.append_interner_to_batch(&mut batch)?;
        self.persist_metadata_with_count(&mut batch, next_count)?;
        self.db.write(batch)?;
        self.commit_interner_watermark(watermark);
        self.record_count = next_count;
        if let Ok(mut cache) = self.cache.lock() {
            cache.put(record_id, record);
        }
        Ok(record_id)
    }

    fn add_records(&mut self, records: Vec<Record>) -> Result<()> {
        self.ensure_healthy()?;
        if records.is_empty() {
            return Ok(());
        }

        let records_cf = self
            .db
            .cf_handle(CF_RECORDS)
            .ok_or_else(|| anyhow!("missing records column family"))?;

        let mut batch = WriteBatch::default();
        let mut prepared_records = Vec::with_capacity(records.len());

        // Prepare all records (assign IDs, intern strings) without writing
        for mut record in records {
            let record_id = self.inner.prepare_record(&mut record)?;
            let key = record_id.0.to_be_bytes();
            with_record_bytes(&record, |bytes| {
                batch.put_cf(records_cf, key, bytes);
                Ok(())
            })?;
            prepared_records.push(record);
        }

        // Index all records
        for record in &prepared_records {
            self.index_record_with_batch(record, &mut batch)?;
        }

        // Persist interner and metadata once for the entire batch
        let next_count = self
            .record_count
            .saturating_add(prepared_records.len() as u64);
        let watermark = self.append_interner_to_batch(&mut batch)?;
        self.persist_metadata_with_count(&mut batch, next_count)?;

        // Single write for all records
        self.db.write(batch)?;
        self.commit_interner_watermark(watermark);
        self.record_count = next_count;

        // Update cache
        if let Ok(mut cache) = self.cache.lock() {
            for record in prepared_records {
                cache.put(record.id, record);
            }
        }

        Ok(())
    }

    fn add_record_if_absent(&mut self, record: Record) -> Result<(RecordId, bool)> {
        self.ensure_healthy()?;
        if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
            self.ensure_idempotent_record(existing, &record)?;
            return Ok((existing, false));
        }
        self.ensure_healthy()?;
        let record_id = self.add_record(record)?;
        Ok((record_id, true))
    }

    fn add_records_if_absent(&mut self, records: Vec<Record>) -> Result<Vec<(RecordId, bool)>> {
        self.ensure_healthy()?;
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // First pass: check which records already exist and separate new ones
        let mut results = Vec::with_capacity(records.len());
        let mut new_records = Vec::new();
        let mut new_record_indices = Vec::new();
        let mut pending_identities = HashMap::new();
        let mut pending_duplicates = Vec::new();

        for (idx, record) in records.into_iter().enumerate() {
            if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
                self.ensure_idempotent_record(existing, &record)?;
                results.push((existing, false));
            } else if let Some(&new_record_index) = pending_identities.get(&record.identity) {
                if !records_have_same_payload(&new_records[new_record_index], &record) {
                    anyhow::bail!(
                        "source record identity appears more than once with different payloads"
                    );
                }
                results.push((RecordId(0), false));
                pending_duplicates.push((idx, new_record_index));
            } else {
                results.push((RecordId(0), true)); // Placeholder, will be filled in
                new_record_indices.push(idx);
                pending_identities.insert(record.identity.clone(), new_records.len());
                new_records.push(record);
            }
        }
        self.ensure_healthy()?;

        if new_records.is_empty() {
            return Ok(results);
        }

        // Batch insert all new records
        let records_cf = self
            .db
            .cf_handle(CF_RECORDS)
            .ok_or_else(|| anyhow!("missing records column family"))?;

        let mut batch = WriteBatch::default();
        let mut prepared_records = Vec::with_capacity(new_records.len());
        let mut assigned_ids = Vec::with_capacity(new_records.len());

        for mut record in new_records {
            let record_id = self.inner.prepare_record(&mut record)?;
            let key = record_id.0.to_be_bytes();
            with_record_bytes(&record, |bytes| {
                batch.put_cf(records_cf, key, bytes);
                Ok(())
            })?;
            assigned_ids.push(record_id);
            prepared_records.push(record);
        }

        // Index all new records
        for record in &prepared_records {
            self.index_record_with_batch(record, &mut batch)?;
        }

        // Persist interner and metadata once
        let next_count = self
            .record_count
            .saturating_add(prepared_records.len() as u64);
        let watermark = self.append_interner_to_batch(&mut batch)?;
        self.persist_metadata_with_count(&mut batch, next_count)?;

        // Single write for all new records
        self.db.write(batch)?;
        self.commit_interner_watermark(watermark);
        self.record_count = next_count;

        // Update results with actual record IDs
        for (i, idx) in new_record_indices.into_iter().enumerate() {
            results[idx].0 = assigned_ids[i];
        }
        for (idx, new_record_index) in pending_duplicates {
            results[idx].0 = assigned_ids[new_record_index];
        }

        // Update cache
        if let Ok(mut cache) = self.cache.lock() {
            for record in prepared_records {
                cache.put(record.id, record);
            }
        }

        Ok(results)
    }

    fn stage_record_if_absent(&mut self, record: Record) -> Result<(RecordId, bool)> {
        PersistentStore::stage_record_if_absent(self, record)
    }

    fn stage_record_with_explicit_id_if_absent(
        &mut self,
        record: Record,
    ) -> Result<(RecordId, bool)> {
        PersistentStore::stage_record_with_explicit_id_if_absent(self, record)
    }

    fn flush_staged_records(&mut self) -> Result<usize> {
        PersistentStore::flush_staged_records(self)
    }

    fn reserve_source_records(
        &mut self,
        reservations: &[SourceRecordReservation],
    ) -> Result<Vec<u32>> {
        self.ensure_healthy()?;
        let cf = self
            .db
            .cf_handle(CF_SOURCE_RESERVATIONS)
            .ok_or_else(|| anyhow!("missing source reservations column family"))?;
        let mut pending = HashMap::with_capacity(reservations.len());
        let mut targets = Vec::with_capacity(reservations.len());

        for reservation in reservations {
            let key = encode_identity_index(&reservation.identity)?;
            let stored = if let Some(value) = pending.get(&key).copied() {
                Some(value)
            } else {
                let bytes = match self.db.get_cf(cf, &key) {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        self.mark_read_fault();
                        return Err(err.into());
                    }
                };
                match bytes {
                    Some(bytes) => match bincode::deserialize::<SourceReservationValue>(&bytes) {
                        Ok(value) => Some(value),
                        Err(err) => {
                            self.mark_read_fault();
                            return Err(err.into());
                        }
                    },
                    None => None,
                }
            };

            if let Some(stored) = stored {
                if stored.payload_digest != reservation.payload_digest {
                    return Err(SourceReservationError::PayloadConflict.into());
                }
                if stored.target_shard_id != reservation.target_shard_id {
                    return Err(SourceReservationError::TargetConflict {
                        existing_shard: stored.target_shard_id,
                        requested_shard: reservation.target_shard_id,
                    }
                    .into());
                }
                targets.push(stored.target_shard_id);
            } else {
                let value = SourceReservationValue {
                    payload_digest: reservation.payload_digest,
                    target_shard_id: reservation.target_shard_id,
                };
                pending.insert(key, value);
                targets.push(value.target_shard_id);
            }
        }

        if !pending.is_empty() {
            let mut batch = WriteBatch::default();
            for (key, value) in pending {
                batch.put_cf(cf, key, bincode::serialize(&value)?);
            }
            let mut options = WriteOptions::default();
            options.set_sync(true);
            self.db.write_opt(batch, &options)?;
        }
        Ok(targets)
    }

    fn source_reservation_backfill(&self) -> Result<Option<(u32, u32)>> {
        self.ensure_healthy()?;
        let cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        let bytes = match self.db.get_cf(cf, KEY_SOURCE_RESERVATION_BACKFILL) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.mark_read_fault();
                return Err(err.into());
            }
        };
        match bytes {
            Some(bytes) => match bincode::deserialize(&bytes) {
                Ok(value) => Ok(Some(value)),
                Err(err) => {
                    self.mark_read_fault();
                    Err(err.into())
                }
            },
            None => Ok(None),
        }
    }

    fn mark_source_reservation_backfill(
        &mut self,
        protocol_version: u32,
        shard_count: u32,
    ) -> Result<()> {
        self.ensure_healthy()?;
        let cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.db.put_cf_opt(
            cf,
            KEY_SOURCE_RESERVATION_BACKFILL,
            bincode::serialize(&(protocol_version, shard_count))?,
            &options,
        )?;
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.ensure_healthy()?;
        self.db.flush_wal(true)?;
        Ok(())
    }

    fn get_record(&self, id: RecordId) -> Option<Record> {
        if let Ok(mut cache) = self.cache.lock() {
            if let Some(record) = cache.get(&id) {
                return Some(record.clone());
            }
        }

        let records_cf = match self.db.cf_handle(CF_RECORDS) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return None;
            }
        };
        let key = id.0.to_be_bytes();
        let bytes = match self.db.get_cf(records_cf, key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return None,
            Err(_) => {
                self.mark_read_fault();
                return None;
            }
        };
        let record: Record = match bincode::deserialize(&bytes) {
            Ok(record) => record,
            Err(_) => {
                self.mark_read_fault();
                return None;
            }
        };
        if let Ok(mut cache) = self.cache.lock() {
            cache.put(id, record.clone());
        }
        Some(record)
    }

    fn get_all_records(&self) -> Vec<Record> {
        let records_cf = match self.db.cf_handle(CF_RECORDS) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return Vec::new();
            }
        };
        let mut records = Vec::new();
        for entry in self.db.iterator_cf(records_cf, IteratorMode::Start) {
            let (_, value) = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            };
            match bincode::deserialize(&value) {
                Ok(record) => records.push(record),
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            }
        }
        records
    }

    fn get_record_id_by_identity(&self, identity: &RecordIdentity) -> Option<RecordId> {
        let identity_cf = match self.db.cf_handle(CF_INDEX_IDENTITY) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return None;
            }
        };
        let key = match encode_identity_index(identity) {
            Ok(key) => key,
            Err(_) => {
                self.mark_read_fault();
                return None;
            }
        };
        let value = match self.db.get_cf(identity_cf, key) {
            Ok(Some(value)) => value,
            Ok(None) => return None,
            Err(_) => {
                self.mark_read_fault();
                return None;
            }
        };
        if value.len() != 4 {
            self.mark_read_fault();
            return None;
        }
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&value);
        Some(RecordId(u32::from_be_bytes(bytes)))
    }

    fn for_each_record(&self, f: &mut dyn FnMut(Record)) {
        let records_cf = match self.db.cf_handle(CF_RECORDS) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return;
            }
        };
        for entry in self.db.iterator_cf(records_cf, IteratorMode::Start) {
            let (_key, value) = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            };
            match bincode::deserialize(&value) {
                Ok(record) => f(record),
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            }
        }
    }

    fn try_for_each_record_ordered(&self, f: &mut dyn FnMut(Record) -> Result<()>) -> Result<()> {
        self.ensure_healthy()?;
        let records_cf = self
            .db
            .cf_handle(CF_RECORDS)
            .ok_or_else(|| anyhow!("missing records column family"))?;
        for entry in self.db.iterator_cf(records_cf, IteratorMode::Start) {
            let (_key, value) = entry.inspect_err(|_| {
                self.mark_read_fault();
            })?;
            let record = bincode::deserialize(&value).inspect_err(|_| {
                self.mark_read_fault();
            })?;
            f(record)?;
        }
        self.ensure_healthy()
    }

    fn get_records_by_entity_type(&self, entity_type: &str) -> Vec<Record> {
        let cf = match self.db.cf_handle(CF_INDEX_ENTITY_TYPE) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return Vec::new();
            }
        };
        let prefix = encode_string_prefix(entity_type);
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut seen = std::collections::HashSet::new();
        let mut records = Vec::new();
        for entry in iter {
            let (key, _) = match entry {
                Ok(pair) => pair,
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            };
            if !key.starts_with(&prefix) {
                break;
            }
            if let Some(record_id) = decode_string_index_record_id(&key, prefix.len()) {
                if seen.insert(record_id) {
                    if let Some(record) = self.get_record(RecordId(record_id)) {
                        records.push(record);
                    } else {
                        self.mark_read_fault();
                        break;
                    }
                }
            } else {
                self.mark_read_fault();
                break;
            }
        }
        records
    }

    fn get_records_by_perspective(&self, perspective: &str) -> Vec<Record> {
        let cf = match self.db.cf_handle(CF_INDEX_PERSPECTIVE) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return Vec::new();
            }
        };
        let prefix = encode_string_prefix(perspective);
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut seen = std::collections::HashSet::new();
        let mut records = Vec::new();
        for entry in iter {
            let (key, _) = match entry {
                Ok(pair) => pair,
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            };
            if !key.starts_with(&prefix) {
                break;
            }
            if let Some(record_id) = decode_string_index_record_id(&key, prefix.len()) {
                if seen.insert(record_id) {
                    if let Some(record) = self.get_record(RecordId(record_id)) {
                        records.push(record);
                    } else {
                        self.mark_read_fault();
                        break;
                    }
                }
            } else {
                self.mark_read_fault();
                break;
            }
        }
        records
    }

    fn get_records_with_attribute(&self, attr: crate::model::AttrId) -> Vec<Record> {
        let cf = match self.db.cf_handle(CF_INDEX_ATTR_VALUE) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return Vec::new();
            }
        };
        let prefix = encode_attr_prefix(attr.0);
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut seen = std::collections::HashSet::new();
        let mut records = Vec::new();
        for entry in iter {
            let (key, _) = match entry {
                Ok(pair) => pair,
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            };
            if !key.starts_with(&prefix) {
                break;
            }
            if let Some(record_id) = decode_attr_value_record_id(&key) {
                if seen.insert(record_id) {
                    if let Some(record) = self.get_record(RecordId(record_id)) {
                        records.push(record);
                    } else {
                        self.mark_read_fault();
                        break;
                    }
                }
            } else {
                self.mark_read_fault();
                break;
            }
        }
        records
    }

    fn get_records_in_interval(&self, interval: crate::temporal::Interval) -> Vec<Record> {
        let cf = match self.db.cf_handle(CF_INDEX_TEMPORAL_BUCKET) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return Vec::new();
            }
        };
        let mut candidates = std::collections::HashSet::new();
        for bucket in buckets_for_interval(interval.start, interval.end) {
            let prefix = bucket.to_be_bytes();
            let iter = self
                .db
                .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
            for entry in iter {
                let (key, _) = match entry {
                    Ok(pair) => pair,
                    Err(_) => {
                        self.mark_read_fault();
                        break;
                    }
                };
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some(record_id) = decode_temporal_record_id(&key) {
                    candidates.insert(record_id);
                } else {
                    self.mark_read_fault();
                    break;
                }
            }
        }
        let mut records = Vec::new();
        for record_id in candidates {
            if let Some(record) = self.get_record(RecordId(record_id)) {
                if record.descriptors.iter().any(|descriptor| {
                    crate::temporal::is_overlapping(&descriptor.interval, &interval)
                }) {
                    records.push(record);
                }
            } else {
                self.mark_read_fault();
                break;
            }
        }
        records
    }

    fn get_records_with_value_in_interval(
        &self,
        attr: crate::model::AttrId,
        value: crate::model::ValueId,
        interval: crate::temporal::Interval,
    ) -> Vec<(RecordId, crate::temporal::Interval)> {
        let cf = match self.db.cf_handle(CF_INDEX_ATTR_VALUE) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return Vec::new();
            }
        };
        let prefix = encode_attr_value_prefix(attr.0, value.0);
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut matches = Vec::new();
        for entry in iter {
            let (key, _) = match entry {
                Ok(pair) => pair,
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            };
            if !key.starts_with(&prefix) {
                break;
            }
            if let Some((record_id, record_interval)) = decode_attr_value_entry(&key) {
                if let Some(overlap) = crate::temporal::intersect(&record_interval, &interval) {
                    matches.push((RecordId(record_id), overlap));
                }
            } else {
                self.mark_read_fault();
                break;
            }
        }
        matches
    }

    fn interner(&self) -> &StringInterner {
        self.inner.interner()
    }

    fn interner_mut(&mut self) -> &mut StringInterner {
        self.inner.interner_mut()
    }

    fn intern_attr(&mut self, attr: &str) -> crate::model::AttrId {
        if let Some(id) = self.inner.interner().get_attr_id(attr) {
            return id;
        }
        if let Some(id) = self.lookup_interner_id(b'A', attr) {
            return crate::model::AttrId(id);
        }
        self.inner.interner_mut().intern_attr(attr)
    }

    fn intern_value(&mut self, value: &str) -> crate::model::ValueId {
        if let Some(id) = self.inner.interner().get_value_id(value) {
            return id;
        }
        if let Some(id) = self.lookup_interner_id(b'V', value) {
            return crate::model::ValueId(id);
        }
        self.inner.interner_mut().intern_value(value)
    }

    fn resolve_attr(&self, id: crate::model::AttrId) -> Option<String> {
        if let Some(value) = self.inner.interner().get_attr(id) {
            return Some(value.clone());
        }
        self.lookup_interner_value(b'a', id.0)
    }

    fn resolve_value(&self, id: crate::model::ValueId) -> Option<String> {
        if let Some(value) = self.inner.interner().get_value(id) {
            return Some(value.clone());
        }
        self.lookup_interner_value(b'v', id.0)
    }

    fn len(&self) -> usize {
        self.record_count as usize
    }

    fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    fn set_cluster_count(&mut self, count: usize) -> Result<()> {
        let previous = self.cluster_count;
        self.cluster_count = count as u64;
        let result = (|| {
            let mut batch = WriteBatch::default();
            self.persist_metadata(&mut batch)?;
            self.db.write(batch)?;
            Ok(())
        })();
        if result.is_err() {
            self.cluster_count = previous;
        }
        result
    }

    fn cluster_count(&self) -> Option<usize> {
        Some(self.cluster_count as usize)
    }

    fn set_conflict_summaries(
        &mut self,
        summaries: &[crate::conflicts::ConflictSummary],
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(CF_CONFLICT_SUMMARIES)
            .ok_or_else(|| anyhow!("missing conflict summaries column family"))?;
        let bytes = bincode::serialize(summaries)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, b"latest", bytes);
        let previous = self.conflict_summary_count;
        self.conflict_summary_count = summaries.len() as u64;
        let result = (|| {
            self.persist_metadata(&mut batch)?;
            self.db.write(batch)?;
            Ok(())
        })();
        if result.is_err() {
            self.conflict_summary_count = previous;
        }
        result
    }

    fn set_cluster_conflict_summaries(
        &mut self,
        cluster_id: crate::model::ClusterId,
        summaries: &[crate::conflicts::ConflictSummary],
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(CF_CONFLICT_SUMMARIES)
            .ok_or_else(|| anyhow!("missing conflict summaries column family"))?;
        let key = cluster_id.0.to_be_bytes();
        let existing = self.db.get_cf(cf, key)?;
        let existing_len = match existing {
            Some(bytes) => {
                bincode::deserialize::<Vec<crate::conflicts::ConflictSummary>>(&bytes)?.len()
            }
            None => 0,
        };
        let new_len = summaries.len();
        let total = self
            .conflict_summary_count
            .saturating_sub(existing_len as u64)
            .saturating_add(new_len as u64);
        let bytes = bincode::serialize(summaries)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, key, bytes);
        let previous = self.conflict_summary_count;
        self.conflict_summary_count = total;
        let result = (|| {
            self.persist_metadata(&mut batch)?;
            self.db.write(batch)?;
            Ok(())
        })();
        if result.is_err() {
            self.conflict_summary_count = previous;
        }
        result
    }

    fn load_conflict_summaries(&self) -> Option<Vec<crate::conflicts::ConflictSummary>> {
        let cf = match self.db.cf_handle(CF_CONFLICT_SUMMARIES) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return None;
            }
        };
        let mut summaries = Vec::new();
        for entry in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            };
            if key.as_ref() == b"latest" {
                continue;
            }
            match bincode::deserialize::<Vec<crate::conflicts::ConflictSummary>>(&value) {
                Ok(mut parsed) => summaries.append(&mut parsed),
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            }
        }
        if summaries.is_empty() {
            None
        } else {
            Some(summaries)
        }
    }

    fn set_cross_shard_conflicts(
        &mut self,
        conflicts: &[crate::sharding::CrossShardConflict],
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        self.db.put_cf(
            cf,
            KEY_CROSS_SHARD_CONFLICTS,
            bincode::serialize(conflicts)?,
        )?;
        Ok(())
    }

    fn load_cross_shard_conflicts(&self) -> Result<Vec<crate::sharding::CrossShardConflict>> {
        let cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        self.db
            .get_cf(cf, KEY_CROSS_SHARD_CONFLICTS)?
            .map(|bytes| bincode::deserialize(&bytes).map_err(Into::into))
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    fn conflict_summary_count(&self) -> Option<usize> {
        Some(self.conflict_summary_count as usize)
    }

    fn set_cluster_assignment(
        &mut self,
        record_id: RecordId,
        cluster_id: crate::model::ClusterId,
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(CF_CLUSTER_ASSIGNMENTS)
            .ok_or_else(|| anyhow!("missing cluster assignments column family"))?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, record_id.0.to_be_bytes(), cluster_id.0.to_be_bytes());
        self.db.write(batch)?;
        Ok(())
    }

    fn set_cluster_assignments_batch(
        &mut self,
        assignments: &[(RecordId, crate::model::ClusterId)],
    ) -> Result<()> {
        if assignments.is_empty() {
            return Ok(());
        }
        let cf = self
            .db
            .cf_handle(CF_CLUSTER_ASSIGNMENTS)
            .ok_or_else(|| anyhow!("missing cluster assignments column family"))?;
        let mut batch = WriteBatch::default();
        for (record_id, cluster_id) in assignments {
            batch.put_cf(cf, record_id.0.to_be_bytes(), cluster_id.0.to_be_bytes());
        }
        self.db.write_opt(batch, &fast_write_opts())?;
        Ok(())
    }

    fn records_in_id_range(
        &self,
        start: RecordId,
        end: RecordId,
        max_results: usize,
    ) -> Vec<Record> {
        let records_cf = match self.db.cf_handle(CF_RECORDS) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return Vec::new();
            }
        };
        let mut records = Vec::new();
        let start_key = start.0.to_be_bytes();
        let iter = self.db.iterator_cf(
            records_cf,
            IteratorMode::From(&start_key, Direction::Forward),
        );
        for entry in iter {
            let (key, value) = match entry {
                Ok(pair) => pair,
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            };
            let record_id = match decode_record_id_key(&key) {
                Some(id) => id,
                None => {
                    self.mark_read_fault();
                    break;
                }
            };
            if record_id >= end.0 {
                break;
            }
            match bincode::deserialize::<Record>(&value) {
                Ok(record) => {
                    records.push(record);
                    if max_results > 0 && records.len() >= max_results {
                        break;
                    }
                }
                Err(_) => {
                    self.mark_read_fault();
                    break;
                }
            }
        }
        records
    }

    fn record_id_bounds(&self) -> Option<(RecordId, RecordId)> {
        let records_cf = match self.db.cf_handle(CF_RECORDS) {
            Some(cf) => cf,
            None => {
                self.mark_read_fault();
                return None;
            }
        };
        let mut start_iter = self.db.iterator_cf(records_cf, IteratorMode::Start);
        let min_id = match start_iter.next() {
            Some(Ok((key, _))) => match decode_record_id_key(&key) {
                Some(id) => RecordId(id),
                None => {
                    self.mark_read_fault();
                    return None;
                }
            },
            Some(Err(_)) => {
                self.mark_read_fault();
                return None;
            }
            None => return None,
        };

        let mut end_iter = self.db.iterator_cf(records_cf, IteratorMode::End);
        let max_id = match end_iter.next() {
            Some(Ok((key, _))) => match decode_record_id_key(&key) {
                Some(id) => RecordId(id),
                None => {
                    self.mark_read_fault();
                    return None;
                }
            },
            Some(Err(_)) => {
                self.mark_read_fault();
                return None;
            }
            None => return None,
        };

        Some((min_id, max_id))
    }

    fn metrics(&self) -> Option<StoreMetrics> {
        let running_compactions = self
            .db
            .property_value("rocksdb.num-running-compactions")
            .ok()
            .flatten()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let running_flushes = self
            .db
            .property_value("rocksdb.num-running-flushes")
            .ok()
            .flatten()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let block_cache_capacity_bytes = self
            .db
            .property_value("rocksdb.block-cache-capacity")
            .ok()
            .flatten()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let block_cache_usage_bytes = self
            .db
            .property_value("rocksdb.block-cache-usage")
            .ok()
            .flatten()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        Some(StoreMetrics {
            persistent: true,
            running_compactions,
            running_flushes,
            block_cache_capacity_bytes,
            block_cache_usage_bytes,
        })
    }

    fn checkpoint(&self, path: &Path) -> Result<()> {
        PersistentStore::checkpoint(self, path)
    }

    fn shared_db(&self) -> Option<Arc<DB>> {
        Some(Arc::clone(&self.db))
    }
}

impl Drop for PersistentStore {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

struct RocksDbTuning {
    block_cache_bytes: u64,
    write_buffer_bytes: u64,
    max_write_buffers: i32,
    target_file_size_base: u64,
    max_bytes_for_level_base: u64,
    bloom_bits_per_key: f64,
    memtable_prefix_bloom_ratio: f64,
    rate_limit_bytes_per_sec: i64,
    compaction_threads: i32,
    flush_threads: i32,
    disable_auto_compaction: bool,
    soft_pending_compaction_bytes: u64,
}

fn load_tuning() -> RocksDbTuning {
    let block_cache_mb = env_u64(ENV_BLOCK_CACHE_MB, DEFAULT_BLOCK_CACHE_MB).max(8);
    let write_buffer_mb = env_u64(ENV_WRITE_BUFFER_MB, DEFAULT_WRITE_BUFFER_MB).max(8);
    let target_file_mb = env_u64(ENV_TARGET_FILE_MB, DEFAULT_TARGET_FILE_MB).max(8);
    let level_base_mb = env_u64(ENV_LEVEL_BASE_MB, DEFAULT_LEVEL_BASE_MB).max(64);
    let max_write_buffers = env_i32(ENV_MAX_WRITE_BUFFERS, DEFAULT_MAX_WRITE_BUFFERS).max(1);
    let bloom_bits_per_key = env_f64(ENV_BLOOM_BITS_PER_KEY, DEFAULT_BLOOM_BITS_PER_KEY);
    let memtable_prefix_bloom_ratio = env_f64(
        ENV_MEMTABLE_PREFIX_BLOOM_RATIO,
        DEFAULT_MEMTABLE_PREFIX_BLOOM_RATIO,
    );
    // Use default rate limit (20 MB/s) to favor ingest throughput
    let rate_limit_mbps = env_u64(ENV_RATE_LIMIT_MBPS, DEFAULT_RATE_LIMIT_MBPS) as i64;
    let compaction_threads = env_i32(ENV_COMPACTION_THREADS, DEFAULT_COMPACTION_THREADS).max(1);
    let flush_threads = env_i32(ENV_FLUSH_THREADS, DEFAULT_FLUSH_THREADS).max(1);
    let disable_auto_compaction =
        env_bool(ENV_DISABLE_AUTO_COMPACTION, DEFAULT_DISABLE_AUTO_COMPACTION);
    let soft_pending_compaction_gb = env_u64(
        ENV_SOFT_PENDING_COMPACTION_GB,
        DEFAULT_SOFT_PENDING_COMPACTION_GB,
    );

    RocksDbTuning {
        block_cache_bytes: block_cache_mb * 1024 * 1024,
        write_buffer_bytes: write_buffer_mb * 1024 * 1024,
        max_write_buffers,
        target_file_size_base: target_file_mb * 1024 * 1024,
        max_bytes_for_level_base: level_base_mb * 1024 * 1024,
        bloom_bits_per_key,
        memtable_prefix_bloom_ratio,
        rate_limit_bytes_per_sec: rate_limit_mbps.saturating_mul(1024 * 1024),
        compaction_threads,
        flush_threads,
        disable_auto_compaction,
        soft_pending_compaction_bytes: soft_pending_compaction_gb * 1024 * 1024 * 1024,
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_i32(key: &str, default: i32) -> i32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

fn build_base_options(tuning: &RocksDbTuning) -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_paranoid_checks(true);
    options.set_write_buffer_size(bytes_to_usize(tuning.write_buffer_bytes));
    options.set_max_write_buffer_number(tuning.max_write_buffers);
    options.set_target_file_size_base(tuning.target_file_size_base);
    options.set_max_bytes_for_level_base(tuning.max_bytes_for_level_base);

    // Separate compaction and flush threads for fine-grained control
    // Favor flush (required for writes) over compaction (can be deferred)
    let total_bg_jobs = tuning.compaction_threads + tuning.flush_threads;
    options.set_max_background_jobs(total_bg_jobs);
    // Increase L0 file limits to delay compaction triggers
    options.set_level_zero_file_num_compaction_trigger(8);
    options.set_level_zero_slowdown_writes_trigger(20);
    options.set_level_zero_stop_writes_trigger(36);

    options.set_level_compaction_dynamic_level_bytes(true);
    options.set_compression_type(DBCompressionType::Zstd);

    // Disable auto compaction if configured (for bulk ingest scenarios)
    if tuning.disable_auto_compaction {
        options.set_disable_auto_compactions(true);
    }

    // Set soft pending compaction limit - delays compaction pressure
    options.set_soft_pending_compaction_bytes_limit(bytes_to_usize(
        tuning.soft_pending_compaction_bytes,
    ));
    // Set hard limit much higher to avoid write stalls
    options.set_hard_pending_compaction_bytes_limit(bytes_to_usize(
        tuning.soft_pending_compaction_bytes * 4,
    ));

    // Rate limit compaction I/O to favor ingest throughput
    if tuning.rate_limit_bytes_per_sec > 0 {
        options.set_ratelimiter(tuning.rate_limit_bytes_per_sec, 100_000, 10);
        options.set_bytes_per_sync(1024 * 1024);
        options.set_wal_bytes_per_sync(1024 * 1024);
    }
    options
}

fn build_block_options(
    cache: &Cache,
    bloom_bits_per_key: f64,
    with_filter: bool,
) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(cache);
    if with_filter {
        block_opts.set_bloom_filter(bloom_bits_per_key, true);
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    }
    block_opts
}

fn build_cf_options(
    base: &Options,
    block_opts: &BlockBasedOptions,
    prefix: Option<SliceTransform>,
    memtable_prefix_bloom_ratio: Option<f64>,
) -> Options {
    let mut options = base.clone();
    options.set_block_based_table_factory(block_opts);
    if let Some(prefix) = prefix {
        options.set_prefix_extractor(prefix);
    }
    if let Some(ratio) = memtable_prefix_bloom_ratio {
        options.set_memtable_prefix_bloom_ratio(ratio);
    }
    options
}

fn bytes_to_usize(value: u64) -> usize {
    value.min(usize::MAX as u64) as usize
}

fn open_db(path: impl AsRef<Path>) -> Result<DB> {
    let tuning = load_tuning();
    let base = build_base_options(&tuning);
    let cache = Cache::new_lru_cache(bytes_to_usize(tuning.block_cache_bytes));

    let data_block_opts = build_block_options(&cache, tuning.bloom_bits_per_key, false);
    let index_block_opts = build_block_options(&cache, tuning.bloom_bits_per_key, true);

    let attr_value_prefix = SliceTransform::create_fixed_prefix(8);
    let temporal_prefix = SliceTransform::create_fixed_prefix(8);

    let cfs = vec![
        ColumnFamilyDescriptor::new(
            CF_RECORDS,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_METADATA,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_INTERNER,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_INDEX_ATTR_VALUE,
            build_cf_options(
                &base,
                &index_block_opts,
                Some(attr_value_prefix),
                Some(tuning.memtable_prefix_bloom_ratio),
            ),
        ),
        ColumnFamilyDescriptor::new(
            CF_INDEX_ENTITY_TYPE,
            build_cf_options(&base, &index_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_INDEX_PERSPECTIVE,
            build_cf_options(&base, &index_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_INDEX_TEMPORAL_BUCKET,
            build_cf_options(
                &base,
                &index_block_opts,
                Some(temporal_prefix),
                Some(tuning.memtable_prefix_bloom_ratio),
            ),
        ),
        ColumnFamilyDescriptor::new(
            CF_INDEX_IDENTITY,
            build_cf_options(&base, &index_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_CONFLICT_SUMMARIES,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_CLUSTER_ASSIGNMENTS,
            build_cf_options(&base, &index_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_SOURCE_RESERVATIONS,
            build_cf_options(&base, &index_block_opts, None, None),
        ),
        // DSU column families - 4-byte record_id keys, optimized for sequential access
        ColumnFamilyDescriptor::new(
            CF_DSU_PARENT,
            build_cf_options(&base, &index_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_DSU_RANK,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_DSU_GUARDS,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_DSU_METADATA,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        // Tiered index column families - variable length keys, optimized for range scans
        ColumnFamilyDescriptor::new(
            CF_INDEX_IDENTITY_KEYS,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_INDEX_KEY_STATS,
            build_cf_options(&base, &index_block_opts, None, None),
        ),
        // Linker state column families - for restart recovery
        ColumnFamilyDescriptor::new(
            CF_LINKER_CLUSTER_IDS,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_LINKER_GLOBAL_IDS,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
        ColumnFamilyDescriptor::new(
            CF_LINKER_METADATA,
            build_cf_options(&base, &data_block_opts, None, None),
        ),
    ];
    Ok(DB::open_cf_descriptors(&base, path, cfs)?)
}

pub(crate) fn durable_state_digest(db: &DB) -> Result<[u8; 32]> {
    fn update_field(digest: &mut Sha256, bytes: &[u8]) {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }

    db.flush_wal(true)?;
    let mut digest = Sha256::new();
    digest.update(b"unirust-durable-state-v1");
    for &name in DURABLE_STATE_COLUMN_FAMILIES {
        update_field(&mut digest, name.as_bytes());
        let cf = db
            .cf_handle(name)
            .ok_or_else(|| anyhow!("missing column family {name}"))?;
        for entry in db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = entry?;
            update_field(&mut digest, &key);
            update_field(&mut digest, &value);
        }
    }
    Ok(digest.finalize().into())
}

fn encode_attr_value_index(attr: u32, value: u32, start: i64, end: i64, record_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + 4 + 8 + 8 + 4);
    key.extend_from_slice(&attr.to_be_bytes());
    key.extend_from_slice(&value.to_be_bytes());
    key.extend_from_slice(&start.to_be_bytes());
    key.extend_from_slice(&end.to_be_bytes());
    key.extend_from_slice(&record_id.to_be_bytes());
    key
}

fn encode_identity_index(identity: &RecordIdentity) -> Result<Vec<u8>> {
    Ok(bincode::serialize(identity)?)
}

fn encode_attr_value_prefix(attr: u32, value: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(8);
    key.extend_from_slice(&attr.to_be_bytes());
    key.extend_from_slice(&value.to_be_bytes());
    key
}

fn encode_attr_prefix(attr: u32) -> Vec<u8> {
    attr.to_be_bytes().to_vec()
}

fn decode_attr_value_entry(key: &[u8]) -> Option<(u32, crate::temporal::Interval)> {
    if key.len() < 4 + 4 + 8 + 8 + 4 {
        return None;
    }
    let start = i64::from_be_bytes(key[8..16].try_into().ok()?);
    let end = i64::from_be_bytes(key[16..24].try_into().ok()?);
    let record_id = u32::from_be_bytes(key[24..28].try_into().ok()?);
    crate::temporal::Interval::new(start, end)
        .ok()
        .map(|interval| (record_id, interval))
}

fn decode_attr_value_record_id(key: &[u8]) -> Option<u32> {
    if key.len() < 28 {
        return None;
    }
    Some(u32::from_be_bytes(key[24..28].try_into().ok()?))
}

fn decode_record_id_key(key: &[u8]) -> Option<u32> {
    if key.len() != 4 {
        return None;
    }
    Some(u32::from_be_bytes(key.try_into().ok()?))
}

fn encode_string_prefix(value: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(value.len() + 1);
    key.extend_from_slice(value.as_bytes());
    key.push(0);
    key
}

fn encode_string_index(value: &str, record_id: u32) -> Vec<u8> {
    let mut key = encode_string_prefix(value);
    key.extend_from_slice(&record_id.to_be_bytes());
    key
}

fn decode_string_index_record_id(key: &[u8], prefix_len: usize) -> Option<u32> {
    if key.len() < prefix_len + 4 {
        return None;
    }
    Some(u32::from_be_bytes(
        key[prefix_len..prefix_len + 4].try_into().ok()?,
    ))
}

fn encode_temporal_bucket(bucket: i64, record_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + 4);
    key.extend_from_slice(&bucket.to_be_bytes());
    key.extend_from_slice(&record_id.to_be_bytes());
    key
}

fn decode_temporal_record_id(key: &[u8]) -> Option<u32> {
    if key.len() < 12 {
        return None;
    }
    Some(u32::from_be_bytes(key[8..12].try_into().ok()?))
}

fn buckets_for_interval(start: i64, end: i64) -> Vec<i64> {
    if end <= start {
        return Vec::new();
    }
    let mut buckets = Vec::new();
    let mut current = start.div_euclid(TEMPORAL_BUCKET_SECONDS);
    let end_bucket = (end - 1).div_euclid(TEMPORAL_BUCKET_SECONDS);
    while current <= end_bucket {
        buckets.push(current);
        current += 1;
    }
    buckets
}

fn is_cf_empty(db: &DB, name: &str) -> Result<bool> {
    let cf = db
        .cf_handle(name)
        .ok_or_else(|| anyhow!("missing column family {name}"))?;
    let mut iter = db.iterator_cf(cf, IteratorMode::Start);
    Ok(iter.next().is_none())
}

impl PersistentStore {
    fn rebuild_indexes_if_needed(&mut self) -> Result<()> {
        let version = load_metadata::<u32>(&self.db, KEY_INDEX_VERSION)?;
        if version == Some(INDEX_FORMAT_VERSION) {
            return Ok(());
        }
        if !is_cf_empty(&self.db, CF_RECORDS)? {
            clear_cf(&self.db, CF_INDEX_ATTR_VALUE)?;
            clear_cf(&self.db, CF_INDEX_ENTITY_TYPE)?;
            clear_cf(&self.db, CF_INDEX_PERSPECTIVE)?;
            clear_cf(&self.db, CF_INDEX_TEMPORAL_BUCKET)?;
            clear_cf(&self.db, CF_INDEX_IDENTITY)?;
            clear_cf(&self.db, CF_CONFLICT_SUMMARIES)?;
            clear_cf(&self.db, CF_CLUSTER_ASSIGNMENTS)?;
            let records_cf = self
                .db
                .cf_handle(CF_RECORDS)
                .ok_or_else(|| anyhow!("missing records column family"))?;
            for entry in self.db.iterator_cf(records_cf, IteratorMode::Start) {
                let (_key, value) = entry?;
                let record: Record = bincode::deserialize(&value)?;
                self.index_record(&record)?;
            }
        }
        save_metadata(&self.db, KEY_INDEX_VERSION, INDEX_FORMAT_VERSION)?;
        Ok(())
    }
}

fn save_metadata<T: serde::Serialize>(db: &DB, key: &[u8], value: T) -> Result<()> {
    let metadata_cf = db
        .cf_handle(CF_METADATA)
        .ok_or_else(|| anyhow!("missing metadata column family"))?;
    let bytes = bincode::serialize(&value)?;
    db.put_cf(metadata_cf, key, bytes)?;
    Ok(())
}

fn repair_db(path: &Path) -> Result<()> {
    let mut options = Options::default();
    options.create_if_missing(true);
    DB::repair(&options, path)?;
    Ok(())
}

fn load_interner_state(db: &DB) -> Result<(StringInterner, u32, u32)> {
    let mut interner = StringInterner::new();
    let next_attr = load_metadata::<u32>(db, KEY_NEXT_ATTR_ID)?.unwrap_or(0);
    let next_value = load_metadata::<u32>(db, KEY_NEXT_VALUE_ID)?.unwrap_or(0);

    let interner_cf = db
        .cf_handle(CF_INTERNER)
        .ok_or_else(|| anyhow!("missing interner column family"))?;

    if std::env::var("UNIRUST_SKIP_INTERNER_REVERSE_INDEX").is_err() {
        ensure_interner_reverse_index(db, interner_cf)?;
    }

    let attr_limit = env_u32("UNIRUST_INTERNER_CACHE_ATTRS", next_attr);
    let value_limit = env_u32("UNIRUST_INTERNER_CACHE_VALUES", next_value);

    let mut loaded_attrs = 0u32;
    let mut loaded_values = 0u32;
    for entry in db.iterator_cf(interner_cf, IteratorMode::Start) {
        let (key, value) = entry?;
        if key.is_empty() {
            continue;
        }
        let prefix = key[0];
        if prefix == b'a' && key.len() == 5 && loaded_attrs < attr_limit {
            let id = u32::from_be_bytes([key[1], key[2], key[3], key[4]]);
            let attr = String::from_utf8(value.to_vec())?;
            interner.insert_attr_with_id(crate::model::AttrId(id), attr);
            loaded_attrs += 1;
        } else if prefix == b'v' && key.len() == 5 && loaded_values < value_limit {
            let id = u32::from_be_bytes([key[1], key[2], key[3], key[4]]);
            let val = String::from_utf8(value.to_vec())?;
            interner.insert_value_with_id(crate::model::ValueId(id), val);
            loaded_values += 1;
        }
    }

    if let Some(bytes) = db.get_cf(interner_cf, KEY_INTERNER)? {
        let legacy: StringInterner = bincode::deserialize(&bytes)?;
        let legacy_next_attr = legacy.next_attr_id();
        let legacy_next_value = legacy.next_value_id();
        for id in 0..legacy_next_attr {
            if let Some(attr) = legacy.get_attr(crate::model::AttrId(id)) {
                let key = encode_interner_key(b'a', id);
                db.put_cf(interner_cf, &key, attr.as_bytes())?;
                let lookup_key = encode_interner_lookup_key(b'A', attr);
                db.put_cf(interner_cf, lookup_key, id.to_be_bytes())?;
            }
        }
        for id in 0..legacy_next_value {
            if let Some(value) = legacy.get_value(crate::model::ValueId(id)) {
                let key = encode_interner_key(b'v', id);
                db.put_cf(interner_cf, &key, value.as_bytes())?;
                let lookup_key = encode_interner_lookup_key(b'V', value);
                db.put_cf(interner_cf, lookup_key, id.to_be_bytes())?;
            }
        }
        if next_attr == 0 && next_value == 0 {
            save_metadata(db, KEY_NEXT_ATTR_ID, legacy_next_attr)?;
            save_metadata(db, KEY_NEXT_VALUE_ID, legacy_next_value)?;
            return Ok((legacy, legacy_next_attr, legacy_next_value));
        }
    }

    interner.set_next_attr_id(next_attr);
    interner.set_next_value_id(next_value);
    Ok((interner, next_attr, next_value))
}

fn ensure_interner_reverse_index(db: &DB, interner_cf: &rocksdb::ColumnFamily) -> Result<()> {
    let mut batch = WriteBatch::default();
    let mut pending = 0usize;
    for entry in db.iterator_cf(interner_cf, IteratorMode::Start) {
        let (key, value) = entry?;
        if key.len() != 5 {
            continue;
        }
        let prefix = key[0];
        if prefix != b'a' && prefix != b'v' {
            continue;
        }
        let string = String::from_utf8(value.to_vec())?;
        let id = u32::from_be_bytes([key[1], key[2], key[3], key[4]]);
        let lookup_key =
            encode_interner_lookup_key(if prefix == b'a' { b'A' } else { b'V' }, &string);
        batch.put_cf(interner_cf, lookup_key, id.to_be_bytes());
        pending += 1;
        if pending >= 10_000 {
            db.write(batch)?;
            batch = WriteBatch::default();
            pending = 0;
        }
    }
    if pending > 0 {
        db.write(batch)?;
    }
    Ok(())
}

fn load_record_count(db: &DB) -> Result<(u64, bool)> {
    if let Some(count) = load_metadata::<u64>(db, KEY_RECORD_COUNT)? {
        return Ok((count, false));
    }
    let count = count_records(db)?;
    Ok((count, true))
}

fn count_records(db: &DB) -> Result<u64> {
    let records_cf = db
        .cf_handle(CF_RECORDS)
        .ok_or_else(|| anyhow!("missing records column family"))?;
    let mut count = 0u64;
    for entry in db.iterator_cf(records_cf, IteratorMode::Start) {
        let _ = entry?;
        count += 1;
    }
    Ok(count)
}

fn validate_or_init_manifest(db: &DB) -> Result<()> {
    let metadata_cf = db
        .cf_handle(CF_METADATA)
        .ok_or_else(|| anyhow!("missing metadata column family"))?;
    if let Some(bytes) = db.get_cf(metadata_cf, KEY_MANIFEST)? {
        let manifest: StorageManifest = bincode::deserialize(&bytes)?;
        if manifest.format_version != STORAGE_FORMAT_VERSION {
            return Err(anyhow!(
                "storage format version mismatch: expected {}, found {}",
                STORAGE_FORMAT_VERSION,
                manifest.format_version
            ));
        }
        return Ok(());
    }

    let manifest = StorageManifest {
        format_version: STORAGE_FORMAT_VERSION,
        app_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let bytes = bincode::serialize(&manifest)?;
    db.put_cf(metadata_cf, KEY_MANIFEST, bytes)?;
    Ok(())
}

fn encode_interner_key(prefix: u8, id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 4);
    key.push(prefix);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn encode_interner_lookup_key(prefix: u8, value: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + value.len());
    key.push(prefix);
    key.extend_from_slice(value.as_bytes());
    key
}

impl PersistentStore {
    fn index_record_with_batch(&self, record: &Record, batch: &mut WriteBatch) -> Result<()> {
        let attr_value_cf = self
            .db
            .cf_handle(CF_INDEX_ATTR_VALUE)
            .ok_or_else(|| anyhow!("missing attr/value index column family"))?;
        let entity_type_cf = self
            .db
            .cf_handle(CF_INDEX_ENTITY_TYPE)
            .ok_or_else(|| anyhow!("missing entity_type index column family"))?;
        let perspective_cf = self
            .db
            .cf_handle(CF_INDEX_PERSPECTIVE)
            .ok_or_else(|| anyhow!("missing perspective index column family"))?;
        let temporal_cf = self
            .db
            .cf_handle(CF_INDEX_TEMPORAL_BUCKET)
            .ok_or_else(|| anyhow!("missing temporal bucket index column family"))?;
        let identity_cf = self
            .db
            .cf_handle(CF_INDEX_IDENTITY)
            .ok_or_else(|| anyhow!("missing identity index column family"))?;

        let record_id = record.id.0;
        let entity_key = encode_string_index(&record.identity.entity_type, record_id);
        batch.put_cf(entity_type_cf, entity_key, []);
        let perspective_key = encode_string_index(&record.identity.perspective, record_id);
        batch.put_cf(perspective_cf, perspective_key, []);
        let identity_key = encode_identity_index(&record.identity)?;
        batch.put_cf(identity_cf, identity_key, record_id.to_be_bytes());

        for descriptor in &record.descriptors {
            let key = encode_attr_value_index(
                descriptor.attr.0,
                descriptor.value.0,
                descriptor.interval.start,
                descriptor.interval.end,
                record_id,
            );
            batch.put_cf(attr_value_cf, key, []);

            for bucket in buckets_for_interval(descriptor.interval.start, descriptor.interval.end) {
                let key = encode_temporal_bucket(bucket, record_id);
                batch.put_cf(temporal_cf, key, []);
            }
        }

        Ok(())
    }
}

fn load_metadata<T: serde::de::DeserializeOwned>(db: &DB, key: &[u8]) -> Result<Option<T>> {
    let metadata_cf = db
        .cf_handle(CF_METADATA)
        .ok_or_else(|| anyhow!("missing metadata column family"))?;
    if let Some(bytes) = db.get_cf(metadata_cf, key)? {
        Ok(Some(bincode::deserialize(&bytes)?))
    } else {
        Ok(None)
    }
}

fn clear_cf_in_batch(db: &DB, cf_name: &str, batch: &mut WriteBatch) -> Result<()> {
    let cf = db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow!("missing column family {cf_name}"))?;
    // Keys are either fixed-width (under 64 bytes) or begin with a discriminator
    // below 0xff, so this exclusive bound covers their complete encoded keyspace.
    // A range tombstone avoids materializing millions of keys.
    batch.delete_range_cf(cf, &[][..], &[u8::MAX; 64][..]);
    Ok(())
}

fn clear_cf(db: &DB, cf_name: &str) -> Result<()> {
    let mut batch = WriteBatch::default();
    clear_cf_in_batch(db, cf_name, &mut batch)?;
    db.write(batch)?;
    Ok(())
}

/// Clear linker structures that can be reconstructed from durable records.
///
/// A crash can leave an older, internally consistent DSU snapshot alongside newer
/// acknowledged records. Reusing that mixed-generation state during a record scan
/// skips merges that are required to rebuild summaries and boundary metadata. Start
/// recovery from an empty derived state instead; records and their persisted
/// assignments remain untouched.
pub(crate) fn clear_rebuildable_linker_state(db: &DB) -> Result<()> {
    let mut batch = WriteBatch::default();
    for cf_name in [
        CF_DSU_PARENT,
        CF_DSU_RANK,
        CF_DSU_GUARDS,
        CF_DSU_METADATA,
        CF_INDEX_IDENTITY_KEYS,
        CF_INDEX_KEY_STATS,
    ] {
        clear_cf_in_batch(db, cf_name, &mut batch)?;
    }
    db.write(batch)?;
    db.flush_wal(true)?;
    Ok(())
}

// ============================================================================
// DSU Persistence Support
// ============================================================================

/// DSU key/value encoding helpers - all keys are 4-byte big-endian u32
pub mod dsu_encoding {
    use crate::dsu::TemporalGuard;
    use crate::model::RecordId;

    /// Encode a record ID as a 4-byte big-endian key
    #[inline]
    pub fn encode_record_key(record_id: RecordId) -> [u8; 4] {
        record_id.0.to_be_bytes()
    }

    /// Decode a record ID from a 4-byte big-endian key
    #[inline]
    pub fn decode_record_key(bytes: &[u8]) -> Option<RecordId> {
        if bytes.len() != 4 {
            return None;
        }
        let mut buf = [0u8; 4];
        buf.copy_from_slice(bytes);
        Some(RecordId(u32::from_be_bytes(buf)))
    }

    /// Encode a parent record ID as a 4-byte value
    #[inline]
    pub fn encode_parent_value(parent_id: RecordId) -> [u8; 4] {
        parent_id.0.to_be_bytes()
    }

    /// Decode a parent record ID from a 4-byte value
    #[inline]
    pub fn decode_parent_value(bytes: &[u8]) -> Option<RecordId> {
        decode_record_key(bytes)
    }

    /// Encode a rank as a 4-byte value
    #[inline]
    pub fn encode_rank_value(rank: u32) -> [u8; 4] {
        rank.to_be_bytes()
    }

    /// Decode a rank from a 4-byte value
    #[inline]
    pub fn decode_rank_value(bytes: &[u8]) -> Option<u32> {
        if bytes.len() != 4 {
            return None;
        }
        let mut buf = [0u8; 4];
        buf.copy_from_slice(bytes);
        Some(u32::from_be_bytes(buf))
    }

    /// Encode guards as bincode serialized bytes
    pub fn encode_guards(guards: &[TemporalGuard]) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(guards)
    }

    /// Decode guards from bincode serialized bytes
    pub fn decode_guards(bytes: &[u8]) -> Result<Vec<TemporalGuard>, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

/// Column family names for DSU persistence (re-exported for external use)
pub mod dsu_cf {
    pub const PARENT: &str = super::CF_DSU_PARENT;
    pub const RANK: &str = super::CF_DSU_RANK;
    pub const GUARDS: &str = super::CF_DSU_GUARDS;
    pub const METADATA: &str = super::CF_DSU_METADATA;
}

/// DSU metadata keys (re-exported for external use)
pub mod dsu_keys {
    pub const NEXT_CLUSTER_ID: &[u8] = super::KEY_DSU_NEXT_CLUSTER_ID;
    pub const CLUSTER_COUNT: &[u8] = super::KEY_DSU_CLUSTER_COUNT;
}

/// Column family names for tiered index persistence
pub mod index_cf {
    pub const IDENTITY_KEYS: &str = super::CF_INDEX_IDENTITY_KEYS;
    pub const KEY_STATS: &str = super::CF_INDEX_KEY_STATS;
}

/// Encoding helpers for tiered index persistence
pub mod index_encoding {
    use crate::model::{KeyValue, RecordId};
    use crate::temporal::Interval;
    use serde::{Deserialize, Serialize};

    /// Compact bucket format for warm/cold tier storage
    /// Uses 16 bytes per interval vs 32+ for full IntervalTree node
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct CompactBucketData {
        /// Record intervals: (record_id, start, end)
        pub record_intervals: Vec<(u32, i64, i64)>,
        /// Cluster intervals: (root_id, start, end)
        pub cluster_intervals: Vec<(u32, i64, i64)>,
    }

    /// Access statistics for tiering decisions
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    pub struct KeyAccessStats {
        /// Number of accesses in current epoch
        pub access_count: u32,
        /// Last access timestamp (epoch seconds)
        pub last_access: i64,
        /// Total query count since creation
        pub total_queries: u64,
        /// Cardinality (number of unique records)
        pub cardinality: u32,
    }

    impl KeyAccessStats {
        /// Calculate tier score (0.0 to 1.0)
        /// Score = 0.4 * recency + 0.4 * frequency + 0.2 * (1 - cardinality_penalty)
        pub fn tier_score(&self, current_time: i64, max_cardinality: u32) -> f64 {
            // Recency score: decay over 24 hours
            let age_seconds = (current_time - self.last_access).max(0) as f64;
            let recency = (-age_seconds / 86400.0).exp();

            // Frequency score: normalized by access count
            let frequency = (self.access_count as f64 / 100.0).min(1.0);

            // Cardinality penalty: high cardinality keys are less useful
            let cardinality_ratio = self.cardinality as f64 / max_cardinality.max(1) as f64;
            let cardinality_score = 1.0 - cardinality_ratio.min(1.0);

            0.4 * recency + 0.4 * frequency + 0.2 * cardinality_score
        }
    }

    /// Encode identity key as bytes for RocksDB key
    /// Format: entity_type_len (2 bytes) + entity_type + key_values (bincode)
    pub fn encode_identity_key(entity_type: &str, key_values: &[KeyValue]) -> Vec<u8> {
        let entity_bytes = entity_type.as_bytes();
        let mut result = Vec::with_capacity(2 + entity_bytes.len() + key_values.len() * 8);

        // Length-prefixed entity type
        let len = entity_bytes.len() as u16;
        result.extend_from_slice(&len.to_be_bytes());
        result.extend_from_slice(entity_bytes);

        // Key values as bincode
        if let Ok(kv_bytes) = bincode::serialize(key_values) {
            result.extend_from_slice(&kv_bytes);
        }

        result
    }

    /// Decode identity key from bytes
    pub fn decode_identity_key(bytes: &[u8]) -> Option<(String, Vec<KeyValue>)> {
        if bytes.len() < 2 {
            return None;
        }

        let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        if bytes.len() < 2 + len {
            return None;
        }

        let entity_type = String::from_utf8(bytes[2..2 + len].to_vec()).ok()?;
        let key_values: Vec<KeyValue> = bincode::deserialize(&bytes[2 + len..]).ok()?;

        Some((entity_type, key_values))
    }

    /// Encode compact bucket data
    pub fn encode_compact_bucket(data: &CompactBucketData) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(data)
    }

    /// Decode compact bucket data
    pub fn decode_compact_bucket(bytes: &[u8]) -> Result<CompactBucketData, bincode::Error> {
        bincode::deserialize(bytes)
    }

    /// Encode key access stats
    pub fn encode_key_stats(stats: &KeyAccessStats) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(stats)
    }

    /// Decode key access stats
    pub fn decode_key_stats(bytes: &[u8]) -> Result<KeyAccessStats, bincode::Error> {
        bincode::deserialize(bytes)
    }

    /// Convert full intervals to compact format
    pub fn intervals_to_compact(intervals: &[(RecordId, Interval)]) -> Vec<(u32, i64, i64)> {
        intervals
            .iter()
            .map(|(id, interval)| (id.0, interval.start, interval.end))
            .collect()
    }

    /// Convert compact format back to full intervals
    pub fn compact_to_intervals(compact: &[(u32, i64, i64)]) -> Vec<(RecordId, Interval)> {
        compact
            .iter()
            .filter_map(|(id, start, end)| {
                Interval::new(*start, *end)
                    .ok()
                    .map(|interval| (RecordId(*id), interval))
            })
            .collect()
    }
}

impl PersistentStore {
    /// Get a reference to the underlying RocksDB database.
    /// Used for persistent DSU operations.
    pub fn db(&self) -> &DB {
        &self.db
    }

    /// Get a shared reference to the RocksDB database.
    /// Used for sharing DB with persistent DSU and tiered index.
    pub fn db_shared(&self) -> Arc<DB> {
        Arc::clone(&self.db)
    }

    /// Get DSU parent column family handle
    pub fn dsu_parent_cf(&self) -> Option<&rocksdb::ColumnFamily> {
        self.db.cf_handle(CF_DSU_PARENT)
    }

    /// Get DSU rank column family handle
    pub fn dsu_rank_cf(&self) -> Option<&rocksdb::ColumnFamily> {
        self.db.cf_handle(CF_DSU_RANK)
    }

    /// Get DSU guards column family handle
    pub fn dsu_guards_cf(&self) -> Option<&rocksdb::ColumnFamily> {
        self.db.cf_handle(CF_DSU_GUARDS)
    }

    /// Get DSU metadata column family handle
    pub fn dsu_metadata_cf(&self) -> Option<&rocksdb::ColumnFamily> {
        self.db.cf_handle(CF_DSU_METADATA)
    }

    /// Load DSU metadata value
    pub fn load_dsu_metadata<T: serde::de::DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<T>> {
        let cf = self
            .dsu_metadata_cf()
            .ok_or_else(|| anyhow!("missing DSU metadata column family"))?;
        if let Some(bytes) = self.db.get_cf(cf, key)? {
            Ok(Some(bincode::deserialize(&bytes)?))
        } else {
            Ok(None)
        }
    }

    /// Save DSU metadata value
    pub fn save_dsu_metadata<T: serde::Serialize>(&self, key: &[u8], value: &T) -> Result<()> {
        let cf = self
            .dsu_metadata_cf()
            .ok_or_else(|| anyhow!("missing DSU metadata column family"))?;
        let bytes = bincode::serialize(value)?;
        self.db.put_cf(cf, key, bytes)?;
        Ok(())
    }

    /// Create a write batch for DSU operations
    pub fn dsu_write_batch(&self) -> WriteBatch {
        WriteBatch::default()
    }

    /// Write a DSU batch to the database
    pub fn write_dsu_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch)?;
        Ok(())
    }
}

/// Column family names for linker state persistence
pub mod linker_cf {
    pub const CLUSTER_IDS: &str = super::CF_LINKER_CLUSTER_IDS;
    pub const GLOBAL_IDS: &str = super::CF_LINKER_GLOBAL_IDS;
    pub const METADATA: &str = super::CF_LINKER_METADATA;
}

/// Encoding helpers for linker state persistence
pub mod linker_encoding {
    use crate::model::{ClusterId, GlobalClusterId, RecordId};

    /// Encode a record ID as a 4-byte big-endian key.
    pub fn encode_record_key(record_id: RecordId) -> [u8; 4] {
        record_id.0.to_be_bytes()
    }

    /// Decode a record ID from a 4-byte big-endian key.
    pub fn decode_record_key(bytes: &[u8]) -> Option<RecordId> {
        if bytes.len() < 4 {
            return None;
        }
        Some(RecordId(u32::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
        ])))
    }

    /// Encode a cluster ID as a 4-byte big-endian value.
    pub fn encode_cluster_id(cluster_id: ClusterId) -> [u8; 4] {
        cluster_id.0.to_be_bytes()
    }

    /// Decode a cluster ID from a 4-byte big-endian value.
    pub fn decode_cluster_id(bytes: &[u8]) -> Option<ClusterId> {
        if bytes.len() < 4 {
            return None;
        }
        Some(ClusterId(u32::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
        ])))
    }

    /// Encode a global cluster ID as an 8-byte value.
    /// Format: shard_id (2 bytes) + version (2 bytes) + local_id (4 bytes)
    pub fn encode_global_cluster_id(global_id: GlobalClusterId) -> [u8; 8] {
        let mut bytes = [0u8; 8];
        bytes[0..2].copy_from_slice(&global_id.shard_id.to_be_bytes());
        bytes[2..4].copy_from_slice(&global_id.version.to_be_bytes());
        bytes[4..8].copy_from_slice(&global_id.local_id.to_be_bytes());
        bytes
    }

    /// Decode a global cluster ID from an 8-byte value.
    pub fn decode_global_cluster_id(bytes: &[u8]) -> Option<GlobalClusterId> {
        if bytes.len() < 8 {
            return None;
        }
        let shard_id = u16::from_be_bytes([bytes[0], bytes[1]]);
        let version = u16::from_be_bytes([bytes[2], bytes[3]]);
        let local_id = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        Some(GlobalClusterId {
            shard_id,
            local_id,
            version,
        })
    }

    /// Encode next_cluster_id as a 4-byte value.
    pub fn encode_next_cluster_id(next_id: u32) -> [u8; 4] {
        next_id.to_be_bytes()
    }

    /// Decode next_cluster_id from a 4-byte value.
    pub fn decode_next_cluster_id(bytes: &[u8]) -> Option<u32> {
        if bytes.len() < 4 {
            return None;
        }
        Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// The metadata key for next_cluster_id
    pub const KEY_NEXT_CLUSTER_ID: &[u8] = b"linker_next_cluster_id";

    /// Prefix for durable cross-shard merge mappings.
    pub const CROSS_SHARD_MERGE_PREFIX: &[u8] = b"cross_shard_merge/";
    pub const KEY_GLOBAL_CLUSTER_ID_SCHEME: &[u8] = b"global_cluster_id_scheme";
    pub const STABLE_RECORD_ANCHOR_SCHEME: u8 = 1;

    pub fn encode_cross_shard_merge_key(secondary: GlobalClusterId) -> Vec<u8> {
        let mut key = Vec::with_capacity(CROSS_SHARD_MERGE_PREFIX.len() + 8);
        key.extend_from_slice(CROSS_SHARD_MERGE_PREFIX);
        key.extend_from_slice(&encode_global_cluster_id(secondary));
        key
    }

    pub fn decode_cross_shard_merge_key(bytes: &[u8]) -> Option<GlobalClusterId> {
        let encoded = bytes.strip_prefix(CROSS_SHARD_MERGE_PREFIX)?;
        if encoded.len() != 8 {
            return None;
        }
        decode_global_cluster_id(encoded)
    }
}

/// Linker state persistence operations
pub struct LinkerStatePersistence<'a> {
    db: &'a DB,
}

impl<'a> LinkerStatePersistence<'a> {
    /// Create a new linker state persistence helper.
    pub fn new(db: &'a DB) -> Self {
        Self { db }
    }

    /// Prepare replay-stable global IDs, removing redirects from the old
    /// allocation-order scheme if this database predates the scheme marker.
    pub fn prepare_stable_global_cluster_ids(&self) -> Result<bool> {
        let cf = self
            .db
            .cf_handle(linker_cf::METADATA)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", linker_cf::METADATA))?;
        match self
            .db
            .get_cf(&cf, linker_encoding::KEY_GLOBAL_CLUSTER_ID_SCHEME)?
            .as_deref()
        {
            Some([linker_encoding::STABLE_RECORD_ANCHOR_SCHEME]) => return Ok(false),
            Some(value) => {
                anyhow::bail!("unsupported global cluster ID scheme marker: {:?}", value);
            }
            None => {}
        }

        let had_legacy_redirects = self
            .db
            .iterator_cf(
                &cf,
                IteratorMode::From(
                    linker_encoding::CROSS_SHARD_MERGE_PREFIX,
                    Direction::Forward,
                ),
            )
            .next()
            .transpose()?
            .is_some_and(|(key, _)| key.starts_with(linker_encoding::CROSS_SHARD_MERGE_PREFIX));

        let mut prefix_end = linker_encoding::CROSS_SHARD_MERGE_PREFIX.to_vec();
        let last = prefix_end
            .last_mut()
            .ok_or_else(|| anyhow!("cross-shard merge prefix must not be empty"))?;
        *last = last
            .checked_add(1)
            .ok_or_else(|| anyhow!("cross-shard merge prefix has no upper bound"))?;

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&cf, linker_encoding::CROSS_SHARD_MERGE_PREFIX, &prefix_end);
        batch.put_cf(
            &cf,
            linker_encoding::KEY_GLOBAL_CLUSTER_ID_SCHEME,
            [linker_encoding::STABLE_RECORD_ANCHOR_SCHEME],
        );
        self.db.write(batch)?;
        self.db.flush_wal(true)?;
        Ok(had_legacy_redirects)
    }

    /// Flush cluster ID mappings to the database.
    pub fn flush_cluster_ids<I>(&self, mappings: I) -> Result<()>
    where
        I: Iterator<Item = (crate::model::RecordId, crate::model::ClusterId)>,
    {
        let cf = self
            .db
            .cf_handle(linker_cf::CLUSTER_IDS)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", linker_cf::CLUSTER_IDS))?;
        let mut batch = WriteBatch::default();
        for (record_id, cluster_id) in mappings {
            batch.put_cf(
                &cf,
                linker_encoding::encode_record_key(record_id),
                linker_encoding::encode_cluster_id(cluster_id),
            );
        }
        self.db.write(batch)?;
        Ok(())
    }

    /// Flush global cluster ID mappings to the database.
    pub fn flush_global_cluster_ids<I>(&self, mappings: I) -> Result<()>
    where
        I: Iterator<Item = (crate::model::RecordId, crate::model::GlobalClusterId)>,
    {
        let cf = self
            .db
            .cf_handle(linker_cf::GLOBAL_IDS)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", linker_cf::GLOBAL_IDS))?;
        let mut batch = WriteBatch::default();
        for (record_id, global_id) in mappings {
            batch.put_cf(
                &cf,
                linker_encoding::encode_record_key(record_id),
                linker_encoding::encode_global_cluster_id(global_id),
            );
        }
        self.db.write(batch)?;
        Ok(())
    }

    /// Save the next_cluster_id value.
    pub fn save_next_cluster_id(&self, next_id: u32) -> Result<()> {
        let cf = self
            .db
            .cf_handle(linker_cf::METADATA)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", linker_cf::METADATA))?;
        self.db.put_cf(
            &cf,
            linker_encoding::KEY_NEXT_CLUSTER_ID,
            linker_encoding::encode_next_cluster_id(next_id),
        )?;
        Ok(())
    }

    /// Persist a cross-shard redirect from a secondary cluster to its primary.
    pub fn save_cross_shard_merge(
        &self,
        secondary: GlobalClusterId,
        primary: GlobalClusterId,
    ) -> Result<()> {
        self.save_cross_shard_merges(&[(secondary, primary)])
    }

    /// Persist cross-shard redirects in one RocksDB write batch.
    pub fn save_cross_shard_merges(
        &self,
        merges: &[(GlobalClusterId, GlobalClusterId)],
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(linker_cf::METADATA)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", linker_cf::METADATA))?;
        let mut batch = WriteBatch::default();
        for (secondary, primary) in merges {
            batch.put_cf(
                &cf,
                linker_encoding::encode_cross_shard_merge_key(*secondary),
                linker_encoding::encode_global_cluster_id(*primary),
            );
        }
        self.db.write(batch)?;
        Ok(())
    }

    /// Load all durable cross-shard redirects.
    pub fn load_cross_shard_merges(&self) -> Result<Vec<(GlobalClusterId, GlobalClusterId)>> {
        let cf = self
            .db
            .cf_handle(linker_cf::METADATA)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", linker_cf::METADATA))?;
        let mut mappings = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(linker_encoding::CROSS_SHARD_MERGE_PREFIX) {
                continue;
            }
            let secondary = linker_encoding::decode_cross_shard_merge_key(&key)
                .ok_or_else(|| anyhow!("invalid persisted cross-shard merge key"))?;
            let primary = linker_encoding::decode_global_cluster_id(&value)
                .filter(|_| value.len() == 8)
                .ok_or_else(|| anyhow!("invalid persisted cross-shard merge value"))?;
            mappings.push((secondary, primary));
        }
        Ok(mappings)
    }

    /// Load the next_cluster_id value.
    pub fn load_next_cluster_id(&self) -> Result<Option<u32>> {
        let cf = self
            .db
            .cf_handle(linker_cf::METADATA)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", linker_cf::METADATA))?;
        match self.db.get_cf(&cf, linker_encoding::KEY_NEXT_CLUSTER_ID)? {
            Some(bytes) => Ok(linker_encoding::decode_next_cluster_id(&bytes)),
            None => Ok(None),
        }
    }

    /// Load all cluster ID mappings from the database.
    pub fn load_cluster_ids(
        &self,
    ) -> Result<Vec<(crate::model::RecordId, crate::model::ClusterId)>> {
        let cf = self
            .db
            .cf_handle(linker_cf::CLUSTER_IDS)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", linker_cf::CLUSTER_IDS))?;
        let mut mappings = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if let (Some(record_id), Some(cluster_id)) = (
                linker_encoding::decode_record_key(&key),
                linker_encoding::decode_cluster_id(&value),
            ) {
                mappings.push((record_id, cluster_id));
            }
        }
        Ok(mappings)
    }

    /// Load all global cluster ID mappings from the database.
    pub fn load_global_cluster_ids(
        &self,
    ) -> Result<Vec<(crate::model::RecordId, crate::model::GlobalClusterId)>> {
        let cf = self
            .db
            .cf_handle(linker_cf::GLOBAL_IDS)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", linker_cf::GLOBAL_IDS))?;
        let mut mappings = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if let (Some(record_id), Some(global_id)) = (
                linker_encoding::decode_record_key(&key),
                linker_encoding::decode_global_cluster_id(&value),
            ) {
                mappings.push((record_id, global_id));
            }
        }
        Ok(mappings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::{DistributedOntologyConfig, IdentityKeyConfig};
    use crate::linker::build_clusters;
    use crate::model::{Descriptor, RecordIdentity};
    use crate::ontology::{IdentityKey, Ontology, StrongIdentifier};
    use crate::query::{query_master_entities, QueryDescriptor, QueryOutcome};
    use crate::temporal::Interval;
    use crate::{StreamingTuning, Unirust};
    use tempfile::tempdir;

    static PERSISTENT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn lock_persistent_tests() -> std::sync::MutexGuard<'static, ()> {
        PERSISTENT_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn persistent_store_round_trip() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let path = dir.path();

        let mut store = PersistentStore::open(path).unwrap();
        let attr = store.interner_mut().intern_attr("email");
        let value = store.interner_mut().intern_value("alice@example.com");
        let record = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
            vec![Descriptor::new(attr, value, Interval::new(0, 10).unwrap())],
        );
        let record_id = store.add_record(record).unwrap();
        drop(store);

        let store = PersistentStore::open(path).unwrap();
        let loaded = store.get_record(record_id).unwrap();
        assert_eq!(loaded.identity.uid, "1");
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn source_reservation_survives_restart_and_rejects_changed_payload() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let identity = RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "durable-source".to_string(),
        );
        let reservation = SourceRecordReservation {
            identity: identity.clone(),
            payload_digest: [7; 32],
            target_shard_id: 1,
        };

        {
            let mut store = PersistentStore::open(dir.path()).unwrap();
            assert_eq!(
                store
                    .reserve_source_records(std::slice::from_ref(&reservation))
                    .unwrap(),
                vec![1]
            );
            store.mark_source_reservation_backfill(2, 3).unwrap();
        }

        let mut store = PersistentStore::open(dir.path()).unwrap();
        assert_eq!(store.source_reservation_backfill().unwrap(), Some((2, 3)));
        assert_eq!(
            store
                .reserve_source_records(std::slice::from_ref(&reservation))
                .unwrap(),
            vec![1]
        );
        let error = store
            .reserve_source_records(&[SourceRecordReservation {
                identity,
                payload_digest: [8; 32],
                target_shard_id: 1,
            }])
            .expect_err("a changed payload must remain rejected after restart");
        assert!(matches!(
            error.downcast_ref::<SourceReservationError>(),
            Some(SourceReservationError::PayloadConflict)
        ));
    }

    #[test]
    fn external_checkpoint_restores_records_and_reservation_state() {
        let _guard = lock_persistent_tests();
        let data_volume = tempdir().unwrap();
        let backup_volume = tempdir().unwrap();
        let original_path = data_volume.path().join("original");
        let checkpoint_path = backup_volume.path().join("checkpoint-1");
        let replacement_path = data_volume.path().join("replacement");
        let identity = RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "restored-source".to_string(),
        );

        {
            let mut store = PersistentStore::open(&original_path).unwrap();
            let record_id = store
                .add_record(Record::new(RecordId(0), identity.clone(), Vec::new()))
                .unwrap();
            assert_eq!(record_id, RecordId(0));
            store
                .reserve_source_records(&[SourceRecordReservation {
                    identity: identity.clone(),
                    payload_digest: [9; 32],
                    target_shard_id: 0,
                }])
                .unwrap();
            store.mark_source_reservation_backfill(2, 1).unwrap();
            store.checkpoint(&checkpoint_path).unwrap();
        }
        prepare_cluster_checkpoint(&checkpoint_path, "unit-restore", 0, 1).unwrap();
        commit_cluster_checkpoint(&checkpoint_path, "unit-restore", 0, 1).unwrap();

        restore_checkpoint(&checkpoint_path, &replacement_path).unwrap();
        let mut restored = PersistentStore::open(&replacement_path).unwrap();
        assert_eq!(
            restored.get_record_id_by_identity(&identity),
            Some(RecordId(0))
        );
        assert_eq!(
            restored.source_reservation_backfill().unwrap(),
            Some((2, 1))
        );
        let error = restored
            .reserve_source_records(&[SourceRecordReservation {
                identity,
                payload_digest: [10; 32],
                target_shard_id: 0,
            }])
            .expect_err("restored reservation must reject a changed payload");
        assert!(matches!(
            error.downcast_ref::<SourceReservationError>(),
            Some(SourceReservationError::PayloadConflict)
        ));
    }

    #[test]
    fn checkpoint_restore_refuses_nonempty_destination() {
        let _guard = lock_persistent_tests();
        let data_volume = tempdir().unwrap();
        let backup_volume = tempdir().unwrap();
        let source_path = data_volume.path().join("source");
        let checkpoint_path = backup_volume.path().join("checkpoint");
        let destination = data_volume.path().join("replacement");

        let store = PersistentStore::open(&source_path).unwrap();
        store.checkpoint(&checkpoint_path).unwrap();
        prepare_cluster_checkpoint(&checkpoint_path, "nonempty-destination", 0, 1).unwrap();
        commit_cluster_checkpoint(&checkpoint_path, "nonempty-destination", 0, 1).unwrap();
        fs::create_dir_all(&destination).unwrap();
        fs::write(destination.join("do-not-overwrite"), b"live data").unwrap();

        let error = restore_checkpoint(&checkpoint_path, &destination)
            .expect_err("restore must not overwrite a nonempty directory");
        assert!(error.to_string().contains("refusing to restore"));
        assert_eq!(
            fs::read(destination.join("do-not-overwrite")).unwrap(),
            b"live data"
        );
    }

    #[test]
    fn checkpoint_restore_rejects_uncommitted_and_wrong_shard_snapshots() {
        let _guard = lock_persistent_tests();
        let data_volume = tempdir().unwrap();
        let backup_volume = tempdir().unwrap();
        let source_path = data_volume.path().join("source");
        let checkpoint_path = backup_volume.path().join("checkpoint");
        let destination = data_volume.path().join("replacement");

        let store = PersistentStore::open(&source_path).unwrap();
        store.checkpoint(&checkpoint_path).unwrap();
        prepare_cluster_checkpoint(&checkpoint_path, "cluster-generation", 0, 2).unwrap();

        restore_checkpoint(&checkpoint_path, &destination)
            .expect_err("prepared but uncommitted checkpoint must be rejected");
        assert!(!destination.exists());

        commit_cluster_checkpoint(&checkpoint_path, "cluster-generation", 0, 2).unwrap();
        restore_checkpoint_for_shard(&checkpoint_path, &destination, Some(1))
            .expect_err("checkpoint from another shard must be rejected");
        assert!(!destination.exists());
    }

    #[test]
    fn restored_data_directory_rejects_incomplete_provenance() {
        let _guard = lock_persistent_tests();
        let data_volume = tempdir().unwrap();
        fs::write(
            data_volume.path().join(CHECKPOINT_MANIFEST_FILE),
            b"incomplete",
        )
        .unwrap();

        let error = read_restored_checkpoint_manifest(data_volume.path())
            .expect_err("one restore marker must fail closed");
        assert!(error
            .to_string()
            .contains("incomplete checkpoint provenance"));
    }

    #[test]
    fn checkpoint_restore_validates_rocksdb_before_publish() {
        let _guard = lock_persistent_tests();
        let data_volume = tempdir().unwrap();
        let backup_volume = tempdir().unwrap();
        let source_path = data_volume.path().join("source");
        let checkpoint_path = backup_volume.path().join("checkpoint");
        let destination = data_volume.path().join("replacement");

        let store = PersistentStore::open(&source_path).unwrap();
        store.checkpoint(&checkpoint_path).unwrap();
        prepare_cluster_checkpoint(&checkpoint_path, "corrupt-generation", 0, 1).unwrap();
        commit_cluster_checkpoint(&checkpoint_path, "corrupt-generation", 0, 1).unwrap();
        fs::write(checkpoint_path.join("CURRENT"), b"not-a-valid-manifest\n").unwrap();

        restore_checkpoint(&checkpoint_path, &destination)
            .expect_err("corrupt RocksDB checkpoint must be rejected");
        assert!(!destination.exists());
    }

    #[cfg(unix)]
    #[test]
    fn checkpoint_restore_rejects_top_level_symlink() {
        let _guard = lock_persistent_tests();
        use std::os::unix::fs::symlink;

        let data_volume = tempdir().unwrap();
        let backup_volume = tempdir().unwrap();
        let source_path = data_volume.path().join("source");
        let checkpoint_path = backup_volume.path().join("checkpoint");
        let checkpoint_link = backup_volume.path().join("checkpoint-link");
        let destination = data_volume.path().join("replacement");

        let store = PersistentStore::open(&source_path).unwrap();
        store.checkpoint(&checkpoint_path).unwrap();
        prepare_cluster_checkpoint(&checkpoint_path, "symlink-generation", 0, 1).unwrap();
        commit_cluster_checkpoint(&checkpoint_path, "symlink-generation", 0, 1).unwrap();
        symlink(&checkpoint_path, &checkpoint_link).unwrap();

        restore_checkpoint(&checkpoint_link, &destination)
            .expect_err("checkpoint symlink must be rejected");
        assert!(!destination.exists());
    }

    #[test]
    fn corrupt_record_read_poison_store_fail_closed() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let path = dir.path();
        let record_id;
        {
            let mut store = PersistentStore::open(path).unwrap();
            let attr = store.interner_mut().intern_attr("email");
            let value = store.interner_mut().intern_value("corrupt@example.com");
            record_id = store
                .add_record(Record::new(
                    RecordId(0),
                    RecordIdentity::new(
                        "person".to_string(),
                        "crm".to_string(),
                        "corrupt".to_string(),
                    ),
                    vec![Descriptor::new(attr, value, Interval::new(0, 10).unwrap())],
                ))
                .unwrap();
        }

        let store = PersistentStore::open(path).unwrap();
        let records_cf = store.db.cf_handle(CF_RECORDS).unwrap();
        store
            .db
            .put_cf(records_cf, record_id.0.to_be_bytes(), b"not-a-record")
            .unwrap();

        assert!(store.get_record(record_id).is_none());
        assert!(store.ensure_healthy().is_err());
        assert!(
            store.sync().is_err(),
            "an ingest acknowledgement must fail after an incomplete read"
        );
    }

    #[test]
    fn staged_batch_deduplicates_source_identity() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let path = dir.path();

        let mut store = PersistentStore::open(path).unwrap();
        let attr = store.interner_mut().intern_attr("email");
        let value = store.interner_mut().intern_value("alice@example.com");
        let identity = RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "alice-1".to_string(),
        );
        let make_record = || {
            Record::new(
                RecordId(0),
                identity.clone(),
                vec![Descriptor::new(attr, value, Interval::new(0, 10).unwrap())],
            )
        };

        let first = store.stage_record_if_absent(make_record()).unwrap();
        let second = store.stage_record_if_absent(make_record()).unwrap();

        assert!(first.1);
        assert!(!second.1);
        assert_eq!(first.0, second.0);
        assert_eq!(store.flush_staged_records().unwrap(), 1);
        drop(store);

        let store = PersistentStore::open(path).unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.get_record_id_by_identity(&identity), Some(first.0));
    }

    #[test]
    fn exhausted_record_id_sequence_survives_restart_without_wrapping() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let path = dir.path();
        let last_identity = RecordIdentity::new(
            "person".to_string(),
            "import".to_string(),
            "last-id".to_string(),
        );

        {
            let mut store = PersistentStore::open(path).unwrap();
            let record = Record::new(RecordId(u32::MAX - 1), last_identity.clone(), Vec::new());
            let (record_id, inserted) = store
                .stage_record_with_explicit_id_if_absent(record)
                .unwrap();
            assert_eq!(record_id, RecordId(u32::MAX - 1));
            assert!(inserted);
            assert_eq!(store.flush_staged_records().unwrap(), 1);
            store.sync().unwrap();
        }

        let mut store = PersistentStore::open(path).unwrap();
        let next = Record::new(
            RecordId(0),
            RecordIdentity::new(
                "person".to_string(),
                "crm".to_string(),
                "must-not-wrap".to_string(),
            ),
            Vec::new(),
        );
        let error = store
            .add_record(next)
            .expect_err("persisted record ID exhaustion must fail closed");
        assert!(error.to_string().contains("record ID space exhausted"));
        assert_eq!(store.len(), 1);
        assert_eq!(
            store.get_record_id_by_identity(&last_identity),
            Some(RecordId(u32::MAX - 1))
        );
        assert!(store.get_record(RecordId(0)).is_none());
    }

    #[test]
    fn batch_insert_deduplicates_source_identity() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let mut store = PersistentStore::open(dir.path()).unwrap();
        let identity = RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "alice-1".to_string(),
        );
        let make_record = || Record::new(RecordId(0), identity.clone(), Vec::new());

        let results = store
            .add_records_if_absent(vec![make_record(), make_record()])
            .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results[0].1);
        assert!(!results[1].1);
        assert_eq!(results[0].0, results[1].0);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn interner_watermark_advances_only_after_commit() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let mut store = PersistentStore::open(dir.path()).unwrap();
        store.interner_mut().intern_attr("email");

        let mut batch = WriteBatch::default();
        let watermark = store.append_interner_to_batch(&mut batch).unwrap();

        assert_eq!(store.persisted_attr_id, 0);
        assert_eq!(watermark.0, 1);
        store.db.write(batch).unwrap();
        store.commit_interner_watermark(watermark);
        assert_eq!(store.persisted_attr_id, 1);
    }

    #[test]
    fn persistent_store_retains_ontology_and_sequence() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let path = dir.path();

        let mut store = PersistentStore::open(path).unwrap();
        let config = DistributedOntologyConfig {
            identity_keys: vec![IdentityKeyConfig {
                name: "email_key".to_string(),
                attributes: vec!["email".to_string()],
            }],
            strong_identifiers: vec!["email".to_string()],
            constraints: Vec::new(),
        };
        let payload = bincode::serialize(&config).unwrap();
        store.save_ontology_config(&payload).unwrap();

        let attr = store.interner_mut().intern_attr("email");
        let value = store.interner_mut().intern_value("first@example.com");
        let record = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
            vec![Descriptor::new(attr, value, Interval::new(0, 10).unwrap())],
        );
        let first_id = store.add_record(record).unwrap();
        assert_eq!(first_id.0, 0);
        drop(store);

        let mut store = PersistentStore::open(path).unwrap();
        let stored = store.load_ontology_config().unwrap().unwrap();
        let decoded: DistributedOntologyConfig = bincode::deserialize(&stored).unwrap();
        assert_eq!(decoded.identity_keys.len(), 1);
        assert_eq!(decoded.strong_identifiers, vec!["email".to_string()]);

        let attr = store.interner_mut().intern_attr("email");
        let value = store.interner_mut().intern_value("second@example.com");
        let record = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "2".to_string()),
            vec![Descriptor::new(attr, value, Interval::new(10, 20).unwrap())],
        );
        let second_id = store.add_record(record).unwrap();
        assert_eq!(second_id.0, 1);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn reset_clears_all_persistent_resolution_state() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let mut store = PersistentStore::open(dir.path()).unwrap();
        store.save_ontology_config(b"retained-config").unwrap();

        for &cf_name in RESET_DATA_CFS {
            let cf = store.db.cf_handle(cf_name).unwrap();
            store.db.put_cf(cf, b"stale-key", b"stale-value").unwrap();
        }
        store.reset_data().unwrap();

        for &cf_name in RESET_DATA_CFS {
            let cf = store.db.cf_handle(cf_name).unwrap();
            assert_eq!(store.db.iterator_cf(cf, IteratorMode::Start).count(), 0);
        }
        assert_eq!(
            store.load_ontology_config().unwrap().as_deref(),
            Some(b"retained-config".as_slice())
        );
    }

    #[test]
    fn persistent_store_preserves_conflict_results() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let path = dir.path();

        let mut store = PersistentStore::open(path).unwrap();
        let email_attr = store.interner_mut().intern_attr("email");
        let email_value_a = store.interner_mut().intern_value("alice@example.com");
        let email_value_b = store.interner_mut().intern_value("bob@example.com");

        let mut ontology = Ontology::new();
        ontology.add_identity_key(IdentityKey::new(vec![email_attr], "email_key".to_string()));
        ontology.add_strong_identifier(StrongIdentifier::new(email_attr, "email".to_string()));

        let mut unirust =
            Unirust::with_store_and_tuning(ontology, store, StreamingTuning::default());

        let record_a = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
            vec![Descriptor::new(
                email_attr,
                email_value_a,
                Interval::new(0, 10).unwrap(),
            )],
        );
        let record_b = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "2".to_string()),
            vec![Descriptor::new(
                email_attr,
                email_value_b,
                Interval::new(0, 10).unwrap(),
            )],
        );
        unirust.stream_records(vec![record_a, record_b]).unwrap();
        let clusters = unirust.build_clusters().unwrap();
        let observations = unirust.detect_conflicts(&clusters).unwrap();
        let conflict_count = observations.len();
        drop(unirust);

        let mut store = PersistentStore::open(path).unwrap();
        let email_attr = store.interner_mut().intern_attr("email");
        let mut ontology = Ontology::new();
        ontology.add_identity_key(IdentityKey::new(vec![email_attr], "email_key".to_string()));
        ontology.add_strong_identifier(StrongIdentifier::new(email_attr, "email".to_string()));

        let unirust = Unirust::with_store_and_tuning(ontology, store, StreamingTuning::default());
        let clusters = unirust.build_clusters().unwrap();
        let observations = unirust.detect_conflicts(&clusters).unwrap();
        assert_eq!(observations.len(), conflict_count);
    }

    #[test]
    fn persistent_store_query_after_restart() {
        let _guard = lock_persistent_tests();
        let dir = tempdir().unwrap();
        let path = dir.path();

        let mut store = PersistentStore::open(path).unwrap();
        let email_attr = store.interner_mut().intern_attr("email");
        let email_value = store.interner_mut().intern_value("alice@example.com");

        let mut ontology = Ontology::new();
        ontology.add_identity_key(IdentityKey::new(vec![email_attr], "email_key".to_string()));

        let record = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
            vec![Descriptor::new(
                email_attr,
                email_value,
                Interval::new(0, 10).unwrap(),
            )],
        );
        store.add_record(record).unwrap();
        let clusters = build_clusters(&store, &ontology).unwrap();
        let outcome = query_master_entities(
            &store,
            &clusters,
            &ontology,
            &[QueryDescriptor {
                attr: email_attr,
                value: email_value,
            }],
            Interval::new(0, 10).unwrap(),
        )
        .unwrap();

        let QueryOutcome::Matches(matches) = outcome else {
            panic!("expected matches before restart");
        };
        assert_eq!(matches.len(), 1);
        drop(store);

        let mut store = PersistentStore::open(path).unwrap();
        let email_attr = store.interner_mut().intern_attr("email");
        let email_value = store.interner_mut().intern_value("alice@example.com");
        let mut ontology = Ontology::new();
        ontology.add_identity_key(IdentityKey::new(vec![email_attr], "email_key".to_string()));

        let clusters = build_clusters(&store, &ontology).unwrap();
        let outcome = query_master_entities(
            &store,
            &clusters,
            &ontology,
            &[QueryDescriptor {
                attr: email_attr,
                value: email_value,
            }],
            Interval::new(0, 10).unwrap(),
        )
        .unwrap();
        let QueryOutcome::Matches(matches) = outcome else {
            panic!("expected matches after restart");
        };
        assert_eq!(matches.len(), 1);
    }
}
