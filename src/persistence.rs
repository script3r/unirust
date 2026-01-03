use crate::model::{Record, RecordId, RecordIdentity, StringInterner};
use crate::store::{RecordStore, Store, StoreMetrics};
use anyhow::{anyhow, Result};
use lru::LruCache;
use rocksdb::{
    checkpoint::Checkpoint, BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType,
    Direction, IteratorMode, Options, SliceTransform, WriteBatch, WriteOptions, DB,
};
use std::path::Path;
use std::sync::{Arc, Mutex};

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

const ENV_BLOCK_CACHE_MB: &str = "UNIRUST_BLOCK_CACHE_MB";
const ENV_WRITE_BUFFER_MB: &str = "UNIRUST_WRITE_BUFFER_MB";
const ENV_MAX_WRITE_BUFFERS: &str = "UNIRUST_MAX_WRITE_BUFFERS";
const ENV_TARGET_FILE_MB: &str = "UNIRUST_TARGET_FILE_MB";
const ENV_LEVEL_BASE_MB: &str = "UNIRUST_LEVEL_BASE_MB";
const ENV_BLOOM_BITS_PER_KEY: &str = "UNIRUST_BLOOM_BITS_PER_KEY";
const ENV_MEMTABLE_PREFIX_BLOOM_RATIO: &str = "UNIRUST_MEMTABLE_PREFIX_BLOOM_RATIO";
const ENV_RATE_LIMIT_MBPS: &str = "UNIRUST_RATE_LIMIT_MBPS";

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
    persisted_attr_id: u32,
    persisted_value_id: u32,
    record_count: u64,
    cluster_count: u64,
    conflict_summary_count: u64,
}

/// Create WriteOptions for fast async writes (no WAL sync)
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
            persisted_attr_id,
            persisted_value_id,
            record_count,
            cluster_count,
            conflict_summary_count,
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

    pub fn persist_interner(&mut self, batch: &mut WriteBatch) -> Result<()> {
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

        self.persisted_attr_id = next_attr;
        self.persisted_value_id = next_value;
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
        Ok(())
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

    pub fn persist_record(&self, record: &Record) -> Result<()> {
        let records_cf = self
            .db
            .cf_handle(CF_RECORDS)
            .ok_or_else(|| anyhow!("missing records column family"))?;
        let key = record.id.0.to_be_bytes();
        let bytes = bincode::serialize(record)?;
        self.db.put_cf(records_cf, key, bytes)?;
        Ok(())
    }

    pub fn index_record(&self, record: &Record) -> Result<()> {
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
        clear_cf(&self.db, CF_RECORDS)?;
        clear_cf(&self.db, CF_INTERNER)?;
        clear_cf(&self.db, CF_INDEX_ATTR_VALUE)?;
        clear_cf(&self.db, CF_INDEX_ENTITY_TYPE)?;
        clear_cf(&self.db, CF_INDEX_PERSPECTIVE)?;
        clear_cf(&self.db, CF_INDEX_TEMPORAL_BUCKET)?;
        clear_cf(&self.db, CF_INDEX_IDENTITY)?;
        clear_cf(&self.db, CF_CONFLICT_SUMMARIES)?;
        clear_cf(&self.db, CF_CLUSTER_ASSIGNMENTS)?;
        remove_metadata_key(&self.db, KEY_NEXT_RECORD_ID)?;
        remove_metadata_key(&self.db, KEY_INDEX_VERSION)?;
        self.inner = Store::new();
        self.persisted_attr_id = 0;
        self.persisted_value_id = 0;
        self.record_count = 0;
        self.cluster_count = 0;
        self.conflict_summary_count = 0;
        let mut batch = WriteBatch::default();
        self.persist_interner(&mut batch)?;
        self.persist_metadata(&mut batch)?;
        self.db.write(batch)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }

    pub fn checkpoint(&self, path: impl AsRef<Path>) -> Result<()> {
        let checkpoint = Checkpoint::new(&self.db)?;
        checkpoint.create_checkpoint(path)?;
        Ok(())
    }

    pub fn persist_state(&mut self) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.persist_interner(&mut batch)?;
        self.persist_metadata(&mut batch)?;
        self.db.write(batch)?;
        Ok(())
    }

    /// Stage a record for later batch write. Returns (record_id, inserted).
    /// The record is added to cache immediately so it's readable, but not yet persisted to DB.
    pub fn stage_record_if_absent(&mut self, mut record: Record) -> Result<(RecordId, bool)> {
        if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
            return Ok((existing, false));
        }

        let record_id = self.inner.prepare_record(&mut record)?;

        // Add to cache immediately so it's readable
        if let Ok(mut cache) = self.cache.lock() {
            cache.put(record_id, record.clone());
        }

        // Stage for later batch write
        if let Ok(mut staged) = self.staged_records.lock() {
            staged.push(record);
        }

        Ok((record_id, true))
    }

    /// Flush all staged records to the database in a single batch write.
    pub fn flush_staged_records(&mut self) -> Result<usize> {
        let records = {
            let mut staged = self
                .staged_records
                .lock()
                .map_err(|_| anyhow!("lock error"))?;
            std::mem::take(&mut *staged)
        };

        if records.is_empty() {
            return Ok(0);
        }

        let count = records.len();
        let records_cf = self
            .db
            .cf_handle(CF_RECORDS)
            .ok_or_else(|| anyhow!("missing records column family"))?;

        let mut batch = WriteBatch::default();

        for record in &records {
            let key = record.id.0.to_be_bytes();
            let bytes = bincode::serialize(record)?;
            batch.put_cf(records_cf, key, bytes);
            self.index_record_with_batch(record, &mut batch)?;
        }

        let next_count = self.record_count.saturating_add(count as u64);
        self.persist_interner(&mut batch)?;
        self.persist_metadata_with_count(&mut batch, next_count)?;

        self.db.write_opt(batch, &fast_write_opts())?;
        self.record_count = next_count;

        Ok(count)
    }

    fn persist_record_count(&self) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.persist_metadata(&mut batch)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn lookup_interner_id(&self, prefix: u8, value: &str) -> Option<u32> {
        let interner_cf = self.db.cf_handle(CF_INTERNER)?;
        let key = encode_interner_lookup_key(prefix, value);
        let bytes = self.db.get_cf(interner_cf, key).ok()??;
        if bytes.len() != 4 {
            return None;
        }
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&bytes);
        Some(u32::from_be_bytes(buf))
    }

    fn lookup_interner_value(&self, prefix: u8, id: u32) -> Option<String> {
        let interner_cf = self.db.cf_handle(CF_INTERNER)?;
        let key = encode_interner_key(prefix, id);
        let bytes = self.db.get_cf(interner_cf, key).ok()??;
        String::from_utf8(bytes).ok()
    }
}

impl RecordStore for PersistentStore {
    fn add_record(&mut self, record: Record) -> Result<RecordId> {
        let mut record = record;
        let record_id = self.inner.prepare_record(&mut record)?;
        let next_count = self.record_count.saturating_add(1);
        let mut batch = WriteBatch::default();
        let records_cf = self
            .db
            .cf_handle(CF_RECORDS)
            .ok_or_else(|| anyhow!("missing records column family"))?;
        let key = record_id.0.to_be_bytes();
        let bytes = bincode::serialize(&record)?;
        batch.put_cf(records_cf, key, bytes);
        self.index_record_with_batch(&record, &mut batch)?;
        self.persist_interner(&mut batch)?;
        self.persist_metadata_with_count(&mut batch, next_count)?;
        self.db.write(batch)?;
        self.record_count = next_count;
        if let Ok(mut cache) = self.cache.lock() {
            cache.put(record_id, record);
        }
        Ok(record_id)
    }

    fn add_records(&mut self, records: Vec<Record>) -> Result<()> {
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
            let bytes = bincode::serialize(&record)?;
            batch.put_cf(records_cf, key, bytes);
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
        self.persist_interner(&mut batch)?;
        self.persist_metadata_with_count(&mut batch, next_count)?;

        // Single write for all records
        self.db.write(batch)?;
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
        if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
            return Ok((existing, false));
        }
        let record_id = self.add_record(record)?;
        Ok((record_id, true))
    }

    fn add_records_if_absent(&mut self, records: Vec<Record>) -> Result<Vec<(RecordId, bool)>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // First pass: check which records already exist and separate new ones
        let mut results = Vec::with_capacity(records.len());
        let mut new_records = Vec::new();
        let mut new_record_indices = Vec::new();

        for (idx, record) in records.into_iter().enumerate() {
            if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
                results.push((existing, false));
            } else {
                results.push((RecordId(0), true)); // Placeholder, will be filled in
                new_record_indices.push(idx);
                new_records.push(record);
            }
        }

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
            let bytes = bincode::serialize(&record)?;
            batch.put_cf(records_cf, key, bytes);
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
        self.persist_interner(&mut batch)?;
        self.persist_metadata_with_count(&mut batch, next_count)?;

        // Single write for all new records
        self.db.write(batch)?;
        self.record_count = next_count;

        // Update results with actual record IDs
        for (i, idx) in new_record_indices.into_iter().enumerate() {
            results[idx].0 = assigned_ids[i];
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

    fn flush_staged_records(&mut self) -> Result<usize> {
        PersistentStore::flush_staged_records(self)
    }

    fn get_record(&self, id: RecordId) -> Option<Record> {
        if let Ok(mut cache) = self.cache.lock() {
            if let Some(record) = cache.get(&id) {
                return Some(record.clone());
            }
        }

        let records_cf = self.db.cf_handle(CF_RECORDS)?;
        let key = id.0.to_be_bytes();
        let bytes = self.db.get_cf(records_cf, key).ok()??;
        let record: Record = bincode::deserialize(&bytes).ok()?;
        if let Ok(mut cache) = self.cache.lock() {
            cache.put(id, record.clone());
        }
        Some(record)
    }

    fn get_all_records(&self) -> Vec<Record> {
        let records_cf = match self.db.cf_handle(CF_RECORDS) {
            Some(cf) => cf,
            None => return Vec::new(),
        };
        self.db
            .iterator_cf(records_cf, IteratorMode::Start)
            .filter_map(|entry| {
                entry
                    .ok()
                    .and_then(|(_, value)| bincode::deserialize(&value).ok())
            })
            .collect()
    }

    fn get_record_id_by_identity(&self, identity: &RecordIdentity) -> Option<RecordId> {
        let identity_cf = self.db.cf_handle(CF_INDEX_IDENTITY)?;
        let key = encode_identity_index(identity).ok()?;
        let value = self.db.get_cf(identity_cf, key).ok()??;
        if value.len() != 4 {
            return None;
        }
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&value);
        Some(RecordId(u32::from_be_bytes(bytes)))
    }

    fn for_each_record(&self, f: &mut dyn FnMut(Record)) {
        let records_cf = match self.db.cf_handle(CF_RECORDS) {
            Some(cf) => cf,
            None => return,
        };
        for (_key, value) in self
            .db
            .iterator_cf(records_cf, IteratorMode::Start)
            .flatten()
        {
            if let Ok(record) = bincode::deserialize(&value) {
                f(record);
            }
        }
    }

    fn get_records_by_entity_type(&self, entity_type: &str) -> Vec<Record> {
        let cf = match self.db.cf_handle(CF_INDEX_ENTITY_TYPE) {
            Some(cf) => cf,
            None => return Vec::new(),
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
                Err(_) => break,
            };
            if !key.starts_with(&prefix) {
                break;
            }
            if let Some(record_id) = decode_string_index_record_id(&key, prefix.len()) {
                if seen.insert(record_id) {
                    if let Some(record) = self.get_record(RecordId(record_id)) {
                        records.push(record);
                    }
                }
            }
        }
        records
    }

    fn get_records_by_perspective(&self, perspective: &str) -> Vec<Record> {
        let cf = match self.db.cf_handle(CF_INDEX_PERSPECTIVE) {
            Some(cf) => cf,
            None => return Vec::new(),
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
                Err(_) => break,
            };
            if !key.starts_with(&prefix) {
                break;
            }
            if let Some(record_id) = decode_string_index_record_id(&key, prefix.len()) {
                if seen.insert(record_id) {
                    if let Some(record) = self.get_record(RecordId(record_id)) {
                        records.push(record);
                    }
                }
            }
        }
        records
    }

    fn get_records_with_attribute(&self, attr: crate::model::AttrId) -> Vec<Record> {
        let cf = match self.db.cf_handle(CF_INDEX_ATTR_VALUE) {
            Some(cf) => cf,
            None => return Vec::new(),
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
                Err(_) => break,
            };
            if !key.starts_with(&prefix) {
                break;
            }
            if let Some(record_id) = decode_attr_value_record_id(&key) {
                if seen.insert(record_id) {
                    if let Some(record) = self.get_record(RecordId(record_id)) {
                        records.push(record);
                    }
                }
            }
        }
        records
    }

    fn get_records_in_interval(&self, interval: crate::temporal::Interval) -> Vec<Record> {
        let cf = match self.db.cf_handle(CF_INDEX_TEMPORAL_BUCKET) {
            Some(cf) => cf,
            None => return Vec::new(),
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
                    Err(_) => break,
                };
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some(record_id) = decode_temporal_record_id(&key) {
                    candidates.insert(record_id);
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
            None => return Vec::new(),
        };
        let prefix = encode_attr_value_prefix(attr.0, value.0);
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut matches = Vec::new();
        for entry in iter {
            let (key, _) = match entry {
                Ok(pair) => pair,
                Err(_) => break,
            };
            if !key.starts_with(&prefix) {
                break;
            }
            if let Some((record_id, record_interval)) = decode_attr_value_entry(&key) {
                if let Some(overlap) = crate::temporal::intersect(&record_interval, &interval) {
                    matches.push((RecordId(record_id), overlap));
                }
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
        self.cluster_count = count as u64;
        let mut batch = WriteBatch::default();
        self.persist_metadata(&mut batch)?;
        self.db.write(batch)?;
        Ok(())
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
        self.conflict_summary_count = summaries.len() as u64;
        self.persist_metadata(&mut batch)?;
        self.db.write(batch)?;
        Ok(())
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
        let existing = self.db.get_cf(cf, key).ok().flatten();
        let existing_len = existing
            .as_ref()
            .and_then(|bytes| {
                bincode::deserialize::<Vec<crate::conflicts::ConflictSummary>>(bytes).ok()
            })
            .map(|summaries| summaries.len())
            .unwrap_or(0);
        let new_len = summaries.len();
        let total = self
            .conflict_summary_count
            .saturating_sub(existing_len as u64)
            .saturating_add(new_len as u64);
        let bytes = bincode::serialize(summaries)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, key, bytes);
        self.conflict_summary_count = total;
        self.persist_metadata(&mut batch)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn load_conflict_summaries(&self) -> Option<Vec<crate::conflicts::ConflictSummary>> {
        let cf = self.db.cf_handle(CF_CONFLICT_SUMMARIES)?;
        let mut summaries = Vec::new();
        for (key, value) in self.db.iterator_cf(cf, IteratorMode::Start).flatten() {
            if key.as_ref() == b"latest" {
                continue;
            }
            if let Ok(mut parsed) =
                bincode::deserialize::<Vec<crate::conflicts::ConflictSummary>>(&value)
            {
                summaries.append(&mut parsed);
            }
        }
        if summaries.is_empty() {
            None
        } else {
            Some(summaries)
        }
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
            None => return Vec::new(),
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
                Err(_) => break,
            };
            let record_id = match decode_record_id_key(&key) {
                Some(id) => id,
                None => continue,
            };
            if record_id >= end.0 {
                break;
            }
            if let Ok(record) = bincode::deserialize::<Record>(&value) {
                records.push(record);
                if max_results > 0 && records.len() >= max_results {
                    break;
                }
            }
        }
        records
    }

    fn record_id_bounds(&self) -> Option<(RecordId, RecordId)> {
        let records_cf = self.db.cf_handle(CF_RECORDS)?;
        let mut start_iter = self.db.iterator_cf(records_cf, IteratorMode::Start);
        let min_id = start_iter
            .next()
            .and_then(|entry| entry.ok())
            .and_then(|(key, _)| decode_record_id_key(&key))
            .map(RecordId)?;

        let mut end_iter = self.db.iterator_cf(records_cf, IteratorMode::End);
        let max_id = end_iter
            .next()
            .and_then(|entry| entry.ok())
            .and_then(|(key, _)| decode_record_id_key(&key))
            .map(RecordId)?;

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
    let rate_limit_mbps = env_u64(ENV_RATE_LIMIT_MBPS, 0) as i64;

    RocksDbTuning {
        block_cache_bytes: block_cache_mb * 1024 * 1024,
        write_buffer_bytes: write_buffer_mb * 1024 * 1024,
        max_write_buffers,
        target_file_size_base: target_file_mb * 1024 * 1024,
        max_bytes_for_level_base: level_base_mb * 1024 * 1024,
        bloom_bits_per_key,
        memtable_prefix_bloom_ratio,
        rate_limit_bytes_per_sec: rate_limit_mbps.saturating_mul(1024 * 1024),
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

fn build_base_options(tuning: &RocksDbTuning) -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_paranoid_checks(true);
    options.set_write_buffer_size(bytes_to_usize(tuning.write_buffer_bytes));
    options.set_max_write_buffer_number(tuning.max_write_buffers);
    options.set_target_file_size_base(tuning.target_file_size_base);
    options.set_max_bytes_for_level_base(tuning.max_bytes_for_level_base);
    options.set_max_background_jobs(4);
    options.set_level_compaction_dynamic_level_bytes(true);
    options.set_compression_type(DBCompressionType::Zstd);
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

fn clear_cf(db: &DB, cf_name: &str) -> Result<()> {
    let cf = db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow!("missing column family {cf_name}"))?;
    let keys: Vec<Vec<u8>> = db
        .iterator_cf(cf, IteratorMode::Start)
        .map(|entry| entry.map(|(key, _)| key.to_vec()))
        .collect::<Result<Vec<_>, _>>()?;
    if keys.is_empty() {
        return Ok(());
    }
    let mut batch = WriteBatch::default();
    for key in keys {
        batch.delete_cf(cf, key);
    }
    db.write(batch)?;
    Ok(())
}

fn remove_metadata_key(db: &DB, key: &[u8]) -> Result<()> {
    let metadata_cf = db
        .cf_handle(CF_METADATA)
        .ok_or_else(|| anyhow!("missing metadata column family"))?;
    db.delete_cf(metadata_cf, key)?;
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

    #[test]
    fn persistent_store_round_trip() {
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
    fn persistent_store_retains_ontology_and_sequence() {
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
        let payload = serde_json::to_vec(&config).unwrap();
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
        let decoded: DistributedOntologyConfig = serde_json::from_slice(&stored).unwrap();
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
    fn persistent_store_preserves_conflict_results() {
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
