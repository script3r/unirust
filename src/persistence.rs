use crate::model::{Record, RecordId, StringInterner};
use crate::store::{RecordStore, Store, StoreMetrics};
use anyhow::{anyhow, Result};
use lru::LruCache;
use rocksdb::{
    checkpoint::Checkpoint, BlockBasedOptions, Cache, ColumnFamilyDescriptor, Direction,
    IteratorMode, Options, SliceTransform, WriteBatch, DB, DBCompressionType,
};
use std::path::Path;
use std::sync::Mutex;

const CF_RECORDS: &str = "records";
const CF_METADATA: &str = "metadata";
const CF_INTERNER: &str = "interner";
const CF_INDEX_ATTR_VALUE: &str = "index_attr_value";
const CF_INDEX_ENTITY_TYPE: &str = "index_entity_type";
const CF_INDEX_PERSPECTIVE: &str = "index_perspective";
const CF_INDEX_TEMPORAL_BUCKET: &str = "index_temporal_bucket";

const KEY_NEXT_RECORD_ID: &[u8] = b"next_record_id";
const KEY_INTERNER: &[u8] = b"interner";
const KEY_ONTOLOGY_CONFIG: &[u8] = b"ontology_config";
const KEY_MANIFEST: &[u8] = b"manifest";
const KEY_INDEX_VERSION: &[u8] = b"index_version";
const KEY_NEXT_ATTR_ID: &[u8] = b"next_attr_id";
const KEY_NEXT_VALUE_ID: &[u8] = b"next_value_id";

const STORAGE_FORMAT_VERSION: u32 = 1;
const INDEX_FORMAT_VERSION: u32 = 1;
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
    db: DB,
    cache: Mutex<LruCache<RecordId, Record>>,
    persisted_attr_id: u32,
    persisted_value_id: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct PersistentOpenOptions {
    pub repair: bool,
}

impl Default for PersistentOpenOptions {
    fn default() -> Self {
        Self { repair: false }
    }
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
        let mut store = Store::with_interner(interner, 0);
        if let Some(next_id) = load_metadata::<u32>(&db, KEY_NEXT_RECORD_ID)? {
            store.set_next_record_id(next_id);
        }

        let mut instance = Self {
            inner: store,
            db,
            cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(DEFAULT_CACHE_CAPACITY)
                    .expect("cache capacity"),
            )),
            persisted_attr_id,
            persisted_value_id,
        };
        instance.rebuild_indexes_if_needed()?;
        instance.load_records_into_store()?;
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
            }
        }

        for id in self.persisted_value_id..next_value {
            let value_id = crate::model::ValueId(id);
            if let Some(value) = interner.get_value(value_id) {
                let key = encode_interner_key(b'v', id);
                batch.put_cf(interner_cf, key, value.as_bytes());
            }
        }

        self.persisted_attr_id = next_attr;
        self.persisted_value_id = next_value;
        batch.put_cf(
            self.db.cf_handle(CF_METADATA).ok_or_else(|| anyhow!("missing metadata column family"))?,
            KEY_NEXT_ATTR_ID,
            bincode::serialize(&next_attr)?,
        );
        batch.put_cf(
            self.db.cf_handle(CF_METADATA).ok_or_else(|| anyhow!("missing metadata column family"))?,
            KEY_NEXT_VALUE_ID,
            bincode::serialize(&next_value)?,
        );
        Ok(())
    }

    pub fn persist_metadata(&self, batch: &mut WriteBatch) -> Result<()> {
        let metadata_cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        let bytes = bincode::serialize(&self.inner.next_record_id())?;
        batch.put_cf(metadata_cf, KEY_NEXT_RECORD_ID, bytes);
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
        remove_metadata_key(&self.db, KEY_NEXT_RECORD_ID)?;
        remove_metadata_key(&self.db, KEY_INDEX_VERSION)?;
        self.inner = Store::new();
        self.persisted_attr_id = 0;
        self.persisted_value_id = 0;
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
}

impl RecordStore for PersistentStore {
    fn add_record(&mut self, record: Record) -> Result<RecordId> {
        let record_id = self.inner.add_record(record)?;
        if let Some(stored) = self.inner.get_record(record_id) {
            let mut batch = WriteBatch::default();
            let records_cf = self
                .db
                .cf_handle(CF_RECORDS)
                .ok_or_else(|| anyhow!("missing records column family"))?;
            let key = record_id.0.to_be_bytes();
            let bytes = bincode::serialize(&stored)?;
            batch.put_cf(records_cf, key, bytes);
            self.index_record_with_batch(&stored, &mut batch)?;
            self.persist_interner(&mut batch)?;
            self.persist_metadata(&mut batch)?;
            self.db.write(batch)?;
            if let Ok(mut cache) = self.cache.lock() {
                cache.put(record_id, stored);
            }
        }
        Ok(record_id)
    }

    fn add_records(&mut self, records: Vec<Record>) -> Result<()> {
        for record in records {
            self.add_record(record)?;
        }
        Ok(())
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
            .filter_map(|entry| entry.ok().and_then(|(_, value)| bincode::deserialize(&value).ok()))
            .collect()
    }

    fn for_each_record(&self, f: &mut dyn FnMut(Record)) {
        let records_cf = match self.db.cf_handle(CF_RECORDS) {
            Some(cf) => cf,
            None => return,
        };
        for entry in self.db.iterator_cf(records_cf, IteratorMode::Start) {
            if let Ok((_key, value)) = entry {
                if let Ok(record) = bincode::deserialize(&value) {
                    f(record);
                }
            }
        }
    }

    fn get_records_by_entity_type(&self, entity_type: &str) -> Vec<Record> {
        let cf = match self.db.cf_handle(CF_INDEX_ENTITY_TYPE) {
            Some(cf) => cf,
            None => return Vec::new(),
        };
        let prefix = encode_string_prefix(entity_type);
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(&prefix, Direction::Forward),
        );
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
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(&prefix, Direction::Forward),
        );
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
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(&prefix, Direction::Forward),
        );
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
            let iter = self.db.iterator_cf(
                cf,
                IteratorMode::From(&prefix, Direction::Forward),
            );
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
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(&prefix, Direction::Forward),
        );
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

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        if let Ok(Some(count)) = load_metadata::<u32>(&self.db, KEY_NEXT_RECORD_ID) {
            return count == 0;
        }
        self.inner.is_empty()
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
        let mut iter = self.db.iterator_cf(
            records_cf,
            IteratorMode::From(&start_key, Direction::Forward),
        );
        while let Some(entry) = iter.next() {
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
    let max_write_buffers =
        env_i32(ENV_MAX_WRITE_BUFFERS, DEFAULT_MAX_WRITE_BUFFERS).max(1);
    let bloom_bits_per_key = env_f64(ENV_BLOOM_BITS_PER_KEY, DEFAULT_BLOOM_BITS_PER_KEY);
    let memtable_prefix_bloom_ratio =
        env_f64(ENV_MEMTABLE_PREFIX_BLOOM_RATIO, DEFAULT_MEMTABLE_PREFIX_BLOOM_RATIO);
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
        options.set_bytes_per_sync(1 * 1024 * 1024);
        options.set_wal_bytes_per_sync(1 * 1024 * 1024);
    }
    options
}

fn build_block_options(cache: &Cache, bloom_bits_per_key: f64, with_filter: bool) -> BlockBasedOptions {
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
        ColumnFamilyDescriptor::new(CF_RECORDS, build_cf_options(&base, &data_block_opts, None, None)),
        ColumnFamilyDescriptor::new(CF_METADATA, build_cf_options(&base, &data_block_opts, None, None)),
        ColumnFamilyDescriptor::new(CF_INTERNER, build_cf_options(&base, &data_block_opts, None, None)),
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
    ];
    Ok(DB::open_cf_descriptors(&base, path, cfs)?)
}

fn encode_attr_value_index(
    attr: u32,
    value: u32,
    start: i64,
    end: i64,
    record_id: u32,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + 4 + 8 + 8 + 4);
    key.extend_from_slice(&attr.to_be_bytes());
    key.extend_from_slice(&value.to_be_bytes());
    key.extend_from_slice(&start.to_be_bytes());
    key.extend_from_slice(&end.to_be_bytes());
    key.extend_from_slice(&record_id.to_be_bytes());
    key
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

    fn load_records_into_store(&mut self) -> Result<()> {
        let records_cf = self
            .db
            .cf_handle(CF_RECORDS)
            .ok_or_else(|| anyhow!("missing records column family"))?;
        for entry in self.db.iterator_cf(records_cf, IteratorMode::Start) {
            let (_key, value) = entry?;
            let record: Record = bincode::deserialize(&value)?;
            self.inner.insert_record(record)?;
        }
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
    if let (Some(next_attr), Some(next_value)) = (
        load_metadata::<u32>(db, KEY_NEXT_ATTR_ID)?,
        load_metadata::<u32>(db, KEY_NEXT_VALUE_ID)?,
    ) {
        let interner_cf = db
            .cf_handle(CF_INTERNER)
            .ok_or_else(|| anyhow!("missing interner column family"))?;
        for id in 0..next_attr {
            let key = encode_interner_key(b'a', id);
            if let Some(value) = db.get_cf(interner_cf, key)? {
                let attr = String::from_utf8(value.to_vec())?;
                interner.insert_attr_with_id(crate::model::AttrId(id), attr);
            }
        }
        for id in 0..next_value {
            let key = encode_interner_key(b'v', id);
            if let Some(value) = db.get_cf(interner_cf, key)? {
                let val = String::from_utf8(value.to_vec())?;
                interner.insert_value_with_id(crate::model::ValueId(id), val);
            }
        }
        return Ok((interner, next_attr, next_value));
    }

    let interner_cf = db
        .cf_handle(CF_INTERNER)
        .ok_or_else(|| anyhow!("missing interner column family"))?;
    if let Some(bytes) = db.get_cf(interner_cf, KEY_INTERNER)? {
        let interner: StringInterner = bincode::deserialize(&bytes)?;
        let next_attr = interner.next_attr_id();
        let next_value = interner.next_value_id();
        save_metadata(db, KEY_NEXT_ATTR_ID, next_attr)?;
        save_metadata(db, KEY_NEXT_VALUE_ID, next_value)?;
        for id in 0..next_attr {
            if let Some(attr) = interner.get_attr(crate::model::AttrId(id)) {
                let key = encode_interner_key(b'a', id);
                db.put_cf(interner_cf, key, attr.as_bytes())?;
            }
        }
        for id in 0..next_value {
            if let Some(value) = interner.get_value(crate::model::ValueId(id)) {
                let key = encode_interner_key(b'v', id);
                db.put_cf(interner_cf, key, value.as_bytes())?;
            }
        }
        return Ok((interner, next_attr, next_value));
    }

    Ok((interner, 0, 0))
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

        let record_id = record.id.0;
        let entity_key = encode_string_index(&record.identity.entity_type, record_id);
        batch.put_cf(entity_type_cf, entity_key, []);
        let perspective_key = encode_string_index(&record.identity.perspective, record_id);
        batch.put_cf(perspective_cf, perspective_key, []);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Descriptor, RecordIdentity};
    use crate::ontology::{IdentityKey, Ontology, StrongIdentifier};
    use crate::temporal::Interval;
    use crate::distributed::{DistributedOntologyConfig, IdentityKeyConfig};
    use crate::linker::build_clusters;
    use crate::query::{query_master_entities, QueryDescriptor, QueryOutcome};
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

        let mut unirust = Unirust::with_store_and_tuning(
            ontology,
            store,
            StreamingTuning::default(),
        );

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

        let unirust = Unirust::with_store_and_tuning(
            ontology,
            store,
            StreamingTuning::default(),
        );
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
        let email_value = store
            .interner_mut()
            .intern_value("alice@example.com");

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
        let email_value = store
            .interner_mut()
            .intern_value("alice@example.com");
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
