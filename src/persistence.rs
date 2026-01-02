use crate::model::{Record, RecordId, StringInterner};
use crate::store::{RecordStore, Store};
use anyhow::{anyhow, Result};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use std::path::Path;

const CF_RECORDS: &str = "records";
const CF_METADATA: &str = "metadata";
const CF_INTERNER: &str = "interner";

const KEY_NEXT_RECORD_ID: &[u8] = b"next_record_id";
const KEY_INTERNER: &[u8] = b"interner";
const KEY_ONTOLOGY_CONFIG: &[u8] = b"ontology_config";

pub struct PersistentStore {
    inner: Store,
    db: DB,
}

impl PersistentStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = open_db(path)?;

        let interner = load_interner(&db)?;
        let mut store = Store::with_interner(interner, 0);
        if let Some(next_id) = load_metadata::<u32>(&db, KEY_NEXT_RECORD_ID)? {
            store.set_next_record_id(next_id);
        }

        let records_cf = db
            .cf_handle(CF_RECORDS)
            .ok_or_else(|| anyhow!("missing records column family"))?;
        for entry in db.iterator_cf(records_cf, IteratorMode::Start) {
            let (_key, value) = entry?;
            let record: Record = bincode::deserialize(&value)?;
            store.insert_record(record)?;
        }

        Ok(Self { inner: store, db })
    }

    pub fn inner(&self) -> &Store {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut Store {
        &mut self.inner
    }

    pub fn persist_interner(&self) -> Result<()> {
        let interner_cf = self
            .db
            .cf_handle(CF_INTERNER)
            .ok_or_else(|| anyhow!("missing interner column family"))?;
        let bytes = bincode::serialize(self.inner.interner())?;
        self.db.put_cf(interner_cf, KEY_INTERNER, bytes)?;
        Ok(())
    }

    pub fn persist_metadata(&self) -> Result<()> {
        let metadata_cf = self
            .db
            .cf_handle(CF_METADATA)
            .ok_or_else(|| anyhow!("missing metadata column family"))?;
        let bytes = bincode::serialize(&self.inner.next_record_id())?;
        self.db.put_cf(metadata_cf, KEY_NEXT_RECORD_ID, bytes)?;
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
        remove_metadata_key(&self.db, KEY_NEXT_RECORD_ID)?;
        self.inner = Store::new();
        self.persist_interner()?;
        self.persist_metadata()?;
        Ok(())
    }
}

impl RecordStore for PersistentStore {
    fn add_record(&mut self, record: Record) -> Result<RecordId> {
        let record_id = self.inner.add_record(record)?;
        if let Some(stored) = self.inner.get_record(record_id) {
            self.persist_record(stored)?;
        }
        self.persist_interner()?;
        self.persist_metadata()?;
        Ok(record_id)
    }

    fn add_records(&mut self, records: Vec<Record>) -> Result<()> {
        for record in records {
            self.add_record(record)?;
        }
        Ok(())
    }

    fn get_record(&self, id: RecordId) -> Option<&Record> {
        self.inner.get_record(id)
    }

    fn get_all_records(&self) -> Vec<&Record> {
        self.inner.get_all_records()
    }

    fn get_records_by_entity_type(&self, entity_type: &str) -> Vec<&Record> {
        self.inner.get_records_by_entity_type(entity_type)
    }

    fn get_records_by_perspective(&self, perspective: &str) -> Vec<&Record> {
        self.inner.get_records_by_perspective(perspective)
    }

    fn get_records_with_attribute(&self, attr: crate::model::AttrId) -> Vec<&Record> {
        self.inner.get_records_with_attribute(attr)
    }

    fn get_records_in_interval(&self, interval: crate::temporal::Interval) -> Vec<&Record> {
        self.inner.get_records_in_interval(interval)
    }

    fn get_records_with_value_in_interval(
        &self,
        attr: crate::model::AttrId,
        value: crate::model::ValueId,
        interval: crate::temporal::Interval,
    ) -> Vec<(RecordId, crate::temporal::Interval)> {
        self.inner
            .get_records_with_value_in_interval(attr, value, interval)
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
        self.inner.is_empty()
    }
}

fn open_db(path: impl AsRef<Path>) -> Result<DB> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let cfs = vec![
        ColumnFamilyDescriptor::new(CF_RECORDS, Options::default()),
        ColumnFamilyDescriptor::new(CF_METADATA, Options::default()),
        ColumnFamilyDescriptor::new(CF_INTERNER, Options::default()),
    ];
    Ok(DB::open_cf_descriptors(&options, path, cfs)?)
}

fn load_interner(db: &DB) -> Result<StringInterner> {
    let interner_cf = db
        .cf_handle(CF_INTERNER)
        .ok_or_else(|| anyhow!("missing interner column family"))?;
    if let Some(bytes) = db.get_cf(interner_cf, KEY_INTERNER)? {
        let interner: StringInterner = bincode::deserialize(&bytes)?;
        Ok(interner)
    } else {
        Ok(StringInterner::new())
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
    use crate::temporal::Interval;
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
}
