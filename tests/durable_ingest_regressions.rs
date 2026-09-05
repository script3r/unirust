use tempfile::tempdir;
use unirust_rs::model::{AttrId, ClusterId, StringInterner};
use unirust_rs::ontology::IdentityKey;
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, Record, RecordId, RecordIdentity, RecordStore,
    StreamingTuning, Unirust,
};

fn ontology() -> Ontology {
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email"));
    ontology
}

fn record(engine: &mut Unirust, source: &str, value: &str) -> Record {
    Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "crm".into(), source.into()),
        vec![Descriptor::new(
            engine.intern_attr("email"),
            engine.intern_value(value),
            Interval::new(0, 10).unwrap(),
        )],
    )
}

fn ingest_variant(
    engine: &mut Unirust,
    records: Vec<Record>,
    variant: usize,
) -> anyhow::Result<()> {
    match variant {
        0 => engine.ingest(records).map(|_| ()),
        1 => engine.stream_records(records).map(|_| ()),
        2 => engine.stream_records_with_conflicts(records).map(|_| ()),
        3 => engine.stream_records_update_graph(records).map(|_| ()),
        _ => unreachable!(),
    }
}

/// Commit records and one assignment, then inject an error at the next durable
/// boundary. This exercises recovery from a partially written ingest result.
struct FailAfterRecordCommit {
    inner: PersistentStore,
    fail_assignment: bool,
}

impl RecordStore for FailAfterRecordCommit {
    fn add_record(&mut self, record: Record) -> anyhow::Result<RecordId> {
        self.inner.add_record(record)
    }
    fn stage_record_if_absent(&mut self, record: Record) -> anyhow::Result<(RecordId, bool)> {
        self.inner.stage_record_if_absent(record)
    }
    fn flush_staged_records(&mut self) -> anyhow::Result<usize> {
        self.inner.flush_staged_records()
    }
    fn discard_staged_records(&mut self) -> anyhow::Result<()> {
        self.inner.discard_staged_records()
    }
    fn get_record(&self, id: RecordId) -> Option<Record> {
        self.inner.get_record(id)
    }
    fn get_record_ref(&self, id: RecordId) -> Option<&Record> {
        self.inner.get_record_ref(id)
    }
    fn get_record_id_by_identity(&self, identity: &RecordIdentity) -> Option<RecordId> {
        self.inner.get_record_id_by_identity(identity)
    }
    fn get_all_records(&self) -> Vec<Record> {
        self.inner.get_all_records()
    }
    fn get_records_by_entity_type(&self, entity_type: &str) -> Vec<Record> {
        self.inner.get_records_by_entity_type(entity_type)
    }
    fn get_records_by_perspective(&self, perspective: &str) -> Vec<Record> {
        self.inner.get_records_by_perspective(perspective)
    }
    fn get_records_with_attribute(&self, attr: AttrId) -> Vec<Record> {
        self.inner.get_records_with_attribute(attr)
    }
    fn get_records_in_interval(&self, interval: Interval) -> Vec<Record> {
        self.inner.get_records_in_interval(interval)
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
    fn records_in_id_range(
        &self,
        start: RecordId,
        end: RecordId,
        max_results: usize,
    ) -> Vec<Record> {
        self.inner.records_in_id_range(start, end, max_results)
    }
    fn record_id_bounds(&self) -> Option<(RecordId, RecordId)> {
        self.inner.record_id_bounds()
    }
    fn shared_db(&self) -> Option<std::sync::Arc<rocksdb::DB>> {
        self.inner.shared_db()
    }
    fn sync(&self) -> anyhow::Result<()> {
        self.inner.sync()
    }
    fn set_cluster_count(&mut self, count: usize) -> anyhow::Result<()> {
        self.inner.set_cluster_count(count)
    }
    fn set_cluster_assignments_batch(
        &mut self,
        assignments: &[(RecordId, ClusterId)],
    ) -> anyhow::Result<()> {
        if self.fail_assignment {
            self.fail_assignment = false;
            let (record_id, cluster_id) = assignments[0];
            self.inner.set_cluster_assignment(record_id, cluster_id)?;
            self.inner.sync()?;
            anyhow::bail!("injected error after durable record commit");
        }
        self.inner.set_cluster_assignments_batch(assignments)
    }
}

#[test]
fn partial_assignment_write_recovers_all_committed_records() -> anyhow::Result<()> {
    for retry_before_reopen in [false, true] {
        let dir = tempdir()?;
        let tuning = StreamingTuning {
            use_persistent_dsu: true,
            use_tiered_index: true,
            ..StreamingTuning::default()
        };
        let store = FailAfterRecordCommit {
            inner: PersistentStore::open(dir.path())?,
            fail_assignment: true,
        };
        let mut engine = Unirust::with_store_and_tuning(ontology(), store, tuning.clone());
        let first = record(&mut engine, "first", "shared@example.com");
        let second = record(&mut engine, "second", "shared@example.com");
        let error = engine.ingest(vec![first.clone(), second]).unwrap_err();
        assert!(error
            .to_string()
            .contains("injected error after durable record commit"));
        assert_eq!(engine.record_count(), 2);
        if retry_before_reopen {
            let result = engine.ingest(vec![first])?;
            assert_eq!(result.cluster_count, 1);
            assert_eq!(engine.record_count(), 2);
        }
        drop(engine);
        let store = PersistentStore::open(dir.path())?;
        let mut recovered = Unirust::with_store_and_tuning(ontology(), store, tuning);
        recovered.initialize_streaming()?;
        assert_eq!(recovered.record_count(), 2);
        let clusters = recovered.clusters()?;
        assert_eq!(clusters.len(), 1);
        for stored in recovered.store().get_all_records() {
            assert_eq!(clusters.get_clusters_for_record(stored.id).len(), 1);
        }
    }
    Ok(())
}

#[test]
fn failed_batches_discard_pending_records_and_partial_resolution() -> anyhow::Result<()> {
    for persistent_dsu in [false, true] {
        for variant in 0..4 {
            let dir = tempdir()?;
            let tuning = StreamingTuning {
                use_persistent_dsu: persistent_dsu,
                use_tiered_index: persistent_dsu,
                ..StreamingTuning::default()
            };
            let store = PersistentStore::open(dir.path())?;
            let mut engine = Unirust::with_store_and_tuning(ontology(), store, tuning.clone());
            let original = record(&mut engine, "original", "original@example.com");
            ingest_variant(&mut engine, vec![original], variant)?;
            let pending = record(&mut engine, "pending", "shared@example.com");
            let pending_identity = pending.identity.clone();
            let changed = record(&mut engine, "original", "changed@example.com");
            let error = ingest_variant(&mut engine, vec![pending.clone(), changed], variant)
                .expect_err("changed immutable source identity must fail");
            assert!(error.to_string().contains("different payload"));
            assert!(engine.get_record_by_identity(&pending_identity)?.is_none());
            assert_eq!(engine.record_count(), 1);

            let next = record(&mut engine, "next", "shared@example.com");
            ingest_variant(&mut engine, vec![next], variant)?;
            assert!(engine.get_record_by_identity(&pending_identity)?.is_none());
            ingest_variant(&mut engine, vec![pending], variant)?;
            assert_eq!(engine.record_count(), 3);
            let clusters = engine.clusters()?;
            assert_eq!(
                clusters.len(),
                2,
                "retried record must resolve with the shared email"
            );
            for stored in engine.store().get_all_records() {
                assert_eq!(clusters.get_clusters_for_record(stored.id).len(), 1);
            }
            drop(engine);

            let store = PersistentStore::open(dir.path())?;
            let mut recovered = Unirust::with_store_and_tuning(ontology(), store, tuning);
            recovered.initialize_streaming()?;
            assert_eq!(recovered.record_count(), 3);
            assert_eq!(recovered.clusters()?.len(), 2);
        }
    }
    Ok(())
}

#[test]
fn failed_first_batch_discards_already_flushed_dsu_nodes() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let tuning = StreamingTuning {
        use_persistent_dsu: true,
        use_tiered_index: true,
        dsu_config: Some(unirust_rs::advanced::PersistentDSUConfig {
            dirty_buffer_size: 1,
            ..Default::default()
        }),
        ..StreamingTuning::default()
    };
    let store = PersistentStore::open(dir.path())?;
    let mut engine = Unirust::with_store_and_tuning(ontology(), store, tuning);
    let first = record(&mut engine, "first", "first@example.com");
    let changed = record(&mut engine, "first", "changed@example.com");
    assert!(engine
        .stream_records_update_graph(vec![first, changed])
        .is_err());
    assert_eq!(engine.record_count(), 0);
    let next = record(&mut engine, "next", "next@example.com");
    let result = engine.ingest(vec![next])?;
    assert_eq!(result.cluster_count, 1);
    assert_eq!(engine.clusters()?.len(), 1);
    Ok(())
}

#[test]
fn duplicate_explicit_id_does_not_overwrite_durable_record() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = PersistentStore::open(dir.path())?;
    let mut engine = Unirust::with_store(ontology(), store);
    let mut original = record(&mut engine, "original", "original@example.com");
    original.id = RecordId(42);
    let original_identity = original.identity.clone();
    engine.ingest(vec![original])?;
    let mut replacement = record(&mut engine, "replacement", "replacement@example.com");
    replacement.id = RecordId(42);
    let replacement_identity = replacement.identity.clone();
    for variant in 0..4 {
        let error = ingest_variant(&mut engine, vec![replacement.clone()], variant)
            .expect_err("an occupied record ID must be rejected");
        assert!(error.to_string().contains("already exists"));
    }
    drop(engine);
    let engine = Unirust::with_store(ontology(), PersistentStore::open(dir.path())?);
    let recovered = engine.get_record_by_identity(&original_identity)?.unwrap();
    assert_eq!(recovered.identity, original_identity);
    assert!(engine
        .get_record_by_identity(&replacement_identity)?
        .is_none());
    assert_eq!(engine.record_count(), 1);
    Ok(())
}

#[test]
fn persistent_write_apis_reject_occupied_and_repeated_explicit_ids() -> anyhow::Result<()> {
    for variant in 0..3 {
        let dir = tempdir()?;
        let mut store = PersistentStore::open(dir.path())?;
        let original = Record::new(
            RecordId(42),
            RecordIdentity::new("person".into(), "crm".into(), "original".into()),
            vec![],
        );
        store.add_record(original.clone())?;
        let replacement = Record::new(
            RecordId(42),
            RecordIdentity::new("person".into(), "crm".into(), "replacement".into()),
            vec![],
        );
        let result = match variant {
            0 => store.add_record(replacement).map(|_| ()),
            1 => store.add_records(vec![replacement]),
            2 => store.add_records_if_absent(vec![replacement]).map(|_| ()),
            _ => unreachable!(),
        };
        assert!(result.is_err());
        assert_eq!(
            store.get_record(RecordId(42)).unwrap().identity,
            original.identity
        );
        assert_eq!(store.len(), 1);

        let first = Record::new(
            RecordId(43),
            RecordIdentity::new("person".into(), "crm".into(), "first".into()),
            vec![],
        );
        let second = Record::new(
            RecordId(43),
            RecordIdentity::new("person".into(), "crm".into(), "second".into()),
            vec![],
        );
        let result = match variant {
            0 | 1 => store.add_records(vec![first, second]),
            2 => store.add_records_if_absent(vec![first, second]).map(|_| ()),
            _ => unreachable!(),
        };
        assert!(result.is_err());
        assert!(store.get_record(RecordId(43)).is_none());
        assert_eq!(store.len(), 1);
    }
    Ok(())
}

#[test]
fn bounded_interner_caches_preserve_durable_descriptors_and_resolution() -> anyhow::Result<()> {
    const CHILD: &str = "UNIRUST_DURABLE_INTERNER_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // Give cache configuration to an isolated child process: no test changes
        // environment variables while another thread can be reading them.
        let status = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "bounded_interner_caches_preserve_durable_descriptors_and_resolution",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("UNIRUST_INTERNER_CACHE_ATTRS", "0")
            .env("UNIRUST_INTERNER_CACHE_VALUES", "0")
            .status()?;
        assert!(status.success(), "bounded-cache child process failed");
        return Ok(());
    }
    let dir = tempdir()?;
    let descriptors;
    {
        let store = PersistentStore::open(dir.path())?;
        let mut engine = Unirust::with_store(ontology(), store);
        let original = record(&mut engine, "original", "known@example.com");
        descriptors = original.descriptors.clone();
        engine.ingest(vec![original])?;
    }
    let store = PersistentStore::open(dir.path())?;
    assert!(store.interner().get_attr_id("email").is_none());
    assert!(store.interner().get_value_id("known@example.com").is_none());
    assert_eq!(store.lookup_attr("email"), Some(descriptors[0].attr));
    assert_eq!(
        store.lookup_value("known@example.com"),
        Some(descriptors[0].value)
    );
    assert!(store.interner().get_attr_id("email").is_none());
    assert!(store.interner().get_value_id("known@example.com").is_none());
    let mut engine = Unirust::with_store(ontology(), store);
    // The record deliberately reuses durable IDs without going through string
    // interning, exercising record preparation as well as the lookup APIs.
    let reused = Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "crm".into(), "reused".into()),
        descriptors,
    );
    let assignments = engine.ingest(vec![reused])?.assignments;
    let named = record(&mut engine, "named", "known@example.com");
    engine.ingest(vec![named])?;
    let persisted = engine.get_record(assignments[0].record_id).unwrap();
    assert_eq!(
        engine
            .resolve_attr(persisted.descriptors[0].attr)
            .as_deref(),
        Some("email")
    );
    assert_eq!(
        engine
            .resolve_value(persisted.descriptors[0].value)
            .as_deref(),
        Some("known@example.com")
    );
    assert_eq!(engine.clusters()?.len(), 1);
    drop(engine);
    let store = PersistentStore::open(dir.path())?;
    let mut recovered = Unirust::with_store(ontology(), store);
    recovered.initialize_streaming()?;
    assert_eq!(recovered.record_count(), 3);
    assert_eq!(recovered.clusters()?.len(), 1);
    Ok(())
}

#[test]
fn ingest_batch_larger_than_record_cache_resolves_every_record() -> anyhow::Result<()> {
    const COUNT: usize = 100_001;
    let dir = tempdir()?;
    let store = PersistentStore::open(dir.path())?;
    let mut engine = Unirust::with_store(Ontology::new(), store);
    let records = (0..COUNT)
        .map(|index| {
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".into(), "crm".into(), format!("record-{index}")),
                vec![],
            )
        })
        .collect::<Vec<_>>();
    let first = records[0].clone();
    let assignments = engine.stream_records(records)?;
    assert_eq!(assignments.len(), COUNT);
    assert_eq!(engine.record_count(), COUNT);
    assert_eq!(engine.streaming_cluster_count(), Some(COUNT));
    let retry = engine.stream_records(vec![first])?;
    assert_eq!(retry[0].record_id, assignments[0].record_id);
    assert_eq!(retry[0].cluster_id, assignments[0].cluster_id);
    drop(engine);
    let store = PersistentStore::open(dir.path())?;
    assert_eq!(store.len(), COUNT);
    assert!(store.get_record(assignments[0].record_id).is_some());
    Ok(())
}
