use tempfile::tempdir;
use unirust_rs::advanced::GlobalClusterId;
use unirust_rs::ontology::IdentityKey;
use unirust_rs::persistence::{dsu_cf, index_cf, linker_cf, linker_encoding};
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, QueryOutcome, Record, RecordId,
    RecordIdentity, RecordStore, StreamingTuning, Unirust,
};

fn ontology() -> Ontology {
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email"));
    ontology.add_identity_key(IdentityKey::from_names(vec!["phone"], "phone"));
    ontology
}

fn record(engine: &mut Unirust, uid: &str, values: &[(&str, &str)], interval: Interval) -> Record {
    Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "crm".into(), uid.into()),
        values
            .iter()
            .map(|(attr, value)| {
                Descriptor::new(
                    engine.intern_attr(attr),
                    engine.intern_value(value),
                    interval,
                )
            })
            .collect(),
    )
}

#[test]
fn selective_queries_follow_bridge_merges_and_match_recovery() -> anyhow::Result<()> {
    for persistent_dsu in [false, true] {
        let dir = tempdir()?;
        let tuning = StreamingTuning {
            use_persistent_dsu: persistent_dsu,
            use_tiered_index: persistent_dsu,
            ..StreamingTuning::default()
        };
        let interval = Interval::new(0, 100)?;
        let mut engine = Unirust::with_store_and_tuning(
            ontology(),
            PersistentStore::open(dir.path())?,
            tuning.clone(),
        );
        let records = vec![
            record(
                &mut engine,
                "email",
                &[("email", "alice@example.com"), ("name", "Alice")],
                interval,
            ),
            record(
                &mut engine,
                "phone",
                &[("phone", "123"), ("city", "London")],
                interval,
            ),
        ];
        engine.stream_records(records)?;
        let query = [
            engine.lookup_query_descriptor("name", "Alice").unwrap(),
            engine.lookup_query_descriptor("city", "London").unwrap(),
        ];
        assert_eq!(
            engine.query(&query, interval)?,
            QueryOutcome::Matches(Vec::new())
        );
        let bridge = record(
            &mut engine,
            "bridge",
            &[("email", "alice@example.com"), ("phone", "123")],
            interval,
        );
        engine.stream_records(vec![bridge])?;
        let result = engine.query(&query, interval)?;
        let QueryOutcome::Matches(matches) = &result else {
            panic!("unexpected conflict: {result:?}")
        };
        assert_eq!(matches.len(), 1);
        assert!(matches[0]
            .golden
            .iter()
            .any(|d| d.attr == "city" && d.value == "London"));
        let unrelated = record(
            &mut engine,
            "unrelated",
            &[("email", "bob@example.com")],
            interval,
        );
        engine.stream_records(vec![unrelated])?;
        assert_eq!(engine.query(&query, interval)?, result);
        drop(engine);

        let mut recovered =
            Unirust::with_store_and_tuning(ontology(), PersistentStore::open(dir.path())?, tuning);
        assert_eq!(recovered.query(&query, interval)?, result);
        recovered.initialize_streaming()?;
        assert_eq!(recovered.query(&query, interval)?, result);
    }
    Ok(())
}

#[test]
fn selective_query_labels_agree_with_full_store_labels() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let interval = Interval::new(0, 100)?;
    let mut config = ontology();
    config.add_identity_key(IdentityKey::from_names(vec!["name", "email"], "name_email"));
    let mut engine = Unirust::with_store(config.clone(), PersistentStore::open(dir.path())?);
    // These different identity values generate the same short human-readable token.
    let records = vec![
        record(
            &mut engine,
            "one",
            &[("name", "Alice"), ("email", "one@example.com")],
            interval,
        ),
        record(
            &mut engine,
            "two",
            &[("name", "Alice"), ("email", "two@example.com")],
            interval,
        ),
    ];
    engine.stream_records(records)?;
    let query = [engine
        .lookup_query_descriptor("email", "one@example.com")
        .unwrap()];
    let result = engine.query(&query, interval)?;
    drop(engine);
    let recovered = Unirust::with_store(config, PersistentStore::open(dir.path())?);
    assert_eq!(recovered.query(&query, interval)?, result);
    Ok(())
}

#[test]
fn restored_cluster_ids_invalidate_cached_composite_labels() -> anyhow::Result<()> {
    for persistent_dsu in [false, true] {
        let dir = tempdir()?;
        let interval = Interval::new(0, 100)?;
        let mut config = Ontology::new();
        config.add_identity_key(IdentityKey::from_names(vec!["name", "email"], "name_email"));
        let tuning = StreamingTuning {
            use_persistent_dsu: persistent_dsu,
            use_tiered_index: persistent_dsu,
            ..StreamingTuning::default()
        };
        let mut engine = Unirust::with_store_and_tuning(
            config.clone(),
            PersistentStore::open(dir.path())?,
            tuning.clone(),
        );
        let mut first = record(
            &mut engine,
            "first",
            &[("name", "Alice"), ("email", "alice@example.com")],
            interval,
        );
        first.id = RecordId(100);
        let mut second = record(
            &mut engine,
            "second",
            &[("name", "Bob"), ("email", "bob@example.com")],
            interval,
        );
        second.id = RecordId(1);
        engine.stream_records(vec![first, second])?;
        let query = [engine
            .lookup_query_descriptor("email", "alice@example.com")
            .unwrap()];
        let expected = engine.query(&query, interval)?;
        engine.checkpoint_linker_state()?;
        drop(engine);

        let mut recovered =
            Unirust::with_store_and_tuning(config, PersistentStore::open(dir.path())?, tuning);
        recovered.initialize_streaming()?;
        // Replay visits ID 1 before ID 100, reversing allocation of local cluster
        // IDs. Warm the composite-label cache under those temporary assignments.
        let before_restore = recovered.query(&query, interval)?;
        let (QueryOutcome::Matches(before), QueryOutcome::Matches(original)) =
            (&before_restore, &expected)
        else {
            panic!("expected one matching entity before and after replay");
        };
        assert_eq!(before.len(), 1);
        assert_eq!(original.len(), 1);
        assert_ne!(before[0].cluster_id, original[0].cluster_id);
        assert_eq!(before[0].cluster_key, original[0].cluster_key);
        assert_eq!(recovered.restore_linker_state()?, 2);
        assert_eq!(recovered.query(&query, interval)?, expected);
    }
    Ok(())
}

#[test]
fn fragment_recovery_preserves_durable_derived_state_and_redirects() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let interval = Interval::new(0, 100)?;
    let tuning = StreamingTuning {
        shard_id: 1,
        use_persistent_dsu: true,
        use_tiered_index: true,
        ..StreamingTuning::default()
    };
    let canonical = GlobalClusterId::new(0, 20, 0);
    {
        let mut engine = Unirust::with_store_and_tuning(
            ontology(),
            PersistentStore::open(dir.path())?,
            tuning.clone(),
        );
        let source = record(
            &mut engine,
            "alice",
            &[("email", "alice@example.com")],
            interval,
        );
        engine.ingest(vec![source])?;
        engine.apply_cross_shard_merge(canonical, GlobalClusterId::new(1, 0, 0))?;
    }
    let store = PersistentStore::open(dir.path())?;
    let db = store.shared_db().unwrap();
    let parent_cf = db.cf_handle(dsu_cf::PARENT).unwrap();
    let index_cf = db.cf_handle(index_cf::IDENTITY_KEYS).unwrap();
    let sentinel_id = 777u32.to_be_bytes();
    db.put_cf(parent_cf, sentinel_id, sentinel_id)?;
    db.put_cf(index_cf, b"query-recovery-sentinel", b"preserve")?;
    let sequence = db.latest_sequence_number();
    let engine = Unirust::with_store_and_tuning(ontology(), store, tuning);
    let query = [engine
        .lookup_query_descriptor("email", "alice@example.com")
        .unwrap()];
    let candidates = engine.query_entity_fragments(&query, interval, &[], false)?;
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].global_id, canonical);
    let hydrated = engine.query_entity_fragments(&query, interval, &[canonical], false)?;
    assert_eq!(hydrated.len(), 1);
    assert_eq!(hydrated[0].global_id, canonical);
    assert!(hydrated[0]
        .golden
        .iter()
        .any(|descriptor| descriptor.value == "alice@example.com"));
    assert_eq!(db.get_cf(parent_cf, sentinel_id)?.unwrap(), sentinel_id);
    assert_eq!(
        db.get_cf(index_cf, b"query-recovery-sentinel")?.unwrap(),
        b"preserve"
    );
    assert_eq!(
        db.latest_sequence_number(),
        sequence,
        "fragment queries must not write RocksDB"
    );
    Ok(())
}

#[test]
fn fragment_queries_allow_fresh_and_reset_stores_without_writes() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = PersistentStore::open(dir.path())?;
    let db = store.shared_db().unwrap();
    let mut engine = Unirust::with_store(ontology(), store);
    let interval = Interval::new(0, 100)?;
    let sequence = db.latest_sequence_number();
    assert!(engine
        .query_entity_fragments(&[], interval, &[], false)?
        .is_empty());
    assert_eq!(db.latest_sequence_number(), sequence);

    engine.initialize_streaming()?;
    engine.reset_with_ontology(ontology())?;
    let sequence = db.latest_sequence_number();
    assert!(engine
        .query_entity_fragments(&[], interval, &[], false)?
        .is_empty());
    assert_eq!(db.latest_sequence_number(), sequence);
    Ok(())
}

#[test]
fn fragment_queries_reject_legacy_or_invalid_scheme_without_migration() -> anyhow::Result<()> {
    for corrupt_marker in [false, true] {
        let dir = tempdir()?;
        let store = PersistentStore::open(dir.path())?;
        let db = store.shared_db().unwrap();
        let metadata = db.cf_handle(linker_cf::METADATA).unwrap();
        let marker = linker_encoding::KEY_GLOBAL_CLUSTER_ID_SCHEME;
        if corrupt_marker {
            db.put_cf(metadata, marker, b"invalid")?;
        }
        let legacy_key =
            linker_encoding::encode_cross_shard_merge_key(GlobalClusterId::new(1, 3, 0));
        let legacy_value = linker_encoding::encode_global_cluster_id(GlobalClusterId::new(0, 2, 0));
        db.put_cf(metadata, &legacy_key, legacy_value)?;
        let sequence = db.latest_sequence_number();
        let engine = Unirust::with_store(ontology(), store);
        let error = engine
            .query_entity_fragments(&[], Interval::new(0, 100)?, &[], false)
            .unwrap_err();
        let expected = if corrupt_marker {
            "unsupported global cluster ID scheme"
        } else {
            "initialize_streaming is required"
        };
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(db.get_cf(metadata, legacy_key)?.unwrap(), legacy_value);
        assert_eq!(
            db.get_cf(metadata, marker)?,
            corrupt_marker.then(|| b"invalid".to_vec())
        );
        assert_eq!(db.latest_sequence_number(), sequence);
    }
    Ok(())
}
