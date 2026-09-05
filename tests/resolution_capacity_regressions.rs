use tempfile::tempdir;
use unirust_rs::dsu::DsuBackend;
use unirust_rs::index::{TierConfig, TieredIdentityKeyIndex};
use unirust_rs::linker::StreamingLinker;
use unirust_rs::model::KeyValue;
use unirust_rs::ontology::IdentityKey;
use unirust_rs::store::RecordStore;
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, Record, RecordId, RecordIdentity,
    StreamingTuning, Unirust,
};

#[test]
fn repeated_single_entity_never_becomes_hot_or_stops_linking() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = PersistentStore::open(dir.path())?;
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email"));
    let mut engine = Unirust::with_store_and_tuning(ontology, store, StreamingTuning::balanced());
    let attr = engine.intern_attr("email");
    let value = engine.intern_value("same@example.com");
    let mut latest = None;
    // A batch exercises production parallel linking. Each record has the exact same
    // key and interval, and there are no conflicting strong IDs.
    let records = (0..300)
        .map(|i| {
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".into(), "crm".into(), format!("r{i}")),
                vec![Descriptor::new(attr, value, Interval::new(0, 100).unwrap())],
            )
        })
        .collect();
    engine.stream_records(records)?;
    eprintln!(
        "after first batch: clusters={:?}, metrics={:?}",
        engine.streaming_cluster_count(),
        engine.linker_metrics_snapshot()
    );
    for i in 300..302 {
        latest = Some(engine.stream_records(vec![Record::new(
            RecordId(0),
            RecordIdentity::new("person".into(), "crm".into(), format!("r{i}")),
            vec![Descriptor::new(attr, value, Interval::new(0, 100)?)],
        )])?);
    }
    eprintln!(
        "after later batches: clusters={:?}, metrics={:?}, latest={latest:?}",
        engine.streaming_cluster_count(),
        engine.linker_metrics_snapshot()
    );
    assert_eq!(engine.streaming_cluster_count(), Some(1));
    assert_eq!(engine.linker_metrics_snapshot().hot_key_exits, 0);
    Ok(())
}

#[test]
fn memory_saver_merges_two_matching_perspectives() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = PersistentStore::open(dir.path())?;
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email"));
    let mut engine =
        Unirust::with_store_and_tuning(ontology, store, StreamingTuning::memory_saver());
    let attr = engine.intern_attr("email");
    let value = engine.intern_value("same@example.com");
    for perspective in ["crm", "billing"] {
        engine.stream_records(vec![Record::new(
            RecordId(0),
            RecordIdentity::new("person".into(), perspective.into(), "first".into()),
            vec![Descriptor::new(attr, value, Interval::new(0, 100)?)],
        )])?;
    }
    eprintln!(
        "memory_saver clusters={:?}, metrics={:?}",
        engine.streaming_cluster_count(),
        engine.linker_metrics_snapshot()
    );
    assert_eq!(engine.streaming_cluster_count(), Some(1));
    Ok(())
}

#[test]
fn cached_key_insertion_enforces_tier_capacity() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut store = PersistentStore::open(dir.path())?;
    let attr = store.intern_attr("email");
    let key = IdentityKey::new(vec![attr], "email".into());
    let config = TierConfig {
        hot_tier_capacity: 2,
        warm_tier_capacity: 20,
        hot_threshold: 1.1,
        tier_management_interval_secs: 0,
        ..TierConfig::default()
    };
    let mut cached_index =
        TieredIdentityKeyIndex::with_config(config.clone(), Some(store.db_shared()));
    let mut direct_index = TieredIdentityKeyIndex::with_config(config, Some(store.db_shared()));
    let mut ontology = Ontology::new();
    ontology.add_identity_key(key.clone());
    for i in 0..10 {
        let value = store.intern_value(&format!("person{i}@example.com"));
        let interval = Interval::new(0, 100)?;
        let id = store.add_record(Record::new(
            RecordId(0),
            RecordIdentity::new("person".into(), "crm".into(), format!("r{i}")),
            vec![Descriptor::new(attr, value, interval)],
        ))?;
        cached_index.add_record_with_cached_keys(
            id,
            id,
            "person",
            vec![(&key, vec![(vec![KeyValue::new(attr, value)], interval)])],
        )?;
        direct_index.add_record(&store.get_record(id).unwrap(), &ontology)?;
    }
    eprintln!(
        "cached insertion: {:?}, ordinary insertion: {:?}",
        cached_index.tier_stats(),
        direct_index.tier_stats()
    );
    assert_eq!(direct_index.tier_stats().hot_keys, 2);
    assert_eq!(cached_index.tier_stats().hot_keys, 2);
    Ok(())
}

#[test]
fn tier_eviction_and_repeated_updates_preserve_temporal_candidates_after_restart(
) -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut store = PersistentStore::open(dir.path())?;
    let attr = store.intern_attr("email");
    let key = IdentityKey::new(vec![attr], "email".into());
    let config = TierConfig {
        hot_tier_capacity: 1,
        warm_tier_capacity: 1,
        tier_management_interval_secs: 0,
        ..TierConfig::default()
    };
    let mut index = TieredIdentityKeyIndex::with_config(config.clone(), Some(store.db_shared()));
    let mut expected = Vec::new();
    // Revisit old keys after both hot and warm eviction. Each value has disjoint
    // temporal observations, so promotion must preserve the entire old bucket.
    for round in 0..3 {
        for n in 0..12 {
            let value = store.intern_value(&format!("person{n}@example.com"));
            let interval = Interval::new(round * 20, round * 20 + 10)?;
            let id = store.add_record(Record::new(
                RecordId(0),
                RecordIdentity::new("person".into(), "crm".into(), format!("{round}-{n}")),
                vec![Descriptor::new(attr, value, interval)],
            ))?;
            index.add_record_with_cached_keys(
                id,
                id,
                "person",
                vec![(&key, vec![(vec![KeyValue::new(attr, value)], interval)])],
            )?;
            expected.push((value, interval, id));
            assert!(index.tier_stats().hot_keys <= 1);
            assert!(index.tier_stats().warm_keys <= 1);
        }
    }
    index.flush_warm_to_cold()?;
    drop(index);
    drop(store);
    let store = PersistentStore::open(dir.path())?;
    let mut index = TieredIdentityKeyIndex::with_config(config, Some(store.db_shared()));
    let mut dsu = DsuBackend::in_memory();
    for (value, interval, id) in expected {
        let candidates = index.find_matching_clusters_overlapping(
            &mut dsu,
            "person",
            &[KeyValue::new(attr, value)],
            interval,
        )?;
        assert_eq!(candidates, &[(id, interval)]);
        let records = index.find_matching_records("person", &[KeyValue::new(attr, value)])?;
        assert_eq!(records.len(), 3);
    }
    Ok(())
}

#[test]
fn long_multi_source_history_retains_membership_and_guards_through_tiers_and_recovery(
) -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = PersistentStore::open(dir.path())?;
    let tuning = StreamingTuning {
        use_tiered_index: true,
        tier_config: Some(TierConfig {
            hot_tier_capacity: 2,
            warm_tier_capacity: 2,
            tier_management_interval_secs: 0,
            ..TierConfig::default()
        }),
        ..StreamingTuning::billion_scale()
    };
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email"));
    ontology.add_strong_identifier(unirust_rs::ontology::StrongIdentifier::from_name(
        "ssn", "ssn",
    ));
    let mut engine = Unirust::with_store_and_tuning(ontology.clone(), store, tuning.clone());
    let email = engine.intern_attr("email");
    let ssn = engine.intern_attr("ssn");
    let id_a = engine.intern_value("111");
    let id_b = engine.intern_value("222");
    for batch in 0..6 {
        let mut records = Vec::new();
        for offset in 0..500 {
            let n = batch * 500 + offset;
            let value = engine.intern_value(&format!("person{}@example.com", n % 10));
            records.push(Record::new(
                RecordId(0),
                RecordIdentity::new(
                    "person".into(),
                    if n % 20 < 10 { "crm" } else { "billing" }.into(),
                    format!("r{n}"),
                ),
                vec![
                    Descriptor::new(email, value, Interval::new(0, 100)?),
                    Descriptor::new(ssn, id_a, Interval::new(0, 100)?),
                ],
            ));
        }
        engine.stream_records(records)?;
        assert_eq!(engine.streaming_cluster_count(), Some(10));
    }
    let value = engine.intern_value("person0@example.com");
    engine.stream_records(vec![Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "crm".into(), "conflicting".into()),
        vec![
            Descriptor::new(email, value, Interval::new(0, 100)?),
            Descriptor::new(ssn, id_b, Interval::new(0, 100)?),
        ],
    )])?;
    assert_eq!(engine.streaming_cluster_count(), Some(11));
    drop(engine);
    let store = PersistentStore::open(dir.path())?;
    let mut engine = Unirust::with_store_and_tuning(ontology, store, tuning);
    let email = engine.intern_attr("email");
    let ssn = engine.intern_attr("ssn");
    let value = engine.intern_value("person0@example.com");
    let id_a = engine.intern_value("111");
    engine.stream_records(vec![Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "crm".into(), "after-restart".into()),
        vec![
            Descriptor::new(email, value, Interval::new(0, 100)?),
            Descriptor::new(ssn, id_a, Interval::new(0, 100)?),
        ],
    )])?;
    assert_eq!(engine.streaming_cluster_count(), Some(11));
    let clusters = engine.clusters()?;
    assert_eq!(clusters.clusters.len(), 11);
    assert_eq!(
        clusters
            .clusters
            .iter()
            .map(|cluster| cluster.records.len())
            .sum::<usize>(),
        3002
    );
    Ok(())
}

#[test]
fn deferred_merge_updates_readonly_membership_and_global_ids() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut store = PersistentStore::open(dir.path())?;
    let attr = store.intern_attr("email");
    let value = store.intern_value("same@example.com");
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::new(vec![attr], "email".into()));
    let tuning = StreamingTuning {
        candidate_cap: 1,
        adaptive_candidate_cap: false,
        stochastic_sampling: false,
        enable_boundary_tracking: true,
        ..StreamingTuning::balanced()
    };
    let mut linker = StreamingLinker::new(&store, &ontology, &tuning)?;
    let mut ids = Vec::new();
    for (i, (start, end)) in [(0, 10), (20, 30), (0, 30)].into_iter().enumerate() {
        let id = store.add_record(Record::new(
            RecordId(0),
            RecordIdentity::new("person".into(), "crm".into(), format!("r{i}")),
            vec![Descriptor::new(attr, value, Interval::new(start, end)?)],
        ))?;
        linker.link_record(&store, &ontology, id)?;
        linker.global_cluster_id_for(id);
        ids.push(id);
    }
    assert_eq!(linker.cluster_count(), 3);
    linker.reconcile_pending(&store, &ontology)?;
    assert_eq!(linker.cluster_count(), 1);
    for id in &ids {
        let cluster = linker.cluster_for_record(*id).unwrap();
        let mut actual = cluster.records;
        actual.sort_by_key(|id| id.0);
        assert_eq!(actual, ids);
        assert_eq!(Some(cluster.id), Some(linker.cluster_id_for(*id)));
        assert_eq!(
            linker.global_cluster_id_for_readonly(*id),
            linker.global_cluster_id_for_readonly(ids[0])
        );
    }
    Ok(())
}

#[test]
fn corrupt_cold_bucket_returns_error_instead_of_missing_candidates() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut store = PersistentStore::open(dir.path())?;
    let attr = store.intern_attr("email");
    let value = store.intern_value("first@example.com");
    let key = IdentityKey::new(vec![attr], "email".into());
    let config = TierConfig {
        hot_tier_capacity: 0,
        warm_tier_capacity: 1,
        ..TierConfig::default()
    };
    let mut index = TieredIdentityKeyIndex::with_config(config, Some(store.db_shared()));
    for (i, value) in [value, store.intern_value("second@example.com")]
        .into_iter()
        .enumerate()
    {
        let interval = Interval::new(0, 100)?;
        let id = store.add_record(Record::new(
            RecordId(0),
            RecordIdentity::new("person".into(), "crm".into(), format!("r{i}")),
            vec![Descriptor::new(attr, value, interval)],
        ))?;
        index.add_record_with_cached_keys(
            id,
            id,
            "person",
            vec![(&key, vec![(vec![KeyValue::new(attr, value)], interval)])],
        )?;
    }
    let encoded = unirust_rs::persistence::index_encoding::encode_identity_key(
        "person",
        &[KeyValue::new(attr, value)],
    );
    let cf = store
        .db()
        .cf_handle(unirust_rs::persistence::index_cf::IDENTITY_KEYS)
        .unwrap();
    store.db().put_cf(cf, encoded, [0xff])?;
    let mut dsu = DsuBackend::in_memory();
    assert!(index
        .find_matching_clusters_overlapping(
            &mut dsu,
            "person",
            &[KeyValue::new(attr, value)],
            Interval::new(0, 100)?
        )
        .is_err());
    assert!(index
        .find_matching_records("person", &[KeyValue::new(attr, value)])
        .is_err());
    Ok(())
}
