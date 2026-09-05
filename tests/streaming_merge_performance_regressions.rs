use std::time::{Duration, Instant};
use tempfile::tempdir;
use unirust_rs::ontology::{IdentityKey, StrongIdentifier};
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, Record, RecordId, RecordIdentity,
    StreamingTuning, Unirust,
};

fn history_ontology() -> Ontology {
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email"));
    ontology.add_strong_identifier(StrongIdentifier::from_name("account", "account"));
    ontology
}

fn add_history(engine: &mut Unirust, order: &[usize]) -> anyhow::Result<Duration> {
    let email = engine.intern_attr("email");
    let account = engine.intern_attr("account");
    let email_value = engine.intern_value("history@example.com");
    let account_value = engine.intern_value("account-a");
    let records = order
        .iter()
        .map(|&n| {
            Ok(Record::new(
                RecordId(0),
                RecordIdentity::new("person".into(), "crm".into(), format!("history-{n}")),
                vec![
                    Descriptor::new(email, email_value, Interval::new(i64::MIN, i64::MAX)?),
                    Descriptor::new(
                        account,
                        account_value,
                        Interval::new(n as i64 * 4, n as i64 * 4 + 1)?,
                    ),
                ],
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let started = Instant::now();
    engine.stream_records(records)?;
    let elapsed = started.elapsed();
    assert_eq!(engine.streaming_cluster_count(), Some(1));
    assert_eq!(
        engine.linker_metrics_snapshot().records_linked,
        order.len() as u64
    );
    Ok(elapsed)
}

fn add_probe(
    engine: &mut Unirust,
    uid: &str,
    perspective: &str,
    account_value: &str,
    interval: Interval,
) -> anyhow::Result<()> {
    let email = engine.intern_attr("email");
    let account = engine.intern_attr("account");
    let email_value = engine.intern_value("history@example.com");
    let account_value = engine.intern_value(account_value);
    engine.stream_records(vec![Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), perspective.into(), uid.into()),
        vec![
            Descriptor::new(email, email_value, Interval::new(i64::MIN, i64::MAX)?),
            Descriptor::new(account, account_value, interval),
        ],
    )])?;
    Ok(())
}

#[test]
fn strong_id_history_preserves_gaps_conflicts_and_sources_after_recovery() -> anyhow::Result<()> {
    for order in [
        (0..192).collect::<Vec<_>>(),
        (0..192).rev().collect(),
        (0..96).flat_map(|n| [n, 191 - n]).collect(),
    ] {
        let dir = tempdir()?;
        let ontology = history_ontology();
        let tuning = StreamingTuning::billion_scale();
        let store = PersistentStore::open(dir.path())?;
        let mut engine = Unirust::with_store_and_tuning(ontology.clone(), store, tuning.clone());
        add_history(&mut engine, &order)?;
        drop(engine);

        let store = PersistentStore::open(dir.path())?;
        let mut engine = Unirust::with_store_and_tuning(ontology, store, tuning);
        engine.initialize_streaming()?;
        assert_eq!(engine.streaming_cluster_count(), Some(1));
        assert_eq!(engine.clusters()?.clusters[0].records.len(), order.len());
        // A different source may observe another strong ID during the same time.
        add_probe(
            &mut engine,
            "billing",
            "billing",
            "account-b",
            Interval::new(0, 1)?,
        )?;
        assert_eq!(engine.streaming_cluster_count(), Some(1));
        // Adjacent endpoints and gaps must not become fabricated observations.
        add_probe(&mut engine, "gap", "crm", "account-b", Interval::new(1, 4)?)?;
        assert_eq!(engine.streaming_cluster_count(), Some(1));
        add_probe(
            &mut engine,
            "conflict",
            "crm",
            "account-b",
            Interval::new(4, 5)?,
        )?;
        assert_eq!(engine.streaming_cluster_count(), Some(2));
    }
    Ok(())
}

#[test]
fn malformed_stored_strong_id_history_preserves_legacy_merges_after_recovery() -> anyhow::Result<()>
{
    let dir = tempdir()?;
    let ontology = history_ontology();
    let tuning = StreamingTuning::billion_scale();
    let store = PersistentStore::open(dir.path())?;
    let mut engine = Unirust::with_store_and_tuning(ontology.clone(), store, tuning.clone());
    let email = engine.intern_attr("email");
    let account = engine.intern_attr("account");
    let email_value = engine.intern_value("history@example.com");
    let account_value = engine.intern_value("account-a");
    engine.stream_records(vec![Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "crm".into(), "malformed-history".into()),
        vec![
            Descriptor::new(email, email_value, Interval::new(i64::MIN, i64::MAX)?),
            Descriptor::new(account, account_value, Interval::new(0, 100)?),
            Descriptor::new(
                account,
                account_value,
                Interval {
                    start: 200,
                    end: -10,
                },
            ),
        ],
    )])?;
    add_probe(
        &mut engine,
        "contained",
        "crm",
        "account-a",
        Interval::new(50, 60)?,
    )?;
    assert_eq!(engine.streaming_cluster_count(), Some(1));
    drop(engine);

    let store = PersistentStore::open(dir.path())?;
    let mut engine = Unirust::with_store_and_tuning(ontology, store, tuning);
    engine.initialize_streaming()?;
    assert_eq!(engine.streaming_cluster_count(), Some(1));
    add_probe(
        &mut engine,
        "recovered",
        "crm",
        "account-a",
        Interval::new(50, 60)?,
    )?;
    assert_eq!(engine.streaming_cluster_count(), Some(1));
    add_probe(
        &mut engine,
        "conflict",
        "crm",
        "account-b",
        Interval::new(50, 60)?,
    )?;
    assert_eq!(engine.streaming_cluster_count(), Some(2));
    Ok(())
}

#[test]
#[ignore = "diagnostic timing; run explicitly with --ignored --nocapture"]
fn persistent_strong_id_history_scaling() -> anyhow::Result<()> {
    let count = std::env::var("UNIRUST_AUDIT_HISTORY_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(12_000);
    for size in [count / 2, count] {
        let dir = tempdir()?;
        let store = PersistentStore::open(dir.path())?;
        let mut engine = Unirust::with_store_and_tuning(
            history_ontology(),
            store,
            StreamingTuning::billion_scale(),
        );
        engine.initialize_streaming()?;
        let order = (0..size).collect::<Vec<_>>();
        let elapsed = add_history(&mut engine, &order)?;
        eprintln!("persistent strong-ID history: records={size} elapsed={elapsed:?}");
    }
    Ok(())
}

fn duplicate_records(engine: &mut Unirust, count: usize, batch: usize) -> Vec<Record> {
    let email = engine.intern_attr("email");
    let account = engine.intern_attr("account");
    let account_value = engine.intern_value("account-a");
    (0..count)
        .map(|n| {
            let value = engine.intern_value(&format!("person{n}@example.com"));
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".into(), "crm".into(), format!("{batch}-{n}")),
                vec![
                    Descriptor::new(email, value, Interval::new(0, 100).unwrap()),
                    Descriptor::new(account, account_value, Interval::new(0, 100).unwrap()),
                ],
            )
        })
        .collect()
}

fn boundary_snapshot(engine: &Unirust) -> Vec<String> {
    let metadata = engine.export_boundary_index().unwrap().export_metadata();
    let mut snapshot = Vec::new();
    for (signature, entries) in metadata.entries {
        for entry in entries {
            let mut hashes = entry.perspective_strong_ids.into_iter().collect::<Vec<_>>();
            hashes.sort_unstable();
            snapshot.push(format!(
                "{signature:?}:{:?}:{:?}:{hashes:?}:{:?}",
                entry.cluster_id, entry.interval, entry.strong_ids
            ));
        }
    }
    snapshot.sort();
    snapshot
}

#[test]
fn duplicate_merges_preserve_unrelated_boundaries_and_bridge_redirects() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let ontology = history_ontology();
    let tuning = StreamingTuning::billion_scale().with_boundary_tracking(true);
    let store = PersistentStore::open(dir.path())?;
    let mut engine = Unirust::with_store_and_tuning(ontology.clone(), store, tuning.clone());
    let count = 256;
    let records = duplicate_records(&mut engine, count, 0);
    let original = engine.stream_records(records)?;
    let before = boundary_snapshot(&engine);
    let global_ids = original
        .iter()
        .map(|entry| engine.global_cluster_id_for_record(entry.record_id))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let records = duplicate_records(&mut engine, count, 1);
    let duplicates = engine.stream_records(records)?;
    assert_eq!(boundary_snapshot(&engine), before);
    assert_eq!(engine.streaming_cluster_count(), Some(count));
    assert!(engine
        .clusters()?
        .clusters
        .iter()
        .all(|cluster| cluster.records.len() == 2));
    assert_eq!(
        engine.linker_metrics_snapshot().records_linked,
        (count * 2) as u64
    );
    for (duplicate, expected) in duplicates.iter().zip(&global_ids) {
        assert_eq!(
            engine.global_cluster_id_for_record(duplicate.record_id)?,
            *expected
        );
    }

    // Merging two established IDs still needs the general redirect/boundary path.
    let email = engine.intern_attr("email");
    let value_a = engine.intern_value("person0@example.com");
    let value_b = engine.intern_value("person1@example.com");
    engine.stream_records(vec![Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "crm".into(), "bridge".into()),
        vec![
            Descriptor::new(email, value_a, Interval::new(0, 100)?),
            Descriptor::new(email, value_b, Interval::new(0, 100)?),
        ],
    )])?;
    assert_eq!(engine.streaming_cluster_count(), Some(count - 1));
    let canonical = engine.global_cluster_id_for_record(original[0].record_id)?;
    assert_eq!(
        engine.global_cluster_id_for_record(original[1].record_id)?,
        canonical
    );
    assert_eq!(
        engine.resolve_global_cluster_id(global_ids[1]),
        Some(canonical)
    );
    let after_bridge = boundary_snapshot(&engine);
    let mut expected_clusters = engine.clusters()?.clusters;
    for cluster in &mut expected_clusters {
        cluster.records.sort_by_key(|id| id.0);
    }
    expected_clusters.sort_by_key(|cluster| cluster.id.0);
    drop(engine);

    let store = PersistentStore::open(dir.path())?;
    let mut engine = Unirust::with_store_and_tuning(ontology, store, tuning);
    engine.initialize_streaming()?;
    assert_eq!(engine.streaming_cluster_count(), Some(count - 1));
    assert_eq!(boundary_snapshot(&engine), after_bridge);
    let mut actual_clusters = engine.clusters()?.clusters;
    for cluster in &mut actual_clusters {
        cluster.records.sort_by_key(|id| id.0);
    }
    actual_clusters.sort_by_key(|cluster| cluster.id.0);
    // Equal-rank DSU representatives can differ when a multi-value bridge is
    // replayed. Membership and public IDs must still be identical.
    let membership = |clusters: Vec<unirust_rs::dsu::Cluster>| {
        clusters
            .into_iter()
            .map(|cluster| (cluster.id, cluster.records))
            .collect::<Vec<_>>()
    };
    assert_eq!(membership(actual_clusters), membership(expected_clusters));
    for n in 2..count {
        assert_eq!(
            engine.global_cluster_id_for_record(original[n].record_id)?,
            global_ids[n]
        );
    }
    assert_eq!(
        engine.global_cluster_id_for_record(original[0].record_id)?,
        canonical
    );
    assert_eq!(
        engine.global_cluster_id_for_record(original[1].record_id)?,
        canonical
    );
    Ok(())
}

#[test]
#[ignore = "diagnostic timing; run explicitly with --ignored --nocapture"]
fn persistent_boundary_duplicate_scaling() -> anyhow::Result<()> {
    let count = std::env::var("UNIRUST_AUDIT_BOUNDARY_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(12_000);
    for size in [count / 2, count] {
        let dir = tempdir()?;
        let store = PersistentStore::open(dir.path())?;
        let mut engine = Unirust::with_store_and_tuning(
            history_ontology(),
            store,
            StreamingTuning::billion_scale().with_boundary_tracking(true),
        );
        let records = duplicate_records(&mut engine, size, 0);
        engine.stream_records(records)?;
        let records = duplicate_records(&mut engine, size, 1);
        let started = Instant::now();
        engine.stream_records(records)?;
        let elapsed = started.elapsed();
        assert_eq!(engine.streaming_cluster_count(), Some(size));
        eprintln!("persistent boundary duplicates: entities={size} elapsed={elapsed:?}");
    }
    Ok(())
}
