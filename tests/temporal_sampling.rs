use tempfile::tempdir;
use unirust_rs::ontology::IdentityKey;
use unirust_rs::store::RecordStore;
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, Record, RecordId, RecordIdentity,
    StreamingTuning, Unirust,
};

#[test]
fn stochastic_sampling_links_unbounded_and_wide_intervals() -> anyhow::Result<()> {
    for interval in [
        Interval::all_time(),
        Interval::from_start(-100),
        Interval::until_end(100),
        Interval::new(i64::MIN + 1, i64::MAX - 1)?,
        Interval::new(0, 100)?,
    ] {
        let dir = tempdir()?;
        let store = PersistentStore::open(dir.path())?;
        let mut ontology = Ontology::new();
        ontology.add_identity_key(IdentityKey::from_names(vec!["name"], "name"));
        // Force sampling even for a single candidate, with full-overlap acceptance
        // probability 1, so this regression does not depend on chance or block size.
        let tuning = StreamingTuning {
            stochastic_sampling: true,
            sampling_threshold: 0,
            sampling_target: 1,
            deferred_reconciliation: false,
            ..StreamingTuning::balanced()
        };
        let mut engine = Unirust::with_store_and_tuning(ontology, store, tuning);
        let attr = engine.intern_attr("name");
        let value = engine.intern_value("Smith");
        let mut assignments = Vec::new();
        for uid in ["first", "second"] {
            let record = Record::new(
                RecordId(0),
                RecordIdentity::new("person".into(), "crm".into(), uid.into()),
                vec![Descriptor::new(attr, value, interval)],
            );
            assignments.extend(engine.stream_records(vec![record])?);
        }
        assert_eq!(engine.linker_metrics_snapshot().stochastic_samples, 1);
        assert_eq!(
            assignments[0].cluster_id, assignments[1].cluster_id,
            "{interval}"
        );
        assert_eq!(engine.streaming_cluster_count(), Some(1));
        assert_eq!(engine.record_count(), 2);
    }
    Ok(())
}

#[test]
fn persistent_temporal_queries_find_wide_intervals_after_restart() -> anyhow::Result<()> {
    let dir = tempdir()?;
    {
        let store = PersistentStore::open(dir.path())?;
        let mut ontology = Ontology::new();
        ontology.add_identity_key(IdentityKey::from_names(vec!["name"], "name"));
        let mut engine = Unirust::with_store(ontology, store);
        let attr = engine.intern_attr("name");
        for (uid, interval) in [
            ("all", Interval::all_time()),
            ("from", Interval::from_start(-100)),
            ("until", Interval::until_end(100)),
            ("wide", Interval::new(i64::MIN + 1, i64::MAX - 1)?),
            ("finite", Interval::new(0, 100)?),
            ("future", Interval::new(400_000_000, 800_000_000)?),
            ("past", Interval::new(-800_000_000, -400_000_000)?),
        ] {
            let value = engine.intern_value(uid);
            let record = Record::new(
                RecordId(0),
                RecordIdentity::new("person".into(), "crm".into(), uid.into()),
                vec![Descriptor::new(attr, value, interval)],
            );
            engine.stream_records(vec![record])?;
        }
    }
    let store = PersistentStore::open(dir.path())?;
    for (interval, expected) in [
        (
            Interval::new(0, 100)?,
            vec!["all", "finite", "from", "until", "wide"],
        ),
        (Interval::new(100, 200)?, vec!["all", "from", "wide"]),
        (Interval::new(i64::MIN, i64::MIN + 1)?, vec!["all", "until"]),
        (Interval::new(i64::MAX - 1, i64::MAX)?, vec!["all", "from"]),
        (
            Interval::new(500_000_000, 500_000_001)?,
            vec!["all", "from", "future", "wide"],
        ),
        (
            Interval::new(-500_000_000, -499_999_999)?,
            vec!["all", "past", "until", "wide"],
        ),
        (
            Interval::all_time(),
            vec!["all", "finite", "from", "future", "past", "until", "wide"],
        ),
        (
            Interval::new(-500_000_000, 500_000_000)?,
            vec!["all", "finite", "from", "future", "past", "until", "wide"],
        ),
        (Interval { start: 0, end: 0 }, vec![]),
    ] {
        let mut actual = store
            .get_records_in_interval(interval)
            .into_iter()
            .map(|record| record.identity.uid)
            .collect::<Vec<_>>();
        actual.sort();
        assert_eq!(actual, expected, "{interval}");
    }
    Ok(())
}
