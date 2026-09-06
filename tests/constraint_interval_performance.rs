use std::time::Instant;

use tempfile::tempdir;
use unirust_rs::conflicts::Observation;
use unirust_rs::ontology::{Constraint, IdentityKey};
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, Record, RecordId, RecordIdentity,
    StreamingTuning, Unirust,
};

fn ontology() -> Ontology {
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email"));
    ontology.add_constraint(Constraint::unique_within_perspective_from_name(
        "status",
        "one_status",
    ));
    ontology
}

fn history(engine: &mut Unirust, uid: &str, value: &str, intervals: &[(i64, i64)]) -> Record {
    let email = engine.intern_attr("email");
    let shared = engine.intern_value("shared@example.com");
    let status = engine.intern_attr("status");
    let value = engine.intern_value(value);
    let mut descriptors = vec![Descriptor::new(email, shared, Interval::all_time())];
    descriptors.extend(
        intervals.iter().map(|&(start, end)| {
            Descriptor::new(status, value, Interval::new(start, end).unwrap())
        }),
    );
    Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "crm".into(), uid.into()),
        descriptors,
    )
}

fn violations(engine: &mut Unirust) -> anyhow::Result<Vec<(Interval, Vec<RecordId>)>> {
    let clusters = engine.clusters()?;
    assert_eq!(clusters.clusters.len(), 1);
    let mut violations = engine
        .detect_conflicts(&clusters)?
        .into_iter()
        .filter_map(|observation| match observation {
            Observation::IndirectConflict(conflict) if conflict.kind == "constraint_violation" => {
                Some((conflict.interval, conflict.participants.records.unwrap()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    violations.sort();
    Ok(violations)
}

#[test]
fn constraint_histories_preserve_adjacency_conflicts_and_participants_after_restart(
) -> anyhow::Result<()> {
    for persistent_dsu in [false, true] {
        let dir = tempdir()?;
        let tuning = StreamingTuning {
            use_persistent_dsu: persistent_dsu,
            use_tiered_index: persistent_dsu,
            ..StreamingTuning::default()
        };
        let mut engine = Unirust::with_store_and_tuning(
            ontology(),
            PersistentStore::open(dir.path())?,
            tuning.clone(),
        );
        let a = (0..16).map(|i| (i * 4, i * 4 + 2)).collect::<Vec<_>>();
        let b = (0..16).map(|i| (i * 4 + 2, i * 4 + 4)).collect::<Vec<_>>();
        let records = vec![
            history(&mut engine, "a", "A", &a),
            history(&mut engine, "b", "B", &b),
        ];
        engine.stream_records(records)?;
        assert!(violations(&mut engine)?.is_empty());
        let extra = history(&mut engine, "extra", "B", &[(1, 5), (41, 45)]);
        engine.stream_records(vec![extra])?;
        let expected = [(1, 2), (4, 5), (41, 42), (44, 45)]
            .into_iter()
            .map(|(start, end)| {
                (
                    Interval::new(start, end).unwrap(),
                    vec![RecordId(0), RecordId(1), RecordId(2)],
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(violations(&mut engine)?, expected);
        engine.checkpoint()?;
        drop(engine);
        let mut engine =
            Unirust::with_store_and_tuning(ontology(), PersistentStore::open(dir.path())?, tuning);
        assert_eq!(violations(&mut engine)?, expected);
    }
    Ok(())
}

#[test]
#[ignore = "manual release-mode timing diagnostic; no timing assertions"]
fn disjoint_constraint_history_scaling() -> anyhow::Result<()> {
    for size in [1_000, 2_000, 4_000] {
        let dir = tempdir()?;
        let mut engine = Unirust::with_store(ontology(), PersistentStore::open(dir.path())?);
        let a = (0..size).map(|i| (i * 4, i * 4 + 2)).collect::<Vec<_>>();
        let b = (0..size)
            .map(|i| (i * 4 + 2, i * 4 + 4))
            .collect::<Vec<_>>();
        let records = vec![
            history(&mut engine, "a", "A", &a),
            history(&mut engine, "b", "B", &b),
        ];
        engine.stream_records(records)?;
        let mut elapsed = Vec::new();
        for _ in 0..5 {
            let start = Instant::now();
            assert!(violations(&mut engine)?.is_empty());
            elapsed.push(start.elapsed().as_secs_f64() * 1_000.0);
        }
        elapsed.sort_by(f64::total_cmp);
        println!(
            "constraint_intervals_per_value={size} median_ms={:.3}",
            elapsed[2]
        );
    }
    Ok(())
}
