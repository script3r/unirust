use std::collections::HashMap;
use std::time::Instant;

use tempfile::tempdir;
use unirust_rs::graph::{golden_for_cluster, GoldenDescriptor};
use unirust_rs::model::{AttrId, ValueId};
use unirust_rs::ontology::IdentityKey;
use unirust_rs::temporal::{coalesce_same_value, difference, intersect};
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, QueryOutcome, Record, RecordId,
    RecordIdentity, RecordStore, StreamingTuning, Unirust,
};

fn ontology() -> Ontology {
    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email"));
    ontology
}

fn coalesce(intervals: &[Interval]) -> Vec<Interval> {
    coalesce_same_value(
        &intervals
            .iter()
            .map(|interval| (*interval, ()))
            .collect::<Vec<_>>(),
    )
    .into_iter()
    .map(|(interval, ())| interval)
    .collect()
}

// Retain the original pairwise subtraction algorithm as a differential oracle.
// It deliberately does not use the production mastering implementation.
fn subtraction_oracle(store: &dyn RecordStore, record_ids: &[RecordId]) -> Vec<GoldenDescriptor> {
    let mut attrs: HashMap<AttrId, HashMap<ValueId, Vec<Interval>>> = HashMap::new();
    for id in record_ids {
        let record = store.get_record(*id).expect("persisted member");
        for descriptor in record.descriptors {
            attrs
                .entry(descriptor.attr)
                .or_default()
                .entry(descriptor.value)
                .or_default()
                .push(descriptor.interval);
        }
    }
    let mut golden = Vec::new();
    for (attr, values) in attrs {
        let candidates: Vec<_> = values
            .into_iter()
            .map(|(value, intervals)| (value, coalesce(&intervals)))
            .collect();
        for (value, intervals) in &candidates {
            let mut remaining = intervals.clone();
            for (other_value, others) in &candidates {
                if value == other_value {
                    continue;
                }
                for other in others {
                    remaining = remaining
                        .into_iter()
                        .flat_map(|segment| difference(&segment, other))
                        .collect();
                }
            }
            for interval in coalesce(&remaining) {
                golden.push(GoldenDescriptor {
                    attr: store.resolve_attr(attr).expect("attribute"),
                    value: store.resolve_value(*value).expect("value"),
                    interval,
                });
            }
        }
    }
    golden.sort_by(|a, b| {
        a.attr
            .cmp(&b.attr)
            .then(a.value.cmp(&b.value))
            .then(a.interval.start.cmp(&b.interval.start))
    });
    golden
}

fn make_record(
    engine: &mut Unirust,
    uid: &str,
    intervals: &[(&str, String, i64, i64)],
) -> anyhow::Result<Record> {
    let mut descriptors = vec![Descriptor::new(
        engine.intern_attr("email"),
        engine.intern_value("entity@example.com"),
        Interval::new(i64::MIN, i64::MAX)?,
    )];
    for (attr, value, start, end) in intervals {
        descriptors.push(Descriptor::new(
            engine.intern_attr(attr),
            engine.intern_value(value),
            Interval::new(*start, *end)?,
        ));
    }
    Ok(Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "source".into(), uid.into()),
        descriptors,
    ))
}

fn assert_mastering(engine: &mut Unirust) -> anyhow::Result<()> {
    let clusters = engine.clusters()?;
    assert_eq!(clusters.clusters.len(), 1);
    let cluster = &clusters.clusters[0];
    let expected = subtraction_oracle(engine.store(), &cluster.records);
    assert_eq!(golden_for_cluster(engine.store(), cluster), expected);
    let descriptor = engine
        .lookup_query_descriptor("email", "entity@example.com")
        .unwrap();
    for interval in [Interval::new(i64::MIN, i64::MAX)?, Interval::new(-13, 19)?] {
        let QueryOutcome::Matches(matches) = engine.query(&[descriptor], interval)? else {
            panic!("one resolved entity cannot conflict with itself")
        };
        assert_eq!(matches.len(), 1);
        let filtered: Vec<_> = expected
            .iter()
            .filter_map(|golden| {
                intersect(&golden.interval, &interval).map(|interval| GoldenDescriptor {
                    attr: golden.attr.clone(),
                    value: golden.value.clone(),
                    interval,
                })
            })
            .collect();
        assert_eq!(matches[0].golden, filtered);
    }
    Ok(())
}

#[test]
fn temporal_mastering_matches_subtraction_before_and_after_restart() -> anyhow::Result<()> {
    for persistent_backends in [false, true] {
        let dir = tempdir()?;
        let tuning = StreamingTuning {
            use_persistent_dsu: persistent_backends,
            use_tiered_index: persistent_backends,
            ..StreamingTuning::default()
        };
        let mut engine = Unirust::with_store_and_tuning(
            ontology(),
            PersistentStore::open(dir.path())?,
            tuning.clone(),
        );
        let mut data = vec![
            ("history", "red".into(), -10, 10),
            ("history", "red".into(), -5, 15),
            ("history", "red".into(), -5, 15),
            ("history", "blue".into(), 0, 5),
            ("history", "green".into(), 5, 10),
            ("history", "red".into(), 15, 20),
            ("history", "green".into(), 20, 25),
            ("history", "red".into(), 30, 40),
            ("history", "red".into(), 35, 45),
            ("extreme", "first".into(), i64::MIN, i64::MIN + 10),
            ("extreme", "nested".into(), i64::MIN + 5, i64::MIN + 8),
            ("extreme", "last".into(), i64::MAX - 10, i64::MAX),
        ];
        let mut random = 0x713d6e09a259_u64;
        for index in 0..120 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let start = ((random >> 20) % 200) as i64 - 100;
            let width = ((random >> 40) % 25) as i64 + 1;
            data.push((
                if index % 2 == 0 {
                    "random_a"
                } else {
                    "random_b"
                },
                format!("value-{}", random % 7),
                start,
                start + width,
            ));
        }
        let mut first = make_record(&mut engine, "first", &data)?;
        for (value, start, end) in [("valid", 0, 10), ("empty", 7, 7), ("reversed", 9, 3)] {
            first.descriptors.push(Descriptor::new(
                engine.intern_attr("malformed"),
                engine.intern_value(value),
                Interval { start, end },
            ));
        }
        engine.stream_records(vec![first])?;
        assert_mastering(&mut engine)?;
        let second = make_record(
            &mut engine,
            "second",
            &[
                ("history", "green".into(), 12, 17),
                ("history", "green".into(), 12, 17),
                ("history", "red".into(), 17, 20),
            ],
        )?;
        engine.stream_records(vec![second])?;
        assert_mastering(&mut engine)?;
        drop(engine);
        let mut recovered =
            Unirust::with_store_and_tuning(ontology(), PersistentStore::open(dir.path())?, tuning);
        assert_mastering(&mut recovered)?;
        recovered.initialize_streaming()?;
        assert_mastering(&mut recovered)?;
    }
    Ok(())
}

#[test]
#[ignore = "manual persistent query scaling benchmark; run release with --ignored --nocapture"]
fn historical_value_query_scaling() -> anyhow::Result<()> {
    for count in [1_000, 2_000, 4_000] {
        let dir = tempdir()?;
        let mut engine = Unirust::with_store(ontology(), PersistentStore::open(dir.path())?);
        let data: Vec<_> = (0..count)
            .map(|index| {
                (
                    "history",
                    format!("version-{index}"),
                    index as i64 * 2,
                    index as i64 * 2 + 1,
                )
            })
            .collect();
        let record = make_record(&mut engine, "history", &data)?;
        engine.stream_records(vec![record])?;
        let descriptor = engine
            .lookup_query_descriptor("email", "entity@example.com")
            .unwrap();
        let mut samples = Vec::new();
        for _ in 0..3 {
            let start = Instant::now();
            let outcome = engine.query(&[descriptor], Interval::new(i64::MIN, i64::MAX)?)?;
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
            let QueryOutcome::Matches(matches) = outcome else {
                panic!("unexpected conflict")
            };
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].golden.len(), count + 1);
        }
        samples.sort_by(f64::total_cmp);
        println!(
            "historical_values={count} median_query_ms={:.3}",
            samples[1]
        );
    }
    Ok(())
}

fn assert_interval_conjunction(
    engine: &Unirust,
    left: &[Interval],
    right: &[Interval],
) -> anyhow::Result<()> {
    let x = engine.lookup_query_descriptor("x", "present").unwrap();
    let y = engine.lookup_query_descriptor("y", "present").unwrap();
    for query_window in [Interval::new(i64::MIN, i64::MAX)?, Interval::new(-27, 21)?] {
        let mut expected = Vec::new();
        for a in left {
            for b in right {
                if let Some(overlap) = intersect(a, b).and_then(|i| intersect(&i, &query_window)) {
                    expected.push(overlap);
                }
            }
        }
        let expected = coalesce(&expected);
        for descriptors in [vec![x, y], vec![y, x], vec![x, y, x]] {
            let QueryOutcome::Matches(matches) = engine.query(&descriptors, query_window)? else {
                panic!("one entity cannot conflict with itself")
            };
            let mut actual: Vec<_> = matches.iter().map(|entry| entry.interval).collect();
            actual.sort();
            assert_eq!(actual, expected);
        }
    }
    Ok(())
}

#[test]
fn temporal_conjunction_matches_pairwise_oracle_after_restart() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut engine = Unirust::with_store(ontology(), PersistentStore::open(dir.path())?);
    let left: Vec<_> = [
        (i64::MIN, i64::MIN + 1),
        (-40, -20),
        (-30, -10),
        (-10, 0),
        (5, 10),
        (5, 10),
        (20, 30),
        (i64::MAX - 5, i64::MAX),
    ]
    .into_iter()
    .map(|(start, end)| Interval::new(start, end).unwrap())
    .collect();
    let right: Vec<_> = [
        (i64::MIN, i64::MIN + 5),
        (-35, -25),
        (-25, -15),
        (0, 5),
        (8, 22),
        (i64::MAX - 10, i64::MAX - 2),
    ]
    .into_iter()
    .map(|(start, end)| Interval::new(start, end).unwrap())
    .collect();
    let data: Vec<_> = left
        .iter()
        .map(|i| ("x", "present".into(), i.start, i.end))
        .chain(
            right
                .iter()
                .map(|i| ("y", "present".into(), i.start, i.end)),
        )
        .collect();
    let record = make_record(&mut engine, "intersection", &data)?;
    engine.stream_records(vec![record])?;
    assert_interval_conjunction(&engine, &left, &right)?;
    drop(engine);
    let mut recovered = Unirust::with_store(ontology(), PersistentStore::open(dir.path())?);
    assert_interval_conjunction(&recovered, &left, &right)?;
    recovered.initialize_streaming()?;
    assert_interval_conjunction(&recovered, &left, &right)?;
    Ok(())
}

#[test]
#[ignore = "manual persistent query scaling benchmark; run release with --ignored --nocapture"]
fn disjoint_conjunction_query_scaling() -> anyhow::Result<()> {
    for count in [2_000, 4_000, 8_000] {
        let dir = tempdir()?;
        let mut engine = Unirust::with_store(ontology(), PersistentStore::open(dir.path())?);
        let data: Vec<_> = (0..count)
            .flat_map(|index| {
                [
                    (
                        "x",
                        "present".into(),
                        index as i64 * 4,
                        index as i64 * 4 + 1,
                    ),
                    (
                        "y",
                        "present".into(),
                        index as i64 * 4 + 2,
                        index as i64 * 4 + 3,
                    ),
                ]
            })
            .collect();
        let record = make_record(&mut engine, "intersection", &data)?;
        engine.stream_records(vec![record])?;
        let query = [
            engine.lookup_query_descriptor("x", "present").unwrap(),
            engine.lookup_query_descriptor("y", "present").unwrap(),
        ];
        let mut samples = Vec::new();
        for _ in 0..3 {
            let start = Instant::now();
            let outcome = engine.query(&query, Interval::new(i64::MIN, i64::MAX)?)?;
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(outcome, QueryOutcome::Matches(Vec::new()));
        }
        samples.sort_by(f64::total_cmp);
        println!(
            "intervals_per_descriptor={count} median_query_ms={:.3}",
            samples[1]
        );
    }
    Ok(())
}
