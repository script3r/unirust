//! Manual measurements for the September 2026 audit. No timing assertions.

use std::time::Instant;

use tempfile::tempdir;
use unirust_rs::ontology::IdentityKey;
use unirust_rs::{
    Descriptor, Interval, Ontology, PersistentStore, QueryDescriptor, QueryOutcome, Record,
    RecordId, RecordIdentity, Unirust,
};

fn make_record(engine: &mut Unirust, id: usize) -> Record {
    let attr = engine.intern_attr("email");
    let value = engine.intern_value(&format!("person-{id}@example.com"));
    Record::new(
        RecordId(0),
        RecordIdentity::new("person".into(), "audit".into(), id.to_string()),
        vec![Descriptor::new(attr, value, Interval::new(0, 100).unwrap())],
    )
}

#[test]
#[ignore = "manual performance audit; run in release mode with --ignored --nocapture"]
fn selective_query_after_unrelated_ingest() -> anyhow::Result<()> {
    for size in [5_000, 20_000, 80_000] {
        let dir = tempdir()?;
        let store = PersistentStore::open(dir.path())?;
        let mut ontology = Ontology::new();
        ontology.add_identity_key(IdentityKey::from_names(vec!["email"], "email"));
        let mut engine = Unirust::with_store(ontology, store);
        for start in (0..size).step_by(1_000) {
            let records = (start..start + 1_000)
                .map(|id| make_record(&mut engine, id))
                .collect();
            engine.stream_records(records)?;
        }
        let query = [QueryDescriptor {
            attr: engine.intern_attr("email"),
            value: engine.intern_value("person-0@example.com"),
        }];
        let mut elapsed = Vec::new();
        for step in 0..3 {
            if step == 2 {
                let record = make_record(&mut engine, size);
                engine.stream_records(vec![record])?;
            }
            let start = Instant::now();
            let outcome = engine.query_master_entities(&query, Interval::new(0, 100)?)?;
            elapsed.push(start.elapsed().as_secs_f64() * 1_000.0);
            match outcome {
                QueryOutcome::Matches(matches) => assert_eq!(matches.len(), 1),
                other => panic!("unexpected query result: {other:?}"),
            }
        }
        println!(
            "records={size} cold_ms={:.3} warm_ms={:.3} after_unrelated_ingest_ms={:.3}",
            elapsed[0], elapsed[1], elapsed[2]
        );
    }
    Ok(())
}
