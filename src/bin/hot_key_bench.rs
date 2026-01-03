use std::time::Instant;

use unirust_rs::linker::StreamingLinker;
use unirust_rs::model::{Descriptor, Record, RecordId, RecordIdentity};
use unirust_rs::ontology::{IdentityKey, Ontology};
use unirust_rs::temporal::Interval;
use unirust_rs::{RecordStore, Store, StreamingTuning, TuningProfile};

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u8>().ok())
        .map(|value| value != 0)
        .unwrap_or(default)
}

fn main() {
    let total = env_u32("UNIRUST_HOTKEY_COUNT", 50_000);
    let multi_perspective = env_bool("UNIRUST_HOTKEY_MULTI_PERSPECTIVE", true);
    let hot_key_threshold = env_u32("UNIRUST_HOT_KEY_THRESHOLD", 50_000) as usize;

    let mut store = Store::new();
    let email_attr = store.intern_attr("email");
    let email_value = store.intern_value("hot@example.com");

    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::new(vec![email_attr], "email_only".to_string()));

    // Use balanced profile but with configurable hot key threshold
    let mut tuning = StreamingTuning::from_profile(TuningProfile::Balanced);
    tuning.hot_key_threshold = hot_key_threshold;

    let mut streaming = StreamingLinker::new(&store, &ontology, &tuning).expect("streaming linker");

    let interval = Interval::new(0, 10).expect("interval");
    let mut max_micros = 0u128;
    let mut last_micros = 0u128;

    let start = Instant::now();
    for idx in 0..total {
        let perspective = if multi_perspective {
            if idx % 2 == 0 {
                "crm"
            } else {
                "erp"
            }
        } else {
            "crm"
        };
        let identity = RecordIdentity::new(
            "person".to_string(),
            perspective.to_string(),
            format!("hot_{:09}", idx),
        );
        let descriptors = vec![Descriptor::new(email_attr, email_value, interval)];
        let record = Record::new(RecordId(0), identity, descriptors);
        let record_id = store.add_record(record).expect("add record");

        let tick = Instant::now();
        streaming
            .link_record(&store, &ontology, record_id)
            .expect("link record");
        let elapsed = tick.elapsed().as_micros();
        max_micros = max_micros.max(elapsed);
        last_micros = elapsed;
    }
    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64().max(1e-9);
    let throughput = (total as f64) / secs;

    println!(
        "hot_key_bench: {} records in {:.3}s ({:.2} rec/s) max_link={}us last_link={}us",
        total, secs, throughput, max_micros, last_micros
    );
}
