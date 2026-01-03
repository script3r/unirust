use std::path::PathBuf;
use std::time::Instant;
use unirust_rs::ontology::{IdentityKey, Ontology, StrongIdentifier};
use unirust_rs::RecordStore;
use unirust_rs::{
    Descriptor, Interval, PersistentStore, Record, RecordId, RecordIdentity, StreamingTuning,
    TuningProfile, Unirust,
};

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_path(key: &str, default: &str) -> PathBuf {
    std::env::var(key)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(default))
}

fn main() {
    let total = env_u32("UNIRUST_SCALE_COUNT", 1_000_000);
    let batch = env_u32("UNIRUST_SCALE_BATCH", 100_000).max(1);
    let overlap = env_f64("UNIRUST_SCALE_OVERLAP", 0.01);
    let data_dir = env_path("UNIRUST_SCALE_DATA_DIR", "./scale_data");
    let reset = std::env::var("UNIRUST_SCALE_RESET").ok().as_deref() == Some("1");

    if reset && data_dir.exists() {
        if let Err(err) = std::fs::remove_dir_all(&data_dir) {
            eprintln!("failed to reset data dir {:?}: {err}", data_dir);
            std::process::exit(1);
        }
    }
    if let Err(err) = std::fs::create_dir_all(&data_dir) {
        eprintln!("failed to create data dir {:?}: {err}", data_dir);
        std::process::exit(1);
    }

    let mut store = match PersistentStore::open(&data_dir) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("failed to open persistent store: {err}");
            std::process::exit(1);
        }
    };

    let name_attr = store.intern_attr("name");
    let email_attr = store.intern_attr("email");
    let phone_attr = store.intern_attr("phone");
    let ssn_attr = store.intern_attr("ssn");
    let shared_name = store.intern_value("John Doe");
    let shared_email = store.intern_value("john@example.com");
    let shared_phone = store.intern_value("555-1234");
    let shared_ssn = store.intern_value("123-45-6789");

    let mut ontology = Ontology::new();
    ontology.add_identity_key(IdentityKey::new(
        vec![name_attr, email_attr],
        "name_email".to_string(),
    ));
    ontology.add_strong_identifier(StrongIdentifier::new(ssn_attr, "ssn".to_string()));

    let mut unirust = Unirust::with_store_and_tuning(
        ontology,
        store,
        StreamingTuning::from_profile(TuningProfile::Balanced),
    );

    let overlap_stride = if overlap <= 0.0 {
        None
    } else {
        let stride = (1.0 / overlap).round().max(1.0) as u32;
        Some(stride)
    };

    let start = Instant::now();
    let mut offset = 1;
    while offset <= total {
        let remaining = total - offset + 1;
        let count = batch.min(remaining);
        let mut records = Vec::with_capacity(count as usize);
        for id in offset..(offset + count) {
            let interval = Interval::new(0, 10).expect("interval");
            let overlap_hit = overlap_stride
                .map(|stride| id % stride == 0)
                .unwrap_or(false);
            let (name_value, email_value) = if overlap_hit {
                (shared_name, shared_email)
            } else {
                let name = format!("Person_{:09}", id);
                let email = format!("person_{:09}@example.com", id);
                let name_value = unirust.store_mut().intern_value(&name);
                let email_value = unirust.store_mut().intern_value(&email);
                (name_value, email_value)
            };

            let phone_value = if id % 2 == 0 {
                shared_phone
            } else {
                let phone = format!("555-{:04}", id % 10000);
                unirust.store_mut().intern_value(&phone)
            };
            let ssn_value = if id % 5 == 0 {
                shared_ssn
            } else {
                let ssn = format!("{:03}-{:02}-{:04}", id % 1000, (id / 10) % 100, id % 10000);
                unirust.store_mut().intern_value(&ssn)
            };

            let identity = RecordIdentity::new(
                "person".to_string(),
                "crm".to_string(),
                format!("crm_{:09}", id),
            );
            let descriptors = vec![
                Descriptor::new(name_attr, name_value, interval),
                Descriptor::new(email_attr, email_value, interval),
                Descriptor::new(phone_attr, phone_value, interval),
                Descriptor::new(ssn_attr, ssn_value, interval),
            ];
            records.push(Record::new(RecordId(0), identity, descriptors));
        }
        if let Err(err) = unirust.stream_records(records) {
            eprintln!("scale persistent bench failed: {err}");
            std::process::exit(1);
        }
        offset += count;
    }

    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64().max(1e-9);
    let throughput = (total as f64) / secs;
    println!(
        "scale_persistent: {total} records in {:.3}s ({:.2} rec/s) dir={}",
        secs,
        throughput,
        data_dir.display()
    );
}
