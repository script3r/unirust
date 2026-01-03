use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::model::{AttrId, Descriptor, Record, RecordId, RecordIdentity, ValueId};
use crate::ontology::{IdentityKey, Ontology, StrongIdentifier};
use crate::temporal::Interval;
use crate::RecordStore;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TestSchema {
    pub name_attr: AttrId,
    pub email_attr: AttrId,
    pub phone_attr: AttrId,
    pub ssn_attr: AttrId,
    pub name_value: ValueId,
    pub email_value: ValueId,
    pub phone_value: ValueId,
    pub ssn_value: ValueId,
}

#[derive(Debug, Clone)]
pub struct GeneratedDataset {
    #[allow(dead_code)]
    pub records: Vec<Record>,
    #[allow(dead_code)]
    pub schema: TestSchema,
}

pub fn generate_dataset(
    store: &mut dyn RecordStore,
    count: u32,
    overlap_probability: f64,
    seed: u64,
) -> GeneratedDataset {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut records = Vec::with_capacity(count as usize);

    let name_attr = store.interner_mut().intern_attr("name");
    let email_attr = store.interner_mut().intern_attr("email");
    let phone_attr = store.interner_mut().intern_attr("phone");
    let ssn_attr = store.interner_mut().intern_attr("ssn");

    let name_value = store.interner_mut().intern_value("John Doe");
    let email_value = store.interner_mut().intern_value("john@example.com");
    let phone_value = store.interner_mut().intern_value("555-1234");
    let ssn_value = store.interner_mut().intern_value("123-45-6789");

    for i in 1..=count {
        let perspectives = ["crm", "erp", "web", "mobile", "api"];
        let perspective = perspectives[rng.random_range(0..perspectives.len())];
        let uid = format!("{}_{:06}", perspective, i);
        let identity = RecordIdentity::new("person".to_string(), perspective.to_string(), uid);

        let start = rng.random_range(0..1000);
        let end = start + rng.random_range(1..100);
        let interval = Interval::new(start, end).expect("valid interval");

        let mut descriptors = Vec::new();

        if rng.random_bool(overlap_probability) {
            descriptors.push(Descriptor::new(name_attr, name_value, interval));
            descriptors.push(Descriptor::new(email_attr, email_value, interval));
        } else {
            let unique_name = format!("Person_{:06}", i);
            let unique_email = format!("person_{:06}@example.com", i);
            let name_value = store.interner_mut().intern_value(&unique_name);
            let email_value = store.interner_mut().intern_value(&unique_email);
            descriptors.push(Descriptor::new(name_attr, name_value, interval));
            descriptors.push(Descriptor::new(email_attr, email_value, interval));
        }

        if rng.random_bool(0.5) {
            descriptors.push(Descriptor::new(phone_attr, phone_value, interval));
        } else {
            let phone = format!("555-{:04}", rng.random_range(1000..9999));
            let phone_value = store.interner_mut().intern_value(&phone);
            descriptors.push(Descriptor::new(phone_attr, phone_value, interval));
        }

        if rng.random_bool(0.8) {
            descriptors.push(Descriptor::new(ssn_attr, ssn_value, interval));
        } else {
            let ssn = format!(
                "{:03}-{:02}-{:04}",
                rng.random_range(100..999),
                rng.random_range(10..99),
                rng.random_range(1000..9999)
            );
            let ssn_value = store.interner_mut().intern_value(&ssn);
            descriptors.push(Descriptor::new(ssn_attr, ssn_value, interval));
        }

        records.push(Record::new(RecordId(i), identity, descriptors));
    }

    GeneratedDataset {
        records,
        schema: TestSchema {
            name_attr,
            email_attr,
            phone_attr,
            ssn_attr,
            name_value,
            email_value,
            phone_value,
            ssn_value,
        },
    }
}

#[allow(dead_code)]
pub fn generate_batch(
    store: &mut dyn RecordStore,
    start_id: u32,
    count: u32,
    overlap_probability: f64,
    seed: u64,
) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(seed ^ start_id as u64);
    let mut records = Vec::with_capacity(count as usize);

    let name_attr = store.interner_mut().intern_attr("name");
    let email_attr = store.interner_mut().intern_attr("email");
    let phone_attr = store.interner_mut().intern_attr("phone");
    let ssn_attr = store.interner_mut().intern_attr("ssn");

    let name_value = store.interner_mut().intern_value("John Doe");
    let email_value = store.interner_mut().intern_value("john@example.com");
    let phone_value = store.interner_mut().intern_value("555-1234");
    let ssn_value = store.interner_mut().intern_value("123-45-6789");

    for offset in 0..count {
        let id = start_id + offset;
        let perspectives = ["crm", "erp", "web", "mobile", "api"];
        let perspective = perspectives[rng.random_range(0..perspectives.len())];
        let uid = format!("{}_{:09}", perspective, id);
        let identity = RecordIdentity::new("person".to_string(), perspective.to_string(), uid);

        let start = rng.random_range(0..1000);
        let end = start + rng.random_range(1..100);
        let interval = Interval::new(start, end).expect("valid interval");

        let mut descriptors = Vec::new();

        if rng.random_bool(overlap_probability) {
            descriptors.push(Descriptor::new(name_attr, name_value, interval));
            descriptors.push(Descriptor::new(email_attr, email_value, interval));
        } else {
            let unique_name = format!("Person_{:09}", id);
            let unique_email = format!("person_{:09}@example.com", id);
            let name_value = store.interner_mut().intern_value(&unique_name);
            let email_value = store.interner_mut().intern_value(&unique_email);
            descriptors.push(Descriptor::new(name_attr, name_value, interval));
            descriptors.push(Descriptor::new(email_attr, email_value, interval));
        }

        if rng.random_bool(0.5) {
            descriptors.push(Descriptor::new(phone_attr, phone_value, interval));
        } else {
            let phone = format!("555-{:04}", rng.random_range(1000..9999));
            let phone_value = store.interner_mut().intern_value(&phone);
            descriptors.push(Descriptor::new(phone_attr, phone_value, interval));
        }

        if rng.random_bool(0.8) {
            descriptors.push(Descriptor::new(ssn_attr, ssn_value, interval));
        } else {
            let ssn = format!(
                "{:03}-{:02}-{:04}",
                rng.random_range(100..999),
                rng.random_range(10..99),
                rng.random_range(1000..9999)
            );
            let ssn_value = store.interner_mut().intern_value(&ssn);
            descriptors.push(Descriptor::new(ssn_attr, ssn_value, interval));
        }

        records.push(Record::new(RecordId(id), identity, descriptors));
    }

    records
}

#[allow(dead_code)]
pub fn default_ontology(schema: &TestSchema) -> Ontology {
    let mut ontology = Ontology::new();
    let identity_key = IdentityKey::new(
        vec![schema.name_attr, schema.email_attr],
        "name_email".to_string(),
    );
    ontology.add_identity_key(identity_key);
    let ssn_strong_id = StrongIdentifier::new(schema.ssn_attr, "ssn".to_string());
    ontology.add_strong_identifier(ssn_strong_id);
    ontology
}
