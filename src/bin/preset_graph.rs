use std::env;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use unirust_rs::graph;
use unirust_rs::linker;
use unirust_rs::model::{Descriptor, Record, RecordId, RecordIdentity};
use unirust_rs::ontology::{IdentityKey, Ontology};
use unirust_rs::store::Store;
use unirust_rs::temporal::Interval;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Preset {
    id: String,
    label: String,
    identity_keys: Vec<String>,
    conflict_attrs: Vec<String>,
    #[serde(default)]
    description: Option<String>,
    records: Vec<PresetRecord>,
    #[serde(default)]
    graph: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PresetRecord {
    id: String,
    uid: String,
    entity_type: String,
    perspective: String,
    descriptors: Vec<PresetDescriptor>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PresetDescriptor {
    attr: String,
    value: String,
    start: i64,
    end: i64,
}

fn main() -> Result<()> {
    let path = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("web/presets.json"));

    let contents = fs::read_to_string(&path)
        .with_context(|| format!("failed to read presets from {}", path.display()))?;
    let mut presets: Vec<Preset> = serde_json::from_str(&contents)?;

    for preset in &mut presets {
        let graph = build_graph_for_preset(preset)?;
        preset.graph = Some(serde_json::to_value(graph)?);
    }

    let output = serde_json::to_string_pretty(&presets)?;
    fs::write(&path, output)?;
    println!("updated graph data for {}", path.display());
    Ok(())
}

fn build_graph_for_preset(preset: &Preset) -> Result<graph::KnowledgeGraph> {
    let mut store = Store::new();
    let mut ontology = Ontology::new();

    let identity_attrs: Vec<_> = preset
        .identity_keys
        .iter()
        .map(|attr| store.interner_mut().intern_attr(attr))
        .collect();
    if !identity_attrs.is_empty() {
        ontology.add_identity_key(IdentityKey::new(identity_attrs, "preset".to_string()));
    }

    for (idx, record) in preset.records.iter().enumerate() {
        let record_id = parse_record_id(&record.id).unwrap_or(0);
        let id = if record_id == 0 {
            RecordId(0)
        } else {
            RecordId(record_id)
        };

        let descriptors = record
            .descriptors
            .iter()
            .map(|descriptor| {
                let attr_id = store.interner_mut().intern_attr(&descriptor.attr);
                let value_id = store.interner_mut().intern_value(&descriptor.value);
                let interval =
                    Interval::new(descriptor.start, descriptor.end).with_context(|| {
                        format!(
                            "invalid interval {}-{} for record {}",
                            descriptor.start, descriptor.end, record.id
                        )
                    })?;
                Ok(Descriptor::new(attr_id, value_id, interval))
            })
            .collect::<Result<Vec<_>>>()?;

        let record = Record::new(
            id,
            RecordIdentity::new(
                record.entity_type.clone(),
                record.perspective.clone(),
                record.uid.clone(),
            ),
            descriptors,
        );
        let record_label = record.id;
        store
            .add_record(record)
            .with_context(|| format!("failed to add record {} (index {})", record_label, idx))?;
    }

    let clusters = linker::build_clusters(&store, &ontology)?;
    graph::export_graph(&store, &clusters, &[], &ontology)
}

fn parse_record_id(raw: &str) -> Option<u32> {
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}
