//! Sharded ingestion for parallel streaming.

use crate::linker::StreamingLinker;
use crate::model::StringInterner;
use crate::model::{Descriptor, KeyValue, Record, RecordId};
use crate::ontology::Ontology;
use crate::store::Store;
use crate::{ClusterId, StreamedClusterAssignment};
use anyhow::Result;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardedClusterAssignment {
    pub shard_id: usize,
    pub record_id: RecordId,
    pub cluster_id: ClusterId,
}

#[derive(Debug)]
pub struct ShardedStreamEngine {
    shards: Vec<ShardState>,
    next_sequence: usize,
}

#[derive(Debug)]
struct ShardState {
    store: Store,
    streamer: StreamingLinker,
    ontology: Ontology,
    record_sequence: std::collections::HashMap<RecordId, usize>,
}

impl ShardedStreamEngine {
    pub fn new(ontology: Ontology, shard_count: usize) -> Result<Self> {
        Self::new_with_tuning(ontology, shard_count, crate::StreamingTuning::default())
    }

    pub fn new_with_tuning(
        ontology: Ontology,
        shard_count: usize,
        tuning: crate::StreamingTuning,
    ) -> Result<Self> {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let store = Store::new();
            let shard_ontology = ontology.clone();
            let streamer = StreamingLinker::new(&store, &shard_ontology, &tuning)?;
            shards.push(ShardState {
                store,
                streamer,
                ontology: shard_ontology,
                record_sequence: std::collections::HashMap::new(),
            });
        }
        Ok(Self {
            shards,
            next_sequence: 0,
        })
    }

    pub fn seed_interners(&mut self, interner: &StringInterner) {
        let mut attrs = interner
            .attr_ids()
            .filter_map(|id| interner.get_attr(id).cloned().map(|name| (id, name)))
            .collect::<Vec<_>>();
        attrs.sort_by_key(|(id, _)| id.0);

        let mut values = interner
            .value_ids()
            .filter_map(|id| interner.get_value(id).cloned().map(|value| (id, value)))
            .collect::<Vec<_>>();
        values.sort_by_key(|(id, _)| id.0);

        for shard in &mut self.shards {
            let shard_interner = shard.store.interner_mut();
            for (_, attr) in &attrs {
                shard_interner.intern_attr(attr);
            }
            for (_, value) in &values {
                shard_interner.intern_value(value);
            }
        }
    }

    pub fn stream_records(
        &mut self,
        records: Vec<Record>,
    ) -> Result<Vec<ShardedClusterAssignment>> {
        if self.shards.is_empty() {
            return Ok(Vec::new());
        }

        let mut buckets: Vec<Vec<(Record, usize)>> = vec![Vec::new(); self.shards.len()];
        for record in records {
            let sequence = self.next_sequence;
            self.next_sequence = self.next_sequence.saturating_add(1);
            let shard_id = shard_for_record(&record, &self.shards[0].ontology, self.shards.len());
            buckets[shard_id].push((record, sequence));
        }

        let shard_count = self.shards.len();
        let shards = std::mem::take(&mut self.shards);
        let mut handles = Vec::with_capacity(shard_count);
        for ((idx, shard), bucket) in shards.into_iter().enumerate().zip(buckets.into_iter()) {
            handles.push(std::thread::spawn(
                move || -> Result<(usize, ShardState, Vec<ShardedClusterAssignment>)> {
                    let mut shard = shard;
                    let assignments = shard.ingest(bucket)?;
                    let mapped = assignments
                        .into_iter()
                        .map(|assignment| ShardedClusterAssignment {
                            shard_id: idx,
                            record_id: assignment.record_id,
                            cluster_id: assignment.cluster_id,
                        })
                        .collect::<Vec<_>>();
                    Ok((idx, shard, mapped))
                },
            ));
        }

        let mut new_shards: Vec<Option<ShardState>> = (0..shard_count).map(|_| None).collect();
        let mut results = Vec::new();
        for handle in handles {
            let (idx, shard, assignments) = handle.join().expect("shard thread panicked")?;
            new_shards[idx] = Some(shard);
            results.extend(assignments);
        }

        self.shards = new_shards
            .into_iter()
            .map(|shard| shard.expect("missing shard state"))
            .collect();

        Ok(results)
    }

    pub fn reconcile_clusters(&self) -> Result<crate::dsu::Clusters> {
        let (_store, clusters) = self.reconcile_store_and_clusters()?;
        Ok(clusters)
    }

    pub fn reconcile_store_and_clusters(&self) -> Result<(Store, crate::dsu::Clusters)> {
        if self.shards.is_empty() {
            return Ok((
                Store::new(),
                crate::dsu::Clusters {
                    clusters: Vec::new(),
                },
            ));
        }

        let mut store = Store::new();
        seed_store_interner_from(&mut store, self.shards[0].store.interner());
        let mut remapped_records = Vec::new();
        for shard in &self.shards {
            let shard_interner = shard.store.interner();
            for record in shard.store.get_all_records() {
                let sequence = shard
                    .record_sequence
                    .get(&record.id)
                    .copied()
                    .unwrap_or(usize::MAX);
                let mut descriptors = Vec::with_capacity(record.descriptors.len());
                for descriptor in &record.descriptors {
                    let attr = shard_interner
                        .get_attr(descriptor.attr)
                        .unwrap_or(&"unknown".to_string())
                        .clone();
                    let value = shard_interner
                        .get_value(descriptor.value)
                        .unwrap_or(&"unknown".to_string())
                        .clone();
                    let attr_id = store.interner_mut().intern_attr(&attr);
                    let value_id = store.interner_mut().intern_value(&value);
                    descriptors.push(Descriptor::new(attr_id, value_id, descriptor.interval));
                }
                let remapped = Record::new(RecordId(0), record.identity.clone(), descriptors);
                remapped_records.push((sequence, remapped));
            }
        }
        remapped_records.sort_by_key(|(sequence, _)| *sequence);
        for (_, record) in remapped_records {
            store.add_record(record)?;
        }

        let clusters = crate::linker::build_clusters(&store, &self.shards[0].ontology)?;
        Ok((store, clusters))
    }
}

impl ShardState {
    fn ingest(&mut self, records: Vec<(Record, usize)>) -> Result<Vec<StreamedClusterAssignment>> {
        let mut assignments = Vec::with_capacity(records.len());
        for (record, sequence) in records {
            let record_id = self.store.add_record(record)?;
            self.record_sequence.insert(record_id, sequence);
            let cluster_id = self
                .streamer
                .link_record(&self.store, &self.ontology, record_id)?;
            assignments.push(StreamedClusterAssignment {
                record_id,
                cluster_id,
            });
        }
        Ok(assignments)
    }
}

fn shard_for_record(record: &Record, ontology: &Ontology, shard_count: usize) -> usize {
    let identity_keys = ontology.identity_keys_for_type(&record.identity.entity_type);
    let key_values = identity_keys
        .first()
        .and_then(|key| extract_key_values(record, key.attributes.as_slice()))
        .unwrap_or_default();

    let mut hasher = DefaultHasher::new();
    record.identity.entity_type.hash(&mut hasher);
    for value in &key_values {
        value.hash(&mut hasher);
    }
    let hash = hasher.finish();
    (hash as usize) % shard_count.max(1)
}

fn extract_key_values(record: &Record, attrs: &[crate::model::AttrId]) -> Option<Vec<KeyValue>> {
    let mut values = Vec::with_capacity(attrs.len());
    for attr in attrs {
        let descriptor = record
            .descriptors
            .iter()
            .filter(|descriptor| descriptor.attr == *attr)
            .max_by_key(|descriptor| descriptor.interval.end - descriptor.interval.start)?;
        values.push(KeyValue::new(*attr, descriptor.value));
    }
    Some(values)
}

fn seed_store_interner_from(store: &mut Store, interner: &StringInterner) {
    let mut attrs = interner
        .attr_ids()
        .filter_map(|id| interner.get_attr(id).cloned().map(|name| (id, name)))
        .collect::<Vec<_>>();
    attrs.sort_by_key(|(id, _)| id.0);

    let mut values = interner
        .value_ids()
        .filter_map(|id| interner.get_value(id).cloned().map(|value| (id, value)))
        .collect::<Vec<_>>();
    values.sort_by_key(|(id, _)| id.0);

    let store_interner = store.interner_mut();
    for (_, attr) in &attrs {
        store_interner.intern_attr(attr);
    }
    for (_, value) in &values {
        store_interner.intern_value(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Descriptor, RecordIdentity};
    use crate::Interval;

    #[test]
    fn sharded_reconcile_matches_single_linker() {
        let mut ontology = Ontology::new();
        let mut store = Store::new();

        let name_attr = store.interner_mut().intern_attr("name");
        let email_attr = store.interner_mut().intern_attr("email");
        ontology.add_identity_key(crate::ontology::IdentityKey::new(
            vec![name_attr, email_attr],
            "name_email".to_string(),
        ));

        let records = vec![
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
                vec![Descriptor::new(
                    name_attr,
                    store.interner_mut().intern_value("alice"),
                    Interval::new(0, 10).unwrap(),
                )],
            ),
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".to_string(), "crm".to_string(), "2".to_string()),
                vec![Descriptor::new(
                    name_attr,
                    store.interner_mut().intern_value("alice"),
                    Interval::new(0, 10).unwrap(),
                )],
            ),
        ];

        let mut sharded = ShardedStreamEngine::new(ontology.clone(), 2).unwrap();
        sharded.stream_records(records.clone()).unwrap();
        let sharded_clusters = sharded.reconcile_clusters().unwrap();

        let mut single_store = Store::new();
        for record in records {
            let mut cloned = record.clone();
            cloned.id = RecordId(0);
            single_store.add_record(cloned).unwrap();
        }
        let single_clusters = crate::linker::build_clusters(&single_store, &ontology).unwrap();

        assert_eq!(
            sharded_clusters.clusters.len(),
            single_clusters.clusters.len()
        );
    }

    #[test]
    fn sharded_stream_assigns_all_records() {
        let mut ontology = Ontology::new();
        let mut store = Store::new();

        let name_attr = store.interner_mut().intern_attr("name");
        let email_attr = store.interner_mut().intern_attr("email");
        ontology.add_identity_key(crate::ontology::IdentityKey::new(
            vec![name_attr, email_attr],
            "name_email".to_string(),
        ));

        let records = (0..20)
            .map(|idx| {
                Record::new(
                    RecordId(0),
                    RecordIdentity::new("person".to_string(), "crm".to_string(), format!("{idx}")),
                    vec![
                        Descriptor::new(
                            name_attr,
                            store.interner_mut().intern_value(&format!("user{idx}")),
                            Interval::new(0, 10).unwrap(),
                        ),
                        Descriptor::new(
                            email_attr,
                            store
                                .interner_mut()
                                .intern_value(&format!("user{idx}@example.com")),
                            Interval::new(0, 10).unwrap(),
                        ),
                    ],
                )
            })
            .collect::<Vec<_>>();

        let mut engine = ShardedStreamEngine::new(ontology, 4).unwrap();
        let assignments = engine.stream_records(records).unwrap();
        assert_eq!(assignments.len(), 20);
        assert!(assignments.iter().all(|assignment| assignment.shard_id < 4));
    }

    #[test]
    fn shard_for_record_is_deterministic() {
        let mut ontology = Ontology::new();
        let mut store = Store::new();

        let name_attr = store.interner_mut().intern_attr("name");
        let email_attr = store.interner_mut().intern_attr("email");
        ontology.add_identity_key(crate::ontology::IdentityKey::new(
            vec![name_attr, email_attr],
            "name_email".to_string(),
        ));

        let record = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
            vec![
                Descriptor::new(
                    name_attr,
                    store.interner_mut().intern_value("alice"),
                    Interval::new(0, 10).unwrap(),
                ),
                Descriptor::new(
                    email_attr,
                    store.interner_mut().intern_value("alice@example.com"),
                    Interval::new(0, 10).unwrap(),
                ),
            ],
        );

        let first = shard_for_record(&record, &ontology, 8);
        let second = shard_for_record(&record, &ontology, 8);
        assert_eq!(first, second);
        assert!(first < 8);
    }
}
