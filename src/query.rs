use crate::dsu::Clusters;
use crate::graph::{cluster_keys_for_clusters, golden_for_cluster, ClusterKey, GoldenDescriptor};
use crate::model::{AttrId, ClusterId, RecordId, ValueId};
use crate::ontology::Ontology;
use crate::store::RecordStore;
use crate::temporal::Interval;
use anyhow::Result;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryDescriptor {
    pub attr: AttrId,
    pub value: ValueId,
}

#[derive(Debug, Clone)]
pub struct QuerySelectivity {
    count: u64,
    total_matches: u64,
    max_matches: u64,
}

impl QuerySelectivity {
    fn record(&mut self, matches: usize) {
        self.count += 1;
        self.total_matches += matches as u64;
        self.max_matches = self.max_matches.max(matches as u64);
    }

    fn estimate(&self) -> usize {
        if self.count == 0 {
            return 0;
        }
        (self.total_matches / self.count) as usize
    }
}

#[derive(Debug, Default)]
pub struct QuerySelectivityStats {
    entries: HashMap<QueryDescriptor, QuerySelectivity>,
}

impl QuerySelectivityStats {
    pub fn order_descriptors(&self, descriptors: &[QueryDescriptor]) -> Vec<QueryDescriptor> {
        let mut ordered = descriptors
            .iter()
            .copied()
            .enumerate()
            .map(|(idx, descriptor)| {
                let estimate = self.entries.get(&descriptor).map(|entry| entry.estimate());
                (estimate.unwrap_or(usize::MAX), idx, descriptor)
            })
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(estimate, idx, _)| (*estimate, *idx));
        ordered
            .into_iter()
            .map(|(_, _, descriptor)| descriptor)
            .collect()
    }

    pub fn record(&mut self, descriptor: QueryDescriptor, matches: usize) {
        self.entries
            .entry(descriptor)
            .or_insert(QuerySelectivity {
                count: 0,
                total_matches: 0,
                max_matches: 0,
            })
            .record(matches);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMatch {
    pub cluster_id: ClusterId,
    pub interval: Interval,
    pub golden: Vec<GoldenDescriptor>,
    pub cluster_key: Option<String>,
    pub cluster_key_identity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryConflict {
    pub interval: Interval,
    pub clusters: Vec<ClusterId>,
    pub descriptors: Vec<QueryDescriptorOverlap>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDescriptorOverlap {
    pub descriptor: QueryDescriptor,
    pub interval: Interval,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOutcome {
    Matches(Vec<QueryMatch>),
    Conflict(QueryConflict),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawMatch {
    cluster_id: ClusterId,
    interval: Interval,
}

pub fn query_master_entities(
    store: &dyn RecordStore,
    clusters: &Clusters,
    ontology: &Ontology,
    descriptors: &[QueryDescriptor],
    interval: Interval,
) -> Result<QueryOutcome> {
    let golden_cache = build_golden_cache(store, clusters);
    let cluster_key_cache = build_cluster_key_cache(store, clusters, ontology);
    let record_to_cluster = build_record_to_cluster_map(clusters);
    query_master_entities_with_cache(
        store,
        clusters,
        descriptors,
        interval,
        &golden_cache,
        &cluster_key_cache,
        &record_to_cluster,
    )
}

pub fn query_master_entities_with_cache(
    store: &dyn RecordStore,
    clusters: &Clusters,
    descriptors: &[QueryDescriptor],
    interval: Interval,
    golden_cache: &std::collections::HashMap<ClusterId, Vec<GoldenDescriptor>>,
    cluster_key_cache: &std::collections::HashMap<ClusterId, ClusterKey>,
    record_to_cluster: &std::collections::HashMap<RecordId, ClusterId>,
) -> Result<QueryOutcome> {
    if descriptors.is_empty() {
        return Ok(QueryOutcome::Matches(Vec::new()));
    }

    let mut candidate_intervals_by_cluster: std::collections::HashMap<ClusterId, Vec<Interval>> =
        std::collections::HashMap::new();

    let mut descriptor_matches = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        let matches =
            store.get_records_with_value_in_interval(descriptor.attr, descriptor.value, interval);
        descriptor_matches.push((descriptor, matches));
    }
    descriptor_matches.sort_by_key(|(_, matches)| matches.len());

    for (idx, (_descriptor, matches)) in descriptor_matches.iter().enumerate() {
        let mut next: std::collections::HashMap<ClusterId, Vec<Interval>> =
            std::collections::HashMap::new();

        for (record_id, record_interval) in matches {
            if let Some(&cluster_id) = record_to_cluster.get(record_id) {
                if let Some(overlap) = crate::temporal::intersect(record_interval, &interval) {
                    next.entry(cluster_id).or_default().push(overlap);
                }
            }
        }

        if idx == 0 {
            for (cluster_id, intervals) in next {
                candidate_intervals_by_cluster
                    .entry(cluster_id)
                    .or_default()
                    .extend(intervals);
            }
        } else {
            let mut intersected = std::collections::HashMap::new();
            for (cluster_id, intervals) in candidate_intervals_by_cluster {
                if let Some(other_intervals) = next.get(&cluster_id) {
                    let merged = intersect_interval_sets(&intervals, other_intervals);
                    if !merged.is_empty() {
                        intersected.insert(cluster_id, merged);
                    }
                }
            }
            candidate_intervals_by_cluster = intersected;
        }

        if candidate_intervals_by_cluster.is_empty() {
            break;
        }
    }

    let mut matches: Vec<RawMatch> = Vec::new();
    for (cluster_id, intervals) in candidate_intervals_by_cluster {
        for merged in coalesce_intervals(&intervals) {
            matches.push(RawMatch {
                cluster_id,
                interval: merged,
            });
        }
    }

    let matches = coalesce_matches_per_cluster(matches);
    if let Some(conflict) = find_overlap_conflict(store, clusters, &matches, descriptors) {
        return Ok(QueryOutcome::Conflict(conflict));
    }
    let matches = matches
        .into_iter()
        .map(|entry| QueryMatch {
            cluster_id: entry.cluster_id,
            interval: entry.interval,
            golden: filter_golden_for_interval(
                golden_cache
                    .get(&entry.cluster_id)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
                entry.interval,
            ),
            cluster_key: cluster_key_cache
                .get(&entry.cluster_id)
                .map(|key| key.value.clone()),
            cluster_key_identity: cluster_key_cache
                .get(&entry.cluster_id)
                .map(|key| key.identity_key.clone()),
        })
        .collect();
    Ok(QueryOutcome::Matches(matches))
}

#[allow(clippy::too_many_arguments)]
pub fn query_master_entities_with_cache_selective(
    store: &dyn RecordStore,
    clusters: &Clusters,
    descriptors: &[QueryDescriptor],
    interval: Interval,
    golden_cache: &std::collections::HashMap<ClusterId, Vec<GoldenDescriptor>>,
    cluster_key_cache: &std::collections::HashMap<ClusterId, ClusterKey>,
    record_to_cluster: &std::collections::HashMap<RecordId, ClusterId>,
    stats: &mut QuerySelectivityStats,
) -> Result<QueryOutcome> {
    if descriptors.is_empty() {
        return Ok(QueryOutcome::Matches(Vec::new()));
    }

    let ordered = stats.order_descriptors(descriptors);

    let mut candidate_intervals_by_cluster: std::collections::HashMap<ClusterId, Vec<Interval>> =
        std::collections::HashMap::new();

    for (idx, descriptor) in ordered.iter().enumerate() {
        let matches =
            store.get_records_with_value_in_interval(descriptor.attr, descriptor.value, interval);
        stats.record(*descriptor, matches.len());
        if matches.is_empty() {
            candidate_intervals_by_cluster.clear();
            break;
        }

        let mut next: std::collections::HashMap<ClusterId, Vec<Interval>> =
            std::collections::HashMap::new();
        for (record_id, record_interval) in matches {
            if let Some(&cluster_id) = record_to_cluster.get(&record_id) {
                if let Some(overlap) = crate::temporal::intersect(&record_interval, &interval) {
                    next.entry(cluster_id).or_default().push(overlap);
                }
            }
        }

        if idx == 0 {
            for (cluster_id, intervals) in next {
                candidate_intervals_by_cluster
                    .entry(cluster_id)
                    .or_default()
                    .extend(intervals);
            }
        } else {
            let mut intersected = std::collections::HashMap::new();
            for (cluster_id, intervals) in candidate_intervals_by_cluster {
                if let Some(other_intervals) = next.get(&cluster_id) {
                    let merged = intersect_interval_sets(&intervals, other_intervals);
                    if !merged.is_empty() {
                        intersected.insert(cluster_id, merged);
                    }
                }
            }
            candidate_intervals_by_cluster = intersected;
        }

        if candidate_intervals_by_cluster.is_empty() {
            break;
        }
    }

    let mut matches: Vec<RawMatch> = Vec::new();
    for (cluster_id, intervals) in candidate_intervals_by_cluster {
        for merged in coalesce_intervals(&intervals) {
            matches.push(RawMatch {
                cluster_id,
                interval: merged,
            });
        }
    }

    let matches = coalesce_matches_per_cluster(matches);
    if let Some(conflict) = find_overlap_conflict(store, clusters, &matches, descriptors) {
        return Ok(QueryOutcome::Conflict(conflict));
    }
    let matches = matches
        .into_iter()
        .map(|entry| QueryMatch {
            cluster_id: entry.cluster_id,
            interval: entry.interval,
            golden: filter_golden_for_interval(
                golden_cache
                    .get(&entry.cluster_id)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
                entry.interval,
            ),
            cluster_key: cluster_key_cache
                .get(&entry.cluster_id)
                .map(|key| key.value.clone()),
            cluster_key_identity: cluster_key_cache
                .get(&entry.cluster_id)
                .map(|key| key.identity_key.clone()),
        })
        .collect();
    Ok(QueryOutcome::Matches(matches))
}

fn intersect_interval_sets(a: &[Interval], b: &[Interval]) -> Vec<Interval> {
    let mut overlaps = Vec::new();

    for interval_a in a {
        for interval_b in b {
            if let Some(overlap) = crate::temporal::intersect(interval_a, interval_b) {
                overlaps.push(overlap);
            }
        }
    }

    coalesce_intervals(&overlaps)
}

fn coalesce_intervals(intervals: &[Interval]) -> Vec<Interval> {
    if intervals.is_empty() {
        return Vec::new();
    }
    crate::temporal::coalesce_same_value(
        &intervals
            .iter()
            .map(|interval| (*interval, ()))
            .collect::<Vec<_>>(),
    )
    .into_iter()
    .map(|(interval, _)| interval)
    .collect()
}

fn coalesce_matches_per_cluster(matches: Vec<RawMatch>) -> Vec<RawMatch> {
    let mut by_cluster: std::collections::HashMap<ClusterId, Vec<Interval>> =
        std::collections::HashMap::new();

    for entry in matches {
        by_cluster
            .entry(entry.cluster_id)
            .or_default()
            .push(entry.interval);
    }

    let mut result = Vec::new();
    for (cluster_id, intervals) in by_cluster {
        for interval in coalesce_intervals(&intervals) {
            result.push(RawMatch {
                cluster_id,
                interval,
            });
        }
    }

    result.sort_by(|a, b| a.interval.start.cmp(&b.interval.start));
    result
}

fn find_overlap_conflict(
    store: &dyn RecordStore,
    clusters: &Clusters,
    matches: &[RawMatch],
    descriptors: &[QueryDescriptor],
) -> Option<QueryConflict> {
    if matches.len() <= 1 {
        return None;
    }

    let mut sorted = matches.to_vec();
    sorted.sort_by(|a, b| a.interval.start.cmp(&b.interval.start));

    for window in sorted.windows(2) {
        let current = &window[0];
        let next = &window[1];
        if current.cluster_id == next.cluster_id {
            continue;
        }
        if crate::temporal::is_overlapping(&current.interval, &next.interval) {
            let overlap = crate::temporal::intersect(&current.interval, &next.interval)
                .unwrap_or(current.interval);
            let conflicting_descriptors = offending_descriptors_for_clusters(
                store,
                clusters,
                descriptors,
                overlap,
                current.cluster_id,
                next.cluster_id,
            );
            let descriptors = if conflicting_descriptors.is_empty() {
                descriptors
                    .iter()
                    .map(|descriptor| QueryDescriptorOverlap {
                        descriptor: *descriptor,
                        interval: overlap,
                    })
                    .collect()
            } else {
                conflicting_descriptors
            };
            return Some(QueryConflict {
                interval: overlap,
                clusters: vec![current.cluster_id, next.cluster_id],
                descriptors,
            });
        }
    }

    None
}

pub fn build_golden_cache(
    store: &dyn RecordStore,
    clusters: &Clusters,
) -> std::collections::HashMap<ClusterId, Vec<GoldenDescriptor>> {
    let mut cache = std::collections::HashMap::new();
    for cluster in &clusters.clusters {
        cache.insert(cluster.id, golden_for_cluster(store, cluster));
    }
    cache
}

pub fn build_cluster_key_cache(
    store: &dyn RecordStore,
    clusters: &Clusters,
    ontology: &Ontology,
) -> std::collections::HashMap<ClusterId, ClusterKey> {
    cluster_keys_for_clusters(store, clusters, ontology)
}

pub fn build_record_to_cluster_map(
    clusters: &Clusters,
) -> std::collections::HashMap<RecordId, ClusterId> {
    let mut map = std::collections::HashMap::new();
    for cluster in &clusters.clusters {
        for record_id in &cluster.records {
            map.insert(*record_id, cluster.id);
        }
    }
    map
}

fn filter_golden_for_interval(
    golden: &[GoldenDescriptor],
    interval: Interval,
) -> Vec<GoldenDescriptor> {
    golden
        .iter()
        .filter_map(|descriptor| {
            crate::temporal::intersect(&descriptor.interval, &interval).map(|overlap| {
                GoldenDescriptor {
                    attr: descriptor.attr.clone(),
                    value: descriptor.value.clone(),
                    interval: overlap,
                }
            })
        })
        .collect()
}

fn offending_descriptors_for_clusters(
    store: &dyn RecordStore,
    clusters: &Clusters,
    descriptors: &[QueryDescriptor],
    interval: Interval,
    cluster_a: ClusterId,
    cluster_b: ClusterId,
) -> Vec<QueryDescriptorOverlap> {
    let records_a = clusters
        .clusters
        .iter()
        .find(|cluster| cluster.id == cluster_a)
        .map(|cluster| cluster.records.as_slice())
        .unwrap_or(&[]);
    let records_b = clusters
        .clusters
        .iter()
        .find(|cluster| cluster.id == cluster_b)
        .map(|cluster| cluster.records.as_slice())
        .unwrap_or(&[]);

    let mut offenders = Vec::new();
    for descriptor in descriptors {
        let overlap_a = descriptor_overlap(store, records_a, *descriptor, interval);
        let overlap_b = descriptor_overlap(store, records_b, *descriptor, interval);
        if let (Some(overlap_a), Some(overlap_b)) = (overlap_a, overlap_b) {
            if let Some(overlap) = crate::temporal::intersect(&overlap_a, &overlap_b) {
                offenders.push(QueryDescriptorOverlap {
                    descriptor: *descriptor,
                    interval: overlap,
                });
            }
        }
    }
    offenders
}

fn descriptor_overlap(
    store: &dyn RecordStore,
    records: &[RecordId],
    descriptor: QueryDescriptor,
    interval: Interval,
) -> Option<Interval> {
    let mut overlaps = Vec::new();
    for record_id in records {
        if let Some(record) = store.get_record(*record_id) {
            for record_descriptor in &record.descriptors {
                if record_descriptor.attr == descriptor.attr
                    && record_descriptor.value == descriptor.value
                    && crate::temporal::is_overlapping(&record_descriptor.interval, &interval)
                {
                    if let Some(overlap) =
                        crate::temporal::intersect(&record_descriptor.interval, &interval)
                    {
                        overlaps.push(overlap);
                    }
                }
            }
        }
    }
    if overlaps.is_empty() {
        None
    } else {
        Some(coalesce_intervals(&overlaps)[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::linker::build_clusters;
    use crate::model::{Descriptor, Record, RecordIdentity};
    use crate::ontology::{IdentityKey, Ontology, StrongIdentifier};
    use crate::store::Store;

    #[test]
    fn query_respects_strong_identifier_conflicts() {
        let mut store = Store::new();
        let mut ontology = Ontology::new();

        let ssn_attr = store.interner_mut().intern_attr("ssn");
        let name_attr = store.interner_mut().intern_attr("name");
        let ssn_value_a = store.interner_mut().intern_value("123-45-6789");
        let ssn_value_b = store.interner_mut().intern_value("987-65-4321");
        let name_value = store.interner_mut().intern_value("Ada");

        ontology.add_identity_key(IdentityKey::new(vec![name_attr], "name".to_string()));
        ontology.add_strong_identifier(StrongIdentifier::new(ssn_attr, "ssn".to_string()));

        let record1 = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
            vec![
                Descriptor::new(name_attr, name_value, Interval::new(0, 10).unwrap()),
                Descriptor::new(ssn_attr, ssn_value_a, Interval::new(0, 10).unwrap()),
            ],
        );
        let record2 = Record::new(
            RecordId(2),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "2".to_string()),
            vec![
                Descriptor::new(name_attr, name_value, Interval::new(5, 15).unwrap()),
                Descriptor::new(ssn_attr, ssn_value_b, Interval::new(5, 15).unwrap()),
            ],
        );

        store.add_records(vec![record1, record2]).unwrap();
        let clusters = build_clusters(&store, &ontology).unwrap();

        let result = query_master_entities(
            &store,
            &clusters,
            &ontology,
            &[QueryDescriptor {
                attr: name_attr,
                value: name_value,
            }],
            Interval::new(0, 20).unwrap(),
        );

        match result.unwrap() {
            QueryOutcome::Conflict(conflict) => {
                assert!(!conflict.clusters.is_empty());
                assert!(!conflict.descriptors.is_empty());
            }
            QueryOutcome::Matches(_) => {
                panic!("expected conflict for overlapping clusters");
            }
        }
    }

    #[test]
    fn query_identity_access_management_scenario() {
        let mut store = Store::new();
        let mut ontology = Ontology::new();

        let email_attr = store.interner_mut().intern_attr("email");
        let org_attr = store.interner_mut().intern_attr("org");
        let role_attr = store.interner_mut().intern_attr("role");

        let email_value = store.interner_mut().intern_value("alice@acme.example");
        let org_value = store.interner_mut().intern_value("acme");
        let role_admin = store.interner_mut().intern_value("admin");
        let role_viewer = store.interner_mut().intern_value("viewer");

        ontology.add_identity_key(IdentityKey::new(vec![email_attr], "email".to_string()));

        let record1 = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "okta".to_string(), "u1".to_string()),
            vec![
                Descriptor::new(email_attr, email_value, Interval::new(0, 30).unwrap()),
                Descriptor::new(org_attr, org_value, Interval::new(0, 30).unwrap()),
                Descriptor::new(role_attr, role_admin, Interval::new(10, 20).unwrap()),
                Descriptor::new(role_attr, role_viewer, Interval::new(20, 30).unwrap()),
            ],
        );
        let record2 = Record::new(
            RecordId(2),
            RecordIdentity::new("person".to_string(), "ldap".to_string(), "u1".to_string()),
            vec![
                Descriptor::new(email_attr, email_value, Interval::new(0, 30).unwrap()),
                Descriptor::new(org_attr, org_value, Interval::new(0, 30).unwrap()),
                Descriptor::new(role_attr, role_admin, Interval::new(12, 18).unwrap()),
            ],
        );

        store.add_records(vec![record1, record2]).unwrap();
        let clusters = build_clusters(&store, &ontology).unwrap();

        let admin_query = query_master_entities(
            &store,
            &clusters,
            &ontology,
            &[
                QueryDescriptor {
                    attr: org_attr,
                    value: org_value,
                },
                QueryDescriptor {
                    attr: role_attr,
                    value: role_admin,
                },
            ],
            Interval::new(0, 30).unwrap(),
        )
        .unwrap();

        match admin_query {
            QueryOutcome::Matches(matches) => {
                assert_eq!(matches.len(), 1);
                assert_eq!(matches[0].interval, Interval::new(10, 20).unwrap());
                assert!(
                    matches[0].golden.iter().any(|desc| desc.attr == "role"),
                    "golden copy should include role descriptors"
                );
            }
            QueryOutcome::Conflict(_) => {
                panic!("unexpected conflict in IAM admin query");
            }
        }

        let viewer_query = query_master_entities(
            &store,
            &clusters,
            &ontology,
            &[QueryDescriptor {
                attr: role_attr,
                value: role_viewer,
            }],
            Interval::new(0, 30).unwrap(),
        )
        .unwrap();

        match viewer_query {
            QueryOutcome::Matches(matches) => {
                assert_eq!(matches.len(), 1);
                assert_eq!(matches[0].interval, Interval::new(20, 30).unwrap());
                assert!(
                    matches[0].golden.iter().any(|desc| desc.attr == "role"),
                    "golden copy should include role descriptors"
                );
            }
            QueryOutcome::Conflict(_) => {
                panic!("unexpected conflict in IAM viewer query");
            }
        }
    }
}
