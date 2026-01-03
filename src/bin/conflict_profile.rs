//! Profiling binary for conflict detection with cluster_size=5000

use std::time::Instant;
use unirust_rs::dsu::{Cluster, Clusters};
use unirust_rs::model::{ClusterId, Descriptor, Record, RecordId, RecordIdentity};
use unirust_rs::ontology::{Ontology, StrongIdentifier};
use unirust_rs::store::RecordStore;
use unirust_rs::temporal::Interval;
use unirust_rs::Store;

fn main() {
    let cluster_size = 5000usize;
    let cluster_count = 100usize;

    println!("=== Conflict Detection Profiler ===\n");
    println!(
        "Configuration: {} clusters × {} records = {} total\n",
        cluster_count,
        cluster_size,
        cluster_count * cluster_size
    );

    let (store, clusters, ontology) = setup(cluster_size, cluster_count);

    // Warm up
    let _ = unirust_rs::conflicts::detect_conflicts(&store, &clusters, &ontology).unwrap();

    println!("--- Baseline ---\n");
    let start = Instant::now();
    let result = unirust_rs::conflicts::detect_conflicts(&store, &clusters, &ontology).unwrap();
    println!(
        "Full detect_conflicts: {:?} ({} observations)\n",
        start.elapsed(),
        result.len()
    );

    // Profile store.get_record performance
    println!("--- Store Access Patterns ---\n");

    let start = Instant::now();
    let mut record_count = 0;
    let mut descriptor_count = 0;
    for cluster in &clusters.clusters {
        for record_id in &cluster.records {
            if let Some(record) = store.get_record(*record_id) {
                record_count += 1;
                descriptor_count += record.descriptors.len();
            }
        }
    }
    let get_record_time = start.elapsed();
    println!(
        "store.get_record() × {}: {:?}",
        record_count, get_record_time
    );
    println!("  Per record: {:?}", get_record_time / record_count as u32);
    println!("  Descriptors accessed: {}\n", descriptor_count);

    // Profile for_each_record vs individual get_record
    let start = Instant::now();
    let mut count = 0;
    store.for_each_record(&mut |_record| {
        count += 1;
    });
    let for_each_time = start.elapsed();
    println!("store.for_each_record() × {}: {:?}", count, for_each_time);
    println!("  Per record: {:?}\n", for_each_time / count as u32);

    // Profile is_strong_identifier
    let start = Instant::now();
    let mut checks = 0;
    store.for_each_record(&mut |record| {
        for descriptor in &record.descriptors {
            let _ = ontology.is_strong_identifier(&record.identity.entity_type, descriptor.attr);
            checks += 1;
        }
    });
    let strong_id_time = start.elapsed();
    println!("is_strong_identifier() × {}: {:?}", checks, strong_id_time);
    println!("  Per check: {:?}\n", strong_id_time / checks as u32);

    // Profile HashMap operations with realistic data
    println!("--- HashMap Performance ---\n");
    profile_hashmap_perf(cluster_size);

    // Profile memory allocation patterns
    println!("\n--- Allocation Patterns ---\n");
    profile_allocations(cluster_size);

    // Profile the sweep line with real conflict output
    println!("\n--- Sweep Line with Output ---\n");
    profile_sweep_with_output(cluster_size);
}

fn setup(cluster_size: usize, cluster_count: usize) -> (Store, Clusters, Ontology) {
    let mut store = Store::new();
    let attr = store.interner_mut().intern_attr("name");
    let strong_attr = store.interner_mut().intern_attr("ssn");

    let mut clusters_vec = Vec::new();
    for cluster_idx in 0..cluster_count {
        let base_id = (cluster_idx * cluster_size) as u32;
        let mut records = Vec::new();

        for i in 0..cluster_size {
            let record_id = RecordId(base_id + i as u32);
            let value = store
                .interner_mut()
                .intern_value(&format!("name_{}_{}", cluster_idx, i));
            let strong_val = store
                .interner_mut()
                .intern_value(&format!("ssn_{}", base_id + i as u32));

            let record = Record::new(
                record_id,
                RecordIdentity::new(
                    "person".to_string(),
                    "source".to_string(),
                    format!("uid_{}_{}", cluster_idx, i),
                ),
                vec![
                    Descriptor::new(attr, value, Interval::new(0, 100).unwrap()),
                    Descriptor::new(strong_attr, strong_val, Interval::new(0, 100).unwrap()),
                ],
            );
            store.add_record(record).unwrap();
            records.push(record_id);
        }

        clusters_vec.push(Cluster::new(
            ClusterId(cluster_idx as u32),
            RecordId(base_id),
            records,
        ));
    }

    let clusters = Clusters {
        clusters: clusters_vec,
    };
    let mut ontology = Ontology::new();
    ontology.add_strong_identifier(StrongIdentifier::new(strong_attr, "ssn".to_string()));

    (store, clusters, ontology)
}

fn profile_hashmap_perf(n: usize) {
    use hashbrown::HashMap;
    use unirust_rs::model::ValueId;

    // Simulate grouping n descriptors by value (all unique values)
    let start = Instant::now();
    let mut map: HashMap<ValueId, Vec<usize>> = HashMap::with_capacity(n);
    for i in 0..n {
        map.entry(ValueId(i as u32))
            .or_insert_with(|| Vec::with_capacity(2))
            .push(i);
    }
    let insert_time = start.elapsed();
    println!("HashMap insert {} unique keys: {:?}", n, insert_time);

    // Simulate iteration
    let start = Instant::now();
    let mut sum = 0;
    for (_, v) in &map {
        sum += v.len();
    }
    let iter_time = start.elapsed();
    println!(
        "HashMap iterate {} entries: {:?} (sum={})",
        n, iter_time, sum
    );

    // Simulate consuming iteration
    let start = Instant::now();
    let mut sum = 0;
    for (_, v) in map {
        sum += v.len();
    }
    let consume_time = start.elapsed();
    println!(
        "HashMap consume {} entries: {:?} (sum={})",
        n, consume_time, sum
    );
}

fn profile_allocations(n: usize) {
    // Profile Vec allocation patterns
    let start = Instant::now();
    let mut vecs: Vec<Vec<usize>> = Vec::with_capacity(n);
    for i in 0..n {
        vecs.push(vec![i]); // Single-element vec (common case)
    }
    let single_vec_time = start.elapsed();
    println!("Allocate {} single-element Vecs: {:?}", n, single_vec_time);
    drop(vecs);

    // Profile with pre-sized vecs
    let start = Instant::now();
    let mut vecs: Vec<Vec<usize>> = Vec::with_capacity(n);
    for i in 0..n {
        vecs.push(vec![i]);
    }
    let presized_time = start.elapsed();
    println!("Allocate {} pre-sized Vecs: {:?}", n, presized_time);
    drop(vecs);

    // Profile tuple events (no heap allocation per event)
    let start = Instant::now();
    let mut events: Vec<(i64, bool, u32)> = Vec::with_capacity(n * 2);
    for i in 0..n {
        events.push((0, true, i as u32));
        events.push((100, false, i as u32));
    }
    let tuple_time = start.elapsed();
    println!("Allocate {} event tuples: {:?}", n * 2, tuple_time);
    drop(events);

    // Profile sorting
    let mut events: Vec<(i64, bool, u32)> = Vec::with_capacity(n * 2);
    for i in 0..n {
        events.push((0, true, i as u32));
        events.push((100, false, i as u32));
    }
    let start = Instant::now();
    events.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let sort_time = start.elapsed();
    println!("Sort {} events: {:?}", n * 2, sort_time);
}

fn profile_sweep_with_output(n: usize) {
    use hashbrown::HashMap;

    // Create events (all same timestamp = many conflicts)
    let mut events: Vec<(i64, bool, u32, u32)> = Vec::with_capacity(n * 2);
    for i in 0..n {
        events.push((0, true, i as u32, i as u32)); // (time, is_start, value_id, record_id)
        events.push((100, false, i as u32, i as u32));
    }
    events.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    // Sweep with conflict counting (no output allocation)
    let start = Instant::now();
    let mut active: HashMap<u32, u32> = HashMap::new();
    let mut conflict_count = 0;
    let mut last_time = None;
    for (time, is_start, value, _record) in &events {
        if let Some(lt) = last_time {
            if *time > lt && active.len() > 1 {
                conflict_count += 1;
            }
        }
        if *is_start {
            *active.entry(*value).or_insert(0) += 1;
        } else if let Some(c) = active.get_mut(value) {
            *c -= 1;
            if *c == 0 {
                active.remove(value);
            }
        }
        last_time = Some(*time);
    }
    let sweep_only_time = start.elapsed();
    println!(
        "Sweep line (count only): {:?} ({} conflicts)",
        sweep_only_time, conflict_count
    );

    // Sweep with output allocation (simulating real behavior)
    let start = Instant::now();
    let mut active: HashMap<u32, Vec<u32>> = HashMap::new();
    type ConflictEntry = (i64, i64, Vec<(u32, Vec<u32>)>);
    let mut conflicts: Vec<ConflictEntry> = Vec::new();
    let mut last_time: Option<i64> = None;
    for (time, is_start, value, record) in &events {
        if let Some(lt) = last_time {
            if *time > lt && active.len() > 1 {
                // Create conflict output (this is expensive!)
                let values: Vec<(u32, Vec<u32>)> =
                    active.iter().map(|(v, r)| (*v, r.clone())).collect();
                conflicts.push((lt, *time, values));
            }
        }
        if *is_start {
            active.entry(*value).or_default().push(*record);
        } else {
            active.remove(value);
        }
        last_time = Some(*time);
    }
    let sweep_output_time = start.elapsed();
    println!(
        "Sweep line (with output): {:?} ({} conflicts)",
        sweep_output_time,
        conflicts.len()
    );

    // The difference shows the cost of conflict output creation
    println!(
        "Output allocation overhead: {:?}",
        sweep_output_time.saturating_sub(sweep_only_time)
    );
}
