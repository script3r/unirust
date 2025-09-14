//! # Utilities Module
//! 
//! Common utility functions for data export, visualization, and other shared functionality.

use crate::Store;
use crate::dsu::Clusters;
use crate::conflicts::Observation;
use crate::ontology::Ontology;
use anyhow::Result;
use std::collections::HashSet;

/// Export the knowledge graph to DOT format for visualization
pub fn export_to_dot(
    store: &Store,
    clusters: &Clusters,
    observations: &[Observation],
    _ontology: &Ontology,
) -> Result<String> {
    let mut dot = String::new();
    
    // DOT header
    dot.push_str("digraph KnowledgeGraph {\n");
    dot.push_str("  rankdir=TB;\n");
    dot.push_str("  node [shape=box, style=filled];\n");
    dot.push_str("  edge [fontsize=10];\n\n");
    
    // Add cluster subgraphs
    for (cluster_id, cluster) in clusters.clusters.iter().enumerate() {
        dot.push_str(&format!("  subgraph cluster_{} {{\n", cluster_id));
        dot.push_str(&format!("    label=\"Cluster {}\";\n", cluster_id));
        dot.push_str("    style=filled;\n");
        dot.push_str("    color=lightgray;\n");
        
        // Add records in this cluster
        for record_id in &cluster.records {
            if let Some(_record) = store.get_record(*record_id) {
                dot.push_str(&format!("    \"{}\" [label=\"{}\", fillcolor=lightblue];\n", 
                    record_id, record_id));
            }
        }
        dot.push_str("  }\n\n");
    }
    
    // Add SAME_AS edges (green) - these are implicit from clusters
    for cluster in &clusters.clusters {
        if cluster.records.len() > 1 {
            for i in 0..cluster.records.len() {
                for j in (i+1)..cluster.records.len() {
                    dot.push_str(&format!("  \"{}\" -> \"{}\" [label=\"SAME_AS\", color=green, style=bold];\n", 
                        cluster.records[i], cluster.records[j]));
                }
            }
        }
    }
    
    // Add CONFLICTS_WITH edges (red) - simplified to avoid duplication
    let mut added_conflicts = HashSet::new();
    for observation in observations {
        match observation {
            Observation::DirectConflict(conflict) => {
                // Find records that have this conflict
                for cluster in &clusters.clusters {
                    if cluster.records.len() > 1 {
                        for i in 0..cluster.records.len() {
                            for j in (i+1)..cluster.records.len() {
                                let edge_key = format!("{}-{}-{}", cluster.records[i].0, cluster.records[j].0, conflict.attribute.0);
                                if !added_conflicts.contains(&edge_key) {
                                    dot.push_str(&format!("  \"{}\" -> \"{}\" [label=\"CONFLICTS\\nattr:{}\", color=red, style=dashed];\n", 
                                        cluster.records[i], cluster.records[j], conflict.attribute.0));
                                    added_conflicts.insert(edge_key);
                                }
                            }
                        }
                    }
                }
            }
            Observation::IndirectConflict(conflict) => {
                // For indirect conflicts, we'd need to determine which records are involved
                // This is a simplified representation
                dot.push_str(&format!("  // Indirect conflict: {}\n", conflict.cause));
            }
            _ => {}
        }
    }
    
    // Add attribute information as node labels
    dot.push_str("\n  // Record details:\n");
    for cluster in &clusters.clusters {
        for record_id in &cluster.records {
            if let Some(record) = store.get_record(*record_id) {
                let mut attr_info = String::new();
                for descriptor in &record.descriptors {
                    if let Some(_attr_name) = store.interner().get_attr(descriptor.attr) {
                        if let Some(value_name) = store.interner().get_value(descriptor.value) {
                            attr_info.push_str(&format!("{}\\n", value_name));
                        }
                    }
                }
                if !attr_info.is_empty() {
                    dot.push_str(&format!("  \"{}\" [label=\"{}\\n{}\\n{}\"];\n", 
                        record_id, record_id, record.identity.perspective, attr_info.trim_end_matches("\\n")));
                }
            }
        }
    }
    
    dot.push_str("}\n");
    Ok(dot)
}

/// Export the knowledge graph to a simplified text format
pub fn export_to_text_summary(
    store: &Store,
    clusters: &Clusters,
    observations: &[Observation],
) -> Result<String> {
    let mut summary = String::new();
    
    summary.push_str("Knowledge Graph Summary\n");
    summary.push_str("======================\n\n");
    
    // Cluster information
    summary.push_str(&format!("Total Clusters: {}\n", clusters.clusters.len()));
    summary.push_str(&format!("Total Records: {}\n", store.len()));
    summary.push_str(&format!("Total Observations: {}\n\n", observations.len()));
    
    // Cluster details
    for (cluster_id, cluster) in clusters.clusters.iter().enumerate() {
        summary.push_str(&format!("Cluster {} ({} records):\n", cluster_id, cluster.records.len()));
        
        for record_id in &cluster.records {
            if let Some(record) = store.get_record(*record_id) {
                summary.push_str(&format!("  - {} ({}:{})\n", 
                    record_id, record.identity.perspective, record.identity.uid));
            }
        }
        summary.push_str("\n");
    }
    
    // Observations summary
    let mut same_as_count = 0;
    let mut conflict_count = 0;
    
    for observation in observations {
        match observation {
            Observation::Merge { .. } => same_as_count += 1,
            Observation::DirectConflict(_) => conflict_count += 1,
            Observation::IndirectConflict(_) => conflict_count += 1,
        }
    }
    
    summary.push_str(&format!("Relationships:\n"));
    summary.push_str(&format!("  SAME_AS: {} relationships\n", same_as_count));
    summary.push_str(&format!("  CONFLICTS: {} relationships\n", conflict_count));
    
    Ok(summary)
}

/// Save DOT content to file
pub fn save_dot_to_file(dot_content: &str, filename: &str) -> Result<()> {
    std::fs::write(filename, dot_content)?;
    println!("DOT content saved to: {}", filename);
    Ok(())
}

/// Generate and save graph visualizations
pub fn generate_graph_visualizations(
    store: &Store,
    clusters: &Clusters,
    observations: &[Observation],
    ontology: &Ontology,
    base_filename: &str,
) -> Result<()> {
    // Generate DOT content
    let dot_content = export_to_dot(store, clusters, observations, ontology)?;
    
    // Save DOT file
    let dot_filename = format!("{}.dot", base_filename);
    save_dot_to_file(&dot_content, &dot_filename)?;
    
    // Generate PNG
    let png_filename = format!("{}.png", base_filename);
    let png_result = std::process::Command::new("dot")
        .args(&["-Tpng", &dot_filename, "-o", &png_filename])
        .output();
    
    match png_result {
        Ok(output) if output.status.success() => {
            println!("PNG visualization saved to: {}", png_filename);
        }
        Ok(output) => {
            eprintln!("Warning: Failed to generate PNG: {}", String::from_utf8_lossy(&output.stderr));
        }
        Err(_) => {
            eprintln!("Warning: 'dot' command not found. Install Graphviz to generate PNG visualizations.");
        }
    }
    
    // Generate SVG
    let svg_filename = format!("{}.svg", base_filename);
    let svg_result = std::process::Command::new("dot")
        .args(&["-Tsvg", &dot_filename, "-o", &svg_filename])
        .output();
    
    match svg_result {
        Ok(output) if output.status.success() => {
            println!("SVG visualization saved to: {}", svg_filename);
        }
        Ok(output) => {
            eprintln!("Warning: Failed to generate SVG: {}", String::from_utf8_lossy(&output.stderr));
        }
        Err(_) => {
            eprintln!("Warning: 'dot' command not found. Install Graphviz to generate SVG visualizations.");
        }
    }
    
    Ok(())
}
