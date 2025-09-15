# Unirust

<div align="center">
  <img src="unirust.png" alt="Unirust Logo" width="350" height="350">
</div>

A general-purpose, temporal-first entity mastering and conflict-resolution engine written in Rust.

## Overview

Unirust provides precise temporal modeling, entity resolution, and conflict detection with strong guarantees about temporal correctness and auditability. It's designed to handle complex entity mastering scenarios where data comes from multiple sources with different perspectives and temporal validity periods.

## Motivation

Traditional entity resolution algorithms ignore temporal information, leading to incorrect merges and lost historical context. When data from multiple sources arrives at different times or has overlapping validity periods, standard approaches fail to maintain temporal consistency.

### Use Cases

**Customer Data Management**
- Merge CRM, e-commerce, and support records while preserving interaction timelines
- Resolve conflicts when customers change contact information over time
- Maintain accurate customer journey mapping across touchpoints

**Financial Services**
- Consolidate trading accounts and positions from multiple systems with different update frequencies
- Track entity relationships and ownership changes for compliance and risk management
- Detect suspicious patterns through temporal entity evolution analysis

**Identity and Access Management**
- Merge user identities across systems while maintaining access history
- Resolve conflicts when user attributes change at different times
- Provide complete audit trails for security governance

**Healthcare Systems**
- Merge patient records from different hospitals while preserving medical history integrity
- Resolve conflicts when patient information is updated asynchronously
- Ensure temporal consistency for critical medical decisions

**Master Data Management**
- Create golden records that preserve temporal context and source attribution
- Handle data quality issues from asynchronous updates across systems
- Maintain lineage and provenance for regulatory compliance

## Features

- **Temporal Model**: Precise interval-based time modeling with Allen's interval relations
- **High-Performance Entity Resolution**: Optimized O(n) blocking algorithm with parallel processing
- **Conflict Detection**: Direct and indirect conflict detection with detailed reporting
- **Knowledge Graph Export**: JSONL, DOT, PNG, and SVG export formats
- **Perspective Support**: Multi-perspective data handling with configurable weights
- **Audit Trail**: Complete traceability of merges and conflicts
- **Scalable Architecture**: Designed to handle millions of entities with streaming and blocking
- **Parallel Processing**: Adaptive parallelization for loosely coupled entities

## Quick Start

### Prerequisites

- Rust 1.70+
- Graphviz (optional, for visualization with `dot` command)
- Git (for cloning and development)

### Installation

```bash
git clone https://github.com/unirust/unirust.git
cd unirust
cargo build
```

Or add to your `Cargo.toml`:
```toml
[dependencies]
unirust = "0.1.0"
```

### Running the Example

```bash
cargo run --example basic_example
```

This will demonstrate:
- Creating an ontology with identity keys and constraints
- Adding records from multiple perspectives
- Building clusters through optimized entity resolution
- Detecting conflicts
- Exporting knowledge graphs in multiple formats

### Running Benchmarks

```bash
cargo bench --bench entity_benchmark
```

This will run performance benchmarks with different entity counts (1000, 5000) and overlap probabilities (1%, 10%, 30%).

## Architecture

### Core Modules

- **`temporal`**: Interval arithmetic and temporal relations
- **`model`**: Core data structures (Record, Descriptor, etc.)
- **`ontology`**: Identity keys, strong identifiers, and constraints
- **`dsu`**: Union-Find with temporal guards
- **`linker`**: High-performance entity resolution with parallel processing and blocking
- **`conflicts`**: Conflict detection and reporting
- **`graph`**: Knowledge graph export
- **`store`**: Record storage and indexing
- **`utils`**: Visualization and export utilities

### Key Concepts

- **Records**: Temporal entities with descriptors and identity information
- **Clusters**: Groups of records representing the same logical entity
- **Identity Keys**: Attributes that must match for records to be considered the same entity
- **Strong Identifiers**: Attributes that prevent merging when they conflict
- **Temporal Guards**: Validation that merges only occur when temporal constraints are satisfied

## Usage

### Basic Entity Resolution

```rust
use unirust::*;

// Create ontology
let mut ontology = Ontology::new();
let name_attr = AttrId(0);
let email_attr = AttrId(1);
let identity_key = IdentityKey::new(vec![name_attr, email_attr], "name_email".to_string());
ontology.add_identity_key(identity_key);

// Create store and add records
let mut store = Store::new();
// ... add records ...

// Build clusters
let clusters = linker::build_clusters(&store, &ontology)?;

// Detect conflicts
let observations = conflicts::detect_conflicts(&store, &clusters, &ontology)?;

// Export knowledge graph
let graph = graph::export_graph(&store, &clusters, &observations, &ontology)?;
```

### Visualization

The library includes utilities for generating visual representations:

```rust
use unirust::utils;

// Export to DOT format
let dot = utils::export_to_dot(&store, &clusters, &observations, &ontology)?;

// Generate PNG/SVG visualizations
utils::generate_graph_visualizations(&store, &clusters, &observations, &ontology, "output")?;
```

## Performance

The linker uses several optimization techniques for high performance:

- **Blocking Algorithm**: Reduces O(n²) complexity to O(n) by grouping records by identity key values
- **Streaming Processing**: Processes edges in streams to maintain O(1) memory usage
- **Parallel Processing**: Adaptive parallelization for loosely coupled entities with high overlap
- **Smart Thresholding**: Automatically chooses sequential vs parallel processing based on block size

Benchmark results demonstrate improved performance, particularly for high-overlap scenarios with parallel processing.

## Examples

See the `examples/` directory for complete working examples:

- **`basic_example.rs`**: Simple entity resolution with conflict detection and visualization

## Development

### Running Tests

```bash
cargo test
```

### Building Documentation

```bash
cargo doc --open
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on how to:

- Report bugs and request features
- Set up a development environment
- Submit pull requests
- Follow our coding standards

### Quick Start for Contributors

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Run the test suite: `cargo test`
5. Commit your changes: `git commit -m 'Add amazing feature'`
6. Push to your branch: `git push origin feature/amazing-feature`
7. Open a Pull Request

## Support

- 📖 [Documentation](https://docs.rs/unirust) (coming soon)
- 🐛 [Issue Tracker](https://github.com/unirust/unirust/issues)
- 💬 [Discussions](https://github.com/unirust/unirust/discussions) (coming soon)

## Acknowledgments

- Inspired by temporal entity resolution research
- Built with the Rust ecosystem and community
- Thanks to all contributors who help improve Unirust