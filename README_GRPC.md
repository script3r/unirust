# Unirust gRPC API

This document describes the gRPC API for the Unirust entity mastering engine, which provides the same functionality as the basic examples but through a network-accessible service.

## Overview

The gRPC API implements the complete Unirust workflow:
1. **Ontology Creation** - Define entity matching rules, constraints, and perspective weights
2. **Record Ingestion** - Add temporal records from multiple data sources
3. **Cluster Building** - Perform entity resolution to group related records
4. **Conflict Detection** - Identify temporal and logical conflicts in the data
5. **Knowledge Graph Export** - Generate knowledge graphs and export in multiple formats

## Architecture

```
┌─────────────┐    gRPC     ┌─────────────────┐    ┌─────────────────┐
│  gRPC Client│ ◄────────► │  gRPC Server    │───►│ Unirust Engine  │
│             │             │                 │    │                 │
│ - Basic     │             │ - Session Mgmt  │    │ - Entity Resolution │
│ - Client    │             │ - API Endpoints │    │ - Conflict Detection │
│ - Example   │             │ - Type Convert  │    │ - Knowledge Export  │
└─────────────┘             └─────────────────┘    └─────────────────┘
```

## Running the Server and Client

### 1. Start the gRPC Server

```bash
# Start the server (listens on 127.0.0.1:50051)
cargo run --example grpc_server
```

### 2. Run the Client Example

```bash
# In another terminal, run the client example
cargo run --example grpc_client_example
```

## API Reference

### Service Definition

```protobuf
service UnirustService {
    // Health and session management
    rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
    rpc CreateOntology(CreateOntologyRequest) returns (CreateOntologyResponse);
    rpc GetSessionInfo(GetSessionInfoRequest) returns (GetSessionInfoResponse);
    rpc ListSessions(ListSessionsRequest) returns (ListSessionsResponse);
    rpc DeleteSession(DeleteSessionRequest) returns (DeleteSessionResponse);
    
    // Core workflow methods
    rpc IngestRecords(IngestRecordsRequest) returns (IngestRecordsResponse);
    rpc BuildClusters(BuildClustersRequest) returns (BuildClustersResponse);
    rpc DetectConflicts(DetectConflictsRequest) returns (DetectConflictsResponse);
    rpc ExportKnowledgeGraph(ExportKnowledgeGraphRequest) returns (ExportKnowledgeGraphResponse);
    rpc ExportFormats(ExportFormatsRequest) returns (ExportFormatsResponse);
}
```

### Core Data Types

#### Record
Represents a temporal entity record from a specific perspective:
```protobuf
message Record {
    uint32 id = 1;
    RecordIdentity identity = 2;
    repeated Descriptor descriptors = 3;
}

message RecordIdentity {
    string entity_type = 1;    // e.g., "person", "organization"
    string perspective = 2;    // e.g., "crm", "erp", "web"
    string uid = 3;           // unique ID within perspective
}

message Descriptor {
    string attribute = 1;      // attribute name, e.g., "name", "email"
    string value = 2;         // attribute value
    Interval interval = 3;    // temporal validity interval
}
```

#### Ontology
Defines the rules for entity matching and conflict resolution:
```protobuf
message Ontology {
    repeated IdentityKey identity_keys = 1;
    repeated StrongIdentifier strong_identifiers = 2;
    repeated Constraint constraints = 3;
    map<string, uint32> perspective_weights = 6;
    // ... other fields
}
```

### Workflow Example

Here's a complete workflow using the gRPC API:

#### 1. Create Ontology and Session
```rust
let ontology = Ontology {
    identity_keys: vec![IdentityKey {
        attributes: vec!["name".to_string(), "email".to_string()],
        name: "name_email".to_string(),
    }],
    strong_identifiers: vec![StrongIdentifier {
        attribute: "ssn".to_string(),
        name: "ssn_unique".to_string(),
    }],
    constraints: vec![Constraint {
        constraint_type: Some(constraint::ConstraintType::Unique(UniqueConstraint {
            attribute: "email".to_string(),
            name: "unique_email".to_string(),
        })),
    }],
    perspective_weights: HashMap::from([
        ("crm".to_string(), 100),
        ("erp".to_string(), 90),
    ]),
    // ...
};

let response = client.create_ontology(CreateOntologyRequest {
    ontology: Some(ontology),
}).await?;

let session_id = response.get_ref().session_id.clone();
```

#### 2. Ingest Records
```rust
let records = vec![
    Record {
        id: 1,
        identity: Some(RecordIdentity {
            entity_type: "person".to_string(),
            perspective: "crm".to_string(),
            uid: "crm_001".to_string(),
        }),
        descriptors: vec![
            Descriptor {
                attribute: "name".to_string(),
                value: "John Doe".to_string(),
                interval: Some(Interval { start: 100, end: 200 }),
            },
            // ... more descriptors
        ],
    },
    // ... more records
];

client.ingest_records(IngestRecordsRequest {
    session_id: session_id.clone(),
    records,
}).await?;
```

#### 3. Build Clusters
```rust
let clusters_response = client.build_clusters(BuildClustersRequest {
    session_id: session_id.clone(),
    use_optimized: true,
}).await?;

let clusters = clusters_response.get_ref().clusters.as_ref().unwrap();
```

#### 4. Detect Conflicts
```rust
let conflicts_response = client.detect_conflicts(DetectConflictsRequest {
    session_id: session_id.clone(),
    clusters: Some(clusters.clone()),
}).await?;

let observations = &conflicts_response.get_ref().observations;
```

#### 5. Export Data
```rust
// Export in multiple formats
let export_response = client.export_formats(ExportFormatsRequest {
    session_id: session_id.clone(),
    clusters: Some(clusters.clone()),
    observations: observations.clone(),
    formats: vec![
        "jsonl".to_string(),
        "dot".to_string(),
        "text_summary".to_string(),
    ],
}).await?;
```

## Key Features

### Session Management
- Each client interaction creates a unique session
- Sessions maintain state (ontology, records, indices)
- Multiple concurrent sessions supported
- Proper cleanup with session deletion

### Temporal Modeling
- All records have temporal descriptors with validity intervals
- Conflicts detected across temporal dimensions
- Support for temporal reasoning and temporal joins

### Multi-Perspective Support
- Records from different source systems ("perspectives")
- Configurable perspective weights for conflict resolution
- Authoritative attributes per perspective

### Conflict Detection
- **Direct Conflicts**: Value conflicts within the same cluster
- **Indirect Conflicts**: Suppressed merges due to constraint violations
- **Merge Observations**: Successful entity linkages with reasoning

### Export Formats
- **JSONL**: Machine-readable knowledge graph format
- **DOT**: GraphViz visualization format
- **Text Summary**: Human-readable summary of results

## Error Handling

The API uses standard gRPC status codes:
- `NOT_FOUND`: Session doesn't exist
- `INVALID_ARGUMENT`: Missing or invalid request parameters
- `INTERNAL`: Server-side processing errors

## Performance Considerations

- Use `use_optimized: true` for cluster building on large datasets
- Sessions are kept in memory - delete sessions when done
- Consider chunking large record sets for ingestion

## Comparison with Basic Example

The gRPC API provides the same functionality as `basic_example.rs` but with these advantages:
- **Network accessible**: Call from any gRPC-compatible client
- **Language agnostic**: Use from Python, Java, Go, etc.
- **Stateful sessions**: Maintain context across multiple operations
- **Concurrent**: Multiple clients can work simultaneously
- **Production ready**: Proper error handling and resource management

See `examples/grpc_client_example.rs` for a complete working example that mirrors the workflow in `examples/basic_example.rs`.