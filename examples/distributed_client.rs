use std::fs;

use unirust_rs::distributed::proto::router_service_client::RouterServiceClient;
use unirust_rs::distributed::proto::{
    ApplyOntologyRequest, ConstraintConfig, ConstraintKind, IdentityKeyConfig,
    IngestRecordsRequest, OntologyConfig, QueryDescriptor, QueryEntitiesRequest, RecordDescriptor,
    RecordIdentity, RecordInput,
};
use unirust_rs::distributed::DistributedOntologyConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let ontology_path = args
        .iter()
        .position(|arg| arg == "--ontology")
        .and_then(|idx| args.get(idx + 1))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("--ontology is required"))?;

    let mut client = RouterServiceClient::connect("http://127.0.0.1:50060").await?;
    let ontology = load_ontology(ontology_path)?;
    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&ontology)),
        })
        .await?;

    let record = RecordInput {
        index: 0,
        identity: Some(RecordIdentity {
            entity_type: "person".to_string(),
            perspective: "crm".to_string(),
            uid: "crm_001".to_string(),
        }),
        descriptors: vec![
            RecordDescriptor {
                attr: "email".to_string(),
                value: "alice@example.com".to_string(),
                start: 0,
                end: 10,
            },
            RecordDescriptor {
                attr: "role".to_string(),
                value: "admin".to_string(),
                start: 0,
                end: 10,
            },
        ],
    };

    let response = client
        .ingest_records(IngestRecordsRequest {
            records: vec![record],
        })
        .await?
        .into_inner();
    println!("Assignments: {:?}", response.assignments);

    let query = QueryEntitiesRequest {
        descriptors: vec![QueryDescriptor {
            attr: "email".to_string(),
            value: "alice@example.com".to_string(),
        }],
        start: 0,
        end: 10,
    };

    let response = client.query_entities(query).await?.into_inner();
    println!("Query response: {:?}", response);

    Ok(())
}

fn load_ontology(path: String) -> anyhow::Result<DistributedOntologyConfig> {
    let raw = fs::read_to_string(path)?;
    let config = serde_json::from_str(&raw)?;
    Ok(config)
}

fn to_proto_config(config: &DistributedOntologyConfig) -> OntologyConfig {
    OntologyConfig {
        identity_keys: config
            .identity_keys
            .iter()
            .map(|entry| IdentityKeyConfig {
                name: entry.name.clone(),
                attributes: entry.attributes.clone(),
            })
            .collect(),
        strong_identifiers: config.strong_identifiers.clone(),
        constraints: config
            .constraints
            .iter()
            .map(|entry| ConstraintConfig {
                name: entry.name.clone(),
                attribute: entry.attribute.clone(),
                kind: match entry.kind {
                    unirust_rs::distributed::ConstraintKind::Unique => {
                        ConstraintKind::Unique.into()
                    }
                    unirust_rs::distributed::ConstraintKind::UniqueWithinPerspective => {
                        ConstraintKind::UniqueWithinPerspective.into()
                    }
                },
            })
            .collect(),
    }
}
