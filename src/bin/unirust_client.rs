use std::fs;
use std::path::PathBuf;

use tonic::transport::Endpoint;
use unirust_rs::distributed::proto::router_service_client::RouterServiceClient;
use unirust_rs::distributed::proto::{
    ApplyOntologyRequest, ConstraintConfig, ConstraintKind, IdentityKeyConfig,
    IngestRecordsRequest, OntologyConfig, QueryDescriptor, QueryEntitiesRequest, RecordDescriptor,
    RecordIdentity, RecordInput,
};
use unirust_rs::distributed::DistributedOntologyConfig;
use unirust_rs::transport_security::load_client_mtls;

fn parse_arg(flag: &str) -> Option<String> {
    let mut args = std::env::args();
    while let Some(arg) = args.next() {
        if arg == flag {
            return args.next();
        }
    }
    None
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let router = parse_arg("--router").unwrap_or_else(|| "http://127.0.0.1:50060".to_string());
    let ontology_path =
        parse_arg("--ontology").ok_or_else(|| anyhow::anyhow!("--ontology is required"))?;
    let tls_ca = parse_arg("--tls-ca").map(PathBuf::from);
    let tls_cert = parse_arg("--tls-cert").map(PathBuf::from);
    let tls_key = parse_arg("--tls-key").map(PathBuf::from);
    let mtls = load_client_mtls(tls_ca.as_deref(), tls_cert.as_deref(), tls_key.as_deref())?;
    if mtls.is_some() && router.starts_with("http://") {
        anyhow::bail!("mTLS requires an https:// router address");
    }
    if mtls.is_none() && router.starts_with("https://") {
        anyhow::bail!("https:// router addresses require mTLS certificate options");
    }
    let endpoint = Endpoint::from_shared(router)?;
    let endpoint = if let Some(mtls) = mtls {
        endpoint.tls_config(mtls)?
    } else {
        endpoint
    };
    let mut client = RouterServiceClient::new(endpoint.connect().await?);

    let ontology = load_ontology(ontology_path)?;
    client
        .set_ontology(ApplyOntologyRequest {
            config: Some(to_proto_config(&ontology)),
        })
        .await?;

    let records = vec![
        RecordInput {
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
        },
        RecordInput {
            index: 1,
            identity: Some(RecordIdentity {
                entity_type: "person".to_string(),
                perspective: "crm".to_string(),
                uid: "crm_002".to_string(),
            }),
            descriptors: vec![
                RecordDescriptor {
                    attr: "email".to_string(),
                    value: "bob@example.com".to_string(),
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
        },
    ];

    let response = client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 0,
            records,
        })
        .await?
        .into_inner();
    println!("Assignments: {:?}", response.assignments);

    let match_query = QueryEntitiesRequest {
        descriptors: vec![QueryDescriptor {
            attr: "email".to_string(),
            value: "alice@example.com".to_string(),
        }],
        start: 0,
        end: 10,
    };
    let match_response = client.query_entities(match_query).await?.into_inner();
    println!("Match query response: {:?}", match_response);

    let conflict_query = QueryEntitiesRequest {
        descriptors: vec![QueryDescriptor {
            attr: "role".to_string(),
            value: "admin".to_string(),
        }],
        start: 0,
        end: 10,
    };
    let conflict_response = client.query_entities(conflict_query).await?.into_inner();
    println!("Conflict query response: {:?}", conflict_response);

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
