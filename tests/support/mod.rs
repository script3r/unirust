use unirust_rs::distributed::{ConstraintConfig, ConstraintKind, DistributedOntologyConfig, IdentityKeyConfig};
use unirust_rs::distributed::proto::{
    ConstraintConfig as ProtoConstraintConfig, ConstraintKind as ProtoConstraintKind,
    IdentityKeyConfig as ProtoIdentityKeyConfig, OntologyConfig,
};

#[allow(dead_code)]
pub fn build_iam_config() -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: vec![IdentityKeyConfig {
            name: "email".to_string(),
            attributes: vec!["email".to_string()],
        }],
        strong_identifiers: vec!["ssn".to_string()],
        constraints: vec![ConstraintConfig {
            name: "unique_email".to_string(),
            attribute: "email".to_string(),
            kind: ConstraintKind::Unique,
        }],
    }
}

#[allow(dead_code)]
pub fn to_proto_config(config: &DistributedOntologyConfig) -> OntologyConfig {
    OntologyConfig {
        identity_keys: config
            .identity_keys
            .iter()
            .map(|entry| ProtoIdentityKeyConfig {
                name: entry.name.clone(),
                attributes: entry.attributes.clone(),
            })
            .collect(),
        strong_identifiers: config.strong_identifiers.clone(),
        constraints: config
            .constraints
            .iter()
            .map(|entry| ProtoConstraintConfig {
                name: entry.name.clone(),
                attribute: entry.attribute.clone(),
                kind: match entry.kind {
                    ConstraintKind::Unique => ProtoConstraintKind::Unique.into(),
                    ConstraintKind::UniqueWithinPerspective => {
                        ProtoConstraintKind::UniqueWithinPerspective.into()
                    }
                },
            })
            .collect(),
    }
}
