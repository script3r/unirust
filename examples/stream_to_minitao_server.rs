use std::time::Duration;

use minitao::proto::minitao_service_client::MinitaoServiceClient;
use minitao::proto::{
    AssocCountRequest, AssocRangeRequest, ObjectGetRequest,
};
use unirust_rs::minitao_grpc::MinitaoGrpcWriter;
use unirust_rs::minitao_store::{
    record_object_id, ASSOC_TYPE_CONFLICT_EDGE, ASSOC_TYPE_SAME_AS,
};
use unirust_rs::ontology::{Constraint, IdentityKey, Ontology};
use unirust_rs::{Descriptor, Interval, Record, RecordId, RecordIdentity, Store, Unirust};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("MINITAO_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let mut writer = connect_writer_with_retry(addr.clone()).await?;
    let mut client = connect_client_with_retry(addr).await?;

    let mut ontology = Ontology::new();
    let mut store = Store::new();

    let name_attr = store.interner_mut().intern_attr("name");
    let email_attr = store.interner_mut().intern_attr("email");
    let phone_attr = store.interner_mut().intern_attr("phone");

    let identity_key = IdentityKey::new(vec![name_attr, email_attr], "name_email".to_string());
    ontology.add_identity_key(identity_key);

    // Optional constraint: lets conflicts show up for phone attribute.
    let phone_constraint = Constraint::unique(phone_attr, "unique_phone".to_string());
    ontology.add_constraint(phone_constraint);

    let name_value = store.interner_mut().intern_value("Ada Lovelace");
    let email_value = store.interner_mut().intern_value("ada@example.com");
    let phone_value_a = store.interner_mut().intern_value("555-1111");
    let phone_value_b = store.interner_mut().intern_value("555-2222");

    let record1 = Record::new(
        RecordId(1),
        RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "crm_001".to_string(),
        ),
        vec![
            Descriptor::new(name_attr, name_value, Interval::new(0, 10)?),
            Descriptor::new(email_attr, email_value, Interval::new(0, 10)?),
            Descriptor::new(phone_attr, phone_value_a, Interval::new(0, 10)?),
        ],
    );
    let record2 = Record::new(
        RecordId(2),
        RecordIdentity::new(
            "person".to_string(),
            "erp".to_string(),
            "erp_001".to_string(),
        ),
        vec![
            Descriptor::new(name_attr, name_value, Interval::new(5, 15)?),
            Descriptor::new(email_attr, email_value, Interval::new(5, 15)?),
            Descriptor::new(phone_attr, phone_value_b, Interval::new(5, 15)?),
        ],
    );

    let mut unirust = Unirust::with_store(ontology, store);

    // First update: single record, no edges.
    let update1 = unirust.stream_record_update_graph(record1)?;
    writer.apply_graph(&update1.graph).await?;
    verify_state(&mut client, update1.assignment.record_id, 0, 0).await?;

    // Second update: same_as + conflict edges should appear.
    let update2 = unirust.stream_record_update_graph(record2)?;
    writer.apply_graph(&update2.graph).await?;
    verify_state(&mut client, update2.assignment.record_id, 0, 2).await?;

    let record1_object = record_object_id(RecordId(1));
    let same_as_count = client
        .assoc_count(AssocCountRequest {
            id1: record1_object,
            atype: ASSOC_TYPE_SAME_AS,
        })
        .await?
        .into_inner()
        .count;
    let conflict_count = client
        .assoc_count(AssocCountRequest {
            id1: record1_object,
            atype: ASSOC_TYPE_CONFLICT_EDGE,
        })
        .await?
        .into_inner()
        .count;

    println!("record1 same_as={} conflicts={}", same_as_count, conflict_count);

    let conflict_assocs = client
        .assoc_range(AssocRangeRequest {
            id1: record1_object,
            atype: ASSOC_TYPE_CONFLICT_EDGE,
            pos: 0,
            limit: 10,
        })
        .await?
        .into_inner()
        .assocs;

    println!("record1 conflict assocs: {}", conflict_assocs.len());
    for assoc in conflict_assocs {
        println!("  conflict assoc id2={} time={}", assoc.id2, assoc.time);
    }

    println!("minitao streaming verification complete");

    Ok(())
}

async fn connect_writer_with_retry(addr: String) -> anyhow::Result<MinitaoGrpcWriter> {
    let mut last_err: Option<anyhow::Error> = None;
    for _ in 0..60 {
        match MinitaoGrpcWriter::connect(addr.clone()).await {
            Ok(writer) => return Ok(writer),
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("unable to connect to minitao")))
}

async fn connect_client_with_retry(
    addr: String,
) -> anyhow::Result<MinitaoServiceClient<tonic::transport::Channel>> {
    let mut last_err: Option<anyhow::Error> = None;
    for _ in 0..60 {
        match MinitaoServiceClient::connect(addr.clone()).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                last_err = Some(err.into());
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("unable to connect to minitao")))
}

async fn verify_state(
    client: &mut MinitaoServiceClient<tonic::transport::Channel>,
    record_id: RecordId,
    same_as_expected: u32,
    conflict_expected: u32,
) -> anyhow::Result<()> {
    let object_id = record_object_id(record_id);
    let object = client
        .object_get(ObjectGetRequest { id: object_id })
        .await?
        .into_inner()
        .object;

    if object.is_none() {
        anyhow::bail!("missing object for record {}", record_id.0);
    }

    let same_as_count = client
        .assoc_count(AssocCountRequest {
            id1: object_id,
            atype: ASSOC_TYPE_SAME_AS,
        })
        .await?
        .into_inner()
        .count;

    let conflict_count = client
        .assoc_count(AssocCountRequest {
            id1: object_id,
            atype: ASSOC_TYPE_CONFLICT_EDGE,
        })
        .await?
        .into_inner()
        .count;

    if same_as_count != same_as_expected {
        anyhow::bail!(
            "same_as count mismatch for record {}: expected {}, got {}",
            record_id.0,
            same_as_expected,
            same_as_count
        );
    }

    if conflict_count != conflict_expected {
        anyhow::bail!(
            "conflict count mismatch for record {}: expected {}, got {}",
            record_id.0,
            conflict_expected,
            conflict_count
        );
    }

    Ok(())
}
