use std::collections::HashSet;

use anyhow::Context;
use minitao::model::{AssocType, MinitaoAssociation, MinitaoObject, ObjectId};
use minitao::proto::minitao_service_client::MinitaoServiceClient;
use minitao::proto::{
    AssocAddRequest, AssocDeleteRequest, Association, Object, ObjectAddRequest, ObjectDeleteRequest,
};
use tonic::transport::Channel;

use crate::graph::KnowledgeGraph;
use crate::minitao_store::build_minitao_payloads;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AssocKey {
    id1: ObjectId,
    atype: AssocType,
    id2: ObjectId,
}

pub struct MinitaoGrpcWriter {
    client: MinitaoServiceClient<Channel>,
    objects: HashSet<ObjectId>,
    assocs: HashSet<AssocKey>,
}

impl MinitaoGrpcWriter {
    pub async fn connect(addr: String) -> anyhow::Result<Self> {
        let client = MinitaoServiceClient::connect(addr).await?;
        Ok(Self {
            client,
            objects: HashSet::new(),
            assocs: HashSet::new(),
        })
    }

    pub async fn apply_graph(&mut self, graph: &KnowledgeGraph) -> anyhow::Result<()> {
        let payloads = build_minitao_payloads(graph)?;
        let new_object_ids: HashSet<ObjectId> = payloads.objects.keys().copied().collect();
        let new_assoc_keys: HashSet<AssocKey> = payloads
            .assocs
            .keys()
            .map(|(id1, atype, id2)| AssocKey {
                id1: *id1,
                atype: *atype,
                id2: *id2,
            })
            .collect();

        let to_delete_objects: Vec<_> = self
            .objects
            .difference(&new_object_ids)
            .copied()
            .collect();
        let to_delete_assocs: Vec<_> = self.assocs.difference(&new_assoc_keys).cloned().collect();

        for key in to_delete_assocs {
            self.client
                .assoc_delete(AssocDeleteRequest {
                    id1: key.id1,
                    atype: key.atype,
                    id2: key.id2,
                })
                .await
                .context("assoc_delete")?;
        }

        for id in to_delete_objects {
            self.client
                .object_delete(ObjectDeleteRequest { id })
                .await
                .context("object_delete")?;
        }

        for obj in payloads.objects.values() {
            self.client
                .object_add(ObjectAddRequest {
                    object: Some(to_proto_object(obj)),
                })
                .await
                .context("object_add")?;
        }

        for assoc in payloads.assocs.values() {
            self.client
                .assoc_add(AssocAddRequest {
                    assoc: Some(to_proto_assoc(assoc)),
                })
                .await
                .context("assoc_add")?;
        }

        self.objects = new_object_ids;
        self.assocs = new_assoc_keys;
        Ok(())
    }
}

fn to_proto_object(obj: &MinitaoObject) -> Object {
    Object {
        id: obj.id,
        otype: obj.otype,
        data: obj.data.clone(),
    }
}

fn to_proto_assoc(assoc: &MinitaoAssociation) -> Association {
    Association {
        id1: assoc.id1,
        atype: assoc.atype,
        id2: assoc.id2,
        time: assoc.time,
        data: assoc.data.clone(),
    }
}
