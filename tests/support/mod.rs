use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::Semaphore;
use tonic::codegen::{http, Service};
use tonic::server::NamedService;
use unirust_rs::distributed::proto::{
    ConstraintConfig as ProtoConstraintConfig, ConstraintKind as ProtoConstraintKind,
    IdentityKeyConfig as ProtoIdentityKeyConfig, OntologyConfig,
};
use unirust_rs::distributed::{
    ConstraintConfig, ConstraintKind, DistributedOntologyConfig, IdentityKeyConfig,
};

#[derive(Clone)]
#[allow(dead_code)]
pub struct StallResponse<S> {
    inner: S,
    path: &'static str,
    stall_next: Arc<AtomicBool>,
    committed: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

impl<S, B> Service<http::Request<B>> for StallResponse<S>
where
    S: Service<
            http::Request<B>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Send,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let should_stall =
            request.uri().path() == self.path && self.stall_next.swap(false, Ordering::AcqRel);
        let future = self.inner.call(request);
        let committed = self.committed.clone();
        let release = self.release.clone();
        Box::pin(async move {
            let response = future.await?;
            if should_stall {
                committed.add_permits(1);
                if let Ok(permit) = release.acquire_owned().await {
                    permit.forget();
                }
            }
            Ok(response)
        })
    }
}

impl<S> NamedService for StallResponse<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

#[allow(dead_code)]
pub struct StallControls {
    committed: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[allow(dead_code)]
impl StallControls {
    pub async fn wait_until_committed(&self, timeout: Duration) -> anyhow::Result<()> {
        let permit =
            tokio::time::timeout(timeout, self.committed.clone().acquire_owned()).await??;
        permit.forget();
        Ok(())
    }

    pub fn release(&self) {
        self.release.add_permits(1);
    }
}

#[allow(dead_code)]
pub fn stall_response<S>(
    inner: S,
    path: &'static str,
    initially_armed: bool,
) -> (StallResponse<S>, StallControls) {
    let stall_next = Arc::new(AtomicBool::new(initially_armed));
    let committed = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    (
        StallResponse {
            inner,
            path,
            stall_next: stall_next.clone(),
            committed: committed.clone(),
            release: release.clone(),
        },
        StallControls { committed, release },
    )
}

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
