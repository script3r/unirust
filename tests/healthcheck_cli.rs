use std::net::SocketAddr;
use std::pin::Pin;
use std::process::{Command, Output};
use std::sync::Arc;
use std::task::{Context, Poll};

use tempfile::tempdir;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::{http, Service};
use tonic::server::NamedService;
use tonic::transport::Server;
use unirust_rs::distributed::proto;
use unirust_rs::distributed::{DistributedOntologyConfig, RouterNode, ShardNode};
use unirust_rs::{StreamingTuning, TuningProfile};

static PERSISTENT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone)]
struct FailHealth<S> {
    inner: S,
}

impl<S, B> Service<http::Request<B>> for FailHealth<S>
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
        if request.uri().path() == "/unirust.ShardService/HealthCheck" {
            return Box::pin(async {
                Ok(tonic::Status::failed_precondition("injected unhealthy shard").into_http())
            });
        }
        Box::pin(self.inner.call(request))
    }
}

impl<S> NamedService for FailHealth<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

async fn run_probe(args: Vec<String>) -> anyhow::Result<Output> {
    Ok(tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_unirust_healthcheck"))
            .args(args)
            .output()
    })
    .await??)
}

async fn spawn_healthy_shard() -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let data_dir = tempdir()?;
    let shard = ShardNode::new_with_data_dir(
        0,
        DistributedOntologyConfig::empty(),
        StreamingTuning::from_profile(TuningProfile::Balanced),
        Some(data_dir.path().to_path_buf()),
        false,
        None,
    )?;
    let handle = tokio::spawn(async move {
        let _data_dir = data_dir;
        Server::builder()
            .add_service(proto::shard_service_server::ShardServiceServer::new(shard))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("shard server");
    });
    Ok((addr, handle))
}

async fn spawn_unhealthy_shard() -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let data_dir = tempdir()?;
    let shard = ShardNode::new_with_data_dir(
        0,
        DistributedOntologyConfig::empty(),
        StreamingTuning::from_profile(TuningProfile::Balanced),
        Some(data_dir.path().to_path_buf()),
        false,
        None,
    )?;
    let service = proto::shard_service_server::ShardServiceServer::new(shard);
    let handle = tokio::spawn(async move {
        let _data_dir = data_dir;
        Server::builder()
            .add_service(FailHealth { inner: service })
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("shard server");
    });
    Ok((addr, handle))
}

async fn spawn_router(
    shard_addr: SocketAddr,
) -> anyhow::Result<(SocketAddr, Arc<RouterNode>, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let router = RouterNode::connect(
        vec![format!("http://{shard_addr}")],
        DistributedOntologyConfig::empty(),
    )
    .await?;
    let server_router = router.clone();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::router_service_server::RouterServiceServer::new(
                server_router,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("router server");
    });
    Ok((addr, router, handle))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthcheck_accepts_healthy_shard_and_router() -> anyhow::Result<()> {
    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let (shard_addr, shard_handle) = spawn_healthy_shard().await?;
    let shard_output = run_probe(vec!["--shard".into(), shard_addr.to_string()]).await?;
    assert!(
        shard_output.status.success(),
        "shard probe failed: {}",
        String::from_utf8_lossy(&shard_output.stderr)
    );

    let (router_addr, _router, router_handle) = spawn_router(shard_addr).await?;
    let router_output = run_probe(vec!["--router".into(), router_addr.to_string()]).await?;
    assert!(
        router_output.status.success(),
        "router probe failed: {}",
        String::from_utf8_lossy(&router_output.stderr)
    );

    router_handle.abort();
    shard_handle.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthcheck_rejects_unhealthy_rpc_behind_open_socket() -> anyhow::Result<()> {
    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let (addr, handle) = spawn_unhealthy_shard().await?;
    let output = run_probe(vec!["--shard".into(), addr.to_string()]).await?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("injected unhealthy shard"),
        "unexpected probe error: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    handle.abort();
    Ok(())
}
