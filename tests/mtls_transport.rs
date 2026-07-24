use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tempfile::{tempdir, TempDir};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, ClientTlsConfig, Server};
use unirust_rs::distributed::proto;
use unirust_rs::distributed::{
    AdaptiveReconciliationConfig, DistributedOntologyConfig, RouterNode, RouterRpcConfig, ShardNode,
};
use unirust_rs::transport_security::{load_client_mtls, load_server_mtls};
use unirust_rs::{StreamingTuning, TuningProfile};

static PERSISTENT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct PemIdentity {
    cert: String,
    key: String,
}

struct TestPki {
    ca: String,
    server: PemIdentity,
    client: PemIdentity,
    rogue_client: PemIdentity,
}

struct PkiPaths {
    _dir: TempDir,
    ca: PathBuf,
    server_cert: PathBuf,
    server_key: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
    rogue_client_cert: PathBuf,
    rogue_client_key: PathBuf,
}

fn certificate_authority() -> anyhow::Result<CertifiedIssuer<'static, KeyPair>> {
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    Ok(CertifiedIssuer::self_signed(params, KeyPair::generate()?)?)
}

fn leaf_identity(
    issuer: &CertifiedIssuer<'static, KeyPair>,
    names: Vec<String>,
    usage: ExtendedKeyUsagePurpose,
) -> anyhow::Result<PemIdentity> {
    let mut params = CertificateParams::new(names)?;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![usage];
    let key = KeyPair::generate()?;
    let cert = params.signed_by(&key, issuer)?;
    Ok(PemIdentity {
        cert: cert.pem(),
        key: key.serialize_pem(),
    })
}

fn test_pki() -> anyhow::Result<TestPki> {
    let issuer = certificate_authority()?;
    let rogue_issuer = certificate_authority()?;
    Ok(TestPki {
        ca: issuer.pem(),
        server: leaf_identity(
            &issuer,
            vec!["127.0.0.1".to_string(), "localhost".to_string()],
            ExtendedKeyUsagePurpose::ServerAuth,
        )?,
        client: leaf_identity(&issuer, Vec::new(), ExtendedKeyUsagePurpose::ClientAuth)?,
        rogue_client: leaf_identity(
            &rogue_issuer,
            Vec::new(),
            ExtendedKeyUsagePurpose::ClientAuth,
        )?,
    })
}

fn write_file(dir: &Path, name: &str, contents: &str) -> anyhow::Result<PathBuf> {
    let path = dir.join(name);
    fs::write(&path, contents)?;
    Ok(path)
}

fn write_pki(pki: &TestPki) -> anyhow::Result<PkiPaths> {
    let dir = tempdir()?;
    let root = dir.path();
    Ok(PkiPaths {
        ca: write_file(root, "ca.pem", &pki.ca)?,
        server_cert: write_file(root, "server.pem", &pki.server.cert)?,
        server_key: write_file(root, "server-key.pem", &pki.server.key)?,
        client_cert: write_file(root, "client.pem", &pki.client.cert)?,
        client_key: write_file(root, "client-key.pem", &pki.client.key)?,
        rogue_client_cert: write_file(root, "rogue-client.pem", &pki.rogue_client.cert)?,
        rogue_client_key: write_file(root, "rogue-client-key.pem", &pki.rogue_client.key)?,
        _dir: dir,
    })
}

async fn spawn_mtls_shard(
    paths: &PkiPaths,
) -> anyhow::Result<(SocketAddr, ShardNode, JoinHandle<()>)> {
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
    let server_shard = shard.clone();
    let tls = load_server_mtls(
        Some(&paths.server_cert),
        Some(&paths.server_key),
        Some(&paths.ca),
    )?
    .expect("complete server mTLS");
    let handle = tokio::spawn(async move {
        let _data_dir = data_dir;
        Server::builder()
            .tls_config(tls)
            .expect("valid test TLS")
            .add_service(proto::shard_service_server::ShardServiceServer::new(
                server_shard,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("mTLS shard server");
    });
    Ok((addr, shard, handle))
}

async fn spawn_mtls_router(
    router: std::sync::Arc<RouterNode>,
    paths: &PkiPaths,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let tls = load_server_mtls(
        Some(&paths.server_cert),
        Some(&paths.server_key),
        Some(&paths.ca),
    )?
    .expect("complete server mTLS");
    let handle = tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)
            .expect("valid test TLS")
            .add_service(proto::router_service_server::RouterServiceServer::new(
                router,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("mTLS router server");
    });
    Ok((addr, handle))
}

async fn run_healthcheck(args: Vec<String>) -> anyhow::Result<Output> {
    Ok(tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_unirust_healthcheck"))
            .args(args)
            .output()
    })
    .await??)
}

fn probe_args(
    service: &str,
    addr: SocketAddr,
    paths: &PkiPaths,
    cert: &Path,
    key: &Path,
) -> Vec<String> {
    vec![
        service.to_string(),
        format!("https://{addr}"),
        "--ca-cert".to_string(),
        paths.ca.display().to_string(),
        "--client-cert".to_string(),
        cert.display().to_string(),
        "--client-key".to_string(),
        key.display().to_string(),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutual_tls_authenticates_complete_router_and_shard_path() -> anyhow::Result<()> {
    let _test_guard = PERSISTENT_TEST_LOCK.lock().await;
    let pki = test_pki()?;
    let paths = write_pki(&pki)?;
    let (shard_addr, shard, shard_handle) = spawn_mtls_shard(&paths).await?;
    let shard_url = format!("https://{shard_addr}");

    let missing_tls_error = match RouterNode::connect_with_runtime_config(
        vec![shard_url.clone()],
        DistributedOntologyConfig::empty(),
        None,
        AdaptiveReconciliationConfig::default(),
        RouterRpcConfig::default(),
    )
    .await
    {
        Ok(_) => anyhow::bail!("router connected to HTTPS shard without trust material"),
        Err(error) => error,
    };
    assert_eq!(missing_tls_error.code(), tonic::Code::InvalidArgument);
    assert!(missing_tls_error
        .message()
        .contains("explicit router-to-shard"));

    let shard_client_tls = load_client_mtls(
        Some(&paths.ca),
        Some(&paths.client_cert),
        Some(&paths.client_key),
    )?
    .expect("complete client mTLS");
    let router = RouterNode::connect_with_runtime_config(
        vec![shard_url],
        DistributedOntologyConfig::empty(),
        None,
        AdaptiveReconciliationConfig::default(),
        RouterRpcConfig {
            shard_mtls: Some(shard_client_tls),
            ..RouterRpcConfig::default()
        },
    )
    .await?;
    let (router_addr, router_handle) = spawn_mtls_router(router, &paths).await?;

    let trusted = run_healthcheck(probe_args(
        "--router",
        router_addr,
        &paths,
        &paths.client_cert,
        &paths.client_key,
    ))
    .await?;
    assert!(
        trusted.status.success(),
        "trusted mTLS probe failed: {}",
        String::from_utf8_lossy(&trusted.stderr)
    );

    let rogue = run_healthcheck(probe_args(
        "--router",
        router_addr,
        &paths,
        &paths.rogue_client_cert,
        &paths.rogue_client_key,
    ))
    .await?;
    assert!(
        !rogue.status.success(),
        "router accepted a client certificate signed by an untrusted CA"
    );

    let endpoint = tonic::transport::Endpoint::from_shared(format!("https://{router_addr}"))?
        .tls_config(
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pki.ca.as_bytes())),
        )?;
    let missing_identity_rejected = match endpoint.connect().await {
        Ok(channel) => proto::router_service_client::RouterServiceClient::new(channel)
            .health_check(proto::HealthCheckRequest {})
            .await
            .is_err(),
        Err(_) => true,
    };
    assert!(
        missing_identity_rejected,
        "router accepted a TLS client without a certificate"
    );

    router_handle.abort();
    shard.shutdown().await?;
    shard_handle.abort();
    Ok(())
}
