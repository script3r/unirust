use std::path::PathBuf;
use std::time::Duration;

use tonic::transport::Endpoint;
use unirust_rs::distributed::proto::{
    self, router_service_client::RouterServiceClient, shard_service_client::ShardServiceClient,
};
use unirust_rs::transport_security::load_client_mtls;

enum Service {
    Router(String),
    Shard(String),
}

struct Options {
    service: Service,
    timeout: Duration,
    ca_cert: Option<PathBuf>,
    client_cert: Option<PathBuf>,
    client_key: Option<PathBuf>,
}

fn print_help() {
    eprintln!(
        r#"unirust_healthcheck - Unirust gRPC readiness probe

USAGE:
    unirust_healthcheck (--router <URI> | --shard <URI>) [--timeout-secs <SECONDS>]

OPTIONS:
        --router <URI>      Check a router and all of its shards
        --shard <URI>       Check one shard's recovery and store health
        --timeout-secs <N>  Connection and RPC timeout [default: 2]
        --ca-cert <FILE>    PEM CA used to verify the service certificate
        --client-cert <FILE>
                            PEM client certificate
        --client-key <FILE> PEM client private key
    -h, --help              Print help
"#
    );
}

fn parse_options() -> anyhow::Result<Option<Options>> {
    let mut service = None;
    let mut timeout_secs = 2u64;
    let mut ca_cert = None;
    let mut client_cert = None;
    let mut client_key = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_help();
                return Ok(None);
            }
            "--router" => {
                let uri = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--router requires a URI"))?;
                if service.is_some() {
                    anyhow::bail!("exactly one of --router or --shard is required");
                }
                service = Some(Service::Router(uri));
            }
            "--shard" => {
                let uri = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--shard requires a URI"))?;
                if service.is_some() {
                    anyhow::bail!("exactly one of --router or --shard is required");
                }
                service = Some(Service::Shard(uri));
            }
            "--timeout-secs" => {
                timeout_secs = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--timeout-secs requires a value"))?
                    .parse()?;
                if timeout_secs == 0 {
                    anyhow::bail!("--timeout-secs must be greater than zero");
                }
            }
            "--ca-cert" => {
                ca_cert = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--ca-cert requires a path"))?
                        .into(),
                );
            }
            "--client-cert" => {
                client_cert = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--client-cert requires a path"))?
                        .into(),
                );
            }
            "--client-key" => {
                client_key = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--client-key requires a path"))?
                        .into(),
                );
            }
            _ => anyhow::bail!("unknown option {arg}"),
        }
    }

    let service =
        service.ok_or_else(|| anyhow::anyhow!("exactly one of --router or --shard is required"))?;
    Ok(Some(Options {
        service,
        timeout: Duration::from_secs(timeout_secs),
        ca_cert,
        client_cert,
        client_key,
    }))
}

fn normalize_uri(uri: String, mtls: bool) -> anyhow::Result<String> {
    if uri.starts_with("http://") || uri.starts_with("https://") {
        if mtls && uri.starts_with("http://") {
            anyhow::bail!("mTLS health checks require an https:// endpoint");
        }
        if !mtls && uri.starts_with("https://") {
            anyhow::bail!("https:// health checks require CA, client certificate, and client key");
        }
        Ok(uri)
    } else if mtls {
        Ok(format!("https://{uri}"))
    } else {
        Ok(format!("http://{uri}"))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Some(options) = parse_options()? else {
        return Ok(());
    };
    let mtls = load_client_mtls(
        options.ca_cert.as_deref(),
        options.client_cert.as_deref(),
        options.client_key.as_deref(),
    )?;
    let uri = match &options.service {
        Service::Router(uri) | Service::Shard(uri) => normalize_uri(uri.clone(), mtls.is_some())?,
    };
    let endpoint = Endpoint::from_shared(uri)?
        .connect_timeout(options.timeout)
        .timeout(options.timeout);
    let endpoint = if let Some(mtls) = mtls {
        endpoint.tls_config(mtls)?
    } else {
        endpoint
    };
    let channel = endpoint.connect().await?;
    let response = match options.service {
        Service::Router(_) => RouterServiceClient::new(channel)
            .health_check(proto::HealthCheckRequest {})
            .await?
            .into_inner(),
        Service::Shard(_) => ShardServiceClient::new(channel)
            .health_check(proto::HealthCheckRequest {})
            .await?
            .into_inner(),
    };
    if response.status != "ok" {
        anyhow::bail!("service reported unhealthy status: {}", response.status);
    }
    println!("ok");
    Ok(())
}
