use std::fs;

use tonic::transport::Server;
use unirust_rs::config::{normalize_shard_addrs, ConfigOverrides, RouterOverrides, UniConfig};
use unirust_rs::distributed::{
    proto, AdaptiveReconciliationConfig, DistributedOntologyConfig, RouterNode,
};

fn parse_arg(flag: &str) -> Option<String> {
    let mut args = std::env::args();
    while let Some(arg) = args.next() {
        if arg == flag {
            return args.next();
        }
    }
    None
}

fn has_flag(flag: &str) -> bool {
    std::env::args().any(|arg| arg == flag)
}

fn print_help() {
    eprintln!(
        r#"unirust_router - Unirust router node

USAGE:
    unirust_router [OPTIONS]

OPTIONS:
    -c, --config <FILE>     Path to config file (TOML)
    -l, --listen <ADDR>     Override listen address [default: 127.0.0.1:50060]
    -s, --shards <ADDRS>    Override shard addresses (comma-separated)
        --shards-file <F>   Path to file containing shard addresses (one per line)
    -o, --ontology <FILE>   Path to ontology config (JSON)
        --config-version    Config version for compatibility checking
    -h, --help              Print help

ENVIRONMENT:
    UNIRUST_CONFIG          Path to config file
    UNIRUST_ROUTER_LISTEN   Listen address
    UNIRUST_ROUTER_SHARDS   Comma-separated shard addresses

CONFIG FILE (unirust.toml):
    [router]
    listen = "0.0.0.0:50060"
    shards = ["shard-0:50061", "shard-1:50061", "shard-2:50061", "shard-3:50061", "shard-4:50061"]
"#
    );
}

fn load_ontology(path: Option<&std::path::Path>) -> anyhow::Result<DistributedOntologyConfig> {
    if let Some(path) = path {
        let raw = fs::read_to_string(path)?;
        let config = serde_json::from_str(&raw)?;
        Ok(config)
    } else {
        Ok(DistributedOntologyConfig::empty())
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if has_flag("-h") || has_flag("--help") {
        print_help();
        return Ok(());
    }

    tracing_subscriber::fmt::init();

    // Build CLI overrides
    let mut overrides = ConfigOverrides::default();
    let mut router_overrides = RouterOverrides::default();

    if let Some(listen) = parse_arg("--listen").or_else(|| parse_arg("-l")) {
        router_overrides.listen = Some(listen.parse()?);
    }

    if let Some(shards_arg) = parse_arg("--shards").or_else(|| parse_arg("-s")) {
        let shards: Vec<String> = shards_arg
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        router_overrides.shards = Some(shards);
    }

    if let Some(shards_file) = parse_arg("--shards-file") {
        router_overrides.shards_file = Some(shards_file.into());
    }

    if let Some(ontology) = parse_arg("--ontology").or_else(|| parse_arg("-o")) {
        router_overrides.ontology = Some(ontology.into());
    }

    if router_overrides.listen.is_some()
        || router_overrides.shards.is_some()
        || router_overrides.shards_file.is_some()
        || router_overrides.ontology.is_some()
    {
        overrides.router = Some(router_overrides);
    }

    // Load config: CLI > Env > File > Defaults
    let config_path = parse_arg("--config")
        .or_else(|| parse_arg("-c"))
        .or_else(|| std::env::var("UNIRUST_CONFIG").ok());
    let config = UniConfig::load(config_path.as_deref(), overrides)?;

    // Load ontology
    let ontology = load_ontology(config.router.ontology.as_deref())?;

    // Get config version from CLI or config
    let config_version = parse_arg("--config-version").or(config.router.config_version.clone());

    // Normalize shard addresses
    let shard_addrs = normalize_shard_addrs(&config.router.shards);
    let reconciliation = AdaptiveReconciliationConfig {
        key_count_threshold: config.reconciliation.key_count_threshold,
        max_staleness: std::time::Duration::from_secs(config.reconciliation.max_staleness_secs),
        idle_ingest_rate: config.reconciliation.idle_ingest_rate,
        min_reconcile_interval: std::time::Duration::from_secs(
            config.reconciliation.min_interval_secs,
        ),
    };

    // Create router node
    let router = if let Some(path) = &config.router.shards_file {
        RouterNode::connect_from_file_with_reconciliation(
            path,
            ontology,
            config_version,
            reconciliation,
        )
        .await?
    } else {
        RouterNode::connect_with_version_and_reconciliation(
            shard_addrs,
            ontology,
            config_version,
            reconciliation,
        )
        .await?
    };

    println!("Unirust router listening on {}", config.router.listen);
    Server::builder()
        .add_service(proto::router_service_server::RouterServiceServer::new(
            router,
        ))
        .serve_with_shutdown(config.router.listen, shutdown_signal())
        .await?;

    Ok(())
}
