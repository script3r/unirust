use std::fs;

use std::time::Duration;

use tonic::transport::{Endpoint, Server};
use unirust_rs::config::{ConfigOverrides, Profile, ShardOverrides, UniConfig};
use unirust_rs::distributed::{proto, DistributedOntologyConfig, ShardNode};
use unirust_rs::transport_security::{load_client_mtls, load_replication_token, load_server_mtls};
use unirust_rs::{restore_checkpoint_for_shard, StreamingTuning};

const MAX_GRPC_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_CONCURRENT_REQUESTS_PER_CONNECTION: usize = 128;

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
        r#"unirust_shard - Unirust shard node

USAGE:
    unirust_shard [OPTIONS]

OPTIONS:
    -c, --config <FILE>     Path to config file (TOML)
    -l, --listen <ADDR>     Override listen address [default: 127.0.0.1:50061]
    -i, --shard-id <ID>     Override shard ID [default: 0]
    -d, --data-dir <DIR>    Override data directory
        --backup-dir <DIR>  Checkpoint root on an independent volume
        --restore-from <DIR>
                            Restore a checkpoint into an empty data directory before startup
    -o, --ontology <FILE>   Path to ontology config (JSON)
    -p, --profile <NAME>    Tuning profile: balanced, low-latency, high-throughput,
                            bulk-ingest, memory-saver, billion-scale,
                            billion-scale-high-performance
        --repair            Run repair on startup
        --ephemeral         Allow an in-memory shard; all data is lost on process exit
        --allow-colocated-checkpoints
                            Permit checkpoints without an independent path (development only)
        --allow-destructive-admin
                            Enable destructive admin RPCs such as Reset
        --tls-cert <FILE>   PEM shard server certificate
        --tls-key <FILE>    PEM shard server private key
        --tls-client-ca <FILE>
                            PEM CA used to require router client certificates
        --replica <URI>     Passive replica endpoint for synchronous replication
        --replica-mode      Run as a passive replica; routers reject this endpoint
        --allow-insecure-replication
                            Permit plaintext replication for isolated development
        --replication-token-file <FILE>
                            Shared secret file (at least 32 bytes) for this replica pair
        --replica-tls-ca <FILE>
                            PEM CA used to verify the replica
        --replica-tls-cert <FILE>
                            PEM primary client certificate presented to the replica
        --replica-tls-key <FILE>
                            PEM primary client private key
        --config-version    Config version for compatibility checking
    -h, --help              Print help

ENVIRONMENT:
    UNIRUST_CONFIG          Path to config file
    UNIRUST_PROFILE         Tuning profile
    UNIRUST_SHARD_LISTEN    Listen address
    UNIRUST_SHARD_ID        Shard ID
    UNIRUST_SHARD_DATA_DIR  Data directory
    UNIRUST_SHARD_BACKUP_DIR
                            Checkpoint root
    UNIRUST_SHARD_TLS_CERT  PEM server certificate
    UNIRUST_SHARD_TLS_KEY   PEM server private key
    UNIRUST_SHARD_TLS_CLIENT_CA
                            PEM CA for required client certificates
    UNIRUST_SHARD_REPLICA   Passive replica endpoint
    UNIRUST_SHARD_REPLICA_MODE
                            Run as a passive replica
    UNIRUST_SHARD_ALLOW_INSECURE_REPLICATION
                            Permit plaintext replication for isolated development
    UNIRUST_SHARD_REPLICATION_TOKEN_FILE
                            Shared secret file for the primary/replica pair
    UNIRUST_SHARD_REPLICA_TLS_CA
                            PEM CA used to verify the replica
    UNIRUST_SHARD_REPLICA_TLS_CERT
                            PEM primary certificate presented to the replica
    UNIRUST_SHARD_REPLICA_TLS_KEY
                            PEM primary client private key

CONFIG FILE (unirust.toml):
    profile = "billion-scale-high-performance"

    [shard]
    listen = "0.0.0.0:50061"
    id = 0
    data_dir = "/var/lib/unirust"
    backup_dir = "/var/backups/unirust/shard-0"
    tls_cert = "/etc/unirust/tls/shard-0.crt"
    tls_key = "/etc/unirust/tls/shard-0.key"
    tls_client_ca = "/etc/unirust/tls/clients-ca.crt"
    replica = "https://shard-0-replica:50061"
    replication_token_file = "/etc/unirust/replication/shard-0.token"
    replica_tls_ca = "/etc/unirust/tls/replicas-ca.crt"
    replica_tls_cert = "/etc/unirust/tls/shard-0-primary.crt"
    replica_tls_key = "/etc/unirust/tls/shard-0-primary.key"
"#
    );
}

fn parse_profile(value: &str) -> Option<Profile> {
    match value {
        "balanced" => Some(Profile::Balanced),
        "low-latency" => Some(Profile::LowLatency),
        "high-throughput" => Some(Profile::HighThroughput),
        "bulk-ingest" => Some(Profile::BulkIngest),
        "memory-saver" => Some(Profile::MemorySaver),
        "billion-scale" => Some(Profile::BillionScale),
        "billion-scale-high-performance" => Some(Profile::BillionScaleHighPerformance),
        _ => None,
    }
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
    let mut shard_overrides = ShardOverrides::default();

    if let Some(profile_str) = parse_arg("--profile").or_else(|| parse_arg("-p")) {
        if let Some(profile) = parse_profile(&profile_str) {
            overrides.profile = Some(profile);
        } else {
            eprintln!("Unknown profile: {}", profile_str);
            std::process::exit(1);
        }
    }

    if let Some(listen) = parse_arg("--listen").or_else(|| parse_arg("-l")) {
        shard_overrides.listen = Some(listen.parse()?);
    }

    if let Some(id_str) = parse_arg("--shard-id").or_else(|| parse_arg("-i")) {
        shard_overrides.id = Some(id_str.parse()?);
    }

    if let Some(data_dir) = parse_arg("--data-dir").or_else(|| parse_arg("-d")) {
        shard_overrides.data_dir = Some(data_dir.into());
    }

    if let Some(backup_dir) = parse_arg("--backup-dir") {
        shard_overrides.backup_dir = Some(backup_dir.into());
    }

    if let Some(ontology) = parse_arg("--ontology").or_else(|| parse_arg("-o")) {
        shard_overrides.ontology = Some(ontology.into());
    }

    if has_flag("--repair") {
        shard_overrides.repair = Some(true);
    }
    if let Some(path) = parse_arg("--tls-cert") {
        shard_overrides.tls_cert = Some(path.into());
    }
    if let Some(path) = parse_arg("--tls-key") {
        shard_overrides.tls_key = Some(path.into());
    }
    if let Some(path) = parse_arg("--tls-client-ca") {
        shard_overrides.tls_client_ca = Some(path.into());
    }
    if let Some(replica) = parse_arg("--replica") {
        shard_overrides.replica = Some(replica);
    }
    if has_flag("--replica-mode") {
        shard_overrides.replica_mode = Some(true);
    }
    if has_flag("--allow-insecure-replication") {
        shard_overrides.allow_insecure_replication = Some(true);
    }
    if let Some(path) = parse_arg("--replication-token-file") {
        shard_overrides.replication_token_file = Some(path.into());
    }
    if let Some(path) = parse_arg("--replica-tls-ca") {
        shard_overrides.replica_tls_ca = Some(path.into());
    }
    if let Some(path) = parse_arg("--replica-tls-cert") {
        shard_overrides.replica_tls_cert = Some(path.into());
    }
    if let Some(path) = parse_arg("--replica-tls-key") {
        shard_overrides.replica_tls_key = Some(path.into());
    }

    if shard_overrides.listen.is_some()
        || shard_overrides.id.is_some()
        || shard_overrides.data_dir.is_some()
        || shard_overrides.backup_dir.is_some()
        || shard_overrides.ontology.is_some()
        || shard_overrides.repair.is_some()
        || shard_overrides.tls_cert.is_some()
        || shard_overrides.tls_key.is_some()
        || shard_overrides.tls_client_ca.is_some()
        || shard_overrides.replica.is_some()
        || shard_overrides.replica_mode.is_some()
        || shard_overrides.allow_insecure_replication.is_some()
        || shard_overrides.replication_token_file.is_some()
        || shard_overrides.replica_tls_ca.is_some()
        || shard_overrides.replica_tls_cert.is_some()
        || shard_overrides.replica_tls_key.is_some()
    {
        overrides.shard = Some(shard_overrides);
    }

    // Load config: CLI > Env > File > Defaults
    let config_path = parse_arg("--config")
        .or_else(|| parse_arg("-c"))
        .or_else(|| std::env::var("UNIRUST_CONFIG").ok());
    let config = UniConfig::load(config_path.as_deref(), overrides)?;
    let server_mtls = load_server_mtls(
        config.shard.tls_cert.as_deref(),
        config.shard.tls_key.as_deref(),
        config.shard.tls_client_ca.as_deref(),
    )?;
    let replica_mtls = load_client_mtls(
        config.shard.replica_tls_ca.as_deref(),
        config.shard.replica_tls_cert.as_deref(),
        config.shard.replica_tls_key.as_deref(),
    )?;
    let replication_token = config
        .shard
        .replication_token_file
        .as_deref()
        .map(load_replication_token)
        .transpose()?;
    if config.shard.data_dir.is_none() && !has_flag("--ephemeral") {
        anyhow::bail!(
            "persistent shard storage is required; configure --data-dir (or use --ephemeral \
             explicitly for disposable development data)"
        );
    }
    if let Some(data_dir) = config.shard.data_dir.as_deref() {
        if !has_flag("--allow-colocated-checkpoints") {
            let backup_dir = config.shard.backup_dir.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "persistent shards require --backup-dir on independent storage (or use \
                     --allow-colocated-checkpoints explicitly for development)"
                )
            })?;
            let data_dir = std::path::absolute(data_dir)?;
            let backup_dir = std::path::absolute(backup_dir)?;
            if data_dir == backup_dir
                || data_dir.starts_with(&backup_dir)
                || backup_dir.starts_with(&data_dir)
            {
                anyhow::bail!(
                    "shard data and checkpoint paths must not overlap: data={}, backup={}",
                    data_dir.display(),
                    backup_dir.display()
                );
            }
        }
    }
    if let Some(source) = parse_arg("--restore-from") {
        let data_dir = config
            .shard
            .data_dir
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--restore-from requires --data-dir"))?;
        restore_checkpoint_for_shard(
            std::path::Path::new(&source),
            data_dir,
            Some(u32::from(config.shard.id)),
        )?;
    }

    // Get tuning from profile
    let profile = config.profile.to_tuning_profile();
    let tuning = StreamingTuning::from_profile(profile)
        .with_shard_id(config.shard.id)
        .with_boundary_tracking(true);

    // Load ontology
    let ontology = load_ontology(config.shard.ontology.as_deref())?;

    // Get config version from CLI or config
    let config_version = parse_arg("--config-version").or(config.shard.config_version.clone());

    // Create shard node
    let mut shard = ShardNode::new_with_storage_paths(
        config.shard.id as u32,
        ontology,
        tuning,
        config.shard.data_dir.clone(),
        config.shard.backup_dir.clone(),
        config.shard.repair,
        config_version,
    )?;
    if config.shard.replica_mode {
        shard = shard.into_replica(
            replication_token
                .clone()
                .ok_or_else(|| anyhow::anyhow!("replica mode requires a replication token"))?,
        )?;
    }
    if let Some(replica) = &config.shard.replica {
        let replica = if replica.starts_with("http://") || replica.starts_with("https://") {
            replica.clone()
        } else if replica_mtls.is_some() {
            format!("https://{replica}")
        } else {
            format!("http://{replica}")
        };
        let uses_https = replica.starts_with("https://");
        let endpoint = Endpoint::from_shared(replica)?
            .connect_timeout(Duration::from_secs(
                config.shard.replica_connect_timeout_secs,
            ))
            .timeout(Duration::from_secs(
                config.shard.replica_request_timeout_secs,
            ))
            .tcp_keepalive(Some(Duration::from_secs(
                config.shard.replica_tcp_keepalive_secs,
            )));
        let endpoint = match (replica_mtls, uses_https) {
            (Some(tls), true) => endpoint.tls_config(tls)?,
            (Some(_), false) => {
                anyhow::bail!("shard-to-replica mTLS requires an https:// replica endpoint");
            }
            (None, true) => {
                anyhow::bail!(
                    "https:// replica endpoints require explicit replica TLS certificate \
                     configuration"
                );
            }
            (None, false) => endpoint,
        };
        let channel = endpoint.connect().await?;
        shard = shard
            .with_replica(
                proto::shard_service_client::ShardServiceClient::new(channel),
                replication_token
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("replication requires a shared token"))?,
            )
            .await?;
    }
    let shard = shard.with_destructive_admin(has_flag("--allow-destructive-admin"));

    println!(
        "Unirust shard {} listening on {}",
        config.shard.id, config.shard.listen
    );
    let mut server = Server::builder()
        .concurrency_limit_per_connection(MAX_CONCURRENT_REQUESTS_PER_CONNECTION)
        .load_shed(true);
    if let Some(server_mtls) = server_mtls {
        server = server.tls_config(server_mtls)?;
    }
    server
        .add_service(
            proto::shard_service_server::ShardServiceServer::new(shard.clone())
                .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES),
        )
        .serve_with_shutdown(config.shard.listen, shutdown_signal())
        .await?;
    shard.shutdown().await?;

    Ok(())
}
