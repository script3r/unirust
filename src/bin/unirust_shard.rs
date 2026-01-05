use std::fs;

use tonic::transport::Server;
use unirust_rs::config::{ConfigOverrides, Profile, ShardOverrides, UniConfig};
use unirust_rs::distributed::{proto, DistributedOntologyConfig, ShardNode};
use unirust_rs::StreamingTuning;

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
    -o, --ontology <FILE>   Path to ontology config (JSON)
    -p, --profile <NAME>    Tuning profile: balanced, low-latency, high-throughput,
                            bulk-ingest, memory-saver, billion-scale,
                            billion-scale-high-performance
        --repair            Run repair on startup
        --config-version    Config version for compatibility checking
    -h, --help              Print help

ENVIRONMENT:
    UNIRUST_CONFIG          Path to config file
    UNIRUST_PROFILE         Tuning profile
    UNIRUST_SHARD_LISTEN    Listen address
    UNIRUST_SHARD_ID        Shard ID
    UNIRUST_SHARD_DATA_DIR  Data directory

CONFIG FILE (unirust.toml):
    profile = "billion-scale-high-performance"

    [shard]
    listen = "0.0.0.0:50061"
    id = 0
    data_dir = "/var/lib/unirust"
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

    if let Some(ontology) = parse_arg("--ontology").or_else(|| parse_arg("-o")) {
        shard_overrides.ontology = Some(ontology.into());
    }

    if has_flag("--repair") {
        shard_overrides.repair = Some(true);
    }

    if shard_overrides.listen.is_some()
        || shard_overrides.id.is_some()
        || shard_overrides.data_dir.is_some()
        || shard_overrides.ontology.is_some()
        || shard_overrides.repair.is_some()
    {
        overrides.shard = Some(shard_overrides);
    }

    // Load config: CLI > Env > File > Defaults
    let config_path = parse_arg("--config")
        .or_else(|| parse_arg("-c"))
        .or_else(|| std::env::var("UNIRUST_CONFIG").ok());
    let config = UniConfig::load(config_path.as_deref(), overrides)?;

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
    let shard = ShardNode::new_with_data_dir(
        config.shard.id as u32,
        ontology,
        tuning,
        config.shard.data_dir.clone(),
        config.shard.repair,
        config_version,
    )?;

    println!(
        "Unirust shard {} listening on {}",
        config.shard.id, config.shard.listen
    );
    Server::builder()
        .add_service(proto::shard_service_server::ShardServiceServer::new(shard))
        .serve(config.shard.listen)
        .await?;

    Ok(())
}
