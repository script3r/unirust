use std::fs;
use std::net::SocketAddr;

use tonic::transport::Server;
use unirust_rs::distributed::{proto, DistributedOntologyConfig, ShardNode};
use unirust_rs::{StreamingTuning, TuningProfile};

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

fn parse_tuning(value: Option<String>) -> StreamingTuning {
    match value.as_deref() {
        Some("low-latency") => StreamingTuning::from_profile(TuningProfile::LowLatency),
        Some("high-throughput") => StreamingTuning::from_profile(TuningProfile::HighThroughput),
        Some("bulk-ingest") => StreamingTuning::from_profile(TuningProfile::BulkIngest),
        Some("memory-saver") => StreamingTuning::from_profile(TuningProfile::MemorySaver),
        _ => StreamingTuning::from_profile(TuningProfile::Balanced),
    }
}

fn load_ontology(path: Option<String>) -> anyhow::Result<DistributedOntologyConfig> {
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
    let listen = parse_arg("--listen").unwrap_or_else(|| "127.0.0.1:50061".to_string());
    let shard_id: u32 = parse_arg("--shard-id")
        .unwrap_or_else(|| "0".to_string())
        .parse()?;
    let ontology_path = parse_arg("--ontology");
    let data_dir = parse_arg("--data-dir");
    let repair = has_flag("--repair");
    let tuning = parse_tuning(parse_arg("--tuning"));

    let ontology = load_ontology(ontology_path)?;
    let addr: SocketAddr = listen.parse()?;
    let shard = ShardNode::new_with_data_dir(
        shard_id,
        ontology,
        tuning,
        data_dir.map(std::path::PathBuf::from),
        repair,
    )?;

    println!("Unirust shard listening on {}", addr);
    Server::builder()
        .add_service(proto::shard_service_server::ShardServiceServer::new(shard))
        .serve(addr)
        .await?;

    Ok(())
}
