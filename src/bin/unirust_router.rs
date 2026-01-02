use std::fs;
use std::net::SocketAddr;

use tonic::transport::Server;
use unirust_rs::distributed::{proto, DistributedOntologyConfig, RouterNode};

fn parse_arg(flag: &str) -> Option<String> {
    let mut args = std::env::args();
    while let Some(arg) = args.next() {
        if arg == flag {
            return args.next();
        }
    }
    None
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
    let listen = parse_arg("--listen").unwrap_or_else(|| "127.0.0.1:50060".to_string());
    let shards_arg = parse_arg("--shards").unwrap_or_else(|| "127.0.0.1:50061".to_string());
    let ontology_path = parse_arg("--ontology");

    let shard_addrs = shards_arg
        .split(',')
        .map(|addr| {
            if addr.starts_with("http://") || addr.starts_with("https://") {
                addr.to_string()
            } else {
                format!("http://{}", addr)
            }
        })
        .collect::<Vec<_>>();

    let ontology = load_ontology(ontology_path)?;
    let router = RouterNode::connect(shard_addrs, ontology).await?;
    let addr: SocketAddr = listen.parse()?;

    println!("Unirust router listening on {}", addr);
    Server::builder()
        .add_service(proto::router_service_server::RouterServiceServer::new(
            router,
        ))
        .serve(addr)
        .await?;

    Ok(())
}
