//! # gRPC Server Binary
//!
//! Runs the Unirust gRPC server

use std::net::SocketAddr;
use unirust::grpc_server::run_grpc_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    
    println!("Starting Unirust gRPC server on {}", addr);
    
    run_grpc_server(addr).await?;
    
    Ok(())
}