use std::path::PathBuf;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use unirust_rs::distributed::proto::router_service_client::RouterServiceClient;
use unirust_rs::distributed::proto::shard_service_client::ShardServiceClient;
use unirust_rs::distributed::proto::{
    ExportRecordsRequest, ImportRecordsChunk, ImportRecordsRequest, RecordIdRangeRequest,
    RouterExportRecordsRequest, RouterImportRecordsRequest, RouterRecordIdRangeRequest,
};
use unirust_rs::transport_security::load_client_mtls;

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

fn normalize_addr(addr: impl AsRef<str>, mtls: bool) -> anyhow::Result<String> {
    let addr = addr.as_ref();
    if addr.starts_with("http://") || addr.starts_with("https://") {
        if mtls && addr.starts_with("http://") {
            anyhow::bail!("mTLS rebalance connections require https:// endpoints");
        }
        if !mtls && addr.starts_with("https://") {
            anyhow::bail!("https:// endpoints require --tls-ca, --tls-cert, and --tls-key");
        }
        Ok(addr.to_string())
    } else if mtls {
        Ok(format!("https://{addr}"))
    } else {
        Ok(format!("http://{addr}"))
    }
}

async fn connect(addr: &str, mtls: Option<&ClientTlsConfig>) -> anyhow::Result<Channel> {
    let endpoint = Endpoint::from_shared(normalize_addr(addr, mtls.is_some())?)?;
    let endpoint = if let Some(mtls) = mtls {
        endpoint.tls_config(mtls.clone())?
    } else {
        endpoint
    };
    Ok(endpoint.connect().await?)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tls_ca = parse_arg("--tls-ca").map(PathBuf::from);
    let tls_cert = parse_arg("--tls-cert").map(PathBuf::from);
    let tls_key = parse_arg("--tls-key").map(PathBuf::from);
    let mtls = load_client_mtls(tls_ca.as_deref(), tls_cert.as_deref(), tls_key.as_deref())?;
    let router = parse_arg("--router");
    if has_flag("--range") {
        let shard_arg = parse_arg("--shard");
        let shard_id_arg = parse_arg("--shard-id");
        let response = if let Some(router) = router.clone() {
            let shard_id: u32 = shard_id_arg
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--shard-id is required with --router"))?
                .parse()?;
            let channel = connect(&router, mtls.as_ref()).await?;
            let mut client = RouterServiceClient::new(channel);
            client
                .get_record_id_range(RouterRecordIdRangeRequest { shard_id })
                .await?
                .into_inner()
        } else {
            let shard = shard_arg
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--shard is required for --range"))?;
            let channel = connect(shard, mtls.as_ref()).await?;
            let mut client = ShardServiceClient::new(channel);
            client
                .get_record_id_range(RecordIdRangeRequest {})
                .await?
                .into_inner()
        };
        if response.empty {
            if let Some(shard_id) = shard_id_arg.as_deref() {
                println!("Shard {} is empty", shard_id);
            } else if let Some(shard) = shard_arg.as_deref() {
                println!("Shard {} is empty", shard);
            } else {
                println!("Shard is empty");
            }
        } else if let Some(shard_id) = shard_id_arg.as_deref() {
            println!(
                "Shard {} range: {}..={} (count {})",
                shard_id, response.min_id, response.max_id, response.record_count
            );
        } else if let Some(shard) = shard_arg.as_deref() {
            println!(
                "Shard {} range: {}..={} (count {})",
                shard, response.min_id, response.max_id, response.record_count
            );
        } else {
            println!(
                "Shard range: {}..={} (count {})",
                response.min_id, response.max_id, response.record_count
            );
        }
        return Ok(());
    }

    if router.is_none() {
        anyhow::bail!(
            "--router is required for imports; direct shard-to-shard copies bypass durable source \
             reservations and are not a safe rebalance operation"
        );
    }

    let source = parse_arg("--source").ok_or_else(|| anyhow::anyhow!("--source is required"))?;
    let target = parse_arg("--target").ok_or_else(|| anyhow::anyhow!("--target is required"))?;
    let start_id: u32 = parse_arg("--start-id")
        .unwrap_or_else(|| "0".to_string())
        .parse()?;
    let end_id: u32 = parse_arg("--end-id")
        .unwrap_or_else(|| "0".to_string())
        .parse()?;
    let batch_size: u32 = parse_arg("--batch-size")
        .unwrap_or_else(|| "1000".to_string())
        .parse()?;
    let use_stream = has_flag("--stream");

    let use_router = router.is_some();
    let mut router_client = if let Some(router) = router {
        Some(RouterServiceClient::new(
            connect(&router, mtls.as_ref()).await?,
        ))
    } else {
        None
    };
    let mut source_client = if use_router {
        None
    } else {
        Some(ShardServiceClient::new(
            connect(&source, mtls.as_ref()).await?,
        ))
    };
    let mut target_client = if use_router {
        None
    } else {
        Some(ShardServiceClient::new(
            connect(&target, mtls.as_ref()).await?,
        ))
    };
    let source_shard_id = if use_router {
        Some(source.parse::<u32>()?)
    } else {
        None
    };
    let target_shard_id = if use_router {
        Some(target.parse::<u32>()?)
    } else {
        None
    };

    let mut next_start_id = start_id;
    let mut total_imported = 0u64;
    if use_stream {
        let (tx, rx) = tokio::sync::mpsc::channel::<ImportRecordsChunk>(4);
        let import_task = if let Some(client) = router_client.as_ref() {
            let mut client = client.clone();
            let shard_id = target_shard_id.expect("target shard id");
            let stream = ReceiverStream::new(rx).map(move |chunk| RouterImportRecordsRequest {
                shard_id,
                records: chunk.records,
            });
            tokio::spawn(async move {
                client
                    .import_records_stream(stream)
                    .await
                    .map(|response| response.into_inner())
                    .map_err(|err| anyhow::anyhow!(err.to_string()))
            })
        } else {
            let mut client = target_client.take().expect("target client");
            tokio::spawn(async move {
                client
                    .import_records_stream(ReceiverStream::new(rx))
                    .await
                    .map(|response| response.into_inner())
                    .map_err(|err| anyhow::anyhow!(err.to_string()))
            })
        };

        let mut export_stream: std::pin::Pin<
            Box<
                dyn tokio_stream::Stream<
                        Item = Result<
                            unirust_rs::distributed::proto::ExportRecordsChunk,
                            anyhow::Error,
                        >,
                    > + Send,
            >,
        > = if let Some(client) = router_client.as_mut() {
            let shard_id = source_shard_id.expect("source shard id");
            let stream = client
                .export_records_stream(RouterExportRecordsRequest {
                    shard_id,
                    start_id,
                    end_id,
                    limit: batch_size,
                })
                .await?
                .into_inner()
                .map(|item| item.map_err(|err| anyhow::anyhow!(err.to_string())));
            Box::pin(stream)
        } else {
            let stream = source_client
                .as_mut()
                .expect("source client")
                .export_records_stream(ExportRecordsRequest {
                    start_id,
                    end_id,
                    limit: batch_size,
                })
                .await?
                .into_inner()
                .map(|item| item.map_err(|err| anyhow::anyhow!(err.to_string())));
            Box::pin(stream)
        };

        while let Some(chunk) = export_stream.next().await {
            let chunk = chunk?;
            if chunk.records.is_empty() {
                continue;
            }
            let count = chunk.records.len() as u64;
            tx.send(ImportRecordsChunk {
                records: chunk.records,
                internal_protocol_version: unirust_rs::distributed::DISTRIBUTED_PROTOCOL_VERSION,
            })
            .await
            .map_err(|_| anyhow::anyhow!("import stream closed"))?;
            total_imported += count;
            println!("Imported {} records (total {})", count, total_imported);
        }
        drop(tx);

        let response = import_task.await??;
        total_imported = response.imported;
    } else {
        loop {
            let response = if let Some(client) = router_client.as_mut() {
                let shard_id = source_shard_id.expect("source shard id");
                client
                    .export_records(RouterExportRecordsRequest {
                        shard_id,
                        start_id: next_start_id,
                        end_id,
                        limit: batch_size,
                    })
                    .await?
                    .into_inner()
            } else {
                source_client
                    .as_mut()
                    .expect("source client")
                    .export_records(ExportRecordsRequest {
                        start_id: next_start_id,
                        end_id,
                        limit: batch_size,
                    })
                    .await?
                    .into_inner()
            };

            if response.records.is_empty() {
                break;
            }

            let imported = if let Some(client) = router_client.as_mut() {
                let shard_id = target_shard_id.expect("target shard id");
                client
                    .import_records(RouterImportRecordsRequest {
                        shard_id,
                        records: response.records,
                    })
                    .await?
                    .into_inner()
                    .imported
            } else {
                target_client
                    .as_mut()
                    .expect("target client")
                    .import_records(ImportRecordsRequest {
                        records: response.records,
                        internal_protocol_version:
                            unirust_rs::distributed::DISTRIBUTED_PROTOCOL_VERSION,
                    })
                    .await?
                    .into_inner()
                    .imported
            };
            total_imported += imported;
            println!("Imported {} records (total {})", imported, total_imported);

            if !response.has_more {
                break;
            }
            if response.next_start_id == 0 {
                return Err(anyhow::anyhow!(
                    "export indicated more records but next_start_id is 0"
                ));
            }
            next_start_id = response.next_start_id;
        }
    }

    println!("Done. Total imported: {}", total_imported);
    Ok(())
}
