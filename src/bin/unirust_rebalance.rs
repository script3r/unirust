use unirust_rs::distributed::proto::router_service_client::RouterServiceClient;
use unirust_rs::distributed::proto::shard_service_client::ShardServiceClient;
use unirust_rs::distributed::proto::{
    ExportRecordsRequest, ImportRecordsRequest, RecordIdRangeRequest,
    RouterExportRecordsRequest, RouterImportRecordsRequest, RouterRecordIdRangeRequest,
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

fn normalize_addr(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let router = parse_arg("--router");
    if has_flag("--range") {
        let shard_arg = parse_arg("--shard");
        let shard_id_arg = parse_arg("--shard-id");
        let response = if let Some(router) = router.clone() {
            let shard_id: u32 = shard_id_arg
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--shard-id is required with --router"))?
                .parse()?;
            let mut client = RouterServiceClient::connect(normalize_addr(&router)).await?;
            client
                .get_record_id_range(RouterRecordIdRangeRequest { shard_id })
                .await?
                .into_inner()
        } else {
            let shard = shard_arg
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--shard is required for --range"))?;
            let mut client = ShardServiceClient::connect(normalize_addr(&shard)).await?;
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
        } else {
            if let Some(shard_id) = shard_id_arg.as_deref() {
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
        }
        return Ok(());
    }

    let source = parse_arg("--source")
        .ok_or_else(|| anyhow::anyhow!("--source is required"))?;
    let target = parse_arg("--target")
        .ok_or_else(|| anyhow::anyhow!("--target is required"))?;
    let start_id: u32 = parse_arg("--start-id")
        .unwrap_or_else(|| "0".to_string())
        .parse()?;
    let end_id: u32 = parse_arg("--end-id")
        .unwrap_or_else(|| "0".to_string())
        .parse()?;
    let batch_size: u32 = parse_arg("--batch-size")
        .unwrap_or_else(|| "1000".to_string())
        .parse()?;

    let use_router = router.is_some();
    let mut router_client = if let Some(router) = router {
        Some(RouterServiceClient::connect(normalize_addr(&router)).await?)
    } else {
        None
    };
    let mut source_client = if use_router {
        None
    } else {
        Some(ShardServiceClient::connect(normalize_addr(&source)).await?)
    };
    let mut target_client = if use_router {
        None
    } else {
        Some(ShardServiceClient::connect(normalize_addr(&target)).await?)
    };
    let source_shard_id = if use_router { Some(source.parse::<u32>()?) } else { None };
    let target_shard_id = if use_router { Some(target.parse::<u32>()?) } else { None };

    let mut next_start_id = start_id;
    let mut total_imported = 0u64;
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
                })
                .await?
                .into_inner()
                .imported
        };
        total_imported += imported;
        println!(
            "Imported {} records (total {})",
            imported, total_imported
        );

        if !response.has_more {
            break;
        }
        if response.next_start_id == 0 {
            return Err(anyhow::anyhow!("export indicated more records but next_start_id is 0"));
        }
        next_start_id = response.next_start_id;
    }

    println!("Done. Total imported: {}", total_imported);
    Ok(())
}
