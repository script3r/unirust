use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::tempdir;
use tokio::time::sleep;
use unirust_rs::distributed::proto::{
    shard_service_client::ShardServiceClient, IngestRecordsRequest, QueryDescriptor,
    QueryEntitiesRequest, RecordDescriptor, RecordIdentity, RecordInput, StatsRequest,
};
use unirust_rs::distributed::DISTRIBUTED_PROTOCOL_VERSION;

mod support;

const REPLICATION_SECRET: &[u8; 32] = b"replication-test-secret-32-bytes";

struct ShardProcess {
    child: Child,
}

struct ShardPaths<'a> {
    data: &'a Path,
    backup: &'a Path,
    ontology: &'a Path,
}

impl ShardProcess {
    fn spawn_replica(
        listen: SocketAddr,
        paths: ShardPaths<'_>,
        token: &Path,
    ) -> anyhow::Result<Self> {
        Self::spawn(
            listen,
            paths,
            &[
                "--replica-mode".to_string(),
                "--allow-insecure-replication".to_string(),
                "--replication-token-file".to_string(),
                path_arg(token)?,
            ],
        )
    }

    fn spawn_primary(
        listen: SocketAddr,
        paths: ShardPaths<'_>,
        token: &Path,
        replica: SocketAddr,
    ) -> anyhow::Result<Self> {
        Self::spawn(
            listen,
            paths,
            &[
                "--replica".to_string(),
                format!("http://{replica}"),
                "--allow-insecure-replication".to_string(),
                "--replication-token-file".to_string(),
                path_arg(token)?,
            ],
        )
    }

    fn spawn_standalone(listen: SocketAddr, paths: ShardPaths<'_>) -> anyhow::Result<Self> {
        Self::spawn(listen, paths, &[])
    }

    fn spawn(
        listen: SocketAddr,
        paths: ShardPaths<'_>,
        extra_args: &[String],
    ) -> anyhow::Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_unirust_shard"));
        command.args([
            "--listen",
            &listen.to_string(),
            "--shard-id",
            "0",
            "--data-dir",
            &path_arg(paths.data)?,
            "--backup-dir",
            &path_arg(paths.backup)?,
            "--ontology",
            &path_arg(paths.ontology)?,
            "--profile",
            "balanced",
        ]);
        let child = command
            .args(extra_args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self { child })
    }

    fn kill_and_wait(&mut self) -> anyhow::Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        let _ = self.child.wait()?;
        Ok(())
    }
}

impl Drop for ShardProcess {
    fn drop(&mut self) {
        let _ = self.kill_and_wait();
    }
}

fn path_arg(path: &Path) -> anyhow::Result<String> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("path is not valid UTF-8: {}", path.display()))
}

fn available_addr() -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(addr)
}

async fn wait_for_shard(
    addr: SocketAddr,
    process: &mut ShardProcess,
) -> anyhow::Result<ShardServiceClient<tonic::transport::Channel>> {
    let endpoint = format!("http://{addr}");
    for _ in 0..200 {
        if let Some(status) = process.child.try_wait()? {
            anyhow::bail!("shard exited before becoming ready: {status}");
        }
        if let Ok(mut client) = ShardServiceClient::connect(endpoint.clone()).await {
            if client
                .health_check(unirust_rs::distributed::proto::HealthCheckRequest {})
                .await
                .is_ok()
            {
                return Ok(client);
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("shard did not become ready at {endpoint}")
}

fn record(index: u32, source: u32) -> RecordInput {
    RecordInput {
        index,
        identity: Some(RecordIdentity {
            entity_type: "person".to_string(),
            perspective: "crm".to_string(),
            uid: format!("replicated-process-{source}"),
        }),
        descriptors: vec![RecordDescriptor {
            attr: "email".to_string(),
            value: format!("entity-{}@example.com", source / 2),
            start: 0,
            end: 10,
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_replica_batch_survives_sigkill_and_primary_volume_loss() -> anyhow::Result<()>
{
    let temp = tempdir()?;
    let primary_data = temp.path().join("primary-data");
    let replica_data = temp.path().join("replica-data");
    let primary_backup = temp.path().join("primary-backup");
    let replica_backup = temp.path().join("replica-backup");
    let ontology = temp.path().join("ontology.json");
    let token = temp.path().join("replication.token");
    std::fs::write(&ontology, serde_json::to_vec(&support::build_iam_config())?)?;
    std::fs::write(&token, REPLICATION_SECRET)?;

    let replica_addr = available_addr()?;
    let mut replica = ShardProcess::spawn_replica(
        replica_addr,
        ShardPaths {
            data: &replica_data,
            backup: &replica_backup,
            ontology: &ontology,
        },
        &token,
    )?;
    let _ = wait_for_shard(replica_addr, &mut replica).await?;

    let primary_addr = available_addr()?;
    let mut primary = ShardProcess::spawn_primary(
        primary_addr,
        ShardPaths {
            data: &primary_data,
            backup: &primary_backup,
            ontology: &ontology,
        },
        &token,
        replica_addr,
    )?;
    let mut client = wait_for_shard(primary_addr, &mut primary).await?;
    let response = client
        .ingest_records(IngestRecordsRequest {
            records: (0..128).map(|source| record(source, source)).collect(),
            internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
        })
        .await?
        .into_inner();
    assert_eq!(response.assignments.len(), 128);
    assert_eq!(
        client
            .get_stats(StatsRequest {})
            .await?
            .into_inner()
            .record_count,
        128
    );

    drop(client);
    primary.kill_and_wait()?;
    replica.kill_and_wait()?;

    let restarted_replica_addr = available_addr()?;
    let mut restarted_replica = ShardProcess::spawn_replica(
        restarted_replica_addr,
        ShardPaths {
            data: &replica_data,
            backup: &replica_backup,
            ontology: &ontology,
        },
        &token,
    )?;
    let _ = wait_for_shard(restarted_replica_addr, &mut restarted_replica).await?;
    let restarted_primary_addr = available_addr()?;
    let mut restarted_primary = ShardProcess::spawn_primary(
        restarted_primary_addr,
        ShardPaths {
            data: &primary_data,
            backup: &primary_backup,
            ontology: &ontology,
        },
        &token,
        restarted_replica_addr,
    )?;
    let mut restarted_client =
        wait_for_shard(restarted_primary_addr, &mut restarted_primary).await?;
    let stats = restarted_client
        .get_stats(StatsRequest {})
        .await?
        .into_inner();
    assert_eq!(stats.record_count, 128);
    assert_eq!(stats.cluster_count, 64);

    drop(restarted_client);
    restarted_primary.kill_and_wait()?;
    restarted_replica.kill_and_wait()?;
    std::fs::remove_dir_all(&primary_data)?;

    let promoted_addr = available_addr()?;
    let mut promoted = ShardProcess::spawn_standalone(
        promoted_addr,
        ShardPaths {
            data: &replica_data,
            backup: &replica_backup,
            ontology: &ontology,
        },
    )?;
    let mut promoted_client = wait_for_shard(promoted_addr, &mut promoted).await?;
    let query = promoted_client
        .query_entities(QueryEntitiesRequest {
            descriptors: vec![QueryDescriptor {
                attr: "email".to_string(),
                value: "entity-0@example.com".to_string(),
            }],
            start: 0,
            end: 10,
        })
        .await?
        .into_inner();
    assert!(query.outcome.is_some());

    let retry = promoted_client
        .ingest_records(IngestRecordsRequest {
            records: vec![record(999, 0)],
            internal_protocol_version: DISTRIBUTED_PROTOCOL_VERSION,
        })
        .await?
        .into_inner();
    assert_eq!(retry.assignments.len(), 1);
    assert_eq!(
        promoted_client
            .get_stats(StatsRequest {})
            .await?
            .into_inner()
            .record_count,
        128
    );
    assert!(!primary_data.exists());
    Ok(())
}
