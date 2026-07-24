use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::tempdir;
use tokio::time::sleep;
use unirust_rs::distributed::proto::{
    self, shard_service_client::ShardServiceClient, IngestRecordsRequest, QueryDescriptor,
    QueryEntitiesRequest, RecordDescriptor, RecordIdentity, RecordInput, StatsRequest,
};

mod support;

struct ShardProcess {
    child: Child,
}

impl ShardProcess {
    fn spawn(listen: SocketAddr, data_dir: &Path, ontology_path: &Path) -> anyhow::Result<Self> {
        let child = Command::new(env!("CARGO_BIN_EXE_unirust_shard"))
            .args([
                "--listen",
                &listen.to_string(),
                "--shard-id",
                "0",
                "--data-dir",
                data_dir
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("data directory is not valid UTF-8"))?,
                "--ontology",
                ontology_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("ontology path is not valid UTF-8"))?,
                "--profile",
                "balanced",
            ])
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

    #[cfg(unix)]
    async fn terminate_and_wait(&mut self) -> anyhow::Result<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()?;
        if !status.success() {
            anyhow::bail!("failed to send SIGTERM to shard process");
        }
        for _ in 0..100 {
            if self.child.try_wait()?.is_some() {
                return Ok(());
            }
            sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!("shard did not exit after SIGTERM");
    }
}

impl Drop for ShardProcess {
    fn drop(&mut self) {
        let _ = self.kill_and_wait();
    }
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
    for _ in 0..100 {
        if let Some(status) = process.child.try_wait()? {
            anyhow::bail!("shard exited before becoming ready: {status}");
        }
        if let Ok(client) = ShardServiceClient::connect(endpoint.clone()).await {
            return Ok(client);
        }
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("shard did not become ready at {endpoint}")
}

fn record(index: u32, uid: String, email: String) -> RecordInput {
    RecordInput {
        index,
        identity: Some(RecordIdentity {
            entity_type: "person".to_string(),
            perspective: "crm".to_string(),
            uid,
        }),
        descriptors: vec![RecordDescriptor {
            attr: "email".to_string(),
            value: email,
            start: 0,
            end: 10,
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_ingest_survives_process_kill_and_restart() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let data_dir = temp_dir.path().join("shard-data");
    let ontology_path = temp_dir.path().join("ontology.json");
    std::fs::write(
        &ontology_path,
        serde_json::to_vec(&support::build_iam_config())?,
    )?;

    let first_addr = available_addr()?;
    let mut process = ShardProcess::spawn(first_addr, &data_dir, &ontology_path)?;
    let mut client = wait_for_shard(first_addr, &mut process).await?;

    let records = (0..128)
        .map(|index| {
            record(
                index,
                format!("process-kill-{index}"),
                format!("entity-{}@example.com", index / 2),
            )
        })
        .collect();
    let response = client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 3,
            records,
        })
        .await?
        .into_inner();
    assert_eq!(response.assignments.len(), 128);
    let original_cluster = response
        .assignments
        .iter()
        .find(|assignment| assignment.index == 0)
        .expect("assignment for first record")
        .cluster_id;

    let stats = client.get_stats(StatsRequest {}).await?.into_inner();
    assert_eq!(stats.record_count, 128);
    assert_eq!(stats.cluster_count, 64);

    drop(client);
    process.kill_and_wait()?;

    let second_addr = available_addr()?;
    let mut restarted = ShardProcess::spawn(second_addr, &data_dir, &ontology_path)?;
    let mut client = wait_for_shard(second_addr, &mut restarted).await?;

    let stats = client.get_stats(StatsRequest {}).await?.into_inner();
    assert_eq!(stats.record_count, 128);
    assert_eq!(stats.cluster_count, 64);

    let query = client
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
    match query.outcome {
        Some(proto::query_entities_response::Outcome::Matches(matches)) => {
            assert_eq!(matches.matches.len(), 1);
        }
        other => anyhow::bail!("expected one recovered entity-resolution match, got {other:?}"),
    }

    let linked_after_restart = client
        .ingest_records(IngestRecordsRequest {
            internal_protocol_version: 3,
            records: vec![record(
                128,
                "process-kill-after-restart".to_string(),
                "entity-0@example.com".to_string(),
            )],
        })
        .await?
        .into_inner();
    assert_eq!(linked_after_restart.assignments.len(), 1);
    assert_eq!(
        linked_after_restart.assignments[0].cluster_id, original_cluster,
        "a post-restart record must resolve into the pre-crash entity"
    );

    let stats = client.get_stats(StatsRequest {}).await?.into_inner();
    assert_eq!(stats.record_count, 129);
    assert_eq!(stats.cluster_count, 64);

    #[cfg(unix)]
    {
        drop(client);
        restarted.terminate_and_wait().await?;

        let third_addr = available_addr()?;
        let mut restarted_after_shutdown =
            ShardProcess::spawn(third_addr, &data_dir, &ontology_path)?;
        let mut client = wait_for_shard(third_addr, &mut restarted_after_shutdown).await?;
        let stats = client.get_stats(StatsRequest {}).await?.into_inner();
        assert_eq!(stats.record_count, 129);
        assert_eq!(stats.cluster_count, 64);

        let linked_after_shutdown = client
            .ingest_records(IngestRecordsRequest {
                internal_protocol_version: 3,
                records: vec![record(
                    129,
                    "graceful-shutdown-after-restart".to_string(),
                    "entity-0@example.com".to_string(),
                )],
            })
            .await?
            .into_inner();
        assert_eq!(linked_after_shutdown.assignments.len(), 1);
        assert_eq!(
            linked_after_shutdown.assignments[0].cluster_id, original_cluster,
            "a graceful restart must preserve entity-resolution identity"
        );
    }

    Ok(())
}
