use std::process::Command;

#[test]
fn router_binary_reads_comma_separated_shards_from_environment() {
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_router"))
        .env_clear()
        .env("UNIRUST_ROUTER_SHARDS", "http://[::1,http://127.0.0.1:1")
        .output()
        .expect("run router binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("invalid URI"),
        "environment shard list was not applied: {stderr}"
    );
}

#[test]
fn router_cli_shards_override_environment() {
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_router"))
        .env_clear()
        .env("UNIRUST_ROUTER_SHARDS", "http://[::1")
        .args(["--shards", ""])
        .output()
        .expect("run router binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("at least one shard address is required"),
        "CLI did not override environment shard list: {stderr}"
    );
}

#[test]
fn router_binary_rejects_unknown_router_environment_variable() {
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_router"))
        .env_clear()
        .env("UNIRUST_ROUTER_CHECKPOINT_INTERVL_SECS", "60")
        .output()
        .expect("run router binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(
            "unknown Unirust configuration variable UNIRUST_ROUTER_CHECKPOINT_INTERVL_SECS"
        ),
        "unexpected stderr: {stderr}"
    );
}
