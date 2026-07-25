use std::process::Command;

use tempfile::tempdir;

#[test]
fn shard_binary_refuses_implicit_ephemeral_storage() {
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_shard"))
        .env_clear()
        .output()
        .expect("run shard binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("persistent shard storage is required"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn shard_binary_reads_persistent_path_from_environment() {
    let source = tempdir().expect("temporary checkpoint source");
    let destination = source.path().join("replacement");
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_shard"))
        .env_clear()
        .env("UNIRUST_SHARD_DATA_DIR", &destination)
        .args([
            "--restore-from",
            source.path().to_str().expect("UTF-8 path"),
        ])
        .output()
        .expect("run shard binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not a RocksDB checkpoint"),
        "environment data directory was not applied: {stderr}"
    );
    assert!(
        !stderr.contains("persistent shard storage is required"),
        "environment data directory was ignored: {stderr}"
    );
}

#[test]
fn shard_binary_rejects_unknown_shard_environment_variable() {
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_shard"))
        .env_clear()
        .env("UNIRUST_SHARD_DAT_DIR", "/tmp/unirust-typo")
        .output()
        .expect("run shard binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unknown Unirust configuration variable UNIRUST_SHARD_DAT_DIR"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn shard_binary_rejects_partial_mtls_configuration() {
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_shard"))
        .env_clear()
        .args(["--tls-cert", "/tmp/server.pem"])
        .output()
        .expect("run shard binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("shard mTLS requires all three certificate paths together"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn shard_binary_rejects_replica_mode_without_a_token() {
    let data = tempdir().expect("temporary data directory");
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_shard"))
        .env_clear()
        .args([
            "--data-dir",
            data.path().to_str().expect("UTF-8 path"),
            "--replica-mode",
        ])
        .output()
        .expect("run shard binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("replication_token_file"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn shard_binary_rejects_short_replication_token() {
    let data = tempdir().expect("temporary data directory");
    let token_dir = tempdir().expect("temporary token directory");
    let token = token_dir.path().join("replication.token");
    std::fs::write(&token, b"too short").expect("write token");
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_shard"))
        .env_clear()
        .args([
            "--data-dir",
            data.path().to_str().expect("UTF-8 path"),
            "--replica-mode",
            "--allow-insecure-replication",
            "--replication-token-file",
            token.to_str().expect("UTF-8 path"),
        ])
        .output()
        .expect("run shard binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("replication token must contain at least 32 bytes"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn shard_binary_requires_mtls_for_replication_by_default() {
    let data = tempdir().expect("temporary data directory");
    let token_dir = tempdir().expect("temporary token directory");
    let token = token_dir.path().join("replication.token");
    std::fs::write(&token, [b'x'; 32]).expect("write token");
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_shard"))
        .env_clear()
        .args([
            "--data-dir",
            data.path().to_str().expect("UTF-8 path"),
            "--replica-mode",
            "--replication-token-file",
            token.to_str().expect("UTF-8 path"),
        ])
        .output()
        .expect("run shard binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("replica mode requires shard server mTLS"),
        "unexpected stderr: {stderr}"
    );
}
