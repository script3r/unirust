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
