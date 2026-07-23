use std::process::Command;

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
