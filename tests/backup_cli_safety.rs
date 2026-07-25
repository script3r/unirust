use std::process::Command;

#[test]
fn backup_cli_requires_an_explicit_operation() {
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_backup"))
        .output()
        .expect("run backup binary");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Usage:"));
}

#[test]
fn backup_cli_rejects_incomplete_export_arguments() {
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_backup"))
        .args(["export", "--destination", "/tmp/not-created"])
        .output()
        .expect("run backup binary");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("at least one committed shard checkpoint"));
}

#[test]
fn backup_cli_rejects_zero_retention() {
    let root = tempfile::tempdir().expect("temporary retention root");
    let output = Command::new(env!("CARGO_BIN_EXE_unirust_backup"))
        .args([
            "prune",
            "--root",
            root.path().to_str().expect("UTF-8 path"),
            "--retain",
            "0",
        ])
        .output()
        .expect("run backup binary");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("keep at least one generation"));
}
