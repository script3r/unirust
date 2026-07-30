use anyhow::{anyhow, Context, Result};
use std::path::PathBuf;
use unirust_rs::{export_cluster_backup, prune_verified_cluster_backups, verify_cluster_backup};

fn usage() -> &'static str {
    "Usage:
  unirust_backup export --destination <path> --checkpoint <path> [--checkpoint <path> ...]
  unirust_backup verify --backup <path>
  unirust_backup prune --root <path> --retain <count>"
}

fn required_value(args: &[String], index: &mut usize, flag: &str) -> Result<String> {
    *index += 1;
    args.get(*index)
        .filter(|value| !value.starts_with("--"))
        .cloned()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn export(args: &[String]) -> Result<()> {
    let mut destination = None;
    let mut checkpoints = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--destination" => {
                destination = Some(PathBuf::from(required_value(
                    args,
                    &mut index,
                    "--destination",
                )?));
            }
            "--checkpoint" => checkpoints.push(PathBuf::from(required_value(
                args,
                &mut index,
                "--checkpoint",
            )?)),
            "--help" | "-h" => {
                println!("{}", usage());
                return Ok(());
            }
            flag => anyhow::bail!("unknown export argument {flag}\n{}", usage()),
        }
        index += 1;
    }
    let destination =
        destination.ok_or_else(|| anyhow!("export requires --destination\n{}", usage()))?;
    let manifest = export_cluster_backup(&checkpoints, &destination)?;
    println!(
        "exported generation {} with {} shards to {}",
        manifest.generation(),
        manifest.shard_count(),
        destination.display()
    );
    Ok(())
}

fn verify(args: &[String]) -> Result<()> {
    let mut backup = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--backup" => {
                backup = Some(PathBuf::from(required_value(args, &mut index, "--backup")?));
            }
            "--help" | "-h" => {
                println!("{}", usage());
                return Ok(());
            }
            flag => anyhow::bail!("unknown verify argument {flag}\n{}", usage()),
        }
        index += 1;
    }
    let backup = backup.ok_or_else(|| anyhow!("verify requires --backup\n{}", usage()))?;
    let manifest = verify_cluster_backup(&backup)?;
    println!(
        "verified generation {} with {} shards at {}",
        manifest.generation(),
        manifest.shard_count(),
        backup.display()
    );
    Ok(())
}

fn prune(args: &[String]) -> Result<()> {
    let mut root = None;
    let mut retain = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--root" => {
                root = Some(PathBuf::from(required_value(args, &mut index, "--root")?));
            }
            "--retain" => {
                let value = required_value(args, &mut index, "--retain")?;
                retain = Some(
                    value
                        .parse::<usize>()
                        .with_context(|| format!("invalid --retain value {value}"))?,
                );
            }
            "--help" | "-h" => {
                println!("{}", usage());
                return Ok(());
            }
            flag => anyhow::bail!("unknown prune argument {flag}\n{}", usage()),
        }
        index += 1;
    }
    let root = root.ok_or_else(|| anyhow!("prune requires --root\n{}", usage()))?;
    let retain = retain.ok_or_else(|| anyhow!("prune requires --retain\n{}", usage()))?;
    let removed = prune_verified_cluster_backups(&root, retain)?;
    println!(
        "removed {} verified backup generation(s) from {}",
        removed.len(),
        root.display()
    );
    Ok(())
}

fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let Some(command) = args.first() else {
        anyhow::bail!("{}", usage());
    };
    match command.as_str() {
        "export" => export(&args[1..]),
        "verify" => verify(&args[1..]),
        "prune" => prune(&args[1..]),
        "--help" | "-h" => {
            println!("{}", usage());
            Ok(())
        }
        command => anyhow::bail!("unknown command {command}\n{}", usage()),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("backup operation failed: {error:#}");
        std::process::exit(1);
    }
}
