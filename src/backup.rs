use crate::persistence::{
    verify_cluster_checkpoint, ClusterCheckpointManifest, CHECKPOINT_PROTOCOL_VERSION,
};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const BACKUP_FORMAT_VERSION: u32 = 1;
const BACKUP_MANIFEST_FILE: &str = "UNIRUST_BACKUP_MANIFEST";
const BACKUP_COMMITTED_FILE: &str = "UNIRUST_BACKUP_COMMITTED";
const COPY_BUFFER_SIZE: usize = 1024 * 1024;
const MAX_BACKUP_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BackupFile {
    path: String,
    length: u64,
    sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BackupShard {
    shard_id: u32,
    files: Vec<BackupFile>,
}

struct ValidatedCheckpointSet {
    generation: String,
    shard_count: u32,
    checkpoints: Vec<(PathBuf, ClusterCheckpointManifest)>,
}

/// Integrity and topology metadata for one exported cluster checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBackupManifest {
    format_version: u32,
    checkpoint_protocol_version: u32,
    generation: String,
    shard_count: u32,
    created_unix_nanos: u128,
    shards: Vec<BackupShard>,
}

impl ClusterBackupManifest {
    pub fn generation(&self) -> &str {
        &self.generation
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    pub fn created_unix_nanos(&self) -> u128 {
        self.created_unix_nanos
    }

    fn validate(&self) -> Result<()> {
        if self.format_version != BACKUP_FORMAT_VERSION {
            anyhow::bail!(
                "unsupported cluster backup format version {}",
                self.format_version
            );
        }
        if self.checkpoint_protocol_version != CHECKPOINT_PROTOCOL_VERSION {
            anyhow::bail!(
                "unsupported checkpoint protocol version {}",
                self.checkpoint_protocol_version
            );
        }
        if self.generation.is_empty() {
            anyhow::bail!("cluster backup generation is empty");
        }
        if self.shard_count == 0 || self.shards.len() != self.shard_count as usize {
            anyhow::bail!(
                "cluster backup contains {} shards but declares {}",
                self.shards.len(),
                self.shard_count
            );
        }
        for (expected, shard) in self.shards.iter().enumerate() {
            if shard.shard_id != expected as u32 {
                anyhow::bail!(
                    "cluster backup shard sequence contains {} at position {}",
                    shard.shard_id,
                    expected
                );
            }
            if shard.files.is_empty() {
                anyhow::bail!("cluster backup shard {} has no files", shard.shard_id);
            }
            let mut previous = None;
            for file in &shard.files {
                validate_relative_file_path(&file.path)?;
                if previous.is_some_and(|value: &str| value >= file.path.as_str()) {
                    anyhow::bail!(
                        "cluster backup shard {} file list is not strictly ordered",
                        shard.shard_id
                    );
                }
                previous = Some(file.path.as_str());
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn validate_relative_file_path(path: &str) -> Result<()> {
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        anyhow::bail!("backup file path must be a safe relative path");
    }
    Ok(())
}

fn sorted_entries(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    Ok(entries)
}

fn digest_file(path: &Path) -> Result<(u64, [u8; 32])> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        anyhow::bail!("backup entry {} must be a real file", path.display());
    }
    let mut source = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut length = 0u64;
    let mut buffer = vec![0u8; COPY_BUFFER_SIZE];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| anyhow!("backup file length overflow"))?;
    }
    Ok((length, digest.finalize().into()))
}

fn copy_file(source: &Path, destination: &Path) -> Result<(u64, [u8; 32])> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        anyhow::bail!("checkpoint entry {} must be a real file", source.display());
    }
    let mut source_file = fs::File::open(source)?;
    let mut destination_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut digest = Sha256::new();
    let mut length = 0u64;
    let mut buffer = vec![0u8; COPY_BUFFER_SIZE];
    loop {
        let read = source_file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        destination_file.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| anyhow!("backup file length overflow"))?;
    }
    destination_file.sync_all()?;
    fs::set_permissions(destination, metadata.permissions())?;
    Ok((length, digest.finalize().into()))
}

fn copy_tree(
    source: &Path,
    destination: &Path,
    relative: &Path,
    files: &mut Vec<BackupFile>,
) -> Result<()> {
    fs::create_dir(destination)?;
    for entry in sorted_entries(source)? {
        let file_type = entry.file_type()?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let relative_path = relative.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree(&source_path, &destination_path, &relative_path, files)?;
        } else if file_type.is_file() {
            let path = relative_path
                .to_str()
                .ok_or_else(|| anyhow!("checkpoint contains a non-UTF-8 file name"))?
                .to_string();
            validate_relative_file_path(&path)?;
            let (length, sha256) = copy_file(&source_path, &destination_path)?;
            files.push(BackupFile {
                path,
                length,
                sha256,
            });
        } else {
            anyhow::bail!(
                "checkpoint contains unsupported entry {}",
                source_path.display()
            );
        }
    }
    sync_directory(destination)?;
    Ok(())
}

fn hash_tree(
    root: &Path,
    current: &Path,
    relative: &Path,
    files: &mut Vec<BackupFile>,
) -> Result<()> {
    let metadata = fs::symlink_metadata(current)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!(
            "backup directory {} must be a real directory",
            current.display()
        );
    }
    for entry in sorted_entries(current)? {
        let file_type = entry.file_type()?;
        let path = entry.path();
        let relative_path = relative.join(entry.file_name());
        if file_type.is_dir() {
            hash_tree(root, &path, &relative_path, files)?;
        } else if file_type.is_file() {
            let relative_string = relative_path
                .to_str()
                .ok_or_else(|| anyhow!("backup contains a non-UTF-8 file name"))?
                .to_string();
            validate_relative_file_path(&relative_string)?;
            let (length, sha256) = digest_file(&path)?;
            files.push(BackupFile {
                path: relative_string,
                length,
                sha256,
            });
        } else {
            anyhow::bail!("backup contains unsupported entry {}", path.display());
        }
    }
    if !current.starts_with(root) {
        anyhow::bail!("backup traversal escaped its shard directory");
    }
    Ok(())
}

fn write_marker(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn read_marker(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        anyhow::bail!("backup marker {} must be a real file", path.display());
    }
    if metadata.len() > MAX_BACKUP_MANIFEST_BYTES {
        anyhow::bail!("backup marker {} is unreasonably large", path.display());
    }
    Ok(fs::read(path)?)
}

fn validate_checkpoint_set(checkpoints: &[PathBuf]) -> Result<ValidatedCheckpointSet> {
    if checkpoints.is_empty() {
        anyhow::bail!("at least one committed shard checkpoint is required");
    }
    let mut validated = Vec::with_capacity(checkpoints.len());
    for checkpoint in checkpoints {
        let manifest = verify_cluster_checkpoint(checkpoint)
            .with_context(|| format!("invalid checkpoint {}", checkpoint.display()))?;
        validated.push((checkpoint.canonicalize()?, manifest));
    }
    validated.sort_by_key(|(_, manifest)| manifest.shard_id());
    let generation = validated[0].1.generation().to_string();
    let shard_count = validated[0].1.shard_count();
    if validated.len() != shard_count as usize {
        anyhow::bail!(
            "checkpoint set contains {} shards but generation {} requires {}",
            validated.len(),
            generation,
            shard_count
        );
    }
    for (expected, (_, manifest)) in validated.iter().enumerate() {
        if manifest.generation() != generation {
            anyhow::bail!(
                "checkpoint set mixes generations {} and {}",
                generation,
                manifest.generation()
            );
        }
        if manifest.shard_count() != shard_count {
            anyhow::bail!("checkpoint set mixes shard counts");
        }
        if manifest.shard_id() != expected as u32 {
            anyhow::bail!(
                "checkpoint set is missing shard {} or contains a duplicate",
                expected
            );
        }
    }
    Ok(ValidatedCheckpointSet {
        generation,
        shard_count,
        checkpoints: validated,
    })
}

/// Copy a complete coordinated checkpoint to another filesystem with an
/// integrity manifest and atomic publication.
pub fn export_cluster_backup(
    checkpoints: &[PathBuf],
    destination: &Path,
) -> Result<ClusterBackupManifest> {
    let validated = validate_checkpoint_set(checkpoints)?;
    if destination.exists() {
        anyhow::bail!(
            "backup destination {} already exists",
            destination.display()
        );
    }
    let destination_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("backup destination must have a UTF-8 directory name"))?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let parent = parent.canonicalize()?;
    let destination = parent.join(destination_name);
    let staging = parent.join(format!(".{destination_name}.tmp"));
    if staging.exists() {
        anyhow::bail!(
            "backup staging directory {} already exists; inspect and remove it before retrying",
            staging.display()
        );
    }
    for (checkpoint, _) in &validated.checkpoints {
        if destination.starts_with(checkpoint) || checkpoint.starts_with(&destination) {
            anyhow::bail!("backup destination and checkpoint sources must be disjoint");
        }
    }

    fs::create_dir(&staging)?;
    let export_result = (|| -> Result<ClusterBackupManifest> {
        let mut shards = Vec::with_capacity(validated.checkpoints.len());
        for (checkpoint, checkpoint_manifest) in validated.checkpoints {
            let shard_id = checkpoint_manifest.shard_id();
            let shard_dir = staging.join(format!("shard-{shard_id}"));
            let mut files = Vec::new();
            copy_tree(&checkpoint, &shard_dir, Path::new(""), &mut files)?;
            files.sort_by(|left, right| left.path.cmp(&right.path));
            shards.push(BackupShard { shard_id, files });
        }
        let manifest = ClusterBackupManifest {
            format_version: BACKUP_FORMAT_VERSION,
            checkpoint_protocol_version: CHECKPOINT_PROTOCOL_VERSION,
            generation: validated.generation,
            shard_count: validated.shard_count,
            created_unix_nanos: SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
            shards,
        };
        manifest.validate()?;
        let bytes = bincode::serialize(&manifest)?;
        write_marker(&staging.join(BACKUP_MANIFEST_FILE), &bytes)?;
        write_marker(&staging.join(BACKUP_COMMITTED_FILE), &bytes)?;
        sync_directory(&staging)?;
        let verified = verify_cluster_backup(&staging)?;
        if verified != manifest {
            anyhow::bail!("staged backup verification returned different metadata");
        }
        Ok(manifest)
    })();

    let manifest = export_result?;
    fs::rename(&staging, &destination)?;
    sync_directory(&parent)?;
    let published = verify_cluster_backup(&destination)?;
    if published != manifest {
        anyhow::bail!("published backup verification returned different metadata");
    }
    Ok(manifest)
}

/// Verify all bytes, shard identities, topology, and RocksDB contents in an
/// exported cluster backup.
pub fn verify_cluster_backup(backup: &Path) -> Result<ClusterBackupManifest> {
    let metadata = fs::symlink_metadata(backup)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!("cluster backup must be a real directory");
    }
    let backup = backup.canonicalize()?;
    let manifest_bytes = read_marker(&backup.join(BACKUP_MANIFEST_FILE))?;
    let committed_bytes = read_marker(&backup.join(BACKUP_COMMITTED_FILE))?;
    if committed_bytes != manifest_bytes {
        anyhow::bail!("cluster backup commit marker does not match its manifest");
    }
    let manifest: ClusterBackupManifest = bincode::deserialize(&manifest_bytes)?;
    manifest.validate()?;

    let expected_root_entries = manifest
        .shards
        .iter()
        .map(|shard| format!("shard-{}", shard.shard_id))
        .chain([
            BACKUP_MANIFEST_FILE.to_string(),
            BACKUP_COMMITTED_FILE.to_string(),
        ])
        .collect::<BTreeSet<_>>();
    let actual_root_entries = sorted_entries(&backup)?
        .into_iter()
        .map(|entry| {
            entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow!("backup contains a non-UTF-8 root entry"))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if actual_root_entries != expected_root_entries {
        anyhow::bail!("cluster backup contains missing or unexpected root entries");
    }

    for shard in &manifest.shards {
        let shard_dir = backup.join(format!("shard-{}", shard.shard_id));
        let mut actual_files = Vec::new();
        hash_tree(&shard_dir, &shard_dir, Path::new(""), &mut actual_files)?;
        actual_files.sort_by(|left, right| left.path.cmp(&right.path));
        if actual_files != shard.files {
            anyhow::bail!("backup shard {} failed file verification", shard.shard_id);
        }
        let checkpoint = verify_cluster_checkpoint(&shard_dir)?;
        if checkpoint.generation() != manifest.generation
            || checkpoint.shard_count() != manifest.shard_count
            || checkpoint.shard_id() != shard.shard_id
        {
            anyhow::bail!(
                "backup shard {} checkpoint provenance does not match the cluster manifest",
                shard.shard_id
            );
        }
    }
    Ok(manifest)
}

/// Retain the newest verified backup sets and remove older verified sets.
///
/// The operation fails before deleting anything if any entry under `root` is
/// incomplete, unexpected, or fails verification.
pub fn prune_verified_cluster_backups(root: &Path, retain: usize) -> Result<Vec<PathBuf>> {
    if retain == 0 {
        anyhow::bail!("backup retention must keep at least one generation");
    }
    let metadata = fs::symlink_metadata(root)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!("backup retention root must be a real directory");
    }
    let root = root.canonicalize()?;
    let mut backups = Vec::new();
    for entry in sorted_entries(&root)? {
        let path = entry.path();
        let file_type = entry.file_type()?;
        if !file_type.is_dir() || file_type.is_symlink() {
            anyhow::bail!(
                "backup retention root contains unexpected entry {}",
                path.display()
            );
        }
        let manifest = verify_cluster_backup(&path).with_context(|| {
            format!("refusing retention with invalid backup {}", path.display())
        })?;
        backups.push((manifest.created_unix_nanos(), path));
    }
    backups.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let remove_count = backups.len().saturating_sub(retain);
    let mut removed = Vec::with_capacity(remove_count);
    for (_, path) in backups.into_iter().take(remove_count) {
        fs::remove_dir_all(&path)?;
        sync_directory(&root)?;
        removed.push(path);
    }
    Ok(removed)
}
