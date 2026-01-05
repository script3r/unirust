//! Large-scale distributed load test TUI for Unirust.
//!
//! Generates cybersecurity entities and streams them to a distributed cluster
//! while displaying real-time performance metrics.
//!
//! Usage:
//! ```bash
//! cargo run --bin unirust_loadtest --features test-support -- \
//!   --count 100000 \
//!   --router http://127.0.0.1:50060 \
//!   --shards "127.0.0.1:50061,127.0.0.1:50062,127.0.0.1:50063" \
//!   --streams 4 \
//!   --batch 5000 \
//!   --overlap 0.1 \
//!   --log /tmp/loadtest.log
//! ```

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph, Row, Table},
    Frame, Terminal,
};
use tokio::sync::RwLock;
use tonic::transport::Channel;

use unirust_rs::distributed::proto::router_service_client::RouterServiceClient;
use unirust_rs::distributed::proto::shard_service_client::ShardServiceClient;
use unirust_rs::distributed::proto::{
    ApplyOntologyRequest, IdentityKeyConfig, IngestRecordsRequest, MetricsRequest, OntologyConfig,
    RecordDescriptor, RecordIdentity, RecordInput, StatsRequest,
};

// =============================================================================
// CONFIGURATION
// =============================================================================

#[derive(Debug, Clone)]
pub struct LoadTestConfig {
    pub count: u64,
    pub router_addr: String,
    pub shard_addrs: Vec<String>,
    pub stream_count: usize,
    pub batch_size: usize,
    pub overlap_probability: f64,
    pub conflict_probability: f64,
    pub cross_shard_probability: f64,
    pub log_file: Option<PathBuf>,
    pub seed: u64,
    pub headless: bool,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            count: 100_000,
            router_addr: "http://127.0.0.1:50060".to_string(),
            shard_addrs: vec![],
            stream_count: 4,
            batch_size: 5000,
            overlap_probability: 0.1,
            conflict_probability: 0.0,
            cross_shard_probability: 0.0,
            log_file: None,
            seed: 42,
            headless: false,
        }
    }
}

fn parse_args() -> LoadTestConfig {
    let mut config = LoadTestConfig::default();
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--count" | "-c" => {
                if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                    config.count = v;
                }
            }
            "--router" | "-r" => {
                if let Some(v) = args.next() {
                    config.router_addr = v;
                }
            }
            "--shards" | "-s" => {
                if let Some(v) = args.next() {
                    config.shard_addrs = v.split(',').map(|s| s.trim().to_string()).collect();
                }
            }
            "--streams" => {
                if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                    config.stream_count = v;
                }
            }
            "--batch" => {
                if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                    config.batch_size = v;
                }
            }
            "--overlap" => {
                if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                    config.overlap_probability = v;
                }
            }
            "--conflict" => {
                if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                    config.conflict_probability = v;
                }
            }
            "--cross-shard" => {
                if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                    config.cross_shard_probability = v;
                }
            }
            "--log" => {
                config.log_file = args.next().map(PathBuf::from);
            }
            "--seed" => {
                if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                    config.seed = v;
                }
            }
            "--headless" => {
                config.headless = true;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {}
        }
    }

    config
}

fn print_help() {
    println!(
        r#"unirust_loadtest - Large-scale distributed load test TUI

USAGE:
    unirust_loadtest [OPTIONS]

OPTIONS:
    -c, --count <N>       Total entities to generate (default: 100000)
    -r, --router <ADDR>   Router gRPC address (default: http://127.0.0.1:50060)
    -s, --shards <ADDRS>  Comma-separated shard addresses for direct metrics
    --streams <N>         Number of concurrent streams (default: 4)
    --batch <N>           Batch size per request (default: 5000)
    --overlap <F>         Overlap probability 0.0-1.0 (default: 0.1)
    --conflict <F>        Conflict probability 0.0-1.0 when overlapping (default: 0.0)
    --cross-shard <F>     Cross-shard merge probability 0.0-1.0 when overlapping (default: 0.0)
                          Creates records that partition to different shards but share
                          a secondary identity key, requiring cross-shard reconciliation.
    --log <PATH>          Log file path (logs to file only)
    --seed <N>            Random seed for reproducibility (default: 42)
    --headless            Run without TUI (for automation/CI)
    -h, --help            Print help

EXAMPLES:
    # Basic test with 100K entities
    unirust_loadtest --count 100000 --router http://127.0.0.1:50060

    # Large scale with custom parallelism
    unirust_loadtest -c 1000000 --streams 8 --batch 5000 --overlap 0.15

    # Test with conflicts (10% overlap, 50% of overlaps have conflicts)
    unirust_loadtest -c 500000 --overlap 0.1 --conflict 0.5

    # Test cross-shard reconciliation (10% overlap, 30% create cross-shard merges)
    unirust_loadtest -c 500000 --overlap 0.1 --cross-shard 0.3

    # With shard metrics and logging
    unirust_loadtest -c 500000 -s "127.0.0.1:50061,127.0.0.1:50062" --log /tmp/test.log
"#
    );
}

// =============================================================================
// CYBERSECURITY ONTOLOGY
// =============================================================================

/// Entity types in the cybersecurity domain
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityType {
    User,
    Session,
    Process,
    NetworkConnection,
    Asset,
    Vulnerability,
    Alert,
    File,
}

impl EntityType {
    fn as_str(&self) -> &'static str {
        match self {
            EntityType::User => "user",
            EntityType::Session => "session",
            EntityType::Process => "process",
            EntityType::NetworkConnection => "network_connection",
            EntityType::Asset => "asset",
            EntityType::Vulnerability => "vulnerability",
            EntityType::Alert => "alert",
            EntityType::File => "file",
        }
    }

    fn all() -> &'static [EntityType] {
        &[
            EntityType::User,
            EntityType::Session,
            EntityType::Process,
            EntityType::NetworkConnection,
            EntityType::Asset,
            EntityType::Vulnerability,
            EntityType::Alert,
            EntityType::File,
        ]
    }

    fn weight(&self) -> u32 {
        // Distribution weights for entity type selection
        match self {
            EntityType::User => 15,
            EntityType::Session => 20,
            EntityType::Process => 25,
            EntityType::NetworkConnection => 15,
            EntityType::Asset => 10,
            EntityType::Vulnerability => 5,
            EntityType::Alert => 5,
            EntityType::File => 5,
        }
    }
}

/// Data perspectives/sources in the cybersecurity domain
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Perspective {
    LDAP,
    SSO,
    ActiveDirectory,
    ServerLogs,
    EDR,
    SIEM,
    VulnScanner,
    AssetInventory,
}

impl Perspective {
    fn as_str(&self) -> &'static str {
        match self {
            Perspective::LDAP => "ldap",
            Perspective::SSO => "sso",
            Perspective::ActiveDirectory => "active_directory",
            Perspective::ServerLogs => "server_logs",
            Perspective::EDR => "edr",
            Perspective::SIEM => "siem",
            Perspective::VulnScanner => "vuln_scanner",
            Perspective::AssetInventory => "asset_inventory",
        }
    }

    fn for_entity_type(entity_type: EntityType) -> &'static [Perspective] {
        match entity_type {
            EntityType::User => &[
                Perspective::LDAP,
                Perspective::SSO,
                Perspective::ActiveDirectory,
            ],
            EntityType::Session => &[Perspective::SSO, Perspective::ServerLogs, Perspective::SIEM],
            EntityType::Process => &[Perspective::EDR, Perspective::ServerLogs],
            EntityType::NetworkConnection => &[Perspective::EDR, Perspective::SIEM],
            EntityType::Asset => &[
                Perspective::AssetInventory,
                Perspective::EDR,
                Perspective::VulnScanner,
            ],
            EntityType::Vulnerability => &[Perspective::VulnScanner, Perspective::SIEM],
            EntityType::Alert => &[Perspective::SIEM, Perspective::EDR],
            EntityType::File => &[Perspective::EDR, Perspective::VulnScanner],
        }
    }
}

fn build_ontology_config() -> OntologyConfig {
    OntologyConfig {
        identity_keys: vec![
            // user_upn_key FIRST - critical for conflict detection!
            // Partitioning uses the first identity key's attribute values.
            // Conflicts have SAME user_upn but DIFFERENT user_sid.
            // If user_sid were first, conflicts would go to different partitions
            // and never be compared against each other.
            IdentityKeyConfig {
                name: "user_upn_key".to_string(),
                attributes: vec!["user_upn".to_string()],
            },
            IdentityKeyConfig {
                name: "user_identity".to_string(),
                attributes: vec!["user_sid".to_string()],
            },
            IdentityKeyConfig {
                name: "session_identity".to_string(),
                attributes: vec!["session_id".to_string()],
            },
            IdentityKeyConfig {
                name: "process_identity".to_string(),
                attributes: vec!["process_id".to_string(), "asset_id".to_string()],
            },
            IdentityKeyConfig {
                name: "connection_identity".to_string(),
                attributes: vec!["flow_id".to_string()],
            },
            // asset_hostname FIRST - same reasoning as user_upn_key
            // Conflicts have SAME hostname but DIFFERENT asset_id
            IdentityKeyConfig {
                name: "asset_hostname".to_string(),
                attributes: vec!["hostname".to_string()],
            },
            IdentityKeyConfig {
                name: "asset_identity".to_string(),
                attributes: vec!["asset_id".to_string()],
            },
            IdentityKeyConfig {
                name: "vuln_identity".to_string(),
                attributes: vec!["cve_id".to_string(), "asset_id".to_string()],
            },
            IdentityKeyConfig {
                name: "alert_identity".to_string(),
                attributes: vec!["alert_id".to_string()],
            },
            IdentityKeyConfig {
                name: "file_identity".to_string(),
                attributes: vec!["file_hash".to_string()],
            },
        ],
        strong_identifiers: vec![
            "user_sid".to_string(),
            "session_id".to_string(),
            "process_hash".to_string(),
            "flow_id".to_string(),
            "asset_id".to_string(),
            "cve_id".to_string(),
            "alert_id".to_string(),
            "file_hash".to_string(),
        ],
        constraints: vec![],
    }
}

// =============================================================================
// ENTITY GENERATOR
// =============================================================================

/// Seed data for creating overlapping entities
struct UserSeed {
    uid: String,
    user_sid: String,
    user_upn: String,
    employee_id: String,
    department: String,
    perspective: String,
}

struct AssetSeed {
    uid: String,
    asset_id: String,
    hostname: String,
    ip_address: String,
    os_type: String,
    perspective: String,
}

/// Generic entity seed for other types
struct EntitySeed {
    uid: String,
    #[allow(dead_code)]
    identity_attr: String,
    identity_value: String,
}

/// Seed for cross-shard merge scenarios - records with same secondary identity key
/// but different primary (partitioning) key
struct CrossShardSeed {
    user_sid: String, // Same across records (secondary identity key for merge)
    perspective: String,
}

pub struct CyberEntityGenerator {
    rng: StdRng,
    overlap_probability: f64,
    conflict_probability: f64,
    cross_shard_probability: f64,
    next_id: u64,
    base_time: i64,

    // Entity pools for overlap creation - larger pools for denser clustering
    user_pool: Vec<UserSeed>,
    asset_pool: Vec<AssetSeed>,
    session_pool: Vec<EntitySeed>,
    process_pool: Vec<EntitySeed>,
    connection_pool: Vec<EntitySeed>,
    alert_pool: Vec<EntitySeed>,
    file_pool: Vec<EntitySeed>,
    vuln_pool: Vec<EntitySeed>,

    // Pool for cross-shard merge scenarios
    cross_shard_user_pool: Vec<CrossShardSeed>,
    #[allow(dead_code)]
    cross_shard_asset_pool: Vec<CrossShardSeed>,

    // Pool size limits - controls cluster density
    pool_size: usize,

    // Weight sum for entity type selection
    total_weight: u32,
}

impl CyberEntityGenerator {
    pub fn new(
        seed: u64,
        overlap_probability: f64,
        conflict_probability: f64,
        cross_shard_probability: f64,
    ) -> Self {
        let total_weight: u32 = EntityType::all().iter().map(|e| e.weight()).sum();
        // Pool size determines cluster density: smaller pool = denser clusters
        // For ~10 entities per cluster with 10% overlap, use pool_size ~= total_entities / 100
        let pool_size = 10_000; // Supports up to 1M entities with ~10 per cluster at 10% overlap

        Self {
            rng: StdRng::seed_from_u64(seed),
            overlap_probability,
            conflict_probability,
            cross_shard_probability,
            next_id: 0,
            base_time: 1704067200, // 2024-01-01 00:00:00 UTC
            user_pool: Vec::with_capacity(pool_size),
            asset_pool: Vec::with_capacity(pool_size),
            session_pool: Vec::with_capacity(pool_size),
            process_pool: Vec::with_capacity(pool_size),
            connection_pool: Vec::with_capacity(pool_size),
            alert_pool: Vec::with_capacity(pool_size),
            file_pool: Vec::with_capacity(pool_size),
            vuln_pool: Vec::with_capacity(pool_size),
            cross_shard_user_pool: Vec::with_capacity(pool_size),
            cross_shard_asset_pool: Vec::with_capacity(pool_size),
            pool_size,
            total_weight,
        }
    }

    fn should_conflict(&mut self) -> bool {
        self.rng.random_bool(self.conflict_probability)
    }

    fn should_cross_shard(&mut self) -> bool {
        self.rng.random_bool(self.cross_shard_probability)
    }

    fn select_entity_type(&mut self) -> EntityType {
        let mut roll = self.rng.random_range(0..self.total_weight);
        for entity_type in EntityType::all() {
            let weight = entity_type.weight();
            if roll < weight {
                return *entity_type;
            }
            roll -= weight;
        }
        EntityType::User // Fallback
    }

    fn select_perspective(&mut self, entity_type: EntityType) -> Perspective {
        let perspectives = Perspective::for_entity_type(entity_type);
        let idx = self.rng.random_range(0..perspectives.len());
        perspectives[idx]
    }

    fn should_overlap(&mut self) -> bool {
        self.rng.random_bool(self.overlap_probability)
    }

    fn generate_interval(&mut self) -> (i64, i64) {
        let start = self.base_time + self.rng.random_range(0..86400 * 30); // Within 30 days
        let duration = self.rng.random_range(60..86400); // 1 minute to 1 day
        (start, start + duration)
    }

    fn next_uid(&mut self) -> String {
        self.next_id += 1;
        format!("{:012}", self.next_id)
    }

    pub fn generate_batch(&mut self, count: usize) -> Vec<RecordInput> {
        let mut batch = Vec::with_capacity(count);

        for idx in 0..count {
            let entity_type = self.select_entity_type();
            let selected_perspective = self.select_perspective(entity_type);
            let perspective_str = selected_perspective.as_str();
            let (start, end) = self.generate_interval();

            // Generate entity with UID - may reuse existing UID for clustering
            // For conflicts, the generate method returns an override perspective
            let (uid, descriptors, perspective_override) = match entity_type {
                EntityType::User => self.generate_user(start, end, perspective_str),
                EntityType::Asset => self.generate_asset(start, end, perspective_str),
                // Other entity types don't have conflict capability (identity key = strong identifier)
                EntityType::Session => {
                    let (uid, desc) = self.generate_session(start, end);
                    (uid, desc, None)
                }
                EntityType::Process => {
                    let (uid, desc) = self.generate_process(start, end);
                    (uid, desc, None)
                }
                EntityType::NetworkConnection => {
                    let (uid, desc) = self.generate_connection(start, end);
                    (uid, desc, None)
                }
                EntityType::Vulnerability => {
                    let (uid, desc) = self.generate_vulnerability(start, end);
                    (uid, desc, None)
                }
                EntityType::Alert => {
                    let (uid, desc) = self.generate_alert(start, end);
                    (uid, desc, None)
                }
                EntityType::File => {
                    let (uid, desc) = self.generate_file(start, end);
                    (uid, desc, None)
                }
            };

            // Use override perspective for conflicts (ensures same perspective as original)
            let final_perspective =
                perspective_override.unwrap_or_else(|| perspective_str.to_string());

            batch.push(RecordInput {
                index: idx as u32,
                identity: Some(RecordIdentity {
                    entity_type: entity_type.as_str().to_string(),
                    perspective: final_perspective,
                    uid,
                }),
                descriptors,
            });
        }

        batch
    }

    /// Generate a user entity. Returns (uid, descriptors, perspective_override).
    /// When overlapping, reuses UID from pool to create clusters.
    /// When conflicting, uses same perspective and different strong identifier.
    /// When cross-shard, uses different user_upn (partitioning key) but same user_sid
    /// (secondary identity key) to create cross-shard merge candidates.
    fn generate_user(
        &mut self,
        start: i64,
        end: i64,
        perspective: &str,
    ) -> (String, Vec<RecordDescriptor>, Option<String>) {
        let is_overlap = self.should_overlap() && !self.user_pool.is_empty();
        let is_conflict = is_overlap && self.should_conflict();
        let is_cross_shard = is_overlap
            && !is_conflict
            && self.should_cross_shard()
            && !self.cross_shard_user_pool.is_empty();

        let (uid, user_sid, user_upn, employee_id, department, perspective_override) =
            if is_cross_shard {
                // CROSS-SHARD: Different user_upn (different shards) but SAME user_sid
                // This creates records that will be detected as merge candidates during
                // cross-shard reconciliation via the user_identity key.
                let idx = self.rng.random_range(0..self.cross_shard_user_pool.len());
                let seed_user_sid = self.cross_shard_user_pool[idx].user_sid.clone();
                let seed_perspective = self.cross_shard_user_pool[idx].perspective.clone();

                let uid = self.next_uid();
                // NEW user_upn - will hash to potentially different shard
                let user_upn = format!("crossshard_user{}@corp.local", self.next_id);
                let employee_id = format!("EMP{:08}", self.rng.random_range(10000..99999999u32));
                let departments = ["Engineering", "Sales", "HR", "Finance", "IT", "Security"];
                let department =
                    departments[self.rng.random_range(0..departments.len())].to_string();

                (
                    uid,
                    seed_user_sid, // SAME user_sid - creates cross-shard merge candidate
                    user_upn,      // DIFFERENT user_upn - goes to different shard
                    employee_id,
                    department,
                    Some(seed_perspective), // Use original perspective
                )
            } else if is_overlap {
                let idx = self.rng.random_range(0..self.user_pool.len());
                // Clone seed values upfront to avoid borrow conflicts
                let seed_uid = self.user_pool[idx].uid.clone();
                let seed_user_sid = self.user_pool[idx].user_sid.clone();
                let seed_user_upn = self.user_pool[idx].user_upn.clone();
                let seed_employee_id = self.user_pool[idx].employee_id.clone();
                let seed_department = self.user_pool[idx].department.clone();
                let seed_perspective = self.user_pool[idx].perspective.clone();

                if is_conflict {
                    // CONFLICT: Same identity key (user_upn) but DIFFERENT strong identifier (user_sid)
                    // CRITICAL: Use NEW UID (different record), SAME perspective, SAME identity key,
                    //           but DIFFERENT strong identifier to create a true conflict
                    let new_uid = self.next_uid();
                    let new_user_sid = format!(
                        "S-1-5-21-{}-{}",
                        self.rng.random::<u32>(),
                        self.rng.random::<u32>()
                    );
                    (
                        new_uid,       // NEW UID - creates a new record (not a duplicate)
                        new_user_sid,  // DIFFERENT strong identifier - creates conflict
                        seed_user_upn, // SAME identity key - makes records candidates for merge
                        seed_employee_id,
                        seed_department,
                        Some(seed_perspective), // SAME perspective - required for conflict detection
                    )
                } else {
                    // Exact overlap - same cluster, no conflict
                    (
                        seed_uid,
                        seed_user_sid,
                        seed_user_upn,
                        seed_employee_id,
                        seed_department,
                        None,
                    )
                }
            } else {
                let uid = self.next_uid();
                let user_sid = format!("S-1-5-21-{}-{}", self.rng.random::<u32>(), self.next_id);
                let user_upn = format!("user{}@corp.local", self.next_id);
                let employee_id = format!("EMP{:08}", self.rng.random_range(10000..99999999u32));
                let departments = ["Engineering", "Sales", "HR", "Finance", "IT", "Security"];
                let department =
                    departments[self.rng.random_range(0..departments.len())].to_string();

                if self.user_pool.len() < self.pool_size {
                    self.user_pool.push(UserSeed {
                        uid: uid.clone(),
                        user_sid: user_sid.clone(),
                        user_upn: user_upn.clone(),
                        employee_id: employee_id.clone(),
                        department: department.clone(),
                        perspective: perspective.to_string(),
                    });
                }

                // Also add to cross-shard pool for future cross-shard merge candidates
                if self.cross_shard_user_pool.len() < self.pool_size {
                    self.cross_shard_user_pool.push(CrossShardSeed {
                        user_sid: user_sid.clone(),
                        perspective: perspective.to_string(),
                    });
                }

                (uid, user_sid, user_upn, employee_id, department, None)
            };

        let descriptors = vec![
            RecordDescriptor {
                attr: "user_sid".to_string(),
                value: user_sid,
                start,
                end,
            },
            RecordDescriptor {
                attr: "user_upn".to_string(),
                value: user_upn,
                start,
                end,
            },
            RecordDescriptor {
                attr: "employee_id".to_string(),
                value: employee_id,
                start,
                end,
            },
            RecordDescriptor {
                attr: "department".to_string(),
                value: department,
                start,
                end,
            },
        ];

        (uid, descriptors, perspective_override)
    }

    fn generate_session(&mut self, start: i64, end: i64) -> (String, Vec<RecordDescriptor>) {
        let is_overlap = self.should_overlap() && !self.session_pool.is_empty();

        let (uid, session_id) = if is_overlap {
            let idx = self.rng.random_range(0..self.session_pool.len());
            let seed = &self.session_pool[idx];
            (seed.uid.clone(), seed.identity_value.clone())
        } else {
            let uid = self.next_uid();
            let session_id = format!("SES-{:016X}", self.rng.random::<u64>());
            if self.session_pool.len() < self.pool_size {
                self.session_pool.push(EntitySeed {
                    uid: uid.clone(),
                    identity_attr: "session_id".to_string(),
                    identity_value: session_id.clone(),
                });
            }
            (uid, session_id)
        };

        let user_ref = if !self.user_pool.is_empty() {
            let idx = self.rng.random_range(0..self.user_pool.len());
            self.user_pool[idx].user_sid.clone()
        } else {
            format!("S-1-5-21-{}", self.rng.random::<u32>())
        };
        let source_ip = format!(
            "10.{}.{}.{}",
            self.rng.random_range(0..256u16),
            self.rng.random_range(0..256u16),
            self.rng.random_range(1..255u16)
        );

        let descriptors = vec![
            RecordDescriptor {
                attr: "session_id".to_string(),
                value: session_id,
                start,
                end,
            },
            RecordDescriptor {
                attr: "user_ref".to_string(),
                value: user_ref,
                start,
                end,
            },
            RecordDescriptor {
                attr: "source_ip".to_string(),
                value: source_ip,
                start,
                end,
            },
            RecordDescriptor {
                attr: "login_time".to_string(),
                value: start.to_string(),
                start,
                end,
            },
        ];
        (uid, descriptors)
    }

    fn generate_process(&mut self, start: i64, end: i64) -> (String, Vec<RecordDescriptor>) {
        let is_overlap = self.should_overlap() && !self.process_pool.is_empty();

        let (uid, process_hash) = if is_overlap {
            let idx = self.rng.random_range(0..self.process_pool.len());
            let seed = &self.process_pool[idx];
            (seed.uid.clone(), seed.identity_value.clone())
        } else {
            let uid = self.next_uid();
            let process_hash = format!("{:064x}", self.rng.random::<u128>());
            if self.process_pool.len() < self.pool_size {
                self.process_pool.push(EntitySeed {
                    uid: uid.clone(),
                    identity_attr: "process_hash".to_string(),
                    identity_value: process_hash.clone(),
                });
            }
            (uid, process_hash)
        };

        let process_id = self.rng.random_range(1000..65535u32);
        let process_names = [
            "svchost.exe",
            "chrome.exe",
            "powershell.exe",
            "cmd.exe",
            "explorer.exe",
            "python.exe",
            "java.exe",
            "notepad.exe",
        ];
        let process_name = process_names[self.rng.random_range(0..process_names.len())].to_string();
        let parent_pid = self.rng.random_range(1..process_id);
        let asset_ref = if !self.asset_pool.is_empty() {
            let idx = self.rng.random_range(0..self.asset_pool.len());
            self.asset_pool[idx].asset_id.clone()
        } else {
            format!("ASSET-{:08}", self.rng.random::<u32>())
        };

        let descriptors = vec![
            RecordDescriptor {
                attr: "process_id".to_string(),
                value: process_id.to_string(),
                start,
                end,
            },
            RecordDescriptor {
                attr: "process_hash".to_string(),
                value: process_hash,
                start,
                end,
            },
            RecordDescriptor {
                attr: "process_name".to_string(),
                value: process_name,
                start,
                end,
            },
            RecordDescriptor {
                attr: "parent_pid".to_string(),
                value: parent_pid.to_string(),
                start,
                end,
            },
            RecordDescriptor {
                attr: "asset_id".to_string(),
                value: asset_ref,
                start,
                end,
            },
        ];
        (uid, descriptors)
    }

    fn generate_connection(&mut self, start: i64, end: i64) -> (String, Vec<RecordDescriptor>) {
        let is_overlap = self.should_overlap() && !self.connection_pool.is_empty();

        let (uid, flow_id) = if is_overlap {
            let idx = self.rng.random_range(0..self.connection_pool.len());
            let seed = &self.connection_pool[idx];
            (seed.uid.clone(), seed.identity_value.clone())
        } else {
            let uid = self.next_uid();
            let flow_id = format!("FLOW-{:016X}", self.rng.random::<u64>());
            if self.connection_pool.len() < self.pool_size {
                self.connection_pool.push(EntitySeed {
                    uid: uid.clone(),
                    identity_attr: "flow_id".to_string(),
                    identity_value: flow_id.clone(),
                });
            }
            (uid, flow_id)
        };

        let src_ip = format!(
            "10.{}.{}.{}",
            self.rng.random_range(0..256u16),
            self.rng.random_range(0..256u16),
            self.rng.random_range(1..255u16)
        );
        let dst_ip = format!(
            "{}.{}.{}.{}",
            self.rng.random_range(1..224u16),
            self.rng.random_range(0..256u16),
            self.rng.random_range(0..256u16),
            self.rng.random_range(1..255u16)
        );
        let src_port = self.rng.random_range(1024..65535u16);
        let dst_port = [80, 443, 22, 3389, 8080, 8443][self.rng.random_range(0..6)];
        let protocol = ["TCP", "UDP"][self.rng.random_range(0..2)].to_string();

        let descriptors = vec![
            RecordDescriptor {
                attr: "flow_id".to_string(),
                value: flow_id,
                start,
                end,
            },
            RecordDescriptor {
                attr: "src_ip".to_string(),
                value: src_ip,
                start,
                end,
            },
            RecordDescriptor {
                attr: "dst_ip".to_string(),
                value: dst_ip,
                start,
                end,
            },
            RecordDescriptor {
                attr: "src_port".to_string(),
                value: src_port.to_string(),
                start,
                end,
            },
            RecordDescriptor {
                attr: "dst_port".to_string(),
                value: dst_port.to_string(),
                start,
                end,
            },
            RecordDescriptor {
                attr: "protocol".to_string(),
                value: protocol,
                start,
                end,
            },
        ];
        (uid, descriptors)
    }

    fn generate_asset(
        &mut self,
        start: i64,
        end: i64,
        perspective: &str,
    ) -> (String, Vec<RecordDescriptor>, Option<String>) {
        let is_overlap = self.should_overlap() && !self.asset_pool.is_empty();
        let is_conflict = is_overlap && self.should_conflict();

        let (uid, asset_id, hostname, ip_address, os_type, perspective_override) = if is_overlap {
            let idx = self.rng.random_range(0..self.asset_pool.len());
            // Clone seed values upfront to avoid borrow conflicts
            let seed_uid = self.asset_pool[idx].uid.clone();
            let seed_asset_id = self.asset_pool[idx].asset_id.clone();
            let seed_hostname = self.asset_pool[idx].hostname.clone();
            let seed_ip_address = self.asset_pool[idx].ip_address.clone();
            let seed_os_type = self.asset_pool[idx].os_type.clone();
            let seed_perspective = self.asset_pool[idx].perspective.clone();

            if is_conflict {
                // CONFLICT: Same hostname (identity key) but DIFFERENT asset_id (strong identifier)
                // CRITICAL: Use NEW UID, SAME perspective, SAME identity key, DIFFERENT strong ID
                let new_uid = self.next_uid();
                let new_asset_id = format!("ASSET-{:08X}", self.rng.random::<u32>());
                (
                    new_uid,
                    new_asset_id,
                    seed_hostname,
                    seed_ip_address,
                    seed_os_type,
                    Some(seed_perspective),
                )
            } else {
                (
                    seed_uid,
                    seed_asset_id,
                    seed_hostname,
                    seed_ip_address,
                    seed_os_type,
                    None,
                )
            }
        } else {
            let uid = self.next_uid();
            let asset_id = format!("ASSET-{:08X}", self.rng.random::<u32>());
            let hostname = format!("HOST-{:06}", self.rng.random_range(1..999999u32));
            let ip_address = format!(
                "10.{}.{}.{}",
                self.rng.random_range(0..256u16),
                self.rng.random_range(0..256u16),
                self.rng.random_range(1..255u16)
            );
            let os_types = [
                "Windows 10",
                "Windows 11",
                "Windows Server 2019",
                "Ubuntu 22.04",
                "RHEL 8",
                "macOS 14",
            ];
            let os_type = os_types[self.rng.random_range(0..os_types.len())].to_string();

            if self.asset_pool.len() < self.pool_size {
                self.asset_pool.push(AssetSeed {
                    uid: uid.clone(),
                    asset_id: asset_id.clone(),
                    hostname: hostname.clone(),
                    ip_address: ip_address.clone(),
                    os_type: os_type.clone(),
                    perspective: perspective.to_string(),
                });
            }
            (uid, asset_id, hostname, ip_address, os_type, None)
        };

        let mac_address = format!(
            "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
            self.rng.random::<u8>(),
            self.rng.random::<u8>(),
            self.rng.random::<u8>(),
            self.rng.random::<u8>(),
            self.rng.random::<u8>(),
            self.rng.random::<u8>()
        );

        let descriptors = vec![
            RecordDescriptor {
                attr: "asset_id".to_string(),
                value: asset_id,
                start,
                end,
            },
            RecordDescriptor {
                attr: "hostname".to_string(),
                value: hostname,
                start,
                end,
            },
            RecordDescriptor {
                attr: "ip_address".to_string(),
                value: ip_address,
                start,
                end,
            },
            RecordDescriptor {
                attr: "mac_address".to_string(),
                value: mac_address,
                start,
                end,
            },
            RecordDescriptor {
                attr: "os_type".to_string(),
                value: os_type,
                start,
                end,
            },
        ];
        (uid, descriptors, perspective_override)
    }

    fn generate_vulnerability(&mut self, start: i64, end: i64) -> (String, Vec<RecordDescriptor>) {
        let is_overlap = self.should_overlap() && !self.vuln_pool.is_empty();
        let is_conflict = is_overlap && self.should_conflict();

        let (uid, cve_id, severity, status) = if is_overlap {
            let idx = self.rng.random_range(0..self.vuln_pool.len());
            let seed = &self.vuln_pool[idx];
            if is_conflict {
                let severities = ["Critical", "High", "Medium", "Low", "Info"];
                let new_severity =
                    severities[self.rng.random_range(0..severities.len())].to_string();
                let statuses = ["Open", "In Progress", "Remediated", "Accepted"];
                let new_status = statuses[self.rng.random_range(0..statuses.len())].to_string();
                (
                    seed.uid.clone(),
                    seed.identity_value.clone(),
                    new_severity,
                    new_status,
                )
            } else {
                (
                    seed.uid.clone(),
                    seed.identity_value.clone(),
                    "Medium".to_string(),
                    "Open".to_string(),
                )
            }
        } else {
            let uid = self.next_uid();
            let cve_id = format!(
                "CVE-{}-{}",
                self.rng.random_range(2020..2025u16),
                self.rng.random_range(1000..99999u32)
            );
            let severities = ["Critical", "High", "Medium", "Low", "Info"];
            let severity = severities[self.rng.random_range(0..severities.len())].to_string();
            let statuses = ["Open", "In Progress", "Remediated", "Accepted"];
            let status = statuses[self.rng.random_range(0..statuses.len())].to_string();

            if self.vuln_pool.len() < self.pool_size {
                self.vuln_pool.push(EntitySeed {
                    uid: uid.clone(),
                    identity_attr: "cve_id".to_string(),
                    identity_value: cve_id.clone(),
                });
            }
            (uid, cve_id, severity, status)
        };

        let asset_ref = if !self.asset_pool.is_empty() {
            let idx = self.rng.random_range(0..self.asset_pool.len());
            self.asset_pool[idx].asset_id.clone()
        } else {
            format!("ASSET-{:08}", self.rng.random::<u32>())
        };
        let cvss_score = format!("{:.1}", self.rng.random_range(0..100u32) as f32 / 10.0);

        let descriptors = vec![
            RecordDescriptor {
                attr: "cve_id".to_string(),
                value: cve_id,
                start,
                end,
            },
            RecordDescriptor {
                attr: "asset_ref".to_string(),
                value: asset_ref,
                start,
                end,
            },
            RecordDescriptor {
                attr: "severity".to_string(),
                value: severity,
                start,
                end,
            },
            RecordDescriptor {
                attr: "cvss_score".to_string(),
                value: cvss_score,
                start,
                end,
            },
            RecordDescriptor {
                attr: "remediation_status".to_string(),
                value: status,
                start,
                end,
            },
        ];
        (uid, descriptors)
    }

    fn generate_alert(&mut self, start: i64, end: i64) -> (String, Vec<RecordDescriptor>) {
        let is_overlap = self.should_overlap() && !self.alert_pool.is_empty();

        let (uid, alert_id) = if is_overlap {
            let idx = self.rng.random_range(0..self.alert_pool.len());
            let seed = &self.alert_pool[idx];
            (seed.uid.clone(), seed.identity_value.clone())
        } else {
            let uid = self.next_uid();
            let alert_id = format!("ALERT-{:016X}", self.rng.random::<u64>());
            if self.alert_pool.len() < self.pool_size {
                self.alert_pool.push(EntitySeed {
                    uid: uid.clone(),
                    identity_attr: "alert_id".to_string(),
                    identity_value: alert_id.clone(),
                });
            }
            (uid, alert_id)
        };

        let alert_types = [
            "Malware",
            "Phishing",
            "Brute Force",
            "Data Exfiltration",
            "Lateral Movement",
            "Privilege Escalation",
            "C2 Communication",
        ];
        let alert_type = alert_types[self.rng.random_range(0..alert_types.len())].to_string();
        let severities = ["Critical", "High", "Medium", "Low"];
        let severity = severities[self.rng.random_range(0..severities.len())].to_string();
        let asset_ref = if !self.asset_pool.is_empty() {
            let idx = self.rng.random_range(0..self.asset_pool.len());
            self.asset_pool[idx].asset_id.clone()
        } else {
            format!("ASSET-{:08}", self.rng.random::<u32>())
        };

        let descriptors = vec![
            RecordDescriptor {
                attr: "alert_id".to_string(),
                value: alert_id,
                start,
                end,
            },
            RecordDescriptor {
                attr: "alert_type".to_string(),
                value: alert_type,
                start,
                end,
            },
            RecordDescriptor {
                attr: "severity".to_string(),
                value: severity,
                start,
                end,
            },
            RecordDescriptor {
                attr: "asset_ref".to_string(),
                value: asset_ref,
                start,
                end,
            },
        ];
        (uid, descriptors)
    }

    fn generate_file(&mut self, start: i64, end: i64) -> (String, Vec<RecordDescriptor>) {
        let is_overlap = self.should_overlap() && !self.file_pool.is_empty();

        let (uid, file_hash) = if is_overlap {
            let idx = self.rng.random_range(0..self.file_pool.len());
            let seed = &self.file_pool[idx];
            (seed.uid.clone(), seed.identity_value.clone())
        } else {
            let uid = self.next_uid();
            let file_hash = format!("{:064x}", self.rng.random::<u128>());
            if self.file_pool.len() < self.pool_size {
                self.file_pool.push(EntitySeed {
                    uid: uid.clone(),
                    identity_attr: "file_hash".to_string(),
                    identity_value: file_hash.clone(),
                });
            }
            (uid, file_hash)
        };

        let paths = [
            "C:\\Windows\\System32\\",
            "C:\\Users\\Public\\",
            "C:\\Program Files\\",
            "/usr/bin/",
            "/tmp/",
            "/home/user/",
        ];
        let exts = [".exe", ".dll", ".ps1", ".sh", ".py", ".bat"];
        let file_path = format!(
            "{}file{:06}{}",
            paths[self.rng.random_range(0..paths.len())],
            self.rng.random_range(1..999999u32),
            exts[self.rng.random_range(0..exts.len())]
        );
        let file_size = self.rng.random_range(1024..10485760u64);
        let asset_ref = if !self.asset_pool.is_empty() {
            let idx = self.rng.random_range(0..self.asset_pool.len());
            self.asset_pool[idx].asset_id.clone()
        } else {
            format!("ASSET-{:08}", self.rng.random::<u32>())
        };

        let descriptors = vec![
            RecordDescriptor {
                attr: "file_hash".to_string(),
                value: file_hash,
                start,
                end,
            },
            RecordDescriptor {
                attr: "file_path".to_string(),
                value: file_path,
                start,
                end,
            },
            RecordDescriptor {
                attr: "file_size".to_string(),
                value: file_size.to_string(),
                start,
                end,
            },
            RecordDescriptor {
                attr: "asset_ref".to_string(),
                value: asset_ref,
                start,
                end,
            },
        ];
        (uid, descriptors)
    }
}

// =============================================================================
// METRICS
// =============================================================================

#[derive(Debug, Default)]
pub struct StreamStats {
    pub records_sent: AtomicU64,
    pub records_acked: AtomicU64,
    pub last_latency_micros: AtomicU64,
    pub errors: AtomicU64,
}

#[derive(Debug, Default)]
pub struct ShardStats {
    pub shard_id: u32,
    pub record_count: AtomicU64,
    pub cluster_count: AtomicU64,
    pub ingest_latency_avg: AtomicU64,
    // Detailed latency metrics
    pub ingest_latency_max: AtomicU64,
    pub ingest_latency_total: AtomicU64,
    pub ingest_request_count: AtomicU64,
    // Store metrics
    pub persistent: AtomicBool,
    pub running_compactions: AtomicU64,
    pub running_flushes: AtomicU64,
    pub block_cache_usage_mb: AtomicU64,
    pub block_cache_capacity_mb: AtomicU64,
}

pub struct LoadTestMetrics {
    pub start_time: Instant,
    pub total_entities: u64,
    pub entities_sent: AtomicU64,
    pub entities_acked: AtomicU64,
    pub cluster_count: AtomicU64,
    pub conflicts_detected: AtomicU64,
    pub merges_performed: AtomicU64,
    // Cross-shard reconciliation stats
    pub cross_shard_merges: AtomicU64,
    pub cross_shard_conflicts: AtomicU64,
    pub boundary_keys_tracked: AtomicU64,
    pub stream_stats: Vec<StreamStats>,
    pub shard_stats: Vec<ShardStats>,
    pub completed: AtomicBool,
    pub error_message: RwLock<Option<String>>,
}

impl LoadTestMetrics {
    pub fn new(stream_count: usize, shard_count: usize, total_entities: u64) -> Self {
        let stream_stats = (0..stream_count).map(|_| StreamStats::default()).collect();
        let shard_stats = (0..shard_count)
            .map(|i| ShardStats {
                shard_id: i as u32,
                ..Default::default()
            })
            .collect();

        Self {
            start_time: Instant::now(),
            total_entities,
            entities_sent: AtomicU64::new(0),
            entities_acked: AtomicU64::new(0),
            cluster_count: AtomicU64::new(0),
            conflicts_detected: AtomicU64::new(0),
            merges_performed: AtomicU64::new(0),
            cross_shard_merges: AtomicU64::new(0),
            cross_shard_conflicts: AtomicU64::new(0),
            boundary_keys_tracked: AtomicU64::new(0),
            stream_stats,
            shard_stats,
            completed: AtomicBool::new(false),
            error_message: RwLock::new(None),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn progress(&self) -> f64 {
        let acked = self.entities_acked.load(Ordering::Relaxed);
        if self.total_entities == 0 {
            1.0
        } else {
            acked as f64 / self.total_entities as f64
        }
    }

    pub fn throughput(&self) -> f64 {
        let acked = self.entities_acked.load(Ordering::Relaxed);
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            acked as f64 / elapsed
        } else {
            0.0
        }
    }

    pub fn eta(&self) -> Option<Duration> {
        let acked = self.entities_acked.load(Ordering::Relaxed);
        let remaining = self.total_entities.saturating_sub(acked);
        let throughput = self.throughput();
        if throughput > 0.0 && remaining > 0 {
            Some(Duration::from_secs_f64(remaining as f64 / throughput))
        } else {
            None
        }
    }

    /// Generate a detailed report for analysis
    pub fn generate_report(&self, config: &LoadTestConfig) -> String {
        let elapsed = self.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let entities_sent = self.entities_sent.load(Ordering::Relaxed);
        let entities_acked = self.entities_acked.load(Ordering::Relaxed);
        let cluster_count = self.cluster_count.load(Ordering::Relaxed);
        let conflicts = self.conflicts_detected.load(Ordering::Relaxed);
        let throughput = self.throughput();

        let mut report = String::new();

        // Header
        report.push_str(
            "================================================================================\n",
        );
        report.push_str("                         UNIRUST LOAD TEST REPORT\n");
        report.push_str(
            "================================================================================\n\n",
        );

        // Timestamp
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        report.push_str(&format!("Timestamp: {}\n", timestamp));
        report.push_str(&format!("Duration: {:.2}s\n\n", elapsed_secs));

        // Configuration
        report.push_str("## Configuration\n");
        report.push_str(&format!("  Target count:     {}\n", config.count));
        report.push_str(&format!("  Router:           {}\n", config.router_addr));
        report.push_str(&format!("  Shards:           {:?}\n", config.shard_addrs));
        report.push_str(&format!("  Stream count:     {}\n", config.stream_count));
        report.push_str(&format!("  Batch size:       {}\n", config.batch_size));
        report.push_str(&format!(
            "  Overlap prob:     {:.1}%\n",
            config.overlap_probability * 100.0
        ));
        report.push_str(&format!(
            "  Conflict prob:    {:.1}%\n",
            config.conflict_probability * 100.0
        ));
        report.push_str(&format!(
            "  Cross-shard prob: {:.1}%\n",
            config.cross_shard_probability * 100.0
        ));
        report.push_str(&format!("  Seed:             {}\n\n", config.seed));

        // Summary
        report.push_str("## Summary\n");
        report.push_str(&format!("  Entities sent:    {}\n", entities_sent));
        report.push_str(&format!("  Entities acked:   {}\n", entities_acked));
        report.push_str(&format!("  Clusters:         {}\n", cluster_count));
        report.push_str(&format!("  Conflicts:        {}\n", conflicts));
        report.push_str(&format!("  Throughput:       {:.2} rec/sec\n", throughput));
        report.push_str(&format!(
            "  Avg latency:      {:.2} ms/batch\n\n",
            if throughput > 0.0 {
                (config.batch_size as f64 / throughput) * 1000.0
            } else {
                0.0
            }
        ));

        // Cross-shard reconciliation stats
        let cross_shard_merges = self.cross_shard_merges.load(Ordering::Relaxed);
        let cross_shard_conflicts = self.cross_shard_conflicts.load(Ordering::Relaxed);
        let boundary_keys = self.boundary_keys_tracked.load(Ordering::Relaxed);

        report.push_str("## Cross-Shard Reconciliation\n");
        report.push_str(&format!("  Boundary keys:    {}\n", boundary_keys));
        report.push_str(&format!(
            "  Cross-shard merges:    {}\n",
            cross_shard_merges
        ));
        report.push_str(&format!(
            "  Cross-shard conflicts: {}\n\n",
            cross_shard_conflicts
        ));

        // Per-stream stats
        report.push_str("## Stream Statistics\n");
        report.push_str("  Stream  Sent        Acked       Latency(ms)  Errors\n");
        report.push_str("  ------  ----------  ----------  -----------  ------\n");
        for (i, stream) in self.stream_stats.iter().enumerate() {
            let sent = stream.records_sent.load(Ordering::Relaxed);
            let acked = stream.records_acked.load(Ordering::Relaxed);
            let latency_us = stream.last_latency_micros.load(Ordering::Relaxed);
            let errors = stream.errors.load(Ordering::Relaxed);
            report.push_str(&format!(
                "  #{:<5} {:>10}  {:>10}  {:>11.1}  {:>6}\n",
                i,
                sent,
                acked,
                latency_us as f64 / 1000.0,
                errors
            ));
        }
        report.push('\n');

        // Per-shard stats (detailed)
        if !self.shard_stats.is_empty() {
            report.push_str("## Shard Statistics\n\n");
            for shard in &self.shard_stats {
                let rec = shard.record_count.load(Ordering::Relaxed);
                let clusters = shard.cluster_count.load(Ordering::Relaxed);
                let latency_avg = shard.ingest_latency_avg.load(Ordering::Relaxed);
                let latency_max = shard.ingest_latency_max.load(Ordering::Relaxed);
                let latency_total = shard.ingest_latency_total.load(Ordering::Relaxed);
                let request_count = shard.ingest_request_count.load(Ordering::Relaxed);
                let persistent = shard.persistent.load(Ordering::Relaxed);
                let compactions = shard.running_compactions.load(Ordering::Relaxed);
                let flushes = shard.running_flushes.load(Ordering::Relaxed);
                let cache_usage = shard.block_cache_usage_mb.load(Ordering::Relaxed);
                let cache_capacity = shard.block_cache_capacity_mb.load(Ordering::Relaxed);

                report.push_str(&format!(
                    "### Shard {} {}\n",
                    shard.shard_id,
                    if persistent {
                        "(persistent)"
                    } else {
                        "(in-memory)"
                    }
                ));
                report.push_str(&format!("  Records ingested:   {}\n", rec));
                report.push_str(&format!("  Clusters:           {}\n", clusters));
                report.push_str(&format!("  Ingest requests:    {}\n", request_count));
                report.push_str(&format!(
                    "  Latency avg:        {:.2} ms\n",
                    latency_avg as f64 / 1000.0
                ));
                report.push_str(&format!(
                    "  Latency max:        {:.2} ms\n",
                    latency_max as f64 / 1000.0
                ));
                report.push_str(&format!(
                    "  Latency total:      {:.2} s\n",
                    latency_total as f64 / 1_000_000.0
                ));

                if persistent {
                    report.push_str(&format!(
                        "  Block cache:        {} / {} MB ({:.1}%)\n",
                        cache_usage,
                        cache_capacity,
                        if cache_capacity > 0 {
                            (cache_usage as f64 / cache_capacity as f64) * 100.0
                        } else {
                            0.0
                        }
                    ));
                    report.push_str(&format!("  Running compactions: {}\n", compactions));
                    report.push_str(&format!("  Running flushes:    {}\n", flushes));
                }
                report.push('\n');
            }

            // Shard balance analysis
            let total_records: u64 = self
                .shard_stats
                .iter()
                .map(|s| s.record_count.load(Ordering::Relaxed))
                .sum();
            if total_records > 0 {
                report.push_str("### Shard Balance\n");
                let expected_per_shard = total_records as f64 / self.shard_stats.len() as f64;
                for shard in &self.shard_stats {
                    let rec = shard.record_count.load(Ordering::Relaxed) as f64;
                    let deviation = ((rec - expected_per_shard) / expected_per_shard) * 100.0;
                    report.push_str(&format!(
                        "  Shard {}: {:.1}% deviation from ideal\n",
                        shard.shard_id, deviation
                    ));
                }
                report.push('\n');
            }
        }

        // Performance metrics
        report.push_str("## Performance Analysis\n");
        let entities_per_stream = entities_acked as f64 / config.stream_count as f64;
        let batches_total = entities_acked as f64 / config.batch_size as f64;
        let batch_latency_avg = if batches_total > 0.0 {
            elapsed_secs * 1000.0 / batches_total
        } else {
            0.0
        };
        report.push_str(&format!("  Total batches:      {:.0}\n", batches_total));
        report.push_str(&format!(
            "  Entities/stream:    {:.0}\n",
            entities_per_stream
        ));
        report.push_str(&format!(
            "  Batch latency avg:  {:.2} ms\n",
            batch_latency_avg
        ));
        report.push_str(&format!(
            "  Records/sec/stream: {:.2}\n",
            throughput / config.stream_count as f64
        ));

        // Bottleneck analysis
        report.push_str("\n## Bottleneck Analysis\n");
        if throughput < 1000.0 {
            report.push_str("  ⚠ Throughput < 1000 rec/sec - likely I/O bound\n");
            report.push_str("    - Check RocksDB sync writes\n");
            report.push_str("    - Consider increasing batch size\n");
            report.push_str("    - Check disk I/O with iostat\n");
        }
        if batch_latency_avg > 500.0 {
            report.push_str("  ⚠ Batch latency > 500ms - processing bottleneck\n");
            report.push_str("    - Check conflict detection overhead\n");
            report.push_str("    - Consider profiling with UNIRUST_PROFILE=1\n");
        }
        let max_stream_latency = self
            .stream_stats
            .iter()
            .map(|s| s.last_latency_micros.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0);
        let min_stream_latency = self
            .stream_stats
            .iter()
            .map(|s| s.last_latency_micros.load(Ordering::Relaxed))
            .min()
            .unwrap_or(0);
        if max_stream_latency > min_stream_latency * 2 && min_stream_latency > 0 {
            report.push_str("  ⚠ Stream latency imbalance detected\n");
            report.push_str("    - Uneven shard distribution or hot keys\n");
        }

        report.push_str(
            "\n================================================================================\n",
        );
        report
    }
}

/// Write the final report to a file
fn write_report(config: &LoadTestConfig, metrics: &LoadTestMetrics) -> Result<PathBuf> {
    let report = metrics.generate_report(config);

    // Generate report filename with timestamp
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let report_path = PathBuf::from(format!("/tmp/unirust_loadtest_{}.txt", timestamp));

    let mut file = std::fs::File::create(&report_path)?;
    file.write_all(report.as_bytes())?;

    // Also print to stdout
    println!("{}", report);
    println!("Report saved to: {}", report_path.display());

    Ok(report_path)
}

// =============================================================================
// PARALLEL STREAMING
// =============================================================================

async fn run_parallel_streams(
    config: &LoadTestConfig,
    metrics: Arc<LoadTestMetrics>,
) -> Result<()> {
    let (tx, rx) = async_channel::bounded::<Vec<RecordInput>>(32);

    // Generator task
    let gen_config = config.clone();
    let gen_metrics = metrics.clone();
    let gen_tx = tx.clone();
    let generator_handle = tokio::spawn(async move {
        let mut generator = CyberEntityGenerator::new(
            gen_config.seed,
            gen_config.overlap_probability,
            gen_config.conflict_probability,
            gen_config.cross_shard_probability,
        );
        let mut remaining = gen_config.count;

        while remaining > 0 {
            let batch_size = gen_config.batch_size.min(remaining as usize);
            let batch = generator.generate_batch(batch_size);
            remaining -= batch_size as u64;
            gen_metrics
                .entities_sent
                .fetch_add(batch_size as u64, Ordering::Relaxed);

            if gen_tx.send(batch).await.is_err() {
                break; // Channel closed
            }
        }
    });

    // Stream tasks
    let stream_handles: Vec<_> = (0..config.stream_count)
        .map(|i| {
            let rx = rx.clone();
            let metrics = metrics.clone();
            let router = config.router_addr.clone();

            tokio::spawn(async move {
                let mut client = match RouterServiceClient::connect(router).await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(stream = i, error = %e, "Failed to connect to router");
                        metrics.stream_stats[i]
                            .errors
                            .fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };

                while let Ok(batch) = rx.recv().await {
                    let batch_len = batch.len();
                    metrics.stream_stats[i]
                        .records_sent
                        .fetch_add(batch_len as u64, Ordering::Relaxed);
                    metrics.entities_sent.fetch_add(batch_len as u64, Ordering::Relaxed);
                    let start = Instant::now();

                    // Retry with exponential backoff
                    const MAX_RETRIES: u32 = 3;
                    let mut last_error = None;
                    let mut success = false;

                    for attempt in 0..=MAX_RETRIES {
                        if attempt > 0 {
                            // Exponential backoff: 10ms, 20ms, 40ms
                            let delay = Duration::from_millis(10 * (1 << (attempt - 1)));
                            tokio::time::sleep(delay).await;
                            tracing::debug!(stream = i, attempt, "Retrying ingest");
                        }

                        match client
                            .ingest_records(IngestRecordsRequest {
                                records: batch.clone(),
                            })
                            .await
                        {
                            Ok(response) => {
                                let count = response.into_inner().assignments.len();
                                metrics.stream_stats[i]
                                    .records_acked
                                    .fetch_add(count as u64, Ordering::Relaxed);
                                metrics
                                    .entities_acked
                                    .fetch_add(count as u64, Ordering::Relaxed);
                                metrics.stream_stats[i]
                                    .last_latency_micros
                                    .store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
                                success = true;
                                break;
                            }
                            Err(e) => {
                                last_error = Some(e);
                            }
                        }
                    }

                    if !success {
                        if let Some(e) = last_error {
                            tracing::error!(stream = i, error = %e, retries = MAX_RETRIES, "Ingest failed after retries");
                        }
                        metrics.stream_stats[i]
                            .errors
                            .fetch_add(1, Ordering::Relaxed);
                        // Still count for progress tracking
                        metrics
                            .entities_acked
                            .fetch_add(batch_len as u64, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    // Wait for generator to finish
    let _ = generator_handle.await;

    // Close the channel to signal streams to stop
    drop(tx);

    // Wait for all streams to finish
    for handle in stream_handles {
        let _ = handle.await;
    }

    metrics.completed.store(true, Ordering::Relaxed);
    Ok(())
}

// =============================================================================
// METRICS POLLING
// =============================================================================

async fn poll_metrics_task(
    config: &LoadTestConfig,
    metrics: Arc<LoadTestMetrics>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(500));

    // Connect to router
    let mut router_client = RouterServiceClient::connect(config.router_addr.clone())
        .await
        .ok();

    // Connect to shards
    let mut shard_clients: Vec<Option<ShardServiceClient<Channel>>> = Vec::new();
    for addr in &config.shard_addrs {
        let client = ShardServiceClient::connect(format!("http://{}", addr))
            .await
            .ok();
        shard_clients.push(client);
    }

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Poll router for aggregated stats
                if let Some(ref mut client) = router_client {
                    if let Ok(response) = client.get_stats(StatsRequest {}).await {
                        let stats = response.into_inner();
                        metrics.cluster_count.store(stats.cluster_count, Ordering::Relaxed);
                        metrics.conflicts_detected.store(stats.conflict_count, Ordering::Relaxed);
                        metrics.cross_shard_merges.store(stats.cross_shard_merges, Ordering::Relaxed);
                        metrics.cross_shard_conflicts.store(stats.cross_shard_conflicts, Ordering::Relaxed);
                        metrics.boundary_keys_tracked.store(stats.boundary_keys_tracked, Ordering::Relaxed);
                    }
                }

                // Poll individual shards for detailed metrics
                for (i, client_opt) in shard_clients.iter_mut().enumerate() {
                    if let Some(ref mut client) = client_opt {
                        if let Ok(response) = client.get_metrics(MetricsRequest {}).await {
                            let m = response.into_inner();
                            if i < metrics.shard_stats.len() {
                                let shard = &metrics.shard_stats[i];
                                shard.record_count.store(m.ingest_records, Ordering::Relaxed);
                                shard.ingest_request_count.store(m.ingest_requests, Ordering::Relaxed);

                                if let Some(latency) = m.ingest_latency {
                                    shard.ingest_latency_total.store(latency.total_micros, Ordering::Relaxed);
                                    shard.ingest_latency_max.store(latency.max_micros, Ordering::Relaxed);
                                    if latency.count > 0 {
                                        shard.ingest_latency_avg.store(
                                            latency.total_micros / latency.count,
                                            Ordering::Relaxed,
                                        );
                                    }
                                }

                                // Store metrics
                                if let Some(store) = m.store {
                                    shard.persistent.store(store.persistent, Ordering::Relaxed);
                                    shard.running_compactions.store(store.running_compactions, Ordering::Relaxed);
                                    shard.running_flushes.store(store.running_flushes, Ordering::Relaxed);
                                    shard.block_cache_usage_mb.store(
                                        store.block_cache_usage_bytes / (1024 * 1024),
                                        Ordering::Relaxed,
                                    );
                                    shard.block_cache_capacity_mb.store(
                                        store.block_cache_capacity_bytes / (1024 * 1024),
                                        Ordering::Relaxed,
                                    );
                                }
                            }
                        }
                        if let Ok(response) = client.get_stats(StatsRequest {}).await {
                            let stats = response.into_inner();
                            if i < metrics.shard_stats.len() {
                                metrics.shard_stats[i].cluster_count.store(stats.cluster_count, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

// =============================================================================
// TUI
// =============================================================================

fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_duration(d: Duration) -> String {
    let total_secs = d.as_secs();
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, secs)
}

fn draw_ui(frame: &mut Frame, config: &LoadTestConfig, metrics: &LoadTestMetrics) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Length(3), // Config
            Constraint::Length(4), // Progress
            Constraint::Length(6), // Throughput + Clusters (with cross-shard stats)
            Constraint::Min(4),    // Streams
            Constraint::Length(4), // Shards (if any)
            Constraint::Length(1), // Footer
        ])
        .split(frame.area());

    // Header
    let elapsed = format_duration(metrics.elapsed());
    let status = if metrics.completed.load(Ordering::Relaxed) {
        Span::styled(
            " COMPLETED ",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(
            " RUNNING ",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    };

    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            "Unirust Load Test",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        status,
        Span::raw("                                    "),
        Span::styled(
            format!("Elapsed: {}", elapsed),
            Style::default().fg(Color::White),
        ),
    ]))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(header, chunks[0]);

    // Config
    let config_text = format!(
        "Count: {} | Router: {} | Streams: {} | Batch: {} | Overlap: {:.0}%",
        format_number(config.count),
        config.router_addr.replace("http://", ""),
        config.stream_count,
        config.batch_size,
        config.overlap_probability * 100.0
    );
    let config_para = Paragraph::new(config_text)
        .style(Style::default().fg(Color::Gray))
        .block(Block::default().title(" Config ").borders(Borders::ALL));
    frame.render_widget(config_para, chunks[1]);

    // Progress
    let progress = metrics.progress();
    let acked = metrics.entities_acked.load(Ordering::Relaxed);
    let eta_str = metrics
        .eta()
        .map(|d| format!("ETA: {}", format_duration(d)))
        .unwrap_or_else(|| "ETA: --:--:--".to_string());

    let gauge = Gauge::default()
        .block(Block::default().title(" Progress ").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Cyan))
        .percent((progress * 100.0) as u16)
        .label(format!(
            "{} / {} ({:.1}%) | {}",
            format_number(acked),
            format_number(config.count),
            progress * 100.0,
            eta_str
        ));
    frame.render_widget(gauge, chunks[2]);

    // Throughput + Clusters (side by side)
    let metrics_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[3]);

    let throughput = metrics.throughput();
    let avg_latency: u64 = metrics
        .stream_stats
        .iter()
        .map(|s| s.last_latency_micros.load(Ordering::Relaxed))
        .sum::<u64>()
        / metrics.stream_stats.len().max(1) as u64;

    let throughput_text = format!(
        "Records/sec: {:.0}\nAvg Latency: {:.2} ms",
        throughput,
        avg_latency as f64 / 1000.0
    );
    let throughput_para = Paragraph::new(throughput_text)
        .style(Style::default().fg(Color::Green))
        .block(Block::default().title(" Throughput ").borders(Borders::ALL));
    frame.render_widget(throughput_para, metrics_chunks[0]);

    let cluster_count = metrics.cluster_count.load(Ordering::Relaxed);
    let conflicts = metrics.conflicts_detected.load(Ordering::Relaxed);
    let cross_shard_merges = metrics.cross_shard_merges.load(Ordering::Relaxed);
    let cross_shard_conflicts = metrics.cross_shard_conflicts.load(Ordering::Relaxed);
    let boundary_keys = metrics.boundary_keys_tracked.load(Ordering::Relaxed);

    let clusters_text = format!(
        "Total: {} | Conflicts: {}\nX-Shard: {} merges, {} blocked\nBoundary keys: {}",
        format_number(cluster_count),
        format_number(conflicts),
        format_number(cross_shard_merges),
        format_number(cross_shard_conflicts),
        format_number(boundary_keys)
    );
    let clusters_para = Paragraph::new(clusters_text)
        .style(Style::default().fg(Color::Yellow))
        .block(Block::default().title(" Clusters ").borders(Borders::ALL));
    frame.render_widget(clusters_para, metrics_chunks[1]);

    // Stream stats
    let stream_rows: Vec<Row> = metrics
        .stream_stats
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let acked = s.records_acked.load(Ordering::Relaxed);
            let latency = s.last_latency_micros.load(Ordering::Relaxed);
            let errors = s.errors.load(Ordering::Relaxed);

            Row::new(vec![
                format!("#{}", i),
                format!("{} sent", format_number(acked)),
                format!("{} ack", format_number(acked)),
                format!("{:.1}ms", latency as f64 / 1000.0),
                if errors > 0 {
                    format!("{} err", errors)
                } else {
                    "OK".to_string()
                },
            ])
        })
        .collect();

    let stream_table = Table::new(
        stream_rows,
        [
            Constraint::Length(4),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(vec!["#", "Sent", "Acked", "Latency", "Status"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::default().title(" Streams ").borders(Borders::ALL));
    frame.render_widget(stream_table, chunks[4]);

    // Shard stats (if any)
    if !metrics.shard_stats.is_empty() {
        let shard_rows: Vec<Row> = metrics
            .shard_stats
            .iter()
            .map(|s| {
                let records = s.record_count.load(Ordering::Relaxed);
                let clusters = s.cluster_count.load(Ordering::Relaxed);
                let latency = s.ingest_latency_avg.load(Ordering::Relaxed);

                Row::new(vec![
                    format!("Shard {}", s.shard_id),
                    format!("{} rec", format_number(records)),
                    format!("{} clusters", format_number(clusters)),
                    format!("{:.1}ms avg", latency as f64 / 1000.0),
                ])
            })
            .collect();

        let shard_table = Table::new(
            shard_rows,
            [
                Constraint::Length(10),
                Constraint::Length(14),
                Constraint::Length(16),
                Constraint::Length(14),
            ],
        )
        .block(Block::default().title(" Shards ").borders(Borders::ALL));
        frame.render_widget(shard_table, chunks[5]);
    }

    // Footer
    let footer = Paragraph::new("[q] Quit").style(Style::default().fg(Color::DarkGray));
    frame.render_widget(footer, chunks[6]);
}

async fn run_tui(
    config: LoadTestConfig,
    metrics: Arc<LoadTestMetrics>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
) -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let tick_rate = Duration::from_millis(100);

    loop {
        // Draw
        terminal.draw(|f| draw_ui(f, &config, &metrics))?;

        // Handle events
        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            let _ = shutdown_tx.send(true);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        // Check if streaming completed - auto-quit
        if metrics.completed.load(Ordering::Relaxed) {
            // Show final state briefly
            terminal.draw(|f| draw_ui(f, &config, &metrics))?;
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = shutdown_tx.send(true);
            break;
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

async fn run_headless(
    config: LoadTestConfig,
    metrics: Arc<LoadTestMetrics>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
) -> Result<()> {
    println!(
        "Running in headless mode - streaming {} entities...",
        config.count
    );
    let start = Instant::now();
    let mut last_print = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Print progress every 5 seconds
        if last_print.elapsed() >= Duration::from_secs(5) {
            let sent = metrics.entities_sent.load(Ordering::Relaxed);
            let acked = metrics.entities_acked.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                acked as f64 / elapsed
            } else {
                0.0
            };
            println!(
                "  Progress: {}/{} sent, {} acked ({:.0} rec/sec)",
                sent, config.count, acked, rate
            );
            last_print = Instant::now();
        }

        // Check if streaming completed
        if metrics.completed.load(Ordering::Relaxed) {
            let _ = shutdown_tx.send(true);
            break;
        }
    }

    println!(
        "Streaming complete in {:.2}s",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_args();

    // Setup file logging if specified
    if let Some(log_path) = &config.log_file {
        let file = std::fs::File::create(log_path)?;
        tracing_subscriber::fmt()
            .with_writer(std::sync::Mutex::new(file))
            .with_ansi(false)
            .init();
    }

    // Initialize metrics
    let metrics = Arc::new(LoadTestMetrics::new(
        config.stream_count,
        config.shard_addrs.len(),
        config.count,
    ));

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Set ontology on router first
    {
        let mut client = RouterServiceClient::connect(config.router_addr.clone()).await?;
        client
            .set_ontology(ApplyOntologyRequest {
                config: Some(build_ontology_config()),
            })
            .await?;
    }

    // Spawn streaming task
    let stream_config = config.clone();
    let stream_metrics = metrics.clone();
    let streaming_handle = tokio::spawn(async move {
        if let Err(e) = run_parallel_streams(&stream_config, stream_metrics.clone()).await {
            tracing::error!(error = %e, "Streaming failed");
            *stream_metrics.error_message.write().await = Some(e.to_string());
        }
    });

    // Spawn metrics polling task
    let poll_config = config.clone();
    let poll_metrics_arc = metrics.clone();
    let poll_shutdown = shutdown_rx.clone();
    let metrics_handle = tokio::spawn(async move {
        poll_metrics_task(&poll_config, poll_metrics_arc, poll_shutdown).await;
    });

    // Run TUI or headless (blocks until quit)
    let tui_config = config.clone();
    if config.headless {
        run_headless(tui_config, metrics.clone(), shutdown_tx).await?;
    } else {
        run_tui(tui_config, metrics.clone(), shutdown_tx).await?;
    }

    // Wait for background tasks
    let _ = streaming_handle.await;
    let _ = metrics_handle.await;

    // Generate and save report
    write_report(&config, &metrics)?;

    Ok(())
}
