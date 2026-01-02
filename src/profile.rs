#[cfg(feature = "profiling")]
use std::sync::{Mutex, OnceLock};
#[cfg(feature = "profiling")]
use std::time::{Duration, Instant};

#[cfg(feature = "profiling")]
#[derive(Debug, Default)]
struct ProfileStats {
    entries: std::collections::HashMap<&'static str, (u64, Duration)>,
}

#[cfg(feature = "profiling")]
impl ProfileStats {
    fn record(&mut self, name: &'static str, duration: Duration) {
        let entry = self.entries.entry(name).or_insert((0, Duration::new(0, 0)));
        entry.0 += 1;
        entry.1 += duration;
    }
}

#[cfg(feature = "profiling")]
static PROFILE: OnceLock<Mutex<ProfileStats>> = OnceLock::new();

#[cfg(feature = "profiling")]
pub struct ProfileGuard {
    name: &'static str,
    start: Instant,
}

#[cfg(feature = "profiling")]
impl Drop for ProfileGuard {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        let profile = PROFILE.get_or_init(|| Mutex::new(ProfileStats::default()));
        if let Ok(mut stats) = profile.lock() {
            stats.record(self.name, duration);
        }
    }
}

#[cfg(feature = "profiling")]
pub fn profile_scope(name: &'static str) -> ProfileGuard {
    ProfileGuard {
        name,
        start: Instant::now(),
    }
}

#[cfg(feature = "profiling")]
pub fn print_report() {
    let profile = PROFILE.get_or_init(|| Mutex::new(ProfileStats::default()));
    let Ok(stats) = profile.lock() else {
        return;
    };

    let mut entries: Vec<_> = stats
        .entries
        .iter()
        .map(|(name, (count, total))| (*name, *count, *total))
        .collect();

    entries.sort_by(|a, b| b.2.cmp(&a.2));

    println!("\nProfiling report:");
    for (name, count, total) in entries {
        let avg = if count == 0 {
            Duration::new(0, 0)
        } else {
            total / count as u32
        };
        println!(
            "  {:<32} total: {:>8.3}s  avg: {:>8.3}ms  count: {}",
            name,
            total.as_secs_f64(),
            avg.as_secs_f64() * 1000.0,
            count
        );
    }
}

#[cfg(not(feature = "profiling"))]
pub struct ProfileGuard;

#[cfg(not(feature = "profiling"))]
pub fn profile_scope(_name: &'static str) -> ProfileGuard {
    ProfileGuard
}

#[cfg(not(feature = "profiling"))]
pub fn print_report() {}
