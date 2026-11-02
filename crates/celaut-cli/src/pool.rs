//! The pool of instances, kept in a file between commands.
//!
//! `celaut` has no process that stays: each command reads the file, changes it and ends. The
//! file is locked while it is read and written, so commands that run at the same time see
//! each other's changes. A start can take minutes, so it runs without the lock.
//!
//! The file holds the tokens of the instances: it is created readable by its owner only.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Where the pool is, unless `--state` or `CELAUT_STATE` says another place.
pub const DEFAULT_STATE: &str = "/tmp/celaut/pool.json";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Pool {
    next: u64,
    pub instances: Vec<Instance>,
    /// The services that were sent to the node already: their next starts use the hash.
    pub sent: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: u64,
    /// The hex id of the service.
    pub service: String,
    /// The hex of the `Configuration` of the start: an instance is reused only for the same
    /// service and configuration.
    pub configuration: String,
    pub uri: String,
    /// The credential of the instance. It never leaves this file.
    pub token: String,
    pub leased: bool,
    /// Unix seconds.
    pub last_used: u64,
    pub timeouts: u32,
    pub failures: u32,
}

impl Pool {
    pub fn next_id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    /// The idle instance of `service` and `configuration` that was given back last.
    pub fn take_idle(&mut self, service: &str, configuration: &str) -> Option<&mut Instance> {
        self.instances
            .iter_mut()
            .filter(|i| !i.leased && i.service == service && i.configuration == configuration)
            .max_by_key(|i| (i.last_used, i.id))
    }

    pub fn get(&mut self, id: u64) -> Option<&mut Instance> {
        self.instances.iter_mut().find(|i| i.id == id)
    }

    pub fn remove(&mut self, id: u64) -> Option<Instance> {
        let at = self.instances.iter().position(|i| i.id == id)?;
        Some(self.instances.remove(at))
    }
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Run `f` on the pool at `path` with the file locked, and write what it left.
pub fn with<R>(path: &Path, f: impl FnOnce(&mut Pool) -> R) -> std::io::Result<R> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)?;
    file.lock()?;
    let mut pool = read(&mut file)?;
    let out = f(&mut pool);
    let text = serde_json::to_vec(&pool).map_err(std::io::Error::other)?;
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&text)?;
    // The lock goes with the file.
    Ok(out)
}

fn read(file: &mut File) -> std::io::Result<Pool> {
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    if text.trim().is_empty() {
        return Ok(Pool::default());
    }
    serde_json::from_str(&text).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("the pool file does not parse: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(pool: &mut Pool, last_used: u64) -> u64 {
        let id = pool.next_id();
        pool.instances.push(Instance {
            id,
            service: "aa".into(),
            configuration: String::new(),
            uri: "10.0.0.2:1".into(),
            token: "t".into(),
            leased: false,
            last_used,
            timeouts: 0,
            failures: 0,
        });
        id
    }

    #[test]
    fn the_instance_given_back_last_goes_out_first() {
        let mut pool = Pool::default();
        instance(&mut pool, 10);
        let last = instance(&mut pool, 20);
        assert_eq!(pool.take_idle("aa", "").unwrap().id, last);
        pool.get(last).unwrap().leased = true;
        assert_ne!(pool.take_idle("aa", "").unwrap().id, last);
        assert!(pool.take_idle("bb", "").is_none());
    }

    #[test]
    fn the_pool_stays_in_its_file() {
        let dir = std::env::temp_dir().join(format!("celaut-pool-{}", std::process::id()));
        let path = dir.join("pool.json");
        let id = with(&path, |p| instance(p, 1)).unwrap();
        let found = with(&path, |p| p.get(id).map(|i| i.uri.clone())).unwrap();
        assert_eq!(found.as_deref(), Some("10.0.0.2:1"));
        let mode = std::os::unix::fs::PermissionsExt::mode(
            &std::fs::metadata(&path).unwrap().permissions(),
        );
        assert_eq!(mode & 0o077, 0, "only the owner reads the tokens");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
