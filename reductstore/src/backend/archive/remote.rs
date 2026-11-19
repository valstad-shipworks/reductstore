// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::index::LocalStat;
use super::stub::Stub;
use crc64fast::Digest;
use log::{info, warn};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;

pub(super) const MARKER_FILE: &str = ".reductstore-archive";
const DATA_DIR: &str = "data";
const TMP_DIR: &str = ".tmp";
const INSTANCE_FILE: &str = "instance";

const COPY_BUFFER_SIZE: usize = 1024 * 1024;
const MAX_CONCURRENT_OPERATIONS: usize = 8;
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Serialize, Deserialize, Default)]
struct Marker {
    #[serde(default)]
    instance: String,
    #[serde(default)]
    created: u64,
}

/// Access to the archive directory.
///
/// The archive is expected to be a network mount that can stall or disappear, so
/// nothing here runs on the caller's task: every operation is a blocking closure on
/// the blocking pool, bounded in number and in time. A stalled mount therefore
/// costs at most `MAX_CONCURRENT_OPERATIONS` parked threads and turns into errors
/// for the callers instead of hanging them.
pub(super) struct Remote {
    root: PathBuf,
    state_dir: PathBuf,
    permits: Arc<Semaphore>,
    timeout: Duration,
    online: AtomicBool,
    suspect: AtomicBool,
    mtime_granularity_ns: AtomicU64,
    tmp_counter: AtomicU64,
    last_error: Mutex<Option<String>>,
}

fn offline_error() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "Archive is offline")
}

/// Errors that say something about a file rather than about the archive as a whole.
fn is_expected(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::AlreadyExists | io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput
    )
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

fn mtime_of(ns: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

impl Remote {
    pub fn new(root: PathBuf, state_dir: PathBuf, timeout: Duration) -> Self {
        Self {
            root,
            state_dir,
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_OPERATIONS)),
            timeout,
            online: AtomicBool::new(false),
            suspect: AtomicBool::new(false),
            mtime_granularity_ns: AtomicU64::new(1),
            tmp_counter: AtomicU64::new(0),
            last_error: Mutex::new(None),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn is_online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
    }

    pub fn is_suspect(&self) -> bool {
        self.suspect.load(Ordering::Relaxed)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().clone()
    }

    pub fn data_path(&self, rel: &Path) -> PathBuf {
        self.root.join(DATA_DIR).join(rel)
    }

    fn tmp_path(&self) -> PathBuf {
        let id = self.tmp_counter.fetch_add(1, Ordering::Relaxed);
        self.root
            .join(TMP_DIR)
            .join(format!("{}-{}", std::process::id(), id))
    }

    fn set_offline(&self, reason: &str) {
        if self.online.swap(false, Ordering::Relaxed) {
            warn!("Archive {:?} went offline: {}", self.root, reason);
        }
        *self.last_error.lock() = Some(reason.to_string());
    }

    /// Run a blocking archive operation with the archive's concurrency and time limits.
    pub async fn run<T: Send + 'static>(
        &self,
        op: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        if !self.is_online() {
            return Err(offline_error());
        }
        self.run_with_timeout(self.timeout, op).await
    }

    async fn run_with_timeout<T: Send + 'static>(
        &self,
        timeout: Duration,
        op: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let permits = Arc::clone(&self.permits);
        let Ok(Ok(permit)) = tokio::time::timeout(timeout, permits.acquire_owned()).await else {
            self.set_offline("the archive does not respond");
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Archive does not respond",
            ));
        };

        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            op()
        });

        match tokio::time::timeout(timeout, task).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(err))) => {
                if !is_expected(&err) {
                    self.suspect.store(true, Ordering::Relaxed);
                    *self.last_error.lock() = Some(err.to_string());
                }
                Err(err)
            }
            Ok(Err(err)) => Err(io::Error::other(err)),
            Err(_) => {
                self.set_offline("an archive operation timed out");
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Archive operation timed out",
                ))
            }
        }
    }

    /// Check that the archive is mounted and belongs to this data folder.
    ///
    /// The archive is recognised by a marker file that the operator creates once on
    /// the share. The server never creates it, so an unmounted share, which looks
    /// like an empty directory, can't be mistaken for a new archive.
    pub async fn connect(&self, adopt: bool) -> bool {
        let root = self.root.clone();
        let state_dir = self.state_dir.clone();
        let was_online = self.is_online();

        let result = self
            .run_with_timeout(PROBE_TIMEOUT.min(self.timeout), move || {
                if was_online {
                    check_identity(&root, &state_dir)
                } else {
                    attach(&root, &state_dir, adopt)
                }
            })
            .await;

        self.suspect.store(false, Ordering::Relaxed);
        match result {
            Ok(granularity) => {
                if let Some(granularity) = granularity {
                    self.mtime_granularity_ns
                        .store(granularity, Ordering::Relaxed);
                }
                if !self.online.swap(true, Ordering::Relaxed) {
                    info!("Archive {:?} is online", self.root);
                }
                *self.last_error.lock() = None;
                true
            }
            Err(err) => {
                self.set_offline(&err.to_string());
                false
            }
        }
    }

    pub fn same_mtime(&self, local_ns: u64, remote_ns: u64) -> bool {
        let granularity = self.mtime_granularity_ns.load(Ordering::Relaxed).max(1);
        local_ns.abs_diff(remote_ns) < granularity
    }

    pub async fn stat(&self, rel: &Path) -> io::Result<Option<(u64, u64)>> {
        let path = self.data_path(rel);
        self.run(move || match std::fs::metadata(&path) {
            Ok(metadata) => {
                let stat = LocalStat::from_metadata(&metadata);
                Ok(Some((stat.len, stat.mtime_ns)))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        })
        .await
    }

    /// Copy a local file into the archive.
    ///
    /// Returns the stat and the CRC64 of the copied version, or `None` when the
    /// file changed while it was read. With `allow_growth` an append during the
    /// copy is accepted, because a longer file still contains the version that
    /// was seen first.
    pub async fn put(
        &self,
        local: &Path,
        rel: &Path,
        allow_growth: bool,
    ) -> io::Result<Option<(LocalStat, u64)>> {
        let local = local.to_path_buf();
        let dst = self.data_path(rel);
        let tmp = self.tmp_path();
        self.run(move || {
            let result = put_file(&local, &dst, &tmp, allow_growth);
            if !matches!(result, Ok(Some(_))) {
                let _ = std::fs::remove_file(&tmp);
            }
            result
        })
        .await
    }

    pub async fn put_bytes(&self, rel: &Path, content: Vec<u8>, mtime_ns: u64) -> io::Result<()> {
        let dst = self.data_path(rel);
        let tmp = self.tmp_path();
        self.run(move || {
            let result = (|| {
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut file = File::create(&tmp)?;
                file.write_all(&content)?;
                file.sync_all()?;
                file.set_modified(mtime_of(mtime_ns))?;
                drop(file);
                std::fs::rename(&tmp, &dst)
            })();
            if result.is_err() {
                let _ = std::fs::remove_file(&tmp);
            }
            result
        })
        .await
    }

    /// Fetch an archived file into `local_tmp` and check it against its stub.
    pub async fn get(&self, rel: &Path, local_tmp: &Path, stub: Stub) -> io::Result<()> {
        let src = self.data_path(rel);
        let local_tmp = local_tmp.to_path_buf();
        self.run(move || {
            let result = get_file(&src, &local_tmp, &stub);
            if result.is_err() {
                let _ = std::fs::remove_file(&local_tmp);
            }
            result
        })
        .await
    }

    pub async fn checksum(&self, rel: &Path) -> io::Result<(u64, u64)> {
        let path = self.data_path(rel);
        self.run(move || checksum(&path)).await
    }

    pub async fn remove_file(&self, rel: &Path) -> io::Result<()> {
        let path = self.data_path(rel);
        self.run(move || match std::fs::remove_file(&path) {
            Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
            _ => Ok(()),
        })
        .await
    }

    /// Remove a directory from the archive.
    ///
    /// When the directory exists again in the data folder, only the archived files
    /// that have no local counterpart are removed.
    pub async fn remove_dir(&self, rel: &Path, local: &Path) -> io::Result<()> {
        let path = self.data_path(rel);
        let local = local.to_path_buf();
        self.run(move || {
            if !local.exists() {
                return match std::fs::remove_dir_all(&path) {
                    Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
                    _ => Ok(()),
                };
            }
            prune_dir(&path, &local)
        })
        .await
    }

    pub async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let from = self.data_path(from);
        let to = self.data_path(to);
        self.run(move || {
            if !from.exists() {
                return Ok(());
            }
            if to.is_dir() {
                std::fs::remove_dir_all(&to)?;
            }
            if let Some(parent) = to.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::rename(&from, &to)
        })
        .await
    }

    pub async fn space(&self) -> io::Result<(u64, u64)> {
        let root = self.root.clone();
        self.run(move || Ok((fs4::available_space(&root)?, fs4::total_space(&root)?)))
            .await
    }
}

fn read_marker(root: &Path) -> io::Result<Marker> {
    let path = root.join(MARKER_FILE);
    let content = std::fs::read(&path).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Archive marker {:?} not found. Mount the share and create this empty file once to initialise the archive",
                    path
                ),
            )
        } else {
            err
        }
    })?;

    if content.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(Marker::default());
    }
    serde_json::from_slice(&content).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid archive marker {:?}: {}", path, err),
        )
    })
}

fn read_local_instance(state_dir: &Path) -> io::Result<Option<String>> {
    match std::fs::read_to_string(state_dir.join(INSTANCE_FILE)) {
        Ok(id) if !id.trim().is_empty() => Ok(Some(id.trim().to_string())),
        Ok(_) => Ok(None),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn write_synced(path: &Path, content: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(content)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)
}

fn check_identity(root: &Path, state_dir: &Path) -> io::Result<Option<u64>> {
    let marker = read_marker(root)?;
    let local = read_local_instance(state_dir)?;
    if local.as_deref() != Some(marker.instance.as_str()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Archive {:?} belongs to another data folder", root),
        ));
    }
    Ok(None)
}

fn attach(root: &Path, state_dir: &Path, adopt: bool) -> io::Result<Option<u64>> {
    let marker = read_marker(root)?;
    let local = read_local_instance(state_dir)?;
    let instance_path = state_dir.join(INSTANCE_FILE);

    match (local, marker.instance.is_empty()) {
        (Some(local), false) if local == marker.instance => {}
        (Some(_), false) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Archive {:?} belongs to another data folder", root),
            ));
        }
        (None, false) if adopt => {
            write_synced(&instance_path, marker.instance.as_bytes())?;
            info!(
                "Attached the data folder to the existing archive {:?}",
                root
            );
        }
        (None, false) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Archive {:?} is already used by another data folder. Set RS_ARCHIVE_ADOPT=true to attach to it",
                    root
                ),
            ));
        }
        (local, true) => {
            let instance = match local {
                Some(instance) => {
                    warn!(
                        "Archive {:?} is empty although the data folder was archived before",
                        root
                    );
                    instance
                }
                None => {
                    let instance = uuid::Uuid::new_v4().to_string();
                    write_synced(&instance_path, instance.as_bytes())?;
                    instance
                }
            };
            let marker = Marker {
                instance,
                created: unix_seconds(),
            };
            let content = serde_json::to_vec(&marker).map_err(io::Error::other)?;
            write_synced(&root.join(MARKER_FILE), &content)?;
            info!("Initialised archive {:?}", root);
        }
    }

    std::fs::create_dir_all(root.join(DATA_DIR))?;
    let tmp_dir = root.join(TMP_DIR);
    if tmp_dir.exists() {
        std::fs::remove_dir_all(&tmp_dir)?;
    }
    std::fs::create_dir_all(&tmp_dir)?;

    Ok(Some(probe_mtime_granularity(&tmp_dir)?))
}

fn probe_mtime_granularity(tmp_dir: &Path) -> io::Result<u64> {
    const PROBE_NS: u64 = 1_700_000_000_123_456_789;
    let path = tmp_dir.join("mtime-probe");
    let file = File::create(&path)?;
    file.set_modified(mtime_of(PROBE_NS))?;
    file.sync_all()?;
    drop(file);
    let stored = LocalStat::of(&path)?.mtime_ns;
    let _ = std::fs::remove_file(&path);

    let lost = PROBE_NS.abs_diff(stored);
    Ok([1u64, 100, 1_000, 1_000_000, 1_000_000_000, 2_000_000_000]
        .into_iter()
        .find(|granularity| lost < *granularity)
        .unwrap_or(2_000_000_000))
}

/// Copy `len` bytes, leaving holes in the destination where the source is all zeros.
///
/// Blocks are preallocated to their maximum size, so most of an unfinished block
/// is a hole that must not turn into real data on the archive.
fn copy_sparse(
    src: &mut File,
    dst: &mut File,
    len: u64,
    mut digest: Option<&mut Digest>,
) -> io::Result<u64> {
    let mut buf = vec![0u8; COPY_BUFFER_SIZE];
    let mut copied = 0u64;
    let mut hole = false;

    while copied < len {
        let want = (len - copied).min(COPY_BUFFER_SIZE as u64) as usize;
        let read = src.read(&mut buf[..want])?;
        if read == 0 {
            break;
        }

        let chunk = &buf[..read];
        if let Some(digest) = digest.as_mut() {
            digest.write(chunk);
        }

        if chunk.iter().all(|byte| *byte == 0) {
            hole = true;
        } else {
            if hole {
                dst.seek(SeekFrom::Start(copied))?;
                hole = false;
            }
            dst.write_all(chunk)?;
        }
        copied += read as u64;
    }

    dst.set_len(copied)?;
    Ok(copied)
}

fn put_file(
    local: &Path,
    dst: &Path,
    tmp: &Path,
    allow_growth: bool,
) -> io::Result<Option<(LocalStat, u64)>> {
    let mut src = File::open(local)?;
    let before = LocalStat::from_metadata(&src.metadata()?);

    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut out = File::create(tmp)?;
    let mut digest = Digest::new();
    let copied = copy_sparse(&mut src, &mut out, before.len, Some(&mut digest))?;
    out.sync_all()?;
    out.set_modified(mtime_of(before.mtime_ns))?;
    drop(out);

    let after = LocalStat::of(local)?;
    let stable = copied == before.len
        && (after.same_file(&before)
            || (allow_growth && after.ino == before.ino && after.len >= before.len));
    if !stable {
        return Ok(None);
    }

    std::fs::rename(tmp, dst)?;
    Ok(Some((before, digest.sum64())))
}

fn get_file(src: &Path, local_tmp: &Path, stub: &Stub) -> io::Result<()> {
    let mut input = File::open(src)?;
    let mut out = File::create(local_tmp)?;
    let mut digest = Digest::new();
    let copied = copy_sparse(&mut input, &mut out, u64::MAX, Some(&mut digest))?;

    if copied != stub.len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Archived file {:?} has {} bytes, expected {}",
                src, copied, stub.len
            ),
        ));
    }
    if stub.crc64.is_some_and(|crc| crc != digest.sum64()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Archived file {:?} failed the integrity check", src),
        ));
    }

    out.sync_all()?;
    out.set_modified(mtime_of(stub.mtime_ns))?;
    Ok(())
}

/// Length and CRC64 of a file.
pub(super) fn checksum(path: &Path) -> io::Result<(u64, u64)> {
    let mut file = File::open(path)?;
    let mut digest = Digest::new();
    let mut buf = vec![0u8; COPY_BUFFER_SIZE];
    let mut len = 0u64;
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        digest.write(&buf[..read]);
        len += read as u64;
    }
    Ok((len, digest.sum64()))
}

fn prune_dir(remote: &Path, local: &Path) -> io::Result<()> {
    let entries = match std::fs::read_dir(remote) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };

    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let local_path = local.join(&name);
        if entry.file_type()?.is_dir() {
            if local_path.exists() {
                prune_dir(&entry.path(), &local_path)?;
            } else {
                std::fs::remove_dir_all(entry.path())?;
            }
        } else if !local_path.exists() && !super::stub::stub_path(&local_path).exists() {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    struct Dirs {
        local: PathBuf,
        archive: PathBuf,
        state: PathBuf,
    }

    #[fixture]
    fn dirs() -> Dirs {
        let root = tempdir().unwrap().keep();
        let dirs = Dirs {
            local: root.join("local"),
            archive: root.join("archive"),
            state: root.join("local/.archive"),
        };
        std::fs::create_dir_all(&dirs.state).unwrap();
        std::fs::create_dir_all(&dirs.archive).unwrap();
        dirs
    }

    fn remote(dirs: &Dirs) -> Remote {
        Remote::new(
            dirs.archive.clone(),
            dirs.state.clone(),
            Duration::from_secs(10),
        )
    }

    async fn online(dirs: &Dirs) -> Remote {
        std::fs::write(dirs.archive.join(MARKER_FILE), b"").unwrap();
        let remote = remote(dirs);
        assert!(remote.connect(false).await);
        remote
    }

    #[rstest]
    #[tokio::test]
    async fn stays_offline_without_marker(dirs: Dirs) {
        let remote = remote(&dirs);
        assert!(!remote.connect(false).await);
        assert!(!remote.is_online());
        assert!(remote.last_error().unwrap().contains("marker"));
        assert_eq!(
            remote.stat(Path::new("a")).await.unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        assert!(!dirs.archive.join(DATA_DIR).exists());
    }

    #[rstest]
    #[tokio::test]
    async fn adopts_empty_marker_and_keeps_identity(dirs: Dirs) {
        let first = online(&dirs).await;
        assert!(first.is_online());
        let instance = std::fs::read_to_string(dirs.state.join(INSTANCE_FILE)).unwrap();
        assert!(!instance.is_empty());

        let second = remote(&dirs);
        assert!(second.connect(false).await);
        assert_eq!(
            std::fs::read_to_string(dirs.state.join(INSTANCE_FILE)).unwrap(),
            instance
        );
    }

    #[rstest]
    #[tokio::test]
    async fn refuses_archive_of_another_data_folder(dirs: Dirs) {
        online(&dirs).await;
        std::fs::write(dirs.state.join(INSTANCE_FILE), "someone-else").unwrap();

        let remote = remote(&dirs);
        assert!(!remote.connect(false).await);
        assert!(remote.last_error().unwrap().contains("another data folder"));
    }

    #[rstest]
    #[tokio::test]
    async fn attaches_to_existing_archive_only_when_asked(dirs: Dirs) {
        online(&dirs).await;
        std::fs::remove_file(dirs.state.join(INSTANCE_FILE)).unwrap();

        assert!(!remote(&dirs).connect(false).await);
        assert!(remote(&dirs).connect(true).await);
        assert!(remote(&dirs).connect(false).await);
    }

    #[rstest]
    #[tokio::test]
    async fn goes_offline_when_marker_disappears(dirs: Dirs) {
        let remote = online(&dirs).await;
        std::fs::remove_file(dirs.archive.join(MARKER_FILE)).unwrap();

        assert!(!remote.connect(false).await);
        assert!(!remote.is_online());
    }

    #[rstest]
    #[tokio::test]
    async fn put_and_get_round_trip_with_holes(dirs: Dirs) {
        let remote = online(&dirs).await;
        let local = dirs.local.join("1.blk");
        let mut content = vec![7u8; 3 * COPY_BUFFER_SIZE];
        content[COPY_BUFFER_SIZE..2 * COPY_BUFFER_SIZE].fill(0);
        std::fs::write(&local, &content).unwrap();
        let file = File::options().write(true).open(&local).unwrap();
        file.set_len(5 * COPY_BUFFER_SIZE as u64).unwrap();
        drop(file);
        content.resize(5 * COPY_BUFFER_SIZE, 0);

        let rel = Path::new("bucket/entry/1.blk");
        let (stat, copied_crc) = remote.put(&local, rel, false).await.unwrap().unwrap();
        assert_eq!(stat.len, content.len() as u64);
        assert_eq!(copied_crc, checksum(&local).unwrap().1);
        assert_eq!(std::fs::read(remote.data_path(rel)).unwrap(), content);

        let (len, mtime_ns) = remote.stat(rel).await.unwrap().unwrap();
        assert_eq!(len, stat.len);
        assert!(remote.same_mtime(stat.mtime_ns, mtime_ns));

        let (len, crc) = remote.checksum(rel).await.unwrap();
        assert_eq!((len, crc), checksum(&local).unwrap());

        let back = dirs.local.join("back.tmp");
        let stub = Stub {
            len,
            mtime_ns: stat.mtime_ns,
            crc64: Some(crc),
            archived_at: 0,
        };
        remote.get(rel, &back, stub.clone()).await.unwrap();
        assert_eq!(std::fs::read(&back).unwrap(), content);
        assert_eq!(LocalStat::of(&back).unwrap().mtime_ns, stat.mtime_ns);

        let bad = Stub {
            crc64: Some(crc ^ 1),
            ..stub
        };
        let err = remote.get(rel, &back, bad).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(!back.exists());
    }

    #[rstest]
    #[tokio::test]
    async fn remove_dir_keeps_files_that_exist_locally_again(dirs: Dirs) {
        let remote = online(&dirs).await;
        let rel = Path::new("bucket/entry");
        let archived = remote.data_path(rel);
        std::fs::create_dir_all(&archived).unwrap();
        std::fs::write(archived.join("old.blk"), b"old").unwrap();
        std::fs::write(archived.join("kept.blk"), b"kept").unwrap();
        std::fs::write(archived.join("stubbed.blk"), b"stubbed").unwrap();

        let local = dirs.local.join("bucket/entry");
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("kept.blk"), b"kept").unwrap();
        std::fs::write(local.join("stubbed.blk.archived"), b"{}").unwrap();

        remote.remove_dir(rel, &local).await.unwrap();
        assert!(!archived.join("old.blk").exists());
        assert!(archived.join("kept.blk").exists());
        assert!(archived.join("stubbed.blk").exists());

        std::fs::remove_dir_all(&local).unwrap();
        remote.remove_dir(rel, &local).await.unwrap();
        assert!(!archived.exists());
    }

    #[rstest]
    #[tokio::test]
    async fn rename_moves_archived_directory(dirs: Dirs) {
        let remote = online(&dirs).await;
        let from = remote.data_path(Path::new("bucket/old"));
        std::fs::create_dir_all(&from).unwrap();
        std::fs::write(from.join("1.blk"), b"data").unwrap();

        remote
            .rename(Path::new("bucket/old"), Path::new("bucket/new"))
            .await
            .unwrap();
        assert!(!from.exists());
        assert!(remote.data_path(Path::new("bucket/new/1.blk")).exists());

        remote
            .rename(Path::new("bucket/missing"), Path::new("bucket/other"))
            .await
            .unwrap();
    }
}
