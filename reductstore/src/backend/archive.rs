// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Storage backend that keeps the data folder on a local disk and mirrors it to an
//! archive directory, see `archive/README.md`.

mod index;
mod journal;
mod remote;
mod stub;
mod worker;

use crate::backend::file::AccessMode;
use crate::backend::{ObjectMetadata, StorageBackend};
use crate::cfg::archive::ArchiveConfig;
use async_trait::async_trait;
use index::{Index, LocalStat, Touch};
use journal::{Journal, Op};
use log::{debug, warn};
use parking_lot::Mutex;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::msg::server_api::ArchiveInfo;
use remote::Remote;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use stub::{stub_path, with_suffix, Stub, RECALL_TMP_EXT, STUB_EXT, STUB_TMP_EXT};

const STATE_DIR: &str = ".archive";
const JOURNAL_FILE: &str = "journal";
const WAL_DIR: &str = ".wal";
const LOCK_FILE: &str = ".lock";
const RECALL_STRIPES: usize = 64;

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    /// Block data, the only files that may leave the data folder
    Data,
    Descriptor,
    Other,
}

fn is_data_name(name: &str) -> bool {
    name.ends_with(".blk") || name.ends_with(".blk.zst")
}

fn kind_of_name(name: &str) -> Option<Kind> {
    if name.ends_with(".tmp") || name.ends_with(".log") || name.ends_with(STUB_EXT) {
        None
    } else if is_data_name(name) {
        Some(Kind::Data)
    } else if name.ends_with(".meta") || name.ends_with(".meta.zst") {
        Some(Kind::Descriptor)
    } else {
        Some(Kind::Other)
    }
}

/// A verified eviction waiting for the file cache to release the file.
struct Staged {
    seq: u64,
    stat: LocalStat,
    staged_at: Instant,
}

#[derive(Default)]
struct Stats {
    last_sync: AtomicU64,
    recalled_files: AtomicU64,
    pending_files: AtomicU64,
    pending_bytes: AtomicU64,
    lag_seconds: AtomicU64,
    space: Mutex<Option<(u64, u64)>>,
}

pub(super) struct Inner {
    root: PathBuf,
    state_dir: PathBuf,
    cfg: ArchiveConfig,
    index: Index,
    remote: Remote,
    journal: Journal,
    staged: Mutex<HashMap<PathBuf, Staged>>,
    ready: Mutex<Vec<PathBuf>>,
    recall_locks: Vec<tokio::sync::Mutex<()>>,
    scanned: AtomicBool,
    stats: Stats,
    schedule: Mutex<worker::Schedule>,
}

pub(crate) struct ArchiveBackend {
    inner: Arc<Inner>,
}

impl ArchiveBackend {
    pub async fn try_new(data_path: PathBuf, cfg: ArchiveConfig) -> Result<Self, ReductError> {
        let backend = Self::build(data_path, cfg)?;
        worker::spawn(&backend.inner);
        Ok(backend)
    }

    fn build(data_path: PathBuf, cfg: ArchiveConfig) -> Result<Self, ReductError> {
        let init = || -> io::Result<Inner> {
            std::fs::create_dir_all(&data_path)?;
            let root = data_path.canonicalize()?;
            let archive = cfg.path.canonicalize().unwrap_or_else(|_| cfg.path.clone());
            if archive.starts_with(&root) || root.starts_with(&archive) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Archive {:?} and data folder {:?} must not contain each other",
                        archive, root
                    ),
                ));
            }

            let state_dir = root.join(STATE_DIR);
            std::fs::create_dir_all(&state_dir)?;
            Ok(Inner {
                journal: Journal::open(state_dir.join(JOURNAL_FILE))?,
                remote: Remote::new(cfg.path.clone(), state_dir.clone(), cfg.io_timeout),
                root,
                state_dir,
                cfg,
                index: Index::default(),
                staged: Mutex::new(HashMap::new()),
                ready: Mutex::new(Vec::new()),
                recall_locks: (0..RECALL_STRIPES)
                    .map(|_| tokio::sync::Mutex::new(()))
                    .collect(),
                scanned: AtomicBool::new(false),
                stats: Stats::default(),
                schedule: Mutex::new(worker::Schedule::default()),
            })
        };

        let inner = init()
            .map_err(|err| internal_server_error!("Failed to initialise archive: {}", err))?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
}

impl Inner {
    fn rel(&self, path: &Path) -> Option<PathBuf> {
        path.strip_prefix(&self.root).ok().map(Path::to_path_buf)
    }

    /// Kind of a file that belongs in the archive, `None` for everything that stays local.
    fn classify(&self, path: &Path) -> Option<Kind> {
        let kind = kind_of_name(path.file_name()?.to_str()?)?;
        let parent = path.parent()?;
        if !parent.starts_with(&self.root)
            || parent.starts_with(&self.state_dir)
            || parent.file_name().is_some_and(|name| name == WAL_DIR)
            || (parent == self.root && path.file_name().is_some_and(|name| name == LOCK_FILE))
        {
            return None;
        }
        Some(kind)
    }

    fn is_data(&self, path: &Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(is_data_name)
            && path.starts_with(&self.root)
    }

    fn recall_lock(&self, path: &Path) -> &tokio::sync::Mutex<()> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut hasher);
        &self.recall_locks[hasher.finish() as usize % RECALL_STRIPES]
    }

    /// Bring a block that lives only in the archive back into the data folder.
    async fn recall(&self, path: &Path) -> io::Result<()> {
        let Some(rel) = self.rel(path).filter(|_| self.is_data(path)) else {
            return Ok(());
        };

        let _guard = self.recall_lock(path).lock().await;
        if path.exists() {
            return Ok(());
        }

        let stub_file = stub_path(path);
        let stub = match Stub::read(&stub_file) {
            Ok(stub) => stub,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };

        let tmp = with_suffix(path, RECALL_TMP_EXT);
        let started = Instant::now();
        if let Err(err) = self.remote.get(&rel, &tmp, stub.clone()).await {
            warn!("Failed to fetch {:?} from the archive: {}", rel, err);
            return Err(io::Error::new(
                err.kind(),
                format!("Failed to fetch {:?} from the archive: {}", rel, err),
            ));
        }

        let committed = self
            .index
            .locked(path, |slot| -> io::Result<bool> {
                if !stub_file.exists() {
                    return Ok(false);
                }
                std::fs::rename(&tmp, path)?;
                let _ = std::fs::remove_file(&stub_file);
                let stat = LocalStat::of(path)?;

                let seq = self.index.next_seq();
                let entry = slot.get_or_insert(Kind::Data);
                entry.mirrored = Some(stat.version());
                entry.notified = false;
                entry.force_copy = false;
                entry.evicting = false;
                entry.last_access_ms = now_ms();
                entry.touch_seq = seq;
                slot.set_archived(None);
                slot.set_local(Some(stat));
                slot.set_dirty(false);
                Ok(true)
            })
            .unwrap_or(Ok(false));

        if !matches!(committed, Ok(true)) {
            let _ = std::fs::remove_file(&tmp);
        }
        if committed? {
            self.stats.recalled_files.fetch_add(1, Ordering::Relaxed);
            debug!(
                "Fetched {:?} ({} bytes) from the archive in {:?}",
                rel,
                stub.len,
                started.elapsed()
            );
        }
        Ok(())
    }

    /// Replace a staged block by its stub, unless it was used since it was verified.
    fn commit_eviction(&self, path: &Path) -> io::Result<bool> {
        let Some(staged) = self.staged.lock().remove(path) else {
            return Ok(false);
        };

        let stub_tmp = with_suffix(path, STUB_TMP_EXT);
        let committed = self
            .index
            .locked(path, |slot| -> io::Result<bool> {
                let Some(entry) = slot.get() else {
                    return Ok(false);
                };
                entry.evicting = false;

                let untouched = entry.archived_len.is_none()
                    && entry.is_mirrored()
                    && entry.touch_seq <= staged.seq
                    && entry.mirrored == Some(staged.stat.version())
                    && LocalStat::of(path).is_ok_and(|now| now.same_file(&staged.stat));
                if !untouched {
                    return Ok(false);
                }

                // The stub has to be durable before the block goes away: a block
                // without either would be dropped by the integrity check on restart.
                std::fs::rename(&stub_tmp, stub_path(path))?;
                if let Some(parent) = path.parent() {
                    sync_dir(parent)?;
                }
                std::fs::remove_file(path)?;

                slot.set_local(None);
                slot.set_archived(Some(staged.stat.len));
                slot.set_dirty(false);
                Ok(true)
            })
            .unwrap_or(Ok(false));

        if !matches!(committed, Ok(true)) {
            let _ = std::fs::remove_file(&stub_tmp);
        }
        if matches!(committed, Ok(true)) {
            debug!("Evicted {:?} from the data folder", path);
        }
        committed
    }

    fn info(&self) -> ArchiveInfo {
        let counters = &self.index.counters;
        let space = *self.stats.space.lock();
        let last_sync = self.stats.last_sync.load(Ordering::Relaxed);

        ArchiveInfo {
            path: self.remote.root().to_string_lossy().into_owned(),
            online: self.remote.is_online(),
            ready: self.scanned.load(Ordering::Relaxed),
            local_size_limit: self.cfg.local_size,
            local_usage: counters.local_bytes.load(Ordering::Relaxed),
            archived_usage: counters.archived_bytes.load(Ordering::Relaxed),
            archived_files: counters.archived_files.load(Ordering::Relaxed),
            pending_usage: self.stats.pending_bytes.load(Ordering::Relaxed),
            pending_files: self.stats.pending_files.load(Ordering::Relaxed),
            pending_removals: self.journal.len() as u64,
            lag: self.stats.lag_seconds.load(Ordering::Relaxed),
            archive_free: space.map(|(free, _)| free),
            archive_total: space.map(|(_, total)| total),
            last_sync: (last_sync > 0).then_some(last_sync),
            recalled_files: self.stats.recalled_files.load(Ordering::Relaxed),
            last_error: self.remote.last_error(),
        }
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

fn remove_if_exists(path: &Path) -> io::Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

#[async_trait]
impl StorageBackend for ArchiveBackend {
    fn path(&self) -> &PathBuf {
        &self.inner.root
    }

    async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let inner = &self.inner;
        let tracked = inner.rel(from).zip(inner.rel(to));
        if let Some((from_rel, to_rel)) = tracked.clone() {
            // Written down before the rename happens: stubs that move with a directory
            // point at archive files that only this operation can move after them.
            inner.journal.push(Op::mv(from_rel, to_rel), true);
        }

        let evicted_file = inner.is_data(from) && !from.exists() && stub_path(from).exists();
        if evicted_file {
            tokio::fs::rename(stub_path(from), stub_path(to)).await?;
        } else {
            tokio::fs::rename(from, to).await?;
        }

        if tracked.is_some() {
            inner.index.rename(from, to);
            let mut staged = inner.staged.lock();
            staged.retain(|path, _| !path.starts_with(from));
        }
        Ok(())
    }

    async fn remove(&self, path: &Path) -> io::Result<()> {
        let inner = &self.inner;
        let Some(kind) = inner.classify(path) else {
            return tokio::fs::remove_file(path).await;
        };

        let removed = inner
            .index
            .locked(path, |slot| -> io::Result<bool> {
                let mut removed = remove_if_exists(path)?;
                if kind == Kind::Data {
                    removed |= remove_if_exists(&stub_path(path))?;
                    let _ = std::fs::remove_file(with_suffix(path, STUB_TMP_EXT));
                }
                slot.remove();
                Ok(removed)
            })
            .unwrap_or(Ok(false))?;

        inner.staged.lock().remove(path);
        if let Some(rel) = inner.rel(path) {
            inner.journal.push(Op::rm(rel), false);
        }

        if removed {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("File {:?} does not exist", path),
            ))
        }
    }

    async fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let inner = &self.inner;
        let tracked = inner
            .rel(path)
            .filter(|rel| !rel.as_os_str().is_empty() && !path.starts_with(&inner.state_dir));
        if let Some(rel) = tracked.clone() {
            inner.journal.push(Op::rmdir(rel), true);
        }

        let result = tokio::fs::remove_dir_all(path).await;
        if tracked.is_some() {
            inner.index.remove_subtree(path);
            inner
                .staged
                .lock()
                .retain(|staged, _| !staged.starts_with(path));
        }
        result
    }

    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        tokio::fs::create_dir_all(path).await
    }

    async fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut dir = tokio::fs::read_dir(path).await?;
        let mut entries = Vec::new();
        let mut evicted = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            let entry = entry.path();
            let Some(name) = entry.to_str() else {
                entries.push(entry);
                continue;
            };

            if name.ends_with(STUB_TMP_EXT) || name.ends_with(RECALL_TMP_EXT) {
                continue;
            }
            match name.strip_suffix(STUB_EXT) {
                Some(block) => evicted.push(PathBuf::from(block)),
                None => entries.push(entry),
            }
        }

        if !evicted.is_empty() {
            let local: HashSet<PathBuf> = entries.iter().cloned().collect();
            entries.extend(evicted.into_iter().filter(|block| !local.contains(block)));
        }
        Ok(entries)
    }

    async fn try_exists(&self, path: &Path) -> io::Result<bool> {
        if path.try_exists()? {
            return Ok(true);
        }
        if self.inner.is_data(path) {
            return stub_path(path).try_exists();
        }
        Ok(false)
    }

    async fn upload(&self, path: &Path) -> io::Result<()> {
        if let Some(kind) = self.inner.classify(path) {
            self.inner.index.mark_dirty(path, kind);
        }
        Ok(())
    }

    async fn download(&self, path: &Path) -> io::Result<()> {
        self.inner.recall(path).await
    }

    async fn update_local_cache(&self, path: &Path, _mode: &AccessMode) -> io::Result<()> {
        if !self.inner.is_data(path) {
            return Ok(());
        }
        match self.inner.index.touch(path) {
            Touch::Evicted if !path.exists() => Err(io::Error::new(
                io::ErrorKind::StaleNetworkFileHandle,
                format!("File {:?} was moved to the archive", path),
            )),
            _ => Ok(()),
        }
    }

    async fn prepare_open(&self, path: &Path, _mode: &AccessMode) -> io::Result<()> {
        if self.inner.is_data(path) {
            self.inner.index.touch(path);
        }
        Ok(())
    }

    async fn invalidate_locally_cached_files(&self) -> Vec<PathBuf> {
        std::mem::take(&mut *self.inner.ready.lock())
    }

    async fn remove_from_local_cache_if_unused(&self, path: &Path) -> io::Result<bool> {
        // An error here would end the sync pass of the file cache, and the block
        // simply stays where it is.
        Ok(self.inner.commit_eviction(path).unwrap_or_else(|err| {
            warn!("Failed to evict {:?}: {}", path, err);
            false
        }))
    }

    fn guards_eviction(&self) -> bool {
        true
    }

    fn stores_in_place(&self) -> bool {
        true
    }

    async fn get_stats(&self, path: &Path) -> io::Result<Option<ObjectMetadata>> {
        match tokio::fs::metadata(path).await {
            Ok(metadata) => Ok(Some(ObjectMetadata {
                size: Some(metadata.len() as i64),
                modified_time: metadata.modified().ok(),
            })),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if !self.inner.is_data(path) {
                    return Ok(None);
                }
                match Stub::read(&stub_path(path)) {
                    Ok(stub) => Ok(Some(ObjectMetadata {
                        size: Some(stub.len as i64),
                        modified_time: Some(UNIX_EPOCH + Duration::from_nanos(stub.mtime_ns)),
                    })),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn remove_from_local_cache(&self, _path: &Path) -> io::Result<()> {
        // The data folder is the primary copy, not a cache of the archive.
        Ok(())
    }

    fn archive_info(&self) -> Option<ArchiveInfo> {
        Some(self.inner.info())
    }
}

#[cfg(test)]
mod tests;
