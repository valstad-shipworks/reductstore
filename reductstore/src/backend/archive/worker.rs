// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::index::{Candidate, LocalStat};
use super::journal::Op;
use super::remote::checksum;
use super::stub::{stub_path, with_suffix, Stub, RECALL_TMP_EXT, STUB_EXT, STUB_TMP_EXT};
use super::{now_ms, Inner, Kind, Staged, WAL_DIR};
use futures_util::stream::{self, StreamExt};
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

const TICK: Duration = Duration::from_secs(1);
const PROBE_INTERVAL: Duration = Duration::from_secs(15);
const STATS_INTERVAL: Duration = Duration::from_secs(5);
const SPACE_INTERVAL: Duration = Duration::from_secs(30);
const STAGED_TIMEOUT: Duration = Duration::from_secs(60);
const MIN_REMOVAL_DELAY: Duration = Duration::from_secs(60);
const CONCURRENT_COPIES: usize = 4;
const MAX_COPIES_PER_CYCLE: usize = 512;
const MAX_EVICTIONS_PER_CYCLE: usize = 32;
const OVER_LIMIT_WARNING_DELAY: Duration = Duration::from_secs(600);
const MAX_COMPARED_FILE_SIZE: u64 = 64 * 1024;

#[derive(Default)]
pub(super) struct Schedule {
    last_probe: Option<Instant>,
    last_scan: Option<Instant>,
    last_stats: Option<Instant>,
    last_space: Option<Instant>,
    over_limit_since: Option<Instant>,
    warned_over_limit: Option<Instant>,
}

fn due(last: Option<Instant>, interval: Duration) -> bool {
    last.is_none_or(|last| last.elapsed() >= interval)
}

pub(super) fn spawn(inner: &Arc<Inner>) {
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        loop {
            let Some(inner) = weak.upgrade() else {
                break;
            };
            inner.cycle().await;
            drop(inner);
            tokio::time::sleep(TICK).await;
        }
    });
}

struct Job {
    path: PathBuf,
    kind: Kind,
    forced: bool,
}

/// Files that have to reach the archive together.
enum Unit {
    /// A block: its data must be in the archive before the descriptor that describes it.
    Block {
        data: Option<Job>,
        descriptor: Option<Job>,
    },
    File(Job),
}

enum Verdict {
    Verified(Option<u64>),
    LocalChanged(LocalStat),
    ArchiveDiffers,
}

/// Directory, block id and whether the block is compressed.
type BlockKey = (PathBuf, String, bool);

fn block_key(path: &Path) -> Option<BlockKey> {
    let name = path.file_name()?.to_str()?;
    let (id, _) = name.split_once('.')?;
    Some((
        path.parent()?.to_path_buf(),
        id.to_string(),
        name.ends_with(".zst"),
    ))
}

fn data_path_of(descriptor: &Path) -> Option<PathBuf> {
    let (dir, id, compressed) = block_key(descriptor)?;
    Some(dir.join(format!(
        "{}{}",
        id,
        if compressed { ".blk.zst" } else { ".blk" }
    )))
}

impl Inner {
    /// One round of background work.
    pub(super) async fn cycle(self: &Arc<Self>) {
        self.check_connection().await;
        self.scan_if_due().await;
        if self.remote.is_online() {
            self.apply_journal().await;
            self.mirror_pass().await;
            self.evict_pass().await;
        }
        self.refresh_stats().await;
    }

    async fn check_connection(&self) {
        let probe = {
            let schedule = self.schedule.lock();
            self.remote.is_suspect() || due(schedule.last_probe, PROBE_INTERVAL)
        };
        if probe {
            self.remote.connect(self.cfg.adopt).await;
            self.schedule.lock().last_probe = Some(Instant::now());
        }
    }

    async fn scan_if_due(self: &Arc<Self>) {
        if !due(self.schedule.lock().last_scan, self.cfg.scan_interval) {
            return;
        }

        let first = !self.scanned.load(Ordering::Relaxed);
        let inner = Arc::clone(self);
        let started = Instant::now();
        if let Err(err) = tokio::task::spawn_blocking(move || inner.scan(first)).await {
            error!("Failed to scan the data folder for the archive: {}", err);
            return;
        }

        if first {
            info!(
                "Indexed the data folder for the archive in {:?}: {} bytes local, {} bytes archived",
                started.elapsed(),
                self.index.counters.local_bytes.load(Ordering::Relaxed),
                self.index.counters.archived_bytes.load(Ordering::Relaxed),
            );
        }
        self.scanned.store(true, Ordering::Relaxed);
        self.schedule.lock().last_scan = Some(Instant::now());
    }

    /// Compare the index with the data folder.
    ///
    /// This is what finds the files that were there before the archive was
    /// enabled and the changes that the engine made without going through the
    /// file cache.
    fn scan(&self, first: bool) {
        let mut visited = HashSet::new();
        let mut stack = vec![self.root.clone()];

        while let Some(dir) = stack.pop() {
            let seq = self.index.current_seq();
            let Ok(listing) = std::fs::read_dir(&dir) else {
                continue;
            };

            let mut files = Vec::new();
            let mut stubs = Vec::new();
            for entry in listing.flatten() {
                let path = entry.path();
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                let Ok(file_type) = entry.file_type() else {
                    continue;
                };

                if file_type.is_dir() {
                    if name != WAL_DIR && path != self.state_dir {
                        stack.push(path);
                    }
                    continue;
                }
                if name.ends_with(STUB_TMP_EXT) || name.ends_with(RECALL_TMP_EXT) {
                    if first {
                        let _ = std::fs::remove_file(&path);
                    }
                    continue;
                }
                if let Some(block) = name.strip_suffix(STUB_EXT) {
                    stubs.push(block.to_string());
                    continue;
                }

                let Some(kind) = self.classify(&path) else {
                    continue;
                };
                let Ok(metadata) = entry.metadata() else {
                    continue;
                };
                files.push((name, kind, LocalStat::from_metadata(&metadata)));
            }

            self.index.reconcile_dir(&dir, files, stubs, seq);
            visited.insert(dir);
        }

        self.index.retain_dirs(&visited);
    }

    /// Repeat the removals and renames of the data folder on the archive.
    ///
    /// Removals of single files wait until the directory has nothing left to copy:
    /// replacing a block, as compression does, removes the old files right away
    /// while the new ones are copied later, and the archive must not be left
    /// without either in between.
    pub(super) async fn apply_journal(&self) {
        let ops = self.journal.snapshot();
        if ops.is_empty() {
            return;
        }

        let now = now_ms();
        let removal_delay = (self.cfg.sync_delay * 4).max(MIN_REMOVAL_DELAY).as_millis() as u64;
        let last_ordered = ops.iter().rposition(|op| !matches!(op, Op::Rm { .. }));
        let mut applied = vec![false; ops.len()];

        for (i, op) in ops.iter().enumerate() {
            let result = match op {
                Op::Rm { path, t } => {
                    let local = self.root.join(path);
                    let must_precede = last_ordered.is_some_and(|ordered| i < ordered);
                    if local.exists() || stub_path(&local).exists() {
                        Ok(true)
                    } else if !must_precede
                        && now.saturating_sub(*t) < removal_delay
                        && local
                            .parent()
                            .is_some_and(|parent| self.index.dir_has_dirty(parent))
                    {
                        Ok(false)
                    } else {
                        self.remote.remove_file(path).await.map(|_| true)
                    }
                }
                Op::Rmdir { path, .. } => self
                    .remote
                    .remove_dir(path, &self.root.join(path))
                    .await
                    .map(|_| true),
                Op::Mv { from, to, .. } => {
                    if self.root.join(to).exists() && !self.root.join(from).exists() {
                        self.remote.rename(from, to).await.map(|_| true)
                    } else {
                        Ok(true)
                    }
                }
            };

            match result {
                Ok(done) => applied[i] = done,
                Err(err) => {
                    warn!("Failed to apply {:?} to the archive: {}", op, err);
                    break;
                }
            }
        }

        self.journal.complete(&applied);
    }

    pub(super) async fn mirror_pass(&self) {
        let now = now_ms();
        let sync_delay = self.cfg.sync_delay.as_millis() as u64;
        let max_lag = self.cfg.max_lag.as_millis() as u64;

        let mut jobs = Vec::new();
        for path in self.index.dirty_snapshot() {
            let entry = self.index.get(&path);
            let Some(entry) = entry.filter(|entry| entry.needs_mirror()) else {
                self.index.locked(&path, |slot| slot.set_dirty(false));
                continue;
            };
            if entry.evicting {
                continue;
            }

            let quiet = now.saturating_sub(entry.dirty_last_ms) >= sync_delay;
            let overdue = now.saturating_sub(entry.dirty_first_ms) >= max_lag;
            if quiet || overdue {
                jobs.push(Job {
                    path,
                    kind: entry.kind,
                    forced: !quiet,
                });
                if jobs.len() >= MAX_COPIES_PER_CYCLE {
                    break;
                }
            }
        }
        if jobs.is_empty() {
            return;
        }

        let mut blocks: HashMap<BlockKey, (Option<Job>, Option<Job>)> = HashMap::new();
        let mut units = Vec::new();
        for job in jobs {
            match (job.kind, block_key(&job.path)) {
                (Kind::Data, Some(key)) => blocks.entry(key).or_default().0 = Some(job),
                (Kind::Descriptor, Some(key)) => blocks.entry(key).or_default().1 = Some(job),
                _ => units.push(Unit::File(job)),
            }
        }
        units.extend(
            blocks
                .into_values()
                .map(|(data, descriptor)| Unit::Block { data, descriptor }),
        );

        stream::iter(units)
            .for_each_concurrent(CONCURRENT_COPIES, |unit| async move {
                let result = match unit {
                    Unit::File(job) => self.mirror_file(&job.path, job.kind, false).await,
                    Unit::Block { data, descriptor } => self.mirror_block(data, descriptor).await,
                };
                if let Err(err) = result {
                    if err.kind() != io::ErrorKind::NotConnected {
                        warn!("Failed to copy to the archive: {}", err);
                    }
                }
            })
            .await;
    }

    async fn mirror_block(&self, data: Option<Job>, descriptor: Option<Job>) -> io::Result<bool> {
        let forced = data.as_ref().is_some_and(|job| job.forced)
            || descriptor.as_ref().is_some_and(|job| job.forced);

        let Some(descriptor) = descriptor else {
            let Some(data) = data else {
                return Ok(true);
            };
            return self.mirror_file(&data.path, Kind::Data, data.forced).await;
        };

        let data_path = match data {
            Some(job) => Some(job.path),
            None => self.pending_data_of(&descriptor.path),
        };

        if !forced {
            if let Some(data_path) = data_path {
                if !self.mirror_file(&data_path, Kind::Data, false).await? {
                    return Ok(false);
                }
            }
            return self
                .mirror_file(&descriptor.path, Kind::Descriptor, false)
                .await;
        }

        // The block is still being written. Take the descriptor first and the data
        // after it: appended data only adds to what that descriptor describes.
        let seq = self.index.current_seq();
        let path = descriptor.path.clone();
        let snapshot = tokio::task::spawn_blocking(move || {
            let stat = LocalStat::of(&path)?;
            Ok::<_, io::Error>((std::fs::read(&path)?, stat))
        })
        .await
        .map_err(io::Error::other)?;
        let (content, stat) = match snapshot {
            Ok(snapshot) => snapshot,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                self.index.remove(&descriptor.path);
                return Ok(false);
            }
            Err(err) => return Err(err),
        };

        if let Some(data_path) = data_path {
            self.mirror_file(&data_path, Kind::Data, true).await?;
            if self
                .index
                .get(&data_path)
                .is_some_and(|entry| entry.mirrored.is_none())
            {
                return Ok(false);
            }
        }

        let Some(rel) = self.rel(&descriptor.path) else {
            return Ok(false);
        };
        self.remote.put_bytes(&rel, content, stat.mtime_ns).await?;
        self.index.locked(&descriptor.path, |slot| {
            slot.get_or_insert(Kind::Descriptor);
            slot.set_local(Some(stat));
        });
        Ok(self.finish_copy(&descriptor.path, stat, seq))
    }

    /// The data file of a descriptor if it still has to be copied before the descriptor.
    fn pending_data_of(&self, descriptor: &Path) -> Option<PathBuf> {
        let data_path = data_path_of(descriptor)?;
        match self.index.get(&data_path) {
            Some(entry) if entry.archived_len.is_some() => None,
            Some(entry) => {
                let unchanged = entry.is_mirrored()
                    && LocalStat::of(&data_path)
                        .is_ok_and(|now| entry.local.is_some_and(|known| known.same_file(&now)));
                (!unchanged).then_some(data_path)
            }
            None => data_path.exists().then_some(data_path),
        }
    }

    /// Copy one file. Returns whether the archive now holds its current version.
    async fn mirror_file(&self, path: &Path, kind: Kind, allow_growth: bool) -> io::Result<bool> {
        let Some(rel) = self.rel(path) else {
            return Ok(false);
        };

        let seq = self.index.current_seq();
        let stat = match LocalStat::of(path) {
            Ok(stat) => stat,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if !stub_path(path).exists() {
                    self.index.remove(path);
                }
                return Ok(false);
            }
            Err(err) => return Err(err),
        };

        let Some(entry) = self.index.locked(path, |slot| {
            slot.get_or_insert(kind);
            slot.set_local(Some(stat));
            slot.get().map(|entry| *entry)
        }) else {
            return Ok(false);
        };
        let Some(entry) = entry else {
            return Ok(false);
        };

        if !entry.notified && !entry.force_copy {
            let in_archive = match entry.mirrored {
                Some(version) => version == stat.version(),
                None => self
                    .remote
                    .stat(&rel)
                    .await?
                    .is_some_and(|(len, mtime_ns)| {
                        len == stat.len && self.remote.same_mtime(stat.mtime_ns, mtime_ns)
                    }),
            };
            if in_archive {
                return Ok(self.finish_copy(path, stat, seq));
            }
        }

        // The engine rewrites some of its small files, such as bucket settings, on a
        // timer. Without this check each rewrite would be a write on the archive.
        let comparable = kind == Kind::Other && stat.len <= MAX_COMPARED_FILE_SIZE;
        if let Some(known) = entry
            .content_crc
            .filter(|_| comparable && !entry.force_copy)
        {
            let local = path.to_path_buf();
            let current = tokio::task::spawn_blocking(move || checksum(&local))
                .await
                .map_err(io::Error::other)?;
            if current.is_ok_and(|current| current == (stat.len, known)) {
                return Ok(self.finish_copy(path, stat, seq));
            }
        }

        match self.remote.put(path, &rel, allow_growth).await? {
            Some((copied, crc)) => {
                self.stats
                    .last_sync
                    .store(now_ms() / 1000, Ordering::Relaxed);
                self.index.locked(path, |slot| {
                    if let Some(entry) = slot.get() {
                        entry.content_crc = comparable.then_some(crc);
                    }
                });
                Ok(self.finish_copy(path, copied, seq))
            }
            None => Ok(false),
        }
    }

    /// Record a copy that started at `seq`. Returns whether the file is clean now.
    fn finish_copy(&self, path: &Path, copied: LocalStat, seq: u64) -> bool {
        self.index
            .locked(path, |slot| {
                let Some(entry) = slot.get() else {
                    return false;
                };
                entry.mirrored = Some(copied.version());
                entry.force_copy = false;
                if entry.dirty_seq <= seq {
                    entry.notified = false;
                }

                let clean = entry.is_mirrored();
                if clean {
                    slot.set_dirty(false);
                } else {
                    entry.dirty_first_ms = now_ms();
                }
                clean
            })
            .unwrap_or(false)
    }

    pub(super) async fn evict_pass(self: &Arc<Self>) {
        let Some(limit) = self.cfg.local_size else {
            return;
        };
        if !self.scanned.load(Ordering::Relaxed) {
            return;
        }

        let staged_bytes = {
            let mut staged = self.staged.lock();
            let expired: Vec<PathBuf> = staged
                .iter()
                .filter(|(_, staged)| staged.staged_at.elapsed() >= STAGED_TIMEOUT)
                .map(|(path, _)| path.clone())
                .collect();
            for path in expired {
                staged.remove(&path);
                let _ = std::fs::remove_file(with_suffix(&path, STUB_TMP_EXT));
                self.index.locked(&path, |slot| {
                    if let Some(entry) = slot.get() {
                        entry.evicting = false;
                    }
                });
            }
            staged.values().map(|staged| staged.stat.alloc).sum::<u64>()
        };

        let used = self.index.counters.local_bytes.load(Ordering::Relaxed);
        let need = used.saturating_sub(limit).saturating_sub(staged_bytes);
        if need == 0 {
            self.schedule.lock().over_limit_since = None;
            return;
        }

        let inner = Arc::clone(self);
        let min_idle = self.cfg.min_idle.as_millis() as u64;
        let candidates = tokio::task::spawn_blocking(move || {
            inner
                .index
                .eviction_candidates(min_idle, need, MAX_EVICTIONS_PER_CYCLE)
        })
        .await
        .unwrap_or_default();

        if candidates.is_empty() {
            // Being over the limit for a while is normal: blocks have to be copied
            // and to sit idle first. It is worth a warning only when it lasts.
            let mut schedule = self.schedule.lock();
            let since = *schedule.over_limit_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= OVER_LIMIT_WARNING_DELAY
                && due(schedule.warned_over_limit, OVER_LIMIT_WARNING_DELAY)
            {
                schedule.warned_over_limit = Some(Instant::now());
                warn!(
                    "The data folder has been {} bytes over its limit for {:?} and no block can be evicted",
                    need,
                    since.elapsed()
                );
            }
            return;
        }
        self.schedule.lock().over_limit_since = None;

        stream::iter(candidates)
            .for_each_concurrent(CONCURRENT_COPIES, |candidate| async move {
                if let Err(err) = self.stage_eviction(&candidate).await {
                    if err.kind() != io::ErrorKind::NotConnected {
                        warn!("Failed to evict {:?}: {}", candidate.path, err);
                    }
                }
            })
            .await;
    }

    /// Check the archive copy of a block and hand the block to the file cache for removal.
    async fn stage_eviction(&self, candidate: &Candidate) -> io::Result<()> {
        let path = &candidate.path;
        let Some(rel) = self.rel(path) else {
            return Ok(());
        };

        let seq = self.index.current_seq();
        let claimed = self
            .index
            .locked(path, |slot| match slot.get() {
                Some(entry)
                    if !entry.evicting && entry.archived_len.is_none() && entry.is_mirrored() =>
                {
                    entry.evicting = true;
                    true
                }
                _ => false,
            })
            .unwrap_or(false);
        if !claimed {
            return Ok(());
        }

        let release = |force_copy: bool, local: Option<LocalStat>| {
            self.index.locked(path, |slot| {
                if let Some(entry) = slot.get() {
                    entry.evicting = false;
                    if force_copy {
                        entry.force_copy = true;
                    }
                }
                if let Some(local) = local {
                    slot.set_local(Some(local));
                }
                if slot.get().is_some_and(|entry| entry.needs_mirror()) {
                    slot.set_dirty(true);
                }
            });
        };

        let verdict = match self.verify(path, &rel, &candidate.stat).await {
            Ok(verdict) => verdict,
            Err(err) => {
                release(false, None);
                return Err(err);
            }
        };

        let crc64 = match verdict {
            Verdict::Verified(crc64) => crc64,
            Verdict::LocalChanged(now) => {
                release(false, Some(now));
                return Ok(());
            }
            Verdict::ArchiveDiffers => {
                warn!(
                    "The archive copy of {:?} differs from the data folder, copying it again",
                    rel
                );
                release(true, None);
                return Ok(());
            }
        };

        let stub = Stub {
            len: candidate.stat.len,
            mtime_ns: candidate.stat.mtime_ns,
            crc64,
            archived_at: now_ms() / 1000,
        };
        let stub_tmp = with_suffix(path, STUB_TMP_EXT);
        let written = tokio::task::spawn_blocking(move || stub.write(&stub_tmp))
            .await
            .map_err(io::Error::other)
            .and_then(|result| result);
        if let Err(err) = written {
            release(false, None);
            return Err(err);
        }

        self.staged.lock().insert(
            path.clone(),
            Staged {
                seq,
                stat: candidate.stat,
                staged_at: Instant::now(),
            },
        );
        self.ready.lock().push(path.clone());
        Ok(())
    }

    async fn verify(&self, path: &Path, rel: &Path, stat: &LocalStat) -> io::Result<Verdict> {
        let now = LocalStat::of(path)?;
        if !now.same_file(stat) {
            return Ok(Verdict::LocalChanged(now));
        }

        if !self.cfg.verify {
            let in_archive = self.remote.stat(rel).await?.is_some_and(|(len, mtime_ns)| {
                len == stat.len && self.remote.same_mtime(stat.mtime_ns, mtime_ns)
            });
            return Ok(if in_archive {
                Verdict::Verified(None)
            } else {
                Verdict::ArchiveDiffers
            });
        }

        let archived = match self.remote.checksum(rel).await {
            Ok(archived) => archived,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(Verdict::ArchiveDiffers)
            }
            Err(err) => return Err(err),
        };

        let local_path = path.to_path_buf();
        let local = tokio::task::spawn_blocking(move || checksum(&local_path))
            .await
            .map_err(io::Error::other)??;

        Ok(if local == archived && local.0 == stat.len {
            Verdict::Verified(Some(local.1))
        } else if local.0 != stat.len {
            Verdict::LocalChanged(LocalStat::of(path)?)
        } else {
            Verdict::ArchiveDiffers
        })
    }

    async fn refresh_stats(self: &Arc<Self>) {
        let (stats_due, space_due) = {
            let schedule = self.schedule.lock();
            (
                due(schedule.last_stats, STATS_INTERVAL),
                due(schedule.last_space, SPACE_INTERVAL),
            )
        };

        if stats_due {
            let inner = Arc::clone(self);
            if let Ok(pending) =
                tokio::task::spawn_blocking(move || inner.index.pending_stats()).await
            {
                let lag = pending
                    .oldest_ms
                    .map(|oldest| now_ms().saturating_sub(oldest) / 1000)
                    .unwrap_or(0);
                self.stats
                    .pending_files
                    .store(pending.files, Ordering::Relaxed);
                self.stats
                    .pending_bytes
                    .store(pending.bytes, Ordering::Relaxed);
                self.stats.lag_seconds.store(lag, Ordering::Relaxed);
            }
            self.schedule.lock().last_stats = Some(Instant::now());
        }

        if space_due && self.remote.is_online() {
            match self.remote.space().await {
                Ok(space) => *self.stats.space.lock() = Some(space),
                Err(err) => debug!("Failed to read the free space of the archive: {}", err),
            }
            self.schedule.lock().last_space = Some(Instant::now());
        }
    }
}
