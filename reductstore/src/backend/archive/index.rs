// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::stub::{stub_path, Stub};
use super::{now_ms, Kind};
use parking_lot::{Mutex, RwLock};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::Metadata;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

pub(super) type Version = (u64, u64, u64);

/// What is known about a file of the data folder at the time it was last looked at.
#[derive(Clone, Copy, Debug)]
pub(super) struct LocalStat {
    pub len: u64,
    pub alloc: u64,
    pub mtime_ns: u64,
    pub ino: u64,
}

impl LocalStat {
    pub fn from_metadata(metadata: &Metadata) -> Self {
        let mtime_ns = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|since| since.as_nanos() as u64)
            .unwrap_or(0);

        #[cfg(unix)]
        let (alloc, ino) = {
            use std::os::unix::fs::MetadataExt;
            (metadata.blocks() * 512, metadata.ino())
        };
        #[cfg(not(unix))]
        let (alloc, ino) = (metadata.len(), 0);

        Self {
            len: metadata.len(),
            alloc,
            mtime_ns,
            ino,
        }
    }

    pub fn of(path: &Path) -> io::Result<Self> {
        Ok(Self::from_metadata(&std::fs::metadata(path)?))
    }

    /// The part of the stat that changes whenever the content does.
    ///
    /// The inode is included because a block that is rewritten through a temporary
    /// file can keep its size and, within the clock tick of the filesystem, its
    /// modification time.
    pub fn version(&self) -> Version {
        (self.len, self.mtime_ns, self.ino)
    }

    pub fn same_file(&self, other: &LocalStat) -> bool {
        self.version() == other.version()
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Entry {
    pub kind: Kind,
    /// Stat of the local file, `None` until it is looked at or when it is evicted
    pub local: Option<LocalStat>,
    /// Version of the local file that the archive copy corresponds to
    pub mirrored: Option<Version>,
    /// Length of the block when it lives only in the archive
    pub archived_len: Option<u64>,
    pub in_dirty: bool,
    /// The engine reported a change that no copy has covered yet
    pub notified: bool,
    /// The archive copy can't be trusted even if its size and time match
    pub force_copy: bool,
    /// CRC64 of the archive copy of a small file, to skip rewrites that change nothing
    pub content_crc: Option<u64>,
    pub evicting: bool,
    pub dirty_first_ms: u64,
    pub dirty_last_ms: u64,
    pub last_access_ms: u64,
    pub touch_seq: u64,
    pub dirty_seq: u64,
}

impl Entry {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            local: None,
            mirrored: None,
            archived_len: None,
            in_dirty: false,
            notified: false,
            force_copy: false,
            content_crc: None,
            evicting: false,
            dirty_first_ms: 0,
            dirty_last_ms: 0,
            last_access_ms: 0,
            touch_seq: 0,
            dirty_seq: 0,
        }
    }

    pub fn needs_mirror(&self) -> bool {
        if self.archived_len.is_some() {
            return false;
        }
        if self.notified || self.force_copy {
            return true;
        }
        match (self.local, self.mirrored) {
            (Some(local), Some(mirrored)) => local.version() != mirrored,
            _ => true,
        }
    }

    pub fn is_mirrored(&self) -> bool {
        !self.needs_mirror()
    }
}

#[derive(Default)]
pub(super) struct Dir {
    files: HashMap<Box<str>, Entry>,
}

#[derive(Default)]
pub(super) struct Counters {
    pub local_bytes: AtomicU64,
    pub archived_bytes: AtomicU64,
    pub archived_files: AtomicU64,
}

pub(super) enum Touch {
    Tracked,
    Untracked,
    Evicted,
}

fn block_id(name: &str) -> Option<u64> {
    name.split('.').next()?.parse().ok()
}

pub(super) struct Candidate {
    pub path: PathBuf,
    pub stat: LocalStat,
    heat: u64,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.heat == other.heat
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.heat.cmp(&other.heat)
    }
}

#[derive(Default, Debug, PartialEq)]
pub(super) struct PendingStats {
    pub files: u64,
    pub bytes: u64,
    pub oldest_ms: Option<u64>,
}

/// In-memory picture of the data folder used to decide what to copy and what to evict.
///
/// It is a cache of the filesystem, never the source of truth: the engine-facing
/// operations answer from the files and stubs on disk, so a stale index can only
/// delay a copy or an eviction.
#[derive(Default)]
pub(super) struct Index {
    dirs: RwLock<HashMap<PathBuf, Arc<Mutex<Dir>>>>,
    dirty: Mutex<HashSet<PathBuf>>,
    seq: AtomicU64,
    pub counters: Counters,
}

/// A file slot of a directory, held under the directory lock.
pub(super) struct Slot<'a> {
    index: &'a Index,
    dir: &'a mut Dir,
    path: &'a Path,
    name: &'a str,
}

impl Slot<'_> {
    pub fn get(&mut self) -> Option<&mut Entry> {
        self.dir.files.get_mut(self.name)
    }

    pub fn get_or_insert(&mut self, kind: Kind) -> &mut Entry {
        if !self.dir.files.contains_key(self.name) {
            self.dir.files.insert(self.name.into(), Entry::new(kind));
        }
        self.dir.files.get_mut(self.name).unwrap()
    }

    pub fn set_local(&mut self, stat: Option<LocalStat>) {
        let counters = &self.index.counters;
        if let Some(entry) = self.dir.files.get_mut(self.name) {
            if let Some(old) = entry.local.take() {
                counters.local_bytes.fetch_sub(old.alloc, Ordering::Relaxed);
            }
            if let Some(new) = stat {
                counters.local_bytes.fetch_add(new.alloc, Ordering::Relaxed);
            }
            entry.local = stat;
        }
    }

    pub fn set_archived(&mut self, len: Option<u64>) {
        let counters = &self.index.counters;
        if let Some(entry) = self.dir.files.get_mut(self.name) {
            if let Some(old) = entry.archived_len.take() {
                counters.archived_bytes.fetch_sub(old, Ordering::Relaxed);
                counters.archived_files.fetch_sub(1, Ordering::Relaxed);
            }
            if let Some(new) = len {
                counters.archived_bytes.fetch_add(new, Ordering::Relaxed);
                counters.archived_files.fetch_add(1, Ordering::Relaxed);
            }
            entry.archived_len = len;
        }
    }

    /// Queue the file for a copy or take it out of the queue.
    pub fn set_dirty(&mut self, dirty: bool) {
        let Some(entry) = self.dir.files.get_mut(self.name) else {
            if !dirty {
                self.index.dirty.lock().remove(self.path);
            }
            return;
        };

        if dirty {
            let now = now_ms();
            if entry.dirty_first_ms == 0 {
                entry.dirty_first_ms = now;
            }
            if entry.dirty_last_ms == 0 {
                entry.dirty_last_ms = now;
            }
            if !entry.in_dirty {
                entry.in_dirty = true;
                self.index.dirty.lock().insert(self.path.to_path_buf());
            }
        } else {
            entry.dirty_first_ms = 0;
            entry.dirty_last_ms = 0;
            if entry.in_dirty {
                entry.in_dirty = false;
                self.index.dirty.lock().remove(self.path);
            }
        }
    }

    pub fn remove(&mut self) {
        self.set_local(None);
        self.set_archived(None);
        self.set_dirty(false);
        self.dir.files.remove(self.name);
    }
}

fn split(path: &Path) -> Option<(&Path, &str)> {
    Some((path.parent()?, path.file_name()?.to_str()?))
}

impl Index {
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    fn dir_or_insert(&self, parent: &Path) -> Arc<Mutex<Dir>> {
        if let Some(dir) = self.dirs.read().get(parent) {
            return Arc::clone(dir);
        }
        Arc::clone(self.dirs.write().entry(parent.to_path_buf()).or_default())
    }

    /// Run `f` on the slot of `path` while holding the lock of its directory.
    ///
    /// Everything that changes a file together with its index entry runs through
    /// here, which is what serialises evictions, recalls and removals of one file.
    pub fn locked<R>(&self, path: &Path, f: impl FnOnce(&mut Slot) -> R) -> Option<R> {
        let (parent, name) = split(path)?;
        let dir = self.dir_or_insert(parent);
        let mut dir = dir.lock();
        Some(f(&mut Slot {
            index: self,
            dir: &mut dir,
            path,
            name,
        }))
    }

    pub fn get(&self, path: &Path) -> Option<Entry> {
        let (parent, name) = split(path)?;
        let dirs = self.dirs.read();
        let dir = dirs.get(parent)?.lock();
        dir.files.get(name).copied()
    }

    /// Record that the engine changed a file.
    pub fn mark_dirty(&self, path: &Path, kind: Kind) {
        self.locked(path, |slot| {
            let seq = slot.index.next_seq();
            let now = now_ms();
            let entry = slot.get_or_insert(kind);
            entry.notified = true;
            entry.touch_seq = seq;
            entry.dirty_seq = seq;
            entry.dirty_last_ms = now;
            let local = entry.archived_len.is_none();
            if local {
                slot.set_dirty(true);
            }
        });
    }

    /// Record an access of a data block without creating an entry for it.
    pub fn touch(&self, path: &Path) -> Touch {
        let Some((parent, name)) = split(path) else {
            return Touch::Untracked;
        };
        let dirs = self.dirs.read();
        let Some(dir) = dirs.get(parent) else {
            return Touch::Untracked;
        };
        let mut dir = dir.lock();
        let Some(entry) = dir.files.get_mut(name) else {
            return Touch::Untracked;
        };
        if entry.archived_len.is_some() {
            return Touch::Evicted;
        }
        entry.last_access_ms = now_ms();
        entry.touch_seq = self.next_seq();
        Touch::Tracked
    }

    pub fn remove(&self, path: &Path) {
        self.locked(path, |slot| slot.remove());
    }

    pub fn remove_subtree(&self, root: &Path) {
        let removed: Vec<Arc<Mutex<Dir>>> = {
            let mut dirs = self.dirs.write();
            let keys: Vec<PathBuf> = dirs
                .keys()
                .filter(|dir| dir.starts_with(root))
                .cloned()
                .collect();
            keys.iter().filter_map(|key| dirs.remove(key)).collect()
        };

        for dir in removed {
            let dir = dir.lock();
            for entry in dir.files.values() {
                self.forget(entry);
            }
        }
        self.dirty.lock().retain(|path| !path.starts_with(root));
    }

    fn forget(&self, entry: &Entry) {
        if let Some(local) = entry.local {
            self.counters
                .local_bytes
                .fetch_sub(local.alloc, Ordering::Relaxed);
        }
        if let Some(len) = entry.archived_len {
            self.counters
                .archived_bytes
                .fetch_sub(len, Ordering::Relaxed);
            self.counters.archived_files.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Follow a rename of a directory or of a single file.
    pub fn rename(&self, from: &Path, to: &Path) {
        let rebase = |path: &Path| path.strip_prefix(from).map(|rest| to.join(rest)).ok();

        let moved_dirs = {
            let mut dirs = self.dirs.write();
            let keys: Vec<PathBuf> = dirs
                .keys()
                .filter(|dir| dir.starts_with(from))
                .cloned()
                .collect();
            for key in &keys {
                if let (Some(dir), Some(new_key)) = (dirs.remove(key), rebase(key)) {
                    dirs.insert(new_key, dir);
                }
            }
            !keys.is_empty()
        };

        if moved_dirs {
            let mut dirty = self.dirty.lock();
            let moved: Vec<PathBuf> = dirty
                .iter()
                .filter(|path| path.starts_with(from))
                .cloned()
                .collect();
            for path in moved {
                dirty.remove(&path);
                if let Some(new_path) = rebase(&path) {
                    dirty.insert(new_path);
                }
            }
            return;
        }

        let entry = self
            .locked(from, |slot| {
                let entry = slot.get().map(|entry| *entry);
                slot.remove();
                entry
            })
            .flatten();
        if let Some(entry) = entry {
            self.locked(to, |slot| {
                *slot.get_or_insert(entry.kind) = Entry {
                    local: None,
                    archived_len: None,
                    in_dirty: false,
                    ..entry
                };
                slot.set_local(entry.local);
                slot.set_archived(entry.archived_len);
                slot.set_dirty(entry.in_dirty);
            });
        }
    }

    #[cfg(test)]
    pub fn dirty_len(&self) -> usize {
        self.dirty.lock().len()
    }

    pub fn dirty_snapshot(&self) -> Vec<PathBuf> {
        self.dirty.lock().iter().cloned().collect()
    }

    pub fn dir_has_dirty(&self, parent: &Path) -> bool {
        let dirs = self.dirs.read();
        let Some(dir) = dirs.get(parent) else {
            return false;
        };
        let dir = dir.lock();
        dir.files.values().any(|entry| entry.in_dirty)
    }

    pub fn pending_stats(&self) -> PendingStats {
        let mut stats = PendingStats::default();
        for path in self.dirty_snapshot() {
            let Some(entry) = self.get(&path) else {
                continue;
            };
            stats.files += 1;
            stats.bytes += entry.local.map(|local| local.len).unwrap_or(0);
            if entry.dirty_first_ms > 0 {
                stats.oldest_ms = Some(stats.oldest_ms.map_or(entry.dirty_first_ms, |oldest| {
                    oldest.min(entry.dirty_first_ms)
                }));
            }
        }
        stats
    }

    /// The coldest data blocks that can leave the data folder, enough to free `need` bytes.
    ///
    /// A block is as warm as the later of its last modification and its last access,
    /// so the folder keeps what was written or read most recently.
    pub fn eviction_candidates(&self, min_idle_ms: u64, need: u64, max: usize) -> Vec<Candidate> {
        let now = now_ms();
        let dirs: Vec<(PathBuf, Arc<Mutex<Dir>>)> = self
            .dirs
            .read()
            .iter()
            .map(|(path, dir)| (path.clone(), Arc::clone(dir)))
            .collect();

        let mut coldest: BinaryHeap<Candidate> = BinaryHeap::with_capacity(max + 1);
        for (dir_path, dir) in dirs {
            let dir = dir.lock();
            // The newest block of an entry is where its next record goes. Without it
            // a write would have to wait for the archive, and fail while it is away.
            let newest = dir
                .files
                .iter()
                .filter(|(_, entry)| entry.kind == Kind::Data)
                .filter_map(|(name, _)| block_id(name))
                .max();
            for (name, entry) in dir.files.iter() {
                let Some(local) = entry.local else {
                    continue;
                };
                if entry.kind != Kind::Data
                    || block_id(name) == newest
                    || entry.evicting
                    || entry.in_dirty
                    || local.alloc == 0
                    || !entry.is_mirrored()
                {
                    continue;
                }

                let heat = (local.mtime_ns / 1_000_000).max(entry.last_access_ms);
                if now.saturating_sub(heat) < min_idle_ms {
                    continue;
                }
                if coldest.len() == max && coldest.peek().is_some_and(|top| top.heat <= heat) {
                    continue;
                }

                coldest.push(Candidate {
                    path: dir_path.join(name.as_ref()),
                    stat: local,
                    heat,
                });
                if coldest.len() > max {
                    coldest.pop();
                }
            }
        }

        let mut freed = 0u64;
        coldest
            .into_sorted_vec()
            .into_iter()
            .take_while(|candidate| {
                let take = freed < need;
                freed += candidate.stat.alloc;
                take
            })
            .collect()
    }

    /// Bring the entries of one directory in line with what a scan found on disk.
    ///
    /// `seq_before` is the sequence number taken before the directory was listed:
    /// entries the engine touched after that are left alone because the listing may
    /// predate them.
    pub fn reconcile_dir(
        &self,
        dir_path: &Path,
        files: Vec<(String, Kind, LocalStat)>,
        stubs: Vec<String>,
        seq_before: u64,
    ) {
        let mut seen: HashSet<&str> = HashSet::with_capacity(files.len() + stubs.len());

        for (name, kind, stat) in files.iter() {
            seen.insert(name.as_str());
            let path = dir_path.join(name);
            self.locked(&path, |slot| {
                let entry = slot.get_or_insert(*kind);
                if entry.dirty_seq > seq_before || entry.evicting {
                    return;
                }
                if entry.archived_len.is_some() {
                    // A stub next to its file is what an interrupted eviction or recall
                    // leaves behind, and in both cases the file is complete.
                    if path.exists() {
                        let _ = std::fs::remove_file(stub_path(&path));
                        slot.set_archived(None);
                    } else {
                        return;
                    }
                }

                let unchanged = slot
                    .get()
                    .and_then(|entry| entry.local)
                    .is_some_and(|known| known.same_file(stat) && known.alloc == stat.alloc);
                if !unchanged {
                    slot.set_local(Some(*stat));
                }
                if slot.get().is_some_and(|entry| entry.needs_mirror()) {
                    slot.set_dirty(true);
                }
            });
        }

        for name in stubs.iter() {
            if seen.contains(name.as_str()) {
                let path = dir_path.join(name);
                self.locked(&path, |slot| {
                    if path.exists() && !slot.get().is_some_and(|entry| entry.evicting) {
                        let _ = std::fs::remove_file(stub_path(&path));
                    }
                });
                continue;
            }
            seen.insert(name.as_str());

            let path = dir_path.join(name);
            self.locked(&path, |slot| {
                let entry = slot.get_or_insert(Kind::Data);
                if entry.archived_len.is_some() || entry.dirty_seq > seq_before {
                    return;
                }
                let stub = Stub::read(&stub_path(&path)).ok();
                let entry = slot.get_or_insert(Kind::Data);
                entry.mirrored = stub.as_ref().map(|stub| (stub.len, stub.mtime_ns, 0));
                entry.notified = false;
                entry.force_copy = false;
                slot.set_local(None);
                slot.set_archived(Some(stub.map(|stub| stub.len).unwrap_or(0)));
                slot.set_dirty(false);
            });
        }

        let Some(dir) = self.dirs.read().get(dir_path).cloned() else {
            return;
        };
        let mut dir = dir.lock();
        let gone: Vec<Box<str>> = dir
            .files
            .iter()
            .filter(|(name, entry)| {
                !seen.contains(name.as_ref())
                    && entry.touch_seq <= seq_before
                    && entry.dirty_seq <= seq_before
                    && !entry.evicting
            })
            .map(|(name, _)| name.clone())
            .collect();
        for name in gone {
            let path = dir_path.join(name.as_ref());
            Slot {
                index: self,
                dir: &mut dir,
                path: &path,
                name: name.as_ref(),
            }
            .remove();
        }
    }

    /// Drop the directories that a scan did not visit and that are gone from disk.
    pub fn retain_dirs(&self, visited: &HashSet<PathBuf>) {
        let stale: Vec<PathBuf> = self
            .dirs
            .read()
            .keys()
            .filter(|dir| !visited.contains(*dir) && !dir.exists())
            .cloned()
            .collect();
        for dir in stale {
            self.remove_subtree(&dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn stat(len: u64, mtime_ns: u64) -> LocalStat {
        LocalStat {
            len,
            alloc: len,
            mtime_ns,
            ino: 1,
        }
    }

    fn track(index: &Index, path: &Path, stat: LocalStat, mirrored: bool) {
        index.locked(path, |slot| {
            slot.get_or_insert(Kind::Data);
            slot.set_local(Some(stat));
            if mirrored {
                slot.get().unwrap().mirrored = Some(stat.version());
            }
        });
    }

    #[rstest]
    fn mark_dirty_queues_file_once() {
        let index = Index::default();
        let path = Path::new("/data/b/e/1.blk");

        index.mark_dirty(path, Kind::Data);
        index.mark_dirty(path, Kind::Data);

        assert_eq!(index.dirty_snapshot(), vec![path.to_path_buf()]);
        assert!(index.get(path).unwrap().needs_mirror());
        assert!(index.dir_has_dirty(Path::new("/data/b/e")));
    }

    #[rstest]
    fn counters_follow_local_and_archived_state() {
        let index = Index::default();
        let path = Path::new("/data/b/e/1.blk");
        track(&index, path, stat(100, 1), true);
        assert_eq!(index.counters.local_bytes.load(Ordering::Relaxed), 100);

        index.locked(path, |slot| {
            slot.set_local(None);
            slot.set_archived(Some(100));
        });
        assert_eq!(index.counters.local_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(index.counters.archived_bytes.load(Ordering::Relaxed), 100);
        assert_eq!(index.counters.archived_files.load(Ordering::Relaxed), 1);

        index.remove(path);
        assert_eq!(index.counters.archived_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(index.counters.archived_files.load(Ordering::Relaxed), 0);
        assert!(index.get(path).is_none());
    }

    #[rstest]
    fn touch_reports_evicted_blocks() {
        let index = Index::default();
        let path = Path::new("/data/b/e/1.blk");
        assert!(matches!(index.touch(path), Touch::Untracked));

        track(&index, path, stat(100, 1), true);
        assert!(matches!(index.touch(path), Touch::Tracked));
        assert!(index.get(path).unwrap().last_access_ms > 0);

        index.locked(path, |slot| slot.set_archived(Some(100)));
        assert!(matches!(index.touch(path), Touch::Evicted));
    }

    #[rstest]
    fn candidates_are_coldest_mirrored_blocks() {
        let index = Index::default();
        let old = Path::new("/data/b/e/1.blk");
        let newer = Path::new("/data/b/e/2.blk");
        let unmirrored = Path::new("/data/b/e/3.blk");
        let descriptor = Path::new("/data/b/e/1.meta");
        track(&index, old, stat(100, 1_000_000), true);
        track(&index, newer, stat(100, 2_000_000), true);
        track(&index, unmirrored, stat(100, 500_000), false);
        index.locked(descriptor, |slot| {
            slot.get_or_insert(Kind::Descriptor).mirrored = Some(stat(10, 1).version());
            slot.set_local(Some(stat(10, 1)));
        });

        let candidates = index.eviction_candidates(0, 150, 16);
        let paths: Vec<&Path> = candidates.iter().map(|c| c.path.as_path()).collect();
        assert_eq!(paths, vec![old, newer]);

        let candidates = index.eviction_candidates(0, 50, 16);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].path, old);
    }

    #[rstest]
    fn candidates_never_include_the_newest_block_of_an_entry() {
        let index = Index::default();
        let only = Path::new("/data/b/single/7.blk");
        let older = Path::new("/data/b/e/1.blk");
        let newest = Path::new("/data/b/e/2.blk.zst");
        track(&index, only, stat(100, 1_000_000), true);
        track(&index, older, stat(100, 3_000_000), true);
        track(&index, newest, stat(100, 2_000_000), true);

        let candidates = index.eviction_candidates(0, 1000, 16);
        let paths: Vec<&Path> = candidates.iter().map(|c| c.path.as_path()).collect();
        assert_eq!(paths, vec![older]);
    }

    #[rstest]
    fn candidates_skip_recently_accessed_blocks() {
        let index = Index::default();
        let path = Path::new("/data/b/e/1.blk");
        track(&index, path, stat(100, 1_000_000), true);
        track(
            &index,
            Path::new("/data/b/e/2.blk"),
            stat(100, 2_000_000),
            true,
        );
        assert_eq!(index.eviction_candidates(60_000, 100, 16).len(), 1);

        index.touch(path);
        assert!(index.eviction_candidates(60_000, 100, 16).is_empty());
    }

    #[rstest]
    fn rename_moves_directories_and_dirty_paths() {
        let index = Index::default();
        let old = Path::new("/data/b/old/1.blk");
        track(&index, old, stat(100, 1), false);
        index.mark_dirty(old, Kind::Data);

        index.rename(Path::new("/data/b/old"), Path::new("/data/b/new"));

        let new = Path::new("/data/b/new/1.blk");
        assert!(index.get(old).is_none());
        assert!(index.get(new).is_some());
        assert_eq!(index.dirty_snapshot(), vec![new.to_path_buf()]);
        assert_eq!(index.counters.local_bytes.load(Ordering::Relaxed), 100);
    }

    #[rstest]
    fn remove_subtree_releases_counters() {
        let index = Index::default();
        track(&index, Path::new("/data/b/e/1.blk"), stat(100, 1), false);
        track(&index, Path::new("/data/b/e/sub/2.blk"), stat(50, 1), false);
        track(&index, Path::new("/data/other/1.blk"), stat(7, 1), false);
        index.mark_dirty(Path::new("/data/b/e/1.blk"), Kind::Data);

        index.remove_subtree(Path::new("/data/b"));

        assert_eq!(index.counters.local_bytes.load(Ordering::Relaxed), 7);
        assert_eq!(index.dirty_len(), 0);
    }

    #[rstest]
    fn reconcile_adds_changed_and_drops_missing_files() {
        let index = Index::default();
        let dir = Path::new("/data/b/e");
        track(&index, &dir.join("gone.blk"), stat(5, 1), true);
        track(&index, &dir.join("same.blk"), stat(10, 1), true);
        track(&index, &dir.join("changed.blk"), stat(10, 1), true);
        let seq = index.current_seq();

        index.reconcile_dir(
            dir,
            vec![
                ("same.blk".to_string(), Kind::Data, stat(10, 1)),
                ("changed.blk".to_string(), Kind::Data, stat(12, 2)),
                ("new.blk".to_string(), Kind::Data, stat(20, 3)),
            ],
            vec![],
            seq,
        );

        assert!(index.get(&dir.join("gone.blk")).is_none());
        assert!(!index.get(&dir.join("same.blk")).unwrap().in_dirty);
        assert!(index.get(&dir.join("changed.blk")).unwrap().in_dirty);
        assert!(index.get(&dir.join("new.blk")).unwrap().in_dirty);
        assert_eq!(index.counters.local_bytes.load(Ordering::Relaxed), 42);
        assert_eq!(index.pending_stats().files, 2);
        assert_eq!(index.pending_stats().bytes, 32);
    }

    #[rstest]
    fn reconcile_keeps_entries_touched_after_listing() {
        let index = Index::default();
        let dir = Path::new("/data/b/e");
        let seq = index.current_seq();
        index.mark_dirty(&dir.join("late.blk"), Kind::Data);

        index.reconcile_dir(dir, vec![], vec![], seq);

        assert!(index.get(&dir.join("late.blk")).is_some());
    }
}
