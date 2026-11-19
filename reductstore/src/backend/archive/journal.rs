// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::now_ms;
use log::warn;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

const COMPACT_AFTER_APPLIED: usize = 1024;

/// A change of the data folder that still has to be repeated on the archive.
///
/// Paths are relative to the data folder.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "op", rename_all = "lowercase")]
pub(super) enum Op {
    Rm { path: PathBuf, t: u64 },
    Rmdir { path: PathBuf, t: u64 },
    Mv { from: PathBuf, to: PathBuf, t: u64 },
}

impl Op {
    pub fn rm(path: PathBuf) -> Self {
        Op::Rm { path, t: now_ms() }
    }

    pub fn rmdir(path: PathBuf) -> Self {
        Op::Rmdir { path, t: now_ms() }
    }

    pub fn mv(from: PathBuf, to: PathBuf) -> Self {
        Op::Mv {
            from,
            to,
            t: now_ms(),
        }
    }
}

struct State {
    ops: Vec<Op>,
    file: Option<File>,
    applied_in_file: usize,
}

/// Removals and renames waiting to be applied to the archive.
///
/// Copies need no journal because a missing copy is found again by comparing the
/// data folder with the archive. A removal leaves nothing behind to compare, so it
/// is written down, which is also what carries it across an archive outage or a
/// restart. Replaying an operation twice is harmless.
pub(super) struct Journal {
    path: PathBuf,
    state: Mutex<State>,
}

impl Journal {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let ops = match std::fs::read_to_string(&path) {
            Ok(content) => content
                .lines()
                .filter_map(|line| serde_json::from_str::<Op>(line).ok())
                .collect(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };

        let journal = Self {
            path,
            state: Mutex::new(State {
                ops,
                file: None,
                applied_in_file: 0,
            }),
        };
        journal.rewrite(&mut journal.state.lock())?;
        Ok(journal)
    }

    pub fn len(&self) -> usize {
        self.state.lock().ops.len()
    }

    pub fn snapshot(&self) -> Vec<Op> {
        self.state.lock().ops.clone()
    }

    /// Append an operation. `durable` waits for it to reach the disk, which is
    /// needed for operations whose loss would leave archived data unreachable.
    pub fn push(&self, op: Op, durable: bool) {
        let mut state = self.state.lock();
        if let Err(err) = Self::append(&self.path, &mut state, &op, durable) {
            warn!("Failed to write archive journal {:?}: {}", self.path, err);
        }
        state.ops.push(op);
    }

    /// Drop the operations of a snapshot that were applied.
    ///
    /// `applied[i]` tells whether the i-th operation of the snapshot is done.
    /// Operations pushed after the snapshot was taken are kept.
    pub fn complete(&self, applied: &[bool]) {
        let mut state = self.state.lock();
        let mut index = 0;
        let mut done = 0;
        state.ops.retain(|_| {
            let keep = !applied.get(index).copied().unwrap_or(false);
            index += 1;
            if !keep {
                done += 1;
            }
            keep
        });

        state.applied_in_file += done;
        if state.applied_in_file > 0
            && (state.ops.is_empty() || state.applied_in_file >= COMPACT_AFTER_APPLIED)
        {
            if let Err(err) = self.rewrite(&mut state) {
                warn!("Failed to compact archive journal {:?}: {}", self.path, err);
            }
        }
    }

    fn append(path: &Path, state: &mut State, op: &Op, durable: bool) -> io::Result<()> {
        if state.file.is_none() {
            state.file = Some(OpenOptions::new().create(true).append(true).open(path)?);
        }
        let file = state.file.as_mut().unwrap();
        let mut line = serde_json::to_vec(op).map_err(io::Error::other)?;
        line.push(b'\n');
        file.write_all(&line)?;
        if durable {
            file.sync_data()?;
        }
        Ok(())
    }

    fn rewrite(&self, state: &mut State) -> io::Result<()> {
        state.file = None;
        let tmp = self.path.with_extension("tmp");
        let mut file = File::create(&tmp)?;
        for op in state.ops.iter() {
            let mut line = serde_json::to_vec(op).map_err(io::Error::other)?;
            line.push(b'\n');
            file.write_all(&line)?;
        }
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, &self.path)?;
        state.applied_in_file = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use tempfile::tempdir;

    #[rstest]
    fn journal_survives_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        let journal = Journal::open(path.clone()).unwrap();
        journal.push(Op::rm(PathBuf::from("b/e/1.blk")), false);
        journal.push(Op::mv(PathBuf::from("b/old"), PathBuf::from("b/new")), true);
        journal.push(Op::rmdir(PathBuf::from("b/e")), true);
        let ops = journal.snapshot();
        drop(journal);

        let journal = Journal::open(path).unwrap();
        assert_eq!(journal.snapshot(), ops);
        assert_eq!(journal.len(), 3);
    }

    #[rstest]
    fn complete_drops_applied_operations_only() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        let journal = Journal::open(path.clone()).unwrap();
        journal.push(Op::rm(PathBuf::from("a")), false);
        journal.push(Op::rm(PathBuf::from("b")), false);
        let snapshot = journal.snapshot();
        journal.push(Op::rm(PathBuf::from("c")), false);

        journal.complete(&[true, false]);

        let left = journal.snapshot();
        assert_eq!(left.len(), 2);
        assert_eq!(left[0], snapshot[1]);
        assert!(matches!(&left[1], Op::Rm { path, .. } if path == Path::new("c")));

        journal.complete(&[true, true]);
        assert_eq!(journal.len(), 0);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
    }

    #[rstest]
    fn journal_ignores_torn_last_line() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        std::fs::write(
            &path,
            "{\"op\":\"rm\",\"path\":\"a\",\"t\":1}\n{\"op\":\"rm\",\"pa",
        )
        .unwrap();

        let journal = Journal::open(path).unwrap();
        assert_eq!(
            journal.snapshot(),
            vec![Op::Rm {
                path: PathBuf::from("a"),
                t: 1
            }]
        );
    }
}
