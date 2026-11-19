// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

pub(super) const STUB_EXT: &str = ".archived";
pub(super) const STUB_TMP_EXT: &str = ".archived.tmp";
pub(super) const RECALL_TMP_EXT: &str = ".recall.tmp";

/// Placeholder left in the data folder for a data block that lives only in the archive.
///
/// The stub is what makes an evicted block visible to the storage engine without
/// asking the archive, so listing and existence checks keep working while the
/// archive is unreachable.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(super) struct Stub {
    pub len: u64,
    pub mtime_ns: u64,
    #[serde(default)]
    pub crc64: Option<u64>,
    #[serde(default)]
    pub archived_at: u64,
}

impl Stub {
    pub fn read(path: &Path) -> io::Result<Self> {
        let content = std::fs::read(path)?;
        serde_json::from_slice(&content).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid archive stub {:?}: {}", path, err),
            )
        })
    }

    pub fn write(&self, path: &Path) -> io::Result<()> {
        let content = serde_json::to_vec(self).map_err(io::Error::other)?;
        let mut file = File::create(path)?;
        file.write_all(&content)?;
        file.sync_all()
    }
}

pub(super) fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

pub(super) fn stub_path(path: &Path) -> PathBuf {
    with_suffix(path, STUB_EXT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use tempfile::tempdir;

    #[rstest]
    fn stub_round_trips() {
        let dir = tempdir().unwrap();
        let path = stub_path(&dir.path().join("1.blk"));
        assert_eq!(path, dir.path().join("1.blk.archived"));

        let stub = Stub {
            len: 10,
            mtime_ns: 20,
            crc64: Some(30),
            archived_at: 40,
        };
        stub.write(&path).unwrap();
        assert_eq!(Stub::read(&path).unwrap(), stub);
    }

    #[rstest]
    fn stub_rejects_garbage() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.blk.archived");
        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(
            Stub::read(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
