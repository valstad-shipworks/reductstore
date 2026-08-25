// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use async_trait::async_trait;
use std::collections::{BTreeSet, HashMap};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::mem;
use std::path::{Path, PathBuf};

use crc64fast::Digest;
use log::warn;
use prost::Message;

use reduct_base::error::ReductError;
use reduct_base::internal_server_error;

use crate::backend::file::File;
use crate::core::file_cache::FILE_CACHE;
use crate::storage::proto::Record;
use byteorder::{BigEndian, ReadBytesExt};

const WAL_FILE_SIZE: u64 = 1_000_000;
const WAL_DIR: &str = ".wal";
const LEGACY_WAL_DIR: &str = "wal";

#[derive(PartialEq, Debug)]
pub(in crate::storage) enum WalEntry {
    WriteRecord(Record),
    UpdateRecord(Record),
    RemoveBlock,
    RemoveRecord(u64),
}

impl WalEntry {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            WalEntry::WriteRecord(record) => {
                let mut buf = Vec::new();
                buf.push(0);

                let record = record.encode_to_vec();
                buf.extend_from_slice(&(record.len() as u64).to_be_bytes());
                buf.extend_from_slice(&record);
                buf
            }
            WalEntry::UpdateRecord(record) => {
                let mut buf = Vec::new();
                buf.push(1);

                let record = record.encode_to_vec();
                buf.extend_from_slice(&(record.len() as u64).to_be_bytes());
                buf.extend_from_slice(&record);
                buf
            }
            WalEntry::RemoveBlock => {
                let mut buf = vec![2];
                buf.extend_from_slice(&0u64.to_be_bytes());
                buf
            }
            WalEntry::RemoveRecord(record_id) => {
                let mut buf = vec![3];
                buf.extend_from_slice(&(mem::size_of_val(record_id) as u64).to_be_bytes());
                buf.extend_from_slice(&record_id.to_be_bytes());
                buf
            }
        }
    }

    pub fn decode(type_code: u8, buf: &[u8]) -> Result<Self, ReductError> {
        match type_code {
            0 => {
                let record = Record::decode(buf).unwrap();
                Ok(WalEntry::WriteRecord(record))
            }
            1 => {
                let record = Record::decode(buf).unwrap();
                Ok(WalEntry::UpdateRecord(record))
            }
            2 => Ok(WalEntry::RemoveBlock),
            3 => {
                let record_id = u64::from_be_bytes(buf.try_into().unwrap());
                Ok(WalEntry::RemoveRecord(record_id))
            }
            _ => Err(internal_server_error!("Invalid WAL entry")),
        }
    }
}

/// Manage WAL logs per block
///
/// The WAL is used to store changes to blocks that have not been written to the block file yet.
///
/// Format in big-endian:
///
/// | Entry Type (8) | Entry Length (64) | Entry Data (variable) | CRC (64) | Stop Marker (8) |
///
#[async_trait]
pub(in crate::storage) trait Wal {
    /// Append a WAL entry to the WAL file
    ///
    /// # Arguments
    ///
    /// * `block_id` - The block id to append the entry to
    /// * `entry` - The WAL entry to append
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the entry was successfully appended
    async fn append(&mut self, block_id: u64, entry: WalEntry) -> Result<(), ReductError>;

    /// Read all WAL entries for a block
    ///
    /// # Arguments
    ///
    /// * `block_id` - The block id to read the entries for
    ///
    /// # Returns
    ///
    /// * A vector of WAL entries
    async fn read(&self, block_id: u64) -> Result<Vec<WalEntry>, ReductError>;

    /// Remove the WAL file for a block
    async fn remove(&mut self, block_id: u64) -> Result<(), ReductError>;

    /// List all WALs
    async fn list(&self) -> Result<Vec<u64>, ReductError>;
}

struct WalImpl {
    root_path: PathBuf,
    file_positions: HashMap<u64, u64>,
    known_blocks: BTreeSet<u64>,
}

impl WalImpl {
    pub async fn try_build(path_buf: PathBuf) -> Result<Self, ReductError> {
        let mut wal = WalImpl {
            root_path: path_buf,
            file_positions: HashMap::new(), // we need to keep track of the file positions for each block because of file cache
            known_blocks: BTreeSet::new(),
        };

        let mut blocks = BTreeSet::new();

        // WAL files can be dirty only in the local cache. Remote backends list
        // objects from remote storage, so check the local WAL directory first to
        // make strict sync/compaction see WALs before FILE_CACHE uploads them.
        let local_entries = match tokio::fs::read_dir(&wal.root_path).await {
            Ok(entries) => Some(entries),
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        };

        if let Some(mut entries) = local_entries {
            while let Some(entry) = entries.next_entry().await? {
                if let Some(block_id) = Self::parse_wal_block_id(&entry.path()) {
                    blocks.insert(block_id);
                }
            }
        }

        for path in FILE_CACHE.read_dir(&wal.root_path).await? {
            if let Some(block_id) = Self::parse_wal_block_id(&path) {
                blocks.insert(block_id);
            }
        }

        wal.known_blocks = blocks;
        Ok(wal)
    }

    fn block_wal_path(&self, block_id: u64) -> PathBuf {
        self.root_path.join(format!("{}.wal", block_id))
    }

    fn parse_wal_block_id(path: &Path) -> Option<u64> {
        if path.extension()? != "wal" {
            return None;
        }

        path.file_stem()?.to_str()?.parse::<u64>().ok()
    }
}

const STOP_MARKER: u8 = 255;

#[async_trait]
impl Wal for WalImpl {
    async fn append(&mut self, block_id: u64, entry: WalEntry) -> Result<(), ReductError> {
        let path = self.block_wal_path(block_id);
        let (mut file, start) = match self.file_positions.get(&block_id) {
            Some(&pos) => {
                let file = FILE_CACHE
                    .write_or_create(&path, SeekFrom::Current(0))
                    .await?;
                (file, pos.saturating_sub(1))
            }
            None => {
                let exists = FILE_CACHE.try_exists(&path).await?;
                let mut file = FILE_CACHE
                    .write_or_create(&path, SeekFrom::Current(0))
                    .await?;
                if exists {
                    warn!(
                        "File position for block {} not found. Overwrite WAL",
                        block_id
                    );
                } else {
                    file.set_len(WAL_FILE_SIZE)?;
                }
                (file, 0)
            }
        };

        let mut buf = entry.encode();
        let mut crc = Digest::new();
        crc.write(&buf);
        buf.extend_from_slice(&crc.sum64().to_be_bytes());
        buf.push(STOP_MARKER);
        File::run_blocking_io(|| {
            file.seek(SeekFrom::Start(start))?;
            file.write_all(&buf)
        })?;

        self.file_positions
            .insert(block_id, start + buf.len() as u64);
        self.known_blocks.insert(block_id);
        Ok(())
    }

    async fn read(&self, block_id: u64) -> Result<Vec<WalEntry>, ReductError> {
        let path = self.block_wal_path(block_id);
        let mut file = FILE_CACHE.read(&path, SeekFrom::Start(0)).await?;

        let mut entries = Vec::new();
        loop {
            let entry_type = match file.read_u8() {
                Ok(t) => t,
                Err(err) => return Err(err.into()),
            };

            if entry_type == STOP_MARKER {
                break;
            }

            let mut crc = Digest::new();
            crc.write(&[entry_type]);

            let len = file.read_u64::<BigEndian>()?;
            crc.write(&len.to_be_bytes());

            let mut buf = vec![0; len as usize];
            file.read_exact(&mut buf)?;
            crc.write(&buf);

            let crc_bytes = file.read_u64::<BigEndian>()?;

            if crc.sum64() != crc_bytes {
                return Err(internal_server_error!("WAL {:?} is corrupted", path));
            }

            let entry = WalEntry::decode(entry_type, &buf)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn remove(&mut self, block_id: u64) -> Result<(), ReductError> {
        if self.known_blocks.contains(&block_id) {
            let path = self.block_wal_path(block_id);
            if FILE_CACHE.try_exists(&path).await? {
                FILE_CACHE.remove(&path).await?;
            }
        }
        self.known_blocks.remove(&block_id);
        self.file_positions.remove(&block_id);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<u64>, ReductError> {
        Ok(self.known_blocks.iter().copied().collect())
    }
}

/// Creates a new Write-Ahead Log (WAL) instance.
///
/// This function initializes a WAL directory at the specified path if it does not already exist,
/// and returns a boxed instance of `Wal` that can be used to manage WAL entries.
///
/// # Arguments
///
/// * `entry_path` - The path where the WAL directory should be created.
///
/// # Returns
///
/// A boxed instance of `Wal` that implements `Send` and `Sync`.
///
pub(in crate::storage) async fn create_wal(
    entry_path: PathBuf,
) -> Result<Box<dyn Wal + Send + Sync>, ReductError> {
    let wal_folder = entry_path.join(WAL_DIR);
    let legacy_wal_folder = entry_path.join(LEGACY_WAL_DIR);

    if !wal_folder.try_exists()? && legacy_wal_folder.try_exists()? {
        if let Err(err) = FILE_CACHE.rename(&legacy_wal_folder, &wal_folder).await {
            warn!(
                "Failed to migrate legacy WAL folder {:?} to {:?}: {}",
                legacy_wal_folder, wal_folder, err
            );
        }
    }

    if !wal_folder.try_exists()? {
        FILE_CACHE.create_dir_all(&wal_folder).await?;
    }

    Ok(Box::new(
        WalImpl::try_build(entry_path.join(WAL_DIR)).await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sync::{reset_rwlock_config, set_rwlock_timeout};
    use reduct_base::error::ErrorCode;
    use rstest::*;
    use serial_test::serial;
    use std::fs::OpenOptions;
    use std::time::Duration;

    #[rstest]
    #[tokio::test]
    async fn test_read(#[future] wal: WalImpl) {
        let mut wal = wal.await;
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();
        wal.append(1, WalEntry::UpdateRecord(Record::default()))
            .await
            .unwrap();
        wal.append(1, WalEntry::RemoveBlock).await.unwrap();
        wal.append(1, WalEntry::RemoveRecord(1)).await.unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf())
            .await
            .unwrap();
        let entries = wal.read(1).await.unwrap();

        assert_eq!(
            entries,
            vec![
                WalEntry::WriteRecord(Record::default()),
                WalEntry::UpdateRecord(Record::default()),
                WalEntry::RemoveBlock,
                WalEntry::RemoveRecord(1)
            ]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove(#[future] wal: WalImpl) {
        let mut wal = wal.await;
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        assert_eq!(wal.read(1).await.unwrap().len(), 1);
        wal.remove(1).await.unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf())
            .await
            .unwrap();
        let err = wal.read(1).await.err().unwrap();
        assert_eq!(&err.status, &ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    async fn test_list(#[future] wal: WalImpl) {
        let mut wal = wal.await;
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();
        wal.append(2, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf())
            .await
            .unwrap();
        let blocks = wal.list().await.unwrap();
        assert_eq!(blocks.len(), 2);
        assert!(blocks.contains(&1));
        assert!(blocks.contains(&2));
    }

    #[test]
    fn test_parse_wal_block_id() {
        assert_eq!(
            WalImpl::parse_wal_block_id(&PathBuf::from("42.wal")),
            Some(42)
        );
        assert_eq!(WalImpl::parse_wal_block_id(&PathBuf::from("42.tmp")), None);
        assert_eq!(WalImpl::parse_wal_block_id(&PathBuf::from("bad.wal")), None);
    }

    #[rstest]
    #[tokio::test]
    async fn test_crc_error(#[future] wal: WalImpl) {
        let mut wal = wal.await;
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        let path = wal.block_wal_path(1);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 1]).unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf())
            .await
            .unwrap();
        let err = wal.read(1).await.err().unwrap();
        assert_eq!(&err.status, &ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    async fn cache_invalidation(#[future] wal: WalImpl) {
        let mut wal = wal.await;
        wal.append(1, WalEntry::UpdateRecord(Record::default()))
            .await
            .unwrap();
        FILE_CACHE
            .discard_recursive(&wal.root_path.join("1.wal"))
            .await
            .unwrap();
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        let entries = wal.read(1).await.unwrap();
        assert_eq!(
            entries,
            vec![
                WalEntry::UpdateRecord(Record::default()),
                WalEntry::WriteRecord(Record::default())
            ],
            "We keep entry after cache invalidation"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_migrate_legacy_wal_dir() {
        let path = tempfile::tempdir().unwrap().keep();
        let entry_path = path.join("entry");
        std::fs::create_dir_all(entry_path.join(LEGACY_WAL_DIR)).unwrap();
        std::fs::write(entry_path.join(LEGACY_WAL_DIR).join("1.wal"), [STOP_MARKER]).unwrap();

        let wal = create_wal(entry_path.clone()).await.unwrap();

        assert!(entry_path.join(WAL_DIR).exists());
        assert!(!entry_path.join(LEGACY_WAL_DIR).exists());
        assert!(wal.read(1).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_try_build_returns_error_if_local_wal_path_is_not_directory() {
        let path = tempfile::tempdir().unwrap().keep();
        let wal_path = path.join(WAL_DIR);
        std::fs::write(&wal_path, b"not a directory").unwrap();

        let err = WalImpl::try_build(wal_path).await.err().unwrap();
        assert_eq!(err.status, ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    async fn test_try_build_missing_local_wal_dir_propagates_backend_error() {
        let path = tempfile::tempdir().unwrap().keep();
        let wal_path = path.join(WAL_DIR);

        let err = WalImpl::try_build(wal_path).await.err().unwrap();
        assert_eq!(err.status, ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    async fn test_position_tracks_stream_after_append(#[future] wal: WalImpl) {
        let mut wal = wal.await;
        for _ in 0..3 {
            wal.append(1, WalEntry::WriteRecord(Record::default()))
                .await
                .unwrap();
            assert_eq!(wal.file_positions[&1], stream_position(&wal, 1).await);
        }
        assert_eq!(wal.read(1).await.unwrap().len(), 3);
    }

    #[rstest]
    #[tokio::test]
    async fn test_append_after_remove_starts_fresh(#[future] wal: WalImpl) {
        let mut wal = wal.await;
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();
        wal.append(1, WalEntry::RemoveRecord(7)).await.unwrap();
        wal.remove(1).await.unwrap();
        assert!(!wal.file_positions.contains_key(&1));

        wal.append(1, WalEntry::UpdateRecord(Record::default()))
            .await
            .unwrap();
        assert_eq!(wal.file_positions[&1], stream_position(&wal, 1).await);
        assert_eq!(
            wal.read(1).await.unwrap(),
            vec![WalEntry::UpdateRecord(Record::default())]
        );
        assert_eq!(wal.list().await.unwrap(), vec![1]);
    }

    #[rstest]
    #[tokio::test]
    async fn test_append_to_listed_block_after_restart(#[future] wal: WalImpl) {
        let mut wal = wal.await;
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        let mut wal = WalImpl::try_build(wal.root_path.clone()).await.unwrap();
        assert_eq!(wal.list().await.unwrap(), vec![1]);
        assert!(wal.file_positions.is_empty());

        wal.append(1, WalEntry::RemoveBlock).await.unwrap();
        assert_eq!(wal.file_positions[&1], stream_position(&wal, 1).await);
        assert_eq!(wal.read(1).await.unwrap(), vec![WalEntry::RemoveBlock]);
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_remove_retries_after_failed_file_removal(#[future] wal: WalImpl) {
        let mut wal = wal.await;
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();
        let position = stream_position(&wal, 1).await;

        let guard = FILE_CACHE
            .write_or_create(&wal.block_wal_path(1), SeekFrom::Current(0))
            .await
            .unwrap();
        set_rwlock_timeout(Duration::from_millis(50));
        let err = wal.remove(1).await.err().unwrap();
        reset_rwlock_config();
        assert_eq!(err.status, ErrorCode::InternalServerError);
        assert_eq!(wal.list().await.unwrap(), vec![1]);
        assert_eq!(wal.file_positions.get(&1), Some(&position));
        drop(guard);

        wal.remove(1).await.unwrap();
        assert!(wal.list().await.unwrap().is_empty());
        assert!(!wal.block_wal_path(1).exists());
    }

    async fn stream_position(wal: &WalImpl, block_id: u64) -> u64 {
        FILE_CACHE
            .write_or_create(&wal.block_wal_path(block_id), SeekFrom::Current(0))
            .await
            .unwrap()
            .stream_position()
            .unwrap()
    }

    #[fixture]
    async fn wal() -> WalImpl {
        let path = tempfile::tempdir().unwrap().keep();
        std::fs::create_dir_all(path.join(WAL_DIR)).unwrap();
        WalImpl::try_build(path.join(WAL_DIR)).await.unwrap()
    }
}
