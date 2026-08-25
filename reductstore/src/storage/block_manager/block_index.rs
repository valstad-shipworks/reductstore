// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use bytes::Bytes;
use crc64fast::Digest;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::{BTreeSet, HashMap};
use std::io::{Read, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::backend::file::File;
use crate::core::file_cache::FILE_CACHE;
use crate::storage::block_manager::block::Block;
use crate::storage::block_manager::{COMPRESSED_DESCRIPTOR_FILE_EXT, DESCRIPTOR_FILE_EXT};
use crate::storage::proto::block_index::Block as BlockEntry;
use crate::storage::proto::{
    ts_to_us, us_to_ts, Block as BlockProto, BlockIndex as BlockIndexProto, MinimalBlock,
};

#[derive(Debug)]
pub(in crate::storage) struct BlockIndex {
    path_buf: PathBuf,
    index_info: HashMap<u64, BlockEntry>,
    index: BTreeSet<u64>,
    total_size: Arc<AtomicU64>,
}

impl Into<BlockEntry> for MinimalBlock {
    fn into(self) -> BlockEntry {
        BlockEntry {
            block_id: ts_to_us(&self.begin_time.unwrap()),
            size: self.size,
            record_count: self.record_count,
            metadata_size: self.metadata_size,
            latest_record_time: self.latest_record_time,
            crc64: None,
            compression: None,
            corrupted: self.corrupted,
            version: self.version,
        }
    }
}

impl Into<BlockEntry> for BlockProto {
    fn into(self) -> BlockEntry {
        BlockEntry {
            block_id: ts_to_us(&self.begin_time.unwrap()),
            size: self.size,
            record_count: self.record_count,
            metadata_size: self.metadata_size,
            latest_record_time: self.latest_record_time,
            crc64: None,
            compression: None,
            corrupted: self.corrupted,
            version: self.version,
        }
    }
}

impl From<&Block> for BlockEntry {
    fn from(block: &Block) -> BlockEntry {
        BlockEntry {
            block_id: block.block_id(),
            size: block.size(),
            record_count: block.record_count(),
            metadata_size: block.metadata_size(),
            latest_record_time: Some(us_to_ts(&block.latest_record_time())),
            crc64: None,
            compression: None,
            corrupted: None,
            version: None,
        }
    }
}

impl From<Block> for BlockEntry {
    fn from(block: Block) -> BlockEntry {
        BlockEntry::from(&block)
    }
}

impl BlockIndex {
    pub fn new(path_buf: PathBuf) -> Self {
        BlockIndex {
            path_buf,
            index_info: HashMap::new(),
            index: BTreeSet::new(),
            total_size: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Insert  or update a new block entry into the index.
    ///
    /// # Arguments
    ///
    /// * `entry` - The block entry to insert.
    ///
    pub fn insert_or_update<T>(&mut self, entry: T)
    where
        T: Into<BlockEntry>,
    {
        self.insert(entry.into());
    }

    /// Insert or update a new block entry into the index with CRC.
    ///
    /// Must be used when the block is written to disk.
    ///
    /// # Arguments
    ///
    /// * `entry` - The block entry to insert.
    /// * `crc` - The CRC value.
    ///
    pub fn insert_or_update_with_crc<T>(&mut self, entry: T, crc: u64)
    where
        T: Into<BlockEntry>,
    {
        let mut block = entry.into();
        block.crc64 = Some(crc);
        self.insert(block);
    }

    pub fn get_block(&self, block_id: u64) -> Option<&BlockEntry> {
        self.index_info.get(&block_id)
    }

    /// Apply an in-place change to an indexed block entry.
    ///
    /// Returns `false` when the block is not in the index.
    pub fn update_block(&mut self, block_id: u64, update: impl FnOnce(&mut BlockEntry)) -> bool {
        let Some(block) = self.index_info.get_mut(&block_id) else {
            return false;
        };

        let before = block.size + block.metadata_size;
        update(block);
        let after = block.size + block.metadata_size;
        self.adjust_total_size(before, after);
        true
    }

    pub fn remove_block(&mut self, block_id: u64) -> Option<BlockEntry> {
        let block = self.index_info.remove(&block_id);
        self.index.remove(&block_id);

        if let Some(block) = &block {
            self.adjust_total_size(block.size + block.metadata_size, 0);
        }

        block
    }

    pub fn is_corrupted(&self, block_id: u64) -> bool {
        self.index_info
            .get(&block_id)
            .is_some_and(|block| block.corrupted == Some(true))
    }

    pub fn mark_corrupted(&mut self, block_id: u64) {
        if let Some(block) = self.index_info.get_mut(&block_id) {
            block.corrupted = Some(true);
        }
    }

    pub fn corrupted_block_count(&self) -> usize {
        self.corrupted_block_ids().len()
    }

    pub fn corrupted_block_ids(&self) -> Vec<u64> {
        self.index
            .iter()
            .copied()
            .filter(|block_id| self.is_corrupted(*block_id))
            .collect()
    }

    pub fn first_active_block_id(&self) -> Option<u64> {
        self.index
            .iter()
            .copied()
            .find(|block_id| !self.is_corrupted(*block_id))
    }

    pub fn last_active_block_id(&self) -> Option<u64> {
        self.index
            .iter()
            .rev()
            .copied()
            .find(|block_id| !self.is_corrupted(*block_id))
    }

    /// The last active block that starts at or before `time`.
    pub fn active_block_id_at(&self, time: u64) -> Option<u64> {
        self.index
            .range(..=time)
            .rev()
            .copied()
            .find(|block_id| !self.is_corrupted(*block_id))
    }

    pub fn active_tree(&self) -> BTreeSet<u64> {
        self.index
            .iter()
            .copied()
            .filter(|block_id| !self.is_corrupted(*block_id))
            .collect()
    }

    pub async fn try_load(path: PathBuf) -> Result<Self, ReductError> {
        if !FILE_CACHE.try_exists(&path).await? {
            return Err(internal_server_error!("Block index {:?} not found", path));
        }

        let mut lock = FILE_CACHE.read(&path, SeekFrom::Start(0)).await?;
        let mut buf = Vec::new();
        if let Err(err) = lock.read_to_end(&mut buf) {
            return Err(internal_server_error!(
                "Failed to read block index {:?}: {}",
                path,
                err
            ));
        };

        if lock.metadata()?.len() == 0 {
            // If the index file is empty, check if there are any block descriptors.
            // If there are, the index file is corrupted.
            let has_block_descriptors = FILE_CACHE
                .read_dir(&path.parent().unwrap().into())
                .await?
                .iter()
                .any(|path| {
                    path.ends_with(DESCRIPTOR_FILE_EXT)
                        || path.ends_with(COMPRESSED_DESCRIPTOR_FILE_EXT)
                });

            if has_block_descriptors {
                return Err(internal_server_error!("Block index {:?} is empty", path));
            }
        }

        let block_index_proto = BlockIndexProto::decode(Bytes::from(buf)).map_err(|err| {
            internal_server_error!("Failed to decode block index {:?}: {}", path, err)
        })?;

        let block_index: BlockIndex = BlockIndex::from_proto(path, block_index_proto)?;
        Ok(block_index)
    }

    pub fn from_proto(path: PathBuf, value: BlockIndexProto) -> Result<Self, ReductError> {
        let mut block_index = BlockIndex::new(path.clone());

        let mut crc = Digest::new();
        value.blocks.into_iter().for_each(|block| {
            let latest_record_time = ts_to_us(block.latest_record_time.as_ref().unwrap());

            crc.write(&block.block_id.to_be_bytes());
            crc.write(&block.size.to_be_bytes());
            crc.write(&block.record_count.to_be_bytes());
            crc.write(&block.metadata_size.to_be_bytes());
            crc.write(&latest_record_time.to_be_bytes());

            if let Some(crc64) = block.crc64 {
                crc.write(&crc64.to_be_bytes());
            }

            if let Some(corrupted) = block.corrupted {
                crc.write(&(corrupted as u8).to_be_bytes());
            }

            if let Some(version) = block.version {
                crc.write(&version.to_be_bytes());
            }

            block_index.insert(block);
        });

        if crc.sum64() != value.crc64 {
            return Err(internal_server_error!(
                "Block index {:?} is corrupted",
                path
            ));
        }

        Ok(block_index)
    }

    pub async fn save(&self) -> Result<(), ReductError> {
        let mut block_index_proto = BlockIndexProto {
            blocks: Vec::new(),
            crc64: 0,
        };

        block_index_proto.blocks = self
            .index_info
            .values()
            .map(|block| {
                let mut block_entry = BlockEntry::default();
                block_entry.block_id = block.block_id;
                block_entry.size = block.size;
                block_entry.record_count = block.record_count;
                block_entry.metadata_size = block.metadata_size;
                block_entry.latest_record_time = block.latest_record_time;
                block_entry.crc64 = block.crc64;
                block_entry.compression = block.compression;
                block_entry.corrupted = block.corrupted;
                block_entry.version = block.version;
                block_entry
            })
            .collect();

        let mut crc = Digest::new();
        block_index_proto.blocks.iter().for_each(|block| {
            crc.write(&block.block_id.to_be_bytes());
            crc.write(&block.size.to_be_bytes());
            crc.write(&block.record_count.to_be_bytes());
            crc.write(&block.metadata_size.to_be_bytes());
            crc.write(&ts_to_us(&block.latest_record_time.unwrap()).to_be_bytes());

            if let Some(crc64) = block.crc64 {
                crc.write(&crc64.to_be_bytes());
            }

            if let Some(corrupted) = block.corrupted {
                crc.write(&(corrupted as u8).to_be_bytes());
            }

            if let Some(version) = block.version {
                crc.write(&version.to_be_bytes());
            }
        });

        block_index_proto.crc64 = crc.sum64();
        let buf = block_index_proto.encode_to_vec();

        let mut lock = FILE_CACHE
            .write_or_create(&self.path_buf, SeekFrom::Start(0))
            .await?;
        File::run_blocking_io(|| {
            lock.set_len(0)?;
            lock.write_all(&buf)
        })
        .map_err(|err| {
            internal_server_error!("Failed to write block index {:?}: {}", self.path_buf, err)
        })?;

        lock.flush_local().await?;

        Ok(())
    }

    pub fn size(&self) -> u64 {
        let size = self.total_size.load(Ordering::Acquire);
        debug_assert_eq!(size, self.fold_size());
        size
    }

    /// Counter mirroring [`BlockIndex::size`], readable without locking the index.
    pub fn size_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.total_size)
    }

    /// Publish this index's size through `counter` from now on.
    pub fn adopt_size_counter(&mut self, counter: Arc<AtomicU64>) {
        counter.store(self.fold_size(), Ordering::Release);
        self.total_size = counter;
    }

    pub fn record_count(&self) -> u64 {
        self.index_info
            .iter()
            .fold(0, |acc, (_, block)| acc + block.record_count)
    }

    pub fn tree(&self) -> &BTreeSet<u64> {
        &self.index
    }

    pub fn info(&self) -> &HashMap<u64, BlockEntry> {
        &self.index_info
    }

    pub async fn sync_all(&mut self) -> Result<(), ReductError> {
        let mut lock = FILE_CACHE
            .write_or_create(&self.path_buf, SeekFrom::Start(0))
            .await?;
        lock.sync_all().await?;
        Ok(())
    }

    fn insert(&mut self, mut block: BlockEntry) {
        let block_id = block.block_id;
        let added = block.size + block.metadata_size;
        if block.version.is_none() {
            block.version = self.index_info.get(&block_id).and_then(|prev| prev.version);
        }
        let removed = self
            .index_info
            .insert(block_id, block)
            .map_or(0, |previous| previous.size + previous.metadata_size);
        self.index.insert(block_id);
        self.adjust_total_size(removed, added);
    }

    fn adjust_total_size(&self, before: u64, after: u64) {
        if after >= before {
            self.total_size.fetch_add(after - before, Ordering::AcqRel);
        } else {
            self.total_size.fetch_sub(before - after, Ordering::AcqRel);
        }
    }

    fn fold_size(&self) -> u64 {
        self.index_info
            .values()
            .fold(0, |acc, block| acc + block.size + block.metadata_size)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::storage::block_manager::BLOCK_INDEX_FILE;
    use crate::storage::proto::block_index::Block as BlockEntry;
    use prost_wkt_types::Timestamp;
    use rstest::rstest;
    use tempfile::tempdir;

    use super::*;

    mod size_counter {
        use super::*;

        fn entry(block_id: u64, size: u64, metadata_size: u64) -> BlockEntry {
            BlockEntry {
                block_id,
                size,
                record_count: 1,
                metadata_size,
                latest_record_time: Some(Timestamp::default()),
                crc64: None,
                compression: None,
                corrupted: None,
                version: None,
            }
        }

        fn assert_counter_matches_fold(index: &BlockIndex) {
            assert_eq!(
                index.size_counter().load(Ordering::SeqCst),
                index.fold_size()
            );
            assert_eq!(index.size(), index.fold_size());
        }

        #[rstest]
        fn tracks_insert_update_remove() {
            let mut index = BlockIndex::new(PathBuf::from("unused"));
            let counter = index.size_counter();
            assert_eq!(counter.load(Ordering::SeqCst), 0);

            index.insert_or_update(entry(1, 10, 2));
            index.insert_or_update_with_crc(entry(2, 5, 1), 7);
            assert_eq!(counter.load(Ordering::SeqCst), 18);
            assert_counter_matches_fold(&index);

            index.insert_or_update(entry(1, 4, 2));
            assert_eq!(counter.load(Ordering::SeqCst), 12);
            assert_counter_matches_fold(&index);

            assert!(index.update_block(2, |block| {
                block.size = 50;
                block.metadata_size = 0;
            }));
            assert_eq!(counter.load(Ordering::SeqCst), 56);
            assert_counter_matches_fold(&index);

            assert!(!index.update_block(99, |block| block.size = 1));
            assert_counter_matches_fold(&index);

            assert!(index.remove_block(1).is_some());
            assert_eq!(counter.load(Ordering::SeqCst), 50);
            assert_counter_matches_fold(&index);

            assert!(index.remove_block(1).is_none());
            index.remove_block(2);
            assert_eq!(counter.load(Ordering::SeqCst), 0);
            assert_counter_matches_fold(&index);
        }

        #[rstest]
        fn tracks_blocks_from_block_struct() {
            let mut index = BlockIndex::new(PathBuf::from("unused"));
            let block = Block::new(3);
            index.insert_or_update(&block);
            index.insert_or_update(block);
            assert_counter_matches_fold(&index);
        }

        #[rstest]
        #[tokio::test]
        async fn restored_from_disk() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);
            let mut index = BlockIndex::new(path.clone());
            index.insert_or_update(entry(1, 10, 2));
            index.insert_or_update(entry(2, 30, 4));
            index.save().await.unwrap();

            let loaded = BlockIndex::try_load(path).await.unwrap();
            assert_eq!(loaded.size_counter().load(Ordering::SeqCst), 46);
            assert_counter_matches_fold(&loaded);
        }

        #[rstest]
        fn adopted_counter_is_published() {
            let mut index = BlockIndex::new(PathBuf::from("unused"));
            index.insert_or_update(entry(1, 10, 2));
            let shared = Arc::new(AtomicU64::new(999));

            index.adopt_size_counter(Arc::clone(&shared));
            assert_eq!(shared.load(Ordering::SeqCst), 12);

            index.insert_or_update(entry(2, 1, 1));
            assert_eq!(shared.load(Ordering::SeqCst), 14);
            assert_counter_matches_fold(&index);
        }
    }

    mod active_block_ids {
        use super::*;

        #[rstest]
        fn skip_corrupted_blocks() {
            let mut index = BlockIndex::new(PathBuf::from("unused"));
            assert_eq!(index.first_active_block_id(), None);
            assert_eq!(index.last_active_block_id(), None);
            assert_eq!(index.active_block_id_at(10), None);

            for block_id in [5u64, 10, 20] {
                index.insert_or_update(Block::new(block_id));
            }
            index.mark_corrupted(5);
            index.mark_corrupted(20);

            assert_eq!(index.first_active_block_id(), Some(10));
            assert_eq!(index.last_active_block_id(), Some(10));
            assert_eq!(index.active_block_id_at(4), None);
            assert_eq!(index.active_block_id_at(5), None);
            assert_eq!(index.active_block_id_at(10), Some(10));
            assert_eq!(index.active_block_id_at(25), Some(10));
            assert_eq!(
                index.active_tree().iter().next_back().copied(),
                index.last_active_block_id()
            );
        }
    }

    mod try_load {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_ok() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let block_index_proto = BlockIndexProto {
                blocks: vec![BlockEntry {
                    block_id: 1,
                    size: 1,
                    record_count: 1,
                    metadata_size: 1,
                    latest_record_time: Some(Timestamp::default()),
                    crc64: None,
                    compression: None,
                    corrupted: None,
                    version: None,
                }],
                crc64: 294433432134063049,
            };
            fs::write(&path, block_index_proto.encode_to_vec()).unwrap();

            let block_index = BlockIndex::try_load(path.clone()).await.unwrap();
            assert_eq!(block_index.size(), 2);
            assert_eq!(block_index.record_count(), 1);
            assert_eq!(block_index.tree().len(), 1);
            assert_eq!(block_index.path_buf, path);
        }

        #[rstest]
        #[tokio::test]
        async fn test_index_file_not_found() {
            let path = PathBuf::from("not_found");
            let block_index = BlockIndex::try_load(path.clone()).await.err().unwrap();
            assert_eq!(
                block_index,
                internal_server_error!("Block index {:?} not found", path)
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_index_file_corrupted() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let block_index_proto = BlockIndexProto {
                blocks: vec![BlockEntry {
                    block_id: 1,
                    size: 1,
                    record_count: 1,
                    metadata_size: 1,
                    latest_record_time: Some(Timestamp::default()),
                    crc64: None,
                    compression: None,
                    corrupted: None,
                    version: None,
                }],
                crc64: 0,
            };
            fs::write(&path, block_index_proto.encode_to_vec()).unwrap();

            let block_index = BlockIndex::try_load(path.clone()).await.err().unwrap();
            assert_eq!(
                block_index,
                internal_server_error!("Block index {:?} is corrupted", path)
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_decode_err() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);
            fs::write(&path, vec![0, 1, 2, 3]).unwrap();

            let block_index = BlockIndex::try_load(path.clone()).await.err().unwrap();
            assert_eq!(block_index, internal_server_error!("Failed to decode block index {:?}: failed to decode Protobuf message: invalid tag value: 0", path));
        }
    }

    mod save {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_ok() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let mut block_index = BlockIndex::new(path.clone());
            block_index.insert_or_update(BlockEntry {
                block_id: 1,
                size: 1,
                record_count: 1,
                metadata_size: 1,
                latest_record_time: Some(Timestamp::default()),
                crc64: None,
                compression: None,
                corrupted: None,
                version: None,
            });

            block_index.save().await.unwrap();

            let block_index_proto = BlockIndex::try_load(path.clone()).await.unwrap();
            assert_eq!(block_index_proto.size(), 2);
            assert_eq!(block_index_proto.record_count(), 1);
            assert_eq!(block_index_proto.tree().len(), 1);
        }

        #[rstest]
        #[tokio::test]
        async fn test_corrupted_round_trip() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let mut block_index = BlockIndex::new(path.clone());
            block_index.insert_or_update(BlockEntry {
                block_id: 1,
                size: 1,
                record_count: 1,
                metadata_size: 1,
                latest_record_time: Some(Timestamp::default()),
                crc64: None,
                compression: None,
                corrupted: None,
                version: None,
            });
            block_index.mark_corrupted(1);

            assert!(block_index.is_corrupted(1));
            assert_eq!(block_index.corrupted_block_count(), 1);
            assert_eq!(block_index.corrupted_block_ids(), vec![1]);

            block_index.save().await.unwrap();

            let block_index = BlockIndex::try_load(path).await.unwrap();
            assert!(block_index.is_corrupted(1));
            assert_eq!(block_index.corrupted_block_count(), 1);
            assert_eq!(block_index.corrupted_block_ids(), vec![1]);
        }
    }

    mod versions {
        use super::*;

        fn entry(version: Option<u64>) -> BlockEntry {
            BlockEntry {
                block_id: 1,
                size: 1,
                record_count: 1,
                metadata_size: 1,
                latest_record_time: Some(Timestamp::default()),
                crc64: None,
                compression: None,
                corrupted: None,
                version,
            }
        }

        #[rstest]
        fn keeps_version_when_update_has_none() {
            let mut block_index = BlockIndex::new(PathBuf::new());
            block_index.insert_or_update_with_crc(entry(Some(3)), 7);
            block_index.insert_or_update(entry(None));

            let block = block_index.get_block(1).unwrap();
            assert_eq!(block.version, Some(3));
            assert_eq!(block.crc64, None);
        }

        #[rstest]
        fn takes_explicit_version() {
            let mut block_index = BlockIndex::new(PathBuf::new());
            block_index.insert_or_update(entry(Some(3)));
            block_index.insert_or_update(entry(Some(5)));

            assert_eq!(block_index.get_block(1).unwrap().version, Some(5));
        }
    }
}
