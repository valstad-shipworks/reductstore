// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::backend::file::File;
use crate::storage::bucket::Bucket;
use crate::storage::entry::Entry;
use log::debug;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::bucket_api::QuotaType;
use reduct_base::{bad_request, internal_server_error};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Last sampled free space of the data folder, reused while it is younger than
/// the configured check interval.
pub(super) struct FreeSpaceCache {
    epoch: Instant,
    sampled_at_ms: AtomicU64,
    available: AtomicU64,
}

impl Default for FreeSpaceCache {
    fn default() -> Self {
        Self {
            epoch: Instant::now(),
            sampled_at_ms: AtomicU64::new(u64::MAX),
            available: AtomicU64::new(0),
        }
    }
}

impl FreeSpaceCache {
    fn get_or_refresh(
        &self,
        interval: Duration,
        sample: impl FnOnce() -> Result<u64, ReductError>,
    ) -> Result<u64, ReductError> {
        let now_ms = self.epoch.elapsed().as_millis() as u64;
        let sampled_at_ms = self.sampled_at_ms.load(Ordering::Acquire);
        if sampled_at_ms != u64::MAX
            && now_ms.saturating_sub(sampled_at_ms) < interval.as_millis() as u64
        {
            return Ok(self.available.load(Ordering::Acquire));
        }

        let available = sample()?;
        self.available.store(available, Ordering::Release);
        self.sampled_at_ms.store(now_ms, Ordering::Release);
        Ok(available)
    }
}

impl Bucket {
    /// Ensure the filesystem that holds the data folder has enough free space to
    /// store an incoming record of `content_size` bytes.
    ///
    /// This complements the configured quota: even when the bucket is within its
    /// quota, the write is rejected early if the underlying filesystem cannot fit
    /// the record. The check runs before any data is written.
    pub(super) fn check_free_disk_space(&self, content_size: u64) -> Result<(), ReductError> {
        let available = self.available_disk_space()?;

        if content_size > available {
            return Err(ReductError::new(
                ErrorCode::InsufficientStorage,
                &format!(
                    "Not enough free disk space in the data folder to write a record of {} bytes: only {} bytes available",
                    content_size, available
                ),
            ));
        }

        Ok(())
    }

    pub(super) async fn keep_quota_for(
        self: &Arc<Self>,
        content_size: u64,
    ) -> Result<(), ReductError> {
        let settings = self.settings.read().await?;
        let quota_size = settings.quota_size.unwrap_or(0);
        match settings.quota_type.clone().unwrap_or(QuotaType::NONE) {
            QuotaType::NONE => Ok(()),
            QuotaType::FIFO => self.remove_oldest_block(content_size, quota_size).await,
            QuotaType::HARD => {
                let total_size = self.cached_size().await?;
                if total_size + content_size > quota_size {
                    Err(bad_request!("Quota of '{}' exceeded", self.name()))
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn remove_oldest_block(
        &self,
        content_size: u64,
        quota_size: u64,
    ) -> Result<(), ReductError> {
        let mut size = self.cached_size().await? + content_size;
        while size > quota_size {
            let mut success = false;

            {
                debug!(
                    "Need more space. Remove an oldest block from bucket '{}'",
                    self.name()
                );

                let entries = self
                    .entries
                    .read()
                    .await?
                    .values()
                    .filter(|entry| entry.is_eligible_for_fifo_eviction())
                    .cloned()
                    .collect::<Vec<Arc<Entry>>>();

                let mut candidates: Vec<(u64, Arc<Entry>)> = Vec::with_capacity(entries.len());
                for entry in entries {
                    let info = entry.info().await?;
                    candidates.push((info.oldest_record, entry));
                }
                candidates.sort_by_key(|entry| entry.0);

                for (_, entry) in candidates {
                    debug!("Remove an oldest block from entry '{}'", entry.name());
                    match entry.try_remove_oldest_block().await {
                        Ok(_) => {
                            success = true;
                            break;
                        }
                        Err(e) => {
                            debug!(
                                "Failed to remove oldest block from entry '{}': {}",
                                entry.name(),
                                e
                            );
                        }
                    }
                }
            }

            if !success {
                return Err(internal_server_error!(
                    "Failed to keep quota of '{}'",
                    self.name()
                ));
            }

            size = self.cached_size().await? + content_size;
        }

        Ok(())
    }

    /// Sum of the entries' tracked sizes; does not lock any entry.
    pub(crate) async fn cached_size(&self) -> Result<u64, ReductError> {
        let entries = self.entries.read().await?;
        Ok(entries.values().map(|entry| entry.cached_size()).sum())
    }

    fn available_disk_space(&self) -> Result<u64, ReductError> {
        let sample = || {
            File::run_blocking_io(|| (self.free_space_fn)(&self.path)).map_err(|e| {
                internal_server_error!("Failed to query free disk space for the data folder: {}", e)
            })
        };

        let interval = self.cfg.engine_config.free_space_check_interval;
        if interval.is_zero() {
            sample()
        } else {
            self.free_space_cache.get_or_refresh(interval, sample)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FreeSpaceCache;
    use crate::cfg::storage_engine::StorageEngineConfig;
    use crate::cfg::Cfg;
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::bucket::tests::{bucket, path, read, write, write_meta};
    use crate::storage::bucket::{Bucket, FreeSpaceFn};
    use reduct_base::error::{ErrorCode, ReductError};
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use rstest::rstest;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Build a bucket whose free-space provider reports a fixed number of
    /// available bytes, so the disk-space gate can be exercised deterministically.
    async fn bucket_with_free_space(
        settings: BucketSettings,
        path: PathBuf,
        free_space_fn: FreeSpaceFn,
    ) -> Arc<Bucket> {
        FILE_CACHE.create_dir_all(&path.join("test")).await.unwrap();
        Arc::new(
            Bucket::builder()
                .name("test")
                .data_path(path)
                .settings(settings)
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .free_space_fn(free_space_fn)
                .build()
                .await
                .unwrap(),
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_fifo_quota_keeping(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                max_block_size: Some(20),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(120),
                max_block_records: Some(100),
            },
            path,
        )
        .await;

        let blob: &[u8] = &[0u8; 40];

        write(&bucket, "test-1", 0, blob).await.unwrap();
        assert_eq!(bucket.clone().info().await.unwrap().info.size, 44);

        write(&bucket, "test-2", 1, blob).await.unwrap();
        assert_eq!(bucket.clone().info().await.unwrap().info.size, 91);

        write(&bucket, "test-3", 2, blob).await.unwrap();
        assert_eq!(bucket.clone().info().await.unwrap().info.size, 94);

        assert_eq!(
            crate::storage::bucket::tests::read(&bucket, "test-1", 0)
                .await
                .err(),
            Some(ReductError::not_found(
                "Record 0 not found in entry test/test-1"
            ))
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fifo_quota_ignores_meta_entries_for_eviction(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                max_block_size: Some(20),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(120),
                max_block_records: Some(100),
            },
            path,
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        write_meta(&bucket, "data-1/$meta", 0, blob).await.unwrap();
        write(&bucket, "data-1", 1, blob).await.unwrap();
        write(&bucket, "data-2", 2, blob).await.unwrap();

        assert!(crate::storage::bucket::tests::read(&bucket, "data-1", 1)
            .await
            .is_err());
        assert!(
            crate::storage::bucket::tests::read(&bucket, "data-1/$meta", 0)
                .await
                .is_ok()
        );
        assert!(crate::storage::bucket::tests::read(&bucket, "data-2", 2)
            .await
            .is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_hard_quota_keeping(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                quota_type: Some(QuotaType::HARD),
                quota_size: Some(100),
                ..BucketSettings::default()
            },
            path,
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        write(&bucket, "test-1", 0, blob).await.unwrap();
        write(&bucket, "test-2", 1, blob).await.unwrap();

        let err = write(&bucket, "test-3", 2, blob).await.err().unwrap();
        assert_eq!(err, ReductError::bad_request("Quota of 'test' exceeded"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_blob_bigger_than_quota(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                max_block_size: Some(5),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(10),
                max_block_records: Some(100),
            },
            path,
        )
        .await;

        write(&bucket, "test-1", 0, b"test").await.unwrap();
        bucket.sync_fs().await.unwrap(); // we need to sync to get the correct size
        assert_eq!(bucket.clone().info().await.unwrap().info.size, 24);

        let result = write(&bucket, "test-2", 1, b"0123456789___").await;
        assert_eq!(
            result.err(),
            Some(ReductError::internal_server_error(
                "Failed to keep quota of 'test'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_rejected_when_disk_full(path: PathBuf) {
        // Simulate a filesystem that only has 10 bytes of free space left.
        let bucket = bucket_with_free_space(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                ..BucketSettings::default()
            },
            path,
            Arc::new(|_| Ok(10)),
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        let err = write(&bucket, "test-1", 0, blob).await.err().unwrap();

        assert_eq!(err.status, ErrorCode::InsufficientStorage);
        assert_eq!(
            err.message,
            "Not enough free disk space in the data folder to write a record of 40 bytes: only 10 bytes available"
        );

        // The write must be rejected before any data is stored.
        assert!(
            read(&bucket, "test-1", 0).await.is_err(),
            "no record should have been written when the disk is full"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_allowed_when_disk_has_space(path: PathBuf) {
        // Plenty of free disk space: the write must succeed as before.
        let bucket = bucket_with_free_space(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                ..BucketSettings::default()
            },
            path,
            Arc::new(|_| Ok(1_000_000)),
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        write(&bucket, "test-1", 0, blob).await.unwrap();
        assert!(read(&bucket, "test-1", 0).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_free_space_sampled_every_write_by_default(path: PathBuf) {
        let calls = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&calls);
        let bucket = bucket_with_free_space(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                ..BucketSettings::default()
            },
            path,
            Arc::new(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(1_000_000)
            }),
        )
        .await;

        bucket.check_free_disk_space(10).unwrap();
        bucket.check_free_disk_space(10).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[rstest]
    #[tokio::test]
    async fn test_free_space_cached_within_interval(path: PathBuf) {
        let calls = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&calls);
        let cfg = Cfg {
            engine_config: StorageEngineConfig {
                free_space_check_interval: Duration::from_secs(3600),
                ..StorageEngineConfig::default()
            },
            ..Cfg::default()
        };
        FILE_CACHE.create_dir_all(&path.join("test")).await.unwrap();
        let bucket = Bucket::builder()
            .name("test")
            .data_path(path)
            .settings(BucketSettings {
                quota_type: Some(QuotaType::NONE),
                ..BucketSettings::default()
            })
            .cfg(cfg)
            .usage_counters(Default::default())
            .free_space_fn(Arc::new(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(20)
            }))
            .build()
            .await
            .unwrap();

        bucket.check_free_disk_space(10).unwrap();
        bucket.check_free_disk_space(10).unwrap();
        let err = bucket.check_free_disk_space(30).unwrap_err();
        assert_eq!(err.status, ErrorCode::InsufficientStorage);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    fn test_free_space_cache_refreshes_when_stale() {
        let cache = FreeSpaceCache::default();
        assert_eq!(cache.get_or_refresh(Duration::ZERO, || Ok(5)).unwrap(), 5);
        assert_eq!(cache.get_or_refresh(Duration::ZERO, || Ok(7)).unwrap(), 7);
        assert_eq!(
            cache
                .get_or_refresh(Duration::from_secs(3600), || Ok(9))
                .unwrap(),
            7
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_disk_full_error_surfaces_provider_failure(path: PathBuf) {
        // A failure while querying free space is reported as an internal error.
        let bucket = bucket_with_free_space(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                ..BucketSettings::default()
            },
            path,
            Arc::new(|_| Err(std::io::Error::other("boom"))),
        )
        .await;

        let blob: &[u8] = &[0u8; 40];
        let err = write(&bucket, "test-1", 0, blob).await.err().unwrap();
        assert_eq!(err.status, ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    async fn test_fifo_quota_removes_compressed_oldest_block(path: PathBuf) {
        let bucket = bucket(
            BucketSettings {
                quota_type: Some(QuotaType::NONE),
                max_block_records: Some(1),
                ..BucketSettings::default()
            },
            path,
        )
        .await;
        let blob: &[u8] = &[0; 40];

        write(&bucket, "entry", 1, blob).await.unwrap();
        write(&bucket, "entry", 2, blob).await.unwrap();
        bucket.sync_fs().await.unwrap();
        let bucket = Arc::new(
            Bucket::builder()
                .path(bucket.path.clone())
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .restore()
                .await
                .unwrap(),
        );
        bucket
            .clone()
            .compress_blocks(Some(vec!["entry".into()]), None, Some(2))
            .await
            .unwrap();

        let compressed_data_path = bucket.path.join("entry/1.blk.zst");
        let compressed_desc_path = bucket.path.join("entry/1.meta.zst");
        assert!(compressed_data_path.exists());
        assert!(compressed_desc_path.exists());

        let size = bucket.clone().info().await.unwrap().info.size;
        bucket
            .set_settings(BucketSettings {
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(size + blob.len() as u64 - 1),
                max_block_records: Some(1),
                ..BucketSettings::default()
            })
            .await
            .unwrap();
        write(&bucket, "entry", 3, blob).await.unwrap();

        assert!(crate::storage::bucket::tests::read(&bucket, "entry", 1)
            .await
            .is_err());
        assert!(!compressed_data_path.exists());
        assert!(!compressed_desc_path.exists());
        assert!(bucket.clone().info().await.unwrap().info.size <= size + blob.len() as u64 - 1);
    }
}
