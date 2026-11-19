// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::remote::MARKER_FILE;
use super::*;
use crate::backend::Backend;
use rstest::{fixture, rstest};
use std::io::{Read, Write};
use std::time::Duration;
use tempfile::tempdir;

const MIB: usize = 1024 * 1024;

struct Setup {
    root: PathBuf,
    archive: PathBuf,
    backend: Backend,
    inner: Arc<Inner>,
}

fn config(archive: &Path) -> ArchiveConfig {
    ArchiveConfig {
        sync_delay: Duration::ZERO,
        max_lag: Duration::from_secs(3600),
        min_idle: Duration::ZERO,
        io_timeout: Duration::from_secs(10),
        scan_interval: Duration::ZERO,
        ..ArchiveConfig::new(archive.to_path_buf())
    }
}

fn open(data: &Path, cfg: ArchiveConfig) -> Setup {
    let archive = cfg.path.clone();
    let backend = ArchiveBackend::build(data.to_path_buf(), cfg).unwrap();
    let inner = Arc::clone(&backend.inner);
    Setup {
        root: inner.root.clone(),
        archive,
        backend: Backend::from_backend(Box::new(backend)),
        inner,
    }
}

const NEWEST_BLOCK: &str = "bucket/entry/999.blk";

fn dirs() -> (PathBuf, PathBuf) {
    let base = tempdir().unwrap().keep();
    let archive = base.join("nas");
    std::fs::create_dir_all(&archive).unwrap();
    std::fs::write(archive.join(MARKER_FILE), b"").unwrap();
    (base.join("data"), archive)
}

#[fixture]
fn setup() -> Setup {
    let (data, archive) = dirs();
    open(&data, config(&archive))
}

/// A setup that evicts. The entry gets a block that is newer than the ones the
/// tests write, because the newest block of an entry always stays local.
fn limited(local_size: u64) -> Setup {
    let (data, archive) = dirs();
    std::fs::create_dir_all(data.join("bucket/entry")).unwrap();
    std::fs::write(data.join(NEWEST_BLOCK), b"").unwrap();
    open(
        &data,
        ArchiveConfig {
            local_size: Some(local_size),
            ..config(&archive)
        },
    )
}

impl Setup {
    fn local(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }

    fn archived(&self, rel: &str) -> PathBuf {
        self.archive.join("data").join(rel)
    }

    /// Write a file the way the engine does: through the backend, followed by a sync.
    async fn write(&self, rel: &str, content: &[u8]) {
        let mut file = self
            .backend
            .open_options()
            .create(true)
            .write(true)
            .open(self.local(rel))
            .await
            .unwrap();
        file.set_len(0).unwrap();
        file.write_all(content).unwrap();
        file.flush_local().await.unwrap();
        file.sync_all().await.unwrap();
    }

    async fn read(&self, rel: &str) -> std::io::Result<Vec<u8>> {
        let mut file = self
            .backend
            .open_options()
            .read(true)
            .open(self.local(rel))
            .await?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        Ok(content)
    }

    fn age(&self, rel: &str, hours: u64) {
        let file = std::fs::File::options()
            .write(true)
            .open(self.local(rel))
            .unwrap();
        file.set_modified(SystemTime::now() - Duration::from_secs(hours * 3600))
            .unwrap();
    }

    /// What the file cache does with the blocks that the backend hands it.
    async fn release_staged(&self) -> usize {
        let staged = self.backend.invalidate_locally_cached_files().await;
        for path in staged.iter() {
            self.backend
                .remove_from_local_cache_if_unused(path)
                .await
                .unwrap();
        }
        staged.len()
    }

    async fn cycle(&self) {
        self.inner.cycle().await;
        self.release_staged().await;
    }

    fn info(&self) -> ArchiveInfo {
        self.backend.archive_info().unwrap()
    }

    fn is_evicted(&self, rel: &str) -> bool {
        !self.local(rel).exists() && stub_path(&self.local(rel)).exists()
    }
}

fn block(seed: u8) -> Vec<u8> {
    (0..MIB).map(|i| (i as u8).wrapping_add(seed) | 1).collect()
}

mod mirroring {
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn copies_files_written_through_the_backend(setup: Setup) {
        setup.write("bucket/entry/1.blk", b"data").await;
        setup.write("bucket/entry/1.meta", b"descriptor").await;
        setup.write("bucket/entry/blocks.idx", b"index").await;
        setup.write("bucket/bucket.settings", b"settings").await;
        assert_eq!(setup.inner.index.dirty_len(), 4);

        setup.cycle().await;

        for (rel, content) in [
            ("bucket/entry/1.blk", "data"),
            ("bucket/entry/1.meta", "descriptor"),
            ("bucket/entry/blocks.idx", "index"),
            ("bucket/bucket.settings", "settings"),
        ] {
            assert_eq!(
                std::fs::read_to_string(setup.archived(rel)).unwrap(),
                content
            );
        }
        assert_eq!(setup.inner.index.dirty_len(), 0);

        let info = setup.info();
        assert!(info.online);
        assert!(info.ready);
        assert!(info.last_sync.is_some());
        assert_eq!(info.archived_usage, 0);
    }

    #[rstest]
    #[tokio::test]
    async fn copies_files_that_existed_before_the_archive(setup: Setup) {
        std::fs::create_dir_all(setup.local("bucket/entry")).unwrap();
        std::fs::write(setup.local("bucket/entry/1.blk"), b"old data").unwrap();

        setup.cycle().await;

        assert_eq!(
            std::fs::read(setup.archived("bucket/entry/1.blk")).unwrap(),
            b"old data"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn copies_changed_file_again(setup: Setup) {
        setup.write("bucket/entry/1.meta", b"v1").await;
        setup.cycle().await;

        setup.write("bucket/entry/1.meta", b"v2").await;
        setup.cycle().await;

        assert_eq!(
            std::fs::read(setup.archived("bucket/entry/1.meta")).unwrap(),
            b"v2"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn does_not_copy_small_file_rewritten_with_same_content(setup: Setup) {
        setup.write("bucket/bucket.settings", b"settings").await;
        setup.cycle().await;
        let archived = setup.archived("bucket/bucket.settings");
        let inode = LocalStat::of(&archived).unwrap().ino;

        setup.write("bucket/bucket.settings", b"settings").await;
        setup.cycle().await;
        assert_eq!(LocalStat::of(&archived).unwrap().ino, inode);
        assert_eq!(setup.inner.index.dirty_len(), 0);

        setup.write("bucket/bucket.settings", b"SETTINGS").await;
        setup.cycle().await;
        assert_eq!(std::fs::read(&archived).unwrap(), b"SETTINGS");
        assert_ne!(LocalStat::of(&archived).unwrap().ino, inode);
    }

    #[rstest]
    #[tokio::test]
    async fn leaves_engine_private_files_local(setup: Setup) {
        setup.write("bucket/entry/.wal/1.wal", b"wal").await;
        setup.write("bucket/entry/1.blk.tmp", b"tmp").await;
        setup.write("bucket/entry/replication.log", b"log").await;
        setup.write(".lock", b"lock").await;

        setup.cycle().await;

        assert_eq!(setup.inner.index.dirty_len(), 0);
        assert!(!setup.archived("bucket").exists());
        assert!(!setup.archived(".lock").exists());
        assert!(!setup.archived(".archive").exists());
    }

    #[rstest]
    #[tokio::test]
    async fn does_not_copy_again_after_restart(setup: Setup) {
        setup.write("bucket/entry/1.blk", b"data").await;
        setup.cycle().await;
        let (data, archive) = (setup.root.clone(), setup.archive.clone());
        drop(setup);

        let setup = open(&data, config(&archive));
        setup.cycle().await;

        assert_eq!(setup.inner.index.dirty_len(), 0);
        assert_eq!(setup.info().last_sync, None);
    }

    #[rstest]
    #[tokio::test]
    async fn copies_data_before_its_descriptor(setup: Setup) {
        setup.write("bucket/entry/1.blk", b"one").await;
        setup.write("bucket/entry/1.meta", b"describes one").await;
        setup.cycle().await;

        // A rewrite of a block that does not go through the file cache.
        std::fs::write(setup.local("bucket/entry/1.blk.new"), b"one and two").unwrap();
        std::fs::rename(
            setup.local("bucket/entry/1.blk.new"),
            setup.local("bucket/entry/1.blk"),
        )
        .unwrap();
        setup
            .write("bucket/entry/1.meta", b"describes one and two")
            .await;
        setup.inner.mirror_pass().await;

        assert_eq!(
            std::fs::read(setup.archived("bucket/entry/1.blk")).unwrap(),
            b"one and two"
        );
        assert_eq!(
            std::fs::read(setup.archived("bucket/entry/1.meta")).unwrap(),
            b"describes one and two"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn copies_block_that_never_settles_after_max_lag() {
        let (data, archive) = dirs();
        let setup = open(
            &data,
            ArchiveConfig {
                sync_delay: Duration::from_secs(3600),
                max_lag: Duration::ZERO,
                ..config(&archive)
            },
        );
        setup.write("bucket/entry/1.blk", b"records").await;
        setup.write("bucket/entry/1.meta", b"descriptor").await;

        setup.cycle().await;

        assert_eq!(
            std::fs::read(setup.archived("bucket/entry/1.blk")).unwrap(),
            b"records"
        );
        assert_eq!(
            std::fs::read(setup.archived("bucket/entry/1.meta")).unwrap(),
            b"descriptor"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn waits_for_file_to_settle() {
        let (data, archive) = dirs();
        let setup = open(
            &data,
            ArchiveConfig {
                sync_delay: Duration::from_secs(3600),
                ..config(&archive)
            },
        );
        setup.write("bucket/entry/1.blk", b"records").await;

        setup.cycle().await;

        assert!(!setup.archived("bucket/entry/1.blk").exists());
        assert_eq!(setup.inner.index.dirty_len(), 1);
    }
}

mod eviction {
    use super::*;

    async fn three_blocks(setup: &Setup) {
        for (id, hours) in [(1u8, 3), (2, 2), (3, 1)] {
            let rel = format!("bucket/entry/{}.blk", id);
            setup.write(&rel, &block(id)).await;
            setup.age(&rel, hours);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn evicts_coldest_block_and_fetches_it_back_on_open() {
        let setup = limited(2 * MIB as u64 + MIB as u64 / 2);
        three_blocks(&setup).await;

        setup.cycle().await;

        assert!(setup.is_evicted("bucket/entry/1.blk"));
        assert!(setup.local("bucket/entry/2.blk").exists());
        assert!(setup.local("bucket/entry/3.blk").exists());
        let info = setup.info();
        assert_eq!(info.archived_files, 1);
        assert_eq!(info.archived_usage, MIB as u64);
        assert!(info.local_usage <= 2 * MIB as u64 + MIB as u64 / 2);

        let evicted = setup.local("bucket/entry/1.blk");
        assert!(setup.backend.try_exists(&evicted).await.unwrap());
        let stats = setup.backend.get_stats(&evicted).await.unwrap().unwrap();
        assert_eq!(stats.size, Some(MIB as i64));
        let listing = setup
            .backend
            .read_dir(&setup.local("bucket/entry"))
            .await
            .unwrap();
        assert_eq!(listing.len(), 4);
        assert!(listing.contains(&evicted));

        assert_eq!(setup.read("bucket/entry/1.blk").await.unwrap(), block(1));
        assert!(evicted.exists());
        assert!(!stub_path(&evicted).exists());
        assert_eq!(setup.info().recalled_files, 1);
        assert_eq!(setup.info().archived_files, 0);
    }

    #[rstest]
    #[tokio::test]
    async fn keeps_block_that_was_just_fetched_back() {
        let setup = limited(2 * MIB as u64 + MIB as u64 / 2);
        three_blocks(&setup).await;
        setup.cycle().await;
        setup.read("bucket/entry/1.blk").await.unwrap();

        setup.cycle().await;

        assert!(setup.local("bucket/entry/1.blk").exists());
        assert!(setup.is_evicted("bucket/entry/2.blk"));
        assert_eq!(setup.info().recalled_files, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn keeps_everything_without_a_limit(setup: Setup) {
        three_blocks(&setup).await;
        setup.cycle().await;
        setup.cycle().await;

        assert_eq!(setup.info().archived_files, 0);
    }

    #[rstest]
    #[tokio::test]
    async fn never_evicts_what_is_not_in_the_archive() {
        let (data, archive) = dirs();
        let setup = open(
            &data,
            ArchiveConfig {
                local_size: Some(1),
                sync_delay: Duration::from_secs(3600),
                ..config(&archive)
            },
        );
        three_blocks(&setup).await;

        setup.cycle().await;

        assert_eq!(setup.info().archived_files, 0);
        assert!(setup.local("bucket/entry/1.blk").exists());
    }

    #[rstest]
    #[tokio::test]
    async fn only_evicts_block_data() {
        let setup = limited(1);
        setup.write("bucket/entry/1.blk", &block(1)).await;
        setup.write("bucket/entry/1.meta", &block(2)).await;
        setup.write("bucket/entry/blocks.idx", &block(3)).await;
        setup.age("bucket/entry/1.blk", 1);

        setup.cycle().await;

        assert!(setup.is_evicted("bucket/entry/1.blk"));
        assert!(setup.local("bucket/entry/1.meta").exists());
        assert!(setup.local("bucket/entry/blocks.idx").exists());
    }

    #[rstest]
    #[tokio::test]
    async fn abandons_eviction_when_block_is_used_meanwhile() {
        let setup = limited(1);
        setup.write("bucket/entry/1.blk", &block(1)).await;
        setup.age("bucket/entry/1.blk", 1);
        setup.inner.cycle().await;

        let path = setup.local("bucket/entry/1.blk");
        setup
            .backend
            .open_options()
            .read(true)
            .open(&path)
            .await
            .unwrap();
        assert_eq!(setup.release_staged().await, 1);

        assert!(path.exists());
        assert!(!stub_path(&path).exists());
        assert!(!with_suffix(&path, STUB_TMP_EXT).exists());
        assert_eq!(setup.info().archived_files, 0);
    }

    #[rstest]
    #[tokio::test]
    async fn abandons_eviction_when_block_changes_meanwhile() {
        let setup = limited(1);
        setup.write("bucket/entry/1.blk", &block(1)).await;
        setup.age("bucket/entry/1.blk", 1);
        setup.inner.cycle().await;

        setup.write("bucket/entry/1.blk", &block(9)).await;
        setup.age("bucket/entry/1.blk", 1);
        assert_eq!(setup.release_staged().await, 1);
        assert!(setup.local("bucket/entry/1.blk").exists());

        setup.cycle().await;
        assert!(setup.is_evicted("bucket/entry/1.blk"));
        assert_eq!(setup.read("bucket/entry/1.blk").await.unwrap(), block(9));
    }

    #[rstest]
    #[tokio::test]
    async fn repairs_archive_copy_instead_of_evicting() {
        let setup = limited(1);
        setup.write("bucket/entry/1.blk", &block(1)).await;
        setup.age("bucket/entry/1.blk", 1);
        assert!(setup.inner.remote.connect(false).await);
        setup.inner.mirror_pass().await;

        let archived = setup.archived("bucket/entry/1.blk");
        let mtime = std::fs::metadata(&archived).unwrap().modified().unwrap();
        std::fs::write(&archived, block(7)).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&archived)
            .unwrap()
            .set_modified(mtime)
            .unwrap();

        setup.inner.cycle().await;
        assert_eq!(setup.release_staged().await, 0);
        assert!(setup.local("bucket/entry/1.blk").exists());

        setup.cycle().await;
        assert_eq!(std::fs::read(&archived).unwrap(), block(1));
        assert!(setup.is_evicted("bucket/entry/1.blk"));
    }

    #[rstest]
    #[tokio::test]
    async fn refuses_block_that_the_archive_damaged() {
        let setup = limited(1);
        setup.write("bucket/entry/1.blk", &block(1)).await;
        setup.age("bucket/entry/1.blk", 1);
        setup.cycle().await;
        assert!(setup.is_evicted("bucket/entry/1.blk"));

        std::fs::write(setup.archived("bucket/entry/1.blk"), block(7)).unwrap();

        let err = setup.read("bucket/entry/1.blk").await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(setup.is_evicted("bucket/entry/1.blk"));
        assert!(!with_suffix(&setup.local("bucket/entry/1.blk"), RECALL_TMP_EXT).exists());
    }

    #[rstest]
    #[tokio::test]
    async fn knows_evicted_blocks_after_restart() {
        let setup = limited(1);
        setup.write("bucket/entry/1.blk", &block(1)).await;
        setup.age("bucket/entry/1.blk", 1);
        setup.cycle().await;
        let (data, archive) = (setup.root.clone(), setup.archive.clone());
        drop(setup);

        let setup = open(
            &data,
            ArchiveConfig {
                local_size: Some(1),
                ..config(&archive)
            },
        );
        let path = setup.local("bucket/entry/1.blk");
        assert!(setup.backend.try_exists(&path).await.unwrap());

        setup.cycle().await;
        assert_eq!(setup.info().archived_files, 1);
        assert_eq!(setup.info().archived_usage, MIB as u64);
        assert_eq!(setup.read("bucket/entry/1.blk").await.unwrap(), block(1));
    }

    #[rstest]
    #[tokio::test]
    async fn reports_stale_descriptor_of_evicted_block() {
        let setup = limited(1);
        setup.write("bucket/entry/1.blk", &block(1)).await;
        setup.age("bucket/entry/1.blk", 1);
        setup.cycle().await;

        let backend = ArchiveBackend {
            inner: Arc::clone(&setup.inner),
        };
        let err = backend
            .update_local_cache(&setup.local("bucket/entry/1.blk"), &AccessMode::Read)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::StaleNetworkFileHandle);
    }
}

mod removal {
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn removes_file_from_archive(setup: Setup) {
        setup.write("bucket/entry/1.blk", b"data").await;
        setup.cycle().await;

        setup
            .backend
            .remove(setup.local("bucket/entry/1.blk"))
            .await
            .unwrap();
        assert_eq!(setup.info().pending_removals, 1);
        setup.cycle().await;

        assert!(!setup.archived("bucket/entry/1.blk").exists());
        assert_eq!(setup.info().pending_removals, 0);
    }

    #[rstest]
    #[tokio::test]
    async fn removes_evicted_block() {
        let setup = limited(1);
        setup.write("bucket/entry/1.blk", &block(1)).await;
        setup.age("bucket/entry/1.blk", 1);
        setup.cycle().await;
        let path = setup.local("bucket/entry/1.blk");

        setup.backend.remove(&path).await.unwrap();
        setup.cycle().await;

        assert!(!stub_path(&path).exists());
        assert!(!setup.backend.try_exists(&path).await.unwrap());
        assert!(!setup.archived("bucket/entry/1.blk").exists());
        assert_eq!(setup.info().archived_files, 0);
        assert_eq!(
            setup.backend.remove(&path).await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn keeps_replaced_block_until_its_replacement_is_copied(setup: Setup) {
        setup.write("bucket/entry/1.blk", b"data").await;
        setup.cycle().await;

        // What compression does: the new files appear, then the old ones go.
        setup.write("bucket/entry/1.blk.zst", b"compressed").await;
        setup
            .backend
            .remove(setup.local("bucket/entry/1.blk"))
            .await
            .unwrap();
        setup.inner.apply_journal().await;
        assert!(setup.archived("bucket/entry/1.blk").exists());

        setup.inner.mirror_pass().await;
        setup.inner.apply_journal().await;
        assert!(setup.archived("bucket/entry/1.blk.zst").exists());
        assert!(!setup.archived("bucket/entry/1.blk").exists());
    }

    #[rstest]
    #[tokio::test]
    async fn keeps_archive_copy_of_file_that_exists_again(setup: Setup) {
        setup.write("bucket/entry/1.blk", b"data").await;
        setup.cycle().await;

        setup
            .backend
            .remove(setup.local("bucket/entry/1.blk"))
            .await
            .unwrap();
        setup.write("bucket/entry/1.blk", b"again").await;
        setup.cycle().await;

        assert_eq!(
            std::fs::read(setup.archived("bucket/entry/1.blk")).unwrap(),
            b"again"
        );
        assert_eq!(setup.info().pending_removals, 0);
    }

    #[rstest]
    #[tokio::test]
    async fn removes_directory_from_archive(setup: Setup) {
        setup.write("bucket/entry/1.blk", b"data").await;
        setup.write("bucket/other/1.blk", b"data").await;
        setup.cycle().await;

        setup
            .backend
            .remove_dir_all(setup.local("bucket/entry"))
            .await
            .unwrap();
        setup.cycle().await;

        assert!(!setup.archived("bucket/entry").exists());
        assert!(setup.archived("bucket/other/1.blk").exists());
        assert_eq!(setup.info().pending_removals, 0);
    }

    #[rstest]
    #[tokio::test]
    async fn renamed_directory_keeps_its_evicted_blocks_reachable() {
        let setup = limited(1);
        setup.write("bucket/old/1.blk", &block(1)).await;
        setup.write("bucket/old/1.meta", b"descriptor").await;
        setup.write("bucket/old/2.blk", b"").await;
        setup.age("bucket/old/1.blk", 1);
        setup.cycle().await;
        assert!(setup.is_evicted("bucket/old/1.blk"));

        setup
            .backend
            .rename(setup.local("bucket/old"), setup.local("bucket/new"))
            .await
            .unwrap();
        setup.cycle().await;

        assert!(!setup.archived("bucket/old").exists());
        assert!(setup.archived("bucket/new/1.meta").exists());
        assert_eq!(setup.read("bucket/new/1.blk").await.unwrap(), block(1));
        assert_eq!(setup.info().pending_removals, 0);
    }
}

mod outage {
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn works_locally_while_archive_is_away_and_catches_up() {
        let setup = limited(1);
        setup.write("bucket/entry/1.blk", &block(1)).await;
        setup.age("bucket/entry/1.blk", 1);
        setup.cycle().await;
        assert!(setup.is_evicted("bucket/entry/1.blk"));

        let away = setup.archive.with_extension("away");
        std::fs::rename(&setup.archive, &away).unwrap();
        std::fs::create_dir_all(&setup.archive).unwrap();
        assert!(!setup.inner.remote.connect(false).await);

        setup.write("bucket/entry/2.blk", &block(2)).await;
        setup.age("bucket/entry/2.blk", 1);
        setup.write("bucket/gone/1.blk", b"data").await;
        setup
            .backend
            .remove_dir_all(setup.local("bucket/gone"))
            .await
            .unwrap();
        setup.cycle().await;

        let info = setup.info();
        assert!(!info.online);
        assert!(info.last_error.unwrap().contains("marker"));
        assert!(setup.local("bucket/entry/2.blk").exists());
        assert!(setup
            .backend
            .try_exists(setup.local("bucket/entry/1.blk"))
            .await
            .unwrap());
        let err = setup.read("bucket/entry/1.blk").await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        assert!(!setup.archive.join("data").exists());

        std::fs::remove_dir_all(&setup.archive).unwrap();
        std::fs::rename(&away, &setup.archive).unwrap();
        assert!(setup.inner.remote.connect(false).await);
        setup.cycle().await;

        assert!(setup.info().online);
        assert_eq!(setup.info().pending_removals, 0);
        assert!(setup.is_evicted("bucket/entry/2.blk"));
        assert_eq!(setup.read("bucket/entry/1.blk").await.unwrap(), block(1));
    }

    #[rstest]
    #[tokio::test]
    async fn removals_survive_restart() {
        let (data, archive) = dirs();
        let setup = open(&data, config(&archive));
        setup.write("bucket/entry/1.blk", b"data").await;
        setup.cycle().await;

        std::fs::remove_file(archive.join(MARKER_FILE)).unwrap();
        assert!(!setup.inner.remote.connect(false).await);
        setup
            .backend
            .remove(setup.local("bucket/entry/1.blk"))
            .await
            .unwrap();
        setup.cycle().await;
        assert!(setup.archived("bucket/entry/1.blk").exists());
        drop(setup);

        let instance = std::fs::read_to_string(data.join(".archive/instance")).unwrap();
        std::fs::write(
            archive.join(MARKER_FILE),
            format!("{{\"instance\":\"{}\"}}", instance),
        )
        .unwrap();
        let setup = open(&data, config(&archive));
        assert_eq!(setup.info().pending_removals, 1);
        setup.cycle().await;

        assert!(!setup.archived("bucket/entry/1.blk").exists());
        assert_eq!(setup.info().pending_removals, 0);
    }

    #[rstest]
    #[tokio::test]
    async fn stays_offline_without_marker() {
        let base = tempdir().unwrap().keep();
        let archive = base.join("nas");
        std::fs::create_dir_all(&archive).unwrap();
        let setup = open(&base.join("data"), config(&archive));
        setup.write("bucket/entry/1.blk", b"data").await;

        setup.cycle().await;

        assert!(!setup.info().online);
        assert_eq!(std::fs::read_dir(&archive).unwrap().count(), 0);
        assert_eq!(setup.read("bucket/entry/1.blk").await.unwrap(), b"data");
    }
}

mod setup_checks {
    use super::*;

    #[rstest]
    fn refuses_archive_inside_data_folder() {
        let base = tempdir().unwrap().keep();
        let data = base.join("data");
        std::fs::create_dir_all(data.join("nas")).unwrap();

        assert!(ArchiveBackend::build(data.clone(), config(&data.join("nas"))).is_err());
        assert!(ArchiveBackend::build(data.join("nested"), config(&data)).is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn behaves_like_a_plain_folder_outside_the_data_folder(setup: Setup) {
        let outside = tempdir().unwrap().keep();
        let path = outside.join("1.blk");
        std::fs::write(&path, b"data").unwrap();

        assert!(setup.backend.try_exists(&path).await.unwrap());
        setup.backend.remove(&path).await.unwrap();
        assert!(!setup.backend.try_exists(&path).await.unwrap());
        assert_eq!(setup.info().pending_removals, 0);
    }
}
