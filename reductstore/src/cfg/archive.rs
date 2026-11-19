// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_SYNC_DELAY_S: u64 = 30;
const DEFAULT_MAX_LAG_S: u64 = 600;
const DEFAULT_MIN_IDLE_S: u64 = 300;
const DEFAULT_IO_TIMEOUT_S: u64 = 120;
const DEFAULT_SCAN_INTERVAL_S: u64 = 300;

/// Settings of the archive tier.
///
/// The archive is a directory, usually a network share, that receives a copy of
/// every file of the data folder. When `local_size` is set, the oldest data blocks
/// that are already in the archive are removed from the data folder to keep it
/// below that size, and fetched back on demand.
#[derive(Clone, Debug, PartialEq)]
pub struct ArchiveConfig {
    /// Directory of the archive
    pub path: PathBuf,
    /// Maximum size of the local data folder, `None` keeps everything local
    pub local_size: Option<u64>,
    /// How long a file must stay unchanged before it is copied
    pub sync_delay: Duration,
    /// How long a file may wait for a copy while it keeps changing
    pub max_lag: Duration,
    /// How long a data block must stay untouched before it can be evicted
    pub min_idle: Duration,
    /// Time limit of a single archive operation
    pub io_timeout: Duration,
    /// Read a block back from the archive and compare it before evicting it
    pub verify: bool,
    /// Attach to an archive that was initialised by another data folder
    pub adopt: bool,
    /// Interval of the consistency scan of the local data folder
    pub scan_interval: Duration,
}

impl ArchiveConfig {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            local_size: None,
            sync_delay: Duration::from_secs(DEFAULT_SYNC_DELAY_S),
            max_lag: Duration::from_secs(DEFAULT_MAX_LAG_S),
            min_idle: Duration::from_secs(DEFAULT_MIN_IDLE_S),
            io_timeout: Duration::from_secs(DEFAULT_IO_TIMEOUT_S),
            verify: true,
            adopt: false,
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_S),
        }
    }
}

pub(super) fn parse_archive_config<EnvGetter: GetEnv>(
    env: &mut Env<EnvGetter>,
) -> Option<ArchiveConfig> {
    let path = env
        .get_optional::<String>("RS_ARCHIVE_PATH")
        .filter(|path| !path.trim().is_empty())?;

    let defaults = ArchiveConfig::new(PathBuf::from(path.trim()));
    let mut seconds = |key: &str, default: Duration| {
        env.get_optional::<u64>(key)
            .map(Duration::from_secs)
            .unwrap_or(default)
    };

    let sync_delay = seconds("RS_ARCHIVE_SYNC_DELAY", defaults.sync_delay);
    let max_lag = seconds("RS_ARCHIVE_MAX_LAG", defaults.max_lag);
    let min_idle = seconds("RS_ARCHIVE_MIN_IDLE", defaults.min_idle);
    let io_timeout = seconds("RS_ARCHIVE_IO_TIMEOUT", defaults.io_timeout);
    let scan_interval = seconds("RS_ARCHIVE_SCAN_INTERVAL", defaults.scan_interval);

    Some(ArchiveConfig {
        local_size: env
            .get_optional::<ByteSize>("RS_ARCHIVE_LOCAL_SIZE")
            .map(|size| size.as_u64())
            .filter(|size| *size > 0),
        sync_delay,
        max_lag,
        min_idle,
        io_timeout: io_timeout.max(Duration::from_secs(1)),
        verify: env
            .get_optional::<bool>("RS_ARCHIVE_VERIFY")
            .unwrap_or(defaults.verify),
        adopt: env
            .get_optional::<bool>("RS_ARCHIVE_ADOPT")
            .unwrap_or(defaults.adopt),
        scan_interval: scan_interval.max(Duration::from_secs(1)),
        ..defaults
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::MockEnvGetter;
    use mockall::predicate::eq;
    use rstest::rstest;
    use std::env::VarError;

    #[rstest]
    fn archive_is_disabled_without_path() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        assert_eq!(parse_archive_config(&mut Env::new(env_getter)), None);
    }

    #[rstest]
    fn archive_uses_defaults_when_only_path_is_set() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_ARCHIVE_PATH"))
            .return_const(Ok("/mnt/nas/reductstore".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        assert_eq!(
            parse_archive_config(&mut Env::new(env_getter)),
            Some(ArchiveConfig::new(PathBuf::from("/mnt/nas/reductstore")))
        );
    }

    #[rstest]
    fn archive_parses_all_settings() {
        let mut env_getter = MockEnvGetter::new();
        for (key, value) in [
            ("RS_ARCHIVE_PATH", "/mnt/nas/reductstore"),
            ("RS_ARCHIVE_LOCAL_SIZE", "500GB"),
            ("RS_ARCHIVE_SYNC_DELAY", "5"),
            ("RS_ARCHIVE_MAX_LAG", "60"),
            ("RS_ARCHIVE_MIN_IDLE", "10"),
            ("RS_ARCHIVE_IO_TIMEOUT", "30"),
            ("RS_ARCHIVE_SCAN_INTERVAL", "120"),
            ("RS_ARCHIVE_VERIFY", "false"),
            ("RS_ARCHIVE_ADOPT", "true"),
        ] {
            env_getter
                .expect_get()
                .with(eq(key))
                .return_const(Ok(value.to_string()));
        }

        assert_eq!(
            parse_archive_config(&mut Env::new(env_getter)),
            Some(ArchiveConfig {
                path: PathBuf::from("/mnt/nas/reductstore"),
                local_size: Some(500_000_000_000),
                sync_delay: Duration::from_secs(5),
                max_lag: Duration::from_secs(60),
                min_idle: Duration::from_secs(10),
                io_timeout: Duration::from_secs(30),
                verify: false,
                adopt: true,
                scan_interval: Duration::from_secs(120),
            })
        );
    }
}
