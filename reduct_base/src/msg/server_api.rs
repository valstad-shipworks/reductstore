// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::msg::bucket_api::{BucketInfo, BucketSettings};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// License information
///
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct License {
    /// Licensee usually is the company name
    pub licensee: String,
    /// Invoice number
    pub invoice: String,
    /// Expiry date
    pub expiry_date: DateTime<Utc>,
    /// Plan name
    pub plan: String,
    /// Number of devices (0 for unlimited)
    pub device_number: u32,
    /// Disk quota in TB (0 for unlimited)
    pub disk_quota: i32,
    /// Fingerprint
    #[serde(default)]
    pub fingerprint: String,
}

impl Display for License {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "\n\tLicensee: {}\n\tInvoice: {}\n\tExpiry Date: {}\n\tPlan: {}\n\tNumber of Devices: {}\n\tDisk Quota: {} TB,\n\tFingerprint: {}",
            self.licensee, self.invoice, self.expiry_date, self.plan, self.device_number, self.disk_quota, self.fingerprint
        )
    }
}

/// Information about a ReductSoftware UG instance
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct ServerInfo {
    /// Version of ReductSoftware UG instance
    pub version: String,
    /// Effective instance name resolved from the runtime configuration
    #[serde(default)]
    pub instance_name: String,
    /// Effective instance role (`STANDALONE`, `PRIMARY`, `SECONDARY`, or `REPLICA`)
    #[serde(default)]
    pub instance_role: String,
    /// Number of buckets
    pub bucket_count: u64,
    /// Disk usage in bytes
    pub usage: u64,
    /// Uptime in seconds
    pub uptime: u64,
    /// Oldest record in instance
    pub oldest_record: u64,
    /// Latest record in instance
    pub latest_record: u64,
    /// Default settings
    pub defaults: Defaults,
    /// License information if it is `None`, then the instance uses Apache-2.0
    pub license: Option<License>,
    /// State of the archive tier, `None` when no archive is configured
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive: Option<ArchiveInfo>,
}

/// State of the archive tier that mirrors the data folder and holds the blocks evicted from it
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
#[serde(default)]
pub struct ArchiveInfo {
    /// Location of the archive
    pub path: String,
    /// The archive is reachable
    pub online: bool,
    /// The local data folder has been indexed, eviction is held back until then
    pub ready: bool,
    /// Maximum size of the local data folder in bytes, `None` disables eviction
    pub local_size_limit: Option<u64>,
    /// Size of the local data folder in bytes
    pub local_usage: u64,
    /// Size of the data that is stored only in the archive in bytes
    pub archived_usage: u64,
    /// Number of data blocks that are stored only in the archive
    pub archived_files: u64,
    /// Size of the local files that still have to be copied to the archive in bytes
    pub pending_usage: u64,
    /// Number of local files that still have to be copied to the archive
    pub pending_files: u64,
    /// Number of removals that still have to be applied to the archive
    pub pending_removals: u64,
    /// Seconds the oldest pending change has been waiting
    pub lag: u64,
    /// Free space of the archive in bytes
    pub archive_free: Option<u64>,
    /// Capacity of the archive in bytes
    pub archive_total: Option<u64>,
    /// UNIX timestamp in seconds of the last file copied to the archive
    pub last_sync: Option<u64>,
    /// Number of data blocks fetched back from the archive since start
    pub recalled_files: u64,
    /// Last archive error
    pub last_error: Option<String>,
}

/// Default settings
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct Defaults {
    pub bucket: BucketSettings,
}

/// BucketLists
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct BucketInfoList {
    pub buckets: Vec<BucketInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_info_deserializes_without_instance_identity() {
        let mut value = serde_json::to_value(ServerInfo {
            instance_name: "edge-a".to_string(),
            instance_role: "PRIMARY".to_string(),
            ..ServerInfo::default()
        })
        .unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("instance_name");
        object.remove("instance_role");

        let info: ServerInfo = serde_json::from_value(value).unwrap();
        assert_eq!(info.instance_name, "");
        assert_eq!(info.instance_role, "");
    }

    #[test]
    fn server_info_omits_archive_when_not_configured() {
        let value = serde_json::to_value(ServerInfo::default()).unwrap();
        assert!(value.get("archive").is_none());

        let info: ServerInfo = serde_json::from_value(value).unwrap();
        assert_eq!(info.archive, None);
    }

    #[test]
    fn archive_info_deserializes_partial_payload() {
        let info: ServerInfo = serde_json::from_value(serde_json::json!({
            "version": "1.21.0",
            "bucket_count": 0,
            "usage": 0,
            "uptime": 0,
            "oldest_record": 0,
            "latest_record": 0,
            "defaults": {"bucket": {}},
            "license": null,
            "archive": {"path": "/mnt/nas", "online": true, "archived_usage": 42}
        }))
        .unwrap();

        let archive = info.archive.unwrap();
        assert_eq!(archive.path, "/mnt/nas");
        assert!(archive.online);
        assert_eq!(archive.archived_usage, 42);
        assert_eq!(archive.local_size_limit, None);
    }
}
