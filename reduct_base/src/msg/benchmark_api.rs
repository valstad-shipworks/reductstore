// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::msg::bucket_api::BucketSettings;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct BenchmarkList {
    pub benchmarks: Vec<BenchmarkDescriptor>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct BenchmarkDescriptor {
    pub name: String,
    pub path: String,
    pub description: String,
    pub defaults: Value,
    pub limits: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct BenchmarkStatus {
    pub busy: bool,
    pub running: Option<String>,
    pub started_at: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct LatencyStats {
    pub p50_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    pub mean_us: u64,
    pub samples: u64,
}

impl LatencyStats {
    pub fn from_micros(mut samples: Vec<u64>) -> Self {
        if samples.is_empty() {
            return LatencyStats::default();
        }
        samples.sort_unstable();
        let len = samples.len();
        let sum: u128 = samples.iter().map(|v| *v as u128).sum();
        LatencyStats {
            p50_us: samples[len * 50 / 100],
            p99_us: samples[(len * 99 / 100).min(len - 1)],
            max_us: samples[len - 1],
            mean_us: (sum / len as u128) as u64,
            samples: len as u64,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct PhaseResult {
    pub elapsed_us: u64,
    pub bytes: u64,
    pub ops: u64,
    pub mb_per_sec: f64,
    pub ops_per_sec: f64,
    pub latency: LatencyStats,
}

impl PhaseResult {
    pub fn new(elapsed_us: u64, bytes: u64, ops: u64, latency_samples_us: Vec<u64>) -> Self {
        let secs = elapsed_us as f64 / 1_000_000.0;
        let (mb_per_sec, ops_per_sec) = if elapsed_us == 0 {
            (0.0, 0.0)
        } else {
            (bytes as f64 / 1_000_000.0 / secs, ops as f64 / secs)
        };
        PhaseResult {
            elapsed_us,
            bytes,
            ops,
            mb_per_sec,
            ops_per_sec,
            latency: LatencyStats::from_micros(latency_samples_us),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DiskFsyncMode {
    #[default]
    None,
    End,
    Each,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(default)]
pub struct DiskBenchmarkRequest {
    pub block_size: u64,
    pub total_size: u64,
    pub fsync: DiskFsyncMode,
    pub read_sequential: bool,
    pub random_reads: u64,
    pub random_read_size: u64,
    pub keep_file: bool,
}

impl Default for DiskBenchmarkRequest {
    fn default() -> Self {
        DiskBenchmarkRequest {
            block_size: 1024 * 1024,
            total_size: 256 * 1024 * 1024,
            fsync: DiskFsyncMode::None,
            read_sequential: true,
            random_reads: 10_000,
            random_read_size: 4096,
            keep_file: false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct DiskBenchmarkResult {
    pub path: String,
    pub block_size: u64,
    pub total_size: u64,
    pub fsync: DiskFsyncMode,
    pub write: PhaseResult,
    pub fsync_us: Option<u64>,
    pub read_sequential: Option<PhaseResult>,
    pub random_read: Option<PhaseResult>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(default)]
pub struct IngestBenchmarkRequest {
    pub record_size: u64,
    pub record_count: u64,
    pub concurrency: usize,
    pub entries: Option<usize>,
    pub labels: usize,
    pub content_type: String,
    pub sync_at_end: bool,
    pub keep_bucket: bool,
    pub bucket_settings: Option<BucketSettings>,
}

impl Default for IngestBenchmarkRequest {
    fn default() -> Self {
        IngestBenchmarkRequest {
            record_size: 100_000,
            record_count: 10_000,
            concurrency: 1,
            entries: None,
            labels: 0,
            content_type: "application/octet-stream".to_string(),
            sync_at_end: true,
            keep_bucket: false,
            bucket_settings: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct IngestBenchmarkResult {
    pub bucket: String,
    pub record_size: u64,
    pub record_count: u64,
    pub concurrency: usize,
    pub entries: usize,
    pub write: PhaseResult,
    pub begin_write_latency: LatencyStats,
    pub sync_us: Option<u64>,
    pub errors: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReadBenchmarkMode {
    Point,
    Query,
    #[default]
    Both,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(default)]
pub struct ReadBenchmarkRequest {
    pub bucket: Option<String>,
    pub entry: Option<String>,
    pub record_size: u64,
    pub record_count: u64,
    pub concurrency: usize,
    pub mode: ReadBenchmarkMode,
    pub random_order: bool,
}

impl Default for ReadBenchmarkRequest {
    fn default() -> Self {
        ReadBenchmarkRequest {
            bucket: None,
            entry: None,
            record_size: 100_000,
            record_count: 10_000,
            concurrency: 1,
            mode: ReadBenchmarkMode::Both,
            random_order: true,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct ReadBenchmarkResult {
    pub bucket: String,
    pub entry: String,
    pub prepared: bool,
    pub prepare_us: u64,
    pub point_read: Option<PhaseResult>,
    pub query: Option<PhaseResult>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn disk_request_defaults_from_empty_json() {
        let req: DiskBenchmarkRequest = serde_json::from_str("{}").unwrap();
        assert_eq!(req, DiskBenchmarkRequest::default());
        assert_eq!(req.block_size, 1024 * 1024);
        assert_eq!(req.fsync, DiskFsyncMode::None);
    }

    #[rstest]
    fn fsync_mode_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&DiskFsyncMode::Each).unwrap(),
            "\"each\""
        );
        let req: DiskBenchmarkRequest = serde_json::from_str(r#"{"fsync":"end"}"#).unwrap();
        assert_eq!(req.fsync, DiskFsyncMode::End);
    }

    #[rstest]
    fn ingest_request_defaults() {
        let req: IngestBenchmarkRequest = serde_json::from_str(r#"{"record_count": 5}"#).unwrap();
        assert_eq!(req.record_count, 5);
        assert_eq!(req.record_size, 100_000);
        assert_eq!(req.concurrency, 1);
        assert_eq!(req.entries, None);
        assert!(req.sync_at_end);
    }

    #[rstest]
    fn latency_stats_empty() {
        assert_eq!(LatencyStats::from_micros(vec![]), LatencyStats::default());
    }

    #[rstest]
    fn latency_stats_single_sample() {
        let stats = LatencyStats::from_micros(vec![7]);
        assert_eq!(stats.p50_us, 7);
        assert_eq!(stats.p99_us, 7);
        assert_eq!(stats.max_us, 7);
        assert_eq!(stats.mean_us, 7);
        assert_eq!(stats.samples, 1);
    }

    #[rstest]
    fn latency_stats_percentiles() {
        let mut samples: Vec<u64> = (1..=100).collect();
        samples.reverse();
        let stats = LatencyStats::from_micros(samples);
        assert_eq!(stats.p50_us, 51);
        assert_eq!(stats.p99_us, 100);
        assert_eq!(stats.max_us, 100);
        assert_eq!(stats.mean_us, 50);
        assert_eq!(stats.samples, 100);
    }

    #[rstest]
    fn phase_result_rates() {
        let phase = PhaseResult::new(2_000_000, 4_000_000, 10, vec![1, 2, 3]);
        assert_eq!(phase.mb_per_sec, 2.0);
        assert_eq!(phase.ops_per_sec, 5.0);
        assert_eq!(phase.latency.samples, 3);

        let zero = PhaseResult::new(0, 10, 1, vec![]);
        assert_eq!(zero.mb_per_sec, 0.0);
        assert_eq!(zero.ops_per_sec, 0.0);
    }

    #[rstest]
    fn read_request_mode_roundtrip() {
        let req: ReadBenchmarkRequest = serde_json::from_str(r#"{"mode":"point"}"#).unwrap();
        assert_eq!(req.mode, ReadBenchmarkMode::Point);
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"mode\":\"point\""));
    }
}
