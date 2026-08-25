// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod disk;
mod ingest;
mod list;
mod read;
mod status;

use crate::api::http::{HttpError, StateKeeper};
use crate::cfg::benchmark::BenchmarkApiConfig;
use crate::storage::engine::StorageEngine;
use axum::body::Body;
use axum::extract::FromRequest;
use axum::http::Request;
use axum::routing::{get, post};
use axum_extra::headers::HeaderMapExt;
use bytes::Bytes;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::benchmark_api::{
    BenchmarkList, BenchmarkStatus, DiskBenchmarkRequest, DiskBenchmarkResult,
    IngestBenchmarkRequest, IngestBenchmarkResult, ReadBenchmarkRequest, ReadBenchmarkResult,
};
use reduct_base::{conflict, timeout, unprocessable_entity};
use reduct_macros::{IntoResponse, Twin};
use std::future::Future;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, MutexGuard};

pub(crate) const MIN_BLOCK_SIZE: u64 = 4 * 1024;
pub(crate) const MAX_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_RECORD_SIZE: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_LABELS: usize = 64;

static BENCH_LOCK: Mutex<()> = Mutex::const_new(());
static RUNNING: parking_lot::Mutex<Option<(String, u64)>> = parking_lot::Mutex::new(None);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct BenchmarkListAxum(BenchmarkList);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct BenchmarkStatusAxum(BenchmarkStatus);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct DiskBenchmarkResultAxum(DiskBenchmarkResult);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct IngestBenchmarkResultAxum(IngestBenchmarkResult);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct ReadBenchmarkResultAxum(ReadBenchmarkResult);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct DiskBenchmarkRequestAxum(DiskBenchmarkRequest);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct IngestBenchmarkRequestAxum(IngestBenchmarkRequest);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct ReadBenchmarkRequestAxum(ReadBenchmarkRequest);

macro_rules! json_body_request {
    ($axum:ident, $inner:ident) => {
        impl<S> FromRequest<S> for $axum
        where
            Bytes: FromRequest<S>,
            S: Send + Sync,
        {
            type Rejection = HttpError;

            async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
                let bytes = Bytes::from_request(req, state)
                    .await
                    .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid body"))?;
                if bytes.is_empty() {
                    return Ok($axum::from($inner::default()));
                }
                serde_json::from_slice::<$inner>(&bytes)
                    .map($axum::from)
                    .map_err(HttpError::from)
            }
        }
    };
}

json_body_request!(DiskBenchmarkRequestAxum, DiskBenchmarkRequest);
json_body_request!(IngestBenchmarkRequestAxum, IngestBenchmarkRequest);
json_body_request!(ReadBenchmarkRequestAxum, ReadBenchmarkRequest);

/// Serializes benchmark runs: one process runs one benchmark at a time so
/// measurements never overlap.
#[derive(Debug)]
pub(super) struct BenchmarkGuard {
    _lock: MutexGuard<'static, ()>,
}

impl BenchmarkGuard {
    pub(super) fn try_acquire(name: &str) -> Result<Self, HttpError> {
        let lock = BENCH_LOCK.try_lock().map_err(|_| {
            let running = RUNNING
                .lock()
                .as_ref()
                .map(|(name, _)| name.clone())
                .unwrap_or_else(|| "unknown".to_string());
            HttpError::from(conflict!("Benchmark '{}' is already running", running))
        })?;
        *RUNNING.lock() = Some((name.to_string(), now_us()));
        Ok(BenchmarkGuard { _lock: lock })
    }

    pub(super) fn status() -> BenchmarkStatus {
        let running = RUNNING.lock().clone();
        BenchmarkStatus {
            busy: running.is_some(),
            running: running.as_ref().map(|(name, _)| name.clone()),
            started_at: running.map(|(_, started_at)| started_at),
        }
    }
}

impl Drop for BenchmarkGuard {
    fn drop(&mut self) {
        *RUNNING.lock() = None;
    }
}

pub(super) fn create_benchmark_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new()
        .route("/", get(list::list_benchmarks))
        .route("/status", get(status::benchmark_status))
        .route("/disk", post(disk::run_disk_benchmark))
        .route("/ingest", post(ingest::run_ingest_benchmark))
        .route("/read", post(read::run_read_benchmark))
}

pub(super) fn validate_disk_request(
    req: &DiskBenchmarkRequest,
    cfg: &BenchmarkApiConfig,
    available_space: u64,
) -> Result<(), HttpError> {
    if req.block_size < MIN_BLOCK_SIZE || req.block_size > MAX_BLOCK_SIZE {
        return Err(unprocessable_entity!(
            "block_size must be between {} and {} bytes",
            MIN_BLOCK_SIZE,
            MAX_BLOCK_SIZE
        )
        .into());
    }
    if !req.block_size.is_power_of_two() {
        return Err(unprocessable_entity!("block_size must be a power of two").into());
    }
    if req.total_size == 0 || !req.total_size.is_multiple_of(req.block_size) {
        return Err(
            unprocessable_entity!("total_size must be a positive multiple of block_size").into(),
        );
    }
    if req.total_size > cfg.max_total_bytes {
        return Err(unprocessable_entity!(
            "total_size {} exceeds RS_BENCHMARK_MAX_TOTAL_SIZE ({})",
            req.total_size,
            cfg.max_total_bytes
        )
        .into());
    }
    if req.total_size >= available_space {
        return Err(unprocessable_entity!(
            "total_size {} exceeds available disk space ({})",
            req.total_size,
            available_space
        )
        .into());
    }
    if req.random_reads > 0 {
        if req.random_read_size == 0
            || req.random_read_size > req.total_size
            || req.random_read_size > MAX_BLOCK_SIZE
        {
            return Err(unprocessable_entity!(
                "random_read_size must be between 1 and min(total_size, {})",
                MAX_BLOCK_SIZE
            )
            .into());
        }
        if req.random_reads > cfg.max_records {
            return Err(unprocessable_entity!(
                "random_reads {} exceeds RS_BENCHMARK_MAX_RECORDS ({})",
                req.random_reads,
                cfg.max_records
            )
            .into());
        }
    }
    Ok(())
}

/// Removes a benchmark bucket when dropped, so a cancelled request (client
/// disconnect) still cleans up. Removal is spawned because Drop cannot await.
pub(super) struct TempBucket {
    storage: Arc<StorageEngine>,
    name: String,
    keep: bool,
}

impl TempBucket {
    pub(super) fn new(storage: &Arc<StorageEngine>, name: &str) -> Self {
        TempBucket {
            storage: Arc::clone(storage),
            name: name.to_string(),
            keep: false,
        }
    }

    pub(super) fn keep(mut self) {
        self.keep = true;
    }
}

impl Drop for TempBucket {
    fn drop(&mut self) {
        if self.keep {
            return;
        }
        let storage = Arc::clone(&self.storage);
        let name = std::mem::take(&mut self.name);
        tokio::spawn(async move {
            if let Err(err) = storage.remove_bucket(&name).await {
                log::warn!("Failed to remove benchmark bucket '{}': {}", name, err);
            }
        });
    }
}

/// Returns the number of entries to spread writers over.
pub(super) fn validate_ingest_request(
    req: &IngestBenchmarkRequest,
    cfg: &BenchmarkApiConfig,
) -> Result<usize, HttpError> {
    if req.record_size == 0 {
        return Err(unprocessable_entity!("record_size must be positive").into());
    }
    if req.record_size > MAX_RECORD_SIZE {
        return Err(
            unprocessable_entity!("record_size must not exceed {} bytes", MAX_RECORD_SIZE).into(),
        );
    }
    if req.labels > MAX_LABELS {
        return Err(unprocessable_entity!("labels must not exceed {}", MAX_LABELS).into());
    }
    if req.record_count == 0 {
        return Err(unprocessable_entity!("record_count must be positive").into());
    }
    if req.record_count > cfg.max_records {
        return Err(unprocessable_entity!(
            "record_count {} exceeds RS_BENCHMARK_MAX_RECORDS ({})",
            req.record_count,
            cfg.max_records
        )
        .into());
    }
    let total = req.record_size.saturating_mul(req.record_count);
    if total > cfg.max_total_bytes {
        return Err(unprocessable_entity!(
            "record_size * record_count ({}) exceeds RS_BENCHMARK_MAX_TOTAL_SIZE ({})",
            total,
            cfg.max_total_bytes
        )
        .into());
    }
    validate_concurrency(req.concurrency, cfg)?;
    let entries = req.entries.unwrap_or(req.concurrency);
    if entries == 0 {
        return Err(unprocessable_entity!("entries must be positive").into());
    }
    if entries < req.concurrency {
        return Err(unprocessable_entity!(
            "entries must be >= concurrency: one writer per entry until same-entry concurrency is fixed"
        )
        .into());
    }
    if entries as u64 > req.record_count {
        return Err(unprocessable_entity!("entries must not exceed record_count").into());
    }
    Ok(entries)
}

pub(super) fn validate_concurrency(
    concurrency: usize,
    cfg: &BenchmarkApiConfig,
) -> Result<(), HttpError> {
    if concurrency == 0 || concurrency > cfg.max_concurrency {
        return Err(unprocessable_entity!(
            "concurrency must be between 1 and RS_BENCHMARK_MAX_CONCURRENCY ({})",
            cfg.max_concurrency
        )
        .into());
    }
    Ok(())
}

pub(super) async fn run_phase<T, F>(
    cfg: &BenchmarkApiConfig,
    phase: &str,
    fut: F,
) -> Result<T, HttpError>
where
    F: Future<Output = Result<T, HttpError>>,
{
    match tokio::time::timeout(cfg.max_duration, fut).await {
        Ok(result) => result,
        Err(_) => Err(timeout!(
            "Benchmark phase '{}' exceeded RS_BENCHMARK_MAX_DURATION ({} s)",
            phase,
            cfg.max_duration.as_secs()
        )
        .into()),
    }
}

pub(super) fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use std::time::Duration;

    static TEST_SERIAL: Mutex<()> = Mutex::const_new(());

    /// Handler tests share the process-wide BENCH_LOCK, so they must not run
    /// concurrently with each other.
    pub(crate) async fn serialize_tests() -> MutexGuard<'static, ()> {
        TEST_SERIAL.lock().await
    }

    pub(crate) async fn read_only_headers(keeper: &Arc<StateKeeper>) -> axum::http::HeaderMap {
        use reduct_base::msg::token_api::{Permissions, TokenCreateRequest};

        let components = keeper.get_anonymous().await.unwrap();
        let token = components
            .token_repo
            .write()
            .await
            .unwrap()
            .generate_token(
                "bench-read-only",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: false,
                        read: vec!["bucket-1".to_string()],
                        write: vec![],
                    },
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await
            .unwrap();
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "Authorization",
            axum::http::HeaderValue::from_str(&format!("Bearer {}", token.value)).unwrap(),
        );
        headers
    }

    #[fixture]
    pub(crate) fn bench_cfg() -> BenchmarkApiConfig {
        BenchmarkApiConfig {
            enabled: true,
            max_total_bytes: 64 * 1024 * 1024,
            max_records: 10_000,
            max_concurrency: 8,
            max_duration: Duration::from_secs(60),
        }
    }

    mod from_request {
        use super::*;
        use futures::stream;
        use reduct_base::error::ErrorCode::UnprocessableEntity;
        use std::io;

        #[rstest]
        #[tokio::test]
        async fn disk_request_valid() {
            let req = Request::builder()
                .body(Body::from(r#"{"block_size": 65536, "fsync": "each"}"#))
                .unwrap();
            let body = DiskBenchmarkRequestAxum::from_request(req, &())
                .await
                .unwrap();
            assert_eq!(body.0.block_size, 65536);
            assert_eq!(
                body.0.fsync,
                reduct_base::msg::benchmark_api::DiskFsyncMode::Each
            );
            assert_eq!(body.0.total_size, 256 * 1024 * 1024);
        }

        #[rstest]
        #[tokio::test]
        async fn empty_body_defaults() {
            let req = Request::builder().body(Body::empty()).unwrap();
            let body = IngestBenchmarkRequestAxum::from_request(req, &())
                .await
                .unwrap();
            assert_eq!(body.0, IngestBenchmarkRequest::default());

            let req = Request::builder().body(Body::from("{}")).unwrap();
            let body = ReadBenchmarkRequestAxum::from_request(req, &())
                .await
                .unwrap();
            assert_eq!(body.0, ReadBenchmarkRequest::default());
        }

        #[rstest]
        #[tokio::test]
        async fn malformed_json() {
            let req = Request::builder().body(Body::from("{bad")).unwrap();
            let err = DiskBenchmarkRequestAxum::from_request(req, &())
                .await
                .unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);

            let req = Request::builder()
                .body(Body::from(r#"{"mode": "sideways"}"#))
                .unwrap();
            let err = ReadBenchmarkRequestAxum::from_request(req, &())
                .await
                .unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
        }

        #[rstest]
        #[tokio::test]
        async fn stream_error() {
            let stream = stream::once(async { Err::<Bytes, _>(io::Error::other("boom")) });
            let req = Request::builder().body(Body::from_stream(stream)).unwrap();
            let err = IngestBenchmarkRequestAxum::from_request(req, &())
                .await
                .unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
            assert_eq!(err.message(), "Invalid body");
        }
    }

    mod validation {
        use super::*;
        use reduct_base::error::ErrorCode::UnprocessableEntity;

        #[rstest]
        fn disk_ok(bench_cfg: BenchmarkApiConfig) {
            let req = DiskBenchmarkRequest {
                block_size: 64 * 1024,
                total_size: 1024 * 1024,
                ..Default::default()
            };
            validate_disk_request(&req, &bench_cfg, u64::MAX).unwrap();
        }

        #[rstest]
        #[case::too_small(1024, 1024 * 1024, 100, "block_size must be between")]
        #[case::too_large(128 * 1024 * 1024, 256 * 1024 * 1024, 100, "block_size must be between")]
        #[case::not_pow2(12 * 1024, 120 * 1024, 100, "power of two")]
        #[case::not_multiple(64 * 1024, 100 * 1024, 100, "multiple of block_size")]
        #[case::zero_total(64 * 1024, 0, 100, "multiple of block_size")]
        #[case::over_cap(1024 * 1024, 128 * 1024 * 1024, 100, "RS_BENCHMARK_MAX_TOTAL_SIZE")]
        #[case::too_many_random(64 * 1024, 1024 * 1024, 20_000, "RS_BENCHMARK_MAX_RECORDS")]
        fn disk_rejects(
            bench_cfg: BenchmarkApiConfig,
            #[case] block_size: u64,
            #[case] total_size: u64,
            #[case] random_reads: u64,
            #[case] expected: &str,
        ) {
            let req = DiskBenchmarkRequest {
                block_size,
                total_size,
                random_reads,
                ..Default::default()
            };
            let err = validate_disk_request(&req, &bench_cfg, u64::MAX).unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
            assert!(
                err.message().contains(expected),
                "unexpected message: {}",
                err.message()
            );
        }

        #[rstest]
        fn disk_rejects_when_disk_full(bench_cfg: BenchmarkApiConfig) {
            let req = DiskBenchmarkRequest {
                block_size: 64 * 1024,
                total_size: 1024 * 1024,
                ..Default::default()
            };
            let err = validate_disk_request(&req, &bench_cfg, 1024 * 1024).unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
            assert!(err.message().contains("available disk space"));
        }

        #[rstest]
        #[case::zero(1024 * 1024, 0)]
        #[case::over_total(1024 * 1024, 2 * 1024 * 1024)]
        #[case::over_block_cap(64 * 1024 * 1024, MAX_BLOCK_SIZE + 64 * 1024)]
        fn disk_rejects_bad_random_read_size(
            bench_cfg: BenchmarkApiConfig,
            #[case] total_size: u64,
            #[case] random_read_size: u64,
        ) {
            let req = DiskBenchmarkRequest {
                block_size: 64 * 1024,
                total_size,
                random_read_size,
                ..Default::default()
            };
            let err = validate_disk_request(&req, &bench_cfg, u64::MAX).unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
            assert!(err.message().contains("random_read_size"));
        }

        #[rstest]
        fn ingest_ok_defaults_entries_to_concurrency(bench_cfg: BenchmarkApiConfig) {
            let req = IngestBenchmarkRequest {
                record_size: 100,
                record_count: 100,
                concurrency: 4,
                ..Default::default()
            };
            assert_eq!(validate_ingest_request(&req, &bench_cfg).unwrap(), 4);
        }

        #[rstest]
        #[case::zero_size(0, 10, 1, None, "record_size")]
        #[case::huge_size(MAX_RECORD_SIZE + 1, 1, 1, None, "record_size must not exceed")]
        #[case::zero_count(10, 0, 1, None, "record_count must be positive")]
        #[case::too_many_records(10, 20_000, 1, None, "RS_BENCHMARK_MAX_RECORDS")]
        #[case::over_total(1024 * 1024, 100, 1, None, "RS_BENCHMARK_MAX_TOTAL_SIZE")]
        #[case::zero_concurrency(10, 10, 0, None, "concurrency must be between")]
        #[case::too_much_concurrency(10, 10, 9, None, "concurrency must be between")]
        #[case::zero_entries(10, 10, 1, Some(0), "entries must be positive")]
        #[case::fewer_entries(10, 10, 4, Some(2), "entries must be >= concurrency")]
        #[case::more_entries_than_records(10, 2, 1, Some(3), "exceed record_count")]
        fn ingest_rejects(
            bench_cfg: BenchmarkApiConfig,
            #[case] record_size: u64,
            #[case] record_count: u64,
            #[case] concurrency: usize,
            #[case] entries: Option<usize>,
            #[case] expected: &str,
        ) {
            let req = IngestBenchmarkRequest {
                record_size,
                record_count,
                concurrency,
                entries,
                ..Default::default()
            };
            let err = validate_ingest_request(&req, &bench_cfg).unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
            assert!(
                err.message().contains(expected),
                "unexpected message: {}",
                err.message()
            );
        }

        #[rstest]
        fn ingest_rejects_too_many_labels(bench_cfg: BenchmarkApiConfig) {
            let req = IngestBenchmarkRequest {
                record_size: 10,
                record_count: 10,
                concurrency: 1,
                labels: MAX_LABELS + 1,
                ..Default::default()
            };
            let err = validate_ingest_request(&req, &bench_cfg).unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
            assert!(err.message().contains("labels must not exceed"));
        }
    }

    mod guard {
        use super::*;
        use reduct_base::error::ErrorCode::Conflict;

        #[rstest]
        #[tokio::test]
        async fn second_acquire_conflicts_until_dropped() {
            let _serial = serialize_tests().await;
            let first = BenchmarkGuard::try_acquire("disk").unwrap();
            let status = BenchmarkGuard::status();
            assert!(status.busy);
            assert_eq!(status.running.as_deref(), Some("disk"));
            assert!(status.started_at.is_some());

            let err = BenchmarkGuard::try_acquire("ingest").unwrap_err();
            assert_eq!(err.status(), Conflict);
            assert_eq!(err.message(), "Benchmark 'disk' is already running");

            drop(first);
            assert!(!BenchmarkGuard::status().busy);
            let _second = BenchmarkGuard::try_acquire("ingest").unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn phase_timeout_maps_to_timeout_error() {
            let cfg = BenchmarkApiConfig {
                max_duration: Duration::from_millis(10),
                ..bench_cfg()
            };
            let err = run_phase(&cfg, "sleep", async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok::<(), HttpError>(())
            })
            .await
            .unwrap_err();
            assert_eq!(err.status(), ErrorCode::Timeout);
        }
    }
}
