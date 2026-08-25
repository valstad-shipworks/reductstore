// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::benchmark::{
    now_us, run_phase, validate_ingest_request, BenchmarkGuard, IngestBenchmarkRequestAxum,
    IngestBenchmarkResultAxum, TempBucket,
};
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use crate::cfg::benchmark::BenchmarkApiConfig;
use crate::storage::engine::StorageEngine;
use axum::extract::State;
use axum_extra::headers::HeaderMap;
use bytes::Bytes;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::msg::benchmark_api::{
    IngestBenchmarkRequest, IngestBenchmarkResult, LatencyStats, PhaseResult,
};
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::Labels;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

// POST /benchmark/ingest
pub(super) async fn run_ingest_benchmark(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    IngestBenchmarkRequestAxum(request): IngestBenchmarkRequestAxum,
) -> Result<IngestBenchmarkResultAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    let cfg = &components.cfg.benchmark_api;
    let entries = validate_ingest_request(&request, cfg)?;

    let _guard = BenchmarkGuard::try_acquire("ingest")?;
    let outcome = run_ingest(
        &components.storage,
        cfg,
        &request,
        entries,
        components.cfg.bucket_defaults.clone(),
    )
    .await?;
    Ok(outcome.result.into())
}

pub(super) struct IngestOutcome {
    pub result: IngestBenchmarkResult,
    /// Written timestamps grouped by entry name.
    pub timestamps: Vec<(String, Vec<u64>)>,
}

/// Writes the requested records into a fresh bucket. The bucket is removed
/// afterwards unless `request.keep_bucket` is set, and always on failure or
/// when the request is cancelled. Writer `t` owns every entry `e` with
/// `e % concurrency == t`, so no entry ever sees concurrent writers.
pub(super) async fn run_ingest(
    storage: &Arc<StorageEngine>,
    cfg: &BenchmarkApiConfig,
    request: &IngestBenchmarkRequest,
    entries: usize,
    default_settings: BucketSettings,
) -> Result<IngestOutcome, HttpError> {
    let bucket = format!("benchmark-{}", uuid::Uuid::new_v4().simple());
    let settings = request.bucket_settings.clone().unwrap_or(default_settings);
    storage.create_bucket(&bucket, settings).await?;
    let temp = TempBucket::new(storage, &bucket);

    let outcome = write_records(storage, cfg, request, entries, &bucket).await;
    if request.keep_bucket && outcome.is_ok() {
        temp.keep();
    }
    outcome
}

struct WriterStats {
    total_us: Vec<u64>,
    begin_us: Vec<u64>,
    written: Vec<(usize, u64)>,
    bytes: u64,
    errors: u64,
    first_error: Option<ReductError>,
}

async fn write_records(
    storage: &Arc<StorageEngine>,
    cfg: &BenchmarkApiConfig,
    request: &IngestBenchmarkRequest,
    entries: usize,
    bucket: &str,
) -> Result<IngestOutcome, HttpError> {
    let mut payload = vec![0u8; request.record_size as usize];
    rand::fill(&mut payload[..]);
    let payload = Bytes::from(payload);

    let labels: Labels = (0..request.labels)
        .map(|i| (format!("label-{}", i), format!("value-{}", i)))
        .collect();
    let ts_base = now_us();
    let concurrency = request.concurrency;
    let record_count = request.record_count;

    let phase = async {
        let start = Instant::now();
        let mut tasks = JoinSet::new();
        for task_id in 0..concurrency {
            let storage = Arc::clone(storage);
            let payload = payload.clone();
            let labels = labels.clone();
            let bucket = bucket.to_string();
            let content_type = request.content_type.clone();
            tasks.spawn(async move {
                let mut stats = WriterStats {
                    total_us: Vec::new(),
                    begin_us: Vec::new(),
                    written: Vec::new(),
                    bytes: 0,
                    errors: 0,
                    first_error: None,
                };
                for i in 0..record_count {
                    let entry_idx = (i % entries as u64) as usize;
                    if entry_idx % concurrency != task_id {
                        continue;
                    }
                    let entry = format!("bench-{}", entry_idx);
                    let t0 = Instant::now();
                    let written = async {
                        let mut writer = storage
                            .begin_write(
                                &bucket,
                                &entry,
                                ts_base + i,
                                payload.len() as u64,
                                content_type.clone(),
                                labels.clone(),
                            )
                            .await?;
                        let begin_us = t0.elapsed().as_micros() as u64;
                        writer.send(Ok(Some(payload.clone()))).await?;
                        writer.send(Ok(None)).await?;
                        Ok::<u64, ReductError>(begin_us)
                    }
                    .await;
                    match written {
                        Ok(begin_us) => {
                            stats.total_us.push(t0.elapsed().as_micros() as u64);
                            stats.begin_us.push(begin_us);
                            stats.written.push((entry_idx, ts_base + i));
                            stats.bytes += payload.len() as u64;
                        }
                        Err(err) => {
                            stats.errors += 1;
                            stats.first_error.get_or_insert(err);
                        }
                    }
                }
                stats
            });
        }

        let mut total_us = Vec::with_capacity(record_count as usize);
        let mut begin_us = Vec::with_capacity(record_count as usize);
        let mut timestamps: Vec<(String, Vec<u64>)> = (0..entries)
            .map(|e| (format!("bench-{}", e), Vec::new()))
            .collect();
        let mut bytes = 0u64;
        let mut errors = 0u64;
        let mut first_error = None;
        while let Some(joined) = tasks.join_next().await {
            let stats = joined.map_err(|err| {
                HttpError::from(internal_server_error!("Ingest writer task failed: {}", err))
            })?;
            total_us.extend(stats.total_us);
            begin_us.extend(stats.begin_us);
            for (entry_idx, ts) in stats.written {
                timestamps[entry_idx].1.push(ts);
            }
            bytes += stats.bytes;
            errors += stats.errors;
            if first_error.is_none() {
                first_error = stats.first_error;
            }
        }
        let elapsed_us = start.elapsed().as_micros() as u64;

        if total_us.is_empty() {
            if let Some(err) = first_error {
                return Err(HttpError::from(err));
            }
        }
        for (_, ts) in &mut timestamps {
            ts.sort_unstable();
        }
        Ok((elapsed_us, total_us, begin_us, timestamps, bytes, errors))
    };
    let (elapsed_us, total_us, begin_us, timestamps, bytes, errors) =
        run_phase(cfg, "ingest", phase).await?;

    let sync_us = if request.sync_at_end {
        let t0 = Instant::now();
        run_phase(cfg, "ingest-sync", async {
            storage.sync_fs().await.map_err(HttpError::from)
        })
        .await?;
        Some(t0.elapsed().as_micros() as u64)
    } else {
        None
    };

    let ops = total_us.len() as u64;

    Ok(IngestOutcome {
        result: IngestBenchmarkResult {
            bucket: bucket.to_string(),
            record_size: request.record_size,
            record_count,
            concurrency,
            entries,
            write: PhaseResult::new(elapsed_us, bytes, ops, total_us),
            begin_write_latency: LatencyStats::from_micros(begin_us),
            sync_us,
            errors,
        },
        timestamps,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::api::http::benchmark::tests::serialize_tests;
    use crate::api::http::tests::{headers, keeper, keeper_with_cfg, storage_limited_keeper};
    use crate::cfg::{Cfg, InstanceRole};
    use crate::storage::engine::MAX_IO_BUFFER_SIZE;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::ReadRecord;
    use rstest::{fixture, rstest};
    use std::time::Duration;

    #[fixture]
    fn small_request() -> IngestBenchmarkRequest {
        IngestBenchmarkRequest {
            record_size: 1000,
            record_count: 50,
            concurrency: 4,
            entries: None,
            labels: 2,
            sync_at_end: true,
            keep_bucket: false,
            ..Default::default()
        }
    }

    pub(crate) async fn wait_bucket_removed(storage: &Arc<StorageEngine>, bucket: &str) {
        for _ in 0..600 {
            let list = storage.get_bucket_list().await.unwrap();
            if !list.buckets.iter().any(|b| b.name == bucket) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("bucket '{}' was not removed", bucket);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ingest_writes_and_deletes_bucket(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: IngestBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;
        let result = run_ingest_benchmark(
            State(Arc::clone(&keeper)),
            headers,
            IngestBenchmarkRequestAxum(small_request),
        )
        .await
        .unwrap()
        .0;

        assert_eq!(result.write.ops, 50);
        assert_eq!(result.write.bytes, 50_000);
        assert_eq!(result.write.latency.samples, 50);
        assert_eq!(result.begin_write_latency.samples, 50);
        assert_eq!(result.entries, 4);
        assert_eq!(result.errors, 0);
        assert!(result.sync_us.is_some());
        assert!(result.write.ops_per_sec > 0.0);

        let storage = Arc::clone(&keeper.get_anonymous().await.unwrap().storage);
        wait_bucket_removed(&storage, &result.bucket).await;
        assert!(!BenchmarkGuard::status().busy);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ingest_keep_bucket(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: IngestBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;
        let result = run_ingest_benchmark(
            State(Arc::clone(&keeper)),
            headers,
            IngestBenchmarkRequestAxum(IngestBenchmarkRequest {
                keep_bucket: true,
                entries: Some(6),
                sync_at_end: false,
                ..small_request
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result.sync_us, None);
        assert_eq!(result.entries, 6);

        let storage = Arc::clone(&keeper.get_anonymous().await.unwrap().storage);
        let bucket = storage
            .get_bucket(&result.bucket)
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let info = Arc::clone(&bucket).info().await.unwrap();
        assert_eq!(info.info.entry_count, 6);
        assert_eq!(info.entries.iter().map(|e| e.record_count).sum::<u64>(), 50);

        let entry = bucket
            .get_entry("bench-0")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let ts = entry.info().await.unwrap().oldest_record;
        let mut reader = entry.begin_read(ts).await.unwrap();
        assert_eq!(reader.meta().content_length(), 1000);
        assert_eq!(reader.meta().labels().len(), 2);
        let chunk = reader.read_chunk().unwrap().unwrap();
        assert_eq!(chunk.len(), 1000);

        storage.remove_bucket(&result.bucket).await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ingest_cancelled_removes_bucket(#[future] keeper: Arc<StateKeeper>) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let storage = Arc::clone(&components.storage);
        let request = IngestBenchmarkRequest {
            record_size: 1000,
            record_count: 5000,
            concurrency: 1,
            sync_at_end: false,
            keep_bucket: true,
            ..Default::default()
        };
        let cancelled = tokio::time::timeout(
            Duration::from_millis(20),
            run_ingest(
                &storage,
                &components.cfg.benchmark_api,
                &request,
                1,
                components.cfg.bucket_defaults.clone(),
            ),
        )
        .await;
        assert!(
            cancelled.is_err(),
            "ingest must still be running when cancelled"
        );

        for _ in 0..600 {
            let list = storage.get_bucket_list().await.unwrap();
            if !list
                .buckets
                .iter()
                .any(|b| b.name.starts_with("benchmark-"))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("benchmark bucket was not removed after cancellation");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ingest_large_record_path(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;
        let result = run_ingest_benchmark(
            State(Arc::clone(&keeper)),
            headers,
            IngestBenchmarkRequestAxum(IngestBenchmarkRequest {
                record_size: MAX_IO_BUFFER_SIZE as u64 + 1,
                record_count: 3,
                concurrency: 1,
                sync_at_end: false,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result.write.ops, 3);
        assert_eq!(result.write.bytes, 3 * (MAX_IO_BUFFER_SIZE as u64 + 1));

        let storage = Arc::clone(&keeper.get_anonymous().await.unwrap().storage);
        wait_bucket_removed(&storage, &result.bucket).await;
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ingest_respects_storage_limit(
        #[future] storage_limited_keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: IngestBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let keeper = storage_limited_keeper.await;
        let err = run_ingest_benchmark(
            State(Arc::clone(&keeper)),
            headers,
            IngestBenchmarkRequestAxum(small_request),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), ErrorCode::InternalServerError);
        assert!(err.message().contains("storage limit exceeded"));
        assert!(!BenchmarkGuard::status().busy);

        let storage = Arc::clone(&keeper.get_anonymous().await.unwrap().storage);
        for _ in 0..600 {
            let list = storage.get_bucket_list().await.unwrap();
            if !list
                .buckets
                .iter()
                .any(|b| b.name.starts_with("benchmark-"))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("benchmark bucket was not removed after failure");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ingest_replica_forbidden(
        headers: HeaderMap,
        small_request: IngestBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let cfg = Cfg {
            data_path: tempfile::tempdir().unwrap().keep(),
            api_token: crate::cfg::ApiToken::Provisioned("init-token".to_string()),
            role: InstanceRole::Replica,
            ..Cfg::default()
        };
        let keeper = keeper_with_cfg(cfg).await;
        let err = run_ingest_benchmark(
            State(keeper),
            headers,
            IngestBenchmarkRequestAxum(small_request),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), ErrorCode::Forbidden);
        assert!(!BenchmarkGuard::status().busy);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ingest_busy_409(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: IngestBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let _running = BenchmarkGuard::try_acquire("disk").unwrap();
        let err = run_ingest_benchmark(
            State(keeper.await),
            headers,
            IngestBenchmarkRequestAxum(small_request),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), ErrorCode::Conflict);
        assert_eq!(err.message(), "Benchmark 'disk' is already running");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ingest_rejects_fewer_entries_than_writers(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: IngestBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let err = run_ingest_benchmark(
            State(keeper.await),
            headers,
            IngestBenchmarkRequestAxum(IngestBenchmarkRequest {
                entries: Some(2),
                ..small_request
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }
}
