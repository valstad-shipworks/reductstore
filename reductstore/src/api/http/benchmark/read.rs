// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::benchmark::ingest::run_ingest;
use crate::api::http::benchmark::{
    run_phase, validate_concurrency, validate_ingest_request, BenchmarkGuard,
    ReadBenchmarkRequestAxum, ReadBenchmarkResultAxum, TempBucket,
};
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use crate::cfg::benchmark::BenchmarkApiConfig;
use crate::storage::engine::StorageEngine;
use crate::storage::entry::{Entry, RecordReader};
use axum::extract::State;
use axum_extra::headers::HeaderMap;
use rand::seq::SliceRandom;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::ReadRecord;
use reduct_base::msg::benchmark_api::{
    IngestBenchmarkRequest, PhaseResult, ReadBenchmarkMode, ReadBenchmarkRequest,
    ReadBenchmarkResult,
};
use reduct_base::msg::entry_api::{QueryEntry, QueryType};
use reduct_base::{internal_server_error, unprocessable_entity};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

// POST /benchmark/read
pub(super) async fn run_read_benchmark(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    ReadBenchmarkRequestAxum(request): ReadBenchmarkRequestAxum,
) -> Result<ReadBenchmarkResultAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    let cfg = &components.cfg.benchmark_api;
    validate_concurrency(request.concurrency, cfg)?;
    let synth_request = match &request.bucket {
        Some(_) => {
            if request.entry.as_deref().unwrap_or("").is_empty() {
                return Err(unprocessable_entity!("entry is required when bucket is set").into());
            }
            None
        }
        None => {
            let synth = IngestBenchmarkRequest {
                record_size: request.record_size,
                record_count: request.record_count,
                concurrency: request.concurrency,
                entries: Some(request.concurrency),
                sync_at_end: false,
                keep_bucket: true,
                ..Default::default()
            };
            validate_ingest_request(&synth, cfg)?;
            Some(synth)
        }
    };

    let _guard = BenchmarkGuard::try_acquire("read")?;
    let storage = &components.storage;

    let prepare_start = Instant::now();
    let (bucket, targets, temp) = match synth_request {
        Some(synth) => {
            let outcome = run_ingest(
                storage,
                cfg,
                &synth,
                request.concurrency,
                components.cfg.bucket_defaults.clone(),
            )
            .await?;
            let temp = TempBucket::new(storage, &outcome.result.bucket);
            (outcome.result.bucket, outcome.timestamps, Some(temp))
        }
        None => {
            let bucket = request.bucket.clone().unwrap_or_default();
            let entry = request.entry.clone().unwrap_or_default();
            let limit = match request.record_count {
                0 => cfg.max_records,
                count => count.min(cfg.max_records),
            } as usize;
            let timestamps = run_phase(
                cfg,
                "read-prepare",
                collect_timestamps(storage, &bucket, &entry, limit),
            )
            .await?;
            (bucket, vec![(entry, timestamps)], None)
        }
    };
    let prepared = temp.is_some();
    let prepare_us = prepare_start.elapsed().as_micros() as u64;

    let result = measure(storage, cfg, &request, &bucket, targets).await;
    drop(temp);
    let (point_read, query) = result?;

    Ok(ReadBenchmarkResult {
        bucket,
        entry: match request.entry {
            Some(entry) => entry,
            None => format!("bench-0..{}", request.concurrency),
        },
        prepared,
        prepare_us,
        point_read,
        query,
    }
    .into())
}

async fn measure(
    storage: &Arc<StorageEngine>,
    cfg: &BenchmarkApiConfig,
    request: &ReadBenchmarkRequest,
    bucket: &str,
    targets: Vec<(String, Vec<u64>)>,
) -> Result<(Option<PhaseResult>, Option<PhaseResult>), HttpError> {
    let bucket_ref = storage.get_bucket(bucket).await?.upgrade()?;
    let mut entries = Vec::with_capacity(targets.len());
    for (name, _) in &targets {
        entries.push(bucket_ref.get_entry(name).await?.upgrade()?);
    }
    let entries = Arc::new(entries);

    let point_read = if matches!(
        request.mode,
        ReadBenchmarkMode::Point | ReadBenchmarkMode::Both
    ) {
        let mut work: Vec<(usize, u64)> = targets
            .iter()
            .enumerate()
            .flat_map(|(idx, (_, ts))| ts.iter().map(move |ts| (idx, *ts)))
            .collect();
        if request.random_order {
            work.shuffle(&mut rand::rng());
        }
        Some(
            run_phase(
                cfg,
                "point-read",
                point_read_phase(Arc::clone(&entries), work, request.concurrency),
            )
            .await?,
        )
    } else {
        None
    };

    let query = if matches!(
        request.mode,
        ReadBenchmarkMode::Query | ReadBenchmarkMode::Both
    ) {
        Some(run_phase(cfg, "query", query_phase(Arc::clone(&entries))).await?)
    } else {
        None
    };

    Ok((point_read, query))
}

struct ReadStats {
    latencies: Vec<u64>,
    bytes: u64,
}

async fn point_read_phase(
    entries: Arc<Vec<Arc<Entry>>>,
    work: Vec<(usize, u64)>,
    concurrency: usize,
) -> Result<PhaseResult, HttpError> {
    let work = Arc::new(work);
    let mut tasks = JoinSet::new();
    for task_id in 0..concurrency {
        let entries = Arc::clone(&entries);
        let work = Arc::clone(&work);
        tasks.spawn(async move {
            let mut stats = ReadStats {
                latencies: Vec::new(),
                bytes: 0,
            };
            for (entry_idx, ts) in work.iter().skip(task_id).step_by(concurrency) {
                let t0 = Instant::now();
                let mut reader = entries[*entry_idx].begin_read(*ts).await?;
                stats.bytes += drain(&mut reader)?;
                stats.latencies.push(t0.elapsed().as_micros() as u64);
            }
            Ok::<ReadStats, ReductError>(stats)
        });
    }
    let start = Instant::now();
    let stats = join_reads(&mut tasks).await?;
    let elapsed = start.elapsed().as_micros() as u64;
    let ops = stats.latencies.len() as u64;
    Ok(PhaseResult::new(elapsed, stats.bytes, ops, stats.latencies))
}

async fn query_phase(entries: Arc<Vec<Arc<Entry>>>) -> Result<PhaseResult, HttpError> {
    let mut tasks = JoinSet::new();
    for entry in entries.iter() {
        let entry = Arc::clone(entry);
        tasks.spawn(async move {
            let mut stats = ReadStats {
                latencies: Vec::new(),
                bytes: 0,
            };
            let query_id = match entry
                .query(QueryEntry {
                    query_type: QueryType::Query,
                    start: Some(0),
                    stop: Some(u64::MAX),
                    ..Default::default()
                })
                .await
            {
                Ok(id) => id,
                Err(err) if err.status() == ErrorCode::NoContent => return Ok(stats),
                Err(err) => return Err(err),
            };
            let (rx, _) = entry.get_query_receiver(query_id).await?;
            loop {
                let t0 = Instant::now();
                let next = {
                    let rc = rx.upgrade()?;
                    let mut rx = rc.write().await?;
                    rx.recv().await
                };
                match next {
                    Some(Ok(mut reader)) => {
                        stats.bytes += drain(&mut reader)?;
                        stats.latencies.push(t0.elapsed().as_micros() as u64);
                    }
                    Some(Err(err)) if err.status() == ErrorCode::NoContent => break,
                    Some(Err(err)) => return Err(err),
                    None => break,
                }
            }
            Ok::<ReadStats, ReductError>(stats)
        });
    }
    let start = Instant::now();
    let stats = join_reads(&mut tasks).await?;
    let elapsed = start.elapsed().as_micros() as u64;
    let ops = stats.latencies.len() as u64;
    Ok(PhaseResult::new(elapsed, stats.bytes, ops, stats.latencies))
}

async fn join_reads(
    tasks: &mut JoinSet<Result<ReadStats, ReductError>>,
) -> Result<ReadStats, HttpError> {
    let mut merged = ReadStats {
        latencies: Vec::new(),
        bytes: 0,
    };
    while let Some(joined) = tasks.join_next().await {
        let stats = joined.map_err(|err| {
            HttpError::from(internal_server_error!("Read task failed: {}", err))
        })??;
        merged.latencies.extend(stats.latencies);
        merged.bytes += stats.bytes;
    }
    Ok(merged)
}

fn drain(reader: &mut RecordReader) -> Result<u64, ReductError> {
    let mut bytes = 0u64;
    while let Some(chunk) = reader.read_chunk() {
        bytes += chunk?.len() as u64;
    }
    Ok(bytes)
}

async fn collect_timestamps(
    storage: &Arc<StorageEngine>,
    bucket: &str,
    entry: &str,
    limit: usize,
) -> Result<Vec<u64>, HttpError> {
    if limit == 0 {
        return Ok(vec![]);
    }
    let entry = storage
        .get_bucket(bucket)
        .await?
        .upgrade()?
        .get_entry(entry)
        .await?
        .upgrade()?;
    let query_id = match entry
        .query(QueryEntry {
            query_type: QueryType::Query,
            start: Some(0),
            stop: Some(u64::MAX),
            only_metadata: Some(true),
            ..Default::default()
        })
        .await
    {
        Ok(id) => id,
        Err(err) if err.status() == ErrorCode::NoContent => return Ok(vec![]),
        Err(err) => return Err(err.into()),
    };
    let (rx, _) = entry.get_query_receiver(query_id).await?;
    let mut timestamps = Vec::new();
    loop {
        let next = {
            let rc = rx.upgrade()?;
            let mut rx = rc.write().await?;
            rx.recv().await
        };
        match next {
            Some(Ok(reader)) => {
                timestamps.push(reader.meta().timestamp());
                if timestamps.len() >= limit {
                    break;
                }
            }
            Some(Err(err)) if err.status() == ErrorCode::NoContent => break,
            Some(Err(err)) => return Err(err.into()),
            None => break,
        }
    }
    Ok(timestamps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::benchmark::ingest::tests::wait_bucket_removed;
    use crate::api::http::benchmark::tests::serialize_tests;
    use crate::api::http::tests::{headers, keeper};
    use rstest::rstest;
    use std::time::Duration;

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_synthesized(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;
        let result = run_read_benchmark(
            State(Arc::clone(&keeper)),
            headers,
            ReadBenchmarkRequestAxum(ReadBenchmarkRequest {
                record_size: 512,
                record_count: 20,
                concurrency: 2,
                mode: ReadBenchmarkMode::Both,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;

        assert!(result.prepared);
        assert!(result.prepare_us > 0);
        let point = result.point_read.unwrap();
        assert_eq!(point.ops, 20);
        assert_eq!(point.bytes, 10240);
        assert_eq!(point.latency.samples, 20);
        let query = result.query.unwrap();
        assert_eq!(query.ops, 20);
        assert_eq!(query.bytes, 10240);

        let storage = Arc::clone(&keeper.get_anonymous().await.unwrap().storage);
        wait_bucket_removed(&storage, &result.bucket).await;
        assert!(!BenchmarkGuard::status().busy);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_existing_bucket_entry(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;
        let result = run_read_benchmark(
            State(Arc::clone(&keeper)),
            headers,
            ReadBenchmarkRequestAxum(ReadBenchmarkRequest {
                bucket: Some("bucket-1".to_string()),
                entry: Some("entry-1".to_string()),
                mode: ReadBenchmarkMode::Point,
                concurrency: 3,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;

        assert!(!result.prepared);
        assert_eq!(result.bucket, "bucket-1");
        assert_eq!(result.entry, "entry-1");
        let point = result.point_read.unwrap();
        assert_eq!(point.ops, 1);
        assert_eq!(point.bytes, 6);
        assert!(result.query.is_none());

        let storage = Arc::clone(&keeper.get_anonymous().await.unwrap().storage);
        assert!(storage.get_bucket("bucket-1").await.is_ok());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_missing_bucket(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let _serial = serialize_tests().await;
        let err = run_read_benchmark(
            State(keeper.await),
            headers,
            ReadBenchmarkRequestAxum(ReadBenchmarkRequest {
                bucket: Some("nope".to_string()),
                entry: Some("entry-1".to_string()),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), ErrorCode::NotFound);
        assert!(!BenchmarkGuard::status().busy);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_requires_entry_with_bucket(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let _serial = serialize_tests().await;
        let err = run_read_benchmark(
            State(keeper.await),
            headers,
            ReadBenchmarkRequestAxum(ReadBenchmarkRequest {
                bucket: Some("bucket-1".to_string()),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_query_ends_on_no_content(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let _serial = serialize_tests().await;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_read_benchmark(
                State(keeper.await),
                headers,
                ReadBenchmarkRequestAxum(ReadBenchmarkRequest {
                    bucket: Some("bucket-1".to_string()),
                    entry: Some("entry-1".to_string()),
                    mode: ReadBenchmarkMode::Query,
                    ..Default::default()
                }),
            ),
        )
        .await
        .expect("query phase must terminate")
        .unwrap()
        .0;
        assert!(result.point_read.is_none());
        let query = result.query.unwrap();
        assert_eq!(query.ops, 1);
        assert_eq!(query.bytes, 6);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_collect_timestamps_respects_limit(#[future] keeper: Arc<StateKeeper>) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let storage = Arc::clone(&components.storage);
        let cfg = &components.cfg.benchmark_api;
        let request = IngestBenchmarkRequest {
            record_size: 10,
            record_count: 20,
            concurrency: 1,
            sync_at_end: false,
            keep_bucket: true,
            ..Default::default()
        };
        let outcome = run_ingest(&storage, cfg, &request, 1, Default::default())
            .await
            .unwrap();
        let bucket = outcome.result.bucket;

        let limited = collect_timestamps(&storage, &bucket, "bench-0", 5)
            .await
            .unwrap();
        assert_eq!(limited, outcome.timestamps[0].1[..5]);
        let all = collect_timestamps(&storage, &bucket, "bench-0", 100)
            .await
            .unwrap();
        assert_eq!(all.len(), 20);
        assert!(collect_timestamps(&storage, &bucket, "bench-0", 0)
            .await
            .unwrap()
            .is_empty());

        storage.remove_bucket(&bucket).await.unwrap();
    }
}
