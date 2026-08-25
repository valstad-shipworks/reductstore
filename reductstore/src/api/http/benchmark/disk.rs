// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::benchmark::{
    run_phase, validate_disk_request, BenchmarkGuard, DiskBenchmarkRequestAxum,
    DiskBenchmarkResultAxum,
};
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::State;
use axum_extra::headers::HeaderMap;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::msg::benchmark_api::{
    DiskBenchmarkRequest, DiskBenchmarkResult, DiskFsyncMode, PhaseResult,
};
use std::fs::{create_dir_all, remove_dir_all, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

pub(super) const BENCHMARK_DIR: &str = ".benchmark";

// POST /benchmark/disk
pub(super) async fn run_disk_benchmark(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    DiskBenchmarkRequestAxum(request): DiskBenchmarkRequestAxum,
) -> Result<DiskBenchmarkResultAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    let cfg = &components.cfg.benchmark_api;
    let data_path = components.storage.data_path().clone();

    let available = fs4::available_space(&data_path).map_err(|err| {
        internal_server_error!("Failed to query free space of {:?}: {}", data_path, err)
    })?;
    validate_disk_request(&request, cfg, available)?;

    let guard = BenchmarkGuard::try_acquire("disk")?;
    let dir = data_path
        .join(BENCHMARK_DIR)
        .join(uuid::Uuid::new_v4().simple().to_string());

    let handle = tokio::task::spawn_blocking(move || {
        let _guard = guard;
        run_blocking(dir, request)
    });
    let result = run_phase(cfg, "disk", async {
        handle
            .await
            .map_err(|err| {
                HttpError::from(internal_server_error!(
                    "Disk benchmark task failed: {}",
                    err
                ))
            })?
            .map_err(HttpError::from)
    })
    .await?;
    Ok(result.into())
}

struct BenchDir {
    path: PathBuf,
    keep: bool,
}

impl Drop for BenchDir {
    fn drop(&mut self) {
        if !self.keep {
            let _ = remove_dir_all(&self.path);
        }
    }
}

fn run_blocking(
    dir: PathBuf,
    request: DiskBenchmarkRequest,
) -> Result<DiskBenchmarkResult, ReductError> {
    create_dir_all(&dir).map_err(|err| io_error("create", &dir, err))?;
    let bench_dir = BenchDir {
        path: dir,
        keep: request.keep_file,
    };
    let file_path = bench_dir.path.join("bench.bin");

    let block_size = request.block_size as usize;
    let blocks = request.total_size / request.block_size;
    let mut buffer = vec![0u8; block_size];
    rand::fill(&mut buffer[..]);

    let mut file = File::create(&file_path).map_err(|err| io_error("create", &file_path, err))?;
    let mut latencies = Vec::with_capacity(blocks as usize);
    let write_start = Instant::now();
    for _ in 0..blocks {
        let t0 = Instant::now();
        file.write_all(&buffer)
            .map_err(|err| io_error("write", &file_path, err))?;
        if request.fsync == DiskFsyncMode::Each {
            file.sync_all()
                .map_err(|err| io_error("fsync", &file_path, err))?;
        }
        latencies.push(t0.elapsed().as_micros() as u64);
    }
    let write_elapsed = write_start.elapsed().as_micros() as u64;

    let fsync_us = if request.fsync == DiskFsyncMode::End {
        let t0 = Instant::now();
        file.sync_all()
            .map_err(|err| io_error("fsync", &file_path, err))?;
        Some(t0.elapsed().as_micros() as u64)
    } else {
        None
    };
    drop(file);

    let write = PhaseResult::new(write_elapsed, request.total_size, blocks, latencies);

    let read_sequential = if request.read_sequential {
        let mut file = File::open(&file_path).map_err(|err| io_error("open", &file_path, err))?;
        let mut latencies = Vec::with_capacity(blocks as usize);
        let start = Instant::now();
        for _ in 0..blocks {
            let t0 = Instant::now();
            file.read_exact(&mut buffer)
                .map_err(|err| io_error("read", &file_path, err))?;
            latencies.push(t0.elapsed().as_micros() as u64);
        }
        Some(PhaseResult::new(
            start.elapsed().as_micros() as u64,
            request.total_size,
            blocks,
            latencies,
        ))
    } else {
        None
    };

    let random_read = if request.random_reads > 0 {
        let mut file = File::open(&file_path).map_err(|err| io_error("open", &file_path, err))?;
        let mut chunk = vec![0u8; request.random_read_size as usize];
        let max_offset = request.total_size - request.random_read_size;
        let mut latencies = Vec::with_capacity(request.random_reads as usize);
        let start = Instant::now();
        for _ in 0..request.random_reads {
            let offset = rand::random_range(0..=max_offset);
            let t0 = Instant::now();
            file.seek(SeekFrom::Start(offset))
                .map_err(|err| io_error("seek", &file_path, err))?;
            file.read_exact(&mut chunk)
                .map_err(|err| io_error("read", &file_path, err))?;
            latencies.push(t0.elapsed().as_micros() as u64);
        }
        Some(PhaseResult::new(
            start.elapsed().as_micros() as u64,
            request.random_reads * request.random_read_size,
            request.random_reads,
            latencies,
        ))
    } else {
        None
    };

    Ok(DiskBenchmarkResult {
        path: file_path.to_string_lossy().to_string(),
        block_size: request.block_size,
        total_size: request.total_size,
        fsync: request.fsync,
        write,
        fsync_us,
        read_sequential,
        random_read,
    })
}

fn io_error(action: &str, path: &Path, err: std::io::Error) -> ReductError {
    internal_server_error!("Disk benchmark failed to {} {:?}: {}", action, path, err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::benchmark::tests::serialize_tests;
    use crate::api::http::tests::{headers, keeper};
    use reduct_base::error::ErrorCode;
    use rstest::{fixture, rstest};

    #[fixture]
    fn small_request() -> DiskBenchmarkRequest {
        DiskBenchmarkRequest {
            block_size: 64 * 1024,
            total_size: 1024 * 1024,
            fsync: DiskFsyncMode::None,
            read_sequential: true,
            random_reads: 100,
            random_read_size: 4096,
            keep_file: false,
        }
    }

    async fn benchmark_dir(keeper: &Arc<StateKeeper>) -> PathBuf {
        keeper
            .get_anonymous()
            .await
            .unwrap()
            .storage
            .data_path()
            .join(BENCHMARK_DIR)
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_disk_small_run(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: DiskBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;
        let result = run_disk_benchmark(
            State(Arc::clone(&keeper)),
            headers,
            DiskBenchmarkRequestAxum(small_request),
        )
        .await
        .unwrap()
        .0;

        assert_eq!(result.write.bytes, 1024 * 1024);
        assert_eq!(result.write.ops, 16);
        assert_eq!(result.write.latency.samples, 16);
        assert!(result.write.mb_per_sec > 0.0);
        assert_eq!(result.fsync_us, None);
        let seq = result.read_sequential.unwrap();
        assert_eq!(seq.bytes, 1024 * 1024);
        assert_eq!(seq.ops, 16);
        let random = result.random_read.unwrap();
        assert_eq!(random.ops, 100);
        assert_eq!(random.bytes, 100 * 4096);
        assert_eq!(random.latency.samples, 100);

        let dir = benchmark_dir(&keeper).await;
        let leftovers = std::fs::read_dir(&dir)
            .map(|entries| entries.count())
            .unwrap_or(0);
        assert_eq!(leftovers, 0, "benchmark dir must be cleaned up");
        assert!(result.path.starts_with(dir.to_str().unwrap()));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_disk_fsync_modes(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: DiskBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;

        let each = run_disk_benchmark(
            State(Arc::clone(&keeper)),
            headers.clone(),
            DiskBenchmarkRequestAxum(DiskBenchmarkRequest {
                fsync: DiskFsyncMode::Each,
                random_reads: 0,
                ..small_request.clone()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(each.fsync_us, None);
        assert_eq!(each.fsync, DiskFsyncMode::Each);
        assert_eq!(each.write.latency.samples, each.write.ops);

        let end = run_disk_benchmark(
            State(keeper),
            headers,
            DiskBenchmarkRequestAxum(DiskBenchmarkRequest {
                fsync: DiskFsyncMode::End,
                random_reads: 0,
                ..small_request
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(end.fsync_us.is_some());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_disk_random_reads_disabled(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: DiskBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let result = run_disk_benchmark(
            State(keeper.await),
            headers,
            DiskBenchmarkRequestAxum(DiskBenchmarkRequest {
                random_reads: 0,
                read_sequential: false,
                ..small_request
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(result.random_read.is_none());
        assert!(result.read_sequential.is_none());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_disk_keep_file(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: DiskBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let result = run_disk_benchmark(
            State(keeper.await),
            headers,
            DiskBenchmarkRequestAxum(DiskBenchmarkRequest {
                keep_file: true,
                random_reads: 0,
                ..small_request
            }),
        )
        .await
        .unwrap()
        .0;
        let path = PathBuf::from(&result.path);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 1024 * 1024);
        remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_disk_busy_returns_409(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: DiskBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let _running = BenchmarkGuard::try_acquire("ingest").unwrap();
        let err = run_disk_benchmark(
            State(keeper.await),
            headers,
            DiskBenchmarkRequestAxum(small_request),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), ErrorCode::Conflict);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_disk_rejects_oversize(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        small_request: DiskBenchmarkRequest,
    ) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;
        let err = run_disk_benchmark(
            State(Arc::clone(&keeper)),
            headers,
            DiskBenchmarkRequestAxum(DiskBenchmarkRequest {
                total_size: 8 * 1024 * 1024 * 1024,
                ..small_request
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
        assert!(!benchmark_dir(&keeper).await.exists());
        assert!(!BenchmarkGuard::status().busy);
    }
}
