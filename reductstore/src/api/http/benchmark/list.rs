// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::benchmark::{BenchmarkListAxum, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE};
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use crate::cfg::benchmark::BenchmarkApiConfig;
use axum::extract::State;
use axum_extra::headers::HeaderMap;
use reduct_base::msg::benchmark_api::{
    BenchmarkDescriptor, BenchmarkList, DiskBenchmarkRequest, IngestBenchmarkRequest,
    ReadBenchmarkRequest,
};
use serde_json::json;
use std::sync::Arc;

// GET /benchmark/
pub(super) async fn list_benchmarks(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<BenchmarkListAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    Ok(describe(&components.cfg.benchmark_api).into())
}

fn describe(cfg: &BenchmarkApiConfig) -> BenchmarkList {
    let common_limits = json!({
        "max_total_bytes": cfg.max_total_bytes,
        "max_records": cfg.max_records,
        "max_concurrency": cfg.max_concurrency,
        "max_duration_secs": cfg.max_duration.as_secs(),
    });
    BenchmarkList {
        benchmarks: vec![
            BenchmarkDescriptor {
                name: "disk".to_string(),
                path: "disk".to_string(),
                description: "Raw file I/O under the data path: sequential write with optional fsync, sequential read and random reads".to_string(),
                defaults: json!(DiskBenchmarkRequest::default()),
                limits: json!({
                    "max_total_bytes": cfg.max_total_bytes,
                    "min_block_size": MIN_BLOCK_SIZE,
                    "max_block_size": MAX_BLOCK_SIZE,
                    "max_random_reads": cfg.max_records,
                    "max_duration_secs": cfg.max_duration.as_secs(),
                }),
            },
            BenchmarkDescriptor {
                name: "ingest".to_string(),
                path: "ingest".to_string(),
                description: "Write records through the storage engine into a temporary bucket with N concurrent writers, one entry per writer".to_string(),
                defaults: json!(IngestBenchmarkRequest::default()),
                limits: common_limits.clone(),
            },
            BenchmarkDescriptor {
                name: "read".to_string(),
                path: "read".to_string(),
                description: "Point reads and a full query over an existing entry or a synthesized temporary bucket".to_string(),
                defaults: json!(ReadBenchmarkRequest::default()),
                limits: common_limits,
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::benchmark::tests::read_only_headers;
    use crate::api::http::tests::{headers, keeper, not_ready_keeper};
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_list_ok(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let list = list_benchmarks(State(keeper.await), headers).await.unwrap();
        let names: Vec<&str> = list.0.benchmarks.iter().map(|b| b.name.as_str()).collect();
        assert_eq!(names, vec!["disk", "ingest", "read"]);
        assert_eq!(
            list.0.benchmarks[0].limits["min_block_size"],
            MIN_BLOCK_SIZE
        );
        assert_eq!(list.0.benchmarks[1].defaults["concurrency"], 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_forbidden(#[future] keeper: Arc<StateKeeper>) {
        let keeper = keeper.await;
        let headers = read_only_headers(&keeper).await;
        let err = list_benchmarks(State(keeper), headers).await.unwrap_err();
        assert_eq!(err.status(), ErrorCode::Forbidden);
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_not_ready(#[future] not_ready_keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = list_benchmarks(State(not_ready_keeper.await), headers)
            .await
            .unwrap_err();
        assert_eq!(err.status(), ErrorCode::ServiceUnavailable);
    }
}
