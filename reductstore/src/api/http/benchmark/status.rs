// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::benchmark::{BenchmarkGuard, BenchmarkStatusAxum};
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::State;
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET /benchmark/status
pub(super) async fn benchmark_status(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<BenchmarkStatusAxum, HttpError> {
    keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    Ok(BenchmarkGuard::status().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::benchmark::tests::{read_only_headers, serialize_tests};
    use crate::api::http::tests::{headers, keeper};
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_status_reflects_guard(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let _serial = serialize_tests().await;
        let keeper = keeper.await;

        let status = benchmark_status(State(Arc::clone(&keeper)), headers.clone())
            .await
            .unwrap();
        assert!(!status.0.busy);
        assert_eq!(status.0.running, None);

        let guard = BenchmarkGuard::try_acquire("ingest").unwrap();
        let status = benchmark_status(State(Arc::clone(&keeper)), headers.clone())
            .await
            .unwrap();
        assert!(status.0.busy);
        assert_eq!(status.0.running.as_deref(), Some("ingest"));
        drop(guard);

        let status = benchmark_status(State(keeper), headers).await.unwrap();
        assert!(!status.0.busy);
    }

    #[rstest]
    #[tokio::test]
    async fn test_status_forbidden(#[future] keeper: Arc<StateKeeper>) {
        let keeper = keeper.await;
        let headers = read_only_headers(&keeper).await;
        let err = benchmark_status(State(keeper), headers).await.unwrap_err();
        assert_eq!(err.status(), ErrorCode::Forbidden);
    }
}
