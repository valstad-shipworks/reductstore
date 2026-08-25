// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{parse_bool, CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct BenchmarkApiConfig {
    pub enabled: bool,
    pub max_total_bytes: u64,
    pub max_records: u64,
    pub max_concurrency: usize,
    pub max_duration: Duration,
}

const DEFAULT_MAX_TOTAL_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_MAX_RECORDS: u64 = 1_000_000;
const DEFAULT_MAX_CONCURRENCY: usize = 64;
const DEFAULT_MAX_DURATION_SECS: u64 = 300;

impl Default for BenchmarkApiConfig {
    fn default() -> Self {
        BenchmarkApiConfig {
            enabled: false,
            max_total_bytes: DEFAULT_MAX_TOTAL_BYTES,
            max_records: DEFAULT_MAX_RECORDS,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            max_duration: Duration::from_secs(DEFAULT_MAX_DURATION_SECS),
        }
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_benchmark_api_config(env: &mut Env<EnvGetter>) -> BenchmarkApiConfig {
        BenchmarkApiConfig {
            enabled: parse_bool(env.get_optional::<String>("RS_BENCHMARK_API"), false),
            max_total_bytes: env
                .get_optional::<ByteSize>("RS_BENCHMARK_MAX_TOTAL_SIZE")
                .map(|size| size.as_u64())
                .unwrap_or(DEFAULT_MAX_TOTAL_BYTES),
            max_records: env
                .get_optional::<u64>("RS_BENCHMARK_MAX_RECORDS")
                .unwrap_or(DEFAULT_MAX_RECORDS),
            max_concurrency: env
                .get_optional::<usize>("RS_BENCHMARK_MAX_CONCURRENCY")
                .unwrap_or(DEFAULT_MAX_CONCURRENCY),
            max_duration: Duration::from_secs(
                env.get_optional::<u64>("RS_BENCHMARK_MAX_DURATION")
                    .unwrap_or(DEFAULT_MAX_DURATION_SECS),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::MockEnvGetter;
    use mockall::predicate::eq;
    use rstest::rstest;
    use std::env::VarError;

    #[rstest]
    fn defaults_when_env_absent() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::<MockEnvGetter>::parse_benchmark_api_config(&mut Env::new(env_getter));
        assert_eq!(cfg, BenchmarkApiConfig::default());
        assert!(!cfg.enabled);
        assert_eq!(cfg.max_total_bytes, 4 * 1024 * 1024 * 1024);
        assert_eq!(cfg.max_records, 1_000_000);
        assert_eq!(cfg.max_concurrency, 64);
        assert_eq!(cfg.max_duration, Duration::from_secs(300));
    }

    #[rstest]
    #[case("yes", true)]
    #[case("1", true)]
    #[case("true", true)]
    #[case("bogus", false)]
    #[case("0", false)]
    fn parses_enabled_flag(#[case] raw: &str, #[case] expected: bool) {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_BENCHMARK_API"))
            .return_const(Ok(raw.to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = CfgParser::<MockEnvGetter>::parse_benchmark_api_config(&mut Env::new(env_getter));
        assert_eq!(cfg.enabled, expected);
    }

    #[rstest]
    fn parses_caps() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_BENCHMARK_API"))
            .return_const(Ok("true".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_BENCHMARK_MAX_TOTAL_SIZE"))
            .return_const(Ok("1GB".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_BENCHMARK_MAX_RECORDS"))
            .return_const(Ok("500".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_BENCHMARK_MAX_CONCURRENCY"))
            .return_const(Ok("8".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_BENCHMARK_MAX_DURATION"))
            .return_const(Ok("30".to_string()));

        let expected = BenchmarkApiConfig {
            enabled: true,
            max_total_bytes: 1_000_000_000,
            max_records: 500,
            max_concurrency: 8,
            max_duration: Duration::from_secs(30),
        };
        assert_eq!(
            expected,
            CfgParser::<MockEnvGetter>::parse_benchmark_api_config(&mut Env::new(env_getter))
        );
    }
}
