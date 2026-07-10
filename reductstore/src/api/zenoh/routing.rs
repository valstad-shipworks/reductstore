// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::zenoh::{ZenohApiConfig, ZenohBucketRouting};

/// Maps a Zenoh key expression to a `(bucket, entry)` pair for both the
/// subscriber (write) and queryable (read) pipelines.
#[derive(Debug, Clone)]
pub(crate) enum BucketRouting {
    /// Everything goes to one bucket; the full key becomes the entry name.
    Static { bucket: String },
    /// The first chunk of the key is the bucket, the rest the entry name.
    /// Single-chunk keys fall back to the configured bucket.
    KeyPrefix { fallback: String },
}

impl BucketRouting {
    pub(crate) fn from_config(config: &ZenohApiConfig) -> Self {
        match config.bucket_routing {
            ZenohBucketRouting::Static => BucketRouting::Static {
                bucket: config.bucket.clone(),
            },
            ZenohBucketRouting::KeyPrefix => BucketRouting::KeyPrefix {
                fallback: config.bucket.clone(),
            },
        }
    }

    /// Resolves a concrete sample/query key into `(bucket, entry)`. Leading
    /// and trailing slashes are stripped from the key first, mirroring the
    /// entry naming of static mode.
    pub(crate) fn resolve<'a>(&'a self, key_expr: &'a str) -> (&'a str, &'a str) {
        let key = key_expr.trim_matches('/');
        match self {
            BucketRouting::Static { bucket } => (bucket, key),
            BucketRouting::KeyPrefix { fallback } => match key.split_once('/') {
                Some((bucket, entry)) if !bucket.is_empty() && !entry.is_empty() => (bucket, entry),
                _ => (fallback, key),
            },
        }
    }

    /// Whether buckets must be created on demand (key-prefix mode) rather
    /// than once at session startup.
    pub(crate) fn is_dynamic(&self) -> bool {
        matches!(self, BucketRouting::KeyPrefix { .. })
    }

    pub(crate) fn describe(&self) -> String {
        match self {
            BucketRouting::Static { bucket } => format!("bucket='{}'", bucket),
            BucketRouting::KeyPrefix { fallback } => {
                format!("bucket=<key prefix> (fallback '{}')", fallback)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn key_prefix() -> BucketRouting {
        BucketRouting::KeyPrefix {
            fallback: "zenoh".to_string(),
        }
    }

    #[rstest]
    #[case("run_abc/motion/welder/commanded", ("run_abc", "motion/welder/commanded"))]
    #[case("/run_abc/lifecycle", ("run_abc", "lifecycle"))]
    #[case("wc_cell/run_started/", ("wc_cell", "run_started"))]
    fn key_prefix_splits_first_chunk(#[case] key: &str, #[case] expected: (&str, &str)) {
        assert_eq!(key_prefix().resolve(key), expected);
    }

    #[rstest]
    #[case("orphan")]
    #[case("/orphan/")]
    fn key_prefix_single_chunk_falls_back(#[case] key: &str) {
        assert_eq!(key_prefix().resolve(key), ("zenoh", "orphan"));
    }

    #[rstest]
    fn static_uses_full_key_as_entry() {
        let routing = BucketRouting::Static {
            bucket: "bucket-1".to_string(),
        };
        assert_eq!(
            routing.resolve("/factory/line1/status"),
            ("bucket-1", "factory/line1/status")
        );
    }

    #[rstest]
    fn dynamic_flag() {
        assert!(key_prefix().is_dynamic());
        assert!(!BucketRouting::Static {
            bucket: "b".to_string()
        }
        .is_dynamic());
    }
}
