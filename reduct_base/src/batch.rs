// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

/// Response header listing the optional protocol features a server implements, as a
/// comma-separated list. Absent on servers that predate it.
pub const API_FEATURES_HEADER: &str = "x-reduct-api-features";

pub mod v1;
pub use v1::{parse_batched_header, sort_headers_by_time, RecordHeader};

pub mod v2;
#[cfg(feature = "io")]
pub use v2::{build_label_delta, make_record_header_value};
pub use v2::{
    decode_entry_name, encode_entry_name, encode_label_name, make_batched_header_name,
    parse_batched_header_name, parse_batched_headers, sort_headers_by_entry_and_time,
    EntryRecordHeader, LabelIndex,
};

pub mod v3;
#[cfg(feature = "io")]
pub use v3::{RecordFrame, StreamEncoder};
pub use v3::{StreamDecoder, StreamItem, StreamRecord, READ_STREAM_FEATURE, STREAM_CONTENT_TYPE};
