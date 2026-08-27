// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::components::{Components, CLIENT_IP_HEADER};
use crate::api::http::entry::QueryEntryAxum;
use crate::api::http::{ErrorCode, HttpError, StateKeeper};
use crate::api::limits::{limit_scope_from_client_ip, LimitScope};
use crate::auth::policy::ReadAccessPolicy;
use crate::cfg::io::IoConfig;
use crate::core::sync::AsyncRwLock;
use crate::storage::bucket::Bucket;
use crate::storage::query::QueryRx;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum_extra::headers::HeaderMap;
use bytes::{Bytes, BytesMut};
use log::debug;
use reduct_base::batch::v3::{StreamEncoder, STREAM_CONTENT_TYPE};
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::QueryType;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time::timeout;

/// How many flushed chunks may sit between the pump and the socket. The pump stops on a
/// full channel, which is what makes a slow client throttle the reads behind it.
const CHANNEL_DEPTH: usize = 4;

// POST /io/:bucket/stream
pub(super) async fn stream_records(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    request: QueryEntryAxum,
) -> Result<impl IntoResponse, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let components = keeper
        .get_with_permissions(
            &headers,
            ReadAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let mut request = request.0;
    request.query_type = QueryType::Query;
    let head_only = request.only_metadata.unwrap_or(false);

    let entry_name = request
        .entries
        .as_ref()
        .and_then(|entries| entries.first())
        .cloned()
        .unwrap_or_default();

    let bucket = components
        .storage
        .get_bucket(bucket_name)
        .await?
        .upgrade()?;
    let query_id = bucket.query(request.clone()).await?;

    components
        .ext_repo
        .register_query(query_id, bucket_name, &entry_name, request)
        .await?;

    let (rx, io_settings) = bucket.get_query_receiver(query_id).await?;
    let scope = limit_scope_from_client_ip(
        headers
            .get(CLIENT_IP_HEADER)
            .and_then(|value| value.to_str().ok()),
    );

    let (tx, chunks) = mpsc::channel(CHANNEL_DEPTH);
    let pump = Pump {
        query_id,
        query_path: format!("{}/{}", bucket_name, query_id),
        rx: rx.upgrade()?,
        bucket: Arc::clone(&bucket),
        components: Arc::clone(&components),
        io_settings,
        scope,
        head_only,
        tx,
    };
    tokio::spawn(pump.run());

    let body = async_stream::stream! {
        let mut chunks = chunks;
        while let Some(chunk) = chunks.recv().await {
            yield chunk;
        }
    };

    Ok((
        [("content-type", STREAM_CONTENT_TYPE)],
        Body::from_stream(body),
    ))
}

struct Pump {
    query_id: u64,
    query_path: String,
    rx: Arc<AsyncRwLock<QueryRx>>,
    bucket: Arc<Bucket>,
    components: Arc<Components>,
    io_settings: IoConfig,
    scope: LimitScope,
    head_only: bool,
    tx: mpsc::Sender<Result<Bytes, HttpError>>,
}

/// Why the pump stopped, and so which terminal frame the client gets.
enum Stop {
    Exhausted,
    Failed(ReductError),
    Disconnected,
}

impl Pump {
    /// Drains the query into the response body until it is exhausted, fails, or the client
    /// goes away, then closes the stream with a terminal frame.
    async fn run(self) {
        let mut encoder = StreamEncoder::new();
        let mut buf = BytesMut::with_capacity(self.io_settings.stream_flush_size * 2);
        let mut unbilled = 0u64;
        let mut idle_since = Instant::now();

        let stop = loop {
            let fetched = timeout(
                self.io_settings.batch_records_timeout,
                self.components
                    .ext_repo
                    .fetch_and_process_record(self.query_id, Arc::clone(&self.rx)),
            )
            .await;

            let readers = match fetched {
                Ok(Some(readers)) => readers,
                Ok(None) => Vec::new(),
                Err(_) => {
                    debug!(
                        "Timeout while waiting for record from query {}",
                        self.query_path
                    );
                    Vec::new()
                }
            };

            if readers.is_empty() {
                if !buf.is_empty() && self.flush(&mut buf, &mut unbilled).await.is_err() {
                    break Stop::Disconnected;
                }
                if idle_since.elapsed() >= self.io_settings.stream_keepalive {
                    encoder.encode_keepalive(&mut buf);
                    if self.flush(&mut buf, &mut unbilled).await.is_err() {
                        break Stop::Disconnected;
                    }
                    idle_since = Instant::now();
                }
                continue;
            }

            idle_since = Instant::now();
            let mut stop = None;
            for reader in readers {
                let mut reader = match reader {
                    Ok(reader) => reader,
                    Err(err) if err.status() == ErrorCode::NoContent => {
                        stop = Some(Stop::Exhausted);
                        break;
                    }
                    Err(err) => {
                        stop = Some(Stop::Failed(err));
                        break;
                    }
                };

                encoder.encode_record_meta(&mut buf, reader.meta(), !self.head_only);

                if !self.head_only {
                    unbilled += reader.meta().content_length();
                    while let Some(chunk) = reader.read_chunk() {
                        match chunk {
                            Ok(chunk) => buf.extend_from_slice(&chunk),
                            Err(err) => {
                                stop = Some(Stop::Failed(err));
                                break;
                            }
                        }

                        if buf.len() >= self.io_settings.stream_flush_size {
                            if let Err(reason) = self.flush(&mut buf, &mut unbilled).await {
                                stop = Some(reason);
                                break;
                            }
                        }
                    }
                }

                if stop.is_some() {
                    break;
                }

                if buf.len() >= self.io_settings.stream_flush_size {
                    if let Err(reason) = self.flush(&mut buf, &mut unbilled).await {
                        stop = Some(reason);
                        break;
                    }
                }
            }

            if let Some(stop) = stop {
                break stop;
            }
        };

        let stop = match stop {
            Stop::Exhausted => match self.charge(&mut unbilled).await {
                Ok(()) => Stop::Exhausted,
                Err(err) => {
                    buf.clear();
                    Stop::Failed(err)
                }
            },
            other => other,
        };

        self.bucket.remove_query(self.query_id).await;

        match stop {
            Stop::Exhausted => encoder.encode_end(&mut buf),
            Stop::Failed(err) => encoder.encode_error(&mut buf, &err),
            Stop::Disconnected => return,
        }
        let _ = self.send(&mut buf).await;
    }

    /// Charges the payload bytes accumulated so far against the egress limit and hands the
    /// buffer to the socket. Nothing over the limit reaches the client.
    async fn flush(&self, buf: &mut BytesMut, unbilled: &mut u64) -> Result<(), Stop> {
        if let Err(err) = self.charge(unbilled).await {
            buf.clear();
            return Err(Stop::Failed(err));
        }
        self.send(buf).await
    }

    async fn charge(&self, unbilled: &mut u64) -> Result<(), ReductError> {
        let billed = std::mem::take(unbilled);
        if billed == 0 {
            return Ok(());
        }
        self.components
            .limits
            .check_egress_for(self.scope.clone(), billed)
            .await
    }

    async fn send(&self, buf: &mut BytesMut) -> Result<(), Stop> {
        if buf.is_empty() {
            return Ok(());
        }
        self.tx
            .send(Ok(buf.split().freeze()))
            .await
            .map_err(|_| Stop::Disconnected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{egress_limited_keeper, headers, keeper, path_to_bucket_1};
    use axum::body::to_bytes;
    use axum::response::IntoResponse;
    use bytes::Bytes;
    use futures_util::StreamExt;
    use reduct_base::batch::v3::{StreamDecoder, StreamItem, StreamRecord};
    use reduct_base::msg::entry_api::QueryEntry;
    use rstest::rstest;
    use std::time::Duration;

    /// Decodes a whole stream response into its records, their payloads, and how it ended.
    fn decode(body: &[u8]) -> (Vec<(StreamRecord, Vec<u8>)>, Option<StreamItem>) {
        let mut decoder = StreamDecoder::new();
        decoder.feed(body);

        let mut records: Vec<(StreamRecord, Vec<u8>)> = Vec::new();
        let mut terminal = None;
        while let Some(item) = decoder.next_item().unwrap() {
            match item {
                StreamItem::Record(record) => records.push((record, Vec::new())),
                StreamItem::Payload(payload) => {
                    records.last_mut().unwrap().1.extend_from_slice(&payload)
                }
                other => terminal = Some(other),
            }
        }
        (records, terminal)
    }

    async fn write_records(keeper: &Arc<StateKeeper>, entry: &str, times: &[u64], data: &str) {
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        for time in times {
            let mut writer = bucket
                .begin_write(
                    entry,
                    *time,
                    data.len() as u64,
                    "text/plain".to_string(),
                    Default::default(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(data.to_string()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }
    }

    async fn stream(
        keeper: Arc<StateKeeper>,
        path: Path<HashMap<String, String>>,
        headers: HeaderMap,
        request: QueryEntry,
    ) -> (Vec<(StreamRecord, Vec<u8>)>, Option<StreamItem>) {
        let response = stream_records(State(keeper), headers, path, QueryEntryAxum(request))
            .await
            .unwrap()
            .into_response();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        decode(&body)
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn streams_past_the_batch_record_cap(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let times: Vec<u64> = (1000..1100).collect();
        write_records(&keeper, "entry-1", &times, "payload").await;

        let (records, terminal) = stream(
            keeper,
            path_to_bucket_1,
            headers,
            QueryEntry {
                entries: Some(vec!["entry-1".into()]),
                start: Some(1000),
                stop: Some(1100),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(
            records.len(),
            100,
            "one response carries the whole query, past the {}-record batch cap",
            10
        );
        assert_eq!(terminal, Some(StreamItem::End));
        assert!(records.iter().all(|(_, payload)| payload == b"payload"));
        let stamps: Vec<u64> = records.iter().map(|(record, _)| record.timestamp).collect();
        assert_eq!(stamps, times);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn streams_several_entries_in_one_response(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        write_records(&keeper, "entry-1", &[1000, 1002], "aa").await;
        write_records(&keeper, "entry-2", &[1001, 1003], "bbb").await;

        let (records, terminal) = stream(
            keeper,
            path_to_bucket_1,
            headers,
            QueryEntry {
                entries: Some(vec!["entry-1".into(), "entry-2".into()]),
                start: Some(1000),
                stop: Some(1004),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(terminal, Some(StreamItem::End));
        let mut seen: Vec<(String, u64, Vec<u8>)> = records
            .into_iter()
            .map(|(record, payload)| (record.entry, record.timestamp, payload))
            .collect();
        seen.sort();
        assert_eq!(
            seen,
            vec![
                ("entry-1".to_string(), 1000, b"aa".to_vec()),
                ("entry-1".to_string(), 1002, b"aa".to_vec()),
                ("entry-2".to_string(), 1001, b"bbb".to_vec()),
                ("entry-2".to_string(), 1003, b"bbb".to_vec()),
            ]
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn omits_payloads_for_a_metadata_only_query(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        write_records(&keeper, "entry-1", &[1000, 1001], "payload").await;

        let (records, terminal) = stream(
            keeper,
            path_to_bucket_1,
            headers,
            QueryEntry {
                entries: Some(vec!["entry-1".into()]),
                start: Some(1000),
                stop: Some(1002),
                only_metadata: Some(true),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(terminal, Some(StreamItem::End));
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|(record, payload)| {
            record.content_length == 7 && !record.has_payload && payload.is_empty()
        }));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn ends_an_empty_query_cleanly(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        write_records(&keeper, "entry-1", &[1000], "x").await;

        let (records, terminal) = stream(
            keeper,
            path_to_bucket_1,
            headers,
            QueryEntry {
                entries: Some(vec!["entry-1".into()]),
                start: Some(5000),
                stop: Some(6000),
                ..Default::default()
            },
        )
        .await;

        assert!(records.is_empty());
        assert_eq!(terminal, Some(StreamItem::End));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn reports_an_exceeded_egress_limit_in_the_body(
        #[future] egress_limited_keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = egress_limited_keeper.await;
        let times: Vec<u64> = (1000..1100).collect();
        write_records(&keeper, "entry-1", &times, "payload").await;

        let (_, terminal) = stream(
            keeper,
            path_to_bucket_1,
            headers,
            QueryEntry {
                entries: Some(vec!["entry-1".into()]),
                start: Some(1000),
                stop: Some(1100),
                ..Default::default()
            },
        )
        .await;

        let Some(StreamItem::Error(err)) = terminal else {
            panic!("expected the stream to end with an error frame, got {terminal:?}");
        };
        assert_eq!(err.status(), ErrorCode::TooManyRequests);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn keeps_a_continuous_query_open_for_records_written_later(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        write_records(&keeper, "entry-1", &[1000], "first").await;

        let response = stream_records(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            QueryEntryAxum(QueryEntry {
                entries: Some(vec!["entry-1".into()]),
                start: Some(1000),
                continuous: Some(true),
                ttl: Some(10),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_response();

        let writer = tokio::spawn({
            let keeper = keeper.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                write_records(&keeper, "entry-1", &[2000], "later").await;
            }
        });

        let mut body = response.into_body().into_data_stream();
        let mut decoder = StreamDecoder::new();
        let mut seen = Vec::new();
        while seen.len() < 2 {
            let chunk = timeout(Duration::from_secs(10), body.next())
                .await
                .expect("the stream stalled")
                .expect("the stream ended early")
                .unwrap();
            decoder.feed(&chunk);
            while let Some(item) = decoder.next_item().unwrap() {
                if let StreamItem::Payload(payload) = item {
                    seen.push(payload);
                }
            }
        }
        writer.await.unwrap();

        assert_eq!(seen[0], "first");
        assert_eq!(seen[1], "later");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn does_not_charge_egress_for_a_metadata_only_query(
        #[future] egress_limited_keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = egress_limited_keeper.await;
        let times: Vec<u64> = (1000..1100).collect();
        write_records(&keeper, "entry-1", &times, "payload").await;

        let (records, terminal) = stream(
            keeper,
            path_to_bucket_1,
            headers,
            QueryEntry {
                entries: Some(vec!["entry-1".into()]),
                start: Some(1000),
                stop: Some(1100),
                only_metadata: Some(true),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(records.len(), 100);
        assert_eq!(terminal, Some(StreamItem::End));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn rejects_an_unknown_bucket_before_streaming(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let path = Path(HashMap::from_iter(vec![(
            "bucket_name".to_string(),
            "no-such-bucket".to_string(),
        )]));

        let err = stream_records(
            State(keeper),
            headers,
            path,
            QueryEntryAxum(QueryEntry {
                entries: Some(vec!["entry-1".into()]),
                ..Default::default()
            }),
        )
        .await
        .err()
        .expect("a missing bucket must fail the request, not the stream");
        assert_eq!(err.status(), ErrorCode::NotFound);
    }
}
