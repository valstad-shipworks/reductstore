// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0
//
// Streaming protocol v3
// ---------------------
// One response carries a whole query: no round trip per batch, and no cap from the
// client's inbound header limit, because the metadata moves into the body.
//
// Frames, each introduced by a one-byte tag:
//   0x01 RECORD     record metadata, then content_length payload bytes if HAS_PAYLOAD
//   0x02 END        the query is exhausted; nothing follows
//   0x03 ERROR      zigzag varint status, then a length-prefixed message; nothing follows
//   0x04 KEEPALIVE  nothing; keeps an idle continuous query's connection warm
//   A body that ends without END or ERROR was truncated.
//
// RECORD frame:
//   u8     flags          SAME_ENTRY | SAME_CONTENT_TYPE | SAME_LABELS | HAS_PAYLOAD
//   string entry          unless SAME_ENTRY
//   varint timestamp      zigzag delta from the previous record in the stream
//   varint content_length
//   string content_type   unless SAME_CONTENT_TYPE
//   labels                unless SAME_LABELS: varint count, then count * (string, string)
//   bytes  payload        content_length bytes, if HAS_PAYLOAD
//
// Encoding rules:
//   - Integers are unsigned LEB128 varints.
//   - Strings (entry names, content types, label names and values) carry a varint code:
//     0 inline (varint length, then bytes), 1 inline and take the next table index,
//     >= 2 a reference to table index `code - 2`. A string is interned on its second
//     use, so a value that never repeats costs one inline copy and never takes a slot.
//   - SAME_CONTENT_TYPE and SAME_LABELS compare against the previous record of the same
//     entry, so interleaved entries each keep their own baseline.
//   - Computed labels are folded into the label map under an `@` prefix, as in v1 and v2.

use crate::error::{ErrorCode, ReductError};
#[cfg(feature = "io")]
use crate::io::RecordMeta;
use crate::Labels;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;

/// Name advertised in [`crate::batch::API_FEATURES_HEADER`] by a server that serves
/// `POST /io/{bucket}/stream`.
pub const READ_STREAM_FEATURE: &str = "read-stream";

/// Content type of a v3 stream response.
pub const STREAM_CONTENT_TYPE: &str = "application/x-reduct-stream-v3";

const TAG_RECORD: u8 = 0x01;
const TAG_END: u8 = 0x02;
const TAG_ERROR: u8 = 0x03;
const TAG_KEEPALIVE: u8 = 0x04;

const FLAG_SAME_ENTRY: u8 = 0x01;
const FLAG_SAME_CONTENT_TYPE: u8 = 0x02;
const FLAG_SAME_LABELS: u8 = 0x04;
const FLAG_HAS_PAYLOAD: u8 = 0x08;

const STR_INLINE: u64 = 0;
const STR_INLINE_INTERN: u64 = 1;
const STR_REF_BASE: u64 = 2;

/// Upper bound on the shared string table, so a stream of never-repeating values cannot
/// grow either side's memory without bound.
const MAX_INTERNED: usize = 8192;

/// Upper bound on an inline string, so a corrupt length cannot make the decoder wait
/// forever for bytes that will never arrive.
const MAX_STRING_LEN: u64 = 1 << 20;

fn put_varint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

fn put_string(buf: &mut BytesMut, code: u64, value: &str) {
    put_varint(buf, code);
    if code < STR_REF_BASE {
        put_varint(buf, value.len() as u64);
        buf.put_slice(value.as_bytes());
    }
}

fn malformed(what: &str) -> ReductError {
    ReductError::new(
        ErrorCode::Unknown,
        &format!("Malformed record stream: {}", what),
    )
}

/// Cursor over the decoder's buffer that reports "not enough bytes yet" as `Ok(None)`.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn u8(&mut self) -> Option<u8> {
        let byte = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(byte)
    }

    fn varint(&mut self) -> Result<Option<u64>, ReductError> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let Some(byte) = self.buf.get(self.pos).copied() else {
                return Ok(None);
            };
            self.pos += 1;
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(Some(result));
            }
            shift += 7;
            if shift >= 64 {
                return Err(malformed("varint overflows 64 bits"));
            }
        }
    }

    fn slice(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        let out = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(out)
    }
}

/// Encoder state for one v3 stream. Records must be fed in stream order; the decoder
/// rebuilds the string table and the per-entry baselines from the same sequence.
#[derive(Default)]
pub struct StreamEncoder {
    strings: HashMap<String, StringSlot>,
    interned: usize,
    prev_timestamp: u64,
    prev_entry: Option<String>,
    prev_meta: HashMap<String, (String, Labels)>,
}

#[derive(Default)]
struct StringSlot {
    index: Option<u64>,
    seen: u32,
}

/// One record's metadata on its way onto the wire.
pub struct RecordFrame<'a> {
    pub entry: &'a str,
    pub timestamp: u64,
    pub content_type: &'a str,
    pub content_length: u64,
    pub labels: &'a Labels,
    /// False for a metadata-only query, where the payload bytes are left out.
    pub with_payload: bool,
}

impl StreamEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// The caller writes `content_length` payload bytes straight after the frame when
    /// `with_payload` is set.
    pub fn encode_record(&mut self, buf: &mut BytesMut, frame: RecordFrame) {
        let RecordFrame {
            entry,
            timestamp,
            content_type,
            content_length,
            labels,
            with_payload,
        } = frame;

        let same_entry = self.prev_entry.as_deref() == Some(entry);
        let baseline = self.prev_meta.get(entry);
        let same_content_type = baseline.is_some_and(|(prev, _)| prev == content_type);
        let same_labels = baseline.is_some_and(|(_, prev)| prev == labels);

        let mut flags = 0u8;
        if same_entry {
            flags |= FLAG_SAME_ENTRY;
        }
        if same_content_type {
            flags |= FLAG_SAME_CONTENT_TYPE;
        }
        if same_labels {
            flags |= FLAG_SAME_LABELS;
        }
        if with_payload {
            flags |= FLAG_HAS_PAYLOAD;
        }

        buf.put_u8(TAG_RECORD);
        buf.put_u8(flags);

        if !same_entry {
            let code = self.string_code(entry);
            put_string(buf, code, entry);
        }

        let delta = (timestamp as i64).wrapping_sub(self.prev_timestamp as i64);
        put_varint(buf, zigzag(delta));
        put_varint(buf, content_length);

        if !same_content_type {
            let code = self.string_code(content_type);
            put_string(buf, code, content_type);
        }

        if !same_labels {
            put_varint(buf, labels.len() as u64);
            for (name, value) in labels {
                let code = self.string_code(name);
                put_string(buf, code, name);
                let code = self.string_code(value);
                put_string(buf, code, value);
            }
        }

        self.prev_timestamp = timestamp;
        if !same_entry {
            self.prev_entry = Some(entry.to_string());
        }
        if !same_content_type || !same_labels {
            self.prev_meta.insert(
                entry.to_string(),
                (content_type.to_string(), labels.clone()),
            );
        }
    }

    /// Folds computed labels into the label map under an `@` prefix.
    #[cfg(feature = "io")]
    pub fn encode_record_meta(
        &mut self,
        buf: &mut BytesMut,
        meta: &RecordMeta,
        with_payload: bool,
    ) {
        let merged;
        let labels = if meta.computed_labels().is_empty() {
            meta.labels()
        } else {
            let mut labels = meta.labels().clone();
            for (name, value) in meta.computed_labels() {
                labels.insert(format!("@{}", name), value.clone());
            }
            merged = labels;
            &merged
        };

        self.encode_record(
            buf,
            RecordFrame {
                entry: meta.entry_name(),
                timestamp: meta.timestamp(),
                content_type: meta.content_type(),
                content_length: meta.content_length(),
                labels,
                with_payload,
            },
        );
    }

    pub fn encode_end(&self, buf: &mut BytesMut) {
        buf.put_u8(TAG_END);
    }

    pub fn encode_error(&self, buf: &mut BytesMut, error: &ReductError) {
        buf.put_u8(TAG_ERROR);
        put_varint(buf, zigzag(error.status() as i16 as i64));
        put_varint(buf, error.message().len() as u64);
        buf.put_slice(error.message().as_bytes());
    }

    /// Carries nothing; keeps an idle connection from being reaped.
    pub fn encode_keepalive(&self, buf: &mut BytesMut) {
        buf.put_u8(TAG_KEEPALIVE);
    }

    fn string_code(&mut self, value: &str) -> u64 {
        if let Some(slot) = self.strings.get_mut(value) {
            if let Some(index) = slot.index {
                return STR_REF_BASE + index;
            }

            slot.seen += 1;
            if slot.seen >= 2 && self.interned < MAX_INTERNED {
                slot.index = Some(self.interned as u64);
                self.interned += 1;
                return STR_INLINE_INTERN;
            }
            return STR_INLINE;
        }

        self.strings.insert(
            value.to_string(),
            StringSlot {
                index: None,
                seen: 1,
            },
        );
        STR_INLINE
    }
}

/// One record's metadata as it came off the wire.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamRecord {
    pub entry: String,
    pub timestamp: u64,
    pub content_type: String,
    pub content_length: u64,
    pub labels: Labels,
    /// False for a metadata-only query, where no payload frames follow the record.
    pub has_payload: bool,
}

/// What [`StreamDecoder::next_item`] hands back.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamItem {
    /// Followed by `content_length` bytes as `Payload` items when `has_payload` is set.
    Record(StreamRecord),
    Payload(Bytes),
    Error(ReductError),
    End,
}

/// Incremental decoder for a v3 stream. Feed it response chunks in order and drain
/// [`StreamDecoder::next_item`] after each; a record larger than a chunk is handed over
/// as several `Payload` items, so nothing larger than one chunk is ever held.
#[derive(Default)]
pub struct StreamDecoder {
    buf: BytesMut,
    state: DecoderState,
    payload_left: u64,
    finished: bool,
}

#[derive(Default)]
struct DecoderState {
    strings: Vec<String>,
    prev_timestamp: u64,
    prev_entry: Option<String>,
    prev_meta: HashMap<String, (String, Labels)>,
}

/// A decoded frame together with how many bytes of the buffer it consumed.
struct Decoded {
    item: StreamItem,
    consumed: usize,
    payload_len: u64,
}

impl StreamDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a chunk of the response body.
    pub fn feed(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// Returns the next item, or `None` when more bytes are needed.
    pub fn next_item(&mut self) -> Result<Option<StreamItem>, ReductError> {
        if self.payload_left > 0 {
            if self.buf.is_empty() {
                return Ok(None);
            }
            let take = self.payload_left.min(self.buf.len() as u64) as usize;
            self.payload_left -= take as u64;
            return Ok(Some(StreamItem::Payload(self.buf.split_to(take).freeze())));
        }

        loop {
            if self.finished || self.buf.is_empty() {
                return Ok(None);
            }

            let mut cursor = Cursor {
                buf: &self.buf,
                pos: 0,
            };
            let Some(tag) = cursor.u8() else {
                return Ok(None);
            };

            let decoded = match tag {
                TAG_RECORD => self.state.decode_record(cursor)?,
                TAG_ERROR => self.state.decode_error(cursor)?,
                TAG_END => Some(Decoded {
                    item: StreamItem::End,
                    consumed: cursor.pos,
                    payload_len: 0,
                }),
                TAG_KEEPALIVE => {
                    self.buf.advance(cursor.pos);
                    continue;
                }
                other => return Err(malformed(&format!("unknown frame tag {:#04x}", other))),
            };

            let Some(decoded) = decoded else {
                return Ok(None);
            };

            self.buf.advance(decoded.consumed);
            self.payload_left = decoded.payload_len;
            self.finished = matches!(decoded.item, StreamItem::End | StreamItem::Error(_));
            return Ok(Some(decoded.item));
        }
    }

    /// True once END or ERROR has been decoded; a body that ends before that was
    /// truncated.
    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

impl DecoderState {
    fn decode_record(&mut self, mut cursor: Cursor) -> Result<Option<Decoded>, ReductError> {
        let Some(flags) = cursor.u8() else {
            return Ok(None);
        };

        let mut pending_strings = Vec::new();
        let entry = if flags & FLAG_SAME_ENTRY != 0 {
            self.prev_entry
                .clone()
                .ok_or_else(|| malformed("first record reuses an entry name"))?
        } else {
            match self.read_string(&mut cursor, &mut pending_strings)? {
                Some(entry) => entry,
                None => return Ok(None),
            }
        };

        let Some(delta) = cursor.varint()? else {
            return Ok(None);
        };
        let Some(content_length) = cursor.varint()? else {
            return Ok(None);
        };
        let timestamp = (self.prev_timestamp as i64).wrapping_add(unzigzag(delta)) as u64;

        let content_type = if flags & FLAG_SAME_CONTENT_TYPE != 0 {
            self.prev_meta
                .get(&entry)
                .map(|(content_type, _)| content_type.clone())
                .ok_or_else(|| malformed("first record of an entry reuses a content type"))?
        } else {
            match self.read_string(&mut cursor, &mut pending_strings)? {
                Some(content_type) => content_type,
                None => return Ok(None),
            }
        };

        let labels = if flags & FLAG_SAME_LABELS != 0 {
            self.prev_meta
                .get(&entry)
                .map(|(_, labels)| labels.clone())
                .ok_or_else(|| malformed("first record of an entry reuses labels"))?
        } else {
            let Some(count) = cursor.varint()? else {
                return Ok(None);
            };
            let mut labels = Labels::with_capacity(count.min(1024) as usize);
            for _ in 0..count {
                let Some(name) = self.read_string(&mut cursor, &mut pending_strings)? else {
                    return Ok(None);
                };
                let Some(value) = self.read_string(&mut cursor, &mut pending_strings)? else {
                    return Ok(None);
                };
                labels.insert(name, value);
            }
            labels
        };

        let consumed = cursor.pos;
        self.strings.extend(pending_strings);
        self.prev_timestamp = timestamp;
        self.prev_entry = Some(entry.clone());
        self.prev_meta
            .insert(entry.clone(), (content_type.clone(), labels.clone()));

        let has_payload = flags & FLAG_HAS_PAYLOAD != 0;
        Ok(Some(Decoded {
            item: StreamItem::Record(StreamRecord {
                entry,
                timestamp,
                content_type,
                content_length,
                labels,
                has_payload,
            }),
            consumed,
            payload_len: if has_payload { content_length } else { 0 },
        }))
    }

    fn decode_error(&mut self, mut cursor: Cursor) -> Result<Option<Decoded>, ReductError> {
        let Some(code) = cursor.varint()? else {
            return Ok(None);
        };
        let Some(len) = cursor.varint()? else {
            return Ok(None);
        };
        if len > MAX_STRING_LEN {
            return Err(malformed("error message is too long"));
        }
        let Some(bytes) = cursor.slice(len as usize) else {
            return Ok(None);
        };
        let message = String::from_utf8(bytes.to_vec())
            .map_err(|_| malformed("error message is not valid UTF-8"))?;

        let status = i16::try_from(unzigzag(code))
            .ok()
            .and_then(|code| ErrorCode::try_from(code).ok())
            .unwrap_or(ErrorCode::Unknown);

        Ok(Some(Decoded {
            item: StreamItem::Error(ReductError::new(status, &message)),
            consumed: cursor.pos,
            payload_len: 0,
        }))
    }

    /// Newly interned strings are collected rather than added straight away, because the
    /// frame they belong to may still be incomplete.
    fn read_string(
        &self,
        cursor: &mut Cursor,
        pending: &mut Vec<String>,
    ) -> Result<Option<String>, ReductError> {
        let Some(code) = cursor.varint()? else {
            return Ok(None);
        };

        if code >= STR_REF_BASE {
            let index = (code - STR_REF_BASE) as usize;
            let value = match index.checked_sub(self.strings.len()) {
                Some(pending_index) => pending.get(pending_index),
                None => self.strings.get(index),
            };
            return match value {
                Some(value) => Ok(Some(value.clone())),
                None => Err(malformed("string reference is out of range")),
            };
        }

        let Some(len) = cursor.varint()? else {
            return Ok(None);
        };
        if len > MAX_STRING_LEN {
            return Err(malformed("inline string is too long"));
        }
        let Some(bytes) = cursor.slice(len as usize) else {
            return Ok(None);
        };
        let value = String::from_utf8(bytes.to_vec())
            .map_err(|_| malformed("inline string is not valid UTF-8"))?;

        if code == STR_INLINE_INTERN {
            if self.strings.len() + pending.len() >= MAX_INTERNED {
                return Err(malformed("string table is full"));
            }
            pending.push(value.clone());
        }

        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn encode_all(records: &[(StreamRecord, &[u8])]) -> BytesMut {
        let mut encoder = StreamEncoder::new();
        let mut buf = BytesMut::new();
        for (record, payload) in records {
            encoder.encode_record(
                &mut buf,
                RecordFrame {
                    entry: &record.entry,
                    timestamp: record.timestamp,
                    content_type: &record.content_type,
                    content_length: record.content_length,
                    labels: &record.labels,
                    with_payload: record.has_payload,
                },
            );
            if record.has_payload {
                buf.put_slice(payload);
            }
        }
        encoder.encode_end(&mut buf);
        buf
    }

    fn decode_all(bytes: &[u8], chunk_size: usize) -> (Vec<(StreamRecord, Vec<u8>)>, bool) {
        let mut decoder = StreamDecoder::new();
        let mut out: Vec<(StreamRecord, Vec<u8>)> = Vec::new();
        let mut ended = false;

        for chunk in bytes.chunks(chunk_size) {
            decoder.feed(chunk);
            while let Some(item) = decoder.next_item().unwrap() {
                match item {
                    StreamItem::Record(record) => out.push((record, Vec::new())),
                    StreamItem::Payload(payload) => {
                        out.last_mut().unwrap().1.extend_from_slice(&payload)
                    }
                    StreamItem::End => ended = true,
                    StreamItem::Error(err) => panic!("unexpected error frame: {}", err),
                }
            }
        }

        (out, ended)
    }

    fn head_frame(entry: &str, timestamp: u64) -> RecordFrame<'_> {
        RecordFrame {
            entry,
            timestamp,
            content_type: "text/plain",
            content_length: 0,
            labels: EMPTY_LABELS.get_or_init(Labels::new),
            with_payload: false,
        }
    }

    static EMPTY_LABELS: std::sync::OnceLock<Labels> = std::sync::OnceLock::new();

    fn record(entry: &str, timestamp: u64, content_type: &str, payload: &[u8]) -> StreamRecord {
        StreamRecord {
            entry: entry.to_string(),
            timestamp,
            content_type: content_type.to_string(),
            content_length: payload.len() as u64,
            labels: Labels::new(),
            has_payload: true,
        }
    }

    #[rstest]
    #[case(1)]
    #[case(3)]
    #[case(7)]
    #[case(64)]
    #[case(4096)]
    fn round_trips_at_any_chunk_boundary(#[case] chunk_size: usize) {
        let mut first = record("entry-1", 1_000_000, "text/plain", b"hello");
        first.labels = labels(&[("host", "svc01"), ("seq", "1")]);
        let mut second = record("entry-1", 1_000_100, "text/plain", b"world!!");
        second.labels = labels(&[("host", "svc01"), ("seq", "2")]);
        let third = record("entry-2", 999_000, "application/cbor", b"\x01\x02\x03");

        let input = vec![
            (first.clone(), b"hello".as_slice()),
            (second.clone(), b"world!!".as_slice()),
            (third.clone(), b"\x01\x02\x03".as_slice()),
        ];
        let bytes = encode_all(&input);
        let (decoded, ended) = decode_all(&bytes, chunk_size);

        assert!(ended);
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].0, first);
        assert_eq!(decoded[0].1, b"hello");
        assert_eq!(decoded[1].0, second);
        assert_eq!(decoded[1].1, b"world!!");
        assert_eq!(decoded[2].0, third);
        assert_eq!(decoded[2].1, b"\x01\x02\x03");
    }

    #[rstest]
    fn reuses_content_type_and_labels_per_entry() {
        let mut a1 = record("a", 100, "application/cbor", b"1");
        a1.labels = labels(&[("k", "v")]);
        let mut b1 = record("b", 101, "text/plain", b"2");
        b1.labels = labels(&[("k", "other")]);
        let mut a2 = record("a", 102, "application/cbor", b"3");
        a2.labels = labels(&[("k", "v")]);

        let bytes = encode_all(&[
            (a1.clone(), b"1".as_slice()),
            (b1.clone(), b"2".as_slice()),
            (a2.clone(), b"3".as_slice()),
        ]);
        let (decoded, _) = decode_all(&bytes, 4096);

        assert_eq!(decoded[2].0, a2, "entry a's baseline survives entry b");
    }

    #[rstest]
    fn interns_repeated_strings() {
        let mut records = Vec::new();
        for i in 0..64u64 {
            let mut rec = record("telemetry", 1_000_000 + i, "application/cbor", b"xxxxxxxx");
            rec.labels = labels(&[("source", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]);
            records.push((rec, b"xxxxxxxx".as_slice()));
        }
        let bytes = encode_all(&records);

        let payload_bytes = 64 * 8;
        let overhead = bytes.len() - payload_bytes;
        assert!(
            overhead < 64 * 6,
            "interning should hold steady-state metadata under 6 B/record, got {} B total",
            overhead
        );

        let (decoded, ended) = decode_all(&bytes, 5);
        assert!(ended);
        assert_eq!(decoded.len(), 64);
        assert_eq!(
            decoded[63].0.labels["source"],
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[rstest]
    fn never_interns_a_value_seen_once() {
        let mut records = Vec::new();
        for i in 0..MAX_INTERNED as u64 + 100 {
            let mut rec = record("e", i, "text/plain", b"");
            rec.labels = labels(&[("ts", &format!("{}", i))]);
            records.push((rec, b"".as_slice()));
        }
        let bytes = encode_all(&records);
        let (decoded, ended) = decode_all(&bytes, 997);

        assert!(ended);
        assert_eq!(decoded.len(), MAX_INTERNED + 100);
        assert_eq!(
            decoded.last().unwrap().0.labels["ts"],
            format!("{}", MAX_INTERNED + 99)
        );
    }

    #[rstest]
    fn carries_metadata_only_records() {
        let mut head = record("e", 5, "text/plain", b"ignored");
        head.has_payload = false;
        head.content_length = 7;

        let bytes = encode_all(&[(head.clone(), b"".as_slice())]);
        let (decoded, ended) = decode_all(&bytes, 4096);

        assert!(ended);
        assert_eq!(decoded[0].0, head);
        assert!(
            decoded[0].1.is_empty(),
            "no payload follows a head-only record"
        );
    }

    #[rstest]
    fn decodes_a_terminal_error() {
        let mut encoder = StreamEncoder::new();
        let mut buf = BytesMut::new();
        encoder.encode_record(&mut buf, head_frame("e", 1));
        encoder.encode_error(
            &mut buf,
            &ReductError::new(ErrorCode::TooManyRequests, "slow down"),
        );

        let mut decoder = StreamDecoder::new();
        decoder.feed(&buf);
        assert!(matches!(
            decoder.next_item().unwrap(),
            Some(StreamItem::Record(_))
        ));
        let Some(StreamItem::Error(err)) = decoder.next_item().unwrap() else {
            panic!("expected an error frame");
        };
        assert_eq!(err.status(), ErrorCode::TooManyRequests);
        assert_eq!(err.message(), "slow down");
        assert!(decoder.is_finished());
    }

    #[rstest]
    fn skips_keepalives() {
        let mut encoder = StreamEncoder::new();
        let mut buf = BytesMut::new();
        encoder.encode_keepalive(&mut buf);
        encoder.encode_keepalive(&mut buf);
        encoder.encode_record(&mut buf, head_frame("e", 1));
        encoder.encode_end(&mut buf);

        let (decoded, ended) = decode_all(&buf, 1);
        assert!(ended);
        assert_eq!(decoded.len(), 1);
    }

    #[rstest]
    fn reports_a_truncated_stream() {
        let bytes = encode_all(&[(record("e", 1, "text/plain", b"abcd"), b"abcd".as_slice())]);
        let mut decoder = StreamDecoder::new();
        decoder.feed(&bytes[..bytes.len() - 3]);
        while decoder.next_item().unwrap().is_some() {}
        assert!(!decoder.is_finished());
    }

    #[rstest]
    fn rejects_an_unknown_tag() {
        let mut decoder = StreamDecoder::new();
        decoder.feed(&[0x7f]);
        assert!(decoder.next_item().is_err());
    }

    #[rstest]
    fn rejects_an_out_of_range_string_reference() {
        let mut buf = BytesMut::new();
        buf.put_u8(TAG_RECORD);
        buf.put_u8(0);
        put_varint(&mut buf, STR_REF_BASE + 9);

        let mut decoder = StreamDecoder::new();
        decoder.feed(&buf);
        assert!(decoder.next_item().is_err());
    }

    #[rstest]
    fn timestamps_may_go_backwards() {
        let bytes = encode_all(&[
            (record("e", 5_000_000, "text/plain", b""), b"".as_slice()),
            (record("e", 4_000_000, "text/plain", b""), b"".as_slice()),
            (record("e", 6_000_000, "text/plain", b""), b"".as_slice()),
        ]);
        let (decoded, _) = decode_all(&bytes, 4096);
        let stamps: Vec<u64> = decoded.iter().map(|(r, _)| r.timestamp).collect();
        assert_eq!(stamps, vec![5_000_000, 4_000_000, 6_000_000]);
    }
}
