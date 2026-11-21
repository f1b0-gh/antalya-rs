// SPDX-License-Identifier: Apache-2.0
//! Documented JSONEachPacketBase64 protocol, with fail-closed terminal detection.
use super::{Measurements, Progress};
use crate::{AntalyaClient, Cause, Error, Format, Phase, Query};
use base64::{Engine, engine::general_purpose::STANDARD};
use hyper::body::Bytes;
use reqwest::{Response, header::HeaderMap};
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;
use std::{fmt, future::Future, pin::Pin, time::Instant};
type Pending = Pin<Box<dyn Future<Output = Result<Response, reqwest::Error>> + Send>>;
pub(super) struct FramedSource {
    pending: Option<Pending>,
    response: Option<Response>,
    pending_error: Option<Pin<Box<dyn Future<Output = Error> + Send>>>,
    query_id: String,
    chunk: Bytes,
    offset: usize,
    line: Vec<u8>,
    line_limit: usize,
    wire_limit: u64,
    terminal: Option<Progress>,
    ended: bool,
    packets: u64,
    read_timer: crate::read_timeout::ReadTimer,
}
impl FramedSource {
    pub(super) fn new(client: &AntalyaClient, query: &Query) -> Result<Self, Error> {
        let mut query = query.clone();
        query.settings.insert(
            "framing_output_format".into(),
            "JSONEachPacketBase64".into(),
        );
        query.settings.insert(
            "output_format_arrow_compression_method".into(),
            "none".into(),
        );
        query
            .settings
            .insert("enable_http_compression".into(), "0".into());
        let request = client.build_request(&query, Format::ArrowStream)?;
        let http = client.http.clone();
        Ok(Self {
            pending: Some(Box::pin(async move { http.execute(request).await })),
            response: None,
            pending_error: None,
            query_id: query.id,
            chunk: Bytes::new(),
            offset: 0,
            line: Vec::new(),
            line_limit: client
                .config
                .execution_limits
                .max_batch_bytes
                .saturating_mul(2)
                .saturating_add(65536)
                .min(32 * 1024 * 1024),
            wire_limit: client
                .config
                .execution_limits
                .max_total_bytes
                .saturating_mul(2)
                .saturating_add(1_048_576),
            terminal: None,
            ended: false,
            packets: 0,
            read_timer: crate::read_timeout::ReadTimer::new(client.config.read_timeout),
        })
    }
    pub(super) fn terminal(&self) -> Option<Progress> {
        if self.ended { self.terminal } else { None }
    }
    pub(super) async fn next_data(
        &mut self,
        metrics: &mut Measurements,
    ) -> Result<Option<Bytes>, Error> {
        if self.ended {
            return Ok(None);
        }
        if let Some(pending) = self.pending.as_mut() {
            let response = crate::metrics::timed(
                &mut metrics.transfer,
                self.read_timer.wait(Phase::Data, pending),
            )
            .await?
            .map_err(|e| Error::from_reqwest(Phase::Data, &e))?;
            self.pending = None;
            if !response.status().is_success() {
                self.pending_error = Some(Box::pin(crate::execution::server_response_error(
                    response,
                    Phase::Data,
                    std::mem::replace(
                        &mut self.read_timer,
                        crate::read_timeout::ReadTimer::new(None),
                    ),
                )));
            } else {
                validate_headers(response.headers(), &self.query_id)?;
                self.response = Some(response);
            }
        }
        if let Some(pending_error) = self.pending_error.as_mut() {
            let error = pending_error.await;
            self.pending_error = None;
            return Err(error);
        }
        loop {
            if self.offset == self.chunk.len() {
                let response = self.response.as_mut().ok_or(Error::InvalidRequest)?;
                let next = crate::metrics::timed(
                    &mut metrics.transfer,
                    self.read_timer.wait(Phase::Data, response.chunk()),
                )
                .await?
                .map_err(|e| Error::from_reqwest(Phase::Data, &e))?;
                match next {
                    Some(chunk) => {
                        metrics.wire_bytes = metrics
                            .wire_bytes
                            .checked_add(chunk.len() as u64)
                            .ok_or_else(limit)?;
                        if metrics.wire_bytes > self.wire_limit {
                            return Err(limit());
                        }
                        self.chunk = chunk;
                        self.offset = 0;
                    }
                    None => {
                        if !self.line.is_empty() || self.terminal.is_none() {
                            return Err(invalid());
                        }
                        self.ended = true;
                        self.response = None;
                        return Ok(None);
                    }
                }
            }
            if !self.take_line()? {
                continue;
            }
            let start = Instant::now();
            let result = self.packet();
            metrics.framing = metrics.framing.saturating_add(start.elapsed());
            self.line.clear();
            self.packets = self.packets.checked_add(1).ok_or_else(limit)?;
            if self.packets > 1_000_000 {
                return Err(limit());
            }
            if let Some(bytes) = result? {
                return Ok(Some(bytes));
            }
            if self.packets.is_multiple_of(64) {
                tokio::task::yield_now().await;
            }
        }
    }
    /// Consume at most one bounded line, independent of HTTP chunk boundaries.
    fn take_line(&mut self) -> Result<bool, Error> {
        let remaining = &self.chunk[self.offset..];
        let len = remaining
            .iter()
            .position(|b| *b == b'\n')
            .map_or(remaining.len(), |p| p + 1);
        if len > self.line_limit.saturating_sub(self.line.len()) {
            return Err(limit());
        }
        self.line.try_reserve(len).map_err(|_| limit())?;
        self.line.extend_from_slice(&remaining[..len]);
        self.offset += len;
        Ok(self.line.last() == Some(&b'\n'))
    }
    fn packet(&mut self) -> Result<Option<Bytes>, Error> {
        // Native data packets need no owned JSON string. Strict base64 input
        // cannot contain quotes, escapes or JSON whitespace, so successful
        // decoding proves this exact envelope is unambiguous. Other forms (even
        // escaped base64 strings) retain the duplicate-rejecting parser below.
        // The line/wire limits are enforced before this point; terminal data
        // still goes through the original rejection path.
        if self.terminal.is_none()
            && let Some(data) = self
                .line
                .strip_prefix(b"{\"packet\":\"data\",\"data\":\"")
                .and_then(|data| data.strip_suffix(b"\"}\n"))
            && let Ok(bytes) = STANDARD.decode(data)
        {
            return Ok(Some(Bytes::from(bytes)));
        }
        let Unique(value) = serde_json::from_slice::<Unique>(&self.line).map_err(|_| invalid())?;
        let map = value
            .as_object()
            .filter(|v| v.len() == 2)
            .ok_or_else(invalid)?;
        let kind = map
            .get("packet")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        if kind == "exception" && map.contains_key("exception") {
            return Err(Error::at(Phase::Control, Cause::QueryFailed));
        }
        if self.terminal.is_some() {
            return Err(invalid());
        }
        match kind {
            "data" => {
                let data = map
                    .get("data")
                    .and_then(Value::as_str)
                    .ok_or_else(invalid)?;
                STANDARD
                    .decode(data)
                    .map(Bytes::from)
                    .map(Some)
                    .map_err(|_| invalid())
            }
            "progress" => {
                let progress = map
                    .get("progress")
                    .and_then(Value::as_object)
                    .ok_or_else(invalid)?;
                let count = |key: &str| -> Result<Option<u64>, Error> {
                    progress
                        .get(key)
                        .map(|v| {
                            v.as_u64()
                                .or_else(|| v.as_str()?.parse().ok())
                                .ok_or_else(invalid)
                        })
                        .transpose()
                };
                let rows = count("result_rows")?;
                let bytes = count("result_bytes")?;
                let memory = count("memory_usage")?;
                let read_rows = count("read_rows")?.unwrap_or(0);
                let read_bytes = count("read_bytes")?.unwrap_or(0);
                // These fields are emitted only in final progress. Zero values
                // may be omitted; ordinary progress never proves zero results.
                if rows.is_some() || bytes.is_some() || memory.is_some() {
                    self.terminal = Some(Progress {
                        read_rows,
                        read_bytes,
                        result_rows: Some(rows.unwrap_or(0)),
                    });
                }
                Ok(None)
            }
            "log" if map.get("log").is_some_and(Value::is_object) => Ok(None),
            "profile_events" if map.get("profile_events").is_some_and(Value::is_array) => Ok(None),
            _ => Err(invalid()),
        }
    }
}
fn validate_headers(headers: &HeaderMap, id: &str) -> Result<(), Error> {
    for name in ["x-clickhouse-query-id", "content-type"] {
        if headers.get_all(name).iter().count() != 1 {
            return Err(invalid());
        }
    }
    if headers
        .get("x-clickhouse-query-id")
        .and_then(|v| v.to_str().ok())
        != Some(id)
    {
        return Err(invalid());
    }
    if headers.contains_key("content-encoding") {
        return Err(invalid());
    }
    if headers
        .get_all("x-clickhouse-exception-code")
        .iter()
        .any(|v| v.to_str().ok() != Some("0"))
    {
        return Err(Error::at(Phase::Control, Cause::QueryFailed));
    }
    let value = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(invalid)?;
    let mut parts = value.split(';').map(str::trim);
    if !parts
        .next()
        .is_some_and(|v| v.eq_ignore_ascii_case("application/x-ndjson"))
    {
        return Err(invalid());
    }
    let (mut payload, mut charset) = (false, false);
    for part in parts {
        let (k, v) = part.split_once('=').ok_or_else(invalid)?;
        match k.trim().to_ascii_lowercase().as_str() {
            "payload" if !payload && v.trim() == "base64" => payload = true,
            "charset" if !charset && v.trim().eq_ignore_ascii_case("utf-8") => charset = true,
            _ => return Err(invalid()),
        }
    }
    if !payload {
        return Err(invalid());
    }
    Ok(())
}
fn invalid() -> Error {
    Error::at(Phase::Control, Cause::InvalidProof)
}
fn limit() -> Error {
    Error::at(Phase::Data, Cause::LimitExceeded)
}

// Reject duplicate keys at every JSON level; serde_json::Value alone would
// silently accept a later contradictory terminal counter. serde_json retains
// its default bounded nesting depth and input is bounded before this parser.
struct Unique(Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("unambiguous bounded JSON")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut m: M) -> Result<Unique, M::Error> {
                let mut out = serde_json::Map::new();
                while let Some(key) = m.next_key::<String>()? {
                    if out.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate JSON key"));
                    }
                    out.insert(key, m.next_value::<Unique>()?.0);
                }
                Ok(Unique(Value::Object(out)))
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut s: S) -> Result<Unique, S::Error> {
                let mut out = Vec::new();
                while let Some(v) = s.next_element::<Unique>()? {
                    out.push(v.0);
                }
                Ok(Unique(Value::Array(out)))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Unique, E> {
                Ok(Unique(Value::String(v.into())))
            }
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Unique, E> {
                Ok(Unique(Value::String(v)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Unique, E> {
                Ok(Unique(Value::from(v)))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Unique, E> {
                Ok(Unique(Value::from(v)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Unique, E> {
                Ok(Unique(Value::from(v)))
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Unique, E> {
                Ok(Unique(Value::Bool(v)))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
        }
        d.deserialize_any(V)
    }
}

#[cfg(test)]
mod chunk_tests {
    use super::*;
    fn source() -> FramedSource {
        let client = AntalyaClient::new(crate::Config::new(
            crate::Endpoint::parse("http://localhost:1").unwrap(),
            crate::Credentials::bearer("test").unwrap(),
        ))
        .unwrap();
        FramedSource::new(&client, &Query::new("SELECT 1", "data-test").unwrap()).unwrap()
    }

    #[test]
    fn data_packets_preserve_json_variants_and_binary_bytes() {
        let mut source = source();
        for payload in [Vec::new(), vec![0], vec![0, 255, 10], vec![255; 100_000]] {
            let data = STANDARD.encode(&payload);
            for line in [
                format!("{{\"packet\":\"data\",\"data\":\"{data}\"}}\n"),
                format!("{{ \"data\": \"{data}\", \"packet\": \"data\" }}\n"),
                format!(
                    "{{\"packet\":\"data\",\"data\":\"{}\"}}\n",
                    data.replace('/', "\\/")
                ),
                format!(
                    "{{\"packet\":\"data\",\"data\":\"{}\"}}\n",
                    data.replace('A', "\\u0041")
                ),
            ] {
                source.line = line.into_bytes();
                assert_eq!(source.packet().unwrap().unwrap().as_ref(), &payload);
            }
        }
    }

    #[test]
    fn data_line_bounds_apply_across_http_chunk_boundaries() {
        let payload: Vec<u8> = (0..=255).collect();
        let line = format!(
            "{{\"packet\":\"data\",\"data\":\"{}\"}}\n",
            STANDARD.encode(&payload)
        );
        for chunk_size in [1, 7, line.len()] {
            let mut source = source();
            source.line_limit = line.len();
            let mut packets = 0;
            for chunk in line.as_bytes().chunks(chunk_size) {
                source.chunk = Bytes::copy_from_slice(chunk);
                source.offset = 0;
                if source.take_line().unwrap() {
                    assert_eq!(source.packet().unwrap().unwrap().as_ref(), &payload);
                    packets += 1;
                }
            }
            assert_eq!(packets, 1);
        }
        for chunk_size in [1, 7, line.len()] {
            let mut source = source();
            source.line_limit = line.len() - 1;
            let mut rejected = false;
            for chunk in line.as_bytes().chunks(chunk_size) {
                source.chunk = Bytes::copy_from_slice(chunk);
                source.offset = 0;
                match source.take_line() {
                    Ok(complete) => assert!(!complete),
                    Err(error) => {
                        assert_eq!(error, limit());
                        rejected = true;
                        break;
                    }
                }
            }
            assert!(rejected);
            assert!(source.line.len() <= source.line_limit);
        }
    }

    #[test]
    fn data_packets_reject_extra_fields_duplicates_and_invalid_base64() {
        let mut source = source();
        for line in [
            b"{\"packet\":\"data\",\"data\":\"AA==\",\"data\":\"AQ==\"}\n".as_slice(),
            b"{\"packet\":\"data\",\"data\":\"AA==\",\"packet\":\"data\"}\n",
            b"{\"packet\":\"data\",\"data\":\"AA==\",\"extra\":null}\n",
            b"{\"packet\":\"data\",\"data\":\"AA==\"}{\"other\":\"\"}\n",
            b"{\"packet\":\"data\",\"data\":\"A\"}\n",
            b"{\"packet\":\"data\",\"data\":\"AB==\"}\n",
            b"{\"packet\":\"data\",\"data\":\"AA==\\q\"}\n",
            b"{\"packet\":\"data\",\"data\":\"AA==\0\"}\n",
            b"{\"packet\":\"data\",\"data\":\"\xff\"}\n",
        ] {
            source.line = line.to_vec();
            assert_eq!(source.packet().unwrap_err(), invalid());
        }
        source.terminal = Some(Progress {
            read_rows: 0,
            read_bytes: 0,
            result_rows: Some(0),
        });
        source.line = b"{\"packet\":\"data\",\"data\":\"AA==\"}\n".to_vec();
        assert_eq!(source.packet().unwrap_err(), invalid());
        source.line = b"{\"packet\":\"exception\",\"exception\":\"late\"}\n".to_vec();
        assert_eq!(
            source.packet().unwrap_err(),
            Error::at(Phase::Control, Cause::QueryFailed)
        );
    }

    #[test]
    fn coalesced_and_fragmented_http_chunks_accept_the_same_packets() {
        let client = AntalyaClient::new(crate::Config::new(
            crate::Endpoint::parse("http://localhost:1").unwrap(),
            crate::Credentials::bearer("test").unwrap(),
        ))
        .unwrap();
        let query = Query::new("SELECT 1", "chunk-test").unwrap();
        let mut body = b"{\"packet\":\"progress\",\"progress\":{\"read_rows\":1}}\n".repeat(100);
        body.extend_from_slice(b"{\"packet\":\"progress\",\"progress\":{\"result_rows\":0}}\n");
        for chunk_size in [1, 7, body.len()] {
            let mut source = FramedSource::new(&client, &query).unwrap();
            source.line_limit = 64;
            let mut packets = 0;
            for chunk in body.chunks(chunk_size) {
                source.chunk = Bytes::copy_from_slice(chunk);
                source.offset = 0;
                while source.offset < source.chunk.len() {
                    if source.take_line().unwrap() {
                        assert!(source.packet().unwrap().is_none());
                        source.line.clear();
                        packets += 1;
                    }
                }
            }
            assert_eq!(packets, 101);
            assert!(source.line.is_empty());
            assert_eq!(source.terminal.unwrap().result_rows, Some(0));
        }
    }
}
