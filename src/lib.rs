//! Thin Antalya integration over upstream HTTP clients.
//!
//! Authorization belongs to the host. SQL and settings must already be approved
//! before building a request. Receiving a batch does not establish query success.

#![cfg_attr(feature = "arrow", doc = include_str!("../README.md"))]
#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

use reqwest::{
    Client, Request, Url,
    header::{HeaderMap, HeaderValue},
};
use std::{collections::BTreeMap, fmt};
mod error;
pub use error::*;
#[cfg(feature = "arrow")]
mod memory;
#[cfg(feature = "arrow")]
mod metrics;
#[cfg(feature = "arrow")]
mod resources;
#[cfg(feature = "arrow")]
pub use metrics::*;
mod config;
pub use config::*;
#[cfg(feature = "arrow")]
mod receiver;
#[cfg(feature = "arrow")]
pub use receiver::{JwtReceiverProfile, ReceiverError};
#[cfg(feature = "arrow")]
mod execution;
#[cfg(feature = "arrow")]
mod export;
#[cfg(feature = "arrow")]
pub use export::*;
pub mod sql;
#[cfg(feature = "arrow")]
pub use execution::*;

#[cfg(feature = "arrow")]
pub use clickhouse;
#[cfg(feature = "arrow")]
pub use clickhouse_ext_arrow::{ArrowClientExt, ArrowCursor, ArrowQueryExt};

/// The exact intended server qualification target, not a runtime compatibility claim.
pub const QUALIFICATION_VERSION: &str = "26.6.8.20004.altinityantalya";

/// HTTP(S) endpoint without embedded credentials, query options or fragment.
/// Hosts must independently enforce their destination/network policy.
#[derive(Clone, Debug)]
pub struct Endpoint(Url);

impl Endpoint {
    pub fn parse(value: &str) -> Result<Self, Error> {
        let url = Url::parse(value).map_err(|_| Error::InvalidEndpoint)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::InvalidEndpoint);
        }
        Ok(Self(url))
    }
}

/// Bearer authentication only. The application owns token acquisition and refresh.
#[derive(Clone)]
pub struct Credentials(HeaderMap);

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credentials([REDACTED])")
    }
}

impl Credentials {
    /// The caller owns token acquisition and refresh.
    pub fn bearer(token: &str) -> Result<Self, Error> {
        if token.is_empty()
            || !token
                .trim_end_matches('=')
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._~+/".contains(&b))
            || token.trim_end_matches('=').is_empty()
        {
            return Err(Error::InvalidCredentials);
        }
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", sensitive(&format!("Bearer {token}"))?);
        Ok(Self(headers))
    }
}

fn sensitive(value: &str) -> Result<HeaderValue, Error> {
    let mut header = HeaderValue::from_str(value).map_err(|_| Error::InvalidCredentials)?;
    header.set_sensitive(true);
    Ok(header)
}

/// Wire result format. Both Arrow variants use Apache Arrow IPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Json,
    /// IPC streaming format, decoded incrementally by the upstream Arrow cursor.
    ArrowStream,
    /// IPC file format, including a footer for random access after download.
    Arrow,
}

impl Format {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::ArrowStream => "ArrowStream",
            Self::Arrow => "Arrow",
        }
    }
}

/// Application-approved SQL, settings and string-valued server parameters.
/// Supply SQL without a trailing semicolon or FORMAT clause. This is not a SQL
/// parser or policy engine; callers may not expose arbitrary settings to users.
#[derive(Clone)]
pub struct Query {
    sql: String,
    id: String,
    settings: BTreeMap<String, String>,
    parameters: BTreeMap<String, Parameter>,
    database: Option<String>,
}

impl Query {
    pub fn new(sql: impl Into<String>, id: impl Into<String>) -> Result<Self, Error> {
        let sql = sql.into();
        let id = id.into();
        if sql.trim().is_empty()
            || sql.len() > 1_048_576
            || id.is_empty()
            || id.len() > 128
            || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(Error::InvalidQuery);
        }
        Ok(Self {
            sql,
            id,
            settings: BTreeMap::new(),
            parameters: BTreeMap::new(),
            database: None,
        })
    }

    pub fn query_id(&self) -> &str {
        &self.id
    }

    pub fn database(mut self, database: &str) -> Result<Self, Error> {
        if database.is_empty() || database.chars().any(char::is_control) {
            return Err(Error::InvalidSetting);
        }
        self.database = Some(database.into());
        Ok(self)
    }

    pub fn typed_parameter(mut self, name: &str, value: Parameter) -> Result<Self, Error> {
        if !identifier(name) {
            return Err(Error::InvalidParameter);
        }
        self.parameters.insert(name.into(), value);
        Ok(self)
    }

    /// Set a query-local server setting, replacing any previous value.
    pub fn setting(mut self, name: &str, value: impl Into<String>) -> Result<Self, Error> {
        if !identifier(name)
            || name.starts_with("param_")
            || matches!(
                name,
                "query"
                    | "query_id"
                    | "user"
                    | "password"
                    | "database"
                    | "default_format"
                    | "session_id"
                    | "session_check"
                    | "session_timeout"
                    | "access_token"
                    | "role"
                    | "roles"
                    | "quota_key"
                    | "stacktrace"
                    | "compress"
                    | "decompress"
                    | "log_comment"
                    | "log_queries"
                    | "log_queries_probability"
                    | "log_queries_min_query_duration_ms"
                    | "log_query_settings"
                    | "framing_output_format"
                    | "output_format_arrow_compression_method"
            )
        {
            return Err(Error::InvalidSetting);
        }
        self.settings.insert(name.to_owned(), value.into());
        Ok(self)
    }

    /// A string parameter for `{name:String}`; escaped for ClickHouse and HTTP.
    /// Use `typed_parameter` for explicit non-string and nullable values.
    pub fn parameter(mut self, name: &str, value: impl Into<String>) -> Result<Self, Error> {
        if !identifier(name) {
            return Err(Error::InvalidParameter);
        }
        self.parameters
            .insert(name.to_owned(), Parameter::String(value.into()));
        Ok(self)
    }

    pub fn swarm(self, cluster: &str) -> Result<Self, Error> {
        if cluster.trim().is_empty() || cluster.chars().any(char::is_control) {
            return Err(Error::InvalidSetting);
        }
        self.setting("object_storage_cluster", cluster)
    }

    pub fn iceberg_snapshot(self, snapshot: i64) -> Result<Self, Error> {
        if snapshot <= 0 {
            return Err(Error::InvalidSnapshot);
        }
        self.setting("iceberg_snapshot_id", snapshot.to_string())
    }

    /// Build only; the caller controls transport and validates response completion.
    pub fn build_request(
        &self,
        client: &Client,
        endpoint: &Endpoint,
        credentials: &Credentials,
        format: Format,
    ) -> Result<Request, Error> {
        self.build(client, endpoint, credentials, Some(format))
    }

    /// Execute DDL/commands without appending a result FORMAT.
    pub fn build_command_request(
        &self,
        client: &Client,
        endpoint: &Endpoint,
        credentials: &Credentials,
    ) -> Result<Request, Error> {
        self.build(client, endpoint, credentials, None)
    }

    fn build(
        &self,
        client: &Client,
        endpoint: &Endpoint,
        credentials: &Credentials,
        format: Option<Format>,
    ) -> Result<Request, Error> {
        let mut url = endpoint.0.clone();
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("query_id", &self.id);
            if let Some(database) = &self.database {
                pairs.append_pair("database", database);
            }
            for (name, value) in &self.settings {
                pairs.append_pair(name, value);
            }
            for (name, value) in &self.parameters {
                pairs.append_pair(&format!("param_{name}"), &value.wire());
            }
        }
        client
            .post(url)
            .headers(credentials.0.clone())
            .body(match format {
                Some(format) => format!("{}\nFORMAT {}", self.sql, format.as_str()),
                None => self.sql.clone(),
            })
            .build()
            .map_err(|_| Error::InvalidRequest)
    }

    /// Reuse the official client with caller-configured authentication/transport.
    /// Escape hatch OUTSIDE the bearer-only/configured transport guarantee.
    /// Prefer `AntalyaClient` for supported configured execution.
    #[cfg(feature = "arrow")]
    pub fn upstream(&self, client: &clickhouse::Client) -> clickhouse::query::Query {
        let mut query = client
            .query_raw(&format!("{}\n", self.sql))
            .with_setting("query_id", &self.id);
        if let Some(database) = &self.database {
            query = query.with_setting("database", database);
        }
        for (name, value) in &self.settings {
            query = query.with_setting(name, value);
        }
        for (name, value) in &self.parameters {
            query = match value {
                Parameter::String(v) => query.param(name, v),
                Parameter::Int64(v) => query.param(name, v),
                Parameter::UInt64(v) => query.param(name, v),
                Parameter::Bool(v) => query.param(name, v),
                Parameter::Null => query.param(name, Option::<String>::None),
            };
        }
        query
    }

    /// Raw IPC bytes for saving or forwarding without decoding/re-encoding.
    /// `Format::Arrow` selects a file; `Format::ArrowStream` selects a stream.
    /// JSON is also accepted. This forwards unvalidated bytes. A fully drained
    /// byte cursor is necessary but not sufficient for success: validate the IPC
    /// artifact and establish query completion before publishing it. Transport
    /// EOF alone does not prove valid IPC or successful server execution.
    #[cfg(feature = "arrow")]
    pub fn fetch_bytes(
        &self,
        client: &clickhouse::Client,
        format: Format,
    ) -> clickhouse::error::Result<clickhouse::query::BytesCursor> {
        self.upstream(client).fetch_bytes(format.as_str())
    }
}

fn identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn escape_parameter(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0' => out.push_str("\\0"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            _ => out.push(c),
        }
    }
    out
}

/// Typed HTTP server parameters. The SQL placeholder supplies the server type.
#[derive(Clone, PartialEq, Eq)]
pub enum Parameter {
    String(String),
    Int64(i64),
    UInt64(u64),
    Bool(bool),
    Null,
}
impl Parameter {
    fn wire(&self) -> String {
        match self {
            Self::String(v) => escape_parameter(v),
            Self::Int64(v) => v.to_string(),
            Self::UInt64(v) => v.to_string(),
            Self::Bool(v) => v.to_string(),
            Self::Null => "\\N".into(),
        }
    }
}
