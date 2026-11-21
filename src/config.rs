use crate::{Credentials, Endpoint, Error, Format, Query};
use std::{fmt, time::Duration};

#[cfg(all(feature = "arrow", feature = "tls"))]
pub(crate) type Connector =
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>;
#[cfg(all(feature = "arrow", not(feature = "tls")))]
pub(crate) type Connector = hyper_util::client::legacy::connect::HttpConnector;
/// Connection reuse options applied to both HTTP clients.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolOptions {
    pub max_idle_per_host: usize,
    pub idle_timeout: Duration,
}
impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            max_idle_per_host: 2,
            idle_timeout: Duration::from_secs(2),
        }
    }
}

/// Shared limits across every clone of a client. Queue capacity is per lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceLimits {
    pub max_active_queries: usize,
    /// Zero refuses an operation immediately when its lane is full.
    pub max_queued_queries: usize,
    pub max_control_queries: usize,
    pub max_export_tasks: usize,
    /// Accounts retained client buffers, not decoder allocations or process RSS.
    pub max_retained_bytes: usize,
}
impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_active_queries: 16,
            max_queued_queries: 64,
            max_control_queries: 4,
            max_export_tasks: 2,
            max_retained_bytes: 64 * 1024 * 1024,
        }
    }
}
impl ResourceLimits {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        // Tokio's semaphore limit is usize::MAX >> 3; acquire_many takes u32.
        // Keep validation available even when the Arrow/Tokio feature is absent.
        let maximum = (usize::MAX >> 3).min(u32::MAX as usize);
        if [
            self.max_active_queries,
            self.max_control_queries,
            self.max_export_tasks,
            self.max_retained_bytes,
        ]
        .into_iter()
        .any(|value| value == 0 || value > maximum)
            || self.max_queued_queries > maximum
        {
            return Err(Error::InvalidSetting);
        }
        Ok(())
    }
}

/// Per-execution bounds. Counts are checked before a batch is returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionLimits {
    pub max_rows: u64,
    pub max_batches: u64,
    /// Sum of retained Arrow storage charges across batches, not wire bytes.
    pub max_total_bytes: u64,
    /// Allocation capacities deduplicated within a batch, plus conservative
    /// array metadata. Independently retained batches are charged separately.
    pub max_batch_bytes: usize,
}
impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
            max_batches: 65_536,
            max_total_bytes: 512 * 1024 * 1024,
            max_batch_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Terminal evidence source and query-log observation strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalStrategy {
    FlushFirst,
    ObserveFirst,
    /// Rely on asynchronous publication without issuing SYSTEM FLUSH LOGS.
    /// The same identity-bound terminal proof and total deadline still apply.
    ObserveOnly,
    /// Require a native framed HTTP success terminator and exact Arrow row count.
    /// High-level Arrow Execution only; commands/admitted transport refuse it.
    /// Unsupported servers fail; completion never replays or falls back to logs.
    InBand,
}

/// Bounded polling delays after a terminal proof is not yet visible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalPolicy {
    pub strategy: TerminalStrategy,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}
impl Default for TerminalPolicy {
    fn default() -> Self {
        Self {
            strategy: TerminalStrategy::FlushFirst,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
        }
    }
}

/// Shared identity, database and total operation timeout. No redirects or retries.
#[derive(Clone, Debug)]
pub struct Config {
    pub(crate) endpoint: Endpoint,
    pub(crate) credentials: Credentials,
    pub(crate) database: Option<String>,
    pub(crate) timeout: Duration,
    pub(crate) read_timeout: Option<Duration>,
    pub(crate) capacity_wait_timeout: Option<Duration>,
    pub(crate) pool: PoolOptions,
    pub(crate) resources: ResourceLimits,
    pub(crate) execution_limits: ExecutionLimits,
    pub(crate) terminal_policy: TerminalPolicy,
    #[cfg(feature = "arrow")]
    pub(crate) receiver_expiry: Option<crate::receiver::ReceiverExpiry>,
}
impl Config {
    pub fn new(endpoint: Endpoint, credentials: Credentials) -> Self {
        Self {
            endpoint,
            credentials,
            database: None,
            timeout: Duration::from_secs(60),
            read_timeout: None,
            capacity_wait_timeout: Some(Duration::from_secs(1)),
            pool: PoolOptions::default(),
            resources: ResourceLimits::default(),
            execution_limits: ExecutionLimits::default(),
            terminal_policy: TerminalPolicy::default(),
            #[cfg(feature = "arrow")]
            receiver_expiry: None,
        }
    }
    /// Configure connection reuse; validated by [`AntalyaClient::new`].
    pub fn pool(mut self, pool: PoolOptions) -> Self {
        self.pool = pool;
        self
    }
    /// Configure shared capacity; validated by [`AntalyaClient::new`].
    pub fn resources(mut self, resources: ResourceLimits) -> Self {
        self.resources = resources;
        self
    }
    /// Configure result bounds; validated by [`AntalyaClient::new`].
    pub fn execution_limits(mut self, limits: ExecutionLimits) -> Self {
        self.execution_limits = limits;
        self
    }
    /// Configure terminal observation; validated by [`AntalyaClient::new`].
    pub fn terminal_policy(mut self, policy: TerminalPolicy) -> Self {
        self.terminal_policy = policy;
        self
    }
    pub fn database(mut self, database: &str) -> Result<Self, Error> {
        self.database = Query::new("SELECT 1", "config")?
            .database(database)?
            .database;
        Ok(self)
    }
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    /// Bound each pending streaming/control network read, independently of the total operation
    /// deadline. Successful reads reset this timer; processing returned output
    /// does not consume it. Cancelling a pending read does not reset its timer.
    /// Direct command execution and raw upstream escape hatches are excluded.
    /// Disabled by default. Zero and unrepresentable durations are invalid.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }
    /// Bound waiting for a full shared byte reservation. Zero refuses immediately
    /// when insufficient capacity is available. Defaults to one second. Configure
    /// a longer wait for deliberate backpressure; the total deadline still applies.
    pub fn capacity_wait_timeout(mut self, timeout: Duration) -> Self {
        self.capacity_wait_timeout = Some(timeout);
        self
    }
}
#[derive(Clone)]
pub struct AntalyaClient {
    pub(crate) config: Config,
    pub(crate) http: reqwest::Client,
    #[cfg(feature = "arrow")]
    pub(crate) upstream: clickhouse::Client,
    #[cfg(feature = "arrow")]
    pub(crate) connector: Connector,
    #[cfg(feature = "arrow")]
    pub(crate) resources: std::sync::Arc<crate::resources::Resources>,
}
impl fmt::Debug for AntalyaClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AntalyaClient([REDACTED])")
    }
}
impl AntalyaClient {
    pub fn new(config: Config) -> Result<Self, Error> {
        config.resources.validate()?;
        let now = std::time::Instant::now();
        let limits = config.execution_limits;
        let policy = config.terminal_policy;
        if limits.max_rows == 0
            || limits.max_batches == 0
            || limits.max_batch_bytes == 0
            || limits.max_batch_bytes > config.resources.max_retained_bytes
            || u64::try_from(limits.max_batch_bytes)
                .map_or(true, |bytes| bytes > limits.max_total_bytes)
            || policy.initial_delay.is_zero()
            || policy.initial_delay > policy.max_delay
            || now.checked_add(policy.max_delay).is_none()
            || now.checked_add(config.pool.idle_timeout).is_none()
            || config.pool.max_idle_per_host > u32::MAX as usize
            || config.timeout.is_zero()
            || config
                .read_timeout
                .is_some_and(|timeout| timeout.is_zero() || now.checked_add(timeout).is_none())
            || config
                .capacity_wait_timeout
                .is_some_and(|timeout| now.checked_add(timeout).is_none())
            || std::time::Instant::now()
                .checked_add(config.timeout)
                .is_none()
        {
            return Err(Error::InvalidSetting);
        }
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .pool_max_idle_per_host(config.pool.max_idle_per_host)
            .pool_idle_timeout(config.pool.idle_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .map_err(|_| Error::InvalidRequest)?;
        #[cfg(feature = "arrow")]
        let (upstream, connector) = {
            let token = config
                .credentials
                .0
                .get("Authorization")
                .ok_or(Error::InvalidCredentials)?
                .to_str()
                .map_err(|_| Error::InvalidCredentials)?
                .strip_prefix("Bearer ")
                .ok_or(Error::InvalidCredentials)?;
            // Use the supported upstream HTTP customization point. Hyper otherwise
            // retries some cancelled reused-connection requests by default.
            let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
            connector.set_keepalive(Some(Duration::from_secs(60)));
            #[cfg(feature = "tls")]
            connector.enforce_http(false);
            #[cfg(feature = "tls")]
            let connector = hyper_rustls::HttpsConnectorBuilder::new()
                .with_provider_and_native_roots(rustls::crypto::aws_lc_rs::default_provider())
                .map_err(|_| Error::InvalidRequest)?
                .https_or_http()
                .enable_http1()
                .wrap_connector(connector);
            let http =
                hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                    .pool_idle_timeout(config.pool.idle_timeout)
                    .pool_max_idle_per_host(config.pool.max_idle_per_host)
                    .retry_canceled_requests(false)
                    .build(connector.clone());
            let upstream = clickhouse::Client::with_http_client(http)
                .with_url(config.endpoint.0.as_str())
                .with_access_token(token)
                .with_compression(clickhouse::Compression::None);
            (upstream, connector)
        };
        #[cfg(feature = "arrow")]
        let resources =
            std::sync::Arc::new(crate::resources::Resources::with_capacity_wait_timeout(
                &config.resources,
                config.capacity_wait_timeout,
            )?);
        Ok(Self {
            config,
            http,
            #[cfg(feature = "arrow")]
            upstream,
            #[cfg(feature = "arrow")]
            connector,
            #[cfg(feature = "arrow")]
            resources,
        })
    }
    /// Select a bearer for future operations, sharing existing connection pools,
    /// admission limits and retained-byte capacity. Existing clones and prepared
    /// executions keep their credentials and deadlines. Performs no I/O or replay.
    ///
    /// Returns `Error::Unsupported` for a receiver-aware JWT client: use
    /// `with_jwt_receiver` with a reviewed profile to preserve its constraints.
    /// Invalid bearer syntax returns `Error::InvalidCredentials`.
    pub fn with_bearer(&self, bearer: &str) -> Result<Self, Error> {
        #[cfg(feature = "arrow")]
        if self.config.receiver_expiry.is_some() {
            return Err(Error::Unsupported);
        }
        let credentials = Credentials::bearer(bearer)?;
        let mut client = self.clone();
        client.config.credentials = credentials;
        #[cfg(feature = "arrow")]
        {
            client.upstream = client.upstream.with_access_token(bearer);
        }
        Ok(client)
    }

    pub fn build_request(&self, query: &Query, format: Format) -> Result<reqwest::Request, Error> {
        self.scoped(query).build_request(
            &self.http,
            &self.config.endpoint,
            &self.config.credentials,
            format,
        )
    }
    pub fn build_command_request(&self, query: &Query) -> Result<reqwest::Request, Error> {
        self.scoped(query).build_command_request(
            &self.http,
            &self.config.endpoint,
            &self.config.credentials,
        )
    }
    pub(crate) fn scoped(&self, query: &Query) -> Query {
        let mut query = query.clone();
        if query.database.is_none() {
            query.database = self.config.database.clone();
        }
        query
    }
}
/// Host authorization metadata. This crate does not issue, validate or refresh grants.
pub struct AuthorizationContext {
    pub receipt_id: String,
    pub expected_principal: String,
}
/// Immutable snapshot of an approved operation, including all query settings,
/// parameters, limits, filters, database, snapshot and destination.
/// The host must revalidate expiry/revocation immediately before execution.
/// There is deliberately no conversion back to a mutable Query.
pub struct AuthorizedRequest {
    pub(crate) query: Query,
    pub(crate) endpoint: Endpoint,
    pub(crate) format: Format,
    pub(crate) authorization: AuthorizationContext,
}
impl AuthorizedRequest {
    pub fn new(
        query: Query,
        endpoint: Endpoint,
        format: Format,
        authorization: AuthorizationContext,
    ) -> Result<Self, Error> {
        if authorization.receipt_id.trim().is_empty()
            || authorization.expected_principal.trim().is_empty()
            || authorization.receipt_id.chars().any(char::is_control)
            || authorization
                .expected_principal
                .chars()
                .any(char::is_control)
        {
            return Err(Error::InvalidQuery);
        }
        Ok(Self {
            query,
            endpoint,
            format,
            authorization,
        })
    }
    pub fn query_id(&self) -> &str {
        self.query.query_id()
    }
    pub fn receipt_id(&self) -> &str {
        &self.authorization.receipt_id
    }
    pub fn expected_principal(&self) -> &str {
        &self.authorization.expected_principal
    }
    pub fn build_request(
        &self,
        client: &reqwest::Client,
        credentials: &Credentials,
    ) -> Result<reqwest::Request, Error> {
        self.query
            .build_request(client, &self.endpoint, credentials, self.format)
    }
}
