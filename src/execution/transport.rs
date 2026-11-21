//! One-shot HTTP admission and bounded response consumption. No decoder or
//! terminal observation runs implicitly on this path.
mod ipc;
use super::{
    Execution, PreparedExecution, RecoveryReceipt, RemoteTermination, TerminalObservation,
};
use crate::{Cause, Error, Format, Phase};
use hyper::{
    Request, Response,
    body::{Body, Bytes, Incoming},
    client::conn::http1,
    header::{HOST, HeaderMap, HeaderValue},
};
pub use ipc::{AdmittedArrow, AdmittedIpc, DecodeError, IpcError, IpcFrame, IpcFrameKind};
use std::{
    fmt,
    future::{Future, poll_fn},
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};
use tokio::{
    sync::OwnedSemaphorePermit,
    task::JoinHandle,
    time::{Instant, timeout_at},
};
use tower_service::Service;

const MAX_HTTP_BUFFER: usize = 64 * 1024;
// Hyper can return a frame larger than its configured read size when a reused
// allocation has spare capacity. Bound that dependency boundary separately from
// the smaller, shared-budget payload chunks exposed to callers.
const MAX_PENDING_FRAME: usize = 4 * MAX_HTTP_BUFFER;
type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<Response<Incoming>, hyper::Error>> + Send>>;

/// Typed transport failures. Display/Debug omit URLs, credentials and server
/// bodies. Original request/connector/HTTP causes remain inspectable via source().
pub enum TransportError {
    Client(Error),
    Receiver(crate::ReceiverError),
    Request(reqwest::Error),
    Header(hyper::header::InvalidHeaderValue),
    Uri(hyper::http::Error),
    Connect(Box<dyn std::error::Error + Send + Sync>),
    Http(hyper::Error),
    Runtime(tokio::runtime::TryCurrentError),
    Deadline,
    Backpressure,
    InvalidRequest,
    InvalidResponse,
    ObservationMismatch,
    HttpStatus(u16),
    ServerException,
    LimitExceeded,
    FrameLimit { size: usize, limit: usize },
    Poisoned,
}
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Client(_) => "client preparation or observation failed",
            Self::Receiver(_) => "receiver admission deadline refused",
            Self::Request(_) => "HTTP request construction failed",
            Self::Header(_) => "HTTP request header construction failed",
            Self::Uri(_) => "HTTP request URI construction failed",
            Self::Connect(_) => "transport connection failed",
            Self::Http(_) => "HTTP transport failed",
            Self::Runtime(_) => "transport polling requires a Tokio runtime",
            Self::Deadline => "transport deadline elapsed",
            Self::Backpressure => "transport is not ready for immediate admission",
            Self::InvalidRequest => "invalid transport request",
            Self::InvalidResponse => "ambiguous transport response",
            Self::ObservationMismatch => "terminal observation belongs to another execution",
            Self::HttpStatus(_) => "HTTP response status refused",
            Self::ServerException => "server response reports an exception",
            Self::LimitExceeded => "wire response exceeds the configured limit",
            Self::FrameLimit { .. } => "HTTP frame exceeds the transport buffer bound",
            Self::Poisoned => "response consumption previously failed",
        })
    }
}
impl fmt::Debug for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Client(e) => Some(e),
            Self::Receiver(e) => Some(e),
            Self::Request(e) => Some(e),
            Self::Header(e) => Some(e),
            Self::Uri(e) => Some(e),
            Self::Connect(e) => Some(e.as_ref()),
            Self::Http(e) => Some(e),
            Self::Runtime(e) => Some(e),
            _ => None,
        }
    }
}
impl From<Error> for TransportError {
    fn from(error: Error) -> Self {
        Self::Client(error)
    }
}

// A single connection task is required to drive admitted HTTP I/O. Its owner
// always aborts it on drop; there is no detached retry or request producer.
struct Driver(JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Original approved execution plus an established, ready HTTP/1 connection.
/// No data request has been sent. Persist the receipt and recheck host authority
/// before admit(). Dropping this value releases local admission without a query.
#[must_use]
pub struct ReadyExecution {
    execution: Execution,
    sender: http1::SendRequest<reqwest::Body>,
    request: Request<reqwest::Body>,
    driver: Driver,
}
enum WireOperation {
    Arrow,
    Command,
}
impl PreparedExecution {
    /// Establish a dedicated connection before final host admission. This may
    /// perform DNS/TCP/TLS I/O but sends no data query. The original deadline
    /// includes this preparation. Credentials and destination are never changed.
    pub async fn ready_transport(self) -> Result<ReadyExecution, TransportError> {
        self.ready(WireOperation::Arrow).await
    }
    pub(super) async fn ready_command_transport(self) -> Result<ReadyExecution, TransportError> {
        self.ready(WireOperation::Command).await
    }
    async fn ready(self, operation: WireOperation) -> Result<ReadyExecution, TransportError> {
        if self.execution.client.config.terminal_policy.strategy == crate::TerminalStrategy::InBand
        {
            return Err(crate::Error::Unsupported.into());
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(TransportError::Runtime)?;
        let execution = self.execution;
        check_deadline(execution.deadline)?;
        // Also require the bounded, principal-bound receipt for this API.
        RecoveryReceipt::validate_parts(
            &execution.client.config.endpoint,
            &execution.query.id,
            &execution.nonce,
            execution
                .principal
                .as_deref()
                .ok_or(Error::InvalidRequest)?,
        )?;
        let request = match operation {
            WireOperation::Arrow => {
                let mut request = execution
                    .client
                    .build_request(&execution.query, Format::ArrowStream)?;
                // Same uncompressed wire profile as the high-level Arrow reader.
                request
                    .url_mut()
                    .query_pairs_mut()
                    .append_pair("output_format_arrow_compression_method", "none");
                request
            }
            WireOperation::Command => execution.client.build_command_request(&execution.query)?,
        };
        let mut request: Request<reqwest::Body> =
            request.try_into().map_err(TransportError::Request)?;
        let destination = request.uri().clone();
        let authority = destination
            .authority()
            .ok_or(TransportError::InvalidRequest)?;
        let host = HeaderValue::from_str(authority.as_str()).map_err(TransportError::Header)?;
        request.headers_mut().insert(HOST, host);
        *request.uri_mut() = hyper::Uri::builder()
            .path_and_query(
                destination
                    .path_and_query()
                    .cloned()
                    .ok_or(TransportError::InvalidRequest)?,
            )
            .build()
            .map_err(TransportError::Uri)?;
        let mut connector = execution.client.connector.clone();
        let connection = timeout_at(execution.deadline, connector.call(destination))
            .await
            .map_err(|_| TransportError::Deadline)?
            .map_err(connection_error)?;
        let (mut sender, connection) = timeout_at(
            execution.deadline,
            http1::Builder::new()
                .max_buf_size(MAX_HTTP_BUFFER)
                .handshake(connection),
        )
        .await
        .map_err(|_| TransportError::Deadline)?
        .map_err(TransportError::Http)?;
        let driver = Driver(runtime.spawn(async move {
            // The response future exposes request failures. This task owns only
            // socket progress and cannot select or replay another request.
            let _ = connection.await;
        }));
        timeout_at(execution.deadline, sender.ready())
            .await
            .map_err(|_| TransportError::Deadline)?
            .map_err(TransportError::Http)?;
        check_deadline(execution.deadline)?;
        Ok(ReadyExecution {
            execution,
            sender,
            request,
            driver,
        })
    }
}
fn execution_receipt(execution: &Execution) -> Result<RecoveryReceipt, Error> {
    RecoveryReceipt::restore(
        execution.client.config.endpoint.clone(),
        execution.query.id.clone(),
        execution.nonce.clone(),
        execution.principal.clone().ok_or(Error::InvalidRequest)?,
    )
    .map(|mut receipt| {
        receipt.log_since = execution.log_since;
        receipt
    })
}
impl ReadyExecution {
    pub(super) fn check_receiver_deadline(&self, before: u64) -> Result<(), TransportError> {
        self.execution
            .client
            .config
            .receiver_expiry
            .as_ref()
            .ok_or(TransportError::Receiver(crate::ReceiverError::Unreviewed))?
            .check(self.execution.principal.as_deref(), before)
            .map_err(TransportError::Receiver)
    }
    pub fn recovery_receipt(&self) -> Result<RecoveryReceipt, Error> {
        execution_receipt(&self.execution)
    }

    /// Admit exactly once to the ready transport before returning. No I/O is
    /// awaited here. Backpressure/expiry refuse before sending; a connection
    /// failure racing admission is uncertain and cannot authorize replay.
    /// Local expiry checking does not implement receiver-side fencing/deadlines.
    pub fn admit(mut self) -> Result<AdmittedResponse, TransportError> {
        check_deadline(self.execution.deadline)?;
        match self
            .sender
            .poll_ready(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Pending => return Err(TransportError::Backpressure),
            Poll::Ready(Err(error)) => return Err(TransportError::Http(error)),
            Poll::Ready(Ok(())) => {}
        }
        // hyper::SendRequest performs dispatch.send synchronously here. The
        // returned future only waits for that request's response.
        let permit = self
            .execution
            .permit
            .take()
            .ok_or(TransportError::InvalidRequest)?;
        let response = Box::pin(self.sender.send_request(self.request));
        let chunk_limit =
            MAX_HTTP_BUFFER.min(self.execution.client.config.resources.max_retained_bytes);
        let read_timer =
            crate::read_timeout::ReadTimer::new(self.execution.client.config.read_timeout);
        Ok(AdmittedResponse {
            read_timer,
            execution: self.execution,
            driver: self.driver,
            response: Some(response),
            body: None,
            pending: Bytes::new(),
            reservation: None,
            permit: Some(permit),
            chunk_limit,
            received: 0,
            eof: false,
            poisoned: false,
            termination_proven: false,
        })
    }
}

/// Shared wire-buffer ownership. Each chunk is detached once from Hyper's
/// potentially larger backing allocation, so its retained payload fits the byte
/// lease. Cloning shares that payload and lease without copying data. Allocation
/// metadata and bounded connection buffers are separate from this payload budget.
#[derive(Clone)]
pub struct ResponseChunk {
    bytes: Arc<[u8]>,
    lease: Arc<crate::resources::ByteLease>,
}
impl Deref for ResponseChunk {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl ResponseChunk {
    pub fn charged_bytes(&self) -> usize {
        self.lease.num_permits()
    }
}

/// A single dispatched request. Consuming its response never sends a query,
/// retries, follows redirects or refreshes credentials. EOF is only HTTP EOF:
/// decode and independent terminal verification remain required for result use.
/// Dropping without terminal proof quarantines the original client's active slot.
#[must_use]
pub struct AdmittedResponse {
    read_timer: crate::read_timeout::ReadTimer,
    execution: Execution,
    driver: Driver,
    response: Option<ResponseFuture>,
    body: Option<Incoming>,
    pending: Bytes,
    reservation: Option<crate::resources::ByteLease>,
    permit: Option<OwnedSemaphorePermit>,
    chunk_limit: usize,
    received: u64,
    eof: bool,
    poisoned: bool,
    termination_proven: bool,
}
impl AdmittedResponse {
    pub fn recovery_receipt(&self) -> Result<RecoveryReceipt, Error> {
        execution_receipt(&self.execution)
    }
    pub fn wire_bytes(&self) -> u64 {
        self.received
    }
    pub fn transport_eof(&self) -> bool {
        self.eof
    }

    /// Receive one bounded wire chunk. Cancelling this borrowed future preserves
    /// the pending response/body and byte reservation; the request is never replayed.
    pub async fn next_chunk(&mut self) -> Result<Option<ResponseChunk>, TransportError> {
        self.read_chunk(false).await
    }
    async fn next_internal_chunk(&mut self) -> Result<Option<ResponseChunk>, TransportError> {
        self.read_chunk(true).await
    }
    async fn read_chunk(
        &mut self,
        internal: bool,
    ) -> Result<Option<ResponseChunk>, TransportError> {
        if self.poisoned {
            return Err(TransportError::Poisoned);
        }
        if self.eof {
            return Ok(None);
        }
        tokio::runtime::Handle::try_current().map_err(TransportError::Runtime)?;
        let result = self.receive(internal).await;
        if result.is_err() {
            self.discard();
        }
        result
    }
    fn discard(&mut self) {
        self.poisoned = true;
        self.body = None;
        self.pending = Bytes::new();
        self.response = None;
        self.reservation = None;
        self.driver.0.abort();
        if self.termination_proven {
            self.permit = None;
        }
    }
    async fn receive(&mut self, internal: bool) -> Result<Option<ResponseChunk>, TransportError> {
        let deadline = self.execution.deadline;
        check_deadline(deadline)?;
        if self.reservation.is_none() {
            let resources = &self.execution.client.resources;
            self.reservation = Some(if internal {
                resources
                    .reserve_internal(self.chunk_limit, true, deadline)
                    .await?
            } else {
                resources.reserve_active(self.chunk_limit, deadline).await?
            });
        }
        if let Some(response) = &mut self.response {
            let response = timeout_at(deadline, self.read_timer.wait(crate::Phase::Data, response))
                .await
                .map_err(|_| TransportError::Deadline)??
                .map_err(TransportError::Http)?;
            check_deadline(deadline)?;
            check_exception(response.headers(), response.status().as_u16())?;
            if response.status() != hyper::StatusCode::OK {
                return Err(TransportError::HttpStatus(response.status().as_u16()));
            }
            if response.body().size_hint().lower()
                > self
                    .execution
                    .client
                    .config
                    .execution_limits
                    .max_total_bytes
            {
                return Err(TransportError::LimitExceeded);
            }
            self.response = None;
            self.body = Some(response.into_body());
        }
        loop {
            if !self.pending.is_empty() {
                let count = self.pending.len().min(self.chunk_limit);
                let bytes = self.pending.split_to(count);
                let mut lease = self.reservation.take().ok_or(TransportError::Poisoned)?;
                drop(lease.split(lease.num_permits().saturating_sub(count)));
                if !internal {
                    lease.make_external();
                }
                return Ok(Some(ResponseChunk {
                    bytes: Arc::from(bytes.as_ref()),
                    lease: Arc::new(lease),
                }));
            }
            let body = self.body.as_mut().ok_or(TransportError::Poisoned)?;
            let frame = timeout_at(
                deadline,
                self.read_timer.wait(
                    crate::Phase::Data,
                    poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)),
                ),
            )
            .await
            .map_err(|_| TransportError::Deadline)??;
            check_deadline(deadline)?;
            let Some(frame) = frame else {
                self.body = None;
                self.reservation = None;
                self.eof = true;
                if self.termination_proven {
                    self.permit = None;
                }
                return Ok(None);
            };
            let frame = frame.map_err(TransportError::Http)?;
            match frame.into_data() {
                Ok(bytes) => {
                    if bytes.is_empty() {
                        continue;
                    }
                    if bytes.len() > MAX_PENDING_FRAME {
                        return Err(TransportError::FrameLimit {
                            size: bytes.len(),
                            limit: MAX_PENDING_FRAME,
                        });
                    }
                    let size =
                        u64::try_from(bytes.len()).map_err(|_| TransportError::LimitExceeded)?;
                    let total = self
                        .received
                        .checked_add(size)
                        .ok_or(TransportError::LimitExceeded)?;
                    if total
                        > self
                            .execution
                            .client
                            .config
                            .execution_limits
                            .max_total_bytes
                    {
                        return Err(TransportError::LimitExceeded);
                    }
                    self.received = total;
                    self.pending = bytes;
                }
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        check_exception(&trailers, 200)?;
                    }
                }
            }
        }
    }
    /// Explicit, separate network operation: observe the original receipt. This
    /// is not part of response waiting. Success proves remote termination only,
    /// never data completeness. The active slot remains held until response EOF,
    /// a response failure/discard, or drop, so unread transport buffers cannot
    /// accumulate outside the active limit after early remote termination.
    pub async fn confirm_termination(&mut self) -> Result<RemoteTermination, TransportError> {
        tokio::runtime::Handle::try_current().map_err(TransportError::Runtime)?;
        let proof = self.execution.confirm_termination().await?;
        self.termination_proven = true;
        if self.eof || self.poisoned {
            self.permit = None;
        }
        Ok(proof)
    }
    /// Locally accept a separately authorized observation of this exact receipt.
    /// No network I/O occurs. An early proof retains the active slot while the
    /// original response is unread; it never establishes result completeness.
    pub fn accept_termination(
        &mut self,
        observed: &TerminalObservation,
    ) -> Result<RemoteTermination, TransportError> {
        if !observed.matches(&self.execution) {
            return Err(TransportError::ObservationMismatch);
        }
        self.execution.progress = observed.progress();
        self.termination_proven = true;
        if self.eof || self.poisoned {
            self.permit = None;
        }
        Ok(observed.termination())
    }
}
impl Drop for AdmittedResponse {
    fn drop(&mut self) {
        if !self.termination_proven
            && let Some(permit) = self.permit.take()
        {
            permit.forget();
        }
    }
}
fn check_deadline(deadline: Instant) -> Result<(), TransportError> {
    if Instant::now() >= deadline {
        Err(TransportError::Deadline)
    } else {
        Ok(())
    }
}
fn connection_error<E: Into<Box<dyn std::error::Error + Send + Sync>>>(error: E) -> TransportError {
    TransportError::Connect(error.into())
}
fn check_exception(headers: &HeaderMap, status: u16) -> Result<(), TransportError> {
    let mut values = headers.get_all("x-clickhouse-exception-code").iter();
    if let Some(code) = values.next() {
        if values.next().is_some() {
            return Err(TransportError::InvalidResponse);
        }
        // Only the exact non-error marker is acceptable, including in trailers.
        // Invalid/nonzero codes are server error indications, never zero defaults.
        if code.as_bytes() != b"0" {
            let error = Error::from_server_response(Phase::Data, status, code.to_str().ok(), &[]);
            if matches!(error.cause(), Some(Cause::ServerResponse(Some(code))) if code != 0) {
                return Err(TransportError::Client(error));
            }
            return Err(TransportError::ServerException);
        }
    }
    Ok(())
}

// Share the validated schema boundary without changing the admitted API.
pub(crate) use ipc::MeasuredMetadataGuard;
