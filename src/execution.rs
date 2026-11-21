// SPDX-License-Identifier: Apache-2.0
mod measured;
use measured::MeasuredCursor;
mod command;
pub use command::{CommandExecution, PreparedCommand, ReadyCommand};
mod transport;
pub use transport::{
    AdmittedArrow, AdmittedIpc, AdmittedResponse, DecodeError, IpcError, IpcFrame, IpcFrameKind,
    ReadyExecution, ResponseChunk, TransportError,
};

use crate::metrics::{TerminalMeasurement, timed};
use crate::{
    AntalyaClient, Cause, Error, ExecutionMetrics, Format, Phase, Query, TerminalStrategy,
    resources::Lane,
};
use arrow_array::RecordBatch;
use serde_json::Value;
use std::{ops::Deref, sync::Arc};
use tokio::{
    sync::OwnedSemaphorePermit,
    time::{Instant, timeout_at},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Unknown,
}

/// Verified remote termination, independently of trustworthy result delivery.
/// This proof never upgrades an Execution's data outcome to Succeeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteTermination {
    Succeeded,
    Failed,
    Cancelled,
}
/// Persist these fields before data dispatch to reconcile after process loss.
/// This is observation provenance, not a grant to execute or publish results.
#[derive(Clone)]
pub struct RecoveryReceipt {
    endpoint: crate::Endpoint,
    query_id: String,
    marker: String,
    principal: String,
    log_since: Option<u64>,
}
impl std::fmt::Debug for RecoveryReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecoveryReceipt([REDACTED])")
    }
}
impl RecoveryReceipt {
    /// Reconstruct trusted journal data. The host owns storage integrity and
    /// destination authorization; this validates only the bounded wire shape.
    pub fn restore(
        endpoint: crate::Endpoint,
        query_id: String,
        marker: String,
        principal: String,
    ) -> Result<Self, Error> {
        Self::validate_parts(&endpoint, &query_id, &marker, &principal)?;
        Ok(Self {
            endpoint,
            query_id,
            marker,
            principal,
            log_since: None,
        })
    }
    /// Restore the partition bound saved with this receipt. Old journals may
    /// omit it and retain an unbounded lookup; never substitute today's date.
    pub fn with_log_since(mut self, unix_seconds: u64) -> Result<Self, Error> {
        if unix_seconds > u64::from(u32::MAX) {
            return Err(Error::InvalidRequest);
        }
        self.log_since = Some(unix_seconds);
        Ok(self)
    }
    /// Persist this optional lower bound alongside the original identity fields.
    /// It is based on preparation time with a one-day clock-skew allowance.
    pub fn log_since(&self) -> Option<u64> {
        self.log_since
    }
    fn validate_parts(
        endpoint: &crate::Endpoint,
        query_id: &str,
        marker: &str,
        principal: &str,
    ) -> Result<(), Error> {
        if endpoint.0.as_str().len() > 4096
            || query_id.is_empty()
            || query_id.len() > 128
            || !query_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || marker.len() != 36
            || uuid::Uuid::parse_str(marker).is_err()
            || principal.trim().is_empty()
            || principal.len() > 1024
            || principal.chars().any(char::is_control)
        {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
    pub fn endpoint(&self) -> &str {
        self.endpoint.0.as_str()
    }
    pub fn query_id(&self) -> &str {
        &self.query_id
    }
    pub fn marker(&self) -> &str {
        &self.marker
    }
    pub fn principal(&self) -> &str {
        &self.principal
    }
}

/// Admission and identity are checked, but no data request exists yet.
/// Dropping this value releases local admission without dispatching a query.
/// The host must persist its receipt and revalidate authorization before start.
pub struct PreparedExecution {
    execution: Execution,
}
impl PreparedExecution {
    pub fn recovery_receipt(&self) -> Result<RecoveryReceipt, Error> {
        let execution = &self.execution;
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
    pub fn start(mut self) -> Result<Execution, Error> {
        if Instant::now() >= self.execution.deadline {
            return Err(Error::at(Phase::Data, Cause::Timeout));
        }
        self.execution.cursor = Some(MeasuredCursor::new(
            &self.execution.client,
            &self.execution.query,
        )?);
        Ok(self.execution)
    }
}

/// Observations never prove completion by themselves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub result_rows: Option<u64>,
}

/// A terminal query-log observation obtained through an authorized client, bound
/// to the exact original receipt. It is not serializable or constructible by a
/// caller. It proves remote termination, not response completeness or permission
/// to publish. The host must independently authorize the observation operation.
#[must_use]
pub struct TerminalObservation {
    receipt: RecoveryReceipt,
    termination: RemoteTermination,
    progress: Option<Progress>,
}
impl std::fmt::Debug for TerminalObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TerminalObservation")
            .field("termination", &self.termination)
            .finish_non_exhaustive()
    }
}
impl TerminalObservation {
    pub fn receipt(&self) -> &RecoveryReceipt {
        &self.receipt
    }
    pub fn termination(&self) -> RemoteTermination {
        self.termination
    }
    pub fn progress(&self) -> Option<Progress> {
        self.progress
    }
    fn matches(&self, execution: &Execution) -> bool {
        self.receipt.endpoint.0 == execution.client.config.endpoint.0
            && self.receipt.query_id == execution.query.id
            && self.receipt.marker == execution.nonce
            && Some(self.receipt.principal.as_str()) == execution.principal.as_deref()
    }
}

/// Retain this wrapper while retaining its Arrow arrays. Clones share the byte lease.
/// Cloning/extracting raw arrays bypasses accounting; decoder allocations are not bounded here.
#[derive(Clone)]
pub struct BudgetedBatch {
    batch: RecordBatch,
    lease: Arc<crate::resources::ByteLease>,
}
impl Deref for BudgetedBatch {
    type Target = RecordBatch;
    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}
impl std::fmt::Debug for BudgetedBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetedBatch")
            .field("rows", &self.batch.num_rows())
            .field("charged_bytes", &self.lease.num_permits())
            .finish()
    }
}
impl BudgetedBatch {
    /// Explicit escape hatch: retained buffers become the caller's responsibility.
    pub fn into_untracked(self) -> RecordBatch {
        self.batch
    }
    /// Canonical retained-storage charge used by this batch's budget lease.
    /// Shared allocations are deduplicated within the batch, not across owners.
    pub fn charged_bytes(&self) -> usize {
        self.lease.num_permits()
    }
}

/// Provisional incremental batches. Dropping this object is not confirmed remote cancellation.
pub struct Execution {
    client: AntalyaClient,
    query: Query,
    nonce: String,
    cursor: Option<MeasuredCursor>,
    deadline: Instant,
    started: Instant,
    outcome: Outcome,
    rows: u64,
    batches: u64,
    decoded_bytes: u64,
    progress: Option<Progress>,
    schema: Option<arrow_schema::SchemaRef>,
    permit: Option<OwnedSemaphorePermit>,
    pending_lease: Option<crate::resources::ByteLease>,
    stream_error: Option<Error>,
    metrics: ExecutionMetrics,
    principal: Option<String>,
    log_since: Option<u64>,
}
impl Execution {
    pub fn query_id(&self) -> &str {
        &self.query.id
    }
    pub fn outcome(&self) -> Outcome {
        self.outcome
    }
    pub fn rows_delivered(&self) -> u64 {
        self.rows
    }
    pub fn batches_delivered(&self) -> u64 {
        self.batches
    }
    pub fn decoded_bytes_delivered(&self) -> u64 {
        self.decoded_bytes
    }
    pub fn progress(&self) -> Option<Progress> {
        self.progress
    }
    pub fn schema(&self) -> Option<arrow_schema::SchemaRef> {
        self.schema.clone()
    }
    pub fn metrics(&self) -> ExecutionMetrics {
        let mut metrics = self.metrics.clone();
        if let Some(cursor) = &self.cursor {
            cursor.update_metrics(&mut metrics);
        }
        if self.outcome == Outcome::Running {
            metrics.total = self.started.elapsed();
        }
        metrics
    }
    fn stop(&mut self, outcome: Outcome, error: Option<Error>) {
        if let Some(cursor) = &self.cursor {
            cursor.update_metrics(&mut self.metrics);
        }
        self.cursor = None;
        self.permit = None;
        self.pending_lease = None;
        self.outcome = outcome;
        self.metrics.total = self.started.elapsed();
        self.metrics.last_error = error;
    }
    fn refuse(&mut self, error: Error) -> Error {
        self.stop(Outcome::Unknown, Some(error));
        error
    }

    /// Untracked batch API. Returned arrays are outside shared retained-byte accounting.
    /// Per-batch/cumulative limits still apply. Prefer `next_batch` for bounded consumers.
    pub async fn next(&mut self) -> Result<Option<RecordBatch>, Error> {
        Ok(self.next_batch().await?.map(BudgetedBatch::into_untracked))
    }
    pub async fn next_batch(&mut self) -> Result<Option<BudgetedBatch>, Error> {
        let batch = self.next_provisional_batch().await?;
        if batch.is_none() {
            self.confirm_completion().await?;
        }
        Ok(batch)
    }

    /// Receive bounded data without waiting for terminal proof at clean stream EOF.
    /// `None` is provisional: the outcome remains Running until confirmation.
    /// Retain this Execution and call `confirm_completion` before treating the
    /// received rows as complete. Dropping it does not prove remote cancellation.
    pub async fn next_provisional_batch(&mut self) -> Result<Option<BudgetedBatch>, Error> {
        if self.outcome != Outcome::Running {
            return match self.outcome {
                Outcome::Succeeded => Ok(None),
                _ => Err(self.metrics.last_error.unwrap_or(Error::Unconfirmed)),
            };
        }
        if self.cursor.is_none() {
            if self.metrics.stream_end.is_some() && self.stream_error.is_none() {
                return Ok(None);
            }
            // A failed/cancelled data poll is not clean EOF. Preserve the strict
            // path's reconciliation and error instead of publishing a preview.
            return self.finish().await;
        }
        // The lease lives in Execution across cancellation of a next_batch future,
        // just like partial IPC buffered by the cancellation-safe upstream cursor.
        if self.pending_lease.is_none() {
            let reserved = timed(
                &mut self.metrics.queued,
                self.client.resources.reserve_bytes(
                    self.client.config.execution_limits.max_batch_bytes,
                    self.deadline,
                ),
            )
            .await;
            self.pending_lease = Some(match reserved {
                Ok(lease) => lease,
                Err(error) => return Err(self.refuse(error)),
            });
        }
        let next = timed(
            &mut self.metrics.data_poll,
            timeout_at(
                self.deadline,
                self.cursor.as_mut().ok_or(Error::InvalidRequest)?.next(),
            ),
        )
        .await;
        if let Some(cursor) = &self.cursor {
            cursor.update_metrics(&mut self.metrics);
        }
        self.schema = self.cursor.as_ref().and_then(|cursor| cursor.schema());
        if Instant::now() >= self.deadline {
            return Err(self.refuse(Error::at(Phase::Data, Cause::Timeout)));
        }
        match next {
            Ok(Ok(Some(batch))) => {
                let limits = &self.client.config.execution_limits;
                let Some(bytes) = crate::memory::batch_bytes(&batch, limits.max_batch_bytes) else {
                    return Err(self.refuse(Error::at(Phase::Data, Cause::LimitExceeded)));
                };
                let totals = self
                    .rows
                    .checked_add(batch.num_rows() as u64)
                    .zip(self.batches.checked_add(1))
                    .zip(self.decoded_bytes.checked_add(bytes as u64));
                let Some(((rows, batches), decoded_bytes)) = totals else {
                    return Err(self.refuse(Error::at(Phase::Data, Cause::LimitExceeded)));
                };
                if bytes > limits.max_batch_bytes
                    || rows > limits.max_rows
                    || batches > limits.max_batches
                    || decoded_bytes > limits.max_total_bytes
                {
                    return Err(self.refuse(Error::at(Phase::Data, Cause::LimitExceeded)));
                }
                // Return unused reservation, retain the exact conservative Arrow memory charge.
                let mut lease = self.pending_lease.take().ok_or(Error::InvalidRequest)?;
                drop(lease.split(lease.num_permits().saturating_sub(bytes)));
                self.rows = rows;
                self.batches = batches;
                self.decoded_bytes = decoded_bytes;
                if self.metrics.first_batch.is_none() {
                    self.metrics.first_batch = Some(self.started.elapsed());
                }
                Ok(Some(BudgetedBatch {
                    batch,
                    lease: Arc::new(lease),
                }))
            }
            Ok(Ok(None)) => {
                self.progress = self.cursor.as_ref().and_then(|cursor| cursor.terminal());
                self.pending_lease = None;
                self.cursor = None;
                self.metrics.stream_end = Some(self.started.elapsed());
                Ok(None)
            }
            Ok(Err(error)) => {
                self.pending_lease = None;
                self.cursor = None;
                self.stream_error = Some(error);
                self.finish().await
            }
            Err(_) => Err(self.refuse(Error::at(Phase::Data, Cause::Timeout))),
        }
    }

    /// Confirm a fully received provisional stream using the original private
    /// proof, principal, exact row count and operation deadline. Calling this
    /// before clean data EOF is an error and does not consume or discard data.
    pub async fn confirm_completion(&mut self) -> Result<(), Error> {
        if self.outcome != Outcome::Running {
            return match self.outcome {
                Outcome::Succeeded => Ok(()),
                _ => Err(self.metrics.last_error.unwrap_or(Error::Unconfirmed)),
            };
        }
        if self.cursor.is_some() || self.metrics.stream_end.is_none() {
            return Err(Error::InvalidRequest);
        }
        self.finish().await.map(|_| ())
    }

    async fn finish(&mut self) -> Result<Option<BudgetedBatch>, Error> {
        // These refusals may precede query-log insertion. Report promptly while
        // preserving uncertainty; a response code is not remote terminal proof.
        if let Some(error) = self.stream_error.filter(|error| {
            error.is_pre_execution_refusal() || error.cause() == Some(Cause::ReadTimeout)
        }) {
            return Err(self.refuse(error));
        }
        if self.client.config.terminal_policy.strategy == TerminalStrategy::InBand {
            let begin = Instant::now();
            let error = self.stream_error.or_else(|| {
                if Instant::now() >= self.deadline {
                    return Some(Error::at(Phase::Control, Cause::Timeout));
                }
                (self.schema.is_none()
                    || self.metrics.stream_end.is_none()
                    || self.progress.and_then(|p| p.result_rows) != Some(self.rows))
                .then(|| Error::at(Phase::Control, Cause::InvalidProof))
            });
            self.metrics.terminal = self.metrics.terminal.saturating_add(begin.elapsed());
            let outcome = match error {
                None => Outcome::Succeeded,
                Some(e) if e.kind() == crate::ErrorKind::QueryFailed => Outcome::Failed,
                Some(_) => Outcome::Unknown,
            };
            self.stop(outcome, error);
            return match error {
                Some(error) => Err(error),
                None => Ok(None),
            };
        }
        let (mut outcome, progress) = self
            .client
            .terminal_until(
                &self.query.id,
                &self.nonce,
                self.deadline,
                &mut self.metrics,
                self.principal.as_deref(),
                self.log_since,
            )
            .await;
        self.progress = progress;
        if let Some(error) = self.stream_error {
            if !matches!(outcome, Outcome::Failed | Outcome::Cancelled) {
                outcome = Outcome::Unknown;
            }
            self.stop(outcome, Some(error));
            return Err(error);
        }
        if outcome == Outcome::Succeeded
            && (self.schema.is_none() || progress.and_then(|p| p.result_rows) != Some(self.rows))
        {
            outcome = Outcome::Unknown;
            self.metrics.last_error = Some(Error::at(Phase::Control, Cause::InvalidProof));
        }
        let error = match outcome {
            Outcome::Succeeded => None,
            Outcome::Failed | Outcome::Cancelled => {
                Some(Error::at(Phase::Control, Cause::QueryFailed))
            }
            _ => Some(
                self.metrics
                    .last_error
                    .unwrap_or(Error::at(Phase::Control, Cause::InvalidProof)),
            ),
        };
        self.stop(outcome, error);
        match error {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }
    pub async fn refresh_progress(&mut self) -> Result<Option<Progress>, Error> {
        let query = probe("SELECT read_rows, read_bytes FROM system.processes WHERE query_id={id:String} AND Settings['log_comment']={nonce:String} AND user=currentUser()")?
            .parameter("id", &self.query.id)?.parameter("nonce", &self.nonce)?;
        self.metrics.control_requests = self.metrics.control_requests.saturating_add(1);
        let value = match self
            .client
            .json_until(&query, self.deadline, Phase::Progress)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                self.metrics.last_control_error = Some(error);
                return Err(error);
            }
        };
        self.progress = value["data"]
            .as_array()
            .filter(|rows| rows.len() == 1)
            .and_then(|rows| progress(&rows[0]));
        Ok(self.progress)
    }
    /// Recheck the original execution marker and principal with a fresh bounded
    /// deadline. A host may release quarantine after this proof, but must not
    /// publish discarded, partial or malformed results. `outcome()` is unchanged.
    /// This observes the original query; it never re-executes it or issues KILL.
    pub async fn confirm_termination(&mut self) -> Result<RemoteTermination, Error> {
        let deadline = Instant::now()
            .checked_add(self.client.config.timeout)
            .ok_or(Error::InvalidSetting)?;
        let (terminal, progress) = self
            .client
            .terminal_until(
                &self.query.id,
                &self.nonce,
                deadline,
                &mut self.metrics,
                self.principal.as_deref(),
                self.log_since,
            )
            .await;
        self.progress = progress;
        match terminal {
            Outcome::Succeeded => Ok(RemoteTermination::Succeeded),
            Outcome::Failed => Ok(RemoteTermination::Failed),
            Outcome::Cancelled => Ok(RemoteTermination::Cancelled),
            Outcome::Unknown | Outcome::Running => Err(self
                .metrics
                .last_error
                .unwrap_or(Error::at(Phase::Control, Cause::InvalidProof))),
        }
    }

    pub async fn cancel(&mut self) -> Result<Outcome, Error> {
        if matches!(self.outcome, Outcome::Succeeded | Outcome::Cancelled) {
            return Ok(self.outcome);
        }
        let query = probe("KILL QUERY WHERE query_id={id:String} AND Settings['log_comment']={nonce:String} AND user=currentUser() SYNC")?
            .parameter("id", &self.query.id)?.parameter("nonce", &self.nonce)?;
        let deadline = Instant::now()
            .checked_add(self.client.config.timeout)
            .ok_or(Error::InvalidSetting)?;
        if let Some(cursor) = &self.cursor {
            cursor.update_metrics(&mut self.metrics);
        }
        self.cursor = None;
        self.pending_lease = None;
        self.stream_error = Some(Error::at(Phase::Cancellation, Cause::InvalidProof));
        self.metrics.control_requests = self.metrics.control_requests.saturating_add(1);
        if let Err(error) = self.client.kill_until(&query, deadline).await {
            return Err(self.refuse(error));
        }
        let (terminal, progress) = self
            .client
            .terminal_until(
                &self.query.id,
                &self.nonce,
                deadline,
                &mut self.metrics,
                self.principal.as_deref(),
                self.log_since,
            )
            .await;
        self.progress = progress;
        // Discarded provisional results cannot be turned into successful delivery by KILL.
        let outcome = if terminal == Outcome::Succeeded {
            Outcome::Unknown
        } else {
            terminal
        };
        let error = if outcome == Outcome::Unknown {
            Some(
                self.metrics
                    .last_error
                    .unwrap_or(Error::at(Phase::Cancellation, Cause::InvalidProof)),
            )
        } else {
            None
        };
        self.stop(outcome, error);
        match error {
            Some(error) => Err(error),
            None => Ok(outcome),
        }
    }
}

pub(crate) fn probe(sql: &str) -> Result<Query, Error> {
    Query::new(sql, format!("antalya-probe-{}", uuid::Uuid::new_v4()))
}
pub(crate) fn tracked(mut query: Query) -> (Query, String, Option<u64>) {
    let nonce = uuid::Uuid::new_v4().to_string();
    for (k, v) in [
        ("log_queries", "1"),
        ("log_queries_probability", "1"),
        ("log_queries_min_query_duration_ms", "0"),
        ("log_query_settings", "1"),
        ("log_comment", nonce.as_str()),
    ] {
        query.settings.insert(k.into(), v.into());
    }
    // Bound partition reads from this execution's preparation, not from the
    // observer's current date. Old receipts without a bound remain supported.
    let log_since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|elapsed| elapsed.as_secs().saturating_sub(86_400))
        .filter(|seconds| *seconds <= u64::from(u32::MAX));
    (query, nonce, log_since)
}
fn number(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}
fn progress(row: &Value) -> Option<Progress> {
    Some(Progress {
        read_rows: number(&row["read_rows"])?,
        read_bytes: number(&row["read_bytes"])?,
        result_rows: number(&row["result_rows"]),
    })
}

#[derive(Clone, Debug)]
pub struct CommandReport {
    pub outcome: Outcome,
    pub metrics: ExecutionMetrics,
}
fn control_json(bytes: &[u8], phase: Phase) -> Result<Value, Error> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| Error::at(phase, Cause::JsonDecoder))?;
    if value.get("exception").is_some() || value.get("error").is_some() {
        return Err(Error::at(phase, Cause::ServerResponse(None)));
    }
    Ok(value)
}

impl AntalyaClient {
    pub async fn fetch_authorized_arrow(
        &self,
        request: crate::AuthorizedRequest,
    ) -> Result<Execution, Error> {
        self.prepare_authorized_arrow(request).await?.start()
    }
    /// Prepare the lifecycle marker without dispatch. Persist recovery_receipt()
    /// before calling start(); the original deadline includes journal latency.
    pub async fn prepare_authorized_arrow(
        &self,
        request: crate::AuthorizedRequest,
    ) -> Result<PreparedExecution, Error> {
        if self.config.endpoint.0 != request.endpoint.0
            || request.format != Format::ArrowStream
            || (request.query.database.is_none() && self.config.database.is_some())
        {
            return Err(Error::InvalidRequest);
        }
        let principal = request.expected_principal().to_owned();
        self.prepare_execution(request.query, Some(principal)).await
    }
    pub async fn fetch_arrow(&self, query: Query) -> Result<Execution, Error> {
        self.prepare_execution(query, None).await?.start()
    }
    async fn prepare_execution(
        &self,
        query: Query,
        principal: Option<String>,
    ) -> Result<PreparedExecution, Error> {
        let started = Instant::now();
        let deadline = started
            .checked_add(self.config.timeout)
            .ok_or(Error::InvalidSetting)?;
        let permit = self.resources.acquire(Lane::Data, deadline).await?;
        let mut metrics = ExecutionMetrics {
            queued: started.elapsed(),
            ..ExecutionMetrics::default()
        };
        if let Some(principal) = principal.as_deref() {
            let begin = Instant::now();
            let identity = self
                .json_until(
                    &probe("SELECT currentUser() AS principal")?,
                    deadline,
                    Phase::Identity,
                )
                .await?;
            metrics.identity = begin.elapsed();
            metrics.control_requests = 1;
            if identity["data"]
                .as_array()
                .is_none_or(|rows| rows.len() != 1)
                || identity["data"][0]["principal"].as_str() != Some(principal)
            {
                return Err(Error::at(Phase::Identity, Cause::IdentityMismatch));
            }
        }
        let (query, nonce, log_since) = tracked(self.scoped(&query));
        Ok(PreparedExecution {
            execution: Execution {
                client: self.clone(),
                query,
                nonce,
                cursor: None,
                deadline,
                started,
                outcome: Outcome::Running,
                rows: 0,
                batches: 0,
                decoded_bytes: 0,
                progress: None,
                schema: None,
                permit: Some(permit),
                pending_lease: None,
                stream_error: None,
                metrics,
                principal,
                log_since,
            },
        })
    }
    /// Observe termination from a durable receipt; never replay the data query.
    /// No returned proof can recover lost data or make partial data publishable.
    pub async fn confirm_termination(
        &self,
        receipt: &RecoveryReceipt,
    ) -> Result<RemoteTermination, Error> {
        Ok(self.observe_terminal(receipt).await?.termination())
    }
    /// Observe the original attempt with this client's current credentials.
    /// The endpoint and authenticated principal must match the retained receipt.
    /// No data query is replayed and no credentials in a pending execution are
    /// replaced. The returned observation can be checked locally against that
    /// execution; it does not by itself validate delivered rows.
    pub async fn observe_terminal(
        &self,
        receipt: &RecoveryReceipt,
    ) -> Result<TerminalObservation, Error> {
        if self.config.endpoint.0 != receipt.endpoint.0 {
            return Err(Error::InvalidRequest);
        }
        let deadline = Instant::now()
            .checked_add(self.config.timeout)
            .ok_or(Error::InvalidSetting)?;
        let mut metrics = ExecutionMetrics::default();
        let (outcome, progress) = self
            .terminal_until(
                &receipt.query_id,
                &receipt.marker,
                deadline,
                &mut metrics,
                Some(&receipt.principal),
                receipt.log_since,
            )
            .await;
        let termination = match outcome {
            Outcome::Succeeded => RemoteTermination::Succeeded,
            Outcome::Failed => RemoteTermination::Failed,
            Outcome::Cancelled => RemoteTermination::Cancelled,
            Outcome::Unknown | Outcome::Running => {
                return Err(metrics
                    .last_error
                    .unwrap_or(Error::at(Phase::Control, Cause::InvalidProof)));
            }
        };
        Ok(TerminalObservation {
            receipt: receipt.clone(),
            termination,
            progress,
        })
    }
    /// Unvalidated escape hatch: the caller owns admission, byte limits and terminal proof.
    pub fn fetch_bytes(
        &self,
        query: &Query,
        format: Format,
    ) -> Result<clickhouse::query::BytesCursor, Error> {
        self.scoped(query)
            .fetch_bytes(&self.upstream, format)
            .map_err(|e| Error::from_upstream(Phase::Data, &e))
    }
    pub async fn execute_command(&self, query: Query) -> Result<Outcome, Error> {
        let report = self.execute_command_report(query).await?;
        if let Some(error) = report
            .metrics
            .last_error
            .filter(|e| e.is_pre_execution_refusal())
        {
            return Err(error);
        }
        Ok(report.outcome)
    }
    pub async fn execute_command_report(&self, query: Query) -> Result<CommandReport, Error> {
        if self.config.terminal_policy.strategy == TerminalStrategy::InBand {
            return Err(Error::Unsupported);
        }
        let started = Instant::now();
        let deadline = started
            .checked_add(self.config.timeout)
            .ok_or(Error::InvalidSetting)?;
        let _permit = self.resources.acquire(Lane::Data, deadline).await?;
        let mut metrics = ExecutionMetrics {
            queued: started.elapsed(),
            ..ExecutionMetrics::default()
        };
        let (query, nonce, log_since) = tracked(self.scoped(&query));
        let result = timed(
            &mut metrics.data_poll,
            complete_before(
                deadline,
                Phase::Data,
                query.upstream(&self.upstream).execute(),
            ),
        )
        .await;
        match result {
            Err(error) => return Err(error),
            Ok(Err(e)) => metrics.last_error = Some(Error::from_upstream(Phase::Data, &e)),
            Ok(Ok(())) => (),
        }
        if metrics
            .last_error
            .is_some_and(|error| error.is_pre_execution_refusal())
        {
            metrics.total = started.elapsed();
            return Ok(CommandReport {
                outcome: Outcome::Unknown,
                metrics,
            });
        }
        // Transport failure cannot establish whether command side effects ran.
        let (outcome, _) = self
            .terminal_until(&query.id, &nonce, deadline, &mut metrics, None, log_since)
            .await;
        metrics.total = started.elapsed();
        Ok(CommandReport { outcome, metrics })
    }
    async fn bounded_response_until(
        &self,
        query: &Query,
        format: Option<Format>,
        deadline: Instant,
        phase: Phase,
    ) -> Result<Vec<u8>, Error> {
        let _permit = self.resources.acquire(Lane::Control, deadline).await?;
        complete_before(deadline, phase, async {
            let request = match format {
                Some(format) => self.build_request(query, format)?,
                None => self.build_command_request(query)?,
            };
            let mut read_timer = crate::read_timeout::ReadTimer::new(self.config.read_timeout);
            let mut response = read_timer
                .wait(phase, self.http.execute(request))
                .await?
                .map_err(|e| Error::from_reqwest(phase, &e))?;
            if !response.status().is_success() {
                return Err(server_response_error(response, phase, read_timer).await);
            }
            const CAP: usize = 1_048_576;
            if response.content_length().is_some_and(|n| n > CAP as u64) {
                return Err(Error::at(phase, Cause::LimitExceeded));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = read_timer
                .wait(phase, response.chunk())
                .await?
                .map_err(|e| Error::from_reqwest(phase, &e))?
            {
                if chunk.len() > CAP.saturating_sub(bytes.len()) {
                    return Err(Error::at(phase, Cause::LimitExceeded));
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        })
        .await?
    }
    pub(crate) async fn json(&self, query: &Query) -> Result<Value, Error> {
        let deadline = Instant::now()
            .checked_add(self.config.timeout)
            .ok_or(Error::InvalidSetting)?;
        self.json_until(query, deadline, Phase::Control).await
    }
    async fn json_until(
        &self,
        query: &Query,
        deadline: Instant,
        phase: Phase,
    ) -> Result<Value, Error> {
        complete_before(deadline, phase, async {
            let bytes = self
                .bounded_response_until(query, Some(Format::Json), deadline, phase)
                .await?;
            control_json(&bytes, phase)
        })
        .await?
    }
    async fn kill_until(&self, query: &Query, deadline: Instant) -> Result<(), Error> {
        let phase = Phase::Cancellation;
        complete_before(deadline, phase, async {
            let bytes = self
                .bounded_response_until(query, Some(Format::Json), deadline, phase)
                .await?;
            // Antalya returns HTTP 200 with no body when KILL matches no active
            // query, including a completion race. This is only an acknowledgement:
            // cancel still requires original-receipt terminal observation below.
            if !bytes.is_empty() {
                control_json(&bytes, phase)?;
            }
            Ok(())
        })
        .await?
    }
    pub(crate) async fn terminal_until(
        &self,
        id: &str,
        nonce: &str,
        deadline: Instant,
        metrics: &mut ExecutionMetrics,
        principal: Option<&str>,
        log_since: Option<u64>,
    ) -> (Outcome, Option<Progress>) {
        let mut measurement = TerminalMeasurement::new(metrics);
        let result = complete_before(
            deadline,
            Phase::Control,
            self.terminal_checked(
                id,
                nonce,
                deadline,
                &mut measurement.metrics,
                principal,
                log_since,
            ),
        )
        .await;
        match result {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                measurement.metrics.last_error = Some(error);
                (Outcome::Unknown, None)
            }
            Err(error) => {
                measurement.metrics.last_error = Some(error);
                (Outcome::Unknown, None)
            }
        }
    }
    async fn flush_logs(&self, deadline: Instant, metrics: &mut ExecutionMetrics) {
        metrics.control_requests = metrics.control_requests.saturating_add(1);
        metrics.flush_attempts = metrics.flush_attempts.saturating_add(1);
        let result = timed(&mut metrics.flush_time, async {
            let result = match probe("SYSTEM FLUSH LOGS query_log") {
                Ok(query) => {
                    self.bounded_response_until(&query, None, deadline, Phase::Control)
                        .await
                }
                Err(error) => Err(error),
            };
            result.and_then(|body| {
                if body.iter().all(u8::is_ascii_whitespace) {
                    Ok(())
                } else {
                    Err(Error::at(Phase::Control, Cause::InvalidProof))
                }
            })
        })
        .await;
        if let Err(error) = result {
            metrics.flush_failures = metrics.flush_failures.saturating_add(1);
            metrics.last_control_error = Some(error);
        }
    }
    async fn terminal_checked(
        &self,
        id: &str,
        nonce: &str,
        deadline: Instant,
        metrics: &mut ExecutionMetrics,
        principal: Option<&str>,
        log_since: Option<u64>,
    ) -> Result<(Outcome, Option<Progress>), Error> {
        let policy = &self.config.terminal_policy;
        let mut delay = policy.initial_delay;
        let mut flushed = policy.strategy == TerminalStrategy::FlushFirst;
        if flushed {
            self.flush_logs(deadline, metrics).await;
        }
        loop {
            let sql = "SELECT countIf(type = 'QueryStart') AS starts, countIf(type = 'QueryFinish') AS finishes, countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')) AS errors, countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 394) AS cancelled, max(read_rows) AS read_rows, max(read_bytes) AS read_bytes, max(result_rows) AS result_rows FROM system.query_log WHERE query_id={id:String} AND log_comment={nonce:String} AND is_initial_query=1 AND user=currentUser()";
            let sql = if log_since.is_some() {
                format!("{sql} AND event_date >= toDate(toDateTime({{log_since:UInt64}}))")
            } else {
                sql.into()
            };
            let mut query = probe(&if principal.is_some() {
                format!("{sql} AND user={{principal:String}} ")
            } else {
                sql
            })?
            .parameter("id", id)?
            .parameter("nonce", nonce)?;
            if let Some(log_since) = log_since {
                query = query.typed_parameter("log_since", crate::Parameter::UInt64(log_since))?;
            }
            if let Some(principal) = principal {
                query = query.parameter("principal", principal)?;
            }
            metrics.control_requests = metrics.control_requests.saturating_add(1);
            metrics.polls = metrics.polls.saturating_add(1);
            let value = timed(
                &mut metrics.observe_time,
                self.json_until(&query, deadline, Phase::Control),
            )
            .await?;
            let rows = value["data"]
                .as_array()
                .filter(|rows| rows.len() == 1)
                .ok_or(Error::at(Phase::Control, Cause::InvalidProof))?;
            let row = &rows[0];
            let counts = ["starts", "finishes", "errors", "cancelled"]
                .map(|key| number(&row[key]).ok_or(Error::at(Phase::Control, Cause::InvalidProof)));
            let [starts, finishes, errors, cancelled] = counts;
            let (starts, finishes, errors, cancelled) = (starts?, finishes?, errors?, cancelled?);
            if cancelled > errors {
                return Err(Error::at(Phase::Control, Cause::InvalidProof));
            }
            if starts <= 1 && finishes == 0 && errors == 0 {
                if !flushed
                    && !matches!(
                        policy.strategy,
                        TerminalStrategy::ObserveOnly | TerminalStrategy::InBand
                    )
                {
                    self.flush_logs(deadline, metrics).await;
                    flushed = true;
                    continue;
                }
                metrics.poll_sleeps = metrics.poll_sleeps.saturating_add(1);
                timed(&mut metrics.poll_wait, tokio::time::sleep(delay)).await;
                delay = delay.saturating_mul(2).min(policy.max_delay);
                continue;
            }
            let outcome = if starts > 1 || finishes.checked_add(errors) != Some(1) {
                Outcome::Unknown
            } else if errors == 1 && cancelled == 1 {
                Outcome::Cancelled
            } else if errors == 1 {
                Outcome::Failed
            } else if starts == 1 && finishes == 1 {
                Outcome::Succeeded
            } else {
                Outcome::Unknown
            };
            if outcome == Outcome::Unknown {
                return Err(Error::at(Phase::Control, Cause::InvalidProof));
            }
            return Ok((outcome, progress(row)));
        }
    }
}

/// Keep only the bounded numeric prefix, never arbitrary error messages. This
/// runs inside the caller's existing deadline and drops the rest of the body.
pub(crate) async fn server_response_error(
    mut response: reqwest::Response,
    phase: Phase,
    mut read_timer: crate::read_timeout::ReadTimer,
) -> Error {
    let status = response.status().as_u16();
    let header = response
        .headers()
        .get("x-clickhouse-exception-code")
        .and_then(|h| h.to_str().ok());
    let error = Error::from_server_response(phase, status, header, &[]);
    if matches!(error.cause(), Some(Cause::ServerResponse(_))) {
        return error;
    }
    let mut prefix = [0_u8; 32];
    let mut used = 0;
    while used < prefix.len() {
        let chunk = match read_timer.wait(phase, response.chunk()).await {
            Ok(Ok(Some(chunk))) => chunk,
            Err(error) => return error,
            Ok(Ok(None) | Err(_)) => break,
        };
        let count = chunk.len().min(prefix.len() - used);
        prefix[used..used + count].copy_from_slice(&chunk[..count]);
        used += count;
        if prefix[..used].contains(&b'.') {
            break;
        }
    }
    Error::from_server_response(phase, status, None, &prefix[..used])
}

async fn complete_before<T>(
    deadline: Instant,
    phase: Phase,
    future: impl std::future::Future<Output = T>,
) -> Result<T, Error> {
    if Instant::now() >= deadline {
        return Err(Error::at(phase, Cause::Timeout));
    }
    let result = timeout_at(deadline, future)
        .await
        .map_err(|_| Error::at(phase, Cause::Timeout))?;
    // Tokio polls ready work before its timer. Synchronous decoding/proof work
    // can also cross the deadline without yielding, so check completion itself.
    if Instant::now() >= deadline {
        return Err(Error::at(phase, Cause::Timeout));
    }
    Ok(result)
}

#[cfg(test)]
mod deadline_tests {
    use super::*;
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    #[tokio::test]
    async fn expired_ready_completion_is_refused() {
        let error = complete_before(Instant::now(), Phase::Progress, async { 7 })
            .await
            .unwrap_err();
        assert_eq!(error, Error::at(Phase::Progress, Cause::Timeout));
    }

    #[tokio::test]
    async fn ready_waiter_resumed_after_deadline_is_refused() {
        let expires = Instant::now() + Duration::from_millis(20);
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut completion = Box::pin(complete_before(expires, Phase::Control, receive));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            completion.as_mut().poll(&mut context),
            Poll::Pending
        ));
        send.send(7).unwrap();
        tokio::time::sleep_until(expires + Duration::from_millis(1)).await;
        assert_eq!(
            completion.await.unwrap_err(),
            Error::at(Phase::Control, Cause::Timeout)
        );
    }

    #[tokio::test]
    async fn synchronous_decode_crossing_deadline_is_refused() {
        let expires = Instant::now() + Duration::from_millis(20);
        let error = complete_before(expires, Phase::Identity, async {
            // Model synchronous decoding while the runtime cannot poll its timer.
            std::thread::sleep(
                expires.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
            );
            serde_json::from_slice::<Value>(br#"{"data":[{"principal":"fixture"}]}"#).unwrap()
        })
        .await
        .unwrap_err();
        assert_eq!(error, Error::at(Phase::Identity, Cause::Timeout));
    }

    #[tokio::test]
    async fn timely_completion_preserves_values_and_typed_failures() {
        let expires = Instant::now() + Duration::from_secs(1);
        assert_eq!(
            complete_before(expires, Phase::Control, async { 7 })
                .await
                .unwrap(),
            7
        );
        let error = Error::at(Phase::Control, Cause::HttpStatus(403));
        let result = complete_before(expires, Phase::Control, async { Err::<(), _>(error) })
            .await
            .unwrap();
        assert_eq!(result.unwrap_err(), error);
    }
}
