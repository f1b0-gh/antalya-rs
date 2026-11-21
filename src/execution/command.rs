// SPDX-License-Identifier: Apache-2.0
use super::{
    AdmittedResponse, CommandReport, Outcome, PreparedExecution, ReadyExecution, RecoveryReceipt,
    TransportError, complete_before, timed,
};
use crate::{AntalyaClient, AuthorizedRequest, Cause, Error, Format, Phase};
use tokio::time::Instant;

/// An admitted, identity-checked command that has not been dispatched.
/// Persist its receipt and revalidate host authorization before starting it.
/// Dropping a preparation releases admission without executing the command.
#[must_use]
pub struct PreparedCommand {
    prepared: PreparedExecution,
}

impl PreparedCommand {
    pub fn recovery_receipt(&self) -> Result<RecoveryReceipt, Error> {
        self.prepared.recovery_receipt()
    }
    /// Establish the original command's connection without dispatching it.
    /// DNS/TCP/TLS preparation consumes the original deadline. Retain the receipt
    /// and recheck host authority before synchronous admission on ReadyCommand.
    pub async fn ready_transport(self) -> Result<ReadyCommand, TransportError> {
        Ok(ReadyCommand {
            transport: self.prepared.ready_command_transport().await?,
        })
    }

    /// Transfers ownership without dispatch. Only polling `run` sends the command.
    pub fn start(self) -> Result<CommandExecution, Error> {
        if Instant::now() >= self.prepared.execution.deadline {
            return Err(Error::at(Phase::Data, Cause::Timeout));
        }
        Ok(CommandExecution {
            prepared: self.prepared,
            dispatched: false,
        })
    }
}

/// A ready command transport, without an appended result FORMAT. Its response
/// owner only receives bytes; terminal controls must be separately authorized.
#[must_use]
pub struct ReadyCommand {
    transport: ReadyExecution,
}
impl ReadyCommand {
    pub fn recovery_receipt(&self) -> Result<RecoveryReceipt, Error> {
        self.transport.recovery_receipt()
    }
    /// Synchronously admit once, without enforcing an external receiver deadline.
    /// HTTP success/EOF is not command success. Accept an exact terminal observation
    /// separately; after a lost reply, command effects can still be proven by the
    /// original receipt, unlike lost SELECT result bytes.
    pub fn admit(self) -> Result<AdmittedResponse, TransportError> {
        self.transport.admit()
    }
    /// Require the frozen reviewed JWT refusal bound to fit the host's Unix-second
    /// deadline, then admit synchronously. The host separately authorizes this
    /// operation and maintains the reviewed endpoint/clock policy.
    pub fn admit_before(self, before: u64) -> Result<AdmittedResponse, TransportError> {
        self.transport.check_receiver_deadline(before)?;
        self.admit()
    }
}

/// Owns one command attempt and its admission until remote termination is proven.
/// Cancelling a borrowed `run` future leaves this owner available for reconciliation.
/// Dropping an unresolved dispatched attempt permanently quarantines its slot in
/// this client's shared admission pool. Retain the owner and reconcile instead.
/// After process loss the host must reconcile durable receipts before recreating
/// admission or issuing new attempts; a new client alone does not make retries safe.
#[must_use]
pub struct CommandExecution {
    prepared: PreparedExecution,
    dispatched: bool,
}

impl CommandExecution {
    pub fn recovery_receipt(&self) -> Result<RecoveryReceipt, Error> {
        self.prepared.recovery_receipt()
    }

    pub fn report(&self) -> CommandReport {
        let execution = &self.prepared.execution;
        let mut metrics = execution.metrics.clone();
        if matches!(execution.outcome, Outcome::Running | Outcome::Unknown) {
            metrics.total = execution.started.elapsed();
        }
        CommandReport {
            outcome: execution.outcome,
            metrics,
        }
    }

    /// Dispatch at most once, then observe terminal evidence within the original
    /// deadline. An error is returned only before dispatch. Afterwards ambiguous
    /// transport/deadline failures produce `Unknown` and retain admission.
    /// Repeated calls return the existing report; use `reconcile` to observe again.
    pub async fn run(&mut self) -> Result<CommandReport, Error> {
        if self.dispatched {
            return Ok(self.report());
        }
        let execution = &mut self.prepared.execution;
        if Instant::now() >= execution.deadline {
            let error = Error::at(Phase::Data, Cause::Timeout);
            execution.stop(Outcome::Unknown, Some(error));
            return Err(error);
        }
        // Set uncertainty before the first await. A cancelled future must never
        // leave the command eligible for another dispatch or release admission.
        self.dispatched = true;
        execution.outcome = Outcome::Unknown;
        let result = timed(
            &mut execution.metrics.data_poll,
            complete_before(
                execution.deadline,
                Phase::Data,
                execution
                    .query
                    .upstream(&execution.client.upstream)
                    .execute(),
            ),
        )
        .await;
        execution.metrics.last_error = match result {
            Err(error) => Some(error),
            Ok(Err(error)) => Some(Error::from_upstream(Phase::Data, &error)),
            Ok(Ok(())) => None,
        };
        if execution
            .metrics
            .last_error
            .is_some_and(|error| error.is_pre_execution_refusal())
        {
            // Admission remains quarantined: the refusal is diagnostic evidence,
            // not proof that remote execution has stopped.
            return Ok(self.report());
        }
        self.observe(self.prepared.execution.deadline).await;
        Ok(self.report())
    }

    /// Observe the original query ID, marker and principal with a fresh bounded
    /// deadline, without replaying the command. This can establish command success
    /// after a lost response; it does not recover SELECT data or validate table contents.
    pub async fn reconcile(&mut self) -> Result<CommandReport, Error> {
        if !self.dispatched {
            return Err(Error::InvalidRequest);
        }
        if self.prepared.execution.outcome != Outcome::Unknown {
            return Ok(self.report());
        }
        let deadline = Instant::now()
            .checked_add(self.prepared.execution.client.config.timeout)
            .ok_or(Error::InvalidSetting)?;
        self.observe(deadline).await;
        Ok(self.report())
    }

    async fn observe(&mut self, deadline: Instant) {
        let execution = &mut self.prepared.execution;
        let (outcome, _) = execution
            .client
            .terminal_until(
                &execution.query.id,
                &execution.nonce,
                deadline,
                &mut execution.metrics,
                execution.principal.as_deref(),
                execution.log_since,
            )
            .await;
        execution.outcome = outcome;
        execution.metrics.total = execution.started.elapsed();
        if matches!(
            outcome,
            Outcome::Succeeded | Outcome::Failed | Outcome::Cancelled
        ) {
            execution.permit = None;
        }
    }
}

impl Drop for CommandExecution {
    fn drop(&mut self) {
        if self.dispatched
            && matches!(
                self.prepared.execution.outcome,
                Outcome::Running | Outcome::Unknown
            )
            && let Some(permit) = self.prepared.execution.permit.take()
        {
            // Forget only the semaphore credit, not the owned query or buffers.
            // The number of quarantined slots cannot exceed configured admission.
            permit.forget();
        }
    }
}

impl AntalyaClient {
    /// Prepare an authorized command using the existing admission, identity and
    /// terminal-proof machinery. `Format::Json` denotes the command authorization;
    /// execution uses the command transport and never appends a FORMAT clause.
    pub async fn prepare_authorized_command(
        &self,
        request: AuthorizedRequest,
    ) -> Result<PreparedCommand, Error> {
        if self.config.terminal_policy.strategy == crate::TerminalStrategy::InBand {
            return Err(Error::Unsupported);
        }
        if self.config.endpoint.0 != request.endpoint.0
            || request.format != Format::Json
            || (request.query.database.is_none() && self.config.database.is_some())
            || request.expected_principal().len() > 1024
            || request.endpoint.0.as_str().len() > 4096
        {
            return Err(Error::InvalidRequest);
        }
        let principal = request.expected_principal().to_owned();
        let prepared = self
            .prepare_execution(request.query, Some(principal))
            .await?;
        Ok(PreparedCommand { prepared })
    }
}
