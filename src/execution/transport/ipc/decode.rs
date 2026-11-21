//! Incremental validated Arrow decoding with explicit completion observation.
mod layout;
mod schema;
use super::{
    AdmittedIpc, IpcError, IpcFrame, IpcFrameKind, ReadyExecution, RecoveryReceipt,
    RemoteTermination, TransportError,
};
use crate::resources::{ByteLease, InternalOwner};
use crate::{BudgetedBatch, Outcome, Progress, TerminalObservation};
use arrow_array::ArrayRef;
use arrow_buffer::Buffer;
use arrow_schema::{Schema, SchemaRef};
use std::{collections::HashMap, fmt, sync::Arc};

pub enum DecodeError {
    Ipc(IpcError),
    Arrow(arrow_schema::ArrowError),
    Schema,
    Layout,
    Limit,
    Incomplete,
    Completion,
    Poisoned,
}
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ipc(_) => "Arrow input failed",
            Self::Arrow(_) => "Arrow value validation failed",
            Self::Schema => "unsupported or invalid Arrow schema",
            Self::Layout => "Arrow buffer layout differs from the schema",
            Self::Limit => "decoded Arrow state exceeds configured bounds",
            Self::Incomplete => "Arrow response is not completely decoded",
            Self::Completion => "original query completion does not prove this decoded result",
            Self::Poisoned => "Arrow consumption previously failed",
        })
    }
}
impl fmt::Debug for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Ipc(e) => Some(e),
            Self::Arrow(e) => Some(e),
            _ => None,
        }
    }
}
impl From<IpcError> for DecodeError {
    fn from(value: IpcError) -> Self {
        Self::Ipc(value)
    }
}
impl From<TransportError> for DecodeError {
    fn from(value: TransportError) -> Self {
        Self::Ipc(value.into())
    }
}
impl From<arrow_schema::ArrowError> for DecodeError {
    fn from(value: arrow_schema::ArrowError) -> Self {
        Self::Arrow(value)
    }
}

/// Provisional batches from one admitted response. Reading never dispatches
/// terminal observations. Keep BudgetedBatch owners while retaining their arrays.
#[must_use]
pub struct AdmittedArrow {
    pending: Option<IpcFrame>,
    schema: Option<SchemaRef>,
    schema_lease: Option<ByteLease>,
    schema_bytes: usize,
    dictionaries: HashMap<i64, ArrayRef>,
    dictionary_leases: HashMap<i64, ByteLease>,
    // Mark encoded backing still retained by a current dictionary. These guards
    // do not retain the backing: dead guards are pruned after replacement/delta.
    dictionary_frames: HashMap<i64, Vec<(usize, InternalOwner)>>,
    rows: u64,
    batches: u64,
    decoded_bytes: u64,
    ended: bool,
    poisoned: bool,
    outcome: Outcome,
    // Drop decoder state before dropping its remote admission owner.
    ipc: AdmittedIpc,
}
impl ReadyExecution {
    /// Admit under a Unix-second external admission deadline. The frozen bearer
    /// must carry a refusal bound from Config::jwt_receiver no later than that
    /// deadline. The trusted host separately admits the action and maintains the
    /// reviewed receiver/clock profile; decoded claims are not authorization.
    pub fn admit_arrow_before(self, before: u64) -> Result<AdmittedArrow, DecodeError> {
        self.check_receiver_deadline(before)?;
        self.admit_arrow()
    }
    /// Select incremental decoded batches after checking local frame/decoder
    /// capacity, then synchronously admit the one prepared request.
    pub fn admit_arrow(self) -> Result<AdmittedArrow, DecodeError> {
        let frame = super::frame_limit(&self.execution)?;
        let config = &self.execution.client.config;
        if frame
            .checked_add(config.execution_limits.max_batch_bytes)
            .and_then(|size| size.checked_add(super::super::MAX_HTTP_BUFFER))
            .is_none_or(|size| size > config.resources.max_retained_bytes)
        {
            return Err(DecodeError::Limit);
        }
        Ok(AdmittedArrow {
            pending: None,
            schema: None,
            schema_lease: None,
            schema_bytes: 0,
            dictionaries: HashMap::new(),
            dictionary_leases: HashMap::new(),
            dictionary_frames: HashMap::new(),
            rows: 0,
            batches: 0,
            decoded_bytes: 0,
            ended: false,
            poisoned: false,
            outcome: Outcome::Running,
            ipc: self.admit_ipc()?,
        })
    }
}
impl AdmittedArrow {
    pub fn recovery_receipt(&self) -> Result<RecoveryReceipt, crate::Error> {
        self.ipc.recovery_receipt()
    }
    pub fn rows_decoded(&self) -> u64 {
        self.rows
    }
    pub fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_deref()
    }
    /// Shared schema for an independently budgeted owner. Like extracting raw
    /// arrays, retaining this reference does not retain the decoder byte lease.
    pub fn shared_schema(&self) -> Option<SchemaRef> {
        self.schema.as_ref().map(Arc::clone)
    }
    /// Conservative schema charge, including for a successful empty result.
    pub fn schema_bytes(&self) -> usize {
        self.schema_bytes
    }
    pub fn outcome(&self) -> Outcome {
        self.outcome
    }

    pub async fn next_batch(&mut self) -> Result<Option<BudgetedBatch>, DecodeError> {
        if self.poisoned {
            return Err(DecodeError::Poisoned);
        }
        if self.ended {
            return Ok(None);
        }
        let result = self.decode_next().await;
        if result.is_err() {
            self.refuse();
        }
        result
    }
    fn refuse(&mut self) {
        self.poisoned = true;
        self.outcome = Outcome::Unknown;
        self.pending = None;
        self.schema = None;
        self.schema_lease = None;
        self.dictionaries.clear();
        self.dictionary_leases.clear();
        self.dictionary_frames.clear();
        self.ipc.discard();
    }
    async fn decode_next(&mut self) -> Result<Option<BudgetedBatch>, DecodeError> {
        tokio::runtime::Handle::try_current().map_err(TransportError::Runtime)?;
        let deadline = self.ipc.response.execution.deadline;
        super::check_deadline(deadline)?;
        let limits = self.ipc.response.execution.client.config.execution_limits;
        loop {
            if self.pending.is_none() {
                self.pending = self.ipc.next_internal_frame(self.schema.is_some()).await?;
            }
            let Some(frame) = self.pending.as_ref() else {
                if self.schema.is_none() || self.rows != self.ipc.declared_rows() {
                    return Err(DecodeError::Incomplete);
                }
                self.ended = true;
                return Ok(None);
            };
            // Dictionary deltas can transiently retain both incoming values and
            // a concatenation. Reserve both before calling the upstream decoder.
            let reserve = limits
                .max_batch_bytes
                .checked_mul(if frame.kind() == IpcFrameKind::DictionaryBatch {
                    2
                } else {
                    1
                })
                .ok_or(DecodeError::Limit)?;
            let mut lease = self
                .ipc
                .response
                .execution
                .client
                .resources
                .reserve_internal(reserve, true, deadline)
                .await
                .map_err(TransportError::Client)?;
            let mut frame = self.pending.take().ok_or(DecodeError::Poisoned)?;
            let frame_owner = frame.lease.take_internal_owner();
            let (prefix, metadata, kind) = (frame.prefix, frame.metadata, frame.kind());
            // The owned frame's byte lease stays alive through all shared Arrow
            // buffers. Upstream alignment copies are separately charged below.
            let buffer = Buffer::from(hyper::body::Bytes::from_owner(frame));
            let allocation = buffer.data_ptr().as_ptr() as usize;
            let message = arrow_ipc::root_as_message(&buffer[prefix..prefix + metadata])
                .map_err(IpcError::Metadata)?;
            if !matches!(
                message.version(),
                arrow_ipc::MetadataVersion::V4 | arrow_ipc::MetadataVersion::V5
            ) {
                return Err(DecodeError::Schema);
            }
            let body = buffer.slice(prefix + metadata);
            match kind {
                IpcFrameKind::Schema => {
                    let schema = schema::read(
                        message.header_as_schema().ok_or(DecodeError::Schema)?,
                        limits.max_batch_bytes,
                    )?;
                    let bytes = schema::size(&schema)?;
                    if bytes > limits.max_batch_bytes {
                        return Err(DecodeError::Limit);
                    }
                    shrink(&mut lease, bytes);
                    self.schema = Some(Arc::new(schema));
                    self.schema_bytes = bytes;
                    self.schema_lease = Some(lease);
                }
                IpcFrameKind::RecordBatch => {
                    let schema = self.schema.as_ref().ok_or(DecodeError::Schema)?;
                    let batch = message
                        .header_as_record_batch()
                        .ok_or(DecodeError::Layout)?;
                    layout::validate(schema, batch, &message.version(), limits.max_batch_bytes)?;
                    let decoded = arrow_ipc::reader::read_record_batch(
                        &body,
                        batch,
                        Arc::clone(schema),
                        &self.dictionaries,
                        None,
                        &message.version(),
                    )?;
                    let bytes = crate::memory::batch_bytes(&decoded, limits.max_batch_bytes)
                        .ok_or(DecodeError::Limit)?
                        .checked_add(self.schema_bytes)
                        .ok_or(DecodeError::Limit)?;
                    let rows = self
                        .rows
                        .checked_add(
                            u64::try_from(decoded.num_rows()).map_err(|_| DecodeError::Limit)?,
                        )
                        .ok_or(DecodeError::Limit)?;
                    let batches = self.batches.checked_add(1).ok_or(DecodeError::Limit)?;
                    let total = self
                        .decoded_bytes
                        .checked_add(u64::try_from(bytes).map_err(|_| DecodeError::Limit)?)
                        .ok_or(DecodeError::Limit)?;
                    if bytes > limits.max_batch_bytes
                        || rows > limits.max_rows
                        || batches > limits.max_batches
                        || total > limits.max_total_bytes
                    {
                        return Err(DecodeError::Limit);
                    }
                    super::check_deadline(deadline)?;
                    shrink(&mut lease, bytes);
                    self.rows = rows;
                    self.batches = batches;
                    self.decoded_bytes = total;
                    lease.make_external();
                    // Encoded frame becomes caller-owned along with the batch.
                    drop(frame_owner);
                    return Ok(Some(BudgetedBatch {
                        batch: decoded,
                        lease: Arc::new(lease),
                    }));
                }
                IpcFrameKind::DictionaryBatch => {
                    let schema = self.schema.as_ref().ok_or(DecodeError::Schema)?;
                    let dictionary = message
                        .header_as_dictionary_batch()
                        .ok_or(DecodeError::Layout)?;
                    let data = dictionary.data().ok_or(DecodeError::Layout)?;
                    let incoming =
                        usize::try_from(data.length()).map_err(|_| DecodeError::Layout)?;
                    let retained = if dictionary.isDelta() {
                        self.dictionaries
                            .get(&dictionary.id())
                            .map_or(0, |values| values.len())
                    } else {
                        0
                    };
                    layout::logical_length(
                        retained.checked_add(incoming).ok_or(DecodeError::Limit)?,
                        limits.max_batch_bytes,
                    )?;
                    let value_type = schema::dictionary_type(schema, dictionary.id())?;
                    layout::validate_values(
                        value_type,
                        data,
                        &message.version(),
                        limits.max_batch_bytes,
                    )?;
                    arrow_ipc::reader::read_dictionary(
                        &body,
                        dictionary,
                        schema,
                        &mut self.dictionaries,
                        &message.version(),
                    )?;
                    let mut total = 0_usize;
                    for values in self.dictionaries.values() {
                        total = total
                            .checked_add(
                                crate::memory::array_bytes(values.as_ref(), limits.max_batch_bytes)
                                    .ok_or(DecodeError::Limit)?,
                            )
                            .ok_or(DecodeError::Limit)?;
                    }
                    if total > limits.max_batch_bytes {
                        return Err(DecodeError::Limit);
                    }
                    let bytes = crate::memory::array_bytes(
                        self.dictionaries
                            .get(&dictionary.id())
                            .ok_or(DecodeError::Layout)?
                            .as_ref(),
                        limits.max_batch_bytes,
                    )
                    .ok_or(DecodeError::Limit)?;
                    shrink(&mut lease, bytes);
                    self.dictionary_leases.insert(dictionary.id(), lease);
                    let retained = crate::memory::array_allocations(
                        self.dictionaries
                            .get(&dictionary.id())
                            .ok_or(DecodeError::Layout)?
                            .as_ref(),
                        limits.max_batch_bytes,
                    )
                    .ok_or(DecodeError::Limit)?;
                    let frames = self.dictionary_frames.entry(dictionary.id()).or_default();
                    // A delta may copy old values or keep view buffers. Only
                    // current dictionary reachability establishes internal ownership;
                    // old caller batches can keep other frame allocations alive.
                    frames.retain(|(base, owner)| owner.is_live() && retained.contains_key(base));
                    if retained.contains_key(&allocation)
                        && let Some(owner) = frame_owner
                    {
                        frames.push((allocation, owner));
                    }
                }
            }
            super::check_deadline(deadline)?;
        }
    }

    /// Explicit control I/O under the original operation deadline. Only a clean
    /// decoded stream plus matching original query-log result_rows can succeed.
    /// This operation must be admitted separately by a governed host.
    pub async fn confirm_completion(&mut self) -> Result<(), DecodeError> {
        if self.poisoned {
            return Err(DecodeError::Poisoned);
        }
        if !self.ended {
            return Err(DecodeError::Incomplete);
        }
        if self.outcome == Outcome::Succeeded {
            return Ok(());
        }
        tokio::runtime::Handle::try_current().map_err(TransportError::Runtime)?;
        if let Err(error) = super::check_deadline(self.ipc.response.execution.deadline) {
            self.refuse();
            return Err(error.into());
        }
        let execution = &mut self.ipc.response.execution;
        let (outcome, progress) = execution
            .client
            .terminal_until(
                &execution.query.id,
                &execution.nonce,
                execution.deadline,
                &mut execution.metrics,
                execution.principal.as_deref(),
            )
            .await;
        execution.progress = progress;
        let control_error = execution.metrics.last_error;
        if matches!(
            outcome,
            Outcome::Succeeded | Outcome::Failed | Outcome::Cancelled
        ) {
            self.ipc.response.termination_proven = true;
            self.ipc.response.permit = None;
        }
        self.finish_completion(outcome, progress, control_error)
    }
    /// Validate a separately obtained terminal observation locally. The exact
    /// receipt, decoded EOF, row count and original deadline must all agree.
    /// A foreign observation does not change or poison this response.
    pub fn accept_completion(&mut self, observed: &TerminalObservation) -> Result<(), DecodeError> {
        if !observed.matches(&self.ipc.response.execution) {
            return Err(TransportError::ObservationMismatch.into());
        }
        if self.poisoned {
            return Err(DecodeError::Poisoned);
        }
        if !self.ended {
            return Err(DecodeError::Incomplete);
        }
        if self.outcome == Outcome::Succeeded {
            return Ok(());
        }
        let termination = self.ipc.response.accept_termination(observed)?;
        let outcome = match termination {
            RemoteTermination::Succeeded => Outcome::Succeeded,
            RemoteTermination::Failed => Outcome::Failed,
            RemoteTermination::Cancelled => Outcome::Cancelled,
        };
        self.finish_completion(outcome, observed.progress(), None)
    }
    fn finish_completion(
        &mut self,
        outcome: Outcome,
        progress: Option<Progress>,
        control_error: Option<crate::Error>,
    ) -> Result<(), DecodeError> {
        let timely = tokio::time::Instant::now() < self.ipc.response.execution.deadline;
        if timely
            && outcome == Outcome::Succeeded
            && progress.and_then(|value| value.result_rows) == Some(self.rows)
        {
            self.outcome = Outcome::Succeeded;
            Ok(())
        } else {
            self.refuse();
            if matches!(outcome, Outcome::Failed | Outcome::Cancelled) {
                self.outcome = outcome;
            }
            if matches!(outcome, Outcome::Unknown | Outcome::Running)
                && let Some(error) = control_error
            {
                return Err(TransportError::Client(error).into());
            }
            if !timely {
                return Err(TransportError::Deadline.into());
            }
            Err(DecodeError::Completion)
        }
    }
    /// Accept termination proof without network I/O or upgrading provisional,
    /// discarded or incomplete output to success. Safe after decode refusal.
    pub fn accept_termination(
        &mut self,
        observed: &TerminalObservation,
    ) -> Result<RemoteTermination, DecodeError> {
        Ok(self.ipc.response.accept_termination(observed)?)
    }
    /// Recovery observation remains possible after decode refusal. It cannot
    /// promote discarded/partial data to a successful result.
    pub async fn confirm_termination(&mut self) -> Result<RemoteTermination, DecodeError> {
        Ok(self.ipc.confirm_termination().await?)
    }
    /// Explicit control I/O: discard delivery before requesting cancellation of
    /// the original query. Dropping this future leaves output poisoned and the
    /// active slot quarantined until a subsequent termination proof. This is not
    /// response waiting and needs independent host authorization/admission.
    pub async fn cancel(&mut self) -> Result<Outcome, DecodeError> {
        self.refuse();
        tokio::runtime::Handle::try_current().map_err(TransportError::Runtime)?;
        let outcome = self
            .ipc
            .response
            .execution
            .cancel()
            .await
            .map_err(TransportError::Client)?;
        if matches!(outcome, Outcome::Cancelled | Outcome::Failed) {
            self.ipc.response.termination_proven = true;
            self.ipc.response.permit = None;
            self.outcome = outcome;
        }
        Ok(self.outcome)
    }
}
fn shrink(lease: &mut ByteLease, bytes: usize) {
    drop(lease.split(lease.num_permits().saturating_sub(bytes)));
}

/// Metadata preflight for the high-level streaming decoder. It reuses the
/// admitted path's schema/layout checks without changing admission semantics.
pub(crate) struct MeasuredMetadataGuard {
    schema: Option<Schema>,
    dictionaries: HashMap<i64, (usize, usize)>,
    limit: usize,
}
impl MeasuredMetadataGuard {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            schema: None,
            dictionaries: HashMap::new(),
            limit,
        }
    }
    pub(crate) fn validate(
        &mut self,
        message: arrow_ipc::Message<'_>,
        body: usize,
    ) -> Result<(), crate::Error> {
        self.check(message, body).map_err(|error| {
            crate::Error::at(
                crate::Phase::Data,
                if matches!(
                    error,
                    DecodeError::Limit | DecodeError::Ipc(IpcError::Limit)
                ) {
                    crate::Cause::LimitExceeded
                } else {
                    crate::Cause::Decoder
                },
            )
        })
    }
    fn check(&mut self, message: arrow_ipc::Message<'_>, body: usize) -> Result<(), DecodeError> {
        if !matches!(
            message.version(),
            arrow_ipc::MetadataVersion::V4 | arrow_ipc::MetadataVersion::V5
        ) {
            return Err(DecodeError::Schema);
        }
        match message.header_type() {
            arrow_ipc::MessageHeader::Schema if self.schema.is_none() && body == 0 => {
                self.schema = Some(schema::read(
                    message.header_as_schema().ok_or(DecodeError::Schema)?,
                    self.limit.max(64 * 1024),
                )?);
            }
            arrow_ipc::MessageHeader::RecordBatch => {
                let schema = self.schema.as_ref().ok_or(DecodeError::Schema)?;
                let batch = message
                    .header_as_record_batch()
                    .ok_or(DecodeError::Layout)?;
                super::validate_batch(batch, body)?;
                layout::validate(schema, batch, &message.version(), self.limit)?;
            }
            arrow_ipc::MessageHeader::DictionaryBatch => {
                let schema = self.schema.as_ref().ok_or(DecodeError::Schema)?;
                let dictionary = message
                    .header_as_dictionary_batch()
                    .ok_or(DecodeError::Layout)?;
                let batch = dictionary.data().ok_or(DecodeError::Layout)?;
                super::validate_batch(batch, body)?;
                layout::validate_values(
                    schema::dictionary_type(schema, dictionary.id())?,
                    batch,
                    &message.version(),
                    self.limit,
                )?;
                let incoming = usize::try_from(batch.length()).map_err(|_| DecodeError::Layout)?;
                let old = if dictionary.isDelta() {
                    *self
                        .dictionaries
                        .get(&dictionary.id())
                        .ok_or(DecodeError::Layout)?
                } else {
                    (0, 0)
                };
                let rows = old.0.checked_add(incoming).ok_or(DecodeError::Limit)?;
                layout::logical_length(rows, self.limit)?;
                // Account repeated/overlapping buffers by their declared lengths,
                // and reserve descriptors/null bitmaps even for bufferless arrays.
                let mut charge = old.1;
                for buffer in batch.buffers().ok_or(DecodeError::Layout)? {
                    charge = charge
                        .checked_add(
                            usize::try_from(buffer.length()).map_err(|_| DecodeError::Layout)?,
                        )
                        .ok_or(DecodeError::Limit)?;
                }
                for node in batch.nodes().ok_or(DecodeError::Layout)? {
                    let rows = usize::try_from(node.length()).map_err(|_| DecodeError::Layout)?;
                    charge = charge
                        .checked_add(rows.div_ceil(8))
                        .and_then(|v| v.checked_add(256))
                        .ok_or(DecodeError::Limit)?;
                }
                let mut total = charge;
                for (id, (_, bytes)) in &self.dictionaries {
                    if *id != dictionary.id() {
                        total = total.checked_add(*bytes).ok_or(DecodeError::Limit)?;
                    }
                }
                if total > self.limit {
                    return Err(DecodeError::Limit);
                }
                self.dictionaries.insert(dictionary.id(), (rows, charge));
            }
            _ => return Err(DecodeError::Layout),
        }
        Ok(())
    }
}
