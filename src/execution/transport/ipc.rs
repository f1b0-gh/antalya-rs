//! Bounded IPC framing before Arrow decoding. Metadata is structurally checked;
//! returned frames and their row declarations are still unverified query data.
mod decode;
use super::{
    AdmittedResponse, ReadyExecution, RecoveryReceipt, RemoteTermination, ResponseChunk,
    TransportError, check_deadline,
};
use crate::resources::ByteLease;
use arrow_ipc::MessageHeader;
pub use decode::{AdmittedArrow, DecodeError};
use std::{collections::TryReserveError, fmt};

const MAX_METADATA: usize = 64 * 1024;

pub enum IpcError {
    Transport(TransportError),
    Metadata(flatbuffers::InvalidFlatbuffer),
    Allocation(TryReserveError),
    InvalidFrame,
    Limit,
    Compression,
    Poisoned,
}
impl fmt::Display for IpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Transport(_) => "IPC transport failed",
            Self::Metadata(_) => "IPC metadata validation failed",
            Self::Allocation(_) => "IPC frame allocation failed",
            Self::InvalidFrame => "invalid or incomplete IPC frame sequence",
            Self::Limit => "IPC frame or declared result exceeds configured bounds",
            Self::Compression => "IPC compression differs from the requested uncompressed profile",
            Self::Poisoned => "IPC framing previously failed",
        })
    }
}
impl fmt::Debug for IpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for IpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(e) => Some(e),
            Self::Metadata(e) => Some(e),
            Self::Allocation(e) => Some(e),
            _ => None,
        }
    }
}
impl From<TransportError> for IpcError {
    fn from(value: TransportError) -> Self {
        Self::Transport(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpcFrameKind {
    Schema,
    RecordBatch,
    DictionaryBatch,
}

/// Structurally checked, uncompressed IPC message. Keep this owner while retaining
/// its bytes. No Arrow array decoding, schema semantics or terminal proof is implied.
pub struct IpcFrame {
    bytes: Vec<u8>,
    lease: ByteLease,
    prefix: usize,
    metadata: usize,
    kind: IpcFrameKind,
}
impl AsRef<[u8]> for IpcFrame {
    fn as_ref(&self) -> &[u8] {
        self.encoded()
    }
}
impl IpcFrame {
    pub fn kind(&self) -> IpcFrameKind {
        self.kind
    }
    pub fn encoded(&self) -> &[u8] {
        &self.bytes
    }
    pub fn metadata(&self) -> &[u8] {
        &self.bytes[self.prefix..self.prefix + self.metadata]
    }
    pub fn body(&self) -> &[u8] {
        &self.bytes[self.prefix + self.metadata..]
    }
    pub fn charged_bytes(&self) -> usize {
        self.lease.num_permits()
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Prefix(usize),
    Metadata {
        prefix: usize,
        length: usize,
    },
    Body {
        prefix: usize,
        metadata: usize,
        length: usize,
        kind: IpcFrameKind,
        rows: u64,
    },
    End,
}

/// Response-only frame reader. Borrowed polls are cancellation safe. EOF means
/// framing ended cleanly, not that Arrow values or query completion are valid.
#[must_use]
pub struct AdmittedIpc {
    chunk: Option<ResponseChunk>,
    offset: usize,
    bytes: Vec<u8>,
    lease: Option<ByteLease>,
    frame_limit: usize,
    phase: Phase,
    schema: bool,
    rows: u64,
    batches: u64,
    ended: bool,
    poisoned: bool,
    // Drop buffered frames and wire leases before releasing remote admission.
    response: AdmittedResponse,
}
impl AdmittedResponse {
    /// Select bounded IPC framing before consuming any raw bytes. Requires room
    /// for one maximum frame plus a wire chunk in the shared payload budget.
    /// A refusal drops this already-admitted owner and quarantines unresolved work.
    pub fn into_ipc(mut self) -> Result<AdmittedIpc, IpcError> {
        let frame_limit = frame_limit(&self.execution)?;
        if self.received != 0 || self.response.is_none() || self.poisoned {
            return Err(IpcError::Limit);
        }
        // A cancelled pre-header raw poll may hold a wire allowance. IPC must
        // acquire its frame before its wire claim, including after conversion.
        self.reservation = None;
        Ok(AdmittedIpc {
            response: self,
            chunk: None,
            offset: 0,
            bytes: Vec::new(),
            lease: None,
            frame_limit,
            phase: Phase::Prefix(4),
            schema: false,
            rows: 0,
            batches: 0,
            ended: false,
            poisoned: false,
        })
    }
}
impl ReadyExecution {
    /// Validate frame capacity before admitting the query, then select IPC
    /// framing. No response is polled and no terminal observation is performed.
    pub fn admit_ipc(self) -> Result<AdmittedIpc, IpcError> {
        frame_limit(&self.execution)?;
        self.admit()?.into_ipc()
    }
}
fn frame_limit(execution: &super::Execution) -> Result<usize, IpcError> {
    let limit = execution
        .client
        .config
        .execution_limits
        .max_batch_bytes
        .checked_add(MAX_METADATA + 8)
        .ok_or(IpcError::Limit)?;
    let wire = super::MAX_HTTP_BUFFER.min(execution.client.resources.retained_capacity());
    if limit
        .checked_add(wire)
        .is_none_or(|size| size > execution.client.resources.retained_capacity())
    {
        return Err(IpcError::Limit);
    }
    Ok(limit)
}
impl AdmittedIpc {
    pub fn recovery_receipt(&self) -> Result<RecoveryReceipt, crate::Error> {
        self.response.recovery_receipt()
    }
    /// Untrusted row declarations, not decoded rows or completion evidence.
    pub fn declared_rows(&self) -> u64 {
        self.rows
    }
    pub fn wire_bytes(&self) -> u64 {
        self.response.wire_bytes()
    }

    pub async fn next_frame(&mut self) -> Result<Option<IpcFrame>, IpcError> {
        let mut frame = self.next_internal_frame(false).await?;
        if let Some(frame) = &mut frame {
            frame.lease.make_external();
        }
        Ok(frame)
    }
    async fn next_internal_frame(&mut self, retained: bool) -> Result<Option<IpcFrame>, IpcError> {
        if self.poisoned {
            return Err(IpcError::Poisoned);
        }
        if self.ended {
            return Ok(None);
        }
        let result = self.receive_frame(retained).await;
        if result.is_err() {
            self.discard();
        }
        result
    }

    fn discard(&mut self) {
        self.poisoned = true;
        self.bytes = Vec::new();
        self.chunk = None;
        self.lease = None;
        self.response.discard();
    }

    async fn receive_frame(&mut self, retained: bool) -> Result<Option<IpcFrame>, IpcError> {
        tokio::runtime::Handle::try_current().map_err(TransportError::Runtime)?;
        check_deadline(self.response.execution.deadline)?;
        if self.lease.is_none() {
            self.lease = Some(
                self.response
                    .execution
                    .client
                    .resources
                    .reserve_internal(
                        self.frame_limit,
                        retained || self.chunk.is_some(),
                        self.response.execution.deadline,
                    )
                    .await
                    .map_err(TransportError::Client)?,
            );
        }
        loop {
            match self.phase {
                Phase::Prefix(target) => {
                    if !self.fill(target).await? {
                        if self.bytes.is_empty() && self.schema {
                            return self.finish();
                        }
                        return Err(IpcError::InvalidFrame);
                    }
                    let word: [u8; 4] = self.bytes[target - 4..target]
                        .try_into()
                        .map_err(|_| IpcError::InvalidFrame)?;
                    if target == 4 && word == [255; 4] {
                        self.phase = Phase::Prefix(8);
                        continue;
                    }
                    let length =
                        usize::try_from(u32::from_le_bytes(word)).map_err(|_| IpcError::Limit)?;
                    if length == 0 {
                        if !self.schema {
                            return Err(IpcError::InvalidFrame);
                        }
                        self.bytes.clear();
                        self.phase = Phase::End;
                    } else if length > MAX_METADATA {
                        return Err(IpcError::Limit);
                    } else {
                        self.phase = Phase::Metadata {
                            prefix: target,
                            length,
                        };
                    }
                }
                Phase::Metadata { prefix, length } => {
                    if !self.fill(prefix + length).await? {
                        return Err(IpcError::InvalidFrame);
                    }
                    let (kind, body, rows) = self.validate_metadata(prefix, length)?;
                    self.phase = Phase::Body {
                        prefix,
                        metadata: length,
                        length: body,
                        kind,
                        rows,
                    };
                }
                Phase::Body {
                    prefix,
                    metadata,
                    length,
                    kind,
                    rows,
                } => {
                    if !self.fill(prefix + metadata + length).await? {
                        return Err(IpcError::InvalidFrame);
                    }
                    check_deadline(self.response.execution.deadline)?;
                    let mut lease = self.lease.take().ok_or(IpcError::Poisoned)?;
                    drop(lease.split(lease.num_permits().saturating_sub(self.bytes.capacity())));
                    match kind {
                        IpcFrameKind::Schema => self.schema = true,
                        IpcFrameKind::RecordBatch => {
                            self.rows += rows;
                            self.batches += 1;
                        }
                        IpcFrameKind::DictionaryBatch => {}
                    }
                    self.phase = Phase::Prefix(4);
                    return Ok(Some(IpcFrame {
                        bytes: std::mem::take(&mut self.bytes),
                        lease,
                        prefix,
                        metadata,
                        kind,
                    }));
                }
                Phase::End => {
                    if self.fill(1).await? {
                        return Err(IpcError::InvalidFrame);
                    }
                    return self.finish();
                }
            }
        }
    }

    fn finish(&mut self) -> Result<Option<IpcFrame>, IpcError> {
        self.ended = true;
        self.bytes = Vec::new();
        self.lease = None;
        self.chunk = None;
        Ok(None)
    }

    async fn fill(&mut self, target: usize) -> Result<bool, IpcError> {
        if target > self.frame_limit {
            return Err(IpcError::Limit);
        }
        if target > self.bytes.len() {
            self.bytes
                .try_reserve_exact(target - self.bytes.len())
                .map_err(IpcError::Allocation)?;
            if self.bytes.capacity() > self.frame_limit {
                return Err(IpcError::Limit);
            }
        }
        while self.bytes.len() < target {
            if self.chunk.is_none() {
                self.chunk = self.response.next_internal_chunk().await?;
                self.offset = 0;
                if self.chunk.is_none() {
                    return Ok(false);
                }
            }
            let chunk = self.chunk.as_ref().ok_or(IpcError::Poisoned)?;
            let count = (target - self.bytes.len()).min(chunk.len() - self.offset);
            self.bytes
                .extend_from_slice(&chunk[self.offset..self.offset + count]);
            self.offset += count;
            if self.offset == chunk.len() {
                self.chunk = None;
            }
        }
        Ok(true)
    }

    fn validate_metadata(
        &self,
        prefix: usize,
        length: usize,
    ) -> Result<(IpcFrameKind, usize, u64), IpcError> {
        let options = flatbuffers::VerifierOptions {
            max_depth: 32,
            max_tables: 4096,
            max_apparent_size: 4 * MAX_METADATA,
            ..Default::default()
        };
        let message =
            arrow_ipc::root_as_message_with_opts(&options, &self.bytes[prefix..prefix + length])
                .map_err(IpcError::Metadata)?;
        let body = usize::try_from(message.bodyLength()).map_err(|_| IpcError::InvalidFrame)?;
        let limits = self.response.execution.client.config.execution_limits;
        if body > limits.max_batch_bytes {
            return Err(IpcError::Limit);
        }
        match message.header_type() {
            MessageHeader::Schema if !self.schema && body == 0 => {
                message
                    .header_as_schema()
                    .filter(|schema| schema.fields().is_some())
                    .ok_or(IpcError::InvalidFrame)?;
                Ok((IpcFrameKind::Schema, 0, 0))
            }
            MessageHeader::RecordBatch if self.schema => {
                let batch = message
                    .header_as_record_batch()
                    .ok_or(IpcError::InvalidFrame)?;
                let rows = validate_batch(batch, body)?;
                if self
                    .rows
                    .checked_add(rows)
                    .is_none_or(|value| value > limits.max_rows)
                    || self
                        .batches
                        .checked_add(1)
                        .is_none_or(|value| value > limits.max_batches)
                {
                    return Err(IpcError::Limit);
                }
                Ok((IpcFrameKind::RecordBatch, body, rows))
            }
            MessageHeader::DictionaryBatch if self.schema => {
                let dictionary = message
                    .header_as_dictionary_batch()
                    .ok_or(IpcError::InvalidFrame)?;
                validate_batch(dictionary.data().ok_or(IpcError::InvalidFrame)?, body)?;
                Ok((IpcFrameKind::DictionaryBatch, body, 0))
            }
            _ => Err(IpcError::InvalidFrame),
        }
    }

    /// Explicit network operation, never invoked by next_frame. Proves only
    /// remote termination; callers must still decode and verify complete results.
    pub async fn confirm_termination(&mut self) -> Result<RemoteTermination, IpcError> {
        Ok(self.response.confirm_termination().await?)
    }
}

fn validate_batch(batch: arrow_ipc::RecordBatch<'_>, body: usize) -> Result<u64, IpcError> {
    if batch.compression().is_some() {
        return Err(IpcError::Compression);
    }
    let rows = u64::try_from(batch.length()).map_err(|_| IpcError::InvalidFrame)?;
    for buffer in batch.buffers().ok_or(IpcError::InvalidFrame)? {
        let offset = usize::try_from(buffer.offset()).map_err(|_| IpcError::InvalidFrame)?;
        let length = usize::try_from(buffer.length()).map_err(|_| IpcError::InvalidFrame)?;
        if offset.checked_add(length).is_none_or(|end| end > body) {
            return Err(IpcError::InvalidFrame);
        }
    }
    for node in batch.nodes().ok_or(IpcError::InvalidFrame)? {
        if node.length() < 0 || node.null_count() < 0 || node.null_count() > node.length() {
            return Err(IpcError::InvalidFrame);
        }
    }
    if batch
        .variadicBufferCounts()
        .is_some_and(|counts| counts.iter().any(|count| count < 0))
    {
        return Err(IpcError::InvalidFrame);
    }
    Ok(rows)
}

pub(crate) use decode::MeasuredMetadataGuard;
