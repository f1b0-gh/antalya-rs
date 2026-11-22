// SPDX-License-Identifier: Apache-2.0
//! Opt-in Zstd preflight. Never send compressed buffers to Arrow until every
//! declared expansion and the schema/layout/dictionary budget has been checked.
//! Arrow's Zstd codec uses bounded single-shot decompression (unlike its LZ4 path).
use crate::{Cause, Error, Phase};
use arrow_buffer::Buffer;
use arrow_ipc::{Message, MessageHeader};

const METADATA_CAP: usize = 64 * 1024;

enum Stage {
    Prefix,
    Metadata { prefix: usize, length: usize },
    Body { prefix: usize, metadata: usize },
}
pub(super) struct Guard {
    bytes: Vec<u8>,
    target: usize,
    stage: Stage,
    ended: bool,
    failed: bool,
    limit: usize,
    metadata: crate::execution::transport::MeasuredMetadataGuard,
}
impl Guard {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            target: 4,
            stage: Stage::Prefix,
            ended: false,
            failed: false,
            limit,
            metadata: crate::execution::transport::MeasuredMetadataGuard::new(limit),
        }
    }
    pub(super) fn finish(&self) -> Result<(), Error> {
        if self.ended && !self.failed && self.bytes.is_empty() {
            Ok(())
        } else {
            Err(bad())
        }
    }
    pub(super) fn next(&mut self, input: &mut Buffer) -> Result<Option<Buffer>, Error> {
        if self.failed {
            return Err(bad());
        }
        let result = self.next_inner(input);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn next_inner(&mut self, input: &mut Buffer) -> Result<Option<Buffer>, Error> {
        loop {
            if self.ended {
                return if input.is_empty() {
                    Ok(None)
                } else {
                    Err(bad())
                };
            }
            if self.bytes.len() < self.target {
                if input.is_empty() {
                    return Ok(None);
                }
                self.bytes
                    .try_reserve_exact(self.target - self.bytes.len())
                    .map_err(|_| limit())?;
                let n = (self.target - self.bytes.len()).min(input.len());
                self.bytes.extend_from_slice(&input[..n]);
                input.advance(n);
                if self.bytes.len() < self.target {
                    return Ok(None);
                }
            }
            match self.stage {
                Stage::Prefix => {
                    let word = u32::from_le_bytes(
                        self.bytes[self.target - 4..self.target]
                            .try_into()
                            .map_err(|_| bad())?,
                    );
                    if word == u32::MAX && self.target == 4 {
                        self.target = 8;
                        continue;
                    }
                    if word == 0 {
                        self.ended = true;
                        return Ok(Some(std::mem::take(&mut self.bytes).into()));
                    }
                    let length = word as usize;
                    if length > METADATA_CAP {
                        return Err(limit());
                    }
                    let prefix = self.target;
                    self.target = prefix + length;
                    self.stage = Stage::Metadata { prefix, length };
                }
                Stage::Metadata { prefix, length } => {
                    let msg = message(&self.bytes[prefix..])?;
                    let body = usize::try_from(msg.bodyLength()).map_err(|_| bad())?;
                    if body > self.limit {
                        return Err(limit());
                    }
                    self.target = self.target.checked_add(body).ok_or_else(limit)?;
                    self.stage = Stage::Body {
                        prefix,
                        metadata: length,
                    };
                }
                Stage::Body { prefix, metadata } => {
                    let msg = message(&self.bytes[prefix..prefix + metadata])?;
                    let body = &self.bytes[prefix + metadata..];
                    validate(msg, body, self.limit, &mut self.metadata)?;
                    self.target = 4;
                    self.stage = Stage::Prefix;
                    return Ok(Some(std::mem::take(&mut self.bytes).into()));
                }
            }
        }
    }
}
fn message(bytes: &[u8]) -> Result<Message<'_>, Error> {
    let options = flatbuffers::VerifierOptions {
        max_depth: 32,
        max_tables: 4096,
        max_apparent_size: 4 * METADATA_CAP,
        ..Default::default()
    };
    arrow_ipc::root_as_message_with_opts(&options, bytes).map_err(|_| bad())
}
fn validate(
    msg: Message<'_>,
    body: &[u8],
    cap: usize,
    guard: &mut crate::execution::transport::MeasuredMetadataGuard,
) -> Result<(), Error> {
    let dictionary = msg.header_as_dictionary_batch();
    let batch = match msg.header_type() {
        MessageHeader::RecordBatch => msg.header_as_record_batch().ok_or_else(bad)?,
        MessageHeader::DictionaryBatch => dictionary.and_then(|d| d.data()).ok_or_else(bad)?,
        _ => return guard.validate(msg, body.len()),
    };
    let Some(compression) = batch.compression() else {
        return guard.validate(msg, body.len());
    };
    if msg.version() != arrow_ipc::MetadataVersion::V5
        || compression.codec() != arrow_ipc::CompressionType::ZSTD
        || compression.method() != arrow_ipc::BodyCompressionMethod::BUFFER
    {
        return Err(bad());
    }
    let mut expanded = 0usize;
    let mut end = 0usize;
    let mut buffers = Vec::new();
    for buffer in batch.buffers().ok_or_else(bad)? {
        let start = usize::try_from(buffer.offset()).map_err(|_| bad())?;
        let len = usize::try_from(buffer.length()).map_err(|_| bad())?;
        let stop = start.checked_add(len).ok_or_else(bad)?;
        if stop > body.len() || (len > 0 && start < end) {
            return Err(bad());
        }
        if len > 0 {
            end = stop;
        }
        let size = if len == 0 {
            0
        } else {
            let prefix = body
                .get(start..start.checked_add(8).ok_or_else(bad)?)
                .filter(|_| len >= 8)
                .ok_or_else(bad)?;
            match i64::from_le_bytes(prefix.try_into().map_err(|_| bad())?) {
                -1 => len - 8,
                n => usize::try_from(n).map_err(|_| bad())?,
            }
        };
        let offset = expanded;
        expanded = expanded.checked_add(size).ok_or_else(limit)?;
        if expanded > cap {
            return Err(limit());
        }
        buffers.push(arrow_ipc::Buffer::new(offset as i64, size as i64));
    }
    // Validate the *expanded* layout, including dictionary history, using the
    // exact same guard as uncompressed IPC. Only descriptors are rebuilt;
    // Arrow receives the original frame and checks the real expansion size.
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let nodes: Vec<_> = batch.nodes().ok_or_else(bad)?.iter().copied().collect();
    let nodes = builder.create_vector(&nodes);
    let buffers = builder.create_vector(&buffers);
    let variadic = batch.variadicBufferCounts().map(|counts| {
        let counts: Vec<_> = counts.iter().collect();
        builder.create_vector(&counts)
    });
    let normalized = arrow_ipc::RecordBatch::create(
        &mut builder,
        &arrow_ipc::RecordBatchArgs {
            length: batch.length(),
            nodes: Some(nodes),
            buffers: Some(buffers),
            variadicBufferCounts: variadic,
            ..Default::default()
        },
    );
    let header = if let Some(d) = dictionary {
        arrow_ipc::DictionaryBatch::create(
            &mut builder,
            &arrow_ipc::DictionaryBatchArgs {
                id: d.id(),
                isDelta: d.isDelta(),
                data: Some(normalized),
            },
        )
        .as_union_value()
    } else {
        normalized.as_union_value()
    };
    let message = arrow_ipc::Message::create(
        &mut builder,
        &arrow_ipc::MessageArgs {
            version: msg.version(),
            header_type: msg.header_type(),
            header: Some(header),
            bodyLength: expanded as i64,
            ..Default::default()
        },
    );
    builder.finish(message, None);
    guard.validate(
        arrow_ipc::root_as_message(builder.finished_data()).map_err(|_| bad())?,
        expanded,
    )
}
fn bad() -> Error {
    Error::at(Phase::Data, Cause::Decoder)
}
fn limit() -> Error {
    Error::at(Phase::Data, Cause::LimitExceeded)
}
