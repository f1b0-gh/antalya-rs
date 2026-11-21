// SPDX-License-Identifier: Apache-2.0
//! Bound IPC envelope accumulation before handing chunks to Arrow's decoder.
use crate::{Cause, Error, Phase};
const METADATA_CAP: usize = 64 * 1024;
enum Stage {
    Word,
    Metadata(usize),
    Body(usize),
    End,
}
pub(super) struct Guard {
    stage: Stage,
    word: Vec<u8>,
    metadata: Vec<u8>,
    continuation: bool,
    limit: usize,
    metadata_guard: crate::execution::transport::MeasuredMetadataGuard,
}
impl Guard {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            stage: Stage::Word,
            word: Vec::with_capacity(4),
            metadata: Vec::new(),
            continuation: false,
            limit,
            metadata_guard: crate::execution::transport::MeasuredMetadataGuard::new(limit),
        }
    }
    pub(super) fn ended(&self) -> bool {
        matches!(self.stage, Stage::End)
    }
    pub(super) fn feed(&mut self, mut input: &[u8]) -> Result<(), Error> {
        while !input.is_empty() {
            match self.stage {
                Stage::Word => {
                    let n = (4 - self.word.len()).min(input.len());
                    self.word.extend_from_slice(&input[..n]);
                    input = &input[n..];
                    if self.word.len() != 4 {
                        continue;
                    }
                    let word =
                        u32::from_le_bytes(self.word.as_slice().try_into().map_err(|_| bad())?);
                    self.word.clear();
                    if word == u32::MAX && !self.continuation {
                        self.continuation = true;
                        continue;
                    }
                    self.continuation = false;
                    if word == 0 {
                        self.stage = Stage::End;
                        continue;
                    }
                    let length = word as usize;
                    if length > METADATA_CAP {
                        return Err(bad());
                    }
                    self.stage = Stage::Metadata(length);
                }
                Stage::Metadata(length) => {
                    let n = (length - self.metadata.len()).min(input.len());
                    self.metadata.extend_from_slice(&input[..n]);
                    input = &input[n..];
                    if self.metadata.len() != length {
                        continue;
                    }
                    let options = flatbuffers::VerifierOptions {
                        max_depth: 32,
                        max_tables: 4096,
                        max_apparent_size: 4 * METADATA_CAP,
                        ..Default::default()
                    };
                    let msg = arrow_ipc::root_as_message_with_opts(&options, &self.metadata)
                        .map_err(|_| bad())?;
                    let body = usize::try_from(msg.bodyLength()).map_err(|_| bad())?;
                    if body > self.limit {
                        return Err(limit());
                    }
                    self.metadata_guard.validate(msg, body)?;
                    self.metadata.clear();
                    self.stage = if body == 0 {
                        Stage::Word
                    } else {
                        Stage::Body(body)
                    };
                }
                Stage::Body(left) => {
                    let n = left.min(input.len());
                    input = &input[n..];
                    self.stage = if n == left {
                        Stage::Word
                    } else {
                        Stage::Body(left - n)
                    };
                }
                Stage::End => return Err(bad()),
            }
        }
        Ok(())
    }
}
fn bad() -> Error {
    Error::at(Phase::Data, Cause::Decoder)
}
fn limit() -> Error {
    Error::at(Phase::Data, Cause::LimitExceeded)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_record_header_is_refused_before_arrow_conversion() {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let mut message = arrow_ipc::MessageBuilder::new(&mut builder);
        message.add_version(arrow_ipc::MetadataVersion::V5);
        message.add_header_type(arrow_ipc::MessageHeader::RecordBatch);
        let message = message.finish();
        builder.finish(message, None);
        let metadata = builder.finished_data();
        let mut payload = (metadata.len() as u32).to_le_bytes().to_vec();
        payload.extend_from_slice(metadata);
        assert!(Guard::new(1024).feed(&payload).is_err());
    }
}

#[cfg(test)]
mod compression_test {
    #[test]
    fn regression_compression_is_refused_before_decode() {
        let batch = arrow_array::RecordBatch::try_from_iter(vec![(
            "v",
            std::sync::Arc::new(arrow_array::Int32Array::from(vec![1, 2])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let mut bytes = Vec::new();
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            &mut bytes,
            &batch.schema(),
            options,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        assert!(super::Guard::new(65536).feed(&bytes).is_err());
    }
}

#[cfg(test)]
mod hostile_layout_tests {
    use super::Guard;
    fn prefix(data_type: arrow_schema::DataType) -> Vec<u8> {
        let schema =
            arrow_schema::Schema::new(vec![arrow_schema::Field::new("v", data_type, false)]);
        let mut bytes = Vec::new();
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
        writer.finish().unwrap();
        drop(writer);
        bytes.truncate(bytes.len() - 8);
        bytes
    }
    fn record(rows: i64, buffers: &[arrow_ipc::Buffer]) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let nodes = builder.create_vector(&[arrow_ipc::FieldNode::new(rows, 0)]);
        let buffers = builder.create_vector(buffers);
        let batch = arrow_ipc::RecordBatch::create(
            &mut builder,
            &arrow_ipc::RecordBatchArgs {
                length: rows,
                nodes: Some(nodes),
                buffers: Some(buffers),
                ..Default::default()
            },
        );
        let message = arrow_ipc::Message::create(
            &mut builder,
            &arrow_ipc::MessageArgs {
                version: arrow_ipc::MetadataVersion::V5,
                header_type: arrow_ipc::MessageHeader::RecordBatch,
                header: Some(batch.as_union_value()),
                bodyLength: 8,
                ..Default::default()
            },
        );
        builder.finish(message, None);
        let mut bytes = u32::MAX.to_le_bytes().to_vec();
        bytes.extend((builder.finished_data().len() as u32).to_le_bytes());
        bytes.extend(builder.finished_data());
        bytes
    }
    #[test]
    fn out_of_body_offsets_are_rejected_before_body_arrives() {
        let mut guard = Guard::new(65536);
        guard.feed(&prefix(arrow_schema::DataType::Int32)).unwrap();
        assert!(
            guard
                .feed(&record(
                    2,
                    &[
                        arrow_ipc::Buffer::new(0, 0),
                        arrow_ipc::Buffer::new(i64::MAX, 8)
                    ]
                ))
                .is_err()
        );
    }
    #[test]
    fn bufferless_logical_expansion_is_bounded_before_body_arrives() {
        let mut guard = Guard::new(65536);
        guard.feed(&prefix(arrow_schema::DataType::Null)).unwrap();
        assert!(guard.feed(&record(1 << 60, &[])).is_err());
    }
}
