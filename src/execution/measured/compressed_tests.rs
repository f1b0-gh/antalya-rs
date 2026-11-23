// SPDX-License-Identifier: Apache-2.0
use super::compressed::Guard;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_buffer::Buffer;
use arrow_ipc::{
    CompressionType,
    reader::StreamDecoder,
    writer::{IpcWriteOptions, StreamWriter},
};
use std::sync::Arc;

fn encoded(codec: Option<CompressionType>) -> (RecordBatch, Vec<u8>) {
    let batch = RecordBatch::try_from_iter(vec![
        ("v", Arc::new(Int32Array::from(vec![1; 2048])) as ArrayRef),
        (
            "s",
            Arc::new(StringArray::from(vec![Some("compressible value"); 2048])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::default()
        .try_with_compression(codec)
        .unwrap();
    let mut writer =
        StreamWriter::try_new_with_options(&mut bytes, &batch.schema(), options).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    (batch, bytes)
}

#[test]
fn zstd_and_uncompressed_values_survive_arbitrary_chunk_boundaries() {
    for codec in [None, Some(CompressionType::ZSTD)] {
        let (expected, bytes) = encoded(codec);
        for chunk_size in [1, 7, 4096, bytes.len()] {
            let mut guard = Guard::new(131072);
            let mut decoder = StreamDecoder::new();
            let mut batches = Vec::new();
            for chunk in bytes.chunks(chunk_size) {
                let mut input = Buffer::from(chunk);
                while let Some(mut frame) = guard.next(&mut input).unwrap() {
                    while !frame.is_empty() {
                        if let Some(batch) = decoder.decode(&mut frame).unwrap() {
                            batches.push(batch);
                        }
                    }
                }
            }
            guard.finish().unwrap();
            decoder.finish().unwrap();
            assert_eq!(batches, vec![expected.clone()]);
        }
    }
}

#[test]
fn lz4_is_refused_and_truncated_frames_are_not_complete() {
    let (_, bytes) = encoded(Some(CompressionType::LZ4_FRAME));
    let mut input = Buffer::from(bytes);
    let mut guard = Guard::new(131072);
    loop {
        match guard.next(&mut input) {
            Err(_) => break,
            Ok(Some(_)) => (),
            Ok(None) => panic!("LZ4 accepted"),
        }
    }
    let (_, bytes) = encoded(Some(CompressionType::ZSTD));
    for cut in [1, 7, bytes.len() - 1, bytes.len() - 8] {
        let mut input = Buffer::from(&bytes[..cut]);
        let mut guard = Guard::new(131072);
        while guard.next(&mut input).unwrap().is_some() {}
        assert!(guard.finish().is_err(), "cut {cut}");
    }
}

#[test]
fn expansion_is_refused_before_arrow_sees_record_batch() {
    let (_, bytes) = encoded(Some(CompressionType::ZSTD));
    let mut guard = Guard::new(8192);
    let mut input = Buffer::from(bytes);
    assert!(guard.next(&mut input).unwrap().is_some()); // schema
    let error = guard.next(&mut input).unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::LimitExceeded);
}

fn first_payload(bytes: &[u8]) -> usize {
    let mut offset = 0;
    loop {
        assert_eq!(&bytes[offset..offset + 4], &u32::MAX.to_le_bytes());
        let len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let msg = arrow_ipc::root_as_message(&bytes[offset + 8..offset + 8 + len]).unwrap();
        if let Some(batch) = msg.header_as_record_batch() {
            let buffer = batch
                .buffers()
                .unwrap()
                .iter()
                .find(|b| b.length() > 8)
                .unwrap();
            return offset + 8 + len + buffer.offset() as usize;
        }
        offset += 8 + len + msg.bodyLength() as usize;
    }
}

#[test]
fn hostile_expansion_prefixes_are_rejected_before_decode() {
    let (_, original) = encoded(Some(CompressionType::ZSTD));
    let start = first_payload(&original);
    for size in [-2i64, i64::MAX, 131073] {
        let mut bytes = original.clone();
        bytes[start..start + 8].copy_from_slice(&size.to_le_bytes());
        let mut input = Buffer::from(bytes);
        let mut guard = Guard::new(131072);
        assert!(guard.next(&mut input).unwrap().is_some());
        assert!(guard.next(&mut input).is_err());
        assert!(guard.next(&mut input).is_err()); // poisoned, cannot resume
    }
}

#[test]
fn lying_but_bounded_expansion_is_rejected_by_arrow() {
    let (_, mut bytes) = encoded(Some(CompressionType::ZSTD));
    let start = first_payload(&bytes);
    // Int32 data needs 8192 bytes. 8193 passes layout minimums but is a lie.
    bytes[start..start + 8].copy_from_slice(&8193i64.to_le_bytes());
    let mut input = Buffer::from(bytes);
    let mut guard = Guard::new(131072);
    let mut decoder = StreamDecoder::new();
    let mut schema = guard.next(&mut input).unwrap().unwrap();
    assert!(decoder.decode(&mut schema).unwrap().is_none());
    let mut frame = guard.next(&mut input).unwrap().unwrap();
    assert!(decoder.decode(&mut frame).is_err());
}

#[test]
fn compressed_dictionary_values_keep_dictionary_validation() {
    let values: arrow_array::DictionaryArray<arrow_array::types::Int8Type> = [
        Some("repeated one"),
        None,
        Some("repeated two"),
        Some("repeated one"),
    ]
    .into_iter()
    .collect();
    let expected = RecordBatch::try_from_iter(vec![("v", Arc::new(values) as ArrayRef)]).unwrap();
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap();
    let mut writer =
        StreamWriter::try_new_with_options(&mut bytes, &expected.schema(), options).unwrap();
    writer.write(&expected).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let mut input = Buffer::from(bytes);
    let mut guard = Guard::new(131072);
    let mut decoder = StreamDecoder::new();
    let mut actual = Vec::new();
    while let Some(mut frame) = guard.next(&mut input).unwrap() {
        while !frame.is_empty() {
            if let Some(batch) = decoder.decode(&mut frame).unwrap() {
                actual.push(batch);
            }
        }
    }
    guard.finish().unwrap();
    decoder.finish().unwrap();
    assert_eq!(actual, vec![expected]);
}
