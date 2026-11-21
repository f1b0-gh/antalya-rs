#![cfg(feature = "arrow")]
#![forbid(unsafe_code)]

#[path = "admitted_transport/http.rs"]
mod http;
mod support;
use antalya_rs::{
    AdmittedIpc, AntalyaClient, AuthorizationContext, AuthorizedRequest, Config, Credentials,
    ExecutionLimits, Format, IpcError, IpcFrameKind, Query,
};
use std::{io, time::Duration};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn admit(server: &support::Server, limits: ExecutionLimits) -> Result<AdmittedIpc> {
    let client = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
            .timeout(Duration::from_secs(2))
            .execution_limits(limits),
    )?;
    let request = AuthorizedRequest::new(
        Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
        server.endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "ipc-test".into(),
            expected_principal: "fixture".into(),
        },
    )?;
    Ok(client
        .prepare_authorized_arrow(request)
        .await?
        .ready_transport()
        .await?
        .admit_ipc()?)
}

#[tokio::test]
async fn frames_preserve_wire_content_and_do_not_observe_termination() -> Result<()> {
    let wire = support::stream(2);
    let server = support::Server::new(wire.clone(), 4, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    let receipt = stream.recovery_receipt()?;
    let mut joined = Vec::new();
    let mut kinds = Vec::new();
    while let Some(frame) = stream.next_frame().await? {
        assert!(frame.charged_bytes() >= frame.encoded().len());
        kinds.push(frame.kind());
        joined.extend_from_slice(frame.encoded());
    }
    assert_eq!(
        kinds,
        [
            IpcFrameKind::Schema,
            IpcFrameKind::RecordBatch,
            IpcFrameKind::RecordBatch
        ]
    );
    assert_eq!(joined, wire[..wire.len() - 8]);
    assert_eq!(stream.declared_rows(), 4);
    assert_eq!(stream.recovery_receipt()?.marker(), receipt.marker());
    let requests = server
        .requests
        .lock()
        .map_err(|_| io::Error::other("request lock"))?;
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains("output_format_arrow_compression_method=none"));
    Ok(())
}

#[tokio::test]
async fn empty_schema_is_valid_but_empty_or_truncated_wire_is_not() -> Result<()> {
    let wire = support::stream(0);
    let server = support::Server::new(wire.clone(), 0, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    assert_eq!(
        stream
            .next_frame()
            .await?
            .ok_or_else(|| io::Error::other("schema"))?
            .kind(),
        IpcFrameKind::Schema
    );
    assert!(stream.next_frame().await?.is_none());
    for body in [Vec::new(), wire[..wire.len() - 1].to_vec(), vec![0xff; 4]] {
        let server = support::Server::new(body, 0, false, 200);
        let mut stream = admit(&server, ExecutionLimits::default()).await?;
        while let Ok(Some(_)) = stream.next_frame().await {}
        assert!(matches!(stream.next_frame().await, Err(IpcError::Poisoned)));
    }
    Ok(())
}

#[tokio::test]
async fn metadata_and_batch_limits_refuse_before_returning_data() -> Result<()> {
    let server = support::Server::new(u32::MAX.to_le_bytes().repeat(2), 0, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    assert!(matches!(stream.next_frame().await, Err(IpcError::Limit)));
    for limits in [
        ExecutionLimits {
            max_rows: 1,
            ..ExecutionLimits::default()
        },
        ExecutionLimits {
            max_batch_bytes: 1,
            ..ExecutionLimits::default()
        },
    ] {
        let server = support::Server::new(support::stream(1), 2, false, 200);
        let mut stream = admit(&server, limits).await?;
        assert!(stream.next_frame().await?.is_some());
        assert!(matches!(stream.next_frame().await, Err(IpcError::Limit)));
    }
    Ok(())
}

#[tokio::test]
async fn metadata_failure_preserves_source_and_eos_refuses_trailing_bytes() -> Result<()> {
    let mut malformed = 8_u32.to_le_bytes().to_vec();
    // An out-of-range FlatBuffer root, rather than a valid empty table.
    malformed.extend([255_u8; 8]);
    let server = support::Server::new(malformed, 0, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    let error = stream
        .next_frame()
        .await
        .err()
        .ok_or_else(|| io::Error::other("expected error"))?;
    assert!(matches!(error, IpcError::Metadata(_)));
    assert!(std::error::Error::source(&error).is_some());
    let mut trailing = support::stream(0);
    trailing.push(1);
    let server = support::Server::new(trailing, 0, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    assert!(stream.next_frame().await?.is_some());
    assert!(matches!(
        stream.next_frame().await,
        Err(IpcError::InvalidFrame)
    ));
    Ok(())
}

#[tokio::test]
async fn fragmented_http_preserves_ipc_and_transport_errors_remain_typed() -> Result<()> {
    use std::io::Write;
    let wire = support::stream(1);
    let mut reply =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    for bytes in wire.chunks(3) {
        write!(reply, "{:x}\r\n", bytes.len())?;
        reply.extend_from_slice(bytes);
        reply.extend_from_slice(b"\r\n");
    }
    reply.extend_from_slice(b"0\r\n\r\n");
    let server = http::Server::new(reply)?;
    let mut stream = server
        .admit_limits(ExecutionLimits::default())
        .await?
        .into_ipc()?;
    let mut count = 0;
    while let Some(frame) = stream.next_frame().await? {
        count += usize::from(frame.kind() == IpcFrameKind::RecordBatch);
    }
    assert_eq!(count, 1);
    drop(stream);
    server.finish()?;
    let server = http::Server::new(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n\xff".to_vec())?;
    let mut stream = server.admit(1024).await?.into_ipc()?;
    let error = stream
        .next_frame()
        .await
        .err()
        .ok_or_else(|| io::Error::other("truncated HTTP"))?;
    assert!(matches!(
        error,
        IpcError::Transport(antalya_rs::TransportError::Http(_))
    ));
    assert!(
        std::error::Error::source(&error)
            .and_then(std::error::Error::source)
            .is_some()
    );
    drop(stream);
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn compressed_frames_are_refused_before_decompression() -> Result<()> {
    use std::sync::Arc;
    let batch = arrow_array::RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(arrow_array::Int32Array::from(vec![0; 256])) as arrow_array::ArrayRef,
    )])?;
    for compression in [
        arrow_ipc::CompressionType::LZ4_FRAME,
        arrow_ipc::CompressionType::ZSTD,
    ] {
        let mut wire = Vec::new();
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(compression))?;
        let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            &mut wire,
            &batch.schema(),
            options,
        )?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);
        let server = support::Server::new(wire, 256, false, 200);
        let mut stream = admit(&server, ExecutionLimits::default()).await?;
        assert!(stream.next_frame().await?.is_some());
        assert!(matches!(
            stream.next_frame().await,
            Err(IpcError::Compression)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn declared_oversized_or_negative_body_is_refused_without_reading_it() -> Result<()> {
    for (size, is_limit) in [(i64::MAX, true), (-1, false)] {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let batch = arrow_ipc::RecordBatch::create(&mut builder, &Default::default());
        let message = arrow_ipc::Message::create(
            &mut builder,
            &arrow_ipc::MessageArgs {
                version: arrow_ipc::MetadataVersion::V5,
                header_type: arrow_ipc::MessageHeader::RecordBatch,
                header: Some(batch.as_union_value()),
                bodyLength: size,
                ..Default::default()
            },
        );
        builder.finish(message, None);
        let mut wire = support::stream(0);
        wire.truncate(wire.len() - 8);
        wire.extend([255; 4]);
        wire.extend(u32::try_from(builder.finished_data().len())?.to_le_bytes());
        wire.extend(builder.finished_data());
        let server = support::Server::new(wire, 0, false, 200);
        let mut stream = admit(&server, ExecutionLimits::default()).await?;
        assert!(stream.next_frame().await?.is_some());
        let error = stream
            .next_frame()
            .await
            .err()
            .ok_or_else(|| io::Error::other("invalid body"))?;
        if is_limit {
            assert!(matches!(error, IpcError::Limit));
        } else {
            assert!(matches!(error, IpcError::InvalidFrame));
        }
    }
    Ok(())
}

#[tokio::test]
async fn insufficient_frame_capacity_refuses_before_data_admission() -> Result<()> {
    let server = support::Server::new(support::stream(0), 0, false, 200);
    let client = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
            .resources(antalya_rs::ResourceLimits {
                max_retained_bytes: 64 * 1024,
                ..Default::default()
            })
            .execution_limits(ExecutionLimits {
                max_batch_bytes: 1024,
                ..Default::default()
            }),
    )?;
    let query = || -> Result<AuthorizedRequest> {
        Ok(AuthorizedRequest::new(
            Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
            server.endpoint.clone(),
            Format::ArrowStream,
            AuthorizationContext {
                receipt_id: "capacity".into(),
                expected_principal: "fixture".into(),
            },
        )?)
    };
    let ready = client
        .prepare_authorized_arrow(query()?)
        .await?
        .ready_transport()
        .await?;
    assert!(matches!(ready.admit_ipc(), Err(IpcError::Limit)));
    {
        let requests = server
            .requests
            .lock()
            .map_err(|_| io::Error::other("request lock"))?;
        assert!(!requests.iter().any(|request| request.contains("SELECT v")));
    }
    assert!(client.prepare_authorized_arrow(query()?).await.is_ok());
    Ok(())
}

#[tokio::test]
async fn cancelled_partial_frame_resumes_without_losing_bytes() -> Result<()> {
    let wire = support::stream(1);
    let mut reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        wire.len()
    )
    .into_bytes();
    reply.extend(&wire);
    // Hold the rest of the response until the test explicitly resumes it. The
    // first frame cannot finish early even if the polling task is descheduled.
    let (release, wait) = std::sync::mpsc::channel();
    let server = http::Server::fragmented(reply, 16, Duration::ZERO, Some((96, wait)))?;
    let mut stream = server
        .admit_limits(ExecutionLimits::default())
        .await?
        .into_ipc()?;
    // Data polling has consumed part of a frame when this borrowed wait is dropped.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.next_frame())
            .await
            .is_err()
    );
    assert!(stream.wire_bytes() > 0);
    release.send(())?;
    let mut joined = Vec::new();
    while let Some(frame) = stream.next_frame().await? {
        joined.extend_from_slice(frame.encoded());
    }
    assert_eq!(joined, wire[..wire.len() - 8]);
    drop(stream);
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn retained_frame_applies_backpressure_and_cancelled_wait_resumes() -> Result<()> {
    use std::sync::Arc;
    let batch = arrow_array::RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(arrow_array::Int32Array::from_iter_values(0..65_536)) as arrow_array::ArrayRef,
    )])?;
    let mut wire = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut wire, &batch.schema())?;
    writer.write(&batch)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    let server = support::Server::new(wire, 131_072, false, 200);
    let client = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
            .timeout(Duration::from_secs(2))
            .resources(antalya_rs::ResourceLimits {
                max_retained_bytes: 512 * 1024 + 2 * 64 * 1024 + 8,
                ..Default::default()
            })
            .execution_limits(ExecutionLimits {
                max_batch_bytes: 512 * 1024,
                ..Default::default()
            }),
    )?;
    let request = AuthorizedRequest::new(
        Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
        server.endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "bounded-frames".into(),
            expected_principal: "fixture".into(),
        },
    )?;
    let mut stream = client
        .prepare_authorized_arrow(request)
        .await?
        .ready_transport()
        .await?
        .admit_ipc()?;
    drop(
        stream
            .next_frame()
            .await?
            .ok_or_else(|| io::Error::other("schema"))?,
    );
    let first = stream
        .next_frame()
        .await?
        .ok_or_else(|| io::Error::other("first batch"))?;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.next_frame())
            .await
            .is_err()
    );
    drop(first);
    let second = stream
        .next_frame()
        .await?
        .ok_or_else(|| io::Error::other("second batch"))?;
    assert_eq!(second.kind(), IpcFrameKind::RecordBatch);
    drop(second);
    assert!(stream.next_frame().await?.is_none());
    assert_eq!(stream.declared_rows(), 131_072);
    assert_eq!(
        server
            .requests
            .lock()
            .map_err(|_| io::Error::other("request lock"))?
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires the digest-pinned bearer fixture"]
async fn live_ipc_frames_preserve_empty_large_and_dictionary_results() -> Result<()> {
    let endpoint = antalya_rs::Endpoint::parse(&std::env::var("ANTALYA_TEST_URL")?)?;
    let client = AntalyaClient::new(
        Config::new(
            endpoint.clone(),
            Credentials::bearer(&std::env::var("ANTALYA_TEST_TOKEN")?)?,
        )
        .timeout(Duration::from_secs(30))
        .execution_limits(ExecutionLimits {
            max_batch_bytes: 1024 * 1024,
            max_total_bytes: 2 * 1024 * 1024,
            ..Default::default()
        }),
    )?;
    for (rows, dictionary) in [(0_u64, false), (100_000, false), (20, true)] {
        let sql = if dictionary {
            format!("SELECT toLowCardinality(toString(number % 3)) AS value FROM numbers({rows})")
        } else {
            format!("SELECT number FROM numbers({rows})")
        };
        let query = Query::new(sql, uuid::Uuid::new_v4().to_string())?.setting(
            "output_format_arrow_low_cardinality_as_dictionary",
            if dictionary { "1" } else { "0" },
        )?;
        let request = AuthorizedRequest::new(
            query,
            endpoint.clone(),
            Format::ArrowStream,
            AuthorizationContext {
                receipt_id: "live-ipc".into(),
                expected_principal: "fixture".into(),
            },
        )?;
        let ready = client
            .prepare_authorized_arrow(request)
            .await?
            .ready_transport()
            .await?;
        let original = ready.recovery_receipt()?;
        let mut stream = ready.admit_ipc()?;
        let mut encoded = Vec::new();
        let mut dictionaries = 0;
        while let Some(frame) = stream.next_frame().await? {
            dictionaries += usize::from(frame.kind() == IpcFrameKind::DictionaryBatch);
            assert!(encoded.len() + frame.encoded().len() <= 2 * 1024 * 1024);
            encoded.extend_from_slice(frame.encoded());
        }
        assert_eq!(stream.declared_rows(), rows);
        assert_eq!(stream.recovery_receipt()?.marker(), original.marker());
        assert_eq!(dictionaries > 0, dictionary);
        // Independent upstream decoder checks frame preservation; this collection
        // is a bounded test oracle, not the future production decode path.
        let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(encoded), None)?;
        let mut decoded = 0;
        for batch in reader {
            let batch = batch?;
            if !dictionary {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .ok_or_else(|| io::Error::other("unexpected live integer type"))?;
                for value in values.iter() {
                    assert_eq!(value, Some(decoded));
                    decoded += 1;
                }
            } else {
                assert!(matches!(
                    batch.schema().field(0).data_type(),
                    arrow_schema::DataType::Dictionary(_, _)
                ));
                decoded += u64::try_from(batch.num_rows())?;
            }
        }
        assert_eq!(decoded, rows);
        assert_eq!(
            stream.confirm_termination().await?,
            antalya_rs::RemoteTermination::Succeeded
        );
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_streams_preserve_frames_or_refuse_internal_capacity() -> Result<()> {
    use std::sync::Arc;
    let batch = arrow_array::RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(arrow_array::Int32Array::from_iter_values(0..65_536)) as arrow_array::ArrayRef,
    )])?;
    let mut wire = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut wire, &batch.schema())?;
    writer.write(&batch)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    for capacity in [4 * 1024 * 1024, 8 * 1024 * 1024] {
        let server = support::Server::new(wire.clone(), 131_072, false, 200);
        let client = AntalyaClient::new(
            Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
                .timeout(Duration::from_secs(10))
                .resources(antalya_rs::ResourceLimits {
                    max_retained_bytes: capacity,
                    ..Default::default()
                })
                .execution_limits(ExecutionLimits {
                    max_batch_bytes: 1024 * 1024,
                    ..Default::default()
                }),
        )?;
        let mut streams = Vec::new();
        for _ in 0..4 {
            let request = AuthorizedRequest::new(
                Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
                server.endpoint.clone(),
                Format::ArrowStream,
                AuthorizationContext {
                    receipt_id: "concurrent-frames".into(),
                    expected_principal: "fixture".into(),
                },
            )?;
            streams.push(
                client
                    .prepare_authorized_arrow(request)
                    .await?
                    .ready_transport()
                    .await?
                    .admit_ipc()?,
            );
        }
        let expected = Arc::new(wire[..wire.len() - 8].to_vec());
        let mut tasks = tokio::task::JoinSet::new();
        for mut stream in streams {
            let expected = Arc::clone(&expected);
            tasks.spawn(async move {
                let mut received = Vec::new();
                loop {
                    match stream.next_frame().await {
                        Ok(Some(frame)) => {
                            received.extend_from_slice(frame.encoded());
                            drop(frame);
                            tokio::task::yield_now().await;
                        }
                        Ok(None) => break,
                        Err(IpcError::Transport(antalya_rs::TransportError::Client(
                            antalya_rs::Error::Context {
                                cause: antalya_rs::Cause::Capacity,
                                ..
                            },
                        ))) => return Ok(false),
                        Err(error) => {
                            return Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync>);
                        }
                    }
                }
                assert_eq!(received, *expected);
                assert_eq!(stream.declared_rows(), 131_072);
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(true)
            });
        }
        let mut completed = 0;
        while let Some(result) = tasks.join_next().await {
            completed += usize::from(result??);
        }
        assert!(completed > 0);
        if capacity == 8 * 1024 * 1024 {
            assert_eq!(completed, 4);
        }
        assert_eq!(
            server
                .requests
                .lock()
                .map_err(|_| io::Error::other("requests"))?
                .len(),
            8
        );
    }
    Ok(())
}
