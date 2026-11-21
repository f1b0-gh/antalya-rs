// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "arrow")]
#![forbid(unsafe_code)]

#[allow(dead_code)]
mod support;
use antalya_rs::{
    AntalyaClient, AuthorizationContext, AuthorizedRequest, Cause, Config, Credentials, ErrorKind,
    ExecutionLimits, Format, Outcome, Phase, Query,
};
use arrow_array::{ArrayRef, BinaryArray, RecordBatch, UInt64Array};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, mpsc},
    time::Duration,
};

const ROWS: usize = 8192;

fn stream(batches: usize) -> Vec<u8> {
    let values = Arc::new(BinaryArray::from_iter_values(
        (0..ROWS).map(|_| vec![b'x'; 128]),
    )) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(UInt64Array::from_iter_values(0..ROWS as u64)) as ArrayRef,
        ),
        ("v", values.clone()),
        ("v_lower", values.clone()),
        ("v_winpath", values),
    ])
    .unwrap();
    let mut output = Vec::new();
    let mut writer =
        arrow_ipc::writer::StreamWriter::try_new(&mut output, &batch.schema()).unwrap();
    for _ in 0..batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    output
}

fn client(server: &support::Server, limits: ExecutionLimits) -> AntalyaClient {
    AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("token").unwrap(),
        )
        .timeout(Duration::from_secs(5))
        .execution_limits(limits),
    )
    .unwrap()
}

fn verify(batch: &antalya_rs::BudgetedBatch) {
    assert_eq!(batch.num_rows(), ROWS);
    assert!(batch.charged_bytes() < 16 * 1024 * 1024);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert!(ids.values().iter().copied().eq(0..ROWS as u64));
    for column in &batch.columns()[1..] {
        let values = column.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(values.iter().all(|v| v == Some(&[b'x'; 128][..])));
    }
}

#[tokio::test]
async fn uncompressed_shared_body_delivers_all_values_under_unchanged_default_caps() {
    let server = support::Server::new(stream(1), ROWS as u64, false, 200);
    let client = client(&server, ExecutionLimits::default());
    let mut execution = client
        .fetch_arrow(Query::new("SELECT id,v,v_lower,v_winpath", "shared-body").unwrap())
        .await
        .unwrap();
    let batch = execution.next_batch().await.unwrap().unwrap();
    verify(&batch);
    drop(batch);
    assert!(execution.next_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Succeeded);
}

#[tokio::test]
async fn admitted_shared_body_keeps_explicit_completion_and_all_values() {
    let server = support::Server::new(stream(1), ROWS as u64, false, 200);
    let client = client(&server, ExecutionLimits::default());
    let request = AuthorizedRequest::new(
        Query::new("SELECT id,v,v_lower,v_winpath", "shared-body-admitted").unwrap(),
        server.endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "shared-body".into(),
            expected_principal: "fixture".into(),
        },
    )
    .unwrap();
    let mut stream = client
        .prepare_authorized_arrow(request)
        .await
        .unwrap()
        .ready_transport()
        .await
        .unwrap()
        .admit_arrow()
        .unwrap();
    let batch = stream.next_batch().await.unwrap().unwrap();
    verify(&batch);
    drop(batch);
    assert!(stream.next_batch().await.unwrap().is_none());
    assert_eq!(stream.outcome(), Outcome::Running);
    stream.confirm_completion().await.unwrap();
    assert_eq!(stream.outcome(), Outcome::Succeeded);
}

#[tokio::test]
async fn true_batch_limit_still_refuses_before_delivery() {
    let server = support::Server::new(stream(1), ROWS as u64, false, 200);
    let client = client(
        &server,
        ExecutionLimits {
            max_batch_bytes: 1024,
            ..ExecutionLimits::default()
        },
    );
    let mut execution = client
        .fetch_arrow(Query::new("SELECT id,v,v_lower,v_winpath", "real-limit").unwrap())
        .await
        .unwrap();
    let error = execution.next_batch().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::LimitExceeded);
    assert_eq!(error.phase(), Some(Phase::Data));
    assert_eq!(error.cause(), Some(Cause::LimitExceeded));
    assert_eq!(execution.batches_delivered(), 0);
    assert_eq!(execution.outcome(), Outcome::Unknown);
}

#[tokio::test]
async fn cumulative_limit_refuses_gated_batches_without_assuming_a_failure_ordinal() {
    const BATCHES: usize = 6;
    const CAP: usize = 16 * 1024 * 1024;
    let encoded = stream(BATCHES);
    // Split at IPC record boundaries. The server cannot put a later batch into
    // the decoder's backing allocation before the consumer requests that batch.
    // Each ~3.3 MiB body and its bounded decoder scratch growth fit the 16 MiB
    // batch cap; six independent payloads necessarily exceed the cumulative cap.
    let (mut offset, mut start) = (0, 0);
    let mut parts = Vec::new();
    while offset < encoded.len() {
        let prefix = u32::from_le_bytes(encoded[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let length = if prefix == u32::MAX {
            let n = u32::from_le_bytes(encoded[offset..offset + 4].try_into().unwrap());
            offset += 4;
            n
        } else {
            prefix
        } as usize;
        if length == 0 {
            break;
        }
        let message = arrow_ipc::root_as_message(&encoded[offset..offset + length]).unwrap();
        offset += length + usize::try_from(message.bodyLength()).unwrap();
        if message.header_type() == arrow_ipc::MessageHeader::RecordBatch {
            parts.push(encoded[start..offset].to_vec());
            start = offset;
        }
    }
    parts
        .last_mut()
        .unwrap()
        .extend_from_slice(&encoded[start..]);
    assert_eq!(parts.len(), BATCHES);
    assert!(parts.iter().all(|part| 2 * part.len() + 16384 < CAP));
    let length = encoded.len();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint =
        antalya_rs::Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let (permit, requested) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut request = Vec::new();
        loop {
            let mut chunk = [0_u8; 4096];
            let read = socket.read(&mut chunk).unwrap();
            assert!(read > 0);
            request.extend_from_slice(&chunk[..read]);
            if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]);
                let body = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|n| n.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if request.len() >= end + 4 + body {
                    break;
                }
            }
        }
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        for part in parts {
            if requested.recv_timeout(Duration::from_secs(10)).is_err() {
                break;
            }
            if socket.write_all(&part).is_err() {
                break;
            }
        }
    });
    let client = AntalyaClient::new(
        Config::new(endpoint, Credentials::bearer("token").unwrap())
            .timeout(Duration::from_secs(10))
            .execution_limits(ExecutionLimits {
                max_batch_bytes: CAP,
                max_total_bytes: CAP as u64,
                ..ExecutionLimits::default()
            }),
    )
    .unwrap();
    let mut execution = client
        .fetch_arrow(Query::new("SELECT values", "cumulative-gated").unwrap())
        .await
        .unwrap();
    let mut delivered = 0;
    let error = loop {
        permit.send(()).unwrap();
        match execution.next_batch().await {
            Ok(Some(batch)) => {
                verify(&batch);
                delivered += 1;
                assert!(delivered < BATCHES);
            }
            Err(error) => break error,
            Ok(None) => panic!("over-limit stream must not complete"),
        }
    };
    drop(permit);
    server.join().unwrap();
    assert!(delivered > 0);
    assert_eq!(execution.batches_delivered(), delivered as u64);
    assert!(execution.decoded_bytes_delivered() <= CAP as u64);
    assert_eq!(error.phase(), Some(Phase::Data));
    assert_eq!(error.cause(), Some(Cause::LimitExceeded));
    assert_eq!(execution.outcome(), Outcome::Unknown);
}
