// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "arrow")]
#[allow(dead_code)]
mod support;
use antalya_rs::{
    AntalyaClient, Config, Credentials, Endpoint, Outcome, Query, TerminalPolicy, TerminalStrategy,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    io::{Read, Write},
    net::TcpListener,
    time::Duration,
};

fn packet_stream(rows: u64, batches: usize) -> Vec<u8> {
    let mut out = format!(
        "{{\"packet\":\"data\",\"data\":\"{}\"}}\n",
        STANDARD.encode(support::stream(batches))
    );
    out += &format!(
        "{{\"packet\":\"progress\",\"progress\":{{\"result_rows\":\"{rows}\",\"result_bytes\":\"0\"}}}}\n"
    );
    out.into_bytes()
}
fn server(
    body: Vec<u8>,
    id: &str,
    content_type: &str,
    cut: bool,
) -> (AntalyaClient, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let id = id.to_owned();
    let content_type = content_type.to_owned();
    let worker = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = Vec::new();
        let mut chunk = [0; 4096];
        while let Ok(n) = socket.read(&mut chunk) {
            if n == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..n]);
            if let Some(end) = request.windows(4).position(|p| p == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&request[..end]);
                let len = head
                    .lines()
                    .find_map(|s| {
                        s.to_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if request.len() >= end + 4 + len {
                    break;
                }
            }
        }
        let request = String::from_utf8_lossy(&request);
        assert!(request.contains("framing_output_format=JSONEachPacketBase64"));
        assert!(
            request
                .to_lowercase()
                .contains("authorization: bearer token")
        );
        write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nX-ClickHouse-Query-Id: {id}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()+usize::from(cut)).unwrap();
        for chunk in body.chunks(7) {
            if socket.write_all(chunk).is_err() {
                break;
            }
        }
    });
    let client = AntalyaClient::new(
        Config::new(endpoint, Credentials::bearer("token").unwrap())
            .timeout(Duration::from_secs(2))
            .terminal_policy(TerminalPolicy {
                strategy: TerminalStrategy::InBand,
                ..Default::default()
            }),
    )
    .unwrap();
    (client, worker)
}
const TYPE: &str = "application/x-ndjson; charset=UTF-8; payload=base64";
#[tokio::test]
async fn framed_success_requires_terminal_count_and_has_no_control_roundtrip() {
    for batches in [0, 1, 2] {
        let (client, worker) = server(
            packet_stream(batches as u64 * 2, batches),
            "framed-success",
            TYPE,
            false,
        );
        let mut e = client
            .fetch_arrow(Query::new("SELECT v", "framed-success").unwrap())
            .await
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = e.next_provisional_batch().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(e.outcome(), Outcome::Running);
        assert_eq!(rows, batches * 2);
        e.confirm_completion().await.unwrap();
        assert_eq!(e.outcome(), Outcome::Succeeded);
        let m = e.metrics();
        assert_eq!(m.control_requests, 0);
        assert_eq!(m.flush_attempts, 0);
        assert_eq!(m.polls, 0);
        assert!(m.transfer.is_some());
        assert!(m.decode.is_some());
        assert!(m.wire_bytes > 0);
        worker.join().unwrap();
    }
}
#[tokio::test]
async fn framed_ambiguous_or_corrupt_tails_never_confirm() {
    let valid = packet_stream(2, 1);
    let data = valid[..valid.iter().position(|b| *b == b'\n').unwrap() + 1].to_vec();
    let mut cases = vec![data.clone(), packet_stream(3, 1)];
    let mut no_eos = support::stream(1);
    no_eos.truncate(no_eos.len() - 8);
    cases.push(format!("{{\"packet\":\"data\",\"data\":\"{}\"}}\n{{\"packet\":\"progress\",\"progress\":{{\"result_rows\":2}}}}\n",STANDARD.encode(no_eos)).into_bytes());
    cases.push(
        [
            valid.clone(),
            b"{\"packet\":\"data\",\"data\":\"AAAA\"}\n".to_vec(),
        ]
        .concat(),
    );
    cases.push(
        [
            data.clone(),
            b"{\"packet\":\"progress\",\"progress\":{\"result_rows\":2,\"result_rows\":0}}\n"
                .to_vec(),
        ]
        .concat(),
    );
    cases.push(
        [
            data.clone(),
            b"{\"packet\":\"progress\",\"progress\":{\"read_rows\":2}}\n".to_vec(),
        ]
        .concat(),
    );
    cases.push(
        [
            data,
            b"{\"packet\":\"exception\",\"exception\":\"late SECRET\"}\n".to_vec(),
        ]
        .concat(),
    );
    cases.push(
        [
            valid.clone(),
            b"{\"packet\":\"progress\",\"progress\":{}}\n".to_vec(),
        ]
        .concat(),
    );
    cases.push([valid.clone(), b"garbage\n".to_vec()].concat());
    cases.push(valid[..valid.len() - 1].to_vec());
    for body in cases {
        let (client, worker) = server(body, "framed-invalid", TYPE, false);
        let mut e = client
            .fetch_arrow(Query::new("SELECT v", "framed-invalid").unwrap())
            .await
            .unwrap();
        loop {
            match e.next_batch().await {
                Ok(Some(_)) => (),
                Ok(None) => panic!("invalid stream succeeded"),
                Err(error) => {
                    assert!(!format!("{error:?}").contains("SECRET"));
                    break;
                }
            }
        }
        assert_ne!(e.outcome(), Outcome::Succeeded);
        assert_eq!(e.metrics().control_requests, 0);
        worker.join().unwrap();
    }
}
#[tokio::test]
async fn framed_wrong_identity_type_or_http_truncation_is_refused() {
    for (id, ty, cut) in [
        ("other", TYPE, false),
        ("framed-meta", "application/octet-stream", false),
        ("framed-meta", TYPE, true),
    ] {
        let (client, worker) = server(packet_stream(2, 1), id, ty, cut);
        let mut e = client
            .fetch_arrow(Query::new("SELECT v", "framed-meta").unwrap())
            .await
            .unwrap();
        loop {
            match e.next_batch().await {
                Ok(Some(_)) => (),
                Ok(None) => panic!("invalid metadata succeeded"),
                Err(_) => break,
            }
        }
        assert_ne!(e.outcome(), Outcome::Succeeded);
        worker.join().unwrap();
    }
}

#[tokio::test]
async fn native_empty_final_memory_counter_distinguishes_zero_rows_from_progress() {
    let data = format!(
        "{{\"packet\":\"data\",\"data\":\"{}\"}}\n",
        STANDARD.encode(support::stream(0))
    );
    let body=(data+"{\"packet\":\"progress\",\"progress\":{\"elapsed_ns\":\"1\",\"memory_usage\":\"1024\"}}\n").into_bytes();
    let (client, worker) = server(body, "empty-memory", TYPE, false);
    let mut e = client
        .fetch_arrow(Query::new("SELECT v", "empty-memory").unwrap())
        .await
        .unwrap();
    assert!(e.next_batch().await.unwrap().is_none());
    assert_eq!(e.outcome(), Outcome::Succeeded);
    worker.join().unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/fixture.py --framed"]
async fn native_framed_empty_rows_late_exception_and_read_denial()
-> Result<(), Box<dyn std::error::Error>> {
    let endpoint = Endpoint::parse(&std::env::var("ANTALYA_TEST_URL")?)?;
    let token = std::env::var("ANTALYA_TEST_TOKEN")?;
    let client = AntalyaClient::new(
        Config::new(endpoint.clone(), Credentials::bearer(&token)?)
            .timeout(Duration::from_secs(10))
            .terminal_policy(TerminalPolicy {
                strategy: TerminalStrategy::InBand,
                ..Default::default()
            }),
    )?;
    for rows in [0, 1, 4096] {
        let mut execution = client
            .fetch_arrow(
                Query::new(
                    format!("SELECT toUInt64(number) AS v FROM numbers({rows})"),
                    format!("native-framed-{}", uuid::Uuid::new_v4()),
                )?
                .setting("max_block_size", "256")?
                .setting("max_threads", "1")?,
            )
            .await?;
        let mut actual = Vec::new();
        let mut batches = 0;
        while let Some(batch) = execution.next_batch().await? {
            batches += 1;
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .ok_or("wrong Arrow type")?;
            actual.extend(values.values().iter().copied());
        }
        assert!(actual.into_iter().eq(0..rows));
        if rows == 4096 {
            assert!(batches > 1);
        }
        assert_eq!(execution.outcome(), Outcome::Succeeded);
        let metrics = execution.metrics();
        assert_eq!(metrics.control_requests, 0);
        assert!(metrics.wire_bytes > 0);
        assert!(metrics.transfer.is_some() && metrics.decode.is_some());
        println!(
            "framed_component_sample {}",
            serde_json::json!({"rows":rows,"batches":batches,"http_entity_bytes":metrics.wire_bytes,"body_wait_us":metrics.transfer.map(|v|v.as_micros()),"arrow_decode_us":metrics.decode.map(|v|v.as_micros()),"framing_us":metrics.framing.as_micros(),"control_requests":metrics.control_requests,"scope":"single functional observation, dev build, shared host"})
        );
    }
    let mut slow = client
        .fetch_arrow(
            Query::new(
                "SELECT number,sleepEachRow(0.005) AS pause FROM numbers(64)",
                format!("native-framed-{}", uuid::Uuid::new_v4()),
            )?
            .setting("max_block_size", "16")?
            .setting("interactive_delay", "1000")?,
        )
        .await?;
    let mut count = 0;
    while let Some(batch) = slow.next_batch().await? {
        count += batch.num_rows();
    }
    assert_eq!(count, 64);
    assert_eq!(slow.outcome(), Outcome::Succeeded);
    assert_eq!(slow.metrics().control_requests, 0);
    let query = Query::new(
        "SELECT number,throwIf(number>=2048) AS bad FROM numbers(10000)",
        format!("native-framed-{}", uuid::Uuid::new_v4()),
    )?
    .setting("max_block_size", "1024")?;
    let mut late = client.fetch_arrow(query).await?;
    let mut delivered = 0;
    loop {
        match late.next_batch().await {
            Ok(Some(batch)) => delivered += batch.num_rows(),
            Ok(None) => return Err("late error became successful EOF".into()),
            Err(error) => {
                assert_eq!(error.kind(), antalya_rs::ErrorKind::QueryFailed);
                break;
            }
        }
    }
    assert!(delivered > 0);
    assert_eq!(late.outcome(), Outcome::Failed);
    assert_eq!(late.metrics().control_requests, 0);
    let denied = AntalyaClient::new(
        Config::new(endpoint, Credentials::bearer("invalid-token")?).terminal_policy(
            TerminalPolicy {
                strategy: TerminalStrategy::InBand,
                ..Default::default()
            },
        ),
    )?;
    let mut e = denied
        .fetch_arrow(Query::new(
            "SELECT 1",
            format!("native-framed-{}", uuid::Uuid::new_v4()),
        )?)
        .await?;
    assert!(e.next_batch().await.is_err());
    assert_ne!(e.outcome(), Outcome::Succeeded);
    Ok(())
}

#[tokio::test]
async fn cancelled_header_and_partial_packet_polls_resume_the_same_request() {
    for (pause_before_headers, compression, partial_ipc) in [
        (true, antalya_rs::ArrowCompression::None, false),
        (false, antalya_rs::ArrowCompression::None, false),
        (true, antalya_rs::ArrowCompression::Zstd, false),
        (false, antalya_rs::ArrowCompression::Zstd, false),
        (false, antalya_rs::ArrowCompression::Zstd, true),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint =
            Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (body, pause_at) = if partial_ipc {
            let batch = arrow_ipc::reader::StreamReader::try_new(
                std::io::Cursor::new(support::stream(1)),
                None,
            )
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
            let mut ipc = Vec::new();
            let options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))
                .unwrap();
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                &mut ipc,
                &batch.schema(),
                options,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            drop(writer);
            // Eight EOS bytes plus four body bytes remain. The first complete
            // data packet therefore leaves IPC staging waiting inside a body.
            let split = ipc.len() - 12;
            let first = format!(
                "{{\"packet\":\"data\",\"data\":\"{}\"}}\n",
                STANDARD.encode(&ipc[..split])
            );
            let rest = format!(
                "{{\"packet\":\"data\",\"data\":\"{}\"}}\n{{\"packet\":\"progress\",\"progress\":{{\"result_rows\":\"2\",\"result_bytes\":\"0\"}}}}\n",
                STANDARD.encode(&ipc[split..])
            );
            let pause_at = first.len();
            ((first + &rest).into_bytes(), pause_at)
        } else {
            (packet_stream(2, 1), 7)
        };
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut buffer = [0; 8192];
            assert!(socket.read(&mut buffer).unwrap() > 0);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {TYPE}\r\nX-ClickHouse-Query-Id: cancel-frame\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            if !pause_before_headers {
                socket.write_all(header.as_bytes()).unwrap();
                socket.write_all(&body[..pause_at]).unwrap();
            }
            std::thread::sleep(Duration::from_millis(10));
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            if pause_before_headers {
                socket.write_all(header.as_bytes()).unwrap();
                socket.write_all(&body).unwrap();
            } else {
                socket.write_all(&body[pause_at..]).unwrap();
            }
        });
        let client = AntalyaClient::new(
            Config::new(endpoint, Credentials::bearer("token").unwrap())
                .timeout(Duration::from_secs(3))
                .arrow_compression(compression)
                .terminal_policy(TerminalPolicy {
                    strategy: TerminalStrategy::InBand,
                    ..Default::default()
                }),
        )
        .unwrap();
        let mut execution = client
            .fetch_arrow(Query::new("SELECT v", "cancel-frame").unwrap())
            .await
            .unwrap();
        tokio::select! {
            result=execution.next_batch()=>panic!("blocked stream returned {result:?}"),
            entered=entered_rx=>entered.unwrap(),
        }
        assert_eq!(execution.outcome(), Outcome::Running);
        assert!(execution.metrics().transfer.unwrap() >= Duration::from_millis(5));
        release_tx.send(()).unwrap();
        assert_eq!(execution.next_batch().await.unwrap().unwrap().num_rows(), 2);
        assert!(execution.next_batch().await.unwrap().is_none());
        assert_eq!(execution.outcome(), Outcome::Succeeded);
        assert_eq!(execution.metrics().control_requests, 0);
        worker.join().unwrap();
    }
}

#[tokio::test]
async fn in_band_commands_refuse_before_dispatch_and_setting_is_reserved() {
    let client = AntalyaClient::new(
        Config::new(
            Endpoint::parse("http://127.0.0.1:1").unwrap(),
            Credentials::bearer("token").unwrap(),
        )
        .terminal_policy(TerminalPolicy {
            strategy: TerminalStrategy::InBand,
            ..Default::default()
        }),
    )
    .unwrap();
    assert_eq!(
        client
            .execute_command(Query::new("CREATE DATABASE forbidden", "command").unwrap())
            .await
            .unwrap_err(),
        antalya_rs::Error::Unsupported
    );
    assert!(
        Query::new("SELECT 1", "setting")
            .unwrap()
            .setting("framing_output_format", "None")
            .is_err()
    );
}

#[tokio::test]
async fn regression_delayed_in_band_confirmation_respects_original_deadline() {
    let (client, worker) = server(packet_stream(0, 0), "late-confirm", TYPE, false);
    let mut e = client
        .fetch_arrow(Query::new("SELECT v", "late-confirm").unwrap())
        .await
        .unwrap();
    assert!(e.next_provisional_batch().await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let error = e.confirm_completion().await.unwrap_err();
    assert_eq!(error.kind(), antalya_rs::ErrorKind::Timeout);
    assert_ne!(e.outcome(), Outcome::Succeeded);
    worker.join().unwrap();
}
