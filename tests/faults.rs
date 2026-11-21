#![cfg(feature = "arrow")]
use antalya_rs::{
    AntalyaClient, Config, Credentials, Endpoint, Error, ErrorKind, ExportOptions, Format, Outcome,
    Overwrite, Query,
};
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::Arc,
    time::Duration,
};

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![7, 8]))],
    )
    .unwrap()
}
fn ipc(format: Format) -> Vec<u8> {
    let batch = batch();
    let mut bytes = Vec::new();
    if format == Format::Arrow {
        let mut writer =
            arrow_ipc::writer::FileWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    } else {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    bytes
}
fn fixture(
    body: Vec<u8>,
    count: usize,
    terminal: &str,
    truncated_http: bool,
) -> (AntalyaClient, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let terminal = terminal.as_bytes().to_vec();
    let task = std::thread::spawn(move || {
        let mut execution_marker = String::new();
        for (index, connection) in listener.incoming().take(count).enumerate() {
            let mut socket = connection.unwrap();
            socket.set_nodelay(true).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut request = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let size = socket.read(&mut chunk).unwrap();
                assert_ne!(size, 0);
                request.extend_from_slice(&chunk[..size]);
                if let Some(end) = request.windows(4).position(|x| x == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|x| x.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let request = String::from_utf8(request).unwrap();
            let lower = request.to_ascii_lowercase();
            assert!(lower.contains("authorization: bearer token\r\n"));
            assert!(!lower.contains("x-clickhouse-user"));
            assert!(!lower.contains("x-clickhouse-key"));
            let target = request
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap();
            let url = reqwest::Url::parse(&format!("http://localhost{target}")).unwrap();
            if index == 0 {
                execution_marker = url
                    .query_pairs()
                    .find(|(key, _)| key == "log_comment")
                    .unwrap()
                    .1
                    .into_owned();
                assert!(!execution_marker.is_empty());
            }
            if index == 2 {
                assert_eq!(
                    url.query_pairs()
                        .find(|(key, _)| key == "param_nonce")
                        .unwrap()
                        .1,
                    execution_marker
                );
                assert!(request.contains("log_comment={nonce:String}"));
            }
            let response = if index == 0 {
                &body
            } else if index == 1 {
                &Vec::new()
            } else {
                &terminal
            };
            if index == 0 && !truncated_http {
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                // Actual HTTP framing splits exception text independently of TCP.
                for chunk in response.chunks(7) {
                    if write!(socket, "{:x}\r\n", chunk.len()).is_err()
                        || socket.write_all(chunk).is_err()
                        || socket.write_all(b"\r\n").is_err()
                    {
                        break;
                    }
                }
                let _ = socket.write_all(b"0\r\n\r\n");
            } else {
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.len() + if index == 0 && truncated_http { 10 } else { 0 }
                )
                .unwrap();
                let _ = socket.write_all(response);
            }
        }
    });
    (
        AntalyaClient::new(
            Config::new(endpoint, Credentials::bearer("token").unwrap())
                .timeout(Duration::from_secs(30)),
        )
        .unwrap(),
        task,
    )
}
const SUCCESS: &str = r#"{"data":[{"starts":"1","finishes":"1","errors":"0","cancelled":"0","result_rows":"2","read_rows":"2","read_bytes":"8"}]}"#;
const UNKNOWN: &str = r#"{"data":[{"starts":"0","finishes":"0","errors":"0","cancelled":"0","result_rows":"0","read_rows":"0","read_bytes":"0"}]}"#;
const FAILED: &str = r#"{"data":[{"starts":"1","finishes":"0","errors":"1","cancelled":"0","result_rows":"2","read_rows":"2","read_bytes":"8"}]}"#;
const CANCELLED: &str = r#"{"data":[{"starts":"1","finishes":"0","errors":"1","cancelled":"1","result_rows":"2","read_rows":"2","read_bytes":"8"}]}"#;
#[tokio::test]
async fn valid_exports_publish_both_encodings_after_matching_terminal_evidence() {
    for format in [Format::Arrow, Format::ArrowStream] {
        let body = ipc(format);
        let (client, task) = fixture(body.clone(), 3, SUCCESS, false);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("result");
        let report = client
            .export(
                Query::new("SELECT 1", "success-1").unwrap(),
                format,
                &path,
                ExportOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            (report.rows, report.batches, report.outcome),
            (2, 1, Outcome::Succeeded)
        );
        assert_eq!(std::fs::read(&path).unwrap(), body);
        task.join().unwrap();
    }
}
#[tokio::test]
async fn late_split_exception_remains_visible_after_delivered_batch() {
    let mut body = ipc(Format::ArrowStream);
    body.truncate(body.len() - 8);
    body.extend_from_slice(
        b"Code: 395. DB::Exception: controlled late failure (QUERY_WAS_CANCELLED)",
    );
    let (client, task) = fixture(body, 3, FAILED, false);
    let mut execution = client
        .fetch_arrow(Query::new("SELECT 1", "late-1").unwrap())
        .await
        .unwrap();
    assert_eq!(execution.next().await.unwrap().unwrap().num_rows(), 2);
    assert!(execution.next().await.is_err());
    assert_eq!(execution.outcome(), Outcome::Failed);
    assert_eq!(execution.rows_delivered(), 2);
    assert!(execution.next().await.is_err());
    task.join().unwrap();
}
#[tokio::test]
async fn malformed_and_premature_http_eof_never_claim_success() {
    for (body, truncated) in [
        (b"not IPC".to_vec(), false),
        (ipc(Format::ArrowStream), true),
    ] {
        let (client, task) = fixture(body, 3, UNKNOWN, truncated);
        let mut execution = client
            .fetch_arrow(Query::new("SELECT 1", "bad-1").unwrap())
            .await
            .unwrap();
        loop {
            match execution.next().await {
                Ok(Some(_)) => continue,
                Err(_) => break,
                Ok(None) => panic!("incomplete execution succeeded"),
            }
        }
        assert_eq!(execution.outcome(), Outcome::Unknown);
        task.join().unwrap();
    }
}
#[tokio::test]
async fn invalid_truncated_and_trailing_ipc_preserve_existing_destination() {
    for format in [Format::Arrow, Format::ArrowStream] {
        let mut truncated = ipc(format);
        truncated.truncate(truncated.len() - 8);
        let mut trailing = ipc(format);
        trailing.extend_from_slice(b"late server error");
        for body in [truncated, trailing, b"bad schema".to_vec()] {
            let (client, task) = fixture(body, 1, SUCCESS, false);
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("result");
            std::fs::write(&path, b"existing").unwrap();
            assert!(
                client
                    .export(
                        Query::new("SELECT 1", "bad-export").unwrap(),
                        format,
                        &path,
                        ExportOptions {
                            overwrite: Overwrite::Replace,
                            max_bytes: 4096
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(std::fs::read(path).unwrap(), b"existing");
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
            task.join().unwrap();
        }
    }
}
#[tokio::test]
async fn failed_cancelled_and_duplicate_terminal_records_never_publish() {
    let duplicate = r#"{"data":[{"starts":"2","finishes":"1","errors":"0","cancelled":"0","result_rows":"2","read_rows":"2","read_bytes":"8"}]}"#;
    for terminal in [FAILED, CANCELLED, duplicate] {
        let (client, task) = fixture(ipc(Format::ArrowStream), 3, terminal, false);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("result");
        assert!(
            client
                .export(
                    Query::new("SELECT 1", "terminal-1").unwrap(),
                    Format::ArrowStream,
                    &path,
                    ExportOptions::default()
                )
                .await
                .is_err()
        );
        assert!(!path.exists());
        task.join().unwrap();
    }
}
#[tokio::test]
async fn overflowing_terminal_counters_fail_without_panicking() {
    let corrupt = r#"{"data":[{"starts":"1","finishes":"18446744073709551615","errors":"2","cancelled":"0","result_rows":"2","read_rows":"2","read_bytes":"8"}]}"#;
    let (client, task) = fixture(ipc(Format::ArrowStream), 3, corrupt, false);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("result");
    let result = client
        .export(
            Query::new("SELECT 1", "overflow").unwrap(),
            Format::ArrowStream,
            &path,
            ExportOptions::default(),
        )
        .await;
    assert!(matches!(result, Err(error) if error.kind() == ErrorKind::Unconfirmed));
    assert!(!path.exists());
    task.join().unwrap();
}
#[tokio::test]
async fn bounded_export_and_no_overwrite_are_enforced() {
    let (client, task) = fixture(ipc(Format::ArrowStream), 1, SUCCESS, false);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("result");
    assert!(matches!(
        client
            .export(
                Query::new("SELECT 1", "limit-1").unwrap(),
                Format::ArrowStream,
                &path,
                ExportOptions {
                    overwrite: Overwrite::Forbid,
                    max_bytes: 1
                }
            )
            .await,
        Err(error) if error.kind() == ErrorKind::LimitExceeded
    ));
    task.join().unwrap();
    assert!(!path.exists());
    std::fs::write(&path, b"existing").unwrap();
    assert!(matches!(
        client
            .export(
                Query::new("SELECT 1", "existing-1").unwrap(),
                Format::ArrowStream,
                &path,
                ExportOptions::default()
            )
            .await,
        Err(error) if error.kind() == ErrorKind::Publication
    ));
    assert_eq!(std::fs::read(path).unwrap(), b"existing");
}
#[tokio::test]
async fn compressed_ipc_export_uses_arrow_supported_codecs() {
    for compression in [
        arrow_ipc::CompressionType::LZ4_FRAME,
        arrow_ipc::CompressionType::ZSTD,
    ] {
        let batch = batch();
        let mut body = Vec::new();
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(compression))
            .unwrap();
        let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
            &mut body,
            &batch.schema(),
            options,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let (client, task) = fixture(body, 3, SUCCESS, false);
        let dir = tempfile::tempdir().unwrap();
        let report = client
            .export(
                Query::new("SELECT 1", "compressed-1").unwrap(),
                Format::Arrow,
                dir.path().join("result"),
                ExportOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(report.rows, 2);
        task.join().unwrap();
    }
}
#[tokio::test]
async fn empty_kill_ack_without_terminal_server_remains_unknown() -> Result<(), Error> {
    let (client, task) = fixture(ipc(Format::ArrowStream), 2, SUCCESS, false);
    let mut execution = client
        .fetch_arrow(Query::new("SELECT 1", "cancel-unknown")?)
        .await?;
    assert!(execution.next().await?.is_some());
    // Empty KILL acknowledgement is valid, but this fixture closes before
    // terminal observation. The transport failure must preserve uncertainty.
    let result = execution.cancel().await;
    assert!(matches!(result, Err(ref error)
        if error.phase() == Some(antalya_rs::Phase::Control)
        && matches!(error.cause(), Some(antalya_rs::Cause::Transport(_)))));
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert!(execution.next().await.is_err());
    assert!(task.join().is_ok());
    Ok(())
}
#[tokio::test]
async fn readonly_completion_polls_after_flush_is_denied() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let body = ipc(Format::ArrowStream);
    let task = std::thread::spawn(move || {
        for (i, stream) in listener.incoming().take(4).enumerate() {
            let mut stream = stream.unwrap();
            let mut request = [0; 16384];
            assert!(stream.read(&mut request).unwrap() > 0);
            let bytes = match i {
                0 => body.as_slice(),
                1 => b"permission denied",
                2 => UNKNOWN.as_bytes(),
                _ => SUCCESS.as_bytes(),
            };
            write!(
                stream,
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                if i == 1 { "403 Forbidden" } else { "200 OK" },
                bytes.len()
            )
            .unwrap();
            stream.write_all(bytes).unwrap();
        }
    });
    let client = AntalyaClient::new(
        Config::new(endpoint, Credentials::bearer("token").unwrap())
            .timeout(Duration::from_secs(3)),
    )
    .unwrap();
    let mut execution = client
        .fetch_arrow(Query::new("SELECT 1", "readonly-1").unwrap())
        .await
        .unwrap();
    assert_eq!(execution.next().await.unwrap().unwrap().num_rows(), 2);
    assert!(execution.next().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    task.join().unwrap();
}
#[tokio::test]
async fn authorized_execution_rejects_target_format_and_database_changes_before_network() {
    use antalya_rs::{AuthorizationContext, AuthorizedRequest};
    let endpoint = Endpoint::parse("http://127.0.0.1:1").unwrap();
    for (target, format, database) in [
        ("http://127.0.0.1:2", Format::ArrowStream, None),
        ("http://127.0.0.1:1", Format::Json, None),
        ("http://127.0.0.1:1", Format::ArrowStream, Some("newdb")),
    ] {
        let mut config = Config::new(endpoint.clone(), Credentials::bearer("token").unwrap());
        if let Some(db) = database {
            config = config.database(db).unwrap();
        }
        let client = AntalyaClient::new(config).unwrap();
        let request = AuthorizedRequest::new(
            Query::new("SELECT 1", "frozen-1").unwrap(),
            Endpoint::parse(target).unwrap(),
            format,
            AuthorizationContext {
                receipt_id: "receipt".into(),
                expected_principal: "reader".into(),
            },
        )
        .unwrap();
        assert!(matches!(
            client.fetch_authorized_arrow(request).await,
            Err(Error::InvalidRequest)
        ));
    }
}
#[tokio::test]
async fn upstream_database_is_resolved_once_without_duplicate_url_keys() {
    for database in [None, Some("override")] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint =
            Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let task = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0; 8192];
            let n = stream.read(&mut buf).unwrap();
            let text = std::str::from_utf8(&buf[..n]).unwrap();
            let target = text
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap();
            let url = reqwest::Url::parse(&format!("http://localhost{target}")).unwrap();
            let values: Vec<_> = url
                .query_pairs()
                .filter(|(k, _)| k == "database")
                .map(|(_, v)| v.into_owned())
                .collect();
            assert_eq!(values, vec![database.unwrap_or("defaultdb")]);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
        });
        let client = AntalyaClient::new(
            Config::new(endpoint, Credentials::bearer("token").unwrap())
                .database("defaultdb")
                .unwrap(),
        )
        .unwrap();
        let mut query = Query::new("SELECT 1", "database-1").unwrap();
        if let Some(db) = database {
            query = query.database(db).unwrap();
        }
        let mut cursor = client.fetch_bytes(&query, Format::Json).unwrap();
        let _ = cursor.next().await;
        task.join().unwrap();
    }
}
#[tokio::test]
async fn identity_and_data_share_one_operation_deadline() {
    use antalya_rs::{AuthorizationContext, AuthorizedRequest};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let target = endpoint.clone();
    let task = std::thread::spawn(move || {
        for (i, connection) in listener.incoming().take(2).enumerate() {
            let mut stream = connection.unwrap();
            let mut request = [0; 16384];
            assert!(stream.read(&mut request).unwrap() > 0);
            std::thread::sleep(Duration::from_millis(120));
            let response = if i == 0 {
                br#"{"data":[{"principal":"reader"}]}"#.to_vec()
            } else {
                ipc(Format::ArrowStream)
            };
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            );
            let _ = stream.write_all(&response);
        }
    });
    let client = AntalyaClient::new(
        Config::new(endpoint, Credentials::bearer("token").unwrap())
            .timeout(Duration::from_millis(200)),
    )
    .unwrap();
    let request = AuthorizedRequest::new(
        Query::new("SELECT 1", "deadline-1").unwrap(),
        target,
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "receipt".into(),
            expected_principal: "reader".into(),
        },
    )
    .unwrap();
    let mut execution = client.fetch_authorized_arrow(request).await.unwrap();
    assert!(matches!(execution.next().await, Err(error) if error.kind() == ErrorKind::Timeout));
    assert_eq!(execution.outcome(), Outcome::Unknown);
    task.join().unwrap();
}
#[tokio::test]
async fn query_finish_cannot_validate_missing_schema_or_lost_whole_batches() {
    let mut boundary = ipc(Format::ArrowStream);
    boundary.truncate(boundary.len() - 8);
    let four_rows = r#"{"data":[{"starts":"1","finishes":"1","errors":"0","cancelled":"0","result_rows":"4","read_rows":"4","read_bytes":"16"}]}"#;
    for body in [Vec::new(), boundary] {
        let (client, task) = fixture(body, 3, four_rows, false);
        let mut execution = client
            .fetch_arrow(Query::new("SELECT 1", "lost-batch-1").unwrap())
            .await
            .unwrap();
        loop {
            match execution.next().await {
                Ok(Some(_)) => continue,
                Err(_) => break,
                Ok(None) => panic!("incomplete response reported success"),
            }
        }
        assert_eq!(execution.outcome(), Outcome::Unknown);
        task.join().unwrap();
    }
}
#[tokio::test]
async fn cancellation_race_cannot_turn_undrained_results_into_success() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let task = std::thread::spawn(move || {
        for (i, connection) in listener.incoming().take(4).enumerate() {
            let mut stream = connection.unwrap();
            let mut request = [0; 16384];
            assert!(stream.read(&mut request).unwrap() > 0);
            let response = match i {
                0 => ipc(Format::ArrowStream),
                1 => br#"{"data":[]}"#.to_vec(),
                2 => Vec::new(),
                _ => SUCCESS.as_bytes().to_vec(),
            };
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            )
            .unwrap();
            stream.write_all(&response).unwrap();
        }
    });
    let client =
        AntalyaClient::new(Config::new(endpoint, Credentials::bearer("token").unwrap())).unwrap();
    let mut execution = client
        .fetch_arrow(Query::new("SELECT 1", "cancel-race").unwrap())
        .await
        .unwrap();
    assert!(execution.next().await.unwrap().is_some());
    assert_eq!(
        execution.cancel().await.unwrap_err().kind(),
        ErrorKind::Unconfirmed
    );
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert!(execution.next().await.is_err());
    task.join().unwrap();
}
#[tokio::test]
async fn command_transport_errors_require_terminal_evidence() {
    for terminal in [SUCCESS, FAILED, UNKNOWN] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint =
            Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let task = std::thread::spawn(move || {
            for (i, connection) in listener.incoming().take(3).enumerate() {
                let mut stream = connection.unwrap();
                let mut request = [0; 16384];
                let n = stream.read(&mut request).unwrap();
                assert!(n > 0);
                if i == 0 {
                    assert!(!String::from_utf8_lossy(&request[..n]).contains("FORMAT"));
                }
                let response = match i {
                    0 => b"intermediary disconnected".as_slice(),
                    1 => b"".as_slice(),
                    _ => terminal.as_bytes(),
                };
                write!(
                    stream,
                    "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    if i == 0 { "502 Bad Gateway" } else { "200 OK" },
                    response.len()
                )
                .unwrap();
                stream.write_all(response).unwrap();
            }
        });
        let client = AntalyaClient::new(
            Config::new(endpoint, Credentials::bearer("token").unwrap())
                .timeout(Duration::from_secs(2)),
        )
        .unwrap();
        let outcome = client
            .execute_command(Query::new("CREATE DATABASE demo", "command-1").unwrap())
            .await
            .unwrap();
        assert_eq!(
            outcome,
            if terminal == SUCCESS {
                Outcome::Succeeded
            } else if terminal == FAILED {
                Outcome::Failed
            } else {
                Outcome::Unknown
            }
        );
        task.join().unwrap();
    }
}

#[tokio::test]
async fn impossible_cancelled_counter_never_proves_success() {
    let corrupt = r#"{"data":[{"starts":1,"finishes":1,"errors":0,"cancelled":1,"result_rows":2,"read_rows":2,"read_bytes":8}]}"#;
    let (client, task) = fixture(ipc(Format::ArrowStream), 3, corrupt, false);
    let mut execution = client
        .fetch_arrow(Query::new("SELECT 1", "impossible-cancelled").unwrap())
        .await
        .unwrap();
    drop(execution.next_batch().await.unwrap());
    assert_eq!(
        execution.next_batch().await.unwrap_err().kind(),
        ErrorKind::Unconfirmed
    );
    assert_eq!(execution.outcome(), Outcome::Unknown);
    task.join().unwrap();
}

#[tokio::test]
async fn export_preserves_malformed_control_response_cause() {
    let (client, task) = fixture(ipc(Format::ArrowStream), 3, "{malformed", false);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("result");
    let error = client
        .export(
            Query::new("SELECT 1", "export-control-cause").unwrap(),
            Format::ArrowStream,
            &path,
            ExportOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.phase(), Some(antalya_rs::Phase::Control));
    assert_eq!(error.cause(), Some(antalya_rs::Cause::JsonDecoder));
    assert!(!path.exists());
    task.join().unwrap();
}
