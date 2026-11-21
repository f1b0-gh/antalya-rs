#![cfg(feature = "arrow")]

use antalya_rs::{
    AntalyaClient, Config, Credentials, Endpoint, ErrorKind, ExportOptions, Format, Query,
};
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn export_enforces_default_cumulative_row_limit_before_publication() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1; 1_000_001]))],
    )
    .unwrap();
    let mut body = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = stop.clone();
    let task = std::thread::spawn(move || {
        let mut index = 0;
        while !server_stop.load(Ordering::Acquire) {
            let (mut socket, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(error) => panic!("fixture accept failed: {error}"),
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut request = [0; 16384];
            assert!(socket.read(&mut request).unwrap() > 0);
            let response = match index {
                0 => body.as_slice(),
                1 => b"",
                _ => br#"{"data":[{"starts":"1","finishes":"1","errors":"0","cancelled":"0","result_rows":"1000001","read_rows":"1000001","read_bytes":"4000004"}]}"#,
            };
            let _ = write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            );
            let _ = socket.write_all(response);
            index += 1;
        }
    });
    let client = AntalyaClient::new(
        Config::new(endpoint, Credentials::bearer("token").unwrap())
            .timeout(Duration::from_secs(3)),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("result.arrow");
    let result = client
        .export(
            Query::new("SELECT 1", "export-row-limit").unwrap(),
            Format::ArrowStream,
            &destination,
            ExportOptions::default(),
        )
        .await;
    stop.store(true, Ordering::Release);
    task.join().unwrap();
    assert!(
        result
            .as_ref()
            .is_err_and(|error| error.kind() == ErrorKind::LimitExceeded),
        "row-limited export unexpectedly succeeded: {result:?}"
    );
    assert!(!destination.exists());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn aborted_export_releases_shared_admission_and_cleans_temporary_storage() {
    use antalya_rs::{Cause, Phase, ResourceLimits};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let (seen, request_seen) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut request = [0; 16384];
        assert!(socket.read(&mut request).unwrap() > 0);
        let _ = seen.send(());
        released.recv().unwrap();
    });
    let client = AntalyaClient::new(
        Config::new(endpoint, Credentials::bearer("token").unwrap())
            .resources(ResourceLimits {
                max_export_tasks: 1,
                max_queued_queries: 0,
                ..ResourceLimits::default()
            })
            .timeout(Duration::from_secs(3)),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("partial");
    let existing = directory.path().join("existing");
    std::fs::write(&existing, b"previous").unwrap();
    let first_client = client.clone();
    let first = tokio::spawn(async move {
        first_client
            .export(
                Query::new("SELECT 1", "export-aborted").unwrap(),
                Format::ArrowStream,
                destination,
                ExportOptions::default(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), request_seen)
        .await
        .unwrap()
        .unwrap();
    let refused = client
        .export(
            Query::new("SELECT 1", "export-refused").unwrap(),
            Format::ArrowStream,
            &existing,
            ExportOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(refused.phase(), Some(Phase::Queue));
    assert_eq!(refused.cause(), Some(Cause::Capacity));
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    server.join().unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let refused = client
                .export(
                    Query::new("SELECT 1", "export-released").unwrap(),
                    Format::ArrowStream,
                    &existing,
                    ExportOptions::default(),
                )
                .await
                .unwrap_err();
            if refused.kind() == ErrorKind::Publication {
                break;
            }
            assert_eq!(refused.cause(), Some(Cause::Capacity));
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(&existing).unwrap(), b"previous");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
