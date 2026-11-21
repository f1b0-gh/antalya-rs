#![cfg(feature = "arrow")]
use antalya_rs::{
    AntalyaClient, Config, Credentials, Endpoint, ExportOptions, Format, Outcome, Overwrite, Query,
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    time::Duration,
};
fn server(body: Vec<u8>) -> (AntalyaClient, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let task = std::thread::spawn(move || {
        for (i, connection) in listener.incoming().take(3).enumerate() {
            let mut socket = connection.unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut request = vec![0; 65536];
            let n = socket.read(&mut request).unwrap();
            assert!(n > 0);
            let response = if i == 0 {
                body.clone()
            } else if i == 1 {
                Vec::new()
            } else {
                br#"{"data":[{"starts":0,"finishes":0,"errors":0,"cancelled":0,"result_rows":"0","read_rows":"0","read_bytes":"0"}]}"#.to_vec()
            };
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            )
            .unwrap();
            socket.write_all(&response).unwrap();
        }
    });
    (
        AntalyaClient::new(
            Config::new(endpoint, Credentials::bearer("token").unwrap())
                .timeout(Duration::from_secs(3)),
        )
        .unwrap(),
        task,
    )
}
fn stream() -> Vec<u8> {
    let schema = arrow_schema::Schema::empty();
    let mut bytes = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
    writer.finish().unwrap();
    drop(writer);
    bytes
}
#[tokio::test]
async fn valid_ipc_and_eof_without_terminal_evidence_remain_unknown() {
    let (client, task) = server(stream());
    let mut execution = client
        .fetch_arrow(Query::new("SELECT 1", "unknown-1").unwrap())
        .await
        .unwrap();
    assert!(execution.next_batch().await.is_err());
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert!(execution.schema().is_some());
    assert_eq!(execution.query_id(), "unknown-1");
    task.join().unwrap();
}
#[tokio::test]
async fn unknown_export_never_overwrites_existing_destination() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("data.arrow");
    std::fs::write(&destination, b"previous").unwrap();
    let (client, task) = server(stream());
    let result = client
        .export(
            Query::new("SELECT 1", "export-1").unwrap(),
            Format::ArrowStream,
            &destination,
            ExportOptions {
                overwrite: Overwrite::Replace,
                max_bytes: 1_000_000,
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(std::fs::read(destination).unwrap(), b"previous");
    task.join().unwrap();
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}
