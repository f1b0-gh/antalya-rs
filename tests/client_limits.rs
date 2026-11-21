#![cfg(feature = "arrow")]
use antalya_rs::{AntalyaClient, Config, Credentials, Endpoint, Query};
use arrow_array::{Int32Array, RecordBatch};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::Arc,
    time::Duration,
};

#[tokio::test]
async fn default_execution_rejects_excess_rows_before_delivery() {
    let batch = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(Int32Array::from(vec![1; 1_000_001])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let mut body = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let task = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut bytes = [0; 8192];
        let read = socket.read(&mut bytes).unwrap();
        assert!(read > 0);
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        let _ = socket.write_all(&body);
    });
    let client =
        AntalyaClient::new(Config::new(endpoint, Credentials::bearer("token").unwrap())).unwrap();
    let mut execution = client
        .fetch_arrow(Query::new("SELECT v", "row-limit").unwrap())
        .await
        .unwrap();
    assert!(
        execution.next_batch().await.is_err(),
        "oversized result must not be delivered"
    );
    task.join().unwrap();
}
