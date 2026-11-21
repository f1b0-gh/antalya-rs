#![cfg(feature = "arrow")]
mod support;

use antalya_rs::{AntalyaClient, Cause, Config, Credentials, Endpoint, Outcome, Query};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct RejectedServer {
    endpoint: Endpoint,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl RejectedServer {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint =
            Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let worker = std::thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                let (mut socket, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("accept: {error}"),
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut request = [0; 16384];
                let count = socket.read(&mut request).unwrap();
                let request = String::from_utf8_lossy(&request[..count]).into_owned();
                let control =
                    request.contains("system.query_log") || request.contains("SYSTEM FLUSH LOGS");
                recorded.lock().unwrap().push(request);
                let (status, body) = if control {
                    (
                        200,
                        r#"{"data":[{"starts":0,"finishes":0,"errors":0,"cancelled":0}]}"#,
                    )
                } else {
                    (
                        500,
                        "Code: 164. DB::Exception: Cannot modify 'log_comment' setting in readonly mode. (READONLY)",
                    )
                };
                let _ = write!(
                    socket,
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        Self {
            endpoint,
            requests,
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for RejectedServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
async fn readonly_setting_refusal_returns_without_query_log_polling() {
    let server = RejectedServer::new();
    let client = AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("fixture").unwrap(),
        )
        .timeout(Duration::from_millis(150)),
    )
    .unwrap();
    let mut execution = client
        .fetch_arrow(Query::new("SELECT 1", "readonly-refusal").unwrap())
        .await
        .unwrap();
    let error = execution.next_batch().await.unwrap_err();
    assert_eq!(error.cause(), Some(Cause::ServerResponse(Some(164))));
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert_eq!(execution.metrics().control_requests, 0);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn terminal_observation_only_flushes_query_log_and_prunes_old_partitions() {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let client = AntalyaClient::new(Config::new(
        server.endpoint.clone(),
        Credentials::bearer("fixture").unwrap(),
    ))
    .unwrap();
    let mut execution = client
        .fetch_arrow(Query::new("SELECT v", "bounded-observation").unwrap())
        .await
        .unwrap();
    drop(execution.next_batch().await.unwrap());
    assert!(execution.next_batch().await.unwrap().is_none());
    let requests = server.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|r| r.contains("SYSTEM FLUSH LOGS query_log"))
    );
    assert!(requests.iter().any(|r| r.contains("system.query_log")
        && r.contains("event_date")
        && r.contains("param_log_since=")));
}

#[tokio::test]
async fn historical_receipts_keep_their_original_partition_bound() {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let client = AntalyaClient::new(Config::new(
        server.endpoint.clone(),
        Credentials::bearer("fixture").unwrap(),
    ))
    .unwrap();
    let receipt = antalya_rs::RecoveryReceipt::restore(
        server.endpoint.clone(),
        "historical-query".into(),
        uuid::Uuid::new_v4().to_string(),
        "fixture".into(),
    )
    .unwrap()
    .with_log_since(1_600_000_000)
    .unwrap();
    assert_eq!(receipt.log_since(), Some(1_600_000_000));
    assert_eq!(
        client
            .observe_terminal(&receipt)
            .await
            .unwrap()
            .termination(),
        antalya_rs::RemoteTermination::Succeeded
    );
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.contains("param_log_since=1600000000"))
    );
    server.requests.lock().unwrap().clear();
    let legacy = antalya_rs::RecoveryReceipt::restore(
        server.endpoint.clone(),
        "historical-query".into(),
        receipt.marker().into(),
        "fixture".into(),
    )
    .unwrap();
    assert_eq!(legacy.log_since(), None);
    assert_eq!(
        client
            .observe_terminal(&legacy)
            .await
            .unwrap()
            .termination(),
        antalya_rs::RemoteTermination::Succeeded
    );
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| !r.contains("param_log_since="))
    );
    assert!(legacy.with_log_since(u64::MAX).is_err());
}

#[tokio::test]
async fn command_settings_refusal_retains_diagnostics_without_polling() {
    let server = RejectedServer::new();
    let client = AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("fixture").unwrap(),
        )
        .timeout(Duration::from_millis(150)),
    )
    .unwrap();
    let report = client
        .execute_command(Query::new("CREATE DATABASE refused", "command-refusal").unwrap())
        .await
        .unwrap();
    assert_eq!(report.outcome, Outcome::Unknown);
    assert_eq!(
        report.metrics.last_error.unwrap().cause(),
        Some(Cause::ServerResponse(Some(164)))
    );
    assert_eq!(report.metrics.control_requests, 0);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}
