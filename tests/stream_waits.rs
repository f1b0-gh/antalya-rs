#![cfg(feature = "arrow")]
mod support;

use antalya_rs::{
    AntalyaClient, Cause, Config, Credentials, Endpoint, ErrorKind, ExecutionLimits, Outcome,
    Query, ResourceLimits, TerminalPolicy, TerminalStrategy,
};
use std::time::Duration;
use support::{Server, stream};

fn query() -> Query {
    Query::new("SELECT v", uuid::Uuid::new_v4().to_string()).unwrap()
}

fn config(server: &Server) -> Config {
    Config::new(
        server.endpoint.clone(),
        Credentials::bearer("token").unwrap(),
    )
    .timeout(Duration::from_secs(2))
}

fn bounded(config: Config) -> Config {
    config
        .resources(ResourceLimits {
            max_retained_bytes: 64 * 1024,
            ..ResourceLimits::default()
        })
        .execution_limits(ExecutionLimits {
            max_batch_bytes: 64 * 1024,
            ..ExecutionLimits::default()
        })
}

#[tokio::test]
async fn retained_output_reports_capacity_wait_expiry_without_partial_claims() {
    for wait in [Duration::ZERO, Duration::from_millis(30)] {
        let server = Server::new(stream(1), 2, false, 200);
        let client =
            AntalyaClient::new(bounded(config(&server)).capacity_wait_timeout(wait)).unwrap();
        let mut first = client.fetch_arrow(query()).await.unwrap();
        let retained = first.next_batch().await.unwrap().unwrap();
        drop(first);
        let mut second = client.fetch_arrow(query()).await.unwrap();
        let error = tokio::time::timeout(Duration::from_millis(500), second.next_batch())
            .await
            .expect("configured capacity wait must end before the total deadline")
            .unwrap_err();
        assert_eq!(error.cause(), Some(Cause::CapacityTimeout));
        assert_eq!(error.kind(), ErrorKind::LimitExceeded);
        assert_eq!(second.outcome(), Outcome::Unknown);
        assert_eq!(
            server.requests.lock().unwrap().len(),
            1,
            "capacity refusal must precede dispatch"
        );
        drop(retained);
        let mut third = client.fetch_arrow(query()).await.unwrap();
        assert_eq!(third.next_batch().await.unwrap().unwrap().num_rows(), 2);
    }
}

struct StalledServer {
    endpoint: Endpoint,
    stop: std::sync::mpsc::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl StalledServer {
    fn new() -> Self {
        Self::profile(false, 200)
    }

    fn profile(identity: bool, status: u16) -> Self {
        Self::with_prefix(identity, status, false)
    }

    fn with_prefix(identity: bool, status: u16, prefix: bool) -> Self {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint =
            Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let (stop, stopped) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            for request_index in 0..=usize::from(identity) {
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if stopped.recv_timeout(Duration::from_millis(5)).is_ok() {
                                return;
                            }
                        }
                        Err(error) => panic!("accept: {error}"),
                    }
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 4096];
                loop {
                    let n = socket.read(&mut chunk).unwrap();
                    if n == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let size = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|value| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + size {
                            break;
                        }
                    }
                }
                if identity && request_index == 0 {
                    let body = br#"{"data":[{"principal":"fixture"}]}"#;
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .unwrap();
                    socket.write_all(body).unwrap();
                    continue;
                }
                let _ = write!(
                    socket,
                    "HTTP/1.1 {status} OK\r\nContent-Length: 10000\r\nContent-Type: application/x-ndjson; payload=base64\r\nX-ClickHouse-Query-Id: wait-stall\r\nConnection: close\r\n\r\n"
                );
                if prefix {
                    for byte in b"Code: 164." {
                        if socket.write_all(&[*byte]).is_err() {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(30));
                    }
                }
                // Keep the body pending until this test drops its owned fixture.
                let _ = stopped.recv_timeout(Duration::from_secs(3));
            }
        });
        Self {
            endpoint,
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for StalledServer {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

#[tokio::test]
async fn cancelled_plain_and_framed_reads_preserve_inactivity_deadlines() {
    for (strategy, status) in [
        (TerminalStrategy::ObserveOnly, 200),
        (TerminalStrategy::InBand, 200),
        (TerminalStrategy::InBand, 500),
    ] {
        let server = StalledServer::profile(false, status);
        let client = AntalyaClient::new(
            Config::new(
                server.endpoint.clone(),
                Credentials::bearer("token").unwrap(),
            )
            .timeout(Duration::from_secs(2))
            .read_timeout(Duration::from_millis(100))
            .terminal_policy(TerminalPolicy {
                strategy,
                ..TerminalPolicy::default()
            }),
        )
        .unwrap();
        let mut execution = client
            .fetch_arrow(Query::new("SELECT v", "wait-stall").unwrap())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), execution.next_batch())
                .await
                .is_err()
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
        let error = tokio::time::timeout(Duration::from_millis(50), execution.next_batch())
            .await
            .expect("cancelled read must keep its original deadline")
            .unwrap_err();
        assert_eq!(error.cause(), Some(Cause::ReadTimeout));
        assert_eq!(execution.outcome(), Outcome::Unknown);
    }
}

#[tokio::test]
async fn total_deadline_wins_when_shorter_than_network_inactivity() {
    let server = StalledServer::new();
    let client = AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("token").unwrap(),
        )
        .timeout(Duration::from_millis(30))
        .read_timeout(Duration::from_millis(500)),
    )
    .unwrap();
    let mut execution = client
        .fetch_arrow(Query::new("SELECT v", "wait-stall").unwrap())
        .await
        .unwrap();
    assert_eq!(
        execution.next_batch().await.unwrap_err().cause(),
        Some(Cause::Timeout)
    );
}

#[tokio::test]
async fn dropping_retained_output_allows_full_reservation_before_capacity_timeout() {
    let server = Server::new(stream(1), 2, false, 200);
    let client = AntalyaClient::new(
        bounded(config(&server)).capacity_wait_timeout(Duration::from_millis(500)),
    )
    .unwrap();
    let mut first = client.fetch_arrow(query()).await.unwrap();
    let retained = first.next_batch().await.unwrap().unwrap();
    drop(first);
    let mut second = client.fetch_arrow(query()).await.unwrap();
    let release = async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(retained);
    };
    let (batch, ()) = tokio::join!(second.next_batch(), release);
    assert_eq!(batch.unwrap().unwrap().num_rows(), 2);
}

#[tokio::test]
async fn slow_caller_does_not_consume_read_inactivity_budget() {
    let server = Server::new(stream(2), 4, false, 200);
    let client =
        AntalyaClient::new(config(&server).read_timeout(Duration::from_millis(30))).unwrap();
    let mut execution = client.fetch_arrow(query()).await.unwrap();
    drop(execution.next_batch().await.unwrap().unwrap());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(execution.next_batch().await.unwrap().unwrap().num_rows(), 2);
}

#[test]
fn wait_policies_validate_duration_bounds_without_rejecting_capacity_fail_fast() {
    let server = Server::new(stream(1), 2, false, 200);
    for invalid in [
        config(&server).read_timeout(Duration::ZERO),
        config(&server).read_timeout(Duration::MAX),
        config(&server).capacity_wait_timeout(Duration::MAX),
    ] {
        assert_eq!(
            AntalyaClient::new(invalid).unwrap_err().kind(),
            ErrorKind::InvalidSetting
        );
    }
    assert!(AntalyaClient::new(config(&server).capacity_wait_timeout(Duration::ZERO)).is_ok());
}

#[tokio::test]
async fn control_and_export_reads_obey_inactivity_policy() {
    // Successful control headers and an error body without a numeric header must
    // both stop waiting under the independent read deadline.
    for status in [200, 500] {
        let server = StalledServer::profile(false, status);
        let client = AntalyaClient::new(
            Config::new(
                server.endpoint.clone(),
                Credentials::bearer("token").unwrap(),
            )
            .timeout(Duration::from_secs(2))
            .read_timeout(Duration::from_millis(50)),
        )
        .unwrap();
        let error = tokio::time::timeout(Duration::from_millis(500), client.capabilities())
            .await
            .expect("control read must stop before the operation deadline")
            .unwrap_err();
        assert_eq!(error.cause(), Some(Cause::ReadTimeout));
    }
    let server = StalledServer::new();
    let client = AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("token").unwrap(),
        )
        .timeout(Duration::from_secs(2))
        .read_timeout(Duration::from_millis(50)),
    )
    .unwrap();
    std::fs::create_dir_all("/tmp/antalya-rs").unwrap();
    let directory = tempfile::tempdir_in("/tmp/antalya-rs").unwrap();
    let destination = directory.path().join("result.arrow");
    let error = tokio::time::timeout(
        Duration::from_millis(500),
        client.export(
            query(),
            antalya_rs::Format::ArrowStream,
            &destination,
            antalya_rs::ExportOptions::default(),
        ),
    )
    .await
    .expect("export read must stop before the operation deadline")
    .unwrap_err();
    assert_eq!(error.cause(), Some(Cause::ReadTimeout));
    assert!(!destination.exists());
}

#[tokio::test]
async fn admitted_read_keeps_inactivity_deadline_across_cancelled_poll() {
    let server = StalledServer::profile(true, 200);
    let client = AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("token").unwrap(),
        )
        .timeout(Duration::from_secs(2))
        .read_timeout(Duration::from_millis(100)),
    )
    .unwrap();
    let request = antalya_rs::AuthorizedRequest::new(
        query(),
        server.endpoint.clone(),
        antalya_rs::Format::ArrowStream,
        antalya_rs::AuthorizationContext {
            receipt_id: "read-wait".into(),
            expected_principal: "fixture".into(),
        },
    )
    .unwrap();
    let mut response = client
        .prepare_authorized_arrow(request)
        .await
        .unwrap()
        .ready_transport()
        .await
        .unwrap()
        .admit()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), response.next_chunk())
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    let error = tokio::time::timeout(Duration::from_millis(50), response.next_chunk())
        .await
        .expect("admitted read must retain its original inactivity deadline")
        .err()
        .expect("stalled response must fail");
    assert!(
        matches!(error, antalya_rs::TransportError::Client(error) if error.cause() == Some(Cause::ReadTimeout))
    );
}

#[tokio::test]
async fn error_prefix_progress_resets_read_timer_and_survives_cancelled_framed_poll() {
    for framed in [false, true] {
        let server = StalledServer::with_prefix(false, 500, true);
        let client = AntalyaClient::new(
            Config::new(
                server.endpoint.clone(),
                Credentials::bearer("token").unwrap(),
            )
            .timeout(Duration::from_secs(2))
            .read_timeout(Duration::from_millis(100))
            .terminal_policy(TerminalPolicy {
                strategy: TerminalStrategy::InBand,
                ..TerminalPolicy::default()
            }),
        )
        .unwrap();
        let error = if framed {
            let mut execution = client
                .fetch_arrow(Query::new("SELECT v", "wait-stall").unwrap())
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(35), execution.next_batch())
                    .await
                    .is_err()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
            execution.next_batch().await.unwrap_err()
        } else {
            client.capabilities().await.unwrap_err()
        };
        assert_eq!(error.cause(), Some(Cause::ServerResponse(Some(164))));
    }
}
