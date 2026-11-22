#![cfg(feature = "arrow")]

use antalya_rs::{
    AntalyaClient, Cause, Config, Credentials, Endpoint, ExecutionLimits, Outcome, Phase, Query,
    ResourceLimits, TerminalPolicy,
};
use std::{
    future::Future,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};
use tokio::sync::oneshot;

const WAIT: Duration = Duration::from_secs(5);
const SUCCESS: &[u8] = br#"{"data":[{"starts":1,"finishes":1,"errors":0,"cancelled":0,"result_rows":2,"read_rows":2,"read_bytes":8}]}"#;

#[tokio::test(flavor = "current_thread")]
async fn cancelled_provisional_poll_resumes_without_losing_its_byte_lease() {
    let (data, mut gate) = Step::gated(RequestKind::Data, stream());
    let server = Server::new(vec![
        data,
        Step::new(RequestKind::Flush, []),
        Step::new(RequestKind::Observe, SUCCESS),
    ]);
    let client = server.client(WAIT);
    let mut execution = client
        .fetch_arrow(query("provisional-resume-data"))
        .await
        .unwrap();
    drop_at_request(execution.next_provisional_batch(), gate.entered()).await;
    assert_eq!(execution.outcome(), Outcome::Running);
    assert_eq!(execution.rows_delivered(), 0);
    gate.release();
    let batch = execution.next_provisional_batch().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert!(batch.charged_bytes() > 0 && batch.charged_bytes() <= 1024);
    drop(batch);
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Running);
    execution.confirm_completion().await.unwrap();
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    assert_eq!(server.requests.load(Ordering::Acquire), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_explicit_confirmation_resumes_without_promoting_preview() {
    let (observation, mut gate) = Step::gated(RequestKind::Observe, SUCCESS);
    let server = Server::new(vec![
        Step::new(RequestKind::Data, stream()),
        Step::new(RequestKind::Flush, []),
        observation,
        Step::new(RequestKind::Flush, []),
        Step::new(RequestKind::Observe, SUCCESS),
    ]);
    let client = server.client(WAIT);
    let mut execution = client
        .fetch_arrow(query("provisional-resume-proof"))
        .await
        .unwrap();
    drop(execution.next_provisional_batch().await.unwrap());
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    drop_at_request(execution.confirm_completion(), gate.entered()).await;
    assert_eq!(execution.outcome(), Outcome::Running);
    assert_eq!(execution.rows_delivered(), 2);
    gate.release();
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Running);
    execution.confirm_completion().await.unwrap();
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    assert_eq!(server.requests.load(Ordering::Acquire), 5);
}

#[tokio::test(flavor = "current_thread")]
async fn late_server_failure_never_confirms_complete_provisional_rows() {
    let failed = br#"{"data":[{"starts":1,"finishes":0,"errors":1,"cancelled":0,"result_rows":2,"read_rows":2,"read_bytes":8}]}"#;
    let server = Server::new(vec![
        Step::new(RequestKind::Data, stream()),
        Step::new(RequestKind::Flush, []),
        Step::new(RequestKind::Observe, failed),
    ]);
    let mut execution = server
        .client(WAIT)
        .fetch_arrow(query("provisional-late-failure"))
        .await
        .unwrap();
    drop(execution.next_provisional_batch().await.unwrap());
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Running);
    assert!(execution.confirm_completion().await.is_err());
    assert_eq!(execution.outcome(), Outcome::Failed);
    assert!(execution.next_provisional_batch().await.is_err());
}

#[derive(Debug, PartialEq, Eq)]
enum RequestKind {
    Data,
    Flush,
    Observe,
    Kill,
}
struct Step {
    kind: RequestKind,
    body: Vec<u8>,
    gate: Option<(oneshot::Sender<()>, mpsc::Receiver<()>)>,
}
impl Step {
    fn new(kind: RequestKind, body: impl Into<Vec<u8>>) -> Self {
        Self {
            kind,
            body: body.into(),
            gate: None,
        }
    }
    fn gated(kind: RequestKind, body: impl Into<Vec<u8>>) -> (Self, Gate) {
        let (seen, entered) = oneshot::channel();
        let (release, released) = mpsc::channel();
        (
            Self {
                kind,
                body: body.into(),
                gate: Some((seen, released)),
            },
            Gate {
                entered: Some(entered),
                release,
            },
        )
    }
}
struct Gate {
    entered: Option<oneshot::Receiver<()>>,
    release: mpsc::Sender<()>,
}
impl Gate {
    fn entered(&mut self) -> oneshot::Receiver<()> {
        self.entered.take().unwrap()
    }
    fn release(self) {
        self.release.send(()).unwrap();
    }
}
struct Server {
    endpoint: Endpoint,
    address: SocketAddr,
    requests: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Server {
    fn new(steps: Vec<Step>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = Endpoint::parse(&format!("http://{address}")).unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let thread = std::thread::spawn(move || {
            for step in steps {
                let (mut socket, _) = listener.accept().unwrap();
                if stopping.load(Ordering::Acquire) {
                    return;
                }
                socket.set_read_timeout(Some(WAIT)).unwrap();
                socket.set_write_timeout(Some(WAIT)).unwrap();
                let request = read_request(&mut socket);
                let kind = if request.contains("system.query_log") {
                    RequestKind::Observe
                } else if request.contains("SYSTEM FLUSH LOGS") {
                    RequestKind::Flush
                } else if request.contains("KILL QUERY") {
                    RequestKind::Kill
                } else {
                    RequestKind::Data
                };
                assert_eq!(kind, step.kind, "unexpected fixture request");
                observed.fetch_add(1, Ordering::AcqRel);
                if let Some((seen, release)) = step.gate {
                    let _ = seen.send(());
                    if release.recv_timeout(WAIT).is_err() {
                        return;
                    }
                }
                let _ = write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    step.body.len()
                );
                let _ = socket.write_all(&step.body);
            }
        });
        Self {
            endpoint,
            address,
            requests,
            stop,
            thread: Some(thread),
        }
    }
    fn client(&self, timeout: Duration) -> AntalyaClient {
        AntalyaClient::new(
            Config::new(self.endpoint.clone(), Credentials::bearer("token").unwrap())
                .timeout(timeout)
                .resources(ResourceLimits {
                    max_retained_bytes: 1024,
                    max_control_queries: 1,
                    max_queued_queries: 0,
                    ..ResourceLimits::default()
                })
                .execution_limits(ExecutionLimits {
                    max_batch_bytes: 1024,
                    ..ExecutionLimits::default()
                }),
        )
        .unwrap()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Wake a pending accept if an assertion failed before all scripted calls.
        let _ = TcpStream::connect_timeout(&self.address, Duration::from_millis(100));
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}
fn read_request(socket: &mut TcpStream) -> String {
    let mut request = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        let count = socket.read(&mut chunk).unwrap();
        assert_ne!(count, 0);
        request.extend_from_slice(&chunk[..count]);
        if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..end]);
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|value| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if request.len() >= end + 4 + length {
                return String::from_utf8(request).unwrap();
            }
        }
    }
}
async fn drop_at_request(future: impl Future, entered: oneshot::Receiver<()>) {
    tokio::time::timeout(WAIT, async {
        tokio::select! {
            _ = future => panic!("operation finished before fixture barrier"),
            result = entered => result.unwrap(),
        }
    })
    .await
    .unwrap();
}
fn query(id: &str) -> Query {
    Query::new("SELECT 1", id).unwrap()
}

fn stream() -> Vec<u8> {
    let batch = arrow_array::RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(arrow_array::Int32Array::from(vec![1, 2])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    bytes
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_data_poll_keeps_byte_reservation_until_execution_drop() {
    let (first_data, mut gate) = Step::gated(RequestKind::Data, stream());
    let server = Server::new(vec![first_data, Step::new(RequestKind::Data, stream())]);
    let client = server.client(Duration::from_secs(1));
    let mut first = client.fetch_arrow(query("retained-first")).await.unwrap();
    drop_at_request(first.next_batch(), gate.entered()).await;
    assert!(first.metrics().data_poll > Duration::ZERO);
    assert_eq!(first.outcome(), Outcome::Running);
    assert_eq!(first.rows_delivered(), 0);

    // A network wait could also be Pending. A typed Queue timeout independently
    // proves the first dropped poll still owns the entire shared byte budget.
    let mut second = client.fetch_arrow(query("retained-second")).await.unwrap();
    let queued_before = second.metrics().queued;
    {
        let mut waiting = Box::pin(second.next_batch());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
    }
    assert!(second.metrics().queued > queued_before);
    let error = tokio::time::timeout(WAIT, second.next_batch())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.phase(), Some(Phase::Queue));
    assert_eq!(error.cause(), Some(Cause::Timeout));
    assert_eq!(error.kind(), antalya_rs::ErrorKind::Timeout);
    assert_eq!(second.outcome(), Outcome::Unknown);
    assert_eq!(server.requests.load(Ordering::Acquire), 1);
    drop(first);
    gate.release();

    let mut third = client.fetch_arrow(query("retained-third")).await.unwrap();
    let batch = tokio::time::timeout(WAIT, third.next_batch())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert!(batch.charged_bytes() > 0 && batch.charged_bytes() <= 1024);
    assert_eq!(server.requests.load(Ordering::Acquire), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_data_poll_reuses_its_reservation_when_resumed() {
    let (first_data, mut gate) = Step::gated(RequestKind::Data, stream());
    let server = Server::new(vec![first_data]);
    let client = server.client(WAIT);
    let mut execution = client.fetch_arrow(query("resume-data")).await.unwrap();
    drop_at_request(execution.next_batch(), gate.entered()).await;
    let previous_poll = execution.metrics().data_poll;
    assert!(previous_poll > Duration::ZERO);
    gate.release();
    let batch = tokio::time::timeout(WAIT, execution.next_batch())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(execution.outcome(), Outcome::Running);
    assert_eq!(execution.rows_delivered(), 2);
    assert!(execution.metrics().data_poll > previous_poll);
    assert!(batch.charged_bytes() > 0 && batch.charged_bytes() <= 1024);
    assert_eq!(server.requests.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_terminal_poll_can_resume_after_cursor_eof() {
    let (observation, mut gate) = Step::gated(RequestKind::Observe, SUCCESS);
    let server = Server::new(vec![
        Step::new(RequestKind::Data, stream()),
        Step::new(RequestKind::Flush, []),
        observation,
        Step::new(RequestKind::Flush, []),
        Step::new(RequestKind::Observe, SUCCESS),
    ]);
    let client = server.client(WAIT);
    let mut execution = client.fetch_arrow(query("resume-terminal")).await.unwrap();
    drop(execution.next_batch().await.unwrap().unwrap());
    drop_at_request(execution.next_batch(), gate.entered()).await;
    assert_eq!(execution.outcome(), Outcome::Running);
    assert_eq!(execution.rows_delivered(), 2);
    let cancelled_metrics = execution.metrics();
    assert!(cancelled_metrics.terminal > Duration::ZERO);
    assert!(cancelled_metrics.observe_time > Duration::ZERO);
    assert!(cancelled_metrics.flush_time > Duration::ZERO);
    gate.release();
    assert!(
        tokio::time::timeout(WAIT, execution.next_batch())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    assert_eq!(execution.metrics().polls, 2);
    assert_eq!(execution.metrics().flush_attempts, 2);
    assert!(execution.metrics().terminal > cancelled_metrics.terminal);
    assert!(execution.metrics().observe_time > cancelled_metrics.observe_time);
    assert!(execution.metrics().flush_time > cancelled_metrics.flush_time);
    assert_eq!(server.requests.load(Ordering::Acquire), 5);
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_cancel_cannot_turn_discarded_cursor_into_delivered_success() {
    let (kill, mut gate) = Step::gated(RequestKind::Kill, br#"{"data":[]}"#);
    let server = Server::new(vec![
        Step::new(RequestKind::Data, stream()),
        kill,
        Step::new(RequestKind::Flush, []),
        Step::new(RequestKind::Observe, SUCCESS),
    ]);
    let client = server.client(WAIT);
    let mut execution = client.fetch_arrow(query("cancel-discarded")).await.unwrap();
    drop(execution.next_batch().await.unwrap().unwrap());
    // All delivered row counts match eventual success; the unread cursor was
    // nevertheless explicitly discarded by cancel before the KILL request.
    drop_at_request(execution.cancel(), gate.entered()).await;
    assert_eq!(execution.outcome(), Outcome::Running);
    gate.release();
    let error = tokio::time::timeout(WAIT, execution.next_batch())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.phase(), Some(Phase::Cancellation));
    assert_eq!(error.cause(), Some(Cause::InvalidProof));
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert_eq!(execution.next_batch().await.unwrap_err(), error);
    assert_eq!(execution.rows_delivered(), 2);
    assert_eq!(server.requests.load(Ordering::Acquire), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_terminal_poll_preserves_prior_decoder_failure() {
    let (observation, mut gate) = Step::gated(RequestKind::Observe, SUCCESS);
    let server = Server::new(vec![
        Step::new(RequestKind::Data, b"invalid IPC"),
        Step::new(RequestKind::Flush, []),
        observation,
        Step::new(RequestKind::Flush, []),
        Step::new(RequestKind::Observe, SUCCESS),
    ]);
    let client = server.client(WAIT);
    let mut execution = client
        .fetch_arrow(query("resume-decode-failure"))
        .await
        .unwrap();
    drop_at_request(execution.next_batch(), gate.entered()).await;
    gate.release();
    let error = tokio::time::timeout(WAIT, execution.next_batch())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.phase(), Some(Phase::Data));
    assert_eq!(error.cause(), Some(Cause::Decoder));
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert_eq!(execution.next_batch().await.unwrap_err(), error);
    assert_eq!(server.requests.load(Ordering::Acquire), 5);
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_timeout_retains_interrupted_backoff_duration() {
    let missing = br#"{"data":[{"starts":1,"finishes":0,"errors":0,"cancelled":0,"result_rows":0,"read_rows":0,"read_bytes":0}]}"#;
    let server = Server::new(vec![
        Step::new(RequestKind::Data, stream()),
        Step::new(RequestKind::Flush, []),
        Step::new(RequestKind::Observe, missing),
    ]);
    let client = AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("token").unwrap(),
        )
        .timeout(Duration::from_secs(1))
        .terminal_policy(TerminalPolicy {
            initial_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(2),
            ..TerminalPolicy::default()
        }),
    )
    .unwrap();
    let mut execution = client.fetch_arrow(query("timeout-backoff")).await.unwrap();
    drop(execution.next_batch().await.unwrap().unwrap());
    let error = execution.next_batch().await.unwrap_err();
    assert_eq!(error.cause(), Some(Cause::Timeout));
    assert_eq!(error.phase(), Some(Phase::Control));
    let metrics = execution.metrics();
    assert_eq!(metrics.poll_sleeps, 1);
    assert!(metrics.poll_wait > Duration::ZERO);
    assert!(metrics.terminal >= metrics.poll_wait);
    assert!(metrics.observe_time > Duration::ZERO);
    assert!(metrics.flush_time > Duration::ZERO);
    assert_eq!(execution.outcome(), Outcome::Unknown);
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_flush_records_elapsed_time_before_resuming() {
    let (flush, mut gate) = Step::gated(RequestKind::Flush, []);
    let server = Server::new(vec![
        Step::new(RequestKind::Data, stream()),
        flush,
        Step::new(RequestKind::Flush, []),
        Step::new(RequestKind::Observe, SUCCESS),
    ]);
    let client = server.client(WAIT);
    let mut execution = client
        .fetch_arrow(query("resume-flush-timing"))
        .await
        .unwrap();
    drop(execution.next_batch().await.unwrap().unwrap());
    drop_at_request(execution.next_batch(), gate.entered()).await;
    let interrupted = execution.metrics();
    assert_eq!(interrupted.flush_attempts, 1);
    assert_eq!(interrupted.polls, 0);
    assert!(interrupted.flush_time > Duration::ZERO);
    assert!(interrupted.terminal >= interrupted.flush_time);
    gate.release();
    assert!(execution.next_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    assert!(execution.metrics().flush_time > interrupted.flush_time);
    assert!(execution.metrics().terminal > interrupted.terminal);
}

#[tokio::test]
async fn empty_kill_ack_still_requires_original_terminal_proof() -> Result<(), antalya_rs::Error> {
    let cancelled = br#"{"data":[{"starts":1,"finishes":0,"errors":1,"cancelled":1,"result_rows":0,"read_rows":0,"read_bytes":0}]}"#;
    for (proof, expected) in [
        (cancelled.as_slice(), Outcome::Cancelled),
        (SUCCESS, Outcome::Unknown),
    ] {
        let server = Server::new(vec![
            Step::new(RequestKind::Data, stream()),
            Step::new(RequestKind::Kill, []),
            Step::new(RequestKind::Flush, []),
            Step::new(RequestKind::Observe, proof),
        ]);
        let client = server.client(WAIT);
        let mut execution = client
            .fetch_arrow(Query::new("SELECT v", "empty-kill-ack")?)
            .await?;
        drop(execution.next_provisional_batch().await?);
        let result = execution.cancel().await;
        if expected == Outcome::Cancelled {
            assert_eq!(result?, expected);
        } else {
            assert!(matches!(result, Err(ref error) if error.cause() == Some(Cause::InvalidProof)));
        }
        assert_eq!(execution.outcome(), expected);
        assert!(execution.next_batch().await.is_err());
        assert_eq!(server.requests.load(Ordering::Acquire), 4);
    }
    Ok(())
}

#[tokio::test]
async fn malformed_nonempty_kill_ack_remains_uncertain() -> Result<(), antalya_rs::Error> {
    let server = Server::new(vec![
        Step::new(RequestKind::Data, stream()),
        Step::new(RequestKind::Kill, b"not JSON"),
    ]);
    let client = server.client(WAIT);
    let mut execution = client
        .fetch_arrow(Query::new("SELECT v", "invalid-kill-ack")?)
        .await?;
    drop(execution.next_provisional_batch().await?);
    assert!(
        matches!(execution.cancel().await, Err(ref error) if error.cause() == Some(Cause::JsonDecoder))
    );
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert_eq!(server.requests.load(Ordering::Acquire), 2);
    Ok(())
}
