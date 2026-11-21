#![cfg(feature = "arrow")]
#![forbid(unsafe_code)]

use antalya_rs::{
    AntalyaClient, AuthorizationContext, AuthorizedRequest, Cause, Config, Credentials, Endpoint,
    Error, Format, Outcome, Phase, Query, RecoveryReceipt, ResourceLimits,
};
use std::{
    io::{self, Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};

#[derive(Debug)]
enum TestError {
    Client(Error),
    Io(io::Error),
    Transport(antalya_rs::TransportError),
    Receiver(antalya_rs::ReceiverError),
}
impl From<antalya_rs::TransportError> for TestError {
    fn from(e: antalya_rs::TransportError) -> Self {
        Self::Transport(e)
    }
}
impl From<antalya_rs::ReceiverError> for TestError {
    fn from(e: antalya_rs::ReceiverError) -> Self {
        Self::Receiver(e)
    }
}
impl From<Error> for TestError {
    fn from(e: Error) -> Self {
        Self::Client(e)
    }
}
impl From<io::Error> for TestError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client(e) => e.fmt(f),
            Self::Io(e) => e.fmt(f),
            Self::Transport(e) => e.fmt(f),
            Self::Receiver(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for TestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Client(e) => e,
            Self::Io(e) => e,
            Self::Transport(e) => e,
            Self::Receiver(e) => e,
        })
    }
}

#[derive(Default)]
struct State {
    requests: Mutex<Vec<String>>,
    proof: AtomicU8,
    block_data: AtomicBool,
    block_proof: AtomicBool,
    stop: AtomicBool,
}
struct Server {
    endpoint: Endpoint,
    state: Arc<State>,
    thread: Option<std::thread::JoinHandle<io::Result<()>>>,
}
impl Server {
    fn new() -> Result<Self, TestError> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr()?))?;
        let state = Arc::new(State::default());
        let shared = Arc::clone(&state);
        let thread = std::thread::spawn(move || serve(listener, &shared));
        Ok(Self {
            endpoint,
            state,
            thread: Some(thread),
        })
    }
    fn client(&self) -> Result<AntalyaClient, Error> {
        AntalyaClient::new(
            Config::new(self.endpoint.clone(), Credentials::bearer("token")?)
                .timeout(Duration::from_secs(2))
                .resources(ResourceLimits {
                    max_active_queries: 1,
                    max_queued_queries: 0,
                    max_control_queries: 1,
                    ..ResourceLimits::default()
                }),
        )
    }
    fn request(&self, id: &str) -> Result<AuthorizedRequest, Error> {
        AuthorizedRequest::new(
            Query::new("CREATE TABLE frozen AS SELECT 1", id)?,
            self.endpoint.clone(),
            Format::Json,
            AuthorizationContext {
                receipt_id: "host-approval".into(),
                expected_principal: "fixture".into(),
            },
        )
    }
    fn requests(&self) -> io::Result<Vec<String>> {
        self.state
            .requests
            .lock()
            .map(|v| v.clone())
            .map_err(|_| io::Error::other("fixture mutex"))
    }
    fn finish(mut self) -> io::Result<()> {
        self.state.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("fixture thread"))??;
        }
        Ok(())
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
fn serve(listener: TcpListener, state: &State) -> io::Result<()> {
    'accept: while !state.stop.load(Ordering::Acquire) {
        let (mut socket, _) = match listener.accept() {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            Err(e) => return Err(e),
        };
        socket.set_read_timeout(Some(Duration::from_secs(1)))?;
        socket.set_write_timeout(Some(Duration::from_secs(1)))?;
        let mut bytes = Vec::new();
        let mut chunk = [0; 4096];
        loop {
            let n = socket.read(&mut chunk)?;
            if n == 0 {
                if bytes.is_empty() {
                    continue 'accept;
                }
                return Err(io::Error::other("incomplete fixture request"));
            }
            if bytes.len() + n > 65536 {
                return Err(io::Error::other("fixture request limit"));
            }
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let size = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<u16>())
                    })
                    .transpose()
                    .map_err(io::Error::other)?
                    .unwrap_or(0);
                if bytes.len() >= end + 4 + usize::from(size) {
                    break;
                }
            }
        }
        let request = String::from_utf8(bytes).map_err(io::Error::other)?;
        let data = request.contains("CREATE TABLE frozen");
        let proof = request.contains("system.query_log");
        let response = if request.contains("currentUser() AS principal") {
            r#"{"data":[{"principal":"fixture"}]}"#.to_owned()
        } else if request.contains("system.query_log") {
            let (starts, finishes, errors, cancelled) = match state.proof.load(Ordering::Acquire) {
                1 => (1, 1, 0, 0),
                2 => (1, 0, 1, 0),
                3 => (1, 0, 1, 1),
                _ => (2, 1, 0, 0),
            };
            format!(
                r#"{{"data":[{{"starts":{starts},"finishes":{finishes},"errors":{errors},"cancelled":{cancelled},"read_rows":0,"read_bytes":0,"result_rows":0}}]}}"#
            )
        } else {
            String::new()
        };
        {
            let mut requests = state
                .requests
                .lock()
                .map_err(|_| io::Error::other("fixture mutex"))?;
            if requests.len() >= 64 {
                return Err(io::Error::other("fixture request count limit"));
            }
            requests.push(request);
        }
        while ((data && state.block_data.load(Ordering::Acquire))
            || (proof && state.block_proof.load(Ordering::Acquire)))
            && !state.stop.load(Ordering::Acquire)
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        // A cancelled client may already have closed its response stream.
        let _ = write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.len(),
            response
        );
    }
    Ok(())
}

#[tokio::test]
async fn ready_command_admits_before_response_poll_and_wait_sends_no_controls()
-> Result<(), TestError> {
    let server = Server::new()?;
    server.state.proof.store(1, Ordering::Release);
    server.state.block_data.store(true, Ordering::Release);
    let client = server.client()?;
    let ready = client
        .prepare_authorized_command(server.request("ready-command")?)
        .await?
        .ready_transport()
        .await?;
    let receipt = ready.recovery_receipt()?;
    assert!(
        !server
            .requests()?
            .iter()
            .any(|r| r.contains("CREATE TABLE frozen"))
    );
    let mut response = ready.admit()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if server
            .requests()?
            .iter()
            .any(|r| r.contains("CREATE TABLE frozen"))
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::other("command was deferred until response polling").into());
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    server.state.block_data.store(false, Ordering::Release);
    assert!(response.next_chunk().await?.is_none());
    assert!(response.transport_eof());
    let requests = server.requests()?;
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.contains("CREATE TABLE frozen"))
            .count(),
        1
    );
    assert!(!requests.iter().any(|r| r.contains("system.query_log")));
    let command = requests
        .iter()
        .find(|r| r.contains("CREATE TABLE frozen"))
        .ok_or_else(|| io::Error::other("missing command"))?;
    assert!(!command.contains("FORMAT"));
    assert!(!command.contains("output_format_arrow"));
    assert!(command.contains(receipt.marker()));
    assert!(
        client
            .prepare_authorized_command(server.request("capacity-before-proof")?)
            .await
            .is_err()
    );
    // A separately admitted client can use current credentials for controls.
    let control = server.client()?;
    let foreign = RecoveryReceipt::restore(
        server.endpoint.clone(),
        receipt.query_id().into(),
        uuid::Uuid::new_v4().to_string(),
        receipt.principal().into(),
    )?;
    assert!(matches!(
        response.accept_termination(&control.observe_terminal(&foreign).await?),
        Err(antalya_rs::TransportError::ObservationMismatch)
    ));
    assert_eq!(
        response.accept_termination(&control.observe_terminal(&receipt).await?)?,
        antalya_rs::RemoteTermination::Succeeded
    );
    drop(
        client
            .prepare_authorized_command(server.request("capacity-after-proof")?)
            .await?,
    );
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn guarded_command_refuses_without_dispatch_and_preserves_slot_on_refusal()
-> Result<(), TestError> {
    const TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJpc3N1ZXIiLCJhdWQiOiJhdWRpZW5jZSIsInN1YiI6ImZpeHR1cmUiLCJleHAiOjQxMDI0NDQ4MDB9.c2ln";
    // Synthetic metadata only: the real fixture verifies signatures/receiver policy.
    for reviewed in [false, true] {
        let server = Server::new()?;
        let config = if reviewed {
            Config::jwt_receiver(
                antalya_rs::JwtReceiverProfile::new(
                    server.endpoint.clone(),
                    "issuer",
                    "audience",
                    "fixture",
                    0,
                )?,
                TOKEN,
            )?
        } else {
            Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
        };
        let client = AntalyaClient::new(config.resources(ResourceLimits {
            max_active_queries: 1,
            max_queued_queries: 0,
            ..ResourceLimits::default()
        }))?;
        let ready = client
            .prepare_authorized_command(server.request("guard-command")?)
            .await?
            .ready_transport()
            .await?;
        assert!(matches!(
            ready.admit_before(4_102_444_800),
            Err(antalya_rs::TransportError::Receiver(_))
        ));
        drop(
            client
                .prepare_authorized_command(server.request("after-refusal")?)
                .await?,
        );
        assert!(
            !server
                .requests()?
                .iter()
                .any(|r| r.contains("CREATE TABLE frozen"))
        );
        server.finish()?;
    }
    Ok(())
}

#[tokio::test]
async fn guarded_command_admits_at_bound_and_eof_drop_does_not_release_unknown_slot()
-> Result<(), TestError> {
    const TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJpc3N1ZXIiLCJhdWQiOiJhdWRpZW5jZSIsInN1YiI6ImZpeHR1cmUiLCJleHAiOjQxMDI0NDQ4MDB9.c2ln";
    let server = Server::new()?;
    let profile = antalya_rs::JwtReceiverProfile::new(
        server.endpoint.clone(),
        "issuer",
        "audience",
        "fixture",
        0,
    )?;
    let client = AntalyaClient::new(Config::jwt_receiver(profile, TOKEN)?.resources(
        ResourceLimits {
            max_active_queries: 1,
            max_queued_queries: 0,
            ..ResourceLimits::default()
        },
    ))?;
    let ready = client
        .prepare_authorized_command(server.request("guard-accepted")?)
        .await?
        .ready_transport()
        .await?;
    let mut response = ready.admit_before(4_102_444_801)?;
    assert!(response.next_chunk().await?.is_none());
    drop(response);
    assert!(
        client
            .prepare_authorized_command(server.request("after-unproved-drop")?)
            .await
            .is_err()
    );
    assert_eq!(
        server
            .requests()?
            .iter()
            .filter(|r| r.contains("CREATE TABLE frozen"))
            .count(),
        1
    );
    assert!(
        !server
            .requests()?
            .iter()
            .any(|r| r.contains("system.query_log"))
    );
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn receipt_precedes_dispatch_and_prepared_drop_releases_capacity() -> Result<(), TestError> {
    let server = Server::new()?;
    let client = server.client()?;
    let prepared = client
        .prepare_authorized_command(server.request("prepare-1")?)
        .await?;
    let receipt = prepared.recovery_receipt()?;
    assert_eq!(receipt.principal(), "fixture");
    assert_eq!(receipt.query_id(), "prepare-1");
    RecoveryReceipt::restore(
        Endpoint::parse(receipt.endpoint())?,
        receipt.query_id().into(),
        receipt.marker().into(),
        receipt.principal().into(),
    )?;
    assert_eq!(server.requests()?.len(), 1);
    drop(prepared);
    drop(
        client
            .prepare_authorized_command(server.request("prepare-2")?)
            .await?,
    );
    assert!(
        server
            .requests()?
            .iter()
            .all(|r| !r.contains("CREATE TABLE frozen"))
    );
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn terminal_outcomes_release_capacity_without_replaying_command() -> Result<(), TestError> {
    for (proof, expected) in [
        (1, Outcome::Succeeded),
        (2, Outcome::Failed),
        (3, Outcome::Cancelled),
    ] {
        let server = Server::new()?;
        server.state.proof.store(proof, Ordering::Release);
        let client = server.client()?;
        let prepared = client
            .prepare_authorized_command(server.request("terminal-1")?)
            .await?;
        let receipt = prepared.recovery_receipt()?;
        let mut execution = prepared.start()?;
        assert_eq!(execution.run().await?.outcome, expected);
        let requests = server.requests()?;
        assert!(
            requests
                .iter()
                .any(|r| r.contains(receipt.marker()) && r.contains("CREATE TABLE frozen"))
        );
        assert!(
            requests
                .iter()
                .any(|r| r.contains("param_principal=fixture") && r.contains("system.query_log"))
        );
        assert_eq!(execution.run().await?.outcome, expected);
        assert_eq!(execution.reconcile().await?.outcome, expected);
        assert_eq!(server.requests()?.len(), requests.len());
        drop(
            client
                .prepare_authorized_command(server.request("terminal-2")?)
                .await?,
        );
        server.finish()?;
    }
    Ok(())
}

#[tokio::test]
async fn unknown_retains_capacity_until_reconciliation() -> Result<(), TestError> {
    let server = Server::new()?;
    let client = server.client()?;
    let mut execution = client
        .prepare_authorized_command(server.request("unknown-1")?)
        .await?
        .start()?;
    assert_eq!(execution.run().await?.outcome, Outcome::Unknown);
    assert!(matches!(
        client
            .prepare_authorized_command(server.request("unknown-2")?)
            .await,
        Err(Error::Context {
            phase: Phase::Queue,
            cause: Cause::Capacity
        })
    ));
    let before = server.requests()?.len();
    assert_eq!(execution.run().await?.outcome, Outcome::Unknown);
    assert_eq!(server.requests()?.len(), before);
    server.state.proof.store(1, Ordering::Release);
    assert_eq!(execution.reconcile().await?.outcome, Outcome::Succeeded);
    assert_eq!(
        server
            .requests()?
            .iter()
            .filter(|r| r.contains("CREATE TABLE frozen"))
            .count(),
        1
    );
    drop(
        client
            .prepare_authorized_command(server.request("unknown-3")?)
            .await?,
    );
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn dropped_data_future_retains_admission_and_never_replays() -> Result<(), TestError> {
    let server = Server::new()?;
    server.state.block_data.store(true, Ordering::Release);
    let client = server.client()?;
    let mut execution = client
        .prepare_authorized_command(server.request("cancel-1")?)
        .await?
        .start()?;
    {
        let run = execution.run();
        tokio::pin!(run);
        let seen = async {
            loop {
                if server
                    .requests()?
                    .iter()
                    .any(|r| r.contains("CREATE TABLE frozen"))
                {
                    return Ok::<_, io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            result = &mut run => { result?; return Err(io::Error::other("command did not stay pending").into()); }
            result = tokio::time::timeout(Duration::from_secs(1), seen) => {
                result.map_err(io::Error::other)??;
            }
        }
    }
    assert_eq!(execution.run().await?.outcome, Outcome::Unknown);
    assert!(matches!(
        client
            .prepare_authorized_command(server.request("cancel-2")?)
            .await,
        Err(Error::Context {
            phase: Phase::Queue,
            cause: Cause::Capacity
        })
    ));
    server.state.proof.store(1, Ordering::Release);
    server.state.block_data.store(false, Ordering::Release);
    assert_eq!(execution.reconcile().await?.outcome, Outcome::Succeeded);
    assert_eq!(
        server
            .requests()?
            .iter()
            .filter(|r| r.contains("CREATE TABLE frozen"))
            .count(),
        1
    );
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn dropping_unknown_command_quarantines_its_slot() -> Result<(), TestError> {
    let server = Server::new()?;
    let client = server.client()?;
    let prepared = client
        .prepare_authorized_command(server.request("drop-1")?)
        .await?;
    let receipt = prepared.recovery_receipt()?;
    let mut execution = prepared.start()?;
    assert_eq!(execution.run().await?.outcome, Outcome::Unknown);
    drop(execution);
    assert!(matches!(
        client
            .prepare_authorized_command(server.request("drop-2")?)
            .await,
        Err(Error::Context {
            phase: Phase::Queue,
            cause: Cause::Capacity
        })
    ));
    // Receipt observation still uses the independent control lane. It cannot
    // return a forgotten slot or authorize a new attempt in the host journal.
    server.state.proof.store(1, Ordering::Release);
    assert_eq!(
        client.confirm_termination(&receipt).await?,
        antalya_rs::RemoteTermination::Succeeded
    );
    assert!(matches!(
        client
            .prepare_authorized_command(server.request("drop-3")?)
            .await,
        Err(Error::Context {
            phase: Phase::Queue,
            cause: Cause::Capacity
        })
    ));
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn invalid_command_authorization_is_rejected_before_network() -> Result<(), TestError> {
    let server = Server::new()?;
    let client = server.client()?;
    for (endpoint, format, principal) in [
        (
            server.endpoint.clone(),
            Format::ArrowStream,
            "fixture".into(),
        ),
        (
            Endpoint::parse("http://127.0.0.1:1")?,
            Format::Json,
            "fixture".into(),
        ),
        (server.endpoint.clone(), Format::Json, "x".repeat(1025)),
    ] {
        let request = AuthorizedRequest::new(
            Query::new("CREATE TABLE frozen AS SELECT 1", "invalid-1")?,
            endpoint,
            format,
            AuthorizationContext {
                receipt_id: "host".into(),
                expected_principal: principal,
            },
        )?;
        assert!(matches!(
            client.prepare_authorized_command(request).await,
            Err(Error::InvalidRequest)
        ));
    }
    assert!(server.requests()?.is_empty());
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn command_identity_mismatch_releases_capacity_without_dispatch() -> Result<(), TestError> {
    let server = Server::new()?;
    let client = server.client()?;
    let request = AuthorizedRequest::new(
        Query::new("CREATE TABLE frozen AS SELECT 1", "identity-1")?,
        server.endpoint.clone(),
        Format::Json,
        AuthorizationContext {
            receipt_id: "host".into(),
            expected_principal: "different".into(),
        },
    )?;
    assert!(matches!(
        client.prepare_authorized_command(request).await,
        Err(Error::Context {
            phase: Phase::Identity,
            cause: Cause::IdentityMismatch
        })
    ));
    let mut execution = client
        .prepare_authorized_command(server.request("identity-2")?)
        .await?
        .start()?;
    assert!(matches!(
        execution.reconcile().await,
        Err(Error::InvalidRequest)
    ));
    drop(execution);
    drop(
        client
            .prepare_authorized_command(server.request("identity-3")?)
            .await?,
    );
    assert!(
        server
            .requests()?
            .iter()
            .all(|r| !r.contains("CREATE TABLE frozen"))
    );
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn expired_preparation_never_dispatches_or_quarantines_capacity() -> Result<(), TestError> {
    let server = Server::new()?;
    let client = server.client()?;
    let prepared = client
        .prepare_authorized_command(server.request("expired-1")?)
        .await?;
    tokio::time::sleep(Duration::from_millis(2010)).await;
    assert!(matches!(
        prepared.start(),
        Err(Error::Context {
            phase: Phase::Data,
            cause: Cause::Timeout
        })
    ));
    let mut execution = client
        .prepare_authorized_command(server.request("expired-2")?)
        .await?
        .start()?;
    tokio::time::sleep(Duration::from_millis(2010)).await;
    assert!(matches!(
        execution.run().await,
        Err(Error::Context {
            phase: Phase::Data,
            cause: Cause::Timeout
        })
    ));
    drop(
        client
            .prepare_authorized_command(server.request("expired-3")?)
            .await?,
    );
    assert!(
        server
            .requests()?
            .iter()
            .all(|r| !r.contains("CREATE TABLE frozen"))
    );
    server.finish()?;
    Ok(())
}

#[tokio::test]
async fn cancelled_terminal_poll_retains_data_slot_and_releases_control_slot()
-> Result<(), TestError> {
    let server = Server::new()?;
    server.state.proof.store(1, Ordering::Release);
    server.state.block_proof.store(true, Ordering::Release);
    let client = server.client()?;
    let mut execution = client
        .prepare_authorized_command(server.request("proof-1")?)
        .await?
        .start()?;
    {
        let run = execution.run();
        tokio::pin!(run);
        let seen = async {
            loop {
                if server
                    .requests()?
                    .iter()
                    .any(|r| r.contains("system.query_log"))
                {
                    return Ok::<_, io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            result = &mut run => { result?; return Err(io::Error::other("proof did not stay pending").into()); }
            result = tokio::time::timeout(Duration::from_secs(1), seen) => {
                result.map_err(io::Error::other)??;
            }
        }
    }
    assert_eq!(execution.run().await?.outcome, Outcome::Unknown);
    assert!(matches!(
        client
            .prepare_authorized_command(server.request("proof-2")?)
            .await,
        Err(Error::Context {
            phase: Phase::Queue,
            cause: Cause::Capacity
        })
    ));
    server.state.block_proof.store(false, Ordering::Release);
    assert_eq!(execution.reconcile().await?.outcome, Outcome::Succeeded);
    assert_eq!(
        server
            .requests()?
            .iter()
            .filter(|r| r.contains("CREATE TABLE frozen"))
            .count(),
        1
    );
    server.finish()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the pinned bearer fixture via scripts/fixture.py"]
async fn real_command_receipt_confirms_a_hot_copy_after_client_restart() -> Result<(), TestError> {
    let endpoint = Endpoint::parse(&std::env::var("ANTALYA_TEST_URL").map_err(io::Error::other)?)?;
    let credentials =
        Credentials::bearer(&std::env::var("ANTALYA_TEST_TOKEN").map_err(io::Error::other)?)?;
    let config = Config::new(endpoint.clone(), credentials).timeout(Duration::from_secs(30));
    let client = AntalyaClient::new(config.clone())?;
    let database = format!("command_{}", uuid::Uuid::new_v4().simple());
    let approve = |sql: &str, format: Format| {
        AuthorizedRequest::new(
            Query::new(sql, format!("command-{}", uuid::Uuid::new_v4()))?,
            endpoint.clone(),
            format,
            AuthorizationContext {
                receipt_id: "qualified-command".into(),
                expected_principal: "fixture".into(),
            },
        )
    };
    let result = async {
        let mut create = client.prepare_authorized_command(approve(&format!("CREATE DATABASE {database}"), Format::Json)?).await?.start()?;
        assert_eq!(create.run().await?.outcome, Outcome::Succeeded);
        let prepared = client.prepare_authorized_command(approve(&format!(
            "CREATE TABLE {database}.frozen ENGINE = MergeTree ORDER BY number AS SELECT number FROM numbers(10)"), Format::Json)?).await?;
        let receipt = prepared.recovery_receipt()?;
        let restored = RecoveryReceipt::restore(Endpoint::parse(receipt.endpoint())?,
            receipt.query_id().into(), receipt.marker().into(), receipt.principal().into())?;
        let mut command = prepared.start()?;
        assert_eq!(command.run().await?.outcome, Outcome::Succeeded);
        drop(command);
        let restarted = AntalyaClient::new(config)?;
        assert_eq!(restarted.confirm_termination(&restored).await?, antalya_rs::RemoteTermination::Succeeded);
        let mut rows = restarted.fetch_authorized_arrow(approve(&format!("SELECT count() AS n FROM {database}.frozen"), Format::ArrowStream)?).await?;
        let batch = rows.next_batch().await?.ok_or_else(|| io::Error::other("missing count batch"))?;
        let values = batch.columns().first().and_then(|a| a.as_any().downcast_ref::<arrow_array::UInt64Array>())
            .ok_or_else(|| io::Error::other("unexpected count type"))?;
        assert_eq!(values.values().as_ref(), &[10]);
        assert!(rows.next_batch().await?.is_none());
        assert_eq!(rows.outcome(), Outcome::Succeeded);
        Ok::<_, TestError>(())
    }.await;
    let cleanup = client
        .execute_command(Query::new(
            format!("DROP DATABASE IF EXISTS {database}"),
            format!("cleanup-{}", uuid::Uuid::new_v4()),
        )?)
        .await;
    result?;
    assert_eq!(cleanup?, Outcome::Succeeded);
    Ok(())
}
