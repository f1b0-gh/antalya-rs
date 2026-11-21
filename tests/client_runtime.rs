#![cfg(feature = "arrow")]
mod support;
use antalya_rs::{
    AntalyaClient, Config, Credentials, ErrorKind, ExecutionLimits, Outcome, Phase, Query,
    ResourceLimits, TerminalPolicy, TerminalStrategy,
};
use std::time::Duration;
use support::{Server, stream};
fn config(server: &Server) -> Config {
    Config::new(
        server.endpoint.clone(),
        Credentials::bearer("token").unwrap(),
    )
    .timeout(Duration::from_secs(2))
}
fn query() -> Query {
    Query::new("SELECT v", uuid::Uuid::new_v4().to_string()).unwrap()
}

#[tokio::test]
async fn observe_only_waits_for_exact_proof_without_flush() {
    for missing in [false, true] {
        let server = Server::new(stream(1), 2, missing, 403);
        let client = AntalyaClient::new(config(&server).terminal_policy(TerminalPolicy {
            strategy: TerminalStrategy::ObserveOnly,
            ..TerminalPolicy::default()
        }))
        .unwrap();
        let mut execution = client.fetch_arrow(query()).await.unwrap();
        drop(execution.next_batch().await.unwrap());
        assert!(execution.next_batch().await.unwrap().is_none());
        assert_eq!(execution.outcome(), Outcome::Succeeded);
        assert_eq!(execution.metrics().flush_attempts, 0);
        assert_eq!(execution.metrics().polls, if missing { 2 } else { 1 });
        assert!(
            !server
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.contains("SYSTEM FLUSH LOGS"))
        );
    }
}

#[tokio::test]
async fn observe_only_rejects_terminal_row_count_mismatch() {
    let server = Server::new(stream(1), 4, false, 200);
    let client = AntalyaClient::new(config(&server).terminal_policy(TerminalPolicy {
        strategy: TerminalStrategy::ObserveOnly,
        ..TerminalPolicy::default()
    }))
    .unwrap();
    let mut execution = client.fetch_arrow(query()).await.unwrap();
    drop(execution.next_batch().await.unwrap());
    assert_eq!(
        execution.next_batch().await.unwrap_err().kind(),
        ErrorKind::Unconfirmed
    );
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert_eq!(execution.metrics().flush_attempts, 0);
}

#[tokio::test]
async fn observe_first_skips_flush_only_with_full_terminal_proof() {
    for missing in [false, true] {
        let server = Server::new(stream(1), 2, missing, 200);
        let client = AntalyaClient::new(config(&server).terminal_policy(TerminalPolicy {
            strategy: TerminalStrategy::ObserveFirst,
            ..TerminalPolicy::default()
        }))
        .unwrap();
        let mut execution = client.fetch_arrow(query()).await.unwrap();
        let batch = execution.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        drop(batch);
        assert!(execution.next_batch().await.unwrap().is_none());
        assert_eq!(execution.outcome(), Outcome::Succeeded);
        let m = execution.metrics();
        assert_eq!(m.flush_attempts, u64::from(missing));
        assert_eq!(m.polls, if missing { 2 } else { 1 });
        assert!(m.first_batch.is_some());
        assert!(m.stream_end.is_some());
        assert!(m.transfer.is_some() && m.decode.is_some());
        assert!(m.total >= m.data_poll && m.total >= m.terminal);
        assert!(m.last_error.is_none());
        assert_eq!(
            server
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.contains("SYSTEM FLUSH LOGS"))
                .count(),
            usize::from(missing)
        );
    }
}
#[tokio::test]
async fn flush_denial_is_observable_without_hiding_later_success() {
    let server = Server::new(stream(1), 2, false, 403);
    let client = AntalyaClient::new(config(&server)).unwrap();
    let mut execution = client.fetch_arrow(query()).await.unwrap();
    drop(execution.next_batch().await.unwrap());
    assert!(execution.next_batch().await.unwrap().is_none());
    let m = execution.metrics();
    assert_eq!(m.flush_failures, 1);
    assert!(m.last_control_error.is_some());
    assert_eq!(execution.outcome(), Outcome::Succeeded);
}
#[tokio::test]
async fn cumulative_batch_limit_refuses_before_second_delivery() {
    let server = Server::new(stream(2), 4, false, 200);
    let client = AntalyaClient::new(config(&server).execution_limits(ExecutionLimits {
        max_batches: 1,
        ..ExecutionLimits::default()
    }))
    .unwrap();
    let mut execution = client.fetch_arrow(query()).await.unwrap();
    drop(execution.next_batch().await.unwrap());
    let error = execution.next_batch().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::LimitExceeded);
    assert_eq!(execution.batches_delivered(), 1);
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert_eq!(execution.metrics().last_error, Some(error));
}
#[tokio::test]
async fn retained_batch_clone_keeps_bytes_after_execution_drop() {
    let server = Server::new(stream(1), 2, false, 200);
    // Leave room for transport backing and Arrow metadata, whose retained size
    // varies with chunk boundaries and buffer alignment. Equal batch and pool
    // limits still make any retained clone block the next full reservation.
    const BUDGET: usize = 64 * 1024;
    let limits = ExecutionLimits {
        max_batch_bytes: BUDGET,
        ..ExecutionLimits::default()
    };
    let resources = ResourceLimits {
        max_retained_bytes: BUDGET,
        ..ResourceLimits::default()
    };
    let client = AntalyaClient::new(
        config(&server)
            .execution_limits(limits)
            .resources(resources),
    )
    .unwrap();
    let mut first = client.fetch_arrow(query()).await.unwrap();
    let batch = first.next_batch().await.unwrap().unwrap();
    assert!(batch.charged_bytes() > 0 && batch.charged_bytes() < BUDGET);
    let retained = batch.clone();
    drop(batch);
    drop(first);
    let mut second = client.fetch_arrow(query()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), second.next_batch())
            .await
            .is_err()
    );
    drop(retained);
    let batch = second.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
}
#[tokio::test]
async fn saturated_data_lane_refuses_queue_but_leaves_control_available() {
    let server = Server::new(stream(1), 2, false, 200);
    let client = AntalyaClient::new(config(&server).resources(ResourceLimits {
        max_active_queries: 1,
        max_queued_queries: 0,
        ..ResourceLimits::default()
    }))
    .unwrap();
    let mut first = client.fetch_arrow(query()).await.unwrap();
    let error = client.clone().fetch_arrow(query()).await.err().unwrap();
    assert_eq!(error.phase(), Some(Phase::Queue));
    drop(first.next_batch().await.unwrap());
    assert!(first.next_batch().await.unwrap().is_none());
    assert!(client.fetch_arrow(query()).await.is_ok());
}
