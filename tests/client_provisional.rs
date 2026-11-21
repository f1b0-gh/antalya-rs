// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "arrow")]
mod support;
use antalya_rs::{
    AntalyaClient, AuthorizationContext, AuthorizedRequest, Config, Credentials, Error, ErrorKind,
    Format, Outcome, Query, ResourceLimits,
};
use std::time::Duration;
use support::{Server, stream};

fn client(server: &Server) -> AntalyaClient {
    AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("token").unwrap(),
        )
        .timeout(Duration::from_secs(2)),
    )
    .unwrap()
}
fn query() -> Query {
    Query::new("SELECT v", uuid::Uuid::new_v4().to_string()).unwrap()
}

#[tokio::test]
async fn provisional_eof_never_implies_success_or_starts_terminal_queries() {
    let server = Server::new(stream(1), 2, false, 200);
    let mut execution = client(&server).fetch_arrow(query()).await.unwrap();
    drop(execution.next_provisional_batch().await.unwrap().unwrap());
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Running);
    assert!(execution.metrics().stream_end.is_some());
    assert_eq!(execution.metrics().polls, 0);
    assert_eq!(execution.metrics().flush_attempts, 0);
    assert!(
        !server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.contains("system.query_log") || r.contains("SYSTEM FLUSH LOGS"))
    );
    // Repeated data EOF is still provisional and does not consume verification.
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    execution.confirm_completion().await.unwrap();
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    let polls = execution.metrics().polls;
    execution.confirm_completion().await.unwrap();
    assert_eq!(execution.metrics().polls, polls);
}

#[tokio::test]
async fn confirmation_before_data_eof_refuses_without_consuming_data_or_changing_outcome() {
    let server = Server::new(stream(1), 2, false, 200);
    let mut execution = client(&server).fetch_arrow(query()).await.unwrap();
    assert_eq!(
        execution.confirm_completion().await.unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(execution.outcome(), Outcome::Running);
    assert!(server.requests.lock().unwrap().is_empty());
    assert_eq!(
        execution
            .next_provisional_batch()
            .await
            .unwrap()
            .unwrap()
            .num_rows(),
        2
    );
    assert_eq!(
        execution.confirm_completion().await.unwrap_err(),
        Error::InvalidRequest
    );
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    execution.confirm_completion().await.unwrap();
}

#[tokio::test]
async fn complete_ipc_missing_a_whole_batch_remains_unconfirmed() {
    let server = Server::new(stream(1), 4, false, 200);
    let mut execution = client(&server).fetch_arrow(query()).await.unwrap();
    drop(execution.next_provisional_batch().await.unwrap());
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    assert_eq!(execution.rows_delivered(), 2);
    assert_eq!(execution.outcome(), Outcome::Running);
    assert_eq!(
        execution.confirm_completion().await.unwrap_err().kind(),
        ErrorKind::Unconfirmed
    );
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert!(execution.confirm_completion().await.is_err());
    assert!(execution.next_provisional_batch().await.is_err());
}

#[tokio::test]
async fn empty_schema_is_provisional_until_zero_row_proof() {
    let server = Server::new(stream(0), 0, false, 200);
    let mut execution = client(&server).fetch_arrow(query()).await.unwrap();
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Running);
    assert!(execution.schema().is_some());
    execution.confirm_completion().await.unwrap();
    assert_eq!(execution.outcome(), Outcome::Succeeded);
}

#[tokio::test]
async fn provisional_data_retains_admission_until_confirmation() {
    let server = Server::new(stream(1), 2, false, 200);
    let client = AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("token").unwrap(),
        )
        .resources(ResourceLimits {
            max_active_queries: 1,
            max_queued_queries: 0,
            ..ResourceLimits::default()
        }),
    )
    .unwrap();
    let mut execution = client.fetch_arrow(query()).await.unwrap();
    drop(execution.next_provisional_batch().await.unwrap());
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    assert_eq!(
        client.fetch_arrow(query()).await.err().unwrap().kind(),
        ErrorKind::LimitExceeded
    );
    execution.confirm_completion().await.unwrap();
    assert!(client.fetch_arrow(query()).await.is_ok());
}

#[tokio::test]
async fn existing_strict_api_still_confirms_after_provisional_batches() {
    let server = Server::new(stream(1), 2, false, 403);
    let mut execution = client(&server).fetch_arrow(query()).await.unwrap();
    drop(execution.next_provisional_batch().await.unwrap());
    assert!(execution.next_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    assert_eq!(execution.metrics().flush_failures, 1);
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn authorized_confirmation_keeps_private_marker_and_principal_binding() {
    let server = Server::new(stream(1), 2, false, 200);
    let request = AuthorizedRequest::new(
        query(),
        server.endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "provisional-receipt".into(),
            expected_principal: "fixture".into(),
        },
    )
    .unwrap();
    let mut execution = client(&server)
        .fetch_authorized_arrow(request)
        .await
        .unwrap();
    drop(execution.next_provisional_batch().await.unwrap());
    assert!(execution.next_provisional_batch().await.unwrap().is_none());
    execution.confirm_completion().await.unwrap();
    let requests = server.requests.lock().unwrap();
    let url = |request: &str| {
        let target = request
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        reqwest::Url::parse(&format!("http://localhost{target}")).unwrap()
    };
    let data = requests.iter().find(|r| r.contains("SELECT v")).unwrap();
    let marker = url(data)
        .query_pairs()
        .find(|(k, _)| k == "log_comment")
        .unwrap()
        .1
        .into_owned();
    let proof = requests
        .iter()
        .find(|r| r.contains("system.query_log"))
        .unwrap();
    let pairs = url(proof)
        .query_pairs()
        .into_owned()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(pairs.get("param_nonce"), Some(&marker));
    assert_eq!(
        pairs.get("param_id").map(String::as_str),
        Some(execution.query_id())
    );
    assert_eq!(
        pairs.get("param_principal").map(String::as_str),
        Some("fixture")
    );
    assert!(proof.contains("log_comment={nonce:String}") && proof.contains("user=currentUser()"));
}
