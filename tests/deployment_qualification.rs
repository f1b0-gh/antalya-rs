//! Deployment contracts qualified against both owned, digest-pinned fixtures.
#![cfg(feature = "arrow")]

use antalya_rs::{
    AntalyaClient, Cause, Config, Credentials, Endpoint, Outcome, Query, TerminalPolicy,
    TerminalStrategy,
};
use std::time::{Duration, Instant};

// Another test must not flush away ObserveOnly publication latency.
static DEPLOYMENT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name}: run scripts/fixture.py"))
}
fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(25))
        .build()
        .unwrap()
}
fn client(token: &str) -> AntalyaClient {
    AntalyaClient::new(
        Config::new(
            Endpoint::parse(&env("ANTALYA_TEST_URL")).unwrap(),
            Credentials::bearer(token).unwrap(),
        )
        .timeout(Duration::from_secs(25))
        .terminal_policy(TerminalPolicy {
            strategy: TerminalStrategy::ObserveOnly,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
        }),
    )
    .unwrap()
}
fn query(sql: &str) -> Query {
    Query::new(sql, format!("deployment-{}", uuid::Uuid::new_v4())).unwrap()
}
async fn raw(token: &str, sql: &str) -> (reqwest::StatusCode, String) {
    let response = http()
        .post(env("ANTALYA_TEST_URL"))
        .bearer_auth(token)
        .body(sql.to_owned())
        .send()
        .await
        .unwrap();
    (response.status(), response.text().await.unwrap())
}
async fn admin(sql: &str) -> String {
    let (status, body) = raw(&env("ANTALYA_TEST_TOKEN"), sql).await;
    assert!(
        status.is_success(),
        "fixture control request failed: {status}"
    );
    body
}

#[tokio::test]
#[ignore = "requires scripts/fixture.py and its SELECT-only JWT users"]
async fn readonly_one_plain_select_succeeds_and_tracked_select_refuses_promptly() {
    let _serial = DEPLOYMENT.lock().await;
    let token = env("ANTALYA_TEST_READONLY1_TOKEN");
    let (status, plain) = raw(&token, "SELECT 42").await;
    assert!(status.is_success());
    assert_eq!(plain.trim(), "42");
    let client = client(&token);
    let query = query("SELECT 42");
    let id = query.query_id().to_owned();
    let started = Instant::now();
    let mut execution = client.fetch_arrow(query).await.unwrap();
    let error = execution.next_batch().await.unwrap_err();
    assert_eq!(error.cause(), Some(Cause::ServerResponse(Some(164))));
    assert_eq!(execution.outcome(), Outcome::Unknown);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(execution.metrics().flush_attempts, 0);
    assert_eq!(execution.metrics().polls, 0);
    admin("SYSTEM FLUSH LOGS query_log").await;
    assert_eq!(
        admin(&format!(
            "SELECT count() FROM system.query_log WHERE query_id='{id}'"
        ))
        .await
        .trim(),
        "0",
        "the settings refusal occurs before query-log evidence exists"
    );
}

#[tokio::test]
#[ignore = "requires scripts/fixture.py with the default query-log interval"]
async fn readonly_two_observe_only_succeeds_without_flush_grant_at_default_interval() {
    let _serial = DEPLOYMENT.lock().await;
    assert_eq!(env("ANTALYA_TEST_LOG_FLUSH_INTERVAL"), "7500");
    let token = env("ANTALYA_TEST_READONLY2_TOKEN");
    let (status, grants) = raw(&token, "SHOW GRANTS").await;
    assert!(status.is_success());
    assert!(grants.contains("GRANT SELECT"));
    assert!(!grants.contains("SYSTEM FLUSH"));
    assert!(
        !raw(&token, "SYSTEM FLUSH LOGS query_log")
            .await
            .0
            .is_success()
    );
    let client = client(&token);
    let mut execution = client.fetch_arrow(query("SELECT 42")).await.unwrap();
    assert_eq!(execution.next_batch().await.unwrap().unwrap().num_rows(), 1);
    assert!(execution.next_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    assert_eq!(execution.metrics().flush_attempts, 0);
    assert!(execution.metrics().polls >= 1);
    // readonly=2 permits the per-execution settings; it still forbids writes.
    assert!(
        !raw(&token, "CREATE DATABASE forbidden_readonly_write")
            .await
            .0
            .is_success()
    );
}

#[tokio::test]
#[ignore = "requires scripts/fixture.py's isolated Docker network"]
async fn catalog_create_and_exception_mask_credentials_in_response_metadata_and_query_log() {
    let _serial = DEPLOYMENT.lock().await;
    use antalya_rs::sql::{Catalog, Identifier};
    let canary = env("ANTALYA_TEST_CATALOG_TOKEN");
    let stub_endpoint = env("ANTALYA_TEST_CATALOG_URL");
    let client = client(&env("ANTALYA_TEST_TOKEN"));
    let capabilities = client.capabilities().await.unwrap();
    for succeeds in [true, false] {
        let name = format!("catalog_{}", uuid::Uuid::new_v4().simple());
        let id = format!("catalog-{}", uuid::Uuid::new_v4());
        let route = if succeeds { "success" } else { "failure" };
        let ddl = Catalog::new(
            Identifier::new(&name).unwrap(),
            Endpoint::parse(&format!("{stub_endpoint}{route}/")).unwrap(),
            "qualification",
            Credentials::bearer(&canary).unwrap(),
        )
        .unwrap()
        .query(&capabilities, &id)
        .unwrap();
        let response = http()
            .execute(client.build_command_request(&ddl).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let response_body = response.text().await.unwrap();
        let metadata = if status.is_success() {
            admin(&format!("SHOW CREATE DATABASE `{name}`")).await
        } else {
            String::new()
        };
        let stored = admin(&format!(
            "SELECT engine_full FROM system.databases WHERE name='{name}'"
        ))
        .await;
        admin("SYSTEM FLUSH LOGS query_log").await;
        let log = admin(&format!("SELECT type, query, exception, Settings FROM system.query_log WHERE query_id='{id}' FORMAT JSONEachRow")).await;
        // Cleanup precedes assertions, including a surprising successful failure case.
        admin(&format!("DROP DATABASE IF EXISTS `{name}` SYNC")).await;
        assert_eq!(
            status.is_success(),
            succeeds,
            "catalog {route} status {status}"
        );
        assert!(
            !response_body.contains(&canary),
            "catalog response leaked the synthetic credential"
        );
        assert!(
            !metadata.contains(&canary),
            "SHOW CREATE leaked the synthetic credential"
        );
        assert!(
            !stored.contains(&canary),
            "system.databases leaked the synthetic credential"
        );
        assert!(
            !log.contains(&canary),
            "query_log leaked the synthetic credential"
        );
        assert!(
            log.contains(if succeeds { "QueryFinish" } else { "Exception" }),
            "missing terminal catalog query-log row"
        );
        let observations =
            std::fs::read_to_string(env("ANTALYA_TEST_CATALOG_OBSERVATIONS")).unwrap();
        assert!(
            observations
                .lines()
                .any(|line| line == format!("{route}\t1")),
            "catalog request did not deliver the expected synthetic credential to the stub"
        );
        if succeeds {
            assert!(metadata.contains("DataLakeCatalog"));
            assert!(stored.contains("DataLakeCatalog"));
        } else {
            assert!(
                response_body.starts_with("Code: 117."),
                "expected the stub's malformed-JSON error, not an unrelated refusal"
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires either digest-pinned fixture; syntax only, no remote IO"]
async fn capability_helpers_parse_on_each_supported_fixture() {
    let _serial = DEPLOYMENT.lock().await;
    use antalya_rs::sql::{Catalog, ExportPart, Hybrid, Identifier, PartitionBy, TableName};
    let client = client(&env("ANTALYA_TEST_TOKEN"));
    let capabilities = client.capabilities().await.unwrap();
    assert_eq!(capabilities.version(), env("ANTALYA_TEST_VERSION"));
    let statements = [
        Catalog::new(
            Identifier::new("catalog_parse").unwrap(),
            Endpoint::parse("http://127.0.0.1:9/").unwrap(),
            "warehouse",
            Credentials::bearer("synthetic-parser-only").unwrap(),
        )
        .unwrap()
        .query(&capabilities, "parse-catalog")
        .unwrap(),
        Hybrid::new(
            TableName::new("default", "combined").unwrap(),
            TableName::new("default", "hot").unwrap(),
            "fixture",
            TableName::new("default", "cold").unwrap(),
            Identifier::new("ts").unwrap(),
            "2026-11-10 00:00:00",
        )
        .unwrap()
        .query(&capabilities, "parse-hybrid")
        .unwrap(),
        ExportPart::new(
            TableName::new("default", "hot").unwrap(),
            "all_1_1_0",
            "http://127.0.0.1:9/export/{_partition_id}.parquet",
            PartitionBy::YearMonth(Identifier::new("ts").unwrap()),
        )
        .unwrap()
        .query(&capabilities, "parse-export")
        .unwrap(),
    ];
    for statement in statements {
        let mut request = client.build_command_request(&statement).unwrap();
        let sql = std::str::from_utf8(request.body().unwrap().as_bytes().unwrap()).unwrap();
        *request.body_mut() = Some(format!("EXPLAIN AST {sql}").into());
        let response = http().execute(request).await.unwrap();
        assert!(
            response.status().is_success(),
            "{} parser failed",
            statement.query_id()
        );
        assert!(response.text().await.unwrap().contains("Query"));
    }
}
