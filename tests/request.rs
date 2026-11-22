use antalya_rs::{Credentials, Endpoint, Format, Query};
use std::collections::BTreeMap;

fn request(query: Query, credentials: Credentials, format: Format) -> reqwest::Request {
    query
        .build_request(
            &reqwest::Client::new(),
            &Endpoint::parse("http://127.0.0.1:8123/").unwrap(),
            &credentials,
            format,
        )
        .unwrap()
}

#[test]
fn preserves_sql_and_encodes_parameters_without_option_injection() {
    let query = Query::new("SELECT '?' AS q, {value:String}", "scan-1")
        .unwrap()
        .setting("readonly", "1")
        .unwrap()
        .parameter("value", "a\t'\\\n&readonly=0")
        .unwrap();
    let req = request(query, Credentials::bearer("secret").unwrap(), Format::Json);
    let pairs: BTreeMap<_, _> = req.url().query_pairs().into_owned().collect();
    assert_eq!(pairs["readonly"], "1");
    assert_eq!(pairs["query_id"], "scan-1");
    assert_eq!(pairs["param_value"], "a\\t\\'\\\\\\n&readonly=0");
    assert_eq!(
        req.body().unwrap().as_bytes().unwrap(),
        b"SELECT '?' AS q, {value:String}\nFORMAT JSON"
    );
    assert_eq!(req.method(), reqwest::Method::POST);
    assert!(req.headers().get("x-clickhouse-user").is_none());
    assert!(req.headers()["authorization"].is_sensitive());
    assert!(!req.url().as_str().contains("secret"));
}

#[test]
fn forbids_endpoint_options_and_embedded_credentials() {
    for endpoint in [
        "file:///tmp/x",
        "http://localhost/?readonly=0",
        "http://user:secret@localhost/",
        "http://localhost/#x",
    ] {
        assert!(Endpoint::parse(endpoint).is_err(), "{endpoint}");
    }
    assert!(Endpoint::parse("https://example.com/clickhouse/").is_ok());
}

#[test]
fn rejects_reserved_settings_and_malformed_parameter_names() {
    for name in [
        "query",
        "query_id",
        "user",
        "password",
        "param_x",
        "database",
        "default_format",
        "session_id",
        "access_token",
        "role",
        "",
        "a&readonly",
    ] {
        assert!(
            Query::new("SELECT 1", "scan-1")
                .unwrap()
                .setting(name, "bad")
                .is_err(),
            "{name}"
        );
    }
    for name in ["", "x&readonly", "1bad", "a.b"] {
        assert!(
            Query::new("SELECT 1", "scan-1")
                .unwrap()
                .parameter(name, "x")
                .is_err()
        );
    }
    for id in ["", "a\nb", "a&readonly=0"] {
        assert!(Query::new("SELECT 1", id).is_err());
    }
    assert!(Query::new("  ", "scan-1").is_err());
}

#[test]
fn query_helpers_are_scoped_and_replace_duplicate_settings() {
    let base = Query::new("SELECT * FROM lake.events", "scan-2").unwrap();
    let query = base
        .clone()
        .swarm("workers")
        .unwrap()
        .iceberg_snapshot(123)
        .unwrap()
        .setting("max_threads", "2")
        .unwrap()
        .setting("max_threads", "4")
        .unwrap();
    let req = request(
        query,
        Credentials::bearer("token").unwrap(),
        Format::ArrowStream,
    );
    let pairs: Vec<_> = req.url().query_pairs().into_owned().collect();
    assert_eq!(pairs.iter().filter(|(k, _)| k == "max_threads").count(), 1);
    let pairs: BTreeMap<_, _> = pairs.into_iter().collect();
    assert_eq!(pairs["max_threads"], "4");
    assert_eq!(pairs["object_storage_cluster"], "workers");
    assert_eq!(pairs["iceberg_snapshot_id"], "123");
    assert_eq!(req.headers()["authorization"], "Bearer token");
    assert!(req.headers()["authorization"].is_sensitive());
    assert!(req.headers().get("x-clickhouse-user").is_none());
    let plain = request(base, Credentials::bearer("token").unwrap(), Format::Json);
    assert!(
        !plain
            .url()
            .query_pairs()
            .any(|(k, _)| k == "object_storage_cluster")
    );
    assert!(
        Query::new("SELECT 1", "scan-1")
            .unwrap()
            .iceberg_snapshot(0)
            .is_err()
    );
    assert!(Query::new("SELECT 1", "scan-1").unwrap().swarm("").is_err());
}

#[test]
fn credentials_reject_header_injection_and_debug_redacts_secrets() {
    assert!(Credentials::bearer("token\r\nInjected: yes").is_err());
    assert!(Credentials::bearer("").is_err());
    assert!(
        !format!("{:?}", Credentials::bearer("password-value").unwrap()).contains("password-value")
    );
    assert!(!format!("{:?}", Credentials::bearer("token-value").unwrap()).contains("token-value"));
}

#[test]
fn trailing_line_comment_cannot_swallow_output_format() {
    let req = request(
        Query::new("SELECT 1 -- comment", "scan-3").unwrap(),
        Credentials::bearer("token").unwrap(),
        Format::ArrowStream,
    );
    assert_eq!(
        req.body().unwrap().as_bytes().unwrap(),
        b"SELECT 1 -- comment\nFORMAT ArrowStream"
    );
}

#[test]
fn supports_arrow_ipc_file_output() {
    let req = request(
        Query::new("SELECT 1", "ipc-file").unwrap(),
        Credentials::bearer("token").unwrap(),
        Format::Arrow,
    );
    assert_eq!(
        req.body().unwrap().as_bytes().unwrap(),
        b"SELECT 1\nFORMAT Arrow"
    );
}
