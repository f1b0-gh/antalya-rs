use antalya_rs::{
    AntalyaClient, AuthorizationContext, AuthorizedRequest, Config, Credentials, Endpoint, Format,
    Parameter, Query,
};
use std::time::Duration;
fn endpoint() -> Endpoint {
    Endpoint::parse("http://127.0.0.1:8123").unwrap()
}
#[test]
fn impossible_deadline_is_a_typed_configuration_error() {
    let result = AntalyaClient::new(
        Config::new(endpoint(), Credentials::bearer("token").unwrap()).timeout(Duration::MAX),
    );
    assert!(matches!(result, Err(antalya_rs::Error::InvalidSetting)));
}
#[test]
fn typed_values_and_database_are_explicit() {
    let q = Query::new(
        "SELECT {n:Int64}, {b:Bool}, {v:Nullable(String)}",
        "typed-1",
    )
    .unwrap()
    .database("analytics")
    .unwrap()
    .typed_parameter("n", Parameter::Int64(-42))
    .unwrap()
    .typed_parameter("b", Parameter::Bool(true))
    .unwrap()
    .typed_parameter("v", Parameter::Null)
    .unwrap();
    let c = AntalyaClient::new(
        Config::new(endpoint(), Credentials::bearer("token").unwrap())
            .timeout(Duration::from_secs(5)),
    )
    .unwrap();
    let r = c.build_request(&q, Format::Json).unwrap();
    let p: std::collections::BTreeMap<_, _> = r.url().query_pairs().collect();
    assert_eq!(p["database"], "analytics");
    assert_eq!(p["param_n"], "-42");
    assert_eq!(p["param_b"], "true");
    assert_eq!(p["param_v"], "\\N");
    assert!(r.headers().get("x-clickhouse-user").is_none());
}
#[test]
fn commands_have_no_format_and_frozen_request_keeps_binding() {
    let q = Query::new("CREATE DATABASE demo", "ddl-1").unwrap();
    let r = q
        .build_command_request(
            &reqwest::Client::new(),
            &endpoint(),
            &Credentials::bearer("token").unwrap(),
        )
        .unwrap();
    assert_eq!(
        r.body().unwrap().as_bytes().unwrap(),
        b"CREATE DATABASE demo"
    );
    let frozen = AuthorizedRequest::new(
        q,
        endpoint(),
        Format::Json,
        AuthorizationContext {
            receipt_id: "receipt".into(),
            expected_principal: "reader".into(),
        },
    )
    .unwrap();
    assert_eq!(frozen.query_id(), "ddl-1");
    assert_eq!(frozen.expected_principal(), "reader");
}
#[test]
fn bearer_tokens_follow_rfc6750_and_do_not_leak() {
    for t in ["", " ", "a:b", "a\"b", "x=middle", "é"] {
        assert!(Credentials::bearer(t).is_err(), "{t}");
    }
    let c = Config::new(endpoint(), Credentials::bearer("secret-token==").unwrap());
    assert!(!format!("{c:?}").contains("secret-token"));
}
#[test]
fn lifecycle_settings_cannot_override_configured_execution_contract() {
    for setting in [
        "log_comment",
        "log_queries",
        "log_queries_probability",
        "log_queries_min_query_duration_ms",
        "log_query_settings",
        "output_format_arrow_compression_method",
    ] {
        assert!(
            Query::new("SELECT 1", "reserved-1")
                .unwrap()
                .setting(setting, "0")
                .is_err(),
            "{setting}"
        );
    }
}

#[test]
fn client_rejects_impossible_resource_and_result_limits() {
    use antalya_rs::{ExecutionLimits, PoolOptions, ResourceLimits, TerminalPolicy};
    let config = || Config::new(endpoint(), Credentials::bearer("token").unwrap());
    for limits in [
        ResourceLimits {
            max_active_queries: 0,
            ..ResourceLimits::default()
        },
        ResourceLimits {
            max_control_queries: 0,
            ..ResourceLimits::default()
        },
        ResourceLimits {
            max_export_tasks: 0,
            ..ResourceLimits::default()
        },
        ResourceLimits {
            max_retained_bytes: usize::MAX,
            ..ResourceLimits::default()
        },
        ResourceLimits {
            max_active_queries: usize::MAX,
            ..ResourceLimits::default()
        },
    ] {
        assert!(matches!(
            AntalyaClient::new(config().resources(limits)),
            Err(antalya_rs::Error::InvalidSetting)
        ));
    }
    for limits in [
        ExecutionLimits {
            max_rows: 0,
            ..ExecutionLimits::default()
        },
        ExecutionLimits {
            max_batches: 0,
            ..ExecutionLimits::default()
        },
        ExecutionLimits {
            max_batch_bytes: 65 * 1024 * 1024,
            ..ExecutionLimits::default()
        },
        ExecutionLimits {
            max_total_bytes: 1,
            ..ExecutionLimits::default()
        },
    ] {
        assert!(matches!(
            AntalyaClient::new(config().execution_limits(limits)),
            Err(antalya_rs::Error::InvalidSetting)
        ));
    }
    assert!(
        AntalyaClient::new(config().pool(PoolOptions {
            idle_timeout: Duration::MAX,
            ..PoolOptions::default()
        }))
        .is_err()
    );
    assert!(
        AntalyaClient::new(config().terminal_policy(TerminalPolicy {
            initial_delay: Duration::ZERO,
            ..TerminalPolicy::default()
        }))
        .is_err()
    );
    assert!(
        AntalyaClient::new(config().terminal_policy(TerminalPolicy {
            initial_delay: Duration::from_secs(1),
            ..TerminalPolicy::default()
        }))
        .is_err()
    );
}

#[test]
fn zero_queue_and_disabled_idle_pool_are_valid() {
    use antalya_rs::{PoolOptions, ResourceLimits};
    let config = Config::new(endpoint(), Credentials::bearer("token").unwrap())
        .resources(ResourceLimits {
            max_queued_queries: 0,
            ..ResourceLimits::default()
        })
        .pool(PoolOptions {
            max_idle_per_host: 0,
            idle_timeout: Duration::ZERO,
        });
    assert!(AntalyaClient::new(config).is_ok());
}

#[test]
fn bearer_replacement_is_validated_redacted_and_preserves_existing_requests() {
    let base = AntalyaClient::new(
        Config::new(endpoint(), Credentials::bearer("original-secret").unwrap())
            .database("events")
            .unwrap(),
    )
    .unwrap();
    let query = Query::new("SELECT 1", "rotated-request").unwrap();
    let original = base.build_request(&query, Format::Json).unwrap();
    for invalid in ["", " ", "bad:token", "token\r\nheader"] {
        assert_eq!(
            base.with_bearer(invalid).unwrap_err(),
            antalya_rs::Error::InvalidCredentials
        );
    }
    let selected = base.with_bearer("rotated-secret==").unwrap();
    let request = selected.build_request(&query, Format::Json).unwrap();
    assert_eq!(
        request.headers()["Authorization"],
        "Bearer rotated-secret=="
    );
    assert!(request.headers()["Authorization"].is_sensitive());
    assert_eq!(
        original.headers()["Authorization"],
        "Bearer original-secret"
    );
    assert_eq!(
        base.build_request(&query, Format::Json).unwrap().headers()["Authorization"],
        "Bearer original-secret"
    );
    assert_eq!(original.url(), request.url());
    assert!(!format!("{selected:?}").contains("rotated-secret"));
}
