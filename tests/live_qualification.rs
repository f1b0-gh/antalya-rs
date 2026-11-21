//! Real-server qualification. Run with scripts/fixture.py; mocks cannot satisfy these tests.
#![cfg(feature = "arrow")]
use antalya_rs::{
    AntalyaClient, Config, Credentials, Endpoint, ExportOptions, Format, Outcome, Overwrite,
    Parameter, Query,
};
use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{DataType, TimeUnit};
use std::{io::Cursor, time::Duration};

fn client() -> AntalyaClient {
    AntalyaClient::new(
        Config::new(
            Endpoint::parse(&std::env::var("ANTALYA_TEST_URL").expect("run scripts/fixture.py"))
                .unwrap(),
            Credentials::bearer(&std::env::var("ANTALYA_TEST_TOKEN").unwrap()).unwrap(),
        )
        .timeout(Duration::from_secs(30)),
    )
    .unwrap()
}
fn query(sql: &str) -> Query {
    Query::new(sql, format!("live-{}", uuid::Uuid::new_v4()))
        .unwrap()
        .setting("output_format_arrow_string_as_string", "1")
        .unwrap()
        .setting("output_format_arrow_low_cardinality_as_dictionary", "1")
        .unwrap()
}
fn work() -> tempfile::TempDir {
    let root = std::path::Path::new("/tmp/antalya-rs");
    std::fs::create_dir_all(root).unwrap();
    assert!(!root.symlink_metadata().unwrap().file_type().is_symlink());
    tempfile::Builder::new()
        .prefix("live-test-")
        .tempdir_in(root)
        .unwrap()
}

#[tokio::test]
#[ignore = "requires the digest-pinned bearer fixture"]
async fn authorized_accounted_stream_checks_principal_limits_and_metrics() {
    use antalya_rs::{
        AuthorizationContext, AuthorizedRequest, Cause, ExecutionLimits, Phase, ResourceLimits,
    };
    let endpoint = Endpoint::parse(&std::env::var("ANTALYA_TEST_URL").unwrap()).unwrap();
    let client = AntalyaClient::new(
        Config::new(
            endpoint.clone(),
            Credentials::bearer(&std::env::var("ANTALYA_TEST_TOKEN").unwrap()).unwrap(),
        )
        .timeout(Duration::from_secs(30))
        .resources(ResourceLimits {
            max_active_queries: 1,
            max_queued_queries: 0,
            ..ResourceLimits::default()
        })
        .execution_limits(ExecutionLimits {
            max_rows: 3,
            ..ExecutionLimits::default()
        }),
    )
    .unwrap();
    let approved = |sql: &str, principal: &str| {
        AuthorizedRequest::new(
            query(sql),
            endpoint.clone(),
            Format::ArrowStream,
            AuthorizationContext {
                receipt_id: "live-hardening".into(),
                expected_principal: principal.into(),
            },
        )
        .unwrap()
    };
    let error = client
        .fetch_authorized_arrow(approved("SELECT number FROM numbers(2)", "wrong-principal"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.phase(), Some(Phase::Identity));
    assert_eq!(error.cause(), Some(Cause::IdentityMismatch));
    let mut oversized = client
        .fetch_authorized_arrow(approved("SELECT number FROM numbers(4)", "fixture"))
        .await
        .unwrap();
    assert_eq!(
        oversized.next_batch().await.unwrap_err().cause(),
        Some(Cause::LimitExceeded)
    );
    assert_eq!(oversized.outcome(), Outcome::Unknown);
    // A local limit failure released the single data slot even while Execution remains alive.
    let mut execution = client
        .fetch_authorized_arrow(approved("SELECT number FROM numbers(2)", "fixture"))
        .await
        .unwrap();
    let mut values = Vec::new();
    while let Some(batch) = execution.next_batch().await.unwrap() {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        values.extend(array.values().iter().copied());
        assert!(batch.charged_bytes() > 0);
    }
    assert_eq!(values, vec![0, 1]);
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    assert_eq!(execution.rows_delivered(), 2);
    let metrics = execution.metrics();
    assert!(metrics.identity > Duration::ZERO && metrics.terminal > Duration::ZERO);
    assert_eq!(metrics.flush_attempts, 1);
    assert!(metrics.control_requests >= 3);
    assert!(metrics.transfer.is_some() && metrics.decode.is_some());
    assert!(metrics.wire_bytes > 0);
}

#[tokio::test]
#[ignore = "requires the digest-pinned bearer fixture"]
async fn ipc_file_and_stream_preserve_schema_values_and_empty_results() {
    let client = client();
    let sql = r#"SELECT CAST(NULL, 'Nullable(Int64)') AS nullable, [toInt32(1),toInt32(2)] AS list, map('a',toInt32(7)) AS map, tuple(toUInt32(9),'x') AS tuple, toDecimal64('12.345',3) AS decimal, toDateTime64('2026-11-15 01:02:03.123456',6,'UTC') AS stamp, toLowCardinality('a') AS dictionary, dynamicElement(CAST(toInt64(7),'Dynamic'),'Int64') AS dynamic_int, toJSONString(CAST('{"a":1}','JSON')) AS json_text"#;
    for format in [Format::Arrow, Format::ArrowStream] {
        let work = work();
        let path = work.path().join("result.arrow");
        let report = client
            .export(query(sql), format, &path, ExportOptions::default())
            .await
            .unwrap();
        assert_eq!(report.outcome, Outcome::Succeeded);
        assert_eq!(report.rows, 1);
        let bytes = std::fs::read(path).unwrap();
        let batch = match format {
            Format::Arrow => arrow_ipc::reader::FileReader::try_new(Cursor::new(bytes), None)
                .unwrap()
                .next()
                .unwrap()
                .unwrap(),
            Format::ArrowStream => {
                arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
            }
            _ => unreachable!(),
        };
        eprintln!("{format:?}: {:?}", batch.schema());
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(0).is_null(0));
        let list = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap()
            .value(0);
        assert_eq!(
            list.as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[1, 2]
        );
        let map = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::MapArray>()
            .unwrap();
        assert_eq!(
            map.keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
        assert_eq!(
            map.values()
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .value(0),
            7
        );
        let tuple = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        assert_eq!(
            tuple
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::UInt32Array>()
                .unwrap()
                .value(0),
            9
        );
        assert_eq!(
            tuple
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "x"
        );
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .unwrap()
                .value(0),
            12345
        );
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1794704523123456
        );
        let dictionary = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>()
            .unwrap();
        assert_eq!(
            dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(dictionary.keys().value(0) as usize),
            "a"
        );

        assert!(matches!(
            batch.schema().field(1).data_type(),
            DataType::List(_)
        ));
        assert!(matches!(
            batch.schema().field(2).data_type(),
            DataType::Map(_, _)
        ));
        assert!(matches!(
            batch.schema().field(3).data_type(),
            DataType::Struct(_)
        ));
        assert_eq!(
            batch.schema().field(4).data_type(),
            &DataType::Decimal128(18, 3)
        );
        assert_eq!(
            batch.schema().field(5).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert!(matches!(
            batch.schema().field(6).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(
            batch
                .column(7)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
        assert_eq!(
            batch
                .column(8)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            r#"{"a":1}"#
        );
        let empty = client
            .export(
                query("SELECT toInt64(1) AS n WHERE 0"),
                format,
                work.path().join("empty.arrow"),
                ExportOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(empty.rows, 0);
        assert_eq!(empty.outcome, Outcome::Succeeded);
    }
}

#[tokio::test]
#[ignore = "requires the digest-pinned bearer fixture"]
async fn typed_parameters_match_both_transports_and_report_completion() {
    let client = client();
    let q = query("SELECT {s:String} AS s,{n:Int64} AS n,{u:UInt64} AS u,{b:Bool} AS b,{nil:Nullable(Int64)} AS nil")
        .parameter("s", Parameter::String("line\nquote'\\end".into())).unwrap()
        .parameter("n", Parameter::Int64(-7)).unwrap()
        .parameter("u", Parameter::UInt64(42)).unwrap()
        .parameter("b", Parameter::Bool(true)).unwrap()
        .parameter("nil", Parameter::Null).unwrap();
    let request = client.build_request(&q, Format::Json).unwrap();
    let response = reqwest::Client::new().execute(request).await.unwrap();
    assert!(response.status().is_success());
    let value: serde_json::Value =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(value["data"][0]["s"], "line\nquote'\\end");
    assert!(value["data"][0]["nil"].is_null());
    // Reused IDs must never confuse terminal evidence with the preceding request.
    let mut execution = client.fetch_arrow(q).await.unwrap();
    let batch = execution.next_batch().await.unwrap().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "line\nquote'\\end"
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -7
    );
    assert!(batch.column(4).is_null(0));
    assert!(execution.next_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    assert_eq!(execution.rows_delivered(), 1);
}

#[tokio::test]
#[ignore = "requires the digest-pinned bearer fixture"]
async fn cancellation_is_confirmed_and_failed_export_preserves_destination() {
    let client = client();
    let mut execution = client
        .fetch_arrow(query(
            "SELECT number,sleepEachRow(0.01) FROM numbers(100000) SETTINGS max_block_size=10",
        ))
        .await
        .unwrap();
    assert!(execution.next_batch().await.unwrap().is_some());
    assert_eq!(execution.cancel().await.unwrap(), Outcome::Cancelled);
    let work = work();
    let dest = work.path().join("keep.arrow");
    std::fs::write(&dest, b"previous artifact").unwrap();
    let result = client
        .export(
            query("SELECT number FROM numbers(10000)"),
            Format::ArrowStream,
            &dest,
            ExportOptions {
                overwrite: Overwrite::Replace,
                max_bytes: 128,
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(std::fs::read(&dest).unwrap(), b"previous artifact");
}

#[tokio::test]
#[ignore = "requires the digest-pinned bearer fixture"]
async fn capabilities_and_generated_ddl_parse_on_pinned_server() {
    use antalya_rs::sql::{Catalog, ExportPart, Hybrid, Identifier, PartitionBy, TableName};
    let client = client();
    let capabilities = client.capabilities().await.unwrap();
    assert_eq!(capabilities.version(), antalya_rs::QUALIFICATION_VERSION);
    let ddl = [
        Catalog::new(
            Identifier::new("catalog_test").unwrap(),
            Endpoint::parse("http://127.0.0.1:9/").unwrap(),
            "warehouse",
            Credentials::bearer("catalog-test").unwrap(),
        )
        .unwrap()
        .query(&capabilities, "ddl-catalog")
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
        .query(&capabilities, "ddl-hybrid")
        .unwrap(),
        ExportPart::new(
            TableName::new("default", "hot").unwrap(),
            "all_1_1_0",
            "http://127.0.0.1:9/export/{_partition_id}.parquet",
            PartitionBy::YearMonth(Identifier::new("ts").unwrap()),
        )
        .unwrap()
        .query(&capabilities, "ddl-export")
        .unwrap(),
    ];
    for query in ddl {
        // Syntax qualification only: EXPLAIN AST never contacts a catalog/object store.
        let mut request = client.build_command_request(&query).unwrap();
        let sql = std::str::from_utf8(request.body().unwrap().as_bytes().unwrap()).unwrap();
        let explain = format!("EXPLAIN AST {sql}");
        *request.body_mut() = Some(explain.into());
        let response = reqwest::Client::new().execute(request).await.unwrap();
        let status = response.status();
        let text = response.text().await.unwrap();
        assert!(
            status.is_success(),
            "DDL parser failed for {}: {text}",
            query.query_id()
        );
        assert!(text.contains("Query"));
    }
}
