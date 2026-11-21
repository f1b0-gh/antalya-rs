#![cfg(feature = "arrow")]
#![forbid(unsafe_code)]
mod support;
use antalya_rs::{
    AdmittedArrow, AntalyaClient, AuthorizationContext, AuthorizedRequest, Config, Credentials,
    DecodeError, ExecutionLimits, Format, Outcome, Query,
};
use std::{io, time::Duration};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn admit(server: &support::Server, limits: ExecutionLimits) -> Result<AdmittedArrow> {
    admit_with_timeout(server, limits, Duration::from_secs(2)).await
}
async fn admit_with_timeout(
    server: &support::Server,
    limits: ExecutionLimits,
    timeout: Duration,
) -> Result<AdmittedArrow> {
    let client = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
            .timeout(timeout)
            .execution_limits(limits),
    )?;
    let request = AuthorizedRequest::new(
        Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
        server.endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "decode-test".into(),
            expected_principal: "fixture".into(),
        },
    )?;
    Ok(client
        .prepare_authorized_arrow(request)
        .await?
        .ready_transport()
        .await?
        .admit_arrow()?)
}

#[tokio::test]
async fn separately_observed_completion_is_bound_to_original_receipt_and_eof() -> Result<()> {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    let receipt = stream.recovery_receipt()?;
    drop(stream.next_batch().await?);
    let control = AntalyaClient::new(Config::new(
        server.endpoint.clone(),
        Credentials::bearer("token")?,
    ))?;
    let observed = control.observe_terminal(&receipt).await?;
    assert_eq!(
        observed.termination(),
        antalya_rs::RemoteTermination::Succeeded
    );
    assert_eq!(observed.receipt().marker(), receipt.marker());
    assert_eq!(observed.progress().and_then(|p| p.result_rows), Some(2));
    assert!(matches!(
        stream.accept_completion(&observed),
        Err(DecodeError::Incomplete)
    ));
    // Early termination proof does not finish consuming the response.
    stream.accept_termination(&observed)?;
    assert_eq!(stream.outcome(), Outcome::Running);
    assert!(stream.next_batch().await?.is_none());
    let unrelated = antalya_rs::RecoveryReceipt::restore(
        server.endpoint.clone(),
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
        "fixture".into(),
    )?;
    // This synthetic fixture reports fixed counters; it lets us construct a
    // valid observation of another identity to test local binding rejection.
    let foreign = control.observe_terminal(&unrelated).await?;
    let calls = server
        .requests
        .lock()
        .map_err(|_| io::Error::other("request lock"))?
        .len();
    assert!(matches!(
        stream.accept_completion(&foreign),
        Err(DecodeError::Ipc(antalya_rs::IpcError::Transport(
            antalya_rs::TransportError::ObservationMismatch
        )))
    ));
    assert_eq!(stream.outcome(), Outcome::Running);
    stream.accept_completion(&observed)?;
    assert_eq!(stream.outcome(), Outcome::Succeeded);
    assert_eq!(
        server
            .requests
            .lock()
            .map_err(|_| io::Error::other("request lock"))?
            .len(),
        calls
    );
    Ok(())
}

#[tokio::test]
async fn separate_terminal_proof_cannot_validate_missing_rows_or_discarded_data() -> Result<()> {
    let server = support::Server::new(support::stream(1), 4, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    let receipt = stream.recovery_receipt()?;
    while stream.next_batch().await?.is_some() {}
    let control = AntalyaClient::new(Config::new(
        server.endpoint.clone(),
        Credentials::bearer("token")?,
    ))?;
    let observed = control.observe_terminal(&receipt).await?;
    assert!(matches!(
        stream.accept_completion(&observed),
        Err(DecodeError::Completion)
    ));
    assert_eq!(stream.outcome(), Outcome::Unknown);
    stream.accept_termination(&observed)?;
    assert!(matches!(
        stream.accept_completion(&observed),
        Err(DecodeError::Poisoned)
    ));
    Ok(())
}

#[tokio::test]
async fn separate_observation_does_not_extend_original_completion_deadline() -> Result<()> {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let mut stream = admit_with_timeout(
        &server,
        ExecutionLimits::default(),
        Duration::from_millis(400),
    )
    .await?;
    let receipt = stream.recovery_receipt()?;
    while stream.next_batch().await?.is_some() {}
    tokio::time::sleep(Duration::from_millis(450)).await;
    let control = AntalyaClient::new(Config::new(
        server.endpoint.clone(),
        Credentials::bearer("token")?,
    ))?;
    let observed = control.observe_terminal(&receipt).await?;
    assert!(matches!(
        stream.accept_completion(&observed),
        Err(DecodeError::Ipc(antalya_rs::IpcError::Transport(
            antalya_rs::TransportError::Deadline
        )))
    ));
    assert_eq!(stream.outcome(), Outcome::Unknown);
    assert_eq!(
        stream.accept_termination(&observed)?,
        antalya_rs::RemoteTermination::Succeeded
    );
    Ok(())
}

#[tokio::test]
async fn terminal_observation_binds_each_receipt_component() -> Result<()> {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let other = support::Server::new(support::stream(1), 2, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    let original = stream.recovery_receipt()?;
    while stream.next_batch().await?.is_some() {}
    for component in 0..4 {
        let endpoint = if component == 0 {
            &other.endpoint
        } else {
            &server.endpoint
        };
        let receipt = antalya_rs::RecoveryReceipt::restore(
            endpoint.clone(),
            if component == 1 {
                uuid::Uuid::new_v4().to_string()
            } else {
                original.query_id().to_owned()
            },
            if component == 2 {
                uuid::Uuid::new_v4().to_string()
            } else {
                original.marker().to_owned()
            },
            if component == 3 {
                "foreign"
            } else {
                original.principal()
            }
            .to_owned(),
        )?;
        let control =
            AntalyaClient::new(Config::new(endpoint.clone(), Credentials::bearer("token")?))?;
        // Fixed query-log fixture permits independently varying each identity.
        let observed = control.observe_terminal(&receipt).await?;
        assert!(matches!(
            stream.accept_termination(&observed),
            Err(DecodeError::Ipc(antalya_rs::IpcError::Transport(
                antalya_rs::TransportError::ObservationMismatch
            )))
        ));
        assert_eq!(stream.outcome(), Outcome::Running);
    }
    stream.confirm_completion().await?;
    Ok(())
}

#[tokio::test]
async fn explicit_cancel_discards_output_even_if_remote_query_already_succeeded() -> Result<()> {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    drop(stream.next_batch().await?);
    let receipt = stream.recovery_receipt()?;
    let schema = stream
        .shared_schema()
        .ok_or_else(|| io::Error::other("missing schema"))?;
    assert!(stream.schema_bytes() > 0);
    assert!(std::ptr::eq(
        schema.as_ref(),
        stream
            .schema()
            .ok_or_else(|| io::Error::other("missing schema"))?
    ));
    // The fixture does not implement KILL; the failed explicit control still
    // invalidates delivery before waiting, and original observation can recover.
    assert!(stream.cancel().await.is_err());
    assert_eq!(stream.outcome(), Outcome::Unknown);
    assert!(matches!(
        stream.next_batch().await,
        Err(DecodeError::Poisoned)
    ));
    assert!(matches!(
        stream.confirm_completion().await,
        Err(DecodeError::Poisoned)
    ));
    assert_eq!(stream.recovery_receipt()?.marker(), receipt.marker());
    assert_eq!(
        stream.confirm_termination().await?,
        antalya_rs::RemoteTermination::Succeeded
    );
    assert_eq!(stream.outcome(), Outcome::Unknown);
    Ok(())
}

#[tokio::test]
async fn decoded_batches_remain_provisional_until_explicit_row_proof() -> Result<()> {
    for batches in [0, 2] {
        let server = support::Server::new(support::stream(batches), 2 * batches as u64, false, 200);
        let mut stream = admit(&server, ExecutionLimits::default()).await?;
        let original = stream.recovery_receipt()?;
        assert!(stream.confirm_completion().await.is_err());
        let mut count = 0;
        while let Some(batch) = stream.next_batch().await? {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .ok_or_else(|| io::Error::other("wrong array type"))?;
            assert_eq!(array.values().as_ref(), &[1, 2]);
            assert!(batch.charged_bytes() >= batch.get_array_memory_size());
            count += batch.num_rows();
        }
        assert_eq!(count, batches * 2);
        assert_eq!(stream.rows_decoded(), count as u64);
        assert!(stream.schema().is_some());
        assert_eq!(stream.outcome(), Outcome::Running);
        assert_eq!(stream.recovery_receipt()?.marker(), original.marker());
        assert_eq!(
            server
                .requests
                .lock()
                .map_err(|_| io::Error::other("request lock"))?
                .len(),
            2
        );
        stream.confirm_completion().await?;
        assert_eq!(stream.outcome(), Outcome::Succeeded);
    }
    Ok(())
}

#[tokio::test]
async fn missing_whole_batch_and_decoded_limit_cannot_be_success() -> Result<()> {
    let server = support::Server::new(support::stream(1), 4, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    drop(stream.next_batch().await?);
    assert!(stream.next_batch().await?.is_none());
    assert!(matches!(
        stream.confirm_completion().await,
        Err(DecodeError::Completion)
    ));
    assert_eq!(stream.outcome(), Outcome::Unknown);
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let mut stream = admit(
        &server,
        ExecutionLimits {
            max_batch_bytes: 128,
            ..Default::default()
        },
    )
    .await?;
    assert!(matches!(stream.next_batch().await, Err(DecodeError::Limit)));
    assert!(matches!(
        stream.next_batch().await,
        Err(DecodeError::Poisoned)
    ));
    assert_eq!(
        server
            .requests
            .lock()
            .map_err(|_| io::Error::other("request lock"))?
            .len(),
        2
    );
    Ok(())
}

fn schema_wire(
    bit_width: i32,
    dictionary_kind: Option<i16>,
    feature: Option<i64>,
) -> Result<Vec<u8>> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let name = builder.create_string("bad");
    let int = arrow_ipc::Int::create(
        &mut builder,
        &arrow_ipc::IntArgs {
            bitWidth: bit_width,
            is_signed: true,
        },
    );
    let dictionary = dictionary_kind.map(|kind| {
        arrow_ipc::DictionaryEncoding::create(
            &mut builder,
            &arrow_ipc::DictionaryEncodingArgs {
                id: 7,
                indexType: Some(int),
                dictionaryKind: arrow_ipc::DictionaryKind(kind),
                ..Default::default()
            },
        )
    });
    let field = arrow_ipc::Field::create(
        &mut builder,
        &arrow_ipc::FieldArgs {
            name: Some(name),
            type_type: arrow_ipc::Type::Int,
            type_: Some(int.as_union_value()),
            dictionary,
            ..Default::default()
        },
    );
    let fields = builder.create_vector(&[field]);
    let features = feature.map(|value| builder.create_vector(&[arrow_ipc::Feature(value)]));
    let schema = arrow_ipc::Schema::create(
        &mut builder,
        &arrow_ipc::SchemaArgs {
            fields: Some(fields),
            features,
            ..Default::default()
        },
    );
    let message = arrow_ipc::Message::create(
        &mut builder,
        &arrow_ipc::MessageArgs {
            version: arrow_ipc::MetadataVersion::V5,
            header_type: arrow_ipc::MessageHeader::Schema,
            header: Some(schema.as_union_value()),
            ..Default::default()
        },
    );
    builder.finish(message, None);
    let mut wire = vec![255; 4];
    wire.extend(u32::try_from(builder.finished_data().len())?.to_le_bytes());
    wire.extend(builder.finished_data());
    wire.extend([255; 4]);
    wire.extend([0; 4]);
    Ok(wire)
}

#[tokio::test]
async fn invalid_schema_is_typed_refusal_before_upstream_conversion() -> Result<()> {
    let server = support::Server::new(schema_wire(7, None, None)?, 0, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    assert!(matches!(
        stream.next_batch().await,
        Err(DecodeError::Schema)
    ));
    assert_eq!(stream.outcome(), Outcome::Unknown);
    Ok(())
}

#[tokio::test]
async fn control_failure_preserves_the_typed_client_cause() -> Result<()> {
    let server = support::Server::new(support::stream(0), 0, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    assert!(stream.next_batch().await?.is_none());
    // No terminal endpoint remains available after the data response completes.
    drop(server);
    let error = stream
        .confirm_completion()
        .await
        .err()
        .ok_or_else(|| io::Error::other("expected refusal"))?;
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(stream.outcome(), Outcome::Unknown);
    Ok(())
}

#[tokio::test]
async fn invalid_utf8_preserves_arrow_cause_and_extra_variadic_counts_refuse() -> Result<()> {
    use std::sync::Arc;
    let text = "unique-payload-to-corrupt";
    let batch = arrow_array::RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(arrow_array::StringArray::from(vec![text])) as arrow_array::ArrayRef,
    )])?;
    let mut wire = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut wire, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    let offset = wire
        .windows(text.len())
        .position(|bytes| bytes == text.as_bytes())
        .ok_or_else(|| io::Error::other("payload"))?;
    wire[offset] = 255;
    let server = support::Server::new(wire, 1, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    let error = stream
        .next_batch()
        .await
        .err()
        .ok_or_else(|| io::Error::other("bad UTF8"))?;
    assert!(matches!(error, DecodeError::Arrow(_)));
    assert!(std::error::Error::source(&error).is_some());
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let nodes = builder.create_vector(&[arrow_ipc::FieldNode::new(2, 0)]);
    let buffers =
        builder.create_vector(&[arrow_ipc::Buffer::new(0, 0), arrow_ipc::Buffer::new(0, 8)]);
    let extra = builder.create_vector(&[0_i64]);
    let batch = arrow_ipc::RecordBatch::create(
        &mut builder,
        &arrow_ipc::RecordBatchArgs {
            length: 2,
            nodes: Some(nodes),
            buffers: Some(buffers),
            variadicBufferCounts: Some(extra),
            ..Default::default()
        },
    );
    let message = arrow_ipc::Message::create(
        &mut builder,
        &arrow_ipc::MessageArgs {
            version: arrow_ipc::MetadataVersion::V5,
            header_type: arrow_ipc::MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: 8,
            ..Default::default()
        },
    );
    builder.finish(message, None);
    let mut wire = support::stream(0);
    wire.truncate(wire.len() - 8);
    wire.extend([255; 4]);
    wire.extend(u32::try_from(builder.finished_data().len())?.to_le_bytes());
    wire.extend(builder.finished_data());
    wire.extend(1_i32.to_le_bytes());
    wire.extend(2_i32.to_le_bytes());
    let server = support::Server::new(wire, 2, false, 200);
    let mut stream = admit(&server, ExecutionLimits::default()).await?;
    assert!(matches!(
        stream.next_batch().await,
        Err(DecodeError::Layout)
    ));
    Ok(())
}

#[tokio::test]
async fn dictionary_replacements_and_deltas_preserve_original_values() -> Result<()> {
    use arrow_array::{DictionaryArray, RecordBatch, types::Int32Type};
    use arrow_ipc::writer::{DictionaryHandling, IpcWriteOptions, StreamWriter};
    use std::sync::Arc;
    for handling in [DictionaryHandling::Resend, DictionaryHandling::Delta] {
        let first: DictionaryArray<Int32Type> = ["a", "b"].into_iter().collect();
        let second: DictionaryArray<Int32Type> = ["a", "b", "c"].into_iter().collect();
        let first =
            RecordBatch::try_from_iter(vec![("v", Arc::new(first) as arrow_array::ArrayRef)])?;
        let second =
            RecordBatch::try_from_iter(vec![("v", Arc::new(second) as arrow_array::ArrayRef)])?;
        let mut wire = Vec::new();
        let mut writer = StreamWriter::try_new_with_options(
            &mut wire,
            &first.schema(),
            IpcWriteOptions::default().with_dictionary_handling(handling),
        )?;
        writer.write(&first)?;
        writer.write(&second)?;
        writer.finish()?;
        drop(writer);
        let server = support::Server::new(wire, 5, false, 200);
        let mut stream = admit(&server, ExecutionLimits::default()).await?;
        let first = stream
            .next_batch()
            .await?
            .ok_or_else(|| io::Error::other("first dictionary"))?;
        let second = stream
            .next_batch()
            .await?
            .ok_or_else(|| io::Error::other("second dictionary"))?;
        // The first retained batch must keep its own dictionary after a replacement.
        for (batch, expected) in [(&first, vec!["a", "b"]), (&second, vec!["a", "b", "c"])] {
            let dictionary = batch
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .ok_or_else(|| io::Error::other("dictionary type"))?;
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| io::Error::other("value type"))?;
            let actual = dictionary
                .keys_iter()
                .map(|index| index.map(|index| values.value(index)))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected.into_iter().map(Some).collect::<Vec<_>>());
        }
        drop((first, second));
        assert!(stream.next_batch().await?.is_none());
        stream.confirm_completion().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the digest-pinned bearer fixture"]
async fn live_expired_data_credential_uses_separately_authorized_completion() -> Result<()> {
    let endpoint = antalya_rs::Endpoint::parse(&std::env::var("ANTALYA_TEST_URL")?)?;
    // Fixed synthetic fixture signer, without adding a crypto implementation or
    // accepting algorithm choices in the client. Captured output is never logged.
    let signed = std::process::Command::new("python3")
        .args(["-c", "import json, os, runpy, sys, time; f=runpy.run_path(sys.argv[1]); e=int(time.time())+8; print(json.dumps({'token':f['token'](os.environ['ANTALYA_TEST_SIGNING_KEY'],expires=e),'expires':e}))"])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/scripts/fixture.py"))
        .output()?;
    if !signed.status.success() {
        return Err(io::Error::other("fixture signer failed").into());
    }
    let value: serde_json::Value = serde_json::from_slice(&signed.stdout)?;
    let token = value["token"]
        .as_str()
        .ok_or_else(|| io::Error::other("fixture token"))?;
    let expires = value["expires"]
        .as_u64()
        .ok_or_else(|| io::Error::other("fixture expiry"))?;
    let profile = || {
        antalya_rs::JwtReceiverProfile::new(
            endpoint.clone(),
            "antalya-rs-fixture",
            "antalya-rs",
            "fixture",
            0,
        )
    };
    // Claims alone never authenticate: keep the metadata intact, corrupt only
    // the signature, and require the real receiver to refuse the preflight.
    let (claims, _) = token
        .rsplit_once('.')
        .ok_or_else(|| io::Error::other("fixture token shape"))?;
    let corrupt = format!("{claims}.{}", "A".repeat(43));
    let base = AntalyaClient::new(
        Config::new(
            endpoint.clone(),
            Credentials::bearer(&std::env::var("ANTALYA_TEST_TOKEN")?)?,
        )
        .timeout(Duration::from_secs(30)),
    )?;
    let forged = base.with_jwt_receiver(profile()?, &corrupt)?;
    let identity = forged
        .prepare_authorized_arrow(AuthorizedRequest::new(
            Query::new(
                "SELECT number FROM numbers(2)",
                uuid::Uuid::new_v4().to_string(),
            )?,
            endpoint.clone(),
            Format::ArrowStream,
            AuthorizationContext {
                receipt_id: "forged-data".into(),
                expected_principal: "fixture".into(),
            },
        )?)
        .await;
    assert!(matches!(
        identity,
        Err(antalya_rs::Error::Context {
            cause: antalya_rs::Cause::HttpStatus(401 | 403),
            ..
        })
    ));
    let data = base.with_jwt_receiver(profile()?, token)?;
    let request = AuthorizedRequest::new(
        Query::new(
            "SELECT number FROM numbers(2)",
            uuid::Uuid::new_v4().to_string(),
        )?,
        endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "expired-data".into(),
            expected_principal: "fixture".into(),
        },
    )?;
    let mut stream = data
        .prepare_authorized_arrow(request)
        .await?
        .ready_transport()
        .await?
        .admit_arrow_before(expires + 1)?;
    let receipt = stream.recovery_receipt()?;
    while stream.next_batch().await?.is_some() {}
    assert_eq!(stream.rows_decoded(), 2);
    // Preparing a connection while a credential is valid must not turn it into
    // an authenticated session that admits another request after token expiry.
    let delayed = data
        .prepare_authorized_arrow(AuthorizedRequest::new(
            Query::new(
                "SELECT number FROM numbers(3)",
                uuid::Uuid::new_v4().to_string(),
            )?,
            endpoint.clone(),
            Format::ArrowStream,
            AuthorizationContext {
                receipt_id: "expired-delayed-admission".into(),
                expected_principal: "fixture".into(),
            },
        )?)
        .await?
        .ready_transport()
        .await?;
    let refused_receipt = delayed.recovery_receipt()?;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    let after_expiry = Duration::from_secs(expires) + Duration::from_millis(250);
    tokio::time::sleep(after_expiry.saturating_sub(now)).await;
    let mut refused = delayed.admit_arrow()?;
    let response = refused.next_batch().await;
    assert!(
        matches!(
            response,
            Err(DecodeError::Ipc(antalya_rs::IpcError::Transport(
                antalya_rs::TransportError::HttpStatus(401 | 403)
            )))
        ),
        "unexpected delayed response: {response:?}"
    );
    // A transport refusal is not a permanent non-application tombstone; the
    // local owner remains uncertain and cannot authorize replay or publication.
    assert_eq!(refused.outcome(), Outcome::Unknown);
    // Use the standalone observation API so this intentionally refused control
    // cannot poison the retained response. It must retain the HTTP cause.
    assert!(matches!(
        data.observe_terminal(&receipt).await,
        Err(antalya_rs::Error::Context {
            cause: antalya_rs::Cause::HttpStatus(401 | 403),
            ..
        })
    ));
    // Same pools and resource lanes; the original control view was not mutated.
    let control = base;
    let observed = control.observe_terminal(&receipt).await?;
    stream.accept_completion(&observed)?;
    assert_eq!(stream.outcome(), Outcome::Succeeded);
    let mut log = control
        .fetch_arrow(
            Query::new(
                "SELECT count() AS n FROM system.query_log WHERE query_id={id:String}",
                uuid::Uuid::new_v4().to_string(),
            )?
            .typed_parameter(
                "id",
                antalya_rs::Parameter::String(refused_receipt.query_id().to_owned()),
            )?,
        )
        .await?;
    let row = log
        .next_batch()
        .await?
        .ok_or_else(|| io::Error::other("missing query-log count"))?;
    let count = row
        .column_by_name("n")
        .and_then(|a| a.as_any().downcast_ref::<arrow_array::UInt64Array>())
        .ok_or_else(|| io::Error::other("invalid query-log count"))?;
    assert_eq!(count.len(), 1);
    assert_eq!(count.value(0), 0);
    assert!(log.next_batch().await?.is_none());
    assert_eq!(log.outcome(), Outcome::Succeeded);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the digest-pinned bearer fixture"]
async fn live_decoding_verifies_empty_large_nested_and_dictionary_queries() -> Result<()> {
    let endpoint = antalya_rs::Endpoint::parse(&std::env::var("ANTALYA_TEST_URL")?)?;
    let client = AntalyaClient::new(
        Config::new(
            endpoint.clone(),
            Credentials::bearer(&std::env::var("ANTALYA_TEST_TOKEN")?)?,
        )
        .timeout(Duration::from_secs(30)),
    )?;
    let cases = [
        ("SELECT number FROM numbers(0)", 0, false),
        ("SELECT number FROM numbers(100000)", 100_000, false),
        (
            "SELECT toLowCardinality(toString(number % 3)) AS value FROM numbers(20)",
            20,
            true,
        ),
        (
            "SELECT [toInt32(number), toInt32(number + 1)] AS items, tuple(toString(number), toUInt64(number)) AS pair, map('k', toUInt64(number)) AS attrs, toDecimal128('1.25', 2) AS amount, toDateTime64(0, 3, 'UTC') AS at FROM numbers(20)",
            20,
            false,
        ),
    ];
    for (index, (sql, expected_rows, dictionary)) in cases.into_iter().enumerate() {
        let query = Query::new(sql, uuid::Uuid::new_v4().to_string())?
            .setting("max_block_size", "8192")?
            .setting(
                "output_format_arrow_low_cardinality_as_dictionary",
                if dictionary { "1" } else { "0" },
            )?;
        let request = AuthorizedRequest::new(
            query,
            endpoint.clone(),
            Format::ArrowStream,
            AuthorizationContext {
                receipt_id: "live-decoder".into(),
                expected_principal: "fixture".into(),
            },
        )?;
        let ready = client
            .prepare_authorized_arrow(request)
            .await?
            .ready_transport()
            .await?;
        let receipt = ready.recovery_receipt()?;
        let mut stream = ready.admit_arrow()?;
        let mut rows = 0_u64;
        let mut batches = 0;
        while let Some(batch) = stream.next_batch().await? {
            batches += 1;
            for row in 0..batch.num_rows() {
                match index {
                    0 | 1 => {
                        let values = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow_array::UInt64Array>()
                            .ok_or_else(|| io::Error::other("number type"))?;
                        assert_eq!(values.value(row), rows);
                    }
                    2 => assert_eq!(
                        dictionary_text(batch.column(0).as_ref(), row)?,
                        (rows % 3).to_string()
                    ),
                    _ => {
                        let list = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow_array::ListArray>()
                            .ok_or_else(|| io::Error::other("list type"))?;
                        let child = list.value(row);
                        let child = child
                            .as_any()
                            .downcast_ref::<arrow_array::Int32Array>()
                            .ok_or_else(|| io::Error::other("list child type"))?;
                        assert_eq!(
                            child.values().as_ref(),
                            &[i32::try_from(rows)?, i32::try_from(rows + 1)?]
                        );
                        let amount = batch
                            .column(3)
                            .as_any()
                            .downcast_ref::<arrow_array::Decimal128Array>()
                            .ok_or_else(|| io::Error::other("decimal type"))?;
                        assert_eq!(amount.value(row), 125);
                        let time = batch
                            .column(4)
                            .as_any()
                            .downcast_ref::<arrow_array::TimestampMillisecondArray>()
                            .ok_or_else(|| io::Error::other("timestamp type"))?;
                        assert_eq!(time.value(row), 0);
                    }
                }
                rows += 1;
            }
        }
        assert_eq!(rows, expected_rows);
        if index == 1 {
            assert!(batches > 1);
        }
        assert_eq!(stream.outcome(), Outcome::Running);
        assert_eq!(stream.recovery_receipt()?.marker(), receipt.marker());
        stream.confirm_completion().await?;
        assert_eq!(stream.outcome(), Outcome::Succeeded);
    }
    Ok(())
}
fn dictionary_text(array: &dyn arrow_array::Array, row: usize) -> Result<&str> {
    use arrow_array::types::*;
    macro_rules! value {
        ($kind:ty) => {{
            let dictionary = array
                .as_any()
                .downcast_ref::<arrow_array::DictionaryArray<$kind>>()
                .ok_or_else(|| io::Error::other("dictionary type"))?;
            let index = dictionary
                .key(row)
                .ok_or_else(|| io::Error::other("null dictionary key"))?;
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| io::Error::other("dictionary string type"))?;
            Ok(values.value(index))
        }};
    }
    use arrow_schema::DataType;
    let DataType::Dictionary(key, _) = array.data_type() else {
        return Err(io::Error::other("missing dictionary").into());
    };
    match key.as_ref() {
        DataType::Int8 => value!(Int8Type),
        DataType::Int16 => value!(Int16Type),
        DataType::Int32 => value!(Int32Type),
        DataType::Int64 => value!(Int64Type),
        DataType::UInt8 => value!(UInt8Type),
        DataType::UInt16 => value!(UInt16Type),
        DataType::UInt32 => value!(UInt32Type),
        DataType::UInt64 => value!(UInt64Type),
        _ => Err(io::Error::other("unsupported dictionary key").into()),
    }
}

#[tokio::test]
async fn decoded_batch_clones_hold_capacity_and_cancelled_decode_wait_resumes() -> Result<()> {
    use std::sync::Arc;
    let batch = arrow_array::RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(arrow_array::UInt64Array::from_iter_values(0..65_536)) as arrow_array::ArrayRef,
    )])?;
    let mut wire = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut wire, &batch.schema())?;
    writer.write(&batch)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    let server = support::Server::new(wire, 131_072, false, 200);
    let client = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
            .timeout(Duration::from_secs(2))
            .resources(antalya_rs::ResourceLimits {
                max_retained_bytes: 2 * 1024 * 1024 + 2 * 64 * 1024 + 8,
                ..Default::default()
            })
            .execution_limits(ExecutionLimits {
                max_batch_bytes: 1024 * 1024,
                ..Default::default()
            }),
    )?;
    let request = AuthorizedRequest::new(
        Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
        server.endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "decode-capacity".into(),
            expected_principal: "fixture".into(),
        },
    )?;
    let mut stream = client
        .prepare_authorized_arrow(request)
        .await?
        .ready_transport()
        .await?
        .admit_arrow()?;
    let first = stream
        .next_batch()
        .await?
        .ok_or_else(|| io::Error::other("first batch"))?;
    let retained = first.clone();
    drop(first);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.next_batch())
            .await
            .is_err()
    );
    assert_eq!(retained.num_rows(), 65_536);
    drop(retained);
    let second = stream
        .next_batch()
        .await?
        .ok_or_else(|| io::Error::other("second batch"))?;
    assert_eq!(second.num_rows(), 65_536);
    drop(second);
    assert!(stream.next_batch().await?.is_none());
    assert_eq!(stream.rows_decoded(), 131_072);
    assert_eq!(
        server
            .requests
            .lock()
            .map_err(|_| io::Error::other("request lock"))?
            .len(),
        2
    );
    stream.confirm_completion().await?;
    Ok(())
}

#[tokio::test]
async fn unknown_dictionary_encoding_and_schema_features_are_refused() -> Result<()> {
    for (dictionary, feature) in [(Some(99), None), (None, Some(99))] {
        let server = support::Server::new(schema_wire(32, dictionary, feature)?, 0, false, 200);
        let mut stream = admit(&server, ExecutionLimits::default()).await?;
        assert!(matches!(
            stream.next_batch().await,
            Err(DecodeError::Schema)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn logical_null_length_is_bounded_before_possible_bitmap_allocation() -> Result<()> {
    use std::sync::Arc;
    let batch = arrow_array::RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(arrow_array::NullArray::new(8193)) as arrow_array::ArrayRef,
    )])?;
    let mut wire = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut wire, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    let server = support::Server::new(wire, 8193, false, 200);
    let mut stream = admit(
        &server,
        ExecutionLimits {
            max_batch_bytes: 1024,
            ..Default::default()
        },
    )
    .await?;
    assert!(matches!(stream.next_batch().await, Err(DecodeError::Limit)));
    Ok(())
}

#[tokio::test]
async fn expired_completion_is_unknown_without_renewing_the_control_deadline() -> Result<()> {
    let server = support::Server::new(support::stream(0), 0, false, 200);
    let mut stream = admit_with_timeout(
        &server,
        ExecutionLimits::default(),
        Duration::from_millis(250),
    )
    .await?;
    assert!(stream.next_batch().await?.is_none());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(stream.confirm_completion().await.is_err());
    assert_eq!(stream.outcome(), Outcome::Unknown);
    assert_eq!(
        server
            .requests
            .lock()
            .map_err(|_| io::Error::other("request lock"))?
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn dictionary_working_set_that_cannot_fit_refuses_capacity_without_waiting_for_deadline()
-> Result<()> {
    use arrow_array::{ArrayRef, DictionaryArray, Int8Array, StringArray, types::Int8Type};
    use std::sync::Arc;
    let values = Arc::new(StringArray::from(vec!["x".repeat(512 * 1024)]));
    let dictionary = DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), values)?;
    let batch =
        arrow_array::RecordBatch::try_from_iter(vec![("v", Arc::new(dictionary) as ArrayRef)])?;
    let mut wire = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut wire, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    let server = support::Server::new(wire, 1, false, 200);
    let client = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
            .timeout(Duration::from_secs(5))
            .resources(antalya_rs::ResourceLimits {
                max_retained_bytes: 2 * 1024 * 1024 + 2 * 64 * 1024 + 8,
                ..Default::default()
            })
            .execution_limits(ExecutionLimits {
                max_batch_bytes: 1024 * 1024,
                ..Default::default()
            }),
    )?;
    let request = AuthorizedRequest::new(
        Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
        server.endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "dictionary-workspace".into(),
            expected_principal: "fixture".into(),
        },
    )?;
    let mut stream = client
        .prepare_authorized_arrow(request)
        .await?
        .ready_transport()
        .await?
        .admit_arrow()?;
    let result = tokio::time::timeout(Duration::from_secs(2), stream.next_batch()).await;
    assert!(
        matches!(
            result,
            Ok(Err(DecodeError::Ipc(antalya_rs::IpcError::Transport(
                antalya_rs::TransportError::Client(antalya_rs::Error::Context {
                    cause: antalya_rs::Cause::Capacity,
                    ..
                })
            ))))
        ),
        "impossible internally-held working set must refuse capacity, got {result:?}"
    );
    assert_eq!(stream.outcome(), Outcome::Unknown);
    assert!(matches!(
        stream.next_batch().await,
        Err(DecodeError::Poisoned)
    ));
    assert_eq!(
        server
            .requests
            .lock()
            .map_err(|_| io::Error::other("requests"))?
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn delta_releases_internal_ownership_of_backing_retained_only_by_old_output() -> Result<()> {
    use arrow_array::{
        ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray, types::Int32Type,
    };
    use arrow_ipc::writer::{DictionaryHandling, IpcWriteOptions, StreamWriter};
    use std::sync::Arc;
    let strings = [
        "a".repeat(128 * 1024),
        "b".repeat(128 * 1024),
        "c".repeat(256 * 1024),
    ];
    let mut batches = Vec::new();
    for n in 1..=3 {
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from_iter_values(0..i32::try_from(n)?),
            Arc::new(StringArray::from_iter_values(strings[..n].iter())),
        )?;
        batches.push(RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(dict) as ArrayRef,
        )])?);
    }
    let mut wire = Vec::new();
    let mut writer = StreamWriter::try_new_with_options(
        &mut wire,
        &batches[0].schema(),
        IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta),
    )?;
    for batch in &batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    drop(writer);
    let server = support::Server::new(wire, 6, false, 200);
    let client = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
            .timeout(Duration::from_secs(5))
            .resources(antalya_rs::ResourceLimits {
                // Allow the second delta's workspace with the first output retained:
                // four 128 KiB allocations, a full 64 KiB wire chunk, and metadata.
                // The third delta still needs the caller to release the first output.
                max_retained_bytes: 2 * 1024 * 1024 + 640 * 1024,
                ..Default::default()
            })
            .execution_limits(ExecutionLimits {
                max_batch_bytes: 1024 * 1024,
                ..Default::default()
            }),
    )?;
    let request = AuthorizedRequest::new(
        Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
        server.endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "delta-ownership".into(),
            expected_principal: "fixture".into(),
        },
    )?;
    let mut stream = client
        .prepare_authorized_arrow(request)
        .await?
        .ready_transport()
        .await?
        .admit_arrow()?;
    let first = stream
        .next_batch()
        .await
        .map_err(|error| io::Error::other(format!("first delta batch: {error}")))?
        .ok_or_else(|| io::Error::other("first"))?;
    // Let the transport prefetch while the caller retains its first batch.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = stream
        .next_batch()
        .await
        .map_err(|error| io::Error::other(format!("second delta batch: {error}")))?
        .ok_or_else(|| io::Error::other("second"))?;
    assert_eq!(dictionary_text(second.column(0).as_ref(), 1)?, strings[1]);
    drop(second);
    let waiting = tokio::time::timeout(Duration::from_millis(50), stream.next_batch()).await;
    assert!(
        waiting.is_err(),
        "old caller-only backing must cause backpressure, got {waiting:?}"
    );
    assert_eq!(dictionary_text(first.column(0).as_ref(), 0)?, strings[0]);
    drop(first);
    let third = stream
        .next_batch()
        .await
        .map_err(|error| io::Error::other(format!("third delta batch: {error}")))?
        .ok_or_else(|| io::Error::other("third"))?;
    assert_eq!(dictionary_text(third.column(0).as_ref(), 2)?, strings[2]);
    drop(third);
    assert!(stream.next_batch().await?.is_none());
    stream.confirm_completion().await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_decoders_complete_or_refuse_capacity_without_deadline_stalls() -> Result<()> {
    use arrow_array::{ArrayRef, DictionaryArray, RecordBatch, types::Int32Type};
    use arrow_ipc::writer::{DictionaryHandling, IpcWriteOptions, StreamWriter};
    use std::sync::Arc;
    for dictionaries in [false, true] {
        let (wire, rows) = if dictionaries {
            let first: DictionaryArray<Int32Type> = ["a", "b"].into_iter().collect();
            let second: DictionaryArray<Int32Type> = ["a", "b", "c"].into_iter().collect();
            let first = RecordBatch::try_from_iter(vec![("v", Arc::new(first) as ArrayRef)])?;
            let second = RecordBatch::try_from_iter(vec![("v", Arc::new(second) as ArrayRef)])?;
            let mut wire = Vec::new();
            let mut writer = StreamWriter::try_new_with_options(
                &mut wire,
                &first.schema(),
                IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta),
            )?;
            writer.write(&first)?;
            writer.write(&second)?;
            writer.finish()?;
            drop(writer);
            (wire, 5)
        } else {
            (support::stream(3), 6)
        };
        let server = support::Server::new(wire, rows, false, 200);
        let client = AntalyaClient::new(
            Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
                .timeout(Duration::from_secs(10)),
        )?;
        let mut streams = Vec::new();
        for _ in 0..8 {
            let request = AuthorizedRequest::new(
                Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
                server.endpoint.clone(),
                Format::ArrowStream,
                AuthorizationContext {
                    receipt_id: "concurrent-decode".into(),
                    expected_principal: "fixture".into(),
                },
            )?;
            streams.push(
                client
                    .prepare_authorized_arrow(request)
                    .await?
                    .ready_transport()
                    .await?
                    .admit_arrow()?,
            );
        }
        let mut tasks = tokio::task::JoinSet::new();
        for mut stream in streams {
            tasks.spawn(async move {
                let consume = async {
                    let mut count = 0;
                    loop {
                        match stream.next_batch().await {
                            Ok(Some(batch)) => {
                                count += batch.num_rows() as u64;
                                drop(batch);
                            }
                            Ok(None) => {
                                assert_eq!(count, rows);
                                assert_eq!(stream.outcome(), Outcome::Running);
                                return Ok(true);
                            }
                            Err(DecodeError::Ipc(antalya_rs::IpcError::Transport(
                                antalya_rs::TransportError::Client(antalya_rs::Error::Context {
                                    cause: antalya_rs::Cause::Capacity,
                                    ..
                                }),
                            ))) => {
                                assert_eq!(stream.outcome(), Outcome::Unknown);
                                return Ok(false);
                            }
                            Err(error) => {
                                return Err(
                                    Box::new(error) as Box<dyn std::error::Error + Send + Sync>
                                );
                            }
                        }
                        tokio::task::yield_now().await;
                    }
                };
                tokio::time::timeout(Duration::from_secs(3), consume).await?
            });
        }
        let mut completed = 0;
        while let Some(result) = tasks.join_next().await {
            completed += usize::from(result??);
        }
        assert!(completed > 0, "at least one decoder must make progress");
        assert_eq!(
            server
                .requests
                .lock()
                .map_err(|_| io::Error::other("requests"))?
                .len(),
            16
        );
    }
    Ok(())
}
