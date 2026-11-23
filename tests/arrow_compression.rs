// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "arrow")]
#[allow(dead_code)]
mod support;
use antalya_rs::{
    AntalyaClient, ArrowCompression, Config, Credentials, Endpoint, ExecutionLimits, Outcome,
    Query, ResourceLimits, TerminalPolicy, TerminalStrategy,
};
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use std::{sync::Arc, time::Duration};

fn stream() -> (RecordBatch, Vec<u8>) {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "v",
            Arc::new(Int32Array::from_iter((0..2048).map(Some))) as ArrayRef,
        ),
        (
            "s",
            Arc::new(StringArray::from_iter((0..2048).map(|i| match i % 3 {
                0 => None,
                1 => Some("repeated é ß data"),
                _ => Some(""),
            }))) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut bytes = Vec::new();
    let options = arrow_ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))
        .unwrap();
    let mut writer =
        arrow_ipc::writer::StreamWriter::try_new_with_options(&mut bytes, &batch.schema(), options)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    (batch, bytes)
}

#[tokio::test]
async fn opt_in_decodes_values_and_cannot_be_overridden_by_query_settings() {
    let (expected, bytes) = stream();
    let server = support::Server::new(bytes, 4096, false, 200);
    let client = AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("token").unwrap(),
        )
        .arrow_compression(ArrowCompression::Zstd),
    )
    .unwrap();
    assert!(
        Query::new("SELECT v,s", "override")
            .unwrap()
            .setting("output_format_arrow_compression_method", "lz4")
            .is_err()
    );
    let mut execution = client
        .fetch_arrow(Query::new("SELECT v,s", "zstd").unwrap())
        .await
        .unwrap();
    for _ in 0..2 {
        let batch = execution.next_batch().await.unwrap().unwrap();
        assert_eq!(&*batch, &expected);
    }
    assert!(execution.next_batch().await.unwrap().is_none());
    assert_eq!(execution.outcome(), Outcome::Succeeded);
    let requests = server.requests.lock().unwrap();
    let request = requests
        .iter()
        .find(|r| r.contains("output_format_arrow_compression_method="))
        .unwrap();
    assert!(request.contains("output_format_arrow_compression_method=zstd"));
    assert!(!request.contains("output_format_arrow_compression_method=lz4"));
}

#[tokio::test]
async fn default_still_requests_none_and_refuses_compressed_response() {
    let (_, bytes) = stream();
    let server = support::Server::new(bytes, 4096, false, 200);
    let client = AntalyaClient::new(Config::new(
        server.endpoint.clone(),
        Credentials::bearer("token").unwrap(),
    ))
    .unwrap();
    let mut execution = client
        .fetch_arrow(Query::new("SELECT v,s", "default").unwrap())
        .await
        .unwrap();
    assert!(execution.next_batch().await.is_err());
    assert_ne!(execution.outcome(), Outcome::Succeeded);
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.contains("output_format_arrow_compression_method=none"))
    );
}

#[test]
fn compression_staging_must_fit_shared_capacity_before_dispatch() {
    let config = Config::new(
        Endpoint::parse("http://127.0.0.1:1").unwrap(),
        Credentials::bearer("token").unwrap(),
    )
    .execution_limits(ExecutionLimits {
        max_batch_bytes: 131072,
        ..Default::default()
    })
    .resources(ResourceLimits {
        max_retained_bytes: 262144,
        ..Default::default()
    });
    assert!(AntalyaClient::new(config.clone()).is_ok());
    assert!(AntalyaClient::new(config.arrow_compression(ArrowCompression::Zstd)).is_err());
}

#[tokio::test]
#[ignore = "requires native Antalya fixture; supports both log and framed terminal policies"]
async fn native_zstd_values_and_late_errors() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = Endpoint::parse(&std::env::var("ANTALYA_TEST_URL")?)?;
    let credentials = Credentials::bearer(&std::env::var("ANTALYA_TEST_TOKEN")?)?;
    for strategy in [TerminalStrategy::FlushFirst, TerminalStrategy::InBand] {
        let client = AntalyaClient::new(
            Config::new(endpoint.clone(), credentials.clone())
                .timeout(Duration::from_secs(30))
                .arrow_compression(ArrowCompression::Zstd)
                .terminal_policy(TerminalPolicy {
                    strategy,
                    ..Default::default()
                }),
        )?;
        for rows in [0, 4096] {
            let mut execution = client.fetch_arrow(Query::new(
                format!("SELECT toUInt64(number) AS id,repeat('abc',100) AS payload FROM numbers({rows})"),
                format!("zstd-{}", uuid::Uuid::new_v4()))?
                .setting("max_block_size", "256")?.setting("max_threads", "1")?
                .setting("output_format_arrow_string_as_string", "0")?
                .setting("output_format_arrow_low_cardinality_as_dictionary", "0")?).await?;
            let mut ids = Vec::new();
            while let Some(batch) = execution.next_batch().await? {
                let numbers = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .ok_or("id type")?;
                ids.extend(numbers.values().iter().copied());
                // Explicit binary-string setting pins the value contract.
                let strings = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .ok_or("payload type")?;
                for value in strings.iter() {
                    assert_eq!(value, Some("abc".repeat(100).as_bytes()));
                }
            }
            assert!(ids.into_iter().eq(0..rows));
            assert_eq!(execution.outcome(), Outcome::Succeeded);
        }
        let mut late = client
            .fetch_arrow(
                Query::new(
                    "SELECT number,throwIf(number>=2048) AS bad FROM numbers(10000)",
                    format!("zstd-late-{}", uuid::Uuid::new_v4()),
                )?
                .setting("max_block_size", "1024")?,
            )
            .await?;
        loop {
            match late.next_batch().await {
                Ok(Some(_)) => (),
                Ok(None) => return Err("late exception became success".into()),
                Err(_) => break,
            }
        }
        assert_ne!(late.outcome(), Outcome::Succeeded);
    }
    Ok(())
}
