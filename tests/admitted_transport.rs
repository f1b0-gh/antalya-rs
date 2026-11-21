#![cfg(feature = "arrow")]
#![forbid(unsafe_code)]

#[path = "admitted_transport/http.rs"]
mod http;
mod support;
use antalya_rs::{
    AntalyaClient, AuthorizationContext, AuthorizedRequest, Cause, Config, Credentials, Error,
    ExecutionLimits, Format, Phase, Query, ResourceLimits, TransportError,
};
use std::{io, time::Duration};
use support::{Server, stream};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn ready_transport_refuses_missing_runtime_without_panicking() -> Result<()> {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    let server = Server::new(stream(0), 0, false, 200);
    let client = client(&server, Duration::from_secs(3))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let prepared = runtime.block_on(client.prepare_authorized_arrow(request(&server)?))?;
    let mut ready = Box::pin(prepared.ready_transport());
    assert!(matches!(
        ready.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(TransportError::Runtime(_)))
    ));
    drop(ready);
    assert_eq!(count(&server, "SELECT v")?, 0);
    assert!(
        runtime
            .block_on(client.prepare_authorized_arrow(request(&server)?))
            .is_ok()
    );
    Ok(())
}

fn client(server: &Server, timeout: Duration) -> std::result::Result<AntalyaClient, Error> {
    AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
            .timeout(timeout)
            .resources(ResourceLimits {
                max_active_queries: 1,
                max_queued_queries: 0,
                ..ResourceLimits::default()
            }),
    )
}
fn request(server: &Server) -> std::result::Result<AuthorizedRequest, Error> {
    AuthorizedRequest::new(
        Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
        server.endpoint.clone(),
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "host-approval".into(),
            expected_principal: "fixture".into(),
        },
    )
}
fn count(server: &Server, needle: &str) -> Result<usize> {
    Ok(server
        .requests
        .lock()
        .map_err(|_| io::Error::other("fixture mutex poisoned"))?
        .iter()
        .filter(|r| r.contains(needle))
        .count())
}

#[tokio::test]
async fn admission_sends_without_polling_response_and_drop_quarantines_slot() -> Result<()> {
    let server = Server::new(stream(1), 2, false, 200);
    let client = client(&server, Duration::from_secs(3))?;
    let prepared = client.prepare_authorized_arrow(request(&server)?).await?;
    let receipt = prepared.recovery_receipt()?;
    let ready = prepared.ready_transport().await?;
    assert_eq!(ready.recovery_receipt()?.marker(), receipt.marker());
    assert_eq!(count(&server, "SELECT v")?, 0);
    let response = ready.admit()?;
    // Neither response headers nor body is polled by this test. Admission itself
    // must have handed the one request to the running connection transport.
    tokio::time::timeout(Duration::from_secs(1), async {
        while count(&server, "SELECT v")? == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    assert_eq!(count(&server, "SELECT v")?, 1);
    assert_eq!(count(&server, "system.query_log")?, 0);
    drop(response);
    assert!(
        client
            .prepare_authorized_arrow(request(&server)?)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn preparation_drop_and_expired_admission_send_no_data() -> Result<()> {
    let server = Server::new(stream(0), 0, false, 200);
    let client = client(&server, Duration::from_millis(250))?;
    let ready = client
        .prepare_authorized_arrow(request(&server)?)
        .await?
        .ready_transport()
        .await?;
    drop(ready);
    let ready = client
        .prepare_authorized_arrow(request(&server)?)
        .await?
        .ready_transport()
        .await?;
    tokio::time::sleep(Duration::from_millis(260)).await;
    assert!(matches!(ready.admit(), Err(TransportError::Deadline)));
    assert_eq!(count(&server, "SELECT v")?, 0);
    assert!(
        client
            .prepare_authorized_arrow(request(&server)?)
            .await
            .is_ok()
    );
    Ok(())
}

#[tokio::test]
async fn body_eof_is_only_transport_eof_and_observation_is_explicit() -> Result<()> {
    let body = stream(1);
    let server = Server::new(body.clone(), 2, false, 200);
    let client = client(&server, Duration::from_secs(3))?;
    let mut response = client
        .prepare_authorized_arrow(request(&server)?)
        .await?
        .ready_transport()
        .await?
        .admit()?;
    let mut received = Vec::new();
    while let Some(chunk) = response.next_chunk().await? {
        assert_eq!(chunk.charged_bytes(), chunk.len());
        received.extend_from_slice(&chunk);
    }
    assert_eq!(received, body);
    assert!(response.next_chunk().await?.is_none());
    assert_eq!(count(&server, "SELECT v")?, 1);
    assert_eq!(count(&server, "system.query_log")?, 0);
    assert!(
        client
            .prepare_authorized_arrow(request(&server)?)
            .await
            .is_err()
    );
    response.confirm_termination().await?;
    assert!(count(&server, "system.query_log")? > 0);
    assert!(
        client
            .prepare_authorized_arrow(request(&server)?)
            .await
            .is_ok()
    );
    Ok(())
}

#[tokio::test]
async fn early_termination_proof_keeps_capacity_until_body_is_drained_or_dropped() -> Result<()> {
    let server = Server::new(stream(1), 2, false, 200);
    let client = client(&server, Duration::from_secs(3))?;
    let mut response = client
        .prepare_authorized_arrow(request(&server)?)
        .await?
        .ready_transport()
        .await?
        .admit()?;
    response.confirm_termination().await?;
    assert!(!response.transport_eof());
    assert!(
        client
            .prepare_authorized_arrow(request(&server)?)
            .await
            .is_err()
    );
    while let Some(chunk) = response.next_chunk().await? {
        drop(chunk);
    }
    let prepared = client.prepare_authorized_arrow(request(&server)?).await?;
    let mut another = prepared.ready_transport().await?.admit()?;
    another.confirm_termination().await?;
    drop(another);
    assert!(
        client
            .prepare_authorized_arrow(request(&server)?)
            .await
            .is_ok()
    );
    Ok(())
}

#[tokio::test]
async fn oversized_wire_response_poisoned_without_terminal_queries() -> Result<()> {
    let server = Server::new(stream(0), 0, false, 200);
    let client = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?).execution_limits(
            ExecutionLimits {
                max_batch_bytes: 8,
                max_total_bytes: 8,
                ..ExecutionLimits::default()
            },
        ),
    )?;
    let mut response = client
        .prepare_authorized_arrow(request(&server)?)
        .await?
        .ready_transport()
        .await?
        .admit()?;
    assert!(matches!(
        response.next_chunk().await,
        Err(TransportError::LimitExceeded)
    ));
    assert!(matches!(
        response.next_chunk().await,
        Err(TransportError::Poisoned)
    ));
    assert_eq!(count(&server, "system.query_log")?, 0);
    Ok(())
}

#[tokio::test]
async fn cancelled_byte_wait_resumes_without_replaying_request() -> Result<()> {
    let body = stream(1);
    let bytes = body.len();
    let server = Server::new(body, 2, false, 200);
    let client = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("token")?)
            .timeout(Duration::from_secs(3))
            .resources(ResourceLimits {
                max_retained_bytes: bytes,
                ..ResourceLimits::default()
            })
            .execution_limits(ExecutionLimits {
                max_batch_bytes: bytes,
                ..ExecutionLimits::default()
            }),
    )?;
    let mut response = client
        .prepare_authorized_arrow(request(&server)?)
        .await?
        .ready_transport()
        .await?
        .admit()?;
    let chunk = response
        .next_chunk()
        .await?
        .ok_or_else(|| io::Error::other("missing fixture body"))?;
    // Any retained nonempty chunk leaves less than the next full reservation.
    assert!(!chunk.is_empty());
    let clone = chunk.clone();
    assert_eq!(chunk.as_ptr(), clone.as_ptr());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), response.next_chunk())
            .await
            .is_err()
    );
    drop(chunk);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), response.next_chunk())
            .await
            .is_err()
    );
    drop(clone);
    while let Some(chunk) = response.next_chunk().await? {
        drop(chunk);
    }
    assert_eq!(count(&server, "SELECT v")?, 1);
    assert_eq!(count(&server, "system.query_log")?, 0);
    Ok(())
}

#[tokio::test]
async fn connection_failure_preserves_source_and_redacts_display() -> Result<()> {
    let server = Server::new(stream(0), 0, false, 200);
    let client = client(&server, Duration::from_secs(2))?;
    let prepared = client.prepare_authorized_arrow(request(&server)?).await?;
    drop(server);
    let error = prepared
        .ready_transport()
        .await
        .err()
        .ok_or_else(|| io::Error::other("closed fixture accepted connection"))?;
    assert!(matches!(error, TransportError::Connect(_)));
    assert!(std::error::Error::source(&error).is_some());
    assert!(!format!("{error:?}").contains("127.0.0.1"));
    assert!(!format!("{error}").contains("token"));
    Ok(())
}

#[tokio::test]
async fn chunked_response_cannot_bypass_cumulative_wire_limit() -> Result<()> {
    let server = http::Server::new(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nabcd\r\n5\r\nefghi\r\n0\r\n\r\n".to_vec())?;
    let mut response = server.admit(8).await?;
    let error = loop {
        match response.next_chunk().await {
            Ok(Some(chunk)) => assert!(chunk.len() <= 8),
            Ok(None) => return Err(io::Error::other("oversized chunked body accepted").into()),
            Err(error) => break error,
        }
    };
    assert!(matches!(error, TransportError::LimitExceeded));
    assert!(response.wire_bytes() <= 8);
    assert!(!response.transport_eof());
    server.finish()
}

#[tokio::test]
async fn bulk_response_is_split_into_bounded_payload_chunks() -> Result<()> {
    let payload: Vec<_> = (0_u8..=u8::MAX).cycle().take(512 * 1024).collect();
    let mut reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    )
    .into_bytes();
    reply.extend_from_slice(&payload);
    let server = http::Server::new(reply)?;
    let mut response = server.admit(1024 * 1024).await?;
    let mut received = Vec::new();
    let mut chunks = 0;
    while let Some(chunk) = response.next_chunk().await? {
        assert!(chunk.len() <= 64 * 1024);
        assert_eq!(chunk.charged_bytes(), chunk.len());
        received.extend_from_slice(&chunk);
        chunks += 1;
    }
    assert_eq!(received, payload);
    assert!(chunks >= 8);
    assert!(response.transport_eof());
    server.finish()
}

#[tokio::test]
async fn truncated_body_preserves_transport_source_and_never_becomes_eof() -> Result<()> {
    let server = http::Server::new(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabc".to_vec(),
    )?;
    let mut response = server.admit(1024).await?;
    let error = loop {
        match response.next_chunk().await {
            Ok(Some(_)) => {}
            Ok(None) => return Err(io::Error::other("truncated body accepted").into()),
            Err(error) => break error,
        }
    };
    assert!(matches!(error, TransportError::Http(_)));
    assert!(std::error::Error::source(&error).is_some());
    assert!(!response.transport_eof());
    assert!(matches!(
        response.next_chunk().await,
        Err(TransportError::Poisoned)
    ));
    server.finish()
}

#[tokio::test]
async fn redirect_and_server_exception_are_refused_without_followup() -> Result<()> {
    for (reply, expected_status) in [
        (b"HTTP/1.1 303 See Other\r\nLocation: http://127.0.0.1:1/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(), Some(303)),
        (b"HTTP/1.1 200 OK\r\nX-ClickHouse-Exception-Code: 42\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(), None),
    ] {
        let server = http::Server::new(reply)?;
        let mut response = server.admit(1024).await?;
        let error = response.next_chunk().await.err().ok_or_else(|| io::Error::other("failed response accepted"))?;
        match expected_status {
            Some(expected) => assert!(matches!(error, TransportError::HttpStatus(status) if status == expected)),
            None => assert!(matches!(error, TransportError::Client(error) if error.cause() == Some(Cause::ServerResponse(Some(42))))),
        }
        assert!(!response.transport_eof());
        server.finish()?;
    }
    Ok(())
}

#[tokio::test]
async fn numeric_exception_headers_preserve_codes_without_reading_error_body() -> Result<()> {
    for (status, code) in [(200, 164), (500, 60), (403, 497), (500, u32::MAX)] {
        // The declared body is incomplete: diagnostics must come from headers
        // without consuming it or reporting a body transport failure.
        let reply = format!(
            "HTTP/1.1 {status} Test\r\nX-ClickHouse-Exception-Code: {code}\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\nSELECT secret-token FROM private_table"
        );
        let server = http::Server::new(reply.into_bytes())?;
        let mut response = server.admit(1024).await?;
        let error = response
            .next_chunk()
            .await
            .err()
            .ok_or_else(|| io::Error::other("exception accepted"))?;
        assert!(
            matches!(error, TransportError::Client(inner) if inner.cause() == Some(Cause::ServerResponse(Some(code))) && inner.phase() == Some(Phase::Data))
        );
        let source = std::error::Error::source(&error)
            .ok_or_else(|| io::Error::other("missing diagnostic source"))?;
        for text in [
            format!("{error:?}"),
            error.to_string(),
            source.to_string(),
            format!("{source:?}"),
        ] {
            for secret in ["SELECT", "secret-token", "private_table"] {
                assert!(!text.contains(secret));
            }
        }
        assert_eq!(response.wire_bytes(), 0);
        assert!(!response.transport_eof());
        assert!(matches!(
            response.next_chunk().await,
            Err(TransportError::Poisoned)
        ));
        server.finish()?;
    }
    Ok(())
}

#[tokio::test]
async fn numeric_exception_trailer_preserves_code_without_claiming_clean_eof() -> Result<()> {
    let server = http::Server::new(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\nX-ClickHouse-Exception-Code: 164\r\n\r\n".to_vec())?;
    let mut response = server.admit(1024).await?;
    let error = response
        .next_chunk()
        .await
        .err()
        .ok_or_else(|| io::Error::other("exception trailer accepted"))?;
    assert!(
        matches!(error, TransportError::Client(inner) if inner.cause() == Some(Cause::ServerResponse(Some(164))))
    );
    assert!(!response.transport_eof());
    server.finish()
}

#[tokio::test]
async fn malformed_and_duplicate_exception_headers_remain_refusals_on_error_statuses() -> Result<()>
{
    for status in [200, 500] {
        for code in [
            "",
            "+164",
            "00",
            "4294967296",
            "00000000164",
            "secret-token",
        ] {
            let reply = format!(
                "HTTP/1.1 {status} Test\r\nX-ClickHouse-Exception-Code: {code}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let server = http::Server::new(reply.into_bytes())?;
            let mut response = server.admit(1024).await?;
            assert!(matches!(
                response.next_chunk().await,
                Err(TransportError::ServerException)
            ));
            assert!(!response.transport_eof());
            server.finish()?;
        }
        let reply = format!(
            "HTTP/1.1 {status} Test\r\nX-ClickHouse-Exception-Code: 0\r\nX-ClickHouse-Exception-Code: 164\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let server = http::Server::new(reply.into_bytes())?;
        let mut response = server.admit(1024).await?;
        assert!(matches!(
            response.next_chunk().await,
            Err(TransportError::InvalidResponse)
        ));
        server.finish()?;
    }
    Ok(())
}

#[tokio::test]
async fn absent_or_zero_exception_header_keeps_status_diagnostic() -> Result<()> {
    for header in ["", "X-ClickHouse-Exception-Code: 0\r\n"] {
        let reply = format!(
            "HTTP/1.1 500 Test\r\n{header}Content-Length: 1000000\r\nConnection: close\r\n\r\nCode: 164. secret-token"
        );
        let server = http::Server::new(reply.into_bytes())?;
        let mut response = server.admit(1024).await?;
        assert!(matches!(
            response.next_chunk().await,
            Err(TransportError::HttpStatus(500))
        ));
        assert_eq!(response.wire_bytes(), 0);
        server.finish()?;
    }
    Ok(())
}

#[tokio::test]
async fn ambiguous_exception_headers_and_partial_status_cannot_be_clean_eof() -> Result<()> {
    for reply in [
        b"HTTP/1.1 200 OK\r\nX-ClickHouse-Exception-Code: 0\r\nX-ClickHouse-Exception-Code: 42\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        b"HTTP/1.1 206 Partial Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\nX-ClickHouse-Exception-Code: 0\r\nX-ClickHouse-Exception-Code: 42\r\n\r\n".to_vec(),
    ] {
        let server = http::Server::new(reply)?;
        let mut response = server.admit(1024).await?;
        assert!(response.next_chunk().await.is_err());
        assert!(!response.transport_eof());
        server.finish()?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the digest-pinned bearer fixture"]
async fn live_admitted_transport_preserves_receipt_and_large_and_empty_arrow() -> Result<()> {
    let endpoint = antalya_rs::Endpoint::parse(&std::env::var("ANTALYA_TEST_URL")?)?;
    let client = AntalyaClient::new(
        Config::new(
            endpoint.clone(),
            Credentials::bearer(&std::env::var("ANTALYA_TEST_TOKEN")?)?,
        )
        .timeout(Duration::from_secs(30))
        .execution_limits(ExecutionLimits {
            max_batch_bytes: 64 * 1024,
            max_total_bytes: 1024 * 1024,
            ..ExecutionLimits::default()
        }),
    )?;
    for rows in [0_u64, 100_000] {
        let request = AuthorizedRequest::new(
            Query::new(
                format!("SELECT number FROM numbers({rows})"),
                uuid::Uuid::new_v4().to_string(),
            )?,
            endpoint.clone(),
            Format::ArrowStream,
            AuthorizationContext {
                receipt_id: "live-transport".into(),
                expected_principal: "fixture".into(),
            },
        )?;
        let ready = client
            .prepare_authorized_arrow(request)
            .await?
            .ready_transport()
            .await?;
        let original = ready.recovery_receipt()?;
        let mut response = ready.admit()?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next_chunk().await? {
            assert!(bytes.len() + chunk.len() <= 1024 * 1024);
            bytes.extend_from_slice(&chunk);
        }
        assert_eq!(response.recovery_receipt()?.query_id(), original.query_id());
        assert_eq!(response.recovery_receipt()?.marker(), original.marker());
        assert_eq!(
            response.recovery_receipt()?.principal(),
            original.principal()
        );
        assert_eq!(response.wire_bytes(), u64::try_from(bytes.len())?);
        let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
        let mut expected = 0;
        for batch in reader {
            let batch = batch?;
            let column = batch
                .column_by_name("number")
                .ok_or_else(|| io::Error::other("missing live number column"))?;
            let numbers = column
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .ok_or_else(|| io::Error::other("unexpected live number type"))?;
            for number in numbers.iter() {
                assert_eq!(number, Some(expected));
                expected += 1;
            }
        }
        assert_eq!(expected, rows);
        assert_eq!(
            response.confirm_termination().await?,
            antalya_rs::RemoteTermination::Succeeded
        );
    }
    Ok(())
}
