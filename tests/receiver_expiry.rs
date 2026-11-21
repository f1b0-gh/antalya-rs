#![cfg(feature = "arrow")]
#![forbid(unsafe_code)]
mod support;
use antalya_rs::{
    AntalyaClient, AuthorizationContext, AuthorizedRequest, Config, Credentials, DecodeError,
    Format, IpcError, JwtReceiverProfile, Query, ReceiverError, TransportError,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use std::io;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// Unsigned synthetic strings test local metadata constraints only. The live
// bearer fixture separately verifies actual server authentication and expiry.
const VALID: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJpc3N1ZXIiLCJhdWQiOiJhdWRpZW5jZSIsInN1YiI6ImZpeHR1cmUiLCJleHAiOjQxMDI0NDQ4MDB9.c2ln";
const EXPIRY: u64 = 4_102_444_800;
fn profile(endpoint: antalya_rs::Endpoint) -> Result<JwtReceiverProfile> {
    Ok(JwtReceiverProfile::new(
        endpoint, "issuer", "audience", "fixture", 2,
    )?)
}
fn request(endpoint: antalya_rs::Endpoint) -> Result<AuthorizedRequest> {
    Ok(AuthorizedRequest::new(
        Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
        endpoint,
        Format::ArrowStream,
        AuthorizationContext {
            receipt_id: "receiver-test".into(),
            expected_principal: "fixture".into(),
        },
    )?)
}

#[test]
fn receiver_profile_binds_exact_credentials_and_conservative_expiry() -> Result {
    let endpoint = antalya_rs::Endpoint::parse("http://127.0.0.1:1")?;
    let config = Config::jwt_receiver(profile(endpoint.clone())?, VALID)?;
    assert_eq!(config.receiver_refuses_by(), Some(EXPIRY + 3));
    assert!(!format!("{config:?}").contains(VALID));
    assert_eq!(
        Config::new(endpoint, Credentials::bearer("opaque")?).receiver_refuses_by(),
        None
    );
    Ok(())
}

#[test]
fn ambiguous_or_unbounded_expiry_metadata_is_refused() -> Result {
    let endpoint = antalya_rs::Endpoint::parse("http://127.0.0.1:1")?;
    for token in [
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJpc3N1ZXIiLCJhdWQiOiJhdWRpZW5jZSIsInN1YiI6ImZpeHR1cmUiLCJleHAiOjQxMDI0NDQ4MDAsImV4cCI6MX0.c2ln",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJpc3N1ZXIiLCJhdWQiOiJhdWRpZW5jZSIsInN1YiI6ImZpeHR1cmUiLCJleHAiOjQxMDI0NDQ4MDAuMH0.c2ln",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJpc3N1ZXIiLCJhdWQiOiJhdWRpZW5jZSIsInN1YiI6ImZpeHR1cmUifQ.c2ln",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJpc3N1ZXIiLCJhdWQiOiJhdWRpZW5jZSIsInN1YiI6ImZpeHR1cmUiLCJleHAiOjE4NDQ2NzQ0MDczNzA5NTUxNjE1fQ.c2ln",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJpc3N1ZXIiLCJhdWQiOiJvdGhlciIsInN1YiI6ImZpeHR1cmUiLCJleHAiOjQxMDI0NDQ4MDB9.c2ln",
        "header.payload.signature",
    ] {
        assert!(Config::jwt_receiver(profile(endpoint.clone())?, token).is_err());
    }
    assert!(matches!(
        Config::jwt_receiver(profile(endpoint)?, &"x".repeat(8193)),
        Err(ReceiverError::Limit)
    ));
    Ok(())
}

#[test]
fn numeric_date_stays_in_exact_supported_receiver_range() -> Result {
    let endpoint = antalya_rs::Endpoint::parse("http://127.0.0.1:1")?;
    for expiry in [u64::from(u32::MAX) + 1, (1_u64 << 53) + 1] {
        let payload =
            format!(r#"{{"iss":"issuer","aud":"audience","sub":"fixture","exp":{expiry}}}"#);
        let token = format!(
            "eyJhbGciOiJIUzI1NiJ9.{}.c2ln",
            URL_SAFE_NO_PAD.encode(payload)
        );
        assert!(matches!(
            Config::jwt_receiver(profile(endpoint.clone())?, &token),
            Err(ReceiverError::Expiry)
        ));
    }
    let payload = format!(
        r#"{{"iss":"issuer","aud":"audience","sub":"fixture","exp":{}}}"#,
        u32::MAX
    );
    let token = synthetic(r#"{"alg":"HS256"}"#, &payload);
    assert_eq!(
        Config::jwt_receiver(profile(endpoint)?, &token)?.receiver_refuses_by(),
        Some(u64::from(u32::MAX) + 3)
    );
    Ok(())
}

fn synthetic(header: &str, payload: &str) -> String {
    format!(
        "{}.{}.c2ln",
        URL_SAFE_NO_PAD.encode(header),
        URL_SAFE_NO_PAD.encode(payload)
    )
}

#[test]
fn duplicate_extensions_and_oversized_metadata_keep_typed_refusals() -> Result {
    use std::error::Error as _;
    let endpoint = antalya_rs::Endpoint::parse("http://127.0.0.1:1")?;
    let payload = r#"{"iss":"issuer","aud":"audience","sub":"fixture","exp":4102444800}"#;
    for header in [
        r#"{"alg":"HS256","alg":"HS256"}"#,
        r#"{"alg":"HS256","crit":[]}"#,
        r#"{"alg":"HS256","b64":false}"#,
    ] {
        let result = Config::jwt_receiver(profile(endpoint.clone())?, &synthetic(header, payload));
        let error = result
            .err()
            .ok_or_else(|| io::Error::other("ambiguous header accepted"))?;
        assert!(matches!(error, ReceiverError::Json(_)));
        assert!(error.source().is_some_and(|e| e.is::<serde_json::Error>()));
    }
    let duplicate =
        r#"{"iss":"issuer","aud":"audience","sub":"fixture","exp":4102444800,"\u0065xp":1}"#;
    assert!(matches!(
        Config::jwt_receiver(
            profile(endpoint.clone())?,
            &synthetic(r#"{"alg":"HS256"}"#, duplicate)
        ),
        Err(ReceiverError::Json(_))
    ));
    let large = format!(r#"{{"padding":"{}"}}"#, "x".repeat(4100));
    assert!(matches!(
        Config::jwt_receiver(profile(endpoint)?, &synthetic(r#"{"alg":"HS256"}"#, &large)),
        Err(ReceiverError::Limit)
    ));
    Ok(())
}

#[tokio::test]
async fn guarded_send_refuses_foreign_principal_and_expired_credential() -> Result {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    for (principal, expiry) in [("other", EXPIRY), ("fixture", 1)] {
        let payload =
            format!(r#"{{"iss":"issuer","aud":"audience","sub":"{principal}","exp":{expiry}}}"#);
        let profile =
            JwtReceiverProfile::new(server.endpoint.clone(), "issuer", "audience", principal, 0)?;
        let config = Config::jwt_receiver(profile, &synthetic(r#"{"alg":"HS256"}"#, &payload))?;
        let ready = AntalyaClient::new(config)?
            .prepare_authorized_arrow(request(server.endpoint.clone())?)
            .await?
            .ready_transport()
            .await?;
        let result = ready.admit_arrow_before(EXPIRY + 1);
        if principal == "other" {
            assert!(matches!(
                result,
                Err(DecodeError::Ipc(IpcError::Transport(
                    TransportError::Receiver(ReceiverError::Scope)
                )))
            ));
        } else {
            assert!(matches!(
                result,
                Err(DecodeError::Ipc(IpcError::Transport(
                    TransportError::Receiver(ReceiverError::Expired)
                )))
            ));
        }
    }
    assert!(
        !server
            .requests
            .lock()
            .map_err(|_| io::Error::other("request lock"))?
            .iter()
            .any(|r| r.contains("SELECT v"))
    );
    Ok(())
}

#[tokio::test]
async fn guarded_send_refuses_missing_profile_or_late_receiver_bound_without_select() -> Result {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    for configured in [false, true] {
        let config = if configured {
            Config::jwt_receiver(profile(server.endpoint.clone())?, VALID)?
        } else {
            Config::new(server.endpoint.clone(), Credentials::bearer("opaque")?)
        };
        let client = AntalyaClient::new(config)?;
        let ready = client
            .prepare_authorized_arrow(request(server.endpoint.clone())?)
            .await?
            .ready_transport()
            .await?;
        let error = ready.admit_arrow_before(EXPIRY + 2);
        assert!(matches!(
            error,
            Err(DecodeError::Ipc(IpcError::Transport(
                TransportError::Receiver(_)
            )))
        ));
    }
    assert!(
        !server
            .requests
            .lock()
            .map_err(|_| io::Error::other("request lock"))?
            .iter()
            .any(|r| r.contains("SELECT v"))
    );
    Ok(())
}

#[tokio::test]
async fn guarded_send_uses_original_receipt_and_current_control_observation() -> Result {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let client = AntalyaClient::new(Config::jwt_receiver(
        profile(server.endpoint.clone())?,
        VALID,
    )?)?;
    let ready = client
        .prepare_authorized_arrow(request(server.endpoint.clone())?)
        .await?
        .ready_transport()
        .await?;
    let receipt = ready.recovery_receipt()?;
    let mut response = ready.admit_arrow_before(EXPIRY + 3)?;
    while response.next_batch().await?.is_some() {}
    let control = AntalyaClient::new(Config::new(
        server.endpoint.clone(),
        Credentials::bearer("control")?,
    ))?;
    let proof = control.observe_terminal(&receipt).await?;
    response.accept_completion(&proof)?;
    assert_eq!(response.rows_decoded(), 2);
    assert_eq!(response.recovery_receipt()?.marker(), receipt.marker());
    assert_eq!(
        server
            .requests
            .lock()
            .map_err(|_| io::Error::other("request lock"))?
            .iter()
            .filter(|r| r.contains("SELECT v"))
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn credential_selection_preserves_original_preparation_and_shared_admission() -> Result {
    use antalya_rs::{Cause, ResourceLimits};
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let original = AntalyaClient::new(
        Config::jwt_receiver(profile(server.endpoint.clone())?, VALID)?.resources(ResourceLimits {
            max_active_queries: 1,
            max_queued_queries: 0,
            ..ResourceLimits::default()
        }),
    )?;
    let prepared = original
        .prepare_authorized_arrow(request(server.endpoint.clone())?)
        .await?;
    let receipt = prepared.recovery_receipt()?;
    let replacement = synthetic(
        r#"{"alg":"HS256"}"#,
        r#"{"iss":"issuer","aud":"audience","sub":"fixture","exp":4102444700}"#,
    );
    let selected = original.with_jwt_receiver(profile(server.endpoint.clone())?, &replacement)?;
    assert!(!format!("{selected:?}").contains(&replacement));
    let refused = selected
        .prepare_authorized_arrow(request(server.endpoint.clone())?)
        .await;
    assert!(matches!(refused, Err(ref e) if e.cause() == Some(Cause::Capacity)));
    let mut response = prepared
        .ready_transport()
        .await?
        .admit_arrow_before(EXPIRY + 3)?;
    while response.next_batch().await?.is_some() {}
    // Current control credentials, original response and receipt: no data replay.
    response.accept_completion(&selected.observe_terminal(&receipt).await?)?;
    drop(response);
    let next = selected
        .prepare_authorized_arrow(request(server.endpoint.clone())?)
        .await?;
    let mut response = next
        .ready_transport()
        .await?
        .admit_arrow_before(EXPIRY - 97)?;
    while response.next_batch().await?.is_some() {}
    let proof = selected
        .observe_terminal(&response.recovery_receipt()?)
        .await?;
    response.accept_completion(&proof)?;
    let captured = server
        .requests
        .lock()
        .map_err(|_| io::Error::other("request lock"))?;
    let data: Vec<_> = captured.iter().filter(|r| r.contains("SELECT v")).collect();
    assert_eq!(data.len(), 2);
    assert!(data[0].contains(VALID));
    assert!(data[1].contains(&replacement));
    assert!(
        captured
            .iter()
            .filter(|r| r.contains("system.query_log"))
            .all(|r| r.contains(&replacement))
    );
    Ok(())
}

#[tokio::test]
async fn credential_selection_cannot_reset_quarantine_or_retarget_client() -> Result {
    use antalya_rs::{Cause, ResourceLimits};
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let original = AntalyaClient::new(
        Config::new(server.endpoint.clone(), Credentials::bearer("opaque")?).resources(
            ResourceLimits {
                max_active_queries: 1,
                max_queued_queries: 0,
                ..ResourceLimits::default()
            },
        ),
    )?;
    assert!(matches!(
        original.with_jwt_receiver(
            profile(antalya_rs::Endpoint::parse("http://127.0.0.1:1")?)?,
            VALID
        ),
        Err(ReceiverError::Scope)
    ));
    let selected = original.with_jwt_receiver(profile(server.endpoint.clone())?, VALID)?;
    let response = selected
        .prepare_authorized_arrow(request(server.endpoint.clone())?)
        .await?
        .ready_transport()
        .await?
        .admit_arrow_before(EXPIRY + 3)?;
    drop(response); // No terminal proof: the original shared lane remains quarantined.
    let renewed = original.with_jwt_receiver(profile(server.endpoint.clone())?, VALID)?;
    for client in [&original, &selected, &renewed] {
        assert!(
            matches!(client.prepare_authorized_arrow(request(server.endpoint.clone())?).await,
            Err(ref e) if e.cause() == Some(Cause::Capacity))
        );
    }
    Ok(())
}

#[tokio::test]
async fn selected_credentials_also_reach_the_legacy_arrow_client() -> Result {
    let server = support::Server::new(support::stream(1), 2, false, 200);
    let base = AntalyaClient::new(Config::new(
        server.endpoint.clone(),
        Credentials::bearer("opaque")?,
    ))?;
    let selected = base.with_jwt_receiver(profile(server.endpoint.clone())?, VALID)?;
    let mut execution = selected
        .fetch_authorized_arrow(request(server.endpoint.clone())?)
        .await?;
    while execution.next_batch().await?.is_some() {}
    assert_eq!(execution.outcome(), antalya_rs::Outcome::Succeeded);
    let captured = server
        .requests
        .lock()
        .map_err(|_| io::Error::other("request lock"))?;
    let data: Vec<_> = captured.iter().filter(|r| r.contains("SELECT v")).collect();
    assert_eq!(data.len(), 1);
    assert!(data[0].contains(VALID));
    assert!(!data[0].contains("Bearer opaque"));
    Ok(())
}
