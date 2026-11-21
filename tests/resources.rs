use antalya_rs::{Cause, Error, ErrorKind, Phase};
use std::error::Error as _;

#[test]
fn diagnostics_keep_typed_causes_without_sensitive_io_text() {
    let source = std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "secret-token SELECT endpoint.invalid",
    );
    let error = Error::from_io(Phase::Write, &source);
    assert_eq!(error.kind(), ErrorKind::Publication);
    assert_eq!(error.phase(), Some(Phase::Write));
    assert_eq!(
        error.cause(),
        Some(Cause::Io(std::io::ErrorKind::PermissionDenied))
    );
    assert!(error.source().is_some());
    for text in [
        error.to_string(),
        format!("{error:?}"),
        error.source().unwrap().to_string(),
    ] {
        for secret in ["secret-token", "SELECT", "endpoint.invalid"] {
            assert!(!text.contains(secret));
        }
    }
}

#[test]
fn legacy_categories_and_contextual_categories_agree() {
    assert_eq!(Error::Timeout.kind(), ErrorKind::Timeout);
    assert_eq!(Error::Timeout.phase(), None);
    assert_eq!(
        Error::at(Phase::Queue, Cause::Timeout).kind(),
        ErrorKind::Timeout
    );
    assert_eq!(
        Error::at(Phase::Queue, Cause::Capacity).kind(),
        ErrorKind::LimitExceeded
    );
    assert_eq!(
        Error::at(Phase::Control, Cause::InvalidProof).kind(),
        ErrorKind::Unconfirmed
    );
}

#[cfg(feature = "arrow")]
#[test]
fn upstream_causes_distinguish_decoder_and_server_without_echoing_messages() {
    let source = clickhouse::error::Error::Other(Box::new(arrow_schema::ArrowError::ParseError(
        "secret SQL".into(),
    )));
    let error = Error::from_upstream(Phase::Data, &source);
    assert_eq!(error.kind(), ErrorKind::InvalidIpc);
    assert_eq!(error.cause(), Some(Cause::Decoder));
    let source = clickhouse::error::Error::BadResponse("secret SQL".into());
    let error = Error::from_upstream(Phase::Data, &source);
    assert_eq!(error.kind(), ErrorKind::Transport);
    assert!(!format!("{error:?}: {error}").contains("secret SQL"));
}

#[test]
fn probe_failures_do_not_claim_target_query_failure() {
    let identity = Error::at(Phase::Identity, Cause::IdentityMismatch);
    assert_eq!(identity.kind(), ErrorKind::InvalidCredentials);
    assert_eq!(identity.phase(), Some(Phase::Identity));
    for cause in [
        Cause::JsonDecoder,
        Cause::ServerResponse(None),
        Cause::ServerResponse(Some(497)),
    ] {
        let error = Error::at(Phase::Control, cause);
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert_eq!(error.cause(), Some(cause));
        assert!(!error.to_string().contains("proves query failure"));
        assert!(!error.to_string().contains("Arrow"));
    }
}

#[cfg(feature = "arrow")]
#[test]
fn server_response_keeps_only_strict_leading_numeric_code() {
    for (message, code) in [
        ("Code: 497. secret SQL endpoint.invalid", Some(497)),
        ("Code: 0. secret", Some(0)),
        ("Code: 12x. secret", None),
        ("Code: 4294967296. secret", None),
        ("secret Code: 497. SQL", None),
        ("Code: +497. secret", None),
        ("Code: . secret", None),
    ] {
        let source = clickhouse::error::Error::BadResponse(message.into());
        let error = Error::from_upstream(Phase::Data, &source);
        assert_eq!(error.cause(), Some(Cause::ServerResponse(code)));
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(!format!("{error:?}: {error}").contains("secret"));
    }
}
