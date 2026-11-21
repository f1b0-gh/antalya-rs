use antalya_rs::{Cause, Error, ErrorKind, Phase};
use std::error::Error as _;

#[test]
fn server_codes_have_useful_static_descriptions_without_server_text() {
    for (code, description) in [
        (60, "unknown table"),
        (62, "query syntax error"),
        (81, "unknown database"),
        (115, "unknown setting"),
        (164, "read-only policy"),
        (497, "access denied"),
        (516, "authentication failed"),
    ] {
        let body = format!(
            "Code: {code}. DB::Exception: SELECT secret-token FROM private_table https://secret.invalid"
        );
        let error = Error::from_server_response(Phase::Data, 500, None, body.as_bytes());
        assert_eq!(error.cause(), Some(Cause::ServerResponse(Some(code))));
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(error.to_string().contains(description));
        for rendered in [
            error.to_string(),
            format!("{error:?}"),
            error.source().unwrap().to_string(),
        ] {
            for secret in ["SELECT", "secret-token", "private_table", "secret.invalid"] {
                assert!(!rendered.contains(secret));
            }
        }
    }
}

#[test]
fn header_code_takes_precedence_and_malformed_header_falls_back_to_body() {
    let error = Error::from_server_response(Phase::Identity, 403, Some("497"), b"Code: 60. secret");
    assert_eq!(error.cause(), Some(Cause::ServerResponse(Some(497))));
    assert_eq!(error.phase(), Some(Phase::Identity));
    for header in ["0", "4294967295"] {
        let error = Error::from_server_response(Phase::Data, 500, Some(header), b"");
        assert_eq!(
            error.cause(),
            Some(Cause::ServerResponse(Some(header.parse().unwrap())))
        );
    }
    for header in [
        "",
        "+497",
        "-497",
        " 497",
        "497 ",
        "497.secret",
        "4294967296",
        "00000000497",
        "secret-token",
        "４９７",
    ] {
        let error =
            Error::from_server_response(Phase::Control, 500, Some(header), b"Code: 60. secret");
        assert_eq!(error.cause(), Some(Cause::ServerResponse(Some(60))));
    }
}

#[test]
fn body_parser_accepts_only_bounded_leading_numeric_codes() {
    for (body, code) in [
        (&b"Code: 0. secret"[..], Some(0)),
        (&b"Code: 4294967295. secret"[..], Some(u32::MAX)),
        (&b"Code: 60.\xff secret"[..], Some(60)),
        (&b"Code: 60"[..], None),
        (&b"Code: 60x. secret"[..], None),
        (&b"Code: +60. secret"[..], None),
        (&b"Code: -60. secret"[..], None),
        (&b"Code: 4294967296. secret"[..], None),
        (&b"Code: 00000000060. secret"[..], None),
        (&b"Code: . secret"[..], None),
        (&b" Code: 60. secret"[..], None),
        (&b"secret Code: 60. secret"[..], None),
        (&b"\xffCode: 60. secret"[..], None),
    ] {
        let error = Error::from_server_response(Phase::Data, 503, None, body);
        assert_eq!(
            error.cause(),
            Some(
                code.map_or(Cause::HttpStatus(503), |code| Cause::ServerResponse(Some(
                    code
                )))
            )
        );
        assert!(!format!("{error:?}: {error}").contains("secret"));
    }
    let mut body = b"Code: 60. ".to_vec();
    body.extend(vec![b'x'; 1024 * 1024]);
    assert_eq!(
        Error::from_server_response(Phase::Data, 500, None, &body).cause(),
        Some(Cause::ServerResponse(Some(60)))
    );
    let header = "9".repeat(1024 * 1024);
    assert_eq!(
        Error::from_server_response(Phase::Data, 502, Some(&header), b"").cause(),
        Some(Cause::HttpStatus(502))
    );
    let body = format!("Code: {header}. secret");
    assert_eq!(
        Error::from_server_response(Phase::Data, 502, None, body.as_bytes()).cause(),
        Some(Cause::HttpStatus(502))
    );
}

#[test]
fn unrecognized_numeric_codes_remain_available_without_invented_explanations() {
    let error = Error::from_server_response(Phase::Data, 500, None, b"Code: 4000000000. secret");
    assert_eq!(
        error.cause(),
        Some(Cause::ServerResponse(Some(4_000_000_000)))
    );
    assert_eq!(
        error.to_string(),
        "Antalya Data: server response error (code 4000000000)"
    );
}

#[test]
fn refusal_classification_does_not_promote_diagnostics_to_terminal_proof() {
    for code in [115, 164, 497, 516] {
        let error = Error::at(Phase::Data, Cause::ServerResponse(Some(code)));
        assert!(error.is_pre_execution_refusal());
        assert_eq!(error.kind(), ErrorKind::Transport);
    }
    for error in [
        Error::Unconfirmed,
        Error::at(Phase::Data, Cause::HttpStatus(403)),
        Error::at(Phase::Data, Cause::ServerResponse(Some(60))),
        Error::at(Phase::Data, Cause::ServerResponse(None)),
    ] {
        assert!(!error.is_pre_execution_refusal());
    }
}

#[test]
fn read_and_capacity_timeouts_are_distinct_and_errors_remain_copy() {
    fn requires_copy<T: Copy>() {}
    requires_copy::<Error>();
    assert_eq!(
        Error::at(Phase::Data, Cause::ReadTimeout).kind(),
        ErrorKind::Timeout
    );
    assert_eq!(
        Error::at(Phase::Data, Cause::CapacityTimeout).kind(),
        ErrorKind::LimitExceeded
    );
    assert!(Cause::ReadTimeout.to_string().contains("inactivity"));
    assert!(Cause::CapacityTimeout.to_string().contains("capacity"));
}

#[cfg(feature = "arrow")]
#[test]
fn upstream_and_http_diagnostics_share_bounded_numeric_parsing() {
    for message in [
        "Code: 60. secret",
        "Code: 164. secret",
        "Code: 00000000060. secret",
        "Code: 4294967296. secret",
        "Code: +60. secret",
    ] {
        let upstream = clickhouse::error::Error::BadResponse(message.into());
        let error = Error::from_upstream(Phase::Data, &upstream);
        let http = Error::from_server_response(Phase::Data, 500, None, message.as_bytes());
        let expected = match http.cause() {
            Some(Cause::ServerResponse(code)) => Some(Cause::ServerResponse(code)),
            _ => Some(Cause::ServerResponse(None)),
        };
        assert_eq!(error.cause(), expected);
        assert!(!format!("{error:?}: {error}").contains("secret"));
    }
}
