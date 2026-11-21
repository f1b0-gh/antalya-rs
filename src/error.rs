use std::{fmt, io};

/// The operation stage at which a sanitized failure was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Queue,
    Identity,
    Data,
    Control,
    Progress,
    Cancellation,
    Write,
    Validation,
    Sync,
    Publication,
}

/// Transport classification without URLs, headers, SQL or server messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Connect,
    Request,
    Body,
    Decode,
    Other,
}

/// Safe typed source. Original error strings are deliberately not retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    Transport(TransportKind),
    HttpStatus(u16),
    /// A server response is not terminal proof of the target query outcome.
    ServerResponse(Option<u32>),
    IdentityMismatch,
    JsonDecoder,
    Decoder,
    Io(io::ErrorKind),
    Timeout,
    /// No response data arrived within the configured read inactivity timeout.
    ReadTimeout,
    /// Shared retained-output capacity was unavailable before its wait deadline.
    CapacityTimeout,
    Capacity,
    LimitExceeded,
    InvalidProof,
    QueryFailed,
    InvalidSetting,
}
impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(kind) => write!(f, "transport failure ({kind:?})"),
            Self::HttpStatus(status) => write!(f, "HTTP status {status}"),
            Self::ServerResponse(Some(code)) => {
                write!(f, "server response error (code {code})")?;
                if let Some(description) = server_code_description(*code) {
                    write!(f, ": {description}")?;
                }
                Ok(())
            }
            Self::ServerResponse(None) => f.write_str("server response error"),
            Self::IdentityMismatch => {
                f.write_str("authenticated principal does not match authorization")
            }
            Self::JsonDecoder => f.write_str("JSON response decoder failure"),
            Self::Decoder => f.write_str("Arrow decoder failure"),
            Self::Io(kind) => write!(f, "I/O failure ({kind:?})"),
            Self::Timeout => f.write_str("operation deadline elapsed"),
            Self::ReadTimeout => f.write_str("response read inactivity deadline elapsed"),
            Self::CapacityTimeout => f.write_str("shared capacity wait deadline elapsed; release retained output or increase capacity"),
            Self::Capacity => f.write_str("insufficient shared capacity"),
            Self::LimitExceeded => f.write_str("configured resource limit exceeded"),
            Self::InvalidProof => f.write_str("terminal evidence is incomplete or invalid"),
            Self::QueryFailed => f.write_str("terminal evidence proves query failure"),
            Self::InvalidSetting => f.write_str("invalid configuration"),
        }
    }
}
impl std::error::Error for Cause {}

/// Stable category shared by legacy errors and contextual diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidEndpoint,
    InvalidCredentials,
    InvalidQuery,
    InvalidSetting,
    InvalidParameter,
    InvalidSnapshot,
    InvalidRequest,
    Unsupported,
    Transport,
    Timeout,
    InvalidIpc,
    LimitExceeded,
    Publication,
    Unconfirmed,
    QueryFailed,
}

/// Errors contain no SQL, credentials, endpoint text or raw upstream messages.
/// Legacy variants remain available; use [`Self::kind`] to classify all errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidEndpoint,
    InvalidCredentials,
    InvalidQuery,
    InvalidSetting,
    InvalidParameter,
    InvalidSnapshot,
    InvalidRequest,
    Unsupported,
    Transport,
    Timeout,
    InvalidIpc,
    LimitExceeded,
    Publication,
    Unconfirmed,
    QueryFailed,
    Context { phase: Phase, cause: Cause },
}
impl Error {
    pub const fn at(phase: Phase, cause: Cause) -> Self {
        Self::Context { phase, cause }
    }

    pub const fn phase(self) -> Option<Phase> {
        match self {
            Self::Context { phase, .. } => Some(phase),
            _ => None,
        }
    }

    pub const fn cause(self) -> Option<Cause> {
        match self {
            Self::Context { cause, .. } => Some(cause),
            _ => None,
        }
    }

    pub const fn kind(self) -> ErrorKind {
        match self {
            Self::InvalidEndpoint => ErrorKind::InvalidEndpoint,
            Self::InvalidCredentials => ErrorKind::InvalidCredentials,
            Self::InvalidQuery => ErrorKind::InvalidQuery,
            Self::InvalidSetting => ErrorKind::InvalidSetting,
            Self::InvalidParameter => ErrorKind::InvalidParameter,
            Self::InvalidSnapshot => ErrorKind::InvalidSnapshot,
            Self::InvalidRequest => ErrorKind::InvalidRequest,
            Self::Unsupported => ErrorKind::Unsupported,
            Self::Transport => ErrorKind::Transport,
            Self::Timeout => ErrorKind::Timeout,
            Self::InvalidIpc => ErrorKind::InvalidIpc,
            Self::LimitExceeded => ErrorKind::LimitExceeded,
            Self::Publication => ErrorKind::Publication,
            Self::Unconfirmed => ErrorKind::Unconfirmed,
            Self::QueryFailed => ErrorKind::QueryFailed,
            Self::Context { phase, cause } => match cause {
                Cause::Transport(_)
                | Cause::HttpStatus(_)
                | Cause::ServerResponse(_)
                | Cause::JsonDecoder => ErrorKind::Transport,
                Cause::IdentityMismatch => ErrorKind::InvalidCredentials,
                Cause::Decoder => ErrorKind::InvalidIpc,
                Cause::Io(_) => match phase {
                    Phase::Write | Phase::Sync | Phase::Publication => ErrorKind::Publication,
                    Phase::Validation => ErrorKind::InvalidIpc,
                    _ => ErrorKind::Transport,
                },
                Cause::Timeout | Cause::ReadTimeout => ErrorKind::Timeout,
                Cause::Capacity | Cause::CapacityTimeout | Cause::LimitExceeded => {
                    ErrorKind::LimitExceeded
                }
                Cause::InvalidProof => ErrorKind::Unconfirmed,
                Cause::QueryFailed => ErrorKind::QueryFailed,
                Cause::InvalidSetting => ErrorKind::InvalidSetting,
            },
        }
    }

    /// Retain only safe classifications, never the request URL or error message.
    pub fn from_reqwest(phase: Phase, error: &reqwest::Error) -> Self {
        let cause = if error.is_timeout() {
            Cause::Timeout
        } else if let Some(status) = error.status() {
            Cause::HttpStatus(status.as_u16())
        } else {
            Cause::Transport(if error.is_connect() {
                TransportKind::Connect
            } else if error.is_body() {
                TransportKind::Body
            } else if error.is_decode() {
                TransportKind::Decode
            } else if error.is_request() || error.is_builder() {
                TransportKind::Request
            } else {
                TransportKind::Other
            })
        };
        Self::at(phase, cause)
    }

    /// Retain the I/O kind without paths or operating-system message text.
    pub fn from_io(phase: Phase, error: &io::Error) -> Self {
        Self::at(phase, Cause::Io(error.kind()))
    }

    /// Classify a failed HTTP response without retaining any server text.
    ///
    /// Prefer a strictly numeric `X-ClickHouse-Exception-Code` header, then a
    /// leading `Code: N.` body prefix. Parsing examines at most ten digits and
    /// never scans the remaining body. Missing or malformed codes fall back to
    /// the HTTP status; unknown numeric codes remain available. Callers must
    /// bound response reads independently.
    pub fn from_server_response(
        phase: Phase,
        status: u16,
        exception_header: Option<&str>,
        body: &[u8],
    ) -> Self {
        let code = exception_header
            .and_then(|header| parse_server_code(header.as_bytes()))
            .or_else(|| body_server_code(body));
        Self::at(
            phase,
            code.map_or(Cause::HttpStatus(status), |code| {
                Cause::ServerResponse(Some(code))
            }),
        )
    }

    /// Whether this code commonly indicates a request setting or access refusal.
    ///
    /// This permits prompt reporting when terminal query-log evidence may never
    /// appear. It is not proof that execution never started or that remote work
    /// stopped, and must not be promoted to a confirmed terminal outcome.
    pub const fn is_pre_execution_refusal(self) -> bool {
        matches!(
            self,
            Self::Context {
                cause: Cause::ServerResponse(Some(115 | 164 | 497 | 516)),
                ..
            }
        )
    }

    /// Classify upstream decoder/transport failures without raw server text.
    #[cfg(feature = "arrow")]
    pub fn from_upstream(phase: Phase, error: &clickhouse::error::Error) -> Self {
        use clickhouse::error::Error as Upstream;
        let cause = match error {
            Upstream::TimedOut => Cause::Timeout,
            Upstream::BadResponse(message) => {
                Cause::ServerResponse(body_server_code(message.as_bytes()))
            }
            Upstream::DataFormat(_)
            | Upstream::Decompression(_)
            | Upstream::NotEnoughData
            | Upstream::InvalidUtf8Encoding(_)
            | Upstream::InvalidTagEncoding(_)
            | Upstream::InvalidColumnsHeader(_)
            | Upstream::SchemaMismatch(_) => Cause::Decoder,
            Upstream::Other(source) if source.is::<arrow_schema::ArrowError>() => Cause::Decoder,
            Upstream::Other(source) => source
                .downcast_ref::<io::Error>()
                .map_or(Cause::Transport(TransportKind::Other), |io| {
                    Cause::Io(io.kind())
                }),
            Upstream::Network(source) => {
                let kind = source
                    .downcast_ref::<hyper_util::client::legacy::Error>()
                    .filter(|error| error.is_connect())
                    .map_or(TransportKind::Other, |_| TransportKind::Connect);
                Cause::Transport(kind)
            }
            // A BadResponse alone is not fresh terminal proof of the query outcome.
            _ => Cause::Transport(TransportKind::Other),
        };
        Self::at(phase, cause)
    }
}

fn parse_server_code(digits: &[u8]) -> Option<u32> {
    if digits.is_empty() || digits.len() > 10 {
        return None;
    }
    digits.iter().try_fold(0_u32, |code, byte| {
        if !byte.is_ascii_digit() {
            return None;
        }
        code.checked_mul(10)?.checked_add(u32::from(byte - b'0'))
    })
}

fn body_server_code(body: &[u8]) -> Option<u32> {
    let remainder = body.strip_prefix(b"Code: ")?;
    let end = remainder.iter().take(11).position(|byte| *byte == b'.')?;
    parse_server_code(&remainder[..end])
}

// Fixed descriptions only: arbitrary exception text can contain SQL, credentials,
// endpoint names, and user-controlled identifiers that cannot be safely filtered.
fn server_code_description(code: u32) -> Option<&'static str> {
    match code {
        60 => Some("unknown table"),
        62 => Some("query syntax error"),
        81 => Some("unknown database"),
        115 => Some("unknown setting"),
        164 => Some("read-only policy rejected query or setting change"),
        497 => Some("access denied"),
        516 => Some("authentication failed"),
        _ => None,
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Self::Context { phase, cause } = self {
            return write!(f, "Antalya {phase:?}: {cause}");
        }
        f.write_str(match self.kind() {
            ErrorKind::InvalidEndpoint => "invalid Antalya endpoint",
            ErrorKind::InvalidCredentials => "invalid Antalya credentials",
            ErrorKind::InvalidQuery => "invalid Antalya query envelope",
            ErrorKind::InvalidSetting => "invalid or reserved Antalya setting",
            ErrorKind::InvalidParameter => "invalid Antalya parameter name",
            ErrorKind::InvalidSnapshot => "Iceberg snapshot ID must be positive",
            ErrorKind::Unsupported => "unsupported Antalya capability or version",
            ErrorKind::Transport => "Antalya transport or server error",
            ErrorKind::Timeout => "Antalya operation timed out; server outcome is unknown",
            ErrorKind::InvalidIpc => "invalid or incomplete Arrow IPC",
            ErrorKind::LimitExceeded => "Antalya response exceeds configured limit",
            ErrorKind::Publication => "could not publish Antalya artifact",
            ErrorKind::Unconfirmed => "Antalya terminal outcome is unconfirmed",
            ErrorKind::QueryFailed => "Antalya query failed",
            ErrorKind::InvalidRequest => "could not construct Antalya request",
        })
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Context { cause, .. } => Some(cause),
            _ => None,
        }
    }
}
