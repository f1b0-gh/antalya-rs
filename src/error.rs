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
            Self::ServerResponse(Some(code)) => write!(f, "server response error (code {code})"),
            Self::ServerResponse(None) => f.write_str("server response error"),
            Self::IdentityMismatch => {
                f.write_str("authenticated principal does not match authorization")
            }
            Self::JsonDecoder => f.write_str("JSON response decoder failure"),
            Self::Decoder => f.write_str("Arrow decoder failure"),
            Self::Io(kind) => write!(f, "I/O failure ({kind:?})"),
            Self::Timeout => f.write_str("operation deadline elapsed"),
            Self::Capacity => f.write_str("operation queue is full"),
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
                Cause::Timeout => ErrorKind::Timeout,
                Cause::Capacity | Cause::LimitExceeded => ErrorKind::LimitExceeded,
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

    /// Classify upstream decoder/transport failures without raw server text.
    #[cfg(feature = "arrow")]
    pub fn from_upstream(phase: Phase, error: &clickhouse::error::Error) -> Self {
        use clickhouse::error::Error as Upstream;
        let cause = match error {
            Upstream::TimedOut => Cause::Timeout,
            Upstream::BadResponse(message) => {
                let code = message
                    .strip_prefix("Code: ")
                    .and_then(|text| text.split_once('.'))
                    .map(|(digits, _)| digits)
                    .filter(|digits| {
                        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
                    })
                    .and_then(|digits| digits.parse::<u32>().ok());
                Cause::ServerResponse(code)
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
