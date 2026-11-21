//! Bound credential expiry to a reviewed receiver deployment, not an authority
//! inferred from decoded claims. The receiver still authenticates every request.
use crate::{AntalyaClient, Config, Credentials, Endpoint};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{
    Deserialize,
    de::{self, IgnoredAny, MapAccess, Visitor},
};
use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

/// The host must independently review and enforce this profile on every HTTP
/// entry point: JWT signature validation with the installed key/algorithm,
/// exact issuer/audience, username_claim=sub, required exp, verifier_leeway=0,
/// token_cache_lifetime=0, and the declared maximum server-clock lag relative
/// to the trusted host clock. This is deployment configuration, not a worker
/// claim or discovered/attested server state. It is deliberately not serde data.
/// No revocation, owner fencing or cryptographic verification is supplied here.
pub struct JwtReceiverProfile {
    endpoint: Endpoint,
    issuer: String,
    audience: String,
    principal: String,
    max_clock_lag_seconds: u32,
}
impl JwtReceiverProfile {
    pub fn new(
        endpoint: Endpoint,
        issuer: &str,
        audience: &str,
        principal: &str,
        max_clock_lag_seconds: u32,
    ) -> Result<Self, ReceiverError> {
        if endpoint.0.as_str().len() > 4096
            || [issuer, audience, principal].iter().any(|s| {
                s.is_empty() || s.len() > 1024 || s.trim() != *s || s.chars().any(char::is_control)
            })
        {
            return Err(ReceiverError::Profile);
        }
        Ok(Self {
            endpoint,
            issuer: issuer.into(),
            audience: audience.into(),
            principal: principal.into(),
            max_clock_lag_seconds,
        })
    }
}

pub enum ReceiverError {
    Profile,
    Unreviewed,
    Token,
    Limit,
    Expiry,
    Scope,
    Deadline,
    Expired,
    Encoding(base64::DecodeSliceError),
    Json(serde_json::Error),
    Credentials(crate::Error),
    Clock(std::time::SystemTimeError),
}
impl fmt::Display for ReceiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Profile => "invalid reviewed JWT receiver profile",
            Self::Unreviewed => "receiver expiry profile is required",
            Self::Token => "unsupported compact JWT profile",
            Self::Limit => "JWT metadata exceeds bounds",
            Self::Expiry => "JWT expiry is missing or cannot be bounded",
            Self::Scope => "JWT or execution scope differs from the receiver profile",
            Self::Deadline => "receiver expiry exceeds the dispatch deadline",
            Self::Expired => "credential or dispatch admission has expired",
            Self::Encoding(_) => "JWT metadata encoding is invalid",
            Self::Json(_) => "JWT metadata shape is invalid",
            Self::Credentials(_) => "JWT bearer credentials are invalid",
            Self::Clock(_) => "host clock is before the Unix epoch",
        })
    }
}
impl fmt::Debug for ReceiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ReceiverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encoding(e) => Some(e),
            Self::Json(e) => Some(e),
            Self::Credentials(e) => Some(e),
            Self::Clock(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ReceiverExpiry {
    principal: String,
    expires: u64,
    refuses_by: u64,
}
impl fmt::Debug for ReceiverExpiry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiverExpiry")
            .field("refuses_by", &self.refuses_by)
            .finish_non_exhaustive()
    }
}
impl ReceiverExpiry {
    pub(crate) fn check(&self, principal: Option<&str>, before: u64) -> Result<(), ReceiverError> {
        if principal != Some(self.principal.as_str()) {
            return Err(ReceiverError::Scope);
        }
        if self.refuses_by > before {
            return Err(ReceiverError::Deadline);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(ReceiverError::Clock)?
            .as_secs();
        if now >= self.expires || now >= before {
            return Err(ReceiverError::Expired);
        }
        Ok(())
    }
}
impl Config {
    /// Freeze the exact bearer with its expiry bound in this configuration.
    /// Claims constrain sending; they do not authorize it or verify a signature.
    /// The host owns the reviewed profile and supplies separate current authority
    /// at dispatch. Guarded Arrow/command admission applies its external deadline.
    /// Integer NumericDate values must be in 1..=u32::MAX (through 2106),
    /// keeping receiver time conversion and one-second precision bounded.
    pub fn jwt_receiver(profile: JwtReceiverProfile, bearer: &str) -> Result<Self, ReceiverError> {
        if bearer.len() > 8192 {
            return Err(ReceiverError::Limit);
        }
        let mut pieces = bearer.split('.');
        let header = pieces.next().ok_or(ReceiverError::Token)?;
        let payload = pieces.next().ok_or(ReceiverError::Token)?;
        let signature = pieces.next().ok_or(ReceiverError::Token)?;
        if pieces.next().is_some()
            || signature.is_empty()
            || signature.len() % 4 == 1
            || !signature
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        {
            return Err(ReceiverError::Token);
        }
        {
            let header = decode::<1024>(header)?;
            if header
                .alg
                .as_deref()
                .is_none_or(|alg| alg.is_empty() || alg == "none")
                || header.typ.as_deref().is_some_and(|typ| typ != "JWT")
            {
                return Err(ReceiverError::Token);
            }
        }
        let claims = decode::<4096>(payload)?;
        if claims.issuer.as_deref() != Some(profile.issuer.as_str())
            || claims.audience.as_deref() != Some(profile.audience.as_str())
            || claims.subject.as_deref() != Some(profile.principal.as_str())
        {
            return Err(ReceiverError::Scope);
        }
        let expires = claims
            .expires
            .filter(|v| (1..=u64::from(u32::MAX)).contains(v))
            .ok_or(ReceiverError::Expiry)?;
        // One extra second covers the receiver's integer NumericDate precision.
        // This is a conservative bound, not a grace period extending the grant.
        let refuses_by = expires
            .checked_add(u64::from(profile.max_clock_lag_seconds))
            .and_then(|v| v.checked_add(1))
            .ok_or(ReceiverError::Expiry)?;
        let mut config = Self::new(
            profile.endpoint,
            Credentials::bearer(bearer).map_err(ReceiverError::Credentials)?,
        );
        config.receiver_expiry = Some(ReceiverExpiry {
            principal: profile.principal,
            expires,
            refuses_by,
        });
        Ok(config)
    }
    /// Unix-second refusal bound, conditional on the host-reviewed deployment
    /// and clock profile. Metadata alone supplies no permission to dispatch.
    pub fn receiver_refuses_by(&self) -> Option<u64> {
        self.receiver_expiry.as_ref().map(|v| v.refuses_by)
    }
}

impl AntalyaClient {
    /// Select host-supplied JWT credentials for future preparations at the same
    /// endpoint, retaining this client's limits, pools and shared resource budget.
    /// Existing preparations and responses keep their original credentials and
    /// deadlines. Quarantined capacity is not reset by creating another view.
    ///
    /// This performs no I/O, token acquisition or signature verification. The host
    /// must review the receiver profile and independently authorize each operation.
    pub fn with_jwt_receiver(
        &self,
        profile: JwtReceiverProfile,
        bearer: &str,
    ) -> Result<Self, ReceiverError> {
        if profile.endpoint.0 != self.config.endpoint.0 {
            return Err(ReceiverError::Scope);
        }
        let selected = Config::jwt_receiver(profile, bearer)?;
        let mut client = self.clone();
        client.config.credentials = selected.credentials;
        client.config.receiver_expiry = selected.receiver_expiry;
        client.upstream = client.upstream.with_access_token(bearer);
        Ok(client)
    }
}

#[derive(Default)]
struct Claims {
    issuer: Option<String>,
    audience: Option<String>,
    subject: Option<String>,
    expires: Option<u64>,
    alg: Option<String>,
    typ: Option<String>,
}
fn decode<const N: usize>(encoded: &str) -> Result<Claims, ReceiverError> {
    if encoded.len() > N * 4 / 3 + 4 {
        return Err(ReceiverError::Limit);
    }
    let mut decoded = [0_u8; N];
    let len = URL_SAFE_NO_PAD
        .decode_slice(encoded, &mut decoded)
        .map_err(ReceiverError::Encoding)?;
    serde_json::from_slice(decoded.get(..len).ok_or(ReceiverError::Limit)?)
        .map_err(ReceiverError::Json)
}
impl<'de> Deserialize<'de> for Claims {
    fn deserialize<D: de::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Members;
        impl<'de> Visitor<'de> for Members {
            type Value = Claims;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bounded JWT object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Claims, A::Error> {
                let mut seen = BTreeMap::new();
                let mut claims = Claims::default();
                while let Some(key) = map.next_key::<String>()? {
                    if seen.len() >= 32 {
                        return Err(de::Error::custom("too many JWT members"));
                    }
                    let Entry::Vacant(entry) = seen.entry(key) else {
                        return Err(de::Error::custom("duplicate JWT member"));
                    };
                    match entry.key().as_str() {
                        "iss" => claims.issuer = Some(map.next_value()?),
                        "aud" => claims.audience = Some(map.next_value()?),
                        "sub" => claims.subject = Some(map.next_value()?),
                        "exp" => claims.expires = Some(map.next_value()?),
                        "alg" => claims.alg = Some(map.next_value()?),
                        "typ" => claims.typ = Some(map.next_value()?),
                        "crit" | "b64" => {
                            return Err(de::Error::custom("unsupported JWT extension"));
                        }
                        _ => {
                            let _: IgnoredAny = map.next_value()?;
                        }
                    }
                    entry.insert(());
                }
                Ok(claims)
            }
        }
        decoder.deserialize_map(Members)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cause, Error, ExecutionLimits, ResourceLimits, resources::Lane};
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
        time::Duration,
    };
    use tokio::time::Instant;

    #[tokio::test]
    async fn selected_credentials_share_all_lanes_and_retained_bytes()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = Endpoint::parse("http://127.0.0.1:1")?;
        let base = AntalyaClient::new(
            Config::new(endpoint.clone(), Credentials::bearer("original")?)
                .resources(ResourceLimits {
                    max_active_queries: 1,
                    max_control_queries: 1,
                    max_export_tasks: 1,
                    max_queued_queries: 0,
                    max_retained_bytes: 8,
                })
                .execution_limits(ExecutionLimits {
                    max_batch_bytes: 8,
                    ..ExecutionLimits::default()
                })
                .database("events")?
                .timeout(Duration::from_secs(7)),
        )?;
        let profile = JwtReceiverProfile::new(endpoint, "issuer", "audience", "fixture", 0)?;
        let selected = base.with_jwt_receiver(profile,
            "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJpc3N1ZXIiLCJhdWQiOiJhdWRpZW5jZSIsInN1YiI6ImZpeHR1cmUiLCJleHAiOjQxMDI0NDQ4MDB9.c2ln")?;
        assert!(matches!(
            selected.with_bearer("opaque"),
            Err(Error::Unsupported)
        ));
        for (selected, expiry) in [
            (base.with_bearer("rotated")?, None),
            (selected, Some(4_102_444_801)),
        ] {
            assert_eq!(base.config.database, selected.config.database);
            assert_eq!(base.config.timeout, selected.config.timeout);
            assert_eq!(
                base.config.execution_limits,
                selected.config.execution_limits
            );
            assert_eq!(base.config.terminal_policy, selected.config.terminal_policy);
            assert_eq!(base.config.receiver_refuses_by(), None);
            assert_eq!(selected.config.receiver_refuses_by(), expiry);
            let deadline = || Instant::now() + Duration::from_secs(2);
            for lane in [Lane::Data, Lane::Control, Lane::Export] {
                let lease = base.resources.acquire(lane, deadline()).await?;
                assert!(matches!(selected.resources.acquire(lane, deadline()).await,
                Err(ref error) if error.cause() == Some(Cause::Capacity)));
                drop(lease);
                drop(selected.resources.acquire(lane, deadline()).await?);
            }
            let retained = base.resources.reserve_bytes(8, deadline()).await?;
            let mut wait = Box::pin(selected.resources.reserve_bytes(1, deadline()));
            assert!(matches!(
                wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            drop(retained);
            let byte = wait.await?;
            assert_eq!(byte.num_permits(), 1);
        }
        Ok(())
    }
}
