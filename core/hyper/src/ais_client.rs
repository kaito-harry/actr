//! AIS HTTP client
//!
//! Encapsulates the logic for sending protobuf requests to the AIS `/register` endpoint.
//! Supports two registration modes:
//! - Package registration: authenticate with manifest_raw + mfr_signature
//! - Linked registration: authenticate with realm authorization
//!
//! Credential renewal is handled by the Credential Manager via `POST /ais/renew`,
//! not by re-calling `/register` with a PSK.

use std::time::Duration;

use prost::Message;
use tracing::{debug, error, info, warn};

use actr_protocol::{
    ErrorResponse, RegisterRequest, RegisterResponse, RenewCredentialRequest,
    RenewCredentialResponse, renew_credential_response,
};

use crate::error::{HyperError, HyperResult};

/// Structured errors returned by POST /ais/renew.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RenewError {
    #[error("invalid renewal request: {0}")]
    InvalidRequest(String),
    #[error("renewal token rejected")]
    TokenRejected,
    #[error("realm unavailable")]
    RealmUnavailable,
    #[error("renewal rate limited")]
    RateLimited { retry_after: Option<Duration> },
    #[error("retryable renewal error: {0}")]
    Retryable(String),
    #[error("renewal protocol error: {0}")]
    Protocol(String),
}

/// AIS HTTP client
///
/// Encapsulates the logic for sending protobuf requests to the AIS /register endpoint.
/// All requests use `application/x-protobuf` encoding.
pub struct AisClient {
    endpoint: String,
    http: reqwest::Client,
    /// Optional realm secret for `x-actrix-realm-secret` header authentication
    realm_secret: Option<String>,
}

impl AisClient {
    /// Create a new AIS client
    ///
    /// `endpoint` is the AIS base URL, e.g. `"http://ais.example.com:8080"`.
    pub fn new(endpoint: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("reqwest::Client build failed (should never happen)");
        Self {
            endpoint: endpoint.into(),
            http,
            realm_secret: None,
        }
    }

    /// Set the realm secret for authentication
    pub fn with_realm_secret(mut self, secret: impl Into<String>) -> Self {
        self.realm_secret = Some(secret.into());
        self
    }

    /// Initial registration: authenticate with MFR manifest
    ///
    /// Sends a RegisterRequest (containing manifest_raw + mfr_signature),
    /// receives a RegisterResponse.
    pub async fn register_with_manifest(
        &self,
        req: RegisterRequest,
    ) -> HyperResult<RegisterResponse> {
        info!(
            endpoint = %self.endpoint,
            "initial registration: registering with AIS via MFR manifest"
        );
        self.do_register(req).await
    }

    /// Linked registration: authenticate with realm authorization.
    ///
    /// Sends a RegisterRequest marked as linked source mode. AIS authorizes it
    /// using the realm secret header instead of MFR package identity.
    pub async fn register_linked(&self, req: RegisterRequest) -> HyperResult<RegisterResponse> {
        info!(
            endpoint = %self.endpoint,
            "linked registration: registering with AIS via realm authorization"
        );
        self.do_register(req).await
    }

    /// Soft renewal: authenticate with the renewal token bound to the current ActrId.
    pub async fn renew_credential(
        &self,
        req: RenewCredentialRequest,
    ) -> Result<RenewCredentialResponse, RenewError> {
        let base = self.endpoint.to_string().trim_end_matches('/').to_string();
        let url = format!("{}/renew", base);
        let body = req.encode_to_vec();

        debug!(url = %url, body_len = body.len(), "sending AIS renew request");

        // Spawn onto tokio worker to avoid stack overflow on GCD cooperative
        // queues (same root cause as do_register).
        let pending = self
            .http
            .post(&url)
            .header("Content-Type", "application/x-protobuf")
            .header("Accept", "application/x-protobuf")
            .body(body)
            .send();

        let response = tokio::task::spawn(pending)
            .await
            .map_err(|e| RenewError::Retryable(format!("spawn failed: {e}")))?
            .map_err(|e| RenewError::Retryable(format!("HTTP request failed: {e}")))?;

        let status = response.status();
        let retry_after = parse_retry_after(response.headers().get(reqwest::header::RETRY_AFTER));
        let bytes = response
            .bytes()
            .await
            .map_err(|e| RenewError::Retryable(format!("failed to read response body: {e}")))?;

        if !status.is_success() {
            return Err(classify_renew_status(status.as_u16(), retry_after));
        }

        let decoded = RenewCredentialResponse::decode(bytes.as_ref())
            .map_err(|e| RenewError::Protocol(format!("response protobuf decode failed: {e}")))?;

        match decoded.result.as_ref() {
            Some(renew_credential_response::Result::Success(_)) => Ok(decoded),
            Some(renew_credential_response::Result::Error(err)) => Err(classify_renew_error(err)),
            None => Err(RenewError::Protocol(
                "renew response missing result".to_string(),
            )),
        }
    }

    /// Send POST /register request, common logic
    ///
    /// Encodes a RegisterRequest as protobuf and POSTs it to `{endpoint}/register`,
    /// then decodes the response as RegisterResponse.
    ///
    /// The HTTP call is spawned onto the tokio runtime to avoid stack overflow
    /// when polled from a GCD cooperative queue (UNIFFI async bridge context).
    /// reqwest/hyper's deeply nested combinator chain (~140 frames of generic
    /// state machines) exceeds GCD thread stack limits on iOS simulator.
    async fn do_register(&self, req: RegisterRequest) -> HyperResult<RegisterResponse> {
        let base = self.endpoint.to_string().trim_end_matches('/').to_string();
        let url = format!("{}/register", base);

        // encode as protobuf bytes
        let body = req.encode_to_vec();

        debug!(url = %url, body_len = body.len(), "sending AIS register request");

        let mut request_builder = self
            .http
            .post(&url)
            .header("Content-Type", "application/x-protobuf")
            .header("Accept", "application/x-protobuf");

        // Include realm secret header if configured
        if let Some(ref secret) = self.realm_secret {
            request_builder = request_builder.header("x-actrix-realm-secret", secret);
        }

        // Spawn the HTTP work onto a tokio worker thread to avoid overflowing
        // the GCD cooperative queue's limited stack when reqwest/hyper's deeply
        // nested combinator chain is polled inline.
        let pending = request_builder.body(body).send();

        let response = tokio::task::spawn(pending)
            .await
            .map_err(|e| {
                error!(url = %url, error = %e, "AIS spawn failed");
                HyperError::AisBootstrapFailed(format!("spawn failed: {e}"))
            })?
            .map_err(|e| {
                error!(url = %url, error = %e, "AIS HTTP request failed");
                HyperError::AisBootstrapFailed(format!("HTTP request failed: {e}"))
            })?;

        let status = response.status();
        if !status.is_success() {
            warn!(url = %url, status = %status, "AIS returned non-2xx status");
            return Err(HyperError::AisBootstrapFailed(format!(
                "AIS returned error status: {status}"
            )));
        }

        let bytes = response.bytes().await.map_err(|e| {
            error!(url = %url, error = %e, "failed to read AIS response body");
            HyperError::AisBootstrapFailed(format!("failed to read response body: {e}"))
        })?;

        debug!(url = %url, response_len = bytes.len(), "received AIS response");

        let resp = RegisterResponse::decode(bytes.as_ref()).map_err(|e| {
            error!(url = %url, error = %e, "failed to decode AIS RegisterResponse");
            HyperError::AisBootstrapFailed(format!("response protobuf decode failed: {e}"))
        })?;

        Ok(resp)
    }
}

fn parse_retry_after(value: Option<&reqwest::header::HeaderValue>) -> Option<Duration> {
    value
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn classify_renew_error(error: &ErrorResponse) -> RenewError {
    classify_renew_status(error.code as u16, None)
}

fn classify_renew_status(status: u16, retry_after: Option<Duration>) -> RenewError {
    match status {
        400 => RenewError::InvalidRequest("invalid renew request".to_string()),
        401 => RenewError::TokenRejected,
        403 => RenewError::RealmUnavailable,
        429 => RenewError::RateLimited { retry_after },
        500 | 502 | 503 | 504 => RenewError::Retryable(format!("AIS returned {status}")),
        other => RenewError::Protocol(format!("unexpected AIS renew status {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::ErrorResponse;
    use reqwest::header::HeaderValue;

    // ── client construction (no network) ────────────────────────────────────

    #[test]
    fn client_new_holds_endpoint_and_no_secret() {
        let c = AisClient::new("http://ais.example.com:8080");
        assert_eq!(c.endpoint, "http://ais.example.com:8080");
        assert!(c.realm_secret.is_none());

        let chained = c.with_realm_secret("s3cr3t");
        assert_eq!(chained.realm_secret.as_deref(), Some("s3cr3t"));
    }

    #[test]
    fn with_realm_secret_is_chainable_idempotent_builder() {
        let c = AisClient::new("http://ais").with_realm_secret("abc");
        assert_eq!(c.realm_secret.as_deref(), Some("abc"));

        let c2 = c.with_realm_secret("def");
        assert_eq!(c2.endpoint, "http://ais");
        assert_eq!(c2.realm_secret.as_deref(), Some("def"));
    }

    // ── parse_retry_after ───────────────────────────────────────────────────

    #[test]
    fn parse_retry_after_valid_seconds() {
        let h = HeaderValue::from_static("120");
        assert_eq!(parse_retry_after(Some(&h)), Some(Duration::from_secs(120)));
    }

    #[test]
    fn parse_retry_after_none() {
        assert_eq!(parse_retry_after(None), None);
    }

    #[test]
    fn parse_retry_after_non_numeric_is_none() {
        // HTTP-date / non-numeric values are not supported → None.
        let h = HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT");
        assert_eq!(parse_retry_after(Some(&h)), None);

        let h2 = HeaderValue::from_static("abc");
        assert_eq!(parse_retry_after(Some(&h2)), None);
    }

    #[test]
    fn parse_retry_after_zero() {
        let h = HeaderValue::from_static("0");
        assert_eq!(parse_retry_after(Some(&h)), Some(Duration::from_secs(0)));
    }

    // ── classify_renew_status ───────────────────────────────────────────────

    #[test]
    fn classify_status_client_errors() {
        assert!(matches!(
            classify_renew_status(400, None),
            RenewError::InvalidRequest(_)
        ));
        assert!(matches!(
            classify_renew_status(401, None),
            RenewError::TokenRejected
        ));
        assert!(matches!(
            classify_renew_status(403, None),
            RenewError::RealmUnavailable
        ));
    }

    #[test]
    fn classify_status_rate_limited_carries_retry_after() {
        // With header present.
        assert!(matches!(
            classify_renew_status(429, Some(Duration::from_secs(30))),
            RenewError::RateLimited { retry_after: Some(d) } if d == Duration::from_secs(30)
        ));
        // Without header.
        assert!(matches!(
            classify_renew_status(429, None),
            RenewError::RateLimited { retry_after: None }
        ));
    }

    #[test]
    fn classify_status_5xx_retryable() {
        for code in [500u16, 502, 503, 504] {
            assert!(
                matches!(classify_renew_status(code, None), RenewError::Retryable(_)),
                "{code} should be Retryable"
            );
        }
    }

    #[test]
    fn classify_status_unknown_is_protocol() {
        assert!(matches!(
            classify_renew_status(418, None),
            RenewError::Protocol(_)
        ));
    }

    // ── classify_renew_error (delegates to status, retry_after=None) ─────────

    #[test]
    fn classify_error_maps_code_to_variant() {
        // 429 via ErrorResponse path never carries retry_after (header lost).
        let rate = ErrorResponse {
            code: 429,
            message: "slow down".into(),
        };
        assert!(matches!(
            classify_renew_error(&rate),
            RenewError::RateLimited { retry_after: None }
        ));

        let unauth = ErrorResponse {
            code: 401,
            message: "bad token".into(),
        };
        assert!(matches!(
            classify_renew_error(&unauth),
            RenewError::TokenRejected
        ));

        let boom = ErrorResponse {
            code: 503,
            message: "down".into(),
        };
        assert!(matches!(
            classify_renew_error(&boom),
            RenewError::Retryable(_)
        ));
    }

    #[test]
    fn renew_error_display_messages() {
        assert!(format!("{}", RenewError::TokenRejected).contains("rejected"));
        assert!(format!("{}", RenewError::RealmUnavailable).contains("unavailable"));
        assert!(format!("{}", RenewError::InvalidRequest("bad".into())).contains("bad"));
    }
}
