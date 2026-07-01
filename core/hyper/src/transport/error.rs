//! Network layer error definitions

use actr_protocol::{ActrError, Classify, ErrorKind};
use thiserror::Error;

/// Network layer error types
#[derive(Error, Debug)]
pub enum NetworkError {
    /// Connection error
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// Signaling error
    #[error("Signaling error: {0}")]
    SignalingError(String),

    /// WebRTC error
    #[error("WebRTC error: {0}")]
    WebRtcError(String),

    /// Protocol error
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    /// Timeout error
    #[error("Timeout error: {0}")]
    TimeoutError(String),

    /// Authentication error
    #[error("Authentication error: {0}")]
    AuthenticationError(String),

    /// Credential expired error (requires re-registration)
    #[error("Credential expired: {0}")]
    CredentialExpired(String),

    /// Permission error
    #[error("Permission error: {0}")]
    PermissionError(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    /// Resource exhausted error
    #[error("Resource exhausted: {0}")]
    ResourceExhaustedError(String),

    /// Network unreachable error
    #[error("Network unreachable: {0}")]
    NetworkUnreachableError(String),

    /// Service discovery error
    #[error("Service discovery error: {0}")]
    ServiceDiscoveryError(String),

    /// NAT traversal error
    #[error("NAT traversal error: {0}")]
    NatTraversalError(String),

    /// Data channel error
    #[error("Data channel error: {0}")]
    DataChannelError(String),

    /// Broadcast error
    #[error("Broadcast error: {0}")]
    BroadcastError(String),

    /// ICE error
    #[error("ICE error: {0}")]
    IceError(String),

    /// DTLS error
    #[error("DTLS error: {0}")]
    DtlsError(String),

    /// STUN/TURN error
    #[error("STUN/TURN error: {0}")]
    StunTurnError(String),

    /// WebSocket error
    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    /// Connection not found error
    #[error("Connection not found: {0}")]
    ConnectionNotFound(String),

    /// Connection closed error (e.g., cancelled during creation)
    #[error("Connection closed: {0}")]
    ConnectionClosed(String),

    /// Feature not implemented error
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// Channel closed error
    #[error("Channel closed: {0}")]
    ChannelClosed(String),

    /// Send error
    #[error("Send error: {0}")]
    SendError(String),

    /// No route error
    #[error("No route: {0}")]
    NoRoute(String),

    /// Invalid operation error
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Invalid argument error
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Channel not found error
    #[error("Channel not found: {0}")]
    ChannelNotFound(String),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// URL parse error
    #[error("URL parse error: {0}")]
    UrlParseError(#[from] url::ParseError),

    /// JSON error
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Other error
    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

impl Classify for NetworkError {
    fn kind(&self) -> ErrorKind {
        match self {
            // Transient: connection-level failures that may resolve on retry
            NetworkError::ConnectionError(_)
            | NetworkError::ConnectionClosed(_)
            | NetworkError::ChannelClosed(_)
            | NetworkError::SendError(_)
            | NetworkError::NetworkUnreachableError(_)
            | NetworkError::ResourceExhaustedError(_)
            | NetworkError::WebSocketError(_)
            | NetworkError::SignalingError(_)
            | NetworkError::WebRtcError(_)
            | NetworkError::NatTraversalError(_)
            | NetworkError::IceError(_) => ErrorKind::Transient,

            // Transient: timeout (framework-internal; caller-set deadlines should be Client)
            NetworkError::TimeoutError(_) => ErrorKind::Transient,

            // Client: caller or config errors that won't fix themselves
            NetworkError::ConnectionNotFound(_)
            | NetworkError::ChannelNotFound(_)
            | NetworkError::NoRoute(_)
            | NetworkError::InvalidArgument(_)
            | NetworkError::InvalidOperation(_)
            | NetworkError::ConfigurationError(_)
            | NetworkError::ServiceDiscoveryError(_) => ErrorKind::Client,

            // Client: auth/permission
            NetworkError::AuthenticationError(_)
            | NetworkError::PermissionError(_)
            | NetworkError::CredentialExpired(_) => ErrorKind::Client,

            // Corrupt: data cannot be decoded
            NetworkError::DeserializationError(_) => ErrorKind::Corrupt,

            // Internal: framework-level issues
            NetworkError::ProtocolError(_)
            | NetworkError::SerializationError(_)
            | NetworkError::DataChannelError(_)
            | NetworkError::BroadcastError(_)
            | NetworkError::DtlsError(_)
            | NetworkError::StunTurnError(_)
            | NetworkError::NotImplemented(_)
            | NetworkError::IoError(_)
            | NetworkError::UrlParseError(_)
            | NetworkError::JsonError(_)
            | NetworkError::Other(_) => ErrorKind::Internal,
        }
    }
}

impl NetworkError {
    /// Get error category
    pub fn category(&self) -> &'static str {
        match self {
            NetworkError::ConnectionError(_) => "connection",
            NetworkError::SignalingError(_) => "signaling",
            NetworkError::WebRtcError(_) => "webrtc",
            NetworkError::ProtocolError(_) => "protocol",
            NetworkError::SerializationError(_) | NetworkError::DeserializationError(_) => {
                "serialization"
            }
            NetworkError::TimeoutError(_) => "timeout",
            NetworkError::AuthenticationError(_) => "authentication",
            NetworkError::PermissionError(_) => "permission",
            NetworkError::ConfigurationError(_) => "configuration",
            NetworkError::ResourceExhaustedError(_) => "resource_exhausted",
            NetworkError::NetworkUnreachableError(_) => "network_unreachable",
            NetworkError::ServiceDiscoveryError(_) => "service_discovery",
            NetworkError::NatTraversalError(_) => "nat_traversal",
            NetworkError::DataChannelError(_) => "data_channel",
            NetworkError::IceError(_) => "ice",
            NetworkError::DtlsError(_) => "dtls",
            NetworkError::StunTurnError(_) => "stun_turn",
            NetworkError::WebSocketError(_) => "websocket",
            NetworkError::ConnectionNotFound(_) => "connection_not_found",
            NetworkError::ConnectionClosed(_) => "connection_closed",
            NetworkError::NotImplemented(_) => "not_implemented",
            NetworkError::ChannelClosed(_) => "channel_closed",
            NetworkError::SendError(_) => "send_error",
            NetworkError::NoRoute(_) => "no_route",
            NetworkError::InvalidOperation(_) => "invalid_operation",
            NetworkError::InvalidArgument(_) => "invalid_argument",
            NetworkError::ChannelNotFound(_) => "channel_not_found",
            NetworkError::IoError(_) => "io",
            NetworkError::UrlParseError(_) => "url_parse",
            NetworkError::JsonError(_) => "json",
            NetworkError::BroadcastError(_) => "broadcast",
            NetworkError::CredentialExpired(_) => "credential_expired",
            NetworkError::Other(_) => "other",
        }
    }

    /// Get error severity (1-10, 10 is most severe)
    pub fn severity(&self) -> u8 {
        match self {
            NetworkError::ConfigurationError(_)
            | NetworkError::AuthenticationError(_)
            | NetworkError::PermissionError(_)
            | NetworkError::CredentialExpired(_) => 10,

            NetworkError::WebRtcError(_)
            | NetworkError::SignalingError(_)
            | NetworkError::ProtocolError(_) => 8,

            NetworkError::ConnectionError(_) | NetworkError::NetworkUnreachableError(_) => 7,

            NetworkError::NatTraversalError(_)
            | NetworkError::IceError(_)
            | NetworkError::DtlsError(_) => 6,

            NetworkError::TimeoutError(_) | NetworkError::ResourceExhaustedError(_) => 5,

            NetworkError::ServiceDiscoveryError(_)
            | NetworkError::DataChannelError(_)
            | NetworkError::BroadcastError(_) => 4,

            NetworkError::SerializationError(_) | NetworkError::DeserializationError(_) => 3,

            NetworkError::WebSocketError(_) | NetworkError::StunTurnError(_) => 3,

            NetworkError::ConnectionNotFound(_)
            | NetworkError::ConnectionClosed(_)
            | NetworkError::ChannelClosed(_)
            | NetworkError::SendError(_)
            | NetworkError::NoRoute(_)
            | NetworkError::ChannelNotFound(_) => 4,

            NetworkError::InvalidOperation(_) | NetworkError::InvalidArgument(_) => 6,

            NetworkError::NotImplemented(_) => 8,

            NetworkError::IoError(_)
            | NetworkError::UrlParseError(_)
            | NetworkError::JsonError(_) => 2,

            NetworkError::Other(_) => 1,
        }
    }
}

// TODO: Implement UnifiedError trait (when actr-protocol provides error_unified module)
// impl UnifiedError for NetworkError { ... }

/// Network layer result type
pub type NetworkResult<T> = Result<T, NetworkError>;

/// Convert from `ActrIdError` (identity parsing) to `NetworkError`
impl From<actr_protocol::ActrIdError> for NetworkError {
    fn from(err: actr_protocol::ActrIdError) -> Self {
        NetworkError::InvalidArgument(err.to_string())
    }
}

/// Convert `NetworkError` to the public top-level `ActrError`.
///
/// This is the single boundary where transport failures become user-visible errors.
impl From<NetworkError> for ActrError {
    fn from(err: NetworkError) -> Self {
        // Preserve specific variants where the protocol surface has a precise
        // counterpart (e.g. caller-deadline TimedOut), so binding consumers can
        // branch on the exact failure mode instead of a coarse Unavailable.
        match &err {
            NetworkError::TimeoutError(_) => return ActrError::TimedOut,
            NetworkError::PermissionError(msg)
            | NetworkError::AuthenticationError(msg)
            | NetworkError::CredentialExpired(msg) => {
                return ActrError::PermissionDenied(msg.clone());
            }
            NetworkError::NoRoute(msg)
            | NetworkError::ConnectionNotFound(msg)
            | NetworkError::ChannelNotFound(msg)
            | NetworkError::ServiceDiscoveryError(msg) => {
                return ActrError::NotFound(msg.clone());
            }
            _ => {}
        }
        match err.kind() {
            ErrorKind::Transient => ActrError::Unavailable(err.to_string()),
            ErrorKind::Client => ActrError::NotFound(err.to_string()),
            ErrorKind::Corrupt => ActrError::DecodeFailure(err.to_string()),
            ErrorKind::Internal => ActrError::Internal(err.to_string()),
        }
    }
}

/// Convert from WebRTC error
impl From<webrtc::Error> for NetworkError {
    fn from(err: webrtc::Error) -> Self {
        NetworkError::WebRtcError(err.to_string())
    }
}

/// Convert from WebSocket error
impl From<tokio_tungstenite::tungstenite::Error> for NetworkError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        NetworkError::WebSocketError(err.to_string())
    }
}

/// Convert from protobuf encode error
impl From<actr_protocol::prost::EncodeError> for NetworkError {
    fn from(err: actr_protocol::prost::EncodeError) -> Self {
        NetworkError::SerializationError(err.to_string())
    }
}

/// Convert from protobuf decode error
impl From<actr_protocol::prost::DecodeError> for NetworkError {
    fn from(err: actr_protocol::prost::DecodeError) -> Self {
        NetworkError::DeserializationError(err.to_string())
    }
}

// TODO: In future, if error statistics needed, can add ErrorStats struct
// Recommend using arrays instead of HashMap (error categories and severities are fixed)

#[cfg(test)]
mod tests {
    use super::*;

    // ── NetworkError::kind() classification ──────────────────────────────────

    #[test]
    fn transient_network_errors() {
        let cases = [
            NetworkError::ConnectionError("x".into()),
            NetworkError::ConnectionClosed("x".into()),
            NetworkError::ChannelClosed("x".into()),
            NetworkError::SendError("x".into()),
            NetworkError::NetworkUnreachableError("x".into()),
            NetworkError::ResourceExhaustedError("x".into()),
            NetworkError::WebSocketError("x".into()),
            NetworkError::SignalingError("x".into()),
            NetworkError::WebRtcError("x".into()),
            NetworkError::NatTraversalError("x".into()),
            NetworkError::IceError("x".into()),
            NetworkError::TimeoutError("x".into()),
        ];
        for e in &cases {
            assert_eq!(e.kind(), ErrorKind::Transient, "{e} should be Transient");
            assert!(e.is_retryable(), "{e} should be retryable");
        }
    }

    #[test]
    fn client_network_errors() {
        let cases = [
            NetworkError::ConnectionNotFound("x".into()),
            NetworkError::ChannelNotFound("x".into()),
            NetworkError::NoRoute("x".into()),
            NetworkError::InvalidArgument("x".into()),
            NetworkError::InvalidOperation("x".into()),
            NetworkError::ConfigurationError("x".into()),
            NetworkError::ServiceDiscoveryError("x".into()),
            NetworkError::AuthenticationError("x".into()),
            NetworkError::PermissionError("x".into()),
            NetworkError::CredentialExpired("x".into()),
        ];
        for e in &cases {
            assert_eq!(e.kind(), ErrorKind::Client, "{e} should be Client");
            assert!(!e.is_retryable(), "{e} should not be retryable");
        }
    }

    #[test]
    fn corrupt_network_error() {
        let e = NetworkError::DeserializationError("bad bytes".into());
        assert_eq!(e.kind(), ErrorKind::Corrupt);
        assert!(e.requires_dlq());
        assert!(!e.is_retryable());
    }

    #[test]
    fn internal_network_errors() {
        let cases = [
            NetworkError::ProtocolError("x".into()),
            NetworkError::SerializationError("x".into()),
            NetworkError::DataChannelError("x".into()),
            NetworkError::BroadcastError("x".into()),
            NetworkError::DtlsError("x".into()),
            NetworkError::StunTurnError("x".into()),
            NetworkError::NotImplemented("x".into()),
        ];
        for e in &cases {
            assert_eq!(e.kind(), ErrorKind::Internal, "{e} should be Internal");
            assert!(!e.is_retryable());
            assert!(!e.requires_dlq());
        }
    }

    // ── From<NetworkError> for ActrError (single boundary conversion) ─────────

    #[test]
    fn transient_network_error_becomes_unavailable() {
        let e: ActrError = NetworkError::ConnectionError("lost".into()).into();
        assert!(matches!(e, ActrError::Unavailable(_)));
        assert!(e.is_retryable());
    }

    #[test]
    fn client_network_error_becomes_not_found() {
        let e: ActrError = NetworkError::NoRoute("dst".into()).into();
        assert!(matches!(e, ActrError::NotFound(_)));
        assert!(!e.is_retryable());
    }

    #[test]
    fn corrupt_network_error_becomes_decode_failure() {
        let e: ActrError = NetworkError::DeserializationError("garbled".into()).into();
        assert!(matches!(e, ActrError::DecodeFailure(_)));
        assert!(e.requires_dlq());
    }

    #[test]
    fn internal_network_error_becomes_internal() {
        let e: ActrError = NetworkError::ProtocolError("bug".into()).into();
        assert!(matches!(e, ActrError::Internal(_)));
        assert!(!e.is_retryable());
        assert!(!e.requires_dlq());
    }

    // ── category() / severity() surface every variant ───────────────────────

    #[test]
    fn category_covers_all_variants() {
        // Exhaustive: one representative per category arm, including the merged
        // serialization/deserialization bucket.
        let cases: Vec<(NetworkError, &str)> = vec![
            (NetworkError::ConnectionError("x".into()), "connection"),
            (NetworkError::SignalingError("x".into()), "signaling"),
            (NetworkError::WebRtcError("x".into()), "webrtc"),
            (NetworkError::ProtocolError("x".into()), "protocol"),
            (
                NetworkError::SerializationError("x".into()),
                "serialization",
            ),
            (
                NetworkError::DeserializationError("x".into()),
                "serialization",
            ),
            (NetworkError::TimeoutError("x".into()), "timeout"),
            (
                NetworkError::AuthenticationError("x".into()),
                "authentication",
            ),
            (NetworkError::PermissionError("x".into()), "permission"),
            (
                NetworkError::ConfigurationError("x".into()),
                "configuration",
            ),
            (
                NetworkError::ResourceExhaustedError("x".into()),
                "resource_exhausted",
            ),
            (
                NetworkError::NetworkUnreachableError("x".into()),
                "network_unreachable",
            ),
            (
                NetworkError::ServiceDiscoveryError("x".into()),
                "service_discovery",
            ),
            (NetworkError::NatTraversalError("x".into()), "nat_traversal"),
            (NetworkError::DataChannelError("x".into()), "data_channel"),
            (NetworkError::IceError("x".into()), "ice"),
            (NetworkError::DtlsError("x".into()), "dtls"),
            (NetworkError::StunTurnError("x".into()), "stun_turn"),
            (NetworkError::WebSocketError("x".into()), "websocket"),
            (
                NetworkError::ConnectionNotFound("x".into()),
                "connection_not_found",
            ),
            (
                NetworkError::ConnectionClosed("x".into()),
                "connection_closed",
            ),
            (NetworkError::NotImplemented("x".into()), "not_implemented"),
            (NetworkError::ChannelClosed("x".into()), "channel_closed"),
            (NetworkError::SendError("x".into()), "send_error"),
            (NetworkError::NoRoute("x".into()), "no_route"),
            (
                NetworkError::InvalidOperation("x".into()),
                "invalid_operation",
            ),
            (
                NetworkError::InvalidArgument("x".into()),
                "invalid_argument",
            ),
            (
                NetworkError::ChannelNotFound("x".into()),
                "channel_not_found",
            ),
            (NetworkError::BroadcastError("x".into()), "broadcast"),
            (
                NetworkError::CredentialExpired("x".into()),
                "credential_expired",
            ),
        ];
        for (err, expected) in &cases {
            assert_eq!(err.category(), *expected, "category mismatch for {err}");
            // category() must be non-empty for every variant.
            assert!(!err.category().is_empty());
        }
    }

    #[test]
    fn severity_is_within_1_to_10_for_all_variants() {
        // Exercises every severity arm and confirms the documented 1..=10 range.
        let all: Vec<NetworkError> = vec![
            NetworkError::ConnectionError("x".into()),
            NetworkError::SignalingError("x".into()),
            NetworkError::WebRtcError("x".into()),
            NetworkError::ProtocolError("x".into()),
            NetworkError::SerializationError("x".into()),
            NetworkError::DeserializationError("x".into()),
            NetworkError::TimeoutError("x".into()),
            NetworkError::AuthenticationError("x".into()),
            NetworkError::PermissionError("x".into()),
            NetworkError::CredentialExpired("x".into()),
            NetworkError::ConfigurationError("x".into()),
            NetworkError::ResourceExhaustedError("x".into()),
            NetworkError::NetworkUnreachableError("x".into()),
            NetworkError::ServiceDiscoveryError("x".into()),
            NetworkError::NatTraversalError("x".into()),
            NetworkError::DataChannelError("x".into()),
            NetworkError::IceError("x".into()),
            NetworkError::DtlsError("x".into()),
            NetworkError::StunTurnError("x".into()),
            NetworkError::WebSocketError("x".into()),
            NetworkError::ConnectionNotFound("x".into()),
            NetworkError::ConnectionClosed("x".into()),
            NetworkError::NotImplemented("x".into()),
            NetworkError::ChannelClosed("x".into()),
            NetworkError::SendError("x".into()),
            NetworkError::NoRoute("x".into()),
            NetworkError::InvalidOperation("x".into()),
            NetworkError::InvalidArgument("x".into()),
            NetworkError::ChannelNotFound("x".into()),
            NetworkError::BroadcastError("x".into()),
        ];
        for e in &all {
            let s = e.severity();
            assert!((1..=10).contains(&s), "severity {s} out of range for {e}");
        }
        // Spot-check a few known tiers.
        assert_eq!(NetworkError::ConfigurationError("x".into()).severity(), 10);
        assert_eq!(NetworkError::Other(anyhow::anyhow!("x")).severity(), 1);
    }

    // ── From conversions into NetworkError ─────────────────────────────────

    #[test]
    fn from_io_error_into_network_error() {
        let e: NetworkError = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "boom").into();
        assert!(matches!(e, NetworkError::IoError(_)));
        assert_eq!(e.category(), "io");
        assert_eq!(e.kind(), ErrorKind::Internal);
    }

    #[test]
    fn from_url_parse_error_into_network_error() {
        let bad = "not a url".parse::<url::Url>().unwrap_err();
        let e: NetworkError = bad.into();
        assert!(matches!(e, NetworkError::UrlParseError(_)));
        assert_eq!(e.category(), "url_parse");
    }

    #[test]
    fn from_json_error_into_network_error() {
        let bad: serde_json::Error =
            serde_json::from_str::<serde_json::Value>("{bad}").unwrap_err();
        let e: NetworkError = bad.into();
        assert!(matches!(e, NetworkError::JsonError(_)));
        assert_eq!(e.category(), "json");
    }

    #[test]
    fn from_anyhow_into_network_error() {
        let e: NetworkError = anyhow::anyhow!("kaboom").into();
        assert!(matches!(e, NetworkError::Other(_)));
        assert_eq!(e.severity(), 1);
        assert_eq!(e.kind(), ErrorKind::Internal);
    }

    #[test]
    fn from_actr_id_error_into_invalid_argument() {
        // An unparseable actr-id string yields ActrIdError, which maps to InvalidArgument.
        let id_err = actr_protocol::ActrId::from_string_repr("").unwrap_err();
        let e: NetworkError = id_err.into();
        assert!(matches!(e, NetworkError::InvalidArgument(_)));
        assert_eq!(e.kind(), ErrorKind::Client);
    }

    #[test]
    fn network_error_display_and_to_actr_error_for_other() {
        // The `Other(anyhow)` arm must round-trip through Display and become ActrError::Internal.
        let e = NetworkError::Other(anyhow::anyhow!("boom"));
        let s = e.to_string();
        assert!(s.contains("boom"));
        let ae: ActrError = e.into();
        assert!(matches!(ae, ActrError::Internal(_)));
    }

    // ── From<NetworkError> for ActrError: kind() fallback arms ──────────────

    #[test]
    fn client_kind_error_without_precise_mapping_becomes_not_found() {
        // InvalidOperation is Client-kind but not in the precise NotFound map
        // (NoRoute/ConnectionNotFound/ChannelNotFound/ServiceDiscovery), so it
        // falls through to `ErrorKind::Client => ActrError::NotFound`.
        let e: ActrError = NetworkError::InvalidOperation("bad op".into()).into();
        assert!(matches!(e, ActrError::NotFound(_)));
    }

    #[test]
    fn transient_kind_error_without_precise_mapping_becomes_unavailable() {
        // ResourceExhaustedError is Transient-kind but not in the precise map.
        let e: ActrError = NetworkError::ResourceExhaustedError("overload".into()).into();
        assert!(matches!(e, ActrError::Unavailable(_)));
    }

    #[test]
    fn io_error_becomes_internal_via_kind_fallback() {
        // IoError is Internal-kind, not in any precise map → Internal.
        let e: ActrError =
            NetworkError::IoError(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "io"))
                .into();
        assert!(matches!(e, ActrError::Internal(_)));
    }
}
