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

    /// Data channel closed error
    #[error("Data channel closed: {0}")]
    DataChannelClosed(String),

    /// Data channel exists but is not currently open/sendable
    #[error("Data channel not open: {0}")]
    DataChannelNotOpen(String),

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

    /// WebSocket closed error
    #[error("WebSocket closed: {0}")]
    WebSocketClosed(String),

    /// Connection not found error
    #[error("Connection not found: {0}")]
    ConnectionNotFound(String),

    /// Connection closed error (e.g., cancelled during creation)
    #[error("Connection closed: {0}")]
    ConnectionClosed(String),

    /// WebRTC peer connection closed error
    #[error("Peer connection closed: {0}")]
    PeerConnectionClosed(String),

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
            | NetworkError::PeerConnectionClosed(_)
            | NetworkError::ChannelClosed(_)
            | NetworkError::DataChannelClosed(_)
            | NetworkError::DataChannelNotOpen(_)
            | NetworkError::SendError(_)
            | NetworkError::NetworkUnreachableError(_)
            | NetworkError::ResourceExhaustedError(_)
            | NetworkError::WebSocketError(_)
            | NetworkError::WebSocketClosed(_)
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
            NetworkError::DataChannelClosed(_) => "data_channel_closed",
            NetworkError::DataChannelNotOpen(_) => "data_channel_not_open",
            NetworkError::IceError(_) => "ice",
            NetworkError::DtlsError(_) => "dtls",
            NetworkError::StunTurnError(_) => "stun_turn",
            NetworkError::WebSocketError(_) => "websocket",
            NetworkError::WebSocketClosed(_) => "websocket_closed",
            NetworkError::ConnectionNotFound(_) => "connection_not_found",
            NetworkError::ConnectionClosed(_) => "connection_closed",
            NetworkError::PeerConnectionClosed(_) => "peer_connection_closed",
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
            | NetworkError::PeerConnectionClosed(_)
            | NetworkError::ChannelClosed(_)
            | NetworkError::DataChannelClosed(_)
            | NetworkError::DataChannelNotOpen(_)
            | NetworkError::WebSocketClosed(_)
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

    /// Return true when the error means the underlying transport is no
    /// longer sendable and should be treated as a stale/closed candidate.
    ///
    /// `ChannelClosed` is intentionally excluded: it is a generic in-process
    /// channel failure and existing non-RPC send paths surface it directly
    /// instead of treating it as a stale WebRTC lane to self-heal.
    ///
    /// Exhaustive by design (mirrors `kind`/`category`/`severity`): adding a
    /// new `NetworkError` variant forces the author to decide its closed-like
    /// status here, so a future closed-transport variant cannot silently miss
    /// stale-candidate self-heal.
    pub fn is_closed_like(&self) -> bool {
        match self {
            // Transport is gone / not sendable: self-heal by evicting the stale candidate.
            NetworkError::ConnectionClosed(_)
            | NetworkError::PeerConnectionClosed(_)
            | NetworkError::DataChannelClosed(_)
            | NetworkError::DataChannelNotOpen(_)
            | NetworkError::WebSocketClosed(_) => true,

            // Not a stale-transport signal.
            NetworkError::ConnectionError(_)
            | NetworkError::SignalingError(_)
            | NetworkError::WebRtcError(_)
            | NetworkError::ProtocolError(_)
            | NetworkError::SerializationError(_)
            | NetworkError::DeserializationError(_)
            | NetworkError::TimeoutError(_)
            | NetworkError::AuthenticationError(_)
            | NetworkError::CredentialExpired(_)
            | NetworkError::PermissionError(_)
            | NetworkError::ConfigurationError(_)
            | NetworkError::ResourceExhaustedError(_)
            | NetworkError::NetworkUnreachableError(_)
            | NetworkError::ServiceDiscoveryError(_)
            | NetworkError::NatTraversalError(_)
            | NetworkError::DataChannelError(_)
            | NetworkError::BroadcastError(_)
            | NetworkError::IceError(_)
            | NetworkError::DtlsError(_)
            | NetworkError::StunTurnError(_)
            | NetworkError::WebSocketError(_)
            | NetworkError::ConnectionNotFound(_)
            | NetworkError::NotImplemented(_)
            | NetworkError::ChannelClosed(_)
            | NetworkError::SendError(_)
            | NetworkError::NoRoute(_)
            | NetworkError::InvalidOperation(_)
            | NetworkError::InvalidArgument(_)
            | NetworkError::ChannelNotFound(_)
            | NetworkError::IoError(_)
            | NetworkError::UrlParseError(_)
            | NetworkError::JsonError(_)
            | NetworkError::Other(_) => false,
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
///
/// Closed / not-open variants are mapped structurally so any future `?` on
/// a webrtc call in a send path cannot regress to an unstructured
/// `WebRtcError` that `is_closed_like()` would miss. The closed set is kept
/// minimal (connection-level errors only); channel-level closed errors are
/// classified with state context by `classify_data_channel_send_error`.
impl From<webrtc::Error> for NetworkError {
    fn from(err: webrtc::Error) -> Self {
        match &err {
            webrtc::Error::ErrConnectionClosed | webrtc::Error::ErrClosedPipe => {
                NetworkError::PeerConnectionClosed(err.to_string())
            }
            webrtc::Error::ErrDataChannelNotOpen | webrtc::Error::ErrSCTPNotEstablished => {
                NetworkError::DataChannelNotOpen(err.to_string())
            }
            _ => NetworkError::WebRtcError(err.to_string()),
        }
    }
}

/// Whether a tungstenite WebSocket error indicates the connection is closed.
///
/// Shared by `From<WsError>` and the WebSocket lane send path so the
/// closed-variant set (`ConnectionClosed` / `AlreadyClosed`) is declared once.
pub(crate) fn is_tungstenite_closed(err: &tokio_tungstenite::tungstenite::Error) -> bool {
    matches!(
        err,
        tokio_tungstenite::tungstenite::Error::ConnectionClosed
            | tokio_tungstenite::tungstenite::Error::AlreadyClosed
    )
}

/// Convert from WebSocket error
impl From<tokio_tungstenite::tungstenite::Error> for NetworkError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        if is_tungstenite_closed(&err) {
            NetworkError::WebSocketClosed(err.to_string())
        } else {
            NetworkError::WebSocketError(err.to_string())
        }
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
#[path = "error_tests.rs"]
mod tests;
