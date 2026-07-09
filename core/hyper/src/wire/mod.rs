//! Wire Layer 0: Physical wire layer
//!
//! Low-level transport implementations:
//! - webrtc: WebRTC transport (DataChannel, MediaTrack, Coordinator, Signaling)
//! - websocket: WebSocket transport
//!
//! **Note**: For intra-process communication, use `crate::transport::HostTransport`

pub mod webrtc;
pub(crate) mod websocket;

use crate::key_cache::KeyFetcher;
use actr_protocol::{AIdCredential, ActrId, Direction};
use async_trait::async_trait;

/// Adapter from SignalingClient to KeyFetcher
///
/// `KeyFetcher::fetch_key` only accepts `key_id`, while `SignalingClient::get_signing_key` also
/// requires `actor_id` and `credential`. This adapter holds the context and forwards calls to the
/// underlying signaling client.
pub(crate) struct SignalingKeyFetcher {
    pub(crate) client: std::sync::Arc<dyn webrtc::SignalingClient>,
    pub(crate) actor_id: ActrId,
    pub(crate) credential: AIdCredential,
}

#[async_trait]
impl KeyFetcher for SignalingKeyFetcher {
    async fn fetch_key(&self, key_id: u32) -> crate::error::HyperResult<(u32, Vec<u8>)> {
        self.client
            .get_signing_key(self.actor_id.clone(), self.credential.clone(), key_id)
            .await
            .map_err(|e| {
                tracing::warn!(key_id, error = ?e, "SignalingKeyFetcher: failed to fetch AIS public key via signaling");
                crate::error::HyperError::AisBootstrapFailed(format!(
                    "signaling get_signing_key failed: {e:?}"
                ))
            })
    }
}

/// Why an inbound `RpcEnvelope.direction` could not be routed.
///
/// `direction_for_routing` returns this so callers can decode the peer
/// for diagnostics only on the drop path, never on the happy path.
#[derive(Debug)]
pub(crate) enum DirectionError {
    /// `direction` field absent from the envelope.
    Missing,
    /// `direction` present but `DIRECTION_UNSPECIFIED`.
    Unspecified,
    /// `direction` present but not a known variant.
    Unknown,
}

/// Classify an inbound `RpcEnvelope.direction` into a routable direction.
///
/// Pure: performs no logging and no peer decode. The caller handles
/// diagnostics on the drop path so the happy path stays free of
/// `ActrId::decode` + `to_string_repr()`.
pub(crate) fn direction_for_routing(
    raw_direction: Option<i32>,
) -> Result<Direction, DirectionError> {
    match raw_direction {
        Some(raw) => match Direction::try_from(raw) {
            Ok(direction @ (Direction::Request | Direction::Response | Direction::Tell)) => {
                Ok(direction)
            }
            Ok(Direction::Unspecified) => Err(DirectionError::Unspecified),
            Err(_) => Err(DirectionError::Unknown),
        },
        None => Err(DirectionError::Missing),
    }
}

// Re-export commonly used types. Submodule-internal types (gate / negotiator /
// connection / websocket) stay reachable via module paths rather than
// duplicated re-exports here.
pub use webrtc::{
    AuthConfig, AuthType, DisconnectReason, ReconnectConfig, SignalingClient, SignalingConfig,
    SignalingEvent, SignalingStats, WebRtcConfig,
};
#[cfg(feature = "test-utils")]
pub use webrtc::{WebRtcCoordinator, WebSocketSignalingClient};
