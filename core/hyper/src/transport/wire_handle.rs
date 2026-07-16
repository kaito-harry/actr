//! Wire Handle - Trait-based abstraction for Wire layer connections
//!
//! WireHandle trait provides a unified interface for different wire connection
//! types (WebRTC, WebSocket, etc.). Platform-specific implementations provide
//! the concrete connection behavior.

use super::error::NetworkResult;
use super::lane::DataLane;
use super::wire_pool::ConnType;
use actr_protocol::{ActrId, PayloadType};
use async_trait::async_trait;
use std::sync::Arc;

/// Wire identity — distinguishes connection sessions even for the same peer.
///
/// Used for stale detection: if a close event arrives with a session_id that
/// no longer matches the active wire, the event is stale and should be ignored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireIdentity {
    WebRtc { peer_id: ActrId, session_id: u64 },
}

/// WireHandle - Unified interface for Wire layer connections
///
/// # Design Philosophy
/// - Uses trait objects for cross-platform extensibility
/// - Supports connection priority comparison (WebRTC > WebSocket)
/// - Platform-specific implementations (native, web) implement this trait
#[async_trait]
pub trait WireHandle: Send + Sync + std::fmt::Debug {
    /// Get connection type
    fn connection_type(&self) -> ConnType;

    /// Connection priority (higher number = higher priority)
    #[allow(dead_code)]
    fn priority(&self) -> u8;

    /// Establish connection
    async fn connect(&self) -> NetworkResult<()>;

    /// Check if connected
    #[allow(dead_code)]
    fn is_connected(&self) -> bool;

    /// Close connection
    async fn close(&self) -> NetworkResult<()>;

    /// Close connection without waiting for graceful transport draining.
    ///
    /// Recovery paths call this after receiving a terminal close event, where
    /// waiting for an already-unusable transport can delay upper-layer cleanup.
    /// Transports without a distinct immediate-close mode retain the normal
    /// close behavior by default.
    async fn close_immediately(&self) -> NetworkResult<()> {
        self.close().await
    }

    /// Get or create DataLane (with caching)
    async fn get_lane(&self, payload_type: PayloadType) -> NetworkResult<Arc<dyn DataLane>>;

    /// Invalidate cached lane (no-op by default).
    ///
    /// Used when the underlying transport (e.g. DataChannel) has closed
    /// and needs to be recreated on next `get_lane` call.
    async fn invalidate_lane(&self, _payload_type: PayloadType) {}

    /// Connection identity for stale detection (default: None).
    ///
    /// WebRTC connections return `WireIdentity::WebRtc { peer_id, session_id }`
    /// so that upper layers can detect whether a close event belongs to the
    /// current active session or a stale one. WebSocket connections return None
    /// since they lack a comparable session concept.
    fn identity(&self) -> Option<WireIdentity> {
        None
    }
}

/// Wire connection status
#[derive(Debug)]
pub enum WireStatus {
    /// Connecting
    Connecting,

    /// Connection ready
    Ready(Arc<dyn WireHandle>),

    /// Connection failed
    Failed,
}

impl Clone for WireStatus {
    fn clone(&self) -> Self {
        match self {
            WireStatus::Connecting => WireStatus::Connecting,
            WireStatus::Ready(handle) => WireStatus::Ready(Arc::clone(handle)),
            WireStatus::Failed => WireStatus::Failed,
        }
    }
}
