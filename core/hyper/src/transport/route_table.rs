//! PayloadType routing extension
//!
//! Provides static routing configuration and retry policy for PayloadType.

use actr_protocol::PayloadType;
use std::time::Duration;

/// Retry policy for a send operation.
///
/// Applies only to transient failures. Non-transient errors are returned immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetryPolicy {
    /// Maximum number of attempts (1 = no retry, 2 = one retry, etc.)
    pub(crate) max_attempts: u32,
    /// Initial backoff delay between attempts.
    pub(crate) initial_delay: Duration,
    /// Maximum backoff delay cap.
    pub(crate) max_delay: Duration,
}

/// DataChannel QoS configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DataChannelQoS {
    /// Signaling: ordered, reliable
    Signal,

    /// Reliable: reliable transmission
    Reliable,

    /// Latency-first: allow packet loss
    LatencyFirst,
}

/// DataLane type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DataLaneType {
    /// WebRTC DataChannel (with QoS)
    WebRtcDataChannel(DataChannelQoS),

    /// WebSocket
    WebSocket,
}

/// PayloadType routing extension
pub(crate) trait PayloadTypeExt {
    /// Whether this payload type carries a `DataChunk` chunk.
    ///
    /// Only `StreamReliable` and `StreamLatencyFirst` are permitted on the
    /// `send_data_chunk` path; centralizing the classification here keeps the
    /// stream-type set in one place as it is referenced from several call sites.
    #[allow(clippy::wrong_self_convention)]
    fn is_stream(self) -> bool;

    /// Whether this payload type carries an `RpcEnvelope` (call/tell path).
    ///
    /// Only `RpcReliable` and `RpcSignal` are permitted on the call/tell
    /// entry points; stream and media types must use their dedicated APIs.
    #[allow(clippy::wrong_self_convention)]
    fn is_rpc(self) -> bool;

    /// Get the list of supported DataLane types (ordered by priority)
    fn data_lane_types(self) -> &'static [DataLaneType];

    /// Retry policy for transient send failures.
    ///
    /// - `RpcSignal`: 1 retry (2 total attempts), 500 ms / 500 ms cap
    ///   (signals are time-sensitive; one fast retry, then give up)
    /// - `RpcReliable`: 4 retries (5 total attempts), 1 s initial / 5 s cap
    ///   (important messages; exponential backoff up to 5 s)
    /// - Stream / Media: no retry (caller owns flow control)
    fn retry_policy(self) -> RetryPolicy;
}

impl PayloadTypeExt for PayloadType {
    #[inline]
    fn is_stream(self) -> bool {
        matches!(
            self,
            PayloadType::StreamReliable | PayloadType::StreamLatencyFirst
        )
    }

    #[inline]
    fn is_rpc(self) -> bool {
        matches!(self, PayloadType::RpcReliable | PayloadType::RpcSignal)
    }

    #[inline]
    fn retry_policy(self) -> RetryPolicy {
        match self {
            // Signals are time-sensitive: one fast retry only
            PayloadType::RpcSignal => RetryPolicy {
                max_attempts: 2,
                initial_delay: Duration::from_millis(500),
                max_delay: Duration::from_millis(500),
            },
            // Reliable RPC: up to 4 retries with exponential backoff
            PayloadType::RpcReliable => RetryPolicy {
                max_attempts: 5,
                initial_delay: Duration::from_secs(1),
                max_delay: Duration::from_secs(5),
            },
            // Stream and media: caller owns flow control; no framework retry
            PayloadType::StreamReliable
            | PayloadType::StreamLatencyFirst
            | PayloadType::MediaRtp => RetryPolicy {
                max_attempts: 1,
                initial_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
            },
        }
    }

    #[inline]
    fn data_lane_types(self) -> &'static [DataLaneType] {
        match self {
            // RPC_RELIABLE - RpcEnvelope with reliable ordered transmission
            PayloadType::RpcReliable => &[
                DataLaneType::WebRtcDataChannel(DataChannelQoS::Reliable),
                DataLaneType::WebSocket,
            ],

            // RPC_SIGNAL - RpcEnvelope with high-priority signaling channel
            PayloadType::RpcSignal => &[
                DataLaneType::WebRtcDataChannel(DataChannelQoS::Signal),
                DataLaneType::WebSocket,
            ],

            // STREAM_RELIABLE - DataChunk with reliable ordered transmission
            PayloadType::StreamReliable => &[
                DataLaneType::WebRtcDataChannel(DataChannelQoS::Reliable),
                DataLaneType::WebSocket,
            ],

            // STREAM_LATENCY_FIRST - DataChunk with low latency partial-reliable transmission
            PayloadType::StreamLatencyFirst => &[
                DataLaneType::WebRtcDataChannel(DataChannelQoS::LatencyFirst),
                DataLaneType::WebSocket,
            ],

            // MEDIA_RTP - Not routed through DataLane, uses MediaFrameRegistry
            PayloadType::MediaRtp => &[],
        }
    }
}

impl DataLaneType {
    /// Determine if WebRTC connection is needed for this DataLane Type
    #[inline]
    pub(crate) fn needs_webrtc(self) -> bool {
        matches!(self, DataLaneType::WebRtcDataChannel(_))
    }
}

#[cfg(test)]
#[path = "route_table_tests.rs"]
mod tests;
