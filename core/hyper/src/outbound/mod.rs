//! Outbound Layer 2: Outbound gate abstraction layer
//!
//! Provides a unified outbound message sending interface for both in-process and cross-process communication.
//!
//! # Design Features
//!
//! - **Enum dispatch**: use enums instead of trait objects to avoid virtual calls
//! - **Zero-cost abstraction**: static dispatch with compile-time type selection
//! - **Unified API**: Host and Peer share the same method signatures

mod data_stream_activity;
mod host_gate;
mod peer_gate;

pub use host_gate::HostGate;
pub use peer_gate::PeerGate;
pub(crate) use peer_gate::PendingRequestsMap;

use actr_framework::{Bytes, MediaSample};
use actr_protocol::{ActorResult, ActrError, ActrId, PayloadType, RpcEnvelope};
use std::sync::Arc;

use crate::transport::PayloadTypeExt;

fn ensure_stream_payload_type(payload_type: PayloadType) -> ActorResult<()> {
    if !payload_type.is_stream() {
        return Err(ActrError::InvalidArgument(format!(
            "send_data_stream requires a stream payload type, got {payload_type:?}"
        )));
    }

    Ok(())
}

/// Gate enum for outbound messaging.
///
/// # Design Principles
///
/// - Use **enum dispatch** instead of trait objects to avoid virtual calls
/// - Preserve **zero-cost abstraction** with compile-time type selection
/// - Stay **fully outbound-only** with no inbound routing logic
///
/// # Performance
///
/// ```text
/// Internal structure of `Gate::send_request()`:
///   match self {
///       Gate::Host(gate) => gate.send_request(...),   // <- static dispatch
///       Gate::Peer(gate) => gate.send_request(...),   // <- static dispatch
///   }
///
/// Performance characteristics:
///   - no vtable lookup
///   - fully inlineable by the compiler
///   - CPU branch prediction hit rate above 95%
/// ```
#[derive(Clone)]
pub(crate) enum Gate {
    /// Host: in-process outbound transport with zero serialization.
    Host(Arc<HostGate>),

    /// Peer: cross-process outbound transport using Protobuf serialization.
    Peer(Arc<PeerGate>),
}

impl Gate {
    /// Send a request and wait for the response with an explicit `PayloadType`.
    pub(crate) async fn send_request_with_type(
        &self,
        target_id: &ActrId,
        payload_type: PayloadType,
        envelope: RpcEnvelope,
    ) -> ActorResult<Bytes> {
        match self {
            Gate::Host(gate) => {
                gate.send_request_with_type(target_id, payload_type, None, envelope)
                    .await
            }
            Gate::Peer(gate) => {
                gate.send_request_with_type(target_id, payload_type, envelope)
                    .await
            }
        }
    }

    /// Send a one-way message with an explicit `PayloadType`.
    pub(crate) async fn send_message_with_type(
        &self,
        target: &ActrId,
        payload_type: PayloadType,
        envelope: RpcEnvelope,
    ) -> ActorResult<()> {
        match self {
            Gate::Host(gate) => {
                gate.send_message_with_type(target, payload_type, None, envelope)
                    .await
            }
            Gate::Peer(gate) => {
                gate.send_tell_with_type(target, payload_type, envelope)
                    .await
            }
        }
    }

    /// Send a media sample over native WebRTC media transport.
    ///
    /// # Parameters
    ///
    /// - `target`: target actor ID
    /// - `track_id`: media track identifier
    /// - `sample`: media sample data
    ///
    /// # Semantics
    ///
    /// - Supported only for `Peer` (WebRTC)
    /// - `Host` returns `NotImplemented`
    /// - Uses `RTCRtpSender` without protobuf overhead
    pub(crate) async fn send_media_sample(
        &self,
        target: &ActrId,
        track_id: &str,
        sample: MediaSample,
    ) -> ActorResult<()> {
        match self {
            Gate::Host(_gate) => {
                // Host does not support MediaTrack (WebRTC-specific feature)
                Err(ActrError::NotImplemented(
                    "MediaTrack is only supported for remote actors via WebRTC".to_string(),
                ))
            }
            Gate::Peer(gate) => gate.send_media_sample(target, track_id, sample).await,
        }
    }

    /// Add a media track to the WebRTC connection
    pub(crate) async fn add_media_track(
        &self,
        target: &ActrId,
        track_id: &str,
        codec: &str,
        media_type: &str,
    ) -> ActorResult<()> {
        match self {
            Gate::Host(_gate) => Err(ActrError::NotImplemented(
                "MediaTrack is only supported for remote actors via WebRTC".to_string(),
            )),
            Gate::Peer(gate) => {
                gate.add_media_track(target, track_id, codec, media_type)
                    .await
            }
        }
    }

    /// Remove a media track from the WebRTC connection.
    pub(crate) async fn remove_media_track(
        &self,
        target: &ActrId,
        track_id: &str,
    ) -> ActorResult<()> {
        match self {
            Gate::Host(_gate) => Err(ActrError::NotImplemented(
                "MediaTrack is only supported for remote actors via WebRTC".to_string(),
            )),
            Gate::Peer(gate) => gate.remove_media_track(target, track_id).await,
        }
    }

    /// Send a `DataStream` over the Fast Path.
    ///
    /// # Parameters
    ///
    /// - `target`: target actor ID
    /// - `payload_type`: `PayloadType` such as `StreamReliable` or `StreamLatencyFirst`
    /// - `stream_id`: `DataStream` identifier already known before serialization
    /// - `data`: serialized `DataStream` bytes
    ///
    /// # Semantics
    ///
    /// - Host: sends through an `mpsc` channel
    /// - Peer: sends through WebRTC DataChannel or WebSocket
    pub(crate) async fn send_data_stream(
        &self,
        target: &ActrId,
        payload_type: actr_protocol::PayloadType,
        stream_id: &str,
        data: Bytes,
    ) -> ActorResult<()> {
        match self {
            Gate::Host(gate) => {
                gate.send_data_stream(target, payload_type, stream_id, data)
                    .await
            }
            Gate::Peer(gate) => {
                gate.send_data_stream(target, payload_type, stream_id, data)
                    .await
            }
        }
    }
}
