//! HostGate - Host transport adapter (outbound)
//!
//! # Responsibilities
//! - Wrap HostTransport (zero serialization, direct RpcEnvelope passing)
//! - Used for intra-process communication (e.g., Shell <-> Workload)
//! - Support PayloadType routing (default Reliable)

use super::ensure_stream_payload_type;
use crate::transport::HostTransport;
use actr_framework::Bytes;
use actr_protocol::{ActorResult, ActrError, ActrId, Direction, PayloadType, RpcEnvelope};
use std::sync::Arc;

/// HostGate - Inproc transport adapter (outbound)
///
/// # Features
/// - Zero serialization: directly pass `RpcEnvelope` objects
/// - Zero copy: use mpsc channel for in-process passing
/// - PayloadType routing: defaults to Reliable, can specify other types via extension methods
/// - High performance: latency < 10us
pub struct HostGate {
    transport: Arc<HostTransport>,
}

impl HostGate {
    /// Create new HostGate
    ///
    /// # Arguments
    /// - `transport`: HostTransport instance
    pub fn new(transport: Arc<HostTransport>) -> Self {
        Self { transport }
    }

    /// Send request and wait for response (with specified PayloadType and identifier)
    ///
    /// # Extension Method
    /// Used for scenarios requiring non-default PayloadType
    ///
    /// # Arguments
    /// - `_target`: Target ActorId (only for logging, not needed for intra-process communication)
    /// - `payload_type`: PayloadType (Reliable, Signal, LatencyFirst, MediaTrack)
    /// - `identifier`: Optional identifier (LatencyFirst needs channel_id, MediaTrack needs track_id)
    /// - `envelope`: Message envelope
    pub async fn send_request_with_type(
        &self,
        _target: &ActrId,
        payload_type: PayloadType,
        identifier: Option<String>,
        mut envelope: RpcEnvelope,
    ) -> ActorResult<Bytes> {
        tracing::debug!(
            "HostGate::send_request_with_type to {:?} (type={:?}, id={:?})",
            _target,
            payload_type,
            identifier
        );

        envelope.direction = Some(Direction::Request as i32);
        self.transport
            .send_request(payload_type, identifier, envelope)
            .await
    }

    /// Send one-way message (with specified PayloadType and identifier)
    ///
    /// # Arguments
    /// - `_target`: Target ActorId (only for logging, not needed for intra-process communication)
    /// - `payload_type`: PayloadType
    /// - `identifier`: Optional identifier
    /// - `envelope`: Message envelope
    pub async fn send_message_with_type(
        &self,
        _target: &ActrId,
        payload_type: PayloadType,
        identifier: Option<String>,
        mut envelope: RpcEnvelope,
    ) -> ActorResult<()> {
        tracing::debug!(
            "HostGate::send_message_with_type to {:?} (type={:?}, id={:?})",
            _target,
            payload_type,
            identifier
        );

        envelope.direction = Some(Direction::Request as i32);
        self.transport
            .send_message(payload_type, identifier, envelope)
            .await
            .map_err(|e| ActrError::Unavailable(e.to_string()))
    }

    /// Send request and wait for response (defaults to Reliable)
    ///
    /// # Arguments
    /// - `target`: Target ActorId (for logging only)
    /// - `envelope`: Message envelope
    ///
    /// # Default behavior
    /// Uses PayloadType::RpcReliable with no identifier
    #[cfg(feature = "test-utils")]
    pub async fn send_request(
        &self,
        target: &ActrId,
        mut envelope: RpcEnvelope,
    ) -> ActorResult<Bytes> {
        tracing::info!(
            "HostGate::send_request to {:?}, request_id={}",
            target,
            envelope.request_id
        );

        envelope.direction = Some(Direction::Request as i32);
        // Default to Reliable (no identifier)
        let result = self
            .transport
            .send_request(PayloadType::RpcReliable, None, envelope)
            .await;

        match &result {
            Ok(_) => tracing::info!("HostGate::send_request completed successfully"),
            Err(e) => tracing::error!("HostGate::send_request failed: {:?}", e),
        }

        result
    }

    /// Send one-way message (defaults to Reliable)
    ///
    /// # Arguments
    /// - `target`: Target ActorId (for logging only)
    /// - `envelope`: Message envelope
    ///
    /// # Default behavior
    /// Uses PayloadType::RpcReliable with no identifier
    #[cfg(feature = "test-utils")]
    pub async fn send_message(
        &self,
        target: &ActrId,
        mut envelope: RpcEnvelope,
    ) -> ActorResult<()> {
        tracing::debug!("HostGate::send_message to {}", target);

        envelope.direction = Some(Direction::Request as i32);
        // Default to Reliable (no identifier)
        self.transport
            .send_message(PayloadType::RpcReliable, None, envelope)
            .await
            .map_err(|e| ActrError::Unavailable(e.to_string()))
    }

    /// Send DataStream (Fast Path)
    ///
    /// # Arguments
    /// - `_target`: Target ActorId (for logging only, not needed for intra-process)
    /// - `payload_type`: PayloadType (StreamReliable or StreamLatencyFirst)
    /// - `stream_id`: DataStream identifier already known before serialization
    /// - `data`: Serialized DataStream bytes
    ///
    /// # Note
    /// For inproc, DataStream is sent via LatencyFirst channel with stream_id as identifier
    pub async fn send_data_stream(
        &self,
        _target: &ActrId,
        payload_type: PayloadType,
        stream_id: &str,
        data: Bytes,
    ) -> ActorResult<()> {
        tracing::debug!(
            "HostGate::send_data_stream stream_id={}, size={} bytes",
            stream_id,
            data.len()
        );

        ensure_stream_payload_type(payload_type)?;

        // Wrap in RpcEnvelope for transport
        #[cfg_attr(not(feature = "opentelemetry"), allow(unused_mut))]
        let mut envelope = RpcEnvelope {
            route_key: "fast_path.data_stream".to_string(),
            payload: Some(data),
            error: None,
            direction: Some(Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: uuid::Uuid::new_v4().to_string(),
            metadata: vec![],
            timeout_ms: 0,
        };
        // Inject tracing context
        #[cfg(feature = "opentelemetry")]
        {
            use crate::wire::webrtc::trace::inject_span_context_to_rpc;
            inject_span_context_to_rpc(&tracing::Span::current(), &mut envelope);
        }

        self.transport
            .send_message(payload_type, Some(stream_id.to_string()), envelope)
            .await
            .map_err(|e| ActrError::Unavailable(e.to_string()))
    }
}

#[cfg(test)]
#[path = "host_gate_tests.rs"]
mod tests;
