//! PeerGate - outbound cross-node transport adapter.
//!
//! Wraps PeerTransport and exposes the standard actor sending interface.

use crate::transport::PeerTransport;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActorResult, ActrError, ActrId, Direction, PayloadType, RpcEnvelope};
use actr_web_common::Dest;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// PeerGate - cross-node transport adapter.
///
/// # Responsibilities
/// - Wrap PeerTransport
/// - Maintain `ActrId -> Dest` mappings
/// - Implement request/response flow via oneshot channels
pub struct PeerGate {
    /// Transport manager
    transport: Arc<PeerTransport>,

    /// `ActrId -> Dest` mapping used to resolve network targets.
    actor_dest_map: Arc<Mutex<HashMap<ActrId, Dest>>>,

    /// Pending requests: request_id → oneshot sender
    pending_requests: Arc<Mutex<HashMap<String, futures::channel::oneshot::Sender<Bytes>>>>,
}

impl PeerGate {
    /// Create a new PeerGate.
    pub fn new(transport: Arc<PeerTransport>) -> Self {
        Self {
            transport,
            actor_dest_map: Arc::new(Mutex::new(HashMap::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register an `ActrId -> Dest` mapping.
    ///
    /// # Purpose
    /// Called when the actor's network location is known.
    pub fn register_actor(&self, actor_id: ActrId, dest: Dest) {
        let mut map = self.actor_dest_map.lock();
        log::debug!("Registering actor mapping: {:?} → {:?}", &actor_id, &dest);
        map.insert(actor_id, dest);
    }

    /// Resolve the Dest for an ActrId.
    fn get_dest(&self, actor_id: &ActrId) -> ActorResult<Dest> {
        let map = self.actor_dest_map.lock();
        map.get(actor_id)
            .cloned()
            .ok_or_else(|| ActrError::NotFound(format!("Actor not found: {:?}", actor_id)))
    }

    fn stamp_envelope_direction(mut envelope: RpcEnvelope, direction: Direction) -> RpcEnvelope {
        envelope.direction = Some(direction as i32);
        envelope
    }

    /// Send a request and wait for the response.
    pub async fn send_request(&self, target: &ActrId, envelope: RpcEnvelope) -> ActorResult<Bytes> {
        let envelope = Self::stamp_envelope_direction(envelope, Direction::Request);

        log::debug!(
            "PeerGate::send_request to {:?}, request_id={}",
            target,
            envelope.request_id
        );

        // 1. Resolve the target destination.
        let dest = self.get_dest(target)?;

        // 2. Create a oneshot channel.
        let (tx, rx) = futures::channel::oneshot::channel();

        // 3. Register the pending request.
        {
            let mut pending = self.pending_requests.lock();
            pending.insert(envelope.request_id.clone(), tx);
        }

        // 4. Serialize the envelope and send it.
        let payload = envelope.encode_to_vec();
        self.transport
            .send(&dest, PayloadType::RpcReliable, &payload)
            .await
            .map_err(|e| ActrError::Unavailable(format!("Send failed: {}", e)))?;

        // 5. Wait for the response.
        let response = rx
            .await
            .map_err(|_| ActrError::Unavailable("Response channel closed".to_string()))?;

        Ok(response)
    }

    /// Send a one-way message without waiting for a response.
    pub async fn send_message(&self, target: &ActrId, envelope: RpcEnvelope) -> ActorResult<()> {
        let envelope = Self::stamp_envelope_direction(envelope, Direction::Request);

        log::debug!(
            "PeerGate::send_message to {:?}, request_id={}",
            target,
            envelope.request_id
        );

        // 1. Resolve the target destination.
        let dest = self.get_dest(target)?;

        // 2. Serialize the envelope and send it with RpcSignal as a one-way payload type.
        let payload = envelope.encode_to_vec();
        self.transport
            .send(&dest, PayloadType::RpcSignal, &payload)
            .await
            .map_err(|e| ActrError::Unavailable(format!("Send failed: {}", e)))?;

        Ok(())
    }

    /// Send a DataStream through the Fast Path.
    pub async fn send_data_stream(
        &self,
        target: &ActrId,
        payload_type: PayloadType,
        data: Bytes,
    ) -> ActorResult<()> {
        log::debug!(
            "PeerGate::send_data_stream to {:?}, type={:?}",
            target,
            payload_type
        );

        // 1. Resolve the target destination.
        let dest = self.get_dest(target)?;

        // 2. Send the DataStream directly.
        self.transport
            .send(&dest, payload_type, &data)
            .await
            .map_err(|e| ActrError::Unavailable(format!("Send failed: {}", e)))?;

        Ok(())
    }

    /// Handle a received response.
    ///
    /// # Purpose
    /// Called by InboundPacketDispatcher when a response is received.
    ///
    /// Returns `true` if the request_id matched and was handled.
    pub fn handle_response(&self, request_id: String, response: Bytes) -> bool {
        let mut pending = self.pending_requests.lock();
        if let Some(tx) = pending.remove(&request_id) {
            let _ = tx.send(response); // Ignore send failures if the receiver was dropped.
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
#[allow(clippy::arc_with_non_send_sync)]
mod tests {
    use super::*;
    use crate::transport::WebWireBuilder;

    fn envelope_with_direction(direction: Option<i32>) -> RpcEnvelope {
        RpcEnvelope {
            request_id: "req-direction".to_string(),
            route_key: "pkg.Service.Method".to_string(),
            direction,
            ..Default::default()
        }
    }

    #[test]
    fn test_peer_gate_creation() {
        let wire_builder = Arc::new(WebWireBuilder::new());
        let manager = Arc::new(PeerTransport::new("test-sw".to_string(), wire_builder));
        let _gate = PeerGate::new(manager);
    }

    #[test]
    fn stamp_envelope_direction_overwrites_missing_and_mismatched_values() {
        let request =
            PeerGate::stamp_envelope_direction(envelope_with_direction(None), Direction::Request);
        assert_eq!(request.direction, Some(Direction::Request as i32));

        let request = PeerGate::stamp_envelope_direction(
            envelope_with_direction(Some(Direction::Response as i32)),
            Direction::Request,
        );
        assert_eq!(request.direction, Some(Direction::Request as i32));
    }
}
