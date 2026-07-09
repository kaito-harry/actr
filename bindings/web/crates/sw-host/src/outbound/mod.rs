//! Outbound layer for sending messages.
//!
//! Mirrors actr's outbound layer and provides a unified sending interface.
//!
//! # Outbound path
//!
//! ```text
//! Actor ctx.call()/tell()
//!   -> Gate::Peer
//!     -> PeerGate (ActrId->Dest mapping + pending_requests)
//!       -> PeerTransport (Dest->DestTransport mapping)
//!         -> DestTransport (event-driven send loop)
//!           -> WirePool (priority selection: WebRTC > WebSocket)
//!             -> WireHandle::WebRTC.get_lane()
//!               -> DataLane::PostMessage { port: dedicated MessagePort }
//!                 -> port.postMessage(data)  [zero-copy, no command protocol]
//!                   -> DOM bridge -> RtcDataChannel.send() -> Remote
//! ```

mod host_gate;
mod peer_gate;

pub use host_gate::HostGate;
pub use peer_gate::PeerGate;

use actr_protocol::{ActorResult, ActrError, ActrId, PayloadType, RpcEnvelope};
use bytes::Bytes;
use std::sync::Arc;

/// Validate a caller-supplied RPC timeout for a REQUEST envelope.
///
/// Wire contract (see `package.proto`): `timeout_ms` MUST be > 0 for
/// `DIRECTION_REQUEST`. Rejecting `<= 0` here prevents web callers from
/// producing the invalid zero/negative REQUEST envelopes that #254 guards
/// against on the native side. Mirrors
/// `actr_hyper::transport::validate_rpc_timeout_ms`.
pub(crate) fn validate_rpc_timeout_ms(timeout_ms: i64) -> ActorResult<()> {
    if timeout_ms <= 0 {
        return Err(ActrError::InvalidArgument(format!(
            "RPC call timeout_ms must be > 0, got {timeout_ms} (fire-and-forget messaging must use tell, not a zero timeout)"
        )));
    }
    Ok(())
}

/// Gate enum for outbound messaging.
///
/// # Variants
///
/// - **Host**: communication between actors inside the SW with zero serialization
/// - **Peer**: cross-node transport through a dedicated MessagePort and the full transport stack
#[derive(Clone)]
pub enum Gate {
    /// Host gate for in-SW communication with zero serialization.
    Host(Arc<HostGate>),

    /// Peer gate for cross-node transport.
    ///
    /// PeerGate -> PeerTransport -> DestTransport
    ///   -> WirePool -> WireHandle -> DataLane::PostMessage (direct send through a dedicated MessagePort)
    Peer(Arc<PeerGate>),
}

impl Gate {
    /// Create a Host gate.
    pub fn host(gate: Arc<HostGate>) -> Self {
        Self::Host(gate)
    }

    /// Create a Peer gate.
    pub fn peer(gate: Arc<PeerGate>) -> Self {
        Self::Peer(gate)
    }

    /// Send a request and wait for the response.
    pub async fn send_request(&self, target: &ActrId, envelope: RpcEnvelope) -> ActorResult<Bytes> {
        match self {
            Gate::Host(gate) => gate.send_request(target, envelope).await,
            Gate::Peer(gate) => gate.send_request(target, envelope).await,
        }
    }

    /// Send a one-way tell without waiting for a response.
    pub async fn send_message(&self, target: &ActrId, envelope: RpcEnvelope) -> ActorResult<()> {
        match self {
            Gate::Host(gate) => gate.send_message(target, envelope).await,
            Gate::Peer(gate) => gate.send_message(target, envelope).await,
        }
    }

    /// Relay an envelope that already carries an explicit routing direction.
    ///
    /// Unlike `send_message` (which stamps `Direction::Tell`), this preserves
    /// the sender's Request/Response/Tell label so a relayed request still
    /// expects a reply on the remote peer.
    ///
    /// Only `Gate::Peer` supports relay: `HostGate`'s only send path is
    /// `send_message`, which would wrongly downgrade a Request to Tell, so
    /// Host returns `InvalidArgument`. The sole caller
    /// (`System::init_message_handler`) always runs against a Peer outgate, so
    /// the Host arm is a defensive guard against misuse, not a live path.
    pub async fn relay_envelope(&self, target: &ActrId, envelope: RpcEnvelope) -> ActorResult<()> {
        match self {
            Gate::Host(_) => Err(ActrError::InvalidArgument(
                "relay_envelope requires a Peer gate; HostGate send_message is tell-only"
                    .to_string(),
            )),
            Gate::Peer(gate) => gate.relay_envelope(target, envelope).await,
        }
    }

    /// Send a DataChunk through the Fast Path.
    pub async fn send_data_chunk(
        &self,
        target: &ActrId,
        payload_type: PayloadType,
        data: Bytes,
    ) -> ActorResult<()> {
        match self {
            Gate::Host(gate) => gate.send_data_chunk(target, payload_type, data).await,
            Gate::Peer(gate) => gate.send_data_chunk(target, payload_type, data).await,
        }
    }

    /// Try to handle a remote response.
    ///
    /// Checks whether this gate has a pending request for `request_id`.
    /// If so, resolves it and returns true; otherwise returns false.
    pub fn try_handle_response(&self, request_id: &str, response: Bytes) -> bool {
        match self {
            Gate::Host(_) => false,
            Gate::Peer(gate) => gate.handle_response(request_id.to_string(), response),
        }
    }
}
