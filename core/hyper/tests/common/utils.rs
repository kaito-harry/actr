//! Common test utilities
//!
//! Helper functions for creating test actors, credentials, and peers

use crate::inbound::MediaFrameRegistry;
use crate::lifecycle::CredentialState;
use crate::wire::webrtc::{
    SignalingClient, WebRtcConfig, WebRtcCoordinator, WebSocketSignalingClient,
};
use crate::wire::{DirectionError, direction_for_routing};
use actr_protocol::prost::Message;
use actr_protocol::{AIdCredential, ActrError, ActrId, ActrType, Direction, Realm, RpcEnvelope};
use std::sync::Arc;

pub fn install_test_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Create a test ActrId with the given serial number
pub fn make_actor_id(serial_number: u64) -> ActrId {
    ActrId {
        realm: Realm { realm_id: 1 },
        serial_number,
        r#type: ActrType {
            manufacturer: "acme".to_string(),
            name: "node".to_string(),
            version: "1.0.0".to_string(),
        },
    }
}

/// Create a dummy credential for testing
pub fn dummy_credential() -> AIdCredential {
    AIdCredential {
        key_id: 7,
        claims: bytes::Bytes::from_static(b"dummy-claims"),
        signature: bytes::Bytes::from(vec![0u8; 64]),
    }
}

/// Create a credential state for testing
pub fn create_credential_state_for_test(credential: AIdCredential) -> CredentialState {
    CredentialState::new(credential, None, None)
}

/// Create a WebRTC peer with WebSocket signaling
///
/// Pins the actor identity on the signaling client *before* the WebSocket
/// connect so mock-actrix binds the WS to this actor and forwards relays to
/// it; otherwise outbound OFFERs are dropped as "unbound target" and the
/// peer connection times out.
///
/// Returns both the coordinator and the signaling client.
pub async fn create_peer_with_websocket(
    id: ActrId,
    server_url: &str,
) -> anyhow::Result<(Arc<WebRtcCoordinator>, Arc<dyn SignalingClient>)> {
    install_test_crypto_provider();

    let credential_state = create_credential_state_for_test(dummy_credential());

    let signaling_client = WebSocketSignalingClient::connect_to_with_identity(
        server_url,
        id.clone(),
        credential_state.clone(),
    )
    .await
    .expect("Failed to connect to test server");

    let config = WebRtcConfig::default();
    let media_registry = Arc::new(MediaFrameRegistry::new());

    let signaling_client_arc = signaling_client as Arc<dyn SignalingClient>;

    let coordinator = Arc::new(WebRtcCoordinator::new(
        id,
        credential_state,
        signaling_client_arc.clone(),
        config,
        media_registry,
    ));

    let c = coordinator.clone();
    tokio::spawn(async move {
        let _ = c.start().await;
    });

    Ok((coordinator, signaling_client_arc))
}

/// Create a WebRTC peer with WebSocket signaling and VNet
///
/// Same as `create_peer_with_websocket` but injects a virtual network
/// so that all ICE/UDP traffic flows through the VNet router.
/// This enables simulating network disconnection at the transport level.
///
/// **Note:** `set_vnet` must be called before `start()`, so this function
/// creates the coordinator as mutable, sets vnet, then wraps in `Arc` and starts.
///
/// # Arguments
/// - `id`: Actor ID for this peer
/// - `server_url`: WebSocket signaling server URL
/// - `vnet`: Virtual network instance (from `VNetPair.net_offerer` or `.net_answerer`)
pub async fn create_peer_with_vnet(
    id: ActrId,
    server_url: &str,
    vnet: Arc<webrtc_util::vnet::net::Net>,
) -> anyhow::Result<(Arc<WebRtcCoordinator>, Arc<dyn SignalingClient>)> {
    install_test_crypto_provider();

    let credential_state = create_credential_state_for_test(dummy_credential());

    let signaling_client = WebSocketSignalingClient::connect_to_with_identity(
        server_url,
        id.clone(),
        credential_state.clone(),
    )
    .await
    .expect("Failed to connect to test server");

    let config = WebRtcConfig::default();
    let media_registry = Arc::new(MediaFrameRegistry::new());

    let signaling_client_arc = signaling_client as Arc<dyn SignalingClient>;

    // Create coordinator as mutable to inject vnet before start
    let mut coordinator = WebRtcCoordinator::new(
        id,
        credential_state,
        signaling_client_arc.clone(),
        config,
        media_registry,
    );

    // Inject VNet BEFORE start
    coordinator.set_vnet(vnet);

    let coordinator = Arc::new(coordinator);
    let c = coordinator.clone();
    tokio::spawn(async move {
        let _ = c.start().await;
    });

    Ok((coordinator, signaling_client_arc))
}

async fn handle_rpc_response(
    gate: &crate::outbound::PeerGate,
    peer_name: &str,
    envelope: RpcEnvelope,
) {
    let request_id = envelope.request_id;
    tracing::debug!("📨 {} received response: {}", peer_name, request_id);

    let result = match (envelope.payload, envelope.error) {
        (_, Some(error)) => Err(ActrError::Unavailable(format!(
            "RPC error {}: {}",
            error.code, error.message
        ))),
        (Some(payload), None) => Ok(payload),
        (None, None) => Err(ActrError::DecodeFailure(
            "Invalid response: no payload or error".to_string(),
        )),
    };

    match gate.handle_response(&request_id, result).await {
        Ok(true) => {
            tracing::debug!("✅ {} handled response for {}", peer_name, request_id);
        }
        Ok(false) => {
            tracing::warn!(
                "⚠️ {} no pending request found for {}",
                peer_name,
                request_id
            );
        }
        Err(e) => {
            tracing::error!("{}: Failed to handle response: {}", peer_name, e);
        }
    }
}

async fn handle_echo_request(
    gate: &crate::outbound::PeerGate,
    peer_name: &str,
    sender_id_bytes: &[u8],
    request: RpcEnvelope,
    should_respond: bool,
) {
    let sender_id = match ActrId::decode(sender_id_bytes) {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("{}: Failed to decode sender ID: {}", peer_name, e);
            return;
        }
    };

    let request_id = request.request_id;
    tracing::debug!("📨 {} received request: {}", peer_name, request_id);

    if !should_respond {
        tracing::debug!("✅ {} handled tell for {}", peer_name, request_id);
        return;
    }

    let response = RpcEnvelope {
        request_id: request_id.clone(),
        route_key: "response".to_string(),
        payload: Some(bytes::Bytes::from("pong")),
        timeout_ms: 0,
        direction: Some(Direction::Response as i32),
        ..Default::default()
    };

    if let Err(e) = gate.send_response(&sender_id, response).await {
        tracing::error!(
            "{}: Failed to send response for {}: {}",
            peer_name,
            request_id,
            e
        );
    } else {
        tracing::debug!("✅ {} sent response for {}", peer_name, request_id);
    }
}

/// Spawn a task to receive and handle RPC responses.
///
/// This function starts a background task that:
/// 1. Receives messages from the coordinator
/// 2. Parses them as RpcEnvelope
/// 3. Routes responses to PeerGate.handle_response
///
/// Do not run this concurrently with another receive-loop helper on the same
/// coordinator. Use [`spawn_rpc_dispatcher`] when a peer handles both requests
/// and responses.
///
/// # Returns
/// A JoinHandle that can be used to abort the task
pub fn spawn_response_receiver(
    coordinator: Arc<WebRtcCoordinator>,
    gate: Arc<crate::outbound::PeerGate>,
    peer_name: &str,
) -> tokio::task::JoinHandle<()> {
    let peer_name = peer_name.to_string();
    tokio::spawn(async move {
        tracing::info!("🎯 {} response receiver task started", peer_name);
        loop {
            match coordinator.receive_message().await {
                Ok(Some((_sender_id_bytes, message_data, _payload_type))) => {
                    match RpcEnvelope::decode(message_data.as_ref()) {
                        Ok(envelope) => {
                            handle_rpc_response(gate.as_ref(), &peer_name, envelope).await;
                        }
                        Err(e) => {
                            tracing::error!("{}: Failed to decode RpcEnvelope: {}", peer_name, e);
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!("📪 {} message channel closed", peer_name);
                    break;
                }
                Err(e) => {
                    tracing::error!("{}: Error receiving message: {}", peer_name, e);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    })
}

/// Spawn an Echo server task.
///
/// This function starts a background task that:
/// 1. Receives RPC requests from the coordinator
/// 2. Sends back a simple "pong" response
///
/// Do not run this concurrently with another receive-loop helper on the same
/// coordinator. Use [`spawn_rpc_dispatcher`] when a peer handles both requests
/// and responses.
///
/// # Returns
/// A JoinHandle that can be used to abort the task
pub fn spawn_echo_responder(
    coordinator: Arc<WebRtcCoordinator>,
    gate: Arc<crate::outbound::PeerGate>,
    peer_name: &str,
) -> tokio::task::JoinHandle<()> {
    let peer_name = peer_name.to_string();
    tokio::spawn(async move {
        tracing::info!("🎯 {} echo responder task started", peer_name);
        loop {
            match coordinator.receive_message().await {
                Ok(Some((sender_id_bytes, message_data, _payload_type))) => {
                    match RpcEnvelope::decode(message_data.as_ref()) {
                        Ok(request) => {
                            handle_echo_request(
                                gate.as_ref(),
                                &peer_name,
                                &sender_id_bytes,
                                request,
                                true,
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::error!("{}: Failed to decode RpcEnvelope: {}", peer_name, e);
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!("📪 {} message channel closed", peer_name);
                    break;
                }
                Err(e) => {
                    tracing::error!("{}: Error receiving message: {}", peer_name, e);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    })
}

/// Spawn a single direction-aware RPC receive loop for a bidirectional peer.
///
/// Each envelope is dequeued and decoded once, then routed by its explicit
/// [`Direction`]. This prevents request and response helpers from racing to
/// consume one coordinator receive queue.
pub fn spawn_rpc_dispatcher(
    coordinator: Arc<WebRtcCoordinator>,
    gate: Arc<crate::outbound::PeerGate>,
    peer_name: &str,
) -> tokio::task::JoinHandle<()> {
    let peer_name = peer_name.to_string();
    tokio::spawn(async move {
        tracing::info!("🎯 {} RPC dispatcher task started", peer_name);
        loop {
            match coordinator.receive_message().await {
                Ok(Some((sender_id_bytes, message_data, _payload_type))) => {
                    let envelope = match RpcEnvelope::decode(message_data.as_ref()) {
                        Ok(envelope) => envelope,
                        Err(e) => {
                            tracing::error!("{}: Failed to decode RpcEnvelope: {}", peer_name, e);
                            continue;
                        }
                    };

                    let direction = match direction_for_routing(envelope.direction) {
                        Ok(direction) => direction,
                        Err(error) => {
                            let reason = match error {
                                DirectionError::Missing => "missing",
                                DirectionError::Unspecified => "unspecified",
                                DirectionError::Unknown => "unknown",
                            };
                            tracing::warn!(
                                peer = %peer_name,
                                request_id = %envelope.request_id,
                                route_key = %envelope.route_key,
                                direction = ?envelope.direction,
                                reason,
                                "invalid RpcEnvelope.direction; dropping"
                            );
                            continue;
                        }
                    };

                    match direction {
                        Direction::Request | Direction::Tell => {
                            let should_respond = direction == Direction::Request;
                            let gate = gate.clone();
                            let peer_name = peer_name.clone();
                            tokio::spawn(async move {
                                handle_echo_request(
                                    gate.as_ref(),
                                    &peer_name,
                                    &sender_id_bytes,
                                    envelope,
                                    should_respond,
                                )
                                .await;
                            });
                        }
                        Direction::Response => {
                            handle_rpc_response(gate.as_ref(), &peer_name, envelope).await;
                        }
                        Direction::Unspecified => {
                            unreachable!("direction_for_routing rejects unspecified directions");
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!("📪 {} message channel closed", peer_name);
                    break;
                }
                Err(e) => {
                    tracing::error!("{}: Error receiving message: {}", peer_name, e);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    })
}
