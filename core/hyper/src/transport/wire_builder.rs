//! WireBuilder - Wire layer component builder
//!
//! Provides default Wire component builder implementation, supporting:
//! - WebRTC P2P connections (through WebRtcCoordinator)
//! - WebSocket transport connections
//! - CancellationToken for terminating in-progress connection creation

use super::Dest; // Re-exported from actr-framework
use super::error::{NetworkError, NetworkResult};
use super::lane::DataLane;
use super::peer_transport::WireBuilder;
use super::wire_handle::WireHandle;
use super::wire_pool::ConnType;
use crate::lifecycle::CredentialState;
use crate::lifecycle::session_state::SessionState;
use crate::outbound::PendingRequestsMap;
use crate::wire::webrtc::WebRtcCoordinator;
use crate::wire::websocket::WebSocketConnection;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActrError, ActrId, PayloadType, RpcEnvelope};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// Default Wire builder configuration
pub struct DefaultWireBuilderConfig {
    /// Local node identity as hex-encoded protobuf `ActrId` bytes, sent in the `X-Actr-Source-ID` header during outbound WebSocket handshakes.
    pub local_id_hex: String,

    /// Enable WebRTC
    pub enable_webrtc: bool,

    /// Enable WebSocket
    pub enable_websocket: bool,

    /// Shared map of discovered WebSocket direct-connect URLs, keyed by ActrId.
    ///
    /// Populated by discovery flow after receiving ws_address info
    /// from the signaling server.  When a connection to an ActrId is needed and this map
    /// contains an entry for it, the stored URL is used instead of the url_template.
    pub discovered_ws_addresses: Arc<RwLock<HashMap<ActrId, String>>>,

    /// Optional local credential state. During outbound WebSocket handshakes the current credential is base64-encoded and sent in the `X-Actr-Credential` header so the peer can verify the Ed25519 signature.
    pub credential_state: Option<CredentialState>,

    /// Unified session state. When present, outbound WebSocket handshakes read
    /// source identity and credential from the current snapshot.
    pub session_state: Option<SessionState>,

    /// Shared pending-requests map.
    ///
    /// When set, outbound WebSocket connections spawn reader tasks that deliver
    /// server responses (and error envelopes) to the waiting `send_request` calls.
    /// Must be the same map as `PeerGate.pending_requests`.
    pub pending_requests: Option<PendingRequestsMap>,
}

impl Default for DefaultWireBuilderConfig {
    fn default() -> Self {
        Self {
            local_id_hex: String::new(),
            enable_webrtc: true,
            enable_websocket: true,
            discovered_ws_addresses: Arc::new(RwLock::new(HashMap::new())),
            credential_state: None,
            session_state: None,
            pending_requests: None,
        }
    }
}

/// Default builder for wire-layer connections.
///
/// Creates WebRTC and/or WebSocket wire handles from configuration and supports attempting multiple connection types during the same creation pass.
pub struct DefaultWireBuilder {
    /// Optional WebRTC coordinator.
    webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,

    /// Local node identity hex string used as `X-Actr-Source-ID` in outbound WebSocket handshakes.
    local_id_hex: String,

    /// Unified session state used for dynamic direct WebSocket identity.
    session_state: Option<SessionState>,

    /// Shared map of discovered WebSocket URLs (from signaling discovery)
    discovered_ws_addresses: Arc<RwLock<HashMap<ActrId, String>>>,

    /// Local credential state used to provide `X-Actr-Credential` during outbound WebSocket handshakes.
    credential_state: Option<CredentialState>,

    /// Pending requests map (shared with PeerGate) for outbound WebSocket response routing.
    pending_requests: Option<PendingRequestsMap>,

    /// Builder configuration.
    config: DefaultWireBuilderConfig,
}

impl DefaultWireBuilder {
    /// Create a new wire builder.
    ///
    /// # Arguments
    /// - `webrtc_coordinator`: WebRTC coordinator when WebRTC support is enabled
    /// - `config`: builder configuration
    pub fn new(
        webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,
        config: DefaultWireBuilderConfig,
    ) -> Self {
        Self {
            webrtc_coordinator,
            local_id_hex: config.local_id_hex.clone(),
            session_state: config.session_state.clone(),
            discovered_ws_addresses: config.discovered_ws_addresses.clone(),
            credential_state: config.credential_state.clone(),
            pending_requests: config.pending_requests.clone(),
            config,
        }
    }

    /// Look up the direct WebSocket URL for the target node, sourced only from service discovery.
    async fn resolve_websocket_url(&self, dest: &Dest) -> Option<String> {
        if let Dest::Actor(actor_id) = dest {
            let map = self.discovered_ws_addresses.read().await;
            if let Some(url) = map.get(actor_id) {
                tracing::debug!(
                    "🔎 [Factory] Using discovered WebSocket URL for {}: {}",
                    actor_id,
                    url
                );
                return Some(url.clone());
            }
        }
        None
    }
}

// ── ClientWebSocketHandle ─────────────────────────────────────────────────────

/// `WireHandle` wrapper around an outbound `WebSocketConnection` that, after
/// a successful `connect()`, spawns lane-reader tasks routing incoming
/// `RpcEnvelope` responses back to the shared `pending_requests` map.
///
/// Without this, the server-side `WebSocketGate.send_response()` would write
/// the response onto the TCP connection but nobody on the client side would
/// consume it from the dispatcher lanes and deliver it to the waiting
/// `send_request_with_type` future.
#[derive(Debug)]
struct ClientWebSocketHandle {
    inner: WebSocketConnection,
    pending_requests: PendingRequestsMap,
}

impl ClientWebSocketHandle {
    fn new(inner: WebSocketConnection, pending_requests: PendingRequestsMap) -> Self {
        Self {
            inner,
            pending_requests,
        }
    }

    /// Spawn a task that reads `RpcReliable` and `RpcSignal` lanes on the
    /// outbound connection and routes every received envelope to
    /// `pending_requests`.
    ///
    /// Responses are identified by `request_id`: if a matching entry exists
    /// in `pending_requests`, the oneshot sender is fired.  Unknown
    /// `request_id` values are dropped with a debug log (they arrive before
    /// the sender registers, which should not happen in practice).
    fn spawn_response_readers(&self) {
        for pt in [PayloadType::RpcReliable, PayloadType::RpcSignal] {
            let conn = self.inner.clone();
            let pending = self.pending_requests.clone();

            tokio::spawn(async move {
                let lane = match conn.get_lane(pt).await {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!("ClientWebSocketHandle: get_lane({pt:?}) failed: {e:?}");
                        return;
                    }
                };

                tracing::debug!("ClientWebSocketHandle: response reader started for {pt:?}");

                loop {
                    let data = match lane.recv().await {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::info!("ClientWebSocketHandle: lane {pt:?} closed: {e:?}");
                            break;
                        }
                    };

                    match RpcEnvelope::decode(&data[..]) {
                        Ok(envelope) => {
                            let request_id = &envelope.request_id;
                            let mut guard = pending.write().await;
                            if let Some((_target, tx)) = guard.remove(request_id.as_str()) {
                                drop(guard);
                                let result: actr_protocol::ActorResult<actr_framework::Bytes> =
                                    match (envelope.payload, envelope.error) {
                                        (Some(payload), None) => Ok(payload),
                                        (None, Some(err)) => {
                                            Err(crate::lifecycle::node::wire_code_to_actr_error(
                                                err.code,
                                                err.message,
                                            ))
                                        }
                                        _ => Err(ActrError::DecodeFailure(
                                            "invalid RpcEnvelope: payload/error inconsistent"
                                                .to_string(),
                                        )),
                                    };
                                let _ = tx.send(result);
                            } else {
                                drop(guard);
                                tracing::debug!(
                                    request_id = %request_id,
                                    "ClientWebSocketHandle: no pending request for incoming envelope, dropping"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "ClientWebSocketHandle: RpcEnvelope decode failed: {e:?}"
                            );
                        }
                    }
                }

                tracing::debug!("ClientWebSocketHandle: response reader exited for {pt:?}");
            });
        }
    }
}

#[async_trait]
impl WireHandle for ClientWebSocketHandle {
    fn connection_type(&self) -> ConnType {
        ConnType::WebSocket
    }

    fn priority(&self) -> u8 {
        self.inner.priority()
    }

    async fn connect(&self) -> NetworkResult<()> {
        self.inner.connect().await?;
        self.spawn_response_readers();
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    async fn close(&self) -> NetworkResult<()> {
        self.inner.close().await
    }

    async fn get_lane(&self, payload_type: PayloadType) -> NetworkResult<Arc<dyn DataLane>> {
        self.inner.get_lane(payload_type).await
    }
}

#[async_trait]
impl WireBuilder for DefaultWireBuilder {
    #[cfg_attr(feature = "opentelemetry", tracing::instrument(skip_all))]
    async fn create_connections(&self, dest: &Dest) -> NetworkResult<Vec<Arc<dyn WireHandle>>> {
        // Delegate to method with no cancel token
        self.create_connections_with_cancel(dest, None).await
    }

    #[cfg_attr(feature = "opentelemetry", tracing::instrument(skip_all))]
    async fn create_connections_with_cancel(
        &self,
        dest: &Dest,
        cancel_token: Option<CancellationToken>,
    ) -> NetworkResult<Vec<Arc<dyn WireHandle>>> {
        let mut connections: Vec<Arc<dyn WireHandle>> = Vec::new();

        // Helper to check cancellation
        let check_cancelled = |token: &Option<CancellationToken>| -> NetworkResult<()> {
            if let Some(t) = token {
                if t.is_cancelled() {
                    return Err(NetworkError::ConnectionClosed(
                        "Connection creation cancelled".to_string(),
                    ));
                }
            }
            Ok(())
        };

        // 1. Check whether the operation was already cancelled.
        check_cancelled(&cancel_token)?;

        // 2. Try to establish a WebSocket connection.
        // The URL comes from service discovery (`discovered_ws_addresses`). If nothing was discovered, skip WebSocket for this attempt.
        if self.config.enable_websocket {
            check_cancelled(&cancel_token)?;

            if let Some(url) = self.resolve_websocket_url(dest).await {
                tracing::debug!("🏭 [Factory] Create WebSocket Connect: {}", url);
                let (local_id_hex, session_credential) =
                    if let Some(session_state) = &self.session_state {
                        let snapshot = session_state.snapshot().await;
                        (
                            hex::encode(snapshot.actor_id.encode_to_vec()),
                            Some(snapshot.credential),
                        )
                    } else {
                        (self.local_id_hex.clone(), None)
                    };
                let mut ws_conn = WebSocketConnection::new(url).with_local_id(local_id_hex);

                // Attach the local credential so the peer `WebSocketGate` can verify the Ed25519 signature.
                if let Some(credential) = session_credential {
                    let cred_bytes = credential.encode_to_vec();
                    use base64::Engine as _;
                    let cred_b64 = base64::engine::general_purpose::STANDARD.encode(&cred_bytes);
                    ws_conn = ws_conn.with_credential_b64(cred_b64);
                } else if let Some(ref cred_state) = self.credential_state {
                    let credential = cred_state.credential().await;
                    let cred_bytes = credential.encode_to_vec();
                    use base64::Engine as _;
                    let cred_b64 = base64::engine::general_purpose::STANDARD.encode(&cred_bytes);
                    ws_conn = ws_conn.with_credential_b64(cred_b64);
                }

                // Wrap the connection so that, after `connect()`, response reader
                // tasks are spawned to deliver server responses to `pending_requests`.
                if let Some(ref pending) = self.pending_requests {
                    connections.push(
                        Arc::new(ClientWebSocketHandle::new(ws_conn, pending.clone()))
                            as Arc<dyn WireHandle>,
                    );
                } else {
                    connections.push(Arc::new(ws_conn) as Arc<dyn WireHandle>);
                }
            } else {
                tracing::debug!(
                    "🔎 [Factory] No WebSocket URL available for {:?}, skipping WS connection",
                    dest
                );
            }
        }

        // 3. Check cancellation before trying WebRTC.
        check_cancelled(&cancel_token)?;

        // 4. Attempt to create a WebRTC connection.
        if self.config.enable_webrtc {
            if let Some(coordinator) = &self.webrtc_coordinator {
                // WebRTC is only supported for actor destinations.
                if dest.is_actor() {
                    tracing::debug!("🏭 [Factory] Creating WebRTC connection to: {:?}", dest);

                    // Check cancellation before long-running operation
                    check_cancelled(&cancel_token)?;

                    match coordinator
                        .create_connection(dest, cancel_token.clone())
                        .await
                    {
                        Ok(webrtc_conn) => {
                            // Check cancellation again after creation.
                            if let Err(e) = check_cancelled(&cancel_token) {
                                // Clean up the newly created connection.
                                if let Err(close_err) = webrtc_conn.close().await {
                                    tracing::warn!(
                                        "⚠️ [Factory] Failed to close cancelled connection: {}",
                                        close_err
                                    );
                                }
                                return Err(e);
                            }
                            connections.push(Arc::new(webrtc_conn) as Arc<dyn WireHandle>);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "❌ [Factory] WebRTC connection creation failed: {:?}: {}",
                                dest,
                                e
                            );
                            // Do not return an error here; allow other connection types to proceed.
                        }
                    }
                } else {
                    tracing::debug!(
                        "ℹ️ [Factory] WebRTC does not support this destination type, skipping"
                    );
                }
            } else {
                tracing::warn!(
                    "⚠️ [Factory] WebRTC is enabled but no WebRtcCoordinator was provided"
                );
            }
        }

        tracing::info!(
            "✨ [Factory] Finished creating {} connections for {:?}",
            connections.len(),
            dest,
        );

        Ok(connections)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::ConnType;
    use actr_protocol::ActrId;

    #[tokio::test]
    async fn test_no_ws_connection_without_discovery() {
        // WebSocket URLs come only from service discovery; without a discovery record no WS connection should be created.
        let config = DefaultWireBuilderConfig {
            enable_websocket: true,
            enable_webrtc: false,
            local_id_hex: "deadbeef".to_string(),
            discovered_ws_addresses: Arc::new(RwLock::new(HashMap::new())),
            credential_state: None,
            session_state: None,
            pending_requests: None,
        };
        let factory = DefaultWireBuilder::new(None, config);
        let dest = Dest::actor(ActrId::default());
        let connections = factory.create_connections(&dest).await.unwrap();
        assert!(connections.is_empty());
    }

    #[tokio::test]
    async fn test_ws_connection_from_discovery() {
        // A discovered address should allow a WS connection to be created.
        let map = Arc::new(RwLock::new(HashMap::new()));
        let actor_id = ActrId::default();
        map.write()
            .await
            .insert(actor_id.clone(), "ws://localhost:9001".to_string());

        let config = DefaultWireBuilderConfig {
            enable_websocket: true,
            enable_webrtc: false,
            local_id_hex: "deadbeef".to_string(),
            discovered_ws_addresses: map,
            credential_state: None,
            session_state: None,
            pending_requests: None,
        };
        let factory = DefaultWireBuilder::new(None, config);
        let dest = Dest::actor(actor_id);
        let connections = factory.create_connections(&dest).await.unwrap();
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].connection_type(), ConnType::WebSocket);
    }

    #[test]
    fn default_config_enables_both_transports_with_empty_identity() {
        let cfg = DefaultWireBuilderConfig::default();
        assert!(cfg.enable_webrtc);
        assert!(cfg.enable_websocket);
        assert!(cfg.local_id_hex.is_empty());
        assert!(cfg.credential_state.is_none());
        assert!(cfg.session_state.is_none());
        assert!(cfg.pending_requests.is_none());
    }

    #[tokio::test]
    async fn cancelled_token_aborts_before_any_connection() {
        // Even with a discovered WS URL present, a pre-cancelled token must
        // short-circuit before opening a connection.
        let map = Arc::new(RwLock::new(HashMap::new()));
        let actor_id = ActrId::default();
        map.write()
            .await
            .insert(actor_id.clone(), "ws://localhost:9001".to_string());

        let config = DefaultWireBuilderConfig {
            enable_websocket: true,
            enable_webrtc: true, // also enabled — must still be skipped due to early cancel
            local_id_hex: "deadbeef".to_string(),
            discovered_ws_addresses: map,
            credential_state: None,
            session_state: None,
            pending_requests: None,
        };
        let factory = DefaultWireBuilder::new(None, config);

        let token = CancellationToken::new();
        token.cancel();

        let dest = Dest::actor(actor_id);
        let res = factory
            .create_connections_with_cancel(&dest, Some(token))
            .await;
        assert!(
            matches!(res, Err(NetworkError::ConnectionClosed(_))),
            "cancelled creation should yield ConnectionClosed, got {res:?}"
        );
    }

    #[tokio::test]
    async fn resolve_websocket_url_miss_then_hit_for_actor() {
        let map = Arc::new(RwLock::new(HashMap::new()));
        let id = ActrId::default();
        let factory = DefaultWireBuilder::new(
            None,
            DefaultWireBuilderConfig {
                discovered_ws_addresses: map.clone(),
                ..Default::default()
            },
        );

        // Cache miss → None.
        assert!(
            factory
                .resolve_websocket_url(&Dest::actor(id.clone()))
                .await
                .is_none()
        );

        // Populate discovery map → hit returns the URL.
        map.write()
            .await
            .insert(id.clone(), "ws://host:7".to_string());
        assert_eq!(
            factory
                .resolve_websocket_url(&Dest::actor(id))
                .await
                .as_deref(),
            Some("ws://host:7")
        );
    }
}
