//! WebSocketGate - message router for inbound WebSocket connections
//!
//! Receives new connections from the `WebSocketServer` channel (including sender ActrId bytes and AIdCredential),
//! performs Ed25519 credential verification for each connection (if `WsAuthContext` is configured),
//! then routes messages by PayloadType to Mailbox or DataStreamRegistry upon successful verification.
//!
//! # Design comparison with WebRtcGate
//!
//! | Concern | WebRtcGate | WebSocketGate |
//! |---------|-----------|---------------|
//! | Transport | WebRTC DataChannel | WebSocket (TCP) |
//! | Sender auth | actrix signaling verifies credential | Local Ed25519 verification (AisKeyCache) |
//! | Message aggregation | `WebRtcCoordinator.receive_message()` | Per-connection `DataLane` reading |

use super::connection::WebSocketConnection;
use super::server::InboundWsConn;
use crate::inbound::DataStreamRegistry;
use crate::key_cache::AisKeyCache;
use crate::lifecycle::CredentialState;
use crate::transport::WsSink;
use crate::wire::SignalingKeyFetcher;
use crate::wire::webrtc::SignalingClient;
use crate::wire::webrtc::{HookCallback, HookEvent};
use actr_framework::Bytes;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{
    AIdCredential, ActrId, DataStream, Direction, IdentityClaims, PayloadType, RpcEnvelope,
};
use actr_protocol::{ActorResult, ActrError};
use actr_runtime_mailbox::{Mailbox, MessagePriority};
use ed25519_dalek::{Signature, Verifier as Ed25519Verifier};
use futures_util::SinkExt;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// Pending requests map type: request_id → (target_actor_id, oneshot response sender)
type PendingRequestsMap =
    Arc<RwLock<HashMap<String, (ActrId, oneshot::Sender<actr_protocol::ActorResult<Bytes>>)>>>;

/// Why an inbound `RpcEnvelope.direction` could not be routed.
///
/// `direction_for_routing` returns this so the caller can decode the peer
/// (for diagnostics) only on the drop path, never on the happy path.
#[derive(Debug)]
enum DirectionError {
    /// `direction` field absent from the envelope.
    Missing,
    /// `direction` present but `DIRECTION_UNSPECIFIED`.
    Unspecified,
    /// `direction` present but not a known variant.
    Unknown,
}

/// WebSocket authentication context (optional)
///
/// When configured, gate will perform Ed25519 credential verification for each inbound connection:
/// - Connections that fail verification are dropped without starting lane readers
/// - Connections without credentials are treated as verification failures
pub(crate) struct WsAuthContext {
    /// AIS signing public key cache (local hit verifies directly, miss fetches via signaling)
    pub(crate) ais_key_cache: Arc<AisKeyCache>,
    /// Local ActrId (needed when requesting public key from signaling on cache miss)
    pub(crate) actor_id: ActrId,
    /// Local credential state (needed for signaling authentication on cache miss)
    pub(crate) credential_state: CredentialState,
    /// Signaling client (used to fetch public key on cache miss)
    pub(crate) signaling_client: Arc<dyn SignalingClient>,
}

/// Registry type: inbound peer ActrId → write-half of the WebSocket connection.
///
/// Populated when a peer connects inbound (after credential verification).
/// Used by `send_response` to route server-to-client responses back over the
/// same connection the client used to send the request — avoiding the need for
/// a reverse WebRTC/WebSocket dial which would fail in test environments.
type InboundSinkMap = Arc<RwLock<HashMap<ActrId, WsSink>>>;

/// WebSocketGate - receives and routes inbound WebSocket messages
pub(crate) struct WebSocketGate {
    /// Inbound connection channel (taken once and moved into background task)
    conn_rx: tokio::sync::Mutex<Option<mpsc::Receiver<InboundWsConn>>>,

    /// Pending requests map (request_id -> (caller_id, oneshot::Sender))
    /// **Shared with PeerGate** for correct Response routing
    pending_requests: PendingRequestsMap,

    /// DataStream registry (fast-path stream message routing)
    data_stream_registry: Arc<DataStreamRegistry>,

    /// Inbound connection authentication context
    auth_ctx: Option<Arc<WsAuthContext>>,

    /// Hook callback for WebSocket peer lifecycle events
    /// (`WebSocketConnectStart` / `Connected` / `Disconnected`).
    hook_callback: OnceLock<HookCallback>,

    /// Inbound peer sinks: peer ActrId → WsSink.
    ///
    /// Populated when a verified peer establishes an inbound connection;
    /// cleaned up on disconnect.  Used by `send_response` to route replies
    /// back over the same inbound WebSocket connection.
    inbound_sinks: InboundSinkMap,
}

impl WebSocketGate {
    /// Create WebSocketGate
    ///
    /// # Arguments
    /// - `conn_rx`: receiver end from `WebSocketServer::bind()`
    /// - `pending_requests`: pending requests map shared with PeerGate
    /// - `data_stream_registry`: DataStream registry
    /// - `auth_ctx`: authentication context (when configured, enforces credential verification on all inbound connections)
    pub fn new(
        conn_rx: mpsc::Receiver<InboundWsConn>,
        pending_requests: PendingRequestsMap,
        data_stream_registry: Arc<DataStreamRegistry>,
        auth_ctx: Option<WsAuthContext>,
    ) -> Self {
        Self {
            conn_rx: tokio::sync::Mutex::new(Some(conn_rx)),
            pending_requests,
            data_stream_registry,
            auth_ctx: auth_ctx.map(Arc::new),
            hook_callback: OnceLock::new(),
            inbound_sinks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Install the WebSocket peer-lifecycle hook callback.
    ///
    /// Idempotent: subsequent calls are silently ignored. Invoked once
    /// during node startup.
    pub fn set_hook_callback(&self, cb: HookCallback) {
        let _ = self.hook_callback.set(cb);
    }

    /// Send a response envelope back to a peer that connected to us inbound.
    ///
    /// Looks up the peer's write-half in `inbound_sinks`.  Returns `Ok(false)`
    /// (not an error) when no inbound connection from `peer` is known — the
    /// caller should fall back to another transport (e.g. `WebRtcGate`).
    /// Returns `Ok(true)` on a successful send.
    pub async fn send_response(
        &self,
        peer: &ActrId,
        mut envelope: RpcEnvelope,
    ) -> ActorResult<bool> {
        let sink_opt = {
            let map = self.inbound_sinks.read().await;
            map.get(peer).cloned()
        };

        let sink = match sink_opt {
            Some(s) => s,
            None => return Ok(false),
        };

        envelope.direction = Some(Direction::Response as i32);

        // Serialize envelope
        let mut payload_buf = Vec::new();
        envelope.encode(&mut payload_buf).map_err(|e| {
            ActrError::Internal(format!("WebSocketGate: encode response failed: {e}"))
        })?;

        // Frame: [payload_type: 1 byte][len: 4 bytes BE][data: N bytes]
        let pt = PayloadType::RpcReliable as u8;
        let len = payload_buf.len() as u32;
        let mut frame = Vec::with_capacity(5 + payload_buf.len());
        frame.push(pt);
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&payload_buf);

        let mut sink_guard = sink.lock().await;
        match sink_guard.as_mut() {
            Some(s) => {
                s.send(WsMessage::Binary(frame.into())).await.map_err(|e| {
                    ActrError::Unavailable(format!("WebSocketGate: send_response failed: {e}"))
                })?;
                tracing::debug!(
                    peer = %peer,
                    request_id = %envelope.request_id,
                    "WebSocketGate: response sent via inbound connection"
                );
                Ok(true)
            }
            None => {
                tracing::warn!(peer = %peer, "WebSocketGate: inbound sink gone, cannot send response");
                Ok(false)
            }
        }
    }

    /// Handle RpcEnvelope: explicit Response wakes the waiting party,
    /// explicit Request enqueues into Mailbox. Missing, Unspecified, or
    /// unknown direction values are invalid and dropped.
    async fn handle_envelope(
        envelope: RpcEnvelope,
        from_bytes: Vec<u8>,
        data: Bytes,
        payload_type: PayloadType,
        pending_requests: PendingRequestsMap,
        mailbox: Arc<dyn Mailbox>,
    ) {
        let request_id = envelope.request_id.clone();

        let direction = match Self::direction_for_routing(envelope.direction) {
            Ok(direction) => direction,
            Err(error) => {
                // Drop path only: decode the peer lazily for diagnostics.
                let peer = Self::peer_for_log(&from_bytes);
                let reason = match error {
                    DirectionError::Missing => "missing",
                    DirectionError::Unspecified => "unspecified",
                    DirectionError::Unknown => "unknown",
                };
                tracing::warn!(
                    request_id = %request_id,
                    peer = %peer,
                    route_key = %envelope.route_key,
                    direction = ?envelope.direction,
                    reason = %reason,
                    "rpc.invalid_direction_dropped: invalid RpcEnvelope.direction; dropping"
                );
                return;
            }
        };

        if matches!(direction, Direction::Request) {
            Self::enqueue_request(from_bytes, data, payload_type, mailbox, &request_id).await;
            return;
        }

        let mut pending = pending_requests.write().await;
        if let Some((target, response_tx)) = pending.remove(&request_id) {
            drop(pending);
            tracing::debug!(
                "📬 WS Received RPC Response: request_id={}, target={}",
                request_id,
                target
            );

            let result = match (envelope.payload, envelope.error) {
                (Some(payload), None) => Ok(payload),
                (None, Some(error)) => Err(crate::lifecycle::node::wire_code_to_actr_error(
                    error.code,
                    error.message,
                )),
                _ => Err(ActrError::DecodeFailure(
                    "Invalid RpcEnvelope: payload and error fields inconsistent".to_string(),
                )),
            };
            let _ = response_tx.send(result);
        } else {
            drop(pending);
            let peer = Self::peer_for_log(&from_bytes);
            tracing::warn!(
                request_id = %request_id,
                peer = %peer,
                route_key = %envelope.route_key,
                "rpc.orphan_response_dropped: envelope marked Response has no pending request; dropping (late reply or peer-mislabeled request)"
            );
        }
    }

    fn peer_for_log(from_bytes: &[u8]) -> String {
        if from_bytes.is_empty() {
            return "unavailable".to_string();
        }

        ActrId::decode(from_bytes)
            .map(|peer| peer.to_string_repr())
            .unwrap_or_else(|e| format!("decode_failed:{e}"))
    }

    /// Classify an inbound `RpcEnvelope.direction` into a routable direction
    /// or a `DirectionError`.
    ///
    /// Pure: performs no logging and no peer decode. The caller handles
    /// diagnostics on the drop path so the happy path stays free of
    /// `ActrId::decode` + `to_string_repr()`.
    fn direction_for_routing(raw_direction: Option<i32>) -> Result<Direction, DirectionError> {
        match raw_direction {
            Some(raw) => match Direction::try_from(raw) {
                Ok(direction @ (Direction::Request | Direction::Response)) => Ok(direction),
                Ok(Direction::Unspecified) => Err(DirectionError::Unspecified),
                Err(_) => Err(DirectionError::Unknown),
            },
            None => Err(DirectionError::Missing),
        }
    }

    /// Enqueue an explicit inbound RPC request to the Mailbox.
    async fn enqueue_request(
        from_bytes: Vec<u8>,
        data: Bytes,
        payload_type: PayloadType,
        mailbox: Arc<dyn Mailbox>,
        request_id: &str,
    ) {
        tracing::debug!("📥 WS Received RPC Request: request_id={}", request_id);

        let priority = match payload_type {
            PayloadType::RpcSignal => MessagePriority::High,
            _ => MessagePriority::Normal,
        };

        match mailbox.enqueue(from_bytes, data.to_vec(), priority).await {
            Ok(msg_id) => {
                tracing::debug!(
                    "✅ WS RPC message enqueued: msg_id={}, priority={:?}",
                    msg_id,
                    priority
                );
            }
            Err(e) => {
                tracing::error!("❌ WS Mailbox enqueue failed: {:?}", e);
            }
        }
    }

    /// Verify AIdCredential Ed25519 signature of an inbound connection
    ///
    /// Returns `Some(verified_actor_id_str)` on successful verification, `None` on failure (already logged).
    /// `source_id_bytes` is the ActrId protobuf bytes from `X-Actr-Source-ID`.
    async fn verify_credential(
        credential: &AIdCredential,
        source_id_bytes: &[u8],
        auth_ctx: &WsAuthContext,
    ) -> Option<()> {
        // Get local credential (for signaling authentication on cache miss)
        let local_credential = auth_ctx.credential_state.credential().await;

        // Construct SignalingKeyFetcher adapter, wrapping signaling client as KeyFetcher
        let fetcher = SignalingKeyFetcher {
            client: auth_ctx.signaling_client.clone(),
            actor_id: auth_ctx.actor_id.clone(),
            credential: local_credential,
        };

        // Get verifying key for key_id from AisKeyCache (local hit or fetch from signaling)
        let verifying_key = match auth_ctx
            .ais_key_cache
            .get_or_fetch(credential.key_id, &fetcher)
            .await
        {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!(
                    key_id = credential.key_id,
                    error = ?e,
                    "WS credential verification failed: unable to get signing key"
                );
                return None;
            }
        };

        // Ed25519 signature verification
        let sig_result =
            credential.signature[..]
                .try_into()
                .ok()
                .and_then(|sig_bytes: [u8; 64]| {
                    let signature = Signature::from_bytes(&sig_bytes);
                    verifying_key
                        .verify(&credential.claims[..], &signature)
                        .ok()
                });
        if sig_result.is_none() {
            tracing::warn!(
                key_id = credential.key_id,
                "WS AIdCredential Ed25519 verification failed"
            );
            return None;
        }

        // Decode IdentityClaims
        let claims = match IdentityClaims::decode(&credential.claims[..]) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(key_id = credential.key_id, error = ?e, "WS IdentityClaims proto decode failed");
                return None;
            }
        };

        // Check expires_at
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if claims.expires_at <= now {
            tracing::warn!(
                key_id = credential.key_id,
                expires_at = claims.expires_at,
                "WS AIdCredential has expired"
            );
            return None;
        }

        // Verify claims.actor_id matches X-Actr-Source-ID (prevent identity claim mismatch)
        match ActrId::decode(source_id_bytes) {
            Ok(source_actor_id) => {
                let source_repr = source_actor_id.to_string_repr();
                if claims.actor_id != source_repr {
                    tracing::warn!(
                        claimed = %claims.actor_id,
                        source_id = %source_repr,
                        "WS credential actor_id does not match X-Actr-Source-ID, rejecting connection"
                    );
                    return None;
                }
                tracing::info!(
                    actor_id = %claims.actor_id,
                    "WS inbound connection identity verification passed"
                );
            }
            Err(e) => {
                tracing::warn!(error = ?e, "WS X-Actr-Source-ID decode failed, rejecting connection");
                return None;
            }
        }

        Some(())
    }

    /// Spawn receive tasks for a single WebSocket connection
    ///
    /// Reads `PayloadType::RpcReliable`, `RpcSignal`, `StreamReliable`,
    /// `StreamLatencyFirst` -- four lanes total, spawning an independent task for each.
    ///
    /// `inbound_sinks`: shared map into which the connection's write-half is
    /// inserted keyed by the peer ActrId.  Removed when all lane tasks exit.
    ///
    /// The sink is registered synchronously (before any lane task is spawned)
    /// so that `send_response` can find it even when the first request arrives
    /// via a parallel transport (e.g. WebRTC) before the lanes are fully up.
    async fn spawn_connection_tasks(
        conn: WebSocketConnection,
        source_id: Vec<u8>,
        pending_requests: PendingRequestsMap,
        data_stream_registry: Arc<DataStreamRegistry>,
        mailbox: Arc<dyn Mailbox>,
        hook_callback: Option<HookCallback>,
        inbound_sinks: InboundSinkMap,
    ) {
        // Fire `WebSocketConnected` for the peer once the connection is
        // accepted. Decoding the peer `ActrId` may fail if the source-id
        // header is malformed — in that case we skip hooks but still run
        // the lane readers so that the connection can fail-fast on its
        // own terms.
        let peer_id = ActrId::decode(&source_id[..]).ok();
        if let (Some(peer), Some(cb)) = (peer_id.clone(), hook_callback.clone()) {
            let cb_for_connected = cb.clone();
            tokio::spawn(async move {
                cb_for_connected(HookEvent::WebSocketConnected { peer_id: peer }).await;
            });
        }

        // Register the write-half so `send_response` can route replies back.
        // Done here (synchronously, before any lane-reader is spawned) so that
        // the first RPC response after connection establishment can always find
        // the sink even when the request arrives via a different transport
        // (e.g. WebRTC) that is already in use.
        if let Some(ref peer) = peer_id {
            let sink = conn.sink();
            inbound_sinks.write().await.insert(peer.clone(), sink);
        }

        // Count active per-lane reader tasks. When the last one exits
        // we fire `WebSocketDisconnected` exactly once (and remove the sink).
        let active_lanes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // Spawn per-PayloadType receive tasks
        for pt in [
            PayloadType::RpcReliable,
            PayloadType::RpcSignal,
            PayloadType::StreamReliable,
            PayloadType::StreamLatencyFirst,
        ] {
            let conn_clone = conn.clone();
            let src = source_id.clone();
            let pending = pending_requests.clone();
            let registry = data_stream_registry.clone();
            let mb = mailbox.clone();
            let active_lanes = active_lanes.clone();
            let peer_id_for_lane = peer_id.clone();
            let hook_cb_for_lane = hook_callback.clone();
            let sinks_for_lane = inbound_sinks.clone();
            active_lanes.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

            tokio::spawn(async move {
                // get_lane lazily creates the mpsc channel and registers in router
                let lane = match conn_clone.get_lane(pt).await {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!("❌ WS get_lane({:?}) failed: {:?}", pt, e);
                        return;
                    }
                };

                tracing::debug!("📡 WS lane reader started for {:?}", pt);

                loop {
                    match lane.recv().await {
                        Ok(data) => {
                            let data_bytes = Bytes::copy_from_slice(&data);

                            match pt {
                                PayloadType::RpcReliable | PayloadType::RpcSignal => {
                                    match RpcEnvelope::decode(&data[..]) {
                                        Ok(envelope) => {
                                            Self::handle_envelope(
                                                envelope,
                                                src.clone(),
                                                data_bytes,
                                                pt,
                                                pending.clone(),
                                                mb.clone(),
                                            )
                                            .await;
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "❌ WS Failed to decode RpcEnvelope: {:?}",
                                                e
                                            );
                                        }
                                    }
                                }
                                PayloadType::StreamReliable | PayloadType::StreamLatencyFirst => {
                                    match DataStream::decode(&data[..]) {
                                        Ok(chunk) => {
                                            tracing::debug!(
                                                "📦 WS Received DataStream: stream_id={}, seq={}",
                                                chunk.stream_id,
                                                chunk.sequence,
                                            );
                                            match ActrId::decode(&src[..]) {
                                                Ok(sender_id) => {
                                                    registry.dispatch(chunk, sender_id).await;
                                                }
                                                Err(e) => {
                                                    tracing::error!(
                                                        "❌ WS Failed to decode sender ActrId: {:?}",
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "❌ WS Failed to decode DataStream: {:?}",
                                                e
                                            );
                                        }
                                    }
                                }
                                PayloadType::MediaRtp => {
                                    tracing::warn!(
                                        "⚠️ MediaRtp received in WebSocketGate (unexpected)"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::info!("🔌 WS lane {:?} closed: {:?}", pt, e);
                            break;
                        }
                    }
                }

                tracing::debug!("📡 WS lane reader exited for {:?}", pt);

                // Last lane out fires the disconnected hook exactly once
                // and removes the inbound sink from the registry.
                let remaining = active_lanes
                    .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
                    .saturating_sub(1);
                if remaining == 0 {
                    if let Some(ref peer) = peer_id_for_lane {
                        sinks_for_lane.write().await.remove(peer);
                        tracing::debug!(peer = %peer, "WS inbound sink removed (all lanes closed)");
                    }
                    if let (Some(peer), Some(cb)) = (peer_id_for_lane, hook_cb_for_lane) {
                        cb(HookEvent::WebSocketDisconnected { peer_id: peer }).await;
                    }
                }
            });
        }
    }

    /// Start the connection accept loop (called by ActrNode, can only be called once)
    ///
    /// Internally takes `conn_rx` and moves it into a background task for lock-free new connection reception.
    /// If `auth_ctx` is configured, performs credential verification for each inbound connection;
    /// drops connections on verification failure, calls `spawn_connection_tasks` on success.
    pub async fn start_receive_loop(&self, mailbox: Arc<dyn Mailbox>) -> ActorResult<()> {
        let rx = self.conn_rx.lock().await.take().ok_or_else(|| {
            ActrError::Internal("WebSocketGate: start_receive_loop already called".to_string())
        })?;

        let pending_requests = self.pending_requests.clone();
        let data_stream_registry = self.data_stream_registry.clone();
        let auth_ctx = self.auth_ctx.clone();
        let hook_cb = self.hook_callback.get().cloned();
        let inbound_sinks = self.inbound_sinks.clone();

        tokio::spawn(async move {
            tracing::info!("🚀 WebSocketGate receive loop started");

            let mut rx = rx;
            while let Some((conn, source_id, credential_opt)) = rx.recv().await {
                tracing::info!(
                    "🔗 WS new inbound connection (source_id len={}, has_credential={})",
                    source_id.len(),
                    credential_opt.is_some()
                );

                // Fire `WebSocketConnectStart` as soon as we observe an
                // inbound connection, before verification — this mirrors
                // how the WebRTC path emits `WebRtcConnectStart` before
                // the selected ICE candidate pair is known.
                if let (Some(cb), Ok(peer)) = (hook_cb.clone(), ActrId::decode(&source_id[..])) {
                    let peer_clone = peer.clone();
                    tokio::spawn(async move {
                        cb(HookEvent::WebSocketConnectStart {
                            peer_id: peer_clone,
                        })
                        .await;
                    });
                }

                // Credential verification (if auth_ctx is configured)
                if let Some(ref ctx) = auth_ctx {
                    match credential_opt {
                        Some(ref credential) => {
                            if Self::verify_credential(credential, &source_id, ctx)
                                .await
                                .is_none()
                            {
                                tracing::warn!(
                                    "WS inbound connection credential verification failed, dropping connection"
                                );
                                continue; // drop connection, wait for next one
                            }
                        }
                        None => {
                            tracing::warn!(
                                "WS inbound connection missing X-Actr-Credential, rejecting connection (auth_ctx configured)"
                            );
                            continue;
                        }
                    }

                    Self::spawn_connection_tasks(
                        conn,
                        source_id,
                        pending_requests.clone(),
                        data_stream_registry.clone(),
                        mailbox.clone(),
                        hook_cb.clone(),
                        inbound_sinks.clone(),
                    )
                    .await;
                } else {
                    tracing::error!(
                        "WS auth_ctx not configured, rejecting connection (configuration error)"
                    );
                }
            }

            tracing::info!("🔌 WebSocketGate receive loop exited");
        });

        Ok(())
    }
}
#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;
