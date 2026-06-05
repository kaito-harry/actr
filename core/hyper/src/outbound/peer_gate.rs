//! PeerGate - Peer transport adapter (outbound)
//!
//! # Responsibilities
//! - Wrap PeerTransport (Protobuf serialization)
//! - Used for cross-process communication (WebRTC + WebSocket)
//! - Maintain pending_requests (Request/Response matching)
//! - Block new requests to peers being cleaned up (closing_peers)

use super::data_stream_activity::{DataStreamActivityTracker, DataStreamRecordState};
use crate::transport::{ConnectionEvent, ConnectionState, Dest, PayloadTypeExt, PeerTransport};
use crate::wire::webrtc::{NETWORK_RECOVERY_TIMEOUT, NetworkRecoveryStatus, WebRtcCoordinator};
use actr_framework::{Bytes, MediaSample};
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActorResult, ActrError, ActrId, Classify, PayloadType, RpcEnvelope};
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, broadcast, oneshot};

/// Pending requests map type: request_id -> (target_actor_id, oneshot response sender)
type PendingRequestsMap =
    Arc<RwLock<HashMap<String, (ActrId, oneshot::Sender<actr_protocol::ActorResult<Bytes>>)>>>;

/// Internal upper bound for a single DataStream send operation.
///
/// DataStream has no caller-provided request deadline like RPC envelopes, so
/// this prevents a stalled WebRTC DataChannel send from holding the mobile
/// caller forever during unrecoverable network loss.
const DATA_STREAM_SEND_TIMEOUT: Duration = Duration::from_secs(15);

/// PeerGate - Outproc transport adapter (outbound)
///
/// # Features
/// - Protobuf serialization: serialize RpcEnvelope to byte stream
/// - Defaults to PayloadType::RpcReliable for RPC messages
/// - Maintain pending_requests for Request/Response matching
/// - Support MediaTrack sending via WebRTC
/// - Block new requests to peers being cleaned up (closing_peers)
pub struct PeerGate {
    /// PeerTransport instance
    transport_manager: Arc<PeerTransport>,

    /// Pending requests: request_id -> (target_actor_id, oneshot::Sender<Bytes>)
    /// Stores both the target ActorId and response sender for efficient cleanup by peer
    pending_requests: PendingRequestsMap,

    /// WebRTC coordinator (optional, for MediaTrack support)
    webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,

    #[allow(unused)]
    /// todo: Peers currently being cleaned up (block new requests) ,closed requests will be cleaned up in event listener
    closing_peers: Arc<RwLock<HashSet<ActrId>>>,

    /// Peers in the network/WebRTC recovery window. The stored session id keeps
    /// late events from older sessions from unblocking a newer recovery.
    recovering_peers: Arc<RwLock<HashMap<ActrId, NetworkRecoveryStatus>>>,

    /// Recently sent `send_data_stream` chunks used to surface delivery
    /// uncertainty when WebRTC/DataChannel state changes mid-stream.
    active_data_streams: Arc<RwLock<DataStreamActivityTracker>>,
}

impl PeerGate {
    fn remember_recovering_peer(
        recovering: &mut HashMap<ActrId, NetworkRecoveryStatus>,
        peer_id: &ActrId,
        session_id: u64,
        reason: &str,
    ) {
        match recovering.entry(peer_id.clone()) {
            Entry::Occupied(entry) if entry.get().session_id == session_id => {
                tracing::debug!(
                    peer_id = ?peer_id,
                    session_id,
                    elapsed_ms = entry.get().elapsed_ms(),
                    recovery_reason = entry.get().reason.as_str(),
                    "Peer already blocked for recovery",
                );
            }
            Entry::Occupied(mut entry) => {
                entry.insert(NetworkRecoveryStatus::new(session_id, reason));
            }
            Entry::Vacant(entry) => {
                entry.insert(NetworkRecoveryStatus::new(session_id, reason));
            }
        }
    }

    fn recovering_error(target: &ActrId, status: &NetworkRecoveryStatus) -> ActrError {
        ActrError::Unavailable(format!(
            "Connection recovering: peer={:?}, session_id={}, reason={}, elapsed_ms={}, timeout_ms={}",
            target,
            status.session_id,
            status.reason.as_str(),
            status.elapsed_ms(),
            NETWORK_RECOVERY_TIMEOUT.as_millis()
        ))
    }

    fn recovery_timeout_error(target: &ActrId, status: &NetworkRecoveryStatus) -> ActrError {
        ActrError::Unavailable(format!(
            "Connection recovery timeout: peer={:?}, session_id={}, reason={}, elapsed_ms={}, timeout_ms={}",
            target,
            status.session_id,
            status.reason.as_str(),
            status.elapsed_ms(),
            NETWORK_RECOVERY_TIMEOUT.as_millis()
        ))
    }

    async fn notify_active_data_streams_uncertain(
        webrtc_coordinator: &WebRtcCoordinator,
        active_data_streams: &Arc<RwLock<DataStreamActivityTracker>>,
        peer_id: &ActrId,
        session_id: u64,
        reason: &str,
    ) {
        let notices = {
            let mut tracker = active_data_streams.write().await;
            tracker.mark_delivery_uncertain(peer_id, session_id, reason, Instant::now())
        };

        for notice in notices {
            webrtc_coordinator
                .notify_data_stream_delivery_uncertain(
                    notice.stream_id,
                    notice.session_id,
                    notice.reason,
                )
                .await;
        }
    }

    async fn record_active_data_stream_if_needed(
        &self,
        target: &ActrId,
        stream_id: &str,
        session_id: u64,
        now: Instant,
    ) -> bool {
        let record_state = {
            let tracker = self.active_data_streams.read().await;
            tracker.record_state(target, stream_id, session_id, now)
        };

        if record_state != DataStreamRecordState::Fresh {
            self.active_data_streams.write().await.record_stream(
                target,
                stream_id.to_string(),
                session_id,
                now,
            );
        }

        record_state == DataStreamRecordState::Missing
    }

    /// Create new PeerGate
    ///
    /// # Arguments
    /// - `transport_manager`: PeerTransport instance
    /// - `webrtc_coordinator`: Optional WebRTC coordinator for MediaTrack support
    pub fn new(
        transport_manager: Arc<PeerTransport>,
        webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,
    ) -> Self {
        let closing_peers = Arc::new(RwLock::new(HashSet::new()));
        let recovering_peers = Arc::new(RwLock::new(HashMap::new()));
        let pending_requests = Arc::new(RwLock::new(HashMap::new()));
        let active_data_streams = Arc::new(RwLock::new(DataStreamActivityTracker::default()));

        // Start event listener if coordinator is available
        // This is the ONLY event subscriber - it triggers top-down cleanup
        if let Some(ref coordinator) = webrtc_coordinator {
            Self::spawn_event_listener(
                coordinator.subscribe_events(),
                Arc::clone(coordinator),
                Arc::clone(&pending_requests),
                Arc::clone(&closing_peers),
                Arc::clone(&recovering_peers),
                Arc::clone(&active_data_streams),
                Arc::clone(&transport_manager),
            );
        }

        Self {
            transport_manager,
            pending_requests,
            webrtc_coordinator,
            closing_peers,
            recovering_peers,
            active_data_streams,
        }
    }

    /// Spawn event listener task to handle connection events
    ///
    /// This is the **ONLY** event subscriber in the cleanup chain.
    /// It triggers top-down cleanup by calling transport_manager.close_transport().
    fn spawn_event_listener(
        mut event_rx: broadcast::Receiver<ConnectionEvent>,
        webrtc_coordinator: Arc<WebRtcCoordinator>,
        pending_requests: PendingRequestsMap,
        closing_peers: Arc<RwLock<HashSet<ActrId>>>,
        recovering_peers: Arc<RwLock<HashMap<ActrId, NetworkRecoveryStatus>>>,
        active_data_streams: Arc<RwLock<DataStreamActivityTracker>>,
        transport_manager: Arc<PeerTransport>,
    ) {
        tokio::spawn(async move {
            loop {
                let event = match event_rx.recv().await {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            "PeerGate event listener lagged by {} events, continuing",
                            n
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::debug!("PeerGate event listener stopped (channel closed)");
                        break;
                    }
                };
                tracing::debug!("PeerGate received connection event: {:?}", event);
                match &event {
                    // Block new requests when connection enters Disconnected/Failed state
                    ConnectionEvent::StateChanged {
                        peer_id,
                        session_id,
                        state: ConnectionState::Disconnected | ConnectionState::Failed,
                        ..
                    } => {
                        if !webrtc_coordinator
                            .is_active_session(peer_id, *session_id)
                            .await
                        {
                            tracing::debug!(
                                peer_id = ?peer_id,
                                event_session_id = session_id,
                                "Ignoring stale recovery state event",
                            );
                            continue;
                        }

                        {
                            let mut recovering = recovering_peers.write().await;
                            Self::remember_recovering_peer(
                                &mut recovering,
                                peer_id,
                                *session_id,
                                "peer state Disconnected/Failed",
                            );
                        }
                        closing_peers.write().await.insert(peer_id.clone());
                        tracing::debug!(
                            "Blocking new requests to peer {} (state: {:?})",
                            peer_id,
                            event
                        );
                    }

                    ConnectionEvent::IceRestartStarted {
                        peer_id,
                        session_id,
                    } => {
                        {
                            let mut recovering = recovering_peers.write().await;
                            Self::remember_recovering_peer(
                                &mut recovering,
                                peer_id,
                                *session_id,
                                "ice/network recovery started",
                            );
                        }
                        tracing::debug!("Peer {} entered ICE/network recovery", peer_id);
                    }

                    ConnectionEvent::StateChanged {
                        peer_id,
                        session_id,
                        state: ConnectionState::Connected,
                        ..
                    }
                    | ConnectionEvent::DataChannelOpened {
                        peer_id,
                        session_id,
                        payload_type: PayloadType::RpcReliable,
                        ..
                    }
                    | ConnectionEvent::IceRestartCompleted {
                        peer_id,
                        session_id,
                        success: true,
                        ..
                    } => {
                        let should_clear = {
                            let recovering = recovering_peers.read().await;
                            recovering
                                .get(peer_id)
                                .map(|status| status.session_id == *session_id)
                                .unwrap_or(true)
                        };

                        if should_clear {
                            recovering_peers.write().await.remove(peer_id);
                            closing_peers.write().await.remove(peer_id);
                            tracing::debug!("Peer {} is sendable again", peer_id);
                        } else {
                            tracing::debug!(
                                peer_id = ?peer_id,
                                event_session_id = session_id,
                                "Ignoring sendable event for stale session",
                            );
                        }
                    }

                    ConnectionEvent::IceRestartCompleted {
                        peer_id,
                        session_id,
                        success: false,
                        ..
                    } => {
                        let should_update = {
                            let recovering = recovering_peers.read().await;
                            recovering
                                .get(peer_id)
                                .map(|status| status.session_id == *session_id)
                                .unwrap_or(true)
                        };

                        if should_update {
                            let mut recovering = recovering_peers.write().await;
                            Self::remember_recovering_peer(
                                &mut recovering,
                                peer_id,
                                *session_id,
                                "ice restart failed",
                            );
                            closing_peers.write().await.insert(peer_id.clone());
                            if webrtc_coordinator
                                .is_active_session(peer_id, *session_id)
                                .await
                            {
                                Self::notify_active_data_streams_uncertain(
                                    &webrtc_coordinator,
                                    &active_data_streams,
                                    peer_id,
                                    *session_id,
                                    "ice restart failed",
                                )
                                .await;
                            }
                        }
                        tracing::debug!(
                            "Peer {} ICE restart failed; keeping sends blocked",
                            peer_id
                        );
                    }

                    ConnectionEvent::DataChannelClosed {
                        peer_id,
                        session_id,
                        payload_type: PayloadType::StreamReliable | PayloadType::StreamLatencyFirst,
                    } => {
                        Self::notify_active_data_streams_uncertain(
                            &webrtc_coordinator,
                            &active_data_streams,
                            peer_id,
                            *session_id,
                            "data channel closed",
                        )
                        .await;
                    }

                    // Clean pending requests and trigger downstream cleanup when connection is fully closed
                    ConnectionEvent::StateChanged {
                        peer_id,
                        state: ConnectionState::Closed,
                        session_id: event_session_id,
                        ..
                    }
                    | ConnectionEvent::ConnectionClosed {
                        peer_id,
                        session_id: event_session_id,
                    } => {
                        let dest = Dest::actor(peer_id.clone());

                        Self::notify_active_data_streams_uncertain(
                            &webrtc_coordinator,
                            &active_data_streams,
                            peer_id,
                            *event_session_id,
                            "connection closed",
                        )
                        .await;

                        {
                            let mut recovering = recovering_peers.write().await;
                            let should_remove = recovering
                                .get(peer_id)
                                .map(|status| status.session_id == *event_session_id)
                                .unwrap_or(false);
                            if should_remove {
                                recovering.remove(peer_id);
                                tracing::debug!(
                                    peer_id = ?peer_id,
                                    session_id = event_session_id,
                                    "Cleared recovery guard for closed peer",
                                );
                            }
                        }
                        webrtc_coordinator
                            .expire_peer_recovery(peer_id, *event_session_id, "connection closed")
                            .await;

                        // A close event may belong to a failed intermediate WebRTC attempt while
                        // the factory is still retrying. In that case the original RPC should keep
                        // waiting instead of being failed as "connection closed".
                        if transport_manager.is_connecting(&dest).await {
                            tracing::debug!(
                                "Ignoring transient close for peer {} while connection factory is still running",
                                peer_id
                            );
                            continue;
                        }

                        // Mark peer as closing (release lock immediately to avoid deadlock)
                        {
                            closing_peers.write().await.insert(peer_id.clone());
                        } // Lock released here

                        // 1. Session-guarded cleanup: only close the transport if the
                        //    active WebRTC wire still carries the same session_id.
                        //    If the identity mismatches (stale event from an old session
                        //    that has already been replaced), skip the close and do NOT
                        //    clean pending requests — they belong to the current wire.
                        match transport_manager
                            .close_transport_if_webrtc_session(&dest, peer_id, *event_session_id)
                            .await
                        {
                            Ok(true) => {
                                tracing::info!(
                                    "Successfully closed transport chain for peer {} (session {})",
                                    peer_id,
                                    event_session_id
                                );
                            }
                            Ok(false) => {
                                tracing::debug!(
                                    "Stale close event for peer {} (session {}), skipping cleanup",
                                    peer_id,
                                    event_session_id
                                );
                                closing_peers.write().await.remove(peer_id);
                                continue;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to close transport for peer {}: {}",
                                    peer_id,
                                    e
                                );
                            }
                        }

                        // 2. Clean pending requests for this peer
                        let mut pending = pending_requests.write().await;

                        // Collect request_ids that belong to this peer
                        let keys_to_remove: Vec<_> = pending
                            .iter()
                            .filter_map(|(req_id, (target, _))| {
                                if target == peer_id {
                                    Some(req_id.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        let cleaned_count = keys_to_remove.len();

                        tracing::info!(
                            "Cleaned {} pending requests for peer {}",
                            cleaned_count,
                            peer_id
                        );

                        // Remove and send error to all pending requests for this peer
                        for key in keys_to_remove {
                            if let Some((_, tx)) = pending.remove(&key) {
                                let _ = tx.send(Err(ActrError::Unavailable(
                                    "Connection closed".to_string(),
                                )));
                            }
                        }
                        drop(pending); // Release lock before calling downstream

                        closing_peers.write().await.remove(peer_id);
                    }

                    _ => {} // Ignore other events
                }
            }
        });
    }

    /// Handle response message (called by MessageDispatcher)
    ///
    /// # Arguments
    /// - `request_id`: Request ID
    /// - `result`: Response data (Ok) or error (Err)
    ///
    /// # Returns
    /// - `Ok(true)`: Successfully woke up waiting request
    /// - `Ok(false)`: No corresponding pending request found
    #[cfg(feature = "test-utils")]
    pub async fn handle_response(
        &self,
        request_id: &str,
        result: actr_protocol::ActorResult<Bytes>,
    ) -> ActorResult<bool> {
        let mut pending = self.pending_requests.write().await;

        if let Some((target, tx)) = pending.remove(request_id) {
            // Wake up waiting request with result (success or error)
            let _ = tx.send(result);
            tracing::debug!("Completed request: {} (target: {})", request_id, target);
            Ok(true)
        } else {
            tracing::warn!("No pending request for: {}", request_id);
            Ok(false)
        }
    }

    /// Get pending requests count (for monitoring)
    #[cfg(feature = "test-utils")]
    pub async fn pending_count(&self) -> usize {
        self.pending_requests.read().await.len()
    }

    /// Register a pending request without sending on the wire.
    #[cfg(feature = "test-utils")]
    pub async fn register_pending_for_test(
        &self,
        request_id: &str,
        target: ActrId,
    ) -> oneshot::Receiver<ActorResult<Bytes>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.pending_requests
            .write()
            .await
            .insert(request_id.to_string(), (target, response_tx));
        response_rx
    }

    /// Get pending_requests reference (for WebRtcGate to share)
    pub fn get_pending_requests(&self) -> PendingRequestsMap {
        self.pending_requests.clone()
    }

    /// Convert ActrId to Dest
    fn actr_id_to_dest(actor_id: &ActrId) -> Dest {
        Dest::actor(actor_id.clone())
    }

    /// Serialize RpcEnvelope to bytes
    fn serialize_envelope(envelope: &RpcEnvelope) -> Vec<u8> {
        envelope.encode_to_vec()
    }

    async fn clear_local_recovery_guard(&self, target: &ActrId, session_id: u64) {
        let mut recovering = self.recovering_peers.write().await;
        let should_remove = recovering
            .get(target)
            .map(|status| status.session_id == session_id)
            .unwrap_or(false);
        if should_remove {
            recovering.remove(target);
        }
        drop(recovering);
        self.closing_peers.write().await.remove(target);
    }

    async fn handle_recovery_timeout(
        &self,
        target: &ActrId,
        dest: &Dest,
        status: &NetworkRecoveryStatus,
        source: &str,
    ) -> ActrError {
        tracing::warn!(
            peer = ?target,
            session_id = status.session_id,
            elapsed_ms = status.elapsed_ms(),
            recovery_reason = status.reason.as_str(),
            source,
            "Connection recovery timed out; closing stale transport",
        );

        self.clear_local_recovery_guard(target, status.session_id)
            .await;

        if let Some(coordinator) = &self.webrtc_coordinator {
            coordinator
                .close_recovering_peer(target, status.session_id, "send preflight recovery timeout")
                .await;
        }

        if let Err(e) = self.transport_manager.close_transport(dest).await {
            tracing::warn!(
                peer = ?target,
                session_id = status.session_id,
                "Failed to close transport after recovery timeout: {}",
                e,
            );
        }

        Self::recovery_timeout_error(target, status)
    }

    #[cfg(feature = "test-utils")]
    pub async fn force_recovery_started_at_for_test(
        &self,
        target: &ActrId,
        started_at: Instant,
    ) -> bool {
        let mut recovering = self.recovering_peers.write().await;
        if let Some(status) = recovering.get_mut(target) {
            status.started_at = started_at;
            true
        } else {
            false
        }
    }

    async fn preflight_send(&self, target: &ActrId, dest: &Dest) -> ActorResult<()> {
        if let Some(coordinator) = &self.webrtc_coordinator {
            coordinator.wait_cleanup_complete().await;

            if let Some(status) = coordinator.peer_recovery_status(target).await {
                if status.is_timed_out() {
                    return Err(self
                        .handle_recovery_timeout(target, dest, &status, "coordinator")
                        .await);
                }
                return Err(Self::recovering_error(target, &status));
            }
        }

        let local_recovery = {
            let recovering = self.recovering_peers.read().await;
            recovering.get(target).cloned()
        };
        if let Some(status) = local_recovery {
            if status.is_timed_out() {
                return Err(self
                    .handle_recovery_timeout(target, dest, &status, "peer gate")
                    .await);
            }
            return Err(Self::recovering_error(target, &status));
        }

        if self.closing_peers.read().await.contains(target)
            || self.transport_manager.is_closing(dest).await
        {
            return Err(ActrError::Unavailable(format!(
                "Connection recovering: peer={:?}, reason=transport closing",
                target,
            )));
        }

        Ok(())
    }
}

impl PeerGate {
    /// Send data via transport with per-PayloadType retry on transient failures.
    ///
    /// Retry is applied only when `NetworkError::kind()` is `Transient`.
    /// Non-transient errors are returned immediately without retry.
    async fn send_with_retry(
        &self,
        dest: &Dest,
        payload_type: PayloadType,
        data: &[u8],
    ) -> ActorResult<()> {
        let policy = payload_type.retry_policy();
        self.send_with_retry_policy(dest, payload_type, data, policy)
            .await
    }

    async fn send_with_retry_policy(
        &self,
        dest: &Dest,
        payload_type: PayloadType,
        data: &[u8],
        policy: crate::transport::RetryPolicy,
    ) -> ActorResult<()> {
        let mut delay = policy.initial_delay;
        let mut attempt = 0u32;

        loop {
            match self.transport_manager.send(dest, payload_type, data).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    let retryable = e.is_retryable();
                    let remaining = policy.max_attempts.saturating_sub(attempt);
                    if retryable && remaining > 0 {
                        tracing::warn!(
                            attempt,
                            remaining,
                            error = %e,
                            "transient send failure, retrying after {:?}",
                            delay,
                        );
                        tokio::time::sleep(delay).await;
                        // exponential backoff capped at max_delay
                        delay = (delay * 2).min(policy.max_delay);
                    } else {
                        if attempt > 1 {
                            tracing::warn!(
                                attempt,
                                error = %e,
                                retryable,
                                "send failed after {} attempt(s)",
                                attempt,
                            );
                        }
                        return Err(e.into());
                    }
                }
            }
        }
    }

    #[cfg(feature = "test-utils")]
    pub async fn send_serialized_with_zero_retry_delay_for_test(
        &self,
        target: &ActrId,
        payload_type: PayloadType,
        data: &[u8],
    ) -> ActorResult<()> {
        let dest = Self::actr_id_to_dest(target);
        self.preflight_send(target, &dest).await?;

        let mut policy = payload_type.retry_policy();
        policy.initial_delay = std::time::Duration::ZERO;
        policy.max_delay = std::time::Duration::ZERO;
        self.send_with_retry_policy(&dest, payload_type, data, policy)
            .await
    }

    /// Send request and wait for response (with specified PayloadType).
    ///
    /// This is primarily used by language bindings / non-generic RPC paths.
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            skip_all,
            name = "PeerGate.send_request",
            fields(
                request_id = %envelope.request_id,
                payload_type = ?payload_type,
                target = %target
            )
        )
    )]
    pub async fn send_request_with_type(
        &self,
        target: &ActrId,
        payload_type: PayloadType,
        envelope: RpcEnvelope,
    ) -> ActorResult<Bytes> {
        // 1. Convert ActrId to Dest and fail fast during recovery before
        // registering pending_requests.
        let dest = Self::actr_id_to_dest(target);
        self.preflight_send(target, &dest).await?;

        // 2. Create oneshot channel for receiving response
        let (response_tx, response_rx) = oneshot::channel();

        // 3. Register pending request with target ActorId
        {
            let mut pending = self.pending_requests.write().await;
            pending.insert(envelope.request_id.clone(), (target.clone(), response_tx));
        }

        // 4. Serialize RpcEnvelope
        let data = Self::serialize_envelope(&envelope);

        // 5. Unified timeout: covers both retry + wait-for-response
        //    so the user-perceived latency never exceeds envelope.timeout_ms.
        let timeout = std::time::Duration::from_millis(envelope.timeout_ms as u64);
        let request_id = envelope.request_id.clone();

        let result = tokio::time::timeout(timeout, async {
            // 5a. Send with per-PayloadType retry on transient failures
            self.send_with_retry(&dest, payload_type, &data).await?;
            tracing::debug!("Sent request to {:?}", target);

            // 5b. Wait for response
            match response_rx.await {
                Ok(result) => result,
                Err(_) => Err(ActrError::Unavailable(
                    "Response channel closed".to_string(),
                )),
            }
        })
        .await;

        match result {
            Ok(inner) => {
                if inner.is_err() {
                    // Send failed or channel closed — clean up pending request
                    self.pending_requests.write().await.remove(&request_id);
                } else {
                    tracing::debug!("Received response for request: {}", request_id);
                }
                inner
            }
            Err(_) => {
                // Timeout — covers both send retry and response wait
                self.pending_requests.write().await.remove(&request_id);
                Err(ActrError::Unavailable(format!(
                    "Request timeout: {}ms",
                    envelope.timeout_ms
                )))
            }
        }
    }

    /// Send request and wait for response (bidirectional communication)
    #[cfg(feature = "test-utils")]
    pub async fn send_request(&self, target: &ActrId, envelope: RpcEnvelope) -> ActorResult<Bytes> {
        self.send_request_with_type(target, PayloadType::RpcReliable, envelope)
            .await
    }

    /// Send one-way message (no response expected)
    #[cfg(feature = "test-utils")]
    pub async fn send_message(&self, target: &ActrId, envelope: RpcEnvelope) -> ActorResult<()> {
        self.send_message_with_type(target, PayloadType::RpcReliable, envelope)
            .await
    }

    /// Send one-way message with specified PayloadType.
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            skip_all,
            name = "PeerGate.send_message",
            fields(
                payload_type = ?payload_type,
                target = %target
            )
        )
    )]
    pub async fn send_message_with_type(
        &self,
        target: &ActrId,
        payload_type: PayloadType,
        envelope: RpcEnvelope,
    ) -> ActorResult<()> {
        let data = Self::serialize_envelope(&envelope);
        let dest = Self::actr_id_to_dest(target);
        self.preflight_send(target, &dest).await?;
        self.send_with_retry(&dest, payload_type, &data).await
    }

    /// Send media sample via WebRTC native track
    ///
    /// # Parameters
    /// - `target`: Target Actor ID
    /// - `track_id`: Media track identifier
    /// - `sample`: Media sample data
    ///
    /// # Implementation Note
    /// Delegates to WebRtcCoordinator which manages WebRTC Tracks
    pub async fn send_media_sample(
        &self,
        target: &ActrId,
        track_id: &str,
        sample: MediaSample,
    ) -> ActorResult<()> {
        tracing::debug!(
            "PeerGate::send_media_sample to {:?}, track_id={}",
            target,
            track_id
        );

        // Check if WebRTC coordinator is available
        let coordinator = self.webrtc_coordinator.as_ref().ok_or_else(|| {
            ActrError::NotImplemented("MediaTrack requires WebRTC coordinator".to_string())
        })?;

        // Delegate to WebRtcCoordinator
        coordinator
            .send_media_sample(target, track_id, sample)
            .await
            .map_err(|e| ActrError::Unavailable(format!("WebRTC send failed: {e}")))?;

        tracing::debug!("Sent media sample to {:?}", target);
        Ok(())
    }

    /// Add a media track to the WebRTC connection with the target
    pub async fn add_media_track(
        &self,
        target: &ActrId,
        track_id: &str,
        codec: &str,
        media_type: &str,
    ) -> ActorResult<()> {
        tracing::debug!(
            "PeerGate::add_media_track to {:?}, track_id={}, codec={}, type={}",
            target,
            track_id,
            codec,
            media_type
        );

        let coordinator = self.webrtc_coordinator.as_ref().ok_or_else(|| {
            ActrError::NotImplemented("MediaTrack requires WebRTC coordinator".to_string())
        })?;

        coordinator
            .add_dynamic_track(target, track_id.to_string(), codec, media_type)
            .await?;

        Ok(())
    }

    /// Remove a media track from the WebRTC connection with the target.
    pub async fn remove_media_track(&self, target: &ActrId, track_id: &str) -> ActorResult<()> {
        tracing::debug!(
            "PeerGate::remove_media_track to {:?}, track_id={}",
            target,
            track_id
        );

        let coordinator = self.webrtc_coordinator.as_ref().ok_or_else(|| {
            ActrError::NotImplemented("MediaTrack requires WebRTC coordinator".to_string())
        })?;

        coordinator.remove_dynamic_track(target, track_id).await?;
        Ok(())
    }

    /// Send DataStream (Fast Path)
    ///
    /// # Parameters
    /// - `target`: Target Actor ID
    /// - `payload_type`: PayloadType (StreamReliable or StreamLatencyFirst)
    /// - `stream_id`: DataStream identifier already known before serialization
    /// - `data`: Serialized DataStream bytes
    ///
    /// # Implementation Note
    /// Sends via PeerTransport using WebRTC DataChannel or WebSocket
    pub async fn send_data_stream(
        &self,
        target: &ActrId,
        payload_type: PayloadType,
        stream_id: &str,
        data: Bytes,
    ) -> ActorResult<()> {
        tracing::debug!(
            "PeerGate::send_data_stream to {:?}, stream_id={}, payload_type={:?}, size={} bytes",
            target,
            stream_id,
            payload_type,
            data.len()
        );

        // // Check if target is being cleaned up
        // if self.closing_peers.read().await.contains(target) {
        //     return Err(ActrError::Unavailable(format!(
        //         "Connection to {} is closing",
        //         target.to_string_repr()
        //     )));
        // }

        // Convert ActrId to Dest
        let dest = Self::actr_id_to_dest(target);
        self.preflight_send(target, &dest).await?;

        let tracks_data_stream = matches!(
            payload_type,
            PayloadType::StreamReliable | PayloadType::StreamLatencyFirst
        );

        let stream_session_id = if tracks_data_stream {
            if let Some(coordinator) = &self.webrtc_coordinator {
                coordinator.get_peer_session_id(target).await
            } else {
                None
            }
        } else {
            None
        };

        let recorded_before_send = if let Some(session_id) = stream_session_id {
            self.record_active_data_stream_if_needed(target, stream_id, session_id, Instant::now())
                .await
        } else {
            false
        };

        let result = match tokio::time::timeout(
            DATA_STREAM_SEND_TIMEOUT,
            self.transport_manager.send(&dest, payload_type, &data),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(ActrError::Unavailable(e.to_string())),
            Err(_) => Err(ActrError::Unavailable(format!(
                "DataStream send timeout: {}ms",
                DATA_STREAM_SEND_TIMEOUT.as_millis()
            ))),
        };

        if tracks_data_stream {
            if result.is_err() {
                if recorded_before_send && let Some(session_id) = stream_session_id {
                    self.active_data_streams
                        .write()
                        .await
                        .remove_stream_session(target, stream_id, session_id);
                }
            } else if stream_session_id.is_none()
                && let Some(coordinator) = &self.webrtc_coordinator
            {
                if let Some(session_id) = coordinator.get_peer_session_id(target).await {
                    self.record_active_data_stream_if_needed(
                        target,
                        stream_id,
                        session_id,
                        Instant::now(),
                    )
                    .await;
                }
            }
        }

        result
    }
}

impl Drop for PeerGate {
    fn drop(&mut self) {
        tracing::debug!("PeerGate dropped");
    }
}
