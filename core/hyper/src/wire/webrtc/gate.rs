//! WebRtcGate - WebRTC-based OutboundGate implementation
//!
//! Uses WebRtcCoordinator to send/receive messages, implementing cross-process RPC communication

use super::coordinator::WebRtcCoordinator;
use crate::inbound::DataStreamRegistry;
#[cfg(feature = "opentelemetry")]
use crate::wire::webrtc::trace::set_parent_from_rpc_envelope;
use actr_framework::Bytes;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{self, ActrId, DataStream, Direction, PayloadType, RpcEnvelope};
use actr_protocol::{ActorResult, ActrError};
use actr_runtime_mailbox::{Mailbox, MessagePriority};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};

/// Pending requests map type: request_id → (target_actor_id, oneshot response sender)
type PendingRequestsMap =
    Arc<RwLock<HashMap<String, (ActrId, oneshot::Sender<actr_protocol::ActorResult<Bytes>>)>>>;
#[cfg(feature = "opentelemetry")]
use tracing::Instrument as _;

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

/// WebRTC Gate - OutboundGate implementation
///
/// # Responsibilities
/// - Implement OutboundGate trait
/// - Send messages using WebRtcCoordinator
/// - Serialize/deserialize RpcEnvelope (Protobuf)
/// - Track pending requests and match responses (by r  equest_id)
/// - Route messages by PayloadType (RPC → Mailbox, DataStream → Registry)
///
/// # Design Principles
/// - Response reuses Request's request_id (standard RPC semantics)
/// - Use pending_requests to distinguish: exists = Response, doesn't exist = Request
/// - Gateway layer doesn't deserialize payloads, raw bytes go directly to Mailbox
/// - **IMPORTANT**: pending_requests should be shared with PeerGate
pub(crate) struct WebRtcGate {
    /// Local Actor ID
    local_id: Arc<RwLock<Option<ActrId>>>,

    /// WebRTC signaling coordinator
    coordinator: Arc<WebRtcCoordinator>,

    /// Pending requests (request_id → (target_actor_id, response channel))
    /// Used to determine if received message is Response (key exists) or Request (key doesn't exist)
    /// **Shared with PeerGate** to ensure correct Response routing
    /// Can send success (Ok(Bytes)) or error (Err(ProtocolError))
    pending_requests: PendingRequestsMap,

    /// DataStream registry for fast-path message routing
    data_stream_registry: Arc<DataStreamRegistry>,
}

impl WebRtcGate {
    /// Create new WebRtcGate with shared pending_requests and DataStreamRegistry
    ///
    /// # Arguments
    /// - `coordinator`: WebRtcCoordinator instance
    /// - `pending_requests`: Shared pending requests (should be same as PeerGate)
    /// - `data_stream_registry`: DataStream registry for fast-path routing
    pub fn new(
        coordinator: Arc<WebRtcCoordinator>,
        pending_requests: PendingRequestsMap,
        data_stream_registry: Arc<DataStreamRegistry>,
    ) -> Self {
        Self {
            local_id: Arc::new(RwLock::new(None)),
            coordinator,
            pending_requests,
            data_stream_registry,
        }
    }

    /// Set local Actor ID
    pub async fn set_local_id(&self, actor_id: ActrId) {
        *self.local_id.write().await = Some(actor_id);
    }

    /// Handle RpcEnvelope message (Response or Request)
    ///
    /// # Arguments
    /// - `envelope`: Deserialized RpcEnvelope
    /// - `from_bytes`: Sender's ActrId bytes (for Mailbox enqueue)
    /// - `data`: Original message bytes (for Mailbox enqueue)
    /// - `payload_type`: PayloadType to determine priority
    /// - `pending_requests`: Shared pending requests map
    /// - `mailbox`: Mailbox for enqueueing requests
    ///
    /// # Behavior
    /// Routes strictly on the sender-set `direction` field:
    /// - `Request`: enqueue to Mailbox, skipping the pending lookup.
    /// - `Response`: wake the waiting caller if a pending entry exists;
    ///   otherwise drop as an orphan late response — never enqueue (fix for #255).
    /// - missing, `Unspecified`, or unknown: warn and drop. There is no
    ///   pending-map inference fallback in the current wire protocol.
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
            // Explicit request — enqueue directly, skip pending lookup.
            Self::enqueue_request(from_bytes, data, payload_type, mailbox, &request_id).await;
            return;
        }

        // Direction::Response: consult pending map.
        let mut pending = pending_requests.write().await;
        if let Some((target, response_tx)) = pending.remove(&request_id) {
            // Response - Wake up waiting caller (bypassing disk, fast path)
            drop(pending); // Release lock
            tracing::debug!(
                "📬 Received RPC Response: request_id={}, target={}",
                request_id,
                target
            );

            // Convert envelope to result
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
            drop(pending); // Release lock
            // Orphan/late response: pending entry was removed (caller timed
            // out). Drop instead of enqueueing as a new request.
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

    /// Enqueue an explicit inbound RPC request to the Mailbox with priority
    /// derived from the inbound `PayloadType`.
    async fn enqueue_request(
        from_bytes: Vec<u8>,
        data: Bytes,
        payload_type: PayloadType,
        mailbox: Arc<dyn Mailbox>,
        request_id: &str,
    ) {
        tracing::debug!("📥 Received RPC Request: request_id={}", request_id);

        // Determine priority based on PayloadType
        let priority = match payload_type {
            PayloadType::RpcSignal => MessagePriority::High,
            PayloadType::RpcReliable => MessagePriority::Normal,
            _ => MessagePriority::Normal,
        };

        tracing::info!(request_id = %request_id, "rpc.mailbox.enqueue");
        // Enqueue to Mailbox (from_bytes and data are original bytes, zero overhead)
        // Convert Bytes to Vec<u8> (Mailbox uses Vec)
        match mailbox.enqueue(from_bytes, data.to_vec(), priority).await {
            Ok(msg_id) => {
                tracing::debug!(
                    "✅ RPC message enqueued to Mailbox: msg_id={}, priority={:?}",
                    msg_id,
                    priority
                );
            }
            Err(e) => {
                tracing::error!("❌ Mailbox enqueue failed: {:?}", e);
            }
        }
    }

    /// Start message receive loop (called by the runtime node)
    ///
    /// # Arguments
    /// - `mailbox`: message queue for persisting inbound requests
    ///
    /// # Architecture
    /// According to three-loop architecture design (framework-runtime-architecture.zh.md):
    /// - WebRtcGate belongs to outer loop (Transport layer)
    /// - Mailbox belongs to inner loop (state path)
    /// - Message flow: WebRTC → WebRtcGate → Mailbox/DataStreamRegistry → Scheduler → ActrNode
    ///
    /// # Message Routing Logic
    /// - Route based on PayloadType:
    ///   - RpcReliable/RpcSignal: Deserialize RpcEnvelope, check pending_requests, enqueue to Mailbox
    ///   - StreamReliable/StreamLatencyFirst: Deserialize DataStream, dispatch to DataStreamRegistry
    pub async fn start_receive_loop(&self, mailbox: Arc<dyn Mailbox>) -> ActorResult<()> {
        let coordinator = self.coordinator.clone();
        let pending_requests = self.pending_requests.clone();
        let data_stream_registry = self.data_stream_registry.clone();
        #[cfg(feature = "opentelemetry")]
        let local_id = self.local_id.clone();

        tokio::spawn(async move {
            loop {
                // Receive message from WebRtcCoordinator (now includes PayloadType)
                match coordinator.receive_message().await {
                    Ok(Some((from_bytes, data, payload_type))) => {
                        tracing::debug!(
                            "📨 WebRtcGate received message: {} bytes, PayloadType: {:?}",
                            data.len(),
                            payload_type
                        );

                        // Route based on PayloadType
                        match payload_type {
                            PayloadType::RpcReliable | PayloadType::RpcSignal => {
                                // RPC path: deserialize RpcEnvelope and route
                                match RpcEnvelope::decode(&data[..]) {
                                    Ok(envelope) => {
                                        #[cfg(feature = "opentelemetry")]
                                        let current_local_id = local_id.read().await.clone();
                                        #[cfg(feature = "opentelemetry")]
                                        let span = {
                                            let actr_id_str = current_local_id
                                                .as_ref()
                                                .map(|id| id.to_string())
                                                .unwrap_or_default();
                                            let span = tracing::info_span!("WebRtcGate.receive_rpc", actr_id = %actr_id_str);
                                            set_parent_from_rpc_envelope(&span, &envelope);
                                            span
                                        };
                                        let handle_envelope_fut = Self::handle_envelope(
                                            envelope,
                                            from_bytes,
                                            data,
                                            payload_type,
                                            pending_requests.clone(),
                                            mailbox.clone(),
                                        );
                                        #[cfg(feature = "opentelemetry")]
                                        let handle_envelope_fut =
                                            handle_envelope_fut.instrument(span);

                                        handle_envelope_fut.await;
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "❌ Failed to deserialize RpcEnvelope: {:?}",
                                            e
                                        );
                                    }
                                }
                            }
                            PayloadType::StreamReliable | PayloadType::StreamLatencyFirst => {
                                // DataStream path: deserialize and dispatch to registry
                                match DataStream::decode(&data[..]) {
                                    Ok(chunk) => {
                                        tracing::debug!(
                                            "📦 Received DataStream: stream_id={}, seq={}, {} bytes",
                                            chunk.stream_id,
                                            chunk.sequence,
                                            chunk.payload.len()
                                        );

                                        // Decode sender ActrId
                                        match ActrId::decode(&from_bytes[..]) {
                                            Ok(sender_id) => {
                                                // Dispatch to DataStreamRegistry (async callback invocation)
                                                data_stream_registry
                                                    .dispatch(chunk, sender_id)
                                                    .await;
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    "❌ Failed to decode sender ActrId: {:?}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "❌ Failed to deserialize DataStream: {:?}",
                                            e
                                        );
                                    }
                                }
                            }
                            PayloadType::MediaRtp => {
                                tracing::warn!(
                                    "⚠️ MediaRtp received in WebRtcGate (should use RTCTrackRemote)"
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        tracing::error!("❌ Message receive failed: {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Send response (called by Mailbox handler loop)
    ///
    /// # Arguments
    /// - `target`: response target ActrId (original request sender)
    /// - `response_envelope`: response RpcEnvelope (**must reuse original request_id**)
    ///
    /// # Design Principle
    /// - Response reuses Request's request_id (caller is responsible)
    /// - Receiver matches to pending_requests by request_id and wakes up waiting caller
    #[cfg_attr(feature = "opentelemetry", tracing::instrument(
        skip_all,
        name = "WebRtcGate.send_response",
        fields(actr_id = tracing::field::Empty)
    ))]
    pub async fn send_response(
        &self,
        target: &ActrId,
        mut response_envelope: RpcEnvelope,
    ) -> ActorResult<()> {
        // Fill actr_id span field at runtime
        #[cfg(feature = "opentelemetry")]
        {
            let local_id = self.local_id.read().await;
            if let Some(ref id) = *local_id {
                tracing::Span::current().record("actr_id", tracing::field::display(id));
            }
        }
        response_envelope.direction = Some(Direction::Response as i32);

        // Serialize RpcEnvelope (Protobuf)
        let mut buf = Vec::new();
        response_envelope
            .encode(&mut buf)
            .map_err(|e| ActrError::Internal(format!("Failed to encode response: {e}")))?;

        // Send
        self.coordinator.send_message(target, &buf).await?;
        tracing::debug!(
            "📤 Sent response: request_id={}, {} bytes",
            response_envelope.request_id,
            buf.len()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::{ActrType, Realm};
    use actr_runtime_mailbox::{MailboxStats, MessageRecord, StorageResult};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    struct CapturingMailbox {
        enqueue_count: AtomicUsize,
        last_priority: Mutex<Option<MessagePriority>>,
    }

    impl CapturingMailbox {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                enqueue_count: AtomicUsize::new(0),
                last_priority: Mutex::new(None),
            })
        }
    }

    #[async_trait]
    impl Mailbox for CapturingMailbox {
        async fn enqueue(
            &self,
            _from: Vec<u8>,
            _payload: Vec<u8>,
            priority: MessagePriority,
        ) -> StorageResult<Uuid> {
            self.enqueue_count.fetch_add(1, Ordering::SeqCst);
            *self.last_priority.lock().unwrap() = Some(priority);
            Ok(Uuid::new_v4())
        }

        async fn dequeue(&self) -> StorageResult<Vec<MessageRecord>> {
            Ok(vec![])
        }

        async fn ack(&self, _: Uuid) -> StorageResult<()> {
            Ok(())
        }

        async fn status(&self) -> StorageResult<MailboxStats> {
            Ok(MailboxStats {
                queued_messages: 0,
                inflight_messages: 0,
                queued_by_priority: Default::default(),
            })
        }
    }

    fn test_actor_id(serial: u64) -> ActrId {
        ActrId {
            realm: Realm { realm_id: 1 },
            serial_number: serial,
            r#type: ActrType {
                manufacturer: "test".to_string(),
                name: "node".to_string(),
                version: "1.0.0".to_string(),
            },
        }
    }

    fn make_rpc_envelope(request_id: &str) -> RpcEnvelope {
        RpcEnvelope {
            request_id: request_id.to_string(),
            route_key: "test".to_string(),
            payload: Some(Bytes::from("hello")),
            error: None,
            direction: Some(Direction::Request as i32),
            timeout_ms: 5000,
            ..Default::default()
        }
    }

    fn empty_pending() -> PendingRequestsMap {
        Arc::new(RwLock::new(HashMap::new()))
    }

    #[tokio::test]
    async fn handle_envelope_missing_direction_is_dropped_not_enqueued() {
        let mailbox = CapturingMailbox::new();
        let pending = empty_pending();

        let mut envelope = make_rpc_envelope("missing-direction");
        envelope.direction = None;
        let data = actr_protocol::prost::Message::encode_to_vec(&envelope);

        WebRtcGate::handle_envelope(
            envelope,
            vec![],
            Bytes::from(data),
            PayloadType::RpcReliable,
            pending,
            mailbox.clone(),
        )
        .await;

        assert_eq!(
            mailbox.enqueue_count.load(Ordering::SeqCst),
            0,
            "missing WebRTC direction must be dropped"
        );
    }

    #[tokio::test]
    async fn handle_envelope_unspecified_direction_is_dropped_not_enqueued() {
        let mailbox = CapturingMailbox::new();
        let pending = empty_pending();

        let mut envelope = make_rpc_envelope("unspecified-direction");
        envelope.direction = Some(Direction::Unspecified as i32);
        let data = actr_protocol::prost::Message::encode_to_vec(&envelope);

        WebRtcGate::handle_envelope(
            envelope,
            vec![],
            Bytes::from(data),
            PayloadType::RpcReliable,
            pending,
            mailbox.clone(),
        )
        .await;

        assert_eq!(
            mailbox.enqueue_count.load(Ordering::SeqCst),
            0,
            "Unspecified WebRTC direction must be dropped"
        );
    }

    #[tokio::test]
    async fn handle_envelope_unknown_direction_is_dropped_not_enqueued() {
        let mailbox = CapturingMailbox::new();
        let pending = empty_pending();

        let mut envelope = make_rpc_envelope("unknown-direction");
        envelope.direction = Some(99);
        let data = actr_protocol::prost::Message::encode_to_vec(&envelope);

        WebRtcGate::handle_envelope(
            envelope,
            vec![],
            Bytes::from(data),
            PayloadType::RpcReliable,
            pending,
            mailbox.clone(),
        )
        .await;

        assert_eq!(
            mailbox.enqueue_count.load(Ordering::SeqCst),
            0,
            "unknown WebRTC direction must be dropped"
        );
    }

    /// An explicit WebRTC Response whose pending entry was already removed
    /// must be dropped instead of being re-enqueued as a request.
    #[tokio::test]
    async fn handle_envelope_explicit_response_with_no_pending_is_dropped_not_enqueued() {
        let mailbox = CapturingMailbox::new();
        let pending = empty_pending();

        let mut envelope = make_rpc_envelope("late-webrtc-1");
        envelope.direction = Some(Direction::Response as i32);
        let data = actr_protocol::prost::Message::encode_to_vec(&envelope);
        let from = actr_protocol::prost::Message::encode_to_vec(&test_actor_id(42));

        WebRtcGate::handle_envelope(
            envelope,
            from,
            Bytes::from(data),
            PayloadType::RpcReliable,
            pending,
            mailbox.clone(),
        )
        .await;

        assert_eq!(
            mailbox.enqueue_count.load(Ordering::SeqCst),
            0,
            "orphan late WebRTC response must not be enqueued as a new request"
        );
    }

    /// An explicit WebRTC Request must be enqueued even if a pending entry with
    /// the same request_id exists; direction wins over pending-map inference.
    #[tokio::test]
    async fn handle_envelope_explicit_request_always_enqueues() {
        let mailbox = CapturingMailbox::new();
        let pending = empty_pending();
        let (tx, _rx) = oneshot::channel();
        pending
            .write()
            .await
            .insert("req-stale".to_string(), (test_actor_id(9), tx));

        let mut envelope = make_rpc_envelope("req-stale");
        envelope.direction = Some(Direction::Request as i32);
        let data = actr_protocol::prost::Message::encode_to_vec(&envelope);

        WebRtcGate::handle_envelope(
            envelope,
            vec![],
            Bytes::from(data),
            PayloadType::RpcReliable,
            pending.clone(),
            mailbox.clone(),
        )
        .await;

        assert_eq!(
            mailbox.enqueue_count.load(Ordering::SeqCst),
            1,
            "explicit WebRTC Request must be enqueued"
        );
        assert!(
            pending.read().await.contains_key("req-stale"),
            "explicit WebRTC Request must not consume the pending entry"
        );
    }

    /// An explicit WebRTC Response with a matching pending entry should still
    /// wake the caller and avoid the mailbox.
    #[tokio::test]
    async fn handle_envelope_explicit_response_with_pending_wakes_caller() {
        let mailbox = CapturingMailbox::new();
        let pending = empty_pending();
        let (tx, rx) = oneshot::channel();
        pending
            .write()
            .await
            .insert("req-resp".to_string(), (test_actor_id(7), tx));

        let mut envelope = make_rpc_envelope("req-resp");
        envelope.payload = Some(Bytes::from("resp"));
        envelope.direction = Some(Direction::Response as i32);
        let data = actr_protocol::prost::Message::encode_to_vec(&envelope);

        WebRtcGate::handle_envelope(
            envelope,
            vec![],
            Bytes::from(data),
            PayloadType::RpcReliable,
            pending,
            mailbox.clone(),
        )
        .await;

        assert_eq!(mailbox.enqueue_count.load(Ordering::SeqCst), 0);
        let result = rx
            .await
            .expect("oneshot must be resolved for explicit WebRTC Response");
        assert!(result.is_ok());
    }
}
