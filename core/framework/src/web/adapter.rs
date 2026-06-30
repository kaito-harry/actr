//! Bridge from `framework::Workload` to `actr_web_abi::host::Workload`.
//!
//! On the web (`wasm32-unknown-unknown` + `feature = "web"`) target the
//! `actr_web_abi::host` module defines its own narrow `Workload` trait — the
//! 17-method contract the service-worker host calls into. User workloads are
//! expressed against the cross-target `framework::Workload` trait instead,
//! so the browser entry point needs a thin adapter that maps one onto the
//! other.
//!
//! `WebWorkloadAdapter<W>` is that adapter. For each inbound call it:
//!
//! 1. Builds a fresh [`WebContext`] bound to the envelope's `request_id`
//!    (or the lifecycle placeholder for non-dispatch hooks).
//! 2. Delegates to the user workload:
//!    - `dispatch` → `<W::Dispatcher as MessageDispatcher>::dispatch`
//!    - lifecycle hooks → the matching `Workload::on_*` method
//! 3. Lowers the result back into the WIT-lowered shapes the web ABI
//!    host trait expects.
//!
//! The adapter is constructed and registered in a single place —
//! `actr_framework::entry!` macro's `cfg(feature = "web")` branch — so user
//! code never names it directly. Keeping the impl here (rather than inside
//! the macro expansion) keeps the translation logic reviewable and unit-
//! testable.

use std::time::{Duration, UNIX_EPOCH};

use actr_protocol::{
    ActrError, ActrId, ConnectionNotReadyInfo, DataStream, MetadataEntry, Realm, RpcEnvelope,
};
use async_trait::async_trait;
use bytes::Bytes;

use crate::web::context::WebContext;
use crate::workload::{
    BackpressureEvent, CredentialEvent, ErrorCategory, ErrorEvent, PeerEvent, WebRtcPeerStatus,
};
use crate::{MessageDispatcher, Workload};

use actr_web_abi::host as web_host;
use actr_web_abi::types as wit;

// ── WIT ⇄ framework event lowering ─────────────────────────────────────
//
// Kept next to the adapter (not in `web::context`) because these events
// only ever appear at the host-import boundary; the context module deals
// with per-call context plumbing, the adapter deals with per-call event
// translation.

fn actr_type_from_wit(t: &wit::ActrType) -> actr_protocol::ActrType {
    actr_protocol::ActrType {
        manufacturer: t.manufacturer.clone(),
        name: t.name.clone(),
        version: t.version.clone(),
    }
}

fn actr_id_from_wit(id: &wit::ActrId) -> ActrId {
    ActrId {
        realm: Realm {
            realm_id: id.realm.realm_id,
        },
        serial_number: id.serial_number,
        r#type: actr_type_from_wit(&id.actr_type),
    }
}

fn actr_type_to_wit(t: &actr_protocol::ActrType) -> wit::ActrType {
    wit::ActrType {
        manufacturer: t.manufacturer.clone(),
        name: t.name.clone(),
        version: t.version.clone(),
    }
}

fn actr_id_to_wit(id: &ActrId) -> wit::ActrId {
    wit::ActrId {
        realm: wit::Realm {
            realm_id: id.realm.realm_id,
        },
        serial_number: id.serial_number,
        actr_type: actr_type_to_wit(&id.r#type),
    }
}

fn timestamp_from_wit(t: wit::Timestamp) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::new(t.seconds, t.nanoseconds)
}

fn peer_event_from_wit(e: wit::PeerEvent) -> PeerEvent {
    PeerEvent {
        peer: actr_id_from_wit(&e.peer),
        relayed: e.relayed,
        status: e.status.map(webrtc_peer_status_from_wit),
    }
}

fn webrtc_peer_status_from_wit(s: wit::WebrtcPeerStatus) -> WebRtcPeerStatus {
    match s {
        wit::WebrtcPeerStatus::Idle => WebRtcPeerStatus::Idle,
        wit::WebrtcPeerStatus::Connecting => WebRtcPeerStatus::Connecting,
        wit::WebrtcPeerStatus::Connected => WebRtcPeerStatus::Connected,
        wit::WebrtcPeerStatus::Recovering => WebRtcPeerStatus::Recovering,
    }
}

fn error_category_from_wit(c: wit::ErrorCategory) -> ErrorCategory {
    match c {
        wit::ErrorCategory::HandlerPanic => ErrorCategory::HandlerPanic,
        wit::ErrorCategory::HandlerError => ErrorCategory::HandlerError,
        wit::ErrorCategory::SignalingFailure => ErrorCategory::SignalingFailure,
        wit::ErrorCategory::TransportFailure => ErrorCategory::TransportFailure,
        wit::ErrorCategory::DataStreamDeliveryUncertain => {
            ErrorCategory::DataStreamDeliveryUncertain
        }
    }
}

fn wit_error_to_proto(e: wit::ActrError) -> ActrError {
    match e {
        wit::ActrError::Unavailable(m) => ActrError::Unavailable(m),
        wit::ActrError::ConnectionNotReady(info) => {
            ActrError::ConnectionNotReady(wit_connection_not_ready_info_to_proto(info))
        }
        wit::ActrError::TimedOut => ActrError::TimedOut,
        wit::ActrError::NotFound(m) => ActrError::NotFound(m),
        wit::ActrError::PermissionDenied(m) => ActrError::PermissionDenied(m),
        wit::ActrError::InvalidArgument(m) => ActrError::InvalidArgument(m),
        wit::ActrError::UnknownRoute(m) => ActrError::UnknownRoute(m),
        wit::ActrError::DependencyNotFound(p) => ActrError::DependencyNotFound {
            service_name: p.service_name,
            message: p.message,
        },
        wit::ActrError::DecodeFailure(m) => ActrError::DecodeFailure(m),
        wit::ActrError::NotImplemented(m) => ActrError::NotImplemented(m),
        wit::ActrError::Internal(m) => ActrError::Internal(m),
    }
}

fn proto_error_to_wit(e: ActrError) -> wit::ActrError {
    match e {
        ActrError::Unavailable(m) => wit::ActrError::Unavailable(m),
        ActrError::ConnectionNotReady(info) => {
            wit::ActrError::ConnectionNotReady(proto_connection_not_ready_info_to_wit(info))
        }
        ActrError::TimedOut => wit::ActrError::TimedOut,
        ActrError::NotFound(m) => wit::ActrError::NotFound(m),
        ActrError::PermissionDenied(m) => wit::ActrError::PermissionDenied(m),
        ActrError::InvalidArgument(m) => wit::ActrError::InvalidArgument(m),
        ActrError::UnknownRoute(m) => wit::ActrError::UnknownRoute(m),
        ActrError::DependencyNotFound {
            service_name,
            message,
        } => wit::ActrError::DependencyNotFound(wit::DependencyNotFoundPayload {
            service_name,
            message,
        }),
        ActrError::DecodeFailure(m) => wit::ActrError::DecodeFailure(m),
        ActrError::NotImplemented(m) => wit::ActrError::NotImplemented(m),
        ActrError::Internal(m) => wit::ActrError::Internal(m),
    }
}

fn wit_connection_not_ready_info_to_proto(
    info: wit::ConnectionNotReadyInfo,
) -> ConnectionNotReadyInfo {
    ConnectionNotReadyInfo {
        retry_after_ms: info.retry_after_ms,
    }
}

fn proto_connection_not_ready_info_to_wit(
    info: ConnectionNotReadyInfo,
) -> wit::ConnectionNotReadyInfo {
    wit::ConnectionNotReadyInfo {
        retry_after_ms: info.retry_after_ms,
    }
}

fn error_event_from_wit(e: wit::ErrorEvent) -> ErrorEvent {
    ErrorEvent {
        source: wit_error_to_proto(e.source),
        category: error_category_from_wit(e.category),
        context: e.context,
        timestamp: timestamp_from_wit(e.timestamp),
    }
}

fn credential_event_from_wit(e: wit::CredentialEvent) -> CredentialEvent {
    CredentialEvent {
        new_expiry: timestamp_from_wit(e.new_expiry),
    }
}

fn backpressure_event_from_wit(e: wit::BackpressureEvent) -> BackpressureEvent {
    BackpressureEvent {
        queue_len: e.queue_len as usize,
        threshold: e.threshold as usize,
    }
}

fn data_stream_from_wit(chunk: wit::DataStream) -> DataStream {
    DataStream {
        stream_id: chunk.stream_id,
        sequence: chunk.sequence,
        payload: chunk.payload.into(),
        metadata: chunk
            .metadata
            .into_iter()
            .map(|entry| MetadataEntry {
                key: entry.key,
                value: entry.value,
            })
            .collect(),
        timestamp_ms: chunk.timestamp_ms,
    }
}

/// Convert the web-ABI flat envelope into the protocol envelope the
/// [`MessageDispatcher`] consumes. The inner `payload` stays empty when
/// the host sent a zero-byte payload so downstream decoders see the
/// same `Option::None` shape native / wasip2 guests do.
fn envelope_from_wit(envelope: wit::RpcEnvelope) -> RpcEnvelope {
    RpcEnvelope {
        request_id: envelope.request_id,
        route_key: envelope.route_key,
        payload: if envelope.payload.is_empty() {
            None
        } else {
            Some(Bytes::from(envelope.payload))
        },
        ..Default::default()
    }
}

// ── Adapter ────────────────────────────────────────────────────────────

/// Bridge between [`framework::Workload`][crate::Workload] and the
/// narrower [`actr_web_abi::host::Workload`] trait.
///
/// One instance is created per wasm module during `entry!` bootstrap
/// (the adapter is cheap to clone — it just wraps the user workload),
/// handed to [`actr_web_abi::host::register_workload`], then leaked to
/// `'static` by the web ABI crate. Every host-exported entry point
/// resolves through that singleton back into the user's workload.
pub struct WebWorkloadAdapter<W> {
    inner: W,
}

impl<W> WebWorkloadAdapter<W> {
    /// Wrap a freshly constructed workload instance.
    ///
    /// The adapter does not take ownership of the workload beyond what
    /// the web ABI's `register_workload` needs: it consumes the value
    /// and leaks it, so cloning the adapter once on bootstrap is enough.
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

// The adapter is cheap to clone when the inner workload is; `Clone` is
// a hard bound of `actr_web_abi::host::register_workload`, so derive it
// from the wrapped type rather than requiring `W` to implement `Clone`
// at the struct-definition site (users get a clearer error at the
// `entry!` expansion point if their workload forgets `Clone`).
impl<W: Clone> Clone for WebWorkloadAdapter<W> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait(?Send)]
impl<W> web_host::Workload for WebWorkloadAdapter<W>
where
    W: Workload + Clone + 'static,
{
    async fn dispatch(&self, envelope: wit::RpcEnvelope) -> Result<Vec<u8>, wit::ActrError> {
        // The per-dispatch WebContext binds `request_id` at construction
        // time (γ-unified §3.3); outbound host imports issued from the
        // handler will thread this id through `DISPATCH_CTXS`.
        let ctx = WebContext::new(ActrId::default(), None, envelope.request_id.clone());
        let envelope = envelope_from_wit(envelope);
        let result = <<W as Workload>::Dispatcher as MessageDispatcher>::dispatch(
            &self.inner,
            envelope,
            &ctx,
        )
        .await;
        match result {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(e) => Err(proto_error_to_wit(e)),
        }
    }

    // ── Lifecycle (4, fallible) ────────────────────────────────────────

    async fn on_start(&self) -> Result<(), wit::ActrError> {
        let ctx = WebContext::for_lifecycle();
        self.inner.on_start(&ctx).await.map_err(proto_error_to_wit)
    }

    async fn on_ready(&self) -> Result<(), wit::ActrError> {
        let ctx = WebContext::for_lifecycle();
        self.inner.on_ready(&ctx).await.map_err(proto_error_to_wit)
    }

    async fn on_stop(&self) -> Result<(), wit::ActrError> {
        let ctx = WebContext::for_lifecycle();
        self.inner.on_stop(&ctx).await.map_err(proto_error_to_wit)
    }

    async fn on_error(&self, event: wit::ErrorEvent) -> Result<(), wit::ActrError> {
        let ctx = WebContext::for_lifecycle();
        let event = error_event_from_wit(event);
        self.inner
            .on_error(&ctx, &event)
            .await
            .map_err(proto_error_to_wit)
    }

    // ── Signaling (3, infallible) ──────────────────────────────────────

    async fn on_signaling_connecting(&self) {
        let ctx = WebContext::for_lifecycle();
        // Mirrors the native adapter: the hosted environment always has
        // an identity available by the time these fire post-handshake,
        // so passing `Some(&ctx)` matches the reconnect path. Initial-
        // connection `None` is a host-driven event never surfaced here.
        self.inner.on_signaling_connecting(Some(&ctx)).await;
    }

    async fn on_signaling_connected(&self) {
        let ctx = WebContext::for_lifecycle();
        self.inner.on_signaling_connected(Some(&ctx)).await;
    }

    async fn on_signaling_disconnected(&self) {
        let ctx = WebContext::for_lifecycle();
        self.inner.on_signaling_disconnected(&ctx).await;
    }

    // ── WebSocket (3, infallible) ──────────────────────────────────────

    async fn on_websocket_connecting(&self, event: wit::PeerEvent) {
        let ctx = WebContext::for_lifecycle();
        let event = peer_event_from_wit(event);
        self.inner.on_websocket_connecting(&ctx, &event).await;
    }

    async fn on_websocket_connected(&self, event: wit::PeerEvent) {
        let ctx = WebContext::for_lifecycle();
        let event = peer_event_from_wit(event);
        self.inner.on_websocket_connected(&ctx, &event).await;
    }

    async fn on_websocket_disconnected(&self, event: wit::PeerEvent) {
        let ctx = WebContext::for_lifecycle();
        let event = peer_event_from_wit(event);
        self.inner.on_websocket_disconnected(&ctx, &event).await;
    }

    // ── WebRTC P2P (3, infallible) ─────────────────────────────────────

    async fn on_webrtc_connecting(&self, event: wit::PeerEvent) {
        let ctx = WebContext::for_lifecycle();
        let event = peer_event_from_wit(event);
        self.inner.on_webrtc_connecting(&ctx, &event).await;
    }

    async fn on_webrtc_connected(&self, event: wit::PeerEvent) {
        let ctx = WebContext::for_lifecycle();
        let event = peer_event_from_wit(event);
        self.inner.on_webrtc_connected(&ctx, &event).await;
    }

    async fn on_webrtc_disconnected(&self, event: wit::PeerEvent) {
        let ctx = WebContext::for_lifecycle();
        let event = peer_event_from_wit(event);
        self.inner.on_webrtc_disconnected(&ctx, &event).await;
    }

    // ── Credential (2, infallible) ─────────────────────────────────────

    async fn on_credential_renewed(&self, event: wit::CredentialEvent) {
        let ctx = WebContext::for_lifecycle();
        let event = credential_event_from_wit(event);
        self.inner.on_credential_renewed(&ctx, &event).await;
    }

    async fn on_credential_expiring(&self, event: wit::CredentialEvent) {
        let ctx = WebContext::for_lifecycle();
        let event = credential_event_from_wit(event);
        self.inner.on_credential_expiring(&ctx, &event).await;
    }

    // ── Mailbox (1, infallible) ────────────────────────────────────────

    async fn on_mailbox_backpressure(&self, event: wit::BackpressureEvent) {
        let ctx = WebContext::for_lifecycle();
        let event = backpressure_event_from_wit(event);
        self.inner.on_mailbox_backpressure(&ctx, &event).await;
    }

    async fn on_data_stream(
        &self,
        chunk: wit::DataStream,
        _sender: wit::ActrId,
    ) -> Result<(), wit::ActrError> {
        let chunk = data_stream_from_wit(chunk);
        Err(proto_error_to_wit(ActrError::NotImplemented(format!(
            "WebWorkloadAdapter::on_data_stream({})",
            chunk.stream_id
        ))))
    }
}
