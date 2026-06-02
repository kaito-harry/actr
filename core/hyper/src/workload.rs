//! Workload runtime abstractions for guest backends.
//!
//! This module replaces the old executor adapter layer. `ActrNode` dispatches
//! directly into a runtime `Workload` enum.

use actr_framework::guest::dynclib_abi::{
    self as guest_abi, HostCallRawV1, HostCallV1, HostDiscoverV1, HostRegisterStreamV1,
    HostSendDataStreamV1, HostTellV1, HostUnregisterStreamV1,
};
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
use actr_framework::guest::dynclib_abi::{AbiPayload, GuestHandleV1};
use actr_framework::{
    BackpressureEvent, CredentialEvent, ErrorEvent, MessageDispatcher, PeerEvent,
    Workload as FrameworkWorkload,
};
use actr_protocol::{ActorResult, ActrError, ActrId, DataStream, RpcEnvelope};
use async_trait::async_trait;
use bytes::Bytes;
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
use prost::Message;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::context::RuntimeContext;

/// ABI-stable invocation context passed into guest runtime on each request.
pub type InvocationContext = guest_abi::InvocationContextV1;

/// Guest-initiated host operation carrying strong-typed ABI payloads.
#[derive(Debug)]
pub enum HostOperation {
    Call(HostCallV1),
    Tell(HostTellV1),
    Discover(HostDiscoverV1),
    CallRaw(HostCallRawV1),
    RegisterStream(HostRegisterStreamV1),
    UnregisterStream(HostUnregisterStreamV1),
    SendDataStream(HostSendDataStreamV1),
}

/// Result of a host operation.
#[derive(Debug)]
pub enum HostOperationResult {
    Bytes(Vec<u8>),
    Done,
    Error(i32),
}

/// Host-side async bridge used by guest runtimes.
///
/// Passed as `&HostAbiFn` through the dispatch path. Wrapped in an `Arc`
/// (rather than the historical `Box`) so that the wasm Component Model
/// host can clone the bridge into its `Store<HostState>` for the
/// duration of a dispatch without forcing every call site to rebox —
/// cloning an `Arc` is a refcount bump, safe to do per dispatch.
pub type HostAbiFn = Arc<
    dyn Fn(HostOperation) -> Pin<Box<dyn Future<Output = HostOperationResult> + Send>>
        + Send
        + Sync,
>;

/// Object-safe handle to a workload linked directly into the host process
/// (e.g. an embedded Swift / Kotlin app, or a Rust process that owns the
/// actor's business code as a struct rather than a packaged binary).
///
/// Plugged into a [`crate::Node`] via
/// `Node::link_handle` (crate-internal object-safe path) or
/// [`crate::Node::link`] (generic convenience that
/// wraps any [`FrameworkWorkload`] implementation in a
/// [`WorkloadAdapter`]).
///
/// A linked handle carries two responsibilities:
///
/// 1. **Observation hooks** — every method from
///    [`actr_framework::Workload`]'s hook surface has an object-safe
///    counterpart here. The runtime bridges these via
///    [`LinkedHandleObserver`] into the internal
///    [`crate::lifecycle::hooks::WorkloadHookObserver`] plumbing.
/// 2. **Inbound RPC dispatch** — the [`LinkedWorkloadHandle::dispatch`]
///    method is invoked by the node's `handle_incoming` path when the
///    node has been linked through the host path. Package-backed
///    attaches (`attach`) continue to dispatch through the WASM / dynclib
///    guest ABI.
#[async_trait]
#[allow(dead_code)]
pub(crate) trait LinkedWorkloadHandle: Send + Sync + 'static {
    // Lifecycle (fallible — hook-path errors are logged & swallowed)
    async fn on_start(&self, _ctx: &RuntimeContext) {}
    async fn on_ready(&self, _ctx: &RuntimeContext) {}
    async fn on_stop(&self, _ctx: &RuntimeContext) {}
    async fn on_error(&self, _ctx: &RuntimeContext, _event: &ErrorEvent) {}

    // Signaling
    async fn on_signaling_connecting(&self, _ctx: Option<&RuntimeContext>) {}
    async fn on_signaling_connected(&self, _ctx: Option<&RuntimeContext>) {}
    async fn on_signaling_disconnected(&self, _ctx: &RuntimeContext) {}

    // WebSocket
    async fn on_websocket_connecting(&self, _ctx: &RuntimeContext, _event: &PeerEvent) {}
    async fn on_websocket_connected(&self, _ctx: &RuntimeContext, _event: &PeerEvent) {}
    async fn on_websocket_disconnected(&self, _ctx: &RuntimeContext, _event: &PeerEvent) {}

    // WebRTC P2P
    async fn on_webrtc_connecting(&self, _ctx: &RuntimeContext, _event: &PeerEvent) {}
    async fn on_webrtc_connected(&self, _ctx: &RuntimeContext, _event: &PeerEvent) {}
    async fn on_webrtc_disconnected(&self, _ctx: &RuntimeContext, _event: &PeerEvent) {}

    // Credential
    async fn on_credential_renewed(&self, _ctx: &RuntimeContext, _event: &CredentialEvent) {}
    async fn on_credential_expiring(&self, _ctx: &RuntimeContext, _event: &CredentialEvent) {}

    // Mailbox
    async fn on_mailbox_backpressure(&self, _ctx: &RuntimeContext, _event: &BackpressureEvent) {}

    /// Dispatch one inbound RPC envelope into the linked workload.
    ///
    /// The default implementation rejects the dispatch with
    /// `ActrError::NotImplemented` so that handles concerned only with
    /// observation hooks (e.g. adapter-only hosts) can be plugged in
    /// without supplying a dispatcher. Generic linked attaches go
    /// through [`WorkloadAdapter`], which overrides this method to call
    /// into the framework's `MessageDispatcher`.
    async fn dispatch(
        &self,
        _envelope: RpcEnvelope,
        _ctx: Arc<RuntimeContext>,
    ) -> ActorResult<Bytes> {
        Err(ActrError::NotImplemented(
            "linked workload handle has no dispatcher bound".to_string(),
        ))
    }
}

/// Generic bridge from a user-defined [`actr_framework::Workload`] (with
/// its associated [`MessageDispatcher`]) to the object-safe
/// [`LinkedWorkloadHandle`] stored on the node.
///
/// `WorkloadAdapter<W>` monomorphises the generic `<C: Context>` methods
/// from the framework trait to the concrete [`RuntimeContext`] type the
/// node carries, and forwards inbound RPC envelopes through
/// `<W::Dispatcher as MessageDispatcher>::dispatch`.
///
/// Callers rarely construct this directly; prefer
/// [`crate::Node::link`] which wraps the workload automatically.
pub(crate) struct WorkloadAdapter<W: FrameworkWorkload> {
    inner: Arc<W>,
}

impl<W: FrameworkWorkload> WorkloadAdapter<W> {
    /// Wrap the workload in an adapter. Equivalent to
    /// `Arc::new(WorkloadAdapter { inner: Arc::new(workload) })` but keeps
    /// the field private so future refactors can change its shape.
    pub fn new(workload: W) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(workload),
        })
    }

    /// Test-friendly dispatch entry point: forwards to the workload's
    /// [`MessageDispatcher`] using any `Context` implementation.
    ///
    /// The production path goes through
    /// [`LinkedWorkloadHandle::dispatch`] with a concrete
    /// [`RuntimeContext`], but that requires the full node plumbing.
    /// Tests and alternate hosts can call this method with a lightweight
    /// `Context` (e.g. `actr_framework::test_support::DummyContext`)
    /// without standing up a running node.
    pub async fn dispatch_with_ctx<C: actr_framework::Context>(
        &self,
        envelope: RpcEnvelope,
        ctx: &C,
    ) -> ActorResult<Bytes> {
        <W::Dispatcher as MessageDispatcher>::dispatch(self.inner.as_ref(), envelope, ctx).await
    }
}

#[async_trait]
impl<W: FrameworkWorkload> LinkedWorkloadHandle for WorkloadAdapter<W> {
    // ── Lifecycle ────────────────────────────────────────────────────────
    async fn on_start(&self, ctx: &RuntimeContext) {
        if let Err(e) = self.inner.on_start(ctx).await {
            tracing::warn!(error = %e, "linked workload on_start returned Err");
        }
    }
    async fn on_ready(&self, ctx: &RuntimeContext) {
        if let Err(e) = self.inner.on_ready(ctx).await {
            tracing::warn!(error = %e, "linked workload on_ready returned Err");
        }
    }
    async fn on_stop(&self, ctx: &RuntimeContext) {
        if let Err(e) = self.inner.on_stop(ctx).await {
            tracing::warn!(error = %e, "linked workload on_stop returned Err");
        }
    }
    async fn on_error(&self, ctx: &RuntimeContext, event: &ErrorEvent) {
        if let Err(e) = self.inner.on_error(ctx, event).await {
            tracing::warn!(error = %e, "linked workload on_error returned Err");
        }
    }

    // ── Signaling ────────────────────────────────────────────────────────
    async fn on_signaling_connecting(&self, ctx: Option<&RuntimeContext>) {
        self.inner.on_signaling_connecting(ctx).await
    }
    async fn on_signaling_connected(&self, ctx: Option<&RuntimeContext>) {
        self.inner.on_signaling_connected(ctx).await
    }
    async fn on_signaling_disconnected(&self, ctx: &RuntimeContext) {
        self.inner.on_signaling_disconnected(ctx).await
    }

    // ── WebSocket ────────────────────────────────────────────────────────
    async fn on_websocket_connecting(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.inner.on_websocket_connecting(ctx, event).await
    }
    async fn on_websocket_connected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.inner.on_websocket_connected(ctx, event).await
    }
    async fn on_websocket_disconnected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.inner.on_websocket_disconnected(ctx, event).await
    }

    // ── WebRTC P2P ───────────────────────────────────────────────────────
    async fn on_webrtc_connecting(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.inner.on_webrtc_connecting(ctx, event).await
    }
    async fn on_webrtc_connected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.inner.on_webrtc_connected(ctx, event).await
    }
    async fn on_webrtc_disconnected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.inner.on_webrtc_disconnected(ctx, event).await
    }

    // ── Credential ───────────────────────────────────────────────────────
    async fn on_credential_renewed(&self, ctx: &RuntimeContext, event: &CredentialEvent) {
        self.inner.on_credential_renewed(ctx, event).await
    }
    async fn on_credential_expiring(&self, ctx: &RuntimeContext, event: &CredentialEvent) {
        self.inner.on_credential_expiring(ctx, event).await
    }

    // ── Mailbox ──────────────────────────────────────────────────────────
    async fn on_mailbox_backpressure(&self, ctx: &RuntimeContext, event: &BackpressureEvent) {
        self.inner.on_mailbox_backpressure(ctx, event).await
    }

    // ── Dispatch ─────────────────────────────────────────────────────────
    async fn dispatch(
        &self,
        envelope: RpcEnvelope,
        ctx: Arc<RuntimeContext>,
    ) -> ActorResult<Bytes> {
        self.dispatch_with_ctx(envelope, ctx.as_ref()).await
    }
}

/// Bridge adapter: forwards every [`LinkedWorkloadHandle`] method to the
/// `pub(crate)` [`crate::lifecycle::hooks::WorkloadHookObserver`] expected by
/// the hook dispatcher. Lets the public linked-handle trait live without
/// exposing the internal hook plumbing.
pub(crate) struct LinkedHandleObserver {
    pub(crate) handle: Arc<dyn LinkedWorkloadHandle>,
}

#[async_trait]
impl crate::lifecycle::hooks::WorkloadHookObserver for LinkedHandleObserver {
    async fn on_start(&self, ctx: &RuntimeContext) {
        self.handle.on_start(ctx).await
    }
    async fn on_ready(&self, ctx: &RuntimeContext) {
        self.handle.on_ready(ctx).await
    }
    async fn on_stop(&self, ctx: &RuntimeContext) {
        self.handle.on_stop(ctx).await
    }
    async fn on_error(&self, ctx: &RuntimeContext, event: &ErrorEvent) {
        self.handle.on_error(ctx, event).await
    }
    async fn on_signaling_connecting(&self, ctx: Option<&RuntimeContext>) {
        self.handle.on_signaling_connecting(ctx).await
    }
    async fn on_signaling_connected(&self, ctx: Option<&RuntimeContext>) {
        self.handle.on_signaling_connected(ctx).await
    }
    async fn on_signaling_disconnected(&self, ctx: &RuntimeContext) {
        self.handle.on_signaling_disconnected(ctx).await
    }
    async fn on_websocket_connecting(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.handle.on_websocket_connecting(ctx, event).await
    }
    async fn on_websocket_connected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.handle.on_websocket_connected(ctx, event).await
    }
    async fn on_websocket_disconnected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.handle.on_websocket_disconnected(ctx, event).await
    }
    async fn on_webrtc_connecting(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.handle.on_webrtc_connecting(ctx, event).await
    }
    async fn on_webrtc_connected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.handle.on_webrtc_connected(ctx, event).await
    }
    async fn on_webrtc_disconnected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.handle.on_webrtc_disconnected(ctx, event).await
    }
    async fn on_credential_renewed(&self, ctx: &RuntimeContext, event: &CredentialEvent) {
        self.handle.on_credential_renewed(ctx, event).await
    }
    async fn on_credential_expiring(&self, ctx: &RuntimeContext, event: &CredentialEvent) {
        self.handle.on_credential_expiring(ctx, event).await
    }
    async fn on_mailbox_backpressure(&self, ctx: &RuntimeContext, event: &BackpressureEvent) {
        self.handle.on_mailbox_backpressure(ctx, event).await
    }
}

/// Runtime workload enum.
///
/// Covers four attach flavours:
///
/// - `Wasm` / `DynClib` — a verified `.actr` package bound through
///   [`crate::Node::attach`]. The package carries a guest binary that the
///   host dispatches RPC envelopes into.
/// - `Linked` — an in-process workload handle bound through
///   [`crate::Node::link`]. Inbound RPC envelopes are forwarded to the
///   handle via [`LinkedWorkloadHandle::dispatch`].
#[allow(clippy::large_enum_variant)]
pub(crate) enum Workload {
    /// Linked in-process workload handle — hosts dispatch and lifecycle
    /// hooks inside the current process without a packaged guest binary.
    Linked(Arc<dyn LinkedWorkloadHandle>),
    #[cfg(feature = "wasm-engine")]
    Wasm(crate::wasm::WasmWorkload),
    #[cfg(feature = "dynclib-engine")]
    DynClib(crate::dynclib::DynClibWorkload),
}

impl std::fmt::Debug for Workload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Workload::Linked(_) => f.write_str("Workload::Linked(<dyn LinkedWorkloadHandle>)"),
            #[cfg(feature = "wasm-engine")]
            Workload::Wasm(w) => f.debug_tuple("Workload::Wasm").field(w).finish(),
            #[cfg(feature = "dynclib-engine")]
            Workload::DynClib(w) => f.debug_tuple("Workload::DynClib").field(w).finish(),
        }
    }
}

impl Workload {
    /// Dispatch one inbound RPC envelope.
    pub(crate) fn dispatch_envelope<'a>(
        &'a mut self,
        envelope: RpcEnvelope,
        ctx: crate::context::RuntimeContext,
        invocation: InvocationContext,
        _host_abi: &'a HostAbiFn,
    ) -> Pin<Box<dyn Future<Output = ActorResult<Bytes>> + Send + 'a>> {
        Box::pin(async move {
            let _ = &invocation;
            match self {
                Workload::Linked(handle) => handle.dispatch(envelope, Arc::new(ctx)).await,
                #[cfg(feature = "wasm-engine")]
                Workload::Wasm(workload) => {
                    let request_bytes = envelope.encode_to_vec();
                    workload
                        .handle(&request_bytes, invocation, _host_abi)
                        .await
                        .map(Bytes::from)
                        .map_err(|e| ActrError::Internal(format!("workload dispatch failed: {e}")))
                }
                #[cfg(feature = "dynclib-engine")]
                Workload::DynClib(workload) => {
                    let request_bytes = envelope.encode_to_vec();
                    workload
                        .handle(&request_bytes, invocation, _host_abi)
                        .await
                        .map(Bytes::from)
                        .map_err(|e| ActrError::Internal(format!("workload dispatch failed: {e}")))
                }
            }
        })
    }

    pub(crate) fn dispatch_data_stream<'a>(
        &'a mut self,
        chunk: DataStream,
        sender: ActrId,
        invocation: InvocationContext,
        host_abi: &'a HostAbiFn,
    ) -> Pin<Box<dyn Future<Output = ActorResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let _ = &invocation;
            match self {
                Workload::Linked(_) => {
                    let _ = (&chunk, &sender, host_abi);
                    Err(ActrError::NotImplemented(
                        "linked workload stream callbacks are registered directly on RuntimeContext"
                            .to_string(),
                    ))
                }
                #[cfg(feature = "wasm-engine")]
                Workload::Wasm(workload) => workload
                    .handle_data_stream(chunk, sender, invocation, host_abi)
                    .await
                    .map_err(|e| {
                        ActrError::Internal(format!("workload stream dispatch failed: {e}"))
                    }),
                #[cfg(feature = "dynclib-engine")]
                Workload::DynClib(workload) => workload
                    .handle_data_stream(chunk, sender, host_abi)
                    .await
                    .map_err(|e| {
                        ActrError::Internal(format!("workload stream dispatch failed: {e}"))
                    }),
            }
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared host-side helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Decode an [`guest_abi::AbiFrame`] into a strongly-typed [`HostOperation`].
///
/// Shared by both WASM and DynClib host backends.
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
pub(crate) fn decode_host_operation(frame: guest_abi::AbiFrame) -> Result<HostOperation, i32> {
    if frame.abi_version != guest_abi::version::V1 {
        return Err(guest_abi::code::PROTOCOL_ERROR);
    }

    match frame.op {
        guest_abi::op::HOST_CALL => {
            let payload = <HostCallV1 as AbiPayload>::decode_payload(&frame.payload)?;
            Ok(HostOperation::Call(payload))
        }
        guest_abi::op::HOST_TELL => {
            let payload = <HostTellV1 as AbiPayload>::decode_payload(&frame.payload)?;
            Ok(HostOperation::Tell(payload))
        }
        guest_abi::op::HOST_CALL_RAW => {
            let payload = <HostCallRawV1 as AbiPayload>::decode_payload(&frame.payload)?;
            Ok(HostOperation::CallRaw(payload))
        }
        guest_abi::op::HOST_DISCOVER => {
            let payload = <HostDiscoverV1 as AbiPayload>::decode_payload(&frame.payload)?;
            Ok(HostOperation::Discover(payload))
        }
        guest_abi::op::HOST_REGISTER_STREAM => {
            let payload = <HostRegisterStreamV1 as AbiPayload>::decode_payload(&frame.payload)?;
            Ok(HostOperation::RegisterStream(payload))
        }
        guest_abi::op::HOST_UNREGISTER_STREAM => {
            let payload = <HostUnregisterStreamV1 as AbiPayload>::decode_payload(&frame.payload)?;
            Ok(HostOperation::UnregisterStream(payload))
        }
        guest_abi::op::HOST_SEND_DATA_STREAM => {
            let payload = <HostSendDataStreamV1 as AbiPayload>::decode_payload(&frame.payload)?;
            Ok(HostOperation::SendDataStream(payload))
        }
        _ => Err(guest_abi::code::UNSUPPORTED_OP),
    }
}

/// Encode an inbound guest dispatch as `GuestHandleV1` wrapped in `AbiFrame`.
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
pub(crate) fn encode_guest_handle_request(
    request_bytes: &[u8],
    ctx: InvocationContext,
) -> Result<Vec<u8>, i32> {
    let request = GuestHandleV1 {
        ctx,
        rpc_envelope: request_bytes.to_vec(),
    };
    let frame = request.to_frame()?;
    guest_abi::encode_message(&frame)
}

#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
pub(crate) fn encode_guest_data_stream_request(
    chunk: DataStream,
    sender: ActrId,
) -> Result<Vec<u8>, i32> {
    let request = guest_abi::GuestDataStreamV1 { chunk, sender };
    let frame = request.to_frame()?;
    guest_abi::encode_message(&frame)
}

/// Decode guest-encoded [`DestV1`] back to [`actr_framework::Dest`].
///
/// Re-exported from `actr_framework::guest::dynclib_abi` for host-side convenience.
pub(crate) fn decode_dest(
    v1: &actr_framework::guest::dynclib_abi::DestV1,
) -> Option<actr_framework::Dest> {
    actr_framework::guest::dynclib_abi::dest_v1_to_dest(v1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_framework::Context as FrameworkContext;
    use actr_framework::test_support::DummyContext;
    use actr_protocol::{ActrId, ActrType, Realm};

    fn make_id(serial: u64) -> ActrId {
        ActrId {
            realm: Realm { realm_id: 1 },
            serial_number: serial,
            r#type: ActrType {
                manufacturer: "test".to_string(),
                name: "UnitTestActor".to_string(),
                version: "0.0.1".to_string(),
            },
        }
    }

    // ── Minimal Workload + Dispatcher used for adapter tests ────────────────
    struct EchoWorkload {
        suffix: String,
    }

    #[async_trait]
    impl FrameworkWorkload for EchoWorkload {
        type Dispatcher = EchoDispatcher;
    }

    struct EchoDispatcher;

    #[async_trait]
    impl MessageDispatcher for EchoDispatcher {
        type Workload = EchoWorkload;

        async fn dispatch<C: FrameworkContext>(
            workload: &Self::Workload,
            envelope: RpcEnvelope,
            _ctx: &C,
        ) -> ActorResult<Bytes> {
            match envelope.route_key.as_str() {
                "echo" => {
                    let payload = envelope
                        .payload
                        .as_ref()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .unwrap_or_default();
                    let reply = format!("{payload}{}", workload.suffix);
                    Ok(Bytes::from(reply.into_bytes()))
                }
                other => Err(ActrError::InvalidArgument(format!(
                    "unknown route: {other}"
                ))),
            }
        }
    }

    #[tokio::test]
    async fn adapter_dispatch_routes_to_workload_dispatcher() {
        let adapter = WorkloadAdapter::new(EchoWorkload {
            suffix: "-ok".to_string(),
        });
        let ctx = DummyContext::new(make_id(42));
        let envelope = RpcEnvelope {
            request_id: "r1".to_string(),
            route_key: "echo".to_string(),
            payload: Some(Bytes::from_static(b"hello")),
            ..Default::default()
        };
        let resp = adapter
            .dispatch_with_ctx(envelope, &ctx)
            .await
            .expect("dispatch must succeed");
        assert_eq!(&resp[..], b"hello-ok");
    }

    #[tokio::test]
    async fn adapter_dispatch_propagates_unknown_route_error() {
        let adapter = WorkloadAdapter::new(EchoWorkload {
            suffix: "-ok".to_string(),
        });
        let ctx = DummyContext::new(make_id(1));
        let envelope = RpcEnvelope {
            request_id: "r2".to_string(),
            route_key: "does/not/exist".to_string(),
            payload: Some(Bytes::new()),
            ..Default::default()
        };
        let err = adapter
            .dispatch_with_ctx(envelope, &ctx)
            .await
            .expect_err("unknown route must error");
        match err {
            ActrError::InvalidArgument(msg) => {
                assert!(msg.contains("unknown route"), "unexpected message: {msg}")
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    /// The object-safe bound is the whole point of `LinkedWorkloadHandle`;
    /// this guard catches anyone accidentally adding a non-object-safe
    /// method to the trait in the future.
    #[test]
    fn linked_workload_handle_is_object_safe() {
        fn accepts(_: Arc<dyn LinkedWorkloadHandle>) {}
        let adapter: Arc<dyn LinkedWorkloadHandle> = WorkloadAdapter::new(EchoWorkload {
            suffix: "-ok".to_string(),
        });
        accepts(adapter);
    }

    /// Verify the `Debug` surface stays stable for linked workloads.
    #[test]
    fn linked_workload_debug_surface() {
        let handle: Arc<dyn LinkedWorkloadHandle> = WorkloadAdapter::new(EchoWorkload {
            suffix: "-ok".to_string(),
        });
        let linked = Workload::Linked(handle);
        assert!(format!("{:?}", linked).starts_with("Workload::Linked"));
    }
}
