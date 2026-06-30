//! Workload runtime abstractions for guest backends.
//!
//! This module replaces the old executor adapter layer. `ActrNode` dispatches
//! directly into a runtime `Workload` enum.

#[cfg(feature = "dynclib-engine")]
use actr_framework::WebRtcPeerStatus;
use actr_framework::guest::dynclib_abi::{
    self as guest_abi, HostCallRawV1, HostCallV1, HostDiscoverV1, HostRegisterStreamV1,
    HostSendDataStreamV1, HostTellV1, HostUnregisterStreamV1,
};
#[cfg(feature = "dynclib-engine")]
use actr_framework::guest::dynclib_abi::{AbiPayload, GuestHandleV1, GuestHookV1};
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

/// Package-backed observation hook event.
///
/// Linked workloads receive observation hooks through [`LinkedHandleObserver`].
/// Package-backed observers lower hook callbacks into this enum and serialize
/// them through [`Workload::dispatch_hook_event`], which enters the Wasm /
/// DynClib guest ABI.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum PackageHookEvent {
    SignalingConnecting,
    SignalingConnected,
    SignalingDisconnected,
    WebSocketConnecting(PeerEvent),
    WebSocketConnected(PeerEvent),
    WebSocketDisconnected(PeerEvent),
    WebRtcConnecting(PeerEvent),
    WebRtcConnected(PeerEvent),
    WebRtcDisconnected(PeerEvent),
    CredentialRenewed(CredentialEvent),
    CredentialExpiring(CredentialEvent),
    MailboxBackpressure(BackpressureEvent),
}

impl PackageHookEvent {
    pub(crate) fn request_id(&self) -> &'static str {
        match self {
            PackageHookEvent::SignalingConnecting => "hook:on_signaling_connecting",
            PackageHookEvent::SignalingConnected => "hook:on_signaling_connected",
            PackageHookEvent::SignalingDisconnected => "hook:on_signaling_disconnected",
            PackageHookEvent::WebSocketConnecting(_) => "hook:on_websocket_connecting",
            PackageHookEvent::WebSocketConnected(_) => "hook:on_websocket_connected",
            PackageHookEvent::WebSocketDisconnected(_) => "hook:on_websocket_disconnected",
            PackageHookEvent::WebRtcConnecting(_) => "hook:on_webrtc_connecting",
            PackageHookEvent::WebRtcConnected(_) => "hook:on_webrtc_connected",
            PackageHookEvent::WebRtcDisconnected(_) => "hook:on_webrtc_disconnected",
            PackageHookEvent::CredentialRenewed(_) => "hook:on_credential_renewed",
            PackageHookEvent::CredentialExpiring(_) => "hook:on_credential_expiring",
            PackageHookEvent::MailboxBackpressure(_) => "hook:on_mailbox_backpressure",
        }
    }
}

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
    // Lifecycle (fallible). The node decides at each lifecycle phase whether
    // an error aborts startup or is logged as best-effort observation.
    async fn on_start(&self, _ctx: &RuntimeContext) -> ActorResult<()> {
        Ok(())
    }
    async fn on_ready(&self, _ctx: &RuntimeContext) -> ActorResult<()> {
        Ok(())
    }
    async fn on_stop(&self, _ctx: &RuntimeContext) -> ActorResult<()> {
        Ok(())
    }
    async fn on_error(&self, _ctx: &RuntimeContext, _event: &ErrorEvent) -> ActorResult<()> {
        Ok(())
    }

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
    async fn on_start(&self, ctx: &RuntimeContext) -> ActorResult<()> {
        self.inner.on_start(ctx).await
    }
    async fn on_ready(&self, ctx: &RuntimeContext) -> ActorResult<()> {
        self.inner.on_ready(ctx).await
    }
    async fn on_stop(&self, ctx: &RuntimeContext) -> ActorResult<()> {
        self.inner.on_stop(ctx).await
    }
    async fn on_error(&self, ctx: &RuntimeContext, event: &ErrorEvent) -> ActorResult<()> {
        self.inner.on_error(ctx, event).await
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
    async fn on_start(&self, ctx: &RuntimeContext) -> ActorResult<()> {
        self.handle.on_start(ctx).await
    }
    async fn on_ready(&self, ctx: &RuntimeContext) -> ActorResult<()> {
        self.handle.on_ready(ctx).await
    }
    async fn on_stop(&self, ctx: &RuntimeContext) -> ActorResult<()> {
        self.handle.on_stop(ctx).await
    }
    async fn on_error(&self, ctx: &RuntimeContext, event: &ErrorEvent) -> ActorResult<()> {
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

/// Bridge adapter for package-backed workloads.
///
/// Attach installs this observer so the regular hook callback path can
/// forward observation events into Wasm / DynClib guests through the same
/// workload ABI used by lifecycle and dispatch entrypoints.
pub(crate) struct PackageHookObserver {
    pub(crate) workload_dispatch: Arc<tokio::sync::Mutex<Workload>>,
}

impl PackageHookObserver {
    async fn dispatch_hook(
        &self,
        label: &'static str,
        ctx: &RuntimeContext,
        event: PackageHookEvent,
    ) {
        use actr_framework::Context as _;

        let invocation = InvocationContext {
            self_id: ctx.self_id().clone(),
            caller_id: None,
            request_id: event.request_id().to_string(),
        };
        let call_executor =
            crate::lifecycle::node::lifecycle_host_abi(ctx.clone(), self.workload_dispatch.clone());
        let mut workload = self.workload_dispatch.lock().await;
        if let Err(e) = workload
            .dispatch_hook_event(event, invocation, &call_executor)
            .await
        {
            tracing::warn!(hook = label, error = %e, "workload package hook returned Err");
        }
    }
}

#[async_trait]
impl crate::lifecycle::hooks::WorkloadHookObserver for PackageHookObserver {
    async fn on_signaling_connecting(&self, ctx: Option<&RuntimeContext>) {
        if let Some(ctx) = ctx {
            self.dispatch_hook(
                "on_signaling_connecting",
                ctx,
                PackageHookEvent::SignalingConnecting,
            )
            .await;
        }
    }

    async fn on_signaling_connected(&self, ctx: Option<&RuntimeContext>) {
        if let Some(ctx) = ctx {
            self.dispatch_hook(
                "on_signaling_connected",
                ctx,
                PackageHookEvent::SignalingConnected,
            )
            .await;
        }
    }

    async fn on_signaling_disconnected(&self, ctx: &RuntimeContext) {
        self.dispatch_hook(
            "on_signaling_disconnected",
            ctx,
            PackageHookEvent::SignalingDisconnected,
        )
        .await;
    }

    async fn on_websocket_connecting(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.dispatch_hook(
            "on_websocket_connecting",
            ctx,
            PackageHookEvent::WebSocketConnecting(event.clone()),
        )
        .await;
    }

    async fn on_websocket_connected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.dispatch_hook(
            "on_websocket_connected",
            ctx,
            PackageHookEvent::WebSocketConnected(event.clone()),
        )
        .await;
    }

    async fn on_websocket_disconnected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.dispatch_hook(
            "on_websocket_disconnected",
            ctx,
            PackageHookEvent::WebSocketDisconnected(event.clone()),
        )
        .await;
    }

    async fn on_webrtc_connecting(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.dispatch_hook(
            "on_webrtc_connecting",
            ctx,
            PackageHookEvent::WebRtcConnecting(event.clone()),
        )
        .await;
    }

    async fn on_webrtc_connected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.dispatch_hook(
            "on_webrtc_connected",
            ctx,
            PackageHookEvent::WebRtcConnected(event.clone()),
        )
        .await;
    }

    async fn on_webrtc_disconnected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        self.dispatch_hook(
            "on_webrtc_disconnected",
            ctx,
            PackageHookEvent::WebRtcDisconnected(event.clone()),
        )
        .await;
    }

    async fn on_credential_renewed(&self, ctx: &RuntimeContext, event: &CredentialEvent) {
        self.dispatch_hook(
            "on_credential_renewed",
            ctx,
            PackageHookEvent::CredentialRenewed(event.clone()),
        )
        .await;
    }

    async fn on_credential_expiring(&self, ctx: &RuntimeContext, event: &CredentialEvent) {
        self.dispatch_hook(
            "on_credential_expiring",
            ctx,
            PackageHookEvent::CredentialExpiring(event.clone()),
        )
        .await;
    }

    async fn on_mailbox_backpressure(&self, ctx: &RuntimeContext, event: &BackpressureEvent) {
        self.dispatch_hook(
            "on_mailbox_backpressure",
            ctx,
            PackageHookEvent::MailboxBackpressure(*event),
        )
        .await;
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
    /// Invoke the workload's `on_start` lifecycle hook.
    pub(crate) fn on_start<'a>(
        &'a mut self,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: &'a HostAbiFn,
    ) -> Pin<Box<dyn Future<Output = ActorResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let _ = (&invocation, host_abi);
            match self {
                Workload::Linked(handle) => handle.on_start(&ctx).await,
                #[cfg(feature = "wasm-engine")]
                Workload::Wasm(workload) => workload
                    .call_on_start(invocation, host_abi)
                    .await
                    .map_err(|e| ActrError::Internal(format!("workload on_start failed: {e}"))),
                #[cfg(feature = "dynclib-engine")]
                Workload::DynClib(workload) => workload
                    .call_on_start(invocation, host_abi)
                    .await
                    .map_err(|e| ActrError::Internal(format!("workload on_start failed: {e}"))),
            }
        })
    }

    /// Invoke the workload's `on_ready` lifecycle hook.
    pub(crate) fn on_ready<'a>(
        &'a mut self,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: &'a HostAbiFn,
    ) -> Pin<Box<dyn Future<Output = ActorResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let _ = (&invocation, host_abi);
            match self {
                Workload::Linked(handle) => handle.on_ready(&ctx).await,
                #[cfg(feature = "wasm-engine")]
                Workload::Wasm(workload) => workload
                    .call_on_ready(invocation, host_abi)
                    .await
                    .map_err(|e| ActrError::Internal(format!("workload on_ready failed: {e}"))),
                #[cfg(feature = "dynclib-engine")]
                Workload::DynClib(workload) => workload
                    .call_on_ready(invocation, host_abi)
                    .await
                    .map_err(|e| ActrError::Internal(format!("workload on_ready failed: {e}"))),
            }
        })
    }

    /// Invoke the workload's `on_stop` lifecycle hook.
    pub(crate) fn on_stop<'a>(
        &'a mut self,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: &'a HostAbiFn,
    ) -> Pin<Box<dyn Future<Output = ActorResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let _ = (&invocation, host_abi);
            match self {
                Workload::Linked(handle) => handle.on_stop(&ctx).await,
                #[cfg(feature = "wasm-engine")]
                Workload::Wasm(workload) => workload
                    .call_on_stop(invocation, host_abi)
                    .await
                    .map_err(|e| ActrError::Internal(format!("workload on_stop failed: {e}"))),
                #[cfg(feature = "dynclib-engine")]
                Workload::DynClib(workload) => workload
                    .call_on_stop(invocation, host_abi)
                    .await
                    .map_err(|e| ActrError::Internal(format!("workload on_stop failed: {e}"))),
            }
        })
    }

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
            let _ = (&chunk, &sender, host_abi);
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

    /// Dispatch an observation hook into a package-backed workload.
    ///
    /// Linked workloads are intentionally a no-op here: they receive the same
    /// events through `hook_observer`, and calling them here would duplicate
    /// every hook invocation on the linked path.
    pub(crate) fn dispatch_hook_event<'a>(
        &'a mut self,
        event: PackageHookEvent,
        invocation: InvocationContext,
        host_abi: &'a HostAbiFn,
    ) -> Pin<Box<dyn Future<Output = ActorResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let _ = (&event, &invocation, host_abi);
            match self {
                Workload::Linked(_) => Ok(()),
                #[cfg(feature = "wasm-engine")]
                Workload::Wasm(workload) => workload
                    .call_hook_event(event, invocation, host_abi)
                    .await
                    .map_err(|e| ActrError::Internal(format!("workload hook failed: {e}"))),
                #[cfg(feature = "dynclib-engine")]
                Workload::DynClib(workload) => workload
                    .call_hook_event(event, invocation, host_abi)
                    .await
                    .map_err(|e| ActrError::Internal(format!("workload hook failed: {e}"))),
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
#[cfg(feature = "dynclib-engine")]
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
#[cfg(feature = "dynclib-engine")]
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

#[cfg(feature = "dynclib-engine")]
pub(crate) fn encode_guest_data_stream_request(
    chunk: DataStream,
    sender: ActrId,
) -> Result<Vec<u8>, i32> {
    let request = guest_abi::GuestDataStreamV1 { chunk, sender };
    let frame = request.to_frame()?;
    guest_abi::encode_message(&frame)
}

/// Encode a host-to-guest lifecycle request as `GuestLifecycleV1` wrapped in `AbiFrame`.
#[cfg(feature = "dynclib-engine")]
pub(crate) fn encode_guest_lifecycle_request(
    hook: u32,
    ctx: InvocationContext,
) -> Result<Vec<u8>, i32> {
    let request = guest_abi::GuestLifecycleV1 { ctx, hook };
    let frame = request.to_frame()?;
    guest_abi::encode_message(&frame)
}

#[cfg(feature = "dynclib-engine")]
fn timestamp_to_v1(time: std::time::SystemTime) -> guest_abi::TimestampV1 {
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    guest_abi::TimestampV1 {
        seconds: duration.as_secs(),
        nanoseconds: duration.subsec_nanos(),
    }
}

#[cfg(feature = "dynclib-engine")]
fn peer_event_to_v1(event: PeerEvent) -> guest_abi::PeerEventV1 {
    guest_abi::PeerEventV1 {
        peer: event.peer,
        relayed: event.relayed,
        status: event.status.map(peer_status_to_v1),
    }
}

#[cfg(feature = "dynclib-engine")]
fn peer_status_to_v1(status: WebRtcPeerStatus) -> u32 {
    use actr_framework::guest::dynclib_abi::webrtc_peer_status as st;
    match status {
        WebRtcPeerStatus::Idle => st::IDLE,
        WebRtcPeerStatus::Connecting => st::CONNECTING,
        WebRtcPeerStatus::Connected => st::CONNECTED,
        WebRtcPeerStatus::Recovering => st::RECOVERING,
    }
}

#[cfg(feature = "dynclib-engine")]
fn credential_event_to_v1(event: CredentialEvent) -> guest_abi::CredentialEventV1 {
    guest_abi::CredentialEventV1 {
        new_expiry: timestamp_to_v1(event.new_expiry),
    }
}

#[cfg(feature = "dynclib-engine")]
fn backpressure_event_to_v1(event: BackpressureEvent) -> guest_abi::BackpressureEventV1 {
    guest_abi::BackpressureEventV1 {
        queue_len: event.queue_len as u64,
        threshold: event.threshold as u64,
    }
}

/// Encode a host-to-guest observation hook request as `GuestHookV1`.
#[cfg(feature = "dynclib-engine")]
pub(crate) fn encode_guest_hook_request(
    event: PackageHookEvent,
    ctx: InvocationContext,
) -> Result<Vec<u8>, i32> {
    let mut request = GuestHookV1 {
        ctx,
        hook: 0,
        peer: None,
        credential: None,
        backpressure: None,
    };

    match event {
        PackageHookEvent::SignalingConnecting => {
            request.hook = guest_abi::runtime_hook::ON_SIGNALING_CONNECTING;
        }
        PackageHookEvent::SignalingConnected => {
            request.hook = guest_abi::runtime_hook::ON_SIGNALING_CONNECTED;
        }
        PackageHookEvent::SignalingDisconnected => {
            request.hook = guest_abi::runtime_hook::ON_SIGNALING_DISCONNECTED;
        }
        PackageHookEvent::WebSocketConnecting(event) => {
            request.hook = guest_abi::runtime_hook::ON_WEBSOCKET_CONNECTING;
            request.peer = Some(peer_event_to_v1(event));
        }
        PackageHookEvent::WebSocketConnected(event) => {
            request.hook = guest_abi::runtime_hook::ON_WEBSOCKET_CONNECTED;
            request.peer = Some(peer_event_to_v1(event));
        }
        PackageHookEvent::WebSocketDisconnected(event) => {
            request.hook = guest_abi::runtime_hook::ON_WEBSOCKET_DISCONNECTED;
            request.peer = Some(peer_event_to_v1(event));
        }
        PackageHookEvent::WebRtcConnecting(event) => {
            request.hook = guest_abi::runtime_hook::ON_WEBRTC_CONNECTING;
            request.peer = Some(peer_event_to_v1(event));
        }
        PackageHookEvent::WebRtcConnected(event) => {
            request.hook = guest_abi::runtime_hook::ON_WEBRTC_CONNECTED;
            request.peer = Some(peer_event_to_v1(event));
        }
        PackageHookEvent::WebRtcDisconnected(event) => {
            request.hook = guest_abi::runtime_hook::ON_WEBRTC_DISCONNECTED;
            request.peer = Some(peer_event_to_v1(event));
        }
        PackageHookEvent::CredentialRenewed(event) => {
            request.hook = guest_abi::runtime_hook::ON_CREDENTIAL_RENEWED;
            request.credential = Some(credential_event_to_v1(event));
        }
        PackageHookEvent::CredentialExpiring(event) => {
            request.hook = guest_abi::runtime_hook::ON_CREDENTIAL_EXPIRING;
            request.credential = Some(credential_event_to_v1(event));
        }
        PackageHookEvent::MailboxBackpressure(event) => {
            request.hook = guest_abi::runtime_hook::ON_MAILBOX_BACKPRESSURE;
            request.backpressure = Some(backpressure_event_to_v1(event));
        }
    }

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
    use crate::inbound::{DataStreamRegistry, MediaFrameRegistry};
    use crate::lifecycle::hooks::{
        HookContextBuilder, WorkloadHookObserverRef, build_hook_callback,
    };
    use crate::outbound::{Gate, HostGate};
    use crate::transport::HostTransport;
    use crate::wire::webrtc::{
        HookEvent, ReconnectConfig, SignalingClient, SignalingConfig, WebSocketSignalingClient,
    };
    use actr_framework::Context as FrameworkContext;
    use actr_framework::test_support::DummyContext;
    use actr_protocol::{AIdCredential, ActrId, ActrType, Realm};
    use tokio::sync::mpsc;

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

    fn test_credential() -> AIdCredential {
        AIdCredential {
            key_id: 1,
            claims: bytes::Bytes::from_static(b"claims"),
            signature: bytes::Bytes::from(vec![0; 64]),
        }
    }

    fn test_runtime_context(serial: u64) -> RuntimeContext {
        let host_transport = Arc::new(HostTransport::new());
        let inproc_gate = Gate::Host(Arc::new(HostGate::new(host_transport)));
        let signaling_client: Arc<dyn SignalingClient> =
            Arc::new(WebSocketSignalingClient::new(SignalingConfig {
                server_url: url::Url::parse("ws://127.0.0.1:9").expect("valid test URL"),
                connection_timeout: 1,
                heartbeat_interval: 30,
                reconnect_config: ReconnectConfig::default(),
                auth_config: None,
                webrtc_role: None,
            }));

        RuntimeContext::new(
            make_id(serial),
            None,
            "workload-test".to_string(),
            inproc_gate,
            None,
            Arc::new(DataStreamRegistry::new()),
            Arc::new(MediaFrameRegistry::new()),
            signaling_client,
            test_credential(),
            None,
            Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            None,
            0,
        )
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

    struct LifecycleFailingWorkload;

    #[async_trait]
    impl FrameworkWorkload for LifecycleFailingWorkload {
        type Dispatcher = LifecycleFailingDispatcher;

        async fn on_start<C: FrameworkContext>(&self, _ctx: &C) -> ActorResult<()> {
            Err(ActrError::Internal("on_start failed".to_string()))
        }
    }

    struct LifecycleFailingDispatcher;

    #[async_trait]
    impl MessageDispatcher for LifecycleFailingDispatcher {
        type Workload = LifecycleFailingWorkload;

        async fn dispatch<C: FrameworkContext>(
            _workload: &Self::Workload,
            _envelope: RpcEnvelope,
            _ctx: &C,
        ) -> ActorResult<Bytes> {
            Ok(Bytes::new())
        }
    }

    struct RecordingWorkload {
        tx: mpsc::UnboundedSender<String>,
    }

    #[async_trait]
    impl FrameworkWorkload for RecordingWorkload {
        type Dispatcher = RecordingDispatcher;

        async fn on_ready<C: FrameworkContext>(&self, ctx: &C) -> ActorResult<()> {
            let _ = self
                .tx
                .send(format!("on_ready:self={}", ctx.self_id().serial_number));
            Ok(())
        }

        async fn on_stop<C: FrameworkContext>(&self, ctx: &C) -> ActorResult<()> {
            let _ = self
                .tx
                .send(format!("on_stop:self={}", ctx.self_id().serial_number));
            Ok(())
        }

        async fn on_websocket_connected<C: FrameworkContext>(&self, ctx: &C, event: &PeerEvent) {
            let _ = self.tx.send(format!(
                "on_websocket_connected:self={}:peer={}:relayed={}",
                ctx.self_id().serial_number,
                event.peer.serial_number,
                match event.relayed {
                    Some(true) => "true",
                    Some(false) => "false",
                    None => "none",
                }
            ));
        }
    }

    struct RecordingDispatcher;

    #[async_trait]
    impl MessageDispatcher for RecordingDispatcher {
        type Workload = RecordingWorkload;

        async fn dispatch<C: FrameworkContext>(
            _workload: &Self::Workload,
            _envelope: RpcEnvelope,
            _ctx: &C,
        ) -> ActorResult<Bytes> {
            Ok(Bytes::new())
        }
    }

    async fn expect_recorded(rx: &mut mpsc::UnboundedReceiver<String>, expected: &'static str) {
        let observed = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("linked hook was not called")
            .expect("recording workload dropped");
        assert_eq!(observed, expected);
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

    #[tokio::test]
    async fn adapter_on_start_propagates_workload_error() {
        let adapter = WorkloadAdapter::new(LifecycleFailingWorkload);
        let ctx = test_runtime_context(7);

        let err = adapter
            .on_start(&ctx)
            .await
            .expect_err("adapter must preserve lifecycle errors");

        match err {
            ActrError::Internal(msg) => {
                assert!(msg.contains("on_start failed"), "unexpected message: {msg}");
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn workload_on_start_propagates_linked_error() {
        let handle: Arc<dyn LinkedWorkloadHandle> = WorkloadAdapter::new(LifecycleFailingWorkload);
        let mut workload = Workload::Linked(handle);
        let ctx = test_runtime_context(8);
        let invocation = InvocationContext {
            self_id: make_id(8),
            caller_id: None,
            request_id: "lifecycle:on_start".to_string(),
        };
        let host_abi: HostAbiFn = Arc::new(|_| Box::pin(async { HostOperationResult::Done }));

        let err = workload
            .on_start(ctx, invocation, &host_abi)
            .await
            .expect_err("workload lifecycle must preserve linked errors");

        match err {
            ActrError::Internal(msg) => {
                assert!(msg.contains("on_start failed"), "unexpected message: {msg}");
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn workload_on_ready_and_on_stop_reach_linked_workload() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handle: Arc<dyn LinkedWorkloadHandle> = WorkloadAdapter::new(RecordingWorkload { tx });
        let mut workload = Workload::Linked(handle);
        let host_abi: HostAbiFn = Arc::new(|_| Box::pin(async { HostOperationResult::Done }));

        workload
            .on_ready(
                test_runtime_context(9),
                InvocationContext {
                    self_id: make_id(9),
                    caller_id: None,
                    request_id: "lifecycle:on_ready".to_string(),
                },
                &host_abi,
            )
            .await
            .expect("linked on_ready should dispatch");
        workload
            .on_stop(
                test_runtime_context(9),
                InvocationContext {
                    self_id: make_id(9),
                    caller_id: None,
                    request_id: "lifecycle:on_stop".to_string(),
                },
                &host_abi,
            )
            .await
            .expect("linked on_stop should dispatch");

        expect_recorded(&mut rx, "on_ready:self=9").await;
        expect_recorded(&mut rx, "on_stop:self=9").await;
    }

    #[tokio::test]
    async fn hook_callback_reaches_linked_workload_once() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handle: Arc<dyn LinkedWorkloadHandle> = WorkloadAdapter::new(RecordingWorkload { tx });
        let observer: WorkloadHookObserverRef = Arc::new(LinkedHandleObserver { handle });
        let ctx = test_runtime_context(10);
        let ctx_builder: HookContextBuilder = Arc::new(move || {
            let ctx = ctx.clone();
            Box::pin(async move { Some(ctx) })
        });
        let cb = build_hook_callback(Some(observer), ctx_builder);

        cb(HookEvent::WebSocketConnected {
            peer_id: make_id(42),
        })
        .await;

        expect_recorded(
            &mut rx,
            "on_websocket_connected:self=10:peer=42:relayed=none",
        )
        .await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(
            rx.try_recv().is_err(),
            "linked workload should receive exactly one hook event"
        );
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
