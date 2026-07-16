//! Runtime-side workload hook plumbing.
//!
//! The user-facing [`actr_framework::Workload`] trait is **not** object-safe
//! (it carries an associated `Dispatcher` type and generic `<C: Context>`
//! methods), so `Arc<dyn Workload>` is not representable. The node still
//! needs a way to dispatch observation events (signaling / transport /
//! credential / mailbox) into whatever workload the shell is hosting
//! through a single object-safe callback surface.
//!
//! This module bridges the gap by defining [`WorkloadHookObserver`] — an
//! object-safe counterpart of the framework's observation hooks — that can
//! be stored as `Option<Arc<dyn WorkloadHookObserver>>` on the running
//! node. Event sources (signaling client, WebRTC coordinator, WebSocket
//! gate, mailbox loop, credential renewal) enqueue events into a single
//! dispatcher task. The dispatcher awaits each observer callback in FIFO
//! order and isolates panics so a misbehaving observer cannot take the node
//! down with it.
//!
//! The framework's built-in tracing defaults still fire regardless of
//! whether an observer is installed — they are invoked by the event-source
//! wire-up sites directly via the existing `HookCallback` plumbing.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;

use actr_framework::{
    BackpressureEvent, CredentialEvent, ErrorCategory, ErrorEvent, PeerEvent, WebRtcPeerStatus,
};
use actr_protocol::{ActorResult, ActrError};
use async_trait::async_trait;
use futures_util::FutureExt as _;

use crate::context::RuntimeContext;
use crate::wire::webrtc::{HookCallback, HookEvent};

/// Object-safe observer that mirrors the observation hooks defined on
/// [`actr_framework::Workload`] but uses the concrete [`RuntimeContext`]
/// and trait objects throughout so it can live behind an `Arc`.
///
/// Hyper wires this observer up from an external adapter (e.g. the FFI
/// `DynamicWorkload`). Each method has a no-op default so adopters can
/// override only the hooks they care about.
///
/// This trait is the object-safe hook surface behind the internal handle
/// used by `Node::link(...)`. Hook delivery flows through this trait;
/// inbound RPC dispatch is handled separately by the sibling
/// `LinkedWorkloadHandle` path in `workload.rs`.
#[async_trait]
#[allow(dead_code)]
pub(crate) trait WorkloadHookObserver: Send + Sync + 'static {
    // Lifecycle (fallible). Startup code awaits these hooks directly when
    // their result participates in node lifecycle semantics.
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
}

/// Shared observer handle held by the running node.
pub(crate) type WorkloadHookObserverRef = Arc<dyn WorkloadHookObserver>;

/// Compose two optional observers into one delivery target.
///
/// Package-backed nodes use this to keep guest hook delivery installed while
/// also allowing a host UI/runtime shell to observe connection state.
pub(crate) fn chain_observers(
    first: Option<WorkloadHookObserverRef>,
    second: Option<WorkloadHookObserverRef>,
) -> Option<WorkloadHookObserverRef> {
    match (first, second) {
        (None, None) => None,
        (Some(observer), None) | (None, Some(observer)) => Some(observer),
        (Some(first), Some(second)) => Some(Arc::new(ChainedHookObserver { first, second })),
    }
}

struct ChainedHookObserver {
    first: WorkloadHookObserverRef,
    second: WorkloadHookObserverRef,
}

#[async_trait]
impl WorkloadHookObserver for ChainedHookObserver {
    async fn on_start(&self, ctx: &RuntimeContext) -> ActorResult<()> {
        call_both_lifecycle(
            self.first.on_start(ctx).await,
            self.second.on_start(ctx).await,
        )
    }

    async fn on_ready(&self, ctx: &RuntimeContext) -> ActorResult<()> {
        call_both_lifecycle(
            self.first.on_ready(ctx).await,
            self.second.on_ready(ctx).await,
        )
    }

    async fn on_stop(&self, ctx: &RuntimeContext) -> ActorResult<()> {
        call_both_lifecycle(
            self.first.on_stop(ctx).await,
            self.second.on_stop(ctx).await,
        )
    }

    async fn on_error(&self, ctx: &RuntimeContext, event: &ErrorEvent) -> ActorResult<()> {
        call_both_lifecycle(
            self.first.on_error(ctx, event).await,
            self.second.on_error(ctx, event).await,
        )
    }

    async fn on_signaling_connecting(&self, ctx: Option<&RuntimeContext>) {
        tokio::join!(
            call_observation_hook(
                "on_signaling_connecting:first",
                self.first.on_signaling_connecting(ctx),
            ),
            call_observation_hook(
                "on_signaling_connecting:second",
                self.second.on_signaling_connecting(ctx),
            ),
        );
    }

    async fn on_signaling_connected(&self, ctx: Option<&RuntimeContext>) {
        tokio::join!(
            call_observation_hook(
                "on_signaling_connected:first",
                self.first.on_signaling_connected(ctx),
            ),
            call_observation_hook(
                "on_signaling_connected:second",
                self.second.on_signaling_connected(ctx),
            ),
        );
    }

    async fn on_signaling_disconnected(&self, ctx: &RuntimeContext) {
        tokio::join!(
            call_observation_hook(
                "on_signaling_disconnected:first",
                self.first.on_signaling_disconnected(ctx),
            ),
            call_observation_hook(
                "on_signaling_disconnected:second",
                self.second.on_signaling_disconnected(ctx),
            ),
        );
    }

    async fn on_websocket_connecting(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        tokio::join!(
            call_observation_hook(
                "on_websocket_connecting:first",
                self.first.on_websocket_connecting(ctx, event),
            ),
            call_observation_hook(
                "on_websocket_connecting:second",
                self.second.on_websocket_connecting(ctx, event),
            ),
        );
    }

    async fn on_websocket_connected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        tokio::join!(
            call_observation_hook(
                "on_websocket_connected:first",
                self.first.on_websocket_connected(ctx, event),
            ),
            call_observation_hook(
                "on_websocket_connected:second",
                self.second.on_websocket_connected(ctx, event),
            ),
        );
    }

    async fn on_websocket_disconnected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        tokio::join!(
            call_observation_hook(
                "on_websocket_disconnected:first",
                self.first.on_websocket_disconnected(ctx, event),
            ),
            call_observation_hook(
                "on_websocket_disconnected:second",
                self.second.on_websocket_disconnected(ctx, event),
            ),
        );
    }

    async fn on_webrtc_connecting(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        tokio::join!(
            call_observation_hook(
                "on_webrtc_connecting:first",
                self.first.on_webrtc_connecting(ctx, event),
            ),
            call_observation_hook(
                "on_webrtc_connecting:second",
                self.second.on_webrtc_connecting(ctx, event),
            ),
        );
    }

    async fn on_webrtc_connected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        tokio::join!(
            call_observation_hook(
                "on_webrtc_connected:first",
                self.first.on_webrtc_connected(ctx, event),
            ),
            call_observation_hook(
                "on_webrtc_connected:second",
                self.second.on_webrtc_connected(ctx, event),
            ),
        );
    }

    async fn on_webrtc_disconnected(&self, ctx: &RuntimeContext, event: &PeerEvent) {
        tokio::join!(
            call_observation_hook(
                "on_webrtc_disconnected:first",
                self.first.on_webrtc_disconnected(ctx, event),
            ),
            call_observation_hook(
                "on_webrtc_disconnected:second",
                self.second.on_webrtc_disconnected(ctx, event),
            ),
        );
    }

    async fn on_credential_renewed(&self, ctx: &RuntimeContext, event: &CredentialEvent) {
        tokio::join!(
            call_observation_hook(
                "on_credential_renewed:first",
                self.first.on_credential_renewed(ctx, event),
            ),
            call_observation_hook(
                "on_credential_renewed:second",
                self.second.on_credential_renewed(ctx, event),
            ),
        );
    }

    async fn on_credential_expiring(&self, ctx: &RuntimeContext, event: &CredentialEvent) {
        tokio::join!(
            call_observation_hook(
                "on_credential_expiring:first",
                self.first.on_credential_expiring(ctx, event),
            ),
            call_observation_hook(
                "on_credential_expiring:second",
                self.second.on_credential_expiring(ctx, event),
            ),
        );
    }

    async fn on_mailbox_backpressure(&self, ctx: &RuntimeContext, event: &BackpressureEvent) {
        tokio::join!(
            call_observation_hook(
                "on_mailbox_backpressure:first",
                self.first.on_mailbox_backpressure(ctx, event),
            ),
            call_observation_hook(
                "on_mailbox_backpressure:second",
                self.second.on_mailbox_backpressure(ctx, event),
            ),
        );
    }
}

fn call_both_lifecycle(first: ActorResult<()>, second: ActorResult<()>) -> ActorResult<()> {
    match (first, second) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
        (Err(first), Err(second)) => Err(ActrError::Internal(format!(
            "multiple workload hook observers failed: {first}; {second}"
        ))),
    }
}

/// Future type produced by a [`HookContextBuilder`].
pub(crate) type HookContextFut = Pin<Box<dyn Future<Output = Option<RuntimeContext>> + Send>>;

/// Lazy builder that produces a `RuntimeContext` (or `None`, when the node
/// does not yet have an identity) used by hook callbacks to invoke the
/// observer trait methods.
pub(crate) type HookContextBuilder = Arc<dyn Fn() -> HookContextFut + Send + Sync + 'static>;

/// Await an observation hook with panic isolation.
///
/// The FIFO dispatcher uses this for each event, while chained observers use
/// it per branch so one panicking observer does not cancel the other branch.
async fn call_observation_hook<F>(label: &'static str, fut: F)
where
    F: Future<Output = ()>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(()) => {}
        Err(panic_payload) => {
            let info = extract_panic_info(panic_payload);
            tracing::error!(
                hook = label,
                panic = %info,
                "workload hook panicked; isolated",
            );
        }
    }
}

/// Await a lifecycle hook with panic isolation and preserve fallible results.
///
/// Startup/shutdown code uses this helper so it can decide whether a lifecycle
/// hook failure should abort or only be logged.
#[allow(dead_code)]
pub(crate) async fn call_lifecycle_hook<F>(label: &'static str, fut: F) -> ActorResult<()>
where
    F: Future<Output = ActorResult<()>>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(result) => result,
        Err(panic_payload) => {
            let info = extract_panic_info(panic_payload);
            Err(ActrError::Internal(format!("{label} panicked: {info}")))
        }
    }
}

fn extract_panic_info(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic>".to_string()
    }
}

/// Build a [`HookCallback`] that logs framework tracing defaults and enqueues
/// every [`HookEvent`] into one FIFO dispatcher task.
///
/// The event-source wiring (`WebSocketSignalingClient`,
/// `WebRtcCoordinator`, `WebSocketGate`, mailbox loop, credential flow)
/// installs the returned closure via `set_hook_callback` so that every
/// state change produces a structured tracing record at the appropriate
/// level regardless of whether a user observer is plugged in.
///
/// Enqueueing is synchronous and non-blocking, so slow foreign observers do
/// not stall connection state machines. The single consumer awaits each
/// observer call before starting the next event, which preserves emission
/// order across Rust tasks and async FFI callback executors. The queue is
/// intentionally unbounded because hook traffic is low-volume and terminal
/// state events must never be dropped.
///
/// `ctx_builder` lazily constructs the `RuntimeContext` needed by
/// observer callbacks; for initial-connection signaling events (where the
/// node has not yet acquired an identity) callers should return `None`.
pub(crate) fn build_hook_callback(
    observer: Option<WorkloadHookObserverRef>,
    ctx_builder: HookContextBuilder,
) -> HookCallback {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(run_hook_dispatcher(receiver, observer, ctx_builder));

    Arc::new(move |event: HookEvent| {
        let label = hook_event_label(&event);
        log_hook_event(&event);
        if sender.send(event).is_err() {
            tracing::error!(
                hook = label,
                "workload hook dispatcher stopped; event was not delivered"
            );
        }
        Box::pin(async {}) as Pin<Box<dyn Future<Output = ()> + Send>>
    })
}

async fn run_hook_dispatcher(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<HookEvent>,
    observer: Option<WorkloadHookObserverRef>,
    ctx_builder: HookContextBuilder,
) {
    while let Some(event) = receiver.recv().await {
        let label = hook_event_label(&event);
        call_observation_hook(
            label,
            deliver_hook_event(observer.as_ref(), &ctx_builder, event),
        )
        .await;
    }
}

async fn deliver_hook_event(
    observer: Option<&WorkloadHookObserverRef>,
    ctx_builder: &HookContextBuilder,
    event: HookEvent,
) {
    let ctx_opt = ctx_builder().await;

    match event {
        HookEvent::SignalingConnectStart { .. } => {
            if let Some(observer) = observer {
                observer.on_signaling_connecting(ctx_opt.as_ref()).await;
            }
        }
        HookEvent::SignalingConnected => {
            if let Some(observer) = observer {
                observer.on_signaling_connected(ctx_opt.as_ref()).await;
            }
        }
        HookEvent::SignalingDisconnected => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                observer.on_signaling_disconnected(ctx).await;
            }
        }
        HookEvent::WebRtcConnectStart { peer_id } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = PeerEvent {
                    peer: peer_id,
                    relayed: None,
                    status: Some(WebRtcPeerStatus::Connecting),
                };
                observer.on_webrtc_connecting(ctx, &event).await;
            }
        }
        HookEvent::WebRtcConnected { peer_id, relayed } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = PeerEvent {
                    peer: peer_id,
                    relayed: Some(relayed),
                    status: Some(WebRtcPeerStatus::Connected),
                };
                observer.on_webrtc_connected(ctx, &event).await;
            }
        }
        HookEvent::WebRtcDisconnected { peer_id, status } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = PeerEvent {
                    peer: peer_id,
                    relayed: None,
                    status: Some(status),
                };
                observer.on_webrtc_disconnected(ctx, &event).await;
            }
        }
        HookEvent::DataChunkDeliveryUncertain {
            stream_id,
            session_id,
            reason,
        } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = ErrorEvent::now(
                    ActrError::Unavailable(
                        "data stream delivery uncertain after WebRTC disconnect".to_string(),
                    ),
                    ErrorCategory::DataChunkDeliveryUncertain,
                    format!("stream_id={stream_id}; session_id={session_id}; reason={reason}"),
                );
                if let Err(e) = observer.on_error(ctx, &event).await {
                    tracing::warn!(error = %e, "workload on_error returned Err");
                }
            }
        }
        HookEvent::WebSocketConnectStart { peer_id } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = PeerEvent {
                    peer: peer_id,
                    relayed: None,
                    status: None,
                };
                observer.on_websocket_connecting(ctx, &event).await;
            }
        }
        HookEvent::WebSocketConnected { peer_id } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = PeerEvent {
                    peer: peer_id,
                    relayed: None,
                    status: None,
                };
                observer.on_websocket_connected(ctx, &event).await;
            }
        }
        HookEvent::WebSocketDisconnected { peer_id } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = PeerEvent {
                    peer: peer_id,
                    relayed: None,
                    status: None,
                };
                observer.on_websocket_disconnected(ctx, &event).await;
            }
        }
        HookEvent::CredentialRenewed { new_expiry } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = CredentialEvent { new_expiry };
                observer.on_credential_renewed(ctx, &event).await;
            }
        }
        HookEvent::CredentialExpiring { new_expiry } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = CredentialEvent { new_expiry };
                observer.on_credential_expiring(ctx, &event).await;
            }
        }
        HookEvent::MailboxBackpressure {
            queue_len,
            threshold,
        } => {
            if let (Some(ctx), Some(observer)) = (ctx_opt.as_ref(), observer) {
                let event = BackpressureEvent {
                    queue_len,
                    threshold,
                };
                observer.on_mailbox_backpressure(ctx, &event).await;
            }
        }
    }
}

fn hook_event_label(event: &HookEvent) -> &'static str {
    match event {
        HookEvent::SignalingConnectStart { .. } => "on_signaling_connecting",
        HookEvent::SignalingConnected => "on_signaling_connected",
        HookEvent::SignalingDisconnected => "on_signaling_disconnected",
        HookEvent::WebRtcConnectStart { .. } => "on_webrtc_connecting",
        HookEvent::WebRtcConnected { .. } => "on_webrtc_connected",
        HookEvent::WebRtcDisconnected { .. } => "on_webrtc_disconnected",
        HookEvent::DataChunkDeliveryUncertain { .. } => "on_error",
        HookEvent::WebSocketConnectStart { .. } => "on_websocket_connecting",
        HookEvent::WebSocketConnected { .. } => "on_websocket_connected",
        HookEvent::WebSocketDisconnected { .. } => "on_websocket_disconnected",
        HookEvent::CredentialRenewed { .. } => "on_credential_renewed",
        HookEvent::CredentialExpiring { .. } => "on_credential_expiring",
        HookEvent::MailboxBackpressure { .. } => "on_mailbox_backpressure",
    }
}

/// Emit the framework-default tracing record for a hook event.
fn log_hook_event(event: &HookEvent) {
    match event {
        HookEvent::SignalingConnectStart { attempt } => {
            tracing::debug!(attempt = *attempt, "signaling connecting");
        }
        HookEvent::SignalingConnected => tracing::info!("signaling connected"),
        HookEvent::SignalingDisconnected => tracing::warn!("signaling disconnected"),
        HookEvent::WebRtcConnectStart { peer_id } => {
            tracing::debug!(peer = %peer_id, "webrtc connecting");
        }
        HookEvent::WebRtcConnected { peer_id, relayed } => {
            tracing::info!(peer = %peer_id, relayed = *relayed, "webrtc connected");
        }
        HookEvent::WebRtcDisconnected { peer_id, status } => {
            tracing::warn!(peer = %peer_id, status = ?status, "webrtc disconnected");
        }
        HookEvent::DataChunkDeliveryUncertain {
            stream_id,
            session_id,
            reason,
        } => {
            tracing::warn!(
                stream_id = %stream_id,
                session_id = *session_id,
                reason = %reason,
                "data stream delivery uncertain",
            );
        }
        HookEvent::WebSocketConnectStart { peer_id } => {
            tracing::debug!(peer = %peer_id, "websocket connecting");
        }
        HookEvent::WebSocketConnected { peer_id } => {
            tracing::info!(peer = %peer_id, "websocket connected");
        }
        HookEvent::WebSocketDisconnected { peer_id } => {
            tracing::warn!(peer = %peer_id, "websocket disconnected");
        }
        HookEvent::CredentialRenewed { new_expiry } => {
            tracing::info!(new_expiry = ?new_expiry, "credential renewed");
        }
        HookEvent::CredentialExpiring { new_expiry } => {
            tracing::warn!(new_expiry = ?new_expiry, "credential expiring soon");
        }
        HookEvent::MailboxBackpressure {
            queue_len,
            threshold,
        } => {
            tracing::warn!(
                queue_len = *queue_len,
                threshold = *threshold,
                "mailbox backpressure",
            );
        }
    }
}

#[cfg(test)]
#[path = "hooks_tests.rs"]
mod tests;
