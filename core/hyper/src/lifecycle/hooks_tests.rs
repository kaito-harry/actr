use super::*;
use crate::context::RuntimeContext;
use crate::inbound::{DataChunkRegistry, MediaFrameRegistry};
use crate::outbound::{Gate, HostGate};
use crate::transport::HostTransport;
use crate::wire::webrtc::{
    ReconnectConfig, SignalingClient, SignalingConfig, WebSocketSignalingClient,
};
use actr_framework::Context as _;
use actr_protocol::{AIdCredential, ActrId, ActrType, Realm};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc};

#[tokio::test(flavor = "current_thread")]
async fn call_observation_hook_survives_panic() {
    call_observation_hook("test", async {
        panic!("intentional");
    })
    .await;
    // If we got here without aborting, the panic was isolated.
}

#[tokio::test(flavor = "current_thread")]
async fn call_observation_hook_runs_clean_body() {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    call_observation_hook("test", async move {
        let _ = tx.send(());
    })
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(1), rx)
        .await
        .expect("hook did not run")
        .expect("sender dropped");
}

#[tokio::test(flavor = "current_thread")]
async fn call_lifecycle_hook_propagates_error() {
    let err = call_lifecycle_hook("on_start", async {
        Err(ActrError::Internal("startup failed".to_string()))
    })
    .await
    .expect_err("lifecycle error must propagate");

    match err {
        ActrError::Internal(msg) => {
            assert!(msg.contains("startup failed"), "unexpected message: {msg}");
        }
        other => panic!("expected Internal, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn call_lifecycle_hook_converts_panic_to_error() {
    let err = call_lifecycle_hook("on_start", async {
        panic!("startup panic");
    })
    .await
    .expect_err("panic must become lifecycle error");

    match err {
        ActrError::Internal(msg) => {
            assert!(
                msg.contains("on_start panicked: startup panic"),
                "unexpected message: {msg}"
            );
        }
        other => panic!("expected Internal, got {other:?}"),
    }
}

fn test_actr_id(serial_number: u64) -> ActrId {
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

fn test_credential() -> AIdCredential {
    AIdCredential {
        key_id: 1,
        claims: bytes::Bytes::from_static(b"claims"),
        signature: bytes::Bytes::from(vec![0; 64]),
    }
}

fn test_runtime_context() -> RuntimeContext {
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
        test_actr_id(1),
        None,
        "hook-test".to_string(),
        inproc_gate,
        None,
        Arc::new(DataChunkRegistry::new()),
        Arc::new(MediaFrameRegistry::new()),
        signaling_client,
        test_credential(),
        None,
        Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        None,
        0,
    )
}

struct ErrorRecorder {
    tx: mpsc::UnboundedSender<ErrorEvent>,
}

#[async_trait::async_trait]
impl WorkloadHookObserver for ErrorRecorder {
    async fn on_error(&self, _ctx: &RuntimeContext, event: &ErrorEvent) -> ActorResult<()> {
        let _ = self.tx.send(event.clone());
        Ok(())
    }
}

struct RecordingObserver {
    tx: mpsc::UnboundedSender<String>,
}

fn relayed_label(relayed: Option<bool>) -> &'static str {
    match relayed {
        Some(true) => "true",
        Some(false) => "false",
        None => "none",
    }
}

#[async_trait::async_trait]
impl WorkloadHookObserver for RecordingObserver {
    async fn on_signaling_connecting(&self, ctx: Option<&RuntimeContext>) {
        let label = if ctx.is_some() { "some" } else { "none" };
        let _ = self.tx.send(format!("on_signaling_connecting:ctx={label}"));
    }

    async fn on_signaling_connected(&self, ctx: Option<&RuntimeContext>) {
        let label = if ctx.is_some() { "some" } else { "none" };
        let _ = self.tx.send(format!("on_signaling_connected:ctx={label}"));
    }

    async fn on_signaling_disconnected(&self, ctx: &RuntimeContext) {
        let _ = self.tx.send(format!(
            "on_signaling_disconnected:self={}",
            ctx.self_id().serial_number
        ));
    }

    async fn on_websocket_connecting(&self, _ctx: &RuntimeContext, event: &PeerEvent) {
        let _ = self.tx.send(format!(
            "on_websocket_connecting:peer={}:relayed={}",
            event.peer.serial_number,
            relayed_label(event.relayed)
        ));
    }

    async fn on_websocket_connected(&self, _ctx: &RuntimeContext, event: &PeerEvent) {
        let _ = self.tx.send(format!(
            "on_websocket_connected:peer={}:relayed={}",
            event.peer.serial_number,
            relayed_label(event.relayed)
        ));
    }

    async fn on_websocket_disconnected(&self, _ctx: &RuntimeContext, event: &PeerEvent) {
        let _ = self.tx.send(format!(
            "on_websocket_disconnected:peer={}:relayed={}",
            event.peer.serial_number,
            relayed_label(event.relayed)
        ));
    }

    async fn on_webrtc_connecting(&self, _ctx: &RuntimeContext, event: &PeerEvent) {
        let _ = self.tx.send(format!(
            "on_webrtc_connecting:peer={}:relayed={}",
            event.peer.serial_number,
            relayed_label(event.relayed)
        ));
    }

    async fn on_webrtc_connected(&self, _ctx: &RuntimeContext, event: &PeerEvent) {
        let _ = self.tx.send(format!(
            "on_webrtc_connected:peer={}:relayed={}",
            event.peer.serial_number,
            relayed_label(event.relayed)
        ));
    }

    async fn on_webrtc_disconnected(&self, _ctx: &RuntimeContext, event: &PeerEvent) {
        let _ = self.tx.send(format!(
            "on_webrtc_disconnected:peer={}:relayed={}",
            event.peer.serial_number,
            relayed_label(event.relayed)
        ));
    }

    async fn on_credential_renewed(&self, _ctx: &RuntimeContext, event: &CredentialEvent) {
        let secs = event
            .new_expiry
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let _ = self.tx.send(format!("on_credential_renewed:expiry={secs}"));
    }

    async fn on_credential_expiring(&self, _ctx: &RuntimeContext, event: &CredentialEvent) {
        let secs = event
            .new_expiry
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let _ = self
            .tx
            .send(format!("on_credential_expiring:expiry={secs}"));
    }

    async fn on_mailbox_backpressure(&self, _ctx: &RuntimeContext, event: &BackpressureEvent) {
        let _ = self.tx.send(format!(
            "on_mailbox_backpressure:queue_len={}:threshold={}",
            event.queue_len, event.threshold
        ));
    }
}

struct BlockingWebRtcObserver {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl WorkloadHookObserver for BlockingWebRtcObserver {
    async fn on_webrtc_connected(&self, _ctx: &RuntimeContext, _event: &PeerEvent) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}

struct BlockingDisconnectObserver {
    statuses: mpsc::UnboundedSender<WebRtcPeerStatus>,
    recovering_entered: Arc<Notify>,
    release_recovering: Arc<Notify>,
}

#[async_trait::async_trait]
impl WorkloadHookObserver for BlockingDisconnectObserver {
    async fn on_webrtc_disconnected(&self, _ctx: &RuntimeContext, event: &PeerEvent) {
        let status = event.status.expect("WebRTC status should be present");
        let _ = self.statuses.send(status);
        if status == WebRtcPeerStatus::Recovering {
            self.recovering_entered.notify_one();
            self.release_recovering.notified().await;
        }
    }
}

async fn expect_recorded(rx: &mut mpsc::UnboundedReceiver<String>, expected: &'static str) {
    let observed = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("observer hook was not called")
        .expect("observer channel dropped");
    assert_eq!(observed, expected);
}

#[tokio::test(flavor = "current_thread")]
async fn hook_callback_routes_observation_hooks_to_observer_with_payload() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let observer: WorkloadHookObserverRef = Arc::new(RecordingObserver { tx });
    let ctx = test_runtime_context();
    let ctx_builder: HookContextBuilder = Arc::new(move || {
        let ctx = ctx.clone();
        Box::pin(async move { Some(ctx) })
    });
    let cb = build_hook_callback(Some(observer), ctx_builder);
    let expiry = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_725_000_000);

    let cases = vec![
        (
            HookEvent::SignalingConnectStart { attempt: 3 },
            "on_signaling_connecting:ctx=some",
        ),
        (
            HookEvent::SignalingConnected,
            "on_signaling_connected:ctx=some",
        ),
        (
            HookEvent::SignalingDisconnected,
            "on_signaling_disconnected:self=1",
        ),
        (
            HookEvent::WebRtcConnectStart {
                peer_id: test_actr_id(2),
            },
            "on_webrtc_connecting:peer=2:relayed=none",
        ),
        (
            HookEvent::WebRtcConnected {
                peer_id: test_actr_id(3),
                relayed: false,
            },
            "on_webrtc_connected:peer=3:relayed=false",
        ),
        (
            HookEvent::WebRtcDisconnected {
                peer_id: test_actr_id(4),
                status: WebRtcPeerStatus::Recovering,
            },
            "on_webrtc_disconnected:peer=4:relayed=none",
        ),
        (
            HookEvent::WebSocketConnectStart {
                peer_id: test_actr_id(5),
            },
            "on_websocket_connecting:peer=5:relayed=none",
        ),
        (
            HookEvent::WebSocketConnected {
                peer_id: test_actr_id(6),
            },
            "on_websocket_connected:peer=6:relayed=none",
        ),
        (
            HookEvent::WebSocketDisconnected {
                peer_id: test_actr_id(7),
            },
            "on_websocket_disconnected:peer=7:relayed=none",
        ),
        (
            HookEvent::CredentialRenewed { new_expiry: expiry },
            "on_credential_renewed:expiry=1725000000",
        ),
        (
            HookEvent::CredentialExpiring { new_expiry: expiry },
            "on_credential_expiring:expiry=1725000000",
        ),
        (
            HookEvent::MailboxBackpressure {
                queue_len: 9,
                threshold: 4,
            },
            "on_mailbox_backpressure:queue_len=9:threshold=4",
        ),
    ];

    for (event, expected) in cases {
        cb(event).await;
        expect_recorded(&mut rx, expected).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn hook_callback_passes_none_for_early_signaling_context() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let observer: WorkloadHookObserverRef = Arc::new(RecordingObserver { tx });
    let ctx_builder: HookContextBuilder = Arc::new(|| Box::pin(async { None }));
    let cb = build_hook_callback(Some(observer), ctx_builder);

    cb(HookEvent::SignalingConnectStart { attempt: 1 }).await;
    expect_recorded(&mut rx, "on_signaling_connecting:ctx=none").await;

    cb(HookEvent::SignalingConnected).await;
    expect_recorded(&mut rx, "on_signaling_connected:ctx=none").await;
}

#[tokio::test(flavor = "current_thread")]
async fn hook_callback_invokes_linked_observer_once_per_event() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let observer: WorkloadHookObserverRef = Arc::new(RecordingObserver { tx });
    let ctx = test_runtime_context();
    let ctx_builder: HookContextBuilder = Arc::new(move || {
        let ctx = ctx.clone();
        Box::pin(async move { Some(ctx) })
    });
    let cb = build_hook_callback(Some(observer), ctx_builder);

    cb(HookEvent::WebSocketConnected {
        peer_id: test_actr_id(42),
    })
    .await;

    expect_recorded(&mut rx, "on_websocket_connected:peer=42:relayed=none").await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(
        rx.try_recv().is_err(),
        "observer should receive exactly one hook event"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn chained_observation_hooks_run_branches_concurrently() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let first: WorkloadHookObserverRef = Arc::new(BlockingWebRtcObserver {
        entered: entered.clone(),
        release: release.clone(),
    });

    let (tx, mut rx) = mpsc::unbounded_channel();
    let second: WorkloadHookObserverRef = Arc::new(RecordingObserver { tx });
    let observer =
        chain_observers(Some(first), Some(second)).expect("chained observer should exist");
    let ctx = test_runtime_context();
    let event = PeerEvent {
        peer: test_actr_id(10),
        relayed: Some(false),
        status: Some(WebRtcPeerStatus::Connected),
    };

    let invocation = tokio::spawn(async move {
        observer.on_webrtc_connected(&ctx, &event).await;
    });

    tokio::time::timeout(std::time::Duration::from_secs(1), entered.notified())
        .await
        .expect("first observer should still be invoked");
    expect_recorded(&mut rx, "on_webrtc_connected:peer=10:relayed=false").await;
    assert!(
        !invocation.is_finished(),
        "chained observer must await the blocked branch before the next event"
    );
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), invocation)
        .await
        .expect("chained observer did not finish after release")
        .expect("chained observer task panicked");
}

#[tokio::test(flavor = "current_thread")]
async fn hook_dispatcher_preserves_recovering_then_idle_order() {
    let (statuses, mut status_rx) = mpsc::unbounded_channel();
    let recovering_entered = Arc::new(Notify::new());
    let release_recovering = Arc::new(Notify::new());
    let observer: WorkloadHookObserverRef = Arc::new(BlockingDisconnectObserver {
        statuses,
        recovering_entered: recovering_entered.clone(),
        release_recovering: release_recovering.clone(),
    });
    let ctx = test_runtime_context();
    let ctx_builder: HookContextBuilder = Arc::new(move || {
        let ctx = ctx.clone();
        Box::pin(async move { Some(ctx) })
    });
    let cb = build_hook_callback(Some(observer), ctx_builder);
    let peer_id = test_actr_id(10);

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        cb(HookEvent::WebRtcDisconnected {
            peer_id: peer_id.clone(),
            status: WebRtcPeerStatus::Recovering,
        }),
    )
    .await
    .expect("event source should return after enqueueing Recovering");

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        recovering_entered.notified(),
    )
    .await
    .expect("Recovering callback was not entered");
    assert_eq!(status_rx.recv().await, Some(WebRtcPeerStatus::Recovering));

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        cb(HookEvent::WebRtcDisconnected {
            peer_id,
            status: WebRtcPeerStatus::Idle,
        }),
    )
    .await
    .expect("event source should return after enqueueing Idle");

    tokio::task::yield_now().await;
    assert!(
        status_rx.try_recv().is_err(),
        "Idle must wait for the earlier Recovering callback to complete"
    );

    release_recovering.notify_one();
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), status_rx.recv())
        .await
        .expect("Idle callback was not delivered after Recovering completed");
    assert_eq!(terminal, Some(WebRtcPeerStatus::Idle));
}

#[tokio::test(flavor = "current_thread")]
async fn data_chunk_uncertain_hook_routes_to_on_error() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let observer: WorkloadHookObserverRef = Arc::new(ErrorRecorder { tx });
    let ctx = test_runtime_context();
    let ctx_builder: HookContextBuilder = Arc::new(move || {
        let ctx = ctx.clone();
        Box::pin(async move { Some(ctx) })
    });
    let cb = build_hook_callback(Some(observer), ctx_builder);

    cb(HookEvent::DataChunkDeliveryUncertain {
        stream_id: "mobile-upload".to_string(),
        session_id: 99,
        reason: "data channel closed".to_string(),
    })
    .await;

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("on_error was not called")
        .expect("error recorder dropped");

    assert_eq!(event.category, ErrorCategory::DataChunkDeliveryUncertain);
    assert!(matches!(event.source, ActrError::Unavailable(_)));
    assert!(event.context.contains("stream_id=mobile-upload"));
    assert!(event.context.contains("session_id=99"));
    assert!(event.context.contains("reason=data channel closed"));
}

// ── chain_observers + ChainedHookObserver delivery ──────────────────────
//
// `chain_observers` composes two optional observers; the resulting
// ChainedHookObserver forwards every hook to both. These tests cover the
// None/Some combinator and all 11 forwarding arms (signaling, websocket,
// webrtc, credential, mailbox).

fn recording() -> (Arc<RecordingObserver>, mpsc::UnboundedReceiver<String>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Arc::new(RecordingObserver { tx }), rx)
}

#[test]
fn chain_observers_combinators() {
    let (a, _rxa) = recording();
    let (b, _rxb) = recording();

    // None + None → None.
    assert!(chain_observers(None, None).is_none());

    // One side present → that observer survives unwrapped. Compare via raw
    // pointer (the returned trait object must point at the same allocation
    // as the concrete Arc we passed in).
    let a_dyn: WorkloadHookObserverRef = a.clone();
    let b_dyn: WorkloadHookObserverRef = b.clone();
    let only_a = chain_observers(Some(a_dyn.clone()), None).unwrap();
    let only_b = chain_observers(None, Some(b_dyn.clone())).unwrap();
    assert!(Arc::ptr_eq(&only_a, &a_dyn));
    assert!(Arc::ptr_eq(&only_b, &b_dyn));

    // Both present → Some (wrapped in ChainedHookObserver, new Arc).
    let chained = chain_observers(Some(a_dyn), Some(b_dyn));
    assert!(chained.is_some());
}

async fn drain(rx: &mut mpsc::UnboundedReceiver<String>, n: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let s = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("observer did not record in time")
            .expect("recorder dropped");
        out.push(s);
    }
    out.sort();
    out
}

#[tokio::test(flavor = "current_thread")]
async fn chained_signaling_hooks_deliver_to_both() {
    let (a, mut rxa) = recording();
    let (b, mut rxb) = recording();
    let chained = chain_observers(Some(a), Some(b)).unwrap();
    let ctx = test_runtime_context();

    chained.on_signaling_connecting(Some(&ctx)).await;
    chained.on_signaling_connected(Some(&ctx)).await;
    chained.on_signaling_disconnected(&ctx).await;

    assert_eq!(
        drain(&mut rxa, 3).await,
        vec![
            "on_signaling_connected:ctx=some",
            "on_signaling_connecting:ctx=some",
            "on_signaling_disconnected:self=1",
        ]
    );
    assert_eq!(
        drain(&mut rxb, 3).await,
        vec![
            "on_signaling_connected:ctx=some",
            "on_signaling_connecting:ctx=some",
            "on_signaling_disconnected:self=1",
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn chained_peer_hooks_deliver_to_both() {
    let (a, mut rxa) = recording();
    let (b, mut rxb) = recording();
    let chained = chain_observers(Some(a), Some(b)).unwrap();
    let ctx = test_runtime_context();
    let peer = PeerEvent {
        peer: test_actr_id(7),
        relayed: Some(false),
        status: None,
    };

    chained.on_websocket_connecting(&ctx, &peer).await;
    chained.on_websocket_connected(&ctx, &peer).await;
    chained.on_websocket_disconnected(&ctx, &peer).await;
    chained.on_webrtc_connecting(&ctx, &peer).await;
    chained.on_webrtc_connected(&ctx, &peer).await;
    chained.on_webrtc_disconnected(&ctx, &peer).await;

    let expected = vec![
        "on_webrtc_connected:peer=7:relayed=false",
        "on_webrtc_connecting:peer=7:relayed=false",
        "on_webrtc_disconnected:peer=7:relayed=false",
        "on_websocket_connected:peer=7:relayed=false",
        "on_websocket_connecting:peer=7:relayed=false",
        "on_websocket_disconnected:peer=7:relayed=false",
    ];
    assert_eq!(drain(&mut rxa, 6).await, expected);
    assert_eq!(drain(&mut rxb, 6).await, expected);
}

#[tokio::test(flavor = "current_thread")]
async fn chained_credential_and_mailbox_hooks_deliver_to_both() {
    let (a, mut rxa) = recording();
    let (b, mut rxb) = recording();
    let chained = chain_observers(Some(a), Some(b)).unwrap();
    let ctx = test_runtime_context();

    let cred = CredentialEvent {
        new_expiry: std::time::UNIX_EPOCH,
    };
    chained.on_credential_renewed(&ctx, &cred).await;
    chained.on_credential_expiring(&ctx, &cred).await;

    let bp = BackpressureEvent {
        queue_len: 5,
        threshold: 10,
    };
    chained.on_mailbox_backpressure(&ctx, &bp).await;

    assert_eq!(
        drain(&mut rxa, 3).await,
        vec![
            "on_credential_expiring:expiry=0",
            "on_credential_renewed:expiry=0",
            "on_mailbox_backpressure:queue_len=5:threshold=10",
        ]
    );
    assert_eq!(
        drain(&mut rxb, 3).await,
        vec![
            "on_credential_expiring:expiry=0",
            "on_credential_renewed:expiry=0",
            "on_mailbox_backpressure:queue_len=5:threshold=10",
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn call_both_lifecycle_combines_results() {
    // call_both_lifecycle is private but reachable through ChainedHookObserver
    // lifecycle hooks. Drive it via on_start with Recording observers (default
    // on_start → Ok) and via ErrorRecorder to get an Err arm.
    let (a, _rxa) = recording();
    let (b, _rxb) = recording();
    let chained = chain_observers(Some(a), Some(b)).unwrap();
    let ctx = test_runtime_context();
    // Both Ok → Ok.
    assert!(chained.on_start(&ctx).await.is_ok());

    // One Err → that error propagates (ErrorRecorder returns Ok on_error, so
    // build an observer that fails on_stop via LifecycleFailing pattern inline).
    struct FailingLifecycle;
    #[async_trait::async_trait]
    impl WorkloadHookObserver for FailingLifecycle {
        async fn on_stop(&self, _ctx: &RuntimeContext) -> ActorResult<()> {
            Err(ActrError::Internal("stop failed".to_string()))
        }
    }
    let chained_err = chain_observers(
        Some(Arc::new(FailingLifecycle) as WorkloadHookObserverRef),
        Some(Arc::new(FailingLifecycle) as WorkloadHookObserverRef),
    )
    .unwrap();
    // Both Err → Internal "multiple ... failed" message.
    let err = chained_err.on_stop(&ctx).await.unwrap_err();
    match err {
        ActrError::Internal(msg) => {
            assert!(
                msg.contains("multiple"),
                "expected combined message, got: {msg}"
            );
        }
        other => panic!("expected Internal, got {other:?}"),
    }
}

// ── build_hook_callback: remaining HookEvent branches ───────────────────
//
// build_hook_callback routes each HookEvent to the observer. The
// DataChunkDeliveryUncertain arm is already covered above; these tests
// drive the WebSocket / WebRTC / Credential / Mailbox / Signaling arms
// plus the None-ctx early-return paths.

fn ctx_builder_always(ctx: RuntimeContext) -> (HookContextBuilder, RuntimeContext) {
    let for_builder = ctx.clone();
    let b: HookContextBuilder = Arc::new(move || {
        let c = for_builder.clone();
        Box::pin(async move { Some(c) })
    });
    (b, ctx)
}

fn ctx_builder_none() -> HookContextBuilder {
    Arc::new(|| Box::pin(async move { None }))
}

async fn expect_n(rx: &mut mpsc::UnboundedReceiver<String>, n: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(
            tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .expect("observer did not record in time")
                .expect("recorder dropped"),
        );
    }
    out.sort();
    out
}

#[tokio::test(flavor = "current_thread")]
async fn hook_callback_signaling_events_with_and_without_ctx() {
    let (obs, mut rx) = recording();
    let observer: WorkloadHookObserverRef = obs;
    let ctx = test_runtime_context();
    let (builder, _ctx) = ctx_builder_always(ctx);
    let cb = build_hook_callback(Some(observer), builder);

    // SignalingConnectStart + SignalingConnected fire even with ctx (they
    // accept Option<&RuntimeContext>).
    cb(HookEvent::SignalingConnectStart { attempt: 1 }).await;
    cb(HookEvent::SignalingConnected).await;
    // SignalingDisconnected requires a ctx; with one present it fires.
    cb(HookEvent::SignalingDisconnected).await;

    let rec = expect_n(&mut rx, 3).await;
    assert!(rec.iter().any(|s| s == "on_signaling_connecting:ctx=some"));
    assert!(rec.iter().any(|s| s == "on_signaling_connected:ctx=some"));
    assert!(rec.iter().any(|s| s == "on_signaling_disconnected:self=1"));

    // None ctx: SignalingDisconnected must NOT fire (no ctx → early return).
    let (obs2, mut rx2) = recording();
    let cb_none = build_hook_callback(Some(obs2 as WorkloadHookObserverRef), ctx_builder_none());
    cb_none(HookEvent::SignalingDisconnected).await;
    // Let the dispatcher process the event; none should arrive without a context.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(
        rx2.try_recv().is_err(),
        "SignalingDisconnected must not fire when ctx is None"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn hook_callback_webrtc_events_deliver_peer() {
    let (obs, mut rx) = recording();
    let ctx = test_runtime_context();
    let (builder, _ctx) = ctx_builder_always(ctx);
    let cb = build_hook_callback(Some(obs as WorkloadHookObserverRef), builder);

    cb(HookEvent::WebRtcConnectStart {
        peer_id: test_actr_id(3),
    })
    .await;
    cb(HookEvent::WebRtcConnected {
        peer_id: test_actr_id(3),
        relayed: true,
    })
    .await;
    cb(HookEvent::WebRtcDisconnected {
        peer_id: test_actr_id(3),
        status: WebRtcPeerStatus::Recovering,
    })
    .await;

    let rec = expect_n(&mut rx, 3).await;
    assert!(
        rec.iter()
            .any(|s| s == "on_webrtc_connecting:peer=3:relayed=none")
    );
    assert!(
        rec.iter()
            .any(|s| s == "on_webrtc_connected:peer=3:relayed=true")
    );
    assert!(
        rec.iter()
            .any(|s| s == "on_webrtc_disconnected:peer=3:relayed=none")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn hook_callback_websocket_events_deliver_peer() {
    let (obs, mut rx) = recording();
    let ctx = test_runtime_context();
    let (builder, _ctx) = ctx_builder_always(ctx);
    let cb = build_hook_callback(Some(obs as WorkloadHookObserverRef), builder);

    cb(HookEvent::WebSocketConnectStart {
        peer_id: test_actr_id(8),
    })
    .await;
    cb(HookEvent::WebSocketConnected {
        peer_id: test_actr_id(8),
    })
    .await;
    cb(HookEvent::WebSocketDisconnected {
        peer_id: test_actr_id(8),
    })
    .await;

    let rec = expect_n(&mut rx, 3).await;
    assert!(
        rec.iter()
            .any(|s| s == "on_websocket_connecting:peer=8:relayed=none")
    );
    assert!(
        rec.iter()
            .any(|s| s == "on_websocket_connected:peer=8:relayed=none")
    );
    assert!(
        rec.iter()
            .any(|s| s == "on_websocket_disconnected:peer=8:relayed=none")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn hook_callback_credential_and_mailbox_events_deliver() {
    let (obs, mut rx) = recording();
    let ctx = test_runtime_context();
    let (builder, _ctx) = ctx_builder_always(ctx);
    let cb = build_hook_callback(Some(obs as WorkloadHookObserverRef), builder);

    cb(HookEvent::CredentialRenewed {
        new_expiry: std::time::UNIX_EPOCH,
    })
    .await;
    cb(HookEvent::CredentialExpiring {
        new_expiry: std::time::UNIX_EPOCH,
    })
    .await;
    cb(HookEvent::MailboxBackpressure {
        queue_len: 9,
        threshold: 4,
    })
    .await;

    let rec = expect_n(&mut rx, 3).await;
    assert!(rec.iter().any(|s| s == "on_credential_renewed:expiry=0"));
    assert!(rec.iter().any(|s| s == "on_credential_expiring:expiry=0"));
    assert!(
        rec.iter()
            .any(|s| s == "on_mailbox_backpressure:queue_len=9:threshold=4")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn hook_callback_none_ctx_skips_ctx_required_events() {
    // WebRTC/WebSocket/Credential/Mailbox all require a ctx; with None they
    // must early-return without invoking the observer.
    let (obs, mut rx) = recording();
    let cb = build_hook_callback(Some(obs as WorkloadHookObserverRef), ctx_builder_none());

    cb(HookEvent::WebRtcConnected {
        peer_id: test_actr_id(1),
        relayed: false,
    })
    .await;
    cb(HookEvent::WebSocketConnected {
        peer_id: test_actr_id(1),
    })
    .await;
    cb(HookEvent::CredentialRenewed {
        new_expiry: std::time::UNIX_EPOCH,
    })
    .await;
    cb(HookEvent::MailboxBackpressure {
        queue_len: 1,
        threshold: 2,
    })
    .await;

    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(
        rx.try_recv().is_err(),
        "ctx-required events must not fire when ctx is None"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn hook_callback_without_observer_still_builds_context_for_observable_events() {
    let ctx = test_runtime_context();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_builder = calls.clone();
    let builder: HookContextBuilder = Arc::new(move || {
        let ctx = ctx.clone();
        let calls = calls_for_builder.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Some(ctx)
        })
    });
    let cb: HookCallback = build_hook_callback(None, builder);

    cb(HookEvent::SignalingConnected).await;
    cb(HookEvent::WebRtcConnected {
        peer_id: test_actr_id(1),
        relayed: true,
    })
    .await;
    cb(HookEvent::MailboxBackpressure {
        queue_len: 1,
        threshold: 2,
    })
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while calls.load(Ordering::SeqCst) < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dispatcher did not build all hook contexts");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "callback should still execute shared logging/context plumbing without an observer"
    );
}

// ── log_hook_event: every variant is accepted by the logging router ─────

#[test]
fn log_hook_event_covers_all_variants() {
    log_hook_event(&HookEvent::SignalingConnectStart { attempt: 2 });
    log_hook_event(&HookEvent::SignalingConnected);
    log_hook_event(&HookEvent::SignalingDisconnected);
    log_hook_event(&HookEvent::WebRtcConnectStart {
        peer_id: test_actr_id(1),
    });
    log_hook_event(&HookEvent::WebRtcConnected {
        peer_id: test_actr_id(1),
        relayed: true,
    });
    log_hook_event(&HookEvent::WebRtcDisconnected {
        peer_id: test_actr_id(1),
        status: WebRtcPeerStatus::Idle,
    });
    log_hook_event(&HookEvent::DataChunkDeliveryUncertain {
        stream_id: "s".into(),
        session_id: 1,
        reason: "r".into(),
    });
    log_hook_event(&HookEvent::WebSocketConnectStart {
        peer_id: test_actr_id(1),
    });
    log_hook_event(&HookEvent::WebSocketConnected {
        peer_id: test_actr_id(1),
    });
    log_hook_event(&HookEvent::WebSocketDisconnected {
        peer_id: test_actr_id(1),
    });
    log_hook_event(&HookEvent::CredentialRenewed {
        new_expiry: std::time::UNIX_EPOCH,
    });
    log_hook_event(&HookEvent::CredentialExpiring {
        new_expiry: std::time::UNIX_EPOCH,
    });
    log_hook_event(&HookEvent::MailboxBackpressure {
        queue_len: 1,
        threshold: 2,
    });
}

// ── extract_panic_info: string and non-string payloads ─────────────────

#[test]
fn extract_panic_info_handles_str_string_and_unknown() {
    let s = extract_panic_info(Box::new("boom"));
    assert_eq!(s, "boom");

    let s = extract_panic_info(Box::new("owned".to_string()));
    assert_eq!(s, "owned");

    let s = extract_panic_info(Box::new(42u64) as Box<dyn std::any::Any + Send>);
    assert_eq!(s, "<non-string panic>");
}
