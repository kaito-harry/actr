use super::*;
use crate::inbound::{DataStreamRegistry, MediaFrameRegistry};
use crate::lifecycle::hooks::{HookContextBuilder, WorkloadHookObserverRef, build_hook_callback};
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
        direction: Some(actr_protocol::Direction::Request as i32),
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
        direction: Some(actr_protocol::Direction::Request as i32),
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

// ── Workload::Linked lifecycle + dispatch branches ──────────────────────
//
// The Linked arm of on_start/on_ready/on_stop/dispatch_envelope forwards
// to the linked handle. Drive them with a real EchoWorkload adapter so the
// forwarding path (and the handle's default hooks) actually executes.

fn linked_echo() -> Workload {
    let handle: Arc<dyn LinkedWorkloadHandle> = WorkloadAdapter::new(EchoWorkload {
        suffix: "-ok".to_string(),
    });
    Workload::Linked(handle)
}

fn dummy_host_abi() -> HostAbiFn {
    Arc::new(|_op: HostOperation| Box::pin(async { HostOperationResult::Done }))
}

fn invocation(serial: u64) -> InvocationContext {
    InvocationContext {
        self_id: make_id(serial),
        caller_id: None,
        request_id: "lifecycle-test".to_string(),
    }
}

#[tokio::test]
async fn linked_lifecycle_hooks_resolve_ok() {
    let mut wl = linked_echo();
    let ctx = test_runtime_context(1);
    let abi = dummy_host_abi();

    wl.on_start(ctx.clone(), invocation(1), &abi).await.unwrap();
    wl.on_ready(ctx.clone(), invocation(1), &abi).await.unwrap();
    wl.on_stop(ctx, invocation(1), &abi).await.unwrap();
}

#[tokio::test]
async fn linked_dispatch_envelope_forwards_to_handle() {
    let mut wl = linked_echo();
    let ctx = test_runtime_context(2);
    let abi = dummy_host_abi();

    // EchoWorkload dispatch succeeds for route_key "echo" — exercises the
    // Linked forwarding arm of dispatch_envelope.
    let envelope = RpcEnvelope {
        route_key: "echo".to_string(),
        payload: Some(Bytes::from_static(b"hi")),
        ..Default::default()
    };
    let result = wl
        .dispatch_envelope(envelope, ctx, invocation(2), &abi)
        .await
        .unwrap();
    assert_eq!(result, Bytes::from_static(b"hi-ok"));
}

#[test]
fn package_hook_event_request_ids_are_unique_and_namespaced() {
    let peer = PeerEvent {
        peer: make_id(5),
        relayed: Some(true),
        status: None,
    };
    let cred = CredentialEvent {
        new_expiry: std::time::SystemTime::UNIX_EPOCH,
    };
    let bp = BackpressureEvent {
        queue_len: 4,
        threshold: 10,
    };

    let events = [
        PackageHookEvent::SignalingConnecting,
        PackageHookEvent::SignalingConnected,
        PackageHookEvent::SignalingDisconnected,
        PackageHookEvent::WebSocketConnecting(peer.clone()),
        PackageHookEvent::WebSocketConnected(peer.clone()),
        PackageHookEvent::WebSocketDisconnected(peer.clone()),
        PackageHookEvent::WebRtcConnecting(peer.clone()),
        PackageHookEvent::WebRtcConnected(peer.clone()),
        PackageHookEvent::WebRtcDisconnected(peer),
        PackageHookEvent::CredentialRenewed(cred.clone()),
        PackageHookEvent::CredentialExpiring(cred),
        PackageHookEvent::MailboxBackpressure(bp),
    ];

    let ids: Vec<&str> = events.iter().map(|e| e.request_id()).collect();

    // Every id is non-empty and lives under the hook:on_ namespace.
    for id in &ids {
        assert!(id.starts_with("hook:on_"), "unexpected id: {id}");
    }

    // All twelve ids are distinct.
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    let before = sorted.len();
    sorted.dedup();
    assert_eq!(sorted.len(), before, "request ids must be unique: {ids:?}");
}
