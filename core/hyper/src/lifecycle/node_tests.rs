use super::*;

// ── Strategy A: keyless zero-overhead scheduler gate ────────────────────────
//
// `scheduler_engaged` is the single predicate the node uses to decide whether
// to spawn the interleaved runner + conflict-key scheduler. Strategy A's whole
// promise lives here: default-on (gate on) with NO declared key must NOT engage
// the scheduler, so a keyless actor stays bit-for-bit the serial M4 path.

#[test]
fn scheduler_engaged_only_with_gate_on_and_declared_keys() {
    // Default-on + keyless ⇒ NOT engaged (the strategy A zero-overhead promise).
    assert!(
        !scheduler_engaged(true, false),
        "keyless actor must stay serial (no scheduler) even with the gate on"
    );
    // Gate on + at least one key ⇒ engaged.
    assert!(
        scheduler_engaged(true, true),
        "a declared conflict key with the gate on must engage the scheduler"
    );
    // Gate off ⇒ never engaged regardless of keys.
    assert!(!scheduler_engaged(false, false));
    assert!(!scheduler_engaged(false, true));
}

#[test]
fn empty_conflict_key_spec_is_reported_empty() {
    // The node's keyless predicate rides on `ConflictKeySpec::is_empty`.
    let keyless = crate::dispatch::ConflictKeySpec::builder()
        .build()
        .expect("build empty spec");
    assert!(keyless.is_empty(), "a spec with no rules must report empty");

    let keyed = crate::dispatch::ConflictKeySpec::builder()
        .method("svc/method", crate::dispatch::KeySource::Sender)
        .build()
        .expect("build keyed spec");
    assert!(
        !keyed.is_empty(),
        "a spec with a rule must report non-empty"
    );
}

#[derive(Debug, PartialEq, Eq)]
enum ShutdownOrderEvent {
    FirstStarted,
    FirstFinished,
    SecondStarted,
    OnStop,
}

async fn next_shutdown_event(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<ShutdownOrderEvent>,
) -> ShutdownOrderEvent {
    tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
        .await
        .expect("shutdown-order event watchdog")
        .expect("shutdown-order event channel open")
}

/// Linked actor used to prove that graceful teardown drains a durable mailbox
/// batch's same-key tail before the lifecycle barrier is allowed to run.
struct ShutdownOrderHandle {
    events: tokio::sync::mpsc::UnboundedSender<ShutdownOrderEvent>,
    release_first: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl LinkedWorkloadHandle for ShutdownOrderHandle {
    async fn dispatch(
        &self,
        envelope: RpcEnvelope,
        _ctx: Arc<crate::context::RuntimeContext>,
    ) -> ActorResult<bytes::Bytes> {
        match envelope.request_id.as_str() {
            "first" => {
                let _ = self.events.send(ShutdownOrderEvent::FirstStarted);
                self.release_first
                    .acquire()
                    .await
                    .expect("release semaphore open")
                    .forget();
                let _ = self.events.send(ShutdownOrderEvent::FirstFinished);
            }
            "second" => {
                let _ = self.events.send(ShutdownOrderEvent::SecondStarted);
            }
            other => panic!("unexpected dispatch {other}"),
        }
        Ok(bytes::Bytes::new())
    }

    async fn on_stop(&self, _ctx: &crate::context::RuntimeContext) -> ActorResult<()> {
        let _ = self.events.send(ShutdownOrderEvent::OnStop);
        Ok(())
    }
}

fn shutdown_order_dispatch(
    runner: Arc<crate::executor::ActorHandle>,
    ctx: crate::context::RuntimeContext,
    request_id: &'static str,
) -> crate::dispatch::scheduler::DispatchFn {
    Box::new(move || {
        Box::pin(async move {
            let envelope = RpcEnvelope {
                request_id: request_id.to_string(),
                ..RpcEnvelope::default()
            };
            let invocation = crate::workload::InvocationContext {
                self_id: ActrId::default(),
                caller_id: None,
                request_id: request_id.to_string(),
            };
            let host_abi: crate::workload::HostAbiFn =
                Arc::new(|_| Box::pin(async { HostOperationResult::Done }));
            runner
                .dispatch_envelope(envelope, ctx, invocation, &host_abi)
                .await
        })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_admits_active_mailbox_batch_before_on_stop() {
    let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel();
    let release_first = Arc::new(tokio::sync::Semaphore::new(0));
    let workload = Workload::Linked(Arc::new(ShutdownOrderHandle {
        events: events_tx,
        release_first: release_first.clone(),
    }) as Arc<dyn LinkedWorkloadHandle>);
    let runner = Arc::new(crate::executor::spawn_runner_with_mode(
        workload,
        crate::executor::RunnerMode::Interleaved,
        None,
    ));
    let scheduler = Arc::new(crate::dispatch::scheduler::SchedulerHandle::spawn(2, 8));
    let ctx =
        runtime_context_with_host_transport(ActrId::default(), Arc::new(HostTransport::new()));
    let key = crate::dispatch::conflict_key::ConflictKey::Scoped {
        domain: Arc::from("shutdown-order"),
        value: bytes::Bytes::from_static(b"same"),
    };
    let mailbox_batch_admission = MailboxBatchAdmission::new();
    // Models one successful dequeue: every record in the returned batch is
    // already Inflight even though only the first has reached the scheduler.
    let active_batch = mailbox_batch_admission
        .enter()
        .await
        .expect("mailbox admission starts open");

    let first = scheduler
        .submit(
            key.clone(),
            shutdown_order_dispatch(runner.clone(), ctx.clone(), "first"),
        )
        .await;
    assert_eq!(
        next_shutdown_event(&mut events_rx).await,
        ShutdownOrderEvent::FirstStarted
    );

    let stop_runner = runner.clone();
    let stop_ctx = ctx.clone();
    let stop = async move {
        let invocation = crate::workload::InvocationContext {
            self_id: ActrId::default(),
            caller_id: None,
            request_id: "lifecycle:on_stop".to_string(),
        };
        let host_abi: crate::workload::HostAbiFn =
            Arc::new(|_| Box::pin(async { HostOperationResult::Done }));
        stop_runner
            .on_stop(stop_ctx, invocation, &host_abi)
            .await
            .expect("on_stop succeeds");
    };
    let shutdown_runner = runner.clone();
    let shutdown_scheduler = scheduler.clone();
    let shutdown_mailbox_admission = mailbox_batch_admission.clone();
    let (prelude_started_tx, prelude_started_rx) = tokio::sync::oneshot::channel();
    let shutdown = tokio::spawn(async move {
        shutdown_actor_runner(
            Some(shutdown_scheduler),
            shutdown_runner,
            Box::pin(async move {
                let _ = prelude_started_tx.send(());
                shutdown_mailbox_admission.close().await;
            }),
            Box::pin(stop),
        )
        .await;
    });

    prelude_started_rx
        .await
        .expect("shutdown prelude started while the batch is active");
    // This is the tail record from the same dequeue batch. Shutdown is already
    // waiting to close mailbox admission, but scheduler intake must remain open
    // until the complete batch has been submitted.
    let second = scheduler
        .submit(
            key,
            shutdown_order_dispatch(runner.clone(), ctx.clone(), "second"),
        )
        .await;
    drop(active_batch);
    assert!(
        mailbox_batch_admission.enter().await.is_none(),
        "a batch starting after shutdown must not dequeue more durable messages"
    );

    release_first.add_permits(1);
    assert_eq!(
        next_shutdown_event(&mut events_rx).await,
        ShutdownOrderEvent::FirstFinished
    );
    assert_eq!(
        next_shutdown_event(&mut events_rx).await,
        ShutdownOrderEvent::SecondStarted,
        "the admitted same-key tail must execute during graceful drain"
    );
    assert_eq!(
        next_shutdown_event(&mut events_rx).await,
        ShutdownOrderEvent::OnStop,
        "on_stop must follow every admitted scheduler job"
    );
    assert!(first.await.expect("scheduler alive").is_ok());
    assert!(second.await.expect("scheduler alive").is_ok());
    tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
        .await
        .expect("shutdown coordinator watchdog")
        .expect("shutdown coordinator task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_shutdown_prelude_aborts_runner() {
    let workload = Workload::Linked(Arc::new(DummyLinkedHandle) as Arc<dyn LinkedWorkloadHandle>);
    let runner = Arc::new(crate::executor::spawn_runner(workload));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let pre_shutdown = async move {
        let _ = entered_tx.send(());
        std::future::pending::<()>().await;
    };

    let shutdown_runner = runner.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_actor_runner(
            None,
            shutdown_runner,
            Box::pin(pre_shutdown),
            Box::pin(async {}),
        )
        .await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .expect("shutdown prelude did not start")
        .expect("shutdown prelude signal dropped");

    shutdown.abort();
    let _ = shutdown.await;
    tokio::time::timeout(std::time::Duration::from_secs(2), runner.join())
        .await
        .expect("cancelled shutdown coordinator detached the actor runner");
}

#[test]
fn connection_not_ready_has_distinct_wire_code() {
    let err = ActrError::ConnectionNotReady(ConnectionNotReadyInfo::new(1200, 6000));

    assert_eq!(protocol_error_to_code(&err), 10011);
}

#[test]
fn connection_not_ready_wire_code_roundtrips_variant_and_retry_hint() {
    let err = wire_code_to_actr_error(
        10011,
        "connection not ready: retry_after_ms=Some(4800)".to_string(),
    );

    match err {
        ActrError::ConnectionNotReady(info) => {
            assert_eq!(info.retry_after_ms, Some(4800));
        }
        other => panic!("expected ConnectionNotReady, got {other:?}"),
    }
}

#[test]
fn connection_not_ready_wire_code_handles_missing_retry_hint() {
    let err = wire_code_to_actr_error(10011, "connection not ready".to_string());

    match err {
        ActrError::ConnectionNotReady(info) => {
            assert_eq!(info.retry_after_ms, None);
        }
        other => panic!("expected ConnectionNotReady, got {other:?}"),
    }
}

// ── host_operation_handler dispatch coverage ────────────────────────────
//
// `host_operation_handler` is the private router invoked when a guest
// workload issues a host call. It is compiled under default features but
// only reached at runtime via the wasm/dynclib engines, so it shows as
// uncovered unless we drive it directly. These tests construct a real
// RuntimeContext (in-process HostTransport, unreachable signaling) and a
// dummy linked workload, then assert the routing outcome for each
// HostOperation variant.

use crate::test_support::runtime_context_with_host_transport;
use crate::transport::HostTransport;
use crate::workload::{HostOperation, HostOperationResult, LinkedWorkloadHandle, Workload};
use actr_framework::guest::dynclib_abi::{
    DestV1, HostCallRawV1, HostCallV1, HostDiscoverV1, HostRegisterStreamV1, HostSendDataChunkV1,
    HostTellV1, HostUnregisterStreamV1, code as abi_code,
};
use actr_protocol::{AIdCredential, ActrId, PayloadType};
use std::sync::Arc;

/// Trivial linked handle that accepts every default hook and rejects
/// dispatch — sufficient because host_operation_handler only touches the
/// workload for the DataChunk callback path (not exercised here).
struct DummyLinkedHandle;
#[async_trait::async_trait]
impl LinkedWorkloadHandle for DummyLinkedHandle {}

async fn harness() -> (
    crate::context::RuntimeContext,
    Arc<crate::executor::ActorHandle>,
) {
    let ctx =
        runtime_context_with_host_transport(ActrId::default(), Arc::new(HostTransport::new()));
    let wl = Workload::Linked(Arc::new(DummyLinkedHandle) as Arc<dyn LinkedWorkloadHandle>);
    (ctx, Arc::new(crate::executor::spawn_runner(wl)))
}

async fn actorless_harness() -> (
    crate::context::RuntimeContext,
    Arc<crate::executor::ActorHandle>,
) {
    use crate::inbound::{DataChunkRegistry, MediaFrameRegistry};
    use crate::outbound::{Gate, HostGate};
    use crate::wire::webrtc::{ReconnectConfig, SignalingConfig, WebSocketSignalingClient};

    let host = Arc::new(HostTransport::new());
    let inproc = Gate::Host(Arc::new(HostGate::new(host)));
    let ctx = crate::context::RuntimeContext::new(
        ActrId::default(),
        None,
        "req".into(),
        inproc,
        None,
        Arc::new(DataChunkRegistry::new()),
        Arc::new(MediaFrameRegistry::new()),
        Arc::new(WebSocketSignalingClient::new(SignalingConfig {
            server_url: url::Url::parse("ws://127.0.0.1:9").unwrap(),
            connection_timeout: 1,
            heartbeat_interval: 30,
            reconnect_config: ReconnectConfig::default(),
            auth_config: None,
            webrtc_role: None,
        })) as Arc<dyn SignalingClient>,
        AIdCredential::default(),
        None,
        Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        None,
        0,
    );
    let wl = Workload::Linked(Arc::new(DummyLinkedHandle) as Arc<dyn LinkedWorkloadHandle>);
    (ctx, Arc::new(crate::executor::spawn_runner(wl)))
}

fn expect_error_code(res: HostOperationResult, expected: i32) {
    match res {
        HostOperationResult::Error(code) => assert_eq!(code, expected),
        other => panic!("expected HostOperationResult::Error({expected}), got {other:?}"),
    }
}

fn weak_runner(
    runner: &Arc<crate::executor::ActorHandle>,
) -> std::sync::Weak<crate::executor::ActorHandle> {
    Arc::downgrade(runner)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callraw_without_peer_returns_error_code() {
    let (ctx, wl) = actorless_harness().await;
    let res = host_operation_handler(
        ctx,
        weak_runner(&wl),
        HostOperation::CallRaw(HostCallRawV1::default()),
    )
    .await;
    // No remote actor gate is installed, so routing fails immediately.
    expect_error_code(res, abi_code::GENERIC_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_with_default_dest_returns_error() {
    let (ctx, wl) = harness().await;
    let res = host_operation_handler(
        ctx,
        weak_runner(&wl),
        HostOperation::Call(HostCallV1::default()),
    )
    .await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tell_with_default_dest_returns_error() {
    let (ctx, wl) = harness().await;
    let res = host_operation_handler(
        ctx,
        weak_runner(&wl),
        HostOperation::Tell(HostTellV1::default()),
    )
    .await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_without_realm_returns_error() {
    let (ctx, wl) = harness().await;
    let res = host_operation_handler(
        ctx,
        weak_runner(&wl),
        HostOperation::Discover(HostDiscoverV1::default()),
    )
    .await;
    // Default ActrType with unreachable signaling → discover errors.
    expect_error_code(res, abi_code::GENERIC_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_and_unregister_stream_roundtrip() {
    let (ctx, wl) = harness().await;
    let register = host_operation_handler(
        ctx.clone(),
        weak_runner(&wl),
        HostOperation::RegisterStream(HostRegisterStreamV1 {
            stream_id: "stream-1".to_string(),
        }),
    )
    .await;
    assert!(
        matches!(register, HostOperationResult::Done),
        "register_stream should succeed against the in-process transport: {register:?}"
    );

    let unregister = host_operation_handler(
        ctx,
        weak_runner(&wl),
        HostOperation::UnregisterStream(HostUnregisterStreamV1 {
            stream_id: "stream-1".to_string(),
        }),
    )
    .await;
    assert!(
        matches!(unregister, HostOperationResult::Done),
        "unregister_stream should succeed after register: {unregister:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_teardown_releases_registered_stream_callback_cycle() {
    let (ctx, wl) = harness().await;
    let registry = ctx.data_chunk_registry_for_test();
    let weak_registry = Arc::downgrade(&registry);
    let cleanup = DataChunkRegistryCleanupGuard::new(&registry);

    let register = host_operation_handler(
        ctx.clone(),
        weak_runner(&wl),
        HostOperation::RegisterStream(HostRegisterStreamV1 {
            stream_id: "cycle-stream".to_string(),
        }),
    )
    .await;
    assert!(matches!(register, HostOperationResult::Done));

    // Execute the exact callback installed by the host bridge. A linked
    // workload intentionally reports NotImplemented for package stream
    // callbacks, which proves the callback reached the live actor runner.
    let callback_result = registry
        .invoke_for_test(
            actr_protocol::DataChunk {
                stream_id: "cycle-stream".to_string(),
                sequence: 1,
                ..Default::default()
            },
            ActrId::default(),
        )
        .await
        .expect("registered callback");
    assert!(matches!(callback_result, Err(ActrError::NotImplemented(_))));

    drop(ctx);
    drop(wl);
    drop(registry);
    assert!(
        weak_registry.upgrade().is_some(),
        "the regression fixture must retain the registry through its callback"
    );

    // `Inner` owns this guard in production. Dropping it clears callbacks,
    // which drops the captured RuntimeContext and releases the registry.
    drop(cleanup);
    assert!(
        weak_registry.upgrade().is_none(),
        "node teardown must break the stream callback ownership cycle"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unregister_unknown_stream_is_idempotent_done() {
    let (ctx, wl) = harness().await;
    let res = host_operation_handler(
        ctx,
        weak_runner(&wl),
        HostOperation::UnregisterStream(HostUnregisterStreamV1 {
            stream_id: "never-registered".to_string(),
        }),
    )
    .await;
    // unregister_stream is idempotent: unknown ids resolve to Done, not Error.
    assert!(
        matches!(res, HostOperationResult::Done),
        "unregister of unknown stream should be idempotent Done: {res:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_data_chunk_rejects_non_stream_payload_type() {
    let (ctx, wl) = harness().await;
    // RpcReliable is a valid PayloadType but not a stream type → PROTOCOL_ERROR.
    let res = host_operation_handler(
        ctx,
        weak_runner(&wl),
        HostOperation::SendDataChunk(HostSendDataChunkV1 {
            dest: DestV1::workload(),
            payload_type: PayloadType::RpcReliable as i32,
            ..Default::default()
        }),
    )
    .await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_data_chunk_rejects_unknown_payload_type() {
    let (ctx, wl) = harness().await;
    let res = host_operation_handler(
        ctx,
        weak_runner(&wl),
        HostOperation::SendDataChunk(HostSendDataChunkV1 {
            dest: DestV1::workload(),
            payload_type: 9999,
            ..Default::default()
        }),
    )
    .await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_data_chunk_valid_type_routes_and_errors() {
    let (ctx, wl) = harness().await;
    // Valid stream payload type, but no route to the default dest → routing error.
    let res = host_operation_handler(
        ctx,
        weak_runner(&wl),
        HostOperation::SendDataChunk(HostSendDataChunkV1 {
            dest: DestV1::workload(),
            payload_type: PayloadType::StreamReliable as i32,
            ..Default::default()
        }),
    )
    .await;
    expect_error_code(res, abi_code::GENERIC_ERROR);
}

// ── stream_callback_host_operation_handler ──────────────────────────────
//
// The stream-callback variant rejects RegisterStream (UNSUPPORTED_OP) and
// otherwise mirrors the main handler. It takes no workload_dispatch, only a
// context + the pending HostOperation.

async fn ctx_only() -> crate::context::RuntimeContext {
    harness().await.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_callback_callraw_without_peer_errors() {
    let ctx = actorless_harness().await.0;
    let res = stream_callback_host_operation_handler(
        ctx,
        HostOperation::CallRaw(HostCallRawV1::default()),
    )
    .await;
    expect_error_code(res, abi_code::GENERIC_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_callback_call_with_default_dest_errors() {
    let ctx = ctx_only().await;
    let res =
        stream_callback_host_operation_handler(ctx, HostOperation::Call(HostCallV1::default()))
            .await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_callback_tell_with_default_dest_errors() {
    let ctx = ctx_only().await;
    let res =
        stream_callback_host_operation_handler(ctx, HostOperation::Tell(HostTellV1::default()))
            .await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_callback_discover_errors() {
    let ctx = ctx_only().await;
    let res = stream_callback_host_operation_handler(
        ctx,
        HostOperation::Discover(HostDiscoverV1::default()),
    )
    .await;
    expect_error_code(res, abi_code::GENERIC_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_callback_register_stream_is_unsupported() {
    let ctx = ctx_only().await;
    let res = stream_callback_host_operation_handler(
        ctx,
        HostOperation::RegisterStream(HostRegisterStreamV1 {
            stream_id: "s".to_string(),
        }),
    )
    .await;
    // Registering a stream from inside a stream callback is rejected.
    expect_error_code(res, abi_code::UNSUPPORTED_OP);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_callback_unregister_is_idempotent_done() {
    let ctx = ctx_only().await;
    let res = stream_callback_host_operation_handler(
        ctx,
        HostOperation::UnregisterStream(HostUnregisterStreamV1 {
            stream_id: "never".to_string(),
        }),
    )
    .await;
    assert!(
        matches!(res, HostOperationResult::Done),
        "stream-callback unregister should be idempotent Done: {res:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_callback_send_rejects_non_stream_payload_type() {
    let ctx = ctx_only().await;
    let res = stream_callback_host_operation_handler(
        ctx,
        HostOperation::SendDataChunk(HostSendDataChunkV1 {
            dest: DestV1::workload(),
            payload_type: PayloadType::RpcReliable as i32,
            ..Default::default()
        }),
    )
    .await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_callback_send_rejects_unknown_payload_type() {
    let ctx = ctx_only().await;
    let res = stream_callback_host_operation_handler(
        ctx,
        HostOperation::SendDataChunk(HostSendDataChunkV1 {
            dest: DestV1::workload(),
            payload_type: 9999,
            ..Default::default()
        }),
    )
    .await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_callback_send_valid_type_routes_and_errors() {
    let ctx = ctx_only().await;
    let res = stream_callback_host_operation_handler(
        ctx,
        HostOperation::SendDataChunk(HostSendDataChunkV1 {
            dest: DestV1::workload(),
            payload_type: PayloadType::StreamLatencyFirst as i32,
            ..Default::default()
        }),
    )
    .await;
    expect_error_code(res, abi_code::GENERIC_ERROR);
}

// ── duplicate_wait_timeout / wait_for_inflight_duplicate ────────────────

#[test]
fn duplicate_wait_timeout_uses_explicit_positive_ms() {
    assert_eq!(
        Inner::duplicate_wait_timeout(5000),
        Duration::from_millis(5000)
    );
}

#[test]
fn duplicate_wait_timeout_falls_back_to_dedup_ttl_for_nonpositive() {
    assert_eq!(Inner::duplicate_wait_timeout(0), DEDUP_TTL);
    assert_eq!(Inner::duplicate_wait_timeout(-1), DEDUP_TTL);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_for_inflight_duplicate_returns_completed_result() {
    use actr_framework::Bytes;
    let (tx, rx) = tokio::sync::watch::channel(None);
    let waiter: DedupWaiter = rx;

    tx.send(Some(Ok(Bytes::from_static(b"done")))).ok();
    let res = Inner::wait_for_inflight_duplicate(waiter, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(res, Bytes::from_static(b"done"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_for_inflight_duplicate_times_out() {
    let (_tx, rx) = tokio::sync::watch::channel(None);
    let waiter: DedupWaiter = rx;
    let err = Inner::wait_for_inflight_duplicate(waiter, Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(
        matches!(err, ActrError::Unavailable(_)),
        "expected Unavailable timeout, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_for_inflight_duplicate_propagates_error_result() {
    let (tx, rx) = tokio::sync::watch::channel(None);
    let waiter: DedupWaiter = rx;
    tx.send(Some(Err(ActrError::NotFound("missing".into()))))
        .ok();
    let err = Inner::wait_for_inflight_duplicate(waiter, Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(err, ActrError::NotFound(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_for_inflight_duplicate_observes_late_completion() {
    let (tx, rx) = tokio::sync::watch::channel(None);
    let waiter: DedupWaiter = rx;
    let tx2 = tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx2.send(Some(Ok(actr_framework::Bytes::from_static(b"late"))))
            .ok();
    });
    let res = Inner::wait_for_inflight_duplicate(waiter, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(res, actr_framework::Bytes::from_static(b"late"));
}

// ── wire_code_to_actr_error: every code arm ────────────────────────────

#[test]
fn wire_code_to_actr_error_maps_known_codes() {
    let m = "msg".to_string();
    assert!(matches!(
        wire_code_to_actr_error(10001, m.clone()),
        ActrError::Unavailable(_)
    ));
    assert!(matches!(
        wire_code_to_actr_error(10002, m.clone()),
        ActrError::TimedOut
    ));
    assert!(matches!(
        wire_code_to_actr_error(10003, m.clone()),
        ActrError::NotFound(_)
    ));
    assert!(matches!(
        wire_code_to_actr_error(10004, m.clone()),
        ActrError::PermissionDenied(_)
    ));
    assert!(matches!(
        wire_code_to_actr_error(10005, m.clone()),
        ActrError::InvalidArgument(_)
    ));
    assert!(matches!(
        wire_code_to_actr_error(10006, m.clone()),
        ActrError::UnknownRoute(_)
    ));
    assert!(matches!(
        wire_code_to_actr_error(10007, m.clone()),
        ActrError::DependencyNotFound { .. }
    ));
    assert!(matches!(
        wire_code_to_actr_error(10008, m.clone()),
        ActrError::DecodeFailure(_)
    ));
    assert!(matches!(
        wire_code_to_actr_error(10009, m.clone()),
        ActrError::NotImplemented(_)
    ));
    assert!(matches!(
        wire_code_to_actr_error(10010, m.clone()),
        ActrError::Internal(_)
    ));
}

#[test]
fn wire_code_to_actr_error_unknown_code_falls_back_to_unavailable() {
    let err = wire_code_to_actr_error(99999, "boom".into());
    match err {
        ActrError::Unavailable(msg) => {
            assert!(msg.contains("rpc error 99999"), "got: {msg}");
            assert!(msg.contains("boom"));
        }
        other => panic!("expected Unavailable fallback, got {other:?}"),
    }
}

#[test]
fn parse_connection_not_ready_retry_hint_extracts_value() {
    assert_eq!(
        parse_connection_not_ready_retry_hint("connection not ready: retry_after_ms=Some(4800)"),
        Some(4800)
    );
}

#[test]
fn parse_connection_not_ready_retry_hint_returns_none_without_marker() {
    assert_eq!(parse_connection_not_ready_retry_hint("plain message"), None);
    assert_eq!(parse_connection_not_ready_retry_hint(""), None);
}

#[test]
fn protocol_error_to_code_maps_all_variants() {
    // Every ActrError variant must map to its documented wire code.
    assert_eq!(
        protocol_error_to_code(&ActrError::Unavailable("x".into())),
        10001
    );
    assert_eq!(protocol_error_to_code(&ActrError::TimedOut), 10002);
    assert_eq!(
        protocol_error_to_code(&ActrError::NotFound("x".into())),
        10003
    );
    assert_eq!(
        protocol_error_to_code(&ActrError::PermissionDenied("x".into())),
        10004
    );
    assert_eq!(
        protocol_error_to_code(&ActrError::InvalidArgument("x".into())),
        10005
    );
    assert_eq!(
        protocol_error_to_code(&ActrError::UnknownRoute("x".into())),
        10006
    );
    assert_eq!(
        protocol_error_to_code(&ActrError::DependencyNotFound {
            service_name: String::new(),
            message: "x".into()
        }),
        10007
    );
    assert_eq!(
        protocol_error_to_code(&ActrError::DecodeFailure("x".into())),
        10008
    );
    assert_eq!(
        protocol_error_to_code(&ActrError::NotImplemented("x".into())),
        10009
    );
    assert_eq!(
        protocol_error_to_code(&ActrError::Internal("x".into())),
        10010
    );
    assert_eq!(
        protocol_error_to_code(&ActrError::ConnectionNotReady(
            ConnectionNotReadyInfo::without_retry_hint()
        )),
        10011
    );
}

// ── direction-based dispatch routing helpers ────────────────────────────

#[test]
fn dispatchable_direction_accepts_request_and_tell_only() {
    use actr_protocol::Direction;

    assert_eq!(
        Inner::dispatchable_direction(Some(Direction::Request as i32)),
        Some(Direction::Request)
    );
    assert_eq!(
        Inner::dispatchable_direction(Some(Direction::Tell as i32)),
        Some(Direction::Tell)
    );
    // Response is routed to pending maps by the gates; in a dispatch loop it
    // is a mislabel and must be dropped.
    assert_eq!(
        Inner::dispatchable_direction(Some(Direction::Response as i32)),
        None
    );
    assert_eq!(
        Inner::dispatchable_direction(Some(Direction::Unspecified as i32)),
        None
    );
    assert_eq!(Inner::dispatchable_direction(Some(99)), None);
    assert_eq!(Inner::dispatchable_direction(None), None);
}

#[test]
fn envelope_is_tell_matches_only_explicit_tell_label() {
    use actr_protocol::Direction;

    let mut envelope = RpcEnvelope {
        request_id: "tell-detect".to_string(),
        route_key: "pkg.Service.Method".to_string(),
        direction: Some(Direction::Tell as i32),
        ..Default::default()
    };
    assert!(Inner::envelope_is_tell(&envelope));

    // A zero timeout alone is NOT a tell marker anymore.
    envelope.direction = Some(Direction::Request as i32);
    envelope.timeout_ms = 0;
    assert!(!Inner::envelope_is_tell(&envelope));

    envelope.direction = None;
    assert!(!Inner::envelope_is_tell(&envelope));
}

#[test]
fn build_response_envelope_pins_zero_timeout_per_contract() {
    use actr_protocol::Direction;

    // Success RESPONSE.
    let ok_env = Inner::build_response_envelope(
        "req-ok".to_string(),
        "pkg.Service.Method".to_string(),
        Some(Bytes::from_static(b"resp")),
        None,
        None,
        None,
    );
    assert_eq!(ok_env.direction, Some(Direction::Response as i32));
    assert_eq!(
        ok_env.timeout_ms, 0,
        "RESPONSE envelopes must carry timeout_ms=0 per package.proto contract"
    );

    // Error RESPONSE also uses 0; payload/error are mutually exclusive.
    let err_env = Inner::build_response_envelope(
        "req-err".to_string(),
        "pkg.Service.Method".to_string(),
        None,
        Some(actr_protocol::ErrorResponse {
            code: 1,
            message: "boom".to_string(),
        }),
        None,
        None,
    );
    assert_eq!(err_env.timeout_ms, 0);
    assert!(err_env.payload.is_none());
    assert!(err_env.error.is_some());
}

// ── handle_incoming TELL dedup semantics ────────────────────────────────
//
// These tests build a real `Inner` (in-memory mailbox, unreachable
// signaling) around a gated linked workload so the in-flight window is
// under deterministic test control — no sleeps.

use crate::lifecycle::node::CredentialState;
use actr_framework::Bytes as FrameworkBytes;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Linked handle whose dispatch blocks until `release` flips to true,
/// signalling entry through `entered_tx` for deterministic sequencing.
struct GatedLinkedHandle {
    entered_tx: tokio::sync::mpsc::UnboundedSender<()>,
    release: tokio::sync::watch::Receiver<bool>,
    runs: Arc<AtomicUsize>,
    result: fn() -> ActorResult<FrameworkBytes>,
}

#[async_trait::async_trait]
impl LinkedWorkloadHandle for GatedLinkedHandle {
    async fn dispatch(
        &self,
        _envelope: RpcEnvelope,
        _ctx: Arc<crate::context::RuntimeContext>,
    ) -> ActorResult<FrameworkBytes> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let _ = self.entered_tx.send(());
        let mut release = self.release.clone();
        while !*release.borrow() {
            if release.changed().await.is_err() {
                break;
            }
        }
        (self.result)()
    }
}

fn dedup_test_config(dir: &tempfile::TempDir) -> actr_config::RuntimeConfig {
    actr_config::RuntimeConfig {
        package: actr_config::PackageInfo {
            name: "DedupTellActor".to_string(),
            actr_type: actr_protocol::ActrType {
                manufacturer: "test-mfr".to_string(),
                name: "DedupTellActor".to_string(),
                version: "0.1.0".to_string(),
            },
            description: None,
            authors: vec![],
            license: None,
        },
        signaling_url: url::Url::parse("ws://127.0.0.1:9/signaling/ws").unwrap(),
        realm: actr_protocol::Realm { realm_id: 7 },
        ais_endpoint: "http://127.0.0.1:9/ais".to_string(),
        realm_secret: None,
        visible_in_discovery: false,
        acl: None,
        mailbox_path: None,
        scripts: std::collections::HashMap::new(),
        webrtc: actr_config::WebRtcConfig::default(),
        websocket_listen_port: None,
        websocket_advertised_host: None,
        observability: actr_config::ObservabilityConfig {
            filter_level: "info".to_string(),
            tracing_enabled: false,
            tracing_endpoint: "http://127.0.0.1:9".to_string(),
            tracing_service_name: "dedup-tell-test".to_string(),
        },
        config_dir: dir.path().to_path_buf(),
        trust: vec![],
        package_path: None,
        web: None,
    }
}

struct TellDedupHarness {
    inner: Arc<Inner>,
    entered_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    release_tx: tokio::sync::watch::Sender<bool>,
    runs: Arc<AtomicUsize>,
    _dir: tempfile::TempDir,
}

async fn tell_dedup_harness(result: fn() -> ActorResult<FrameworkBytes>) -> TellDedupHarness {
    let dir = tempfile::TempDir::new().unwrap();
    let (entered_tx, entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = tokio::sync::watch::channel(false);
    let runs = Arc::new(AtomicUsize::new(0));

    let handle = GatedLinkedHandle {
        entered_tx,
        release: release_rx,
        runs: runs.clone(),
        result,
    };
    let workload = Workload::Linked(Arc::new(handle) as Arc<dyn LinkedWorkloadHandle>);
    let config = dedup_test_config(&dir);
    let actor_id = ActrId {
        realm: config.realm,
        serial_number: 1,
        r#type: config.package.actr_type.clone(),
    };

    let mut inner = Inner::build(
        config,
        workload,
        None,
        None,
        100,
        Duration::from_secs(60),
        crate::config::DispatchConcurrency::default(),
        None,
    )
    .await
    .expect("Inner::build must succeed with in-memory mailbox");
    // handle_incoming requires post-start identity/credential state; set it
    // directly instead of driving the full registration path.
    inner.actor_id = Some(actor_id);
    inner.credential_state = Some(CredentialState::new(AIdCredential::default(), None, None));

    TellDedupHarness {
        inner: Arc::new(inner),
        entered_rx,
        release_tx,
        runs,
        _dir: dir,
    }
}

fn tell_envelope(request_id: &str) -> RpcEnvelope {
    RpcEnvelope {
        request_id: request_id.to_string(),
        route_key: "pkg.Service.Method".to_string(),
        payload: Some(FrameworkBytes::from_static(b"tell-payload")),
        error: None,
        direction: Some(Direction::Tell as i32),
        traceparent: None,
        tracestate: None,
        metadata: vec![],
        // Documented filler for TELL; must not influence dedup behavior.
        timeout_ms: 0,
    }
}

/// A duplicate TELL arriving while the original is still in flight is
/// dropped immediately — it must not wait `duplicate_wait_timeout` (which
/// maps 0 to the 30 s DEDUP_TTL) and the handler must run exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_inflight_tell_is_dropped_immediately() {
    let mut h = tell_dedup_harness(|| Ok(FrameworkBytes::from_static(b"unsent-response"))).await;
    let envelope = tell_envelope("dup-tell-inflight");

    // Task A: original tell, blocks inside the handler on the release gate.
    let inner_a = h.inner.clone();
    let env_a = envelope.clone();
    let original = tokio::spawn(async move { inner_a.handle_incoming(env_a, None).await });

    // Deterministic: wait until the handler has actually been entered.
    tokio::time::timeout(Duration::from_secs(5), h.entered_rx.recv())
        .await
        .expect("handler must be entered")
        .expect("entered channel must stay open");

    // Duplicate while in flight: must resolve without waiting for the
    // original (the release gate is still closed, so any wait would hang
    // until this test's own 5 s guard fires).
    let duplicate = tokio::time::timeout(
        Duration::from_secs(5),
        h.inner.handle_incoming(envelope.clone(), None),
    )
    .await
    .expect("duplicate in-flight tell must return immediately, not wait for the original")
    .expect("duplicate tell drop must resolve Ok");
    assert!(
        duplicate.is_empty(),
        "dropped duplicate tell must carry no payload"
    );
    assert_eq!(
        h.runs.load(Ordering::SeqCst),
        1,
        "duplicate tell must not re-enter the handler"
    );

    // Release the original and let it complete.
    h.release_tx.send(true).unwrap();
    let original_result = original.await.unwrap().unwrap();
    assert_eq!(
        original_result,
        FrameworkBytes::from_static(b"unsent-response"),
        "the original tell still returns the handler result to its loop"
    );

    // Post-completion duplicate: served from the dedup cache, which stores
    // EMPTY bytes for a successful tell (the response is never sent, so the
    // unsent payload is not retained for the DEDUP_TTL).
    let cached = h.inner.handle_incoming(envelope, None).await.unwrap();
    assert!(
        cached.is_empty(),
        "completed tell must cache empty bytes, not the unsent response"
    );
    assert_eq!(
        h.runs.load(Ordering::SeqCst),
        1,
        "post-completion duplicate tell must not re-run the handler"
    );
}

/// A TELL whose handler fails still completes its dedup entry (with the
/// error), so a retry with the same request_id observes the failure instead
/// of re-running the handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tell_handler_error_completes_dedup_entry() {
    let mut h =
        tell_dedup_harness(|| Err(ActrError::Internal("tell handler failed".to_string()))).await;
    let envelope = tell_envelope("dup-tell-error");

    let inner_a = h.inner.clone();
    let env_a = envelope.clone();
    let original = tokio::spawn(async move { inner_a.handle_incoming(env_a, None).await });

    tokio::time::timeout(Duration::from_secs(5), h.entered_rx.recv())
        .await
        .expect("handler must be entered")
        .expect("entered channel must stay open");
    h.release_tx.send(true).unwrap();

    let original_err = original.await.unwrap().unwrap_err();
    assert!(matches!(original_err, ActrError::Internal(_)));

    // Retry with the same request_id: served from the dedup cache (the
    // recorded error), handler not re-entered.
    let retried = h.inner.handle_incoming(envelope, None).await;
    assert!(
        matches!(retried, Err(ActrError::Internal(_))),
        "retried tell must observe the cached handler error, got {retried:?}"
    );
    assert_eq!(
        h.runs.load(Ordering::SeqCst),
        1,
        "retried tell must not re-run the failed handler"
    );
}

/// Interop pin: a REQUEST arriving with `timeout_ms == 0` (buggy or
/// pre-contract sender) is still dispatched and answered — receiver-side
/// permissiveness is part of the #254 contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_with_zero_timeout_is_still_dispatched() {
    let mut h = tell_dedup_harness(|| Ok(FrameworkBytes::from_static(b"answered"))).await;
    let mut envelope = tell_envelope("zero-timeout-request");
    envelope.direction = Some(Direction::Request as i32);
    envelope.timeout_ms = 0;

    let inner_a = h.inner.clone();
    let env_a = envelope.clone();
    let call = tokio::spawn(async move { inner_a.handle_incoming(env_a, None).await });

    tokio::time::timeout(Duration::from_secs(5), h.entered_rx.recv())
        .await
        .expect("zero-timeout request must still reach the handler")
        .expect("entered channel must stay open");
    h.release_tx.send(true).unwrap();

    let response = call.await.unwrap().unwrap();
    assert_eq!(
        response,
        FrameworkBytes::from_static(b"answered"),
        "a zero-timeout REQUEST must still be answered"
    );
    assert_eq!(h.runs.load(Ordering::SeqCst), 1);
}
