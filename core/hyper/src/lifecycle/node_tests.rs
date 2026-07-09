use super::*;

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
    Arc<tokio::sync::Mutex<Workload>>,
) {
    let ctx =
        runtime_context_with_host_transport(ActrId::default(), Arc::new(HostTransport::new()));
    let wl = Workload::Linked(Arc::new(DummyLinkedHandle) as Arc<dyn LinkedWorkloadHandle>);
    (ctx, Arc::new(tokio::sync::Mutex::new(wl)))
}

async fn actorless_harness() -> (
    crate::context::RuntimeContext,
    Arc<tokio::sync::Mutex<Workload>>,
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
    (ctx, Arc::new(tokio::sync::Mutex::new(wl)))
}

fn expect_error_code(res: HostOperationResult, expected: i32) {
    match res {
        HostOperationResult::Error(code) => assert_eq!(code, expected),
        other => panic!("expected HostOperationResult::Error({expected}), got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callraw_without_peer_returns_error_code() {
    let (ctx, wl) = actorless_harness().await;
    let res =
        host_operation_handler(ctx, wl, HostOperation::CallRaw(HostCallRawV1::default())).await;
    // No remote actor gate is installed, so routing fails immediately.
    expect_error_code(res, abi_code::GENERIC_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_with_default_dest_returns_error() {
    let (ctx, wl) = harness().await;
    let res = host_operation_handler(ctx, wl, HostOperation::Call(HostCallV1::default())).await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tell_with_default_dest_returns_error() {
    let (ctx, wl) = harness().await;
    let res = host_operation_handler(ctx, wl, HostOperation::Tell(HostTellV1::default())).await;
    expect_error_code(res, abi_code::PROTOCOL_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discover_without_realm_returns_error() {
    let (ctx, wl) = harness().await;
    let res =
        host_operation_handler(ctx, wl, HostOperation::Discover(HostDiscoverV1::default())).await;
    // Default ActrType with unreachable signaling → discover errors.
    expect_error_code(res, abi_code::GENERIC_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_and_unregister_stream_roundtrip() {
    let (ctx, wl) = harness().await;
    let register = host_operation_handler(
        ctx.clone(),
        wl.clone(),
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
        wl,
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
async fn unregister_unknown_stream_is_idempotent_done() {
    let (ctx, wl) = harness().await;
    let res = host_operation_handler(
        ctx,
        wl,
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
        wl,
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
        wl,
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
        wl,
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
