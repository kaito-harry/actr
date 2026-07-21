//! Dynclib actor e2e integration tests
//!
//! Validates the full call chain:
//! host (DynclibHost/DynClibWorkload) -> actr_handle -> DynclibContext::call_raw()
//!                                    -> vtable trampoline -> host ABI -> response
//!
//! # Test scenarios
//!
//! 1. Unknown route -> returns error
//! 2. Echo route (no outbound calls) -> returns payload as-is
//! 3. Double route (triggers vtable call trampoline) -> returns x*2
//! 4. Multiple dispatches -> verifies state isolation between calls

#![cfg(feature = "dynclib-engine")]

use actr_framework::guest::dynclib_abi::{InitPayloadV1, version};
use std::path::{Path, PathBuf};
use std::process::Command;

use actr_hyper::dynclib::DynclibHost;
use actr_hyper::test_support::{dynclib_active_bridge_count, instantiate_dynclib_workload};
use actr_hyper::workload::{HostAbiFn, HostOperation, HostOperationResult, InvocationContext};
use actr_protocol::{ActrId, ActrType, Realm, RpcEnvelope, prost::Message as ProstMessage};

// ---- helpers ---------------------------------------------------------------

fn fixture_so_path() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture_dir = manifest_dir.join("tests/dynclib_actor_fixture");

    // Build the fixture cdylib
    let status = Command::new("cargo")
        .args(["build"])
        .current_dir(&fixture_dir)
        .status()
        .expect("failed to build dynclib fixture");
    assert!(status.success(), "dynclib fixture build failed");

    let target_dir = manifest_dir.join("../../target/core-hyper-tests-dynclib-actor-fixture/debug");
    if cfg!(target_os = "linux") {
        target_dir.join("libdynclib_actor_fixture.so")
    } else if cfg!(target_os = "macos") {
        target_dir.join("libdynclib_actor_fixture.dylib")
    } else {
        target_dir.join("dynclib_actor_fixture.dll")
    }
}

fn make_envelope(route_key: &str, payload: Vec<u8>) -> Vec<u8> {
    let envelope = RpcEnvelope {
        route_key: route_key.to_string(),
        payload: Some(payload.into()),
        direction: Some(actr_protocol::Direction::Request as i32),
        ..Default::default()
    };
    envelope.encode_to_vec()
}

fn test_actr_id() -> ActrId {
    ActrId {
        realm: Realm { realm_id: 1 },
        serial_number: 1,
        r#type: ActrType {
            manufacturer: "test".to_string(),
            name: "fixture".to_string(),
            version: "0.1.0".to_string(),
        },
    }
}

fn test_ctx() -> InvocationContext {
    InvocationContext {
        self_id: test_actr_id(),
        caller_id: None,
        request_id: "test-req-001".to_string(),
    }
}

fn test_config() -> InitPayloadV1 {
    InitPayloadV1 {
        version: version::V1,
        actr_type: "test:fixture:0.1.0".to_string(),
        credential: Vec::new(),
        actor_id: Vec::new(),
        realm_id: 1,
    }
}

fn noop_executor() -> HostAbiFn {
    std::sync::Arc::new(|_pending| Box::pin(async { HostOperationResult::Error(-1) }))
}

/// Process-level lock ensuring dynclib tests run one at a time.
///
/// A single SO image carries module-global guest state (`entry!` installs
/// a process-global actor state via `actr_init`). Concurrent test threads
/// that each call `actr_init` on the same image race on that global,
/// producing undefined behaviour. The lock serialises the full lifecycle
/// (load → init → handle → drop) for each test without requiring
/// `--test-threads=1` for the entire binary.
///
/// `tokio::sync::Mutex` rather than `std::sync::Mutex` so the guard can be
/// held safely across the `instance.handle(...).await` dispatch — the std
/// guard would trip clippy's `await_holding_lock` lint and could deadlock
/// if the runtime moved the awaiting task to a different thread mid-await.
static DYNCLIB_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---- tests -----------------------------------------------------------------

/// Unknown route -> dispatch returns error
#[tokio::test]
async fn dynclib_unknown_route_returns_error() {
    let _guard = DYNCLIB_SERIAL.lock().await;
    let so_path = fixture_so_path();
    let host = DynclibHost::load(&so_path).expect("load SO");
    let mut instance = instantiate_dynclib_workload(host, &test_config()).expect("instantiate");

    let req_bytes = make_envelope("unknown/route", vec![1, 0, 0, 0]);
    let executor = noop_executor();

    let result = instance.handle(&req_bytes, test_ctx(), &executor).await;

    assert!(result.is_err(), "unknown route should return error");
    instance.shutdown().await.expect("shutdown dynclib");
}

/// Echo route -> returns payload without outbound calls
#[tokio::test]
async fn dynclib_echo_returns_payload() {
    let _guard = DYNCLIB_SERIAL.lock().await;
    let so_path = fixture_so_path();
    let host = DynclibHost::load(&so_path).expect("load SO");
    let mut instance = instantiate_dynclib_workload(host, &test_config()).expect("instantiate");

    let payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let req_bytes = make_envelope("test/echo", payload.clone());
    let executor = noop_executor();

    let result = instance
        .handle(&req_bytes, test_ctx(), &executor)
        .await
        .expect("echo dispatch failed");

    assert_eq!(result, payload, "echo should return payload as-is");
    instance.shutdown().await.expect("shutdown dynclib");
}

/// Double route -> triggers vtable call trampoline, returns x*2
#[tokio::test]
async fn dynclib_double_dispatch() {
    let _guard = DYNCLIB_SERIAL.lock().await;
    let so_path = fixture_so_path();
    let host = DynclibHost::load(&so_path).expect("load SO");
    let mut instance = instantiate_dynclib_workload(host, &test_config()).expect("instantiate");

    let x: i32 = 7;
    let req_bytes = make_envelope("test/double", x.to_le_bytes().to_vec());

    // The fixture calls ctx.call_raw(), which encodes HOST_CALL_RAW and decodes
    // on the host side as HostOperation::CallRaw.
    let executor: HostAbiFn = std::sync::Arc::new(|pending| {
        Box::pin(async move {
            match pending {
                HostOperation::CallRaw(req) => {
                    assert_eq!(req.route_key, "test/double_impl", "route_key mismatch");
                    assert_eq!(req.payload.len(), 4, "payload should be 4 bytes");

                    let val = i32::from_le_bytes([
                        req.payload[0],
                        req.payload[1],
                        req.payload[2],
                        req.payload[3],
                    ]);
                    assert_eq!(val, 7, "guest should pass x=7");

                    // mock: return x * 2
                    let doubled = (val * 2).to_le_bytes().to_vec();
                    HostOperationResult::Bytes(doubled)
                }
                other => panic!(
                    "expected HostOperation::CallRaw, got {:?}",
                    std::mem::discriminant(&other)
                ),
            }
        })
    });

    let result = instance
        .handle(&req_bytes, test_ctx(), &executor)
        .await
        .expect("double dispatch failed");

    assert_eq!(result.len(), 4, "response should be 4 bytes");
    let resp_val = i32::from_le_bytes([result[0], result[1], result[2], result[3]]);
    assert_eq!(resp_val, 14, "response should be 7 * 2 = 14");
    instance.shutdown().await.expect("shutdown dynclib");
}

/// Multiple dispatches -> verifies state does not leak between calls
#[tokio::test]
async fn dynclib_multiple_dispatches() {
    let _guard = DYNCLIB_SERIAL.lock().await;
    let so_path = fixture_so_path();
    let host = DynclibHost::load(&so_path).expect("load SO");
    let mut instance = instantiate_dynclib_workload(host, &test_config()).expect("instantiate");

    for x in [1i32, 5, 42, 100] {
        let req_bytes = make_envelope("test/double", x.to_le_bytes().to_vec());

        let executor: HostAbiFn = std::sync::Arc::new(|pending| {
            Box::pin(async move {
                match pending {
                    HostOperation::CallRaw(req) => {
                        let val = i32::from_le_bytes([
                            req.payload[0],
                            req.payload[1],
                            req.payload[2],
                            req.payload[3],
                        ]);
                        HostOperationResult::Bytes((val * 2).to_le_bytes().to_vec())
                    }
                    _ => HostOperationResult::Error(-1),
                }
            })
        });

        let result = instance
            .handle(&req_bytes, test_ctx(), &executor)
            .await
            .expect("dispatch failed");

        let resp_val = i32::from_le_bytes([result[0], result[1], result[2], result[3]]);
        assert_eq!(resp_val, x * 2, "dispatch({x}) should return {}", x * 2);
    }

    instance.shutdown().await.expect("shutdown dynclib");
}

/// A future that returns Pending is driven by the guest-owned Tokio runtime.
#[tokio::test]
async fn dynclib_tokio_timer_completes() {
    let _guard = DYNCLIB_SERIAL.lock().await;
    let so_path = fixture_so_path();
    let host = DynclibHost::load(&so_path).expect("load SO");
    let mut instance = instantiate_dynclib_workload(host, &test_config()).expect("instantiate");

    let payload = b"after-timer".to_vec();
    let result = instance
        .handle(
            &make_envelope("test/sleep", payload.clone()),
            test_ctx(),
            &noop_executor(),
        )
        .await
        .expect("timer dispatch failed");

    assert_eq!(result, payload);
    instance.shutdown().await.expect("shutdown dynclib");
    assert_eq!(dynclib_active_bridge_count(), 0);
}

/// A detached Tokio task can retain Context and call the host after dispatch returns.
#[tokio::test]
async fn dynclib_spawned_task_calls_host_after_dispatch() {
    let _guard = DYNCLIB_SERIAL.lock().await;
    let so_path = fixture_so_path();
    let host = DynclibHost::load(&so_path).expect("load SO");
    let mut instance = instantiate_dynclib_workload(host, &test_config()).expect("instantiate");
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let executor: HostAbiFn = std::sync::Arc::new(move |pending| {
        let event_tx = event_tx.clone();
        Box::pin(async move {
            match pending {
                HostOperation::CallRaw(req) if req.route_key == "test/spawned" => {
                    let _ = event_tx.send(req.payload);
                    HostOperationResult::Bytes(Vec::new())
                }
                _ => HostOperationResult::Error(-1),
            }
        })
    });
    let payload = b"background-context".to_vec();

    let result = instance
        .handle(
            &make_envelope("test/spawn", payload.clone()),
            test_ctx(),
            &executor,
        )
        .await
        .expect("spawn dispatch failed");
    assert_eq!(result, b"spawned");

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), event_rx.recv())
        .await
        .expect("spawned task timed out")
        .expect("spawned task channel closed");
    assert_eq!(event, payload);

    instance.shutdown().await.expect("shutdown dynclib");
    assert_eq!(dynclib_active_bridge_count(), 0);
}

/// Managed tasks are cancelled and their retained bridge tokens are released on shutdown.
#[tokio::test]
async fn dynclib_shutdown_drains_managed_tasks() {
    let _guard = DYNCLIB_SERIAL.lock().await;
    let so_path = fixture_so_path();
    let host = DynclibHost::load(&so_path).expect("load SO");
    let mut instance = instantiate_dynclib_workload(host, &test_config()).expect("instantiate");
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();

    let executor: HostAbiFn = std::sync::Arc::new(move |pending| {
        let started_tx = started_tx.clone();
        Box::pin(async move {
            match pending {
                HostOperation::CallRaw(req) if req.route_key == "test/background-started" => {
                    let _ = started_tx.send(());
                    HostOperationResult::Bytes(Vec::new())
                }
                _ => HostOperationResult::Error(-1),
            }
        })
    });

    let result = instance
        .handle(
            &make_envelope("test/spawn-pending", Vec::new()),
            test_ctx(),
            &executor,
        )
        .await
        .expect("managed spawn dispatch failed");
    assert_eq!(result, b"spawned");

    tokio::time::timeout(std::time::Duration::from_secs(2), started_rx.recv())
        .await
        .expect("managed task did not start")
        .expect("managed task channel closed");
    assert_eq!(dynclib_active_bridge_count(), 1);

    instance.shutdown().await.expect("shutdown dynclib");
    assert_eq!(dynclib_active_bridge_count(), 0);
}

/// A reloaded image can be re-initialised after the previous instance shut
/// down, even when the shared library stays mapped across the cycle.
///
/// Loading the image twice keeps `dlopen`'s refcount above zero after the
/// first host is dropped, so the library is not unmapped and module-global
/// guest state is not reinitialised by the loader. `actr_shutdown` must
/// therefore reset that state itself — otherwise the second `actr_init` sees
/// a stale Tokio runtime cell and returns `INIT_FAILED`.
#[tokio::test]
async fn dynclib_reinit_after_shutdown_same_image() {
    let _guard = DYNCLIB_SERIAL.lock().await;
    let so_path = fixture_so_path();

    // Hold an extra reference so the first host's `dlclose` cannot unmap the
    // image. This makes the stale-static-state regression deterministic on
    // every supported native platform.
    let keeper = DynclibHost::load(&so_path).expect("load keeper SO");

    let mut first = instantiate_dynclib_workload(
        DynclibHost::load(&so_path).expect("load SO 1"),
        &test_config(),
    )
    .expect("first instantiate");
    let result = first
        .handle(
            &make_envelope("test/echo", b"one".to_vec()),
            test_ctx(),
            &noop_executor(),
        )
        .await
        .expect("first dispatch");
    assert_eq!(result, b"one");
    first.shutdown().await.expect("first shutdown");
    drop(first);

    // The image is still mapped through `keeper`, so successful re-init proves
    // shutdown reset guest runtime state instead of relying on loader teardown.
    let mut second =
        instantiate_dynclib_workload(keeper, &test_config()).expect("second instantiate");
    let result = second
        .handle(
            &make_envelope("test/echo", b"two".to_vec()),
            test_ctx(),
            &noop_executor(),
        )
        .await
        .expect("second dispatch");
    assert_eq!(result, b"two");
    second.shutdown().await.expect("second shutdown");
}
