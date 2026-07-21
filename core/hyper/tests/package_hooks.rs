#![cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]

#[cfg(feature = "dynclib-engine")]
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "dynclib-engine")]
use std::sync::LazyLock;
use std::time::{Duration, UNIX_EPOCH};

use actr_framework::WebRtcPeerStatus;
use actr_hyper::test_support::{TestPackageHookEvent, runtime_context_with_host_transport};
use actr_hyper::workload::{HostAbiFn, HostOperation, HostOperationResult, InvocationContext};
use actr_protocol::{ActrError, ActrId, ActrType, PayloadType, Realm};

#[cfg(all(feature = "wasm-engine", actr_wasm_fixture_available))]
use actr_hyper::test_support::instantiate_wasm_workload;
#[cfg(all(feature = "wasm-engine", actr_wasm_fixture_available))]
use actr_hyper::wasm::WasmHost;

#[cfg(feature = "dynclib-engine")]
use actr_framework::guest::dynclib_abi::{InitPayloadV1, version};
#[cfg(feature = "dynclib-engine")]
use actr_hyper::dynclib::DynclibHost;
#[cfg(feature = "dynclib-engine")]
use actr_hyper::test_support::instantiate_dynclib_workload;

#[cfg(all(feature = "wasm-engine", actr_wasm_fixture_available))]
#[path = "wasm_actor_fixture.rs"]
mod wasm_actor_fixture;

#[cfg(feature = "dynclib-engine")]
static DYNCLIB_PACKAGE_HOOK_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

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
    test_ctx_with_request_id("package-hook-test")
}

fn test_ctx_with_request_id(request_id: &'static str) -> InvocationContext {
    InvocationContext {
        self_id: test_actr_id(),
        caller_id: None,
        request_id: request_id.to_string(),
    }
}

fn package_hook_cases() -> Vec<(TestPackageHookEvent, &'static str)> {
    let peer = test_actr_id();
    let expiry = UNIX_EPOCH + Duration::from_secs(1_725_000_000);
    vec![
        (
            TestPackageHookEvent::SignalingConnecting,
            "on_signaling_connecting",
        ),
        (
            TestPackageHookEvent::SignalingConnected,
            "on_signaling_connected",
        ),
        (
            TestPackageHookEvent::SignalingDisconnected,
            "on_signaling_disconnected",
        ),
        (
            TestPackageHookEvent::WebSocketConnecting { peer: peer.clone() },
            "on_websocket_connecting:peer=1:relayed=none:status=none",
        ),
        (
            TestPackageHookEvent::WebSocketConnected { peer: peer.clone() },
            "on_websocket_connected:peer=1:relayed=none:status=none",
        ),
        (
            TestPackageHookEvent::WebSocketDisconnected { peer: peer.clone() },
            "on_websocket_disconnected:peer=1:relayed=none:status=none",
        ),
        (
            TestPackageHookEvent::WebRtcConnecting { peer: peer.clone() },
            "on_webrtc_connecting:peer=1:relayed=none:status=connecting",
        ),
        (
            TestPackageHookEvent::WebRtcConnected {
                peer: peer.clone(),
                relayed: true,
            },
            "on_webrtc_connected:peer=1:relayed=true:status=connected",
        ),
        (
            TestPackageHookEvent::WebRtcDisconnected {
                peer: peer.clone(),
                status: WebRtcPeerStatus::Recovering,
            },
            "on_webrtc_disconnected:peer=1:relayed=none:status=recovering",
        ),
        (
            TestPackageHookEvent::WebRtcDisconnected {
                peer,
                status: WebRtcPeerStatus::Idle,
            },
            "on_webrtc_disconnected:peer=1:relayed=none:status=idle",
        ),
        (
            TestPackageHookEvent::CredentialRenewed { new_expiry: expiry },
            "on_credential_renewed:expiry=1725000000",
        ),
        (
            TestPackageHookEvent::CredentialExpiring { new_expiry: expiry },
            "on_credential_expiring:expiry=1725000000",
        ),
        (
            TestPackageHookEvent::MailboxBackpressure {
                queue_len: 7,
                threshold: 3,
            },
            "on_mailbox_backpressure:queue_len=7:threshold=3",
        ),
    ]
}

fn recording_bridge() -> (HostAbiFn, tokio::sync::mpsc::UnboundedReceiver<String>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let bridge: HostAbiFn = Arc::new(move |op| {
        let tx = tx.clone();
        Box::pin(async move {
            match op {
                HostOperation::CallRaw(req) if req.route_key == "test/record_hook" => {
                    let name = String::from_utf8(req.payload).expect("hook name is utf8");
                    let _ = tx.send(name);
                    HostOperationResult::Bytes(Vec::new())
                }
                _ => HostOperationResult::Error(-1),
            }
        })
    });
    (bridge, rx)
}

fn recording_host_transport() -> (
    Arc<actr_hyper::HostTransport>,
    tokio::sync::mpsc::UnboundedReceiver<String>,
    tokio::task::JoinHandle<()>,
) {
    let host_transport = Arc::new(actr_hyper::HostTransport::new());
    let recorder_transport = host_transport.clone();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let request_lane = recorder_transport
            .get_lane(PayloadType::RpcReliable, None)
            .await
            .expect("host transport reliable lane should exist");
        while let Ok(envelope) = request_lane.recv_envelope().await {
            let request_id = envelope.request_id.clone();
            match envelope.route_key.as_str() {
                "test/record_hook" => {
                    let payload = envelope.payload.unwrap_or_default();
                    let name = String::from_utf8(payload.to_vec()).expect("hook name is utf8");
                    let _ = tx.send(name);
                    let _ = recorder_transport
                        .complete_response(&request_id, bytes::Bytes::new())
                        .await;
                }
                route_key => {
                    let _ = tx.send(format!("unexpected route: {route_key}"));
                    let _ = recorder_transport
                        .complete_error(&request_id, ActrError::UnknownRoute(route_key.to_string()))
                        .await;
                }
            }
        }
    });

    (host_transport, rx, task)
}

async fn assert_recorded_hooks(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<String>,
    expected: Vec<&'static str>,
) {
    for expected_name in expected {
        let observed = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for hook record")
            .expect("recording bridge dropped");
        assert_eq!(observed, expected_name);
    }
}

#[cfg(all(feature = "wasm-engine", actr_wasm_fixture_available))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_package_receives_runtime_hook_events() {
    let host =
        WasmHost::compile(wasm_actor_fixture::WASM_ACTOR_FIXTURE).expect("compile component");
    let mut workload = instantiate_wasm_workload(&host)
        .await
        .expect("instantiate wasm workload");
    let (bridge, rx) = recording_bridge();
    let cases = package_hook_cases();
    let expected = cases.iter().map(|(_, name)| *name).collect::<Vec<_>>();

    for (event, _) in cases {
        workload
            .call_hook_event(event, test_ctx(), &bridge)
            .await
            .expect("wasm hook event should dispatch");
    }

    assert_recorded_hooks(rx, expected).await;
}

#[cfg(all(feature = "wasm-engine", actr_wasm_fixture_available))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_package_observer_bridge_reaches_guest_hooks() {
    let host =
        WasmHost::compile(wasm_actor_fixture::WASM_ACTOR_FIXTURE).expect("compile component");
    let workload = instantiate_wasm_workload(&host)
        .await
        .expect("instantiate wasm workload");
    let observer = workload.into_package_hook_observer();
    let (host_transport, rx, recorder) = recording_host_transport();
    let ctx = runtime_context_with_host_transport(test_actr_id(), host_transport);
    let cases = package_hook_cases();
    let expected = cases.iter().map(|(_, name)| *name).collect::<Vec<_>>();

    for (event, _) in cases {
        observer.call(event, &ctx).await;
    }

    assert_recorded_hooks(rx, expected).await;
    recorder.abort();
}

#[cfg(all(feature = "wasm-engine", actr_wasm_fixture_available))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_package_receives_on_ready_and_on_stop() {
    let host =
        WasmHost::compile(wasm_actor_fixture::WASM_ACTOR_FIXTURE).expect("compile component");
    let mut workload = instantiate_wasm_workload(&host)
        .await
        .expect("instantiate wasm workload");
    let (bridge, rx) = recording_bridge();

    workload
        .call_on_ready(test_ctx_with_request_id("lifecycle:on_ready"), &bridge)
        .await
        .expect("wasm on_ready should dispatch");
    workload
        .call_on_stop(test_ctx_with_request_id("lifecycle:on_stop"), &bridge)
        .await
        .expect("wasm on_stop should dispatch");

    assert_recorded_hooks(rx, vec!["on_ready", "on_stop"]).await;
}

#[cfg(feature = "dynclib-engine")]
fn fixture_so_path() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture_dir = manifest_dir.join("tests/dynclib_actor_fixture");

    let status = std::process::Command::new("cargo")
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

#[cfg(feature = "dynclib-engine")]
fn dynclib_init_payload() -> InitPayloadV1 {
    InitPayloadV1 {
        version: version::V1,
        actr_type: "test:fixture:0.1.0".to_string(),
        credential: Vec::new(),
        actor_id: Vec::new(),
        realm_id: 1,
    }
}

#[cfg(feature = "dynclib-engine")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynclib_package_receives_runtime_hook_events() {
    let _guard = DYNCLIB_PACKAGE_HOOK_TEST_LOCK.lock().await;
    let host = DynclibHost::load(fixture_so_path()).expect("load dynclib fixture");
    let mut workload =
        instantiate_dynclib_workload(host, &dynclib_init_payload()).expect("instantiate dynclib");
    let (bridge, rx) = recording_bridge();
    let cases = package_hook_cases();
    let expected = cases.iter().map(|(_, name)| *name).collect::<Vec<_>>();

    for (event, _) in cases {
        workload
            .call_hook_event(event, test_ctx(), &bridge)
            .await
            .expect("dynclib hook event should dispatch");
    }

    assert_recorded_hooks(rx, expected).await;
    workload.shutdown().await.expect("shutdown dynclib");
}

#[cfg(feature = "dynclib-engine")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynclib_package_observer_bridge_reaches_guest_hooks() {
    let _guard = DYNCLIB_PACKAGE_HOOK_TEST_LOCK.lock().await;
    let host = DynclibHost::load(fixture_so_path()).expect("load dynclib fixture");
    let workload =
        instantiate_dynclib_workload(host, &dynclib_init_payload()).expect("instantiate dynclib");
    let observer = workload.into_package_hook_observer();
    let (host_transport, rx, recorder) = recording_host_transport();
    let ctx = runtime_context_with_host_transport(test_actr_id(), host_transport);
    let cases = package_hook_cases();
    let expected = cases.iter().map(|(_, name)| *name).collect::<Vec<_>>();

    for (event, _) in cases {
        observer.call(event, &ctx).await;
    }

    assert_recorded_hooks(rx, expected).await;
    observer.shutdown().await;
    recorder.abort();
}

#[cfg(feature = "dynclib-engine")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynclib_package_receives_on_ready_and_on_stop() {
    let _guard = DYNCLIB_PACKAGE_HOOK_TEST_LOCK.lock().await;
    let host = DynclibHost::load(fixture_so_path()).expect("load dynclib fixture");
    let mut workload =
        instantiate_dynclib_workload(host, &dynclib_init_payload()).expect("instantiate dynclib");
    let (bridge, rx) = recording_bridge();

    workload
        .call_on_ready(test_ctx_with_request_id("lifecycle:on_ready"), &bridge)
        .await
        .expect("dynclib on_ready should dispatch");
    workload
        .call_on_stop(test_ctx_with_request_id("lifecycle:on_stop"), &bridge)
        .await
        .expect("dynclib on_stop should dispatch");

    assert_recorded_hooks(rx, vec!["on_ready", "on_stop"]).await;
}
