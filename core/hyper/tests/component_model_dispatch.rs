//! Component Model dispatch regression tests (Phase 1 Commit 6).
//!
//! Ports the runnable subset of the Phase 0.5 async spike
//! (`experiments/component-spike-async/host/src/main.rs`) into the real
//! actr host surface. Each test drives `WasmHost::compile` →
//! `instantiate` → `call_on_start` → `handle` against the rebuilt
//! `wasm_actor_fixture` Component, mirroring the shape of
//! `core/hyper/src/wasm/host.rs::WasmWorkload::handle`.
//!
//! Skipped from the spike:
//! - **Test 3** (concurrent dispatches on the same instance) — wasmtime's
//!   `Store<T>` is not `Sync` and `call_dispatch` takes `&mut Store<T>`,
//!   so the Rust borrow checker prevents writing that test in safe code.
//!   The spike confirmed the guarantee at compile time; there is no
//!   runtime behaviour left to verify.
//! - **Test 5** (guest-side async ergonomics) — compile-time covered by
//!   the guest framework tests; not a runtime concern.
//! - **Test 6** (100-dispatch throughput) — superseded by the Commit 6
//!   `component_model_per_call_overhead` micro-bench below, which times
//!   1000 sequential dispatches without the 50 ms host sleep so the
//!   overhead number is directly comparable to the spike's 1.1 ms/call
//!   baseline.

#![cfg(feature = "wasm-engine")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use actr_hyper::test_support::instantiate_wasm_workload;
use actr_hyper::wasm::{WasmError, WasmHost};
use actr_hyper::workload::{HostAbiFn, HostOperation, HostOperationResult, InvocationContext};
use actr_protocol::{ActrId, ActrType, Realm, RpcEnvelope, prost::Message as ProstMessage};

#[path = "wasm_actor_fixture.rs"]
mod wasm_actor_fixture;

// ─── helpers ────────────────────────────────────────────────────────────────

fn fixture_component_bytes() -> &'static [u8] {
    wasm_actor_fixture::WASM_ACTOR_FIXTURE
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
        request_id: "test-req".to_string(),
    }
}

fn make_envelope(route_key: &str, payload: Vec<u8>) -> Vec<u8> {
    RpcEnvelope {
        route_key: route_key.to_string(),
        payload: Some(payload.into()),
        request_id: "test-req".to_string(),
        direction: Some(actr_protocol::Direction::Request as i32),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Build a host-side bridge that answers `test/double_impl` `call_raw`
/// requests by multiplying the inbound i32 by two. Accepts an optional
/// per-call sleep so tests can exercise the async suspension path.
///
/// Returns (bridge, invocation counter). The counter is shared with the
/// bridge so tests can assert the bridge was actually reached.
fn doubling_bridge(sleep: Option<Duration>) -> (HostAbiFn, Arc<AtomicU64>) {
    let counter = Arc::new(AtomicU64::new(0));
    let counter_clone = counter.clone();
    let bridge: HostAbiFn = Arc::new(move |op| {
        let counter = counter_clone.clone();
        let sleep = sleep;
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            if let Some(dur) = sleep {
                tokio::time::sleep(dur).await;
            }
            match op {
                HostOperation::CallRaw(req) if req.route_key == "test/double_impl" => {
                    if req.payload.len() < 4 {
                        return HostOperationResult::Error(-1);
                    }
                    let x = i32::from_le_bytes([
                        req.payload[0],
                        req.payload[1],
                        req.payload[2],
                        req.payload[3],
                    ]);
                    HostOperationResult::Bytes((x * 2).to_le_bytes().to_vec())
                }
                _ => HostOperationResult::Error(-1),
            }
        })
    });
    (bridge, counter)
}

/// Bridge that never gets called (every test/echo dispatch is expected
/// to stay inside the guest without issuing a host import).
fn unreachable_bridge() -> HostAbiFn {
    Arc::new(|_| Box::pin(async move { HostOperationResult::Error(-1) }))
}

// ─── Test 1 — basic async dispatch round-trip ───────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_model_basic_echo_round_trip() {
    let host = WasmHost::compile(fixture_component_bytes()).expect("compile component");
    let mut wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    // NB: `call_on_start` is skipped in every test in this module. The
    // Phase 1 Commit 3 guest adapter unconditionally builds a
    // `WasmContext` via the `get-self-id` / `get-caller-id` /
    // `get-request-id` host imports from inside every lifecycle hook,
    // and the host deliberately traps those imports when no invocation
    // context is installed (see core/hyper/src/wasm/host.rs). Threading
    // an invocation through the lifecycle path is Phase 1 follow-up
    // scope; these tests cover only `handle`, which installs the
    // context before dispatching.
    let payload = b"hello-component".to_vec();
    let req = make_envelope("test/echo", payload.clone());
    let bridge = unreachable_bridge();

    let reply = wl
        .handle(&req, test_ctx(), &bridge)
        .await
        .expect("echo dispatch should succeed");
    assert_eq!(reply, payload, "test/echo must round-trip the payload");
}

// ─── Test 2 — cross-instance parallelism ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn component_model_cross_instance_parallelism() {
    let host = WasmHost::compile(fixture_component_bytes()).expect("compile component");
    let mut wl_a = instantiate_wasm_workload(&host)
        .await
        .expect("instantiate A");
    let mut wl_b = instantiate_wasm_workload(&host)
        .await
        .expect("instantiate B");

    // on_start skipped on both instances — see
    // `component_model_basic_echo_round_trip` for why.

    // Each instance gets a bridge that sleeps 50 ms before responding —
    // if the two dispatches are truly concurrent the wall-clock total is
    // ~50 ms, not ~100 ms.
    let (bridge_a, ca) = doubling_bridge(Some(Duration::from_millis(50)));
    let (bridge_b, cb) = doubling_bridge(Some(Duration::from_millis(50)));

    let req_a = make_envelope("test/double", 7i32.to_le_bytes().to_vec());
    let req_b = make_envelope("test/double", 11i32.to_le_bytes().to_vec());

    let ctx_a = test_ctx();
    let ctx_b = test_ctx();

    let t0 = Instant::now();
    let (ra, rb) = tokio::join!(
        async { wl_a.handle(&req_a, ctx_a, &bridge_a).await },
        async { wl_b.handle(&req_b, ctx_b, &bridge_b).await },
    );
    let elapsed = t0.elapsed();

    let reply_a = ra.expect("dispatch A should succeed");
    let reply_b = rb.expect("dispatch B should succeed");

    let val_a = i32::from_le_bytes([reply_a[0], reply_a[1], reply_a[2], reply_a[3]]);
    let val_b = i32::from_le_bytes([reply_b[0], reply_b[1], reply_b[2], reply_b[3]]);
    assert_eq!(val_a, 14, "7 * 2 = 14 from bridge A");
    assert_eq!(val_b, 22, "11 * 2 = 22 from bridge B");

    assert_eq!(ca.load(Ordering::SeqCst), 1, "bridge A must be called once");
    assert_eq!(cb.load(Ordering::SeqCst), 1, "bridge B must be called once");

    // 50 ms sleep per bridge + overhead; serial dispatches would be ≥100 ms.
    // Use 90 ms as a soft ceiling to tolerate scheduler jitter but still
    // catch genuine serialization.
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    assert!(
        elapsed_ms < 90.0,
        "cross-instance dispatches must run concurrently; saw {elapsed_ms:.1} ms \
         (two 50 ms host sleeps, serial would be ~100 ms)"
    );
}

// ─── Test 4 — host executor free during guest await ─────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_model_executor_non_blocking_during_host_await() {
    let host = WasmHost::compile(fixture_component_bytes()).expect("compile component");
    let mut wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    // on_start skipped — see `component_model_basic_echo_round_trip`.

    let tick_count = Arc::new(AtomicU64::new(0));
    let tick_stop = Arc::new(AtomicBool::new(false));

    // Ticker runs every 10 ms. If the tokio executor is blocked during
    // the guest host-import await, it will record ~0 ticks; otherwise
    // it records several.
    let tc = tick_count.clone();
    let ts = tick_stop.clone();
    let ticker = tokio::spawn(async move {
        while !ts.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tc.fetch_add(1, Ordering::SeqCst);
        }
    });

    let (bridge, _counter) = doubling_bridge(Some(Duration::from_millis(80)));
    let req = make_envelope("test/double", 3i32.to_le_bytes().to_vec());
    let _ = wl
        .handle(&req, test_ctx(), &bridge)
        .await
        .expect("double dispatch should succeed");

    tick_stop.store(true, Ordering::SeqCst);
    let _ = ticker.await;
    let ticks = tick_count.load(Ordering::SeqCst);

    // 80 ms of sleep → ~8 ticks. Allow some jitter; require at least 3
    // to catch the wasmtime-blocks-executor regression.
    assert!(
        ticks >= 3,
        "tokio executor must keep running during guest host-import await; saw {ticks} ticks \
         (expected ~8 ticks over 80 ms sleep)"
    );
}

// ─── Test 7 — error variant propagation guest → host ────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_model_error_variant_propagates() {
    let host = WasmHost::compile(fixture_component_bytes()).expect("compile component");
    let mut wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    // on_start skipped — see `component_model_basic_echo_round_trip`.

    let bridge = unreachable_bridge();
    let req = make_envelope("unknown/route", Vec::new());

    let err = wl
        .handle(&req, test_ctx(), &bridge)
        .await
        .expect_err("unknown route must surface guest error");

    match &err {
        WasmError::ExecutionFailed(msg) => {
            assert!(
                msg.contains("UnknownRoute"),
                "error should carry the UnknownRoute variant, got: {msg}"
            );
        }
        other => panic!("expected ExecutionFailed, got {other:?}"),
    }
}

// ─── Test 8 — guest panic after host suspension surfaces as trap ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_model_panic_after_await_surfaces_as_trap() {
    let host = WasmHost::compile(fixture_component_bytes()).expect("compile component");
    let mut wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    // on_start skipped — see `component_model_basic_echo_round_trip`.

    // Bridge replies with any bytes; the guest panics immediately after
    // the .await returns.
    let (bridge, counter) = doubling_bridge(Some(Duration::from_millis(10)));
    let req = make_envelope("test/boom-after-await", 1i32.to_le_bytes().to_vec());

    let err = wl
        .handle(&req, test_ctx(), &bridge)
        .await
        .expect_err("post-await panic must surface as a host-visible error");

    // Bridge must have been reached: confirms the await resumed before
    // the panic fired, exactly like Phase 0.5 Test 8.
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "host bridge must have serviced the pre-panic call_raw exactly once"
    );

    match &err {
        WasmError::ExecutionFailed(msg) => {
            // The guest panic surfaces through wasmtime as a trap. The
            // message shape varies slightly across wasmtime versions, so
            // match on either "trap" or "panic" rather than exact text.
            let lower = msg.to_ascii_lowercase();
            assert!(
                lower.contains("trap") || lower.contains("panic"),
                "expected trap/panic in error message, got: {msg}"
            );
        }
        other => panic!("expected ExecutionFailed, got {other:?}"),
    }
}

// ─── Per-call overhead micro-benchmark (supersedes spike Test 6) ────────────

/// Measure 1000 sequential `test/echo` dispatches and print the per-call
/// overhead. `test/echo` stays entirely inside the guest (no host await),
/// so the measurement is directly comparable to the Phase 0.5 spike's
/// 1.1 ms/call baseline.
///
/// Not an assertion-carrying test: the stop-and-report trigger for a
/// >10× regression is documented in the Phase 1 plan and runs as an
/// inspection of the eprintln output. Wall time varies with hardware;
/// the test passes as long as the 1000-dispatch loop completes without
/// error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_model_per_call_overhead() {
    let host = WasmHost::compile(fixture_component_bytes()).expect("compile component");
    let mut wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    // on_start skipped — see `component_model_basic_echo_round_trip`.

    let bridge = unreachable_bridge();
    let payload = vec![0u8; 64];
    let req = make_envelope("test/echo", payload);

    // Warm-up: first dispatch often amortises JIT / paging costs.
    let _ = wl.handle(&req, test_ctx(), &bridge).await.expect("warm-up");

    let iters: u64 = 1000;
    let t0 = Instant::now();
    for _ in 0..iters {
        let _ = wl
            .handle(&req, test_ctx(), &bridge)
            .await
            .expect("bench dispatch");
    }
    let elapsed = t0.elapsed();
    let per_call_us = elapsed.as_secs_f64() * 1_000_000.0 / iters as f64;
    eprintln!(
        "[component_model_per_call_overhead] {iters} sequential dispatches in {:.2} ms; \
         per call: {per_call_us:.2} us (Phase 0.5 spike baseline: ~1100 us with 50 ms host sleep \
         folded in; this measurement excludes host sleep so numbers are not one-to-one).",
        elapsed.as_secs_f64() * 1000.0
    );
}

// ─── Phase 1 follow-up — call_on_start no longer traps ──────────────────────
//
// Before this followup, the host invoked lifecycle exports without installing
// the synthetic invocation context that guest `WasmContext::from_host()` reads.
// The fix threads an invocation through `call_on_start`, so lifecycle hooks can
// use normal context accessors without trapping.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_model_call_on_start_does_not_trap() {
    let host = WasmHost::compile(fixture_component_bytes()).expect("compile component");
    let mut wl = instantiate_wasm_workload(&host).await.expect("instantiate");

    // Fixture uses a request-id-sensitive `on_start`. The test support wrapper
    // installs a non-package lifecycle request id, so this completes cleanly
    // while still exercising the context import path.
    wl.call_on_start()
        .await
        .expect("call_on_start should no longer trap with a lifecycle invocation context");

    // Sanity: subsequent dispatch path still works normally.
    let req = make_envelope("test/echo", b"after-on-start".to_vec());
    let reply = wl
        .handle(&req, test_ctx(), &unreachable_bridge())
        .await
        .expect("dispatch after on_start should succeed");
    assert_eq!(reply, b"after-on-start");
}
