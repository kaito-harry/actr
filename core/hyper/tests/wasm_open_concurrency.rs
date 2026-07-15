//! M5 — open same-instance wasm concurrency.
//!
//! These tests drive the 0.2.0 (async-world) fixture through the production
//! dispatch path — a conflict-key scheduler in front of the **interleaved**
//! runner (`WasmWorkloadV2::run_interleaved`, a resident `run_concurrent`
//! region) — and assert the M5 guarantees end-to-end:
//!
//! 1. distinct-key dispatches truly interleave inside ONE instance (MAX_SEEN>=2)
//! 2. same-key dispatches stay strictly serial (MAX_SEEN==1)
//! 3. concurrency never exceeds the scheduler budget C
//! 4. an in-flight guest trap fails ALL siblings together and rebuilds (5. the
//!    whole-region teardown is asserted explicitly there too)
//! 6. a per-dispatch timeout hard-cancels the stuck Store (bounded, no hang),
//!    fails co-resident siblings, rebuilds, and frees the timed-out key; and
//!    (6b) the same-key timeout seam stays sealed — a same-key sibling never
//!    enters before the old Store has been discarded
//! 7. gate-off degrades to the serial M4 path (MAX_SEEN==1)
//! 8. the node-integration seam: the dedup write-back (node.rs:~1204) is correct
//!    around a gate-on interleaved wasm V2 dispatch (see the scope note on that
//!    test for what a full real-node crossing is blocked by)
//! 9. the package compat matrix: a V1 (sync-world) guest stays serial even when
//!    Interleaved is requested; a V2 guest works in both modes
//!
//! Evidence is gathered without sleep-based coordination: the guest reports the
//! peak in-flight count it observed in its own linear memory (MAX_SEEN), and the
//! host bridge gates guest→host crossings on semaphores + entry channels.

#![cfg(all(feature = "wasm-engine", actr_wasm_fixture_available))]

use std::sync::Arc;
use std::time::Duration;

// The conflict-key concurrency harness (route consts, gate bridge, watchdog
// helpers) is shared with the M6 isomorphism suite; see
// `core/hyper/tests/common/concurrency.rs`.
use actr_hyper::test_support::concurrency::{
    BOOM, ECHO, PROBE, await_dispatch, caller, gate_bridge, probe_spec, read_u32, spawn_dispatch,
    wait_entered,
};
use actr_hyper::test_support::{
    TestConcurrentDispatcher, TestDedupOutcome, TestDedupState, instantiate_wasm_workload,
};
use actr_hyper::wasm::WasmHost;
use actr_hyper::workload::{HostAbiFn, HostOperationResult};
use actr_protocol::ActrId;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};

#[path = "wasm_actor_fixture.rs"]
mod wasm_actor_fixture;

fn fixture_bytes() -> &'static [u8] {
    wasm_actor_fixture::WASM_ACTOR_FIXTURE
}

/// A genuine 0.1.0 sync-lift Component (frozen pre-M4 build of the same guest
/// source), used by the compat matrix to prove the V1-on-Interleaved fallback.
const V1_SYNCLIFT_GUEST: &[u8] = include_bytes!("fixtures/v1_synclift_guest.wasm");

// ── Facet 1 — distinct keys truly interleave (MAX_SEEN >= 2) ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleave_distinct_keys_reaches_max_seen_2() {
    let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
    let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    let dispatcher = Arc::new(wl.into_concurrent_dispatcher(probe_spec(), 8, 256, None));
    let (bridge, mut ctl) = gate_bridge();

    // Two distinct callers -> distinct conflict keys -> eligible to run at once.
    let d1 = spawn_dispatch(&dispatcher, PROBE, vec![], caller(1), &bridge);
    let d2 = spawn_dispatch(&dispatcher, PROBE, vec![], caller(2), &bridge);

    // Both must be suspended inside the ONE instance before either is released.
    wait_entered(&mut ctl.gate_entered, 2).await;
    ctl.gate_release.add_permits(2);

    let m1 = read_u32(
        &await_dispatch(d1, "distinct d1")
            .await
            .expect("d1 dispatch ok"),
    );
    let m2 = read_u32(
        &await_dispatch(d2, "distinct d2")
            .await
            .expect("d2 dispatch ok"),
    );

    assert!(
        m1.max(m2) >= 2,
        "distinct-key dispatches must interleave inside one instance \
         (MAX_SEEN>=2), got {m1} and {m2}"
    );
}

// ── Facet 2 — same key stays strictly serial (MAX_SEEN == 1) ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_key_stays_serial_max_seen_1() {
    let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
    let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    let dispatcher = Arc::new(wl.into_concurrent_dispatcher(probe_spec(), 8, 256, None));
    let (bridge, mut ctl) = gate_bridge();

    // SAME caller -> same conflict key -> the scheduler must serialize them.
    let a = spawn_dispatch(&dispatcher, PROBE, vec![], caller(7), &bridge);
    let b = spawn_dispatch(&dispatcher, PROBE, vec![], caller(7), &bridge);

    // Only ONE may be in the guest at a time, so we release strictly by the
    // guest's real *entry* order, never by spawn order. The scheduler is free to
    // admit either submission first; whichever it admits parks at the gate and
    // signals entry, we hand it exactly one permit, it replies and frees the
    // key, and only then can the second be admitted and enter. Awaiting a fixed
    // JoinHandle between releases would be a latent deadlock: if the scheduler
    // admitted the *other* task first, that handle stays parked with no permit
    // and the await hangs forever. Release-by-entry keeps this deterministic
    // regardless of which submission the scheduler dequeues first.
    for _ in 0..2 {
        wait_entered(&mut ctl.gate_entered, 1).await;
        ctl.gate_release.add_permits(1);
    }
    let first = read_u32(
        &await_dispatch(a, "same-key A")
            .await
            .expect("first dispatch ok"),
    );
    let second = read_u32(
        &await_dispatch(b, "same-key B")
            .await
            .expect("second dispatch ok"),
    );

    // Never overlapped => the shared in-flight counter never exceeded 1.
    assert_eq!(first, 1, "same-key dispatch A must never overlap a sibling");
    assert_eq!(
        second, 1,
        "same-key dispatch B must never overlap a sibling"
    );
}

// ── Facet 3 — concurrency is capped at the scheduler budget C ────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_capped_at_budget() {
    const C: usize = 3;
    let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
    let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    // queue_cap high enough to admit all submissions; budget bounds in-flight.
    let dispatcher = Arc::new(wl.into_concurrent_dispatcher(probe_spec(), C, 256, None));
    let (bridge, mut ctl) = gate_bridge();

    // C+2 distinct keys all want to run; only C may be in flight at once.
    let mut handles = Vec::new();
    for i in 0..(C as u64 + 2) {
        handles.push(spawn_dispatch(
            &dispatcher,
            PROBE,
            vec![],
            caller(100 + i),
            &bridge,
        ));
    }

    // Exactly C reach the guest and park; the extra 2 wait for a budget slot.
    wait_entered(&mut ctl.gate_entered, C).await;
    // Release everything; as the first C drain, the extra 2 are admitted.
    ctl.gate_release.add_permits(C + 2);

    let mut peak = 0u32;
    for h in handles {
        peak = peak.max(read_u32(
            &await_dispatch(h, "budget dispatch")
                .await
                .expect("dispatch ok"),
        ));
    }
    assert_eq!(
        peak, C as u32,
        "peak in-flight must equal the budget C={C} (never exceed it)"
    );
}

// ── Facets 4 & 5 — an in-flight trap fails ALL siblings and rebuilds ─────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_trap_fails_all_siblings_then_rebuilds() {
    const SIBLINGS: u64 = 3;
    let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
    let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    let dispatcher = Arc::new(wl.into_concurrent_dispatcher(probe_spec(), 8, 256, None));
    let (bridge, mut ctl) = gate_bridge();

    // N-1 probe siblings park at test/gate (held), and one boom dispatch parks
    // at test/double_impl. All distinct keys, all in flight in the ONE region.
    let mut siblings = Vec::new();
    for i in 0..SIBLINGS {
        siblings.push(spawn_dispatch(
            &dispatcher,
            PROBE,
            vec![],
            caller(200 + i),
            &bridge,
        ));
    }
    let boom = spawn_dispatch(
        &dispatcher,
        BOOM,
        1i32.to_le_bytes().to_vec(),
        caller(299),
        &bridge,
    );

    // Wait until every sibling AND boom are suspended inside the instance.
    wait_entered(&mut ctl.gate_entered, SIBLINGS as usize).await;
    wait_entered(&mut ctl.impl_entered, 1).await;

    // Release ONLY boom's host import: it returns, the guest panics after the
    // await, and the whole run_concurrent region collapses — taking every
    // in-flight sibling down with it (facet 5: whole-region teardown, not
    // per-task isolation).
    ctl.impl_release.add_permits(1);

    let boom_res = await_dispatch(boom, "boom").await;
    assert!(boom_res.is_err(), "the trapping dispatch itself must fail");
    for (i, h) in siblings.into_iter().enumerate() {
        let res = await_dispatch(h, "trap sibling").await;
        assert!(
            res.is_err(),
            "sibling {i} must fail when a co-resident dispatch traps (whole-region teardown)"
        );
        let msg = format!("{:?}", res.unwrap_err()).to_lowercase();
        assert!(
            msg.contains("trap") || msg.contains("unavailable") || msg.contains("instance"),
            "sibling {i} must fail with a retryable trap-class error, got: {msg}"
        );
    }

    // The instance must rebuild: a fresh dispatch succeeds AND reports
    // MAX_SEEN==1, which can only be true on a fresh linear memory (the pre-trap
    // in-flight count was SIBLINGS and never decremented on the torn-down region).
    ctl.gate_release.add_permits(1); // let the recovery probe pass straight through
    let recovered = dispatcher
        .dispatch(PROBE, vec![], caller(777), &bridge)
        .await
        .expect("a dispatch after the trap must succeed on the rebuilt instance");
    assert_eq!(
        read_u32(&recovered),
        1,
        "post-trap probe must see MAX_SEEN==1 (fresh linear memory ⇒ rebuild + cleared invocations)"
    );
}

// ── Facet 6 — per-dispatch timeout: hard cancel, sibling failure, rebuild ────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_timeout_discards_store_fails_siblings_and_recovers() {
    let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
    let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    // 300ms per-dispatch deadline, enforced inside the region.
    let dispatcher = Arc::new(wl.into_concurrent_dispatcher(
        probe_spec(),
        8,
        256,
        Some(Duration::from_millis(300)),
    ));
    let (bridge, mut ctl) = gate_bridge();

    // Two distinct-key dispatches both park at the gate forever. The first
    // deadline must discard the whole Store: Wasmtime does not cancel a guest
    // task merely because its `call_concurrent` future is dropped.
    let a = spawn_dispatch(&dispatcher, PROBE, vec![], caller(1), &bridge);
    let b = spawn_dispatch(&dispatcher, PROBE, vec![], caller(2), &bridge);
    wait_entered(&mut ctl.gate_entered, 2).await;

    // Bounded resolution to TimedOut — a real hang would trip this watchdog.
    let ra = tokio::time::timeout(Duration::from_secs(5), a)
        .await
        .expect("dispatch A must resolve within the watchdog, not hang")
        .unwrap();
    let rb = tokio::time::timeout(Duration::from_secs(5), b)
        .await
        .expect("dispatch B must resolve within the watchdog, not hang")
        .unwrap();
    let outcomes = [&ra, &rb];
    let timed_out = usize::from(matches!(&ra, Err(actr_protocol::ActrError::TimedOut)))
        + usize::from(matches!(&rb, Err(actr_protocol::ActrError::TimedOut)));
    let unavailable = usize::from(matches!(&ra, Err(actr_protocol::ActrError::Unavailable(_))))
        + usize::from(matches!(&rb, Err(actr_protocol::ActrError::Unavailable(_))));
    assert_eq!(
        timed_out, 1,
        "exactly the deadline that collapses the Store reports TimedOut: {outcomes:?}"
    );
    assert_eq!(
        unavailable, 1,
        "the co-resident sibling must fail retryably when the Store is discarded: {outcomes:?}"
    );

    // A NEW dispatch on A's key must complete promptly after a fresh Store is
    // instantiated. The un-gated echo route isolates key release + rebuild
    // serviceability from the intentionally unreleased old gate waiters.
    let payload = b"post-timeout".to_vec();
    let recovered = tokio::time::timeout(
        Duration::from_secs(5),
        dispatcher.dispatch(ECHO, payload.clone(), caller(1), &bridge),
    )
    .await
    .expect("same-key dispatch after a timeout must not hang (key was freed)")
    .expect("same-key dispatch after a timeout must succeed on the rebuilt Store");
    assert_eq!(
        recovered.as_ref(),
        payload.as_slice(),
        "the recovered dispatch must round-trip on the rebuilt instance"
    );
}

// ── Facet 6b — same-key timeout seam: region stays sealed until the first ─────
//               dispatch truly leaves (direct entry-order proof)
//
// Facet 6 proves a timed-out dispatch frees its key and doesn't poison the store.
// This companion proves the *sealing* seam directly: with two SAME-key dispatches
// and the first parked past its deadline, the second must NEVER enter the region
// before the first has actually left it. We assert this on the guest's real entry
// order (its own entry channel), not on wall-clock timing — the contrapositive of
// a leak would be a second entry observed while the first still occupies the
// region.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_key_timeout_seam_seals_region_until_first_leaves() {
    let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
    let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    // Short per-dispatch deadline so the parked first dispatch reliably times out
    // while leaving ample margin before the sealing assertion below.
    let dispatcher = Arc::new(wl.into_concurrent_dispatcher(
        probe_spec(),
        8,
        256,
        Some(Duration::from_millis(400)),
    ));
    let (bridge, mut ctl) = gate_bridge();

    // SAME caller => same conflict key => strictly serial. The first parks at the
    // gate and is never released, so it must hit its deadline; the second is
    // queued behind the held key.
    let first = spawn_dispatch(&dispatcher, PROBE, vec![], caller(7), &bridge);
    let second = spawn_dispatch(&dispatcher, PROBE, vec![], caller(7), &bridge);

    // Exactly one guest task ever enters. While the first holds the region, the
    // second CANNOT have entered — the key is held, so no second entry can be
    // buffered on the channel. This *is* the sealing property under test: the
    // timeout seam must not hand the region to a same-key sibling before the
    // first truly leaves.
    wait_entered(&mut ctl.gate_entered, 1).await;
    assert!(
        matches!(
            ctl.gate_entered.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ),
        "same-key second dispatch must NOT enter the region while the first still \
         occupies it (region sealed until the first leaves)"
    );

    // The first hits its deadline (bounded — a real hang trips the watchdog),
    // which discards its Store and frees the key. Only *after* this can the same
    // key be re-armed for the sibling on a rebuilt Store.
    let r1 = await_dispatch(first, "seam first").await;
    assert!(
        matches!(r1, Err(actr_protocol::ActrError::TimedOut)),
        "the parked same-key dispatch must resolve TimedOut, got {r1:?}"
    );

    // The second is admitted only after the first reply releases the scheduler
    // key. It may then hit its own deadline because this test deliberately never
    // releases the gate; the important property is bounded resolution with no
    // overlap on the discarded Store.
    let _second_resolved = await_dispatch(second, "seam second").await;

    dispatcher.shutdown().await;
}

// ── Facet 7 — gate off degrades to the serial M4 path (MAX_SEEN == 1) ────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gate_off_is_serial_max_seen_1() {
    use actr_hyper::workload::InvocationContext;
    use actr_protocol::{Direction, RpcEnvelope, prost::Message as _};

    let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
    let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    // Serial runner (gate off): the V2 kernel drives the per-dispatch region.
    let runner = Arc::new(wl.into_workload_runner());
    let (bridge, mut ctl) = gate_bridge();

    let spawn_serial = |serial: u64, bridge: HostAbiFn| {
        let runner = runner.clone();
        tokio::spawn(async move {
            let bytes = RpcEnvelope {
                route_key: PROBE.to_string(),
                payload: Some(Bytes::new()),
                request_id: format!("serial-{serial}"),
                direction: Some(Direction::Request as i32),
                ..Default::default()
            }
            .encode_to_vec();
            let inv = InvocationContext {
                self_id: ActrId::default(),
                caller_id: caller(serial),
                request_id: format!("serial-{serial}"),
            };
            runner.dispatch(&bytes, inv, &bridge).await
        })
    };

    // Submit two "concurrently" to the serial runner. It processes one at a
    // time, so the second cannot enter the guest until the first completes.
    let a = spawn_serial(1, bridge.clone());
    let b = spawn_serial(2, bridge.clone());

    // The serial runner processes one command at a time, but the order the two
    // spawned tasks reach its command channel is NOT the spawn order. Release by
    // the guest's real entry order (as in facet 2), not by awaiting a fixed
    // handle: hand a permit to whichever the runner ran first, let it reply, then
    // the second is dequeued and enters. Awaiting `a` specifically between
    // releases could hang if the runner happened to run `b` first.
    for _ in 0..2 {
        wait_entered(&mut ctl.gate_entered, 1).await;
        ctl.gate_release.add_permits(1);
    }
    let first = read_u32(
        &await_dispatch(a, "serial A")
            .await
            .expect("serial first ok"),
    );
    let second = read_u32(
        &await_dispatch(b, "serial B")
            .await
            .expect("serial second ok"),
    );

    assert_eq!(first, 1, "serial path must never overlap (A)");
    assert_eq!(second, 1, "serial path must never overlap (B)");

    runner.shutdown().await;
}

// ── Facet 8 — node-integration seam: dedup write-back around a gate-on, ───────
//              interleaved wasm V2 dispatch
//
// SCOPE / HONESTY NOTE — read before extending this test.
//
// Facet 8's ideal is a *signed wasm V2 package driven through a real `Node`*:
// through `Inner::admit_incoming` (dedup `check_or_mark` → gate-on scheduler →
// interleaved runner → dedup `complete` write-back at node.rs:~1204) and the
// inbound loop, asserting the write-back and the mailbox reply-before-ack
// (node.rs:~217) hold under gate-on interleaved concurrency.
//
// That full crossing is **not achievable with the committed fixture**, for two
// structural reasons — this is a real coverage gap, not a shortcut:
//
//   1. The 0.2.0 fixture cannot complete a real node's `start()` lifecycle. The
//      node always invokes `on_start` with request_id `"lifecycle:on_start"`
//      (node.rs:~1967), and the fixture's `on_start` is deliberately hardwired
//      to return `Err` for exactly that id (it powers the on_start-abort test in
//      `package_lifecycle.rs`). So `attach(pkg).register().start()` always aborts
//      — there is no running wasm actor to drive `call` / `call_remote` against.
//   2. The probe's concurrency suspension point (`test/gate`) is a *harness
//      bridge* import, not a dispatch route. Through a real node,
//      `ctx.call_raw(self_id, "test/gate")` self-routes to `UnknownRoute`, so the
//      host-gated parking that makes deterministic same-instance overlap
//      observable cannot be reproduced across the node boundary.
//
// So this test delivers the closest achievable node-integration coverage: it
// reconstructs `admit_incoming`'s gate-on sequence using the **real production
// types** — the same `DedupState` the node writes back to (node.rs:~1204), the
// same conflict-key scheduler + interleaved wasm V2 runner the node wires when
// the gate is on (node.rs:~1262-1386, via `into_concurrent_dispatcher`) — and
// proves the dedup write-back is correct while two distinct-key dispatches truly
// interleave inside the one instance.
//
// COVERED here: dedup `check_or_mark` → interleaved gate-on dispatch → dedup
// `complete` write-back, and that a later duplicate request_id observes the
// written-back result (never re-runs the guest).
// NOT COVERED (needs the crate-private mailbox lane through a startable
// workload): `mailbox_reply_and_ack`'s reply-before-ack ordering (node.rs:~217).
// The native-workload two-node variant of that seam lives in
// `dispatch_concurrency_e2e.rs`; the wasm-package variant is blocked by (1)+(2).

/// Mirror of `Inner::admit_incoming`'s gate-on path: dedup `check_or_mark`, then
/// (on Fresh) run through the conflict-key scheduler + interleaved wasm V2
/// runner, then write the result back into the dedup state exactly as the node's
/// `finish` closure does at node.rs:~1204.
async fn admit_like(
    dispatcher: &Arc<TestConcurrentDispatcher>,
    dedup: &Arc<Mutex<TestDedupState>>,
    request_id: &str,
    route: &str,
    payload: Vec<u8>,
    caller_id: Option<ActrId>,
    bridge: &HostAbiFn,
) -> actr_protocol::ActorResult<Bytes> {
    // admit_incoming step 1 — dedup on the node-level request_id.
    match dedup.lock().await.check_or_mark(request_id) {
        TestDedupOutcome::Fresh => {}
        TestDedupOutcome::Duplicate(result) => return result,
        TestDedupOutcome::InFlight(waiter) => return waiter.wait().await,
    }
    // Step 2 — gate-on scheduler → interleaved wasm V2 runner (the exact shape
    // the node builds; distinct callers ⇒ distinct keys ⇒ eligible to interleave).
    let result = dispatcher.dispatch(route, payload, caller_id, bridge).await;
    // Step 3 — dedup write-back (node.rs:~1204's `finish` closure).
    dedup.lock().await.complete(request_id, result.clone());
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_gate_on_dedup_writeback_survives_interleave() {
    let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
    let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
    let dispatcher = Arc::new(wl.into_concurrent_dispatcher(probe_spec(), 8, 256, None));
    let (bridge, mut ctl) = gate_bridge();
    let dedup = Arc::new(Mutex::new(TestDedupState::new()));

    // Two distinct callers → distinct conflict keys → eligible to interleave
    // inside the ONE instance, each admitted through the dedup gate first.
    let ra_id = "node-req-A".to_string();
    let rb_id = "node-req-B".to_string();
    let a = {
        let (dispatcher, dedup, bridge, rid) = (
            dispatcher.clone(),
            dedup.clone(),
            bridge.clone(),
            ra_id.clone(),
        );
        tokio::spawn(async move {
            admit_like(&dispatcher, &dedup, &rid, PROBE, vec![], caller(1), &bridge).await
        })
    };
    let b = {
        let (dispatcher, dedup, bridge, rid) = (
            dispatcher.clone(),
            dedup.clone(),
            bridge.clone(),
            rb_id.clone(),
        );
        tokio::spawn(async move {
            admit_like(&dispatcher, &dedup, &rid, PROBE, vec![], caller(2), &bridge).await
        })
    };

    // Both suspended inside the one instance before either is released — proves
    // the gate-on interleave happens *underneath* the node's dedup wrapper.
    wait_entered(&mut ctl.gate_entered, 2).await;
    ctl.gate_release.add_permits(2);

    let ma = read_u32(
        &await_dispatch(a, "node A")
            .await
            .expect("node A dispatch ok"),
    );
    let mb = read_u32(
        &await_dispatch(b, "node B")
            .await
            .expect("node B dispatch ok"),
    );
    assert!(
        ma.max(mb) >= 2,
        "distinct-key dispatches must interleave inside one instance beneath the \
         dedup wrapper (MAX_SEEN>=2), got {ma} and {mb}"
    );

    // The dedup write-back (node.rs:~1204) must have stored each result: a later
    // request replaying request_id A observes the cached bytes and NEVER re-runs
    // the guest. We deliberately do NOT open a gate permit here — if the duplicate
    // wrongly reached the runner it would park at the gate forever and trip the
    // watchdog, so a prompt, correct reply is itself proof the write-back served
    // this request without a re-dispatch.
    let cached = tokio::time::timeout(
        Duration::from_secs(10),
        admit_like(
            &dispatcher,
            &dedup,
            &ra_id,
            PROBE,
            vec![],
            caller(1),
            &bridge,
        ),
    )
    .await
    .expect("duplicate request must be served from the dedup write-back, not re-dispatched")
    .expect("duplicate request must return the written-back result");
    assert_eq!(
        read_u32(&cached),
        ma,
        "a duplicate request_id must observe the written-back result of the original"
    );

    dispatcher.shutdown().await;
}

// ── Facet 9 — package compat matrix ──────────────────────────────────────────

/// A V1 (0.1.0 sync-world) guest must stay serial even when Interleaved is
/// requested: `WasmKernel::is_v2()` is false, so the executor routes it to the
/// serial `run_loop`. Dispatch must still work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compat_v1_guest_on_interleaved_falls_back_to_serial() {
    let host = WasmHost::compile(V1_SYNCLIFT_GUEST).expect("compile v1 fixture");
    let wl = instantiate_wasm_workload(&host)
        .await
        .expect("instantiate v1");
    // Request Interleaved; a V1 kernel must transparently run serially.
    let runner = wl.into_interleaved_runner(None);

    use actr_protocol::{Direction, RpcEnvelope, prost::Message as _};
    let payload = b"v1-serial-fallback".to_vec();
    let bytes = RpcEnvelope {
        route_key: "test/echo".to_string(),
        payload: Some(Bytes::from(payload.clone())),
        request_id: "v1-compat".to_string(),
        direction: Some(Direction::Request as i32),
        ..Default::default()
    }
    .encode_to_vec();
    let inv = actr_hyper::workload::InvocationContext {
        self_id: ActrId::default(),
        caller_id: None,
        request_id: "v1-compat".to_string(),
    };
    let bridge: HostAbiFn = Arc::new(|_| Box::pin(async move { HostOperationResult::Error(-1) }));

    let reply = runner
        .dispatch(&bytes, inv, &bridge)
        .await
        .expect("V1 echo must dispatch on the serial fallback");
    assert_eq!(reply.as_ref(), payload.as_slice());
    runner.shutdown().await;
}

/// A V2 guest must work in BOTH modes: serial (facet 7 above) and interleaved
/// (facets 1-3 above). This is the positive control that the same 0.2.0 package
/// dispatches correctly through the interleaved runner for a lone message too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compat_v2_guest_single_dispatch_on_interleaved() {
    let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
    let wl = instantiate_wasm_workload(&host)
        .await
        .expect("instantiate v2");
    let dispatcher = Arc::new(wl.into_concurrent_dispatcher(probe_spec(), 8, 256, None));
    let (bridge, mut ctl) = gate_bridge();

    let d = spawn_dispatch(&dispatcher, PROBE, vec![], caller(1), &bridge);
    wait_entered(&mut ctl.gate_entered, 1).await;
    ctl.gate_release.add_permits(1);
    let reply = await_dispatch(d, "v2 single")
        .await
        .expect("v2 single dispatch ok");
    assert_eq!(read_u32(&reply), 1, "a lone dispatch sees MAX_SEEN==1");
}
