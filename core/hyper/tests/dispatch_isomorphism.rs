//! M6 §2 — WASM/native dual-basis conflict-key **isomorphism**.
//!
//! Proves that the two workloads whose dispatch the interleaved runner may
//! multiplex — a native `Linked` guest (`executor::run_loop_interleaved`) and a
//! WASM V2 guest (`WasmWorkloadV2::run_interleaved`) — satisfy the SAME
//! conflict-key concurrency contract when driven through the SAME production
//! shape (a budgeted conflict-key scheduler in front of the interleaved runner).
//!
//! The proof uses ONE guest source (`tests/wasm_actor_fixture/src/lib.rs`),
//! compiled two ways: natively as a `Linked` workload (the `entry!` macro
//! expands to nothing off-wasm) and as a WASM Component (`build.rs`). Its
//! `test/inflight-probe` route reports the peak concurrency the instance ever
//! observed (MAX_SEEN, a per-instance counter), and each basis exposes the SAME
//! gate — a guest→host suspension point held open until the test releases it —
//! so evidence is gathered with zero sleep-based coordination.
//!
//! This is a pure test increment: it changes no concurrency mechanism and no
//! `default` feature gating.
//!
//! ## Property matrix (property × basis)
//!
//! | Property                                   | native `Linked` | WASM V2 |
//! |--------------------------------------------|-----------------|---------|
//! | P1 distinct keys interleave (MAX_SEEN≥2)   | ✅ `p1::native` | ✅ `p1::wasm` |
//! | P2 same key strictly serial (MAX_SEEN==1)  | ✅ `p2::native` | ✅ `p2::wasm` |
//! | P3 concurrency capped at budget C          | ✅ `p3::native` | ✅ `p3::wasm` |
//! | P5 timeout hard-cancels unsafe execution    | ✅ terminate runner | ✅ rebuild Store |
//! | P6 keyless ⇒ degenerate serial (MAX_SEEN==1)| ✅ `p6::native` | ✅ `p6::wasm` |
//! | SA default-on keyless ⇒ serial, no scheduler| ✅ `sa::native` | ✅ `sa::wasm` |
//! | P4 panic/trap fails co-resident work safely | ✅ terminate runner | ✅ rebuild Store |
//!
//! ### P4 — same fail-closed rule, different recovery capability
//!
//! Both bases stop co-resident work after an unexpected guest fault. Continuing
//! would expose shared state that may have been partially mutated before the
//! unwind/trap. Their post-fault availability differs only because the host can
//! reconstruct one basis but not the other:
//!
//! * **native** — a dispatch that panics after a host await is caught by
//!   `executor::run_loop_interleaved`; the triggering caller receives
//!   `Internal`, then the runner terminates and drops all siblings/queued work.
//!   A generic `LinkedWorkloadHandle` has no reconstruction factory, so later
//!   calls remain unavailable.
//! * **WASM** — a guest trap unwinds the whole resident `run_concurrent` region,
//!   so every co-resident sibling fails together and the instance is rebuilt
//!   (a shared linear memory CAN be left mid-mutation, so tearing the region
//!   down is the only safe recovery).
//!
//! The two P4 tests therefore both assert sibling failure, then assert the
//! intentionally different recovery result: native follow-up is unavailable;
//! WASM rebuilds and a fresh probe reports MAX_SEEN==1.
//!
//! ### P5 — same hard-cancellation rule, different recovery capability
//!
//! Dropping either future is not proof that execution stopped: Wasmtime 46 keeps
//! a `call_concurrent` guest task alive, and an arbitrary native future has no
//! cancellation-safety contract. Both runners therefore fail co-resident work
//! before releasing scheduler keys. WASM can recover by dropping and rebuilding
//! its Store; a linked native actor has no generic re-instantiation mechanism,
//! so its runner terminates and later calls are unavailable.
//!
//! ### SA — strategy-A default-on keyless serialization (M6 §1)
//!
//! Default-on turns the dispatch gate on by default, but a keyless actor must
//! stay bit-for-bit serial with NO scheduler spawned. The routing decision is
//! unit-tested (`lifecycle::node_tests::scheduler_engaged_*`); the *behaviour* is
//! proven here on both bases via `prop_default_on_keyless_serial`, which drives a
//! scheduler-less `RunnerMode::Serial` runner (the exact keyless default-on
//! shape) and asserts distinct callers still never interleave (MAX_SEEN == 1).
//! A stress variant runs it 25× to catch any nondeterministic concurrency leak.

#![cfg(all(
    feature = "wasm-engine",
    feature = "test-utils",
    actr_wasm_fixture_available
))]

use std::sync::Arc;
use std::time::Duration;

use actr_hyper::ConflictKeySpec;
use actr_hyper::test_support::concurrency::{
    BOOM, ECHO, GateControls, PROBE, await_dispatch, caller, gate_bridge, probe_spec, read_u32,
    spawn_dispatch, spawn_native_gate, wait_entered,
};
use actr_hyper::test_support::{
    ConcurrentDispatch, TestConcurrentDispatcher, TestNativeConcurrentDispatcher,
    TestNativeSerialDispatcher, TestSerialDispatcher, instantiate_wasm_workload,
};
use actr_hyper::wasm::WasmHost;
use actr_hyper::workload::{HostAbiFn, HostOperationResult};

// WASM basis: the compiled Component bytes (built by `build.rs`).
#[path = "wasm_actor_fixture.rs"]
mod wasm_actor_fixture;

fn fixture_bytes() -> &'static [u8] {
    wasm_actor_fixture::WASM_ACTOR_FIXTURE
}

// Native basis: the SAME guest source, compiled into this test binary. Off
// wasm32 the `entry!` macro expands to nothing, leaving only the plain
// `DoubleActor` / `DoubleDispatcher` types. The `unexpected_cfgs` allow silences
// the guest crate's `cfg(feature = "cdylib")` gate, which is not a feature of
// `actr-hyper` (the fixture crate silences it in its own manifest; the `#[path]`
// include compiles here under this crate's lint config instead).
#[allow(unexpected_cfgs)]
#[path = "wasm_actor_fixture/src/lib.rs"]
mod fixture_native;

/// A conflict-key spec that declares NO routes, so every dispatch — regardless
/// of caller — projects to the global [`ConflictKey::Serial`] singleton and the
/// scheduler serializes them all. This is the "keyless / gate-degenerate" case
/// (P6): even distinct callers cannot interleave.
fn keyless_spec() -> ConflictKeySpec {
    ConflictKeySpec::builder()
        .build()
        .expect("build empty (keyless) conflict-key spec")
}

// ─────────────────────────────────────────────────────────────────────────────
// Basis abstraction: one interface, two implementations. Every isomorphic
// property is written ONCE, generic over `B: Basis`, and the `iso_test!` macro
// instantiates it for both bases.
// ─────────────────────────────────────────────────────────────────────────────

/// One conflict-key dispatcher basis: how to stand up the production gate-on
/// shape and its matching gate harness.
#[async_trait::async_trait]
trait Basis {
    type Dispatcher: ConcurrentDispatch + 'static;
    /// The scheduler-less serial runner this basis builds for the keyless
    /// default-on path (strategy A).
    type SerialDispatcher: ConcurrentDispatch + 'static;
    /// Human-readable label used in assertion messages.
    const NAME: &'static str;
    /// Whether hard cancellation can construct a fresh actor instance.
    const RECOVERS_AFTER_TIMEOUT: bool;

    /// Build a dispatcher (budgeted conflict-key scheduler → interleaved runner)
    /// over `spec` with an optional per-dispatch deadline, plus the guest→host
    /// bridge and the [`GateControls`] a test uses to hold guest calls suspended
    /// and release them deterministically.
    async fn build_with_timeout(
        spec: ConflictKeySpec,
        budget: usize,
        queue_cap: usize,
        dispatch_timeout: Option<Duration>,
    ) -> (Arc<Self::Dispatcher>, HostAbiFn, GateControls);

    /// Convenience: [`Self::build_with_timeout`] with no deadline. (Kept as a
    /// required method rather than a provided default so `async_trait` does not
    /// impose an extra `Self: Send` bound on generic callers.)
    async fn build(
        spec: ConflictKeySpec,
        budget: usize,
        queue_cap: usize,
    ) -> (Arc<Self::Dispatcher>, HostAbiFn, GateControls);

    /// Build the keyless default-on shape: a `RunnerMode::Serial` runner with NO
    /// scheduler, plus the same gate harness. Proves default-on keyless stays
    /// serial (strategy A) behaviourally, per basis.
    async fn build_serial() -> (Arc<Self::SerialDispatcher>, HostAbiFn, GateControls);
}

/// WASM V2 basis — a resident `run_concurrent` region.
struct WasmBasis;

#[async_trait::async_trait]
impl Basis for WasmBasis {
    type Dispatcher = TestConcurrentDispatcher;
    type SerialDispatcher = TestSerialDispatcher;
    const NAME: &'static str = "wasm";
    const RECOVERS_AFTER_TIMEOUT: bool = true;

    async fn build_with_timeout(
        spec: ConflictKeySpec,
        budget: usize,
        queue_cap: usize,
        dispatch_timeout: Option<Duration>,
    ) -> (Arc<Self::Dispatcher>, HostAbiFn, GateControls) {
        let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
        let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
        let dispatcher =
            Arc::new(wl.into_concurrent_dispatcher(spec, budget, queue_cap, dispatch_timeout));
        let (bridge, ctl) = gate_bridge();
        (dispatcher, bridge, ctl)
    }

    async fn build(
        spec: ConflictKeySpec,
        budget: usize,
        queue_cap: usize,
    ) -> (Arc<Self::Dispatcher>, HostAbiFn, GateControls) {
        Self::build_with_timeout(spec, budget, queue_cap, None).await
    }

    async fn build_serial() -> (Arc<Self::SerialDispatcher>, HostAbiFn, GateControls) {
        let host = WasmHost::compile(fixture_bytes()).expect("compile v2 fixture");
        let wl = instantiate_wasm_workload(&host).await.expect("instantiate");
        let dispatcher = Arc::new(wl.into_serial_dispatcher());
        let (bridge, ctl) = gate_bridge();
        (dispatcher, bridge, ctl)
    }
}

/// Native `Linked` basis — a `FuturesUnordered` of `&self` dispatches.
struct NativeBasis;

#[async_trait::async_trait]
impl Basis for NativeBasis {
    type Dispatcher = TestNativeConcurrentDispatcher;
    type SerialDispatcher = TestNativeSerialDispatcher;
    const NAME: &'static str = "native";
    const RECOVERS_AFTER_TIMEOUT: bool = false;

    async fn build_with_timeout(
        spec: ConflictKeySpec,
        budget: usize,
        queue_cap: usize,
        dispatch_timeout: Option<Duration>,
    ) -> (Arc<Self::Dispatcher>, HostAbiFn, GateControls) {
        let dispatcher = Arc::new(TestNativeConcurrentDispatcher::spawn(
            fixture_native::DoubleActor::default(),
            spec,
            budget,
            queue_cap,
            dispatch_timeout,
        ));
        // The gate reads the shared HostTransport; the `HostAbiFn` is ignored by
        // the native runner and supplied only for signature parity.
        let ctl = spawn_native_gate(dispatcher.host_transport());
        let bridge: HostAbiFn = Arc::new(|_| Box::pin(async { HostOperationResult::Done }));
        (dispatcher, bridge, ctl)
    }

    async fn build(
        spec: ConflictKeySpec,
        budget: usize,
        queue_cap: usize,
    ) -> (Arc<Self::Dispatcher>, HostAbiFn, GateControls) {
        Self::build_with_timeout(spec, budget, queue_cap, None).await
    }

    async fn build_serial() -> (Arc<Self::SerialDispatcher>, HostAbiFn, GateControls) {
        let dispatcher = Arc::new(TestNativeSerialDispatcher::spawn(
            fixture_native::DoubleActor::default(),
        ));
        let ctl = spawn_native_gate(dispatcher.host_transport());
        let bridge: HostAbiFn = Arc::new(|_| Box::pin(async { HostOperationResult::Done }));
        (dispatcher, bridge, ctl)
    }
}

/// Emit one `#[tokio::test]` per basis for a generic property body.
macro_rules! iso_test {
    ($module:ident, $body:ident) => {
        mod $module {
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn native() {
                super::$body::<super::NativeBasis>().await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn wasm() {
                super::$body::<super::WasmBasis>().await;
            }
        }
    };
}

// ── P1 — distinct keys truly interleave (MAX_SEEN >= 2) ──────────────────────

async fn prop_distinct_keys_interleave<B: Basis>() {
    let (d, bridge, mut ctl) = B::build(probe_spec(), 8, 256).await;

    // Two distinct callers → distinct conflict keys → eligible to run at once.
    let d1 = spawn_dispatch(&d, PROBE, vec![], caller(1), &bridge);
    let d2 = spawn_dispatch(&d, PROBE, vec![], caller(2), &bridge);

    // Both must be suspended inside the ONE instance before either is released.
    wait_entered(&mut ctl.gate_entered, 2).await;
    ctl.gate_release.add_permits(2);

    let m1 = read_u32(&await_dispatch(d1, "p1 d1").await.expect("d1 ok"));
    let m2 = read_u32(&await_dispatch(d2, "p1 d2").await.expect("d2 ok"));

    assert!(
        m1.max(m2) >= 2,
        "[{}] distinct-key dispatches must interleave inside one instance \
         (MAX_SEEN>=2), got {m1} and {m2}",
        B::NAME
    );
    d.shutdown().await;
}
iso_test!(p1_distinct_keys_interleave, prop_distinct_keys_interleave);

// ── P2 — same key stays strictly serial (MAX_SEEN == 1) ──────────────────────

async fn prop_same_key_serial<B: Basis>() {
    let (d, bridge, mut ctl) = B::build(probe_spec(), 8, 256).await;

    // SAME caller → same conflict key → the scheduler must serialize them.
    let a = spawn_dispatch(&d, PROBE, vec![], caller(7), &bridge);
    let b = spawn_dispatch(&d, PROBE, vec![], caller(7), &bridge);

    // Only ONE may be in the guest at a time, so release strictly by the guest's
    // real *entry* order, never by spawn order (the scheduler is free to admit
    // either submission first). Awaiting a fixed handle between releases would be
    // a latent deadlock if the other was admitted first; release-by-entry keeps
    // this deterministic regardless of dequeue order.
    for _ in 0..2 {
        wait_entered(&mut ctl.gate_entered, 1).await;
        ctl.gate_release.add_permits(1);
    }
    let first = read_u32(&await_dispatch(a, "p2 A").await.expect("A ok"));
    let second = read_u32(&await_dispatch(b, "p2 B").await.expect("B ok"));

    assert_eq!(
        first,
        1,
        "[{}] same-key dispatch A must never overlap a sibling",
        B::NAME
    );
    assert_eq!(
        second,
        1,
        "[{}] same-key dispatch B must never overlap a sibling",
        B::NAME
    );
    d.shutdown().await;
}
iso_test!(p2_same_key_serial, prop_same_key_serial);

// ── P3 — concurrency is capped at the scheduler budget C ─────────────────────

async fn prop_budget_cap<B: Basis>() {
    const C: usize = 3;
    // queue_cap high enough to admit all submissions; budget bounds in-flight.
    let (d, bridge, mut ctl) = B::build(probe_spec(), C, 256).await;

    // C+2 distinct keys all want to run; only C may be in flight at once.
    let mut handles = Vec::new();
    for i in 0..(C as u64 + 2) {
        handles.push(spawn_dispatch(&d, PROBE, vec![], caller(100 + i), &bridge));
    }

    // Exactly C reach the guest and park; the extra 2 wait for a budget slot.
    wait_entered(&mut ctl.gate_entered, C).await;
    // Release everything; as the first C drain, the extra 2 are admitted.
    ctl.gate_release.add_permits(C + 2);

    let mut peak = 0u32;
    for h in handles {
        peak = peak.max(read_u32(
            &await_dispatch(h, "p3 dispatch").await.expect("ok"),
        ));
    }
    assert_eq!(
        peak,
        C as u32,
        "[{}] peak in-flight must equal the budget C={C} (never exceed it)",
        B::NAME
    );
    d.shutdown().await;
}
iso_test!(p3_budget_cap, prop_budget_cap);

// ── P6 — keyless spec degenerates to serial (MAX_SEEN == 1) ──────────────────

async fn prop_keyless_serial<B: Basis>() {
    // No route declared → every dispatch projects to the global Serial key, so
    // even DISTINCT callers cannot interleave.
    let (d, bridge, mut ctl) = B::build(keyless_spec(), 8, 256).await;

    // Distinct callers, yet keyless ⇒ same Serial key ⇒ strictly serial.
    let a = spawn_dispatch(&d, PROBE, vec![], caller(1), &bridge);
    let b = spawn_dispatch(&d, PROBE, vec![], caller(2), &bridge);

    for _ in 0..2 {
        wait_entered(&mut ctl.gate_entered, 1).await;
        ctl.gate_release.add_permits(1);
    }
    let first = read_u32(&await_dispatch(a, "p6 A").await.expect("A ok"));
    let second = read_u32(&await_dispatch(b, "p6 B").await.expect("B ok"));

    assert_eq!(
        first,
        1,
        "[{}] keyless dispatch A must degrade to serial (no interleave)",
        B::NAME
    );
    assert_eq!(
        second,
        1,
        "[{}] keyless dispatch B must degrade to serial (no interleave)",
        B::NAME
    );
    d.shutdown().await;
}
iso_test!(p6_keyless_serial, prop_keyless_serial);

// ── P5 — per-dispatch timeout: hard cancel, bounded, safe recovery policy ───
//
// A parked-forever dispatch must resolve within its deadline, and no sibling may
// keep using potentially partial state. WASM rebuilds a fresh Store; native
// linked workloads terminate because the host cannot generically reconstruct
// them. No sleeps: the gate bridge parks guest calls forever.

async fn prop_dispatch_timeout<B: Basis>() {
    // 300ms per-dispatch deadline, enforced inside the interleaved runner.
    let (d, bridge, mut ctl) =
        B::build_with_timeout(probe_spec(), 8, 256, Some(Duration::from_millis(300))).await;

    // Two distinct-key dispatches park at the gate forever. The first deadline
    // hard-cancels the actor basis and fails the co-resident sibling.
    let a = spawn_dispatch(&d, PROBE, vec![], caller(1), &bridge);
    let b = spawn_dispatch(&d, PROBE, vec![], caller(2), &bridge);
    wait_entered(&mut ctl.gate_entered, 2).await;

    // Bounded resolution to TimedOut — a real hang trips the await watchdog.
    let ra = await_dispatch(a, "p5 A").await;
    let rb = await_dispatch(b, "p5 B").await;
    let outcomes = [&ra, &rb];
    let timed_out = usize::from(matches!(&ra, Err(actr_protocol::ActrError::TimedOut)))
        + usize::from(matches!(&rb, Err(actr_protocol::ActrError::TimedOut)));
    let unavailable = usize::from(matches!(&ra, Err(actr_protocol::ActrError::Unavailable(_))))
        + usize::from(matches!(&rb, Err(actr_protocol::ActrError::Unavailable(_))));
    assert_eq!(
        timed_out,
        1,
        "[{}] exactly the triggering deadline reports TimedOut: {outcomes:?}",
        B::NAME
    );
    assert_eq!(
        unavailable,
        1,
        "[{}] the co-resident sibling must fail retryably: {outcomes:?}",
        B::NAME
    );

    // Probe the post-timeout policy on A's key. WASM must rebuild and answer;
    // native linked execution must reject because it cannot be reconstructed.
    // The un-gated ECHO route isolates that policy from old gate waiters.
    let payload = b"post-timeout".to_vec();
    let follow_up = tokio::time::timeout(
        Duration::from_secs(5),
        d.dispatch(ECHO, payload.clone(), caller(1), &bridge),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "[{}] same-key dispatch after a timeout must not hang",
            B::NAME
        )
    });
    if B::RECOVERS_AFTER_TIMEOUT {
        let recovered = follow_up.unwrap_or_else(|e| {
            panic!(
                "[{}] same-key dispatch must succeed on a rebuilt instance: {e:?}",
                B::NAME
            )
        });
        assert_eq!(
            recovered.as_ref(),
            payload.as_slice(),
            "[{}] rebuilt instance must round-trip the follow-up",
            B::NAME
        );
    } else {
        assert!(
            matches!(follow_up, Err(actr_protocol::ActrError::Unavailable(_))),
            "[{}] linked actor must stay unavailable after unsafe cancellation, got {follow_up:?}",
            B::NAME
        );
    }
    d.shutdown().await;
}
iso_test!(p5_dispatch_timeout, prop_dispatch_timeout);

// ── SA — strategy-A default-on keyless: serial, no scheduler (MAX_SEEN == 1) ──
//
// Default-on turns the gate on, but a keyless actor must stay bit-for-bit the
// M4 serial runner with NO scheduler. This drives that exact shape (a
// scheduler-less `RunnerMode::Serial` runner) and proves distinct callers still
// never interleave — the strategy-A promise, behaviourally, per basis.

async fn prop_default_on_keyless_serial<B: Basis>() {
    let (d, bridge, mut ctl) = B::build_serial().await;

    // DISTINCT callers (would be distinct keys under a scheduler), yet the
    // keyless serial runner processes one at a time — so neither may overlap.
    let a = spawn_dispatch(&d, PROBE, vec![], caller(1), &bridge);
    let b = spawn_dispatch(&d, PROBE, vec![], caller(2), &bridge);

    // Release strictly by the guest's real entry order (never spawn order): the
    // serial runner admits one at a time, so exactly one enters, we release it,
    // it replies, then the next enters. Awaiting a fixed handle between releases
    // could deadlock if the runner ran the other first.
    for _ in 0..2 {
        wait_entered(&mut ctl.gate_entered, 1).await;
        ctl.gate_release.add_permits(1);
    }
    let first = read_u32(&await_dispatch(a, "sa A").await.expect("A ok"));
    let second = read_u32(&await_dispatch(b, "sa B").await.expect("B ok"));

    assert_eq!(
        first,
        1,
        "[{}] default-on keyless must never overlap (A) — strategy-A promise",
        B::NAME
    );
    assert_eq!(
        second,
        1,
        "[{}] default-on keyless must never overlap (B) — strategy-A promise",
        B::NAME
    );
    d.shutdown().await;
}
iso_test!(sa_default_on_keyless_serial, prop_default_on_keyless_serial);

// Stress the keyless serial face: repeat the strategy-A serialization proof 25×
// per basis. A default-on concurrency leak would be nondeterministic, so a
// single pass could miss it; a MAX_SEEN>1 on any iteration fails the run.
async fn stress_default_on_keyless_serial<B: Basis>() {
    for _ in 0..25 {
        prop_default_on_keyless_serial::<B>().await;
    }
}
iso_test!(
    sa_default_on_keyless_serial_stress,
    stress_default_on_keyless_serial
);

// ── P4 — fault containment: fail co-resident work, recover if possible ─
//
// See the module header. These remain separate because native termination and
// WASM rebuild have different post-fault availability.

/// Native `Linked`: return `Internal` to the panicking dispatch, then terminate
/// the runner and fail every co-resident sibling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p4_native_panic_terminates_runner_and_fails_siblings() {
    const SIBLINGS: u64 = 3;
    let (d, bridge, mut ctl) = NativeBasis::build(probe_spec(), 8, 256).await;

    // SIBLINGS probes park at test/gate; one boom parks at test/double_impl.
    // All distinct keys, all in flight on the ONE native instance.
    let mut siblings = Vec::new();
    for i in 0..SIBLINGS {
        siblings.push(spawn_dispatch(&d, PROBE, vec![], caller(200 + i), &bridge));
    }
    let boom = spawn_dispatch(&d, BOOM, 1i32.to_le_bytes().to_vec(), caller(299), &bridge);

    wait_entered(&mut ctl.gate_entered, SIBLINGS as usize).await;
    wait_entered(&mut ctl.impl_entered, 1).await;

    // Release ONLY boom's host import: it returns, the guest panics after the
    // await, and `run_loop_interleaved` reports Internal before terminating the
    // runner and dropping every co-resident future.
    ctl.impl_release.add_permits(1);
    let boom_res = await_dispatch(boom, "p4-native boom").await;
    assert!(
        matches!(boom_res, Err(actr_protocol::ActrError::Internal(_))),
        "the panicking dispatch itself must receive Internal, got {boom_res:?}"
    );

    // Siblings stay parked at their host await until runner termination drops
    // them; none may resume against potentially partial native actor state.
    for (i, h) in siblings.into_iter().enumerate() {
        let result = await_dispatch(h, "p4-native sibling").await;
        assert!(
            matches!(result, Err(actr_protocol::ActrError::Unavailable(_))),
            "native sibling {i} must fail unavailable after a co-resident panic, got {result:?}"
        );
    }

    // A generic linked actor cannot be reconstructed, so the scheduler may
    // accept a follow-up but the terminated runner must reject it.
    let after = d.dispatch(ECHO, vec![], caller(777), &bridge).await;
    assert!(
        matches!(after, Err(actr_protocol::ActrError::Unavailable(_))),
        "native follow-up must remain unavailable after panic, got {after:?}"
    );
    d.shutdown().await;
}

/// WASM V2: an in-flight guest trap collapses the WHOLE region — every
/// co-resident sibling fails together and the instance is rebuilt (a follow-up
/// probe sees a fresh MAX_SEEN==1 on new linear memory).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p4_wasm_trap_collapses_region_then_rebuilds() {
    const SIBLINGS: u64 = 3;
    let (d, bridge, mut ctl) = WasmBasis::build(probe_spec(), 8, 256).await;

    let mut siblings = Vec::new();
    for i in 0..SIBLINGS {
        siblings.push(spawn_dispatch(&d, PROBE, vec![], caller(200 + i), &bridge));
    }
    let boom = spawn_dispatch(&d, BOOM, 1i32.to_le_bytes().to_vec(), caller(299), &bridge);

    wait_entered(&mut ctl.gate_entered, SIBLINGS as usize).await;
    wait_entered(&mut ctl.impl_entered, 1).await;

    // Release ONLY boom: the guest panics after the await, the wasm store traps,
    // and the whole run_concurrent region collapses — taking every in-flight
    // sibling down with it (whole-region teardown, not per-task isolation).
    ctl.impl_release.add_permits(1);
    let boom_res = await_dispatch(boom, "p4-wasm boom").await;
    assert!(boom_res.is_err(), "the trapping dispatch itself must fail");

    for (i, h) in siblings.into_iter().enumerate() {
        let res = await_dispatch(h, "p4-wasm sibling").await;
        assert!(
            res.is_err(),
            "wasm sibling {i} must FAIL when a co-resident dispatch traps (whole-region teardown)"
        );
    }

    // The instance rebuilds: a fresh dispatch succeeds AND reports MAX_SEEN==1,
    // which can only be true on fresh linear memory (the pre-trap in-flight peak
    // was SIBLINGS and never decremented on the torn-down region).
    ctl.gate_release.add_permits(1);
    let recovered = read_u32(
        &d.dispatch(PROBE, vec![], caller(777), &bridge)
            .await
            .expect("a dispatch after the trap must succeed on the rebuilt instance"),
    );
    assert_eq!(
        recovered, 1,
        "wasm: rebuild ⇒ post-trap probe sees MAX_SEEN==1 (fresh linear memory)"
    );
    d.shutdown().await;
}

// ── Control — an un-gated ECHO round-trips on both bases ─────────────────────
//
// A suspension-free positive control: proves the plumbing (scheduler → runner →
// guest → reply) is sound on each basis independent of the gate.

async fn prop_echo_round_trips<B: Basis>() {
    let (d, bridge, _ctl) = B::build(probe_spec(), 8, 256).await;
    let payload = b"iso-echo".to_vec();
    let reply = d
        .dispatch(ECHO, payload.clone(), caller(1), &bridge)
        .await
        .expect("echo must dispatch");
    assert_eq!(
        reply.as_ref(),
        payload.as_slice(),
        "[{}] echo must round-trip the payload unchanged",
        B::NAME
    );
    d.shutdown().await;
}
iso_test!(echo_round_trips, prop_echo_round_trips);
