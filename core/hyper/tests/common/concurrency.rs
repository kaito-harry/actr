//! Shared conflict-key concurrency harness for the M5 (`wasm_open_concurrency`)
//! and M6 (`dispatch_isomorphism`) suites.
//!
//! Everything a property test needs to drive a budgeted conflict-key scheduler
//! in front of an **interleaved** runner, gathered without sleep-based
//! coordination:
//!
//! * route constants + the conflict-key spec the fixture declares,
//! * a `caller(serial)` helper that controls the conflict key (distinct callers
//!   → distinct keys → eligible to interleave; same caller → same key → serial),
//! * the two guest→host **gate** bridges — one per basis — that hold a guest's
//!   `ctx.call_raw` suspension point parked until the test deterministically
//!   releases it, exposing the SAME [`GateControls`] shape either way, and
//! * watchdog-guarded spawn/await helpers so any orchestration accident fails
//!   the test fast instead of hanging CI.
//!
//! This module is compiled into the library (via `test_support`) so its `pub`
//! items are externally reachable and never trip per-integration-test-crate
//! dead-code warnings.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use actr_framework::Bytes;
use actr_protocol::{ActrId, ActrType, Realm};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;

use crate::workload::{HostAbiFn, HostOperation, HostOperationResult};
use crate::{ConflictKeySpec, KeySource};

use super::ConcurrentDispatch;

/// The same-instance interleave probe: increments a per-instance in-flight
/// counter, parks at `test/gate`, then reports the peak concurrency (MAX_SEEN)
/// this instance ever observed as a 4-byte LE u32.
pub const PROBE: &str = "test/inflight-probe";
/// A dispatch that parks at `test/double_impl` and then panics/traps after the
/// await returns — the fault-injection route.
pub const BOOM: &str = "test/boom-after-await";
/// An un-gated round-trip (no host import) — a suspension-free control.
pub const ECHO: &str = "test/echo";

/// A distinct caller per `serial`, so the conflict key derived from the sender
/// is distinct (or, for equal serials, identical).
pub fn caller(serial: u64) -> Option<ActrId> {
    Some(ActrId {
        realm: Realm { realm_id: 1 },
        serial_number: serial,
        r#type: ActrType {
            manufacturer: "test".to_string(),
            name: "fixture".to_string(),
            version: "0.2.0".to_string(),
        },
    })
}

/// Declares every probe route as keyed by the sender, so the test controls
/// concurrency purely through which `caller` it passes.
pub fn probe_spec() -> ConflictKeySpec {
    ConflictKeySpec::builder()
        .method(PROBE, KeySource::Sender)
        .method(BOOM, KeySource::Sender)
        .method(ECHO, KeySource::Sender)
        .build()
        .expect("build conflict-key spec")
}

/// Decode a 4-byte LE u32 reply (the guest's reported MAX_SEEN).
pub fn read_u32(b: &Bytes) -> u32 {
    assert!(
        b.len() >= 4,
        "reply must be a 4-byte LE u32, got {} bytes",
        b.len()
    );
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

/// Two independently-gated guest→host suspension points, exposed identically for
/// both bases:
/// * `test/gate`        — where `test/inflight-probe` parks,
/// * `test/double_impl` — where `test/boom-after-await` parks.
///
/// Each import signals an entry channel and then parks on its release semaphore,
/// so a test can hold N guest tasks suspended in the ONE instance at once and
/// release them deterministically — no sleeps.
pub struct GateControls {
    pub gate_entered: mpsc::UnboundedReceiver<()>,
    pub gate_release: Arc<Semaphore>,
    pub impl_entered: mpsc::UnboundedReceiver<()>,
    pub impl_release: Arc<Semaphore>,
    /// Total guest→host crossings observed (diagnostic; not asserted).
    pub calls: Arc<AtomicU64>,
}

/// WASM basis gate: an `HostAbiFn` bridge that intercepts the guest's
/// `ctx.call_raw` at the Component ABI boundary.
pub fn gate_bridge() -> (HostAbiFn, GateControls) {
    let (gate_tx, gate_rx) = mpsc::unbounded_channel();
    let (impl_tx, impl_rx) = mpsc::unbounded_channel();
    let gate_release = Arc::new(Semaphore::new(0));
    let impl_release = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicU64::new(0));

    let bridge: HostAbiFn = {
        let gate_release = gate_release.clone();
        let impl_release = impl_release.clone();
        let calls = calls.clone();
        Arc::new(move |op| {
            let gate_tx = gate_tx.clone();
            let impl_tx = impl_tx.clone();
            let gate_release = gate_release.clone();
            let impl_release = impl_release.clone();
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                match op {
                    HostOperation::CallRaw(req) if req.route_key == "test/gate" => {
                        let _ = gate_tx.send(());
                        gate_release
                            .acquire()
                            .await
                            .expect("gate semaphore open")
                            .forget();
                        HostOperationResult::Done
                    }
                    HostOperation::CallRaw(req) if req.route_key == "test/double_impl" => {
                        let _ = impl_tx.send(());
                        impl_release
                            .acquire()
                            .await
                            .expect("impl semaphore open")
                            .forget();
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
        })
    };
    (
        bridge,
        GateControls {
            gate_entered: gate_rx,
            gate_release,
            impl_entered: impl_rx,
            impl_release,
            calls,
        },
    )
}

/// Native basis gate: a reader over the shared [`crate::transport::HostTransport`]
/// backing a [`super::TestNativeConcurrentDispatcher`]. It drains the guest's
/// outbound `ctx.call_raw` requests raw (`recv_reliable_raw`, so they are not
/// self-completed), parks each on the same entry-channel + release-semaphore
/// discipline as [`gate_bridge`], and answers via `complete_response` /
/// `complete_error`. This is the native mirror of the WASM `HostAbiFn` bridge
/// and yields the SAME [`GateControls`].
///
/// Each request is handled on its own task so N distinct-key guest calls can be
/// parked at the gate simultaneously (the reader never blocks on a release
/// before draining the next request).
pub fn spawn_native_gate(transport: Arc<crate::transport::HostTransport>) -> GateControls {
    let (gate_tx, gate_rx) = mpsc::unbounded_channel();
    let (impl_tx, impl_rx) = mpsc::unbounded_channel();
    let gate_release = Arc::new(Semaphore::new(0));
    let impl_release = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicU64::new(0));

    {
        let transport = transport.clone();
        let gate_release = gate_release.clone();
        let impl_release = impl_release.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            while let Some(env) = transport.recv_reliable_raw().await {
                calls.fetch_add(1, Ordering::SeqCst);
                let rid = env.request_id.clone();
                let route = env.route_key.clone();
                let payload = env.payload.clone().unwrap_or_default();
                let transport = transport.clone();
                match route.as_str() {
                    "test/gate" => {
                        let gate_tx = gate_tx.clone();
                        let gate_release = gate_release.clone();
                        tokio::spawn(async move {
                            let _ = gate_tx.send(());
                            gate_release
                                .acquire()
                                .await
                                .expect("gate semaphore open")
                                .forget();
                            let _ = transport.complete_response(&rid, Bytes::new()).await;
                        });
                    }
                    "test/double_impl" => {
                        let impl_tx = impl_tx.clone();
                        let impl_release = impl_release.clone();
                        tokio::spawn(async move {
                            let _ = impl_tx.send(());
                            impl_release
                                .acquire()
                                .await
                                .expect("impl semaphore open")
                                .forget();
                            if payload.len() < 4 {
                                let _ = transport
                                    .complete_error(
                                        &rid,
                                        actr_protocol::ActrError::InvalidArgument(
                                            "double_impl payload too short".to_string(),
                                        ),
                                    )
                                    .await;
                            } else {
                                let x = i32::from_le_bytes([
                                    payload[0], payload[1], payload[2], payload[3],
                                ]);
                                let _ = transport
                                    .complete_response(
                                        &rid,
                                        Bytes::from((x * 2).to_le_bytes().to_vec()),
                                    )
                                    .await;
                            }
                        });
                    }
                    other => {
                        let _ = transport
                            .complete_error(
                                &rid,
                                actr_protocol::ActrError::UnknownRoute(other.to_string()),
                            )
                            .await;
                    }
                }
            }
        });
    }

    GateControls {
        gate_entered: gate_rx,
        gate_release,
        impl_entered: impl_rx,
        impl_release,
        calls,
    }
}

/// Block (bounded by a watchdog) until `n` guest tasks have signalled entry on
/// `rx`. Receiving from a channel is an event wait, not sleep-coordination.
pub async fn wait_entered(rx: &mut mpsc::UnboundedReceiver<()>, n: usize) {
    for i in 0..n {
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("watchdog: only {i}/{n} guest entries arrived"))
            .expect("entry channel open");
    }
}

/// Spawn a dispatch through any [`ConcurrentDispatch`] basis.
pub fn spawn_dispatch<D: ConcurrentDispatch + 'static>(
    dispatcher: &Arc<D>,
    route: &str,
    payload: Vec<u8>,
    caller_id: Option<ActrId>,
    bridge: &HostAbiFn,
) -> JoinHandle<actr_protocol::ActorResult<Bytes>> {
    let dispatcher = dispatcher.clone();
    let bridge = bridge.clone();
    let route = route.to_string();
    tokio::spawn(async move {
        dispatcher
            .dispatch(&route, payload, caller_id, &bridge)
            .await
    })
}

/// Await a spawned dispatch **under a watchdog**. Any orchestration accident — a
/// mis-ordered release, a lost permit, a scheduler stall — trips the timeout and
/// fails the test fast rather than hanging CI forever. The timeout is a generous
/// upper bound a correct run never approaches.
pub async fn await_dispatch(
    h: JoinHandle<actr_protocol::ActorResult<Bytes>>,
    label: &str,
) -> actr_protocol::ActorResult<Bytes> {
    tokio::time::timeout(Duration::from_secs(20), h)
        .await
        .unwrap_or_else(|_| {
            panic!("watchdog: dispatch `{label}` did not resolve within 20s (hang)")
        })
        .unwrap_or_else(|e| panic!("dispatch `{label}` task panicked or was cancelled: {e}"))
}

// ─────────────────────────────────────────────────────────────────────────────
// IO-load gate bridges (used by `benches/dispatch_throughput.rs`).
//
// The gate bridges above (`gate_bridge` / `spawn_native_gate`) park each guest
// `ctx.call_raw` on a *semaphore* the test releases by hand — event-driven
// coordination, no sleeps. The throughput benchmark needs the OPPOSITE: it must
// model an actual IO **workload** — a host import that takes a fixed wall-clock
// window `io_delay` to service (a network/disk round-trip stand-in). So these
// two IO-gate variants complete each `test/gate` crossing after `sleep(io_delay)`.
//
// IMPORTANT — the `sleep` here is the BENCHMARKED IO LOAD ITSELF, not test
// coordination. The project rule "never use `sleep` to coordinate/synchronise"
// forbids sleeping to *wait for another task to make progress*; here nothing is
// being waited on — the sleep IS the simulated IO service time under test. With
// `io_delay = 0` the gate returns immediately, isolating pure per-dispatch
// plumbing overhead (scheduler + runner + region + one guest→host round-trip).
//
// Serial base (K dispatches, one at a time)  ⇒ wall-clock ≈ K·io_delay.
// Interleaved base (budget C, distinct keys) ⇒ wall-clock ≈ ⌈K/C⌉·io_delay,
// because up to C IO windows overlap inside the ONE resident instance.

/// WASM IO-load gate: a `HostAbiFn` that services each `test/gate` crossing
/// after `io_delay` (the simulated IO service time — see the module note on why
/// this sleep is the load, not coordination), then resolves `Done`. Any other
/// route errors. Unlike [`gate_bridge`] it needs no [`GateControls`] — it
/// releases itself on the timer, so a benchmark can fire K dispatches and just
/// await them.
pub fn io_gate_bridge(io_delay: Duration) -> HostAbiFn {
    Arc::new(move |op| {
        Box::pin(async move {
            match op {
                HostOperation::CallRaw(req) if req.route_key == "test/gate" => {
                    // Simulated IO service window (the load under measurement).
                    tokio::time::sleep(io_delay).await;
                    HostOperationResult::Done
                }
                _ => HostOperationResult::Error(-1),
            }
        })
    })
}

/// Native IO-load gate: the mirror of [`spawn_native_io_gate`]'s WASM sibling.
/// Drains the shared [`crate::transport::HostTransport`] raw and answers each
/// `test/gate` request after `io_delay` on its own task, so up to C co-resident
/// guest calls can be in their IO window simultaneously. Returns the reader's
/// [`JoinHandle`] so a benchmark can drop/abort it at teardown.
pub fn spawn_native_io_gate(
    transport: Arc<crate::transport::HostTransport>,
    io_delay: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(env) = transport.recv_reliable_raw().await {
            let rid = env.request_id.clone();
            let route = env.route_key.clone();
            let transport = transport.clone();
            match route.as_str() {
                "test/gate" => {
                    tokio::spawn(async move {
                        // Simulated IO service window (the load under measurement).
                        tokio::time::sleep(io_delay).await;
                        let _ = transport.complete_response(&rid, Bytes::new()).await;
                    });
                }
                other => {
                    let _ = transport
                        .complete_error(
                            &rid,
                            actr_protocol::ActrError::UnknownRoute(other.to_string()),
                        )
                        .await;
                }
            }
        }
    })
}
