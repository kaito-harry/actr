//! M6 §3 — dispatch throughput / per-dispatch overhead benchmark.
//!
//! A citable measurement of what the v2 same-instance **distinct-key
//! interleave** buys under an IO-bound load, relative to the serial baseline,
//! plus a per-dispatch net-overhead number the RFC (§4) references.
//!
//! ## What it measures
//!
//! One guest source (`tests/wasm_actor_fixture`) compiled two ways — a WASM V2
//! Component and a native `Linked` in-process guest — driven through the exact
//! production dispatch shapes (§ `test_support`):
//!
//! * **serial**      — keyless `RunnerMode::Serial`, NO scheduler (the strategy-A
//!   default-on keyless path). K dispatches run strictly one at a time.
//! * **interleaved** — a budgeted (`C`) conflict-key scheduler in front of the
//!   interleaved runner, distinct callers ⇒ distinct keys ⇒ eligible to overlap
//!   inside the ONE resident instance.
//! * **keyed-serial** (overhead probe) — the SAME scheduler+region as
//!   interleaved but a keyless spec, so every dispatch projects to the global
//!   `Serial` key and is forced serial. Its gap over plain `serial` isolates the
//!   scheduler + resident-region standing cost.
//!
//! ## Load model
//!
//! Each dispatch takes the `test/inflight-probe` route, whose only guest→host
//! crossing is one `ctx.call_raw("test/gate", …)`. The benchmark's IO-gate
//! (`concurrency::io_gate_bridge` / `spawn_native_io_gate`) services that
//! crossing after a fixed window `L` — modelling one unit of IO work per
//! dispatch. We sweep `L ∈ {0, 1ms, 10ms, 50ms}` to trace how the interleave
//! payoff grows with the IO fraction. At `L = 0` the gate returns immediately,
//! isolating pure per-dispatch plumbing overhead.
//!
//! NOTE on the sleep in the IO-gate: that `tokio::time::sleep(L)` is the
//! *benchmarked IO workload itself*, not test/coordination sleep — see the long
//! note in `tests/common/concurrency.rs`. Nothing waits on another task's
//! progress; the sleep IS the simulated IO service time under measurement.
//!
//! ## Expected shape
//!
//! * serial      wall-clock ≈ K·L
//! * interleaved wall-clock ≈ ⌈K/C⌉·L
//! * speedup (serial/interleaved) → min(C, K) as L grows (IO dominates overhead)
//!
//! ## Methodology
//!
//! Release build; a fixed-worker (`WORKER_THREADS`) tokio runtime the bench owns
//! (the load is multi-threaded async, so criterion's synchronous harness does
//! not apply — hence `harness = false` + this `fn main`). Per (basis, L) we
//! warm up, then take `N_SAMPLES` **variance-aware A/B** samples: within each
//! sample iteration all three modes run back-to-back so shared system drift
//! (frequency scaling, scheduler noise) hits every mode equally. We report the
//! **median** (robust to outliers) plus the sample **stddev** and min/max.

// The `#[path]` fixture modules stay at top level (relative to `benches/`), each
// gated on the same predicate as the real bench so a toolchain-less build (no
// wasm fixture) drops them cleanly and falls through to the stub `main`.
#[cfg(all(
    feature = "wasm-engine",
    feature = "test-utils",
    actr_wasm_fixture_available
))]
#[path = "../tests/wasm_actor_fixture.rs"]
mod wasm_actor_fixture;

// Native basis: the SAME guest source, compiled into this bench binary. Off
// wasm32 the `entry!` macro expands to nothing, leaving only the plain
// `DoubleActor` type.
#[cfg(all(
    feature = "wasm-engine",
    feature = "test-utils",
    actr_wasm_fixture_available
))]
#[allow(unexpected_cfgs)]
#[path = "../tests/wasm_actor_fixture/src/lib.rs"]
mod fixture_native;

#[cfg(all(
    feature = "wasm-engine",
    feature = "test-utils",
    actr_wasm_fixture_available
))]
mod bench_impl {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use actr_hyper::ConflictKeySpec;
    use actr_hyper::test_support::concurrency::{
        PROBE, caller, io_gate_bridge, probe_spec, spawn_native_io_gate,
    };
    use actr_hyper::test_support::{
        ConcurrentDispatch, TestNativeConcurrentDispatcher, TestNativeSerialDispatcher,
        instantiate_wasm_workload,
    };
    use actr_hyper::wasm::WasmHost;
    use actr_hyper::workload::{HostAbiFn, HostOperationResult};
    use tokio::task::JoinHandle;

    use super::{fixture_native, wasm_actor_fixture};

    fn fixture_bytes() -> &'static [u8] {
        wasm_actor_fixture::WASM_ACTOR_FIXTURE
    }

    // ── Tunables ──────────────────────────────────────────────────────────────

    /// Dispatches per batch (one measured unit of work).
    const K: usize = 48;
    /// Interleave budget C: max concurrent distinct-key dispatches in one instance.
    const C: usize = 8;
    /// Scheduler queue cap — generous so every submission is admitted, not shed.
    const QUEUE_CAP: usize = 4096;
    /// Discarded warm-up batches per (basis, L) before sampling.
    const N_WARMUP: usize = 3;
    /// A/B samples per (basis, L). Odd ⇒ an unambiguous median.
    const N_SAMPLES: usize = 11;
    /// Fixed tokio worker threads (documented, reproducible).
    const WORKER_THREADS: usize = 4;
    /// IO service windows swept to trace payoff-vs-IO-fraction (microseconds).
    const L_VALUES_US: &[u64] = &[0, 1_000, 10_000, 50_000];

    /// Keyless spec: no route declared ⇒ every dispatch projects to the global
    /// `Serial` key. Fronting the interleaved region with this forces it serial —
    /// the keyed-serial overhead probe.
    fn keyless_spec() -> ConflictKeySpec {
        ConflictKeySpec::builder()
            .build()
            .expect("build empty (keyless) conflict-key spec")
    }

    // ── One benchmark suite (three dispatchers sharing a basis + L) ─────────────

    struct Suite {
        serial: Arc<dyn ConcurrentDispatch>,
        interleaved: Arc<dyn ConcurrentDispatch>,
        keyed_serial: Arc<dyn ConcurrentDispatch>,
        /// Guest→host bridge passed per dispatch. On WASM this is the IO-gate; on
        /// native the runner ignores it (the shared-transport gate reader services
        /// the crossings instead) and it is a no-op stub for signature parity.
        bridge: HostAbiFn,
        /// Native gate reader tasks (one per dispatcher's shared transport); empty
        /// on WASM. Aborted at teardown.
        gates: Vec<JoinHandle<()>>,
    }

    impl Suite {
        async fn shutdown(self) {
            self.serial.shutdown().await;
            self.interleaved.shutdown().await;
            self.keyed_serial.shutdown().await;
            for g in self.gates {
                g.abort();
            }
        }
    }

    /// Build the three WASM V2 dispatchers for IO window `l`, all instantiated
    /// from one compiled host.
    async fn build_wasm(host: &WasmHost, l: Duration) -> Suite {
        let serial: Arc<dyn ConcurrentDispatch> = {
            let wl = instantiate_wasm_workload(host)
                .await
                .expect("instantiate wasm (serial)");
            Arc::new(wl.into_serial_dispatcher())
        };
        let interleaved: Arc<dyn ConcurrentDispatch> = {
            let wl = instantiate_wasm_workload(host)
                .await
                .expect("instantiate wasm (interleaved)");
            Arc::new(wl.into_concurrent_dispatcher(probe_spec(), C, QUEUE_CAP, None))
        };
        let keyed_serial: Arc<dyn ConcurrentDispatch> = {
            let wl = instantiate_wasm_workload(host)
                .await
                .expect("instantiate wasm (keyed-serial)");
            Arc::new(wl.into_concurrent_dispatcher(keyless_spec(), C, QUEUE_CAP, None))
        };
        Suite {
            serial,
            interleaved,
            keyed_serial,
            bridge: io_gate_bridge(l),
            gates: Vec::new(),
        }
    }

    /// Build the three native `Linked` dispatchers for IO window `l`, each with
    /// its own shared-transport IO-gate reader.
    fn build_native(l: Duration) -> Suite {
        let serial_d = Arc::new(TestNativeSerialDispatcher::spawn(
            fixture_native::DoubleActor::default(),
        ));
        let g_serial = spawn_native_io_gate(serial_d.host_transport(), l);
        let serial: Arc<dyn ConcurrentDispatch> = serial_d;

        let interleaved_d = Arc::new(TestNativeConcurrentDispatcher::spawn(
            fixture_native::DoubleActor::default(),
            probe_spec(),
            C,
            QUEUE_CAP,
            None,
        ));
        let g_inter = spawn_native_io_gate(interleaved_d.host_transport(), l);
        let interleaved: Arc<dyn ConcurrentDispatch> = interleaved_d;

        let keyed_d = Arc::new(TestNativeConcurrentDispatcher::spawn(
            fixture_native::DoubleActor::default(),
            keyless_spec(),
            C,
            QUEUE_CAP,
            None,
        ));
        let g_keyed = spawn_native_io_gate(keyed_d.host_transport(), l);
        let keyed_serial: Arc<dyn ConcurrentDispatch> = keyed_d;

        // Ignored by the native runner; supplied for `dispatch` signature parity.
        let bridge: HostAbiFn = Arc::new(|_| Box::pin(async { HostOperationResult::Done }));

        Suite {
            serial,
            interleaved,
            keyed_serial,
            bridge,
            gates: vec![g_serial, g_inter, g_keyed],
        }
    }

    /// Fire `K` dispatches at one dispatcher concurrently (distinct callers ⇒
    /// distinct conflict keys), await them all, and return the wall-clock elapsed.
    async fn run_batch(d: &Arc<dyn ConcurrentDispatch>, bridge: &HostAbiFn) -> Duration {
        let start = Instant::now();
        let mut handles = Vec::with_capacity(K);
        for i in 0..K {
            let d = d.clone();
            let bridge = bridge.clone();
            let cid = caller(i as u64);
            handles.push(tokio::spawn(async move {
                d.dispatch(PROBE, Vec::new(), cid, &bridge).await
            }));
        }
        for h in handles {
            h.await
                .expect("dispatch task joined")
                .expect("dispatch resolved ok");
        }
        start.elapsed()
    }

    // ── Sampling + statistics ───────────────────────────────────────────────────

    #[derive(Clone, Copy)]
    struct Stats {
        median_s: f64,
        mean_s: f64,
        stddev_s: f64,
        min_s: f64,
        max_s: f64,
    }

    fn summarize(mut xs: Vec<f64>) -> Stats {
        xs.sort_by(|a, b| a.partial_cmp(b).expect("no NaN samples"));
        let n = xs.len();
        let median_s = xs[n / 2];
        let mean_s = xs.iter().sum::<f64>() / n as f64;
        let var = xs.iter().map(|x| (x - mean_s).powi(2)).sum::<f64>() / n as f64;
        Stats {
            median_s,
            mean_s,
            stddev_s: var.sqrt(),
            min_s: xs[0],
            max_s: xs[n - 1],
        }
    }

    struct ModeSamples {
        serial: Vec<f64>,
        interleaved: Vec<f64>,
        keyed_serial: Vec<f64>,
    }

    /// Warm up, then take `N_SAMPLES` variance-aware A/B samples: all three modes
    /// run back-to-back inside each iteration so system drift hits them equally.
    async fn measure(suite: &Suite) -> ModeSamples {
        for _ in 0..N_WARMUP {
            let _ = run_batch(&suite.serial, &suite.bridge).await;
            let _ = run_batch(&suite.interleaved, &suite.bridge).await;
            let _ = run_batch(&suite.keyed_serial, &suite.bridge).await;
        }
        let mut out = ModeSamples {
            serial: Vec::with_capacity(N_SAMPLES),
            interleaved: Vec::with_capacity(N_SAMPLES),
            keyed_serial: Vec::with_capacity(N_SAMPLES),
        };
        for _ in 0..N_SAMPLES {
            out.serial
                .push(run_batch(&suite.serial, &suite.bridge).await.as_secs_f64());
            out.interleaved.push(
                run_batch(&suite.interleaved, &suite.bridge)
                    .await
                    .as_secs_f64(),
            );
            out.keyed_serial.push(
                run_batch(&suite.keyed_serial, &suite.bridge)
                    .await
                    .as_secs_f64(),
            );
        }
        out
    }

    fn format_l(l_us: u64) -> String {
        if l_us == 0 {
            "0".to_string()
        } else if l_us.is_multiple_of(1000) {
            format!("{}ms", l_us / 1000)
        } else {
            format!("{l_us}us")
        }
    }

    fn print_row(basis: &str, l_us: u64, mode: &str, s: &Stats) {
        let per_dispatch_us = s.median_s / K as f64 * 1e6;
        let throughput = K as f64 / s.median_s;
        println!(
            "| {basis:<6} | {:>6} | {mode:<12} | {:>10.3} | {:>9.3} | {:>9.3} | {:>10.1} | {:>12.2} |",
            format_l(l_us),
            s.median_s * 1e3,
            s.stddev_s * 1e3,
            (s.max_s - s.min_s) * 1e3,
            per_dispatch_us,
            throughput,
        );
    }

    async fn run_basis(name: &str, host: Option<&WasmHost>) {
        for &l_us in L_VALUES_US {
            let l = Duration::from_micros(l_us);
            let suite = match host {
                Some(h) => build_wasm(h, l).await,
                None => build_native(l),
            };
            let samples = measure(&suite).await;

            let serial = summarize(samples.serial);
            let interleaved = summarize(samples.interleaved);
            let keyed_serial = summarize(samples.keyed_serial);

            print_row(name, l_us, "serial", &serial);
            print_row(name, l_us, "interleaved", &interleaved);
            print_row(name, l_us, "keyed-serial", &keyed_serial);

            let speedup = serial.median_s / interleaved.median_s;
            let region_overhead_us = (keyed_serial.median_s - serial.median_s) / K as f64 * 1e6;
            println!(
                "|        |        | -> {name} L={}: speedup={speedup:.2}x (ideal min(C,K)={}), \
                 keyed-serial per-dispatch region cost={region_overhead_us:+.2}us \
                 (serial mean={:.3}ms std={:.3}ms, interleaved mean={:.3}ms std={:.3}ms)",
                format_l(l_us),
                C.min(K),
                serial.mean_s * 1e3,
                serial.stddev_s * 1e3,
                interleaved.mean_s * 1e3,
                interleaved.stddev_s * 1e3,
            );

            suite.shutdown().await;
        }
    }

    pub fn run() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKER_THREADS)
            .enable_all()
            .build()
            .expect("build tokio runtime");

        rt.block_on(async {
            println!("# dispatch_throughput");
            println!(
                "config: K={K} dispatches/batch, C={C} budget, queue_cap={QUEUE_CAP}, \
                 worker_threads={WORKER_THREADS}, warmup={N_WARMUP}, \
                 samples={N_SAMPLES} (A/B interleaved)"
            );
            println!(
                "load model: 1 host round-trip/dispatch, IO-gate services it after L (simulated IO)"
            );
            println!();
            println!(
                "| basis  |      L | mode         | median(ms) | std(ms)   | range(ms) | per-disp(us) | throughput/s |"
            );
            println!(
                "|--------|--------|--------------|------------|-----------|-----------|--------------|--------------|"
            );

            let host = WasmHost::compile(fixture_bytes()).expect("compile wasm fixture");
            run_basis("wasm", Some(&host)).await;
            run_basis("native", None).await;
        });
    }
}

#[cfg(all(
    feature = "wasm-engine",
    feature = "test-utils",
    actr_wasm_fixture_available
))]
fn main() {
    bench_impl::run();
}

#[cfg(not(all(
    feature = "wasm-engine",
    feature = "test-utils",
    actr_wasm_fixture_available
)))]
fn main() {
    eprintln!(
        "dispatch_throughput skipped: requires `--features wasm-engine,test-utils` and the \
         wasm32-wasip2 fixture toolchain (build.rs emits `actr_wasm_fixture_available`)."
    );
}
