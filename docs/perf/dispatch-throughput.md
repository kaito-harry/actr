# Dispatch throughput — v2 same-instance interleave vs serial (M6 §3)

A citable benchmark of what the v2 **same-instance distinct-key interleave**
buys under an IO-bound load, relative to the serial baseline, plus a
per-dispatch net-overhead number. Referenced by the M6 RFC (§4).

- Benchmark: `core/hyper/benches/dispatch_throughput.rs` (`harness = false`).
- Shared IO-gate helpers: `core/hyper/tests/common/concurrency.rs`
  (`io_gate_bridge` / `spawn_native_io_gate`).
- Run: `CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS="-Awarnings" ACTR_REQUIRE_WASM_FIXTURE=1 \`
  `cargo +1.95.0 bench -p actr-hyper --features wasm-engine,test-utils --bench dispatch_throughput`

## Method

- **Bases** — one guest source (`tests/wasm_actor_fixture`) compiled two ways:
  a WASM V2 Component (resident `run_concurrent` region) and a native `Linked`
  in-process guest. Both driven through the exact production dispatch shapes in
  `test_support`.
- **Modes** —
  - `serial`: keyless `RunnerMode::Serial`, no scheduler (the strategy-A
    default-on keyless path). K dispatches run strictly one at a time.
  - `interleaved`: budgeted (`C`) conflict-key scheduler → interleaved runner;
    distinct callers ⇒ distinct keys ⇒ eligible to overlap in the ONE instance.
  - `keyed-serial` (overhead probe): the SAME scheduler+region as `interleaved`
    but a keyless spec, so every dispatch projects to the global `Serial` key and
    is forced serial. Its gap over `serial` isolates the scheduler + resident
    region standing cost.
- **Load model** — each dispatch takes `test/inflight-probe`, whose only
  guest→host crossing is one `ctx.call_raw("test/gate", …)`. The IO-gate services
  that crossing after a fixed window `L` (one unit of simulated IO work per
  dispatch). `L ∈ {0, 1ms, 10ms, 50ms}` traces payoff-vs-IO-fraction; `L = 0`
  isolates pure per-dispatch plumbing overhead. The `sleep(L)` inside the IO-gate
  is the *benchmarked IO workload itself*, not coordination sleep.
- **Statistics** — release (`bench`) profile; a bench-owned tokio runtime with
  `worker_threads = 4`; `K = 48` dispatches/batch; `C = 8` budget;
  `queue_cap = 4096`; 3 warm-up batches then 11 **variance-aware A/B** samples per
  (basis, L) — all three modes run back-to-back inside each sample iteration so
  shared system drift hits them equally. Reported: median (robust), sample
  stddev, min/max range.
- **Environment** — Linux 6.8, rustc 1.95.0, wasm-component-ld 0.5.22. Numbers
  below are one representative run (2026-07-09); the shape (not the absolute µs)
  is what the RFC cites.

## Expected vs observed shape

- `serial` wall-clock ≈ K·L; `interleaved` ≈ ⌈K/C⌉·L (= 6·L for K=48, C=8).
- speedup (serial/interleaved) → min(C, K) = 8 as L grows (IO dominates overhead).

Both hold: at L=50ms, serial = 2455ms ≈ 48·50ms and interleaved = 309ms ≈ 6·50ms.

## Results

Median wall-clock (ms) for a K=48 batch, per-dispatch (median/K, µs), throughput
(K/median, dispatch·s⁻¹). std/range in ms.

| basis  |    L | mode         | median(ms) | std(ms) | range(ms) | per-disp(µs) | throughput/s |
|--------|------|--------------|-----------:|--------:|----------:|-------------:|-------------:|
| wasm   |    0 | serial       |     53.247 |   1.225 |     4.502 |       1109.3 |       901.46 |
| wasm   |    0 | interleaved  |      8.792 |   0.154 |     0.516 |        183.2 |      5459.69 |
| wasm   |    0 | keyed-serial |     55.484 |   0.624 |     2.132 |       1155.9 |       865.11 |
| wasm   |  1ms | serial       |    102.428 |   0.563 |     2.550 |       2133.9 |       468.62 |
| wasm   |  1ms | interleaved  |     14.928 |   0.359 |     1.373 |        311.0 |      3215.50 |
| wasm   |  1ms | keyed-serial |    103.123 |   0.860 |     2.594 |       2148.4 |       465.47 |
| wasm   | 10ms | serial       |    533.353 |   1.364 |     4.940 |      11111.5 |        90.00 |
| wasm   | 10ms | interleaved  |     68.917 |   0.109 |     0.431 |       1435.8 |       696.49 |
| wasm   | 10ms | keyed-serial |    535.658 |   0.647 |     2.095 |      11159.5 |        89.61 |
| wasm   | 50ms | serial       |   2454.674 |   1.844 |     6.264 |      51139.0 |        19.55 |
| wasm   | 50ms | interleaved  |    309.211 |   0.498 |     1.821 |       6441.9 |       155.23 |
| wasm   | 50ms | keyed-serial |   2456.400 |   1.644 |     4.839 |      51175.0 |        19.54 |
| native |    0 | serial       |     52.977 |   1.002 |     2.246 |       1103.7 |       906.06 |
| native |    0 | interleaved  |      7.969 |   0.302 |     1.267 |        166.0 |      6023.43 |
| native |    0 | keyed-serial |     53.546 |   0.798 |     2.142 |       1115.5 |       896.42 |
| native |  1ms | serial       |    101.075 |   0.642 |     1.984 |       2105.7 |       474.90 |
| native |  1ms | interleaved  |     13.913 |   0.317 |     1.134 |        289.9 |      3449.98 |
| native |  1ms | keyed-serial |    100.913 |   0.543 |     1.584 |       2102.3 |       475.66 |
| native | 10ms | serial       |    532.773 |   0.834 |     2.193 |      11099.4 |        90.09 |
| native | 10ms | interleaved  |     68.066 |   0.148 |     0.559 |       1418.0 |       705.19 |
| native | 10ms | keyed-serial |    533.573 |   0.551 |     2.094 |      11116.1 |        89.96 |
| native | 50ms | serial       |   2453.795 |   1.614 |     5.474 |      51120.7 |        19.56 |
| native | 50ms | interleaved  |    308.485 |   0.364 |     1.089 |       6426.8 |       155.60 |
| native | 50ms | keyed-serial |   2454.821 |   1.616 |     6.311 |      51142.1 |        19.55 |

### Speedup (interleaved vs serial), by IO window

| L    | wasm speedup | native speedup | ideal min(C,K) |
|------|-------------:|---------------:|---------------:|
| 0    |        6.06× |          6.65× |              8 |
| 1ms  |        6.86× |          7.26× |              8 |
| 10ms |        7.74× |          7.83× |              8 |
| 50ms |        7.94× |          7.95× |              8 |

The interleave payoff rises monotonically toward the ideal `min(C,K)=8` as the IO
fraction grows — because at higher L the fixed per-dispatch overhead is amortized
away and the ⌈K/C⌉·L wall-clock floor dominates. At L=0 the payoff is bounded
below 8 purely by that overhead, not by any concurrency defect.

## Headline numbers (for RFC citation)

- **Interleave speedup → ~8× (= budget C)** under IO-bound load: at L=50ms,
  **7.94× (WASM)** / **7.95× (native)**; throughput 19.5 → 155 dispatch/s.
- **Per-dispatch net overhead ≈ 1.1ms** (L=0, serial, one full guest→host→guest
  round-trip through the runner): **1109µs (WASM)** vs **1104µs (native)** —
  within ~0.5% of each other. Corroborated by the L=1ms serial cost of ~2.1ms/
  dispatch (≈ 1.1ms overhead + 1ms IO).
- **Dual-basis isomorphism holds on performance too:** WASM and native track
  within a few percent across every mode and every L — the resident WASM
  `run_concurrent` region is not a throughput tax over the native `&self`
  `FuturesUnordered`.
- **Scheduler + resident-region standing cost is cheap:** keyed-serial (gate on,
  forced serial) over plain serial adds only ~**+10…+48µs per dispatch**
  (WASM +14…+48µs; native −3…+21µs, i.e. within noise). Turning the gate on buys
  concurrency without a meaningful idle-serial penalty.
- **Low variance / clean signal:** sample stddev stays < 2ms even at the 2.4s
  serial-L=50ms scale (< 0.1% relative), so the A/B medians are trustworthy.

## Reproducing

The bench is gated on `wasm-engine` + `test-utils` + the wasm fixture toolchain
(`build.rs` emits `actr_wasm_fixture_available`). Without them the bench `main`
is a no-op stub. Always export `CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS="-Awarnings"`
so the workspace's ambient wasm rustflags (e.g. `-fuse-ld=mold`) do not leak into
`wasm-component-ld` and break the fixture link.
