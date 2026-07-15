# component-spike-async (Phase 0.5)

Standalone validation of the **async** end-to-end path for WASM Component Model
+ WIT + wit-bindgen + wasmtime, building on Phase 0 (`experiments/component-spike/`).

Phase 0 proved the synchronous path. This spike answers: **can wasmtime's
`Component::instantiate_async` + bindgen async export/import path replace
actr's handwritten cooperative-suspend (asyncify) machinery in
`core/hyper/src/wasm/host.rs`?**

Each sub-crate declares its own `[workspace]` so the repo-root `Cargo.lock` is
untouched.

## Layout

```
experiments/component-spike-async/
  wit/actr-spike-async.wit   WIT with design-note header on async semantics
  guest/                     Rust workload built to a Component (wasm32-wasip2)
                             with bindgen `async: true`
  host/                      Rust host using wasmtime::component::bindgen
                             with `imports: { default: async | trappable }`
                             and `exports: { default: async }`
  build.sh                   one-shot: build guest, print Component WIT,
                             build + run host (8 tests)
  REPORT.md                  findings + Phase 1 planning notes
```

## Prerequisites

- Rust 1.95+
- `rustup target add wasm32-wasip2`
- `cargo install wasm-tools --version 1.253.0` (for `wasm-tools validate/strip`)
- **`cargo install wasm-component-ld --version 0.5.26`** — 0.5.22 is the first
  release that parses the async component custom sections wit-bindgen 0.57
  emits. `build.sh` points `RUSTFLAGS=-Clinker=...` at the newer binary.

## Run

```
./build.sh
```

## Tests

The host runs 8 tests; see `REPORT.md` for results and interpretation.

1. basic async dispatch round-trip (guest awaits a host import)
2. concurrent dispatches on DIFFERENT instances (expect ~50ms, not 100ms)
3. concurrent dispatches on the SAME instance — type-system forces serial
4. host thread free during guest await (tokio ticker test)
5. guest-side async ergonomics
6. throughput: 100 sequential dispatches + overhead per call
7. error variant propagation across async boundary
8. guest panic AFTER a suspension point -> Trap

## See also

- `REPORT.md` — full findings, versions, WIT/bindgen syntax gotchas, and
  recommendations for Phase 1.
- `experiments/component-spike/REPORT.md` — Phase 0 baseline.
- `core/hyper/src/wasm/host.rs` — the handwritten cooperative-suspend driver
  this spike aims to replace.
