//! Component Model bindings for the `actr:workload@0.1.0` WIT contract.
//!
//! Phase 1, Commit 2: these bindings now back the single path the wasm
//! workload runtime takes — the legacy handwritten ptr/len ABI
//! (core/hyper/src/wasm/abi.rs, core/framework/src/guest/dynclib_abi.rs,
//! entry!-generated `actr_init` / `actr_handle` / `actr_alloc` / `actr_free`)
//! is replaced by the Component Model canonical ABI.
//!
//! # Async shape
//!
//! Host-side imports are generated as `async fn` via
//! `imports: { default: async | trappable }` and guest exports as `async
//! fn` via `exports: { default: async }`. The underlying WIT is plain
//! sync `func` — this is the Phase 0.5-validated combination that keeps
//! actr's single-threaded-actor invariant while still driving real I/O
//! through tokio.
//!
//! # Why `async | trappable`
//!
//! `async` makes every import an `async fn`; `trappable` lets host
//! implementations return a `Result<T, wasmtime::Error>` so that
//! host-side failures (e.g. a downstream RPC timeout) cleanly surface as
//! `Result` rather than forcing a `Trap`. The generated return shape is
//! `wasmtime::Result<Result<T, actr-error>>`: the outer `Result` signals
//! trap-level failure, the inner `Result` is the WIT variant return.
//!
//! # Why no `with` map
//!
//! Generated Rust types mirroring the WIT records live in this module's
//! `actr::workload::types` namespace. The host translates at the
//! boundary (inside the `Host` impl) rather than remapping to
//! actr_protocol / actr_framework types via `with: { ... }`: boundary
//! translation keeps the generated bindings self-contained and makes the
//! mapping rules reviewable in `host.rs`.

wasmtime::component::bindgen!({
    world: "actr-workload-guest",
    // `wit/actr-workload.wit` is a checked-in copy of
    // `core/framework/wit/actr-workload.wit`. The copy lives inside this
    // crate (not `../framework/wit`) so it ships in the publish tarball and
    // `cargo package --all-features` can build without the sibling framework
    // crate. Drift is guarded by CI (`cmp` against the framework original).
    path: "wit",
    imports: { default: async | trappable },
    exports: { default: async },
});
