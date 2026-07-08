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
//! fn` via `exports: { default: async }`. This is a *host-side* choice: it
//! lets the host await real I/O (a downstream RPC) inside an import while a
//! wasmtime fiber suspends the guest, keeping actr's single-threaded-actor
//! invariant while still driving I/O through tokio.
//!
//! This is orthogonal to how the guest is *lifted*. Since M3 the guest is
//! lifted synchronously (wit-bindgen without `async: true`), because
//! wasmtime 46 rejects the async canonical option on the plain-`func` WIT
//! world. Guest-side asynchrony is emulated on the guest by driving each
//! `async fn` hook to completion; host-side asynchrony (this bindgen shape)
//! is unchanged.
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
