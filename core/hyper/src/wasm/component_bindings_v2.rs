//! Component Model bindings for the `actr:workload@0.2.0` async WIT world.
//!
//! This is the run_concurrent-ready sibling of
//! [`super::component_bindings`] (the 0.1.0, synchronous world). The 0.2.0
//! world declares every host import and workload export as a real WIT
//! `async func`, so wasmtime 46 emits a fundamentally different binding
//! shape here:
//!
//! - **Imports** become an *Accessor-based* host trait: the methods are
//!   `async` **associated functions** taking `&Accessor<HostState, Self>`
//!   rather than `&mut self`. Store access is synchronous-only, via
//!   `accessor.with(|a| ...)`, and its borrow cannot be held across an
//!   `.await`. This is what lets several invocations be in flight on one
//!   instance at once (their `&mut Store` borrows never overlap).
//! - **Exports** become `call_dispatch(&Accessor<..>, ...)` etc., driven
//!   from inside a `Store::run_concurrent(async |accessor| ...)` region.
//!
//! # No `async` flag
//!
//! Unlike the 0.1.0 bindings (`imports: { default: async | trappable }`),
//! the async shape here is driven by the WIT `async func` declarations, not
//! by a bindgen flag. wasmtime 46 detects the async world and generates the
//! Accessor trait automatically. We keep `imports: { default: trappable }`
//! so host implementations return `wasmtime::Result<Result<T, actr-error>>`
//! — the outer `Result` is trap-level, the inner is the WIT variant return
//! (the "double `?`" shape). This mirrors the proven Phase 0.75
//! `component-spike-runconcurrent` host.
//!
//! The 0.1.0 bindings in `component_bindings.rs` are untouched; the two
//! worlds coexist and the load path probes which one a component implements.

wasmtime::component::bindgen!({
    world: "actr-workload-guest-v2",
    // `wit-v2/actr-workload.wit` is a checked-in byte-identical copy of
    // `core/framework/wit-v2/actr-workload.wit`, kept in-crate so the
    // publish tarball is self-contained. Drift is guarded by CI (`cmp`).
    path: "wit-v2",
    imports: { default: trappable },
});
