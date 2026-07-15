//! Component Model bindings generated from `core/framework/wit/actr-workload.wit`.
//!
//! Re-runs `wit_bindgen::generate!` once per compiled guest; the emitted
//! types live under `exports::actr::workload::workload` (the guest-provided
//! `Guest` trait) and `actr::workload::{host, types}` (host imports consumed
//! by the guest).
//!
//! Only compiled for `wasm32-wasip2` — the surrounding `guest::wasm` module
//! is `#[cfg(target_arch = "wasm32")]`-gated, so hosts never see this code
//! or the underlying `wit-bindgen` crate.
//!
//! # Async world (`actr:workload@0.2.0`)
//!
//! The WIT world (`actr:workload@0.2.0`) declares every host import and
//! workload export as a real `async func`. wit-bindgen 0.58 auto-generates
//! async Rust bindings from that — the `Guest` trait methods and the host
//! imports are all `async fn`, with NO `async: true` flag (the async-ness
//! comes from the WIT itself). This is the guest half of the run_concurrent
//! substrate: the host drives these async exports through
//! `Store::run_concurrent`, and the guest just `.await`s its host imports.
//!
//! This replaces the M3 synchronous lift. Per-call identity is no longer
//! read back through `get-self-id` / `get-caller-id` / `get-request-id`
//! imports (removed in 0.2.0); it arrives as an explicit `invocation-ctx`
//! parameter on every guest export. The SDK carries its token into every
//! host import so the host can key the calling invocation even under
//! concurrent in-flight calls.
//!
//! # `generate_all`
//!
//! The WIT world imports `host` and exports `workload`; `generate_all`
//! tells wit-bindgen to emit bindings for both sides. Without it only the
//! exports surface is generated.

wit_bindgen::generate!({
    world: "actr-workload-guest-v2",
    path: "wit-v2",
    generate_all,
    // The `entry!` macro in `guest/mod.rs` expands inside the user's crate
    // and needs to call `export!` from user-crate scope. Default `export!`
    // is crate-private; `pub_export_macro: true` makes it `pub` so it can
    // be invoked via `::actr_framework::guest::wasm::generated::export!(...)`.
    pub_export_macro: true,
});
