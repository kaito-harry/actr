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
//! # Synchronous lift (no `async` flag)
//!
//! The WIT world (`actr:workload@0.1.0`) declares every function as a plain
//! (synchronous) `func`. Earlier revisions passed `async: true` here, which
//! made wit-bindgen emit async-ABI (async-lift) custom sections. wasmtime
//! 46's embedded wasmparser now enforces `check_asyncness`: the `async`
//! canonical option is only valid on WIT `async func` types, so async-lift
//! components built from a sync world are rejected at `Component::from_binary`
//! ("the `async` canonical option requires an async function type"). There is
//! no Config/feature escape hatch.
//!
//! So the flag is removed and the exports are lifted synchronously. Host
//! imports appear as ordinary (non-`async`) functions at the Rust surface;
//! the guest's `async fn` workload hooks are driven to completion by
//! `adapter::complete_sync`. Host-side asynchrony (fiber suspension while a
//! host import is in flight) is unchanged — it is provided by wasmtime's
//! `call_async` fibers on the host, not by the guest ABI.
//!
//! # `generate_all`
//!
//! The WIT world imports `host` and exports `workload`; `generate_all`
//! tells wit-bindgen to emit bindings for both sides. Without it only the
//! exports surface is generated.

wit_bindgen::generate!({
    world: "actr-workload-guest",
    path: "wit",
    generate_all,
    // The `entry!` macro in `guest/mod.rs` expands inside the user's crate
    // and needs to call `export!` from user-crate scope. Default `export!`
    // is crate-private; `pub_export_macro: true` makes it `pub` so it can
    // be invoked via `::actr_framework::guest::wasm::generated::export!(...)`.
    pub_export_macro: true,
});
