//! WASM workload execution engine (feature = "wasm-engine").
//!
//! Backed by the Component Model (wasmtime 43 + wit-bindgen) as of
//! Phase 1 Commit 2; see `core/framework/wit/actr-workload.wit` for the
//! contract.

pub(crate) mod component_bindings;
pub(crate) mod component_bindings_v2;
mod error;
mod host;
mod host_v2;
mod runtime_limits;

pub use error::WasmError;
pub use host::WasmHost;
pub(crate) use host::WasmKernel;
pub use runtime_limits::{WasmRuntimeStats, wasm_runtime_stats};
