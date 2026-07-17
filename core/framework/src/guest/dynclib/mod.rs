//! Dynclib (cdylib) guest-side runtime module
//!
//! Runs in native shared libraries (.so/.dylib/.dll). Provides `DynclibContext`
//! (Context impl) that communicates with the host via `HostVTable` function pointers.
//!
//! Handler futures run on a guest-owned Tokio runtime. `tokio::spawn` therefore
//! works while a handler is active. Prefer [`spawn`] for long-lived tasks so the
//! runtime can abort and join them before the shared library is unloaded.

pub mod context;
mod runtime;

pub use context::DynclibContext;
pub use runtime::spawn;

#[doc(hidden)]
pub use runtime::{block_on, initialize, shutdown};
