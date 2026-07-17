//! Dynclib actor execution engine
//!
//! Loads native shared libraries (.so/.dylib/.dll) compiled as cdylib actors.
//! Provides [`DynclibHost`] for library loading; per-instance workloads stay
//! internal to Hyper.

mod error;
mod host;

pub use error::DynclibError;
pub(crate) use host::DynClibWorkload;
pub use host::DynclibHost;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use host::active_bridge_count;
