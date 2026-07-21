//! Host callback function table for dynclib guests.
//!
//! Dynclib guests use a single business entry point plus a buffer free callback.

/// Host callback function pointer table.
///
/// # Safety
///
/// - All function pointers remain valid from `actr_init` until process exit.
/// - The host guarantees serialized entry per guest instance.
/// - All buffers returned by `invoke` must be released with `free_host_buf`.
/// - Context tokens remain valid while retained by the guest.
#[repr(C)]
pub struct HostVTable {
    /// Retain a host bridge token for a cloned or background context.
    pub retain_context: unsafe extern "C" fn(u64) -> i32,

    /// Release a previously retained host bridge token.
    pub release_context: unsafe extern "C" fn(u64),

    /// Invoke a host operation encoded as `AbiFrame` through a retained token.
    pub invoke: unsafe extern "C" fn(u64, *const u8, usize, *mut *mut u8, *mut usize) -> i32,

    /// Free a host-allocated buffer returned by `invoke`.
    pub free_host_buf: unsafe extern "C" fn(*mut u8, usize),
}
