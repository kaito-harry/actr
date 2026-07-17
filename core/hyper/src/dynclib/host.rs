//! DynclibHost / DynClibWorkload — native shared-library actor execution engine
//!
//! Loads a cdylib SO/dylib/DLL and resolves the standard ABI symbols:
//!
//! - `actr_init(vtable, init_ptr, init_len) -> i32`
//! - `actr_handle(req_ptr, req_len, resp_out, resp_len_out) -> i32`
//! - `actr_free_response(ptr, len)`
//! - `actr_shutdown() -> i32`
//!
//! The guest library calls back into the host through a `HostVTable` passed at
//! init time. VTable trampolines bridge the synchronous C ABI with the async
//! Rust host ABI bridge through retained per-invocation tokens.
//!
//! Each loaded shared-library image currently supports exactly one logical actor
//! instance. If the host wants to run two actors from the same dynclib package,
//! it must load two independent library images and keep dispatch serialized per
//! workload.
//!
//! TODO: Decide whether Dynclib should eventually support a "one host loads once,
//! many workloads instantiate independently" model like WASM. That requires an
//! explicit instance design at the ABI/runtime boundary instead of relying on
//! module-global guest state.

use std::collections::HashMap;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use actr_framework::guest::dynclib_abi::{self as guest_abi, AbiReply, InitPayloadV1};
use libloading::Library;

use actr_framework::guest::vtable::HostVTable;
use actr_protocol::{ActrId, DataChunk};

use crate::workload::{
    HostAbiFn, HostOperation, HostOperationResult, InvocationContext, PackageHookEvent,
    encode_guest_data_chunk_request, encode_guest_handle_request, encode_guest_hook_request,
    encode_guest_lifecycle_request,
};

use super::error::{DynclibError, DynclibResult};

// ─────────────────────────────────────────────────────────────────────────────
// C ABI function signatures expected from the guest SO
// ─────────────────────────────────────────────────────────────────────────────

/// `actr_init(vtable: *const HostVTable, init_ptr: *const u8, init_len: usize) -> i32`
type InitFn = unsafe extern "C" fn(
    vtable: *const HostVTable,
    init_payload: *const u8,
    init_len: usize,
) -> i32;

/// `actr_handle(req_ptr: *const u8, req_len: usize, resp_out: *mut *mut u8, resp_len_out: *mut usize) -> i32`
type HandleFn = unsafe extern "C" fn(
    req: *const u8,
    req_len: usize,
    resp_out: *mut *mut u8,
    resp_len_out: *mut usize,
) -> i32;

/// `actr_free_response(ptr: *mut u8, len: usize)`
type FreeResponseFn = unsafe extern "C" fn(ptr: *mut u8, len: usize);

/// `actr_shutdown() -> i32`
type ShutdownFn = unsafe extern "C" fn() -> i32;

// ─────────────────────────────────────────────────────────────────────────────
// Retained host bridge registry
// ─────────────────────────────────────────────────────────────────────────────

struct BridgeEntry {
    executor: HostAbiFn,
    runtime: tokio::runtime::Handle,
    retain_count: usize,
}

fn bridge_registry() -> &'static Mutex<HashMap<u64, BridgeEntry>> {
    static REGISTRY: OnceLock<Mutex<HashMap<u64, BridgeEntry>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_bridge(executor: &HostAbiFn, runtime: tokio::runtime::Handle) -> u64 {
    static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);
    let token = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
    bridge_registry()
        .lock()
        .expect("dynclib bridge registry poisoned")
        .insert(
            token,
            BridgeEntry {
                executor: executor.clone(),
                runtime,
                retain_count: 1,
            },
        );
    token
}

fn retain_bridge(token: u64) -> bool {
    let Ok(mut registry) = bridge_registry().lock() else {
        return false;
    };
    let Some(entry) = registry.get_mut(&token) else {
        return false;
    };
    entry.retain_count += 1;
    true
}

fn release_bridge(token: u64) {
    let Ok(mut registry) = bridge_registry().lock() else {
        return;
    };
    let should_remove = match registry.get_mut(&token) {
        Some(entry) if entry.retain_count > 1 => {
            entry.retain_count -= 1;
            false
        }
        Some(_) => true,
        None => false,
    };
    if should_remove {
        registry.remove(&token);
    }
}

struct BridgeRegistration {
    token: u64,
}

impl BridgeRegistration {
    fn new(executor: &HostAbiFn) -> Self {
        Self {
            token: register_bridge(executor, tokio::runtime::Handle::current()),
        }
    }
}

impl Drop for BridgeRegistration {
    fn drop(&mut self) {
        release_bridge(self.token);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// VTable trampoline implementations
// ─────────────────────────────────────────────────────────────────────────────

/// Allocate a buffer and copy `data` into it, writing the pointer and length
/// into the caller-provided out parameters.
///
/// # Safety
/// `out_ptr` and `out_len` must be valid, aligned, non-null pointers.
unsafe fn host_alloc_and_write(data: &[u8], out_ptr: *mut *mut u8, out_len: *mut usize) {
    let len = data.len();
    let buf = if len > 0 {
        let layout = std::alloc::Layout::from_size_align(len, 1).expect("invalid layout");
        // Safety: layout has non-zero size (len > 0).
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        // Safety: ptr is valid for `len` bytes; data.len() == len.
        unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, len) };
        ptr
    } else {
        ptr::null_mut()
    };
    // Safety: caller guarantees out_ptr/out_len are valid.
    unsafe {
        *out_ptr = buf;
        *out_len = len;
    }
}

/// Execute a host operation through a retained bridge token.
fn trampoline_execute(token: u64, pending: HostOperation) -> HostOperationResult {
    let bridge = bridge_registry().lock().ok().and_then(|registry| {
        registry
            .get(&token)
            .map(|entry| (entry.executor.clone(), entry.runtime.clone()))
    });
    let Some((executor, runtime)) = bridge else {
        tracing::error!(token, "dynclib trampoline: bridge token not found");
        return HostOperationResult::Error(guest_abi::code::GENERIC_ERROR);
    };

    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    runtime.spawn(async move {
        let result = executor(pending).await;
        let _ = result_tx.send(result);
    });
    result_rx.recv().unwrap_or_else(|_| {
        tracing::error!(token, "dynclib trampoline: host executor stopped");
        HostOperationResult::Error(guest_abi::code::GENERIC_ERROR)
    })
}

/// Read bytes from raw pointer + length, returning an empty Vec on null/zero.
///
/// # Safety
/// If `ptr` is non-null, `ptr` must be valid for reads of `len` bytes.
unsafe fn read_raw_bytes(ptr: *const u8, len: usize) -> Vec<u8> {
    if ptr.is_null() || len == 0 {
        return Vec::new();
    }
    // Safety: caller guarantees ptr is valid for `len` bytes.
    unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
}

use crate::workload::decode_host_operation;

unsafe extern "C" fn vtable_retain_context(token: u64) -> i32 {
    if retain_bridge(token) {
        guest_abi::code::SUCCESS
    } else {
        guest_abi::code::GENERIC_ERROR
    }
}

unsafe extern "C" fn vtable_release_context(token: u64) {
    release_bridge(token);
}

unsafe extern "C" fn vtable_invoke(
    token: u64,
    frame_ptr: *const u8,
    frame_len: usize,
    resp_ptr_out: *mut *mut u8,
    resp_len_out: *mut usize,
) -> i32 {
    if resp_ptr_out.is_null() || resp_len_out.is_null() {
        return guest_abi::code::PROTOCOL_ERROR;
    }

    let frame_bytes = unsafe { read_raw_bytes(frame_ptr, frame_len) };
    let frame = match guest_abi::decode_message::<guest_abi::AbiFrame>(&frame_bytes) {
        Ok(frame) => frame,
        Err(code) => return code,
    };

    let pending = match decode_host_operation(frame) {
        Ok(pending) => pending,
        Err(code) => return code,
    };

    let reply = match trampoline_execute(token, pending) {
        HostOperationResult::Bytes(bytes) => AbiReply {
            abi_version: guest_abi::version::V1,
            status: guest_abi::code::SUCCESS,
            payload: bytes,
        },
        HostOperationResult::Done => AbiReply {
            abi_version: guest_abi::version::V1,
            status: guest_abi::code::SUCCESS,
            payload: Vec::new(),
        },
        HostOperationResult::Error(code) => AbiReply {
            abi_version: guest_abi::version::V1,
            status: code,
            payload: Vec::new(),
        },
    };

    let reply_bytes = match guest_abi::encode_message(&reply) {
        Ok(reply_bytes) => reply_bytes,
        Err(code) => return code,
    };

    unsafe { host_alloc_and_write(&reply_bytes, resp_ptr_out, resp_len_out) };
    guest_abi::code::SUCCESS
}

// ── VTable::free_host_buf ───────────────────────────────────────────────────

unsafe extern "C" fn vtable_free_host_buf(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    let layout = std::alloc::Layout::from_size_align(len, 1).expect("invalid layout in free");
    // Safety: the buffer was allocated by `host_alloc_and_write` using
    // `std::alloc::alloc` with Layout::from_size_align(len, 1). The guest
    // must not use the pointer after calling this function.
    unsafe { std::alloc::dealloc(ptr, layout) };
}

/// Static VTable instance with all trampolines wired up.
static HOST_VTABLE: HostVTable = HostVTable {
    retain_context: vtable_retain_context,
    release_context: vtable_release_context,
    invoke: vtable_invoke,
    free_host_buf: vtable_free_host_buf,
};

// ─────────────────────────────────────────────────────────────────────────────
// DynclibHost
// ─────────────────────────────────────────────────────────────────────────────

/// Native shared-library host engine.
///
/// Loads and holds a single `.so` / `.dylib` / `.dll` image. Resolves ABI
/// symbols once at load time.
///
/// Under the current guest ABI, a loaded dynclib image supports only one
/// successful `actr_init` because guest state is module-global and no instance
/// handle is exposed back to the host. To create multiple independent
/// `DynClibWorkload`s today, Hyper must load multiple library images.
///
/// TODO: Revisit this contract if Dynclib gains a real per-instance ABI.
pub struct DynclibHost {
    /// Loaded shared library handle. Must outlive all resolved function pointers.
    _library: Library,
    init_fn: InitFn,
    handle_fn: HandleFn,
    free_response_fn: FreeResponseFn,
    shutdown_fn: ShutdownFn,
}

impl std::fmt::Debug for DynclibHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynclibHost").finish_non_exhaustive()
    }
}

// Safety: The Library handle and resolved function pointers are safe to send
// across threads. The resolved symbols point into memory-mapped shared library
// code which is process-global and immutable.
unsafe impl Send for DynclibHost {}
unsafe impl Sync for DynclibHost {}

impl DynclibHost {
    /// Load a shared library from the given filesystem path.
    ///
    /// Resolves the required ABI symbols (`actr_init`, `actr_handle`,
    /// `actr_free_response`, `actr_shutdown`). Returns an error if any symbol
    /// is missing.
    pub fn load(path: impl AsRef<Path>) -> DynclibResult<Self> {
        let path = path.as_ref();
        tracing::info!(path = %path.display(), "loading dynclib actor");

        // Safety: loading a shared library executes its static initialisers,
        // which is inherently unsafe. The caller must ensure the library is
        // trusted (e.g. verified by Hyper's package verification).
        let library = unsafe {
            Library::new(path)
                .map_err(|e| DynclibError::LoadFailed(format!("{}: {e}", path.display())))?
        };

        // Safety: we resolve raw symbol pointers and transmute them to typed
        // function pointers. The caller must guarantee that the SO exports
        // these symbols with the correct C ABI signatures.
        let init_fn: InitFn = unsafe {
            let sym =
                library
                    .get::<InitFn>(b"actr_init\0")
                    .map_err(|e| DynclibError::MissingSymbol {
                        symbol: "actr_init".into(),
                        detail: e.to_string(),
                    })?;
            *sym
        };

        let handle_fn: HandleFn = unsafe {
            let sym = library.get::<HandleFn>(b"actr_handle\0").map_err(|e| {
                DynclibError::MissingSymbol {
                    symbol: "actr_handle".into(),
                    detail: e.to_string(),
                }
            })?;
            *sym
        };

        let free_response_fn: FreeResponseFn = unsafe {
            let sym = library
                .get::<FreeResponseFn>(b"actr_free_response\0")
                .map_err(|e| DynclibError::MissingSymbol {
                    symbol: "actr_free_response".into(),
                    detail: e.to_string(),
                })?;
            *sym
        };

        let shutdown_fn: ShutdownFn = unsafe {
            let sym = library.get::<ShutdownFn>(b"actr_shutdown\0").map_err(|e| {
                DynclibError::MissingSymbol {
                    symbol: "actr_shutdown".into(),
                    detail: e.to_string(),
                }
            })?;
            *sym
        };

        tracing::info!(path = %path.display(), "dynclib symbols resolved successfully");

        Ok(Self {
            _library: library,
            init_fn,
            handle_fn,
            free_response_fn,
            shutdown_fn,
        })
    }

    /// Initialise an actor instance inside the loaded library.
    ///
    /// Calls the guest's `actr_init(vtable, init_ptr, init_len)`.
    pub(crate) fn instantiate(
        &self,
        init_payload: &InitPayloadV1,
    ) -> DynclibResult<DynclibInstance> {
        let init_bytes = guest_abi::encode_message(init_payload).map_err(|code| {
            DynclibError::DispatchFailed(format!("init payload encode failed: {code}"))
        })?;
        let init_ptr = if init_bytes.is_empty() {
            ptr::null()
        } else {
            init_bytes.as_ptr()
        };

        // Safety: `actr_init` is a C function resolved from the shared
        // library. `HOST_VTABLE` is a static with stable address. `init_ptr`
        // and `init_bytes.len()` describe a valid byte slice (or null/0).
        let result = unsafe { (self.init_fn)(&HOST_VTABLE, init_ptr, init_bytes.len()) };

        if result != 0 {
            tracing::error!(code = result, "actr_init failed");
            return Err(DynclibError::InitFailed(result));
        }

        tracing::info!("dynclib actor initialised successfully");

        Ok(DynclibInstance {
            handle_fn: self.handle_fn,
            free_response_fn: self.free_response_fn,
            shutdown_fn: self.shutdown_fn,
            is_shutdown: false,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DynclibInstance
// ─────────────────────────────────────────────────────────────────────────────

/// Per-actor instance backed by a native shared library.
///
/// Holds cached function pointers for `actr_handle` and `actr_free_response`.
/// `actr_init` initializes exactly one logical actor state inside this instance.
/// **Not `Sync`**: callers must serialise access (e.g. via `Mutex<DynClibWorkload>`)
/// and must not enter `actr_handle` concurrently for the same instance.
pub(crate) struct DynclibInstance {
    handle_fn: HandleFn,
    free_response_fn: FreeResponseFn,
    shutdown_fn: ShutdownFn,
    is_shutdown: bool,
}

impl std::fmt::Debug for DynclibInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynclibInstance").finish_non_exhaustive()
    }
}

// Safety: function pointers reference process-global SO code.
unsafe impl Send for DynclibInstance {}

/// Workload wrapper that keeps the loaded library alive for the lifetime of the actor instance.
///
/// Field order matters: Rust drops fields in declaration order, so `instance`
/// (which holds raw function pointers into the loaded library) must be dropped
/// before `_host` (which unloads the library).
#[derive(Debug)]
pub(crate) struct DynClibWorkload {
    instance: DynclibInstance,
    _host: DynclibHost,
}

impl DynClibWorkload {
    pub(crate) fn new(host: DynclibHost, instance: DynclibInstance) -> Self {
        Self {
            instance,
            _host: host,
        }
    }
}

impl DynclibInstance {
    /// Dispatch a request through the guest actor.
    ///
    /// This method:
    /// 1. Calls the guest's `actr_handle` on a blocking thread
    /// 2. Copies the response and frees the guest-allocated buffer
    /// 3. Leaves retained bridge tokens alive for any spawned guest tasks
    async fn handle_encoded_request(&mut self, request_owned: Vec<u8>) -> DynclibResult<Vec<u8>> {
        if self.is_shutdown {
            return Err(DynclibError::DispatchFailed(
                "dynclib instance is shut down".into(),
            ));
        }
        let handle_fn = self.handle_fn;
        let free_response_fn = self.free_response_fn;

        let result = tokio::task::spawn_blocking(move || {
            // Prepare output pointers.
            let mut resp_ptr: *mut u8 = ptr::null_mut();
            let mut resp_len: usize = 0;

            // Safety: `handle_fn` is a C function from the loaded SO.
            // `request_owned` is a valid Vec<u8> and `as_ptr()`/`len()` describe
            // a valid slice. `resp_ptr` and `resp_len` are stack-local variables
            // whose addresses are valid for the duration of the call.
            let code = unsafe {
                (handle_fn)(
                    request_owned.as_ptr(),
                    request_owned.len(),
                    &mut resp_ptr,
                    &mut resp_len,
                )
            };

            // Copy response bytes before freeing the guest buffer.
            let response = if !resp_ptr.is_null() && resp_len > 0 {
                // Safety: the guest set resp_ptr/resp_len to describe a valid
                // allocation. We copy before calling free_response_fn.
                let data = unsafe { std::slice::from_raw_parts(resp_ptr, resp_len).to_vec() };

                // Safety: free the guest-allocated response buffer with the
                // guest's own free function.
                unsafe { (free_response_fn)(resp_ptr, resp_len) };

                data
            } else {
                Vec::new()
            };

            if code != 0 {
                tracing::warn!(code, "actr_handle returned error");
                return Err(DynclibError::DispatchFailed(format!(
                    "actr_handle returned error code {code}"
                )));
            }

            tracing::debug!(
                req_bytes = request_owned.len(),
                resp_bytes = response.len(),
                "actr_handle completed"
            );

            Ok(response)
        })
        .await
        .map_err(|e| DynclibError::DispatchFailed(format!("spawn_blocking panicked: {e}")))??;

        let reply = guest_abi::decode_message::<AbiReply>(&result).map_err(|code| {
            DynclibError::DispatchFailed(format!(
                "guest returned malformed AbiReply with code {code}"
            ))
        })?;

        if reply.status != guest_abi::code::SUCCESS {
            let message = String::from_utf8(reply.payload)
                .unwrap_or_else(|_| format!("guest returned status {}", reply.status));
            return Err(DynclibError::DispatchFailed(message));
        }

        Ok(reply.payload)
    }

    pub(crate) async fn handle(
        &mut self,
        request_bytes: &[u8],
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<Vec<u8>> {
        let bridge = BridgeRegistration::new(call_executor);
        let request_owned =
            encode_guest_handle_request(request_bytes, ctx, bridge.token).map_err(|code| {
                DynclibError::DispatchFailed(format!(
                    "guest handle frame serialization failed: {code}"
                ))
            })?;
        self.handle_encoded_request(request_owned).await
    }

    pub(crate) async fn handle_data_chunk(
        &mut self,
        chunk: DataChunk,
        sender: ActrId,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<()> {
        let bridge = BridgeRegistration::new(call_executor);
        let request_owned =
            encode_guest_data_chunk_request(chunk, sender, bridge.token).map_err(|code| {
                DynclibError::DispatchFailed(format!(
                    "guest data stream frame serialization failed: {code}"
                ))
            })?;
        self.handle_encoded_request(request_owned).await.map(|_| ())
    }

    pub(crate) async fn handle_lifecycle(
        &mut self,
        hook: u32,
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<()> {
        let bridge = BridgeRegistration::new(call_executor);
        let request_owned =
            encode_guest_lifecycle_request(hook, ctx, bridge.token).map_err(|code| {
                DynclibError::DispatchFailed(format!(
                    "guest lifecycle frame serialization failed: {code}"
                ))
            })?;
        self.handle_encoded_request(request_owned).await.map(|_| ())
    }

    pub(crate) async fn handle_hook_event(
        &mut self,
        event: PackageHookEvent,
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<()> {
        let bridge = BridgeRegistration::new(call_executor);
        let request_owned =
            encode_guest_hook_request(event, ctx, bridge.token).map_err(|code| {
                DynclibError::DispatchFailed(format!(
                    "guest hook frame serialization failed: {code}"
                ))
            })?;
        self.handle_encoded_request(request_owned).await.map(|_| ())
    }

    async fn shutdown(&mut self) -> DynclibResult<()> {
        if self.is_shutdown {
            return Ok(());
        }

        let shutdown_fn = self.shutdown_fn;
        let code = tokio::task::spawn_blocking(move || unsafe { shutdown_fn() })
            .await
            .map_err(|error| {
                DynclibError::DispatchFailed(format!("actr_shutdown panicked: {error}"))
            })?;
        if code != guest_abi::code::SUCCESS {
            return Err(DynclibError::DispatchFailed(format!(
                "actr_shutdown returned error code {code}"
            )));
        }
        self.is_shutdown = true;
        Ok(())
    }
}

impl Drop for DynclibInstance {
    fn drop(&mut self) {
        if self.is_shutdown {
            return;
        }

        let shutdown_fn = self.shutdown_fn;
        let result = std::thread::Builder::new()
            .name("actr-dynclib-shutdown".into())
            .spawn(move || unsafe { shutdown_fn() })
            .and_then(|thread| {
                thread
                    .join()
                    .map_err(|_| std::io::Error::other("actr_shutdown panicked"))
            });
        if let Err(error) = result {
            tracing::error!(%error, "failed to shut down dynclib before unload");
        }
        self.is_shutdown = true;
    }
}

impl DynClibWorkload {
    pub(crate) async fn handle(
        &mut self,
        request_bytes: &[u8],
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<Vec<u8>> {
        self.instance
            .handle(request_bytes, ctx, call_executor)
            .await
    }

    pub(crate) async fn handle_data_chunk(
        &mut self,
        chunk: DataChunk,
        sender: ActrId,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<()> {
        self.instance
            .handle_data_chunk(chunk, sender, call_executor)
            .await
    }

    pub(crate) async fn call_on_start(
        &mut self,
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<()> {
        self.instance
            .handle_lifecycle(guest_abi::lifecycle_hook::ON_START, ctx, call_executor)
            .await
    }

    pub(crate) async fn call_on_ready(
        &mut self,
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<()> {
        self.instance
            .handle_lifecycle(guest_abi::lifecycle_hook::ON_READY, ctx, call_executor)
            .await
    }

    pub(crate) async fn call_on_stop(
        &mut self,
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<()> {
        let hook_result = self
            .instance
            .handle_lifecycle(guest_abi::lifecycle_hook::ON_STOP, ctx, call_executor)
            .await;
        let shutdown_result = self.instance.shutdown().await;
        hook_result.and(shutdown_result)
    }

    pub(crate) async fn call_hook_event(
        &mut self,
        event: PackageHookEvent,
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> DynclibResult<()> {
        self.instance
            .handle_hook_event(event, ctx, call_executor)
            .await
    }

    pub(crate) async fn shutdown(&mut self) -> DynclibResult<()> {
        self.instance.shutdown().await
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn active_bridge_count() -> usize {
    bridge_registry()
        .lock()
        .map(|registry| registry.len())
        .unwrap_or_default()
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
