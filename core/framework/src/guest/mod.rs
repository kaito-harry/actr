//! Guest-side runtime module.
//!
//! Provides the unified [`entry!`] macro and platform-specific runtime
//! glue. Actor developers write one `entry!(MyActor)`; the macro selects
//! the correct ABI at compile time based on the target.
//!
//! # Execution contract
//!
//! - One loaded guest instance corresponds to one logical actor instance.
//! - The runtime serialises dispatch into the guest instance. Concurrent
//!   dispatches within the same instance are forbidden by the host
//!   (wasmtime enforces this via `&mut Store<HostState>`; dynclib hosts
//!   enforce via handle ownership).
//!
//! # Supported platforms
//!
//! - **WASM Component Model** (`target_arch = "wasm32"`, no `web` feature):
//!   wit-bindgen generates the `Guest` trait + `host` imports from
//!   `core/framework/wit/actr-workload.wit`; the [`entry!`] macro
//!   produces an adapter that bridges the user's [`Workload`] impl into
//!   the generated `Guest`. Targets `wasm32-wasip2` and requires
//!   `wasm-component-ld 0.5.22+` as the linker (see
//!   `experiments/component-spike-async/REPORT.md`).
//! - **Web ABI / wasm-bindgen** (`target_arch = "wasm32"` + `feature = "web"`):
//!   expands to a [`wasm_bindgen(start)`][wbgstart] bootstrap that wraps the
//!   user [`Workload`] in `web::WebWorkloadAdapter` and hands it to
//!   `actr_web_abi::host::register_workload`. Per Option U γ-unified §4.5
//!   the same user source compiles against both wasm32 ABIs; only the
//!   macro expansion differs. Targets `wasm32-unknown-unknown`.
//! - **cdylib** (`feature = "cdylib"`): HostVTable function-pointer
//!   bridge used for native shared-library guests (iOS / Android).
//!
//! [wbgstart]: https://rustwasm.github.io/wasm-bindgen/reference/attributes/on-rust-exports/start.html

pub mod dynclib_abi;
pub mod vtable;

/// Decode a dynclib `WebRtcPeerStatus` discriminant (see
/// [`dynclib_abi::webrtc_peer_status`]) into the framework enum. Used by the
/// `entry!` macro when materialising [`crate::PeerEvent`] from `PeerEventV1`.
pub fn peer_status_from_v1(status: u32) -> Option<crate::WebRtcPeerStatus> {
    use crate::guest::dynclib_abi::webrtc_peer_status as st;
    match status {
        st::IDLE => Some(crate::WebRtcPeerStatus::Idle),
        st::CONNECTING => Some(crate::WebRtcPeerStatus::Connecting),
        st::CONNECTED => Some(crate::WebRtcPeerStatus::Connected),
        st::RECOVERING => Some(crate::WebRtcPeerStatus::Recovering),
        other => {
            tracing::warn!(
                discriminant = other,
                "unknown dynclib WebRtcPeerStatus discriminant; dropping peer status"
            );
            None
        }
    }
}

// The Component Model wasm runtime glue is gated on `not(feature = "web")`
// so the `wasm32-unknown-unknown` + `web` target (which routes through
// `actr-web-abi` instead) does not link the wit-bindgen host imports that
// only resolve in a `wasm32-wasip2` Component environment.
#[cfg(all(target_arch = "wasm32", not(feature = "web")))]
pub mod wasm;

#[cfg(feature = "cdylib")]
pub mod dynclib;

// Re-exports used by the `entry!` macro so macro expansions under a
// user crate can reference adapter / binding items via a stable path
// without having to know the internal module layout.
#[cfg(all(target_arch = "wasm32", not(feature = "web")))]
#[doc(hidden)]
pub mod __wasm_macro_support {
    // The `entry!` macro expands inside the user's crate, so every name
    // it references must be reachable from an external crate path. Both
    // the adapter helpers and the generated WIT types are re-exported
    // here so the macro has a single stable `$crate::guest::__wasm_macro_support::*`
    // prefix to poke at.
    pub use super::wasm::adapter::{
        WorkloadCell, complete_sync, run_dispatch, run_on_credential_expiring, run_on_credential_renewed,
        run_on_data_stream, run_on_error, run_on_mailbox_backpressure, run_on_ready,
        run_on_signaling_connected, run_on_signaling_connecting, run_on_signaling_disconnected,
        run_on_start, run_on_stop, run_on_webrtc_connected, run_on_webrtc_connecting,
        run_on_webrtc_disconnected, run_on_websocket_connected, run_on_websocket_connecting,
        run_on_websocket_disconnected,
    };
    pub use super::wasm::generated::actr::workload::types::{
        ActrError as WitActrError, ActrId as WitActrId, BackpressureEvent as WitBackpressureEvent,
        CredentialEvent as WitCredentialEvent, DataStream as WitDataStream,
        ErrorEvent as WitErrorEvent, PeerEvent as WitPeerEvent, RpcEnvelope as WitRpcEnvelope,
    };
    pub use super::wasm::generated::exports::actr::workload::workload::Guest;
}

/// Generate Component Model exports for a [`Workload`][crate::Workload]
/// type.
///
/// Platform ABI is auto-selected by target:
///
/// - `#[cfg(all(target_arch = "wasm32", not(feature = "web")))]` — expands
///   to an `impl Guest for __ActrEntryAdapter { ... }` bridging the user's
///   [`Workload`][crate::Workload] into the `actr:workload/workload`
///   export contract. The runtime calls `Dispatcher::dispatch` through
///   the `dispatch` export and every observation hook through its
///   matching WIT export.
/// - `#[cfg(all(target_arch = "wasm32", feature = "web"))]` — expands to a
///   `#[wasm_bindgen(start)]` bootstrap that wraps the user workload in a
///   `WebWorkloadAdapter` and calls `actr_web_abi::host::register_workload`.
///   Only the 17 `#[wasm_bindgen]` entry points generated inside
///   `actr-web-abi::host` are exported to the Service Worker host.
/// - `#[cfg(feature = "cdylib")]` — expands to the legacy
///   `actr_init` / `actr_handle` / `actr_free_response` C-ABI exports
///   used by native shared-library hosts.
///
/// # Arguments
///
/// - `$workload_type`: type implementing
///   `actr_framework::Workload + Send + Sync + 'static`.
/// - `$init_expr` (optional): expression returning a fresh instance of
///   `$workload_type`. Defaults to `<$workload_type as Default>::default()`.
///
/// # Usage
///
/// ```rust,ignore
/// use actr_framework::entry;
///
/// entry!(EchoServiceWorkload<MyService>);
///
/// // Or with a custom constructor:
/// entry!(
///     EchoServiceWorkload<MyService>,
///     EchoServiceWorkload::new(MyService::new())
/// );
/// ```
#[macro_export]
macro_rules! entry {
    // Single-argument form: default-construct the workload.
    ($workload_type:ty) => {
        $crate::entry!($workload_type, <$workload_type as ::core::default::Default>::default());
    };

    // Two-argument form: caller supplies the init expression.
    ($workload_type:ty, $init_expr:expr) => {
        // ── WASM Component Model exports ──────────────────────────────────
        //
        // wit-bindgen generates an `exports::actr::workload::workload::Guest`
        // trait with 17 async methods (one `dispatch` + sixteen hooks). We
        // emit a single zero-sized adapter struct in user-crate scope and
        // route every method through helpers in `actr_framework::guest::wasm::adapter`.
        //
        // Skipped when the `web` feature is on — that path uses the
        // wasm-bindgen + `actr-web-abi` pipeline below instead of the
        // Component Model exports.
        #[cfg(all(target_arch = "wasm32", not(feature = "web")))]
        const _: () = {
            // Module-local singleton cell. Lazy-init on first call; subsequent
            // dispatches reuse the same instance.
            static __ACTR_WORKLOAD: $crate::guest::__wasm_macro_support::WorkloadCell<$workload_type> =
                $crate::guest::__wasm_macro_support::WorkloadCell::new();

            fn __actr_workload() -> &'static $workload_type {
                __ACTR_WORKLOAD.get_or_init(|| -> $workload_type { $init_expr })
            }

            struct __ActrEntryAdapter;

            impl $crate::guest::__wasm_macro_support::Guest for __ActrEntryAdapter {
                fn dispatch(
                    envelope: $crate::guest::__wasm_macro_support::WitRpcEnvelope,
                ) -> ::core::result::Result<
                    ::std::vec::Vec<u8>,
                    $crate::guest::__wasm_macro_support::WitActrError,
                > {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_dispatch(
                        __actr_workload(),
                        envelope,
                    ))
                }

                fn on_start() -> ::core::result::Result<
                    (),
                    $crate::guest::__wasm_macro_support::WitActrError,
                > {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_start(__actr_workload()))
                }

                fn on_ready() -> ::core::result::Result<
                    (),
                    $crate::guest::__wasm_macro_support::WitActrError,
                > {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_ready(__actr_workload()))
                }

                fn on_stop() -> ::core::result::Result<
                    (),
                    $crate::guest::__wasm_macro_support::WitActrError,
                > {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_stop(__actr_workload()))
                }

                fn on_error(
                    event: $crate::guest::__wasm_macro_support::WitErrorEvent,
                ) -> ::core::result::Result<
                    (),
                    $crate::guest::__wasm_macro_support::WitActrError,
                > {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_error(__actr_workload(), event))
                }

                fn on_signaling_connecting() {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_signaling_connecting(
                        __actr_workload(),
                    ))
                }

                fn on_signaling_connected() {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_signaling_connected(
                        __actr_workload(),
                    ))
                }

                fn on_signaling_disconnected() {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_signaling_disconnected(
                        __actr_workload(),
                    ))
                }

                fn on_websocket_connecting(
                    event: $crate::guest::__wasm_macro_support::WitPeerEvent,
                ) {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_websocket_connecting(
                        __actr_workload(),
                        event,
                    ))
                }

                fn on_websocket_connected(
                    event: $crate::guest::__wasm_macro_support::WitPeerEvent,
                ) {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_websocket_connected(
                        __actr_workload(),
                        event,
                    ))
                }

                fn on_websocket_disconnected(
                    event: $crate::guest::__wasm_macro_support::WitPeerEvent,
                ) {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_websocket_disconnected(
                        __actr_workload(),
                        event,
                    ))
                }

                fn on_webrtc_connecting(
                    event: $crate::guest::__wasm_macro_support::WitPeerEvent,
                ) {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_webrtc_connecting(
                        __actr_workload(),
                        event,
                    ))
                }

                fn on_webrtc_connected(
                    event: $crate::guest::__wasm_macro_support::WitPeerEvent,
                ) {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_webrtc_connected(
                        __actr_workload(),
                        event,
                    ))
                }

                fn on_webrtc_disconnected(
                    event: $crate::guest::__wasm_macro_support::WitPeerEvent,
                ) {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_webrtc_disconnected(
                        __actr_workload(),
                        event,
                    ))
                }

                fn on_credential_renewed(
                    event: $crate::guest::__wasm_macro_support::WitCredentialEvent,
                ) {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_credential_renewed(
                        __actr_workload(),
                        event,
                    ))
                }

                fn on_credential_expiring(
                    event: $crate::guest::__wasm_macro_support::WitCredentialEvent,
                ) {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_credential_expiring(
                        __actr_workload(),
                        event,
                    ))
                }

                fn on_mailbox_backpressure(
                    event: $crate::guest::__wasm_macro_support::WitBackpressureEvent,
                ) {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_mailbox_backpressure(
                        __actr_workload(),
                        event,
                    ))
                }

                fn on_data_stream(
                    chunk: $crate::guest::__wasm_macro_support::WitDataStream,
                    sender: $crate::guest::__wasm_macro_support::WitActrId,
                ) -> ::core::result::Result<
                    (),
                    $crate::guest::__wasm_macro_support::WitActrError,
                > {
                    $crate::guest::__wasm_macro_support::complete_sync($crate::guest::__wasm_macro_support::run_on_data_stream(chunk, sender))
                }
            }

            $crate::guest::wasm::generated::export!(__ActrEntryAdapter with_types_in $crate::guest::wasm::generated);
        };

        // ── Web (wasm-bindgen + actr-web-abi) exports ─────────────────────
        //
        // Phase 6b: wrap the user workload in `WebWorkloadAdapter` and hand
        // it to `actr_web_abi::host::register_workload` from a wasm-bindgen
        // `start` hook. The 17 `#[wasm_bindgen]` entry points exported by
        // `actr-web-abi::host` are the public surface the Service Worker
        // dispatches into; they resolve through the registered adapter back
        // into the user's `Workload` impl. See
        // `bindings/web/docs/option-u-phase6-gamma-unified.zh.md` §4.5.
        #[cfg(all(target_arch = "wasm32", feature = "web"))]
        const _: () = {
            // `wasm_bindgen(start)` functions are invoked once per module
            // instantiation by the wasm-bindgen runtime. `register_workload`
            // itself panics on double-registration, so wrapping it inside a
            // `start` fn naturally enforces single-shot bootstrap.
            //
            // The attribute is referenced through its fully-qualified path
            // so the user's crate does not need its own `wasm-bindgen`
            // dependency — `actr-framework` re-exports the attribute through
            // `web::__web_macro_support` under `feature = "web"`.
            #[$crate::web::__web_macro_support::wasm_bindgen(start)]
            fn __actr_web_bootstrap() {
                let workload: $workload_type = $init_expr;
                let adapter =
                    $crate::web::__web_macro_support::WebWorkloadAdapter::new(workload);
                $crate::web::__web_macro_support::register_workload(adapter);
            }
        };

        // ── cdylib ABI exports ────────────────────────────────────────────
        //
        // Unchanged from pre-Phase-1; the Component Model rewrite is WASM-
        // only and does not touch the native shared-library path.
        #[cfg(feature = "cdylib")]
        const _: () = {
            static mut __ACTR_WORKLOAD: Option<$workload_type> = None;
            static mut __ACTR_VTABLE: Option<*const $crate::guest::vtable::HostVTable> = None;

            /// Initialize actor
            ///
            /// Host calls this after dlopen, passing HostVTable and init payload.
            /// Returns 0 on success, negative on error.
            /// Repeated calls return `INIT_FAILED` (one init per guest instance).
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn actr_init(
                vtable: *const $crate::guest::vtable::HostVTable,
                init_ptr: *const u8,
                init_len: usize,
            ) -> i32 {
                if vtable.is_null() {
                    return $crate::guest::dynclib_abi::code::INIT_FAILED;
                }

                let init_bytes = if init_ptr.is_null() || init_len == 0 {
                    &[][..]
                } else {
                    unsafe { std::slice::from_raw_parts(init_ptr, init_len) }
                };

                // TODO: `actr_init` currently only validates that `InitPayloadV1`
                // is decodable. The payload fields themselves are not yet
                // consumed by the guest runtime on the dynclib path. This is a
                // legacy gap carried forward from the previous init model.
                if $crate::guest::dynclib_abi::decode_message::<$crate::guest::dynclib_abi::InitPayloadV1>(
                    init_bytes,
                )
                .is_err()
                {
                    return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR;
                }

                let workload: $workload_type = $init_expr;
                unsafe {
                    if __ACTR_WORKLOAD.is_some() {
                        return $crate::guest::dynclib_abi::code::INIT_FAILED;
                    }
                    __ACTR_VTABLE = Some(vtable);
                    __ACTR_WORKLOAD = Some(workload);
                }
                $crate::guest::dynclib_abi::code::SUCCESS
            }

            /// Handle one runtime ABI frame.
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn actr_handle(
                req_ptr: *const u8,
                req_len: usize,
                resp_out: *mut *mut u8,
                resp_len_out: *mut usize,
            ) -> i32 {
                use actr_protocol::prost::Message as ProstMessage;
                use $crate::{MessageDispatcher, Workload};

                // Get vtable
                let vtable = match unsafe { __ACTR_VTABLE } {
                    Some(vt) => vt,
                    None => return $crate::guest::dynclib_abi::code::INIT_FAILED,
                };

                // Read runtime frame
                if req_ptr.is_null() {
                    return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR;
                }
                let req_bytes = unsafe { std::slice::from_raw_parts(req_ptr, req_len) };

                let frame = match $crate::guest::dynclib_abi::decode_message::<
                    $crate::guest::dynclib_abi::AbiFrame,
                >(req_bytes) {
                    Ok(f) => f,
                    Err(_) => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                };

                if frame.op == $crate::guest::dynclib_abi::op::GUEST_LIFECYCLE {
                    let payload = match <$crate::guest::dynclib_abi::GuestLifecycleV1 as $crate::guest::dynclib_abi::AbiPayload>::decode_payload(&frame.payload) {
                        Ok(payload) => payload,
                        Err(_) => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                    };

                    let ctx = match unsafe {
                        $crate::guest::dynclib::context::DynclibContext::from_invocation(vtable, payload.ctx)
                    } {
                        Ok(c) => c,
                        Err(_) => return $crate::guest::dynclib_abi::code::HANDLE_FAILED,
                    };

                    let workload = unsafe {
                        match __ACTR_WORKLOAD.as_ref() {
                            Some(w) => w,
                            None => return $crate::guest::dynclib_abi::code::INIT_FAILED,
                        }
                    };

                    let lifecycle_result = match payload.hook {
                        $crate::guest::dynclib_abi::lifecycle_hook::ON_START => {
                            let fut = workload.on_start(&ctx);
                            let waker = std::task::Waker::noop();
                            let mut cx = std::task::Context::from_waker(waker);
                            let mut pinned = std::pin::pin!(fut);
                            match pinned.as_mut().poll(&mut cx) {
                                std::task::Poll::Ready(v) => v,
                                std::task::Poll::Pending => {
                                    return $crate::guest::dynclib_abi::code::HANDLE_FAILED;
                                }
                            }
                        }
                        $crate::guest::dynclib_abi::lifecycle_hook::ON_READY => {
                            let fut = workload.on_ready(&ctx);
                            let waker = std::task::Waker::noop();
                            let mut cx = std::task::Context::from_waker(waker);
                            let mut pinned = std::pin::pin!(fut);
                            match pinned.as_mut().poll(&mut cx) {
                                std::task::Poll::Ready(v) => v,
                                std::task::Poll::Pending => {
                                    return $crate::guest::dynclib_abi::code::HANDLE_FAILED;
                                }
                            }
                        }
                        $crate::guest::dynclib_abi::lifecycle_hook::ON_STOP => {
                            let fut = workload.on_stop(&ctx);
                            let waker = std::task::Waker::noop();
                            let mut cx = std::task::Context::from_waker(waker);
                            let mut pinned = std::pin::pin!(fut);
                            match pinned.as_mut().poll(&mut cx) {
                                std::task::Poll::Ready(v) => v,
                                std::task::Poll::Pending => {
                                    return $crate::guest::dynclib_abi::code::HANDLE_FAILED;
                                }
                            }
                        }
                        _ => return $crate::guest::dynclib_abi::code::UNSUPPORTED_OP,
                    };

                    let resp_bytes = match lifecycle_result {
                        Ok(()) => match $crate::guest::dynclib_abi::success_reply(::std::vec::Vec::new()) {
                            Ok(bytes) => bytes,
                            Err(code) => return code,
                        },
                        Err(err) => match $crate::guest::dynclib_abi::error_reply(
                            $crate::guest::dynclib_abi::code::HANDLE_FAILED,
                            err.to_string().into_bytes(),
                        ) {
                            Ok(bytes) => bytes,
                            Err(code) => return code,
                        },
                    };

                    let resp_len = resp_bytes.len();
                    let layout = match std::alloc::Layout::from_size_align(resp_len.max(1), 1) {
                        Ok(l) => l,
                        Err(_) => return $crate::guest::dynclib_abi::code::GENERIC_ERROR,
                    };
                    let ptr = unsafe { std::alloc::alloc(layout) };
                    if ptr.is_null() {
                        return $crate::guest::dynclib_abi::code::GENERIC_ERROR;
                    }

                    unsafe {
                        std::ptr::copy_nonoverlapping(resp_bytes.as_ptr(), ptr, resp_len);
                        *resp_out = ptr;
                        *resp_len_out = resp_len;
                    }

                    return $crate::guest::dynclib_abi::code::SUCCESS;
                }

                if frame.op == $crate::guest::dynclib_abi::op::GUEST_HOOK {
                    let payload = match <$crate::guest::dynclib_abi::GuestHookV1 as $crate::guest::dynclib_abi::AbiPayload>::decode_payload(&frame.payload) {
                        Ok(payload) => payload,
                        Err(_) => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                    };

                    let ctx = match unsafe {
                        $crate::guest::dynclib::context::DynclibContext::from_invocation(vtable, payload.ctx)
                    } {
                        Ok(c) => c,
                        Err(_) => return $crate::guest::dynclib_abi::code::HANDLE_FAILED,
                    };

                    let workload = unsafe {
                        match __ACTR_WORKLOAD.as_ref() {
                            Some(w) => w,
                            None => return $crate::guest::dynclib_abi::code::INIT_FAILED,
                        }
                    };

                    macro_rules! __actr_poll_unit {
                        ($future:expr) => {{
                            let fut = $future;
                            let waker = std::task::Waker::noop();
                            let mut cx = std::task::Context::from_waker(waker);
                            let mut pinned = std::pin::pin!(fut);
                            match pinned.as_mut().poll(&mut cx) {
                                std::task::Poll::Ready(()) => {}
                                std::task::Poll::Pending => {
                                    return $crate::guest::dynclib_abi::code::HANDLE_FAILED;
                                }
                            }
                        }};
                    }

                    let peer_event = |peer: $crate::guest::dynclib_abi::PeerEventV1| {
                        $crate::PeerEvent {
                            peer: peer.peer,
                            relayed: peer.relayed,
                            status: peer
                                .status
                                .and_then($crate::guest::peer_status_from_v1),
                        }
                    };

                    let timestamp = |ts: $crate::guest::dynclib_abi::TimestampV1| {
                        std::time::UNIX_EPOCH
                            + std::time::Duration::new(ts.seconds, ts.nanoseconds)
                    };

                    match payload.hook {
                        $crate::guest::dynclib_abi::runtime_hook::ON_SIGNALING_CONNECTING => {
                            __actr_poll_unit!(workload.on_signaling_connecting(Some(&ctx)));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_SIGNALING_CONNECTED => {
                            __actr_poll_unit!(workload.on_signaling_connected(Some(&ctx)));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_SIGNALING_DISCONNECTED => {
                            __actr_poll_unit!(workload.on_signaling_disconnected(&ctx));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_WEBSOCKET_CONNECTING => {
                            let event = match payload.peer {
                                Some(peer) => peer_event(peer),
                                None => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                            };
                            __actr_poll_unit!(workload.on_websocket_connecting(&ctx, &event));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_WEBSOCKET_CONNECTED => {
                            let event = match payload.peer {
                                Some(peer) => peer_event(peer),
                                None => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                            };
                            __actr_poll_unit!(workload.on_websocket_connected(&ctx, &event));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_WEBSOCKET_DISCONNECTED => {
                            let event = match payload.peer {
                                Some(peer) => peer_event(peer),
                                None => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                            };
                            __actr_poll_unit!(workload.on_websocket_disconnected(&ctx, &event));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_WEBRTC_CONNECTING => {
                            let event = match payload.peer {
                                Some(peer) => peer_event(peer),
                                None => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                            };
                            __actr_poll_unit!(workload.on_webrtc_connecting(&ctx, &event));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_WEBRTC_CONNECTED => {
                            let event = match payload.peer {
                                Some(peer) => peer_event(peer),
                                None => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                            };
                            __actr_poll_unit!(workload.on_webrtc_connected(&ctx, &event));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_WEBRTC_DISCONNECTED => {
                            let event = match payload.peer {
                                Some(peer) => peer_event(peer),
                                None => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                            };
                            __actr_poll_unit!(workload.on_webrtc_disconnected(&ctx, &event));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_CREDENTIAL_RENEWED => {
                            let event = match payload.credential {
                                Some(credential) => $crate::CredentialEvent {
                                    new_expiry: timestamp(credential.new_expiry),
                                },
                                None => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                            };
                            __actr_poll_unit!(workload.on_credential_renewed(&ctx, &event));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_CREDENTIAL_EXPIRING => {
                            let event = match payload.credential {
                                Some(credential) => $crate::CredentialEvent {
                                    new_expiry: timestamp(credential.new_expiry),
                                },
                                None => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                            };
                            __actr_poll_unit!(workload.on_credential_expiring(&ctx, &event));
                        }
                        $crate::guest::dynclib_abi::runtime_hook::ON_MAILBOX_BACKPRESSURE => {
                            let event = match payload.backpressure {
                                Some(backpressure) => $crate::BackpressureEvent {
                                    queue_len: backpressure.queue_len as usize,
                                    threshold: backpressure.threshold as usize,
                                },
                                None => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                            };
                            __actr_poll_unit!(workload.on_mailbox_backpressure(&ctx, &event));
                        }
                        _ => return $crate::guest::dynclib_abi::code::UNSUPPORTED_OP,
                    }

                    let resp_bytes = match $crate::guest::dynclib_abi::success_reply(::std::vec::Vec::new()) {
                        Ok(bytes) => bytes,
                        Err(code) => return code,
                    };

                    let resp_len = resp_bytes.len();
                    let layout = match std::alloc::Layout::from_size_align(resp_len.max(1), 1) {
                        Ok(l) => l,
                        Err(_) => return $crate::guest::dynclib_abi::code::GENERIC_ERROR,
                    };
                    let ptr = unsafe { std::alloc::alloc(layout) };
                    if ptr.is_null() {
                        return $crate::guest::dynclib_abi::code::GENERIC_ERROR;
                    }

                    unsafe {
                        std::ptr::copy_nonoverlapping(resp_bytes.as_ptr(), ptr, resp_len);
                        *resp_out = ptr;
                        *resp_len_out = resp_len;
                    }

                    return $crate::guest::dynclib_abi::code::SUCCESS;
                }

                if frame.op == $crate::guest::dynclib_abi::op::GUEST_DATA_STREAM {
                    let payload = match <$crate::guest::dynclib_abi::GuestDataStreamV1 as $crate::guest::dynclib_abi::AbiPayload>::decode_payload(&frame.payload) {
                        Ok(payload) => payload,
                        Err(_) => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                    };

                    let resp_bytes = match $crate::guest::dynclib::context::dispatch_registered_stream(payload) {
                        Ok(()) => match $crate::guest::dynclib_abi::success_reply(::std::vec::Vec::new()) {
                            Ok(bytes) => bytes,
                            Err(code) => return code,
                        },
                        Err(err) => match $crate::guest::dynclib_abi::error_reply(
                            $crate::guest::dynclib_abi::code::HANDLE_FAILED,
                            err.to_string().into_bytes(),
                        ) {
                            Ok(bytes) => bytes,
                            Err(code) => return code,
                        },
                    };

                    let resp_len = resp_bytes.len();
                    let layout = match std::alloc::Layout::from_size_align(resp_len.max(1), 1) {
                        Ok(l) => l,
                        Err(_) => return $crate::guest::dynclib_abi::code::GENERIC_ERROR,
                    };
                    let ptr = unsafe { std::alloc::alloc(layout) };
                    if ptr.is_null() {
                        return $crate::guest::dynclib_abi::code::GENERIC_ERROR;
                    }

                    unsafe {
                        std::ptr::copy_nonoverlapping(resp_bytes.as_ptr(), ptr, resp_len);
                        *resp_out = ptr;
                        *resp_len_out = resp_len;
                    }

                    return $crate::guest::dynclib_abi::code::SUCCESS;
                }

                if frame.op != $crate::guest::dynclib_abi::op::GUEST_HANDLE {
                    return $crate::guest::dynclib_abi::code::UNSUPPORTED_OP;
                }

                let handle = match <$crate::guest::dynclib_abi::GuestHandleV1 as $crate::guest::dynclib_abi::AbiPayload>::decode_payload(&frame.payload) {
                    Ok(handle) => handle,
                    Err(_) => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                };

                let envelope = match actr_protocol::RpcEnvelope::decode(handle.rpc_envelope.as_slice()) {
                    Ok(e) => e,
                    Err(_) => return $crate::guest::dynclib_abi::code::PROTOCOL_ERROR,
                };

                let ctx = match unsafe {
                    $crate::guest::dynclib::context::DynclibContext::from_invocation(vtable, handle.ctx)
                } {
                    Ok(c) => c,
                    Err(_) => return $crate::guest::dynclib_abi::code::HANDLE_FAILED,
                };

                // Get workload reference
                let workload = unsafe {
                    match __ACTR_WORKLOAD.as_ref() {
                        Some(w) => w,
                        None => return $crate::guest::dynclib_abi::code::INIT_FAILED,
                    }
                };

                // Route and execute via MessageDispatcher
                type Dispatcher = <$workload_type as Workload>::Dispatcher;

                // cdylib is native environment, can use tokio or synchronous execution
                // Here we use the same single-threaded poll strategy as the old WASM path:
                // All host callbacks (vtable function pointers) are synchronous, Future
                // completes in one poll.
                let resp_result = {
                    let fut = Dispatcher::dispatch(workload, envelope, &ctx);
                    let waker = std::task::Waker::noop();
                    let mut cx = std::task::Context::from_waker(waker);
                    let mut pinned = std::pin::pin!(fut);
                    match pinned.as_mut().poll(&mut cx) {
                        std::task::Poll::Ready(v) => v,
                        std::task::Poll::Pending => {
                            return $crate::guest::dynclib_abi::code::HANDLE_FAILED;
                        }
                    }
                };

                let resp_bytes = match resp_result {
                    Ok(b) => match $crate::guest::dynclib_abi::success_reply(b.to_vec()) {
                        Ok(bytes) => bytes,
                        Err(code) => return code,
                    },
                    Err(err) => match $crate::guest::dynclib_abi::error_reply(
                        $crate::guest::dynclib_abi::code::HANDLE_FAILED,
                        err.to_string().into_bytes(),
                    ) {
                        Ok(bytes) => bytes,
                        Err(code) => return code,
                    },
                };

                // Allocate response buffer on guest heap
                let resp_len = resp_bytes.len();
                let layout = match std::alloc::Layout::from_size_align(resp_len.max(1), 1) {
                    Ok(l) => l,
                    Err(_) => return $crate::guest::dynclib_abi::code::GENERIC_ERROR,
                };
                let ptr = unsafe { std::alloc::alloc(layout) };
                if ptr.is_null() {
                    return $crate::guest::dynclib_abi::code::GENERIC_ERROR;
                }

                unsafe {
                    std::ptr::copy_nonoverlapping(resp_bytes.as_ptr(), ptr, resp_len);
                    *resp_out = ptr;
                    *resp_len_out = resp_len;
                }

                $crate::guest::dynclib_abi::code::SUCCESS
            }

            /// Free guest-allocated response buffer
            ///
            /// Host calls this after using the response data returned by `actr_handle`.
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn actr_free_response(ptr: *mut u8, len: usize) {
                if ptr.is_null() || len == 0 {
                    return;
                }
                let layout = match std::alloc::Layout::from_size_align(len, 1) {
                    Ok(l) => l,
                    Err(_) => return,
                };
                unsafe { std::alloc::dealloc(ptr, layout) };
            }
        };
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_status_from_v1_drops_unknown_discriminants() {
        use crate::guest::dynclib_abi::webrtc_peer_status as st;

        assert_eq!(
            peer_status_from_v1(st::IDLE),
            Some(crate::WebRtcPeerStatus::Idle)
        );
        assert_eq!(
            peer_status_from_v1(st::CONNECTING),
            Some(crate::WebRtcPeerStatus::Connecting)
        );
        assert_eq!(
            peer_status_from_v1(st::CONNECTED),
            Some(crate::WebRtcPeerStatus::Connected)
        );
        assert_eq!(
            peer_status_from_v1(st::RECOVERING),
            Some(crate::WebRtcPeerStatus::Recovering)
        );
        assert_eq!(peer_status_from_v1(u32::MAX), None);
    }
}
