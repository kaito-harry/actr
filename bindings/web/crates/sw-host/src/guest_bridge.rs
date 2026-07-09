//! Wasm-bindgen guest bridge for the Web runtime.
//!
//! # Role
//!
//! This crate (`actr-sw-host`, Rust compiled to wasm32 via wasm-bindgen) plays
//! two roles in the SW-side guest pipeline:
//!
//! 1. **Guest dispatch driver** — [`register_guest_workload`] accepts a JS
//!    callback that forwards to the wasm-bindgen guest module's `dispatch`
//!    export (emitted by `actr-web-abi`, see Option U in
//!    `bindings/web/docs/option-u-wit-compile-web.zh.md`). The runtime wraps
//!    that callback behind the [`WasmWorkload`] abstraction used by the
//!    inbound packet dispatcher, threading per-dispatch `RuntimeContext`
//!    into [`DISPATCH_CTXS`] keyed by `request_id`.
//! 2. **Host import implementations** — [`host_call_async`],
//!    [`host_call_raw_async`], [`host_discover_async`], [`host_log_message`],
//!    [`host_tell_async`] and the per-dispatch context getters expose the
//!    Rust side of the WIT `actr:workload/host` interface as `#[wasm_bindgen]`
//!    JS functions. The SW glue (`actor.sw.js`) installs `actrHost*` JS
//!    globals that proxy onto these.
//!
//! # WIT host interface, as JS function names
//!
//! | WIT                                 | JS function (this module)        |
//! |-------------------------------------|-----------------------------------|
//! | `host.call(target, route, payload)` | [`host_call_async`]              |
//! | `host.tell(target, route, payload)` | [`host_tell_async`]              |
//! | `host.call-raw(id, route, payload)` | [`host_call_raw_async`]          |
//! | `host.discover(type)`               | [`host_discover_async`]          |
//! | `host.log-message(level, message)`  | [`host_log_message`]             |
//! | `host.get-self-id()`                | [`host_get_self_id`]             |
//! | `host.get-caller-id()`              | [`host_get_caller_id`]           |
//! | `host.get-request-id()`             | [`host_get_request_id`]          |
//!
//! # Dispatch flow
//!
//! ```text
//!  inbound RPC ─► WasmWorkload ─► ServiceHandlerFn
//!                                   │
//!                                   ├─ ctx_insert(request_id, ctx)
//!                                   ├─ build envelope JS object
//!                                   ├─ await dispatchFn(envelope)         ─┐
//!                                   │    (wasm-bindgen guest dispatch)     │
//!                                   │                                      │
//!                                   │      during guest execution the      │
//!                                   │      `actrHost*` globals call back   │
//!                                   │      into host_*_async(request_id,   │
//!                                   │      …) which looks the ctx up in    │
//!                                   │      DISPATCH_CTXS and routes        │
//!                                   │      through RuntimeContext          │
//!                                   │                                      │
//!                                   ├─ ctx_remove(request_id)             ◄┘
//!                                   └─ return reply bytes
//! ```
//!
//! Multiple concurrent dispatches coexist in `DISPATCH_CTXS`; each host
//! import resolves the owning context by the `request_id` the runtime wove
//! through the WIT surface. See TD-003 for the single-slot bug this
//! replaces.
//!
//! # History
//!
//! This module previously also fronted a Component Model + `jco`-transpiled
//! guest path. Phase 8 (Option U §11) deleted that path; the function
//! historically exported as `register_component_workload` is now the
//! generically-named [`register_guest_workload`], used by the wasm-bindgen
//! guest pipeline that replaced it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;

use actr_framework::Dest;
use actr_protocol::{ActrError, ActrId, ActrType, Direction, RpcEnvelope};
use bytes::Bytes;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

use crate::context::RuntimeContext;
use crate::web_context::WebContext;
use crate::workload::{ServiceHandlerFn, WasmWorkload};

// ─────────────────────────────────────────────────────────────────────────────
// Per-dispatch context
// ─────────────────────────────────────────────────────────────────────────────

// Per-request `RuntimeContext` table, keyed by `request_id`. Each concurrent
// guest dispatch registers its context on entry and removes it on exit; the
// host-import functions (`host_*_async`) look up the context by the
// `request_id` passed as their first parameter.
//
// JS is single-threaded, so `RefCell::borrow_mut` is race-free: insert /
// remove happen at dispatch boundaries, lookups are bounded by a borrow that
// drops before the host-import function awaits anything else.
//
// This replaces the former single `GUEST_CTX` slot, which only supported one
// in-flight dispatch at a time and was the root cause of TD-003.
thread_local! {
    // NOTE: `HashMap::new` is not `const` on stable Rust, so this slot cannot
    // use the `const { ... }` form (that shortcut is available for the old
    // `Option<Rc<_>>` slot). The lazy-init cost is a one-time `Default`
    // invocation per thread; irrelevant in the single-threaded SW.
    static DISPATCH_CTXS: RefCell<HashMap<String, Rc<RuntimeContext>>> =
        RefCell::new(HashMap::new());
}

/// Register a `RuntimeContext` for the given `request_id` at the start of a
/// guest dispatch. Logs an error if the slot is already occupied — the
/// runtime should guarantee unique request IDs; collisions indicate a
/// framework-level invariant violation.
fn ctx_insert(request_id: String, ctx: Rc<RuntimeContext>) {
    DISPATCH_CTXS.with(|cell| {
        let mut map = cell.borrow_mut();
        if map.contains_key(&request_id) {
            log::error!(
                "[GuestBridge] ctx_insert overwriting existing entry for request_id={request_id}"
            );
        }
        map.insert(request_id, ctx);
    });
}

/// Look up the `RuntimeContext` for an in-flight dispatch by `request_id`.
/// Returns a JS error string if no matching entry is present — host imports
/// called outside an active dispatch (or with a stale `request_id`) reject
/// rather than panic.
fn ctx_get(request_id: &str) -> Result<Rc<RuntimeContext>, JsValue> {
    DISPATCH_CTXS.with(|cell| {
        cell.borrow().get(request_id).cloned().ok_or_else(|| {
            JsValue::from_str(&format!("no guest context for request_id={request_id}"))
        })
    })
}

/// Remove the per-dispatch context after the guest's `dispatch` future
/// completes (success or failure). Safe to call when the entry is already
/// absent.
fn ctx_remove(request_id: &str) {
    DISPATCH_CTXS.with(|cell| {
        cell.borrow_mut().remove(request_id);
    });
}

/// RAII guard: removes the dispatch context on drop so early-return / panic /
/// future-cancellation paths all clean up deterministically.
struct DispatchCtxGuard {
    request_id: String,
}

impl Drop for DispatchCtxGuard {
    fn drop(&mut self) {
        ctx_remove(&self.request_id);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JS <-> Rust type helpers for the WIT surface
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a WIT `actr-id` record (as a JS object) into the protobuf
/// `ActrId`. The JS caller builds this from the WBG guest's `actrIdToJs` shape
/// — `{ realm: { realmId }, serialNumber, type: { manufacturer, name, version } }`
/// — and passes it through to Rust.
fn parse_actr_id(value: &JsValue) -> Result<ActrId, JsValue> {
    let realm = js_sys::Reflect::get(value, &JsValue::from_str("realm"))?;
    let realm_id = js_sys::Reflect::get(&realm, &JsValue::from_str("realmId"))?
        .as_f64()
        .ok_or_else(|| JsValue::from_str("realm.realmId not a number"))? as u32;

    let serial_number = js_sys::Reflect::get(value, &JsValue::from_str("serialNumber"))?;
    let serial_number = if let Some(f) = serial_number.as_f64() {
        f as u64
    } else {
        // The WBG guest emits `bigint` for WIT u64; fall back to string parse.
        let s = serial_number
            .as_string()
            .ok_or_else(|| JsValue::from_str("serialNumber not number/string"))?;
        s.parse::<u64>()
            .map_err(|e| JsValue::from_str(&format!("serialNumber parse: {e}")))?
    };

    // `type` collides with the WIT escape (`%type`); the WBG guest emits plain `type`
    // on the JS object. Probe both for robustness.
    let ty = js_sys::Reflect::get(value, &JsValue::from_str("type"))?;
    let ty = if ty.is_undefined() {
        js_sys::Reflect::get(value, &JsValue::from_str("%type"))?
    } else {
        ty
    };
    let manufacturer = js_sys::Reflect::get(&ty, &JsValue::from_str("manufacturer"))?
        .as_string()
        .ok_or_else(|| JsValue::from_str("type.manufacturer missing"))?;
    let name = js_sys::Reflect::get(&ty, &JsValue::from_str("name"))?
        .as_string()
        .ok_or_else(|| JsValue::from_str("type.name missing"))?;
    let version = js_sys::Reflect::get(&ty, &JsValue::from_str("version"))?
        .as_string()
        .ok_or_else(|| JsValue::from_str("type.version missing"))?;

    Ok(ActrId {
        realm: actr_protocol::Realm { realm_id },
        serial_number,
        r#type: ActrType {
            manufacturer,
            name,
            version,
        },
    })
}

/// Serialise `ActrId` into the JS-object shape the WBG guest expects. Mirror of
/// [`parse_actr_id`].
fn actr_id_to_js(id: &ActrId) -> JsValue {
    let realm_obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &realm_obj,
        &JsValue::from_str("realmId"),
        &JsValue::from_f64(id.realm.realm_id as f64),
    );

    let type_obj = js_sys::Object::new();
    let t = &id.r#type;
    let _ = js_sys::Reflect::set(
        &type_obj,
        &JsValue::from_str("manufacturer"),
        &JsValue::from_str(&t.manufacturer),
    );
    let _ = js_sys::Reflect::set(
        &type_obj,
        &JsValue::from_str("name"),
        &JsValue::from_str(&t.name),
    );
    let _ = js_sys::Reflect::set(
        &type_obj,
        &JsValue::from_str("version"),
        &JsValue::from_str(&t.version),
    );

    let obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&obj, &JsValue::from_str("realm"), &realm_obj);
    // Use `BigInt` for serial-number to match the guest's `bigint` u64 representation.
    let serial = js_sys::BigInt::from(id.serial_number);
    let _ = js_sys::Reflect::set(&obj, &JsValue::from_str("serialNumber"), &serial);
    let _ = js_sys::Reflect::set(&obj, &JsValue::from_str("type"), &type_obj);
    obj.into()
}

/// Parse a WIT `dest` variant JS object: `{ tag: 'host' | 'workload' | 'peer', val?: ActrId }`.
fn parse_dest(value: &JsValue) -> Result<Dest, JsValue> {
    let tag = js_sys::Reflect::get(value, &JsValue::from_str("tag"))?
        .as_string()
        .ok_or_else(|| JsValue::from_str("dest.tag missing or not a string"))?;
    match tag.as_str() {
        "host" => Ok(Dest::Host),
        "workload" => Ok(Dest::Workload),
        "peer" => {
            let val = js_sys::Reflect::get(value, &JsValue::from_str("val"))?;
            if val.is_undefined() || val.is_null() {
                return Err(JsValue::from_str("dest.peer missing val"));
            }
            Ok(Dest::Peer(parse_actr_id(&val)?))
        }
        other => Err(JsValue::from_str(&format!("unknown dest tag: {other}"))),
    }
}

/// Parse a WIT `actr-type` record JS object.
fn parse_actr_type(value: &JsValue) -> Result<ActrType, JsValue> {
    Ok(ActrType {
        manufacturer: js_sys::Reflect::get(value, &JsValue::from_str("manufacturer"))?
            .as_string()
            .ok_or_else(|| JsValue::from_str("manufacturer missing"))?,
        name: js_sys::Reflect::get(value, &JsValue::from_str("name"))?
            .as_string()
            .ok_or_else(|| JsValue::from_str("name missing"))?,
        version: js_sys::Reflect::get(value, &JsValue::from_str("version"))?
            .as_string()
            .ok_or_else(|| JsValue::from_str("version missing"))?,
    })
}

/// Translate an internal `ActrError` into a JS `Error` suitable for the guest's
/// `result<_, actr-error>` error arm. The JS glue in the SW wraps these
/// with the matching variant tags; here we flatten to message text plus a
/// machine-readable `name` attribute so the SW can re-tag deterministically.
fn actr_error_to_js(error: ActrError) -> JsValue {
    let mut retry_after_ms = None;
    let (tag, message) = match &error {
        ActrError::Unavailable(m) => ("unavailable", m.clone()),
        ActrError::ConnectionNotReady(info) => {
            retry_after_ms = info.retry_after_ms;
            ("connection-not-ready", format!("{info}"))
        }
        ActrError::TimedOut => ("timed-out", String::new()),
        ActrError::NotFound(m) => ("not-found", m.clone()),
        ActrError::PermissionDenied(m) => ("permission-denied", m.clone()),
        ActrError::InvalidArgument(m) => ("invalid-argument", m.clone()),
        ActrError::UnknownRoute(r) => ("unknown-route", r.clone()),
        ActrError::DependencyNotFound {
            service_name,
            message,
        } => ("dependency-not-found", format!("{service_name}: {message}")),
        ActrError::DecodeFailure(m) => ("decode-failure", m.clone()),
        ActrError::NotImplemented(m) => ("not-implemented", m.clone()),
        ActrError::Internal(m) => ("internal", m.clone()),
    };
    let err = js_sys::Error::new(&format!("{tag}: {message}"));
    let _ = js_sys::Reflect::set(&err, &JsValue::from_str("name"), &JsValue::from_str(tag));
    let _ = js_sys::Reflect::set(
        &err,
        &JsValue::from_str("actrErrorTag"),
        &JsValue::from_str(tag),
    );
    if tag == "connection-not-ready" {
        let value = retry_after_ms
            .map(|value| JsValue::from_f64(value as f64))
            .unwrap_or(JsValue::NULL);
        let _ = js_sys::Reflect::set(&err, &JsValue::from_str("actrRetryAfterMs"), &value);
    }
    err.into()
}

// ─────────────────────────────────────────────────────────────────────────────
// Public wasm-bindgen surface — host imports
// ─────────────────────────────────────────────────────────────────────────────

/// WIT `host.call-raw(target, route_key, payload) -> result<list<u8>, actr-error>`
///
/// Async; returns a Promise that resolves to a `Uint8Array` on success or
/// rejects with a JS `Error` whose `actrErrorTag` names the WIT variant.
///
/// The `request_id` first parameter identifies the owning dispatch and is
/// threaded through by the guest-side wrapper
/// (`actr_web_abi::guest::call_raw_with_request_id`). Two concurrent
/// dispatches no longer share a single thread-local context slot — they
/// resolve their respective `RuntimeContext` via `DISPATCH_CTXS`.
#[wasm_bindgen]
pub async fn host_call_raw_async(
    request_id: String,
    target: JsValue,
    route_key: String,
    payload: js_sys::Uint8Array,
) -> Result<js_sys::Uint8Array, JsValue> {
    let ctx = ctx_get(&request_id)?;
    let target = parse_actr_id(&target)?;
    let payload_bytes = payload.to_vec();

    log::info!(
        "[SW][GuestBridge] host.call-raw request_id={} target=<{}> route={} len={}",
        request_id,
        target.serial_number,
        route_key,
        payload_bytes.len()
    );

    match ctx
        .call_raw(&target, &route_key, &payload_bytes, 30_000)
        .await
    {
        Ok(response) => Ok(js_sys::Uint8Array::from(&response[..])),
        Err(e) => Err(actr_error_to_js(e)),
    }
}

/// WIT `host.call(target, route_key, payload) -> result<list<u8>, actr-error>`
///
/// The web runtime only supports `dest::peer` for typed calls today (it has
/// no in-browser Host/Workload routing); other variants return
/// `not-implemented`. Keeps the WIT contract uniform between server and
/// browser — the variant arm exists, it just isn't wired.
#[wasm_bindgen]
pub async fn host_call_async(
    request_id: String,
    target: JsValue,
    route_key: String,
    payload: js_sys::Uint8Array,
) -> Result<js_sys::Uint8Array, JsValue> {
    let ctx = ctx_get(&request_id)?;
    let dest = parse_dest(&target)?;
    let payload_bytes = payload.to_vec();

    let actor_id = match dest {
        Dest::Peer(id) => id,
        Dest::Host | Dest::Workload => {
            return Err(actr_error_to_js(ActrError::NotImplemented(
                "host.call with Host/Workload dest is unsupported in the web runtime".into(),
            )));
        }
    };

    log::info!(
        "[SW][GuestBridge] host.call request_id={} target=<{}> route={} len={}",
        request_id,
        actor_id.serial_number,
        route_key,
        payload_bytes.len()
    );

    match ctx
        .call_raw(&actor_id, &route_key, &payload_bytes, 30_000)
        .await
    {
        Ok(response) => Ok(js_sys::Uint8Array::from(&response[..])),
        Err(e) => Err(actr_error_to_js(e)),
    }
}

/// WIT `host.tell(target, route_key, payload) -> result<_, actr-error>`.
///
/// Fire-and-forget semantics: the envelope is stamped `Direction::Tell` via
/// `tell_raw`, no pending entry is registered, and no response ever arrives.
/// Only `Dest::Peer` is wired.
#[wasm_bindgen]
pub async fn host_tell_async(
    request_id: String,
    target: JsValue,
    route_key: String,
    payload: js_sys::Uint8Array,
) -> Result<(), JsValue> {
    let ctx = ctx_get(&request_id)?;
    let dest = parse_dest(&target)?;
    let payload_bytes = payload.to_vec();

    let actor_id = match dest {
        Dest::Peer(id) => id,
        Dest::Host | Dest::Workload => {
            return Err(actr_error_to_js(ActrError::NotImplemented(
                "host.tell with Host/Workload dest is unsupported in the web runtime".into(),
            )));
        }
    };

    match ctx.tell_raw(&actor_id, &route_key, payload_bytes).await {
        Ok(()) => Ok(()),
        Err(e) => Err(actr_error_to_js(e)),
    }
}

/// WIT `host.discover(target_type) -> result<actr-id, actr-error>`.
#[wasm_bindgen]
pub async fn host_discover_async(
    request_id: String,
    target_type: JsValue,
) -> Result<JsValue, JsValue> {
    let ctx = ctx_get(&request_id)?;
    let target_type = parse_actr_type(&target_type)?;

    log::info!(
        "[SW][GuestBridge] host.discover request_id={} target={}:{}:{}",
        request_id,
        target_type.manufacturer,
        target_type.name,
        target_type.version
    );

    match ctx.discover(&target_type).await {
        Ok(id) => Ok(actr_id_to_js(&id)),
        Err(e) => Err(actr_error_to_js(e)),
    }
}

/// WIT `host.log-message(level, message)`.
///
/// Maps to `log` crate levels. Levels outside the `trace/debug/info/warn/error`
/// set silently fall through to `info`. The `request_id` parameter is carried
/// for uniformity with the other host imports (and to annotate the log line);
/// it does not gate execution — logging from unknown dispatches still
/// surfaces.
#[wasm_bindgen]
pub fn host_log_message(request_id: String, level: String, message: String) {
    match level.as_str() {
        "error" => log::error!("[guest][req={request_id}] {message}"),
        "warn" => log::warn!("[guest][req={request_id}] {message}"),
        "debug" => log::debug!("[guest][req={request_id}] {message}"),
        "trace" => log::trace!("[guest][req={request_id}] {message}"),
        _ => log::info!("[guest][req={request_id}] {message}"),
    }
}

/// WIT `host.get-self-id() -> actr-id`.
#[wasm_bindgen]
pub fn host_get_self_id(request_id: String) -> Result<JsValue, JsValue> {
    let ctx = ctx_get(&request_id)?;
    Ok(actr_id_to_js(ctx.self_id()))
}

/// WIT `host.get-caller-id() -> option<actr-id>`. Returns `null` when the
/// host did not install a caller for this dispatch (lifecycle hooks).
#[wasm_bindgen]
pub fn host_get_caller_id(request_id: String) -> Result<JsValue, JsValue> {
    let ctx = ctx_get(&request_id)?;
    Ok(match ctx.caller_id() {
        Some(id) => actr_id_to_js(id),
        None => JsValue::NULL,
    })
}

/// WIT `host.get-request-id() -> string`.
///
/// Retaining the `request_id` input here is deliberate: the input and output
/// MUST match. It is asserted, giving us a cheap round-trip sanity check
/// between the guest-side wrapper (which has the request_id in hand from the
/// envelope) and the host-side dispatch table. The alternative — omitting
/// the parameter and treating it as a sentinel — would break uniformity
/// with the other 7 imports and require the WIT codegen to special-case it.
#[wasm_bindgen]
pub fn host_get_request_id(request_id: String) -> Result<String, JsValue> {
    let ctx = ctx_get(&request_id)?;
    let stored = ctx.request_id();
    debug_assert_eq!(
        stored, request_id,
        "host_get_request_id round-trip mismatch: stored={stored}, param={request_id}"
    );
    Ok(stored.to_string())
}

// ─────────────────────────────────────────────────────────────────────────────
// Guest workload registration — dispatch surface
// ─────────────────────────────────────────────────────────────────────────────

/// Register a wasm-bindgen guest workload.
///
/// `dispatch_fn` is a JS callback that forwards to the guest module's
/// `dispatch` export (emitted by `actr-web-abi`'s `__actr_workload_dispatch`).
/// Its signature must match:
///
/// ```text
/// async (envelope: RpcEnvelopeJs) => Uint8Array
/// ```
///
/// where `RpcEnvelopeJs` is the camelCase record built by sw-host on the
/// inbound side: `{ requestId: string, routeKey: string, payload: Uint8Array }`.
///
/// The JS side is responsible for:
/// 1. Instantiating the wasm-bindgen guest bundle (`<name>.wbg/guest.js` +
///    `_bg.wasm`) emitted by `tools/wit-compile-web` for the generated
///    `actr-web-abi` shim.
/// 2. Installing the `actrHost*` JS globals that the guest imports — they
///    proxy onto the `host_*_async` / `host_*` wasm-bindgen exports from
///    this crate (see `bindings/web/packages/web-sdk/src/actor.sw.js`).
/// 3. Passing `(envelope) => guestBindgen.dispatch(envelope)` here as
///    `dispatch_fn`.
///
/// When this function is invoked the runtime installs the `ServiceHandlerFn`
/// used by [`WasmWorkload`], which the inbound dispatcher drives.
///
/// # Naming
///
/// Pre-Phase-8 this was `register_component_workload`, when the SW also
/// supported a Component Model + `jco`-transpiled guest. With CM removed
/// (Option U §11), the WBG-only name is the accurate one.
#[wasm_bindgen]
pub fn register_guest_workload(dispatch_fn: js_sys::Function) {
    let handler: ServiceHandlerFn = Rc::new(
        move |route_key: &str, body: &[u8], ctx: Rc<RuntimeContext>| {
            let dispatch_fn = dispatch_fn.clone();
            let route_key = route_key.to_string();
            let body = body.to_vec();

            Box::pin(async move {
                let request_id = ctx.request_id().to_string();

                log::info!(
                    "[SW][GuestBridge] dispatch enter request_id={} route={} body_len={}",
                    request_id,
                    route_key,
                    body.len()
                );

                // Build the `rpc-envelope` JS object the WBG guest expects.
                let envelope_js = js_sys::Object::new();
                let _ = js_sys::Reflect::set(
                    &envelope_js,
                    &JsValue::from_str("requestId"),
                    &JsValue::from_str(&request_id),
                );
                let _ = js_sys::Reflect::set(
                    &envelope_js,
                    &JsValue::from_str("routeKey"),
                    &JsValue::from_str(&route_key),
                );
                let _ = js_sys::Reflect::set(
                    &envelope_js,
                    &JsValue::from_str("payload"),
                    &js_sys::Uint8Array::from(&body[..]).into(),
                );

                // Register the context for this request_id, and ensure it is
                // removed on every exit path (sync throw, promise reject,
                // promise resolve, future-cancellation) via the RAII guard.
                ctx_insert(request_id.clone(), ctx.clone());
                let _guard = DispatchCtxGuard {
                    request_id: request_id.clone(),
                };

                let result = match dispatch_fn.call1(&JsValue::NULL, &envelope_js) {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!(
                            "[SW][GuestBridge] dispatch threw synchronously request_id={request_id}: {e:?}"
                        );
                        return Err(format!("guest dispatch threw: {e:?}"));
                    }
                };

                log::info!(
                    "[SW][GuestBridge] dispatch_fn invoked request_id={}, awaiting promise (is_promise={})",
                    request_id,
                    result.is_instance_of::<js_sys::Promise>()
                );

                // The WBG guest's `dispatch` (from `actr-web-abi`) is async;
                // the return is always a Promise. Await it, then to bytes.
                let resolved = if result.is_instance_of::<js_sys::Promise>() {
                    let promise = js_sys::Promise::from(result);
                    match wasm_bindgen_futures::JsFuture::from(promise).await {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!(
                                "[SW][GuestBridge] dispatch promise rejected request_id={request_id}: {e:?}"
                            );
                            // The guest rejects with an `Error`-shaped variant;
                            // tag/message was set by `actr_error_to_js` on the
                            // host side, or by the guest-thrown error directly.
                            return Err(format!("guest dispatch rejected: {e:?}"));
                        }
                    }
                } else {
                    // Defensive: treat a sync return as an immediate Uint8Array.
                    result
                };

                log::info!("[SW][GuestBridge] dispatch promise resolved request_id={request_id}");

                if resolved.is_null() || resolved.is_undefined() {
                    return Err("guest dispatch returned null/undefined".to_string());
                }

                let arr = resolved
                    .dyn_into::<js_sys::Uint8Array>()
                    .map_err(|e| format!("guest dispatch did not return Uint8Array: {e:?}"))?;
                Ok(arr.to_vec())
                // `_guard` drops here, calling ctx_remove(&request_id).
            }) as Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>>>>
        },
    );

    crate::register_workload(WasmWorkload::new(handler));
    log::info!("[SW] Guest workload registered via wasm-bindgen bridge");
}

// ─────────────────────────────────────────────────────────────────────────────
// RpcEnvelope helpers (exposed for tests / advanced JS consumers)
// ─────────────────────────────────────────────────────────────────────────────

/// Construct an `RpcEnvelope` in Rust from parts (used internally when the
/// runtime synthesizes envelopes outside the dispatch callback path). Kept
/// private to the crate; not exported to wasm-bindgen.
#[allow(dead_code)]
pub(crate) fn envelope_from_parts(
    request_id: &str,
    route_key: &str,
    payload: Vec<u8>,
    timeout_ms: i64,
) -> RpcEnvelope {
    RpcEnvelope {
        request_id: request_id.to_string(),
        route_key: route_key.to_string(),
        payload: Some(Bytes::from(payload)),
        error: None,
        direction: Some(Direction::Request as i32),
        traceparent: None,
        tracestate: None,
        metadata: vec![],
        timeout_ms,
    }
}
