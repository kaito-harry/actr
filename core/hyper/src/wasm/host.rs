//! WasmHost — Wasmtime Component Model host engine.
//!
//! Drives wasm workloads packaged as Component Model components. A single
//! [`WasmHost`] compiles a component once; Hyper can then derive multiple
//! internal runtime instances from that compilation, one per logical actor.
//!
//! # Contract
//!
//! The guest component must implement the `actr:workload@0.1.0`
//! `actr-workload-guest` world defined in
//! `core/framework/wit/actr-workload.wit`. That contract carries one
//! `dispatch(envelope)` export for inbound RPC plus sixteen observation
//! hooks (lifecycle + signaling + transport + credential + mailbox),
//! exactly mirroring [`actr_framework::Workload`].
//!
//! Host imports (`call`, `tell`, `call-raw`, `discover`, `log-message`)
//! are serviced via the caller-supplied [`HostAbiFn`] bridge threaded
//! into the WASM [`Store`].
//!
//! # Async model
//!
//! Every host import is an `async fn` on the generated Rust trait
//! (Component Model async binding mode). Chaining `Component::instantiate_async`
//! with `call_dispatch(&mut store, env).await` lets the guest `.await`
//! host imports through the Component Model async ABI.
//!
//! Same-instance single-threadedness is compile-time-enforced: each
//! dispatch takes `&mut Store<HostState>`, so the Rust borrow checker
//! forbids concurrent hooks on the same actor. Cross-instance
//! parallelism still works because each instantiated runtime owns its own
//! `Store`.

use std::collections::HashMap;
use std::sync::Arc;

use actr_framework::guest::dynclib_abi::InitPayloadV1;
use actr_framework::{BackpressureEvent, CredentialEvent, PeerEvent, WebRtcPeerStatus};
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{
    ActrError, ActrId, ActrType, ConnectionNotReadyInfo, DataChunk, MetadataEntry, PayloadType,
    Realm, RpcEnvelope,
};
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, OptLevel, RegallocAlgorithm, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::config::WasmRuntimeLimits;

use super::host_v2::WasmWorkloadV2;
use super::runtime_limits::{
    EpochTicker, StorePermit, StoreResourceLimiter, acquire_compile, acquire_instantiate,
    acquire_invocation, acquire_store, record_compile_failure, record_instantiate_failure,
    record_resource_denial, record_timeout,
};

use super::component_bindings::ActrWorkloadGuest;
use super::component_bindings::actr::workload::host::Host as HostImports;
use super::component_bindings::actr::workload::types::Host as TypesHost;
use super::component_bindings::actr::workload::types::{
    self as wit_types, ActrError as WitActrError, ActrId as WitActrId, ActrType as WitActrType,
    BackpressureEvent as WitBackpressureEvent, CredentialEvent as WitCredentialEvent,
    DataChunk as WitDataChunk, Dest as WitDest, PayloadType as WitPayloadType,
    PeerEvent as WitPeerEvent, Realm as WitRealm, RpcEnvelope as WitRpcEnvelope,
    WebrtcPeerStatus as WitWebrtcPeerStatus,
};
use crate::wasm::error::{WasmError, WasmResult};
use crate::workload::{
    HostAbiFn, HostOperation, HostOperationResult, InvocationContext, PackageHookEvent,
};

use actr_framework::guest::dynclib_abi as guest_abi;
use actr_framework::guest::dynclib_abi::{
    HostCallRawV1, HostCallV1, HostDiscoverV1, HostRegisterStreamV1, HostSendDataChunkV1,
    HostTellV1, HostUnregisterStreamV1,
};

// ─────────────────────────────────────────────────────────────────────────────
// Engine configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Build a wasmtime [`Config`] enabling the Component Model async path.
///
/// Guests are lifted synchronously since M3 (wit-bindgen 0.58 without
/// `async: true`), so `wasm_component_model_async(true)` is no longer needed
/// to validate guest custom sections. On wasmtime 46 the component-model
/// async validation is on by default anyway; we still set it explicitly to
/// pin behaviour against upstream default drift and as forward wiring for the
/// M4 async world. Host-side asynchrony (fiber suspension across host calls)
/// comes from the `async` Cargo feature + `call_async`, independent of this.
///
/// We additionally pin `concurrency_support(true)` — the wasmtime 46 gate
/// (`Config::concurrency_support`, `config.rs`: "This option defaults to
/// `true`") that governs the component-model concurrency surface. When it is
/// off, `Store::run_concurrent` panics. It is on by default in 46, but the M5
/// concurrent runner depends on it, so we set it explicitly to be self-
/// documenting and to guard against an upstream default flip. Enabling it
/// alongside `wasm_component_model_async(true)` is the supported combination.
fn build_engine(limits: &WasmRuntimeLimits) -> WasmResult<Engine> {
    let mut config = Config::new();
    // `async_support(true)` was required before wasmtime 43; since then
    // the `async` Cargo feature alone enables async at the engine level.
    // We pair it with explicit component-model flags to be self-documenting.
    config.wasm_component_model(true);
    config.wasm_component_model_async(true);
    // Explicitly gate the concurrency surface (`run_concurrent`) on. Defaults
    // to true on wasmtime 46; pinned here so a future default flip can never
    // silently turn the M5 concurrent runner into a runtime panic.
    config.concurrency_support(true);
    // issue #346: fuel + epoch preempt non-yielding compute (a pure-compute
    // infinite loop never awaits a host import, so DispatchConcurrency's
    // dispatch_timeout cannot interrupt it — fuel/epoch insert check points
    // into the compiled wasm). Stack caps guard against stack-overflow
    // recursion. `fuel_async_yield_interval = None` traps on exhaustion; the
    // instance is rebuilt on the next entry (`WasmWorkload::ensure_instance`).
    config.consume_fuel(true);
    config.epoch_interruption(true);
    config.max_wasm_stack(limits.max_wasm_stack);
    config.async_stack_size(limits.async_stack_size);
    // NOTE: `fuel_async_yield_interval` is a `Store` method in wasmtime 46
    // (not `Config`); set per-store in `instantiate_parts` / `instantiate_parts_v2`.
    if std::env::var_os("ACTR_WASM_FAST_COMPILE").is_some() {
        config.cranelift_opt_level(OptLevel::None);
        config.cranelift_regalloc_algorithm(RegallocAlgorithm::SinglePass);
    }
    Engine::new(&config)
        .map_err(|e| WasmError::LoadFailed(format!("wasmtime engine construction failed: {e}")))
}

// ─────────────────────────────────────────────────────────────────────────────
// HostState — per-Store runtime state
// ─────────────────────────────────────────────────────────────────────────────

/// Per-instance host state threaded through the wasmtime [`Store`].
///
/// Holds:
/// - the WASI p2 context + resource table (required for any component
///   that transitively imports WASI interfaces);
/// - an optional [`InvocationContext`] carrying self_id / caller_id /
///   request_id for the currently-active dispatch (set before
///   `call_dispatch` and cleared after);
/// - an optional [`HostAbiFn`] bridge servicing guest->host operations;
///   forwards to the host runtime's outbound RPC executor (registered
///   by Hyper's internal workload dispatch path).
pub(crate) struct HostState {
    wasi: WasiCtx,
    table: ResourceTable,
    pub(crate) invocation: Option<InvocationContext>,
    pub(crate) host_abi: Option<HostAbiFn>,
    /// Per-invocation table keyed by `ctx-token`, used by the V2 (0.2.0
    /// async world) Accessor-based host imports. Under the serial M4 runner
    /// this holds at most one live entry, but keeping it a map means the M5
    /// concurrent runner needs zero changes here: each in-flight invocation
    /// keys its own `HostAbiFn` / `InvocationContext` by its token, so the
    /// static Accessor import methods recover the correct one without a
    /// shared single slot that would cross-talk.
    ///
    /// The V1 (0.1.0 sync world) path never touches this map — it keeps
    /// using the `invocation` / `host_abi` single slots above.
    invocations: HashMap<u64, InvocationEntry>,
    /// V2 stream callbacks are registered during one invocation but execute
    /// later, after that invocation's token has been retired. Guest callbacks
    /// commonly capture the registering `WasmContext`, so remember which token
    /// that context carries for each stream. While a DataChunk callback is
    /// active, `stream_token_aliases` temporarily resolves the captured token
    /// to the callback invocation's live token/HostAbiFn.
    stream_context_tokens: HashMap<String, u64>,
    stream_token_aliases: HashMap<u64, u64>,
    /// Monotonic token allocator. Reset to zero whenever the map is cleared
    /// on a trap-poison so a rebuilt instance starts from a clean sheet.
    next_token: u64,
    /// issue #346: per-store resource limits (memory/table/instance caps).
    /// Installed via `Store::limiter` so `memory.grow`/`table.grow` over the
    /// bound trap (or return an error, per `trap_on_grow_failure`).
    pub(crate) limits: StoreResourceLimiter,
}

/// One live invocation's host-facing state, keyed by `ctx-token` in
/// [`HostState::invocations`].
pub(crate) struct InvocationEntry {
    #[allow(dead_code)]
    pub(crate) ctx: InvocationContext,
    pub(crate) host_abi: HostAbiFn,
}

impl std::fmt::Debug for HostState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostState")
            .field("invocation", &self.invocation)
            .field("host_abi", &self.host_abi.as_ref().map(|_| "<fn>"))
            .field("invocations", &self.invocations.len())
            .field("stream_context_tokens", &self.stream_context_tokens.len())
            .field("stream_token_aliases", &self.stream_token_aliases.len())
            .field("next_token", &self.next_token)
            .finish_non_exhaustive()
    }
}

impl HostState {
    pub(crate) fn new(limits: &WasmRuntimeLimits) -> Self {
        Self {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            table: ResourceTable::new(),
            invocation: None,
            host_abi: None,
            invocations: HashMap::new(),
            stream_context_tokens: HashMap::new(),
            stream_token_aliases: HashMap::new(),
            next_token: 0,
            limits: StoreResourceLimiter::new(limits),
        }
    }

    /// Allocate a fresh `ctx-token` and register a live invocation's
    /// `InvocationContext` + `HostAbiFn` under it. Returns the token to
    /// thread into the guest's `invocation-ctx` (V2 world). Tokens are
    /// monotonic within a Store's lifetime.
    pub(crate) fn alloc_invocation(&mut self, ctx: InvocationContext, host_abi: HostAbiFn) -> u64 {
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1);
        self.invocations
            .insert(token, InvocationEntry { ctx, host_abi });
        token
    }

    /// Clone the `HostAbiFn` registered for `token`, if any. `HostAbiFn` is
    /// an `Arc`, so the clone is a refcount bump safe to carry across an
    /// `.await` inside an Accessor host method (the store borrow is not
    /// held across the await).
    pub(crate) fn invocation_host_abi(&self, token: u64) -> Option<HostAbiFn> {
        let token = self
            .stream_token_aliases
            .get(&token)
            .copied()
            .unwrap_or(token);
        self.invocations
            .get(&token)
            .map(|e| Arc::clone(&e.host_abi))
    }

    /// Associate a successfully registered stream with the token embedded in
    /// the guest callback's captured `WasmContext`.
    pub(crate) fn register_stream_context(&mut self, stream_id: String, token: u64) {
        self.stream_context_tokens.insert(stream_id, token);
    }

    /// Forget the captured context associated with an unregistered stream.
    pub(crate) fn unregister_stream_context(&mut self, stream_id: &str) {
        self.stream_context_tokens.remove(stream_id);
    }

    /// Temporarily route a registered callback's captured token through the
    /// currently-active DataChunk invocation. DataChunk commands are runner
    /// barriers, so at most one alias for a stream callback is active.
    pub(crate) fn begin_stream_callback(
        &mut self,
        stream_id: &str,
        invocation_token: u64,
    ) -> Option<u64> {
        let captured_token = self.stream_context_tokens.get(stream_id).copied()?;
        if captured_token != invocation_token {
            self.stream_token_aliases
                .insert(captured_token, invocation_token);
        }
        Some(captured_token)
    }

    /// Remove the temporary alias installed by [`Self::begin_stream_callback`].
    pub(crate) fn end_stream_callback(
        &mut self,
        captured_token: Option<u64>,
        invocation_token: u64,
    ) {
        let Some(captured_token) = captured_token else {
            return;
        };
        if captured_token != invocation_token
            && self.stream_token_aliases.get(&captured_token) == Some(&invocation_token)
        {
            self.stream_token_aliases.remove(&captured_token);
        }
    }

    /// Retire the invocation registered for `token` once its guest call has
    /// completed (success or business error). No-op if already gone.
    pub(crate) fn remove_invocation(&mut self, token: u64) {
        self.invocations.remove(&token);
    }

    pub(crate) fn invocation_count(&self) -> usize {
        self.invocations.len()
    }

    /// Drop every live invocation and reset the token counter. Called when a
    /// trap poisons the store: the whole in-flight set is dead, so the
    /// rebuilt instance starts clean.
    pub(crate) fn clear_invocations(&mut self) {
        self.invocations.clear();
        self.stream_context_tokens.clear();
        self.stream_token_aliases.clear();
        self.next_token = 0;
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

// `types` is a types-only interface (no functions); bindgen still
// generates a marker `Host` trait that must be implemented by the host
// state. Empty impl satisfies the linker's expectations.
impl TypesHost for HostState {}

// ─────────────────────────────────────────────────────────────────────────────
// Generated `Host` trait implementation
// ─────────────────────────────────────────────────────────────────────────────

/// Forward a guest-initiated [`HostOperation`] through the installed
/// [`HostAbiFn`] bridge, translating the [`HostOperationResult`] into
/// the generated WIT return shape.
///
/// Returns `Ok(Ok(...))` on success, `Ok(Err(WitActrError::...))` on
/// guest-visible operation failure. The outer wasmtime::Result is
/// reserved for trap-level faults (missing bridge, ABI-error codes the
/// host cannot translate into `actr-error`).
fn forward_host_operation(
    state: &HostState,
    op: HostOperation,
) -> impl std::future::Future<Output = wasmtime::Result<Result<Vec<u8>, WitActrError>>> + Send + 'static
{
    let host_abi = state.host_abi.clone();
    async move {
        let Some(host_abi) = host_abi else {
            return Err(wasmtime::Error::msg(
                "host ABI bridge not installed for this dispatch",
            ));
        };
        match (host_abi)(op).await {
            HostOperationResult::Bytes(bytes) => Ok(Ok(bytes)),
            HostOperationResult::Done => Ok(Ok(Vec::new())),
            HostOperationResult::Error(code) => Ok(Err(actr_error_from_abi_code(code))),
        }
    }
}

impl HostImports for HostState {
    async fn call(
        &mut self,
        target: WitDest,
        route_key: String,
        payload: Vec<u8>,
    ) -> wasmtime::Result<Result<Vec<u8>, WitActrError>> {
        let op = HostOperation::Call(HostCallV1 {
            route_key,
            dest: wit_dest_to_v1(&target),
            payload,
        });
        forward_host_operation(self, op).await
    }

    async fn tell(
        &mut self,
        target: WitDest,
        route_key: String,
        payload: Vec<u8>,
    ) -> wasmtime::Result<Result<(), WitActrError>> {
        let op = HostOperation::Tell(HostTellV1 {
            route_key,
            dest: wit_dest_to_v1(&target),
            payload,
        });
        match forward_host_operation(self, op).await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    async fn call_raw(
        &mut self,
        target: WitActrId,
        route_key: String,
        payload: Vec<u8>,
    ) -> wasmtime::Result<Result<Vec<u8>, WitActrError>> {
        let op = HostOperation::CallRaw(HostCallRawV1 {
            route_key,
            target: wit_actr_id_to_proto(&target),
            payload,
        });
        forward_host_operation(self, op).await
    }

    async fn discover(
        &mut self,
        target_type: WitActrType,
    ) -> wasmtime::Result<Result<WitActrId, WitActrError>> {
        let op = HostOperation::Discover(HostDiscoverV1 {
            target_type: wit_actr_type_to_proto(&target_type),
        });
        match forward_host_operation(self, op).await? {
            Ok(bytes) => match ActrId::decode(bytes.as_slice()) {
                Ok(id) => Ok(Ok(proto_actr_id_to_wit(&id))),
                Err(e) => Ok(Err(WitActrError::DecodeFailure(format!(
                    "host discover returned undecodable ActrId: {e}"
                )))),
            },
            Err(e) => Ok(Err(e)),
        }
    }

    async fn register_stream(
        &mut self,
        stream_id: String,
    ) -> wasmtime::Result<Result<(), WitActrError>> {
        let op = HostOperation::RegisterStream(HostRegisterStreamV1 { stream_id });
        match forward_host_operation(self, op).await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    async fn unregister_stream(
        &mut self,
        stream_id: String,
    ) -> wasmtime::Result<Result<(), WitActrError>> {
        let op = HostOperation::UnregisterStream(HostUnregisterStreamV1 { stream_id });
        match forward_host_operation(self, op).await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    async fn send_data_chunk(
        &mut self,
        target: WitDest,
        chunk: WitDataChunk,
        payload_type: WitPayloadType,
    ) -> wasmtime::Result<Result<(), WitActrError>> {
        let op = HostOperation::SendDataChunk(HostSendDataChunkV1 {
            dest: wit_dest_to_v1(&target),
            chunk: wit_data_chunk_to_proto(chunk),
            payload_type: wit_payload_type_to_proto(payload_type) as i32,
        });
        match forward_host_operation(self, op).await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    async fn log_message(&mut self, level: String, message: String) -> wasmtime::Result<()> {
        match level.as_str() {
            "error" => tracing::error!(target: "wasm-guest", "{message}"),
            "warn" => tracing::warn!(target: "wasm-guest", "{message}"),
            "info" => tracing::info!(target: "wasm-guest", "{message}"),
            "debug" => tracing::debug!(target: "wasm-guest", "{message}"),
            "trace" => tracing::trace!(target: "wasm-guest", "{message}"),
            other => tracing::info!(target: "wasm-guest", level = %other, "{message}"),
        }
        Ok(())
    }

    /// Return the current dispatch's `self-id`.
    ///
    /// Traps when called outside of an active dispatch: the guest is
    /// consulting per-call context that the host has not installed, which
    /// almost certainly means the workload accessed `ctx` from a constructor
    /// or a lifecycle hook that should not thread invocation context.
    async fn get_self_id(&mut self) -> wasmtime::Result<WitActrId> {
        let ctx = self.invocation.as_ref().ok_or_else(|| {
            wasmtime::Error::msg(
                "get_self_id called outside of an active dispatch (no invocation context installed)",
            )
        })?;
        Ok(proto_actr_id_to_wit(&ctx.self_id))
    }

    async fn get_caller_id(&mut self) -> wasmtime::Result<Option<WitActrId>> {
        let ctx = self.invocation.as_ref().ok_or_else(|| {
            wasmtime::Error::msg(
                "get_caller_id called outside of an active dispatch (no invocation context installed)",
            )
        })?;
        Ok(ctx.caller_id.as_ref().map(proto_actr_id_to_wit))
    }

    async fn get_request_id(&mut self) -> wasmtime::Result<String> {
        let ctx = self.invocation.as_ref().ok_or_else(|| {
            wasmtime::Error::msg(
                "get_request_id called outside of an active dispatch (no invocation context installed)",
            )
        })?;
        Ok(ctx.request_id.clone())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WIT ↔ actr_protocol / actr_framework translation
// ─────────────────────────────────────────────────────────────────────────────

fn wit_realm_to_proto(r: &WitRealm) -> Realm {
    Realm {
        realm_id: r.realm_id,
    }
}

fn proto_realm_to_wit(r: &Realm) -> WitRealm {
    WitRealm {
        realm_id: r.realm_id,
    }
}

fn wit_actr_type_to_proto(t: &WitActrType) -> ActrType {
    ActrType {
        manufacturer: t.manufacturer.clone(),
        name: t.name.clone(),
        version: t.version.clone(),
    }
}

fn proto_actr_type_to_wit(t: &ActrType) -> WitActrType {
    WitActrType {
        manufacturer: t.manufacturer.clone(),
        name: t.name.clone(),
        version: t.version.clone(),
    }
}

fn wit_actr_id_to_proto(id: &WitActrId) -> ActrId {
    ActrId {
        realm: wit_realm_to_proto(&id.realm),
        serial_number: id.serial_number,
        r#type: wit_actr_type_to_proto(&id.type_),
    }
}

fn proto_actr_id_to_wit(id: &ActrId) -> WitActrId {
    WitActrId {
        realm: proto_realm_to_wit(&id.realm),
        serial_number: id.serial_number,
        type_: proto_actr_type_to_wit(&id.r#type),
    }
}

fn connection_not_ready_info_to_wit(
    info: &ConnectionNotReadyInfo,
) -> wit_types::ConnectionNotReadyInfo {
    wit_types::ConnectionNotReadyInfo {
        retry_after_ms: info.retry_after_ms,
    }
}

fn wit_connection_not_ready_info_to_proto(
    info: wit_types::ConnectionNotReadyInfo,
) -> ConnectionNotReadyInfo {
    ConnectionNotReadyInfo {
        retry_after_ms: info.retry_after_ms,
    }
}

fn wit_dest_to_v1(dest: &WitDest) -> guest_abi::DestV1 {
    match dest {
        WitDest::Host => guest_abi::DestV1::host(),
        WitDest::Workload => guest_abi::DestV1::workload(),
        WitDest::Peer(id) => guest_abi::DestV1::peer(wit_actr_id_to_proto(id)),
    }
}

fn actr_error_from_abi_code(code: i32) -> WitActrError {
    match code {
        guest_abi::code::GENERIC_ERROR => WitActrError::Internal("generic ABI error".into()),
        guest_abi::code::INIT_FAILED => WitActrError::Internal("init failed".into()),
        guest_abi::code::HANDLE_FAILED => WitActrError::Internal("handle failed".into()),
        guest_abi::code::ALLOC_FAILED => WitActrError::Internal("allocation failed".into()),
        guest_abi::code::PROTOCOL_ERROR => WitActrError::DecodeFailure("protocol error".into()),
        guest_abi::code::BUFFER_TOO_SMALL => {
            WitActrError::Internal("reply buffer too small".into())
        }
        guest_abi::code::UNSUPPORTED_OP => {
            WitActrError::NotImplemented("unsupported ABI operation".into())
        }
        other => WitActrError::Internal(format!("ABI status {other}")),
    }
}

#[allow(dead_code)]
fn actr_error_to_wit(e: &ActrError) -> WitActrError {
    match e {
        ActrError::Unavailable(msg) => WitActrError::Unavailable(msg.clone()),
        ActrError::ConnectionNotReady(info) => {
            WitActrError::ConnectionNotReady(connection_not_ready_info_to_wit(info))
        }
        ActrError::TimedOut => WitActrError::TimedOut,
        ActrError::NotFound(msg) => WitActrError::NotFound(msg.clone()),
        ActrError::PermissionDenied(msg) => WitActrError::PermissionDenied(msg.clone()),
        ActrError::InvalidArgument(msg) => WitActrError::InvalidArgument(msg.clone()),
        ActrError::UnknownRoute(msg) => WitActrError::UnknownRoute(msg.clone()),
        ActrError::DependencyNotFound {
            service_name,
            message,
        } => WitActrError::DependencyNotFound(wit_types::DependencyNotFoundPayload {
            service_name: service_name.clone(),
            message: message.clone(),
        }),
        ActrError::DecodeFailure(msg) => WitActrError::DecodeFailure(msg.clone()),
        ActrError::NotImplemented(msg) => WitActrError::NotImplemented(msg.clone()),
        ActrError::Internal(msg) => WitActrError::Internal(msg.clone()),
    }
}

fn wit_actr_error_to_proto(e: WitActrError) -> ActrError {
    match e {
        WitActrError::Unavailable(msg) => ActrError::Unavailable(msg),
        WitActrError::ConnectionNotReady(info) => {
            ActrError::ConnectionNotReady(wit_connection_not_ready_info_to_proto(info))
        }
        WitActrError::TimedOut => ActrError::TimedOut,
        WitActrError::NotFound(msg) => ActrError::NotFound(msg),
        WitActrError::PermissionDenied(msg) => ActrError::PermissionDenied(msg),
        WitActrError::InvalidArgument(msg) => ActrError::InvalidArgument(msg),
        WitActrError::UnknownRoute(msg) => ActrError::UnknownRoute(msg),
        WitActrError::DependencyNotFound(p) => ActrError::DependencyNotFound {
            service_name: p.service_name,
            message: p.message,
        },
        WitActrError::DecodeFailure(msg) => ActrError::DecodeFailure(msg),
        WitActrError::NotImplemented(msg) => ActrError::NotImplemented(msg),
        WitActrError::Internal(msg) => ActrError::Internal(msg),
    }
}

fn rpc_envelope_to_wit(envelope: &RpcEnvelope) -> WitRpcEnvelope {
    WitRpcEnvelope {
        request_id: envelope.request_id.clone(),
        route_key: envelope.route_key.clone(),
        payload: envelope
            .payload
            .as_ref()
            .map(|b| b.to_vec())
            .unwrap_or_default(),
    }
}

fn proto_data_chunk_to_wit(chunk: DataChunk) -> WitDataChunk {
    WitDataChunk {
        stream_id: chunk.stream_id,
        sequence: chunk.sequence,
        payload: chunk.payload.to_vec(),
        metadata: chunk
            .metadata
            .into_iter()
            .map(|entry| wit_types::MetadataEntry {
                key: entry.key,
                value: entry.value,
            })
            .collect(),
        timestamp_ms: chunk.timestamp_ms,
    }
}

fn proto_peer_event_to_wit(event: PeerEvent) -> WitPeerEvent {
    WitPeerEvent {
        peer: proto_actr_id_to_wit(&event.peer),
        relayed: event.relayed,
        status: event.status.map(proto_webrtc_peer_status_to_wit),
    }
}

fn proto_webrtc_peer_status_to_wit(status: WebRtcPeerStatus) -> WitWebrtcPeerStatus {
    match status {
        WebRtcPeerStatus::Idle => WitWebrtcPeerStatus::Idle,
        WebRtcPeerStatus::Connecting => WitWebrtcPeerStatus::Connecting,
        WebRtcPeerStatus::Connected => WitWebrtcPeerStatus::Connected,
        WebRtcPeerStatus::Recovering => WitWebrtcPeerStatus::Recovering,
    }
}

fn system_time_to_wit(time: std::time::SystemTime) -> wit_types::Timestamp {
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    wit_types::Timestamp {
        seconds: duration.as_secs(),
        nanoseconds: duration.subsec_nanos(),
    }
}

fn proto_credential_event_to_wit(event: CredentialEvent) -> WitCredentialEvent {
    WitCredentialEvent {
        new_expiry: system_time_to_wit(event.new_expiry),
    }
}

fn proto_backpressure_event_to_wit(event: BackpressureEvent) -> WitBackpressureEvent {
    WitBackpressureEvent {
        queue_len: event.queue_len as u64,
        threshold: event.threshold as u64,
    }
}

fn wit_data_chunk_to_proto(chunk: WitDataChunk) -> DataChunk {
    DataChunk {
        stream_id: chunk.stream_id,
        sequence: chunk.sequence,
        payload: chunk.payload.into(),
        metadata: chunk
            .metadata
            .into_iter()
            .map(|entry| MetadataEntry {
                key: entry.key,
                value: entry.value,
            })
            .collect(),
        timestamp_ms: chunk.timestamp_ms,
    }
}

fn wit_payload_type_to_proto(payload_type: WitPayloadType) -> PayloadType {
    match payload_type {
        WitPayloadType::RpcReliable => PayloadType::RpcReliable,
        WitPayloadType::RpcSignal => PayloadType::RpcSignal,
        WitPayloadType::StreamReliable => PayloadType::StreamReliable,
        WitPayloadType::StreamLatencyFirst => PayloadType::StreamLatencyFirst,
        WitPayloadType::MediaRtp => PayloadType::MediaRtp,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WasmHost
// ─────────────────────────────────────────────────────────────────────────────

/// Compiled wasm Component engine.
///
/// One `WasmHost` corresponds to one compiled component. Hyper uses it
/// internally to instantiate one runtime workload per actor instance.
pub struct WasmHost {
    engine: Engine,
    component: Component,
    limits: WasmRuntimeLimits,
    epoch_ticker: Arc<EpochTicker>,
}

impl std::fmt::Debug for WasmHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmHost").finish_non_exhaustive()
    }
}

impl WasmHost {
    /// Compile a Component from raw bytes.
    ///
    /// CPU-intensive; callers should run this on a blocking task.
    /// Errors include non-Component inputs (e.g. a legacy core wasm
    /// module saved in a pre-Phase-1 `.actr` package) — callers get a
    /// clear `LoadFailed` in that case and should surface the migration
    /// guidance up to the `.actr` loader.
    ///
    /// A second class of legacy input is a package built by an old SDK
    /// (wit-bindgen <= 0.57 with the async-lift ABI). wasmtime 46 rejects
    /// the `async` canonical option on a synchronous WIT function type, so
    /// those binaries fail here; we map that to an actionable rebuild hint.
    pub fn compile(wasm_bytes: &[u8]) -> WasmResult<Self> {
        Self::compile_with_limits(wasm_bytes, &WasmRuntimeLimits::default())
    }

    /// Compile with explicit resource limits (issue #346). Production callers
    /// thread the configured [`WasmRuntimeLimits`]; tests use [`compile`].
    pub fn compile_with_limits(wasm_bytes: &[u8], limits: &WasmRuntimeLimits) -> WasmResult<Self> {
        limits.validate().map_err(WasmError::VerificationFailed)?;
        if wasm_bytes.len() > limits.max_component_bytes {
            record_resource_denial();
            return Err(WasmError::ResourceLimitExceeded("component byte size"));
        }
        let _compile_permit = acquire_compile(limits)?;
        let engine = build_engine(limits)?;
        let epoch_ticker = EpochTicker::spawn(&engine, limits.epoch_tick)?;
        let component = Component::from_binary(&engine, wasm_bytes).map_err(|e| {
            record_compile_failure();
            let raw = format!("{e:#}");
            if raw.contains("`async` canonical option requires an async function type") {
                return WasmError::LoadFailed(format!(
                    "this .actr package was built by an old SDK (wit-bindgen \
                     <= 0.57 async-lift ABI), which wasmtime 46 rejects per \
                     the Component Model spec. Rebuild it with the current SDK \
                     (synchronous-lift packages run on both new and old hosts). \
                     wasmtime reported: {raw}"
                ));
            }
            WasmError::LoadFailed(format!(
                "wasm bytes did not load as a Component (this host \
                 requires Component Model binaries as of .actr format \
                 bump; wasmtime reported: {raw})"
            ))
        })?;
        tracing::info!(wasm_bytes = wasm_bytes.len(), "wasm Component compiled");
        Ok(Self {
            engine,
            component,
            limits: *limits,
            epoch_ticker,
        })
    }

    /// Instantiate the component into a runnable internal workload.
    ///
    /// Probes which `actr:workload` world the component implements
    /// (0.1.0 sync → [`WasmKernel::V1`], 0.2.0 async → [`WasmKernel::V2`])
    /// and instantiates it on the matching execution path. Old 0.1.0
    /// sync-lift packages keep taking the serial path unchanged; 0.2.0
    /// packages run through the `run_concurrent` async path.
    ///
    /// Builds a fresh [`Linker`] per instance (cheap), registers WASI
    /// p2 as well as the generated `actr:workload/host` linker, and
    /// runs `Component::instantiate_async`.
    pub(crate) async fn instantiate(&self) -> WasmResult<WasmKernel> {
        let store_permit = acquire_store(&self.limits)?;
        match probe_world(&self.component, &self.engine)? {
            WasmWorkloadKind::V1Serial => {
                let (store, bindings) =
                    instantiate_parts(&self.engine, &self.component, &self.limits).await?;
                tracing::info!("wasm Component instantiated (v1 serial world)");
                Ok(WasmKernel::V1(WasmWorkload {
                    engine: self.engine.clone(),
                    component: self.component.clone(),
                    limits: self.limits,
                    store: Some(store),
                    bindings,
                    _epoch_ticker: Arc::clone(&self.epoch_ticker),
                    _store_permit: store_permit,
                    poisoned: false,
                    rebuilds: 0,
                }))
            }
            WasmWorkloadKind::V2Concurrent => {
                let v2 = WasmWorkloadV2::instantiate(
                    &self.engine,
                    &self.component,
                    &self.limits,
                    Arc::clone(&self.epoch_ticker),
                    store_permit,
                )
                .await?;
                Ok(WasmKernel::V2(v2))
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// World probing + dual-world kernel
// ─────────────────────────────────────────────────────────────────────────────

/// Which `actr:workload` world a compiled component implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WasmWorkloadKind {
    /// `actr:workload@0.1.0` — synchronous world, serial `&mut Store` path.
    V1Serial,
    /// `actr:workload@0.2.0` — async world, `run_concurrent` path.
    V2Concurrent,
}

/// Inspect a compiled [`Component`]'s exported instances and decide which
/// workload world it implements, purely from the component type — never
/// reads or writes the `.actr` manifest, so the decision is grounded in
/// what the binary actually exports. Exactly one recognised `workload`
/// world must be present; zero or both is a [`WasmError::LoadFailed`] with
/// a rebuild hint.
fn probe_world(component: &Component, engine: &Engine) -> WasmResult<WasmWorkloadKind> {
    let mut saw_v1 = false;
    let mut saw_v2 = false;
    for (name, _item) in component.component_type().exports(engine) {
        if name.starts_with("actr:workload/workload@0.2.0") {
            saw_v2 = true;
        } else if name.starts_with("actr:workload/workload@0.1.0") {
            saw_v1 = true;
        }
    }
    match (saw_v1, saw_v2) {
        (true, false) => Ok(WasmWorkloadKind::V1Serial),
        (false, true) => Ok(WasmWorkloadKind::V2Concurrent),
        (false, false) => Err(WasmError::LoadFailed(
            "component exports no recognised actr:workload/workload world \
             (expected @0.1.0 or @0.2.0); rebuild the package with a current SDK"
                .to_string(),
        )),
        (true, true) => Err(WasmError::LoadFailed(
            "component exports both actr:workload/workload@0.1.0 and @0.2.0; \
             a package must implement exactly one world"
                .to_string(),
        )),
    }
}

/// Dual-world execution kernel wrapped by [`crate::workload::Workload::Wasm`].
///
/// A thin dispatcher that forwards each guest entry to whichever world the
/// loaded component implements. The `Workload` enum and its callers see one
/// uniform `&mut self` surface; the V1/V2 split lives entirely here.
pub(crate) enum WasmKernel {
    V1(WasmWorkload),
    V2(WasmWorkloadV2),
}

impl std::fmt::Debug for WasmKernel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WasmKernel::V1(w) => f.debug_tuple("WasmKernel::V1").field(w).finish(),
            WasmKernel::V2(w) => f.debug_tuple("WasmKernel::V2").field(w).finish(),
        }
    }
}

impl WasmKernel {
    pub(crate) fn init(&mut self, init_payload: &InitPayloadV1) -> WasmResult<()> {
        match self {
            WasmKernel::V1(w) => w.init(init_payload),
            WasmKernel::V2(w) => w.init(init_payload),
        }
    }

    #[cfg(feature = "test-utils")]
    pub(crate) fn rebuild_count(&self) -> u64 {
        match self {
            WasmKernel::V1(w) => w.rebuild_count(),
            WasmKernel::V2(w) => w.rebuild_count(),
        }
    }

    /// Whether this kernel is the 0.2.0 async world (the only kernel that can
    /// drive a resident `run_concurrent` region for same-instance interleaved
    /// concurrency). The interleaved runner arm in [`crate::executor`] routes
    /// on this: a V2 kernel gets `run_interleaved`; a V1 kernel (0.1.0 sync
    /// world) always falls back to the serial `run_loop`.
    pub(crate) fn is_v2(&self) -> bool {
        matches!(self, WasmKernel::V2(_))
    }

    pub(crate) async fn call_on_start(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        match self {
            WasmKernel::V1(w) => w.call_on_start(ctx, host_abi).await,
            WasmKernel::V2(w) => w.call_on_start(ctx, host_abi).await,
        }
    }

    pub(crate) async fn call_on_ready(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        match self {
            WasmKernel::V1(w) => w.call_on_ready(ctx, host_abi).await,
            WasmKernel::V2(w) => w.call_on_ready(ctx, host_abi).await,
        }
    }

    pub(crate) async fn call_on_stop(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        match self {
            WasmKernel::V1(w) => w.call_on_stop(ctx, host_abi).await,
            WasmKernel::V2(w) => w.call_on_stop(ctx, host_abi).await,
        }
    }

    pub(crate) async fn call_hook_event(
        &mut self,
        event: PackageHookEvent,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        match self {
            WasmKernel::V1(w) => w.call_hook_event(event, ctx, host_abi).await,
            WasmKernel::V2(w) => w.call_hook_event(event, ctx, host_abi).await,
        }
    }

    pub(crate) async fn handle(
        &mut self,
        request_bytes: &[u8],
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<Vec<u8>> {
        match self {
            WasmKernel::V1(w) => w.handle(request_bytes, ctx, host_abi).await,
            WasmKernel::V2(w) => w.handle(request_bytes, ctx, host_abi).await,
        }
    }

    pub(crate) async fn handle_data_chunk(
        &mut self,
        chunk: DataChunk,
        sender: ActrId,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        match self {
            WasmKernel::V1(w) => w.handle_data_chunk(chunk, sender, ctx, host_abi).await,
            WasmKernel::V2(w) => w.handle_data_chunk(chunk, sender, ctx, host_abi).await,
        }
    }
}

/// Build a fresh [`Store`] + component instance from an already-compiled
/// [`Engine`]/[`Component`] pair.
///
/// Shared by [`WasmHost::instantiate`] (first instantiation) and
/// [`WasmWorkload::ensure_instance`] (rebuild after a guest trap poisons
/// the store). Only re-runs `instantiate_async`; the component compilation
/// is not repeated — both `Engine` and `Component` are `Arc`-backed and
/// cheap to clone. A fresh [`Linker`] is built each time (cheap) so the
/// WASI p2 and `actr:workload/host` imports are registered against the new
/// store.
/// Convert the invocation timeout into an epoch deadline (tick count).
/// Rounded up so the deadline is never shorter than the timeout. The epoch
/// interrupt is a backstop for fuel: it fires even if a tight loop burns no
/// fuel in a single Wasm op, provided a background ticker is advancing the
/// engine epoch (`Store::set_epoch_deadline` is relative to the engine epoch
/// at call time).
pub(crate) fn epoch_deadline_ticks(limits: &WasmRuntimeLimits) -> u64 {
    let tick_nanos = limits.epoch_tick.as_nanos().max(1);
    u64::try_from(limits.invocation_timeout.as_nanos() / tick_nanos)
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

async fn instantiate_parts(
    engine: &Engine,
    component: &Component,
    limits: &WasmRuntimeLimits,
) -> WasmResult<(Store<HostState>, ActrWorkloadGuest)> {
    let _instantiate_permit = acquire_instantiate(limits)?;
    let mut linker: Linker<HostState> = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(|e| {
        WasmError::LoadFailed(format!("failed to register WASI p2 linker imports: {e}"))
    })?;
    ActrWorkloadGuest::add_to_linker::<_, HasSelf<_>>(&mut linker, |s| s).map_err(|e| {
        WasmError::LoadFailed(format!(
            "failed to register actr:workload/host linker imports: {e}"
        ))
    })?;

    let mut store = Store::new(engine, HostState::new(limits));
    // issue #346: install the per-store resource limiter (memory/table/instance
    // caps) and seed fuel + epoch deadline so instantiation itself is bounded.
    store.limiter(|s| &mut s.limits);
    store
        .fuel_async_yield_interval(limits.fuel_async_yield_interval)
        .map_err(|e| WasmError::LoadFailed(format!("set fuel_async_yield_interval: {e}")))?;
    store
        .set_fuel(limits.fuel_per_invocation)
        .map_err(|e| WasmError::LoadFailed(format!("set fuel: {e}")))?;
    store.set_epoch_deadline(epoch_deadline_ticks(limits));
    let bindings = match tokio::time::timeout(
        limits.invocation_timeout,
        ActrWorkloadGuest::instantiate_async(&mut store, component, &linker),
    )
    .await
    {
        Ok(Ok(bindings)) => bindings,
        Ok(Err(error)) => {
            record_instantiate_failure();
            return Err(WasmError::LoadFailed(format!(
                "Component instantiate_async failed: {error:#}"
            )));
        }
        Err(_) => {
            record_instantiate_failure();
            record_timeout();
            return Err(WasmError::InvocationTimeout(limits.invocation_timeout));
        }
    };

    Ok((store, bindings))
}

// ─────────────────────────────────────────────────────────────────────────────
// WasmWorkload
// ─────────────────────────────────────────────────────────────────────────────

/// Single wasm actor instance driven through the Component Model.
///
/// Holds a [`Store<HostState>`] and cached generated bindings. `handle`
/// takes `&mut self`; wasmtime's `Store<T>` is not `Sync`, so the Rust
/// borrow checker rejects any caller attempting to drive two hooks on
/// the same instance concurrently — the single-threaded-actor invariant
/// is compile-time-enforced.
///
/// # Store poisoning and lazy rebuild
///
/// A guest trap (panic, unreachable, or a host-import bridge fault raised
/// through the trappable error path) poisons the underlying wasmtime
/// [`Store`] as of wasmtime v42+: any further guest entry on that store
/// fails with a "cannot enter component instance" style error. To keep an
/// actor serviceable after such a fault, every guest entry method
/// (`handle`, `handle_data_chunk`, the lifecycle hooks, `call_hook_event`)
/// first calls [`WasmWorkload::ensure_instance`]: if the store is poisoned
/// it is transparently rebuilt (fresh [`Store`] + `instantiate_async`) from
/// the retained [`Engine`]/[`Component`] before the call proceeds. The
/// rebuild only re-instantiates — no recompilation.
///
/// The rebuild is *lazy* (performed on the next inbound call, not at the
/// trap site) and only re-establishes a serviceable instance. It does
/// **not** replay `on-start`/lifecycle hooks and does **not** replay any
/// in-flight or queued message: the guest's in-memory linear-memory state
/// is lost (a `warn` is logged to make that explicit). Replay / hook
/// adjudication (rebuild vs discard vs escalate) is out of scope here and
/// belongs to the later mailbox/replay milestone (v2 plan §3.3 B0 boundary).
pub(crate) struct WasmWorkload {
    /// Retained for rebuilding a poisoned store. `Arc`-backed, cheap clone.
    engine: Engine,
    /// Retained for rebuilding a poisoned store. `Arc`-backed, cheap clone.
    component: Component,
    /// issue #346: resource limits retained to re-seed fuel/epoch/limiter on
    /// a post-trap rebuild (`ensure_instance`).
    limits: WasmRuntimeLimits,
    store: Option<Store<HostState>>,
    bindings: ActrWorkloadGuest,
    _epoch_ticker: Arc<EpochTicker>,
    _store_permit: StorePermit,
    /// Set when a guest entry trapped; the store is unusable until rebuilt.
    poisoned: bool,
    /// Count of successful rebuilds, for observability / tests.
    rebuilds: u64,
}

impl std::fmt::Debug for WasmWorkload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmWorkload")
            .field("poisoned", &self.poisoned)
            .field("rebuilds", &self.rebuilds)
            .finish_non_exhaustive()
    }
}

impl WasmWorkload {
    /// Legacy init entry — carried over from the pre-Component path so
    /// `core/hyper/src/lib.rs::load_wasm_workload_inner` still compiles
    /// unchanged. In the Component model, explicit init is implicit
    /// (the guest's static constructors run inside `instantiate_async`);
    /// a later commit will rewire the lifecycle so on-start / init
    /// payload flow through the WIT hooks directly.
    ///
    /// For now this simply logs the intent and returns Ok.
    pub(crate) fn init(&mut self, init_payload: &InitPayloadV1) -> WasmResult<()> {
        tracing::debug!(
            actr_type = %init_payload.actr_type,
            realm_id = init_payload.realm_id,
            "wasm Component workload init (Component-model lifecycle handles this implicitly)"
        );
        Ok(())
    }

    fn install_invocation(&mut self, ctx: InvocationContext, host_abi: &HostAbiFn) {
        let host_abi_clone: HostAbiFn = Arc::clone(host_abi);
        let state = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .data_mut();
        state.invocation = Some(ctx);
        state.host_abi = Some(host_abi_clone);
    }

    fn clear_invocation(&mut self) {
        let state = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .data_mut();
        state.invocation = None;
        state.host_abi = None;
    }

    /// Number of times the poisoned store has been rebuilt. Test/observability
    /// hook — monotonic, incremented once per successful re-instantiation.
    #[cfg(feature = "test-utils")]
    pub(crate) fn rebuild_count(&self) -> u64 {
        self.rebuilds
    }

    /// issue #346: re-seed per-entry fuel + epoch deadline (V1 serial path).
    fn reseed_fuel(&mut self) -> WasmResult<()> {
        self.store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .set_fuel(self.limits.fuel_per_invocation)
            .map_err(|e| WasmError::ExecutionFailed(format!("set fuel: {e}")))?;
        self.store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .set_epoch_deadline(epoch_deadline_ticks(&self.limits));
        Ok(())
    }

    /// Ensure the wasm store is usable before a guest entry.
    ///
    /// If the store is not poisoned this is a no-op. If a previous guest
    /// entry trapped (see [`WasmWorkload::trap_poison`]), the store is
    /// rebuilt from the retained [`Engine`]/[`Component`]: a fresh
    /// [`Store`] + `instantiate_async`, discarding the guest's prior
    /// in-memory state. On success the poison flag clears and the rebuild
    /// counter advances; on failure the instance stays poisoned and the
    /// next call retries, so a transient rebuild failure is not fatal.
    async fn ensure_instance(&mut self) -> WasmResult<()> {
        if !self.poisoned {
            return Ok(());
        }

        tracing::warn!(
            rebuild_attempt = self.rebuilds + 1,
            "rebuilding poisoned wasm store after a prior guest trap; \
             guest in-memory state is discarded (lifecycle/queue not replayed)"
        );

        // A poisoned Store can no longer execute guest code but still owns all
        // of its linear memories. Drop it before constructing the replacement,
        // so rebuilds never transiently hold two physical Stores. The owning
        // workload deliberately retains its process reservation throughout.
        drop(self.store.take());

        match instantiate_parts(&self.engine, &self.component, &self.limits).await {
            Ok((store, bindings)) => {
                self.store = Some(store);
                self.bindings = bindings;
                self.poisoned = false;
                self.rebuilds += 1;
                tracing::info!(
                    rebuilds = self.rebuilds,
                    "wasm store rebuilt; instance serviceable again"
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "failed to rebuild poisoned wasm store; instance stays poisoned, will retry on next call"
                );
                match e {
                    e @ WasmError::ResourceLimitExceeded(_) => Err(e),
                    e => Err(WasmError::LoadFailed(format!(
                        "failed to rebuild poisoned wasm store: {e}"
                    ))),
                }
            }
        }
    }

    /// Mark the store poisoned after a guest entry trapped.
    ///
    /// A trap (outer `wasmtime::Result` `Err`) is an instance-level fatal
    /// fault: the store cannot be re-entered. We flag it so the next guest
    /// entry rebuilds a fresh instance, and return a distinct
    /// [`WasmError::InstanceTrapped`] so callers/telemetry can tell a
    /// trap-level failure apart from a guest-visible business error
    /// (`ExecutionFailed`).
    fn trap_poison(&mut self, entry: &str, trap: wasmtime::Error) -> WasmError {
        self.poisoned = true;
        if let Some(store) = self.store.as_mut() {
            let state = store.data_mut();
            state.invocation = None;
            state.host_abi = None;
        }
        // A trapped Store is not serviceable. Discard it immediately so no
        // linear memory or latent guest task survives until the next entry.
        drop(self.store.take());
        tracing::error!(
            entry,
            error = %trap,
            "wasm guest trapped; store discarded (instance-level fatal). \
             In-memory guest state is lost; a fresh instance is rebuilt before the next call"
        );
        super::error::classify_trap(entry, trap)
    }

    fn timeout_poison(&mut self) -> WasmError {
        self.poisoned = true;
        if let Some(store) = self.store.as_mut() {
            let state = store.data_mut();
            state.invocation = None;
            state.host_abi = None;
        }
        drop(self.store.take());
        record_timeout();
        WasmError::InvocationTimeout(self.limits.invocation_timeout)
    }

    /// Invoke the workload's `on-start` lifecycle hook.
    ///
    /// Phase 1 exposes this as a distinct entry point so the host can
    /// pump the lifecycle after instantiation. Returns `Err` on a host
    /// trap or an `actr-error` variant from the guest.
    ///
    pub(crate) async fn call_on_start(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.ensure_instance().await?;
        let _invocation_permit = acquire_invocation(&self.limits)?;
        self.reseed_fuel()?;
        self.install_invocation(ctx, host_abi);

        let result = tokio::time::timeout(
            self.limits.invocation_timeout,
            self.bindings.actr_workload_workload().call_on_start(
                self.store
                    .as_mut()
                    .expect("serviceable wasm instance must have a Store"),
            ),
        )
        .await;

        let result = match result {
            Ok(result) => result,
            Err(_) => return Err(self.timeout_poison()),
        };

        self.clear_invocation();

        let inner = match result {
            Ok(inner) => inner,
            Err(trap) => return Err(self.trap_poison("on_start", trap)),
        };
        inner.map_err(|e| WasmError::ExecutionFailed(format!("on_start error: {:?}", e)))?;
        Ok(())
    }

    pub(crate) async fn call_on_ready(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.ensure_instance().await?;
        let _invocation_permit = acquire_invocation(&self.limits)?;
        self.reseed_fuel()?;
        self.install_invocation(ctx, host_abi);

        let result = tokio::time::timeout(
            self.limits.invocation_timeout,
            self.bindings.actr_workload_workload().call_on_ready(
                self.store
                    .as_mut()
                    .expect("serviceable wasm instance must have a Store"),
            ),
        )
        .await;

        let result = match result {
            Ok(result) => result,
            Err(_) => return Err(self.timeout_poison()),
        };

        self.clear_invocation();

        let inner = match result {
            Ok(inner) => inner,
            Err(trap) => return Err(self.trap_poison("on_ready", trap)),
        };
        inner.map_err(|e| WasmError::ExecutionFailed(format!("on_ready error: {:?}", e)))?;
        Ok(())
    }

    pub(crate) async fn call_on_stop(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.ensure_instance().await?;
        let _invocation_permit = acquire_invocation(&self.limits)?;
        self.reseed_fuel()?;
        self.install_invocation(ctx, host_abi);

        let result = tokio::time::timeout(
            self.limits.invocation_timeout,
            self.bindings.actr_workload_workload().call_on_stop(
                self.store
                    .as_mut()
                    .expect("serviceable wasm instance must have a Store"),
            ),
        )
        .await;

        let result = match result {
            Ok(result) => result,
            Err(_) => return Err(self.timeout_poison()),
        };

        self.clear_invocation();

        let inner = match result {
            Ok(inner) => inner,
            Err(trap) => return Err(self.trap_poison("on_stop", trap)),
        };
        inner.map_err(|e| WasmError::ExecutionFailed(format!("on_stop error: {:?}", e)))?;
        Ok(())
    }

    pub(crate) async fn call_hook_event(
        &mut self,
        event: PackageHookEvent,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.ensure_instance().await?;
        let _invocation_permit = acquire_invocation(&self.limits)?;
        let label = event.request_id();
        self.reseed_fuel()?;
        self.install_invocation(ctx, host_abi);
        let result = tokio::time::timeout(self.limits.invocation_timeout, async {
            match event {
                PackageHookEvent::SignalingConnecting => {
                    self.bindings
                        .actr_workload_workload()
                        .call_on_signaling_connecting(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                        )
                        .await
                }
                PackageHookEvent::SignalingConnected => {
                    self.bindings
                        .actr_workload_workload()
                        .call_on_signaling_connected(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                        )
                        .await
                }
                PackageHookEvent::SignalingDisconnected => {
                    self.bindings
                        .actr_workload_workload()
                        .call_on_signaling_disconnected(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                        )
                        .await
                }
                PackageHookEvent::WebSocketConnecting(event) => {
                    let event = proto_peer_event_to_wit(event);
                    self.bindings
                        .actr_workload_workload()
                        .call_on_websocket_connecting(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                            &event,
                        )
                        .await
                }
                PackageHookEvent::WebSocketConnected(event) => {
                    let event = proto_peer_event_to_wit(event);
                    self.bindings
                        .actr_workload_workload()
                        .call_on_websocket_connected(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                            &event,
                        )
                        .await
                }
                PackageHookEvent::WebSocketDisconnected(event) => {
                    let event = proto_peer_event_to_wit(event);
                    self.bindings
                        .actr_workload_workload()
                        .call_on_websocket_disconnected(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                            &event,
                        )
                        .await
                }
                PackageHookEvent::WebRtcConnecting(event) => {
                    let event = proto_peer_event_to_wit(event);
                    self.bindings
                        .actr_workload_workload()
                        .call_on_webrtc_connecting(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                            &event,
                        )
                        .await
                }
                PackageHookEvent::WebRtcConnected(event) => {
                    let event = proto_peer_event_to_wit(event);
                    self.bindings
                        .actr_workload_workload()
                        .call_on_webrtc_connected(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                            &event,
                        )
                        .await
                }
                PackageHookEvent::WebRtcDisconnected(event) => {
                    let event = proto_peer_event_to_wit(event);
                    self.bindings
                        .actr_workload_workload()
                        .call_on_webrtc_disconnected(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                            &event,
                        )
                        .await
                }
                PackageHookEvent::CredentialRenewed(event) => {
                    let event = proto_credential_event_to_wit(event);
                    self.bindings
                        .actr_workload_workload()
                        .call_on_credential_renewed(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                            event,
                        )
                        .await
                }
                PackageHookEvent::CredentialExpiring(event) => {
                    let event = proto_credential_event_to_wit(event);
                    self.bindings
                        .actr_workload_workload()
                        .call_on_credential_expiring(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                            event,
                        )
                        .await
                }
                PackageHookEvent::MailboxBackpressure(event) => {
                    let event = proto_backpressure_event_to_wit(event);
                    self.bindings
                        .actr_workload_workload()
                        .call_on_mailbox_backpressure(
                            self.store
                                .as_mut()
                                .expect("serviceable wasm instance must have a Store"),
                            event,
                        )
                        .await
                }
            }
        })
        .await;
        let result = match result {
            Ok(result) => result,
            Err(_) => return Err(self.timeout_poison()),
        };
        self.clear_invocation();

        if let Err(trap) = result {
            return Err(self.trap_poison(label, trap));
        }
        Ok(())
    }

    /// Handle one inbound RPC request.
    ///
    /// `request_bytes` is a prost-encoded [`RpcEnvelope`] produced by
    /// the hyper dispatcher. The envelope is decoded on the host side
    /// and passed as a typed Component Model record to the guest; the
    /// guest returns raw reply bytes or a structured `actr-error`.
    ///
    /// `ctx` + `host_abi` are threaded through the `Store<HostState>`
    /// for the duration of this call. The `host_abi` bridge services
    /// any guest-initiated `call` / `tell` / `call-raw` / `discover`
    /// operations during `.await` points inside the guest.
    pub(crate) async fn handle(
        &mut self,
        request_bytes: &[u8],
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<Vec<u8>> {
        // Rebuild the store first if a prior guest entry poisoned it. Done
        // before envelope decode so a decode failure (which never touches
        // the guest) does not gate on a rebuild it does not need.
        self.ensure_instance().await?;

        // Decode the envelope. If the caller handed us malformed bytes
        // we surface that as an ExecutionFailed; historically this case
        // could never happen in practice because the caller encodes the
        // envelope immediately before calling us, but defending against
        // it keeps the host side self-documenting.
        let envelope = RpcEnvelope::decode(request_bytes).map_err(|e| {
            WasmError::ExecutionFailed(format!(
                "host failed to decode RpcEnvelope before dispatch: {e}"
            ))
        })?;
        let _invocation_permit = acquire_invocation(&self.limits)?;

        // Thread per-call context into the Store. `HostAbiFn` is an
        // `Arc<...>` (Phase-1 type bump); cloning it is a refcount bump
        // with `'static` lifetime, safe to carry across the async
        // dispatch boundary. A fresh clone is installed per dispatch
        // so the bridge is dropped when the dispatch completes or
        // traps.
        self.reseed_fuel()?;
        self.install_invocation(ctx, host_abi);

        let wit_envelope = rpc_envelope_to_wit(&envelope);
        let dispatch_result = tokio::time::timeout(
            self.limits.invocation_timeout,
            self.bindings.actr_workload_workload().call_dispatch(
                self.store
                    .as_mut()
                    .expect("serviceable wasm instance must have a Store"),
                &wit_envelope,
            ),
        )
        .await;

        let dispatch_result = match dispatch_result {
            Ok(result) => result,
            Err(_) => return Err(self.timeout_poison()),
        };

        // Always clear per-call state regardless of outcome. On a trap the
        // Store is poisoned (flagged below via `trap_poison`) and rebuilt
        // lazily on the next call; clearing here keeps per-call state tidy
        // either way.
        self.clear_invocation();

        match dispatch_result {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(wit_err)) => Err(WasmError::ExecutionFailed(format!(
                "guest dispatch returned error: {:?}",
                wit_actr_error_to_proto(wit_err)
            ))),
            Err(trap) => Err(self.trap_poison("dispatch", trap)),
        }
    }

    pub(crate) async fn handle_data_chunk(
        &mut self,
        chunk: DataChunk,
        sender: ActrId,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.ensure_instance().await?;
        let _invocation_permit = acquire_invocation(&self.limits)?;
        self.reseed_fuel()?;
        self.install_invocation(ctx, host_abi);

        let wit_chunk = proto_data_chunk_to_wit(chunk);
        let wit_sender = proto_actr_id_to_wit(&sender);
        let result = tokio::time::timeout(
            self.limits.invocation_timeout,
            self.bindings.actr_workload_workload().call_on_data_chunk(
                self.store
                    .as_mut()
                    .expect("serviceable wasm instance must have a Store"),
                &wit_chunk,
                &wit_sender,
            ),
        )
        .await;
        let result = match result {
            Ok(result) => result,
            Err(_) => return Err(self.timeout_poison()),
        };
        self.clear_invocation();
        let inner = match result {
            Ok(inner) => inner,
            Err(trap) => return Err(self.trap_poison("on_data_chunk", trap)),
        };
        inner.map_err(|e| {
            WasmError::ExecutionFailed(format!(
                "on_data_chunk error: {:?}",
                wit_actr_error_to_proto(e)
            ))
        })?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
