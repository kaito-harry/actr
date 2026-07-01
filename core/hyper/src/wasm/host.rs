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

use std::sync::Arc;

use actr_framework::guest::dynclib_abi::InitPayloadV1;
use actr_framework::{BackpressureEvent, CredentialEvent, PeerEvent, WebRtcPeerStatus};
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{
    ActrError, ActrId, ActrType, ConnectionNotReadyInfo, DataStream, MetadataEntry, PayloadType,
    Realm, RpcEnvelope,
};
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, OptLevel, RegallocAlgorithm, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use super::component_bindings::ActrWorkloadGuest;
use super::component_bindings::actr::workload::host::Host as HostImports;
use super::component_bindings::actr::workload::types::Host as TypesHost;
use super::component_bindings::actr::workload::types::{
    self as wit_types, ActrError as WitActrError, ActrId as WitActrId, ActrType as WitActrType,
    BackpressureEvent as WitBackpressureEvent, CredentialEvent as WitCredentialEvent,
    DataStream as WitDataStream, Dest as WitDest, PayloadType as WitPayloadType,
    PeerEvent as WitPeerEvent, Realm as WitRealm, RpcEnvelope as WitRpcEnvelope,
    WebrtcPeerStatus as WitWebrtcPeerStatus,
};
use crate::wasm::error::{WasmError, WasmResult};
use crate::workload::{
    HostAbiFn, HostOperation, HostOperationResult, InvocationContext, PackageHookEvent,
};

use actr_framework::guest::dynclib_abi as guest_abi;
use actr_framework::guest::dynclib_abi::{
    HostCallRawV1, HostCallV1, HostDiscoverV1, HostRegisterStreamV1, HostSendDataStreamV1,
    HostTellV1, HostUnregisterStreamV1,
};

// ─────────────────────────────────────────────────────────────────────────────
// Engine configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Build a wasmtime [`Config`] enabling the Component Model async path.
///
/// `wasm_component_model_async(true)` is load-bearing even when the WIT
/// functions are declared sync — wit-bindgen 0.57 with `async: true` on
/// the guest emits `context.get` (async-ABI primitive), and the host
/// engine must recognise that opcode to validate the component. See the
/// Phase 0.5 spike REPORT for details.
fn build_engine() -> WasmResult<Engine> {
    let mut config = Config::new();
    // `async_support(true)` was required before wasmtime 43; since then
    // the `async` Cargo feature alone enables async at the engine level.
    // We pair it with explicit component-model flags to be self-documenting.
    config.wasm_component_model(true);
    config.wasm_component_model_async(true);
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
}

impl std::fmt::Debug for HostState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostState")
            .field("invocation", &self.invocation)
            .field("host_abi", &self.host_abi.as_ref().map(|_| "<fn>"))
            .finish_non_exhaustive()
    }
}

impl HostState {
    fn new() -> Self {
        Self {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            table: ResourceTable::new(),
            invocation: None,
            host_abi: None,
        }
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

    async fn send_data_stream(
        &mut self,
        target: WitDest,
        chunk: WitDataStream,
        payload_type: WitPayloadType,
    ) -> wasmtime::Result<Result<(), WitActrError>> {
        let op = HostOperation::SendDataStream(HostSendDataStreamV1 {
            dest: wit_dest_to_v1(&target),
            chunk: wit_data_stream_to_proto(chunk),
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
        WitDest::Shell => guest_abi::DestV1::shell(),
        WitDest::Local => guest_abi::DestV1::local(),
        WitDest::Actor(id) => guest_abi::DestV1::actor(wit_actr_id_to_proto(id)),
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

fn proto_data_stream_to_wit(chunk: DataStream) -> WitDataStream {
    WitDataStream {
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

fn wit_data_stream_to_proto(chunk: WitDataStream) -> DataStream {
    DataStream {
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
    pub fn compile(wasm_bytes: &[u8]) -> WasmResult<Self> {
        let engine = build_engine()?;
        let component = Component::from_binary(&engine, wasm_bytes).map_err(|e| {
            WasmError::LoadFailed(format!(
                "wasm bytes did not load as a Component (this host \
                 requires Component Model binaries as of .actr format \
                 bump; wasmtime reported: {e})"
            ))
        })?;
        tracing::info!(wasm_bytes = wasm_bytes.len(), "wasm Component compiled");
        Ok(Self { engine, component })
    }

    /// Instantiate the component into a runnable internal workload.
    ///
    /// Builds a fresh [`Linker`] per instance (cheap), registers WASI
    /// p2 as well as the generated `actr:workload/host` linker, and
    /// runs `Component::instantiate_async` which drives any
    /// component-model initialisation through the host reactor. The
    /// resulting bindings are cached on the returned workload
    /// for subsequent `call_dispatch` and hook invocations.
    pub(crate) async fn instantiate(&self) -> WasmResult<WasmWorkload> {
        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(|e| {
            WasmError::LoadFailed(format!("failed to register WASI p2 linker imports: {e}"))
        })?;
        ActrWorkloadGuest::add_to_linker::<_, HasSelf<_>>(&mut linker, |s| s).map_err(|e| {
            WasmError::LoadFailed(format!(
                "failed to register actr:workload/host linker imports: {e}"
            ))
        })?;

        let mut store = Store::new(&self.engine, HostState::new());
        let bindings = ActrWorkloadGuest::instantiate_async(&mut store, &self.component, &linker)
            .await
            .map_err(|e| {
                WasmError::LoadFailed(format!("Component instantiate_async failed: {e:#}"))
            })?;

        tracing::info!("wasm Component instantiated");
        Ok(WasmWorkload { store, bindings })
    }
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
pub(crate) struct WasmWorkload {
    store: Store<HostState>,
    bindings: ActrWorkloadGuest,
}

impl std::fmt::Debug for WasmWorkload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmWorkload").finish_non_exhaustive()
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
        let state = self.store.data_mut();
        state.invocation = Some(ctx);
        state.host_abi = Some(host_abi_clone);
    }

    fn clear_invocation(&mut self) {
        let state = self.store.data_mut();
        state.invocation = None;
        state.host_abi = None;
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
        self.install_invocation(ctx, host_abi);

        let result = self
            .bindings
            .actr_workload_workload()
            .call_on_start(&mut self.store)
            .await;

        self.clear_invocation();

        let result =
            result.map_err(|e| WasmError::ExecutionFailed(format!("on_start trap: {e}")))?;
        result.map_err(|e| WasmError::ExecutionFailed(format!("on_start error: {:?}", e)))?;
        Ok(())
    }

    pub(crate) async fn call_on_ready(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.install_invocation(ctx, host_abi);

        let result = self
            .bindings
            .actr_workload_workload()
            .call_on_ready(&mut self.store)
            .await;

        self.clear_invocation();

        let result =
            result.map_err(|e| WasmError::ExecutionFailed(format!("on_ready trap: {e}")))?;
        result.map_err(|e| WasmError::ExecutionFailed(format!("on_ready error: {:?}", e)))?;
        Ok(())
    }

    pub(crate) async fn call_on_stop(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.install_invocation(ctx, host_abi);

        let result = self
            .bindings
            .actr_workload_workload()
            .call_on_stop(&mut self.store)
            .await;

        self.clear_invocation();

        let result =
            result.map_err(|e| WasmError::ExecutionFailed(format!("on_stop trap: {e}")))?;
        result.map_err(|e| WasmError::ExecutionFailed(format!("on_stop error: {:?}", e)))?;
        Ok(())
    }

    pub(crate) async fn call_hook_event(
        &mut self,
        event: PackageHookEvent,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        let label = event.request_id();
        self.install_invocation(ctx, host_abi);
        let result = match event {
            PackageHookEvent::SignalingConnecting => {
                self.bindings
                    .actr_workload_workload()
                    .call_on_signaling_connecting(&mut self.store)
                    .await
            }
            PackageHookEvent::SignalingConnected => {
                self.bindings
                    .actr_workload_workload()
                    .call_on_signaling_connected(&mut self.store)
                    .await
            }
            PackageHookEvent::SignalingDisconnected => {
                self.bindings
                    .actr_workload_workload()
                    .call_on_signaling_disconnected(&mut self.store)
                    .await
            }
            PackageHookEvent::WebSocketConnecting(event) => {
                let event = proto_peer_event_to_wit(event);
                self.bindings
                    .actr_workload_workload()
                    .call_on_websocket_connecting(&mut self.store, &event)
                    .await
            }
            PackageHookEvent::WebSocketConnected(event) => {
                let event = proto_peer_event_to_wit(event);
                self.bindings
                    .actr_workload_workload()
                    .call_on_websocket_connected(&mut self.store, &event)
                    .await
            }
            PackageHookEvent::WebSocketDisconnected(event) => {
                let event = proto_peer_event_to_wit(event);
                self.bindings
                    .actr_workload_workload()
                    .call_on_websocket_disconnected(&mut self.store, &event)
                    .await
            }
            PackageHookEvent::WebRtcConnecting(event) => {
                let event = proto_peer_event_to_wit(event);
                self.bindings
                    .actr_workload_workload()
                    .call_on_webrtc_connecting(&mut self.store, &event)
                    .await
            }
            PackageHookEvent::WebRtcConnected(event) => {
                let event = proto_peer_event_to_wit(event);
                self.bindings
                    .actr_workload_workload()
                    .call_on_webrtc_connected(&mut self.store, &event)
                    .await
            }
            PackageHookEvent::WebRtcDisconnected(event) => {
                let event = proto_peer_event_to_wit(event);
                self.bindings
                    .actr_workload_workload()
                    .call_on_webrtc_disconnected(&mut self.store, &event)
                    .await
            }
            PackageHookEvent::CredentialRenewed(event) => {
                let event = proto_credential_event_to_wit(event);
                self.bindings
                    .actr_workload_workload()
                    .call_on_credential_renewed(&mut self.store, event)
                    .await
            }
            PackageHookEvent::CredentialExpiring(event) => {
                let event = proto_credential_event_to_wit(event);
                self.bindings
                    .actr_workload_workload()
                    .call_on_credential_expiring(&mut self.store, event)
                    .await
            }
            PackageHookEvent::MailboxBackpressure(event) => {
                let event = proto_backpressure_event_to_wit(event);
                self.bindings
                    .actr_workload_workload()
                    .call_on_mailbox_backpressure(&mut self.store, event)
                    .await
            }
        };
        self.clear_invocation();

        result.map_err(|e| WasmError::ExecutionFailed(format!("{label} trap: {e}")))?;
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

        // Thread per-call context into the Store. `HostAbiFn` is an
        // `Arc<...>` (Phase-1 type bump); cloning it is a refcount bump
        // with `'static` lifetime, safe to carry across the async
        // dispatch boundary. A fresh clone is installed per dispatch
        // so the bridge is dropped when the dispatch completes or
        // traps.
        self.install_invocation(ctx, host_abi);

        let wit_envelope = rpc_envelope_to_wit(&envelope);
        let dispatch_result = self
            .bindings
            .actr_workload_workload()
            .call_dispatch(&mut self.store, &wit_envelope)
            .await;

        // Always clear per-call state regardless of outcome — a trap
        // poisons the Store (wasmtime drops further use anyway) but
        // clearing keeps the state machine observable even when the
        // caller decides to retain the store for a next dispatch.
        self.clear_invocation();

        match dispatch_result {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(wit_err)) => Err(WasmError::ExecutionFailed(format!(
                "guest dispatch returned error: {:?}",
                wit_actr_error_to_proto(wit_err)
            ))),
            Err(trap) => Err(WasmError::ExecutionFailed(format!(
                "guest dispatch trapped: {trap}"
            ))),
        }
    }

    pub(crate) async fn handle_data_stream(
        &mut self,
        chunk: DataStream,
        sender: ActrId,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.install_invocation(ctx, host_abi);

        let wit_chunk = proto_data_stream_to_wit(chunk);
        let wit_sender = proto_actr_id_to_wit(&sender);
        let result = self
            .bindings
            .actr_workload_workload()
            .call_on_data_stream(&mut self.store, &wit_chunk, &wit_sender)
            .await;
        self.clear_invocation();
        let result =
            result.map_err(|e| WasmError::ExecutionFailed(format!("on_data_stream trap: {e}")))?;
        result.map_err(|e| {
            WasmError::ExecutionFailed(format!(
                "on_data_stream error: {:?}",
                wit_actr_error_to_proto(e)
            ))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_rejects_non_wasm_bytes() {
        let err = WasmHost::compile(b"definitely not a wasm component").unwrap_err();
        assert!(matches!(err, WasmError::LoadFailed(_)));
        assert!(err.to_string().contains("Component"));
    }

    #[test]
    fn compile_rejects_empty_bytes() {
        let err = WasmHost::compile(&[]).unwrap_err();
        assert!(matches!(err, WasmError::LoadFailed(_)));
    }

    #[test]
    fn compile_rejects_legacy_core_module_magic() {
        // `\0asm` magic + invalid body must still fail (host requires
        // Component Model binaries).
        let bogus = b"\0asm\x01\x00\x00\x00garbage";
        let err = WasmHost::compile(bogus).unwrap_err();
        assert!(matches!(err, WasmError::LoadFailed(_)));
    }
}
