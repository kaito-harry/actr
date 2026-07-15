//! WasmWorkloadV2 — the `actr:workload@0.2.0` async-world execution path.
//!
//! Sibling of [`super::host::WasmWorkload`] (the 0.1.0 serial path). Where
//! the V1 path drives the guest through `call_dispatch(&mut store, ...)`
//! (borrow-checker-serialized), V2 drives it through a
//! `Store::run_concurrent(async |accessor| ...)` region and Accessor-based
//! host imports. Under M4 the region holds exactly ONE task at a time (the
//! runner is still serial), so behaviour is identical to V1 end-to-end;
//! M5 opens the region to `FuturesUnordered` for real same-instance
//! concurrency with zero further changes to the host-import side (each
//! in-flight invocation keys its `HostAbiFn` by `ctx-token`).
//!
//! The host-import trait here is Accessor-based: methods are static async
//! associated functions taking `&Accessor<HostState, Self>`, and store
//! access is synchronous-only via `accessor.with(|a| ...)` (its borrow
//! cannot cross an `.await`). This is what makes several `&mut Store`
//! borrows non-overlapping across suspension points. The shape is lifted
//! directly from the Phase 0.75 `component-spike-runconcurrent` host.

use actr_framework::guest::dynclib_abi as guest_abi;
use actr_framework::guest::dynclib_abi::{
    HostCallRawV1, HostCallV1, HostDiscoverV1, HostRegisterStreamV1, HostSendDataChunkV1,
    HostTellV1, HostUnregisterStreamV1,
};
use actr_framework::{BackpressureEvent, CredentialEvent, PeerEvent, WebRtcPeerStatus};
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{
    ActrError, ActrId, ActrType, ConnectionNotReadyInfo, DataChunk, MetadataEntry, PayloadType,
    Realm, RpcEnvelope,
};
use wasmtime::component::{Accessor, Component, HasData, Linker};
use wasmtime::{AsContextMut, Engine, Store};

use super::component_bindings_v2::ActrWorkloadGuestV2;
use super::component_bindings_v2::actr::workload::host::{Host as HostImportsV2, HostWithStore};
use super::component_bindings_v2::actr::workload::types::{
    self as wit2, ActrError as WitActrError, ActrId as WitActrId, ActrType as WitActrType,
    BackpressureEvent as WitBackpressureEvent, CredentialEvent as WitCredentialEvent,
    DataChunk as WitDataChunk, Dest as WitDest, Host as TypesHostV2,
    InvocationCtx as WitInvocationCtx, PayloadType as WitPayloadType, PeerEvent as WitPeerEvent,
    Realm as WitRealm, RpcEnvelope as WitRpcEnvelope, WebrtcPeerStatus as WitWebrtcPeerStatus,
};
use super::error::{WasmError, WasmResult};
use super::host::{HostState, epoch_deadline_ticks};
use super::runtime_limits::{
    EpochTicker, QuotaPermit, StorePermit, acquire_instantiate, acquire_invocation,
    record_instantiate_failure, record_timeout,
};
use crate::config::WasmRuntimeLimits;
use crate::executor::{ActorCmd, LifecyclePhase};
use crate::workload::{
    HostAbiFn, HostOperation, HostOperationResult, InvocationContext, PackageHookEvent,
};
use actr_protocol::ActorResult;
use bytes::Bytes;
use futures_util::FutureExt as _;
use futures_util::future::{BoxFuture, poll_fn};
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument as _;

// ─────────────────────────────────────────────────────────────────────────────
// HasData projection + host-import Accessor trait
// ─────────────────────────────────────────────────────────────────────────────

// `type Data<'a> = &'a mut HostState` means "give the Accessor host methods
// `&mut HostState`". Required by the wasmtime 46 async binding shape.
impl HasData for HostState {
    type Data<'a> = &'a mut HostState;
}

// `types` is a types-only interface; bindgen still emits a marker `Host`
// trait the host state must implement. Empty impl satisfies the linker.
impl TypesHostV2 for HostState {}

// Store-less marker trait (imports needing only `self`). The blanket
// `impl Host for &mut T` needs this to exist; empty impl suffices.
impl HostImportsV2 for HostState {}

/// Forward a guest-initiated [`HostOperation`] through a `HostAbiFn` cloned
/// out of the per-invocation table (keyed by `ctx-token`). The `HostAbiFn`
/// is an `Arc`, cloned synchronously via `accessor.with` before this future
/// is awaited, so no store borrow is held across the `.await`.
async fn run_host_operation(
    host_abi: Option<HostAbiFn>,
    op: HostOperation,
) -> wasmtime::Result<Result<Vec<u8>, WitActrError>> {
    let Some(host_abi) = host_abi else {
        return Err(wasmtime::Error::msg(
            "host ABI bridge not installed for this ctx-token (unknown or retired invocation)",
        ));
    };
    match (host_abi)(op).await {
        HostOperationResult::Bytes(bytes) => Ok(Ok(bytes)),
        HostOperationResult::Done => Ok(Ok(Vec::new())),
        HostOperationResult::Error(code) => Ok(Err(actr_error_from_abi_code(code))),
    }
}

impl HostWithStore<HostState> for HostState {
    async fn call(
        accessor: &Accessor<HostState, Self>,
        ctx_token: u64,
        target: WitDest,
        route_key: String,
        payload: Vec<u8>,
    ) -> wasmtime::Result<Result<Vec<u8>, WitActrError>> {
        let host_abi = accessor.with(|mut a| a.get().invocation_host_abi(ctx_token));
        let op = HostOperation::Call(HostCallV1 {
            route_key,
            dest: wit_dest_to_v1(&target),
            payload,
        });
        run_host_operation(host_abi, op).await
    }

    async fn tell(
        accessor: &Accessor<HostState, Self>,
        ctx_token: u64,
        target: WitDest,
        route_key: String,
        payload: Vec<u8>,
    ) -> wasmtime::Result<Result<(), WitActrError>> {
        let host_abi = accessor.with(|mut a| a.get().invocation_host_abi(ctx_token));
        let op = HostOperation::Tell(HostTellV1 {
            route_key,
            dest: wit_dest_to_v1(&target),
            payload,
        });
        match run_host_operation(host_abi, op).await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    async fn call_raw(
        accessor: &Accessor<HostState, Self>,
        ctx_token: u64,
        target: WitActrId,
        route_key: String,
        payload: Vec<u8>,
    ) -> wasmtime::Result<Result<Vec<u8>, WitActrError>> {
        let host_abi = accessor.with(|mut a| a.get().invocation_host_abi(ctx_token));
        let op = HostOperation::CallRaw(HostCallRawV1 {
            route_key,
            target: wit_actr_id_to_proto(&target),
            payload,
        });
        run_host_operation(host_abi, op).await
    }

    async fn discover(
        accessor: &Accessor<HostState, Self>,
        ctx_token: u64,
        target_type: WitActrType,
    ) -> wasmtime::Result<Result<WitActrId, WitActrError>> {
        let host_abi = accessor.with(|mut a| a.get().invocation_host_abi(ctx_token));
        let op = HostOperation::Discover(HostDiscoverV1 {
            target_type: wit_actr_type_to_proto(&target_type),
        });
        match run_host_operation(host_abi, op).await? {
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
        accessor: &Accessor<HostState, Self>,
        ctx_token: u64,
        stream_id: String,
    ) -> wasmtime::Result<Result<(), WitActrError>> {
        let host_abi = accessor.with(|mut a| a.get().invocation_host_abi(ctx_token));
        let op = HostOperation::RegisterStream(HostRegisterStreamV1 {
            stream_id: stream_id.clone(),
        });
        match run_host_operation(host_abi, op).await? {
            Ok(_) => {
                accessor.with(|mut a| {
                    a.get().register_stream_context(stream_id, ctx_token);
                });
                Ok(Ok(()))
            }
            Err(e) => Ok(Err(e)),
        }
    }

    async fn unregister_stream(
        accessor: &Accessor<HostState, Self>,
        ctx_token: u64,
        stream_id: String,
    ) -> wasmtime::Result<Result<(), WitActrError>> {
        let host_abi = accessor.with(|mut a| a.get().invocation_host_abi(ctx_token));
        let op = HostOperation::UnregisterStream(HostUnregisterStreamV1 {
            stream_id: stream_id.clone(),
        });
        match run_host_operation(host_abi, op).await? {
            Ok(_) => {
                accessor.with(|mut a| {
                    a.get().unregister_stream_context(&stream_id);
                });
                Ok(Ok(()))
            }
            Err(e) => Ok(Err(e)),
        }
    }

    async fn send_data_chunk(
        accessor: &Accessor<HostState, Self>,
        ctx_token: u64,
        target: WitDest,
        chunk: WitDataChunk,
        payload_type: WitPayloadType,
    ) -> wasmtime::Result<Result<(), WitActrError>> {
        let host_abi = accessor.with(|mut a| a.get().invocation_host_abi(ctx_token));
        let op = HostOperation::SendDataChunk(HostSendDataChunkV1 {
            dest: wit_dest_to_v1(&target),
            chunk: wit_data_chunk_to_proto(chunk),
            payload_type: wit_payload_type_to_proto(payload_type) as i32,
        });
        match run_host_operation(host_abi, op).await? {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(e)),
        }
    }

    async fn log_message(
        _accessor: &Accessor<HostState, Self>,
        _ctx_token: u64,
        level: String,
        message: String,
    ) -> wasmtime::Result<()> {
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
}

// ─────────────────────────────────────────────────────────────────────────────
// WIT (0.2.0) ↔ actr_protocol / actr_framework translation
//
// The 0.2.0 bindgen emits its own distinct type namespace, so these mirror
// the 0.1.0 helpers in `host.rs` but target the v2 `wit2` structs.
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

fn wit_connection_not_ready_info_to_proto(
    info: wit2::ConnectionNotReadyInfo,
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

fn invocation_ctx_to_wit(ctx: &InvocationContext, ctx_token: u64) -> WitInvocationCtx {
    WitInvocationCtx {
        ctx_token,
        self_id: proto_actr_id_to_wit(&ctx.self_id),
        caller_id: ctx.caller_id.as_ref().map(proto_actr_id_to_wit),
        request_id: ctx.request_id.clone(),
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
            .map(|entry| wit2::MetadataEntry {
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

fn system_time_to_wit(time: std::time::SystemTime) -> wit2::Timestamp {
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    wit2::Timestamp {
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
// Instantiation
// ─────────────────────────────────────────────────────────────────────────────

/// Build a fresh async-world [`Store`] + component instance. Registers WASI
/// p2 and the Accessor-based `actr:workload/host` (0.2.0) linker imports.
async fn instantiate_parts_v2(
    engine: &Engine,
    component: &Component,
    limits: &WasmRuntimeLimits,
) -> WasmResult<(Store<HostState>, ActrWorkloadGuestV2)> {
    let _instantiate_permit = acquire_instantiate(limits)?;
    let mut linker: Linker<HostState> = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(|e| {
        WasmError::LoadFailed(format!("failed to register WASI p2 linker imports: {e}"))
    })?;
    // D = HostState (impls HasData + HostWithStore + Host); host_getter is identity.
    super::component_bindings_v2::actr::workload::host::add_to_linker::<HostState, HostState>(
        &mut linker,
        |s| s,
    )
    .map_err(|e| {
        WasmError::LoadFailed(format!(
            "failed to register actr:workload/host@0.2.0 linker imports: {e}"
        ))
    })?;

    let mut store = Store::new(engine, HostState::new(limits));
    // issue #346: per-store limiter + fuel/epoch seed, same as the V1 path.
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
        ActrWorkloadGuestV2::instantiate_async(&mut store, component, &linker),
    )
    .await
    {
        Ok(Ok(bindings)) => bindings,
        Ok(Err(error)) => {
            record_instantiate_failure();
            return Err(WasmError::LoadFailed(format!(
                "Component instantiate_async (v2) failed: {error:#}"
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
// WasmWorkloadV2
// ─────────────────────────────────────────────────────────────────────────────

/// Single 0.2.0 async-world wasm actor instance.
///
/// Mirrors [`super::host::WasmWorkload`]'s lifecycle (engine/component/store
/// plus poison/rebuild), but every guest entry runs inside a single-task
/// `Store::run_concurrent` region. The per-invocation `ctx-token` is
/// allocated into [`HostState`]'s invocation table just before the region
/// opens and retired after it closes; a trap clears the whole table.
pub(crate) struct WasmWorkloadV2 {
    engine: Engine,
    component: Component,
    /// issue #346: resource limits retained for post-trap rebuild + per-entry
    /// fuel/epoch re-seed.
    limits: WasmRuntimeLimits,
    store: Option<Store<HostState>>,
    bindings: ActrWorkloadGuestV2,
    _epoch_ticker: Arc<EpochTicker>,
    _store_permit: StorePermit,
    poisoned: bool,
    rebuilds: u64,
}

impl std::fmt::Debug for WasmWorkloadV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmWorkloadV2")
            .field("poisoned", &self.poisoned)
            .field("rebuilds", &self.rebuilds)
            .finish_non_exhaustive()
    }
}

impl WasmWorkloadV2 {
    /// Build a V2 instance from an already-compiled engine/component pair.
    pub(crate) async fn instantiate(
        engine: &Engine,
        component: &Component,
        limits: &WasmRuntimeLimits,
        epoch_ticker: Arc<EpochTicker>,
        store_permit: StorePermit,
    ) -> WasmResult<Self> {
        let (store, bindings) = instantiate_parts_v2(engine, component, limits).await?;
        tracing::info!("wasm Component instantiated (v2 async world)");
        Ok(Self {
            engine: engine.clone(),
            component: component.clone(),
            limits: *limits,
            store: Some(store),
            bindings,
            _epoch_ticker: epoch_ticker,
            _store_permit: store_permit,
            poisoned: false,
            rebuilds: 0,
        })
    }

    /// Legacy init entry — mirrors the V1 path so the loader stays uniform.
    pub(crate) fn init(&mut self, init_payload: &guest_abi::InitPayloadV1) -> WasmResult<()> {
        tracing::debug!(
            actr_type = %init_payload.actr_type,
            realm_id = init_payload.realm_id,
            "wasm Component workload init (v2; Component-model lifecycle handles this implicitly)"
        );
        Ok(())
    }

    #[cfg(feature = "test-utils")]
    pub(crate) fn rebuild_count(&self) -> u64 {
        self.rebuilds
    }

    /// issue #346: re-seed per-entry fuel + epoch deadline. Call before every
    /// guest entry so a runaway compute loop is preempted (fuel check points)
    /// and the epoch backstop is armed.
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

    /// Rebuild a poisoned store (fresh Store + re-instantiate), discarding
    /// the guest's in-memory state. No-op if not poisoned.
    async fn ensure_instance(&mut self) -> WasmResult<()> {
        if !self.poisoned {
            return Ok(());
        }
        tracing::warn!(
            rebuild_attempt = self.rebuilds + 1,
            "rebuilding poisoned wasm store (v2) after a prior guest trap; \
             guest in-memory state is discarded (lifecycle/queue not replayed)"
        );
        drop(self.store.take());
        match instantiate_parts_v2(&self.engine, &self.component, &self.limits).await {
            Ok((store, bindings)) => {
                self.store = Some(store);
                self.bindings = bindings;
                self.poisoned = false;
                self.rebuilds += 1;
                tracing::info!(
                    rebuilds = self.rebuilds,
                    "wasm store rebuilt (v2); serviceable"
                );
                Ok(())
            }
            Err(e @ WasmError::ResourceLimitExceeded(_)) => Err(e),
            Err(e) => Err(WasmError::LoadFailed(format!(
                "failed to rebuild poisoned wasm store (v2): {e}"
            ))),
        }
    }

    /// Mark the store poisoned after a trap and clear the whole invocation
    /// table (every in-flight token is dead). Returns a distinct
    /// [`WasmError::InstanceTrapped`].
    fn trap_poison(&mut self, entry: &str, trap: wasmtime::Error) -> WasmError {
        self.poisoned = true;
        if let Some(store) = self.store.as_mut() {
            store.data_mut().clear_invocations();
        }
        // Wasmtime does not promise that dropping an individual concurrent
        // call cancels its guest task. The physical Store is the fault and
        // cancellation boundary, so never retain it after a trap.
        drop(self.store.take());
        tracing::error!(
            entry,
            error = %trap,
            "wasm guest trapped (v2); store discarded (instance-level fatal). \
             In-memory guest state is lost; a fresh instance is rebuilt before the next call"
        );
        super::error::classify_trap(entry, trap)
    }

    fn timeout_poison(&mut self) -> WasmError {
        self.poisoned = true;
        if let Some(store) = self.store.as_mut() {
            store.data_mut().clear_invocations();
        }
        drop(self.store.take());
        record_timeout();
        WasmError::InvocationTimeout(self.limits.invocation_timeout)
    }

    /// Handle one inbound RPC request through the async world.
    pub(crate) async fn handle(
        &mut self,
        request_bytes: &[u8],
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<Vec<u8>> {
        self.ensure_instance().await?;

        let envelope = RpcEnvelope::decode(request_bytes).map_err(|e| {
            WasmError::ExecutionFailed(format!(
                "host failed to decode RpcEnvelope before dispatch: {e}"
            ))
        })?;
        let wit_envelope = rpc_envelope_to_wit(&envelope);
        let _invocation_permit = acquire_invocation(&self.limits)?;
        self.reseed_fuel()?;

        // Register this invocation and thread its token into the guest.
        let token = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .data_mut()
            .alloc_invocation(ctx.clone(), host_abi.clone());
        let inv = invocation_ctx_to_wit(&ctx, token);
        let bindings = &self.bindings;
        let region_fut = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .run_concurrent(async move |accessor| {
                bindings
                    .actr_workload_workload()
                    .call_dispatch(accessor, wit_envelope, inv)
                    .await
            });
        let region = match tokio::time::timeout(self.limits.invocation_timeout, region_fut).await {
            Ok(region) => region,
            Err(_) => return Err(self.timeout_poison()),
        };

        // Region closed: retire the token (unless the whole table was
        // cleared by a trap-poison below).
        if !self.poisoned {
            self.store
                .as_mut()
                .expect("serviceable wasm instance must have a Store")
                .data_mut()
                .remove_invocation(token);
        }

        match region {
            // Region-level failure (trap surfaced out of run_concurrent).
            Err(trap) => Err(self.trap_poison("dispatch", trap)),
            Ok(call) => match call {
                Ok(Ok(bytes)) => Ok(bytes),
                Ok(Err(wit_err)) => Err(WasmError::ExecutionFailed(format!(
                    "guest dispatch returned error: {:?}",
                    wit_actr_error_to_proto(wit_err)
                ))),
                Err(trap) => Err(self.trap_poison("dispatch", trap)),
            },
        }
    }

    pub(crate) async fn call_on_start(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.ensure_instance().await?;
        let _invocation_permit = acquire_invocation(&self.limits)?;
        // Arm the Store before installing invocation state so every early
        // error path leaves the per-call table empty.
        self.reseed_fuel()?;
        let token = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .data_mut()
            .alloc_invocation(ctx.clone(), host_abi.clone());
        let inv = invocation_ctx_to_wit(&ctx, token);
        // issue #346: re-seed fuel + epoch deadline before the guest entry,
        // then bound the whole run_concurrent region with the wall-clock
        // timeout. fuel/epoch preempt non-yielding compute; the timeout bounds
        // a guest that awaits a host import indefinitely.
        let bindings = &self.bindings;
        let region_fut = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .run_concurrent(async move |accessor| {
                bindings
                    .actr_workload_workload()
                    .call_on_start(accessor, inv)
                    .await
            });
        let region = match tokio::time::timeout(self.limits.invocation_timeout, region_fut).await {
            Ok(region) => region,
            Err(_) => return Err(self.timeout_poison()),
        };
        self.finish_lifecycle("on_start", token, region)
    }

    pub(crate) async fn call_on_ready(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.ensure_instance().await?;
        let _invocation_permit = acquire_invocation(&self.limits)?;
        self.reseed_fuel()?;
        let token = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .data_mut()
            .alloc_invocation(ctx.clone(), host_abi.clone());
        let inv = invocation_ctx_to_wit(&ctx, token);
        let bindings = &self.bindings;
        let region_fut = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .run_concurrent(async move |accessor| {
                bindings
                    .actr_workload_workload()
                    .call_on_ready(accessor, inv)
                    .await
            });
        let region = match tokio::time::timeout(self.limits.invocation_timeout, region_fut).await {
            Ok(region) => region,
            Err(_) => return Err(self.timeout_poison()),
        };
        self.finish_lifecycle("on_ready", token, region)
    }

    pub(crate) async fn call_on_stop(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.ensure_instance().await?;
        let _invocation_permit = acquire_invocation(&self.limits)?;
        self.reseed_fuel()?;
        let token = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .data_mut()
            .alloc_invocation(ctx.clone(), host_abi.clone());
        let inv = invocation_ctx_to_wit(&ctx, token);
        let bindings = &self.bindings;
        let region_fut = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .run_concurrent(async move |accessor| {
                bindings
                    .actr_workload_workload()
                    .call_on_stop(accessor, inv)
                    .await
            });
        let region = match tokio::time::timeout(self.limits.invocation_timeout, region_fut).await {
            Ok(region) => region,
            Err(_) => return Err(self.timeout_poison()),
        };
        self.finish_lifecycle("on_stop", token, region)
    }

    /// Retire the token and classify a fallible-hook region outcome: outer
    /// `Err`/inner trap → poison+rebuild; inner business `Err` →
    /// `ExecutionFailed` (does NOT poison).
    fn finish_lifecycle(
        &mut self,
        label: &str,
        token: u64,
        region: wasmtime::Result<wasmtime::Result<Result<(), WitActrError>>>,
    ) -> WasmResult<()> {
        if !self.poisoned {
            self.store
                .as_mut()
                .expect("serviceable wasm instance must have a Store")
                .data_mut()
                .remove_invocation(token);
        }
        match region {
            Err(trap) => Err(self.trap_poison(label, trap)),
            Ok(call_result) => match call_result {
                Ok(inner) => inner.map_err(|e| {
                    WasmError::ExecutionFailed(format!(
                        "{label} error: {:?}",
                        wit_actr_error_to_proto(e)
                    ))
                }),
                Err(trap) => Err(self.trap_poison(label, trap)),
            },
        }
    }

    /// Drive one DataChunk fast-path chunk.
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
        let stream_id = chunk.stream_id.clone();
        let wit_chunk = proto_data_chunk_to_wit(chunk);
        let wit_sender = proto_actr_id_to_wit(&sender);
        let (token, captured_token) = {
            let state = self
                .store
                .as_mut()
                .expect("serviceable wasm instance must have a Store")
                .data_mut();
            let token = state.alloc_invocation(ctx.clone(), host_abi.clone());
            let captured_token = state.begin_stream_callback(&stream_id, token);
            (token, captured_token)
        };
        let inv = invocation_ctx_to_wit(&ctx, token);

        let bindings = &self.bindings;
        let region_fut = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .run_concurrent(async move |accessor| {
                bindings
                    .actr_workload_workload()
                    .call_on_data_chunk(accessor, wit_chunk, wit_sender, inv)
                    .await
            });
        let region = match tokio::time::timeout(self.limits.invocation_timeout, region_fut).await {
            Ok(region) => region,
            Err(_) => return Err(self.timeout_poison()),
        };

        if !self.poisoned {
            let state = self
                .store
                .as_mut()
                .expect("serviceable wasm instance must have a Store")
                .data_mut();
            state.end_stream_callback(captured_token, token);
            state.remove_invocation(token);
        }

        match region {
            Err(trap) => Err(self.trap_poison("on_data_chunk", trap)),
            Ok(call) => match call {
                Ok(inner) => inner.map_err(|e| {
                    WasmError::ExecutionFailed(format!(
                        "on_data_chunk error: {:?}",
                        wit_actr_error_to_proto(e)
                    ))
                }),
                Err(trap) => Err(self.trap_poison("on_data_chunk", trap)),
            },
        }
    }

    /// Drive one infallible observation hook. The full invocation context is
    /// passed to the guest and its token is registered so the hook's own host
    /// imports (e.g. `ctx.call_raw`) resolve their `HostAbiFn`.
    pub(crate) async fn call_hook_event(
        &mut self,
        event: PackageHookEvent,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> WasmResult<()> {
        self.ensure_instance().await?;
        let _invocation_permit = acquire_invocation(&self.limits)?;
        self.reseed_fuel()?;
        let label = event.request_id();
        let token = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .data_mut()
            .alloc_invocation(ctx.clone(), host_abi.clone());
        let inv = invocation_ctx_to_wit(&ctx, token);

        let bindings = &self.bindings;
        let region_fut = self
            .store
            .as_mut()
            .expect("serviceable wasm instance must have a Store")
            .run_concurrent(async move |accessor| {
                run_hook_region(accessor, bindings, event, inv).await
            });
        let region = match tokio::time::timeout(self.limits.invocation_timeout, region_fut).await {
            Ok(region) => region,
            Err(_) => return Err(self.timeout_poison()),
        };

        if !self.poisoned {
            self.store
                .as_mut()
                .expect("serviceable wasm instance must have a Store")
                .data_mut()
                .remove_invocation(token);
        }

        match region {
            Err(trap) => Err(self.trap_poison(label, trap)),
            Ok(inner) => inner.map_err(|trap| self.trap_poison(label, trap)),
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // M5 — open concurrency: the resident `run_concurrent` region runner
    // ─────────────────────────────────────────────────────────────────────

    /// Drive this instance as a **resident** `run_concurrent` region for the
    /// whole life of the runner task, so several distinct-conflict-key
    /// dispatches are genuinely in flight on the ONE wasm instance at once
    /// and interleave at their host-import `.await` points (the M0 spike's
    /// Mechanism-1 substrate).
    ///
    /// Ownership contract (kept transparent to [`crate::executor`]): `self` is
    /// consumed and moved into the runner task; `cmd_rx` is the *same* command
    /// channel the serial `run_loop` would own, carrying frozen [`ActorCmd`]s.
    /// A `Dispatch` is pushed into a `FuturesUnordered` inside the live region;
    /// every other command is a **barrier** (drain in-flight, run alone,
    /// resume), preserving lifecycle ordering and the single-runner invariant.
    ///
    /// # Concurrency width
    ///
    /// The runner sets **no** width cap and never inspects conflict keys: the
    /// upstream scheduler's budget `C` is the single source of truth for how
    /// many dispatches are ever in flight (it only re-arms a key after that
    /// key's reply resolves), so `FuturesUnordered` is naturally `≤ C` and
    /// same-key FIFO is enforced one layer up.
    ///
    /// # Fault isolation
    ///
    /// The reply ledger lives **outside** the region (`Arc<Mutex<..>>`, locked
    /// only for momentary insert/remove, never across an `.await`). A guest
    /// fault may collapse the entire region to an outer `Err` or surface from
    /// an individual generated call future. Either way the ledger is the only
    /// place the in-flight siblings' reply senders survive: the supervisor
    /// drains it, fails every pending reply with a retryable error, poisons +
    /// clears the invocation table, rebuilds a fresh store, and re-enters a new
    /// region with the *same* `cmd_rx` (the command queue is plain Rust data a
    /// trap cannot destroy).
    pub(crate) async fn run_interleaved(
        mut self,
        mut cmd_rx: mpsc::Receiver<ActorCmd>,
        dispatch_timeout: Option<Duration>,
    ) {
        // Region-external reply ledger (hard constraint): survives a region
        // trap so no caller is left hanging when the whole region collapses.
        let ledger: Arc<Mutex<HashMap<u64, PendingReply>>> = Arc::new(Mutex::new(HashMap::new()));
        let pending_barrier: Arc<Mutex<Option<ActorCmd>>> = Arc::new(Mutex::new(None));
        let (deadline_tx, mut deadline_rx) = mpsc::unbounded_channel();
        let mut region_generation = 0_u64;

        loop {
            // Rebuild a poisoned store before (re)entering the region.
            if let Err(error) = self.ensure_instance().await {
                if matches!(error, WasmError::ResourceLimitExceeded(_)) {
                    tracing::warn!(%error, "v2 interleaved runner: rebuild quota busy; retrying");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                tracing::error!(
                    %error,
                    "v2 interleaved runner: store rebuild failed; terminating runner \
                     and failing all pending replies"
                );
                let error =
                    ActrError::Unavailable("actor instance unrecoverable after trap".to_string());
                drain_and_fail(&ledger, error.clone());
                fail_pending_command(&pending_barrier, error);
                return;
            }

            if let Err(error) = self.reseed_fuel() {
                tracing::error!(%error, "v2 interleaved runner: failed to arm execution budget");
                let error =
                    ActrError::Unavailable("actor execution budget unavailable".to_string());
                drain_and_fail(&ledger, error.clone());
                fail_pending_command(&pending_barrier, error);
                return;
            }

            let bindings = &self.bindings;
            let ledger_region = Arc::clone(&ledger);
            let pending_barrier_region = Arc::clone(&pending_barrier);
            let deadline_tx_region = deadline_tx.clone();
            let cmd_rx_ref = &mut cmd_rx;
            let limits = self.limits;
            region_generation = region_generation.wrapping_add(1);
            let generation = region_generation;

            let exit = {
                let region = self
                    .store
                    .as_mut()
                    .expect("serviceable wasm instance must have a Store")
                    .run_concurrent(async move |accessor| {
                        resident_region(
                            accessor,
                            bindings,
                            cmd_rx_ref,
                            &ledger_region,
                            &pending_barrier_region,
                            &deadline_tx_region,
                            generation,
                            dispatch_timeout,
                            limits,
                        )
                        .await
                    });
                tokio::pin!(region);
                loop {
                    tokio::select! {
                        result = &mut region => break ResidentRunExit::Region(result),
                        Some(expired) = deadline_rx.recv() => {
                            let live = expired.generation == generation && ledger
                                .lock()
                                .expect("reply ledger mutex poisoned")
                                .contains_key(&expired.token);
                            if live {
                                break ResidentRunExit::TimedOut(expired.token);
                            }
                        }
                    }
                }
            };

            match exit {
                // Clean exit: `cmd_rx` closed or an explicit `Shutdown`. All
                // work drained and replied before we got here.
                ResidentRunExit::Region(Ok(RegionExit::Closed | RegionExit::Shutdown)) => return,
                ResidentRunExit::Region(Ok(RegionExit::CallerCanceled(token))) => {
                    self.fail_all_and_cancel(&ledger, token);
                }
                ResidentRunExit::Region(Ok(RegionExit::GuestCallFailed { token, error })) => {
                    self.fail_all_and_inner_error(&ledger, token, error);
                }
                ResidentRunExit::TimedOut(token) => {
                    self.fail_all_and_timeout(&ledger, token);
                }
                // A guest trap tore the whole region down. Fail every in-flight
                // sibling, poison, and loop to rebuild + re-enter.
                ResidentRunExit::Region(Err(trap)) => self.fail_all_and_poison(&ledger, trap),
            }
        }
    }

    /// Trap recovery for the resident region: fail every still-pending reply in
    /// the region-external ledger (a trap is a whole-instance fault — siblings
    /// are collateral, not adjudicated individually), then poison + clear the
    /// invocation table so the next loop iteration rebuilds a fresh store.
    fn fail_all_and_poison(
        &mut self,
        ledger: &Arc<Mutex<HashMap<u64, PendingReply>>>,
        trap: wasmtime::Error,
    ) {
        let classified = super::error::classify_trap("interleaved region", trap);
        self.discard_store();
        let failed = drain_and_fail(ledger, ActrError::Unavailable(classified.to_string()));
        tracing::error!(
            error = %classified,
            failed_siblings = failed,
            rebuild_attempt = self.rebuilds + 1,
            "wasm guest trapped (v2 interleaved region); whole region collapsed, \
             all in-flight siblings failed with a retryable error; store poisoned, \
             rebuilding a fresh instance"
        );
    }

    fn fail_all_and_timeout(
        &mut self,
        ledger: &Arc<Mutex<HashMap<u64, PendingReply>>>,
        timed_out_token: u64,
    ) {
        // Dropping the whole Store is Wasmtime's hard cancellation boundary
        // for call_concurrent tasks. Do it before resolving any reply so the
        // scheduler cannot free a key while timed-out guest work still runs.
        self.discard_store();
        let timed_out = ledger
            .lock()
            .expect("reply ledger mutex poisoned")
            .remove(&timed_out_token);
        if let Some(pending) = timed_out {
            pending.fail(ActrError::TimedOut);
        }
        let failed = drain_and_fail(
            ledger,
            ActrError::Unavailable("actor instance timed out; message may be retried".to_string()),
        );
        record_timeout();
        tracing::error!(
            failed_siblings = failed,
            rebuild_attempt = self.rebuilds + 1,
            "wasm guest exceeded the resident-region security deadline; store poisoned"
        );
    }

    fn fail_all_and_inner_error(
        &mut self,
        ledger: &Arc<Mutex<HashMap<u64, PendingReply>>>,
        failed_token: u64,
        error: wasmtime::Error,
    ) {
        // A `call_concurrent` error is an unhealthy-instance boundary even
        // when Wasmtime returns it through the individual call future rather
        // than as the outer `run_concurrent` error. Drop the physical Store
        // before resolving any reply; sibling guest tasks remain attached to
        // that Store after their Rust futures are dropped.
        self.discard_store();
        let classified = super::error::classify_trap("interleaved guest call", error);
        let failed = drain_and_fail(ledger, ActrError::Unavailable(classified.to_string()));
        tracing::error!(
            token = failed_token,
            error = %classified,
            failed_calls = failed,
            rebuild_attempt = self.rebuilds + 1,
            "wasm guest call failed inside the resident region; store discarded, \
             triggering call and all in-flight siblings failed"
        );
    }

    fn fail_all_and_cancel(
        &mut self,
        ledger: &Arc<Mutex<HashMap<u64, PendingReply>>>,
        canceled_token: u64,
    ) {
        // Caller cancellation is not proof that a Component call stopped.
        // Collapse the physical Store before releasing its invocation permit
        // or any sibling reply, matching the timeout hard boundary.
        self.discard_store();
        let canceled = ledger
            .lock()
            .expect("reply ledger mutex poisoned")
            .remove(&canceled_token);
        drop(canceled);
        let failed = drain_and_fail(
            ledger,
            ActrError::Unavailable(
                "actor instance canceled; in-flight message may be retried".to_string(),
            ),
        );
        tracing::warn!(
            failed_siblings = failed,
            rebuild_attempt = self.rebuilds + 1,
            "wasm caller canceled an active resident-region invocation; store poisoned"
        );
    }

    fn discard_store(&mut self) {
        self.poisoned = true;
        if let Some(store) = self.store.as_mut() {
            store.data_mut().clear_invocations();
        }
        drop(self.store.take());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Resident-region internals (M5)
// ─────────────────────────────────────────────────────────────────────────────

/// One live invocation's reply sender, parked in the region-external ledger.
/// `Dispatch` replies carry `Bytes`; every barrier reply is unit.
enum PendingReply {
    Dispatch {
        reply: ReplySlot<Bytes>,
        _permit: QuotaPermit,
        _deadline: DeadlineGuard,
    },
    Unit {
        reply: ReplySlot<()>,
        _permit: QuotaPermit,
        _deadline: DeadlineGuard,
    },
}

/// A reply sender that remains outside the `run_concurrent` region while an
/// in-region future watches for receiver cancellation. `poll_closed` only
/// borrows the sender momentarily, so trap recovery can still take and resolve
/// it after every future in the region has been dropped.
struct ReplySlot<T> {
    sender: Arc<Mutex<Option<oneshot::Sender<ActorResult<T>>>>>,
}

impl<T> Clone for ReplySlot<T> {
    fn clone(&self) -> Self {
        Self {
            sender: Arc::clone(&self.sender),
        }
    }
}

impl<T> ReplySlot<T> {
    fn new(sender: oneshot::Sender<ActorResult<T>>) -> Self {
        Self {
            sender: Arc::new(Mutex::new(Some(sender))),
        }
    }

    fn send(&self, result: ActorResult<T>) {
        let sender = self
            .sender
            .lock()
            .expect("reply slot mutex poisoned")
            .take();
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }

    async fn closed(&self) {
        poll_fn(|cx| {
            let mut sender = self.sender.lock().expect("reply slot mutex poisoned");
            match sender.as_mut() {
                Some(sender) => sender.poll_closed(cx),
                None => std::task::Poll::Ready(()),
            }
        })
        .await
    }
}

struct DeadlineGuard {
    abort: tokio::task::AbortHandle,
}

impl Drop for DeadlineGuard {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

fn arm_deadline(
    generation: u64,
    token: u64,
    timeout: Duration,
    deadline_tx: &mpsc::UnboundedSender<DeadlineExpired>,
) -> DeadlineGuard {
    let deadline_tx = deadline_tx.clone();
    let task = tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        let _ = deadline_tx.send(DeadlineExpired { generation, token });
    });
    DeadlineGuard {
        abort: task.abort_handle(),
    }
}

impl PendingReply {
    fn fail(self, err: ActrError) {
        match self {
            PendingReply::Dispatch { reply, .. } => reply.send(Err(err)),
            PendingReply::Unit { reply, .. } => reply.send(Err(err)),
        }
    }
}

#[derive(Clone, Copy)]
struct DeadlineExpired {
    generation: u64,
    token: u64,
}

/// Structured ways a resident region ends. Region-wide traps surface as the
/// outer `run_concurrent` `Err`; individual generated calls may instead carry
/// a Store-fatal Wasmtime error through [`RegionExit::GuestCallFailed`].
enum RegionExit {
    /// `cmd_rx` closed (all handles dropped) and in-flight drained.
    Closed,
    /// Explicit `ActorCmd::Shutdown` barrier.
    Shutdown,
    /// The caller dropped a reply while its guest entry was still active.
    CallerCanceled(u64),
    /// An individual `call_concurrent` future returned a Wasmtime error. This
    /// is fatal to the shared Store even when the outer region stayed alive.
    GuestCallFailed { token: u64, error: wasmtime::Error },
}

enum ResidentRunExit {
    Region(wasmtime::Result<RegionExit>),
    TimedOut(u64),
}

/// What the select loop should do after a barrier finishes.
enum BarrierNext {
    Continue,
    Shutdown,
    CallerCanceled(u64),
    GuestCallFailed { token: u64, error: wasmtime::Error },
}

/// The outcome of one in-flight dispatch future. A caller cancellation is
/// escalated to the outer supervisor, which drops the whole Store: Wasmtime 46
/// explicitly does not cancel a guest task when its `call_concurrent` future is
/// dropped.
enum DispatchOutcome {
    Completed(wasmtime::Result<Result<Vec<u8>, WitActrError>>),
    CallerCanceled,
}

/// Force every individual Wasmtime call error onto the Store-fatal path. The
/// generated concurrent bindings return this error one layer inside
/// `run_concurrent`, so matching only the region's outer `Err` is insufficient.
enum GuestCallCompletion<T> {
    Completed(T),
    StoreFatal(wasmtime::Error),
}

fn guest_call_completion<T>(result: wasmtime::Result<T>) -> GuestCallCompletion<T> {
    match result {
        Ok(value) => GuestCallCompletion::Completed(value),
        Err(error) => GuestCallCompletion::StoreFatal(error),
    }
}

/// Drain the region-external ledger, failing every pending reply. Returns how
/// many were failed. Used on trap recovery and on unrecoverable teardown.
fn drain_and_fail(ledger: &Arc<Mutex<HashMap<u64, PendingReply>>>, err: ActrError) -> usize {
    let drained: Vec<PendingReply> = {
        let mut guard = ledger.lock().expect("reply ledger mutex poisoned");
        guard.drain().map(|(_, v)| v).collect()
    };
    let n = drained.len();
    for pending in drained {
        pending.fail(err.clone());
    }
    n
}

fn fail_pending_command(pending: &Arc<Mutex<Option<ActorCmd>>>, error: ActrError) {
    let command = pending
        .lock()
        .expect("pending barrier mutex poisoned")
        .take();
    let Some(command) = command else {
        return;
    };
    match command {
        ActorCmd::Dispatch { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        ActorCmd::Lifecycle { reply, .. }
        | ActorCmd::DataChunk { reply, .. }
        | ActorCmd::Hook { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        ActorCmd::Shutdown { done } => {
            if let Some(done) = done {
                let _ = done.send(());
            }
        }
    }
}

/// Commands abandoned while queued must never enter guest code. Shutdown has
/// no caller result and remains executable even if its optional completion
/// observer disappeared.
fn command_reply_is_closed(command: &ActorCmd) -> bool {
    match command {
        ActorCmd::Dispatch { reply, .. } => reply.is_closed(),
        ActorCmd::Lifecycle { reply, .. } => reply.is_closed(),
        ActorCmd::DataChunk { reply, .. } => reply.is_closed(),
        ActorCmd::Hook { reply, .. } => reply.is_closed(),
        ActorCmd::Shutdown { .. } => false,
    }
}

/// Map a completed dispatch outcome to the caller-facing [`ActorResult`],
/// matching the serial `handle` path's error shaping so gate-off and gate-on
/// are behaviourally identical for the same guest.
fn classify_dispatch(outcome: Result<Vec<u8>, WitActrError>) -> ActorResult<Bytes> {
    match outcome {
        Ok(bytes) => Ok(Bytes::from(bytes)),
        Err(wit_err) => Err(ActrError::Internal(format!(
            "workload dispatch failed: guest dispatch returned error: {:?}",
            wit_actr_error_to_proto(wit_err)
        ))),
    }
}

/// Map a completed barrier (lifecycle / data-chunk) outcome to a unit reply,
/// mirroring the serial path's `workload {label} failed: ...` shaping.
fn classify_unit(label: &str, res: Result<(), WitActrError>) -> ActorResult<()> {
    match res {
        Ok(()) => Ok(()),
        Err(wit_err) => Err(ActrError::Internal(format!(
            "workload {label} failed: {:?}",
            wit_actr_error_to_proto(wit_err)
        ))),
    }
}

/// The resident `select!` loop that runs *inside* the live `run_concurrent`
/// region. Accepts new commands off `cmd_rx`, drives dispatches concurrently in
/// a `FuturesUnordered`, and runs every non-dispatch command as a barrier.
// Keep the borrowed region state explicit: grouping it behind another owner
// would obscure which values must outlive every in-flight guest future.
#[allow(clippy::too_many_arguments)]
async fn resident_region(
    accessor: &Accessor<HostState>,
    bindings: &ActrWorkloadGuestV2,
    cmd_rx: &mut mpsc::Receiver<ActorCmd>,
    ledger: &Arc<Mutex<HashMap<u64, PendingReply>>>,
    pending_barrier: &Arc<Mutex<Option<ActorCmd>>>,
    deadline_tx: &mpsc::UnboundedSender<DeadlineExpired>,
    generation: u64,
    dispatch_timeout: Option<Duration>,
    limits: WasmRuntimeLimits,
) -> RegionExit {
    // One long-lived workload proxy for the whole region: each in-flight
    // `call_dispatch` future borrows it, so it must outlive the
    // `FuturesUnordered` (hoisted, exactly as the M0 spike does).
    let wl = bindings.actr_workload_workload();
    let mut inflight: FuturesUnordered<BoxFuture<'_, (u64, DispatchOutcome)>> =
        FuturesUnordered::new();
    let mut open = true;
    let mut fuel_budget = RegionFuelBudget::default();

    loop {
        // A queued barrier runs alone, only once every in-flight dispatch has
        // drained — this is the single-runner + lifecycle-ordering guarantee.
        let barrier = if inflight.is_empty() {
            pending_barrier
                .lock()
                .expect("pending barrier mutex poisoned")
                .take()
        } else {
            None
        };
        if let Some(barrier) = barrier {
            match run_barrier(
                accessor,
                bindings,
                ledger,
                deadline_tx,
                generation,
                barrier,
                limits,
                &mut fuel_budget,
            )
            .await
            {
                BarrierNext::Continue => continue,
                BarrierNext::Shutdown => return RegionExit::Shutdown,
                BarrierNext::CallerCanceled(token) => {
                    return RegionExit::CallerCanceled(token);
                }
                BarrierNext::GuestCallFailed { token, error } => {
                    return RegionExit::GuestCallFailed { token, error };
                }
            }
        }
        let barrier_pending = pending_barrier
            .lock()
            .expect("pending barrier mutex poisoned")
            .is_some();
        if !open && inflight.is_empty() && !barrier_pending {
            return RegionExit::Closed;
        }

        tokio::select! {
            biased;
            // Stop pulling new commands while a barrier is draining.
            maybe_cmd = cmd_rx.recv(), if open && !barrier_pending => {
                match maybe_cmd {
                    None => open = false,
                    Some(command) if command_reply_is_closed(&command) => continue,
                    Some(ActorCmd::Dispatch { envelope, ctx, invocation, host_abi, span, reply }) => {
                        // `ctx` (RuntimeContext) drives only the Linked path.
                        let _ = ctx;
                        let permit = match acquire_invocation(&limits) {
                            Ok(permit) => permit,
                            Err(error) => {
                                let _ = reply.send(Err(ActrError::Unavailable(error.to_string())));
                                continue;
                            }
                        };
                        let wit_env = rpc_envelope_to_wit(&envelope);
                        // Token allocation moves inside the region: the store is
                        // owned by the region, so we go through the accessor (the
                        // host-import path already does the same).
                        let (token, live) = accessor.with(|mut a| {
                            let state = a.get();
                            let token = state.alloc_invocation(invocation.clone(), host_abi);
                            (token, state.invocation_count())
                        });
                        let deadline = dispatch_timeout
                            .unwrap_or(limits.invocation_timeout)
                            .min(limits.invocation_timeout);
                        let reply = ReplySlot::new(reply);
                        ledger
                            .lock()
                            .expect("reply ledger mutex poisoned")
                            .insert(token, PendingReply::Dispatch {
                                reply: reply.clone(),
                                _permit: permit,
                                _deadline: arm_deadline(
                                    generation,
                                    token,
                                    deadline,
                                    deadline_tx,
                                ),
                            });
                        if let Err(error) =
                            arm_region_budget(accessor, &limits, live, &mut fuel_budget)
                        {
                            return RegionExit::GuestCallFailed { token, error };
                        }
                        let inv = invocation_ctx_to_wit(&invocation, token);
                        let call = wl.call_dispatch(accessor, wit_env, inv);
                        let fut = async move {
                            let outcome = tokio::select! {
                                biased;
                                _ = reply.closed() => DispatchOutcome::CallerCanceled,
                                result = call => DispatchOutcome::Completed(result),
                            };
                            (token, outcome)
                        };
                        inflight.push(fut.instrument(span).boxed());
                    }
                    Some(barrier) => {
                        *pending_barrier
                            .lock()
                            .expect("pending barrier mutex poisoned") = Some(barrier);
                    }
                }
            }
            Some((token, outcome)) = inflight.next(), if !inflight.is_empty() => {
                if matches!(outcome, DispatchOutcome::CallerCanceled) {
                    return RegionExit::CallerCanceled(token);
                }
                let call = match outcome {
                    DispatchOutcome::Completed(result) => result,
                    DispatchOutcome::CallerCanceled => unreachable!(
                        "caller cancellation returned before guest result classification"
                    ),
                };
                let call_result = match guest_call_completion(call) {
                    GuestCallCompletion::Completed(result) => result,
                    GuestCallCompletion::StoreFatal(error) => {
                        return RegionExit::GuestCallFailed { token, error };
                    }
                };
                let live = accessor.with(|mut a| {
                    let state = a.get();
                    state.remove_invocation(token);
                    state.invocation_count()
                });
                if let Err(error) =
                    clamp_region_budget(accessor, &limits, live, &mut fuel_budget)
                {
                    return RegionExit::GuestCallFailed { token, error };
                }
                let pending = ledger
                    .lock()
                    .expect("reply ledger mutex poisoned")
                    .remove(&token);
                if let Some(PendingReply::Dispatch { reply, .. }) = pending {
                    reply.send(classify_dispatch(call_result));
                }
            }
        }
    }
}

/// Run one barrier command alone inside the region (in-flight already drained).
/// Registers its reply in the ledger *before* the guest call so a trap during
/// the barrier fails it via the outer supervisor.
// These are the same explicit borrowed region dependencies as resident_region;
// keeping them separate makes the barrier's cancellation boundaries visible.
#[allow(clippy::too_many_arguments)]
async fn run_barrier(
    accessor: &Accessor<HostState>,
    bindings: &ActrWorkloadGuestV2,
    ledger: &Arc<Mutex<HashMap<u64, PendingReply>>>,
    deadline_tx: &mpsc::UnboundedSender<DeadlineExpired>,
    generation: u64,
    cmd: ActorCmd,
    limits: WasmRuntimeLimits,
    fuel_budget: &mut RegionFuelBudget,
) -> BarrierNext {
    match cmd {
        ActorCmd::Lifecycle {
            phase,
            ctx,
            invocation,
            host_abi,
            span,
            reply,
        } => {
            if reply.is_closed() {
                return BarrierNext::Continue;
            }
            let _ = ctx;
            let permit = match acquire_invocation(&limits) {
                Ok(permit) => permit,
                Err(error) => {
                    let _ = reply.send(Err(ActrError::Unavailable(error.to_string())));
                    return BarrierNext::Continue;
                }
            };
            let (token, live) = accessor.with(|mut a| {
                let state = a.get();
                let token = state.alloc_invocation(invocation.clone(), host_abi);
                (token, state.invocation_count())
            });
            let inv = invocation_ctx_to_wit(&invocation, token);
            let reply = ReplySlot::new(reply);
            ledger.lock().expect("reply ledger mutex poisoned").insert(
                token,
                PendingReply::Unit {
                    reply: reply.clone(),
                    _permit: permit,
                    _deadline: arm_deadline(
                        generation,
                        token,
                        limits.invocation_timeout,
                        deadline_tx,
                    ),
                },
            );
            if let Err(error) = arm_region_budget(accessor, &limits, live, fuel_budget) {
                return BarrierNext::GuestCallFailed { token, error };
            }
            let call = run_lifecycle_region(accessor, bindings, phase, inv).instrument(span);
            let res = tokio::select! {
                biased;
                _ = reply.closed() => return BarrierNext::CallerCanceled(token),
                result = call => result,
            };
            let call_result = match guest_call_completion(res) {
                GuestCallCompletion::Completed(result) => result,
                GuestCallCompletion::StoreFatal(error) => {
                    return BarrierNext::GuestCallFailed { token, error };
                }
            };
            let live = accessor.with(|mut a| {
                let state = a.get();
                state.remove_invocation(token);
                state.invocation_count()
            });
            if let Err(error) = clamp_region_budget(accessor, &limits, live, fuel_budget) {
                return BarrierNext::GuestCallFailed { token, error };
            }
            if let Some(PendingReply::Unit { reply, .. }) = ledger
                .lock()
                .expect("reply ledger mutex poisoned")
                .remove(&token)
            {
                reply.send(classify_unit(phase.panic_label(), call_result));
            }
            BarrierNext::Continue
        }
        ActorCmd::DataChunk {
            chunk,
            sender,
            invocation,
            host_abi,
            span,
            reply,
        } => {
            if reply.is_closed() {
                return BarrierNext::Continue;
            }
            let permit = match acquire_invocation(&limits) {
                Ok(permit) => permit,
                Err(error) => {
                    let _ = reply.send(Err(ActrError::Unavailable(error.to_string())));
                    return BarrierNext::Continue;
                }
            };
            let stream_id = chunk.stream_id.clone();
            let (token, live) = accessor.with(|mut a| {
                let state = a.get();
                let token = state.alloc_invocation(invocation.clone(), host_abi);
                (token, state.invocation_count())
            });
            let captured_token =
                accessor.with(|mut a| a.get().begin_stream_callback(&stream_id, token));
            let wit_chunk = proto_data_chunk_to_wit(chunk);
            let wit_sender = proto_actr_id_to_wit(&sender);
            let inv = invocation_ctx_to_wit(&invocation, token);
            let reply = ReplySlot::new(reply);
            ledger.lock().expect("reply ledger mutex poisoned").insert(
                token,
                PendingReply::Unit {
                    reply: reply.clone(),
                    _permit: permit,
                    _deadline: arm_deadline(
                        generation,
                        token,
                        limits.invocation_timeout,
                        deadline_tx,
                    ),
                },
            );
            if let Err(error) = arm_region_budget(accessor, &limits, live, fuel_budget) {
                return BarrierNext::GuestCallFailed { token, error };
            }
            let call = run_data_chunk_region(accessor, bindings, wit_chunk, wit_sender, inv)
                .instrument(span);
            let res = tokio::select! {
                biased;
                _ = reply.closed() => return BarrierNext::CallerCanceled(token),
                result = call => result,
            };
            let call_result = match guest_call_completion(res) {
                GuestCallCompletion::Completed(result) => result,
                GuestCallCompletion::StoreFatal(error) => {
                    return BarrierNext::GuestCallFailed { token, error };
                }
            };
            let live = accessor.with(|mut a| {
                let state = a.get();
                state.end_stream_callback(captured_token, token);
                state.remove_invocation(token);
                state.invocation_count()
            });
            if let Err(error) = clamp_region_budget(accessor, &limits, live, fuel_budget) {
                return BarrierNext::GuestCallFailed { token, error };
            }
            if let Some(PendingReply::Unit { reply, .. }) = ledger
                .lock()
                .expect("reply ledger mutex poisoned")
                .remove(&token)
            {
                reply.send(classify_unit("on_data_chunk", call_result));
            }
            BarrierNext::Continue
        }
        ActorCmd::Hook {
            event,
            invocation,
            host_abi,
            span,
            reply,
        } => {
            if reply.is_closed() {
                return BarrierNext::Continue;
            }
            let permit = match acquire_invocation(&limits) {
                Ok(permit) => permit,
                Err(error) => {
                    let _ = reply.send(Err(ActrError::Unavailable(error.to_string())));
                    return BarrierNext::Continue;
                }
            };
            let (token, live) = accessor.with(|mut a| {
                let state = a.get();
                let token = state.alloc_invocation(invocation.clone(), host_abi);
                (token, state.invocation_count())
            });
            let inv = invocation_ctx_to_wit(&invocation, token);
            let reply = ReplySlot::new(reply);
            ledger.lock().expect("reply ledger mutex poisoned").insert(
                token,
                PendingReply::Unit {
                    reply: reply.clone(),
                    _permit: permit,
                    _deadline: arm_deadline(
                        generation,
                        token,
                        limits.invocation_timeout,
                        deadline_tx,
                    ),
                },
            );
            if let Err(error) = arm_region_budget(accessor, &limits, live, fuel_budget) {
                return BarrierNext::GuestCallFailed { token, error };
            }
            let call = run_hook_region(accessor, bindings, event, inv).instrument(span);
            let res = tokio::select! {
                biased;
                _ = reply.closed() => return BarrierNext::CallerCanceled(token),
                result = call => result,
            };
            match guest_call_completion(res) {
                GuestCallCompletion::Completed(()) => {}
                GuestCallCompletion::StoreFatal(error) => {
                    return BarrierNext::GuestCallFailed { token, error };
                }
            }
            let live = accessor.with(|mut a| {
                let state = a.get();
                state.remove_invocation(token);
                state.invocation_count()
            });
            if let Err(error) = clamp_region_budget(accessor, &limits, live, fuel_budget) {
                return BarrierNext::GuestCallFailed { token, error };
            }
            if let Some(PendingReply::Unit { reply, .. }) = ledger
                .lock()
                .expect("reply ledger mutex poisoned")
                .remove(&token)
            {
                reply.send(Ok(()));
            }
            BarrierNext::Continue
        }
        ActorCmd::Shutdown { done } => {
            if let Some(done) = done {
                let _ = done.send(());
            }
            BarrierNext::Shutdown
        }
        // `Dispatch` is never routed here — it is handled in the select loop.
        ActorCmd::Dispatch { reply, .. } => {
            let _ = reply.send(Err(ActrError::Internal(
                "internal: dispatch reached the barrier path".to_string(),
            )));
            BarrierNext::Continue
        }
    }
}

/// Monotonic fuel accounting for one resident region.
///
/// Wasmtime exposes fuel per Store, not per concurrent guest task. Granting a
/// fresh slice for every admission lets a long-lived invocation steal an
/// unbounded sequence of slices from short-lived siblings. Instead, a busy
/// generation earns fuel only when it reaches a new concurrency high-water
/// mark. The high-water mark resets only after the Store becomes quiescent.
/// This preserves real concurrency while making admission churn fail closed.
#[derive(Debug, Default)]
struct RegionFuelBudget {
    peak_live: usize,
}

impl RegionFuelBudget {
    fn admission_target(&self, current_fuel: u64, live: usize, fuel_per_invocation: u64) -> u64 {
        let newly_earned = live.saturating_sub(self.peak_live);
        let additional =
            fuel_per_invocation.saturating_mul(u64::try_from(newly_earned).unwrap_or(u64::MAX));
        current_fuel
            .saturating_add(additional)
            .min(fuel_cap(fuel_per_invocation, live))
    }

    fn record_admission(&mut self, live: usize) {
        self.peak_live = self.peak_live.max(live);
    }

    fn completion_target(
        &mut self,
        current_fuel: u64,
        live: usize,
        fuel_per_invocation: u64,
    ) -> u64 {
        if live == 0 {
            self.peak_live = 0;
            0
        } else {
            current_fuel.min(fuel_cap(fuel_per_invocation, live))
        }
    }
}

/// Arm one newly admitted invocation without replenishing an existing busy
/// generation unless this admission establishes a new concurrency peak.
fn arm_region_budget(
    accessor: &Accessor<HostState>,
    limits: &WasmRuntimeLimits,
    live: usize,
    budget: &mut RegionFuelBudget,
) -> wasmtime::Result<()> {
    let target = accessor.with(|mut access| {
        let mut store = access.as_context_mut();
        let fuel = budget.admission_target(store.get_fuel()?, live, limits.fuel_per_invocation);
        store.set_fuel(fuel)?;
        store.set_epoch_deadline(epoch_deadline_ticks(limits));
        Ok::<u64, wasmtime::Error>(fuel)
    })?;
    budget.record_admission(live);
    tracing::trace!(
        live,
        target,
        peak = budget.peak_live,
        "armed resident wasm fuel budget"
    );
    Ok(())
}

fn clamp_region_budget(
    accessor: &Accessor<HostState>,
    limits: &WasmRuntimeLimits,
    live: usize,
    budget: &mut RegionFuelBudget,
) -> wasmtime::Result<()> {
    accessor.with(|mut access| {
        let mut store = access.as_context_mut();
        let fuel = budget.completion_target(store.get_fuel()?, live, limits.fuel_per_invocation);
        store.set_fuel(fuel)
    })
}

fn fuel_cap(fuel_per_invocation: u64, live: usize) -> u64 {
    fuel_per_invocation.saturating_mul(u64::try_from(live).unwrap_or(u64::MAX))
}

/// Region-internal lifecycle-hook call (shared by the per-region serial methods
/// and the interleaved barrier path).
async fn run_lifecycle_region(
    accessor: &Accessor<HostState>,
    bindings: &ActrWorkloadGuestV2,
    phase: LifecyclePhase,
    inv: WitInvocationCtx,
) -> wasmtime::Result<Result<(), WitActrError>> {
    let wl = bindings.actr_workload_workload();
    match phase {
        LifecyclePhase::OnStart => wl.call_on_start(accessor, inv).await,
        LifecyclePhase::OnReady => wl.call_on_ready(accessor, inv).await,
        LifecyclePhase::OnStop => wl.call_on_stop(accessor, inv).await,
    }
}

/// Region-internal data-chunk call.
async fn run_data_chunk_region(
    accessor: &Accessor<HostState>,
    bindings: &ActrWorkloadGuestV2,
    wit_chunk: WitDataChunk,
    wit_sender: WitActrId,
    inv: WitInvocationCtx,
) -> wasmtime::Result<Result<(), WitActrError>> {
    bindings
        .actr_workload_workload()
        .call_on_data_chunk(accessor, wit_chunk, wit_sender, inv)
        .await
}

/// Region-internal observation-hook call. Extracted from `call_hook_event` so
/// both the per-region serial path (M4) and the interleaved barrier path (M5)
/// share exactly one copy of the twelve-arm dispatch.
async fn run_hook_region(
    accessor: &Accessor<HostState>,
    bindings: &ActrWorkloadGuestV2,
    event: PackageHookEvent,
    inv: WitInvocationCtx,
) -> wasmtime::Result<()> {
    let wl = bindings.actr_workload_workload();
    match event {
        PackageHookEvent::SignalingConnecting => {
            wl.call_on_signaling_connecting(accessor, inv).await
        }
        PackageHookEvent::SignalingConnected => wl.call_on_signaling_connected(accessor, inv).await,
        PackageHookEvent::SignalingDisconnected => {
            wl.call_on_signaling_disconnected(accessor, inv).await
        }
        PackageHookEvent::WebSocketConnecting(event) => {
            wl.call_on_websocket_connecting(accessor, proto_peer_event_to_wit(event), inv)
                .await
        }
        PackageHookEvent::WebSocketConnected(event) => {
            wl.call_on_websocket_connected(accessor, proto_peer_event_to_wit(event), inv)
                .await
        }
        PackageHookEvent::WebSocketDisconnected(event) => {
            wl.call_on_websocket_disconnected(accessor, proto_peer_event_to_wit(event), inv)
                .await
        }
        PackageHookEvent::WebRtcConnecting(event) => {
            wl.call_on_webrtc_connecting(accessor, proto_peer_event_to_wit(event), inv)
                .await
        }
        PackageHookEvent::WebRtcConnected(event) => {
            wl.call_on_webrtc_connected(accessor, proto_peer_event_to_wit(event), inv)
                .await
        }
        PackageHookEvent::WebRtcDisconnected(event) => {
            wl.call_on_webrtc_disconnected(accessor, proto_peer_event_to_wit(event), inv)
                .await
        }
        PackageHookEvent::CredentialRenewed(event) => {
            wl.call_on_credential_renewed(accessor, proto_credential_event_to_wit(event), inv)
                .await
        }
        PackageHookEvent::CredentialExpiring(event) => {
            wl.call_on_credential_expiring(accessor, proto_credential_event_to_wit(event), inv)
                .await
        }
        PackageHookEvent::MailboxBackpressure(event) => {
            wl.call_on_mailbox_backpressure(accessor, proto_backpressure_event_to_wit(event), inv)
                .await
        }
    }
}

#[cfg(test)]
mod resident_tests {
    use super::*;

    #[test]
    fn inner_wasmtime_error_is_store_fatal() {
        let completion = guest_call_completion::<()>(Err(wasmtime::Error::msg("guest trap")));
        assert!(matches!(completion, GuestCallCompletion::StoreFatal(_)));
    }

    #[test]
    fn short_admission_churn_does_not_refill_a_long_lived_invocation() {
        const SLICE: u64 = 1_000;
        let mut budget = RegionFuelBudget::default();

        // The region is initially seeded with one slice. Reaching width two
        // earns the second and records the high-water mark.
        let mut fuel = budget.admission_target(SLICE, 1, SLICE);
        budget.record_admission(1);
        fuel = budget.admission_target(fuel, 2, SLICE);
        budget.record_admission(2);
        assert_eq!(fuel, 2 * SLICE);

        // The short sibling exits and the long-lived call then consumes most
        // of the remaining shared pool.
        fuel = budget.completion_target(fuel, 1, SLICE);
        assert_eq!(fuel, SLICE);
        fuel = 250;

        // Repeated 1 -> 2 -> 1 churn never earns another slice because the
        // busy generation already reached width two.
        for _ in 0..100 {
            fuel = budget.admission_target(fuel, 2, SLICE);
            budget.record_admission(2);
            assert_eq!(fuel, 250);
            fuel = budget.completion_target(fuel, 1, SLICE);
            assert_eq!(fuel, 250);
        }

        // Full quiescence starts a new generation, so a later independent
        // invocation receives its normal slice.
        assert_eq!(budget.completion_target(fuel, 0, SLICE), 0);
        fuel = budget.admission_target(0, 1, SLICE);
        budget.record_admission(1);
        assert_eq!(fuel, SLICE);
    }
}
