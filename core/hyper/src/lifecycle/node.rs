//! Node runtime inner — holds all running-state fields for an attached node.
//!
//! This module is the internal implementation backing the public
//! `Node<Attached>` / `Node<Registered>` typestate chain defined in
//! `crate::lib`. The struct itself is crate-private; consumers interact with
//! it indirectly through `Node<S>` → `ActrRef` transitions.

use crate::actr_ref::{ActrRef, ActrRefShared};
use crate::ais_client::AisClient;
use crate::context::{BootstrapContextBuilder, RuntimeContext};
use crate::inbound::{DataChunkRegistry, MediaFrameRegistry};
use crate::lifecycle::credential_manager::{
    CredentialManager, HardRebindHandles, RegistrationContext,
};
use crate::lifecycle::dedup::{DEDUP_TTL, DedupOutcome, DedupState, DedupWaiter};
use crate::lifecycle::session_state::{SessionSnapshot, SessionState};
use crate::outbound::Gate;
use crate::transport::HostTransport;
use crate::wire::webrtc::SignalingClient;
#[cfg(feature = "opentelemetry")]
use crate::wire::webrtc::trace::{inject_span_context_to_rpc, set_parent_from_rpc_envelope};
use actr_framework::Bytes;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{
    AIdCredential, ActorResult, ActrError, ActrId, ConnectionNotReadyInfo, Direction, PayloadType,
    RegisterAuthMode, RegisterRequest, RpcEnvelope, TurnCredential, register_response,
};
use actr_runtime::check_acl_permission;
use actr_runtime_mailbox::{DeadLetterQueue, Mailbox};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
#[cfg(feature = "opentelemetry")]
use tracing::Instrument as _;

/// Internal running-state of an attached node.
///
/// Holds every field required to run a workload after `Hyper::attach` has
/// bound a package. Kept private to the crate: external callers use the
/// public `Node<S>` wrappers in `crate::lib` and the `ActrRef` handle
/// returned by `Node::start`.
pub(crate) struct Inner {
    /// Runtime configuration
    pub(crate) config: actr_config::RuntimeConfig,

    /// SQLite persistent mailbox
    pub(crate) mailbox: Arc<dyn Mailbox>,

    /// Dead Letter Queue for poison messages
    pub(crate) dlq: Arc<dyn DeadLetterQueue>,

    /// In-process gate for `Dest::Host` / `Dest::Workload` calls.
    ///
    /// Created in `build()` together with `shell_to_workload` so the inproc
    /// lane is usable as soon as the node exists, even before registration.
    pub(crate) inproc_gate: Gate,

    /// Cross-process gate for `Dest::Peer(_)` calls.
    ///
    /// `None` until `start()` finishes WebRTC / PeerGate initialization. Any
    /// outbound call issued before that point returns `Internal("PeerGate
    /// not initialized yet")` — see `RuntimeContext::select_gate`.
    pub(crate) outproc_gate: Option<Gate>,

    /// DataChunk callback registry shared between the inbound WebRTC / WS
    /// gates (which dispatch into it) and `RuntimeContext`
    /// (register_stream / send_data_chunk).
    pub(crate) data_chunk_registry: Arc<DataChunkRegistry>,

    /// MediaTrack callback registry shared between WebRTC media tracks and
    /// `RuntimeContext` (register_media_track / send_media_sample).
    pub(crate) media_frame_registry: Arc<MediaFrameRegistry>,

    /// Signaling client
    pub(crate) signaling_client: Arc<dyn SignalingClient>,

    /// Actor ID (obtained after startup)
    pub(crate) actor_id: Option<ActrId>,

    /// Actor Credential (obtained after startup, used for subsequent authentication messages)
    pub(crate) credential_state: Option<CredentialState>,

    /// Unified identity/credential snapshot for renewal and hard rebind.
    pub(crate) session_state: Option<SessionState>,

    /// WebRTC coordinator (created after startup)
    pub(crate) webrtc_coordinator: Option<Arc<crate::wire::webrtc::WebRtcCoordinator>>,

    /// Peer transport manager (created after startup)
    pub(crate) peer_transport: Option<Arc<crate::transport::PeerTransport>>,

    /// WebRTC Gate (created after startup)
    pub(crate) webrtc_gate: Option<Arc<crate::wire::webrtc::gate::WebRtcGate>>,

    /// WebSocket Gate (direct-connect mode inbound, optional)
    pub(crate) websocket_gate: Option<Arc<crate::wire::websocket::WebSocketGate>>,

    /// Shell → Workload transport (REQUEST direction)
    ///
    /// Workload receives REQUEST from Shell (zero serialization, direct RpcEnvelope passing)
    pub(crate) shell_to_workload: Option<Arc<HostTransport>>,

    /// Workload → Shell transport (RESPONSE direction)
    ///
    /// Workload sends RESPONSE to Shell (separate pending_requests from Shell's)
    pub(crate) workload_to_shell: Option<Arc<HostTransport>>,

    /// Shutdown token for graceful shutdown
    pub(crate) shutdown_token: CancellationToken,

    /// Packaged manifest.lock.toml content loaded at startup for fingerprint lookups.
    ///
    /// Wrapped in `Arc` so per-request `RuntimeContext` clones only bump a refcount
    /// instead of deep-cloning the dependency vector.
    pub(crate) actr_lock: Option<Arc<actr_config::lock::LockFile>>,
    /// Network event receiver (from NetworkEventHandle)
    pub(crate) network_event_rx:
        Option<tokio::sync::mpsc::Receiver<crate::lifecycle::network_event::NetworkEventRequest>>,

    /// Network event debounce configuration
    pub(crate) network_event_debounce_config:
        Option<crate::lifecycle::network_event::DebounceConfig>,

    /// Request deduplication state (15 s TTL response cache, prevents double-processing on retry)
    pub(crate) dedup_state: Arc<Mutex<DedupState>>,

    /// Verified package manifest for package-backed nodes.
    #[allow(dead_code)]
    pub(crate) package_manifest: Option<actr_pack::PackageManifest>,

    /// Pre-issued registration credential injected by the Hyper layer during
    /// the `Attached → Registered` state transition. `start()` uses it directly
    /// instead of re-registering with the signaling server.
    pub(crate) preregistered_credential: Option<actr_protocol::register_response::RegisterOk>,

    /// Registration context matching `preregistered_credential`, used for
    /// hard rebind if renewal token is no longer usable.
    pub(crate) preregistered_registration_context: Option<RegistrationContext>,

    /// Shared WebSocket direct-connect address map populated by discovery
    ///
    /// Shared with `DefaultWireBuilder` so discovered ws:// URLs can be reused
    /// directly instead of relying on a static url_template
    /// The map is keyed by `ActrId`.
    pub(crate) discovered_ws_addresses:
        Arc<tokio::sync::RwLock<std::collections::HashMap<ActrId, String>>>,

    /// Runtime workload (WASM, dynclib, etc.)
    ///
    /// `handle_incoming` dispatches through this workload.
    ///
    /// The `Mutex` serializes dispatch into a single guest actor instance:
    /// `WasmWorkload::handle` and `DynClibWorkload::handle` both take
    /// `&mut self` because the underlying Wasmtime `Store` / native guest
    /// ABI is single-threaded, so concurrent dispatch through the same
    /// instance would be unsound. Lifecycle hooks also take this lock because
    /// package-backed WASM / dynclib workloads expose them on the same guest
    /// instance; transport and other observation hooks reach linked workloads
    /// through `hook_observer` without holding this lock.
    pub(crate) workload_dispatch: Arc<Mutex<crate::workload::Workload>>,

    /// Optional shell-side observer that receives linked-workload transport /
    /// credential / mailbox hook invocations.
    ///
    /// `None` means "no observer installed"; the built-in tracing defaults
    /// still fire from the event-source wiring sites. When `Some`, hook
    /// invocations are dispatched through `lifecycle::hooks::spawn_hook`
    /// so panics in observer code cannot unwind into the event source.
    #[allow(dead_code)]
    pub(crate) hook_observer: Option<crate::lifecycle::hooks::WorkloadHookObserverRef>,

    /// Queue-length threshold at which the mailbox backpressure
    /// watchdog fires the framework `on_mailbox_backpressure` hook.
    ///
    /// Resolved from [`HyperConfig`] at node construction time so the
    /// runtime loop does not need to hold a reference back to `HyperConfig`.
    pub(crate) mailbox_backpressure_threshold: usize,

    /// Lead time before credential expiry at which the framework fires
    /// the `on_credential_expiring` hook. Resolved from [`HyperConfig`]
    /// at node construction time.
    #[allow(dead_code)]
    pub(crate) credential_expiry_warning: Duration,
}

/// Credential state for shared access between tasks
#[derive(Clone)]
pub struct CredentialState {
    inner: Arc<RwLock<CredentialStateInner>>,
}

#[derive(Clone)]
struct CredentialStateInner {
    credential: AIdCredential,
    expires_at: Option<prost_types::Timestamp>,
    /// HMAC time-limited TURN credential, updated together with credential on registration/renewal
    turn_credential: Option<TurnCredential>,
}

impl CredentialState {
    /// Create a new CredentialState with TURN credential
    pub fn new(
        credential: AIdCredential,
        expires_at: Option<prost_types::Timestamp>,
        turn_credential: Option<TurnCredential>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CredentialStateInner {
                credential,
                expires_at,
                turn_credential,
            })),
        }
    }

    pub async fn credential(&self) -> AIdCredential {
        self.inner.read().await.credential.clone()
    }

    pub async fn expires_at(&self) -> Option<prost_types::Timestamp> {
        self.inner.read().await.expires_at
    }

    /// Get TURN credential (HMAC time-limited credential)
    pub async fn turn_credential(&self) -> Option<TurnCredential> {
        self.inner.read().await.turn_credential.clone()
    }

    /// Update credential and TURN credential
    ///
    /// Called on credential renewal; only overwrites the old TURN credential when the new one is not empty
    pub(crate) async fn update(
        &self,
        credential: AIdCredential,
        expires_at: Option<prost_types::Timestamp>,
        turn_credential: Option<TurnCredential>,
    ) {
        let mut guard = self.inner.write().await;
        guard.credential = credential;
        guard.expires_at = expires_at;
        if turn_credential.is_some() {
            guard.turn_credential = turn_credential;
        }
    }
}

/// Host operation executor - routes guest outbound calls through RuntimeContext
///
/// Called by the workload dispatch path in `handle_incoming`.
async fn host_operation_handler(
    ctx: crate::context::RuntimeContext,
    workload_dispatch: Arc<Mutex<crate::workload::Workload>>,
    pending: crate::workload::HostOperation,
) -> crate::workload::HostOperationResult {
    use crate::workload::{HostOperation, HostOperationResult, decode_dest};
    use actr_framework::guest::dynclib_abi::code as abi_code;
    use actr_framework::{Context as _, Dest};
    use actr_protocol::{DataChunk, PayloadType};

    /// Map `ActrError` to ABI error code, preserving semantics for guest-side discrimination
    fn actr_error_to_code(err: &ActrError) -> i32 {
        match err {
            ActrError::DecodeFailure(_) | ActrError::InvalidArgument(_) => abi_code::PROTOCOL_ERROR,
            _ => abi_code::GENERIC_ERROR,
        }
    }

    match pending {
        HostOperation::CallRaw(req) => {
            match ctx
                .call_raw(
                    &Dest::Peer(req.target),
                    req.route_key,
                    PayloadType::RpcReliable,
                    bytes::Bytes::from(req.payload),
                    30_000,
                )
                .await
            {
                Ok(resp) => HostOperationResult::Bytes(resp.to_vec()),
                Err(e) => {
                    tracing::error!("call_raw routing failed: {e:?}");
                    HostOperationResult::Error(actr_error_to_code(&e))
                }
            }
        }

        HostOperation::Call(req) => {
            let dest = match decode_dest(&req.dest) {
                Some(d) => d,
                None => {
                    tracing::error!(route_key = req.route_key, "call: dest decode failed");
                    return HostOperationResult::Error(abi_code::PROTOCOL_ERROR);
                }
            };
            match ctx
                .call_raw(
                    &dest,
                    req.route_key,
                    PayloadType::RpcReliable,
                    bytes::Bytes::from(req.payload),
                    30_000,
                )
                .await
            {
                Ok(resp) => HostOperationResult::Bytes(resp.to_vec()),
                Err(e) => {
                    tracing::error!("call routing failed: {e:?}");
                    HostOperationResult::Error(actr_error_to_code(&e))
                }
            }
        }

        HostOperation::Tell(req) => {
            let dest = match decode_dest(&req.dest) {
                Some(d) => d,
                None => {
                    tracing::error!(route_key = req.route_key, "tell: dest decode failed");
                    return HostOperationResult::Error(abi_code::PROTOCOL_ERROR);
                }
            };
            match ctx
                .tell_raw(
                    &dest,
                    req.route_key,
                    PayloadType::RpcReliable,
                    bytes::Bytes::from(req.payload),
                )
                .await
            {
                Ok(()) => HostOperationResult::Done,
                Err(e) => {
                    tracing::error!("tell routing failed: {e:?}");
                    HostOperationResult::Error(actr_error_to_code(&e))
                }
            }
        }

        HostOperation::Discover(req) => {
            match ctx.discover_route_candidate(&req.target_type).await {
                Ok(id) => HostOperationResult::Bytes(id.encode_to_vec()),
                Err(e) => {
                    tracing::error!("discover failed: {e:?}");
                    HostOperationResult::Error(actr_error_to_code(&e))
                }
            }
        }

        HostOperation::RegisterStream(req) => {
            let stream_id = req.stream_id;
            let callback_ctx = ctx.clone();
            let callback_workload_dispatch = workload_dispatch.clone();
            match ctx
                .register_stream(stream_id, move |chunk: DataChunk, sender| {
                    let ctx_for_executor = callback_ctx.clone();
                    let workload_dispatch = callback_workload_dispatch.clone();
                    Box::pin(async move {
                        let invocation = crate::workload::InvocationContext {
                            self_id: actr_framework::Context::self_id(&ctx_for_executor).clone(),
                            caller_id: Some(sender.clone()),
                            request_id: format!(
                                "data-chunk:{}:{}",
                                chunk.stream_id, chunk.sequence
                            ),
                        };
                        let call_executor: crate::workload::HostAbiFn =
                            std::sync::Arc::new(move |pending| {
                                let ctx = ctx_for_executor.clone();
                                Box::pin(async move {
                                    stream_callback_host_operation_handler(ctx, pending).await
                                })
                            });
                        let mut guard = workload_dispatch.lock().await;
                        guard
                            .dispatch_data_chunk(chunk, sender, invocation, &call_executor)
                            .await
                    })
                })
                .await
            {
                Ok(()) => HostOperationResult::Done,
                Err(e) => {
                    tracing::error!("register_stream failed: {e:?}");
                    HostOperationResult::Error(actr_error_to_code(&e))
                }
            }
        }

        HostOperation::UnregisterStream(req) => match ctx.unregister_stream(&req.stream_id).await {
            Ok(()) => HostOperationResult::Done,
            Err(e) => {
                tracing::error!("unregister_stream failed: {e:?}");
                HostOperationResult::Error(actr_error_to_code(&e))
            }
        },

        HostOperation::SendDataChunk(req) => {
            let dest = match decode_dest(&req.dest) {
                Some(d) => d,
                None => {
                    tracing::error!("send_data_chunk: dest decode failed");
                    return HostOperationResult::Error(abi_code::PROTOCOL_ERROR);
                }
            };
            let payload_type = match PayloadType::try_from(req.payload_type) {
                Ok(PayloadType::StreamReliable | PayloadType::StreamLatencyFirst) => {
                    PayloadType::try_from(req.payload_type).expect("checked payload type")
                }
                Ok(other) => {
                    tracing::error!(?other, "send_data_chunk: invalid stream payload type");
                    return HostOperationResult::Error(abi_code::PROTOCOL_ERROR);
                }
                Err(_) => {
                    tracing::error!(
                        payload_type = req.payload_type,
                        "send_data_chunk: unknown payload type"
                    );
                    return HostOperationResult::Error(abi_code::PROTOCOL_ERROR);
                }
            };
            match ctx.send_data_chunk(&dest, req.chunk, payload_type).await {
                Ok(()) => HostOperationResult::Done,
                Err(e) => {
                    tracing::error!("send_data_chunk failed: {e:?}");
                    HostOperationResult::Error(actr_error_to_code(&e))
                }
            }
        }
    }
}

fn lifecycle_invocation(
    actor_id: &ActrId,
    request_id: &'static str,
) -> crate::workload::InvocationContext {
    crate::workload::InvocationContext {
        self_id: actor_id.clone(),
        caller_id: None,
        request_id: request_id.to_string(),
    }
}

pub(crate) fn lifecycle_host_abi(
    ctx: crate::context::RuntimeContext,
    workload_dispatch: Arc<Mutex<crate::workload::Workload>>,
) -> crate::workload::HostAbiFn {
    std::sync::Arc::new(move |pending| {
        let ctx = ctx.clone();
        let workload_dispatch = workload_dispatch.clone();
        Box::pin(async move { host_operation_handler(ctx, workload_dispatch, pending).await })
    })
}

async fn stream_callback_host_operation_handler(
    ctx: crate::context::RuntimeContext,
    pending: crate::workload::HostOperation,
) -> crate::workload::HostOperationResult {
    use crate::workload::{HostOperation, HostOperationResult, decode_dest};
    use actr_framework::guest::dynclib_abi::code as abi_code;
    use actr_framework::{Context as _, Dest};
    use actr_protocol::PayloadType;

    fn actr_error_to_code(err: &ActrError) -> i32 {
        match err {
            ActrError::DecodeFailure(_) | ActrError::InvalidArgument(_) => abi_code::PROTOCOL_ERROR,
            _ => abi_code::GENERIC_ERROR,
        }
    }

    match pending {
        HostOperation::CallRaw(req) => {
            match ctx
                .call_raw(
                    &Dest::Peer(req.target),
                    req.route_key,
                    PayloadType::RpcReliable,
                    bytes::Bytes::from(req.payload),
                    30_000,
                )
                .await
            {
                Ok(resp) => HostOperationResult::Bytes(resp.to_vec()),
                Err(e) => HostOperationResult::Error(actr_error_to_code(&e)),
            }
        }
        HostOperation::Call(req) => {
            let dest = match decode_dest(&req.dest) {
                Some(d) => d,
                None => return HostOperationResult::Error(abi_code::PROTOCOL_ERROR),
            };
            match ctx
                .call_raw(
                    &dest,
                    req.route_key,
                    PayloadType::RpcReliable,
                    bytes::Bytes::from(req.payload),
                    30_000,
                )
                .await
            {
                Ok(resp) => HostOperationResult::Bytes(resp.to_vec()),
                Err(e) => HostOperationResult::Error(actr_error_to_code(&e)),
            }
        }
        HostOperation::Tell(req) => {
            let dest = match decode_dest(&req.dest) {
                Some(d) => d,
                None => return HostOperationResult::Error(abi_code::PROTOCOL_ERROR),
            };
            match ctx
                .tell_raw(
                    &dest,
                    req.route_key,
                    PayloadType::RpcReliable,
                    bytes::Bytes::from(req.payload),
                )
                .await
            {
                Ok(()) => HostOperationResult::Done,
                Err(e) => HostOperationResult::Error(actr_error_to_code(&e)),
            }
        }
        HostOperation::Discover(req) => {
            match ctx.discover_route_candidate(&req.target_type).await {
                Ok(id) => HostOperationResult::Bytes(id.encode_to_vec()),
                Err(e) => HostOperationResult::Error(actr_error_to_code(&e)),
            }
        }
        HostOperation::RegisterStream(_) => {
            tracing::error!("register_stream from inside a stream callback is not supported");
            HostOperationResult::Error(abi_code::UNSUPPORTED_OP)
        }
        HostOperation::UnregisterStream(req) => match ctx.unregister_stream(&req.stream_id).await {
            Ok(()) => HostOperationResult::Done,
            Err(e) => HostOperationResult::Error(actr_error_to_code(&e)),
        },
        HostOperation::SendDataChunk(req) => {
            let dest = match decode_dest(&req.dest) {
                Some(d) => d,
                None => return HostOperationResult::Error(abi_code::PROTOCOL_ERROR),
            };
            let payload_type = match PayloadType::try_from(req.payload_type) {
                Ok(PayloadType::StreamReliable | PayloadType::StreamLatencyFirst) => {
                    PayloadType::try_from(req.payload_type).expect("checked payload type")
                }
                Ok(_) | Err(_) => return HostOperationResult::Error(abi_code::PROTOCOL_ERROR),
            };
            match ctx.send_data_chunk(&dest, req.chunk, payload_type).await {
                Ok(()) => HostOperationResult::Done,
                Err(e) => HostOperationResult::Error(actr_error_to_code(&e)),
            }
        }
    }
}

/// Map `ActrError` to a stable, unique numeric code for the wire
/// `ErrorResponse`.  Each variant has a distinct code so
/// `wire_code_to_actr_error` can reconstruct the exact variant on the
/// receiving side.
///
/// Code allocation (100xx namespace to avoid collisions with HTTP):
///
/// | code  | variant            |
/// |-------|--------------------|
/// | 10001 | Unavailable        |
/// | 10002 | TimedOut           |
/// | 10003 | NotFound           |
/// | 10004 | PermissionDenied   |
/// | 10005 | InvalidArgument    |
/// | 10006 | UnknownRoute       |
/// | 10007 | DependencyNotFound |
/// | 10008 | DecodeFailure      |
/// | 10009 | NotImplemented     |
/// | 10010 | Internal           |
/// | 10011 | ConnectionNotReady |
pub(crate) fn protocol_error_to_code(err: &ActrError) -> u32 {
    match err {
        ActrError::Unavailable(_) => 10001,
        ActrError::ConnectionNotReady(_) => 10011,
        ActrError::TimedOut => 10002,
        ActrError::NotFound(_) => 10003,
        ActrError::PermissionDenied(_) => 10004,
        ActrError::InvalidArgument(_) => 10005,
        ActrError::UnknownRoute(_) => 10006,
        ActrError::DependencyNotFound { .. } => 10007,
        ActrError::DecodeFailure(_) => 10008,
        ActrError::NotImplemented(_) => 10009,
        ActrError::Internal(_) => 10010,
    }
}

/// Reconstruct an `ActrError` from a wire code + message pair.
///
/// This is the inverse of `protocol_error_to_code`.  Unknown codes fall
/// back to `ActrError::Unavailable` to avoid silent data loss.
pub(crate) fn wire_code_to_actr_error(code: u32, message: String) -> ActrError {
    match code {
        10001 => ActrError::Unavailable(message),
        10002 => ActrError::TimedOut,
        10003 => ActrError::NotFound(message),
        10004 => ActrError::PermissionDenied(message),
        10005 => ActrError::InvalidArgument(message),
        10006 => ActrError::UnknownRoute(message),
        10007 => ActrError::DependencyNotFound {
            service_name: String::new(),
            message,
        },
        10008 => ActrError::DecodeFailure(message),
        10009 => ActrError::NotImplemented(message),
        10010 => ActrError::Internal(message),
        10011 => ActrError::ConnectionNotReady(ConnectionNotReadyInfo {
            retry_after_ms: parse_connection_not_ready_retry_hint(&message),
        }),
        // Legacy HTTP-ish codes emitted before this scheme was introduced,
        // or any unknown future code: treat as Unavailable.
        _ => ActrError::Unavailable(format!("rpc error {code}: {message}")),
    }
}

fn parse_connection_not_ready_retry_hint(message: &str) -> Option<u64> {
    // ErrorResponse currently carries only a code and display string. Keep this
    // parser narrow and best-effort until the wire error payload grows typed details.
    let marker = "retry_after_ms=Some(";
    let start = message.find(marker)? + marker.len();
    let rest = &message[start..];
    let end = rest.find(')')?;
    rest[..end].parse().ok()
}

impl Inner {
    #[allow(dead_code)]
    pub(crate) fn package_manifest(&self) -> Option<&actr_pack::PackageManifest> {
        self.package_manifest.as_ref()
    }

    /// Network event processing loop (background task)
    ///
    /// # Responsibilities
    /// - Receive network events from Channel
    /// - Delegate to NetworkEventProcessor for handling
    /// - Record processing time and send results
    async fn network_event_loop(
        event_rx: tokio::sync::mpsc::Receiver<crate::lifecycle::network_event::NetworkEventRequest>,
        event_processor: Arc<dyn crate::lifecycle::network_event::NetworkEventProcessor>,
        shutdown_token: CancellationToken,
    ) {
        crate::lifecycle::network_event::run_network_event_reconciler(
            event_rx,
            event_processor,
            shutdown_token,
        )
        .await;
    }

    /// Whether the inbound envelope is an explicit fire-and-forget tell.
    ///
    /// This is the ONLY tell marker: receivers never infer tell-ness from
    /// `timeout_ms == 0` (see the wire contract in `package.proto`).
    pub(crate) fn envelope_is_tell(envelope: &RpcEnvelope) -> bool {
        envelope.direction == Some(Direction::Tell as i32)
    }

    /// Direction-based routing decision for the server dispatch loops.
    ///
    /// Returns `Some(direction)` for the two dispatchable kinds (`Request`,
    /// `Tell`). Everything else — missing, `Unspecified`, `Response`
    /// (mislabel: responses are routed to pending maps by the gates and
    /// never reach a dispatch loop), or unknown future values — yields
    /// `None`, which the loops warn about and drop.
    pub(crate) fn dispatchable_direction(raw_direction: Option<i32>) -> Option<Direction> {
        match crate::wire::direction_for_routing(raw_direction) {
            // Response is routed to pending maps by the gates, never
            // dispatched; missing / Unspecified / unknown are invalid here.
            Ok(direction @ (Direction::Request | Direction::Tell)) => Some(direction),
            _ => None,
        }
    }

    /// Whether an inbound envelope expects a reply, decided solely from its
    /// direction label: `Request` → yes, `Tell` → no (fire-and-forget). Any
    /// non-dispatchable direction yields `None` so the caller can warn + drop.
    ///
    /// Single source of truth for the "does this dispatch expect a response?"
    /// policy shared by the Shell→Guest and mailbox receive loops.
    pub(crate) fn dispatch_expects_response(raw_direction: Option<i32>) -> Option<bool> {
        Self::dispatchable_direction(raw_direction).map(|direction| direction != Direction::Tell)
    }

    /// Build a RESPONSE envelope for a handled request.
    ///
    /// `timeout_ms` is always 0: per the `package.proto` wire contract only
    /// `DIRECTION_REQUEST` carries a positive deadline — RESPONSE (and TELL)
    /// use 0 as documented filler, and receivers MUST ignore it. Centralizing
    /// RESPONSE construction here pins that contract in one tested place.
    pub(crate) fn build_response_envelope(
        request_id: String,
        route_key: String,
        payload: Option<Bytes>,
        error: Option<actr_protocol::ErrorResponse>,
        traceparent: Option<String>,
        tracestate: Option<String>,
    ) -> RpcEnvelope {
        RpcEnvelope {
            request_id,
            route_key,
            payload,
            error,
            direction: Some(Direction::Response as i32),
            traceparent,
            tracestate,
            metadata: Vec::new(),
            timeout_ms: 0,
        }
    }

    fn duplicate_wait_timeout(timeout_ms: i64) -> Duration {
        if timeout_ms > 0 {
            Duration::from_millis(timeout_ms as u64)
        } else {
            DEDUP_TTL
        }
    }

    async fn wait_for_inflight_duplicate(
        mut waiter: DedupWaiter,
        timeout: Duration,
    ) -> ActorResult<Bytes> {
        let wait_for_result = async {
            loop {
                if let Some(result) = waiter.borrow().clone() {
                    return result;
                }

                if waiter.changed().await.is_err() {
                    if let Some(result) = waiter.borrow().clone() {
                        return result;
                    }
                    return Err(ActrError::Unavailable(
                        "duplicate request result unavailable".to_string(),
                    ));
                }
            }
        };

        match tokio::time::timeout(timeout, wait_for_result).await {
            Ok(result) => result,
            Err(_) => Err(ActrError::Unavailable(format!(
                "duplicate request in-flight timed out after {}ms",
                timeout.as_millis()
            ))),
        }
    }

    /// - Single-hop calls: effectively identical
    /// - Multi-hop calls: trace_id spans all hops, request_id per hop
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            skip_all,
            name = "ActrNode.handle_incoming",
            fields(
                actr_id = %self.actor_id.as_ref().map(|id| id.to_string()).unwrap_or_default(),
                route_key = %envelope.route_key,
                request_id = %envelope.request_id,
            )
        )
    )]
    pub async fn handle_incoming(
        &self,
        envelope: RpcEnvelope,
        caller_id: Option<&ActrId>,
    ) -> ActorResult<Bytes> {
        // Log received message
        if let Some(caller) = caller_id {
            tracing::debug!(
                "📨 Handling incoming message: route_key={}, caller={}, request_id={}",
                envelope.route_key,
                caller,
                envelope.request_id
            );
        } else {
            tracing::debug!(
                "📨 Handling incoming message: route_key={}, request_id={}",
                envelope.route_key,
                envelope.request_id
            );
        }

        // 0. Get actor_id early for ACL check
        let actor_id = self.actor_id.as_ref().ok_or_else(|| {
            ActrError::Internal(
                "Actor ID not set - node must be started before handling messages".to_string(),
            )
        })?;

        // 0.1. ACL Permission Check (before processing message)
        let acl_allowed = check_acl_permission(caller_id, actor_id, self.config.acl.as_ref())
            .map_err(|err_msg| ActrError::Internal(format!("ACL check failed: {}", err_msg)))?;

        if !acl_allowed {
            tracing::warn!(
                severity = 5,
                error_category = "acl_denied",
                request_id = %envelope.request_id,
                route_key = %envelope.route_key,
                caller = %caller_id
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "<none>".to_string()),
                "🚫 ACL: Permission denied"
            );

            return Err(ActrError::PermissionDenied(format!(
                "ACL denied: {} is not allowed to call {}",
                caller_id
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string()),
                actor_id
            )));
        }

        // 0.2. Deduplication: return cached response for retried request_ids.
        //
        // TELL envelopes keep their dedup entry (retry protection for
        // RpcReliable's up-to-5 same-request_id attempts), but their
        // duplicate handling differs: no caller is waiting for a reply, so
        // a duplicate arriving while the original is still in flight is
        // dropped immediately instead of blocking the receive loop for
        // `duplicate_wait_timeout` (which maps 0 to the 30 s DEDUP_TTL).
        let is_tell = Self::envelope_is_tell(&envelope);
        let outcome = {
            self.dedup_state
                .lock()
                .await
                .check_or_mark(&envelope.request_id)
        };
        match outcome {
            DedupOutcome::Fresh => {} // proceed normally
            DedupOutcome::InFlight(waiter) => {
                if is_tell {
                    tracing::debug!(
                        request_id = %envelope.request_id,
                        route_key = %envelope.route_key,
                        "duplicate tell in-flight; dropping duplicate immediately (fire-and-forget)"
                    );
                    return Ok(Bytes::new());
                }
                tracing::debug!(
                    request_id = %envelope.request_id,
                    route_key = %envelope.route_key,
                    "duplicate request in-flight; waiting for original result"
                );
                return Self::wait_for_inflight_duplicate(
                    waiter,
                    Self::duplicate_wait_timeout(envelope.timeout_ms),
                )
                .await;
            }
            DedupOutcome::Duplicate(cached) => {
                tracing::debug!(
                    request_id = %envelope.request_id,
                    route_key = %envelope.route_key,
                    "♻️ returning cached response for duplicate request_id"
                );
                return cached;
            }
        }

        // 1. Create Context with caller_id from transport layer
        let credential_state = self.credential_state.clone().ok_or_else(|| {
            ActrError::Internal(
                "Credential not set - node must be started before handling messages".to_string(),
            )
        })?;
        let ctx = self.make_runtime_context(
            actor_id,
            caller_id, // caller_id from transport layer (MessageRecord.from)
            &envelope.request_id,
            &credential_state.credential().await,
        );

        // 2. Dispatch
        let dispatch_ctx = crate::workload::InvocationContext {
            self_id: actor_id.clone(),
            caller_id: caller_id.cloned(),
            request_id: envelope.request_id.clone(),
        };
        let ctx_for_executor = ctx.clone();
        let workload_for_executor = self.workload_dispatch.clone();
        let call_executor: crate::workload::HostAbiFn = std::sync::Arc::new(move |pending| {
            let ctx = ctx_for_executor.clone();
            let workload_dispatch = workload_for_executor.clone();
            Box::pin(async move { host_operation_handler(ctx, workload_dispatch, pending).await })
        });

        let mut guard = self.workload_dispatch.lock().await;
        let result = guard
            .dispatch_envelope(envelope.clone(), ctx.clone(), dispatch_ctx, &call_executor)
            .await;

        match &result {
            Ok(_) => tracing::debug!(
                request_id = %envelope.request_id,
                route_key = %envelope.route_key,
                "✅ Message handled successfully"
            ),
            Err(e) => tracing::error!(
                severity = 6,
                error_category = "handler_error",
                request_id = %envelope.request_id,
                route_key = %envelope.route_key,
                "❌ Message handling failed: {:?}", e
            ),
        }

        // 3. Store completed result in dedup cache before returning.
        //
        // Completion happens on handler Err too, so a retried request (or
        // tell) observes the recorded failure instead of re-running the
        // handler. For a successful TELL the response bytes are never sent,
        // so cache empty bytes instead of retaining the unsent payload for
        // the full DEDUP_TTL.
        let cached = match (&result, is_tell) {
            (Ok(_), true) => Ok(Bytes::new()),
            _ => result.clone(),
        };
        self.dedup_state
            .lock()
            .await
            .complete(&envelope.request_id, cached);

        result
    }

    /// Build a new `Inner` from config and runtime workload.
    ///
    /// This is the internal constructor behind the public node builders and
    /// Hyper package attach helpers.
    pub(crate) async fn build(
        config: actr_config::RuntimeConfig,
        workload: crate::workload::Workload,
        package_manifest: Option<actr_pack::PackageManifest>,
        packaged_lock: Option<actr_config::lock::LockFile>,
        mailbox_backpressure_threshold: usize,
        credential_expiry_warning: Duration,
    ) -> ActorResult<Self> {
        use crate::outbound::{Gate, HostGate};
        use crate::wire::webrtc::{ReconnectConfig, SignalingConfig, WebSocketSignalingClient};

        tracing::info!("🚀 Initializing ActrNode");

        // Initialize Mailbox
        let mailbox_path = config
            .mailbox_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| ":memory:".to_string());

        tracing::info!("📂 Mailbox database path: {}", mailbox_path);

        let mailbox: Arc<dyn actr_runtime_mailbox::Mailbox> = Arc::new(
            actr_runtime_mailbox::SqliteMailbox::new(&mailbox_path)
                .await
                .map_err(|e| {
                    actr_protocol::ActrError::Unavailable(format!("Mailbox init failed: {e}"))
                })?,
        );

        // Initialize Dead Letter Queue
        let dlq_path = if mailbox_path == ":memory:" {
            ":memory:".to_string()
        } else {
            format!("{mailbox_path}.dlq")
        };

        let dlq: Arc<dyn actr_runtime_mailbox::DeadLetterQueue> = Arc::new(
            actr_runtime_mailbox::SqliteDeadLetterQueue::new_standalone(&dlq_path)
                .await
                .map_err(|e| {
                    actr_protocol::ActrError::Unavailable(format!("DLQ init failed: {e}"))
                })?,
        );
        tracing::info!("✅ Dead Letter Queue initialized");

        // Initialize signaling client
        let webrtc_role = if config.webrtc.advanced.prefer_answerer() {
            Some("answer".to_string())
        } else {
            None
        };

        let signaling_config = SignalingConfig {
            server_url: config.signaling_url.clone(),
            connection_timeout: 30,
            heartbeat_interval: 30,
            reconnect_config: ReconnectConfig::default(),
            auth_config: None,
            webrtc_role,
        };

        let client = Arc::new(WebSocketSignalingClient::new(signaling_config));
        client.start_reconnect_manager();
        let signaling_client: Arc<dyn crate::wire::webrtc::SignalingClient> = client;

        // Initialize inproc infrastructure (Shell ↔ Guest)
        let shell_to_workload = Arc::new(HostTransport::new());
        let workload_to_shell = Arc::new(HostTransport::new());
        let inproc_gate = Gate::Host(Arc::new(HostGate::new(shell_to_workload.clone())));

        let data_chunk_registry = Arc::new(DataChunkRegistry::new());
        let media_frame_registry = Arc::new(MediaFrameRegistry::new());

        tracing::info!("✅ Inproc infrastructure initialized (bidirectional Shell ↔ Guest)");

        let actr_lock = if let Some(lock) = packaged_lock {
            tracing::info!(
                "📋 Loaded packaged manifest.lock.toml with {} dependencies",
                lock.dependencies.len()
            );
            Some(Arc::new(lock))
        } else {
            tracing::warn!(
                "⚠️ manifest.lock.toml not found in package. Continuing without dependency fingerprints."
            );
            None
        };

        tracing::info!("✅ ActrNode initialized");

        Ok(Self {
            config,
            mailbox,
            dlq,
            inproc_gate,
            outproc_gate: None, // Populated in start() once WebRTC / PeerGate is ready.
            data_chunk_registry,
            media_frame_registry,
            signaling_client,
            actor_id: None,
            credential_state: None,
            session_state: None,
            webrtc_coordinator: None,
            peer_transport: None,
            webrtc_gate: None,
            websocket_gate: None,
            shell_to_workload: Some(shell_to_workload),
            workload_to_shell: Some(workload_to_shell),
            shutdown_token: CancellationToken::new(),
            actr_lock,
            network_event_rx: None,
            network_event_debounce_config: None,
            dedup_state: Arc::new(Mutex::new(DedupState::new())),
            package_manifest,
            preregistered_credential: None,
            preregistered_registration_context: None,
            discovered_ws_addresses: Arc::new(tokio::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            workload_dispatch: Arc::new(Mutex::new(workload)),
            hook_observer: None,
            mailbox_backpressure_threshold,
            credential_expiry_warning,
        })
    }

    /// Snapshot the current runtime handles into a `BootstrapContextBuilder`.
    ///
    /// The returned builder is cloned into long-lived hook closures and into
    /// `ActrRefShared` so those paths can materialize bootstrap contexts
    /// without retaining a reference back to `Inner`. The snapshot freezes
    /// `outproc_gate` and `actr_lock` at call time — callers that want to
    /// observe a later-initialized `outproc_gate` must rebuild.
    pub(crate) fn bootstrap_ctx_builder(&self) -> BootstrapContextBuilder {
        BootstrapContextBuilder::new(
            self.inproc_gate.clone(),
            self.outproc_gate.clone(),
            self.data_chunk_registry.clone(),
            self.media_frame_registry.clone(),
            self.signaling_client.clone(),
            self.actr_lock.clone(),
            self.discovered_ws_addresses.clone(),
            self.session_state.clone(),
            self.session_state
                .as_ref()
                .and_then(SessionState::generation_sync)
                .unwrap_or(0),
        )
    }

    /// Build a `RuntimeContext` for the per-request dispatch path.
    ///
    /// Unlike `BootstrapContextBuilder::build_bootstrap`, this carries the
    /// envelope's caller identity and request id through into the context.
    pub(crate) fn make_runtime_context(
        &self,
        self_id: &ActrId,
        caller_id: Option<&ActrId>,
        request_id: &str,
        credential: &AIdCredential,
    ) -> RuntimeContext {
        RuntimeContext::new(
            self_id.clone(),
            caller_id.cloned(),
            request_id.to_string(),
            self.inproc_gate.clone(),
            self.outproc_gate.clone(),
            self.data_chunk_registry.clone(),
            self.media_frame_registry.clone(),
            self.signaling_client.clone(),
            credential.clone(),
            self.actr_lock.clone(),
            self.discovered_ws_addresses.clone(),
            self.session_state.clone(),
            self.session_state
                .as_ref()
                .and_then(SessionState::generation_sync)
                .unwrap_or(0),
        )
    }

    /// Create network event processing infrastructure (called on demand, before `start()`).
    ///
    /// # Parameters
    /// - `debounce_ms`: Debounce window in milliseconds. If 0, no debounce.
    ///
    /// # Panics
    /// Panics if called more than once.
    pub fn create_network_event_handle(
        &mut self,
        debounce_ms: u64,
    ) -> crate::lifecycle::NetworkEventHandle {
        if self.network_event_rx.is_some() {
            panic!("create_network_event_handle() can only be called once");
        }

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(100);

        let debounce_config = if debounce_ms > 0 {
            Some(crate::lifecycle::network_event::DebounceConfig {
                window: std::time::Duration::from_millis(debounce_ms),
            })
        } else {
            None
        };

        self.network_event_rx = Some(event_rx);
        self.network_event_debounce_config = debounce_config;

        tracing::info!(
            debounce_ms,
            channel_capacity = 100_u64,
            "network_event.node.handle_created"
        );

        crate::lifecycle::NetworkEventHandle::new(event_tx)
    }

    /// Attach a credential already issued by AIS so that `start()` can skip
    /// the signaling registration step.
    ///
    /// Called by the Hyper layer between `Hyper::register()` and `Hyper::start()`.
    pub fn set_preregistered_credential(&mut self, register_ok: register_response::RegisterOk) {
        tracing::debug!("Pre-registered credential attached; start() will skip AIS registration");
        self.preregistered_credential = Some(register_ok);
    }

    pub(crate) fn set_preregistered_registration_context(&mut self, ctx: RegistrationContext) {
        self.preregistered_registration_context = Some(ctx);
    }

    /// Start the system
    pub async fn start(mut self) -> ActorResult<ActrRef> {
        tracing::info!("🚀 Starting ActrNode");
        tracing::info!("Actr Rust version: {}", env!("CARGO_PKG_VERSION"));

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 1. Build RegisterRequest
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Get ActrType from configuration
        let actr_type = self.config.actr_type().clone();
        tracing::info!("📋 Actor type: {}", actr_type);

        // ServiceSpec is derived by the Hyper layer from the verified package
        // (see `service_spec::calculate_service_spec_from_package`). The raw
        // ActrNode::start() path has no package context and always sends None
        // on its own RegisterRequest; callers that need a spec must go
        // through `Hyper::register()`.
        let service_spec = None;

        // If a WebSocket listen port is configured, build the advertised ws:// address
        // to register with the signaling server so clients can discover it.
        let ws_address = if let Some(port) = self.config.websocket_listen_port {
            let host = self
                .config
                .websocket_advertised_host
                .as_deref()
                .unwrap_or("127.0.0.1");
            Some(format!("ws://{}:{}", host, port))
        } else {
            None
        };

        if let Some(ref addr) = ws_address {
            tracing::info!(
                "📡 Advertising WebSocket address to signaling server: {}",
                addr
            );
        }

        let register_request = RegisterRequest {
            actr_type: actr_type.clone(),
            realm: self.config.realm,
            service_spec,
            acl: self.config.acl.clone(),
            service: None,
            ws_address,
            auth_mode: Some(RegisterAuthMode::Linked as i32),
            ..Default::default()
        };

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 1. Obtain registration info (Hyper pre-injected or AIS HTTP)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let register_ok = if let Some(injected) = self.preregistered_credential.take() {
            tracing::info!(
                "Using Hyper pre-injected registration credential; skipping AIS registration"
            );
            injected
        } else {
            let ais_endpoint = &self.config.ais_endpoint;
            tracing::info!(
                ais_endpoint = %ais_endpoint,
                "Registering actor with AIS via HTTP"
            );
            let mut ais = AisClient::new(ais_endpoint);
            if let Some(ref secret) = self.config.realm_secret {
                ais = ais.with_realm_secret(secret);
            }
            let resp = ais
                .register_linked(register_request.clone())
                .await
                .map_err(|e| ActrError::Unavailable(format!("AIS registration failed: {e}")))?;
            match resp.result {
                Some(register_response::Result::Success(ok)) => {
                    tracing::info!("✅ AIS HTTP registration successful");
                    ok
                }
                Some(register_response::Result::Error(error)) => {
                    tracing::error!(
                        severity = 10,
                        error_category = "registration_error",
                        error_code = error.code,
                        "❌ AIS registration failed: code={}, message={}",
                        error.code,
                        error.message
                    );
                    return Err(ActrError::Unavailable(format!(
                        "AIS registration rejected: {} (code: {})",
                        error.message, error.code
                    )));
                }
                None => {
                    tracing::error!(
                        severity = 10,
                        error_category = "registration_error",
                        "❌ AIS registration response missing result"
                    );
                    return Err(ActrError::Unavailable(
                        "Invalid AIS registration response: missing result".to_string(),
                    ));
                }
            }
        };

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3. Set credential on signaling client, then connect signaling WS
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // The signaling server requires credential params in the WS URL for
        // authentication. We must set actor_id + credential BEFORE connecting
        // so that build_url_with_identity() includes them in the query string.
        let pre_connect_credential_state = {
            let actor_id = register_ok.actr_id.clone();
            let credential_state = CredentialState::new(
                register_ok.credential.clone(),
                register_ok.credential_expires_at,
                Some(register_ok.turn_credential.clone()),
            );
            self.signaling_client.set_actor_id(actor_id).await;
            self.signaling_client
                .set_credential_state(credential_state.clone())
                .await;
            credential_state
        };

        // Install the signaling-side hook callback so that
        // SignalingConnectStart / Connected / Disconnected events flow
        // through the framework tracing defaults and into a
        // user-installed observer. Done BEFORE connect() so the initial
        // attempt produces a SignalingConnectStart event.
        {
            let actor_id = register_ok.actr_id.clone();
            let credential_state = pre_connect_credential_state.clone();
            // Snapshot at this point — outproc_gate is still None here, so
            // signaling-event contexts will carry None for outproc_gate
            // (matching the pre-existing behavior prior to B13 refactor).
            let ctx_builder_snapshot = self.bootstrap_ctx_builder();
            let ctx_builder: crate::lifecycle::hooks::HookContextBuilder = Arc::new(move || {
                let snapshot = ctx_builder_snapshot.clone();
                let actor_id = actor_id.clone();
                let credential_state = credential_state.clone();
                Box::pin(async move {
                    Some(snapshot.build_bootstrap(&actor_id, &credential_state.credential().await))
                })
            });
            let cb = crate::lifecycle::hooks::build_hook_callback(
                self.hook_observer.clone(),
                ctx_builder,
            );
            self.signaling_client.set_hook_callback(cb);
        }

        tracing::info!("📡 Connecting to signaling server (with credential)");
        self.signaling_client
            .connect()
            .await
            .map_err(|e| ActrError::Unavailable(format!("Signaling connect failed: {e}")))?;
        tracing::info!("✅ Connected to signaling server");

        // Collect background task handles so they can be managed by ActrRefShared later.
        let mut task_handles = Vec::new();

        // Node-level hook callback, built inside the registration
        // setup block below and published back out into this wider
        // scope so the mailbox backpressure watchdog can subscribe.
        let node_hook_callback: Option<crate::wire::webrtc::HookCallback>;
        let session_state: SessionState;
        let credential_manager: CredentialManager;

        {
            let actor_id = register_ok.actr_id;
            let credential = register_ok.credential;
            let credential_expires_at = register_ok.credential_expires_at;

            tracing::info!("🆔 Assigned ActrId: {}", actor_id);
            tracing::info!("🔐 Received credential (key_id: {})", credential.key_id);
            tracing::info!(
                "💓 Signaling heartbeat interval: {} seconds",
                register_ok.signaling_heartbeat_interval_secs
            );

            // TurnCredential is a required field; should always be present under normal registration.
            tracing::debug!("TurnCredential received, TURN authentication ready");

            if let Some(expires_at) = &register_ok.credential_expires_at {
                tracing::debug!("⏰ Credential expires at: {}s", expires_at.seconds);
            }

            // Store ActrId and credential state
            self.actor_id = Some(actor_id.clone());
            let credential_state = CredentialState::new(
                credential.clone(),
                credential_expires_at,
                Some(register_ok.turn_credential.clone()),
            );
            self.credential_state = Some(credential_state.clone());
            session_state = SessionState::new(SessionSnapshot {
                actor_id: actor_id.clone(),
                credential,
                credential_expires_at: credential_expires_at.unwrap_or_default(),
                turn_credential: register_ok.turn_credential.clone(),
                renewal_token: register_ok.renewal_token.clone().unwrap_or_default(),
                renewal_token_expires_at: register_ok.renewal_token_expires_at.unwrap_or_default(),
                generation: 1,
            });
            self.session_state = Some(session_state.clone());
            let registration_context = self
                .preregistered_registration_context
                .take()
                .unwrap_or_else(|| RegistrationContext::Linked {
                    request: register_request.clone(),
                    realm_secret: self.config.realm_secret.clone(),
                });
            credential_manager = CredentialManager::new(
                session_state.clone(),
                registration_context,
                self.config.ais_endpoint.clone(),
                self.config.realm_secret.clone(),
            );

            // Build the node-level lifecycle hook callback once: it is
            // reused for the initial `on_credential_renewed`, handed to
            // the heartbeat task for subsequent credential events, and
            // handed to the mailbox backpressure watchdog for
            // `on_mailbox_backpressure` on rising-edge crossings.
            //
            // The signaling layer already has its own callback installed
            // above — this second callback only carries credential and
            // mailbox-backpressure events, so no overlap with the
            // signaling-event plumbing.
            node_hook_callback =
                {
                    let actor_id_for_hook = actor_id.clone();
                    let credential_state_for_hook = credential_state.clone();
                    // Snapshot at this point — outproc_gate is still None
                    // here; credential / mailbox hook contexts inherit that
                    // and therefore cannot issue Dest::Peer(_) calls (same
                    // behavior as before B13 refactor).
                    let ctx_builder_snapshot = self.bootstrap_ctx_builder();
                    let ctx_builder: crate::lifecycle::hooks::HookContextBuilder =
                        Arc::new(move || {
                            let snapshot = ctx_builder_snapshot.clone();
                            let actor_id = actor_id_for_hook.clone();
                            let credential_state = credential_state_for_hook.clone();
                            Box::pin(async move {
                                Some(snapshot.build_bootstrap(
                                    &actor_id,
                                    &credential_state.credential().await,
                                ))
                            })
                        });
                    Some(crate::lifecycle::hooks::build_hook_callback(
                        self.hook_observer.clone(),
                        ctx_builder,
                    ))
                };
            credential_manager
                .install_hook_callback(node_hook_callback.clone())
                .await;

            // Fire `on_credential_renewed` at initial registration: the
            // credential is considered "renewed" from "nothing" to the
            // value just issued by AIS. Subsequent renewals fire the
            // same hook from `lifecycle::heartbeat`.
            if let Some(expires_at) = &register_ok.credential_expires_at {
                let new_expiry = std::time::UNIX_EPOCH
                    + std::time::Duration::from_secs(expires_at.seconds.max(0) as u64);
                if let Some(cb) = node_hook_callback.as_ref() {
                    cb(crate::wire::webrtc::HookEvent::CredentialRenewed { new_expiry }).await;
                } else {
                    tracing::info!(new_expiry = ?new_expiry, "credential renewed");
                }
            }

            // Note: actor_id and credential_state were already set on signaling_client
            // before connect (step 3 above), so reconnect URLs already carry correct auth.

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // 1.3. Inproc transports were filled in during `build()`; nothing
            //      to stage here now that ContextFactory has been removed.
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            tracing::info!("✅ Inproc infrastructure already ready (created in ActrNode::build())");

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // 1.5. Create WebRTC infrastructure
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            tracing::info!("🌐 Initializing WebRTC infrastructure");

            let media_frame_registry = self.media_frame_registry.clone();

            // Create WebRtcCoordinator
            let coordinator = Arc::new(crate::wire::webrtc::WebRtcCoordinator::new(
                actor_id.clone(),
                credential_state.clone(),
                self.signaling_client.clone(),
                self.config.webrtc.clone(),
                media_frame_registry,
            ));

            // Install the WebRTC hook callback — fires
            // WebRtcConnectStart / Connected (with relayed info) /
            // Disconnected HookEvents on every peer state change.
            {
                let actor_id_for_hook = actor_id.clone();
                let credential_state_for_hook = credential_state.clone();
                // Snapshot before outproc_gate is wired up (just below). This
                // preserves the pre-refactor behavior where WebRTC-event
                // hook contexts carry outproc_gate = None.
                let ctx_builder_snapshot = self.bootstrap_ctx_builder();
                let ctx_builder: crate::lifecycle::hooks::HookContextBuilder =
                    Arc::new(move || {
                        let snapshot = ctx_builder_snapshot.clone();
                        let actor_id = actor_id_for_hook.clone();
                        let credential_state = credential_state_for_hook.clone();
                        Box::pin(async move {
                            Some(
                                snapshot.build_bootstrap(
                                    &actor_id,
                                    &credential_state.credential().await,
                                ),
                            )
                        })
                    });
                let cb = crate::lifecycle::hooks::build_hook_callback(
                    self.hook_observer.clone(),
                    ctx_builder,
                );
                coordinator.set_hook_callback(cb);
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // 1.6. Create PeerTransport + PeerGate (new architecture)
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            tracing::info!("🏗️  Creating PeerTransport with WebRTC support");

            // Pre-allocate the pending-requests map so it can be shared between
            // DefaultWireBuilder (for outbound WS response reader tasks) and
            // PeerGate (for request/response matching).
            let pending_requests: crate::outbound::PendingRequestsMap =
                Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));

            // Create DefaultWireBuilder with WebRTC coordinator
            use crate::transport::{DefaultWireBuilder, DefaultWireBuilderConfig};

            // WebSocket channel always enabled: target ws:// address is fully discovered at runtime
            // Direct-connect mode: encode local node ActrId as hex, sent as X-Actr-Node-Id
            let local_id_hex = hex::encode(actor_id.encode_to_vec());
            let wire_builder_config = DefaultWireBuilderConfig {
                local_id_hex,
                enable_webrtc: true,
                enable_websocket: true,
                // Share the discovered_ws_addresses map so that post-discovery calls
                // can use the signaling-provided ws:// URL for this actor node.
                discovered_ws_addresses: self.discovered_ws_addresses.clone(),
                // Pass credential_state so outbound WS handshake carries X-Actr-Credential,
                // enabling peer WebSocketGate to perform Ed25519 signature verification.
                credential_state: Some(credential_state.clone()),
                session_state: Some(session_state.clone()),
                // Pass pending_requests so outbound WS connections spawn reader tasks
                // to deliver server responses back to `send_request_with_type` futures.
                pending_requests: Some(pending_requests.clone()),
            };
            let wire_builder = Arc::new(DefaultWireBuilder::new(
                Some(coordinator.clone()),
                wire_builder_config,
            ));

            // Create PeerTransport
            use crate::transport::PeerTransport;
            let transport_manager = Arc::new(PeerTransport::new(actor_id.clone(), wire_builder));
            self.peer_transport = Some(transport_manager.clone());

            // Create PeerGate with the pre-allocated pending_requests map and WebRTC coordinator.
            use crate::outbound::PeerGate;
            let outproc_gate = Arc::new(PeerGate::with_pending_requests(
                transport_manager.clone(),
                Some(coordinator.clone()),
                pending_requests.clone(),
            ));
            let outproc_gate_enum = Gate::Peer(outproc_gate.clone());
            tracing::info!("PeerTransport + PeerGate initialized");

            let data_chunk_registry = self.data_chunk_registry.clone();

            // Create WebRtcGate with shared pending_requests and DataChunkRegistry
            let gate = Arc::new(crate::wire::webrtc::gate::WebRtcGate::new(
                coordinator.clone(),
                pending_requests,
                data_chunk_registry.clone(),
            ));
            // Set local_id
            gate.set_local_id(actor_id.clone()).await;
            tracing::info!(
                "✅ WebRtcGate created with shared pending_requests and DataChunkRegistry"
            );

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // 1.7. Wire the outproc gate into Inner so subsequent
            //      `make_runtime_context` / `bootstrap_ctx_builder` calls
            //      observe it. All per-request contexts created by
            //      `handle_incoming` go through this field live.
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            tracing::info!("🔧 Wiring outproc_gate into node");
            self.outproc_gate = Some(outproc_gate_enum);
            tracing::info!("✅ Node runtime gates fully initialized (inproc + outproc)");

            // Save references
            self.webrtc_coordinator = Some(coordinator.clone());
            self.webrtc_gate = Some(gate.clone());
            credential_manager
                .install_hard_rebind_handles(HardRebindHandles {
                    signaling_client: self.signaling_client.clone(),
                    credential_state: credential_state.clone(),
                    webrtc_coordinator: Some(coordinator.clone()),
                    webrtc_gate: Some(gate.clone()),
                    peer_transport: Some(transport_manager.clone()),
                })
                .await;
            tracing::info!("✅ WebRTC infrastructure initialized");

            // Fire `on_start` once the runtime context can see the initialized
            // gates, before starting request-accepting/background loops. Its
            // Err/panic aborts Node::start.
            {
                let startup_ctx = self
                    .bootstrap_ctx_builder()
                    .build_bootstrap(&actor_id, &credential_state.credential().await);
                let invocation = lifecycle_invocation(&actor_id, "lifecycle:on_start");
                let call_executor =
                    lifecycle_host_abi(startup_ctx.clone(), self.workload_dispatch.clone());
                let mut workload = self.workload_dispatch.lock().await;
                crate::lifecycle::hooks::call_lifecycle_hook(
                    "on_start",
                    workload.on_start(startup_ctx, invocation, &call_executor),
                )
                .await?;
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // 1.7.6. WebSocket Server (direct-connect mode, optional)
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if let Some(listen_port) = self.config.websocket_listen_port {
                tracing::info!(
                    "🔌 WebSocket direct-connect mode enabled, binding port {}",
                    listen_port
                );
                use crate::key_cache::AisKeyCache;
                use crate::wire::websocket::gate::WsAuthContext;
                use crate::wire::websocket::{WebSocketGate, WebSocketServer};

                // Build AisKeyCache and seed it with the signing key from the registration response
                let ais_key_cache = AisKeyCache::new();
                if !register_ok.signing_pubkey.is_empty() {
                    match ais_key_cache
                        .seed(register_ok.signing_key_id, &register_ok.signing_pubkey)
                        .await
                    {
                        Ok(()) => tracing::info!(
                            key_id = register_ok.signing_key_id,
                            "🔑 AisKeyCache seeded from RegisterOk"
                        ),
                        Err(e) => tracing::warn!(
                            key_id = register_ok.signing_key_id,
                            error = ?e,
                            "AisKeyCache seed failed; WebSocket will reject all inbound connections"
                        ),
                    }
                } else {
                    tracing::warn!(
                        "RegisterOk missing signing_pubkey; WebSocket credential verification will degrade"
                    );
                }

                let auth_ctx = WsAuthContext {
                    ais_key_cache,
                    actor_id: actor_id.clone(),
                    credential_state: credential_state.clone(),
                    signaling_client: self.signaling_client.clone(),
                };

                match WebSocketServer::bind(listen_port).await {
                    Ok((ws_server, conn_rx)) => {
                        ws_server.start(self.shutdown_token.clone());
                        let ws_gate = Arc::new(WebSocketGate::new(
                            conn_rx,
                            outproc_gate.get_pending_requests(),
                            data_chunk_registry.clone(),
                            Some(auth_ctx),
                        ));

                        // Install the WebSocket peer-lifecycle hook.
                        {
                            let actor_id_for_hook = actor_id.clone();
                            let credential_state_for_hook = credential_state.clone();
                            // Snapshot taken after outproc_gate is live: ws
                            // peer-lifecycle hook contexts can issue
                            // Dest::Peer(_) calls.
                            let ctx_builder_snapshot = self.bootstrap_ctx_builder();
                            let ctx_builder: crate::lifecycle::hooks::HookContextBuilder =
                                Arc::new(move || {
                                    let snapshot = ctx_builder_snapshot.clone();
                                    let actor_id = actor_id_for_hook.clone();
                                    let credential_state = credential_state_for_hook.clone();
                                    Box::pin(async move {
                                        Some(snapshot.build_bootstrap(
                                            &actor_id,
                                            &credential_state.credential().await,
                                        ))
                                    })
                                });
                            let cb = crate::lifecycle::hooks::build_hook_callback(
                                self.hook_observer.clone(),
                                ctx_builder,
                            );
                            ws_gate.set_hook_callback(cb);
                        }

                        self.websocket_gate = Some(ws_gate);
                        tracing::info!(
                            "✅ WebSocketServer + WebSocketGate initialized (credential auth enabled)"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "❌ Failed to bind WebSocket server on port {}: {:?}",
                            listen_port,
                            e
                        );
                    }
                }
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // 1.7.5. Create shared state for credential management
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // Shared credential state initialized above; reused across tasks

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // 1.8. Spawn heartbeat task (periodic Ping to signaling server)
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            {
                let shutdown = self.shutdown_token.clone();
                let client = self.signaling_client.clone();
                let actor_id_for_heartbeat = actor_id.clone();
                let credential_state_for_heartbeat = credential_state.clone();
                let mailbox_for_heartbeat = self.mailbox.clone();
                let register_request_for_heartbeat = register_request.clone();
                let credential_manager_for_heartbeat = credential_manager.clone();
                let webrtc_coordinator_for_heartbeat = self.webrtc_coordinator.clone();
                let webrtc_gate_for_heartbeat = self.webrtc_gate.clone();

                // Use interval from registration response, default to 30s
                let heartbeat_interval_secs = register_ok.signaling_heartbeat_interval_secs;
                let heartbeat_interval = if heartbeat_interval_secs > 0 {
                    Duration::from_secs(heartbeat_interval_secs as u64)
                } else {
                    Duration::from_secs(30)
                };
                let ais_endpoint_for_heartbeat = self.config.ais_endpoint.clone();
                let realm_secret_for_heartbeat = self.config.realm_secret.clone();
                let heartbeat_handle = tokio::spawn(crate::lifecycle::heartbeat::heartbeat_task(
                    shutdown,
                    client,
                    actor_id_for_heartbeat,
                    credential_state_for_heartbeat,
                    mailbox_for_heartbeat,
                    heartbeat_interval,
                    register_request_for_heartbeat,
                    ais_endpoint_for_heartbeat,
                    realm_secret_for_heartbeat,
                    Some(credential_manager_for_heartbeat),
                    node_hook_callback.clone(),
                    webrtc_coordinator_for_heartbeat,
                    webrtc_gate_for_heartbeat,
                ));
                task_handles.push(heartbeat_handle);
            }
            tracing::info!(
                "✅ Heartbeat task started (interval: {}s)",
                register_ok.signaling_heartbeat_interval_secs
            );

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // 1.8.5. Spawn network event processing loop
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if let Some(event_rx) = self.network_event_rx.take() {
                use crate::lifecycle::network_event::DefaultNetworkEventProcessor;

                // Create DefaultNetworkEventProcessor
                // If debounce config exists, use new_with_debounce
                let event_processor =
                    if let Some(config) = self.network_event_debounce_config.clone() {
                        Arc::new(
                            DefaultNetworkEventProcessor::new_with_debounce_and_peer_transport(
                                self.signaling_client.clone(),
                                self.webrtc_coordinator.clone(),
                                config,
                                self.peer_transport.clone(),
                            ),
                        )
                    } else {
                        Arc::new(DefaultNetworkEventProcessor::new_with_peer_transport(
                            self.signaling_client.clone(),
                            self.webrtc_coordinator.clone(),
                            self.peer_transport.clone(),
                        ))
                    };

                let shutdown = self.shutdown_token.clone();
                let network_event_handle = tokio::spawn(async move {
                    Self::network_event_loop(event_rx, event_processor, shutdown).await;
                });
                task_handles.push(network_event_handle);
                tracing::info!("network_event.node.loop_started");
            } else {
                tracing::debug!("network_event.node.loop_not_started_no_handle");
            }

            {
                // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                // 1.9. Spawn dedicated Unregister task (best-effort, with timeout)
                // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                //
                // This task:
                // - Waits for shutdown_token to be cancelled (e.g., wait_for_ctrl_c_and_shutdown)
                // - Then sends UnregisterRequest via signaling client with a timeout
                //
                // NOTE: we push its JoinHandle into task_handles so it can be aborted
                // by ActrRefShared::Drop if needed.
                let shutdown = self.shutdown_token.clone();
                let client = self.signaling_client.clone();
                let actor_id_for_unreg = actor_id.clone();
                let credential_state_for_unreg = credential_state.clone();
                let webrtc_coordinator = self.webrtc_coordinator.clone();

                let unregister_handle = tokio::spawn(async move {
                    // Wait for shutdown signal
                    shutdown.cancelled().await;
                    tracing::info!(
                        "📡 Shutdown signal received, sending UnregisterRequest for Actor {}",
                        actor_id_for_unreg
                    );

                    // 1. Close all WebRTC peer connections first (if any)
                    if let Some(coord) = webrtc_coordinator {
                        if let Err(e) = coord.close_all_peers().await {
                            tracing::warn!(
                                "⚠️ Failed to close all WebRTC peers before UnregisterRequest: {}",
                                e
                            );
                        } else {
                            tracing::info!("✅ All WebRTC peers closed before UnregisterRequest");
                        }
                    } else {
                        tracing::debug!(
                            "WebRTC coordinator not found before UnregisterRequest (no WebRTC?)"
                        );
                    }

                    // 2. Then send UnregisterRequest with a timeout (e.g. 5 seconds)
                    let result = tokio::time::timeout(
                        Duration::from_secs(5),
                        client.send_unregister_request(
                            actor_id_for_unreg.clone(),
                            credential_state_for_unreg.credential().await,
                            Some("Graceful shutdown".to_string()),
                        ),
                    )
                    .await;
                    tracing::info!("UnregisterRequest result: {:?}", result);
                    match result {
                        Ok(Ok(_)) => {
                            tracing::info!(
                                "✅ UnregisterRequest sent to signaling server for Actor {}",
                                actor_id_for_unreg
                            );
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(
                                "⚠️ Failed to send UnregisterRequest for Actor {}: {}",
                                actor_id_for_unreg,
                                e
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                "⚠️ UnregisterRequest timeout (5s) for Actor {}",
                                actor_id_for_unreg
                            );
                        }
                    }
                });

                task_handles.push(unregister_handle);
            }
        } // end registration setup block

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 2. Transport layer initialization (completed via WebRTC infrastructure)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        tracing::info!("✅ Transport layer initialized via WebRTC infrastructure");

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3.1 Convert to Arc (before starting background loops)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Clone actor_id before moving self into Arc
        let actor_id = self
            .actor_id
            .as_ref()
            .ok_or_else(|| ActrError::Internal("Actor ID not set".to_string()))?
            .clone();
        // Snapshot now that outproc_gate has been wired above; this builder
        // is shared between on_start / on_stop hooks and the ActrRefShared
        // handle returned to the caller.
        let mut bootstrap_ctx_builder = self.bootstrap_ctx_builder();
        bootstrap_ctx_builder.set_session_state(Some(session_state.clone()));
        bootstrap_ctx_builder.set_generation(session_state.generation().await);
        let credential_state = self
            .credential_state
            .clone()
            .expect("CredentialState must be initialized in start()");
        let session_state = credential_manager.session_state();
        let shutdown_token = self.shutdown_token.clone();
        let node_ref = Arc::new(self);

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3.2. Register workload-level stop hook.
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        {
            let node = node_ref.clone();
            let actor_id = actor_id.clone();
            let credential_state = credential_state.clone();
            let shutdown = shutdown_token.clone();
            let on_stop_handle = tokio::spawn(async move {
                shutdown.cancelled().await;
                let stop_ctx = node
                    .bootstrap_ctx_builder()
                    .build_bootstrap(&actor_id, &credential_state.credential().await);
                let invocation = lifecycle_invocation(&actor_id, "lifecycle:on_stop");
                let call_executor =
                    lifecycle_host_abi(stop_ctx.clone(), node.workload_dispatch.clone());
                let mut workload = node.workload_dispatch.lock().await;
                if let Err(e) = crate::lifecycle::hooks::call_lifecycle_hook(
                    "on_stop",
                    workload.on_stop(stop_ctx, invocation, &call_executor),
                )
                .await
                {
                    tracing::warn!(error = %e, "workload on_stop returned Err");
                }
            });
            task_handles.push(on_stop_handle);
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 3.5. Start WebRTC background loops
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        tracing::info!("🚀 Starting WebRTC background loops");

        // Start WebRtcCoordinator signaling loop
        if let Some(coordinator) = &node_ref.webrtc_coordinator {
            coordinator.clone().start().await.map_err(|e| {
                ActrError::Unavailable(format!("WebRtcCoordinator start failed: {e}"))
            })?;
            tracing::info!("✅ WebRtcCoordinator signaling loop started");
        }

        // Start WebRtcGate message receive loop (route to Mailbox)
        if let Some(gate) = &node_ref.webrtc_gate {
            gate.start_receive_loop(node_ref.mailbox.clone())
                .await
                .map_err(|e| {
                    ActrError::Unavailable(format!("WebRtcGate receive loop start failed: {e}"))
                })?;
            tracing::info!("✅ WebRtcGate → Mailbox routing started");
        }

        // Start WebSocketGate message receive loop (route to Mailbox, direct-connect mode)
        if let Some(ws_gate) = &node_ref.websocket_gate {
            ws_gate
                .start_receive_loop(node_ref.mailbox.clone())
                .await
                .map_err(|e| {
                    ActrError::Unavailable(format!("WebSocketGate receive loop start failed: {e}"))
                })?;
            tracing::info!("✅ WebSocketGate → Mailbox routing started");
        }
        tracing::info!("✅ WebRTC background loops started");

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 4.6. Start Inproc receive loop (Shell → Guest)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if let Some(shell_to_workload) = &node_ref.shell_to_workload {
            tracing::info!("🔄 Starting Inproc receive loop (Shell → Guest)");
            // Start Guest receive loop (Shell → Guest REQUEST)
            if let Some(workload_to_shell) = &node_ref.workload_to_shell {
                let node = node_ref.clone();
                let request_rx_lane = shell_to_workload
                    .get_lane(PayloadType::RpcReliable, None)
                    .await
                    .map_err(|e| {
                        ActrError::Unavailable(format!("Failed to get guest receive lane: {e}"))
                    })?;
                let response_tx = workload_to_shell.clone();
                let shutdown = shutdown_token.clone();

                let inproc_handle = tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                tracing::info!("📭 Guest receive loop (Shell → Guest) received shutdown signal");
                                break;
                            }
                            envelope_result = request_rx_lane.recv_envelope() => {
                                match envelope_result {
                                    Ok(envelope) => {
                                        let request_id = envelope.request_id.clone();
                                        tracing::debug!("📨 Guest received REQUEST from Shell: request_id={}", request_id);

                                        // Route strictly on the explicit direction label:
                                        // Request runs the handler and sends a response;
                                        // Tell runs the handler for its side effects but
                                        // sends nothing — an unwanted reply becomes an
                                        // orphan response on the caller (#262). Anything
                                        // else (missing / Unspecified / Response / unknown)
                                        // is invalid on a dispatch lane: warn and drop.
                                        // timeout_ms is never consulted for tell-ness.
                                        let expects_response = match Inner::dispatch_expects_response(envelope.direction) {
                                            Some(v) => v,
                                            None => {
                                                tracing::warn!(
                                                    request_id = %request_id,
                                                    route_key = %envelope.route_key,
                                                    direction = ?envelope.direction,
                                                    "rpc.invalid_direction_dropped: non-dispatchable RpcEnvelope.direction on Shell → Guest lane; dropping"
                                                );
                                                continue;
                                            }
                                        };

                                        // Extract and set tracing context from envelope
                                        #[cfg(feature = "opentelemetry")]
                                        let span = {
                                            let actr_id_str = node.actor_id.as_ref().map(|id| id.to_string()).unwrap_or_default();
                                            let span = tracing::info_span!("ActrNode.lane_receive", actr_id = %actr_id_str, request_id = %request_id);
                                            set_parent_from_rpc_envelope(&span, &envelope);
                                            span
                                        };

                                        // Shell calls have no caller_id (local process communication)
                                        let handle_incoming_fut = node.handle_incoming(envelope.clone(), None);
                                        #[cfg(feature = "opentelemetry")]
                                        let handle_incoming_fut = handle_incoming_fut.instrument(span.clone());

                                        match handle_incoming_fut.await {
                                            Ok(response_bytes) => {
                                                if expects_response {
                                                    // Send RESPONSE back via workload_to_shell
                                                    // Keep same route_key (no prefix needed - separate channels!)
                                                    #[cfg_attr(not(feature = "opentelemetry"), allow(unused_mut))]
                                                    let mut response_envelope = Inner::build_response_envelope(
                                                        request_id.clone(),
                                                        envelope.route_key.clone(),
                                                        Some(response_bytes),
                                                        None,
                                                        None,
                                                        None,
                                                    );
                                                    // Inject tracing context
                                                    #[cfg(feature = "opentelemetry")]
                                                    inject_span_context_to_rpc(&span, &mut response_envelope);

                                                    // Send via Guest → Shell channel
                                                    let send_response_fut = response_tx.send_message(PayloadType::RpcReliable, None, response_envelope);
                                                    #[cfg(feature = "opentelemetry")]
                                                    let send_response_fut = send_response_fut.instrument(span.clone());
                                                    if let Err(e) = send_response_fut.await {
                                                        tracing::error!(
                                                            severity = 7,
                                                            error_category = "transport_error",
                                                            request_id = %request_id,
                                                            "❌ Failed to send RESPONSE to Shell: {:?}",
                                                            e
                                                        );
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    severity = 6,
                                                    error_category = "handler_error",
                                                    request_id = %request_id,
                                                    route_key = %envelope.route_key,
                                                    "❌ Guest message handling failed: {:?}",
                                                    e
                                                );

                                                // Keep the local error log above for every failure,
                                                // but skip sending an error envelope for a `tell`:
                                                // the caller registered no pending entry to receive it.
                                                if expects_response {
                                                    // Send error response (system-level error on envelope)
                                                    let error_response = actr_protocol::ErrorResponse {
                                                        code: protocol_error_to_code(&e),
                                                        message: e.to_string(),
                                                    };
                                                    #[cfg_attr(not(feature = "opentelemetry"), allow(unused_mut))]
                                                    let mut error_envelope = Inner::build_response_envelope(
                                                        request_id.clone(),
                                                        envelope.route_key.clone(),
                                                        None,
                                                        Some(error_response),
                                                        envelope.traceparent.clone(),
                                                        envelope.tracestate.clone(),
                                                    );
                                                    // Inject tracing context
                                                    #[cfg(feature = "opentelemetry")]
                                                    inject_span_context_to_rpc(&span, &mut error_envelope);

                                                    let send_error_response_fut = response_tx.send_message(PayloadType::RpcReliable, None, error_envelope);
                                                    #[cfg(feature = "opentelemetry")]
                                                    let send_error_response_fut = send_error_response_fut.instrument(span);
                                                    if let Err(send_err) = send_error_response_fut.await {
                                                        tracing::error!(
                                                            severity = 7,
                                                            error_category = "transport_error",
                                                            request_id = %request_id,
                                                            "❌ Failed to send ERROR response to Shell: {:?}",
                                                            send_err
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            severity = 8,
                                            error_category = "transport_error",
                                            "❌ Failed to receive from Shell → Guest lane: {:?}",
                                            e
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    tracing::info!("✅ Guest receive loop (Shell → Guest) terminated gracefully");
                });
                task_handles.push(inproc_handle);
            }
        }
        tracing::info!("✅ Guest receive loop (Shell → Guest REQUEST) started");

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 4.7. Start Shell receive loop (Guest → Shell RESPONSE)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        tracing::info!("🔄 Starting Shell receive loop (Guest → Shell RESPONSE)");
        if let Some(workload_to_shell) = &node_ref.workload_to_shell {
            // Start Shell receive loop (Guest → Shell RESPONSE)
            if let Some(shell_to_workload) = &node_ref.shell_to_workload {
                let response_rx_lane = workload_to_shell
                    .get_lane(PayloadType::RpcReliable, None)
                    .await
                    .map_err(|e| {
                        ActrError::Unavailable(format!("Failed to get shell receive lane: {e}"))
                    })?;
                let request_mgr = shell_to_workload.clone();
                let shutdown = shutdown_token.clone();

                let shell_receive_handle = tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                tracing::info!("📭 Shell receive loop (Guest → Shell) received shutdown signal");
                                break;
                            }
                            envelope_result = response_rx_lane.recv_envelope() => {
                                match envelope_result {
                                    Ok(envelope) => {
                                        tracing::debug!(
                                            "📨 Shell received RESPONSE from Guest: request_id={}",
                                            envelope.request_id
                                        );

                                        // Check if response is success or error
                                        match (envelope.payload, envelope.error) {
                                            (Some(payload), None) => {
                                                // Success response
                                                if let Err(e) = request_mgr
                                                    .complete_response(&envelope.request_id, payload)
                                                    .await
                                                {
                                                    tracing::warn!(
                                                        severity = 4,
                                                        error_category = "orphan_response",
                                                        request_id = %envelope.request_id,
                                                        "⚠️  No pending request found for response: {:?}",
                                                        e
                                                    );
                                                }
                                            }
                                            (None, Some(error)) => {
                                                // Error response — reconstruct the precise ActrError variant
                                                // from the wire code so binding-visible classification
                                                // (UnknownRoute / PermissionDenied / TimedOut / …) is preserved
                                                // instead of collapsing every error into Unavailable.
                                                let actr_err = wire_code_to_actr_error(error.code, error.message);
                                                if let Err(e) = request_mgr
                                                    .complete_error(&envelope.request_id, actr_err)
                                                    .await
                                                {
                                                    tracing::warn!(
                                                        severity = 4,
                                                        error_category = "orphan_response",
                                                        request_id = %envelope.request_id,
                                                        "⚠️  No pending request found for error response: {:?}",
                                                        e
                                                    );
                                                }
                                            }
                                            _ => {
                                                tracing::error!(
                                                    severity = 7,
                                                    error_category = "protocol_error",
                                                    request_id = %envelope.request_id,
                                                    "❌ Invalid RpcEnvelope: both payload and error are present or both absent"
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            severity = 8,
                                            error_category = "transport_error",
                                            "❌ Failed to receive from Guest → Shell lane: {:?}",
                                            e
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    tracing::info!("✅ Shell receive loop (Guest → Shell) terminated gracefully");
                });
                task_handles.push(shell_receive_handle);
            }
        }
        tracing::info!("✅ Shell receive loop (Guest → Shell RESPONSE) started");

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 4.9. Mailbox backpressure watchdog
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        //
        // Emits the framework `on_mailbox_backpressure` hook once per
        // rising-edge crossing of the configured threshold.
        //
        // Preferred path: a push-based notification from the mailbox
        // backend via [`Mailbox::set_depth_observer`], which runs
        // synchronously on every enqueue and has zero worst-case delay.
        //
        // Fallback path: mailbox backends without depth support (or
        // which can't cheaply compute depth on every enqueue) keep
        // using a 1 Hz poll of [`Mailbox::status`].
        let backpressure_threshold = node_ref.mailbox_backpressure_threshold;
        {
            use std::sync::atomic::{AtomicBool, Ordering};
            let mailbox = node_ref.mailbox.clone();
            let shutdown = shutdown_token.clone();
            let hook_cb = node_hook_callback.clone();
            let triggered = Arc::new(AtomicBool::new(false));

            // Shared rising-edge state + hook-firing closure used by
            // both the push and polling code paths.
            let fire_if_rising = {
                let triggered = triggered.clone();
                let hook_cb = hook_cb.clone();
                Arc::new(move |queue_len: usize| {
                    if queue_len >= backpressure_threshold {
                        if !triggered.swap(true, Ordering::AcqRel) {
                            if let Some(cb) = hook_cb.as_ref() {
                                let cb = cb.clone();
                                tokio::spawn(async move {
                                    cb(crate::wire::webrtc::HookEvent::MailboxBackpressure {
                                        queue_len,
                                        threshold: backpressure_threshold,
                                    })
                                    .await;
                                });
                            } else {
                                tracing::warn!(
                                    queue_len,
                                    threshold = backpressure_threshold,
                                    "mailbox backpressure",
                                );
                            }
                        }
                    } else if triggered.swap(false, Ordering::AcqRel) {
                        tracing::info!(
                            queue_len,
                            threshold = backpressure_threshold,
                            "mailbox backpressure cleared",
                        );
                    }
                })
            };

            // Try the push path first. The observer installs only if
            // the backend supports it; otherwise `installed` is `false`
            // and we fall through to polling.
            struct EnqueueObserver {
                fire: Arc<dyn Fn(usize) + Send + Sync + 'static>,
            }
            impl actr_runtime_mailbox::MailboxDepthObserver for EnqueueObserver {
                fn on_depth_change(&self, queued_messages: usize) {
                    (self.fire)(queued_messages);
                }
            }

            let installed = {
                let observer: Arc<dyn actr_runtime_mailbox::MailboxDepthObserver> =
                    Arc::new(EnqueueObserver {
                        fire: fire_if_rising.clone(),
                    });
                mailbox.set_depth_observer(observer)
            };

            if installed {
                tracing::debug!("mailbox backpressure watchdog: push notifications enabled");
            } else {
                tracing::debug!(
                    "mailbox backpressure watchdog: backend does not support push, falling back to 1 Hz polling"
                );
                let mailbox_for_poll = mailbox.clone();
                let shutdown_for_poll = shutdown.clone();
                let fire_for_poll = fire_if_rising.clone();
                let watchdog_handle = tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(Duration::from_secs(1));
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tokio::select! {
                            _ = shutdown_for_poll.cancelled() => {
                                tracing::debug!(
                                    "mailbox backpressure watchdog shutting down"
                                );
                                break;
                            }
                            _ = ticker.tick() => {
                                let status = match mailbox_for_poll.status().await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        tracing::debug!(?e, "mailbox status poll failed");
                                        continue;
                                    }
                                };
                                fire_for_poll(status.queued_messages as usize);
                            }
                        }
                    }
                });
                task_handles.push(watchdog_handle);
            }
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // 5. Start Mailbox processing loop (State Path)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        tracing::info!("🔄 Starting Mailbox processing loop (State Path)");
        {
            let node = node_ref.clone();
            let mailbox = node_ref.mailbox.clone();
            let webrtc_gate = node_ref.webrtc_gate.clone();
            let ws_gate = node_ref.websocket_gate.clone();
            let shutdown = shutdown_token.clone();

            let mailbox_handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        // Listen for shutdown signal
                        _ = shutdown.cancelled() => {
                            tracing::info!("📭 Mailbox loop received shutdown signal");
                            break;
                        }
                        // Dequeue messages (by priority)
                        result = mailbox.dequeue() => {
                            match result {
                                Ok(messages) => {
                                    if messages.is_empty() {
                                        // Queue empty, sleep briefly
                                        tokio::time::sleep(Duration::from_millis(10)).await;
                                        continue;
                                    }
                                    tracing::debug!("📬 Mailbox dequeue: {} messages", messages.len());

                                    // Process messages one by one
                                    for msg_record in messages {
                                        // Deserialize RpcEnvelope (Protobuf)
                                        match RpcEnvelope::decode(&msg_record.payload[..]) {
                                            Ok(envelope) => {
                                                let request_id = envelope.request_id.clone();
                                                let queue_latency_ms = (chrono::Utc::now() - msg_record.created_at).num_milliseconds();
                                                tracing::info!(request_id = %request_id, queue_latency_ms = queue_latency_ms, "rpc.mailbox.dequeued");

                                                tracing::debug!("📦 Processing message: request_id={}", request_id);

                                                // Route strictly on the explicit direction label:
                                                // Request runs the handler and sends a response;
                                                // Tell runs the handler but suppresses the reply —
                                                // an unwanted reply becomes an orphan response on
                                                // the caller (#262). Anything else (missing /
                                                // Unspecified / Response / unknown) is invalid in
                                                // the dispatch mailbox: warn, ack (to avoid
                                                // redelivery), and drop. timeout_ms is never
                                                // consulted for tell-ness.
                                                let expects_response = match Inner::dispatch_expects_response(envelope.direction) {
                                                    Some(v) => v,
                                                    None => {
                                                        tracing::warn!(
                                                            request_id = %request_id,
                                                            route_key = %envelope.route_key,
                                                            direction = ?envelope.direction,
                                                            "rpc.invalid_direction_dropped: non-dispatchable RpcEnvelope.direction in mailbox; dropping"
                                                        );
                                                        if let Err(e) = mailbox.ack(msg_record.id).await {
                                                            tracing::error!(
                                                                severity = 9,
                                                                error_category = "mailbox_error",
                                                                request_id = %request_id,
                                                                message_id = %msg_record.id,
                                                                "❌ Mailbox ACK failed for dropped envelope: {:?}",
                                                                e
                                                            );
                                                        }
                                                        continue;
                                                    }
                                                };

                                                #[cfg(feature = "opentelemetry")]
                                                let span = {
                                                    let actr_id_str = node.actor_id.as_ref().map(|id| id.to_string()).unwrap_or_default();
                                                    let span = tracing::info_span!("ActrNode.mailbox_receive", actr_id = %actr_id_str, request_id = %request_id, queue_wait_ms = queue_latency_ms);
                                                    set_parent_from_rpc_envelope(&span, &envelope);
                                                    span
                                                };

                                                // Decode caller_id from MessageRecord.from (transport layer)
                                                let caller_id_result = ActrId::decode(&msg_record.from[..]);
                                                let caller_id_ref = caller_id_result.as_ref().ok();

                                                if caller_id_ref.is_none() {
                                                    tracing::warn!(
                                                        request_id = %request_id,
                                                        "⚠️  Failed to decode caller_id from MessageRecord.from"
                                                    );
                                                }

                                                // Call handle_incoming with caller_id from transport layer
                                                let handle_incoming_fut = node.handle_incoming(envelope.clone(), caller_id_ref);
                                                #[cfg(feature = "opentelemetry")]
                                                let handle_incoming_fut = handle_incoming_fut.instrument(span.clone());

                                                /// Send `response_envelope` back to `caller` via the
                                                /// best available transport.
                                                ///
                                                /// Priority: inbound WebSocket connection (if caller
                                                /// dialled us directly) → WebRTC gate.  Returns the
                                                /// first transport error encountered, if any.
                                                async fn send_envelope_to_caller(
                                                    ws_gate: &Option<Arc<crate::wire::websocket::WebSocketGate>>,
                                                    webrtc_gate: &Option<Arc<crate::wire::webrtc::gate::WebRtcGate>>,
                                                    caller: &ActrId,
                                                    response_envelope: RpcEnvelope,
                                                    request_id: &str,
                                                ) {
                                                    // 1. Try inbound WebSocket connection first.
                                                    if let Some(wsg) = ws_gate {
                                                        match wsg.send_response(caller, response_envelope.clone()).await {
                                                            Ok(true) => return, // sent successfully
                                                            Ok(false) => {
                                                                tracing::debug!(
                                                                    request_id = request_id,
                                                                    caller = %caller,
                                                                    "No inbound WS connection for caller; falling back to WebRTC gate"
                                                                );
                                                            }
                                                            Err(e) => {
                                                                tracing::warn!(
                                                                    severity = 5,
                                                                    error_category = "transport_error",
                                                                    request_id = request_id,
                                                                    "WebSocketGate send_response failed, falling back: {:?}", e
                                                                );
                                                            }
                                                        }
                                                    }

                                                    // 2. Fall back to WebRTC gate.
                                                    if let Some(gate) = webrtc_gate {
                                                        if let Err(e) = gate.send_response(caller, response_envelope).await {
                                                            tracing::error!(
                                                                severity = 7,
                                                                error_category = "transport_error",
                                                                request_id = request_id,
                                                                "❌ WebRtcGate send_response failed: {:?}", e
                                                            );
                                                        }
                                                    } else {
                                                        tracing::error!(
                                                            severity = 7,
                                                            error_category = "transport_error",
                                                            request_id = request_id,
                                                            "❌ No gate available to send response"
                                                        );
                                                    }
                                                }

                                                match handle_incoming_fut.await {
                                                    Ok(response_bytes) => {
                                                        match caller_id_result {
                                                            Ok(caller) if expects_response => {
                                                                // Construct response RpcEnvelope (reuse request_id!)
                                                                #[cfg_attr(not(feature = "opentelemetry"), allow(unused_mut))]
                                                                let mut response_envelope = Inner::build_response_envelope(
                                                                    request_id.clone(),
                                                                    envelope.route_key.clone(),
                                                                    Some(response_bytes),
                                                                    None,
                                                                    envelope.traceparent.clone(),
                                                                    envelope.tracestate.clone(),
                                                                );
                                                                // Inject tracing context
                                                                #[cfg(feature = "opentelemetry")]
                                                                inject_span_context_to_rpc(&span, &mut response_envelope);

                                                                #[cfg(feature = "opentelemetry")]
                                                                let send_fut = send_envelope_to_caller(
                                                                    &ws_gate,
                                                                    &webrtc_gate,
                                                                    &caller,
                                                                    response_envelope,
                                                                    &request_id,
                                                                ).instrument(span);
                                                                #[cfg(not(feature = "opentelemetry"))]
                                                                let send_fut = send_envelope_to_caller(
                                                                    &ws_gate,
                                                                    &webrtc_gate,
                                                                    &caller,
                                                                    response_envelope,
                                                                    &request_id,
                                                                );
                                                                send_fut.await;
                                                            }
                                                            // `tell` handled successfully: side
                                                            // effects ran, no reply is sent.
                                                            Ok(_) => {}
                                                            Err(e) => {
                                                                tracing::error!(
                                                                    severity = 8,
                                                                    error_category = "protobuf_decode",
                                                                    request_id = %envelope.request_id,
                                                                    "❌ Failed to decode caller_id: {:?}",
                                                                    e
                                                                );
                                                            }
                                                        }

                                                        // ACK message
                                                        if let Err(e) = mailbox.ack(msg_record.id).await {
                                                            tracing::error!(
                                                                severity = 9,
                                                                error_category = "mailbox_error",
                                                                request_id = %envelope.request_id,
                                                                message_id = %msg_record.id,
                                                                "❌ Mailbox ACK failed: {:?}",
                                                                e
                                                            );
                                                        }
                                                    }
                                                    Err(e) => {
                                                        tracing::error!(
                                                            severity = 6,
                                                            error_category = "handler_error",
                                                            request_id = %envelope.request_id,
                                                            route_key = %envelope.route_key,
                                                            "❌ handle_incoming failed: {:?}", e
                                                        );

                                                        // Send error envelope back to caller so it
                                                        // receives a structured error rather than
                                                        // waiting until its deadline fires. A `tell`
                                                        // (Direction::Tell) registered no pending
                                                        // entry, so skip the reply — the local error
                                                        // log above still records the failure (#262).
                                                        if let (true, Ok(caller)) =
                                                            (expects_response, caller_id_result)
                                                        {
                                                            let error_response = actr_protocol::ErrorResponse {
                                                                code: protocol_error_to_code(&e),
                                                                message: e.to_string(),
                                                            };
                                                            #[cfg_attr(not(feature = "opentelemetry"), allow(unused_mut))]
                                                            let mut error_envelope = Inner::build_response_envelope(
                                                                request_id.clone(),
                                                                envelope.route_key.clone(),
                                                                None,
                                                                Some(error_response),
                                                                envelope.traceparent.clone(),
                                                                envelope.tracestate.clone(),
                                                            );
                                                            #[cfg(feature = "opentelemetry")]
                                                            inject_span_context_to_rpc(&span, &mut error_envelope);

                                                            send_envelope_to_caller(
                                                                &ws_gate,
                                                                &webrtc_gate,
                                                                &caller,
                                                                error_envelope,
                                                                &request_id,
                                                            ).await;
                                                        }

                                                        // ACK to avoid infinite retries
                                                        let _ = mailbox.ack(msg_record.id).await;
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                // Poison message - cannot decode RpcEnvelope
                                                tracing::error!(
                                                    severity = 9,
                                                    error_category = "protobuf_decode",
                                                    message_id = %msg_record.id,
                                                    "❌ Poison message: Failed to deserialize RpcEnvelope: {:?}",
                                                    e
                                                );

                                                // Write to Dead Letter Queue
                                                use actr_runtime_mailbox::DlqRecord;
                                                use chrono::Utc;
                                                use uuid::Uuid;

                                                let dlq_record = DlqRecord {
                                                    id: Uuid::new_v4(),
                                                    original_message_id: Some(msg_record.id.to_string()),
                                                    from: Some(msg_record.from.clone()),
                                                    to: node.actor_id.as_ref().map(|id| {
                                                        let mut buf = Vec::new();
                                                        id.encode(&mut buf).unwrap();
                                                        buf
                                                    }),
                                                    raw_bytes: msg_record.payload.clone(),
                                                    error_message: format!("Protobuf decode failed: {e}"),
                                                    error_category: "protobuf_decode".to_string(),
                                                    trace_id: format!("mailbox-{}", msg_record.id),
                                                    request_id: None,
                                                    created_at: Utc::now(),
                                                    redrive_attempts: 0,
                                                    last_redrive_at: None,
                                                    context: Some(format!(
                                                        r#"{{"source":"mailbox","priority":"{}"}}"#,
                                                        match msg_record.priority {
                                                            actr_runtime_mailbox::MessagePriority::High => "high",
                                                            actr_runtime_mailbox::MessagePriority::Normal => "normal",
                                                        }
                                                    )),
                                                };

                                                if let Err(dlq_err) = node.dlq.enqueue(dlq_record).await {
                                                    tracing::error!(
                                                        severity = 10,
                                                        "❌ CRITICAL: Failed to write poison message to DLQ: {:?}",
                                                        dlq_err
                                                    );
                                                } else {
                                                    tracing::warn!(
                                                        severity = 9,
                                                        "☠️ Poison message moved to DLQ: message_id={}",
                                                        msg_record.id
                                                    );
                                                }

                                                // ACK the poison message to remove from mailbox
                                                let _ = mailbox.ack(msg_record.id).await;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(
                                        severity = 9,
                                        error_category = "mailbox_error",
                                        "❌ Mailbox dequeue failed: {:?}", e
                                    );
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }
                    }
                }
                tracing::info!("✅ Mailbox processing loop terminated gracefully");
            });

            task_handles.push(mailbox_handle);
        }
        tracing::info!("✅ Mailbox processing loop started");
        tracing::info!("✅ ActrNode started successfully");

        {
            let ready_ctx = bootstrap_ctx_builder
                .build_bootstrap(&actor_id, &credential_state.credential().await);
            let invocation = lifecycle_invocation(&actor_id, "lifecycle:on_ready");
            let call_executor =
                lifecycle_host_abi(ready_ctx.clone(), node_ref.workload_dispatch.clone());
            let mut workload = node_ref.workload_dispatch.lock().await;
            if let Err(e) = crate::lifecycle::hooks::call_lifecycle_hook(
                "on_ready",
                workload.on_ready(ready_ctx, invocation, &call_executor),
            )
            .await
            {
                tracing::warn!(error = %e, "workload on_ready returned Err");
            }
        }

        // Create ActrRefShared
        let shared = Arc::new(ActrRefShared {
            actor_id,
            bootstrap_ctx_builder,
            credential_state,
            session_state: Some(session_state),
            shutdown_token,
            task_handles: Mutex::new(task_handles),
        });

        // Create ActrRef
        tracing::info!("✅ ActrRef created (Shell → Guest communication handle)");

        Ok(ActrRef { shared })
    }
}

#[cfg(test)]
#[path = "node_tests.rs"]
mod tests;
