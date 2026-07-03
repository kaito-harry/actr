//! Runtime Context Implementation
//!
//! Implements the Context trait defined in actr-framework.

use crate::inbound::{DataStreamRegistry, MediaFrameRegistry};
use crate::lifecycle::session_state::{SessionPhase, SessionState};
use crate::outbound::Gate;
use crate::wire::webrtc::SignalingClient;
#[cfg(feature = "opentelemetry")]
use crate::wire::webrtc::trace::inject_span_context_to_rpc;
use actr_config::lock::LockFile;
use actr_framework::{Bytes, Context, DataStream, Dest, MediaSample};
use actr_protocol::{
    AIdCredential, ActorResult, ActrError, ActrId, ActrType, ConnectionNotReadyInfo, Direction,
    PayloadType, RouteCandidatesRequest, RpcEnvelope, RpcRequest, route_candidates_request,
};
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// RuntimeContext - Runtime's implementation of Context trait
///
/// # Design Features
///
/// - **Zero vtable**: internally uses Gate enum dispatch (not dyn)
/// - **Smart routing**: automatically selects Host or Peer based on Dest
/// - **Full implementation**: contains complete call/tell logic (encode, send, decode)
/// - **Type safety**: generic methods provide compile-time type checking
///
/// # Performance
///
/// - Gate is an enum, uses static dispatch
/// - Compiler can fully inline the entire call chain
/// - Zero virtual function call overhead
#[derive(Clone)]
pub struct RuntimeContext {
    self_id: ActrId,
    caller_id: Option<ActrId>,
    request_id: String,
    inproc_gate: Gate,          // Shell/Local calls - immediately available
    outproc_gate: Option<Gate>, // Remote Actor calls - lazily initialized
    data_stream_registry: Arc<DataStreamRegistry>, // DataStream callback registry
    media_frame_registry: Arc<MediaFrameRegistry>, // MediaTrack callback registry
    signaling_client: Arc<dyn SignalingClient>,
    credential: AIdCredential,
    actr_lock: Option<Arc<LockFile>>, // packaged manifest.lock.toml for fingerprint lookups
    /// Shared map of discovered direct-connect WebSocket URLs, keyed by ActrId.
    /// Populated by `discover_route_candidate` from the signaling `ws_address_map`,
    /// then read by `DefaultWireBuilder` when establishing outbound connections.
    discovered_ws_addresses: Arc<RwLock<HashMap<ActrId, String>>>,
    /// Session state handle — when set, outbound sends dynamically read the
    /// current credential from the snapshot (soft renew propagates automatically).
    /// `None` during transition; will become required.
    pub(crate) session_state: Option<SessionState>,
    /// Generation captured at context creation time. After a hard rebind,
    /// outbound sends from old-generation contexts return `ConnectionNotReady`.
    pub(crate) context_generation: u64,
}

impl RuntimeContext {
    /// Create a new `RuntimeContext`.
    ///
    /// # Parameters
    ///
    /// - `self_id`: ID of the current actor
    /// - `caller_id`: optional caller actor ID
    /// - `request_id`: unique ID for the current request
    /// - `inproc_gate`: in-process gate, immediately available
    /// - `outproc_gate`: cross-process gate, possibly `None` until WebRTC initialization completes
    /// - `data_stream_registry`: callback registry for `DataStream`
    /// - `media_frame_registry`: callback registry for `MediaTrack`
    /// - `signaling_client`: signaling client used for route discovery
    /// - `credential`: credentials used when calling signaling interfaces
    /// - `actr_lock`: shared packaged `manifest.lock.toml` used for fingerprint lookup (wrapped in `Arc` so context clones stay cheap)
    /// - `discovered_ws_addresses`: shared map written by `discover_route_candidate` and read by `DefaultWireBuilder`
    #[allow(clippy::too_many_arguments)] // Internal API - all parameters are required
    pub(crate) fn new(
        self_id: ActrId,
        caller_id: Option<ActrId>,
        request_id: String,
        inproc_gate: Gate,
        outproc_gate: Option<Gate>,
        data_stream_registry: Arc<DataStreamRegistry>,
        media_frame_registry: Arc<MediaFrameRegistry>,
        signaling_client: Arc<dyn SignalingClient>,
        credential: AIdCredential,
        actr_lock: Option<Arc<LockFile>>,
        discovered_ws_addresses: Arc<RwLock<HashMap<ActrId, String>>>,
        session_state: Option<SessionState>,
        context_generation: u64,
    ) -> Self {
        Self {
            self_id,
            caller_id,
            request_id,
            inproc_gate,
            outproc_gate,
            data_stream_registry,
            media_frame_registry,
            signaling_client,
            credential,
            actr_lock,
            discovered_ws_addresses,
            session_state,
            context_generation,
        }
    }

    /// Select the appropriate gate based on `Dest`.
    ///
    /// - `Dest::Shell` -> `inproc_gate`
    /// - `Dest::Local` -> `inproc_gate`
    /// - `Dest::Actor(_)` -> `outproc_gate`, which must already be initialized
    #[inline]
    fn select_gate(&self, dest: &Dest) -> ActorResult<&Gate> {
        match dest {
            Dest::Shell | Dest::Local => Ok(&self.inproc_gate),
            Dest::Actor(_) => self.outproc_gate.as_ref().ok_or_else(|| {
                ActrError::Internal(
                    "PeerGate not initialized yet (WebRTC setup in progress)".to_string(),
                )
            }),
        }
    }

    /// Extract the target `ActrId` from `Dest`.
    ///
    /// - `Dest::Shell` -> `self_id` for reverse Workload-to-App calls
    /// - `Dest::Local` -> `self_id` for local workload calls
    /// - `Dest::Actor(id)` -> remote actor ID
    #[inline]
    fn extract_target_id<'a>(&'a self, dest: &'a Dest) -> &'a ActrId {
        match dest {
            Dest::Shell | Dest::Local => &self.self_id,
            Dest::Actor(id) => id,
        }
    }

    async fn ensure_session_ready(&self) -> ActorResult<()> {
        let Some(session_state) = &self.session_state else {
            return Ok(());
        };

        if !session_state
            .is_current_generation(self.context_generation)
            .await
        {
            return Err(ActrError::ConnectionNotReady(
                ConnectionNotReadyInfo::without_retry_hint(),
            ));
        }

        if session_state.phase().await != SessionPhase::Active {
            return Err(ActrError::ConnectionNotReady(
                ConnectionNotReadyInfo::without_retry_hint(),
            ));
        }

        Ok(())
    }

    /// Execute a non-generic RPC request call (useful for language bindings).
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            skip_all,
            name = "RuntimeContext.call_raw",
            fields(
                actr_id = %self.self_id,
                route_key = %route_key,
            )
        )
    )]
    pub async fn call_raw(
        &self,
        target: &Dest,
        route_key: String,
        payload_type: PayloadType,
        payload: Bytes,
        timeout_ms: i64,
    ) -> ActorResult<Bytes> {
        self.ensure_session_ready().await?;

        #[cfg(feature = "opentelemetry")]
        use crate::wire::webrtc::trace::inject_span_context_to_rpc;

        #[cfg_attr(not(feature = "opentelemetry"), allow(unused_mut))]
        let mut envelope = RpcEnvelope {
            route_key,
            payload: Some(payload),
            error: None,
            direction: Some(Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: uuid::Uuid::new_v4().to_string(),
            metadata: vec![],
            timeout_ms,
        };
        #[cfg(feature = "opentelemetry")]
        inject_span_context_to_rpc(&tracing::Span::current(), &mut envelope);

        let gate = self.select_gate(target)?;
        let target_id = self.extract_target_id(target);
        gate.send_request_with_type(target_id, payload_type, envelope)
            .await
    }

    /// Execute a non-generic RPC message call (fire-and-forget, useful for language bindings).
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            skip_all,
            name = "RuntimeContext.tell_raw",
            fields(
                actr_id = %self.self_id,
                route_key = %route_key,
            )
        )
    )]
    pub async fn tell_raw(
        &self,
        target: &Dest,
        route_key: String,
        payload_type: PayloadType,
        payload: Bytes,
    ) -> ActorResult<()> {
        self.ensure_session_ready().await?;

        #[cfg(feature = "opentelemetry")]
        use crate::wire::webrtc::trace::inject_span_context_to_rpc;

        #[cfg_attr(not(feature = "opentelemetry"), allow(unused_mut))]
        let mut envelope = RpcEnvelope {
            route_key,
            payload: Some(payload),
            error: None,
            direction: Some(Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: uuid::Uuid::new_v4().to_string(),
            metadata: vec![],
            timeout_ms: 0,
        };
        #[cfg(feature = "opentelemetry")]
        inject_span_context_to_rpc(&tracing::Span::current(), &mut envelope);

        let gate = self.select_gate(target)?;
        let target_id = self.extract_target_id(target);
        gate.send_message_with_type(target_id, payload_type, envelope)
            .await
    }

    /// Send DataStream with an explicit payload type (lane selection).
    ///
    /// Convenience wrapper for language bindings that prefer positional `payload_type`
    /// before `chunk`. Equivalent to calling `Context::send_data_stream` directly.
    pub async fn send_data_stream_with_type(
        &self,
        target: &Dest,
        payload_type: actr_protocol::PayloadType,
        chunk: DataStream,
    ) -> ActorResult<()> {
        self.ensure_session_ready().await?;

        use actr_protocol::prost::Message as ProstMessage;

        let payload = chunk.encode_to_vec();
        let stream_id = chunk.stream_id.as_str();

        let gate = self.select_gate(target)?;
        let target_id = self.extract_target_id(target);

        gate.send_data_stream(
            target_id,
            payload_type,
            stream_id,
            bytes::Bytes::from(payload),
        )
        .await
    }

    /// Get dependency fingerprint from the packaged manifest.lock.toml
    fn get_dependency_fingerprint(&self, target_type: &ActrType) -> Option<String> {
        let actr_lock = self.actr_lock.as_ref()?;

        let key = target_type.to_string_repr();

        // Try by full key
        if let Some(dep) = actr_lock.get_dependency(&key) {
            return Some(dep.fingerprint.clone());
        }

        // Fallback to scanning dependencies when the exact key is not present.
        for dep in &actr_lock.dependencies {
            if Self::matches_dependency_actr_type(&dep.actr_type, target_type) {
                return Some(dep.fingerprint.clone());
            }
        }

        None
    }

    fn matches_dependency_actr_type(raw: &str, target_type: &ActrType) -> bool {
        let Ok(dep_type) = ActrType::from_string_repr(raw) else {
            return false;
        };

        dep_type == *target_type
    }

    /// Internal: Send discovery request to signaling server
    async fn send_discovery_request(
        &self,
        target_type: &ActrType,
        candidate_count: u32,
        client_fingerprint: String,
    ) -> ActorResult<InternalDiscoveryResult> {
        let criteria = route_candidates_request::NodeSelectionCriteria {
            candidate_count,
            ranking_factors: Vec::new(),
            minimal_dependency_requirement: None,
            minimal_health_requirement: None,
        };

        let request = RouteCandidatesRequest {
            target_type: target_type.clone(),
            criteria: Some(criteria),
            client_location: None,
            client_fingerprint,
        };

        let (source_id, credential) = if let Some(session_state) = &self.session_state {
            (
                session_state.actor_id().await,
                session_state.credential().await,
            )
        } else {
            (self.self_id.clone(), self.credential.clone())
        };

        let response = self
            .signaling_client
            .send_route_candidates_request(source_id, credential, request)
            .await
            .map_err(|e| ActrError::Unavailable(format!("Route candidates request failed: {e}")))?;

        match response.result {
            Some(actr_protocol::route_candidates_response::Result::Success(success)) => {
                Ok(InternalDiscoveryResult {
                    candidates: success.candidates,
                    ws_address_map: success.ws_address_map,
                })
            }
            Some(actr_protocol::route_candidates_response::Result::Error(err)) => {
                Err(ActrError::Unavailable(format!(
                    "Route candidates error {}: {}",
                    err.code, err.message
                )))
            }
            None => Err(ActrError::Unavailable(
                "Invalid route candidates response: missing result".to_string(),
            )),
        }
    }
}

/// Internal discovery result structure
struct InternalDiscoveryResult {
    candidates: Vec<ActrId>,
    /// Direct-connect WebSocket URLs returned alongside candidates.
    ws_address_map: Vec<actr_protocol::WsAddressEntry>,
}

/// Template used to materialize `RuntimeContext` instances for lifecycle
/// bootstrap / observation paths (on_start / on_stop, signaling hooks, WebRTC
/// hooks, ActrRef::app_context, ...).
///
/// Unlike the per-request dispatch path, which constructs `RuntimeContext`
/// directly from `Inner`, this builder is a **detachable snapshot** of the
/// handles needed to build a context. It is cloned into long-lived hook
/// closures and into `ActrRefShared` so those paths don't need to retain a
/// reference back to `Inner`.
///
/// # Fields
///
/// Mirrors `RuntimeContext`'s non-per-request state. `outproc_gate` is
/// `Option` because hook builders can be captured *before* WebRTC
/// initialization finishes; such snapshots will simply emit contexts with
/// `outproc_gate = None`, matching the pre-existing semantics.
#[derive(Clone)]
pub(crate) struct BootstrapContextBuilder {
    inproc_gate: Gate,
    outproc_gate: Option<Gate>,
    data_stream_registry: Arc<DataStreamRegistry>,
    media_frame_registry: Arc<MediaFrameRegistry>,
    signaling_client: Arc<dyn SignalingClient>,
    actr_lock: Option<Arc<LockFile>>,
    /// Shared map populated by discover_route_candidate; forwarded into each
    /// RuntimeContext so discovery results are visible to DefaultWireBuilder.
    discovered_ws_addresses: Arc<RwLock<HashMap<ActrId, String>>>,
    /// Session state handle — when set, built contexts will read credentials
    /// dynamically from the snapshot.
    session_state: Option<SessionState>,
    /// Generation number for stale-context detection after hard rebind.
    generation: u64,
}

impl BootstrapContextBuilder {
    /// Assemble a new builder from the runtime handles. All parameters are
    /// snapshotted by clone; later mutations on the origin (e.g. the node's
    /// own `actr_lock`) are intentionally not observed — callers that need
    /// a fresh snapshot must re-build.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inproc_gate: Gate,
        outproc_gate: Option<Gate>,
        data_stream_registry: Arc<DataStreamRegistry>,
        media_frame_registry: Arc<MediaFrameRegistry>,
        signaling_client: Arc<dyn SignalingClient>,
        actr_lock: Option<Arc<LockFile>>,
        discovered_ws_addresses: Arc<RwLock<HashMap<ActrId, String>>>,
        session_state: Option<SessionState>,
        generation: u64,
    ) -> Self {
        Self {
            inproc_gate,
            outproc_gate,
            data_stream_registry,
            media_frame_registry,
            signaling_client,
            actr_lock,
            discovered_ws_addresses,
            session_state,
            generation,
        }
    }

    /// Set or replace the session state handle after builder construction.
    pub(crate) fn set_session_state(&mut self, ss: Option<SessionState>) {
        self.session_state = ss;
    }

    /// Update the generation (called after hard rebind).
    pub(crate) fn set_generation(&mut self, generation: u64) {
        self.generation = generation;
    }

    /// Materialize a bootstrap `RuntimeContext` for lifecycle hooks.
    ///
    /// The produced context has no caller (`caller_id = None`) and a freshly
    /// generated `request_id`; it is intended for on_start / on_stop /
    /// transport-event observation where no inbound envelope drives the
    /// request identity.
    pub(crate) fn build_bootstrap(
        &self,
        self_id: &ActrId,
        credential: &AIdCredential,
    ) -> RuntimeContext {
        let generation = self
            .session_state
            .as_ref()
            .and_then(SessionState::generation_sync)
            .unwrap_or(self.generation);

        RuntimeContext::new(
            self_id.clone(),
            None,
            uuid::Uuid::new_v4().to_string(),
            self.inproc_gate.clone(),
            self.outproc_gate.clone(),
            self.data_stream_registry.clone(),
            self.media_frame_registry.clone(),
            self.signaling_client.clone(),
            credential.clone(),
            self.actr_lock.clone(),
            self.discovered_ws_addresses.clone(),
            self.session_state.clone(),
            generation,
        )
    }
}

#[async_trait]
impl Context for RuntimeContext {
    // ========== Data Access Methods ==========

    fn self_id(&self) -> &ActrId {
        &self.self_id
    }

    fn caller_id(&self) -> Option<&ActrId> {
        self.caller_id.as_ref()
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    // ========== Communication Methods ==========
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            skip_all,
            name = "RuntimeContext.call",
            fields(actr_id = %self.self_id)
        )
    )]
    async fn call<R: RpcRequest>(&self, target: &Dest, request: R) -> ActorResult<R::Response> {
        self.ensure_session_ready().await?;

        use actr_protocol::prost::Message as ProstMessage;

        // 1. Encode the request as protobuf bytes.
        let payload: Bytes = request.encode_to_vec().into();

        // 2. Get the compile-time route key from the RpcRequest trait.
        let route_key = R::route_key().to_string();

        // 3. Build the RpcEnvelope with W3C tracing fields.
        #[cfg_attr(not(feature = "opentelemetry"), allow(unused_mut))]
        let mut envelope = RpcEnvelope {
            route_key,
            payload: Some(payload),
            error: None,
            direction: Some(Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: uuid::Uuid::new_v4().to_string(), // Generate a new request_id.
            metadata: vec![],
            timeout_ms: 30000, // Default to a 30-second timeout.
        };
        // Inject tracing context from current span
        #[cfg(feature = "opentelemetry")]
        inject_span_context_to_rpc(&tracing::Span::current(), &mut envelope);

        // 4. Select a gate from Dest and extract the target ActrId.
        let gate = self.select_gate(target)?;
        let target_id = self.extract_target_id(target);

        // 5. Send via Gate enum dispatch without virtual calls.
        // Respect request's declared payload type (lane selection)
        let response_bytes = gate
            .send_request_with_type(target_id, R::payload_type(), envelope)
            .await?;

        // 6. Decode the typed response.
        R::Response::decode(&*response_bytes).map_err(|e| {
            ActrError::DecodeFailure(format!(
                "Failed to decode {}: {}",
                std::any::type_name::<R::Response>(),
                e
            ))
        })
    }

    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            skip_all,
            name = "RuntimeContext.tell",
            fields(actr_id = %self.self_id)
        )
    )]
    async fn tell<R: RpcRequest>(&self, target: &Dest, message: R) -> ActorResult<()> {
        self.ensure_session_ready().await?;

        // 1. Encode the message.
        let payload: Bytes = message.encode_to_vec().into();

        // 2. Get the route key.
        let route_key = R::route_key().to_string();

        // 3. Build the RpcEnvelope for fire-and-forget delivery.
        #[cfg_attr(not(feature = "opentelemetry"), allow(unused_mut))]
        let mut envelope = RpcEnvelope {
            route_key,
            payload: Some(payload),
            error: None,
            direction: Some(Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: uuid::Uuid::new_v4().to_string(),
            metadata: vec![],
            timeout_ms: 0, // Zero means no response is expected.
        };
        // Inject tracing context from current span
        #[cfg(feature = "opentelemetry")]
        inject_span_context_to_rpc(&tracing::Span::current(), &mut envelope);

        // 4. Select a gate from Dest and extract the target ActrId.
        let gate = self.select_gate(target)?;
        let target_id = self.extract_target_id(target);

        // 5. Dispatch through the Gate enum while preserving payload type.
        gate.send_message_with_type(target_id, R::payload_type(), envelope)
            .await
    }

    // ========== Fast Path: DataStream Methods ==========

    async fn register_stream<F>(&self, stream_id: String, callback: F) -> ActorResult<()>
    where
        F: Fn(DataStream, ActrId) -> BoxFuture<'static, ActorResult<()>> + Send + Sync + 'static,
    {
        tracing::debug!(
            "📊 Registering DataStream callback for stream_id: {}",
            stream_id
        );
        self.data_stream_registry
            .register(stream_id, Arc::new(callback));
        Ok(())
    }

    async fn unregister_stream(&self, stream_id: &str) -> ActorResult<()> {
        tracing::debug!(
            "🚫 Unregistering DataStream callback for stream_id: {}",
            stream_id
        );
        self.data_stream_registry.unregister(stream_id);
        Ok(())
    }

    async fn send_data_stream(
        &self,
        target: &Dest,
        chunk: DataStream,
        payload_type: actr_protocol::PayloadType,
    ) -> ActorResult<()> {
        self.ensure_session_ready().await?;

        use actr_protocol::prost::Message as ProstMessage;

        // 1. Serialize DataStream to bytes
        let payload = chunk.encode_to_vec();
        let stream_id = chunk.stream_id.as_str();

        tracing::debug!(
            "📤 Sending DataStream: stream_id={}, sequence={}, size={} bytes",
            stream_id,
            chunk.sequence,
            payload.len()
        );

        // 2. Select gate based on Dest
        let gate = self.select_gate(target)?;
        let target_id = self.extract_target_id(target);

        // 3. Send via Gate with the caller-specified PayloadType
        gate.send_data_stream(
            target_id,
            payload_type,
            stream_id,
            bytes::Bytes::from(payload),
        )
        .await
    }

    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            skip_all,
            name = "RuntimeContext.discover_route_candidate",
            fields(
                actr_id = %self.self_id,
                target_type = %target_type,
            )
        )
    )]
    async fn discover_route_candidate(&self, target_type: &ActrType) -> ActorResult<ActrId> {
        self.ensure_session_ready().await?;

        if !self.signaling_client.is_connected() {
            return Err(ActrError::Unavailable(
                "Signaling client is not connected.".to_string(),
            ));
        }

        let service_name = format!("{}:{}", target_type.manufacturer, target_type.name);

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Step 1: Get fingerprint from manifest.lock.toml (when available)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let client_fingerprint = match self.get_dependency_fingerprint(target_type) {
            Some(fingerprint) => fingerprint,
            None => {
                if self.actr_lock.is_none() {
                    tracing::debug!(
                        "manifest.lock.toml not loaded; sending discovery without fingerprint for '{}'",
                        service_name
                    );
                    String::new()
                } else {
                    tracing::error!(
                        severity = 10,
                        error_category = "dependency_missing",
                        "❌ DEPENDENCY NOT FOUND: Service '{}' is not declared in manifest.lock.toml.\n\
                         Please run 'actr deps install' to generate the lock file with all dependencies.",
                        service_name
                    );
                    return Err(ActrError::DependencyNotFound {
                        service_name: service_name.clone(),
                        message: format!(
                            "Dependency '{}' not found in manifest.lock.toml. Run 'actr deps install' to resolve dependencies.",
                            service_name
                        ),
                    });
                }
            }
        };

        if !client_fingerprint.is_empty() {
            tracing::debug!(
                "📋 Found dependency fingerprint for '{}': {}",
                service_name,
                &client_fingerprint[..20.min(client_fingerprint.len())]
            );
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Step 2: Send discovery request to signaling server
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        let result = self
            .send_discovery_request(target_type, 1, client_fingerprint)
            .await?;

        tracing::info!(
            "Discovery result [{}]: {} candidates, {} ws_address entries",
            service_name,
            result.candidates.len(),
            result.ws_address_map.len(),
        );

        // Populate the shared discovered_ws_addresses map so DefaultWireBuilder
        // can use direct WebSocket connections instead of WebRTC for peers that
        // advertise a ws:// address through the signaling server.
        if !result.ws_address_map.is_empty() {
            let mut map = self.discovered_ws_addresses.write().await;
            for entry in result.ws_address_map {
                if let Some(url) = entry.ws_address {
                    tracing::debug!(
                        actor_id = ?entry.candidate_id,
                        ws_url = %url,
                        "discovered direct WebSocket address",
                    );
                    map.insert(entry.candidate_id, url);
                }
            }
        }

        result.candidates.into_iter().next().ok_or_else(|| {
            ActrError::NotFound(format!(
                "No route candidates for type {}/{}",
                target_type.manufacturer, target_type.name
            ))
        })
    }

    async fn call_raw(
        &self,
        target: &ActrId,
        route_key: &str,
        payload: Bytes,
    ) -> ActorResult<Bytes> {
        // Guest-facing trait entry: remote raw RPC with reliable lane and a
        // 30 s default timeout. Delegate to the inherent `call_raw` so both
        // entry points share one RpcEnvelope construction / gate-selection
        // path and stay in sync.
        RuntimeContext::call_raw(
            self,
            &Dest::Actor(target.clone()),
            route_key.to_string(),
            PayloadType::RpcReliable,
            payload,
            30_000,
        )
        .await
    }

    // ========== Fast Path: MediaTrack Methods ==========

    async fn register_media_track<F>(&self, track_id: String, callback: F) -> ActorResult<()>
    where
        F: Fn(MediaSample, ActrId) -> BoxFuture<'static, ActorResult<()>> + Send + Sync + 'static,
    {
        tracing::debug!(
            "📹 Registering MediaTrack callback for track_id: {}",
            track_id
        );
        self.media_frame_registry
            .register(track_id, Arc::new(callback));
        Ok(())
    }

    async fn unregister_media_track(&self, track_id: &str) -> ActorResult<()> {
        tracing::debug!(
            "📹 Unregistering MediaTrack callback for track_id: {}",
            track_id
        );
        self.media_frame_registry.unregister(track_id);
        Ok(())
    }

    async fn send_media_sample(
        &self,
        target: &Dest,
        track_id: &str,
        sample: MediaSample,
    ) -> ActorResult<()> {
        // 1. Select appropriate gate based on Dest
        let gate = self.select_gate(target)?;

        // 2. Extract target ActrId
        let target_id = self.extract_target_id(target);

        // 3. Send via Gate (delegates to WebRTC Track)
        gate.send_media_sample(target_id, track_id, sample).await
    }

    async fn add_media_track(
        &self,
        target: &Dest,
        track_id: &str,
        codec: &str,
        media_type: &str,
    ) -> ActorResult<()> {
        let gate = self.select_gate(target)?;
        let target_id = self.extract_target_id(target);
        gate.add_media_track(target_id, track_id, codec, media_type)
            .await
    }

    async fn remove_media_track(&self, target: &Dest, track_id: &str) -> ActorResult<()> {
        let gate = self.select_gate(target)?;
        let target_id = self.extract_target_id(target);
        gate.remove_media_track(target_id, track_id).await
    }
}

#[cfg(test)]
#[path = "context_tests.rs"]
mod tests;
