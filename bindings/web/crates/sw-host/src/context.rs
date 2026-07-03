//! Context for actor-internal execution.
//!
//! Mirrors the `actr` `Context` trait and provides communication primitives
//! used inside actors.

use std::rc::Rc;

use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{
    ActorResult, ActrId, ActrType, Direction, MetadataEntry, RpcEnvelope, RpcRequest,
};
use bytes::Bytes;

use crate::WebContext;
use crate::outbound::Gate;
use crate::web_context::RuntimeBridge;

/// Runtime context for actors in the Web runtime.
///
/// Mirrors `actr`'s `RuntimeContext`.
pub struct RuntimeContext {
    /// Current actor ID.
    self_id: ActrId,

    /// Caller actor ID.
    caller_id: Option<ActrId>,

    /// Trace identifiers.
    traceparent: String,
    tracestate: String,

    /// Request ID.
    request_id: String,

    /// Outbound gate.
    gate: Gate,

    /// Runtime bridge for operations that require lower-level runtime support,
    /// such as `call_raw` and `discover`.
    bridge: Option<Rc<dyn RuntimeBridge>>,
}

impl RuntimeContext {
    /// Create a new context.
    pub fn new(
        self_id: ActrId,
        caller_id: Option<ActrId>,
        traceparent: String,
        tracestate: String,
        request_id: String,
        gate: Gate,
    ) -> Self {
        Self {
            self_id,
            caller_id,
            traceparent,
            tracestate,
            request_id,
            gate,
            bridge: None,
        }
    }

    /// Attach a `RuntimeBridge` for handler execution.
    pub fn with_bridge(mut self, bridge: Rc<dyn RuntimeBridge>) -> Self {
        self.bridge = Some(bridge);
        self
    }
}

#[async_trait::async_trait(?Send)]
impl WebContext for RuntimeContext {
    // ========== Basic Info ==========

    fn self_id(&self) -> &ActrId {
        &self.self_id
    }

    fn caller_id(&self) -> Option<&ActrId> {
        self.caller_id.as_ref()
    }

    fn trace_id(&self) -> &str {
        &self.traceparent
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    // ========== RPC Communication ==========

    async fn call_raw(
        &self,
        target: &ActrId,
        route_key: &str,
        payload: &[u8],
        timeout_ms: i64,
    ) -> ActorResult<Vec<u8>> {
        let request_id = js_sys::Math::random().to_string();

        // Register the pending RPC through the bridge so `handle_fast_path`
        // can treat it as a response instead of an inbound request.
        if let Some(bridge) = &self.bridge {
            bridge.register_pending_rpc(request_id.clone());
            // Ensure the WebRTC connection to the target is ready.
            if let Err(error) = bridge.ensure_connection(target).await {
                log::warn!(
                    "[Context] ensure_connection skipped for {}: {}",
                    target,
                    error
                );
            }
        }

        let envelope = RpcEnvelope {
            route_key: route_key.to_string(),
            payload: Some(Bytes::from(payload.to_vec())),
            error: None,
            direction: Some(Direction::Request as i32),
            traceparent: Some(self.traceparent.clone()),
            tracestate: Some(self.tracestate.clone()),
            request_id,
            metadata: vec![MetadataEntry {
                key: "sender_actr_id".to_string(),
                value: self.self_id.to_string_repr(),
            }],
            timeout_ms,
        };

        let response_bytes = self.gate.send_request(target, envelope).await?;
        Ok(response_bytes.to_vec())
    }

    async fn discover(&self, target_type: &ActrType) -> ActorResult<ActrId> {
        match &self.bridge {
            Some(bridge) => bridge.discover_target(target_type).await,
            None => Err(actr_protocol::ActrError::Unavailable(
                "RuntimeBridge not available for discover".to_string(),
            )),
        }
    }

    // ========== Type-Safe Messaging ==========

    async fn call<R: RpcRequest>(&self, target: &ActrId, request: R) -> ActorResult<R::Response> {
        let request_id = js_sys::Math::random().to_string();

        if let Some(bridge) = &self.bridge {
            bridge.register_pending_rpc(request_id.clone());
            if let Err(error) = bridge.ensure_connection(target).await {
                log::warn!(
                    "[Context] ensure_connection skipped for {}: {}",
                    target,
                    error
                );
            }
        }

        // 1. Encode the request into protobuf bytes.
        let payload: Bytes = request.encode_to_vec().into();

        // 2. Read the route key from the `RpcRequest` trait.
        let route_key = R::route_key().to_string();

        // 3. Build an `RpcEnvelope` carrying the current trace context.
        let envelope = RpcEnvelope {
            route_key,
            payload: Some(payload),
            error: None,
            direction: Some(Direction::Request as i32),
            traceparent: Some(self.traceparent.clone()),
            tracestate: Some(self.tracestate.clone()),
            request_id,
            metadata: vec![MetadataEntry {
                key: "sender_actr_id".to_string(),
                value: self.self_id.to_string_repr(),
            }],
            timeout_ms: 30000,
        };

        // 4. Send through the gate.
        let response_bytes = self.gate.send_request(target, envelope).await?;

        // 5. Decode the response.
        R::Response::decode(&*response_bytes).map_err(|e| {
            actr_protocol::ActrError::DecodeFailure(format!(
                "Failed to decode {}: {}",
                std::any::type_name::<R::Response>(),
                e
            ))
        })
    }

    async fn tell<R: RpcRequest>(&self, target: &ActrId, message: R) -> ActorResult<()> {
        if let Some(bridge) = &self.bridge {
            if let Err(error) = bridge.ensure_connection(target).await {
                log::warn!(
                    "[Context] ensure_connection skipped for {}: {}",
                    target,
                    error
                );
            }
        }

        // 1. Encode the message.
        let payload: Bytes = message.encode_to_vec().into();

        // 2. Fetch the route key.
        let route_key = R::route_key().to_string();

        // 3. Build an `RpcEnvelope` with fire-and-forget semantics.
        let envelope = RpcEnvelope {
            route_key,
            payload: Some(payload),
            error: None,
            direction: Some(Direction::Request as i32),
            traceparent: Some(self.traceparent.clone()),
            tracestate: Some(self.tracestate.clone()),
            request_id: js_sys::Math::random().to_string(), // Simplified ID generation.
            metadata: vec![MetadataEntry {
                key: "sender_actr_id".to_string(),
                value: self.self_id.to_string_repr(),
            }],
            timeout_ms: 0, // `0` means do not wait for a response.
        };

        // 4. Send through the gate.
        self.gate.send_message(target, envelope).await
    }

    // ========== Stream Registration ==========
    //
    // Stream registration on the SW side mainly forwards registration requests
    // to the DOM side. The actual callback runs on the DOM fast path.

    async fn register_stream(
        &self,
        stream_id: String,
        callback: Box<dyn FnMut(Bytes) + 'static>,
    ) -> ActorResult<()> {
        log::info!("[Context] register_stream: {}", stream_id);

        match &self.bridge {
            Some(bridge) => bridge.register_stream_handler(stream_id, callback),
            None => Err(actr_protocol::ActrError::Unavailable(
                "RuntimeBridge not available for register_stream".to_string(),
            )),
        }
    }

    async fn unregister_stream(&self, stream_id: &str) -> ActorResult<()> {
        log::info!("[Context] unregister_stream: {}", stream_id);

        match &self.bridge {
            Some(bridge) => bridge.unregister_stream_handler(stream_id),
            None => Err(actr_protocol::ActrError::Unavailable(
                "RuntimeBridge not available for unregister_stream".to_string(),
            )),
        }
    }

    async fn register_media_track(
        &self,
        _track_id: String,
        _callback: Box<dyn FnMut(Bytes) + 'static>,
    ) -> ActorResult<()> {
        // Media-track fast path is intentionally not wired on the web target
        // (see core/framework/src/web/context.rs §"DataStream / MediaTrack
        // fast paths"). Returning Unavailable is consistent with the
        // framework `WebContext` shape so callers fail loud rather than
        // believe a registration that did nothing.
        Err(actr_protocol::ActrError::Unavailable(
            "register_media_track is not supported in the web runtime".to_string(),
        ))
    }

    async fn unregister_media_track(&self, _track_id: &str) -> ActorResult<()> {
        Err(actr_protocol::ActrError::Unavailable(
            "unregister_media_track is not supported in the web runtime".to_string(),
        ))
    }

    // ========== Stream Sending ==========

    async fn send_media_sample(
        &self,
        target: &ActrId,
        track_id: &str,
        data: Bytes,
    ) -> ActorResult<()> {
        log::debug!(
            "[Context] send_media_sample: target={:?}, track_id={}, size={}",
            target,
            track_id,
            data.len()
        );

        // Prefix the payload with `track_id`.
        // Format: [track_id_len(4) | track_id(N) | data(M)]
        let track_id_bytes = track_id.as_bytes();
        let mut payload = Vec::with_capacity(4 + track_id_bytes.len() + data.len());
        payload.extend_from_slice(&(track_id_bytes.len() as u32).to_be_bytes());
        payload.extend_from_slice(track_id_bytes);
        payload.extend_from_slice(&data);

        // Send fast-path data through the gate.
        self.gate
            .send_data_stream(
                target,
                actr_protocol::PayloadType::MediaRtp,
                Bytes::from(payload),
            )
            .await
    }

    async fn send_data_stream(
        &self,
        target: &ActrId,
        stream_id: &str,
        data: Bytes,
    ) -> ActorResult<()> {
        log::debug!(
            "[Context] send_data_stream: target={:?}, stream_id={}, size={}",
            target,
            stream_id,
            data.len()
        );

        // Prefix the payload with `stream_id`.
        // Format: [stream_id_len(4) | stream_id(N) | data(M)]
        let stream_id_bytes = stream_id.as_bytes();
        let mut payload = Vec::with_capacity(4 + stream_id_bytes.len() + data.len());
        payload.extend_from_slice(&(stream_id_bytes.len() as u32).to_be_bytes());
        payload.extend_from_slice(stream_id_bytes);
        payload.extend_from_slice(&data);

        // Send fast-path data through the gate (defaulting to STREAM_RELIABLE).
        self.gate
            .send_data_stream(
                target,
                actr_protocol::PayloadType::StreamReliable,
                Bytes::from(payload),
            )
            .await
    }
}
