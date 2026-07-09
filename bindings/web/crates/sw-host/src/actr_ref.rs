//! ActrRef - lightweight reference to a running actor (Web version)
//!
//! # Design Philosophy
//!
//! `ActrRef` is a lightweight reference to a running actor and provides:
//!
//! - **RPC calls**: invoke actor methods from the DOM side to the SW side
//! - **Lifecycle control**: trigger shutdown and wait for completion
//!
//! # Key Characteristics
//!
//! - **Cloneable**: can be shared across tasks
//! - **Lightweight**: only contains one Arc to shared state
//! - **Code-gen friendly**: generated RPC methods bind naturally to this type
//!
//! # Usage
//!
//! ```rust,ignore
//! let actr = node.start().await?;
//!
//! // Clone and use it across tasks
//! let actr1 = actr.clone();
//! wasm_bindgen_futures::spawn_local(async move {
//!     actr1.call(SomeRequest { ... }).await?;
//! });
//!
//! // Shut down
//! actr.shutdown();
//! actr.wait_for_shutdown().await;
//! ```

use std::marker::PhantomData;
use std::sync::Arc;

use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActorResult, ActrError, ActrId, Direction, RpcEnvelope};
use bytes::Bytes;

use crate::outbound::HostGate;
use crate::trace::inject_span_context_to_rpc;
use actr_framework::Workload;

/// ActrRef - lightweight reference to a running actor (Web version)
///
/// This is the primary handle returned by `ActrNode::start()`.
///
/// # Code Generation Pattern
///
/// The `actr-cli` code generator emits type-safe RPC methods on `ActrRef`.
///
/// ## Proto Definition
///
/// ```protobuf
/// service EchoService {
///   rpc Echo(EchoRequest) returns (EchoResponse);
/// }
/// ```
///
/// ## Generated Code
///
/// ```rust,ignore
/// impl ActrRef<EchoServiceWorkload> {
///     pub async fn echo(&self, request: EchoRequest) -> ActorResult<EchoResponse> {
///         self.call(request).await
///     }
/// }
/// ```
pub struct ActrRef<W: Workload> {
    pub(crate) shared: Arc<ActrRefShared>,
    _phantom: PhantomData<W>,
}

impl<W: Workload> Clone for ActrRef<W> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            _phantom: PhantomData,
        }
    }
}

/// Shared state between all ActrRef clones
///
/// This is an internal implementation detail. When the final `ActrRef` is dropped,
/// the `Drop` implementation on this structure triggers shutdown and cleanup.
pub(crate) struct ActrRefShared {
    /// Actor ID
    pub(crate) actor_id: ActrId,

    /// Host gate for DOM → SW RPC
    /// Unlike core actr, the Web version only needs a Host gate here.
    pub(crate) host_gate: Arc<HostGate>,

    /// Shutdown flag
    pub(crate) shutdown: Arc<parking_lot::Mutex<bool>>,
}

impl<W: Workload> ActrRef<W> {
    /// Create new ActrRef from shared state
    ///
    /// Internal API used by `ActrNode::start()`.
    #[allow(dead_code)]
    pub(crate) fn new(shared: Arc<ActrRefShared>) -> Self {
        Self {
            shared,
            _phantom: PhantomData,
        }
    }

    /// Get Actor ID
    pub fn actor_id(&self) -> &ActrId {
        &self.shared.actor_id
    }

    /// Call Actor method (DOM → SW RPC)
    ///
    /// This is a generic method used by generated RPC methods.
    /// Most users should call the generated methods instead.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Generic call
    /// let response: EchoResponse = actr.call(EchoRequest {
    ///     message: "Hello".to_string(),
    /// }).await?;
    ///
    /// // Generated method (recommended)
    /// let response = actr.echo(EchoRequest {
    ///     message: "Hello".to_string(),
    /// }).await?;
    /// ```
    pub async fn call<R>(&self, request: R) -> ActorResult<R::Response>
    where
        R: actr_protocol::RpcRequest + ProstMessage,
    {
        // Encode the request.
        let payload: Bytes = request.encode_to_vec().into();

        // Create the envelope.
        let mut envelope = RpcEnvelope {
            route_key: R::route_key().to_string(),
            payload: Some(payload),
            error: None,
            direction: Some(Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: format!("req-{}", js_sys::Math::random()),
            metadata: vec![],
            timeout_ms: 30000,
        };

        // Inject trace context to RPC envelope
        inject_span_context_to_rpc(&tracing::Span::current(), &mut envelope);

        // Send the request and wait for the response.
        let response_bytes = self
            .shared
            .host_gate
            .send_request(&self.shared.actor_id, envelope)
            .await?;

        // Decode the response.
        R::Response::decode(&*response_bytes)
            .map_err(|e| ActrError::DecodeFailure(format!("Failed to decode response: {e}")))
    }

    /// Send one-way message to Actor (DOM → SW, fire-and-forget)
    ///
    /// Unlike `call()`, this method does not wait for a response.
    /// Use it for notifications or commands that need no acknowledgement.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Send a notification without waiting for a response
    /// actr.tell(LogEvent {
    ///     level: "INFO".to_string(),
    ///     message: "User logged in".to_string(),
    /// }).await?;
    /// ```
    pub async fn tell<R>(&self, message: R) -> ActorResult<()>
    where
        R: actr_protocol::RpcRequest + ProstMessage,
    {
        // Encode the message.
        let payload: Bytes = message.encode_to_vec().into();

        // Create envelope with initial traceparent and tracestate set to None.
        // One-way messages carry the explicit Direction::Tell label; the zero
        // timeout is documented filler, not a tell marker.
        let mut envelope = RpcEnvelope {
            route_key: R::route_key().to_string(),
            payload: Some(payload),
            error: None,
            direction: Some(Direction::Tell as i32),
            traceparent: None,
            tracestate: None,
            request_id: format!("req-{}", js_sys::Math::random()),
            metadata: vec![],
            timeout_ms: 0,
        };

        // Inject trace context to RPC envelope
        inject_span_context_to_rpc(&tracing::Span::current(), &mut envelope);

        // Send the message without waiting for a response.
        self.shared
            .host_gate
            .send_message(&self.shared.actor_id, envelope)
            .await
    }

    /// Trigger Actor shutdown
    ///
    /// This signals the actor to stop without waiting for completion.
    /// Use `wait_for_shutdown()` to wait for cleanup.
    pub fn shutdown(&self) {
        log::info!("🛑 Shutdown requested for Actor {:?}", self.shared.actor_id);
        let mut shutdown = self.shared.shutdown.lock();
        *shutdown = true;
    }

    /// Wait for Actor to fully shutdown
    ///
    /// Wait until the shutdown flag is set.
    /// The Web version uses polling because tokio is not available.
    pub async fn wait_for_shutdown(&self) {
        loop {
            let is_shutdown = *self.shared.shutdown.lock();
            if is_shutdown {
                break;
            }

            // Wait briefly with gloo_timers, which works in the Service Worker environment.
            gloo_timers::future::TimeoutFuture::new(100).await;
        }
    }

    /// Check if Actor is shutting down
    pub fn is_shutting_down(&self) -> bool {
        *self.shared.shutdown.lock()
    }
}

impl Drop for ActrRefShared {
    fn drop(&mut self) {
        log::info!(
            "🧹 ActrRefShared dropping - cleaning up Actor {:?}",
            self.actor_id
        );

        // Set the shutdown flag.
        *self.shutdown.lock() = true;

        log::debug!("✅ Actor {:?} marked for shutdown", self.actor_id);
    }
}
