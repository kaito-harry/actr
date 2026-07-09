//! WebContext - actor context specialized for the Web environment.
//!
//! Differences from `actr_framework::Context`:
//! - No `Send + Sync` requirement because Service Workers are single-threaded
//! - Uses `?Send` in async traits
//! - Adjusted to browser limitations
//!
//! # Design
//!
//! - `RuntimeBridge`: bridge trait that decouples `context.rs` from `runtime.rs`
//! - `WebContext`: full actor context trait covering RPC, discovery, streams, and more
//! - `RuntimeContext` in `context.rs`: the only implementation, used directly by `ServiceHandlerFn`

use actr_protocol::{ActorResult, ActrId, ActrType, RpcRequest};
use bytes::Bytes;

/// RuntimeBridge abstracts the runtime infrastructure.
///
/// Provides the lower-level capabilities needed by RuntimeContext, such as
/// pending RPC registration, discovery, and connection management.
/// This keeps `context.rs` from depending directly on `runtime.rs`.
///
/// The implementation (`SwRuntimeBridge`) lives in `runtime.rs` and holds references
/// to `SwRuntime`, `System`, and related components.
#[async_trait::async_trait(?Send)]
pub trait RuntimeBridge {
    /// Register a pending RPC so `handle_fast_path` can recognize the response.
    fn register_pending_rpc(&self, request_id: String);

    /// Discover a target actor through the signaling service.
    async fn discover_target(&self, target_type: &ActrType) -> ActorResult<ActrId>;

    /// Ensure the WebRTC connection to the target actor is ready and register the
    /// `ActrId -> Dest` mapping.
    async fn ensure_connection(&self, target_id: &ActrId) -> ActorResult<()>;

    /// Register a stream callback handler.
    fn register_stream_handler(
        &self,
        stream_id: String,
        callback: Box<dyn FnMut(Bytes) + 'static>,
    ) -> ActorResult<()>;

    /// Unregister a stream callback handler.
    fn unregister_stream_handler(&self, stream_id: &str) -> ActorResult<()>;
}

/// Actor execution context for the Web environment.
///
/// Equivalent to the native Context trait but adapted for a single-threaded environment.
/// Includes all actor communication capabilities: typed/raw RPC, discovery, and stream operations.
///
/// `ServiceHandlerFn` uses `Rc<RuntimeContext>` directly instead of a trait object
/// so handlers can call all methods, including generic ones like `call<R>` and `tell<R>`.
#[async_trait::async_trait(?Send)]
pub trait WebContext {
    // ========== Basic Information ==========

    /// Return the current actor ID.
    fn self_id(&self) -> &ActrId;

    /// Return the caller actor ID.
    fn caller_id(&self) -> Option<&ActrId>;

    /// Return the distributed trace ID.
    fn trace_id(&self) -> &str;

    /// Return the unique request ID.
    fn request_id(&self) -> &str;

    // ========== RPC ==========

    /// Send a raw RPC request and wait for the response.
    ///
    /// Useful for cases like UnifiedDispatcher that need dynamic route keys.
    ///
    /// # Parameters
    /// - `target`: Target actor ID
    /// - `route_key`: Route key such as `"echo.EchoService.Echo"`
    /// - `payload`: Serialized request payload
    /// - `timeout_ms`: Timeout in milliseconds
    async fn call_raw(
        &self,
        target: &ActrId,
        route_key: &str,
        payload: &[u8],
        timeout_ms: i64,
    ) -> ActorResult<Vec<u8>>;

    /// Send a raw fire-and-forget message without waiting for a response.
    ///
    /// The envelope is stamped `Direction::Tell`; the receiver runs the
    /// handler but never replies, and no pending entry is registered here.
    /// `payload` is moved into the envelope with no extra copy.
    async fn tell_raw(&self, target: &ActrId, route_key: &str, payload: Vec<u8>)
    -> ActorResult<()>;

    /// Discover the target actor through signaling service discovery.
    async fn discover(&self, target_type: &ActrType) -> ActorResult<ActrId>;

    // ========== Typed Communication ==========

    /// Send a typed RPC request and wait for the response.
    async fn call<R: RpcRequest>(&self, target: &ActrId, request: R) -> ActorResult<R::Response>;

    /// Send a one-way message without waiting for a response.
    async fn tell<R: RpcRequest>(&self, target: &ActrId, request: R) -> ActorResult<()>;

    // ========== Stream Registration ==========

    /// Register a stream data handler.
    ///
    /// After registration, `STREAM_*` data sent by the target actor is delivered
    /// directly to the callback through the Fast Path.
    async fn register_stream(
        &self,
        stream_id: String,
        callback: Box<dyn FnMut(Bytes) + 'static>,
    ) -> ActorResult<()>;

    /// Unregister a stream data handler.
    async fn unregister_stream(&self, stream_id: &str) -> ActorResult<()>;

    /// Register a media track handler.
    ///
    /// Used for Fast Path handling of WebRTC MediaTrack audio/video traffic.
    async fn register_media_track(
        &self,
        track_id: String,
        callback: Box<dyn FnMut(Bytes) + 'static>,
    ) -> ActorResult<()>;

    /// Unregister a media track handler.
    async fn unregister_media_track(&self, track_id: &str) -> ActorResult<()>;

    // ========== Stream Sending ==========

    /// Send media sample data such as RTP packets.
    ///
    /// Uses the Fast Path with latency below 1 ms.
    async fn send_media_sample(
        &self,
        target: &ActrId,
        track_id: &str,
        data: Bytes,
    ) -> ActorResult<()>;

    /// Send stream data.
    ///
    /// Uses the Fast Path with latency around 3 ms.
    async fn send_data_chunk(
        &self,
        target: &ActrId,
        stream_id: &str,
        data: Bytes,
    ) -> ActorResult<()>;
}
