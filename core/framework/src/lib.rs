//! # actr-framework
//!
//! Actor-RTC core programming interface layer - defines the contract for user-framework interaction.
//!
//! ## Architectural Positioning
//!
//! `actr-framework` is the **SDK interface layer** of the actr system, positioned in the middle tier:
//!
//! ```text
//! User Application (Workload implementation)
//!        ↓ depends on
//! actr-framework (this crate)  ← Stable API contract (trait definitions only)
//!        ↓ depends on
//! actr-protocol                ← Data type definitions
//!        ↑ implements
//! actr-runtime                 ← Runtime implementation (implements Context trait)
//! ```
//!
//! ## Core Responsibilities
//!
//! 1. **Define user programming interface**: `Workload`, `MessageDispatcher` traits
//! 2. **Define execution context interface**: `Context` trait (implemented by runtime)
//! 3. **Type-safe RPC**: `Context::call()` and `Context::tell()` methods
//! 4. **Lifecycle management**: `on_start`, `on_stop` hooks
//!
//! ## Design Principles
//!
//! ### 1. Interface-only, Zero Implementation
//!
//! ```rust,ignore
//! // ✅ Framework defines
//! pub trait Context {
//!     async fn call<R: RpcRequest>(...) -> ActorResult<R::Response>;
//! }
//!
//! // ✅ Runtime implements
//! impl Context for RuntimeContext { ... }
//! ```
//!
//! Framework **contains no implementation code**, all logic is in runtime.
//!
//! ### 2. Dependency Inversion Principle
//!
//! - Framework defines traits, Runtime implements traits
//! - User code only depends on framework, not runtime
//! - Context trait can be mocked for unit testing
//!
//! ### 3. Zero-Cost Abstraction
//!
//! - Use generics instead of trait objects (`<C: Context>` not `&dyn Context`)
//! - Compile-time monomorphization, static dispatch
//! - Compiler can fully inline the entire call chain
//! - Zero virtual function call overhead
//!
//! ## Core Type System
//!
//! ### 4-Trait Architecture
//!
//! actr builds a type-safe message handling system with 4 traits:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │ 1. RpcRequest trait (actr-protocol)                     │
//! │    - Associates Request and Response types              │
//! │    - Provides route_key() static method                 │
//! └─────────────────────────────────────────────────────────┘
//!                         ↓ used by
//! ┌─────────────────────────────────────────────────────────┐
//! │ 2. Concrete Handler trait (code generated)             │
//! │    - e.g., EchoServiceHandler<C: Context>               │
//! │    - async fn echo<C: Context>(&self, req, ctx: &C)     │
//! └─────────────────────────────────────────────────────────┘
//!                         ↓ wrapped by
//! ┌─────────────────────────────────────────────────────────┐
//! │ 3. MessageDispatcher trait (this crate)                 │
//! │    - Static routing: route_key → handler method         │
//! │    - Zero-sized type (ZST), zero runtime overhead       │
//! └─────────────────────────────────────────────────────────┘
//!                         ↓ associated with
//! ┌─────────────────────────────────────────────────────────┐
//! │ 4. Workload trait (this crate)                          │
//! │    - Associates Dispatcher type                         │
//! │    - Provides on_start(), on_stop() hooks               │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! use actr_framework::{Context, Workload};
//!
//! // Code-generated Handler trait
//! #[async_trait]
//! pub trait EchoServiceHandler: Send + Sync + 'static {
//!     async fn echo<C: Context>(
//!         &self,
//!         req: EchoRequest,
//!         ctx: &C,
//!     ) -> ActorResult<EchoResponse>;
//! }
//!
//! // User implements business logic
//! impl EchoServiceHandler for MyService {
//!     async fn echo<C: Context>(
//!         &self,
//!         req: EchoRequest,
//!         ctx: &C,
//!     ) -> ActorResult<EchoResponse> {
//!         // Access context data
//!         tracing::info!("trace_id: {}", ctx.trace_id());
//!
//!         // Type-safe RPC call
//!         let response = ctx.call(&target, another_request).await?;
//!
//!         Ok(EchoResponse {
//!             reply: format!("Echo: {}", req.message),
//!         })
//!     }
//! }
//! ```

// Module declarations
mod context;
mod dest;
mod dispatcher;
mod service_handler;
mod workload;

// Optional utilities module
pub mod util;

// Guest-side runtime module (WASM and dynclib ABI)
pub mod guest;

// Web-target (`wasm32-unknown-unknown`) runtime glue. Compiled only when
// the `web` feature is enabled on a wasm32 build — see `src/web/mod.rs`
// for the target / feature gating rationale.
#[cfg(all(target_arch = "wasm32", feature = "web"))]
pub mod web;

// Test helpers (lightweight Context implementation)
// Only compiled under the test-utils feature (includes uuid v4, incompatible with wasm32 target)
#[cfg(feature = "test-utils")]
pub mod test_support;

// Public re-exports
pub use context::{Context, LogLevel, MaybeSendBoxFuture, MaybeSendSync, MediaSample, MediaType};
pub use dest::Dest;
pub use dispatcher::MessageDispatcher;
pub use service_handler::ServiceHandler;
pub use workload::{
    BackpressureEvent, CredentialEvent, ErrorCategory, ErrorEvent, PeerEvent, WebRtcPeerStatus,
    Workload,
};

// Re-export commonly used types for user convenience
pub use bytes::Bytes;

// Re-export async_trait to avoid users having to add it as a dependency
pub use async_trait::async_trait;

// Re-export DataChunk from protocol
pub use actr_protocol::DataChunk;

// Re-export backoff utilities
pub use util::backoff::{BackoffBuilder, ExponentialBackoff};
