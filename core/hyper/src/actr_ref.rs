//! ActrRef - Lightweight reference to a running Actor
//!
//! # Key Characteristics
//!
//! - **Cloneable**: Can be shared across tasks
//! - **Lightweight**: Contains only an `Arc` to shared state
//! - **Auto-cleanup**: Last `ActrRef` drop triggers resource cleanup
//!
//! # Usage
//!
//! ```rust,ignore
//! let actr = Node::from_config_file("actr.toml")
//!     .await?
//!     .attach(&package)
//!     .await?
//!     .register(&ais_endpoint)
//!     .await?
//!     .start()
//!     .await?;
//!
//! println!("actor id = {:?}", actr.actor_id());
//!
//! // Wait for process signals and then perform a graceful shutdown.
//! actr.wait_for_ctrl_c_and_shutdown().await?;
//! ```
//!
//! The typestate chain is `Node<Init> → Node<Attached> → Node<Registered>
//! → ActrRef`. `Node::from_hyper` is the escape hatch when you need to own
//! `HyperConfig` construction yourself.

use crate::context::{BootstrapContextBuilder, RuntimeContext};
use crate::lifecycle::CredentialState;
use crate::lifecycle::session_state::SessionState;
use actr_framework::{Context as _, Dest};
use actr_protocol::{ActorResult, ActrError, ActrId, ActrType, RpcRequest};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Shared state between all `ActrRef` clones
///
/// This is an internal implementation detail. When the last `ActrRef` is dropped,
/// this struct's `Drop` impl will trigger shutdown and cleanup all resources.
pub(crate) struct ActrRefShared {
    /// Actor ID (stored for fast access; `session_state` is the source of truth
    /// when available).
    pub(crate) actor_id: ActrId,
    /// Builder used to materialize application-side runtime contexts on demand.
    pub(crate) bootstrap_ctx_builder: BootstrapContextBuilder,
    /// Current credential state for building application-side contexts
    /// (legacy — being replaced by `session_state`).
    pub(crate) credential_state: CredentialState,
    /// Unified session state (replaces `credential_state` + scattered identity
    /// when set). `None` during transition; will become required.
    pub(crate) session_state: Option<SessionState>,
    /// Shutdown signal
    pub(crate) shutdown_token: CancellationToken,
    /// Background task handles (receive loops, WebRTC coordinator, etc.)
    pub(crate) task_handles: Mutex<Vec<JoinHandle<()>>>,
}

/// ActrRef - Lightweight reference to a running Actor
///
/// This is the primary handle returned by `ActrNode::start()`.
pub struct ActrRef {
    pub(crate) shared: Arc<ActrRefShared>,
}

impl Clone for ActrRef {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl ActrRef {
    /// Get Actor ID (cloned from the current session snapshot when
    /// `SessionState` is available, otherwise from the stored field).
    ///
    /// This is now a cloned value, not a reference — the identity can
    /// change across a hard rebind, and callers should always read the
    /// current value, not a stale reference.
    pub fn actor_id(&self) -> ActrId {
        if let Some(ref session_state) = self.shared.session_state
            && let Some(actor_id) = session_state.actor_id_sync()
        {
            return actor_id;
        }
        self.shared.actor_id.clone()
    }

    /// Get a reference to the actor ID (legacy — prefer `actor_id()` clone).
    pub fn actor_id_ref(&self) -> &ActrId {
        &self.shared.actor_id
    }

    /// Return a clone of the `SessionState` handle, if set.
    pub fn session_state(&self) -> Option<SessionState> {
        self.shared.session_state.clone()
    }

    /// Call the local workload with a typed RPC request.
    ///
    /// Convenience wrapper around `app_context().call(&Dest::Workload, request)`.
    /// Use this from app-side code to invoke the local guest workload.
    pub async fn call<R: RpcRequest>(&self, request: R) -> ActorResult<R::Response> {
        self.app_context()
            .await
            .call(&Dest::Workload, request)
            .await
    }

    /// Call a remote actor directly with a typed RPC request.
    ///
    /// Convenience wrapper around `app_context().call(&Dest::Peer(target), request)`.
    /// Use this when the client has no local guest workload and calls the remote actor directly.
    pub async fn call_remote<R: RpcRequest>(
        &self,
        target: ActrId,
        request: R,
    ) -> ActorResult<R::Response> {
        self.app_context()
            .await
            .call(&Dest::Peer(target), request)
            .await
    }

    /// Discover route candidates for the given actor type.
    ///
    /// Returns up to `count` actor IDs registered under `target_type`.
    /// Convenience wrapper for app-side discovery without holding a `RuntimeContext`.
    ///
    /// Note: The signaling protocol currently returns one candidate per request.
    /// This method will make up to `count` requests to collect multiple unique candidates.
    pub async fn discover_route_candidates(
        &self,
        target_type: &ActrType,
        count: usize,
    ) -> ActorResult<Vec<ActrId>> {
        let ctx = self.app_context().await;
        let mut results = Vec::with_capacity(count);

        for _ in 0..count {
            match ctx.discover_route_candidate(target_type).await {
                Ok(id) => {
                    if !results.contains(&id) {
                        results.push(id);
                    }
                }
                Err(e) => {
                    // Return partial results if we have any, otherwise propagate error
                    if results.is_empty() {
                        return Err(e);
                    }
                    break;
                }
            }
        }
        Ok(results)
    }

    /// Create an application-side runtime context bound to this running actor.
    ///
    /// When `SessionState` is available, the context is built from the
    /// current snapshot (so soft renews propagate automatically). Otherwise
    /// falls back to the legacy `credential_state`.
    pub async fn app_context(&self) -> RuntimeContext {
        if let Some(ref ss) = self.shared.session_state {
            let credential = ss.credential().await;
            let actor_id = ss.actor_id().await;
            self.shared
                .bootstrap_ctx_builder
                .build_bootstrap(&actor_id, &credential)
        } else {
            let credential = self.shared.credential_state.credential().await;
            self.shared
                .bootstrap_ctx_builder
                .build_bootstrap(&self.shared.actor_id, &credential)
        }
    }

    /// Trigger Actor shutdown
    ///
    /// This signals the Actor to stop, but does not wait for completion.
    /// Use `wait_for_shutdown()` to wait for cleanup to finish.
    pub fn shutdown(&self) {
        tracing::info!(
            "🛑 Shutdown requested for Actor {}",
            actr_protocol::ActrId::to_string_repr(&self.shared.actor_id)
        );
        self.shared.shutdown_token.cancel();
    }

    /// Wait for Actor to fully shutdown
    ///
    /// This waits for the shutdown signal to be triggered.
    /// All background tasks will be aborted when the last `ActrRef` is dropped.
    pub async fn wait_for_shutdown(&self) {
        self.shared.shutdown_token.cancelled().await;
        // Take ownership of the current handles so we can await them as Futures.
        let mut guard = self.shared.task_handles.lock().await;
        let handles = std::mem::take(&mut *guard);
        drop(guard);
        tracing::debug!("Waiting for tasks to complete: {:?}", handles.len());

        // All tasks have been asked to shut down; wait for them with a timeout,
        // and abort any that don't finish in time to avoid leaking background work.
        for handle in handles {
            let sleep = tokio::time::sleep(Duration::from_secs(5));
            tokio::pin!(handle);
            tokio::pin!(sleep);

            tokio::select! {
                res = &mut handle => {
                    match res {
                        Ok(_) => {
                            tracing::debug!("Task completed");
                        }
                        Err(e) => {
                            tracing::error!("Task failed: {:?}", e);
                        }
                    }
                }
                _ = sleep => {
                    tracing::warn!("Task timed out after 5s, aborting");
                    handle.abort();
                }
            }
        }
    }

    /// Check if Actor is shutting down
    pub fn is_shutting_down(&self) -> bool {
        self.shared.shutdown_token.is_cancelled()
    }

    /// This consumes the `ActrRef` and waits for signal (Ctrl+C / SIGTERM),
    /// then triggers shutdown.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let actr = node.start().await?;
    /// actr.wait_for_ctrl_c_and_shutdown().await?;
    /// ```
    pub async fn wait_for_ctrl_c_and_shutdown(self) -> ActorResult<()> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let mut sigint = signal(SignalKind::interrupt()).map_err(|e| {
                ActrError::Unavailable(format!("Signal handler error (SIGINT): {e}"))
            })?;
            let mut sigterm = signal(SignalKind::terminate()).map_err(|e| {
                ActrError::Unavailable(format!("Signal handler error (SIGTERM): {e}"))
            })?;

            tokio::select! {
                _ = sigint.recv() => tracing::info!("📡 Received SIGINT (Ctrl+C) signal"),
                _ = sigterm.recv() => tracing::info!("📡 Received SIGTERM signal"),
            }
        }

        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .map_err(|e| ActrError::Unavailable(format!("Ctrl+C signal error: {e}")))?;
            tracing::info!("📡 Received Ctrl+C signal");
        }

        self.shutdown();
        self.wait_for_shutdown().await;
        Ok(())
    }
}

impl Drop for ActrRefShared {
    fn drop(&mut self) {
        tracing::info!(
            "🧹 ActrRefShared dropping - cleaning up Actor {}",
            actr_protocol::ActrId::to_string_repr(&self.actor_id)
        );

        // Cancel shutdown token
        self.shutdown_token.cancel();
        // Abort all background tasks (best-effort)
        if let Ok(mut handles) = self.task_handles.try_lock() {
            for handle in handles.drain(..) {
                handle.abort();
            }
        } else {
            tracing::warn!(
                "⚠️ Failed to lock task_handles mutex during Drop; some tasks may still be running"
            );
        }

        tracing::debug!(
            "✅ All background tasks aborted for Actor {}",
            actr_protocol::ActrId::to_string_repr(&self.actor_id)
        );
    }
}
