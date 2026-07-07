//! PeerTransport - Cross-process transport manager
//!
//! Manages transport layer for multiple Dests, providing unified send/recv interface
//!
//! # Naming Convention
//! - **PeerTransport**: Manages cross-process communication (WebRTC, WebSocket)
//! - **HostTransport**: Manages intra-process communication (mpsc channels)
//!
//! These two form a symmetric design, handling different transport scenarios

use super::Dest; // Re-exported from actr-framework
use super::dest_transport::DestTransport;
use super::error::{NetworkError, NetworkResult};
use super::wire_handle::{WireHandle, WireIdentity};
use actr_protocol::{ActrId, PayloadType};
use async_trait::async_trait;
use either::Either;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio_util::sync::CancellationToken;

/// Wire builder trait: asynchronously creates Wire components based on Dest
///
/// Implement this trait to customize Wire layer component creation logic (e.g., WebRTC, WebSocket)
#[async_trait]
pub trait WireBuilder: Send + Sync {
    /// Create Wire handle list to specified Dest
    ///
    /// # Arguments
    /// - `dest`: Target destination
    ///
    /// # Returns
    /// - Wire handle list (may contain multiple types: WebSocket, WebRTC, etc.)
    async fn create_connections(&self, dest: &Dest) -> NetworkResult<Vec<Arc<dyn WireHandle>>>;

    /// Create Wire handle list with cancellation support
    ///
    /// # Arguments
    /// - `dest`: Target destination
    /// - `cancel_token`: Optional cancellation token to terminate the operation
    ///
    /// # Returns
    /// - Wire handle list (may contain multiple types: WebSocket, WebRTC, etc.)
    /// - Returns error if cancelled
    ///
    /// Default implementation ignores the cancel token and calls `create_connections`.
    async fn create_connections_with_cancel(
        &self,
        dest: &Dest,
        cancel_token: Option<CancellationToken>,
    ) -> NetworkResult<Vec<Arc<dyn WireHandle>>> {
        // Check if already cancelled
        if let Some(ref token) = cancel_token {
            if token.is_cancelled() {
                return Err(NetworkError::ConnectionClosed(
                    "Connection creation cancelled".to_string(),
                ));
            }
        }

        // Default: just call create_connections
        self.create_connections(dest).await
    }
}

/// Destination transport state
///
/// Uses Either to manage connection lifecycle:
/// - Left: Connecting state with shared Notify (multiple waiters)
/// - Right: Connected state with DestTransport
type DestState = Either<Arc<Notify>, Arc<DestTransport>>;

/// Reference to the DestTransport that accepted a send.
///
/// `wire_identity` is available for WebRTC sessions. `transport` guards wires
/// without their own session identity, such as WebSocket or fake test wires,
/// so timeout cleanup cannot close a replacement DestTransport.
#[derive(Clone)]
pub(crate) struct DestTransportRef {
    transport: Weak<DestTransport>,
    pub(crate) wire_identity: Option<WireIdentity>,
}

impl DestTransportRef {
    fn new(transport: &Arc<DestTransport>, wire_identity: Option<WireIdentity>) -> Self {
        Self {
            transport: Arc::downgrade(transport),
            wire_identity,
        }
    }
}

/// PeerTransport - Cross-process transport manager
///
/// Responsibilities:
/// - Manage transport layer for multiple Dests (each Dest maps to one DestTransport)
/// - Create DestTransport on-demand (lazy initialization)
/// - Provide unified send/recv interface
/// - Support custom connection factories
/// - Prevent duplicate connection creation using Either state machine
///
/// # Comparison with HostTransport
/// - **PeerTransport**: Cross-process, uses WebRTC/WebSocket
/// - **HostTransport**: Intra-process, uses mpsc channels, zero serialization
///
/// # State Machine
/// ```text
/// None -> Connecting(Notify) -> Connected(Transport)
///         |                      |
///      (multiple waiters)     (ready)
/// ```
pub struct PeerTransport {
    /// Local Actor ID
    #[allow(dead_code)]
    local_id: ActrId,

    /// Dest -> DestState mapping (Either state machine)
    transports: Arc<RwLock<HashMap<Dest, DestState>>>,

    /// Wire builder (used to create Wire handles for new DestTransport)
    conn_factory: Arc<dyn WireBuilder>,

    /// Cancellation tokens for in-progress connection creation
    /// Dest -> CancellationToken (for cancelling ongoing connection attempts)
    pending_tokens: Arc<Mutex<HashMap<Dest, CancellationToken>>>,

    #[allow(unused)]
    /// todo: Set of peers currently being closed (to reject new connection attempts) ,closed requests will be cleaned up in event listener
    closing_peers: Arc<RwLock<HashSet<Dest>>>,
}

impl PeerTransport {
    /// Create new PeerTransport
    ///
    /// # Arguments
    /// - `local_id`: Local Actor ID
    /// - `conn_factory`: Wire builder, asynchronously creates Wire handle list based on Dest
    pub fn new(local_id: ActrId, conn_factory: Arc<dyn WireBuilder>) -> Self {
        Self {
            local_id,
            transports: Arc::new(RwLock::new(HashMap::new())),
            conn_factory,
            pending_tokens: Arc::new(Mutex::new(HashMap::new())),
            closing_peers: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Check if a destination is currently being closed
    #[allow(dead_code)]
    pub async fn is_closing(&self, dest: &Dest) -> bool {
        self.closing_peers.read().await.contains(dest)
    }

    /// Check whether a destination is currently in the Connecting state.
    pub async fn is_connecting(&self, dest: &Dest) -> bool {
        let transports = self.transports.read().await;
        matches!(transports.get(dest), Some(Either::Left(_)))
    }

    /// Get or create DestTransport for specified Dest
    ///
    /// # Arguments
    /// - `dest`: Target destination
    ///
    /// # Returns
    /// - DestTransport for this Dest (Arc-shared)
    ///
    /// # State Machine
    /// Uses Either to prevent duplicate connections:
    /// 1. If Connected -> return transport
    /// 2. If Connecting -> wait for notify, then retry
    /// 3. If None -> insert Connecting(notify), create connection outside lock
    #[cfg_attr(feature = "opentelemetry", tracing::instrument(
        skip_all,
        name = "PeerTransport.get_or_create_transport",
        fields(dest = ?dest.as_actor_id().map(|id| id))
    ))]
    pub(crate) async fn get_or_create_transport(
        &self,
        dest: &Dest,
    ) -> NetworkResult<Arc<DestTransport>> {
        // 0. Check if dest is being closed - fast fail
        if self.closing_peers.read().await.contains(dest) {
            return Err(NetworkError::ConnectionClosed(format!(
                "Destination {:?} is being closed.",
                dest
            )));
        }

        loop {
            // 1. Fast path: check current state
            let state_opt = {
                let transports = self.transports.read().await;
                transports.get(dest).cloned()
            };

            match state_opt {
                // Already connected - fast path
                Some(Either::Right(transport)) => {
                    tracing::debug!("Reusing existing DestTransport: {:?}", dest);
                    return Ok(transport);
                }
                // Currently connecting - wait for completion
                Some(Either::Left(notify)) => {
                    tracing::debug!("Waiting for ongoing connection: {:?}", dest);
                    notify.notified().await;
                    // Check if cancelled during wait
                    if self.closing_peers.read().await.contains(dest) {
                        return Err(NetworkError::ConnectionClosed(format!(
                            "Destination {:?} was closed while waiting",
                            dest
                        )));
                    }
                    // Retry after notification
                    continue;
                }
                // Not exists - need to create
                None => {
                    // Enter slow path
                }
            }

            // 2. Slow path: try to become the creator
            let (notify, is_creator) = {
                let mut transports = self.transports.write().await;

                // Double-check: may have been created while waiting for write lock
                match transports.get(dest) {
                    Some(Either::Right(transport)) => {
                        return Ok(Arc::clone(transport));
                    }
                    Some(Either::Left(notify)) => {
                        // Another thread is creating, wait for it
                        (Arc::clone(notify), false)
                    }
                    None => {
                        // Check closing again before creating
                        if self.closing_peers.read().await.contains(dest) {
                            return Err(NetworkError::ConnectionClosed(format!(
                                "Destination {:?} is being closed",
                                dest
                            )));
                        }
                        // We are the creator, insert Connecting state
                        let notify = Arc::new(Notify::new());
                        transports.insert(dest.clone(), Either::Left(Arc::clone(&notify)));
                        tracing::debug!("Inserted Connecting state for: {:?}", dest);
                        (notify, true)
                    }
                }
            };

            if !is_creator {
                // Wait for the actual creator
                tracing::debug!("Another thread is creating connection: {:?}", dest);
                // Add a 10-second timeout while waiting on notify.
                match tokio::time::timeout(Duration::from_secs(10), notify.notified()).await {
                    Ok(_) => continue,
                    Err(e) => {
                        return Err(NetworkError::TimeoutError(format!(
                            "Timeout waiting for notification: {:?} {}",
                            dest, e
                        )));
                    }
                }
            }

            // 3. We are the creator - create connections OUTSIDE lock
            tracing::info!("Creating new connection for: {:?}", dest);

            // Create cancellation token for this connection attempt
            let cancel_token = CancellationToken::new();
            {
                let mut tokens = self.pending_tokens.lock().await;
                tokens.insert(dest.clone(), cancel_token.clone());
            }

            let result = async {
                let connections = self
                    .conn_factory
                    .create_connections_with_cancel(dest, Some(cancel_token.clone()))
                    .await?;

                if cancel_token.is_cancelled() {
                    for conn in connections {
                        if let Err(e) = conn.close().await {
                            tracing::warn!(
                                "Failed to close connection created after cancellation for {:?}: {}",
                                dest,
                                e
                            );
                        }
                    }
                    return Err(NetworkError::ConnectionClosed(
                        "Connection creation cancelled".to_string(),
                    ));
                }

                if connections.is_empty() {
                    return Err(NetworkError::ConfigurationError(format!(
                        "Connection factory returned no connections: {dest:?}"
                    )));
                }

                tracing::info!(
                    "Creating DestTransport: {:?} ({} connections)",
                    dest,
                    connections.len()
                );
                let transport = DestTransport::new(dest.clone(), connections).await?;

                if cancel_token.is_cancelled() {
                    if let Err(e) = transport.close().await {
                        tracing::warn!(
                            "Failed to close DestTransport created after cancellation for {:?}: {}",
                            dest,
                            e
                        );
                    }
                    return Err(NetworkError::ConnectionClosed(
                        "Connection creation cancelled".to_string(),
                    ));
                }

                Ok(Arc::new(transport))
            }
            .await;

            // 4. Clean up pending token (connection attempt finished)
            {
                let mut tokens = self.pending_tokens.lock().await;
                tokens.remove(dest);
            }

            // 5. Update state and notify waiters
            let mut transports = self.transports.write().await;

            match result {
                Ok(transport) => {
                    tracing::info!("Connection established: {:?}", dest);
                    transports.insert(dest.clone(), Either::Right(Arc::clone(&transport)));
                    drop(transports);
                    self.spawn_ready_monitor(dest.clone(), Arc::clone(&transport));
                    notify.notify_waiters();
                    return Ok(transport);
                }
                Err(e) => {
                    tracing::error!("Connection failed: {:?}: {}", dest, e);
                    transports.remove(dest);
                    drop(transports);
                    notify.notify_waiters();
                    return Err(e);
                }
            }
        }
    }

    /// Send message to specified Dest
    ///
    /// # Arguments
    /// - `dest`: Target destination
    /// - `payload_type`: Message type
    /// - `data`: Message data
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// mgr.send(&dest, PayloadType::RpcSignal, b"hello").await?;
    /// ```
    pub async fn send(
        &self,
        dest: &Dest,
        payload_type: PayloadType,
        data: &[u8],
    ) -> NetworkResult<()> {
        self.send_with_identity(dest, payload_type, data)
            .await
            .map(|_| ())
    }

    pub(crate) async fn send_with_identity(
        &self,
        dest: &Dest,
        payload_type: PayloadType,
        data: &[u8],
    ) -> NetworkResult<DestTransportRef> {
        // Get or create DestTransport for this Dest
        let transport = self.get_or_create_transport(dest).await?;

        // Send through DestTransport
        let wire_identity = transport.send_with_identity(payload_type, data).await?;
        Ok(DestTransportRef::new(&transport, wire_identity))
    }

    /// Session-guarded close of only the WebRTC connection for the specified
    /// Dest, leaving the WebSocket connection (if any) alive.
    ///
    /// Close **only** the WebRTC connection for `dest` when its session still
    /// matches `(peer_id, session_id)`, leaving the WebSocket fallback lane
    /// (and any in-flight RPC responses it carries) intact.
    ///
    /// Returns `Ok(true)` when the WebRTC connection was actually closed.
    /// Returns `Ok(false)` when the event is stale (identity mismatch) or the
    /// transport state has already changed — in that case **no** pending
    /// requests should be cleaned, and the peer should be removed from
    /// `closing_peers` immediately.
    ///
    /// Called by the WebRTC connection-closed event handler so that outbound
    /// WebSocket response-reader tasks are not inadvertently terminated when a
    /// WebRTC peer disconnects.  The WebSocket connection will be cleaned up
    /// separately by `spawn_ready_monitor` once it too drops.
    ///
    /// # Naming hazard
    ///
    /// The near-identical name [`close_transport_if_webrtc_session`] (below)
    /// has the **opposite** teardown scope: it performs a *full* close
    /// (including WebSocket) under the same session guard. The two are easily
    /// confused — pick by the teardown scope you want, not by gut.
    pub(crate) async fn close_webrtc_transport_if_session(
        &self,
        dest: &Dest,
        peer_id: &ActrId,
        session_id: u64,
    ) -> NetworkResult<bool> {
        // Clone the current transport without awaiting under the map lock.
        let transport = {
            let transports = self.transports.read().await;
            match transports.get(dest) {
                Some(Either::Right(transport)) => Some(Arc::clone(transport)),
                _ => None, // Connecting or absent → stale
            }
        };

        let Some(transport) = transport else {
            tracing::debug!(
                "Stale close event for {:?} (session {} mismatch or transport absent)",
                dest,
                session_id
            );
            return Ok(false);
        };

        if !transport.matches_webrtc_session(peer_id, session_id).await {
            tracing::debug!(
                "Stale close event for {:?} (session {} mismatch or transport absent)",
                dest,
                session_id
            );
            return Ok(false);
        }

        tracing::debug!(
            "Closing only WebRTC connection for {:?} (WebSocket kept alive)",
            dest
        );
        transport
            .close_connection(super::wire_pool::ConnType::WebRTC)
            .await?;
        Ok(true)
    }

    /// Close DestTransport for specified Dest (full teardown, including WebSocket).
    ///
    /// # Arguments
    /// - `dest`: Target destination
    #[allow(dead_code)]
    pub async fn close_transport(&self, dest: &Dest) -> NetworkResult<()> {
        // 1. Mark as closing
        self.closing_peers.write().await.insert(dest.clone());

        // 2. Inspect current state first.
        //    If the destination is still in Connecting state, let the current creator
        //    finish its internal retry/cleanup path instead of converting a failed
        //    single attempt into a whole-operation cancellation.
        let current_state = {
            let transports = self.transports.read().await;
            transports.get(dest).cloned()
        };

        match current_state {
            Some(Either::Left(notify)) => {
                {
                    let mut tokens = self.pending_tokens.lock().await;
                    if let Some(token) = tokens.remove(dest) {
                        tracing::info!("Cancelling in-progress connection for {:?}", dest);
                        token.cancel();
                    }
                }

                {
                    let mut transports = self.transports.write().await;
                    if matches!(transports.get(dest), Some(Either::Left(_))) {
                        transports.remove(dest);
                    }
                }

                notify.notify_waiters();
            }
            Some(Either::Right(_)) => {
                // 3. Cancel any auxiliary pending connection creation state.
                {
                    let mut tokens = self.pending_tokens.lock().await;
                    if let Some(token) = tokens.remove(dest) {
                        tracing::info!("Cancelling in-progress connection for {:?}", dest);
                        token.cancel();
                    }
                }

                // 4. Remove and close the established transport.
                let state = {
                    let mut transports = self.transports.write().await;
                    transports.remove(dest)
                };

                if let Some(Either::Right(transport)) = state {
                    tracing::info!("Closing DestTransport: {:?}", dest);
                    transport.close().await?;
                }
            }
            None => {
                tracing::debug!(
                    "Ignoring close request for {:?}; no transport state exists",
                    dest
                );
            }
        }

        // 5. Remove from closing set after cleanup completes
        self.closing_peers.write().await.remove(dest);

        Ok(())
    }

    /// Session-guarded close: only tear down the transport if the active
    /// WebRTC wire still matches the given `(peer_id, session_id)` pair.
    ///
    /// Returns `Ok(true)` when the transport was actually closed.
    /// Returns `Ok(false)` when the event is stale (identity mismatch)
    /// or the transport state has already changed — in that case **no**
    /// pending requests should be cleaned, and the peer should be removed
    /// from `closing_peers` immediately.
    pub(crate) async fn close_transport_if_webrtc_session(
        &self,
        dest: &Dest,
        peer_id: &ActrId,
        session_id: u64,
    ) -> NetworkResult<bool> {
        // 1. Clone the current transport without awaiting under the map lock.
        let transport = {
            let transports = self.transports.read().await;
            match transports.get(dest) {
                Some(Either::Right(transport)) => Some(Arc::clone(transport)),
                _ => None, // Connecting or absent → stale
            }
        };

        let Some(transport) = transport else {
            tracing::debug!(
                "Stale close event for {:?} (session {} mismatch or transport absent)",
                dest,
                session_id
            );
            return Ok(false);
        };

        if !transport.matches_webrtc_session(peer_id, session_id).await {
            tracing::debug!(
                "Stale close event for {:?} (session {} mismatch or transport absent)",
                dest,
                session_id
            );
            return Ok(false);
        }

        // 2. Identity matches → proceed with the normal close path.
        self.close_transport(dest).await?;
        Ok(true)
    }

    /// Instance-guarded full close for transports without a wire session
    /// identity. This avoids letting an old request timeout close a replacement
    /// DestTransport for the same destination.
    pub(crate) async fn close_transport_if_current(
        &self,
        dest: &Dest,
        sent_transport: &DestTransportRef,
    ) -> NetworkResult<bool> {
        let Some(sent_transport) = sent_transport.transport.upgrade() else {
            tracing::debug!(
                "Skipped close for {:?}; sent DestTransport is already gone",
                dest
            );
            return Ok(false);
        };

        let matches_current = {
            let transports = self.transports.read().await;
            matches!(
                transports.get(dest),
                Some(Either::Right(transport)) if Arc::ptr_eq(transport, &sent_transport)
            )
        };

        if !matches_current {
            tracing::debug!(
                "Skipped close for {:?}; DestTransport instance no longer matches",
                dest
            );
            return Ok(false);
        }

        self.closing_peers.write().await.insert(dest.clone());

        let transport = {
            let mut transports = self.transports.write().await;
            match transports.get(dest) {
                Some(Either::Right(transport)) if Arc::ptr_eq(transport, &sent_transport) => {
                    match transports.remove(dest) {
                        Some(Either::Right(transport)) => Some(transport),
                        _ => None,
                    }
                }
                _ => None,
            }
        };

        let Some(transport) = transport else {
            tracing::debug!(
                "Skipped close for {:?}; DestTransport instance no longer matches",
                dest
            );
            self.closing_peers.write().await.remove(dest);
            return Ok(false);
        };

        tracing::info!("Closing current DestTransport for {:?}", dest);
        let result = transport.close().await.map(|_| true);

        self.closing_peers.write().await.remove(dest);
        result
    }

    /// Close all DestTransports
    #[allow(dead_code)]
    pub async fn close_all(&self) -> NetworkResult<()> {
        {
            let mut tokens = self.pending_tokens.lock().await;
            for (dest, token) in tokens.drain() {
                tracing::info!("Cancelling in-progress connection for {:?}", dest);
                token.cancel();
            }
        }

        let states = {
            let mut transports = self.transports.write().await;
            tracing::info!("Closing all DestTransports (count: {})", transports.len());
            transports.drain().collect::<Vec<_>>()
        };

        for (dest, state) in states {
            match state {
                Either::Right(transport) => {
                    self.closing_peers.write().await.insert(dest.clone());
                    if let Err(e) = transport.close().await {
                        tracing::warn!("Failed to close DestTransport {:?}: {}", dest, e);
                    }
                    self.closing_peers.write().await.remove(&dest);
                }
                Either::Left(notify) => {
                    self.closing_peers.write().await.insert(dest.clone());
                    tracing::debug!("Cancelled Connecting state for: {:?}", dest);
                    notify.notify_waiters();
                    self.closing_peers.write().await.remove(&dest);
                }
            }
        }

        Ok(())
    }

    /// Get count of currently managed Dests
    #[cfg(feature = "test-utils")]
    pub async fn dest_count(&self) -> usize {
        self.transports.read().await.len()
    }

    /// Get local Actor ID
    #[inline]
    #[cfg(feature = "test-utils")]
    pub fn local_id(&self) -> &ActrId {
        &self.local_id
    }

    /// List all connected Dests
    #[cfg(feature = "test-utils")]
    pub async fn list_dests(&self) -> Vec<Dest> {
        self.transports.read().await.keys().cloned().collect()
    }

    /// Check if connection to specified Dest exists
    #[cfg(feature = "test-utils")]
    pub async fn has_dest(&self, dest: &Dest) -> bool {
        self.transports.read().await.contains_key(dest)
    }

    /// Monitor a DestTransport ready-set and remove it when all connections are gone.
    fn spawn_ready_monitor(&self, dest: Dest, transport: Arc<DestTransport>) {
        let transports = Arc::clone(&self.transports);
        tokio::spawn(async move {
            let mut rx = transport.watch_ready();
            let mut had_ready = !rx.borrow().is_empty();

            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let ready = rx.borrow().clone();

                if ready.is_empty() && had_ready {
                    // Only remove if the same transport is still mapped.
                    let mut map = transports.write().await;
                    let matched = matches!(
                        map.get(&dest),
                        Some(Either::Right(existing)) if Arc::ptr_eq(existing, &transport)
                    );
                    if matched {
                        map.remove(&dest);
                        drop(map);

                        tracing::warn!(
                            "Removing DestTransport for {:?} after all connections closed",
                            dest
                        );
                        if let Err(e) = transport.close().await {
                            tracing::warn!("Failed to close DestTransport {:?}: {}", dest, e);
                        }
                    }
                    break;
                }

                if !ready.is_empty() {
                    had_ready = true;
                }
            }
        });
    }

    /// Spawn health checker background task with smart reconnect
    ///
    /// Periodically checks all DestTransport health status:
    /// - If some connections failed -> trigger smart reconnect (reuse working connections)
    /// - If all connections failed -> remove entire DestTransport
    ///
    /// # Arguments
    /// - `interval`: Health check interval (recommended: 10-30 seconds)
    ///
    /// # Returns
    /// - JoinHandle for the background task (can be used to cancel)
    ///
    /// # Example
    /// ```rust,ignore
    /// let mgr = Arc::new(PeerTransport::new(local_id, factory));
    /// let health_check_handle = mgr.spawn_health_checker(Duration::from_secs(10));
    /// ```
    #[cfg(feature = "test-utils")]
    pub fn spawn_health_checker(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let transports = Arc::clone(&self.transports);
        let conn_factory = Arc::clone(&self.conn_factory);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval_timer.tick().await;

                // Collect snapshot of connected Dests first (no async under lock)
                let snapshot: Vec<(Dest, Arc<DestTransport>)> = {
                    let transports_read = transports.read().await;

                    transports_read
                        .iter()
                        .filter_map(|(dest, state)| {
                            // Only check Connected transports, skip Connecting
                            if let Either::Right(transport) = state {
                                Some((dest.clone(), Arc::clone(transport)))
                            } else {
                                None
                            }
                        })
                        .collect()
                };

                // Process each Dest outside of the lock
                for (dest_clone, transport) in snapshot {
                    let healthy = transport.has_healthy_connection().await;

                    if !healthy {
                        // All connections failed - schedule for removal
                        tracing::warn!("All connections failed for {:?}, will remove", dest_clone);

                        // Remove entire DestTransport
                        let mut transports_write = transports.write().await;
                        if let Some(Either::Right(transport)) = transports_write.remove(&dest_clone)
                        {
                            tracing::info!(
                                "Removing completely failed DestTransport: {:?}",
                                dest_clone
                            );
                            // Drop lock before awaiting close
                            drop(transports_write);

                            if let Err(e) = transport.close().await {
                                tracing::warn!(
                                    "Failed to close DestTransport {:?}: {}",
                                    dest_clone,
                                    e
                                );
                            }
                        } else {
                            // State changed between snapshot and removal; skip safely
                            drop(transports_write);
                        }
                    } else {
                        // At least one connection is working
                        // Try to reconnect failed ones (smart reconnect)
                        tracing::debug!("Triggering smart reconnect for: {:?}", dest_clone);
                        if let Err(e) = transport
                            .retry_failed_connections(&dest_clone, conn_factory.as_ref())
                            .await
                        {
                            tracing::warn!("Smart reconnect failed for {:?}: {}", dest_clone, e);
                        }
                    }
                }
            }
        })
    }
}

impl Drop for PeerTransport {
    fn drop(&mut self) {
        tracing::debug!("PeerTransport dropped");
        // Note: async cleanup requires external call to close_all()
    }
}

#[cfg(test)]
#[path = "peer_transport_tests.rs"]
mod tests;
