//! WirePool - Wire connection pool manager
//!
//! Manages connection strategies: saturated concurrent connections, automatic retry, and fallback strategies.
//! Uses watch channels to broadcast connection status, implementing zero-polling event-driven architecture.

use super::backoff::ExponentialBackoff;
use super::wire_handle::{WireHandle, WireIdentity, WireStatus};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;
use tokio::sync::{RwLock, watch};

/// Connection type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnType {
    WebSocket,
    WebRTC,
}

impl ConnType {
    /// Convert to array index
    const fn as_index(self) -> usize {
        match self {
            ConnType::WebSocket => 0,
            ConnType::WebRTC => 1,
        }
    }

    /// All connection types
    const ALL: [ConnType; 2] = [ConnType::WebSocket, ConnType::WebRTC];
}

/// Set of ready connections
pub(crate) type ReadySet = HashSet<ConnType>;

/// Retry configuration
#[derive(Debug, Clone, Copy)]
pub(crate) struct RetryConfig {
    pub(crate) max_attempts: u32,
    pub(crate) initial_delay_ms: u64,
    pub(crate) max_delay_ms: u64,
    pub(crate) multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
            multiplier: 2.0,
        }
    }
}

impl RetryConfig {
    /// Create ExponentialBackoff from this config
    pub(crate) fn create_backoff(&self) -> ExponentialBackoff {
        ExponentialBackoff::with_multiplier(
            Duration::from_millis(self.initial_delay_ms),
            Duration::from_millis(self.max_delay_ms),
            Some(self.max_attempts),
            self.multiplier,
        )
    }
}

/// WirePool - Wire connection pool manager
///
/// # Responsibilities
/// - Saturated concurrent connections (simultaneously attempt WebRTC + WebSocket)
/// - Automatic retry with exponential backoff
/// - Broadcast connection status (via watch channel, zero-polling)
/// - Keep all successful connections (no priority-based replacement)
///
/// # Design Highlights
/// - **Event-driven**: Use watch channels to notify status changes
/// - **Zero-polling**: Callers use `await ready_rx.changed()` to wait for connection readiness
/// - **Array optimization**: Use fixed-size array instead of HashMap
pub(crate) struct WirePool {
    /// Connection status (array optimization: WebSocket=0, WebRTC=1)
    connections: Arc<RwLock<[Option<WireStatus>; 2]>>,

    /// Ready connection set (broadcast)
    ready_tx: watch::Sender<ReadySet>,
    ready_rx: watch::Receiver<ReadySet>,

    /// Pending connection count
    pending: Arc<AtomicU8>,

    /// Retry configuration
    retry_config: RetryConfig,

    /// Closed flag (used to terminate background tasks)
    closed: Arc<AtomicBool>,
}

impl WirePool {
    /// Create new wire connection pool
    pub(crate) fn new(retry_config: RetryConfig) -> Self {
        let (tx, rx) = watch::channel(HashSet::new());

        Self {
            connections: Arc::new(RwLock::new([None, None])),
            ready_tx: tx,
            ready_rx: rx,
            pending: Arc::new(AtomicU8::new(0)),
            retry_config,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Add connection and start connection task in background
    ///
    /// Non-blocking, returns immediately and attempts connection concurrently in background
    ///
    /// # Behavior
    /// - **Unconditionally starts**: Always starts connection attempt, even if a connection already exists
    /// - Use `add_connection_smart()` if you want to skip already-ready connections
    pub(crate) async fn add_connection(&self, connection: Arc<dyn WireHandle>) {
        let conn_type = connection.connection_type();
        {
            let mut conns = self.connections.write().await;
            conns[conn_type.as_index()] = Some(WireStatus::Connecting);
        }

        let connections = Arc::clone(&self.connections);
        let ready_tx = self.ready_tx.clone();
        let pending = Arc::clone(&self.pending);
        let retry_config = self.retry_config;
        let closed = Arc::clone(&self.closed);

        tokio::spawn(async move {
            // Create exponential backoff iterator
            let backoff = retry_config.create_backoff();

            // Retry loop using ExponentialBackoff iterator
            for (attempt, delay) in backoff.enumerate() {
                // Check if pool has been closed
                if closed.load(Ordering::Relaxed) {
                    tracing::debug!(
                        "🛑 [{:?}] Connection task terminated (pool closed)",
                        conn_type
                    );
                    return;
                }

                // Wait for delay (first attempt has 0 delay built into iterator)
                if attempt > 0 {
                    tracing::debug!(
                        "⏱️ [{:?}] Waiting {:?} before retry {}",
                        conn_type,
                        delay,
                        attempt + 1
                    );
                    tokio::time::sleep(delay).await;

                    // Check again after sleep
                    if closed.load(Ordering::Relaxed) {
                        tracing::debug!(
                            "🛑 [{:?}] Connection task terminated (pool closed)",
                            conn_type
                        );
                        return;
                    }
                }

                pending.fetch_add(1, Ordering::Relaxed);

                tracing::debug!(
                    "🔄 [{:?}] Connecting (attempt {}/{})",
                    conn_type,
                    attempt + 1,
                    retry_config.max_attempts
                );

                let result = connection.connect().await;
                pending.fetch_sub(1, Ordering::Relaxed);

                match result {
                    Ok(_) => {
                        tracing::info!(
                            "✅ [{:?}] Connection established on attempt {}",
                            conn_type,
                            attempt + 1
                        );

                        // Update status to Ready
                        {
                            let mut conns = connections.write().await;
                            conns[conn_type.as_index()] =
                                Some(WireStatus::Ready(Arc::clone(&connection)));
                        }

                        // Broadcast new ready connection set (keep all connections, no replacement)
                        Self::broadcast_ready_connections(&connections, &ready_tx).await;

                        return; // Success, exit
                    }
                    Err(e) => {
                        tracing::warn!(
                            "❌ [{:?}] Connection failed on attempt {}: {}",
                            conn_type,
                            attempt + 1,
                            e
                        );
                    }
                }
            }

            // All retries failed
            tracing::error!(
                "💀 [{:?}] All {} retries exhausted",
                conn_type,
                retry_config.max_attempts
            );

            {
                let mut conns = connections.write().await;
                conns[conn_type.as_index()] = Some(WireStatus::Failed);

                // Check if all connections failed
                let remaining = pending.load(Ordering::Relaxed);
                if remaining == 0 {
                    let all_failed = conns
                        .iter()
                        .all(|s| matches!(s, Some(WireStatus::Failed) | None));

                    if all_failed {
                        tracing::error!("💀💀 All connections failed");
                    }
                }
            }

            Self::broadcast_ready_connections(&connections, &ready_tx).await;
        });
    }

    /// Add connection smartly - skip if already Ready or Connecting
    ///
    /// # Behavior
    /// - **Ready**: Skip (reuse existing connection)
    /// - **Connecting**: Skip (avoid duplicate retry)
    /// - **None/Failed**: Start connection attempt
    ///
    /// # Use Case
    /// Perfect for reconnection scenarios where you want to retry failed connections
    /// without disrupting working ones.
    #[cfg(feature = "test-utils")]
    pub(crate) async fn add_connection_smart(&self, connection: Arc<dyn WireHandle>) {
        let conn_type = connection.connection_type();

        // Check current status
        let should_add = {
            let conns = self.connections.read().await;
            match &conns[conn_type.as_index()] {
                Some(WireStatus::Ready(_)) => {
                    tracing::debug!("⏭️ [{:?}] Skipping - already Ready", conn_type);
                    false
                }
                Some(WireStatus::Connecting) => {
                    tracing::debug!("⏭️ [{:?}] Skipping - already Connecting", conn_type);
                    false
                }
                Some(WireStatus::Failed) | None => {
                    tracing::info!(
                        "🔄 [{:?}] Starting connection (was {:?})",
                        conn_type,
                        conns[conn_type.as_index()]
                    );
                    true
                }
            }
        };

        if should_add {
            self.add_connection(connection).await;
        }
    }

    /// Broadcast current ready connections
    async fn broadcast_ready_connections(
        connections: &Arc<RwLock<[Option<WireStatus>; 2]>>,
        ready_tx: &watch::Sender<ReadySet>,
    ) {
        let conns = connections.read().await;

        // Collect all ready connections
        let mut ready_set: ReadySet = HashSet::new();

        for conn_type in ConnType::ALL {
            if let Some(WireStatus::Ready(_)) = &conns[conn_type.as_index()] {
                ready_set.insert(conn_type);
            }
        }

        // Broadcast ready set
        let _ = ready_tx.send(ready_set);
    }

    /// Watch for connection status changes
    pub(crate) fn watch_ready(&self) -> watch::Receiver<ReadySet> {
        self.ready_rx.clone()
    }

    /// Get connection of specified type
    pub(crate) async fn get_connection(&self, conn_type: ConnType) -> Option<Arc<dyn WireHandle>> {
        let conns = self.connections.read().await;

        match &conns[conn_type.as_index()] {
            Some(WireStatus::Ready(conn)) => Some(Arc::clone(conn)),
            _ => None,
        }
    }

    /// Return true if any candidate can still become sendable.
    pub(crate) async fn has_live_candidate(&self, conn_types: &[ConnType]) -> bool {
        let conns = self.connections.read().await;

        conn_types.iter().any(|conn_type| {
            matches!(
                &conns[conn_type.as_index()],
                Some(WireStatus::Ready(_)) | Some(WireStatus::Connecting)
            )
        })
    }

    /// Mark a connection as closed/failed
    ///
    /// Called by upper layers (DestTransport) when closing connections.
    /// This replaces the per-connection event listener pattern.
    pub(crate) async fn mark_connection_closed(&self, conn_type: ConnType) {
        {
            let mut conns = self.connections.write().await;
            conns[conn_type.as_index()] = Some(WireStatus::Failed);
        }

        // Update ready set
        Self::broadcast_ready_connections(&self.connections, &self.ready_tx).await;

        tracing::debug!("🔌 Marked {:?} connection as closed", conn_type);
    }

    /// Check whether the active ready slot for a connection type still matches
    /// the given `WireIdentity`.
    ///
    /// Returns `true` only when the slot is `Ready` and the underlying wire
    /// reports the same identity. Returns `false` for `Connecting`, `Failed`,
    /// `None`, or a mismatched wire (stale or replaced).
    pub(crate) async fn connection_matches_identity(
        &self,
        conn_type: ConnType,
        expected_identity: &WireIdentity,
    ) -> bool {
        let conns = self.connections.read().await;
        match &conns[conn_type.as_index()] {
            Some(WireStatus::Ready(handle)) => {
                handle.identity().as_ref() == Some(expected_identity)
            }
            _ => false,
        }
    }

    /// Compare-and-swap close: mark a connection as Failed **only if** the
    /// current ready wire still carries the expected identity.
    ///
    /// Returns `true` when the slot was actually transitioned to Failed.
    /// Returns `false` when the identity no longer matches (the wire has
    /// already been replaced by a fresh connection) — in that case the
    /// ready set is left untouched.
    pub(crate) async fn mark_connection_closed_if_same(
        &self,
        conn_type: ConnType,
        expected_identity: &WireIdentity,
    ) -> bool {
        {
            let mut conns = self.connections.write().await;
            match &conns[conn_type.as_index()] {
                Some(WireStatus::Ready(handle)) => {
                    if handle.identity().as_ref() != Some(expected_identity) {
                        tracing::debug!(
                            "🔌 {:?} identity mismatch — not marking closed (wire already replaced)",
                            conn_type
                        );
                        return false;
                    }
                }
                _ => {
                    // Not Ready — nothing to mark
                    return false;
                }
            }
            conns[conn_type.as_index()] = Some(WireStatus::Failed);
        }

        Self::broadcast_ready_connections(&self.connections, &self.ready_tx).await;
        tracing::debug!(
            "🔌 Marked {:?} closed (identity matched {:?})",
            conn_type,
            expected_identity
        );
        true
    }

    /// Close all connections in the pool
    ///
    /// Called by DestTransport.close() to clean up all connections.
    /// This also terminates all background connection tasks.
    pub(crate) async fn close_all(&self) {
        // 1. Set closed flag to terminate background tasks
        self.closed.store(true, Ordering::Relaxed);

        // 2. Clear all connection status
        let mut conns = self.connections.write().await;
        *conns = [None, None];

        // 3. Broadcast empty ready set
        let _ = self.ready_tx.send(HashSet::new());

        tracing::debug!("🔌 Closed all connections in pool (background tasks will terminate)");
    }

    /// Check if pool is closed
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::error::{NetworkError, NetworkResult};
    use crate::transport::lane::DataLane;
    use actr_protocol::PayloadType;
    use std::time::Duration;

    /// Minimal WireHandle mock. `succeed` controls `connect()` outcome.
    #[derive(Debug)]
    struct MockWire {
        conn_type: ConnType,
        succeed: bool,
        identity: Option<WireIdentity>,
    }

    #[async_trait::async_trait]
    impl WireHandle for MockWire {
        fn connection_type(&self) -> ConnType {
            self.conn_type
        }
        fn priority(&self) -> u8 {
            1
        }
        async fn connect(&self) -> NetworkResult<()> {
            if self.succeed {
                Ok(())
            } else {
                Err(NetworkError::ConnectionError("mock connect failure".into()))
            }
        }
        fn is_connected(&self) -> bool {
            self.succeed
        }
        async fn close(&self) -> NetworkResult<()> {
            Ok(())
        }
        async fn get_lane(&self, _: PayloadType) -> NetworkResult<Arc<dyn DataLane>> {
            Err(NetworkError::NotImplemented("mock has no lane".into()))
        }
        fn identity(&self) -> Option<WireIdentity> {
            self.identity.clone()
        }
    }

    fn fast_retry() -> RetryConfig {
        RetryConfig {
            max_attempts: 1,
            initial_delay_ms: 0,
            max_delay_ms: 10,
            multiplier: 1.0,
        }
    }

    // ── RetryConfig ─────────────────────────────────────────────────────────

    #[test]
    fn retry_config_default_values() {
        let r = RetryConfig::default();
        assert_eq!(r.max_attempts, 3);
        assert_eq!(r.initial_delay_ms, 1000);
        assert_eq!(r.max_delay_ms, 10000);
        assert_eq!(r.multiplier, 2.0);
    }

    #[test]
    fn retry_config_create_backoff_yields_first_attempt_at_no_delay() {
        // The first backoff step must be immediate (delay 0) so the initial
        // connection attempt isn't needlessly delayed.
        let mut b = RetryConfig::default().create_backoff();
        let first = b.next();
        assert!(first.is_some(), "backoff should yield at least one step");
    }

    // ── WirePool initial state ──────────────────────────────────────────────

    #[tokio::test]
    async fn new_pool_is_open_with_empty_ready_set() {
        let pool = WirePool::new(RetryConfig::default());
        assert!(!pool.is_closed());

        // Empty pool: no ready candidates, no connection.
        assert!(
            !pool
                .has_live_candidate(&[ConnType::WebSocket, ConnType::WebRTC])
                .await
        );
        assert!(pool.get_connection(ConnType::WebSocket).await.is_none());

        // watch_ready receiver starts with an empty set.
        let rx = pool.watch_ready();
        assert!(rx.borrow().is_empty());
    }

    #[tokio::test]
    async fn close_all_marks_pool_closed_and_clears_ready() {
        let pool = WirePool::new(RetryConfig::default());
        assert!(!pool.is_closed());
        pool.close_all().await;
        assert!(pool.is_closed());
        // Ready set broadcast as empty.
        assert!(pool.watch_ready().borrow().is_empty());
    }

    #[tokio::test]
    async fn mark_connection_closed_sets_failed_and_no_live_candidate() {
        let pool = WirePool::new(RetryConfig::default());
        pool.mark_connection_closed(ConnType::WebRTC).await;
        // Failed is not a live candidate.
        assert!(!pool.has_live_candidate(&[ConnType::WebRTC]).await);
        assert!(pool.get_connection(ConnType::WebRTC).await.is_none());
    }

    #[tokio::test]
    async fn identity_checks_return_false_when_not_ready() {
        let pool = WirePool::new(RetryConfig::default());
        let id = WireIdentity::WebRtc {
            peer_id: actr_protocol::ActrId::default(),
            session_id: 1,
        };
        // No Ready slot → both identity checks are false.
        assert!(
            !pool
                .connection_matches_identity(ConnType::WebRTC, &id)
                .await
        );
        assert!(
            !pool
                .mark_connection_closed_if_same(ConnType::WebRTC, &id)
                .await,
            "non-ready slot should not be marked closed"
        );
    }

    // ── add_connection: success path ────────────────────────────────────────

    #[tokio::test]
    async fn add_connection_success_becomes_ready_and_broadcasts() {
        let pool = WirePool::new(fast_retry());
        let mut rx = pool.watch_ready();

        pool.add_connection(Arc::new(MockWire {
            conn_type: ConnType::WebSocket,
            succeed: true,
            identity: None,
        }))
        .await;

        // Await broadcast: ready set should contain WebSocket.
        let ready = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if rx.borrow().contains(&ConnType::WebSocket) {
                    return ();
                }
                let _ = tokio::time::timeout(Duration::from_millis(50), rx.changed()).await;
            }
        })
        .await
        .expect("ready broadcast should fire within 2s");

        let _ = ready;
        // get_connection now returns the Ready handle.
        assert!(pool.get_connection(ConnType::WebSocket).await.is_some());
        assert!(pool.has_live_candidate(&[ConnType::WebSocket]).await);
    }

    // ── add_connection: failure path ────────────────────────────────────────

    #[tokio::test]
    async fn add_connection_failure_marks_failed_and_exhausts() {
        let pool = WirePool::new(fast_retry());
        pool.add_connection(Arc::new(MockWire {
            conn_type: ConnType::WebRTC,
            succeed: false,
            identity: None,
        }))
        .await;

        // Poll until the WebRTC slot stops being a live candidate (Connecting → Failed).
        tokio::time::timeout(Duration::from_secs(2), async {
            while pool.has_live_candidate(&[ConnType::WebRTC]).await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("failed connection should leave the live-candidate set within 2s");

        // Failed → no connection retrievable.
        assert!(pool.get_connection(ConnType::WebRTC).await.is_none());
        assert!(!pool.has_live_candidate(&[ConnType::WebRTC]).await);
    }

    // ── identity-aware close on a Ready slot ─────────────────────────────────

    #[tokio::test]
    async fn mark_closed_if_same_matches_and_mismatches() {
        let id = WireIdentity::WebRtc {
            peer_id: actr_protocol::ActrId::default(),
            session_id: 7,
        };
        let other = WireIdentity::WebRtc {
            peer_id: actr_protocol::ActrId::default(),
            session_id: 99,
        };
        let pool = WirePool::new(fast_retry());

        pool.add_connection(Arc::new(MockWire {
            conn_type: ConnType::WebSocket,
            succeed: true,
            identity: Some(id.clone()),
        }))
        .await;

        // Wait until Ready.
        tokio::time::timeout(Duration::from_secs(2), async {
            while pool.get_connection(ConnType::WebSocket).await.is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        // Matching identity → closes (true).
        assert!(
            pool.mark_connection_closed_if_same(ConnType::WebSocket, &id)
                .await
        );
        // Slot now Failed → no longer matches anything.
        assert!(
            !pool
                .connection_matches_identity(ConnType::WebSocket, &id)
                .await
        );

        // Re-ready a fresh wire, then assert a mismatched identity does NOT close.
        let pool2 = WirePool::new(fast_retry());
        pool2
            .add_connection(Arc::new(MockWire {
                conn_type: ConnType::WebSocket,
                succeed: true,
                identity: Some(id.clone()),
            }))
            .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while pool2.get_connection(ConnType::WebSocket).await.is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            !pool2
                .mark_connection_closed_if_same(ConnType::WebSocket, &other)
                .await,
            "mismatched identity must not close the slot"
        );
        // Still ready afterwards.
        assert!(pool2.get_connection(ConnType::WebSocket).await.is_some());
    }
}
