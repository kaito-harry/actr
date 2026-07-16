//! Network Event Handling Architecture
//!
//! This module defines the network event handling infrastructure.
//!
//! # Architecture Overview
//!
//! ```text
//!        ┌─────────────────────────────────────────────┐
//!        │ (FFI Path - Implemented)  (Actor Path - TODO)
//!        ▼                                             ▼
//! ┌──────────────────────────┐      ┌──────────────────────────┐
//! │ NetworkEventHandle       │      │ Direct Proto Message     │
//! │ • Platform FFI calls     │      │ • Actor call/tell        │
//! │ • Send via channel       │      │ • Send to actor mailbox  │
//! │ • Await result           │      │ • No handle needed       │
//! └────────┬─────────────────┘      └──────┬───────────────────┘
//!          │                               │
//!          └───────────────┬───────────────┘
//!                          │ Both trigger
//!                          ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │  ActrNode::network_event_loop()                         │
//! │  • Receive event from channel (FFI path)                │
//! │  • Or handle message directly (Actor path - TODO)       │
//! │  • Delegate to NetworkEventProcessor                    │
//! │  • Send result back via channel                         │
//! └──────────────────────┬──────────────────────────────────┘
//!                        │ Delegate
//!                        ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │  NetworkEventProcessor (Trait)                          │
//! │                                                          │
//! │  DefaultNetworkEventProcessor:                          │
//! │  • reconcile settled network/app events                 │
//! │  • execute one recovery action                          │
//! │    └─► Offline / Probe / Restore / Cleanup / Reconnect  │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Components
//!
//! - **NetworkEvent**: Unified mobile network/app/command events
//! - **NetworkEventResult**: Processing result with success/error/duration
//! - **NetworkEventProcessor**: Trait for custom event handling logic
//! - **DefaultNetworkEventProcessor**: Default implementation with signaling + WebRTC recovery
//!
//! # Usage Patterns
//!
//! ## 1. Platform FFI Call (Primary, Implemented)
//! ```ignore
//! // Platform layer calls NetworkEventHandle via FFI
//! let network_handle = system.create_network_event_handle();
//! let result = network_handle.handle_network_path_changed(snapshot).await?;
//! if result.success {
//!     println!("Processed in {}ms", result.duration_ms);
//! }
//! ```
//!
//! ## 2. Actor Proto Message (Optional, TODO)
//! ```ignore
//! // TODO: actors send proto message directly (not yet implemented)
//! actor_ref.call(NetworkPathChangedMessage { snapshot }).await?;
//! ```
//!
//! **Key Differences:**
//! - FFI path: Uses NetworkEventHandle + channel (implemented)
//! - Actor path: Direct proto message to mailbox (TODO, future enhancement)

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use crate::transport::PeerTransport;
use crate::wire::webrtc::{CleanupGuard, SignalingClient, WebRtcCoordinator};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::connection_supervisor::{ConnectionFact, ConnectionSupervisor};

const NETWORK_EVENT_SETTLE_WINDOW: Duration = Duration::from_millis(400);
const NETWORK_EVENT_RESULT_TIMEOUT: Duration = Duration::from_secs(5);
const SIGNALING_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
pub(super) const LONG_BACKGROUND_RECONNECT_THRESHOLD_MS: u64 = 30_000;
static NEXT_NETWORK_EVENT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Keeps outbound sends behind a network-event lifecycle barrier while the
/// reconciler settles and processes a queued batch.
pub struct NetworkEventBarrier {
    _cleanup_guard: CleanupGuard,
}

/// Mobile network path snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkSnapshot {
    pub sequence: u64,
    pub availability: NetworkAvailability,
    pub transport: NetworkTransportFlags,
    pub is_expensive: bool,
    pub is_constrained: bool,
}

impl NetworkSnapshot {
    pub fn is_offline(&self) -> bool {
        matches!(self.availability, NetworkAvailability::Unavailable)
    }

    pub fn should_restore(&self) -> bool {
        matches!(self.availability, NetworkAvailability::Available)
    }
}

/// Whether the platform currently has a usable network path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NetworkAvailability {
    Unknown,
    Available,
    Unavailable,
}

/// Active network transport flags. Multiple flags can be true at the same time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct NetworkTransportFlags {
    pub wifi: bool,
    pub cellular: bool,
    pub ethernet: bool,
    pub vpn: bool,
    pub other: bool,
}

/// App lifecycle state relevant to connection recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppLifecycleState {
    Background,
    Foreground { background_duration_ms: u64 },
}

/// Reason for a cleanup-only operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CleanupReason {
    AppTerminating,
    UserLogout,
    StaleConnectionSuspected,
    ManualReset,
}

/// Reason for a forced cleanup + restore operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReconnectReason {
    NetworkPathChanged,
    LongBackground,
    ProbeFailed,
    ManualReconnect,
    StaleConnectionSuspected,
}

/// Network event type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NetworkEvent {
    /// Full mobile network path changed.
    NetworkPathChanged { snapshot: NetworkSnapshot },

    /// App lifecycle changed.
    AppLifecycleChanged { state: AppLifecycleState },

    /// Proactively clean up all connections
    ///
    /// Used for app lifecycle management scenarios:
    /// - App entering background
    /// - User actively logging out
    /// - App about to exit
    CleanupConnections { reason: CleanupReason },

    /// Proactively clean up and restore connections.
    ForceReconnect { reason: ReconnectReason },
}

/// Final action selected from a settled batch of network events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkRecoveryAction {
    Noop,
    Offline,
    Probe,
    Restore,
    CleanupOnly,
    ForceReconnect,
}

fn network_event_needs_lifecycle_barrier(event: &NetworkEvent) -> bool {
    match event {
        NetworkEvent::NetworkPathChanged { snapshot } => {
            snapshot.is_offline() || snapshot.should_restore()
        }
        NetworkEvent::AppLifecycleChanged { state } => match state {
            AppLifecycleState::Background => false,
            AppLifecycleState::Foreground {
                background_duration_ms,
            } => *background_duration_ms >= LONG_BACKGROUND_RECONNECT_THRESHOLD_MS,
        },
        NetworkEvent::CleanupConnections { .. } | NetworkEvent::ForceReconnect { .. } => true,
    }
}

/// Network event processing result
#[derive(Debug, Clone)]
pub struct NetworkEventResult {
    /// Event type
    pub event: NetworkEvent,

    /// Whether processing succeeded
    pub success: bool,

    /// Error message (if failed)
    pub error: Option<String>,

    /// Processing duration (milliseconds)
    pub duration_ms: u64,
}

impl NetworkEventResult {
    pub fn success(event: NetworkEvent, duration_ms: u64) -> Self {
        Self {
            event,
            success: true,
            error: None,
            duration_ms,
        }
    }

    pub fn failure(event: NetworkEvent, error: String, duration_ms: u64) -> Self {
        Self {
            event,
            success: false,
            error: Some(error),
            duration_ms,
        }
    }
}

/// Network event processor trait
///
/// Defines the processing logic for network events; can be custom-implemented by users
#[async_trait::async_trait]
pub trait NetworkEventProcessor: Send + Sync {
    /// Enter a lifecycle barrier as soon as a queued event is observed by the
    /// reconciler. The default is no barrier for custom processors.
    fn begin_network_event_barrier(&self, _event: &NetworkEvent) -> Option<NetworkEventBarrier> {
        None
    }

    /// Process network available event
    ///
    /// # Returns
    /// - `Ok(())`: processing succeeded
    /// - `Err(String)`: processing failed, contains error message
    async fn process_network_available(&self) -> Result<(), String>;

    /// Process network lost event
    ///
    /// # Returns
    /// - `Ok(())`: processing succeeded
    /// - `Err(String)`: processing failed, contains error message
    async fn process_network_lost(&self) -> Result<(), String>;

    /// Process network type changed event
    ///
    /// # Returns
    /// - `Ok(())`: processing succeeded
    /// - `Err(String)`: processing failed, contains error message
    async fn process_network_type_changed(
        &self,
        is_wifi: bool,
        is_cellular: bool,
    ) -> Result<(), String>;

    /// Proactively clean up all connections
    ///
    /// This method proactively cleans up all network connections. Applicable scenarios:
    /// - App entering background (iOS/Android)
    /// - User actively logging out
    /// - App about to exit
    /// - Need to reset network state
    ///
    /// # FFI Binding Note
    ///
    /// This method is specifically designed for FFI bindings, allowing upper-layer
    /// platform code (Swift/Kotlin) to proactively manage connection lifecycle
    /// through the unified `NetworkEventProcessor` interface.
    ///
    /// # Difference from Event Response
    ///
    /// - `process_network_lost()`: passively responds to network disconnection events
    /// - `cleanup_connections()`: proactively cleans up connections (independent of network events)
    ///
    /// # Returns
    /// - `Ok(())`: cleanup succeeded
    /// - `Err(String)`: cleanup failed, contains error message
    async fn cleanup_connections(&self) -> Result<(), String>;

    /// Probe existing connectivity without forcing cleanup.
    async fn probe_connectivity(&self) -> Result<(), String> {
        Ok(())
    }

    /// Proactively clean up and restore connections.
    async fn force_reconnect(&self) -> Result<(), String> {
        self.cleanup_connections().await?;
        self.process_network_available().await
    }

    /// Process the final action selected from a settled event batch.
    ///
    /// Custom processors can rely on the default mapping. The default runtime
    /// processor overrides this to bypass per-event debounce after reconciliation.
    async fn process_network_recovery_action(
        &self,
        action: NetworkRecoveryAction,
    ) -> Result<(), String> {
        match action {
            NetworkRecoveryAction::Noop => Ok(()),
            NetworkRecoveryAction::Offline => self.process_network_lost().await,
            NetworkRecoveryAction::Probe => self.probe_connectivity().await,
            NetworkRecoveryAction::Restore => self.process_network_available().await,
            NetworkRecoveryAction::CleanupOnly => self.cleanup_connections().await,
            NetworkRecoveryAction::ForceReconnect => self.force_reconnect().await,
        }
    }
}

/// Debounce configuration
#[derive(Debug, Clone)]
pub struct DebounceConfig {
    /// Debounce time window (duplicate events within this window are ignored)
    pub window: Duration,
}

impl Default for DebounceConfig {
    fn default() -> Self {
        Self {
            // Default debounce window
            window: Duration::from_secs(2),
        }
    }
}

/// Debounce state tracking
#[derive(Debug)]
struct DebounceState {
    last_available: tokio::sync::Mutex<Option<Instant>>,
    last_lost: tokio::sync::Mutex<Option<Instant>>,
    last_type_changed: tokio::sync::Mutex<Option<Instant>>,
}

impl DebounceState {
    fn new() -> Self {
        Self {
            last_available: tokio::sync::Mutex::new(None),
            last_lost: tokio::sync::Mutex::new(None),
            last_type_changed: tokio::sync::Mutex::new(None),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DebounceEvent {
    Available,
    Lost,
    TypeChanged,
}

#[derive(Debug)]
struct SignalingRecoveryState {
    connect_lock: tokio::sync::Mutex<()>,
    last_successful_connect: tokio::sync::Mutex<Option<Instant>>,
}

impl SignalingRecoveryState {
    fn new() -> Self {
        Self {
            connect_lock: tokio::sync::Mutex::new(()),
            last_successful_connect: tokio::sync::Mutex::new(None),
        }
    }
}

/// Default network event processor implementation
pub struct DefaultNetworkEventProcessor {
    signaling_client: Arc<dyn SignalingClient>,
    webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,
    peer_transport: Option<Arc<PeerTransport>>,
    debounce_config: DebounceConfig,
    debounce_state: Arc<DebounceState>,
    recovery_state: Arc<SignalingRecoveryState>,
}

impl DefaultNetworkEventProcessor {
    pub fn new(
        signaling_client: Arc<dyn SignalingClient>,
        webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,
    ) -> Self {
        Self::new_with_debounce_and_peer_transport(
            signaling_client,
            webrtc_coordinator,
            DebounceConfig::default(),
            None,
        )
    }

    pub fn new_with_debounce(
        signaling_client: Arc<dyn SignalingClient>,
        webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,
        debounce_config: DebounceConfig,
    ) -> Self {
        Self::new_with_debounce_and_peer_transport(
            signaling_client,
            webrtc_coordinator,
            debounce_config,
            None,
        )
    }

    pub(crate) fn new_with_peer_transport(
        signaling_client: Arc<dyn SignalingClient>,
        webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,
        peer_transport: Option<Arc<PeerTransport>>,
    ) -> Self {
        Self::new_with_debounce_and_peer_transport(
            signaling_client,
            webrtc_coordinator,
            DebounceConfig::default(),
            peer_transport,
        )
    }

    pub(crate) fn new_with_debounce_and_peer_transport(
        signaling_client: Arc<dyn SignalingClient>,
        webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,
        debounce_config: DebounceConfig,
        peer_transport: Option<Arc<PeerTransport>>,
    ) -> Self {
        Self {
            signaling_client,
            webrtc_coordinator,
            peer_transport,
            debounce_config,
            debounce_state: Arc::new(DebounceState::new()),
            recovery_state: Arc::new(SignalingRecoveryState::new()),
        }
    }

    fn lifecycle_barrier(&self) -> Option<NetworkEventBarrier> {
        self.webrtc_coordinator
            .as_ref()
            .map(|coordinator| NetworkEventBarrier {
                _cleanup_guard: coordinator.cleanup_guard(),
            })
    }

    /// Check whether an event should be filtered by debounce
    ///
    /// # Returns
    /// - `true`: the event should be processed
    /// - `false`: the event is within the debounce window and should be ignored
    async fn should_process_event(&self, event: DebounceEvent) -> bool {
        let now = Instant::now();

        match event {
            DebounceEvent::Available => {
                let mut last = self.debounce_state.last_available.lock().await;
                if let Some(last_time) = *last {
                    if now.duration_since(last_time) < self.debounce_config.window {
                        tracing::debug!(
                            "⏸️  Debouncing Network Available event (last event was {:?} ago)",
                            now.duration_since(last_time)
                        );
                        return false;
                    }
                }
                *last = Some(now);
                true
            }
            DebounceEvent::Lost => {
                let mut last = self.debounce_state.last_lost.lock().await;
                if let Some(last_time) = *last {
                    if now.duration_since(last_time) < self.debounce_config.window {
                        tracing::debug!(
                            "⏸️  Debouncing Network Lost event (last event was {:?} ago)",
                            now.duration_since(last_time)
                        );
                        return false;
                    }
                }
                *last = Some(now);
                true
            }
            DebounceEvent::TypeChanged => {
                let mut last = self.debounce_state.last_type_changed.lock().await;
                if let Some(last_time) = *last {
                    if now.duration_since(last_time) < self.debounce_config.window {
                        tracing::debug!(
                            "⏸️  Debouncing Network TypeChanged event (last event was {:?} ago)",
                            now.duration_since(last_time)
                        );
                        return false;
                    }
                }
                *last = Some(now);
                true
            }
        }
    }

    async fn ensure_signaling_healthy_once(&self, reason: &str) -> Result<(), String> {
        let _guard = self.recovery_state.connect_lock.lock().await;

        if !self.signaling_client.is_connected() {
            tracing::info!(
                reason = reason,
                "Network recovery event resetting signaling reconnect backoff before connect"
            );
            self.signaling_client
                .schedule_auto_reconnect_reset_backoff();
            tracing::info!(reason = reason, "🔄 Connecting signaling");
            self.signaling_client.connect_once().await.map_err(|e| {
                let err_msg = format!("WebSocket connect failed: {}", e);
                tracing::error!("❌ {}", err_msg);
                err_msg
            })?;

            *self.recovery_state.last_successful_connect.lock().await = Some(Instant::now());
            tracing::info!(reason = reason, "✅ Signaling connected");
            return Ok(());
        }

        tracing::debug!(
            reason = reason,
            timeout_ms = SIGNALING_PROBE_TIMEOUT.as_millis() as u64,
            "🔎 Probing existing signaling WebSocket"
        );

        match self
            .signaling_client
            .probe_alive(SIGNALING_PROBE_TIMEOUT)
            .await
        {
            Ok(()) => {
                tracing::debug!(
                    reason = reason,
                    "✅ Signaling probe succeeded; keeping existing WebSocket"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    reason = reason,
                    "⚠️ Signaling probe failed; rebuilding WebSocket: {}",
                    e
                );

                if let Err(disconnect_err) = self.signaling_client.disconnect().await {
                    tracing::warn!(
                        reason = reason,
                        "⚠️ Failed to disconnect unhealthy signaling before rebuild: {}",
                        disconnect_err
                    );
                }

                tracing::info!(
                    reason = reason,
                    "Network recovery event resetting signaling reconnect backoff before rebuild"
                );
                self.signaling_client
                    .schedule_auto_reconnect_reset_backoff();
                tracing::info!(reason = reason, "🔄 Rebuilding signaling: connecting");
                self.signaling_client
                    .connect_once()
                    .await
                    .map_err(|connect_err| {
                        let err_msg = format!("WebSocket rebuild failed: {}", connect_err);
                        tracing::error!("❌ {}", err_msg);
                        err_msg
                    })?;

                *self.recovery_state.last_successful_connect.lock().await = Some(Instant::now());
                tracing::info!(reason = reason, "✅ Signaling rebuilt");
                Ok(())
            }
        }
    }

    async fn restore_signaling_and_webrtc(&self, reason: &str) -> Result<(), String> {
        let _cleanup_guard = self.lifecycle_barrier();
        let recovery_targets = if let Some(coordinator) = self.webrtc_coordinator.clone() {
            coordinator.begin_network_recovery(reason).await
        } else {
            Vec::new()
        };

        self.ensure_signaling_healthy_once(reason).await?;

        let coordinator = self.webrtc_coordinator.clone();

        if let Some(coordinator) = coordinator {
            if recovery_targets.is_empty() {
                tracing::info!("♻️ Resuming ICE restart for peers already in network recovery");
            } else {
                tracing::info!("♻️ Triggering ICE restart for recovering connections...");
            }
            coordinator.restart_network_recovery_connections().await;
        }

        Ok(())
    }

    fn schedule_auto_reconnect_after_recovery_failure(&self, reason: &str, err: &str) {
        tracing::warn!(
            reason = reason,
            error = %err,
            "Network recovery failed; ensuring signaling auto-reconnect remains scheduled"
        );
        self.signaling_client.schedule_auto_reconnect();
    }

    async fn restore_signaling_and_webrtc_from_network_event(
        &self,
        reason: &str,
    ) -> Result<(), String> {
        let result = self.restore_signaling_and_webrtc(reason).await;
        if let Err(err) = &result {
            self.schedule_auto_reconnect_after_recovery_failure(reason, err);
        }
        result
    }

    async fn probe_or_restore(&self, reason: &str) -> Result<(), String> {
        match self.probe_connectivity().await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!(
                    reason = reason,
                    "Connectivity probe failed; restoring connections: {}",
                    e
                );
                if let Err(disconnect_err) = self.signaling_client.disconnect().await {
                    tracing::warn!(
                        reason = reason,
                        "Failed to disconnect unhealthy signaling before restore: {}",
                        disconnect_err
                    );
                }
                self.restore_signaling_and_webrtc_from_network_event(reason)
                    .await
            }
        }
    }

    async fn process_offline(&self) -> Result<(), String> {
        let _cleanup_guard = self.lifecycle_barrier();
        tracing::info!("📱 Processing: Network offline");

        if let Some(ref coordinator) = self.webrtc_coordinator {
            coordinator.begin_network_recovery("NetworkLost").await;
            tracing::info!("🧹 Clearing pending ICE restart attempts...");
            coordinator.clear_pending_restarts().await;
        }

        tracing::info!("🔌 Disconnecting WebSocket...");
        let _ = self.signaling_client.disconnect().await;

        Ok(())
    }
}

#[async_trait::async_trait]
impl NetworkEventProcessor for DefaultNetworkEventProcessor {
    fn begin_network_event_barrier(&self, event: &NetworkEvent) -> Option<NetworkEventBarrier> {
        if network_event_needs_lifecycle_barrier(event) {
            self.lifecycle_barrier()
        } else {
            None
        }
    }

    /// Process network available event
    async fn process_network_available(&self) -> Result<(), String> {
        // Debounce check
        let should_process = self.should_process_event(DebounceEvent::Available).await;
        if !should_process && self.signaling_client.is_connected() {
            return Ok(());
        }

        tracing::info!("📱 Processing: Network available");

        self.restore_signaling_and_webrtc_from_network_event("NetworkAvailable")
            .await
    }

    /// Process network lost event
    async fn process_network_lost(&self) -> Result<(), String> {
        // Debounce check
        if !self.should_process_event(DebounceEvent::Lost).await {
            return Ok(());
        }

        self.process_offline().await
    }

    /// Process network type changed event
    async fn process_network_type_changed(
        &self,
        is_wifi: bool,
        is_cellular: bool,
    ) -> Result<(), String> {
        // Debounce check
        let should_process = self.should_process_event(DebounceEvent::TypeChanged).await;
        if !should_process && self.signaling_client.is_connected() {
            return Ok(());
        }

        tracing::info!(
            "📱 Processing: Network type changed (WiFi={}, Cellular={})",
            is_wifi,
            is_cellular
        );

        self.restore_signaling_and_webrtc_from_network_event("NetworkTypeChanged")
            .await
    }

    /// Proactively clean up all connections
    ///
    /// Differs from `process_network_lost()`:
    /// - No debounce check (proactive calls always execute)
    /// - Intended for app lifecycle management, not network event response
    async fn cleanup_connections(&self) -> Result<(), String> {
        let _cleanup_guard = self.lifecycle_barrier();
        let mut cleanup_error = None;
        let mut initial_coordinator_close_failed = false;

        tracing::info!("🧹 Manually cleaning up all connections...");

        // Step 1: Stop old signaling ingress before reopening any peer
        // lifecycle. Disconnect resets the inbound queue, so delayed Offer,
        // RoleAssignment, and ICE messages cannot repopulate state after drain.
        tracing::info!("🔌 Disconnecting WebSocket before peer cleanup...");
        match self.signaling_client.disconnect().await {
            Ok(_) => {
                tracing::info!("✅ WebSocket disconnected successfully");
            }
            Err(e) => {
                let err_msg = format!("Failed to disconnect WebSocket before cleanup: {e}");
                tracing::warn!("⚠️  {}", err_msg);
                cleanup_error = Some(err_msg);
            }
        }

        // Step 2: Clear pending ICE restart attempts.
        if let Some(ref coordinator) = self.webrtc_coordinator {
            tracing::info!("♻️  Clearing pending ICE restart attempts...");
            coordinator.clear_pending_restarts().await;
        }

        // Step 3: Remove coordinator-owned peer sessions first and force-close
        // them without draining DataChannels. On mobile, WebRTC may continue to
        // report Connected after the OS route has disappeared, so a graceful
        // drain here can only wait for data that can no longer be delivered.
        if let Some(ref coordinator) = self.webrtc_coordinator {
            tracing::info!("🔻 Force-closing all WebRTC peer connections...");
            if let Err(e) = coordinator.close_all_peers_immediately().await {
                let err_msg = format!("Failed to close all peers: {}", e);
                tracing::warn!("⚠️  {}", err_msg);
                initial_coordinator_close_failed = true;
            } else {
                tracing::info!("✅ All WebRTC peer connections closed");
            }
        }

        // Step 4: Cancel PeerTransport singleflight and close any remaining
        // established transport handles after the coordinator sessions are gone.
        if let Some(ref peer_transport) = self.peer_transport {
            tracing::info!("🔻 Closing all PeerTransport connections...");
            if let Err(e) = peer_transport.close_all().await {
                let err_msg = format!("Failed to close peer transports: {}", e);
                tracing::warn!("⚠️  {}", err_msg);
                cleanup_error.get_or_insert(err_msg);
            } else {
                tracing::info!("✅ All PeerTransport connections closed");
            }
        }

        // Step 5: A cancelled PeerTransport creator may have crossed the first
        // drain before observing its token. The signaling socket is already
        // down, so this final authoritative sweep closes anything it handed to
        // the coordinator without opening another ingress window.
        if let Some(ref coordinator) = self.webrtc_coordinator {
            tracing::info!("🔻 Finalizing WebRTC coordinator cleanup...");
            if let Err(e) = coordinator.close_all_peers_immediately().await {
                let err_msg = format!("Failed to finalize peer cleanup: {e}");
                tracing::warn!("⚠️  {}", err_msg);
                cleanup_error.get_or_insert(err_msg);
            } else if initial_coordinator_close_failed {
                tracing::info!("✅ Final WebRTC cleanup recovered the initial close failure");
            }
        }

        if let Some(err) = cleanup_error {
            tracing::warn!(
                error = %err,
                "Connection cleanup released remaining resources but did not fully quiesce"
            );
            Err(err)
        } else {
            tracing::info!("✅ Connection cleanup completed");
            Ok(())
        }
    }

    async fn probe_connectivity(&self) -> Result<(), String> {
        self.signaling_client
            .probe_alive(SIGNALING_PROBE_TIMEOUT)
            .await
            .map_err(|e| format!("Signaling probe failed: {}", e))
    }

    async fn force_reconnect(&self) -> Result<(), String> {
        self.cleanup_connections().await?;
        self.restore_signaling_and_webrtc_from_network_event("ForceReconnect")
            .await
    }

    async fn process_network_recovery_action(
        &self,
        action: NetworkRecoveryAction,
    ) -> Result<(), String> {
        match action {
            NetworkRecoveryAction::Noop => Ok(()),
            NetworkRecoveryAction::Offline => self.process_offline().await,
            NetworkRecoveryAction::Probe => self.probe_or_restore("Probe").await,
            NetworkRecoveryAction::Restore => {
                self.restore_signaling_and_webrtc_from_network_event("NetworkEventBatch")
                    .await
            }
            NetworkRecoveryAction::CleanupOnly => self.cleanup_connections().await,
            NetworkRecoveryAction::ForceReconnect => self.force_reconnect().await,
        }
    }
}

pub fn select_network_recovery_action(events: &[NetworkEvent]) -> NetworkRecoveryAction {
    ConnectionSupervisor::select_action(events)
}

pub async fn process_network_event_batch(
    events: Vec<NetworkEvent>,
    processor: Arc<dyn NetworkEventProcessor>,
) -> Vec<NetworkEventResult> {
    if events.is_empty() {
        return Vec::new();
    }

    let action = select_network_recovery_action(&events);
    let start = Instant::now();

    tracing::info!(
        event_count = events.len(),
        action = ?action,
        "network_event.action.start"
    );

    let result = processor.process_network_recovery_action(action).await;

    let duration_ms = start.elapsed().as_millis() as u64;
    match &result {
        Ok(()) => tracing::info!(
            event_count = events.len(),
            action = ?action,
            duration_ms,
            "network_event.action.completed"
        ),
        Err(e) => tracing::warn!(
            event_count = events.len(),
            action = ?action,
            duration_ms,
            error = %e,
            "network_event.action.completed"
        ),
    }

    events
        .into_iter()
        .map(|event| match &result {
            Ok(()) => NetworkEventResult::success(event, duration_ms),
            Err(e) => NetworkEventResult::failure(event, e.clone(), duration_ms),
        })
        .collect()
}

pub struct NetworkEventRequest {
    pub event: NetworkEvent,
    pub result_tx: oneshot::Sender<NetworkEventResult>,
}

pub async fn run_network_event_reconciler(
    mut event_rx: mpsc::Receiver<NetworkEventRequest>,
    processor: Arc<dyn NetworkEventProcessor>,
    shutdown_token: CancellationToken,
) {
    tracing::info!("🔄 Network event reconciler started");

    loop {
        tokio::select! {
            Some(first_request) = event_rx.recv() => {
                tracing::debug!(
                    event = ?first_request.event,
                    "network_event.reconciler.received"
                );
                let mut event_barrier = processor.begin_network_event_barrier(&first_request.event);
                let mut requests = vec![first_request];
                let settle = tokio::time::sleep(NETWORK_EVENT_SETTLE_WINDOW);
                tokio::pin!(settle);

                loop {
                    tokio::select! {
                        Some(next_request) = event_rx.recv() => {
                            tracing::debug!(
                                event = ?next_request.event,
                                "network_event.reconciler.coalesced"
                            );
                            if event_barrier.is_none() {
                                event_barrier = processor.begin_network_event_barrier(&next_request.event);
                            }
                            requests.push(next_request);
                        }
                        _ = &mut settle => {
                            break;
                        }
                        _ = shutdown_token.cancelled() => {
                            tracing::info!("🛑 Network event reconciler shutting down");
                            return;
                        }
                        else => {
                            break;
                        }
                    }
                }

                while let Ok(next_request) = event_rx.try_recv() {
                    tracing::debug!(
                        event = ?next_request.event,
                        "network_event.reconciler.coalesced"
                    );
                    if event_barrier.is_none() {
                        event_barrier = processor.begin_network_event_barrier(&next_request.event);
                    }
                    requests.push(next_request);
                }

                let events = requests
                    .iter()
                    .map(|request| request.event.clone())
                    .collect::<Vec<_>>();
                let action = select_network_recovery_action(&events);
                let facts = events
                    .iter()
                    .map(ConnectionFact::from_network_event)
                    .collect::<Vec<_>>();
                tracing::info!(
                    event_count = events.len(),
                    action = ?action,
                    events = ?events,
                    facts = ?facts,
                    settle_window_ms = NETWORK_EVENT_SETTLE_WINDOW.as_millis() as u64,
                    "network_event.reconciler.batch_reconciled"
                );

                let results = process_network_event_batch(events, processor.clone()).await;
                drop(event_barrier);

                for (request, result) in requests.into_iter().zip(results) {
                    if request.result_tx.send(result).is_err() {
                        tracing::debug!("Network event caller dropped before receiving result");
                    }
                }
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("🛑 Network event reconciler shutting down");
                break;
            }
            else => break,
        }
    }
}

/// Network Event Handle
///
/// Lightweight handle for sending network events and receiving processing results.
/// Created before `ActrNode::start()` to bridge platform network events.
pub struct NetworkEventHandle {
    /// Event sender (to ActrNode)
    event_tx: mpsc::Sender<NetworkEventRequest>,
    result_timeout: Duration,
}

impl NetworkEventHandle {
    /// Create a new NetworkEventHandle
    pub fn new(event_tx: mpsc::Sender<NetworkEventRequest>) -> Self {
        Self::new_with_result_timeout(event_tx, NETWORK_EVENT_RESULT_TIMEOUT)
    }

    /// Create a new NetworkEventHandle with a custom result timeout.
    ///
    /// Production bindings use [`NetworkEventHandle::new`]. Tests can use this
    /// constructor to verify bounded waiting without sleeping for the full
    /// binding timeout.
    pub fn new_with_result_timeout(
        event_tx: mpsc::Sender<NetworkEventRequest>,
        result_timeout: Duration,
    ) -> Self {
        Self {
            event_tx,
            result_timeout,
        }
    }

    /// Handle full network path changes.
    pub async fn handle_network_path_changed(
        &self,
        snapshot: NetworkSnapshot,
    ) -> Result<NetworkEventResult, String> {
        self.send_event_and_await_result(NetworkEvent::NetworkPathChanged { snapshot })
            .await
    }

    /// Handle app lifecycle changes.
    pub async fn handle_app_lifecycle_changed(
        &self,
        state: AppLifecycleState,
    ) -> Result<NetworkEventResult, String> {
        self.send_event_and_await_result(NetworkEvent::AppLifecycleChanged { state })
            .await
    }

    /// Proactively clean up all connections with a reason. This never reconnects.
    pub async fn cleanup_connections(
        &self,
        reason: CleanupReason,
    ) -> Result<NetworkEventResult, String> {
        self.send_event_and_await_result(NetworkEvent::CleanupConnections { reason })
            .await
    }

    /// Force cleanup and reconnect.
    pub async fn force_reconnect(
        &self,
        reason: ReconnectReason,
    ) -> Result<NetworkEventResult, String> {
        self.send_event_and_await_result(NetworkEvent::ForceReconnect { reason })
            .await
    }

    /// Send event and await result (internal helper)
    async fn send_event_and_await_result(
        &self,
        event: NetworkEvent,
    ) -> Result<NetworkEventResult, String> {
        let event_request_id = NEXT_NETWORK_EVENT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        let (result_tx, result_rx) = oneshot::channel();
        let request = NetworkEventRequest {
            event: event.clone(),
            result_tx,
        };

        tracing::info!(
            event_request_id,
            event = ?event,
            result_timeout_ms = self.result_timeout.as_millis() as u64,
            "network_event.handle.enqueue"
        );

        if let Err(e) = self.event_tx.send(request).await {
            let err = format!("Failed to send network event: {}", e);
            tracing::warn!(
                event_request_id,
                event = ?event,
                error = %err,
                "network_event.handle.enqueue_failed"
            );
            return Err(err);
        }

        let result = match tokio::time::timeout(self.result_timeout, result_rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err("Failed to receive network event result".to_string()),
            Err(_) => Err(format!(
                "Timed out waiting for network event result after {}ms",
                self.result_timeout.as_millis()
            )),
        };

        let wait_ms = start.elapsed().as_millis() as u64;
        match &result {
            Ok(result) if result.success => tracing::info!(
                event_request_id,
                event = ?event,
                result_event = ?result.event,
                duration_ms = result.duration_ms,
                wait_ms,
                "network_event.handle.result_received"
            ),
            Ok(result) => tracing::warn!(
                event_request_id,
                event = ?event,
                result_event = ?result.event,
                duration_ms = result.duration_ms,
                wait_ms,
                error = ?result.error,
                "network_event.handle.result_received"
            ),
            Err(e) => tracing::warn!(
                event_request_id,
                event = ?event,
                wait_ms,
                error = %e,
                "network_event.handle.result_failed"
            ),
        }

        result
    }
}

impl Clone for NetworkEventHandle {
    fn clone(&self) -> Self {
        Self {
            event_tx: self.event_tx.clone(),
            result_timeout: self.result_timeout,
        }
    }
}

#[cfg(test)]
#[path = "network_event_tests.rs"]
mod tests;
