//! signaling clientImplementation
//!
//! Based on protobuf Definition'ssignalingprotocol, using SignalingEnvelope conclude construct

#[cfg(feature = "opentelemetry")]
use super::trace;
use crate::lifecycle::CredentialState;
use crate::transport::{NetworkError, NetworkResult};
#[cfg(feature = "opentelemetry")]
use crate::wire::webrtc::trace::extract_trace_context;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{
    AIdCredential, ActrId, ActrToSignaling, CredentialUpdateRequest, GetSigningKeyRequest,
    PeerToSignaling, Ping, Pong, RegisterRequest, RegisterResponse, RouteCandidatesRequest,
    RouteCandidatesResponse, ServiceAvailabilityState, SignalingEnvelope, UnregisterRequest,
    UnregisterResponse, actr_to_signaling, peer_to_signaling, signaling_envelope,
    signaling_to_actr,
};
use async_trait::async_trait;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
#[cfg(feature = "opentelemetry")]
use tracing_opentelemetry::OpenTelemetrySpanExt;
use url::Url;

/// WebSocket sink type alias for the split write half of a signaling connection
type WsSink = Arc<
    tokio::sync::Mutex<
        Option<
            futures_util::stream::SplitSink<
                WebSocketStream<MaybeTlsStream<TcpStream>>,
                tokio_tungstenite::tungstenite::Message,
            >,
        >,
    >,
>;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Constants
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Default timeout in seconds for waiting for signaling request/response RPCs.
const RESPONSE_TIMEOUT_SECS: u64 = 5;
// WebSocket-level keepalive to detect silent half-open connections
const PING_INTERVAL_SECS: u64 = 5;
const PONG_TIMEOUT_SECS: u64 = 10;
const SIGNALING_SEND_TIMEOUT_SECS: u64 = 5;
const CONCURRENT_CONNECT_WAIT_TIMEOUT_SECS: u64 = 5;
const DISCONNECT_LOCK_TIMEOUT_SECS: u64 = 5;
const DISCONNECT_CLOSE_TIMEOUT_SECS: u64 = 1;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// configurationType
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// signalingconfiguration
#[derive(Debug, Clone)]
pub struct SignalingConfig {
    /// signaling server URL
    pub server_url: Url,

    /// Connecttimeout temporal duration （seconds）
    pub connection_timeout: u64,

    /// center skipinterval（seconds）
    pub heartbeat_interval: u64,

    /// reconnection configuration
    pub reconnect_config: ReconnectConfig,

    /// acknowledge verify configuration
    pub auth_config: Option<AuthConfig>,

    /// WebRTC role preference: "answer" if this node has advanced config
    pub webrtc_role: Option<String>,
}

/// reconnection configuration
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// whether start usage automatic reconnection
    pub enabled: bool,

    /// maximum reconnection attempts
    pub max_attempts: u32,

    /// initial reconnection delay（seconds）
    pub initial_delay: u64,

    /// maximum reconnection delay（seconds）
    pub max_delay: u64,

    /// Backoff multiplier factor
    pub backoff_multiplier: f64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 10,
            initial_delay: 1,
            max_delay: 60,
            backoff_multiplier: 2.0,
        }
    }
}

/// acknowledge verify configuration
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// acknowledge verify Type
    pub auth_type: AuthType,

    /// acknowledge verify credential data
    pub credentials: HashMap<String, String>,
}

/// acknowledge verify Type
#[derive(Debug, Clone)]
pub enum AuthType {
    /// no acknowledge verify
    None,
    /// Bearer Token
    BearerToken,
    /// API Key
    ApiKey,
    /// JWT
    Jwt,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Client interface and implementation
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// signaling client connect port
///
/// # interior mutability
/// allMethodusing `&self` and non `&mut self`, with for conveniencein Arc in shared.
/// Implementation class needs interior mutability （ like Mutex）to manage WebSocket connection status.
#[async_trait]
pub trait SignalingClient: Send + Sync {
    /// Connecttosignaling server
    async fn connect(&self) -> NetworkResult<()>;

    /// Perform a single explicit connection attempt.
    ///
    /// Network recovery events use this path so a failed restore attempt can
    /// return quickly instead of sleeping inside the normal reconnect backoff.
    async fn connect_once(&self) -> NetworkResult<()> {
        self.connect().await
    }

    /// Re-enable and wake the automatic reconnect manager after an explicit
    /// lifecycle recovery attempt failed.
    fn schedule_auto_reconnect(&self) {}

    /// Re-enable automatic reconnect and restart its backoff sequence.
    ///
    /// Network restoration signals should use this path because they represent
    /// a fresh external condition, not just another passive connection failure.
    fn schedule_auto_reconnect_reset_backoff(&self) {
        self.schedule_auto_reconnect();
    }

    /// DisconnectConnect
    async fn disconnect(&self) -> NetworkResult<()>;

    /// Probe whether the existing signaling WebSocket is truly alive.
    ///
    /// The default implementation only checks local state. WebSocket-backed
    /// clients override this with an active Ping/Pong probe to catch half-open
    /// sockets before network recovery decides whether to reconnect.
    async fn probe_alive(&self, _timeout: Duration) -> NetworkResult<()> {
        if self.is_connected() {
            Ok(())
        } else {
            Err(NetworkError::ConnectionError(
                "Signaling client is not connected".to_string(),
            ))
        }
    }

    /// Deprecated: Registration now happens via AIS HTTP; this WS path is no longer used.
    /// Kept for backward compatibility; will be removed in a future release.
    async fn send_register_request(
        &self,
        request: RegisterRequest,
    ) -> NetworkResult<RegisterResponse>;

    /// Send UnregisterRequest to signaling server (Actr → Signaling flow)
    ///
    /// This is used when an Actor is shutting down gracefully and wants to
    /// proactively notify the signaling server that it is no longer available.
    async fn send_unregister_request(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
        reason: Option<String>,
    ) -> NetworkResult<UnregisterResponse>;

    /// Send center skip（Registerafter stream process, using ActrToSignaling）
    /// Returns Pong response if received, error if timeout or no response
    async fn send_heartbeat(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
        availability: ServiceAvailabilityState,
        power_reserve: f32,
        mailbox_backlog: f32,
    ) -> NetworkResult<Pong>;

    /// Send RouteCandidatesRequest (requires authenticated Actor session)
    async fn send_route_candidates_request(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
        request: RouteCandidatesRequest,
    ) -> NetworkResult<RouteCandidatesResponse>;

    /// Query AIS Ed25519 signing public key via signaling
    ///
    /// Returns `(key_id, pubkey_bytes)` where pubkey_bytes is the 32-byte raw public key.
    /// Typically called by AisKeyCache on cache miss; should not be used directly in hot paths.
    async fn get_signing_key(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
        key_id: u32,
    ) -> NetworkResult<(u32, Vec<u8>)>;

    /// Send CredentialUpdateRequest to refresh the Actor's credential
    ///
    /// This is used to refresh the credential before it expires. The server responds
    /// with a RegisterResponse containing the new credential and expiration time.
    async fn send_credential_update_request(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
    ) -> NetworkResult<RegisterResponse>;

    /// Sendsignalingsignal seal （ pass usage Method）
    async fn send_envelope(&self, envelope: SignalingEnvelope) -> NetworkResult<()>;

    /// Receivesignalingsignal seal
    async fn receive_envelope(&self) -> NetworkResult<Option<SignalingEnvelope>>;

    /// Check connection status
    fn is_connected(&self) -> bool;

    /// GetConnect statistics info
    fn get_stats(&self) -> SignalingStats;
    /// Subscribe to signaling events (state transitions).
    fn subscribe_events(&self) -> broadcast::Receiver<SignalingEvent>;

    /// Set actor ID and credential state for reconnect URL parameters.
    async fn set_actor_id(&self, actor_id: ActrId);
    async fn set_credential_state(&self, credential_state: CredentialState);

    /// Clear stored actor ID and credential state.
    ///
    /// After calling this, `connect()` will produce a clean WebSocket URL
    /// without identity query parameters, so the signaling server treats
    /// the connection as brand-new rather than a reconnect of the old actor.
    /// This is required before re-registration when the credential has expired.
    async fn clear_identity(&self);

    /// Set a lifecycle hook callback that will be invoked (and awaited)
    /// whenever signaling state changes (connect/disconnect).
    /// Default implementation is a no-op for clients that don't support hooks.
    fn set_hook_callback(&self, _cb: HookCallback) {}
}

/// High-level signaling connection state (kept for quick boolean checks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connected,
}

/// Signaling state transition events.
///
/// Unlike `ConnectionState` (which is a snapshot), these represent discrete
/// transitions and are delivered via `broadcast` so every subscriber sees
/// every event, even if the same state occurs twice in a row.
#[derive(Debug, Clone)]
pub enum SignalingEvent {
    /// About to start a connection attempt (includes retry count).
    ConnectStart { attempt: u32 },
    /// Connection successfully established.
    Connected,
    /// Connection lost.
    Disconnected { reason: DisconnectReason },
}

/// Reason why the signaling connection was lost.
#[derive(Debug, Clone)]
pub enum DisconnectReason {
    /// WebSocket stream ended (receiver task exited normally).
    StreamEnded,
    /// No Pong received within the timeout window.
    PongTimeout,
    /// Failed to send a WebSocket Ping frame.
    PingSendFailed,
    /// Credential expired (heartbeat 401).
    CredentialExpired,
    /// Explicit disconnect() call or external trigger.
    Manual,
    /// Connection attempt failed with an error.
    ConnectionFailed(String),
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Hook callback for synchronous lifecycle notification
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Events that trigger workload lifecycle hooks.
///
/// Used by `HookCallback` to invoke workload hooks synchronously (awaited)
/// at the point where the state change occurs.
#[derive(Clone, Debug)]
pub enum HookEvent {
    // ── Signaling ──
    SignalingConnectStart {
        attempt: u32,
    },
    SignalingConnected,
    SignalingDisconnected,
    // ── WebRTC ──
    WebRtcConnectStart {
        peer_id: ActrId,
    },
    WebRtcConnected {
        peer_id: ActrId,
        relayed: bool,
    },
    WebRtcDisconnected {
        peer_id: ActrId,
    },
    DataStreamDeliveryUncertain {
        stream_id: String,
        session_id: u64,
        reason: String,
    },
    // ── WebSocket ──
    WebSocketConnectStart {
        peer_id: ActrId,
    },
    WebSocketConnected {
        peer_id: ActrId,
    },
    WebSocketDisconnected {
        peer_id: ActrId,
    },
    // ── Credential ──
    CredentialRenewed {
        new_expiry: std::time::SystemTime,
    },
    CredentialExpiring {
        new_expiry: std::time::SystemTime,
    },
    // ── Mailbox ──
    MailboxBackpressure {
        queue_len: usize,
        threshold: usize,
    },
}

/// Callback closure that is awaited when a hook event occurs.
///
/// Set once via `set_hook_callback()`. All state-change paths invoke this
/// closure and `.await` its result before proceeding.
pub type HookCallback =
    Arc<dyn Fn(HookEvent) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Debug, Clone, Copy)]
enum ConnectIntent {
    Explicit,
    AutoReconnect { generation: u64 },
}

/// WebSocket signaling clientImplementation
pub struct WebSocketSignalingClient {
    config: SignalingConfig,
    actor_id: tokio::sync::Mutex<Option<ActrId>>,
    credential_state: tokio::sync::Mutex<Option<CredentialState>>,
    /// WebSocket write end （using Mutex Implementation interior mutability ）
    ws_sink: WsSink,
    /// WebSocket read end （using Mutex Implementation interior mutability ）
    ws_stream: tokio::sync::Mutex<
        Option<futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    >,
    /// connection status
    connected: Arc<AtomicBool>,
    /// Connection in progress flag (prevents concurrent connect attempts)
    connecting: Arc<AtomicBool>,
    /// statistics info
    stats: Arc<AtomicSignalingStats>,
    /// Envelope count number device
    envelope_counter: tokio::sync::Mutex<u64>,
    /// Pending reply waiters (reply_for -> oneshot)
    pending_replies: Arc<tokio::sync::Mutex<HashMap<String, oneshot::Sender<SignalingEnvelope>>>>,
    /// Pending WebSocket Pong waiters (ping payload -> oneshot)
    pending_pongs: Arc<tokio::sync::Mutex<HashMap<Vec<u8>, oneshot::Sender<()>>>>,
    /// Monotonic probe payload counter.
    probe_counter: AtomicU64,
    /// Inbound envelope channel for unmatched messages (ActrRelay / push)
    inbound_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<SignalingEnvelope>>>,
    inbound_tx: tokio::sync::Mutex<mpsc::UnboundedSender<SignalingEnvelope>>,
    /// Background receive task handle to allow graceful shutdown
    receiver_task: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Background ping task to detect half-open connections
    ping_task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Connection state broadcast channel (event-driven)
    event_tx: broadcast::Sender<SignalingEvent>,
    /// Last time we saw inbound traffic (pong/any message), unix epoch seconds
    last_pong: Arc<AtomicU64>,
    /// Flag to track if reconnect manager has been started
    reconnector_started: Arc<AtomicBool>,
    /// Notify channel to wake up the reconnect manager
    reconnect_notify: Arc<tokio::sync::Notify>,
    /// Explicit disconnects from lifecycle/cleanup suppress stale auto-reconnect cycles.
    auto_reconnect_suppressed: AtomicBool,
    /// Incremented by explicit disconnects to invalidate in-flight auto-reconnect attempts.
    reconnect_generation: AtomicU64,
    /// Incremented by external recovery events that should restart reconnect backoff.
    reconnect_backoff_reset_generation: AtomicU64,
    /// Hook callback for synchronous lifecycle notification (set once, lock-free read)
    hook_callback: OnceLock<HookCallback>,
}

impl WebSocketSignalingClient {
    /// Create new WebSocket signaling client
    pub fn new(config: SignalingConfig) -> Self {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = broadcast::channel(64);
        Self {
            config,
            actor_id: tokio::sync::Mutex::new(None),
            credential_state: tokio::sync::Mutex::new(None),
            ws_sink: Arc::new(tokio::sync::Mutex::new(None)),
            ws_stream: tokio::sync::Mutex::new(None),
            connected: Arc::new(AtomicBool::new(false)),
            connecting: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(AtomicSignalingStats::default()),
            envelope_counter: tokio::sync::Mutex::new(0),
            pending_replies: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            pending_pongs: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            probe_counter: AtomicU64::new(0),
            inbound_rx: Arc::new(tokio::sync::Mutex::new(inbound_rx)),
            inbound_tx: tokio::sync::Mutex::new(inbound_tx),
            receiver_task: Arc::new(tokio::sync::Mutex::new(None)),
            ping_task: tokio::sync::Mutex::new(None),
            event_tx,
            last_pong: Arc::new(AtomicU64::new(0)),
            reconnector_started: Arc::new(AtomicBool::new(false)),
            reconnect_notify: Arc::new(tokio::sync::Notify::new()),
            auto_reconnect_suppressed: AtomicBool::new(false),
            reconnect_generation: AtomicU64::new(0),
            reconnect_backoff_reset_generation: AtomicU64::new(0),
            hook_callback: OnceLock::new(),
        }
    }

    /// Start the reconnect manager if enabled in config and not already started.
    ///
    /// The manager waits on a `Notify` and runs an exponential-backoff retry loop
    /// each time it is woken up.
    /// Invoke the hook callback and await its completion.
    /// No-op if no callback has been set yet.
    async fn invoke_hook(&self, event: HookEvent) {
        if let Some(cb) = self.hook_callback.get() {
            cb(event).await;
        }
    }

    async fn publish_disconnected_transition(
        was_connected: bool,
        stats: &Arc<AtomicSignalingStats>,
        event_tx: &broadcast::Sender<SignalingEvent>,
        hook_callback: Option<HookCallback>,
        reason: DisconnectReason,
        reconnect_notify: Option<&Arc<tokio::sync::Notify>>,
    ) -> bool {
        if !was_connected {
            return false;
        }

        stats.disconnections.fetch_add(1, Ordering::Relaxed);

        if let Some(cb) = hook_callback {
            cb(HookEvent::SignalingDisconnected).await;
        }

        let _ = event_tx.send(SignalingEvent::Disconnected { reason });

        if let Some(notify) = reconnect_notify {
            notify.notify_one();
        }

        true
    }

    pub fn start_reconnect_manager(self: &Arc<Self>) {
        if !self.config.reconnect_config.enabled {
            return;
        }
        if self
            .reconnector_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return; // already started
        }

        tracing::info!("🔄 Starting reconnect manager for signaling client");

        let client = Arc::downgrade(self);
        let notify = self.reconnect_notify.clone();

        tokio::spawn(async move {
            loop {
                let reconnect_requested = tokio::select! {
                    _ = notify.notified() => true,
                    _ = tokio::time::sleep(Duration::from_secs(30)) => false,
                };

                if !reconnect_requested && client.upgrade().is_none() {
                    break;
                }
                if !reconnect_requested {
                    continue;
                }

                let Some(client) = client.upgrade() else {
                    break;
                };

                if !client.config.reconnect_config.enabled {
                    break;
                }

                if Arc::strong_count(&client) <= 1 {
                    break;
                }

                // Run reconnect cycle with exponential backoff
                client.run_reconnect_cycle().await;
            }
        });
    }

    /// Execute one full reconnect cycle with exponential backoff + jitter.
    async fn run_reconnect_cycle(self: &Arc<Self>) {
        use actr_framework::ExponentialBackoff;

        let cfg = &self.config.reconnect_config;
        let generation = self.reconnect_generation.load(Ordering::Acquire);

        if Arc::strong_count(self) <= 1 {
            tracing::debug!("Stopping signaling auto-reconnect cycle after owner drop");
            return;
        }

        if self.auto_reconnect_cancelled(generation) {
            tracing::debug!("Skipping signaling auto-reconnect cycle after explicit disconnect");
            return;
        }

        if self.connected.load(Ordering::Acquire) {
            tracing::debug!("🔎 Probing connected signaling before reconnect cycle");
            match self
                .probe_alive(Duration::from_secs(PONG_TIMEOUT_SECS))
                .await
            {
                Ok(()) => {
                    tracing::debug!("Signaling probe succeeded, skipping reconnect cycle");
                    return;
                }
                Err(e) => {
                    tracing::warn!("Signaling probe failed before reconnect: {e}");
                    if let Err(disconnect_err) = self.disconnect_internal(false).await {
                        tracing::warn!(
                            "⚠️ Disconnect cleanup failed after failed probe (non-fatal): {disconnect_err}"
                        );
                    }
                }
            }
        }

        'cycle: loop {
            let backoff_reset_generation = self
                .reconnect_backoff_reset_generation
                .load(Ordering::Acquire);
            let backoff = ExponentialBackoff::builder()
                .initial_delay(std::time::Duration::from_secs(cfg.initial_delay.max(1)))
                .max_delay(std::time::Duration::from_secs(cfg.max_delay.max(1)))
                .max_retries(cfg.max_attempts)
                .with_jitter()
                .build();

            let mut attempt: u32 = 0;

            for delay in backoff {
                if Arc::strong_count(self) <= 1 {
                    tracing::debug!("Stopping signaling auto-reconnect cycle after owner drop");
                    return;
                }

                if self.auto_reconnect_cancelled(generation) {
                    tracing::debug!(
                        "Stopping signaling auto-reconnect cycle after explicit disconnect"
                    );
                    return;
                }

                if self.connected.load(Ordering::Acquire) {
                    tracing::debug!("Already connected, aborting reconnect cycle");
                    return;
                }

                attempt += 1;
                let _ = self.event_tx.send(SignalingEvent::ConnectStart { attempt });

                match self.connect_once_for_auto_reconnect(generation).await {
                    Ok(()) => {
                        tracing::info!("✅ Signaling reconnect succeeded on attempt {attempt}");
                        return;
                    }
                    Err(e) => {
                        if self.auto_reconnect_cancelled(generation) {
                            tracing::debug!(
                                "Stopping signaling auto-reconnect cycle after explicit disconnect"
                            );
                            return;
                        }

                        tracing::warn!(
                            "❌ Reconnect attempt {attempt} failed: {e}, retrying in {delay:?}"
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = self.reconnect_notify.notified() => {
                                tracing::debug!("Explicit reconnect request interrupted reconnect backoff");
                            }
                        }
                        if self
                            .reconnect_backoff_reset_generation
                            .load(Ordering::Acquire)
                            != backoff_reset_generation
                        {
                            tracing::debug!(
                                "Restarting signaling reconnect backoff after external recovery event"
                            );
                            continue 'cycle;
                        }
                        if Arc::strong_count(self) <= 1 {
                            tracing::debug!(
                                "Stopping signaling auto-reconnect cycle after owner drop"
                            );
                            return;
                        }
                        if self.auto_reconnect_cancelled(generation) {
                            tracing::debug!(
                                "Stopping signaling auto-reconnect cycle after explicit disconnect"
                            );
                            return;
                        }
                    }
                }
            }

            // All retries exhausted — enter cooldown, then allow future wakeups
            tracing::error!("Reconnect failed after {attempt} attempts, entering cooldown");
            let cooldown = std::time::Duration::from_secs(cfg.max_delay.max(1) * 2);
            tokio::select! {
                _ = tokio::time::sleep(cooldown) => {}
                _ = self.reconnect_notify.notified() => {
                    tracing::debug!("Explicit reconnect request interrupted reconnect cooldown");
                }
            }
            if self
                .reconnect_backoff_reset_generation
                .load(Ordering::Acquire)
                != backoff_reset_generation
            {
                tracing::debug!(
                    "Restarting signaling reconnect backoff after external recovery event"
                );
                continue 'cycle;
            }
            if self.auto_reconnect_cancelled(generation) {
                tracing::debug!(
                    "Signaling auto-reconnect cooldown ended after explicit disconnect suppression"
                );
            }
            // After cooldown, the loop returns to notify.notified() and can be woken again
            return;
        }
    }

    /// Test-only convenience constructor: create, connect, and return a client.
    ///
    /// The returned client has no `actor_id` / `credential_state` bound, so the
    /// signaling URL carries no identity query parameters — mock-actrix will
    /// not bind the WebSocket to any registry entry. Use this only for tests
    /// that explicitly exercise the unbound path; integration tests that need
    /// peer-to-peer relay should use [`Self::connect_to_with_identity`].
    #[cfg(feature = "test-utils")]
    pub async fn connect_to(url: &str) -> NetworkResult<Arc<Self>> {
        let config = SignalingConfig {
            server_url: url.parse()?,
            connection_timeout: 5,
            heartbeat_interval: 30,
            reconnect_config: ReconnectConfig::default(),
            auth_config: None,
            webrtc_role: None,
        };

        let client = Arc::new(Self::new(config));
        client.start_reconnect_manager();
        client.connect().await?;
        Ok(client)
    }

    /// Test-only constructor that pins identity *before* the WebSocket
    /// handshake so mock-actrix can bind the connection to the actor on
    /// register (`?actor_id=…` query parameter). Required by integration
    /// tests that rely on peer-to-peer signaling relay — without this binding
    /// mock-actrix drops outbound relays for "unbound target".
    #[cfg(feature = "test-utils")]
    pub async fn connect_to_with_identity(
        url: &str,
        actor_id: ActrId,
        credential_state: CredentialState,
    ) -> NetworkResult<Arc<Self>> {
        let config = SignalingConfig {
            server_url: url.parse()?,
            connection_timeout: 5,
            heartbeat_interval: 30,
            reconnect_config: ReconnectConfig::default(),
            auth_config: None,
            webrtc_role: None,
        };

        let client = Arc::new(Self::new(config));
        client.set_actor_id(actor_id).await;
        client.set_credential_state(credential_state).await;
        client.start_reconnect_manager();
        client.connect().await?;
        Ok(client)
    }

    /// alive integrate down a envelope ID
    async fn next_envelope_id(&self) -> String {
        let mut counter = self.envelope_counter.lock().await;
        *counter += 1;
        format!("env-{}", *counter)
    }

    /// Create SignalingEnvelope
    async fn create_envelope(&self, flow: signaling_envelope::Flow) -> SignalingEnvelope {
        SignalingEnvelope {
            envelope_version: 1,
            envelope_id: self.next_envelope_id().await,
            reply_for: None,
            timestamp: prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            },
            traceparent: None,
            tracestate: None,
            flow: Some(flow),
        }
    }

    /// Reset inbound channel for a fresh session (useful after disconnects).
    async fn reset_inbound_channel(&self) {
        self.drop_pending_replies("inbound channel reset").await;
        self.drop_pending_pongs("inbound channel reset").await;

        let (tx, rx) = mpsc::unbounded_channel();
        *self.inbound_tx.lock().await = tx;
        *self.inbound_rx.lock().await = rx;
    }

    async fn drop_pending_replies(&self, reason: &'static str) {
        let dropped = {
            let mut pending = self.pending_replies.lock().await;
            let dropped = pending.len();
            pending.clear();
            dropped
        };

        if dropped > 0 {
            tracing::debug!(reason, dropped, "Dropping pending signaling reply waiters");
        }
    }

    async fn drop_pending_pongs(&self, reason: &'static str) {
        let dropped = {
            let mut pending = self.pending_pongs.lock().await;
            let dropped = pending.len();
            pending.clear();
            dropped
        };

        if dropped > 0 {
            tracing::debug!(reason, dropped, "Dropping pending signaling pong waiters");
        }
    }

    /// Build signaling URL with actor identity and Ed25519 credential params for authentication.
    ///
    /// Passes `actor_id`, `key_id`, `claims` (base64), `signature` (base64) as URL query params
    /// so the signaling server can validate the credential before upgrading the WebSocket.
    async fn build_url_with_identity(&self) -> Url {
        let mut url = self.config.server_url.clone();
        let actor_id_opt = self.actor_id.lock().await.clone();
        if let Some(actor_id) = actor_id_opt {
            let actor_str = actr_protocol::ActrId::to_string_repr(&actor_id);
            url.query_pairs_mut().append_pair("actor_id", &actor_str);
        }

        // Pass Ed25519 credential in URL for initial WS auth
        let cred_state_opt = self.credential_state.lock().await.clone();
        if let Some(cred_state) = cred_state_opt {
            let cred = cred_state.credential().await;
            let claims_b64 = base64::engine::general_purpose::STANDARD.encode(&cred.claims);
            let sig_b64 = base64::engine::general_purpose::STANDARD.encode(&cred.signature);
            url.query_pairs_mut()
                .append_pair("key_id", &cred.key_id.to_string())
                .append_pair("claims", &claims_b64)
                .append_pair("signature", &sig_b64);
        }

        // Add WebRTC role preference if configured
        if let Some(role) = &self.config.webrtc_role {
            url.query_pairs_mut().append_pair("webrtc_role", role);
        }

        url
    }

    fn redact_signaling_url_for_log(url: &Url) -> String {
        let mut redacted = url.clone();
        let pairs: Vec<(String, String)> = redacted
            .query_pairs()
            .map(|(key, value)| {
                let redacted_value = match key.to_ascii_lowercase().as_str() {
                    "claims" | "signature" | "token" | "authorization" | "bearer"
                    | "access_token" | "api_key" => "REDACTED".to_string(),
                    _ => value.into_owned(),
                };
                (key.into_owned(), redacted_value)
            })
            .collect();

        redacted.set_query(None);
        if !pairs.is_empty() {
            let mut query = redacted.query_pairs_mut();
            for (key, value) in pairs {
                query.append_pair(&key, &value);
            }
        }

        redacted.to_string()
    }

    fn auto_reconnect_cancelled(&self, generation: u64) -> bool {
        self.auto_reconnect_suppressed.load(Ordering::Acquire)
            || self.reconnect_generation.load(Ordering::Acquire) != generation
    }

    /// Establish a single signaling WebSocket connection attempt, honoring connection_timeout.
    ///
    /// This does not perform any retry logic; callers that want retries should wrap this.
    async fn establish_connection_once(&self) -> NetworkResult<()> {
        self.establish_connection_once_with_intent(ConnectIntent::Explicit)
            .await
    }

    async fn establish_connection_once_for_auto_reconnect(
        &self,
        generation: u64,
    ) -> NetworkResult<()> {
        self.establish_connection_once_with_intent(ConnectIntent::AutoReconnect { generation })
            .await
    }

    async fn establish_connection_once_with_intent(
        &self,
        intent: ConnectIntent,
    ) -> NetworkResult<()> {
        // Guard: Check if already connected (handles rare TOCTOU scenarios)
        if self.connected.load(Ordering::Acquire) {
            tracing::debug!("Connection already established, skipping establish_connection_once()");
            return Ok(());
        }

        let url = self.build_url_with_identity().await;
        let timeout_secs = self.config.connection_timeout;
        tracing::debug!(
            "Establishing connection to URL: {}",
            Self::redact_signaling_url_for_log(&url)
        );
        // After disconnection, data written to the buffer will continue to be sent once the network recovers
        let config = WebSocketConfig::default().write_buffer_size(0);
        // Connect with an optional timeout. A timeout of 0 means "no timeout".
        let connect_result = if timeout_secs == 0 {
            connect_async_with_config(url.as_str(), Some(config), false).await
        } else {
            let timeout_duration = std::time::Duration::from_secs(timeout_secs);
            tokio::time::timeout(
                timeout_duration,
                connect_async_with_config(url.as_str(), Some(config), false),
            )
            .await
            .map_err(|_| {
                NetworkError::ConnectionError(format!(
                    "Signaling connect timeout after {}s",
                    timeout_secs
                ))
            })?
        }?;

        let (ws_stream, _) = connect_result;

        // Split read/write halves and initialize client state
        let (sink, stream) = ws_stream.split();

        if let ConnectIntent::AutoReconnect { generation } = intent
            && self.auto_reconnect_cancelled(generation)
        {
            tracing::debug!(
                generation,
                "Discarding completed signaling auto-reconnect after explicit disconnect"
            );
            let mut sink = sink;
            if let Err(e) = sink.close().await {
                tracing::warn!(
                    "Signaling auto-reconnect socket close failed after cancellation: {}",
                    e
                );
            }
            return Err(NetworkError::ConnectionError(
                "Signaling auto-reconnect was cancelled by explicit disconnect".to_string(),
            ));
        }

        *self.ws_sink.lock().await = Some(sink);
        *self.ws_stream.lock().await = Some(stream);
        self.connected.store(true, Ordering::Release);
        self.auto_reconnect_suppressed
            .store(false, Ordering::Release);
        self.last_pong.store(current_unix_secs(), Ordering::Release);
        // Invoke hook synchronously, then broadcast for other subscribers
        self.invoke_hook(HookEvent::SignalingConnected).await;
        let _ = self.event_tx.send(SignalingEvent::Connected);

        self.stats.connections.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Connect to signaling server with retry and exponential backoff based on reconnect_config.
    async fn connect_with_retries(&self) -> NetworkResult<()> {
        use actr_framework::ExponentialBackoff;

        let cfg = &self.config.reconnect_config;

        // If reconnect is disabled, just attempt once.
        if !cfg.enabled {
            return self.connect_once().await;
        }

        let mut last_err = None;

        'cycle: loop {
            let backoff_reset_generation = self
                .reconnect_backoff_reset_generation
                .load(Ordering::Acquire);
            let backoff = ExponentialBackoff::builder()
                .initial_delay(std::time::Duration::from_secs(cfg.initial_delay.max(1)))
                .max_delay(std::time::Duration::from_secs(cfg.max_delay.max(1)))
                .max_retries(cfg.max_attempts)
                .with_jitter()
                .build();

            // First attempt immediately (delay = 0), subsequent delays from backoff
            for (attempt, delay) in std::iter::once(std::time::Duration::ZERO)
                .chain(backoff)
                .enumerate()
            {
                let attempt = attempt as u32 + 1;
                self.invoke_hook(HookEvent::SignalingConnectStart { attempt })
                    .await;
                if delay > std::time::Duration::ZERO {
                    tracing::info!("Retry signaling connect after {delay:?} (attempt {attempt})");
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = self.reconnect_notify.notified() => {
                            tracing::debug!("Explicit reconnect request interrupted signaling connect backoff");
                        }
                    }
                    if self
                        .reconnect_backoff_reset_generation
                        .load(Ordering::Acquire)
                        != backoff_reset_generation
                    {
                        tracing::debug!(
                            "Restarting explicit signaling connect backoff after external recovery event"
                        );
                        continue 'cycle;
                    }
                }

                match self.connect_once().await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        tracing::warn!("Signaling connect attempt {attempt} failed: {e:?}");
                        last_err = Some(e);
                        if self
                            .reconnect_backoff_reset_generation
                            .load(Ordering::Acquire)
                            != backoff_reset_generation
                        {
                            tracing::debug!(
                                "Restarting explicit signaling connect backoff after external recovery event"
                            );
                            continue 'cycle;
                        }
                    }
                }
            }

            let total = cfg.max_attempts + 1; // backoff max_retries + first attempt
            tracing::error!("Signaling connect failed after {total} attempts, giving up");
            return Err(last_err.unwrap_or_else(|| {
                NetworkError::ConnectionError("All connection attempts failed".to_string())
            }));
        }
    }

    /// Send envelope and wait for response with timeout and error handling.
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(skip_all, fields(envelope_id = %envelope.envelope_id))
    )]
    async fn send_envelope_and_wait_response(
        &self,
        envelope: SignalingEnvelope,
    ) -> NetworkResult<SignalingEnvelope> {
        let reply_for = envelope.envelope_id.clone();

        // Register waiter before sending
        let (tx, rx) = oneshot::channel();
        self.pending_replies
            .lock()
            .await
            .insert(reply_for.clone(), tx);

        if let Err(e) = self.send_envelope(envelope).await {
            // Cleanup waiter on immediate send failure to avoid leaks.
            self.pending_replies.lock().await.remove(&reply_for);
            return Err(e);
        }

        let result =
            tokio::time::timeout(std::time::Duration::from_secs(RESPONSE_TIMEOUT_SECS), rx).await;
        // Clean up waiter on timeout
        if result.is_err() {
            self.pending_replies.lock().await.remove(&reply_for);
        }

        let response_envelope = result
            .map_err(|_| {
                NetworkError::ConnectionError(
                    "Timed out waiting for signaling response".to_string(),
                )
            })?
            .map_err(|_| {
                NetworkError::ConnectionError(
                    "Receiver dropped while waiting for signaling response".to_string(),
                )
            })?;

        Ok(response_envelope)
    }

    /// Spawn background receiver to demux envelopes by reply_for.
    async fn start_receiver(&self) {
        let mut stream_guard = self.ws_stream.lock().await;
        if stream_guard.is_none() {
            return;
        }

        let mut stream = stream_guard.take().expect("stream exists");
        let pending = self.pending_replies.clone();
        let inbound_tx = { self.inbound_tx.lock().await.clone() };
        let stats = self.stats.clone();
        let connected = self.connected.clone();
        let event_tx = self.event_tx.clone();
        let last_pong = self.last_pong.clone();
        let pending_pongs = self.pending_pongs.clone();
        let reconnect_notify = self.reconnect_notify.clone();
        let reconnect_enabled = self.config.reconnect_config.enabled;
        let hook_callback = self.hook_callback.get().cloned();
        let handle = tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                        // Any inbound traffic counts as liveness
                        last_pong.store(current_unix_secs(), Ordering::Release);
                        match SignalingEnvelope::decode(&data[..]) {
                            Ok(envelope) => {
                                #[cfg(feature = "opentelemetry")]
                                let span = {
                                    let span = tracing::info_span!("signaling.receive_envelope", envelope_id = %envelope.envelope_id);
                                    span.set_parent(extract_trace_context(&envelope));
                                    span
                                };

                                stats.messages_received.fetch_add(1, Ordering::Relaxed);
                                tracing::debug!("Received message: {:?}", envelope);
                                if let Some(reply_for) = envelope.reply_for.clone() {
                                    if let Some(sender) = pending.lock().await.remove(&reply_for) {
                                        #[cfg(feature = "opentelemetry")]
                                        let _ = span.enter();
                                        if let Err(e) = sender.send(envelope) {
                                            stats.errors.fetch_add(1, Ordering::Relaxed);
                                            tracing::warn!(
                                                "Failed to send reply envelope to waiter: {e:?}",
                                            );
                                        }
                                        continue;
                                    }
                                }
                                tracing::debug!(
                                    "Unmatched or push message -> forward to inbound channel"
                                );
                                // Unmatched or push message -> forward to inbound channel
                                if let Err(e) = inbound_tx.send(envelope) {
                                    stats.errors.fetch_add(1, Ordering::Relaxed);
                                    tracing::warn!(
                                        "Failed to send envelope to inbound channel: {e:?}"
                                    );
                                }
                            }
                            Err(e) => {
                                stats.errors.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!("Failed to decode SignalingEnvelope: {e}");
                            }
                        }
                    }
                    Ok(tokio_tungstenite::tungstenite::Message::Pong(payload)) => {
                        tracing::debug!("Received pong");
                        last_pong.store(current_unix_secs(), Ordering::Release);
                        if let Some(sender) = pending_pongs.lock().await.remove(&payload.to_vec()) {
                            let _ = sender.send(());
                        }
                    }
                    Ok(tokio_tungstenite::tungstenite::Message::Ping(_)) => {
                        tracing::debug!("Received ping");
                        last_pong.store(current_unix_secs(), Ordering::Release);
                    }
                    Ok(other) => {
                        tracing::warn!("Received non-binary frame, ignoring: {other:?}");
                    }
                    Err(e) => {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                        tracing::error!("Signaling receive error: {e}");
                        break;
                    }
                }
            }

            tracing::warn!("Stream terminated");
            // If explicit disconnect already marked the client disconnected,
            // do not start an automatic reconnect cycle for the intentional
            // close. The disconnect path publishes its own Manual event.
            let was_connected = connected.swap(false, Ordering::AcqRel);
            Self::publish_disconnected_transition(
                was_connected,
                &stats,
                &event_tx,
                hook_callback,
                DisconnectReason::StreamEnded,
                reconnect_enabled.then_some(&reconnect_notify),
            )
            .await;
            pending_pongs.lock().await.clear();
        });

        *self.receiver_task.lock().await = Some(handle);
    }

    /// Spawn background ping task to detect half-open connections where writes do not fail but peer is gone.
    /// fixme: merge to heartbeat task
    async fn start_ping_task(&self) {
        let mut existing = self.ping_task.lock().await;
        if let Some(handle) = existing.as_ref() {
            if handle.is_finished() {
                existing.take();
            } else {
                return;
            }
        }

        let sink = self.ws_sink.clone();
        let connected = self.connected.clone();
        let stats = self.stats.clone();
        let event_tx = self.event_tx.clone();
        let last_pong = self.last_pong.clone();
        let receiver_task_clone = Arc::clone(&self.receiver_task);
        let reconnect_notify = self.reconnect_notify.clone();
        let reconnect_enabled = self.config.reconnect_config.enabled;
        let hook_callback = self.hook_callback.get().cloned();

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(PING_INTERVAL_SECS)).await;

                if !connected.load(Ordering::Acquire) {
                    break;
                }

                // Send ping; mark disconnect on failure.
                let mut disconnect_reason = None;
                {
                    let mut sink_guard = sink.lock().await;
                    if let Some(sink) = sink_guard.as_mut() {
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(SIGNALING_SEND_TIMEOUT_SECS),
                            sink.send(tokio_tungstenite::tungstenite::Message::Ping(
                                Vec::new().into(),
                            )),
                        )
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                tracing::warn!("Signaling ping send failed: {e}");
                                disconnect_reason = Some(DisconnectReason::PingSendFailed);
                            }
                            Err(_) => {
                                tracing::warn!("Signaling ping send timed out");
                                disconnect_reason = Some(DisconnectReason::PingSendFailed);
                            }
                        }
                    } else {
                        tracing::warn!("Signaling not connected");
                        disconnect_reason = Some(DisconnectReason::PingSendFailed);
                    }
                }

                if let Some(reason) = disconnect_reason {
                    let was_connected = connected.swap(false, Ordering::AcqRel);
                    Self::publish_disconnected_transition(
                        was_connected,
                        &stats,
                        &event_tx,
                        hook_callback.clone(),
                        reason,
                        reconnect_enabled.then_some(&reconnect_notify),
                    )
                    .await;
                    break;
                }

                // Check for stale pong
                let now = current_unix_secs();
                let last = last_pong.load(Ordering::Acquire);
                if now.saturating_sub(last) > PONG_TIMEOUT_SECS {
                    tracing::warn!(
                        "Signaling pong timeout (last seen {}s ago), marking disconnected",
                        now.saturating_sub(last)
                    );
                    if let Some(handle) = receiver_task_clone.lock().await.take() {
                        handle.abort();
                    }
                    let was_connected = connected.swap(false, Ordering::AcqRel);
                    Self::publish_disconnected_transition(
                        was_connected,
                        &stats,
                        &event_tx,
                        hook_callback.clone(),
                        DisconnectReason::PongTimeout,
                        reconnect_enabled.then_some(&reconnect_notify),
                    )
                    .await;
                    break;
                }
            }
        });

        *existing = Some(handle);
    }

    async fn disconnect_internal(&self, suppress_auto_reconnect: bool) -> NetworkResult<()> {
        if suppress_auto_reconnect {
            self.reconnect_generation.fetch_add(1, Ordering::AcqRel);
            self.auto_reconnect_suppressed
                .store(true, Ordering::Release);
            self.reconnect_notify.notify_waiters();
        }

        self.drop_pending_replies("signaling disconnect").await;
        self.drop_pending_pongs("signaling disconnect").await;
        let was_connected = self.connected.swap(false, Ordering::AcqRel);

        // Stop background tasks before taking the WebSocket sink/stream locks.
        // A ping or receiver task can be inside a socket operation while holding
        // one of those locks; aborting first keeps disconnect from waiting on
        // the task it is about to shut down.
        let ping_handle = match tokio::time::timeout(
            std::time::Duration::from_secs(DISCONNECT_LOCK_TIMEOUT_SECS),
            self.ping_task.lock(),
        )
        .await
        {
            Ok(mut task_guard) => task_guard.take(),
            Err(_) => {
                tracing::warn!("Timed out waiting for signaling ping task lock during disconnect");
                None
            }
        };
        if let Some(handle) = ping_handle {
            handle.abort();
        }

        let receiver_handle = match tokio::time::timeout(
            std::time::Duration::from_secs(DISCONNECT_LOCK_TIMEOUT_SECS),
            self.receiver_task.lock(),
        )
        .await
        {
            Ok(mut task_guard) => task_guard.take(),
            Err(_) => {
                tracing::warn!(
                    "Timed out waiting for signaling receiver task lock during disconnect"
                );
                None
            }
        };
        if let Some(handle) = receiver_handle {
            handle.abort();
        }

        // Fetch and close the sink without holding the mutex during the close
        // await. The lock itself is bounded because a stalled send can hold it
        // on broken mobile network transitions.
        let sink = match tokio::time::timeout(
            std::time::Duration::from_secs(DISCONNECT_LOCK_TIMEOUT_SECS),
            self.ws_sink.lock(),
        )
        .await
        {
            Ok(mut sink_guard) => sink_guard.take(),
            Err(_) => {
                tracing::warn!(
                    "Timed out waiting for signaling WebSocket sink lock during disconnect"
                );
                None
            }
        };

        if let Some(mut sink) = sink {
            match tokio::time::timeout(
                std::time::Duration::from_secs(DISCONNECT_CLOSE_TIMEOUT_SECS),
                sink.close(),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!("Signaling WebSocket close failed during disconnect: {}", e);
                }
                Err(_) => {
                    tracing::warn!(
                        "Signaling WebSocket close timed out during disconnect; continuing cleanup"
                    );
                }
            }
        }

        match tokio::time::timeout(
            std::time::Duration::from_secs(DISCONNECT_LOCK_TIMEOUT_SECS),
            self.ws_stream.lock(),
        )
        .await
        {
            Ok(mut stream_guard) => {
                stream_guard.take();
            }
            Err(_) => {
                tracing::warn!(
                    "Timed out waiting for signaling WebSocket stream lock during disconnect"
                );
            }
        }

        self.reset_inbound_channel().await;

        // Invoke hook synchronously, then broadcast for other subscribers
        Self::publish_disconnected_transition(
            was_connected,
            &self.stats,
            &self.event_tx,
            self.hook_callback.get().cloned(),
            DisconnectReason::Manual,
            None,
        )
        .await;

        Ok(())
    }

    async fn connect_once_for_auto_reconnect(&self, generation: u64) -> NetworkResult<()> {
        if self.auto_reconnect_cancelled(generation) {
            return Err(NetworkError::ConnectionError(
                "Signaling auto-reconnect was cancelled".to_string(),
            ));
        }

        if self.connected.load(Ordering::Acquire) {
            tracing::debug!("Already connected, skipping auto-reconnect connect_once()");
            return Ok(());
        }

        match self
            .connecting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(_) => {
                if self.connected.load(Ordering::Acquire) {
                    tracing::debug!("Already connected, skipping auto-reconnect connect_once()");
                    return Ok(());
                }

                tracing::debug!(
                    "Another connection attempt in progress, waiting for state change..."
                );
                let result = self.wait_for_connection_result().await;
                if self.auto_reconnect_cancelled(generation) {
                    return Err(NetworkError::ConnectionError(
                        "Signaling auto-reconnect was cancelled".to_string(),
                    ));
                }
                return result;
            }
        }

        if self.auto_reconnect_cancelled(generation) {
            self.connecting.store(false, Ordering::Release);
            return Err(NetworkError::ConnectionError(
                "Signaling auto-reconnect was cancelled".to_string(),
            ));
        }

        if self.connected.load(Ordering::Acquire) {
            tracing::debug!("Connection completed by another task while acquiring lock");
            self.connecting.store(false, Ordering::Release);
            return Ok(());
        }

        tracing::debug!(
            generation,
            "Acquired connection lock, establishing one auto-reconnect signaling attempt..."
        );

        let result = self
            .establish_connection_once_for_auto_reconnect(generation)
            .await;
        self.connecting.store(false, Ordering::Release);

        match result {
            Ok(()) => {
                if self.auto_reconnect_cancelled(generation) {
                    self.disconnect_internal(false).await?;
                    return Err(NetworkError::ConnectionError(
                        "Signaling auto-reconnect was cancelled".to_string(),
                    ));
                }
                self.start_receiver().await;
                self.start_ping_task().await;
                Ok(())
            }
            Err(e) => {
                if !self.auto_reconnect_cancelled(generation) {
                    let _ = self.event_tx.send(SignalingEvent::Disconnected {
                        reason: DisconnectReason::ConnectionFailed(e.to_string()),
                    });
                    tracing::error!("Connection attempt failed: {e}");
                }
                Err(e)
            }
        }
    }

    /// Wait for ongoing connection attempt to complete (used when another task is connecting).
    ///
    /// Uses the broadcast channel to wait for a Connected event without recursion.
    async fn wait_for_connection_result(&self) -> NetworkResult<()> {
        let mut event_rx = self.event_tx.subscribe();
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(CONCURRENT_CONNECT_WAIT_TIMEOUT_SECS);

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    // Final check before giving up
                    if self.connected.load(Ordering::Acquire) {
                        tracing::debug!("Connection succeeded just before timeout");
                        return Ok(());
                    }
                    return Err(NetworkError::ConnectionError(
                        "Timeout waiting for concurrent connection attempt".to_string(),
                    ));
                }
                result = event_rx.recv() => {
                    match result {
                        Ok(SignalingEvent::Connected) => {
                            tracing::debug!("Connection established by another task");
                            return Ok(());
                        }
                        Ok(SignalingEvent::Disconnected { reason }) => {
                            return Err(NetworkError::ConnectionError(format!(
                                "Concurrent signaling connection failed: {reason:?}"
                            )));
                        }
                        Ok(_) => continue, // ConnectStart — keep waiting
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("Event receiver lagged by {n} events");
                            // Check current state after lag
                            if self.connected.load(Ordering::Acquire) {
                                return Ok(());
                            }
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            return Err(NetworkError::ConnectionError(
                                "Event channel closed while waiting for connection".to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl SignalingClient for WebSocketSignalingClient {
    async fn connect(&self) -> NetworkResult<()> {
        if self.connected.load(Ordering::Acquire) {
            tracing::debug!("Already connected, skipping connect()");
            return Ok(());
        }

        self.connect_with_retries().await
    }

    async fn connect_once(&self) -> NetworkResult<()> {
        loop {
            if self.connected.load(Ordering::Acquire) {
                tracing::debug!("Already connected, skipping connect_once()");
                return Ok(());
            }

            match self
                .connecting
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(_) => {
                    if self.connected.load(Ordering::Acquire) {
                        tracing::debug!("Already connected, skipping connect_once()");
                        return Ok(());
                    }

                    tracing::debug!(
                        "Another connection attempt in progress, waiting for state change..."
                    );
                    match self.wait_for_connection_result().await {
                        Ok(()) => return Ok(()),
                        Err(e)
                            if !self.connected.load(Ordering::Acquire)
                                && !self.connecting.load(Ordering::Acquire) =>
                        {
                            tracing::debug!(
                                "Concurrent signaling connection failed; explicit connect_once will retry immediately: {e}"
                            );
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        if self.connected.load(Ordering::Acquire) {
            tracing::debug!("Connection completed by another task while acquiring lock");
            self.connecting.store(false, Ordering::Release);
            return Ok(());
        }

        tracing::debug!(
            "Acquired connection lock, establishing one signaling connection attempt..."
        );

        let result = self.establish_connection_once().await;
        self.connecting.store(false, Ordering::Release);

        match result {
            Ok(()) => {
                self.start_receiver().await;
                self.start_ping_task().await;
                Ok(())
            }
            Err(e) => {
                let _ = self.event_tx.send(SignalingEvent::Disconnected {
                    reason: DisconnectReason::ConnectionFailed(e.to_string()),
                });
                tracing::error!("Connection attempt failed: {e}");
                Err(e)
            }
        }
    }

    fn schedule_auto_reconnect(&self) {
        if !self.config.reconnect_config.enabled {
            tracing::debug!("Skipping signaling auto-reconnect schedule; config disabled");
            return;
        }

        self.auto_reconnect_suppressed
            .store(false, Ordering::Release);
        self.reconnect_notify.notify_one();
    }

    fn schedule_auto_reconnect_reset_backoff(&self) {
        if !self.config.reconnect_config.enabled {
            tracing::debug!("Skipping signaling auto-reconnect schedule; config disabled");
            return;
        }

        self.auto_reconnect_suppressed
            .store(false, Ordering::Release);
        self.reconnect_backoff_reset_generation
            .fetch_add(1, Ordering::AcqRel);
        self.reconnect_notify.notify_one();
    }

    async fn disconnect(&self) -> NetworkResult<()> {
        self.disconnect_internal(true).await
    }

    async fn probe_alive(&self, timeout: Duration) -> NetworkResult<()> {
        if !self.connected.load(Ordering::Acquire) {
            return Err(NetworkError::ConnectionError(
                "Signaling client is not connected".to_string(),
            ));
        }

        let probe_id = self.probe_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let payload =
            format!("actr-signaling-probe-{probe_id}-{}", current_unix_secs()).into_bytes();
        let (tx, rx) = oneshot::channel();
        self.pending_pongs.lock().await.insert(payload.clone(), tx);

        let send_timeout = std::cmp::min(
            timeout,
            std::time::Duration::from_secs(SIGNALING_SEND_TIMEOUT_SECS),
        );
        let ping_payload = payload.clone();
        let send_result = tokio::time::timeout(send_timeout, async {
            let mut sink_guard = self.ws_sink.lock().await;
            match sink_guard.as_mut() {
                Some(sink) => sink
                    .send(tokio_tungstenite::tungstenite::Message::Ping(
                        ping_payload.into(),
                    ))
                    .await
                    .map_err(|e| {
                        NetworkError::ConnectionError(format!("Signaling probe ping failed: {e}"))
                    }),
                None => Err(NetworkError::ConnectionError(
                    "Signaling probe failed: WebSocket sink is not available".to_string(),
                )),
            }
        })
        .await
        .unwrap_or_else(|_| {
            Err(NetworkError::TimeoutError(format!(
                "Timed out sending signaling probe ping after {}ms",
                send_timeout.as_millis()
            )))
        });

        if let Err(e) = send_result {
            self.pending_pongs.lock().await.remove(&payload);
            let was_connected = self.connected.swap(false, Ordering::AcqRel);
            Self::publish_disconnected_transition(
                was_connected,
                &self.stats,
                &self.event_tx,
                self.hook_callback.get().cloned(),
                DisconnectReason::PingSendFailed,
                None,
            )
            .await;
            return Err(e);
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(())) => {
                self.last_pong.store(current_unix_secs(), Ordering::Release);
                Ok(())
            }
            Ok(Err(_)) => {
                self.pending_pongs.lock().await.remove(&payload);
                Err(NetworkError::ConnectionError(
                    "Signaling probe pong waiter dropped".to_string(),
                ))
            }
            Err(_) => {
                self.pending_pongs.lock().await.remove(&payload);
                Err(NetworkError::TimeoutError(format!(
                    "Timed out waiting for signaling probe pong after {}ms",
                    timeout.as_millis()
                )))
            }
        }
    }

    #[cfg_attr(feature = "opentelemetry", tracing::instrument(skip_all))]
    async fn send_register_request(
        &self,
        request: RegisterRequest,
    ) -> NetworkResult<RegisterResponse> {
        // Create PeerToSignaling stream process （Register front ）
        let flow = signaling_envelope::Flow::PeerToServer(PeerToSignaling {
            payload: Some(peer_to_signaling::Payload::RegisterRequest(request)),
        });

        let envelope = self.create_envelope(flow).await;
        let response_envelope = self.send_envelope_and_wait_response(envelope).await?;

        if let Some(signaling_envelope::Flow::ServerToActr(server_to_actr)) = response_envelope.flow
        {
            if let Some(signaling_to_actr::Payload::RegisterResponse(response)) =
                server_to_actr.payload
            {
                return Ok(response);
            }
        }

        Err(NetworkError::ConnectionError(
            "Invalid registration response".to_string(),
        ))
    }

    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(skip_all, fields(actor_id = %actor_id))
    )]
    async fn send_unregister_request(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
        reason: Option<String>,
    ) -> NetworkResult<UnregisterResponse> {
        // Build UnregisterRequest payload
        let request = UnregisterRequest {
            actr_id: actor_id.clone(),
            reason,
        };

        // Wrap into ActrToSignaling flow
        let flow = signaling_envelope::Flow::ActrToServer(ActrToSignaling {
            source: actor_id,
            credential,
            payload: Some(actr_to_signaling::Payload::UnregisterRequest(request)),
        });

        // Send envelope (fire-and-forget)
        let envelope = self.create_envelope(flow).await;
        self.send_envelope(envelope).await?;

        // Do not wait for UnregisterResponse here because the signaling stream
        // is also consumed by WebRtcCoordinator. Waiting could race with that loop
        // and lead to spurious timeouts. Treat Unregister as best-effort.
        // not wait for the response , because the signaling stream have multi customers use it, fixme: should wait for the response
        Ok(UnregisterResponse {
            result: Some(actr_protocol::unregister_response::Result::Success(
                actr_protocol::unregister_response::UnregisterOk {},
            )),
        })
    }

    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(level = "debug", skip_all, fields(actor_id = %actor_id))
    )]
    async fn send_heartbeat(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
        availability: ServiceAvailabilityState,
        power_reserve: f32,
        mailbox_backlog: f32,
    ) -> NetworkResult<Pong> {
        let ping = Ping {
            availability: availability as i32,
            power_reserve,
            mailbox_backlog,
            sticky_client_ids: vec![], // TODO: Implement sticky session tracking
        };

        let flow = signaling_envelope::Flow::ActrToServer(ActrToSignaling {
            source: actor_id,
            credential,
            payload: Some(actr_to_signaling::Payload::Ping(ping)),
        });

        let envelope = self.create_envelope(flow).await;
        let reply_for = envelope.envelope_id.clone();

        // Register waiter before sending
        let (tx, rx) = oneshot::channel();
        self.pending_replies
            .lock()
            .await
            .insert(reply_for.clone(), tx);

        if let Err(e) = self.send_envelope(envelope).await {
            // Cleanup waiter on immediate send failure to avoid leaks.
            self.pending_replies.lock().await.remove(&reply_for);
            return Err(e);
        }

        // Wait for response
        let response_envelope = rx.await.map_err(|_| {
            NetworkError::ConnectionError(
                "Receiver dropped while waiting for heartbeat response".to_string(),
            )
        })?;

        // Extract Pong from response, or handle Error response
        if let Some(signaling_envelope::Flow::ServerToActr(server_to_actr)) = response_envelope.flow
        {
            match server_to_actr.payload {
                Some(signaling_to_actr::Payload::Pong(pong)) => {
                    return Ok(pong);
                }
                Some(signaling_to_actr::Payload::Error(err)) => {
                    // Check if it's a credential expired error (401)
                    if err.code == 401 {
                        return Err(NetworkError::CredentialExpired(err.message));
                    }
                    return Err(NetworkError::AuthenticationError(format!(
                        "{} ({})",
                        err.message, err.code
                    )));
                }
                _ => {}
            }
        }

        Err(NetworkError::ConnectionError(
            "Received response but not a Pong message".to_string(),
        ))
    }

    #[cfg_attr(feature = "opentelemetry", tracing::instrument(skip_all))]
    async fn send_route_candidates_request(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
        request: RouteCandidatesRequest,
    ) -> NetworkResult<RouteCandidatesResponse> {
        let flow = signaling_envelope::Flow::ActrToServer(ActrToSignaling {
            source: actor_id,
            credential,
            payload: Some(actr_to_signaling::Payload::RouteCandidatesRequest(request)),
        });

        let envelope = self.create_envelope(flow).await;
        let response_envelope = self.send_envelope_and_wait_response(envelope).await?;

        if let Some(signaling_envelope::Flow::ServerToActr(server_to_actr)) = response_envelope.flow
        {
            match server_to_actr.payload {
                Some(signaling_to_actr::Payload::RouteCandidatesResponse(response)) => {
                    return Ok(response);
                }
                Some(signaling_to_actr::Payload::Error(err)) => {
                    return Err(NetworkError::ServiceDiscoveryError(format!(
                        "{} ({})",
                        err.message, err.code
                    )));
                }
                _ => {}
            }
        }

        Err(NetworkError::ConnectionError(
            "Invalid route candidates response".to_string(),
        ))
    }

    async fn get_signing_key(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
        key_id: u32,
    ) -> NetworkResult<(u32, Vec<u8>)> {
        let flow = signaling_envelope::Flow::ActrToServer(ActrToSignaling {
            source: actor_id,
            credential,
            payload: Some(actr_to_signaling::Payload::GetSigningKeyRequest(
                GetSigningKeyRequest { key_id },
            )),
        });

        let envelope = self.create_envelope(flow).await;
        let response_envelope = self.send_envelope_and_wait_response(envelope).await?;

        if let Some(signaling_envelope::Flow::ServerToActr(server_to_actr)) = response_envelope.flow
        {
            match server_to_actr.payload {
                Some(signaling_to_actr::Payload::GetSigningKeyResponse(resp)) => {
                    return Ok((resp.key_id, resp.pubkey.to_vec()));
                }
                Some(signaling_to_actr::Payload::Error(err)) => {
                    return Err(NetworkError::ConnectionError(format!(
                        "get_signing_key failed: {} ({})",
                        err.message, err.code
                    )));
                }
                _ => {}
            }
        }

        Err(NetworkError::ConnectionError(
            "get_signing_key: invalid response".to_string(),
        ))
    }

    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(level = "debug", skip_all, fields(actor_id = %actor_id))
    )]
    async fn send_credential_update_request(
        &self,
        actor_id: ActrId,
        credential: AIdCredential,
    ) -> NetworkResult<RegisterResponse> {
        let request = CredentialUpdateRequest {
            actr_id: actor_id.clone(),
        };

        let flow = signaling_envelope::Flow::ActrToServer(ActrToSignaling {
            source: actor_id,
            credential,
            payload: Some(actr_to_signaling::Payload::CredentialUpdateRequest(request)),
        });

        let envelope = self.create_envelope(flow).await;
        let response_envelope = self.send_envelope_and_wait_response(envelope).await?;

        if let Some(signaling_envelope::Flow::ServerToActr(server_to_actr)) = response_envelope.flow
        {
            match server_to_actr.payload {
                Some(signaling_to_actr::Payload::RegisterResponse(response)) => {
                    return Ok(response);
                }
                Some(signaling_to_actr::Payload::Error(err)) => {
                    return Err(NetworkError::ConnectionError(format!(
                        "Credential update failed: {} ({})",
                        err.message, err.code
                    )));
                }
                _ => {}
            }
        }

        Err(NetworkError::ConnectionError(
            "Invalid credential update response".to_string(),
        ))
    }

    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(level = "debug", skip_all, fields(envelope_id = %envelope.envelope_id))
    )]
    async fn send_envelope(&self, envelope: SignalingEnvelope) -> NetworkResult<()> {
        #[cfg(feature = "opentelemetry")]
        let envelope = {
            let mut envelope = envelope;
            trace::inject_span_context(&tracing::Span::current(), &mut envelope);
            envelope
        };

        // Check connection state first to avoid sending on stale/closed connections
        // This prevents "Broken pipe" errors when ws_sink exists but connection is dead
        if !self.is_connected() {
            return Err(NetworkError::ConnectionError(
                "Cannot send: WebSocket not connected".to_string(),
            ));
        }

        let mut sink_guard = self.ws_sink.lock().await;

        if let Some(sink) = sink_guard.as_mut() {
            // using protobuf binary serialization
            let mut buf = Vec::new();
            envelope.encode(&mut buf)?;
            let msg = tokio_tungstenite::tungstenite::Message::Binary(buf.into());
            match tokio::time::timeout(
                std::time::Duration::from_secs(SIGNALING_SEND_TIMEOUT_SECS),
                sink.send(msg),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    self.connected.store(false, Ordering::Release);
                    return Err(NetworkError::ConnectionError(
                        "Signaling WebSocket send timed out".to_string(),
                    ));
                }
            }

            self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("Stats: {:?}", self.stats.snapshot());
            Ok(())
        } else {
            Err(NetworkError::ConnectionError("Not connected".to_string()))
        }
    }

    async fn receive_envelope(&self) -> NetworkResult<Option<SignalingEnvelope>> {
        let mut rx = self.inbound_rx.lock().await;
        match rx.recv().await {
            Some(envelope) => Ok(Some(envelope)),
            None => {
                tracing::error!("Inbound channel closed");
                Err(NetworkError::ConnectionError(
                    "Inbound channel closed".to_string(),
                ))
            }
        }
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn get_stats(&self) -> SignalingStats {
        self.stats.snapshot()
    }

    fn subscribe_events(&self) -> broadcast::Receiver<SignalingEvent> {
        self.event_tx.subscribe()
    }

    async fn set_actor_id(&self, actor_id: ActrId) {
        *self.actor_id.lock().await = Some(actor_id);
    }

    async fn set_credential_state(&self, credential_state: CredentialState) {
        *self.credential_state.lock().await = Some(credential_state);
    }

    async fn clear_identity(&self) {
        *self.actor_id.lock().await = None;
        *self.credential_state.lock().await = None;
    }

    fn set_hook_callback(&self, cb: HookCallback) {
        let _ = self.hook_callback.set(cb);
    }
}

/// signaling statistics info
#[derive(Debug)]
pub(crate) struct AtomicSignalingStats {
    /// Connect attempts
    pub connections: AtomicU64,

    /// DisconnectConnect attempts
    pub disconnections: AtomicU64,

    /// Send'smessage number
    pub messages_sent: AtomicU64,

    /// Receive'smessage number
    pub messages_received: AtomicU64,

    /// Send's center skip number
    /// TODO: Wire heartbeat counters when heartbeat send/receive paths are instrumented; currently never incremented.
    pub heartbeats_sent: AtomicU64,

    /// Receive's center skip number
    /// TODO: Wire heartbeat counters when heartbeat send/receive paths are instrumented; currently never incremented.
    pub heartbeats_received: AtomicU64,

    /// Error attempts
    pub errors: AtomicU64,
}

impl Default for AtomicSignalingStats {
    fn default() -> Self {
        Self {
            connections: AtomicU64::new(0),
            disconnections: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            heartbeats_sent: AtomicU64::new(0),
            heartbeats_received: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}

/// Snapshot of statistics for serialization and reading
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct SignalingStats {
    /// Connect attempts
    pub connections: u64,

    /// DisconnectConnect attempts
    pub disconnections: u64,

    /// Send'smessage number
    pub messages_sent: u64,

    /// Receive'smessage number
    pub messages_received: u64,

    /// Send's center skip number
    pub heartbeats_sent: u64,

    /// Receive's center skip number
    pub heartbeats_received: u64,

    /// Error attempts
    pub errors: u64,
}

impl AtomicSignalingStats {
    /// Create a snapshot of current statistics
    pub fn snapshot(&self) -> SignalingStats {
        SignalingStats {
            connections: self.connections.load(Ordering::Relaxed),
            disconnections: self.disconnections.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            heartbeats_sent: self.heartbeats_sent.load(Ordering::Relaxed),
            heartbeats_received: self.heartbeats_received.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

fn current_unix_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering as UsizeOrdering};

    /// Simple fake SignalingClient implementation for testing the reconnect helper.
    struct FakeSignalingClient {
        event_tx: broadcast::Sender<SignalingEvent>,
        connected: AtomicBool,
        connect_calls: Arc<AtomicUsize>,
        actor_id: tokio::sync::Mutex<Option<ActrId>>,
        credential_state: tokio::sync::Mutex<Option<CredentialState>>,
    }

    #[async_trait]
    impl SignalingClient for FakeSignalingClient {
        async fn connect(&self) -> NetworkResult<()> {
            self.connect_calls.fetch_add(1, UsizeOrdering::SeqCst);
            Ok(())
        }

        async fn disconnect(&self) -> NetworkResult<()> {
            Ok(())
        }

        async fn send_register_request(
            &self,
            _request: RegisterRequest,
        ) -> NetworkResult<RegisterResponse> {
            unimplemented!("not needed in tests");
        }

        async fn send_unregister_request(
            &self,
            _actor_id: ActrId,
            _credential: AIdCredential,
            _reason: Option<String>,
        ) -> NetworkResult<UnregisterResponse> {
            unimplemented!("not needed in tests");
        }

        async fn send_heartbeat(
            &self,
            _actor_id: ActrId,
            _credential: AIdCredential,
            _availability: ServiceAvailabilityState,
            _power_reserve: f32,
            _mailbox_backlog: f32,
        ) -> NetworkResult<Pong> {
            unimplemented!("not needed in tests");
        }

        async fn send_route_candidates_request(
            &self,
            _actor_id: ActrId,
            _credential: AIdCredential,
            _request: RouteCandidatesRequest,
        ) -> NetworkResult<RouteCandidatesResponse> {
            unimplemented!("not needed in tests");
        }

        async fn get_signing_key(
            &self,
            _actor_id: ActrId,
            _credential: AIdCredential,
            _key_id: u32,
        ) -> NetworkResult<(u32, Vec<u8>)> {
            unimplemented!("not needed in tests");
        }

        async fn send_credential_update_request(
            &self,
            _actor_id: ActrId,
            _credential: AIdCredential,
        ) -> NetworkResult<RegisterResponse> {
            unimplemented!("not needed in tests");
        }

        async fn send_envelope(&self, _envelope: SignalingEnvelope) -> NetworkResult<()> {
            unimplemented!("not needed in tests");
        }

        async fn receive_envelope(&self) -> NetworkResult<Option<SignalingEnvelope>> {
            unimplemented!("not needed in tests");
        }

        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::SeqCst)
        }

        fn get_stats(&self) -> SignalingStats {
            SignalingStats::default()
        }

        fn subscribe_events(&self) -> broadcast::Receiver<SignalingEvent> {
            self.event_tx.subscribe()
        }

        async fn set_actor_id(&self, actor_id: ActrId) {
            *self.actor_id.lock().await = Some(actor_id);
        }

        async fn set_credential_state(&self, credential_state: CredentialState) {
            *self.credential_state.lock().await = Some(credential_state);
        }

        async fn clear_identity(&self) {
            *self.actor_id.lock().await = None;
            *self.credential_state.lock().await = None;
        }
    }

    fn make_fake_client() -> Arc<FakeSignalingClient> {
        let (event_tx, _erx) = broadcast::channel(64);
        Arc::new(FakeSignalingClient {
            event_tx,
            connected: AtomicBool::new(false),
            connect_calls: Arc::new(AtomicUsize::new(0)),
            actor_id: tokio::sync::Mutex::new(None),
            credential_state: tokio::sync::Mutex::new(None),
        })
    }

    /// Helper: create a minimal SignalingConfig with an unreachable URL.
    fn make_config() -> SignalingConfig {
        SignalingConfig {
            server_url: Url::parse("ws://127.0.0.1:1/signaling/ws").unwrap(),
            connection_timeout: 2,
            heartbeat_interval: 30,
            reconnect_config: ReconnectConfig::default(),
            auth_config: None,
            webrtc_role: None,
        }
    }

    /// Helper: create a WebSocketSignalingClient wrapped in Arc
    fn make_ws_client(config: SignalingConfig) -> Arc<WebSocketSignalingClient> {
        Arc::new(WebSocketSignalingClient::new(config))
    }

    #[tokio::test]
    async fn probe_alive_times_out_when_sink_lock_is_stalled() {
        let client = make_ws_client(make_config());
        client.connected.store(true, Ordering::Release);

        let _sink_guard = client.ws_sink.lock().await;

        let result = tokio::time::timeout(
            Duration::from_millis(250),
            client.probe_alive(Duration::from_millis(20)),
        )
        .await
        .expect("probe should be bounded by its own timeout");

        let err = result.expect_err("stalled sink lock should fail the probe");
        assert!(
            err.to_string()
                .contains("Timed out sending signaling probe ping"),
            "unexpected error: {err}"
        );
        assert!(
            !client.is_connected(),
            "stalled probe send should mark signaling disconnected"
        );
        assert_eq!(client.get_stats().disconnections, 1);
        assert!(
            client.pending_pongs.lock().await.is_empty(),
            "failed probe send should remove its pending pong waiter"
        );
    }

    #[tokio::test]
    async fn explicit_connect_once_retries_after_concurrent_attempt_fails() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let server_url = format!(
            "ws://{}/signaling/ws",
            listener
                .local_addr()
                .expect("test listener should have local addr")
        );
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("test server should accept tcp connection");
            let ws_stream = tokio_tungstenite::accept_async(stream)
                .await
                .expect("test server should complete websocket handshake");
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(ws_stream);
        });

        let mut config = make_config();
        config.server_url = Url::parse(&server_url).expect("test websocket URL should parse");
        config.connection_timeout = 2;
        config.reconnect_config = ReconnectConfig {
            enabled: false,
            ..ReconnectConfig::default()
        };
        let client = make_ws_client(config);

        client.connecting.store(true, Ordering::Release);
        let connect_task = {
            let client = client.clone();
            tokio::spawn(async move { client.connect_once().await })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        client.connecting.store(false, Ordering::Release);
        let _ = client.event_tx.send(SignalingEvent::Disconnected {
            reason: DisconnectReason::ConnectionFailed("simulated auto attempt failed".into()),
        });

        tokio::time::timeout(Duration::from_secs(2), connect_task)
            .await
            .expect("explicit connect_once should not wait for auto backoff")
            .expect("connect_once task should not panic")
            .expect("explicit connect_once should retry after concurrent failure");

        assert!(
            client.is_connected(),
            "explicit recovery connect should establish signaling"
        );

        client.disconnect().await.ok();
        let _ = tokio::time::timeout(Duration::from_secs(1), server_task).await;
    }

    #[tokio::test]
    async fn network_restore_connect_once_preempts_connect_backoff() {
        let reserved_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should reserve a local port");
        let addr = reserved_listener
            .local_addr()
            .expect("reserved listener should have local addr");
        drop(reserved_listener);

        let mut config = make_config();
        config.server_url =
            Url::parse(&format!("ws://{addr}/signaling/ws")).expect("test URL should parse");
        config.connection_timeout = 1;
        config.reconnect_config = ReconnectConfig {
            enabled: true,
            max_attempts: 10,
            initial_delay: 30,
            max_delay: 30,
            backoff_multiplier: 1.0,
        };
        let client = make_ws_client(config);
        let mut rx = client.subscribe_events();

        let long_connect_task = {
            let client = client.clone();
            tokio::spawn(async move { client.connect().await })
        };

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match rx.recv().await {
                    Ok(SignalingEvent::Disconnected {
                        reason: DisconnectReason::ConnectionFailed(_),
                    }) => break,
                    Ok(_) => continue,
                    Err(e) => panic!("unexpected signaling event receive error: {e}"),
                }
            }
        })
        .await
        .expect("long connect should fail first attempt and enter backoff");
        assert!(
            !client.connecting.load(Ordering::Acquire),
            "connect() must release connecting while sleeping in backoff"
        );

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("network restore should make the signaling endpoint reachable");
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("restored test server should accept tcp connection");
            let ws_stream = tokio_tungstenite::accept_async(stream)
                .await
                .expect("restored test server should complete websocket handshake");
            tokio::time::sleep(Duration::from_millis(250)).await;
            drop(ws_stream);
        });

        let restore_result = tokio::time::timeout(
            Duration::from_secs(CONCURRENT_CONNECT_WAIT_TIMEOUT_SECS + 2),
            {
                let client = client.clone();
                async move { client.connect_once().await }
            },
        )
        .await
        .expect("restore connect_once should complete within the concurrent wait window");

        long_connect_task.abort();
        server_task.abort();
        client.disconnect().await.ok();

        assert!(
            restore_result.is_ok(),
            "network restore should not be blocked by an older connect() backoff; got {restore_result:?}"
        );
    }

    #[tokio::test]
    async fn explicit_connect_backoff_reset_restarts_attempt_sequence() {
        let reserved_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should reserve a local port");
        let addr = reserved_listener
            .local_addr()
            .expect("reserved listener should have local addr");
        drop(reserved_listener);

        let mut config = make_config();
        config.server_url =
            Url::parse(&format!("ws://{addr}/signaling/ws")).expect("test URL should parse");
        config.connection_timeout = 1;
        config.reconnect_config = ReconnectConfig {
            enabled: true,
            max_attempts: 10,
            initial_delay: 30,
            max_delay: 30,
            backoff_multiplier: 1.0,
        };
        let client = make_ws_client(config);

        let (attempt_tx, mut attempt_rx) = tokio::sync::mpsc::unbounded_channel();
        let hook_callback: HookCallback = Arc::new(move |event| {
            let attempt_tx = attempt_tx.clone();
            Box::pin(async move {
                if let HookEvent::SignalingConnectStart { attempt } = event {
                    let _ = attempt_tx.send(attempt);
                }
            })
        });
        client.set_hook_callback(hook_callback);

        let connect_task = {
            let client = client.clone();
            tokio::spawn(async move { client.connect().await })
        };

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), attempt_rx.recv())
                .await
                .expect("connect should publish attempt 1"),
            Some(1)
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), attempt_rx.recv())
                .await
                .expect("connect should enter first backoff as attempt 2"),
            Some(2)
        );

        client.schedule_auto_reconnect_reset_backoff();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), attempt_rx.recv())
                .await
                .expect("reset should restart explicit connect attempts"),
            Some(1),
            "network recovery reset should restart explicit connect() backoff from attempt 1"
        );

        connect_task.abort();
        client.disconnect().await.ok();
    }

    #[tokio::test]
    async fn test_publish_disconnected_transition_fires_hook_once() {
        let stats = Arc::new(AtomicSignalingStats::default());
        let (event_tx, mut event_rx) = broadcast::channel(4);
        let hook_count = Arc::new(AtomicUsize::new(0));
        let hook_count_for_cb = hook_count.clone();
        let hook_callback: HookCallback = Arc::new(move |event| {
            let hook_count = hook_count_for_cb.clone();
            Box::pin(async move {
                if matches!(event, HookEvent::SignalingDisconnected) {
                    hook_count.fetch_add(1, UsizeOrdering::SeqCst);
                }
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        });

        let first = WebSocketSignalingClient::publish_disconnected_transition(
            true,
            &stats,
            &event_tx,
            Some(hook_callback.clone()),
            DisconnectReason::StreamEnded,
            None,
        )
        .await;
        assert!(
            first,
            "first connected->disconnected transition should publish"
        );
        assert_eq!(hook_count.load(UsizeOrdering::SeqCst), 1);
        assert_eq!(stats.snapshot().disconnections, 1);
        assert!(matches!(
            event_rx.recv().await,
            Ok(SignalingEvent::Disconnected {
                reason: DisconnectReason::StreamEnded
            })
        ));

        let second = WebSocketSignalingClient::publish_disconnected_transition(
            false,
            &stats,
            &event_tx,
            Some(hook_callback),
            DisconnectReason::PongTimeout,
            None,
        )
        .await;
        assert!(
            !second,
            "stale duplicate disconnected transition should be ignored"
        );
        assert_eq!(hook_count.load(UsizeOrdering::SeqCst), 1);
        assert_eq!(stats.snapshot().disconnections, 1);
        assert!(event_rx.try_recv().is_err());
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1. Configuration defaults
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[test]
    fn test_reconnect_config_defaults() {
        let cfg = ReconnectConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.max_attempts, 10);
        assert_eq!(cfg.initial_delay, 1);
        assert_eq!(cfg.max_delay, 60);
        assert!((cfg.backoff_multiplier - 2.0).abs() < f64::EPSILON);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2. Initial state
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[test]
    fn test_websocket_signaling_client_initial_state_disconnected() {
        let client = WebSocketSignalingClient::new(make_config());
        assert!(
            !client.is_connected(),
            "newly created client should be Disconnected"
        );
        assert!(
            !client.connecting.load(Ordering::Acquire),
            "newly created client should not be in connecting state"
        );
        assert!(
            !client.reconnector_started.load(Ordering::Acquire),
            "reconnect manager should not be started automatically"
        );
    }

    #[test]
    fn test_initial_stats_are_zero() {
        let client = WebSocketSignalingClient::new(make_config());
        let stats = client.get_stats();
        assert_eq!(stats.connections, 0);
        assert_eq!(stats.disconnections, 0);
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.errors, 0);
    }

    #[test]
    fn test_signaling_url_log_redacts_credential_query_params() {
        let url = Url::parse(
            "wss://example.com/signaling?actor_id=abc&key_id=7&claims=claims-value&signature=signature-value&token=token-value",
        )
        .unwrap();

        let redacted = WebSocketSignalingClient::redact_signaling_url_for_log(&url);

        assert!(redacted.contains("actor_id=abc"));
        assert!(redacted.contains("key_id=7"));
        assert!(redacted.contains("claims=REDACTED"));
        assert!(redacted.contains("signature=REDACTED"));
        assert!(redacted.contains("token=REDACTED"));
        assert!(!redacted.contains("claims-value"));
        assert!(!redacted.contains("signature-value"));
        assert!(!redacted.contains("token-value"));
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 3. Reconnect manager idempotency
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_reconnect_manager_idempotent() {
        let client = make_ws_client(make_config());

        // First start should succeed
        client.start_reconnect_manager();
        assert!(
            client.reconnector_started.load(Ordering::Acquire),
            "reconnector_started should be true after first call"
        );

        // Second call should not start a new manager (CAS fails)
        client.start_reconnect_manager();
        // Multiple managers would cause flaky tests due to duplicate reconnections; mainly verify the flag
        assert!(client.reconnector_started.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_reconnect_manager_disabled_when_config_disabled() {
        let mut config = make_config();
        config.reconnect_config.enabled = false;
        let client = make_ws_client(config);

        client.start_reconnect_manager();
        assert!(
            !client.reconnector_started.load(Ordering::Acquire),
            "reconnect manager should not start when reconnect config is disabled"
        );
    }

    #[tokio::test]
    async fn test_reconnect_manager_does_not_keep_client_alive() {
        let client = make_ws_client(make_config());
        let weak = Arc::downgrade(&client);

        client.start_reconnect_manager();
        drop(client);

        assert!(
            weak.upgrade().is_none(),
            "reconnect manager must not keep signaling client alive after owner drop"
        );
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 4. connect() concurrency exclusion
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_connect_fast_path_when_already_connected() {
        let client = make_ws_client(make_config());
        // Manually set as connected
        client.connected.store(true, Ordering::Release);

        // connect() should return Ok immediately without establishing a new connection
        let result = client.connect().await;
        assert!(
            result.is_ok(),
            "connect() should return Ok when already connected"
        );
        // Should not change connecting flag
        assert!(!client.connecting.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_connect_sets_connecting_flag() {
        let mut config = make_config();
        config.reconnect_config.enabled = false; // disable retry, fail fast
        config.connection_timeout = 1;
        let client = make_ws_client(config);

        // Connection will fail (unreachable address), but should properly clean up connecting flag
        let result = client.connect().await;
        assert!(
            result.is_err(),
            "connecting to unreachable address should fail"
        );
        assert!(
            !client.connecting.load(Ordering::Acquire),
            "connecting flag should be cleared after connection failure"
        );
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 5. Event broadcast
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_event_subscribe_receives_events() {
        let client = make_ws_client(make_config());
        let mut rx = client.subscribe_events();

        // Manually send event
        let _ = client.event_tx.send(SignalingEvent::Connected);

        match tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await {
            Ok(Ok(SignalingEvent::Connected)) => {} // expect Connected event
            other => panic!("expected Connected event, but got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_disconnect_event_on_connect_failure() {
        let mut config = make_config();
        config.reconnect_config.enabled = false;
        config.connection_timeout = 1;
        let client = make_ws_client(config);
        let mut rx = client.subscribe_events();

        // Connection fails
        let _ = client.connect().await;

        // Should receive Disconnected(ConnectionFailed) event
        match tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await {
            Ok(Ok(SignalingEvent::Disconnected {
                reason: DisconnectReason::ConnectionFailed(_),
            })) => {} // expected
            other => panic!(
                "expected Disconnected(ConnectionFailed) event, but got {:?}",
                other
            ),
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 6. disconnect() state cleanup
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_disconnect_clears_connected_flag() {
        let client = make_ws_client(make_config());
        // Simulate connected state
        client.connected.store(true, Ordering::Release);
        assert!(client.is_connected());

        let result = client.disconnect().await;
        assert!(result.is_ok());
        assert!(
            !client.is_connected(),
            "should be Disconnected after disconnect()"
        );
    }

    #[tokio::test]
    async fn test_disconnect_increments_disconnection_stat() {
        let client = make_ws_client(make_config());
        client.connected.store(true, Ordering::Release);

        let stats_before = client.get_stats().disconnections;
        let _ = client.disconnect().await;
        let stats_after = client.get_stats().disconnections;
        assert_eq!(
            stats_after,
            stats_before + 1,
            "disconnect() should increment disconnection count"
        );
    }

    #[tokio::test]
    async fn test_disconnect_idempotent() {
        let client = make_ws_client(make_config());

        // Calling disconnect() while not connected should not panic
        let r1 = client.disconnect().await;
        let r2 = client.disconnect().await;
        assert!(r1.is_ok());
        assert!(r2.is_ok());
        assert!(!client.is_connected());
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 7. Reconnect notify mechanism
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_reconnect_notify_wakes_waiter() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();
        let woken = Arc::new(AtomicBool::new(false));
        let woken_clone = woken.clone();

        let handle = tokio::spawn(async move {
            notify_clone.notified().await;
            woken_clone.store(true, Ordering::Release);
        });

        // Ensure waiter has registered
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !woken.load(Ordering::Acquire),
            "should not be woken before notification"
        );

        // Trigger notification
        notify.notify_one();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            woken.load(Ordering::Acquire),
            "should be woken after notification"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn test_schedule_auto_reconnect_reenables_after_explicit_disconnect() {
        let client = make_ws_client(make_config());

        client
            .disconnect()
            .await
            .expect("explicit disconnect should be idempotent");
        assert!(
            client.auto_reconnect_suppressed.load(Ordering::Acquire),
            "explicit disconnect should suppress stale auto-reconnect cycles"
        );

        client.schedule_auto_reconnect();

        assert!(
            !client.auto_reconnect_suppressed.load(Ordering::Acquire),
            "scheduling a fresh auto-reconnect should clear explicit disconnect suppression"
        );
    }

    #[tokio::test]
    async fn test_schedule_auto_reconnect_reset_backoff_restarts_attempt_sequence() {
        let mut config = make_config();
        config.connection_timeout = 1;
        config.reconnect_config = ReconnectConfig {
            enabled: true,
            max_attempts: 5,
            initial_delay: 30,
            max_delay: 30,
            backoff_multiplier: 1.0,
        };
        let client = make_ws_client(config);
        let mut rx = client.subscribe_events();

        let reconnect_client = client.clone();
        let reconnect_task = tokio::spawn(async move {
            reconnect_client.run_reconnect_cycle().await;
        });

        match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Ok(SignalingEvent::ConnectStart { attempt: 1 })) => {}
            other => panic!("expected first reconnect attempt, got {other:?}"),
        }

        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Ok(SignalingEvent::Disconnected {
                reason: DisconnectReason::ConnectionFailed(_),
            })) => {}
            other => panic!("expected first reconnect failure, got {other:?}"),
        }

        client.schedule_auto_reconnect_reset_backoff();

        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Ok(SignalingEvent::ConnectStart { attempt: 1 })) => {}
            other => panic!("expected reset reconnect attempt to restart at 1, got {other:?}"),
        }

        client
            .disconnect()
            .await
            .expect("explicit disconnect should stop reconnect cycle");
        tokio::time::timeout(Duration::from_secs(2), reconnect_task)
            .await
            .expect("reconnect cycle should stop after explicit disconnect")
            .expect("reconnect task should not panic");
    }

    #[tokio::test]
    async fn test_explicit_disconnect_suppresses_reconnect_cycle_in_backoff() {
        let mut config = make_config();
        config.connection_timeout = 1;
        config.reconnect_config = ReconnectConfig {
            enabled: true,
            max_attempts: 5,
            initial_delay: 1,
            max_delay: 1,
            backoff_multiplier: 1.0,
        };
        let client = make_ws_client(config);
        let mut rx = client.subscribe_events();

        let reconnect_client = client.clone();
        let reconnect_task = tokio::spawn(async move {
            reconnect_client.run_reconnect_cycle().await;
        });

        match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Ok(SignalingEvent::ConnectStart { attempt: 1 })) => {}
            other => panic!("expected first reconnect attempt, got {other:?}"),
        }

        client
            .disconnect()
            .await
            .expect("explicit disconnect should be idempotent");

        tokio::time::timeout(Duration::from_secs(2), reconnect_task)
            .await
            .expect("suppressed reconnect cycle should exit promptly")
            .expect("reconnect task should not panic");

        while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
        {
            if let SignalingEvent::ConnectStart { attempt } = event {
                panic!("suppressed reconnect cycle sent unexpected attempt {attempt}");
            }
        }

        assert!(
            client.auto_reconnect_suppressed.load(Ordering::Acquire),
            "explicit disconnect should suppress stale auto-reconnect cycles"
        );
    }

    #[tokio::test]
    async fn test_explicit_disconnect_suppresses_in_flight_auto_reconnect_connected_event() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let server_url = format!(
            "ws://{}/signaling/ws",
            listener
                .local_addr()
                .expect("test listener should have local addr")
        );
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("test server should accept tcp connection");
            let _ = release_rx.await;
            let ws_stream = tokio_tungstenite::accept_async(stream)
                .await
                .expect("test server should complete websocket handshake");
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(ws_stream);
        });

        let mut config = make_config();
        config.server_url = Url::parse(&server_url).expect("test websocket URL should parse");
        config.connection_timeout = 5;
        config.reconnect_config = ReconnectConfig {
            enabled: true,
            max_attempts: 3,
            initial_delay: 1,
            max_delay: 1,
            backoff_multiplier: 1.0,
        };
        let client = make_ws_client(config);
        let mut rx = client.subscribe_events();

        let reconnect_client = client.clone();
        let reconnect_task = tokio::spawn(async move {
            reconnect_client.run_reconnect_cycle().await;
        });

        match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Ok(SignalingEvent::ConnectStart { attempt: 1 })) => {}
            other => panic!("expected first reconnect attempt, got {other:?}"),
        }

        client
            .disconnect()
            .await
            .expect("explicit disconnect should cancel the in-flight auto-reconnect");
        release_tx
            .send(())
            .expect("test server handshake should still be waiting");

        tokio::time::timeout(Duration::from_secs(2), reconnect_task)
            .await
            .expect("cancelled in-flight reconnect should exit promptly")
            .expect("reconnect task should not panic");

        while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await
        {
            assert!(
                !matches!(event, SignalingEvent::Connected),
                "cancelled auto-reconnect must not publish Connected"
            );
        }

        assert!(
            !client.is_connected(),
            "cancelled auto-reconnect must not leave signaling connected"
        );

        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("test server task should finish")
            .expect("test server task should not panic");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 8. URL construction tests
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_build_url_without_identity() {
        let config = make_config();
        let expected_base = config.server_url.to_string();
        let client = WebSocketSignalingClient::new(config);

        let url = client.build_url_with_identity().await;
        assert_eq!(
            url.to_string(),
            expected_base,
            "URL should not contain identity parameters when actor_id is not set"
        );
    }

    #[tokio::test]
    async fn test_build_url_with_webrtc_role() {
        let mut config = make_config();
        config.webrtc_role = Some("answer".to_string());
        let client = WebSocketSignalingClient::new(config);

        let url = client.build_url_with_identity().await;
        assert!(
            url.query().unwrap_or("").contains("webrtc_role=answer"),
            "URL should contain webrtc_role parameter, actual URL: {}",
            url
        );
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 9. Inbound channel reset
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_reset_inbound_channel_creates_fresh_channel() {
        let client = WebSocketSignalingClient::new(make_config());

        // Get old tx and send a message
        {
            let tx = client.inbound_tx.lock().await;
            let _ = tx.send(SignalingEnvelope::default());
        }

        // Reset channel
        client.reset_inbound_channel().await;

        // Old messages should not be visible in the new channel
        let mut rx = client.inbound_rx.lock().await;
        let result = rx.try_recv();
        assert!(
            result.is_err(),
            "old messages should not be visible in the new channel after reset"
        );
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 10. Envelope ID incrementing
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_envelope_id_monotonically_increasing() {
        let client = WebSocketSignalingClient::new(make_config());

        let id1 = client.next_envelope_id().await;
        let id2 = client.next_envelope_id().await;
        let id3 = client.next_envelope_id().await;

        assert_eq!(id1, "env-1");
        assert_eq!(id2, "env-2");
        assert_eq!(id3, "env-3");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 11. send_envelope should return error when not connected
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_send_envelope_fails_when_not_connected() {
        let client = WebSocketSignalingClient::new(make_config());
        let envelope = SignalingEnvelope::default();

        let result = client.send_envelope(envelope).await;
        assert!(
            result.is_err(),
            "send_envelope should return error when not connected"
        );
        match result {
            Err(NetworkError::ConnectionError(msg)) => {
                assert!(
                    msg.contains("not connected") || msg.contains("Not connected"),
                    "error message should contain 'not connected', actual: {}",
                    msg
                );
            }
            other => panic!("expected ConnectionError, got {:?}", other),
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 12. FakeSignalingClient trait implementation verification
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    #[tokio::test]
    async fn test_fake_client_tracks_connect_calls() {
        let client = make_fake_client();
        assert_eq!(client.connect_calls.load(UsizeOrdering::SeqCst), 0);

        client.connect().await.unwrap();
        client.connect().await.unwrap();
        client.connect().await.unwrap();

        assert_eq!(
            client.connect_calls.load(UsizeOrdering::SeqCst),
            3,
            "FakeSignalingClient should accurately track connect call count"
        );
    }
}
