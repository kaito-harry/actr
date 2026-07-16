// WebRTC Signaling Coordinator - Coordinates WebRTC P2P connection establishment

#[allow(dead_code)]
fn is_ipv4_candidate_allowed(cand: &str) -> bool {
    // Only filter out IPv6 candidates (link-local and other IPv6 addresses)
    // Allow all IPv4 candidates (private and public IPs)
    if cand.contains("fe80::") || cand.contains(" udp6 ") || cand.contains("::") {
        return false;
    }

    // Accept all IPv4 candidates by default
    // This includes: loopback (127.x), private (10.x, 172.x, 192.168.x), and public IPs
    true
}

// Responsibilities:
// - Listen to WebRTC signaling messages from SignalingClient
// - Handle Offer/Answer/ICE candidate exchanges
// - Establish and manage RTCPeerConnection instances
// - Create and cache WebRtcConnection instances
// - Aggregate messages from all peers

use super::connection::WebRtcConnection;
use super::negotiator::WebRtcNegotiator;
#[cfg(feature = "opentelemetry")]
use super::trace;
use super::{SignalingClient, WebRtcConfig, WebRtcInboundMessage};
use crate::INITIAL_CONNECTION_TIMEOUT;
use crate::inbound::MediaFrameRegistry;
use crate::lifecycle::CredentialState;
use crate::transport::{ConnectionEvent, ConnectionEventBroadcaster, ConnectionState};
use actr_framework::Bytes;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{
    AIdCredential, ActrId, ActrRelay, IceCandidate, IceRestartRequest, PayloadType, RoleAssignment,
    RoleNegotiation, SignalingEnvelope, actr_relay, session_description::Type as SdpType,
    signaling_envelope,
};
use actr_protocol::{ActorResult, ActrError};
use std::collections::{HashMap, VecDeque, hash_map::Entry};
use std::{
    sync::{
        Arc, RwLock as StdRwLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
#[cfg(feature = "opentelemetry")]
use tracing_opentelemetry::OpenTelemetrySpanExt;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::ice_transport::ice_gathering_state::RTCIceGatheringState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::{RTCPeerConnection, peer_connection_state::RTCPeerConnectionState};
use webrtc::track::track_local::TrackLocalWriter;

const ICE_RESTART_MAX_RETRIES: u32 = 10;
const ICE_RESTART_TIMEOUT: Duration = Duration::from_secs(6);
const ICE_RESTART_INITIAL_BACKOFF_MS: u64 = 1000; // 1s initial
const ICE_RESTART_MAX_BACKOFF_MS: u64 = 5000; // 5s max (1s -> 2s -> 4s -> 5s -> ...)
const ICE_RESTART_MIN_OFFER_INTERVAL: Duration = Duration::from_secs(2);
const ICE_GATHERING_RETRY_INTERVAL: Duration = Duration::from_millis(500);
const ICE_RESTART_MAX_TOTAL_DURATION: Duration = Duration::from_secs(60);
const ICE_GATHERING_TIMEOUT: Duration = Duration::from_secs(10);
const ICE_CONNECTED_TIMEOUT: Duration = Duration::from_secs(10);
const DATA_CHANNEL_AFTER_ICE_TIMEOUT: Duration = Duration::from_secs(2);
const ROLE_NEGOTIATION_TIMEOUT: Duration = Duration::from_secs(5);
const ROLE_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
pub const NETWORK_RECOVERY_TIMEOUT: Duration = Duration::from_secs(6);
const CLEANUP_BARRIER_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_CONNECTION_CLOSE_FALLBACK_TIMEOUT: Duration = Duration::from_millis(500);
const REMOTE_CANDIDATE_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_ALL_HOOK_TIMEOUT: Duration = Duration::from_secs(1);
#[cfg(not(test))]
const CLOSE_ALL_QUIESCE_TIMEOUT: Duration = Duration::from_secs(6);
#[cfg(test)]
const CLOSE_ALL_QUIESCE_TIMEOUT: Duration = Duration::from_millis(500);
const ANSWERER_RECOVERY_STALE_TIMEOUT: Duration = ICE_RESTART_MAX_TOTAL_DURATION;
const CONNECTION_FACTORY_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);
const CONNECTION_FACTORY_MAX_RETRY_DELAY: Duration = Duration::from_secs(10);
const WEBRTC_RPC_INBOUND_QUEUE_DEPTH: usize = 256;
const WEBRTC_RELIABLE_INBOUND_QUEUE_DEPTH: usize = 64;
const WEBRTC_LATENCY_FIRST_INBOUND_QUEUE_DEPTH: usize = 64;
const MAX_PENDING_ICE_CANDIDATES_PER_PEER: usize = 256;
const MAX_KNOWN_REMOTE_ICE_GENERATIONS: usize = 8;

tokio::task_local! {
    /// Identifies the exact close-all flight invoking a lifecycle hook so only
    /// re-entry into that same coordinator flight bypasses its own wait.
    static CLOSE_ALL_HOOK_REENTRY: Arc<CloseAllFlight>;
}

// Health check constants
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(10);
const MAX_FAILED_DURATION: Duration = Duration::from_secs(60); // 1 minute
/// How long a peer may sit in `Disconnected` before the health check reaps it.
///
/// Deliberately larger than the ICE restart budget
/// (`ICE_RESTART_MAX_TOTAL_DURATION` = 60s, measured from the restart trigger,
/// which is at or after the transition into Disconnected): a legitimate
/// recovery either succeeds — leaving Disconnected and resetting
/// `last_state_change` — or exhausts its budget and self-cleans within 60s of
/// the transition. Anything still Disconnected past 90s (budget + margin) is
/// unrecoverable (answerer-side peers never run a restart task; offerer-side
/// tasks can be aborted by `clear_pending_restarts`) and would otherwise pin
/// its RTCPeerConnection — and the ICE UDP sockets it holds — forever.
const MAX_DISCONNECTED_DURATION: Duration = Duration::from_secs(90);

/// Decide whether the health check should reap a peer, given its real-time
/// connection state and how long it has been in that state.
///
/// - `Failed`/`Closed` are terminal and reaped after `MAX_FAILED_DURATION`.
/// - `Disconnected` is reaped after `MAX_DISCONNECTED_DURATION` (90s), which
///   strictly exceeds the ICE restart budget — see the constant's docs.
///   The `ice_restart_inflight` flag is deliberately NOT consulted here: it is
///   a per-attempt flag (false between backoff attempts and always false on
///   the answerer side), so it cannot distinguish "recovering" from "stuck";
///   worse, a flag stuck at `true` (e.g. a restart task aborted after setting
///   it) would suppress reaping forever and recreate the very leak this check
///   exists to stop. The generous time threshold is the actual guard.
///
/// Returns the cleanup reason, or `None` if the peer should be kept.
fn stale_peer_reap_reason(
    current_state: RTCPeerConnectionState,
    duration_since_change: Duration,
) -> Option<String> {
    let threshold = match current_state {
        RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed => MAX_FAILED_DURATION,
        RTCPeerConnectionState::Disconnected => MAX_DISCONNECTED_DURATION,
        _ => return None,
    };
    (duration_since_change > threshold).then(|| {
        format!(
            "{:?} for {}s",
            current_state,
            duration_since_change.as_secs()
        )
    })
}

fn nonempty_candidate_ufrag(candidate: &IceCandidate) -> Option<&str> {
    candidate
        .username_fragment
        .as_deref()
        .filter(|ufrag| !ufrag.is_empty())
}

fn ice_ufrag_from_description(
    description: &RTCSessionDescription,
    sdp_mid: Option<&str>,
    sdp_mline_index: Option<u32>,
) -> Option<String> {
    let parsed = description.unmarshal().ok()?;

    // RFC 8839 section 5.4: a media-level ICE credential overrides the
    // session-level value for that media section.
    let media = sdp_mid
        .filter(|mid| !mid.is_empty())
        .and_then(|mid| {
            parsed.media_descriptions.iter().find(|media| {
                media
                    .attribute("mid")
                    .flatten()
                    .is_some_and(|media_mid| media_mid == mid)
            })
        })
        .or_else(|| {
            sdp_mline_index
                .and_then(|index| usize::try_from(index).ok())
                .and_then(|index| parsed.media_descriptions.get(index))
        });
    if let Some(ufrag) = media
        .and_then(|media| media.attribute("ice-ufrag").flatten())
        .filter(|ufrag| !ufrag.is_empty())
    {
        return Some(ufrag.to_owned());
    }

    if let Some(ufrag) = parsed
        .attribute("ice-ufrag")
        .filter(|ufrag| !ufrag.is_empty())
    {
        return Some(ufrag.clone());
    }

    let mut unique_ufrag = None;
    for ufrag in parsed.media_descriptions.iter().filter_map(|media| {
        media
            .attribute("ice-ufrag")
            .flatten()
            .filter(|ufrag| !ufrag.is_empty())
    }) {
        match unique_ufrag.as_deref() {
            Some(existing) if existing != ufrag => return None,
            Some(_) => {}
            None => unique_ufrag = Some(ufrag.to_owned()),
        }
    }

    unique_ufrag
}

fn ice_ufrags_from_description(description: &RTCSessionDescription) -> Vec<String> {
    let Ok(parsed) = description.unmarshal() else {
        return Vec::new();
    };

    let mut ufrags = Vec::new();
    let mut remember = |ufrag: &str| {
        if !ufrag.is_empty() && !ufrags.iter().any(|known| known == ufrag) {
            ufrags.push(ufrag.to_owned());
        }
    };

    if let Some(ufrag) = parsed.attribute("ice-ufrag") {
        remember(ufrag);
    }
    for ufrag in parsed.media_descriptions.iter().filter_map(|media| {
        media
            .attribute("ice-ufrag")
            .flatten()
            .filter(|ufrag| !ufrag.is_empty())
    }) {
        remember(ufrag);
    }

    ufrags
}

fn candidate_matches_description(
    candidate: &IceCandidate,
    description: &RTCSessionDescription,
) -> bool {
    let Some(candidate_ufrag) = nonempty_candidate_ufrag(candidate) else {
        return false;
    };
    let Some(description_ufrag) = ice_ufrag_from_description(
        description,
        candidate.sdp_mid.as_deref(),
        candidate.sdp_mline_index,
    ) else {
        return false;
    };

    candidate_ufrag == description_ufrag
}

/// Per-peer negotiation state (role, ready signals)
/// Consolidates multiple related fields into a single lock to reduce contention.
#[derive(Default)]
struct PeerNegotiationState {
    /// Shared in-flight role arbitration for this peer.
    role_flight: Option<RoleNegotiationFlight>,
    /// Ready notifier for answerer path
    ready_tx: Option<oneshot::Sender<()>>,
    /// Ready receiver for proactive offerer path
    ready_rx: Option<oneshot::Receiver<()>>,
    /// Whether remote peer has fixed network configuration
    remote_fixed: bool,
}

struct RoleNegotiationFlight {
    lifecycle_epoch: u64,
    result_tx: watch::Sender<Option<ActorResult<bool>>>,
}

use actr_framework::{ExponentialBackoff, WebRtcPeerStatus};

/// Type alias for message receivers (from all peers, split by traffic class).
type MessageRx = Arc<Mutex<mpsc::Receiver<WebRtcInboundMessage>>>;
type RestartSignalingGate = Arc<Mutex<()>>;
type RestartSignalingGates = Arc<Mutex<HashMap<ActrId, Weak<Mutex<()>>>>>;

#[derive(Default)]
enum LocalIceGenerationState {
    #[default]
    Idle,
    Buffering(Vec<RTCIceCandidateInit>),
    Suppressed,
}

#[derive(Default)]
struct PeerIceSignalingState {
    pending_local_sdp_exchange_id: Option<String>,
    local_generation: LocalIceGenerationState,
    known_remote_ufrags: VecDeque<String>,
}

impl PeerIceSignalingState {
    fn begin_local_generation(&mut self) {
        self.local_generation = LocalIceGenerationState::Buffering(Vec::new());
    }

    fn suppress_local_generation(&mut self) {
        self.local_generation = LocalIceGenerationState::Suppressed;
    }

    fn clear_pending_restart(&mut self) {
        self.pending_local_sdp_exchange_id = None;
        if matches!(self.local_generation, LocalIceGenerationState::Buffering(_)) {
            self.suppress_local_generation();
        }
    }

    fn remember_remote_ufrag(&mut self, ufrag: String) {
        self.known_remote_ufrags.retain(|known| known != &ufrag);
        self.known_remote_ufrags.push_back(ufrag);
        while self.known_remote_ufrags.len() > MAX_KNOWN_REMOTE_ICE_GENERATIONS {
            self.known_remote_ufrags.pop_front();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalCandidateDisposition {
    SendNow,
    Buffered,
    Suppressed,
    StaleSession,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteCandidateDisposition {
    Apply,
    DropStale,
    BufferFuture,
}

fn classify_remote_candidate_ufrag(
    candidate_ufrag: &str,
    current_ufrag: &str,
    known: &VecDeque<String>,
) -> RemoteCandidateDisposition {
    if candidate_ufrag == current_ufrag {
        RemoteCandidateDisposition::Apply
    } else if known.iter().any(|ufrag| ufrag == candidate_ufrag) {
        RemoteCandidateDisposition::DropStale
    } else {
        RemoteCandidateDisposition::BufferFuture
    }
}

struct PeerSignalingCommitState {
    gates: RestartSignalingGates,
    lifecycle_gate: Mutex<()>,
    closing_all: AtomicBool,
    peer_lifecycle_epoch: AtomicU64,
    restart_cancellation_epoch: AtomicU64,
    close_all_generation: AtomicU64,
    close_all_flight: Mutex<Option<Arc<CloseAllFlight>>>,
}

#[derive(Debug)]
struct CloseAllFlight {
    generation: u64,
    result_tx: watch::Sender<Option<ActorResult<()>>>,
}

impl CloseAllFlight {
    fn new(generation: u64) -> Self {
        let (result_tx, _result_rx) = watch::channel(None);
        Self {
            generation,
            result_tx,
        }
    }

    async fn wait(&self) -> ActorResult<()> {
        let mut result_rx = self.result_tx.subscribe();
        loop {
            if let Some(result) = result_rx.borrow().clone() {
                return result;
            }
            result_rx.changed().await.map_err(|_| {
                ActrError::Internal("WebRTC close-all result channel closed".to_string())
            })?;
        }
    }
}

impl Default for PeerSignalingCommitState {
    fn default() -> Self {
        Self {
            gates: Arc::new(Mutex::new(HashMap::new())),
            lifecycle_gate: Mutex::new(()),
            closing_all: AtomicBool::new(false),
            peer_lifecycle_epoch: AtomicU64::new(0),
            restart_cancellation_epoch: AtomicU64::new(0),
            close_all_generation: AtomicU64::new(0),
            close_all_flight: Mutex::new(None),
        }
    }
}

#[derive(Default)]
struct CoordinatorBackgroundTasks {
    lifecycle_gate: Mutex<()>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

#[derive(Clone)]
struct PeerSignalingCommitContext {
    peers: Arc<RwLock<HashMap<ActrId, PeerState>>>,
    state: Arc<PeerSignalingCommitState>,
}

struct PeerSignalingCommitGuard {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerCloseMode {
    Graceful,
    Immediate,
}

impl PeerSignalingCommitContext {
    async fn gate_for(&self, peer_id: &ActrId) -> RestartSignalingGate {
        WebRtcCoordinator::restart_signaling_gate_for_map(&self.state.gates, peer_id).await
    }

    async fn acquire_commit(
        &self,
        peer_id: &ActrId,
        session_id: u64,
        restart_epoch: Option<u64>,
    ) -> Option<PeerSignalingCommitGuard> {
        if self.state.closing_all.load(Ordering::Acquire) {
            return None;
        }

        let guard = self.gate_for(peer_id).await.lock_owned().await;
        if self.state.closing_all.load(Ordering::Acquire)
            || restart_epoch.is_some_and(|epoch| {
                self.state
                    .restart_cancellation_epoch
                    .load(Ordering::Acquire)
                    != epoch
            })
            || self
                .peers
                .read()
                .await
                .get(peer_id)
                .is_none_or(|peer| peer.session_id != session_id)
        {
            return None;
        }

        Some(PeerSignalingCommitGuard { _guard: guard })
    }

    async fn acquire_pre_session_commit(
        &self,
        peer_id: &ActrId,
        lifecycle_epoch: u64,
    ) -> Option<PeerSignalingCommitGuard> {
        if self.state.closing_all.load(Ordering::Acquire)
            || self.state.peer_lifecycle_epoch.load(Ordering::Acquire) != lifecycle_epoch
        {
            return None;
        }

        let guard = self.gate_for(peer_id).await.lock_owned().await;
        if self.state.closing_all.load(Ordering::Acquire)
            || self.state.peer_lifecycle_epoch.load(Ordering::Acquire) != lifecycle_epoch
        {
            return None;
        }

        Some(PeerSignalingCommitGuard { _guard: guard })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicRtcHookState {
    Unknown,
    Idle,
    Connecting,
    Connected,
    Recovering,
}

/// Peer connection state
struct PeerState {
    /// RTCPeerConnection (for receiving ICE candidates)
    peer_connection: Arc<RTCPeerConnection>,

    /// WebRtcConnection (for business message transmission)
    webrtc_conn: WebRtcConnection,

    /// Connection ready notification (for initiate_connection to wait)
    ready_tx: Option<oneshot::Sender<()>>,

    /// Whether we are the offerer for the current session (affects ICE restart handling)
    is_offerer: bool,

    /// Session-scoped SDP and ICE generation state.
    ice_signaling: PeerIceSignalingState,

    /// Whether ICE restart is in progress (controls buffering and retries)
    ice_restart_inflight: bool,

    /// Restart attempts counter (resets on success)
    ice_restart_attempts: u32,

    /// In-flight Offerer restart or Answerer restart-notification task handle.
    /// Both variants share lifecycle cancellation and de-duplication.
    restart_task_handle: Option<JoinHandle<()>>,

    /// Used to wake up the backoff sleep in `do_ice_restart_inner`
    /// when external events (NetworkEvent::Available, IceRestartRequest) indicate
    /// that the network may have recovered. Notify is idempotent — multiple
    /// calls to `notify_one()` are safe and won't cause duplicate restarts.
    restart_wake: Arc<tokio::sync::Notify>,

    /// Wake an in-flight ICE restart task while it is sleeping before the next
    /// offer attempt, without interrupting an offer that is already awaiting
    /// completion.
    restart_retry_wake: Arc<tokio::sync::Notify>,

    /// Last time we sent an ICE restart offer for this peer.
    last_ice_restart_offer_at: Option<Instant>,

    /// Last state change timestamp (for health check)
    last_state_change: std::time::Instant,

    /// Current connection state (for health check)
    current_state: RTCPeerConnectionState,

    /// Whether this session has ever reached ICE/DTLS Connected.
    ever_ice_connected: bool,

    /// Whether this session has ever had an open DataChannel.
    ever_data_channel_opened: bool,

    /// Whether `on_webrtc_connected` has been emitted for the current
    /// sendable window.
    sendable_hook_reported: bool,

    /// Whether `on_webrtc_disconnected` has been emitted for the current
    /// unavailable/recovery window.
    unavailable_hook_reported: bool,

    /// Last WebRTC state exposed through workload hooks for this session.
    public_hook_state: PublicRtcHookState,

    /// Session ID for this connection (matches WebRtcConnection.session_id())
    session_id: u64,

    /// Receive loop JoinHandles (one per PayloadType, aborted during cleanup)
    receive_handles: Vec<JoinHandle<()>>,
}

impl PeerState {
    fn update_connection_state(&mut self, state: RTCPeerConnectionState) {
        self.current_state = state;
        self.last_state_change = std::time::Instant::now();
        if matches!(state, RTCPeerConnectionState::Connected) {
            self.ever_ice_connected = true;
        }
    }

    fn mark_data_channel_opened(&mut self) {
        self.ever_data_channel_opened = true;
    }

    fn mark_sendable_hook_reported(&mut self) {
        self.sendable_hook_reported = true;
        self.unavailable_hook_reported = false;
        self.public_hook_state = PublicRtcHookState::Connected;
    }

    fn mark_connecting_hook_reported(&mut self) {
        self.sendable_hook_reported = false;
        self.unavailable_hook_reported = false;
        self.public_hook_state = PublicRtcHookState::Connecting;
    }

    fn mark_sendable_transition_pending(&mut self) {
        self.sendable_hook_reported = false;
    }

    fn mark_unavailable_hook_reported(&mut self) {
        self.sendable_hook_reported = false;
        self.unavailable_hook_reported = true;
        self.public_hook_state = PublicRtcHookState::Recovering;
    }

    fn mark_public_idle_hook_reported(&mut self) {
        self.sendable_hook_reported = false;
        self.unavailable_hook_reported = true;
        self.public_hook_state = PublicRtcHookState::Idle;
    }

    fn is_network_recovery_eligible(&self) -> bool {
        self.ever_ice_connected || self.ever_data_channel_opened
    }
}

enum IceRestartWaitOutcome {
    Completed,
    TimedOut,
    Woken,
}

#[derive(Clone, Debug)]
pub struct NetworkRecoveryStatus {
    pub session_id: u64,
    pub started_at: Instant,
    pub reason: String,
}

type RecoveryStatusTarget = (ActrId, NetworkRecoveryStatus);
type RestartRetryWakeTarget = (ActrId, u64, Arc<tokio::sync::Notify>);
type NetworkRecoveryRestartPlan = (
    Vec<RecoveryStatusTarget>,
    Vec<RestartRetryWakeTarget>,
    Vec<RecoveryStatusTarget>,
    Vec<ActrId>,
);

impl NetworkRecoveryStatus {
    pub(crate) fn new(session_id: u64, reason: impl Into<String>) -> Self {
        Self {
            session_id,
            started_at: Instant::now(),
            reason: reason.into(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn elapsed_ms(&self) -> u128 {
        self.elapsed().as_millis()
    }

    pub fn is_timed_out(&self) -> bool {
        self.elapsed() >= NETWORK_RECOVERY_TIMEOUT
    }
}

/// WebRTC signaling coordinator
pub struct WebRtcCoordinator {
    /// Local Actor ID
    local_id: Arc<StdRwLock<ActrId>>,

    /// Local credentials
    credential_state: CredentialState,

    /// SignalingClient (for sending ICE/SDP)
    signaling_client: Arc<dyn SignalingClient>,

    /// WebRTC negotiator
    negotiator: WebRtcNegotiator,

    /// Peer state mapping (ActrId → PeerState)
    peers: Arc<RwLock<HashMap<ActrId, PeerState>>>,

    /// Pending ICE candidates (received before remote description is set).
    /// Full metadata is retained so stale ICE generations can be discarded.
    pending_candidates: Arc<RwLock<HashMap<ActrId, Vec<IceCandidate>>>>,

    /// RPC receive channel (aggregated from all peers)
    /// Format: (sender_id_bytes, message_data, payload_type)
    rpc_message_rx: MessageRx,
    rpc_message_tx: mpsc::Sender<WebRtcInboundMessage>,

    /// Reliable stream receive channel (aggregated from all peers).
    /// Split from RPC and from LatencyFirst so StreamReliable backpressure can
    /// starve neither RPC delivery nor LatencyFirst drop-newest handling.
    reliable_message_rx: MessageRx,
    reliable_message_tx: mpsc::Sender<WebRtcInboundMessage>,

    /// LatencyFirst stream receive channel (aggregated from all peers).
    /// Isolated from Reliable so a backpressured reliable stream cannot stall
    /// LatencyFirst chunks upstream of the registry's drop-newest policy.
    latency_first_message_rx: MessageRx,
    latency_first_message_tx: mpsc::Sender<WebRtcInboundMessage>,

    /// MediaTrack callback registry (for WebRTC native media streams)
    media_frame_registry: Arc<MediaFrameRegistry>,

    /// Per-peer negotiation state (role, ready signals, restart tasks)
    /// Single lock consolidating pending_role, pending_ready, pending_ready_wait, and in_flight_restarts
    peer_negotiation: Arc<Mutex<HashMap<ActrId, PeerNegotiationState>>>,

    /// Connection event broadcaster for notifying all layers
    event_broadcaster: ConnectionEventBroadcaster,

    /// Peers that have entered network recovery before WebRTC reports a final state.
    ///
    /// The stored session id prevents a late event from an old peer connection
    /// from clearing the recovery guard for a newer session. `started_at`
    /// bounds how long senders may fail fast with "Connection recovering".
    network_recovering_peers: Arc<RwLock<HashMap<ActrId, NetworkRecoveryStatus>>>,

    /// Hook callback for synchronous lifecycle notification (set once, shared with connections)
    hook_callback: std::sync::OnceLock<crate::wire::webrtc::HookCallback>,

    /// Active foreground/manual cleanup depth. Outbound sends wait for this to
    /// reach zero before starting a fresh WebRTC negotiation.
    cleanup_depth: Arc<AtomicUsize>,
    cleanup_notify: Arc<tokio::sync::Notify>,

    /// Peer-scoped signaling serialization and full-shutdown lifecycle state.
    peer_signaling: Arc<PeerSignalingCommitState>,

    /// Long-running tasks created by `start`, owned for deterministic shutdown.
    background_tasks: CoordinatorBackgroundTasks,

    /// Root tracing contexts for connection initiation (ActrId → Context)
    #[cfg(feature = "opentelemetry")]
    root_context_map: Arc<RwLock<HashMap<ActrId, opentelemetry::Context>>>,
}

/// RAII guard that keeps outbound sends behind the cleanup barrier until drop.
pub struct CleanupGuard {
    coordinator: Arc<WebRtcCoordinator>,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        self.coordinator.finish_cleanup();
    }
}

struct CloseAllStateGuard {
    state: Arc<PeerSignalingCommitState>,
    flight: Arc<CloseAllFlight>,
    completed: bool,
}

enum CloseAllEntry {
    Leader(CloseAllStateGuard),
    Follower(Arc<CloseAllFlight>),
}

impl CloseAllStateGuard {
    async fn enter(state: Arc<PeerSignalingCommitState>) -> CloseAllEntry {
        // `lifecycle_gate` serializes callers of this method. Lock the flight
        // slot before flipping `closing_all` so cancellation cannot expose an
        // active state without a joinable per-generation result channel.
        let mut active_flight = state.close_all_flight.lock().await;
        if state
            .closing_all
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return CloseAllEntry::Follower(
                active_flight
                    .as_ref()
                    .expect("active close-all must publish its flight before releasing lifecycle")
                    .clone(),
            );
        }
        state.peer_lifecycle_epoch.fetch_add(1, Ordering::AcqRel);
        state
            .restart_cancellation_epoch
            .fetch_add(1, Ordering::AcqRel);
        let generation = state.close_all_generation.fetch_add(1, Ordering::AcqRel) + 1;
        let flight = Arc::new(CloseAllFlight::new(generation));
        *active_flight = Some(Arc::clone(&flight));
        drop(active_flight);
        CloseAllEntry::Leader(Self {
            state,
            flight,
            completed: false,
        })
    }

    fn publish(&self, result: ActorResult<()>) {
        // Each generation owns a dedicated channel, so reopening first cannot
        // let a newer flight overwrite this result. Followers of this flight
        // only return after the shutdown admission flag has been released.
        self.state.closing_all.store(false, Ordering::Release);
        self.flight.result_tx.send_replace(Some(result));
    }

    fn flight(&self) -> Arc<CloseAllFlight> {
        Arc::clone(&self.flight)
    }

    fn complete(mut self, result: ActorResult<()>) {
        self.publish(result);
        self.completed = true;
    }
}

impl Drop for CloseAllStateGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.publish(Err(ActrError::Unavailable(
                "WebRTC close-all operation was cancelled".to_string(),
            )));
        }
    }
}

/// Owns peer states after the close-all state commit.
///
/// If the caller is cancelled while running a lifecycle hook, `Drop` moves all
/// remaining states into hook-free teardown tasks. This keeps physical cleanup
/// cancellation-safe without allowing an old session's delayed Idle hook to
/// arrive after a replacement session has connected.
struct DrainedPeerCleanupGuard {
    peers: Vec<(ActrId, PeerState)>,
    network_recovering_peers: Arc<RwLock<HashMap<ActrId, NetworkRecoveryStatus>>>,
}

impl DrainedPeerCleanupGuard {
    fn new(
        peers: Vec<(ActrId, PeerState)>,
        network_recovering_peers: Arc<RwLock<HashMap<ActrId, NetworkRecoveryStatus>>>,
    ) -> Self {
        Self {
            peers,
            network_recovering_peers,
        }
    }

    fn last(&self) -> Option<&(ActrId, PeerState)> {
        self.peers.last()
    }

    fn pop(&mut self) -> Option<(ActrId, PeerState)> {
        self.peers.pop()
    }
}

impl Drop for DrainedPeerCleanupGuard {
    fn drop(&mut self) {
        let peers = std::mem::take(&mut self.peers);
        if peers.is_empty() {
            return;
        }

        let network_recovering_peers = Arc::clone(&self.network_recovering_peers);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            for (_, state) in peers {
                for handle in &state.receive_handles {
                    handle.abort();
                }
            }
            tracing::error!("Cannot finish cancelled WebRTC peer teardown without a Tokio runtime");
            return;
        };

        for (peer_id, state) in peers {
            let network_recovering_peers = Arc::clone(&network_recovering_peers);
            runtime.spawn(async move {
                WebRtcCoordinator::teardown_removed_peer_state_with(
                    &network_recovering_peers,
                    None,
                    &peer_id,
                    state,
                    false,
                    PeerCloseMode::Immediate,
                    "cancelled close all peers",
                )
                .await;
            });
        }
    }
}

impl WebRtcCoordinator {
    /// Create new coordinator
    pub fn new(
        local_id: ActrId,
        credential_state: CredentialState,
        signaling_client: Arc<dyn SignalingClient>,
        webrtc_config: WebRtcConfig,
        media_frame_registry: Arc<MediaFrameRegistry>,
    ) -> Self {
        let (rpc_message_tx, rpc_message_rx) = mpsc::channel(WEBRTC_RPC_INBOUND_QUEUE_DEPTH);
        let (reliable_message_tx, reliable_message_rx) =
            mpsc::channel(WEBRTC_RELIABLE_INBOUND_QUEUE_DEPTH);
        let (latency_first_message_tx, latency_first_message_rx) =
            mpsc::channel(WEBRTC_LATENCY_FIRST_INBOUND_QUEUE_DEPTH);
        let negotiator = WebRtcNegotiator::new(webrtc_config, credential_state.clone());

        Self {
            local_id: Arc::new(StdRwLock::new(local_id)),
            credential_state,
            signaling_client,
            negotiator,
            peers: Arc::new(RwLock::new(HashMap::new())),
            pending_candidates: Arc::new(RwLock::new(HashMap::new())),
            rpc_message_rx: Arc::new(Mutex::new(rpc_message_rx)),
            rpc_message_tx,
            reliable_message_rx: Arc::new(Mutex::new(reliable_message_rx)),
            reliable_message_tx,
            latency_first_message_rx: Arc::new(Mutex::new(latency_first_message_rx)),
            latency_first_message_tx,
            media_frame_registry,
            peer_negotiation: Arc::new(Mutex::new(HashMap::new())),
            event_broadcaster: ConnectionEventBroadcaster::new(),
            network_recovering_peers: Arc::new(RwLock::new(HashMap::new())),
            hook_callback: std::sync::OnceLock::new(),
            cleanup_depth: Arc::new(AtomicUsize::new(0)),
            cleanup_notify: Arc::new(tokio::sync::Notify::new()),
            peer_signaling: Arc::new(PeerSignalingCommitState::default()),
            background_tasks: CoordinatorBackgroundTasks::default(),
            #[cfg(feature = "opentelemetry")]
            root_context_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn message_tx_for_payload(
        payload_type: PayloadType,
        rpc_message_tx: &mpsc::Sender<WebRtcInboundMessage>,
        reliable_message_tx: &mpsc::Sender<WebRtcInboundMessage>,
        latency_first_message_tx: &mpsc::Sender<WebRtcInboundMessage>,
    ) -> Option<mpsc::Sender<WebRtcInboundMessage>> {
        match payload_type {
            PayloadType::RpcReliable | PayloadType::RpcSignal => Some(rpc_message_tx.clone()),
            PayloadType::StreamReliable => Some(reliable_message_tx.clone()),
            PayloadType::StreamLatencyFirst => Some(latency_first_message_tx.clone()),
            PayloadType::MediaRtp => None,
        }
    }

    fn local_id_snapshot(&self) -> ActrId {
        self.local_id
            .read()
            .expect("WebRtcCoordinator local_id lock poisoned")
            .clone()
    }

    /// Update the local Actor ID after AIS re-registration assigns a new identity.
    pub async fn set_local_id(&self, actor_id: ActrId) {
        *self
            .local_id
            .write()
            .expect("WebRtcCoordinator local_id lock poisoned") = actor_id;
    }

    #[cfg(test)]
    pub(crate) fn local_id_for_test(&self) -> ActrId {
        self.local_id_snapshot()
    }

    /// Enter a cleanup window. While any guard is alive, outbound sends wait
    /// before starting new WebRTC negotiation.
    pub fn cleanup_guard(self: &Arc<Self>) -> CleanupGuard {
        let depth = self.cleanup_depth.fetch_add(1, Ordering::AcqRel) + 1;
        tracing::debug!("🚧 WebRTC cleanup barrier entered, depth={}", depth);
        CleanupGuard {
            coordinator: Arc::clone(self),
        }
    }

    /// Wait for any active cleanup window to finish. This is intentionally
    /// best-effort so a leaked or misused guard cannot permanently block sends.
    pub async fn wait_cleanup_complete(&self) {
        let wait = async {
            loop {
                let notified = self.cleanup_notify.notified();
                if self.cleanup_depth.load(Ordering::Acquire) == 0 {
                    return;
                }
                notified.await;
            }
        };

        if tokio::time::timeout(CLEANUP_BARRIER_WAIT_TIMEOUT, wait)
            .await
            .is_err()
        {
            tracing::warn!(
                "⏱️ WebRTC cleanup barrier wait timed out after {:?}; continuing outbound send",
                CLEANUP_BARRIER_WAIT_TIMEOUT
            );
        }
    }

    fn finish_cleanup(&self) {
        match self
            .cleanup_depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |depth| {
                (depth > 0).then_some(depth - 1)
            }) {
            Ok(1) => {
                tracing::debug!("✅ WebRTC cleanup barrier released");
                self.cleanup_notify.notify_waiters();
            }
            Ok(depth) => {
                tracing::debug!("↘️ WebRTC cleanup barrier depth decreased to {}", depth - 1);
            }
            Err(_) => {
                tracing::warn!("⚠️ WebRTC cleanup barrier release requested at depth=0");
                self.cleanup_notify.notify_waiters();
            }
        }
    }

    async fn serialize_local_ice_candidate(
        peer_connection: &RTCPeerConnection,
        candidate: &RTCIceCandidate,
    ) -> Result<IceCandidate, String> {
        let candidate_json = candidate
            .to_json()
            .map_err(|err| format!("ICE Candidate serialization failed: {err}"))?;
        let local_description = peer_connection
            .local_description()
            .await
            .ok_or_else(|| "local description is missing for gathered ICE candidate".to_string())?;
        let username_fragment = ice_ufrag_from_description(
            &local_description,
            candidate_json.sdp_mid.as_deref(),
            candidate_json.sdp_mline_index.map(u32::from),
        )
        .ok_or_else(|| "local SDP does not contain an unambiguous ICE ufrag".to_string())?;

        Ok(Self::ice_candidate_from_json(
            candidate_json,
            username_fragment,
        ))
    }

    fn ice_candidate_from_json(
        candidate_json: RTCIceCandidateInit,
        username_fragment: String,
    ) -> IceCandidate {
        IceCandidate {
            candidate: candidate_json.candidate,
            sdp_mid: candidate_json
                .sdp_mid
                .filter(|sdp_mid| !sdp_mid.trim().is_empty()),
            sdp_mline_index: candidate_json.sdp_mline_index.map(u32::from),
            username_fragment: Some(username_fragment),
        }
    }

    fn peer_signaling_commit_context(&self) -> PeerSignalingCommitContext {
        PeerSignalingCommitContext {
            peers: Arc::clone(&self.peers),
            state: Arc::clone(&self.peer_signaling),
        }
    }

    async fn local_candidate_disposition(
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
        peer_id: &ActrId,
        session_id: u64,
        candidate_json: RTCIceCandidateInit,
    ) -> LocalCandidateDisposition {
        let mut peers = peers.write().await;
        let Some(state) = peers.get_mut(peer_id) else {
            return LocalCandidateDisposition::StaleSession;
        };
        if state.session_id != session_id {
            return LocalCandidateDisposition::StaleSession;
        }

        match &mut state.ice_signaling.local_generation {
            LocalIceGenerationState::Idle => LocalCandidateDisposition::SendNow,
            LocalIceGenerationState::Suppressed => LocalCandidateDisposition::Suppressed,
            LocalIceGenerationState::Buffering(candidates) => {
                if candidates.len() >= MAX_PENDING_ICE_CANDIDATES_PER_PEER {
                    candidates.remove(0);
                    tracing::warn!(
                        "⚠️ Local ICE candidate buffer full for {}; dropping oldest candidate",
                        peer_id
                    );
                }
                candidates.push(candidate_json);
                LocalCandidateDisposition::Buffered
            }
        }
    }

    async fn classify_local_candidate(
        &self,
        peer_id: &ActrId,
        session_id: u64,
        candidate: &RTCIceCandidate,
    ) -> Result<LocalCandidateDisposition, String> {
        let candidate_json = candidate
            .to_json()
            .map_err(|err| format!("ICE Candidate serialization failed: {err}"))?;
        Ok(
            Self::local_candidate_disposition(&self.peers, peer_id, session_id, candidate_json)
                .await,
        )
    }

    async fn begin_local_ice_generation(
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
        peer_id: &ActrId,
        session_id: u64,
    ) -> bool {
        let mut peers = peers.write().await;
        let Some(state) = peers.get_mut(peer_id) else {
            return false;
        };
        if state.session_id != session_id {
            return false;
        }

        state.ice_signaling.begin_local_generation();
        true
    }

    async fn suppress_local_ice_generation(
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
        peer_id: &ActrId,
        session_id: u64,
    ) {
        let mut peers = peers.write().await;
        if let Some(state) = peers.get_mut(peer_id)
            && state.session_id == session_id
        {
            state.ice_signaling.suppress_local_generation();
        }
    }

    async fn finish_local_ice_generation(
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
        peer_id: &ActrId,
        session_id: u64,
        description: &RTCSessionDescription,
    ) -> Result<Vec<IceCandidate>, String> {
        let mut peers = peers.write().await;
        let Some(state) = peers.get_mut(peer_id) else {
            return Ok(Vec::new());
        };
        if state.session_id != session_id {
            return Ok(Vec::new());
        }

        let candidates = match std::mem::take(&mut state.ice_signaling.local_generation) {
            LocalIceGenerationState::Buffering(candidates) => candidates,
            LocalIceGenerationState::Idle | LocalIceGenerationState::Suppressed => {
                return Ok(Vec::new());
            }
        };
        let mut prepared = Vec::with_capacity(candidates.len());
        for candidate_json in candidates {
            let Some(username_fragment) = ice_ufrag_from_description(
                description,
                candidate_json.sdp_mid.as_deref(),
                candidate_json.sdp_mline_index.map(u32::from),
            ) else {
                tracing::warn!(
                    peer_id = %peer_id,
                    session_id,
                    sdp_mid = ?candidate_json.sdp_mid,
                    sdp_mline_index = ?candidate_json.sdp_mline_index,
                    "Skipping buffered local ICE candidate because the restart SDP has no unambiguous ICE ufrag"
                );
                continue;
            };
            prepared.push(Self::ice_candidate_from_json(
                candidate_json,
                username_fragment,
            ));
        }
        Ok(prepared)
    }

    async fn restart_signaling_gate_for(&self, peer_id: &ActrId) -> RestartSignalingGate {
        self.peer_signaling_commit_context().gate_for(peer_id).await
    }

    async fn restart_signaling_gate_for_map(
        gates: &RestartSignalingGates,
        peer_id: &ActrId,
    ) -> RestartSignalingGate {
        let mut gates = gates.lock().await;
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(peer_id).and_then(Weak::upgrade) {
            return gate;
        }

        let gate = Arc::new(Mutex::new(()));
        gates.insert(peer_id.clone(), Arc::downgrade(&gate));
        gate
    }

    async fn prune_restart_signaling_gates(gates: &RestartSignalingGates) {
        gates.lock().await.retain(|_, gate| gate.strong_count() > 0);
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_prepared_local_ice_candidates(
        commit_context: &PeerSignalingCommitContext,
        signaling_client: &Arc<dyn SignalingClient>,
        local_id: &ActrId,
        credential_state: &CredentialState,
        target: &ActrId,
        session_id: u64,
        restart_epoch: Option<u64>,
        candidates: Vec<IceCandidate>,
    ) {
        if candidates.is_empty() {
            return;
        }

        let credential = credential_state.credential().await;
        for candidate in candidates {
            let envelope = Self::build_actr_relay_envelope(
                local_id.clone(),
                credential.clone(),
                target,
                actr_relay::Payload::IceCandidate(candidate),
            );
            let Some(commit_guard) = commit_context
                .acquire_commit(target, session_id, restart_epoch)
                .await
            else {
                tracing::debug!(
                    peer_id = %target,
                    session_id,
                    "Stopping buffered local ICE candidate commits for a stale peer session"
                );
                break;
            };
            if let Err(err) = Self::send_peer_signaling_envelope_while_guarded(
                &commit_guard,
                signaling_client,
                envelope,
            )
            .await
            {
                tracing::warn!(
                    "⚠️ Failed to send buffered local ICE candidate to {}: {}",
                    target,
                    err
                );
                drop(commit_guard);
                break;
            }
            // A batch may contain hundreds of candidates. Release the peer
            // gate after every bounded send so close-all can quiesce the peer
            // instead of waiting for the entire batch.
            drop(commit_guard);
        }
        WebRtcCoordinator::prune_restart_signaling_gates(&commit_context.state.gates).await;
    }

    /// Get a subscriber for connection events
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<ConnectionEvent> {
        self.event_broadcaster.subscribe()
    }

    /// Set the hook callback (once). Shared with all new connections.
    pub fn set_hook_callback(&self, cb: crate::wire::webrtc::HookCallback) {
        let _ = self.hook_callback.set(cb);
    }

    async fn invoke_hook_callback(
        hook_callback: Option<&crate::wire::webrtc::HookCallback>,
        event: crate::wire::webrtc::HookEvent,
    ) {
        if let Some(cb) = hook_callback {
            cb(event).await;
        }
    }

    async fn invoke_hook(&self, event: crate::wire::webrtc::HookEvent) {
        Self::invoke_hook_callback(self.hook_callback.get(), event).await;
    }

    pub(crate) async fn notify_data_chunk_delivery_uncertain(
        &self,
        stream_id: String,
        session_id: u64,
        reason: String,
    ) {
        self.invoke_hook(crate::wire::webrtc::HookEvent::DataChunkDeliveryUncertain {
            stream_id,
            session_id,
            reason,
        })
        .await;
    }

    async fn selected_pair_is_relayed(peer_connection: &Arc<RTCPeerConnection>) -> bool {
        let sctp = peer_connection.sctp();
        let dtls = sctp.transport();
        let ice = dtls.ice_transport();
        ice.get_selected_candidate_pair()
            .await
            .map(|pair| pair.to_string().contains("relay"))
            .unwrap_or(false)
    }

    async fn notify_webrtc_connected_if_sendable(
        &self,
        peer_id: &ActrId,
        session_id: u64,
        reason: &str,
    ) {
        let (peer_connection, webrtc_conn) = {
            let peers = self.peers.read().await;
            let Some(state) = peers.get(peer_id) else {
                return;
            };
            if state.session_id != session_id
                || state.sendable_hook_reported
                || state.ice_restart_inflight
                || state.current_state != RTCPeerConnectionState::Connected
            {
                return;
            }
            (state.peer_connection.clone(), state.webrtc_conn.clone())
        };

        if !webrtc_conn.has_open_data_channel().await {
            tracing::debug!(
                peer_id = ?peer_id,
                session_id = session_id,
                reason = reason,
                "PeerConnection is connected but no DataChannel is open; not emitting ready hook"
            );
            return;
        }

        let relayed = Self::selected_pair_is_relayed(&peer_connection).await;

        let should_notify = {
            let mut peers = self.peers.write().await;
            let Some(state) = peers.get_mut(peer_id) else {
                return;
            };
            if state.session_id != session_id
                || state.sendable_hook_reported
                || state.ice_restart_inflight
                || state.current_state != RTCPeerConnectionState::Connected
            {
                false
            } else {
                state.mark_sendable_hook_reported();
                true
            }
        };

        if should_notify {
            tracing::info!(
                peer_id = ?peer_id,
                session_id = session_id,
                relayed = relayed,
                reason = reason,
                "WebRTC peer is business-sendable; emitting connected hook"
            );
            self.invoke_hook(crate::wire::webrtc::HookEvent::WebRtcConnected {
                peer_id: peer_id.clone(),
                relayed,
            })
            .await;
        }
    }

    async fn notify_webrtc_connecting_if_new_session(
        &self,
        peer_id: &ActrId,
        session_id: u64,
        reason: &str,
    ) {
        let should_notify = {
            let mut peers = self.peers.write().await;
            let Some(state) = peers.get_mut(peer_id) else {
                return;
            };
            if state.session_id != session_id
                || state.public_hook_state == PublicRtcHookState::Connecting
                || state.ice_restart_inflight
                || state.unavailable_hook_reported
                || state.ever_ice_connected
                || state.ever_data_channel_opened
            {
                false
            } else {
                state.mark_connecting_hook_reported();
                true
            }
        };

        if should_notify {
            tracing::info!(
                peer_id = ?peer_id,
                session_id = session_id,
                reason = reason,
                "WebRTC peer is establishing a new business connection; emitting connecting hook"
            );
            self.invoke_hook(crate::wire::webrtc::HookEvent::WebRtcConnectStart {
                peer_id: peer_id.clone(),
            })
            .await;
        }
    }

    async fn notify_webrtc_status_if_changed(
        &self,
        peer_id: &ActrId,
        session_id: u64,
        status: WebRtcPeerStatus,
        reason: &str,
    ) {
        let next_public_state = match status {
            WebRtcPeerStatus::Idle => PublicRtcHookState::Idle,
            WebRtcPeerStatus::Connecting => PublicRtcHookState::Connecting,
            WebRtcPeerStatus::Connected => PublicRtcHookState::Connected,
            WebRtcPeerStatus::Recovering => PublicRtcHookState::Recovering,
        };
        let should_notify = {
            let mut peers = self.peers.write().await;
            let Some(state) = peers.get_mut(peer_id) else {
                return;
            };
            if state.session_id != session_id || state.public_hook_state == next_public_state {
                false
            } else {
                match status {
                    WebRtcPeerStatus::Idle => state.mark_public_idle_hook_reported(),
                    WebRtcPeerStatus::Recovering => state.mark_unavailable_hook_reported(),
                    WebRtcPeerStatus::Connecting => state.mark_connecting_hook_reported(),
                    WebRtcPeerStatus::Connected => state.mark_sendable_hook_reported(),
                }
                true
            }
        };

        if should_notify {
            tracing::info!(
                peer_id = ?peer_id,
                session_id = session_id,
                status = ?status,
                reason = reason,
                "WebRTC peer public status changed; emitting hook"
            );
            match status {
                WebRtcPeerStatus::Idle | WebRtcPeerStatus::Recovering => {
                    self.invoke_hook(crate::wire::webrtc::HookEvent::WebRtcDisconnected {
                        peer_id: peer_id.clone(),
                        status,
                    })
                    .await;
                }
                WebRtcPeerStatus::Connecting => {
                    self.invoke_hook(crate::wire::webrtc::HookEvent::WebRtcConnectStart {
                        peer_id: peer_id.clone(),
                    })
                    .await;
                }
                WebRtcPeerStatus::Connected => {
                    tracing::warn!(
                        peer_id = ?peer_id,
                        session_id = session_id,
                        reason = reason,
                        "Ignoring generic connected status emission; use notify_webrtc_connected_if_sendable"
                    );
                }
            }
        }
    }

    async fn notify_webrtc_recovering_once(&self, peer_id: &ActrId, session_id: u64, reason: &str) {
        self.notify_webrtc_status_if_changed(
            peer_id,
            session_id,
            WebRtcPeerStatus::Recovering,
            reason,
        )
        .await;
    }

    async fn notify_webrtc_idle_if_changed(&self, peer_id: &ActrId, session_id: u64, reason: &str) {
        self.notify_webrtc_status_if_changed(peer_id, session_id, WebRtcPeerStatus::Idle, reason)
            .await;
    }

    async fn notify_removed_peer_idle_if_needed(
        hook_callback: Option<&crate::wire::webrtc::HookCallback>,
        peer_id: &ActrId,
        session_id: u64,
        state: &PeerState,
        reason: &str,
    ) {
        if hook_callback.is_none()
            || matches!(
                state.public_hook_state,
                PublicRtcHookState::Unknown | PublicRtcHookState::Idle
            )
        {
            return;
        }

        tracing::info!(
            peer_id = ?peer_id,
            session_id = session_id,
            previous_status = ?state.public_hook_state,
            reason = reason,
            "WebRTC peer cleanup reached terminal idle; emitting hook"
        );
        Self::invoke_hook_callback(
            hook_callback,
            crate::wire::webrtc::HookEvent::WebRtcDisconnected {
                peer_id: peer_id.clone(),
                status: WebRtcPeerStatus::Idle,
            },
        )
        .await;
    }

    async fn teardown_removed_peer_state_with(
        network_recovering_peers: &Arc<RwLock<HashMap<ActrId, NetworkRecoveryStatus>>>,
        hook_callback: Option<&crate::wire::webrtc::HookCallback>,
        target: &ActrId,
        mut state: PeerState,
        abort_restart_task: bool,
        close_mode: PeerCloseMode,
        reason: &str,
    ) {
        let session_id = state.session_id;

        if abort_restart_task {
            if let Some(handle) = state.restart_task_handle.take() {
                handle.abort();
                tracing::debug!(
                    "🛑 Aborted restart task for serial={}, session_id={}, reason={}",
                    target,
                    session_id,
                    reason
                );
                if let Err(err) = handle.await
                    && !err.is_cancelled()
                {
                    tracing::warn!(
                        "⚠️ Restart task join failed for serial={}, session_id={}, reason={}: {}",
                        target,
                        session_id,
                        reason,
                        err
                    );
                }
            }
        }

        for handle in &state.receive_handles {
            handle.abort();
        }
        if !state.receive_handles.is_empty() {
            tracing::debug!(
                "🛑 Aborted {} receive loops for serial={}, session_id={}, reason={}",
                state.receive_handles.len(),
                target,
                session_id,
                reason
            );
        }

        Self::clear_peer_recovering_in(network_recovering_peers, target, session_id, reason).await;
        Self::notify_removed_peer_idle_if_needed(hook_callback, target, session_id, &state, reason)
            .await;

        let close_result = match close_mode {
            PeerCloseMode::Graceful => state.webrtc_conn.close().await,
            PeerCloseMode::Immediate => state.webrtc_conn.close_immediately().await,
        };
        if let Err(e) = close_result {
            tracing::warn!(
                "⚠️ Failed to close webrtc_conn during cleanup for {} (session_id={}, reason={}): {}",
                target,
                session_id,
                reason,
                e
            );
            match tokio::time::timeout(
                PEER_CONNECTION_CLOSE_FALLBACK_TIMEOUT,
                state.peer_connection.close(),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        "⚠️ Failed to close peer_connection during cleanup for {} (session_id={}, reason={}): {}",
                        target,
                        session_id,
                        reason,
                        e
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        "⚠️ Timed out closing peer_connection during cleanup for {} (session_id={}, reason={})",
                        target,
                        session_id,
                        reason
                    );
                }
            }
        }
    }

    async fn teardown_removed_peer_state(
        &self,
        target: &ActrId,
        state: PeerState,
        abort_restart_task: bool,
        reason: &str,
    ) {
        // Transfer ownership before the first suspension point. If the caller
        // is timed out or aborted while waiting, dropping the JoinHandle only
        // detaches this task; the removed connection and its receive/restart
        // tasks are still closed to completion.
        let network_recovering_peers = Arc::clone(&self.network_recovering_peers);
        let hook_callback = self.hook_callback.get().cloned();
        let target = target.clone();
        let reason = reason.to_owned();
        let teardown_task = tokio::spawn(async move {
            Self::teardown_removed_peer_state_with(
                &network_recovering_peers,
                hook_callback.as_ref(),
                &target,
                state,
                abort_restart_task,
                PeerCloseMode::Immediate,
                &reason,
            )
            .await;
        });
        if let Err(err) = teardown_task.await {
            tracing::error!(error = %err, "WebRTC peer teardown task failed");
        }
    }

    /// Inject a virtual network for integration testing.
    ///
    /// **Must be called before `start()`** — all subsequently created
    /// RTCPeerConnections will use this VNet instead of real OS networking.
    ///
    /// # Example
    /// ```rust,ignore
    /// let vnet_pair = VNetPair::new().await?;
    /// coordinator.set_vnet(vnet_pair.net_offerer.clone());
    /// coordinator.start().await?;
    /// ```
    #[cfg(feature = "test-utils")]
    pub fn set_vnet(&mut self, vnet: std::sync::Arc<webrtc::util::vnet::net::Net>) {
        self.negotiator.set_vnet(vnet);
    }

    /// Close a cached WebRTC DataChannel for integration tests.
    ///
    /// This keeps production APIs from exposing WebRTC internals while allowing
    /// regression tests to trigger the real `RTCDataChannel::on_close` path.
    #[cfg(feature = "test-utils")]
    pub async fn close_data_channel_for_test(
        &self,
        peer_id: &ActrId,
        payload_type: PayloadType,
    ) -> ActorResult<u64> {
        let idx = payload_type as usize;
        if idx >= 4 {
            return Err(ActrError::Internal(format!(
                "PayloadType does not use a WebRTC DataChannel: {payload_type:?}"
            )));
        }

        let (session_id, webrtc_conn) = {
            let peers = self.peers.read().await;
            let state = peers.get(peer_id).ok_or_else(|| {
                ActrError::Internal(format!(
                    "Peer connection not found for test close: {peer_id}"
                ))
            })?;
            (state.session_id, state.webrtc_conn.clone())
        };

        let channels = webrtc_conn.data_channels().await;
        let channel = channels
            .get(idx)
            .and_then(Clone::clone)
            .ok_or_else(|| {
                ActrError::Internal(format!(
                    "DataChannel not found for test close: peer={peer_id}, payload_type={payload_type:?}"
                ))
            })?;

        channel
            .close()
            .await
            .map_err(|e| ActrError::Internal(format!("Failed to close DataChannel: {e}")))?;

        Ok(session_id)
    }

    /// Check whether the current WebRTC connection to a peer still has an open DataChannel.
    #[cfg(feature = "test-utils")]
    pub async fn has_open_data_channel_for_test(&self, peer_id: &ActrId) -> ActorResult<bool> {
        let webrtc_conn = {
            let peers = self.peers.read().await;
            peers.get(peer_id).map(|state| state.webrtc_conn.clone())
        };
        let Some(webrtc_conn) = webrtc_conn else {
            return Ok(false);
        };

        Ok(webrtc_conn.has_open_data_channel().await)
    }

    /// Get the event sender for sharing with WebRtcConnection instances
    pub fn event_sender(&self) -> tokio::sync::broadcast::Sender<ConnectionEvent> {
        self.event_broadcaster.sender()
    }

    /// Get the session_id for a specific peer's current connection (if any)
    pub async fn get_peer_session_id(&self, peer_id: &ActrId) -> Option<u64> {
        let peers = self.peers.read().await;
        peers.get(peer_id).map(|state| state.session_id)
    }

    fn should_retrigger_existing_recovery(existing_reason: &str, new_reason: &str) -> bool {
        existing_reason == "NetworkLost" && new_reason != "NetworkLost"
    }

    /// Mark all active peers as recovering as soon as the platform reports a
    /// network restore/change. This is intentionally earlier than WebRTC state
    /// callbacks, which may lag behind the real network switch.
    pub async fn begin_network_recovery(&self, reason: &str) -> Vec<ActrId> {
        let peers: Vec<(ActrId, u64)> = {
            let peers = self.peers.read().await;
            peers
                .iter()
                .filter_map(|(peer_id, state)| {
                    if state.is_network_recovery_eligible() {
                        Some((peer_id.clone(), state.session_id))
                    } else {
                        tracing::debug!(
                            peer_id = ?peer_id,
                            session_id = state.session_id,
                            current_state = ?state.current_state,
                            "⏭️ Skipping network recovery for never-ready session"
                        );
                        None
                    }
                })
                .collect()
        };

        if peers.is_empty() {
            return Vec::new();
        }

        let mut newly_marked = Vec::new();
        {
            let mut recovering = self.network_recovering_peers.write().await;
            for (peer_id, session_id) in &peers {
                match recovering.entry(peer_id.clone()) {
                    Entry::Occupied(mut entry) if entry.get().session_id == *session_id => {
                        if Self::should_retrigger_existing_recovery(
                            entry.get().reason.as_str(),
                            reason,
                        ) {
                            tracing::debug!(
                                "🚧 Peer {} already in network recovery from {}, session_id={}, elapsed_ms={}; retriggering for {}",
                                peer_id,
                                entry.get().reason.as_str(),
                                session_id,
                                entry.get().elapsed_ms(),
                                reason
                            );
                            entry.get_mut().reason = reason.to_string();
                            newly_marked.push((peer_id.clone(), *session_id));
                            continue;
                        }
                        tracing::debug!(
                            "🚧 Peer {} already in network recovery, session_id={}, elapsed_ms={}, reason={}",
                            peer_id,
                            session_id,
                            entry.get().elapsed_ms(),
                            entry.get().reason.as_str()
                        );
                    }
                    Entry::Occupied(mut entry) => {
                        entry.insert(NetworkRecoveryStatus::new(*session_id, reason));
                        newly_marked.push((peer_id.clone(), *session_id));
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(NetworkRecoveryStatus::new(*session_id, reason));
                        newly_marked.push((peer_id.clone(), *session_id));
                    }
                }
            }
        }

        for (peer_id, session_id) in &newly_marked {
            tracing::debug!(
                "🚧 Marking peer {} as network recovering, session_id={}, reason={}",
                peer_id,
                session_id,
                reason
            );
            self.event_broadcaster
                .send(ConnectionEvent::IceRestartStarted {
                    peer_id: peer_id.clone(),
                    session_id: *session_id,
                });
        }

        newly_marked
            .into_iter()
            .map(|(peer_id, _)| peer_id)
            .collect()
    }

    /// Check whether a peer is in the recovery window.
    pub async fn is_peer_recovering(&self, peer_id: &ActrId) -> bool {
        self.peer_recovery_status(peer_id).await.is_some()
    }

    /// Return the guarded recovery session for diagnostics.
    pub async fn peer_recovery_session(&self, peer_id: &ActrId) -> Option<u64> {
        self.peer_recovery_status(peer_id)
            .await
            .map(|status| status.session_id)
    }

    /// Return the guarded recovery status for diagnostics and send preflight.
    pub async fn peer_recovery_status(&self, peer_id: &ActrId) -> Option<NetworkRecoveryStatus> {
        let status = {
            let recovering = self.network_recovering_peers.read().await;
            recovering.get(peer_id).cloned()
        };

        let status = status?;

        let is_current_session = {
            let peers = self.peers.read().await;
            peers
                .get(peer_id)
                .map(|state| state.session_id == status.session_id)
                .unwrap_or(false)
        };

        if !is_current_session {
            let mut recovering = self.network_recovering_peers.write().await;
            if recovering
                .get(peer_id)
                .map(|current| current.session_id == status.session_id)
                .unwrap_or(false)
            {
                recovering.remove(peer_id);
            }
            return None;
        }

        Some(status)
    }

    async fn peer_sendable_session(&self, peer_id: &ActrId) -> Option<u64> {
        let (session_id, ice_restart_inflight, peer_connection, webrtc_conn) = {
            let peers = self.peers.read().await;
            let state = peers.get(peer_id)?;
            (
                state.session_id,
                state.ice_restart_inflight,
                state.peer_connection.clone(),
                state.webrtc_conn.clone(),
            )
        };

        if ice_restart_inflight
            || peer_connection.connection_state() != RTCPeerConnectionState::Connected
        {
            return None;
        }

        if webrtc_conn.has_open_data_channel().await {
            Some(session_id)
        } else {
            None
        }
    }

    pub(crate) async fn is_peer_sendable_session(&self, peer_id: &ActrId, session_id: u64) -> bool {
        self.peer_sendable_session(peer_id)
            .await
            .is_some_and(|current_session_id| current_session_id == session_id)
    }

    /// Wait until the current session for `peer_id` has an open DataChannel and
    /// no ICE restart is in flight.
    pub async fn wait_for_peer_sendable(&self, peer_id: &ActrId, timeout: Duration) -> Option<u64> {
        let mut event_rx = self.event_broadcaster.subscribe();
        if let Some(session_id) = self.peer_sendable_session(peer_id).await {
            return Some(session_id);
        }

        let target_peer = peer_id.clone();
        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                _ = &mut sleep => return None,
                res = event_rx.recv() => {
                    match res {
                        Ok(ConnectionEvent::StateChanged {
                            peer_id,
                            state: ConnectionState::Connected,
                            ..
                        })
                        | Ok(ConnectionEvent::DataChannelOpened { peer_id, .. })
                        | Ok(ConnectionEvent::IceRestartCompleted {
                            peer_id,
                            success: true,
                            ..
                        }) if peer_id == target_peer => {
                            if let Some(session_id) = self.peer_sendable_session(&peer_id).await {
                                return Some(session_id);
                            }
                        }
                        Ok(_) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("Peer sendable wait lagged by {} events", n);
                            if let Some(session_id) = self.peer_sendable_session(&target_peer).await {
                                return Some(session_id);
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }
        }
    }

    pub async fn expire_peer_recovery(
        &self,
        peer_id: &ActrId,
        session_id: u64,
        reason: &str,
    ) -> bool {
        let mut recovering = self.network_recovering_peers.write().await;
        let status = recovering.get(peer_id).cloned();
        let should_remove = status
            .as_ref()
            .map(|current| current.session_id == session_id)
            .unwrap_or(false);

        if should_remove {
            recovering.remove(peer_id);
            if let Some(status) = status {
                tracing::warn!(
                    peer_id = ?peer_id,
                    session_id = session_id,
                    elapsed_ms = status.elapsed_ms(),
                    recovery_reason = status.reason.as_str(),
                    expire_reason = reason,
                    "⏱️ Peer network recovery guard expired"
                );
            }
            true
        } else {
            false
        }
    }

    pub async fn close_recovering_peer(
        &self,
        peer_id: &ActrId,
        session_id: u64,
        reason: &str,
    ) -> bool {
        self.expire_peer_recovery(peer_id, session_id, reason).await;
        self.cleanup_connection_if_session(peer_id, session_id, true, reason)
            .await
    }

    #[cfg(feature = "test-utils")]
    pub async fn force_peer_recovery_started_at_for_test(
        &self,
        peer_id: &ActrId,
        started_at: Instant,
    ) -> bool {
        let mut recovering = self.network_recovering_peers.write().await;
        if let Some(status) = recovering.get_mut(peer_id) {
            status.started_at = started_at;
            true
        } else {
            false
        }
    }

    #[cfg(feature = "test-utils")]
    pub async fn peer_session_id_for_test(&self, peer_id: &ActrId) -> Option<u64> {
        self.peers
            .read()
            .await
            .get(peer_id)
            .map(|state| state.webrtc_conn.session_id())
    }

    async fn mark_peer_recovering(&self, peer_id: &ActrId, session_id: u64, reason: &str) {
        let mut should_notify = false;
        {
            let mut recovering = self.network_recovering_peers.write().await;
            match recovering.entry(peer_id.clone()) {
                Entry::Occupied(entry) if entry.get().session_id == session_id => {
                    tracing::debug!(
                        peer_id = ?peer_id,
                        session_id = session_id,
                        elapsed_ms = entry.get().elapsed_ms(),
                        recovery_reason = entry.get().reason.as_str(),
                        "🚧 Peer already in network recovery"
                    );
                }
                Entry::Occupied(mut entry) => {
                    entry.insert(NetworkRecoveryStatus::new(session_id, reason));
                    should_notify = true;
                }
                Entry::Vacant(entry) => {
                    entry.insert(NetworkRecoveryStatus::new(session_id, reason));
                    should_notify = true;
                }
            }
        }
        if should_notify {
            self.event_broadcaster
                .send(ConnectionEvent::IceRestartStarted {
                    peer_id: peer_id.clone(),
                    session_id,
                });
        }
    }

    async fn clear_peer_recovering_in(
        network_recovering_peers: &Arc<RwLock<HashMap<ActrId, NetworkRecoveryStatus>>>,
        peer_id: &ActrId,
        session_id: u64,
        reason: &str,
    ) {
        let mut recovering = network_recovering_peers.write().await;
        let should_clear = recovering
            .get(peer_id)
            .map(|status| status.session_id == session_id)
            .unwrap_or(false);
        if should_clear {
            let status = recovering.remove(peer_id);
            tracing::debug!(
                peer_id = ?peer_id,
                session_id = session_id,
                elapsed_ms = status.as_ref().map(|status| status.elapsed_ms()).unwrap_or(0),
                reason = reason,
                "✅ Peer left network recovery"
            );
        }
    }

    async fn clear_peer_recovering(&self, peer_id: &ActrId, session_id: u64, reason: &str) {
        Self::clear_peer_recovering_in(&self.network_recovering_peers, peer_id, session_id, reason)
            .await;
    }

    async fn clear_peer_recovering_if_sendable(
        &self,
        peer_id: &ActrId,
        session_id: u64,
        reason: &str,
    ) {
        if self.is_peer_sendable_session(peer_id, session_id).await {
            self.clear_peer_recovering(peer_id, session_id, reason)
                .await;
            self.notify_webrtc_connected_if_sendable(peer_id, session_id, reason)
                .await;
        } else {
            tracing::debug!(
                peer_id = ?peer_id,
                session_id = session_id,
                reason = reason,
                "Peer is not sendable yet; keeping network recovery guard"
            );
        }
    }

    /// Trigger ICE restart for peers currently guarded by a network recovery event.
    ///
    /// This is deliberately broader than `retry_failed_connections()`: mobile
    /// platforms can report a network switch before WebRTC has moved from
    /// `Connected` to `Disconnected`, so the local offerer must proactively
    /// restart ICE instead of waiting for a delayed state callback.
    pub async fn restart_network_recovery_connections(self: &Arc<Self>) {
        self.restart_network_recovery_connections_matching(None)
            .await;
    }

    pub async fn restart_network_recovery_connections_for(
        self: &Arc<Self>,
        target_peer_ids: &[ActrId],
    ) {
        if target_peer_ids.is_empty() {
            return;
        }
        self.restart_network_recovery_connections_matching(Some(target_peer_ids))
            .await;
    }

    async fn restart_network_recovery_connections_matching(
        self: &Arc<Self>,
        target_filter: Option<&[ActrId]>,
    ) {
        let (stale_answerers, wake_targets, ineligible_guards, targets): NetworkRecoveryRestartPlan = {
            let recovery_snapshot: Vec<RecoveryStatusTarget> = self
                .network_recovering_peers
                .read()
                .await
                .iter()
                .map(|(peer_id, status)| (peer_id.clone(), status.clone()))
                .collect();

            if recovery_snapshot.is_empty() {
                return;
            }

            let peers = self.peers.read().await;
            let mut stale_answerers = Vec::new();
            let mut wake_targets = Vec::new();
            let mut ineligible_guards = Vec::new();
            let mut targets = Vec::new();

            for (peer_id, recovery_status) in recovery_snapshot.iter() {
                if let Some(target_filter) = target_filter {
                    if !target_filter.iter().any(|target| target == peer_id) {
                        continue;
                    }
                }

                let Some(state) = peers.get(peer_id) else {
                    continue;
                };
                let session_matches = state.session_id == recovery_status.session_id;
                if !session_matches {
                    continue;
                }

                if !state.is_network_recovery_eligible() {
                    tracing::debug!(
                        peer_id = ?peer_id,
                        session_id = recovery_status.session_id,
                        recovery_reason = recovery_status.reason.as_str(),
                        current_state = ?state.current_state,
                        "⏭️ Clearing network recovery guard for never-ready session"
                    );
                    ineligible_guards.push((peer_id.clone(), recovery_status.clone()));
                    continue;
                }

                if !state.is_offerer && recovery_status.elapsed() >= ANSWERER_RECOVERY_STALE_TIMEOUT
                {
                    stale_answerers.push((peer_id.clone(), recovery_status.clone()));
                    continue;
                }

                let restart_task_running = state
                    .restart_task_handle
                    .as_ref()
                    .map(|handle| !handle.is_finished())
                    .unwrap_or(false);

                if restart_task_running {
                    wake_targets.push((
                        peer_id.clone(),
                        recovery_status.session_id,
                        state.restart_retry_wake.clone(),
                    ));
                } else if !state.ice_restart_inflight {
                    targets.push(peer_id.clone());
                } else {
                    tracing::debug!(
                        peer_id = ?peer_id,
                        session_id = recovery_status.session_id,
                        "🚧 ICE restart is marked in-flight without a running retry task; not starting a duplicate restart"
                    );
                }
            }

            (stale_answerers, wake_targets, ineligible_guards, targets)
        };

        for (target, recovery_status) in ineligible_guards {
            self.clear_peer_recovering(
                &target,
                recovery_status.session_id,
                "never-ready session is not eligible for network recovery",
            )
            .await;
        }

        for (target, recovery_status) in stale_answerers {
            tracing::warn!(
                peer_id = ?target,
                session_id = recovery_status.session_id,
                elapsed_ms = recovery_status.elapsed_ms(),
                stale_timeout_ms = ANSWERER_RECOVERY_STALE_TIMEOUT.as_millis(),
                recovery_reason = recovery_status.reason.as_str(),
                "⏱️ Answerer recovery is stale; closing old session before fresh connection"
            );
            self.close_recovering_peer(
                &target,
                recovery_status.session_id,
                "answerer long network recovery timeout",
            )
            .await;
        }

        for (target, session_id, restart_retry_wake) in wake_targets {
            tracing::info!(
                "🔔 Waking existing ICE restart retry for network recovery peer {}, session_id={}",
                target,
                session_id
            );
            restart_retry_wake.notify_one();
        }

        for target in targets {
            tracing::info!("♻️ Restarting ICE for network recovery peer {}", target);
            if let Err(e) = self.restart_ice(&target).await {
                tracing::warn!("⚠️ Failed to restart ICE for {}: {}", target, e);
            }
        }
    }

    /// Trigger ICE restart for all connections in Failed/Disconnected state
    pub async fn retry_failed_connections(self: &Arc<Self>) {
        let peers = self.peers.read().await;
        // Collect peers that need restart to avoid holding lock during async operations
        let mut targets = Vec::new();

        for (peer_id, state) in peers.iter() {
            match state.current_state {
                RTCPeerConnectionState::Failed | RTCPeerConnectionState::Disconnected
                    if !state.ice_restart_inflight =>
                {
                    targets.push(peer_id.clone());
                }
                _ => {
                    // Only restart non-failed/disconnected connections in test mode
                    // Note: Use feature flag instead of #[cfg(test)] to work with integration tests
                    #[cfg(feature = "test-utils")]
                    {
                        tracing::debug!(
                            "Actor {:?} is in state {:?}, test restart (test-utils feature enabled)",
                            peer_id,
                            state.current_state
                        );
                        targets.push(peer_id.clone());
                    }
                }
            }
        }
        drop(peers); // Release lock

        for peer_id in targets {
            tracing::info!("♻️ Auto-retrying failed connection to actor {:?}", peer_id);
            if let Err(e) = self.restart_ice(&peer_id).await {
                tracing::error!("❌ Failed to restart ICE for {:?}: {}", peer_id, e);
            }
        }
    }

    /// Clear pending ICE restart attempts (called on network loss)
    pub async fn clear_pending_restarts(&self) {
        self.peer_signaling
            .restart_cancellation_epoch
            .fetch_add(1, Ordering::AcqRel);
        let restart_handles = {
            let mut peers = self.peers.write().await;
            let mut restart_handles = Vec::new();

            for (peer_id, state) in peers.iter_mut() {
                let handle = state.restart_task_handle.take();
                if state.ice_restart_inflight || handle.is_some() {
                    tracing::info!("🛑 Aborting pending ICE restart for {:?}", peer_id);
                    if let Some(handle) = handle {
                        handle.abort();
                        restart_handles.push((peer_id.clone(), handle));
                    }
                    state.ice_restart_inflight = false;
                    state.ice_restart_attempts = 0;
                    state.ice_signaling.pending_local_sdp_exchange_id = None;
                }
                state.ice_signaling.clear_pending_restart();
            }

            restart_handles
        };

        for (peer_id, handle) in restart_handles {
            if let Err(err) = handle.await
                && !err.is_cancelled()
            {
                tracing::warn!("⚠️ ICE restart task join failed for {:?}: {}", peer_id, err);
            }
        }
    }

    /// Start internal event listener for handling connection close events
    ///
    /// This listens for ConnectionClosed and DataChannelClosed events and triggers
    /// cleanup of WebRtcCoordinator's internal resources (peers map, pending candidates, etc.)
    fn spawn_internal_event_listener(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let mut event_rx = self.event_broadcaster.subscribe();
        let coordinator = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                match event_rx.recv().await {
                    Ok(event) => {
                        if let Some(coord) = coordinator.upgrade() {
                            match &event {
                                ConnectionEvent::StateChanged {
                                    peer_id,
                                    session_id,
                                    state: ConnectionState::Connecting,
                                    ..
                                } => {
                                    let mut peers = coord.peers.write().await;
                                    if let Some(state) = peers.get_mut(peer_id)
                                        && state.session_id == *session_id
                                    {
                                        state.update_connection_state(
                                            RTCPeerConnectionState::Connecting,
                                        );
                                        state.mark_sendable_transition_pending();
                                    }
                                    drop(peers);
                                    coord
                                        .notify_webrtc_connecting_if_new_session(
                                            peer_id,
                                            *session_id,
                                            "peer connection connecting",
                                        )
                                        .await;
                                }
                                ConnectionEvent::StateChanged {
                                    peer_id,
                                    session_id,
                                    state: ConnectionState::Connected,
                                    ..
                                } => {
                                    {
                                        let mut peers = coord.peers.write().await;
                                        if let Some(state) = peers.get_mut(peer_id)
                                            && state.session_id == *session_id
                                        {
                                            state.update_connection_state(
                                                RTCPeerConnectionState::Connected,
                                            );
                                        }
                                    }
                                    coord
                                        .clear_peer_recovering_if_sendable(
                                            peer_id,
                                            *session_id,
                                            "peer connection connected",
                                        )
                                        .await;
                                }
                                ConnectionEvent::StateChanged {
                                    peer_id,
                                    session_id,
                                    state:
                                        state
                                        @ (ConnectionState::Disconnected | ConnectionState::Failed),
                                    ..
                                } => {
                                    let recovery_eligible = {
                                        let mut peers = coord.peers.write().await;
                                        if let Some(peer_state) = peers.get_mut(peer_id)
                                            && peer_state.session_id == *session_id
                                        {
                                            let rtc_state = match state {
                                                ConnectionState::Disconnected => {
                                                    RTCPeerConnectionState::Disconnected
                                                }
                                                ConnectionState::Failed => {
                                                    RTCPeerConnectionState::Failed
                                                }
                                                _ => unreachable!(
                                                    "state pattern only matches unavailable states"
                                                ),
                                            };
                                            peer_state.update_connection_state(rtc_state);
                                            Some(peer_state.is_network_recovery_eligible())
                                        } else {
                                            None
                                        }
                                    };
                                    let reason = match state {
                                        ConnectionState::Disconnected => "peer state Disconnected",
                                        ConnectionState::Failed => "peer state Failed",
                                        _ => unreachable!(
                                            "state pattern only matches unavailable states"
                                        ),
                                    };
                                    match recovery_eligible {
                                        Some(true) => {
                                            coord
                                                .notify_webrtc_recovering_once(
                                                    peer_id,
                                                    *session_id,
                                                    reason,
                                                )
                                                .await;
                                        }
                                        Some(false) => {
                                            // Initial connection attempt failed before the peer ever
                                            // reached a usable state. Terminate at Idle rather than
                                            // Recovering so clients don't mistake a first-time failure
                                            // for a recovery window.
                                            coord
                                                .notify_webrtc_idle_if_changed(
                                                    peer_id,
                                                    *session_id,
                                                    reason,
                                                )
                                                .await;
                                        }
                                        None => {
                                            tracing::debug!(
                                                peer_id = ?peer_id,
                                                session_id = *session_id,
                                                reason = reason,
                                                "Ignoring unavailable state for stale or missing WebRTC peer"
                                            );
                                        }
                                    }
                                }
                                ConnectionEvent::DataChannelOpened {
                                    peer_id,
                                    session_id,
                                    ..
                                } => {
                                    {
                                        let mut peers = coord.peers.write().await;
                                        if let Some(state) = peers.get_mut(peer_id)
                                            && state.session_id == *session_id
                                        {
                                            state.mark_data_channel_opened();
                                        }
                                    }
                                    coord
                                        .clear_peer_recovering_if_sendable(
                                            peer_id,
                                            *session_id,
                                            "data channel opened",
                                        )
                                        .await;
                                }
                                ConnectionEvent::IceRestartStarted {
                                    peer_id,
                                    session_id,
                                } => {
                                    coord
                                        .notify_webrtc_recovering_once(
                                            peer_id,
                                            *session_id,
                                            "ice/network recovery started",
                                        )
                                        .await;
                                }
                                ConnectionEvent::IceRestartCompleted {
                                    peer_id,
                                    session_id,
                                    success: true,
                                    ..
                                } => {
                                    coord
                                        .clear_peer_recovering_if_sendable(
                                            peer_id,
                                            *session_id,
                                            "ice restart completed",
                                        )
                                        .await;
                                }
                                ConnectionEvent::IceRestartCompleted {
                                    peer_id,
                                    session_id,
                                    success: false,
                                    ..
                                } => {
                                    coord
                                        .notify_webrtc_idle_if_changed(
                                            peer_id,
                                            *session_id,
                                            "ice restart failed",
                                        )
                                        .await;
                                    coord
                                        .clear_peer_recovering(
                                            peer_id,
                                            *session_id,
                                            "ice restart failed",
                                        )
                                        .await;
                                }
                                ConnectionEvent::ConnectionClosed {
                                    peer_id,
                                    session_id,
                                }
                                | ConnectionEvent::StateChanged {
                                    peer_id,
                                    session_id,
                                    state: ConnectionState::Closed,
                                    ..
                                } => {
                                    coord
                                        .notify_webrtc_idle_if_changed(
                                            peer_id,
                                            *session_id,
                                            "connection closed",
                                        )
                                        .await;
                                    coord
                                        .clear_peer_recovering(
                                            peer_id,
                                            *session_id,
                                            "connection closed",
                                        )
                                        .await;
                                }
                                ConnectionEvent::DataChannelClosed {
                                    peer_id,
                                    session_id,
                                    ..
                                } => {
                                    coord
                                        .notify_webrtc_recovering_once(
                                            peer_id,
                                            *session_id,
                                            "data channel closed",
                                        )
                                        .await;
                                }
                                _ => {}
                            }

                            // Extract peer_id and check if cleanup is needed
                            // Key: compare event.session_id with current PeerState.session_id
                            // to avoid stale events from old connections triggering cleanup on new ones
                            let peer_session_to_cleanup = match &event {
                                ConnectionEvent::DataChannelClosed {
                                    peer_id,
                                    session_id,
                                    payload_type,
                                    ..
                                } => {
                                    let peers_guard = coord.peers.read().await;
                                    match peers_guard.get(peer_id) {
                                        Some(state) if state.session_id == *session_id => {
                                            tracing::warn!(
                                                "⚠️ DataChannel closed for peer {}, payload_type={:?}, session={}; triggering cleanup",
                                                peer_id,
                                                payload_type,
                                                session_id
                                            );
                                            Some((peer_id.clone(), *session_id))
                                        }
                                        Some(state) => {
                                            tracing::debug!(
                                                "ℹ️ Ignoring stale DataChannelClosed for peer {} (event_session={}, current_session={})",
                                                peer_id,
                                                session_id,
                                                state.session_id
                                            );
                                            None
                                        }
                                        None => {
                                            tracing::debug!(
                                                "ℹ️ DataChannel closed for peer {} but already cleaned up",
                                                peer_id
                                            );
                                            None
                                        }
                                    }
                                }
                                ConnectionEvent::ConnectionClosed {
                                    peer_id,
                                    session_id,
                                    ..
                                } => {
                                    let peers_guard = coord.peers.read().await;
                                    match peers_guard.get(peer_id) {
                                        Some(state) if state.session_id == *session_id => {
                                            tracing::warn!(
                                                "⚠️ Connection closed for peer {}, session={}; triggering cleanup",
                                                peer_id,
                                                session_id
                                            );
                                            Some((peer_id.clone(), *session_id))
                                        }
                                        Some(state) => {
                                            tracing::debug!(
                                                "ℹ️ Ignoring stale ConnectionClosed for peer {} (event_session={}, current_session={})",
                                                peer_id,
                                                session_id,
                                                state.session_id
                                            );
                                            None
                                        }
                                        None => {
                                            tracing::debug!(
                                                "ℹ️ Connection closed for peer {} but already cleaned up",
                                                peer_id
                                            );
                                            None
                                        }
                                    }
                                }
                                ConnectionEvent::StateChanged {
                                    peer_id,
                                    session_id,
                                    state,
                                    ..
                                } => {
                                    use crate::transport::ConnectionState;
                                    if matches!(state, ConnectionState::Closed) {
                                        let peers_guard = coord.peers.read().await;
                                        match peers_guard.get(peer_id) {
                                            Some(ps) if ps.session_id == *session_id => {
                                                tracing::warn!(
                                                    "⚠️ PeerConnection Closed for peer {}, session={}; triggering cleanup",
                                                    peer_id,
                                                    session_id
                                                );
                                                Some((peer_id.clone(), *session_id))
                                            }
                                            Some(ps) => {
                                                tracing::debug!(
                                                    "ℹ️ Ignoring stale StateChanged::Closed for peer {} (event_session={}, current_session={})",
                                                    peer_id,
                                                    session_id,
                                                    ps.session_id
                                                );
                                                None
                                            }
                                            None => {
                                                tracing::debug!(
                                                    "ℹ️ PeerConnection Closed for peer {} but already cleaned up",
                                                    peer_id
                                                );
                                                None
                                            }
                                        }
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            };

                            // Cleanup outside the match to avoid holding read lock
                            if let Some((peer_id, session_id)) = peer_session_to_cleanup {
                                coord
                                    .cleanup_connection_if_session(
                                        &peer_id,
                                        session_id,
                                        true,
                                        "connection event",
                                    )
                                    .await;
                            }
                        } else {
                            // Coordinator dropped, exit
                            tracing::debug!(
                                "🔌 WebRtcCoordinator internal event listener stopping (coordinator dropped)"
                            );
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            "⚠️ WebRtcCoordinator internal event listener lagged by {} events",
                            n
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        tracing::debug!(
                            "🔌 WebRtcCoordinator internal event listener stopped (channel closed)"
                        );
                        break;
                    }
                }
            }
        })
    }

    async fn is_peer_session_connected(
        peer_id: &ActrId,
        expected_session_id: u64,
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
    ) -> bool {
        let peers = peers.read().await;
        peers.get(peer_id).is_some_and(|state| {
            state.session_id == expected_session_id
                && matches!(state.current_state, RTCPeerConnectionState::Connected)
        })
    }

    /// Wait for a new connection to become fully ready in two stages.
    ///
    /// Stage 1 waits for ICE/DTLS to reach Connected. Stage 2 then gives SCTP/DataChannel
    /// a shorter budget to open before the connection is reported ready.
    async fn wait_for_connection_ready_event(
        peer_id: &ActrId,
        expected_session_id: u64,
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
        event_broadcaster: &ConnectionEventBroadcaster,
        webrtc_conn: &super::connection::WebRtcConnection,
        ice_connected_timeout: Duration,
        data_channel_after_ice_timeout: Duration,
    ) -> bool {
        // Quick check: if DataChannel is already open, return immediately
        if webrtc_conn.has_open_data_channel().await {
            tracing::debug!("✅ DataChannel already open for peer {}", peer_id);
            return true;
        }

        // Subscribe to events
        let mut event_rx = event_broadcaster.subscribe();
        let target_peer = peer_id.clone();
        let mut ice_connected =
            Self::is_peer_session_connected(peer_id, expected_session_id, peers).await;

        if ice_connected {
            tracing::debug!(
                "✅ ICE already connected for peer {}, waiting for DataChannel",
                target_peer
            );
        }

        // Create a pinned sleep future for the current stage timeout.
        let sleep = tokio::time::sleep(if ice_connected {
            data_channel_after_ice_timeout
        } else {
            ice_connected_timeout
        });
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                _ = &mut sleep => {
                    if ice_connected {
                        tracing::warn!(
                            "⚠️ Timeout waiting for DataChannel to open after ICE connected for peer {} (session_id={}, {:?})",
                            target_peer,
                            expected_session_id,
                            data_channel_after_ice_timeout
                        );
                    } else {
                        tracing::warn!(
                            "⚠️ Timeout waiting for ICE connected for peer {} (session_id={}, {:?})",
                            target_peer,
                            expected_session_id,
                            ice_connected_timeout
                        );
                    }
                    return false;
                }
                res = event_rx.recv() => {
                    match res {
                        Ok(ConnectionEvent::DataChannelOpened {
                            peer_id,
                            session_id,
                            payload_type,
                        }) if peer_id == target_peer && session_id == expected_session_id =>
                        {
                            tracing::info!(
                                "✅ DataChannel opened for peer {} (session_id={}, payload_type={:?}, event-driven)",
                                peer_id,
                                session_id,
                                payload_type
                            );
                            return true;
                        }
                        Ok(ConnectionEvent::StateChanged {
                            peer_id,
                            session_id,
                            state: ConnectionState::Connected,
                        }) if peer_id == target_peer && session_id == expected_session_id => {
                            if webrtc_conn.has_open_data_channel().await {
                                tracing::info!(
                                    "✅ DataChannel already open when ICE connected for peer {} (session_id={})",
                                    peer_id,
                                    session_id
                                );
                                return true;
                            }

                            if !ice_connected {
                                ice_connected = true;
                                sleep.as_mut().reset(
                                    tokio::time::Instant::now() + data_channel_after_ice_timeout
                                );
                                tracing::info!(
                                    "✅ ICE connected for peer {} (session_id={}); waiting {:?} for DataChannel",
                                    peer_id,
                                    session_id,
                                    data_channel_after_ice_timeout
                                );
                            }
                        }
                        Ok(ConnectionEvent::StateChanged {
                            peer_id,
                            session_id,
                            state: ConnectionState::Failed | ConnectionState::Closed,
                        }) if peer_id == target_peer && session_id == expected_session_id => {
                            tracing::warn!(
                                "⚠️ Connection entered terminal state before DataChannel ready for peer {} (session_id={})",
                                peer_id,
                                session_id
                            );
                            return false;
                        }
                        Ok(ConnectionEvent::ConnectionClosed {
                            peer_id,
                            session_id,
                        }) if peer_id == target_peer && session_id == expected_session_id => {
                            tracing::warn!(
                                "⚠️ Connection closed before DataChannel ready for peer {} (session_id={})",
                                peer_id,
                                session_id
                            );
                            return false;
                        }
                        Ok(_) => {
                            // Other events, continue waiting
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("⚠️ Event stream lagged by {} events, continuing...", n);
                            if webrtc_conn.has_open_data_channel().await {
                                tracing::info!(
                                    "✅ DataChannel open after event lag for peer {} (session_id={})",
                                    target_peer,
                                    expected_session_id
                                );
                                return true;
                            }
                            if !ice_connected
                                && Self::is_peer_session_connected(
                                    &target_peer,
                                    expected_session_id,
                                    peers,
                                )
                                .await
                            {
                                ice_connected = true;
                                sleep.as_mut().reset(
                                    tokio::time::Instant::now() + data_channel_after_ice_timeout
                                );
                                tracing::info!(
                                    "✅ ICE connected after event lag for peer {} (session_id={}); waiting {:?} for DataChannel",
                                    target_peer,
                                    expected_session_id,
                                    data_channel_after_ice_timeout
                                );
                            }
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::warn!("⚠️ Event channel closed while waiting for connection ready");
                            return false;
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn is_active_session(&self, peer_id: &ActrId, session_id: u64) -> bool {
        self.peers
            .read()
            .await
            .get(peer_id)
            .is_some_and(|state| state.session_id == session_id)
    }

    /// Start health check task to clean up stale connections
    ///
    /// Periodically checks real-time peer connection states and cleans up:
    /// - connections in Failed/Closed state for longer than `MAX_FAILED_DURATION`
    /// - connections stuck in Disconnected for longer than
    ///   `MAX_DISCONNECTED_DURATION` (past the ICE restart budget, so the
    ///   restart mechanism can no longer bring them back)
    fn spawn_health_check_task(self: &Arc<Self>) -> JoinHandle<()> {
        let coordinator = Arc::downgrade(self);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(HEALTH_CHECK_INTERVAL);
            interval.tick().await; // Skip first immediate tick

            loop {
                interval.tick().await;

                if let Some(coord) = coordinator.upgrade() {
                    coord.check_and_cleanup_stale_connections().await;
                } else {
                    tracing::debug!("🔌 Health check task stopping (coordinator dropped)");
                    break;
                }
            }

            tracing::info!("🛑 Health check task exited");
        })
    }

    /// Check and cleanup stale peer connections
    ///
    /// This method identifies peers that should be cleaned up based on:
    /// - Failed/Closed state duration exceeding `MAX_FAILED_DURATION`
    /// - Disconnected state duration exceeding `MAX_DISCONNECTED_DURATION`
    ///   (past the ICE restart budget; see `stale_peer_reap_reason`)
    ///
    /// A peer stuck in Disconnected past the restart budget will not recover
    /// on its own: when ICE restart exhausts without reaching Failed (or never
    /// runs, on the answerer side) the peer lingers forever — and with it the
    /// RTCPeerConnection and the per-interface ICE UDP sockets it holds,
    /// leaking fds for the process lifetime.
    async fn check_and_cleanup_stale_connections(&self) {
        let peers_to_cleanup: Vec<(ActrId, u64, String)> = {
            let peers = self.peers.read().await;
            let now = std::time::Instant::now();

            peers
                .iter()
                .filter_map(|(peer_id, state)| {
                    // Get current real-time state from RTCPeerConnection
                    let current_state = state.peer_connection.connection_state();
                    let duration_since_change = now.duration_since(state.last_state_change);

                    let reason = stale_peer_reap_reason(current_state, duration_since_change)?;
                    tracing::warn!("🧹 Marking peer {} for cleanup: {}", peer_id, reason);
                    Some((peer_id.clone(), state.session_id, reason))
                })
                .collect()
        };

        // Cleanup marked peers
        if !peers_to_cleanup.is_empty() {
            tracing::info!(
                "🧹 Health check: cleaning up {} stale connection(s)",
                peers_to_cleanup.len()
            );

            for (peer_id, session_id, reason) in peers_to_cleanup {
                tracing::info!(
                    "🧹 Cleaning up stale connection for peer {}: {}",
                    peer_id,
                    reason
                );
                self.cleanup_connection_if_session(&peer_id, session_id, true, &reason)
                    .await;
            }
        }
    }

    /// Start signaling coordinator (listen for ActrRelay messages)
    ///
    /// This method starts a background task that continuously listens for messages from SignalingClient
    /// and handles WebRTC-related signaling (Offer/Answer/ICE). Repeated calls
    /// while running are idempotent; calling `start` after a completed
    /// [`Self::shutdown_background_tasks`] starts a fresh task set.
    pub async fn start(self: Arc<Self>) -> ActorResult<()> {
        tracing::info!("🚀 WebRtcCoordinator starting signaling loop");

        // Keep start, shutdown, and restart linearizable without holding the
        // handle-ownership lock while tasks are joined.
        let _lifecycle_guard = self.background_tasks.lifecycle_gate.lock().await;
        let mut background_tasks = self.background_tasks.handles.lock().await;
        if !background_tasks.is_empty() {
            tracing::debug!("WebRtcCoordinator background tasks already started");
            return Ok(());
        }

        // Start internal event listener for connection close handling
        let internal_event_listener = self.spawn_internal_event_listener();

        // Start health check task for cleaning up stale connections
        let health_check = self.spawn_health_check_task();

        let coordinator = self.clone();
        let signaling_loop = tokio::spawn(async move {
            loop {
                // 1. Receive message from SignalingClient
                match coordinator.signaling_client.receive_envelope().await {
                    Ok(Some(envelope)) => {
                        #[cfg(feature = "opentelemetry")]
                        let (span, remote_ctx) = {
                            let remote_ctx = trace::extract_trace_context(&envelope);
                            let span = tracing::info_span!(
                                "WebRtcCoordinator.handle_envelope",
                                envelope_id = envelope.envelope_id,
                                reply_for = ?envelope.reply_for
                            );
                            span.set_parent(remote_ctx.clone());
                            (span, remote_ctx)
                        };

                        let handle_envelope_fut = coordinator.handle_envelope(
                            envelope,
                            #[cfg(feature = "opentelemetry")]
                            remote_ctx,
                        );
                        #[cfg(feature = "opentelemetry")]
                        let handle_envelope_fut = handle_envelope_fut.instrument(span);
                        handle_envelope_fut.await;
                    }
                    Ok(None) => {
                        tracing::info!(
                            "🔌 SignalingClient connection closed, exiting signaling loop"
                        );
                        break;
                    }
                    Err(e) => {
                        tracing::error!("❌ Signaling receive error: {}", e);
                        // Continue loop, don't exit (may be temporary error)
                    }
                }
            }

            tracing::info!("🛑 WebRtcCoordinator signaling loop exited");
        });
        background_tasks.extend([internal_event_listener, health_check, signaling_loop]);

        Ok(())
    }

    /// Abort and join every long-running task created by [`Self::start`].
    ///
    /// Shutdown calls are serialized with each other and with `start`. Handles
    /// are removed while holding the ownership lock, then awaited after
    /// releasing that lock so task completion cannot deadlock registration.
    pub async fn shutdown_background_tasks(&self) {
        let _lifecycle_guard = self.background_tasks.lifecycle_gate.lock().await;
        let handles = {
            let mut background_tasks = self.background_tasks.handles.lock().await;
            std::mem::take(&mut *background_tasks)
        };

        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }
    }

    /// Handle received signaling envelope
    async fn handle_envelope(
        self: &Arc<Self>,
        envelope: SignalingEnvelope,
        #[cfg(feature = "opentelemetry")] remote_ctx: opentelemetry::Context,
    ) {
        // Decode SignalingEnvelope
        match envelope.flow {
            Some(signaling_envelope::Flow::ActrRelay(relay)) => {
                let source = relay.source;
                let target = relay.target;
                #[cfg(feature = "opentelemetry")]
                self.root_context_map
                    .write()
                    .await
                    .insert(source.clone(), remote_ctx);
                match relay.payload {
                    Some(actr_relay::Payload::SessionDescription(sd)) => match sd.r#type() {
                        SdpType::Offer => {
                            tracing::info!("📥 Received Offer from {}", source);
                            if let Err(e) =
                                self.handle_offer(&source, sd.sdp, sd.sdp_exchange_id).await
                            {
                                tracing::error!("❌ Failed to handle Offer: {}", e);
                            }
                        }
                        SdpType::Answer => {
                            tracing::info!("📥 Received Answer from {}", source);
                            if let Err(e) = self
                                .handle_answer(&source, sd.sdp, sd.sdp_exchange_id)
                                .await
                            {
                                tracing::error!("❌ Failed to handle Answer: {}", e);
                            }
                        }
                        SdpType::RenegotiationOffer => {
                            tracing::info!("📥 Received RenegotiationOffer from {:?}", source);
                            if let Err(e) = self
                                .handle_renegotiation_offer(&source, sd.sdp, sd.sdp_exchange_id)
                                .await
                            {
                                tracing::error!("❌ Failed to handle RenegotiationOffer: {}", e);
                            }
                        }
                        SdpType::IceRestartOffer => {
                            tracing::info!("♻️ Received ICE Restart Offer from {:?}", source);
                            if let Err(e) = self
                                .handle_ice_restart_offer(&source, sd.sdp, sd.sdp_exchange_id)
                                .await
                            {
                                tracing::error!("❌ Failed to handle ICE Restart Offer: {}", e);
                            }
                        }
                    },
                    Some(actr_relay::Payload::RoleAssignment(assign)) => {
                        let local_id = self.local_id_snapshot();
                        tracing::info!(
                            "🎭 Received RoleAssignment from {:?}, is_offerer={} (source peer), local_id={}",
                            source,
                            assign.is_offerer,
                            local_id,
                        );
                        let peer = if source == local_id {
                            target.clone()
                        } else {
                            source.clone()
                        };
                        self.handle_role_assignment(assign, peer).await;
                    }
                    Some(actr_relay::Payload::IceCandidate(ice)) => {
                        tracing::debug!("📥 Received ICE Candidate from {:?}", source);
                        if let Err(e) = self.handle_ice_candidate(&source, ice).await {
                            tracing::error!("❌ Failed to handle ICE Candidate: {}", e);
                        }
                    }
                    Some(actr_relay::Payload::RoleNegotiation(_)) => {
                        tracing::trace!(
                            "📥 Received RoleNegotiation payload; ignored by WebRtcCoordinator"
                        );
                    }
                    Some(actr_relay::Payload::IceRestartRequest(req)) => {
                        tracing::info!(
                            "📥 Received IceRestartRequest from serial={}, reason={:?}",
                            source,
                            req.reason
                        );
                        if let Err(e) = self.handle_ice_restart_request(&source, req.reason).await {
                            tracing::error!("❌ Failed to handle IceRestartRequest: {}", e);
                        }
                    }
                    None => {
                        tracing::warn!("⚠️ ActrRelay missing payload");
                    }
                }
            }
            Some(other_flow) => {
                tracing::warn!("⚠️ Ignoring non-ActrRelay flow: {:?}", other_flow);
            }
            None => {
                tracing::warn!("⚠️ SignalingEnvelope missing flow");
            }
        }
    }

    /// Close all peer connections and clear internal peer state.
    ///
    /// This is typically called during shutdown to ensure that all
    /// RTCPeerConnection instances are closed and associated state
    /// (pending ICE candidates, WebRtcConnection state) is dropped.
    pub async fn close_all_peers(&self) -> ActorResult<()> {
        self.close_all_peers_with_mode(PeerCloseMode::Graceful)
            .await
    }

    /// Close all peers without waiting for DataChannel buffers to drain.
    ///
    /// Mobile recovery paths use this because the OS can leave WebRTC's
    /// connection state at `Connected` after the underlying route is gone.
    pub async fn close_all_peers_immediately(&self) -> ActorResult<()> {
        self.close_all_peers_with_mode(PeerCloseMode::Immediate)
            .await
    }

    async fn close_all_peers_with_mode(&self, close_mode: PeerCloseMode) -> ActorResult<()> {
        let lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
        let close_state_guard =
            match CloseAllStateGuard::enter(Arc::clone(&self.peer_signaling)).await {
                CloseAllEntry::Leader(guard) => guard,
                CloseAllEntry::Follower(flight) => {
                    drop(lifecycle_guard);
                    let is_same_hook_flight = CLOSE_ALL_HOOK_REENTRY
                        .try_with(|hook_flight| Arc::ptr_eq(hook_flight, &flight))
                        .unwrap_or(false);
                    if is_same_hook_flight {
                        tracing::debug!(
                            generation = flight.generation,
                            "Returning from close-all re-entry made by its own lifecycle hook"
                        );
                        return Ok(());
                    }

                    tracing::debug!(
                        generation = flight.generation,
                        "WebRTC peer shutdown is already in progress; joining its exact flight"
                    );
                    return flight.wait().await;
                }
            };

        let close_flight = close_state_guard.flight();
        let result = self
            .run_close_all_peers(close_mode, lifecycle_guard, close_flight)
            .await;
        close_state_guard.complete(result.clone());
        result
    }

    async fn run_close_all_peers(
        &self,
        close_mode: PeerCloseMode,
        lifecycle_guard: tokio::sync::MutexGuard<'_, ()>,
        close_flight: Arc<CloseAllFlight>,
    ) -> ActorResult<()> {
        tracing::info!("🔻 Closing all WebRTC peer connections");

        // Clone everything needed after the drain up front. Once peer states
        // leave `self.peers`, they are synchronously handed to a cancellation
        // guard so they cannot be dropped before physical teardown completes.
        let restart_signaling_gates = Arc::clone(&self.peer_signaling.gates);
        let network_recovering_peers = Arc::clone(&self.network_recovering_peers);
        let hook_callback = self.hook_callback.get().cloned();

        // Abort tracked restart tasks before waiting for their signaling gates.
        // A blocked tracked send holds its gate, so cancellation must complete
        // before the state phase attempts to acquire every peer gate.
        let restart_handles = {
            let mut peers = self.peers.write().await;
            let mut restart_handles = Vec::new();
            for (peer_id, state) in peers.iter_mut() {
                if let Some(handle) = state.restart_task_handle.take() {
                    handle.abort();
                    restart_handles.push((peer_id.clone(), handle));
                }
                state.ice_restart_inflight = false;
                state.ice_restart_attempts = 0;
                state.ice_signaling.clear_pending_restart();
            }
            restart_handles
        };

        let state_phase_deadline = tokio::time::Instant::now() + CLOSE_ALL_QUIESCE_TIMEOUT;
        let restart_count = restart_handles.len();
        if tokio::time::timeout_at(state_phase_deadline, async move {
            for (peer_id, handle) in restart_handles {
                if let Err(err) = handle.await
                    && !err.is_cancelled()
                {
                    tracing::warn!("⚠️ ICE restart task join failed for {}: {}", peer_id, err);
                }
            }
        })
        .await
        .is_err()
        {
            tracing::warn!(
                task_count = restart_count,
                timeout_ms = CLOSE_ALL_QUIESCE_TIMEOUT.as_millis(),
                "Timed out waiting for aborted WebRTC restart tasks"
            );
            return Err(ActrError::TimedOut);
        }

        // Acquire every active-session and pre-session signaling gate in a
        // stable order, then drain while those gates are held. A send that
        // already crossed its final lifecycle/session check must finish before
        // its state is discarded; queued sends observe the epoch change.
        let (all_peers, late_restart_handles) =
            tokio::time::timeout_at(state_phase_deadline, async {
                loop {
                    let mut peer_ids: Vec<ActrId> = {
                        let peers = self.peers.read().await;
                        peers.keys().cloned().collect()
                    };
                    peer_ids.extend({
                        let negotiations = self.peer_negotiation.lock().await;
                        negotiations.keys().cloned().collect::<Vec<_>>()
                    });
                    peer_ids.sort_by_key(|peer_id| peer_id.encode_to_vec());
                    peer_ids.dedup();

                    let mut peer_gates = Vec::with_capacity(peer_ids.len());
                    for peer_id in &peer_ids {
                        peer_gates.push((
                            peer_id.clone(),
                            self.restart_signaling_gate_for(peer_id).await,
                        ));
                    }
                    let mut signaling_guards = Vec::with_capacity(peer_gates.len());
                    for (_, gate) in &peer_gates {
                        signaling_guards.push(gate.lock().await);
                    }

                    // Acquire auxiliary-state locks before `peers`, but do not
                    // mutate them until all locks needed for the state commit
                    // are held. Cancellation before the drain therefore leaves
                    // both peer and auxiliary state intact.
                    let mut pending_candidates = self.pending_candidates.write().await;
                    #[cfg(feature = "opentelemetry")]
                    let mut root_contexts = self.root_context_map.write().await;
                    let mut peer_negotiation = self.peer_negotiation.lock().await;
                    let mut peers = self.peers.write().await;
                    if peers.keys().any(|peer_id| !peer_ids.contains(peer_id))
                        || peer_negotiation
                            .keys()
                            .any(|peer_id| !peer_ids.contains(peer_id))
                    {
                        drop(peers);
                        drop(peer_negotiation);
                        #[cfg(feature = "opentelemetry")]
                        drop(root_contexts);
                        drop(pending_candidates);
                        drop(signaling_guards);
                        drop(peer_gates);
                        continue;
                    }

                    let mut all_peers: Vec<(ActrId, PeerState)> = peers.drain().collect();
                    peer_negotiation.clear();
                    pending_candidates.clear();
                    #[cfg(feature = "opentelemetry")]
                    root_contexts.clear();
                    drop(peers);
                    drop(peer_negotiation);
                    #[cfg(feature = "opentelemetry")]
                    drop(root_contexts);
                    drop(pending_candidates);
                    let mut late_restart_handles = Vec::new();
                    for (peer_id, state) in &mut all_peers {
                        if let Some(handle) = state.restart_task_handle.take() {
                            handle.abort();
                            late_restart_handles.push((peer_id.clone(), handle));
                        }
                    }

                    drop(signaling_guards);
                    drop(peer_gates);
                    break (all_peers, late_restart_handles);
                }
            })
            .await
            .map_err(|_| {
                tracing::warn!(
                    timeout_ms = CLOSE_ALL_QUIESCE_TIMEOUT.as_millis(),
                    "Timed out waiting for WebRTC peer signaling commits to quiesce"
                );
                ActrError::TimedOut
            })?;

        // This handoff is synchronous with the successful drain. If this
        // future is cancelled at any later await, the guard finishes physical
        // cleanup without emitting a potentially stale old-session hook.
        let mut drained_peers =
            DrainedPeerCleanupGuard::new(all_peers, Arc::clone(&network_recovering_peers));

        // Hooks and connection shutdown may call back into the coordinator or
        // stall in WebRTC. They must run after every coordinator gate is free.
        drop(lifecycle_guard);

        // Keep entries that still have live holders. Removing one would allow
        // a concurrent lookup to create a second gate for the same peer and
        // break signaling serialization. Dead weak entries can be discarded.
        Self::prune_restart_signaling_gates(&restart_signaling_gates).await;

        let restart_count = late_restart_handles.len();
        if tokio::time::timeout(CLOSE_ALL_QUIESCE_TIMEOUT, async move {
            for (peer_id, handle) in late_restart_handles {
                if let Err(err) = handle.await
                    && !err.is_cancelled()
                {
                    tracing::warn!("⚠️ ICE restart task join failed for {}: {}", peer_id, err);
                }
            }
        })
        .await
        .is_err()
        {
            tracing::warn!(
                task_count = restart_count,
                timeout_ms = CLOSE_ALL_QUIESCE_TIMEOUT.as_millis(),
                "Timed out waiting for aborted WebRTC restart tasks"
            );
        }

        // Hooks remain in the caller's future. Direct close-all re-entry is
        // identified with task-local context; indirect/custom hook stalls are
        // bounded by one deadline for the whole batch. Once a hook completes
        // (or is cancelled at the deadline), physical teardown owns the peer.
        let mut close_tasks = Vec::new();
        let hook_deadline = tokio::time::Instant::now() + CLOSE_ALL_HOOK_TIMEOUT;
        while let Some((peer_id, state)) = drained_peers.last() {
            for handle in &state.receive_handles {
                handle.abort();
            }
            Self::clear_peer_recovering_in(
                &network_recovering_peers,
                peer_id,
                state.session_id,
                "close all peers",
            )
            .await;
            let hook = CLOSE_ALL_HOOK_REENTRY.scope(
                Arc::clone(&close_flight),
                Self::notify_removed_peer_idle_if_needed(
                    hook_callback.as_ref(),
                    peer_id,
                    state.session_id,
                    state,
                    "close all peers",
                ),
            );
            if tokio::time::timeout_at(hook_deadline, hook).await.is_err() {
                tracing::warn!(
                    peer_id = %peer_id,
                    session_id = state.session_id,
                    timeout_ms = CLOSE_ALL_HOOK_TIMEOUT.as_millis(),
                    "Timed out invoking WebRTC close-all lifecycle hook"
                );
            }

            let (peer_id, state) = drained_peers
                .pop()
                .expect("drained peer must remain owned until its hook completes");
            let recovery_state = Arc::clone(&network_recovering_peers);
            let task_peer_id = peer_id.clone();
            let task = tokio::spawn(async move {
                tracing::info!("🔻 Closing PeerConnection for {}", task_peer_id);
                Self::teardown_removed_peer_state_with(
                    &recovery_state,
                    None,
                    &task_peer_id,
                    state,
                    false,
                    close_mode,
                    "close all peers",
                )
                .await;
            });
            close_tasks.push((peer_id, task));
        }

        let mut first_join_error = None;
        for (peer_id, task) in close_tasks {
            if let Err(err) = task.await {
                tracing::error!(
                    peer_id = %peer_id,
                    error = %err,
                    "WebRTC peer teardown task failed"
                );
                first_join_error.get_or_insert(err);
            }
        }

        // Signaling may have delivered more auxiliary data while physical
        // cleanup was running. Sweep it once more under the lifecycle gate
        // while `closing_all` still rejects new peer sessions.
        let _final_lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
        let mut pending_candidates = self.pending_candidates.write().await;
        #[cfg(feature = "opentelemetry")]
        let mut root_contexts = self.root_context_map.write().await;
        let mut peer_negotiation = self.peer_negotiation.lock().await;
        pending_candidates.clear();
        peer_negotiation.clear();
        #[cfg(feature = "opentelemetry")]
        root_contexts.clear();
        drop(peer_negotiation);
        #[cfg(feature = "opentelemetry")]
        drop(root_contexts);
        drop(pending_candidates);

        if let Some(err) = first_join_error {
            Err(ActrError::Internal(format!(
                "WebRTC peer teardown task failed: {err}"
            )))
        } else {
            Ok(())
        }
    }

    fn new_envelope_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    async fn record_pending_local_offer(
        &self,
        target: &ActrId,
        session_id: u64,
        sdp_exchange_id: String,
    ) -> ActorResult<()> {
        if Self::record_pending_local_offer_for_peer(
            &self.peers,
            target,
            session_id,
            sdp_exchange_id,
        )
        .await
        {
            Ok(())
        } else {
            Err(ActrError::Internal(format!(
                "Peer state not found while recording pending offer: {target:?}"
            )))
        }
    }

    async fn record_pending_local_offer_for_peer(
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
        target: &ActrId,
        session_id: u64,
        sdp_exchange_id: String,
    ) -> bool {
        let mut peers = peers.write().await;
        let Some(state) = peers.get_mut(target) else {
            return false;
        };
        if state.session_id != session_id {
            return false;
        }

        state.ice_signaling.pending_local_sdp_exchange_id = Some(sdp_exchange_id);
        true
    }

    async fn clear_pending_local_offer(
        &self,
        target: &ActrId,
        session_id: u64,
        sdp_exchange_id: &str,
    ) {
        Self::clear_pending_local_offer_for_peer(&self.peers, target, session_id, sdp_exchange_id)
            .await;
    }

    async fn clear_pending_local_offer_for_peer(
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
        target: &ActrId,
        session_id: u64,
        sdp_exchange_id: &str,
    ) {
        let mut peers = peers.write().await;
        if let Some(state) = peers.get_mut(target) {
            let should_clear = state.session_id == session_id
                && state
                    .ice_signaling
                    .pending_local_sdp_exchange_id
                    .as_ref()
                    .is_some_and(|pending| pending == sdp_exchange_id);
            if should_clear {
                state.ice_signaling.pending_local_sdp_exchange_id = None;
            }
        }
    }

    fn build_actr_relay_envelope(
        local_id: ActrId,
        credential: AIdCredential,
        target: &ActrId,
        payload: actr_relay::Payload,
    ) -> SignalingEnvelope {
        let relay = ActrRelay {
            source: local_id,
            credential,
            target: target.clone(),
            payload: Some(payload),
        };

        SignalingEnvelope {
            envelope_version: 1,
            envelope_id: Self::new_envelope_id(),
            reply_for: None,
            timestamp: prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            },
            traceparent: None,
            tracestate: None,
            flow: Some(signaling_envelope::Flow::ActrRelay(relay)),
        }
    }

    /// The only raw send boundary for peer-scoped signaling.
    ///
    /// Requiring the peer commit guard in the type signature prevents new
    /// RoleNegotiation/Offer/Answer/ICE call sites from accidentally bypassing
    /// cleanup and lifecycle/session validation.
    async fn send_peer_signaling_envelope_while_guarded(
        _commit_guard: &PeerSignalingCommitGuard,
        signaling_client: &Arc<dyn SignalingClient>,
        envelope: SignalingEnvelope,
    ) -> crate::transport::NetworkResult<()> {
        signaling_client.send_envelope(envelope).await
    }

    async fn send_peer_actr_relay_while_guarded(
        &self,
        commit_guard: &PeerSignalingCommitGuard,
        target: &ActrId,
        payload: actr_relay::Payload,
    ) -> ActorResult<()> {
        let credential = self.credential_state.credential().await;
        let envelope =
            Self::build_actr_relay_envelope(self.local_id_snapshot(), credential, target, payload);

        Self::send_peer_signaling_envelope_while_guarded(
            commit_guard,
            &self.signaling_client,
            envelope,
        )
        .await
        .map_err(|e| ActrError::Unavailable(format!("Signaling server unavailable: {e}")))?;

        Ok(())
    }

    async fn commit_peer_signaling(
        &self,
        target: &ActrId,
        session_id: u64,
        restart_epoch: Option<u64>,
        payload: actr_relay::Payload,
    ) -> ActorResult<bool> {
        // Resolve mutable identity state before taking the per-peer gate so a
        // credential refresh cannot unnecessarily block cleanup for this peer.
        let credential = self.credential_state.credential().await;
        let envelope =
            Self::build_actr_relay_envelope(self.local_id_snapshot(), credential, target, payload);
        let commit_context = self.peer_signaling_commit_context();
        let Some(commit_guard) = commit_context
            .acquire_commit(target, session_id, restart_epoch)
            .await
        else {
            return Ok(false);
        };
        let send_result = Self::send_peer_signaling_envelope_while_guarded(
            &commit_guard,
            &self.signaling_client,
            envelope,
        )
        .await
        .map(|()| true)
        .map_err(|e| ActrError::Unavailable(format!("Signaling server unavailable: {e}")));
        drop(commit_guard);
        Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;
        send_result
    }

    async fn send_role_negotiation(
        &self,
        target: &ActrId,
        role_negotiation: RoleNegotiation,
        lifecycle_epoch: u64,
    ) -> ActorResult<bool> {
        let envelope = Self::build_actr_relay_envelope(
            self.local_id_snapshot(),
            self.credential_state.credential().await,
            target,
            actr_relay::Payload::RoleNegotiation(role_negotiation),
        );
        let commit_context = self.peer_signaling_commit_context();
        let Some(commit_guard) = commit_context
            .acquire_pre_session_commit(target, lifecycle_epoch)
            .await
        else {
            return Ok(false);
        };
        let result = Self::send_peer_signaling_envelope_while_guarded(
            &commit_guard,
            &self.signaling_client,
            envelope,
        )
        .await
        .map(|()| true)
        .map_err(|e| ActrError::Unavailable(format!("Signaling server unavailable: {e}")));
        drop(commit_guard);
        Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;
        result
    }

    fn role_flight_matches(
        receiver: &watch::Receiver<Option<ActorResult<bool>>>,
        flight: &RoleNegotiationFlight,
    ) -> bool {
        receiver.same_channel(&flight.result_tx.subscribe())
    }

    async fn finish_role_flight_if_current(
        &self,
        target: &ActrId,
        lifecycle_epoch: u64,
        receiver: &watch::Receiver<Option<ActorResult<bool>>>,
        result: ActorResult<bool>,
    ) -> bool {
        let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
        let mut negotiations = self.peer_negotiation.lock().await;
        let Some(state) = negotiations.get_mut(target) else {
            return false;
        };
        let matches = state.role_flight.as_ref().is_some_and(|flight| {
            flight.lifecycle_epoch == lifecycle_epoch && Self::role_flight_matches(receiver, flight)
        });
        if !matches {
            return false;
        }

        let flight = state
            .role_flight
            .take()
            .expect("matching role flight must still be present");
        flight.result_tx.send_replace(Some(result));
        true
    }

    async fn wait_for_role_result(
        receiver: &mut watch::Receiver<Option<ActorResult<bool>>>,
    ) -> ActorResult<bool> {
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }
            receiver.changed().await.map_err(|_| {
                ActrError::Unavailable(
                    "Role negotiation was cancelled before assignment".to_string(),
                )
            })?;
        }
    }

    async fn prepare_answerer_wait_if_lifecycle_current(
        &self,
        target: &ActrId,
        lifecycle_epoch: u64,
    ) -> ActorResult<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
        if self.peer_signaling.closing_all.load(Ordering::Acquire)
            || self
                .peer_signaling
                .peer_lifecycle_epoch
                .load(Ordering::Acquire)
                != lifecycle_epoch
        {
            return Err(ActrError::Unavailable(format!(
                "WebRTC peer lifecycle changed before answerer wait: {target}"
            )));
        }

        self.peer_negotiation
            .lock()
            .await
            .entry(target.clone())
            .or_default()
            .ready_tx = Some(tx);
        Ok(rx)
    }

    async fn store_ready_receiver_if_lifecycle_current(
        &self,
        target: &ActrId,
        lifecycle_epoch: u64,
        ready_rx: oneshot::Receiver<()>,
    ) {
        let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
        if self.peer_signaling.closing_all.load(Ordering::Acquire)
            || self
                .peer_signaling
                .peer_lifecycle_epoch
                .load(Ordering::Acquire)
                != lifecycle_epoch
        {
            return;
        }
        self.peer_negotiation
            .lock()
            .await
            .entry(target.clone())
            .or_default()
            .ready_rx = Some(ready_rx);
    }

    async fn ensure_answerer_wait_if_lifecycle_current(
        &self,
        target: &ActrId,
        lifecycle_epoch: u64,
    ) -> ActorResult<()> {
        let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
        if self.peer_signaling.closing_all.load(Ordering::Acquire)
            || self
                .peer_signaling
                .peer_lifecycle_epoch
                .load(Ordering::Acquire)
                != lifecycle_epoch
        {
            return Err(ActrError::Unavailable(format!(
                "WebRTC peer lifecycle changed before answerer wait: {target}"
            )));
        }

        let mut negotiations = self.peer_negotiation.lock().await;
        let state = negotiations.entry(target.clone()).or_default();
        if state.ready_tx.is_none() {
            let (tx, _rx) = oneshot::channel();
            state.ready_tx = Some(tx);
        }
        Ok(())
    }

    /// Initiate connection (create Offer)
    ///
    /// Acts as the initiator, sending a WebRTC connection request to the target peer
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(level = "info", skip_all, fields(actr_id = %self.local_id_snapshot(), target_id = %target))
    )]
    pub async fn initiate_connection(
        self: &Arc<Self>,
        target: &ActrId,
    ) -> ActorResult<oneshot::Receiver<()>> {
        tracing::info!("🚀 Initiating P2P connection to {}", target);

        let Some(peer_lifecycle_epoch) = self.capture_peer_lifecycle_epoch().await else {
            return Err(ActrError::Unavailable(format!(
                "WebRTC peer shutdown is active: {target}"
            )));
        };

        // Role negotiation is per-peer singleflight: concurrent callers share
        // one signaling request and one assignment result.
        let is_offerer = self.negotiate_role(target, peer_lifecycle_epoch).await?;
        tracing::debug!(
            "Role negotiation decided we are {:?} for {}",
            if is_offerer { "offerer" } else { "answerer" },
            target
        );
        if !is_offerer {
            return self
                .prepare_answerer_wait_if_lifecycle_current(target, peer_lifecycle_epoch)
                .await;
        }

        self.start_offer_connection(target, true, peer_lifecycle_epoch)
            .await
    }

    /// Create and send an offer (offerer path). If `skip_negotiation` is true, assumes role is already determined.
    /// This method includes retry logic for initial connection failures.
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(skip_all, fields(actr_id = %self.local_id_snapshot(), target_id = %target))
    )]
    async fn start_offer_connection(
        self: &Arc<Self>,
        target: &ActrId,
        skip_negotiation: bool,
        peer_lifecycle_epoch: u64,
    ) -> ActorResult<oneshot::Receiver<()>> {
        if !skip_negotiation {
            let role_result = self.negotiate_role(target, peer_lifecycle_epoch).await?;

            if !role_result {
                tracing::info!(
                    "🎭 Role negotiation decided we are answerer for {}, waiting for offer",
                    target
                );
                return self
                    .prepare_answerer_wait_if_lifecycle_current(target, peer_lifecycle_epoch)
                    .await;
            }
        }

        // Single connection attempt (no retry)
        tracing::info!("🔄 Starting connection to actr_id={}", target);

        match self
            .do_single_offer_connection(target, peer_lifecycle_epoch)
            .await
        {
            Ok((ready_rx, webrtc_conn)) => {
                // Wait for connection to be ready with timeout
                match tokio::time::timeout(INITIAL_CONNECTION_TIMEOUT, ready_rx).await {
                    Ok(Ok(())) => {
                        tracing::info!("✅ Connection established to serial={}", target);
                        // Return a new channel that's already signaled
                        let (tx, rx) = oneshot::channel();
                        let _ = tx.send(());
                        Ok(rx)
                    }
                    Ok(Err(_)) => {
                        tracing::warn!(
                            "⚠️ Connection failed (channel closed) for serial={}",
                            target
                        );
                        // Cleanup failed connection attempt
                        self.cleanup_failed_connection(target, webrtc_conn).await;
                        Err(ActrError::Internal(
                            "Connection ready channel closed".to_string(),
                        ))
                    }
                    Err(_) => {
                        tracing::warn!("⚠️ Connection timed out for serial={}", target);
                        // Cleanup failed connection attempt
                        self.cleanup_failed_connection(target, webrtc_conn).await;
                        Err(ActrError::TimedOut)
                    }
                }
            }
            Err(e) => {
                tracing::warn!("⚠️ Connection failed for serial={}: {}", target, e);
                Err(e)
            }
        }
    }

    /// Cleanup a failed connection attempt
    ///
    /// NOTE: Releases the write lock BEFORE calling close() to avoid blocking
    /// other operations on `peers` during potentially slow close operations.
    async fn cleanup_failed_connection(&self, target: &ActrId, webrtc_conn: WebRtcConnection) {
        let session_id = webrtc_conn.session_id();
        let removed = self
            .cleanup_connection_if_session(target, session_id, true, "failed connection attempt")
            .await;

        // If a newer session replaced the failed attempt, close only the stale
        // WebRtcConnection we still hold and leave the active peer map intact.
        if !removed {
            if let Err(e) = webrtc_conn.close().await {
                tracing::warn!(
                    "⚠️ Failed to close stale WebRtcConnection during cleanup for {} (session_id={}): {}",
                    target,
                    session_id,
                    e
                );
            }
        }

        tracing::debug!(
            "🧹 Cleaned up failed connection attempt for serial={}, session_id={}, removed_active={}",
            target,
            session_id,
            removed
        );
    }

    /// Cleanup a cancelled connection attempt (simpler version without WebRtcConnection)
    ///
    /// Used when connection creation is cancelled before completion.
    ///
    /// IMPORTANT: This method must release all locks before calling close() methods
    /// to avoid deadlock, since close() may trigger events that call this method again.
    async fn cleanup_cancelled_connection(&self, target: &ActrId, reason: &str) {
        self.cleanup_cancelled_connection_inner(target, reason)
            .await;
    }

    async fn cleanup_cancelled_connection_for_offer(
        &self,
        target: &ActrId,
        expected_session_id: u64,
        reason: &str,
        incoming_offer: &RTCSessionDescription,
    ) -> bool {
        // Abort only the session observed by this Offer. If close-all or a
        // newer Offer already replaced it, leave that newer session untouched.
        {
            let mut peers = self.peers.write().await;
            if let Some(state) = peers
                .get_mut(target)
                .filter(|state| state.session_id == expected_session_id)
                && let Some(handle) = state.restart_task_handle.take()
            {
                handle.abort();
            }
        }

        let restart_signaling_gate = self.restart_signaling_gate_for(target).await;
        let state_to_close = {
            let _signaling_guard = restart_signaling_gate.lock().await;
            // Match close-all's auxiliary-state lock order. Mutate candidates
            // and negotiation state only in the same commit that removes the
            // exact observed session.
            let mut pending_candidates = self.pending_candidates.write().await;
            let mut peer_negotiation = self.peer_negotiation.lock().await;
            let mut peers = self.peers.write().await;
            let state = match peers.get(target) {
                Some(state) if state.session_id == expected_session_id => peers.remove(target),
                _ => None,
            };
            if state.is_some() {
                let remove_entry = if let Some(candidates) = pending_candidates.get_mut(target) {
                    candidates.retain(|candidate| {
                        candidate_matches_description(candidate, incoming_offer)
                    });
                    candidates.is_empty()
                } else {
                    false
                };
                if remove_entry {
                    pending_candidates.remove(target);
                }
                // The negotiation entry may already contain the answerer
                // waiter and remote capabilities for this incoming Offer.
                // Only an offerer-side ready receiver belongs to the session
                // being replaced.
                if let Some(negotiation) = peer_negotiation.get_mut(target) {
                    negotiation.ready_rx = None;
                }
            }
            state
        };
        drop(restart_signaling_gate);

        let Some(state) = state_to_close else {
            Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;
            tracing::debug!(
                peer_id = %target,
                expected_session_id,
                "Skipping replacement-Offer cleanup after the observed peer session changed"
            );
            return false;
        };

        self.teardown_removed_peer_state(target, state, true, reason)
            .await;
        Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;
        tracing::debug!(
            peer_id = %target,
            expected_session_id,
            "Retained only pending candidates matching the replacement Offer"
        );
        true
    }

    async fn cleanup_connection_for_role_assignment_if_session(
        &self,
        target: &ActrId,
        expected_session_id: u64,
        reason: &str,
    ) -> bool {
        // A restart task may hold the peer signaling gate. Cancel only the
        // observed session's task before waiting for that gate to quiesce.
        {
            let mut peers = self.peers.write().await;
            if let Some(state) = peers
                .get_mut(target)
                .filter(|state| state.session_id == expected_session_id)
                && let Some(handle) = state.restart_task_handle.take()
            {
                handle.abort();
            }
        }

        let restart_signaling_gate = self.restart_signaling_gate_for(target).await;
        let state_to_close = {
            let _signaling_guard = restart_signaling_gate.lock().await;
            let mut pending_candidates = self.pending_candidates.write().await;
            let mut peer_negotiation = self.peer_negotiation.lock().await;
            let mut peers = self.peers.write().await;
            let state = match peers.get(target) {
                Some(state) if state.session_id == expected_session_id => peers.remove(target),
                _ => None,
            };
            if state.is_some() {
                pending_candidates.remove(target);
                // Preserve the exact role flight and remote capability that
                // caused this switch, while cancelling readiness owned by the
                // removed connection.
                if let Some(negotiation) = peer_negotiation.get_mut(target) {
                    negotiation.ready_tx = None;
                    negotiation.ready_rx = None;
                }
            }
            state
        };
        drop(restart_signaling_gate);

        let Some(state) = state_to_close else {
            Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;
            return false;
        };
        // Keep the current role flight intact so this exact RoleAssignment can
        // complete after teardown commits.
        self.teardown_removed_peer_state(target, state, true, reason)
            .await;
        Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;
        true
    }

    async fn cleanup_cancelled_connection_inner(&self, target: &ActrId, reason: &str) {
        tracing::debug!(
            "🧹 Starting cleanup for cancelled connection serial={}, reason={}",
            target,
            reason
        );

        // 1. Remove from peers map FIRST, release lock, THEN close
        //    This avoids deadlock: close() sends events that may trigger this method again
        let restart_signaling_gate = self.restart_signaling_gate_for(target).await;
        let state_to_close = {
            let _signaling_guard = restart_signaling_gate.lock().await;
            let mut pending_candidates = self.pending_candidates.write().await;
            let mut peer_negotiation = self.peer_negotiation.lock().await;
            let mut peers = self.peers.write().await;
            let state = peers.remove(target);
            pending_candidates.remove(target);
            peer_negotiation.remove(target);
            state
        }; // Lock released here
        drop(restart_signaling_gate);

        // 2. Close via webrtc_conn.close() which internally handles:
        //    - Idempotent close (try_close guard)
        //    - Cancel session token
        //    - Drain DataChannel buffers
        //    - Close peer_connection
        //    - Clear caches
        //    - Broadcast ConnectionClosed event with real session_id
        //
        // NOTE: Previously this method manually sent ConnectionClosed (with session_id=0)
        //       AND separately called peer_connection.close(), causing double close + double events.
        if let Some(state) = state_to_close {
            self.teardown_removed_peer_state(target, state, true, reason)
                .await;
        }
        Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;

        tracing::debug!(
            "🧹 Cleaned up cancelled connection for serial={}, reason={}",
            target,
            reason
        );
    }

    async fn cleanup_connection_if_session(
        &self,
        target: &ActrId,
        expected_session_id: u64,
        abort_restart_task: bool,
        reason: &str,
    ) -> bool {
        if abort_restart_task {
            // A restart task may itself be holding the peer signaling gate.
            // Abort only the observed session's task before waiting for that
            // gate, then let the gate acquisition quiesce it.
            let mut peers = self.peers.write().await;
            if let Some(state) = peers
                .get_mut(target)
                .filter(|state| state.session_id == expected_session_id)
                && let Some(handle) = state.restart_task_handle.take()
            {
                handle.abort();
            }
        }

        let restart_signaling_gate = self.restart_signaling_gate_for(target).await;
        let state_to_close = {
            let _signaling_guard = restart_signaling_gate.lock().await;
            let mut pending_candidates = self.pending_candidates.write().await;
            let mut peer_negotiation = self.peer_negotiation.lock().await;
            let mut peers = self.peers.write().await;
            let state = match peers.get(target) {
                Some(state) if state.session_id == expected_session_id => peers.remove(target),
                Some(state) => {
                    tracing::debug!(
                        "⏭️ Skip WebRTC cleanup for serial={} (reason={}): active_session_id={} != expected_session_id={}",
                        target,
                        reason,
                        state.session_id,
                        expected_session_id
                    );
                    None
                }
                None => {
                    tracing::debug!(
                        "⏭️ Skip WebRTC cleanup for serial={} (reason={}): peer already removed, expected_session_id={}",
                        target,
                        reason,
                        expected_session_id
                    );
                    None
                }
            };
            if state.is_some() {
                pending_candidates.remove(target);
                peer_negotiation.remove(target);
            }
            state
        };
        drop(restart_signaling_gate);

        let Some(state) = state_to_close else {
            Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;
            return false;
        };

        let session_id = state.session_id;
        tracing::debug!(
            "🧹 Cleaning WebRTC peer connection serial={}, session_id={}, reason={}",
            target,
            session_id,
            reason
        );

        self.teardown_removed_peer_state(target, state, abort_restart_task, reason)
            .await;
        Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;

        tracing::debug!(
            "🧹 Cleaned WebRTC peer connection serial={}, session_id={}, reason={}",
            target,
            session_id,
            reason
        );
        true
    }

    async fn capture_peer_lifecycle_epoch(&self) -> Option<u64> {
        if self.peer_signaling.closing_all.load(Ordering::Acquire) {
            return None;
        }

        let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
        if self.peer_signaling.closing_all.load(Ordering::Acquire) {
            return None;
        }

        Some(
            self.peer_signaling
                .peer_lifecycle_epoch
                .load(Ordering::Acquire),
        )
    }

    async fn insert_peer_if_lifecycle_current(
        &self,
        peer_id: ActrId,
        state: PeerState,
        expected_epoch: u64,
    ) -> Result<(), PeerState> {
        let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
        if self.peer_signaling.closing_all.load(Ordering::Acquire)
            || self
                .peer_signaling
                .peer_lifecycle_epoch
                .load(Ordering::Acquire)
                != expected_epoch
        {
            return Err(state);
        }

        let restart_signaling_gate = self.restart_signaling_gate_for(&peer_id).await;
        let _signaling_guard = restart_signaling_gate.lock().await;
        // Do not overwrite a session that won a concurrent inbound/outbound
        // setup race. Replacement paths must first remove the exact observed
        // session under this same peer gate.
        let mut peers = self.peers.write().await;
        if peers.contains_key(&peer_id) {
            return Err(state);
        }
        peers.insert(peer_id, state);
        Ok(())
    }

    async fn store_receive_handles_if_session_current(
        &self,
        peer_id: &ActrId,
        session_id: u64,
        receive_handles: Vec<JoinHandle<()>>,
    ) {
        let restart_signaling_gate = self.restart_signaling_gate_for(peer_id).await;
        let stored = {
            let _signaling_guard = restart_signaling_gate.lock().await;
            let mut peers = self.peers.write().await;
            match peers.get_mut(peer_id) {
                Some(state) if state.session_id == session_id => {
                    state.receive_handles = receive_handles;
                    true
                }
                _ => {
                    for handle in &receive_handles {
                        handle.abort();
                    }
                    false
                }
            }
        };
        drop(restart_signaling_gate);
        Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;
        if !stored {
            tracing::debug!(
                peer_id = %peer_id,
                session_id,
                "Discarded receive tasks created for a replaced peer session"
            );
        }
    }

    /// Perform a single offer connection attempt (without retry logic)
    async fn do_single_offer_connection(
        self: &Arc<Self>,
        target: &ActrId,
        peer_lifecycle_epoch: u64,
    ) -> ActorResult<(oneshot::Receiver<()>, WebRtcConnection)> {
        // Retrieve remote_fixed from peer negotiation state
        let remote_fixed = {
            let neg = self.peer_negotiation.lock().await;
            neg.get(target).map(|s| s.remote_fixed).unwrap_or(false)
        };

        // Create PeerConnection as Offerer (active side)
        let peer_connection = self
            .negotiator
            .create_peer_connection(false, remote_fixed)
            .await?;
        let peer_connection_arc = Arc::new(peer_connection);

        // 2. Create WebRtcConnection (shares Arc<RTCPeerConnection>) and
        //    install state-change handler with ICE-restart wiring.
        let webrtc_conn = WebRtcConnection::new(
            target.clone(),
            Arc::clone(&peer_connection_arc),
            self.event_broadcaster.sender(),
        );
        self.install_peer_state_handler(
            webrtc_conn.clone(),
            Arc::clone(&peer_connection_arc),
            target.clone(),
        );

        // 2.5. CRITICAL: Insert peer state early as placeholder to prevent race conditions
        // Create ready channel now, will be populated in step 8
        let (ready_tx, ready_rx) = oneshot::channel();
        let peer_state = PeerState {
            peer_connection: peer_connection_arc.clone(),
            webrtc_conn: webrtc_conn.clone(),
            ready_tx: Some(ready_tx),
            is_offerer: true,
            ice_signaling: PeerIceSignalingState::default(),
            ice_restart_inflight: false,
            ice_restart_attempts: 0,
            restart_task_handle: None,
            restart_wake: Arc::new(tokio::sync::Notify::new()),
            restart_retry_wake: Arc::new(tokio::sync::Notify::new()),
            last_ice_restart_offer_at: None,
            last_state_change: std::time::Instant::now(),
            current_state: RTCPeerConnectionState::New,
            ever_ice_connected: false,
            ever_data_channel_opened: false,
            sendable_hook_reported: false,
            unavailable_hook_reported: false,
            public_hook_state: PublicRtcHookState::Unknown,
            session_id: webrtc_conn.session_id(),
            receive_handles: Vec::new(),
        };
        if let Err(peer_state) = self
            .insert_peer_if_lifecycle_current(target.clone(), peer_state, peer_lifecycle_epoch)
            .await
        {
            self.teardown_removed_peer_state(
                target,
                peer_state,
                false,
                "peer lifecycle changed during offerer setup",
            )
            .await;
            return Err(ActrError::Unavailable(format!(
                "WebRTC peer lifecycle changed during connection setup: {target}"
            )));
        }
        tracing::debug!(
            "🔒 Inserted placeholder peer state for {} (offerer)",
            target
        );

        // 3. Pre-create negotiated DataChannel for Reliable to trigger ICE gathering
        let _reliable_lane = webrtc_conn
            .get_lane(actr_protocol::PayloadType::RpcReliable)
            .await?;
        tracing::debug!("Pre-created Reliable DataChannel for ICE gathering");

        // 4. Register on_track callback for receiving MediaTrack (WebRTC native media)
        let media_registry = Arc::clone(&self.media_frame_registry);
        let sender_id = target.clone();
        peer_connection_arc.on_track(Box::new(move |track, _receiver, _transceiver| {
            let media_registry = Arc::clone(&media_registry);
            let sender_id = sender_id.clone();

            // Extract codec and media type from track metadata before spawning
            let track_codec = track.codec();
            let codec_name = track_codec
                .capability
                .mime_type
                .split('/')
                .next_back()
                .unwrap_or("unknown")
                .to_uppercase();
            let media_type = match track.kind() {
                webrtc::rtp_transceiver::rtp_codec::RTPCodecType::Audio => {
                    actr_framework::MediaType::Audio
                }
                _ => actr_framework::MediaType::Video,
            };

            Box::pin(async move {
                let track_id = track.id();
                tracing::info!(
                    "📹 Received MediaTrack: track_id={}, codec={}, media_type={:?}, sender={}",
                    track_id,
                    codec_name,
                    media_type,
                    sender_id
                );

                let codec_name = codec_name.clone();
                let media_type = media_type;
                tokio::spawn(async move {
                    loop {
                        match track.read_rtp().await {
                            Ok((rtp_packet, _attributes)) => {
                                let payload_data = rtp_packet.payload.clone();
                                let timestamp = rtp_packet.header.timestamp;
                                let sample = actr_framework::MediaSample {
                                    data: payload_data,
                                    timestamp,
                                    codec: codec_name.clone(),
                                    media_type,
                                };
                                media_registry
                                    .dispatch(&track_id, sample, sender_id.clone())
                                    .await;
                            }
                            Err(e) => {
                                tracing::error!(
                                    "❌ Failed to read RTP from track {}: {}",
                                    track_id,
                                    e
                                );
                                break;
                            }
                        }
                    }
                    tracing::info!("🛑 MediaTrack reader task exited for track_id={}", track_id);
                });
            })
        }));

        // 5. Set ICE candidate callback (local ICE candidate collection)
        let coordinator = Arc::downgrade(self);
        let target_id = target.clone();
        let candidate_session_id = webrtc_conn.session_id();
        let candidate_peer_connection = Arc::clone(&peer_connection_arc);
        #[cfg(feature = "opentelemetry")]
        let root_context_map = self.root_context_map.clone();
        peer_connection_arc.on_ice_candidate(Box::new(
            move |candidate: Option<RTCIceCandidate>| {
                let coordinator = coordinator.clone();
                let target_id = target_id.clone();
                let candidate_peer_connection = Arc::clone(&candidate_peer_connection);
                #[cfg(feature = "opentelemetry")]
                let root_context_map = root_context_map.clone();
                Box::pin(async move {
                    if let Some(cand) = candidate {
                        if let Some(coord) = coordinator.upgrade() {
                            if !coord
                                .is_active_session(&target_id, candidate_session_id)
                                .await
                            {
                                tracing::debug!(
                                    "⏭️ Ignoring ICE Candidate from stale local session: peer={}, session_id={}",
                                    target_id,
                                    candidate_session_id
                                );
                                return;
                            }

                            match coord
                                .classify_local_candidate(
                                    &target_id,
                                    candidate_session_id,
                                    &cand,
                                )
                                .await
                            {
                                Ok(LocalCandidateDisposition::Buffered) => {
                                    tracing::debug!(
                                        "🔖 Buffered local ICE candidate until restart SDP is sent: peer={}",
                                        target_id
                                    );
                                    return;
                                }
                                Ok(LocalCandidateDisposition::Suppressed) => {
                                    tracing::debug!(
                                        "⏭️ Suppressed local ICE candidate for uncommitted generation: peer={}",
                                        target_id
                                    );
                                    return;
                                }
                                Ok(LocalCandidateDisposition::StaleSession) => return,
                                Ok(LocalCandidateDisposition::SendNow) => {}
                                Err(e) => {
                                    tracing::error!("❌ Failed to buffer ICE Candidate: {}", e);
                                    return;
                                }
                            }

                            let ice_candidate = match Self::serialize_local_ice_candidate(
                                &candidate_peer_connection,
                                &cand,
                            )
                            .await
                            {
                                Ok(candidate) => candidate,
                                Err(e) => {
                                    tracing::error!("❌ Failed to prepare ICE Candidate: {}", e);
                                    return;
                                }
                            };

                            if !coord
                                .is_active_session(&target_id, candidate_session_id)
                                .await
                            {
                                tracing::debug!(
                                    "⏭️ Ignoring ICE Candidate prepared for stale session: peer={}, session_id={}",
                                    target_id,
                                    candidate_session_id
                                );
                                return;
                            }

                            let payload = actr_relay::Payload::IceCandidate(ice_candidate);

                            // Get root context at callback execution time (not at setup time)
                            #[cfg(feature = "opentelemetry")]
                            let span = {
                                let span = tracing::info_span!(
                                    "send_ice_candidate",
                                    target_id = %target_id
                                );
                                if let Some(ctx) =
                                    root_context_map.read().await.get(&target_id).cloned()
                                {
                                    span.set_parent(ctx);
                                } else {
                                    tracing::warn!(
                                        "⚠️ No root context found for target_id={}",
                                        target_id
                                    );
                                }
                                span
                            };
                            let send_actr_relay_fut = coord.commit_peer_signaling(
                                &target_id,
                                candidate_session_id,
                                None,
                                payload,
                            );
                            #[cfg(feature = "opentelemetry")]
                            let send_actr_relay_fut = send_actr_relay_fut.instrument(span);
                            match send_actr_relay_fut.await {
                                Ok(true) => tracing::debug!("✅ Sent ICE Candidate"),
                                Ok(false) => tracing::debug!(
                                    "⏭️ Skipped ICE Candidate from stale local session: peer={}, session_id={}",
                                    target_id,
                                    candidate_session_id
                                ),
                                Err(e) => {
                                    tracing::error!("❌ Failed to send ICE Candidate: {}", e)
                                }
                            }
                        }
                    } else {
                        tracing::debug!("❌ ICE Candidate is None");
                    }
                })
            },
        ));

        // 6. Create Offer
        let offer_sdp = self.negotiator.create_offer(&peer_connection_arc).await?;
        let sdp_exchange_id = Self::new_envelope_id();
        self.record_pending_local_offer(target, webrtc_conn.session_id(), sdp_exchange_id.clone())
            .await?;

        // 8. Send Offer via signaling server
        let session_desc = actr_protocol::SessionDescription {
            r#type: SdpType::Offer as i32,
            sdp: offer_sdp,
            sdp_exchange_id: Some(sdp_exchange_id.clone()),
        };
        let payload = actr_relay::Payload::SessionDescription(session_desc);
        match self
            .commit_peer_signaling(target, webrtc_conn.session_id(), None, payload)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.clear_pending_local_offer(target, webrtc_conn.session_id(), &sdp_exchange_id)
                    .await;
                return Err(ActrError::Unavailable(format!(
                    "Peer session changed before Offer signaling commit: {target}"
                )));
            }
            Err(err) => {
                self.clear_pending_local_offer(target, webrtc_conn.session_id(), &sdp_exchange_id)
                    .await;
                return Err(err);
            }
        }

        tracing::info!("✅ Sent Offer to {}", target);

        // 10. Start receive loop (receive and aggregate messages from this peer)
        let receive_handles = self
            .start_peer_receive_loop(target.clone(), webrtc_conn.clone())
            .await;

        // Store receive handles only on the session that created them. A
        // replacement Offer may have committed while the loops were starting.
        self.store_receive_handles_if_session_current(
            target,
            webrtc_conn.session_id(),
            receive_handles,
        )
        .await;

        Ok((ready_rx, webrtc_conn))
    }

    /// Handle received Offer (passive side)
    ///
    /// Called when receiving a connection request from another peer.
    /// Supports both initial negotiation and renegotiation.
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(level = "info", skip_all, fields(actr_id = %self.local_id_snapshot(), remote_id = %from))
    )]
    async fn handle_offer(
        self: &Arc<Self>,
        from: &ActrId,
        offer_sdp: String,
        sdp_exchange_id: Option<String>,
    ) -> ActorResult<()> {
        let Some(peer_lifecycle_epoch) = self.capture_peer_lifecycle_epoch().await else {
            tracing::debug!(
                "Ignoring Offer from {} because WebRTC peer shutdown is active",
                from
            );
            return Ok(());
        };
        let Some(sdp_exchange_id) = sdp_exchange_id else {
            tracing::warn!(
                "🚫 Ignoring Offer from {} without sdp_exchange_id correlation",
                from
            );
            return Ok(());
        };

        // ========== PrepareForIncomingOffer: Clean up existing connection if any ==========
        let existing_session_id = {
            let peers = self.peers.read().await;
            peers.get(from).map(|state| state.session_id)
        };

        if let Some(existing_session_id) = existing_session_id {
            tracing::info!(
                "🔄 Existing connection found for serial={}, preparing for new Offer",
                from
            );

            let incoming_offer = RTCSessionDescription::offer(offer_sdp.clone()).map_err(|e| {
                ActrError::Internal(format!("Failed to parse replacement Offer: {e}"))
            })?;

            // Clean up old connection using unified cleanup method
            if !self
                .cleanup_cancelled_connection_for_offer(
                    from,
                    existing_session_id,
                    "replaced by incoming offer",
                    &incoming_offer,
                )
                .await
            {
                tracing::debug!(
                    peer_id = %from,
                    existing_session_id,
                    "Ignoring Offer after the peer session changed during replacement cleanup"
                );
                return Ok(());
            }
        }
        // ========== PrepareForIncomingOffer END ==========

        tracing::info!("📥 Handling Offer from actr_id={}", from);

        // Retrieve remote_fixed from peer negotiation state
        let remote_fixed = {
            let neg = self.peer_negotiation.lock().await;
            neg.get(from).map(|s| s.remote_fixed).unwrap_or(false)
        };

        // 1. Create RTCPeerConnection as Answerer (passive side) - applies advanced parameters
        let peer_connection = self
            .negotiator
            .create_peer_connection(true, remote_fixed)
            .await?;
        let peer_connection_arc = Arc::new(peer_connection);

        // 2. Create WebRtcConnection (shares Arc<RTCPeerConnection>)
        let webrtc_conn = WebRtcConnection::new(
            from.clone(),
            Arc::clone(&peer_connection_arc),
            self.event_broadcaster.sender(),
        );

        // CRITICAL: Insert peer state immediately as a placeholder to prevent race conditions.
        // This prevents ensure_connection from creating a duplicate connection while we're
        // still setting up callbacks and negotiating the connection.
        // The state will be updated later after Answer is sent (step 6).
        let peer_state = PeerState {
            peer_connection: peer_connection_arc.clone(),
            webrtc_conn: webrtc_conn.clone(),
            ready_tx: None,
            is_offerer: false,
            ice_signaling: PeerIceSignalingState::default(),
            ice_restart_inflight: false,
            ice_restart_attempts: 0,
            restart_task_handle: None,
            restart_wake: Arc::new(tokio::sync::Notify::new()),
            restart_retry_wake: Arc::new(tokio::sync::Notify::new()),
            last_ice_restart_offer_at: None,
            last_state_change: std::time::Instant::now(),
            current_state: RTCPeerConnectionState::New,
            ever_ice_connected: false,
            ever_data_channel_opened: false,
            sendable_hook_reported: false,
            unavailable_hook_reported: false,
            public_hook_state: PublicRtcHookState::Unknown,
            session_id: webrtc_conn.session_id(),
            receive_handles: Vec::new(),
        };
        if let Err(peer_state) = self
            .insert_peer_if_lifecycle_current(from.clone(), peer_state, peer_lifecycle_epoch)
            .await
        {
            self.teardown_removed_peer_state(
                from,
                peer_state,
                false,
                "peer lifecycle changed during answerer setup",
            )
            .await;
            tracing::debug!(
                "Ignoring Offer from {} because WebRTC peer shutdown crossed setup",
                from
            );
            return Ok(());
        }
        tracing::debug!("🔒 Inserted placeholder peer state for {} (answerer)", from);

        // 3. Install the shared state handler. On Disconnected/Failed the
        // Offerer creates an ICE restart offer, while the Answerer only sends
        // IceRestartRequest so the Offerer remains the negotiation initiator.
        self.install_peer_state_handler(
            webrtc_conn.clone(),
            Arc::clone(&peer_connection_arc),
            from.clone(),
        );

        // 4. Register on_data_channel handler to reuse negotiated channels created by the offerer
        let conn_for_data_channel = webrtc_conn.clone();

        let from_id_for_data_channel = from.clone();
        let coord_weak_for_state = Arc::downgrade(self);
        let rpc_message_tx = self.rpc_message_tx.clone();
        let reliable_message_tx = self.reliable_message_tx.clone();
        let latency_first_message_tx = self.latency_first_message_tx.clone();
        peer_connection_arc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let conn = conn_for_data_channel.clone();
            let coord_weak = coord_weak_for_state.clone();
            let peer_id = from_id_for_data_channel.clone();
            let rpc_message_tx = rpc_message_tx.clone();
            let reliable_message_tx = reliable_message_tx.clone();
            let latency_first_message_tx = latency_first_message_tx.clone();
            Box::pin(async move {
                let channel_id = dc.id();
                let label = dc.label();
                let dc_for_registration = Arc::clone(&dc);

                let payload_type = PayloadType::from_str_name(label);

                if let Some(coord) = coord_weak.upgrade() {
                    let session_id = conn.session_id();
                    if !coord.is_active_session(&peer_id, session_id).await {
                        tracing::debug!(
                            "⏭️ Ignoring DataChannel from stale session: peer={}, session_id={}, label={}, channel_id={}",
                            peer_id,
                            session_id,
                            label,
                            channel_id
                        );
                        return;
                    }

                    let ready_tx = {
                        let mut neg = coord.peer_negotiation.lock().await;
                        neg.get_mut(&peer_id).and_then(|s| s.ready_tx.take())
                    };
                    if let Some(tx) = ready_tx {
                        tracing::info!(
                            "✅ [Answerer] Connection ready, sending notification for {}",
                            peer_id
                        );
                        let _ = tx.send(());
                    }
                }

                match payload_type {
                    Some(pt) => {
                        let Some(message_tx) = Self::message_tx_for_payload(
                            pt,
                            &rpc_message_tx,
                            &reliable_message_tx,
                            &latency_first_message_tx,
                        ) else {
                            tracing::warn!(
                                "❌ Received unsupported DataChannel label={} id={}",
                                label,
                                channel_id
                            );
                            return;
                        };
                        if let Err(e) = conn
                            .register_received_data_channel(dc_for_registration, pt, message_tx)
                            .await
                        {
                            tracing::warn!(
                                "❌ Failed to register received DataChannel label={} id={}: {}",
                                label,
                                channel_id,
                                e
                            );
                        } else {
                            tracing::debug!(
                                "📨 Registered DataChannel from offerer label={} id={}",
                                label,
                                channel_id
                            );
                        }
                    }
                    None => {
                        tracing::warn!(
                            "❓ Ignoring DataChannel with unmapped id={} label={}",
                            channel_id,
                            label
                        );
                    }
                }
            })
        }));

        // 4. Register on_track callback for receiving MediaTrack (WebRTC native media)
        let media_registry = Arc::clone(&self.media_frame_registry);
        let sender_id = from.clone();
        peer_connection_arc.on_track(Box::new(move |track, _receiver, _transceiver| {
            let media_registry = Arc::clone(&media_registry);
            let sender_id = sender_id.clone();

            // Extract codec and media type from track metadata before spawning
            let track_codec = track.codec();
            let codec_name = track_codec
                .capability
                .mime_type
                .split('/')
                .next_back()
                .unwrap_or("unknown")
                .to_uppercase();
            let media_type = match track.kind() {
                webrtc::rtp_transceiver::rtp_codec::RTPCodecType::Audio => {
                    actr_framework::MediaType::Audio
                }
                _ => actr_framework::MediaType::Video,
            };

            Box::pin(async move {
                let track_id = track.id();
                tracing::info!(
                    "📹 Received MediaTrack: track_id={}, codec={}, media_type={:?}, sender={}",
                    track_id,
                    codec_name,
                    media_type,
                    sender_id
                );

                let codec_name = codec_name.clone();
                let media_type = media_type;
                tokio::spawn(async move {
                    loop {
                        match track.read_rtp().await {
                            Ok((rtp_packet, _attributes)) => {
                                let payload_data = rtp_packet.payload.clone();
                                let timestamp = rtp_packet.header.timestamp;
                                let sample = actr_framework::MediaSample {
                                    data: payload_data,
                                    timestamp,
                                    codec: codec_name.clone(),
                                    media_type,
                                };
                                media_registry
                                    .dispatch(&track_id, sample, sender_id.clone())
                                    .await;
                            }
                            Err(e) => {
                                tracing::error!(
                                    "❌ Failed to read RTP from track {}: {}",
                                    track_id,
                                    e
                                );
                                break;
                            }
                        }
                    }
                    tracing::info!("🛑 MediaTrack reader task exited for track_id={}", track_id);
                });
            })
        }));

        // 5. Set ICE candidate callback (local ICE candidate collection)
        let coordinator = Arc::downgrade(self);
        let target_id = from.clone();
        let candidate_session_id = webrtc_conn.session_id();
        let candidate_peer_connection = Arc::clone(&peer_connection_arc);
        #[cfg(feature = "opentelemetry")]
        let root_context_map = self.root_context_map.clone();
        peer_connection_arc.on_ice_candidate(Box::new(
            move |candidate: Option<RTCIceCandidate>| {
                let coordinator = coordinator.clone();
                let target_id = target_id.clone();
                let candidate_peer_connection = Arc::clone(&candidate_peer_connection);
                #[cfg(feature = "opentelemetry")]
                let root_context_map = root_context_map.clone();
                Box::pin(async move {
                    if let Some(cand) = candidate {
                        if let Some(coord) = coordinator.upgrade() {
                            if !coord
                                .is_active_session(&target_id, candidate_session_id)
                                .await
                            {
                                tracing::debug!(
                                    "⏭️ Ignoring ICE Candidate from stale local session: peer={}, session_id={}",
                                    target_id,
                                    candidate_session_id
                                );
                                return;
                            }

                            match coord
                                .classify_local_candidate(
                                    &target_id,
                                    candidate_session_id,
                                    &cand,
                                )
                                .await
                            {
                                Ok(LocalCandidateDisposition::Buffered) => {
                                    tracing::debug!(
                                        "🔖 Buffered local ICE candidate until restart SDP is sent: peer={}",
                                        target_id
                                    );
                                    return;
                                }
                                Ok(LocalCandidateDisposition::Suppressed) => {
                                    tracing::debug!(
                                        "⏭️ Suppressed local ICE candidate for uncommitted generation: peer={}",
                                        target_id
                                    );
                                    return;
                                }
                                Ok(LocalCandidateDisposition::StaleSession) => return,
                                Ok(LocalCandidateDisposition::SendNow) => {}
                                Err(e) => {
                                    tracing::error!("❌ Failed to buffer ICE Candidate: {}", e);
                                    return;
                                }
                            }

                            let ice_candidate = match Self::serialize_local_ice_candidate(
                                &candidate_peer_connection,
                                &cand,
                            )
                            .await
                            {
                                Ok(candidate) => candidate,
                                Err(e) => {
                                    tracing::error!("❌ Failed to prepare ICE Candidate: {}", e);
                                    return;
                                }
                            };

                            if !coord
                                .is_active_session(&target_id, candidate_session_id)
                                .await
                            {
                                tracing::debug!(
                                    "⏭️ Ignoring ICE Candidate prepared for stale session: peer={}, session_id={}",
                                    target_id,
                                    candidate_session_id
                                );
                                return;
                            }

                            let payload = actr_relay::Payload::IceCandidate(ice_candidate);

                            // Get root context at callback execution time (not at setup time)
                            #[cfg(feature = "opentelemetry")]
                            let span = {
                                let span = tracing::info_span!(
                                    "send_ice_candidate",
                                    target_id = %target_id
                                );
                                if let Some(ctx) =
                                    root_context_map.read().await.get(&target_id).cloned()
                                {
                                    span.set_parent(ctx);
                                } else {
                                    tracing::warn!(
                                        "⚠️ No root context found for target_id={}",
                                        target_id
                                    );
                                }
                                span
                            };
                            let send_actr_relay_fut = coord.commit_peer_signaling(
                                &target_id,
                                candidate_session_id,
                                None,
                                payload,
                            );
                            #[cfg(feature = "opentelemetry")]
                            let send_actr_relay_fut = send_actr_relay_fut.instrument(span);
                            match send_actr_relay_fut.await {
                                Ok(true) => tracing::debug!(
                                    "🔄 Handle offer Sent ICE Candidate to serial={}",
                                    target_id
                                ),
                                Ok(false) => tracing::debug!(
                                    "⏭️ Skipped ICE Candidate from stale local session: peer={}, session_id={}",
                                    target_id,
                                    candidate_session_id
                                ),
                                Err(e) => {
                                    tracing::error!("❌ Failed to send ICE Candidate: {}", e)
                                }
                            }
                        }
                    }
                })
            },
        ));

        // 5. Create Answer
        let answer_sdp = self
            .negotiator
            .create_answer(&peer_connection_arc, offer_sdp)
            .await?;

        // 7. Send Answer via signaling server
        let session_desc = actr_protocol::SessionDescription {
            r#type: SdpType::Answer as i32,
            sdp: answer_sdp,
            sdp_exchange_id: Some(sdp_exchange_id),
        };
        let payload = actr_relay::Payload::SessionDescription(session_desc);
        if !self
            .commit_peer_signaling(from, webrtc_conn.session_id(), None, payload)
            .await?
        {
            tracing::debug!(
                "⏭️ Skipped Answer for stale peer session: peer={}, session_id={}",
                from,
                webrtc_conn.session_id()
            );
            return Ok(());
        }

        tracing::info!("✅ Sent Answer to {}", from);

        // 8. Flush any buffered ICE candidates (remote description is now set)
        self.flush_pending_candidates(from, webrtc_conn.session_id(), &peer_connection_arc)
            .await?;

        // Note: ready notification is sent in on_data_channel callback
        // when DataChannel is actually registered (see above)

        Ok(())
    }

    /// Handle received Answer (initiator side)
    ///
    /// Supports both initial negotiation and renegotiation answers.
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            level = "info",
            skip_all,
            fields(
                remote.id = %from,
                answer_len = answer_sdp.len()
            )
        )
    )]
    async fn handle_answer(
        self: &Arc<Self>,
        from: &ActrId,
        answer_sdp: String,
        sdp_exchange_id: Option<String>,
    ) -> ActorResult<()> {
        let Some(sdp_exchange_id) = sdp_exchange_id.as_deref() else {
            tracing::warn!(
                "🚫 Ignoring Answer from {} without sdp_exchange_id correlation",
                from
            );
            return Ok(());
        };

        // Get corresponding PeerConnection and ready_tx only after the Answer
        // is proven to belong to the current local Offer. A stale Answer must
        // not consume ready_tx or mutate the PeerConnection.
        let (peer_connection, ready_tx, webrtc_conn, is_renegotiation, session_id, offer_id) = {
            let mut peers = self.peers.write().await;
            tracing::info!(
                "🔍 [LOOKUP] Searching for: id={}, total peers={}",
                from,
                peers.len()
            );
            for k in peers.keys() {
                tracing::info!("   📌 [LOOKUP] Stored: id={}", k);
            }
            let state = peers.get_mut(from).ok_or_else(|| {
                ActrError::Internal(format!("Peer not found: {}", from.to_string_repr()))
            })?;

            let Some(pending_sdp_exchange_id) =
                state.ice_signaling.pending_local_sdp_exchange_id.as_ref()
            else {
                tracing::warn!(
                    "🚫 Ignoring Answer from {} because no local Offer is pending",
                    from
                );
                return Ok(());
            };

            let pending_sdp_exchange_id = pending_sdp_exchange_id.clone();

            if pending_sdp_exchange_id != sdp_exchange_id {
                tracing::warn!(
                    "🚫 Ignoring stale Answer from {}: sdp_exchange_id={} current_exchange={}",
                    from,
                    sdp_exchange_id,
                    pending_sdp_exchange_id
                );
                return Ok(());
            }

            let pc = state.peer_connection.clone();
            let tx = state.ready_tx.take();
            let wc = state.webrtc_conn.clone();
            let is_reneg = tx.is_none(); // If ready_tx already taken, this is renegotiation
            (
                pc,
                tx,
                wc,
                is_reneg,
                state.session_id,
                pending_sdp_exchange_id,
            )
        };

        if is_renegotiation {
            tracing::info!("🔄 Handling renegotiation Answer from {}", from);
        } else {
            tracing::info!("📥 Handling initial Answer from {}", from);
        }

        // Handle Answer (set remote SDP)
        if let Err(e) = self
            .negotiator
            .handle_answer(&peer_connection, answer_sdp)
            .await
        {
            self.clear_pending_local_offer(from, session_id, &offer_id)
                .await;
            return Err(e.into());
        }

        self.clear_pending_local_offer(from, session_id, &offer_id)
            .await;

        // Flush any buffered ICE candidates (remote description is now set)
        self.flush_pending_candidates(from, session_id, &peer_connection)
            .await?;

        tracing::info!("✅ WebRTC connection negotiation completed: {}", from);

        // Wait for ICE and DataChannel to be ready before reporting the connection ready.
        let peers = Arc::clone(&self.peers);
        let from_id = from.clone();
        let webrtc_conn_for_wait = webrtc_conn.clone();
        let wait_session_id = webrtc_conn.session_id();
        let event_broadcaster = self.event_broadcaster.clone();

        tokio::spawn(async move {
            let opened = Self::wait_for_connection_ready_event(
                &from_id,
                wait_session_id,
                &peers,
                &event_broadcaster,
                &webrtc_conn_for_wait,
                ICE_CONNECTED_TIMEOUT,
                DATA_CHANNEL_AFTER_ICE_TIMEOUT,
            )
            .await;

            if opened {
                tracing::info!("✅ DataChannel verified open, connection fully ready");

                // Mark ICE restart attempt complete
                let mut completed_restart = false;
                let mut peers_guard = peers.write().await;
                if let Some(s) = peers_guard.get_mut(&from_id) {
                    if s.session_id == wait_session_id {
                        s.mark_data_channel_opened();
                        completed_restart = s.ice_restart_inflight;
                        s.ice_restart_inflight = false;
                        s.ice_restart_attempts = 0;
                    }
                }
                drop(peers_guard);

                if completed_restart {
                    event_broadcaster.send(ConnectionEvent::IceRestartCompleted {
                        peer_id: from_id.clone(),
                        session_id: wait_session_id,
                        success: true,
                    });
                }

                if let Some(tx) = ready_tx {
                    let _ = tx.send(());
                }
            } else {
                tracing::warn!(
                    "⚠️ Connection did not become ready within staged timeout for peer {}, session_id={}",
                    from_id,
                    wait_session_id
                );
            }
        });

        Ok(())
    }

    /// Flush buffered ICE candidates for a peer
    ///
    /// Called after remote description is set, to add any candidates that arrived early
    async fn flush_pending_candidates(
        &self,
        peer_id: &ActrId,
        expected_session_id: u64,
        peer_connection: &RTCPeerConnection,
    ) -> ActorResult<()> {
        match tokio::time::timeout(
            REMOTE_CANDIDATE_FLUSH_TIMEOUT,
            self.flush_pending_candidates_inner(peer_id, expected_session_id, peer_connection),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!(
                    peer_id = %peer_id,
                    expected_session_id,
                    timeout_ms = REMOTE_CANDIDATE_FLUSH_TIMEOUT.as_millis(),
                    "Timed out flushing remote ICE candidates"
                );
                Err(ActrError::TimedOut)
            }
        }
    }

    async fn flush_pending_candidates_inner(
        &self,
        peer_id: &ActrId,
        expected_session_id: u64,
        peer_connection: &RTCPeerConnection,
    ) -> ActorResult<()> {
        let remote_description = peer_connection.remote_description().await.ok_or_else(|| {
            ActrError::Internal(format!(
                "Cannot flush ICE candidates for {peer_id}: remote description is missing"
            ))
        })?;

        // Classify the complete buffer and retain unknown future generations
        // in one session-guarded state commit. The pending-candidate lock is
        // acquired before `peers`, matching the close-all lock order. Only
        // candidates ready to apply leave shared state before an await.
        let candidates_to_apply = {
            let mut pending = self.pending_candidates.write().await;
            let mut peers = self.peers.write().await;
            let Some(state) = peers.get_mut(peer_id) else {
                tracing::debug!(
                    "⏭️ Skip pending ICE flush for removed peer={}, expected_session_id={}",
                    peer_id,
                    expected_session_id
                );
                return Ok(());
            };
            if state.session_id != expected_session_id {
                tracing::debug!(
                    "⏭️ Skip stale pending ICE flush for peer={}: active_session_id={} expected_session_id={}",
                    peer_id,
                    state.session_id,
                    expected_session_id
                );
                return Ok(());
            }

            for current_ufrag in ice_ufrags_from_description(&remote_description) {
                state.ice_signaling.remember_remote_ufrag(current_ufrag);
            }
            let known_remote_ufrags = state.ice_signaling.known_remote_ufrags.clone();
            let candidates = pending.remove(peer_id).unwrap_or_default();
            tracing::debug!(
                "🔄 Flushing {} buffered ICE candidates for {:?}",
                candidates.len(),
                peer_id
            );

            let mut candidates_to_apply = Vec::new();
            let mut future_generation_candidates = Vec::new();
            for candidate in candidates {
                let Some(candidate_ufrag) = nonempty_candidate_ufrag(&candidate) else {
                    tracing::warn!(
                        "🚫 Dropping buffered ICE candidate from {} without username_fragment",
                        peer_id
                    );
                    continue;
                };
                let Some(current_ufrag) = ice_ufrag_from_description(
                    &remote_description,
                    candidate.sdp_mid.as_deref(),
                    candidate.sdp_mline_index,
                ) else {
                    tracing::debug!(
                        "🔖 Retaining buffered ICE candidate until its media generation is known: peer={}, candidate_ufrag={}",
                        peer_id,
                        candidate_ufrag
                    );
                    future_generation_candidates.push(candidate);
                    continue;
                };

                match classify_remote_candidate_ufrag(
                    candidate_ufrag,
                    &current_ufrag,
                    &known_remote_ufrags,
                ) {
                    RemoteCandidateDisposition::Apply => {
                        candidates_to_apply.push(candidate);
                    }
                    RemoteCandidateDisposition::DropStale => {
                        tracing::debug!(
                            "🗑️ Dropping buffered ICE candidate from known stale generation: peer={}, candidate_ufrag={}, current_ufrag={}",
                            peer_id,
                            candidate_ufrag,
                            current_ufrag
                        );
                    }
                    RemoteCandidateDisposition::BufferFuture => {
                        tracing::debug!(
                            "🔖 Retaining buffered ICE candidate for an unknown future generation: peer={}, candidate_ufrag={}, current_ufrag={}",
                            peer_id,
                            candidate_ufrag,
                            current_ufrag
                        );
                        future_generation_candidates.push(candidate);
                    }
                }
            }

            if !future_generation_candidates.is_empty() {
                pending.insert(peer_id.clone(), future_generation_candidates);
            }

            candidates_to_apply
        };

        for candidate in candidates_to_apply {
            if let Err(e) = self
                .negotiator
                .add_ice_candidate(peer_connection, candidate)
                .await
            {
                tracing::warn!("⚠️ Failed to add buffered ICE candidate: {}", e);
            }
        }

        Ok(())
    }

    async fn buffer_pending_remote_candidate_if_lifecycle_current(
        &self,
        peer_id: &ActrId,
        candidate: IceCandidate,
        expected_lifecycle_epoch: u64,
    ) -> bool {
        let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
        if self.peer_signaling.closing_all.load(Ordering::Acquire)
            || self
                .peer_signaling
                .peer_lifecycle_epoch
                .load(Ordering::Acquire)
                != expected_lifecycle_epoch
        {
            return false;
        }
        let mut pending = self.pending_candidates.write().await;
        Self::push_pending_remote_candidate(&mut pending, peer_id, candidate);
        true
    }

    async fn buffer_pending_remote_candidate_if_session_current(
        &self,
        peer_id: &ActrId,
        expected_session_id: u64,
        candidate: IceCandidate,
    ) -> bool {
        let mut pending = self.pending_candidates.write().await;
        let peers = self.peers.read().await;
        if peers
            .get(peer_id)
            .is_none_or(|state| state.session_id != expected_session_id)
        {
            tracing::debug!(
                "⏭️ Discarding buffered ICE candidate after peer session changed: peer={}, expected_session_id={}",
                peer_id,
                expected_session_id
            );
            return false;
        }

        Self::push_pending_remote_candidate(&mut pending, peer_id, candidate);
        true
    }

    fn push_pending_remote_candidate(
        pending: &mut HashMap<ActrId, Vec<IceCandidate>>,
        peer_id: &ActrId,
        candidate: IceCandidate,
    ) {
        let candidates = pending.entry(peer_id.clone()).or_default();
        if candidates.len() >= MAX_PENDING_ICE_CANDIDATES_PER_PEER {
            candidates.remove(0);
            tracing::warn!(
                "⚠️ Remote ICE candidate buffer full for {}; dropping oldest candidate",
                peer_id
            );
        }
        candidates.push(candidate);
    }

    /// Handle received ICE Candidate
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            level = "trace",
            skip_all,
            fields(
                remote.id = %from,
                candidate_len = candidate.candidate.len(),
                candidate_ufrag = ?candidate.username_fragment
            )
        )
    )]
    async fn handle_ice_candidate(
        self: &Arc<Self>,
        from: &ActrId,
        candidate: IceCandidate,
    ) -> ActorResult<()> {
        tracing::trace!("📥 Received ICE Candidate from {}", from);

        let Some(peer_lifecycle_epoch) = self.capture_peer_lifecycle_epoch().await else {
            tracing::debug!(
                peer_id = %from,
                "Ignoring ICE candidate while WebRTC peer shutdown is active"
            );
            return Ok(());
        };

        let Some(candidate_ufrag) = nonempty_candidate_ufrag(&candidate).map(str::to_owned) else {
            tracing::warn!(
                "🚫 Dropping ICE candidate from {} without username_fragment",
                from
            );
            return Ok(());
        };

        // DEBUG: Temporarily disable candidate filtering for local testing
        // TODO: Re-enable proper filtering for production
        // if !is_ipv4_candidate_allowed(&candidate) {
        //     tracing::debug!("🚫 Ignoring ICE candidate from {:?}: {}", from, candidate);
        //     return Ok(());
        // }

        // Try to get peer and check if remote description is set
        let peer_opt = {
            let peers = self.peers.read().await;
            peers.get(from).map(|state| {
                (
                    state.peer_connection.clone(),
                    state.session_id,
                    state.ice_signaling.known_remote_ufrags.clone(),
                )
            })
        };

        match peer_opt {
            Some((peer_connection, session_id, known_remote_ufrags)) => {
                // Check if remote description is set
                if let Some(remote_description) = peer_connection.remote_description().await {
                    let Some(current_ufrag) = ice_ufrag_from_description(
                        &remote_description,
                        candidate.sdp_mid.as_deref(),
                        candidate.sdp_mline_index,
                    ) else {
                        tracing::debug!(
                            "🔖 Buffering ICE candidate until its media generation is known: peer={}, candidate_ufrag={}",
                            from,
                            candidate_ufrag
                        );
                        self.buffer_pending_remote_candidate_if_session_current(
                            from, session_id, candidate,
                        )
                        .await;
                        return Ok(());
                    };

                    match classify_remote_candidate_ufrag(
                        &candidate_ufrag,
                        &current_ufrag,
                        &known_remote_ufrags,
                    ) {
                        RemoteCandidateDisposition::Apply => {
                            match tokio::time::timeout(
                                REMOTE_CANDIDATE_FLUSH_TIMEOUT,
                                self.negotiator
                                    .add_ice_candidate(&peer_connection, candidate),
                            )
                            .await
                            {
                                Ok(result) => result?,
                                Err(_) => {
                                    tracing::warn!(
                                        peer_id = %from,
                                        session_id,
                                        timeout_ms = REMOTE_CANDIDATE_FLUSH_TIMEOUT.as_millis(),
                                        "Timed out adding remote ICE candidate"
                                    );
                                    return Err(ActrError::TimedOut);
                                }
                            }
                            tracing::trace!("✅ Added ICE Candidate from {}", from);
                        }
                        RemoteCandidateDisposition::DropStale => {
                            tracing::debug!(
                                "🗑️ Dropping ICE candidate from known stale generation: peer={}, candidate_ufrag={}, current_ufrag={}",
                                from,
                                candidate_ufrag,
                                current_ufrag
                            );
                        }
                        RemoteCandidateDisposition::BufferFuture => {
                            tracing::debug!(
                                "🔖 Buffering ICE candidate from an unknown future generation: peer={}, candidate_ufrag={}, current_ufrag={}",
                                from,
                                candidate_ufrag,
                                current_ufrag
                            );
                            self.buffer_pending_remote_candidate_if_session_current(
                                from, session_id, candidate,
                            )
                            .await;
                        }
                    }
                } else {
                    // Buffer for later (remote description not yet set)
                    if self
                        .buffer_pending_remote_candidate_if_session_current(
                            from, session_id, candidate,
                        )
                        .await
                    {
                        tracing::debug!(
                            "🔖 Buffered ICE candidate from {:?} (remote description not yet set)",
                            from
                        );
                    }
                }
            }
            None => {
                // Buffer for when peer is created
                if self
                    .buffer_pending_remote_candidate_if_lifecycle_current(
                        from,
                        candidate,
                        peer_lifecycle_epoch,
                    )
                    .await
                {
                    tracing::debug!(
                        "🔖 Buffered ICE candidate from {:?} (peer not yet created)",
                        from
                    );
                } else {
                    tracing::debug!(
                        peer_id = %from,
                        "Discarding ICE candidate after peer lifecycle changed"
                    );
                }
            }
        }

        Ok(())
    }

    /// Start peer receive loop
    ///
    /// Starts a background task for each peer to receive messages from WebRtcConnection and aggregate by traffic class.
    ///
    /// IMPORTANT: We need to listen to ALL PayloadTypes, not just RpcReliable:
    /// - RpcReliable, RpcSignal: for RPC messages
    /// - StreamReliable, StreamLatencyFirst: for DataChunk messages
    async fn start_peer_receive_loop(
        &self,
        peer_id: ActrId,
        webrtc_conn: WebRtcConnection,
    ) -> Vec<JoinHandle<()>> {
        let rpc_message_tx = self.rpc_message_tx.clone();
        let reliable_message_tx = self.reliable_message_tx.clone();
        let latency_first_message_tx = self.latency_first_message_tx.clone();
        let mut handles = Vec::new();

        // Listen to all relevant PayloadTypes
        let payload_types = vec![
            PayloadType::RpcReliable,
            PayloadType::RpcSignal,
            PayloadType::StreamReliable,
            PayloadType::StreamLatencyFirst,
        ];

        for payload_type in payload_types {
            let Some(message_tx_clone) = Self::message_tx_for_payload(
                payload_type,
                &rpc_message_tx,
                &reliable_message_tx,
                &latency_first_message_tx,
            ) else {
                continue;
            };
            let peer_id_clone = peer_id.clone();
            let webrtc_conn_clone = webrtc_conn.clone();

            let handle = tokio::spawn(async move {
                tracing::debug!(
                    "📡 Starting receive loop for peer {:?}, PayloadType: {:?}",
                    peer_id_clone,
                    payload_type
                );

                // Get Lane for this PayloadType
                let lane = match webrtc_conn_clone.get_lane(payload_type).await {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!(
                            "❌ Failed to get Lane for {:?}, PayloadType {:?}: {}",
                            peer_id_clone,
                            payload_type,
                            e
                        );
                        return;
                    }
                };

                // Continuously receive messages
                loop {
                    match lane.recv().await {
                        Ok(data) => {
                            tracing::debug!(
                                "📨 Received message from {:?} (PayloadType: {:?}): {} bytes",
                                peer_id_clone,
                                payload_type,
                                data.len()
                            );

                            // Serialize peer_id as bytes
                            let peer_id_bytes = peer_id_clone.encode_to_vec();

                            // Send to the traffic-class aggregation channel (include PayloadType).
                            // Stream sends are bounded so reliable stream backpressure can reach
                            // the lane reader instead of growing an unbounded coordinator queue.
                            if let Err(e) = message_tx_clone
                                .send((peer_id_bytes, data, payload_type))
                                .await
                            {
                                tracing::error!("❌ Message aggregation failed: {:?}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "❌ Peer {:?} message receive failed (PayloadType: {:?}): {}",
                                peer_id_clone,
                                payload_type,
                                e
                            );
                            break;
                        }
                    }
                }

                tracing::debug!(
                    "📡 Receive loop exited for peer {:?}, PayloadType: {:?}",
                    peer_id_clone,
                    payload_type
                );
            });
            handles.push(handle);
        }
        handles
    }

    /// Send message to specified peer
    ///
    /// If connection doesn't exist, automatically initiates WebRTC connection and waits for it to be ready.
    /// Supports retry with exponential backoff on transient errors.
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(skip_all, fields(actr_id = %self.local_id_snapshot(), target_id = %target))
    )]
    pub(crate) async fn send_message(
        self: &Arc<Self>,
        target: &ActrId,
        data: &[u8],
    ) -> ActorResult<()> {
        const MAX_RETRIES: u32 = 3;
        const OVERALL_TIMEOUT: Duration = Duration::from_secs(30);

        tracing::debug!("📤 Sending message to {:?}: {} bytes", target, data.len());

        // Wrap entire operation with overall timeout
        let result = tokio::time::timeout(
            OVERALL_TIMEOUT,
            self.send_message_with_retry(target, data, MAX_RETRIES),
        )
        .await;

        match result {
            Ok(inner_result) => inner_result,
            Err(_) => {
                tracing::error!(
                    "⏰ Overall timeout ({}s) exceeded for send_message to {}",
                    OVERALL_TIMEOUT.as_secs(),
                    target
                );
                self.cleanup_cancelled_connection(target, "send_message overall timeout")
                    .await;
                Err(ActrError::TimedOut)
            }
        }
    }

    /// Inner implementation of send_message with retry logic
    async fn send_message_with_retry(
        self: &Arc<Self>,
        target: &ActrId,
        data: &[u8],
        max_retries: u32,
    ) -> ActorResult<()> {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(1), // initial delay
            Duration::from_secs(10),  // max delay
            None,                     // no limit (we control manually)
        );

        let mut last_error = None;

        for attempt in 0..=max_retries {
            // Wait before retry (skip first attempt)
            if attempt > 0 {
                let delay = backoff.next().unwrap_or(Duration::from_secs(5));
                tracing::info!(
                    "🔄 Retrying send_message to {} (attempt {}/{}, delay {:?})",
                    target,
                    attempt + 1,
                    max_retries + 1,
                    delay
                );
                tokio::time::sleep(delay).await;
            }

            match self.try_send_message_once(target, data).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Only retry on transient errors
                    let should_retry = matches!(&e, ActrError::TimedOut | ActrError::Internal(_));

                    if !should_retry {
                        return Err(e);
                    }

                    tracing::warn!(
                        "⚠️ send_message attempt {}/{} failed: {}",
                        attempt + 1,
                        max_retries + 1,
                        e
                    );
                    last_error = Some(e);

                    // Cleanup connection before retry (might be stale)
                    self.cleanup_cancelled_connection(target, "send_message retry cleanup")
                        .await;
                }
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| {
            ActrError::Internal("send_message failed after all retries".to_string())
        }))
    }

    /// Single attempt to send a message
    async fn try_send_message_once(
        self: &Arc<Self>,
        target: &ActrId,
        data: &[u8],
    ) -> ActorResult<()> {
        self.ensure_connected(target).await?;

        // Get corresponding WebRtcConnection
        let webrtc_conn = {
            let peers = self.peers.read().await;
            peers
                .get(target)
                .map(|state| state.webrtc_conn.clone())
                .ok_or_else(|| {
                    ActrError::Internal(format!("Peer connection not found: {target:?}"))
                })?
        };

        // Get Reliable Lane
        let lane = webrtc_conn
            .get_lane(PayloadType::RpcReliable)
            .await
            .map_err(|e| ActrError::Internal(format!("Failed to get Lane: {e}")))?;

        // Send message (convert to Bytes)
        lane.send(Bytes::copy_from_slice(data))
            .await
            .map_err(|e| ActrError::Internal(format!("Failed to send message: {e}")))?;

        Ok(())
    }

    async fn ensure_connected(self: &Arc<Self>, target: &ActrId) -> ActorResult<()> {
        // Check if connection exists or is being established
        let has_connection = loop {
            let state = {
                let peers = self.peers.read().await;
                peers
                    .get(target)
                    .map(|s| (s.current_state, s.last_state_change))
            };

            match state {
                Some((
                    RTCPeerConnectionState::New | RTCPeerConnectionState::Connecting,
                    started,
                )) => {
                    // Connection is being established, check if it's still fresh
                    if started.elapsed() < INITIAL_CONNECTION_TIMEOUT {
                        // Wait a bit and check again
                        tracing::debug!(
                            "⏳ Connection to {} is being established, waiting...",
                            target
                        );
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    } else {
                        // Connecting timeout, treat as not connected
                        tracing::warn!("⏰ Connection to {} timed out while connecting", target);
                        break false;
                    }
                }
                Some((RTCPeerConnectionState::Connected, _)) => {
                    // Connection exists and is ready
                    break true;
                }
                Some(_) => {
                    // Connection exists but in other state (Disconnected/Failed/Closed)
                    // Let initiate_connection handle it
                    break false;
                }
                None => {
                    // No connection exists
                    break false;
                }
            }
        };

        #[cfg(feature = "opentelemetry")]
        let _ = self
            .root_context_map
            .write()
            .await
            .insert(target.clone(), tracing::Span::current().context());

        // If connection doesn't exist, initiate connection
        if !has_connection {
            tracing::info!(
                "🔗 First send to {:?}, initiating role negotiation + WebRTC connection",
                target
            );

            let ready_rx = self.initiate_connection(target).await?;
            tracing::debug!(?ready_rx, "ready_rx");

            // Wait for connection to be ready within the per-attempt initial connection budget.
            match tokio::time::timeout(INITIAL_CONNECTION_TIMEOUT, ready_rx).await {
                Ok(Ok(())) => {
                    tracing::info!("✅ WebRTC connection ready: {}", target);
                }
                Ok(Err(_)) => {
                    return Err(ActrError::Internal(
                        "Connection establishment failed (channel closed)".to_string(),
                    ));
                }
                Err(_) => {
                    return Err(ActrError::TimedOut);
                }
            }
        }

        Ok(())
    }

    /// Receive an RPC message (aggregated from all peers).
    ///
    /// Returns: Option<(sender_id_bytes, message_data, payload_type)>.
    pub async fn receive_rpc_message(&self) -> ActorResult<Option<WebRtcInboundMessage>> {
        let mut rx = self.rpc_message_rx.lock().await;
        Ok(rx.recv().await)
    }

    /// Receive a reliable stream message (aggregated from all peers).
    ///
    /// Reliable stream messages have their own queue and receive loop, split
    /// from RPC and from LatencyFirst, so StreamReliable backpressure can
    /// starve neither RPC delivery nor LatencyFirst drop-newest handling.
    pub async fn receive_reliable_message(&self) -> ActorResult<Option<WebRtcInboundMessage>> {
        let mut rx = self.reliable_message_rx.lock().await;
        Ok(rx.recv().await)
    }

    /// Receive a LatencyFirst stream message (aggregated from all peers).
    ///
    /// Isolated from Reliable so a backpressured reliable stream cannot stall
    /// LatencyFirst chunks upstream of the registry's drop-newest policy.
    pub async fn receive_latency_first_message(&self) -> ActorResult<Option<WebRtcInboundMessage>> {
        let mut rx = self.latency_first_message_rx.lock().await;
        Ok(rx.recv().await)
    }

    /// Compatibility receive path for existing tests/tools that do not care
    /// about traffic-class isolation.
    #[allow(dead_code)]
    pub async fn receive_message(&self) -> ActorResult<Option<WebRtcInboundMessage>> {
        tokio::select! {
            rpc = self.receive_rpc_message() => rpc,
            reliable = self.receive_reliable_message() => reliable,
            latency_first = self.receive_latency_first_message() => latency_first,
        }
    }

    /// Create WebRTC connection (factory method)
    ///
    /// For ConnectionFactory, creates a WebRTC connection to the specified Dest.
    /// If connection already exists, returns it directly; otherwise initiates new connection and waits for it to be ready.
    /// Supports retry with exponential backoff on timeout or channel errors.
    /// The entire method has a 30-second overall timeout.
    ///
    /// # Arguments
    /// - `dest`: destination (must be Peer type)
    /// - `cancel_token`: optional cancellation token to terminate the operation
    ///
    /// # Returns
    /// - `Ok(WebRtcConnection)`: ready WebRTC connection
    /// - `Err`: WebRTC only supports Peer targets, connection cancelled, or connection establishment failed
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(skip_all, fields(actr_id = %self.local_id_snapshot(), target_id = ?dest.as_peer_id().map(|id| id)))
    )]
    pub(crate) async fn create_connection(
        self: &Arc<Self>,
        dest: &crate::transport::Dest,
        cancel_token: Option<CancellationToken>,
    ) -> ActorResult<WebRtcConnection> {
        // Overall timeout for the entire create_connection operation
        const OVERALL_TIMEOUT: Duration = Duration::from_secs(30);

        // Extract target_id first (before timeout wrapper) for cleanup
        let target_id = dest.as_peer_id().ok_or_else(|| {
            ActrError::InvalidArgument("WebRTC only supports Peer targets, not Host".to_string())
        })?;

        // Wrap the entire operation with overall timeout
        let result = tokio::time::timeout(
            OVERALL_TIMEOUT,
            self.create_connection_inner(dest, cancel_token.clone()),
        )
        .await;

        match result {
            Ok(inner_result) => inner_result,
            Err(_) => {
                // Overall timeout exceeded
                tracing::error!(
                    "⏰ [Factory] Overall timeout ({}s) exceeded for connection to {}",
                    OVERALL_TIMEOUT.as_secs(),
                    target_id
                );
                self.cleanup_cancelled_connection(target_id, "connection factory overall timeout")
                    .await;
                Err(ActrError::TimedOut)
            }
        }
    }

    /// Inner implementation of create_connection without overall timeout
    async fn create_connection_inner(
        self: &Arc<Self>,
        dest: &crate::transport::Dest,
        cancel_token: Option<CancellationToken>,
    ) -> ActorResult<WebRtcConnection> {
        // Check cancellation at entry
        if let Some(ref token) = cancel_token {
            if token.is_cancelled() {
                return Err(ActrError::Internal(
                    "Connection creation cancelled before starting".to_string(),
                ));
            }
        }

        // 1. Check if dest is Peer
        let target_id = dest.as_peer_id().ok_or_else(|| {
            ActrError::InvalidArgument("WebRTC only supports Peer targets, not Host".to_string())
        })?;

        tracing::debug!("🏭 [Factory] Creating WebRTC connection to {:?}", target_id);

        // 2. Check if connection already exists
        {
            let peers = self.peers.read().await;
            if let Some(state) = peers.get(target_id) {
                tracing::debug!(
                    "♻️ [Factory] Reusing existing WebRTC connection: {:?}",
                    target_id
                );
                return Ok(state.webrtc_conn.clone());
            }
        }

        // 3. Retry loop with exponential backoff (max 3 retries)
        const MAX_RETRIES: u32 = 3;
        let mut backoff = ExponentialBackoff::new(
            CONNECTION_FACTORY_INITIAL_RETRY_DELAY,
            CONNECTION_FACTORY_MAX_RETRY_DELAY,
            None, // no limit (we control manually)
        );

        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            // Check cancellation before each attempt
            if let Some(ref token) = cancel_token {
                if token.is_cancelled() {
                    return Err(ActrError::Internal(
                        "Connection creation cancelled".to_string(),
                    ));
                }
            }

            // Wait before retry (skip first attempt)
            if attempt > 0 {
                let delay = backoff.next().unwrap_or(CONNECTION_FACTORY_MAX_RETRY_DELAY);
                tracing::info!(
                    "🔄 [Factory] Retrying connection to {} (attempt {}/{}, delay {:?})",
                    target_id,
                    attempt + 1,
                    MAX_RETRIES + 1,
                    delay
                );

                // Interruptible sleep with cancellation
                if let Some(ref token) = cancel_token {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => {
                            self.cleanup_cancelled_connection(
                                target_id,
                                "connection creation cancelled during retry wait",
                            )
                            .await;
                            return Err(ActrError::Internal(
                                "Connection creation cancelled during retry wait".to_string(),
                            ));
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                } else {
                    tokio::time::sleep(delay).await;
                }
            } else {
                tracing::info!(
                    "🔨 [Factory] Initiating new WebRTC connection: {:?}",
                    target_id
                );
            }

            // Attempt connection
            match self
                .try_create_connection_once(target_id, cancel_token.as_ref())
                .await
            {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    // Check if this is a cancellation error - don't retry
                    if let Some(ref token) = cancel_token {
                        if token.is_cancelled() {
                            return Err(e);
                        }
                    }

                    // Only retry on timeout or transient errors
                    let should_retry = matches!(&e, ActrError::TimedOut | ActrError::Internal(_));

                    if !should_retry {
                        return Err(e);
                    }

                    tracing::warn!(
                        "⚠️ [Factory] Connection attempt {}/{} failed: {}",
                        attempt + 1,
                        MAX_RETRIES + 1,
                        e
                    );
                    last_error = Some(e);

                    // Cleanup failed connection before retry
                    self.cleanup_cancelled_connection(target_id, "connection retry cleanup")
                        .await;
                }
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| {
            ActrError::Internal("Connection failed after all retries".to_string())
        }))
    }

    /// Single attempt to create a WebRTC connection
    async fn try_create_connection_once(
        self: &Arc<Self>,
        target_id: &ActrId,
        cancel_token: Option<&CancellationToken>,
    ) -> ActorResult<WebRtcConnection> {
        #[cfg(feature = "opentelemetry")]
        self.root_context_map
            .write()
            .await
            .insert(target_id.clone(), tracing::Span::current().context());

        let ready_rx = self.initiate_connection(target_id).await?;

        // Check cancellation after initiation
        if let Some(token) = cancel_token {
            if token.is_cancelled() {
                self.cleanup_cancelled_connection(
                    target_id,
                    "connection creation cancelled after initiation",
                )
                .await;
                return Err(ActrError::Internal(
                    "Connection creation cancelled after initiation".to_string(),
                ));
            }
        }

        // Wait for connection to be ready with cancellation support.
        let timeout_duration = INITIAL_CONNECTION_TIMEOUT;

        let wait_result = if let Some(token) = cancel_token {
            tokio::select! {
                biased;
                _ = token.cancelled() => {
                    self.cleanup_cancelled_connection(
                        target_id,
                        "connection creation cancelled while waiting",
                    )
                    .await;
                    return Err(ActrError::Internal(
                        "Connection creation cancelled while waiting".to_string(),
                    ));
                }
                _ = tokio::time::sleep(timeout_duration) => {
                    Err(ActrError::TimedOut)
                }
                result = ready_rx => {
                    result.map_err(|_| ActrError::Internal(
                        "Connection establishment failed (channel closed)".to_string(),
                    ))
                }
            }
        } else {
            tokio::time::timeout(timeout_duration, ready_rx)
                .await
                .map_err(|_| ActrError::TimedOut)?
                .map_err(|_| {
                    ActrError::Internal(
                        "Connection establishment failed (channel closed)".to_string(),
                    )
                })
        };

        wait_result?;

        tracing::info!("✅ [Factory] WebRTC connection ready: {:?}", target_id);

        // Final cancellation check
        if let Some(token) = cancel_token {
            if token.is_cancelled() {
                self.cleanup_cancelled_connection(
                    target_id,
                    "connection creation cancelled after ready",
                )
                .await;
                return Err(ActrError::Internal(
                    "Connection creation cancelled after ready".to_string(),
                ));
            }
        }

        // Get and return WebRtcConnection
        let peers = self.peers.read().await;
        peers
            .get(target_id)
            .map(|state| state.webrtc_conn.clone())
            .ok_or_else(|| {
                ActrError::Internal("Peer not found after connection establishment".to_string())
            })
    }

    /// Map codec name to RTP dynamic payload type
    fn codec_to_payload_type(codec: &str) -> u8 {
        match codec.to_uppercase().as_str() {
            "VP8" => 96,
            "H264" => 97,
            "VP9" => 98,
            "OPUS" => 111,
            _ => 96,
        }
    }

    /// Send media sample to target Actor via WebRTC Track
    ///
    /// # Arguments
    /// - `target`: Target Actor ID
    /// - `track_id`: Media track identifier
    /// - `sample`: Media sample to send
    ///
    /// # Returns
    /// Ok(()) if sent successfully
    pub async fn send_media_sample(
        &self,
        target: &actr_protocol::ActrId,
        track_id: &str,
        sample: actr_framework::MediaSample,
    ) -> ActorResult<()> {
        use webrtc::rtp::header::Header as RtpHeader;
        use webrtc::rtp::packet::Packet as RtpPacket;

        // 1. Get PeerState for target
        let peers = self.peers.read().await;
        let peer_state = peers.get(target).ok_or_else(|| {
            ActrError::Internal(format!(
                "No connection to target: {}",
                target.to_string_repr()
            ))
        })?;

        // 2. Get Track from WebRtcConnection
        let track = peer_state
            .webrtc_conn
            .get_media_track(track_id)
            .await
            .ok_or_else(|| ActrError::Internal(format!("Track not found: {track_id}")))?;

        // 3. Get next sequence number for this track
        let sequence_number = peer_state
            .webrtc_conn
            .next_sequence_number(track_id)
            .await
            .ok_or_else(|| {
                ActrError::Internal(format!("Sequence number not found for track: {track_id}"))
            })?;

        // 4. Get SSRC for this track
        let ssrc = peer_state
            .webrtc_conn
            .get_ssrc(track_id)
            .await
            .ok_or_else(|| ActrError::Internal(format!("SSRC not found for track: {track_id}")))?;

        // 5. Construct RTP packet from MediaSample
        let rtp_packet = RtpPacket {
            header: RtpHeader {
                version: 2,
                padding: false,
                extension: false,
                marker: true, // Mark each sample (simplified)
                payload_type: Self::codec_to_payload_type(&sample.codec),
                sequence_number, // Per-track sequence number (wraps at 65535)
                timestamp: sample.timestamp,
                ssrc, // Unique SSRC per track (randomly generated)
                ..Default::default()
            },
            payload: sample.data,
        };

        // 6. Send RTP packet via track
        track
            .write_rtp(&rtp_packet)
            .await
            .map_err(|e| ActrError::Internal(format!("Failed to write RTP: {e}")))?;

        tracing::debug!(
            "📤 Sent MediaSample: track_id={}, seq={}, ssrc=0x{:08x}, timestamp={}, size={}",
            track_id,
            sequence_number,
            ssrc,
            sample.timestamp,
            rtp_packet.payload.len()
        );

        Ok(())
    }

    /// Add dynamic media track and trigger SDP renegotiation
    ///
    /// # Arguments
    /// - `target`: Target Actor ID
    /// - `track_id`: Media track identifier
    /// - `codec`: Codec name (e.g., "VP8", "H264", "OPUS")
    /// - `media_type`: Media type ("video" or "audio")
    ///
    /// # Returns
    /// Ok(()) if track added and renegotiation completed successfully
    ///
    /// # Note
    /// This triggers SDP renegotiation on the existing PeerConnection.
    /// The connection remains active and existing tracks continue transmitting.
    pub async fn add_dynamic_track(
        self: &Arc<Self>,
        target: &actr_protocol::ActrId,
        track_id: String,
        codec: &str,
        media_type: &str,
    ) -> ActorResult<()> {
        tracing::info!(
            "🎬 Adding dynamic track: track_id={}, codec={}, type={}, target={}",
            track_id,
            codec,
            media_type,
            target
        );

        // Ensure the first track addition can establish the connection on demand.
        self.ensure_connected(target).await?;

        // 1. Get existing peer state and extract needed parts
        let (webrtc_conn, peer_connection) = {
            let peers = self.peers.read().await;
            let state = peers.get(target).ok_or_else(|| {
                ActrError::Internal(format!(
                    "No connection to target: {}",
                    target.to_string_repr()
                ))
            })?;
            (state.webrtc_conn.clone(), state.peer_connection.clone())
        };

        // 2. Add track to existing PeerConnection
        webrtc_conn
            .add_media_track(track_id.clone(), codec, media_type)
            .await?;

        tracing::info!("✅ Added track to PeerConnection: {}", track_id);

        // 3. Trigger SDP renegotiation
        let root_span = tracing::info_span!("add_track", target_id = %target);
        #[cfg(feature = "opentelemetry")]
        self.root_context_map
            .write()
            .await
            .insert(target.clone(), root_span.context());

        self.renegotiate_connection(target, &peer_connection)
            .instrument(root_span)
            .await?;

        tracing::info!("✅ Dynamic track added successfully: {}", track_id);

        Ok(())
    }

    /// Remove a dynamic media track and trigger SDP renegotiation when needed.
    pub async fn remove_dynamic_track(
        self: &Arc<Self>,
        target: &actr_protocol::ActrId,
        track_id: &str,
    ) -> ActorResult<()> {
        tracing::info!(
            "🗑️ Removing dynamic track: track_id={}, target={}",
            track_id,
            target
        );

        let Some((webrtc_conn, peer_connection)) = ({
            let peers = self.peers.read().await;
            peers
                .get(target)
                .map(|state| (state.webrtc_conn.clone(), state.peer_connection.clone()))
        }) else {
            tracing::debug!(
                "Skip removing track {} because no connection exists for {}",
                track_id,
                target
            );
            return Ok(());
        };

        if webrtc_conn.get_media_track(track_id).await.is_none() {
            tracing::debug!("Skip removing missing track {}", track_id);
            return Ok(());
        }

        webrtc_conn.remove_media_track(track_id).await?;

        let root_span = tracing::info_span!("remove_track", target_id = %target);
        #[cfg(feature = "opentelemetry")]
        self.root_context_map
            .write()
            .await
            .insert(target.clone(), root_span.context());

        self.renegotiate_connection(target, &peer_connection)
            .instrument(root_span)
            .await?;

        tracing::info!("✅ Dynamic track removed successfully: {}", track_id);
        Ok(())
    }

    /// Renegotiate SDP with existing peer
    ///
    /// Creates new Offer with updated track list and exchanges SDP.
    /// ICE connection remains active (no restart).
    async fn renegotiate_connection(
        &self,
        target: &actr_protocol::ActrId,
        peer_connection: &Arc<RTCPeerConnection>,
    ) -> ActorResult<()> {
        tracing::info!("🔄 Starting SDP renegotiation with {}", target);

        // 1. Create new Offer (includes all tracks: old + new)
        let offer = peer_connection.create_offer(None).await.map_err(|e| {
            ActrError::Internal(format!("Failed to create renegotiation offer: {e}"))
        })?;
        let offer_sdp = offer.sdp.clone();

        // 2. Set local description
        peer_connection
            .set_local_description(offer)
            .await
            .map_err(|e| ActrError::Internal(format!("Failed to set local description: {e}")))?;

        tracing::debug!(
            "📝 Created renegotiation Offer (SDP length: {})",
            offer_sdp.len()
        );

        let session_id = {
            let peers = self.peers.read().await;
            let state = peers.get(target).ok_or_else(|| {
                ActrError::Internal(format!(
                    "Peer state not found for renegotiation: {target:?}"
                ))
            })?;
            state.session_id
        };
        let sdp_exchange_id = Self::new_envelope_id();
        self.record_pending_local_offer(target, session_id, sdp_exchange_id.clone())
            .await?;

        // 3. Send Offer via signaling server
        let session_desc = actr_protocol::SessionDescription {
            r#type: SdpType::RenegotiationOffer as i32,
            sdp: offer_sdp,
            sdp_exchange_id: Some(sdp_exchange_id.clone()),
        };
        let payload = actr_relay::Payload::SessionDescription(session_desc);
        match self
            .commit_peer_signaling(target, session_id, None, payload)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.clear_pending_local_offer(target, session_id, &sdp_exchange_id)
                    .await;
                return Err(ActrError::Unavailable(format!(
                    "Peer session changed before renegotiation Offer signaling commit: {target}"
                )));
            }
            Err(err) => {
                self.clear_pending_local_offer(target, session_id, &sdp_exchange_id)
                    .await;
                return Err(err);
            }
        }

        tracing::info!("✅ Sent renegotiation Offer to {}", target);

        // 4. Answer will be handled by existing handle_answer() method
        // Note: We don't wait for Answer here to avoid blocking.
        // The renegotiation completes asynchronously when Answer arrives.

        Ok(())
    }

    /// Trigger ICE recovery for an existing connection.
    ///
    /// The Offerer creates the ICE restart offer. The Answerer never creates an
    /// offer here; it only sends `IceRestartRequest` so the Offerer can start or
    /// wake its restart loop.
    ///
    /// Uses atomic state management within peers lock for complete de-duplication.
    /// If an Offerer restart fails after all retries, it attempts to establish a
    /// new connection.
    pub async fn restart_ice(self: &Arc<Self>, target: &actr_protocol::ActrId) -> ActorResult<()> {
        if self.peer_signaling.closing_all.load(Ordering::Acquire) {
            tracing::debug!(
                "⏭️ Skip ICE restart to serial={}: all peers are closing",
                target
            );
            return Ok(());
        }
        let cancellation_epoch = self
            .peer_signaling
            .restart_cancellation_epoch
            .load(Ordering::Acquire);

        // Prepare all clones needed for the spawned task
        let target_clone = target.clone();
        let peers_arc = Arc::clone(&self.peers);
        let negotiator = self.negotiator.clone();
        let local_id = self.local_id_snapshot();
        let credential_state = self.credential_state.clone();
        let signaling_client = Arc::clone(&self.signaling_client);
        let coordinator_weak = Arc::downgrade(self);
        let commit_context = self.peer_signaling_commit_context();
        let restart_signaling_gate = commit_context.gate_for(target).await;

        // Serialize task creation with cleanup. If cleanup started while this
        // call was queued for the gate, discard it; recovery calls that begin
        // afterward capture the new epoch and remain eligible.
        let _signaling_guard = restart_signaling_gate.lock().await;
        if self.peer_signaling.closing_all.load(Ordering::Acquire)
            || self
                .peer_signaling
                .restart_cancellation_epoch
                .load(Ordering::Acquire)
                != cancellation_epoch
        {
            tracing::debug!(
                "⏭️ Skip ICE restart to serial={}: peer shutdown or cancellation advanced",
                target
            );
            return Ok(());
        }

        // CRITICAL FIX: Perform all state checks, spawn, and handle assignment
        // within a SINGLE lock scope to eliminate race condition window
        let mut peers = self.peers.write().await;
        if self.peer_signaling.closing_all.load(Ordering::Acquire)
            || self
                .peer_signaling
                .restart_cancellation_epoch
                .load(Ordering::Acquire)
                != cancellation_epoch
        {
            tracing::debug!(
                "⏭️ Skip ICE restart to serial={}: shutdown began before peer state update",
                target
            );
            return Ok(());
        }
        tracing::info!("Restarting ICE for target: {}", target);
        if let Some(state) = peers.get_mut(target) {
            // 1. Check if restart is already in-flight using restart_task_handle
            if let Some(ref handle) = state.restart_task_handle {
                let is_finished = handle.is_finished();
                tracing::warn!(
                    "🔍 [DEBUG] restart_task_handle exists, is_finished={} for serial={}",
                    is_finished,
                    target
                );
                if !is_finished {
                    if state.is_offerer {
                        // Wake the Offerer's backoff sleep so the in-flight
                        // restart retries immediately. Notify is idempotent.
                        tracing::info!(
                            "⚡ ICE restart already in-flight for serial={}, waking up backoff",
                            target
                        );
                        state.restart_wake.notify_one();
                    } else {
                        tracing::info!(
                            "⏭️ IceRestartRequest notification already in-flight for serial={}",
                            target
                        );
                    }
                    return Ok(());
                }
            } else {
                tracing::warn!(
                    "🔍 [DEBUG] restart_task_handle is None for serial={}",
                    target
                );
            }

            // 2. Also check ice_restart_inflight flag as a backup
            tracing::warn!(
                "🔍 [DEBUG] ice_restart_inflight={} for serial={}",
                state.ice_restart_inflight,
                target
            );
            if state.ice_restart_inflight {
                state.restart_wake.notify_one();
                tracing::warn!(
                    "🚫 ICE restart already in-flight for serial={}, waking up backoff (ice_restart_inflight=true)",
                    target
                );
                return Ok(());
            }

            // 3. Check if we are the offerer
            if !state.is_offerer {
                // Track the Answerer's notification task in the same lifecycle
                // slot as the Offerer's restart task. Cleanup can then abort and
                // await it before declaring signaling quiescent.
                let restart_session_id = state.session_id;
                tracing::info!(
                    "📤 Not offerer for serial={}, sending IceRestartRequest to notify offerer",
                    target
                );
                let handle = tokio::spawn(async move {
                    let send_result = Self::send_ice_restart_request_if_current(
                        &target_clone,
                        restart_session_id,
                        &commit_context,
                        &local_id,
                        &credential_state,
                        cancellation_epoch,
                        &signaling_client,
                        "network_recovered",
                    )
                    .await;

                    match send_result {
                        Some(Ok(())) => tracing::info!(
                            "✅ IceRestartRequest sent to offerer serial={}, session_id={}",
                            target_clone,
                            restart_session_id
                        ),
                        Some(Err(err)) => tracing::warn!(
                            "⚠️ Failed to send IceRestartRequest to serial={}, session_id={}: {}",
                            target_clone,
                            restart_session_id,
                            err
                        ),
                        None => tracing::debug!(
                            "⏭️ Cancelled stale IceRestartRequest to serial={}, session_id={}",
                            target_clone,
                            restart_session_id
                        ),
                    }

                    Self::prune_restart_signaling_gates(&commit_context.state.gates).await;

                    let mut peers = peers_arc.write().await;
                    if let Some(state) = peers.get_mut(&target_clone)
                        && state.session_id == restart_session_id
                    {
                        state.restart_task_handle = None;
                    }
                });
                state.restart_task_handle = Some(handle);
                return Ok(());
            }

            // 4. Set flag to prevent concurrent restarts
            state.ice_restart_inflight = true;

            // Clone peer_connection/session and restart wake handles while we have the lock.
            let peer_connection = state.peer_connection.clone();
            let restart_session_id = state.session_id;
            let restart_wake = state.restart_wake.clone();
            let restart_retry_wake = state.restart_retry_wake.clone();

            tracing::info!(
                "♻️ Initiating ICE restart to serial={}, session_id={}",
                target,
                restart_session_id
            );

            self.mark_peer_recovering(target, restart_session_id, "ice/network recovery started")
                .await;

            // 5. Spawn restart task (STILL WITHIN THE LOCK - this is the fix!)
            let handle = tokio::spawn(async move {
                let restart_result = Self::do_ice_restart_inner(
                    &target_clone,
                    restart_session_id,
                    &peers_arc,
                    peer_connection,
                    &negotiator,
                    &local_id,
                    credential_state,
                    &signaling_client,
                    restart_wake,
                    restart_retry_wake,
                    commit_context,
                    cancellation_epoch,
                )
                .await;

                match restart_result {
                    Ok(true) => {
                        tracing::info!(
                            "✅ ICE restart completed for serial={}, session_id={}",
                            target_clone,
                            restart_session_id
                        );
                    }
                    Ok(false) => {
                        // ICE restart failed after all retries, clean up and try to establish new connection
                        tracing::warn!(
                            "⚠️ ICE restart exhausted for serial={}, session_id={}, cleaning up matched session",
                            target_clone,
                            restart_session_id
                        );

                        if let Some(coord) = coordinator_weak.upgrade() {
                            coord
                                .event_broadcaster
                                .send(ConnectionEvent::IceRestartCompleted {
                                    peer_id: target_clone.clone(),
                                    session_id: restart_session_id,
                                    success: false,
                                });
                            // First, clean up the old connection resources
                            tracing::info!(
                                "🧹 Cleaning up old connection after ICE restart failure for serial={}, session_id={}",
                                target_clone,
                                restart_session_id
                            );
                            coord
                                .cleanup_connection_if_session(
                                    &target_clone,
                                    restart_session_id,
                                    false,
                                    "ICE restart exhausted",
                                )
                                .await;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "❌ ICE restart failed for serial={}, session_id={}: {}",
                            target_clone,
                            restart_session_id,
                            e
                        );

                        // Clean up resources on error
                        if let Some(coord) = coordinator_weak.upgrade() {
                            coord
                                .event_broadcaster
                                .send(ConnectionEvent::IceRestartCompleted {
                                    peer_id: target_clone.clone(),
                                    session_id: restart_session_id,
                                    success: false,
                                });
                            tracing::info!(
                                "🧹 Cleaning up connection after ICE restart error for serial={}, session_id={}",
                                target_clone,
                                restart_session_id
                            );
                            coord
                                .cleanup_connection_if_session(
                                    &target_clone,
                                    restart_session_id,
                                    false,
                                    "ICE restart error",
                                )
                                .await;
                        }
                    }
                }

                // Cleanup restart_task_handle registration
                {
                    let mut peers_guard = peers_arc.write().await;
                    if let Some(state) = peers_guard.get_mut(&target_clone) {
                        if state.session_id == restart_session_id {
                            state.restart_task_handle = None;
                        } else {
                            tracing::debug!(
                                "⏭️ Skip clearing restart handle for stale ICE restart task: serial={}, task_session_id={}, active_session_id={}",
                                target_clone,
                                restart_session_id,
                                state.session_id
                            );
                        }
                    }
                }
            });

            // 6. Store the restart handle immediately (STILL WITHIN THE SAME LOCK!)
            // This completes the atomic state transition - no race condition possible
            state.restart_task_handle = Some(handle);
        } else {
            tracing::warn!("🚫 Skip ICE restart to serial={}: peer not found", target);
        }

        // Lock is released here - all state is consistent
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_ice_restart_request_if_current(
        target: &ActrId,
        restart_session_id: u64,
        commit_context: &PeerSignalingCommitContext,
        local_id: &ActrId,
        credential_state: &CredentialState,
        cancellation_epoch: u64,
        signaling_client: &Arc<dyn SignalingClient>,
        reason: &str,
    ) -> Option<crate::transport::NetworkResult<()>> {
        tracing::info!(
            "📤 Sending IceRestartRequest to offerer serial={} (reason={})",
            target,
            reason
        );

        let envelope = Self::build_actr_relay_envelope(
            local_id.clone(),
            credential_state.credential().await,
            target,
            actr_relay::Payload::IceRestartRequest(IceRestartRequest {
                reason: Some(reason.to_string()),
            }),
        );

        let commit_guard = commit_context
            .acquire_commit(target, restart_session_id, Some(cancellation_epoch))
            .await?;
        Some(
            Self::send_peer_signaling_envelope_while_guarded(
                &commit_guard,
                signaling_client,
                envelope,
            )
            .await,
        )
    }

    /// Handle incoming IceRestartRequest from Answerer (Approach A).
    ///
    /// Three scenarios:
    /// 1. Restart already in-flight & task running → wake up backoff via `notify_one()`
    /// 2. No restart in-flight & we are offerer → initiate new `restart_ice()`
    /// 3. We are not the offerer → ignore (shouldn't happen in normal flow)
    async fn handle_ice_restart_request(
        self: &Arc<Self>,
        from: &ActrId,
        reason: Option<String>,
    ) -> ActorResult<()> {
        let (is_offerer, has_inflight_restart) = {
            let peers = self.peers.read().await;
            match peers.get(from) {
                Some(state) => {
                    let has_inflight = state
                        .restart_task_handle
                        .as_ref()
                        .map(|h| !h.is_finished())
                        .unwrap_or(false);
                    (state.is_offerer, has_inflight)
                }
                None => {
                    tracing::warn!("⚠️ IceRestartRequest from unknown peer serial={}", from);
                    return Ok(());
                }
            }
        };

        if !is_offerer {
            tracing::warn!(
                "⚠️ Received IceRestartRequest but we are not offerer for serial={}",
                from
            );
            return Ok(());
        }

        if has_inflight_restart {
            // Restart task is running — it's either in backoff sleep or waiting for answer.
            // Either way, notify_one() is safe and idempotent:
            //   - If in backoff sleep: wakes up immediately, retries ICE restart
            //   - If waiting for answer: notify stored, consumed after wait_for_completion timeout
            //   - If creating/sending offer: notify stored, consumed at next backoff
            tracing::info!(
                "⚡ Waking up ICE restart backoff for serial={} (peer notification, reason={:?})",
                from,
                reason
            );
            let peers = self.peers.read().await;
            if let Some(state) = peers.get(from) {
                state.restart_wake.notify_one();
            }
        } else {
            // No restart running — initiate one now
            tracing::info!(
                "♻️ Initiating ICE restart for serial={} upon peer request (reason={:?})",
                from,
                reason
            );
            self.restart_ice(from).await?;
        }

        Ok(())
    }

    /// Internal ICE restart implementation with retries.
    /// Returns Ok(true) if restart succeeded, Ok(false) if all retries exhausted.
    #[allow(clippy::too_many_arguments)]
    async fn do_ice_restart_inner(
        target: &ActrId,
        restart_session_id: u64,
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
        peer_connection: Arc<RTCPeerConnection>,
        negotiator: &WebRtcNegotiator,
        local_id: &ActrId,
        credential_state: CredentialState,
        signaling_client: &Arc<dyn SignalingClient>,
        restart_wake: Arc<tokio::sync::Notify>,
        restart_retry_wake: Arc<tokio::sync::Notify>,
        commit_context: PeerSignalingCommitContext,
        cancellation_epoch: u64,
    ) -> ActorResult<bool> {
        let restart_cancellation_epoch = &commit_context.state.restart_cancellation_epoch;
        // Use enhanced backoff with total duration limit
        let backoff = ExponentialBackoff::with_total_duration(
            Duration::from_millis(ICE_RESTART_INITIAL_BACKOFF_MS),
            Duration::from_millis(ICE_RESTART_MAX_BACKOFF_MS),
            Some(ICE_RESTART_MAX_RETRIES),
            ICE_RESTART_MAX_TOTAL_DURATION,
        );

        let mut restart_ok = false;
        let mut gathering_started_at: Option<Instant> = None;

        for delay in backoff {
            if restart_cancellation_epoch.load(Ordering::Acquire) != cancellation_epoch {
                tracing::debug!(
                    "⏭️ Stopping cancelled ICE restart for serial={}, session_id={}",
                    target,
                    restart_session_id
                );
                return Ok(true);
            }

            // ========== Guard 1: Check signaling state ==========
            if !signaling_client.is_connected() {
                tracing::debug!(
                    "🔄 Signaling not ready for ICE restart to serial={}, will retry after {:?}",
                    target,
                    delay
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = restart_wake.notified() => {
                        tracing::info!(
                            "⚡ Backoff interrupted by wake notification (signaling guard), serial={}",
                            target
                        );
                    }
                    _ = restart_retry_wake.notified() => {
                        tracing::info!(
                            "🔔 ICE restart retry wait resumed after signaling recovery for serial={}, reason=signaling_not_ready",
                            target
                        );
                    }
                }
                continue; // Skip this iteration, don't create offer
            }

            // ========== Guard 2: Check ICE gathering state (with timeout detection) ==========
            let gathering_state = peer_connection.ice_gathering_state();
            if gathering_state == RTCIceGatheringState::Gathering {
                let started = gathering_started_at.get_or_insert_with(Instant::now);
                let gathering_duration = started.elapsed();

                if gathering_duration > ICE_GATHERING_TIMEOUT {
                    tracing::error!(
                        "❌ ICE gathering stuck for {:?}, aborting ICE restart for serial={}",
                        gathering_duration,
                        target
                    );
                    // Close peer connection to stop gathering
                    let _ = peer_connection.close().await;
                    return Ok(false);
                }

                tracing::debug!(
                    "⏳ ICE gathering in progress ({:?} elapsed), will retry after {:?}",
                    gathering_duration,
                    ICE_GATHERING_RETRY_INTERVAL
                );
                tokio::select! {
                    _ = tokio::time::sleep(ICE_GATHERING_RETRY_INTERVAL) => {}
                    _ = restart_wake.notified() => {
                        tracing::info!(
                            "⚡ Backoff interrupted by wake notification (gathering guard), serial={}",
                            target
                        );
                    }
                    _ = restart_retry_wake.notified() => {
                        tracing::info!(
                            "🔔 ICE restart retry wait resumed after signaling recovery for serial={}, reason=ice_gathering",
                            target
                        );
                    }
                }
                continue; // Skip this iteration, wait for gathering to complete
            } else {
                // Not gathering, reset timer
                gathering_started_at = None;
            }

            let throttle_remaining = {
                let peers_guard = peers.read().await;
                match peers_guard.get(target) {
                    Some(state) if state.session_id == restart_session_id => {
                        state.last_ice_restart_offer_at.and_then(|sent_at| {
                            ICE_RESTART_MIN_OFFER_INTERVAL.checked_sub(sent_at.elapsed())
                        })
                    }
                    Some(state) => {
                        tracing::debug!(
                            "⏭️ Stopping stale ICE restart before offer throttle check for serial={}, task_session_id={}, active_session_id={}",
                            target,
                            restart_session_id,
                            state.session_id
                        );
                        return Ok(true);
                    }
                    None => {
                        tracing::warn!(
                            "🚫 Peer state removed before ICE restart offer throttle check for serial={}, session_id={}",
                            target,
                            restart_session_id
                        );
                        return Ok(true);
                    }
                }
            };

            if let Some(remaining) = throttle_remaining {
                tracing::debug!(
                    "⏳ Throttling ICE restart offer to serial={} for {:?}",
                    target,
                    remaining
                );
                tokio::select! {
                    _ = tokio::time::sleep(remaining) => {}
                    _ = restart_wake.notified() => {
                        tracing::info!(
                            "⚡ ICE restart offer throttle interrupted by wake notification for serial={}",
                            target
                        );
                    }
                    _ = restart_retry_wake.notified() => {
                        tracing::info!(
                            "🔔 ICE restart retry wait resumed after signaling recovery for serial={}, reason=offer_throttle",
                            target
                        );
                    }
                }
                continue;
            }

            // ========== Both guards passed, safe to create offer ==========
            // Phase 1: under lock — read/check state, set flags, collect attempt number.
            // We must NOT call create_ice_restart_offer under lock because it can
            // synchronously trigger ICE-candidate callbacks that also inspect `peers`,
            // creating a self-deadlock.
            let attempt = {
                let mut peers_guard = peers.write().await;
                let state = match peers_guard.get_mut(target) {
                    Some(s) if s.session_id == restart_session_id => s,
                    Some(s) => {
                        tracing::debug!(
                            "⏭️ Stopping stale ICE restart for serial={}, task_session_id={}, active_session_id={}",
                            target,
                            restart_session_id,
                            s.session_id
                        );
                        return Ok(true);
                    }
                    None => {
                        tracing::warn!(
                            "🚫 Peer state not found during ICE restart for serial={}, session_id={}",
                            target,
                            restart_session_id
                        );
                        return Ok(true);
                    }
                };

                if !state.is_offerer {
                    tracing::warn!(
                        "🚫 Skip ICE restart to serial={}, session_id={}: we are not the offerer",
                        target,
                        restart_session_id
                    );
                    state.ice_restart_inflight = false;
                    state.ice_restart_attempts = 0;
                    return Ok(false);
                }

                // IMPORTANT: Set ice_restart_inflight to true for EACH attempt
                // It was set to false after the previous attempt timed out.
                // wait_for_restart_completion checks this flag, so we must set it
                // before each attempt to avoid false positive success detection.
                state.ice_restart_inflight = true;
                state.last_ice_restart_offer_at = Some(Instant::now());
                // webrtc-rs restarts the ICE agent and starts gathering inside
                // create_offer(ice_restart=true), before the new local SDP is
                // installed. Buffer callbacks until that SDP is signaled.
                state.ice_signaling.begin_local_generation();

                state.ice_restart_attempts += 1;
                state.ice_restart_attempts
            }; // lock released here

            // Phase 2: outside lock — create the offer (may trigger ICE callbacks).
            if restart_cancellation_epoch.load(Ordering::Acquire) != cancellation_epoch {
                tracing::debug!(
                    "⏭️ Stopping cancelled ICE restart before offer creation for serial={}, session_id={}",
                    target,
                    restart_session_id
                );
                return Ok(true);
            }
            let offer_sdp = negotiator
                .create_ice_restart_offer(&peer_connection)
                .await?;
            let offer_description =
                RTCSessionDescription::offer(offer_sdp.clone()).map_err(|e| {
                    ActrError::Internal(format!("Failed to parse local ICE restart offer: {e}"))
                })?;

            if restart_cancellation_epoch.load(Ordering::Acquire) != cancellation_epoch {
                tracing::debug!(
                    "⏭️ Stopping cancelled ICE restart after offer creation for serial={}, session_id={}",
                    target,
                    restart_session_id
                );
                return Ok(true);
            }

            // Phase 3: re-acquire lock — verify peer/session is still current.
            {
                let peers_guard = peers.read().await;
                match peers_guard.get(target) {
                    Some(state) if state.session_id == restart_session_id => {}
                    Some(state) => {
                        tracing::debug!(
                            "⏭️ Stopping stale ICE restart after offer creation for serial={}, task_session_id={}, active_session_id={}",
                            target,
                            restart_session_id,
                            state.session_id
                        );
                        return Ok(true);
                    }
                    None => {
                        tracing::warn!(
                            "🚫 Peer state removed after ICE restart offer creation for serial={}, session_id={}",
                            target,
                            restart_session_id
                        );
                        return Ok(true);
                    }
                }
            }

            // Send ICE restart offer
            let sdp_exchange_id = Self::new_envelope_id();
            let envelope = Self::build_actr_relay_envelope(
                local_id.clone(),
                credential_state.credential().await,
                target,
                actr_relay::Payload::SessionDescription(actr_protocol::SessionDescription {
                    r#type: SdpType::IceRestartOffer as i32,
                    sdp: offer_sdp,
                    sdp_exchange_id: Some(sdp_exchange_id.clone()),
                }),
            );

            let Some(commit_guard) = commit_context
                .acquire_commit(target, restart_session_id, Some(cancellation_epoch))
                .await
            else {
                Self::suppress_local_ice_generation(peers, target, restart_session_id).await;
                tracing::debug!(
                    "⏭️ Stopping stale ICE restart at signaling boundary for serial={}, session_id={}",
                    target,
                    restart_session_id
                );
                return Ok(true);
            };

            if !Self::record_pending_local_offer_for_peer(
                peers,
                target,
                restart_session_id,
                sdp_exchange_id.clone(),
            )
            .await
            {
                Self::suppress_local_ice_generation(peers, target, restart_session_id).await;
                drop(commit_guard);
                tracing::debug!(
                    "⏭️ Stopping stale ICE restart before send for serial={}, session_id={}",
                    target,
                    restart_session_id
                );
                return Ok(true);
            }

            if let Err(e) = Self::send_peer_signaling_envelope_while_guarded(
                &commit_guard,
                signaling_client,
                envelope,
            )
            .await
            {
                tracing::error!(
                    "❌ Failed to send ICE restart offer to serial={}: {}",
                    target,
                    e
                );
                Self::clear_pending_local_offer_for_peer(
                    peers,
                    target,
                    restart_session_id,
                    &sdp_exchange_id,
                )
                .await;
                Self::suppress_local_ice_generation(peers, target, restart_session_id).await;
                // Mark inflight as false and continue to next retry
                let mut peers_guard = peers.write().await;
                match peers_guard.get_mut(target) {
                    Some(state) if state.session_id == restart_session_id => {
                        state.ice_restart_inflight = false;
                    }
                    Some(state) => {
                        tracing::debug!(
                            "⏭️ Stopping stale ICE restart after send failure for serial={}, task_session_id={}, active_session_id={}",
                            target,
                            restart_session_id,
                            state.session_id
                        );
                        return Ok(true);
                    }
                    None => return Ok(true),
                }
                drop(peers_guard);
                drop(commit_guard);
                Self::prune_restart_signaling_gates(&commit_context.state.gates).await;
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = restart_wake.notified() => {
                        tracing::info!(
                            "⚡ Backoff interrupted by wake notification (send failed), serial={}",
                            target
                        );
                    }
                    _ = restart_retry_wake.notified() => {
                        tracing::info!(
                            "🔔 ICE restart retry wait resumed after signaling recovery for serial={}, reason=send_offer_failed",
                            target
                        );
                    }
                }
                continue;
            }

            // The Offer is one atomic signaling commit. Candidate delivery is
            // a sequence of separate commits so cleanup can win between sends.
            drop(commit_guard);

            let buffered_candidates = Self::finish_local_ice_generation(
                peers,
                target,
                restart_session_id,
                &offer_description,
            )
            .await
            .map_err(ActrError::Internal)?;
            Self::commit_prepared_local_ice_candidates(
                &commit_context,
                signaling_client,
                local_id,
                &credential_state,
                target,
                restart_session_id,
                Some(cancellation_epoch),
                buffered_candidates,
            )
            .await;

            tracing::info!(
                "♻️ ICE restart attempt {} sent to serial={}",
                attempt,
                target
            );

            // Wait for restart completion
            let wait_outcome = Self::wait_for_restart_completion_static(
                peers,
                target,
                restart_session_id,
                ICE_RESTART_TIMEOUT,
                &restart_wake,
            )
            .await;

            match wait_outcome {
                IceRestartWaitOutcome::Completed => {
                    restart_ok = true;
                    break;
                }
                IceRestartWaitOutcome::Woken => {
                    tracing::info!(
                        "🔔 ICE restart completion wait interrupted for serial={}, re-evaluating retry state",
                        target
                    );

                    let mut peers_guard = peers.write().await;
                    match peers_guard.get_mut(target) {
                        Some(state) if state.session_id == restart_session_id => {
                            state.ice_restart_inflight = false;
                            if state
                                .ice_signaling
                                .pending_local_sdp_exchange_id
                                .as_ref()
                                .is_some_and(|pending| pending == &sdp_exchange_id)
                            {
                                state.ice_signaling.pending_local_sdp_exchange_id = None;
                            }
                        }
                        Some(state) => {
                            tracing::debug!(
                                "⏭️ Stopping stale ICE restart after wake for serial={}, task_session_id={}, active_session_id={}",
                                target,
                                restart_session_id,
                                state.session_id
                            );
                            return Ok(true);
                        }
                        None => return Ok(true),
                    }
                    continue;
                }
                IceRestartWaitOutcome::TimedOut => {}
            }

            tracing::warn!(
                "⚠️ ICE restart attempt {} timed out for serial={}",
                attempt,
                target
            );

            // Mark current attempt ended
            {
                let mut peers_guard = peers.write().await;
                match peers_guard.get_mut(target) {
                    Some(state) if state.session_id == restart_session_id => {
                        state.ice_restart_inflight = false;
                        if state
                            .ice_signaling
                            .pending_local_sdp_exchange_id
                            .as_ref()
                            .is_some_and(|pending| pending == &sdp_exchange_id)
                        {
                            state.ice_signaling.pending_local_sdp_exchange_id = None;
                        }
                    }
                    Some(state) => {
                        tracing::debug!(
                            "⏭️ Stopping stale ICE restart after timeout for serial={}, task_session_id={}, active_session_id={}",
                            target,
                            restart_session_id,
                            state.session_id
                        );
                        return Ok(true);
                    }
                    None => return Ok(true),
                }
            }
            // Exponential backoff before retrying (can be interrupted by restart_wake)
            tracing::info!(
                "⏳ Waiting {:?} before next ICE restart attempt to serial={}",
                delay,
                target
            );
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = restart_wake.notified() => {
                    tracing::info!(
                        "⚡ ICE restart backoff interrupted by wake notification for serial={}",
                        target
                    );
                }
                _ = restart_retry_wake.notified() => {
                    tracing::info!(
                        "🔔 ICE restart retry wait resumed after signaling recovery for serial={}, reason=attempt_timeout",
                        target
                    );
                }
            }
        }

        if !restart_ok {
            tracing::warn!(
                "⚠️ Backoff iterator exhausted for serial={}, session_id={}, stopping retries",
                target,
                restart_session_id
            );
            return Ok(false);
        }

        Ok(true)
    }

    /// Static version of wait_for_restart_completion for use in spawned task
    /// Uses read lock for checking status to avoid blocking other peers
    ///
    /// Success is determined by BOTH conditions:
    /// 1. ice_restart_inflight is false (answer received and processed)
    /// 2. current_state is Connected (actual connection restored)
    async fn wait_for_restart_completion_static(
        peers: &Arc<RwLock<HashMap<ActrId, PeerState>>>,
        target: &ActrId,
        restart_session_id: u64,
        timeout: Duration,
        restart_wake: &tokio::sync::Notify,
    ) -> IceRestartWaitOutcome {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let timeout_sleep = tokio::time::sleep(timeout);
        tokio::pin!(timeout_sleep);

        loop {
            tokio::select! {
                _ = &mut timeout_sleep => {
                    return IceRestartWaitOutcome::TimedOut;
                }
                _ = restart_wake.notified() => {
                    return IceRestartWaitOutcome::Woken;
                }
                _ = interval.tick() => {
                    // Use read lock to check status (allows concurrent access)
                    let is_done = {
                        let peers_guard = peers.read().await;
                        match peers_guard.get(target) {
                            Some(state) if state.session_id != restart_session_id => {
                                tracing::debug!(
                                    "⏭️ Treating ICE restart as complete because session changed: serial={}, task_session_id={}, active_session_id={}",
                                    target,
                                    restart_session_id,
                                    state.session_id
                                );
                                return IceRestartWaitOutcome::Completed;
                            }
                            // SUCCESS = answer has cleared the in-flight marker and
                            // the peer connection is still Connected.
                            Some(state) => {
                                !state.ice_restart_inflight
                                    && matches!(
                                        state.current_state,
                                        RTCPeerConnectionState::Connected
                                    )
                            }
                            None => return IceRestartWaitOutcome::Completed,
                        }
                    };

                    if is_done {
                        // Only acquire write lock when actually need to reset counter
                        let mut peers_guard = peers.write().await;
                        if let Some(state) = peers_guard.get_mut(target) {
                            if state.session_id == restart_session_id {
                                state.ice_restart_attempts = 0;
                            }
                        }
                        return IceRestartWaitOutcome::Completed;
                    }
                }
            }
        }
    }

    /// Handle renegotiation Offer (existing connection)
    ///
    /// Called when receiving an Offer on an already-established connection.
    /// This happens when the remote peer adds/removes tracks dynamically.
    #[allow(dead_code)]
    async fn handle_renegotiation_offer(
        &self,
        from: &ActrId,
        offer_sdp: String,
        sdp_exchange_id: Option<String>,
    ) -> ActorResult<()> {
        let Some(sdp_exchange_id) = sdp_exchange_id else {
            tracing::warn!(
                "🚫 Ignoring renegotiation Offer from {} without sdp_exchange_id correlation",
                from
            );
            return Ok(());
        };

        tracing::info!("🔄 Processing renegotiation Offer from {}", from);

        // 1. Get existing peer connection
        let (peer_connection, session_id) = {
            let peers = self.peers.read().await;
            let state = peers.get(from).ok_or_else(|| {
                ActrError::Internal("Peer state not found for renegotiation".to_string())
            })?;
            (state.peer_connection.clone(), state.session_id)
        };

        // 2. Set remote description (new Offer)
        let offer =
            webrtc::peer_connection::sdp::session_description::RTCSessionDescription::offer(
                offer_sdp,
            )
            .map_err(|e| {
                ActrError::Internal(format!("Failed to parse renegotiation offer: {e}"))
            })?;
        peer_connection
            .set_remote_description(offer)
            .await
            .map_err(|e| ActrError::Internal(format!("Failed to set remote description: {e}")))?;

        tracing::debug!("✅ Set remote description (renegotiation Offer)");

        // 3. Create Answer
        let answer = peer_connection.create_answer(None).await.map_err(|e| {
            ActrError::Internal(format!("Failed to create renegotiation answer: {e}"))
        })?;
        let answer_sdp = answer.sdp.clone();

        // 4. Set local description
        peer_connection
            .set_local_description(answer)
            .await
            .map_err(|e| ActrError::Internal(format!("Failed to set local description: {e}")))?;

        tracing::debug!(
            "✅ Created renegotiation Answer (SDP length: {})",
            answer_sdp.len()
        );

        // 5. Send Answer via signaling server
        let session_desc = actr_protocol::SessionDescription {
            r#type: SdpType::Answer as i32,
            sdp: answer_sdp,
            sdp_exchange_id: Some(sdp_exchange_id),
        };
        let payload = actr_relay::Payload::SessionDescription(session_desc);
        if !self
            .commit_peer_signaling(from, session_id, None, payload)
            .await?
        {
            tracing::debug!(
                "⏭️ Skipped renegotiation Answer for stale peer session: peer={}, session_id={}",
                from,
                session_id
            );
            return Ok(());
        }

        tracing::info!("✅ Sent renegotiation Answer to {}", from);

        // Note: on_track callback will automatically trigger for new remote tracks
        // No need to manually handle track additions here

        Ok(())
    }

    /// Handle ICE restart Offer on an existing connection.
    /// Only the answerer should accept restart; offerer-side restarts are initiated locally.
    async fn handle_ice_restart_offer(
        self: &Arc<Self>,
        from: &ActrId,
        offer_sdp: String,
        sdp_exchange_id: Option<String>,
    ) -> ActorResult<()> {
        let Some(sdp_exchange_id) = sdp_exchange_id else {
            tracing::warn!(
                "🚫 Ignoring ICE Restart Offer from {} without sdp_exchange_id correlation",
                from
            );
            return Ok(());
        };
        let cancellation_epoch = self
            .peer_signaling
            .restart_cancellation_epoch
            .load(Ordering::Acquire);

        // Locate peer state and ensure we are not the offerer
        let (peer_connection, is_offerer, session_id) = {
            let peers = self.peers.read().await;
            let Some(state) = peers.get(from) else {
                drop(peers);
                tracing::warn!(
                    "🚫 ICE restart offer received for unknown peer {}; notifying idle",
                    from
                );
                self.invoke_hook(crate::wire::webrtc::HookEvent::WebRtcDisconnected {
                    peer_id: from.clone(),
                    status: WebRtcPeerStatus::Idle,
                })
                .await;
                return Ok(());
            };
            (
                state.peer_connection.clone(),
                state.is_offerer,
                state.session_id,
            )
        };

        if is_offerer {
            tracing::warn!(
                "🚫 Ignoring ICE restart offer from {:?}: we are current offerer",
                from
            );
            return Ok(());
        }

        if !Self::begin_local_ice_generation(&self.peers, from, session_id).await {
            tracing::debug!(
                "⏭️ ICE restart answer cancelled because peer session changed: peer={}, session_id={}",
                from,
                session_id
            );
            return Ok(());
        }

        // Apply remote restart offer and generate answer outside the signaling
        // gate. Cleanup may proceed during this work; the current-session check
        // below decides whether the result is still eligible to be committed.
        let answer_sdp = match self
            .negotiator
            .create_answer(&peer_connection, offer_sdp)
            .await
        {
            Ok(answer_sdp) => answer_sdp,
            Err(err) => {
                Self::suppress_local_ice_generation(&self.peers, from, session_id).await;
                return Err(err.into());
            }
        };
        let answer_description = match RTCSessionDescription::answer(answer_sdp.clone()) {
            Ok(answer_description) => answer_description,
            Err(err) => {
                Self::suppress_local_ice_generation(&self.peers, from, session_id).await;
                return Err(ActrError::Internal(format!(
                    "Failed to parse local ICE restart answer: {err}"
                )));
            }
        };

        let commit_context = self.peer_signaling_commit_context();
        let Some(commit_guard) = commit_context
            .acquire_commit(from, session_id, Some(cancellation_epoch))
            .await
        else {
            Self::suppress_local_ice_generation(&self.peers, from, session_id).await;
            tracing::debug!(
                "⏭️ ICE restart answer signalling cancelled at commit boundary: peer={}, session_id={}",
                from,
                session_id
            );
            return Ok(());
        };

        let session_desc = actr_protocol::SessionDescription {
            r#type: SdpType::Answer as i32,
            sdp: answer_sdp,
            sdp_exchange_id: Some(sdp_exchange_id),
        };
        if let Err(err) = self
            .send_peer_actr_relay_while_guarded(
                &commit_guard,
                from,
                actr_relay::Payload::SessionDescription(session_desc),
            )
            .await
        {
            Self::suppress_local_ice_generation(&self.peers, from, session_id).await;
            drop(commit_guard);
            Self::prune_restart_signaling_gates(&self.peer_signaling.gates).await;
            self.cleanup_connection_if_session(
                from,
                session_id,
                true,
                "ICE restart Answer signaling failed",
            )
            .await;
            return Err(err);
        }
        drop(commit_guard);

        let buffered_candidates =
            Self::finish_local_ice_generation(&self.peers, from, session_id, &answer_description)
                .await
                .map_err(ActrError::Internal)?;
        let local_id = self.local_id_snapshot();
        Self::commit_prepared_local_ice_candidates(
            &commit_context,
            &self.signaling_client,
            &local_id,
            &self.credential_state,
            from,
            session_id,
            Some(cancellation_epoch),
            buffered_candidates,
        )
        .await;

        // Candidate application is bounded independently and intentionally
        // runs without the signaling gate. A concurrent close may remove the
        // session; the flush's session checks then turn it into a no-op.
        self.flush_pending_candidates(from, session_id, &peer_connection)
            .await?;

        tracing::info!(
            "✅ Completed ICE restart answer to serial={}, session_id={}; waiting for ICE Connected before marking sendable",
            from,
            session_id
        );

        Ok(())
    }

    /// Handle role assignment result
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(skip_all, fields(actr_id = %self.local_id_snapshot(), peer_id = %peer))
    )]
    async fn handle_role_assignment(self: &Arc<Self>, assign: RoleAssignment, peer: ActrId) {
        tracing::debug!(?assign, ?peer, "handle_role_assignment");

        // Snapshot the exact role flight and peer session that existed when
        // this assignment arrived. Later cleanup and completion are accepted
        // only if those identities still match.
        let (peer_lifecycle_epoch, assignment_flight_rx, existing_session_id) = {
            let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
            if self.peer_signaling.closing_all.load(Ordering::Acquire) {
                tracing::debug!(
                    peer_id = %peer,
                    "Ignoring RoleAssignment while WebRTC close-all is active"
                );
                return;
            }
            let peer_lifecycle_epoch = self
                .peer_signaling
                .peer_lifecycle_epoch
                .load(Ordering::Acquire);
            let mut neg = self.peer_negotiation.lock().await;
            let state = neg.entry(peer.clone()).or_default();
            state.remote_fixed = assign.remote_fixed.unwrap_or(false);
            let assignment_flight_rx = state
                .role_flight
                .as_ref()
                .filter(|flight| flight.lifecycle_epoch == peer_lifecycle_epoch)
                .map(|flight| flight.result_tx.subscribe());
            tracing::info!(
                "🔧 Stored remote_fixed={} for peer {}",
                state.remote_fixed,
                peer
            );
            drop(neg);
            let existing_session_id = if assign.is_offerer {
                self.peers
                    .read()
                    .await
                    .get(&peer)
                    .map(|state| state.session_id)
            } else {
                None
            };
            (
                peer_lifecycle_epoch,
                assignment_flight_rx,
                existing_session_id,
            )
        };

        if let Some(existing_session_id) = existing_session_id {
            tracing::info!(
                peer_id = %peer,
                existing_session_id,
                "Assigned as offerer; cleaning the exact previously observed session"
            );
            self.cleanup_connection_for_role_assignment_if_session(
                &peer,
                existing_session_id,
                "role changed to offerer",
            )
            .await;
        }

        // Revalidate after cleanup. A close-all that crossed this handler has
        // advanced the epoch and removed the snapshotted flight, so this old
        // assignment cannot wake or mutate a new lifecycle.
        {
            let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
            if self.peer_signaling.closing_all.load(Ordering::Acquire)
                || self
                    .peer_signaling
                    .peer_lifecycle_epoch
                    .load(Ordering::Acquire)
                    != peer_lifecycle_epoch
            {
                tracing::debug!(
                    peer_id = %peer,
                    "Ignoring RoleAssignment after peer lifecycle changed"
                );
                return;
            }

            let mut negotiations = self.peer_negotiation.lock().await;
            let state = negotiations.entry(peer.clone()).or_default();
            if let Some(assignment_flight_rx) = assignment_flight_rx.as_ref() {
                let flight_matches = state.role_flight.as_ref().is_some_and(|flight| {
                    flight.lifecycle_epoch == peer_lifecycle_epoch
                        && Self::role_flight_matches(assignment_flight_rx, flight)
                });
                if !flight_matches {
                    tracing::debug!(
                        peer_id = %peer,
                        "Ignoring RoleAssignment after its role flight was replaced"
                    );
                    return;
                }

                let flight = state
                    .role_flight
                    .take()
                    .expect("matching role flight must still be present");
                flight.result_tx.send_replace(Some(Ok(assign.is_offerer)));
                return;
            }

            // No waiter at receipt is the normal passive-side path. If a new
            // local negotiation appeared after receipt, do not let this older
            // unsolicited assignment satisfy or race that newer flight.
            if state.role_flight.is_some() {
                tracing::debug!(
                    peer_id = %peer,
                    "Ignoring unsolicited RoleAssignment after a newer local role flight started"
                );
                return;
            }
            drop(negotiations);

            if self.peers.read().await.contains_key(&peer) {
                tracing::debug!(
                    peer_id = %peer,
                    "Peer already has a connection; unsolicited RoleAssignment needs no action"
                );
                return;
            }
        }

        // Passive peers trust the assignment received in the current signaling
        // lifecycle. Starting a second RoleNegotiation here would add a round
        // trip and could compete with a concurrent local connection attempt.
        if assign.is_offerer {
            tracing::info!(
                "🎭 Acting as offerer to {} per assignment (no pending negotiation)",
                peer
            );
            // Spawn the offer connection in background to avoid blocking signaling loop
            let this = Arc::clone(self);
            let peer_clone = peer.clone();
            #[cfg(feature = "opentelemetry")]
            let current_span = tracing::Span::current();
            tokio::spawn(async move {
                let start_offer_fut =
                    this.start_offer_connection(&peer_clone, true, peer_lifecycle_epoch);
                #[cfg(feature = "opentelemetry")]
                let start_offer_fut = start_offer_fut.instrument(current_span);
                match start_offer_fut.await {
                    Ok(ready_rx) => {
                        this.store_ready_receiver_if_lifecycle_current(
                            &peer_clone,
                            peer_lifecycle_epoch,
                            ready_rx,
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "⚠️ Failed to start proactive offer connection to {}: {}",
                            peer_clone,
                            e
                        );
                    }
                }
            });
        } else {
            tracing::debug!(
                "🎭 Assignment marks us as answerer for {}, waiting for offer (no pending negotiation)",
                peer
            );
            if let Err(err) = self
                .ensure_answerer_wait_if_lifecycle_current(&peer, peer_lifecycle_epoch)
                .await
            {
                tracing::debug!(
                    peer_id = %peer,
                    error = %err,
                    "RoleAssignment became stale before answerer wait was installed"
                );
                return;
            }

            // If the offer is lost, keep waiting as answerer. RoleNegotiation broadcasts
            // RoleAssignment to both peers, so retrying it here can make the original
            // offerer start a duplicate connection attempt.
            let weak = Arc::downgrade(self);
            let peer_clone = peer.clone();
            tokio::spawn(async move {
                tokio::time::sleep(ROLE_WAIT_TIMEOUT).await;
                if let Some(coord) = weak.upgrade() {
                    // Exit if connection already exists or ready has been consumed
                    if coord.peers.read().await.contains_key(&peer_clone) {
                        return;
                    }
                    let still_waiting = {
                        let neg = coord.peer_negotiation.lock().await;
                        neg.get(&peer_clone)
                            .and_then(|s| s.ready_tx.as_ref())
                            .is_some()
                    };
                    if still_waiting {
                        tracing::warn!(
                            "⏳ Waiting for offer from {} timed out; continuing to wait as answerer without role renegotiation",
                            peer_clone
                        );
                    }
                }
            });
        }
    }

    /// Initiate role negotiation and await assignment
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(skip_all, fields(actr_id = %self.local_id_snapshot(), target_id = %target))
    )]
    async fn negotiate_role(
        &self,
        target: &ActrId,
        peer_lifecycle_epoch: u64,
    ) -> ActorResult<bool> {
        // Install or join a per-peer flight under the lifecycle gate. Only the
        // leader sends RoleNegotiation; every concurrent caller observes the
        // same retained result without replacing another caller's waiter.
        let (mut result_rx, is_leader) = {
            let _lifecycle_guard = self.peer_signaling.lifecycle_gate.lock().await;
            if self.peer_signaling.closing_all.load(Ordering::Acquire)
                || self
                    .peer_signaling
                    .peer_lifecycle_epoch
                    .load(Ordering::Acquire)
                    != peer_lifecycle_epoch
            {
                return Err(ActrError::Unavailable(format!(
                    "WebRTC peer lifecycle changed before role negotiation: {target}"
                )));
            }
            let mut negotiations = self.peer_negotiation.lock().await;
            let state = negotiations.entry(target.clone()).or_default();
            if let Some(flight) = state
                .role_flight
                .as_ref()
                .filter(|flight| flight.lifecycle_epoch == peer_lifecycle_epoch)
            {
                (flight.result_tx.subscribe(), false)
            } else {
                if let Some(stale_flight) = state.role_flight.take() {
                    stale_flight
                        .result_tx
                        .send_replace(Some(Err(ActrError::Unavailable(
                            "Role negotiation was superseded by a new lifecycle".to_string(),
                        ))));
                }
                let (result_tx, result_rx) = watch::channel(None);
                state.role_flight = Some(RoleNegotiationFlight {
                    lifecycle_epoch: peer_lifecycle_epoch,
                    result_tx,
                });
                (result_rx, true)
            }
        };

        if is_leader {
            let local_id = self.local_id_snapshot();
            let role_negotiation = RoleNegotiation {
                from: local_id.clone(),
                to: target.clone(),
                realm_id: local_id.realm.realm_id,
            };

            tracing::debug!("🔄 Sending role negotiation to serial={}", target);
            let send_result = match self
                .send_role_negotiation(target, role_negotiation, peer_lifecycle_epoch)
                .await
            {
                Ok(true) => Ok(()),
                Ok(false) => Err(ActrError::Unavailable(format!(
                    "WebRTC peer lifecycle changed before RoleNegotiation commit: {target}"
                ))),
                Err(err) => Err(err),
            };
            if let Err(err) = send_result {
                self.finish_role_flight_if_current(
                    target,
                    peer_lifecycle_epoch,
                    &result_rx,
                    Err(err.clone()),
                )
                .await;
                return Err(err);
            }
        } else {
            tracing::debug!(
                "🔗 Joining in-flight role negotiation for serial={}",
                target
            );
        }

        match tokio::time::timeout(
            ROLE_NEGOTIATION_TIMEOUT,
            Self::wait_for_role_result(&mut result_rx),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.finish_role_flight_if_current(
                    target,
                    peer_lifecycle_epoch,
                    &result_rx,
                    Err(ActrError::TimedOut),
                )
                .await;
                Err(ActrError::TimedOut)
            }
        }
    }

    async fn handle_peer_state_change(
        self: &Arc<Self>,
        webrtc_conn: &WebRtcConnection,
        target: &ActrId,
        session_id: u64,
        state: RTCPeerConnectionState,
    ) {
        tracing::info!("📡 PeerConnection state for {} -> {:?}", target, state);

        // Only the active session may update state or trigger recovery. The
        // PeerConnection callback can outlive a replaced connection.
        let is_active_session = {
            let mut peers = self.peers.write().await;
            match peers.get_mut(target) {
                Some(peer_state) if peer_state.session_id == session_id => {
                    peer_state.update_connection_state(state);
                    true
                }
                Some(peer_state) => {
                    tracing::debug!(
                        "⏭️ Ignoring stale PeerConnection state for peer {}, event_session_id={}, active_session_id={}",
                        target,
                        session_id,
                        peer_state.session_id
                    );
                    false
                }
                None => false,
            }
        };

        if !is_active_session
            || !matches!(
                state,
                RTCPeerConnectionState::Disconnected | RTCPeerConnectionState::Failed
            )
        {
            return;
        }

        // Log buffered_amount for all open DataChannels so callers can assess
        // how much data may not have been delivered due to the abrupt disconnect.
        {
            use webrtc::data_channel::data_channel_state::RTCDataChannelState;
            let channels = webrtc_conn.data_channels().await;
            for (idx, channel_opt) in channels.iter().enumerate() {
                if let Some(channel) = channel_opt {
                    if channel.ready_state() == RTCDataChannelState::Open {
                        let buffered = channel.buffered_amount().await;
                        tracing::warn!(
                            peer_id = %target,
                            channel = %channel.label(),
                            channel_idx = idx,
                            connection_state = ?state,
                            buffered_bytes = buffered,
                            "Abnormal disconnect detected; \
                             buffered data may not have been delivered to peer",
                        );
                    }
                }
            }
        }

        if let Err(e) = self.restart_ice(target).await {
            tracing::warn!(
                "⚠️ Failed to trigger ICE recovery for {} after {:?}: {}",
                target,
                state,
                e
            );
        }
    }

    /// Install the shared PeerConnection state handler for both roles.
    ///
    /// On Disconnected/Failed, the Offerer starts ICE restart negotiation. The
    /// Answerer only sends `IceRestartRequest`; it never creates a restart offer.
    fn install_peer_state_handler(
        self: &Arc<Self>,
        webrtc_conn: WebRtcConnection,
        peer_connection: Arc<RTCPeerConnection>,
        target: ActrId,
    ) {
        let coord = Arc::downgrade(self);
        let session_id = webrtc_conn.session_id();
        peer_connection.on_peer_connection_state_change(Box::new(
            move |state: RTCPeerConnectionState| {
                let coord = coord.clone();
                let target = target.clone();
                let webrtc_conn = webrtc_conn.clone();
                Box::pin(async move {
                    // First run the base WebRtcConnection cleanup.
                    webrtc_conn.handle_state_change(state).await;

                    if let Some(c) = coord.upgrade() {
                        c.handle_peer_state_change(&webrtc_conn, &target, session_id, state)
                            .await;
                    }
                })
            },
        ));
    }
}

#[cfg(test)]
#[path = "coordinator_tests.rs"]
mod tests;
