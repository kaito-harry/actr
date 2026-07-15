//! Test Harness for PeerGate integration tests
//!
//! Provides `TestPeer` and `TestHarness` for building multi-peer test
//! topologies with optional VNet-based network simulation.
//!
//! ## Usage
//!
//! ```rust,ignore
//! // Basic two-peer test
//! let mut harness = TestHarness::new().await;
//! harness.add_peer(100).await;
//! harness.add_peer(200).await;
//! harness.connect(100, 200).await;
//!
//! // Multi-peer with VNet (disconnect simulation)
//! let mut harness = TestHarness::with_vnet().await;
//! harness.add_peer(100).await;
//! harness.add_peer(200).await;
//! harness.add_peer(300).await;
//! harness.connect(200, 100).await;
//! harness.connect(300, 100).await;
//!
//! harness.simulate_disconnect();
//! // ... verify ICE restart triggered
//! harness.simulate_reconnect();
//! ```

use super::signaling::TestSignalingServer;
use super::utils::{
    create_peer_with_vnet, create_peer_with_websocket, make_actor_id, spawn_echo_responder,
    spawn_response_receiver, spawn_rpc_dispatcher,
};
use super::vnet::VNetPair;
use crate::lifecycle::DefaultNetworkEventProcessor;
use crate::outbound::PeerGate;
use crate::transport::{DefaultWireBuilder, DefaultWireBuilderConfig, PeerTransport};
use crate::wire::webrtc::{SignalingClient, WebRtcCoordinator};
use actr_protocol::{ActrId, RpcEnvelope};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// A single test peer encapsulating all components needed for peer communication.
pub struct TestPeer {
    /// Actor ID for this peer
    pub id: ActrId,
    /// WebRTC coordinator (connection management, ICE restart, signaling)
    pub coordinator: Arc<WebRtcCoordinator>,
    /// Signaling client used by network event processors in mobile-style tests.
    pub signaling_client: Arc<dyn SignalingClient>,
    /// PeerGate (message sending, pending request management)
    pub gate: Arc<PeerGate>,
    /// Transport manager (wire pool, dest transport management)
    pub transport_manager: Arc<PeerTransport>,
}

impl TestPeer {
    /// Subscribe to connection events from this peer's coordinator
    pub fn subscribe_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::transport::ConnectionEvent> {
        self.coordinator.subscribe_events()
    }

    /// Get pending request count
    pub async fn pending_count(&self) -> usize {
        self.gate.pending_count().await
    }

    /// Trigger ICE restart to a target peer
    pub async fn restart_ice(&self, target_serial: u64) -> anyhow::Result<()> {
        let target_id = make_actor_id(target_serial);
        self.coordinator
            .restart_ice(&target_id)
            .await
            .map_err(|e| anyhow::anyhow!("ICE restart failed: {}", e))
    }

    /// Retry failed connections
    pub async fn retry_failed(&self) {
        self.coordinator.retry_failed_connections().await;
    }

    /// Create a network event processor for this peer.
    pub fn network_processor(&self) -> Arc<DefaultNetworkEventProcessor> {
        Arc::new(DefaultNetworkEventProcessor::new_with_peer_transport(
            self.signaling_client.clone(),
            Some(self.coordinator.clone()),
            Some(self.transport_manager.clone()),
        ))
    }

    /// Send a test RPC request to a target peer (fire-and-forget, returns handle)
    pub fn spawn_request(
        &self,
        target_serial: u64,
        request_id: &str,
        timeout_ms: u32,
    ) -> tokio::task::JoinHandle<actr_protocol::ActorResult<actr_framework::Bytes>> {
        let gate = self.gate.clone();
        let target_id = make_actor_id(target_serial);
        let envelope = RpcEnvelope {
            request_id: request_id.to_string(),
            route_key: "test.method".to_string(),
            payload: Some(bytes::Bytes::from("test_payload")),
            direction: Some(actr_protocol::Direction::Request as i32),
            timeout_ms: timeout_ms as i64,
            ..Default::default()
        };
        tokio::spawn(async move { gate.send_request(&target_id, envelope).await })
    }

    /// Send a ConnectionEvent to simulate state changes
    pub fn send_event(&self, event: crate::transport::ConnectionEvent) {
        let _ = self.coordinator.event_sender().send(event);
    }

    /// Start an echo responder on this peer.
    ///
    /// Receives RPC requests from the coordinator and sends back "pong" responses.
    /// Call this on the **target** peer before sending requests.
    pub fn start_echo_responder(&self, name: &str) -> tokio::task::JoinHandle<()> {
        spawn_echo_responder(self.coordinator.clone(), self.gate.clone(), name)
    }

    /// Start a response receiver on this peer.
    ///
    /// Receives RPC responses from the coordinator and routes them to
    /// `gate.handle_response()` to wake up pending requests.
    /// Call this on the **source** peer before sending requests.
    pub fn start_response_receiver(&self, name: &str) -> tokio::task::JoinHandle<()> {
        spawn_response_receiver(self.coordinator.clone(), self.gate.clone(), name)
    }

    /// Start a direction-aware RPC dispatcher on this peer.
    ///
    /// This is the sole coordinator receive-loop consumer for tests where a
    /// peer must handle both incoming requests and incoming responses.
    pub fn start_rpc_dispatcher(&self, name: &str) -> tokio::task::JoinHandle<()> {
        spawn_rpc_dispatcher(self.coordinator.clone(), self.gate.clone(), name)
    }
}

/// Test harness supporting dynamic multi-peer topologies with optional VNet.
///
/// Manages a signaling server, optional VNet pair, and a collection of `TestPeer`s
/// indexed by their serial number.
pub struct TestHarness {
    /// Shared signaling server
    pub server: TestSignalingServer,
    /// Optional VNet pair for network simulation
    pub vnet: Option<VNetPair>,
    /// Peers indexed by serial_number
    peers: HashMap<u64, TestPeer>,
    /// Background task handles (echo responders, response receivers)
    _bg_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl TestHarness {
    /// Create a new TestHarness without VNet (no network simulation).
    pub async fn new() -> Self {
        let server = TestSignalingServer::start()
            .await
            .expect("Failed to start signaling server");
        Self {
            server,
            vnet: None,
            peers: HashMap::new(),
            _bg_tasks: Vec::new(),
        }
    }

    /// Create a new TestHarness with VNet for network disconnection simulation.
    pub async fn with_vnet() -> Self {
        let server = TestSignalingServer::start()
            .await
            .expect("Failed to start signaling server");
        let vnet = VNetPair::new().await.expect("Failed to create VNet pair");
        Self {
            server,
            vnet: Some(vnet),
            peers: HashMap::new(),
            _bg_tasks: Vec::new(),
        }
    }

    /// Add a peer with the given serial number.
    ///
    /// If VNet is enabled, the peer is assigned to offerer or answerer network
    /// based on addition order (first = offerer, rest = answerer).
    /// This supports the common topology of "multiple peers connecting to one hub".
    ///
    /// # Arguments
    /// - `serial`: Serial number for the ActrId (used as peer key)
    pub async fn add_peer(&mut self, serial: u64) {
        assert!(
            !self.peers.contains_key(&serial),
            "Peer with serial {} already exists",
            serial
        );

        let id = make_actor_id(serial);
        let server_url = self.server.url();

        let (coordinator, signaling_client) = if let Some(ref vnet) = self.vnet {
            // With VNet: assign first peer to offerer net, rest to answerer net
            let net = if self.peers.is_empty() {
                vnet.net_offerer.clone()
            } else {
                vnet.net_answerer.clone()
            };
            create_peer_with_vnet(id.clone(), &server_url, net)
                .await
                .expect("Failed to create peer with vnet")
        } else {
            // Without VNet: standard WebSocket connection
            create_peer_with_websocket(id.clone(), &server_url)
                .await
                .expect("Failed to create peer")
        };

        // Build PeerGate with full transport stack
        let wire_config = DefaultWireBuilderConfig::default();
        let wire_builder = Arc::new(DefaultWireBuilder::new(
            Some(coordinator.clone()),
            wire_config,
        ));
        let transport_manager = Arc::new(PeerTransport::new(id.clone(), wire_builder));
        let gate = Arc::new(PeerGate::new(
            transport_manager.clone(),
            Some(coordinator.clone()),
        ));

        self.peers.insert(
            serial,
            TestPeer {
                id,
                coordinator,
                signaling_client,
                gate,
                transport_manager,
            },
        );

        tracing::info!("✅ Added test peer with serial {}", serial);
    }

    /// Get a reference to a peer by serial number.
    ///
    /// # Panics
    /// Panics if the peer doesn't exist.
    pub fn peer(&self, serial: u64) -> &TestPeer {
        self.peers
            .get(&serial)
            .unwrap_or_else(|| panic!("Peer with serial {} not found", serial))
    }

    /// Get a mutable reference to a peer by serial number.
    pub fn peer_mut(&mut self, serial: u64) -> &mut TestPeer {
        self.peers
            .get_mut(&serial)
            .unwrap_or_else(|| panic!("Peer with serial {} not found", serial))
    }

    /// Get all peer serial numbers.
    pub fn peer_serials(&self) -> Vec<u64> {
        self.peers.keys().copied().collect()
    }

    /// Get total peer count.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Establish a connection from one peer to another **by sending a message
    /// through the PeerGate**.
    ///
    /// This triggers the full transport stack:
    /// `PeerGate → PeerTransport (lazy create) → WireBuilder → WebRTC`
    ///
    /// Internally:
    /// 1. Starts an **echo responder** on the target peer
    /// 2. Starts a **response receiver** on the source peer
    /// 3. Sends a test RPC request through the source peer's gate
    /// 4. The transport manager lazily creates the WebRTC connection
    /// 5. The echo responder sends back a "pong" response
    /// 6. Request success = end-to-end connection verified
    ///
    /// # Arguments
    /// - `from_serial`: Initiating peer's serial number
    /// - `to_serial`: Target peer's serial number
    pub async fn connect(&mut self, from_serial: u64, to_serial: u64) {
        self.connect_with_timeout(from_serial, to_serial, Duration::from_secs(15))
            .await;
    }

    /// Establish a connection with custom timeout (via gate message).
    pub async fn connect_with_timeout(
        &mut self,
        from_serial: u64,
        to_serial: u64,
        timeout: Duration,
    ) {
        // Extract Arc handles first to avoid borrow overlap with self._bg_tasks
        let (from_coord, from_gate, to_coord, to_gate, target_id) = {
            let from_peer = self.peer(from_serial);
            let to_peer = self.peer(to_serial);
            (
                from_peer.coordinator.clone(),
                from_peer.gate.clone(),
                to_peer.coordinator.clone(),
                to_peer.gate.clone(),
                to_peer.id.clone(),
            )
        };

        tracing::info!(
            "🔗 Connecting peer {} → peer {} (via gate message)...",
            from_serial,
            to_serial
        );

        // 1. Start echo responder on target peer (receives requests, sends "pong")
        let echo_handle = spawn_echo_responder(to_coord, to_gate, &format!("echo_{}", to_serial));
        self._bg_tasks.push(echo_handle);

        // 2. Start response receiver on source peer (routes responses to gate.handle_response)
        let recv_handle = spawn_response_receiver(
            from_coord,
            from_gate.clone(),
            &format!("recv_{}", from_serial),
        );
        self._bg_tasks.push(recv_handle);

        // 3. Send a test request through the gate — this triggers lazy connection creation
        let request_id = format!("connect_test_{}_{}", from_serial, to_serial);

        let envelope = RpcEnvelope {
            request_id: request_id.clone(),
            route_key: "test.ping".to_string(),
            payload: Some(bytes::Bytes::from("ping")),
            direction: Some(actr_protocol::Direction::Request as i32),
            timeout_ms: timeout.as_millis() as i64,
            ..Default::default()
        };

        match tokio::time::timeout(timeout, from_gate.send_request(&target_id, envelope)).await {
            Ok(Ok(response)) => {
                tracing::info!(
                    "✅ Connection established and verified: {} → {} (response: {} bytes)",
                    from_serial,
                    to_serial,
                    response.len()
                );
            }
            Ok(Err(e)) => panic!("Connection {} → {} failed: {}", from_serial, to_serial, e),
            Err(_) => panic!(
                "Connection {} → {} timed out after {:?}",
                from_serial, to_serial, timeout
            ),
        }

        // Brief stabilization delay
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    /// Block all network traffic AND pause signaling (simulate full network outage).
    ///
    /// This simulates a real-world disconnection where:
    /// - UDP traffic is blocked (VNet) → ICE connectivity checks fail
    /// - Signaling messages stop forwarding → ICE restart offers can't reach the peer
    ///
    /// # Panics
    /// Panics if VNet is not enabled.
    pub fn simulate_disconnect(&self) {
        let vnet = self
            .vnet
            .as_ref()
            .expect("simulate_disconnect requires VNet (use TestHarness::with_vnet())");
        tracing::warn!("🔴 Simulating full network disconnection (VNet + signaling)");
        vnet.block_network();
        self.server.pause_forwarding();
    }

    /// Unblock network traffic AND resume signaling (simulate network recovery).
    ///
    /// # Panics
    /// Panics if VNet is not enabled.
    pub fn simulate_reconnect(&self) {
        let vnet = self
            .vnet
            .as_ref()
            .expect("simulate_reconnect requires VNet (use TestHarness::with_vnet())");
        tracing::info!("🟢 Simulating network recovery (VNet + signaling)");
        self.server.resume_forwarding();
        vnet.unblock_network();
    }

    /// Check if network is currently blocked.
    pub fn is_disconnected(&self) -> bool {
        self.vnet.as_ref().is_some_and(|v| v.is_blocked())
    }

    /// Wait until the signaling server's ICE restart count increases beyond `min_count`.
    ///
    /// # Arguments
    /// - `min_count`: Minimum expected ICE restart count
    /// - `timeout`: Maximum time to wait
    ///
    /// # Returns
    /// The final ICE restart count
    pub async fn wait_for_ice_restart_count(&self, min_count: u32, timeout: Duration) -> u32 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let count = self.server.get_ice_restart_count();
            if count >= min_count {
                return count;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "Timed out waiting for ICE restart count >= {} (current: {})",
                    min_count, count
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait until the signaling server's ICE restart request count reaches `min_count`.
    pub async fn wait_for_ice_restart_request_count(
        &self,
        min_count: u32,
        timeout: Duration,
    ) -> u32 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let count = self.server.get_ice_restart_request_count();
            if count >= min_count {
                return count;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "Timed out waiting for ICE restart request count >= {} (current: {})",
                    min_count, count
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Reset the signaling server's counters.
    pub fn reset_counters(&self) {
        self.server.reset_counters();
    }

    /// Get current ICE restart count from the signaling server.
    pub fn ice_restart_count(&self) -> u32 {
        self.server.get_ice_restart_count()
    }
}
