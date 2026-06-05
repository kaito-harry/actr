use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use actr_hyper::lifecycle::{
    AppLifecycleState, CleanupReason, ConnectionFact, ConnectionSupervisor, CredentialState,
    DebounceConfig, DefaultNetworkEventProcessor, NetworkAvailability, NetworkEvent,
    NetworkEventHandle, NetworkEventProcessor, NetworkEventRequest, NetworkEventResult,
    NetworkRecoveryAction, NetworkSnapshot, NetworkTransportFlags, ReconnectReason,
    process_network_event_batch, run_network_event_reconciler, select_network_recovery_action,
};
use actr_hyper::transport::{NetworkError, NetworkResult};
use actr_hyper::wire::webrtc::{DisconnectReason, SignalingClient, SignalingEvent, SignalingStats};
use actr_protocol::{
    AIdCredential, ActrId, Pong, RegisterRequest, RegisterResponse, RouteCandidatesRequest,
    RouteCandidatesResponse, SignalingEnvelope, UnregisterResponse,
};
use tokio::sync::broadcast;

struct FakeSignalingClient {
    connected: AtomicBool,
    connections: AtomicU64,
    connect_once_calls: AtomicU64,
    disconnections: AtomicU64,
    probe_calls: AtomicU64,
    probe_success: AtomicBool,
    event_tx: broadcast::Sender<SignalingEvent>,
    connect_delay: Duration,
    connect_once_delay: Duration,
}

impl FakeSignalingClient {
    fn new() -> Self {
        Self::new_with_delays(Duration::ZERO, Duration::ZERO)
    }

    fn new_with_delays(connect_delay: Duration, connect_once_delay: Duration) -> Self {
        let (event_tx, _event_rx) = broadcast::channel(64);
        Self {
            connected: AtomicBool::new(false),
            connections: AtomicU64::new(0),
            connect_once_calls: AtomicU64::new(0),
            disconnections: AtomicU64::new(0),
            probe_calls: AtomicU64::new(0),
            probe_success: AtomicBool::new(true),
            event_tx,
            connect_delay,
            connect_once_delay,
        }
    }

    fn stats(&self) -> SignalingStats {
        SignalingStats {
            connections: self.connections.load(Ordering::SeqCst),
            disconnections: self.disconnections.load(Ordering::SeqCst),
            ..SignalingStats::default()
        }
    }

    fn connect_once_calls(&self) -> u64 {
        self.connect_once_calls.load(Ordering::SeqCst)
    }

    fn probe_calls(&self) -> u64 {
        self.probe_calls.load(Ordering::SeqCst)
    }

    fn set_probe_success(&self, success: bool) {
        self.probe_success.store(success, Ordering::SeqCst);
    }

    fn publish_connected(&self) {
        self.connected.store(true, Ordering::SeqCst);
        self.connections.fetch_add(1, Ordering::SeqCst);
        let _ = self.event_tx.send(SignalingEvent::Connected);
    }
}

#[async_trait::async_trait]
impl SignalingClient for FakeSignalingClient {
    async fn connect(&self) -> NetworkResult<()> {
        if !self.connect_delay.is_zero() {
            tokio::time::sleep(self.connect_delay).await;
        }
        self.publish_connected();
        Ok(())
    }

    async fn connect_once(&self) -> NetworkResult<()> {
        self.connect_once_calls.fetch_add(1, Ordering::SeqCst);
        if !self.connect_once_delay.is_zero() {
            tokio::time::sleep(self.connect_once_delay).await;
        }
        self.publish_connected();
        Ok(())
    }

    async fn disconnect(&self) -> NetworkResult<()> {
        self.connected.store(false, Ordering::SeqCst);
        self.disconnections.fetch_add(1, Ordering::SeqCst);
        let _ = self.event_tx.send(SignalingEvent::Disconnected {
            reason: DisconnectReason::Manual,
        });
        Ok(())
    }

    async fn probe_alive(&self, _timeout: Duration) -> NetworkResult<()> {
        self.probe_calls.fetch_add(1, Ordering::SeqCst);
        if !self.is_connected() {
            return Err(NetworkError::ConnectionError(
                "fake signaling is disconnected".to_string(),
            ));
        }
        if self.probe_success.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(NetworkError::TimeoutError(
                "fake signaling probe timed out".to_string(),
            ))
        }
    }

    async fn send_register_request(
        &self,
        _request: RegisterRequest,
    ) -> NetworkResult<RegisterResponse> {
        Err(NetworkError::NotImplemented(
            "register request not implemented in fake client".to_string(),
        ))
    }

    async fn send_unregister_request(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _reason: Option<String>,
    ) -> NetworkResult<UnregisterResponse> {
        Err(NetworkError::NotImplemented(
            "unregister request not implemented in fake client".to_string(),
        ))
    }

    async fn send_heartbeat(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _availability: actr_protocol::ServiceAvailabilityState,
        _power_reserve: f32,
        _mailbox_backlog: f32,
    ) -> NetworkResult<Pong> {
        Err(NetworkError::NotImplemented(
            "heartbeat not implemented in fake client".to_string(),
        ))
    }

    async fn send_route_candidates_request(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _request: RouteCandidatesRequest,
    ) -> NetworkResult<RouteCandidatesResponse> {
        Err(NetworkError::NotImplemented(
            "route candidates not implemented in fake client".to_string(),
        ))
    }

    async fn get_signing_key(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _key_id: u32,
    ) -> NetworkResult<(u32, Vec<u8>)> {
        Err(NetworkError::NotImplemented(
            "get_signing_key not implemented in fake client".to_string(),
        ))
    }

    async fn send_credential_update_request(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
    ) -> NetworkResult<RegisterResponse> {
        Err(NetworkError::NotImplemented(
            "credential update not implemented in fake client".to_string(),
        ))
    }

    async fn send_envelope(&self, _envelope: SignalingEnvelope) -> NetworkResult<()> {
        Err(NetworkError::NotImplemented(
            "send_envelope not implemented in fake client".to_string(),
        ))
    }

    async fn receive_envelope(&self) -> NetworkResult<Option<SignalingEnvelope>> {
        Err(NetworkError::NotImplemented(
            "receive_envelope not implemented in fake client".to_string(),
        ))
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    fn get_stats(&self) -> SignalingStats {
        self.stats()
    }

    fn subscribe_events(&self) -> broadcast::Receiver<SignalingEvent> {
        self.event_tx.subscribe()
    }

    async fn set_actor_id(&self, _actor_id: ActrId) {}

    async fn set_credential_state(&self, _credential_state: CredentialState) {}

    async fn clear_identity(&self) {}
}

fn snapshot(
    sequence: u64,
    availability: NetworkAvailability,
    wifi: bool,
    cellular: bool,
    vpn: bool,
) -> NetworkSnapshot {
    snapshot_with_flags(
        sequence,
        availability,
        NetworkTransportFlags {
            wifi,
            cellular,
            ethernet: false,
            vpn,
            other: false,
        },
        false,
        false,
    )
}

fn snapshot_with_flags(
    sequence: u64,
    availability: NetworkAvailability,
    transport: NetworkTransportFlags,
    is_expensive: bool,
    is_constrained: bool,
) -> NetworkSnapshot {
    NetworkSnapshot {
        sequence,
        availability,
        transport,
        is_expensive,
        is_constrained,
    }
}

fn path_event(snapshot: NetworkSnapshot) -> NetworkEvent {
    NetworkEvent::NetworkPathChanged { snapshot }
}

fn online_event(sequence: u64) -> NetworkEvent {
    NetworkEvent::NetworkPathChanged {
        snapshot: snapshot(sequence, NetworkAvailability::Available, true, false, false),
    }
}

fn offline_event(sequence: u64) -> NetworkEvent {
    NetworkEvent::NetworkPathChanged {
        snapshot: snapshot(
            sequence,
            NetworkAvailability::Unavailable,
            false,
            false,
            false,
        ),
    }
}

fn wifi_event(sequence: u64) -> NetworkEvent {
    online_event(sequence)
}

fn cellular_event(sequence: u64) -> NetworkEvent {
    NetworkEvent::NetworkPathChanged {
        snapshot: snapshot(sequence, NetworkAvailability::Available, false, true, false),
    }
}

fn foreground_event(background_duration_ms: u64) -> NetworkEvent {
    NetworkEvent::AppLifecycleChanged {
        state: AppLifecycleState::Foreground {
            background_duration_ms,
        },
    }
}

fn background_event() -> NetworkEvent {
    NetworkEvent::AppLifecycleChanged {
        state: AppLifecycleState::Background,
    }
}

#[test]
fn test_l0_documented_event_action_matrix() {
    struct Case {
        case_id: &'static str,
        events: Vec<NetworkEvent>,
        expected: NetworkRecoveryAction,
    }

    let legacy_available = |sequence| {
        path_event(snapshot(
            sequence,
            NetworkAvailability::Available,
            false,
            false,
            false,
        ))
    };

    let cases = vec![
        Case {
            case_id: "L0-01 empty events",
            events: vec![],
            expected: NetworkRecoveryAction::Noop,
        },
        Case {
            case_id: "L0-02 available",
            events: vec![online_event(1)],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-03 unavailable",
            events: vec![offline_event(1)],
            expected: NetworkRecoveryAction::Offline,
        },
        Case {
            case_id: "L0-04 wifi",
            events: vec![wifi_event(1)],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-05 cellular",
            events: vec![cellular_event(1)],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-06 other transport",
            events: vec![path_event(snapshot_with_flags(
                1,
                NetworkAvailability::Available,
                NetworkTransportFlags {
                    other: true,
                    ..Default::default()
                },
                false,
                false,
            ))],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-07 cleanup",
            events: vec![NetworkEvent::CleanupConnections {
                reason: CleanupReason::ManualReset,
            }],
            expected: NetworkRecoveryAction::CleanupOnly,
        },
        Case {
            case_id: "L0-08 unavailable then available",
            events: vec![offline_event(1), wifi_event(2)],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-09 available then unavailable",
            events: vec![online_event(1), offline_event(2)],
            expected: NetworkRecoveryAction::Offline,
        },
        Case {
            case_id: "L0-10 cleanup before available",
            events: vec![
                NetworkEvent::CleanupConnections {
                    reason: CleanupReason::ManualReset,
                },
                wifi_event(1),
            ],
            expected: NetworkRecoveryAction::CleanupOnly,
        },
        Case {
            case_id: "L0-11 cleanup suppresses later restore",
            events: vec![
                online_event(1),
                NetworkEvent::CleanupConnections {
                    reason: CleanupReason::ManualReset,
                },
                cellular_event(2),
            ],
            expected: NetworkRecoveryAction::CleanupOnly,
        },
        Case {
            case_id: "L0-12 cleanup after available",
            events: vec![
                wifi_event(1),
                NetworkEvent::CleanupConnections {
                    reason: CleanupReason::ManualReset,
                },
            ],
            expected: NetworkRecoveryAction::CleanupOnly,
        },
        Case {
            case_id: "L0-13 background alone noops",
            events: vec![background_event()],
            expected: NetworkRecoveryAction::Noop,
        },
        Case {
            case_id: "L0-14 short foreground probes",
            events: vec![foreground_event(5_000)],
            expected: NetworkRecoveryAction::Probe,
        },
        Case {
            case_id: "L0-15 latest sequence wins after flapping",
            events: vec![
                offline_event(1),
                online_event(2),
                offline_event(3),
                online_event(4),
            ],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-16 legacy available maps to path available",
            events: vec![legacy_available(1)],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-17 latest wifi sequence wins",
            events: vec![offline_event(1), wifi_event(2)],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-18 unknown availability probes",
            events: vec![path_event(snapshot(
                1,
                NetworkAvailability::Unknown,
                false,
                false,
                false,
            ))],
            expected: NetworkRecoveryAction::Probe,
        },
        Case {
            case_id: "L0-19 vpn available restores",
            events: vec![path_event(snapshot_with_flags(
                1,
                NetworkAvailability::Available,
                NetworkTransportFlags {
                    vpn: true,
                    other: true,
                    ..Default::default()
                },
                false,
                false,
            ))],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-20 expensive constrained available stays restore",
            events: vec![path_event(snapshot_with_flags(
                1,
                NetworkAvailability::Available,
                NetworkTransportFlags {
                    cellular: true,
                    ..Default::default()
                },
                true,
                true,
            ))],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-21 long foreground forces reconnect",
            events: vec![foreground_event(60_000)],
            expected: NetworkRecoveryAction::ForceReconnect,
        },
        Case {
            case_id: "L0-22 long foreground and online forces reconnect",
            events: vec![foreground_event(60_000), cellular_event(2)],
            expected: NetworkRecoveryAction::ForceReconnect,
        },
        Case {
            case_id: "L0-23 force reconnect with online path",
            events: vec![
                NetworkEvent::ForceReconnect {
                    reason: ReconnectReason::ManualReconnect,
                },
                online_event(1),
            ],
            expected: NetworkRecoveryAction::ForceReconnect,
        },
        Case {
            case_id: "L0-24 offline suppresses force reconnect",
            events: vec![
                NetworkEvent::ForceReconnect {
                    reason: ReconnectReason::ManualReconnect,
                },
                offline_event(1),
            ],
            expected: NetworkRecoveryAction::Offline,
        },
        Case {
            case_id: "L0-25 app terminating cleanup",
            events: vec![NetworkEvent::CleanupConnections {
                reason: CleanupReason::AppTerminating,
            }],
            expected: NetworkRecoveryAction::CleanupOnly,
        },
        Case {
            case_id: "L0-26 older unavailable snapshot is ignored",
            events: vec![
                path_event(snapshot(
                    2,
                    NetworkAvailability::Available,
                    true,
                    false,
                    true,
                )),
                offline_event(1),
            ],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            case_id: "L0-27 long foreground stays offline with offline path",
            events: vec![foreground_event(60_000), offline_event(1)],
            expected: NetworkRecoveryAction::Offline,
        },
    ];

    for case in cases {
        assert_eq!(
            select_network_recovery_action(&case.events),
            case.expected,
            "{} selected unexpected action for {:?}",
            case.case_id,
            case.events
        );
        assert_eq!(
            ConnectionSupervisor::select_action(&case.events),
            case.expected,
            "{} supervisor selected unexpected action for {:?}",
            case.case_id,
            case.events
        );
    }
}

#[tokio::test]
async fn test_l0_duplicate_path_storms_execute_one_settled_action() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");

    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    ));

    struct Case {
        name: &'static str,
        events: Vec<NetworkEvent>,
        expected_probe_calls: u64,
        expected_connections: u64,
        expected_disconnections: u64,
    }

    let cases = vec![
        Case {
            name: "duplicate_available",
            events: (1..=10).map(online_event).collect(),
            expected_probe_calls: 1,
            expected_connections: 1,
            expected_disconnections: 0,
        },
        Case {
            name: "duplicate_unavailable",
            events: (11..=20).map(offline_event).collect(),
            expected_probe_calls: 1,
            expected_connections: 1,
            expected_disconnections: 1,
        },
    ];

    for case in cases {
        let expected_len = case.events.len();
        let results = process_network_event_batch(case.events, processor.clone()).await;
        assert_eq!(
            results.len(),
            expected_len,
            "{} should return one result per event",
            case.name
        );
        assert!(
            results.iter().all(|result| result.success),
            "{} results should all succeed: {results:?}",
            case.name
        );
        assert_eq!(
            client.probe_calls(),
            case.expected_probe_calls,
            "{} should have expected probe count",
            case.name
        );

        let stats = client.get_stats();
        assert_eq!(
            stats.connections, case.expected_connections,
            "{} should have expected connection count",
            case.name
        );
        assert_eq!(
            stats.disconnections, case.expected_disconnections,
            "{} should have expected disconnection count",
            case.name
        );
    }
}

#[tokio::test]
async fn test_network_available_probes_when_already_connected() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");

    let processor = DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    );

    processor
        .process_network_available()
        .await
        .expect("first available should succeed");

    let stats = client.get_stats();
    assert_eq!(
        stats.connections, 1,
        "Available should keep a healthy connected signaling client"
    );
    assert_eq!(
        stats.disconnections, 0,
        "Available should not disconnect when signaling probe succeeds"
    );
    assert_eq!(client.probe_calls(), 1);
    assert_eq!(client.connect_once_calls(), 0);

    processor
        .process_network_available()
        .await
        .expect("second available should be debounced");

    let stats = client.get_stats();
    assert_eq!(stats.connections, 1, "debounced call should not reconnect");
    assert_eq!(
        stats.disconnections, 0,
        "debounced call should not disconnect"
    );
    assert_eq!(client.probe_calls(), 1, "debounced call should not probe");

    tokio::time::sleep(Duration::from_millis(600)).await;

    processor
        .process_network_available()
        .await
        .expect("available after window should succeed");

    let stats = client.get_stats();
    assert_eq!(
        stats.connections, 1,
        "Available after debounce window should keep healthy signaling"
    );
    assert_eq!(stats.disconnections, 0);
    assert_eq!(
        client.probe_calls(),
        2,
        "Available after debounce window should probe again"
    );
}

#[tokio::test]
async fn test_network_available_rebuilds_when_signaling_probe_fails() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");
    client.set_probe_success(false);

    let processor = DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    );

    processor
        .process_network_available()
        .await
        .expect("available should rebuild after failed probe");

    let stats = client.get_stats();
    assert_eq!(client.probe_calls(), 1);
    assert_eq!(
        stats.disconnections, 1,
        "failed probe should disconnect the half-open signaling socket"
    );
    assert_eq!(
        stats.connections, 2,
        "failed probe should reconnect signaling once"
    );
    assert_eq!(client.connect_once_calls(), 1);
    assert!(client.is_connected());
}

#[tokio::test]
async fn test_network_available_connects_without_probe_when_disconnected() {
    let client = Arc::new(FakeSignalingClient::new());

    let processor = DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    );

    processor
        .process_network_available()
        .await
        .expect("available should connect disconnected signaling");

    let stats = client.get_stats();
    assert_eq!(client.probe_calls(), 0);
    assert_eq!(client.connect_once_calls(), 1);
    assert_eq!(stats.connections, 1);
    assert_eq!(stats.disconnections, 0);
    assert!(client.is_connected());
}

#[tokio::test]
async fn test_debounce_does_not_cross_event_types() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");

    let processor = DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    );

    processor
        .process_network_available()
        .await
        .expect("available should succeed");

    processor
        .process_network_lost()
        .await
        .expect("lost should not be debounced by available");

    let stats = client.get_stats();
    assert_eq!(
        stats.connections, 1,
        "Available should keep a healthy connected client"
    );
    assert_eq!(
        stats.disconnections, 1,
        "Lost should disconnect even when Available was processed first"
    );
    assert_eq!(client.probe_calls(), 1);
}

#[tokio::test]
async fn test_direct_available_then_type_changed_probes_each_event_type() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init()
        .ok();

    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");

    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(2000),
        },
    ));

    processor
        .process_network_available()
        .await
        .expect("first available should succeed");

    let stats_after_available = client.get_stats();
    assert_eq!(
        stats_after_available.connections, 1,
        "First Available should keep healthy connected signaling"
    );
    assert_eq!(
        stats_after_available.disconnections, 0,
        "First Available should not disconnect healthy signaling"
    );
    assert!(client.is_connected(), "Should be connected after Available");
    assert_eq!(client.probe_calls(), 1);

    tokio::time::sleep(Duration::from_millis(10)).await;

    processor
        .process_network_type_changed(true, false)
        .await
        .expect("type changed should not return error");

    let stats_after_type_changed = client.get_stats();
    assert_eq!(
        stats_after_type_changed.connections, 1,
        "TypeChanged should keep an already healthy signaling client"
    );
    assert_eq!(
        stats_after_type_changed.disconnections, 0,
        "TypeChanged should not disconnect healthy signaling"
    );
    assert_eq!(
        client.probe_calls(),
        2,
        "Available and TypeChanged should each probe when outside their debounce buckets"
    );
    assert!(
        client.is_connected(),
        "After TypeChanged, signaling should still be connected"
    );
}

#[tokio::test]
async fn test_type_changed_works_without_prior_available() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");

    let processor = DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(2000),
        },
    );

    processor
        .process_network_type_changed(true, false)
        .await
        .expect("type changed should succeed");

    let stats = client.get_stats();
    assert!(client.is_connected());
    assert_eq!(
        stats.connections, 1,
        "TypeChanged should keep healthy connected signaling"
    );
    assert_eq!(
        stats.disconnections, 0,
        "TypeChanged should not disconnect signaling when probe succeeds"
    );
    assert_eq!(client.probe_calls(), 1);
    assert_eq!(client.connect_once_calls(), 0);
}

#[tokio::test]
async fn test_batch_available_type_changed_probes_signaling_once() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");

    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    ));

    let action = select_network_recovery_action(&[online_event(1), wifi_event(2)]);
    assert_eq!(action, NetworkRecoveryAction::Restore);

    let results =
        process_network_event_batch(vec![online_event(1), wifi_event(2)], processor).await;

    assert_eq!(results.len(), 2, "each merged request should get a result");
    assert!(results.iter().all(|result| result.success));
    assert!(client.is_connected(), "signaling should remain connected");

    let stats = client.get_stats();
    assert_eq!(
        stats.connections, 1,
        "Available + TypeChanged should keep a healthy connected signaling client"
    );
    assert_eq!(
        stats.disconnections, 0,
        "Available + TypeChanged should not disconnect when probe succeeds"
    );
    assert_eq!(
        client.connect_once_calls(),
        0,
        "batched restore should not reconnect when signaling probe succeeds"
    );
    assert_eq!(
        client.probe_calls(),
        1,
        "batched restore should perform one signaling probe"
    );
}

#[tokio::test]
async fn test_batch_restore_rebuilds_once_when_signaling_probe_fails() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");
    client.set_probe_success(false);

    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    ));

    let results =
        process_network_event_batch(vec![online_event(1), cellular_event(2)], processor).await;

    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.success));
    assert!(client.is_connected());

    let stats = client.get_stats();
    assert_eq!(client.probe_calls(), 1);
    assert_eq!(
        stats.disconnections, 1,
        "batched restore should disconnect once after failed probe"
    );
    assert_eq!(
        stats.connections, 2,
        "batched restore should reconnect once after failed probe"
    );
    assert_eq!(client.connect_once_calls(), 1);
}

#[tokio::test]
async fn test_batch_lost_available_type_changed_prefers_restore() {
    let client = Arc::new(FakeSignalingClient::new());

    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    ));

    let events = vec![offline_event(1), online_event(2), cellular_event(3)];
    assert_eq!(
        select_network_recovery_action(&events),
        NetworkRecoveryAction::Restore
    );

    let results = process_network_event_batch(events, processor).await;

    assert_eq!(results.len(), 3, "each merged request should get a result");
    assert!(results.iter().all(|result| result.success));
    assert!(
        client.is_connected(),
        "signaling should be connected after restore"
    );

    let stats = client.get_stats();
    assert_eq!(stats.connections, 1);
    assert_eq!(client.connect_once_calls(), 1);
    assert_eq!(
        client.probe_calls(),
        0,
        "disconnected restore should connect directly without probing"
    );
    assert_eq!(
        stats.disconnections, 0,
        "Lost in the same settle batch as restore should not force an extra disconnect"
    );
}

#[test]
fn test_batch_action_uses_latest_network_state_event() {
    let available_last = vec![online_event(1), offline_event(2), online_event(3)];
    assert_eq!(
        select_network_recovery_action(&available_last),
        NetworkRecoveryAction::Restore,
        "Available after Lost means the settled final state is online"
    );

    let lost_last = vec![offline_event(1), online_event(2), offline_event(3)];
    assert_eq!(
        select_network_recovery_action(&lost_last),
        NetworkRecoveryAction::Offline,
        "Lost after Available means the settled final state is offline"
    );
}

#[test]
fn test_connection_supervisor_fact_matrix() {
    struct Case {
        name: &'static str,
        facts: Vec<ConnectionFact>,
        expected: NetworkRecoveryAction,
    }

    let cases = vec![
        Case {
            name: "background_only",
            facts: vec![ConnectionFact::AppEnteredBackground],
            expected: NetworkRecoveryAction::Noop,
        },
        Case {
            name: "short_foreground_without_network_fact",
            facts: vec![ConnectionFact::AppEnteredForeground {
                background_duration_ms: 5_000,
            }],
            expected: NetworkRecoveryAction::Probe,
        },
        Case {
            name: "foreground_then_online",
            facts: vec![
                ConnectionFact::AppEnteredBackground,
                ConnectionFact::AppEnteredForeground {
                    background_duration_ms: 5_000,
                },
                ConnectionFact::NetworkSnapshotChanged(snapshot(
                    1,
                    NetworkAvailability::Available,
                    true,
                    false,
                    false,
                )),
            ],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            name: "cleanup_suppresses_later_restore",
            facts: vec![
                ConnectionFact::CleanupRequested(CleanupReason::UserLogout),
                ConnectionFact::NetworkSnapshotChanged(snapshot(
                    1,
                    NetworkAvailability::Available,
                    true,
                    false,
                    false,
                )),
            ],
            expected: NetworkRecoveryAction::CleanupOnly,
        },
        Case {
            name: "latest_snapshot_sequence_wins",
            facts: vec![
                ConnectionFact::NetworkSnapshotChanged(snapshot(
                    2,
                    NetworkAvailability::Available,
                    true,
                    false,
                    true,
                )),
                ConnectionFact::NetworkSnapshotChanged(snapshot(
                    1,
                    NetworkAvailability::Unavailable,
                    false,
                    false,
                    false,
                )),
            ],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            name: "offline_suppresses_forced_reconnect",
            facts: vec![
                ConnectionFact::ForceReconnectRequested(ReconnectReason::ManualReconnect),
                ConnectionFact::NetworkSnapshotChanged(snapshot(
                    1,
                    NetworkAvailability::Unavailable,
                    false,
                    false,
                    false,
                )),
            ],
            expected: NetworkRecoveryAction::Offline,
        },
    ];

    for case in cases {
        let mut supervisor = ConnectionSupervisor::new();
        for fact in case.facts {
            supervisor.submit_fact(fact);
        }
        assert_eq!(
            supervisor.reconcile(),
            case.expected,
            "{} selected unexpected action",
            case.name
        );
    }
}

#[tokio::test]
async fn test_cleanup_batches_disconnect_without_reconnect() {
    struct Case {
        name: &'static str,
        events: Vec<NetworkEvent>,
        delayed_connect: bool,
        timeout: Option<Duration>,
    }

    let cases = vec![
        Case {
            name: "cleanup_with_available_and_wifi",
            events: vec![
                NetworkEvent::CleanupConnections {
                    reason: CleanupReason::ManualReset,
                },
                online_event(1),
                wifi_event(2),
            ],
            delayed_connect: false,
            timeout: None,
        },
        Case {
            name: "cleanup_with_available_does_not_enter_reconnect_backoff",
            events: vec![
                NetworkEvent::CleanupConnections {
                    reason: CleanupReason::ManualReset,
                },
                online_event(1),
            ],
            delayed_connect: true,
            timeout: Some(Duration::from_millis(250)),
        },
    ];

    for case in cases {
        let client = if case.delayed_connect {
            let client = Arc::new(FakeSignalingClient::new_with_delays(
                Duration::from_secs(5),
                Duration::ZERO,
            ));
            client.publish_connected();
            client
        } else {
            let client = Arc::new(FakeSignalingClient::new());
            client.connect().await.expect("initial connect");
            client
        };

        let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
            client.clone(),
            None,
            DebounceConfig {
                window: Duration::from_millis(500),
            },
        ));

        assert_eq!(
            select_network_recovery_action(&case.events),
            NetworkRecoveryAction::CleanupOnly,
            "{} should select cleanup only",
            case.name
        );

        let expected_len = case.events.len();
        let results = match case.timeout {
            Some(timeout) => {
                tokio::time::timeout(timeout, process_network_event_batch(case.events, processor))
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "{} must not be blocked by the regular reconnect backoff path",
                            case.name
                        )
                    })
            }
            None => process_network_event_batch(case.events, processor).await,
        };

        assert_eq!(
            results.len(),
            expected_len,
            "{} should return one result per merged request",
            case.name
        );
        assert!(
            results.iter().all(|result| result.success),
            "{} results should all succeed: {results:?}",
            case.name
        );
        assert!(!client.is_connected(), "{} should not reconnect", case.name);
        assert_eq!(
            client.connect_once_calls(),
            0,
            "{} should not connect_once",
            case.name
        );
        assert_eq!(client.probe_calls(), 0, "{} should not probe", case.name);

        let stats = client.get_stats();
        assert_eq!(
            stats.connections, 1,
            "{} initial connection only",
            case.name
        );
        assert_eq!(
            stats.disconnections, 1,
            "{} should preserve exactly one signaling disconnect",
            case.name
        );
    }
}

#[tokio::test]
async fn test_network_event_handle_settle_window_merges_events_once() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");

    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    ));

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);
    let handle = NetworkEventHandle::new(event_tx);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let processor: Arc<dyn NetworkEventProcessor> = processor;
    let reconciler_shutdown = shutdown.clone();

    let reconciler = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    let lost = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .handle_network_path_changed(match offline_event(1) {
                    NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                    _ => unreachable!(),
                })
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let available = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .handle_network_path_changed(match online_event(2) {
                    NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                    _ => unreachable!(),
                })
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let type_changed = tokio::spawn(async move {
        handle
            .handle_network_path_changed(match wifi_event(3) {
                NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                _ => unreachable!(),
            })
            .await
    });

    let lost_result = lost.await.expect("lost task should not panic").unwrap();
    let available_result = available
        .await
        .expect("available task should not panic")
        .unwrap();
    let type_changed_result = type_changed
        .await
        .expect("type changed task should not panic")
        .unwrap();

    assert!(lost_result.success);
    assert!(available_result.success);
    assert!(type_changed_result.success);
    assert!(matches!(
        lost_result.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));
    assert!(matches!(
        available_result.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));
    assert!(matches!(
        type_changed_result.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));
    assert!(client.is_connected());

    let stats = client.get_stats();
    assert_eq!(
        stats.connections, 1,
        "Lost + Available + TypeChanged in one settle window should keep healthy signaling"
    );
    assert_eq!(
        stats.disconnections, 0,
        "Batched restore should not disconnect when signaling probe succeeds"
    );
    assert_eq!(client.probe_calls(), 1, "Batched restore should probe once");

    shutdown.cancel();
    reconciler.await.expect("reconciler task should not panic");
}

#[tokio::test]
async fn test_repeated_foreground_restore_batches_probe_once_per_cycle() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");

    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    ));

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);
    let handle = NetworkEventHandle::new(event_tx);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let processor: Arc<dyn NetworkEventProcessor> = processor;
    let reconciler_shutdown = shutdown.clone();

    let reconciler = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    const CYCLES: u64 = 5;

    for cycle in 1..=CYCLES {
        let available = {
            let handle = handle.clone();
            tokio::spawn(async move {
                handle
                    .handle_network_path_changed(match online_event(cycle * 2) {
                        NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                        _ => unreachable!(),
                    })
                    .await
            })
        };

        tokio::time::sleep(Duration::from_millis(20)).await;

        let type_changed = {
            let handle = handle.clone();
            tokio::spawn(async move {
                handle
                    .handle_network_path_changed(match cellular_event(cycle * 2 + 1) {
                        NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                        _ => unreachable!(),
                    })
                    .await
            })
        };

        let available_result = available
            .await
            .expect("available task should not panic")
            .unwrap();
        let type_changed_result = type_changed
            .await
            .expect("type changed task should not panic")
            .unwrap();

        assert!(
            available_result.success,
            "foreground Available should succeed in cycle {}",
            cycle
        );
        assert!(
            type_changed_result.success,
            "foreground TypeChanged should succeed in cycle {}",
            cycle
        );
        assert!(
            client.is_connected(),
            "signaling should remain connected after foreground cycle {}",
            cycle
        );

        let stats = client.get_stats();
        assert_eq!(
            stats.connections, 1,
            "foreground cycle {} should keep the original healthy signaling connection",
            cycle
        );
        assert_eq!(
            stats.disconnections, 0,
            "foreground cycle {} should not disconnect healthy signaling",
            cycle
        );
        assert_eq!(
            client.connect_once_calls(),
            0,
            "foreground cycle {} should not reconnect healthy signaling",
            cycle
        );
        assert_eq!(
            client.probe_calls(),
            cycle,
            "foreground cycle {} should probe once for the settled restore batch",
            cycle
        );
    }

    shutdown.cancel();
    reconciler.await.expect("reconciler task should not panic");
}

#[tokio::test]
async fn test_l1_pre_start_queued_event_drains_when_reconciler_starts() {
    let client = Arc::new(FakeSignalingClient::new());
    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    ));

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_secs(2));

    let pre_start_call = tokio::spawn(async move {
        handle
            .handle_network_path_changed(match online_event(1) {
                NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                _ => unreachable!(),
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !pre_start_call.is_finished(),
        "pre-start event should wait while the reconciler is not running"
    );

    let shutdown = tokio_util::sync::CancellationToken::new();
    let processor: Arc<dyn NetworkEventProcessor> = processor;
    let reconciler_shutdown = shutdown.clone();
    let reconciler = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    let result = pre_start_call
        .await
        .expect("pre-start event task should not panic")
        .expect("queued pre-start event should complete after reconciler starts");
    assert!(result.success);
    assert!(matches!(
        result.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));
    assert!(client.is_connected());
    assert_eq!(client.connect_once_calls(), 1);

    shutdown.cancel();
    reconciler.await.expect("reconciler task should not panic");
}

#[tokio::test]
async fn test_l1_old_handle_after_reconciler_shutdown_fails_fast() {
    let client = Arc::new(FakeSignalingClient::new());
    let processor = Arc::new(DefaultNetworkEventProcessor::new(client, None));
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_millis(100));

    let shutdown = tokio_util::sync::CancellationToken::new();
    let processor: Arc<dyn NetworkEventProcessor> = processor;
    let reconciler_shutdown = shutdown.clone();
    let reconciler = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    shutdown.cancel();
    reconciler.await.expect("reconciler task should not panic");

    let err = handle
        .handle_network_path_changed(match online_event(1) {
            NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
            _ => unreachable!(),
        })
        .await
        .expect_err("old handle should fail after reconciler shutdown");

    assert!(
        err.contains("Failed to send network event"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_l1_reconciler_shutdown_during_settle_window_is_bounded() {
    let client = Arc::new(FakeSignalingClient::new());
    let processor = Arc::new(DefaultNetworkEventProcessor::new(client, None));
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_millis(150));

    let shutdown = tokio_util::sync::CancellationToken::new();
    let processor: Arc<dyn NetworkEventProcessor> = processor;
    let reconciler_shutdown = shutdown.clone();
    let reconciler = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    let event_call = tokio::spawn(async move {
        handle
            .handle_network_path_changed(match online_event(1) {
                NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                _ => unreachable!(),
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(25)).await;
    shutdown.cancel();
    reconciler.await.expect("reconciler task should not panic");

    let err = event_call
        .await
        .expect("event call should not panic")
        .expect_err("shutdown during settle should not leave caller waiting forever");
    assert!(
        err.contains("Timed out waiting for network event result")
            || err.contains("Failed to receive network event result"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_l1_command_apis_complete_through_network_event_handle() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");
    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(500),
        },
    ));

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_secs(2));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let processor: Arc<dyn NetworkEventProcessor> = processor;
    let reconciler_shutdown = shutdown.clone();
    let reconciler = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    let cleanup = handle
        .cleanup_connections(CleanupReason::ManualReset)
        .await
        .expect("cleanup command should complete through handle");
    assert!(cleanup.success);
    assert!(matches!(
        cleanup.event,
        NetworkEvent::CleanupConnections {
            reason: CleanupReason::ManualReset
        }
    ));
    assert!(!client.is_connected());

    let reconnect = handle
        .force_reconnect(ReconnectReason::ManualReconnect)
        .await
        .expect("force reconnect command should complete through handle");
    assert!(reconnect.success);
    assert!(matches!(
        reconnect.event,
        NetworkEvent::ForceReconnect {
            reason: ReconnectReason::ManualReconnect
        }
    ));
    assert!(client.is_connected());
    assert_eq!(client.connect_once_calls(), 1);

    shutdown.cancel();
    reconciler.await.expect("reconciler task should not panic");
}

#[tokio::test]
async fn test_network_event_handle_fails_fast_when_receiver_closed() {
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);
    drop(event_rx);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_millis(100));

    let err = handle
        .handle_network_path_changed(match online_event(1) {
            NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
            _ => unreachable!(),
        })
        .await
        .expect_err("closed network event receiver should fail");

    assert!(
        err.contains("Failed to send network event"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_network_event_handle_pending_request_is_bounded_by_deadline() {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_millis(100));

    let call = tokio::spawn(async move {
        handle
            .handle_network_path_changed(match online_event(1) {
                NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                _ => unreachable!(),
            })
            .await
    });
    let _request = event_rx
        .recv()
        .await
        .expect("request should be queued before timeout");

    let err = call
        .await
        .expect("event call should not panic")
        .expect_err("pending request should time out");

    assert!(
        err.contains("Timed out waiting for network event result"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_reconciler_ignores_cancelled_network_event_callers() {
    let client = Arc::new(FakeSignalingClient::new());
    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client,
        None,
        DebounceConfig {
            window: Duration::from_millis(10),
        },
    ));

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_secs(1));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let processor: Arc<dyn NetworkEventProcessor> = processor;
    let reconciler_shutdown = shutdown.clone();

    let reconciler = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    let cancelled = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .handle_network_path_changed(match online_event(1) {
                    NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                    _ => unreachable!(),
                })
                .await
        })
    };
    cancelled.abort();

    let result = handle
        .handle_network_path_changed(match offline_event(2) {
            NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
            _ => unreachable!(),
        })
        .await
        .expect("subsequent event should still complete");
    assert!(matches!(
        result.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));
    assert!(result.success);

    shutdown.cancel();
    reconciler.await.expect("reconciler task should not panic");
}

#[tokio::test]
async fn test_l1_handle_drop_while_event_pending_does_not_poison_reconciler() {
    let client = Arc::new(FakeSignalingClient::new());
    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client,
        None,
        DebounceConfig {
            window: Duration::from_millis(10),
        },
    ));

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);
    let pending_handle =
        NetworkEventHandle::new_with_result_timeout(event_tx.clone(), Duration::from_secs(1));
    let live_handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_secs(2));

    let pending_call = tokio::spawn(async move {
        pending_handle
            .handle_network_path_changed(match online_event(1) {
                NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                _ => unreachable!(),
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(25)).await;
    pending_call.abort();
    let _ = pending_call.await;

    let shutdown = tokio_util::sync::CancellationToken::new();
    let processor: Arc<dyn NetworkEventProcessor> = processor;
    let reconciler_shutdown = shutdown.clone();
    let reconciler = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    let result = live_handle
        .handle_network_path_changed(match offline_event(2) {
            NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
            _ => unreachable!(),
        })
        .await
        .expect("new event should complete after old handle was dropped while pending");
    assert!(result.success);
    assert!(matches!(
        result.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));

    shutdown.cancel();
    reconciler.await.expect("reconciler task should not panic");
}

#[tokio::test]
async fn test_network_event_handle_preserves_per_request_result_correlation() {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<NetworkEventRequest>(10);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_secs(1));

    let available = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .handle_network_path_changed(match online_event(1) {
                    NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                    _ => unreachable!(),
                })
                .await
        })
    };
    let lost = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .handle_network_path_changed(match offline_event(2) {
                    NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                    _ => unreachable!(),
                })
                .await
        })
    };

    let first = event_rx.recv().await.expect("first request");
    let second = event_rx.recv().await.expect("second request");

    second
        .result_tx
        .send(NetworkEventResult::success(second.event.clone(), 1))
        .expect("second caller should receive result");
    first
        .result_tx
        .send(NetworkEventResult::success(first.event.clone(), 1))
        .expect("first caller should receive result");

    let available_result = available
        .await
        .expect("available task should not panic")
        .expect("available should complete");
    let lost_result = lost
        .await
        .expect("lost task should not panic")
        .expect("lost should complete");

    assert!(matches!(
        available_result.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));
    assert!(matches!(
        lost_result.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));
}

#[tokio::test]
async fn test_l1_cloned_handles_mixed_concurrent_calls_complete_without_crossed_results() {
    let client = Arc::new(FakeSignalingClient::new());
    client.connect().await.expect("initial connect");
    let processor = Arc::new(DefaultNetworkEventProcessor::new_with_debounce(
        client.clone(),
        None,
        DebounceConfig {
            window: Duration::from_millis(50),
        },
    ));

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, Duration::from_secs(2));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let processor: Arc<dyn NetworkEventProcessor> = processor;
    let reconciler_shutdown = shutdown.clone();
    let reconciler = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    let network = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .handle_network_path_changed(match online_event(1) {
                    NetworkEvent::NetworkPathChanged { snapshot } => snapshot,
                    _ => unreachable!(),
                })
                .await
        })
    };
    let lifecycle = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .handle_app_lifecycle_changed(AppLifecycleState::Foreground {
                    background_duration_ms: 5_000,
                })
                .await
        })
    };
    let reconnect = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .force_reconnect(ReconnectReason::ManualReconnect)
                .await
        })
    };

    let network = network
        .await
        .expect("network task should not panic")
        .expect("network event should complete");
    let lifecycle = lifecycle
        .await
        .expect("lifecycle task should not panic")
        .expect("lifecycle event should complete");
    let reconnect = reconnect
        .await
        .expect("reconnect task should not panic")
        .expect("reconnect command should complete");

    assert!(matches!(
        network.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));
    assert!(matches!(
        lifecycle.event,
        NetworkEvent::AppLifecycleChanged {
            state: AppLifecycleState::Foreground { .. }
        }
    ));
    assert!(matches!(
        reconnect.event,
        NetworkEvent::ForceReconnect {
            reason: ReconnectReason::ManualReconnect
        }
    ));
    assert!(network.success && lifecycle.success && reconnect.success);

    shutdown.cancel();
    reconciler.await.expect("reconciler task should not panic");
}
