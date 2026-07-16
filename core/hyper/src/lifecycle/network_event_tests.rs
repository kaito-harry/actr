use super::*;
use crate::lifecycle::CredentialState;
use crate::transport::{NetworkError, NetworkResult};
use crate::wire::webrtc::{SignalingEvent, SignalingStats};
use actr_protocol::{
    AIdCredential, ActrId, Pong, RegisterRequest, RegisterResponse, RouteCandidatesRequest,
    RouteCandidatesResponse, ServiceAvailabilityState, SignalingEnvelope, UnregisterResponse,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use tokio::sync::broadcast;

struct ForceReconnectFakeSignalingClient {
    connected: AtomicBool,
    connect_once_should_fail: bool,
    disconnect_calls: AtomicUsize,
    connect_once_calls: AtomicUsize,
    auto_reconnect_suppressed: AtomicBool,
    suppress_auto_reconnect_calls: AtomicUsize,
    schedule_auto_reconnect_calls: AtomicUsize,
    schedule_auto_reconnect_reset_backoff_calls: AtomicUsize,
    event_tx: broadcast::Sender<SignalingEvent>,
}

impl ForceReconnectFakeSignalingClient {
    fn new(connect_once_should_fail: bool) -> Self {
        let (event_tx, _rx) = broadcast::channel(8);
        Self {
            connected: AtomicBool::new(false),
            connect_once_should_fail,
            disconnect_calls: AtomicUsize::new(0),
            connect_once_calls: AtomicUsize::new(0),
            auto_reconnect_suppressed: AtomicBool::new(false),
            suppress_auto_reconnect_calls: AtomicUsize::new(0),
            schedule_auto_reconnect_calls: AtomicUsize::new(0),
            schedule_auto_reconnect_reset_backoff_calls: AtomicUsize::new(0),
            event_tx,
        }
    }
}

#[async_trait::async_trait]
impl SignalingClient for ForceReconnectFakeSignalingClient {
    async fn connect(&self) -> NetworkResult<()> {
        Ok(())
    }

    async fn connect_once(&self) -> NetworkResult<()> {
        self.connect_once_calls.fetch_add(1, AtomicOrdering::SeqCst);
        if self.connect_once_should_fail {
            return Err(NetworkError::ConnectionError(
                "forced connect_once failure".to_string(),
            ));
        }

        self.connected.store(true, AtomicOrdering::SeqCst);
        Ok(())
    }

    fn suppress_auto_reconnect(&self) {
        self.auto_reconnect_suppressed
            .store(true, AtomicOrdering::SeqCst);
        self.suppress_auto_reconnect_calls
            .fetch_add(1, AtomicOrdering::SeqCst);
    }

    fn schedule_auto_reconnect(&self) {
        self.auto_reconnect_suppressed
            .store(false, AtomicOrdering::SeqCst);
        self.schedule_auto_reconnect_calls
            .fetch_add(1, AtomicOrdering::SeqCst);
    }

    fn schedule_auto_reconnect_reset_backoff(&self) {
        self.schedule_auto_reconnect_reset_backoff_calls
            .fetch_add(1, AtomicOrdering::SeqCst);
        self.schedule_auto_reconnect();
    }

    async fn disconnect(&self) -> NetworkResult<()> {
        self.disconnect_calls.fetch_add(1, AtomicOrdering::SeqCst);
        self.suppress_auto_reconnect();
        self.connected.store(false, AtomicOrdering::SeqCst);
        Ok(())
    }

    async fn send_register_request(
        &self,
        _request: RegisterRequest,
    ) -> NetworkResult<RegisterResponse> {
        Err(NetworkError::ConnectionError("unused".to_string()))
    }

    async fn send_unregister_request(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _reason: Option<String>,
    ) -> NetworkResult<UnregisterResponse> {
        Err(NetworkError::ConnectionError("unused".to_string()))
    }

    async fn send_heartbeat(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _availability: ServiceAvailabilityState,
        _power_reserve: f32,
        _mailbox_backlog: f32,
    ) -> NetworkResult<Pong> {
        Err(NetworkError::ConnectionError("unused".to_string()))
    }

    async fn send_route_candidates_request(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _request: RouteCandidatesRequest,
    ) -> NetworkResult<RouteCandidatesResponse> {
        Err(NetworkError::ConnectionError("unused".to_string()))
    }

    async fn get_signing_key(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _key_id: u32,
    ) -> NetworkResult<(u32, Vec<u8>)> {
        Err(NetworkError::ConnectionError("unused".to_string()))
    }

    async fn send_envelope(&self, _envelope: SignalingEnvelope) -> NetworkResult<()> {
        Err(NetworkError::ConnectionError("unused".to_string()))
    }

    async fn receive_envelope(&self) -> NetworkResult<Option<SignalingEnvelope>> {
        Err(NetworkError::ConnectionError("unused".to_string()))
    }

    fn is_connected(&self) -> bool {
        self.connected.load(AtomicOrdering::SeqCst)
    }

    fn get_stats(&self) -> SignalingStats {
        SignalingStats::default()
    }

    fn subscribe_events(&self) -> broadcast::Receiver<SignalingEvent> {
        self.event_tx.subscribe()
    }

    async fn set_actor_id(&self, _actor_id: ActrId) {}

    async fn set_credential_state(&self, _credential_state: CredentialState) {}

    async fn clear_identity(&self) {}
}

fn snapshot(sequence: u64, availability: NetworkAvailability) -> NetworkSnapshot {
    NetworkSnapshot {
        sequence,
        availability,
        transport: NetworkTransportFlags::default(),
        is_expensive: false,
        is_constrained: false,
    }
}

#[test]
fn lifecycle_barrier_is_scoped_to_events_that_change_connections() {
    let cases = [
        (
            NetworkEvent::NetworkPathChanged {
                snapshot: snapshot(1, NetworkAvailability::Unavailable),
            },
            true,
        ),
        (
            NetworkEvent::NetworkPathChanged {
                snapshot: snapshot(2, NetworkAvailability::Available),
            },
            true,
        ),
        (
            NetworkEvent::NetworkPathChanged {
                snapshot: snapshot(3, NetworkAvailability::Unknown),
            },
            false,
        ),
        (
            NetworkEvent::AppLifecycleChanged {
                state: AppLifecycleState::Background,
            },
            false,
        ),
        (
            NetworkEvent::AppLifecycleChanged {
                state: AppLifecycleState::Foreground {
                    background_duration_ms: LONG_BACKGROUND_RECONNECT_THRESHOLD_MS - 1,
                },
            },
            false,
        ),
        (
            NetworkEvent::AppLifecycleChanged {
                state: AppLifecycleState::Foreground {
                    background_duration_ms: LONG_BACKGROUND_RECONNECT_THRESHOLD_MS,
                },
            },
            true,
        ),
        (
            NetworkEvent::CleanupConnections {
                reason: CleanupReason::ManualReset,
            },
            true,
        ),
        (
            NetworkEvent::ForceReconnect {
                reason: ReconnectReason::ManualReconnect,
            },
            true,
        ),
    ];

    for (event, expected) in cases {
        assert_eq!(
            network_event_needs_lifecycle_barrier(&event),
            expected,
            "{event:?}"
        );
    }
}

#[test]
fn long_foreground_suppresses_auto_reconnect_before_settle() {
    let signaling = Arc::new(ForceReconnectFakeSignalingClient::new(false));
    let processor = DefaultNetworkEventProcessor::new(signaling.clone(), None);

    processor.prepare_network_event(&NetworkEvent::AppLifecycleChanged {
        state: AppLifecycleState::Foreground {
            background_duration_ms: LONG_BACKGROUND_RECONNECT_THRESHOLD_MS - 1,
        },
    });
    assert_eq!(
        signaling
            .suppress_auto_reconnect_calls
            .load(AtomicOrdering::SeqCst),
        0,
        "short foreground recovery must keep the Probe path unchanged"
    );

    processor.prepare_network_event(&NetworkEvent::AppLifecycleChanged {
        state: AppLifecycleState::Foreground {
            background_duration_ms: LONG_BACKGROUND_RECONNECT_THRESHOLD_MS,
        },
    });
    assert_eq!(
        signaling
            .suppress_auto_reconnect_calls
            .load(AtomicOrdering::SeqCst),
        1,
        "long foreground recovery must suppress stale auto-reconnect before settling"
    );
}

#[tokio::test]
async fn force_reconnect_reenables_auto_reconnect_after_early_suppression() {
    let signaling = Arc::new(ForceReconnectFakeSignalingClient::new(false));
    let processor = DefaultNetworkEventProcessor::new(signaling.clone(), None);
    let long_foreground = NetworkEvent::AppLifecycleChanged {
        state: AppLifecycleState::Foreground {
            background_duration_ms: LONG_BACKGROUND_RECONNECT_THRESHOLD_MS,
        },
    };

    processor.prepare_network_event(&long_foreground);
    assert!(
        signaling
            .auto_reconnect_suppressed
            .load(AtomicOrdering::SeqCst),
        "long foreground preparation should pause automatic reconnect"
    );

    processor
        .force_reconnect()
        .await
        .expect("ForceReconnect should restore signaling");

    assert!(
        !signaling
            .auto_reconnect_suppressed
            .load(AtomicOrdering::SeqCst),
        "successful ForceReconnect should re-enable future automatic reconnects"
    );
    assert_eq!(
        signaling
            .schedule_auto_reconnect_reset_backoff_calls
            .load(AtomicOrdering::SeqCst),
        1,
        "ForceReconnect should re-arm automatic reconnect before its explicit restore"
    );
}

#[tokio::test]
async fn force_reconnect_failure_schedules_auto_reconnect() {
    let signaling = Arc::new(ForceReconnectFakeSignalingClient::new(true));
    let processor = DefaultNetworkEventProcessor::new(signaling.clone(), None);

    let result = processor.force_reconnect().await;

    assert!(result.is_err());
    assert_eq!(
        signaling.disconnect_calls.load(AtomicOrdering::SeqCst),
        1,
        "ForceReconnect cleanup should disconnect signaling once"
    );
    assert_eq!(
        signaling.connect_once_calls.load(AtomicOrdering::SeqCst),
        1,
        "ForceReconnect restore should make one quick connect attempt"
    );
    assert_eq!(
        signaling
            .schedule_auto_reconnect_calls
            .load(AtomicOrdering::SeqCst),
        2,
        "ForceReconnect should wake auto-reconnect before restore and keep it scheduled after failure"
    );
    assert_eq!(
        signaling
            .schedule_auto_reconnect_reset_backoff_calls
            .load(AtomicOrdering::SeqCst),
        1,
        "ForceReconnect should reset reconnect backoff before the quick restore attempt"
    );
}

#[tokio::test]
async fn restore_failure_schedules_auto_reconnect_reset_backoff() {
    let signaling = Arc::new(ForceReconnectFakeSignalingClient::new(true));
    let processor = DefaultNetworkEventProcessor::new(signaling.clone(), None);

    let result = processor
        .process_network_recovery_action(NetworkRecoveryAction::Restore)
        .await;

    assert!(result.is_err());
    assert_eq!(
        signaling.connect_once_calls.load(AtomicOrdering::SeqCst),
        1,
        "Restore should make one quick connect attempt"
    );
    assert_eq!(
        signaling
            .schedule_auto_reconnect_reset_backoff_calls
            .load(AtomicOrdering::SeqCst),
        1,
        "failed Restore should reset reconnect backoff"
    );
}

#[tokio::test]
async fn restore_schedules_reset_backoff_before_quick_connect() {
    let signaling = Arc::new(ForceReconnectFakeSignalingClient::new(false));
    let processor = DefaultNetworkEventProcessor::new(signaling.clone(), None);

    processor
        .process_network_recovery_action(NetworkRecoveryAction::Restore)
        .await
        .expect("Restore should connect successfully");

    assert_eq!(
        signaling.connect_once_calls.load(AtomicOrdering::SeqCst),
        1,
        "Restore should make one quick connect attempt"
    );
    assert_eq!(
        signaling
            .schedule_auto_reconnect_reset_backoff_calls
            .load(AtomicOrdering::SeqCst),
        1,
        "Restore should reset reconnect backoff before the quick connect attempt"
    );
}

#[test]
fn snapshot_is_offline_and_should_restore() {
    let offline = snapshot(1, NetworkAvailability::Unavailable);
    assert!(offline.is_offline());
    assert!(!offline.should_restore());

    let online = snapshot(2, NetworkAvailability::Available);
    assert!(!online.is_offline());
    assert!(online.should_restore());

    // Unknown is neither offline (not Unavailable) nor restorable (not Available).
    let unknown = snapshot(3, NetworkAvailability::Unknown);
    assert!(!unknown.is_offline());
    assert!(!unknown.should_restore());
}
