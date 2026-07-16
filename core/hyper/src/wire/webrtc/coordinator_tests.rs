use super::*;
use actr_protocol::{
    AIdCredential, Pong, RegisterRequest, RegisterResponse, RouteCandidatesRequest,
    RouteCandidatesResponse, ServiceAvailabilityState, UnregisterResponse,
};
use std::sync::atomic::AtomicBool;
use tokio::sync::{Semaphore, broadcast, mpsc};

struct TaskDropFlag(Arc<AtomicBool>);

impl Drop for TaskDropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn test_actor_id(serial_number: u64) -> ActrId {
    ActrId {
        realm: actr_protocol::Realm { realm_id: 1 },
        serial_number,
        r#type: actr_protocol::ActrType {
            manufacturer: "acme".to_string(),
            name: "node".to_string(),
            version: "1.0.0".to_string(),
        },
    }
}

fn test_credential() -> AIdCredential {
    AIdCredential {
        key_id: 7,
        claims: Bytes::from_static(b"claims"),
        signature: Bytes::from(vec![0u8; 64]),
    }
}

async fn insert_pending_offer_peer(
    coordinator: &Arc<WebRtcCoordinator>,
    peer_id: ActrId,
    sdp_exchange_id: &str,
) -> u64 {
    let api = webrtc::api::APIBuilder::new().build();
    let peer_connection = Arc::new(
        api.new_peer_connection(Default::default())
            .await
            .expect("test peer connection should be created"),
    );
    let webrtc_conn = WebRtcConnection::new(
        peer_id.clone(),
        peer_connection.clone(),
        coordinator.event_broadcaster.sender(),
    );
    let session_id = webrtc_conn.session_id();
    let (ready_tx, _ready_rx) = oneshot::channel();

    coordinator.peers.write().await.insert(
        peer_id,
        PeerState {
            peer_connection,
            webrtc_conn,
            ready_tx: Some(ready_tx),
            is_offerer: true,
            ice_signaling: PeerIceSignalingState {
                pending_local_sdp_exchange_id: Some(sdp_exchange_id.to_string()),
                ..PeerIceSignalingState::default()
            },
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
            session_id,
            receive_handles: Vec::new(),
        },
    );

    session_id
}

async fn mark_peer_as_answerer(
    coordinator: &Arc<WebRtcCoordinator>,
    peer_id: &ActrId,
) -> (u64, WebRtcConnection) {
    let mut peers = coordinator.peers.write().await;
    let state = peers.get_mut(peer_id).expect("peer should exist");
    state.is_offerer = false;
    state.update_connection_state(RTCPeerConnectionState::Connected);
    (state.session_id, state.webrtc_conn.clone())
}

struct CapturingSignalingClient {
    sent: Mutex<Vec<SignalingEnvelope>>,
    event_tx: broadcast::Sender<super::super::SignalingEvent>,
    connected: AtomicBool,
    send_control: Option<Arc<SendControl>>,
}

struct SendControl {
    block: bool,
    fail: bool,
    started: Semaphore,
    release: Semaphore,
    dropped: Arc<AtomicBool>,
}

impl SendControl {
    fn blocking() -> Arc<Self> {
        Arc::new(Self {
            block: true,
            fail: false,
            started: Semaphore::new(0),
            release: Semaphore::new(0),
            dropped: Arc::new(AtomicBool::new(false)),
        })
    }

    fn failing() -> Arc<Self> {
        Arc::new(Self {
            block: false,
            fail: true,
            started: Semaphore::new(0),
            release: Semaphore::new(0),
            dropped: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn wait_until_started(&self) {
        self.started
            .acquire()
            .await
            .expect("send-start semaphore should remain open")
            .forget();
    }
}

impl CapturingSignalingClient {
    fn new() -> Self {
        Self::with_connected(true)
    }

    fn with_connected(connected: bool) -> Self {
        let (event_tx, _rx) = broadcast::channel(16);
        Self {
            sent: Mutex::new(Vec::new()),
            event_tx,
            connected: AtomicBool::new(connected),
            send_control: None,
        }
    }

    fn with_send_control(send_control: Arc<SendControl>) -> Self {
        let (event_tx, _rx) = broadcast::channel(16);
        Self {
            sent: Mutex::new(Vec::new()),
            event_tx,
            connected: AtomicBool::new(true),
            send_control: Some(send_control),
        }
    }

    fn reconnect(&self) {
        self.connected.store(true, Ordering::Release);
        let _ = self.event_tx.send(super::super::SignalingEvent::Connected);
    }

    async fn last_relay_source(&self) -> ActrId {
        let sent = self.sent.lock().await;
        let envelope = sent.last().expect("relay envelope should be sent");
        let Some(signaling_envelope::Flow::ActrRelay(relay)) = &envelope.flow else {
            panic!("expected ActrRelay envelope");
        };
        relay.source.clone()
    }

    async fn sent_envelopes(&self) -> Vec<SignalingEnvelope> {
        self.sent.lock().await.clone()
    }

    async fn wait_for_sent_envelopes(&self, expected: usize) -> Vec<SignalingEnvelope> {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let sent = self.sent_envelopes().await;
                if sent.len() >= expected {
                    return sent;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("signaling envelopes should be sent before timeout")
    }
}

#[async_trait::async_trait]
impl SignalingClient for CapturingSignalingClient {
    async fn connect(&self) -> crate::transport::NetworkResult<()> {
        Ok(())
    }

    async fn connect_once(&self) -> crate::transport::NetworkResult<()> {
        Ok(())
    }

    async fn disconnect(&self) -> crate::transport::NetworkResult<()> {
        Ok(())
    }

    async fn send_register_request(
        &self,
        _request: RegisterRequest,
    ) -> crate::transport::NetworkResult<RegisterResponse> {
        unimplemented!("not used by this test")
    }

    async fn send_unregister_request(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _reason: Option<String>,
    ) -> crate::transport::NetworkResult<UnregisterResponse> {
        unimplemented!("not used by this test")
    }

    async fn send_heartbeat(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _availability: ServiceAvailabilityState,
        _power_reserve: f32,
        _mailbox_backlog: f32,
    ) -> crate::transport::NetworkResult<Pong> {
        unimplemented!("not used by this test")
    }

    async fn send_route_candidates_request(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _request: RouteCandidatesRequest,
    ) -> crate::transport::NetworkResult<RouteCandidatesResponse> {
        unimplemented!("not used by this test")
    }

    async fn get_signing_key(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _key_id: u32,
    ) -> crate::transport::NetworkResult<(u32, Vec<u8>)> {
        unimplemented!("not used by this test")
    }

    async fn send_envelope(
        &self,
        envelope: SignalingEnvelope,
    ) -> crate::transport::NetworkResult<()> {
        if !self.is_connected() {
            return Err(crate::transport::NetworkError::ConnectionError(
                "injected disconnected signaling client".to_string(),
            ));
        }
        if let Some(control) = &self.send_control {
            let _drop_flag = TaskDropFlag(Arc::clone(&control.dropped));
            control.started.add_permits(1);
            if control.block {
                control
                    .release
                    .acquire()
                    .await
                    .expect("send-release semaphore should remain open")
                    .forget();
            }
            if control.fail {
                return Err(crate::transport::NetworkError::ConnectionError(
                    "injected signaling send failure".to_string(),
                ));
            }
        }
        self.sent.lock().await.push(envelope);
        Ok(())
    }

    async fn receive_envelope(&self) -> crate::transport::NetworkResult<Option<SignalingEnvelope>> {
        std::future::pending().await
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn get_stats(&self) -> super::super::SignalingStats {
        super::super::SignalingStats::default()
    }

    fn subscribe_events(&self) -> broadcast::Receiver<super::super::SignalingEvent> {
        self.event_tx.subscribe()
    }

    async fn set_actor_id(&self, _actor_id: ActrId) {}

    async fn set_credential_state(&self, _credential_state: CredentialState) {}

    async fn clear_identity(&self) {}
}

fn new_test_coordinator(local_id: ActrId) -> Arc<WebRtcCoordinator> {
    Arc::new(WebRtcCoordinator::new(
        local_id,
        CredentialState::new(test_credential(), None, None),
        Arc::new(CapturingSignalingClient::new()),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ))
}

#[tokio::test]
async fn coordinator_background_tasks_are_joined_by_shutdown() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    Arc::clone(&coordinator)
        .start()
        .await
        .expect("coordinator should start");
    Arc::clone(&coordinator)
        .start()
        .await
        .expect("repeated start should be idempotent");

    let abort_handles = {
        let handles = coordinator.background_tasks.handles.lock().await;
        assert_eq!(handles.len(), 3, "start should own exactly three tasks");
        handles
            .iter()
            .map(JoinHandle::abort_handle)
            .collect::<Vec<_>>()
    };

    let ((), ()) = tokio::join!(
        coordinator.shutdown_background_tasks(),
        coordinator.shutdown_background_tasks()
    );

    assert!(coordinator.background_tasks.handles.lock().await.is_empty());
    assert!(
        abort_handles
            .iter()
            .all(tokio::task::AbortHandle::is_finished),
        "shutdown must not return before all three tasks have finished"
    );

    Arc::clone(&coordinator)
        .start()
        .await
        .expect("start after shutdown should restart the task set");
    assert_eq!(coordinator.background_tasks.handles.lock().await.len(), 3);
    coordinator.shutdown_background_tasks().await;
}

#[tokio::test]
async fn coordinator_start_is_linearized_after_inflight_shutdown() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    Arc::clone(&coordinator)
        .start()
        .await
        .expect("coordinator should start");
    let original_abort_handles = {
        let handles = coordinator.background_tasks.handles.lock().await;
        handles
            .iter()
            .map(JoinHandle::abort_handle)
            .collect::<Vec<_>>()
    };

    let lifecycle_guard = coordinator.background_tasks.lifecycle_gate.lock().await;
    let shutdown_task = tokio::spawn({
        let coordinator = Arc::clone(&coordinator);
        async move { coordinator.shutdown_background_tasks().await }
    });
    tokio::task::yield_now().await;
    let restart_task = tokio::spawn({
        let coordinator = Arc::clone(&coordinator);
        async move { coordinator.start().await }
    });
    tokio::task::yield_now().await;
    drop(lifecycle_guard);

    shutdown_task
        .await
        .expect("overlapping shutdown should finish");
    assert!(
        original_abort_handles
            .iter()
            .all(tokio::task::AbortHandle::is_finished),
        "the original task set must finish before the queued restart"
    );
    restart_task
        .await
        .expect("queued restart task should finish")
        .expect("queued restart should succeed");
    assert_eq!(coordinator.background_tasks.handles.lock().await.len(), 3);

    coordinator.shutdown_background_tasks().await;
}

#[tokio::test]
async fn answerer_unavailable_states_request_offerer_ice_restart() {
    for unavailable_state in [
        RTCPeerConnectionState::Disconnected,
        RTCPeerConnectionState::Failed,
    ] {
        let local_id = test_actor_id(1);
        let peer_id = test_actor_id(99);
        let credential_state = CredentialState::new(test_credential(), None, None);
        let signaling_client = Arc::new(CapturingSignalingClient::new());
        let coordinator = Arc::new(WebRtcCoordinator::new(
            local_id,
            credential_state,
            signaling_client.clone(),
            WebRtcConfig::default(),
            Arc::new(MediaFrameRegistry::new()),
        ));

        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;
        let (session_id, webrtc_conn) = mark_peer_as_answerer(&coordinator, &peer_id).await;

        coordinator
            .handle_peer_state_change(&webrtc_conn, &peer_id, session_id, unavailable_state)
            .await;

        let sent = signaling_client.wait_for_sent_envelopes(1).await;
        assert_eq!(
            sent.len(),
            1,
            "Answerer should send one recovery request after {unavailable_state:?}"
        );
        let Some(signaling_envelope::Flow::ActrRelay(relay)) = &sent[0].flow else {
            panic!("expected ActrRelay envelope");
        };
        assert!(
            matches!(
                relay.payload.as_ref(),
                Some(actr_relay::Payload::IceRestartRequest(_))
            ),
            "Answerer must request the Offerer to restart ICE, not create an offer"
        );
    }
}

#[tokio::test]
async fn stale_answerer_state_does_not_request_ice_restart() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::new());
    let coordinator = Arc::new(WebRtcCoordinator::new(
        local_id,
        credential_state,
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));

    insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;
    let (session_id, webrtc_conn) = mark_peer_as_answerer(&coordinator, &peer_id).await;

    coordinator
        .handle_peer_state_change(
            &webrtc_conn,
            &peer_id,
            session_id + 1,
            RTCPeerConnectionState::Disconnected,
        )
        .await;

    assert!(
        signaling_client.sent_envelopes().await.is_empty(),
        "stale Answerer callbacks must not send IceRestartRequest"
    );
    let peers = coordinator.peers.read().await;
    assert_eq!(
        peers
            .get(&peer_id)
            .expect("active peer should remain")
            .current_state,
        RTCPeerConnectionState::Connected,
        "stale callbacks must not update the active session"
    );
}

#[tokio::test]
async fn answerer_waits_for_signaling_reconnect_before_request() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::with_connected(false));
    let coordinator = Arc::new(WebRtcCoordinator::new(
        local_id,
        credential_state,
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));

    insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;
    let (session_id, webrtc_conn) = mark_peer_as_answerer(&coordinator, &peer_id).await;

    coordinator
        .handle_peer_state_change(
            &webrtc_conn,
            &peer_id,
            session_id,
            RTCPeerConnectionState::Disconnected,
        )
        .await;

    assert!(signaling_client.sent_envelopes().await.is_empty());
    signaling_client.reconnect();

    let sent = signaling_client.wait_for_sent_envelopes(1).await;
    assert_eq!(
        sent.len(),
        1,
        "Answerer should send exactly once after reconnect"
    );
}

#[tokio::test]
async fn recovered_answerer_skips_deferred_restart_request() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::with_connected(false));
    let coordinator = Arc::new(WebRtcCoordinator::new(
        local_id,
        credential_state,
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));

    insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;
    let (session_id, webrtc_conn) = mark_peer_as_answerer(&coordinator, &peer_id).await;

    coordinator
        .handle_peer_state_change(
            &webrtc_conn,
            &peer_id,
            session_id,
            RTCPeerConnectionState::Disconnected,
        )
        .await;
    coordinator
        .peers
        .write()
        .await
        .get_mut(&peer_id)
        .expect("peer should remain current")
        .update_connection_state(RTCPeerConnectionState::Connected);

    signaling_client.reconnect();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let task_cleared = coordinator
                .peers
                .read()
                .await
                .get(&peer_id)
                .is_some_and(|state| state.restart_task_handle.is_none());
            if task_cleared {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deferred Answerer request task should finish after reconnect");

    assert!(
        signaling_client.sent_envelopes().await.is_empty(),
        "recovered Answerer must not send a late IceRestartRequest"
    );
}

#[tokio::test]
async fn reliable_queue_backpressure_does_not_block_rpc_queue() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let peer_bytes = test_actor_id(2).encode_to_vec();

    for _ in 0..WEBRTC_RELIABLE_INBOUND_QUEUE_DEPTH {
        coordinator
            .reliable_message_tx
            .try_send((
                peer_bytes.clone(),
                Bytes::from_static(b"reliable"),
                PayloadType::StreamReliable,
            ))
            .expect("reliable queue should accept up to its configured depth");
    }
    assert!(
        coordinator
            .reliable_message_tx
            .try_send((
                peer_bytes.clone(),
                Bytes::from_static(b"overflow"),
                PayloadType::StreamReliable,
            ))
            .is_err(),
        "reliable queue should be full"
    );

    coordinator
        .rpc_message_tx
        .send((
            peer_bytes,
            Bytes::from_static(b"rpc"),
            PayloadType::RpcReliable,
        ))
        .await
        .expect("full reliable queue must not block RPC queue");

    let (_from, data, payload_type) =
        tokio::time::timeout(Duration::from_secs(1), coordinator.receive_rpc_message())
            .await
            .expect("RPC receive should not wait behind reliable backpressure")
            .expect("RPC receive should not fail")
            .expect("RPC message should be present");

    assert_eq!(payload_type, PayloadType::RpcReliable);
    assert_eq!(&data[..], b"rpc");
}

#[tokio::test]
async fn reliable_queue_backpressure_does_not_block_latency_first() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let peer_bytes = test_actor_id(2).encode_to_vec();

    // Saturate the reliable queue so reliable traffic is backpressured.
    for _ in 0..WEBRTC_RELIABLE_INBOUND_QUEUE_DEPTH {
        coordinator
            .reliable_message_tx
            .try_send((
                peer_bytes.clone(),
                Bytes::from_static(b"reliable"),
                PayloadType::StreamReliable,
            ))
            .expect("reliable queue should accept up to its configured depth");
    }
    assert!(
        coordinator
            .reliable_message_tx
            .try_send((
                peer_bytes.clone(),
                Bytes::from_static(b"overflow"),
                PayloadType::StreamReliable,
            ))
            .is_err(),
        "reliable queue should be full"
    );

    // A full reliable queue must not block latency-first delivery: the two
    // classes have separate queues and receive loops, so a backpressured
    // reliable stream cannot stall latency-first chunks upstream of the
    // registry's drop-newest policy.
    coordinator
        .latency_first_message_tx
        .send((
            peer_bytes,
            Bytes::from_static(b"lf"),
            PayloadType::StreamLatencyFirst,
        ))
        .await
        .expect("full reliable queue must not block latency-first queue");

    let (_from, data, payload_type) = tokio::time::timeout(
        Duration::from_secs(1),
        coordinator.receive_latency_first_message(),
    )
    .await
    .expect("latency-first receive should not wait behind reliable backpressure")
    .expect("latency-first receive should not fail")
    .expect("latency-first message should be present");

    assert_eq!(payload_type, PayloadType::StreamLatencyFirst);
    assert_eq!(&data[..], b"lf");
}

fn install_hook_recorder(
    coordinator: &Arc<WebRtcCoordinator>,
) -> mpsc::UnboundedReceiver<crate::wire::webrtc::HookEvent> {
    let (hook_tx, hook_rx) = mpsc::unbounded_channel();
    let hook: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    coordinator.set_hook_callback(hook);
    hook_rx
}

async fn expect_disconnected_hook(
    hook_rx: &mut mpsc::UnboundedReceiver<crate::wire::webrtc::HookEvent>,
    peer_id: &ActrId,
    expected_status: WebRtcPeerStatus,
    message: &str,
) {
    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect(message)
        .expect("hook channel should remain open");
    match event {
        crate::wire::webrtc::HookEvent::WebRtcDisconnected {
            peer_id: got,
            status,
        } => {
            assert_eq!(got, peer_id.clone());
            assert_eq!(status, expected_status);
        }
        other => panic!("unexpected hook event: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_recovering_and_idle_hooks_follow_state_commit_order() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.update_connection_state(RTCPeerConnectionState::Connected);
        state.mark_data_channel_opened();
        state.mark_sendable_hook_reported();
    }

    let recovering_entered = Arc::new(tokio::sync::Notify::new());
    let release_recovering = Arc::new(tokio::sync::Notify::new());
    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    let hook: crate::wire::webrtc::HookCallback = Arc::new({
        let recovering_entered = Arc::clone(&recovering_entered);
        let release_recovering = Arc::clone(&release_recovering);
        move |event| {
            let hook_tx = hook_tx.clone();
            let recovering_entered = Arc::clone(&recovering_entered);
            let release_recovering = Arc::clone(&release_recovering);
            Box::pin(async move {
                if matches!(
                    &event,
                    crate::wire::webrtc::HookEvent::WebRtcDisconnected {
                        status: WebRtcPeerStatus::Recovering,
                        ..
                    }
                ) {
                    recovering_entered.notify_one();
                    release_recovering.notified().await;
                }
                let _ = hook_tx.send(event);
            })
        }
    });
    coordinator.set_hook_callback(hook);

    let recovering_task = {
        let coordinator = Arc::clone(&coordinator);
        let peer_id = peer_id.clone();
        tokio::spawn(async move {
            coordinator
                .notify_webrtc_recovering_once(&peer_id, session_id, "disconnect detected")
                .await;
        })
    };

    tokio::time::timeout(Duration::from_secs(1), recovering_entered.notified())
        .await
        .expect("Recovering hook should enter its callback");

    let idle_started = Arc::new(tokio::sync::Notify::new());
    let idle_task = {
        let coordinator = Arc::clone(&coordinator);
        let peer_id = peer_id.clone();
        let idle_started = Arc::clone(&idle_started);
        tokio::spawn(async move {
            idle_started.notify_one();
            coordinator
                .notify_webrtc_idle_if_changed(&peer_id, session_id, "cleanup completed")
                .await;
        })
    };

    tokio::time::timeout(Duration::from_secs(1), idle_started.notified())
        .await
        .expect("Idle task should start while Recovering callback is blocked");

    assert!(
        tokio::time::timeout(Duration::from_millis(100), hook_rx.recv())
            .await
            .is_err(),
        "Idle must not overtake the earlier Recovering state commit"
    );

    release_recovering.notify_one();
    expect_disconnected_hook(
        &mut hook_rx,
        &peer_id,
        WebRtcPeerStatus::Recovering,
        "Recovering should be delivered first",
    )
    .await;
    expect_disconnected_hook(
        &mut hook_rx,
        &peer_id,
        WebRtcPeerStatus::Idle,
        "Idle should follow Recovering",
    )
    .await;

    recovering_task
        .await
        .expect("Recovering task should finish");
    idle_task.await.expect("Idle task should finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removed_peer_idle_is_enqueued_before_replacement_gate_reopens() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let peer_id = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;
    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.update_connection_state(RTCPeerConnectionState::Connected);
        state.mark_data_channel_opened();
        state.mark_sendable_hook_reported();
    }

    let mut hook_rx = install_hook_recorder(&coordinator);
    let replacement_gate = coordinator.restart_signaling_gate_for(&peer_id).await;
    let hook_emission_guard = coordinator.hook_emission_lock.lock().await;
    let recovery_guard = coordinator.network_recovering_peers.write().await;

    let cleanup_task = {
        let coordinator = Arc::clone(&coordinator);
        let peer_id = peer_id.clone();
        tokio::spawn(async move {
            coordinator
                .cleanup_connection_if_session(&peer_id, session_id, true, "replace old session")
                .await
        })
    };

    tokio::time::timeout(Duration::from_secs(1), async {
        while coordinator.peers.read().await.contains_key(&peer_id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old peer should be removed before teardown completes");

    let replacement_gate_entered = Arc::new(tokio::sync::Notify::new());
    let replacement_task = {
        let coordinator = Arc::clone(&coordinator);
        let peer_id = peer_id.clone();
        let replacement_gate_entered = Arc::clone(&replacement_gate_entered);
        tokio::spawn(async move {
            {
                let _replacement_guard = replacement_gate.lock().await;
                replacement_gate_entered.notify_one();
            }
            coordinator
                .invoke_hook(crate::wire::webrtc::HookEvent::WebRtcConnected {
                    peer_id,
                    relayed: false,
                })
                .await;
        })
    };

    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            replacement_gate_entered.notified(),
        )
        .await
        .is_err(),
        "replacement gate must remain closed until the removed peer's Idle hook is enqueued"
    );

    drop(hook_emission_guard);
    expect_disconnected_hook(
        &mut hook_rx,
        &peer_id,
        WebRtcPeerStatus::Idle,
        "removed peer Idle should be delivered before replacement hooks",
    )
    .await;

    let replacement_event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("replacement hook should follow removed peer Idle")
        .expect("hook channel should remain open");
    match replacement_event {
        crate::wire::webrtc::HookEvent::WebRtcConnected {
            peer_id: got,
            relayed,
        } => {
            assert_eq!(got, peer_id);
            assert!(!relayed);
        }
        other => panic!("unexpected replacement hook event: {other:?}"),
    }

    replacement_task
        .await
        .expect("replacement hook task should finish");
    drop(recovery_guard);
    assert!(
        cleanup_task
            .await
            .expect("old peer cleanup task should finish"),
        "old peer cleanup should remove the expected session"
    );
}

#[tokio::test]
async fn webrtc_connected_hook_waits_for_open_data_channel() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.update_connection_state(RTCPeerConnectionState::Connected);
    }

    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    let hook: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    coordinator.set_hook_callback(hook);

    coordinator
        .clear_peer_recovering_if_sendable(&peer_id, session_id, "peer connection connected")
        .await;

    let observed = tokio::time::timeout(Duration::from_millis(100), hook_rx.recv()).await;
    assert!(
        observed.is_err(),
        "connected hook must wait for an open DataChannel, got {observed:?}"
    );
}

#[tokio::test]
async fn connecting_state_reopens_connected_hook_window() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.update_connection_state(RTCPeerConnectionState::Connected);
        state.mark_sendable_hook_reported();
    }

    let listener = coordinator.spawn_internal_event_listener();
    coordinator
        .event_broadcaster
        .send(ConnectionEvent::StateChanged {
            peer_id: peer_id.clone(),
            session_id,
            state: ConnectionState::Connecting,
        });

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            {
                let peers = coordinator.peers.read().await;
                let state = peers.get(&peer_id).expect("peer should exist");
                if state.current_state == RTCPeerConnectionState::Connecting
                    && !state.sendable_hook_reported
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connecting event should reopen the connected hook window");
    listener.abort();
}

#[tokio::test]
async fn initial_connecting_state_emits_connecting_hook() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    let hook: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    coordinator.set_hook_callback(hook);

    let listener = coordinator.spawn_internal_event_listener();
    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::StateChanged {
            peer_id: peer_id.clone(),
            session_id,
            state: ConnectionState::Connecting,
        });

    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("connecting hook should be emitted")
        .expect("hook channel should remain open");
    match event {
        crate::wire::webrtc::HookEvent::WebRtcConnectStart { peer_id: got } => {
            assert_eq!(got, peer_id);
        }
        other => panic!("unexpected hook event: {other:?}"),
    }
    listener.abort();
}

#[tokio::test]
async fn initial_failure_emits_idle_not_recovering() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    // Fresh peer that has never reached ICE connected / DataChannel opened,
    // so a failure must terminate at Idle rather than Recovering.
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    let hook: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    coordinator.set_hook_callback(hook);

    let listener = coordinator.spawn_internal_event_listener();

    // Initial connecting attempt.
    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::StateChanged {
            peer_id: peer_id.clone(),
            session_id,
            state: ConnectionState::Connecting,
        });
    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("connecting hook should be emitted")
        .expect("hook channel should remain open");
    assert!(
        matches!(
            event,
            crate::wire::webrtc::HookEvent::WebRtcConnectStart { .. }
        ),
        "unexpected hook event: {event:?}"
    );

    // The attempt fails before the peer ever became usable.
    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::StateChanged {
            peer_id: peer_id.clone(),
            session_id,
            state: ConnectionState::Disconnected,
        });
    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("failed initial attempt should emit a disconnected hook")
        .expect("hook channel should remain open");
    match event {
        crate::wire::webrtc::HookEvent::WebRtcDisconnected {
            peer_id: got,
            status,
        } => {
            assert_eq!(got, peer_id);
            assert_eq!(
                status,
                WebRtcPeerStatus::Idle,
                "initial failure must terminate at Idle, not Recovering"
            );
        }
        other => panic!("unexpected hook event: {other:?}"),
    }

    // No Recovering should follow a terminal Idle for a never-connected peer.
    let trailing = tokio::time::timeout(Duration::from_millis(100), hook_rx.recv()).await;
    assert!(
        trailing.is_err(),
        "no further hook expected after terminal Idle, got {trailing:?}"
    );
    listener.abort();
}

#[tokio::test]
async fn recovery_connecting_state_does_not_emit_connecting_hook() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.update_connection_state(RTCPeerConnectionState::Connected);
        state.mark_sendable_hook_reported();
    }

    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    let hook: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    coordinator.set_hook_callback(hook);

    let listener = coordinator.spawn_internal_event_listener();
    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::StateChanged {
            peer_id: peer_id.clone(),
            session_id,
            state: ConnectionState::Disconnected,
        });

    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("disconnected hook should be emitted")
        .expect("hook channel should remain open");
    match event {
        crate::wire::webrtc::HookEvent::WebRtcDisconnected {
            peer_id: got,
            status,
        } => {
            assert_eq!(got, peer_id);
            assert_eq!(status, WebRtcPeerStatus::Recovering);
        }
        other => panic!("unexpected hook event: {other:?}"),
    }

    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::StateChanged {
            peer_id: peer_id.clone(),
            session_id,
            state: ConnectionState::Connecting,
        });
    let connecting = tokio::time::timeout(Duration::from_millis(100), hook_rx.recv()).await;
    assert!(
        connecting.is_err(),
        "recovery Connecting must not emit a public connecting hook, got {connecting:?}"
    );
    listener.abort();
}

#[tokio::test]
async fn data_channel_close_cleanup_emits_terminal_idle_hook() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.update_connection_state(RTCPeerConnectionState::Connected);
        state.mark_data_channel_opened();
        state.mark_sendable_hook_reported();
    }

    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    let hook: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    coordinator.set_hook_callback(hook);

    let listener = coordinator.spawn_internal_event_listener();
    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::DataChannelClosed {
            peer_id: peer_id.clone(),
            session_id,
            payload_type: PayloadType::RpcReliable,
        });

    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("data channel close should emit recovering hook")
        .expect("hook channel should remain open");
    match event {
        crate::wire::webrtc::HookEvent::WebRtcDisconnected {
            peer_id: got,
            status,
        } => {
            assert_eq!(got, peer_id);
            assert_eq!(status, WebRtcPeerStatus::Recovering);
        }
        other => panic!("unexpected hook event: {other:?}"),
    }

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !coordinator.peers.read().await.contains_key(&peer_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("data channel close should clean up the peer state");

    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("cleanup should emit terminal idle hook after recovering")
        .expect("hook channel should remain open");
    match event {
        crate::wire::webrtc::HookEvent::WebRtcDisconnected {
            peer_id: got,
            status,
        } => {
            assert_eq!(got, peer_id);
            assert_eq!(status, WebRtcPeerStatus::Idle);
        }
        other => panic!("unexpected hook event: {other:?}"),
    }

    listener.abort();
}

#[tokio::test]
async fn cancelled_cleanup_emits_terminal_idle_for_connected_peer() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.update_connection_state(RTCPeerConnectionState::Connected);
        state.mark_data_channel_opened();
        state.mark_sendable_hook_reported();
    }

    let mut hook_rx = install_hook_recorder(&coordinator);
    coordinator
        .cleanup_cancelled_connection(&peer_id, "test connected peer replacement")
        .await;

    expect_disconnected_hook(
        &mut hook_rx,
        &peer_id,
        WebRtcPeerStatus::Idle,
        "cancelled cleanup should emit terminal idle for connected peer",
    )
    .await;
    assert!(
        !coordinator.peers.read().await.contains_key(&peer_id),
        "cancelled cleanup should remove peer state"
    );
}

#[tokio::test]
async fn cancelled_cleanup_clears_recovery_guard_for_removed_session() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    coordinator
        .mark_peer_recovering(&peer_id, session_id, "test recovery guard")
        .await;
    assert!(
        coordinator
            .network_recovering_peers
            .read()
            .await
            .contains_key(&peer_id),
        "test setup should mark peer as recovering"
    );

    coordinator
        .cleanup_cancelled_connection(&peer_id, "test recovery guard cleanup")
        .await;

    assert!(
        !coordinator
            .network_recovering_peers
            .read()
            .await
            .contains_key(&peer_id),
        "cancelled cleanup should clear the removed session's recovery guard"
    );
}

#[tokio::test]
async fn cancelled_cleanup_emits_terminal_idle_after_recovering_peer() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.update_connection_state(RTCPeerConnectionState::Connected);
        state.mark_data_channel_opened();
        state.mark_sendable_hook_reported();
    }

    let mut hook_rx = install_hook_recorder(&coordinator);
    let listener = coordinator.spawn_internal_event_listener();
    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::StateChanged {
            peer_id: peer_id.clone(),
            session_id,
            state: ConnectionState::Failed,
        });

    expect_disconnected_hook(
        &mut hook_rx,
        &peer_id,
        WebRtcPeerStatus::Recovering,
        "failed state should emit recovering hook before stale cleanup",
    )
    .await;

    coordinator
        .cleanup_cancelled_connection(&peer_id, "test stale failed peer cleanup")
        .await;
    expect_disconnected_hook(
        &mut hook_rx,
        &peer_id,
        WebRtcPeerStatus::Idle,
        "cancelled cleanup should emit terminal idle after recovering",
    )
    .await;
    assert!(
        !coordinator.peers.read().await.contains_key(&peer_id),
        "cancelled cleanup should remove peer state"
    );
    listener.abort();
}

#[tokio::test]
async fn failed_ice_restart_after_recovering_emits_terminal_idle() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.update_connection_state(RTCPeerConnectionState::Connected);
        state.mark_data_channel_opened();
        state.mark_sendable_hook_reported();
    }

    let mut hook_rx = install_hook_recorder(&coordinator);
    let listener = coordinator.spawn_internal_event_listener();
    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::StateChanged {
            peer_id: peer_id.clone(),
            session_id,
            state: ConnectionState::Disconnected,
        });

    expect_disconnected_hook(
        &mut hook_rx,
        &peer_id,
        WebRtcPeerStatus::Recovering,
        "disconnected state should emit recovering hook",
    )
    .await;

    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::IceRestartCompleted {
            peer_id: peer_id.clone(),
            session_id,
            success: false,
        });
    expect_disconnected_hook(
        &mut hook_rx,
        &peer_id,
        WebRtcPeerStatus::Idle,
        "failed ICE restart should emit terminal idle after recovering",
    )
    .await;
    listener.abort();
}

#[tokio::test]
async fn failed_ice_restart_after_public_connecting_emits_disconnected() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    let hook: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    coordinator.set_hook_callback(hook);

    let listener = coordinator.spawn_internal_event_listener();
    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::StateChanged {
            peer_id: peer_id.clone(),
            session_id,
            state: ConnectionState::Connecting,
        });
    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("connecting hook should be emitted")
        .expect("hook channel should remain open");
    assert!(
        matches!(
            event,
            crate::wire::webrtc::HookEvent::WebRtcConnectStart { .. }
        ),
        "unexpected hook event: {event:?}"
    );

    let _ = coordinator
        .event_broadcaster
        .send(ConnectionEvent::IceRestartCompleted {
            peer_id: peer_id.clone(),
            session_id,
            success: false,
        });
    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("failed restart should emit disconnected hook")
        .expect("hook channel should remain open");
    match event {
        crate::wire::webrtc::HookEvent::WebRtcDisconnected {
            peer_id: got,
            status,
        } => {
            assert_eq!(got, peer_id);
            assert_eq!(status, WebRtcPeerStatus::Idle);
        }
        other => panic!("unexpected hook event: {other:?}"),
    }
    listener.abort();
}

#[tokio::test]
async fn webrtc_disconnected_hook_is_session_guarded_and_deduped() {
    let local_id = test_actor_id(1);
    let peer_id = test_actor_id(99);
    let coordinator = new_test_coordinator(local_id);
    let session_id =
        insert_pending_offer_peer(&coordinator, peer_id.clone(), "current-exchange").await;

    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    let hook: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    coordinator.set_hook_callback(hook);

    coordinator
        .notify_webrtc_recovering_once(&peer_id, session_id + 1, "stale session")
        .await;
    let stale = tokio::time::timeout(Duration::from_millis(100), hook_rx.recv()).await;
    assert!(
        stale.is_err(),
        "stale session must not emit disconnected hook, got {stale:?}"
    );

    coordinator
        .notify_webrtc_recovering_once(&peer_id, session_id, "peer state Disconnected")
        .await;
    let event = tokio::time::timeout(Duration::from_secs(1), hook_rx.recv())
        .await
        .expect("disconnected hook should be emitted")
        .expect("hook channel should remain open");
    match event {
        crate::wire::webrtc::HookEvent::WebRtcDisconnected {
            peer_id: got,
            status,
        } => {
            assert_eq!(got, peer_id);
            assert_eq!(status, WebRtcPeerStatus::Recovering);
        }
        other => panic!("unexpected hook event: {other:?}"),
    }

    coordinator
        .notify_webrtc_recovering_once(&peer_id, session_id, "duplicate unavailable event")
        .await;
    let duplicate = tokio::time::timeout(Duration::from_millis(100), hook_rx.recv()).await;
    assert!(
        duplicate.is_err(),
        "duplicate unavailable event must not emit another hook, got {duplicate:?}"
    );
}

#[tokio::test]
async fn relay_source_uses_updated_local_id_after_re_registration() {
    let initial_id = test_actor_id(1);
    let renewed_id = test_actor_id(2);
    let target_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::new());
    let coordinator = Arc::new(WebRtcCoordinator::new(
        initial_id,
        credential_state,
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "current-exchange").await;

    coordinator.set_local_id(renewed_id.clone()).await;
    assert!(
        coordinator
            .commit_peer_signaling(
                &target_id,
                session_id,
                None,
                actr_relay::Payload::IceCandidate(actr_protocol::IceCandidate {
                    candidate: "candidate:0 1 UDP 1 127.0.0.1 9 typ host".to_string(),
                    sdp_mid: None,
                    sdp_mline_index: None,
                    username_fragment: None,
                }),
            )
            .await
            .expect("relay should be sent")
    );

    assert_eq!(signaling_client.last_relay_source().await, renewed_id);
}

#[tokio::test]
async fn actr_relay_answer_can_carry_sdp_exchange_id() {
    let local_id = test_actor_id(1);
    let target_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::new());
    let coordinator = Arc::new(WebRtcCoordinator::new(
        local_id,
        credential_state,
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "current-exchange").await;

    let payload = actr_relay::Payload::SessionDescription(actr_protocol::SessionDescription {
        r#type: SdpType::Answer as i32,
        sdp: "answer-sdp".to_string(),
        sdp_exchange_id: Some("exchange-1".to_string()),
    });
    assert!(
        coordinator
            .commit_peer_signaling(&target_id, session_id, None, payload)
            .await
            .expect("relay answer should be sent")
    );

    let sent = signaling_client.sent_envelopes().await;
    assert_eq!(sent.len(), 1);
    let envelope = &sent[0];
    assert!(envelope.reply_for.is_none());
    let Some(signaling_envelope::Flow::ActrRelay(relay)) = &envelope.flow else {
        panic!("expected ActrRelay envelope");
    };
    let Some(actr_relay::Payload::SessionDescription(sd)) = relay.payload.as_ref() else {
        panic!("expected SessionDescription payload");
    };
    assert_eq!(sd.r#type(), SdpType::Answer);
    assert_eq!(sd.sdp, "answer-sdp");
    assert_eq!(sd.sdp_exchange_id.as_deref(), Some("exchange-1"));
}

#[tokio::test]
async fn stale_answer_sdp_exchange_id_does_not_consume_pending_offer() {
    let local_id = test_actor_id(1);
    let target_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::new());
    let coordinator = Arc::new(WebRtcCoordinator::new(
        local_id,
        credential_state,
        signaling_client,
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));

    insert_pending_offer_peer(&coordinator, target_id.clone(), "current-exchange").await;

    coordinator
        .handle_answer(
            &target_id,
            "stale-answer-sdp".to_string(),
            Some("old-exchange".to_string()),
        )
        .await
        .expect("stale answer should be ignored without error");

    let peers = coordinator.peers.read().await;
    let state = peers.get(&target_id).expect("peer should remain");
    assert!(
        state.ready_tx.is_some(),
        "stale Answer must not consume the initial connection ready signal"
    );
    let pending = state
        .ice_signaling
        .pending_local_sdp_exchange_id
        .as_deref()
        .expect("stale Answer must not clear the active pending offer");
    assert_eq!(pending, "current-exchange");
}

#[tokio::test]
async fn clear_pending_restarts_clears_pending_sdp_exchange() {
    let local_id = test_actor_id(1);
    let target_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::new());
    let coordinator = Arc::new(WebRtcCoordinator::new(
        local_id,
        credential_state,
        signaling_client,
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));

    insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;
    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&target_id).expect("peer should exist");
        state.ice_restart_inflight = true;
        state.ice_restart_attempts = 1;
    }

    coordinator.clear_pending_restarts().await;

    let peers = coordinator.peers.read().await;
    let state = peers.get(&target_id).expect("peer should remain");
    assert!(
        state.ice_signaling.pending_local_sdp_exchange_id.is_none(),
        "aborted ICE restart must not leave a stale pending SDP exchange"
    );
    assert!(!state.ice_restart_inflight);
    assert_eq!(state.ice_restart_attempts, 0);
}

#[test]
fn ice_candidate_serialization_omits_empty_sdp_mid() {
    let candidate = WebRtcCoordinator::ice_candidate_from_json(
        RTCIceCandidateInit {
            candidate: "candidate:1 1 UDP 1 127.0.0.1 5000 typ host".to_string(),
            sdp_mid: Some(String::new()),
            sdp_mline_index: Some(0),
            username_fragment: None,
        },
        "generation".to_string(),
    );

    assert_eq!(candidate.sdp_mid, None);
    assert_eq!(candidate.sdp_mline_index, Some(0));
}

#[test]
fn ice_candidates_require_the_current_remote_ufrag() {
    let description = RTCSessionDescription::offer(
        "v=0\r\n\
         o=- 0 0 IN IP4 127.0.0.1\r\n\
         s=-\r\n\
         t=0 0\r\n\
         m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
         a=mid:0\r\n\
         a=ice-ufrag:current-generation\r\n\
         a=ice-pwd:test-password\r\n"
            .to_string(),
    )
    .expect("test SDP should parse");
    let candidate = |username_fragment: Option<&str>| IceCandidate {
        candidate: "candidate:1 1 UDP 1 127.0.0.1 5000 typ host".to_string(),
        sdp_mid: Some("0".to_string()),
        sdp_mline_index: Some(0),
        username_fragment: username_fragment.map(str::to_owned),
    };

    assert!(candidate_matches_description(
        &candidate(Some("current-generation")),
        &description
    ));
    assert!(!candidate_matches_description(
        &candidate(Some("stale-generation")),
        &description
    ));
    assert!(!candidate_matches_description(
        &candidate(None),
        &description
    ));
    assert!(!candidate_matches_description(
        &candidate(Some("")),
        &description
    ));
}

#[test]
fn known_old_remote_generation_is_dropped_instead_of_rebuffered() {
    let mut state = PeerIceSignalingState::default();
    state.remember_remote_ufrag("old".to_string());
    state.remember_remote_ufrag("current".to_string());

    assert_eq!(
        classify_remote_candidate_ufrag("old", "current", &state.known_remote_ufrags),
        RemoteCandidateDisposition::DropStale,
    );
}

#[test]
fn unknown_future_remote_generation_stays_buffered_until_its_sdp_arrives() {
    let mut state = PeerIceSignalingState::default();
    state.remember_remote_ufrag("current".to_string());

    assert_eq!(
        classify_remote_candidate_ufrag("future", "current", &state.known_remote_ufrags),
        RemoteCandidateDisposition::BufferFuture,
    );
    assert_eq!(
        classify_remote_candidate_ufrag("future", "future", &state.known_remote_ufrags),
        RemoteCandidateDisposition::Apply,
    );
}

#[test]
fn remote_generation_history_is_bounded_and_session_scoped() {
    let mut state = PeerIceSignalingState::default();
    for generation in 0..9 {
        state.remember_remote_ufrag(format!("generation-{generation}"));
    }
    assert_eq!(state.known_remote_ufrags.len(), 8);
    assert_eq!(
        state
            .known_remote_ufrags
            .front()
            .expect("bounded history should retain entries"),
        "generation-1"
    );
    assert_eq!(
        state
            .known_remote_ufrags
            .back()
            .expect("bounded history should retain entries"),
        "generation-8"
    );

    let replacement = PeerIceSignalingState::default();
    assert!(replacement.known_remote_ufrags.is_empty());
}

#[test]
fn media_level_ice_ufrag_overrides_session_level_value() {
    let description = RTCSessionDescription::offer(
        "v=0\r\n\
         o=- 0 0 IN IP4 127.0.0.1\r\n\
         s=-\r\n\
         t=0 0\r\n\
         a=ice-ufrag:session-generation\r\n\
         m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
         a=mid:0\r\n\
         a=ice-ufrag:media-generation\r\n\
         a=ice-pwd:test-password\r\n\
         m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
         a=mid:1\r\n\
         a=ice-ufrag:second-media-generation\r\n\
         a=ice-pwd:test-password\r\n"
            .to_string(),
    )
    .expect("test SDP should parse");

    assert_eq!(
        ice_ufrag_from_description(&description, Some("0"), Some(0)).as_deref(),
        Some("media-generation")
    );
    assert_eq!(
        ice_ufrag_from_description(&description, None, None).as_deref(),
        Some("session-generation")
    );
    assert_eq!(
        ice_ufrag_from_description(&description, Some(""), Some(0)).as_deref(),
        Some("media-generation")
    );
    assert_eq!(
        ice_ufrags_from_description(&description),
        vec![
            "session-generation".to_string(),
            "media-generation".to_string(),
            "second-media-generation".to_string(),
        ],
        "generation history must include every current media-level ufrag"
    );
}

#[tokio::test]
async fn aborted_local_generation_suppresses_late_candidates_until_next_generation() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, target.clone(), "restart-exchange").await;

    assert!(
        WebRtcCoordinator::begin_local_ice_generation(&coordinator.peers, &target, session_id)
            .await
    );
    WebRtcCoordinator::suppress_local_ice_generation(&coordinator.peers, &target, session_id).await;

    let disposition = WebRtcCoordinator::local_candidate_disposition(
        &coordinator.peers,
        &target,
        session_id,
        RTCIceCandidateInit::default(),
    )
    .await;
    assert_eq!(disposition, LocalCandidateDisposition::Suppressed);

    assert!(
        WebRtcCoordinator::begin_local_ice_generation(&coordinator.peers, &target, session_id)
            .await
    );
    let peers = coordinator.peers.read().await;
    assert!(matches!(
        peers
            .get(&target)
            .expect("peer should exist")
            .ice_signaling
            .local_generation,
        LocalIceGenerationState::Buffering(_)
    ));
}

#[tokio::test]
async fn candidate_gathered_before_restart_local_sdp_uses_new_generation_ufrag() {
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        Arc::new(CapturingSignalingClient::new()),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;

    assert!(
        WebRtcCoordinator::begin_local_ice_generation(&coordinator.peers, &target_id, session_id,)
            .await
    );
    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&target_id).expect("peer should exist");
        let LocalIceGenerationState::Buffering(candidates) =
            &mut state.ice_signaling.local_generation
        else {
            panic!("local generation should be buffering");
        };
        candidates.push(RTCIceCandidateInit {
            candidate: "candidate:1 1 UDP 1 127.0.0.1 5000 typ host".to_string(),
            sdp_mid: Some("0".to_string()),
            sdp_mline_index: Some(0),
            username_fragment: None,
        });
    }

    let new_description = RTCSessionDescription::offer(
        "v=0\r\n\
         o=- 0 0 IN IP4 127.0.0.1\r\n\
         s=-\r\n\
         t=0 0\r\n\
         m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
         a=mid:0\r\n\
         a=ice-ufrag:new-generation\r\n\
         a=ice-pwd:test-password\r\n"
            .to_string(),
    )
    .expect("test SDP should parse");
    let candidates = WebRtcCoordinator::finish_local_ice_generation(
        &coordinator.peers,
        &target_id,
        session_id,
        &new_description,
    )
    .await
    .expect("buffered candidate should use the new SDP");

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].username_fragment.as_deref(),
        Some("new-generation")
    );
    let peers = coordinator.peers.read().await;
    assert!(matches!(
        peers
            .get(&target_id)
            .expect("peer should exist")
            .ice_signaling
            .local_generation,
        LocalIceGenerationState::Idle
    ));
}

#[tokio::test]
async fn ambiguous_buffered_candidate_does_not_abort_local_ice_generation() {
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        Arc::new(CapturingSignalingClient::new()),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;

    assert!(
        WebRtcCoordinator::begin_local_ice_generation(&coordinator.peers, &target_id, session_id)
            .await
    );
    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&target_id).expect("peer should exist");
        let LocalIceGenerationState::Buffering(candidates) =
            &mut state.ice_signaling.local_generation
        else {
            panic!("local generation should be buffering");
        };
        candidates.push(RTCIceCandidateInit {
            candidate: "candidate:1 1 UDP 1 127.0.0.1 5000 typ host".to_string(),
            sdp_mid: None,
            sdp_mline_index: None,
            username_fragment: None,
        });
        candidates.push(RTCIceCandidateInit {
            candidate: "candidate:2 1 UDP 1 127.0.0.1 5001 typ host".to_string(),
            sdp_mid: Some("1".to_string()),
            sdp_mline_index: Some(1),
            username_fragment: None,
        });
    }

    let new_description = RTCSessionDescription::offer(
        "v=0\r\n\
         o=- 0 0 IN IP4 127.0.0.1\r\n\
         s=-\r\n\
         t=0 0\r\n\
         m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
         a=mid:0\r\n\
         a=ice-ufrag:generation-zero\r\n\
         a=ice-pwd:test-password-zero\r\n\
         m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
         a=mid:1\r\n\
         a=ice-ufrag:generation-one\r\n\
         a=ice-pwd:test-password-one\r\n"
            .to_string(),
    )
    .expect("test SDP should parse");
    let candidates = WebRtcCoordinator::finish_local_ice_generation(
        &coordinator.peers,
        &target_id,
        session_id,
        &new_description,
    )
    .await
    .expect("one ambiguous candidate must not abort the generation");

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].username_fragment.as_deref(),
        Some("generation-one")
    );
    assert_eq!(candidates[0].sdp_mid.as_deref(), Some("1"));
}

#[tokio::test]
async fn ice_restart_answer_create_error_resets_local_generation() {
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        Arc::new(CapturingSignalingClient::new()),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;
    coordinator
        .peers
        .write()
        .await
        .get_mut(&target_id)
        .expect("peer should exist")
        .is_offerer = false;

    let result = coordinator
        .handle_ice_restart_offer(
            &target_id,
            "not-valid-sdp".to_string(),
            Some("restart-exchange".to_string()),
        )
        .await;
    assert!(result.is_err(), "invalid restart offer should fail");

    let peers = coordinator.peers.read().await;
    let state = peers.get(&target_id).expect("peer should remain");
    assert!(matches!(
        state.ice_signaling.local_generation,
        LocalIceGenerationState::Suppressed
    ));
    drop(peers);
    coordinator
        .close_all_peers()
        .await
        .expect("test peer should close");
}

#[tokio::test]
async fn answer_send_failure_suppresses_candidates_and_removes_matching_session() {
    let send_control = SendControl::failing();
    let signaling_client = Arc::new(CapturingSignalingClient::with_send_control(Arc::clone(
        &send_control,
    )));
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;
    coordinator
        .peers
        .write()
        .await
        .get_mut(&target_id)
        .expect("peer should exist")
        .is_offerer = false;

    let api = webrtc::api::APIBuilder::new().build();
    let remote_pc = api
        .new_peer_connection(Default::default())
        .await
        .expect("remote peer connection should be created");
    remote_pc
        .create_data_channel("test", None)
        .await
        .expect("data channel should be created");
    let restart_offer = remote_pc
        .create_offer(None)
        .await
        .expect("restart offer should be created");
    remote_pc
        .set_local_description(restart_offer.clone())
        .await
        .expect("restart offer should become the remote local description");

    let error = coordinator
        .handle_ice_restart_offer(
            &target_id,
            restart_offer.sdp,
            Some("restart-exchange".to_string()),
        )
        .await
        .expect_err("injected Answer send failure should surface");
    assert!(
        error
            .to_string()
            .contains("injected signaling send failure")
    );
    tokio::time::timeout(Duration::from_secs(1), send_control.wait_until_started())
        .await
        .expect("the Answer send path must reach the signaling client");

    assert!(
        !coordinator.peers.read().await.contains_key(&target_id),
        "a Session with an uncommitted local Answer cannot remain active"
    );
    assert!(signaling_client.sent_envelopes().await.is_empty());
    remote_pc.close().await.expect("remote peer should close");
}

#[tokio::test]
async fn cleanup_waits_for_inflight_restart_answer_signaling() {
    let send_control = SendControl::blocking();
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        Arc::new(CapturingSignalingClient::with_send_control(Arc::clone(
            &send_control,
        ))),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;
    coordinator
        .peers
        .write()
        .await
        .get_mut(&target_id)
        .expect("peer should exist")
        .is_offerer = false;

    let api = webrtc::api::APIBuilder::new().build();
    let remote_pc = api
        .new_peer_connection(Default::default())
        .await
        .expect("remote peer connection should be created");
    remote_pc
        .create_data_channel("test", None)
        .await
        .expect("data channel should be created");
    let restart_offer = remote_pc
        .create_offer(None)
        .await
        .expect("restart offer should be created");
    remote_pc
        .set_local_description(restart_offer.clone())
        .await
        .expect("restart offer should become the remote local description");

    let restart_coordinator = Arc::clone(&coordinator);
    let restart_target = target_id.clone();
    let restart_handler = tokio::spawn(async move {
        restart_coordinator
            .handle_ice_restart_offer(
                &restart_target,
                restart_offer.sdp,
                Some("restart-exchange".to_string()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), send_control.wait_until_started())
        .await
        .expect("restart Answer must reach the signaling boundary");

    let cleanup_coordinator = Arc::clone(&coordinator);
    let cleanup_target = target_id.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_coordinator
            .cleanup_cancelled_connection(&cleanup_target, "test concurrent cleanup")
            .await;
    });
    let peer_removed_before_send_release = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !coordinator.peers.read().await.contains_key(&target_id) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();

    send_control.release.add_permits(256);
    restart_handler
        .await
        .expect("restart handler task should join")
        .expect("restart handler should complete");
    cleanup.await.expect("cleanup task should join");
    remote_pc.close().await.expect("remote peer should close");

    assert!(
        !peer_removed_before_send_release,
        "cleanup must not remove the peer while restart Answer signaling is in flight"
    );
    assert!(coordinator.peers.read().await.is_empty());
}

#[tokio::test]
async fn offerer_commit_keeps_cleanup_out_until_offer_and_candidates_finish() {
    let control = SendControl::blocking();
    let client = Arc::new(CapturingSignalingClient::with_send_control(Arc::clone(
        &control,
    )));
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, target.clone(), "restart-exchange").await;
    let epoch = coordinator
        .peer_signaling
        .restart_cancellation_epoch
        .load(Ordering::Acquire);
    let context = coordinator.peer_signaling_commit_context();
    let commit_target = target.clone();
    let commit_client: Arc<dyn SignalingClient> = client.clone();
    let commit = tokio::spawn(async move {
        let Some(_guard) = context
            .acquire_commit(&commit_target, session_id, Some(epoch))
            .await
        else {
            return false;
        };
        commit_client
            .send_envelope(SignalingEnvelope::default())
            .await
            .expect("offer send should succeed");
        commit_client
            .send_envelope(SignalingEnvelope::default())
            .await
            .expect("candidate send should succeed");
        true
    });

    control.wait_until_started().await;
    control.release.add_permits(1);
    control.wait_until_started().await;

    let cleanup_coordinator = Arc::clone(&coordinator);
    let cleanup_target = target.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_coordinator
            .cleanup_cancelled_connection(&cleanup_target, "test restart commit")
            .await;
    });
    tokio::task::yield_now().await;
    assert!(
        coordinator.peers.read().await.contains_key(&target),
        "cleanup must wait for the complete peer signaling commit"
    );

    control.release.add_permits(1);
    assert!(commit.await.expect("commit task should join"));
    cleanup.await.expect("cleanup task should join");
    assert_eq!(client.sent_envelopes().await.len(), 2);
    assert!(coordinator.peers.read().await.is_empty());
}

#[tokio::test]
async fn session_guarded_candidate_send_serializes_with_cleanup() {
    let send_control = SendControl::blocking();
    let signaling_client = Arc::new(CapturingSignalingClient::with_send_control(Arc::clone(
        &send_control,
    )));
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "current-exchange").await;
    let candidate = IceCandidate {
        candidate: "candidate:1 1 UDP 1 127.0.0.1 5000 typ host".to_string(),
        sdp_mid: Some("0".to_string()),
        sdp_mline_index: Some(0),
        username_fragment: Some("current-generation".to_string()),
    };

    let send_coordinator = Arc::clone(&coordinator);
    let send_target = target_id.clone();
    let send = tokio::spawn(async move {
        send_coordinator
            .commit_peer_signaling(
                &send_target,
                session_id,
                None,
                actr_relay::Payload::IceCandidate(candidate),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), send_control.wait_until_started())
        .await
        .expect("candidate send must reach the signaling boundary");

    let cleanup_coordinator = Arc::clone(&coordinator);
    let cleanup_target = target_id.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_coordinator
            .cleanup_cancelled_connection(&cleanup_target, "test candidate cleanup")
            .await;
    });
    let peer_removed_before_send_release = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !coordinator.peers.read().await.contains_key(&target_id) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();

    send_control.release.add_permits(1);
    assert!(
        send.await
            .expect("candidate send task should join")
            .expect("candidate send should succeed"),
        "current-session candidate should be sent"
    );
    cleanup.await.expect("cleanup task should join");

    assert!(
        !peer_removed_before_send_release,
        "cleanup must wait for the current-session candidate signaling commit"
    );
    assert_eq!(signaling_client.sent_envelopes().await.len(), 1);
}

#[tokio::test]
async fn candidate_arriving_before_restart_offer_is_buffered() {
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        Arc::new(CapturingSignalingClient::new()),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;

    let api = webrtc::api::APIBuilder::new().build();
    let remote_pc = api
        .new_peer_connection(Default::default())
        .await
        .expect("remote peer connection should be created");
    remote_pc
        .create_data_channel("test", None)
        .await
        .expect("data channel should be created");
    let current_offer = remote_pc
        .create_offer(None)
        .await
        .expect("current offer should be created");
    remote_pc
        .set_local_description(current_offer.clone())
        .await
        .expect("remote local description should be set");
    let peer_connection = coordinator
        .peers
        .read()
        .await
        .get(&target_id)
        .expect("peer should exist")
        .peer_connection
        .clone();
    peer_connection
        .set_remote_description(current_offer)
        .await
        .expect("current remote description should be set");

    let future_candidate = IceCandidate {
        candidate: "candidate:1 1 UDP 1 127.0.0.1 5000 typ host".to_string(),
        sdp_mid: Some("0".to_string()),
        sdp_mline_index: Some(0),
        username_fragment: Some("future-generation".to_string()),
    };
    coordinator
        .handle_ice_candidate(&target_id, future_candidate.clone())
        .await
        .expect("reordered candidate should be accepted for buffering");

    let pending = coordinator.pending_candidates.read().await;
    assert_eq!(pending.get(&target_id), Some(&vec![future_candidate]));
    drop(pending);
    remote_pc.close().await.expect("remote peer should close");
    coordinator
        .close_all_peers()
        .await
        .expect("coordinator peers should close");
}

#[tokio::test]
async fn pending_candidate_flush_drops_known_old_generation_and_keeps_unknown_future() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target_id = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;

    let api = webrtc::api::APIBuilder::new().build();
    let remote_pc = api
        .new_peer_connection(Default::default())
        .await
        .expect("remote peer connection should be created");
    remote_pc
        .create_data_channel("test", None)
        .await
        .expect("data channel should be created");
    let current_offer = remote_pc
        .create_offer(None)
        .await
        .expect("current offer should be created");
    let current_ufrag = ice_ufrag_from_description(&current_offer, None, None)
        .expect("generated offer should contain one ICE generation");
    remote_pc
        .set_local_description(current_offer.clone())
        .await
        .expect("remote local description should be set");

    let peer_connection = {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&target_id).expect("peer should exist");
        state
            .ice_signaling
            .remember_remote_ufrag("known-old-generation".to_string());
        state.peer_connection.clone()
    };
    peer_connection
        .set_remote_description(current_offer)
        .await
        .expect("current remote description should be set");

    let known_old_candidate = IceCandidate {
        candidate: "candidate:1 1 UDP 1 127.0.0.1 5000 typ host".to_string(),
        sdp_mid: None,
        sdp_mline_index: Some(0),
        username_fragment: Some("known-old-generation".to_string()),
    };
    let unknown_future_candidate = IceCandidate {
        candidate: "candidate:2 1 UDP 1 127.0.0.1 5001 typ host".to_string(),
        sdp_mid: None,
        sdp_mline_index: Some(0),
        username_fragment: Some("unknown-future-generation".to_string()),
    };
    coordinator.pending_candidates.write().await.insert(
        target_id.clone(),
        vec![
            known_old_candidate.clone(),
            unknown_future_candidate.clone(),
        ],
    );

    coordinator
        .flush_pending_candidates(&target_id, session_id.wrapping_add(1), &peer_connection)
        .await
        .expect("stale-session flush should be ignored cleanly");
    assert_eq!(
        coordinator.pending_candidates.read().await.get(&target_id),
        Some(&vec![
            known_old_candidate.clone(),
            unknown_future_candidate.clone()
        ]),
        "a stale flush must not consume the current session's candidate buffer"
    );
    {
        let peers = coordinator.peers.read().await;
        let history = &peers
            .get(&target_id)
            .expect("peer should remain current")
            .ice_signaling
            .known_remote_ufrags;
        assert_eq!(
            history,
            &VecDeque::from(["known-old-generation".to_string()]),
            "a stale flush must not record SDP state into the current session"
        );
    }

    coordinator
        .flush_pending_candidates(&target_id, session_id, &peer_connection)
        .await
        .expect("pending candidate flush should succeed");

    assert_eq!(
        coordinator.pending_candidates.read().await.get(&target_id),
        Some(&vec![unknown_future_candidate.clone()])
    );

    coordinator
        .handle_ice_candidate(&target_id, known_old_candidate)
        .await
        .expect("a late stale candidate should be discarded cleanly");
    assert_eq!(
        coordinator.pending_candidates.read().await.get(&target_id),
        Some(&vec![unknown_future_candidate.clone()]),
        "a known stale candidate arriving after flush must not be rebuffered"
    );
    let peers = coordinator.peers.read().await;
    let history = &peers
        .get(&target_id)
        .expect("peer should remain current")
        .ice_signaling
        .known_remote_ufrags;
    assert!(history.iter().any(|known| known == "known-old-generation"));
    assert!(history.iter().any(|known| known == &current_ufrag));
    drop(peers);

    remote_pc.close().await.expect("remote peer should close");
    coordinator
        .close_all_peers()
        .await
        .expect("coordinator peers should close");
}

#[tokio::test(start_paused = true)]
async fn pending_candidate_flush_obeys_its_total_deadline() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target_id = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;

    let api = webrtc::api::APIBuilder::new().build();
    let remote_pc = api
        .new_peer_connection(Default::default())
        .await
        .expect("remote peer connection should be created");
    remote_pc
        .create_data_channel("test", None)
        .await
        .expect("data channel should be created");
    let current_offer = remote_pc
        .create_offer(None)
        .await
        .expect("current offer should be created");
    remote_pc
        .set_local_description(current_offer.clone())
        .await
        .expect("remote local description should be set");

    let peer_connection = coordinator
        .peers
        .read()
        .await
        .get(&target_id)
        .expect("peer should exist")
        .peer_connection
        .clone();
    peer_connection
        .set_remote_description(current_offer)
        .await
        .expect("current remote description should be set");

    let pending_guard = coordinator.pending_candidates.write().await;
    let flush_task = tokio::spawn({
        let coordinator = Arc::clone(&coordinator);
        let target_id = target_id.clone();
        let peer_connection = Arc::clone(&peer_connection);
        async move {
            coordinator
                .flush_pending_candidates(&target_id, session_id, &peer_connection)
                .await
        }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(REMOTE_CANDIDATE_FLUSH_TIMEOUT + Duration::from_millis(1)).await;

    let error = flush_task
        .await
        .expect("flush task should finish")
        .expect_err("flush should time out while its state lock is blocked");
    assert!(matches!(error, ActrError::TimedOut));
    drop(pending_guard);

    remote_pc.close().await.expect("remote peer should close");
    coordinator
        .close_all_peers()
        .await
        .expect("coordinator peers should close");
}

#[tokio::test]
async fn replacement_cleanup_preserves_candidates_for_incoming_offer() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target_id = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "current-exchange").await;

    let incoming_offer = RTCSessionDescription::offer(
        "v=0\r\n\
         o=- 0 0 IN IP4 127.0.0.1\r\n\
         s=-\r\n\
         t=0 0\r\n\
         m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
         a=mid:0\r\n\
         a=ice-ufrag:incoming-generation\r\n\
         a=ice-pwd:test-password\r\n"
            .to_string(),
    )
    .expect("incoming Offer should parse");
    let stale_candidate = IceCandidate {
        candidate: "candidate:1 1 UDP 1 127.0.0.1 5000 typ host".to_string(),
        sdp_mid: Some("0".to_string()),
        sdp_mline_index: Some(0),
        username_fragment: Some("old-generation".to_string()),
    };
    let incoming_candidate = IceCandidate {
        candidate: "candidate:2 1 UDP 1 127.0.0.1 5001 typ host".to_string(),
        sdp_mid: Some("0".to_string()),
        sdp_mline_index: Some(0),
        username_fragment: Some("incoming-generation".to_string()),
    };
    coordinator.pending_candidates.write().await.insert(
        target_id.clone(),
        vec![stale_candidate, incoming_candidate.clone()],
    );

    assert!(
        coordinator
            .cleanup_cancelled_connection_for_offer(
                &target_id,
                session_id,
                "replaced by incoming Offer",
                &incoming_offer,
            )
            .await
    );

    assert!(coordinator.peers.read().await.get(&target_id).is_none());
    assert_eq!(
        coordinator.pending_candidates.read().await.get(&target_id),
        Some(&vec![incoming_candidate])
    );
}

#[tokio::test]
async fn stalled_restart_commit_for_one_peer_does_not_block_another_peer_gate() {
    let coordinator = WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        Arc::new(CapturingSignalingClient::new()),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    );
    let peer_a = test_actor_id(10);
    let peer_b = test_actor_id(20);
    let gate_a = coordinator.restart_signaling_gate_for(&peer_a).await;
    let _blocked_send = gate_a.lock().await;
    let gate_b = coordinator.restart_signaling_gate_for(&peer_b).await;

    let _peer_b_guard = tokio::time::timeout(Duration::from_millis(100), gate_b.lock())
        .await
        .expect("peer B cleanup gate must not wait for peer A signaling");
}

#[tokio::test]
async fn expired_restart_signaling_gates_are_pruned() {
    let coordinator = WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        Arc::new(CapturingSignalingClient::new()),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    );
    let peer_a = test_actor_id(10);
    let peer_b = test_actor_id(20);

    let gate_a = coordinator.restart_signaling_gate_for(&peer_a).await;
    let gate_a_again = coordinator.restart_signaling_gate_for(&peer_a).await;
    assert!(Arc::ptr_eq(&gate_a, &gate_a_again));
    WebRtcCoordinator::prune_restart_signaling_gates(&coordinator.peer_signaling.gates).await;
    let gate_a_after_prune = coordinator.restart_signaling_gate_for(&peer_a).await;
    assert!(Arc::ptr_eq(&gate_a, &gate_a_after_prune));
    assert_eq!(coordinator.peer_signaling.gates.lock().await.len(), 1);
    drop(gate_a);
    drop(gate_a_again);
    drop(gate_a_after_prune);

    let _gate_b = coordinator.restart_signaling_gate_for(&peer_b).await;
    let gates = coordinator.peer_signaling.gates.lock().await;
    assert!(!gates.contains_key(&peer_a));
    assert!(gates.contains_key(&peer_b));
}

#[tokio::test]
async fn clear_pending_restarts_waits_for_task_cancellation() {
    let local_id = test_actor_id(1);
    let target_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::new());
    let coordinator = Arc::new(WebRtcCoordinator::new(
        local_id,
        credential_state,
        signaling_client,
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;

    let dropped = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = oneshot::channel();
    let task_dropped = Arc::clone(&dropped);
    let handle = tokio::spawn(async move {
        let _drop_flag = TaskDropFlag(task_dropped);
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    });
    started_rx
        .await
        .expect("restart task should enter its pending state");

    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&target_id).expect("peer should exist");
        state.restart_task_handle = Some(handle);
        state.ice_restart_inflight = true;
    }

    coordinator.clear_pending_restarts().await;

    assert!(
        dropped.load(Ordering::Acquire),
        "clear_pending_restarts must await cancellation before returning"
    );
}

#[tokio::test]
async fn clear_pending_restarts_cancels_blocked_answerer_restart_request() {
    let send_control = SendControl::blocking();
    let signaling_client = Arc::new(CapturingSignalingClient::with_send_control(Arc::clone(
        &send_control,
    )));
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;
    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&target_id).expect("peer should exist");
        state.is_offerer = false;
        state.update_connection_state(RTCPeerConnectionState::Disconnected);
    }

    coordinator
        .restart_ice(&target_id)
        .await
        .expect("answerer restart request should be scheduled");
    tokio::time::timeout(Duration::from_secs(1), send_control.wait_until_started())
        .await
        .expect("the Answerer notification must reach the signaling client");

    tokio::time::timeout(Duration::from_secs(1), coordinator.clear_pending_restarts())
        .await
        .expect("restart cleanup must cancel the blocked signaling send");
    assert!(send_control.dropped.load(Ordering::Acquire));
    assert!(signaling_client.sent_envelopes().await.is_empty());
    let peers = coordinator.peers.read().await;
    assert!(
        peers
            .get(&target_id)
            .expect("peer should remain after restart cleanup")
            .restart_task_handle
            .is_none()
    );
    drop(peers);
    coordinator
        .close_all_peers()
        .await
        .expect("test peer should close");
}

#[tokio::test]
async fn close_all_hook_reentry_returns_without_self_deadlock() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target.clone(), "current-exchange").await;
    coordinator
        .peers
        .write()
        .await
        .get_mut(&target)
        .expect("peer should exist")
        .public_hook_state = PublicRtcHookState::Connected;

    let reentrant = Arc::clone(&coordinator);
    let hook: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
        let reentrant = Arc::clone(&reentrant);
        Box::pin(async move {
            if matches!(
                event,
                crate::wire::webrtc::HookEvent::WebRtcDisconnected { .. }
            ) {
                reentrant
                    .close_all_peers()
                    .await
                    .expect("reentrant close-all should return cleanly");
            }
        })
    });
    coordinator.set_hook_callback(hook);

    tokio::time::timeout(Duration::from_secs(1), coordinator.close_all_peers())
        .await
        .expect("reentrant close-all must not deadlock")
        .expect("outer close-all should succeed");
    assert!(coordinator.peers.read().await.is_empty());
}

#[tokio::test]
async fn cancelled_close_all_restores_shutdown_state() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target.clone(), "current-exchange").await;

    let gate = coordinator
        .peer_signaling_commit_context()
        .gate_for(&target)
        .await;
    let held_gate = gate.lock().await;
    let closing = {
        let coordinator = Arc::clone(&coordinator);
        tokio::spawn(async move { coordinator.close_all_peers().await })
    };

    tokio::time::timeout(Duration::from_secs(1), async {
        while !coordinator
            .peer_signaling
            .closing_all
            .load(Ordering::Acquire)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("close-all should enter shutdown state");

    closing.abort();
    assert!(
        closing
            .await
            .expect_err("close-all should be cancelled")
            .is_cancelled(),
        "aborting close-all should cancel its future"
    );
    assert!(
        !coordinator
            .peer_signaling
            .closing_all
            .load(Ordering::Acquire),
        "cancellation must restore the shutdown flag"
    );

    drop(held_gate);
    coordinator
        .close_all_peers()
        .await
        .expect("test peer should close after cancellation");
}

#[tokio::test]
async fn cancelled_close_all_after_drain_still_finishes_owned_teardown() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target.clone(), "current-exchange").await;
    let (connection, old_peer_connection) = {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&target).expect("peer should exist");
        state.public_hook_state = PublicRtcHookState::Connected;
        (state.webrtc_conn.clone(), state.peer_connection.clone())
    };

    let hook_started = Arc::new(Semaphore::new(0));
    let hook_release = Arc::new(Semaphore::new(0));
    let hook_completions = Arc::new(Mutex::new(Vec::new()));
    let hook: crate::wire::webrtc::HookCallback = Arc::new({
        let hook_started = Arc::clone(&hook_started);
        let hook_release = Arc::clone(&hook_release);
        let hook_completions = Arc::clone(&hook_completions);
        move |event| {
            let hook_started = Arc::clone(&hook_started);
            let hook_release = Arc::clone(&hook_release);
            let hook_completions = Arc::clone(&hook_completions);
            Box::pin(async move {
                match event {
                    crate::wire::webrtc::HookEvent::WebRtcDisconnected { .. } => {
                        hook_started.add_permits(1);
                        hook_release
                            .acquire()
                            .await
                            .expect("hook release semaphore should remain open")
                            .forget();
                        hook_completions.lock().await.push("old-idle");
                    }
                    crate::wire::webrtc::HookEvent::WebRtcConnected { .. } => {
                        hook_completions.lock().await.push("new-connected");
                    }
                    _ => {}
                }
            })
        }
    });
    coordinator.set_hook_callback(hook);

    let closing = {
        let coordinator = Arc::clone(&coordinator);
        tokio::spawn(async move { coordinator.close_all_peers().await })
    };
    tokio::time::timeout(Duration::from_secs(1), hook_started.acquire())
        .await
        .expect("teardown hook should start after peers are drained")
        .expect("hook-start semaphore should remain open")
        .forget();

    assert!(
        coordinator.peers.read().await.is_empty(),
        "close-all must drain peer state before invoking the teardown hook"
    );
    assert!(crate::transport::WireHandle::is_connected(&connection));

    closing.abort();
    assert!(
        closing
            .await
            .expect_err("outer close-all should be cancelled")
            .is_cancelled()
    );
    assert!(
        !coordinator
            .peer_signaling
            .closing_all
            .load(Ordering::Acquire),
        "cancelling the outer close-all must restore the shutdown flag"
    );

    let new_session_id =
        insert_pending_offer_peer(&coordinator, target.clone(), "new-exchange").await;
    coordinator
        .pending_candidates
        .write()
        .await
        .insert(target.clone(), Vec::new());
    coordinator
        .invoke_hook(crate::wire::webrtc::HookEvent::WebRtcConnected {
            peer_id: target.clone(),
            relayed: false,
        })
        .await;

    hook_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while crate::transport::WireHandle::is_connected(&connection)
            || Arc::strong_count(&old_peer_connection) > 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned teardown must fully close and release the old peer after outer cancellation");

    assert_eq!(
        hook_completions.lock().await.as_slice(),
        ["new-connected"],
        "a cancelled old-session hook must not complete after the new session hook"
    );
    assert_eq!(
        coordinator
            .peers
            .read()
            .await
            .get(&target)
            .expect("new session must survive old-session teardown")
            .session_id,
        new_session_id
    );
    assert!(
        coordinator
            .pending_candidates
            .read()
            .await
            .contains_key(&target),
        "old-session teardown must not clear new-session candidate state"
    );

    coordinator
        .close_all_peers()
        .await
        .expect("new test session should close");
}

#[tokio::test]
async fn peer_creation_epoch_capture_rejects_active_close_all() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let lifecycle_guard = coordinator.peer_signaling.lifecycle_gate.lock().await;
    let close_state_guard =
        match CloseAllStateGuard::enter(Arc::clone(&coordinator.peer_signaling)).await {
            CloseAllEntry::Leader(guard) => guard,
            CloseAllEntry::Follower(_) => panic!("test close-all state should start"),
        };
    drop(lifecycle_guard);

    assert!(
        coordinator.capture_peer_lifecycle_epoch().await.is_none(),
        "peer creation must stop before allocating a PeerConnection during close-all"
    );

    drop(close_state_guard);
    assert!(
        coordinator.capture_peer_lifecycle_epoch().await.is_some(),
        "peer creation may capture an epoch again after close-all finishes"
    );
}

#[tokio::test]
async fn stale_peer_creation_epoch_cannot_insert_after_close_all() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target.clone(), "current-exchange").await;
    let stale_epoch = coordinator
        .peer_signaling
        .peer_lifecycle_epoch
        .load(Ordering::Acquire);
    let peer = coordinator
        .peers
        .write()
        .await
        .remove(&target)
        .expect("test peer should exist");

    coordinator
        .close_all_peers()
        .await
        .expect("empty close-all should succeed");

    let rejected = coordinator
        .insert_peer_if_lifecycle_current(target.clone(), peer, stale_epoch)
        .await;
    let peer = match rejected {
        Ok(()) => panic!("stale peer creation must not cross the shutdown barrier"),
        Err(peer) => peer,
    };
    assert!(coordinator.peers.read().await.is_empty());
    coordinator
        .teardown_removed_peer_state(&target, peer, false, None, "stale test insertion")
        .await;
}

#[tokio::test]
async fn close_all_times_out_when_peer_commit_cannot_quiesce() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target.clone(), "current-exchange").await;

    let gate = coordinator
        .peer_signaling_commit_context()
        .gate_for(&target)
        .await;
    let held_gate = gate.lock().await;
    let result = tokio::time::timeout(Duration::from_secs(1), coordinator.close_all_peers())
        .await
        .expect("close-all should enforce its own quiesce deadline");
    assert!(matches!(result, Err(ActrError::TimedOut)));
    assert!(
        coordinator.peers.read().await.contains_key(&target),
        "a timed-out state commit must not partially drain peers"
    );
    assert!(
        !coordinator
            .peer_signaling
            .closing_all
            .load(Ordering::Acquire),
        "the shutdown flag must be restored after a deadline"
    );

    drop(held_gate);
    coordinator
        .close_all_peers()
        .await
        .expect("test peer should close after releasing its commit gate");
}

#[tokio::test]
async fn close_all_restart_join_timeout_restores_shutdown_state_without_drain() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target.clone(), "current-exchange").await;

    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let restart_handle = tokio::task::spawn_blocking(move || {
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    });
    started_rx
        .await
        .expect("blocking restart task should start before close-all");
    coordinator
        .peers
        .write()
        .await
        .get_mut(&target)
        .expect("peer should exist")
        .restart_task_handle = Some(restart_handle);

    let result = tokio::time::timeout(Duration::from_secs(1), coordinator.close_all_peers())
        .await
        .expect("close-all must enforce its own restart-join deadline");
    assert!(matches!(result, Err(ActrError::TimedOut)));
    assert!(
        coordinator.peers.read().await.contains_key(&target),
        "a restart-join timeout must happen before peer state is drained"
    );
    assert!(
        !coordinator
            .peer_signaling
            .closing_all
            .load(Ordering::Acquire),
        "the shutdown flag must be restored after restart-join timeout"
    );

    release_tx
        .send(())
        .expect("blocking restart task should still be releasable");
    coordinator
        .close_all_peers()
        .await
        .expect("peer should close after the blocking restart task is released");
}

#[tokio::test]
async fn close_all_closes_the_session_aware_webrtc_connection() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let target = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target.clone(), "current-exchange").await;
    let connection = coordinator
        .peers
        .read()
        .await
        .get(&target)
        .expect("test peer should exist")
        .webrtc_conn
        .clone();
    assert!(crate::transport::WireHandle::is_connected(&connection));

    coordinator
        .close_all_peers()
        .await
        .expect("close-all should succeed");

    assert!(
        !crate::transport::WireHandle::is_connected(&connection),
        "close-all must close the WebRtcConnection session, not only the raw peer"
    );
}

#[tokio::test]
async fn close_all_peers_cancels_blocked_answerer_restart_request() {
    let send_control = SendControl::blocking();
    let signaling_client = Arc::new(CapturingSignalingClient::with_send_control(Arc::clone(
        &send_control,
    )));
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;
    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&target_id).expect("peer should exist");
        state.is_offerer = false;
        state.update_connection_state(RTCPeerConnectionState::Disconnected);
    }

    coordinator
        .restart_ice(&target_id)
        .await
        .expect("answerer restart request should be scheduled");
    tokio::time::timeout(Duration::from_secs(1), send_control.wait_until_started())
        .await
        .expect("the Answerer notification must reach the signaling client");

    tokio::time::timeout(Duration::from_secs(1), coordinator.close_all_peers())
        .await
        .expect("peer cleanup must cancel the blocked signaling send")
        .expect("test peer should close");
    assert!(send_control.dropped.load(Ordering::Acquire));
    assert!(signaling_client.sent_envelopes().await.is_empty());
    assert!(coordinator.peers.read().await.is_empty());
}

#[tokio::test]
async fn close_all_peers_waits_for_restart_signaling_commit_before_drain() {
    let send_control = SendControl::blocking();
    let signaling_client = Arc::new(CapturingSignalingClient::with_send_control(Arc::clone(
        &send_control,
    )));
    let coordinator = Arc::new(WebRtcCoordinator::new(
        test_actor_id(1),
        CredentialState::new(test_credential(), None, None),
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let target_id = test_actor_id(99);
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;
    let commit_context = coordinator.peer_signaling_commit_context();
    let cancellation_epoch = coordinator
        .peer_signaling
        .restart_cancellation_epoch
        .load(Ordering::Acquire);
    let signaling_for_task: Arc<dyn SignalingClient> = signaling_client.clone();
    let target_for_task = target_id.clone();
    let signaling_task = tokio::spawn(async move {
        let commit_guard = commit_context
            .acquire_commit(&target_for_task, session_id, Some(cancellation_epoch))
            .await?;
        Some(
            WebRtcCoordinator::send_peer_signaling_envelope_while_guarded(
                &commit_guard,
                &signaling_for_task,
                SignalingEnvelope::default(),
            )
            .await,
        )
    });
    tokio::time::timeout(Duration::from_secs(1), send_control.wait_until_started())
        .await
        .expect("restart signaling must pass its final current-session check");

    let close_coordinator = Arc::clone(&coordinator);
    let close = tokio::spawn(async move { close_coordinator.close_all_peers().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !coordinator
            .peer_signaling
            .closing_all
            .load(Ordering::Acquire)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("close-all must enter its signaling quiescence window");
    tokio::time::timeout(
        Duration::from_millis(100),
        coordinator.restart_ice(&target_id),
    )
    .await
    .expect("a new restart must not queue behind signaling while close-all is active")
    .expect("restart rejection during close-all should be clean");

    let peer_removed_before_send_release =
        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if !coordinator.peers.read().await.contains_key(&target_id) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();

    send_control.release.add_permits(1);
    signaling_task
        .await
        .expect("restart signaling task should join")
        .expect("current restart signaling should not be rejected")
        .expect("restart signaling commit should succeed");
    close
        .await
        .expect("close-all task should join")
        .expect("all peers should close");

    assert!(
        !peer_removed_before_send_release,
        "close_all_peers must not drain a peer while its restart signaling commit is in flight"
    );
    assert_eq!(signaling_client.sent_envelopes().await.len(), 1);
    assert!(coordinator.peers.read().await.is_empty());
}

#[tokio::test]
async fn restart_cancellation_epoch_rejects_queued_restart() {
    let local_id = test_actor_id(1);
    let target_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::new());
    let coordinator = Arc::new(WebRtcCoordinator::new(
        local_id,
        credential_state,
        signaling_client,
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;

    let restart_signaling_gate = coordinator.restart_signaling_gate_for(&target_id).await;
    let signaling_guard = restart_signaling_gate.lock().await;
    let queued_coordinator = Arc::clone(&coordinator);
    let queued_target = target_id.clone();
    let queued_restart =
        tokio::spawn(async move { queued_coordinator.restart_ice(&queued_target).await });
    tokio::task::yield_now().await;

    let clearing_coordinator = Arc::clone(&coordinator);
    let clear = tokio::spawn(async move {
        clearing_coordinator.clear_pending_restarts().await;
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while coordinator
            .peer_signaling
            .restart_cancellation_epoch
            .load(Ordering::Acquire)
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("restart cancellation should advance the epoch");

    drop(signaling_guard);
    queued_restart
        .await
        .expect("queued restart task should join")
        .expect("queued restart should be discarded cleanly");
    clear.await.expect("restart clearing task should join");

    let peers = coordinator.peers.read().await;
    let state = peers.get(&target_id).expect("peer should remain");
    assert!(state.restart_task_handle.is_none());
    assert!(!state.ice_restart_inflight);
}

#[tokio::test]
async fn stale_peer_at_restart_signaling_boundary_does_not_send_offer() {
    let local_id = test_actor_id(1);
    let target_id = test_actor_id(99);
    let credential_state = CredentialState::new(test_credential(), None, None);
    let signaling_client = Arc::new(CapturingSignalingClient::new());
    let coordinator = Arc::new(WebRtcCoordinator::new(
        local_id,
        credential_state,
        signaling_client.clone(),
        WebRtcConfig::default(),
        Arc::new(MediaFrameRegistry::new()),
    ));
    let session_id =
        insert_pending_offer_peer(&coordinator, target_id.clone(), "restart-exchange").await;

    let restart_signaling_gate = coordinator.restart_signaling_gate_for(&target_id).await;
    let signaling_guard = restart_signaling_gate.lock().await;
    let cancellation_epoch = coordinator
        .peer_signaling
        .restart_cancellation_epoch
        .load(Ordering::Acquire);
    let signaling_coordinator = Arc::clone(&coordinator);
    let target_for_task = target_id.clone();
    let signaling_task = tokio::spawn(async move {
        signaling_coordinator
            .commit_peer_signaling(
                &target_for_task,
                session_id,
                Some(cancellation_epoch),
                actr_relay::Payload::SessionDescription(actr_protocol::SessionDescription {
                    r#type: SdpType::IceRestartOffer as i32,
                    sdp: "restart-offer".to_string(),
                    sdp_exchange_id: Some("restart-exchange".to_string()),
                }),
            )
            .await
    });
    tokio::task::yield_now().await;

    let removed_state = coordinator
        .peers
        .write()
        .await
        .remove(&target_id)
        .expect("peer should still exist before boundary validation");
    drop(signaling_guard);

    let send_result = tokio::time::timeout(Duration::from_secs(3), signaling_task)
        .await
        .expect("signaling task should finish after the gate is released")
        .expect("signaling task should join")
        .expect("stale signaling should be rejected without transport error");
    assert!(
        !send_result,
        "the signaling boundary must reject a removed peer"
    );
    assert!(
        signaling_client.sent_envelopes().await.is_empty(),
        "a restart offer must not be sent after its peer is removed"
    );

    removed_state
        .webrtc_conn
        .close()
        .await
        .expect("test peer should close");
}

#[test]
fn test_exponential_backoff_basic() {
    // Test basic exponential backoff: 5s -> 10s (capped)
    let mut backoff = ExponentialBackoff::new(
        Duration::from_secs(5),  // initial
        Duration::from_secs(10), // max
        Some(5),                 // max retries
    );

    // First delay: 5s
    assert_eq!(backoff.next(), Some(Duration::from_secs(5)));
    // Second delay: 10s (5*2 = 10, at max)
    assert_eq!(backoff.next(), Some(Duration::from_secs(10)));
    // Third delay: 10s (capped at max)
    assert_eq!(backoff.next(), Some(Duration::from_secs(10)));
    // Fourth delay: 10s
    assert_eq!(backoff.next(), Some(Duration::from_secs(10)));
    // Fifth delay: 10s
    assert_eq!(backoff.next(), Some(Duration::from_secs(10)));
    // Sixth: None (max retries reached)
    assert_eq!(backoff.next(), None);
}

#[test]
fn test_exponential_backoff_sequence_1_2_4_5() {
    // Test the exact ICE restart sequence: 1s -> 2s -> 4s -> 5s...
    let mut backoff = ExponentialBackoff::new(
        Duration::from_millis(ICE_RESTART_INITIAL_BACKOFF_MS),
        Duration::from_millis(ICE_RESTART_MAX_BACKOFF_MS),
        Some(10),
    );

    let delays: Vec<Duration> = backoff.by_ref().take(6).collect();

    assert_eq!(delays[0], Duration::from_secs(1)); // 1s
    assert_eq!(delays[1], Duration::from_secs(2)); // 2s
    assert_eq!(delays[2], Duration::from_secs(4)); // 4s
    assert_eq!(delays[3], Duration::from_secs(5)); // 5s (capped)
    assert_eq!(delays[4], Duration::from_secs(5)); // 5s
    assert_eq!(delays[5], Duration::from_secs(5)); // 5s
}

#[test]
fn test_exponential_backoff_with_total_duration() {
    // Test that with_total_duration sets up the backoff correctly
    // Verify behavior: backoff should produce delays until total duration is exceeded
    let mut backoff = ExponentialBackoff::with_total_duration(
        Duration::from_millis(100), // initial
        Duration::from_millis(200), // max
        Some(5),                    // max retries
        Duration::from_secs(60),    // total duration
    );

    // Should produce at least one delay since total duration is large
    let first = backoff.next();
    assert!(first.is_some(), "should produce at least one delay");
    assert_eq!(first.unwrap(), Duration::from_millis(100));
}

#[test]
fn test_exponential_backoff_no_max_retries() {
    // Test backoff without retry limit (None)
    let mut backoff = ExponentialBackoff::new(
        Duration::from_secs(1),
        Duration::from_secs(4),
        None, // no retry limit
    );

    // Should continue indefinitely (we just test a few)
    assert_eq!(backoff.next(), Some(Duration::from_secs(1)));
    assert_eq!(backoff.next(), Some(Duration::from_secs(2)));
    assert_eq!(backoff.next(), Some(Duration::from_secs(4))); // capped
    assert_eq!(backoff.next(), Some(Duration::from_secs(4)));
    assert_eq!(backoff.next(), Some(Duration::from_secs(4)));
    // Would continue forever...
}

#[test]
fn test_exponential_backoff_max_delay_cap() {
    // Test that delay is properly capped at max AFTER initial
    // Note: The first call returns initial_delay, then it's doubled and capped
    let mut backoff = ExponentialBackoff::new(
        Duration::from_secs(8),  // initial
        Duration::from_secs(10), // max
        Some(4),
    );

    // First: 8s (initial, not capped yet)
    assert_eq!(backoff.next(), Some(Duration::from_secs(8)));
    // Second: 10s (8*2=16, capped to 10)
    assert_eq!(backoff.next(), Some(Duration::from_secs(10)));
    // Third: 10s (capped)
    assert_eq!(backoff.next(), Some(Duration::from_secs(10)));
    // Fourth: 10s
    assert_eq!(backoff.next(), Some(Duration::from_secs(10)));
    // Fifth: None (max retries reached)
    assert_eq!(backoff.next(), None);
}

#[test]
fn codec_to_payload_type_maps_known_and_unknown() {
    assert_eq!(WebRtcCoordinator::codec_to_payload_type("VP8"), 96);
    assert_eq!(WebRtcCoordinator::codec_to_payload_type("H264"), 97);
    assert_eq!(WebRtcCoordinator::codec_to_payload_type("VP9"), 98);
    assert_eq!(WebRtcCoordinator::codec_to_payload_type("OPUS"), 111);
    // Case-insensitive.
    assert_eq!(WebRtcCoordinator::codec_to_payload_type("h264"), 97);
    assert_eq!(WebRtcCoordinator::codec_to_payload_type("opus"), 111);
    // Unknown codec falls back to 96 (VP8 default).
    assert_eq!(WebRtcCoordinator::codec_to_payload_type("AV1"), 96);
    assert_eq!(WebRtcCoordinator::codec_to_payload_type(""), 96);
}

#[test]
fn stale_peer_reap_uses_dedicated_disconnected_threshold() {
    use RTCPeerConnectionState::*;

    // Terminal states (Failed/Closed) are reaped after MAX_FAILED_DURATION (60s).
    assert!(stale_peer_reap_reason(Failed, Duration::from_secs(61)).is_some());
    assert!(stale_peer_reap_reason(Closed, Duration::from_secs(61)).is_some());
    assert!(stale_peer_reap_reason(Failed, Duration::from_secs(59)).is_none());
    assert!(stale_peer_reap_reason(Closed, Duration::from_secs(59)).is_none());

    // Disconnected gets the dedicated 90s threshold (restart budget + margin):
    // reaped past 90s ...
    assert!(stale_peer_reap_reason(Disconnected, Duration::from_secs(91)).is_some());
    // ... but NOT at the terminal-state threshold, where an ICE restart
    // (60s budget, measured from its trigger, not from the state change)
    // may still legitimately be recovering the peer ...
    assert!(stale_peer_reap_reason(Disconnected, Duration::from_secs(61)).is_none());
    assert!(stale_peer_reap_reason(Disconnected, Duration::from_secs(90)).is_none());
    // ... and a fresh Disconnected peer is never touched.
    assert!(stale_peer_reap_reason(Disconnected, Duration::from_secs(5)).is_none());

    // Healthy/transient states are never reaped, no matter how old.
    for state in [New, Connecting, Connected] {
        assert!(
            stale_peer_reap_reason(state, Duration::from_secs(3600)).is_none(),
            "{state:?} must never be reaped"
        );
    }
}

/// The health check reads the REAL-TIME connection state from the
/// RTCPeerConnection, not the cached `current_state`, so a peer whose pc is
/// healthy must survive even with an ancient `last_state_change`.
#[tokio::test]
async fn health_check_keeps_healthy_peer_with_stale_timestamp() {
    let coordinator = new_test_coordinator(test_actor_id(1));
    let peer_id = test_actor_id(2);
    insert_pending_offer_peer(&coordinator, peer_id.clone(), "exchange-hc").await;

    // Backdate the state-change timestamp far past every reap threshold.
    // (checked_sub keeps the test valid on hosts with a short Instant epoch;
    // the fallback leaves the timestamp fresh, which still must not reap.)
    {
        let mut peers = coordinator.peers.write().await;
        let state = peers.get_mut(&peer_id).expect("peer should exist");
        state.last_state_change = std::time::Instant::now()
            .checked_sub(Duration::from_secs(3600))
            .unwrap_or_else(std::time::Instant::now);
    }

    coordinator.check_and_cleanup_stale_connections().await;

    assert!(
        coordinator.peers.read().await.contains_key(&peer_id),
        "a peer whose RTCPeerConnection reports a healthy state (New) must \
         not be reaped regardless of last_state_change age"
    );
}

#[test]
fn is_ipv4_candidate_allowed_filters_ipv6_and_accepts_ipv4() {
    // IPv6 candidates are rejected.
    assert!(!is_ipv4_candidate_allowed("candidate:... fe80::1 ..."));
    assert!(!is_ipv4_candidate_allowed("candidate:... udp6 ..."));
    assert!(!is_ipv4_candidate_allowed("candidate:... ::1 ..."));

    // IPv4 candidates are accepted (loopback, private, public).
    assert!(is_ipv4_candidate_allowed("candidate:... 127.0.0.1 ..."));
    assert!(is_ipv4_candidate_allowed("candidate:... 192.168.1.10 ..."));
    assert!(is_ipv4_candidate_allowed("candidate:... 10.0.0.5 ..."));
    // No IPv6 marker → accepted.
    assert!(is_ipv4_candidate_allowed(
        "candidate:... 203.0.113.7 udp ..."
    ));
}
