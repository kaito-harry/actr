//! Integration tests for in-flight large WebRTC messages across mobile-style
//! network and app-lifecycle interruptions.

use actr_framework::Bytes;
use actr_hyper::lifecycle::{
    NetworkAvailability, NetworkEvent, NetworkEventHandle, NetworkEventProcessor,
    NetworkEventResult, NetworkRecoveryAction, NetworkSnapshot, NetworkTransportFlags,
    ReconnectReason, process_network_event_batch, run_network_event_reconciler,
    select_network_recovery_action,
};
use actr_hyper::outbound::PeerGate;
use actr_hyper::test_support::{
    TestHarness, WebRtcFragmentSendEvent, WebRtcFragmentSendHook,
    install_webrtc_fragment_send_hook_for_test,
};
use actr_hyper::transport::{ConnectionEvent, ConnectionState};
use actr_hyper::wire::webrtc::{HookCallback, HookEvent, WebRtcCoordinator};
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActrError, ActrId, DataChunk, Direction, PayloadType, RpcEnvelope};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const LARGE_PAYLOAD_SIZE: usize = 200 * 1024;

#[derive(Clone, Copy)]
struct RoleCase {
    name: &'static str,
    mobile_serial: u64,
    server_serial: u64,
}

#[derive(Clone, Copy)]
struct DirectionCase {
    name: &'static str,
    from_serial: u64,
    to_serial: u64,
}

impl RoleCase {
    fn directions(self) -> [DirectionCase; 2] {
        [
            DirectionCase {
                name: "mobile_to_server",
                from_serial: self.mobile_serial,
                to_serial: self.server_serial,
            },
            DirectionCase {
                name: "server_to_mobile",
                from_serial: self.server_serial,
                to_serial: self.mobile_serial,
            },
        ]
    }
}

const ROLE_CASES: [RoleCase; 2] = [
    RoleCase {
        name: "mobile_offerer",
        mobile_serial: 100,
        server_serial: 200,
    },
    RoleCase {
        name: "mobile_answerer",
        mobile_serial: 200,
        server_serial: 100,
    },
];

struct BackgroundTasks {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
}

fn generate_test_data(size: usize) -> (Vec<u8>, [u8; 32]) {
    let data: Vec<u8> = (0..size).map(|i| ((i * 31 + 7) % 251) as u8).collect();
    let hash: [u8; 32] = Sha256::digest(&data).into();
    (data, hash)
}

fn network_event(sequence: u64, available: bool, wifi: bool, cellular: bool) -> NetworkEvent {
    NetworkEvent::NetworkPathChanged {
        snapshot: NetworkSnapshot {
            sequence,
            availability: if available {
                NetworkAvailability::Available
            } else {
                NetworkAvailability::Unavailable
            },
            transport: NetworkTransportFlags {
                wifi,
                cellular,
                ethernet: false,
                vpn: false,
                other: false,
            },
            is_expensive: false,
            is_constrained: false,
        },
    }
}

fn wifi_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, true, true, false)
}

fn cellular_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, true, false, true)
}

fn offline_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, false, false, false)
}

fn spawn_rpc_router(
    coordinator: Arc<WebRtcCoordinator>,
    gate: Arc<PeerGate>,
    name: &str,
) -> tokio::task::JoinHandle<()> {
    let name = name.to_string();
    tokio::spawn(async move {
        loop {
            match coordinator.receive_message().await {
                Ok(Some((sender_id_bytes, message_data, _payload_type))) => {
                    let sender_id = match ActrId::decode(&sender_id_bytes[..]) {
                        Ok(id) => id,
                        Err(e) => {
                            tracing::error!("{} failed to decode sender ID: {}", name, e);
                            continue;
                        }
                    };

                    let envelope = match RpcEnvelope::decode(message_data.as_ref()) {
                        Ok(envelope) => envelope,
                        Err(e) => {
                            tracing::error!("{} failed to decode RpcEnvelope: {}", name, e);
                            continue;
                        }
                    };

                    if envelope.route_key == "response" {
                        let result = if let Some(error) = envelope.error {
                            Err(actr_protocol::ActrError::Unavailable(format!(
                                "RPC error {}: {}",
                                error.code, error.message
                            )))
                        } else if let Some(payload) = envelope.payload {
                            Ok(payload)
                        } else {
                            Err(actr_protocol::ActrError::DecodeFailure(
                                "Invalid response: no payload or error".to_string(),
                            ))
                        };

                        if let Err(e) = gate.handle_response(&envelope.request_id, result).await {
                            tracing::error!(
                                "{} failed to handle response {}: {}",
                                name,
                                envelope.request_id,
                                e
                            );
                        }
                        continue;
                    }

                    let response = RpcEnvelope {
                        request_id: envelope.request_id.clone(),
                        route_key: "response".to_string(),
                        payload: envelope.payload.clone(),
                        timeout_ms: 0,
                        direction: Some(Direction::Response as i32),
                        ..Default::default()
                    };

                    if let Err(e) = gate.send_response(&sender_id, response).await {
                        tracing::error!(
                            "{} failed to send echo response for {}: {}",
                            name,
                            envelope.request_id,
                            e
                        );
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("{} receive loop failed: {}", name, e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
}

async fn setup_mobile_to_server_with_serials(
    mobile_serial: u64,
    server_serial: u64,
) -> (TestHarness, BackgroundTasks) {
    let mut harness = TestHarness::with_vnet().await;
    for serial in [
        mobile_serial.min(server_serial),
        mobile_serial.max(server_serial),
    ] {
        harness.add_peer(serial).await;
    }

    let bg_tasks = BackgroundTasks {
        handles: vec![
            spawn_rpc_router(
                harness.peer(mobile_serial).coordinator.clone(),
                harness.peer(mobile_serial).gate.clone(),
                "mobile_rpc_router",
            ),
            spawn_rpc_router(
                harness.peer(server_serial).coordinator.clone(),
                harness.peer(server_serial).gate.clone(),
                "server_rpc_router",
            ),
        ],
    };

    let server_id = harness.peer(server_serial).id.clone();
    let setup_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut setup_attempt = 0;
    loop {
        setup_attempt += 1;
        let setup = RpcEnvelope {
            request_id: format!("mobile_setup_ping_{setup_attempt}"),
            route_key: "test.setup".to_string(),
            payload: Some(Bytes::from_static(b"ping")),
            direction: Some(Direction::Request as i32),
            timeout_ms: 5_000,
            ..Default::default()
        };

        match tokio::time::timeout(
            Duration::from_secs(7),
            harness
                .peer(mobile_serial)
                .gate
                .send_request(&server_id, setup),
        )
        .await
        {
            Ok(Ok(_)) => break,
            Ok(Err(err)) => {
                let msg = err.to_string();
                assert!(
                    is_expected_bounded_transport_failure(&msg),
                    "mobile setup request failed with unexpected error: {msg}"
                );
            }
            Err(_) => {}
        }

        if tokio::time::Instant::now() >= setup_deadline {
            panic!("mobile setup request did not succeed within 30s");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    (harness, bg_tasks)
}

fn spawn_large_request(
    gate: Arc<PeerGate>,
    target_id: ActrId,
    request_id: &str,
    data: Vec<u8>,
    timeout_ms: i64,
) -> tokio::task::JoinHandle<actr_protocol::ActorResult<Bytes>> {
    let request_id = request_id.to_string();
    tokio::spawn(async move {
        let envelope = RpcEnvelope {
            request_id,
            route_key: "test.large_echo".to_string(),
            payload: Some(Bytes::from(data)),
            direction: Some(Direction::Request as i32),
            timeout_ms,
            ..Default::default()
        };
        gate.send_request(&target_id, envelope).await
    })
}

fn assert_payload_integrity(label: &str, response: &Bytes, data: &[u8], expected_hash: &[u8; 32]) {
    assert_eq!(
        response.len(),
        data.len(),
        "{} response length mismatch",
        label
    );

    let response_hash: [u8; 32] = Sha256::digest(response.as_ref()).into();
    assert_eq!(
        &response_hash, expected_hash,
        "{} response SHA-256 mismatch",
        label
    );

    assert_eq!(
        response.as_ref(),
        data,
        "{} response bytes were corrupted",
        label
    );
}

async fn expect_large_request_ok_between(
    harness: &TestHarness,
    from_serial: u64,
    to_serial: u64,
    request_id: &str,
    data: &[u8],
    expected_hash: &[u8; 32],
    timeout: Duration,
) {
    let handle = spawn_large_request(
        harness.peer(from_serial).gate.clone(),
        harness.peer(to_serial).id.clone(),
        request_id,
        data.to_vec(),
        timeout.as_millis() as i64,
    );

    let response = tokio::time::timeout(timeout + Duration::from_secs(2), handle)
        .await
        .unwrap_or_else(|_| panic!("{} did not complete within {:?}", request_id, timeout))
        .expect("large request task panicked")
        .unwrap_or_else(|e| panic!("{} failed: {}", request_id, e));

    assert_payload_integrity(request_id, &response, data, expected_hash);
    assert_eq!(
        harness.peer(from_serial).pending_count().await,
        0,
        "{} should leave no pending request",
        request_id
    );
}

async fn expect_large_request_eventually_ok_between(
    harness: &TestHarness,
    from_serial: u64,
    to_serial: u64,
    request_id: &str,
    data: &[u8],
    expected_hash: &[u8; 32],
    deadline: Duration,
) {
    let stop_at = tokio::time::Instant::now() + deadline;
    let mut attempt = 0;

    loop {
        attempt += 1;
        let attempt_id = format!("{request_id}_{attempt}");
        let handle = spawn_large_request(
            harness.peer(from_serial).gate.clone(),
            harness.peer(to_serial).id.clone(),
            &attempt_id,
            data.to_vec(),
            5_000,
        );

        match tokio::time::timeout(Duration::from_secs(7), handle).await {
            Ok(Ok(Ok(response))) => {
                assert_payload_integrity(&attempt_id, &response, data, expected_hash);
                assert_eq!(
                    harness.peer(from_serial).pending_count().await,
                    0,
                    "{} should leave no pending request",
                    request_id
                );
                return;
            }
            Ok(Ok(Err(err))) => {
                let msg = err.to_string();
                assert!(
                    is_expected_bounded_transport_failure(&msg),
                    "{} got unexpected retry error: {}",
                    request_id,
                    msg
                );
            }
            Ok(Err(err)) => panic!("{} retry task panicked: {}", request_id, err),
            Err(_) => {}
        }

        if tokio::time::Instant::now() >= stop_at {
            panic!("{} did not recover within {:?}", request_id, deadline);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn assert_pending_empty(harness: &TestHarness, serial: u64, label: &str) {
    assert_eq!(
        harness.peer(serial).pending_count().await,
        0,
        "{label} should leave no pending request state"
    );
}

fn is_expected_bounded_transport_failure(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "connection",
        "request timeout",
        "timed out",
        "closed",
        "recovering",
        "data channel",
        "datachannel",
        "channel error",
        "not opened",
        "timeout",
        "unavailable",
        "not found",
        "no route",
        "all transport candidates exhausted",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn pause_next_multifragment_send_after(
    frag_index: u16,
) -> (
    actr_hyper::test_support::WebRtcFragmentSendHookGuard,
    oneshot::Receiver<WebRtcFragmentSendEvent>,
    Arc<Notify>,
) {
    let (event_tx, event_rx) = oneshot::channel();
    let event_tx = Arc::new(StdMutex::new(Some(event_tx)));
    let release = Arc::new(Notify::new());
    let paused = Arc::new(AtomicBool::new(false));

    let hook: WebRtcFragmentSendHook = {
        let event_tx = Arc::clone(&event_tx);
        let release = Arc::clone(&release);
        let paused = Arc::clone(&paused);
        Arc::new(move |event: WebRtcFragmentSendEvent| {
            let event_tx = Arc::clone(&event_tx);
            let release = Arc::clone(&release);
            let paused = Arc::clone(&paused);
            Box::pin(async move {
                if event.total_frags <= 1 || event.frag_index != frag_index {
                    return;
                }
                if paused.swap(true, Ordering::SeqCst) {
                    return;
                }
                if let Some(tx) = event_tx
                    .lock()
                    .expect("pause hook sender mutex poisoned")
                    .take()
                {
                    let _ = tx.send(event);
                }
                release.notified().await;
            })
        })
    };

    (
        install_webrtc_fragment_send_hook_for_test(hook),
        event_rx,
        release,
    )
}

async fn wait_for_send_to_pause(
    event_rx: oneshot::Receiver<WebRtcFragmentSendEvent>,
    label: &str,
) -> WebRtcFragmentSendEvent {
    let event = tokio::time::timeout(Duration::from_secs(10), event_rx)
        .await
        .unwrap_or_else(|_| panic!("{} did not pause during fragmented send", label))
        .expect("fragment pause hook dropped before firing");

    assert!(
        event.total_frags > 1,
        "{} must pause a multi-fragment message",
        label
    );
    event
}

async fn expect_bounded_failure(
    handle: tokio::task::JoinHandle<actr_protocol::ActorResult<Bytes>>,
    label: &str,
    timeout: Duration,
) -> String {
    match tokio::time::timeout(timeout, handle).await {
        Ok(Ok(Err(err))) => err.to_string(),
        Ok(Ok(Ok(response))) => panic!(
            "{} unexpectedly succeeded with {} bytes",
            label,
            response.len()
        ),
        Ok(Err(err)) => panic!("{} task panicked: {}", label, err),
        Err(_) => panic!("{} did not fail within {:?}", label, timeout),
    }
}

async fn expect_bounded_completion(
    handle: tokio::task::JoinHandle<actr_protocol::ActorResult<Bytes>>,
    label: &str,
    data: &[u8],
    expected_hash: &[u8; 32],
    timeout: Duration,
) {
    match tokio::time::timeout(timeout, handle).await {
        Ok(Ok(Ok(response))) => assert_payload_integrity(label, &response, data, expected_hash),
        Ok(Ok(Err(err))) => {
            let msg = err.to_string();
            assert!(
                is_expected_bounded_transport_failure(&msg),
                "{} got unexpected bounded failure: {}",
                label,
                msg
            );
        }
        Ok(Err(err)) => panic!("{} task panicked: {}", label, err),
        Err(_) => panic!("{} did not complete within {:?}", label, timeout),
    }
}

async fn process_mobile_events(harness: &TestHarness, case: RoleCase, events: Vec<NetworkEvent>) {
    process_mobile_events_for(harness, case.mobile_serial, events).await;
}

async fn process_mobile_events_for(
    harness: &TestHarness,
    mobile_serial: u64,
    events: Vec<NetworkEvent>,
) {
    let results =
        process_network_event_batch(events, harness.peer(mobile_serial).network_processor()).await;
    assert!(results.iter().all(|result| result.success));
}

fn spawn_mobile_event_storm(handle: NetworkEventHandle) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let batches = [
            vec![
                offline_event(101),
                network_event(102, true, false, false),
                cellular_event(103),
            ],
            vec![
                NetworkEvent::ForceReconnect {
                    reason: ReconnectReason::StaleConnectionSuspected,
                },
                network_event(104, true, false, false),
                wifi_event(105),
            ],
            vec![network_event(106, true, false, true), wifi_event(107)],
        ];

        let mut tasks = Vec::new();
        for batch in batches {
            for event in batch {
                let handle = handle.clone();
                tasks.push(tokio::spawn(async move {
                    submit_mobile_event(handle, event).await
                }));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        for task in tasks {
            let result = task
                .await
                .expect("mobile network event task should not panic")
                .expect("mobile network event should complete");
            assert!(result.success, "mobile network event failed: {result:?}");
        }
    })
}

fn spawn_network_event_reconciler(
    processor: Arc<dyn NetworkEventProcessor>,
    result_timeout: Duration,
) -> (
    NetworkEventHandle,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let (event_tx, event_rx) = mpsc::channel(32);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, result_timeout);
    let shutdown = CancellationToken::new();
    let reconciler_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    (handle, shutdown, task)
}

async fn submit_mobile_event(
    handle: NetworkEventHandle,
    event: NetworkEvent,
) -> Result<NetworkEventResult, String> {
    match event {
        NetworkEvent::NetworkPathChanged { snapshot } => {
            handle.handle_network_path_changed(snapshot).await
        }
        NetworkEvent::AppLifecycleChanged { state } => {
            handle.handle_app_lifecycle_changed(state).await
        }
        NetworkEvent::CleanupConnections { reason } => handle.cleanup_connections(reason).await,
        NetworkEvent::ForceReconnect { reason } => handle.force_reconnect(reason).await,
    }
}

async fn inflight_short_offline_recovers_original_request(case: RoleCase) {
    for direction in case.directions() {
        let (harness, _bg_tasks) =
            setup_mobile_to_server_with_serials(case.mobile_serial, case.server_serial).await;
        let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);
        let request_id = format!("{}_{}_inflight_short_offline", case.name, direction.name);

        let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
        let request = spawn_large_request(
            harness.peer(direction.from_serial).gate.clone(),
            harness.peer(direction.to_serial).id.clone(),
            &request_id,
            data.clone(),
            30_000,
        );

        let event = wait_for_send_to_pause(event_rx, &request_id).await;
        tracing::info!(
            "{} short offline paused msg_id={} fragment {}/{}",
            direction.name,
            event.msg_id,
            event.frag_index + 1,
            event.total_frags
        );

        harness.simulate_disconnect();
        tokio::time::sleep(Duration::from_secs(1)).await;
        harness.simulate_reconnect();
        release_send.notify_waiters();
        drop(hook_guard);

        let response = tokio::time::timeout(Duration::from_secs(30), request)
            .await
            .expect("short offline request hung")
            .expect("short offline request task panicked")
            .expect("short offline request should recover");

        assert_payload_integrity(&request_id, &response, &data, &hash);
        assert_pending_empty(&harness, direction.from_serial, &request_id).await;
    }
}

async fn inflight_network_type_switch_recovers_or_retries_once(case: RoleCase) {
    for direction in case.directions() {
        let (harness, _bg_tasks) =
            setup_mobile_to_server_with_serials(case.mobile_serial, case.server_serial).await;
        let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);
        let request_id = format!(
            "{}_{}_inflight_network_type_switch",
            case.name, direction.name
        );

        let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
        let request = spawn_large_request(
            harness.peer(direction.from_serial).gate.clone(),
            harness.peer(direction.to_serial).id.clone(),
            &request_id,
            data.clone(),
            30_000,
        );

        let event = wait_for_send_to_pause(event_rx, &request_id).await;
        tracing::info!(
            "{} network type switch paused msg_id={} fragment {}/{}",
            direction.name,
            event.msg_id,
            event.frag_index + 1,
            event.total_frags
        );

        let events = vec![cellular_event(1)];
        assert_eq!(
            select_network_recovery_action(&events),
            NetworkRecoveryAction::Restore
        );
        process_mobile_events(&harness, case, events).await;

        release_send.notify_waiters();
        drop(hook_guard);

        let result = tokio::time::timeout(Duration::from_secs(32), request)
            .await
            .expect("network type switch request hung")
            .expect("network type switch request task panicked");

        match result {
            Ok(response) => {
                assert_payload_integrity(&request_id, &response, &data, &hash);
                assert_pending_empty(&harness, direction.from_serial, &request_id).await;
            }
            Err(ActrError::TimedOut) => {
                assert_pending_empty(&harness, direction.from_serial, &request_id).await;

                let retry_id = format!(
                    "{}_{}_inflight_network_type_switch_retry",
                    case.name, direction.name
                );
                expect_large_request_ok_between(
                    &harness,
                    direction.from_serial,
                    direction.to_serial,
                    &retry_id,
                    &data,
                    &hash,
                    Duration::from_secs(30),
                )
                .await;
            }
            Err(err) => panic!(
                "network type switch request should succeed or time out before one retry: {err}"
            ),
        }
    }
}

async fn inflight_long_offline_fails_bounded_then_retries(case: RoleCase) {
    for direction in case.directions() {
        let (harness, _bg_tasks) =
            setup_mobile_to_server_with_serials(case.mobile_serial, case.server_serial).await;
        let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);
        let request_id = format!("{}_{}_inflight_long_offline", case.name, direction.name);
        let retry_id = format!("{}_{}_long_offline_retry", case.name, direction.name);

        let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
        let request = spawn_large_request(
            harness.peer(direction.from_serial).gate.clone(),
            harness.peer(direction.to_serial).id.clone(),
            &request_id,
            data.clone(),
            2_000,
        );

        let event = wait_for_send_to_pause(event_rx, &request_id).await;
        tracing::info!(
            "{} long offline paused msg_id={} fragment {}/{}",
            direction.name,
            event.msg_id,
            event.frag_index + 1,
            event.total_frags
        );

        harness.simulate_disconnect();
        process_mobile_events(&harness, case, vec![offline_event(1)]).await;

        let err = expect_bounded_failure(request, &request_id, Duration::from_secs(5)).await;
        assert!(
            (err.contains("Request timeout") || err.contains("timed out")),
            "long offline should time out the in-flight request, got: {err}"
        );
        assert_pending_empty(&harness, direction.from_serial, &request_id).await;
        // Unblock the parked fragment send before recovery begins. The
        // original request has already timed out, so the send must be released
        // before reconnect or the retry cannot make forward progress. Unlike
        // the short-offline case (which notifies after reconnect, while the
        // original request is still alive), this path must notify first:
        // dropping `release_send` at end of scope does not wake the waiter, so
        // `notify_waiters()` must precede `drop(hook_guard)`.
        release_send.notify_waiters();
        drop(hook_guard);

        harness.simulate_reconnect();
        let events = vec![
            NetworkEvent::ForceReconnect {
                reason: ReconnectReason::StaleConnectionSuspected,
            },
            network_event(2, true, false, false),
            cellular_event(3),
        ];
        assert_eq!(
            select_network_recovery_action(&events),
            NetworkRecoveryAction::ForceReconnect
        );
        process_mobile_events(&harness, case, events).await;

        expect_large_request_eventually_ok_between(
            &harness,
            direction.from_serial,
            direction.to_serial,
            &retry_id,
            &data,
            &hash,
            Duration::from_secs(25),
        )
        .await;
    }
}

async fn inflight_short_background_survives_foreground_restore(case: RoleCase) {
    for direction in case.directions() {
        let (harness, _bg_tasks) =
            setup_mobile_to_server_with_serials(case.mobile_serial, case.server_serial).await;
        let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);
        let request_id = format!("{}_{}_inflight_short_background", case.name, direction.name);
        let retry_id = format!("{}_{}_short_background_retry", case.name, direction.name);

        let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
        let request = spawn_large_request(
            harness.peer(direction.from_serial).gate.clone(),
            harness.peer(direction.to_serial).id.clone(),
            &request_id,
            data.clone(),
            5_000,
        );

        let event = wait_for_send_to_pause(event_rx, &request_id).await;
        tracing::info!(
            "{} short background paused msg_id={} fragment {}/{}",
            direction.name,
            event.msg_id,
            event.frag_index + 1,
            event.total_frags
        );

        tokio::time::sleep(Duration::from_millis(500)).await;
        let events = vec![network_event(1, true, false, false), wifi_event(2)];
        assert_eq!(
            select_network_recovery_action(&events),
            NetworkRecoveryAction::Restore
        );
        process_mobile_events(&harness, case, events).await;

        release_send.notify_waiters();
        drop(hook_guard);

        expect_bounded_completion(request, &request_id, &data, &hash, Duration::from_secs(8)).await;
        assert_pending_empty(&harness, direction.from_serial, &request_id).await;

        // If the original request failed, the caller retries until the recovery
        // guard clears. Fresh requests remain intentionally fail-fast.
        expect_large_request_eventually_ok_between(
            &harness,
            direction.from_serial,
            direction.to_serial,
            &retry_id,
            &data,
            &hash,
            Duration::from_secs(30),
        )
        .await;
    }
}

async fn inflight_long_background_is_bounded_and_retries(case: RoleCase) {
    for direction in case.directions() {
        let (harness, _bg_tasks) =
            setup_mobile_to_server_with_serials(case.mobile_serial, case.server_serial).await;
        let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);
        let request_id = format!("{}_{}_inflight_long_background", case.name, direction.name);
        let retry_id = format!("{}_{}_long_background_retry", case.name, direction.name);

        let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
        let request = spawn_large_request(
            harness.peer(direction.from_serial).gate.clone(),
            harness.peer(direction.to_serial).id.clone(),
            &request_id,
            data.clone(),
            8_000,
        );

        let event = wait_for_send_to_pause(event_rx, &request_id).await;
        tracing::info!(
            "{} long background paused msg_id={} fragment {}/{}",
            direction.name,
            event.msg_id,
            event.frag_index + 1,
            event.total_frags
        );

        let events = vec![
            NetworkEvent::ForceReconnect {
                reason: ReconnectReason::LongBackground,
            },
            network_event(1, true, false, false),
            wifi_event(2),
        ];
        assert_eq!(
            select_network_recovery_action(&events),
            NetworkRecoveryAction::ForceReconnect
        );
        process_mobile_events(&harness, case, events).await;

        release_send.notify_waiters();
        drop(hook_guard);

        expect_bounded_completion(request, &request_id, &data, &hash, Duration::from_secs(12))
            .await;
        assert_pending_empty(&harness, direction.from_serial, &request_id).await;

        expect_large_request_eventually_ok_between(
            &harness,
            direction.from_serial,
            direction.to_serial,
            &retry_id,
            &data,
            &hash,
            Duration::from_secs(25),
        )
        .await;
    }
}

async fn mobile_large_message_baseline_after_recovery(case: RoleCase) {
    for direction in case.directions() {
        let (harness, _bg_tasks) =
            setup_mobile_to_server_with_serials(case.mobile_serial, case.server_serial).await;
        let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);
        let request_id = format!("{}_{}_mobile_large_baseline", case.name, direction.name);

        expect_large_request_ok_between(
            &harness,
            direction.from_serial,
            direction.to_serial,
            &request_id,
            &data,
            &hash,
            Duration::from_secs(30),
        )
        .await;
    }
}

async fn mobile_data_chunk_channel_close_emits_delivery_uncertain_hook(case: RoleCase) {
    for direction in case.directions() {
        let (harness, _bg_tasks) =
            setup_mobile_to_server_with_serials(case.mobile_serial, case.server_serial).await;
        let target_id = harness.peer(direction.to_serial).id.clone();

        let (hook_tx, mut hook_rx) = mpsc::unbounded_channel::<HookEvent>();
        let hook: HookCallback = Arc::new(move |event| {
            let hook_tx = hook_tx.clone();
            Box::pin(async move {
                let _ = hook_tx.send(event);
            })
        });
        harness
            .peer(direction.from_serial)
            .coordinator
            .set_hook_callback(hook);

        let stream = DataChunk {
            stream_id: format!("{}-{}-large-data-chunk", case.name, direction.name),
            sequence: 7,
            payload: Bytes::from(vec![0x5a; LARGE_PAYLOAD_SIZE]),
            metadata: Vec::new(),
            timestamp_ms: Some(0),
        };
        let payload = Bytes::from(stream.encode_to_vec());

        harness
            .peer(direction.from_serial)
            .gate
            .send_data_chunk(
                &target_id,
                PayloadType::StreamReliable,
                &stream.stream_id,
                payload,
            )
            .await
            .expect("data stream send should reach transport");

        let session_id = harness
            .peer(direction.from_serial)
            .coordinator
            .get_peer_session_id(&target_id)
            .await
            .expect("source should have an active WebRTC session to target");

        for state in [ConnectionState::Disconnected, ConnectionState::Failed] {
            harness
                .peer(direction.from_serial)
                .send_event(ConnectionEvent::StateChanged {
                    peer_id: target_id.clone(),
                    session_id,
                    state,
                });
        }
        harness
            .peer(direction.from_serial)
            .send_event(ConnectionEvent::IceRestartStarted {
                peer_id: target_id.clone(),
                session_id,
            });

        let nonterminal_event = tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                if let Some(event) = hook_rx.recv().await {
                    if matches!(event, HookEvent::DataChunkDeliveryUncertain { .. }) {
                        return event;
                    }
                }
            }
        })
        .await;
        assert!(
            nonterminal_event.is_err(),
            "{} non-terminal recovery state should not emit delivery uncertainty: {nonterminal_event:?}",
            direction.name
        );

        harness
            .peer(direction.from_serial)
            .send_event(ConnectionEvent::DataChannelClosed {
                peer_id: target_id.clone(),
                session_id,
                payload_type: PayloadType::StreamReliable,
            });

        let event = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(event) = hook_rx.recv().await {
                    if matches!(event, HookEvent::DataChunkDeliveryUncertain { .. }) {
                        return event;
                    }
                }
            }
        })
        .await
        .expect("data stream delivery uncertain hook was not emitted");

        match event {
            HookEvent::DataChunkDeliveryUncertain {
                stream_id,
                session_id: got_session_id,
                reason,
            } => {
                assert_eq!(stream_id, stream.stream_id);
                assert_eq!(got_session_id, session_id);
                assert_eq!(reason, "data channel closed");
            }
            other => panic!("unexpected hook event: {other:?}"),
        }
    }
}

async fn inflight_data_chunk_long_offline_is_bounded_or_delivery_uncertain(case: RoleCase) {
    for direction in case.directions() {
        let (harness, _bg_tasks) =
            setup_mobile_to_server_with_serials(case.mobile_serial, case.server_serial).await;
        let target_id = harness.peer(direction.to_serial).id.clone();

        let (hook_tx, mut hook_rx) = mpsc::unbounded_channel::<HookEvent>();
        let hook: HookCallback = Arc::new(move |event| {
            let hook_tx = hook_tx.clone();
            Box::pin(async move {
                let _ = hook_tx.send(event);
            })
        });
        harness
            .peer(direction.from_serial)
            .coordinator
            .set_hook_callback(hook);

        let stream = DataChunk {
            stream_id: format!(
                "{}-{}-inflight-long-offline-stream",
                case.name, direction.name
            ),
            sequence: 11,
            payload: Bytes::from(vec![0x33; LARGE_PAYLOAD_SIZE]),
            metadata: Vec::new(),
            timestamp_ms: Some(0),
        };
        let payload = Bytes::from(stream.encode_to_vec());

        let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
        let send_task = {
            let gate = harness.peer(direction.from_serial).gate.clone();
            let target_id = target_id.clone();
            let stream_id = stream.stream_id.clone();
            tokio::spawn(async move {
                gate.send_data_chunk(&target_id, PayloadType::StreamReliable, &stream_id, payload)
                    .await
            })
        };

        let event = wait_for_send_to_pause(event_rx, "long offline data stream").await;
        tracing::info!(
            "{} long offline data stream paused msg_id={} fragment {}/{}",
            direction.name,
            event.msg_id,
            event.frag_index + 1,
            event.total_frags
        );

        harness.simulate_disconnect();
        process_mobile_events(&harness, case, vec![offline_event(1)]).await;
        release_send.notify_waiters();
        drop(hook_guard);

        let send_result = tokio::time::timeout(Duration::from_secs(20), send_task)
            .await
            .expect("in-flight DataChunk send should not hang after long offline")
            .expect("in-flight DataChunk task should not panic");

        if let Err(err) = send_result {
            let msg = err.to_string();
            assert!(
                is_expected_bounded_transport_failure(&msg),
                "unexpected in-flight DataChunk failure: {msg}"
            );
        } else {
            let event = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(event) = hook_rx.recv().await {
                        if matches!(event, HookEvent::DataChunkDeliveryUncertain { .. }) {
                            return event;
                        }
                    }
                }
            })
            .await
            .expect("successful in-flight DataChunk during offline must emit delivery uncertainty");

            match event {
                HookEvent::DataChunkDeliveryUncertain { stream_id, .. } => {
                    assert_eq!(stream_id, stream.stream_id);
                }
                other => panic!("unexpected hook event: {other:?}"),
            }
        }

        assert_pending_empty(&harness, direction.from_serial, &stream.stream_id).await;

        harness.simulate_reconnect();
        process_mobile_events(
            &harness,
            case,
            vec![
                NetworkEvent::ForceReconnect {
                    reason: ReconnectReason::StaleConnectionSuspected,
                },
                network_event(2, true, false, false),
                wifi_event(3),
            ],
        )
        .await;

        let (retry_payload, retry_hash) = generate_test_data(LARGE_PAYLOAD_SIZE);
        let retry_id = format!(
            "{}_{}_after_data_chunk_long_offline",
            case.name, direction.name
        );
        expect_large_request_eventually_ok_between(
            &harness,
            direction.from_serial,
            direction.to_serial,
            &retry_id,
            &retry_payload,
            &retry_hash,
            Duration::from_secs(25),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mobile_event_storm_during_call_and_data_chunk_does_not_hang() {
    init_tracing();

    for case in ROLE_CASES {
        let (harness, _bg_tasks) =
            setup_mobile_to_server_with_serials(case.mobile_serial, case.server_serial).await;
        let server_id = harness.peer(case.server_serial).id.clone();
        let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);
        let (network_handle, network_shutdown, network_task) = spawn_network_event_reconciler(
            harness.peer(case.mobile_serial).network_processor(),
            Duration::from_secs(20),
        );

        harness.simulate_disconnect();
        tokio::time::sleep(Duration::from_secs(1)).await;
        harness.simulate_reconnect();

        let storm_task = spawn_mobile_event_storm(network_handle);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let call_id = format!("{}_mobile_event_storm_concurrent_call", case.name);
        let call = spawn_large_request(
            harness.peer(case.mobile_serial).gate.clone(),
            server_id.clone(),
            &call_id,
            data.clone(),
            5_000,
        );

        let stream = DataChunk {
            stream_id: format!("{}-mobile-event-storm-concurrent-stream", case.name),
            sequence: 19,
            payload: Bytes::from(vec![0x42; LARGE_PAYLOAD_SIZE]),
            metadata: Vec::new(),
            timestamp_ms: Some(0),
        };
        let payload = Bytes::from(stream.encode_to_vec());
        let stream_task = {
            let gate = harness.peer(case.mobile_serial).gate.clone();
            let stream_id = stream.stream_id.clone();
            tokio::spawn(async move {
                gate.send_data_chunk(&server_id, PayloadType::StreamReliable, &stream_id, payload)
                    .await
            })
        };

        expect_bounded_completion(call, &call_id, &data, &hash, Duration::from_secs(8)).await;

        let stream_result = tokio::time::timeout(Duration::from_secs(20), stream_task)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{} DataChunk send during mobile event storm should not hang",
                    case.name
                )
            })
            .expect("DataChunk send task should not panic");
        if let Err(err) = stream_result {
            let msg = err.to_string();
            assert!(
                is_expected_bounded_transport_failure(&msg),
                "{} unexpected DataChunk error during mobile event storm: {msg}",
                case.name
            );
        }

        storm_task
            .await
            .expect("mobile event storm task should not panic");
        network_shutdown.cancel();
        network_task
            .await
            .expect("network event reconciler task should not panic");
        assert_eq!(
            harness.peer(case.mobile_serial).pending_count().await,
            0,
            "{} mobile event storm send path should not leak pending state",
            case.name
        );

        let (retry_payload, retry_hash) = generate_test_data(LARGE_PAYLOAD_SIZE);
        expect_large_request_ok_between(
            &harness,
            case.mobile_serial,
            case.server_serial,
            &format!("{}_after_mobile_event_storm_retry_once", case.name),
            &retry_payload,
            &retry_hash,
            Duration::from_secs(30),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mobile_inflight_large_message_interruptions() {
    init_tracing();

    for case in ROLE_CASES {
        mobile_large_message_baseline_after_recovery(case).await;
        inflight_network_type_switch_recovers_or_retries_once(case).await;
        inflight_short_offline_recovers_original_request(case).await;
        inflight_long_offline_fails_bounded_then_retries(case).await;
        inflight_short_background_survives_foreground_restore(case).await;
        inflight_long_background_is_bounded_and_retries(case).await;
        mobile_data_chunk_channel_close_emits_delivery_uncertain_hook(case).await;
        inflight_data_chunk_long_offline_is_bounded_or_delivery_uncertain(case).await;
    }
}
