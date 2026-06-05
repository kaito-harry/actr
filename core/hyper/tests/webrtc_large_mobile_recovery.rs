//! Integration tests for in-flight large WebRTC messages across mobile-style
//! network and app-lifecycle interruptions.

use actr_framework::Bytes;
use actr_hyper::lifecycle::{
    NetworkAvailability, NetworkEvent, NetworkRecoveryAction, NetworkSnapshot,
    NetworkTransportFlags, ReconnectReason, process_network_event_batch,
    select_network_recovery_action,
};
use actr_hyper::outbound::PeerGate;
use actr_hyper::test_support::{
    TestHarness, WebRtcFragmentSendEvent, WebRtcFragmentSendHook,
    install_webrtc_fragment_send_hook_for_test, spawn_response_receiver,
};
use actr_hyper::transport::{ConnectionEvent, ConnectionState};
use actr_hyper::wire::webrtc::{HookCallback, HookEvent, WebRtcCoordinator};
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActrId, DataStream, PayloadType, RpcEnvelope};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};

const SERVER: u64 = 100;
const MOBILE: u64 = 200;
const LARGE_PAYLOAD_SIZE: usize = 200 * 1024;

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

fn spawn_data_echo_responder(
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

                    let request = match RpcEnvelope::decode(message_data.as_ref()) {
                        Ok(request) => request,
                        Err(e) => {
                            tracing::error!("{} failed to decode RpcEnvelope: {}", name, e);
                            continue;
                        }
                    };

                    let response = RpcEnvelope {
                        request_id: request.request_id.clone(),
                        route_key: "response".to_string(),
                        payload: request.payload.clone(),
                        timeout_ms: 0,
                        ..Default::default()
                    };

                    if let Err(e) = gate.send_message(&sender_id, response).await {
                        tracing::error!(
                            "{} failed to send echo response for {}: {}",
                            name,
                            request.request_id,
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

async fn setup_mobile_to_server() -> (TestHarness, BackgroundTasks) {
    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(SERVER).await;
    harness.add_peer(MOBILE).await;

    let bg_tasks = BackgroundTasks {
        handles: vec![
            spawn_data_echo_responder(
                harness.peer(SERVER).coordinator.clone(),
                harness.peer(SERVER).gate.clone(),
                "server_data_echo",
            ),
            spawn_response_receiver(
                harness.peer(MOBILE).coordinator.clone(),
                harness.peer(MOBILE).gate.clone(),
                "mobile_response_receiver",
            ),
        ],
    };

    let server_id = harness.peer(SERVER).id.clone();
    let setup_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut setup_attempt = 0;
    loop {
        setup_attempt += 1;
        let setup = RpcEnvelope {
            request_id: format!("mobile_setup_ping_{setup_attempt}"),
            route_key: "test.setup".to_string(),
            payload: Some(Bytes::from_static(b"ping")),
            timeout_ms: 5_000,
            ..Default::default()
        };

        match tokio::time::timeout(
            Duration::from_secs(7),
            harness.peer(MOBILE).gate.send_request(&server_id, setup),
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

async fn expect_large_request_ok(
    harness: &TestHarness,
    request_id: &str,
    data: &[u8],
    expected_hash: &[u8; 32],
    timeout: Duration,
) {
    let handle = spawn_large_request(
        harness.peer(MOBILE).gate.clone(),
        harness.peer(SERVER).id.clone(),
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
        harness.peer(MOBILE).pending_count().await,
        0,
        "{} should leave no pending request",
        request_id
    );
}

async fn expect_large_request_eventually_ok(
    harness: &TestHarness,
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
            harness.peer(MOBILE).gate.clone(),
            harness.peer(SERVER).id.clone(),
            &attempt_id,
            data.to_vec(),
            5_000,
        );

        match tokio::time::timeout(Duration::from_secs(7), handle).await {
            Ok(Ok(Ok(response))) => {
                assert_payload_integrity(&attempt_id, &response, data, expected_hash);
                assert_eq!(
                    harness.peer(MOBILE).pending_count().await,
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

fn is_expected_bounded_transport_failure(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "connection",
        "request timeout",
        "closed",
        "recovering",
        "data channel",
        "datachannel",
        "channel error",
        "not opened",
        "timeout",
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

async fn process_mobile_events(harness: &TestHarness, events: Vec<NetworkEvent>) {
    let results =
        process_network_event_batch(events, harness.peer(MOBILE).network_processor()).await;
    assert!(results.iter().all(|result| result.success));
}

async fn inflight_short_offline_recovers_original_request() {
    let (harness, _bg_tasks) = setup_mobile_to_server().await;
    let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);

    let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
    let request = spawn_large_request(
        harness.peer(MOBILE).gate.clone(),
        harness.peer(SERVER).id.clone(),
        "inflight_short_offline",
        data.clone(),
        30_000,
    );

    let event = wait_for_send_to_pause(event_rx, "short offline").await;
    tracing::info!(
        "short offline paused msg_id={} fragment {}/{}",
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

    assert_payload_integrity("short offline", &response, &data, &hash);
    assert_eq!(harness.peer(MOBILE).pending_count().await, 0);
}

async fn inflight_network_type_switch_recovers_original_request() {
    let (harness, _bg_tasks) = setup_mobile_to_server().await;
    let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);

    let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
    let request = spawn_large_request(
        harness.peer(MOBILE).gate.clone(),
        harness.peer(SERVER).id.clone(),
        "inflight_network_type_switch",
        data.clone(),
        30_000,
    );

    let event = wait_for_send_to_pause(event_rx, "network type switch").await;
    tracing::info!(
        "network type switch paused msg_id={} fragment {}/{}",
        event.msg_id,
        event.frag_index + 1,
        event.total_frags
    );

    let events = vec![cellular_event(1)];
    assert_eq!(
        select_network_recovery_action(&events),
        NetworkRecoveryAction::Restore
    );
    process_mobile_events(&harness, events).await;

    release_send.notify_waiters();
    drop(hook_guard);

    let response = tokio::time::timeout(Duration::from_secs(30), request)
        .await
        .expect("network type switch request hung")
        .expect("network type switch request task panicked")
        .expect("network type switch request should recover");

    assert_payload_integrity("network type switch", &response, &data, &hash);
    assert_eq!(harness.peer(MOBILE).pending_count().await, 0);
}

async fn inflight_long_offline_fails_bounded_then_retries() {
    let (harness, _bg_tasks) = setup_mobile_to_server().await;
    let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);

    let (hook_guard, event_rx, _release_send) = pause_next_multifragment_send_after(1);
    let request = spawn_large_request(
        harness.peer(MOBILE).gate.clone(),
        harness.peer(SERVER).id.clone(),
        "inflight_long_offline",
        data.clone(),
        2_000,
    );

    let event = wait_for_send_to_pause(event_rx, "long offline").await;
    tracing::info!(
        "long offline paused msg_id={} fragment {}/{}",
        event.msg_id,
        event.frag_index + 1,
        event.total_frags
    );

    harness.simulate_disconnect();
    process_mobile_events(&harness, vec![offline_event(1)]).await;

    let err = expect_bounded_failure(
        request,
        "long offline original request",
        Duration::from_secs(5),
    )
    .await;
    assert!(
        err.contains("Request timeout"),
        "long offline should time out the in-flight request, got: {err}"
    );
    assert_eq!(harness.peer(MOBILE).pending_count().await, 0);
    drop(hook_guard);

    harness.simulate_reconnect();
    process_mobile_events(
        &harness,
        vec![network_event(2, true, false, false), cellular_event(3)],
    )
    .await;

    expect_large_request_eventually_ok(
        &harness,
        "long_offline_retry",
        &data,
        &hash,
        Duration::from_secs(25),
    )
    .await;
}

async fn inflight_short_background_survives_foreground_restore() {
    let (harness, _bg_tasks) = setup_mobile_to_server().await;
    let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);

    let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
    let request = spawn_large_request(
        harness.peer(MOBILE).gate.clone(),
        harness.peer(SERVER).id.clone(),
        "inflight_short_background",
        data.clone(),
        30_000,
    );

    let event = wait_for_send_to_pause(event_rx, "short background").await;
    tracing::info!(
        "short background paused msg_id={} fragment {}/{}",
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
    process_mobile_events(&harness, events).await;

    release_send.notify_waiters();
    drop(hook_guard);

    let response = tokio::time::timeout(Duration::from_secs(30), request)
        .await
        .expect("short background request hung")
        .expect("short background request task panicked")
        .expect("short background request should complete");

    assert_payload_integrity("short background", &response, &data, &hash);
    assert_eq!(harness.peer(MOBILE).pending_count().await, 0);
}

async fn inflight_long_background_is_bounded_and_retries() {
    let (harness, _bg_tasks) = setup_mobile_to_server().await;
    let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);

    let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
    let request = spawn_large_request(
        harness.peer(MOBILE).gate.clone(),
        harness.peer(SERVER).id.clone(),
        "inflight_long_background",
        data.clone(),
        8_000,
    );

    let event = wait_for_send_to_pause(event_rx, "long background").await;
    tracing::info!(
        "long background paused msg_id={} fragment {}/{}",
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
    process_mobile_events(&harness, events).await;

    release_send.notify_waiters();
    drop(hook_guard);

    expect_bounded_completion(
        request,
        "long background original request",
        &data,
        &hash,
        Duration::from_secs(12),
    )
    .await;
    assert_eq!(harness.peer(MOBILE).pending_count().await, 0);

    expect_large_request_eventually_ok(
        &harness,
        "long_background_retry",
        &data,
        &hash,
        Duration::from_secs(25),
    )
    .await;
}

async fn mobile_large_message_baseline_after_recovery() {
    let (harness, _bg_tasks) = setup_mobile_to_server().await;
    let (data, hash) = generate_test_data(LARGE_PAYLOAD_SIZE);

    expect_large_request_ok(
        &harness,
        "mobile_large_baseline",
        &data,
        &hash,
        Duration::from_secs(30),
    )
    .await;
}

async fn mobile_data_stream_channel_close_emits_delivery_uncertain_hook() {
    let (harness, _bg_tasks) = setup_mobile_to_server().await;
    let server_id = harness.peer(SERVER).id.clone();

    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel::<HookEvent>();
    let hook: HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    harness.peer(MOBILE).coordinator.set_hook_callback(hook);

    let stream = DataStream {
        stream_id: "mobile-large-data-stream".to_string(),
        sequence: 7,
        payload: Bytes::from(vec![0x5a; LARGE_PAYLOAD_SIZE]),
        metadata: Vec::new(),
        timestamp_ms: Some(0),
    };
    let payload = Bytes::from(stream.encode_to_vec());

    harness
        .peer(MOBILE)
        .gate
        .send_data_stream(
            &server_id,
            PayloadType::StreamReliable,
            &stream.stream_id,
            payload,
        )
        .await
        .expect("mobile data stream send should reach transport");

    let session_id = harness
        .peer(MOBILE)
        .coordinator
        .get_peer_session_id(&server_id)
        .await
        .expect("mobile should have an active WebRTC session to server");

    for state in [ConnectionState::Disconnected, ConnectionState::Failed] {
        harness
            .peer(MOBILE)
            .send_event(ConnectionEvent::StateChanged {
                peer_id: server_id.clone(),
                session_id,
                state,
            });
    }
    harness
        .peer(MOBILE)
        .send_event(ConnectionEvent::IceRestartStarted {
            peer_id: server_id.clone(),
            session_id,
        });

    let nonterminal_event = tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            if let Some(event) = hook_rx.recv().await {
                if matches!(event, HookEvent::DataStreamDeliveryUncertain { .. }) {
                    return event;
                }
            }
        }
    })
    .await;
    assert!(
        nonterminal_event.is_err(),
        "non-terminal recovery state should not emit delivery uncertainty: {nonterminal_event:?}"
    );

    harness
        .peer(MOBILE)
        .send_event(ConnectionEvent::DataChannelClosed {
            peer_id: server_id.clone(),
            session_id,
            payload_type: PayloadType::StreamReliable,
        });

    let event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = hook_rx.recv().await {
                if matches!(event, HookEvent::DataStreamDeliveryUncertain { .. }) {
                    return event;
                }
            }
        }
    })
    .await
    .expect("data stream delivery uncertain hook was not emitted");

    match event {
        HookEvent::DataStreamDeliveryUncertain {
            stream_id,
            session_id: got_session_id,
            reason,
        } => {
            assert_eq!(stream_id, "mobile-large-data-stream");
            assert_eq!(got_session_id, session_id);
            assert_eq!(reason, "data channel closed");
        }
        other => panic!("unexpected hook event: {other:?}"),
    }
}

async fn inflight_data_stream_long_offline_is_bounded_or_delivery_uncertain() {
    let (harness, _bg_tasks) = setup_mobile_to_server().await;
    let server_id = harness.peer(SERVER).id.clone();

    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel::<HookEvent>();
    let hook: HookCallback = Arc::new(move |event| {
        let hook_tx = hook_tx.clone();
        Box::pin(async move {
            let _ = hook_tx.send(event);
        })
    });
    harness.peer(MOBILE).coordinator.set_hook_callback(hook);

    let stream = DataStream {
        stream_id: "inflight-long-offline-stream".to_string(),
        sequence: 11,
        payload: Bytes::from(vec![0x33; LARGE_PAYLOAD_SIZE]),
        metadata: Vec::new(),
        timestamp_ms: Some(0),
    };
    let payload = Bytes::from(stream.encode_to_vec());

    let (hook_guard, event_rx, release_send) = pause_next_multifragment_send_after(1);
    let send_task = {
        let gate = harness.peer(MOBILE).gate.clone();
        let server_id = server_id.clone();
        let stream_id = stream.stream_id.clone();
        tokio::spawn(async move {
            gate.send_data_stream(&server_id, PayloadType::StreamReliable, &stream_id, payload)
                .await
        })
    };

    let event = wait_for_send_to_pause(event_rx, "long offline data stream").await;
    tracing::info!(
        "long offline data stream paused msg_id={} fragment {}/{}",
        event.msg_id,
        event.frag_index + 1,
        event.total_frags
    );

    harness.simulate_disconnect();
    process_mobile_events(&harness, vec![offline_event(1)]).await;
    release_send.notify_waiters();
    drop(hook_guard);

    // PeerGate bounds DataStream sends at 15s; the assertion window must allow
    // the production timeout to fire instead of racing it with a shorter test timeout.
    let send_result = tokio::time::timeout(Duration::from_secs(20), send_task)
        .await
        .expect("in-flight DataStream send should not hang after long offline")
        .expect("in-flight DataStream task should not panic");

    if let Err(err) = send_result {
        let msg = err.to_string();
        assert!(
            is_expected_bounded_transport_failure(&msg),
            "unexpected in-flight DataStream failure: {msg}"
        );
    } else {
        let event = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(event) = hook_rx.recv().await {
                    if matches!(event, HookEvent::DataStreamDeliveryUncertain { .. }) {
                        return event;
                    }
                }
            }
        })
        .await
        .expect("successful in-flight DataStream during offline must emit delivery uncertainty");

        match event {
            HookEvent::DataStreamDeliveryUncertain { stream_id, .. } => {
                assert_eq!(stream_id, "inflight-long-offline-stream");
            }
            other => panic!("unexpected hook event: {other:?}"),
        }
    }

    assert_eq!(
        harness.peer(MOBILE).pending_count().await,
        0,
        "in-flight DataStream offline path should not leak RPC pending state"
    );

    harness.simulate_reconnect();
    process_mobile_events(
        &harness,
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
    expect_large_request_eventually_ok(
        &harness,
        "after_data_stream_long_offline",
        &retry_payload,
        &retry_hash,
        Duration::from_secs(25),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mobile_inflight_large_message_interruptions() {
    init_tracing();

    mobile_large_message_baseline_after_recovery().await;
    inflight_network_type_switch_recovers_original_request().await;
    inflight_short_offline_recovers_original_request().await;
    inflight_long_offline_fails_bounded_then_retries().await;
    inflight_short_background_survives_foreground_restore().await;
    inflight_long_background_is_bounded_and_retries().await;
    mobile_data_stream_channel_close_emits_delivery_uncertain_hook().await;
    inflight_data_stream_long_offline_is_bounded_or_delivery_uncertain().await;
}
