//! Integration tests for PeerGate disconnection/reconnection
//!
//! Uses TestHarness for multi-peer topology with VNet network simulation.
//!
//! Tests focus on:
//! - Two-peer disconnect → network event → ICE restart → reconnect
//! - Offerer vs Answerer recovery latency comparison
//!
//! ## Recovery latency tests (Test 2 & 3)
//!
//! Both tests use a **short outage (8s)** so the connection stays in the
//! peers map and `do_ice_restart_inner` is still running (in its backoff loop).
//!
//! The key difference (Plan A implemented):
//! - **Offerer test**: offerer calls `retry_failed()` → `restart_ice()` → already inflight → wakes backoff
//! - **Answerer test**: answerer calls `retry_failed()` → `restart_ice()` → `!is_offerer`
//!   → sends IceRestartRequest → Offerer receives → wakes backoff → immediate retry

use actr_hyper::lifecycle::{
    DefaultNetworkEventProcessor, NetworkAvailability, NetworkEvent, NetworkEventProcessor,
    NetworkRecoveryAction, NetworkSnapshot, NetworkTransportFlags, ReconnectReason,
    process_network_event_batch, select_network_recovery_action,
};
use actr_hyper::test_support::TestHarness;
use actr_hyper::transport::{ConnectionEvent, ConnectionState, Dest};
use actr_protocol::{ActrId, PayloadType};
use std::time::{Duration, Instant};

/// Initialize tracing for test output
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
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

fn offline_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, false, false, false)
}

fn online_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, true, false, false)
}

fn wifi_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, true, true, false)
}

async fn wait_for_data_channel_opened(
    event_rx: &mut tokio::sync::broadcast::Receiver<ConnectionEvent>,
    peer_id: &ActrId,
    payload_type: PayloadType,
    timeout: Duration,
) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for {:?} DataChannelOpened for peer {:?}",
            payload_type,
            peer_id
        );

        match tokio::time::timeout(remaining, event_rx.recv()).await {
            Ok(Ok(ConnectionEvent::DataChannelOpened {
                peer_id: event_peer,
                session_id,
                payload_type: event_payload_type,
            })) if &event_peer == peer_id && event_payload_type == payload_type => {
                return session_id;
            }
            Ok(Ok(_)) => {}
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                tracing::warn!("Connection event receiver lagged by {} events", n);
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("connection event channel closed while waiting for DataChannelOpened");
            }
            Err(_) => {
                panic!(
                    "timed out waiting for {:?} DataChannelOpened for peer {:?}",
                    payload_type, peer_id
                );
            }
        }
    }
}

async fn wait_for_data_channel_close_chain(
    event_rx: &mut tokio::sync::broadcast::Receiver<ConnectionEvent>,
    peer_id: &ActrId,
    session_id: u64,
    timeout: Duration,
) -> PayloadType {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut closed_payload_type = None;
    let mut saw_peer_connection_closed = false;
    let mut saw_connection_closed = false;

    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let event = match tokio::time::timeout(remaining, event_rx.recv()).await {
            Ok(Ok(event)) => event,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                tracing::warn!("Connection event receiver lagged by {} events", n);
                continue;
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("connection event channel closed while waiting for close chain");
            }
            Err(_) => break,
        };

        match event {
            ConnectionEvent::DataChannelClosed {
                peer_id: event_peer,
                session_id: event_session_id,
                payload_type,
            } if &event_peer == peer_id && event_session_id == session_id => {
                tracing::info!(
                    "Observed DataChannelClosed for peer {:?}, session_id={}, payload_type={:?}",
                    peer_id,
                    session_id,
                    payload_type
                );
                closed_payload_type.get_or_insert(payload_type);
            }
            ConnectionEvent::ConnectionClosed {
                peer_id: event_peer,
                session_id: event_session_id,
            } if &event_peer == peer_id && event_session_id == session_id => {
                tracing::info!(
                    "Observed ConnectionClosed for peer {:?}, session_id={}",
                    peer_id,
                    session_id
                );
                saw_connection_closed = true;
            }
            ConnectionEvent::StateChanged {
                peer_id: event_peer,
                session_id: event_session_id,
                state: ConnectionState::Closed,
            } if &event_peer == peer_id && event_session_id == session_id => {
                tracing::info!(
                    "Observed PeerConnection Closed for peer {:?}, session_id={}",
                    peer_id,
                    session_id
                );
                saw_peer_connection_closed = true;
            }
            _ => {}
        }

        if let Some(payload_type) = closed_payload_type
            && saw_peer_connection_closed
            && saw_connection_closed
        {
            return payload_type;
        }
    }

    panic!(
        "timed out waiting for DataChannelClosed -> PeerConnection Closed -> ConnectionClosed chain for peer {:?}, session_id={}, saw_data_channel_closed={}, saw_peer_connection_closed={}, saw_connection_closed={}",
        peer_id,
        session_id,
        closed_payload_type.is_some(),
        saw_peer_connection_closed,
        saw_connection_closed
    );
}

async fn wait_for_connection_closed(
    event_rx: &mut tokio::sync::broadcast::Receiver<ConnectionEvent>,
    peer_id: &ActrId,
    session_id: u64,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for ConnectionClosed for peer {:?}, session_id={}",
            peer_id,
            session_id
        );

        match tokio::time::timeout(remaining, event_rx.recv()).await {
            Ok(Ok(ConnectionEvent::ConnectionClosed {
                peer_id: event_peer,
                session_id: event_session_id,
            })) if &event_peer == peer_id && event_session_id == session_id => return,
            Ok(Ok(_)) => {}
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                tracing::warn!("Connection event receiver lagged by {} events", n);
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("connection event channel closed while waiting for ConnectionClosed");
            }
            Err(_) => {
                panic!(
                    "timed out waiting for ConnectionClosed for peer {:?}, session_id={}",
                    peer_id, session_id
                );
            }
        }
    }
}

async fn wait_for_peer_state(
    event_rx: &mut tokio::sync::broadcast::Receiver<ConnectionEvent>,
    peer_id: &ActrId,
    wanted_states: &[ConnectionState],
    timeout: Duration,
) -> (u64, ConnectionState) {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for peer {} to enter one of {:?}",
            peer_id,
            wanted_states
        );

        match tokio::time::timeout(remaining, event_rx.recv()).await {
            Ok(Ok(ConnectionEvent::StateChanged {
                peer_id: event_peer,
                session_id,
                state,
            })) if &event_peer == peer_id && wanted_states.contains(&state) => {
                return (session_id, state);
            }
            Ok(Ok(_)) => {}
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                tracing::warn!("Connection event receiver lagged by {} events", n);
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("connection event channel closed while waiting for peer state");
            }
            Err(_) => {
                panic!(
                    "timed out waiting for peer {} to enter one of {:?}",
                    peer_id, wanted_states
                );
            }
        }
    }
}

async fn expect_connection_recovering(
    handle: tokio::task::JoinHandle<actr_protocol::ActorResult<actr_framework::Bytes>>,
    label: &str,
) {
    match tokio::time::timeout(Duration::from_secs(3), handle).await {
        Ok(Ok(Err(err))) => {
            let msg = err.to_string();
            assert!(
                msg.contains("Connection recovering"),
                "{label} failed, but not with Connection recovering: {msg}"
            );
        }
        Ok(Ok(Ok(response))) => {
            panic!(
                "{label} unexpectedly succeeded with {} response bytes",
                response.len()
            );
        }
        Ok(Err(err)) => panic!("{label} task panicked: {err}"),
        Err(_) => panic!("{label} did not finish within the outer timeout"),
    }
}

async fn expect_request_eventually_ok(
    harness: &TestHarness,
    from_serial: u64,
    to_serial: u64,
    request_prefix: &str,
    total_timeout: Duration,
    attempt_timeout_ms: u32,
) -> actr_framework::Bytes {
    let deadline = tokio::time::Instant::now() + total_timeout;
    let mut attempt = 0;

    loop {
        attempt += 1;
        let request_id = format!("{request_prefix}_{attempt}");
        let handle =
            harness
                .peer(from_serial)
                .spawn_request(to_serial, &request_id, attempt_timeout_ms);

        let last_error = match tokio::time::timeout(
            Duration::from_millis(attempt_timeout_ms as u64) + Duration::from_secs(1),
            handle,
        )
        .await
        {
            Ok(Ok(Ok(response))) => return response,
            Ok(Ok(Err(err))) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("Connection recovering")
                        || msg.contains("Request timeout")
                        || msg.contains("Connection"),
                    "unexpected retry error while waiting for recovery: {msg}"
                );
                msg
            }
            Ok(Err(err)) => panic!("{request_prefix} retry task panicked: {err}"),
            Err(_) => format!("{request_prefix} attempt {attempt} timed out"),
        };

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "{request_prefix} did not succeed within {:?}; last error: {}",
                total_timeout, last_error
            );
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_signaling_reconnect(
    harness: &TestHarness,
    min_connections: u32,
    min_disconnections: u32,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let connections = harness.server.get_connection_count();
        let disconnections = harness.server.get_disconnection_count();
        if connections >= min_connections && disconnections >= min_disconnections {
            return;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for signaling reconnect counters: connections >= {}, disconnections >= {}; current connections={}, disconnections={}",
                min_connections, min_disconnections, connections, disconnections
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn force_reconnect_and_wait_for_new_session(
    harness: &TestHarness,
    from_serial: u64,
    to_serial: u64,
    old_session_id: u64,
    request_prefix: &str,
) -> u64 {
    let target_id = harness.peer(to_serial).id.clone();
    let results = process_network_event_batch(
        vec![
            NetworkEvent::ForceReconnect {
                reason: ReconnectReason::ManualReconnect,
            },
            wifi_event(1),
        ],
        harness.peer(from_serial).network_processor(),
    )
    .await;
    assert!(
        results.iter().all(|result| result.success),
        "force reconnect batch should succeed: {results:?}"
    );

    let response = expect_request_eventually_ok(
        harness,
        from_serial,
        to_serial,
        request_prefix,
        Duration::from_secs(20),
        2_000,
    )
    .await;
    assert!(!response.is_empty());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(session_id) = harness
            .peer(from_serial)
            .coordinator
            .get_peer_session_id(&target_id)
            .await
        {
            if session_id != old_session_id {
                return session_id;
            }
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for a new session after force reconnect"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Clone, Copy)]
enum LateOldSessionEvent {
    Failed,
    Closed,
    Ready,
}

impl LateOldSessionEvent {
    fn name(self) -> &'static str {
        match self {
            Self::Failed => "old_failed",
            Self::Closed => "old_closed",
            Self::Ready => "old_ready",
        }
    }

    fn reconnect_prefix(self) -> &'static str {
        match self {
            Self::Failed => "rc09_force_reconnect",
            Self::Closed => "rc10_force_reconnect",
            Self::Ready => "rc11_force_reconnect",
        }
    }

    fn verification_prefix(self) -> &'static str {
        match self {
            Self::Failed => "rc09_late_failed_new_session_still_usable",
            Self::Closed => "rc10_late_closed_new_session_still_usable",
            Self::Ready => "rc11_late_ready_new_session_still_usable",
        }
    }

    fn settle_delay(self) -> Duration {
        match self {
            Self::Closed => Duration::from_millis(300),
            Self::Failed | Self::Ready => Duration::from_millis(150),
        }
    }

    fn inject(self, harness: &TestHarness, peer_serial: u64, target_id: &ActrId, session_id: u64) {
        match self {
            Self::Failed => {
                harness
                    .peer(peer_serial)
                    .send_event(ConnectionEvent::StateChanged {
                        peer_id: target_id.clone(),
                        session_id,
                        state: ConnectionState::Failed,
                    });
            }
            Self::Closed => {
                harness
                    .peer(peer_serial)
                    .send_event(ConnectionEvent::DataChannelClosed {
                        peer_id: target_id.clone(),
                        session_id,
                        payload_type: PayloadType::RpcReliable,
                    });
                harness
                    .peer(peer_serial)
                    .send_event(ConnectionEvent::StateChanged {
                        peer_id: target_id.clone(),
                        session_id,
                        state: ConnectionState::Closed,
                    });
                harness
                    .peer(peer_serial)
                    .send_event(ConnectionEvent::ConnectionClosed {
                        peer_id: target_id.clone(),
                        session_id,
                    });
            }
            Self::Ready => {
                harness
                    .peer(peer_serial)
                    .send_event(ConnectionEvent::DataChannelOpened {
                        peer_id: target_id.clone(),
                        session_id,
                        payload_type: PayloadType::RpcReliable,
                    });
                harness
                    .peer(peer_serial)
                    .send_event(ConnectionEvent::IceRestartCompleted {
                        peer_id: target_id.clone(),
                        session_id,
                        success: true,
                    });
                harness
                    .peer(peer_serial)
                    .send_event(ConnectionEvent::StateChanged {
                        peer_id: target_id.clone(),
                        session_id,
                        state: ConnectionState::Connected,
                    });
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unreachable_peer_request_fails_bounded_and_clears_pending() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;

    let source = harness.peer(100);
    let request = source.spawn_request(999, "unreachable_peer_bounded_failure", 500);

    match tokio::time::timeout(Duration::from_secs(3), request).await {
        Ok(Ok(Err(err))) => {
            let message = err.to_string();
            assert!(
                message.contains("Request timeout")
                    || message.contains("timeout")
                    || message.contains("Connection")
                    || message.contains("Unavailable")
                    || message.contains("unavailable"),
                "unreachable peer should fail with an explainable bounded error, got: {message}"
            );
        }
        Ok(Ok(Ok(response))) => panic!(
            "unreachable peer request unexpectedly succeeded with {} bytes",
            response.len()
        ),
        Ok(Err(err)) => panic!("unreachable peer request task panicked: {err}"),
        Err(_) => panic!("unreachable peer request did not complete within bounded timeout"),
    }

    assert_eq!(
        source.pending_count().await,
        0,
        "unreachable peer request must clear pending state after bounded failure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_l2_signaling_unreachable_e2e_fails_bounded_without_pending_leak() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;

    harness.simulate_disconnect();
    for serial in [100, 200] {
        harness
            .peer(serial)
            .signaling_client
            .disconnect()
            .await
            .expect("test should explicitly disconnect signaling before server shutdown");
    }
    harness.server.shutdown().await;
    harness
        .vnet
        .as_ref()
        .expect("VNet harness should have network controls")
        .unblock_network();

    let results = tokio::time::timeout(
        Duration::from_secs(5),
        process_network_event_batch(
            vec![
                NetworkEvent::ForceReconnect {
                    reason: ReconnectReason::NetworkPathChanged,
                },
                wifi_event(1),
            ],
            harness.peer(100).network_processor(),
        ),
    )
    .await
    .expect("signaling-unreachable network event should be bounded");

    assert!(
        results.iter().any(|result| !result.success),
        "network event should report failure while signaling server is unreachable: {results:?}"
    );

    let request = harness
        .peer(100)
        .spawn_request(200, "l2_signaling_unreachable_request", 1_000);
    match tokio::time::timeout(Duration::from_secs(4), request).await {
        Ok(Ok(Err(err))) => {
            let msg = err.to_string();
            assert!(
                msg.contains("Connection")
                    || msg.contains("timeout")
                    || msg.contains("Unavailable")
                    || msg.contains("unavailable"),
                "unexpected signaling-unreachable request error: {msg}"
            );
        }
        Ok(Ok(Ok(response))) => panic!(
            "request unexpectedly succeeded while signaling is unreachable: {} bytes",
            response.len()
        ),
        Ok(Err(err)) => panic!("request task panicked: {err}"),
        Err(_) => panic!("request should not hang while signaling is unreachable"),
    }

    assert_eq!(
        harness.peer(100).pending_count().await,
        0,
        "signaling-unreachable request must not leak pending state"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_l2_data_channel_not_ready_during_initial_connect_fails_bounded_then_recovers() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    let _bg_tasks = [
        harness
            .peer(200)
            .start_echo_responder("l2_dc_not_ready_echo"),
        harness
            .peer(100)
            .start_response_receiver("l2_dc_not_ready_recv"),
    ];

    harness
        .vnet
        .as_ref()
        .expect("VNet harness should have network controls")
        .block_network();

    let blocked = harness
        .peer(100)
        .spawn_request(200, "l2_data_channel_not_ready_initial", 1_500);
    match tokio::time::timeout(Duration::from_secs(5), blocked).await {
        Ok(Ok(Err(err))) => {
            let msg = err.to_string();
            assert!(
                msg.contains("Request timeout")
                    || msg.contains("timeout")
                    || msg.contains("Connection")
                    || msg.contains("DataChannel")
                    || msg.contains("data channel"),
                "unexpected DataChannel-not-ready error: {msg}"
            );
        }
        Ok(Ok(Ok(response))) => panic!(
            "request unexpectedly succeeded while DataChannel cannot become ready: {} bytes",
            response.len()
        ),
        Ok(Err(err)) => panic!("blocked request task panicked: {err}"),
        Err(_) => panic!("DataChannel-not-ready request should fail within a bounded deadline"),
    }
    assert_eq!(
        harness.peer(100).pending_count().await,
        0,
        "DataChannel-not-ready failure must clear pending state"
    );

    harness
        .vnet
        .as_ref()
        .expect("VNet harness should have network controls")
        .unblock_network();

    let restore_results = process_network_event_batch(
        vec![
            NetworkEvent::ForceReconnect {
                reason: ReconnectReason::StaleConnectionSuspected,
            },
            wifi_event(2),
        ],
        harness.peer(100).network_processor(),
    )
    .await;
    assert!(
        restore_results.iter().all(|result| result.success),
        "mobile force reconnect should clean stale not-ready transport: {restore_results:?}"
    );

    let response = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "l2_data_channel_not_ready_recovered",
        Duration::from_secs(25),
        2_000,
    )
    .await;
    assert!(!response.is_empty());
}

// ==================== DataChannel close cleanup ====================

#[tokio::test]
async fn test_answerer_network_change_requests_offerer_ice_restart() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    tracing::info!("Step 1: Establishing connection with 100 as offerer and 200 as answerer");
    harness.connect(100, 200).await;
    harness.reset_counters();

    tracing::info!("Step 2: Processing network change on answerer peer 200");
    harness
        .peer(200)
        .network_processor()
        .process_network_type_changed(true, false)
        .await
        .expect("answerer network change should process successfully");

    let request_count = harness
        .wait_for_ice_restart_request_count(1, Duration::from_secs(5))
        .await;
    tracing::info!(
        "ICE restart request count after answerer network change: {}",
        request_count
    );

    let offer_count = harness
        .wait_for_ice_restart_count(1, Duration::from_secs(10))
        .await;
    tracing::info!(
        "ICE restart offer count after answerer request: {}",
        offer_count
    );

    let response = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "answerer_requested_restart_verify",
        Duration::from_secs(10),
        2_000,
    )
    .await;
    tracing::info!(
        "Connection remained usable after answerer-requested ICE restart: {} bytes",
        response.len()
    );
}

#[tokio::test]
async fn test_answerer_ice_restart_answer_does_not_unblock_before_connected() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    tracing::info!("Step 1: Establishing connection with 100 as offerer and 200 as answerer");
    harness.connect(100, 200).await;
    harness.reset_counters();

    let offerer_id = harness.peer(100).id.clone();
    let answerer = harness.peer(200);
    let mut answerer_events = answerer.subscribe_events();

    tracing::info!("Step 2: Keep signaling alive but block UDP before answerer recovery");
    harness
        .vnet
        .as_ref()
        .expect("test requires VNet")
        .block_network();

    tracing::info!("Step 3: Trigger answerer network recovery and wait until it answers restart");
    answerer
        .network_processor()
        .process_network_type_changed(true, false)
        .await
        .expect("answerer network change should process successfully");

    harness
        .wait_for_ice_restart_request_count(1, Duration::from_secs(5))
        .await;
    harness
        .wait_for_ice_restart_count(1, Duration::from_secs(10))
        .await;

    let (_session_id, state) = wait_for_peer_state(
        &mut answerer_events,
        &offerer_id,
        &[ConnectionState::Connecting],
        Duration::from_secs(5),
    )
    .await;
    tracing::info!(
        "Answerer observed restart negotiation state {:?} while UDP is still blocked",
        state
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    tracing::info!("Step 4: Sends must still fail fast until ICE reaches Connected");
    let early = answerer.spawn_request(100, "answerer-answer-before-connected", 500);
    expect_connection_recovering(early, "answerer send before ICE Connected").await;
}

#[tokio::test]
async fn test_stale_answerer_recovery_closes_old_session_on_network_restore() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    tracing::info!("Step 1: Establishing connection with 100 as offerer and 200 as answerer");
    harness.connect(100, 200).await;
    harness.reset_counters();

    let offerer_id = harness.peer(100).id.clone();
    let answerer = harness.peer(200);
    let mut answerer_events = answerer.subscribe_events();

    tracing::info!("Step 2: Mark answerer peer 200 as recovering for a long outage");
    answerer
        .coordinator
        .begin_network_recovery("test long answerer recovery")
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let status = answerer
        .coordinator
        .peer_recovery_status(&offerer_id)
        .await
        .expect("answerer should guard the existing offerer session");
    let session_id = status.session_id;
    assert!(
        answerer
            .coordinator
            .force_peer_recovery_started_at_for_test(
                &offerer_id,
                Instant::now() - Duration::from_secs(61),
            )
            .await,
        "test should be able to age the answerer recovery guard"
    );

    tracing::info!("Step 3: Network restore should close stale answerer session, not request ICE");
    answerer
        .coordinator
        .restart_network_recovery_connections()
        .await;

    wait_for_connection_closed(
        &mut answerer_events,
        &offerer_id,
        session_id,
        Duration::from_secs(3),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        answerer
            .coordinator
            .peer_recovery_status(&offerer_id)
            .await
            .is_none(),
        "stale answerer recovery should clear the coordinator guard"
    );
    assert_eq!(
        harness.server.get_ice_restart_request_count(),
        0,
        "stale answerer recovery should close locally instead of asking offerer to restart ICE"
    );
}

#[tokio::test]
async fn test_duplicate_network_recovery_same_session_is_coalesced() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    harness.connect(100, 200).await;

    let peer_100 = harness.peer(100);
    let target_id = harness.peer(200).id.clone();
    let mut event_rx = peer_100.subscribe_events();

    let first_targets = peer_100
        .coordinator
        .begin_network_recovery("first network event")
        .await;
    assert_eq!(
        first_targets,
        vec![target_id.clone()],
        "first network event should mark the active peer for recovery"
    );

    let first_session_id = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match event_rx.recv().await {
                Ok(ConnectionEvent::IceRestartStarted {
                    peer_id,
                    session_id,
                }) if peer_id == target_id => return session_id,
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("connection event channel closed")
                }
            }
        }
    })
    .await
    .expect("first recovery event should emit IceRestartStarted");

    let aged_started_at = Instant::now() - Duration::from_secs(3);
    assert!(
        peer_100
            .coordinator
            .force_peer_recovery_started_at_for_test(&target_id, aged_started_at)
            .await,
        "test should be able to age the recovery guard"
    );

    let second_targets = peer_100
        .coordinator
        .begin_network_recovery("second network event")
        .await;
    assert!(
        second_targets.is_empty(),
        "duplicate network event for the same session should be coalesced"
    );

    let status = peer_100
        .coordinator
        .peer_recovery_status(&target_id)
        .await
        .expect("recovery guard should remain active");
    assert_eq!(status.session_id, first_session_id);
    assert_eq!(status.reason, "first network event");
    assert!(
        status.elapsed() >= Duration::from_secs(3),
        "duplicate recovery should not refresh the guard timer"
    );

    let duplicate = tokio::time::timeout(Duration::from_millis(150), async {
        loop {
            match event_rx.recv().await {
                Ok(ConnectionEvent::IceRestartStarted { .. }) => return,
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    })
    .await;
    assert!(
        duplicate.is_err(),
        "duplicate network event should not emit another IceRestartStarted"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_late_old_session_events_do_not_affect_new_session() {
    init_tracing();

    for case in [
        LateOldSessionEvent::Failed,
        LateOldSessionEvent::Closed,
        LateOldSessionEvent::Ready,
    ] {
        let mut harness = TestHarness::new().await;
        harness.add_peer(100).await;
        harness.add_peer(200).await;
        harness.connect(100, 200).await;

        let target_id = harness.peer(200).id.clone();
        let old_session_id = harness
            .peer(100)
            .coordinator
            .get_peer_session_id(&target_id)
            .await
            .expect("initial session should exist");

        let new_session_id = force_reconnect_and_wait_for_new_session(
            &harness,
            100,
            200,
            old_session_id,
            case.reconnect_prefix(),
        )
        .await;

        case.inject(&harness, 100, &target_id, old_session_id);
        tokio::time::sleep(case.settle_delay()).await;

        assert_eq!(
            harness
                .peer(100)
                .coordinator
                .get_peer_session_id(&target_id)
                .await,
            Some(new_session_id),
            "{} late old event must not replace or close the active session",
            case.name()
        );

        if matches!(case, LateOldSessionEvent::Closed) {
            assert!(
                harness.peer(100).transport_manager.dest_count().await >= 1,
                "{} late old event must not remove the current DestTransport",
                case.name()
            );
        }

        let response = expect_request_eventually_ok(
            &harness,
            100,
            200,
            case.verification_prefix(),
            Duration::from_secs(10),
            2_000,
        )
        .await;
        assert!(!response.is_empty(), "{} should remain usable", case.name());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_both_peers_first_rpc_concurrently_is_bounded_then_mobile_restore_recovers() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    let _bg_tasks = [
        harness.peer(100).start_echo_responder("rc14_echo_100"),
        harness.peer(200).start_echo_responder("rc14_echo_200"),
        harness.peer(100).start_response_receiver("rc14_recv_100"),
        harness.peer(200).start_response_receiver("rc14_recv_200"),
    ];

    let request_100_to_200 =
        harness
            .peer(100)
            .spawn_request(200, "rc14_first_rpc_100_to_200", 5_000);
    let request_200_to_100 =
        harness
            .peer(200)
            .spawn_request(100, "rc14_first_rpc_200_to_100", 5_000);

    let (result_100_to_200, result_200_to_100) =
        tokio::time::timeout(Duration::from_secs(8), async {
            tokio::join!(request_100_to_200, request_200_to_100)
        })
        .await
        .expect("simultaneous first RPCs should be bounded");

    for (label, result) in [
        ("100 -> 200", result_100_to_200),
        ("200 -> 100", result_200_to_100),
    ] {
        match result.expect("first RPC task should not panic") {
            Ok(response) => assert!(
                !response.is_empty(),
                "{label} first RPC returned an empty response"
            ),
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("Request timeout")
                        || msg.contains("timeout")
                        || msg.contains("Connection")
                        || msg.contains("closed")
                        || msg.contains("DataChannel")
                        || msg.contains("data channel"),
                    "{label} first RPC should fail with a bounded transport error, got: {msg}"
                );
            }
        }
    }

    assert_eq!(harness.peer(100).pending_count().await, 0);
    assert_eq!(harness.peer(200).pending_count().await, 0);

    let restore_100 = process_network_event_batch(
        vec![
            NetworkEvent::ForceReconnect {
                reason: ReconnectReason::StaleConnectionSuspected,
            },
            wifi_event(1),
        ],
        harness.peer(100).network_processor(),
    );
    let restore_200 = process_network_event_batch(
        vec![
            NetworkEvent::ForceReconnect {
                reason: ReconnectReason::StaleConnectionSuspected,
            },
            wifi_event(1),
        ],
        harness.peer(200).network_processor(),
    );
    let (restore_100, restore_200) = tokio::join!(restore_100, restore_200);
    assert!(
        restore_100.iter().all(|result| result.success),
        "peer 100 restore should succeed after simultaneous first-send race: {restore_100:?}"
    );
    assert!(
        restore_200.iter().all(|result| result.success),
        "peer 200 restore should succeed after simultaneous first-send race: {restore_200:?}"
    );

    let response_100_to_200 = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "rc14_recovered_rpc_100_to_200",
        Duration::from_secs(15),
        2_000,
    )
    .await;
    let response_200_to_100 = expect_request_eventually_ok(
        &harness,
        200,
        100,
        "rc14_recovered_rpc_200_to_100",
        Duration::from_secs(15),
        2_000,
    )
    .await;
    assert!(!response_100_to_200.is_empty());
    assert!(!response_200_to_100.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_signaling_restore_wakes_existing_restart_without_duplicate_offer() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    tracing::info!("Step 1: Establish connection with peer 100 as offerer");
    harness.connect(100, 200).await;
    harness.reset_counters();

    let offerer = harness.peer(100);

    tracing::info!("Step 2: Start network recovery while offerer signaling is disconnected");
    offerer
        .signaling_client
        .disconnect()
        .await
        .expect("test should disconnect offerer signaling");
    offerer
        .coordinator
        .begin_network_recovery("NetworkLost")
        .await;
    offerer
        .coordinator
        .restart_network_recovery_connections()
        .await;

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        harness.ice_restart_count(),
        0,
        "ICE restart must not send an offer while signaling is disconnected"
    );

    tracing::info!("Step 3: Reconnect signaling and issue repeated recovery resumes");
    offerer
        .signaling_client
        .connect_once()
        .await
        .expect("test should reconnect offerer signaling");
    for _ in 0..5 {
        offerer
            .coordinator
            .restart_network_recovery_connections()
            .await;
    }

    harness
        .wait_for_ice_restart_count(1, Duration::from_secs(3))
        .await;
    tokio::time::sleep(Duration::from_millis(2500)).await;

    assert_eq!(
        harness.ice_restart_count(),
        1,
        "repeated recovery resumes should wake the existing restart task, not send duplicate offers"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_default_network_processor_mobile_restore_wakes_existing_restart_retry() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    tracing::info!("Step 1: Establish connection with peer 100 as offerer");
    harness.connect(100, 200).await;
    harness.reset_counters();

    let offerer_signaling = harness.peer(100).signaling_client.clone();
    let offerer_coordinator = harness.peer(100).coordinator.clone();
    let network_processor = harness.peer(100).network_processor();

    tracing::info!("Step 2: Start network recovery while offerer signaling is disconnected");
    offerer_signaling
        .disconnect()
        .await
        .expect("test should disconnect offerer signaling");
    offerer_coordinator
        .begin_network_recovery("NetworkLost")
        .await;
    offerer_coordinator
        .restart_network_recovery_connections()
        .await;

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        harness.ice_restart_count(),
        0,
        "ICE restart must not send an offer while signaling is disconnected"
    );

    tracing::info!("Step 3: Restore through the real DefaultNetworkEventProcessor");
    let events = vec![offline_event(1), online_event(2), wifi_event(3)];
    assert_eq!(
        select_network_recovery_action(&events),
        NetworkRecoveryAction::Restore
    );

    let started = Instant::now();
    let results = process_network_event_batch(events, network_processor).await;
    assert!(
        results.iter().all(|result| result.success),
        "DefaultNetworkEventProcessor restore should succeed: {results:?}"
    );

    harness
        .wait_for_ice_restart_count(1, Duration::from_secs(3))
        .await;
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "mobile restore should wake the existing ICE restart retry before the 5s backoff expires"
    );

    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        harness.ice_restart_count(),
        1,
        "DefaultNetworkEventProcessor restore should wake the existing restart task, not send duplicate offers"
    );
}

#[tokio::test]
async fn test_network_recovery_guard_times_out_after_6s_and_closes_transport() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    tracing::info!("Step 1: Establishing WebRTC connection 100 -> 200");
    harness.connect(100, 200).await;

    let target_id = harness.peer(200).id.clone();
    let dest = Dest::actor(target_id.clone());

    assert!(
        harness.peer(100).transport_manager.has_dest(&dest).await,
        "initial DestTransport should be cached before recovery guard timeout"
    );

    tracing::info!("Step 2: Mark the offerer peer as recovering via NetworkEvent guard");
    harness
        .peer(100)
        .coordinator
        .begin_network_recovery("test recovery timeout")
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let status = harness
        .peer(100)
        .coordinator
        .peer_recovery_status(&target_id)
        .await
        .expect("target should be guarded by network recovery");
    assert!(
        status.session_id > 0,
        "recovery guard should record the active WebRTC session id"
    );
    assert!(
        !status.is_timed_out(),
        "fresh network recovery guard should not be timed out"
    );

    tracing::info!("Step 3: Sends inside the 6s recovery window fail fast");
    let early = harness
        .peer(100)
        .spawn_request(200, "recovery-window-fast-fail", 30_000);
    expect_connection_recovering(early, "request inside recovery window").await;

    tracing::info!("Step 4: Age the guard beyond 6s and verify timeout cleanup");
    let expired_started_at = Instant::now() - Duration::from_secs(7);
    assert!(
        harness
            .peer(100)
            .coordinator
            .force_peer_recovery_started_at_for_test(&target_id, expired_started_at)
            .await,
        "test should be able to age the coordinator recovery guard"
    );

    let timed_out = harness
        .peer(100)
        .spawn_request(200, "recovery-window-timeout", 30_000);
    match tokio::time::timeout(Duration::from_secs(3), timed_out).await {
        Ok(Ok(Err(err))) => {
            let msg = err.to_string();
            assert!(
                msg.contains("Connection recovery timeout"),
                "expected recovery timeout error, got: {msg}"
            );
            assert!(
                msg.contains("timeout_ms=6000"),
                "timeout error should report the 6s recovery budget: {msg}"
            );
        }
        Ok(Ok(Ok(response))) => panic!(
            "timed-out recovery request unexpectedly succeeded with {} bytes",
            response.len()
        ),
        Ok(Err(err)) => panic!("timed-out recovery request task panicked: {err}"),
        Err(_) => panic!("timed-out recovery request did not fail fast"),
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !harness.peer(100).transport_manager.has_dest(&dest).await,
        "recovery timeout should close and remove the stale DestTransport"
    );
    assert!(
        harness
            .peer(100)
            .coordinator
            .peer_recovery_status(&target_id)
            .await
            .is_none(),
        "recovery timeout should clear the coordinator guard"
    );
}

#[tokio::test]
async fn test_connection_closed_clears_recovery_guard_when_transport_already_removed() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    let target_id = harness.peer(200).id.clone();
    let synthetic_session_id = 77;

    tracing::info!("Step 1: Simulate a recovery guard for a session with no cached transport");
    harness
        .peer(100)
        .send_event(ConnectionEvent::IceRestartStarted {
            peer_id: target_id.clone(),
            session_id: synthetic_session_id,
        });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let blocked = harness
        .peer(100)
        .spawn_request(200, "synthetic-recovery-blocks-send", 5_000);
    expect_connection_recovering(blocked, "request before close event").await;

    tracing::info!("Step 2: Close the same session after the transport was already removed");
    harness
        .peer(100)
        .send_event(ConnectionEvent::ConnectionClosed {
            peer_id: target_id,
            session_id: synthetic_session_id,
        });
    tokio::time::sleep(Duration::from_millis(200)).await;

    tracing::info!("Step 3: A later send should create a fresh transport instead of waiting 15s");
    harness
        .connect_with_timeout(100, 200, Duration::from_secs(5))
        .await;
}

#[tokio::test]
async fn test_data_channel_on_close_cleans_webrtc_transport() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    let target_id = harness.peer(200).id.clone();
    let dest = Dest::actor(target_id.clone());
    let mut event_rx = harness.peer(100).subscribe_events();

    tracing::info!("Step 1: Establishing WebRTC connection 100 -> 200");
    harness.connect(100, 200).await;

    let session_id = wait_for_data_channel_opened(
        &mut event_rx,
        &target_id,
        PayloadType::RpcReliable,
        Duration::from_secs(5),
    )
    .await;
    tracing::info!(
        "Observed initial RpcReliable DataChannel for peer {:?}, session_id={}",
        target_id,
        session_id
    );

    assert!(
        harness.peer(100).transport_manager.has_dest(&dest).await,
        "initial DestTransport should be cached before DataChannel close"
    );

    tracing::info!("Step 2: Closing RpcReliable DataChannel to trigger on_close cleanup");
    let closed_session_id = harness
        .peer(100)
        .coordinator
        .close_data_channel_for_test(&target_id, PayloadType::RpcReliable)
        .await
        .expect("active RpcReliable DataChannel should be closable");
    assert_eq!(
        closed_session_id, session_id,
        "test should close the same WebRTC session observed during connect"
    );

    let closed_payload_type = wait_for_data_channel_close_chain(
        &mut event_rx,
        &target_id,
        session_id,
        Duration::from_secs(10),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        !harness
            .peer(100)
            .coordinator
            .has_open_data_channel_for_test(&target_id)
            .await
            .expect("DataChannel state should be queryable after close"),
        "DataChannel on_close should leave no open DataChannel on the closed WebRTC session"
    );
    assert!(
        !harness.peer(100).transport_manager.has_dest(&dest).await,
        "DataChannel on_close should lead to ConnectionClosed and remove stale DestTransport"
    );

    tracing::info!(
        "DataChannel close chain cleaned transport for peer {:?}, session_id={}, first_closed_payload_type={:?}",
        target_id,
        session_id,
        closed_payload_type
    );
}

// ==================== Test 1: Two-peer disconnect/reconnect with NetworkEvent ====================

/// Test: disconnect two peers via VNet + signaling pause,
/// simulate a network-online snapshot (retry_failed_connections),
/// verify the connection is actually recovered by sending a message through the gate.
#[tokio::test]
async fn test_two_peer_disconnect_reconnect() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    tracing::info!("🔗 Step 1: Establishing connection 100 → 200...");
    harness.connect(100, 200).await;

    // Record baseline
    harness.reset_counters();

    tracing::info!("🔴 Step 2: Simulating full network outage (VNet + signaling)...");
    harness.simulate_disconnect();

    // Wait for ICE to detect disconnection
    tracing::info!("⏳ Waiting for ICE disconnection detection...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Verify ICE restart was triggered (even though it can't succeed — signaling is down)
    let post_disconnect_count = harness.ice_restart_count();
    tracing::info!(
        "📊 ICE restart count during outage: {}",
        post_disconnect_count
    );

    tracing::info!("🟢 Step 3: Restoring network (VNet + signaling)...");
    harness.simulate_reconnect();

    // Step 4: Simulate network-online snapshot -> triggers retry_failed_connections()
    // This is what happens in production when the platform layer detects network recovery
    tracing::info!("Step 4: Triggering network-online snapshot (retry_failed_connections)...");
    let start = tokio::time::Instant::now();
    harness.peer(100).retry_failed().await;

    // Wait for ICE restart to complete on the recovered network
    tracing::info!("⏳ Waiting for ICE restart to complete...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let recovery_time = start.elapsed();
    tracing::info!(
        "Recovery time (from network-online snapshot): {:?}",
        recovery_time
    );

    // Step 5: Verify connection is ACTUALLY recovered by sending a message
    tracing::info!("📤 Step 5: Verifying connection recovery via gate message...");
    let peer_a = harness.peer(100);
    let request_handle = peer_a.spawn_request(200, "reconnect_verify_1", 10000);

    match tokio::time::timeout(Duration::from_secs(10), request_handle).await {
        Ok(Ok(Ok(response))) => {
            tracing::info!(
                "✅ Connection recovered! Response: {} bytes, total recovery: {:?}",
                response.len(),
                start.elapsed()
            );
        }
        Ok(Ok(Err(e))) => {
            panic!("❌ Connection NOT recovered — request failed: {}", e);
        }
        Ok(Err(e)) => panic!("Request task panicked: {}", e),
        Err(_) => panic!("❌ Connection NOT recovered — request timed out after 10s"),
    }

    tracing::info!("✅ test_two_peer_disconnect_reconnect passed!");
}

#[tokio::test]
async fn test_lost_available_type_changed_batch_restores_webrtc_end_to_end() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    tracing::info!("🔗 Step 1: Establishing connection 100 → 200...");
    harness.connect(100, 200).await;

    harness.reset_counters();

    tracing::info!("🔴 Step 2: Simulating full network outage (VNet + signaling)...");
    harness.simulate_disconnect();
    tokio::time::sleep(Duration::from_secs(8)).await;

    tracing::info!("🟢 Step 3: Restoring network (VNet + signaling)...");
    harness.simulate_reconnect();

    let events = vec![offline_event(1), online_event(2), wifi_event(3)];
    assert_eq!(
        select_network_recovery_action(&events),
        NetworkRecoveryAction::Restore
    );

    tracing::info!("📱 Step 4: Processing Lost -> Available -> TypeChanged as one batch...");
    let results = process_network_event_batch(events, harness.peer(100).network_processor()).await;
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|result| result.success));

    let restart_count = harness
        .wait_for_ice_restart_count(1, Duration::from_secs(10))
        .await;
    tracing::info!(
        "📊 ICE restart count after batched restore: {}",
        restart_count
    );

    tracing::info!("📤 Step 5: Verifying WebRTC recovery via gate message...");
    let response = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "batched_network_restore_verify",
        Duration::from_secs(20),
        2_000,
    )
    .await;
    tracing::info!(
        "✅ WebRTC recovered after batched network events: {} bytes",
        response.len()
    );
}

#[tokio::test]
async fn test_cleanup_available_type_changed_batch_rebuilds_webrtc_end_to_end() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    tracing::info!("🔗 Step 1: Establishing connection 100 → 200...");
    harness.connect(100, 200).await;

    harness.reset_counters();

    let events = vec![
        NetworkEvent::ForceReconnect {
            reason: ReconnectReason::LongBackground,
        },
        online_event(1),
        wifi_event(2),
    ];
    assert_eq!(
        select_network_recovery_action(&events),
        NetworkRecoveryAction::ForceReconnect
    );

    tracing::info!(
        "📱 Step 2: Processing CleanupConnections -> Available -> TypeChanged as one batch..."
    );
    let results = process_network_event_batch(events, harness.peer(100).network_processor()).await;
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|result| result.success));

    tracing::info!("📤 Step 3: Verifying WebRTC can rebuild via gate message...");
    let request_handle =
        harness
            .peer(100)
            .spawn_request(200, "cleanup_batch_rebuild_verify", 15000);

    match tokio::time::timeout(Duration::from_secs(15), request_handle).await {
        Ok(Ok(Ok(response))) => {
            tracing::info!(
                "✅ WebRTC rebuilt after cleanup batch: {} bytes",
                response.len()
            );
        }
        Ok(Ok(Err(e))) => panic!("❌ WebRTC not rebuilt — request failed: {}", e),
        Ok(Err(e)) => panic!("Request task panicked: {}", e),
        Err(_) => panic!("❌ WebRTC not rebuilt — request timed out after 15s"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cleanup_during_ice_restart_prioritizes_cleanup_then_mobile_restore() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;
    harness.reset_counters();

    let target = harness.peer(200).id.clone();

    tracing::info!("Step 1: Block UDP and start mobile recovery/ICE restart");
    harness
        .vnet
        .as_ref()
        .expect("VNet harness should have network controls")
        .block_network();
    harness
        .peer(100)
        .coordinator
        .begin_network_recovery("rc12 cleanup during ice restart")
        .await;

    let restart_task = {
        let coordinator = harness.peer(100).coordinator.clone();
        tokio::spawn(async move {
            coordinator.restart_network_recovery_connections().await;
        })
    };

    tokio::time::sleep(Duration::from_millis(200)).await;

    tracing::info!("Step 2: Cleanup overlaps the in-flight restart window");
    let cleanup_result = tokio::time::timeout(
        Duration::from_secs(8),
        harness.peer(100).network_processor().cleanup_connections(),
    )
    .await
    .expect("cleanup during ICE restart should be bounded");
    assert!(
        cleanup_result.is_ok(),
        "cleanup during ICE restart should not fail: {:?}",
        cleanup_result
    );

    tokio::time::timeout(Duration::from_secs(2), restart_task)
        .await
        .expect("restart trigger task should not hang")
        .expect("restart trigger task should not panic");

    assert!(
        harness
            .peer(100)
            .coordinator
            .peer_recovery_status(&target)
            .await
            .is_none(),
        "cleanup should clear the old recovery guard once the old peer is gone"
    );
    assert_eq!(
        harness.peer(100).transport_manager.dest_count().await,
        0,
        "cleanup during ICE restart must not leave stale transport state"
    );

    tracing::info!("Step 3: Only a later mobile event restores connectivity");
    harness
        .vnet
        .as_ref()
        .expect("VNet harness should have network controls")
        .unblock_network();
    let results =
        process_network_event_batch(vec![wifi_event(1)], harness.peer(100).network_processor())
            .await;
    assert!(results.iter().all(|result| result.success));

    let response = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "rc12_cleanup_then_mobile_restore",
        Duration::from_secs(20),
        2_000,
    )
    .await;
    assert!(!response.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_signaling_reconnect_overlapping_cleanup_does_not_revive_old_transport() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;
    harness.reset_counters();

    tracing::info!("Step 1: Start a signaling reconnect while cleanup is about to run");
    harness
        .peer(100)
        .signaling_client
        .disconnect()
        .await
        .expect("test should disconnect signaling before reconnect race");

    let reconnect_task = {
        let signaling = harness.peer(100).signaling_client.clone();
        tokio::spawn(async move { signaling.connect_once().await })
    };

    tokio::time::sleep(Duration::from_millis(25)).await;

    tracing::info!("Step 2: Cleanup overlaps reconnect and must remove old WebRTC transport");
    let cleanup_result = tokio::time::timeout(
        Duration::from_secs(8),
        harness.peer(100).network_processor().cleanup_connections(),
    )
    .await
    .expect("cleanup overlapping signaling reconnect should be bounded");
    assert!(
        cleanup_result.is_ok(),
        "cleanup overlapping signaling reconnect should not fail: {:?}",
        cleanup_result
    );

    let _ = tokio::time::timeout(Duration::from_secs(5), reconnect_task)
        .await
        .expect("signaling reconnect task should not hang")
        .expect("signaling reconnect task should not panic");

    assert_eq!(
        harness.peer(100).transport_manager.dest_count().await,
        0,
        "overlapped signaling reconnect must not revive the old DestTransport"
    );
    assert_eq!(
        harness.peer(100).pending_count().await,
        0,
        "cleanup/reconnect overlap must leave no pending RPC state"
    );

    tracing::info!("Step 3: Explicit mobile restore establishes a fresh connection");
    let results =
        process_network_event_batch(vec![wifi_event(1)], harness.peer(100).network_processor())
            .await;
    assert!(results.iter().all(|result| result.success));

    let response = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "rc13_signaling_cleanup_then_restore",
        Duration::from_secs(20),
        2_000,
    )
    .await;
    assert!(!response.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_both_peers_simultaneous_mobile_restore_has_bounded_offer_count() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;
    harness.reset_counters();

    tracing::info!("Step 1: Full outage puts both peers into a mobile recovery scenario");
    harness.simulate_disconnect();
    tokio::time::sleep(Duration::from_secs(8)).await;
    harness.simulate_reconnect();

    let events = vec![offline_event(1), online_event(2), wifi_event(3)];
    assert_eq!(
        select_network_recovery_action(&events),
        NetworkRecoveryAction::Restore
    );

    tracing::info!("Step 2: Both mobile endpoints report restore concurrently");
    let peer_100_processor = harness.peer(100).network_processor();
    let peer_200_processor = harness.peer(200).network_processor();
    let peer_100_events = events.clone();
    let peer_200_events = events;

    let (results_100, results_200) = tokio::join!(
        process_network_event_batch(peer_100_events, peer_100_processor),
        process_network_event_batch(peer_200_events, peer_200_processor),
    );
    assert!(results_100.iter().all(|result| result.success));
    assert!(results_200.iter().all(|result| result.success));

    let restart_count = harness
        .wait_for_ice_restart_count(1, Duration::from_secs(10))
        .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let final_restart_count = harness.ice_restart_count();
    assert!(
        final_restart_count <= restart_count + 3,
        "simultaneous mobile restore should not create an offer storm: first={}, final={}",
        restart_count,
        final_restart_count
    );

    let response = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "rc15_simultaneous_mobile_restore",
        Duration::from_secs(20),
        2_000,
    )
    .await;
    assert!(!response.is_empty());
}

// ==================== Test 2: Offerer recovery latency ====================

/// Test: offerer-triggered recovery after network outage.
///
/// Topology: peer 200 → peer 100 (offerer, echo responder on 100)
///
/// Flow:
/// 1. Establish connection
/// 2. Full network outage (VNet + signaling) for 8s
///    → ICE Disconnected → auto-restart triggered on offerer (peer 100)
///    → First attempt fails (signaling blocked) → enters backoff
/// 3. Unblock network
/// 4. Offerer (peer 100) calls `retry_failed_connections()` (simulating network-online snapshot)
///    → `restart_ice()` but already inflight → no-op (dedup check)
/// 5. Measure time from unblock to message delivery
///
/// Key observation: `retry_failed()` on offerer is a no-op because
/// `do_ice_restart_inner` is already running. Recovery depends entirely on
/// the existing backoff timer expiring and retrying.
#[tokio::test]
async fn test_offerer_recovery_latency() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await; // offerer (first peer → offerer VNet)
    harness.add_peer(200).await; // answerer

    tracing::info!("🔗 Step 1: Establishing connection 200 → 100...");
    tracing::info!("   Peer 100 = offerer (echo responder)");
    tracing::info!("   Peer 200 = answerer (message sender)");
    harness.connect(200, 100).await;

    harness.reset_counters();

    // === Step 2: Short outage — connection stays in peers map ===
    tracing::info!("🔴 Step 2: Full network outage (VNet + signaling)...");
    harness.simulate_disconnect();

    // Wait for ICE Disconnected → auto-restart → first attempt fails → enters backoff
    tracing::info!("⏳ Waiting 8s for auto-restart to enter backoff...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let outage_restart_count = harness.ice_restart_count();
    tracing::info!(
        "📊 ICE restart attempts during outage: {} (all failed — signaling blocked)",
        outage_restart_count
    );

    // === Step 3: Unblock network — start measuring ===
    tracing::info!("🟢 Step 3: Restoring network — timer starts NOW");
    let recovery_start = std::time::Instant::now();
    harness.simulate_reconnect();

    // === Step 4: Offerer calls retry_failed (simulating network-online snapshot) ===
    tracing::info!("📱 Step 4: Offerer (100) calls retry_failed_connections()...");
    tracing::info!("   → restart_ice() will find restart already inflight → no-op");
    harness.peer(100).retry_failed().await;

    // === Step 5: Wait for recovery and send message ===
    tracing::info!("📤 Step 5: Sending message 200→100 to verify recovery...");
    let response = expect_request_eventually_ok(
        &harness,
        200,
        100,
        "offerer_recovery",
        Duration::from_secs(30),
        2_000,
    )
    .await;
    let e2e_latency = recovery_start.elapsed();
    tracing::info!(
        "✅ Offerer recovery succeeded! Response: {} bytes",
        response.len()
    );

    let total_restart_count = harness.ice_restart_count();

    tracing::info!("╔══════════════════════════════════════════════════════╗");
    tracing::info!("║   Offerer Recovery Summary                          ║");
    tracing::info!("╠══════════════════════════════════════════════════════╣");
    tracing::info!("║ E2E recovery latency: {:?}", e2e_latency);
    tracing::info!("║   (from network unblock to message response)");
    tracing::info!(
        "║ ICE restart attempts: {} during outage, {} total",
        outage_restart_count,
        total_restart_count
    );
    tracing::info!("║ Note: retry_failed() on offerer = no-op (restart");
    tracing::info!("║   already inflight, dedup check blocks it)");
    tracing::info!("╚══════════════════════════════════════════════════════╝");

    tracing::info!("✅ test_offerer_recovery_latency passed!");
}

// ==================== Test 3: Answerer recovery latency ====================

/// Test: answerer-triggered recovery after network outage (Plan A).
///
/// Topology: peer 200 → peer 100 (offerer, echo responder on 100)
///
/// Same setup as offerer test, BUT:
/// 4. **Answerer (peer 200)** calls `retry_failed_connections()` instead
///    → `restart_ice()` → `!is_offerer` → sends IceRestartRequest to Offerer
/// 5. Offerer receives IceRestartRequest → `notify_one()` wakes backoff
///    → immediate ICE restart retry → FASTER recovery
#[tokio::test]
async fn test_answerer_recovery_latency() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await; // offerer (first peer → offerer VNet)
    harness.add_peer(200).await; // answerer

    tracing::info!("🔗 Step 1: Establishing connection 200 → 100...");
    tracing::info!("   Peer 100 = offerer (echo responder)");
    tracing::info!("   Peer 200 = answerer (message sender, focus of this test)");
    harness.connect(200, 100).await;

    harness.reset_counters();

    // === Step 2: Short outage — connection stays in peers map ===
    tracing::info!("🔴 Step 2: Full network outage (VNet + signaling)...");
    harness.simulate_disconnect();

    tracing::info!("⏳ Waiting 8s for auto-restart to enter backoff...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let outage_restart_count = harness.ice_restart_count();
    tracing::info!(
        "📊 ICE restart attempts during outage: {} (all failed — signaling blocked)",
        outage_restart_count
    );

    // === Step 3: Unblock network — start measuring ===
    tracing::info!("🟢 Step 3: Restoring network — timer starts NOW");
    let recovery_start = std::time::Instant::now();
    harness.simulate_reconnect();

    // === Step 4: ANSWERER calls retry_failed (simulating network-online snapshot) ===
    tracing::info!("📱 Step 4: Answerer (200) calls retry_failed_connections()...");
    tracing::info!("   → restart_ice() → !is_offerer → sends IceRestartRequest to Offerer");
    harness.peer(200).retry_failed().await;

    // === Step 5: Wait for recovery and send message ===
    tracing::info!("📤 Step 5: Sending message 200→100 to verify recovery...");
    let response = expect_request_eventually_ok(
        &harness,
        200,
        100,
        "answerer_recovery",
        Duration::from_secs(30),
        2_000,
    )
    .await;
    let e2e_latency = recovery_start.elapsed();
    tracing::info!(
        "✅ Answerer (200) recovered! Response: {} bytes",
        response.len()
    );

    let total_restart_count = harness.ice_restart_count();

    tracing::info!("╔══════════════════════════════════════════════════════╗");
    tracing::info!("║   Answerer Recovery Summary                         ║");
    tracing::info!("╠══════════════════════════════════════════════════════╣");
    tracing::info!("║ E2E recovery latency: {:?}", e2e_latency);
    tracing::info!("║   (from network unblock to message response)");
    tracing::info!(
        "║ ICE restart attempts: {} during outage, {} total",
        outage_restart_count,
        total_restart_count
    );
    tracing::info!("║ Plan A: retry_failed() on answerer -> IceRestartRequest");
    tracing::info!("║   → Offerer wakes backoff → immediate retry");
    tracing::info!("╚══════════════════════════════════════════════════════╝");

    tracing::info!("✅ test_answerer_recovery_latency completed!");
}

// ==================== Repro: NetworkEvent returns before WebRTC is usable ====================

/// Reproduces the mobile 5G -> WiFi failure mode:
///
/// 1. The client-side NetworkEvent path returns success after it reconnects
///    signaling and starts/retries WebRTC recovery.
/// 2. That success does not mean the reliable DataChannel is usable yet.
/// 3. RPCs sent immediately after the event now fail fast with
///    `Connection recovering` before they enter pending_requests.
/// 4. A later retry succeeds once UDP/signaling are restored.
#[tokio::test]
#[ignore = "slow VNet recovery regression test"]
async fn repro_network_event_returns_before_webrtc_ready_causing_early_rpc_timeouts() {
    init_tracing();

    const CLIENT: u64 = 100;
    const SERVER: u64 = 200;

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(CLIENT).await;
    harness.add_peer(SERVER).await;

    let server_id = harness.peer(SERVER).id.clone();
    let mut client_events = harness.peer(CLIENT).subscribe_events();

    tracing::info!("Step 1: Establish client -> server WebRTC RPC path");
    harness.connect(CLIENT, SERVER).await;
    harness.reset_counters();

    tracing::info!(
        "Step 2: Simulate network switch window: UDP blocked, signaling forwarding paused"
    );
    harness
        .vnet
        .as_ref()
        .expect("test requires VNet")
        .block_network();
    harness.server.pause_forwarding();

    let (session_id, state) = wait_for_peer_state(
        &mut client_events,
        &server_id,
        &[ConnectionState::Disconnected, ConnectionState::Failed],
        Duration::from_secs(12),
    )
    .await;
    tracing::info!(
        "Client observed server session {} enter {:?}",
        session_id,
        state
    );

    tracing::info!("Step 3: Run the same NetworkEvent processor used by mobile bindings");
    assert!(
        harness.peer(CLIENT).signaling_client.is_connected(),
        "client signaling should be connected before NetworkEvent closes it"
    );
    let processor = std::sync::Arc::new(DefaultNetworkEventProcessor::new(
        harness.peer(CLIENT).signaling_client.clone(),
        Some(harness.peer(CLIENT).coordinator.clone()),
    ));
    let event_started = std::time::Instant::now();
    let results = process_network_event_batch(vec![wifi_event(1)], processor).await;
    assert!(
        results.iter().all(|result| result.success),
        "NetworkEvent path change should report success: {:?}",
        results
    );
    let event_elapsed = event_started.elapsed();
    tracing::info!(
        "network snapshot event returned in {:?}; ICE restart offers observed={}",
        event_elapsed,
        harness.ice_restart_count()
    );
    wait_for_signaling_reconnect(&harness, 1, 1, Duration::from_secs(2)).await;
    assert!(
        harness.peer(CLIENT).signaling_client.is_connected(),
        "client signaling should be reconnected after NetworkEvent returns"
    );
    assert!(
        event_elapsed < Duration::from_secs(3),
        "NetworkEvent returned too slowly for this repro: {:?}",
        event_elapsed
    );

    tracing::info!("Step 4: Send two RPCs immediately after NetworkEvent returns");
    let client = harness.peer(CLIENT);
    let early_1 = client.spawn_request(SERVER, "network-event-early-timeout-1", 500);
    let early_2 = client.spawn_request(SERVER, "network-event-early-timeout-2", 800);

    expect_connection_recovering(early_1, "first immediate RPC").await;
    expect_connection_recovering(early_2, "second immediate RPC").await;

    tracing::info!("Step 5: Finish network recovery; a later retry should succeed");
    harness.simulate_reconnect();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut attempt = 0;
    loop {
        attempt += 1;
        let request_id = format!("network-event-late-success-{attempt}");
        let late_success = harness
            .peer(CLIENT)
            .spawn_request(SERVER, &request_id, 2_000);

        match tokio::time::timeout(Duration::from_secs(3), late_success).await {
            Ok(Ok(Ok(response))) => {
                tracing::info!(
                    "Retry after delayed recovery received {} bytes on attempt {}",
                    response.len(),
                    attempt
                );
                assert_eq!(&response[..], b"pong");
                break;
            }
            Ok(Ok(Err(err))) => {
                let msg = err.to_string();
                if tokio::time::Instant::now() >= deadline {
                    panic!("retry after recovery should eventually succeed, last error: {msg}");
                }
                assert!(
                    msg.contains("Connection recovering")
                        || msg.contains("Request timeout")
                        || msg.contains("Connection"),
                    "unexpected retry error while waiting for recovery: {msg}"
                );
            }
            Ok(Err(err)) => panic!("retry task panicked: {err}"),
            Err(_) if tokio::time::Instant::now() < deadline => {}
            Err(_) => panic!("retry did not complete after network recovery"),
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// ==================== Regression: foreground cleanup overlaps a new send ====================

/// Exercises the slow-send relative timeline from the mobile log:
///
/// - T+0ms: app returns foreground.
/// - T+403ms: cleanup starts and closes the old client-side WebRTC peers.
/// - T+1972ms: user sends a new RPC.
/// - T+2405ms: cleanup disconnects/rebuilds the signaling WebSocket.
///
/// Before the cleanup barrier, the fresh WebRTC negotiation could be
/// interrupted after SDP exchange but before usable ICE candidate exchange,
/// then wait for the 10s connection-establishment timeout plus the 5s factory
/// retry backoff. With the fix, the send waits for cleanup and avoids that slow
/// retry path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "log-timeline foreground cleanup/request-overlap regression"]
async fn regression_log_timeline_foreground_cleanup_waits_before_new_send() {
    init_tracing();

    const SERVER: u64 = 100;
    const CLIENT: u64 = 200;
    const CLEANUP_START_MS: u64 = 403;
    const USER_SEND_MS: u64 = 1_972;
    const CLEANUP_SIGNALING_DISCONNECT_MS: u64 = 2_405;

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(SERVER).await;
    harness.add_peer(CLIENT).await;

    tracing::info!("Step 1: Establish the pre-background client -> server path");
    harness
        .connect_with_timeout(CLIENT, SERVER, Duration::from_secs(30))
        .await;
    harness.reset_counters();

    tracing::info!("Step 2: Start the foreground cleanup timeline");
    assert!(
        harness.peer(CLIENT).signaling_client.is_connected(),
        "client signaling should be connected before foreground cleanup"
    );

    let foreground_started = tokio::time::Instant::now();
    let cleanup_coordinator = harness.peer(CLIENT).coordinator.clone();
    let cleanup_signaling = harness.peer(CLIENT).signaling_client.clone();
    let cleanup_task = tokio::spawn(async move {
        tokio::time::sleep_until(foreground_started + Duration::from_millis(CLEANUP_START_MS))
            .await;
        let _cleanup_guard = cleanup_coordinator.cleanup_guard();

        cleanup_coordinator.clear_pending_restarts().await;
        cleanup_coordinator
            .close_all_peers()
            .await
            .map_err(|err| err.to_string())?;

        tokio::time::sleep_until(
            foreground_started + Duration::from_millis(CLEANUP_SIGNALING_DISCONNECT_MS),
        )
        .await;
        if cleanup_signaling.is_connected() {
            cleanup_signaling
                .disconnect()
                .await
                .map_err(|err| err.to_string())?;
        }
        cleanup_signaling
            .connect_once()
            .await
            .map_err(|err| err.to_string())?;

        Ok::<(), String>(())
    });

    tokio::time::sleep_until(foreground_started + Duration::from_millis(USER_SEND_MS)).await;

    tracing::info!("Step 3: User sends according to the log timeline");
    harness.server.drop_next_ice_candidates_for(
        2,
        Duration::from_millis(CLEANUP_SIGNALING_DISCONNECT_MS - USER_SEND_MS),
    );
    let send_started = Instant::now();
    let request =
        harness
            .peer(CLIENT)
            .spawn_request(SERVER, "log-timeline-foreground-overlap-send", 30_000);

    tokio::time::sleep_until(
        foreground_started + Duration::from_millis(CLEANUP_SIGNALING_DISCONNECT_MS),
    )
    .await;

    let response = match tokio::time::timeout(Duration::from_secs(35), request).await {
        Ok(Ok(Ok(response))) => response,
        Ok(Ok(Err(err))) => panic!("overlapped foreground send failed: {err}"),
        Ok(Err(err)) => panic!("overlapped foreground send task panicked: {err}"),
        Err(_) => panic!("overlapped foreground send did not complete within 35s"),
    };
    let send_elapsed = send_started.elapsed();

    let cleanup_result = cleanup_task.await.expect("cleanup task panicked");
    assert!(
        cleanup_result.is_ok(),
        "foreground cleanup should complete: {:?}",
        cleanup_result
    );

    tracing::info!(
        "Log-timeline foreground-overlap send completed in {:?} with {} response bytes",
        send_elapsed,
        response.len()
    );

    assert_eq!(&response[..], b"pong");
    assert!(
        send_elapsed < Duration::from_secs(8),
        "send should wait for cleanup instead of hitting the 10s timeout + 5s retry path, got {:?}",
        send_elapsed
    );
}
