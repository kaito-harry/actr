//! Mobile full-disconnect recovery tests.
//!
//! The mobile peer is intentionally the WebRTC answerer. A short semantic
//! recovery window should keep the old WebRTC session and recover via ICE
//! restart; a stale recovery window should close the old session and let the
//! next RPC rebuild it. Both cases keep the old WebSocket half-open until the
//! restore path probes and rebuilds signaling.

use std::time::{Duration, Instant};

use actr_hyper::lifecycle::{
    NetworkEvent, NetworkRecoveryAction, process_network_event_batch,
    select_network_recovery_action,
};
use actr_hyper::test_support::TestHarness;
use actr_protocol::ActrId;

const MOBILE: u64 = 200;
const SERVER: u64 = 100;
const ICE_RESTART_SEMANTIC_ELAPSED: Duration = Duration::from_secs(15);
const REBUILD_SEMANTIC_ELAPSED: Duration = Duration::from_secs(65);
const HALF_OPEN_SETTLE: Duration = Duration::from_millis(100);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
}

async fn setup_mobile_server_harness() -> TestHarness {
    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(MOBILE).await;
    harness.add_peer(SERVER).await;
    harness.connect(MOBILE, SERVER).await;
    harness
}

fn server_id(harness: &TestHarness) -> ActrId {
    harness.peer(SERVER).id.clone()
}

async fn mobile_peer_session_id(harness: &TestHarness) -> Option<u64> {
    harness
        .peer(MOBILE)
        .coordinator
        .peer_session_id_for_test(&server_id(harness))
        .await
}

fn assert_event_batch_action(
    label: &str,
    events: &[NetworkEvent],
    expected: NetworkRecoveryAction,
) {
    let action = select_network_recovery_action(events);
    assert_eq!(
        action, expected,
        "{label} should reconcile to {:?}",
        expected
    );
}

async fn process_mobile_events(
    harness: &TestHarness,
    label: &str,
    events: Vec<NetworkEvent>,
    expected: NetworkRecoveryAction,
) {
    assert_event_batch_action(label, &events, expected);
    let results =
        process_network_event_batch(events, harness.peer(MOBILE).network_processor()).await;
    assert!(
        results.iter().all(|result| result.success),
        "{label} network event processing failed: {:?}",
        results
    );
}

async fn expect_mobile_request_ok(harness: &TestHarness, request_id: &str) -> usize {
    let started = Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let handle = harness.peer(MOBILE).spawn_request(SERVER, request_id, 1000);
        match handle.await {
            Ok(Ok(response)) => {
                tracing::info!(
                    request_id,
                    attempts,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    response_len = response.len(),
                    "mobile request recovered"
                );
                return response.len();
            }
            Ok(Err(err)) => {
                if started.elapsed() >= RECOVERY_TIMEOUT {
                    panic!(
                        "{request_id} did not recover within {:?}: {err}",
                        RECOVERY_TIMEOUT
                    );
                }
                tracing::info!(
                    request_id,
                    attempts,
                    error = %err,
                    "mobile request not ready yet; retrying"
                );
            }
            Err(err) => {
                if started.elapsed() >= RECOVERY_TIMEOUT {
                    panic!("{request_id} task failed and recovery timeout elapsed: {err}",);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_mobile_recovery_cleared(harness: &TestHarness, label: &str) {
    let peer_id = server_id(harness);
    let deadline = tokio::time::Instant::now() + RECOVERY_TIMEOUT;

    loop {
        if harness
            .peer(MOBILE)
            .coordinator
            .peer_recovery_status(&peer_id)
            .await
            .is_none()
        {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "{label} did not clear mobile recovery guard within {:?}",
            RECOVERY_TIMEOUT
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn enter_half_open_recovery_window(
    harness: &TestHarness,
    label: &str,
    semantic_elapsed: Duration,
) {
    let vnet = harness
        .vnet
        .as_ref()
        .expect("half-open recovery test requires VNet");
    let mobile = harness.peer(MOBILE);
    let peer_id = server_id(harness);

    vnet.block_network();
    harness.server.pause_forwarding();
    harness.server.blackhole_websocket_io();

    let targets = mobile.coordinator.begin_network_recovery(label).await;
    assert!(
        targets.iter().any(|target| target == &peer_id),
        "{label} should mark the server peer as recovering"
    );
    mobile.coordinator.clear_pending_restarts().await;

    let started_at = Instant::now()
        .checked_sub(semantic_elapsed)
        .expect("semantic elapsed should be representable");
    assert!(
        mobile
            .coordinator
            .force_peer_recovery_started_at_for_test(&peer_id, started_at)
            .await,
        "{label} should be able to age the recovery guard"
    );

    tokio::time::sleep(HALF_OPEN_SETTLE).await;
}

async fn restore_half_open_and_process_network_available(
    harness: &TestHarness,
    label: &str,
) -> Duration {
    let started = Instant::now();
    let connection_count_before_restore = harness.server.get_connection_count();

    harness.simulate_reconnect();

    process_mobile_events(
        harness,
        label,
        vec![
            NetworkEvent::Available,
            NetworkEvent::TypeChanged {
                is_wifi: true,
                is_cellular: false,
            },
        ],
        NetworkRecoveryAction::Restore,
    )
    .await;

    let connection_count_after_restore = harness.server.get_connection_count();
    assert!(
        connection_count_after_restore > connection_count_before_restore,
        "{label} should rebuild signaling after probing the half-open WebSocket"
    );

    let elapsed = started.elapsed();
    tracing::info!(
        case = label,
        elapsed_ms = elapsed.as_millis() as u64,
        connection_count_before_restore,
        connection_count_after_restore,
        "mobile network restore processed"
    );

    harness.server.restore_websocket_io();
    elapsed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mobile_half_open_15s_semantics_recovers_with_ice_restart() {
    init_tracing();

    let harness = setup_mobile_server_harness().await;
    harness.reset_counters();

    let old_session = mobile_peer_session_id(&harness)
        .await
        .expect("mobile should have an initial WebRTC session");

    enter_half_open_recovery_window(
        &harness,
        "half_open_15s_ice_restart",
        ICE_RESTART_SEMANTIC_ELAPSED,
    )
    .await;
    restore_half_open_and_process_network_available(&harness, "half_open_15s_ice_restart").await;

    harness
        .wait_for_ice_restart_request_count(1, Duration::from_secs(3))
        .await;

    wait_for_mobile_recovery_cleared(&harness, "half_open_15s_ice_restart").await;

    let response_len = expect_mobile_request_ok(&harness, "half_open_15s_ice_restart_verify").await;
    let recovered_session = mobile_peer_session_id(&harness)
        .await
        .expect("mobile should still have a WebRTC session after ICE restart");

    assert_eq!(
        recovered_session, old_session,
        "15s semantic recovery should keep the existing WebRTC session"
    );
    assert!(response_len > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mobile_half_open_65s_semantics_rebuilds_webrtc() {
    init_tracing();

    let harness = setup_mobile_server_harness().await;
    harness.reset_counters();

    let old_session = mobile_peer_session_id(&harness)
        .await
        .expect("mobile should have an initial WebRTC session");

    enter_half_open_recovery_window(&harness, "half_open_65s_rebuild", REBUILD_SEMANTIC_ELAPSED)
        .await;
    restore_half_open_and_process_network_available(&harness, "half_open_65s_rebuild").await;

    assert_eq!(
        mobile_peer_session_id(&harness).await,
        None,
        "65s semantic recovery should close the stale answerer session before new sends"
    );
    assert_eq!(
        harness.server.get_ice_restart_request_count(),
        0,
        "stale answerer recovery should rebuild instead of requesting ICE restart"
    );

    let response_len = expect_mobile_request_ok(&harness, "half_open_65s_rebuild_verify").await;
    let rebuilt_session = mobile_peer_session_id(&harness)
        .await
        .expect("mobile should rebuild a WebRTC session after the next RPC");

    assert_ne!(
        rebuilt_session, old_session,
        "65s semantic recovery should rebuild with a new WebRTC session"
    );
    assert!(response_len > 0);
}
