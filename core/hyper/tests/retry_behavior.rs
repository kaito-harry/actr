//! Integration tests for `call` / `tell` / `send_data_stream` retry behavior.
//!
//! These tests exercise the three retry layers in the outbound send path:
//!
//! ```text
//! Layer 1  preflight_send()           — recovery/closing guard blocks
//! Layer 2  send_with_retry()          — exponential backoff on Transient errors
//! Layer 3  DestTransport::send()      — conn_watcher event-driven reconnection wait
//! ```
//!
//! ## Key invariants under test
//!
//! 1. Only `ErrorKind::Transient` errors trigger `send_with_retry` retries.
//! 2. `call` wraps send+wait in an envelope timeout (default 30s).
//! 3. `tell` has no envelope timeout; retry policy alone limits attempts.
//! 4. `send_data_stream` does NOT use `send_with_retry` at all.
//! 5. `ConnectionClosed` events clean up `pending_requests` — the response
//!    channel is severed even if `send_with_retry` is still running.
//! 6. `close_transport_if_webrtc_session` is session-guarded: stale close
//!    events (mismatched identity) do NOT kill pending requests on a new wire.
//! 7. `get_or_create_transport` deduplicates concurrent callers via
//!    `Either<Notify, DestTransport>` — one creator, many waiters.

use actr_framework::Bytes;
use actr_hyper::test_support::TestHarness;
use actr_hyper::test_support::{
    expect_request_eventually_ok, wait_for_data_channel_close_chain, wait_for_data_channel_opened,
    wait_for_peer_state,
};
use actr_hyper::transport::{ConnectionEvent, ConnectionState};
use actr_hyper::wire::webrtc::{HookCallback, HookEvent};
use actr_protocol::{ActrError, PayloadType};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Initialize tracing for test output.
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
}

fn recording_hook_callback() -> (
    HookCallback,
    tokio::sync::mpsc::UnboundedReceiver<HookEvent>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let hook: HookCallback = Arc::new(move |event| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(event);
        })
    });
    (hook, rx)
}

async fn wait_for_webrtc_disconnected_hook(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<HookEvent>,
    peer_id: &actr_protocol::ActrId,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for WebRtcDisconnected hook for peer {peer_id}"
        );

        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(HookEvent::WebRtcDisconnected { peer_id: got, .. })) if &got == peer_id => {
                return;
            }
            Ok(Some(other)) => {
                tracing::debug!("ignoring hook while waiting for disconnected: {other:?}");
            }
            Ok(None) => panic!("hook channel closed while waiting for disconnected"),
            Err(_) => panic!("timed out waiting for WebRtcDisconnected hook for peer {peer_id}"),
        }
    }
}

async fn wait_for_webrtc_connected_hook(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<HookEvent>,
    peer_id: &actr_protocol::ActrId,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for WebRtcConnected hook for peer {peer_id}"
        );

        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(HookEvent::WebRtcConnected { peer_id: got, .. })) if &got == peer_id => {
                return;
            }
            Ok(Some(other)) => {
                tracing::debug!("ignoring hook while waiting for connected: {other:?}");
            }
            Ok(None) => panic!("hook channel closed while waiting for connected"),
            Err(_) => panic!("timed out waiting for WebRtcConnected hook for peer {peer_id}"),
        }
    }
}

async fn assert_no_webrtc_connected_hook(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<HookEvent>,
    peer_id: &actr_protocol::ActrId,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;

    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(HookEvent::WebRtcConnected { peer_id: got, .. })) if &got == peer_id => {
                panic!("stale recovery event unexpectedly emitted WebRtcConnected for {peer_id}");
            }
            Ok(Some(other)) => {
                tracing::debug!("ignoring hook while asserting no connected: {other:?}");
            }
            Ok(None) | Err(_) => return,
        }
    }
}

async fn expect_connection_not_ready(
    handle: tokio::task::JoinHandle<actr_protocol::ActorResult<Bytes>>,
    label: &str,
) {
    match tokio::time::timeout(Duration::from_secs(3), handle).await {
        Ok(Ok(Err(ActrError::ConnectionNotReady(info)))) => {
            assert!(
                info.retry_after_ms.is_some(),
                "{label} should include retry_after_ms"
            );
        }
        Ok(Ok(Err(other))) => panic!("{label} failed with unexpected error: {other}"),
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

// ─── Scenario 1 ─────────────────────────────────────────────────────────────
// call recovers after a VNet outage: block UDP → wait for ICE disconnect →
// issue call (blocked by preflight_send during recovery) → unblock UDP →
// call eventually succeeds.
//
// This tests the full recovery chain:
//   preflight_send blocks → recovery clears → send_with_retry / conn_watcher
//   delivers on the rebuilt connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_succeeds_after_wirepool_reconnect() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    // Subscribe BEFORE connect.
    let mut event_rx = harness.peer(100).subscribe_events();
    let target_id = harness.peer(200).id.clone();

    harness.connect(100, 200).await;
    harness.reset_counters();

    // Wait for the initial connection to be fully established.
    let _initial_session = wait_for_data_channel_opened(
        &mut event_rx,
        &target_id,
        PayloadType::RpcReliable,
        Duration::from_secs(5),
    )
    .await;

    // 1. Block UDP only — signaling stays up so ICE restart can negotiate.
    harness
        .vnet
        .as_ref()
        .expect("test requires VNet")
        .block_network();

    // Wait for ICE to detect the failure.
    let (_session_id, _state) = wait_for_peer_state(
        &mut event_rx,
        &target_id,
        &[ConnectionState::Disconnected, ConnectionState::Failed],
        Duration::from_secs(12),
    )
    .await;

    // 2. Unblock UDP — the in-flight ICE restart should now complete.
    harness.vnet.as_ref().unwrap().unblock_network();

    // 3. Issue a call — it may be temporarily blocked by preflight_send
    //    during the recovery window, but should eventually succeed.
    let response = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "wirepool_reconnect",
        Duration::from_secs(20),
        5_000,
    )
    .await;

    tracing::info!(
        "call succeeded after WirePool reconnection: {} bytes",
        response.len()
    );
}

// ─── Scenario 2 ─────────────────────────────────────────────────────────────
// When the network is fully down (UDP + signaling), calls fail. Once the
// network recovers and retry_failed is triggered, a new call succeeds.
//
// This verifies the "cleanup then rebuild" path:
//   full outage → transport torn down → network restore → retry_failed →
//   new transport created → call succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_fails_during_outage_then_succeeds_after_recovery() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;
    harness.reset_counters();

    // Full outage.
    harness.simulate_disconnect();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Call should fail (network down).
    let handle = harness.peer(100).spawn_request(200, "outage_call", 3_000);
    match tokio::time::timeout(Duration::from_secs(5), handle).await {
        Ok(Ok(Err(_))) => {
            tracing::info!("call correctly failed during outage");
        }
        Ok(Ok(Ok(_))) => {
            // Might succeed if the DataChannel buffer hasn't drained yet —
            // that's fine, we mainly test the recovery path below.
            tracing::info!("call unexpectedly succeeded (buffered data)");
        }
        Ok(Err(e)) => panic!("task panicked: {}", e),
        Err(_) => panic!("call timed out without error"),
    }

    // Restore network.
    harness.simulate_reconnect();
    harness.peer(100).retry_failed().await;

    // After recovery, call should succeed.
    let response = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "post_recovery_call",
        Duration::from_secs(30),
        5_000,
    )
    .await;

    tracing::info!("call succeeded after recovery: {} bytes", response.len());
}

// ─── Scenario 3 ─────────────────────────────────────────────────────────────
// Concurrent calls to the same target share a single DestTransport — the
// `get_or_create_transport` Either state machine deduplicates connection
// creation.
//
// Verify: dest_count == 1, both calls succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_calls_share_dest_transport() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    // Start echo responder and response receiver so calls can complete.
    let peer_100 = harness.peer(100);
    let peer_200 = harness.peer(200);

    let echo_handle = peer_100.start_echo_responder("echo_100");
    let recv_handle_200 = peer_200.start_response_receiver("recv_200");
    let _ = (echo_handle, recv_handle_200);

    let target_id = peer_100.id.clone();

    // Issue two concurrent calls from peer_200 → peer_100.
    // Both trigger lazy connection creation.
    let gate_200 = peer_200.gate.clone();
    let handle_1 = tokio::spawn({
        let gate = gate_200.clone();
        let target = target_id.clone();
        async move {
            gate.send_request(
                &target,
                actr_protocol::RpcEnvelope {
                    request_id: "concurrent_1".into(),
                    route_key: "test.method".into(),
                    payload: Some(bytes::Bytes::from("req1")),
                    direction: Some(actr_protocol::Direction::Request as i32),
                    timeout_ms: 15_000,
                    ..Default::default()
                },
            )
            .await
        }
    });
    let handle_2 = tokio::spawn({
        let gate = gate_200.clone();
        let target = target_id.clone();
        async move {
            gate.send_request(
                &target,
                actr_protocol::RpcEnvelope {
                    request_id: "concurrent_2".into(),
                    route_key: "test.method".into(),
                    payload: Some(bytes::Bytes::from("req2")),
                    direction: Some(actr_protocol::Direction::Request as i32),
                    timeout_ms: 15_000,
                    ..Default::default()
                },
            )
            .await
        }
    });

    let result_1 = tokio::time::timeout(Duration::from_secs(15), handle_1).await;
    let result_2 = tokio::time::timeout(Duration::from_secs(15), handle_2).await;

    let r1 = result_1
        .expect("call_1 should complete")
        .expect("call_1 no panic");
    let r2 = result_2
        .expect("call_2 should complete")
        .expect("call_2 no panic");
    assert!(r1.is_ok(), "call_1 should succeed, got: {:?}", r1);
    assert!(r2.is_ok(), "call_2 should succeed, got: {:?}", r2);

    // Only one DestTransport should have been created.
    let dest_count = peer_200.transport_manager.dest_count().await;
    assert_eq!(
        dest_count, 1,
        "only one DestTransport should exist for the target, got {}",
        dest_count
    );
}

// ─── Scenario 4 ─────────────────────────────────────────────────────────────
// send_with_retry's backoff sleep runs concurrently with the ConnectionClosed
// cleanup path. When the close chain runs, pending_requests are cleaned up and
// the oneshot fires — the caller gets the error promptly without waiting for
// the full envelope timeout.
//
// The key correctness property: no deadlock between the event listener and
// send_with_retry, and the caller gets an error promptly after cleanup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_returns_promptly_after_connection_closed_cleanup() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    // Subscribe BEFORE connect.
    let mut event_rx = harness.peer(100).subscribe_events();
    let target_id = harness.peer(200).id.clone();

    harness.connect(100, 200).await;

    let session_id = wait_for_data_channel_opened(
        &mut event_rx,
        &target_id,
        PayloadType::RpcReliable,
        Duration::from_secs(5),
    )
    .await;

    // Issue a call, then immediately kill the DataChannel.
    let peer_100 = harness.peer(100);
    let request_handle = peer_100.spawn_request(200, "cleanup_race_test", 30_000);

    // Close the DataChannel — triggers the cleanup path.
    peer_100
        .coordinator
        .close_data_channel_for_test(&target_id, PayloadType::RpcReliable)
        .await
        .expect("should close data channel");

    // Wait for close chain to complete.
    wait_for_data_channel_close_chain(
        &mut event_rx,
        &target_id,
        session_id,
        Duration::from_secs(10),
    )
    .await;

    // The call should return within 5s (either success because the data was
    // already in the SCTP buffer before the channel closed, or error because
    // the cleanup fired). Either way it should not hang for 30s.
    let start = Instant::now();
    match tokio::time::timeout(Duration::from_secs(8), request_handle).await {
        Ok(Ok(Err(e))) => {
            let elapsed = start.elapsed();
            let msg = e.to_string();
            // Error is expected — cleanup severed the response channel.
            // Depending on timing, cleanup may surface as a connection-style
            // error or as NoRoute once all candidates are exhausted.
            let cleanup_error = matches!(
                &e,
                ActrError::NotFound(msg) if msg.contains("all transport candidates exhausted")
            ) || msg.contains("Connection")
                || msg.contains("connection")
                || msg.contains("DataChannel closed")
                || msg.contains("Data channel")
                || msg.contains("Unavailable")
                || msg.contains("recovering");

            assert!(
                cleanup_error,
                "expected prompt cleanup/connection error, got: {}",
                msg
            );
            assert!(
                elapsed < Duration::from_secs(5),
                "call should return promptly after cleanup, took {:?}",
                elapsed
            );
            tracing::info!("call returned promptly in {:?} with: {}", elapsed, msg);
        }
        Ok(Ok(Ok(_))) => {
            // Data was already sent before the channel closed — the call
            // might succeed if the response arrives via a fallback path.
            // This is a valid outcome; the important thing is it didn't hang.
            tracing::info!("call succeeded (data was buffered before close)");
        }
        Ok(Err(e)) => panic!("task panicked: {}", e),
        Err(_) => {
            panic!("call did not return within 8s — cleanup may not have fired");
        }
    }
}

// ─── Scenario 5 ─────────────────────────────────────────────────────────────
// A stale ConnectionClosed event (from an old session) must NOT kill pending
// requests on a new connection. The session identity guard in
// `close_transport_if_webrtc_session` prevents this.
//
// We use `send_event` to inject a synthetic stale ConnectionClosed with a
// session_id that doesn't match the current wire. The test verifies that a
// call on the current connection is not affected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stale_close_event_does_not_kill_pending_requests() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;

    let target_id = harness.peer(200).id.clone();

    // Verify baseline works.
    let baseline = harness
        .peer(100)
        .spawn_request(200, "baseline_stale", 5_000);
    let _ = tokio::time::timeout(Duration::from_secs(5), baseline)
        .await
        .expect("baseline should complete")
        .expect("no panic")
        .expect("baseline ok");

    // Issue a call on the current connection.
    let on_current_conn = harness
        .peer(100)
        .spawn_request(200, "stale_event_test", 5_000);

    // Inject a stale ConnectionClosed event with an obviously wrong session_id
    // that won't match any active wire identity.
    harness
        .peer(100)
        .send_event(ConnectionEvent::ConnectionClosed {
            peer_id: target_id.clone(),
            session_id: 99999, // deliberately non-matching
        });

    // The call on the current connection must survive the stale event.
    match tokio::time::timeout(Duration::from_secs(8), on_current_conn).await {
        Ok(Ok(Ok(response))) => {
            tracing::info!("call survived stale close event: {} bytes", response.len());
        }
        Ok(Ok(Err(e))) => {
            // The call may still fail for unrelated reasons (e.g. timing),
            // but it should NOT be because of the stale event.
            let msg = e.to_string();
            assert!(
                !msg.contains("Connection closed"),
                "call was killed by stale close event: {}",
                msg
            );
            tracing::warn!("call failed for unrelated reason: {}", msg);
        }
        Ok(Err(e)) => panic!("task panicked: {}", e),
        Err(_) => panic!("call timed out — may have been killed by stale event"),
    }
}

// ─── Scenario 6 ─────────────────────────────────────────────────────────────
// `tell` (fire-and-forget) does not register pending_requests. After a
// successful tell, the pending_requests map should be empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tell_does_not_register_pending_requests() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;

    let target_id = harness.peer(200).id.clone();
    let gate = harness.peer(100).gate.clone();

    // Before: no pending requests.
    assert_eq!(
        harness.peer(100).pending_count().await,
        0,
        "no pending requests before tell"
    );

    // Send a tell (fire-and-forget).
    let envelope = actr_protocol::RpcEnvelope {
        request_id: "tell_no_pending".into(),
        route_key: "test.tell".into(),
        payload: Some(bytes::Bytes::from("fire_and_forget")),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 0, // tell semantics
        ..Default::default()
    };

    let result = gate.send_message(&target_id, envelope).await;
    assert!(
        result.is_ok(),
        "tell should succeed on a healthy connection"
    );

    // After: still no pending requests.
    assert_eq!(
        harness.peer(100).pending_count().await,
        0,
        "tell should not register pending requests"
    );
}

// ─── Scenario 7 ─────────────────────────────────────────────────────────────
// `send_data_stream` does NOT use `send_with_retry`. When the recovery guard
// is active (peer in recovery window), send_data_stream is rejected by
// preflight_send with "connection not ready" — just like call/tell. The key
// difference is that send_data_stream won't retry on Transient transport
// errors either.
//
// We test the preflight_send rejection which is the most reliable failure
// mode to trigger (recovery guard is deterministic).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_send_data_stream_rejected_during_recovery() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;

    let target_id = harness.peer(200).id.clone();

    // First verify send_data_stream works on a healthy connection.
    let gate = harness.peer(100).gate.clone();
    let healthy_result = gate
        .send_data_stream(
            &target_id,
            PayloadType::StreamReliable,
            "healthy_stream",
            bytes::Bytes::from("test"),
        )
        .await;
    assert!(
        healthy_result.is_ok(),
        "send_data_stream should succeed on healthy connection"
    );

    // Now put the peer in recovery.
    harness
        .peer(100)
        .coordinator
        .begin_network_recovery("test stream recovery")
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // send_data_stream should be rejected by preflight_send — no retry.
    let start = Instant::now();
    let result = gate
        .send_data_stream(
            &target_id,
            PayloadType::StreamReliable,
            "recovery_stream",
            bytes::Bytes::from("test"),
        )
        .await;

    let elapsed = start.elapsed();
    assert!(
        result.is_err(),
        "send_data_stream should fail during recovery window"
    );
    let err = result.unwrap_err();
    match err {
        ActrError::ConnectionNotReady(info) => {
            assert!(
                info.retry_after_ms.is_some(),
                "retry_after_ms should be present"
            );
        }
        other => panic!("expected ConnectionNotReady error, got: {other}"),
    }
    // Should be fast — no retry backoff.
    assert!(
        elapsed < Duration::from_secs(2),
        "send_data_stream should fail fast without retry, took {:?}",
        elapsed
    );
    tracing::info!(
        "send_data_stream correctly rejected without retry in {:?} with ConnectionNotReady",
        elapsed
    );
}

// ─── Scenario 8 ─────────────────────────────────────────────────────────────
// The `call` envelope timeout (e.g. 3s) truncates the retry policy
// (RpcReliable: 5 attempts, 1s→5s backoff ≈ 17s total). The caller sees a
// timeout error after 3s, not after 5 retry attempts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_envelope_timeout_truncates_retry_backoff() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;
    harness.reset_counters();

    // Block everything — no way for retries to succeed.
    harness.simulate_disconnect();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Issue a call with a SHORT envelope timeout (3s).
    // RpcReliable would normally retry 5 times over ~17s.
    let start = Instant::now();
    let handle = harness
        .peer(100)
        .spawn_request(200, "short_timeout_test", 3_000);

    match tokio::time::timeout(Duration::from_secs(6), handle).await {
        Ok(Ok(Err(e))) => {
            let elapsed = start.elapsed();
            let msg = e.to_string();
            assert!(
                msg.contains("timeout") || msg.contains("timed out") || msg.contains("Unavailable"),
                "expected timeout/unavailable error, got: {}",
                msg
            );
            // Should fail close to the 3s envelope timeout, not the ~17s
            // full retry policy duration.
            assert!(
                elapsed >= Duration::from_secs(3),
                "should wait at least the 3s timeout, took {:?}",
                elapsed
            );
            assert!(
                elapsed < Duration::from_secs(5),
                "should not exceed envelope timeout by much, took {:?}",
                elapsed
            );
            tracing::info!(
                "call correctly timed out in {:?} (3s envelope < 17s full retry)",
                elapsed
            );
        }
        Ok(Ok(Ok(_))) => panic!("call should timeout when network is down"),
        Ok(Err(e)) => panic!("task panicked: {}", e),
        Err(_) => panic!("outer timeout — call did not respect envelope timeout"),
    }
}

// ─── Scenario 9 ─────────────────────────────────────────────────────────────
// Full disconnect → reconnect cycle with `call`: verifies that
// `send_with_retry` can drive a call to eventual success after the transport
// has been fully rebuilt. This tests the interaction between Layer 2
// (send_with_retry) and the event-driven reconnection in Layer 3
// (DestTransport), plus the `preflight_send` recovery guard.
//
// This is the "happy path" end-to-end retry scenario that mirrors a real
// mobile network switch (WiFi ↔ cellular).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_succeeds_after_full_disconnect_reconnect_cycle() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;
    harness.reset_counters();

    // 1. Full outage — both UDP and signaling.
    harness.simulate_disconnect();

    // Wait for ICE to detect failure and start auto-restart (which will fail
    // because signaling is also down).
    tokio::time::sleep(Duration::from_secs(8)).await;

    // 2. Restore everything.
    harness.simulate_reconnect();

    // 3. Trigger recovery (simulating a mobile NetworkEvent::Available).
    harness.peer(100).retry_failed().await;

    // 4. Issue a call — it should succeed once the connection rebuilds.
    let response = expect_request_eventually_ok(
        &harness,
        100,
        200,
        "full_cycle_retry",
        Duration::from_secs(30),
        5_000,
    )
    .await;

    tracing::info!(
        "call succeeded after full disconnect/reconnect: {} bytes",
        response.len()
    );
}

// ─── Scenario 10 ────────────────────────────────────────────────────────────
// When a call is blocked in `preflight_send` (peer is in the 6s recovery
// window), it returns `ConnectionNotReady` immediately — it does NOT wait
// for the recovery to complete. This is not a retry; it is a fast-fail so the
// caller can decide what to do and retry after `on_webrtc_connected`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_fast_fails_during_recovery_window() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;
    // Manually enter network recovery on peer 100.
    harness
        .peer(100)
        .coordinator
        .begin_network_recovery("test recovery window")
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a call — should be rejected by preflight_send immediately.
    let start = Instant::now();
    let handle = harness
        .peer(100)
        .spawn_request(200, "recovery_window_fast_fail", 30_000);

    match tokio::time::timeout(Duration::from_secs(5), handle).await {
        Ok(Ok(Err(e))) => {
            let elapsed = start.elapsed();
            match e {
                ActrError::ConnectionNotReady(info) => {
                    assert!(
                        info.retry_after_ms.is_some(),
                        "retry_after_ms should be present"
                    );
                }
                other => panic!("expected ConnectionNotReady error, got: {other}"),
            }
            // Should be nearly instant, not waiting for 30s.
            assert!(
                elapsed < Duration::from_secs(2),
                "preflight_send should fast-fail, took {:?}",
                elapsed
            );
            tracing::info!(
                "preflight_send correctly fast-failed in {:?} with ConnectionNotReady",
                elapsed
            );
        }
        Ok(Ok(Ok(_))) => panic!("call should be blocked during recovery window"),
        Ok(Err(e)) => panic!("task panicked: {}", e),
        Err(_) => panic!("call timed out — preflight_send did not fast-fail"),
    }
}

// ─── Scenario 11 ────────────────────────────────────────────────────────────
// Mobile UI retry flow:
//   connected + sendable → network recovery guard → call fast-fails with
//   ConnectionNotReady → stale completion does not emit ready → current
//   sendable completion emits WebRtcConnected → retry after the hook succeeds.
//
// This protects the contract that `on_webrtc_connected(peer)` is the reliable
// signal for mobile callers to clear a Recovering UI state and retry a send.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_recovery_ready_hook_unblocks_mobile_retry() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;

    let target_id = harness.peer(200).id.clone();
    let session_id = harness
        .peer(100)
        .coordinator
        .get_peer_session_id(&target_id)
        .await
        .expect("initial connection should have a current WebRTC session");
    assert!(
        harness
            .peer(100)
            .coordinator
            .has_open_data_channel_for_test(&target_id)
            .await
            .expect("data channel state should be queryable"),
        "initial connection must have an open data channel before entering recovery"
    );

    let (hook, mut hook_rx) = recording_hook_callback();
    harness.peer(100).coordinator.set_hook_callback(hook);

    let guarded = harness
        .peer(100)
        .coordinator
        .begin_network_recovery("mobile network switch")
        .await;
    assert!(
        guarded.iter().any(|peer| peer == &target_id),
        "network recovery should guard the already-sendable target"
    );
    wait_for_webrtc_disconnected_hook(&mut hook_rx, &target_id, Duration::from_secs(1)).await;

    let guarded_request = harness
        .peer(100)
        .spawn_request(200, "mobile_retry_guarded", 30_000);
    expect_connection_not_ready(guarded_request, "request during recovery guard").await;

    harness
        .peer(100)
        .send_event(ConnectionEvent::IceRestartCompleted {
            peer_id: target_id.clone(),
            session_id: session_id + 1,
            success: true,
        });
    assert_no_webrtc_connected_hook(&mut hook_rx, &target_id, Duration::from_millis(200)).await;

    let stale_request =
        harness
            .peer(100)
            .spawn_request(200, "mobile_retry_after_stale_completion", 30_000);
    expect_connection_not_ready(stale_request, "request after stale recovery completion").await;

    harness
        .peer(100)
        .send_event(ConnectionEvent::IceRestartCompleted {
            peer_id: target_id.clone(),
            session_id,
            success: true,
        });
    wait_for_webrtc_connected_hook(&mut hook_rx, &target_id, Duration::from_secs(1)).await;
    assert!(
        harness
            .peer(100)
            .coordinator
            .peer_recovery_status(&target_id)
            .await
            .is_none(),
        "ready hook should only be emitted after the recovery guard is cleared"
    );

    let retry_after_ready =
        harness
            .peer(100)
            .spawn_request(200, "mobile_retry_after_ready_hook", 5_000);
    match tokio::time::timeout(Duration::from_secs(6), retry_after_ready).await {
        Ok(Ok(Ok(response))) => {
            tracing::info!(
                "retry after WebRtcConnected hook succeeded with {} bytes",
                response.len()
            );
        }
        Ok(Ok(Err(err))) => panic!("retry after ready hook failed: {err}"),
        Ok(Err(err)) => panic!("retry task panicked: {err}"),
        Err(_) => panic!("retry after ready hook timed out"),
    }
}
