//! Integration tests for retry dedup and request-id semantics.
//!
//! These tests verify the core correctness properties of the retry path:
//!
//! 1. `send_with_retry` re-sends the **same** serialized bytes (same
//!    request_id) on each attempt. Receiver-side dedup is covered by the
//!    deterministic retry core tests; this file focuses on pending response
//!    bookkeeping and wire-level request-id semantics.
//!
//! 2. `pending_requests` is a `HashMap<String, oneshot::Sender>` keyed by
//!    request_id. A **single call** registers **one** entry. If the same
//!    request_id arrives in `handle_response` twice (because the receiver
//!    processed a duplicate), only the **first** response is delivered —
//!    the second finds no pending entry (`Ok(false)`).
//!
//! 3. When `ConnectionClosed` cleanup removes a pending request's oneshot,
//!    any later response for that request_id is silently dropped
//!    (`Ok(false)` from `handle_response`). The caller has already received
//!    "Connection closed".
//!
//! 4. The `msg_id` in the WebRTC fragment header is per-lane and increments
//!    monotonically. Each `send_with_retry` attempt gets a fresh `msg_id`
//!    (because `DestTransport::send` may get a fresh lane). The fragment
//!    `msg_id` is NOT the same as the `request_id` in the RpcEnvelope.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use actr_hyper::test_support::TestHarness;
use actr_hyper::test_support::WebRtcFragmentSendEvent;
use actr_hyper::test_support::wait_for_data_channel_opened;
use actr_protocol::PayloadType;
use std::time::{Duration, Instant};

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
}

// ─── Test 1: pending request registration is keyed by request_id ─────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_single_pending_request_id_registration() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    let gate = harness.peer(100).gate.clone();
    let target_id = harness.peer(200).id.clone();

    let _rx = gate
        .register_pending_for_test("dedup_test_1", target_id)
        .await;

    assert_eq!(
        gate.pending_count().await,
        1,
        "one request_id should create exactly one pending entry"
    );
}

// ─── Test 2: Two calls get different request_ids ─────────────────────────
//
// Each `send_request_with_type` call generates its own `request_id` (from
// the `RpcEnvelope`). Two concurrent calls should have two distinct entries
// in `pending_requests`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_two_pending_requests_have_distinct_request_ids() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    let gate = harness.peer(100).gate.clone();
    let target_id = harness.peer(200).id.clone();

    let _rx1 = gate
        .register_pending_for_test("distinct_req_1", target_id.clone())
        .await;
    let _rx2 = gate
        .register_pending_for_test("distinct_req_2", target_id)
        .await;

    assert_eq!(
        gate.pending_count().await,
        2,
        "two distinct request_ids should create two pending entries"
    );
}

// ─── Test 3: handle_response deduplicates by request_id ──────────────────
//
// `pending_requests` is a `HashMap<String, (ActrId, oneshot::Sender)>`.
// `handle_response` does `pending.remove(request_id)` — it consumes the
// oneshot Sender on the first call. A second response for the same
// request_id finds no entry and returns `Ok(false)`.
//
// This test verifies:
// - First response → delivered to caller (Ok(true))
// - Duplicate response for the same request_id → silently dropped (Ok(false))
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_handle_response_deduplicates_by_request_id() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    let gate = harness.peer(100).gate.clone();
    let target_id = harness.peer(200).id.clone();

    let response_rx = gate
        .register_pending_for_test("dedup_response_test", target_id)
        .await;

    // Deliver the first response — should succeed (Ok(true)).
    let first = gate
        .handle_response("dedup_response_test", Ok(bytes::Bytes::from("pong")))
        .await
        .expect("handle_response should not error");
    assert!(
        first,
        "first response for request_id should be delivered (Ok(true))"
    );

    // Deliver a duplicate response for the same request_id — should be
    // silently dropped (Ok(false)), NOT error.
    let second = gate
        .handle_response("dedup_response_test", Ok(bytes::Bytes::from("pong2")))
        .await
        .expect("handle_response should not error");
    assert!(
        !second,
        "duplicate response for same request_id should be dropped (Ok(false))"
    );

    // Verify the caller received the FIRST response only.
    match tokio::time::timeout(Duration::from_secs(3), response_rx).await {
        Ok(Ok(Ok(response))) => {
            assert_eq!(
                &response[..],
                b"pong",
                "caller should receive the first response, not the duplicate"
            );
        }
        Ok(Ok(Err(e))) => panic!("response should succeed, got: {}", e),
        Ok(Err(e)) => panic!("response sender dropped: {}", e),
        Err(_) => panic!("response wait timed out"),
    }
}

// ─── Test 4: ConnectionClosed cleanup makes late response a no-op ────────
//
// When a `ConnectionClosed` event fires, the PeerGate event listener removes
// the pending request entry and sends "Connection closed" on the oneshot.
// After that, if a response arrives for the same request_id, `handle_response`
// returns `Ok(false)` — the response is silently dropped.
//
// This is critical: the caller has already received "Connection closed" and
// may decide to retry with a NEW request_id. The late response must NOT be
// delivered to the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_late_response_after_connection_closed_is_dropped() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

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

    let gate = harness.peer(100).gate.clone();
    let response_rx = gate
        .register_pending_for_test("late_response_test", target_id.clone())
        .await;

    // Verify the request is pending.
    assert_eq!(
        harness.peer(100).pending_count().await,
        1,
        "request should be pending before close"
    );

    // Close the DataChannel → triggers cleanup.
    harness
        .peer(100)
        .coordinator
        .close_data_channel_for_test(&target_id, PayloadType::RpcReliable)
        .await
        .expect("should close data channel");

    actr_hyper::test_support::wait_for_data_channel_close_chain(
        &mut event_rx,
        &target_id,
        session_id,
        Duration::from_secs(10),
    )
    .await;

    // Give the event listener time to clean up pending_requests.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify the request is NO LONGER pending — cleanup removed it.
    assert_eq!(
        harness.peer(100).pending_count().await,
        0,
        "pending request should be cleaned up after ConnectionClosed"
    );

    // Now simulate a late response arriving for the cleaned-up request.
    let late_result = gate
        .handle_response("late_response_test", Ok(bytes::Bytes::from("late_pong")))
        .await
        .expect("handle_response should not error");
    assert!(
        !late_result,
        "late response for cleaned-up request_id should be dropped (Ok(false))"
    );

    // The caller should have already received "Connection closed", not the
    // late response.
    match tokio::time::timeout(Duration::from_secs(3), response_rx).await {
        Ok(Ok(Err(e))) => {
            let msg = e.to_string();
            assert!(
                msg.contains("Connection")
                    || msg.contains("connection")
                    || msg.contains("Unavailable")
                    || msg.contains("recovering"),
                "caller should get connection error, not late response, got: {}",
                msg
            );
            tracing::info!("caller correctly got error, not late response: {}", msg);
        }
        Ok(Ok(Ok(response))) => panic!(
            "caller should not receive late response after cleanup: {:?}",
            response
        ),
        Ok(Err(e)) => panic!("response sender dropped without cleanup error: {}", e),
        Err(_) => panic!("response wait timed out"),
    }
}

// ─── Test 5: Successful send does NOT retry ─────────────────────────────
//
// `send_with_retry` only retries on ERROR. If the first `send()` returns
// `Ok(())`, the loop exits immediately. The same data is NOT sent again.
//
// We verify this by counting WebRTC fragment sends during a successful call.
// A single call should produce exactly one message on the wire (one msg_id).
// This also verifies that fragment `msg_id` is a transport-level u32 counter,
// independent from the string request_id in the `RpcEnvelope`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_successful_send_does_not_retry_and_uses_transport_msg_id() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;

    let mut event_rx = harness.peer(100).subscribe_events();
    let target_id = harness.peer(200).id.clone();

    harness.connect(100, 200).await;

    wait_for_data_channel_opened(
        &mut event_rx,
        &target_id,
        PayloadType::RpcReliable,
        Duration::from_secs(5),
    )
    .await;

    // Install a hook to capture fragment sends.
    let captured: Arc<Mutex<Vec<WebRtcFragmentSendEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let _guard = actr_hyper::test_support::install_webrtc_fragment_send_hook_for_test(Arc::new(
        move |event: WebRtcFragmentSendEvent| {
            captured_clone.lock().unwrap().push(event);
            Box::pin(async {})
        },
    ));

    // Clear any events from the connect phase.
    captured.lock().unwrap().clear();

    // Send a call on a healthy connection — should succeed on first attempt.
    let start = Instant::now();
    let handle = harness
        .peer(100)
        .spawn_request(200, "no_retry_success", 10_000);
    let result = tokio::time::timeout(Duration::from_secs(10), handle).await;

    // Should succeed.
    match result {
        Ok(Ok(Ok(_))) => {}
        Ok(Ok(Err(e))) => panic!("call should succeed: {}", e),
        Ok(Err(e)) => panic!("task panicked: {}", e),
        Err(_) => panic!("call timed out"),
    }

    let elapsed = start.elapsed();
    let events = captured.lock().unwrap();
    assert!(
        !events.is_empty(),
        "should have captured at least one fragment send event"
    );

    // On a healthy connection, there should be exactly ONE message sent
    // (one unique msg_id for the request). If send_with_retry were
    // retrying, we'd see multiple msg_ids or more fragments.
    let msg_ids: HashSet<u32> = events.iter().map(|e| e.msg_id).collect();
    assert_eq!(
        msg_ids.len(),
        1,
        "successful call should send data exactly once (1 unique msg_id), got {} msg_ids: {:?}",
        msg_ids.len(),
        msg_ids
    );

    for event in events.iter() {
        tracing::info!(
            "fragment: msg_id={}, frag_index={}/total={}, payload_len={}, msg_len={}",
            event.msg_id,
            event.frag_index,
            event.total_frags,
            event.fragment_payload_len,
            event.message_len,
        );
    }

    tracing::info!(
        "call completed in {:?} with {} fragments, {} unique msg_ids",
        elapsed,
        events.len(),
        msg_ids.len()
    );
}

// ─── Test 6: tell uses no response channel ───────────────────────────────
//
// Unlike `call`, `tell` has no pending_requests entry and no oneshot channel.
// A late response with the same request_id should therefore be ignored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tell_has_no_pending_response_channel() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness.add_peer(100).await;
    harness.add_peer(200).await;
    harness.connect(100, 200).await;

    let gate = harness.peer(100).gate.clone();
    let target_id = harness.peer(200).id.clone();

    // Send a tell — no pending request should be created.
    let envelope = actr_protocol::RpcEnvelope {
        request_id: "tell_retry_test".into(),
        route_key: "test.tell".into(),
        payload: Some(bytes::Bytes::from("fire_and_forget")),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 0,
        ..Default::default()
    };

    let result = gate.send_message(&target_id, envelope.clone()).await;
    assert!(result.is_ok(), "tell should succeed");

    // No pending request for tell.
    assert_eq!(
        harness.peer(100).pending_count().await,
        0,
        "tell should not create pending requests"
    );

    // If we artificially call handle_response for a tell's request_id,
    // it should find no pending entry (Ok(false)).
    let late = gate
        .handle_response("tell_retry_test", Ok(bytes::Bytes::from("unexpected")))
        .await
        .expect("handle_response should not error");
    assert!(!late, "tell request_id should never be in pending_requests");
}
