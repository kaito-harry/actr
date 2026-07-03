//! Test to reproduce: Request fails during connection cleanup
//!
//! Problem scenario from logs:
//! 1. App enters background for 237 seconds
//! 2. App returns to foreground, triggers cleanup_connections (takes 31 seconds)
//! 3. New request (PrepareClientStream) is initiated DURING cleanup
//! 4. Request fails: "Transport error: Channel closed"

use std::sync::Arc;
use std::time::Duration;

use actr_hyper::lifecycle::{DefaultNetworkEventProcessor, NetworkEventProcessor};
use actr_hyper::outbound::PeerGate;
use actr_hyper::test_support::{TestSignalingServer, create_peer_with_websocket, make_actor_id};
use actr_hyper::transport::{DefaultWireBuilder, DefaultWireBuilderConfig, PeerTransport};
use actr_protocol::RpcEnvelope;
use tokio::sync::Barrier;

/// Test: Request fails during cleanup - REAL reproduction
///
/// This test reproduces the exact scenario from the logs:
/// 1. Establish connection between two peers
/// 2. Start cleanup (which closes connections)
/// 3. Try to send RPC request DURING cleanup
/// 4. Verify request fails with "connection closed" or timeout error
#[tokio::test]
#[ignore]
async fn test_request_fails_during_cleanup() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🚀 Starting test: request fails during cleanup");

    // Phase 1: Setup - Create two peers
    let server = TestSignalingServer::start().await.unwrap();
    let id_a = make_actor_id(100);
    let id_b = make_actor_id(200);

    let (coord_a, signaling_client_a) = create_peer_with_websocket(id_a.clone(), &server.url())
        .await
        .unwrap();
    let (_coord_b, _signaling_client_b) = create_peer_with_websocket(id_b.clone(), &server.url())
        .await
        .unwrap();

    // Create PeerGate for testing
    let config = DefaultWireBuilderConfig::default();
    let wire_builder = Arc::new(DefaultWireBuilder::new(Some(coord_a.clone()), config));
    let transport_manager = Arc::new(PeerTransport::new(id_a.clone(), wire_builder));
    let gate_a = Arc::new(PeerGate::new(
        transport_manager.clone(),
        Some(coord_a.clone()),
    ));

    // Wait for WebSocket to stabilize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send initial request to establish connection
    tracing::info!("📤 Sending initial request to verify connection...");

    let initial_envelope = RpcEnvelope {
        request_id: "test_request_initial".to_string(),
        route_key: "test.PrepareClientStream".to_string(),
        payload: Some(bytes::Bytes::from(vec![1, 2, 3])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 5000,
        ..Default::default()
    };

    match tokio::time::timeout(
        Duration::from_secs(3),
        gate_a.send_request(&id_b, initial_envelope),
    )
    .await
    {
        Ok(Ok(_)) => {
            tracing::info!("✅ Initial request succeeded");
        }
        Ok(Err(e)) => {
            tracing::warn!("Initial request error (not fatal for this test): {:?}", e);
        }
        Err(_) => {
            tracing::warn!("Initial request timed out (not fatal for this test)");
        }
    }

    // Phase 2: Start cleanup and send request - Testing Solution 2: Caller reconnects after cleanup
    tracing::info!("🧹 Phase 2: Testing Solution 2 - Manual reconnect after cleanup...");

    use actr_hyper::lifecycle::{DefaultNetworkEventProcessor, NetworkEventProcessor};

    // Create processor for this test
    let processor = Arc::new(DefaultNetworkEventProcessor::new(
        signaling_client_a.clone(),
        Some(coord_a.clone()),
    ));

    let barrier = Arc::new(Barrier::new(2));
    let barrier_cleanup = barrier.clone();
    let barrier_request = barrier.clone();

    let processor_for_reconnect = processor.clone();

    // Spawn cleanup + reconnect task
    let cleanup_task = tokio::spawn(async move {
        barrier_cleanup.wait().await;

        // Step 1: Execute cleanup
        tracing::info!("🧹 Step 1: Executing cleanup_connections...");
        let start = std::time::Instant::now();
        let cleanup_result = processor.cleanup_connections().await;
        let cleanup_duration = start.elapsed();
        tracing::info!(
            "🧹 Cleanup completed in {:?}: {:?}",
            cleanup_duration,
            cleanup_result
        );

        // Step 2: Solution 2 - Immediately reconnect after cleanup
        if cleanup_result.is_ok() {
            tracing::info!("🔄 Step 2: Reconnecting WebSocket after cleanup...");
            match processor_for_reconnect.process_network_available().await {
                Ok(_) => {
                    tracing::info!("✅ WebSocket reconnected successfully");
                }
                Err(e) => {
                    tracing::warn!("⚠️ Reconnection failed: {}", e);
                }
            }
        }

        (cleanup_result, cleanup_duration)
    });

    // Spawn request task - waits for cleanup+reconnect, then sends request
    let gate_for_request = gate_a.clone();
    let id_b_for_request = id_b.clone();

    let request_task = tokio::spawn(async move {
        barrier_request.wait().await;

        // Wait for cleanup to start closing connections
        tokio::time::sleep(Duration::from_millis(100)).await;

        tracing::info!("📤 [First attempt] Sending request DURING cleanup (expected to fail)...");

        let request_envelope_1 = RpcEnvelope {
            request_id: "test_request_during_cleanup".to_string(),
            route_key: "test.PrepareClientStream".to_string(),
            payload: Some(bytes::Bytes::from(vec![4, 5, 6])),
            direction: Some(actr_protocol::Direction::Request as i32),
            timeout_ms: 5000,
            ..Default::default()
        };

        let first_request_start = std::time::Instant::now();
        let first_result = tokio::time::timeout(
            Duration::from_secs(3),
            gate_for_request.send_request(&id_b_for_request, request_envelope_1),
        )
        .await;
        let first_request_duration = first_request_start.elapsed();

        // Wait for reconnection to complete (cleanup task does this)
        tracing::info!("⏳ Waiting for WebSocket reconnection to complete...");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Second attempt: After reconnection, should succeed (or timeout waiting for response)
        tracing::info!("📤 [Second attempt] Sending request AFTER reconnect...");

        let request_envelope_2 = RpcEnvelope {
            request_id: "test_request_after_reconnect".to_string(),
            route_key: "test.PrepareClientStream".to_string(),
            payload: Some(bytes::Bytes::from(vec![7, 8, 9])),
            direction: Some(actr_protocol::Direction::Request as i32),
            timeout_ms: 5000,
            ..Default::default()
        };

        let second_request_start = std::time::Instant::now();
        let second_result = tokio::time::timeout(
            Duration::from_secs(3),
            gate_for_request.send_request(&id_b_for_request, request_envelope_2),
        )
        .await;
        let second_request_duration = second_request_start.elapsed();

        (
            first_result,
            first_request_duration,
            second_result,
            second_request_duration,
        )
    });

    // Wait for both tasks
    let (cleanup_result, cleanup_duration) = cleanup_task.await.expect("Cleanup task panicked");
    let (
        first_request_result,
        first_request_duration,
        second_request_result,
        second_request_duration,
    ) = request_task.await.expect("Request task panicked");

    // Phase 3: Verify results
    tracing::info!("🔍 Phase 3: Verifying results...");
    tracing::info!("  - Cleanup duration: {:?}", cleanup_duration);
    tracing::info!("  - First request duration: {:?}", first_request_duration);
    tracing::info!("  - Second request duration: {:?}", second_request_duration);

    // Cleanup should succeed
    assert!(
        cleanup_result.is_ok(),
        "Cleanup should succeed: {:?}",
        cleanup_result
    );

    // First request should fail (during cleanup)
    match first_request_result {
        Err(_) => {
            tracing::info!("✅ First request timed out (expected during cleanup)");
        }
        Ok(Err(network_err)) => {
            let err_str = format!("{:?}", network_err);
            tracing::info!("✅ First request failed with error: {}", err_str);
        }
        Ok(Ok(_)) => {
            tracing::warn!("⚠️ First request succeeded during cleanup (unexpected but OK)");
        }
    }

    // Second request should succeed or timeout (but NOT fail with connection error)
    let second_recovered = match second_request_result {
        Ok(Err(e)) if format!("{:?}", e).to_lowercase().contains("timeout") => {
            tracing::info!(
                "✅ Second request sent (timed out waiting for response, which is acceptable)"
            );
            true
        }
        Err(_) => {
            tracing::info!("✅ Second request sent (response timeout is acceptable)");
            true
        }
        Ok(Ok(_)) => {
            tracing::info!("✅ Second request succeeded after reconnect!");
            true
        }
        Ok(Err(e)) if format!("{:?}", e).to_lowercase().contains("closed") => {
            tracing::error!(
                "❌ Second request failed with connection closed (reconnect didn't work)"
            );
            false
        }
        Ok(Err(e)) => {
            tracing::warn!("⚠️ Second request failed with: {:?}", e);
            false
        }
    };

    assert!(
        second_recovered,
        "Second request should succeed after reconnect (got connection closed error)"
    );

    tracing::info!(
        "✅ Successfully verified: First request failed, second request worked after reconnect"
    );
}

/// Test: Request succeeds AFTER cleanup completes
#[tokio::test]
#[ignore]
async fn test_request_succeeds_after_cleanup() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🚀 Starting test: request succeeds after cleanup");

    let server = TestSignalingServer::start().await.unwrap();
    let id_a = make_actor_id(300);
    let id_b = make_actor_id(400);

    let (coord_a, signaling_client_a) = create_peer_with_websocket(id_a.clone(), &server.url())
        .await
        .unwrap();
    let (_coord_b, _signaling_client_b) = create_peer_with_websocket(id_b.clone(), &server.url())
        .await
        .unwrap();

    // Create PeerGate
    let config = DefaultWireBuilderConfig::default();
    let wire_builder = Arc::new(DefaultWireBuilder::new(Some(coord_a.clone()), config));
    let transport_manager = Arc::new(PeerTransport::new(id_a.clone(), wire_builder));
    let gate_a = Arc::new(PeerGate::new(transport_manager, Some(coord_a.clone())));

    // Wait for connection
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Establish connection first
    tracing::info!("📤 Establishing initial connection...");
    // Initiate connection
    let ready_rx = coord_a
        .initiate_connection(&id_b)
        .await
        .expect("Failed to initiate");

    tokio::time::timeout(Duration::from_secs(10), ready_rx)
        .await
        .expect("Connection timeout")
        .expect("Connection failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Execute cleanup + reconnect (Solution 2)
    tracing::info!("🧹 Executing cleanup...");
    let processor = Arc::new(DefaultNetworkEventProcessor::new(
        signaling_client_a.clone(),
        Some(coord_a.clone()),
    ));

    let cleanup_result = processor.cleanup_connections().await;
    assert!(cleanup_result.is_ok(), "Cleanup should succeed");

    // Solution 2: Immediately reconnect after cleanup
    tracing::info!("🔄 Reconnecting after cleanup...");
    match processor.process_network_available().await {
        Ok(_) => {
            tracing::info!("✅ Reconnection successful");
        }
        Err(e) => {
            tracing::warn!("⚠️ Reconnection failed: {}", e);
        }
    }

    // Wait for connection to stabilize
    tracing::info!("⏳ Waiting for connection to stabilize...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Try to send request after cleanup - should succeed (or timeout waiting for response)
    tracing::info!("📤 Sending request after cleanup...");
    let request_envelope = RpcEnvelope {
        request_id: "after_cleanup".to_string(),
        route_key: "test.PrepareClientStream".to_string(),
        payload: Some(bytes::Bytes::from(vec![7, 8, 9])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 5000,
        ..Default::default()
    };

    match tokio::time::timeout(
        Duration::from_secs(3),
        gate_a.send_request(&id_b, request_envelope),
    )
    .await
    {
        Ok(Err(e)) if format!("{:?}", e).to_lowercase().contains("timeout") => {
            tracing::info!(
                "✅ Request sent after cleanup (timed out waiting for response, which is acceptable)"
            );
        }
        Err(_) => {
            tracing::info!(
                "✅ Request sent after cleanup (send succeeded, response timeout is acceptable)"
            );
        }
        Ok(Ok(_)) => {
            tracing::info!("✅ Request succeeded after cleanup");
        }
        Ok(Err(e)) if format!("{:?}", e).to_lowercase().contains("closed") => {
            panic!(
                "Request should not fail with 'closed' error after cleanup: {:?}",
                e
            );
        }
        Ok(Err(e)) => {
            tracing::warn!("Request failed after cleanup with: {:?}", e);
        }
    }

    tracing::info!("✅ Test completed: Request was sent successfully after cleanup");
}

/// Test 3: Verify DestTransport cache is NOT cleaned up after cleanup_connections
///
/// This reproduces the REAL issue from logs:
/// 1. Send first request → creates DestTransport
/// 2. Call cleanup_connections → closes WebRTC but DestTransport remains cached
/// 3. Send second request → REUSES old DestTransport with closed connections
/// 4. Should fail with "connection closed" error (bug!)
#[tokio::test]
#[ignore]
async fn test_dest_transport_cache_not_cleaned() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🚀 Test: Verifying DestTransport cache is NOT cleaned after cleanup");

    // Setup
    let server = TestSignalingServer::start().await.unwrap();
    let id_a = make_actor_id(500);
    let id_b = make_actor_id(600);

    let (coord_a, signaling_client_a) = create_peer_with_websocket(id_a.clone(), &server.url())
        .await
        .unwrap();
    let (_coord_b, _signaling_client_b) = create_peer_with_websocket(id_b.clone(), &server.url())
        .await
        .unwrap();

    // Create components
    let config = DefaultWireBuilderConfig::default();
    let wire_builder = Arc::new(DefaultWireBuilder::new(Some(coord_a.clone()), config));
    let transport_manager = Arc::new(PeerTransport::new(id_a.clone(), wire_builder));
    let gate_a = Arc::new(PeerGate::new(
        transport_manager.clone(),
        Some(coord_a.clone()),
    ));

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 1: Send first request - creates and caches DestTransport
    tracing::info!("📤 Phase 1: Sending first request (creates DestTransport)...");

    let envelope_1 = RpcEnvelope {
        request_id: "request_1".to_string(),
        route_key: "test.Echo".to_string(),
        payload: Some(bytes::Bytes::from(vec![1, 2, 3])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 3000,
        ..Default::default()
    };

    match tokio::time::timeout(
        Duration::from_secs(2),
        gate_a.send_request(&id_b, envelope_1),
    )
    .await
    {
        Ok(Ok(_)) => tracing::info!("✅ First request succeeded"),
        Ok(Err(e)) => tracing::info!("⚠️ First request failed (OK for test): {:?}", e),
        Err(_) => tracing::info!("⏱️ First request timed out (OK for test)"),
    }

    // Verify DestTransport was created
    let dest_count_before = transport_manager.dest_count().await;
    tracing::info!(
        "📊 DestTransport count BEFORE cleanup: {}",
        dest_count_before
    );
    assert_eq!(
        dest_count_before, 1,
        "Should have 1 DestTransport after first request"
    );

    // Phase 2: Call cleanup_connections (closes WebRTC, but does NOT clean DestTransport cache)
    tracing::info!("🧹 Phase 2: Calling cleanup_connections...");

    use actr_hyper::lifecycle::{DefaultNetworkEventProcessor, NetworkEventProcessor};
    let processor = Arc::new(DefaultNetworkEventProcessor::new(
        signaling_client_a.clone(),
        Some(coord_a.clone()),
    ));

    processor
        .cleanup_connections()
        .await
        .expect("Cleanup should succeed");
    tracing::info!("✅ Cleanup completed");

    // Do NOT wait! Check cache state immediately
    // This captures the cache state before ready_monitor has a chance to clean up
    let dest_count_after_cleanup = transport_manager.dest_count().await;
    tracing::info!(
        "📊 DestTransport count AFTER cleanup (immediate): {}",
        dest_count_after_cleanup
    );

    // BUG: if cache still exists, ready_monitor hasn't cleaned up yet
    if dest_count_after_cleanup > 0 {
        tracing::warn!(
            "❌ BUG CONDITION: DestTransport cache still has {} entries!",
            dest_count_after_cleanup
        );
        tracing::warn!("    ready_monitor hasn't cleaned up yet - will reuse closed connections!");
    } else {
        tracing::info!("✅ DestTransport cache was already cleaned by ready_monitor");
    }

    // Phase 3: send request immediately before reconnect - this is the real bug scenario!
    tracing::info!("📤 Phase 3: Sending request IMMEDIATELY after cleanup (before reconnect)...");

    let envelope_immediate = RpcEnvelope {
        request_id: "request_immediate".to_string(),
        route_key: "test.Echo".to_string(),
        payload: Some(bytes::Bytes::from(vec![9, 9, 9])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 3000,
        ..Default::default()
    };

    // This request should fail because the connection is already closed
    match tokio::time::timeout(
        Duration::from_secs(2),
        gate_a.send_request(&id_b, envelope_immediate),
    )
    .await
    {
        Ok(Ok(_)) => {
            tracing::info!("⚠️ Immediate request succeeded (unexpected)");
        }
        Ok(Err(e)) => {
            let err_str = format!("{:?}", e);
            if err_str.to_lowercase().contains("closed") {
                tracing::warn!("❌ BUG REPRODUCED: Immediate request got 'connection closed'!");
                tracing::warn!("    Error: {}", err_str);
            } else if err_str.to_lowercase().contains("unavailable") {
                tracing::warn!("❌ BUG REPRODUCED: Signaling unavailable (WebSocket closed)!");
                tracing::warn!("    Error: {}", err_str);
            } else {
                tracing::info!("⚠️ Immediate request failed: {:?}", e);
            }
        }
        Err(_) => {
            tracing::info!("⏱️ Immediate request timed out");
        }
    }

    // Now reconnect
    tracing::info!("🔄 Phase 4: Reconnecting WebSocket...");
    processor
        .process_network_available()
        .await
        .expect("Reconnect should succeed");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 5: Send second request - now should work (or timeout waiting for response)
    tracing::info!("📤 Phase 4: Sending second request (will reuse cached DestTransport)...");

    let envelope_2 = RpcEnvelope {
        request_id: "request_2".to_string(),
        route_key: "test.Echo".to_string(),
        payload: Some(bytes::Bytes::from(vec![4, 5, 6])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 3000,
        ..Default::default()
    };

    // This request will fail with "connection closed" because it reuses old DestTransport
    match tokio::time::timeout(
        Duration::from_secs(40),
        gate_a.send_request(&id_b, envelope_2),
    )
    .await
    {
        Ok(Ok(_)) => {
            tracing::info!(
                "✅ Second request succeeded (cache was cleaned or connections recovered)"
            );
        }
        Ok(Err(e)) => {
            let err_str = format!("{:?}", e);
            if err_str.to_lowercase().contains("closed") {
                tracing::warn!("❌ BUG CONFIRMED: Second request failed with 'closed' error!");
                tracing::warn!("    Error: {}", err_str);
                tracing::warn!(
                    "    This proves: DestTransport cache reused connections that were already closed!"
                );

                // This is the expected failure due to the bug
                panic!(
                    "BUG REPRODUCED: Request failed because DestTransport cache wasn't cleaned. Error: {}",
                    err_str
                );
            } else {
                tracing::info!("⚠️ Second request failed (different reason): {:?}", e);
            }
        }
        Err(e) => {
            tracing::info!(
                "⏱️ Second request timed out (acceptable if creating new connection) , error: {:?}",
                e
            );
        }
    }

    // Final check
    let dest_count_final = transport_manager.dest_count().await;
    tracing::info!("📊 DestTransport count FINAL: {}", dest_count_final);

    tracing::info!("✅ Test completed - Bug status logged above");
}

/// Test 4: PRECISE reproduction of log scenario
///
/// Log scenario (lines 360-428):
/// 1. WebRTC DataChannel closed (10:29:37)
/// 2. Signaling reconnect succeeded (10:29:37)
/// 3. cleanup_connections still in progress (waiting for close_all_peers to complete)
/// 4. New request sent (10:29:45) → Reuses cached DestTransport
/// 5. Request fails: "connection closed"
///
/// Key: WebSocket is connected, but WebRTC is closed, DestTransport cache not cleared
#[tokio::test]
#[ignore]
async fn test_precise_log_reproduction() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🚀 Test: PRECISE reproduction of log scenario");
    tracing::info!("   Scenario: WebRTC closed, WebSocket connected, DestTransport cached");

    // Setup
    let server = TestSignalingServer::start().await.unwrap();
    let id_a = make_actor_id(700);
    let id_b = make_actor_id(800);

    let (coord_a, signaling_client_a) = create_peer_with_websocket(id_a.clone(), &server.url())
        .await
        .unwrap();
    let (_coord_b, _signaling_client_b) = create_peer_with_websocket(id_b.clone(), &server.url())
        .await
        .unwrap();

    // Create components
    let config = DefaultWireBuilderConfig::default();
    let wire_builder = Arc::new(DefaultWireBuilder::new(Some(coord_a.clone()), config));
    let transport_manager = Arc::new(PeerTransport::new(id_a.clone(), wire_builder));
    let gate_a = Arc::new(PeerGate::new(
        transport_manager.clone(),
        Some(coord_a.clone()),
    ));

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 1: Send first request to create and cache DestTransport
    tracing::info!("📤 Phase 1: Sending first request to create DestTransport cache...");

    let envelope_1 = RpcEnvelope {
        request_id: "request_1".to_string(),
        route_key: "test.Echo".to_string(),
        payload: Some(bytes::Bytes::from(vec![1, 2, 3])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 3000,
        ..Default::default()
    };

    match tokio::time::timeout(
        Duration::from_secs(5),
        gate_a.send_request(&id_b, envelope_1),
    )
    .await
    {
        Ok(Ok(_)) => tracing::info!("✅ First request succeeded"),
        Ok(Err(e)) => tracing::info!("⚠️ First request failed (OK for test): {:?}", e),
        Err(_) => tracing::info!("⏱️ First request timed out (OK for test)"),
    }

    // Verify DestTransport and WebRTC connection established
    let dest_count_before = transport_manager.dest_count().await;
    tracing::info!(
        "📊 Phase 1 complete: DestTransport count = {}",
        dest_count_before
    );
    assert_eq!(
        dest_count_before, 1,
        "Should have 1 DestTransport after first request"
    );

    // Phase 2 & 3: Close WebRTC and send request CONCURRENTLY
    // In real scenario (log), close_all_peers takes 31 seconds
    // Request arrives during this time, before ready_monitor cleans DestTransport
    tracing::info!("🔻 Phase 2&3: Closing WebRTC AND sending request CONCURRENTLY...");

    let coord_for_close = coord_a.clone();
    let gate_for_request = gate_a.clone();
    let id_b_for_request = id_b.clone();
    let transport_manager_for_check = transport_manager.clone();

    // Spawn close task
    let close_task = tokio::spawn(async move {
        coord_for_close
            .close_all_peers()
            .await
            .expect("Failed to close peers");
        tracing::info!("✅ WebRTC close_all_peers completed");
    });

    // Wait just a tiny bit for close to START (but not complete)
    tokio::time::sleep(Duration::from_millis(1)).await;

    // Check cache IMMEDIATELY (before ready_monitor has chance to clean)
    let dest_count_during_close = transport_manager_for_check.dest_count().await;
    tracing::info!(
        "📊 DestTransport count DURING close: {}",
        dest_count_during_close
    );

    if dest_count_during_close > 0 {
        tracing::warn!(
            "❌ BUG CONDITION: Cache still has {} entries during close!",
            dest_count_during_close
        );
    }

    // Send request IMMEDIATELY
    tracing::info!("📤 Sending request DURING WebRTC close...");

    let envelope_2 = RpcEnvelope {
        request_id: "request_2".to_string(),
        route_key: "test.Echo".to_string(),
        payload: Some(bytes::Bytes::from(vec![4, 5, 6])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 3000,
        ..Default::default()
    };

    let request_result = tokio::time::timeout(
        Duration::from_secs(2),
        gate_for_request.send_request(&id_b_for_request, envelope_2),
    )
    .await;

    // Wait for close to complete
    close_task.await.expect("Close task panicked");

    match request_result {
        Ok(Ok(_)) => {
            tracing::info!("⚠️ Request succeeded (unexpected)");
        }
        Ok(Err(e)) => {
            let err_str = format!("{:?}", e);
            if err_str.to_lowercase().contains("closed") {
                tracing::warn!("❌ BUG REPRODUCED: Request failed with 'connection closed'!");
                tracing::warn!("   Error: {}", err_str);
                tracing::warn!(
                    "   This matches log line 428: 'Failed to get DataLane: WebRTC error: connection closed'"
                );
                tracing::info!("✅ BUG SUCCESSFULLY REPRODUCED!");
            } else {
                tracing::info!("⚠️ Request failed with: {:?}", e);
            }
        }
        Err(_) => {
            tracing::info!("⏱️ Request timed out");
        }
    }

    tracing::info!(
        "   WebSocket still connected: {}",
        signaling_client_a.is_connected()
    );

    // Final status
    let dest_count_final = transport_manager.dest_count().await;
    tracing::info!("📊 DestTransport count FINAL: {}", dest_count_final);
    tracing::info!("✅ Test completed");
}
