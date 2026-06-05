//! Network Event Handling Integration Tests
//!
//! Tests the network event handling mechanism with real WebRTC connections and WebSocket signaling.
//! These tests verify:
//! - Network available event triggers reconnection and ICE restart
//! - Network lost event handles cleanup correctly
//! - Network type changed event triggers full recovery sequence
//! - Result feedback mechanism works correctly

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use actr_hyper::lifecycle::{
    DefaultNetworkEventProcessor, NetworkAvailability, NetworkEvent, NetworkEventHandle,
    NetworkEventProcessor, NetworkEventResult, NetworkSnapshot, NetworkTransportFlags,
    process_network_event_batch,
};
use actr_hyper::test_support::{TestSignalingServer, create_peer_with_websocket, make_actor_id};

fn network_snapshot(sequence: u64, available: bool) -> NetworkSnapshot {
    NetworkSnapshot {
        sequence,
        availability: if available {
            NetworkAvailability::Available
        } else {
            NetworkAvailability::Unavailable
        },
        transport: NetworkTransportFlags {
            wifi: available,
            cellular: false,
            ethernet: false,
            vpn: false,
            other: false,
        },
        is_expensive: false,
        is_constrained: false,
    }
}

// ==================== Tests ====================

/// Test network available triggers recovery
#[tokio::test]
async fn test_network_available_triggers_recovery() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🧪 Test: Network available triggers recovery");

    let server = TestSignalingServer::start().await.unwrap();

    // Create two peers
    let id_peer_a = make_actor_id(100);
    let id_peer_b = make_actor_id(200);

    let (coordinator_a, signaling_client_a) =
        create_peer_with_websocket(id_peer_a.clone(), &server.url())
            .await
            .unwrap();
    let (_coordinator_b, _signaling_client_b) =
        create_peer_with_websocket(id_peer_b.clone(), &server.url())
            .await
            .unwrap();

    // Establish initial connection
    tracing::info!("🔗 Establishing initial peer connection...");
    let ready_rx = coordinator_a
        .initiate_connection(&id_peer_b)
        .await
        .expect("initiate failed");

    match tokio::time::timeout(Duration::from_secs(10), ready_rx).await {
        Ok(Ok(_)) => {
            tracing::info!("✅ Initial peer connection established!");
        }
        Ok(Err(_)) => panic!("Connection failed (channel closed)"),
        Err(_) => panic!("Connection timed out"),
    }

    // Wait for connection to stabilize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create NetworkEventProcessor
    // Note: DefaultNetworkEventProcessor constructor uses SignalingClient
    let processor = Arc::new(DefaultNetworkEventProcessor::new(
        signaling_client_a.clone(),
        Some(coordinator_a.clone()),
    ));

    // Create channels for NetworkEventHandle
    let (event_tx, mut event_rx) = mpsc::channel(10);
    let network_handle = NetworkEventHandle::new(event_tx);

    // Start event loop to process events
    let processor_clone = processor.clone();
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown_token.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(request) = event_rx.recv() => {
                    let event = request.event;
                    tracing::info!("📥 Processing event: {:?}", event);
                    let mut results =
                        process_network_event_batch(vec![event], processor_clone.clone()).await;
                    let event_result = results.remove(0);
                    let _ = request.result_tx.send(event_result);
                }
                _ = shutdown_clone.cancelled() => break,
            }
        }
    });

    // Reset counters before triggering event
    server.reset_counters();
    let initial_ice_restart_count = server.get_ice_restart_count();

    // Trigger Network Available event (should trigger ICE restart)
    tracing::info!("📱 Triggering network available event...");
    let result = network_handle
        .handle_network_path_changed(network_snapshot(1, true))
        .await
        .expect("Failed to handle network available");

    tracing::info!(
        "📊 Result: success={}, duration={}ms",
        result.success,
        result.duration_ms
    );
    assert!(
        result.success,
        "Network available processing should succeed"
    );

    // Allow some time for ICE restart offers to be sent
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Verify ICE restart occurred
    let new_ice_restart_count = server.get_ice_restart_count();
    tracing::info!(
        "📊 ICE restart offers: {} -> {}",
        initial_ice_restart_count,
        new_ice_restart_count
    );
    assert!(
        new_ice_restart_count > initial_ice_restart_count,
        "Should have triggered ICE restart"
    );

    shutdown_token.cancel();
}

/// Test network lost cleanup
#[tokio::test]
async fn test_network_lost_cleanup() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🧪 Test: Network lost cleanup");

    let server = TestSignalingServer::start().await.unwrap();
    let id_peer_a = make_actor_id(300);

    // We only need one peer/client for this test
    let (coordinator_a, signaling_client_a) =
        create_peer_with_websocket(id_peer_a.clone(), &server.url())
            .await
            .unwrap();

    // Create processor
    let processor = Arc::new(DefaultNetworkEventProcessor::new(
        signaling_client_a.clone(),
        Some(coordinator_a.clone()),
    ));

    // Create handle
    let (event_tx, mut event_rx) = mpsc::channel(10);
    let network_handle = NetworkEventHandle::new(event_tx);

    // Start event loop
    let processor_clone = processor.clone();
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown_token.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(request) = event_rx.recv() => {
                    let event = request.event;
                    let mut results =
                        process_network_event_batch(vec![event], processor_clone.clone()).await;
                    let event_result = results.remove(0);
                    let _ = request.result_tx.send(event_result);
                }
                _ = shutdown_clone.cancelled() => break,
            }
        }
    });

    // Verify initial state: Connected
    assert!(signaling_client_a.is_connected());

    // Trigger Network Lost
    tracing::info!("📱 Triggering network lost event...");

    // We can use the handle...
    let result = network_handle
        .handle_network_path_changed(network_snapshot(1, false))
        .await
        .expect("Failed to handle network lost");

    tracing::info!("📊 Result: success={}", result.success);
    assert!(result.success);

    // Verify state: Should be disconnected (or at least disconnect was called)
    // Note: WebSocket client might auto-reconnect if server is still up.
    // But process_network_lost calls client.disconnect().
    // Let's verify that logic.

    // For test stability with real websocket, we check if disconnect message was sent or similar?
    // Or just trust the `result.success` from the processor which calls `client.disconnect()`.
    // Since we are using a real client, `disconnect()` should close the connection.

    // Let's check `is_connected()`
    // Even if it reconnects, there should be a window where it is disconnected.
    // But `process_network_lost` awaits `disconnect()`.

    // Wait a brief moment for update
    tokio::time::sleep(Duration::from_millis(50)).await;

    // NOTE: Real WebSocketSignalingClient implementation of disconnect() sets state to Disconnected.
    let is_connected = signaling_client_a.is_connected();
    tracing::info!("� Is connected: {}", is_connected);
    assert!(
        !is_connected,
        "Client should be disconnected after network lost"
    );

    shutdown_token.cancel();
}

/// Test result feedback mechanism
#[tokio::test]
async fn test_result_feedback_mechanism() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🧪 Test: Result feedback mechanism");

    let (event_tx, mut event_rx) = mpsc::channel(10);
    let network_handle = NetworkEventHandle::new(event_tx);

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown_token.clone();

    // Spawn dummy processor loop
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(request) = event_rx.recv() => {
                    let event = request.event;
                    // Simulate processing delay
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    // Always return success for this test
                    let result = NetworkEventResult::success(event, 50);
                    let _ = request.result_tx.send(result);
                }
                _ = shutdown_clone.cancelled() => break,
            }
        }
    });

    // Send event and wait for result
    tracing::info!("📱 Sending event and waiting for result...");
    let result = network_handle
        .handle_network_path_changed(network_snapshot(1, true))
        .await
        .expect("Failed to get result");

    tracing::info!("📊 Got result: {:?}", result);
    assert!(matches!(
        result.event,
        NetworkEvent::NetworkPathChanged { .. }
    ));
    assert!(result.success);
    assert!(result.duration_ms >= 50);

    shutdown_token.cancel();
    tracing::info!("✅ Result feedback test passed");
}

/// Test network repeatedly changing (multiple Available/Lost cycles)
#[tokio::test]
async fn test_network_repeatedly_changing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🧪 Test: Network repeatedly changing");

    let server = TestSignalingServer::start().await.unwrap();

    // Create two peers to establish a real WebRTC connection
    let id_peer_a = make_actor_id(600);
    let id_peer_b = make_actor_id(700);

    let (coordinator_a, signaling_client_a) =
        create_peer_with_websocket(id_peer_a.clone(), &server.url())
            .await
            .unwrap();
    let (_coordinator_b, _signaling_client_b) =
        create_peer_with_websocket(id_peer_b.clone(), &server.url())
            .await
            .unwrap();

    // Establish initial connection
    tracing::info!("🔗 Establishing initial peer connection...");
    let ready_rx = coordinator_a
        .initiate_connection(&id_peer_b)
        .await
        .expect("initiate failed");

    match tokio::time::timeout(Duration::from_secs(10), ready_rx).await {
        Ok(Ok(_)) => {
            tracing::info!("✅ Initial peer connection established!");
        }
        Ok(Err(_)) => panic!("Connection failed (channel closed)"),
        Err(_) => panic!("Connection timed out"),
    }

    // Wait for connection to stabilize
    tokio::time::sleep(Duration::from_millis(500)).await;

    let processor = Arc::new(DefaultNetworkEventProcessor::new(
        signaling_client_a.clone(),
        Some(coordinator_a.clone()),
    ));

    // Create channels and handle
    let (event_tx, mut event_rx) = mpsc::channel(10);
    let network_handle = NetworkEventHandle::new(event_tx);

    // Start event loop
    let processor_clone = processor.clone();
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown_token.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(request) = event_rx.recv() => {
                    let event = request.event;
                    let mut results =
                        process_network_event_batch(vec![event], processor_clone.clone()).await;
                    let event_result = results.remove(0);
                    let _ = request.result_tx.send(event_result);
                }
                _ = shutdown_clone.cancelled() => break,
            }
        }
    });

    // Wait for initialization
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Reset counters
    server.reset_counters();

    // Simulate multiple network change cycles
    const CYCLES: usize = 3;
    let initial_count = server.get_ice_restart_count();

    for cycle in 1..=CYCLES {
        tracing::info!("🔄 Network change cycle {}/{}", cycle, CYCLES);

        // Network Lost
        tracing::info!("📱 Cycle {}: Triggering network lost event...", cycle);
        let result = network_handle
            .handle_network_path_changed(network_snapshot(cycle as u64 * 2, false))
            .await
            .expect("Failed to handle network lost");

        tracing::info!(
            "📊 Cycle {}: Lost result: success={}, duration={}ms",
            cycle,
            result.success,
            result.duration_ms
        );

        assert!(
            result.success,
            "Network lost should succeed in cycle {}",
            cycle
        );

        // Wait a bit
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Network Available (triggers ICE restart)
        tracing::info!("📱 Cycle {}: Triggering network available event...", cycle);
        let result = network_handle
            .handle_network_path_changed(network_snapshot(cycle as u64 * 2 + 1, true))
            .await
            .expect("Failed to handle network available");

        tracing::info!(
            "📊 Cycle {}: Available result: success={}, duration={}ms",
            cycle,
            result.success,
            result.duration_ms
        );

        assert!(
            result.success,
            "Network available should succeed in cycle {}",
            cycle
        );

        // Wait for ICE restart to complete
        tokio::time::sleep(Duration::from_millis(2000)).await;
    }

    // Verify ICE restart happened multiple times
    let final_count = server.get_ice_restart_count();
    let delta = final_count - initial_count;
    tracing::info!(
        "📊 ICE restart offers: {} -> {} (delta: {})",
        initial_count,
        final_count,
        delta
    );

    // We expect roughly CYCLES amounts of restarts. It might be less if some are deduplicated, but should be at least 1.
    assert!(
        delta >= 1,
        "Should have at least 1 ICE restart offer, got {}",
        delta
    );

    shutdown_token.cancel();
    tracing::info!("✅ Network repeatedly changing test completed successfully");
}

/// Test manual cleanup_connections (no debounce, always executes)
#[tokio::test]
async fn test_manual_cleanup_connections() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🧪 Test: Manual cleanup_connections");

    let server = TestSignalingServer::start().await.unwrap();
    let id_peer_a = make_actor_id(800);

    let (coordinator_a, signaling_client_a) =
        create_peer_with_websocket(id_peer_a.clone(), &server.url())
            .await
            .unwrap();

    // Create processor
    let processor = Arc::new(DefaultNetworkEventProcessor::new(
        signaling_client_a.clone(),
        Some(coordinator_a.clone()),
    ));

    // Verify initial state: Connected
    assert!(signaling_client_a.is_connected());

    // Call cleanup_connections directly (bypassing event loop and debounce)
    tracing::info!("🧹 Calling cleanup_connections() directly...");
    let result = processor.cleanup_connections().await;

    tracing::info!("📊 Result: {:?}", result);
    assert!(result.is_ok(), "cleanup_connections should succeed");

    // Verify state: Should be disconnected
    tokio::time::sleep(Duration::from_millis(100)).await;
    let is_connected = signaling_client_a.is_connected();
    tracing::info!("📊 Is connected after cleanup: {}", is_connected);
    assert!(
        !is_connected,
        "Client should be disconnected after cleanup_connections"
    );

    // Test: cleanup_connections is NOT debounced (call it twice rapidly)
    tracing::info!("🔄 Testing that cleanup_connections is NOT debounced...");

    // Reconnect first
    signaling_client_a.connect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(signaling_client_a.is_connected(), "Should be reconnected");

    // Call cleanup twice in rapid succession
    let start = Instant::now();
    let result1 = processor.cleanup_connections().await;
    let result2 = processor.cleanup_connections().await;
    let elapsed = start.elapsed();

    tracing::info!("📊 First cleanup: {:?}", result1);
    tracing::info!("📊 Second cleanup: {:?}", result2);
    tracing::info!("📊 Elapsed time for both calls: {:?}", elapsed);

    // Both should succeed (no debouncing)
    assert!(result1.is_ok(), "First cleanup should succeed");
    assert!(result2.is_ok(), "Second cleanup should succeed");

    // Should complete quickly (no debounce delay)
    assert!(
        elapsed < Duration::from_millis(500),
        "cleanup_connections should not be debounced"
    );

    tracing::info!("✅ Manual cleanup_connections test passed!");
}

/// Test cleanup_connections followed by network events (recovery after manual cleanup)
#[tokio::test]
async fn test_cleanup_then_network_events() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("🧪 Test: Cleanup then network events (recovery after manual cleanup)");

    let server = TestSignalingServer::start().await.unwrap();
    let id_peer_a = make_actor_id(900);
    let id_peer_b = make_actor_id(1000);

    let (coordinator_a, signaling_client_a) =
        create_peer_with_websocket(id_peer_a.clone(), &server.url())
            .await
            .unwrap();
    let (_coordinator_b, _signaling_client_b) =
        create_peer_with_websocket(id_peer_b.clone(), &server.url())
            .await
            .unwrap();

    // Establish initial connection
    tracing::info!("🔗 Establishing initial peer connection...");
    let ready_rx = coordinator_a
        .initiate_connection(&id_peer_b)
        .await
        .expect("initiate failed");

    match tokio::time::timeout(Duration::from_secs(10), ready_rx).await {
        Ok(Ok(_)) => {
            tracing::info!("✅ Initial peer connection established!");
        }
        Ok(Err(_)) => panic!("Connection failed (channel closed)"),
        Err(_) => panic!("Connection timed out"),
    }

    // Wait for connection to stabilize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create processor
    let processor = Arc::new(DefaultNetworkEventProcessor::new(
        signaling_client_a.clone(),
        Some(coordinator_a.clone()),
    ));

    // Step 1: Manual cleanup (simulating app going to background)
    tracing::info!("📱 Simulating app going to background - calling cleanup_connections()...");
    let result = processor.cleanup_connections().await;
    assert!(result.is_ok(), "cleanup should succeed");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !signaling_client_a.is_connected(),
        "Should be disconnected after cleanup"
    );

    // Step 2: Trigger network available event (simulating app coming back from background)
    tracing::info!("📱 Simulating app returning from background - triggering network available...");
    server.reset_counters();

    let result = processor.process_network_available().await;
    tracing::info!("📊 Network available result: {:?}", result);
    assert!(
        result.is_ok(),
        "Network available should succeed after cleanup"
    );

    // Verify reconnection
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        signaling_client_a.is_connected(),
        "Should be reconnected after network available"
    );

    // Note: After cleanup_connections closes all peer connections,
    // process_network_available cannot perform ICE restart (no existing connections).
    // The application needs to re-initiate connections manually.
    // This test primarily verifies that cleanup doesn't break the signaling layer.

    // Step 3: Manually re-establish connection to verify system is healthy
    tracing::info!("🔄 Re-establishing connection after cleanup...");
    let ready_rx = coordinator_a
        .initiate_connection(&id_peer_b)
        .await
        .expect("re-initiate failed");

    match tokio::time::timeout(Duration::from_secs(10), ready_rx).await {
        Ok(Ok(_)) => {
            tracing::info!("✅ Connection re-established successfully!");
        }
        Ok(Err(_)) => panic!("Re-connection failed (channel closed)"),
        Err(_) => panic!("Re-connection timed out"),
    }

    tracing::info!("✅ Cleanup then network events test passed!");
}
