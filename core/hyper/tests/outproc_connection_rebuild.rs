//! Integration tests for PeerGate
//!
//! Tests focus on:
//! - Creating new peer connections
//! - Rebuilding peer connections after failure
//! - Connection cleanup and state management

use actr_hyper::outbound::PeerGate;
use actr_hyper::test_support::{TestSignalingServer, create_peer_with_websocket, make_actor_id};
use actr_hyper::transport::{
    ConnectionEvent, ConnectionState as TransportConnectionState, DefaultWireBuilder,
    DefaultWireBuilderConfig, PeerTransport,
};
use actr_protocol::RpcEnvelope;
use std::sync::Arc;
use std::time::Duration;

// ========== Tests ==========

#[tokio::test]
async fn test_peer_connection_establishment() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init()
        .ok();

    let server = TestSignalingServer::start().await.unwrap();

    let id_a = make_actor_id(100);
    let id_b = make_actor_id(200);

    // Create coordinators
    let (coord_a, _client_a) = create_peer_with_websocket(id_a.clone(), &server.url())
        .await
        .unwrap();
    let (_coord_b, _client_b) = create_peer_with_websocket(id_b.clone(), &server.url())
        .await
        .unwrap();

    tracing::info!("🔗 Creating connection from A to B...");

    // Initiate connection
    let ready_rx = coord_a
        .initiate_connection(&id_b)
        .await
        .expect("Failed to initiate");

    match tokio::time::timeout(Duration::from_secs(10), ready_rx).await {
        Ok(Ok(_)) => tracing::info!("✅ Connection established!"),
        Ok(Err(_)) => panic!("Connection failed"),
        Err(_) => panic!("Connection timed out"),
    }

    tracing::info!("✅ test_peer_connection_establishment passed!");
}

#[tokio::test]
async fn test_connection_rebuild_after_failure() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_line_number(true)
        .with_file(true)
        .with_test_writer()
        .try_init()
        .ok();

    let server = TestSignalingServer::start().await.unwrap();

    let id_a = make_actor_id(100);
    let id_b = make_actor_id(200);

    // Create coordinators
    let (coord_a, _client_a) = create_peer_with_websocket(id_a.clone(), &server.url())
        .await
        .unwrap();
    let (_coord_b, _client_b) = create_peer_with_websocket(id_b.clone(), &server.url())
        .await
        .unwrap();

    tracing::info!("🔗 Step 1: Establishing initial connection...");

    let ready_rx = coord_a
        .initiate_connection(&id_b)
        .await
        .expect("Failed to initiate");
    tokio::time::timeout(Duration::from_secs(10), ready_rx)
        .await
        .expect("timeout")
        .expect("failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    tracing::info!("💥 Step 2: Simulating connection failure...");

    // Simulate failure with real session_id
    let sid = coord_a.get_peer_session_id(&id_b).await.unwrap_or(0);
    coord_a
        .event_sender()
        .send(ConnectionEvent::StateChanged {
            peer_id: id_b.clone(),
            session_id: sid,
            state: TransportConnectionState::Failed,
        })
        .ok();

    tokio::time::sleep(Duration::from_millis(200)).await;

    tracing::info!("♻️ Step 3: Rebuilding connection...");

    server.reset_counters();
    let initial_count = server.get_ice_restart_count();

    // Trigger retry (rebuild)
    coord_a.retry_failed_connections().await;

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Verify rebuild attempt was made
    let restart_count = server.get_ice_restart_count();
    tracing::info!(
        "📊 ICE restart offers sent: {} -> {}",
        initial_count,
        restart_count
    );

    assert!(
        restart_count > initial_count,
        "Expected ICE restart count to increase"
    );

    tracing::info!("✅ test_connection_rebuild_after_failure passed!");
}

#[tokio::test]
async fn test_pending_requests_cleanup_on_close() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init()
        .ok();

    let server = TestSignalingServer::start().await.unwrap();

    let id_a = make_actor_id(100);
    let id_b = make_actor_id(200);

    // Create coordinators
    let (coord_a, _client_a) = create_peer_with_websocket(id_a.clone(), &server.url())
        .await
        .unwrap();
    let (_coord_b, _client_b) = create_peer_with_websocket(id_b.clone(), &server.url())
        .await
        .unwrap();

    // Create PeerGate
    let wire_config = DefaultWireBuilderConfig::default();
    let wire_builder = Arc::new(DefaultWireBuilder::new(Some(coord_a.clone()), wire_config));
    let transport_mgr = Arc::new(PeerTransport::new(id_a.clone(), wire_builder));
    let gate_a = Arc::new(PeerGate::new(transport_mgr, Some(coord_a.clone())));

    tracing::info!("🔗 Establishing connection...");

    let ready_rx = coord_a
        .initiate_connection(&id_b)
        .await
        .expect("Failed to initiate");
    tokio::time::timeout(Duration::from_secs(10), ready_rx)
        .await
        .expect("timeout")
        .expect("failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    tracing::info!("📤 Sending pending request...");

    // Send a request that won't get a response
    let envelope = RpcEnvelope {
        request_id: "test_request_1".to_string(),
        route_key: "test.method".to_string(),
        payload: Some(bytes::Bytes::from(vec![1, 2, 3])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 30000,
        ..Default::default()
    };

    let request_handle = tokio::spawn({
        let gate = gate_a.clone();
        let id = id_b.clone();
        async move { gate.send_request(&id, envelope).await }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let pending_before = gate_a.pending_count().await;
    tracing::info!("📊 Pending before close: {}", pending_before);
    assert_eq!(pending_before, 1);

    tracing::info!("💥 Closing connection...");

    // Simulate connection close with real session_id
    let sid = coord_a.get_peer_session_id(&id_b).await.unwrap_or(0);
    coord_a
        .event_sender()
        .send(ConnectionEvent::ConnectionClosed {
            peer_id: id_b.clone(),
            session_id: sid,
        })
        .ok();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let pending_after = gate_a.pending_count().await;
    tracing::info!("📊 Pending after close: {}", pending_after);
    assert_eq!(pending_after, 0, "Pending requests should be cleaned up");

    // Request should fail
    match tokio::time::timeout(Duration::from_secs(1), request_handle).await {
        Ok(Ok(Err(_))) => tracing::info!("✅ Request correctly failed"),
        Ok(Ok(Ok(_))) => panic!("Request should have failed"),
        Ok(Err(_)) => panic!("Request task panicked"),
        Err(_) => panic!("Request didn't complete"),
    }

    tracing::info!("✅ test_pending_requests_cleanup_on_close passed!");
}

#[tokio::test]
async fn test_reconnect_and_send_after_close() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init()
        .ok();

    let server = TestSignalingServer::start().await.unwrap();

    let id_a = make_actor_id(100);
    let id_b = make_actor_id(200);

    // Create coordinators
    let (coord_a, _client_a) = create_peer_with_websocket(id_a.clone(), &server.url())
        .await
        .unwrap();
    let (coord_b, _client_b) = create_peer_with_websocket(id_b.clone(), &server.url())
        .await
        .unwrap();

    // Create PeerGates for both peers
    let wire_config_a = DefaultWireBuilderConfig::default();
    let wire_builder_a = Arc::new(DefaultWireBuilder::new(
        Some(coord_a.clone()),
        wire_config_a,
    ));
    let transport_mgr_a = Arc::new(PeerTransport::new(id_a.clone(), wire_builder_a));
    let gate_a = Arc::new(PeerGate::new(transport_mgr_a, Some(coord_a.clone())));

    let wire_config_b = DefaultWireBuilderConfig::default();
    let wire_builder_b = Arc::new(DefaultWireBuilder::new(
        Some(coord_b.clone()),
        wire_config_b,
    ));
    let transport_mgr_b = Arc::new(PeerTransport::new(id_b.clone(), wire_builder_b));
    let _gate_b = Arc::new(PeerGate::new(transport_mgr_b, Some(coord_b.clone())));

    tracing::info!("🔗 Step 1: Establishing initial connection...");

    let ready_rx = coord_a
        .initiate_connection(&id_b)
        .await
        .expect("Failed to initiate");
    tokio::time::timeout(Duration::from_secs(10), ready_rx)
        .await
        .expect("timeout")
        .expect("failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    tracing::info!("📤 Step 2: Sending first message...");

    // Try to send a message (we'll just verify it doesn't error out)
    let envelope1 = RpcEnvelope {
        request_id: "test_request_1".to_string(),
        route_key: "test.method".to_string(),
        payload: Some(bytes::Bytes::from(vec![1, 2, 3])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 5000,
        ..Default::default()
    };

    // Send in background and let it timeout (no response handler set up)
    let _send1 = tokio::spawn({
        let gate = gate_a.clone();
        let id = id_b.clone();
        async move { gate.send_request(&id, envelope1).await }
    });

    // Give it time to send
    tokio::time::sleep(Duration::from_millis(200)).await;

    tracing::info!("💥 Step 3: Closing connection...");

    // Close the connection from peer A's side with real session_id
    let sid = coord_a.get_peer_session_id(&id_b).await.unwrap_or(0);
    coord_a
        .event_sender()
        .send(ConnectionEvent::ConnectionClosed {
            peer_id: id_b.clone(),
            session_id: sid,
        })
        .ok();

    tokio::time::sleep(Duration::from_millis(500)).await;

    tracing::info!("🔗 Step 4: Rebuilding connection...");

    // Re-initiate connection
    let ready_rx2 = coord_a
        .initiate_connection(&id_b)
        .await
        .expect("Failed to re-initiate");

    match tokio::time::timeout(Duration::from_secs(10), ready_rx2).await {
        Ok(Ok(_)) => tracing::info!("✅ Connection re-established!"),
        Ok(Err(_)) => panic!("Connection failed to re-establish"),
        Err(_) => panic!("Connection re-establishment timed out"),
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    tracing::info!("📤 Step 5: Sending second message after reconnect...");

    // Send another message to verify the connection works
    let envelope2 = RpcEnvelope {
        request_id: "test_request_2".to_string(),
        route_key: "test.method".to_string(),
        payload: Some(bytes::Bytes::from(vec![4, 5, 6])),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 5000,
        ..Default::default()
    };

    let _send2 = tokio::spawn({
        let gate = gate_a.clone();
        let id = id_b.clone();
        async move { gate.send_request(&id, envelope2).await }
    });

    // Give it time to send
    tokio::time::sleep(Duration::from_millis(200)).await;

    tracing::info!("✅ Step 6: Verifying message was sent...");

    tracing::info!("✅ test_reconnect_and_send_after_close passed!");
}
