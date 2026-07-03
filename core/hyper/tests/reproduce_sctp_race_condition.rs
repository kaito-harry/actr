//! Reproduce SCTP race condition
//!
//! This test runs on **unfixed** code, aiming to reproduce:
//! 1. After ICE restart, ICE layer reports Connected
//! 2. Application layer immediately sends RPC request
//! 3. SCTP layer is not ready yet, causing write failure
//! 4. Error: No route to host (os error 65) or Connection closed
//!
//! Expected results:
//! - Unfixed code: test fails, SCTP errors detected
//! - Fixed code: test passes, no SCTP errors

use actr_hyper::{
    outbound::PeerGate,
    test_support::{
        TestSignalingServer, create_peer_with_websocket, make_actor_id, spawn_echo_responder,
        spawn_response_receiver,
    },
    transport::{
        ConnectionEvent, ConnectionState, DefaultWireBuilder, DefaultWireBuilderConfig,
        PeerTransport,
    },
};
use actr_protocol::RpcEnvelope;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_reproduce_sctp_race_after_ice_restart() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .with_line_number(true)
        .with_file(true)
        .try_init()
        .ok();

    tracing::warn!("");
    tracing::warn!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    tracing::warn!("SCTP race condition reproduction test");
    tracing::warn!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    tracing::warn!("Goal: send RPC immediately after ICE restart to trigger SCTP error");
    tracing::warn!("");

    // Start signaling server
    let server = TestSignalingServer::start().await.unwrap();

    let id_a = make_actor_id(100);
    let id_b = make_actor_id(200);

    // Create two peers
    tracing::info!("Creating peers...");
    let (coord_a, _client_a) = create_peer_with_websocket(id_a.clone(), &server.url())
        .await
        .unwrap();
    let (coord_b, _client_b) = create_peer_with_websocket(id_b.clone(), &server.url())
        .await
        .unwrap();

    // Create PeerGate (for sending RPC)
    let wire_config_a = DefaultWireBuilderConfig::default();
    let wire_builder_a: Arc<dyn actr_hyper::transport::WireBuilder> = Arc::new(
        DefaultWireBuilder::new(Some(coord_a.clone()), wire_config_a),
    );
    let transport_mgr_a = Arc::new(PeerTransport::new(id_a.clone(), wire_builder_a));
    let gate_a = Arc::new(PeerGate::new(transport_mgr_a, Some(coord_a.clone())));

    // Create PeerGate for peer B (for responding)
    let wire_config_b = DefaultWireBuilderConfig::default();
    let wire_builder_b: Arc<dyn actr_hyper::transport::WireBuilder> = Arc::new(
        DefaultWireBuilder::new(Some(coord_b.clone()), wire_config_b),
    );
    let transport_mgr_b = Arc::new(PeerTransport::new(id_b.clone(), wire_builder_b));
    let gate_b = Arc::new(PeerGate::new(transport_mgr_b, Some(coord_b.clone())));

    // Start peer B's echo responder task
    let responder_task = spawn_echo_responder(coord_b.clone(), gate_b.clone(), "Peer 200");

    // Start peer A's response receiver task
    let receiver_task = spawn_response_receiver(coord_a.clone(), gate_a.clone(), "Peer 100");

    // Establish initial connection
    tracing::info!("Establishing initial connection...");
    let ready_rx = coord_a.initiate_connection(&id_b).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), ready_rx)
        .await
        .expect("Connection timeout")
        .expect("Connection failed");

    tracing::info!("Initial connection established");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe to connection events
    let mut event_rx = coord_a.subscribe_events();

    // Trigger ICE restart
    tracing::warn!("");
    tracing::warn!("Triggering ICE restart (simulating network change)...");
    coord_a.restart_ice(&id_b).await.unwrap();

    // Monitor connection state and send immediately after Connected
    let mut send_attempts = 0;
    let max_attempts = 3;
    let mut errors_detected = Vec::new();

    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                if let ConnectionEvent::StateChanged { peer_id, state, .. } = event {
                    if peer_id == id_b && state == ConnectionState::Connected {
                        send_attempts += 1;

                        tracing::warn!("");
                        tracing::warn!("Attempt {}/{}: ICE Connected detected!", send_attempts, max_attempts);
                        tracing::warn!("   Sending RPC requests immediately (without waiting)...");

                        // Send multiple RPC requests immediately (increase trigger probability)
                        for i in 0..10 {
                            let envelope = RpcEnvelope {
                                request_id: format!("test-{}-{}", send_attempts, i),
                                route_key: "test.ping".to_string(),
                                payload: Some(bytes::Bytes::from(format!("attempt {} msg {}", send_attempts, i))),
                                direction: Some(actr_protocol::Direction::Request as i32),
                                timeout_ms: 5000,
                                ..Default::default()
                            };

                            // Send via Gate (public API)
                            let result = gate_a.send_request(&id_b, envelope).await;

                            match result {
                                Ok(_) => {
                                    tracing::debug!("   Message {} sent successfully", i);
                                }
                                Err(e) => {
                                    let err_str = e.to_string();
                                    tracing::error!("   Message {} failed: {}", i, err_str);

                                    // Check if it's an SCTP-related error
                                    if err_str.contains("No route to host")
                                        || err_str.contains("os error 65")
                                        || err_str.contains("Connection closed")
                                        || err_str.contains("Transport error")
                                        || err_str.contains("EHOSTUNREACH") {
                                        errors_detected.push((send_attempts, i, err_str.clone()));
                                        tracing::error!("   SCTP-related error detected!");
                                    }
                                }
                            }

                            // Very short delay (simulate rapid sending)
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }

                        if send_attempts >= max_attempts {
                            break;
                        }
                    }
                }
            }
            _ = &mut timeout => {
                tracing::warn!("Timeout reached");
                break;
            }
        }
    }

    // Analyze results
    tracing::warn!("");
    tracing::warn!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    tracing::warn!("Test result analysis");
    tracing::warn!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    tracing::warn!("Send attempts: {}", send_attempts);
    tracing::warn!("Errors detected: {}", errors_detected.len());

    if !errors_detected.is_empty() {
        tracing::error!("");
        tracing::error!("SCTP race condition reproduced!");
        tracing::error!("");
        tracing::error!("Error details:");
        for (attempt, msg_id, error) in &errors_detected {
            tracing::error!("  - Attempt {}, Message {}: {}", attempt, msg_id, error);
        }
        tracing::error!("");
        tracing::error!("Root cause analysis:");
        tracing::error!("  ICE layer reports Connected, but SCTP layer is not ready yet");
        tracing::error!("  Application layer sends data immediately, causing write failure");
        tracing::error!("");
        tracing::error!("Solution:");
        tracing::error!("  Wait for DataChannel Open after ICE Connected");
        tracing::error!("  Only mark connection as usable after SCTP layer is fully ready");
        tracing::error!("");

        panic!(
            "SCTP race condition reproduced: {} errors detected",
            errors_detected.len()
        );
    } else {
        tracing::warn!("");
        tracing::warn!("Could not reproduce SCTP race condition");
        tracing::warn!("");
        tracing::warn!("Possible reasons:");
        tracing::warn!("  1. Local network too fast (localhost), race window too small");
        tracing::warn!("  2. Timing not precise enough (needs more attempts)");
        tracing::warn!("  3. Fix already in effect (if running on fixed code)");
        tracing::warn!("");
        tracing::warn!("Suggestions:");
        tracing::warn!("  - Run test multiple times (10-20 runs)");
        tracing::warn!("  - Test on real network environment");
        tracing::warn!("  - Verify running on unfixed code");
        tracing::warn!("");

        // Note: test passes even if not reproduced (probabilistic issue)
        // But logs clearly indicate the situation
    }

    // Cleanup tasks
    responder_task.abort();
    receiver_task.abort();
}
