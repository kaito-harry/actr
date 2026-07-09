//! Reproduce stale WebRTC send-path reuse on the answerer after the peer fails.
//!
//! The production failure was not that signaling stopped. The narrower contract
//! is that after the answerer observes WebRTC `Disconnected`/`Failed`, new
//! outbound RPCs must not continue to use the old cached `DestTransport` or
//! DataChannel just because local ready state still looks open.

use actr_hyper::test_support::TestHarness;
use actr_hyper::transport::{ConnectionEvent, ConnectionState, Dest};
use actr_protocol::{ActrId, PayloadType, RpcEnvelope};
use std::time::Duration;

const OFFERER_SERIAL: u64 = 100;
const ANSWERER_SERIAL: u64 = 200;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
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
            "timed out waiting for {:?} DataChannelOpened for peer {}",
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
                tracing::warn!("connection event receiver lagged by {} events", n);
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("connection event channel closed while waiting for DataChannelOpened");
            }
            Err(_) => {
                panic!(
                    "timed out waiting for {:?} DataChannelOpened for peer {}",
                    payload_type, peer_id
                );
            }
        }
    }
}

#[tokio::test]
async fn answerer_failed_peer_does_not_keep_sending_on_stale_webrtc() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(OFFERER_SERIAL).await;
    harness.add_peer(ANSWERER_SERIAL).await;

    let offerer_id = harness.peer(OFFERER_SERIAL).id.clone();
    let offerer_dest = Dest::peer(offerer_id.clone());
    let mut answerer_events = harness.peer(ANSWERER_SERIAL).subscribe_events();

    tracing::info!("Step 1: establish initial WebRTC transport via offerer -> answerer");
    harness.connect(OFFERER_SERIAL, ANSWERER_SERIAL).await;

    let session_id = wait_for_data_channel_opened(
        &mut answerer_events,
        &offerer_id,
        PayloadType::RpcReliable,
        Duration::from_secs(5),
    )
    .await;

    let answerer_peer = harness.peer(ANSWERER_SERIAL);
    assert!(
        answerer_peer
            .transport_manager
            .has_dest(&offerer_dest)
            .await,
        "answerer should have cached a DestTransport to the offerer after sending the connect response"
    );

    tracing::info!(
        "Step 2: answerer observes offerer side as Failed, session_id={}",
        session_id
    );
    answerer_peer.send_event(ConnectionEvent::StateChanged {
        peer_id: offerer_id.clone(),
        session_id,
        state: ConnectionState::Failed,
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    let _offerer_echo = harness
        .peer(OFFERER_SERIAL)
        .start_echo_responder("offerer_echo_for_answerer_probe");
    let _answerer_receiver = answerer_peer.start_response_receiver("answerer_probe_receiver");

    let envelope = RpcEnvelope {
        request_id: "answerer-failed-peer-should-not-use-stale-webrtc".to_string(),
        route_key: "test.ping".to_string(),
        payload: Some(bytes::Bytes::from_static(b"answerer-ping-after-failed")),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 30_000,
        ..Default::default()
    };

    tracing::info!("Step 3: answerer sends after Failed; it must not write to stale WebRTC");
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        answerer_peer.gate.send_request(&offerer_id, envelope),
    )
    .await;

    match result {
        Ok(Err(err)) => {
            let msg = err.to_string();
            assert!(
                msg.contains("connection not ready")
                    || msg.contains("closing")
                    || msg.contains("not ready")
                    || msg.contains("Disconnected")
                    || msg.contains("Failed")
                    || msg.contains("Connection closed"),
                "unexpected fast-fail error after Failed event: {msg}"
            );
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "request should fail fast instead of waiting for the RPC timeout"
            );
        }
        Ok(Ok(_)) => {
            panic!("answerer request unexpectedly succeeded through a peer already marked Failed");
        }
        Err(_) => {
            panic!("answerer request hung for 2s; likely reused stale WebRTC transport");
        }
    }
}

#[tokio::test]
async fn stale_failed_event_does_not_reblock_peer_after_same_session_recovers() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(OFFERER_SERIAL).await;
    harness.add_peer(ANSWERER_SERIAL).await;

    let offerer_id = harness.peer(OFFERER_SERIAL).id.clone();
    let mut answerer_events = harness.peer(ANSWERER_SERIAL).subscribe_events();

    tracing::info!("Step 1: establish initial WebRTC transport via offerer -> answerer");
    harness.connect(OFFERER_SERIAL, ANSWERER_SERIAL).await;

    let session_id = wait_for_data_channel_opened(
        &mut answerer_events,
        &offerer_id,
        PayloadType::RpcReliable,
        Duration::from_secs(5),
    )
    .await;
    let stale_session_id = session_id.saturating_sub(1);

    let answerer_peer = harness.peer(ANSWERER_SERIAL);

    tracing::info!(
        "Step 2: current session enters Failed, then recovers through Connected, session_id={}",
        session_id
    );
    answerer_peer.send_event(ConnectionEvent::StateChanged {
        peer_id: offerer_id.clone(),
        session_id,
        state: ConnectionState::Failed,
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    answerer_peer.send_event(ConnectionEvent::StateChanged {
        peer_id: offerer_id.clone(),
        session_id,
        state: ConnectionState::Connected,
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    tracing::info!(
        "Step 3: delayed Failed from stale session must not reblock peer, stale_session_id={}",
        stale_session_id
    );
    answerer_peer.send_event(ConnectionEvent::StateChanged {
        peer_id: offerer_id.clone(),
        session_id: stale_session_id,
        state: ConnectionState::Failed,
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let envelope = RpcEnvelope {
        request_id: "answerer-send-after-stale-failed-event".to_string(),
        route_key: "test.ping".to_string(),
        payload: Some(bytes::Bytes::from_static(
            b"answerer-ping-after-stale-failed",
        )),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 30_000,
        ..Default::default()
    };

    tracing::info!("Step 4: answerer sends after stale Failed; send should go through");
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        answerer_peer.gate.send_message(&offerer_id, envelope),
    )
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            panic!("answerer send failed after stale Failed event: {err}");
        }
        Err(_) => {
            panic!("answerer send hung after stale Failed event");
        }
    }
}
