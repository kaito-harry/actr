//! Reproduce stale DestTransport reuse after a real disconnect.
//!
//! The original production failure needed two ingredients:
//! 1. a real network outage that closes the old WebRTC connection
//! 2. an event storm that can make the old cleanup listener stop
//!
//! This integration test keeps the network failure real (`vnet` disconnect) and
//! uses an event storm only to stress the broadcast channel enough for the old
//! cleanup path to miss the later close event.
//!
//! Note: current `main` now contains session-guarded cleanup, so this
//! file is kept as a manual diagnosis test and is ignored by default. It now
//! answers two direct questions:
//! 1. after a real disconnect + event storm, does the later `StreamReliable`
//!    send still hang on the current branch?
//! 2. does a stale close event avoid deleting the current active transport?
//! 3. does a normal RPC call still recover after the same stream probe?

use actr_hyper::test_support::TestHarness;
use actr_hyper::transport::{ConnectionEvent, ConnectionState, Dest};
use actr_protocol::prost::Message;
use actr_protocol::{DataChunk, PayloadType};
use std::time::Duration;

const SOURCE_SERIAL: u64 = 100;
const TARGET_SERIAL: u64 = 200;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
}

fn spawn_event_storm(
    event_tx: tokio::sync::broadcast::Sender<ConnectionEvent>,
    peer_id: actr_protocol::ActrId,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let bursts = [
            Duration::from_millis(0),
            Duration::from_secs(5),
            Duration::from_secs(10),
        ];

        for delay in bursts {
            tokio::time::sleep(delay).await;

            for seq in 0..256 {
                let event = match seq % 4 {
                    0 => ConnectionEvent::StateChanged {
                        peer_id: peer_id.clone(),
                        session_id: 0,
                        state: ConnectionState::Connecting,
                    },
                    1 => ConnectionEvent::DataChannelOpened {
                        peer_id: peer_id.clone(),
                        session_id: 0,
                        payload_type: PayloadType::RpcSignal,
                    },
                    2 => ConnectionEvent::IceRestartStarted {
                        peer_id: peer_id.clone(),
                        session_id: 0,
                    },
                    _ => ConnectionEvent::NewRoleAssignment {
                        peer_id: peer_id.clone(),
                        is_offerer: seq % 2 == 0,
                    },
                };

                let _ = event_tx.send(event);
            }

            tracing::info!("🌪️ Injected 256 synthetic connection events");
        }
    })
}

fn is_expected_recovery_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "connection",
        "request timeout",
        "timed out",
        "closed",
        "recovering",
        "data channel",
        "datachannel",
        "channel error",
        "not opened",
        "timeout",
        "unavailable",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

async fn expect_request_eventually_ok(harness: &TestHarness, request_id: &str, deadline: Duration) {
    let stop_at = tokio::time::Instant::now() + deadline;
    let mut attempt = 0;

    loop {
        attempt += 1;
        let attempt_id = format!("{request_id}_{attempt}");
        let handle = harness
            .peer(SOURCE_SERIAL)
            .spawn_request(TARGET_SERIAL, &attempt_id, 2_000);

        match tokio::time::timeout(Duration::from_secs(3), handle).await {
            Ok(Ok(Ok(response))) => {
                assert!(
                    !response.is_empty(),
                    "{request_id} should receive a non-empty response"
                );
                return;
            }
            Ok(Ok(Err(err))) => {
                let msg = err.to_string();
                assert!(
                    is_expected_recovery_error(&msg),
                    "{request_id} failed with unexpected error: {msg}"
                );
            }
            Ok(Err(err)) => panic!("{request_id} task panicked: {err}"),
            Err(_) => {}
        }

        if tokio::time::Instant::now() >= stop_at {
            panic!("{request_id} did not recover within {:?}", deadline);
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
#[ignore = "manual diagnosis for post-disconnect stale transport behavior"]
async fn test_real_disconnect_stream_send_recovers_and_stale_close_is_ignored() {
    init_tracing();

    let mut harness = TestHarness::with_vnet().await;
    harness.add_peer(SOURCE_SERIAL).await;
    harness.add_peer(TARGET_SERIAL).await;

    tracing::info!(
        "🔗 Step 1: Establish initial connection {} -> {}",
        SOURCE_SERIAL,
        TARGET_SERIAL
    );
    harness.connect(SOURCE_SERIAL, TARGET_SERIAL).await;

    let source_peer = harness.peer(SOURCE_SERIAL);
    let target_id = harness.peer(TARGET_SERIAL).id.clone();
    let dest = Dest::peer(target_id.clone());

    assert!(
        source_peer.transport_manager.has_dest(&dest).await,
        "expected initial DestTransport cache entry"
    );

    tracing::info!("🔴 Step 2: Real disconnect + broadcast storm");
    let storm_handle = spawn_event_storm(source_peer.coordinator.event_sender(), target_id.clone());
    harness.simulate_disconnect();
    tokio::time::sleep(Duration::from_secs(15)).await;
    storm_handle.await.expect("event storm task should finish");

    let dest_still_cached = source_peer.transport_manager.has_dest(&dest).await;
    let cached_dests = source_peer.transport_manager.list_dests().await;
    tracing::info!("📦 Cached DestTransports after outage: {:?}", cached_dests);

    tracing::info!(
        "🧭 After outage, stale transport cached before reconnect = {}",
        dest_still_cached
    );

    tracing::info!(
        "🟢 Step 3: Restore network and probe actual send path before any sentinel cleanup"
    );
    harness.simulate_reconnect();
    tokio::time::sleep(Duration::from_secs(5)).await;

    tracing::info!("📤 Step 4: Probe StreamReliable through the public gate path");
    let stream = DataChunk {
        stream_id: "stale-transport-stream".to_string(),
        sequence: 1,
        payload: bytes::Bytes::from_static(b"stream-payload"),
        metadata: Vec::new(),
        timestamp_ms: Some(0),
    };
    let payload = bytes::Bytes::from(stream.encode_to_vec());
    let send_fut = source_peer.gate.send_data_chunk(
        &target_id,
        PayloadType::StreamReliable,
        &stream.stream_id,
        payload,
    );

    match tokio::time::timeout(Duration::from_secs(5), send_fut).await {
        Ok(Ok(())) => {
            tracing::info!("✅ StreamReliable send succeeded after reconnect");
        }
        Ok(Err(e)) => {
            tracing::info!("ℹ️ StreamReliable send failed fast after reconnect: {}", e);
        }
        Err(_) => {
            panic!("StreamReliable send still hung for 5s after reconnect");
        }
    }

    tracing::info!("📤 Step 5: Probe normal RPC call after StreamReliable probe");
    expect_request_eventually_ok(
        &harness,
        "stale-transport-after-stream-call",
        Duration::from_secs(15),
    )
    .await;

    tracing::info!("🧪 Step 6: Send stale ConnectionClosed sentinel");
    let receiver_count = source_peer
        .coordinator
        .event_sender()
        .send(ConnectionEvent::ConnectionClosed {
            peer_id: target_id.clone(),
            session_id: 0,
        })
        .expect("sentinel event should be broadcast");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let dest_after_sentinel = source_peer.transport_manager.has_dest(&dest).await;
    tracing::info!(
        "🧪 Stale ConnectionClosed delivered to {} receivers, dest still cached={}",
        receiver_count,
        dest_after_sentinel
    );

    assert!(
        receiver_count > 0,
        "expected stale sentinel close event to reach at least one receiver"
    );
    assert!(
        dest_after_sentinel,
        "stale session close event must not clean the active transport"
    );
}
