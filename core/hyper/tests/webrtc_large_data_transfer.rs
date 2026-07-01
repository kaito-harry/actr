//! Integration test: Large data transfer via WebRTC DataChannel
//!
//! Verifies that the fragmentation / reassembly layer in `WebRtcDataLane`
//! correctly handles payloads that exceed the 64 KB per-message limit of
//! WebRTC DataChannel.
//!
//! Test scenarios:
//! - 100 KB payload  (2 fragments)
//! - 200 KB payload  (4 fragments)
//! - 512 KB payload  (8 fragments)
//! - 1 MB payload    (16 fragments)
//! - Bidirectional large data exchange
//! - Sequential large messages (verify msg_id counter works)

use actr_hyper::outbound::PeerGate;
use actr_hyper::test_support::{TestHarness, make_actor_id, spawn_response_receiver};
use actr_hyper::wire::webrtc::WebRtcCoordinator;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActrId, RpcEnvelope};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

/// Serialize large WebRTC/SCTP integration cases on constrained CI runners.
static WEBRTC_LARGE_DATA_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

struct BackgroundTasks(Vec<tokio::task::JoinHandle<()>>);

impl BackgroundTasks {
    async fn shutdown(mut self) {
        for task in &self.0 {
            task.abort();
        }
        while let Some(task) = self.0.pop() {
            let _ = task.await;
        }
    }
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
}

/// DC_MAX_PAYLOAD_SIZE mirrors the production constant for fragment boundary checks.
const DC_MAX_PAYLOAD_SIZE: usize = 65535 - 8; // 65527

/// Generate deterministic test data of the given size and its SHA-256 hash.
///
/// Uses a repeating byte pattern (`i % 251`, a prime near 256) to avoid
/// false-positive equality from zero-fill or periodic patterns that align
/// with fragment boundaries.
fn generate_test_data(size: usize) -> (Vec<u8>, [u8; 32]) {
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let hash: [u8; 32] = Sha256::digest(&data).into();
    (data, hash)
}

/// Spawn a "data echo" responder on the target peer.
///
/// Unlike the default `start_echo_responder` (which returns a fixed "pong"),
/// this responder echoes back the **full request payload** so that the sender
/// can verify end-to-end data integrity.
fn spawn_data_echo_responder(
    coordinator: Arc<WebRtcCoordinator>,
    gate: Arc<PeerGate>,
    name: &str,
) -> tokio::task::JoinHandle<()> {
    let name = name.to_string();
    tokio::spawn(async move {
        tracing::info!("🎯 {} data-echo responder started", name);
        loop {
            match coordinator.receive_message().await {
                Ok(Some((sender_id_bytes, message_data, _payload_type))) => {
                    let sender_id = match ActrId::decode(&sender_id_bytes[..]) {
                        Ok(id) => id,
                        Err(e) => {
                            tracing::error!("{}: Failed to decode sender ID: {}", name, e);
                            continue;
                        }
                    };

                    match RpcEnvelope::decode(message_data.as_ref()) {
                        Ok(request) => {
                            let payload_len =
                                request.payload.as_ref().map(|p| p.len()).unwrap_or(0);
                            tracing::info!(
                                "📨 {} received request: {} ({} bytes payload)",
                                name,
                                request.request_id,
                                payload_len,
                            );

                            // Echo back the same payload
                            let response = RpcEnvelope {
                                request_id: request.request_id.clone(),
                                route_key: "response".to_string(),
                                payload: request.payload.clone(),
                                timeout_ms: 0,
                                ..Default::default()
                            };

                            if let Err(e) = gate.send_message(&sender_id, response).await {
                                tracing::error!(
                                    "{}: Failed to send echo response for {}: {}",
                                    name,
                                    request.request_id,
                                    e
                                );
                            } else {
                                tracing::info!(
                                    "✅ {} echoed {} bytes for {}",
                                    name,
                                    payload_len,
                                    request.request_id,
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!("{}: Failed to decode RpcEnvelope: {}", name, e);
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!("📪 {} channel closed", name);
                    break;
                }
                Err(e) => {
                    tracing::error!("{}: Error receiving message: {}", name, e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
}

/// Set up a two-peer connection with data-echo responder.
///
/// This replaces `harness.connect()` because `connect()` starts its own
/// "pong" echo responder that would race with our data-echo responder.
///
/// Returns the harness with connected peers. The data-echo responder runs
/// on peer `to_serial`, and the response receiver runs on peer `from_serial`.
async fn setup_connected_peers(from_serial: u64, to_serial: u64) -> (TestHarness, BackgroundTasks) {
    let mut harness = TestHarness::new().await;
    harness.add_peer(from_serial).await;
    harness.add_peer(to_serial).await;

    let mut bg_tasks = Vec::new();

    // Start data-echo responder on target peer (echoes full payload)
    let echo_handle = spawn_data_echo_responder(
        harness.peer(to_serial).coordinator.clone(),
        harness.peer(to_serial).gate.clone(),
        &format!("data_echo_{}", to_serial),
    );
    bg_tasks.push(echo_handle);

    // Start response receiver on source peer
    let recv_handle = spawn_response_receiver(
        harness.peer(from_serial).coordinator.clone(),
        harness.peer(from_serial).gate.clone(),
        &format!("recv_{}", from_serial),
    );
    bg_tasks.push(recv_handle);

    // Send initial small request to establish connection (triggers lazy WebRTC setup)
    let target_id = make_actor_id(to_serial);
    let envelope = RpcEnvelope {
        request_id: format!("setup_connect_{}_{}", from_serial, to_serial),
        route_key: "test.ping".to_string(),
        payload: Some(Bytes::from("ping")),
        timeout_ms: 15_000,
        ..Default::default()
    };

    let gate = harness.peer(from_serial).gate.clone();
    match tokio::time::timeout(
        Duration::from_secs(15),
        gate.send_request(&target_id, envelope),
    )
    .await
    {
        Ok(Ok(response)) => {
            tracing::info!(
                "✅ Connection established: {} → {} (response: {} bytes)",
                from_serial,
                to_serial,
                response.len(),
            );
        }
        Ok(Err(e)) => panic!("Connection {} → {} failed: {}", from_serial, to_serial, e),
        Err(_) => panic!("Connection {} → {} timed out", from_serial, to_serial),
    }

    // Brief stabilization
    tokio::time::sleep(Duration::from_millis(300)).await;

    (harness, BackgroundTasks(bg_tasks))
}

/// Helper: send a large payload and verify the echoed response with multi-layer
/// integrity checks.
///
/// Verification layers (in order):
/// 1. **Length check** — catches truncation or padding
/// 2. **SHA-256 hash** — cryptographic proof of content integrity
/// 3. **Fragment boundary sampling** — checks bytes at fragment seams where
///    reassembly errors are most likely
/// 4. **Head / tail check** — verifies the first and last 64 bytes
/// 5. **Full byte comparison** — final fallback (logs first mismatch offset)
async fn send_and_verify(
    gate: &PeerGate,
    target_id: &ActrId,
    request_id: &str,
    data: &[u8],
    expected_hash: &[u8; 32],
    timeout: Duration,
) {
    let envelope = RpcEnvelope {
        request_id: request_id.to_string(),
        route_key: "test.large_echo".to_string(),
        payload: Some(Bytes::from(data.to_vec())),
        timeout_ms: timeout.as_millis() as i64,
        ..Default::default()
    };

    let result = tokio::time::timeout(timeout, gate.send_request(target_id, envelope))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Timed out sending {} bytes (request_id={})",
                data.len(),
                request_id,
            )
        })
        .unwrap_or_else(|e| {
            panic!(
                "send_request failed for {} bytes (request_id={}): {}",
                data.len(),
                request_id,
                e,
            )
        });

    // ── Layer 1: Length check ──
    assert_eq!(
        result.len(),
        data.len(),
        "[{}] Length mismatch: expected {} bytes, got {} bytes",
        request_id,
        data.len(),
        result.len(),
    );

    // ── Layer 2: SHA-256 hash ──
    let result_hash: [u8; 32] = Sha256::digest(result.as_ref()).into();
    assert_eq!(
        &result_hash,
        expected_hash,
        "[{}] SHA-256 mismatch!\n  expected: {}\n  got:      {}",
        request_id,
        hex_str(expected_hash),
        hex_str(&result_hash),
    );

    // ── Layer 3: Fragment boundary sampling ──
    // Check bytes at positions where fragment reassembly joins occur.
    // Corruption at fragment seams is the most common reassembly bug.
    let boundary_offsets: Vec<usize> = (1..=16)
        .map(|i| i * DC_MAX_PAYLOAD_SIZE)
        .filter(|&off| off < data.len())
        .collect();

    for &offset in &boundary_offsets {
        let window = 4.min(data.len() - offset);
        assert_eq!(
            &result.as_ref()[offset..offset + window],
            &data[offset..offset + window],
            "[{}] Fragment boundary corruption at offset {} (fragment seam)",
            request_id,
            offset,
        );
    }

    // ── Layer 4: Head / tail check ──
    let check_len = 64.min(data.len());
    assert_eq!(
        &result.as_ref()[..check_len],
        &data[..check_len],
        "[{}] Head mismatch in first {} bytes",
        request_id,
        check_len,
    );
    assert_eq!(
        &result.as_ref()[data.len() - check_len..],
        &data[data.len() - check_len..],
        "[{}] Tail mismatch in last {} bytes",
        request_id,
        check_len,
    );

    // ── Layer 5: Full byte comparison (with diagnostic offset on failure) ──
    if result.as_ref() != data {
        let first_diff = result
            .as_ref()
            .iter()
            .zip(data.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(0);
        panic!(
            "[{}] Full byte comparison failed! First mismatch at offset {}: \
             got 0x{:02x}, expected 0x{:02x} (total {} bytes)",
            request_id,
            first_diff,
            result.as_ref()[first_diff],
            data[first_diff],
            data.len(),
        );
    }

    tracing::info!(
        "✅ Verified {} bytes round-trip for {} (SHA-256: {}..)",
        data.len(),
        request_id,
        &hex_str(expected_hash)[..16],
    );
}

/// Format a hash slice as a hex string.
fn hex_str(hash: &[u8; 32]) -> String {
    hash.iter().map(|b| format!("{:02x}", b)).collect()
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test: 100 KB payload (crosses 64 KB fragmentation boundary)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Verify that a 100 KB payload is correctly fragmented, transmitted, and
/// reassembled across the DataChannel.
#[tokio::test]
async fn test_large_data_100kb() {
    init_tracing();
    let _test_guard = WEBRTC_LARGE_DATA_TEST_LOCK.lock().await;
    tracing::info!("═══ test_large_data_100kb ═══");

    let (harness, _bg) = setup_connected_peers(100, 200).await;

    let (data, hash) = generate_test_data(100 * 1024);
    let target_id = make_actor_id(200);
    send_and_verify(
        &harness.peer(100).gate,
        &target_id,
        "large_100kb",
        &data,
        &hash,
        Duration::from_secs(30),
    )
    .await;

    tracing::info!("✅ test_large_data_100kb passed!");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test: 200 KB payload
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_large_data_200kb() {
    init_tracing();
    let _test_guard = WEBRTC_LARGE_DATA_TEST_LOCK.lock().await;
    tracing::info!("═══ test_large_data_200kb ═══");

    let (harness, _bg) = setup_connected_peers(100, 200).await;

    let (data, hash) = generate_test_data(200 * 1024);
    let target_id = make_actor_id(200);
    send_and_verify(
        &harness.peer(100).gate,
        &target_id,
        "large_200kb",
        &data,
        &hash,
        Duration::from_secs(30),
    )
    .await;

    tracing::info!("✅ test_large_data_200kb passed!");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test: 512 KB payload
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_large_data_512kb() {
    init_tracing();
    let _test_guard = WEBRTC_LARGE_DATA_TEST_LOCK.lock().await;
    tracing::info!("═══ test_large_data_512kb ═══");

    let (harness, _bg) = setup_connected_peers(100, 200).await;

    let (data, hash) = generate_test_data(512 * 1024);
    let target_id = make_actor_id(200);
    send_and_verify(
        &harness.peer(100).gate,
        &target_id,
        "large_512kb",
        &data,
        &hash,
        Duration::from_secs(60),
    )
    .await;

    tracing::info!("✅ test_large_data_512kb passed!");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test: 1 MB payload (stress test, marked #[ignore] for CI speed)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
#[ignore = "slow test, run with --include-ignored for stress testing"]
async fn test_large_data_1mb() {
    init_tracing();
    let _test_guard = WEBRTC_LARGE_DATA_TEST_LOCK.lock().await;
    tracing::info!("═══ test_large_data_1mb ═══");

    let (harness, _bg) = setup_connected_peers(100, 200).await;

    let (data, hash) = generate_test_data(1024 * 1024);
    let target_id = make_actor_id(200);
    send_and_verify(
        &harness.peer(100).gate,
        &target_id,
        "large_1mb",
        &data,
        &hash,
        Duration::from_secs(120),
    )
    .await;

    tracing::info!("✅ test_large_data_1mb passed!");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test: Sequential large messages (verify msg_id counter isolation)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Send multiple large payloads in sequence over the same DataChannel
/// to verify that the msg_id counter and reassembly buffer handle
/// consecutive fragmented messages correctly.
#[tokio::test]
async fn test_sequential_large_messages() {
    init_tracing();
    let _test_guard = WEBRTC_LARGE_DATA_TEST_LOCK.lock().await;
    tracing::info!("═══ test_sequential_large_messages ═══");

    let (harness, _bg) = setup_connected_peers(100, 200).await;

    let target_id = make_actor_id(200);
    let sizes = [80 * 1024, 150 * 1024, 100 * 1024, 200 * 1024];

    for (i, &size) in sizes.iter().enumerate() {
        let (data, hash) = generate_test_data(size);
        let req_id = format!("seq_large_{}", i);
        tracing::info!("📤 Sending sequential message {}: {} bytes", i, size);
        send_and_verify(
            &harness.peer(100).gate,
            &target_id,
            &req_id,
            &data,
            &hash,
            Duration::from_secs(30),
        )
        .await;
    }

    tracing::info!("✅ test_sequential_large_messages passed!");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test: Bidirectional large data exchange
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Both peers can send large payloads to each other,
/// verifying that fragmentation/reassembly works correctly in both
/// directions on the same DataChannel pair.
///
/// NOTE: The test uses sequential (not concurrent) direction switching because
/// each peer's `receive_message()` channel is single-consumer. Running both
/// echo_responder and response_receiver on the same peer would cause message
/// stealing. The sequential approach still validates both directions.
#[tokio::test(flavor = "multi_thread")]
async fn test_bidirectional_large_data() {
    init_tracing();
    let _test_guard = WEBRTC_LARGE_DATA_TEST_LOCK.lock().await;
    tracing::info!("═══ test_bidirectional_large_data ═══");

    // ── Direction 1: peer 100 → peer 200 (128 KB) ──
    tracing::info!("📤 Direction 1: peer 100 → peer 200 (128 KB)");
    let (harness, forward_tasks) = setup_connected_peers(100, 200).await;

    let (data_a, hash_a) = generate_test_data(128 * 1024);
    let target_200 = make_actor_id(200);
    send_and_verify(
        &harness.peer(100).gate,
        &target_200,
        "bidir_100_to_200",
        &data_a,
        &hash_a,
        Duration::from_secs(30),
    )
    .await;
    forward_tasks.shutdown().await;

    // ── Direction 2: peer 200 → peer 100 (96 KB, answerer sends) ──
    tracing::info!("📤 Direction 2: peer 200 → peer 100 (96 KB, answerer sends)");

    // Swap the single-consumer message handlers while retaining the same peer
    // connection established above.
    let _reverse_tasks = BackgroundTasks(vec![
        spawn_data_echo_responder(
            harness.peer(100).coordinator.clone(),
            harness.peer(100).gate.clone(),
            "data_echo_100",
        ),
        spawn_response_receiver(
            harness.peer(200).coordinator.clone(),
            harness.peer(200).gate.clone(),
            "recv_200",
        ),
    ]);

    // Verify the reverse request path before sending the large payload.
    let target_100 = make_actor_id(100);
    let setup_env = RpcEnvelope {
        request_id: "bidir2_setup".to_string(),
        route_key: "test.ping".to_string(),
        payload: Some(Bytes::from("ping")),
        timeout_ms: 15_000,
        ..Default::default()
    };
    tokio::time::timeout(
        Duration::from_secs(15),
        harness.peer(200).gate.send_request(&target_100, setup_env),
    )
    .await
    .expect("setup 200→100 timed out")
    .expect("setup 200→100 failed");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let (data_b, hash_b) = generate_test_data(96 * 1024);
    send_and_verify(
        &harness.peer(200).gate,
        &target_100,
        "bidir_200_to_100",
        &data_b,
        &hash_b,
        Duration::from_secs(30),
    )
    .await;

    tracing::info!(
        "✅ test_bidirectional_large_data passed! (128KB + 96KB round-tripped in both directions)"
    );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test: Boundary – exactly under 64KB (single fragment, no split needed)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Verify that a payload small enough to fit in a single fragment
/// (after protobuf serialization adds overhead) travels as one fragment
/// without triggering the multi-fragment path.
#[tokio::test]
async fn test_boundary_max_single_fragment() {
    init_tracing();
    let _test_guard = WEBRTC_LARGE_DATA_TEST_LOCK.lock().await;
    tracing::info!("═══ test_boundary_max_single_fragment ═══");

    let (harness, _bg) = setup_connected_peers(100, 200).await;

    // DC_MAX_PAYLOAD_SIZE = 65535 - 8 = 65527 bytes
    // But the payload goes through protobuf serialization (RpcEnvelope),
    // so the on-wire size is larger than the raw payload.
    // Use 60KB to stay in single-fragment territory for the serialized envelope.
    let (data, hash) = generate_test_data(60 * 1024);
    let target_id = make_actor_id(200);
    send_and_verify(
        &harness.peer(100).gate,
        &target_id,
        "boundary_single_frag",
        &data,
        &hash,
        Duration::from_secs(30),
    )
    .await;

    tracing::info!("✅ test_boundary_max_single_fragment passed!");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test: Boundary – one byte over 64KB (triggers fragmentation)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Verify that a payload just over the single-fragment limit correctly
/// triggers the 2-fragment path.
#[tokio::test]
async fn test_boundary_just_over_single_fragment() {
    init_tracing();
    let _test_guard = WEBRTC_LARGE_DATA_TEST_LOCK.lock().await;
    tracing::info!("═══ test_boundary_just_over_single_fragment ═══");

    let (harness, _bg) = setup_connected_peers(100, 200).await;

    // Use 65KB to ensure the protobuf-serialized envelope exceeds
    // the single-fragment limit and enters the multi-fragment path.
    let (data, hash) = generate_test_data(65 * 1024);
    let target_id = make_actor_id(200);
    send_and_verify(
        &harness.peer(100).gate,
        &target_id,
        "boundary_two_frags",
        &data,
        &hash,
        Duration::from_secs(30),
    )
    .await;

    tracing::info!("✅ test_boundary_just_over_single_fragment passed!");
}
