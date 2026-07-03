//! Retry + exponential backoff timing tests for `PeerGate::send_with_retry`.
//!
//! # What is tested
//!
//! `PeerGate::send_with_retry` (called by `send_message_with_type`) implements
//! per-`PayloadType` caller-level retry with exponential backoff:
//!
//! ```text
//! delay(0) = initial_delay
//! delay(n) = min(delay(n-1) * 2, max_delay)
//! ```
//!
//! Retry only triggers for `is_retryable()` errors (`ErrorKind::Transient`).
//! Non-transient errors (`Client`, `Internal`, `Corrupt`) are returned immediately
//! without consuming any retry budget.
//!
//! # Retry policy (from `route_table.rs`)
//!
//! | PayloadType    | max_attempts | initial_delay | max_delay |
//! |----------------|-------------|---------------|-----------|
//! | RpcSignal      | 2           | 500 ms        | 500 ms    |
//! | RpcReliable    | 5           | 1 s           | 5 s       |
//! | Stream / Media | 1 (no retry)| —             | —         |
//!
//! # Mock architecture
//!
//! `WireBuilder` is injected into `PeerTransport`, which is wrapped by `PeerGate`.
//! A `FailingWireBuilder` returns `NetworkError::ConnectionError` (Transient) for
//! the first `fail_count` calls and a success `WireHandle` thereafter.
//!
//! `NetworkError::ConnectionError` maps to `ErrorKind::Transient`, so
//! `send_with_retry` retries the send after the configured backoff delay.
//!
//! # DLQ dispatch wiring status (2026-04-27)
//!
//! Transport-level retry path (`peer_gate.rs`) does NOT call `dlq.enqueue`.
//! Only the mailbox dispatch loop (`node.rs`) writes to the DLQ on decode
//! failure. Tracking: "transport-level DLQ routing" (not yet implemented).

use actr_hyper::outbound::PeerGate;
use actr_hyper::transport::{
    ConnType, DataLane, NetworkError, NetworkResult, PeerTransport, WireBuilder, WireHandle,
};
use actr_protocol::{ActrId, ActrType, PayloadType, Realm, RpcEnvelope};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

// ── helpers ───────────────────────────────────────────────────────────────────

fn test_actor_id() -> ActrId {
    ActrId {
        realm: Realm { realm_id: 1 },
        serial_number: 42,
        r#type: ActrType {
            manufacturer: "test".to_string(),
            name: "retry-target".to_string(),
            version: "0.1.0".to_string(),
        },
    }
}

fn local_actor_id() -> ActrId {
    ActrId {
        realm: Realm { realm_id: 1 },
        serial_number: 1,
        r#type: ActrType {
            manufacturer: "test".to_string(),
            name: "retry-sender".to_string(),
            version: "0.1.0".to_string(),
        },
    }
}

fn signal_envelope() -> RpcEnvelope {
    RpcEnvelope {
        request_id: Uuid::new_v4().to_string(),
        route_key: "test/noop".to_string(),
        payload: Some(vec![0u8; 4].into()),
        direction: Some(actr_protocol::Direction::Request as i32),
        timeout_ms: 5000,
        ..Default::default()
    }
}

// ── mock: WireHandle that always succeeds ─────────────────────────────────────

/// Minimal `WireHandle` whose `connect()` succeeds immediately and whose
/// `get_lane()` returns a no-op `DataLane`.
#[derive(Debug)]
struct AlwaysReadyWire;

#[async_trait]
impl WireHandle for AlwaysReadyWire {
    fn connection_type(&self) -> ConnType {
        ConnType::WebSocket
    }

    fn priority(&self) -> u8 {
        10
    }

    async fn connect(&self) -> NetworkResult<()> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        true
    }

    async fn close(&self) -> NetworkResult<()> {
        Ok(())
    }

    async fn get_lane(&self, _payload_type: PayloadType) -> NetworkResult<Arc<dyn DataLane>> {
        Ok(Arc::new(NoopLane))
    }
}

// ── mock: DataLane that always succeeds ──────────────────────────────────────

#[derive(Debug)]
struct NoopLane;

#[async_trait]
impl DataLane for NoopLane {
    fn lane_type(&self) -> &'static str {
        "Noop"
    }

    async fn send(&self, _data: Bytes) -> NetworkResult<()> {
        Ok(())
    }
}

// ── mock: WireBuilder that fails `fail_count` times then succeeds ─────────────

/// A `WireBuilder` that:
/// - Records the virtual-time `Instant` of each call in `call_times`
/// - Returns `NetworkError::ConnectionError` for the first `fail_count` calls
/// - Returns an `AlwaysReadyWire` on subsequent calls
#[derive(Clone)]
struct ControlledWireBuilder {
    fail_count: u32,
    calls: Arc<AtomicU32>,
    call_times: Arc<std::sync::Mutex<Vec<Instant>>>,
}

impl ControlledWireBuilder {
    fn new(fail_count: u32) -> Self {
        Self {
            fail_count,
            calls: Arc::new(AtomicU32::new(0)),
            call_times: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl WireBuilder for ControlledWireBuilder {
    async fn create_connections(
        &self,
        _dest: &actr_hyper::transport::Dest,
    ) -> NetworkResult<Vec<Arc<dyn WireHandle>>> {
        let call_no = self.calls.fetch_add(1, Ordering::Relaxed);
        {
            let mut times = self.call_times.lock().unwrap();
            times.push(Instant::now());
        }
        if call_no < self.fail_count {
            Err(NetworkError::ConnectionError(format!(
                "simulated transient failure #{call_no}"
            )))
        } else {
            Ok(vec![Arc::new(AlwaysReadyWire)])
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// `RpcSignal` retry policy: max_attempts=2, initial_delay=500ms, max_delay=500ms.
///
/// When `WireBuilder` always fails:
/// - Attempt 1 fails (Transient) → backoff 500 ms → attempt 2
/// - Attempt 2 fails (remaining=0) → returns Err
///
/// Total virtual time elapsed must be ≥ 500 ms (one backoff interval).
#[tokio::test(start_paused = true)]
async fn rpc_signal_retries_once_then_fails_with_correct_backoff() {
    let builder = ControlledWireBuilder::new(u32::MAX); // always fail
    let call_times_ref = Arc::clone(&builder.call_times);

    let transport = Arc::new(PeerTransport::new(local_actor_id(), Arc::new(builder)));
    let gate = PeerGate::new(transport, None);

    let start = Instant::now();
    let result = gate
        .send_message_with_type(&test_actor_id(), PayloadType::RpcSignal, signal_envelope())
        .await;
    let elapsed = start.elapsed();

    // Must fail (exhausted retries)
    assert!(
        result.is_err(),
        "send must fail when all attempts are exhausted"
    );

    // Exactly 2 calls to create_connections (1 initial + 1 retry)
    let times = call_times_ref.lock().unwrap();
    assert_eq!(
        times.len(),
        2,
        "RpcSignal must trigger exactly 1 retry (2 total attempts)"
    );

    // Backoff between attempt 1 and attempt 2 must be ≥ 500 ms
    let gap = times[1].duration_since(times[0]);
    assert!(
        gap >= Duration::from_millis(490), // allow 10ms tolerance for timing precision
        "backoff gap must be ≥ 500 ms, got {:?}",
        gap
    );

    // Total elapsed reflects the one 500ms backoff wait
    assert!(
        elapsed >= Duration::from_millis(490),
        "total elapsed must reflect the 500ms backoff, got {:?}",
        elapsed
    );
}

/// Non-retryable errors (`NetworkError::ConfigurationError` → `ErrorKind::Client`)
/// must be returned immediately without consuming any retry budget.
///
/// No backoff delay should be incurred regardless of `PayloadType`.
#[tokio::test(start_paused = true)]
async fn client_error_is_not_retried() {
    /// `WireBuilder` that always returns a Client (non-retryable) error.
    struct ClientErrorBuilder;

    #[async_trait]
    impl WireBuilder for ClientErrorBuilder {
        async fn create_connections(
            &self,
            _dest: &actr_hyper::transport::Dest,
        ) -> NetworkResult<Vec<Arc<dyn WireHandle>>> {
            Err(NetworkError::ConfigurationError(
                "bad config — not retryable".to_string(),
            ))
        }
    }

    let transport = Arc::new(PeerTransport::new(
        local_actor_id(),
        Arc::new(ClientErrorBuilder),
    ));
    let gate = PeerGate::new(transport, None);

    let start = Instant::now();
    let result = gate
        .send_message_with_type(
            &test_actor_id(),
            PayloadType::RpcReliable,
            signal_envelope(),
        )
        .await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "Client error must propagate as Err");

    // No backoff must have occurred: elapsed should be < 100ms (essentially zero
    // virtual time consumed because no sleep was hit)
    assert!(
        elapsed < Duration::from_millis(100),
        "Client error must not trigger any backoff delay, elapsed={:?}",
        elapsed
    );
}

/// `RpcReliable` retry policy: max_attempts=5, initial_delay=1s, max_delay=5s.
///
/// With 2 consecutive failures followed by success:
/// - Attempt 1 fails → backoff 1 s → attempt 2
/// - Attempt 2 fails → backoff 2 s → attempt 3
/// - Attempt 3 → `WireBuilder` returns `AlwaysReadyWire` → send succeeds
///
/// Total backoff must be ≥ 3 s (1 s + 2 s).
///
/// Note: after `get_or_create_transport` succeeds, `DestTransport::send` waits
/// for `WirePool` to become Ready (via watch channel). In `start_paused` mode,
/// `tokio::spawn` tasks run when the executor polls them after each `await`.
/// We yield after starting the send to let the background connect task complete.
#[tokio::test(start_paused = true)]
async fn rpc_reliable_two_transient_failures_then_success() {
    let builder = ControlledWireBuilder::new(2); // fail twice, succeed on 3rd call
    let calls_ref = Arc::clone(&builder.calls);
    let call_times_ref = Arc::clone(&builder.call_times);

    let transport = Arc::new(PeerTransport::new(local_actor_id(), Arc::new(builder)));
    let gate = Arc::new(PeerGate::new(transport, None));

    let start = Instant::now();

    // Drive the send in a spawned task so the test executor can interleave
    // the background WirePool connection task with our main task.
    let gate2 = Arc::clone(&gate);
    let target = test_actor_id();
    let result = gate2
        .send_message_with_type(&target, PayloadType::RpcReliable, signal_envelope())
        .await;

    let elapsed = start.elapsed();

    assert!(
        result.is_ok(),
        "send must succeed after 2 transient failures: {:?}",
        result
    );

    // 3 calls total: 2 failures + 1 success
    assert_eq!(
        calls_ref.load(Ordering::Relaxed),
        3,
        "WireBuilder must be called exactly 3 times"
    );

    // Verify backoff timing: gap[0→1] ≥ 1s, gap[1→2] ≥ 2s
    let times = call_times_ref.lock().unwrap();
    assert_eq!(times.len(), 3);

    let gap_0_1 = times[1].duration_since(times[0]);
    assert!(
        gap_0_1 >= Duration::from_millis(990),
        "first backoff must be ≥ 1s, got {:?}",
        gap_0_1
    );

    let gap_1_2 = times[2].duration_since(times[1]);
    assert!(
        gap_1_2 >= Duration::from_millis(1990),
        "second backoff must be ≥ 2s (exponential), got {:?}",
        gap_1_2
    );

    // Total elapsed ≥ 1s + 2s = 3s
    assert!(
        elapsed >= Duration::from_millis(2990),
        "total elapsed must be ≥ 3s, got {:?}",
        elapsed
    );
}
