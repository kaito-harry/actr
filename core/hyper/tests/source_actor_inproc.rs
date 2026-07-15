//! Source Actor integration test — Shell <-> Workload via HostGate (inproc)
//!
//! Verifies the complete bidirectional communication path between Shell and
//! Workload using HostGate + HostTransport without any network transport.
//!
//! # Test Scenarios
//!
//! 1. **Shell -> Workload RPC**: Send a typed request, receive response
//! 2. **Unknown route**: Workload returns error for unknown route_key
//! 3. **Multiple sequential calls**: Verify channel reuse works correctly
//! 4. **Error propagation**: Workload handler error propagated to Shell

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use actr_framework::{
    Bytes, Context, MaybeSendBoxFuture, MaybeSendSync, MessageDispatcher, Workload,
};
use actr_hyper::outbound::HostGate;
use actr_hyper::transport::HostTransport;
use actr_protocol::{ActorResult, ActrError, ActrId, PayloadType, RpcEnvelope};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Test Workload: DoubleWorkload — doubles an i32
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Simple workload that doubles integers and uppercases strings.
struct DoubleWorkload;

#[async_trait]
impl Workload for DoubleWorkload {
    type Dispatcher = DoubleDispatcher;
}

struct DoubleDispatcher;

#[async_trait]
impl MessageDispatcher for DoubleDispatcher {
    type Workload = DoubleWorkload;

    async fn dispatch<C: Context>(
        _workload: &Self::Workload,
        envelope: RpcEnvelope,
        _ctx: &C,
    ) -> ActorResult<Bytes> {
        match envelope.route_key.as_str() {
            "test/double" => {
                let payload = envelope
                    .payload
                    .as_ref()
                    .ok_or_else(|| ActrError::InvalidArgument("missing payload".to_string()))?;
                if payload.len() != 4 {
                    return Err(ActrError::InvalidArgument(format!(
                        "expected 4 bytes, got {}",
                        payload.len()
                    )));
                }
                let val = i32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let doubled = (val * 2).to_le_bytes().to_vec();
                Ok(Bytes::from(doubled))
            }
            "test/uppercase" => {
                let payload = envelope
                    .payload
                    .as_ref()
                    .ok_or_else(|| ActrError::InvalidArgument("missing payload".to_string()))?;
                let s = String::from_utf8_lossy(payload).to_uppercase();
                Ok(Bytes::from(s.into_bytes()))
            }
            "test/error" => Err(ActrError::Internal("intentional test error".to_string())),
            other => Err(ActrError::InvalidArgument(format!(
                "unknown route: {other}"
            ))),
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Helper: build the inproc infrastructure that node construction normally sets up
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

struct InprocTestHarness {
    /// Shell uses this gate to send requests to the Workload
    shell_gate: Arc<HostGate>,
    /// Raw Shell → Workload transport, for injecting hand-crafted envelopes
    /// that bypass the gate's direction stamping (interop / drop tests).
    shell_to_workload: Arc<HostTransport>,
    /// Shutdown signal
    shutdown_token: CancellationToken,
    /// Number of times the Workload handler actually ran (its side effect).
    /// Incremented once per dispatched envelope, regardless of `call` vs `tell`.
    handler_runs: Arc<AtomicUsize>,
    /// Number of envelopes observed on the Shell response lane. A `call`
    /// produces exactly one; a `tell` (suppressed reply) produces zero.
    responses_seen: Arc<AtomicUsize>,
    /// Direction label of the last envelope received by the Workload loop —
    /// pins what the sender gates actually put on the wire.
    last_direction: Arc<std::sync::Mutex<Option<i32>>>,
    /// Receives one item after the Workload finishes handling (or dropping)
    /// each inbound envelope, so tests can await the outcome deterministically
    /// (buffered channel — no lost wakeups, no polling with sleeps).
    handled_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>,
    /// Background task handles
    _task_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl InprocTestHarness {
    /// Build and start the full Shell <-> Workload plumbing.
    ///
    /// This mirrors what node construction + `ActrNode::start()` does for the inproc path,
    /// but without signaling, WebRTC, or any network components.
    async fn build() -> Self {
        // Direction 1: Shell -> Workload (REQUEST)
        let shell_to_workload = Arc::new(HostTransport::new());
        // Direction 2: Workload -> Shell (RESPONSE)
        let workload_to_shell = Arc::new(HostTransport::new());

        // Shell sends via this gate
        let shell_gate = Arc::new(HostGate::new(shell_to_workload.clone()));

        let shutdown_token = CancellationToken::new();

        // ── Spawn the Workload receive loop (mirrors actr_node.rs step 4.6) ──
        let workload_rx = shell_to_workload.clone();
        let response_tx = workload_to_shell.clone();
        let shutdown = shutdown_token.clone();

        let workload = Arc::new(DoubleWorkload);

        let request_rx_lane = workload_rx
            .get_lane(PayloadType::RpcReliable, None)
            .await
            .expect("failed to get Workload receive lane");

        let handler_runs = Arc::new(AtomicUsize::new(0));
        let handler_runs_loop = handler_runs.clone();
        let last_direction = Arc::new(std::sync::Mutex::new(None::<i32>));
        let last_direction_loop = last_direction.clone();
        let (handled_tx, handled_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let workload_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    envelope_result = request_rx_lane.recv_envelope() => {
                        match envelope_result {
                            Ok(envelope) => {
                                let request_id = envelope.request_id.clone();
                                let route_key = envelope.route_key.clone();
                                *last_direction_loop.lock().unwrap() = envelope.direction;

                                // Route strictly on the explicit direction label, mirroring
                                // the node.rs guest/peer receive loops: Request runs the
                                // handler and sends a response; Tell runs the handler for
                                // its side effects but sends nothing — an unwanted reply
                                // becomes an orphan response on the caller (#262). Anything
                                // else (missing / Unspecified / Response / unknown) is
                                // invalid on a dispatch lane and dropped with a warning.
                                // timeout_ms is never consulted for tell-ness.
                                let expects_response = match envelope
                                    .direction
                                    .map(actr_protocol::Direction::try_from)
                                {
                                    Some(Ok(actr_protocol::Direction::Request)) => true,
                                    Some(Ok(actr_protocol::Direction::Tell)) => false,
                                    other => {
                                        tracing::warn!(
                                            request_id = %request_id,
                                            direction = ?other,
                                            "invalid direction on dispatch lane; dropping"
                                        );
                                        // Signal the drop so tests can await it.
                                        let _ = handled_tx.send(());
                                        continue;
                                    }
                                };

                                // Dispatch to Workload (use a mock context). This runs the
                                // handler for its side effects unconditionally — including
                                // for a `tell` — exactly as the production receive loop does.
                                let ctx = MockContext;
                                let result = <DoubleDispatcher as MessageDispatcher>::dispatch(
                                    &workload,
                                    envelope.clone(),
                                    &ctx,
                                ).await;
                                handler_runs_loop.fetch_add(1, Ordering::SeqCst);

                                match result {
                                    Ok(response_bytes) => {
                                        if expects_response {
                                            let response_envelope = RpcEnvelope {
                                                route_key: route_key.clone(),
                                                payload: Some(response_bytes),
                                                error: None,
                                                direction: Some(actr_protocol::Direction::Response as i32),
                                                traceparent: None,
                                                tracestate: None,
                                                request_id: request_id.clone(),
                                                metadata: Vec::new(),
                                                timeout_ms: 0, // RESPONSE filler; contract: only REQUEST carries a deadline.
                                            };
                                            if let Err(e) = response_tx
                                                .send_message(PayloadType::RpcReliable, None, response_envelope)
                                                .await
                                            {
                                                tracing::error!("failed to send response: {:?}", e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        // Keep the local error log for every failure, but skip
                                        // sending an error envelope back for a `tell`.
                                        tracing::error!("workload handler failed: {:?}", e);
                                        if expects_response {
                                            let error_response = actr_protocol::ErrorResponse {
                                                code: error_to_code(&e),
                                                message: e.to_string(),
                                            };
                                            let error_envelope = RpcEnvelope {
                                                route_key: route_key.clone(),
                                                payload: None,
                                                error: Some(error_response),
                                                direction: Some(actr_protocol::Direction::Response as i32),
                                                traceparent: None,
                                                tracestate: None,
                                                request_id: request_id.clone(),
                                                metadata: Vec::new(),
                                                timeout_ms: 0, // RESPONSE filler; contract: only REQUEST carries a deadline.
                                            };
                                            if let Err(e) = response_tx
                                                .send_message(PayloadType::RpcReliable, None, error_envelope)
                                                .await
                                            {
                                                tracing::error!("failed to send error response: {:?}", e);
                                            }
                                        }
                                    }
                                }

                                // Signal that this envelope has been fully handled
                                // (side effect done, reply decision made).
                                let _ = handled_tx.send(());
                            }
                            Err(e) => {
                                tracing::error!("workload receive error: {:?}", e);
                                break;
                            }
                        }
                    }
                }
            }
        });

        // ── Spawn the Shell receive loop (mirrors actr_node.rs step 4.7) ──
        let response_rx = workload_to_shell.clone();
        let request_mgr = shell_to_workload.clone();
        let shutdown2 = shutdown_token.clone();

        let response_rx_lane = response_rx
            .get_lane(PayloadType::RpcReliable, None)
            .await
            .expect("failed to get Shell receive lane");

        let responses_seen = Arc::new(AtomicUsize::new(0));
        let responses_seen_loop = responses_seen.clone();

        let shell_receive_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown2.cancelled() => break,
                    envelope_result = response_rx_lane.recv_envelope() => {
                        match envelope_result {
                            Ok(envelope) => {
                                // Count every envelope that reaches the Shell response lane.
                                // A suppressed `tell` reply never arrives here.
                                responses_seen_loop.fetch_add(1, Ordering::SeqCst);
                                match (envelope.payload, envelope.error) {
                                    (Some(payload), None) => {
                                        if let Err(e) = request_mgr
                                            .complete_response(&envelope.request_id, payload)
                                            .await
                                        {
                                            tracing::warn!("orphan response: {:?}", e);
                                        }
                                    }
                                    (None, Some(error)) => {
                                        let actr_err = ActrError::Unavailable(
                                            format!("RPC error {}: {}", error.code, error.message)
                                        );
                                        if let Err(e) = request_mgr
                                            .complete_error(&envelope.request_id, actr_err)
                                            .await
                                        {
                                            tracing::warn!("orphan error response: {:?}", e);
                                        }
                                    }
                                    _ => {
                                        tracing::error!("invalid envelope: both or neither payload/error");
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("shell receive error: {:?}", e);
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self {
            shell_gate,
            shell_to_workload,
            shutdown_token,
            handler_runs,
            responses_seen,
            last_direction,
            handled_rx: tokio::sync::Mutex::new(handled_rx),
            _task_handles: vec![workload_handle, shell_receive_handle],
        }
    }

    /// Send a request from Shell to Workload and get back the raw response bytes.
    async fn call_raw(&self, route_key: &str, payload: Vec<u8>) -> ActorResult<Bytes> {
        let actor_id = test_actor_id();
        let envelope = RpcEnvelope {
            route_key: route_key.to_string(),
            payload: Some(Bytes::from(payload)),
            error: None,
            direction: Some(actr_protocol::Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: uuid::Uuid::new_v4().to_string(),
            metadata: Vec::new(),
            timeout_ms: 5000,
        };
        self.shell_gate.send_request(&actor_id, envelope).await
    }

    /// Send a fire-and-forget `tell` from Shell to Workload.
    ///
    /// Mirrors `RuntimeContext::tell`: the one-way message path stamps
    /// `Direction::Tell` (the gate overrides whatever is set here) and no
    /// pending response entry is registered on the sender side. The zero
    /// timeout is documented filler, not a tell marker.
    async fn tell_raw(&self, route_key: &str, payload: Vec<u8>) -> ActorResult<()> {
        let actor_id = test_actor_id();
        let envelope = RpcEnvelope {
            route_key: route_key.to_string(),
            payload: Some(Bytes::from(payload)),
            error: None,
            direction: Some(actr_protocol::Direction::Tell as i32),
            traceparent: None,
            tracestate: None,
            request_id: uuid::Uuid::new_v4().to_string(),
            metadata: Vec::new(),
            timeout_ms: 0, // Documented filler for TELL; not a tell marker.
        };
        self.shell_gate.send_message(&actor_id, envelope).await
    }

    /// Inject a hand-crafted envelope on the Shell → Workload lane without
    /// any gate-side direction stamping (for interop / drop-path tests).
    async fn send_raw_envelope(&self, envelope: RpcEnvelope) -> ActorResult<()> {
        self.shell_to_workload
            .send_message(PayloadType::RpcReliable, None, envelope)
            .await
            .map_err(|e| ActrError::Unavailable(e.to_string()))
    }

    /// Direction label of the last envelope seen by the Workload loop.
    fn last_direction(&self) -> Option<i32> {
        *self.last_direction.lock().unwrap()
    }

    /// Number of times the Workload handler ran (its observable side effect).
    fn handler_runs(&self) -> usize {
        self.handler_runs.load(Ordering::SeqCst)
    }

    /// Number of envelopes that reached the Shell response lane.
    fn responses_seen(&self) -> usize {
        self.responses_seen.load(Ordering::SeqCst)
    }

    /// Await the completion of the next handled inbound envelope, bounded by
    /// `timeout`. Deterministic (buffered channel), so no sleeps are needed.
    ///
    /// # Panics
    /// Panics if no envelope is handled within `timeout`.
    async fn await_handled(&self, timeout: std::time::Duration) {
        let mut rx = self.handled_rx.lock().await;
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(())) => {}
            Ok(None) => panic!("workload loop terminated before handling the envelope"),
            Err(_) => panic!("timed out waiting for the workload to handle the envelope"),
        }
    }

    /// Shutdown and clean up
    fn shutdown(&self) {
        self.shutdown_token.cancel();
    }
}

impl Drop for InprocTestHarness {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
        for h in self._task_handles.drain(..) {
            h.abort();
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Minimal mock Context (dispatch only needs it for the trait bound)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Clone)]
struct MockContext;

#[async_trait]
impl Context for MockContext {
    fn self_id(&self) -> &ActrId {
        static ID: std::sync::OnceLock<ActrId> = std::sync::OnceLock::new();
        ID.get_or_init(test_actor_id)
    }

    fn caller_id(&self) -> Option<&ActrId> {
        None
    }

    fn request_id(&self) -> &str {
        "mock-request"
    }

    async fn call<R: actr_protocol::RpcRequest>(
        &self,
        _target: &actr_framework::Dest,
        _request: R,
    ) -> ActorResult<R::Response> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn tell<R: actr_protocol::RpcRequest>(
        &self,
        _target: &actr_framework::Dest,
        _message: R,
    ) -> ActorResult<()> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn register_stream<F>(&self, _stream_id: String, _callback: F) -> ActorResult<()>
    where
        F: Fn(actr_protocol::DataChunk, ActrId) -> MaybeSendBoxFuture<'static, ActorResult<()>>
            + MaybeSendSync
            + 'static,
    {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn unregister_stream(&self, _stream_id: &str) -> ActorResult<()> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn send_data_chunk(
        &self,
        _target: &actr_framework::Dest,
        _chunk: actr_protocol::DataChunk,
        _payload_type: actr_protocol::PayloadType,
    ) -> ActorResult<()> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn discover_route_candidate(
        &self,
        _target_type: &actr_protocol::ActrType,
    ) -> ActorResult<ActrId> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn call_raw(
        &self,
        _target: &ActrId,
        _route_key: &str,
        _payload: bytes::Bytes,
    ) -> ActorResult<bytes::Bytes> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn register_media_track<F>(&self, _track_id: String, _callback: F) -> ActorResult<()>
    where
        F: Fn(
                actr_framework::MediaSample,
                ActrId,
            ) -> futures_util::future::BoxFuture<'static, ActorResult<()>>
            + Send
            + Sync
            + 'static,
    {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn unregister_media_track(&self, _track_id: &str) -> ActorResult<()> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn send_media_sample(
        &self,
        _target: &actr_framework::Dest,
        _track_id: &str,
        _sample: actr_framework::MediaSample,
    ) -> ActorResult<()> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn add_media_track(
        &self,
        _target: &actr_framework::Dest,
        _track_id: &str,
        _codec: &str,
        _media_type: &str,
    ) -> ActorResult<()> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn remove_media_track(
        &self,
        _target: &actr_framework::Dest,
        _track_id: &str,
    ) -> ActorResult<()> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Helpers
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

fn test_actor_id() -> ActrId {
    ActrId {
        realm: actr_protocol::Realm { realm_id: 1 },
        serial_number: 42,
        r#type: actr_protocol::ActrType {
            manufacturer: "test".to_string(),
            name: "double-service".to_string(),
            version: "0.1.0".to_string(),
        },
    }
}

fn error_to_code(e: &ActrError) -> u32 {
    match e {
        ActrError::InvalidArgument(_) => 400,
        ActrError::NotImplemented(_) => 501,
        ActrError::Internal(_) => 500,
        _ => 503,
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Tests
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Test 1: Shell -> Workload RPC round-trip (double an i32)
#[tokio::test]
async fn shell_to_workload_double() {
    let harness = InprocTestHarness::build().await;

    let x: i32 = 21;
    let result = harness
        .call_raw("test/double", x.to_le_bytes().to_vec())
        .await;

    let resp = result.expect("RPC call should succeed");
    assert_eq!(resp.len(), 4, "response should be 4 bytes");
    let val = i32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
    assert_eq!(val, 42, "21 * 2 should be 42");

    harness.shutdown();
}

/// Test 2: Shell -> Workload string RPC (uppercase)
#[tokio::test]
async fn shell_to_workload_uppercase() {
    let harness = InprocTestHarness::build().await;

    let result = harness
        .call_raw("test/uppercase", b"hello world".to_vec())
        .await;

    let resp = result.expect("RPC call should succeed");
    let s = String::from_utf8(resp.to_vec()).expect("valid utf8");
    assert_eq!(s, "HELLO WORLD");

    harness.shutdown();
}

/// Test 3: Unknown route returns error
#[tokio::test]
async fn unknown_route_returns_error() {
    let harness = InprocTestHarness::build().await;

    let result = harness.call_raw("nonexistent/route", vec![]).await;

    assert!(result.is_err(), "unknown route should return error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("unknown route"),
        "error message should mention unknown route, got: {err_msg}"
    );

    harness.shutdown();
}

/// Test 4: Workload handler error propagates to Shell
#[tokio::test]
async fn error_propagation() {
    let harness = InprocTestHarness::build().await;

    let result = harness.call_raw("test/error", vec![]).await;

    assert!(result.is_err(), "test/error route should return error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("intentional test error"),
        "error should propagate handler message, got: {err_msg}"
    );

    harness.shutdown();
}

/// Test 5: Multiple sequential calls reuse the same channel correctly
#[tokio::test]
async fn multiple_sequential_calls() {
    let harness = InprocTestHarness::build().await;

    for x in [1i32, 5, 42, -7, 0, 1000] {
        let result = harness
            .call_raw("test/double", x.to_le_bytes().to_vec())
            .await;

        let resp = result.unwrap_or_else(|e| panic!("call for x={x} failed: {e}"));
        let val = i32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
        assert_eq!(val, x * 2, "double({x}) should be {}", x * 2);
    }

    harness.shutdown();
}

/// Test 6: Concurrent calls from multiple tasks
#[tokio::test]
async fn concurrent_calls() {
    let harness = Arc::new(InprocTestHarness::build().await);

    let mut handles = Vec::new();
    for i in 0..10i32 {
        let h = harness.clone();
        handles.push(tokio::spawn(async move {
            let result = h.call_raw("test/double", i.to_le_bytes().to_vec()).await;
            let resp = result.unwrap_or_else(|e| panic!("concurrent call i={i} failed: {e}"));
            let val = i32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
            assert_eq!(val, i * 2, "concurrent double({i}) should be {}", i * 2);
        }));
    }

    for h in handles {
        h.await.expect("task should not panic");
    }

    harness.shutdown();
}

/// Test 7: a `tell` runs the handler but sends NO response (#262).
///
/// The receive loop must run the handler for its side effects for a
/// fire-and-forget `tell` (explicit `Direction::Tell` on the wire), but it
/// must NOT emit a reply. An unwanted reply would arrive as an orphan
/// response on the caller.
#[tokio::test]
async fn tell_runs_handler_but_sends_no_response() {
    let harness = InprocTestHarness::build().await;

    // Baseline: nothing handled, nothing on the response lane yet.
    assert_eq!(harness.handler_runs(), 0);
    assert_eq!(harness.responses_seen(), 0);

    // Fire-and-forget `tell` to a served route.
    harness
        .tell_raw("test/double", 21i32.to_le_bytes().to_vec())
        .await
        .expect("tell send should succeed");

    // Wait deterministically for the workload to finish handling it.
    harness
        .await_handled(std::time::Duration::from_secs(5))
        .await;

    // The tell arrived with the explicit fire-and-forget label — receivers
    // no longer infer tell-ness from timeout_ms.
    assert_eq!(
        harness.last_direction(),
        Some(actr_protocol::Direction::Tell as i32),
        "a tell must carry Direction::Tell on the wire"
    );
    // The handler ran (side effect observed) ...
    assert_eq!(
        harness.handler_runs(),
        1,
        "the tell handler must run for its side effects"
    );
    // ... but no response envelope was emitted back to the Shell.
    assert_eq!(
        harness.responses_seen(),
        0,
        "a tell must not produce a response envelope (#262)"
    );

    harness.shutdown();
}

/// Test 8: a `tell` whose handler errors still sends NO error response.
///
/// The local error is logged (visible in the loop), but no error envelope is
/// sent back — the caller registered no pending entry to receive it.
#[tokio::test]
async fn tell_with_handler_error_sends_no_error_response() {
    let harness = InprocTestHarness::build().await;

    harness
        .tell_raw("test/error", vec![])
        .await
        .expect("tell send should succeed");

    harness
        .await_handled(std::time::Duration::from_secs(5))
        .await;

    assert_eq!(
        harness.handler_runs(),
        1,
        "the failing tell handler must still run"
    );
    assert_eq!(
        harness.responses_seen(),
        0,
        "a tell must not produce an error response envelope (#262)"
    );

    harness.shutdown();
}

/// Test 9 (regression guard): a `call` to the same route still gets its
/// RESPONSE and completes normally. The tell suppression must not affect the
/// request/response path.
#[tokio::test]
async fn call_still_receives_its_response() {
    let harness = InprocTestHarness::build().await;

    let resp = harness
        .call_raw("test/double", 21i32.to_le_bytes().to_vec())
        .await
        .expect("call should succeed");
    let val = i32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
    assert_eq!(val, 42, "21 * 2 should be 42");

    // Exactly one response envelope reached the Shell response lane.
    assert_eq!(harness.handler_runs(), 1, "the call handler ran once");
    assert_eq!(
        harness.responses_seen(),
        1,
        "a call must receive exactly one response envelope"
    );

    harness.shutdown();
}

/// Test 10 (interop pin, #254): a REQUEST arriving with `timeout_ms == 0`
/// (buggy or pre-contract sender) is still dispatched AND answered.
/// Sender-side validation rejects zero timeouts, but receivers stay
/// permissive — tell-ness comes only from `Direction::Tell`.
#[tokio::test]
async fn request_with_zero_timeout_is_still_dispatched_and_answered() {
    let harness = InprocTestHarness::build().await;

    let envelope = RpcEnvelope {
        route_key: "test/double".to_string(),
        payload: Some(Bytes::from(21i32.to_le_bytes().to_vec())),
        error: None,
        direction: Some(actr_protocol::Direction::Request as i32),
        traceparent: None,
        tracestate: None,
        request_id: uuid::Uuid::new_v4().to_string(),
        metadata: Vec::new(),
        timeout_ms: 0, // Contract violation by the sender; receiver must not care.
    };
    harness
        .send_raw_envelope(envelope)
        .await
        .expect("raw send should succeed");

    harness
        .await_handled(std::time::Duration::from_secs(5))
        .await;

    assert_eq!(
        harness.handler_runs(),
        1,
        "a zero-timeout REQUEST must still be dispatched"
    );
    assert_eq!(
        harness.responses_seen(),
        1,
        "a zero-timeout REQUEST must still be answered (receiver permissive)"
    );

    harness.shutdown();
}

/// Test 11: an Unspecified direction is invalid on the dispatch lane and is
/// dropped without running the handler or emitting a reply.
#[tokio::test]
async fn unspecified_direction_is_dropped_without_dispatch() {
    let harness = InprocTestHarness::build().await;

    let envelope = RpcEnvelope {
        route_key: "test/double".to_string(),
        payload: Some(Bytes::from(21i32.to_le_bytes().to_vec())),
        error: None,
        direction: Some(actr_protocol::Direction::Unspecified as i32),
        traceparent: None,
        tracestate: None,
        request_id: uuid::Uuid::new_v4().to_string(),
        metadata: Vec::new(),
        timeout_ms: 5000,
    };
    harness
        .send_raw_envelope(envelope)
        .await
        .expect("raw send should succeed");

    harness
        .await_handled(std::time::Duration::from_secs(5))
        .await;

    assert_eq!(
        harness.handler_runs(),
        0,
        "an Unspecified-direction envelope must not be dispatched"
    );
    assert_eq!(
        harness.responses_seen(),
        0,
        "an Unspecified-direction envelope must not produce a reply"
    );

    harness.shutdown();
}

/// Test 12: a Response-labeled envelope on the dispatch lane is a mislabel
/// (responses are routed to pending maps by the gates) and is dropped.
#[tokio::test]
async fn response_direction_on_dispatch_lane_is_dropped() {
    let harness = InprocTestHarness::build().await;

    let envelope = RpcEnvelope {
        route_key: "test/double".to_string(),
        payload: Some(Bytes::from(21i32.to_le_bytes().to_vec())),
        error: None,
        direction: Some(actr_protocol::Direction::Response as i32),
        traceparent: None,
        tracestate: None,
        request_id: uuid::Uuid::new_v4().to_string(),
        metadata: Vec::new(),
        timeout_ms: 5000,
    };
    harness
        .send_raw_envelope(envelope)
        .await
        .expect("raw send should succeed");

    harness
        .await_handled(std::time::Duration::from_secs(5))
        .await;

    assert_eq!(
        harness.handler_runs(),
        0,
        "a Response-labeled envelope must not be dispatched as a request"
    );
    assert_eq!(
        harness.responses_seen(),
        0,
        "a dropped mislabel must not produce a reply"
    );

    harness.shutdown();
}
