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

use actr_framework::{Bytes, Context, MessageDispatcher, Workload};
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
    /// Shutdown signal
    shutdown_token: CancellationToken,
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

        let workload_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    envelope_result = request_rx_lane.recv_envelope() => {
                        match envelope_result {
                            Ok(envelope) => {
                                let request_id = envelope.request_id.clone();
                                let route_key = envelope.route_key.clone();

                                // Dispatch to Workload (use a mock context)
                                let ctx = MockContext;
                                let result = <DoubleDispatcher as MessageDispatcher>::dispatch(
                                    &workload,
                                    envelope.clone(),
                                    &ctx,
                                ).await;

                                match result {
                                    Ok(response_bytes) => {
                                        let response_envelope = RpcEnvelope {
                                            route_key: route_key.clone(),
                                            payload: Some(response_bytes),
                                            error: None,
                                            direction: Some(actr_protocol::Direction::Response as i32),
                                            traceparent: None,
                                            tracestate: None,
                                            request_id: request_id.clone(),
                                            metadata: Vec::new(),
                                            timeout_ms: 30000,
                                        };
                                        if let Err(e) = response_tx
                                            .send_message(PayloadType::RpcReliable, None, response_envelope)
                                            .await
                                        {
                                            tracing::error!("failed to send response: {:?}", e);
                                        }
                                    }
                                    Err(e) => {
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
                                            timeout_ms: 30000,
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

        let shell_receive_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown2.cancelled() => break,
                    envelope_result = response_rx_lane.recv_envelope() => {
                        match envelope_result {
                            Ok(envelope) => {
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
            shutdown_token,
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
        F: Fn(
                actr_protocol::DataStream,
                ActrId,
            ) -> futures_util::future::BoxFuture<'static, ActorResult<()>>
            + Send
            + Sync
            + 'static,
    {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn unregister_stream(&self, _stream_id: &str) -> ActorResult<()> {
        Err(ActrError::NotImplemented("mock context".to_string()))
    }

    async fn send_data_stream(
        &self,
        _target: &actr_framework::Dest,
        _chunk: actr_protocol::DataStream,
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
