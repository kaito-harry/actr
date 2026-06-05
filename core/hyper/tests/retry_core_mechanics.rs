//! Deterministic tests for the outbound retry core.
//!
//! The VNet retry tests exercise realistic recovery flows, but they cannot
//! precisely force "attempt 1 delivered, then returned a transient error".
//! This file uses fake transport components so we can assert the retry and
//! receiver-dedup invariants directly.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

type ErrorCtor = fn(String) -> NetworkError;

use actr_hyper::lifecycle::{
    NetworkAvailability, NetworkEvent, NetworkEventProcessor, NetworkRecoveryAction,
    NetworkSnapshot, NetworkTransportFlags, process_network_event_batch,
};
use actr_hyper::outbound::PeerGate;
use actr_hyper::test_support::{TestDedupOutcome, TestDedupState, make_actor_id};
use actr_hyper::transport::{
    ConnType, DataLane, Dest, NetworkError, NetworkResult, PeerTransport, WireBuilder, WireHandle,
};
use actr_protocol::prost::Message;

fn network_event(sequence: u64, available: bool, wifi: bool, cellular: bool) -> NetworkEvent {
    NetworkEvent::NetworkPathChanged {
        snapshot: NetworkSnapshot {
            sequence,
            availability: if available {
                NetworkAvailability::Available
            } else {
                NetworkAvailability::Unavailable
            },
            transport: NetworkTransportFlags {
                wifi,
                cellular,
                ethernet: false,
                vpn: false,
                other: false,
            },
            is_expensive: false,
            is_constrained: false,
        },
    }
}
use actr_protocol::{ActrId, PayloadType, RpcEnvelope};
use async_trait::async_trait;
use bytes::Bytes;

type DeliveryHook = Arc<dyn Fn(Bytes) + Send + Sync + 'static>;

struct MobileEventRecorder {
    actions: Arc<StdMutex<Vec<NetworkRecoveryAction>>>,
    delay: Duration,
}

#[async_trait]
impl NetworkEventProcessor for MobileEventRecorder {
    async fn process_network_available(&self) -> Result<(), String> {
        self.actions
            .lock()
            .expect("mobile event actions mutex poisoned")
            .push(NetworkRecoveryAction::Restore);
        Ok(())
    }

    async fn process_network_lost(&self) -> Result<(), String> {
        self.actions
            .lock()
            .expect("mobile event actions mutex poisoned")
            .push(NetworkRecoveryAction::Offline);
        Ok(())
    }

    async fn process_network_type_changed(
        &self,
        _is_wifi: bool,
        _is_cellular: bool,
    ) -> Result<(), String> {
        self.actions
            .lock()
            .expect("mobile event actions mutex poisoned")
            .push(NetworkRecoveryAction::Restore);
        Ok(())
    }

    async fn cleanup_connections(&self) -> Result<(), String> {
        self.actions
            .lock()
            .expect("mobile event actions mutex poisoned")
            .push(NetworkRecoveryAction::CleanupOnly);
        Ok(())
    }

    async fn process_network_recovery_action(
        &self,
        action: NetworkRecoveryAction,
    ) -> Result<(), String> {
        self.actions
            .lock()
            .expect("mobile event actions mutex poisoned")
            .push(action);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(())
    }
}

#[derive(Default)]
struct BuilderStats {
    create_calls: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl BuilderStats {
    fn begin_create(&self) {
        self.create_calls.fetch_add(1, Ordering::SeqCst);
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.record_max_in_flight(current);
    }

    fn end_create(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    fn record_max_in_flight(&self, value: usize) {
        let mut observed = self.max_in_flight.load(Ordering::SeqCst);
        while value > observed {
            match self.max_in_flight.compare_exchange(
                observed,
                value,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(next) => observed = next,
            }
        }
    }
}

struct ScriptedLane {
    sent: Arc<StdMutex<Vec<Bytes>>>,
    outcomes: Arc<StdMutex<VecDeque<NetworkResult<()>>>>,
    delivery_hook: Option<DeliveryHook>,
}

impl ScriptedLane {
    fn new(outcomes: Vec<NetworkResult<()>>) -> Self {
        Self {
            sent: Arc::new(StdMutex::new(Vec::new())),
            outcomes: Arc::new(StdMutex::new(VecDeque::from(outcomes))),
            delivery_hook: None,
        }
    }

    fn with_delivery_hook(mut self, hook: DeliveryHook) -> Self {
        self.delivery_hook = Some(hook);
        self
    }

    fn sent_payloads(&self) -> Vec<Bytes> {
        self.sent
            .lock()
            .expect("sent payload mutex poisoned")
            .clone()
    }
}

impl fmt::Debug for ScriptedLane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScriptedLane").finish_non_exhaustive()
    }
}

#[async_trait]
impl DataLane for ScriptedLane {
    async fn send(&self, data: Bytes) -> NetworkResult<()> {
        self.sent
            .lock()
            .expect("sent payload mutex poisoned")
            .push(data.clone());

        if let Some(hook) = &self.delivery_hook {
            hook(data);
        }

        self.outcomes
            .lock()
            .expect("outcome mutex poisoned")
            .pop_front()
            .unwrap_or(Ok(()))
    }

    fn lane_type(&self) -> &'static str {
        "scripted"
    }
}

struct StaticWire {
    lane: Arc<ScriptedLane>,
    connect_calls: AtomicUsize,
}

impl StaticWire {
    fn new(lane: Arc<ScriptedLane>) -> Self {
        Self {
            lane,
            connect_calls: AtomicUsize::new(0),
        }
    }
}

impl fmt::Debug for StaticWire {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticWire").finish_non_exhaustive()
    }
}

#[async_trait]
impl WireHandle for StaticWire {
    fn connection_type(&self) -> ConnType {
        ConnType::WebRTC
    }

    fn priority(&self) -> u8 {
        100
    }

    async fn connect(&self) -> NetworkResult<()> {
        self.connect_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn is_connected(&self) -> bool {
        true
    }

    async fn close(&self) -> NetworkResult<()> {
        Ok(())
    }

    async fn get_lane(&self, _payload_type: PayloadType) -> NetworkResult<Arc<dyn DataLane>> {
        Ok(self.lane.clone())
    }
}

struct ScriptedWireBuilder {
    wire: Arc<StaticWire>,
    stats: Arc<BuilderStats>,
    delay: Duration,
}

#[async_trait]
impl WireBuilder for ScriptedWireBuilder {
    async fn create_connections(&self, _dest: &Dest) -> NetworkResult<Vec<Arc<dyn WireHandle>>> {
        self.stats.begin_create();
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.stats.end_create();

        let wire: Arc<dyn WireHandle> = self.wire.clone();
        Ok(vec![wire])
    }
}

struct PausedWireBuilder {
    wire: Arc<StaticWire>,
    stats: Arc<BuilderStats>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl WireBuilder for PausedWireBuilder {
    async fn create_connections(&self, _dest: &Dest) -> NetworkResult<Vec<Arc<dyn WireHandle>>> {
        self.stats.begin_create();
        self.started.notify_waiters();
        self.release.notified().await;
        self.stats.end_create();

        let wire: Arc<dyn WireHandle> = self.wire.clone();
        Ok(vec![wire])
    }

    async fn create_connections_with_cancel(
        &self,
        dest: &Dest,
        cancel_token: Option<tokio_util::sync::CancellationToken>,
    ) -> NetworkResult<Vec<Arc<dyn WireHandle>>> {
        self.stats.begin_create();
        self.started.notify_waiters();

        match cancel_token {
            Some(token) => {
                tokio::select! {
                    _ = self.release.notified() => {
                        self.stats.end_create();
                        let wire: Arc<dyn WireHandle> = self.wire.clone();
                        Ok(vec![wire])
                    }
                    _ = token.cancelled() => {
                        self.stats.end_create();
                        Err(NetworkError::ConnectionClosed(format!(
                            "connection creation cancelled for {dest:?}"
                        )))
                    }
                }
            }
            None => {
                self.release.notified().await;
                self.stats.end_create();
                let wire: Arc<dyn WireHandle> = self.wire.clone();
                Ok(vec![wire])
            }
        }
    }
}

fn gate_with_lane(
    lane: ScriptedLane,
    builder_delay: Duration,
) -> (Arc<PeerGate>, Arc<ScriptedLane>, Arc<BuilderStats>) {
    let (gate, lane, stats, _transport) = gate_with_lane_and_transport(lane, builder_delay);
    (gate, lane, stats)
}

fn gate_with_lane_and_transport(
    lane: ScriptedLane,
    builder_delay: Duration,
) -> (
    Arc<PeerGate>,
    Arc<ScriptedLane>,
    Arc<BuilderStats>,
    Arc<PeerTransport>,
) {
    let lane = Arc::new(lane);
    let wire = Arc::new(StaticWire::new(lane.clone()));
    let stats = Arc::new(BuilderStats::default());
    let builder = Arc::new(ScriptedWireBuilder {
        wire,
        stats: stats.clone(),
        delay: builder_delay,
    });
    let transport = Arc::new(PeerTransport::new(make_actor_id(1), builder));
    let gate = Arc::new(PeerGate::new(transport.clone(), None));
    (gate, lane, stats, transport)
}

struct PausedBuilderFixture {
    transport: Arc<PeerTransport>,
    #[expect(dead_code)]
    lane: Arc<ScriptedLane>,
    stats: Arc<BuilderStats>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

fn transport_with_paused_builder() -> PausedBuilderFixture {
    let lane = Arc::new(ScriptedLane::new(vec![Ok(())]));
    let wire = Arc::new(StaticWire::new(lane.clone()));
    let stats = Arc::new(BuilderStats::default());
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let builder = Arc::new(PausedWireBuilder {
        wire,
        stats: stats.clone(),
        started: started.clone(),
        release: release.clone(),
    });
    let transport = Arc::new(PeerTransport::new(make_actor_id(1), builder));
    PausedBuilderFixture {
        transport,
        lane,
        stats,
        started,
        release,
    }
}

#[derive(Clone, Copy)]
enum CreateCancelAction {
    Cleanup,
    Shutdown,
}

impl CreateCancelAction {
    fn name(self) -> &'static str {
        match self {
            Self::Cleanup => "cleanup",
            Self::Shutdown => "shutdown",
        }
    }

    fn payload(self) -> &'static [u8] {
        match self {
            Self::Cleanup => b"cleanup-race",
            Self::Shutdown => b"shutdown-race",
        }
    }

    async fn close(self, transport: &PeerTransport, dest: &Dest) {
        match self {
            Self::Cleanup => transport
                .close_transport(dest)
                .await
                .expect("cleanup close should be idempotent"),
            Self::Shutdown => transport
                .close_all()
                .await
                .expect("shutdown close_all should be idempotent"),
        }
    }
}

fn envelope(request_id: &str) -> RpcEnvelope {
    envelope_with_timeout(request_id, 30_000)
}

fn envelope_with_timeout(request_id: &str, timeout_ms: i64) -> RpcEnvelope {
    RpcEnvelope {
        request_id: request_id.to_string(),
        route_key: "test.retry".to_string(),
        payload: Some(Bytes::from_static(b"payload")),
        timeout_ms,
        ..Default::default()
    }
}

fn encoded_envelope(request_id: &str) -> Vec<u8> {
    envelope(request_id).encode_to_vec()
}

fn decode_request_id(data: &[u8]) -> String {
    RpcEnvelope::decode(data)
        .expect("sent bytes should decode as RpcEnvelope")
        .request_id
}

fn receiver_handle_with_dedup(
    dedup: &mut TestDedupState,
    request_id: &str,
    handler_calls: &AtomicUsize,
) -> String {
    match dedup.check_or_mark(request_id) {
        TestDedupOutcome::Fresh => {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            let result = Ok(Bytes::from_static(b"handled"));
            dedup.complete(request_id, result.clone());
            "ok:handled".to_string()
        }
        TestDedupOutcome::InFlight(_) => "err:in-flight".to_string(),
        TestDedupOutcome::Duplicate(Ok(bytes)) => {
            format!("ok:{}", String::from_utf8_lossy(bytes.as_ref()))
        }
        TestDedupOutcome::Duplicate(Err(err)) => format!("err:{err}"),
    }
}

fn install_immediate_receiver_delivery_hook(
    receiver_dedup: Arc<StdMutex<TestDedupState>>,
    handler_calls: Arc<AtomicUsize>,
    receiver_results: Arc<StdMutex<Vec<String>>>,
) -> DeliveryHook {
    Arc::new(move |data: Bytes| {
        let request_id = decode_request_id(data.as_ref());
        let result = {
            let mut dedup = receiver_dedup
                .lock()
                .expect("receiver dedup mutex poisoned");
            receiver_handle_with_dedup(&mut dedup, &request_id, &handler_calls)
        };
        receiver_results
            .lock()
            .expect("receiver results mutex poisoned")
            .push(result);
    })
}

fn actor_result_label(result: actr_protocol::ActorResult<Bytes>) -> String {
    match result {
        Ok(bytes) => format!("ok:{}", String::from_utf8_lossy(bytes.as_ref())),
        Err(err) => format!("err:{err}"),
    }
}

async fn wait_for_receiver_results(
    results: Arc<StdMutex<Vec<String>>>,
    expected_len: usize,
) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let current = results
            .lock()
            .expect("receiver results mutex poisoned")
            .clone();
        if current.len() >= expected_len {
            return current;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected_len} receiver results, got {current:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn install_slow_receiver_delivery_hook(
    receiver_dedup: Arc<StdMutex<TestDedupState>>,
    handler_calls: Arc<AtomicUsize>,
    receiver_results: Arc<StdMutex<Vec<String>>>,
    handler_delay: Duration,
) -> DeliveryHook {
    Arc::new(move |data: Bytes| {
        let request_id = decode_request_id(data.as_ref());
        let outcome = {
            receiver_dedup
                .lock()
                .expect("receiver dedup mutex poisoned")
                .check_or_mark(&request_id)
        };

        match outcome {
            TestDedupOutcome::Fresh => {
                handler_calls.fetch_add(1, Ordering::SeqCst);
                let receiver_dedup = receiver_dedup.clone();
                let receiver_results = receiver_results.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(handler_delay).await;
                    let result = Ok(Bytes::from_static(b"slow-handled"));
                    receiver_dedup
                        .lock()
                        .expect("receiver dedup mutex poisoned")
                        .complete(&request_id, result.clone());
                    receiver_results
                        .lock()
                        .expect("receiver results mutex poisoned")
                        .push(actor_result_label(result));
                });
            }
            TestDedupOutcome::InFlight(waiter) => {
                let receiver_results = receiver_results.clone();
                tokio::spawn(async move {
                    let result = waiter.wait().await;
                    receiver_results
                        .lock()
                        .expect("receiver results mutex poisoned")
                        .push(actor_result_label(result));
                });
            }
            TestDedupOutcome::Duplicate(result) => {
                receiver_results
                    .lock()
                    .expect("receiver results mutex poisoned")
                    .push(actor_result_label(result));
            }
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_attempt_matrix_matches_payload_type_policy() {
    let target = make_actor_id(2);
    let cases = [
        (PayloadType::RpcReliable, 5usize),
        (PayloadType::RpcSignal, 2usize),
        (PayloadType::StreamReliable, 1usize),
        (PayloadType::StreamLatencyFirst, 1usize),
    ];

    for (payload_type, expected_attempts) in cases {
        let outcomes = (0..expected_attempts)
            .map(|_| Err(NetworkError::SendError("transient failure".into())))
            .collect();
        let (gate, lane, _stats) = gate_with_lane(ScriptedLane::new(outcomes), Duration::ZERO);
        let result = gate
            .send_serialized_with_zero_retry_delay_for_test(
                &target,
                payload_type,
                &encoded_envelope(&format!("matrix-{payload_type:?}")),
            )
            .await;

        assert!(
            result.is_err(),
            "{payload_type:?} should fail after exhausting retry attempts"
        );
        assert_eq!(
            lane.sent_payloads().len(),
            expected_attempts,
            "{payload_type:?} attempt count should match its retry policy"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retryable_error_kinds_retry_once_preserve_request_id_and_reuse_transport() {
    let target = make_actor_id(2);
    let cases: [(&str, ErrorCtor); 12] = [
        ("ConnectionError", NetworkError::ConnectionError),
        ("ConnectionClosed", NetworkError::ConnectionClosed),
        ("ChannelClosed", NetworkError::ChannelClosed),
        ("SendError", NetworkError::SendError),
        (
            "NetworkUnreachableError",
            NetworkError::NetworkUnreachableError,
        ),
        (
            "ResourceExhaustedError",
            NetworkError::ResourceExhaustedError,
        ),
        ("WebSocketError", NetworkError::WebSocketError),
        ("SignalingError", NetworkError::SignalingError),
        ("WebRtcError", NetworkError::WebRtcError),
        ("NatTraversalError", NetworkError::NatTraversalError),
        ("IceError", NetworkError::IceError),
        ("TimeoutError", NetworkError::TimeoutError),
    ];

    for (name, make_error) in cases {
        let request_id = format!("retryable-{name}");
        let (gate, lane, stats) = gate_with_lane(
            ScriptedLane::new(vec![Err(make_error(format!("{name} transient"))), Ok(())]),
            Duration::ZERO,
        );

        gate.send_serialized_with_zero_retry_delay_for_test(
            &target,
            PayloadType::RpcReliable,
            &encoded_envelope(&request_id),
        )
        .await
        .unwrap_or_else(|err| panic!("{name} should retry and then succeed: {err}"));

        let sent = lane.sent_payloads();
        let request_ids: Vec<_> = sent
            .iter()
            .map(|payload| decode_request_id(payload.as_ref()))
            .collect();
        assert_eq!(sent.len(), 2, "{name} should trigger exactly one retry");
        assert_eq!(
            request_ids,
            vec![request_id.clone(), request_id],
            "{name} retry must preserve request_id"
        );
        assert_eq!(
            stats.create_calls.load(Ordering::SeqCst),
            1,
            "{name} retry should reuse the existing DestTransport"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_retryable_error_kinds_do_not_retry() {
    let target = make_actor_id(2);
    let cases: [(&str, ErrorCtor); 8] = [
        ("NoRoute", NetworkError::NoRoute),
        ("InvalidOperation", NetworkError::InvalidOperation),
        ("InvalidArgument", NetworkError::InvalidArgument),
        ("ConfigurationError", NetworkError::ConfigurationError),
        ("AuthenticationError", NetworkError::AuthenticationError),
        ("PermissionError", NetworkError::PermissionError),
        ("DataChannelError", NetworkError::DataChannelError),
        ("ProtocolError", NetworkError::ProtocolError),
    ];

    for (name, make_error) in cases {
        let (gate, lane, _stats) = gate_with_lane(
            ScriptedLane::new(vec![Err(make_error(format!("{name} final"))), Ok(())]),
            Duration::ZERO,
        );

        let result = gate
            .send_serialized_with_zero_retry_delay_for_test(
                &target,
                PayloadType::RpcReliable,
                &encoded_envelope(&format!("non-retryable-{name}")),
            )
            .await;

        assert!(result.is_err(), "{name} should surface immediately");
        assert_eq!(
            lane.sent_payloads().len(),
            1,
            "{name} must not trigger retry"
        );
        assert_eq!(
            decode_request_id(lane.sent_payloads()[0].as_ref()),
            format!("non-retryable-{name}"),
            "{name} should send the original request exactly once"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_connection_creation_is_singleflight_and_does_not_duplicate_messages() {
    let target = make_actor_id(2);
    let message_count = 10usize;
    let (gate, lane, stats) =
        gate_with_lane(ScriptedLane::new(Vec::new()), Duration::from_millis(1500));

    let mut tasks = Vec::new();
    for index in 0..message_count {
        let gate = gate.clone();
        let target: ActrId = target.clone();
        tasks.push(tokio::spawn(async move {
            gate.send_message_with_type(
                &target,
                PayloadType::RpcReliable,
                envelope(&format!("slow-connect-{index}")),
            )
            .await
        }));
    }

    for task in tasks {
        task.await
            .expect("send task should not panic")
            .expect("send should succeed after the slow connection is ready");
    }

    let sent = lane.sent_payloads();
    let request_ids: HashSet<_> = sent
        .iter()
        .map(|payload| decode_request_id(payload.as_ref()))
        .collect();

    assert_eq!(
        stats.create_calls.load(Ordering::SeqCst),
        1,
        "all callers should wait on the same slow connection creation"
    );
    assert_eq!(
        stats.max_in_flight.load(Ordering::SeqCst),
        1,
        "connection creation itself should never run concurrently"
    );
    assert_eq!(
        sent.len(),
        message_count,
        "slow connection creation must not cause duplicate sends"
    );
    assert_eq!(
        request_ids.len(),
        message_count,
        "each original message should be sent exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambiguous_delivery_retry_is_receiver_deduped_for_serialized_send_and_tell() {
    #[derive(Clone, Copy)]
    enum SendPath {
        SerializedCore,
        Tell,
    }

    for path in [SendPath::SerializedCore, SendPath::Tell] {
        let (label, request_id) = match path {
            SendPath::SerializedCore => ("serialized core send", "ambiguous-serialized"),
            SendPath::Tell => ("tell", "ambiguous-tell"),
        };
        let target = make_actor_id(2);
        let receiver_dedup = Arc::new(StdMutex::new(TestDedupState::new()));
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let receiver_results = Arc::new(StdMutex::new(Vec::<String>::new()));
        let delivery_hook = install_immediate_receiver_delivery_hook(
            receiver_dedup,
            handler_calls.clone(),
            receiver_results.clone(),
        );

        let (gate, lane, _stats) = gate_with_lane(
            ScriptedLane::new(vec![
                Err(NetworkError::SendError(format!(
                    "{label} delivered but sender observed failure"
                ))),
                Ok(()),
            ])
            .with_delivery_hook(delivery_hook),
            Duration::ZERO,
        );

        match path {
            SendPath::SerializedCore => {
                gate.send_serialized_with_zero_retry_delay_for_test(
                    &target,
                    PayloadType::RpcReliable,
                    &encoded_envelope(request_id),
                )
                .await
            }
            SendPath::Tell => {
                gate.send_message_with_type(&target, PayloadType::RpcReliable, envelope(request_id))
                    .await
            }
        }
        .unwrap_or_else(|err| panic!("{label} retry should eventually succeed: {err}"));

        let sent = lane.sent_payloads();
        let request_ids: Vec<_> = sent
            .iter()
            .map(|payload| decode_request_id(payload.as_ref()))
            .collect();
        let receiver_results = receiver_results
            .lock()
            .expect("receiver results mutex poisoned")
            .clone();

        assert_eq!(sent.len(), 2, "{label} should be retried once");
        assert_eq!(
            request_ids,
            vec![request_id.to_string(), request_id.to_string()],
            "{label} retry must preserve request_id"
        );
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            1,
            "{label} receiver dedup should execute the handler only once"
        );
        assert_eq!(
            receiver_results,
            vec!["ok:handled".to_string(), "ok:handled".to_string()],
            "{label} duplicate delivery should receive the cached handler result"
        );
        if matches!(path, SendPath::Tell) {
            assert_eq!(
                gate.pending_count().await,
                0,
                "tell must not register a pending response"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_duplicate_waits_for_original_result_and_times_out_without_completion() {
    let receiver_dedup = Arc::new(StdMutex::new(TestDedupState::new()));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let receiver_results = Arc::new(StdMutex::new(Vec::<String>::new()));
    let request_id = "slow-handler-duplicate".to_string();

    let first_outcome = {
        receiver_dedup
            .lock()
            .expect("receiver dedup mutex poisoned")
            .check_or_mark(&request_id)
    };
    assert!(matches!(first_outcome, TestDedupOutcome::Fresh));
    handler_calls.fetch_add(1, Ordering::SeqCst);

    let duplicate_waiter = {
        match receiver_dedup
            .lock()
            .expect("receiver dedup mutex poisoned")
            .check_or_mark(&request_id)
        {
            TestDedupOutcome::InFlight(waiter) => waiter,
            other => panic!("expected in-flight waiter, got {other:?}"),
        }
    };

    let completion_dedup = receiver_dedup.clone();
    let completion_results = receiver_results.clone();
    let completion_request_id = request_id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let result = Ok(Bytes::from_static(b"slow-handled"));
        completion_dedup
            .lock()
            .expect("receiver dedup mutex poisoned")
            .complete(&completion_request_id, result.clone());
        completion_results
            .lock()
            .expect("receiver results mutex poisoned")
            .push(actor_result_label(result));
    });

    let duplicate_result = duplicate_waiter
        .wait()
        .await
        .expect("duplicate should receive original result");

    assert_eq!(
        String::from_utf8_lossy(duplicate_result.as_ref()),
        "slow-handled"
    );
    assert_eq!(
        handler_calls.load(Ordering::SeqCst),
        1,
        "duplicate must not re-enter the handler while original is in-flight"
    );

    let receiver_results = wait_for_receiver_results(receiver_results, 1).await;
    assert_eq!(receiver_results, vec!["ok:slow-handled".to_string()]);

    let receiver_dedup = Arc::new(StdMutex::new(TestDedupState::new()));
    let request_id = "suspended-original-request";

    assert!(matches!(
        receiver_dedup
            .lock()
            .expect("receiver dedup mutex poisoned")
            .check_or_mark(request_id),
        TestDedupOutcome::Fresh
    ));

    let waiter = match receiver_dedup
        .lock()
        .expect("receiver dedup mutex poisoned")
        .check_or_mark(request_id)
    {
        TestDedupOutcome::InFlight(waiter) => waiter,
        other => panic!("expected in-flight waiter, got {other:?}"),
    };

    let start = std::time::Instant::now();
    let result = waiter.wait_timeout(Duration::from_millis(50)).await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "waiter should time out without complete()");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("in-flight timed out"),
        "timeout should be explicit"
    );
    assert!(
        elapsed >= Duration::from_millis(50),
        "timeout guard should wait for the configured timeout, took {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "timeout guard should return promptly, took {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_timeout_then_late_old_response_does_not_complete_new_request() {
    let target = make_actor_id(2);
    let (gate, _lane, _stats) =
        gate_with_lane(ScriptedLane::new(vec![Ok(()), Ok(())]), Duration::ZERO);

    let old_request = {
        let gate = gate.clone();
        let target = target.clone();
        tokio::spawn(async move {
            gate.send_request(&target, envelope_with_timeout("rc25-old-request", 100))
                .await
        })
    };

    let old_error = tokio::time::timeout(Duration::from_secs(2), old_request)
        .await
        .expect("old request should finish within its deadline")
        .expect("old request task should not panic")
        .expect_err("old request should time out before any response arrives");
    assert!(
        old_error.to_string().contains("Request timeout"),
        "old request should fail with explicit timeout, got: {old_error}"
    );
    assert_eq!(
        gate.pending_count().await,
        0,
        "timed-out request must be removed from pending map"
    );

    let new_request = {
        let gate = gate.clone();
        let target = target.clone();
        tokio::spawn(async move {
            gate.send_request(&target, envelope_with_timeout("rc25-new-request", 5_000))
                .await
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        gate.pending_count().await,
        1,
        "new request should be pending before responses are injected"
    );

    assert!(
        !gate
            .handle_response("rc25-old-request", Ok(Bytes::from_static(b"old-response")))
            .await
            .expect("late old response should be handled without error"),
        "late old response must not complete any current request"
    );
    assert_eq!(
        gate.pending_count().await,
        1,
        "late old response must leave the new request pending"
    );

    assert!(
        gate.handle_response("rc25-new-request", Ok(Bytes::from_static(b"new-response")))
            .await
            .expect("new response should be handled"),
        "new response should complete the new request"
    );

    let response = tokio::time::timeout(Duration::from_secs(2), new_request)
        .await
        .expect("new request should complete after its own response")
        .expect("new request task should not panic")
        .expect("new request should receive its own response");
    assert_eq!(response, Bytes::from_static(b"new-response"));
    assert_eq!(gate.pending_count().await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambiguous_delivery_with_slow_handler_does_not_return_inflight_error() {
    let target = make_actor_id(2);
    let receiver_dedup = Arc::new(StdMutex::new(TestDedupState::new()));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let receiver_results = Arc::new(StdMutex::new(Vec::<String>::new()));

    let delivery_hook = install_slow_receiver_delivery_hook(
        receiver_dedup,
        handler_calls.clone(),
        receiver_results.clone(),
        Duration::from_millis(100),
    );

    let (gate, lane, _stats) = gate_with_lane(
        ScriptedLane::new(vec![
            Err(NetworkError::SendError(
                "delivered but sender observed failure".into(),
            )),
            Ok(()),
        ])
        .with_delivery_hook(delivery_hook),
        Duration::ZERO,
    );

    gate.send_serialized_with_zero_retry_delay_for_test(
        &target,
        PayloadType::RpcReliable,
        &encoded_envelope("ambiguous-slow-handler"),
    )
    .await
    .expect("retry should eventually succeed");

    let sent = lane.sent_payloads();
    let request_ids: Vec<_> = sent
        .iter()
        .map(|payload| decode_request_id(payload.as_ref()))
        .collect();
    let receiver_results = wait_for_receiver_results(receiver_results, 2).await;

    assert_eq!(sent.len(), 2, "ambiguous delivery should cause one retry");
    assert_eq!(
        request_ids,
        vec![
            "ambiguous-slow-handler".to_string(),
            "ambiguous-slow-handler".to_string()
        ],
        "both deliveries must carry the same request_id"
    );
    assert_eq!(
        handler_calls.load(Ordering::SeqCst),
        1,
        "slow in-flight duplicate should wait, not execute handler twice"
    );
    assert_eq!(
        receiver_results,
        vec!["ok:slow-handled".to_string(), "ok:slow-handled".to_string()],
        "duplicate should return the original result instead of an in-flight error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mobile_event_batch_during_retry_backoff_does_not_duplicate_handler() {
    let target = make_actor_id(2);
    let receiver_dedup = Arc::new(StdMutex::new(TestDedupState::new()));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let receiver_results = Arc::new(StdMutex::new(Vec::<String>::new()));
    let delivery_hook = install_slow_receiver_delivery_hook(
        receiver_dedup,
        handler_calls.clone(),
        receiver_results.clone(),
        Duration::from_millis(1200),
    );

    let (gate, lane, _stats) = gate_with_lane(
        ScriptedLane::new(vec![
            Err(NetworkError::SendError(
                "delivered before mobile event storm".into(),
            )),
            Ok(()),
        ])
        .with_delivery_hook(delivery_hook),
        Duration::ZERO,
    );

    let send_task = tokio::spawn({
        let gate = gate.clone();
        let target = target.clone();
        async move {
            gate.send_message_with_type(
                &target,
                PayloadType::RpcReliable,
                envelope("mobile-event-during-retry"),
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let actions = Arc::new(StdMutex::new(Vec::new()));
    let processor = Arc::new(MobileEventRecorder {
        actions: actions.clone(),
        delay: Duration::from_millis(100),
    });
    let results = process_network_event_batch(
        vec![
            network_event(1, false, false, false),
            network_event(2, true, false, false),
            network_event(3, true, false, true),
        ],
        processor,
    )
    .await;

    send_task
        .await
        .expect("send task should not panic")
        .expect("send should recover after retry");

    let sent = lane.sent_payloads();
    let request_ids: Vec<_> = sent
        .iter()
        .map(|payload| decode_request_id(payload.as_ref()))
        .collect();
    let receiver_results = wait_for_receiver_results(receiver_results, 2).await;
    let actions = actions
        .lock()
        .expect("mobile event actions mutex poisoned")
        .clone();

    assert_eq!(results.len(), 3, "batch should return one result per event");
    assert!(results.iter().all(|result| result.success));
    assert_eq!(
        actions,
        vec![NetworkRecoveryAction::Restore],
        "Lost -> Available -> TypeChanged should settle to one Restore action"
    );
    assert_eq!(sent.len(), 2, "send should retry exactly once");
    assert_eq!(
        request_ids,
        vec![
            "mobile-event-during-retry".to_string(),
            "mobile-event-during-retry".to_string()
        ],
        "retry during mobile event storm must preserve request_id"
    );
    assert_eq!(
        handler_calls.load(Ordering::SeqCst),
        1,
        "mobile event storm during retry must not duplicate handler execution"
    );
    assert_eq!(
        receiver_results,
        vec!["ok:slow-handled".to_string(), "ok:slow-handled".to_string()],
        "retry duplicate should wait for/cache original handler result"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continuous_requests_during_mobile_event_storm_complete_without_pending_leak() {
    let target = make_actor_id(2);
    let outcomes = std::iter::repeat_with(|| Ok(()))
        .take(6)
        .collect::<Vec<_>>();
    let (gate, lane, _stats) =
        gate_with_lane(ScriptedLane::new(outcomes), Duration::from_millis(10));

    let request_ids = (0..6)
        .map(|index| format!("mobile-event-storm-continuous-{index}"))
        .collect::<Vec<_>>();

    let mut request_tasks = Vec::new();
    for request_id in &request_ids {
        let gate = gate.clone();
        let target = target.clone();
        let request_id = request_id.clone();
        request_tasks.push(tokio::spawn(async move {
            gate.send_request(&target, envelope(&request_id)).await
        }));
    }

    let mut response_tasks = Vec::new();
    for request_id in &request_ids {
        let gate = gate.clone();
        let request_id = request_id.clone();
        response_tasks.push(tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            loop {
                if gate
                    .handle_response(&request_id, Ok(Bytes::from_static(b"storm-ok")))
                    .await
                    .expect("test response injection should not fail")
                {
                    return;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "request {request_id} was never registered as pending"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    let actions = Arc::new(StdMutex::new(Vec::new()));
    let processor = Arc::new(MobileEventRecorder {
        actions: actions.clone(),
        delay: Duration::from_millis(30),
    });
    let storm_task = tokio::spawn(async move {
        let batches = [
            vec![
                network_event(1, false, false, false),
                network_event(2, true, false, false),
                network_event(3, true, false, true),
            ],
            vec![network_event(4, false, false, false)],
            vec![network_event(5, true, true, false)],
        ];

        for batch in batches {
            let results = process_network_event_batch(batch, processor.clone()).await;
            assert!(results.iter().all(|result| result.success));
        }
    });

    for task in response_tasks {
        task.await
            .expect("response injection task should not panic");
    }

    for task in request_tasks {
        let response = task
            .await
            .expect("request task should not panic")
            .expect("request should complete during mobile event storm");
        assert_eq!(response, Bytes::from_static(b"storm-ok"));
    }

    storm_task
        .await
        .expect("mobile event storm task should not panic");

    assert_eq!(
        gate.pending_count().await,
        0,
        "continuous requests during event storm should leave no pending requests"
    );
    assert_eq!(
        lane.sent_payloads().len(),
        request_ids.len(),
        "each continuous request should send exactly once"
    );

    let actions = actions
        .lock()
        .expect("mobile event actions mutex poisoned")
        .clone();
    assert_eq!(
        actions,
        vec![
            NetworkRecoveryAction::Restore,
            NetworkRecoveryAction::Offline,
            NetworkRecoveryAction::Restore,
        ],
        "event storm batches should settle to one action per batch"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_transport_cancelled_by_close_action_does_not_leave_stale_dest() {
    for action in [CreateCancelAction::Cleanup, CreateCancelAction::Shutdown] {
        let f = transport_with_paused_builder();
        let transport = f.transport;
        let stats = f.stats;
        let started = f.started;
        let release = f.release;
        let target = make_actor_id(2);
        let dest = Dest::actor(target);

        let send_task = tokio::spawn({
            let transport = transport.clone();
            let dest = dest.clone();
            async move {
                transport
                    .send(&dest, PayloadType::RpcReliable, action.payload())
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("connection creation should start");
        assert!(
            transport.is_connecting(&dest).await,
            "destination should be in Connecting state before {}",
            action.name()
        );

        action.close(&transport, &dest).await;

        let result = tokio::time::timeout(Duration::from_secs(1), send_task)
            .await
            .unwrap_or_else(|_| panic!("{}-cancelled create should finish promptly", action.name()))
            .expect("send task should not panic");
        let err = match result {
            Ok(_) => panic!(
                "send should fail after {} cancels connection creation",
                action.name()
            ),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("cancelled") || err.to_string().contains("closed"),
            "unexpected {} cancellation error: {err}",
            action.name()
        );

        release.notify_waiters();
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            transport.dest_count().await,
            0,
            "{} during connection creation must not leave a stale DestTransport",
            action.name()
        );
        assert_eq!(stats.create_calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_during_inflight_rpc_is_bounded_and_next_rpc_is_clean() {
    let (gate, lane, _stats, transport) =
        gate_with_lane_and_transport(ScriptedLane::new(vec![Ok(()), Ok(())]), Duration::ZERO);
    let target = make_actor_id(2);
    let dest = Dest::actor(target.clone());

    let first = tokio::spawn({
        let gate = gate.clone();
        let target = target.clone();
        async move {
            gate.send_request(&target, envelope_with_timeout("cleanup-inflight-rpc", 150))
                .await
        }
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if !lane.sent_payloads().is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "first RPC was never sent before cleanup"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    transport
        .close_transport(&dest)
        .await
        .expect("cleanup should close active transport");

    let err = tokio::time::timeout(Duration::from_secs(1), first)
        .await
        .expect("cleanup-overlapped RPC should complete within caller deadline")
        .expect("RPC task should not panic")
        .expect_err("in-flight RPC without response should fail boundedly");
    assert!(
        err.to_string().contains("Request timeout"),
        "in-flight RPC should fail with an explicit deadline error, got: {err}"
    );
    assert_eq!(
        gate.pending_count().await,
        0,
        "timed-out cleanup-overlapped RPC should clear pending state"
    );
    assert_eq!(
        transport.dest_count().await,
        0,
        "cleanup should remove the closed transport before the next send"
    );

    let second = tokio::spawn({
        let gate = gate.clone();
        let target = target.clone();
        async move {
            gate.send_request(&target, envelope_with_timeout("after-cleanup-rpc", 1_000))
                .await
        }
    });

    let response_task = tokio::spawn({
        let gate = gate.clone();
        async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            loop {
                if gate
                    .handle_response(
                        "after-cleanup-rpc",
                        Ok(Bytes::from_static(b"after-cleanup-ok")),
                    )
                    .await
                    .expect("test response injection should not fail")
                {
                    return;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "second RPC was never registered as pending"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });

    response_task
        .await
        .expect("response injection task should not panic");
    let response = second
        .await
        .expect("second RPC task should not panic")
        .expect("second RPC should succeed after cleanup");
    assert_eq!(response, Bytes::from_static(b"after-cleanup-ok"));
    assert_eq!(
        gate.pending_count().await,
        0,
        "successful post-cleanup RPC should leave no pending state"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrecoverable_rpc_send_failure_clears_pending_without_waiting_for_deadline() {
    let (gate, lane, _stats, _transport) = gate_with_lane_and_transport(
        ScriptedLane::new(vec![Err(NetworkError::NoRoute(
            "rpc route permanently unavailable".into(),
        ))]),
        Duration::ZERO,
    );
    let target = make_actor_id(2);

    let start = std::time::Instant::now();
    let err = gate
        .send_request(&target, envelope_with_timeout("unrecoverable-rpc", 5_000))
        .await
        .expect_err("unrecoverable RPC send failure should return an explicit error");

    assert!(
        start.elapsed() < Duration::from_secs(1),
        "non-retryable RPC failure should not wait for the request deadline"
    );
    assert!(
        err.to_string().contains("No route") || err.to_string().contains("permanently unavailable"),
        "unexpected RPC failure error: {err}"
    );
    assert_eq!(lane.sent_payloads().len(), 1);
    assert_eq!(
        gate.pending_count().await,
        0,
        "failed RPC send should remove pending state immediately"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrecoverable_data_stream_send_failure_is_explicit_and_bounded() {
    let (gate, lane, _stats, _transport) = gate_with_lane_and_transport(
        ScriptedLane::new(vec![Err(NetworkError::ChannelClosed(
            "stream channel permanently closed".into(),
        ))]),
        Duration::ZERO,
    );
    let target = make_actor_id(2);

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        gate.send_data_stream(
            &target,
            PayloadType::StreamReliable,
            "unrecoverable-stream",
            Bytes::from_static(b"stream-payload"),
        ),
    )
    .await
    .expect("unrecoverable DataStream send should not hang");

    let err = result.expect_err("unrecoverable DataStream send should fail explicitly");
    assert!(
        err.to_string().contains("permanently closed")
            || err.to_string().contains("Channel closed"),
        "unexpected DataStream failure error: {err}"
    );
    assert_eq!(
        lane.sent_payloads().len(),
        1,
        "DataStream should make one bounded send attempt"
    );
}
