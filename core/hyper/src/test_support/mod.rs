//! Shared integration-test helpers enabled by the `test-utils` feature.
//!
//! These modules are used by `core/hyper/tests/*` and live under the library
//! so their public APIs are treated as externally reachable rather than dead
//! code inside each individual integration test crate.

#[cfg(feature = "wasm-engine")]
use crate::HostOperationResult;
use crate::{BinaryKind, Hyper, WorkloadPackage};
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
use crate::{HostAbiFn, InvocationContext};
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
use actr_framework::guest::dynclib_abi::InitPayloadV1;
use actr_pack::PackageManifest;
use actr_protocol::{AIdCredential, ActrId};
use std::sync::Arc;

#[path = "../../tests/common/harness.rs"]
pub mod harness;
#[path = "../../tests/common/signaling.rs"]
pub mod signaling;
#[path = "../../tests/common/utils.rs"]
pub mod utils;
#[path = "../../tests/common/vnet.rs"]
pub mod vnet;
#[path = "../../tests/common/wait.rs"]
pub mod wait;

pub use harness::{TestHarness, TestPeer};
pub use signaling::TestSignalingServer;
pub use utils::{
    create_credential_state_for_test, create_peer_with_vnet, create_peer_with_websocket,
    dummy_credential, install_test_crypto_provider, make_actor_id, spawn_echo_responder,
    spawn_response_receiver,
};
pub use vnet::{VNetPair, create_vnet_pair};
pub use wait::*;

pub use crate::transport::lane::{
    WebRtcFragmentSendEvent, WebRtcFragmentSendHook, WebRtcFragmentSendHookGuard,
    install_webrtc_fragment_send_hook_for_test,
};

/// Assert whether an attached node has the runtime hook observer installed.
///
/// Package attach uses this observer to bridge observation hooks into Wasm /
/// DynClib guests; linked attach installs its own linked observer.
pub fn attached_node_has_hook_observer(node: &crate::Node<crate::Attached>) -> bool {
    node.attachment
        .as_ref()
        .expect("Node<Attached> without attachment")
        .node
        .hook_observer
        .is_some()
}

/// Build a lightweight [`RuntimeContext`] backed by an in-process
/// [`HostTransport`] for integration tests that need guest host-calls to
/// complete without starting a full node.
pub fn runtime_context_with_host_transport(
    self_id: ActrId,
    host_transport: Arc<crate::transport::HostTransport>,
) -> crate::context::RuntimeContext {
    use crate::inbound::{DataChunkRegistry, MediaFrameRegistry};
    use crate::outbound::{Gate, HostGate};
    use crate::wire::webrtc::{
        ReconnectConfig, SignalingClient, SignalingConfig, WebSocketSignalingClient,
    };

    let inproc_gate = Gate::Host(Arc::new(HostGate::new(host_transport)));
    let outproc_gate = Some(inproc_gate.clone());
    let signaling_client: Arc<dyn SignalingClient> =
        Arc::new(WebSocketSignalingClient::new(SignalingConfig {
            server_url: url::Url::parse("ws://127.0.0.1:9").expect("valid test URL"),
            connection_timeout: 1,
            heartbeat_interval: 30,
            reconnect_config: ReconnectConfig::default(),
            auth_config: None,
            webrtc_role: None,
        }));

    crate::context::RuntimeContext::new(
        self_id,
        None,
        "package-hook-observer-test".to_string(),
        inproc_gate,
        outproc_gate,
        Arc::new(DataChunkRegistry::new()),
        Arc::new(MediaFrameRegistry::new()),
        signaling_client,
        AIdCredential {
            key_id: 1,
            claims: bytes::Bytes::from_static(b"claims"),
            signature: bytes::Bytes::from(vec![0; 64]),
        },
        None,
        Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        None,
        0,
    )
}

/// Public test-facing mirror of package observation hook events.
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
#[derive(Debug, Clone)]
pub enum TestPackageHookEvent {
    SignalingConnecting,
    SignalingConnected,
    SignalingDisconnected,
    WebSocketConnecting {
        peer: ActrId,
    },
    WebSocketConnected {
        peer: ActrId,
    },
    WebSocketDisconnected {
        peer: ActrId,
    },
    WebRtcConnecting {
        peer: ActrId,
    },
    WebRtcConnected {
        peer: ActrId,
        relayed: bool,
    },
    WebRtcDisconnected {
        peer: ActrId,
        status: actr_framework::WebRtcPeerStatus,
    },
    CredentialRenewed {
        new_expiry: std::time::SystemTime,
    },
    CredentialExpiring {
        new_expiry: std::time::SystemTime,
    },
    MailboxBackpressure {
        queue_len: usize,
        threshold: usize,
    },
}

#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
impl From<TestPackageHookEvent> for crate::workload::PackageHookEvent {
    fn from(event: TestPackageHookEvent) -> Self {
        match event {
            TestPackageHookEvent::SignalingConnecting => Self::SignalingConnecting,
            TestPackageHookEvent::SignalingConnected => Self::SignalingConnected,
            TestPackageHookEvent::SignalingDisconnected => Self::SignalingDisconnected,
            TestPackageHookEvent::WebSocketConnecting { peer } => {
                Self::WebSocketConnecting(actr_framework::PeerEvent {
                    peer,
                    relayed: None,
                    status: None,
                })
            }
            TestPackageHookEvent::WebSocketConnected { peer } => {
                Self::WebSocketConnected(actr_framework::PeerEvent {
                    peer,
                    relayed: None,
                    status: None,
                })
            }
            TestPackageHookEvent::WebSocketDisconnected { peer } => {
                Self::WebSocketDisconnected(actr_framework::PeerEvent {
                    peer,
                    relayed: None,
                    status: None,
                })
            }
            TestPackageHookEvent::WebRtcConnecting { peer } => {
                Self::WebRtcConnecting(actr_framework::PeerEvent {
                    peer,
                    relayed: None,
                    status: Some(actr_framework::WebRtcPeerStatus::Connecting),
                })
            }
            TestPackageHookEvent::WebRtcConnected { peer, relayed } => {
                Self::WebRtcConnected(actr_framework::PeerEvent {
                    peer,
                    relayed: Some(relayed),
                    status: Some(actr_framework::WebRtcPeerStatus::Connected),
                })
            }
            TestPackageHookEvent::WebRtcDisconnected { peer, status } => {
                Self::WebRtcDisconnected(actr_framework::PeerEvent {
                    peer,
                    relayed: None,
                    status: Some(status),
                })
            }
            TestPackageHookEvent::CredentialRenewed { new_expiry } => {
                Self::CredentialRenewed(actr_framework::CredentialEvent { new_expiry })
            }
            TestPackageHookEvent::CredentialExpiring { new_expiry } => {
                Self::CredentialExpiring(actr_framework::CredentialEvent { new_expiry })
            }
            TestPackageHookEvent::MailboxBackpressure {
                queue_len,
                threshold,
            } => Self::MailboxBackpressure(actr_framework::BackpressureEvent {
                queue_len,
                threshold,
            }),
        }
    }
}

/// Test-only wrapper around the package hook observer installed by
/// `Node::attach`. This keeps the crate-private observer trait hidden while
/// allowing integration tests to verify the observer-to-guest bridge.
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
pub struct TestPackageHookObserver {
    observer: crate::workload::PackageHookObserver,
}

#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
impl TestPackageHookObserver {
    fn from_workload(workload: crate::workload::Workload) -> Self {
        let workload_dispatch = Arc::new(crate::executor::spawn_runner(workload));
        Self {
            observer: crate::workload::PackageHookObserver { workload_dispatch },
        }
    }

    pub async fn call(&self, event: TestPackageHookEvent, ctx: &crate::context::RuntimeContext) {
        use crate::lifecycle::hooks::WorkloadHookObserver as _;

        match event {
            TestPackageHookEvent::SignalingConnecting => {
                self.observer.on_signaling_connecting(Some(ctx)).await;
            }
            TestPackageHookEvent::SignalingConnected => {
                self.observer.on_signaling_connected(Some(ctx)).await;
            }
            TestPackageHookEvent::SignalingDisconnected => {
                self.observer.on_signaling_disconnected(ctx).await;
            }
            TestPackageHookEvent::WebSocketConnecting { peer } => {
                self.observer
                    .on_websocket_connecting(
                        ctx,
                        &actr_framework::PeerEvent {
                            peer,
                            relayed: None,
                            status: None,
                        },
                    )
                    .await;
            }
            TestPackageHookEvent::WebSocketConnected { peer } => {
                self.observer
                    .on_websocket_connected(
                        ctx,
                        &actr_framework::PeerEvent {
                            peer,
                            relayed: None,
                            status: None,
                        },
                    )
                    .await;
            }
            TestPackageHookEvent::WebSocketDisconnected { peer } => {
                self.observer
                    .on_websocket_disconnected(
                        ctx,
                        &actr_framework::PeerEvent {
                            peer,
                            relayed: None,
                            status: None,
                        },
                    )
                    .await;
            }
            TestPackageHookEvent::WebRtcConnecting { peer } => {
                self.observer
                    .on_webrtc_connecting(
                        ctx,
                        &actr_framework::PeerEvent {
                            peer,
                            relayed: None,
                            status: Some(actr_framework::WebRtcPeerStatus::Connecting),
                        },
                    )
                    .await;
            }
            TestPackageHookEvent::WebRtcConnected { peer, relayed } => {
                self.observer
                    .on_webrtc_connected(
                        ctx,
                        &actr_framework::PeerEvent {
                            peer,
                            relayed: Some(relayed),
                            status: Some(actr_framework::WebRtcPeerStatus::Connected),
                        },
                    )
                    .await;
            }
            TestPackageHookEvent::WebRtcDisconnected { peer, status } => {
                self.observer
                    .on_webrtc_disconnected(
                        ctx,
                        &actr_framework::PeerEvent {
                            peer,
                            relayed: None,
                            status: Some(status),
                        },
                    )
                    .await;
            }
            TestPackageHookEvent::CredentialRenewed { new_expiry } => {
                self.observer
                    .on_credential_renewed(ctx, &actr_framework::CredentialEvent { new_expiry })
                    .await;
            }
            TestPackageHookEvent::CredentialExpiring { new_expiry } => {
                self.observer
                    .on_credential_expiring(ctx, &actr_framework::CredentialEvent { new_expiry })
                    .await;
            }
            TestPackageHookEvent::MailboxBackpressure {
                queue_len,
                threshold,
            } => {
                self.observer
                    .on_mailbox_backpressure(
                        ctx,
                        &actr_framework::BackpressureEvent {
                            queue_len,
                            threshold,
                        },
                    )
                    .await;
            }
        }
    }
}

pub struct TestDedupWaiter {
    inner: crate::lifecycle::dedup::DedupWaiter,
}

impl std::fmt::Debug for TestDedupWaiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestDedupWaiter").finish_non_exhaustive()
    }
}

impl TestDedupWaiter {
    pub async fn wait(mut self) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        loop {
            if let Some(result) = self.inner.borrow().clone() {
                return result;
            }

            if self.inner.changed().await.is_err() {
                if let Some(result) = self.inner.borrow().clone() {
                    return result;
                }
                return Err(actr_protocol::ActrError::Unavailable(
                    "duplicate request result unavailable".to_string(),
                ));
            }
        }
    }

    pub async fn wait_timeout(
        self,
        timeout: std::time::Duration,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        match tokio::time::timeout(timeout, self.wait()).await {
            Ok(result) => result,
            Err(_) => Err(actr_protocol::ActrError::Unavailable(format!(
                "duplicate request in-flight timed out after {}ms",
                timeout.as_millis()
            ))),
        }
    }
}

#[derive(Debug)]
pub enum TestDedupOutcome {
    Fresh,
    InFlight(TestDedupWaiter),
    Duplicate(actr_protocol::ActorResult<actr_framework::Bytes>),
}

#[derive(Debug)]
pub struct TestDedupState {
    inner: crate::lifecycle::dedup::DedupState,
}

impl TestDedupState {
    pub fn new() -> Self {
        Self {
            inner: crate::lifecycle::dedup::DedupState::new(),
        }
    }

    pub fn check_or_mark(&mut self, request_id: &str) -> TestDedupOutcome {
        match self.inner.check_or_mark(request_id) {
            crate::lifecycle::dedup::DedupOutcome::Fresh => TestDedupOutcome::Fresh,
            crate::lifecycle::dedup::DedupOutcome::InFlight(waiter) => {
                TestDedupOutcome::InFlight(TestDedupWaiter { inner: waiter })
            }
            crate::lifecycle::dedup::DedupOutcome::Duplicate(result) => {
                TestDedupOutcome::Duplicate(result)
            }
        }
    }

    pub fn complete(
        &mut self,
        request_id: &str,
        result: actr_protocol::ActorResult<actr_framework::Bytes>,
    ) {
        self.inner.complete(request_id, result);
    }
}

impl Default for TestDedupState {
    fn default() -> Self {
        Self::new()
    }
}

/// Test-only summary of package loading results.
///
/// This keeps `LoadedWorkload` crate-private while preserving the assertions
/// integration tests care about: selected backend plus parsed manifest.
#[derive(Debug, Clone)]
pub struct LoadedWorkloadSummary {
    pub binary_kind: BinaryKind,
    manifest: PackageManifest,
}

impl LoadedWorkloadSummary {
    pub fn manifest(&self) -> &PackageManifest {
        &self.manifest
    }
}

/// Verify a package, pick the execution backend, and return a test-facing
/// summary without exposing the runtime workload internals on the public API.
pub async fn inspect_workload_package(
    hyper: &Hyper,
    package: &WorkloadPackage,
) -> crate::error::HyperResult<LoadedWorkloadSummary> {
    let loaded = hyper.load_workload_package(package).await?;
    Ok(LoadedWorkloadSummary {
        binary_kind: loaded.binary_kind,
        manifest: loaded.verified.manifest,
    })
}

/// Test-only wrapper around Hyper's internal Component Model workload instance.
#[cfg(feature = "wasm-engine")]
#[derive(Debug)]
pub struct TestWasmWorkload {
    inner: crate::wasm::WasmKernel,
}

#[cfg(feature = "wasm-engine")]
impl TestWasmWorkload {
    pub fn init(&mut self, init_payload: &InitPayloadV1) -> Result<(), crate::wasm::WasmError> {
        self.inner.init(init_payload)
    }

    pub async fn call_on_start(&mut self) -> Result<(), crate::wasm::WasmError> {
        let ctx = InvocationContext {
            self_id: actr_protocol::ActrId::default(),
            caller_id: None,
            request_id: "test:on_start".to_string(),
        };
        let host_abi: HostAbiFn =
            std::sync::Arc::new(|_| Box::pin(async { HostOperationResult::Done }));
        self.inner.call_on_start(ctx, &host_abi).await
    }

    pub async fn call_on_ready(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> Result<(), crate::wasm::WasmError> {
        self.inner.call_on_ready(ctx, host_abi).await
    }

    pub async fn call_on_stop(
        &mut self,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> Result<(), crate::wasm::WasmError> {
        self.inner.call_on_stop(ctx, host_abi).await
    }

    pub async fn call_hook_event(
        &mut self,
        event: TestPackageHookEvent,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> Result<(), crate::wasm::WasmError> {
        self.inner
            .call_hook_event(event.into(), ctx, host_abi)
            .await
    }

    pub async fn handle(
        &mut self,
        request_bytes: &[u8],
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> Result<Vec<u8>, crate::wasm::WasmError> {
        self.inner.handle(request_bytes, ctx, host_abi).await
    }

    pub async fn handle_data_chunk(
        &mut self,
        chunk: actr_protocol::DataChunk,
        sender: ActrId,
        ctx: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> Result<(), crate::wasm::WasmError> {
        self.inner
            .handle_data_chunk(chunk, sender, ctx, host_abi)
            .await
    }

    /// Number of times the underlying store was rebuilt after a guest trap.
    /// Lets integration tests assert that a trap triggered a rebuild and a
    /// business error did not.
    pub fn rebuild_count(&self) -> u64 {
        self.inner.rebuild_count()
    }

    pub fn into_package_hook_observer(self) -> TestPackageHookObserver {
        TestPackageHookObserver::from_workload(crate::workload::Workload::Wasm(self.inner))
    }
}

/// Instantiate a Component Model workload for integration tests without
/// exposing Hyper's internal runtime workload type on the public API.
#[cfg(feature = "wasm-engine")]
pub async fn instantiate_wasm_workload(
    host: &crate::wasm::WasmHost,
) -> Result<TestWasmWorkload, crate::wasm::WasmError> {
    Ok(TestWasmWorkload {
        inner: host.instantiate().await?,
    })
}

/// Test-only wrapper around Hyper's internal dynclib workload instance.
#[cfg(feature = "dynclib-engine")]
#[derive(Debug)]
pub struct TestDynclibWorkload {
    inner: crate::dynclib::DynClibWorkload,
}

#[cfg(feature = "dynclib-engine")]
impl TestDynclibWorkload {
    pub async fn handle(
        &mut self,
        request_bytes: &[u8],
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> Result<Vec<u8>, crate::dynclib::DynclibError> {
        self.inner.handle(request_bytes, ctx, call_executor).await
    }

    pub async fn call_hook_event(
        &mut self,
        event: TestPackageHookEvent,
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> Result<(), crate::dynclib::DynclibError> {
        self.inner
            .call_hook_event(event.into(), ctx, call_executor)
            .await
    }

    pub async fn call_on_ready(
        &mut self,
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> Result<(), crate::dynclib::DynclibError> {
        self.inner.call_on_ready(ctx, call_executor).await
    }

    pub async fn call_on_stop(
        &mut self,
        ctx: InvocationContext,
        call_executor: &HostAbiFn,
    ) -> Result<(), crate::dynclib::DynclibError> {
        self.inner.call_on_stop(ctx, call_executor).await
    }

    pub fn into_package_hook_observer(self) -> TestPackageHookObserver {
        TestPackageHookObserver::from_workload(crate::workload::Workload::DynClib(self.inner))
    }
}

/// Instantiate a dynclib workload for integration tests while keeping
/// `DynclibInstance` crate-private.
#[cfg(feature = "dynclib-engine")]
pub fn instantiate_dynclib_workload(
    host: crate::dynclib::DynclibHost,
    init_payload: &InitPayloadV1,
) -> Result<TestDynclibWorkload, crate::dynclib::DynclibError> {
    let instance = host.instantiate(init_payload)?;
    Ok(TestDynclibWorkload {
        inner: crate::dynclib::DynClibWorkload::new(host, instance),
    })
}

/// Test-only driver that exercises a workload **through the per-actor serial
/// command runner** (the production dispatch path), rather than calling the
/// engine workload's `&mut self` methods directly. Lets integration tests
/// assert that dispatch / lifecycle / trap-recovery behave identically once
/// funneled through the command channel.
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
pub struct TestWorkloadRunner {
    handle: crate::executor::ActorHandle,
}

#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
impl TestWorkloadRunner {
    /// Spawn a serial runner owning `workload`. Crate-internal because
    /// `Workload` is a `pub(crate)` type; integration tests obtain a runner via
    /// [`TestWasmWorkload::into_workload_runner`].
    pub(crate) fn spawn(workload: crate::workload::Workload) -> Self {
        Self {
            handle: crate::executor::spawn_runner(workload),
        }
    }

    fn scratch_ctx() -> crate::context::RuntimeContext {
        runtime_context_with_host_transport(
            ActrId::default(),
            Arc::new(crate::transport::HostTransport::new()),
        )
    }

    /// Dispatch one encoded RPC envelope through the runner.
    pub async fn dispatch(
        &self,
        envelope_bytes: &[u8],
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        use actr_protocol::prost::Message as _;
        let envelope = actr_protocol::RpcEnvelope::decode(envelope_bytes)
            .map_err(|e| actr_protocol::ActrError::DecodeFailure(e.to_string()))?;
        self.handle
            .dispatch_envelope(envelope, Self::scratch_ctx(), invocation, host_abi)
            .await
    }

    /// Drive the `on_start` lifecycle hook through the runner.
    pub async fn on_start(
        &self,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<()> {
        self.handle
            .on_start(Self::scratch_ctx(), invocation, host_abi)
            .await
    }

    /// Deliver a DataChunk barrier through the production actor runner.
    pub async fn data_chunk(
        &self,
        chunk: actr_protocol::DataChunk,
        sender: ActrId,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<()> {
        self.handle
            .dispatch_data_chunk(chunk, sender, invocation, host_abi)
            .await
    }

    /// Deterministically stop the runner and wait for its task to finish.
    pub async fn shutdown(&self) {
        self.handle.shutdown().await;
    }
}

#[cfg(feature = "wasm-engine")]
impl TestWasmWorkload {
    /// Move this workload behind a serial command runner for tests that drive
    /// the production dispatch path (see [`TestWorkloadRunner`]).
    pub fn into_workload_runner(self) -> TestWorkloadRunner {
        TestWorkloadRunner::spawn(crate::workload::Workload::Wasm(self.inner))
    }

    /// Move this workload behind an **interleaved** command runner — for a
    /// 0.2.0 (V2) kernel this drives the resident `run_concurrent` region so
    /// distinct commands submitted concurrently really interleave inside the
    /// one instance (M5). `dispatch_timeout` is supervised outside the region
    /// so expiry can discard the physical Store before replying.
    pub fn into_interleaved_runner(
        self,
        dispatch_timeout: Option<std::time::Duration>,
    ) -> TestWorkloadRunner {
        TestWorkloadRunner {
            handle: crate::executor::spawn_runner_with_mode(
                crate::workload::Workload::Wasm(self.inner),
                crate::executor::RunnerMode::Interleaved,
                dispatch_timeout,
            ),
        }
    }

    /// Wire this workload behind the full production dispatch path: a
    /// conflict-key scheduler (budget `C` / queue cap `M`) in front of an
    /// interleaved runner. This is the exact shape the node builds when the
    /// dispatch-concurrency gate is on, and is what the M5 concurrency tests
    /// drive to exercise same-key serial + distinct-key interleave together.
    pub fn into_concurrent_dispatcher(
        self,
        spec: crate::dispatch::ConflictKeySpec,
        budget: usize,
        queue_cap: usize,
        dispatch_timeout: Option<std::time::Duration>,
    ) -> TestConcurrentDispatcher {
        let handle = crate::executor::spawn_runner_with_mode(
            crate::workload::Workload::Wasm(self.inner),
            crate::executor::RunnerMode::Interleaved,
            dispatch_timeout,
        );
        let scheduler = crate::dispatch::scheduler::SchedulerHandle::spawn(budget, queue_cap);
        TestConcurrentDispatcher {
            handle: Arc::new(handle),
            scheduler: Arc::new(scheduler),
            spec: Arc::new(spec),
            next_request_id: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

/// Test driver that mirrors the node's gate-on dispatch path: it projects each
/// inbound RPC to its [`crate::dispatch::ConflictKey`] via a
/// [`crate::dispatch::ConflictKeySpec`], submits it to a budgeted conflict-key
/// scheduler, and lets the scheduler feed an **interleaved** wasm runner. Lets
/// M5 integration tests assert same-key serial + distinct-key interleave
/// end-to-end without standing up a whole node.
#[cfg(feature = "wasm-engine")]
pub struct TestConcurrentDispatcher {
    handle: Arc<crate::executor::ActorHandle>,
    scheduler: Arc<crate::dispatch::scheduler::SchedulerHandle>,
    spec: Arc<crate::dispatch::ConflictKeySpec>,
    next_request_id: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "wasm-engine")]
impl TestConcurrentDispatcher {
    fn scratch_ctx() -> crate::context::RuntimeContext {
        runtime_context_with_host_transport(
            ActrId::default(),
            Arc::new(crate::transport::HostTransport::new()),
        )
    }

    /// Dispatch one RPC through scheduler → interleaved runner. The conflict key
    /// is derived from `caller_id` (when the route is declared `KeySource::Sender`)
    /// so a test controls concurrency purely by which caller it passes: distinct
    /// callers → distinct keys → concurrent; same caller → same key → serial.
    pub async fn dispatch(
        &self,
        route_key: &str,
        payload: Vec<u8>,
        caller_id: Option<ActrId>,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        use std::sync::atomic::Ordering;
        let rid = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let request_id = format!("concurrent-req-{rid}");
        let payload_bytes = actr_framework::Bytes::from(payload);
        let envelope = actr_protocol::RpcEnvelope {
            route_key: route_key.to_string(),
            payload: Some(payload_bytes.clone()),
            request_id: request_id.clone(),
            direction: Some(actr_protocol::Direction::Request as i32),
            ..Default::default()
        };
        let key = self
            .spec
            .extract(route_key, caller_id.as_ref(), payload_bytes.as_ref());
        let invocation = InvocationContext {
            self_id: ActrId::default(),
            caller_id,
            request_id,
        };
        let handle = self.handle.clone();
        let host_abi = host_abi.clone();
        let ctx = Self::scratch_ctx();
        let run: crate::dispatch::scheduler::DispatchFn = Box::new(move || {
            Box::pin(async move {
                handle
                    .dispatch_envelope(envelope, ctx, invocation, &host_abi)
                    .await
            })
        });
        let rx = self.scheduler.submit(key, run).await;
        rx.await.unwrap_or_else(|_| {
            Err(actr_protocol::ActrError::Unavailable(
                "dispatch scheduler terminated".to_string(),
            ))
        })
    }

    /// Deterministically tear down the scheduler then the runner.
    pub async fn shutdown(&self) {
        self.scheduler.shutdown().await;
        self.handle.shutdown().await;
    }
}

/// Basis-agnostic view of a conflict-key dispatcher: the exact production shape
/// (a budgeted conflict-key scheduler feeding an **interleaved** runner) exposed
/// through one signature so a single property-test body can drive either the
/// WASM V2 kernel ([`TestConcurrentDispatcher`]) or the native `Linked` runner
/// ([`TestNativeConcurrentDispatcher`]). Proving the two satisfy the SAME
/// conflict-key properties through this one interface is the M6 isomorphism.
///
/// `host_abi` is the guest→host bridge. On WASM it intercepts `ctx.call_raw`
/// suspension points directly; on native the guest's `ctx.call_raw` instead
/// travels the shared `HostTransport` (see
/// [`TestNativeConcurrentDispatcher::host_transport`]) and this argument is
/// accepted for signature parity but ignored by the native runner.
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
#[async_trait::async_trait]
pub trait ConcurrentDispatch: Send + Sync {
    /// Dispatch one RPC through the scheduler → interleaved runner. Distinct
    /// callers project to distinct conflict keys (eligible to interleave); the
    /// same caller projects to the same key (strictly serial).
    async fn dispatch(
        &self,
        route_key: &str,
        payload: Vec<u8>,
        caller_id: Option<ActrId>,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes>;

    /// Deterministically tear down the scheduler then the runner.
    async fn shutdown(&self);
}

#[cfg(feature = "wasm-engine")]
#[async_trait::async_trait]
impl ConcurrentDispatch for TestConcurrentDispatcher {
    async fn dispatch(
        &self,
        route_key: &str,
        payload: Vec<u8>,
        caller_id: Option<ActrId>,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        TestConcurrentDispatcher::dispatch(self, route_key, payload, caller_id, host_abi).await
    }

    async fn shutdown(&self) {
        TestConcurrentDispatcher::shutdown(self).await
    }
}

/// Native mirror of [`TestConcurrentDispatcher`]: a budgeted conflict-key
/// scheduler feeding the **interleaved** runner for a `Workload::Linked`
/// in-process guest (`executor::run_loop_interleaved`). It is deliberately
/// signature-identical to the WASM dispatcher so the M6 isomorphism suite drives
/// both through [`ConcurrentDispatch`].
///
/// The one structural difference from the WASM path is where the guest's
/// `ctx.call_raw` suspension points land: the native `Linked` runner ignores the
/// per-dispatch `HostAbiFn`, so every dispatch shares ONE
/// [`crate::transport::HostTransport`] and a gate harness drains it via
/// [`crate::transport::HostTransport::recv_reliable_raw`] to hold guest calls
/// suspended and release them deterministically — the native equivalent of the
/// WASM `HostAbiFn` bridge. Sharing one transport across the concurrent
/// dispatches is what lets a single reader observe every co-resident guest call
/// (each carries a unique uuid `request_id`, so the shared correlation map never
/// collides).
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
pub struct TestNativeConcurrentDispatcher {
    handle: Arc<crate::executor::ActorHandle>,
    scheduler: Arc<crate::dispatch::scheduler::SchedulerHandle>,
    spec: Arc<crate::dispatch::ConflictKeySpec>,
    transport: Arc<crate::transport::HostTransport>,
    next_request_id: std::sync::atomic::AtomicU64,
}

#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
impl TestNativeConcurrentDispatcher {
    /// Wire a native `Linked` workload behind the production gate-on shape: a
    /// conflict-key scheduler (budget `C` / queue cap `M`) in front of an
    /// interleaved runner. `dispatch_timeout` uses the same fail-closed policy as
    /// the WASM path: the triggering call resolves `TimedOut`, siblings fail,
    /// and the native runner terminates because arbitrary linked actor state
    /// cannot be reconstructed safely after cancellation. An unexpected guest
    /// panic is contained the same way after returning `Internal` to its caller.
    pub fn spawn<W: actr_framework::Workload>(
        workload: W,
        spec: crate::dispatch::ConflictKeySpec,
        budget: usize,
        queue_cap: usize,
        dispatch_timeout: Option<std::time::Duration>,
    ) -> Self {
        let adapter: Arc<dyn crate::workload::LinkedWorkloadHandle> =
            crate::workload::WorkloadAdapter::new(workload);
        let handle = crate::executor::spawn_runner_with_mode(
            crate::workload::Workload::Linked(adapter),
            crate::executor::RunnerMode::Interleaved,
            dispatch_timeout,
        );
        let scheduler = crate::dispatch::scheduler::SchedulerHandle::spawn(budget, queue_cap);
        Self {
            handle: Arc::new(handle),
            scheduler: Arc::new(scheduler),
            spec: Arc::new(spec),
            transport: Arc::new(crate::transport::HostTransport::new()),
            next_request_id: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The shared host transport backing every dispatch's `RuntimeContext`. A
    /// gate harness drains it (`recv_reliable_raw`) to intercept the guest's
    /// `ctx.call_raw` suspension points and release them deterministically.
    pub fn host_transport(&self) -> Arc<crate::transport::HostTransport> {
        self.transport.clone()
    }

    fn ctx(&self) -> crate::context::RuntimeContext {
        runtime_context_with_host_transport(ActrId::default(), self.transport.clone())
    }

    /// Dispatch one RPC through scheduler → interleaved native runner. Mirrors
    /// [`TestConcurrentDispatcher::dispatch`] exactly, differing only in that the
    /// `RuntimeContext` is built from the shared transport (so guest host-calls
    /// are observable at the gate).
    pub async fn dispatch(
        &self,
        route_key: &str,
        payload: Vec<u8>,
        caller_id: Option<ActrId>,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        use std::sync::atomic::Ordering;
        let rid = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let request_id = format!("native-concurrent-req-{rid}");
        let payload_bytes = actr_framework::Bytes::from(payload);
        let envelope = actr_protocol::RpcEnvelope {
            route_key: route_key.to_string(),
            payload: Some(payload_bytes.clone()),
            request_id: request_id.clone(),
            direction: Some(actr_protocol::Direction::Request as i32),
            ..Default::default()
        };
        let key = self
            .spec
            .extract(route_key, caller_id.as_ref(), payload_bytes.as_ref());
        let invocation = InvocationContext {
            self_id: ActrId::default(),
            caller_id,
            request_id,
        };
        let handle = self.handle.clone();
        let host_abi = host_abi.clone();
        let ctx = self.ctx();
        let run: crate::dispatch::scheduler::DispatchFn = Box::new(move || {
            Box::pin(async move {
                handle
                    .dispatch_envelope(envelope, ctx, invocation, &host_abi)
                    .await
            })
        });
        let rx = self.scheduler.submit(key, run).await;
        rx.await.unwrap_or_else(|_| {
            Err(actr_protocol::ActrError::Unavailable(
                "dispatch scheduler terminated".to_string(),
            ))
        })
    }

    /// Deterministically tear down the scheduler then the runner.
    pub async fn shutdown(&self) {
        self.scheduler.shutdown().await;
        self.handle.shutdown().await;
    }
}

#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
#[async_trait::async_trait]
impl ConcurrentDispatch for TestNativeConcurrentDispatcher {
    async fn dispatch(
        &self,
        route_key: &str,
        payload: Vec<u8>,
        caller_id: Option<ActrId>,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        TestNativeConcurrentDispatcher::dispatch(self, route_key, payload, caller_id, host_abi)
            .await
    }

    async fn shutdown(&self) {
        TestNativeConcurrentDispatcher::shutdown(self).await
    }
}

/// Strategy-A **keyless serial** driver (WASM V2): a `RunnerMode::Serial` runner
/// with NO conflict-key scheduler in front — the exact shape the node builds for
/// a keyless actor under default-on. It exposes the same [`ConcurrentDispatch`]
/// interface as the gate-on dispatchers so one generic property body can assert
/// that, keyless, even distinct callers never interleave (MAX_SEEN == 1). There
/// is deliberately no conflict-key projection here: a keyless actor never
/// projects keys at all.
#[cfg(feature = "wasm-engine")]
pub struct TestSerialDispatcher {
    handle: Arc<crate::executor::ActorHandle>,
    next_request_id: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "wasm-engine")]
impl TestWasmWorkload {
    /// Wire this workload behind the keyless default-on path: a serial runner,
    /// no scheduler. See [`TestSerialDispatcher`].
    pub fn into_serial_dispatcher(self) -> TestSerialDispatcher {
        let handle = crate::executor::spawn_runner_with_mode(
            crate::workload::Workload::Wasm(self.inner),
            crate::executor::RunnerMode::Serial,
            None,
        );
        TestSerialDispatcher {
            handle: Arc::new(handle),
            next_request_id: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[cfg(feature = "wasm-engine")]
impl TestSerialDispatcher {
    fn scratch_ctx() -> crate::context::RuntimeContext {
        runtime_context_with_host_transport(
            ActrId::default(),
            Arc::new(crate::transport::HostTransport::new()),
        )
    }

    /// Dispatch one RPC straight through the serial runner (no scheduler). The
    /// `caller_id` still rides the envelope for parity, but with no scheduler it
    /// cannot buy any concurrency — the runner is serial by construction.
    pub async fn dispatch(
        &self,
        route_key: &str,
        payload: Vec<u8>,
        caller_id: Option<ActrId>,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        use std::sync::atomic::Ordering;
        let rid = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let request_id = format!("serial-req-{rid}");
        let payload_bytes = actr_framework::Bytes::from(payload);
        let envelope = actr_protocol::RpcEnvelope {
            route_key: route_key.to_string(),
            payload: Some(payload_bytes),
            request_id: request_id.clone(),
            direction: Some(actr_protocol::Direction::Request as i32),
            ..Default::default()
        };
        let invocation = InvocationContext {
            self_id: ActrId::default(),
            caller_id,
            request_id,
        };
        self.handle
            .dispatch_envelope(envelope, Self::scratch_ctx(), invocation, host_abi)
            .await
    }

    /// Deterministically tear down the runner.
    pub async fn shutdown(&self) {
        self.handle.shutdown().await;
    }
}

#[cfg(feature = "wasm-engine")]
#[async_trait::async_trait]
impl ConcurrentDispatch for TestSerialDispatcher {
    async fn dispatch(
        &self,
        route_key: &str,
        payload: Vec<u8>,
        caller_id: Option<ActrId>,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        TestSerialDispatcher::dispatch(self, route_key, payload, caller_id, host_abi).await
    }

    async fn shutdown(&self) {
        TestSerialDispatcher::shutdown(self).await
    }
}

/// Native mirror of [`TestSerialDispatcher`]: a `RunnerMode::Serial` runner over
/// a `Workload::Linked` in-process guest, NO scheduler — the native keyless
/// default-on path. Guest `ctx.call_raw` suspension points travel the shared
/// [`crate::transport::HostTransport`] exactly as in
/// [`TestNativeConcurrentDispatcher`], so the same native gate harness drives it.
#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
pub struct TestNativeSerialDispatcher {
    handle: Arc<crate::executor::ActorHandle>,
    transport: Arc<crate::transport::HostTransport>,
    next_request_id: std::sync::atomic::AtomicU64,
}

#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
impl TestNativeSerialDispatcher {
    /// Wire a native `Linked` workload behind the keyless default-on path: a
    /// serial runner, no scheduler.
    pub fn spawn<W: actr_framework::Workload>(workload: W) -> Self {
        let adapter: Arc<dyn crate::workload::LinkedWorkloadHandle> =
            crate::workload::WorkloadAdapter::new(workload);
        let handle = crate::executor::spawn_runner_with_mode(
            crate::workload::Workload::Linked(adapter),
            crate::executor::RunnerMode::Serial,
            None,
        );
        Self {
            handle: Arc::new(handle),
            transport: Arc::new(crate::transport::HostTransport::new()),
            next_request_id: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The shared host transport backing every dispatch's `RuntimeContext` (the
    /// native gate drains it — the same shape as [`TestNativeConcurrentDispatcher`]).
    pub fn host_transport(&self) -> Arc<crate::transport::HostTransport> {
        self.transport.clone()
    }

    fn ctx(&self) -> crate::context::RuntimeContext {
        runtime_context_with_host_transport(ActrId::default(), self.transport.clone())
    }

    /// Dispatch one RPC straight through the serial native runner (no scheduler).
    pub async fn dispatch(
        &self,
        route_key: &str,
        payload: Vec<u8>,
        caller_id: Option<ActrId>,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        use std::sync::atomic::Ordering;
        let rid = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let request_id = format!("native-serial-req-{rid}");
        let payload_bytes = actr_framework::Bytes::from(payload);
        let envelope = actr_protocol::RpcEnvelope {
            route_key: route_key.to_string(),
            payload: Some(payload_bytes),
            request_id: request_id.clone(),
            direction: Some(actr_protocol::Direction::Request as i32),
            ..Default::default()
        };
        let invocation = InvocationContext {
            self_id: ActrId::default(),
            caller_id,
            request_id,
        };
        self.handle
            .dispatch_envelope(envelope, self.ctx(), invocation, host_abi)
            .await
    }

    /// Deterministically tear down the runner.
    pub async fn shutdown(&self) {
        self.handle.shutdown().await;
    }
}

#[cfg(any(feature = "wasm-engine", feature = "dynclib-engine"))]
#[async_trait::async_trait]
impl ConcurrentDispatch for TestNativeSerialDispatcher {
    async fn dispatch(
        &self,
        route_key: &str,
        payload: Vec<u8>,
        caller_id: Option<ActrId>,
        host_abi: &HostAbiFn,
    ) -> actr_protocol::ActorResult<actr_framework::Bytes> {
        TestNativeSerialDispatcher::dispatch(self, route_key, payload, caller_id, host_abi).await
    }

    async fn shutdown(&self) {
        TestNativeSerialDispatcher::shutdown(self).await
    }
}

// The shared M6 conflict-key concurrency harness (route consts, conflict-key
// spec, gate bridges for both bases, watchdog helpers). Lives under
// `tests/common/` but is compiled into the library so its `pub` items are
// externally reachable (no per-integration-test-crate dead-code warnings), the
// same pattern the WebRTC `harness`/`wait`/… modules above use.
#[cfg(feature = "wasm-engine")]
#[path = "../../tests/common/concurrency.rs"]
pub mod concurrency;
