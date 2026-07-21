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
        let workload_dispatch = Arc::new(tokio::sync::Mutex::new(workload));
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

    /// Deterministically stop the owning workload runner and its backend.
    pub async fn shutdown(&self) {
        self.observer.workload_dispatch.shutdown().await;
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
    inner: crate::wasm::WasmWorkload,
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

    pub async fn shutdown(&mut self) -> Result<(), crate::dynclib::DynclibError> {
        self.inner.shutdown().await
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

#[cfg(feature = "dynclib-engine")]
pub fn dynclib_active_bridge_count() -> usize {
    crate::dynclib::active_bridge_count()
}
