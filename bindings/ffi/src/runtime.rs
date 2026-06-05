//! Runtime wrappers for UniFFI export

use crate::error::{ActrError, ActrResult};
use crate::types::{
    ActrId, ActrType, AppLifecycleState, CleanupReason, NetworkEventResult, NetworkSnapshot,
    PayloadType, ReconnectReason,
};
use crate::workload::DynamicWorkload;
use actr_framework::{Bytes, Dest};
use actr_hyper::{ActrRef, NetworkEventHandle, Node, Registered, WorkloadPackage};
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Wrapper for a package-backed runtime before startup.
#[derive(uniffi::Object)]
pub struct ActrNode {
    inner: Mutex<Option<Node<Registered>>>,
    network_event_handle: Mutex<Option<NetworkEventHandle>>,
}

#[uniffi::export]
impl ActrNode {
    /// Create a new runtime wrapper from config and a verified `.actr` package file.
    #[uniffi::constructor(async_runtime = "tokio")]
    pub async fn new_from_package_file(
        config_path: String,
        package_path: String,
    ) -> ActrResult<Arc<Self>> {
        let package_bytes = std::fs::read(&package_path).map_err(|e| {
            error!("Failed to read package at {}: {}", package_path, e);
            ActrError::Config {
                msg: format!("Failed to read package at {}: {}", package_path, e),
            }
        })?;
        let package = WorkloadPackage::new(package_bytes);

        // Node::from_config_file owns config parsing, [hyper] section
        // parsing (data_dir / trust), and Hyper construction — the shell
        // only composes observability + attach + register + start on top
        // of the returned Node<Init>.
        let init = Node::from_config_file(&config_path).await.map_err(|e| {
            error!("Failed to load runtime config: {}", e);
            ActrError::Config {
                msg: format!("Failed to load runtime config `{}`: {}", config_path, e),
            }
        })?;
        crate::logger::init_observability(init.runtime_config().observability.clone());

        info!(
            config_path = %config_path,
            package_path = %package_path,
            "Creating package-backed runtime wrapper",
        );

        let attached = init.attach(&package).await.map_err(|e| {
            error!("Failed to attach package-backed node: {}", e);
            ActrError::Internal {
                msg: format!("Failed to attach package-backed node: {e}"),
            }
        })?;
        let ais_endpoint = attached.ais_endpoint().to_string();
        let registered = attached.register(&ais_endpoint).await.map_err(|e| {
            error!("AIS registration failed: {}", e);
            ActrError::Internal {
                msg: format!("AIS registration failed: {e}"),
            }
        })?;

        Ok(Arc::new(Self {
            inner: Mutex::new(Some(registered)),
            network_event_handle: Mutex::new(None),
        }))
    }

    /// Create a linked/static runtime from a foreign-language workload.
    #[uniffi::constructor(async_runtime = "tokio")]
    pub async fn new_from_linked_workload(
        config_path: String,
        actor_type: ActrType,
        workload: Arc<DynamicWorkload>,
    ) -> ActrResult<Arc<Self>> {
        let actor_type: actr_protocol::ActrType = actor_type.into();
        let init = load_linked_init(&config_path).await?;
        let attached = init
            .with_actor_type(actor_type.clone())
            .link(workload.as_ref().clone())
            .await
            .map_err(|e| ActrError::Internal {
                msg: format!("Failed to link foreign workload: {e}"),
            })?;
        register_linked_node(attached).await
    }

    /// Create a network event handle for platform callbacks.
    ///
    /// This must be called before `start()`.
    pub fn create_network_event_handle(&self) -> ActrResult<Arc<NetworkEventHandleWrapper>> {
        let mut handle_guard = self.network_event_handle.lock();
        if let Some(handle) = handle_guard.as_ref() {
            info!(
                api = "create_network_event_handle",
                reused = true,
                "network_event.ffi.handle_created"
            );
            return Ok(Arc::new(NetworkEventHandleWrapper {
                inner: handle.clone(),
            }));
        }

        let mut node_guard = self.inner.lock();
        let node = node_guard.as_mut().ok_or_else(|| ActrError::Internal {
            msg: "runtime node is no longer available".to_string(),
        })?;

        let handle = node.create_network_event_handle(0);
        *handle_guard = Some(handle.clone());

        info!(
            api = "create_network_event_handle",
            reused = false,
            debounce_ms = 0_u64,
            "network_event.ffi.handle_created"
        );

        Ok(Arc::new(NetworkEventHandleWrapper { inner: handle }))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl ActrNode {
    /// Start the package-backed node and return a running actor reference.
    pub async fn start(self: Arc<Self>) -> ActrResult<Arc<ActrRefWrapper>> {
        let hyper = self
            .inner
            .lock()
            .take()
            .ok_or_else(|| ActrError::Internal {
                msg: "ActrNode already started".to_string(),
            })?;

        let actr_ref = hyper.start().await.map_err(ActrError::from)?;

        Ok(Arc::new(ActrRefWrapper { inner: actr_ref }))
    }
}

/// Wrapper for `NetworkEventHandle` - network lifecycle callbacks.
#[derive(uniffi::Object)]
pub struct NetworkEventHandleWrapper {
    inner: NetworkEventHandle,
}

#[uniffi::export(async_runtime = "tokio")]
impl NetworkEventHandleWrapper {
    /// Handle a full network path change.
    pub async fn handle_network_path_changed(
        &self,
        snapshot: NetworkSnapshot,
    ) -> ActrResult<NetworkEventResult> {
        info!(
            api = "handle_network_path_changed",
            snapshot = ?snapshot,
            "network_event.ffi.event_received"
        );

        let result = self
            .inner
            .handle_network_path_changed(snapshot.into())
            .await
            .map_err(|e| {
                warn!(
                    api = "handle_network_path_changed",
                    error = %e,
                    "network_event.ffi.event_failed"
                );
                ActrError::Internal { msg: e }
            })?;
        let result = result.into();
        log_network_event_result("handle_network_path_changed", &result);
        Ok(result)
    }

    /// Handle an app lifecycle change.
    pub async fn handle_app_lifecycle_changed(
        &self,
        state: AppLifecycleState,
    ) -> ActrResult<NetworkEventResult> {
        info!(
            api = "handle_app_lifecycle_changed",
            state = ?state,
            "network_event.ffi.event_received"
        );

        let result = self
            .inner
            .handle_app_lifecycle_changed(state.into())
            .await
            .map_err(|e| {
                warn!(
                    api = "handle_app_lifecycle_changed",
                    error = %e,
                    "network_event.ffi.event_failed"
                );
                ActrError::Internal { msg: e }
            })?;
        let result = result.into();
        log_network_event_result("handle_app_lifecycle_changed", &result);
        Ok(result)
    }

    /// Cleanup all connections without reconnecting.
    pub async fn cleanup_connections(
        &self,
        reason: CleanupReason,
    ) -> ActrResult<NetworkEventResult> {
        info!(
            api = "cleanup_connections",
            reason = ?reason,
            "network_event.ffi.event_received"
        );

        let result = self
            .inner
            .cleanup_connections(reason.into())
            .await
            .map_err(|e| {
                warn!(
                    api = "cleanup_connections",
                    error = %e,
                    "network_event.ffi.event_failed"
                );
                ActrError::Internal { msg: e }
            })?;
        let result = result.into();
        log_network_event_result("cleanup_connections", &result);
        Ok(result)
    }

    /// Force cleanup and reconnect.
    pub async fn force_reconnect(&self, reason: ReconnectReason) -> ActrResult<NetworkEventResult> {
        info!(
            api = "force_reconnect",
            reason = ?reason,
            "network_event.ffi.event_received"
        );

        let result = self
            .inner
            .force_reconnect(reason.into())
            .await
            .map_err(|e| {
                warn!(
                    api = "force_reconnect",
                    error = %e,
                    "network_event.ffi.event_failed"
                );
                ActrError::Internal { msg: e }
            })?;
        let result = result.into();
        log_network_event_result("force_reconnect", &result);
        Ok(result)
    }
}

fn log_network_event_result(api: &'static str, result: &NetworkEventResult) {
    if result.success {
        info!(
            api,
            event = ?result.event,
            duration_ms = result.duration_ms,
            "network_event.ffi.event_completed"
        );
    } else {
        warn!(
            api,
            event = ?result.event,
            duration_ms = result.duration_ms,
            error = ?result.error,
            "network_event.ffi.event_completed"
        );
    }
}

/// Wrapper for a running actor reference.
#[derive(uniffi::Object)]
pub struct ActrRefWrapper {
    inner: ActrRef,
}

#[uniffi::export(async_runtime = "tokio")]
impl ActrRefWrapper {
    /// Get the actor's ID.
    pub fn actor_id(&self) -> ActrId {
        self.inner.actor_id().clone().into()
    }

    /// Discover actors of the specified type.
    pub async fn discover(&self, target_type: ActrType, count: u32) -> ActrResult<Vec<ActrId>> {
        let proto_type: actr_protocol::ActrType = target_type.into();
        info!(
            "discover: looking for {} (count={count})",
            proto_type.to_string_repr(),
        );

        match self
            .inner
            .discover_route_candidates(&proto_type, count as usize)
            .await
        {
            Ok(ids) => {
                info!("discover: found {} candidates", ids.len());
                for id in &ids {
                    debug!("candidate: {}", id.to_string_repr());
                }
                Ok(ids.into_iter().map(Into::into).collect())
            }
            Err(e) => {
                error!("discover failed: {}", e);
                Err(ActrError::from(e))
            }
        }
    }

    /// Trigger shutdown.
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }

    /// Wait for shutdown to complete.
    pub async fn wait_for_shutdown(&self) {
        self.inner.wait_for_shutdown().await;
    }

    /// Check if shutdown is already in progress.
    pub fn is_shutting_down(&self) -> bool {
        self.inner.is_shutting_down()
    }

    /// Call the local guest workload via RPC.
    pub async fn call(
        &self,
        route_key: String,
        payload_type: PayloadType,
        request_payload: Vec<u8>,
        timeout_ms: i64,
    ) -> ActrResult<Vec<u8>> {
        let proto_payload_type: actr_protocol::PayloadType = payload_type.into();
        let ctx = self.inner.app_context().await;

        let response_bytes = ctx
            .call_raw(
                &Dest::Local,
                route_key,
                proto_payload_type,
                Bytes::from(request_payload),
                timeout_ms,
            )
            .await?;

        Ok(response_bytes.to_vec())
    }

    /// Send a one-way message to the local guest workload.
    pub async fn tell(
        &self,
        route_key: String,
        payload_type: PayloadType,
        message_payload: Vec<u8>,
    ) -> ActrResult<()> {
        let proto_payload_type: actr_protocol::PayloadType = payload_type.into();
        let ctx = self.inner.app_context().await;

        ctx.tell_raw(
            &Dest::Local,
            route_key,
            proto_payload_type,
            Bytes::from(message_payload),
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
impl ActrRefWrapper {
    pub(crate) async fn app_context_for_test(&self) -> actr_hyper::context::RuntimeContext {
        self.inner.app_context().await
    }
}

async fn load_linked_init(config_path: &str) -> ActrResult<Node<actr_hyper::Init>> {
    let init = Node::from_config_file(config_path).await.map_err(|e| {
        error!("Failed to load runtime config: {}", e);
        ActrError::Config {
            msg: format!("Failed to load runtime config `{}`: {}", config_path, e),
        }
    })?;
    crate::logger::init_observability(init.runtime_config().observability.clone());
    info!(config_path = %config_path, "Creating linked runtime wrapper");
    Ok(init)
}

async fn register_linked_node(attached: Node<actr_hyper::Attached>) -> ActrResult<Arc<ActrNode>> {
    let ais_endpoint = attached.ais_endpoint().to_string();
    let registered = attached.register(&ais_endpoint).await.map_err(|e| {
        error!("AIS registration failed: {}", e);
        ActrError::Internal {
            msg: format!("AIS registration failed: {e}"),
        }
    })?;

    Ok(Arc::new(ActrNode {
        inner: Mutex::new(Some(registered)),
        network_event_handle: Mutex::new(None),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextBridge;
    use crate::workload::{ErrorEventBridge, RpcEnvelopeBridge, WorkloadLifecycleBridge};
    use actr_mock_actrix::MockActrixServer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    #[derive(Default)]
    struct TestLifecycleBridge {
        starts: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl WorkloadLifecycleBridge for TestLifecycleBridge {
        async fn on_start(&self, _ctx: Arc<ContextBridge>) -> ActrResult<()> {
            self.starts.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn on_ready(&self, _ctx: Arc<ContextBridge>) -> ActrResult<()> {
            Ok(())
        }

        async fn on_stop(&self, _ctx: Arc<ContextBridge>) -> ActrResult<()> {
            Ok(())
        }

        async fn on_error(
            &self,
            _ctx: Arc<ContextBridge>,
            _event: ErrorEventBridge,
        ) -> ActrResult<()> {
            Ok(())
        }

        async fn dispatch(
            &self,
            _ctx: Arc<ContextBridge>,
            _envelope: RpcEnvelopeBridge,
        ) -> ActrResult<Vec<u8>> {
            Ok(Vec::new())
        }
    }

    fn consumed_node_wrapper_for_test() -> Arc<ActrNode> {
        Arc::new(ActrNode {
            inner: Mutex::new(None),
            network_event_handle: Mutex::new(None),
        })
    }

    fn write_test_config(dir: &std::path::Path, server: &MockActrixServer) -> std::path::PathBuf {
        let config_path = dir.join("actr.toml");
        let data_dir = dir.display().to_string().replace('\\', "/");
        std::fs::write(
            &config_path,
            format!(
                "edition = 1\n\
                 [signaling]\n\
                 url = \"{}\"\n\
                 [ais_endpoint]\n\
                 url = \"{}/ais\"\n\
                 [deployment]\n\
                 realm_id = 1\n\
                 [hyper]\n\
                 data_dir = \"{}\"\n\
                 [hyper.trust]\n\
                 kind = \"dev_only\"\n",
                server.ws_url(),
                server.http_url(),
                data_dir,
            ),
        )
        .expect("write actr.toml");
        config_path
    }

    async fn linked_node_for_test(config_path: &std::path::Path) -> ActrResult<Arc<ActrNode>> {
        let workload = DynamicWorkload::new(
            Box::new(TestLifecycleBridge::default()),
            None,
            None,
            None,
            None,
            None,
        );

        ActrNode::new_from_linked_workload(
            config_path.display().to_string(),
            ActrType {
                manufacturer: "acme".to_string(),
                name: "RuntimeNetworkEventProbe".to_string(),
                version: "0.1.0".to_string(),
            },
            workload,
        )
        .await
    }

    #[tokio::test]
    async fn start_after_node_consumed_fails_fast() {
        let node = consumed_node_wrapper_for_test();

        let err = match node.start().await {
            Ok(_) => panic!("second start should fail"),
            Err(err) => err,
        };
        match err {
            ActrError::Internal { msg } => {
                assert!(
                    msg.contains("already started"),
                    "unexpected start error: {msg}"
                );
            }
            other => panic!("unexpected start error variant: {other:?}"),
        }
    }

    #[test]
    fn create_network_event_handle_after_node_consumed_fails_fast() {
        let node = consumed_node_wrapper_for_test();

        let err = match node.create_network_event_handle() {
            Ok(_) => panic!("old node should not create a new network event handle"),
            Err(err) => err,
        };
        match err {
            ActrError::Internal { msg } => {
                assert!(
                    msg.contains("no longer available"),
                    "unexpected handle error: {msg}"
                );
            }
            other => panic!("unexpected handle error variant: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_create_network_event_handle_and_start_is_bounded() {
        let mut server = MockActrixServer::start()
            .await
            .expect("mock actrix server should start");
        let temp = tempdir().expect("temp dir");
        let config_path = write_test_config(temp.path(), &server);
        let node = linked_node_for_test(&config_path)
            .await
            .expect("linked workload node should be created");

        let create_node = node.clone();
        let create_task =
            tokio::task::spawn_blocking(move || create_node.create_network_event_handle());
        let start_task = tokio::spawn(node.clone().start());

        let (create_result, start_result) =
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::join!(create_task, start_task)
            })
            .await
            .expect("concurrent create/start should not hang");

        let create_result = create_result.expect("create task should not panic");
        let actr_ref = start_result
            .expect("start task should not panic")
            .expect("start should succeed");

        match create_result {
            Ok(_) => {}
            Err(ActrError::Internal { msg }) => {
                assert!(
                    msg.contains("no longer available"),
                    "unexpected create/start race error: {msg}"
                );
            }
            Err(other) => panic!("unexpected create/start race error: {other:?}"),
        }

        actr_ref.shutdown();
        actr_ref.wait_for_shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_create_network_event_handle_reuses_cached_channel_until_start() {
        let mut server = MockActrixServer::start()
            .await
            .expect("mock actrix server should start");
        let temp = tempdir().expect("temp dir");
        let config_path = write_test_config(temp.path(), &server);
        let node = linked_node_for_test(&config_path)
            .await
            .expect("linked workload node should be created");

        let mut create_tasks = Vec::new();
        for _ in 0..8 {
            let node = node.clone();
            create_tasks.push(tokio::task::spawn_blocking(move || {
                node.create_network_event_handle()
            }));
        }

        let mut handles = Vec::new();
        for task in create_tasks {
            handles.push(
                task.await
                    .expect("create task should not panic")
                    .expect("repeated create should reuse cached handle"),
            );
        }

        let actr_ref = node
            .start()
            .await
            .expect("node should start after handle reuse");

        let mut event_tasks = Vec::new();
        for (idx, handle) in handles.into_iter().enumerate() {
            event_tasks.push(tokio::spawn(async move {
                handle
                    .handle_network_path_changed(NetworkSnapshot {
                        sequence: idx as u64 + 1,
                        availability: crate::types::NetworkAvailability::Available,
                        transport: crate::types::NetworkTransportFlags {
                            wifi: true,
                            cellular: false,
                            ethernet: false,
                            vpn: false,
                            other: false,
                        },
                        is_expensive: false,
                        is_constrained: false,
                    })
                    .await
            }));
        }

        for task in event_tasks {
            let result = task
                .await
                .expect("event task should not panic")
                .expect("cached handle event should complete after start");
            assert!(result.success, "event failed: {:?}", result.error);
        }

        actr_ref.shutdown();
        actr_ref.wait_for_shutdown().await;
        server.shutdown().await;
    }
}
