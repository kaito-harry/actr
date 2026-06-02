//! Runtime wrappers for UniFFI export

use crate::error::{ActrError, ActrResult};
use crate::types::{ActrId, ActrType, NetworkEventResult, PayloadType};
use crate::workload::DynamicWorkload;
use actr_framework::{Bytes, Dest};
use actr_hyper::{ActrRef, NetworkEventHandle, Node, Registered, WorkloadPackage};
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::{debug, error, info};

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
        let init = Node::from_config_file(&config_path).await.map_err(|e| {
            error!("Failed to load runtime config: {}", e);
            ActrError::Config {
                msg: format!("Failed to load runtime config `{}`: {}", config_path, e),
            }
        })?;
        let init = init.with_actor_type(actor_type.clone());
        crate::logger::init_observability(init.runtime_config().observability.clone());

        info!(
            config_path = %config_path,
            actor_type = %actor_type.to_string_repr(),
            "Creating linked foreign workload runtime wrapper",
        );

        let attached = init.link(workload.as_ref().clone()).await.map_err(|e| {
            error!("Failed to link foreign workload: {}", e);
            ActrError::Internal {
                msg: format!("Failed to link foreign workload: {e}"),
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

    /// Create a network event handle for platform callbacks.
    ///
    /// This must be called before `start()`.
    pub fn create_network_event_handle(&self) -> ActrResult<Arc<NetworkEventHandleWrapper>> {
        let mut handle_guard = self.network_event_handle.lock();
        if let Some(handle) = handle_guard.as_ref() {
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
    /// Handle network available event.
    pub async fn handle_network_available(&self) -> ActrResult<NetworkEventResult> {
        let result = self
            .inner
            .handle_network_available()
            .await
            .map_err(|e| ActrError::Internal { msg: e })?;
        Ok(result.into())
    }

    /// Handle network lost event.
    pub async fn handle_network_lost(&self) -> ActrResult<NetworkEventResult> {
        let result = self
            .inner
            .handle_network_lost()
            .await
            .map_err(|e| ActrError::Internal { msg: e })?;
        Ok(result.into())
    }

    /// Handle network type changed event.
    pub async fn handle_network_type_changed(
        &self,
        is_wifi: bool,
        is_cellular: bool,
    ) -> ActrResult<NetworkEventResult> {
        let result = self
            .inner
            .handle_network_type_changed(is_wifi, is_cellular)
            .await
            .map_err(|e| ActrError::Internal { msg: e })?;
        Ok(result.into())
    }

    /// Cleanup all connections.
    pub async fn cleanup_connections(&self) -> ActrResult<NetworkEventResult> {
        let result = self
            .inner
            .cleanup_connections()
            .await
            .map_err(|e| ActrError::Internal { msg: e })?;
        Ok(result.into())
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
