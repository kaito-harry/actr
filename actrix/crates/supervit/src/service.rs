use crate::error::Result as SupervitResult;
use crate::metrics::collect_system_metrics;
use crate::realm::{RealmMetadata, load_realm_metadata, persist_realm_metadata, realm_to_proto};
use actrix_common::ServiceCollector;
use actrix_common::realm::{Realm, RealmConfig};
use actrix_proto::SupervisedService;
use actrix_proto::{
    ConfigType, CreateRealmRequest, CreateRealmResponse, DeleteRealmRequest, DeleteRealmResponse,
    GetConfigRequest, GetConfigResponse, GetNodeInfoRequest, GetNodeInfoResponse, GetRealmRequest,
    GetRealmResponse, ListRealmsRequest, ListRealmsResponse, RealmInfo, ResourceType,
    ServiceStatus, ShutdownRequest, ShutdownResponse, SystemMetrics, UpdateConfigRequest,
    UpdateConfigResponse, UpdateRealmRequest, UpdateRealmResponse,
};
use chrono::Utc;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::warn;

type MetricsFuture = Pin<Box<dyn Future<Output = SupervitResult<SystemMetrics>> + Send>>;
type MetricsProvider = Arc<dyn Fn() -> MetricsFuture + Send + Sync>;
type ShutdownFuture = Pin<Box<dyn Future<Output = SupervitResult<()>> + Send>>;
type ShutdownHandler =
    Arc<dyn Fn(bool, Option<i32>, Option<String>) -> ShutdownFuture + Send + Sync>;
type GrpcResult<T> = std::result::Result<T, Status>;

#[derive(Hash, Eq, PartialEq, Clone)]
struct ConfigKey {
    config_type: i32,
    key: String,
}

/// Supervisord gRPC service (SupervisedService) implementation.
///
/// Handles realm lifecycle, config delivery, and node control coming from Supervisor.
#[derive(Clone)]
pub struct Supervisord {
    node_id: String,
    name: String,
    location_tag: String,
    version: String,
    config_store: Arc<RwLock<HashMap<ConfigKey, String>>>,
    metrics_provider: MetricsProvider,
    shutdown_handler: Option<ShutdownHandler>,
    service_collector: ServiceCollector,
    started_at: Instant,
}

impl Supervisord {
    /// Create a new supervisord service instance.
    pub fn new(
        node_id: impl Into<String>,
        name: impl Into<String>,
        location_tag: impl Into<String>,
        version: impl Into<String>,
        service_collector: ServiceCollector,
    ) -> SupervitResult<Self> {
        Ok(Self {
            node_id: node_id.into(),
            name: name.into(),
            location_tag: location_tag.into(),
            version: version.into(),
            config_store: Arc::new(RwLock::new(HashMap::new())),
            metrics_provider: Arc::new(|| Box::pin(async { collect_system_metrics().await })),
            shutdown_handler: None,
            service_collector,
            started_at: Instant::now(),
        })
    }

    /// Override the metrics provider used by GetNodeInfo.
    pub fn with_metrics_provider<F, Fut>(mut self, provider: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = SupervitResult<SystemMetrics>> + Send + 'static,
    {
        self.metrics_provider = Arc::new(move || {
            let fut = provider();
            Box::pin(fut)
        });
        self
    }

    /// Attach a shutdown handler invoked when Shutdown is accepted.
    pub fn with_shutdown_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(bool, Option<i32>, Option<String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = SupervitResult<()>> + Send + 'static,
    {
        self.shutdown_handler = Some(Arc::new(move |graceful, timeout, reason| {
            let fut = handler(graceful, timeout, reason);
            Box::pin(fut)
        }));
        self
    }

    fn build_config_key(config_type: ConfigType, key: String) -> ConfigKey {
        ConfigKey {
            config_type: config_type as i32,
            key,
        }
    }

    async fn get_realm(&self, realm_id: u32) -> GrpcResult<(Realm, RealmMetadata)> {
        let realm = Realm::get_by_realm_id(realm_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to load realm: {e}")))?;

        let realm =
            realm.ok_or_else(|| Status::not_found(format!("Realm not found: {}", realm_id)))?;

        let rowid = realm.rowid.ok_or_else(|| {
            Status::internal(format!(
                "Realm missing rowid for realm_id {}",
                realm.realm_id
            ))
        })?;

        let metadata = load_realm_metadata(rowid)
            .await
            .map_err(|e| Status::internal(format!("Failed to load realm metadata: {e}")))?;

        Ok((realm, metadata))
    }

    async fn build_realm_info(&self, realm: Realm) -> GrpcResult<RealmInfo> {
        let rowid = realm.rowid.ok_or_else(|| {
            Status::internal(format!(
                "Realm missing rowid for realm_id {}",
                realm.realm_id
            ))
        })?;

        let metadata = load_realm_metadata(rowid)
            .await
            .map_err(|e| Status::internal(format!("Failed to load realm metadata: {e}")))?;

        Ok(realm_to_proto(&realm, &metadata))
    }

    async fn persist_metadata_for(
        &self,
        realm: &Realm,
        metadata: &RealmMetadata,
    ) -> GrpcResult<()> {
        let rowid = realm.rowid.ok_or_else(|| {
            Status::internal(format!(
                "Realm missing rowid for realm_id {}",
                realm.realm_id
            ))
        })?;

        persist_realm_metadata(rowid, metadata)
            .await
            .map_err(|e| Status::internal(format!("Failed to persist realm metadata: {e}")))
    }

    async fn delete_realm_configs(&self, realm: &Realm) -> GrpcResult<()> {
        if let Some(rowid) = realm.rowid {
            RealmConfig::delete_by_realm(rowid)
                .await
                .map_err(|e| Status::internal(format!("Failed to delete realm configs: {e}")))?;
        }
        Ok(())
    }

    async fn collect_metrics(&self) -> GrpcResult<SystemMetrics> {
        (self.metrics_provider)()
            .await
            .map_err(|e| Status::internal(format!("Failed to collect metrics: {e}")))
    }

    /// Collect current service statuses for this node.
    ///
    /// Returns all service statuses from the service registry.
    pub async fn service_statuses(&self) -> Vec<ServiceStatus> {
        self.service_collector.all_statuses().await
    }
}

#[tonic::async_trait]
impl SupervisedService for Supervisord {
    async fn update_config(
        &self,
        request: Request<UpdateConfigRequest>,
    ) -> GrpcResult<Response<UpdateConfigResponse>> {
        let req = request.into_inner();

        let key = Self::build_config_key(req.config_type(), req.config_key);
        let mut store = self.config_store.write().await;
        let old_value = store.insert(key, req.config_value.clone());

        let response = UpdateConfigResponse {
            success: true,
            error_message: None,
            old_value,
        };

        Ok(Response::new(response))
    }

    async fn get_config(
        &self,
        request: Request<GetConfigRequest>,
    ) -> GrpcResult<Response<GetConfigResponse>> {
        let req = request.into_inner();

        let key = Self::build_config_key(req.config_type(), req.config_key);
        let store = self.config_store.read().await;

        if let Some(value) = store.get(&key) {
            let response = GetConfigResponse {
                success: true,
                error_message: None,
                config_value: Some(value.clone()),
            };
            Ok(Response::new(response))
        } else {
            let response = GetConfigResponse {
                success: false,
                error_message: Some("Config not found".to_string()),
                config_value: None,
            };
            Ok(Response::new(response))
        }
    }

    async fn create_realm(
        &self,
        request: Request<CreateRealmRequest>,
    ) -> GrpcResult<Response<CreateRealmResponse>> {
        let req = request.into_inner();
        tracing::info!("CreateRealm request received: realm_id={}", req.realm_id);

        let use_servers: Vec<ResourceType> = req
            .use_servers
            .iter()
            .filter_map(|v| ResourceType::try_from(*v).ok())
            .collect();

        let mut realm =
            Realm::new(req.realm_id, req.name.clone()).with_expires_at(req.expires_at as i64);

        let save_result = realm.save().await;

        if let Err(err) = save_result {
            let resp = CreateRealmResponse {
                success: false,
                error_message: Some(format!("Failed to create realm: {}", err)),
                realm: None,
            };
            return Ok(Response::new(resp));
        }

        let metadata = RealmMetadata {
            enabled: req.enabled,
            use_servers,
            version: req.version,
        };

        if let Err(status) = self.persist_metadata_for(&realm, &metadata).await {
            let err_msg = status.message().to_string();
            warn!("Realm created but metadata persistence failed: {}", err_msg);

            if let Err(clean_err) = self.delete_realm_configs(&realm).await {
                warn!(
                    "Failed to clean realm configs after metadata error (realm_id={}): {}",
                    realm.realm_id, clean_err
                );
            }

            if let Err(delete_err) = Realm::delete_instance(realm.realm_id).await {
                warn!(
                    "Failed to roll back realm after metadata error (realm_id={}): {}",
                    realm.realm_id, delete_err
                );
            }

            let response = CreateRealmResponse {
                success: false,
                error_message: Some(format!("Failed to persist realm metadata: {}", err_msg)),
                realm: None,
            };
            return Ok(Response::new(response));
        }

        let realm_info = realm_to_proto(&realm, &metadata);

        let response = CreateRealmResponse {
            success: true,
            error_message: None,
            realm: Some(realm_info),
        };

        Ok(Response::new(response))
    }

    async fn get_realm(
        &self,
        request: Request<GetRealmRequest>,
    ) -> GrpcResult<Response<GetRealmResponse>> {
        let req = request.into_inner();
        tracing::debug!("GetRealm request received: realm_id={}", req.realm_id);

        match self.get_realm(req.realm_id).await {
            Ok((realm, metadata)) => {
                let response = GetRealmResponse {
                    success: true,
                    error_message: None,
                    realm: Some(realm_to_proto(&realm, &metadata)),
                };
                Ok(Response::new(response))
            }
            Err(status) if status.code() == tonic::Code::NotFound => {
                let response = GetRealmResponse {
                    success: false,
                    error_message: Some(status.message().to_string()),
                    realm: None,
                };
                Ok(Response::new(response))
            }
            Err(e) => Err(e),
        }
    }

    async fn update_realm(
        &self,
        request: Request<UpdateRealmRequest>,
    ) -> GrpcResult<Response<UpdateRealmResponse>> {
        let req = request.into_inner();

        let realm_loaded = self.get_realm(req.realm_id).await;
        let (mut realm, mut metadata) = match realm_loaded {
            Ok(data) => data,
            Err(status) if status.code() == tonic::Code::NotFound => {
                let response = UpdateRealmResponse {
                    success: false,
                    error_message: Some(status.message().to_string()),
                    realm: None,
                };
                return Ok(Response::new(response));
            }
            Err(e) => return Err(e),
        };

        let original_realm = realm.clone();
        let original_metadata = metadata.clone();

        if let Some(name) = req.name {
            realm.name = name;
        }
        if let Some(enabled) = req.enabled {
            metadata.enabled = enabled;
        }

        let save_result = realm.save().await;
        if let Err(err) = save_result {
            let response = UpdateRealmResponse {
                success: false,
                error_message: Some(format!("Failed to update realm: {}", err)),
                realm: None,
            };
            return Ok(Response::new(response));
        }

        if let Err(status) = self.persist_metadata_for(&realm, &metadata).await {
            let err_msg = status.message().to_string();
            warn!("Realm metadata update failed: {}", err_msg);

            let mut rollback_realm = original_realm;
            if let Err(rollback_err) = rollback_realm.save().await {
                warn!(
                    "Failed to roll back realm after metadata error (realm_id={}): {}",
                    rollback_realm.realm_id, rollback_err
                );
            }

            if let Err(rollback_meta_err) = self
                .persist_metadata_for(&rollback_realm, &original_metadata)
                .await
            {
                warn!(
                    "Failed to roll back realm metadata (realm_id={}): {}",
                    rollback_realm.realm_id, rollback_meta_err
                );
            }

            let response = UpdateRealmResponse {
                success: false,
                error_message: Some(format!("Failed to persist realm metadata: {}", err_msg)),
                realm: None,
            };
            return Ok(Response::new(response));
        }

        let response = UpdateRealmResponse {
            success: true,
            error_message: None,
            realm: Some(realm_to_proto(&realm, &metadata)),
        };

        Ok(Response::new(response))
    }

    async fn delete_realm(
        &self,
        request: Request<DeleteRealmRequest>,
    ) -> GrpcResult<Response<DeleteRealmResponse>> {
        let req = request.into_inner();

        let realm = Realm::get_by_realm_id(req.realm_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to load realm: {e}")))?;

        let Some(realm) = realm else {
            let response = DeleteRealmResponse {
                success: false,
                error_message: Some("Realm not found".to_string()),
            };
            return Ok(Response::new(response));
        };

        let delete_result = Realm::delete_instance(req.realm_id).await;

        match delete_result {
            Ok(affected) if affected > 0 => {
                self.delete_realm_configs(&realm).await?;
                let response = DeleteRealmResponse {
                    success: true,
                    error_message: None,
                };
                Ok(Response::new(response))
            }
            Ok(_) => {
                let response = DeleteRealmResponse {
                    success: false,
                    error_message: Some("Realm not found".to_string()),
                };
                Ok(Response::new(response))
            }
            Err(err) => {
                let response = DeleteRealmResponse {
                    success: false,
                    error_message: Some(format!("Failed to delete realm: {}", err)),
                };
                Ok(Response::new(response))
            }
        }
    }

    async fn list_realms(
        &self,
        request: Request<ListRealmsRequest>,
    ) -> GrpcResult<Response<ListRealmsResponse>> {
        let req = request.into_inner();
        tracing::debug!(
            "ListRealms request received: page_size={:?}, page_token={:?}",
            req.page_size,
            req.page_token
        );

        let realms = Realm::get_all()
            .await
            .map_err(|e| Status::internal(format!("Failed to load realm list: {}", e)))?;

        let mut realms_info = Vec::with_capacity(realms.len());
        for realm in realms {
            match self.build_realm_info(realm).await {
                Ok(info) => realms_info.push(info),
                Err(e) => warn!("Skip realm due to metadata error: {}", e),
            }
        }

        let total_count = realms_info.len() as u32;

        let response = ListRealmsResponse {
            success: true,
            error_message: None,
            realms: realms_info,
            next_page_token: None,
            total_count,
        };

        Ok(Response::new(response))
    }

    async fn get_node_info(
        &self,
        request: Request<GetNodeInfoRequest>,
    ) -> GrpcResult<Response<GetNodeInfoResponse>> {
        let _req = request.into_inner();
        tracing::debug!("GetNodeInfo request received");

        let uptime_secs = self.started_at.elapsed().as_secs() as i64;
        let metrics = self.collect_metrics().await?;
        let services = self.service_statuses().await;

        let response = GetNodeInfoResponse {
            success: true,
            error_message: None,
            node_id: self.node_id.clone(),
            name: self.name.clone(),
            version: self.version.clone(),
            location_tag: self.location_tag.clone(),
            uptime_secs,
            current_metrics: Some(metrics),
            services,
        };

        Ok(Response::new(response))
    }

    async fn shutdown(
        &self,
        request: Request<ShutdownRequest>,
    ) -> GrpcResult<Response<ShutdownResponse>> {
        let req = request.into_inner();

        if let Some(handler) = &self.shutdown_handler {
            if let Err(e) = handler(req.graceful, req.timeout_secs, req.reason.clone()).await {
                let response = ShutdownResponse {
                    accepted: false,
                    error_message: Some(format!("Shutdown handler failed: {}", e)),
                    estimated_shutdown_time: None,
                };
                return Ok(Response::new(response));
            }
        } else {
            warn!("Shutdown requested but no handler registered");
        }

        let estimated = if req.graceful {
            req.timeout_secs.map(|v| Utc::now().timestamp() + v as i64)
        } else {
            Some(Utc::now().timestamp())
        };

        let response = ShutdownResponse {
            accepted: true,
            error_message: None,
            estimated_shutdown_time: estimated,
        };

        Ok(Response::new(response))
    }
}
