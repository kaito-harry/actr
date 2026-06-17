use crate::error::Result as AdminResult;
use crate::metrics::collect_system_metrics;
use crate::realm::realm_to_proto;
use actrix_proto::NodeAdminService;
use actrix_proto::{
    ConfigOverrideEntry as ProtoConfigOverrideEntry, ConfigType, CreateRealmRequest,
    CreateRealmResponse, DeleteConfigOverrideRequest, DeleteConfigOverrideResponse,
    DeleteRealmRequest, DeleteRealmResponse, GetConfigRequest, GetConfigResponse,
    GetNodeInfoRequest, GetNodeInfoResponse, GetRealmRequest, GetRealmResponse,
    ListConfigOverridesRequest, ListConfigOverridesResponse, ListRealmsRequest, ListRealmsResponse,
    RealmInfo, ServiceStatus, SetConfigOverrideRequest, SetConfigOverrideResponse, ShutdownRequest,
    ShutdownResponse, SystemMetrics, UpdateConfigRequest, UpdateConfigResponse, UpdateRealmRequest,
    UpdateRealmResponse,
};
use chrono::Utc;
use platform::ServiceCollector;
use platform::config::ActrixConfig;
use platform::config::config_store::{ConfigOverride, ConfigOverrideStore};
use platform::config::registry;
use platform::config::resolver::{self, ResolvedField};
use platform::realm::{
    DEFAULT_REALM_SECRET_PREVIOUS_GRACE_SECS, hash_realm_secret, rotate_realm_secret,
};
use platform::realm::{Realm, RealmStatus};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

type MetricsFuture = Pin<Box<dyn Future<Output = AdminResult<SystemMetrics>> + Send>>;
type MetricsProvider = Arc<dyn Fn() -> MetricsFuture + Send + Sync>;
type ShutdownFuture = Pin<Box<dyn Future<Output = AdminResult<()>> + Send>>;
type ShutdownHandler =
    Arc<dyn Fn(bool, Option<i32>, Option<String>) -> ShutdownFuture + Send + Sync>;
type ReloadFuture = Pin<Box<dyn Future<Output = AdminResult<bool>> + Send>>;
type ReloadHandler = Arc<dyn Fn() -> ReloadFuture + Send + Sync>;
type GrpcResult<T> = std::result::Result<T, Status>;

#[derive(Hash, Eq, PartialEq, Clone)]
struct ConfigKey {
    config_type: i32,
    key: String,
}

/// AdminApiService — canonical business-logic layer for node administration.
///
/// Both the REST/BFF (admin_api.rs) and the gRPC NodeAdminService trait call
/// into the `*_direct()` methods defined here.  REST handlers should be thin
/// wrappers that parse the request and format the JSON response.
#[derive(Clone)]
pub struct AdminApiService {
    node_id: String,
    name: String,
    location_tag: String,
    version: String,
    config_store: Arc<RwLock<HashMap<ConfigKey, String>>>,
    /// SQLite-backed config override store (L2 dynamic overrides).
    /// Present when running in AdminUi head mode.
    override_store: Option<Arc<ConfigOverrideStore>>,
    metrics_provider: MetricsProvider,
    shutdown_handler: Option<ShutdownHandler>,
    reload_handler: Option<ReloadHandler>,
    service_collector: ServiceCollector,
    started_at: Instant,
    /// Runtime config snapshot (set in AdminUi head mode).
    running_config: Option<ActrixConfig>,
    /// Raw TOML content for L1 detection (resolver).
    toml_content: Option<String>,
    /// Path to config.toml on disk.
    config_path: Option<PathBuf>,
}

impl AdminApiService {
    /// Create a new control gRPC service instance.
    pub fn new(
        node_id: impl Into<String>,
        name: impl Into<String>,
        location_tag: impl Into<String>,
        version: impl Into<String>,
        service_collector: ServiceCollector,
    ) -> AdminResult<Self> {
        Ok(Self {
            node_id: node_id.into(),
            name: name.into(),
            location_tag: location_tag.into(),
            version: version.into(),
            config_store: Arc::new(RwLock::new(HashMap::new())),
            override_store: None,
            metrics_provider: Arc::new(|| Box::pin(async { collect_system_metrics().await })),
            shutdown_handler: None,
            reload_handler: None,
            service_collector,
            started_at: Instant::now(),
            running_config: None,
            toml_content: None,
            config_path: None,
        })
    }

    /// Override the metrics provider used by GetNodeInfo.
    pub fn with_metrics_provider<F, Fut>(mut self, provider: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = AdminResult<SystemMetrics>> + Send + 'static,
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
        Fut: Future<Output = AdminResult<()>> + Send + 'static,
    {
        self.shutdown_handler = Some(Arc::new(move |graceful, timeout, reason| {
            let fut = handler(graceful, timeout, reason);
            Box::pin(fut)
        }));
        self
    }

    /// Attach a SQLite-backed config override store for L2 dynamic overrides.
    pub fn with_override_store(mut self, store: Arc<ConfigOverrideStore>) -> Self {
        self.override_store = Some(store);
        self
    }

    /// Attach a reload handler invoked when reload is requested.
    pub fn with_reload_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = AdminResult<bool>> + Send + 'static,
    {
        self.reload_handler = Some(Arc::new(move || {
            let fut = handler();
            Box::pin(fut)
        }));
        self
    }

    /// Attach the runtime config snapshot (AdminUi head mode).
    pub fn with_running_config(mut self, config: ActrixConfig) -> Self {
        self.running_config = Some(config);
        self
    }

    /// Attach the raw TOML content for L1 detection.
    pub fn with_toml_content(mut self, content: String) -> Self {
        self.toml_content = Some(content);
        self
    }

    /// Attach the config file path.
    pub fn with_config_path(mut self, path: PathBuf) -> Self {
        self.config_path = Some(path);
        self
    }

    /// Get the override store reference.
    pub fn override_store(&self) -> Option<&Arc<ConfigOverrideStore>> {
        self.override_store.as_ref()
    }

    fn build_config_key(config_type: ConfigType, key: String) -> ConfigKey {
        ConfigKey {
            config_type: config_type as i32,
            key,
        }
    }

    async fn load_realm(&self, realm_id: u32) -> GrpcResult<Realm> {
        let realm = Realm::get(realm_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to load realm: {e}")))?;

        realm.ok_or_else(|| Status::not_found(format!("Realm not found: {realm_id}")))
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

    // ── Helper: get overrides list ──────────────────────────────────

    async fn overrides_list(&self) -> Vec<ConfigOverride> {
        if let Some(store) = &self.override_store {
            store.list_all().await.unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    fn toml_content_ref(&self) -> &str {
        self.toml_content.as_deref().unwrap_or("")
    }

    fn require_running_config(&self) -> GrpcResult<&ActrixConfig> {
        self.running_config
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Running config not available"))
    }

    // ── Direct methods (transport-agnostic) ─────────────────────────

    /// Resolve the effective node name (L2 override → L1 config → running).
    pub async fn resolve_node_name(&self) -> String {
        let overrides = self.overrides_list().await;
        let fields = resolver::resolve_for_service("platform", self.toml_content_ref(), &overrides);

        fields
            .iter()
            .find(|f| f.key == "name")
            .map(|f| f.effective_value.trim().to_string())
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| {
                self.running_config
                    .as_ref()
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| self.name.clone())
            })
    }

    /// Health check info.
    pub async fn health_info(&self) -> HealthInfo {
        let node = self.resolve_node_name().await;
        HealthInfo {
            status: "healthy".to_string(),
            node,
            version: self.version.clone(),
        }
    }

    /// List L2 dynamic config overrides.
    pub async fn list_overrides_direct(&self) -> GrpcResult<Vec<ConfigOverride>> {
        let store = self
            .override_store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Override store not available"))?;
        store
            .list_all()
            .await
            .map_err(|e| Status::internal(format!("Failed to list overrides: {e}")))
    }

    /// Set a dynamic config override.
    pub async fn set_override_direct(&self, key: &str, value: &str, by: &str) -> GrpcResult<()> {
        let store = self
            .override_store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Override store not available"))?;
        store
            .set(key, value, by)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))
    }

    /// Delete a dynamic config override.
    pub async fn delete_override_direct(&self, key: &str) -> GrpcResult<bool> {
        let store = self
            .override_store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Override store not available"))?;
        store
            .delete(key)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete override: {e}")))
    }

    /// Get platform detail with resolved config fields.
    pub async fn get_platform_detail_direct(&self) -> GrpcResult<PlatformDetail> {
        let cfg = self.require_running_config()?;

        let http_bind = cfg.bind.http.as_ref().map(|h| {
            serde_json::json!({
                "ip": h.ip, "port": h.port,
                "domain_name": h.domain_name, "advertised_ip": h.advertised_ip,
                "advertised_port": h.effective_advertised_port(),
                "cert": h.cert, "key": h.key,
                "is_tls": h.is_tls(),
            })
        });

        let config = serde_json::json!({
            "enable": cfg.enable,
            "name": cfg.name,
            "env": cfg.env,
            "location_tag": cfg.location_tag,
            "sqlite_path": cfg.sqlite_path.display().to_string(),
            "bind": {
                "http": http_bind,
                "ice": {
                    "ip": cfg.bind.ice.ip.to_string(),
                    "port": cfg.bind.ice.port,
                    "advertised_ip": cfg.bind.ice.advertised_ip.to_string(),
                    "advertised_port": cfg.bind.ice.advertised_port,
                },
            },
            "turn": {
                "relay_port_range": cfg.turn.relay_port_range,
            },
            "recording": {
                "service_name": cfg.recording.service_name,
            },
            "control": {
                "head": cfg.control.head,
                "admin_ui": {
                    "session_expiry_secs": cfg.control.admin_ui.session_expiry_secs,
                },
            },
        });

        let overrides = self.overrides_list().await;
        let config_fields =
            resolver::resolve_for_service("platform", self.toml_content_ref(), &overrides);

        Ok(PlatformDetail {
            config,
            config_fields,
        })
    }

    /// Get service detail with resolved config fields.
    pub async fn get_service_detail_direct(&self, name: &str) -> GrpcResult<ServiceDetail> {
        let type_id = service_name_to_type_id(name)
            .ok_or_else(|| Status::not_found(format!("Unknown service: {name}")))?;

        let cfg = self.require_running_config()?;

        let enabled = match name {
            "stun" => cfg.is_stun_enabled(),
            "turn" => cfg.is_turn_enabled(),
            "signaling" => cfg.is_signaling_enabled(),
            "ais" => cfg.is_ais_enabled(),
            "signer" => cfg.is_signer_enabled(),
            _ => false,
        };

        let statuses = self.service_statuses().await;
        let status = statuses.iter().find(|s| s.r#type == type_id).map(|s| {
            serde_json::json!({
                "name": s.name,
                "type": s.r#type,
                "is_healthy": s.is_healthy,
                "active_connections": s.active_connections,
                "total_requests": s.total_requests,
                "failed_requests": s.failed_requests,
                "average_latency_ms": s.average_latency_ms,
                "url": s.url,
                "port": s.port,
                "domain": s.domain,
            })
        });

        let config: Option<JsonValue> = match name {
            "stun" => Some(serde_json::json!({
                "bind_ip": cfg.bind.ice.ip,
                "bind_port": cfg.bind.ice.port,
                "advertised_ip": cfg.bind.ice.advertised_ip,
                "advertised_port": cfg.bind.ice.advertised_port,
            })),
            "turn" => Some(serde_json::json!({
                "bind_ip": cfg.bind.ice.ip,
                "bind_port": cfg.bind.ice.port,
                "advertised_ip": cfg.bind.ice.advertised_ip,
                "advertised_port": cfg.bind.ice.advertised_port,
                "relay_port_range": cfg.turn.relay_port_range,
                "realm": cfg.turn.realm,
            })),
            "signaling" => cfg.services.signaling.as_ref().map(|s| {
                serde_json::json!({
                    "ws_path": s.server.ws_path,
                    "rate_limit": {
                        "connection": {
                            "enabled": s.server.rate_limit.connection.enabled,
                            "per_minute": s.server.rate_limit.connection.per_minute,
                            "burst_size": s.server.rate_limit.connection.burst_size,
                            "max_concurrent_per_ip": s.server.rate_limit.connection.max_concurrent_per_ip,
                        },
                        "message": {
                            "enabled": s.server.rate_limit.message.enabled,
                            "per_second": s.server.rate_limit.message.per_second,
                            "burst_size": s.server.rate_limit.message.burst_size,
                        }
                    },
                    "dependencies": {
                        "signer": s.dependencies.signer.as_ref().map(|k| serde_json::json!({"endpoint": k.endpoint})),
                        "ais": s.dependencies.ais.as_ref().map(|a| serde_json::json!({"endpoint": a.endpoint})),
                    }
                })
            }),
            "ais" => cfg.services.ais.as_ref().map(|a| {
                serde_json::json!({
                    "token_ttl_secs": a.server.token_ttl_secs,
                    "signaling_heartbeat_interval_secs": a.server.signaling_heartbeat_interval_secs,
                    "dependencies": {
                        "signer": a.dependencies.signer.as_ref().map(|k| serde_json::json!({"endpoint": k.endpoint})),
                    }
                })
            }),
            "signer" => cfg.services.signer.as_ref().map(|k| {
                serde_json::json!({
                    "storage_backend": format!("{:?}", k.storage.backend),
                    "key_ttl_seconds": k.storage.key_ttl_seconds,
                    "tolerance_seconds": k.tolerance_seconds,
                })
            }),
            _ => None,
        };

        let config_fields = if !name.is_empty() {
            let overrides = self.overrides_list().await;
            Some(resolver::resolve_for_service(
                name,
                self.toml_content_ref(),
                &overrides,
            ))
        } else {
            None
        };

        Ok(ServiceDetail {
            enabled,
            status,
            config,
            config_fields,
        })
    }

    /// Get config registry (all field definitions).
    pub fn get_registry_direct(&self) -> &'static [registry::ConfigFieldDef] {
        registry::all_fields()
    }

    /// Read config file from disk.
    pub async fn get_config_file_direct(&self) -> GrpcResult<ConfigFileContent> {
        let path = self
            .config_path
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Config path not available"))?;
        let path_display = path.display().to_string();
        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| Status::internal(format!("Failed to read config file: {e}")))?;
        Ok(ConfigFileContent {
            content,
            path: path_display,
        })
    }

    /// Write config file to disk (with TOML validation).
    pub async fn save_config_file_direct(&self, content: &str) -> GrpcResult<()> {
        let path = self
            .config_path
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Config path not available"))?;

        // Parse as TOML to validate syntax
        let new_config = ActrixConfig::from_toml(content)
            .map_err(|e| Status::invalid_argument(format!("Invalid TOML: {e}")))?;

        // Validate config semantics
        if let Err(errors) = new_config.validate() {
            let non_warnings: Vec<_> = errors
                .iter()
                .filter(|e| !e.starts_with("Warning:"))
                .cloned()
                .collect();
            if !non_warnings.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "Validation failed: {}",
                    non_warnings.join("; ")
                )));
            }
        }

        tokio::fs::write(path, content.as_bytes())
            .await
            .map_err(|e| Status::internal(format!("Failed to write: {e}")))?;

        platform::recording::info!("Config file saved via admin API");
        Ok(())
    }

    /// Trigger reload (SIGHUP).
    pub async fn reload_direct(&self) -> GrpcResult<bool> {
        platform::recording::info!("Reload requested via admin API, sending SIGHUP to self");
        #[cfg(unix)]
        {
            let pid = std::process::id() as i32;
            // SAFETY: sending a signal to our own process is safe
            let ret = unsafe { nix::libc::kill(pid, nix::libc::SIGHUP) };
            if ret == 0 {
                Ok(true)
            } else {
                Err(Status::internal("Failed to send SIGHUP"))
            }
        }
        #[cfg(not(unix))]
        {
            Err(Status::unimplemented(
                "Reload not supported on this platform",
            ))
        }
    }

    /// Query Signer keys from the SQLite database.
    pub async fn get_signer_keys_direct(&self) -> GrpcResult<KsKeysResult> {
        let cfg = self.require_running_config()?;
        let db_path = cfg.sqlite_path.join("signer_keys.db");
        if !db_path.exists() {
            return Ok(KsKeysResult {
                keys: vec![],
                total_count: 0,
            });
        }

        let url = format!("sqlite:{}?mode=ro", db_path.display());
        let options = SqliteConnectOptions::from_str(&url)
            .map_err(|e| Status::internal(format!("DB connect error: {e}")))?
            .read_only(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| Status::internal(format!("DB pool error: {e}")))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let rows = sqlx::query_as::<_, (i64, i32, i64, i64)>(
            "SELECT key_id, length(public_key) as pk_size, created_at, expires_at FROM keys ORDER BY created_at DESC LIMIT 5",
        )
        .fetch_all(&pool)
        .await
        .map_err(|e| Status::internal(format!("Query error: {e}")))?;

        let total_count = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM keys")
            .fetch_one(&pool)
            .await
            .map(|r| r.0)
            .unwrap_or(0);

        let keys = rows
            .iter()
            .map(|(key_id, pk_size, created_at, expires_at)| {
                let is_expired = *expires_at > 0 && *expires_at < now;
                KeyInfo {
                    key_id: *key_id,
                    pk_size: *pk_size,
                    created_at: Some(*created_at),
                    fetched_at: None,
                    expires_at: *expires_at,
                    tolerance_seconds: None,
                    is_expired,
                }
            })
            .collect();

        Ok(KsKeysResult { keys, total_count })
    }

    /// Cleanup expired Signer keys.
    pub async fn cleanup_signer_keys_direct(&self) -> GrpcResult<KsCleanupResult> {
        let cfg = self.require_running_config()?;
        let db_path = cfg.sqlite_path.join("signer_keys.db");
        if !db_path.exists() {
            return Ok(KsCleanupResult {
                deleted: 0,
                remaining: 0,
                tolerance_seconds: 0,
            });
        }

        let url = format!("sqlite:{}", db_path.display());
        let options = SqliteConnectOptions::from_str(&url)
            .map_err(|e| Status::internal(format!("DB connect error: {e}")))?;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| Status::internal(format!("DB pool error: {e}")))?;

        let tolerance = cfg
            .services
            .signer
            .as_ref()
            .map(|k| k.tolerance_seconds)
            .unwrap_or(3600);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let cutoff = now - tolerance as i64;

        let result = sqlx::query("DELETE FROM keys WHERE expires_at > 0 AND expires_at < ?")
            .bind(cutoff)
            .execute(&pool)
            .await
            .map_err(|e| Status::internal(format!("Cleanup error: {e}")))?;

        let remaining = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM keys")
            .fetch_one(&pool)
            .await
            .map(|r| r.0)
            .unwrap_or(0);

        Ok(KsCleanupResult {
            deleted: result.rows_affected(),
            remaining,
            tolerance_seconds: tolerance,
        })
    }

    /// Query AIS keys from the SQLite database.
    pub async fn get_ais_keys_direct(&self) -> GrpcResult<Vec<KeyInfo>> {
        let cfg = self.require_running_config()?;
        let db_path = cfg.sqlite_path.join("ais_keys.db");
        if !db_path.exists() {
            return Ok(vec![]);
        }

        let url = format!("sqlite:{}?mode=ro", db_path.display());
        let options = SqliteConnectOptions::from_str(&url)
            .map_err(|e| Status::internal(format!("DB connect error: {e}")))?
            .read_only(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| Status::internal(format!("DB pool error: {e}")))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let row = sqlx::query_as::<_, (i64, i32, i64, i64, i64)>(
            "SELECT key_id, length(public_key) as pk_size, fetched_at, expires_at, tolerance_seconds FROM current_key WHERE id = 1",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|e| Status::internal(format!("Query error: {e}")))?;

        match row {
            Some((key_id, pk_size, fetched_at, expires_at, tolerance_seconds)) => {
                let is_expired = expires_at > 0 && expires_at < now;
                Ok(vec![KeyInfo {
                    key_id,
                    pk_size,
                    created_at: None,
                    fetched_at: Some(fetched_at),
                    expires_at,
                    tolerance_seconds: Some(tolerance_seconds),
                    is_expired,
                }])
            }
            None => Ok(vec![]),
        }
    }

    /// Get node info including metrics and service statuses.
    pub async fn node_info_direct(&self) -> GrpcResult<GetNodeInfoResponse> {
        platform::recording::debug!("GetNodeInfo request received");

        let uptime_secs = self.started_at.elapsed().as_secs() as i64;
        let metrics = self.collect_metrics().await?;
        let services = self.service_statuses().await;

        Ok(GetNodeInfoResponse {
            success: true,
            error_message: None,
            node_id: self.node_id.clone(),
            name: self.name.clone(),
            version: self.version.clone(),
            location_tag: self.location_tag.clone(),
            uptime_secs,
            current_metrics: Some(metrics),
            services,
        })
    }

    /// List all realms.
    pub async fn list_realms_direct(&self) -> GrpcResult<ListRealmsResponse> {
        platform::recording::debug!("ListRealms request received");

        let realms = Realm::get_all()
            .await
            .map_err(|e| Status::internal(format!("Failed to load realm list: {e}")))?;

        let realms_info: Vec<RealmInfo> = realms.iter().map(realm_to_proto).collect();
        let total_count = realms_info.len() as u32;

        Ok(ListRealmsResponse {
            success: true,
            error_message: None,
            realms: realms_info,
            next_page_token: None,
            total_count,
        })
    }

    async fn create_local_realm_internal(
        &self,
        req: CreateRealmRequest,
        expose_plain_secret: bool,
    ) -> GrpcResult<(CreateRealmResponse, Option<String>)> {
        platform::recording::info!("Create local realm request received: name={}", req.name);

        // Generate secret at creation time
        let plain_secret = platform::realm::secret::generate_realm_secret();
        let secret_hash = hash_realm_secret(&plain_secret);

        let mut realm = match Realm::create(req.name.clone(), secret_hash).await {
            Ok(r) => r,
            Err(err) => {
                return Ok((
                    CreateRealmResponse {
                        success: false,
                        error_message: Some(format!("Failed to create realm: {err}")),
                        realm: None,
                    },
                    None,
                ));
            }
        };

        // Apply optional fields
        realm.enabled = req.enabled;
        if req.expires_at > 0 {
            realm.expires_at = Some(req.expires_at);
        }
        if let Some(status) = req.status.as_deref() {
            match parse_realm_status(status) {
                Ok(status) => realm.status = status,
                Err(err) => {
                    return Ok((
                        CreateRealmResponse {
                            success: false,
                            error_message: Some(err),
                            realm: None,
                        },
                        None,
                    ));
                }
            }
        }
        if let Err(err) = realm.save().await {
            // Rollback
            let _ = Realm::delete(realm.id).await;
            return Ok((
                CreateRealmResponse {
                    success: false,
                    error_message: Some(format!("Failed to save realm settings: {err}")),
                    realm: None,
                },
                None,
            ));
        }

        platform::recording::info!("Realm created: id={}, name={}", realm.id, realm.name);

        let realm_info = realm_to_proto(&realm);

        Ok((
            CreateRealmResponse {
                success: true,
                error_message: None,
                realm: Some(realm_info),
            },
            if expose_plain_secret {
                Some(plain_secret)
            } else {
                None
            },
        ))
    }

    async fn create_managed_realm_internal(
        &self,
        req: CreateRealmRequest,
    ) -> GrpcResult<CreateRealmResponse> {
        let Some(realm_id) = req.realm_id else {
            return Ok(CreateRealmResponse {
                success: false,
                error_message: Some("realm_id is required for managed CreateRealm".to_string()),
                realm: None,
            });
        };
        let Some(secret_current) = req.secret_current_hash else {
            return Ok(CreateRealmResponse {
                success: false,
                error_message: Some(
                    "secret_current_hash is required for managed CreateRealm".to_string(),
                ),
                realm: None,
            });
        };

        let status = match req.status.as_deref() {
            Some(status) => match parse_realm_status(status) {
                Ok(status) => status,
                Err(err) => {
                    return Ok(CreateRealmResponse {
                        success: false,
                        error_message: Some(err),
                        realm: None,
                    });
                }
            },
            None => RealmStatus::Active,
        };
        let secret_previous =
            build_secret_previous(req.secret_previous_hash, req.secret_previous_valid_until);

        platform::recording::info!(
            "Create managed realm request received: id={}, name={}",
            realm_id,
            req.name
        );

        match Realm::upsert_managed(
            realm_id,
            req.name,
            status,
            req.enabled,
            (req.expires_at > 0).then_some(req.expires_at),
            secret_current,
            secret_previous,
        )
        .await
        {
            Ok(realm) => Ok(CreateRealmResponse {
                success: true,
                error_message: None,
                realm: Some(realm_to_proto(&realm)),
            }),
            Err(err) => Ok(CreateRealmResponse {
                success: false,
                error_message: Some(format!("Failed to upsert managed realm: {err}")),
                realm: None,
            }),
        }
    }

    /// Create a realm and return the plaintext realm secret once.
    pub async fn create_realm_with_secret_direct(
        &self,
        req: CreateRealmRequest,
    ) -> GrpcResult<(CreateRealmResponse, Option<String>)> {
        self.create_local_realm_internal(req, true).await
    }

    /// Create a realm (gRPC path, does not expose plaintext secret in response).
    pub async fn create_realm_direct(
        &self,
        req: CreateRealmRequest,
    ) -> GrpcResult<CreateRealmResponse> {
        self.create_managed_realm_internal(req).await
    }

    /// Get a single realm by ID.
    pub async fn get_realm_direct(&self, realm_id: u32) -> GrpcResult<GetRealmResponse> {
        platform::recording::debug!("GetRealm request received: realm_id={}", realm_id);

        match self.load_realm(realm_id).await {
            Ok(realm) => Ok(GetRealmResponse {
                success: true,
                error_message: None,
                realm: Some(realm_to_proto(&realm)),
            }),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(GetRealmResponse {
                success: false,
                error_message: Some(status.message().to_string()),
                realm: None,
            }),
            Err(e) => Err(e),
        }
    }

    /// Update an existing realm.
    pub async fn update_realm_direct(
        &self,
        req: UpdateRealmRequest,
    ) -> GrpcResult<UpdateRealmResponse> {
        let mut realm = match self.load_realm(req.realm_id).await {
            Ok(r) => r,
            Err(status) if status.code() == tonic::Code::NotFound => {
                return Ok(UpdateRealmResponse {
                    success: false,
                    error_message: Some(status.message().to_string()),
                    realm: None,
                });
            }
            Err(e) => return Err(e),
        };

        if let Some(name) = req.name {
            realm.name = name;
        }
        if let Some(enabled) = req.enabled {
            realm.enabled = enabled;
        }
        if let Some(status) = req.status.as_deref() {
            match parse_realm_status(status) {
                Ok(status) => realm.status = status,
                Err(err) => {
                    return Ok(UpdateRealmResponse {
                        success: false,
                        error_message: Some(err),
                        realm: None,
                    });
                }
            }
        }
        if let Some(expires_at) = req.expires_at {
            realm.expires_at = (expires_at > 0).then_some(expires_at);
        }
        if let Some(secret_current_hash) = req.secret_current_hash {
            if secret_current_hash.trim().is_empty() {
                return Ok(UpdateRealmResponse {
                    success: false,
                    error_message: Some("secret_current_hash must not be empty".to_string()),
                    realm: None,
                });
            }
            realm.secret_current = secret_current_hash;
        }
        if req.secret_previous_hash.is_some() || req.secret_previous_valid_until.is_some() {
            realm.secret_previous =
                build_secret_previous(req.secret_previous_hash, req.secret_previous_valid_until);
        }

        if let Err(err) = realm.save().await {
            return Ok(UpdateRealmResponse {
                success: false,
                error_message: Some(format!("Failed to update realm: {err}")),
                realm: None,
            });
        }

        Ok(UpdateRealmResponse {
            success: true,
            error_message: None,
            realm: Some(realm_to_proto(&realm)),
        })
    }

    /// Delete a realm by ID.
    pub async fn delete_realm_direct(&self, realm_id: u32) -> GrpcResult<DeleteRealmResponse> {
        match Realm::soft_delete(realm_id).await {
            Ok(true) => Ok(DeleteRealmResponse {
                success: true,
                error_message: None,
            }),
            Ok(false) => Ok(DeleteRealmResponse {
                success: false,
                error_message: Some("Realm not found".to_string()),
            }),
            Err(err) => Ok(DeleteRealmResponse {
                success: false,
                error_message: Some(format!("Failed to delete realm: {err}")),
            }),
        }
    }

    /// Hard-delete a realm for local Admin UI standalone mode.
    pub async fn delete_realm_hard_direct(&self, realm_id: u32) -> GrpcResult<DeleteRealmResponse> {
        match Realm::delete(realm_id).await {
            Ok(affected) if affected > 0 => Ok(DeleteRealmResponse {
                success: true,
                error_message: None,
            }),
            Ok(_) => Ok(DeleteRealmResponse {
                success: false,
                error_message: Some("Realm not found".to_string()),
            }),
            Err(err) => Ok(DeleteRealmResponse {
                success: false,
                error_message: Some(format!("Failed to delete realm: {err}")),
            }),
        }
    }

    /// Rotate realm secret and return plaintext once.
    pub async fn rotate_realm_secret_direct(
        &self,
        realm_id: u32,
    ) -> GrpcResult<RealmSecretRotationResult> {
        let rotated = rotate_realm_secret(realm_id, Some(DEFAULT_REALM_SECRET_PREVIOUS_GRACE_SECS))
            .await
            .map_err(|e| Status::internal(format!("Failed to rotate realm secret: {e}")))?;

        Ok(RealmSecretRotationResult {
            realm_id,
            realm_secret: rotated.new_secret,
            previous_valid_until: rotated.previous_valid_until,
            grace_seconds: DEFAULT_REALM_SECRET_PREVIOUS_GRACE_SECS,
        })
    }

    /// Get a config value.
    pub async fn get_config_direct(
        &self,
        config_type: ConfigType,
        config_key: String,
    ) -> GrpcResult<GetConfigResponse> {
        let key = Self::build_config_key(config_type, config_key);
        let store = self.config_store.read().await;

        if let Some(value) = store.get(&key) {
            Ok(GetConfigResponse {
                success: true,
                error_message: None,
                config_value: Some(value.clone()),
            })
        } else {
            Ok(GetConfigResponse {
                success: false,
                error_message: Some("Config not found".to_string()),
                config_value: None,
            })
        }
    }

    /// Update a config value.
    pub async fn update_config_direct(
        &self,
        config_type: ConfigType,
        config_key: String,
        config_value: String,
    ) -> GrpcResult<UpdateConfigResponse> {
        let key = Self::build_config_key(config_type, config_key);
        let mut store = self.config_store.write().await;
        let old_value = store.insert(key, config_value);

        Ok(UpdateConfigResponse {
            success: true,
            error_message: None,
            old_value,
        })
    }

    /// Request node shutdown.
    pub async fn shutdown_direct(
        &self,
        graceful: bool,
        timeout_secs: Option<i32>,
        reason: Option<String>,
    ) -> GrpcResult<ShutdownResponse> {
        if let Some(handler) = &self.shutdown_handler {
            if let Err(e) = handler(graceful, timeout_secs, reason.clone()).await {
                return Ok(ShutdownResponse {
                    accepted: false,
                    error_message: Some(format!("Shutdown handler failed: {e}")),
                    estimated_shutdown_time: None,
                });
            }
        } else {
            platform::recording::warn!("Shutdown requested but no handler registered");
        }

        let estimated = if graceful {
            timeout_secs.map(|v| Utc::now().timestamp() + v as i64)
        } else {
            Some(Utc::now().timestamp())
        };

        Ok(ShutdownResponse {
            accepted: true,
            error_message: None,
            estimated_shutdown_time: estimated,
        })
    }
}

// ── Response structs (transport-agnostic, Serialize) ─────────────

/// Health check response.
#[derive(Debug, Clone, Serialize)]
pub struct HealthInfo {
    pub status: String,
    pub node: String,
    pub version: String,
}

/// Platform detail with resolved config fields.
#[derive(Debug, Clone, Serialize)]
pub struct PlatformDetail {
    pub config: JsonValue,
    pub config_fields: Vec<ResolvedField>,
}

/// Service detail with resolved config fields.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceDetail {
    pub enabled: bool,
    pub status: Option<JsonValue>,
    pub config: Option<JsonValue>,
    pub config_fields: Option<Vec<ResolvedField>>,
}

/// Config file content with path.
#[derive(Debug, Clone, Serialize)]
pub struct ConfigFileContent {
    pub content: String,
    pub path: String,
}

/// Key info (shared by Signer and AIS).
#[derive(Debug, Clone, Serialize)]
pub struct KeyInfo {
    pub key_id: i64,
    pub pk_size: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetched_at: Option<i64>,
    pub expires_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tolerance_seconds: Option<i64>,
    pub is_expired: bool,
}

/// Realm secret 轮转结果（明文 secret 仅返回一次）。
#[derive(Debug, Clone, Serialize)]
pub struct RealmSecretRotationResult {
    pub realm_id: u32,
    pub realm_secret: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_valid_until: Option<u64>,
    pub grace_seconds: u64,
}

/// Signer keys query result.
#[derive(Debug, Clone, Serialize)]
pub struct KsKeysResult {
    pub keys: Vec<KeyInfo>,
    pub total_count: i64,
}

/// Signer cleanup result.
#[derive(Debug, Clone, Serialize)]
pub struct KsCleanupResult {
    pub deleted: u64,
    pub remaining: i64,
    pub tolerance_seconds: u64,
}

/// Map service name string to the ResourceType integer used in ServiceStatus.type
fn service_name_to_type_id(name: &str) -> Option<i32> {
    match name {
        "stun" => Some(1),
        "turn" => Some(2),
        "signaling" => Some(3),
        "ais" => Some(4),
        "signer" => Some(5),
        _ => None,
    }
}

fn parse_realm_status(value: &str) -> std::result::Result<RealmStatus, String> {
    RealmStatus::from_str(value).map_err(|_| {
        format!("Invalid realm status '{value}' (expected Active, Inactive, Suspended)")
    })
}

fn build_secret_previous(hash: Option<String>, valid_until: Option<u64>) -> Option<(String, u64)> {
    match (hash, valid_until) {
        (Some(hash), Some(valid_until)) if !hash.trim().is_empty() && valid_until > 0 => {
            Some((hash, valid_until))
        }
        _ => None,
    }
}

#[tonic::async_trait]
impl NodeAdminService for AdminApiService {
    async fn update_config(
        &self,
        request: Request<UpdateConfigRequest>,
    ) -> GrpcResult<Response<UpdateConfigResponse>> {
        let req = request.into_inner();
        self.update_config_direct(req.config_type(), req.config_key, req.config_value)
            .await
            .map(Response::new)
    }

    async fn get_config(
        &self,
        request: Request<GetConfigRequest>,
    ) -> GrpcResult<Response<GetConfigResponse>> {
        let req = request.into_inner();
        self.get_config_direct(req.config_type(), req.config_key)
            .await
            .map(Response::new)
    }

    async fn create_realm(
        &self,
        request: Request<CreateRealmRequest>,
    ) -> GrpcResult<Response<CreateRealmResponse>> {
        self.create_realm_direct(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_realm(
        &self,
        request: Request<GetRealmRequest>,
    ) -> GrpcResult<Response<GetRealmResponse>> {
        let req = request.into_inner();
        self.get_realm_direct(req.realm_id).await.map(Response::new)
    }

    async fn update_realm(
        &self,
        request: Request<UpdateRealmRequest>,
    ) -> GrpcResult<Response<UpdateRealmResponse>> {
        self.update_realm_direct(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn delete_realm(
        &self,
        request: Request<DeleteRealmRequest>,
    ) -> GrpcResult<Response<DeleteRealmResponse>> {
        let req = request.into_inner();
        self.delete_realm_direct(req.realm_id)
            .await
            .map(Response::new)
    }

    async fn list_realms(
        &self,
        request: Request<ListRealmsRequest>,
    ) -> GrpcResult<Response<ListRealmsResponse>> {
        let _req = request.into_inner();
        self.list_realms_direct().await.map(Response::new)
    }

    async fn get_node_info(
        &self,
        request: Request<GetNodeInfoRequest>,
    ) -> GrpcResult<Response<GetNodeInfoResponse>> {
        let _req = request.into_inner();
        self.node_info_direct().await.map(Response::new)
    }

    async fn shutdown(
        &self,
        request: Request<ShutdownRequest>,
    ) -> GrpcResult<Response<ShutdownResponse>> {
        let req = request.into_inner();
        self.shutdown_direct(req.graceful, req.timeout_secs, req.reason)
            .await
            .map(Response::new)
    }

    async fn list_config_overrides(
        &self,
        _request: Request<ListConfigOverridesRequest>,
    ) -> GrpcResult<Response<ListConfigOverridesResponse>> {
        match self.list_overrides_direct().await {
            Ok(overrides) => {
                let entries = overrides
                    .into_iter()
                    .map(|o| ProtoConfigOverrideEntry {
                        key_path: o.key_path,
                        value: o.value,
                        updated_at: o.updated_at,
                        updated_by: o.updated_by,
                    })
                    .collect();
                Ok(Response::new(ListConfigOverridesResponse {
                    success: true,
                    error_message: None,
                    overrides: entries,
                }))
            }
            Err(e) => Ok(Response::new(ListConfigOverridesResponse {
                success: false,
                error_message: Some(e.message().to_string()),
                overrides: vec![],
            })),
        }
    }

    async fn set_config_override(
        &self,
        request: Request<SetConfigOverrideRequest>,
    ) -> GrpcResult<Response<SetConfigOverrideResponse>> {
        let req = request.into_inner();
        let by = req.updated_by.as_deref().unwrap_or("admin");
        match self.set_override_direct(&req.key, &req.value, by).await {
            Ok(()) => Ok(Response::new(SetConfigOverrideResponse {
                success: true,
                error_message: None,
            })),
            Err(e) => Ok(Response::new(SetConfigOverrideResponse {
                success: false,
                error_message: Some(e.message().to_string()),
            })),
        }
    }

    async fn delete_config_override(
        &self,
        request: Request<DeleteConfigOverrideRequest>,
    ) -> GrpcResult<Response<DeleteConfigOverrideResponse>> {
        let req = request.into_inner();
        match self.delete_override_direct(&req.key).await {
            Ok(deleted) => Ok(Response::new(DeleteConfigOverrideResponse {
                success: true,
                error_message: None,
                deleted,
            })),
            Err(e) => Ok(Response::new(DeleteConfigOverrideResponse {
                success: false,
                error_message: Some(e.message().to_string()),
                deleted: false,
            })),
        }
    }
}
