//! 服务管理器
//!
//! 实现了服务的启动、停止和管理逻辑
//! 服务管理器模块 - 负责管理多个服务的生命周期

use super::{HttpRouterService, IceService, http::build_control_router};
use crate::service::container::ServiceContainer;
use crate::service::grpc::build_signer_grpc_router;
use crate::service::http::{AisService, MfrService, SignalingService};
use crate::service::ice::{StunService, TurnService};
use anyhow::Result;
use axum::Router;
use axum::extract::connect_info::ConnectInfo;
use axum_server::tls_rustls::RustlsConfig;
use platform::{
    ServiceCollector, ServiceCounters, ServiceInfo, ServiceType, TlsConfigurer,
    config::ActrixConfig, config::config_store::ConfigOverrideStore, monitoring::MetricsStore,
};

/// ResourceType integer constants (from proto enum).
mod resource_type {
    pub const STUN: i32 = 1;
    pub const TURN: i32 = 2;
    pub const SIGNALING: i32 = 3;
    pub const AIS: i32 = 4;
    pub const SIGNER: i32 = 5;
    pub const MFR: i32 = 6;

    /// Map a ServiceType to its proto ResourceType integer.
    pub fn from_service_type(st: &super::ServiceType) -> i32 {
        match st {
            super::ServiceType::Stun => STUN,
            super::ServiceType::Turn => TURN,
            super::ServiceType::Signaling => SIGNALING,
            super::ServiceType::Ais => AIS,
            super::ServiceType::Ks => SIGNER,
            super::ServiceType::Mfr => MFR,
        }
    }
}
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::{Ready, ready};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tower_service::Service;
use url::Url;

/// Swappable make service for hot-reloading the Router.
///
/// Each new TCP connection clones the current Router from the watch channel.
/// SIGHUP triggers a new Router to be sent through the channel, so new
/// connections immediately use the updated routes while existing connections
/// continue with their previously obtained Router.
#[derive(Clone)]
struct SwappableMakeService {
    router_rx: watch::Receiver<Router>,
}

impl SwappableMakeService {
    fn new(router_rx: watch::Receiver<Router>) -> Self {
        Self { router_rx }
    }
}

impl Service<SocketAddr> for SwappableMakeService {
    type Response = Router;
    type Error = Infallible;
    type Future = Ready<Result<Router, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, target: SocketAddr) -> Self::Future {
        let router = self.router_rx.borrow().clone();
        // Inject ConnectInfo<SocketAddr> so handlers (e.g. signaling WS) can extract it.
        let router = router.layer(axum::Extension(ConnectInfo(target)));
        ready(Ok(router))
    }
}

/// 服务管理器，负责管理多个服务的生命周期
#[derive(Debug)]
pub struct ServiceManager {
    services: Vec<ServiceContainer>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    service_collector: ServiceCollector,
    config: ActrixConfig,
    config_path: PathBuf,
    /// Watch channel sender for hot-swapping the Router.
    router_tx: Option<watch::Sender<Router>>,
    /// TLS configuration handle for certificate hot-reload.
    tls_config: Option<RustlsConfig>,
    /// Independent shutdown channel for ICE services (allows restart without
    /// affecting the HTTP server).
    ice_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    /// Join handles for running ICE service tasks.
    ice_handles: Vec<JoinHandle<()>>,
    /// axum_server Handle for graceful HTTP shutdown.
    http_handle: Option<axum_server::Handle>,
    /// JWT secret for Admin UI, persisted across reloads.
    jwt_secret: Option<Vec<u8>>,
    /// SQLite-backed config override store, reused across reloads.
    config_store: Option<Arc<ConfigOverrideStore>>,
    /// Cancellation token for background tasks spawned during router build.
    /// Cancelled on reload to stop old tasks before spawning new ones.
    bg_cancel: Option<tokio_util::sync::CancellationToken>,
    /// In-memory metrics ring buffer, reused across reloads.
    metrics_store: MetricsStore,
    /// Per-service-type atomic counters, keyed by ResourceType i32.
    service_counters: Arc<HashMap<i32, Arc<ServiceCounters>>>,
}

impl ServiceManager {
    /// 创建新的服务管理器
    pub fn new(
        config: ActrixConfig,
        shutdown_tx: tokio::sync::broadcast::Sender<()>,
        config_path: PathBuf,
    ) -> Self {
        Self {
            services: Vec::new(),
            shutdown_tx,
            service_collector: ServiceCollector::new(),
            config,
            config_path,
            router_tx: None,
            tls_config: None,
            ice_shutdown_tx: None,
            ice_handles: Vec::new(),
            http_handle: None,
            jwt_secret: None,
            config_store: None,
            bg_cancel: None,
            metrics_store: MetricsStore::new(),
            service_counters: Arc::new(HashMap::new()),
        }
    }

    /// Build a counters map for all five service types.
    ///
    /// Called once during initial start. The same Arc'd counters are reused
    /// across hot-reloads so the sampler sees continuous data.
    fn build_service_counters() -> Arc<HashMap<i32, Arc<ServiceCounters>>> {
        let mut map = HashMap::new();
        map.insert(resource_type::STUN, Arc::new(ServiceCounters::new()));
        map.insert(resource_type::TURN, Arc::new(ServiceCounters::new()));
        map.insert(resource_type::SIGNALING, Arc::new(ServiceCounters::new()));
        map.insert(resource_type::AIS, Arc::new(ServiceCounters::new()));
        map.insert(resource_type::SIGNER, Arc::new(ServiceCounters::new()));
        Arc::new(map)
    }

    /// 添加服务到管理器
    pub fn add_service(&mut self, service: ServiceContainer) {
        platform::recording::info!("Adding service '{}' to manager", service.info().name);
        self.services.push(service);
    }

    /// 注册服务到管理平台
    pub async fn register_services(&self, services: Vec<ServiceInfo>) -> Result<()> {
        // 当前控制面为 pull 模式（/admin），不做主动注册。
        platform::recording::debug!(
            "Control plane is pull-based, skipping active service registration for {} services",
            services.len()
        );
        Ok(())
    }

    /// Rebuild ServiceCollector entries based on the current config.
    ///
    /// Removes entries for disabled services and ensures enabled services
    /// have entries. Called before building the router so the admin dashboard
    /// reflects the correct set of active services.
    async fn refresh_service_collector(&mut self) {
        let svc_types = [
            (
                "Signaling Service",
                ServiceType::Signaling,
                self.config.is_signaling_enabled(),
            ),
            (
                "AIS Service",
                ServiceType::Ais,
                self.config.is_ais_enabled(),
            ),
            (
                "Signer Service",
                ServiceType::Ks,
                self.config.is_signer_enabled(),
            ),
        ];

        for (name, svc_type, enabled) in &svc_types {
            if *enabled {
                // Ensure entry exists (may already be there from initial start)
                if self.service_collector.get(name).await.is_none() {
                    let mut info = ServiceInfo::new(*name, svc_type.clone(), None, &self.config);
                    let rt_id = resource_type::from_service_type(svc_type);
                    if let Some(ctr) = self.service_counters.get(&rt_id) {
                        info.set_counters(ctr.clone());
                    }
                    self.service_collector.insert(name.to_string(), info).await;
                }
            } else {
                // Remove disabled services
                self.service_collector.remove(name).await;
            }
        }
    }

    /// Build the combined Router from configuration.
    ///
    /// This is used both for initial startup and for reload. It creates fresh
    /// service instances, builds their routers, merges the control router,
    /// adds /metrics, and applies global middleware layers.
    async fn build_router_from_config(&mut self) -> Result<Router> {
        use crate::service::trace::http_trace_layer;
        use tower_http::cors::CorsLayer;

        // Cancel any previously spawned background tasks (cleanup loops, key refresh)
        if let Some(old_cancel) = self.bg_cancel.take() {
            old_cancel.cancel();
        }
        let cancel = tokio_util::sync::CancellationToken::new();
        self.bg_cancel = Some(cancel.clone());

        let mut app = Router::new();

        // Refresh ServiceCollector to reflect currently enabled services.
        // This ensures the admin dashboard shows accurate service status after reload.
        self.refresh_service_collector().await;

        // Initialize or reuse the ConfigOverrideStore across reloads.
        let config_store = if let Some(ref store) = self.config_store {
            store.clone()
        } else {
            let pool = platform::storage::db::get_database().get_pool().clone();
            let store = Arc::new(
                ConfigOverrideStore::new(pool)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create config override store: {e}"))?,
            );
            self.config_store = Some(store.clone());
            store
        };

        // Apply L2 overrides to the running config
        let overrides = config_store.list_all().await.unwrap_or_default();
        if !overrides.is_empty() {
            let toml_content = std::fs::read_to_string(&self.config_path).unwrap_or_default();
            match platform::config::resolver::apply_overrides(&toml_content, &overrides) {
                Ok(merged) => {
                    platform::recording::info!(
                        "Applied {} config override(s) from database",
                        overrides.len()
                    );
                    self.config = merged;
                }
                Err(e) => {
                    platform::recording::warn!(
                        "Failed to apply config overrides, using file-only config: {}",
                        e
                    );
                }
            }
        }

        // Control plane always present.
        // Pass existing JWT secret so admin sessions survive reloads.
        let (control_router, jwt_secret) = build_control_router(
            &self.config,
            self.service_collector.clone(),
            self.shutdown_tx.clone(),
            self.config_path.clone(),
            self.jwt_secret.clone(),
            config_store,
            self.metrics_store.clone(),
        )
        .await?;
        self.jwt_secret = Some(jwt_secret);
        platform::recording::info!(
            "Adding control routes: admin_ui={}, grpc_api={}",
            self.config.admin_ui_enabled(),
            self.config.grpc_api_enabled()
        );
        app = app.merge(control_router);

        // Business services based on enable bitmask
        if self.config.is_signaling_enabled() {
            let mut svc = SignalingService::new(self.config.clone(), cancel.clone());
            if let Some(ctr) = self.service_counters.get(&(resource_type::SIGNALING)) {
                svc.set_counters(ctr.clone());
            }
            match svc.build_router().await {
                Ok(router) => {
                    let prefix = svc.route_prefix();
                    platform::recording::info!(
                        "Adding route '{}' for service '{}'",
                        prefix,
                        svc.info().name
                    );
                    app = app.nest(prefix, router);
                }
                Err(e) => {
                    platform::recording::error!(
                        "Failed to build router for service '{}': {:?}",
                        svc.info().name,
                        e
                    );
                }
            }
        }

        if self.config.is_ais_enabled() {
            let mut svc = AisService::new(self.config.clone(), cancel.clone());
            if let Some(ctr) = self.service_counters.get(&(resource_type::AIS)) {
                svc.set_counters(ctr.clone());
            }
            match svc.build_router().await {
                Ok(router) => {
                    let prefix = svc.route_prefix();
                    platform::recording::info!(
                        "Adding route '{}' for service '{}'",
                        prefix,
                        svc.info().name
                    );
                    app = app.nest(prefix, router);
                }
                Err(e) => {
                    platform::recording::error!(
                        "Failed to build router for service '{}': {:?}",
                        svc.info().name,
                        e
                    );
                }
            }
        }

        if self.config.is_signer_enabled() {
            let ks_counters = self.service_counters.get(&(resource_type::SIGNER)).cloned();
            match build_signer_grpc_router(&self.config, ks_counters).await {
                Ok(router) => {
                    platform::recording::info!(
                        "Adding gRPC route '/signer.v1.Signer/<Method>' for service 'Signer Service'"
                    );
                    app = app.merge(router);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to build gRPC router for service 'Signer Service': {:?}",
                        e,
                    ));
                }
            }
        }

        // MFR service - always mounted (no config gate yet)
        {
            let mut svc = MfrService::new(self.config.clone());
            match svc.build_router().await {
                Ok(router) => {
                    let prefix = svc.route_prefix();
                    platform::recording::info!(
                        "Adding route '{}' for service '{}'",
                        prefix,
                        svc.info().name
                    );
                    app = app.nest(prefix, router);
                }
                Err(e) => {
                    platform::recording::error!(
                        "Failed to build router for service '{}': {:?}",
                        svc.info().name,
                        e
                    );
                }
            }
        }

        // Observability endpoints: /health, /metrics, /{service}/health, /{service}/metrics
        {
            use crate::service::http::observability::{
                ObservabilityState, build_observability_router,
            };

            let obs_state = Arc::new(ObservabilityState {
                collector: self.service_collector.clone(),
                config: self.config.clone(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            });
            let obs_router = build_observability_router(obs_state);
            platform::recording::info!(
                "Adding observability routes: /health, /metrics, /{{service}}/health, /{{service}}/metrics"
            );
            app = app.merge(obs_router);
        }

        // Global middleware layers
        app = app
            .layer(http_trace_layer()) // HTTP tracing (with OpenTelemetry context propagation)
            .layer(CorsLayer::permissive()); // CORS support

        Ok(app)
    }

    /// 启动所有服务
    pub async fn start_all(&mut self) -> Result<Vec<JoinHandle<()>>> {
        platform::recording::info!(
            "Starting all {} types ({}) services.",
            self.services.len(),
            self.services
                .iter()
                .map(|s| s.info().service_type.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Build service counters (once) and start the background sampler.
        self.service_counters = Self::build_service_counters();
        self.metrics_store
            .clone()
            .start_sampler(self.service_counters.clone(), Duration::from_secs(60));

        let services = std::mem::take(&mut self.services);
        let mut ice_services = Vec::new();

        // Separate HTTP router services (used for on_start callbacks only) and ICE services.
        // Wire service counters into ICE services.
        let mut http_service_containers = Vec::new();
        for mut service in services {
            if service.is_http_router() {
                http_service_containers.push(service);
            } else if service.is_ice() {
                // Attach counters to ICE services
                match &mut service {
                    ServiceContainer::Stun(s) => {
                        if let Some(ctr) = self.service_counters.get(&resource_type::STUN) {
                            s.counters = Some(ctr.clone());
                        }
                    }
                    ServiceContainer::Turn(s) => {
                        if let Some(ctr) = self.service_counters.get(&resource_type::TURN) {
                            s.counters = Some(ctr.clone());
                        }
                    }
                    _ => {}
                }
                ice_services.push(service);
            }
        }

        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();
        let mut handle_futs = Vec::new();

        // Start HTTP server (control plane /admin always on)
        let handle = self
            .start_http_services(http_service_containers, notify_clone)
            .await?;
        handle_futs.push(handle);
        notify.notified().await;
        let notify_clone = notify.clone();

        // Create ICE shutdown channel
        let (ice_shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(10);
        self.ice_shutdown_tx = Some(ice_shutdown_tx);

        // Start ICE services (handles tracked internally for restart)
        for service in ice_services {
            let handle = self
                .start_ice_service(service, notify_clone.clone())
                .await?;
            self.ice_handles.push(handle);
            notify.notified().await;
        }

        let services = self.service_collector.values().await;
        self.register_services(services).await?;

        Ok(handle_futs)
    }

    /// 启动HTTP服务器，合并所有HTTP路由服务
    async fn start_http_services(
        &mut self,
        mut service_containers: Vec<ServiceContainer>,
        notify: Arc<Notify>,
    ) -> Result<JoinHandle<()>> {
        let http_config = self
            .config
            .bind
            .http
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No bind.http configuration found"))?;

        let is_tls = http_config.is_tls();
        let protocol = if is_tls { "HTTPS" } else { "HTTP" };

        platform::recording::info!(
            "Starting {} server with {} route services (environment: {})",
            protocol,
            service_containers.len(),
            self.config.env
        );

        let bind_addr = parse_bind_socket_addr("bind.http.ip", &http_config.ip, http_config.port)?;
        let scheme = http_config.scheme();
        let public_url = Url::parse(&format!(
            "{}://{}:{}",
            scheme, http_config.domain_name, http_config.port
        ))
        .map_err(|e| anyhow::anyhow!("Failed to parse {} URL: {e}", scheme.to_uppercase()))?;

        let tls_config = if is_tls {
            let cert = http_config.cert.as_ref().unwrap();
            let key = http_config.key.as_ref().unwrap();
            TlsConfigurer::install_crypto_provider();
            Some(RustlsConfig::from_pem_file(cert, key).await?)
        } else {
            None
        };

        // Save TLS config handle for certificate hot-reload
        self.tls_config = tls_config.clone();

        // Build the initial router
        let initial_router = self.build_router_from_config().await?;

        // Fire on_start callbacks for HTTP services (for ServiceCollector registration)
        for service in &mut service_containers {
            let service_name = service.info().name.clone();
            if let Some(result) = service.on_start(public_url.clone()).await {
                self.service_collector
                    .insert(service_name.clone(), service.info().clone())
                    .await;
                if let Err(e) = result {
                    platform::recording::error!(
                        "Failed to start service '{}': {:?}",
                        service_name,
                        e
                    );
                }
            }
        }

        // Register KS gRPC service as running (it has no on_start callback)
        if self.config.is_signer_enabled()
            && let Some(mut info) = self.service_collector.get("Signer Service").await
        {
            info.set_running(public_url.clone());
            self.service_collector
                .insert("Signer Service".to_string(), info)
                .await;
        }

        // Create watch channel for router hot-swap
        let (router_tx, router_rx) = watch::channel(initial_router);
        self.router_tx = Some(router_tx);

        let make_svc = SwappableMakeService::new(router_rx);

        // Create axum_server Handle for graceful shutdown
        let handle = axum_server::Handle::new();
        self.http_handle = Some(handle.clone());

        // Spawn shutdown listener that triggers graceful shutdown on the Handle
        let handle_for_shutdown = handle.clone();
        let shutdown_tx = self.shutdown_tx.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            let _ = shutdown_rx.recv().await;
            platform::recording::info!("{} server received shutdown signal", protocol);
            handle_for_shutdown.graceful_shutdown(Some(Duration::from_secs(10)));
        });

        platform::recording::info!("{} server listening on {}", protocol, bind_addr);
        notify.notify_one();

        let shutdown_tx_for_error = shutdown_tx.clone();
        let fut = if let Some(tls_config) = tls_config {
            // Start HTTPS server with axum_server
            tokio::spawn(async move {
                if let Err(e) = axum_server::bind_rustls(bind_addr, tls_config)
                    .handle(handle)
                    .serve(make_svc)
                    .await
                {
                    platform::recording::error!("HTTPS server error: {}", e);
                    let _ = shutdown_tx_for_error.send(());
                }
                platform::recording::info!("HTTPS server stopped");
            })
        } else {
            // Eagerly bind the TCP listener so port conflicts are detected
            // before spawning the server task.
            let listener = tokio::net::TcpListener::bind(bind_addr)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to bind to address '{bind_addr}': {e}"))?;
            let std_listener = listener.into_std()?;

            tokio::spawn(async move {
                if let Err(e) = axum_server::from_tcp(std_listener)
                    .handle(handle)
                    .serve(make_svc)
                    .await
                {
                    platform::recording::error!("HTTP server error: {}", e);
                    let _ = shutdown_tx_for_error.send(());
                }
                platform::recording::info!("HTTP server stopped");
            })
        };

        Ok(fut)
    }

    /// 启动单个ICE服务
    async fn start_ice_service(
        &mut self,
        service: ServiceContainer,
        notify: Arc<Notify>,
    ) -> Result<JoinHandle<()>> {
        // ICE services use the independent ice_shutdown channel so they can be
        // restarted during reload without affecting the HTTP server.
        let shutdown_rx = self
            .ice_shutdown_tx
            .as_ref()
            .expect("ice_shutdown_tx must be initialized before starting ICE services")
            .subscribe();
        let shutdown_tx = self.shutdown_tx.clone();
        let service_name = service.info().name.clone();
        let bind_addr = self.config.bind.ice.ip.clone();
        let config = self.config.clone();

        match service {
            ServiceContainer::Stun(mut s) => {
                let (tx, rx) = tokio::sync::oneshot::channel::<ServiceInfo>();
                let handle = tokio::spawn(async move {
                    if let Err(e) = s.start(shutdown_rx, tx).await {
                        platform::recording::error!("Failed to start STUN service: {:?}", e);
                        let _ = shutdown_tx.send(());
                    }
                });
                let info = rx
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to receive STUN service info: {e}"))?;
                self.service_collector.insert(info.name.clone(), info).await;
                notify.notify_one();
                Ok(handle)
            }
            ServiceContainer::Turn(mut s) => {
                let (tx, rx) = tokio::sync::oneshot::channel::<ServiceInfo>();
                let handle = tokio::spawn(async move {
                    if let Err(e) = s.start(shutdown_rx, tx).await {
                        platform::recording::error!("Failed to start TURN service: {:?}", e);
                        let _ = shutdown_tx.send(());
                    }
                });
                let info = rx
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to receive TURN service info: {e}"))?;

                self.service_collector
                    .insert(info.name.clone(), info.clone())
                    .await;

                let mut stun_info =
                    ServiceInfo::new("STUN Server", ServiceType::Stun, None, &config);
                stun_info.set_running(
                    Url::parse(&format!("stun:{}:{}", bind_addr, info.port_info))
                        .map_err(|e| anyhow::anyhow!("Failed to parse STUN URL: {e}"))?,
                );
                if let Some(ctr) = self.service_counters.get(&(resource_type::STUN)) {
                    stun_info.set_counters(ctr.clone());
                }
                self.service_collector
                    .insert(stun_info.name.clone(), stun_info)
                    .await;
                notify.notify_one();
                Ok(handle)
            }
            _ => {
                platform::recording::error!(
                    "Invalid service type for ICE service: {}",
                    service_name
                );
                Err(anyhow::anyhow!(
                    "Invalid service type for ICE service: {service_name}"
                ))
            }
        }
    }

    /// Restart ICE services with updated configuration.
    async fn restart_ice_services(&mut self) -> Result<()> {
        // 1. Signal old ICE services to stop
        if let Some(ref old_tx) = self.ice_shutdown_tx {
            let _ = old_tx.send(());
        }

        // 2. Wait for old handles to finish (5s timeout)
        let old_handles = std::mem::take(&mut self.ice_handles);
        for handle in old_handles {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        // 3. Create new ICE shutdown channel
        let (ice_shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(10);
        self.ice_shutdown_tx = Some(ice_shutdown_tx);

        // 4. Start new ICE services based on current config
        let notify = Arc::new(Notify::new());

        if self.config.is_ice_enabled() {
            if self.config.is_turn_enabled() {
                platform::recording::info!("Reloading TURN Server (UDP, includes STUN)");
                let mut turn_service = TurnService::new(self.config.clone());
                if let Some(ctr) = self.service_counters.get(&(resource_type::TURN)) {
                    turn_service = turn_service.with_counters(ctr.clone());
                }
                let handle = self
                    .start_ice_service(ServiceContainer::turn(turn_service), notify.clone())
                    .await?;
                self.ice_handles.push(handle);
                notify.notified().await;
            } else if self.config.is_stun_enabled() {
                platform::recording::info!("Reloading STUN Server (UDP)");
                let mut stun_service = StunService::new(self.config.clone());
                if let Some(ctr) = self.service_counters.get(&(resource_type::STUN)) {
                    stun_service = stun_service.with_counters(ctr.clone());
                }
                let handle = self
                    .start_ice_service(ServiceContainer::stun(stun_service), notify.clone())
                    .await?;
                self.ice_handles.push(handle);
                notify.notified().await;
            }
        }

        Ok(())
    }

    /// Check whether config changes require a full process restart.
    ///
    /// Returns a list of human-readable reasons. An empty list means
    /// hot-reload is sufficient.
    fn check_restart_required(old: &ActrixConfig, new: &ActrixConfig) -> Vec<String> {
        let mut reasons = Vec::new();

        if old.bind.http != new.bind.http {
            reasons.push("bind.http changed".into());
        }
        if old.sqlite_path != new.sqlite_path {
            reasons.push("sqlite_path changed".into());
        }
        if old.recording != new.recording {
            reasons.push("recording config changed".into());
        }
        if old.env != new.env {
            reasons.push("env changed".into());
        }

        reasons
    }

    /// Reload configuration and hot-swap services.
    ///
    /// On success, new TCP connections will use the updated Router, TLS
    /// certificates are refreshed, and ICE services are restarted if their
    /// configuration changed.
    ///
    /// On failure at any stage, the error is logged and the previous
    /// configuration continues to operate.
    pub async fn reload(&mut self) -> Result<()> {
        platform::recording::info!("Reloading configuration from {:?}", self.config_path);

        // 1. Read & validate new config
        let new_config = match ActrixConfig::from_file(&self.config_path) {
            Ok(cfg) => cfg,
            Err(e) => {
                platform::recording::error!(
                    "Config reload failed: parse error: {}. Keeping old config.",
                    e
                );
                return Err(anyhow::anyhow!("Config parse error: {e}"));
            }
        };

        if let Err(errors) = new_config.validate() {
            let non_warnings: Vec<_> = errors
                .iter()
                .filter(|e| !e.starts_with("Warning:"))
                .collect();
            if !non_warnings.is_empty() {
                platform::recording::error!(
                    "Config reload failed: validation errors: {:?}. Keeping old config.",
                    non_warnings
                );
                return Err(anyhow::anyhow!(
                    "Config validation failed: {}",
                    non_warnings
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join("; ")
                ));
            }
            // Warnings only — proceed
            for w in errors.iter().filter(|e| e.starts_with("Warning:")) {
                platform::recording::warn!("Config reload warning: {}", w);
            }
        }

        let old_config = self.config.clone();
        self.config = new_config;

        // 2. Warn about config changes that require a full restart
        let restart_reasons = Self::check_restart_required(&old_config, &self.config);
        if !restart_reasons.is_empty() {
            for reason in &restart_reasons {
                platform::recording::warn!("Config change ignored (requires restart): {}", reason);
            }
            platform::recording::warn!("Run `systemctl restart actrix` to apply these changes.");
            // Revert immutable fields to old values so runtime state stays consistent
            self.config.bind.http = old_config.bind.http.clone();
            self.config.sqlite_path = old_config.sqlite_path.clone();
            self.config.recording = old_config.recording.clone();
            self.config.env = old_config.env.clone();
        }

        // 3. Build new Router
        match self.build_router_from_config().await {
            Ok(new_router) => {
                if let Some(ref tx) = self.router_tx {
                    let _ = tx.send(new_router);
                    platform::recording::info!(
                        "Router hot-swapped successfully. New connections will use updated routes."
                    );
                }
            }
            Err(e) => {
                platform::recording::error!(
                    "Failed to build new router: {}. Keeping old router.",
                    e
                );
                self.config = old_config;
                return Err(e);
            }
        }

        // 4. Reload TLS certificates if applicable
        if let Some(ref tls_config) = self.tls_config
            && let Some(ref http_config) = self.config.bind.http
            && let (Some(cert), Some(key)) = (&http_config.cert, &http_config.key)
        {
            match tls_config.reload_from_pem_file(cert, key).await {
                Ok(()) => {
                    platform::recording::info!("TLS certificates reloaded successfully");
                }
                Err(e) => {
                    platform::recording::error!(
                        "Failed to reload TLS certificates: {}. Old certificates remain active.",
                        e
                    );
                }
            }
        }

        // 5. Restart ICE services if their config changed
        let ice_changed = old_config.bind.ice != self.config.bind.ice
            || old_config.turn != self.config.turn
            || old_config.is_ice_enabled() != self.config.is_ice_enabled()
            || old_config.is_stun_enabled() != self.config.is_stun_enabled()
            || old_config.is_turn_enabled() != self.config.is_turn_enabled();

        if ice_changed {
            platform::recording::info!("ICE configuration changed, restarting ICE services...");
            if let Err(e) = self.restart_ice_services().await {
                platform::recording::error!(
                    "Failed to restart ICE services: {}. ICE in degraded state.",
                    e
                );
            }
        }

        platform::recording::info!("Configuration reload completed successfully");
        Ok(())
    }

    /// Stop all services
    pub async fn stop_all(&mut self) -> Result<()> {
        platform::recording::info!("Stopping all services");

        // Cancel background tasks (cleanup loops, key refresh)
        if let Some(cancel) = self.bg_cancel.take() {
            cancel.cancel();
        }

        // Stop ICE services via independent channel
        if let Some(ref ice_tx) = self.ice_shutdown_tx {
            let _ = ice_tx.send(());
        }

        // Broadcast global shutdown
        let _ = self.shutdown_tx.send(());

        // Wait for ICE handles
        let ice_handles = std::mem::take(&mut self.ice_handles);
        for handle in ice_handles {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        for service in &mut self.services {
            match service {
                ServiceContainer::Signaling(s) => s.on_stop().await.unwrap(),
                ServiceContainer::Ais(s) => s.on_stop().await.unwrap(),
                ServiceContainer::Mfr(s) => s.on_stop().await.unwrap(),
                ServiceContainer::Stun(s) => s.stop().await.unwrap(),
                ServiceContainer::Turn(s) => s.stop().await.unwrap(),
            }
        }

        platform::recording::info!("All services stopped");
        Ok(())
    }
}

fn parse_bind_socket_addr(field_name: &str, ip: &str, port: u16) -> Result<SocketAddr> {
    let parsed_ip = ip
        .parse::<IpAddr>()
        .map_err(|e| anyhow::anyhow!("Invalid {field_name} '{ip}': {e}"))?;
    Ok(SocketAddr::new(parsed_ip, port))
}
