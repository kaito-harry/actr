//\! 服务管理器
//\!
//\! 实现了服务的启动、停止和管理逻辑
//! 服务管理器模块 - 负责管理多个服务的生命周期

use super::{HttpRouterService, IceService};
use crate::service::ServiceType;
use crate::service::container::ServiceContainer;
use crate::service::info::ServiceInfo;
// use crate::service::ResourceRegistrationPayload; // WebSocket 模式下使用，gRPC 模式已不需要
use actrix_common::{TlsConfigurer, config::ActrixConfig};
use anyhow::Result;
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
// use serde_json::json; // WebSocket 模式下使用，gRPC 模式已不需要
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use url::Url;

/// 服务管理器，负责管理多个服务的生命周期
#[derive(Debug)]
pub struct ServiceManager {
    services: Vec<ServiceContainer>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    collected_service_info: Arc<RwLock<HashMap<String, ServiceInfo>>>, // 收集的服务信息
    config: ActrixConfig,
}

impl ServiceManager {
    /// 创建新的服务管理器
    pub fn new(config: ActrixConfig, shutdown_tx: tokio::sync::broadcast::Sender<()>) -> Self {
        Self {
            services: Vec::new(),
            shutdown_tx,
            collected_service_info: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// 添加服务到管理器
    pub fn add_service(&mut self, service: ServiceContainer) {
        info!("Adding service '{}' to manager", service.info().name);
        self.services.push(service);
    }

    /// 注册服务到管理平台
    pub async fn register_services(&self, services: Vec<ServiceInfo>) -> Result<()> {
        // 检查是否配置了管理平台
        let managed_config = match &self.config.supervisor {
            Some(config) => config,
            None => {
                warn!(
                    "No management platform configured, skipping service registration for '{:?}'",
                    services
                );
                return Ok(());
            }
        };

        // gRPC 模式下，服务注册通过 SupervitClient 的 StreamStatus 自动完成
        info!(
            "Service registration via gRPC mode: {} services will be reported through SupervitClient to {}",
            services.len(),
            managed_config.server_addr
        );

        // TODO: 如果需要，可以在这里初始化 SupervitClient 并启动状态上报
        // 目前假设 SupervitClient 已在其他地方初始化

        Ok(())
    }

    /// 启动所有服务
    pub async fn start_all(&mut self) -> Result<Vec<JoinHandle<()>>> {
        info!(
            "Starting all {} types ({}) services.",
            self.services.len(),
            self.services
                .iter()
                .map(|s| s.info().service_type.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let services = std::mem::take(&mut self.services);
        let mut http_services = Vec::new();
        let mut ice_services = Vec::new();

        // 分离HTTP路由服务和ICE服务
        for service in services {
            if service.is_http_router() {
                http_services.push(service);
            } else if service.is_ice() {
                ice_services.push(service);
            }
        }
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();
        let mut handle_futs = Vec::new();
        // 启动HTTP服务器（合并所有HTTP路由服务）
        if !http_services.is_empty() {
            let handle = self
                .start_http_services(http_services, notify_clone)
                .await?;
            handle_futs.push(handle);
        }
        notify.notified().await;
        let notify_clone = notify.clone();

        // 启动ICE服务
        for service in ice_services {
            let handle = self
                .start_ice_service(service, notify_clone.clone())
                .await?;
            handle_futs.push(handle);
            notify.notified().await;
        }

        let services = self
            .collected_service_info
            .read()
            .map_err(|e| anyhow::anyhow!("Failed to read collected service info: {e}"))?
            .values()
            .cloned()
            .collect();
        // 注册HTTP、ICE服务到管理平台
        self.register_services(services).await?;

        Ok(handle_futs)
    }

    /// 启动HTTP服务器，合并所有HTTP路由服务
    async fn start_http_services(
        &mut self,
        mut services: Vec<ServiceContainer>,
        notify: Arc<Notify>,
    ) -> Result<JoinHandle<()>> {
        let is_dev = self.config.env.to_lowercase() == "dev";
        let protocol = if is_dev { "HTTP" } else { "HTTPS" };

        info!(
            "Starting {} server with {} route services (environment: {})",
            protocol,
            services.len(),
            self.config.env
        );

        // 确定绑定配置
        let (bind_addr, public_url, tls_config) = if is_dev {
            // 开发环境优先使用HTTP，如果没有则使用HTTPS
            if let Some(ref http_config) = self.config.bind.http {
                let bind_addr = format!("{}:{}", http_config.ip, http_config.port);
                let public_url = Url::parse(&format!(
                    "http://{}:{}",
                    http_config.domain_name, http_config.port
                ))
                .map_err(|e| anyhow::anyhow!("Failed to parse HTTP URL: {e}"))?;
                (bind_addr, public_url, None)
            } else if let Some(ref https_config) = self.config.bind.https {
                let bind_addr = format!("{}:{}", https_config.ip, https_config.port);
                let public_url = Url::parse(&format!(
                    "https://{}:{}",
                    https_config.domain_name, https_config.port
                ))
                .map_err(|e| anyhow::anyhow!("Failed to parse HTTPS URL: {e}"))?;

                // 初始化加密提供程序
                TlsConfigurer::install_crypto_provider();
                let tls_config =
                    Some(RustlsConfig::from_pem_file(&https_config.cert, &https_config.key).await?);
                (bind_addr, public_url, tls_config)
            } else {
                return Err(anyhow::anyhow!(
                    "No HTTP or HTTPS binding configuration found"
                ));
            }
        } else {
            // 生产环境必须使用HTTPS
            if let Some(ref https_config) = self.config.bind.https {
                let bind_addr = format!("{}:{}", https_config.ip, https_config.port);
                let public_url = Url::parse(&format!(
                    "https://{}:{}",
                    https_config.domain_name, https_config.port
                ))
                .map_err(|e| anyhow::anyhow!("Failed to parse HTTPS URL: {e}"))?;

                // 初始化加密提供程序
                TlsConfigurer::install_crypto_provider();
                let tls_config =
                    Some(RustlsConfig::from_pem_file(&https_config.cert, &https_config.key).await?);
                (bind_addr, public_url, tls_config)
            } else {
                return Err(anyhow::anyhow!(
                    "HTTPS binding configuration is required for production environment"
                ));
            }
        };

        let collected_service_info = self.collected_service_info.clone();

        // 构建合并的路由器
        let mut app = Router::new();
        let mut http_services_info = Vec::new();

        // 添加 HTTP 追踪层（支持 OpenTelemetry 上下文传播）
        use crate::service::trace::http_trace_layer;
        use tower_http::cors::CorsLayer;

        for service in &mut services {
            let route_prefix = match service.route_prefix() {
                Some(prefix) => prefix.to_string(),
                None => continue,
            };

            let service_name = service.info().name.clone();

            let router_result = match service.build_router().await {
                Some(result) => result,
                None => continue,
            };

            match router_result {
                Ok(router) => {
                    info!(
                        "Adding route '{}' for service '{}'",
                        route_prefix, service_name
                    );
                    app = app.nest(&route_prefix, router);

                    // 记录服务信息用于后续状态更新
                    http_services_info.push((service_name.clone(), route_prefix.clone()));

                    // 调用 on_start 回调
                    let start_result = match service.on_start(public_url.clone()).await {
                        Some(result) => {
                            // 更新服务信息到收集器
                            collected_service_info
                                .write()
                                .map_err(|e| anyhow::anyhow!("Failed to write service info: {e}"))?
                                .insert(service_name.clone(), service.info().clone());
                            result
                        }
                        None => Ok(()),
                    };

                    if let Err(e) = start_result {
                        error!("Failed to start service '{}': {:?}", service_name, e);
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to build router for service '{}': {:?}",
                        service_name, e
                    );
                }
            }
        }

        // 添加全局 Prometheus metrics 端点
        info!("Adding /metrics endpoint for Prometheus");
        app = app.route("/metrics", axum::routing::get(metrics_handler));

        // 添加全局中间件层
        app = app
            .layer(http_trace_layer()) // HTTP 追踪（包含 OpenTelemetry 上下文传播）
            .layer(CorsLayer::permissive()); // CORS 支持

        // 启动服务器
        let addr: std::net::SocketAddr = bind_addr
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid bind address '{bind_addr}': {e}"))?;

        info!("{} server listening on {}", protocol, addr);
        notify.notify_one();

        let shutdown_tx = self.shutdown_tx.clone();
        let fut = if let Some(tls_config) = tls_config {
            // 启动HTTPS服务器
            let server = axum_server::bind_rustls(addr, tls_config)
                .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>());
            tokio::spawn(async move {
                let mut shutdown_rx = shutdown_tx.subscribe();
                tokio::select! {
                    result = server => {
                        if let Err(e) = result {
                            error!("HTTPS server error: {}", e);
                            let _ = shutdown_tx.send(());
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("HTTPS server received shutdown signal");
                    }
                }
                info!("HTTPS server stopped");
            })
        } else {
            // 启动HTTP服务器
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to bind to address '{addr}': {e}"))?;

            tokio::spawn(async move {
                let mut shutdown_rx = shutdown_tx.subscribe();
                let server = axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.recv().await;
                    info!("HTTP server received shutdown signal");
                });
                if let Err(e) = server.await {
                    error!("HTTP server error: {}", e);
                    let _ = shutdown_tx.send(());
                }
                info!("HTTP server stopped");
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
        let shutdown_rx = self.shutdown_tx.subscribe();
        let shutdown_tx = self.shutdown_tx.clone();
        let service_name = service.info().name.clone();
        let collected_service_info = self.collected_service_info.clone();
        let bind_addr = self.config.bind.ice.domain_name.clone();
        let config = self.config.clone();

        match service {
            ServiceContainer::Stun(mut s) => {
                let (tx, rx) = tokio::sync::oneshot::channel::<ServiceInfo>();
                let handle = tokio::spawn(async move {
                    if let Err(e) = s.start(shutdown_rx, tx).await {
                        error!("Failed to start STUN service: {:?}", e);
                        let _ = shutdown_tx.send(());
                    }
                });
                let info = rx
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to receive STUN service info: {e}"))?;
                collected_service_info
                    .write()
                    .map_err(|e| anyhow::anyhow!("Failed to write STUN service info: {e}"))?
                    .insert(info.name.clone(), info);
                notify.notify_one();
                Ok(handle)
            }
            ServiceContainer::Turn(mut s) => {
                let (tx, rx) = tokio::sync::oneshot::channel::<ServiceInfo>();
                let handle = tokio::spawn(async move {
                    if let Err(e) = s.start(shutdown_rx, tx).await {
                        error!("Failed to start TURN service: {:?}", e);
                        let _ = shutdown_tx.send(());
                    }
                });
                let info = rx
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to receive TURN service info: {e}"))?;

                collected_service_info
                    .write()
                    .map_err(|e| anyhow::anyhow!("Failed to write TURN service info: {e}"))?
                    .insert(info.name.clone(), info.clone());
                // turn 服务需要注册两个服务，一个是turn，一个是stun

                let mut stun_info =
                    ServiceInfo::new("STUN Server", ServiceType::Stun, None, &config);

                stun_info.set_running(
                    Url::parse(&format!("stun:{}:{}", bind_addr, info.port_info))
                        .map_err(|e| anyhow::anyhow!("Failed to parse STUN URL: {e}"))?,
                );

                collected_service_info
                    .write()
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to write STUN info for TURN service: {e}")
                    })?
                    .insert(stun_info.name.clone(), stun_info);
                notify.notify_one();
                Ok(handle)
            }
            _ => {
                error!("Invalid service type for ICE service: {}", service_name);
                Err(anyhow::anyhow!(
                    "Invalid service type for ICE service: {}",
                    service_name
                ))
            }
        }
    }

    /// 停止所有服务
    pub async fn stop_all(&mut self) -> Result<()> {
        info!("Stopping all services");

        let _ = self.shutdown_tx.send(());
        for service in &mut self.services {
            match service {
                ServiceContainer::Supervit(s) => s.on_stop().await.unwrap(),
                ServiceContainer::Signaling(s) => s.on_stop().await.unwrap(),
                ServiceContainer::Ais(s) => s.on_stop().await.unwrap(),
                ServiceContainer::Stun(s) => s.stop().await.unwrap(),
                ServiceContainer::Turn(s) => s.stop().await.unwrap(),
                ServiceContainer::Ks(s) => s.on_stop().await.unwrap(),
            }
        }

        info!("All services stopped");
        Ok(())
    }
}

/// Prometheus metrics 端点处理器
async fn metrics_handler() -> String {
    actrix_common::metrics::export_metrics()
}
