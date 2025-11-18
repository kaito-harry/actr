//! Axum Router 集成
//!
//! 提供 SignalingServer 的 Axum Router 适配器

use crate::server::{SignalingServer, SignalingServerHandle};
use actrix_common::aid::credential::validator::AIdCredentialValidator;
use actrix_common::config::ActrixConfig;
use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{
        ConnectInfo, State,
        ws::{WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};

/// Signaling Server 状态（用于 Axum State）
#[derive(Clone)]
pub struct SignalingState {
    pub server: Arc<SignalingServer>,
}

/// 创建 Signaling Axum Router
///
/// 返回一个可以挂载到主 HTTP 服务器的 Router
pub async fn create_signaling_router() -> Result<Router> {
    info!("Creating Signaling Axum router");

    let server = SignalingServer::new();
    let state = SignalingState {
        server: Arc::new(server),
    };

    let router = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(state);

    info!("Signaling Axum router created successfully");
    Ok(router)
}

/// 创建 Signaling Axum Router（带配置）
///
/// 初始化 AIdCredentialValidator 和 AIS 客户端，并返回可挂载的 Router
pub async fn create_signaling_router_with_config(config: &ActrixConfig) -> Result<Router> {
    info!("Creating Signaling Axum router with config");

    // 初始化 AIdCredentialValidator
    if let Some(signaling_config) = &config.services.signaling {
        if let Some(ks_client_config) = signaling_config.get_ks_client_config(config) {
            info!("Initializing AIdCredentialValidator with KS config");
            AIdCredentialValidator::init(&ks_client_config, config.get_actrix_shared_key())
                .await
                .map_err(|e| {
                    error!("Failed to initialize AIdCredentialValidator: {}", e);
                    anyhow::anyhow!("AIdCredentialValidator initialization failed: {e}")
                })?;
            info!("✅ AIdCredentialValidator initialized successfully");
        } else {
            warn!("⚠️  No KS config found for Signaling service, credential validation will fail");
            warn!("    Please configure services.signaling.dependencies.ks in config.toml");
        }
    } else {
        warn!("⚠️  Signaling config not found, credential validation will fail");
    }

    // 创建 SignalingServer
    let mut server = SignalingServer::new();

    // 初始化 ServiceRegistry 持久化缓存（用于重启恢复）
    let cache_ttl_secs = 3600; // 1 小时 TTL
    let cache_db_path = PathBuf::from(&config.sqlite).join("signaling_cache.db");
    if let Some(parent) = cache_db_path.parent() {
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "Failed to create ServiceRegistry cache directory: {}",
                    parent.display()
                )
            })?;
        }
    }
    match crate::service_registry_storage::ServiceRegistryStorage::new(
        &cache_db_path,
        Some(cache_ttl_secs),
    )
    .await
    {
        Ok(storage) => {
            let storage_arc = Arc::new(storage);
            info!(
                "✅ ServiceRegistry cache initialized at: {}",
                cache_db_path.display()
            );

            // 设置存储到 ServiceRegistry
            {
                let mut registry = server.service_registry.write().await;
                registry.set_storage(storage_arc.clone());

                // 从缓存恢复服务列表
                match registry.restore_from_storage().await {
                    Ok(count) => {
                        if count > 0 {
                            info!("✅ Restored {} services from cache", count);
                        }
                    }
                    Err(e) => {
                        warn!("⚠️  Failed to restore services from cache: {}", e);
                    }
                }
            }

            // 启动定期清理任务（每 5 分钟清理一次过期数据）
            let storage_for_cleanup = storage_arc.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // 5 分钟
                loop {
                    interval.tick().await;
                    match storage_for_cleanup.cleanup_expired().await {
                        Ok(deleted) => {
                            if deleted > 0 {
                                info!("🧹 Cleaned up {} expired services from cache", deleted);
                            }
                        }
                        Err(e) => {
                            error!("Failed to cleanup expired services: {:?}", e);
                        }
                    }
                }
            });
        }
        Err(e) => {
            warn!("⚠️  Failed to initialize ServiceRegistry cache: {:?}", e);
            warn!("    Service discovery will work but won't survive restarts");
        }
    }

    // 初始化速率限制器（如果配置存在）
    if let Some(signaling_config) = &config.services.signaling {
        let rate_limit_config = &signaling_config.server.rate_limit;

        // 初始化连接速率限制器
        if rate_limit_config.connection.enabled {
            info!(
                "Initializing connection rate limiter: {}/min, burst: {}, max concurrent: {}/IP",
                rate_limit_config.connection.per_minute,
                rate_limit_config.connection.burst_size,
                rate_limit_config.connection.max_concurrent_per_ip
            );
            server.connection_rate_limiter = Some(Arc::new(
                crate::ratelimit::ConnectionRateLimiter::new(rate_limit_config.connection.clone()),
            ));
            info!("✅ Connection rate limiter initialized");
        } else {
            info!("⚠️  Connection rate limiting is disabled");
        }

        // 初始化消息速率限制器
        if rate_limit_config.message.enabled {
            info!(
                "Initializing message rate limiter: {}/sec, burst: {}",
                rate_limit_config.message.per_second, rate_limit_config.message.burst_size
            );
            server.message_rate_limiter = Some(Arc::new(
                crate::ratelimit::MessageRateLimiter::new(rate_limit_config.message.clone()),
            ));
            info!("✅ Message rate limiter initialized");
        } else {
            info!("⚠️  Message rate limiting is disabled");
        }
    }

    // 初始化 AIS 客户端（如果配置存在）
    if let Some(signaling_config) = &config.services.signaling {
        if let Some(ais_client_config) = signaling_config.get_ais_client_config(config) {
            info!(
                "Initializing AIS client with endpoint: {}",
                ais_client_config.endpoint
            );
            match crate::ais_client::AisClient::new(&crate::ais_client::AisClientConfig {
                endpoint: ais_client_config.endpoint.clone(),
                timeout_seconds: ais_client_config.timeout_seconds,
            }) {
                Ok(ais_client) => {
                    server.ais_client = Some(Arc::new(ais_client));
                    info!("✅ AIS client initialized successfully");
                }
                Err(e) => {
                    error!("Failed to initialize AIS client: {:?}", e);
                    warn!("⚠️  Credential refresh will not be available");
                }
            }
        } else {
            info!("ℹ️  No AIS config found, credential refresh will not be available");
        }
    }

    // 创建 Router
    let state = SignalingState {
        server: Arc::new(server),
    };

    let router = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(state);

    info!("Signaling Axum router created successfully");
    Ok(router)
}

/// WebSocket 升级处理器
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<SignalingState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let client_ip = addr.ip();

    // 检查连接速率限制
    if let Some(ref limiter) = state.server.connection_rate_limiter {
        if let Err(e) = limiter.check_connection(client_ip).await {
            warn!("🚫 IP {} 连接速率限制触发: {}", client_ip, e);
            return axum::http::StatusCode::TOO_MANY_REQUESTS.into_response();
        }
    }

    ws.on_upgrade(move |socket| handle_websocket(socket, state, client_ip))
}

/// WebSocket 连接处理
async fn handle_websocket(socket: WebSocket, state: SignalingState, client_ip: std::net::IpAddr) {
    info!("📡 新 WebSocket 连接: IP={}", client_ip);

    // 增加连接计数
    if let Some(ref limiter) = state.server.connection_rate_limiter {
        limiter.increment_connection(client_ip).await;
    }

    // 创建 SignalingServerHandle
    let server_handle = SignalingServerHandle {
        clients: state.server.clients.clone(),
        next_serial_number: state.server.next_serial_number.clone(),
        service_registry: state.server.service_registry.clone(),
        presence_manager: state.server.presence_manager.clone(),
        ais_client: state.server.ais_client.clone(),
        compatibility_cache: state.server.compatibility_cache.clone(),
        connection_rate_limiter: state.server.connection_rate_limiter.clone(),
        message_rate_limiter: state.server.message_rate_limiter.clone(),
    };

    // 调用 SignalingServer 的 WebSocket 处理函数
    if let Err(e) = crate::handle_websocket_connection(socket, server_handle, Some(client_ip)).await
    {
        error!("WebSocket connection error: {}", e);
    }

    // 减少连接计数
    if let Some(ref limiter) = state.server.connection_rate_limiter {
        limiter.decrement_connection(client_ip).await;
    }
}
