//! Axum Router 集成
//!
//! 提供 SignalingServer 的 Axum Router 适配器

use crate::server::{SignalingServer, SignalingServerHandle};
use actr_protocol::{AIdCredential, ActrId, ActrIdExt};
use anyhow::{Context as _, Result};
use axum::{
    Router,
    extract::{
        ConnectInfo, Query, State,
        ws::{WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use base64::Engine as _;
use platform::aid::credential::validator::AIdCredentialValidator;
use platform::config::ActrixConfig;
use platform::monitoring::ServiceCounters;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{collections::HashMap, str::FromStr};

/// Signaling Server 状态（用于 Axum State）
#[derive(Clone)]
pub struct SignalingState {
    pub server: Arc<SignalingServer>,
    /// Service-level counters for metrics collection.
    pub counters: Option<Arc<ServiceCounters>>,
    /// When true, require actor_id/token query params and validate credentials
    /// before upgrading the WebSocket connection. Production routers set this
    /// to true; the bare `create_signaling_router()` helper leaves it false.
    pub require_url_auth: bool,
}

/// 创建 Signaling Axum Router
///
/// 返回一个可以挂载到主 HTTP 服务器的 Router
pub async fn create_signaling_router() -> Result<Router> {
    platform::recording::info!("Creating Signaling Axum router");

    let server = SignalingServer::new();
    let state = SignalingState {
        server: Arc::new(server),
        counters: None,
        require_url_auth: false,
    };

    let router = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(state);

    platform::recording::info!("Signaling Axum router created successfully");
    Ok(router)
}

/// 创建 Signaling Axum Router（带配置）
///
/// 初始化 AIdCredentialValidator，并返回可挂载的 Router
pub async fn create_signaling_router_with_config(
    config: &ActrixConfig,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<Router> {
    create_signaling_router_with_config_and_counters(config, cancel, None).await
}

/// Create a Signaling Axum Router with config and optional service counters.
pub async fn create_signaling_router_with_config_and_counters(
    config: &ActrixConfig,
    cancel: tokio_util::sync::CancellationToken,
    counters: Option<Arc<ServiceCounters>>,
) -> Result<Router> {
    platform::recording::info!("Creating Signaling Axum router with config");

    // 初始化 AIdCredentialValidator（Ed25519 模式，使用本地 key_cache DB）
    AIdCredentialValidator::init(&config.sqlite_path)
        .await
        .map_err(|e| {
            platform::recording::error!("Failed to initialize AIdCredentialValidator: {}", e);
            anyhow::anyhow!("AIdCredentialValidator initialization failed: {e}")
        })?;
    platform::recording::info!("✅ AIdCredentialValidator (Ed25519) initialized");

    // 创建 SignalingServer
    let mut server = SignalingServer::new();

    // 初始化 ServiceRegistry 持久化缓存（用于重启恢复）
    let cache_ttl_secs = crate::service_registry_storage::DEFAULT_SERVICE_TTL_SECS;

    if !config.sqlite_path.exists() {
        std::fs::create_dir_all(&config.sqlite_path).with_context(|| {
            format!(
                "Failed to create SQLite data directory: {}",
                config.sqlite_path.display()
            )
        })?;
    }
    let cache_db_file = config.sqlite_path.join("signaling_cache.db");

    match crate::service_registry_storage::ServiceRegistryStorage::new(
        &cache_db_file,
        Some(cache_ttl_secs),
    )
    .await
    {
        Ok(storage) => {
            let storage_arc = Arc::new(storage);
            platform::recording::info!(
                "✅ ServiceRegistry cache initialized at: {}",
                cache_db_file.display()
            );

            // 设置存储到 ServiceRegistry
            {
                let mut registry = server.service_registry.write().await;
                registry.set_storage(storage_arc.clone());

                // 从缓存恢复服务列表
                match registry.restore_from_storage().await {
                    Ok(count) => {
                        if count > 0 {
                            platform::recording::info!("✅ Restored {} services from cache", count);
                        }
                    }
                    Err(e) => {
                        platform::recording::warn!(
                            "⚠️  Failed to restore services from cache: {}",
                            e
                        );
                    }
                }
            }

            // 启动定期清理任务（每 5 分钟清理一次过期数据）
            let storage_for_cleanup = storage_arc.clone();
            let cancel_for_storage = cancel.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // 5 分钟
                loop {
                    tokio::select! {
                        _ = cancel_for_storage.cancelled() => break,
                        _ = interval.tick() => {}
                    }
                    // 清理过期的服务注册
                    match storage_for_cleanup.cleanup_expired().await {
                        Ok(deleted) => {
                            if deleted > 0 {
                                platform::recording::info!(
                                    "🧹 Cleaned up {} expired services from cache",
                                    deleted
                                );
                            }
                        }
                        Err(e) => {
                            platform::recording::error!(
                                "Failed to cleanup expired services: {:?}",
                                e
                            );
                        }
                    }

                }
                platform::recording::debug!("Signaling storage cleanup task cancelled");
            });
        }
        Err(e) => {
            platform::recording::warn!("⚠️  Failed to initialize ServiceRegistry cache: {:?}", e);
            platform::recording::warn!(
                "    Service discovery will work but won't survive restarts"
            );
        }
    }

    // Start the periodic cleanup task for the ServiceRegistry memory table (cleanup expired services, avoid stale connections)
    {
        let registry_for_cleanup = server.service_registry.clone();
        let cancel_for_registry = cancel.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                crate::service_registry::CLEANUP_INTERVAL_SECS,
            ));
            loop {
                tokio::select! {
                    _ = cancel_for_registry.cancelled() => break,
                    _ = interval.tick() => {}
                }
                let mut registry = registry_for_cleanup.write().await;
                registry.cleanup_expired_services();
            }
            platform::recording::debug!("Signaling registry cleanup task cancelled");
        });
    }

    // 初始化速率限制器（如果配置存在）
    if let Some(signaling_config) = &config.services.signaling {
        let rate_limit_config = &signaling_config.server.rate_limit;

        // 初始化连接速率限制器
        if rate_limit_config.connection.enabled {
            platform::recording::info!(
                "Initializing connection rate limiter: {}/min, burst: {}, max concurrent: {}/IP",
                rate_limit_config.connection.per_minute,
                rate_limit_config.connection.burst_size,
                rate_limit_config.connection.max_concurrent_per_ip
            );
            server.connection_rate_limiter = Some(Arc::new(
                crate::ratelimit::ConnectionRateLimiter::new(rate_limit_config.connection.clone()),
            ));
            platform::recording::info!("✅ Connection rate limiter initialized");
        } else {
            platform::recording::info!("⚠️  Connection rate limiting is disabled");
        }

        // 初始化消息速率限制器
        if rate_limit_config.message.enabled {
            platform::recording::info!(
                "Initializing message rate limiter: {}/sec, burst: {}",
                rate_limit_config.message.per_second,
                rate_limit_config.message.burst_size
            );
            server.message_rate_limiter = Some(Arc::new(
                crate::ratelimit::MessageRateLimiter::new(rate_limit_config.message.clone()),
            ));
            platform::recording::info!("✅ Message rate limiter initialized");
        } else {
            platform::recording::info!("⚠️  Message rate limiting is disabled");
        }
    }

    // 创建 Router
    let state = SignalingState {
        server: Arc::new(server),
        counters,
        require_url_auth: true,
    };

    let router = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(state);

    platform::recording::info!("Signaling Axum router created successfully");
    Ok(router)
}

/// WebSocket 升级处理器
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<SignalingState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let client_ip = addr.ip();

    // 检查连接速率限制
    if let Some(ref limiter) = state.server.connection_rate_limiter
        && let Err(e) = limiter.check_connection(client_ip).await
    {
        platform::recording::warn!("🚫 IP {} 连接速率限制触发: {}", client_ip, e);
        if let Some(ref ctr) = state.counters {
            ctr.record_request(false, 0.0).await;
        }
        return axum::http::StatusCode::TOO_MANY_REQUESTS.into_response();
    }

    // URL-based credential validation (production path)
    let url_identity = if state.require_url_auth {
        let (actor_id, credential) = match parse_url_identity(&params) {
            Ok(identity) => identity,
            Err(e) => {
                platform::recording::warn!(
                    "🚫 WebSocket 鉴权参数缺失/无效: ip={} reason={}",
                    client_ip,
                    e
                );
                return axum::http::StatusCode::UNAUTHORIZED.into_response();
            }
        };

        match AIdCredentialValidator::check(&credential, actor_id.realm.realm_id).await {
            Ok((claims, _in_tolerance)) => {
                let expected_actor_id = actor_id.to_string_repr();
                if claims.actor_id != expected_actor_id {
                    platform::recording::warn!(
                        "🚫 WebSocket 凭证与 actor_id 不匹配: ip={} claim_actor_id={} query_actor_id={}",
                        client_ip,
                        claims.actor_id,
                        expected_actor_id
                    );
                    return axum::http::StatusCode::UNAUTHORIZED.into_response();
                }
            }
            Err(e) => {
                platform::recording::warn!(
                    "🚫 WebSocket credential 验证失败: ip={} error={}",
                    client_ip,
                    e
                );
                return axum::http::StatusCode::UNAUTHORIZED.into_response();
            }
        }

        Some((actor_id, credential))
    } else {
        None
    };

    let webrtc_role = params.get("webrtc_role").cloned();
    if let Some(ref role) = webrtc_role {
        platform::recording::info!("🎭 WebRTC 角色: {}", role);
    }

    // Record successful WebSocket upgrade request
    if let Some(ref ctr) = state.counters {
        ctr.record_request(true, 0.0).await;
    }

    ws.on_upgrade(move |socket| {
        handle_websocket(socket, state, client_ip, url_identity, webrtc_role)
    })
}

/// WebSocket 连接处理
async fn handle_websocket(
    socket: WebSocket,
    state: SignalingState,
    client_ip: std::net::IpAddr,
    url_identity: Option<(ActrId, AIdCredential)>,
    webrtc_role: Option<String>,
) {
    platform::recording::info!("📡 新 WebSocket 连接: IP={}", client_ip);

    // Increment service-level active connections counter
    if let Some(ref ctr) = state.counters {
        ctr.inc_conns();
    }

    // 增加连接计数
    if let Some(ref limiter) = state.server.connection_rate_limiter {
        limiter.increment_connection(client_ip).await;
    }

    // 创建 SignalingServerHandle
    let server_handle = SignalingServerHandle {
        clients: state.server.clients.clone(),
        actor_id_index: state.server.actor_id_index.clone(),
        service_registry: state.server.service_registry.clone(),
        presence_manager: state.server.presence_manager.clone(),
        connection_rate_limiter: state.server.connection_rate_limiter.clone(),
        message_rate_limiter: state.server.message_rate_limiter.clone(),
    };

    // 调用 SignalingServer 的 WebSocket 处理函数
    if let Err(e) = crate::handle_websocket_connection(
        socket,
        server_handle,
        Some(client_ip),
        url_identity,
        webrtc_role,
    )
    .await
    {
        platform::recording::error!("WebSocket connection error: {}", e);
    }

    // Decrement service-level active connections counter
    if let Some(ref ctr) = state.counters {
        ctr.dec_conns();
    }

    // 减少连接计数
    if let Some(ref limiter) = state.server.connection_rate_limiter {
        limiter.decrement_connection(client_ip).await;
    }
}

fn parse_url_identity(
    params: &HashMap<String, String>,
) -> std::result::Result<(ActrId, AIdCredential), String> {
    let actor_str = params
        .get("actor_id")
        .ok_or_else(|| "missing actor_id".to_string())?;
    let actor_id =
        ActrIdExt::from_string_repr(actor_str).map_err(|e| format!("invalid actor_id: {e}"))?;

    let key_id = params
        .get("key_id")
        .ok_or_else(|| "missing key_id".to_string())
        .and_then(|s| u32::from_str(s).map_err(|e| format!("invalid key_id: {e}")))?;

    let claims_b64 = params
        .get("claims")
        .ok_or_else(|| "missing claims".to_string())?;
    let claims = base64::engine::general_purpose::STANDARD
        .decode(claims_b64)
        .map_err(|e| format!("invalid claims base64: {e}"))?;

    let signature_b64 = params
        .get("signature")
        .ok_or_else(|| "missing signature".to_string())?;
    let signature = base64::engine::general_purpose::STANDARD
        .decode(signature_b64)
        .map_err(|e| format!("invalid signature base64: {e}"))?;

    Ok((
        actor_id,
        AIdCredential {
            key_id,
            claims: claims.into(),
            signature: signature.into(),
        },
    ))
}
