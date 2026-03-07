//! Actor-RTC 信令服务器 - 基于 protobuf SignalingEnvelope
//!
//! 完全基于 protobuf 协议，使用 WebSocket Binary 消息传输
//!
//! # 功能概览
//!
//! ## 已实现的核心功能
//!
//! ### 基础信令流程
//! - ✅ Actor 注册 / 注销 (`RegisterRequest`, `UnregisterRequest`)
//! - ✅ 心跳机制 (`Ping` / `Pong`)
//! - ✅ WebRTC 信令中继 (`ActrRelay` - ICE / SDP)
//!
//! ### 扩展功能
//! - ✅ 服务发现 (`DiscoveryRequest` / `DiscoveryResponse`)
//! - ✅ 负载均衡路由 (`RouteCandidatesRequest` / `RouteCandidatesResponse`)
//!   - 多因素排序：功率储备、邮箱积压、兼容性评分、地理距离、客户端粘性
//!   - 集成 GlobalCompatibilityCache 实现实时兼容性计算
//!   - 精确匹配快速路径优化
//! - ✅ Presence 订阅 (`SubscribeActrUpRequest` / `ActrUpEvent`)
//! - ✅ Credential 刷新 (`CredentialUpdateRequest` - 通过 AIS 客户端)
//! - ✅ 负载指标存储 (`handle_ping()` - 存储到 ServiceRegistry 用于负载均衡)
//!
//! ## 待完成的功能（可选增强）
//!
//! 1. **Credential 验证** (可选安全增强)
//!    - `handle_actr_to_server()` - 验证 Actor 消息中的 credential
//!    - `handle_actr_relay()` - 验证中继消息的 credential
//!
//! 2. **ServiceSpec 和 ACL 持久化** (可选访问控制)
//!    - `handle_register_request()` - 持久化服务规格和访问控制规则
//!    - 用于细粒度的服务间访问控制

use actr_protocol::{
    AIdCredential, ActrId, ActrIdExt, ActrRelay, ActrToSignaling, ActrType, ActrUpEvent,
    ErrorResponse, PeerToSignaling, Ping, Pong, Realm, RegisterRequest, RegisterResponse,
    RoleAssignment, RoleNegotiation, SignalingEnvelope, SignalingToActr, actr_relay,
    actr_to_signaling, peer_to_signaling, register_response, signaling_envelope, signaling_to_actr,
};
use actrix_common::aid::credential::validator::AIdCredentialValidator;
use actrix_common::realm::Realm as RealmEntity;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, info_span, warn};
use uuid::Uuid;

// Axum WebSocket
use axum::extract::ws::{Message as WsMessage, WebSocket};

use crate::actr_type_utils::type_key;
use crate::load_balancer::LoadBalancer;
use crate::presence::PresenceManager;
use crate::service_registry::ServiceRegistry;
#[cfg(feature = "opentelemetry")]
use crate::trace::{extract_trace_context, inject_trace_context};
use tracing::Instrument;
#[cfg(feature = "opentelemetry")]
use tracing::instrument;

/// 信令服务器状态
#[derive(Debug)]
pub struct SignalingServer {
    /// 已连接的客户端
    pub clients: Arc<RwLock<HashMap<String, ClientConnection>>>,
    /// 通过 ActorId 查找 client_id 的索引
    pub actor_id_index: Arc<RwLock<HashMap<ActrId, String>>>,
    /// 服务注册表
    pub service_registry: Arc<RwLock<ServiceRegistry>>,
    /// Presence 订阅管理器
    pub presence_manager: Arc<RwLock<PresenceManager>>,
    /// AIS 客户端（用于 ActorId 分配和 Credential 签发）
    pub ais_client: Option<Arc<crate::ais_client::AisClient>>,
    /// 兼容性缓存（用于 BEST_COMPATIBILITY 排序）
    pub compatibility_cache: Arc<RwLock<crate::compatibility_cache::GlobalCompatibilityCache>>,
    /// 连接速率限制器
    pub connection_rate_limiter: Option<Arc<crate::ratelimit::ConnectionRateLimiter>>,
    /// 消息速率限制器
    pub message_rate_limiter: Option<Arc<crate::ratelimit::MessageRateLimiter>>,
}

/// 客户端连接信息
#[derive(Debug)]
pub struct ClientConnection {
    pub id: String,
    pub actor_id: Option<ActrId>,
    pub credential: Option<AIdCredential>,
    pub direct_sender: tokio::sync::mpsc::UnboundedSender<WsMessage>,
    pub client_ip: Option<std::net::IpAddr>,
    /// WebRTC 角色：\"answer\" 或 None (默认为 offer)
    pub webrtc_role: Option<String>,
}

/// 信令服务器句柄 - 用于在异步任务中操作服务器
#[derive(Debug, Clone)]
pub struct SignalingServerHandle {
    pub clients: Arc<RwLock<HashMap<String, ClientConnection>>>,
    pub actor_id_index: Arc<RwLock<HashMap<ActrId, String>>>,
    pub service_registry: Arc<RwLock<ServiceRegistry>>,
    pub presence_manager: Arc<RwLock<PresenceManager>>,
    pub ais_client: Option<Arc<crate::ais_client::AisClient>>,
    pub compatibility_cache: Arc<RwLock<crate::compatibility_cache::GlobalCompatibilityCache>>,
    pub connection_rate_limiter: Option<Arc<crate::ratelimit::ConnectionRateLimiter>>,
    pub message_rate_limiter: Option<Arc<crate::ratelimit::MessageRateLimiter>>,
}
impl SignalingServerHandle {
    /// 创建 SignalingEnvelope
    #[cfg_attr(
        feature = "opentelemetry",
        instrument(level = "debug", skip_all, fields(reply_for))
    )]
    fn create_envelope(
        &self,
        flow: signaling_envelope::Flow,
        reply_for: Option<&str>,
    ) -> SignalingEnvelope {
        #[allow(unused_mut)]
        let mut envelope = SignalingEnvelope {
            envelope_version: 1,
            envelope_id: Uuid::new_v4().to_string(),
            reply_for: reply_for.map(|id| id.to_string()),
            timestamp: prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            },
            traceparent: None,
            tracestate: None,
            flow: Some(flow),
        };
        debug!(
            "Created envelope: envelope_id={}, reply_for={reply_for:?}",
            envelope.envelope_id,
        );
        envelope
    }

    #[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all))]
    fn create_new_envelope(&self, flow: signaling_envelope::Flow) -> SignalingEnvelope {
        self.create_envelope(flow, None)
    }
}

impl Default for SignalingServer {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalingServer {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            actor_id_index: Arc::new(RwLock::new(HashMap::new())),
            service_registry: Arc::new(RwLock::new(ServiceRegistry::new())),
            presence_manager: Arc::new(RwLock::new(PresenceManager::new())),
            ais_client: None, // 在 axum_router 中初始化
            compatibility_cache: Arc::new(RwLock::new(
                crate::compatibility_cache::GlobalCompatibilityCache::new(),
            )),
            connection_rate_limiter: None, // 在 axum_router 中根据配置初始化
            message_rate_limiter: None,    // 在 axum_router 中根据配置初始化
        }
    }
}

/// 处理 WebSocket 连接
pub async fn handle_websocket_connection(
    websocket: WebSocket,
    server: SignalingServerHandle,
    client_ip: Option<std::net::IpAddr>,
    url_identity: Option<(ActrId, AIdCredential)>,
    webrtc_role: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client_id = Uuid::new_v4().to_string();
    info!(
        "🔗 新 WebSocket 客户端连接: {} (IP: {:?})",
        client_id, client_ip
    );

    // 分离读写流
    let (mut ws_sender, mut ws_receiver) = websocket.split();

    // 创建专用的发送通道用于点对点消息
    let (direct_tx, mut direct_rx) = tokio::sync::mpsc::unbounded_channel();

    // 注册客户端（包含专用发送器）
    {
        let mut clients_guard = server.clients.write().await;

        // 如果 URL 已带 actor_id，则移除已有相同 actor 的连接（避免 stale 映射）。
        let (actor_for_entry, cred_for_entry) =
            if let Some((actor_id, credential)) = url_identity.clone() {
                let mut to_remove = Vec::new();
                for (cid, conn) in clients_guard.iter() {
                    if conn.actor_id.as_ref() == Some(&actor_id) {
                        to_remove.push(cid.clone());
                    }
                }
                for cid in to_remove {
                    clients_guard.remove(&cid);
                    info!("🧹 Removed stale client {} for actor {:?}", cid, actor_id);
                }
                (Some(actor_id), Some(credential))
            } else {
                (None, None)
            };

        clients_guard.insert(
            client_id.clone(),
            ClientConnection {
                id: client_id.clone(),
                actor_id: actor_for_entry,
                credential: cred_for_entry,
                direct_sender: direct_tx,
                client_ip,
                webrtc_role: webrtc_role.clone(),
            },
        );
    }

    // 处理客户端消息的任务
    let server_for_receive = server.clone();
    let client_id_for_receive = client_id.clone();

    let receive_task = tokio::spawn(async move {
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(WsMessage::Binary(data)) => {
                    if let Err(e) =
                        handle_client_envelope(&data, &client_id_for_receive, &server_for_receive)
                            .await
                    {
                        error!("处理客户端信令错误: {}", e);
                        break;
                    }
                }
                Ok(WsMessage::Close(_)) => {
                    info!("客户端 {} 主动断开连接", client_id_for_receive);
                    break;
                }
                Err(e) => {
                    error!("WebSocket 错误: {}", e);
                    break;
                }
                _ => {
                    warn!("收到非 Binary 消息，忽略");
                }
            }
        }

        // 清理客户端
        cleanup_client(&client_id_for_receive, &server_for_receive).await;
    });

    // 处理发送消息的任务
    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                // 处理点对点消息
                msg = direct_rx.recv() => {
                    match msg {
                        Some(message) => {
                            if ws_sender.send(message).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    });

    // 等待任一任务完成
    tokio::select! {
        _ = receive_task => {},
        _ = send_task => {},
    }

    // 清理客户端连接
    cleanup_client(&client_id, &server).await;
    info!("🔌 客户端 {} 已断开连接", client_id);

    Ok(())
}

/// 处理客户端发送的 SignalingEnvelope
async fn handle_client_envelope(
    data: &[u8],
    client_id: &str,
    server: &SignalingServerHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    // 检查消息速率限制
    if let Some(ref limiter) = server.message_rate_limiter
        && let Err(e) = limiter.check_message(client_id).await
    {
        warn!("🚫 连接 {} 消息速率限制触发: {}", client_id, e);
        // 发送错误响应
        let error_response = ErrorResponse {
            code: 429,
            message: e,
        };
        let error_envelope =
            server.create_new_envelope(signaling_envelope::Flow::EnvelopeError(error_response));
        send_envelope_to_client(client_id, error_envelope, server).await?;
        return Ok(());
    }

    // 解码 protobuf 消息
    let envelope = SignalingEnvelope::decode(data)?;

    #[cfg(feature = "opentelemetry")]
    let remote_context = extract_trace_context(&envelope);

    let span = info_span!(
        "signaling.handle_envelope",
        envelope_id = %envelope.envelope_id,
        client_id = %client_id
    );
    #[cfg(feature = "opentelemetry")]
    {
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let _ = span.set_parent(remote_context.clone());
    }

    async move {
        debug!("📨 收到信令消息 envelope_id={}", envelope.envelope_id);

        // 根据流向处理消息
        match envelope.flow {
            Some(signaling_envelope::Flow::PeerToServer(peer_to_server)) => {
                handle_peer_to_server(peer_to_server, client_id, server, &envelope.envelope_id)
                    .await
            }
            Some(signaling_envelope::Flow::ActrToServer(actr_to_server)) => {
                handle_actr_to_server(actr_to_server, client_id, server, &envelope.envelope_id)
                    .await
            }
            Some(signaling_envelope::Flow::ActrRelay(ref relay)) => {
                #[cfg(feature = "opentelemetry")]
                {
                    handle_actr_relay(
                        relay.clone(),
                        client_id,
                        server,
                        &envelope.envelope_id,
                        remote_context,
                    )
                    .await
                }
                #[cfg(not(feature = "opentelemetry"))]
                {
                    handle_actr_relay(relay.clone(), client_id, server, &envelope.envelope_id).await
                }
            }
            Some(signaling_envelope::Flow::EnvelopeError(error)) => {
                error!(
                    "收到 envelope 错误: code={}, message={}",
                    error.code, error.message
                );
                Ok(())
            }
            _ => {
                warn!("未知的信令流向");
                Ok(())
            }
        }
    }
    .instrument(span)
    .await
}

/// 处理 PeerToSignaling 流程（注册前）
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_peer_to_server(
    peer_to_server: PeerToSignaling,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match peer_to_server.payload {
        Some(peer_to_signaling::Payload::RegisterRequest(register_request)) => {
            // 验证 RegisterRequest 中的 realm 是否存在、未过期、状态正常
            let realm_id = register_request.realm.realm_id;
            if let Err(e) = RealmEntity::validate_realm(realm_id).await {
                warn!("⚠️  RegisterRequest realm 验证失败: {}", e);
                // 使用 register-specific 错误响应
                send_register_error(
                    client_id,
                    403,
                    &format!("Realm validation failed: {e}"),
                    server,
                    request_envelope_id,
                )
                .await?;
                return Ok(());
            }

            handle_register_request(register_request, client_id, server, request_envelope_id)
                .await?;
        }
        None => {
            warn!("PeerToSignaling 消息缺少 payload");
        }
    }
    Ok(())
}

/// 处理注册请求
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_register_request(
    request: RegisterRequest,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "🎯 处理注册请求: type={}/{}, has_service_spec={}, has_acl={}",
        request.actr_type.manufacturer,
        request.actr_type.name,
        request.service_spec.is_some(),
        request.acl.is_some()
    );

    // 记录 ServiceSpec 和 ACL 信息
    if let Some(ref service_spec) = request.service_spec {
        info!(
            "  📦 ServiceSpec: fingerprint={}, packages={}, tags={:?}",
            service_spec.fingerprint,
            service_spec.protobufs.len(),
            service_spec.tags
        );
    }

    if let Some(ref acl) = request.acl {
        info!("  🔐 ACL 规则数量: {}", acl.rules.len());
    }

    // 检查是否已经注册过
    if let Some(client) = server.clients.read().await.get(client_id)
        && client.actor_id.is_some()
    {
        send_register_error(
            client_id,
            409,
            "Already registered",
            server,
            request_envelope_id,
        )
        .await?;
        return Ok(());
    }

    // 通过 AIS 分配 ActorId 和 Credential
    let ais_client = match &server.ais_client {
        Some(client) => client,
        None => {
            error!(
                "❌ AIS 未配置，无法处理注册请求 (realm={}, type={}/{})",
                request.realm.realm_id, request.actr_type.manufacturer, request.actr_type.name
            );
            send_register_error(
                client_id,
                500,
                "AIS not configured; registration is unavailable",
                server,
                request_envelope_id,
            )
            .await?;
            return Ok(());
        }
    };

    let register_ok = match ais_client
        .refresh_credential(request.realm.realm_id, request.actr_type.clone())
        .await
    {
        Ok(ais_response) => {
            // 解析 AIS 响应
            match ais_response.result {
                Some(register_response::Result::Success(register_ok)) => {
                    info!(
                        "✅ AIS 分配 ActorId: realm={}, serial={}",
                        register_ok.actr_id.realm.realm_id, register_ok.actr_id.serial_number
                    );
                    register_ok
                }
                Some(register_response::Result::Error(err)) => {
                    error!(
                        "❌ AIS 注册失败: code={}, message={}",
                        err.code, err.message
                    );
                    send_register_error(
                        client_id,
                        err.code,
                        &err.message,
                        server,
                        request_envelope_id,
                    )
                    .await?;
                    return Ok(());
                }
                None => {
                    error!("❌ AIS 返回空响应");
                    send_register_error(
                        client_id,
                        500,
                        "AIS returned empty response",
                        server,
                        request_envelope_id,
                    )
                    .await?;
                    return Ok(());
                }
            }
        }
        Err(e) => {
            error!("❌ 调用 AIS 失败: {}", e);
            send_register_error(
                client_id,
                500,
                &format!("Failed to call AIS: {e}"),
                server,
                request_envelope_id,
            )
            .await?;
            return Ok(());
        }
    };

    // 注册服务到 ServiceRegistry（存储 ServiceSpec 和 ACL）
    {
        let mut registry = server.service_registry.write().await;

        // 从 ServiceSpec 中提取服务名称，如果没有则使用 ActrType 作为服务名
        let service_name = request
            .service_spec
            .as_ref()
            .map(|spec| spec.name.clone())
            .unwrap_or_else(|| {
                format!(
                    "{}/{}",
                    register_ok.actr_id.r#type.manufacturer, register_ok.actr_id.r#type.name
                )
            });

        // 从 ServiceSpec 中提取 message_types（proto packages）
        let message_types = request
            .service_spec
            .as_ref()
            .map(|spec| {
                spec.protobufs
                    .iter()
                    .map(|proto| proto.package.clone())
                    .collect()
            })
            .unwrap_or_default();

        if let Err(e) = registry.register_service_full(
            register_ok.actr_id.clone(),
            service_name,
            message_types,
            None, // capabilities 当前不使用
            request.service_spec.clone(),
            request.acl.clone(),
            request.ws_address.clone(),
        ) {
            warn!("⚠️  注册服务到 ServiceRegistry 失败: {}", e);
        } else {
            info!(
                "✅ 服务已注册到 ServiceRegistry (serial={})",
                register_ok.actr_id.serial_number
            );
        }
        drop(registry);
    }

    // 持久化 ACL 规则到数据库
    if let Some(ref acl) = request.acl {
        use actrix_common::realm::acl::ActorAcl;

        let realm_id = register_ok.actr_id.realm.realm_id;
        let my_type = type_key(&register_ok.actr_id.r#type);

        for rule in &acl.rules {
            // actr_protocol::Acl 是反向设计：principals 可以访问"我"
            // 需要转换为数据库的正向设计：from_type -> to_type
            let permission = rule.permission == actr_protocol::acl_rule::Permission::Allow as i32;

            for principal in &rule.principals {
                // 提取 principal 的类型（如果没有则跳过）
                let from_type = match &principal.actr_type {
                    Some(actr_type) => type_key(actr_type),
                    None => {
                        warn!("⚠️  ACL principal 缺少 actr_type，跳过");
                        continue;
                    }
                };

                // 保存规则：from_type (principal) -> to_type (me)
                let mut actor_acl =
                    ActorAcl::new(realm_id, from_type.clone(), my_type.clone(), permission);

                match actor_acl.save().await {
                    Ok(acl_id) => {
                        info!(
                            "✅ ACL 规则已保存: {} -> {} : {} (id={})",
                            from_type,
                            my_type,
                            if permission { "ALLOW" } else { "DENY" },
                            acl_id
                        );
                    }
                    Err(e) => {
                        warn!(
                            "⚠️  保存 ACL 规则失败 ({} -> {}): {}",
                            from_type, my_type, e
                        );
                    }
                }
            }
        }
    }

    // 更新客户端信息和 ActorId 索引
    // Hold clients lock until actor_id_index update completes to prevent race condition
    // where cleanup_client removes the client between releasing clients lock and
    // acquiring actor_id_index lock, leading to stale index entries.
    {
        let mut clients_guard = server.clients.write().await;
        if let Some(client) = clients_guard.get_mut(client_id) {
            client.actor_id = Some(register_ok.actr_id.clone());
            client.credential = Some(register_ok.credential.clone());
        }
    }
    {
        let mut actor_index = server.actor_id_index.write().await;
        actor_index.insert(register_ok.actr_id.clone(), client_id.to_string());
    }

    // 直接使用 AIS 返回的 register_ok（包含 psk 和 public_key）
    let response = RegisterResponse {
        result: Some(register_response::Result::Success(register_ok.clone())),
    };

    // 构造 SignalingToActr 流程
    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: register_ok.actr_id.clone(),
        payload: Some(signaling_to_actr::Payload::RegisterResponse(response)),
    });

    // 创建响应 envelope
    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));

    send_envelope_to_client(client_id, response_envelope, server).await?;

    // 通知所有订阅了该 ActrType 的订阅者（带 ACL 过滤）
    let presence = server.presence_manager.read().await;
    let subscribers = presence
        .get_subscribers_with_acl(&register_ok.actr_id)
        .await;

    if !subscribers.is_empty() {
        info!(
            "📢 Actor {}/{} 上线，通知 {} 个 ACL 授权的订阅者",
            register_ok.actr_id.r#type.manufacturer,
            register_ok.actr_id.r#type.name,
            subscribers.len()
        );

        // 构造 ActrUpEvent
        let actr_up_event = ActrUpEvent {
            actor_id: register_ok.actr_id.clone(),
        };

        // 为每个订阅者构造并发送通知
        for subscriber_id in subscribers {
            let subscriber_client_id =
                match resolve_client_id_by_actor_id(&subscriber_id, server).await {
                    Ok(id) => id,
                    Err(e) => {
                        warn!(
                            "⚠️  订阅者 {} 索引缺失或不一致: {}",
                            subscriber_id.serial_number, e
                        );
                        continue;
                    }
                };

            let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
                target: subscriber_id,
                payload: Some(signaling_to_actr::Payload::ActrUpEvent(
                    actr_up_event.clone(),
                )),
            });

            let event_envelope = server.create_new_envelope(flow);

            if let Err(e) =
                send_envelope_to_client(&subscriber_client_id, event_envelope, server).await
            {
                warn!("⚠️  发送 ActrUpEvent 到订阅者失败: {}", e);
            }
        }
    }
    drop(presence);

    Ok(())
}

/// 发送注册错误响应
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn send_register_error(
    client_id: &str,
    code: u32,
    message: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let error_response = ErrorResponse {
        code,
        message: message.to_string(),
    };

    let response = RegisterResponse {
        result: Some(register_response::Result::Error(error_response)),
    };

    // 创建临时 ActrId（用于响应）
    let temp_actor_id = ActrId {
        realm: Realm { realm_id: 0 },
        serial_number: 0,
        r#type: ActrType {
            manufacturer: "temp".to_string(),
            name: "temp".to_string(),
            version: None,
        },
    };

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: temp_actor_id,
        payload: Some(signaling_to_actr::Payload::RegisterResponse(response)),
    });

    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));

    send_envelope_to_client(client_id, response_envelope, server).await?;

    Ok(())
}

/// 处理 ActrToSignaling 流程（注册后）
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_actr_to_server(
    actr_to_server: ActrToSignaling,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = actr_to_server.source.clone();

    info!("📬 处理来自 Actor {} 的消息", source.serial_number);

    // 验证 Realm 是否存在、未过期、状态正常
    let realm_id = source.realm.realm_id;
    if let Err(e) = RealmEntity::validate_realm(realm_id).await {
        warn!("⚠️  Actor {} realm 验证失败: {}", source.serial_number, e);
        send_error_response(
            client_id,
            &source,
            403,
            &format!("Realm validation failed: {e}"),
            server,
            Some(request_envelope_id),
        )
        .await?;
        return Ok(());
    }

    // 验证 credential 并获取容忍期状态
    let in_tolerance_period = match AIdCredentialValidator::check(
        &actr_to_server.credential,
        source.realm.realm_id,
    )
    .await
    {
        Ok((_claims, in_tolerance)) => in_tolerance,
        Err(e) => {
            warn!(
                "⚠️  Actor {} credential 验证失败: {}",
                source.serial_number, e
            );
            // 发送错误响应
            send_error_response(
                client_id,
                &source,
                401,
                &format!("Credential validation failed: {e}"),
                server,
                Some(request_envelope_id),
            )
            .await?;
            return Ok(());
        }
    };

    match actr_to_server.payload {
        Some(actr_to_signaling::Payload::Ping(ping)) => {
            handle_ping(
                source,
                ping,
                client_id,
                server,
                request_envelope_id,
                in_tolerance_period,
            )
            .await?;
        }
        Some(actr_to_signaling::Payload::UnregisterRequest(req)) => {
            handle_unregister(source, req, client_id, server, request_envelope_id).await?;
        }
        Some(actr_to_signaling::Payload::CredentialUpdateRequest(req)) => {
            if source != req.actr_id {
                tracing::error!(
                    "CredentialUpdateRequest actr_id mismatch: source={} actr_id={}",
                    source.to_string_repr(),
                    req.actr_id.to_string_repr()
                );
                return Ok(());
            }
            handle_credential_update(source, client_id, server, request_envelope_id).await?;
        }
        Some(actr_to_signaling::Payload::DiscoveryRequest(req)) => {
            handle_discovery_request(source, req, client_id, server, request_envelope_id).await?;
        }
        Some(actr_to_signaling::Payload::RouteCandidatesRequest(req)) => {
            handle_route_candidates_request(source, req, client_id, server, request_envelope_id)
                .await?;
        }
        Some(actr_to_signaling::Payload::GetServiceSpecRequest(req)) => {
            handle_get_service_spec_request(source, req, client_id, server, request_envelope_id)
                .await?;
        }
        Some(actr_to_signaling::Payload::SubscribeActrUpRequest(req)) => {
            handle_subscribe_actr_up(source, req, client_id, server, request_envelope_id).await?;
        }
        Some(actr_to_signaling::Payload::UnsubscribeActrUpRequest(req)) => {
            handle_unsubscribe_actr_up(source, req, client_id, server, request_envelope_id).await?;
        }
        Some(actr_to_signaling::Payload::Error(error)) => {
            error!(
                "收到客户端错误报告 (Actor {}): code={}, message={}",
                source.serial_number, error.code, error.message
            );
        }
        None => {
            warn!("ActrToSignaling 消息缺少 payload");
        }
    }

    Ok(())
}

/// 处理心跳
async fn handle_ping(
    source: ActrId,
    ping: Ping,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
    in_tolerance_period: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "💓 收到 Actor {} 心跳: availability={}, power_reserve={:.2}, mailbox_backlog={:.2}, sticky_clients={}{}",
        source.serial_number,
        ping.availability,
        ping.power_reserve,
        ping.mailbox_backlog,
        ping.sticky_client_ids.len(),
        if in_tolerance_period {
            " [⚠️ Key in tolerance period]"
        } else {
            ""
        }
    );

    // 存储负载指标到 ServiceRegistry
    let mut registry = server.service_registry.write().await;
    if let Err(e) = registry.update_load_metrics(
        &source,
        ping.availability,
        ping.power_reserve,
        ping.mailbox_backlog,
    ) {
        warn!(
            "更新 Actor {} 负载指标失败: {}, 尝试从数据库恢复服务",
            source.serial_number, e
        );

        // 尝试从数据库恢复服务
        match registry.restore_service_from_storage(&source).await {
            Ok(true) => {
                info!(
                    "✅ 成功从数据库恢复 Actor {} 的服务注册",
                    source.serial_number
                );

                // 恢复后再次尝试更新负载指标
                if let Err(e2) = registry.update_load_metrics(
                    &source,
                    ping.availability,
                    ping.power_reserve,
                    ping.mailbox_backlog,
                ) {
                    error!(
                        "❌ 从数据库恢复后仍无法更新 Actor {} 的负载指标: {}",
                        source.serial_number, e2
                    );
                } else {
                    info!(
                        "✅ Actor {} 服务恢复后负载指标更新成功",
                        source.serial_number
                    );
                }
            }
            Ok(false) => {
                warn!(
                    "⚠️  数据库中未找到 Actor {} 的服务信息 (可能已过期或从未注册)",
                    source.serial_number
                );
                // TODO: 可选 - 在 Pong 响应中添加警告，提示客户端重新注册
            }
            Err(e) => {
                error!(
                    "❌ 从数据库恢复 Actor {} 的服务失败: {}",
                    source.serial_number, e
                );
            }
        }
    }
    drop(registry);

    // 创建 Pong 响应
    let mut pong = Pong {
        seq: chrono::Utc::now().timestamp() as u64,
        suggest_interval_secs: Some(30),
        credential_warning: None,
    };

    // 如果密钥在容忍期，添加警告
    if in_tolerance_period {
        warn!(
            "⚠️  Actor {} credential key is in tolerance period",
            source.serial_number
        );
        pong.credential_warning = Some(actr_protocol::CredentialWarning {
            r#type: actr_protocol::credential_warning::WarningType::KeyInTolerancePeriod as i32,
            message:
                "Your credential key is in tolerance period. Please update your credential soon."
                    .to_string(),
        });
    }

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: source,
        payload: Some(signaling_to_actr::Payload::Pong(pong)),
    });

    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));

    send_envelope_to_client(client_id, response_envelope, server).await?;

    Ok(())
}

/// 处理注销
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_unregister(
    source: ActrId,
    req: actr_protocol::UnregisterRequest,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "👋 Actor {} 注销: reason={:?}",
        source.serial_number,
        req.reason.as_deref().unwrap_or("未提供")
    );

    // 发送 UnregisterResponse
    let response = actr_protocol::UnregisterResponse {
        result: Some(actr_protocol::unregister_response::Result::Success(
            actr_protocol::unregister_response::UnregisterOk {},
        )),
    };

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: source,
        payload: Some(signaling_to_actr::Payload::UnregisterResponse(response)),
    });

    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
    send_envelope_to_client(client_id, response_envelope, server).await?;

    // 清理客户端连接
    cleanup_client(client_id, server).await;

    Ok(())
}

/// 通过 actor_id_index 快速解析 client_id，保持索引与 clients 同步
async fn resolve_client_id_by_actor_id(
    actor_id: &ActrId,
    server: &SignalingServerHandle,
) -> Result<String, String> {
    let client_id = {
        let index_guard = server.actor_id_index.read().await;
        index_guard.get(actor_id).cloned()
    };

    let client_id = match client_id {
        Some(id) => id,
        None => {
            warn!(
                "⚠️  Actor {} 缺少 client_id 索引，可能尚未注册或已清理",
                format_actor_id(actor_id)
            );
            return Err("client_id not found for actor_id".into());
        }
    };

    let exists = server.clients.read().await.contains_key(&client_id);
    if !exists {
        warn!(
            "⚠️  Actor {} 索引指向不存在的客户端 {}，索引可能已过期",
            format_actor_id(actor_id),
            client_id
        );
        return Err("actor_id_index stale for actor_id".into());
    }

    Ok(client_id)
}

fn format_actor_id(actor_id: &ActrId) -> String {
    format!(
        "realm={} serial={}",
        actor_id.realm.realm_id, actor_id.serial_number
    )
}

/// 处理 ActrRelay（WebRTC 信令中继）
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_actr_relay(
    relay: ActrRelay,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
    #[cfg(feature = "opentelemetry")] remote_context: opentelemetry::Context,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = relay.source.clone();
    let target = &relay.target;
    // 验证源 Actor 的 realm（存在、未过期且状态正常）
    let realm_id = source.realm.realm_id;
    if let Err(e) = RealmEntity::validate_realm(realm_id).await {
        warn!("⚠️  Actor {} realm 验证失败: {}", source.serial_number, e);
        send_error_response(
            client_id,
            &source,
            403,
            &format!("Realm validation failed: {e}"),
            server,
            Some(request_envelope_id),
        )
        .await?;
        return Ok(());
    }

    info!(
        "🔀 中继信令: {} -> {}",
        source.serial_number, target.serial_number
    );

    tracing::debug!(?relay, "handle_actr_relay");

    // ACL check: can source relay to target?
    use actrix_common::realm::acl::ActorAcl;
    let source_realm = source.realm.realm_id;
    let target_realm = target.realm.realm_id;

    // Cross-realm relay is denied by default for security
    if source_realm != target_realm {
        warn!(
            "⚠️  ACL denied cross-realm relay: realm {} -> realm {}",
            source_realm, target_realm
        );
        send_error_response(
            client_id,
            &source,
            403,
            "Cross-realm relay is not allowed",
            server,
            Some(request_envelope_id),
        )
        .await?;
        return Ok(());
    }

    // Same realm: check ACL rules (always enforced)
    let source_type = type_key(&source.r#type);
    let target_type = type_key(&target.r#type);

    let can_relay = ActorAcl::can_discover(source_realm, &source_type, &target_type)
        .await
        .unwrap_or(false);

    if !can_relay {
        warn!(
            "⚠️  ACL denied relay: {} -> {}",
            source.serial_number, target.serial_number
        );
        send_error_response(
            client_id,
            &source,
            403,
            "ACL policy denies relay to target actor",
            server,
            Some(request_envelope_id),
        )
        .await?;
        return Ok(());
    }

    // 验证 credential
    // 验证 credential，并提取 claims 用于后续身份核查
    let claims = match AIdCredentialValidator::check(&relay.credential, source.realm.realm_id)
        .await
    {
        Ok((claims, _)) => claims,
        Err(e) => {
            warn!(
                "⚠️  Actor {} credential 验证失败: {}",
                source.serial_number, e
            );
            send_error_response(
                client_id,
                &source,
                401,
                &format!("Credential validation failed: {e}"),
                server,
                Some(request_envelope_id),
            )
            .await?;
            return Ok(());
        }
    };

    // 验证 credential 绑定的 actor_id 与 relay.source 一致，防止客户端伪造来源身份
    let source_repr = source.to_string_repr();
    if claims.actor_id != source_repr {
        warn!(
            "⚠️  relay.source 与 credential 绑定的 actor_id 不一致: source={}, credential={}",
            source_repr, claims.actor_id
        );
        send_error_response(
            client_id,
            &source,
            403,
            "Source identity mismatch: relay.source does not match credential",
            server,
            Some(request_envelope_id),
        )
        .await?;
        return Ok(());
    }

    // Role negotiation: server decides offerer/answerer and notifies both parties
    if let Some(actr_relay::Payload::RoleNegotiation(RoleNegotiation { from, to, .. })) =
        relay.payload.clone()
    {
        let clients_guard = server.clients.read().await;

        // 使用 determine_webrtc_role 函数确定角色
        let is_offerer = determine_webrtc_role(&from, &to, &clients_guard);

        // 判断双方是否有固定网络配置 (webrtc_role == "answer")
        let from_fixed = clients_guard
            .values()
            .find(|c| c.actor_id.as_ref() == Some(&from))
            .and_then(|c| c.webrtc_role.as_deref())
            == Some("answer");

        let to_fixed = clients_guard
            .values()
            .find(|c| c.actor_id.as_ref() == Some(&to))
            .and_then(|c| c.webrtc_role.as_deref())
            == Some("answer");

        drop(clients_guard);

        // 发送给 from 的 RoleAssignment，remote_fixed 表示 to 的配置状态
        let new_relay = ActrRelay {
            // source: peer actor (对端)，target: 该 assignment 的接收方
            source: from.clone(),
            credential: relay.credential.clone(),
            target: to.clone(),
            payload: Some(actr_relay::Payload::RoleAssignment(RoleAssignment {
                is_offerer,
                remote_fixed: Some(to_fixed),
            })),
        };
        send_role_assignment(
            &from,
            server,
            new_relay.clone(),
            #[cfg(feature = "opentelemetry")]
            remote_context.clone(),
        )
        .await?;

        // 发送给 to 的 RoleAssignment，remote_fixed 表示 from 的配置状态
        let new_relay = ActrRelay {
            // source: peer actor (对端)，target: 该 assignment 的接收方
            source: from.clone(),
            credential: relay.credential.clone(),
            target: to.clone(),
            payload: Some(actr_relay::Payload::RoleAssignment(RoleAssignment {
                is_offerer: !is_offerer,
                remote_fixed: Some(from_fixed),
            })),
        };

        send_role_assignment(
            &to,
            server,
            new_relay,
            #[cfg(feature = "opentelemetry")]
            remote_context,
        )
        .await?;

        return Ok(());
    }

    // 查找目标客户端并转发其他中继消息
    let clients_guard = server.clients.read().await;
    let target_client_id = clients_guard.iter().find_map(|(id, client)| {
        client.actor_id.as_ref().and_then(|actor_id| {
            if actor_id.realm.realm_id == target.realm.realm_id
                && actor_id.serial_number == target.serial_number
            {
                Some(id.clone())
            } else {
                None
            }
        })
    });

    if let Some(target_client_id) = target_client_id {
        // 重新构造 envelope 并转发
        let flow = signaling_envelope::Flow::ActrRelay(relay);
        #[allow(unused_mut)]
        let mut forward_envelope = server.create_new_envelope(flow);

        // Inject the original trace context into the forwarded envelope to ensure end-to-end tracing
        #[cfg(feature = "opentelemetry")]
        inject_trace_context(&remote_context, &mut forward_envelope);
        send_envelope_to_client(&target_client_id, forward_envelope, server).await?;

        info!("✅ 信令中继成功");
    } else {
        warn!("⚠️ 未找到目标 Actor {}", target.serial_number);
    }

    Ok(())
}

// 计算用于排序的 ActorId key，确保角色分配可重复
fn actor_order_key(id: &ActrId) -> (u32, u64, String, String) {
    (
        id.realm.realm_id,
        id.serial_number,
        id.r#type.manufacturer.clone(),
        id.r#type.name.clone(),
    )
}

/// 根据双方的角色偏好和 ActorId 确定发起方是否为 offerer
///
/// # 角色判定规则:
/// 1. 如果一方明确要求当 "answer" 而另一方没有要求，则满足该要求
/// 2. 如果双方都有相同偏好（都想当 "answer" 或都没要求），则回退到 ActrId 静态排序逻辑
///
/// # Arguments
/// * `from` - 发起方的 ActorId
/// * `to` - 接收方的 ActorId
/// * `clients` - 客户端连接映射表,用于查找角色偏好
///
/// # Returns
/// * `true` - 发起方应该是 offerer
/// * `false` - 发起方应该是 answerer
fn determine_webrtc_role(
    from: &ActrId,
    to: &ActrId,
    clients: &HashMap<String, ClientConnection>,
) -> bool {
    // 从客户端连接中查找双方的角色偏好
    let from_role = clients
        .values()
        .find(|c| c.actor_id.as_ref() == Some(from))
        .and_then(|c| c.webrtc_role.as_deref());

    let to_role = clients
        .values()
        .find(|c| c.actor_id.as_ref() == Some(to))
        .and_then(|c| c.webrtc_role.as_deref());

    let is_offerer = if from_role == Some("answer") && to_role != Some("answer") {
        false
    } else if to_role == Some("answer") && from_role != Some("answer") {
        true
    } else {
        // 其他情况(双方都偏好 answer 或都无偏好)，使用 ActorId 排序
        actor_order_key(from) < actor_order_key(to)
    };

    info!(
        "⚖️ 角色协商完成: {} (role={:?}) -> {} (role={:?}), is_offerer={}",
        from.serial_number, from_role, to.serial_number, to_role, is_offerer
    );

    is_offerer
}

#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all))]
async fn send_role_assignment(
    target_actor: &ActrId,
    server: &SignalingServerHandle,
    relay: ActrRelay,
    #[cfg(feature = "opentelemetry")] remote_context: opentelemetry::Context,
) -> Result<(), Box<dyn std::error::Error>> {
    let flow = signaling_envelope::Flow::ActrRelay(relay);
    #[allow(unused_mut)]
    let mut envelope = server.create_new_envelope(flow);

    #[cfg(feature = "opentelemetry")]
    inject_trace_context(&remote_context, &mut envelope);

    let mut buf = Vec::new();
    envelope.encode(&mut buf)?;

    let clients_guard = server.clients.read().await;
    if let Some(client) = clients_guard.values().find(|client| {
        client.actor_id.as_ref().is_some_and(|id| {
            id.realm.realm_id == target_actor.realm.realm_id
                && id.serial_number == target_actor.serial_number
        })
    }) {
        debug!(
            "send_role_assignment: 发送 envelope 到客户端 {:?}",
            client.actor_id
        );
        client
            .direct_sender
            .send(WsMessage::Binary(buf.into()))
            .map_err(|e| e.into())
    } else {
        warn!(
            "⚠️ send_role_assignment: 未找到目标 Actor {}",
            target_actor.serial_number
        );
        Ok(())
    }
}

/// 发送 SignalingEnvelope 到客户端
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = envelope.envelope_id)))]
async fn send_envelope_to_client(
    client_id: &str,
    #[allow(unused_mut)] mut envelope: SignalingEnvelope,
    server: &SignalingServerHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let clients_guard = server.clients.read().await;

    if let Some(client) = clients_guard.get(client_id) {
        #[cfg(feature = "opentelemetry")]
        {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            let context = tracing::Span::current().context();
            inject_trace_context(&context, &mut envelope);
        }

        // 编码 protobuf
        let mut buf = Vec::new();
        envelope.encode(&mut buf)?;

        // 发送 Binary 消息
        match client.direct_sender.send(WsMessage::Binary(buf.into())) {
            Ok(_) => {
                info!("✅ 成功发送 envelope 到客户端 {}", client_id);
                Ok(())
            }
            Err(e) => {
                error!("❌ 发送失败: {}", e);
                Err(format!("发送失败: {e}").into())
            }
        }
    } else {
        warn!("⚠️ 未找到客户端 {}", client_id);
        Err(format!("客户端 {client_id} 未找到").into())
    }
}

/// 清理客户端连接
async fn cleanup_client(client_id: &str, server: &SignalingServerHandle) {
    let removed_client = {
        let mut clients_guard = server.clients.write().await;
        clients_guard.remove(client_id)
    };

    if let Some(client) = removed_client {
        if let Some(actor_id) = client.actor_id {
            info!("🧹 清理 Actor {} 的连接", actor_id.serial_number);

            // Remove all services for this Actor from the ServiceRegistry to avoid stale ghost instances
            server
                .service_registry
                .write()
                .await
                .unregister_actor(&actor_id);

            let mut actor_index = server.actor_id_index.write().await;
            match actor_index.remove(&actor_id) {
                Some(mapped_client) if mapped_client != client_id => warn!(
                    "⚠️  Actor {} 索引指向意外客户端 {}，已移除",
                    actor_id.serial_number, mapped_client
                ),
                None => warn!("⚠️  Actor {} 清理时未找到索引条目", actor_id.serial_number),
                _ => {}
            }
        }

        // 移除消息速率限制器
        if let Some(ref limiter) = server.message_rate_limiter {
            limiter.remove_connection(client_id).await;
        }
    }
}

/// 处理 Credential 更新请求
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_credential_update(
    source: ActrId,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "🔑 处理 Actor {} 的 Credential 更新请求",
        source.serial_number
    );

    // 检查是否配置了 AIS 客户端
    let ais_client = match &server.ais_client {
        Some(client) => client,
        None => {
            warn!("⚠️  AIS 客户端未配置，无法刷新 Credential");
            let error_response = ErrorResponse {
                code: 503,
                message: "AIS service not configured".to_string(),
            };

            let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
                target: source.clone(),
                payload: Some(signaling_to_actr::Payload::Error(error_response)),
            });

            let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
            send_envelope_to_client(client_id, response_envelope, server).await?;
            return Ok(());
        }
    };

    // 调用 AIS 刷新 Credential
    match ais_client
        .refresh_credential(source.realm.realm_id, source.r#type.clone())
        .await
    {
        Ok(register_response) => {
            use actr_protocol::register_response::Result as RegisterResult;

            match register_response.result {
                Some(RegisterResult::Success(register_ok)) => {
                    let new_credential = register_ok.credential;
                    let expires_at = register_ok.credential_expires_at;

                    // 更新客户端连接中存储的 credential
                    {
                        let mut clients_guard = server.clients.write().await;
                        if let Some(client_conn) = clients_guard.get_mut(client_id) {
                            client_conn.credential = Some(new_credential.clone());
                            info!(
                                "✅ 已更新 Actor {} 的 Credential (key_id={})",
                                source.serial_number, new_credential.token_key_id
                            );
                        }
                    }

                    // 返回成功响应（使用 RegisterResponse，因为协议中没有 CredentialUpdateResponse）
                    // 新的 PSK 已被加密到 token 中，客户端必须同步更新本地 PSK，否则 TURN 认证会失败
                    use actr_protocol::register_response::RegisterOk;
                    let response = actr_protocol::RegisterResponse {
                        result: Some(actr_protocol::register_response::Result::Success(
                            RegisterOk {
                                actr_id: source.clone(),
                                credential: new_credential.clone(),
                                psk: register_ok.psk.clone(), // 发送新 PSK，确保客户端与 token 中的 PSK 保持同步
                                credential_expires_at: expires_at,
                                signaling_heartbeat_interval_secs: 30, // 保持心跳间隔
                            },
                        )),
                    };

                    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
                        target: source,
                        payload: Some(signaling_to_actr::Payload::RegisterResponse(response)),
                    });

                    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
                    send_envelope_to_client(client_id, response_envelope, server).await?;

                    info!("✅ Credential 更新成功");
                }
                Some(RegisterResult::Error(err)) => {
                    error!("❌ AIS 返回错误: {} - {}", err.code, err.message);

                    let error_response = ErrorResponse {
                        code: err.code,
                        message: format!("AIS error: {}", err.message),
                    };

                    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
                        target: source,
                        payload: Some(signaling_to_actr::Payload::Error(error_response)),
                    });

                    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
                    send_envelope_to_client(client_id, response_envelope, server).await?;
                }
                None => {
                    error!("❌ AIS 返回空响应");

                    let error_response = ErrorResponse {
                        code: 500,
                        message: "AIS returned empty response".to_string(),
                    };

                    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
                        target: source,
                        payload: Some(signaling_to_actr::Payload::Error(error_response)),
                    });

                    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
                    send_envelope_to_client(client_id, response_envelope, server).await?;
                }
            }
        }
        Err(e) => {
            error!("❌ 调用 AIS 失败: {}", e);

            let error_response = ErrorResponse {
                code: 500,
                message: format!("Failed to refresh credential: {e}"),
            };

            let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
                target: source,
                payload: Some(signaling_to_actr::Payload::Error(error_response)),
            });

            let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
            send_envelope_to_client(client_id, response_envelope, server).await?;
        }
    }

    Ok(())
}

/// 处理服务发现请求
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_discovery_request(
    source: ActrId,
    req: actr_protocol::DiscoveryRequest,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "🔍 处理 Actor {} 的 Discovery 请求: manufacturer={:?}, limit={}",
        source.serial_number,
        req.manufacturer.as_deref().unwrap_or("*"),
        req.limit.unwrap_or(64)
    );

    // 从 ServiceRegistry 查询所有服务
    let registry = server.service_registry.read().await;
    let services = registry.discover_all(req.manufacturer.as_deref());
    let total_count = services.len(); // Save count before moving

    // Apply ACL filtering (if ACL is enabled)
    use actrix_common::realm::acl::ActorAcl;
    let source_realm = source.realm.realm_id;
    let source_type = type_key(&source.r#type);

    let mut acl_filtered_services = Vec::new();

    // ACL always enabled: filter services based on ACL rules
    for service in services {
        let target_realm = service.actor_id.realm.realm_id;
        let target_type = type_key(&service.actor_id.r#type);

        // Only check ACL if in same realm
        if source_realm == target_realm {
            match ActorAcl::can_discover(source_realm, &source_type, &target_type).await {
                Ok(true) => acl_filtered_services.push(service),
                Ok(false) => {
                    debug!(
                        "ACL denied discovery: {} cannot discover {}",
                        source.serial_number, service.actor_id.serial_number
                    );
                }
                Err(e) => {
                    warn!(
                        "ACL check failed for {} -> {}: {}",
                        source.serial_number, service.actor_id.serial_number, e
                    );
                }
            }
        } else {
            // Cross-realm discovery denied
            debug!(
                "Cross-realm discovery denied: {} -> {}",
                source_realm, target_realm
            );
        }
    }
    info!(
        "ACL filtering: {} -> {} services",
        total_count,
        acl_filtered_services.len()
    );

    // 按 ActrType 聚合服务（使用 HashMap 去重）
    use std::collections::HashMap;
    let mut type_map: HashMap<String, actr_protocol::discovery_response::TypeEntry> =
        HashMap::new();

    for service in acl_filtered_services {
        let type_key = type_key(&service.actor_id.r#type);

        // 如果该类型还未添加，创建新条目
        type_map.entry(type_key).or_insert_with(|| {
            let (fingerprint, description, published_at, tags) = service
                .service_spec
                .as_ref()
                .map(|spec| {
                    (
                        spec.fingerprint.clone(),
                        spec.description.clone(),
                        spec.published_at,
                        spec.tags.clone(),
                    )
                })
                .unwrap_or_else(|| ("unknown".to_string(), None, None, Vec::new()));

            actr_protocol::discovery_response::TypeEntry {
                actr_type: service.actor_id.r#type.clone(),
                name: service.service_name.clone(),
                description,
                service_fingerprint: fingerprint,
                published_at,
                tags,
            }
        });
    }

    // 转换为 Vec 并应用 limit
    let mut entries: Vec<_> = type_map.into_values().collect();
    let limit = req.limit.unwrap_or(64) as usize;
    entries.truncate(limit);

    drop(registry);

    info!(
        "✅ 为 Actor {} 返回 {} 个服务类型",
        source.serial_number,
        entries.len()
    );

    let response = actr_protocol::DiscoveryResponse {
        result: Some(actr_protocol::discovery_response::Result::Success(
            actr_protocol::discovery_response::DiscoveryOk { entries },
        )),
    };

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: source,
        payload: Some(signaling_to_actr::Payload::DiscoveryResponse(response)),
    });

    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
    send_envelope_to_client(client_id, response_envelope, server).await?;

    Ok(())
}

/// 处理路由候选请求（负载均衡）
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_route_candidates_request(
    source: ActrId,
    req: actr_protocol::RouteCandidatesRequest,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // 从请求中获取 client_fingerprint，如果存在则启用兼容性协商模式
    let client_fingerprint_from_req = req.client_fingerprint.trim().to_string();

    info!(
        "🎯 处理 Actor {} 的 RouteCandidates 请求: target_type={}/{}, client_fp={:?}",
        source.serial_number,
        req.target_type.manufacturer,
        req.target_type.name,
        client_fingerprint_from_req
    );

    // 从 ServiceRegistry 查询所有匹配 target_type 的实例
    let registry = server.service_registry.read().await;
    let candidates = registry.find_by_actr_type(&req.target_type);
    drop(registry);

    let total_candidates = candidates.len();

    if candidates.is_empty() {
        info!(
            "⚠️  未找到 {}/{} 类型的服务实例",
            req.target_type.manufacturer, req.target_type.name
        );
    } else {
        info!(
            "📋 找到 {} 个 {}/{} 类型的候选实例",
            total_candidates, req.target_type.manufacturer, req.target_type.name
        );
    }

    // Apply ACL filtering
    use actrix_common::realm::acl::ActorAcl;
    let source_realm = source.realm.realm_id;
    let source_type = type_key(&source.r#type);

    let mut acl_filtered_candidates = Vec::new();
    for candidate in candidates {
        let target_realm = candidate.actor_id.realm.realm_id;
        let candidate_type_key = type_key(&candidate.actor_id.r#type);

        if source_realm == target_realm {
            match ActorAcl::can_discover(source_realm, &source_type, &candidate_type_key).await {
                Ok(true) => acl_filtered_candidates.push(candidate),
                Ok(false) => {
                    debug!(
                        "ACL denied route candidate: {} cannot access {}",
                        source.serial_number, candidate.actor_id.serial_number
                    );
                }
                Err(e) => {
                    warn!(
                        "ACL check failed for {} -> {}: {}",
                        source.serial_number, candidate.actor_id.serial_number, e
                    );
                }
            }
        } else {
            debug!(
                "Cross-realm route candidate denied: {} -> {}",
                source_realm, target_realm
            );
        }
    }
    info!(
        "ACL filtering for route candidates: {} -> {} candidates",
        total_candidates,
        acl_filtered_candidates.len()
    );

    // 获取客户端 fingerprint（优先使用请求中的，否则从 registry 获取）
    let client_fingerprint = client_fingerprint_from_req;

    // 从请求中提取客户端位置（如果提供）
    let client_location = req.client_location.as_ref().and_then(|loc| {
        if let (Some(lat), Some(lon)) = (loc.latitude, loc.longitude) {
            Some((lat, lon))
        } else {
            None
        }
    });

    // 兼容性协商逻辑
    let (ranked_actor_ids, compatibility_info, has_exact_match, is_sub_healthy, ws_address_map) =
        if !client_fingerprint.is_empty() {
            // 有 client_fingerprint 就启用协商模式
            perform_compatibility_negotiation(
                &acl_filtered_candidates,
                &client_fingerprint,
                &req.target_type,
                server,
                req.criteria.as_ref(),
                client_id,
                client_location,
            )
            .await
        } else {
            // 非协商模式：使用原有的 LoadBalancer 排序
            let cache_guard = server.compatibility_cache.read().await;
            let compatibility_cache = Some(&*cache_guard);

            // 先提取 ws_address 信息（rank_candidates 会 move acl_filtered_candidates）
            let ws_info: Vec<(ActrId, Option<String>)> = acl_filtered_candidates
                .iter()
                .map(|c| (c.actor_id.clone(), c.ws_address.clone()))
                .collect();

            let ranked = LoadBalancer::rank_candidates(
                acl_filtered_candidates,
                req.criteria.as_ref(),
                Some(client_id),
                client_location,
                compatibility_cache,
                None,
            );

            // ws_address 通过专用参数返回，compat_info 保持为空
            (ranked, vec![], None, None, ws_info)
        };

    info!(
        "✅ 为 Actor {} 返回 {} 个候选 (has_exact_match={:?}, is_sub_healthy={:?})",
        source.serial_number,
        ranked_actor_ids.len(),
        has_exact_match,
        is_sub_healthy
    );

    let ws_address_map_proto: Vec<actr_protocol::WsAddressEntry> = ws_address_map
        .into_iter()
        .map(|(id, ws)| actr_protocol::WsAddressEntry {
            candidate_id: id,
            ws_address: ws,
        })
        .collect();

    let response = actr_protocol::RouteCandidatesResponse {
        result: Some(actr_protocol::route_candidates_response::Result::Success(
            actr_protocol::route_candidates_response::RouteCandidatesOk {
                candidates: ranked_actor_ids,
                compatibility_info,
                has_exact_match,
                is_sub_healthy,
                ws_address_map: ws_address_map_proto,
            },
        )),
    };

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: source,
        payload: Some(signaling_to_actr::Payload::RouteCandidatesResponse(
            response,
        )),
    });

    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
    send_envelope_to_client(client_id, response_envelope, server).await?;

    Ok(())
}

/// 执行完整的兼容性协商
///
/// 返回：(排序后的 ActrId 列表, 兼容性信息列表, 是否有精确匹配, 是否处于亚健康状态)
async fn perform_compatibility_negotiation(
    candidates: &[crate::service_registry::ServiceInfo],
    client_fingerprint: &str,
    target_type: &ActrType,
    server: &SignalingServerHandle,
    criteria: Option<&actr_protocol::route_candidates_request::NodeSelectionCriteria>,
    _client_id: &str,
    _client_location: Option<(f64, f64)>,
) -> (
    Vec<ActrId>,
    Vec<actr_protocol::CandidateCompatibilityInfo>,
    Option<bool>,
    Option<bool>,
    Vec<(ActrId, Option<String>)>,
) {
    use crate::compatibility_cache::CompatibilityReportData;
    use actr_version::{CompatibilityAnalysisResult, CompatibilityLevel, ServiceCompatibility};

    let mut exact_matches: Vec<ActrId> = Vec::new();
    let mut compatible_candidates: Vec<(
        ActrId,
        actr_protocol::CompatibilityLevel,
        Option<CompatibilityAnalysisResult>,
    )> = Vec::new();
    let mut compatibility_info: Vec<actr_protocol::CandidateCompatibilityInfo> = Vec::new();

    // 获取 ServiceRegistryStorage 用于查询 Proto specs
    let storage = {
        let registry = server.service_registry.read().await;
        registry.get_storage()
    };

    // 获取客户端的 ServiceSpec（从 service_specs 表）
    let client_spec = if let Some(ref storage) = storage {
        match storage
            .get_proto_by_fingerprint(target_type, client_fingerprint)
            .await
        {
            Ok(Some(spec)) => Some(spec),
            Ok(None) => {
                warn!(
                    "⚠️ 客户端 fingerprint {} 未找到对应的 spec，将仅使用指纹匹配",
                    client_fingerprint
                );
                None
            }
            Err(e) => {
                warn!("获取客户端 spec 失败: {}", e);
                None
            }
        }
    } else {
        None
    };

    // 遍历候选实例进行兼容性检查
    for candidate in candidates {
        let candidate_fingerprint = candidate
            .service_spec
            .as_ref()
            .map(|s| s.fingerprint.clone())
            .unwrap_or_default();
        let candidate_type_key = type_key(&candidate.actor_id.r#type);

        // 第一步：精确匹配检查（快速路径）
        if candidate_fingerprint == client_fingerprint {
            info!(
                "✅ 精确匹配: candidate={} fingerprint={}",
                candidate.actor_id.serial_number, candidate_fingerprint
            );
            exact_matches.push(candidate.actor_id.clone());
            compatibility_info.push(actr_protocol::CandidateCompatibilityInfo {
                candidate_id: candidate.actor_id.clone(),
                candidate_fingerprint: candidate_fingerprint.clone(),
                analysis_result: None, // 精确匹配无需分析
                is_exact_match: Some(true),
                ws_address: candidate.ws_address.clone(),
            });
            continue;
        }

        // 第二步：检查全局兼容性缓存
        let cache_key = crate::compatibility_cache::GlobalCompatibilityCache::build_cache_key(
            &candidate_type_key,
            client_fingerprint,
            &candidate_fingerprint,
        );

        let mut cache_guard = server.compatibility_cache.write().await;
        let cache_response = cache_guard.query(&cache_key);
        drop(cache_guard);

        if cache_response.hit {
            // 缓存命中，使用缓存的 CompatibilityAnalysisResult
            if let Some(cached_analysis) = cache_response.analysis_result {
                let is_compatible = cached_analysis.is_compatible();
                let level = match cached_analysis.level {
                    CompatibilityLevel::FullyCompatible => {
                        actr_protocol::CompatibilityLevel::FullyCompatible
                    }
                    CompatibilityLevel::BackwardCompatible => {
                        actr_protocol::CompatibilityLevel::BackwardCompatible
                    }
                    CompatibilityLevel::BreakingChanges => {
                        actr_protocol::CompatibilityLevel::BreakingChanges
                    }
                };

                if is_compatible {
                    compatible_candidates.push((
                        candidate.actor_id.clone(),
                        level,
                        Some(cached_analysis.clone()),
                    ));
                }

                // 将 CompatibilityAnalysisResult 转换为 proto 版本
                let proto_result = convert_to_proto_analysis_result(&cached_analysis);

                compatibility_info.push(actr_protocol::CandidateCompatibilityInfo {
                    candidate_id: candidate.actor_id.clone(),
                    candidate_fingerprint: candidate_fingerprint.clone(),
                    analysis_result: Some(proto_result),
                    is_exact_match: Some(false),
                    ws_address: candidate.ws_address.clone(),
                });

                info!(
                    "🔄 缓存命中: candidate={} level={:?}",
                    candidate.actor_id.serial_number, cached_analysis.level
                );
                continue;
            }
        }

        // 第三步：执行兼容性分析（缓存未命中）
        if client_spec.is_none() {
            // 没有客户端 spec，无法进行深度分析
            compatibility_info.push(actr_protocol::CandidateCompatibilityInfo {
                candidate_id: candidate.actor_id.clone(),
                candidate_fingerprint: candidate_fingerprint.clone(),
                analysis_result: None,
                is_exact_match: Some(false),
                ws_address: candidate.ws_address.clone(),
            });
            continue;
        }

        let candidate_spec = match &candidate.service_spec {
            Some(spec) => spec,
            None => {
                compatibility_info.push(actr_protocol::CandidateCompatibilityInfo {
                    candidate_id: candidate.actor_id.clone(),
                    candidate_fingerprint: candidate_fingerprint.clone(),
                    analysis_result: None,
                    is_exact_match: Some(false),
                    ws_address: candidate.ws_address.clone(),
                });
                continue;
            }
        };

        // 使用 actr-version 进行深度兼容性分析
        match ServiceCompatibility::analyze_compatibility(
            client_spec.as_ref().unwrap(),
            candidate_spec,
        ) {
            Ok(analysis_result) => {
                let is_compatible = analysis_result.is_compatible();
                let level = match analysis_result.level {
                    CompatibilityLevel::FullyCompatible => {
                        actr_protocol::CompatibilityLevel::FullyCompatible
                    }
                    CompatibilityLevel::BackwardCompatible => {
                        actr_protocol::CompatibilityLevel::BackwardCompatible
                    }
                    CompatibilityLevel::BreakingChanges => {
                        actr_protocol::CompatibilityLevel::BreakingChanges
                    }
                };

                // 缓存分析结果
                {
                    let mut cache_guard = server.compatibility_cache.write().await;
                    cache_guard.store(CompatibilityReportData {
                        from_fingerprint: client_fingerprint.to_string(),
                        to_fingerprint: candidate_fingerprint.clone(),
                        service_type: candidate_type_key.clone(),
                        analysis_result: analysis_result.clone(),
                    });
                }

                if is_compatible {
                    compatible_candidates.push((
                        candidate.actor_id.clone(),
                        level,
                        Some(analysis_result.clone()),
                    ));
                }

                let proto_result = convert_to_proto_analysis_result(&analysis_result);

                compatibility_info.push(actr_protocol::CandidateCompatibilityInfo {
                    candidate_id: candidate.actor_id.clone(),
                    candidate_fingerprint: candidate_fingerprint.clone(),
                    analysis_result: Some(proto_result),
                    is_exact_match: Some(false),
                    ws_address: candidate.ws_address.clone(),
                });

                info!(
                    "🔍 兼容性分析: candidate={} level={:?}",
                    candidate.actor_id.serial_number, analysis_result.level
                );
            }
            Err(e) => {
                warn!(
                    "兼容性分析失败: candidate={} error={}",
                    candidate.actor_id.serial_number, e
                );
                compatibility_info.push(actr_protocol::CandidateCompatibilityInfo {
                    candidate_id: candidate.actor_id.clone(),
                    candidate_fingerprint: candidate_fingerprint.clone(),
                    analysis_result: None,
                    is_exact_match: Some(false),
                    ws_address: candidate.ws_address.clone(),
                });
            }
        }
    }

    // 确定返回结果
    let has_exact_match = !exact_matches.is_empty();
    let is_sub_healthy = !has_exact_match && !compatible_candidates.is_empty();

    // 构建最终的候选列表
    let final_candidates: Vec<ActrId> = if has_exact_match {
        // 优先返回精确匹配
        exact_matches
    } else if !compatible_candidates.is_empty() {
        // 返回兼容的候选（按兼容性级别排序）
        let mut sorted = compatible_candidates;
        sorted.sort_by_key(|(_, level, _)| *level as i32);
        sorted.into_iter().map(|(id, _, _)| id).collect()
    } else {
        // 没有兼容的候选
        vec![]
    };

    // 如果有数量限制，应用
    let limit = criteria
        .map(|c| c.candidate_count as usize)
        .unwrap_or(usize::MAX);
    let limited_candidates: Vec<ActrId> = final_candidates.into_iter().take(limit).collect();

    // Build ws_address_map from all candidates (same as non-negotiation path).
    // The actr client reads ws_address exclusively from ws_address_map,
    // so we must populate it here too.
    let ws_address_map: Vec<(ActrId, Option<String>)> = candidates
        .iter()
        .map(|c| (c.actor_id.clone(), c.ws_address.clone()))
        .collect();

    (
        limited_candidates,
        compatibility_info,
        Some(has_exact_match),
        Some(is_sub_healthy),
        ws_address_map,
    )
}

/// 将 actr-version 的 CompatibilityAnalysisResult 转换为 proto 版本
fn convert_to_proto_analysis_result(
    result: &actr_version::CompatibilityAnalysisResult,
) -> actr_protocol::CompatibilityAnalysisResult {
    let proto_level = match result.level {
        actr_version::CompatibilityLevel::FullyCompatible => {
            actr_protocol::CompatibilityLevel::FullyCompatible
        }
        actr_version::CompatibilityLevel::BackwardCompatible => {
            actr_protocol::CompatibilityLevel::BackwardCompatible
        }
        actr_version::CompatibilityLevel::BreakingChanges => {
            actr_protocol::CompatibilityLevel::BreakingChanges
        }
    };

    let changes: Vec<actr_protocol::ProtocolChange> = result
        .changes
        .iter()
        .map(|c| actr_protocol::ProtocolChange {
            change_type: c.change_type.clone(),
            file_name: c.file_name.clone(),
            location: c.location.clone(),
            description: c.description.clone(),
            is_breaking: c.is_breaking,
        })
        .collect();

    let breaking_changes: Vec<actr_protocol::ProtocolChange> = result
        .breaking_changes
        .iter()
        .map(|c| actr_protocol::ProtocolChange {
            change_type: c.rule.clone(),
            file_name: c.file.clone(),
            location: c.location.clone(),
            description: c.message.clone(),
            is_breaking: true,
        })
        .collect();

    actr_protocol::CompatibilityAnalysisResult {
        level: proto_level as i32,
        changes,
        breaking_changes,
        base_fingerprint: result.base_semantic_fingerprint.clone(),
        candidate_fingerprint: result.candidate_semantic_fingerprint.clone(),
        analyzed_at: result.analyzed_at.timestamp(),
    }
}

/// Handle GetServiceSpec request
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_get_service_spec_request(
    source: ActrId,
    req: actr_protocol::GetServiceSpecRequest,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let service_name = req.name.as_str();
    info!(
        "Handle GetServiceSpec request for Actor {} name={}",
        source.to_string_repr(),
        service_name
    );

    // Find matching ServiceSpec in ServiceRegistry
    let result = server
        .service_registry
        .read()
        .await
        .discover_by_service_name(service_name)
        .into_iter()
        .find_map(|service| service.service_spec.clone())
        .map(actr_protocol::get_service_spec_response::Result::Success)
        .unwrap_or_else(|| {
            actr_protocol::get_service_spec_response::Result::Error(ErrorResponse {
                code: 404,
                message: format!("Service specification not found for name={service_name}"),
            })
        });

    let response = actr_protocol::GetServiceSpecResponse {
        result: Some(result),
    };

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: source,
        payload: Some(signaling_to_actr::Payload::GetServiceSpecResponse(response)),
    });

    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
    send_envelope_to_client(client_id, response_envelope, server).await?;

    Ok(())
}

/// 处理订阅 Actor 上线事件
#[cfg_attr(feature = "opentelemetry", instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_subscribe_actr_up(
    source: ActrId,
    req: actr_protocol::SubscribeActrUpRequest,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "📢 Actor {} 订阅服务上线事件: target_type={}/{}",
        source.serial_number, req.target_type.manufacturer, req.target_type.name
    );

    // 添加订阅到 PresenceManager
    let mut presence = server.presence_manager.write().await;
    presence.subscribe(source.clone(), req.target_type);
    drop(presence);

    let response = actr_protocol::SubscribeActrUpResponse {
        result: Some(actr_protocol::subscribe_actr_up_response::Result::Success(
            actr_protocol::subscribe_actr_up_response::SubscribeOk {},
        )),
    };

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: source,
        payload: Some(signaling_to_actr::Payload::SubscribeActrUpResponse(
            response,
        )),
    });

    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
    send_envelope_to_client(client_id, response_envelope, server).await?;

    Ok(())
}

/// 处理取消订阅 Actor 上线事件
#[cfg_attr(feature = "opentelemetry", tracing::instrument(level = "debug", skip_all, fields(client_id, envelope_id = request_envelope_id)))]
async fn handle_unsubscribe_actr_up(
    source: ActrId,
    req: actr_protocol::UnsubscribeActrUpRequest,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "🔕 Actor {} 取消订阅服务上线事件: target_type={}/{}",
        source.serial_number, req.target_type.manufacturer, req.target_type.name
    );

    // 从 PresenceManager 移除订阅
    let mut presence = server.presence_manager.write().await;
    let removed = presence.unsubscribe(&source, &req.target_type);
    drop(presence);

    if !removed {
        warn!(
            "Actor {} 未订阅过 {}/{}",
            source.serial_number, req.target_type.manufacturer, req.target_type.name
        );
    }

    let response = actr_protocol::UnsubscribeActrUpResponse {
        result: Some(
            actr_protocol::unsubscribe_actr_up_response::Result::Success(
                actr_protocol::unsubscribe_actr_up_response::UnsubscribeOk {},
            ),
        ),
    };

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: source,
        payload: Some(signaling_to_actr::Payload::UnsubscribeActrUpResponse(
            response,
        )),
    });

    let response_envelope = server.create_envelope(flow, Some(request_envelope_id));
    send_envelope_to_client(client_id, response_envelope, server).await?;

    Ok(())
}

/// 发送通用错误响应
#[cfg_attr(feature = "opentelemetry", tracing::instrument(level = "debug", skip_all, fields(client_id, reply_for = ?reply_for, target = ?target)))]
async fn send_error_response(
    client_id: &str,
    target: &ActrId,
    code: u32,
    message: &str,
    server: &SignalingServerHandle,
    reply_for: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let error_response = ErrorResponse {
        code,
        message: message.to_string(),
    };

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: target.clone(),
        payload: Some(signaling_to_actr::Payload::Error(error_response)),
    });

    let response_envelope = server.create_envelope(flow, reply_for);
    send_envelope_to_client(client_id, response_envelope, server).await?;

    Ok(())
}

// Main function removed - SignalingServer can now be instantiated and started from other modules

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::{ActrType, Realm};
    use std::collections::HashMap;

    /// 创建测试用的 ActrId
    fn create_test_actr_id(serial: u64) -> ActrId {
        ActrId {
            serial_number: serial,
            realm: Realm { realm_id: 1001 },
            r#type: ActrType {
                manufacturer: "test".to_string(),
                name: "device".to_string(),
                version: None,
            },
        }
    }

    /// 创建测试用的 ClientConnection
    fn create_test_client(actor_id: ActrId, webrtc_role: Option<String>) -> ClientConnection {
        ClientConnection {
            id: uuid::Uuid::new_v4().to_string(),
            actor_id: Some(actor_id),
            credential: None,
            direct_sender: tokio::sync::mpsc::unbounded_channel().0,
            client_ip: None,
            webrtc_role,
        }
    }

    #[test]
    fn test_determine_webrtc_role() {
        // 测试数据: (from_serial, to_serial, from_role, to_role, expected_is_offerer, description)
        let test_cases = vec![
            (2, 1, None, None, false, "双方都没有偏好,使用 ActorId 排序"),
            (
                2,
                1,
                None,
                Some("answer"),
                true,
                "接收方偏好 answer,发起方应该是 offerer",
            ),
            (
                2,
                1,
                Some("answer"),
                None,
                false,
                "发起方偏好 answer,发起方应该是 answerer",
            ),
            (
                2,
                1,
                Some("answer"),
                Some("answer"),
                false,
                "双方都偏好 answer,回退到排序逻辑",
            ),
            (
                1,
                2,
                None,
                None,
                true,
                "serial 1 < serial 2,发起方应该是 offerer",
            ),
            (
                1,
                2,
                Some("answer"),
                Some("answer"),
                true,
                "双方都偏好 answer,serial 1 应该是 offerer",
            ),
        ];

        for (from_serial, to_serial, from_role, to_role, expected, desc) in test_cases {
            let from = create_test_actr_id(from_serial);
            let to = create_test_actr_id(to_serial);

            // 创建 clients map
            let mut clients = HashMap::new();
            clients.insert(
                format!("client_{from_serial}"),
                create_test_client(from.clone(), from_role.map(|s| s.to_string())),
            );
            clients.insert(
                format!("client_{to_serial}"),
                create_test_client(to.clone(), to_role.map(|s| s.to_string())),
            );

            let is_offerer = determine_webrtc_role(&from, &to, &clients);

            assert_eq!(
                is_offerer, expected,
                "测试失败: {desc} (from={from_serial}, to={to_serial}, from_role={from_role:?}, to_role={to_role:?})"
            );
        }
    }
}
