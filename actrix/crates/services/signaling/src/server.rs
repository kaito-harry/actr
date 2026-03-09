//! Actrix 信令服务器 - 基于 protobuf SignalingEnvelope
//!
//! 完全基于 protobuf 协议，使用 WebSocket Binary 消息传输
//!
//! # 功能概览
//!
//! ## 已实现的核心功能
//!
//! ### 基础信令流程
//! - ✅ Actor 注销 (`UnregisterRequest`)
//! - ✅ 心跳机制 (`Ping` / `Pong`)
//! - ✅ WebRTC 信令中继 (`ActrRelay` - ICE / SDP)
//!
//! ### 扩展功能
//! - ✅ 服务发现 (`DiscoveryRequest` / `DiscoveryResponse`)
//! - ✅ 负载均衡路由 (`RouteCandidatesRequest` / `RouteCandidatesResponse`)
//!   - 多因素排序：功率储备、邮箱积压、地理距离、客户端粘性
//!   - 精确匹配优先策略
//! - ✅ Presence 订阅 (`SubscribeActrUpRequest` / `ActrUpEvent`)
//! - ❌ Credential 刷新已迁移到 AIS HTTP
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
    AIdCredential, ActrId, ActrIdExt, ActrRelay, ActrToSignaling, ActrType, ErrorResponse,
    PeerToSignaling, Ping, Pong, Realm, RegisterResponse, RoleAssignment, RoleNegotiation,
    SignalingEnvelope, SignalingToActr, actr_relay, actr_to_signaling, peer_to_signaling,
    register_response, signaling_envelope, signaling_to_actr,
};
use futures_util::{SinkExt, StreamExt};
use platform::aid::credential::validator::AIdCredentialValidator;
use platform::realm::Realm as RealmEntity;
use prost::Message as ProstMessage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info_span;
use uuid::Uuid;

// Axum WebSocket
use axum::extract::ws::{Message as WsMessage, WebSocket};

use crate::load_balancer::LoadBalancer;
use crate::presence::PresenceManager;
use crate::service_registry::ServiceRegistry;
#[cfg(feature = "opentelemetry")]
use crate::trace::{extract_trace_context, inject_trace_context};
use tracing::Instrument;
#[cfg(feature = "opentelemetry")]
use tracing::instrument;

fn type_key(actor_type: &ActrType) -> String {
    format!("{}:{}", actor_type.manufacturer, actor_type.name)
}

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
        platform::recording::debug!(
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
    platform::recording::info!(
        "🔗 新 WebSocket 客户端连接: {} (IP: {:?})",
        client_id,
        client_ip
    );

    // 分离读写流
    let (mut ws_sender, mut ws_receiver) = websocket.split();

    // 创建专用的发送通道用于点对点消息
    let (direct_tx, mut direct_rx) = tokio::sync::mpsc::unbounded_channel();

    // 注册客户端（包含专用发送器）
    let (actor_id, credential) = match url_identity {
        Some(identity) => (Some(identity.0), Some(identity.1)),
        None => (None, None),
    };
    let actor_id_for_registry = actor_id.clone();
    {
        let mut clients_guard = server.clients.write().await;

        // 移除已有相同 actor 的连接（避免 stale 映射）。
        if let Some(ref aid) = actor_id {
            let mut to_remove = Vec::new();
            for (cid, conn) in clients_guard.iter() {
                if conn.actor_id.as_ref() == Some(aid) {
                    to_remove.push(cid.clone());
                }
            }
            for cid in to_remove {
                clients_guard.remove(&cid);
                platform::recording::info!("🧹 Removed stale client {} for actor {:?}", cid, aid);
            }
        }

        clients_guard.insert(
            client_id.clone(),
            ClientConnection {
                id: client_id.clone(),
                actor_id,
                credential,
                direct_sender: direct_tx,
                client_ip,
                webrtc_role: webrtc_role.clone(),
            },
        );
    }

    // Register actor in service registry for discovery/routing (only when URL identity is provided)
    if let Some(actor_id_for_registry) = actor_id_for_registry {
        {
            // Load pending service_spec and ws_address from DB (written by AIS during registration)
            let (service_spec, ws_address) = {
                let db = platform::storage::db::get_database();
                let pool = db.get_pool();
                let row: Option<(Option<Vec<u8>>, Option<String>)> = sqlx::query_as(
                    "SELECT service_spec_blob, ws_address FROM pending_registration \
                     WHERE serial_number = ? AND realm_id = ?",
                )
                .bind(actor_id_for_registry.serial_number as i64)
                .bind(actor_id_for_registry.realm.realm_id as i64)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();
                match row {
                    Some((blob, ws)) => (
                        blob.and_then(|b| actr_protocol::ServiceSpec::decode(b.as_slice()).ok()),
                        ws,
                    ),
                    None => (None, None),
                }
            };

            let service_name = format!(
                "{}:{}",
                actor_id_for_registry.r#type.manufacturer, actor_id_for_registry.r#type.name
            );
            let mut registry = server.service_registry.write().await;
            if let Err(e) = registry.register_service_full(
                actor_id_for_registry.clone(),
                service_name,
                vec![],
                None,
                service_spec,
                None,
                ws_address,
            ) {
                platform::recording::warn!("Failed to register actor in service registry: {}", e);
            }
        }
        // Notify presence subscribers about the new actor
        {
            let actor_id_for_presence = actor_id_for_registry.clone();
            let presence = server.presence_manager.read().await;
            let subscribers = presence
                .get_subscribers_with_acl(&actor_id_for_presence)
                .await;
            drop(presence);

            for subscriber_id in &subscribers {
                // Find the client_id for this subscriber
                let sub_client_id = {
                    let idx = server.actor_id_index.read().await;
                    idx.get(subscriber_id).cloned()
                };
                if let Some(sub_cid) = sub_client_id {
                    let event = actr_protocol::ActrUpEvent {
                        actor_id: actor_id_for_presence.clone(),
                    };
                    let flow =
                        signaling_envelope::Flow::ServerToActr(actr_protocol::SignalingToActr {
                            target: subscriber_id.clone(),
                            payload: Some(actr_protocol::signaling_to_actr::Payload::ActrUpEvent(
                                event,
                            )),
                        });
                    let envelope = server.create_envelope(flow, None);
                    if let Err(e) = send_envelope_to_client(&sub_cid, envelope, &server).await {
                        platform::recording::warn!(
                            "Failed to send ActrUp notification to {}: {}",
                            subscriber_id.serial_number,
                            e
                        );
                    }
                }
            }
        }
        {
            let mut actor_index = server.actor_id_index.write().await;
            actor_index.insert(actor_id_for_registry, client_id.clone());
        }
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
                        platform::recording::error!("处理客户端信令错误: {}", e);
                        break;
                    }
                }
                Ok(WsMessage::Close(_)) => {
                    platform::recording::info!("客户端 {} 主动断开连接", client_id_for_receive);
                    break;
                }
                Err(e) => {
                    platform::recording::error!("WebSocket 错误: {}", e);
                    break;
                }
                _ => {
                    platform::recording::warn!("收到非 Binary 消息，忽略");
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
    platform::recording::info!("🔌 客户端 {} 已断开连接", client_id);

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
        platform::recording::warn!("🚫 连接 {} 消息速率限制触发: {}", client_id, e);
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
        platform::recording::debug!("📨 收到信令消息 envelope_id={}", envelope.envelope_id);

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
                platform::recording::error!(
                    "收到 envelope 错误: code={}, message={}",
                    error.code,
                    error.message
                );
                Ok(())
            }
            _ => {
                platform::recording::warn!("未知的信令流向");
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
        Some(peer_to_signaling::Payload::RegisterRequest(_)) => {
            // 注册功能已迁移到 AIS HTTP，信令不再中转注册请求
            platform::recording::warn!(
                "⚠️  RegisterRequest rejected: use /ais/register HTTP endpoint instead"
            );
            send_register_error(
                client_id,
                410,
                "Registration via signaling is no longer supported; use /ais/register HTTP endpoint",
                server,
                request_envelope_id,
            )
            .await?;
        }
        None => {
            platform::recording::warn!("PeerToSignaling 消息缺少 payload");
        }
    }
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
            version: String::new(),
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

    platform::recording::info!("📬 处理来自 Actor {} 的消息", source.serial_number);

    // 验证 Realm 是否存在、未过期、状态正常
    let realm_id = source.realm.realm_id;
    if let Err(e) = RealmEntity::validate_realm(realm_id).await {
        platform::recording::warn!("⚠️  Actor {} realm 验证失败: {}", source.serial_number, e);
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
            platform::recording::warn!(
                "⚠️  Actor {} credential 验证失败: {}",
                source.serial_number,
                e
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
        Some(actr_to_signaling::Payload::CredentialUpdateRequest(_)) => {
            // Credential 刷新已迁移到 AIS HTTP，信令不再中转
            platform::recording::warn!(
                "⚠️  CredentialUpdateRequest rejected: use /ais/register HTTP endpoint instead"
            );
            send_error_response(
                client_id,
                &source,
                410,
                "Credential refresh via signaling is no longer supported; use /ais/register HTTP endpoint",
                server,
                Some(request_envelope_id),
            )
            .await?;
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
        Some(actr_to_signaling::Payload::GetSigningKeyRequest(req)) => {
            handle_get_signing_key_request(source, req, client_id, server, request_envelope_id)
                .await?;
        }
        Some(actr_to_signaling::Payload::Error(error)) => {
            platform::recording::error!(
                "收到客户端错误报告 (Actor {}): code={}, message={}",
                source.serial_number,
                error.code,
                error.message
            );
        }
        None => {
            platform::recording::warn!("ActrToSignaling 消息缺少 payload");
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
    platform::recording::info!(
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
        platform::recording::warn!(
            "更新 Actor {} 负载指标失败: {}, 尝试从数据库恢复服务",
            source.serial_number,
            e
        );

        // 尝试从数据库恢复服务
        match registry.restore_service_from_storage(&source).await {
            Ok(true) => {
                platform::recording::info!(
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
                    platform::recording::error!(
                        "❌ 从数据库恢复后仍无法更新 Actor {} 的负载指标: {}",
                        source.serial_number,
                        e2
                    );
                } else {
                    platform::recording::info!(
                        "✅ Actor {} 服务恢复后负载指标更新成功",
                        source.serial_number
                    );
                }
            }
            Ok(false) => {
                platform::recording::warn!(
                    "⚠️  数据库中未找到 Actor {} 的服务信息 (可能已过期或从未注册)",
                    source.serial_number
                );
                // TODO: 可选 - 在 Pong 响应中添加警告，提示客户端重新注册
            }
            Err(e) => {
                platform::recording::error!(
                    "❌ 从数据库恢复 Actor {} 的服务失败: {}",
                    source.serial_number,
                    e
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
        platform::recording::warn!(
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
    platform::recording::info!(
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
#[allow(dead_code)]
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
            platform::recording::warn!(
                "⚠️  Actor {} 缺少 client_id 索引，可能尚未注册或已清理",
                format_actor_id(actor_id)
            );
            return Err("client_id not found for actor_id".into());
        }
    };

    let exists = server.clients.read().await.contains_key(&client_id);
    if !exists {
        platform::recording::warn!(
            "⚠️  Actor {} 索引指向不存在的客户端 {}，索引可能已过期",
            format_actor_id(actor_id),
            client_id
        );
        return Err("actor_id_index stale for actor_id".into());
    }

    Ok(client_id)
}

#[allow(dead_code)]
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
        platform::recording::warn!("⚠️  Actor {} realm 验证失败: {}", source.serial_number, e);
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

    platform::recording::info!(
        "🔀 中继信令: {} -> {}",
        source.serial_number,
        target.serial_number
    );

    platform::recording::debug!("handle_actr_relay: relay={:?}", relay);

    // ACL check: can source relay to target?
    use platform::realm::acl::ActorAcl;
    let source_realm = source.realm.realm_id;
    let target_realm = target.realm.realm_id;

    // ACL 统一判定：同 realm / 跨 realm 都走同一规则查询
    let source_type = type_key(&source.r#type);
    let target_type = type_key(&target.r#type);

    let can_relay = ActorAcl::can_discover(source_realm, target_realm, &source_type, &target_type)
        .await
        .unwrap_or(false);

    if !can_relay {
        platform::recording::warn!(
            "⚠️  ACL denied relay: {} -> {}",
            source.serial_number,
            target.serial_number
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
    if let Err(e) = AIdCredentialValidator::check(&relay.credential, source.realm.realm_id)
        .await
        .map(|(claims, _)| claims)
    {
        platform::recording::warn!(
            "⚠️  Actor {} credential 验证失败: {}",
            source.serial_number,
            e
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

    // Role negotiation: server decides offerer/answerer and notifies both parties
    if let Some(actr_relay::Payload::RoleNegotiation(RoleNegotiation { from, to, .. })) =
        relay.payload.clone()
    {
        let clients_guard = server.clients.read().await;

        // 使用 determine_webrtc_role 函数确定角色
        let is_offerer = determine_webrtc_role(&from, &to, &clients_guard);

        drop(clients_guard);

        // 发送给 from 的 RoleAssignment
        let new_relay = ActrRelay {
            // source: peer actor (对端)，target: 该 assignment 的接收方
            source: from.clone(),
            credential: relay.credential.clone(),
            target: to.clone(),
            payload: Some(actr_relay::Payload::RoleAssignment(RoleAssignment {
                is_offerer,
                remote_fixed: None,
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

        // 发送给 to 的 RoleAssignment
        let new_relay = ActrRelay {
            // source: peer actor (对端)，target: 该 assignment 的接收方
            source: from.clone(),
            credential: relay.credential.clone(),
            target: to.clone(),
            payload: Some(actr_relay::Payload::RoleAssignment(RoleAssignment {
                is_offerer: !is_offerer,
                remote_fixed: None,
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

        platform::recording::info!("✅ 信令中继成功");
    } else {
        platform::recording::warn!("⚠️ 未找到目标 Actor {}", target.serial_number);
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

    platform::recording::info!(
        "⚖️ 角色协商完成: {} (role={:?}) -> {} (role={:?}), is_offerer={}",
        from.serial_number,
        from_role,
        to.serial_number,
        to_role,
        is_offerer
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
        platform::recording::debug!(
            "send_role_assignment: 发送 envelope 到客户端 {:?}",
            client.actor_id
        );
        client
            .direct_sender
            .send(WsMessage::Binary(buf.into()))
            .map_err(|e| e.into())
    } else {
        platform::recording::warn!(
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
                platform::recording::info!("✅ 成功发送 envelope 到客户端 {}", client_id);
                Ok(())
            }
            Err(e) => {
                platform::recording::error!("❌ 发送失败: {}", e);
                Err(format!("发送失败: {e}").into())
            }
        }
    } else {
        platform::recording::warn!("⚠️ 未找到客户端 {}", client_id);
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
            platform::recording::info!("🧹 清理 Actor {} 的连接", actor_id.serial_number);

            // Remove all services for this Actor from the ServiceRegistry to avoid stale ghost instances
            server
                .service_registry
                .write()
                .await
                .unregister_actor(&actor_id);

            let mut actor_index = server.actor_id_index.write().await;
            match actor_index.remove(&actor_id) {
                Some(mapped_client) if mapped_client != client_id => platform::recording::warn!(
                    "⚠️  Actor {} 索引指向意外客户端 {}，已移除",
                    actor_id.serial_number,
                    mapped_client
                ),
                None => platform::recording::warn!(
                    "⚠️  Actor {} 清理时未找到索引条目",
                    actor_id.serial_number
                ),
                _ => {}
            }
        }

        // 移除消息速率限制器
        if let Some(ref limiter) = server.message_rate_limiter {
            limiter.remove_connection(client_id).await;
        }
    }
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
    platform::recording::info!(
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
    use platform::realm::acl::ActorAcl;
    let source_realm = source.realm.realm_id;
    let source_type = type_key(&source.r#type);

    let mut acl_filtered_services = Vec::new();

    // ACL always enabled: filter services based on ACL rules
    for service in services {
        let target_realm = service.actor_id.realm.realm_id;
        let target_type = type_key(&service.actor_id.r#type);

        match ActorAcl::can_discover(source_realm, target_realm, &source_type, &target_type).await {
            Ok(true) => acl_filtered_services.push(service),
            Ok(false) => {
                platform::recording::debug!(
                    "ACL denied discovery: {} (realm {}) cannot discover {} (realm {})",
                    source.serial_number,
                    source_realm,
                    service.actor_id.serial_number,
                    target_realm
                );
            }
            Err(e) => {
                platform::recording::warn!(
                    "ACL check failed for {} (realm {}) -> {} (realm {}): {}",
                    source.serial_number,
                    source_realm,
                    service.actor_id.serial_number,
                    target_realm,
                    e
                );
            }
        }
    }
    platform::recording::info!(
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

    platform::recording::info!(
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
    platform::recording::info!(
        "🎯 处理 Actor {} 的 RouteCandidates 请求: target_type={}/{}",
        source.serial_number,
        req.target_type.manufacturer,
        req.target_type.name,
    );

    // 从 ServiceRegistry 查询所有匹配 target_type 的实例
    let registry = server.service_registry.read().await;
    let candidates = registry.find_by_actr_type(&req.target_type);
    drop(registry);

    let total_candidates = candidates.len();

    if candidates.is_empty() {
        platform::recording::info!(
            "⚠️  未找到 {}/{} 类型的服务实例",
            req.target_type.manufacturer,
            req.target_type.name
        );
    } else {
        platform::recording::info!(
            "📋 找到 {} 个 {}/{} 类型的候选实例",
            total_candidates,
            req.target_type.manufacturer,
            req.target_type.name
        );
    }

    // Apply ACL filtering
    use platform::realm::acl::ActorAcl;
    let source_realm = source.realm.realm_id;
    let source_type = type_key(&source.r#type);

    let mut acl_filtered_candidates = Vec::new();
    for candidate in candidates {
        let target_realm = candidate.actor_id.realm.realm_id;
        let target_type = type_key(&candidate.actor_id.r#type);

        match ActorAcl::can_discover(source_realm, target_realm, &source_type, &target_type).await {
            Ok(true) => acl_filtered_candidates.push(candidate),
            Ok(false) => {
                platform::recording::debug!(
                    "ACL denied route candidate: {} (realm {}) cannot access {} (realm {})",
                    source.serial_number,
                    source_realm,
                    candidate.actor_id.serial_number,
                    target_realm
                );
            }
            Err(e) => {
                platform::recording::warn!(
                    "ACL check failed for {} (realm {}) -> {} (realm {}): {}",
                    source.serial_number,
                    source_realm,
                    candidate.actor_id.serial_number,
                    target_realm,
                    e
                );
            }
        }
    }
    platform::recording::info!(
        "ACL filtering for route candidates: {} -> {} candidates",
        total_candidates,
        acl_filtered_candidates.len()
    );

    // 从请求中提取客户端位置（如果提供）
    let client_location = req.client_location.as_ref().and_then(|loc| {
        if let (Some(lat), Some(lon)) = (loc.latitude, loc.longitude) {
            Some((lat, lon))
        } else {
            None
        }
    });

    // Extract ws_address before consuming acl_filtered_candidates
    let ws_address_by_id: std::collections::HashMap<ActrId, Option<String>> =
        acl_filtered_candidates
            .iter()
            .map(|c| (c.actor_id.clone(), c.ws_address.clone()))
            .collect();

    let ranked_actor_ids = LoadBalancer::rank_candidates(
        acl_filtered_candidates,
        req.criteria.as_ref(),
        req.client_fingerprint.trim(),
        Some(client_id),
        client_location,
    );

    platform::recording::info!(
        "✅ 为 Actor {} 返回 {} 个候选",
        source.serial_number,
        ranked_actor_ids.len(),
    );

    let ws_address_map: Vec<actr_protocol::WsAddressEntry> = ranked_actor_ids
        .iter()
        .filter_map(|id| {
            ws_address_by_id.get(id).map(|ws| actr_protocol::WsAddressEntry {
                candidate_id: id.clone(),
                ws_address: ws.clone(),
            })
        })
        .collect();

    let response = actr_protocol::RouteCandidatesResponse {
        result: Some(actr_protocol::route_candidates_response::Result::Success(
            actr_protocol::route_candidates_response::RouteCandidatesOk {
                candidates: ranked_actor_ids,
                ws_address_map,
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
    platform::recording::info!(
        "Handle GetServiceSpec request for Actor {} name={}",
        source.to_string_repr(),
        service_name
    );

    // Find matching ServiceSpec in ServiceRegistry by spec name
    let result = server
        .service_registry
        .read()
        .await
        .discover_by_spec_name(service_name)
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

/// Handle GetSigningKey request — proxies key lookup via local KeyCache
async fn handle_get_signing_key_request(
    source: ActrId,
    req: actr_protocol::GetSigningKeyRequest,
    client_id: &str,
    server: &SignalingServerHandle,
    request_envelope_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let key_id = req.key_id;
    platform::recording::debug!(
        "Actor {} requested signing key: key_id={}",
        source.serial_number,
        key_id
    );

    let response = match AIdCredentialValidator::get_key_bytes(key_id).await {
        Ok(Some(pubkey_bytes)) => actr_protocol::GetSigningKeyResponse {
            key_id,
            pubkey: pubkey_bytes.into(),
        },
        Ok(None) => {
            platform::recording::warn!(
                "GetSigningKeyRequest: key_id={} not found in cache",
                key_id
            );
            send_error_response(
                client_id,
                &source,
                404,
                &format!("signing key not found: key_id={key_id}"),
                server,
                Some(request_envelope_id),
            )
            .await?;
            return Ok(());
        }
        Err(e) => {
            platform::recording::error!(
                "GetSigningKeyRequest: key_id={} lookup error: {}",
                key_id,
                e
            );
            send_error_response(
                client_id,
                &source,
                500,
                "internal error looking up signing key",
                server,
                Some(request_envelope_id),
            )
            .await?;
            return Ok(());
        }
    };

    let flow = signaling_envelope::Flow::ServerToActr(SignalingToActr {
        target: source,
        payload: Some(signaling_to_actr::Payload::GetSigningKeyResponse(response)),
    });
    let envelope = server.create_envelope(flow, Some(request_envelope_id));
    send_envelope_to_client(client_id, envelope, server).await?;

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
    platform::recording::info!(
        "📢 Actor {} 订阅服务上线事件: target_type={}/{}",
        source.serial_number,
        req.target_type.manufacturer,
        req.target_type.name
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
    platform::recording::info!(
        "🔕 Actor {} 取消订阅服务上线事件: target_type={}/{}",
        source.serial_number,
        req.target_type.manufacturer,
        req.target_type.name
    );

    // 从 PresenceManager 移除订阅
    let mut presence = server.presence_manager.write().await;
    let removed = presence.unsubscribe(&source, &req.target_type);
    drop(presence);

    if !removed {
        platform::recording::warn!(
            "Actor {} 未订阅过 {}/{}",
            source.serial_number,
            req.target_type.manufacturer,
            req.target_type.name
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
                version: String::new(),
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
