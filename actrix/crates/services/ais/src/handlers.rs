//! AIS (Actor Identity Service) HTTP Handler

use crate::{issuer::AIdIssuer, ratelimit::ip_rate_limiter};
use actr_protocol::{ErrorResponse, RegisterRequest, RegisterResponse, register_response};
use axum::{Router, body::Bytes, extract::State, http::HeaderMap, response::Json, routing::post};
use platform::aid::AidError;
use platform::monitoring::ServiceCounters;
use platform::realm::{
    REALM_SECRET_HEADER, Realm as RealmEntity, RealmSecretCheck, acl::ActorAcl, verify_realm_secret,
};
use prost::Message;
use serde_json::{Value, json};
use std::sync::Arc;

/// AIS 服务状态
#[derive(Clone)]
pub struct AISState {
    pub issuer: Arc<AIdIssuer>,
    /// Service-level counters for metrics collection.
    pub counters: Option<Arc<ServiceCounters>>,
}

impl AISState {
    pub fn new(issuer: AIdIssuer) -> Self {
        Self {
            issuer: Arc::new(issuer),
            counters: None,
        }
    }

    pub fn with_counters(mut self, counters: Arc<ServiceCounters>) -> Self {
        self.counters = Some(counters);
        self
    }
}

/// 创建 AIS 服务的路由
///
/// 应用限流中间件：
/// - IP 级别：100 req/min（防止单个 IP 的 DoS 攻击）
pub fn create_router(state: AISState) -> Router {
    Router::new()
        .route("/register", post(register_actr))
        .route("/health", axum::routing::get(health_check))
        .route("/rotate-key", post(rotate_key))
        .route("/current-key", axum::routing::get(get_current_key))
        .layer(ip_rate_limiter())
        .with_state(state)
}

/// ActrId 注册处理器 - 严格按照 proto 定义返回 RegisterResponse
/// RegisterRequest -> RegisterResponse
async fn register_actr(State(state): State<AISState>, headers: HeaderMap, body: Bytes) -> Bytes {
    let start = std::time::Instant::now();

    // 解析 protobuf 请求
    let request = match RegisterRequest::decode(body) {
        Ok(req) => req,
        Err(err) => {
            platform::recording::error!("Failed to decode protobuf request: {}", err);
            let error_result = RegisterResponse {
                result: Some(register_response::Result::Error(ErrorResponse {
                    code: 400, // Bad Request
                    message: format!("Invalid protobuf: {err}"),
                })),
            };
            return encode_result(error_result);
        }
    };

    platform::recording::debug!(
        "Received register request for realm {}, type {}:{}",
        request.realm.realm_id,
        request.actr_type.manufacturer,
        request.actr_type.name
    );

    // 验证 Realm 是否存在、状态正常、未过期
    if let Err(e) = RealmEntity::validate_realm(request.realm.realm_id).await {
        let error_result = RegisterResponse {
            result: Some(register_response::Result::Error(ErrorResponse {
                code: 403,
                message: format!("Realm validation failed: {e}"),
            })),
        };
        return encode_result(error_result);
    }

    // 校验 realm secret（历史 realm 未配置 secret 时兼容放行）
    let provided_secret = headers
        .get(REALM_SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty());

    match verify_realm_secret(request.realm.realm_id, provided_secret).await {
        Ok(RealmSecretCheck::NotConfigured)
        | Ok(RealmSecretCheck::ValidCurrent)
        | Ok(RealmSecretCheck::ValidPrevious) => {}
        Ok(RealmSecretCheck::MissingRequired) => {
            let error_result = RegisterResponse {
                result: Some(register_response::Result::Error(ErrorResponse {
                    code: 403,
                    message: "Realm secret required".to_string(),
                })),
            };
            return encode_result(error_result);
        }
        Ok(RealmSecretCheck::Invalid) => {
            let error_result = RegisterResponse {
                result: Some(register_response::Result::Error(ErrorResponse {
                    code: 403,
                    message: "Invalid realm secret".to_string(),
                })),
            };
            return encode_result(error_result);
        }
        Err(e) => {
            platform::recording::error!(
                "Failed to verify realm secret for realm {}: {}",
                request.realm.realm_id,
                e
            );
            let error_result = RegisterResponse {
                result: Some(register_response::Result::Error(ErrorResponse {
                    code: 500,
                    message: "Internal error while verifying realm secret".to_string(),
                })),
            };
            return encode_result(error_result);
        }
    }

    // 调用 issuer 签发 credential
    let result = match state.issuer.issue_credential(&request).await {
        Ok(response) => {
            if let Some(register_response::Result::Success(ref register_ok)) = response.result {
                platform::recording::debug!(
                    "Successfully registered ActrId: realm={}, serial_number={}, type={}:{}",
                    register_ok.actr_id.realm.realm_id,
                    register_ok.actr_id.serial_number,
                    register_ok.actr_id.r#type.manufacturer,
                    register_ok.actr_id.r#type.name
                );

                // Persist ACL rules to database
                if let Some(ref acl) = request.acl {
                    let realm_id = register_ok.actr_id.realm.realm_id;
                    let my_type = format!(
                        "{}:{}",
                        register_ok.actr_id.r#type.manufacturer, register_ok.actr_id.r#type.name
                    );

                    for rule in &acl.rules {
                        let permission =
                            rule.permission == actr_protocol::acl_rule::Permission::Allow as i32;

                        let from_type =
                            format!("{}:{}", rule.from_type.manufacturer, rule.from_type.name);

                        let source_realm_id = match &rule.source_realm {
                            Some(actr_protocol::acl_rule::SourceRealm::RealmId(id)) => Some(*id),
                            _ => None, // AnyRealm or unset: no specific source realm
                        };

                        let mut actor_acl = if let Some(src) = source_realm_id {
                            ActorAcl::new_with_source_realm(
                                realm_id,
                                Some(src),
                                from_type.clone(),
                                my_type.clone(),
                                permission,
                            )
                        } else {
                            ActorAcl::new(
                                realm_id,
                                from_type.clone(),
                                my_type.clone(),
                                permission,
                            )
                        };

                        if let Err(e) = actor_acl.save().await {
                            platform::recording::warn!(
                                "Failed to save ACL rule ({} -> {}): {}",
                                from_type,
                                my_type,
                                e
                            );
                        }
                    }
                }

                // Store pending registration (service_spec) for signaling to pick up
                if request.service_spec.is_some() || request.ws_address.is_some() {
                    let serial = register_ok.actr_id.serial_number;
                    let realm = register_ok.actr_id.realm.realm_id;
                    let spec_blob = request
                        .service_spec
                        .as_ref()
                        .map(prost::Message::encode_to_vec);
                    let ws_address = request.ws_address.clone();
                    let db = platform::storage::db::get_database();
                    let pool = db.get_pool();
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    let _ = sqlx::query(
                        "INSERT OR REPLACE INTO pending_registration \
                         (serial_number, realm_id, service_spec_blob, ws_address, created_at) \
                         VALUES (?, ?, ?, ?, ?)",
                    )
                    .bind(serial as i64)
                    .bind(realm as i64)
                    .bind(spec_blob)
                    .bind(ws_address)
                    .bind(now)
                    .execute(pool)
                    .await;
                }
            }
            if let Some(ref ctr) = state.counters {
                ctr.record_request(true, start.elapsed().as_secs_f64() * 1000.0)
                    .await;
            }
            response
        }
        Err(err) => {
            platform::recording::error!("Failed to register ActrId: {}", err);
            if let Some(ref ctr) = state.counters {
                ctr.record_request(false, start.elapsed().as_secs_f64() * 1000.0)
                    .await;
            }
            RegisterResponse {
                result: Some(register_response::Result::Error(
                    aid_error_to_error_response(err),
                )),
            }
        }
    };

    encode_result(result)
}

/// 健康检查端点
///
/// 执行以下检查：
/// 1. 数据库连接是否正常
/// 2. KS 服务是否可访问
/// 3. 密钥缓存是否有效
async fn health_check(State(state): State<AISState>) -> Json<Value> {
    let mut checks = json!({
        "service": "ais",
        "version": env!("CARGO_PKG_VERSION"),
        "status": "healthy"
    });

    // 检查数据库连接
    let db_status = match state.issuer.check_database_health().await {
        Ok(()) => "ok",
        Err(e) => {
            platform::recording::error!("Database health check failed: {}", e);
            checks["status"] = json!("degraded");
            "failed"
        }
    };
    checks["database"] = json!(db_status);

    // 检查 KS 服务连通性
    let ks_status = match state.issuer.check_ks_health().await {
        Ok(()) => "ok",
        Err(e) => {
            platform::recording::error!("KS health check failed: {}", e);
            checks["status"] = json!("degraded");
            "failed"
        }
    };
    checks["ks_service"] = json!(ks_status);

    // 检查密钥缓存状态
    let cache_status = match state.issuer.check_key_cache_health().await {
        Ok(info) => json!({"status": "ok", "key_id": info.key_id, "expires_in": info.expires_in}),
        Err(e) => {
            platform::recording::error!("Key cache health check failed: {}", e);
            checks["status"] = json!("degraded");
            json!({"status": "failed", "error": e.to_string()})
        }
    };
    checks["key_cache"] = cache_status;

    Json(checks)
}

/// 手动触发密钥轮替
///
/// 立即从 KS 生成新密钥并更新缓存
/// 返回新的 key_id
async fn rotate_key(State(state): State<AISState>) -> Json<Value> {
    match state.issuer.rotate_key().await {
        Ok(new_key_id) => Json(json!({
            "status": "success",
            "message": "Key rotated successfully",
            "new_key_id": new_key_id
        })),
        Err(e) => {
            platform::recording::error!("Failed to rotate key: {}", e);
            Json(json!({
                "status": "error",
                "message": format!("Key rotation failed: {}", e)
            }))
        }
    }
}

/// 获取当前使用的密钥 ID
///
/// 用于监控和调试
async fn get_current_key(State(state): State<AISState>) -> Json<Value> {
    match state.issuer.get_current_key_id().await {
        Ok(key_id) => Json(json!({
            "status": "success",
            "key_id": key_id
        })),
        Err(e) => {
            platform::recording::error!("Failed to get current key: {}", e);
            Json(json!({
                "status": "error",
                "message": format!("Failed to get key: {}", e)
            }))
        }
    }
}

/// 编码 RegisterResponse 为 protobuf 字节
fn encode_result(result: RegisterResponse) -> Bytes {
    let mut buf = Vec::new();
    if let Err(err) = result.encode(&mut buf) {
        platform::recording::error!("Failed to encode RegisterResponse: {}", err);
        // 返回一个编码错误的 ErrorResponse
        let error_result = RegisterResponse {
            result: Some(register_response::Result::Error(ErrorResponse {
                code: 500,
                message: format!("Failed to encode response: {err}"),
            })),
        };
        let mut fallback_buf = Vec::new();
        let _ = error_result.encode(&mut fallback_buf);
        return Bytes::from(fallback_buf);
    }
    Bytes::from(buf)
}

/// 将 AidError 转换为 proto ErrorResponse
///
/// 错误码映射策略：
/// - 4xx: 客户端错误（格式、过期、验证失败）
/// - 5xx: 服务端错误（生成失败、内部错误）
fn aid_error_to_error_response(err: AidError) -> ErrorResponse {
    let code = match &err {
        // 客户端错误 (4xx)
        AidError::InvalidFormat => 400,
        AidError::InvalidPrefix => 400,
        AidError::EmptyId => 400,
        AidError::InvalidTimestamp(_) => 400,
        AidError::Base64DecodeError(_) => 400,
        AidError::HexDecodeError(_) => 400,
        AidError::Expired => 401,
        AidError::RealmError(_) => 403, // Forbidden

        // 服务端错误 (5xx)
        AidError::GenerationFailed(msg) => {
            // 如果是 KS 不可用，返回 503 (Service Unavailable)
            if msg.contains("KS unavailable") || msg.contains("KS service") {
                503
            } else {
                500
            }
        }
        AidError::InvalidSignature(_) => 500,
        AidError::DecodeFailure(_) => 500,
    };

    ErrorResponse {
        code,
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::{AIdCredential, ActrType, Realm};
    use prost::bytes::Bytes as ProstBytes;

    #[test]
    fn test_protobuf_request_encoding_decoding() {
        // 测试完整的 protobuf 请求编解码
        let actr_type = ActrType {
            manufacturer: "apple".to_string(),
            name: "iPhone15".to_string(),
            version: None,
        };

        let realm = Realm { realm_id: 12345 };

        let request = RegisterRequest {
            actr_type,
            realm,
            service: None,
            service_spec: None,
            acl: None,
            ws_address: None,
        };

        // 编码
        let mut buf = Vec::new();
        request.encode(&mut buf).unwrap();

        // 解码
        let decoded_request = RegisterRequest::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded_request.realm.realm_id, 12345);
        assert_eq!(decoded_request.actr_type.manufacturer, "apple");
        assert_eq!(decoded_request.actr_type.name, "iPhone15");
    }

    #[test]
    fn test_protobuf_minimal_request() {
        // 测试最小字段的 protobuf 请求
        let request = RegisterRequest {
            actr_type: ActrType {
                manufacturer: "test".to_string(),
                name: "actor".to_string(),
                version: None,
            },
            realm: Realm { realm_id: 456 },
            service: None,
            service_spec: None,
            acl: None,
            ws_address: None,
        };

        // 编码解码循环
        let mut buf = Vec::new();
        request.encode(&mut buf).unwrap();
        let decoded_request = RegisterRequest::decode(buf.as_slice()).unwrap();

        assert_eq!(decoded_request.realm.realm_id, 456);
        assert_eq!(decoded_request.actr_type.manufacturer, "test");
        assert_eq!(decoded_request.actr_type.name, "actor");
    }

    #[test]
    fn test_register_response_success() {
        use actr_protocol::{ActrId, register_response::RegisterOk};
        use prost_types::Timestamp;

        // 测试成功的 RegisterResponse
        let register_ok = RegisterOk {
            actr_id: ActrId {
                realm: Realm { realm_id: 1 },
                serial_number: 123456,
                r#type: ActrType {
                    manufacturer: "test".to_string(),
                    name: "actor".to_string(),
                    version: None,
                },
            },
            credential: AIdCredential {
                key_id: 42,
                claims: ProstBytes::from(vec![1, 2, 3]),
                signature: ProstBytes::from(vec![0u8; 64]),
            },
            turn_credential: actr_protocol::TurnCredential::default(),
            signing_pubkey: ProstBytes::from(vec![0u8; 32]),
            signing_key_id: 42,
            credential_expires_at: Some(Timestamp {
                seconds: 1234567890,
                nanos: 0,
            }),
            signaling_heartbeat_interval_secs: 30,
        };

        let response = RegisterResponse {
            result: Some(register_response::Result::Success(register_ok)),
        };

        // 编码解码循环
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();
        let decoded_response = RegisterResponse::decode(buf.as_slice()).unwrap();

        assert!(decoded_response.result.is_some());
        if let Some(register_response::Result::Success(resp)) = decoded_response.result {
            assert_eq!(resp.actr_id.realm.realm_id, 1);
            assert_eq!(resp.actr_id.serial_number, 123456);
            assert_eq!(resp.credential.key_id, 42);
            assert_eq!(resp.signaling_heartbeat_interval_secs, 30);
        } else {
            panic!("Expected success result");
        }
    }

    #[test]
    fn test_register_response_error() {
        // 测试错误的 RegisterResponse
        let error = ErrorResponse {
            code: 400,
            message: "Bad request".to_string(),
        };

        let response = RegisterResponse {
            result: Some(register_response::Result::Error(error)),
        };

        // 编码解码循环
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();
        let decoded_response = RegisterResponse::decode(buf.as_slice()).unwrap();

        assert!(decoded_response.result.is_some());
        if let Some(register_response::Result::Error(err)) = decoded_response.result {
            assert_eq!(err.code, 400);
            assert_eq!(err.message, "Bad request");
        } else {
            panic!("Expected error result");
        }
    }
}
