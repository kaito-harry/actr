//! KS gRPC 服务实现

use crate::{error::KsError, storage::KeyStorage};
use nonce_auth::{CredentialVerifier, NonceError, storage::NonceStorage};
use std::sync::Arc;
use tonic::{Request, Response, Status};

// 导入生成的 protobuf 代码
use actrix_proto::admin::v1::NonceCredential;
use actrix_proto::ks::v1::key_server_server::{KeyServer, KeyServerServer};
use actrix_proto::ks::v1::*;

/// KS gRPC 服务状态
#[derive(Clone)]
pub struct KsGrpcService {
    pub storage: KeyStorage,
    pub nonce_storage: Arc<dyn NonceStorage + Send + Sync>,
    pub psk: String,
    pub tolerance_seconds: u64,
}

impl KsGrpcService {
    /// 创建新的 gRPC 服务实例
    pub fn new<N: NonceStorage + Send + Sync + 'static>(
        storage: KeyStorage,
        nonce_storage: N,
        psk: String,
        tolerance_seconds: u64,
    ) -> Self {
        Self {
            storage,
            nonce_storage: Arc::new(nonce_storage),
            psk,
            tolerance_seconds,
        }
    }

    /// 验证请求的 nonce 凭证
    async fn verify_credential(
        &self,
        credential: &NonceCredential,
        request_payload: &str,
    ) -> Result<(), KsError> {
        // 将 protobuf NonceCredential 转换为 nonce_auth::NonceCredential
        let nonce_credential = nonce_auth::NonceCredential {
            timestamp: credential.timestamp,
            nonce: credential.nonce.clone(),
            signature: credential.signature.clone(),
        };

        let verify_result = CredentialVerifier::new(self.nonce_storage.clone())
            .with_secret(self.psk.as_bytes())
            .verify(&nonce_credential, request_payload.as_bytes())
            .await;

        verify_result.map_err(|e| match e {
            NonceError::DuplicateNonce => KsError::ReplayAttack("Nonce already used".to_string()),
            NonceError::TimestampOutOfWindow => {
                KsError::Authentication("Request timestamp out of range".to_string())
            }
            NonceError::InvalidSignature => {
                KsError::Authentication("Invalid signature".to_string())
            }
            _ => KsError::Internal(format!("Authentication error: {e}")),
        })?;

        Ok(())
    }
}

#[tonic::async_trait]
impl KeyServer for KsGrpcService {
    /// 生成新的 Ed25519 签名密钥对
    async fn generate_signing_key(
        &self,
        request: Request<GenerateSigningKeyRequest>,
    ) -> Result<Response<GenerateSigningKeyResponse>, Status> {
        crate::recording::info!("Received gRPC GenerateSigningKey request");

        let req = request.into_inner();

        let request_data = "generate_signing_key";
        self.verify_credential(&req.credential, request_data)
            .await
            .map_err(|e| Status::unauthenticated(format!("Authentication failed: {e}")))?;

        // 生成 Ed25519 密钥对（私钥存储于后端）
        let key_pair = self
            .storage
            .generate_and_store_key()
            .await
            .map_err(|e| Status::internal(format!("Failed to generate signing key: {e}")))?;

        // 获取密钥记录以获取过期时间
        let key_record = self
            .storage
            .get_key_record(key_pair.key_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get key record: {e}")))?
            .ok_or_else(|| Status::internal("Failed to get key record after creation"))?;

        crate::recording::info!("Generated Ed25519 signing key with key_id: {}", key_pair.key_id);

        let response = GenerateSigningKeyResponse {
            key_id: key_pair.key_id,
            verifying_key: key_pair.verifying_key,
            expires_at: key_record.expires_at,
            tolerance_seconds: self.tolerance_seconds,
        };

        Ok(Response::new(response))
    }

    /// 使用指定密钥对消息进行 Ed25519 签名
    async fn sign(
        &self,
        request: Request<SignRequest>,
    ) -> Result<Response<SignResponse>, Status> {
        let req = request.into_inner();
        let key_id = req.key_id;

        crate::recording::info!("Received gRPC Sign request for key_id: {}", key_id);

        // nonce payload 绑定到特定 key_id，防止重用
        let request_data = format!("sign:{key_id}");
        self.verify_credential(&req.credential, &request_data)
            .await
            .map_err(|e| Status::unauthenticated(format!("Authentication failed: {e}")))?;

        // 检查密钥是否在容忍期内有效
        let key_record = self
            .storage
            .get_key_record(key_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get key record: {e}")))?
            .ok_or_else(|| Status::not_found(format!("Key not found: {key_id}")))?;

        if key_record.expires_at > 0 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if key_record.expires_at + self.tolerance_seconds < now {
                crate::recording::warn!("Key {} has expired beyond tolerance period", key_id);
                return Err(Status::not_found(format!("Key {key_id} has expired")));
            }

            if key_record.expires_at < now {
                crate::recording::warn!("Key {} is in tolerance period, signing allowed", key_id);
            }
        }

        // 在后端内部完成签名，私钥不离开 KS
        let signature = self
            .storage
            .sign(key_id, &req.message)
            .await
            .map_err(|e| match e {
                crate::error::KsError::NotFound(_) => {
                    Status::not_found(format!("Key not found: {key_id}"))
                }
                other => Status::internal(format!("Sign failed: {other}")),
            })?;

        crate::recording::info!("Signed message for key_id: {}", key_id);

        Ok(Response::new(SignResponse { signature }))
    }

    /// 获取指定 key_id 的验证公钥
    async fn get_verifying_key(
        &self,
        request: Request<GetVerifyingKeyRequest>,
    ) -> Result<Response<GetVerifyingKeyResponse>, Status> {
        let req = request.into_inner();
        let key_id = req.key_id;

        crate::recording::info!("Received gRPC GetVerifyingKey request for key_id: {}", key_id);

        let request_data = format!("get_verifying_key:{key_id}");
        self.verify_credential(&req.credential, &request_data)
            .await
            .map_err(|e| Status::unauthenticated(format!("Authentication failed: {e}")))?;

        let key_record = self
            .storage
            .get_key_record(key_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to get key record: {e}")))?
            .ok_or_else(|| Status::not_found(format!("Key not found: {key_id}")))?;

        // 检查是否超过容忍期
        if key_record.expires_at > 0 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            if key_record.expires_at + self.tolerance_seconds < now {
                crate::recording::warn!("Key {} has expired beyond tolerance period", key_id);
                return Err(Status::not_found(format!("Key {key_id} has expired")));
            }
        }

        crate::recording::debug!("Returning verifying key for key_id: {}", key_id);

        Ok(Response::new(GetVerifyingKeyResponse {
            key_id,
            verifying_key: key_record.public_key,
            expires_at: key_record.expires_at,
            tolerance_seconds: self.tolerance_seconds,
        }))
    }

    /// 健康检查
    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        crate::recording::debug!("gRPC health check requested");
        let req = request.into_inner();

        // 与其他方法一致，health 也要求 nonce-auth
        self.verify_credential(&req.credential, "health_check")
            .await
            .map_err(|e| Status::unauthenticated(format!("Authentication failed: {e}")))?;

        let key_count = self
            .storage
            .get_key_count()
            .await
            .map_err(|e| Status::internal(format!("Failed to get key count: {e}")))?;

        let response = HealthCheckResponse {
            status: "healthy".to_string(),
            service: "ks".to_string(),
            backend: self.storage.backend_name().to_string(),
            key_count,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        Ok(Response::new(response))
    }
}

/// 创建 gRPC 服务器
pub fn create_grpc_service<N: NonceStorage + Send + Sync + 'static>(
    storage: KeyStorage,
    nonce_storage: N,
    psk: String,
    tolerance_seconds: u64,
) -> KeyServerServer<KsGrpcService> {
    let service = KsGrpcService::new(storage, nonce_storage, psk, tolerance_seconds);
    KeyServerServer::new(service)
}
