//! KS 客户端包装器
//!
//! 提供统一的 KS 客户端接口，支持 gRPC 客户端（需要 &mut self）

use ks::{GrpcClient, GrpcClientConfig};
use platform::aid::AidError;
use std::sync::Arc;
use tokio::sync::RwLock;

/// KS 客户端包装器（用于 gRPC 客户端）
#[derive(Clone)]
pub struct KsClientWrapper {
    inner: Arc<RwLock<Option<GrpcClient>>>,
    grpc_config: GrpcClientConfig,
}

impl KsClientWrapper {
    /// 创建新的 KS 客户端包装器
    pub fn new(grpc_config: GrpcClientConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
            grpc_config,
        }
    }

    /// 从 KS 申请新的 Ed25519 签名密钥，返回 (key_id, verifying_key_bytes[32], expires_at, tolerance_secs)
    /// 私钥保留在 KS 服务端
    pub async fn generate_signing_key(&self) -> Result<(u32, [u8; 32], u64, u64), ks::KsError> {
        let mut guard = self.inner.write().await;
        if guard.is_none() {
            let client = GrpcClient::new(&self.grpc_config).await?;
            *guard = Some(client);
        }
        guard
            .as_mut()
            .expect("grpc client must exist after lazy init")
            .generate_signing_key()
            .await
    }

    /// 使用 KS 中的密钥对消息进行 Ed25519 签名，返回 64 字节签名
    /// 私钥不离开 KS 服务
    pub async fn sign(&self, key_id: u32, message: &[u8]) -> Result<Vec<u8>, ks::KsError> {
        let mut guard = self.inner.write().await;
        if guard.is_none() {
            let client = GrpcClient::new(&self.grpc_config).await?;
            *guard = Some(client);
        }
        guard
            .as_mut()
            .expect("grpc client must exist after lazy init")
            .sign(key_id, message)
            .await
    }

    /// 获取指定 key_id 的验证公钥（verifying key）
    ///
    /// 返回 (verifying_key_bytes [u8;32], expires_at, tolerance_seconds)
    pub async fn get_verifying_key(
        &self,
        key_id: u32,
    ) -> Result<([u8; 32], u64, u64), ks::KsError> {
        let mut guard = self.inner.write().await;
        if guard.is_none() {
            let client = GrpcClient::new(&self.grpc_config).await?;
            *guard = Some(client);
        }
        guard
            .as_mut()
            .expect("grpc client must exist after lazy init")
            .get_verifying_key(key_id)
            .await
    }

    /// 健康检查
    pub async fn health_check(&self) -> Result<String, ks::KsError> {
        let mut guard = self.inner.write().await;
        if guard.is_none() {
            let client = GrpcClient::new(&self.grpc_config).await?;
            *guard = Some(client);
        }
        guard
            .as_mut()
            .expect("grpc client must exist after lazy init")
            .health_check()
            .await
    }
}

/// 从配置创建 KS 客户端包装器
pub async fn create_ks_client(
    config: &platform::config::ks::KsClientConfig,
    actrix_shared_key: &str,
) -> Result<KsClientWrapper, AidError> {
    let grpc_config = ks::GrpcClientConfig {
        endpoint: config.endpoint.clone(),
        actrix_shared_key: actrix_shared_key.to_string(),
        timeout_seconds: config.timeout_seconds,
        enable_tls: config.enable_tls,
        tls_domain: config.tls_domain.clone(),
        ca_cert: config.ca_cert.clone(),
        client_cert: config.client_cert.clone(),
        client_key: config.client_key.clone(),
    };

    Ok(KsClientWrapper::new(grpc_config))
}
