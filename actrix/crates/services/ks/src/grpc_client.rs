//! KS gRPC 客户端

use crate::error::KsError;
use actrix_proto::admin::v1::NonceCredential;
use actrix_proto::ks::v1::{
    GenerateSigningKeyRequest, GetVerifyingKeyRequest, HealthCheckRequest, SignRequest,
    key_server_client::KeyServerClient,
};
use base64::prelude::*;
use ed25519_dalek::VerifyingKey;
use nonce_auth::CredentialBuilder;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

/// KS gRPC 客户端配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GrpcClientConfig {
    /// KS 服务地址 (gRPC endpoint)
    ///
    /// 例如: "http://127.0.0.1:8080" 或 "https://ks.example.com:8443"
    pub endpoint: String,

    /// Actrix 共享密钥（用于认证）
    pub actrix_shared_key: String,

    /// 请求超时时间（秒）
    pub timeout_seconds: u64,

    /// 是否启用 TLS
    pub enable_tls: bool,

    /// TLS 域名（启用 TLS 时必需）
    pub tls_domain: Option<String>,

    /// CA 证书路径（用于验证服务端）
    pub ca_cert: Option<String>,

    /// 客户端证书路径（mTLS）
    pub client_cert: Option<String>,

    /// 客户端私钥路径（mTLS）
    pub client_key: Option<String>,
}

/// KS gRPC 客户端
pub struct GrpcClient {
    client: KeyServerClient<Channel>,
    actrix_shared_key: String,
}

impl GrpcClient {
    /// 创建新的 KS gRPC 客户端
    pub async fn new(config: &GrpcClientConfig) -> Result<Self, KsError> {
        let mut endpoint = Endpoint::from_shared(config.endpoint.clone())
            .map_err(|e| KsError::Internal(format!("Invalid endpoint: {e}")))?
            .timeout(Duration::from_secs(config.timeout_seconds))
            .connect_timeout(Duration::from_secs(config.timeout_seconds));

        // 如果启用 TLS，配置 TLS/mTLS
        if config.enable_tls {
            let tls_config = Self::build_tls_config(config)?;
            endpoint = endpoint
                .tls_config(tls_config)
                .map_err(|e| KsError::Internal(format!("TLS configuration error: {e}")))?;
            crate::recording::info!("TLS enabled for KS gRPC client");
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| KsError::Internal(format!("Failed to connect to KS: {e}")))?;

        let client = KeyServerClient::new(channel);

        Ok(Self {
            client,
            actrix_shared_key: config.actrix_shared_key.clone(),
        })
    }

    /// 构建 TLS 配置
    fn build_tls_config(config: &GrpcClientConfig) -> Result<ClientTlsConfig, KsError> {
        let tls_domain = config.tls_domain.as_ref().ok_or_else(|| {
            KsError::Config("tls_domain is required when enable_tls is true".to_string())
        })?;

        let mut tls_config = ClientTlsConfig::new().domain_name(tls_domain);

        crate::recording::debug!("Configuring TLS with domain: {}", tls_domain);

        // 加载 CA 证书
        if let Some(ca_cert_path) = &config.ca_cert {
            crate::recording::debug!("Loading CA certificate from: {}", ca_cert_path);
            let ca_cert_pem = std::fs::read(ca_cert_path).map_err(|e| {
                KsError::Config(format!(
                    "Failed to read CA certificate from {ca_cert_path}: {e}"
                ))
            })?;

            let ca_cert = Certificate::from_pem(ca_cert_pem);
            tls_config = tls_config.ca_certificate(ca_cert);
            crate::recording::info!("CA certificate loaded for server verification");
        }

        // 加载客户端证书和私钥（mTLS）
        if let (Some(cert_path), Some(key_path)) = (&config.client_cert, &config.client_key) {
            crate::recording::debug!("Loading client certificate from: {}", cert_path);
            crate::recording::debug!("Loading client private key from: {}", key_path);

            let client_cert_pem = std::fs::read(cert_path).map_err(|e| {
                KsError::Config(format!(
                    "Failed to read client certificate from {cert_path}: {e}"
                ))
            })?;

            let client_key_pem = std::fs::read(key_path).map_err(|e| {
                KsError::Config(format!(
                    "Failed to read client private key from {key_path}: {e}"
                ))
            })?;

            let identity = Identity::from_pem(client_cert_pem, client_key_pem);
            tls_config = tls_config.identity(identity);
            crate::recording::info!("mTLS enabled: client certificate and key loaded");
        } else if config.client_cert.is_some() || config.client_key.is_some() {
            return Err(KsError::Config(
                "Both client_cert and client_key must be provided for mTLS".to_string(),
            ));
        }

        Ok(tls_config)
    }

    /// 从 KS 服务生成新的 Ed25519 签名密钥对
    ///
    /// 返回 (key_id, verifying_key_bytes[32], expires_at, tolerance_seconds)
    /// 私钥保留在 KS 服务端，不返回给调用方
    pub async fn generate_signing_key(
        &mut self,
    ) -> Result<(u32, [u8; 32], u64, u64), KsError> {
        let request_data = "generate_signing_key";

        // 创建 nonce credential
        let nonce_credential = CredentialBuilder::new(self.actrix_shared_key.as_bytes())
            .sign(request_data.as_bytes())?;

        // 转换为 protobuf NonceCredential
        let credential = NonceCredential {
            timestamp: nonce_credential.timestamp,
            nonce: nonce_credential.nonce,
            signature: nonce_credential.signature,
        };

        let request = tonic::Request::new(GenerateSigningKeyRequest { credential });

        crate::recording::debug!("Requesting Ed25519 signing key generation from KS via gRPC");

        let response = self
            .client
            .generate_signing_key(request)
            .await
            .map_err(|e| KsError::Internal(format!("gRPC GenerateSigningKey failed: {e}")))?;

        let resp = response.into_inner();

        // 解码 verifying key（32 字节 Ed25519 公钥）
        let verifying_key_bytes = BASE64_STANDARD
            .decode(&resp.verifying_key)
            .map_err(|e| KsError::Crypto(format!("Failed to decode verifying key: {e}")))?;

        if verifying_key_bytes.len() != 32 {
            return Err(KsError::Crypto(format!(
                "Invalid verifying key length: expected 32 bytes, got {}",
                verifying_key_bytes.len()
            )));
        }

        // 验证 verifying key 合法性
        let vk_array: [u8; 32] = verifying_key_bytes
            .try_into()
            .map_err(|_| KsError::Crypto("Failed to convert verifying key to array".to_string()))?;

        VerifyingKey::from_bytes(&vk_array)
            .map_err(|e| KsError::Crypto(format!("Invalid Ed25519 verifying key: {e}")))?;

        crate::recording::info!(
            "Successfully generated Ed25519 signing key via gRPC: key_id={}, expires_at={}, tolerance_seconds={}",
            resp.key_id,
            resp.expires_at,
            resp.tolerance_seconds
        );

        Ok((resp.key_id, vk_array, resp.expires_at, resp.tolerance_seconds))
    }

    /// 使用 KS 服务中的密钥对消息进行 Ed25519 签名
    ///
    /// 返回 64 字节 Ed25519 签名
    /// 私钥不离开 KS 服务
    pub async fn sign(&mut self, key_id: u32, message: &[u8]) -> Result<Vec<u8>, KsError> {
        let request_data = format!("sign:{key_id}");

        // 创建 nonce credential
        let nonce_credential = CredentialBuilder::new(self.actrix_shared_key.as_bytes())
            .sign(request_data.as_bytes())?;

        // 转换为 protobuf NonceCredential
        let credential = NonceCredential {
            timestamp: nonce_credential.timestamp,
            nonce: nonce_credential.nonce,
            signature: nonce_credential.signature,
        };

        let request = tonic::Request::new(SignRequest {
            key_id,
            message: message.to_vec(),
            credential,
        });

        crate::recording::debug!("Requesting signature for key_id={} from KS via gRPC", key_id);

        let response = self
            .client
            .sign(request)
            .await
            .map_err(|e| KsError::Internal(format!("gRPC Sign failed: {e}")))?;

        let resp = response.into_inner();

        if resp.signature.len() != 64 {
            return Err(KsError::Crypto(format!(
                "Invalid signature length: expected 64 bytes, got {}",
                resp.signature.len()
            )));
        }

        crate::recording::info!("Successfully obtained signature for key_id={} via gRPC", key_id);

        Ok(resp.signature)
    }

    /// 获取指定 key_id 的验证公钥（verifying key）
    ///
    /// 返回 (verifying_key_bytes [u8;32], expires_at, tolerance_seconds)
    pub async fn get_verifying_key(
        &mut self,
        key_id: u32,
    ) -> Result<([u8; 32], u64, u64), KsError> {
        let request_data = format!("get_verifying_key:{key_id}");
        let nonce_credential = CredentialBuilder::new(self.actrix_shared_key.as_bytes())
            .sign(request_data.as_bytes())?;

        let credential = NonceCredential {
            timestamp: nonce_credential.timestamp,
            nonce: nonce_credential.nonce,
            signature: nonce_credential.signature,
        };

        let request =
            tonic::Request::new(GetVerifyingKeyRequest { key_id, credential });

        let response = self
            .client
            .get_verifying_key(request)
            .await
            .map_err(|e| KsError::Internal(format!("gRPC GetVerifyingKey failed: {e}")))?;

        let resp = response.into_inner();

        let key_bytes = BASE64_STANDARD
            .decode(&resp.verifying_key)
            .map_err(|e| KsError::Internal(format!("Invalid verifying key base64: {e}")))?;

        let key_array: [u8; 32] = key_bytes
            .try_into()
            .map_err(|_| KsError::Internal("Verifying key must be 32 bytes".to_string()))?;

        // 验证是有效的 Ed25519 公钥
        VerifyingKey::from_bytes(&key_array)
            .map_err(|e| KsError::Internal(format!("Invalid verifying key: {e}")))?;

        Ok((key_array, resp.expires_at, resp.tolerance_seconds))
    }

    /// 健康检查
    pub async fn health_check(&mut self) -> Result<String, KsError> {
        let request_data = "health_check";
        let nonce_credential = CredentialBuilder::new(self.actrix_shared_key.as_bytes())
            .sign(request_data.as_bytes())?;

        let credential = NonceCredential {
            timestamp: nonce_credential.timestamp,
            nonce: nonce_credential.nonce,
            signature: nonce_credential.signature,
        };

        let request = tonic::Request::new(HealthCheckRequest { credential });

        let response = self
            .client
            .health_check(request)
            .await
            .map_err(|e| KsError::Internal(format!("gRPC HealthCheck failed: {e}")))?;

        let resp = response.into_inner();
        Ok(resp.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_client_config() {
        let config = GrpcClientConfig {
            endpoint: "http://127.0.0.1:8080".to_string(),
            actrix_shared_key: "test-key".to_string(),
            timeout_seconds: 30,
            enable_tls: false,
            tls_domain: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
        };

        assert_eq!(config.endpoint, "http://127.0.0.1:8080");
        assert!(!config.enable_tls);
    }

    #[test]
    fn test_tls_config_validation() {
        let config = GrpcClientConfig {
            endpoint: "https://ks.example.com:8443".to_string(),
            actrix_shared_key: "test-key".to_string(),
            timeout_seconds: 30,
            enable_tls: true,
            tls_domain: None, // 缺少 tls_domain
            ca_cert: None,
            client_cert: None,
            client_key: None,
        };

        let result = GrpcClient::build_tls_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_mtls_partial_config() {
        let config = GrpcClientConfig {
            endpoint: "https://ks.example.com:8443".to_string(),
            actrix_shared_key: "test-key".to_string(),
            timeout_seconds: 30,
            enable_tls: true,
            tls_domain: Some("ks.example.com".to_string()),
            ca_cert: None,
            client_cert: Some("/path/to/cert.pem".to_string()),
            client_key: None, // 缺少 client_key
        };

        let result = GrpcClient::build_tls_config(&config);
        assert!(result.is_err());
    }
}
