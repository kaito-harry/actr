//! AIS (Actor Identity Service) 客户端
//!
//! 用于 Signaling 服务调用 AIS 重新签发 Credential

use actr_protocol::{ActrType, Realm, RegisterRequest, RegisterResponse, register_response};
use anyhow::{Result, anyhow};
use prost::Message;
use std::time::Duration;
use tracing::{debug, error};

/// AIS 客户端配置
#[derive(Debug, Clone)]
pub struct AisClientConfig {
    /// AIS 服务端点 URL (例如: "http://127.0.0.1:8443")
    pub endpoint: String,
    /// 请求超时时间（秒）
    pub timeout_seconds: u64,
}

impl Default for AisClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8443".to_string(),
            timeout_seconds: 30,
        }
    }
}

/// AIS 客户端
#[derive(Debug)]
pub struct AisClient {
    endpoint: String,
    client: reqwest::Client,
}

impl AisClient {
    /// 创建新的 AIS 客户端
    pub fn new(config: &AisClientConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .danger_accept_invalid_certs(true) // 开发环境允许自签名证书
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {e}"))?;

        Ok(Self {
            endpoint: config.endpoint.clone(),
            client,
        })
    }

    /// 刷新 Credential（调用 AIS /register 接口）
    ///
    /// # 参数
    /// - `realm_id`: Realm ID
    /// - `actr_type`: Actor 类型
    ///
    /// # 返回
    /// - `Ok(RegisterResponse)`: 成功响应
    /// - `Err`: 网络错误或 AIS 返回错误
    pub async fn refresh_credential(
        &self,
        realm_id: u32,
        actr_type: ActrType,
    ) -> Result<RegisterResponse> {
        let url = format!("{}/ais/register", self.endpoint);

        // 构造 RegisterRequest
        let request = RegisterRequest {
            realm: Realm { realm_id },
            actr_type: actr_type.clone(),
            service_spec: None,
            acl: None,
        };

        // 序列化为 protobuf
        let mut request_bytes = Vec::new();
        request
            .encode(&mut request_bytes)
            .map_err(|e| anyhow!("Failed to encode request: {e}"))?;

        debug!(
            "Sending refresh_credential request to {} (realm={}, type={}:{})",
            url, realm_id, request.actr_type.manufacturer, request.actr_type.name
        );

        // 发送 HTTP POST 请求
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(request_bytes)
            .send()
            .await
            .map_err(|e| anyhow!("HTTP request failed: {e}"))?;

        // 检查 HTTP 状态码
        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<no body>".to_string());
            error!("AIS returned HTTP {}: {}", status, body);
            return Err(anyhow!("AIS HTTP error {status}: {body}"));
        }

        // 解析 protobuf 响应
        let response_bytes = response
            .bytes()
            .await
            .map_err(|e| anyhow!("Failed to read response body: {e}"))?;

        let register_response = RegisterResponse::decode(&response_bytes[..])
            .map_err(|e| anyhow!("Failed to decode response: {e}"))?;

        // 检查响应结果
        match &register_response.result {
            Some(register_response::Result::Success(ok)) => {
                debug!(
                    "Successfully refreshed credential: realm={}, serial_number={}",
                    ok.actr_id.realm.realm_id, ok.actr_id.serial_number
                );
                Ok(register_response)
            }
            Some(register_response::Result::Error(err)) => {
                error!("AIS returned error: {} - {}", err.code, err.message);
                Err(anyhow!("AIS error {}: {}", err.code, err.message))
            }
            None => Err(anyhow!("Empty response from AIS")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ais_client_config_default() {
        let config = AisClientConfig::default();
        assert_eq!(config.endpoint, "https://127.0.0.1:8443");
        assert_eq!(config.timeout_seconds, 30);
    }

    #[test]
    fn test_ais_client_creation() {
        let config = AisClientConfig::default();
        let client = AisClient::new(&config);
        assert!(client.is_ok());
    }
}
