//! AIS (Actor Identity Service) 配置

use crate::config::ks::KsClientConfig;
use serde::{Deserialize, Serialize};

/// AIS 服务配置
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AisConfig {
    /// 是否启用 AIS 服务
    #[serde(default)]
    pub enabled: bool,

    /// AIS 服务器配置
    #[serde(default)]
    pub server: AisServerConfig,

    /// AIS 的依赖服务配置
    #[serde(default)]
    pub dependencies: AisDependencies,
}

/// AIS 服务器配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AisServerConfig {
    /// 绑定 IP 地址
    pub ip: String,

    /// 绑定端口
    pub port: u16,

    /// 数据库路径
    pub database_path: String,

    /// Signaling Server 心跳间隔（秒）
    ///
    /// 在 RegisterResponse 中返回，指导客户端连接 Signaling Server 后的心跳频率
    #[serde(default = "default_signaling_heartbeat_interval_secs")]
    pub signaling_heartbeat_interval_secs: u32,

    /// Token 有效期（秒）
    ///
    /// 生成的 AIdCredential 的过期时间
    #[serde(default = "default_token_ttl_secs")]
    pub token_ttl_secs: u64,
}

/// AIS 依赖的外部服务
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AisDependencies {
    /// KS 客户端配置
    ///
    /// 如果未配置，AIS 会自动查找本地 KS 服务：
    /// - 如果 services.ks.enabled = true，使用 localhost:KS_PORT
    /// - 否则返回配置错误
    #[serde(default)]
    pub ks: Option<KsClientConfig>,
}

impl Default for AisServerConfig {
    fn default() -> Self {
        Self {
            ip: "0.0.0.0".to_string(),
            port: 8081,
            database_path: "ais.db".to_string(),
            signaling_heartbeat_interval_secs: default_signaling_heartbeat_interval_secs(),
            token_ttl_secs: default_token_ttl_secs(),
        }
    }
}

/// 默认 Signaling Server 心跳间隔：30 秒
fn default_signaling_heartbeat_interval_secs() -> u32 {
    30
}

/// 默认 Token 有效期：1 小时（3600 秒）
fn default_token_ttl_secs() -> u64 {
    3600
}

impl AisConfig {
    /// 获取 KS 客户端配置
    ///
    /// 支持智能默认：
    /// 1. 如果显式配置了 dependencies.ks，直接返回
    /// 2. 如果本地启用了 KS 服务，返回指向本地 KS 的配置
    /// 3. 否则返回 None
    pub fn get_ks_client_config(
        &self,
        global_config: &super::ActrixConfig,
    ) -> Option<KsClientConfig> {
        // 优先使用显式配置
        if let Some(ref ks_config) = self.dependencies.ks {
            return Some(ks_config.clone());
        }

        // 回退：检查是否启用了本地 KS 服务
        if let Some(ref ks_service) = global_config.services.ks {
            if ks_service.enabled {
                // 自动生成指向本地 KS 的客户端配置
                // gRPC 使用独立端口 50052（HTTP router 使用 8443/8080）
                let grpc_port = 50052;
                let grpc_protocol = "http"; // 默认不启用 TLS，可通过配置开启

                return Some(KsClientConfig {
                    endpoint: format!("{grpc_protocol}://127.0.0.1:{grpc_port}"),
                    #[allow(deprecated)]
                    psk: global_config.actrix_shared_key.clone(),
                    timeout_seconds: 30,
                    enable_tls: false,
                    tls_domain: None,
                    ca_cert: None,
                    client_cert: None,
                    client_key: None,
                });
            }
        }

        None
    }
}
