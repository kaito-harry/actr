use serde::{Deserialize, Serialize};

/// Supervisor 平台集成配置（顶层共享设置 + 角色子段）
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SupervisorConfig {
    /// 连接超时（秒）
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,

    /// 状态上报间隔（秒）
    #[serde(default = "default_status_interval")]
    pub status_report_interval_secs: u64,

    /// 健康检查间隔（秒）
    #[serde(default = "default_health_check_interval")]
    pub health_check_interval_secs: u64,

    /// 是否启用 TLS
    #[serde(default)]
    pub enable_tls: bool,

    /// TLS 域名（用于证书验证）
    pub tls_domain: Option<String>,

    /// Client certificate path (for mTLS)
    ///
    /// If provided, mutual TLS (mTLS) is enabled.
    /// Must be provided together with client_key.
    pub client_cert: Option<String>,

    /// Client private key path (for mTLS)
    ///
    /// If provided, mutual TLS (mTLS) is enabled.
    /// Must be provided together with client_cert.
    pub client_key: Option<String>,

    /// CA certificate path used to verify the server certificate
    ///
    /// If provided, this CA is used to verify the server certificate.
    /// If not provided, system default CAs are used.
    pub ca_cert: Option<String>,

    /// Maximum allowed clock skew in seconds
    ///
    /// Used for nonce-auth timestamp validation.
    /// Default is 300 seconds (5 minutes).
    #[serde(default = "default_max_clock_skew")]
    pub max_clock_skew_secs: u64,

    /// Supervisord 回调服务配置（供管理平台回连）
    #[serde(default)]
    pub supervisord: SupervisordConfig,

    /// Supervisor 客户端配置（主动注册 + 上报）
    #[serde(default)]
    pub client: SupervisorClientConfig,
}

/// Supervisord gRPC 服务配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SupervisordConfig {
    /// 节点显示名称（必填），默认回退到 node_id
    ///
    /// 在 UI 和监控中用于展示的节点名称。建议使用具有业务含义的名称。
    #[serde(default = "default_node_name")]
    pub node_name: String,

    /// Bind IP address
    ///
    /// Network interface IP address to bind the supervisord gRPC service.
    /// Typically use "0.0.0.0" to listen on all interfaces.
    #[serde(default = "default_bind_ip")]
    pub ip: String,

    /// Bind port
    ///
    /// Port number for the supervisord gRPC service to listen on.
    /// Default is 50055.
    #[serde(default = "default_bind_port")]
    pub port: u16,

    /// Advertised IP address
    ///
    /// Public IP address that clients (Supervisor) will use to connect.
    /// In NAT environments, this is typically the router's public IP.
    #[serde(default = "default_advertised_ip")]
    pub advertised_ip: String,
}

/// Supervisor 客户端配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SupervisorClientConfig {
    /// 节点唯一标识符
    ///
    /// 在 Supervisor 平台中的唯一标识符，用于识别此服务实例。
    pub node_id: String,

    /// Supervisor gRPC endpoint
    ///
    /// gRPC 服务器地址，格式：http://hostname:port 或 https://hostname:port
    /// 示例：http://supervisor.example.com:50051
    pub endpoint: String,

    /// Shared secret (hex encoded for HMAC signatures)
    ///
    /// Shared secret used for nonce-auth authentication.
    /// Must be at least 32 bytes (64 hex characters).
    /// Example: generate with `openssl rand -hex 32`
    pub shared_secret: String,
}

fn default_connect_timeout() -> u64 {
    30
}

fn default_status_interval() -> u64 {
    60
}

fn default_health_check_interval() -> u64 {
    30
}

fn default_max_clock_skew() -> u64 {
    300 // 5 minutes
}

fn default_node_name() -> String {
    String::from("actrix-node")
}

fn default_bind_ip() -> String {
    "0.0.0.0".to_string()
}

fn default_bind_port() -> u16 {
    50055
}

fn default_advertised_ip() -> String {
    "127.0.0.1".to_string()
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: default_connect_timeout(),
            status_report_interval_secs: default_status_interval(),
            health_check_interval_secs: default_health_check_interval(),
            enable_tls: false,
            tls_domain: None,
            client_cert: None,
            client_key: None,
            ca_cert: None,
            max_clock_skew_secs: default_max_clock_skew(),
            supervisord: SupervisordConfig::default(),
            client: SupervisorClientConfig::default(),
        }
    }
}

impl SupervisorConfig {
    /// Get the bind address as SocketAddr string
    ///
    /// Returns a string in the format "ip:port" for binding the gRPC server.
    pub fn bind_addr(&self) -> String {
        self.supervisord.bind_addr()
    }

    /// Get the advertised address for registration
    ///
    /// Returns a string in the format "advertised_ip:port" for Supervisor registration.
    pub fn advertised_addr(&self) -> String {
        self.supervisord.advertised_addr()
    }

    pub fn node_name(&self) -> &str {
        self.supervisord.node_name.as_str()
    }

    pub fn node_id(&self) -> &str {
        self.client.node_id.as_str()
    }

    /// Get the client endpoint
    pub fn endpoint(&self) -> &str {
        self.client.endpoint.as_str()
    }

    /// Get the shared secret
    pub fn shared_secret(&self) -> &str {
        self.client.shared_secret.as_str()
    }

    /// Validate configuration correctness
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the configuration is valid
    /// - `Err(String)` if the configuration is invalid with a descriptive message
    pub fn validate(&self) -> Result<(), String> {
        let mut errors = Vec::new();

        // client section
        let client = &self.client;
        if client.node_id.trim().is_empty() {
            errors.push("supervisor.client.node_id cannot be empty".to_string());
        }

        if client.endpoint.trim().is_empty() {
            errors.push("supervisor.client.endpoint cannot be empty".to_string());
        } else if !client.endpoint.starts_with("http://")
            && !client.endpoint.starts_with("https://")
        {
            errors
                .push("supervisor.client.endpoint must start with http:// or https://".to_string());
        }

        let secret = &client.shared_secret;
        if secret.trim().is_empty() {
            errors
                .push("supervisor.client.shared_secret is required for authentication".to_string());
        } else {
            if secret.len() < 64 {
                errors.push(
                    "supervisor.client.shared_secret must be at least 64 hex characters (32 bytes)"
                        .to_string(),
                );
            }
            if hex::decode(secret).is_err() {
                errors
                    .push("supervisor.client.shared_secret must be a valid hex string".to_string());
            }
        }

        // supervisord section
        let supervisord = &self.supervisord;
        if supervisord.node_name.trim().is_empty() {
            errors.push("supervisor.supervisord.node_name cannot be empty".to_string());
        }
        if supervisord.ip.trim().is_empty() {
            errors.push("supervisor.supervisord.ip cannot be empty".to_string());
        }

        if supervisord.advertised_ip.trim().is_empty() {
            errors.push("supervisor.supervisord.advertised_ip cannot be empty".to_string());
        }

        if supervisord.port == 0 {
            errors.push("supervisor.supervisord.port must be greater than 0".to_string());
        }

        if self.enable_tls && self.tls_domain.is_none() {
            errors.push("tls_domain is required when enable_tls is true".to_string());
        }

        // Validate mTLS configuration completeness
        if self.client_cert.is_some() || self.client_key.is_some() {
            if self.client_cert.is_none() || self.client_key.is_none() {
                errors
                    .push("Both client_cert and client_key must be provided for mTLS".to_string());
            }
            if !self.enable_tls {
                errors.push("enable_tls must be true when using mTLS".to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join(", "))
        }
    }
}

impl SupervisordConfig {
    /// 返回绑定地址 "ip:port"
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    /// 返回对外发布地址 "advertised_ip:port"
    pub fn advertised_addr(&self) -> String {
        format!("{}:{}", self.advertised_ip, self.port)
    }
}

impl Default for SupervisordConfig {
    fn default() -> Self {
        Self {
            node_name: default_node_name(),
            ip: default_bind_ip(),
            port: default_bind_port(),
            advertised_ip: default_advertised_ip(),
        }
    }
}

impl Default for SupervisorClientConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            endpoint: "http://localhost:50051".to_string(),
            shared_secret: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_secret() -> String {
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string()
    }

    fn base_config() -> SupervisorConfig {
        SupervisorConfig {
            client: SupervisorClientConfig {
                node_id: "test-node".to_string(),
                endpoint: "http://localhost:50051".to_string(),
                shared_secret: valid_secret(),
            },
            supervisord: SupervisordConfig::default(),
            ..Default::default()
        }
    }

    #[test]
    fn test_default_config() {
        let config = SupervisorConfig::default();
        let client = &config.client;
        assert_eq!(client.endpoint, "http://localhost:50051");
        assert_eq!(config.connect_timeout_secs, 30);
        assert_eq!(config.status_report_interval_secs, 60);
        assert_eq!(config.health_check_interval_secs, 30);
        assert_eq!(config.max_clock_skew_secs, 300);
        assert!(!config.enable_tls);
    }

    #[test]
    fn test_validate_empty_node_id() {
        let mut config = base_config();
        config.client.node_id = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_url() {
        let mut config = base_config();
        config.client.endpoint = "invalid-url".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_tls_without_domain() {
        let mut config = base_config();
        config.enable_tls = true;
        config.client.endpoint = "https://localhost:50051".to_string();
        config.tls_domain = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_mtls_partial_config() {
        let mut config = base_config();
        config.enable_tls = true;
        config.tls_domain = Some("localhost".to_string());
        config.client.endpoint = "https://localhost:50051".to_string();
        config.client_cert = Some("/path/to/cert.pem".to_string());
        config.client_key = None; // missing private key
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_mtls_without_tls() {
        let mut config = base_config();
        config.enable_tls = false;
        config.client_cert = Some("/path/to/cert.pem".to_string());
        config.client_key = Some("/path/to/key.pem".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_shared_secret() {
        let mut config = base_config();
        config.client.shared_secret = "short".to_string(); // too short
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_hex_shared_secret() {
        let mut config = base_config();
        config.client.shared_secret = "x".repeat(64); // invalid hex
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_valid_config() {
        let config = base_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_valid_tls_config() {
        let mut config = base_config();
        config.enable_tls = true;
        config.client.endpoint = "https://supervisor.example.com:50051".to_string();
        config.tls_domain = Some("supervisor.example.com".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_valid_mtls_config() {
        let mut config = base_config();
        config.enable_tls = true;
        config.client.endpoint = "https://supervisor.example.com:50051".to_string();
        config.tls_domain = Some("supervisor.example.com".to_string());
        config.client_cert = Some("/path/to/cert.pem".to_string());
        config.client_key = Some("/path/to/key.pem".to_string());
        config.ca_cert = Some("/path/to/ca.pem".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_valid_shared_secret() {
        let mut config = base_config();
        config.client.shared_secret = valid_secret();
        assert!(config.validate().is_ok());
    }
}
