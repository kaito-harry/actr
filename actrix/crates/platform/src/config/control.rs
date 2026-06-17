use serde::{Deserialize, Serialize};

/// Control 头类型。
///
/// - `admin_ui`: 提供本地管理 UI（HTTP）。
/// - `grpc_api`: 提供给集群 supervisor 的 gRPC API（复用主 HTTP 端口）。
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ControlHead {
    #[default]
    AdminUi,
    GrpcApi,
}

/// gRPC 控制面配置。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ControlGrpcApiConfig {
    /// 是否启用 NodeAdminService gRPC API。
    #[serde(default)]
    pub enabled: bool,

    /// 节点 ID（用于认证载荷）
    #[serde(default = "default_grpc_node_id")]
    pub node_id: String,

    /// 节点展示名（为空时回退到 node_id）
    #[serde(default = "default_grpc_node_name")]
    pub node_name: String,

    /// nonce-auth 共享密钥（hex, 至少 64 个字符）
    #[serde(default)]
    pub shared_secret: String,

    /// 允许的最大时钟偏差（秒）
    #[serde(default = "default_max_clock_skew_secs")]
    pub max_clock_skew_secs: u64,
}

/// Admin UI 配置。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AdminUiConfig {
    /// 是否启用本地 Admin UI。
    #[serde(default = "default_admin_ui_enabled")]
    pub enabled: bool,

    /// 登录密码（admin_ui 模式必填，≥8 字符）
    #[serde(default)]
    pub password: String,

    /// JWT 会话过期时间（秒），默认 86400（24 小时）
    #[serde(default = "default_session_expiry_secs")]
    pub session_expiry_secs: u64,
}

/// Control 常驻配置。
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ControlConfig {
    /// 旧版二选一头模式。新配置优先使用 admin_ui.enabled/grpc_api.enabled。
    #[serde(default)]
    pub head: Option<ControlHead>,

    /// gRPC 头参数（仅 grpc_api 模式使用）
    #[serde(default)]
    pub grpc_api: ControlGrpcApiConfig,

    /// Admin UI 参数（仅 admin_ui 模式使用）
    #[serde(default)]
    pub admin_ui: AdminUiConfig,
}

fn default_grpc_node_id() -> String {
    "actrix-node".to_string()
}

fn default_grpc_node_name() -> String {
    "actrix-node".to_string()
}

fn default_max_clock_skew_secs() -> u64 {
    300
}

fn default_session_expiry_secs() -> u64 {
    86400
}

fn default_admin_ui_enabled() -> bool {
    true
}

impl Default for AdminUiConfig {
    fn default() -> Self {
        Self {
            enabled: default_admin_ui_enabled(),
            password: String::new(),
            session_expiry_secs: default_session_expiry_secs(),
        }
    }
}

impl Default for ControlGrpcApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: default_grpc_node_id(),
            node_name: default_grpc_node_name(),
            shared_secret: String::new(),
            max_clock_skew_secs: default_max_clock_skew_secs(),
        }
    }
}

impl ControlGrpcApiConfig {
    pub fn effective_node_name(&self) -> String {
        let trimmed = self.node_name.trim();
        if trimmed.is_empty() {
            self.node_id.trim().to_string()
        } else {
            trimmed.to_string()
        }
    }
}

impl ControlConfig {
    pub fn admin_ui_enabled(&self) -> bool {
        match self.head {
            Some(ControlHead::AdminUi) => true,
            Some(ControlHead::GrpcApi) => false,
            None => self.admin_ui.enabled,
        }
    }

    pub fn grpc_api_enabled(&self) -> bool {
        match self.head {
            Some(ControlHead::AdminUi) => false,
            Some(ControlHead::GrpcApi) => true,
            None => self.grpc_api.enabled,
        }
    }

    pub fn superv_managed(&self) -> bool {
        self.grpc_api_enabled()
    }

    pub fn validate(&self) -> Result<(), String> {
        if !self.admin_ui_enabled() && !self.grpc_api_enabled() {
            return Err("at least one control endpoint must be enabled".to_string());
        }

        if self.admin_ui_enabled() {
            self.validate_admin_ui()?;
        }

        if self.grpc_api_enabled() {
            self.validate_grpc_api()?;
        }

        Ok(())
    }

    fn validate_admin_ui(&self) -> Result<(), String> {
        let cfg = &self.admin_ui;

        if cfg.password.is_empty() {
            return Err(
                "control.admin_ui.password is required when admin_ui is enabled".to_string(),
            );
        }

        if cfg.password.len() < 8 {
            return Err("control.admin_ui.password must be at least 8 characters".to_string());
        }

        if cfg.session_expiry_secs == 0 {
            return Err("control.admin_ui.session_expiry_secs must be greater than 0".to_string());
        }

        Ok(())
    }

    fn validate_grpc_api(&self) -> Result<(), String> {
        let cfg = &self.grpc_api;

        if cfg.node_id.trim().is_empty() {
            return Err("control.grpc_api.node_id cannot be empty".to_string());
        }

        if cfg.shared_secret.trim().is_empty() {
            return Err(
                "control.grpc_api.shared_secret is required when grpc_api is enabled".to_string(),
            );
        }

        if cfg.shared_secret.len() < 64 {
            return Err(
                "control.grpc_api.shared_secret must be at least 64 hex characters (32 bytes)"
                    .to_string(),
            );
        }

        if hex::decode(&cfg.shared_secret).is_err() {
            return Err("control.grpc_api.shared_secret must be a valid hex string".to_string());
        }

        if cfg.max_clock_skew_secs == 0 {
            return Err("control.grpc_api.max_clock_skew_secs must be greater than 0".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_control_head_is_admin_ui() {
        let cfg = ControlConfig::default();
        assert!(cfg.admin_ui_enabled());
        assert!(!cfg.grpc_api_enabled());
    }

    #[test]
    fn admin_ui_requires_password() {
        let cfg = ControlConfig {
            head: Some(ControlHead::AdminUi),
            admin_ui: AdminUiConfig {
                password: String::new(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(cfg.validate().is_err());
    }

    #[test]
    fn admin_ui_rejects_short_password() {
        let cfg = ControlConfig {
            head: Some(ControlHead::AdminUi),
            admin_ui: AdminUiConfig {
                password: "short".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(cfg.validate().is_err());
    }

    #[test]
    fn admin_ui_accepts_valid_password() {
        let cfg = ControlConfig {
            head: Some(ControlHead::AdminUi),
            admin_ui: AdminUiConfig {
                password: "changeme123".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn grpc_api_requires_shared_secret() {
        let cfg = ControlConfig {
            head: Some(ControlHead::GrpcApi),
            grpc_api: ControlGrpcApiConfig {
                shared_secret: String::new(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(cfg.validate().is_err());
    }

    #[test]
    fn grpc_api_accepts_valid_secret() {
        let cfg = ControlConfig {
            head: Some(ControlHead::GrpcApi),
            grpc_api: ControlGrpcApiConfig {
                shared_secret: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn enabled_flags_allow_admin_ui_and_grpc_together() {
        let cfg = ControlConfig {
            head: None,
            admin_ui: AdminUiConfig {
                enabled: true,
                password: "changeme123".to_string(),
                ..Default::default()
            },
            grpc_api: ControlGrpcApiConfig {
                enabled: true,
                shared_secret: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                ..Default::default()
            },
        };

        assert!(cfg.admin_ui_enabled());
        assert!(cfg.grpc_api_enabled());
        assert!(cfg.superv_managed());
        assert!(cfg.validate().is_ok());
    }
}
