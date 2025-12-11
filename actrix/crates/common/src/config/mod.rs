//! 统一配置管理系统
//!
//! 本模块是 Actor-RTC 辅助服务配置的"单一真理之源"。
//! 所有配置项的定义、文档、默认值都在这里统一管理。

pub mod ais;
pub mod bind;
pub mod ks;
pub mod services;
pub mod signaling;
pub mod supervisor;
pub mod tracing;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub mod turn;

pub use crate::config::ais::AisConfig;
pub use crate::config::bind::BindConfig;
pub use crate::config::services::ServicesConfig;
pub use crate::config::signaling::SignalingConfig;
pub use crate::config::supervisor::SupervisorConfig;
pub use crate::config::tracing::TracingConfig;
pub use crate::config::turn::TurnConfig;
use ::ks::storage::StorageBackend;
use std::path::{Path, PathBuf};

/// Actor-RTC 辅助服务的主配置结构体
///
/// 这是系统的核心配置，包含了所有服务的配置信息。
/// 配置文件使用 TOML 格式，支持完整的类型安全加载。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ActrixConfig {
    /// Service enable flags (bitmask) - Primary switch for all services
    ///
    /// This is the primary control mechanism for enabling services. Each service
    /// must have its corresponding bit set in this mask to be enabled. All services
    /// (Signaling, AIS, KS, STUN, TURN) are controlled exclusively by this bitmask.
    ///
    /// Bit positions:
    /// - Bit 0 (1): Signaling service
    /// - Bit 1 (2): STUN service
    /// - Bit 2 (4): TURN service
    /// - Bit 3 (8): AIS (Actor Identity Service)
    /// - Bit 4 (16): KS (Key Server)
    ///
    /// Examples:
    /// - `enable = 31` enables all services (1+2+4+8+16=31)
    /// - `enable = 6` enables STUN + TURN (2+4=6)
    /// - `enable = 7` enables Signaling + STUN + TURN (1+2+4=7)
    #[serde(default = "default_enable")]
    pub enable: u8,

    /// 服务器实例名称
    ///
    /// 用于标识不同的服务器实例，在集群部署中用于区分节点。
    /// 建议使用有意义的命名规则，如：actrix-01, actrix-prod-east-1 等。
    pub name: String,

    /// 运行环境标识
    ///
    /// 指定当前运行环境，影响安全策略和默认行为：
    /// - "dev": 开发环境，允许 HTTP，证书检查较松
    /// - "prod": 生产环境，强制 HTTPS，严格的安全检查
    /// - "test": 测试环境，用于自动化测试
    pub env: String,

    /// 运行用户（可选）
    ///
    /// 指定服务运行的系统用户。服务会在绑定端口后切换到此用户运行，
    /// 以提高安全性。留空则保持当前用户。
    pub user: Option<String>,

    /// 运行用户组（可选）
    ///
    /// 指定服务运行的系统用户组。与 user 配置配合使用。
    pub group: Option<String>,

    /// PID 文件路径（可选）
    ///
    /// 用于存储进程 ID 的文件路径。系统管理工具可以使用此文件
    /// 来监控和管理服务进程。
    pub pid: Option<String>,

    /// 网络绑定配置
    ///
    /// 定义各种网络服务的绑定地址和端口配置。
    pub bind: BindConfig,

    /// TURN 服务特定配置
    ///
    /// TURN 中继服务的专用配置，包括公网地址、端口范围、认证域等。
    pub turn: TurnConfig,

    /// 位置标签
    ///
    /// 用于标识服务器的地理位置或逻辑分组，便于运维管理和监控。
    /// 例如：us-west-1, office-beijing, edge-node-01
    pub location_tag: String,

    /// Supervisor 平台集成配置（可选）
    ///
    /// 配置与 Supervisor 管理平台的集成，包括认证信息和连接地址。
    /// 如果不需要接入管理平台，可以省略此配置段。
    pub supervisor: Option<SupervisorConfig>,

    /// 服务配置集合
    ///
    /// 包含所有业务服务的配置，每个服务可以独立配置自己的参数和依赖。
    /// 采用服务级别的配置结构，实现高内聚低耦合。
    #[serde(default)]
    pub services: ServicesConfig,

    /// SQLite 数据库文件存储目录路径
    ///
    /// 指定用于存储所有 SQLite 数据库文件的目录路径。
    /// 主数据库文件将存储为 `{sqlite_path}/actrix.db`。
    /// 包括 Realm 信息、访问控制列表、nonce 缓存等。
    #[serde(
        serialize_with = "serialize_pathbuf",
        deserialize_with = "deserialize_pathbuf"
    )]
    pub sqlite_path: PathBuf,

    /// Actrix 内部服务通信共享密钥
    ///
    /// 用于 Actrix 各服务之间的内部通信认证，如 AIS 与 KS 之间的通信。
    /// 这是系统级的内部认证密钥，仅用于服务间通信，不应用于对外业务。
    ///
    /// 注意：
    /// - 此密钥仅限 Actrix 内部服务使用
    /// - 不应用于 Realm 业务或外部 API 访问
    /// - 在生产环境中应使用强随机密钥
    /// - 字段名保留 actrix_shared_key 以保持向后兼容
    pub actrix_shared_key: String,

    /// 可观测性配置（日志 + 追踪）
    ///
    /// 将日志和 OpenTelemetry 追踪配置合并到统一的 observability 段，便于统一管理。
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

/// 可观测性配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObservabilityConfig {
    /// 过滤级别（用于日志与追踪）
    ///
    /// 支持 EnvFilter 语法（如 "info,hyper=warn"）。默认值 "info"。
    #[serde(default = "default_filter_level")]
    pub filter_level: String,

    #[serde(default)]
    pub log: LogConfig,

    /// OpenTelemetry 追踪配置（可选）
    ///
    /// 配置分布式追踪系统，支持导出到 Jaeger/Grafana Tempo 等 OTLP 后端。
    /// 需要编译时启用 `opentelemetry` feature。
    #[serde(default)]
    pub tracing: TracingConfig,
}

/// 日志配置
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogConfig {
    /// 日志输出目标
    ///
    /// 控制日志输出位置：
    /// - "console": 仅输出到控制台（默认）
    /// - "file": 输出到文件
    #[serde(default = "default_log_output")]
    pub output: String,

    /// 日志轮转开关
    ///
    /// 当 output = "file" 时有效：
    /// - true: 按天轮转日志文件
    /// - false: 追加到单个文件
    #[serde(default)]
    pub rotate: bool,

    /// 日志文件路径
    ///
    /// 当 output = "file" 时有效
    #[serde(default = "default_log_path")]
    pub path: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log: LogConfig::default(),
            filter_level: default_filter_level(),
            tracing: TracingConfig::default(),
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            output: default_log_output(),
            rotate: false,
            path: default_log_path(),
        }
    }
}

fn default_enable() -> u8 {
    6 // STUN + TURN (默认启用 ICE 服务)
}

fn default_log_output() -> String {
    "console".to_string()
}

fn default_log_path() -> String {
    "logs/".to_string()
}

fn default_filter_level() -> String {
    "info".to_string()
}

fn serialize_pathbuf<S>(path: &Path, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    path.display().to_string().serialize(serializer)
}

fn deserialize_pathbuf<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(PathBuf::from(s))
}

impl Default for ActrixConfig {
    fn default() -> Self {
        Self {
            enable: default_enable(), // 默认启用 STUN + TURN
            name: "actrix-default".to_string(),
            env: "dev".to_string(),
            user: None,
            group: None,
            pid: Some("logs/actrix.pid".to_string()),
            bind: BindConfig::default(),
            turn: TurnConfig::default(),
            location_tag: "default-location".to_string(),
            supervisor: None,
            services: ServicesConfig::default(),
            sqlite_path: PathBuf::from("database"),
            actrix_shared_key: "XDDYE8d+yMfdXcdWMrXprcUk2uzjnmoX6nCfFw1gGIg=".to_string(),
            observability: ObservabilityConfig::default(),
        }
    }
}

// 服务启用标志位常量
pub const ENABLE_SIGNALING: u8 = 0b00001;
pub const ENABLE_STUN: u8 = 0b00010;
pub const ENABLE_TURN: u8 = 0b00100;
pub const ENABLE_AIS: u8 = 0b01000;
pub const ENABLE_KS: u8 = 0b10000;

impl ActrixConfig {
    /// 检查是否启用了信令服务
    ///
    /// Service is enabled if the ENABLE_SIGNALING bit is set in the enable bitmask.
    pub fn is_signaling_enabled(&self) -> bool {
        self.enable & ENABLE_SIGNALING != 0
    }

    /// 检查是否启用了 STUN 服务
    pub fn is_stun_enabled(&self) -> bool {
        self.enable & ENABLE_STUN != 0
    }

    /// 检查是否启用了 TURN 服务
    pub fn is_turn_enabled(&self) -> bool {
        self.enable & ENABLE_TURN != 0
    }

    /// 检查是否启用了 AIS (AId Issue Service) 身份认证服务
    ///
    /// Service is enabled if the ENABLE_AIS bit is set in the enable bitmask.
    pub fn is_ais_enabled(&self) -> bool {
        self.enable & ENABLE_AIS != 0
    }

    /// 检查是否启用了 KS (Key Server) 密钥服务
    ///
    /// Service is enabled if the ENABLE_KS bit is set in the enable bitmask.
    pub fn is_ks_enabled(&self) -> bool {
        self.enable & ENABLE_KS != 0
    }

    /// 检查是否启用了 ICE 服务（STUN 或 TURN）
    pub fn is_ice_enabled(&self) -> bool {
        self.is_stun_enabled() || self.is_turn_enabled()
    }

    /// 检查是否启用了 Supervisor 客户端
    pub fn is_supervisor_enabled(&self) -> bool {
        self.supervisor.as_ref().is_some_and(|config| {
            !config.client.node_id.trim().is_empty() && !config.client.endpoint.trim().is_empty()
        })
    }

    /// 获取 PID 文件路径，如果没有配置则使用默认值
    pub fn get_pid_path(&self) -> Option<String> {
        self.pid.clone().or_else(|| {
            // 如果没有配置 pid，使用默认值 logs/actrix.pid
            Some("logs/actrix.pid".to_string())
        })
    }

    /// 获取 Actrix 内部服务通信共享密钥
    ///
    /// 此密钥用于 Actrix 系统内部服务间的认证通信，
    /// 如 AIS 与 KS 之间的服务调用。
    ///
    /// 注意：此密钥仅用于内部服务通信，不应用于对外业务
    pub fn get_actrix_shared_key(&self) -> &str {
        &self.actrix_shared_key
    }

    /// 获取追踪配置
    ///
    /// 返回 OpenTelemetry 追踪配置的引用
    pub fn tracing_config(&self) -> &TracingConfig {
        &self.observability.tracing
    }

    /// 返回可观测性配置引用
    pub fn observability_config(&self) -> &ObservabilityConfig {
        &self.observability
    }

    /// 返回日志配置引用
    pub fn log_config(&self) -> &LogConfig {
        &self.observability.log
    }

    /// 检查是否使用控制台日志输出
    pub fn is_console_logging(&self) -> bool {
        self.observability.log.output == "console"
    }

    /// 检查是否应该轮转日志
    pub fn should_rotate_logs(&self) -> bool {
        self.observability.log.output == "file" && self.observability.log.rotate
    }

    /// 获取日志/追踪过滤级别，优先使用 RUST_LOG
    pub fn get_filter_level(&self) -> String {
        std::env::var("RUST_LOG")
            .ok()
            .and_then(|v| {
                let trimmed = v.trim().to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            })
            .unwrap_or_else(|| self.observability.filter_level.clone())
    }

    /// 从文件加载配置
    pub fn from_file<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path_ref = path.as_ref();

        // Check if file exists
        if !path_ref.exists() {
            return Err(format!("Configuration file does not exist: {path_ref:?}").into());
        }

        // Check if path is a file, not a directory
        if !path_ref.is_file() {
            return Err(format!("Path is not a valid file: {path_ref:?}").into());
        }

        // Read file content
        let content = std::fs::read_to_string(path_ref)?;

        // Parse TOML content
        let config: ActrixConfig = toml::from_str(&content)?;

        Ok(config)
    }

    /// 从 TOML 字符串加载配置
    pub fn from_toml(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(content)
    }

    /// 将配置序列化为 TOML 字符串
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string(self)
    }

    /// 验证配置有效性
    ///
    /// 检查所有配置项的合法性，包括：
    /// - 必需字段是否存在
    /// - 数值范围是否合理
    /// - 文件路径是否有效
    /// - 服务配置是否一致
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // 验证位掩码值范围 (0-31, 5 bits)
        if self.enable > 31 {
            errors.push(format!(
                "Invalid enable bitmask value: {}. Must be between 0 and 31 (5 bits)",
                self.enable
            ));
        }

        // 验证实例名称
        if self.name.trim().is_empty() {
            errors.push("Instance name cannot be empty".to_string());
        }

        // 验证环境
        if !["dev", "prod", "test"].contains(&self.env.as_str()) {
            errors.push(format!(
                "Invalid environment '{}', must be one of: dev, prod, test",
                self.env
            ));
        }

        // 验证过滤级别（EnvFilter 语法）
        {
            let main_level = self
                .observability
                .filter_level
                .split(',')
                .next()
                .unwrap_or("")
                .trim();
            if !["trace", "debug", "info", "warn", "error"].contains(&main_level) {
                errors.push(format!(
                    "Invalid filter level '{}', must start with one of: trace, debug, info, warn, error",
                    self.observability.filter_level
                ));
            }
        }

        // 验证日志输出
        if !["console", "file"].contains(&self.observability.log.output.as_str()) {
            errors.push(format!(
                "Invalid log output '{}' (observability.log.output), must be 'console' or 'file'",
                self.observability.log.output
            ));
        }

        // 验证 actrix_shared_key
        if self.actrix_shared_key.contains("default") || self.actrix_shared_key.contains("change") {
            errors.push("Security warning: actrix_shared_key appears to be a default value. Please change it!".to_string());
        }
        if self.actrix_shared_key.len() < 16 {
            errors.push("Security warning: actrix_shared_key is too short, recommend at least 16 characters".to_string());
        }

        // 验证 SQLite 路径
        if self
            .sqlite_path
            .to_str()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
        {
            errors.push("SQLite database path cannot be empty".to_string());
        }

        // 验证追踪配置
        if let Err(e) = self.observability.tracing.validate() {
            errors.push(format!("Tracing configuration error: {e}"));
        }

        // 验证 TURN 配置（如果启用）
        if self.is_turn_enabled() {
            if self.turn.advertised_ip.trim().is_empty() {
                errors.push("TURN advertised_ip is required when TURN is enabled".to_string());
            }
            if self.turn.realm.trim().is_empty() {
                errors.push("TURN realm is required when TURN is enabled".to_string());
            }
            // 验证 advertised_ip 格式
            if self.turn.advertised_ip.parse::<std::net::IpAddr>().is_err() {
                errors.push(format!(
                    "Invalid TURN advertised_ip '{}', must be a valid IP address",
                    self.turn.advertised_ip
                ));
            }
        }

        // 验证 KS 配置（如果启用）
        if self.is_ks_enabled() {
            if let Some(ref ks) = self.services.ks {
                // 验证存储配置
                match ks.storage.backend {
                    StorageBackend::Sqlite => {}
                    StorageBackend::Redis => {
                        if ks.storage.redis.is_none() {
                            errors.push(
                                "KS is configured to use Redis but redis config is missing"
                                    .to_string(),
                            );
                        }
                    }
                    StorageBackend::Postgres => {
                        if ks.storage.postgres.is_none() {
                            errors.push(
                                "KS is configured to use PostgreSQL but postgres config is missing"
                                    .to_string(),
                            );
                        }
                    }
                }
            } else {
                // KS 位掩码已设置但 services.ks 配置缺失
                errors.push(
                    "KS service is enabled (ENABLE_KS bit is set) but services.ks configuration is missing".to_string(),
                );
            }
        }

        // 验证 AIS 配置（如果启用）
        if self.is_ais_enabled() {
            // 检查是否能获取 KS 配置（显式配置或自动默认）
            if let Some(ref ais) = self.services.ais {
                if ais.get_ks_client_config(self).is_none() {
                    errors.push(
                        "AIS service is enabled but no KS available: \
                        either configure services.ais.dependencies.ks or enable local KS service"
                            .to_string(),
                    );
                }
            } else {
                // AIS 位掩码已设置但 services.ais 配置缺失
                errors.push(
                    "AIS service is enabled (ENABLE_AIS bit is set) but services.ais configuration is missing".to_string(),
                );
            }
        }

        // 验证 Signaling 配置（如果启用）
        if self.is_signaling_enabled() {
            if let Some(ref signaling) = self.services.signaling {
                if signaling.dependencies.ks.is_none()
                    && !(self.is_ks_enabled() && self.services.ks.is_some())
                {
                    errors.push(
                    "Signaling dependencies.ks is not configured; enable KS (bitmask + services.ks) to use local defaults"
                        .to_string(),
                );
                }

                if signaling.dependencies.ais.is_none()
                    && !(self.is_ais_enabled() && self.services.ais.is_some())
                {
                    errors.push(
                    "Signaling dependencies.ais is not configured; enable AIS (bitmask + services.ais) to use local defaults"
                        .to_string(),
                );
                }
            } else {
                errors.push(
                    "Signaling service is enabled (ENABLE_SIGNALING bit is set) but services.signaling configuration is missing"
                        .to_string(),
                );
            }
        }

        // 生产环境额外检查
        if self.env == "prod" {
            // 生产环境应使用 HTTPS
            if let Some(ref https) = self.bind.https {
                if https.port == 0 {
                    errors.push(
                        "Production environment should enable HTTPS with valid port".to_string(),
                    );
                }
            } else {
                errors.push("Production environment should enable HTTPS".to_string());
            }

            // 生产环境应使用文件日志
            if self.observability.log.output == "console" {
                errors.push("Warning: Production environment should use file logging (observability.log.output = \"file\")".to_string());
            }

            // 生产环境建议启用日志轮转
            if self.observability.log.output == "file" && !self.observability.log.rotate {
                errors.push("Warning: Production environment should enable log rotation (observability.log.rotate = true)".to_string());
            }
        }

        // Supervisor 配置校验
        if let Some(ref supervisor) = self.supervisor
            && let Err(e) = supervisor.validate()
        {
            errors.push(format!("Supervisor configuration error: {e}"));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::ks::KsServiceConfig;

    #[test]
    fn test_default_config() {
        let config = ActrixConfig::default();
        assert_eq!(config.enable, 6); // 默认启用 STUN + TURN
        assert_eq!(config.name, "actrix-default");
        assert_eq!(config.env, "dev");
        assert!(!config.is_signaling_enabled()); // Signaling 默认不启用
        assert!(config.is_stun_enabled());
        assert!(config.is_turn_enabled());
        assert!(!config.is_ais_enabled()); // AIS 默认不启用
        assert!(!config.is_ks_enabled()); // KS 默认不启用
    }

    #[test]
    fn test_toml_serialization() {
        let config = ActrixConfig::default();
        let toml_str = config.to_toml().unwrap();
        assert!(toml_str.contains("enable = 6")); // STUN + TURN
        assert!(toml_str.contains("name = \"actrix-default\""));
        assert!(
            toml_str
                .contains("actrix_shared_key = \"XDDYE8d+yMfdXcdWMrXprcUk2uzjnmoX6nCfFw1gGIg=\"")
        );

        let parsed_config = ActrixConfig::from_toml(&toml_str).unwrap();
        assert_eq!(parsed_config.enable, config.enable);
        assert_eq!(parsed_config.name, config.name);
        assert_eq!(parsed_config.actrix_shared_key, config.actrix_shared_key);
    }

    #[test]
    fn test_actrix_shared_key() {
        let config = ActrixConfig::default();
        assert_eq!(
            config.get_actrix_shared_key(),
            "XDDYE8d+yMfdXcdWMrXprcUk2uzjnmoX6nCfFw1gGIg="
        );

        // 测试自定义共享密钥
        let mut custom_config = config;
        custom_config.actrix_shared_key = "custom-shared-key".to_string();
        assert_eq!(custom_config.get_actrix_shared_key(), "custom-shared-key");
    }

    #[test]
    fn test_service_flags() {
        let mut config = ActrixConfig::default();

        // Test Signaling service: bitmask-only control
        // Case 1: Bitmask not set, service not enabled
        config.enable = 0;
        config.services.signaling = Some(SignalingConfig {
            server: signaling::SignalingServerConfig::default(),
            dependencies: signaling::SignalingDependencies::default(),
        });
        assert!(!config.is_signaling_enabled());

        // Case 2: Bitmask set -> enabled (regardless of services.* config)
        config.enable = ENABLE_SIGNALING;
        assert!(config.is_signaling_enabled());

        // Case 3: Bitmask set, with services.* config -> still enabled
        config.services.signaling = Some(SignalingConfig {
            server: signaling::SignalingServerConfig::default(),
            dependencies: signaling::SignalingDependencies::default(),
        });
        assert!(config.is_signaling_enabled());

        // Case 4: Bitmask set, no services.* config -> enabled
        config.services.signaling = None;
        assert!(config.is_signaling_enabled());

        // Test AIS service: bitmask-only control
        config.enable = 0;
        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies::default(),
        });
        assert!(!config.is_ais_enabled());

        // Case 2: Bitmask set -> enabled (regardless of services.* config)
        config.enable = ENABLE_AIS;
        assert!(config.is_ais_enabled());

        // Case 3: Bitmask set, with services.* config -> still enabled
        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies::default(),
        });
        assert!(config.is_ais_enabled());

        // Case 4: Bitmask set, no services.* config -> enabled
        config.services.ais = None;
        assert!(config.is_ais_enabled());

        // Test KS service: bitmask-only control
        config.enable = 0;
        config.services.ks = Some(KsServiceConfig {
            ..Default::default()
        });
        assert!(!config.is_ks_enabled());

        // Case 2: Bitmask set -> enabled (regardless of services.* config)
        config.enable = ENABLE_KS;
        assert!(config.is_ks_enabled());

        // Case 3: Bitmask set, with services.* config -> still enabled
        config.services.ks = Some(KsServiceConfig {
            ..Default::default()
        });
        assert!(config.is_ks_enabled());

        // Case 4: Bitmask set, no services.* config -> enabled
        config.services.ks = None;
        assert!(config.is_ks_enabled());

        // Test ICE services (STUN/TURN use bitmask only)
        config.enable = ENABLE_STUN;
        assert!(config.is_stun_enabled());
        assert!(config.is_ice_enabled());

        config.enable = ENABLE_TURN;
        assert!(config.is_turn_enabled());
        assert!(config.is_ice_enabled());
    }

    #[test]
    fn test_ais_auto_ks_config() {
        let mut config = ActrixConfig::default();

        // 场景 1: 启用本地 KS，AIS 不配置 KS 客户端，应自动使用本地 KS
        config.enable = ENABLE_KS | ENABLE_AIS; // Enable both KS and AIS via bitmask
        config.services.ks = Some(KsServiceConfig {
            ..Default::default()
        });

        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies { ks: None }, // 未配置 KS
        });

        // 应该能获取到自动生成的 KS 配置
        let ks_config = config
            .services
            .ais
            .as_ref()
            .unwrap()
            .get_ks_client_config(&config);
        assert!(ks_config.is_some());
        let ks_config = ks_config.unwrap();
        // gRPC 默认端口 50052
        assert_eq!(ks_config.endpoint, "http://127.0.0.1:50052");

        // 场景 2: 显式配置 KS 客户端，应使用显式配置
        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies {
                ks: Some(crate::config::ks::KsClientConfig {
                    endpoint: "http://remote-ks:50052".to_string(),
                    timeout_seconds: 10,
                    enable_tls: false,
                    tls_domain: None,
                    ca_cert: None,
                    client_cert: None,
                    client_key: None,
                }),
            },
        });

        let ks_config = config
            .services
            .ais
            .as_ref()
            .unwrap()
            .get_ks_client_config(&config);
        assert!(ks_config.is_some());
        let ks_config = ks_config.unwrap();
        assert_eq!(ks_config.endpoint, "http://remote-ks:50052"); // 使用显式配置

        // 场景 3: 没有本地 KS，也没有显式配置，应返回 None
        config.services.ks = None;
        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies { ks: None },
        });
        config.enable = ENABLE_AIS; // Enable AIS via bitmask

        let ks_config = config
            .services
            .ais
            .as_ref()
            .unwrap()
            .get_ks_client_config(&config);
        assert!(ks_config.is_none());
    }

    #[test]
    fn test_signaling_auto_ks_config() {
        let mut config = ActrixConfig::default();

        // 启用本地 KS 和 Signaling
        config.enable = ENABLE_KS | ENABLE_SIGNALING; // Enable both via bitmask
        config.services.ks = Some(KsServiceConfig {
            ..Default::default()
        });

        config.services.signaling = Some(SignalingConfig {
            server: signaling::SignalingServerConfig::default(),
            dependencies: signaling::SignalingDependencies {
                ks: None,
                ais: None,
            },
        });

        // Signaling 应该能获取到自动生成的 KS 配置
        let ks_config = config
            .services
            .signaling
            .as_ref()
            .unwrap()
            .get_ks_client_config(&config);
        assert!(ks_config.is_some());
        let ks_config = ks_config.unwrap();
        // gRPC 默认端口 50052
        assert_eq!(ks_config.endpoint, "http://127.0.0.1:50052");
    }

    #[test]
    fn test_validate_bitmask_consistency() {
        let mut config = ActrixConfig::default();

        // Case 1: AIS bitmask set but services.ais config missing - should warn
        config.enable = ENABLE_AIS;
        config.services.ais = None;

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains(
            "AIS service is enabled (ENABLE_AIS bit is set) but services.ais configuration is missing"
        )));

        // Case 2: KS bitmask set but services.ks config missing - should warn
        config.enable = ENABLE_KS;
        config.services.ais = None;
        config.services.ks = None;

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| {
            e.contains("KS service is enabled (ENABLE_KS bit is set) but services.ks configuration is missing")
        }));

        // Case 3: Signaling bitmask set but services.signaling config missing - should error
        config.enable = ENABLE_SIGNALING;
        config.services.signaling = None;
        config.services.ks = None;

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| {
            e.contains("Signaling service is enabled (ENABLE_SIGNALING bit is set) but services.signaling configuration is missing")
        }));

        // Case 4: Signaling enabled with config present - should pass (if other validations pass)
        // Note: Unlike AIS, Signaling validation does not check KS availability
        config.enable = ENABLE_SIGNALING;
        config.services.signaling = Some(SignalingConfig {
            server: signaling::SignalingServerConfig::default(),
            dependencies: signaling::SignalingDependencies {
                ks: None,
                ais: None,
            },
        });
        config.services.ks = None; // No local KS

        let result = config.validate();
        // Signaling with config should not have bitmask consistency errors
        // (may have other validation errors like sqlite_path, etc.)
        if let Err(errors) = result {
            assert!(
                !errors
                    .iter()
                    .any(|e| e.contains("Signaling service is enabled (ENABLE_SIGNALING bit is set) but services.signaling configuration is missing"))
            );
            assert!(
                !errors
                    .iter()
                    .any(|e| e.contains("bit is not set in enable bitmask"))
            );
        }

        // Case 5: Bitmask set and services.* config present - should pass (if other validations pass)
        config.enable = ENABLE_AIS | ENABLE_KS;
        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies {
                ks: Some(crate::config::ks::KsClientConfig {
                    endpoint: "http://127.0.0.1:50052".to_string(),
                    timeout_seconds: 10,
                    enable_tls: false,
                    tls_domain: None,
                    ca_cert: None,
                    client_cert: None,
                    client_key: None,
                }),
            },
        });
        config.services.ks = Some(KsServiceConfig {
            storage: ::ks::storage::StorageConfig {
                backend: ::ks::storage::StorageBackend::Sqlite,
                key_ttl_seconds: 3600,
                sqlite: Some(::ks::storage::SqliteConfig {}),
                redis: None,
                postgres: None,
            },
            kek: None,
            kek_env: None,
            kek_file: None,
            tolerance_seconds: 3600,
        });

        // Should not have bitmask consistency errors (may have other validation errors)
        let result = config.validate();
        if let Err(errors) = result {
            assert!(
                !errors
                    .iter()
                    .any(|e| e.contains("bit is not set in enable bitmask"))
            );
        }
    }

    #[test]
    fn test_signaling_dependencies_require_local_services_when_missing_clients() {
        let mut config = ActrixConfig::default();
        config.enable = ENABLE_SIGNALING;
        config.services.signaling = Some(SignalingConfig {
            server: signaling::SignalingServerConfig::default(),
            dependencies: signaling::SignalingDependencies::default(),
        });
        config.services.ks = None;
        config.services.ais = None;

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("Signaling dependencies.ks is not configured"))
        );
        assert!(
            errors
                .iter()
                .any(|e| e.contains("Signaling dependencies.ais is not configured"))
        );
    }

    #[test]
    fn test_signaling_dependencies_accept_local_services_when_enabled() {
        let mut config = ActrixConfig::default();
        config.enable = ENABLE_SIGNALING | ENABLE_KS | ENABLE_AIS;
        config.services.signaling = Some(SignalingConfig {
            server: signaling::SignalingServerConfig::default(),
            dependencies: signaling::SignalingDependencies::default(),
        });
        config.services.ks = Some(KsServiceConfig {
            ..Default::default()
        });
        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies::default(),
        });

        let result = config.validate();
        assert!(result.is_ok());
    }
}
