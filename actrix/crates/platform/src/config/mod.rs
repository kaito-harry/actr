//! 统一配置管理系统
//!
//! 本模块是 Actrix 辅助服务配置的"单一真理之源"。
//! 所有配置项的定义、文档、默认值都在这里统一管理。

pub mod ais;
pub mod bind;
pub mod config_store;
pub mod control;
pub mod registry;
pub mod resolver;
pub mod services;
pub mod signaling;
pub mod signer;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub mod turn;

pub use crate::config::ais::AisConfig;
pub use crate::config::bind::BindConfig;
pub use crate::config::control::{AdminUiConfig, ControlConfig, ControlHead};
pub use crate::config::services::ServicesConfig;
pub use crate::config::signaling::SignalingConfig;
pub use crate::config::turn::TurnConfig;
use ::signer::storage::StorageBackend;
use std::path::{Path, PathBuf};
use url::Url;

/// Actrix 辅助服务的主配置结构体
///
/// 这是系统的核心配置，包含了所有服务的配置信息。
/// 配置文件使用 TOML 格式，支持完整的类型安全加载。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ActrixConfig {
    /// Service enable flags (bitmask) - Primary switch for业务服务
    ///
    /// This is the primary control mechanism for enabling业务服务。
    /// `control` 是常驻控制面能力，不受该位掩码控制。
    ///
    /// Bit positions（仅业务服务）:
    /// - Bit 0 (1): Signaling service
    /// - Bit 1 (2): STUN service
    /// - Bit 2 (4): TURN service
    /// - Bit 3 (8): AIS (Actor Identity Service)
    /// - Bit 4 (16): Signer
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

    /// 控制面配置（常驻）
    ///
    /// 控制面始终可用，不受 `enable` 位掩码控制。
    #[serde(default)]
    pub control: ControlConfig,

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
    /// 用于 Actrix 各服务之间的内部通信认证，如 AIS 与 Signer 之间的通信。
    /// 这是系统级的内部认证密钥，仅用于服务间通信，不应用于对外业务。
    ///
    /// 注意：
    /// - 此密钥仅限 Actrix 内部服务使用
    /// - 不应用于 Realm 业务或外部 API 访问
    /// - 在生产环境中应使用强随机密钥
    /// - 字段名保留 actrix_shared_key 以保持向后兼容
    pub actrix_shared_key: String,

    /// 记录管线配置（日志 + 追踪）
    ///
    /// 将日志和 OpenTelemetry 追踪配置合并到统一的 recording 段，便于统一管理。
    #[serde(default)]
    pub recording: RecordingConfig,

    /// Monitoring endpoints configuration
    ///
    /// Controls authentication for `/health` and `/metrics` endpoints.
    /// Supports IP whitelist (CIDR) and/or HTTP Basic Auth (htpasswd file).
    /// When both are configured, a request matching either is allowed (OR logic).
    /// Per-service overrides replace the global config for that service.
    #[serde(default)]
    pub monitoring: MonitoringConfig,
}

/// 记录管线配置
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RecordingConfig {
    /// 全局记录出口 URI（single sink）
    ///
    /// 作为所有通道默认出口；可被 [recording.<channel>] 覆盖。
    ///
    /// 支持：
    /// - file://...
    /// - otlp+http://...
    /// - otlp+grpc://...
    #[serde(default)]
    pub sink: Option<String>,

    /// OTLP 上报 service.name
    #[serde(default = "default_recording_service_name")]
    pub service_name: String,

    /// observability 通道配置
    #[serde(default = "default_observability_channel")]
    pub observability: RecordingChannelConfig,
    /// audit 通道配置
    #[serde(default = "default_audit_channel")]
    pub audit: RecordingChannelConfig,
    /// security 通道配置
    #[serde(default = "default_security_channel")]
    pub security: RecordingChannelConfig,
    /// operations 通道配置
    #[serde(default = "default_operations_channel")]
    pub operations: RecordingChannelConfig,
}

/// 单通道配置：语义过滤器 + 出口覆盖
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct RecordingChannelConfig {
    /// 通道语义过滤器
    ///
    /// 每个通道有自己的过滤词汇：
    /// - observability: off / digest / detailed / full
    /// - audit: off / mutations / all
    /// - security: off / critical / high / medium / all
    /// - operations: off / lifecycle / detailed
    #[serde(default)]
    pub filter: String,

    /// 记录出口 URI（single sink）
    ///
    /// 支持：
    /// - file://...
    /// - otlp+http://...
    /// - otlp+grpc://...
    #[serde(default)]
    pub sink: Option<String>,
}

fn default_audit_channel() -> RecordingChannelConfig {
    RecordingChannelConfig {
        filter: "mutations".to_string(),
        sink: None,
    }
}

fn default_security_channel() -> RecordingChannelConfig {
    RecordingChannelConfig {
        filter: "all".to_string(),
        sink: None,
    }
}

fn default_operations_channel() -> RecordingChannelConfig {
    RecordingChannelConfig {
        filter: "lifecycle".to_string(),
        sink: None,
    }
}

fn default_observability_channel() -> RecordingChannelConfig {
    RecordingChannelConfig {
        filter: "digest".to_string(),
        sink: None,
    }
}

impl Default for RecordingConfig {
    fn default() -> Self {
        Self {
            sink: None,
            service_name: default_recording_service_name(),
            observability: default_observability_channel(),
            audit: default_audit_channel(),
            security: default_security_channel(),
            operations: default_operations_channel(),
        }
    }
}

fn normalized_optional_uri(uri: &Option<String>) -> Option<String> {
    uri.as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn any_recording_file_sink(recording: &RecordingConfig) -> bool {
    [
        &recording.sink,
        &recording.observability.sink,
        &recording.audit.sink,
        &recording.security.sink,
        &recording.operations.sink,
    ]
    .into_iter()
    .filter_map(normalized_optional_uri)
    .any(|sink| sink.starts_with("file://"))
}

fn validate_recording_sink_field(field: &str, value: &Option<String>, errors: &mut Vec<String>) {
    let Some(uri) = normalized_optional_uri(value) else {
        return;
    };

    let parsed = match Url::parse(&uri) {
        Ok(parsed) => parsed,
        Err(error) => {
            errors.push(format!(
                "Invalid URI in {field}: {uri} (parse error: {error})"
            ));
            return;
        }
    };

    match parsed.scheme() {
        "file" => {
            if parsed.to_file_path().is_err() {
                errors.push(format!(
                    "Invalid file URI in {field}: {uri} (cannot convert to local file path)"
                ));
            }
        }
        "otlp+http" | "otlp+grpc" => {
            if parsed.host_str().is_none() {
                errors.push(format!(
                    "Invalid OTLP URI in {field}: {uri} (host is required)"
                ));
            }
        }
        _ => {
            errors.push(format!(
                "Invalid URI scheme in {field}: expected file://, otlp+http:// or otlp+grpc://, got {}",
                parsed.scheme()
            ));
        }
    }
}

/// Monitoring endpoint auth configuration.
///
/// Both `allowed_ips` and `htpasswd_file` can be used simultaneously.
/// When both are configured, a request matching either is allowed (OR logic).
/// When neither is configured, endpoints are publicly accessible.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MonitoringConfig {
    /// IP whitelist in CIDR notation (e.g. "10.0.0.0/8", "127.0.0.1").
    /// A bare IP without prefix length implies /32 (IPv4) or /128 (IPv6).
    #[serde(default)]
    pub allowed_ips: Vec<String>,

    /// Path to an htpasswd-format file for HTTP Basic Auth.
    /// Each line: `username:password` (plaintext).
    #[serde(default)]
    pub htpasswd_file: String,

    /// Per-service overrides. When a service key is present here,
    /// it completely replaces the global config for that service.
    #[serde(default)]
    pub services: std::collections::HashMap<String, MonitoringServiceOverride>,
}

/// Per-service monitoring auth override.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MonitoringServiceOverride {
    #[serde(default)]
    pub allowed_ips: Vec<String>,
    #[serde(default)]
    pub htpasswd_file: String,
}

impl MonitoringConfig {
    /// Returns true if no auth is configured (globally).
    pub fn is_open(&self) -> bool {
        self.allowed_ips.is_empty() && self.htpasswd_file.is_empty()
    }

    /// Get the effective auth config for a given service.
    /// Per-service override replaces global when present.
    pub fn effective_for(&self, service: &str) -> (&[String], &str) {
        if let Some(ovr) = self.services.get(service) {
            (ovr.allowed_ips.as_slice(), ovr.htpasswd_file.as_str())
        } else {
            (self.allowed_ips.as_slice(), self.htpasswd_file.as_str())
        }
    }

    /// Returns true if no auth is configured for the given service.
    pub fn is_open_for(&self, service: &str) -> bool {
        let (ips, htpasswd) = self.effective_for(service);
        ips.is_empty() && htpasswd.is_empty()
    }
}

fn default_enable() -> u8 {
    31 // Signaling(1) + STUN(2) + TURN(4) + AIS(8) + Signer(16)
}

fn default_recording_service_name() -> String {
    "actrix".to_string()
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
            control: ControlConfig::default(),
            services: ServicesConfig::default(),
            sqlite_path: PathBuf::from("database"),
            actrix_shared_key: "XDDYE8d+yMfdXcdWMrXprcUk2uzjnmoX6nCfFw1gGIg=".to_string(),
            recording: RecordingConfig::default(),
            monitoring: MonitoringConfig::default(),
        }
    }
}

// 服务启用标志位常量
pub const ENABLE_SIGNALING: u8 = 0b00001;
pub const ENABLE_STUN: u8 = 0b00010;
pub const ENABLE_TURN: u8 = 0b00100;
pub const ENABLE_AIS: u8 = 0b01000;
pub const ENABLE_SIGNER: u8 = 0b10000;

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

    /// 检查是否启用了 Signer 密钥服务
    ///
    /// Service is enabled if the ENABLE_SIGNER bit is set in the enable bitmask.
    pub fn is_signer_enabled(&self) -> bool {
        self.enable & ENABLE_SIGNER != 0
    }

    /// 检查是否启用了 ICE 服务（STUN 或 TURN）
    pub fn is_ice_enabled(&self) -> bool {
        self.is_stun_enabled() || self.is_turn_enabled()
    }

    /// 当前控制面头类型
    pub fn control_head(&self) -> ControlHead {
        if self.control.grpc_api_enabled() && !self.control.admin_ui_enabled() {
            ControlHead::GrpcApi
        } else {
            ControlHead::AdminUi
        }
    }

    /// Admin UI 是否启用。
    pub fn admin_ui_enabled(&self) -> bool {
        self.control.admin_ui_enabled()
    }

    /// NodeAdminService gRPC API 是否启用。
    pub fn grpc_api_enabled(&self) -> bool {
        self.control.grpc_api_enabled()
    }

    /// 是否处于 superv 接管模式。
    pub fn superv_managed(&self) -> bool {
        self.control.superv_managed()
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
    /// 如 AIS 与 Signer 之间的服务调用。
    ///
    /// 注意：此密钥仅用于内部服务通信，不应用于对外业务
    pub fn get_actrix_shared_key(&self) -> &str {
        &self.actrix_shared_key
    }

    /// 返回记录管线配置引用
    pub fn recording_config(&self) -> &RecordingConfig {
        &self.recording
    }

    /// 返回 OTLP service.name
    pub fn recording_service_name(&self) -> &str {
        &self.recording.service_name
    }

    /// 获取复合日志/追踪过滤级别，优先使用 RUST_LOG。
    ///
    /// With semantic filters, channel targets are set to TRACE so that
    /// all events reach the recording layer (which applies its own gate).
    /// The base level stays `info` to suppress third-party noise.
    pub fn get_filter_level(&self) -> String {
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            let trimmed = rust_log.trim().to_string();
            if !trimmed.is_empty() {
                return trimmed;
            }
        }

        "info,actrix::observability=trace,actrix::audit=trace,actrix::security=trace,actrix::operations=trace".to_string()
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

        // control 始终挂载在主 HTTP 端口，必须存在绑定。
        if self.bind.http.is_none() {
            errors.push(
                "Control plane requires bind.http because /admin is always served".to_string(),
            );
        }

        // 验证各通道语义过滤器（空字符串 = 使用通道默认值，跳过验证）
        {
            let check = |name: &str, value: &str, valid: &[&str]| -> Option<String> {
                if value.is_empty() || valid.contains(&value) {
                    None
                } else {
                    Some(format!(
                        "Invalid recording.{name}.filter '{value}', must be one of: {}",
                        valid.join(", ")
                    ))
                }
            };
            if let Some(e) = check(
                "observability",
                &self.recording.observability.filter,
                &["off", "digest", "detailed", "full"],
            ) {
                errors.push(e);
            }
            if let Some(e) = check(
                "audit",
                &self.recording.audit.filter,
                &["off", "mutations", "all"],
            ) {
                errors.push(e);
            }
            if let Some(e) = check(
                "security",
                &self.recording.security.filter,
                &["off", "critical", "high", "medium", "all"],
            ) {
                errors.push(e);
            }
            if let Some(e) = check(
                "operations",
                &self.recording.operations.filter,
                &["off", "lifecycle", "detailed"],
            ) {
                errors.push(e);
            }
        }

        if self.recording.service_name.trim().is_empty() {
            errors.push("recording.service_name cannot be empty".to_string());
        }

        // 验证新的 URI 记录出口配置（single sink, global + per-channel）
        validate_recording_sink_field("recording.sink", &self.recording.sink, &mut errors);
        validate_recording_sink_field(
            "recording.observability.sink",
            &self.recording.observability.sink,
            &mut errors,
        );
        validate_recording_sink_field(
            "recording.audit.sink",
            &self.recording.audit.sink,
            &mut errors,
        );
        validate_recording_sink_field(
            "recording.security.sink",
            &self.recording.security.sink,
            &mut errors,
        );
        validate_recording_sink_field(
            "recording.operations.sink",
            &self.recording.operations.sink,
            &mut errors,
        );

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

        // 验证 ICE 配置（如果启用 STUN 或 TURN）
        if self.is_ice_enabled() {
            if self.bind.ice.advertised_ip.trim().is_empty() {
                errors.push(
                    "bind.ice.advertised_ip is required when ICE services are enabled".to_string(),
                );
            }
            // 验证 advertised_ip 格式
            if self
                .bind
                .ice
                .advertised_ip
                .parse::<std::net::IpAddr>()
                .is_err()
            {
                errors.push(format!(
                    "Invalid bind.ice.advertised_ip '{}', must be a valid IP address",
                    self.bind.ice.advertised_ip
                ));
            }
        }

        // 验证 TURN 配置（如果启用）
        if self.is_turn_enabled() && self.turn.realm.trim().is_empty() {
            errors.push("TURN realm is required when TURN is enabled".to_string());
        }

        // 验证 Signer 配置（如果启用）
        if self.is_signer_enabled() {
            if let Some(ref signer_cfg) = self.services.signer {
                // 验证存储配置
                match signer_cfg.storage.backend {
                    StorageBackend::Sqlite => {}
                    StorageBackend::Postgres => {
                        if signer_cfg.storage.postgres.is_none() {
                            errors.push(
                                "Signer is configured to use PostgreSQL but postgres config is missing"
                                    .to_string(),
                            );
                        }
                    }
                }
            } else {
                // Signer 位掩码已设置但 services.signer 配置缺失
                errors.push(
                    "Signer service is enabled (ENABLE_SIGNER bit is set) but services.signer configuration is missing".to_string(),
                );
            }
        }

        // 验证 AIS 配置（如果启用）
        if self.is_ais_enabled() {
            // 检查是否能获取 KS 配置（显式配置或自动默认）
            if let Some(ref ais) = self.services.ais {
                if ais.get_signer_client_config(self).is_none() {
                    errors.push(
                        "AIS service is enabled but no Signer available: \
                        either configure services.ais.dependencies.signer or enable local Signer service"
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
                if signaling.dependencies.signer.is_none()
                    && !(self.is_signer_enabled() && self.services.signer.is_some())
                {
                    errors.push(
                    "Signaling dependencies.signer is not configured; enable Signer (bitmask + services.signer) to use local defaults"
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
            // 生产环境应使用 HTTPS (bind.http 必须存在且 is_tls() == true)
            if let Some(ref http) = self.bind.http {
                if !http.is_tls() {
                    errors.push(
                        "Production environment should enable HTTPS (configure cert and key in bind.http)".to_string(),
                    );
                }
            } else {
                errors.push("Production environment requires bind.http configuration".to_string());
            }

            // 生产环境建议至少配置一个 file:// 出口
            if !any_recording_file_sink(&self.recording) {
                errors.push("Warning: Production environment should configure recording.sink (file://...) or channel-specific recording.<channel>.sink".to_string());
            }
        }

        if let Err(e) = self.control.validate() {
            errors.push(format!("Control configuration error: {e}"));
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
    use ::signer::SignerServiceConfig;

    #[test]
    fn example_config_loads_and_validates() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.toml");
        let cfg = ActrixConfig::from_file(&path).expect("example config should parse");
        cfg.validate().expect("example config should validate");
        assert!(
            cfg.admin_ui_enabled(),
            "example should have admin_ui enabled"
        );
        assert!(
            !cfg.grpc_api_enabled(),
            "example should have grpc_api disabled by default"
        );
    }

    #[test]
    fn test_default_config() {
        let config = ActrixConfig::default();
        assert_eq!(config.enable, 31); // 默认启用所有服务
        assert_eq!(config.name, "actrix-default");
        assert_eq!(config.env, "dev");
        assert!(config.is_signaling_enabled());
        assert!(config.is_stun_enabled());
        assert!(config.is_turn_enabled());
        assert!(config.is_ais_enabled());
        assert!(config.is_signer_enabled());
        assert_eq!(config.recording.service_name, "actrix");
        assert!(config.recording.sink.is_none());
        assert_eq!(config.recording.observability.filter, "digest");
        assert_eq!(config.recording.audit.filter, "mutations");
        assert_eq!(config.recording.security.filter, "all");
        assert_eq!(config.recording.operations.filter, "lifecycle");
    }

    #[test]
    fn test_recording_global_plus_spec_deserialize() {
        let mut config = ActrixConfig::default();
        config.recording.sink = Some("file:///tmp/actrix.log".to_string());
        config.recording.audit.sink = Some("otlp+http://127.0.0.1:4318/v1/logs".to_string());
        config.recording.security.sink = Some("otlp+grpc://127.0.0.1:4317".to_string());

        let toml = config.to_toml().expect("config should serialize");
        let parsed = ActrixConfig::from_toml(&toml).expect("config should deserialize");
        assert_eq!(
            parsed.recording.sink.as_deref(),
            Some("file:///tmp/actrix.log")
        );
        assert_eq!(
            parsed.recording.audit.sink.as_deref(),
            Some("otlp+http://127.0.0.1:4318/v1/logs")
        );
        assert_eq!(
            parsed.recording.security.sink.as_deref(),
            Some("otlp+grpc://127.0.0.1:4317")
        );
    }

    #[test]
    fn test_recording_sink_validation_rejects_invalid_scheme() {
        let mut config = ActrixConfig::default();
        config.recording.sink = Some("http://127.0.0.1:4317".to_string());

        let errors = config.validate().expect_err("validation should fail");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("Invalid URI scheme in recording.sink")),
            "expected invalid scheme error, got: {errors:?}"
        );
    }

    #[test]
    fn test_recording_sink_validation_accepts_supported_schemes() {
        let mut config = ActrixConfig {
            enable: 0,
            ..Default::default()
        };
        config.control.admin_ui.password = "testpassword123".to_string();
        config.recording.sink = Some("file:///tmp/actrix.log".to_string());
        config.recording.audit.sink = Some("otlp+http://127.0.0.1:4318/v1/logs".to_string());
        config.recording.security.sink = Some("otlp+grpc://127.0.0.1:4317".to_string());

        assert!(
            config.validate().is_ok(),
            "supported sink schemes should pass validation"
        );
    }

    #[test]
    fn test_recording_sink_validation_rejects_otlp_uri_without_host() {
        let mut config = ActrixConfig::default();
        config.recording.sink = Some("otlp+grpc:///v1/traces".to_string());

        let errors = config.validate().expect_err("validation should fail");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("Invalid OTLP URI in recording.sink")),
            "expected missing-host error, got: {errors:?}"
        );
    }

    #[test]
    fn test_recording_sink_validation_rejects_invalid_file_uri() {
        let mut config = ActrixConfig::default();
        config.recording.sink = Some("file://relative-path.log".to_string());

        let errors = config.validate().expect_err("validation should fail");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("Invalid file URI in recording.sink")),
            "expected invalid file-uri error, got: {errors:?}"
        );
    }

    #[test]
    fn test_toml_serialization() {
        let config = ActrixConfig::default();
        let toml_str = config.to_toml().unwrap();
        assert!(toml_str.contains("enable = 31")); // all services
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
        let mut config = ActrixConfig {
            enable: 0,
            ..ActrixConfig::default()
        };

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
        config.services.signer = Some(SignerServiceConfig {
            ..Default::default()
        });
        assert!(!config.is_signer_enabled());

        // Case 2: Bitmask set -> enabled (regardless of services.* config)
        config.enable = ENABLE_SIGNER;
        assert!(config.is_signer_enabled());

        // Case 3: Bitmask set, with services.* config -> still enabled
        config.services.signer = Some(SignerServiceConfig {
            ..Default::default()
        });
        assert!(config.is_signer_enabled());

        // Case 4: Bitmask set, no services.* config -> enabled
        config.services.signer = None;
        assert!(config.is_signer_enabled());

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
        let mut config = ActrixConfig {
            enable: ENABLE_SIGNER | ENABLE_AIS,
            ..ActrixConfig::default()
        };

        // 场景 1: 启用本地 KS，AIS 不配置 KS 客户端，应自动使用本地 KS
        config.enable = ENABLE_SIGNER | ENABLE_AIS; // Enable both KS and AIS via bitmask
        config.services.signer = Some(SignerServiceConfig {
            ..Default::default()
        });

        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies { signer: None }, // 未配置 KS
        });

        // 应该能获取到自动生成的 KS 配置
        let ks_config = config
            .services
            .ais
            .as_ref()
            .unwrap()
            .get_signer_client_config(&config);
        assert!(ks_config.is_some());
        let ks_config = ks_config.unwrap();
        // KS gRPC 复用主 HTTP 端口（默认 8080）
        assert_eq!(ks_config.endpoint, "http://127.0.0.1:8080");

        // 场景 2: 显式配置 KS 客户端，应使用显式配置
        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies {
                signer: Some(crate::config::signer::SignerClientConfig {
                    endpoint: "http://remote-ks:8080".to_string(),
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
            .get_signer_client_config(&config);
        assert!(ks_config.is_some());
        let ks_config = ks_config.unwrap();
        assert_eq!(ks_config.endpoint, "http://remote-ks:8080"); // 使用显式配置

        // 场景 3: 没有本地 KS，也没有显式配置，应返回 None
        config.services.signer = None;
        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies { signer: None },
        });
        config.enable = ENABLE_AIS; // Enable AIS via bitmask

        let ks_config = config
            .services
            .ais
            .as_ref()
            .unwrap()
            .get_signer_client_config(&config);
        assert!(ks_config.is_none());
    }

    #[test]
    fn test_signaling_auto_ks_config() {
        let mut config = ActrixConfig {
            enable: ENABLE_SIGNER | ENABLE_SIGNALING,
            ..ActrixConfig::default()
        };

        // 启用本地 KS 和 Signaling
        config.enable = ENABLE_SIGNER | ENABLE_SIGNALING; // Enable both via bitmask
        config.services.signer = Some(SignerServiceConfig {
            ..Default::default()
        });

        config.services.signaling = Some(SignalingConfig {
            server: signaling::SignalingServerConfig::default(),
            dependencies: signaling::SignalingDependencies {
                signer: None,
                ais: None,
            },
        });

        // Signaling 应该能获取到自动生成的 KS 配置
        let ks_config = config
            .services
            .signaling
            .as_ref()
            .unwrap()
            .get_signer_client_config(&config);
        assert!(ks_config.is_some());
        let ks_config = ks_config.unwrap();
        // KS gRPC 复用主 HTTP 端口（默认 8080）
        assert_eq!(ks_config.endpoint, "http://127.0.0.1:8080");
    }

    #[test]
    fn test_validate_bitmask_consistency() {
        let mut config = ActrixConfig {
            enable: ENABLE_AIS,
            ..ActrixConfig::default()
        };

        // Case 1: AIS bitmask set but services.ais config missing - should warn
        config.enable = ENABLE_AIS;
        config.services.ais = None;

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains(
            "AIS service is enabled (ENABLE_AIS bit is set) but services.ais configuration is missing"
        )));

        // Case 2: Signer bitmask set but services.signer config missing - should warn
        config.enable = ENABLE_SIGNER;
        config.services.ais = None;
        config.services.signer = None;

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| {
            e.contains("Signer service is enabled (ENABLE_SIGNER bit is set) but services.signer configuration is missing")
        }));

        // Case 3: Signaling bitmask set but services.signaling config missing - should error
        config.enable = ENABLE_SIGNALING;
        config.services.signaling = None;
        config.services.signer = None;

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
                signer: None,
                ais: None,
            },
        });
        config.services.signer = None; // No local KS

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
        config.enable = ENABLE_AIS | ENABLE_SIGNER;
        config.services.ais = Some(AisConfig {
            server: ais::AisServerConfig::default(),
            dependencies: ais::AisDependencies {
                signer: Some(crate::config::signer::SignerClientConfig {
                    endpoint: "http://127.0.0.1:8080".to_string(),
                    timeout_seconds: 10,
                    enable_tls: false,
                    tls_domain: None,
                    ca_cert: None,
                    client_cert: None,
                    client_key: None,
                }),
            },
        });
        config.services.signer = Some(SignerServiceConfig {
            storage: ::signer::storage::StorageConfig {
                backend: ::signer::storage::StorageBackend::Sqlite,
                key_ttl_seconds: 3600,
                sqlite: Some(::signer::storage::SqliteConfig {}),
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
        let mut config = ActrixConfig {
            enable: ENABLE_SIGNALING,
            ..ActrixConfig::default()
        };
        config.services.signaling = Some(SignalingConfig {
            server: signaling::SignalingServerConfig::default(),
            dependencies: signaling::SignalingDependencies::default(),
        });
        config.services.signer = None;
        config.services.ais = None;

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("Signaling dependencies.signer is not configured"))
        );
        assert!(
            errors
                .iter()
                .any(|e| e.contains("Signaling dependencies.ais is not configured"))
        );
    }

    #[test]
    fn test_signaling_dependencies_accept_local_services_when_enabled() {
        let mut config = ActrixConfig {
            enable: ENABLE_SIGNALING | ENABLE_SIGNER | ENABLE_AIS,
            ..ActrixConfig::default()
        };
        config.control.admin_ui.password = "testpassword123".to_string();
        config.services.signaling = Some(SignalingConfig {
            server: signaling::SignalingServerConfig::default(),
            dependencies: signaling::SignalingDependencies::default(),
        });
        config.services.signer = Some(SignerServiceConfig {
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
