//! 配置注册表
//!
//! 静态注册表，声明每个可配置字段的元信息（类型、默认值、是否支持动态覆盖/热重载）。

use serde::Serialize;

/// 配置值类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigValueType {
    String,
    U8,
    U16,
    U32,
    U64,
    Bool,
    Enum,
    /// "start-end" where both are u16, start <= end
    Range16,
    /// IP address (v4 or v6), validated via std::net::IpAddr
    Ip,
    /// Local filesystem path
    Fpath,
    /// Domain name / hostname
    Domain,
    /// URI path (starts with `/`)
    #[serde(rename = "uri_path")]
    UriPath,
}

impl ConfigValueType {
    /// Validate that a string value can be parsed as this type.
    /// For Enum validation, use `ConfigFieldDef::validate` which checks choices.
    pub fn validate_str(&self, value: &str) -> bool {
        match self {
            ConfigValueType::String | ConfigValueType::Enum | ConfigValueType::Fpath => true,
            ConfigValueType::U8 => value.parse::<u8>().is_ok(),
            ConfigValueType::U16 => value.parse::<u16>().is_ok(),
            ConfigValueType::U32 => value.parse::<u32>().is_ok(),
            ConfigValueType::U64 => value.parse::<u64>().is_ok(),
            ConfigValueType::Bool => value == "true" || value == "false",
            ConfigValueType::Range16 => {
                let Some((a, b)) = value.split_once('-') else {
                    return false;
                };
                let (Ok(start), Ok(end)) = (a.parse::<u16>(), b.parse::<u16>()) else {
                    return false;
                };
                start <= end
            }
            ConfigValueType::Ip => value.parse::<std::net::IpAddr>().is_ok(),
            ConfigValueType::Domain => {
                !value.is_empty()
                    && value.len() <= 253
                    && value.split('.').all(|label| {
                        !label.is_empty()
                            && label.len() <= 63
                            && label
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                    })
            }
            ConfigValueType::UriPath => value.starts_with('/'),
        }
    }
}

impl std::fmt::Display for ConfigValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigValueType::String => write!(f, "string"),
            ConfigValueType::U8 => write!(f, "u8"),
            ConfigValueType::U16 => write!(f, "u16"),
            ConfigValueType::U32 => write!(f, "u32"),
            ConfigValueType::U64 => write!(f, "u64"),
            ConfigValueType::Bool => write!(f, "bool"),
            ConfigValueType::Enum => write!(f, "enum"),
            ConfigValueType::Range16 => write!(f, "range16"),
            ConfigValueType::Ip => write!(f, "ip"),
            ConfigValueType::Fpath => write!(f, "fpath"),
            ConfigValueType::Domain => write!(f, "domain"),
            ConfigValueType::UriPath => write!(f, "uri_path"),
        }
    }
}

/// 配置字段定义
#[derive(Debug, Clone, Serialize)]
pub struct ConfigFieldDef {
    /// Dot-separated key, e.g. "turn.realm"
    pub key: &'static str,
    /// TOML navigation path, e.g. "turn.realm"
    pub toml_path: &'static str,
    /// Value type
    pub value_type: ConfigValueType,
    /// Default value (as string)
    pub default_value: &'static str,
    /// Can be overridden via API at runtime
    pub dynamic: bool,
    /// Takes effect on SIGHUP reload (without restart)
    pub reloadable: bool,
    /// Human-readable description
    pub description: &'static str,
    /// Service this field belongs to
    pub service: &'static str,
    /// Valid choices for Enum type (empty for other types)
    pub choices: &'static [&'static str],
}

impl ConfigFieldDef {
    /// Validate a string value against this field's type and choices.
    pub fn validate(&self, value: &str) -> bool {
        if !self.value_type.validate_str(value) {
            return false;
        }
        if self.value_type == ConfigValueType::Enum && !self.choices.is_empty() {
            return self.choices.contains(&value);
        }
        true
    }
}

/// Complete static registry of all config fields.
static REGISTRY: &[ConfigFieldDef] = &[
    // ── Platform ──────────────────────────────────────────────────
    ConfigFieldDef {
        key: "enable",
        toml_path: "enable",
        value_type: ConfigValueType::U8,
        default_value: "31",
        dynamic: false,
        reloadable: false,
        description: "Service enable bitmask",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "name",
        toml_path: "name",
        value_type: ConfigValueType::String,
        default_value: "actrix-default",
        dynamic: false,
        reloadable: false,
        description: "Server instance name",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "env",
        toml_path: "env",
        value_type: ConfigValueType::Enum,
        default_value: "dev",
        dynamic: false,
        reloadable: false,
        description: "Environment",
        service: "platform",
        choices: &["dev", "prod", "test"],
    },
    ConfigFieldDef {
        key: "location_tag",
        toml_path: "location_tag",
        value_type: ConfigValueType::String,
        default_value: "default-location",
        dynamic: false,
        reloadable: false,
        description: "Geographic location tag",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "sqlite_path",
        toml_path: "sqlite_path",
        value_type: ConfigValueType::Fpath,
        default_value: "database",
        dynamic: false,
        reloadable: false,
        description: "SQLite database directory",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.http.ip",
        toml_path: "bind.http.ip",
        value_type: ConfigValueType::Ip,
        default_value: "::",
        dynamic: false,
        reloadable: false,
        description: "HTTP listen address",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.http.port",
        toml_path: "bind.http.port",
        value_type: ConfigValueType::U16,
        default_value: "8080",
        dynamic: false,
        reloadable: false,
        description: "HTTP port",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.http.domain_name",
        toml_path: "bind.http.domain_name",
        value_type: ConfigValueType::Domain,
        default_value: "localhost",
        dynamic: false,
        reloadable: false,
        description: "HTTP domain name",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.http.advertised_ip",
        toml_path: "bind.http.advertised_ip",
        value_type: ConfigValueType::Ip,
        default_value: "127.0.0.1",
        dynamic: false,
        reloadable: false,
        description: "HTTP advertised IP",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.http.advertised_port",
        toml_path: "bind.http.advertised_port",
        value_type: ConfigValueType::U16,
        default_value: "0",
        dynamic: false,
        reloadable: false,
        description: "HTTP advertised port (0 = same as port)",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.http.cert",
        toml_path: "bind.http.cert",
        value_type: ConfigValueType::Fpath,
        default_value: "",
        dynamic: false,
        reloadable: false,
        description: "TLS certificate path (enables HTTPS)",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.http.key",
        toml_path: "bind.http.key",
        value_type: ConfigValueType::Fpath,
        default_value: "",
        dynamic: false,
        reloadable: false,
        description: "TLS private key path (enables HTTPS)",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "recording.sink",
        toml_path: "recording.sink",
        value_type: ConfigValueType::String,
        default_value: "",
        dynamic: false,
        reloadable: false,
        description: "Global sink URI (file:// | otlp+http:// | otlp+grpc://)",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "recording.service_name",
        toml_path: "recording.service_name",
        value_type: ConfigValueType::String,
        default_value: "actrix",
        dynamic: false,
        reloadable: false,
        description: "OTLP service name",
        service: "platform",
        choices: &[],
    },
    // ── observability channel ──
    ConfigFieldDef {
        key: "recording.observability.filter",
        toml_path: "recording.observability.filter",
        value_type: ConfigValueType::Enum,
        default_value: "digest",
        dynamic: true,
        reloadable: true,
        description: "Observability resolution: off / digest / detailed / full",
        service: "platform",
        choices: &["off", "digest", "detailed", "full"],
    },
    ConfigFieldDef {
        key: "recording.observability.sink",
        toml_path: "recording.observability.sink",
        value_type: ConfigValueType::String,
        default_value: "",
        dynamic: false,
        reloadable: false,
        description: "Observability channel sink override",
        service: "platform",
        choices: &[],
    },
    // ── audit channel ──
    ConfigFieldDef {
        key: "recording.audit.filter",
        toml_path: "recording.audit.filter",
        value_type: ConfigValueType::Enum,
        default_value: "mutations",
        dynamic: true,
        reloadable: true,
        description: "Audit scope: off / mutations / all",
        service: "platform",
        choices: &["off", "mutations", "all"],
    },
    ConfigFieldDef {
        key: "recording.audit.sink",
        toml_path: "recording.audit.sink",
        value_type: ConfigValueType::String,
        default_value: "",
        dynamic: false,
        reloadable: false,
        description: "Audit channel sink override",
        service: "platform",
        choices: &[],
    },
    // ── security channel ──
    ConfigFieldDef {
        key: "recording.security.filter",
        toml_path: "recording.security.filter",
        value_type: ConfigValueType::Enum,
        default_value: "all",
        dynamic: true,
        reloadable: true,
        description: "Security severity threshold: off / critical / high / medium / all",
        service: "platform",
        choices: &["off", "critical", "high", "medium", "all"],
    },
    ConfigFieldDef {
        key: "recording.security.sink",
        toml_path: "recording.security.sink",
        value_type: ConfigValueType::String,
        default_value: "",
        dynamic: false,
        reloadable: false,
        description: "Security channel sink override",
        service: "platform",
        choices: &[],
    },
    // ── operations channel ──
    ConfigFieldDef {
        key: "recording.operations.filter",
        toml_path: "recording.operations.filter",
        value_type: ConfigValueType::Enum,
        default_value: "lifecycle",
        dynamic: true,
        reloadable: true,
        description: "Operations detail: off / lifecycle / detailed",
        service: "platform",
        choices: &["off", "lifecycle", "detailed"],
    },
    ConfigFieldDef {
        key: "recording.operations.sink",
        toml_path: "recording.operations.sink",
        value_type: ConfigValueType::String,
        default_value: "",
        dynamic: false,
        reloadable: false,
        description: "Operations channel sink override",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "control.head",
        toml_path: "control.head",
        value_type: ConfigValueType::Enum,
        default_value: "admin_ui",
        dynamic: false,
        reloadable: false,
        description: "Legacy control plane mode override (prefer admin_ui.enabled / grpc_api.enabled)",
        service: "platform",
        choices: &["admin_ui", "grpc_api"],
    },
    ConfigFieldDef {
        key: "control.admin_ui.enabled",
        toml_path: "control.admin_ui.enabled",
        value_type: ConfigValueType::Bool,
        default_value: "true",
        dynamic: false,
        reloadable: false,
        description: "Enable local Admin UI",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "control.grpc_api.enabled",
        toml_path: "control.grpc_api.enabled",
        value_type: ConfigValueType::Bool,
        default_value: "false",
        dynamic: false,
        reloadable: false,
        description: "Enable NodeAdminService gRPC control API",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "control.admin_ui.session_expiry_secs",
        toml_path: "control.admin_ui.session_expiry_secs",
        value_type: ConfigValueType::U64,
        default_value: "86400",
        dynamic: true,
        reloadable: true,
        description: "Admin session TTL",
        service: "platform",
        choices: &[],
    },
    // ── ICE binding (shared by STUN + TURN) ────────────────────────
    ConfigFieldDef {
        key: "bind.ice.ip",
        toml_path: "bind.ice.ip",
        value_type: ConfigValueType::Ip,
        default_value: "0.0.0.0",
        dynamic: false,
        reloadable: false,
        description: "Local IP address the ICE server listens on",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.ice.port",
        toml_path: "bind.ice.port",
        value_type: ConfigValueType::U16,
        default_value: "3478",
        dynamic: false,
        reloadable: false,
        description: "UDP port the ICE server binds to",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.ice.advertised_ip",
        toml_path: "bind.ice.advertised_ip",
        value_type: ConfigValueType::Ip,
        default_value: "127.0.0.1",
        dynamic: false,
        reloadable: false,
        description: "Public IP advertised to clients",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "bind.ice.advertised_port",
        toml_path: "bind.ice.advertised_port",
        value_type: ConfigValueType::U16,
        default_value: "3478",
        dynamic: false,
        reloadable: false,
        description: "Public-facing port advertised to clients",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "turn.relay_port_range",
        toml_path: "turn.relay_port_range",
        value_type: ConfigValueType::Range16,
        default_value: "49152-65535",
        dynamic: false,
        reloadable: false,
        description: "UDP port range for relay allocations",
        service: "platform",
        choices: &[],
    },
    ConfigFieldDef {
        key: "turn.realm",
        toml_path: "turn.realm",
        value_type: ConfigValueType::Domain,
        default_value: "actrix.local",
        dynamic: true,
        reloadable: true,
        description: "Credential scope for long-term authentication (RFC 8489)",
        service: "turn",
        choices: &[],
    },
    // ── Signaling ─────────────────────────────────────────────────
    ConfigFieldDef {
        key: "services.signaling.server.ws_path",
        toml_path: "services.signaling.server.ws_path",
        value_type: ConfigValueType::UriPath,
        default_value: "/signaling",
        dynamic: false,
        reloadable: true,
        description: "WebSocket endpoint path for client connections",
        service: "signaling",
        choices: &[],
    },
    ConfigFieldDef {
        key: "services.signaling.server.rate_limit.connection.enabled",
        toml_path: "services.signaling.server.rate_limit.connection.enabled",
        value_type: ConfigValueType::Bool,
        default_value: "true",
        dynamic: true,
        reloadable: true,
        description: "Enable connection-level rate limiting",
        service: "signaling",
        choices: &[],
    },
    ConfigFieldDef {
        key: "services.signaling.server.rate_limit.connection.per_minute",
        toml_path: "services.signaling.server.rate_limit.connection.per_minute",
        value_type: ConfigValueType::U32,
        default_value: "5",
        dynamic: true,
        reloadable: true,
        description: "Max new connections per IP per minute",
        service: "signaling",
        choices: &[],
    },
    ConfigFieldDef {
        key: "services.signaling.server.rate_limit.connection.burst_size",
        toml_path: "services.signaling.server.rate_limit.connection.burst_size",
        value_type: ConfigValueType::U32,
        default_value: "10",
        dynamic: true,
        reloadable: true,
        description: "Burst allowance for connection rate",
        service: "signaling",
        choices: &[],
    },
    ConfigFieldDef {
        key: "services.signaling.server.rate_limit.connection.max_concurrent_per_ip",
        toml_path: "services.signaling.server.rate_limit.connection.max_concurrent_per_ip",
        value_type: ConfigValueType::U32,
        default_value: "100",
        dynamic: true,
        reloadable: true,
        description: "Max concurrent connections per IP",
        service: "signaling",
        choices: &[],
    },
    ConfigFieldDef {
        key: "services.signaling.server.rate_limit.message.enabled",
        toml_path: "services.signaling.server.rate_limit.message.enabled",
        value_type: ConfigValueType::Bool,
        default_value: "true",
        dynamic: true,
        reloadable: true,
        description: "Enable message-level rate limiting",
        service: "signaling",
        choices: &[],
    },
    ConfigFieldDef {
        key: "services.signaling.server.rate_limit.message.per_second",
        toml_path: "services.signaling.server.rate_limit.message.per_second",
        value_type: ConfigValueType::U32,
        default_value: "10",
        dynamic: true,
        reloadable: true,
        description: "Max messages per connection per second",
        service: "signaling",
        choices: &[],
    },
    ConfigFieldDef {
        key: "services.signaling.server.rate_limit.message.burst_size",
        toml_path: "services.signaling.server.rate_limit.message.burst_size",
        value_type: ConfigValueType::U32,
        default_value: "50",
        dynamic: true,
        reloadable: true,
        description: "Burst allowance for message rate",
        service: "signaling",
        choices: &[],
    },
    // ── AIS ───────────────────────────────────────────────────────
    ConfigFieldDef {
        key: "services.ais.server.token_ttl_secs",
        toml_path: "services.ais.server.token_ttl_secs",
        value_type: ConfigValueType::U64,
        default_value: "3600",
        dynamic: true,
        reloadable: true,
        description: "Token time-to-live in seconds",
        service: "ais",
        choices: &[],
    },
    ConfigFieldDef {
        key: "services.ais.server.signaling_heartbeat_interval_secs",
        toml_path: "services.ais.server.signaling_heartbeat_interval_secs",
        value_type: ConfigValueType::U32,
        default_value: "30",
        dynamic: true,
        reloadable: true,
        description: "Heartbeat interval for signaling connections",
        service: "ais",
        choices: &[],
    },
    // ── Signer ────────────────────────────────────────────────────────
    ConfigFieldDef {
        key: "services.signer.storage.key_ttl_seconds",
        toml_path: "services.signer.storage.key_ttl_seconds",
        value_type: ConfigValueType::U64,
        default_value: "3600",
        dynamic: true,
        reloadable: true,
        description: "Key time-to-live in seconds",
        service: "signer",
        choices: &[],
    },
    ConfigFieldDef {
        key: "services.signer.tolerance_seconds",
        toml_path: "services.signer.tolerance_seconds",
        value_type: ConfigValueType::U64,
        default_value: "300",
        dynamic: true,
        reloadable: true,
        description: "Clock skew tolerance for key validation",
        service: "signer",
        choices: &[],
    },
    // ── Monitoring ─────────────────────────────────────────────────
    ConfigFieldDef {
        key: "monitoring.htpasswd_file",
        toml_path: "monitoring.htpasswd_file",
        value_type: ConfigValueType::Fpath,
        default_value: "",
        dynamic: true,
        reloadable: true,
        description: "Path to htpasswd file for monitoring endpoint Basic Auth",
        service: "platform",
        choices: &[],
    },
];

/// Look up a field definition by its key.
pub fn get_field(key: &str) -> Option<&'static ConfigFieldDef> {
    REGISTRY.iter().find(|f| f.key == key)
}

/// Get all field definitions for a given service.
pub fn fields_for_service(service: &str) -> Vec<&'static ConfigFieldDef> {
    REGISTRY.iter().filter(|f| f.service == service).collect()
}

/// Get all field definitions in the registry.
pub fn all_fields() -> &'static [ConfigFieldDef] {
    REGISTRY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_field() {
        let field = get_field("turn.realm").unwrap();
        assert_eq!(field.service, "turn");
        assert!(field.dynamic);
        assert!(field.reloadable);
        assert_eq!(field.default_value, "actrix.local");
    }

    #[test]
    fn test_get_field_not_found() {
        assert!(get_field("nonexistent.field").is_none());
    }

    #[test]
    fn test_fields_for_service() {
        let platform_fields = fields_for_service("platform");
        assert_eq!(platform_fields.len(), 32);
        assert!(platform_fields.iter().all(|f| f.service == "platform"));

        let stun_fields = fields_for_service("stun");
        assert_eq!(stun_fields.len(), 0);

        let signaling_fields = fields_for_service("signaling");
        assert_eq!(signaling_fields.len(), 8);
    }

    #[test]
    fn test_validate_type() {
        assert!(ConfigValueType::U8.validate_str("31"));
        assert!(!ConfigValueType::U8.validate_str("256"));
        assert!(ConfigValueType::U16.validate_str("3478"));
        assert!(!ConfigValueType::U16.validate_str("99999"));
        assert!(ConfigValueType::U64.validate_str("99999"));
        assert!(ConfigValueType::Bool.validate_str("true"));
        assert!(!ConfigValueType::Bool.validate_str("yes"));
        assert!(ConfigValueType::String.validate_str("anything"));

        assert!(ConfigValueType::U32.validate_str("100000"));
        assert!(!ConfigValueType::U32.validate_str("-1"));

        // Ip
        assert!(ConfigValueType::Ip.validate_str("127.0.0.1"));
        assert!(ConfigValueType::Ip.validate_str("0.0.0.0"));
        assert!(ConfigValueType::Ip.validate_str("::"));
        assert!(ConfigValueType::Ip.validate_str("::1"));
        assert!(ConfigValueType::Ip.validate_str("2001:db8::1"));
        assert!(!ConfigValueType::Ip.validate_str("not-an-ip"));
        assert!(!ConfigValueType::Ip.validate_str("999.999.999.999"));

        // Fpath
        assert!(ConfigValueType::Fpath.validate_str("/etc/actrix/config.toml"));
        assert!(ConfigValueType::Fpath.validate_str("relative/path"));

        // Domain
        assert!(ConfigValueType::Domain.validate_str("localhost"));
        assert!(ConfigValueType::Domain.validate_str("actrix.example.com"));
        assert!(ConfigValueType::Domain.validate_str("actrix.local"));
        assert!(!ConfigValueType::Domain.validate_str(""));
        assert!(!ConfigValueType::Domain.validate_str("bad domain.com")); // space

        // UriPath
        assert!(ConfigValueType::UriPath.validate_str("/signaling"));
        assert!(ConfigValueType::UriPath.validate_str("/"));
        assert!(!ConfigValueType::UriPath.validate_str("signaling")); // no leading slash

        // Range16
        assert!(ConfigValueType::Range16.validate_str("49152-65535"));
        assert!(ConfigValueType::Range16.validate_str("1024-1024"));
        assert!(!ConfigValueType::Range16.validate_str("65535-1024")); // start > end
        assert!(!ConfigValueType::Range16.validate_str("abc-def"));
        assert!(!ConfigValueType::Range16.validate_str("1024")); // no dash
    }

    #[test]
    fn test_validate_enum() {
        let field = get_field("env").unwrap();
        assert_eq!(field.value_type, ConfigValueType::Enum);
        assert!(field.validate("dev"));
        assert!(field.validate("prod"));
        assert!(!field.validate("staging"));

        let level = get_field("recording.observability.filter").unwrap();
        assert!(level.validate("digest"));
        assert!(level.validate("full"));
        assert!(!level.validate("verbose"));
    }

    #[test]
    fn test_all_fields_have_valid_defaults() {
        for field in all_fields() {
            assert!(
                field.validate(field.default_value),
                "Field '{}' has invalid default '{}' for type {:?}",
                field.key,
                field.default_value,
                field.value_type
            );
        }
    }
}
