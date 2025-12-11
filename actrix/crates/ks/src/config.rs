//! Key Server (KS) 配置
//!
//! KS 服务用于生成和管理加密密钥，为其他服务提供密钥生成和公钥查询功能

use crate::crypto::KekSource;
use crate::storage::StorageConfig;
use serde::{Deserialize, Serialize};

/// KS 服务配置
///
/// Service enable/disable is controlled by the bitmask in ActrixConfig.enable.
/// The ENABLE_KS bit (bit 4) must be set to enable this service.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KsServiceConfig {
    /// 存储配置
    #[serde(default)]
    pub storage: StorageConfig,

    /// 容忍期 (秒)
    ///
    /// 密钥过期后的一段时间内，仍然允许获取私钥，用于平滑过渡
    /// 默认: 3600 (1小时)
    #[serde(default = "default_tolerance")]
    pub tolerance_seconds: u64,

    /// KEK (Key Encryption Key) - 直接配置
    ///
    /// 用于加密存储的私钥。支持两种格式：
    /// - 64 字符的十六进制字符串（32 字节）
    /// - 44 字符的 Base64 字符串（32 字节）
    ///
    /// 注意：直接在配置文件中存储 KEK 不够安全，生产环境建议使用 kek_env 或 kek_file
    #[serde(default)]
    pub kek: Option<String>,

    /// KEK 环境变量名称
    ///
    /// 从指定的环境变量读取 KEK，比直接配置更安全
    /// 例如：kek_env = "ACTRIX_KS_KEK"
    #[serde(default)]
    pub kek_env: Option<String>,

    /// KEK 文件路径
    ///
    /// 从文件读取 KEK，文件应该包含 64 字符的十六进制字符串或 44 字符的 Base64 字符串
    /// 文件权限应设置为 600 (仅所有者可读写)
    #[serde(default)]
    pub kek_file: Option<String>,
}

fn default_tolerance() -> u64 {
    3600
}

impl Default for KsServiceConfig {
    fn default() -> Self {
        Self {
            storage: Default::default(),
            tolerance_seconds: default_tolerance(),
            kek: None,
            kek_env: None,
            kek_file: None,
        }
    }
}

impl KsServiceConfig {
    /// 获取 KEK 源
    ///
    /// 优先级: kek_file > kek_env > kek
    /// 如果都未配置，返回 None（使用无加密模式）
    pub fn get_kek_source(&self) -> Option<KekSource> {
        if let Some(path) = &self.kek_file {
            return Some(KekSource::File(path.clone()));
        }

        if let Some(env_var) = &self.kek_env {
            return Some(KekSource::Environment(env_var.clone()));
        }

        if let Some(kek) = &self.kek {
            return Some(KekSource::Direct(kek.clone()));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{SqliteConfig, StorageBackend};

    #[test]
    fn test_default_ks_service_config() {
        let config = KsServiceConfig::default();
        assert_eq!(config.storage.backend, StorageBackend::Sqlite);
    }

    #[test]
    fn test_serialize_ks_service_config() {
        let config = KsServiceConfig {
            storage: StorageConfig {
                backend: StorageBackend::Sqlite,
                key_ttl_seconds: 7200,
                sqlite: Some(SqliteConfig {}),
                redis: None,
                postgres: None,
            },
            kek: None,
            kek_env: None,
            kek_file: None,
            tolerance_seconds: 3600,
        };

        let toml = toml::to_string(&config).unwrap();
        assert!(!toml.contains("enabled")); // enabled field should not be present
        assert!(toml.contains("key_ttl_seconds = 7200"));
    }

    #[test]
    fn test_kek_source_priority() {
        // 未配置 KEK
        let config = KsServiceConfig::default();
        assert!(config.get_kek_source().is_none());

        // 仅配置 kek
        let config = KsServiceConfig {
            kek: Some("test_kek".to_string()),
            ..Default::default()
        };
        match config.get_kek_source() {
            Some(KekSource::Direct(k)) => assert_eq!(k, "test_kek"),
            _ => panic!("Expected Direct KEK source"),
        }

        // 配置 kek 和 kek_env，优先使用 kek_env
        let config = KsServiceConfig {
            kek: Some("test_kek".to_string()),
            kek_env: Some("TEST_ENV".to_string()),
            ..Default::default()
        };
        match config.get_kek_source() {
            Some(KekSource::Environment(e)) => assert_eq!(e, "TEST_ENV"),
            _ => panic!("Expected Environment KEK source"),
        }

        // 配置所有三个，优先使用 kek_file
        let config = KsServiceConfig {
            kek: Some("test_kek".to_string()),
            kek_env: Some("TEST_ENV".to_string()),
            kek_file: Some("/path/to/kek".to_string()),
            ..Default::default()
        };
        match config.get_kek_source() {
            Some(KekSource::File(f)) => assert_eq!(f, "/path/to/kek"),
            _ => panic!("Expected File KEK source"),
        }
    }
}
