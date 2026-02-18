//! 存储后端配置
//!
//! 定义各种存储后端的配置结构

use serde::{Deserialize, Serialize};

/// 存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 存储后端类型
    pub backend: StorageBackend,

    /// 密钥有效期（秒）
    ///
    /// 生成的密钥的有效期时间，超过此时间的密钥将被视为过期
    /// 设置为 0 表示永不过期
    pub key_ttl_seconds: u64,

    /// SQLite 配置（当 backend = "sqlite" 时必需）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlite: Option<SqliteConfig>,

    /// PostgreSQL 配置（当 backend = "postgres" 时必需）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres: Option<PostgresConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::Sqlite,
            key_ttl_seconds: 3600,
            sqlite: Some(SqliteConfig::default()),
            postgres: None,
        }
    }
}

/// 存储后端类型枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// SQLite 数据库
    Sqlite,
    /// PostgreSQL 数据库
    Postgres,
}

/// SQLite 配置
///
/// 注意：数据库路径通过 KeyStorage::from_config 的 db_path 参数传入
/// TODO: define grain config for sqlite
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SqliteConfig {}

/// PostgreSQL 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// 数据库主机地址
    pub host: String,

    /// 数据库端口
    pub port: u16,

    /// 数据库名称
    pub database: String,

    /// 用户名
    pub username: String,

    /// 密码
    pub password: String,

    /// 连接池大小
    #[serde(default = "default_postgres_pool_size")]
    pub pool_size: u32,

    /// 连接最大生命周期（秒）
    #[serde(default = "default_max_lifetime_secs")]
    pub max_lifetime_secs: u64,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: "actrix_ks".to_string(),
            username: "actrix".to_string(),
            password: "".to_string(),
            pool_size: default_postgres_pool_size(),
            max_lifetime_secs: default_max_lifetime_secs(),
        }
    }
}

fn default_postgres_pool_size() -> u32 {
    20
}

fn default_max_lifetime_secs() -> u64 {
    3600
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_storage_config() {
        let config = StorageConfig::default();
        assert_eq!(config.backend, StorageBackend::Sqlite);
        assert_eq!(config.key_ttl_seconds, 3600);
        assert!(config.sqlite.is_some());
    }

    #[test]
    fn test_serialize_sqlite_config() {
        let config = StorageConfig {
            backend: StorageBackend::Sqlite,
            key_ttl_seconds: 7200,
            sqlite: Some(SqliteConfig {}),
            postgres: None,
        };

        let toml = toml::to_string(&config).unwrap();
        assert!(toml.contains("backend = \"sqlite\""));
        assert!(toml.contains("key_ttl_seconds = 7200"));
    }

    #[test]
    fn test_deserialize_postgres_config() {
        let toml_str = r#"
            backend = "postgres"
            key_ttl_seconds = 1800

            [postgres]
            host = "localhost"
            port = 5432
            database = "actrix"
            username = "actrix"
            password = "secret"
            pool_size = 30
        "#;

        let config: StorageConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.backend, StorageBackend::Postgres);
        assert_eq!(config.key_ttl_seconds, 1800);

        let postgres = config.postgres.unwrap();
        assert_eq!(postgres.host, "localhost");
        assert_eq!(postgres.port, 5432);
        assert_eq!(postgres.database, "actrix");
        assert_eq!(postgres.username, "actrix");
        assert_eq!(postgres.password, "secret");
        assert_eq!(postgres.pool_size, 30);
    }
}
