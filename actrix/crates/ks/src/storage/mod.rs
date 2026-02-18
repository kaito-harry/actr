//! KS 存储模块
//!
//! 提供多种存储后端支持：SQLite, PostgreSQL
//!
//! # 设计
//!
//! - `KeyStorageBackend` trait 定义统一的异步接口
//! - `KeyStorage` enum 封装不同的后端实现
//! - 通过 `StorageConfig` 配置选择和初始化后端

use std::path::Path;

pub mod backend;
pub mod config;

// SQLite 始终可用（使用 rusqlite）
pub mod sqlite;

#[cfg(feature = "backend-postgres")]
pub mod postgres;

use crate::crypto::KeyEncryptor;
use crate::error::{KsError, KsResult};
use crate::types::{KeyPair, KeyRecord};

pub use backend::KeyStorageBackend;
pub use config::{PostgresConfig, SqliteConfig, StorageBackend, StorageConfig};

use sqlite::SqliteBackend;

#[cfg(feature = "backend-postgres")]
use postgres::PostgresBackend;

/// 密钥存储统一接口
///
/// 使用 enum 而不是 trait object 的好处：
/// - 零成本抽象（无虚函数调用）
/// - 可以 Clone
/// - 编译期类型检查
#[derive(Clone, Debug)]
pub enum KeyStorage {
    /// SQLite 存储后端（始终可用）
    Sqlite(Box<SqliteBackend>),

    /// PostgreSQL 存储后端
    #[cfg(feature = "backend-postgres")]
    Postgres(PostgresBackend),
}

impl KeyStorage {
    /// 从配置创建存储实例
    ///
    /// # Arguments
    /// * `config` - 存储配置
    /// * `encryptor` - 密钥加密器
    /// * `db_path` - 数据库文件存储目录路径（当 backend = "sqlite" 时必需，来自 ActrixConfig.sqlite_path）
    ///
    /// # Returns
    /// 初始化完成的存储实例
    ///
    /// # Errors
    /// - 缺少对应后端的配置
    /// - 后端初始化失败
    /// - 后端功能未启用（feature flag）
    pub async fn from_config<P: AsRef<Path>>(
        config: &StorageConfig,
        encryptor: KeyEncryptor,
        db_path: P,
    ) -> KsResult<Self> {
        match config.backend {
            StorageBackend::Sqlite => {
                let cfg = config
                    .sqlite
                    .as_ref()
                    .ok_or_else(|| KsError::Config("Missing SQLite config".into()))?;
                let backend =
                    SqliteBackend::new(cfg, config.key_ttl_seconds, encryptor, db_path.as_ref())
                        .await?;
                Ok(Self::Sqlite(Box::new(backend)))
            }

            #[cfg(feature = "backend-postgres")]
            StorageBackend::Postgres => {
                let cfg = config
                    .postgres
                    .as_ref()
                    .ok_or_else(|| KsError::Config("Missing PostgreSQL config".into()))?;
                let backend = PostgresBackend::new(cfg, config.key_ttl_seconds).await?;
                Ok(Self::Postgres(backend))
            }

            #[cfg(not(feature = "backend-postgres"))]
            StorageBackend::Postgres => Err(KsError::Config(
                "PostgreSQL backend not enabled. Compile with --features backend-postgres".into(),
            )),
        }
    }

    /// 生成并存储新的密钥对
    pub async fn generate_and_store_key(&self) -> KsResult<KeyPair> {
        match self {
            Self::Sqlite(b) => b.generate_and_store_key().await,

            #[cfg(feature = "backend-postgres")]
            Self::Postgres(b) => b.generate_and_store_key().await,
        }
    }

    /// 根据 key_id 查询公钥
    pub async fn get_public_key(&self, key_id: u32) -> KsResult<Option<String>> {
        match self {
            Self::Sqlite(b) => b.get_public_key(key_id).await,

            #[cfg(feature = "backend-postgres")]
            Self::Postgres(b) => b.get_public_key(key_id).await,
        }
    }

    /// 根据 key_id 查询私钥
    pub async fn get_secret_key(&self, key_id: u32) -> KsResult<Option<String>> {
        match self {
            Self::Sqlite(b) => b.get_secret_key(key_id).await,

            #[cfg(feature = "backend-postgres")]
            Self::Postgres(b) => b.get_secret_key(key_id).await,
        }
    }

    /// 获取完整的密钥记录
    pub async fn get_key_record(&self, key_id: u32) -> KsResult<Option<KeyRecord>> {
        match self {
            Self::Sqlite(b) => b.get_key_record(key_id).await,

            #[cfg(feature = "backend-postgres")]
            Self::Postgres(b) => b.get_key_record(key_id).await,
        }
    }

    /// 获取密钥总数
    pub async fn get_key_count(&self) -> KsResult<u32> {
        match self {
            Self::Sqlite(b) => b.get_key_count().await,

            #[cfg(feature = "backend-postgres")]
            Self::Postgres(b) => b.get_key_count().await,
        }
    }

    /// 清理过期的密钥
    pub async fn cleanup_expired_keys(&self) -> KsResult<u32> {
        match self {
            Self::Sqlite(b) => b.cleanup_expired_keys().await,

            #[cfg(feature = "backend-postgres")]
            Self::Postgres(b) => b.cleanup_expired_keys().await,
        }
    }

    /// 获取后端类型名称
    pub fn backend_name(&self) -> &'static str {
        match self {
            Self::Sqlite(_) => "SQLite",

            #[cfg(feature = "backend-postgres")]
            Self::Postgres(_) => "Postgres",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_storage_from_config_sqlite() {
        let temp_dir = tempdir().unwrap();
        let config = StorageConfig {
            backend: StorageBackend::Sqlite,
            key_ttl_seconds: 3600,
            sqlite: Some(SqliteConfig {}),
            postgres: None,
        };

        let storage = KeyStorage::from_config(
            &config,
            crate::crypto::KeyEncryptor::no_encryption(),
            temp_dir.path(),
        )
        .await
        .unwrap();
        assert_eq!(storage.backend_name(), "SQLite");

        // 测试基本操作
        let key_pair = storage.generate_and_store_key().await.unwrap();
        assert!(key_pair.key_id > 0);

        let public_key = storage.get_public_key(key_pair.key_id).await.unwrap();
        assert_eq!(public_key, Some(key_pair.public_key));
    }

    #[tokio::test]
    async fn test_missing_backend_config() {
        let config = StorageConfig {
            backend: StorageBackend::Sqlite,
            key_ttl_seconds: 3600,
            sqlite: None, // 缺少配置
            postgres: None,
        };

        let temp_dir = tempdir().unwrap();
        let result = KeyStorage::from_config(
            &config,
            crate::crypto::KeyEncryptor::no_encryption(),
            temp_dir.path(),
        )
        .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing SQLite config")
        );
    }
}
