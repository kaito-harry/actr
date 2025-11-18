//! SQLite 存储后端实现
//!
//! 使用 sqlx 提供原生异步 SQLite 存储支持

use crate::crypto::KeyEncryptor;
use crate::error::{KsError, KsResult};
use crate::storage::backend::KeyStorageBackend;
use crate::storage::config::SqliteConfig;
use crate::types::{KeyPair, KeyRecord};
use async_trait::async_trait;
use base64::prelude::*;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, trace};

/// SQLite 存储后端
#[derive(Clone)]
pub struct SqliteBackend {
    pool: SqlitePool,
    key_ttl: u64,
    encryptor: KeyEncryptor,
}

impl std::fmt::Debug for SqliteBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteBackend")
            .field("key_ttl", &self.key_ttl)
            .field("encryption_enabled", &self.encryptor.is_enabled())
            .finish()
    }
}

impl SqliteBackend {
    /// 创建新的 SQLite 后端实例
    ///
    /// # Arguments
    /// * `config` - SQLite 配置
    /// * `key_ttl` - 密钥有效期（秒），0 表示永不过期
    /// * `encryptor` - 密钥加密器
    pub async fn new(
        config: &SqliteConfig,
        key_ttl: u64,
        encryptor: KeyEncryptor,
    ) -> KsResult<Self> {
        let path = &config.path;

        // 确保数据库目录存在
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                KsError::Internal(format!("Failed to create database directory: {e}"))
            })?;
        }

        // 创建连接选项并启用 WAL 模式
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{path}"))
            .map_err(|e| KsError::Internal(format!("Failed to parse SQLite URL: {e}")))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));

        // 创建连接池
        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await
            .map_err(|e| KsError::Internal(format!("Failed to connect to SQLite: {e}")))?;

        let backend = Self {
            pool,
            key_ttl,
            encryptor,
        };

        // 初始化数据库表
        backend.init().await?;

        info!(
            "SQLite storage initialized with sqlx: path={}, key_ttl={}s, encryption={}, WAL mode enabled",
            path,
            key_ttl,
            backend.encryptor.is_enabled()
        );

        Ok(backend)
    }
}

#[async_trait]
impl KeyStorageBackend for SqliteBackend {
    async fn init(&self) -> KsResult<()> {
        // 创建密钥表
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS keys (
                key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                public_key TEXT NOT NULL,
                secret_key TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| KsError::Internal(format!("Failed to create keys table: {e}")))?;

        // 创建索引
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_keys_expires_at ON keys(expires_at)")
            .execute(&self.pool)
            .await
            .map_err(|e| KsError::Internal(format!("Failed to create index: {e}")))?;

        debug!("SQLite tables and indexes initialized");
        Ok(())
    }

    async fn generate_and_store_key(&self) -> KsResult<KeyPair> {
        // 生成椭圆曲线密钥对
        let (secret_key, public_key) = ecies::utils::generate_keypair();

        // 编码为 Base64
        let secret_key_b64 = BASE64_STANDARD.encode(secret_key.serialize());
        let public_key_b64 = BASE64_STANDARD.encode(public_key.serialize_compressed());

        // 加密私钥（如果启用）
        let encrypted_secret_key = self.encryptor.encrypt(&secret_key_b64)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // 计算过期时间
        let expires_at = if self.key_ttl == 0 {
            0 // 永不过期
        } else {
            now + self.key_ttl as i64
        };

        // 插入密钥并返回 ID（存储加密后的私钥）
        let result = sqlx::query(
            r#"INSERT INTO keys (public_key, secret_key, created_at, expires_at)
               VALUES (?1, ?2, ?3, ?4)"#,
        )
        .bind(&public_key_b64)
        .bind(&encrypted_secret_key)
        .bind(now)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| KsError::Internal(format!("Failed to insert key: {e}")))?;

        let key_id = result.last_insert_rowid() as u32;

        debug!("Generated key with ID: {}", key_id);

        // 返回明文私钥（供调用方使用）
        Ok(KeyPair {
            key_id,
            secret_key: secret_key_b64,
            public_key: public_key_b64,
        })
    }

    async fn get_public_key(&self, key_id: u32) -> KsResult<Option<String>> {
        let result = sqlx::query_as::<_, (String,)>("SELECT public_key FROM keys WHERE key_id = ?")
            .bind(key_id as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                KsError::Internal(format!(
                    "Failed to query public key for key_id {key_id}: {e}"
                ))
            })?;

        if let Some((public_key,)) = result {
            debug!("Found public key for key_id: {}", key_id);
            Ok(Some(public_key))
        } else {
            debug!("No public key found for key_id: {}", key_id);
            Ok(None)
        }
    }

    async fn get_secret_key(&self, key_id: u32) -> KsResult<Option<String>> {
        let result = sqlx::query_as::<_, (String,)>("SELECT secret_key FROM keys WHERE key_id = ?")
            .bind(key_id as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                KsError::Internal(format!(
                    "Failed to query secret key for key_id {key_id}: {e}"
                ))
            })?;

        if let Some((encrypted_secret_key,)) = result {
            trace!("Secret key found in SQLite database");
            // 解密私钥（如果启用了加密）
            let decrypted_secret_key = self.encryptor.decrypt(&encrypted_secret_key)?;
            Ok(Some(decrypted_secret_key))
        } else {
            trace!("Secret key not found in SQLite database");
            Ok(None)
        }
    }

    async fn get_key_record(&self, key_id: u32) -> KsResult<Option<KeyRecord>> {
        let result = sqlx::query_as::<_, (i64, String, i64, i64)>(
            "SELECT key_id, public_key, created_at, expires_at FROM keys WHERE key_id = ?",
        )
        .bind(key_id as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            KsError::Internal(format!(
                "Failed to query key record for key_id {key_id}: {e}"
            ))
        })?;

        if let Some((key_id_db, public_key, created_at, expires_at)) = result {
            debug!("Found key record for key_id: {}", key_id);
            Ok(Some(KeyRecord {
                key_id: key_id_db as u32,
                public_key,
                created_at: created_at as u64,
                expires_at: expires_at as u64,
            }))
        } else {
            debug!("No key record found for key_id: {}", key_id);
            Ok(None)
        }
    }

    async fn get_key_count(&self) -> KsResult<u32> {
        let result = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM keys")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| KsError::Internal(format!("Failed to get key count: {e}")))?;

        Ok(result.0 as u32)
    }

    async fn cleanup_expired_keys(&self) -> KsResult<u32> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let result = sqlx::query("DELETE FROM keys WHERE expires_at > 0 AND expires_at < ?")
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| KsError::Internal(format!("Failed to cleanup expired keys: {e}")))?;

        let deleted = result.rows_affected() as u32;
        if deleted > 0 {
            debug!("Cleaned up {} expired keys", deleted);
        }

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_test_backend() -> SqliteBackend {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let config = SqliteConfig {
            path: db_path.to_string_lossy().to_string(),
        };

        SqliteBackend::new(&config, 3600, crate::crypto::KeyEncryptor::no_encryption())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_sqlite_init() {
        let backend = create_test_backend().await;
        let count = backend.get_key_count().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_generate_and_query() {
        let backend = create_test_backend().await;

        // 生成密钥
        let key_pair = backend.generate_and_store_key().await.unwrap();
        assert!(key_pair.key_id > 0);
        assert!(!key_pair.public_key.is_empty());
        assert!(!key_pair.secret_key.is_empty());

        // 查询公钥
        let public_key = backend.get_public_key(key_pair.key_id).await.unwrap();
        assert_eq!(public_key, Some(key_pair.public_key.clone()));

        // 查询私钥
        let secret_key = backend.get_secret_key(key_pair.key_id).await.unwrap();
        assert_eq!(secret_key, Some(key_pair.secret_key));

        // 查询完整记录
        let record = backend.get_key_record(key_pair.key_id).await.unwrap();
        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.key_id, key_pair.key_id);
        assert_eq!(record.public_key, key_pair.public_key);
    }

    #[tokio::test]
    async fn test_query_nonexistent_key() {
        let backend = create_test_backend().await;

        let public_key = backend.get_public_key(999).await.unwrap();
        assert_eq!(public_key, None);

        let secret_key = backend.get_secret_key(999).await.unwrap();
        assert_eq!(secret_key, None);

        let record = backend.get_key_record(999).await.unwrap();
        assert_eq!(record, None);
    }

    #[tokio::test]
    async fn test_key_count() {
        let backend = create_test_backend().await;

        assert_eq!(backend.get_key_count().await.unwrap(), 0);

        backend.generate_and_store_key().await.unwrap();
        assert_eq!(backend.get_key_count().await.unwrap(), 1);

        backend.generate_and_store_key().await.unwrap();
        assert_eq!(backend.get_key_count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_cleanup_expired_keys() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let config = SqliteConfig {
            path: db_path.to_string_lossy().to_string(),
        };

        // 创建 TTL 为 1 秒的后端
        let backend = SqliteBackend::new(&config, 1, crate::crypto::KeyEncryptor::no_encryption())
            .await
            .unwrap();

        // 生成密钥
        backend.generate_and_store_key().await.unwrap();
        assert_eq!(backend.get_key_count().await.unwrap(), 1);

        // 等待过期
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // 清理过期密钥
        let cleaned = backend.cleanup_expired_keys().await.unwrap();
        assert_eq!(cleaned, 1);
        assert_eq!(backend.get_key_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_zero_ttl_never_expires() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let config = SqliteConfig {
            path: db_path.to_string_lossy().to_string(),
        };

        // TTL 为 0（永不过期）
        let backend = SqliteBackend::new(&config, 0, crate::crypto::KeyEncryptor::no_encryption())
            .await
            .unwrap();

        backend.generate_and_store_key().await.unwrap();
        assert_eq!(backend.get_key_count().await.unwrap(), 1);

        // 清理不应删除永不过期的密钥
        let cleaned = backend.cleanup_expired_keys().await.unwrap();
        assert_eq!(cleaned, 0);
        assert_eq!(backend.get_key_count().await.unwrap(), 1);
    }
}
