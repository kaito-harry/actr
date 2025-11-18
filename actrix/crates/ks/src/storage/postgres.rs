//! PostgreSQL 存储后端实现
//!
//! 使用 sqlx 提供 PostgreSQL 存储支持

use crate::error::{KsError, KsResult};
use crate::storage::backend::KeyStorageBackend;
use crate::storage::config::PostgresConfig;
use crate::types::{KeyPair, KeyRecord};
use async_trait::async_trait;
use base64::prelude::*;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

/// PostgreSQL 存储后端
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
    key_ttl: u64,
}

impl std::fmt::Debug for PostgresBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresBackend")
            .field("key_ttl", &self.key_ttl)
            .finish()
    }
}

impl PostgresBackend {
    /// 创建新的 PostgreSQL 后端实例
    ///
    /// # Arguments
    /// * `config` - PostgreSQL 配置
    /// * `key_ttl` - 密钥有效期（秒），0 表示永不过期
    pub async fn new(config: &PostgresConfig, key_ttl: u64) -> KsResult<Self> {
        // 构建连接 URL
        let url = format!(
            "postgres://{}:{}@{}:{}/{}",
            config.username, config.password, config.host, config.port, config.database
        );

        // 创建连接池
        let pool = PgPoolOptions::new()
            .max_connections(config.pool_size)
            .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
            .connect(&url)
            .await
            .map_err(|e| KsError::Internal(format!("Failed to connect to PostgreSQL: {e}")))?;

        let backend = Self { pool, key_ttl };

        // 初始化数据库表
        backend.init().await?;

        info!(
            "PostgreSQL storage initialized: host={}:{}, db={}, key_ttl={}s",
            config.host, config.port, config.database, key_ttl
        );

        Ok(backend)
    }
}

#[async_trait]
impl KeyStorageBackend for PostgresBackend {
    async fn init(&self) -> KsResult<()> {
        // 创建密钥表
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS keys (
                key_id SERIAL PRIMARY KEY,
                public_key TEXT NOT NULL,
                secret_key TEXT NOT NULL,
                created_at BIGINT NOT NULL,
                expires_at BIGINT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| KsError::Internal(format!("Failed to create keys table: {e}")))?;

        // 创建索引以提高过期查询性能
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_keys_expires_at ON keys(expires_at) WHERE expires_at > 0",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| KsError::Internal(format!("Failed to create index: {e}")))?;

        debug!("PostgreSQL tables and indexes initialized");
        Ok(())
    }

    async fn generate_and_store_key(&self) -> KsResult<KeyPair> {
        // 生成椭圆曲线密钥对
        let (secret_key, public_key) = ecies::utils::generate_keypair();

        // 编码为 Base64
        let secret_key_b64 = BASE64_STANDARD.encode(secret_key.serialize());
        let public_key_b64 = BASE64_STANDARD.encode(public_key.serialize_compressed());

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

        // 插入密钥并获取自动生成的 key_id
        let row = sqlx::query_as::<_, (i32,)>(
            r#"
            INSERT INTO keys (public_key, secret_key, created_at, expires_at)
            VALUES ($1, $2, $3, $4)
            RETURNING key_id
            "#,
        )
        .bind(&public_key_b64)
        .bind(&secret_key_b64)
        .bind(now)
        .bind(expires_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| KsError::Internal(format!("Failed to insert key: {e}")))?;

        let key_id = row.0 as u32;

        info!(
            "Generated and stored new key pair in PostgreSQL: key_id={}, expires_at={}",
            key_id, expires_at
        );

        Ok(KeyPair {
            key_id,
            secret_key: secret_key_b64,
            public_key: public_key_b64,
        })
    }

    async fn get_public_key(&self, key_id: u32) -> KsResult<Option<String>> {
        let result =
            sqlx::query_scalar::<_, String>("SELECT public_key FROM keys WHERE key_id = $1")
                .bind(key_id as i32)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| {
                    KsError::Internal(format!(
                        "Failed to query public key for key_id {key_id}: {e}"
                    ))
                })?;

        if result.is_some() {
            debug!("Found public key for key_id: {} in PostgreSQL", key_id);
        } else {
            debug!("No public key found for key_id: {} in PostgreSQL", key_id);
        }

        Ok(result)
    }

    async fn get_secret_key(&self, key_id: u32) -> KsResult<Option<String>> {
        let result =
            sqlx::query_scalar::<_, String>("SELECT secret_key FROM keys WHERE key_id = $1")
                .bind(key_id as i32)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| {
                    KsError::Internal(format!(
                        "Failed to query secret key for key_id {key_id}: {e}"
                    ))
                })?;

        if result.is_some() {
            trace!("Secret key found in PostgreSQL database");
        } else {
            trace!("Secret key not found in PostgreSQL database");
        }

        Ok(result)
    }

    async fn get_key_record(&self, key_id: u32) -> KsResult<Option<KeyRecord>> {
        let result = sqlx::query_as::<_, (i32, String, i64, i64)>(
            "SELECT key_id, public_key, created_at, expires_at FROM keys WHERE key_id = $1",
        )
        .bind(key_id as i32)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            KsError::Internal(format!(
                "Failed to query key record for key_id {key_id}: {e}"
            ))
        })?;

        match result {
            Some((id, public_key, created_at, expires_at)) => {
                debug!("Found key record for key_id: {} in PostgreSQL", key_id);
                Ok(Some(KeyRecord {
                    key_id: id as u32,
                    public_key,
                    created_at: created_at as u64,
                    expires_at: expires_at as u64,
                }))
            }
            None => {
                debug!("No key record found for key_id: {} in PostgreSQL", key_id);
                Ok(None)
            }
        }
    }

    async fn get_key_count(&self) -> KsResult<u32> {
        let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM keys")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| KsError::Internal(format!("Failed to get key count: {e}")))?;

        Ok(count as u32)
    }

    async fn cleanup_expired_keys(&self) -> KsResult<u32> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // 删除过期的密钥（expires_at > 0 且 < now）
        let result = sqlx::query("DELETE FROM keys WHERE expires_at > 0 AND expires_at < $1")
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| KsError::Internal(format!("Failed to cleanup expired keys: {e}")))?;

        let deleted_count = result.rows_affected() as u32;

        if deleted_count > 0 {
            info!("Cleaned up {} expired keys from PostgreSQL", deleted_count);
        }

        Ok(deleted_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_test_backend() -> PostgresBackend {
        let config = PostgresConfig {
            host: std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: std::env::var("POSTGRES_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5432),
            database: std::env::var("POSTGRES_DB").unwrap_or_else(|_| "ks_test".to_string()),
            username: std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string()),
            password: std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_string()),
            pool_size: 5,
            max_lifetime_secs: 3600,
        };

        PostgresBackend::new(&config, 3600).await.unwrap()
    }

    async fn cleanup_test_data(backend: &PostgresBackend) {
        sqlx::query("TRUNCATE TABLE keys RESTART IDENTITY")
            .execute(&backend.pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore] // 需要 PostgreSQL 服务器
    async fn test_postgres_init() {
        let backend = create_test_backend().await;
        cleanup_test_data(&backend).await;

        let count = backend.get_key_count().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    #[ignore] // 需要 PostgreSQL 服务器
    async fn test_generate_and_query() {
        let backend = create_test_backend().await;
        cleanup_test_data(&backend).await;

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

        cleanup_test_data(&backend).await;
    }

    #[tokio::test]
    #[ignore] // 需要 PostgreSQL 服务器
    async fn test_query_nonexistent_key() {
        let backend = create_test_backend().await;
        cleanup_test_data(&backend).await;

        let result = backend.get_public_key(99999).await.unwrap();
        assert_eq!(result, None);

        cleanup_test_data(&backend).await;
    }

    #[tokio::test]
    #[ignore] // 需要 PostgreSQL 服务器
    async fn test_key_count() {
        let backend = create_test_backend().await;
        cleanup_test_data(&backend).await;

        assert_eq!(backend.get_key_count().await.unwrap(), 0);

        backend.generate_and_store_key().await.unwrap();
        assert_eq!(backend.get_key_count().await.unwrap(), 1);

        backend.generate_and_store_key().await.unwrap();
        assert_eq!(backend.get_key_count().await.unwrap(), 2);

        cleanup_test_data(&backend).await;
    }

    #[tokio::test]
    #[ignore] // 需要 PostgreSQL 服务器
    async fn test_cleanup_expired_keys() {
        let config = PostgresConfig {
            host: std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: std::env::var("POSTGRES_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5432),
            database: std::env::var("POSTGRES_DB").unwrap_or_else(|_| "ks_test".to_string()),
            username: std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string()),
            password: std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_string()),
            pool_size: 5,
            max_lifetime_secs: 3600,
        };

        // 创建 TTL 为 1 秒的后端
        let backend = PostgresBackend::new(&config, 1).await.unwrap();
        cleanup_test_data(&backend).await;

        // 生成密钥
        backend.generate_and_store_key().await.unwrap();
        assert_eq!(backend.get_key_count().await.unwrap(), 1);

        // 等待过期
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // 清理过期密钥
        let cleaned = backend.cleanup_expired_keys().await.unwrap();
        assert_eq!(cleaned, 1);
        assert_eq!(backend.get_key_count().await.unwrap(), 0);

        cleanup_test_data(&backend).await;
    }

    #[tokio::test]
    #[ignore] // 需要 PostgreSQL 服务器
    async fn test_zero_ttl_never_expires() {
        let config = PostgresConfig {
            host: std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: std::env::var("POSTGRES_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5432),
            database: std::env::var("POSTGRES_DB").unwrap_or_else(|_| "ks_test".to_string()),
            username: std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string()),
            password: std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_string()),
            pool_size: 5,
            max_lifetime_secs: 3600,
        };

        // TTL 为 0（永不过期）
        let backend = PostgresBackend::new(&config, 0).await.unwrap();
        cleanup_test_data(&backend).await;

        backend.generate_and_store_key().await.unwrap();
        assert_eq!(backend.get_key_count().await.unwrap(), 1);

        // 清理不应删除永不过期的密钥
        let cleaned = backend.cleanup_expired_keys().await.unwrap();
        assert_eq!(cleaned, 0);
        assert_eq!(backend.get_key_count().await.unwrap(), 1);

        cleanup_test_data(&backend).await;
    }
}
