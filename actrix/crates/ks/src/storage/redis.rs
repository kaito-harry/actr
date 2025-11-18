//! Redis 存储后端实现
//!
//! 使用 Redis 提供高性能内存存储支持

use crate::error::{KsError, KsResult};
use crate::storage::backend::KeyStorageBackend;
use crate::storage::config::RedisConfig;
use crate::types::{KeyPair, KeyRecord};
use async_trait::async_trait;
use base64::prelude::*;
use deadpool_redis::{Config, Pool, Runtime};
use redis::AsyncCommands;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

/// Redis 存储后端
///
/// 数据结构设计：
/// - ks:counter:key_id -> Integer (自增计数器)
/// - ks:key:{key_id} -> Hash {public_key, secret_key, created_at, expires_at}
#[derive(Clone)]
pub struct RedisBackend {
    pool: Pool,
    key_ttl: u64,
}

impl std::fmt::Debug for RedisBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisBackend")
            .field("key_ttl", &self.key_ttl)
            .finish()
    }
}

impl RedisBackend {
    /// 创建新的 Redis 后端实例
    ///
    /// # Arguments
    /// * `config` - Redis 配置
    /// * `key_ttl` - 密钥有效期（秒），0 表示永不过期
    pub async fn new(config: &RedisConfig, key_ttl: u64) -> KsResult<Self> {
        let cfg = Config::from_url(&config.url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| KsError::Internal(format!("Failed to create Redis pool: {e}")))?;

        // 测试连接
        let mut conn = pool
            .get()
            .await
            .map_err(|e| KsError::Internal(format!("Failed to connect to Redis: {e}")))?;

        redis::cmd("PING")
            .query_async::<_, String>(&mut *conn)
            .await
            .map_err(|e| KsError::Internal(format!("Redis PING failed: {e}")))?;

        info!(
            "Redis storage initialized: url={}, key_ttl={}s",
            config.url, key_ttl
        );

        Ok(Self { pool, key_ttl })
    }
}

#[async_trait]
impl KeyStorageBackend for RedisBackend {
    async fn init(&self) -> KsResult<()> {
        // Redis 不需要初始化表结构
        debug!("Redis backend initialized (no schema needed)");
        Ok(())
    }

    async fn generate_and_store_key(&self) -> KsResult<KeyPair> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KsError::Internal(format!("Failed to get Redis connection: {e}")))?;

        // 生成椭圆曲线密钥对
        let (secret_key, public_key) = ecies::utils::generate_keypair();

        // 编码为 Base64
        let secret_key_b64 = BASE64_STANDARD.encode(secret_key.serialize());
        let public_key_b64 = BASE64_STANDARD.encode(public_key.serialize_compressed());

        // 原子性生成 key_id
        let key_id: u32 = conn
            .incr("ks:counter:key_id", 1)
            .await
            .map_err(|e| KsError::Internal(format!("Failed to increment key_id counter: {e}")))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let expires_at = if self.key_ttl == 0 {
            0 // 永不过期
        } else {
            now + self.key_ttl
        };

        // 存储到 Hash
        let hash_key = format!("ks:key:{key_id}");

        let _: () = redis::pipe()
            .atomic()
            .hset(&hash_key, "public_key", &public_key_b64)
            .hset(&hash_key, "secret_key", &secret_key_b64)
            .hset(&hash_key, "created_at", now)
            .hset(&hash_key, "expires_at", expires_at)
            .query_async(&mut *conn)
            .await
            .map_err(|e| KsError::Internal(format!("Failed to store key in Redis: {e}")))?;

        // 设置 TTL（如果不是永久密钥）
        if self.key_ttl > 0 {
            let _: () = conn
                .expire(&hash_key, self.key_ttl as i64)
                .await
                .map_err(|e| KsError::Internal(format!("Failed to set TTL: {e}")))?;
        }

        info!(
            "Generated and stored new key pair in Redis: key_id={}, expires_at={}",
            key_id, expires_at
        );

        Ok(KeyPair {
            key_id,
            secret_key: secret_key_b64,
            public_key: public_key_b64,
        })
    }

    async fn get_public_key(&self, key_id: u32) -> KsResult<Option<String>> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KsError::Internal(format!("Failed to get Redis connection: {e}")))?;

        let hash_key = format!("ks:key:{key_id}");

        let result: Option<String> = conn.hget(&hash_key, "public_key").await.map_err(|e| {
            KsError::Internal(format!("Failed to get public_key for key_id {key_id}: {e}"))
        })?;

        if result.is_some() {
            debug!("Found public key for key_id: {} in Redis", key_id);
        } else {
            debug!("No public key found for key_id: {} in Redis", key_id);
        }

        Ok(result)
    }

    async fn get_secret_key(&self, key_id: u32) -> KsResult<Option<String>> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KsError::Internal(format!("Failed to get Redis connection: {e}")))?;

        let hash_key = format!("ks:key:{key_id}");

        let result: Option<String> = conn.hget(&hash_key, "secret_key").await.map_err(|e| {
            KsError::Internal(format!("Failed to get secret_key for key_id {key_id}: {e}"))
        })?;

        if result.is_some() {
            trace!("Secret key found in Redis cache");
        } else {
            trace!("Secret key not found in Redis cache");
        }

        Ok(result)
    }

    async fn get_key_record(&self, key_id: u32) -> KsResult<Option<KeyRecord>> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KsError::Internal(format!("Failed to get Redis connection: {e}")))?;

        let hash_key = format!("ks:key:{key_id}");

        // 获取整个 Hash
        let values: Vec<(String, String)> = conn.hgetall(&hash_key).await.map_err(|e| {
            KsError::Internal(format!("Failed to get key record for key_id {key_id}: {e}"))
        })?;

        if values.is_empty() {
            debug!("No key record found for key_id: {} in Redis", key_id);
            return Ok(None);
        }

        // 解析 Hash 数据
        let mut public_key = None;
        let mut created_at = None;
        let mut expires_at = None;

        for (field, value) in values {
            match field.as_str() {
                "public_key" => public_key = Some(value),
                "created_at" => created_at = value.parse().ok(),
                "expires_at" => expires_at = value.parse().ok(),
                _ => {}
            }
        }

        match (public_key, created_at, expires_at) {
            (Some(pk), Some(ca), Some(ea)) => {
                debug!("Found key record for key_id: {} in Redis", key_id);
                Ok(Some(KeyRecord {
                    key_id,
                    public_key: pk,
                    created_at: ca,
                    expires_at: ea,
                }))
            }
            _ => {
                debug!("Incomplete key record for key_id: {} in Redis", key_id);
                Ok(None)
            }
        }
    }

    async fn get_key_count(&self) -> KsResult<u32> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KsError::Internal(format!("Failed to get Redis connection: {e}")))?;

        // 使用 SCAN 遍历 ks:key:* 模式的 key
        let mut cursor = 0u64;
        let mut count = 0u32;

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("ks:key:*")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await
                .map_err(|e| KsError::Internal(format!("Failed to scan keys: {e}")))?;

            count += keys.len() as u32;
            cursor = new_cursor;

            if cursor == 0 {
                break;
            }
        }

        Ok(count)
    }

    async fn cleanup_expired_keys(&self) -> KsResult<u32> {
        // Redis 会自动清理过期的 key（通过 EXPIRE 设置的 TTL）
        // 这个方法主要用于手动清理，但在 Redis 中通常不需要

        // 为了与其他后端保持一致，我们可以扫描并删除已过期但未被 Redis 自动清理的 key
        // 但由于我们设置了 EXPIRE，Redis 会自动处理，所以这里返回 0

        debug!("Redis automatically handles key expiration via TTL");
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_redis_url() -> String {
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/0".to_string())
    }

    async fn create_test_backend() -> RedisBackend {
        let config = RedisConfig {
            url: get_redis_url(),
            pool_size: 5,
            timeout_ms: 5000,
        };

        RedisBackend::new(&config, 3600).await.unwrap()
    }

    async fn cleanup_test_data(backend: &RedisBackend) {
        let mut conn = backend.pool.get().await.unwrap();
        let _: () = redis::cmd("FLUSHDB").query_async(&mut *conn).await.unwrap();
    }

    #[tokio::test]
    #[ignore] // 需要 Redis 服务器
    async fn test_redis_init() {
        let backend = create_test_backend().await;
        cleanup_test_data(&backend).await;

        let count = backend.get_key_count().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    #[ignore] // 需要 Redis 服务器
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
    #[ignore] // 需要 Redis 服务器
    async fn test_query_nonexistent_key() {
        let backend = create_test_backend().await;
        cleanup_test_data(&backend).await;

        let result = backend.get_public_key(99999).await.unwrap();
        assert_eq!(result, None);

        cleanup_test_data(&backend).await;
    }

    #[tokio::test]
    #[ignore] // 需要 Redis 服务器
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
    #[ignore] // 需要 Redis 服务器
    async fn test_ttl_expiration() {
        let config = RedisConfig {
            url: get_redis_url(),
            pool_size: 5,
            timeout_ms: 5000,
        };

        // 创建 TTL 为 2 秒的后端
        let backend = RedisBackend::new(&config, 2).await.unwrap();
        cleanup_test_data(&backend).await;

        // 生成密钥
        let key_pair = backend.generate_and_store_key().await.unwrap();
        assert_eq!(backend.get_key_count().await.unwrap(), 1);

        // 等待过期
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Redis 应该自动删除过期的 key
        let result = backend.get_public_key(key_pair.key_id).await.unwrap();
        assert_eq!(result, None);

        cleanup_test_data(&backend).await;
    }
}
