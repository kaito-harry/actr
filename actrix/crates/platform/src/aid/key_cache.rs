//! AIS 签名公钥缓存
//!
//! 为验证器提供本地 SQLite 缓存，避免重复从 AIS 注册响应中重新获取 Ed25519 公钥。
//! 缓存项以 key_id 为索引，存储对应的 Ed25519 verifying key（32 bytes）和过期时间。

use crate::aid::credential::error::AidError;
use base64::prelude::*;
use ed25519_dalek::VerifyingKey;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info};

/// AIS 签名公钥缓存管理器
///
/// 缓存 AIS Ed25519 verifying key，以 key_id 为索引。
/// 用于 signaling 等服务在验证 AIdCredential 时按 key_id 查找对应公钥。
#[derive(Debug, Clone)]
pub struct KeyCache {
    pool: SqlitePool,
    last_cleanup_time: Arc<Mutex<u64>>,
}

impl KeyCache {
    /// 创建新的密钥缓存实例并初始化 SQLite 表
    pub async fn new<P: AsRef<Path>>(cache_db_file: P) -> Result<Self, AidError> {
        let database_url = format!("sqlite:{}", cache_db_file.as_ref().display());
        let options = SqliteConnectOptions::from_str(&database_url)
            .map_err(|e| AidError::DecodeFailure(format!("invalid database URL: {e}")))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(|e| {
                AidError::DecodeFailure(format!("failed to open key cache database: {e}"))
            })?;

        let cache = Self {
            pool,
            last_cleanup_time: Arc::new(Mutex::new(0)),
        };

        cache.init_tables().await?;

        info!(
            "AIS 公钥缓存初始化完成，数据库：{}",
            cache_db_file.as_ref().display()
        );
        Ok(cache)
    }

    /// 初始化缓存数据库表
    async fn init_tables(&self) -> Result<(), AidError> {
        // key_cache 表：存储 Ed25519 verifying key（base64 编码，32 bytes）和过期时间
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS key_cache (
                key_id     INTEGER PRIMARY KEY,
                pubkey     TEXT NOT NULL,
                cached_at  INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| AidError::DecodeFailure(format!("failed to create key_cache table: {e}")))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_cache_expires_at ON key_cache(expires_at)")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                AidError::DecodeFailure(format!("failed to create key_cache index: {e}"))
            })?;

        debug!("AIS 公钥缓存数据库表已就绪");
        Ok(())
    }

    /// 从缓存中获取 Ed25519 verifying key
    ///
    /// 返回 `(VerifyingKey, expires_at)`；若缓存不存在或已过期则返回 `None`。
    pub async fn get_cached_key(
        &self,
        key_id: u32,
    ) -> Result<Option<(VerifyingKey, u64)>, AidError> {
        self.maybe_cleanup().await;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let result =
            sqlx::query("SELECT pubkey, expires_at FROM key_cache WHERE key_id = ?1")
                .bind(key_id as i64)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| {
                    error!(key_id, "查询 AIS 公钥缓存失败：{}", e);
                    AidError::DecodeFailure(format!("key cache query error: {e}"))
                })?;

        match result {
            Some(row) => {
                let pubkey_b64: String = row.try_get("pubkey").map_err(|e| {
                    AidError::DecodeFailure(format!("failed to read pubkey column: {e}"))
                })?;
                let expires_at: i64 = row.try_get("expires_at").map_err(|e| {
                    AidError::DecodeFailure(format!("failed to read expires_at column: {e}"))
                })?;

                // 缓存条目已过期，删除并返回 None
                if expires_at > 0 && (expires_at as u64) <= now {
                    debug!(key_id, "AIS 公钥缓存已过期，删除");
                    let _ = sqlx::query("DELETE FROM key_cache WHERE key_id = ?1")
                        .bind(key_id as i64)
                        .execute(&self.pool)
                        .await;
                    return Ok(None);
                }

                // base64 解码 → 32 bytes → VerifyingKey
                let pubkey_bytes = BASE64_STANDARD
                    .decode(&pubkey_b64)
                    .map_err(|e| {
                        AidError::DecodeFailure(format!("failed to base64 decode pubkey: {e}"))
                    })?;

                let pubkey_array: [u8; 32] = pubkey_bytes.try_into().map_err(|_| {
                    AidError::DecodeFailure(
                        "pubkey 长度无效，期望 32 bytes（Ed25519 VerifyingKey）".to_string(),
                    )
                })?;

                let verifying_key = VerifyingKey::from_bytes(&pubkey_array).map_err(|e| {
                    AidError::DecodeFailure(format!("invalid Ed25519 verifying key: {e}"))
                })?;

                debug!(key_id, "命中 AIS 公钥缓存");
                Ok(Some((verifying_key, expires_at as u64)))
            }
            None => {
                debug!(key_id, "AIS 公钥缓存未命中");
                Ok(None)
            }
        }
    }

    /// 将 Ed25519 verifying key 写入缓存
    ///
    /// `expires_at` 为 Unix 秒时间戳，传 0 表示永不过期。
    pub async fn cache_key(
        &self,
        key_id: u32,
        verifying_key: &VerifyingKey,
        expires_at: u64,
    ) -> Result<(), AidError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let pubkey_b64 = BASE64_STANDARD.encode(verifying_key.as_bytes());

        sqlx::query(
            "REPLACE INTO key_cache (key_id, pubkey, cached_at, expires_at) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(key_id as i64)
        .bind(&pubkey_b64)
        .bind(now as i64)
        .bind(expires_at as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| AidError::DecodeFailure(format!("failed to write key cache: {e}")))?;

        debug!(key_id, expires_at, "AIS 公钥已写入缓存");
        Ok(())
    }

    /// 清理所有已过期的缓存条目
    pub async fn cleanup_expired_keys(&self) -> Result<u32, AidError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let result =
            sqlx::query("DELETE FROM key_cache WHERE expires_at > 0 AND expires_at < ?1")
                .bind(now as i64)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    AidError::DecodeFailure(format!("failed to cleanup expired keys: {e}"))
                })?;

        let deleted = result.rows_affected() as u32;
        if deleted > 0 {
            info!("AIS 公钥缓存：已清理 {} 条过期记录", deleted);
        }
        Ok(deleted)
    }

    /// 返回缓存中的条目总数（用于监控/调试）
    pub async fn get_cached_key_count(&self) -> Result<u32, AidError> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM key_cache")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                AidError::DecodeFailure(format!("failed to count cached keys: {e}"))
            })?;

        let count: i64 = row
            .try_get("count")
            .map_err(|e| AidError::DecodeFailure(format!("failed to read count: {e}")))?;

        Ok(count as u32)
    }

    /// 内部：每小时触发一次过期清理
    async fn maybe_cleanup(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let should_cleanup = {
            let last = self.last_cleanup_time.lock().unwrap();
            now.saturating_sub(*last) >= 3600
        };

        if should_cleanup {
            debug!("触发 AIS 公钥缓存定时清理");
            match self.cleanup_expired_keys().await {
                Ok(n) if n > 0 => info!("AIS 公钥缓存清理完成，删除 {} 条", n),
                Ok(_) => {}
                Err(e) => error!("AIS 公钥缓存清理失败：{}", e),
            }
            let mut last = self.last_cleanup_time.lock().unwrap();
            *last = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use tempfile::tempdir;

    fn generate_test_key() -> VerifyingKey {
        let signing_key = SigningKey::generate(&mut OsRng);
        signing_key.verifying_key()
    }

    #[tokio::test]
    async fn test_cache_creation() {
        let temp_dir = tempdir().unwrap();
        let cache = KeyCache::new(temp_dir.path().join("test.db")).await;
        assert!(cache.is_ok());
    }

    #[tokio::test]
    async fn test_key_caching_and_retrieval() {
        let temp_dir = tempdir().unwrap();
        let cache = KeyCache::new(temp_dir.path().join("test.db")).await.unwrap();

        let verifying_key = generate_test_key();
        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;

        cache.cache_key(1, &verifying_key, expires_at).await.unwrap();
        assert_eq!(cache.get_cached_key_count().await.unwrap(), 1);

        let cached = cache.get_cached_key(1).await.unwrap();
        assert!(cached.is_some());
        let (retrieved_key, retrieved_expires) = cached.unwrap();
        assert_eq!(verifying_key.as_bytes(), retrieved_key.as_bytes());
        assert_eq!(expires_at, retrieved_expires);
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let temp_dir = tempdir().unwrap();
        let cache = KeyCache::new(temp_dir.path().join("test.db")).await.unwrap();

        let verifying_key = generate_test_key();
        // 设置已过期的时间戳
        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(1);

        cache.cache_key(1, &verifying_key, expires_at).await.unwrap();
        // 过期条目应返回 None
        let cached = cache.get_cached_key(1).await.unwrap();
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_cache_cleanup() {
        let temp_dir = tempdir().unwrap();
        let cache = KeyCache::new(temp_dir.path().join("test.db")).await.unwrap();

        let verifying_key = generate_test_key();
        let expired_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(1);

        cache.cache_key(1, &verifying_key, expired_at).await.unwrap();
        assert_eq!(cache.get_cached_key_count().await.unwrap(), 1);

        let cleaned = cache.cleanup_expired_keys().await.unwrap();
        assert_eq!(cleaned, 1);
        assert_eq!(cache.get_cached_key_count().await.unwrap(), 0);
    }
}
