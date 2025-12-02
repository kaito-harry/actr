//! TURN 认证器
//!
//! 实现 TURN 服务器的认证和授权功能，带 LRU 缓存优化

use actr_protocol::turn::{Claims, Token};
use lru::LruCache;
use once_cell::sync::Lazy;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use tracing::{debug, error, warn};
use turn_crate::Error;
use turn_crate::auth::AuthHandler;
use twox_hash::XxHash64;

/// TURN 认证器
pub struct Authenticator;

impl Authenticator {
    pub fn new() -> Result<Self, Error> {
        tracing::info!("TURN 认证器初始化完成 (启用 LRU 缓存)");
        Ok(Self)
    }

    /// 获取缓存统计信息（用于监控和调试）
    pub fn cache_stats() -> (usize, usize) {
        let cache = AUTH_KEY_CACHE.lock().expect("auth cache poisoned");
        (cache.len(), cache.cap().get())
    }

    /// 清空缓存（用于测试或手动重置）
    #[allow(dead_code)]
    pub fn clear_cache() {
        let mut cache = AUTH_KEY_CACHE.lock().expect("auth cache poisoned");
        cache.clear();
        tracing::info!("TURN 认证密钥缓存已清空");
    }
}

// 全局 LRU 缓存，用于存储认证密钥
// 缓存键: (username, realm) 的哈希值 (u128)
// 缓存值: MD5(username:realm:psk) 的结果 (Vec<u8>)
// 容量: 4096 个条目
// 策略: LRU (Least Recently Used)
const AUTH_CACHE_CAPACITY: usize = 4096;

static AUTH_KEY_CACHE: Lazy<Mutex<LruCache<u128, Vec<u8>>>> = Lazy::new(|| {
    let cap = NonZeroUsize::new(AUTH_CACHE_CAPACITY).expect("AUTH_CACHE_CAPACITY must be non-zero");
    Mutex::new(LruCache::new(cap))
});

/// 计算缓存键
///
/// 使用 XxHash64 分别对 username 和 realm 进行哈希，
/// 然后组合成 u128 作为缓存键
#[inline]
fn compute_cache_key(username: &str, realm: &str) -> u128 {
    let mut h1 = XxHash64::with_seed(0);
    h1.write(username.as_bytes());
    let k1 = h1.finish();

    let mut h2 = XxHash64::with_seed(0);
    h2.write(realm.as_bytes());
    let k2 = h2.finish();

    ((k1 as u128) << 64) | (k2 as u128)
}

impl AuthHandler for Authenticator {
    fn auth_handle(
        &self,
        username: &str,
        server_realm: &str,
        src_addr: SocketAddr,
    ) -> Result<Vec<u8>, Error> {
        debug!(
            "处理 TURN 认证请求: username={}, realm={}, src={}",
            username, server_realm, src_addr
        );

        // 1️⃣ 首先尝试缓存命中（仅基于 username + realm，无需解析 Claims）
        let cache_key = compute_cache_key(username, server_realm);
        if let Some(cached) = AUTH_KEY_CACHE
            .lock()
            .expect("auth cache poisoned")
            .get(&cache_key)
            .cloned()
        {
            debug!("TURN 认证缓存命中: username={}", username);
            return Ok(cached);
        }

        // 2️⃣ 缓存未命中，解析 Claims 获取 PSK
        let claims: Claims = serde_json::from_str(username).map_err(|e| {
            warn!("无法解析 Claims: username={}, error={}", username, e);
            Error::Other(format!("Failed to parse claims: {e}"))
        })?;

        // 3️⃣ 从 Claims 解密获取 Token
        let token: Token = match claims.get_token() {
            Ok(token) => token,
            Err(e) => {
                error!(
                    "无法解密 token: tenant_id={}, key_id={}, error={}",
                    claims.tenant_id, claims.key_id, e
                );
                return Err(Error::Other(format!("Failed to decrypt token: {e}")));
            }
        };

        // 4️⃣ 从 Token 获取真实的 PSK
        let psk = token.psk;

        debug!(
            "成功解密 token: tenant_id={}, act_type={}, psk_len={}",
            token.tenant_id,
            token.act_type,
            psk.len()
        );

        // 5️⃣ 计算认证密钥: MD5(username:realm:psk)
        let integrity_text = format!("{username}:{server_realm}:{psk}");
        debug!("TURN 认证完整性文本长度: {}", integrity_text.len());

        let digest = md5::compute(integrity_text.as_bytes());
        let result = digest.to_vec();

        // 6️⃣ 存入缓存
        AUTH_KEY_CACHE
            .lock()
            .expect("auth cache poisoned")
            .put(cache_key, result.clone());

        debug!(
            "TURN 认证成功: username={}, cache_size={}/{}",
            username,
            Self::cache_stats().0,
            Self::cache_stats().1
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_authenticator_creation() {
        let _auth = Authenticator::new().expect("Failed to create authenticator");
    }

    #[test]
    fn test_cache_key_computation() {
        let key1 = compute_cache_key("user1", "realm1");
        let key2 = compute_cache_key("user1", "realm1");
        let key3 = compute_cache_key("user2", "realm1");

        // 相同输入应该产生相同的缓存键
        assert_eq!(key1, key2);

        // 不同输入应该产生不同的缓存键
        assert_ne!(key1, key3);
    }

    #[test]
    #[serial]
    fn test_cache_stats() {
        Authenticator::clear_cache();
        let (size, capacity) = Authenticator::cache_stats();

        assert_eq!(size, 0);
        assert_eq!(capacity, AUTH_CACHE_CAPACITY);
    }

    #[test]
    fn test_md5_computation() {
        let integrity_text = "testuser:testrealm:testpsk";
        let result = md5::compute(integrity_text.as_bytes());

        // 验证 MD5 结果长度为 16 字节
        assert_eq!(result.len(), 16);

        // 验证结果一致性
        let result2 = md5::compute(integrity_text.as_bytes());
        assert_eq!(result.to_vec(), result2.to_vec());
    }
}
