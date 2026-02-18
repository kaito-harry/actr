//! TURN 认证器
//!
//! 实现 TURN 服务器的认证和授权功能，带 LRU 缓存优化

use actr_protocol::AIdCredential;
use actr_protocol::turn::Claims;
use actrix_common::aid::credential::validator::AIdCredentialValidator;
use actrix_common::realm::Realm as RealmEntity;
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
            "Processing TURN authentication request: username={:?}, realm={}, src={}",
            username.as_bytes(),
            server_realm,
            src_addr
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

        // 2️⃣ 缓存未命中，解析 Claims 获取 key_id
        let claims = Claims::decode(username).map_err(|e| {
            warn!(
                "Failed to parse claims: username={:?}, error={}",
                username.as_bytes(),
                e
            );
            Error::Other(format!("Failed to parse claims: {e}"))
        })?;

        // 3️⃣ Use AIdCredentialValidator to decrypt and verify the claims
        let credential = AIdCredential {
            encrypted_token: claims.token.clone(),
            token_key_id: claims.key_id,
        };

        let identity_claims = AIdCredentialValidator::check_sync(&credential, claims.realm_id)
            .map_err(|e| {
                error!(
                    "Failed to decrypt or verify claims: realm_id={}, key_id={}, error={}",
                    claims.realm_id, claims.key_id, e
                );
                Error::Other(format!("Failed to check credential: {e}"))
            })?;

        // 4️⃣ 验证 Realm 是否存在、未过期、状态正常
        if let Err(e) = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::try_current()
                .map_err(|_| "Not in tokio runtime context")?;
            handle.block_on(async { RealmEntity::validate_realm(identity_claims.realm_id).await })
        }) {
            warn!(
                "⚠️  TURN 认证 realm 验证失败: realm_id={}, actor_id={}, error={}",
                identity_claims.realm_id, identity_claims.actor_id, e
            );
            return Err(Error::Other(format!("Realm validation failed: {e}")));
        }

        let psk = identity_claims.psk;

        // 5️⃣ 计算认证密钥: MD5(username:realm:psk)
        // transform psk to hex string for MD5 calculation
        let psk_hex = hex::encode(&psk);
        let integrity_text = format!("{username}:{server_realm}:{psk_hex}");

        let digest = md5::compute(integrity_text.as_bytes());
        let result = digest.to_vec();

        // 6️⃣ 存入缓存
        AUTH_KEY_CACHE
            .lock()
            .expect("auth cache poisoned")
            .put(cache_key, result.clone());

        debug!(
            "TURN authentication successful: realm_id={}, actor_id={}, cache_size={}/{}",
            identity_claims.realm_id,
            identity_claims.actor_id,
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
    use std::net::SocketAddr;

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

    #[test]
    #[serial]
    fn test_auth_handle_rejects_invalid_claims() {
        Authenticator::clear_cache();
        let auth = Authenticator::new().expect("authenticator should initialize");
        let src_addr: SocketAddr = "127.0.0.1:3478".parse().expect("valid socket addr");

        let err = auth
            .auth_handle("invalid-claims-format", "actor-rtc.local", src_addr)
            .expect_err("invalid claims should be rejected");

        assert!(
            err.to_string().contains("Failed to parse claims")
                || err.to_string().contains("Failed to check credential"),
            "unexpected error: {err}"
        );
        assert_eq!(
            Authenticator::cache_stats().0,
            0,
            "invalid claims should not populate cache"
        );
    }

    #[test]
    #[serial]
    fn test_auth_handle_uses_cached_key_before_claim_decode() {
        Authenticator::clear_cache();
        let auth = Authenticator::new().expect("authenticator should initialize");
        let username = "non-decodable-user";
        let server_realm = "actor-rtc.local";
        let src_addr: SocketAddr = "127.0.0.1:3478".parse().expect("valid socket addr");
        let expected_key = vec![0xAB; 16];

        let cache_key = compute_cache_key(username, server_realm);
        AUTH_KEY_CACHE
            .lock()
            .expect("auth cache poisoned")
            .put(cache_key, expected_key.clone());

        let result = auth
            .auth_handle(username, server_realm, src_addr)
            .expect("cached value should short-circuit claim decode");

        assert_eq!(result, expected_key);
    }
}
