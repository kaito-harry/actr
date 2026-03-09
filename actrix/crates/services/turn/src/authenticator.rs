//! TURN 认证器
//!
//! 实现 TURN 服务器的认证和授权功能，基于 HMAC-SHA1 时效凭证（coturn --use-auth-secret 兼容格式）。
//!
//! # 认证流程
//!
//! 1. AIS 生成 TurnCredential { username = "<expires_at>:<actor_id>", password = base64(HMAC-SHA1(turn_secret, username)) }
//! 2. 客户端使用 (username, password) 向 TURN 服务器认证
//! 3. TURN 服务器计算 MD5(username:realm:password) 作为 long-term credential key
//! 4. turn_crate 用此 key 验证请求中的 HMAC-SHA1 message integrity

use base64::prelude::*;
use hmac::{Hmac, Mac};
use lru::LruCache;
use once_cell::sync::Lazy;
use sha1::Sha1;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use turn_crate::Error;
use turn_crate::auth::AuthHandler;
use twox_hash::XxHash64;

type HmacSha1 = Hmac<Sha1>;

// 全局 LRU 缓存，用于存储认证密钥
// 缓存键: (username, realm) 的哈希值 (u128)
// 缓存值: MD5(username:realm:password) 的结果 (Vec<u8>)
const AUTH_CACHE_CAPACITY: usize = 4096;

static AUTH_KEY_CACHE: Lazy<Mutex<LruCache<u128, Vec<u8>>>> = Lazy::new(|| {
    let cap = NonZeroUsize::new(AUTH_CACHE_CAPACITY).expect("AUTH_CACHE_CAPACITY must be non-zero");
    Mutex::new(LruCache::new(cap))
});

/// TURN 认证器（HMAC 时效凭证模式）
pub struct Authenticator {
    turn_secret: String,
}

impl Authenticator {
    pub fn new(turn_secret: String) -> Result<Self, Error> {
        platform::recording::info!("TURN 认证器初始化完成 (HMAC 时效凭证模式, LRU 缓存)");
        Ok(Self { turn_secret })
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
        platform::recording::info!("TURN 认证密钥缓存已清空");
    }

    /// 计算 HMAC-SHA1 password（base64 编码）
    ///
    /// `password = base64(HMAC-SHA1(turn_secret, username))`
    fn compute_password(&self, username: &str) -> String {
        let mut mac = HmacSha1::new_from_slice(self.turn_secret.as_bytes())
            .expect("HMAC-SHA1 accepts any key length");
        mac.update(username.as_bytes());
        BASE64_STANDARD.encode(mac.finalize().into_bytes())
    }
}

/// 计算缓存键
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
        platform::recording::debug!(
            "Processing TURN auth: username={}, realm={}, src={}",
            username,
            server_realm,
            src_addr
        );

        // 1️⃣ 缓存命中检查
        let cache_key = compute_cache_key(username, server_realm);
        if let Some(cached) = AUTH_KEY_CACHE
            .lock()
            .expect("auth cache poisoned")
            .get(&cache_key)
            .cloned()
        {
            platform::recording::debug!("TURN 认证缓存命中: username={}", username);
            return Ok(cached);
        }

        // 2️⃣ 解析 username = "<expires_at>:<actor_id>"
        let (expires_at_str, actor_id) = username.split_once(':').ok_or_else(|| {
            platform::recording::warn!(
                "TURN username 格式错误（期望 '<expires_at>:<actor_id>'）: username={}",
                username
            );
            Error::Other("invalid username format".to_string())
        })?;

        let expires_at: u64 = expires_at_str.parse().map_err(|e| {
            platform::recording::warn!(
                "TURN username expires_at 解析失败: username={}, error={}",
                username,
                e
            );
            Error::Other(format!("invalid expires_at in username: {e}"))
        })?;

        // 3️⃣ 检查凭证是否过期
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if expires_at <= now {
            platform::recording::warn!(
                "TURN 凭证已过期: username={}, expires_at={}, now={}",
                username,
                expires_at,
                now
            );
            return Err(Error::Other("TURN credential expired".to_string()));
        }

        // 4️⃣ 计算 HMAC-SHA1 密码
        let password = self.compute_password(username);

        // 5️⃣ 计算 long-term credential key: MD5(username:realm:password)
        let integrity_text = format!("{username}:{server_realm}:{password}");
        let digest = md5::compute(integrity_text.as_bytes());
        let result = digest.to_vec();

        // 6️⃣ 存入缓存
        AUTH_KEY_CACHE
            .lock()
            .expect("auth cache poisoned")
            .put(cache_key, result.clone());

        platform::recording::debug!(
            "TURN 认证成功: actor_id={}, expires_at={}, cache_size={}/{}",
            actor_id,
            expires_at,
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

    fn make_auth() -> Authenticator {
        Authenticator::new("test-secret".to_string()).expect("authenticator should initialize")
    }

    #[test]
    fn test_authenticator_creation() {
        let _auth = make_auth();
    }

    #[test]
    fn test_cache_key_computation() {
        let key1 = compute_cache_key("user1", "realm1");
        let key2 = compute_cache_key("user1", "realm1");
        let key3 = compute_cache_key("user2", "realm1");
        assert_eq!(key1, key2);
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
        let integrity_text = "testuser:testrealm:testpass";
        let result = md5::compute(integrity_text.as_bytes());
        assert_eq!(result.len(), 16);
        let result2 = md5::compute(integrity_text.as_bytes());
        assert_eq!(result.to_vec(), result2.to_vec());
    }

    #[test]
    #[serial]
    fn test_auth_handle_rejects_invalid_username_format() {
        Authenticator::clear_cache();
        let auth = make_auth();
        let src_addr: SocketAddr = "127.0.0.1:3478".parse().expect("valid socket addr");

        let err = auth
            .auth_handle("invalid-no-colon", "actrix.local", src_addr)
            .expect_err("invalid username should be rejected");

        assert!(
            err.to_string().contains("invalid username format"),
            "unexpected error: {err}"
        );
    }

    #[test]
    #[serial]
    fn test_auth_handle_rejects_expired_credential() {
        Authenticator::clear_cache();
        let auth = make_auth();
        let src_addr: SocketAddr = "127.0.0.1:3478".parse().expect("valid socket addr");

        // 已过期的 username（expires_at 在过去）
        let username = "1000000000:test-actor"; // timestamp in the past
        let err = auth
            .auth_handle(username, "actrix.local", src_addr)
            .expect_err("expired credential should be rejected");

        assert!(
            err.to_string().contains("expired"),
            "unexpected error: {err}"
        );
    }

    #[test]
    #[serial]
    fn test_auth_handle_accepts_valid_credential() {
        Authenticator::clear_cache();
        let auth = make_auth();
        let src_addr: SocketAddr = "127.0.0.1:3478".parse().expect("valid socket addr");

        // 未过期的凭证
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expires_at = now + 3600;
        let username = format!("{expires_at}:test-actor");

        let result = auth.auth_handle(&username, "actrix.local", src_addr);
        assert!(result.is_ok(), "valid credential should succeed");
        assert_eq!(result.unwrap().len(), 16, "should return 16-byte MD5 key");
    }

    #[test]
    #[serial]
    fn test_auth_handle_uses_cache() {
        Authenticator::clear_cache();
        let auth = make_auth();
        let username = "non-decodable-user";
        let server_realm = "actrix.local";
        let src_addr: SocketAddr = "127.0.0.1:3478".parse().expect("valid socket addr");
        let expected_key = vec![0xAB; 16];

        let cache_key = compute_cache_key(username, server_realm);
        AUTH_KEY_CACHE
            .lock()
            .expect("auth cache poisoned")
            .put(cache_key, expected_key.clone());

        let result = auth
            .auth_handle(username, server_realm, src_addr)
            .expect("cached value should short-circuit parsing");

        assert_eq!(result, expected_key);
    }
}
