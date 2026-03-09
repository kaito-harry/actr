//! AId Credential 验证器
//!
//! 基于 Ed25519 签名验证 AId Credential：
//! - claims 是明文 proto bytes，由 AIS 的 Ed25519 私钥签名
//! - 验证方通过 `key_cache` 按 key_id 查找对应 VerifyingKey 完成验证

use super::error::AidError;
use super::verifier::AIdCredentialVerifier;
use crate::aid::key_cache::KeyCache;
use actr_protocol::{AIdCredential, IdentityClaims};
use once_cell::sync::OnceCell;
use std::sync::Arc;

static KEY_CACHE: OnceCell<Arc<KeyCache>> = OnceCell::new();

/// AId Token 验证器 - 静态方法验证 AIdCredential（Ed25519 签名）
pub struct AIdCredentialValidator;

impl AIdCredentialValidator {
    /// 初始化全局 KeyCache 实例（通常在服务启动时调用一次）
    ///
    /// 可多次调用，首次调用有效；后续调用被忽略（幂等）。
    pub async fn init(sqlite_path: &std::path::Path) -> Result<(), AidError> {
        if KEY_CACHE.get().is_some() {
            return Ok(());
        }
        let cache_db = sqlite_path.join("signaling_key_cache.db");
        let cache = KeyCache::new(&cache_db).await?;
        // Best-effort set; concurrent callers may race but both produce valid caches
        let _ = KEY_CACHE.set(Arc::new(cache));
        Ok(())
    }

    /// 获取全局 KeyCache 实例
    fn get_cache() -> Result<Arc<KeyCache>, AidError> {
        KEY_CACHE.get().cloned().ok_or_else(|| {
            AidError::InvalidFormat
        })
    }

    /// 校验 AIdCredential：Ed25519 签名验证 + realm_id 检查
    ///
    /// # Returns
    /// `Ok((IdentityClaims, false))` — Ed25519 模式无容忍期概念，第二个值恒为 `false`
    pub async fn check(
        credential: &AIdCredential,
        realm_id: u32,
    ) -> Result<(IdentityClaims, bool), AidError> {
        let cache = Self::get_cache()?;
        let key_id = credential.key_id;

        let (verifying_key, _expires_at) = cache
            .get_cached_key(key_id)
            .await?
            .ok_or_else(|| {
                AidError::InvalidFormat // key_id not found in cache
            })?;

        let claims = AIdCredentialVerifier::verify(credential, &verifying_key)?;

        if claims.realm_id != realm_id {
            return Err(AidError::InvalidFormat);
        }

        Ok((claims, false))
    }

    /// 返回 key_id 对应的 Ed25519 公钥字节（用于 GetSigningKeyRequest 响应）
    pub async fn get_key_bytes(key_id: u32) -> Result<Option<Vec<u8>>, AidError> {
        let cache = Self::get_cache()?;
        let result = cache.get_cached_key(key_id).await?;
        Ok(result.map(|(verifying_key, _expires_at)| verifying_key.as_bytes().to_vec()))
    }

    /// 同步校验（用于非 async 上下文，如 TURN 认证）
    pub fn check_sync(
        credential: &AIdCredential,
        realm_id: u32,
    ) -> Result<IdentityClaims, AidError> {
        tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::try_current().map_err(|_| AidError::InvalidFormat)?;
            let (claims, _) = handle.block_on(Self::check(credential, realm_id))?;
            Ok(claims)
        })
    }
}
