//! AId Credential 验证器
//!
//! 负责验证 AId Credential 的 Ed25519 签名，并解码 IdentityClaims。
//! 相比旧的 ECIES 解密方式，新方案使用非对称签名验证：
//! - claims 是明文 proto bytes，由 AIS 的 Ed25519 私钥签名
//! - 验证方只需持有对应的 verifying key（公钥）即可完成验证

use super::error::AidError;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{AIdCredential, IdentityClaims};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};

/// AId Credential 验证器
///
/// 提供纯函数式的 Ed25519 签名验证，无内部状态。
/// verifying_key 由调用方提供，通常来自 AIS 注册响应中的 signing_pubkey 或本地密钥缓存。
pub struct AIdCredentialVerifier;

impl AIdCredentialVerifier {
    /// 验证 AIdCredential 的 Ed25519 签名并返回 IdentityClaims
    ///
    /// # 验证流程
    /// 1. 从 credential.signature 解析 64 字节 Ed25519 签名
    /// 2. 用 verifying_key 对 credential.claims（明文 proto bytes）验签
    /// 3. proto decode IdentityClaims
    /// 4. 检查 expires_at 是否已过期
    ///
    /// # Arguments
    /// * `credential` - 待验证的 AIdCredential，字段为 key_id / claims / signature
    /// * `verifying_key` - Ed25519 公钥，由调用方提供（来自 AIS 注册响应或密钥缓存）
    ///
    /// # Returns
    /// * `Ok(IdentityClaims)` - 验签通过且未过期，返回解码后的身份声明
    /// * `Err(AidError)` - 签名无效、格式错误或 token 已过期
    pub fn verify(
        credential: &AIdCredential,
        verifying_key: &VerifyingKey,
    ) -> Result<IdentityClaims, AidError> {
        crate::recording::debug!(
            "验证 AIdCredential Ed25519 签名：key_id={}, claims_len={}",
            credential.key_id,
            credential.claims.len()
        );

        // 1. 解析 signature（必须恰好 64 bytes）
        let sig_bytes: [u8; 64] = credential.signature[..].try_into().map_err(|_| {
            AidError::InvalidSignature(format!(
                "signature 长度无效：期望 64 bytes，实际 {} bytes",
                credential.signature.len()
            ))
        })?;
        let signature = Signature::from_bytes(&sig_bytes);

        // 2. 验证签名（claims 是被签名的明文 bytes）
        verifying_key
            .verify(&credential.claims[..], &signature)
            .map_err(|e| AidError::InvalidSignature(format!("Ed25519 签名验证失败：{e}")))?;

        // 3. proto decode IdentityClaims
        let claims = IdentityClaims::decode(&credential.claims[..])
            .map_err(|e| AidError::DecodeFailure(format!("IdentityClaims proto 解码失败：{e}")))?;

        // 4. 检查过期
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if claims.expires_at <= now {
            return Err(AidError::Expired);
        }

        crate::recording::debug!(
            "AIdCredential 验证通过：key_id={}, actor_id={}, expires_at={}",
            credential.key_id,
            claims.actor_id,
            claims.expires_at
        );

        Ok(claims)
    }
}
