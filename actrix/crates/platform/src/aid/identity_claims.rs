//! Identity Claims 结构
//!
//! 用于 AIS (Actor Identity Service) 的身份声明

use serde::{Deserialize, Serialize};

/// Identity Claims - 用于 AIS 的身份验证
///
/// 此结构体从 AIdCredential 中解密得到，包含用户的身份信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityClaims {
    /// Realm ID (安全域标识符)
    pub realm_id: u32,

    /// Actor ID 字符串表示
    /// 格式: {serial_number_hex}@{realm_id}/{manufacturer}:{name}[:{version}]
    /// 示例: "fed02d3f000000@12345/apple:user:1"
    pub actor_id: String,

    /// Token 过期时间 (Unix timestamp, seconds)
    pub expr_time: u64,

    /// Pre-shared key (PSK) for TURN authentication
    /// 256-bit (32 bytes) pre-shared key used for TURN server authentication
    pub psk: Vec<u8>,
}

impl IdentityClaims {
    /// 创建新的 IdentityClaims
    pub fn new(realm_id: u32, actor_id: String, expr_time: u64, psk: Vec<u8>) -> Self {
        Self {
            realm_id,
            actor_id,
            expr_time,
            psk,
        }
    }

    /// 从 actr_protocol::ActrId 创建 IdentityClaims
    pub fn from_actr_id(actr_id: &actr_protocol::ActrId, expr_time: u64, psk: Vec<u8>) -> Self {
        Self {
            realm_id: actr_id.realm.realm_id,
            actor_id: actr_id.to_string_repr(),
            expr_time,
            psk,
        }
    }

    /// 检查 Token 是否过期
    pub fn is_expired(&self) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now > self.expr_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_identity_claims_creation() {
        let psk = vec![0u8; 32];
        let claims = IdentityClaims::new(
            12345,
            "1a2b3c@12345/apple:user:1".to_string(),
            1730614800,
            psk.clone(),
        );

        assert_eq!(claims.realm_id, 12345);
        assert_eq!(claims.actor_id, "1a2b3c@12345/apple:user:1");
        assert_eq!(claims.expr_time, 1730614800);
        assert_eq!(claims.psk, psk);
    }

    #[test]
    fn test_identity_claims_expiration() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let psk = vec![0u8; 32];

        // 未过期
        let valid_claims =
            IdentityClaims::new(1, "123@1/acme:test:1".to_string(), now + 3600, psk.clone());
        assert!(!valid_claims.is_expired());

        // 已过期
        let expired_claims = IdentityClaims::new(1, "123@1/acme:test:1".to_string(), now - 1, psk);
        assert!(expired_claims.is_expired());
    }
}
