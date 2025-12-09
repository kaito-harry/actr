//! Realm 验证逻辑
//!
//! 包含 Realm 相关的业务规则验证和检查

use super::model::{Realm, RealmStatus};

/// Realm 验证相关实现
impl Realm {
    /// 检查 Realm 是否存在
    pub async fn exists_by_realm_id(realm_id: u32) -> bool {
        Self::get_by_realm_id(realm_id)
            .await
            .unwrap_or(None)
            .is_some()
    }

    /// 验证 Realm 是否可用（存在、未过期、状态正常）
    ///
    /// 返回 Ok(Realm) 表示 Realm 可用
    /// 返回 Err(msg) 表示 Realm 不可用，附带原因
    pub async fn validate_realm(realm_id: u32) -> Result<Realm, String> {
        let realm = Self::get_by_realm_id(realm_id)
            .await
            .map_err(|e| format!("Failed to query realm: {}", e))?
            .ok_or_else(|| format!("Realm {} not found", realm_id))?;

        if realm.is_expired() {
            return Err(format!("Realm {} has expired", realm_id));
        }

        if realm.status() != RealmStatus::Normal {
            return Err(format!(
                "Realm {} is not in Normal status (current: {})",
                realm_id,
                realm.status()
            ));
        }

        Ok(realm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_expiration_check() {
        let past_time = Utc::now().timestamp() - 3600; // 1 hour ago
        let realm = Realm::new(99999, "Expired App".to_string()).with_expires_at(past_time);

        // Set expired time to test expiration
        assert!(realm.is_expired());

        // Test non-expiring realm
        let realm2 = Realm::new(99998, "Non-Expired App".to_string());
        assert!(!realm2.is_expired());
    }

    #[test]
    fn test_is_active() {
        // Active realm
        let future_time = Utc::now().timestamp() + 3600; // 1 hour in the future
        let realm = Realm::new(11111, "Active App".to_string()).with_expires_at(future_time);
        assert!(realm.is_active());

        // Expired realm
        let past_time = Utc::now().timestamp() - 3600;
        let realm2 = Realm::new(22222, "Expired App".to_string()).with_expires_at(past_time);
        assert!(!realm2.is_active());

        // Suspended realm
        let mut realm3 = Realm::new(33333, "Suspended App".to_string());
        realm3.set_status(RealmStatus::Suspended);
        assert!(!realm3.is_active());
    }
}
