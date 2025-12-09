//! Realm 核心数据结构
//!
//! 定义 Realm 实体的核心数据结构和基础方法

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::str::FromStr;
use strum::{Display, EnumString};

/// Realm 状态枚举
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, Display, EnumString)]
pub enum RealmStatus {
    #[default]
    Normal,
    Suspended,
    Terminated,
}

/// Realm 是用于分离不同应用程序资源的虚拟概念。
///
#[derive(Debug, Clone, Serialize, Deserialize, Default, FromRow)]
pub struct Realm {
    pub rowid: Option<u32>,

    // 基础字段
    pub realm_id: u32,
    pub name: String,

    // 状态和过期时间
    pub status: String,          // 存储 RealmStatus 的字符串形式
    pub expires_at: Option<i64>, // Unix timestamp (seconds)

    // 元数据字段
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
}

impl Realm {
    pub fn new(realm_id: u32, name: String) -> Self {
        let now = Utc::now().timestamp();
        Self {
            rowid: None,
            realm_id,
            name,
            status: RealmStatus::Normal.to_string(),
            expires_at: None,
            created_at: Some(now),
            updated_at: Some(now),
        }
    }

    pub fn with_expires_at(mut self, expires_at: i64) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn status(&self) -> RealmStatus {
        RealmStatus::from_str(&self.status).unwrap_or_default()
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            let now = Utc::now().timestamp();
            now > expires_at
        } else {
            false // 没有设置过期时间则永不过期
        }
    }

    pub fn is_active(&self) -> bool {
        self.status() == RealmStatus::Normal && !self.is_expired()
    }

    // Setter methods for admin operations
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_status(&mut self, status: RealmStatus) {
        self.status = status.to_string();
    }

    pub fn set_expires_at(&mut self, expires_at: Option<i64>) {
        self.expires_at = expires_at;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_realm_creation() {
        let realm = Realm::new(12345, "test_name".to_string());

        assert_eq!(realm.realm_id, 12345u32);
        assert_eq!(realm.name, "test_name");
        assert_eq!(realm.status(), RealmStatus::Normal);
        assert!(realm.created_at.is_some());
        assert!(realm.updated_at.is_some());
    }

    #[test]
    fn test_realm_with_expires_at() {
        let future_time = Utc::now().timestamp() + 3600; // 1 hour from now
        let realm = Realm::new(54321, "Auth App".to_string()).with_expires_at(future_time);

        assert_eq!(realm.name, "Auth App");
        assert_eq!(realm.expires_at, Some(future_time));
        assert!(!realm.is_expired());
        assert!(realm.is_active());
    }

    #[test]
    fn test_realm_expired() {
        let past_time = Utc::now().timestamp() - 3600; // 1 hour ago
        let realm = Realm::new(11111, "Expired App".to_string()).with_expires_at(past_time);

        assert!(realm.is_expired());
        assert!(!realm.is_active());
    }

    #[test]
    fn test_realm_status() {
        let mut realm = Realm::new(22222, "Status Test".to_string());
        assert_eq!(realm.status(), RealmStatus::Normal);

        realm.set_status(RealmStatus::Suspended);
        assert_eq!(realm.status(), RealmStatus::Suspended);
        assert!(!realm.is_active());
    }
}
