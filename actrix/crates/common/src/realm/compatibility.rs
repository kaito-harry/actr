//! Realm 兼容性方法
//!
//! 提供与原有 Realm 查询兼容的API

use super::error::RealmError;
use super::model::Realm;

/// 兼容性方法
impl Realm {
    /// 获取所有 Realm
    pub async fn get_all_realms() -> Result<Vec<Realm>, RealmError> {
        Self::get_all().await
    }

    /// 根据 realm_id 获取 Realm
    pub async fn get_realm(realm_id: u32) -> Result<Option<Realm>, RealmError> {
        Self::get_by_realm_id(realm_id).await
    }
}
