//! 密钥存储后端抽象接口
//!
//! 定义了所有存储后端必须实现的统一异步接口

use crate::error::KsResult;
use crate::types::{KeyPair, KeyRecord};
use async_trait::async_trait;

/// 密钥存储后端抽象接口
///
/// 所有存储后端（SQLite, PostgreSQL）都需要实现此 trait
/// 提供统一的异步 API 用于密钥的生成、存储、查询和管理
#[async_trait]
pub trait KeyStorageBackend: Send + Sync {
    /// 初始化存储后端
    ///
    /// 执行必要的初始化操作，如创建表、索引等
    async fn init(&self) -> KsResult<()>;

    /// 生成并存储新的密钥对
    ///
    /// 自动生成椭圆曲线密钥对，存储到后端，并返回包含 key_id 的完整密钥信息
    ///
    /// # Returns
    /// 包含 key_id、public_key 和 secret_key 的密钥对结构
    async fn generate_and_store_key(&self) -> KsResult<KeyPair>;

    /// 根据 key_id 查询公钥
    ///
    /// # Arguments
    /// * `key_id` - 密钥 ID
    ///
    /// # Returns
    /// * `Ok(Some(public_key))` - 找到公钥
    /// * `Ok(None)` - 密钥不存在
    /// * `Err(...)` - 存储错误
    async fn get_public_key(&self, key_id: u32) -> KsResult<Option<String>>;

    /// 根据 key_id 查询私钥
    ///
    /// # Arguments
    /// * `key_id` - 密钥 ID
    ///
    /// # Returns
    /// * `Ok(Some(secret_key))` - 找到私钥
    /// * `Ok(None)` - 密钥不存在
    /// * `Err(...)` - 存储错误
    async fn get_secret_key(&self, key_id: u32) -> KsResult<Option<String>>;

    /// 获取完整的密钥记录（包含元数据）
    ///
    /// # Arguments
    /// * `key_id` - 密钥 ID
    ///
    /// # Returns
    /// * `Ok(Some(record))` - 找到密钥记录
    /// * `Ok(None)` - 密钥不存在
    /// * `Err(...)` - 存储错误
    async fn get_key_record(&self, key_id: u32) -> KsResult<Option<KeyRecord>>;

    /// 获取存储中的密钥总数
    ///
    /// # Returns
    /// 密钥总数（包括过期和未过期的）
    async fn get_key_count(&self) -> KsResult<u32>;

    /// 清理过期的密钥
    ///
    /// 删除所有已过期的密钥记录（expires_at > 0 且 < 当前时间）
    ///
    /// # Returns
    /// 被清理的密钥数量
    async fn cleanup_expired_keys(&self) -> KsResult<u32>;
}
