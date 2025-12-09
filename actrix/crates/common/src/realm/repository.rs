//! Realm 数据库操作
//!
//! 包含所有与 Realm 数据持久化相关的CRUD操作

use chrono::Utc;

use super::error::RealmError;
use super::model::Realm;
use crate::storage::db::get_database;

/// Realm 数据库操作实现
impl Realm {
    /// 保存 Realm 到数据库
    /// 如果是新 Realm 则插入，如果已存在提示已存在
    pub async fn save(&mut self) -> Result<u32, RealmError> {
        let now = Utc::now().timestamp();
        let db = get_database();
        let pool = db.get_pool();

        if self.rowid.is_none() {
            // 检查是否已存在相同的 realm_id（应该全局唯一）
            let exists: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM realm WHERE realm_id = ?")
                .bind(self.realm_id)
                .fetch_one(pool)
                .await?;

            if exists.0 > 0 {
                return Err(RealmError::DatabaseError(
                    "UNIQUE constraint failed: realm.realm_id".to_string(),
                ));
            }

            self.created_at = Some(now);
            self.updated_at = Some(now);

            // 插入新记录
            let result = sqlx::query(
                "INSERT INTO realm (realm_id, name, status, expires_at, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(self.realm_id)
            .bind(&self.name)
            .bind(&self.status)
            .bind(self.expires_at)
            .bind(self.created_at)
            .bind(self.updated_at)
            .execute(pool)
            .await?;

            let new_rowid = result.last_insert_rowid().try_into().unwrap();
            self.rowid = Some(new_rowid);
            Ok(new_rowid)
        } else {
            self.updated_at = Some(now);

            // 更新现有记录
            sqlx::query(
                "UPDATE realm SET realm_id = ?, name = ?, status = ?, expires_at = ?, updated_at = ?
                 WHERE rowid = ?",
            )
            .bind(self.realm_id)
            .bind(&self.name)
            .bind(&self.status)
            .bind(self.expires_at)
            .bind(self.updated_at)
            .bind(self.rowid)
            .execute(pool)
            .await?;

            self.rowid.ok_or_else(|| {
                RealmError::DatabaseError("Realm rowid is missing after update".to_string())
            })
        }
    }

    pub async fn get_all() -> Result<Vec<Realm>, RealmError> {
        let db = get_database();
        let pool = db.get_pool();

        let realms = sqlx::query_as::<_, Realm>(
            "SELECT rowid, realm_id, name, status, expires_at, created_at, updated_at
             FROM realm",
        )
        .fetch_all(pool)
        .await?;

        tracing::info!("获取所有 Realm: {:?}", realms);
        Ok(realms)
    }

    pub async fn delete_instance(realm_id: u32) -> Result<u64, RealmError> {
        let db = get_database();
        let pool = db.get_pool();

        let result = sqlx::query("DELETE FROM realm WHERE realm_id = ?")
            .bind(realm_id)
            .execute(pool)
            .await?;

        Ok(result.rows_affected())
    }

    pub async fn get(id: u32) -> Result<Option<Self>, RealmError> {
        let db = get_database();
        let pool = db.get_pool();

        let result = sqlx::query_as::<_, Realm>(
            "SELECT rowid, realm_id, name, status, expires_at, created_at, updated_at
             FROM realm WHERE rowid = ?",
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        Ok(result)
    }

    pub async fn get_by_name(name: &str) -> Result<Option<Self>, RealmError> {
        let db = get_database();
        let pool = db.get_pool();

        let result = sqlx::query_as::<_, Realm>(
            "SELECT rowid, realm_id, name, status, expires_at, created_at, updated_at
             FROM realm WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(pool)
        .await?;

        Ok(result)
    }

    pub async fn get_by_realm_id(realm_id: u32) -> Result<Option<Self>, RealmError> {
        let db = get_database();
        let pool = db.get_pool();

        let result = sqlx::query_as::<_, Realm>(
            "SELECT rowid, realm_id, name, status, expires_at, created_at, updated_at
             FROM realm WHERE realm_id = ?",
        )
        .bind(realm_id)
        .fetch_optional(pool)
        .await?;

        Ok(result)
    }

    pub async fn list() -> Result<Vec<Self>, RealmError> {
        Self::get_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test_utils::utils::setup_test_db;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_database_schema() -> anyhow::Result<()> {
        setup_test_db().await?;

        // 创建一个 Realm 来触发表创建，使用唯一名称
        let realm_id = rand::random::<u32>();
        let mut realm = Realm::new(realm_id, "test_name".to_string());
        let _rowid = realm.save().await?;

        // 查询表结构
        let db = get_database();
        let pool = db.get_pool();

        let schema_info: Option<(String,)> =
            sqlx::query_as("SELECT sql FROM sqlite_master WHERE type='table' AND name='realm'")
                .fetch_optional(pool)
                .await?;
        println!("Schema query result: {schema_info:?}");

        // 查询索引信息
        let index_info: Vec<(String,)> =
            sqlx::query_as("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='realm'")
                .fetch_all(pool)
                .await?;
        println!("Index query result: {index_info:?}");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_duplicate_realm_name() -> anyhow::Result<()> {
        setup_test_db().await?;

        let realm_id = rand::random::<u32>();

        let mut realm1 = Realm::new(realm_id, "test_name".to_string());
        let realm1_id = realm1.save().await?;
        println!("Created first realm with ID: {realm1_id}");

        // Try to create another Realm with the same realm_id
        let mut realm2 = Realm::new(realm_id, "test_name".to_string());
        let result = realm2.save().await;

        println!("Second realm save result: {result:?}");

        // Should fail due to duplicate name
        assert!(result.is_err());
        if let Err(RealmError::DatabaseError(msg)) = result {
            println!("Got database error: {msg}");
            assert!(msg.contains("UNIQUE constraint failed") || msg.contains("already exists"));
        } else {
            panic!("Expected DatabaseError for duplicate realm name, got: {result:?}");
        }

        Ok(())
    }
}
