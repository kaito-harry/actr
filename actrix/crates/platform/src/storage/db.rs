//! 数据库连接和操作管理
//!
//! 提供基于 sqlx 的数据库连接池和基本操作

use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

/// 数据库管理器
#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    /// 创建新的数据库实例
    ///
    /// # Arguments
    /// * `path` - 数据库文件存储目录路径，必须已存在
    ///   主数据库文件将存储为 `{path}/actrix.db`
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_file = path.as_ref().join("actrix.db");

        // 创建连接选项并启用 WAL 模式
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_file.display()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));

        // 创建连接池
        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await?;

        let db = Self { pool };

        // 初始化数据库表结构
        db.initialize_schema().await?;

        Ok(db)
    }

    /// 初始化数据库表结构
    async fn initialize_schema(&self) -> Result<()> {
        // 创建 Realm 表
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS realm (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'Active',
                enabled INTEGER NOT NULL DEFAULT 1,
                expires_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER,
                secret_current TEXT NOT NULL DEFAULT '',
                secret_previous_hash TEXT,
                secret_previous_valid_until INTEGER
            )",
        )
        .execute(&self.pool)
        .await?;

        // Set autoincrement start to 2^25 = 33554432
        // Only insert if not already present (fresh database)
        sqlx::query("INSERT OR IGNORE INTO sqlite_sequence(name, seq) VALUES('realm', 33554431)")
            .execute(&self.pool)
            .await?;

        // 创建访问控制列表表
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS actoracl (
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                realm_id INTEGER NOT NULL,
                source_realm_id INTEGER,
                from_type TEXT NOT NULL,
                to_type TEXT NOT NULL,
                access INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // 创建索引
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_realm_name
             ON realm(name)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_actoracl_realm_id
             ON actoracl(realm_id)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_actoracl_lookup
             ON actoracl(realm_id, source_realm_id, from_type, to_type)",
        )
        .execute(&self.pool)
        .await?;

        // Pending registration data: AIS writes, signaling reads on WS connect
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pending_registration (
                serial_number INTEGER PRIMARY KEY,
                realm_id INTEGER NOT NULL,
                service_spec_blob BLOB,
                ws_address TEXT,
                created_at INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // Migrate: add ws_address column if it doesn't exist (for existing databases)
        let _ = sqlx::query(
            "ALTER TABLE pending_registration ADD COLUMN ws_address TEXT",
        )
        .execute(&self.pool)
        .await; // intentionally ignore error (column may already exist)

        Ok(())
    }

    /// 获取数据库连接池
    pub fn get_pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// 执行 SQL 语句并返回影响的行数
    pub async fn execute(&self, sql: &str) -> Result<u64> {
        let result = sqlx::query(sql).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }
}

use tokio::sync::OnceCell;

/// 全局数据库实例
static GLOBAL_DATABASE: OnceCell<Database> = OnceCell::const_new();

/// 设置全局数据库路径
pub async fn set_db_path(path: &Path) -> Result<()> {
    let database = Database::new(path).await?;
    GLOBAL_DATABASE
        .set(database)
        .map_err(|_| anyhow::anyhow!("Database already initialized"))?;
    Ok(())
}

/// 获取全局数据库实例
pub fn get_database() -> &'static Database {
    GLOBAL_DATABASE
        .get()
        .expect("Database not initialized. Call set_db_path first.")
}

/// 检查数据库是否已初始化
pub fn is_database_initialized() -> bool {
    GLOBAL_DATABASE.get().is_some()
}
