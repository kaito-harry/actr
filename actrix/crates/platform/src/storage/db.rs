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
        let _ = sqlx::query("ALTER TABLE pending_registration ADD COLUMN ws_address TEXT")
            .execute(&self.pool)
            .await; // intentionally ignore error (column may already exist)

        // MFR (Manufacturer Registry) tables
        // name = GitHub user/org login (lowercased), serves as manufacturer identity
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS mfr (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT    NOT NULL UNIQUE,
                public_key   TEXT    NOT NULL DEFAULT '',
                contact      TEXT,
                status       TEXT    NOT NULL DEFAULT 'pending',
                created_at   INTEGER NOT NULL,
                updated_at   INTEGER,
                verified_at  INTEGER,
                suspended_at INTEGER,
                revoked_at   INTEGER,
                key_expires_at  INTEGER
            )",
        )
        .execute(&self.pool)
        .await?;

        // GitHub verification challenge for identity verification
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS mfr_challenge (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                mfr_id      INTEGER NOT NULL REFERENCES mfr(id),
                token       TEXT    NOT NULL,
                verify_url  TEXT    NOT NULL DEFAULT '',
                expires_at  INTEGER NOT NULL,
                verified_at INTEGER,
                created_at  INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS mfr_package (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                mfr_id       INTEGER NOT NULL REFERENCES mfr(id),
                manufacturer TEXT    NOT NULL,
                name         TEXT    NOT NULL,
                version      TEXT    NOT NULL,
                type_str     TEXT    NOT NULL,
                target       TEXT    NOT NULL,
                manifest     TEXT    NOT NULL,
                signature    TEXT    NOT NULL,
                signing_key_id TEXT,
                status       TEXT    NOT NULL DEFAULT 'active',
                published_at INTEGER NOT NULL,
                revoked_at   INTEGER,
                UNIQUE(manufacturer, name, version, target)
            )",
        )
        .execute(&self.pool)
        .await?;

        // Migrate: add target column if it doesn't exist (for existing databases)
        let _ = sqlx::query(
            "ALTER TABLE mfr_package ADD COLUMN target TEXT NOT NULL DEFAULT 'wasm32-wasip1'",
        )
        .execute(&self.pool)
        .await; // intentionally ignore error (column may already exist)

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_mfr_package_type ON mfr_package(type_str)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_mfr_package_mfr ON mfr_package(mfr_id, status)",
        )
        .execute(&self.pool)
        .await?;

        // Migrate: add proto_files column for proto filing (JSON text, nullable)
        let _ = sqlx::query("ALTER TABLE mfr_package ADD COLUMN proto_files TEXT")
            .execute(&self.pool)
            .await; // intentionally ignore error (column may already exist)

        // Migrate: record the MFR key that authenticated each newly published package.
        // Existing rows remain nullable and are resolved from their signed manifest.
        let _ = sqlx::query("ALTER TABLE mfr_package ADD COLUMN signing_key_id TEXT")
            .execute(&self.pool)
            .await; // intentionally ignore error (column may already exist)

        // Migrate: add key_id column (auto-assigned on activate/renew)
        let _ = sqlx::query("ALTER TABLE mfr ADD COLUMN key_id TEXT NOT NULL DEFAULT ''")
            .execute(&self.pool)
            .await;

        // MFR key history: stores retired public keys for JWKS-style rotation
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS mfr_key_history (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                mfr_id       INTEGER NOT NULL REFERENCES mfr(id),
                key_id       TEXT    NOT NULL,
                public_key   TEXT    NOT NULL,
                status       TEXT    NOT NULL DEFAULT 'retired',
                created_at   INTEGER NOT NULL,
                retired_at   INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_mfr_key_history_lookup
             ON mfr_key_history(mfr_id, key_id)",
        )
        .execute(&self.pool)
        .await?;

        // Publish nonce table for Challenge-Response authentication on /mfr/pkg/publish
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS mfr_publish_nonce (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                mfr_id     INTEGER NOT NULL REFERENCES mfr(id),
                nonce      BLOB    NOT NULL UNIQUE,
                status     TEXT    NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_mfr_publish_nonce_expires
             ON mfr_publish_nonce(expires_at)",
        )
        .execute(&self.pool)
        .await?;

        // AIS unpublished package manufacturer-proof nonce table. AIS inserts
        // after manufacturer_auth_signature verification; the unique index is the
        // replay guard.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ais_manufacturer_auth_nonce (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                manufacturer TEXT    NOT NULL,
                key_id       TEXT    NOT NULL,
                nonce        BLOB    NOT NULL,
                created_at   INTEGER NOT NULL,
                expires_at   INTEGER NOT NULL,
                UNIQUE(manufacturer, key_id, nonce)
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_ais_manufacturer_auth_nonce_expires
             ON ais_manufacturer_auth_nonce(expires_at)",
        )
        .execute(&self.pool)
        .await?;

        // Backfill key_id for existing MFRs that have a public_key but empty key_id.
        // This runs on every startup but is a no-op when all rows already have a key_id.
        {
            use base64::Engine as _;
            use sha2::{Digest, Sha256};

            let rows: Vec<(i64, String)> = sqlx::query_as(
                "SELECT id, public_key FROM mfr WHERE key_id = '' AND public_key != ''",
            )
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default();

            for (id, public_key_b64) in rows {
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(&public_key_b64)
                {
                    let hash = Sha256::digest(&bytes);
                    let hex_str: String = hash.iter().map(|b| format!("{b:02x}")).collect();
                    let key_id = format!("mfr-{}", &hex_str[..16]);
                    let _ = sqlx::query("UPDATE mfr SET key_id = ? WHERE id = ?")
                        .bind(&key_id)
                        .bind(id)
                        .execute(&self.pool)
                        .await;
                    crate::recording::info!(
                        "backfilled key_id from public_key fingerprint: id={}, key_id={}",
                        id,
                        key_id
                    );
                }
            }
        }

        // AIS renewal token table — only stores SHA-256(token), never the raw token.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ais_renewal_token (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_id TEXT NOT NULL,
                token_hash BLOB NOT NULL UNIQUE,
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_ais_renewal_token_actor
             ON ais_renewal_token(actor_id)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_ais_renewal_token_expires
             ON ais_renewal_token(expires_at)",
        )
        .execute(&self.pool)
        .await?;

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
