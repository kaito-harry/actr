use crate::MfrError;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PkgStatus {
    #[default]
    Active,
    Revoked,
}

impl std::fmt::Display for PkgStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PkgStatus::Active => write!(f, "active"),
            PkgStatus::Revoked => write!(f, "revoked"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActrPackage {
    pub id: i64,
    pub mfr_id: i64,
    pub manufacturer: String,
    pub name: String,
    pub version: String,
    pub type_str: String,
    /// Target platform (e.g. "wasm32-wasip1", "x86_64-unknown-linux-gnu")
    pub target: String,
    pub manifest: String,
    pub signature: String,
    /// MFR key that authenticated this package at publish time.
    pub signing_key_id: Option<String>,
    /// Proto files JSON for filing/audit (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proto_files: Option<String>,
    pub status: PkgStatus,
    pub published_at: i64,
    pub revoked_at: Option<i64>,
}

impl<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for ActrPackage {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        let status_str: String = row.try_get("status")?;
        let status = match status_str.as_str() {
            "revoked" => PkgStatus::Revoked,
            _ => PkgStatus::Active,
        };
        Ok(ActrPackage {
            id: row.try_get("id")?,
            mfr_id: row.try_get("mfr_id")?,
            manufacturer: row.try_get("manufacturer")?,
            name: row.try_get("name")?,
            version: row.try_get("version")?,
            type_str: row.try_get("type_str")?,
            target: row.try_get("target").unwrap_or_default(),
            manifest: row.try_get("manifest")?,
            signature: row.try_get("signature")?,
            signing_key_id: row.try_get("signing_key_id").unwrap_or(None),
            proto_files: row.try_get("proto_files").unwrap_or_default(),
            status,
            published_at: row.try_get("published_at")?,
            revoked_at: row.try_get("revoked_at")?,
        })
    }
}

impl ActrPackage {
    #[allow(clippy::too_many_arguments)]
    pub async fn publish(
        pool: &SqlitePool,
        mfr_id: i64,
        manufacturer: &str,
        name: &str,
        version: &str,
        target: &str,
        manifest: &str,
        signature: &str,
        signing_key_id: &str,
        proto_files: Option<&str>,
    ) -> Result<Self, MfrError> {
        let type_str = format!("{}:{}:{}", manufacturer, name, version);
        let now = Utc::now().timestamp();
        let id = sqlx::query(
            "INSERT INTO mfr_package (mfr_id, manufacturer, name, version, type_str, target, manifest, signature, signing_key_id, proto_files, status, published_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)",
        )
        .bind(mfr_id)
        .bind(manufacturer)
        .bind(name)
        .bind(version)
        .bind(&type_str)
        .bind(target)
        .bind(manifest)
        .bind(signature)
        .bind(signing_key_id)
        .bind(proto_files)
        .bind(now)
        .execute(pool)
        .await
        .map_err(|e| {
            if e.to_string().contains("UNIQUE") {
                MfrError::PackageAlreadyPublished
            } else {
                MfrError::Database(e)
            }
        })?
        .last_insert_rowid();

        Self::get_by_id(pool, id).await?.ok_or(MfrError::NotFound)
    }

    pub async fn get_by_id(pool: &SqlitePool, id: i64) -> Result<Option<Self>, MfrError> {
        Ok(
            sqlx::query_as::<_, ActrPackage>("SELECT * FROM mfr_package WHERE id = ?")
                .bind(id)
                .fetch_optional(pool)
                .await?,
        )
    }

    pub async fn get_by_type(pool: &SqlitePool, type_str: &str) -> Result<Option<Self>, MfrError> {
        Ok(sqlx::query_as::<_, ActrPackage>(
            "SELECT * FROM mfr_package WHERE type_str = ? AND status = 'active'",
        )
        .bind(type_str)
        .fetch_optional(pool)
        .await?)
    }

    /// Lookup by type_str + target platform, for cross-platform package distribution.
    pub async fn get_by_type_and_target(
        pool: &SqlitePool,
        type_str: &str,
        target: &str,
    ) -> Result<Option<Self>, MfrError> {
        Ok(sqlx::query_as::<_, ActrPackage>(
            "SELECT * FROM mfr_package WHERE type_str = ? AND target = ? AND status = 'active'",
        )
        .bind(type_str)
        .bind(target)
        .fetch_optional(pool)
        .await?)
    }

    /// Lookup a package tuple regardless of lifecycle status.
    ///
    /// AIS uses this to distinguish a package that was never published from a
    /// package that was explicitly revoked. A revoked tuple must not fall
    /// through to unpublished-package authentication.
    pub async fn get_by_type_and_target_any_status(
        pool: &SqlitePool,
        type_str: &str,
        target: &str,
    ) -> Result<Option<Self>, MfrError> {
        Ok(sqlx::query_as::<_, ActrPackage>(
            "SELECT * FROM mfr_package WHERE type_str = ? AND target = ?",
        )
        .bind(type_str)
        .bind(target)
        .fetch_optional(pool)
        .await?)
    }

    pub async fn list_by_mfr(pool: &SqlitePool, mfr_id: i64) -> Result<Vec<Self>, MfrError> {
        Ok(sqlx::query_as::<_, ActrPackage>(
            "SELECT * FROM mfr_package WHERE mfr_id = ? ORDER BY published_at DESC",
        )
        .bind(mfr_id)
        .fetch_all(pool)
        .await?)
    }

    pub async fn revoke(&mut self, pool: &SqlitePool) -> Result<(), MfrError> {
        let now = Utc::now().timestamp();
        sqlx::query("UPDATE mfr_package SET status = 'revoked', revoked_at = ? WHERE id = ?")
            .bind(now)
            .bind(self.id)
            .execute(pool)
            .await?;
        self.status = PkgStatus::Revoked;
        self.revoked_at = Some(now);
        Ok(())
    }
}
