use crate::error::SupervitError;
use actrix_common::realm::{Realm, RealmConfig};
use actrix_common::storage::is_database_initialized;
use actrix_proto::{RealmInfo, ResourceType};
use chrono::Utc;
use std::convert::TryFrom;
use std::str::FromStr;
use tracing::{debug, warn};

/// Config key for realm enable flag
pub const REALM_ENABLED_KEY: &str = "realm.enabled";
/// Config key for realm allowed service types
pub const REALM_USE_SERVERS_KEY: &str = "realm.use_servers";
/// Config key for realm version (assigned by Boss for sync tracking)
pub const REALM_VERSION_KEY: &str = "realm.version";

/// Realm metadata stored alongside the core realm record
#[derive(Debug, Clone, Default)]
pub struct RealmMetadata {
    pub enabled: bool,
    pub use_servers: Vec<ResourceType>,
    /// Realm version assigned by Boss for sync tracking
    pub version: u64,
}

/// Convert a realm record and metadata into proto RealmInfo
pub fn realm_to_proto(realm: &Realm, metadata: &RealmMetadata) -> RealmInfo {
    let created_at = realm.created_at.unwrap_or_else(|| Utc::now().timestamp());
    let updated_at = realm.updated_at.unwrap_or(created_at);
    let use_servers: Vec<i32> = metadata.use_servers.iter().map(|v| *v as i32).collect();

    RealmInfo {
        realm_id: realm.realm_id,
        name: realm.name.clone(),
        enabled: metadata.enabled,
        created_at,
        updated_at: Some(updated_at),
        use_servers,
        version: metadata.version,
        expires_at: realm.expires_at.unwrap_or(0) as u64,
        status: realm.status.clone(),
    }
}

/// Load realm metadata from RealmConfig table
pub async fn load_realm_metadata(realm_rowid: u32) -> Result<RealmMetadata, SupervitError> {
    let enabled = load_enabled_flag(realm_rowid).await?;
    let use_servers = load_use_servers(realm_rowid).await?;
    let version = load_version(realm_rowid).await?;

    Ok(RealmMetadata {
        enabled,
        use_servers,
        version,
    })
}

/// Persist realm metadata into RealmConfig table
pub async fn persist_realm_metadata(
    realm_rowid: u32,
    metadata: &RealmMetadata,
) -> Result<(), SupervitError> {
    upsert_config_value(realm_rowid, REALM_ENABLED_KEY, metadata.enabled.to_string()).await?;

    let serialized_use_servers = serialize_use_servers(&metadata.use_servers)?;
    upsert_config_value(realm_rowid, REALM_USE_SERVERS_KEY, serialized_use_servers).await?;

    upsert_config_value(realm_rowid, REALM_VERSION_KEY, metadata.version.to_string()).await?;

    Ok(())
}

async fn load_enabled_flag(realm_rowid: u32) -> Result<bool, SupervitError> {
    let config = RealmConfig::get_by_realm_and_key(realm_rowid, REALM_ENABLED_KEY)
        .await
        .map_err(|e| SupervitError::Internal(format!("Failed to load realm enabled flag: {e}")))?;

    if let Some(cfg) = config {
        Ok(bool::from_str(cfg.value()).unwrap_or(true))
    } else {
        Ok(true)
    }
}

async fn load_version(realm_rowid: u32) -> Result<u64, SupervitError> {
    let config = RealmConfig::get_by_realm_and_key(realm_rowid, REALM_VERSION_KEY)
        .await
        .map_err(|e| SupervitError::Internal(format!("Failed to load realm version: {e}")))?;

    if let Some(cfg) = config {
        Ok(u64::from_str(cfg.value()).unwrap_or(0))
    } else {
        Ok(0)
    }
}

async fn load_use_servers(realm_rowid: u32) -> Result<Vec<ResourceType>, SupervitError> {
    let config = RealmConfig::get_by_realm_and_key(realm_rowid, REALM_USE_SERVERS_KEY)
        .await
        .map_err(|e| SupervitError::Internal(format!("Failed to load realm services: {e}")))?;

    if let Some(cfg) = config {
        Ok(parse_use_servers(cfg.value()))
    } else {
        Ok(Vec::new())
    }
}

async fn upsert_config_value(
    realm_rowid: u32,
    key: &str,
    value: String,
) -> Result<(), SupervitError> {
    let existing = RealmConfig::get_by_realm_and_key(realm_rowid, key)
        .await
        .map_err(|e| SupervitError::Internal(format!("Failed to read realm config: {e}")))?;

    match existing {
        Some(mut cfg) => {
            cfg.set_value(value);
            cfg.save().await.map_err(|e| {
                SupervitError::Internal(format!("Failed to update realm config: {e}"))
            })?;
        }
        None => {
            let mut cfg = RealmConfig::new(realm_rowid, key.to_string(), value);
            cfg.save().await.map_err(|e| {
                SupervitError::Internal(format!("Failed to create realm config: {e}"))
            })?;
        }
    }

    Ok(())
}

fn serialize_use_servers(use_servers: &[ResourceType]) -> Result<String, SupervitError> {
    let values: Vec<i32> = use_servers.iter().map(|v| *v as i32).collect();
    serde_json::to_string(&values)
        .map_err(|e| SupervitError::Internal(format!("Failed to serialize use_servers: {e}")))
}

fn parse_use_servers(raw: &str) -> Vec<ResourceType> {
    match serde_json::from_str::<Vec<i32>>(raw) {
        Ok(values) => values
            .into_iter()
            .filter_map(|v| ResourceType::try_from(v).ok())
            .collect(),
        Err(e) => {
            warn!(
                "Failed to parse realm use_servers, fallback to empty: {}",
                e
            );
            Vec::new()
        }
    }
}

/// Get the maximum realm version across all realms.
///
/// This is used to report the sync status to the Supervisor.
/// The Supervisor can use this to detect version lag and push missing realms.
///
/// Returns 0 if the database is not initialized or no realms exist.
pub async fn get_max_realm_version() -> Result<u64, SupervitError> {
    // Check if database is initialized to avoid panic in test environments
    if !is_database_initialized() {
        debug!("Database not initialized, returning 0 for max realm version");
        return Ok(0);
    }

    let realms = match Realm::get_all().await {
        Ok(t) => t,
        Err(e) => {
            debug!("Failed to load realm list: {}", e);
            return Ok(0);
        }
    };

    let mut max_version: u64 = 0;

    for realm in realms {
        if let Some(rowid) = realm.rowid {
            match load_version(rowid).await {
                Ok(version) => {
                    if version > max_version {
                        max_version = version;
                    }
                }
                Err(e) => {
                    debug!("Skip realm {} version check: {}", realm.realm_id, e);
                }
            }
        }
    }

    Ok(max_version)
}
