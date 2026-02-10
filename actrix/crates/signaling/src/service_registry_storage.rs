//! ServiceRegistry SQLite 持久化缓存层
//!
//! ## 设计原则
//!
//! 1. **SQLite 作为缓存**：不是主数据源，用于重启恢复
//! 2. **数据有 TTL**：默认 1 小时过期，自动清理
//! 3. **内存优先**：HashMap 是主存储，SQLite 是备份
//!
//! ## 数据流
//!
//! - 启动：SQLite → HashMap（恢复缓存）
//! - 注册：HashMap + SQLite（双写）
//! - 查询：HashMap（快速）
//! - 心跳：HashMap + SQLite（更新 TTL）
//! - 清理：定期清理过期数据

use crate::service_registry::{ServiceCapabilities, ServiceInfo, ServiceLocation, ServiceStatus};
use actr_protocol::{Acl, ActrId, ServiceSpec};
use anyhow::{Context, Result};
use prost::Message as ProstMessage;
use serde_json;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{debug, error, info};

/// ServiceRegistry 持久化存储
#[derive(Debug)]
pub struct ServiceRegistryStorage {
    pool: SqlitePool,
    /// TTL（秒），默认 3600 秒（1 小时）
    default_ttl_secs: u64,
    /// Proto specs TTL（秒），默认 604800 秒（7 天）
    proto_ttl_secs: u64,
}

/// 默认服务 TTL（1 小时）
pub const DEFAULT_SERVICE_TTL_SECS: u64 = 12 * 3600; // 临时方案

impl ServiceRegistryStorage {
    /// 创建存储实例
    pub async fn new(database_file: impl AsRef<Path>, ttl_secs: Option<u64>) -> Result<Self> {
        let db_path = database_file.as_ref();

        // 确保数据库目录存在
        if let Some(parent) = db_path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create database directory: {}", parent.display())
            })?;
            info!("📁 Created database directory: {}", parent.display());
        }

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&format!("sqlite:{}?mode=rwc", db_path.display()))
            .await
            .with_context(|| format!("Failed to connect to database: {}", db_path.display()))?;

        let storage = Self {
            pool,
            default_ttl_secs: ttl_secs.unwrap_or(DEFAULT_SERVICE_TTL_SECS),
            proto_ttl_secs: 604800, // Proto specs 默认 7 天
        };

        storage.init_schema().await?;
        info!(
            "✅ ServiceRegistryStorage initialized with TTL={}s",
            storage.default_ttl_secs
        );

        Ok(storage)
    }

    /// 初始化数据库表结构
    async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS service_registry (
                -- ActorId 字段
                actor_serial_number INTEGER NOT NULL,
                actor_realm_id INTEGER NOT NULL,
                actor_manufacturer TEXT NOT NULL,
                actor_device_name TEXT NOT NULL,

                -- 服务基本信息
                service_name TEXT NOT NULL,
                message_types TEXT NOT NULL,  -- JSON array

                -- 能力和状态
                capabilities_json TEXT,  -- JSON, nullable
                status TEXT NOT NULL,  -- Available/Busy/Maintenance/Unavailable

                -- ServiceSpec (protobuf BLOB)
                service_spec_blob BLOB,

                -- ACL (protobuf BLOB)
                acl_blob BLOB,

                -- 负载指标
                service_availability_state INTEGER,
                power_reserve REAL,
                mailbox_backlog REAL,
                worst_dependency_health_state INTEGER,
                protocol_compatibility_score REAL,

                -- 地理位置
                geo_region TEXT,
                geo_longitude REAL,
                geo_latitude REAL,

                -- 粘滞客户端
                sticky_client_ids TEXT,  -- JSON array

                -- 时间戳（Unix timestamp）
                registered_at INTEGER NOT NULL,
                last_heartbeat_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,  -- TTL 过期时间

                PRIMARY KEY (actor_serial_number, actor_realm_id, service_name)
            );

            CREATE INDEX IF NOT EXISTS idx_expires_at ON service_registry(expires_at);
            CREATE INDEX IF NOT EXISTS idx_service_name ON service_registry(service_name);
            "#,
        )
        .execute(&self.pool)
        .await
        .with_context(|| "Failed to create service_registry table")?;

        // service_specs 表：存储 Proto 内容用于兼容性协商
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS service_specs (
                actr_type_manufacturer TEXT NOT NULL,
                actr_type_name TEXT NOT NULL,
                service_fingerprint TEXT NOT NULL,
                proto_content BLOB NOT NULL,
                last_accessed INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                PRIMARY KEY (actr_type_manufacturer, actr_type_name, service_fingerprint)
            );

            CREATE INDEX IF NOT EXISTS idx_service_specs_expires_at ON service_specs(expires_at);
            CREATE INDEX IF NOT EXISTS idx_service_specs_last_accessed ON service_specs(last_accessed);
            "#,
        )
        .execute(&self.pool)
        .await
        .with_context(|| "Failed to create service_specs table")?;

        info!("Database schema initialized");
        Ok(())
    }

    /// 保存服务信息
    pub async fn save_service(&self, service: &ServiceInfo) -> Result<()> {
        let now = current_timestamp();
        let expires_at = now + self.default_ttl_secs;

        // 序列化复杂字段
        let message_types_json = serde_json::to_string(&service.message_types)?;
        let capabilities_json = service
            .capabilities
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let sticky_client_ids_json = serde_json::to_string(&service.sticky_client_ids)?;

        // ServiceSpec 序列化为 protobuf bytes
        let service_spec_blob = service.service_spec.as_ref().and_then(|spec| {
            let mut buf = Vec::new();
            spec.encode(&mut buf).ok()?;
            Some(buf)
        });

        // ACL 序列化为 protobuf bytes
        let acl_blob = service.acl.as_ref().and_then(|acl| {
            let mut buf = Vec::new();
            acl.encode(&mut buf).ok()?;
            Some(buf)
        });

        // 提取 ActorId 字段
        let actor_type = &service.actor_id.r#type;
        let actor_realm = &service.actor_id.realm;

        sqlx::query(
            r#"
            INSERT INTO service_registry (
                actor_serial_number, actor_realm_id, actor_manufacturer, actor_device_name,
                service_name, message_types, capabilities_json, status,
                service_spec_blob, acl_blob,
                service_availability_state, power_reserve, mailbox_backlog,
                worst_dependency_health_state, protocol_compatibility_score,
                geo_region, geo_longitude, geo_latitude,
                sticky_client_ids,
                registered_at, last_heartbeat_at, expires_at
            ) VALUES (
                ?1, ?2, ?3, ?4,
                ?5, ?6, ?7, ?8,
                ?9, ?10,
                ?11, ?12, ?13,
                ?14, ?15,
                ?16, ?17, ?18,
                ?19,
                ?20, ?21, ?22
            )
            ON CONFLICT(actor_serial_number, actor_realm_id, service_name)
            DO UPDATE SET
                message_types = excluded.message_types,
                capabilities_json = excluded.capabilities_json,
                status = excluded.status,
                service_spec_blob = excluded.service_spec_blob,
                acl_blob = excluded.acl_blob,
                service_availability_state = excluded.service_availability_state,
                power_reserve = excluded.power_reserve,
                mailbox_backlog = excluded.mailbox_backlog,
                worst_dependency_health_state = excluded.worst_dependency_health_state,
                protocol_compatibility_score = excluded.protocol_compatibility_score,
                geo_region = excluded.geo_region,
                geo_longitude = excluded.geo_longitude,
                geo_latitude = excluded.geo_latitude,
                sticky_client_ids = excluded.sticky_client_ids,
                last_heartbeat_at = excluded.last_heartbeat_at,
                expires_at = excluded.expires_at
            "#,
        )
        .bind(service.actor_id.serial_number as i64)
        .bind(actor_realm.realm_id as i64)
        .bind(&actor_type.manufacturer)
        .bind(&actor_type.name)
        .bind(&service.service_name)
        .bind(&message_types_json)
        .bind(capabilities_json.as_deref())
        .bind(status_to_string(&service.status))
        .bind(service_spec_blob.as_deref())
        .bind(acl_blob.as_deref())
        .bind(service.service_availability_state.map(|v| v as i64))
        .bind(service.power_reserve.map(|v| v as f64))
        .bind(service.mailbox_backlog.map(|v| v as f64))
        .bind(service.worst_dependency_health_state.map(|v| v as i64))
        .bind(service.protocol_compatibility_score.map(|v| v as f64))
        .bind(service.geo_location.as_ref().map(|g| g.region.as_str()))
        .bind(service.geo_location.as_ref().and_then(|g| g.longitude))
        .bind(service.geo_location.as_ref().and_then(|g| g.latitude))
        .bind(&sticky_client_ids_json)
        .bind(now as i64)
        .bind(service.last_heartbeat_time_secs as i64)
        .bind(expires_at as i64)
        .execute(&self.pool)
        .await
        .with_context(|| format!("Failed to save service: {}", service.service_name))?;

        debug!(
            "Saved service to cache: {} (Actor {}, expires in {}s)",
            service.service_name, service.actor_id.serial_number, self.default_ttl_secs
        );

        Ok(())
    }

    /// 更新心跳时间和 TTL
    pub async fn update_heartbeat(&self, actor_id: &ActrId, service_name: &str) -> Result<()> {
        let now = current_timestamp();
        let expires_at = now + self.default_ttl_secs;

        let rows_affected = sqlx::query(
            r#"
            UPDATE service_registry
            SET last_heartbeat_at = ?1, expires_at = ?2
            WHERE actor_serial_number = ?3 AND actor_realm_id = ?4 AND service_name = ?5
            "#,
        )
        .bind(now as i64)
        .bind(expires_at as i64)
        .bind(actor_id.serial_number as i64)
        .bind(actor_id.realm.realm_id as i64)
        .bind(service_name)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if rows_affected > 0 {
            debug!(
                "Updated heartbeat: {} (Actor {}, TTL extended to {})",
                service_name, actor_id.serial_number, expires_at
            );
        }

        Ok(())
    }

    /// 删除服务
    pub async fn delete_service(&self, actor_id: &ActrId, service_name: &str) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM service_registry
            WHERE actor_serial_number = ?1 AND actor_realm_id = ?2 AND service_name = ?3
            "#,
        )
        .bind(actor_id.serial_number as i64)
        .bind(actor_id.realm.realm_id as i64)
        .bind(service_name)
        .execute(&self.pool)
        .await?;

        debug!(
            "Deleted service from cache: {} (Actor {})",
            service_name, actor_id.serial_number
        );
        Ok(())
    }

    /// 加载所有有效服务（启动时恢复）
    pub async fn load_all_services(&self) -> Result<Vec<ServiceInfo>> {
        let now = current_timestamp();

        let rows = sqlx::query(
            r#"
            SELECT
                actor_serial_number, actor_realm_id, actor_manufacturer, actor_device_name,
                service_name, message_types, capabilities_json, status,
                service_spec_blob, acl_blob,
                service_availability_state, power_reserve, mailbox_backlog,
                worst_dependency_health_state, protocol_compatibility_score,
                geo_region, geo_longitude, geo_latitude,
                sticky_client_ids,
                last_heartbeat_at
            FROM service_registry
            WHERE expires_at > ?1
            ORDER BY service_name, actor_serial_number
            "#,
        )
        .bind(now as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut services = Vec::new();

        for row in rows {
            match self.row_to_service_info(row) {
                Ok(service) => services.push(service),
                Err(e) => {
                    error!("Failed to deserialize service from cache: {:?}", e);
                }
            }
        }

        info!("Loaded {} services from cache", services.len());
        Ok(services)
    }

    /// 根据 ActorId 加载服务（用于心跳恢复）
    ///
    /// 当收到心跳时发现内存中没有该服务注册，尝试从数据库恢复。
    /// 只返回未过期的服务（expires_at > now）。
    ///
    /// # Arguments
    ///
    /// * `actor_id` - Actor ID
    ///
    /// # Returns
    ///
    /// 该 Actor 的所有未过期服务列表
    pub async fn load_services_by_actor_id(&self, actor_id: &ActrId) -> Result<Vec<ServiceInfo>> {
        let now = current_timestamp();

        let rows = sqlx::query(
            r#"
            SELECT
                actor_serial_number, actor_realm_id, actor_manufacturer, actor_device_name,
                service_name, message_types, capabilities_json, status,
                service_spec_blob, acl_blob,
                service_availability_state, power_reserve, mailbox_backlog,
                worst_dependency_health_state, protocol_compatibility_score,
                geo_region, geo_longitude, geo_latitude,
                sticky_client_ids,
                last_heartbeat_at
            FROM service_registry
            WHERE actor_serial_number = ?1 
              AND actor_realm_id = ?2 
              AND expires_at > ?3
            ORDER BY service_name
            "#,
        )
        .bind(actor_id.serial_number as i64)
        .bind(actor_id.realm.realm_id as i64)
        .bind(now as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut services = Vec::new();

        for row in rows {
            match self.row_to_service_info(row) {
                Ok(service) => services.push(service),
                Err(e) => {
                    error!(
                        "Failed to deserialize service from cache for Actor {}: {:?}",
                        actor_id.serial_number, e
                    );
                }
            }
        }

        if !services.is_empty() {
            debug!(
                "Loaded {} services from cache for Actor {}",
                services.len(),
                actor_id.serial_number
            );
        }

        Ok(services)
    }

    /// 清理过期数据
    pub async fn cleanup_expired(&self) -> Result<u64> {
        let now = current_timestamp();

        let result = sqlx::query(
            r#"
            DELETE FROM service_registry WHERE expires_at <= ?1
            "#,
        )
        .bind(now as i64)
        .execute(&self.pool)
        .await?;

        let deleted_count = result.rows_affected();
        if deleted_count > 0 {
            info!("Cleaned up {} expired services from cache", deleted_count);
        }

        Ok(deleted_count)
    }

    /// 将数据库行转换为 ServiceInfo
    fn row_to_service_info(&self, row: sqlx::sqlite::SqliteRow) -> Result<ServiceInfo> {
        use sqlx::Row;

        // ActorId
        let actor_id = ActrId {
            serial_number: row.get::<i64, _>("actor_serial_number") as u64,
            realm: actr_protocol::Realm {
                realm_id: row.get::<i64, _>("actor_realm_id") as u32,
            },
            r#type: actr_protocol::ActrType {
                manufacturer: row.get("actor_manufacturer"),
                name: row.get("actor_device_name"),
            },
        };

        // 基本字段
        let service_name: String = row.get("service_name");
        let message_types: Vec<String> = serde_json::from_str(row.get("message_types"))?;
        let status = string_to_status(row.get("status"))?;

        // 可选字段
        let capabilities: Option<ServiceCapabilities> = row
            .get::<Option<String>, _>("capabilities_json")
            .map(|s| serde_json::from_str(&s))
            .transpose()?;

        // ServiceSpec (protobuf BLOB)
        let service_spec: Option<ServiceSpec> = row
            .get::<Option<Vec<u8>>, _>("service_spec_blob")
            .map(|bytes| ServiceSpec::decode(&bytes[..]))
            .transpose()
            .ok()
            .flatten();

        // ACL (protobuf BLOB)
        let acl: Option<Acl> = row
            .get::<Option<Vec<u8>>, _>("acl_blob")
            .map(|bytes| Acl::decode(&bytes[..]))
            .transpose()
            .ok()
            .flatten();

        // 地理位置
        let geo_location =
            row.get::<Option<String>, _>("geo_region")
                .map(|region| ServiceLocation {
                    region,
                    longitude: row.get("geo_longitude"),
                    latitude: row.get("geo_latitude"),
                });

        // 粘滞客户端
        let sticky_client_ids: Vec<String> = serde_json::from_str(row.get("sticky_client_ids"))?;

        Ok(ServiceInfo {
            actor_id,
            service_name,
            message_types,
            capabilities,
            status,
            last_heartbeat_time_secs: row.get::<i64, _>("last_heartbeat_at") as u64,
            service_spec,
            acl,
            service_availability_state: row
                .get::<Option<i64>, _>("service_availability_state")
                .map(|v| v as i32),
            power_reserve: row.get::<Option<f64>, _>("power_reserve").map(|v| v as f32),
            mailbox_backlog: row
                .get::<Option<f64>, _>("mailbox_backlog")
                .map(|v| v as f32),
            worst_dependency_health_state: row
                .get::<Option<i64>, _>("worst_dependency_health_state")
                .map(|v| v as i32),
            protocol_compatibility_score: row
                .get::<Option<f64>, _>("protocol_compatibility_score")
                .map(|v| v as f32),
            geo_location,
            sticky_client_ids,
        })
    }

    /// 获取统计信息
    pub async fn get_stats(&self) -> Result<CacheStats> {
        let now = current_timestamp();

        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM service_registry")
            .fetch_one(&self.pool)
            .await?;

        let expired: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM service_registry WHERE expires_at <= ?1")
                .bind(now as i64)
                .fetch_one(&self.pool)
                .await?;

        Ok(CacheStats {
            total_services: total as u64,
            expired_services: expired as u64,
            valid_services: (total - expired) as u64,
        })
    }

    // =========================================================================
    // service_specs 表方法：存储 Proto 内容用于兼容性协商
    // =========================================================================

    /// 保存 Proto spec（用于兼容性协商）
    ///
    /// 在 Actor 注册时，如果 ServiceSpec 存在，则提取 Proto 并保存到 service_specs 表。
    /// 使用 INSERT OR REPLACE 策略，相同指纹的 proto 会更新时间戳。
    pub async fn save_proto_spec(
        &self,
        actr_type: &actr_protocol::ActrType,
        service_spec: &ServiceSpec,
    ) -> Result<()> {
        let now = current_timestamp();
        let expires_at = now + self.proto_ttl_secs;

        // 序列化 ServiceSpec 为 protobuf bytes（包含完整的 proto 内容）
        let mut proto_content = Vec::new();
        service_spec
            .encode(&mut proto_content)
            .with_context(|| "Failed to encode ServiceSpec")?;

        sqlx::query(
            r#"
            INSERT INTO service_specs (actr_type_manufacturer, actr_type_name, service_fingerprint, proto_content, last_accessed, expires_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(actr_type_manufacturer, actr_type_name, service_fingerprint)
            DO UPDATE SET proto_content = excluded.proto_content, last_accessed = excluded.last_accessed, expires_at = excluded.expires_at
            "#,
        )
        .bind(&actr_type.manufacturer)
        .bind(&actr_type.name)
        .bind(&service_spec.fingerprint)
        .bind(&proto_content)
        .bind(now as i64)
        .bind(expires_at as i64)
        .execute(&self.pool)
        .await
        .with_context(|| format!("Failed to save proto spec for {}/{}", actr_type.manufacturer, actr_type.name))?;

        debug!(
            "Saved proto spec: {}/{} fingerprint={} (expires in {}s)",
            actr_type.manufacturer, actr_type.name, service_spec.fingerprint, self.proto_ttl_secs
        );

        Ok(())
    }

    /// 根据指纹获取 Proto spec
    ///
    /// 查询匹配的 Proto，如果找到则更新访问时间（异步执行，避免阻塞查询）。
    pub async fn get_proto_by_fingerprint(
        &self,
        actr_type: &actr_protocol::ActrType,
        fingerprint: &str,
    ) -> Result<Option<ServiceSpec>> {
        let now = current_timestamp();

        // 查询
        let row = sqlx::query(
            r#"SELECT proto_content FROM service_specs 
               WHERE actr_type_manufacturer = ?1 
               AND actr_type_name = ?2 
               AND service_fingerprint = ?3
               AND expires_at > ?4"#,
        )
        .bind(&actr_type.manufacturer)
        .bind(&actr_type.name)
        .bind(fingerprint)
        .bind(now as i64)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            use sqlx::Row;
            let proto_content: Vec<u8> = row.get("proto_content");
            let service_spec = ServiceSpec::decode(&proto_content[..])
                .with_context(|| "Failed to decode ServiceSpec")?;

            // 异步更新访问时间和过期时间
            let pool_clone = self.pool.clone();
            let manufacturer = actr_type.manufacturer.clone();
            let name = actr_type.name.clone();
            let fingerprint_clone = fingerprint.to_string();
            let ttl = self.proto_ttl_secs;
            tokio::spawn(async move {
                let new_expires_at = now + ttl;
                let _ = sqlx::query(
                    r#"UPDATE service_specs SET last_accessed = ?1, expires_at = ?2 
                       WHERE actr_type_manufacturer = ?3 AND actr_type_name = ?4 AND service_fingerprint = ?5"#,
                )
                .bind(now as i64)
                .bind(new_expires_at as i64)
                .bind(&manufacturer)
                .bind(&name)
                .bind(&fingerprint_clone)
                .execute(&pool_clone)
                .await;
            });

            debug!(
                "Found proto spec: {}/{} fingerprint={}",
                actr_type.manufacturer, actr_type.name, fingerprint
            );
            Ok(Some(service_spec))
        } else {
            debug!(
                "Proto spec not found: {}/{} fingerprint={}",
                actr_type.manufacturer, actr_type.name, fingerprint
            );
            Ok(None)
        }
    }

    /// 清理过期的 proto specs
    pub async fn cleanup_expired_proto_specs(&self) -> Result<u64> {
        let now = current_timestamp();

        let result = sqlx::query("DELETE FROM service_specs WHERE expires_at <= ?1")
            .bind(now as i64)
            .execute(&self.pool)
            .await?;

        let deleted_count = result.rows_affected();
        if deleted_count > 0 {
            info!(
                "Cleaned up {} expired proto specs from cache",
                deleted_count
            );
        }

        Ok(deleted_count)
    }
}

/// 缓存统计信息
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_services: u64,
    pub expired_services: u64,
    pub valid_services: u64,
}

/// ServiceStatus 转字符串
fn status_to_string(status: &ServiceStatus) -> &'static str {
    match status {
        ServiceStatus::Available => "Available",
        ServiceStatus::Busy => "Busy",
        ServiceStatus::Maintenance => "Maintenance",
        ServiceStatus::Unavailable => "Unavailable",
    }
}

/// 字符串转 ServiceStatus
fn string_to_status(s: &str) -> Result<ServiceStatus> {
    match s {
        "Available" => Ok(ServiceStatus::Available),
        "Busy" => Ok(ServiceStatus::Busy),
        "Maintenance" => Ok(ServiceStatus::Maintenance),
        "Unavailable" => Ok(ServiceStatus::Unavailable),
        _ => Err(anyhow::anyhow!("Invalid service status: {s}")),
    }
}

/// 获取当前 Unix 时间戳（秒）
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::{ActrType, Realm};

    fn create_test_actor_id(serial: u64) -> ActrId {
        ActrId {
            serial_number: serial,
            realm: Realm { realm_id: 1001 },
            r#type: ActrType {
                manufacturer: "test-mfg".to_string(),
                name: "test-device".to_string(),
            },
        }
    }

    fn create_test_service(serial: u64, name: &str) -> ServiceInfo {
        ServiceInfo {
            actor_id: create_test_actor_id(serial),
            service_name: name.to_string(),
            message_types: vec!["test.Message".to_string()],
            capabilities: None,
            status: ServiceStatus::Available,
            last_heartbeat_time_secs: current_timestamp(),
            service_spec: None,
            acl: None,
            service_availability_state: None,
            power_reserve: Some(0.8),
            mailbox_backlog: Some(0.2),
            worst_dependency_health_state: None,
            protocol_compatibility_score: None,
            geo_location: None,
            sticky_client_ids: vec![],
        }
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let storage = ServiceRegistryStorage::new(":memory:", Some(3600))
            .await
            .unwrap();

        let service = create_test_service(1, "test-service");
        storage.save_service(&service).await.unwrap();

        let loaded = storage.load_all_services().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].service_name, "test-service");
        assert_eq!(loaded[0].actor_id.serial_number, 1);
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let storage = ServiceRegistryStorage::new(":memory:", Some(1))
            .await
            .unwrap();

        let service = create_test_service(1, "expiring-service");
        storage.save_service(&service).await.unwrap();

        // 立即加载应该能找到
        let loaded = storage.load_all_services().await.unwrap();
        assert_eq!(loaded.len(), 1);

        // 等待过期
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // 过期后加载为空
        let loaded = storage.load_all_services().await.unwrap();
        assert_eq!(loaded.len(), 0);
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let storage = ServiceRegistryStorage::new(":memory:", Some(1))
            .await
            .unwrap();

        let service = create_test_service(1, "test-service");
        storage.save_service(&service).await.unwrap();

        // 等待过期
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // 清理
        let deleted = storage.cleanup_expired().await.unwrap();
        assert_eq!(deleted, 1);

        // 验证已删除
        let stats = storage.get_stats().await.unwrap();
        assert_eq!(stats.total_services, 0);
    }

    #[tokio::test]
    async fn test_update_heartbeat() {
        let storage = ServiceRegistryStorage::new(":memory:", Some(10))
            .await
            .unwrap();

        let service = create_test_service(1, "test-service");
        storage.save_service(&service).await.unwrap();

        // 更新心跳
        storage
            .update_heartbeat(&service.actor_id, &service.service_name)
            .await
            .unwrap();

        // 验证更新成功
        let loaded = storage.load_all_services().await.unwrap();
        assert_eq!(loaded.len(), 1);
    }
}
