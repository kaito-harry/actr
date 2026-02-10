//! 服务注册表实现
//!
//! 负责管理所有已注册的服务，提供服务发现功能
//!
//! ## 持久化策略
//!
//! - **内存 HashMap**：主存储，快速查询
//! - **SQLite 缓存**：可选，用于重启恢复
//! - **后台写入**：不阻塞主逻辑，异步写入数据库

use actr_protocol::{ActrId, ActrType};
use actrix_common::RealmError;
use actrix_common::realm::acl::ActorAcl;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

use crate::service_registry_storage::ServiceRegistryStorage;

/// 服务过期阈值（秒）- 超过此时间未收到心跳则认为服务过期
pub const SERVICE_EXPIRY_THRESHOLD_SECS: u64 = 5 * 60;

/// 清理任务执行间隔（秒）
pub const CLEANUP_INTERVAL_SECS: u64 = 30;

/// 服务能力描述
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceCapabilities {
    /// 最大并发处理数
    pub max_concurrent_requests: Option<u32>,
    /// 支持的版本范围
    pub version_range: Option<String>,
    /// 所在区域
    pub region: Option<String>,
    /// 自定义标签
    pub tags: Option<HashMap<String, String>>,
}

/// 服务状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ServiceStatus {
    Available,
    Busy,
    Maintenance,
    Unavailable,
}

/// 服务信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub actor_id: ActrId,
    pub service_name: String,
    pub message_types: Vec<String>,
    pub capabilities: Option<ServiceCapabilities>,
    pub status: ServiceStatus,
    pub last_heartbeat_time_secs: u64, // Unix timestamp

    // 新增字段：协议规格
    /// 服务协议规格（包含 fingerprint、protobufs、tags 等）
    #[serde(skip)]
    pub service_spec: Option<actr_protocol::ServiceSpec>,

    // 新增字段：访问控制列表
    /// ACL 规则
    #[serde(skip)]
    pub acl: Option<actr_protocol::Acl>,

    // 新增字段：负载指标（来自 Ping 消息）
    /// 服务可用性状态（protobuf enum ServiceAvailabilityState，使用 i32 存储）
    /// FULL=0, DEGRADED=1, OVERLOADED=2, UNAVAILABLE=3
    pub service_availability_state: Option<i32>,
    /// 剩余处理能力 (0.0 ~ 1.0)
    pub power_reserve: Option<f32>,
    /// 消息积压 (0.0 ~ 1.0)
    pub mailbox_backlog: Option<f32>,

    // 新增字段：依赖健康状态（protobuf enum，使用 i32 存储）
    /// 最坏依赖健康状态（多个依赖聚合结果，worst-case first 策略）
    /// protobuf enum ServiceDependencyState: HEALTHY=0, WARNING=1, BROKEN=2
    pub worst_dependency_health_state: Option<i32>,

    // 新增字段：负载均衡排序所需的复杂指标
    /// 协议兼容性分数（0.0 ~ 1.0，基于 protobuf fingerprint 计算）
    pub protocol_compatibility_score: Option<f32>,
    /// 地理位置信息（区域 + 经纬度）
    pub geo_location: Option<ServiceLocation>,
    /// 粘滞客户端 ID 列表（会话保持，从 Ping 消息获取）
    pub sticky_client_ids: Vec<String>,
}

/// 服务地理位置信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceLocation {
    /// 地理区域（如 "us-west", "cn-beijing"）
    pub region: String,
    /// 经度（可选）
    pub longitude: Option<f64>,
    /// 纬度（可选）
    pub latitude: Option<f64>,
}

/// 服务需求描述
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRequirements {
    /// 最小版本要求
    pub min_version: Option<String>,
    /// 区域偏好
    pub preferred_regions: Option<Vec<String>>,
    /// 必需标签
    pub required_tags: Option<HashMap<String, String>>,
}

/// 服务性能指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceMetrics {
    /// 当前活跃连接数
    pub active_connections: u32,
    /// 平均响应时间（毫秒）
    pub avg_response_time_ms: f64,
    /// 错误率
    pub error_rate: f64,
}

/// 服务注册表
#[derive(Debug, Default)]
pub struct ServiceRegistry {
    /// 服务映射表：service_name -> 服务实例列表
    services: HashMap<String, Vec<ServiceInfo>>,
    /// 消息类型映射表：message_type -> service_name 列表
    message_type_index: HashMap<String, Vec<String>>,
    /// Actor ID 映射表：actor_id -> 服务列表
    actor_index: HashMap<ActrId, Vec<String>>,
    /// SQLite 持久化缓存（可选）
    storage: Option<Arc<ServiceRegistryStorage>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置持久化存储（启动时调用）
    pub fn set_storage(&mut self, storage: Arc<ServiceRegistryStorage>) {
        info!("ServiceRegistry 启用 SQLite 持久化缓存");
        self.storage = Some(storage);
    }

    /// 从存储恢复服务列表（启动时调用）
    pub async fn restore_from_storage(&mut self) -> Result<usize, String> {
        let storage = match &self.storage {
            Some(s) => s,
            None => {
                warn!("未配置存储，跳过恢复");
                return Ok(0);
            }
        };

        match storage.load_all_services().await {
            Ok(services) => {
                let count = services.len();
                info!("从缓存恢复 {} 个服务", count);

                // 将服务加载到内存
                for service in services {
                    let actor_id = service.actor_id.clone();
                    let service_name = service.service_name.clone();
                    let message_types = service.message_types.clone();

                    // 添加到服务映射表
                    self.services
                        .entry(service_name.clone())
                        .or_default()
                        .push(service);

                    // 更新消息类型索引
                    for message_type in &message_types {
                        self.message_type_index
                            .entry(message_type.clone())
                            .or_default()
                            .push(service_name.clone());
                    }

                    // 更新 Actor 索引
                    self.actor_index
                        .entry(actor_id)
                        .or_default()
                        .push(service_name);
                }

                Ok(count)
            }
            Err(e) => {
                error!("从缓存恢复服务失败: {}", e);
                Err(format!("恢复失败: {e}"))
            }
        }
    }

    /// 注册服务（完整版本，支持 ServiceSpec 和 ACL）
    pub fn register_service_full(
        &mut self,
        actor_id: ActrId,
        service_name: String,
        message_types: Vec<String>,
        capabilities: Option<ServiceCapabilities>,
        service_spec: Option<actr_protocol::ServiceSpec>,
        acl: Option<actr_protocol::Acl>,
    ) -> Result<(), String> {
        info!(
            "注册服务: {} (Actor {}), has_spec={}, has_acl={}",
            service_name,
            actor_id.serial_number,
            service_spec.is_some(),
            acl.is_some()
        );

        let service_info = ServiceInfo {
            actor_id: actor_id.clone(),
            service_name: service_name.clone(),
            message_types: message_types.clone(),
            capabilities,
            status: ServiceStatus::Available,
            last_heartbeat_time_secs: current_timestamp(),
            service_spec,
            acl,
            service_availability_state: None,
            power_reserve: None,
            mailbox_backlog: None,
            worst_dependency_health_state: None,
            protocol_compatibility_score: None,
            geo_location: None,
            sticky_client_ids: Vec::new(),
        };

        // 异步写入 SQLite 缓存（后台任务，不阻塞）
        if let Some(storage) = self.storage.clone() {
            let service_to_save = service_info.clone();
            let actr_type = actor_id.r#type.clone();
            let service_spec_to_save = service_to_save.service_spec.clone();
            tokio::spawn(async move {
                // 保存服务信息
                if let Err(e) = storage.save_service(&service_to_save).await {
                    error!("保存服务到缓存失败: {}", e);
                }

                // 保存 Proto spec 到 service_specs 表（用于兼容性协商）
                if let Some(ref spec) = service_spec_to_save {
                    if let Err(e) = storage.save_proto_spec(&actr_type, spec).await {
                        error!("保存 Proto spec 到缓存失败: {}", e);
                    } else {
                        info!(
                            "✅ Proto spec 已保存: {}/{} fingerprint={}",
                            actr_type.manufacturer, actr_type.name, spec.fingerprint
                        );
                    }
                }
            });
        }

        // 添加到服务映射表
        self.services
            .entry(service_name.clone())
            .or_default()
            .push(service_info);

        // 更新消息类型索引
        for message_type in &message_types {
            self.message_type_index
                .entry(message_type.clone())
                .or_default()
                .push(service_name.clone());
        }

        // 更新 Actor 索引
        self.actor_index
            .entry(actor_id.clone())
            .or_default()
            .push(service_name.clone());

        Ok(())
    }

    /// 注册服务（简化版本，向后兼容）
    pub fn register_service(
        &mut self,
        actor_id: ActrId,
        service_name: String,
        message_types: Vec<String>,
        capabilities: Option<ServiceCapabilities>,
    ) -> Result<(), String> {
        self.register_service_full(
            actor_id,
            service_name,
            message_types,
            capabilities,
            None,
            None,
        )
    }

    /// 更新服务的负载指标（从 Ping 消息中获取）
    pub fn update_load_metrics(
        &mut self,
        actor_id: &ActrId,
        service_availability_state: i32, // ServiceAvailabilityState as i32
        power_reserve: f32,
        mailbox_backlog: f32,
    ) -> Result<(), String> {
        debug!(
            "更新 Actor {} 负载指标: service_availability_state={}, power={:.2}, backlog={:.2}",
            actor_id.serial_number, service_availability_state, power_reserve, mailbox_backlog
        );

        // 查找该 Actor 的所有服务
        if let Some(service_names) = self.actor_index.get(actor_id) {
            for service_name in service_names {
                if let Some(services) = self.services.get_mut(service_name) {
                    for service in services {
                        if service.actor_id == *actor_id {
                            service.service_availability_state = Some(service_availability_state);
                            service.power_reserve = Some(power_reserve);
                            service.mailbox_backlog = Some(mailbox_backlog);
                            service.last_heartbeat_time_secs = current_timestamp();
                            debug!("负载指标更新成功: {}", service_name);

                            // 异步更新 SQLite 缓存的心跳时间（后台任务，不阻塞）
                            if let Some(storage) = self.storage.clone() {
                                let actor_id_clone = actor_id.clone();
                                let service_name_clone = service_name.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = storage
                                        .update_heartbeat(&actor_id_clone, &service_name_clone)
                                        .await
                                    {
                                        error!("更新缓存心跳失败: {}", e);
                                    }
                                });
                            }
                        }
                    }
                }
            }
            Ok(())
        } else {
            Err(format!(
                "未找到 Actor {} 的服务注册",
                actor_id.serial_number
            ))
        }
    }

    /// 获取服务的 ServiceSpec（用于兼容性检查）
    pub fn get_service_spec(&self, actor_id: &ActrId) -> Option<&actr_protocol::ServiceSpec> {
        // 从 actor_index 找到服务名，再从 services 中找到实例
        self.actor_index.get(actor_id).and_then(|service_names| {
            service_names.first().and_then(|service_name| {
                self.services.get(service_name).and_then(|services| {
                    services
                        .iter()
                        .find(|s| &s.actor_id == actor_id)
                        .and_then(|s| s.service_spec.as_ref())
                })
            })
        })
    }

    /// 获取服务的 ACL（用于访问控制）
    pub fn get_acl(&self, actor_id: &ActrId) -> Option<&actr_protocol::Acl> {
        self.actor_index.get(actor_id).and_then(|service_names| {
            service_names.first().and_then(|service_name| {
                self.services.get(service_name).and_then(|services| {
                    services
                        .iter()
                        .find(|s| &s.actor_id == actor_id)
                        .and_then(|s| s.acl.as_ref())
                })
            })
        })
    }

    /// 获取 ServiceRegistryStorage 引用（用于兼容性协商时查询 proto specs）
    pub fn get_storage(&self) -> Option<Arc<ServiceRegistryStorage>> {
        self.storage.clone()
    }

    /// 根据消息类型发现服务
    pub fn discover_by_message_type(&self, message_type: &str) -> Vec<&ServiceInfo> {
        debug!("根据消息类型发现服务: {}", message_type);

        if let Some(service_names) = self.message_type_index.get(message_type) {
            let mut services = Vec::new();
            for service_name in service_names {
                if let Some(service_instances) = self.services.get(service_name) {
                    // 只返回可用的服务实例
                    services.extend(
                        service_instances
                            .iter()
                            .filter(|s| s.status == ServiceStatus::Available),
                    );
                }
            }
            services
        } else {
            debug!("未找到支持消息类型 {} 的服务", message_type);
            Vec::new()
        }
    }

    /// 根据服务名发现服务
    pub fn discover_by_service_name(&self, service_name: &str) -> Vec<&ServiceInfo> {
        debug!("根据服务名发现服务: {}", service_name);

        if let Some(services) = self.services.get(service_name) {
            services
                .iter()
                .filter(|s| s.status == ServiceStatus::Available)
                .collect()
        } else {
            debug!("未找到服务: {}", service_name);
            Vec::new()
        }
    }

    /// 根据需求发现服务
    pub fn discover_by_requirements(
        &self,
        requirements: &ServiceRequirements,
    ) -> Vec<&ServiceInfo> {
        debug!("根据需求发现服务: {:?}", requirements);

        let mut matching_services = Vec::new();

        for services in self.services.values() {
            for service in services {
                if service.status != ServiceStatus::Available {
                    continue;
                }

                // 检查版本要求
                if let (Some(min_version), Some(capabilities)) =
                    (&requirements.min_version, &service.capabilities)
                    && let Some(version_range) = &capabilities.version_range
                {
                    // 简单的版本比较，实际应该使用语义版本
                    if version_range < min_version {
                        continue;
                    }
                }

                // 检查区域偏好
                if let (Some(preferred_regions), Some(capabilities)) =
                    (&requirements.preferred_regions, &service.capabilities)
                    && let Some(region) = &capabilities.region
                    && !preferred_regions.contains(region)
                {
                    continue;
                }

                // 检查必需标签
                if let (Some(required_tags), Some(capabilities)) =
                    (&requirements.required_tags, &service.capabilities)
                {
                    if let Some(service_tags) = &capabilities.tags {
                        let mut all_tags_match = true;
                        for (key, value) in required_tags {
                            if service_tags.get(key) != Some(value) {
                                all_tags_match = false;
                                break;
                            }
                        }
                        if !all_tags_match {
                            continue;
                        }
                    } else {
                        // 服务没有标签但要求有标签
                        continue;
                    }
                }

                matching_services.push(service);
            }
        }

        matching_services
    }

    /// 更新服务状态
    pub fn update_service_status(
        &mut self,
        actor_id: &ActrId,
        service_name: &str,
        status: ServiceStatus,
        metrics: Option<ServiceMetrics>,
    ) -> Result<(), String> {
        debug!(
            "更新服务状态: {} (Actor {}) -> {:?}",
            service_name, actor_id.serial_number, status
        );

        if let Some(services) = self.services.get_mut(service_name) {
            for service in services {
                if service.actor_id == *actor_id {
                    service.status = status;
                    service.last_heartbeat_time_secs = current_timestamp();

                    if let Some(_metrics) = metrics {
                        // 这里可以存储性能指标，暂时忽略
                        debug!("收到服务性能指标数据");
                    }

                    return Ok(());
                }
            }
        }

        Err(format!(
            "未找到服务实例: {} (Actor {})",
            service_name, actor_id.serial_number
        ))
    }

    /// 注销服务
    pub fn unregister_service(
        &mut self,
        actor_id: &ActrId,
        service_name: &str,
    ) -> Result<(), String> {
        info!(
            "注销服务: {} (Actor {})",
            service_name, actor_id.serial_number
        );

        // 从服务映射表中移除
        if let Some(services) = self.services.get_mut(service_name) {
            let original_len = services.len();
            services.retain(|s| s.actor_id != *actor_id);

            if services.len() == original_len {
                return Err(format!(
                    "未找到要注销的服务实例: {} (Actor {})",
                    service_name, actor_id.serial_number
                ));
            }

            // 如果这是最后一个实例，清理消息类型索引
            if services.is_empty() {
                self.services.remove(service_name);

                // 从消息类型索引中移除
                self.message_type_index.retain(|_, service_names| {
                    service_names.retain(|name| name != service_name);
                    !service_names.is_empty()
                });
            }
        }

        // 从 Actor 索引中移除
        if let Some(actor_services) = self.actor_index.get_mut(actor_id) {
            actor_services.retain(|name| name != service_name);

            if actor_services.is_empty() {
                self.actor_index.remove(actor_id);
            }
        }

        // 异步从 SQLite 缓存删除（后台任务，不阻塞）
        if let Some(storage) = self.storage.clone() {
            let actor_id_clone = actor_id.clone();
            let service_name_owned = service_name.to_string();
            tokio::spawn(async move {
                if let Err(e) = storage
                    .delete_service(&actor_id_clone, &service_name_owned)
                    .await
                {
                    error!("从缓存删除服务失败: {}", e);
                }
            });
        }

        Ok(())
    }

    /// 注销 Actor 的所有服务
    pub fn unregister_actor(&mut self, actor_id: &ActrId) {
        info!("注销 Actor {} 的所有服务", actor_id.serial_number);

        if let Some(service_names) = self.actor_index.remove(actor_id) {
            for service_name in &service_names {
                let _ = self.unregister_service(actor_id, service_name);
            }
        }
    }

    /// 清理过期服务（超过指定时间未更新）
    ///
    /// 注意：此方法只清理内存中的服务，不删除数据库中的数据。
    /// 这样可以在断网 5-60 分钟后通过心跳从数据库恢复服务。
    /// 数据库的清理由独立的定时任务处理（TTL = 1小时）。
    pub fn cleanup_expired_services(&mut self) {
        let current_time = current_timestamp();
        let expiry_threshold = SERVICE_EXPIRY_THRESHOLD_SECS;

        let mut services_to_remove = Vec::new();

        for (service_name, services) in &self.services {
            for service in services {
                if current_time - service.last_heartbeat_time_secs > expiry_threshold {
                    services_to_remove.push((service.actor_id.clone(), service_name.clone()));
                }
            }
        }

        for (actor_id, service_name) in services_to_remove {
            warn!(
                "清理内存中的过期服务: {} (Actor {}) [数据库保留用于恢复]",
                service_name, actor_id.serial_number
            );
            // 只清理内存，不删除数据库
            let _ = self.unregister_service_memory_only(&actor_id, &service_name);
        }
    }

    /// 只从内存中注销服务，不删除数据库数据
    ///
    /// 用于过期服务清理，保留数据库数据以便后续恢复。
    fn unregister_service_memory_only(
        &mut self,
        actor_id: &ActrId,
        service_name: &str,
    ) -> Result<(), String> {
        // 从服务映射表中移除
        if let Some(services) = self.services.get_mut(service_name) {
            let original_len = services.len();
            services.retain(|s| s.actor_id != *actor_id);

            if services.len() == original_len {
                return Err(format!(
                    "未找到要注销的服务实例: {} (Actor {})",
                    service_name, actor_id.serial_number
                ));
            }

            // 如果这是最后一个实例，清理消息类型索引
            if services.is_empty() {
                self.services.remove(service_name);

                // 从消息类型索引中移除
                self.message_type_index.retain(|_, service_names| {
                    service_names.retain(|name| name != service_name);
                    !service_names.is_empty()
                });
            }
        }

        // 从 Actor 索引中移除
        if let Some(actor_services) = self.actor_index.get_mut(actor_id) {
            actor_services.retain(|name| name != service_name);

            if actor_services.is_empty() {
                self.actor_index.remove(actor_id);
            }
        }

        // 注意：不删除数据库数据，保留用于后续恢复

        Ok(())
    }

    /// 从数据库恢复服务（心跳恢复时使用）
    ///
    /// 当收到心跳但内存中找不到服务时，尝试从数据库恢复。
    /// 这通常发生在断网超过 5 分钟（内存清理阈值）但小于 1 小时（数据库 TTL）的情况。
    ///
    /// # Arguments
    ///
    /// * `actor_id` - 要恢复的 Actor ID
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - 成功从数据库恢复了至少一个服务
    /// * `Ok(false)` - 数据库中没有找到该 Actor 的服务（可能已过期或从未注册）
    /// * `Err(String)` - 恢复过程出错
    pub async fn restore_service_from_storage(
        &mut self,
        actor_id: &ActrId,
    ) -> Result<bool, String> {
        // 检查是否有存储后端
        let storage = match &self.storage {
            Some(s) => s,
            None => {
                debug!("No storage backend available for service recovery");
                return Ok(false);
            }
        };

        // 从数据库加载该 Actor 的服务
        let services = storage
            .load_services_by_actor_id(actor_id)
            .await
            .map_err(|e| format!("Failed to load services from storage: {}", e))?;

        if services.is_empty() {
            debug!(
                "No services found in storage for Actor {}",
                actor_id.serial_number
            );
            return Ok(false);
        }

        info!(
            "🔄 Restoring {} service(s) from storage for Actor {}",
            services.len(),
            actor_id.serial_number
        );

        // 将每个服务重新注册到内存
        for service in services {
            // 添加到服务映射表
            self.services
                .entry(service.service_name.clone())
                .or_default()
                .push(service.clone());

            // 更新消息类型索引
            for message_type in &service.message_types {
                self.message_type_index
                    .entry(message_type.clone())
                    .or_default()
                    .push(service.service_name.clone());
            }

            // 更新 Actor 索引
            self.actor_index
                .entry(service.actor_id.clone())
                .or_default()
                .push(service.service_name.clone());

            info!(
                "  ✅ Restored service: {} (Actor {})",
                service.service_name, service.actor_id.serial_number
            );
        }

        Ok(true)
    }

    /// 获取所有服务统计信息
    pub fn get_service_stats(&self) -> HashMap<String, usize> {
        self.services
            .iter()
            .map(|(name, instances)| {
                let available_count = instances
                    .iter()
                    .filter(|s| s.status == ServiceStatus::Available)
                    .count();
                (name.clone(), available_count)
            })
            .collect()
    }

    /// 获取消息类型映射统计
    pub fn get_message_type_stats(&self) -> HashMap<String, usize> {
        self.message_type_index
            .iter()
            .map(|(msg_type, services)| (msg_type.clone(), services.len()))
            .collect()
    }

    /// 获取所有服务（用于服务发现）
    ///
    /// # 参数
    /// - `manufacturer`: 可选的制造商过滤器
    ///
    /// # 返回
    /// 所有匹配的服务实例列表
    pub fn discover_all(&self, manufacturer: Option<&str>) -> Vec<&ServiceInfo> {
        let mut results = Vec::new();

        for services in self.services.values() {
            for service in services {
                // 只返回可用的服务
                if service.status != ServiceStatus::Available {
                    continue;
                }

                // 按 manufacturer 过滤
                if let Some(mfr) = manufacturer
                    && service.actor_id.r#type.manufacturer != mfr
                {
                    continue;
                }

                results.push(service);
            }
        }

        results
    }

    /// 按 ActrType 查询服务实例（用于负载均衡路由）
    ///
    /// # 参数
    /// - `target_type`: 目标 Actor 类型
    ///
    /// # 返回
    /// 所有匹配该类型的可用服务实例（克隆）
    pub fn find_by_actr_type(&self, target_type: &ActrType) -> Vec<ServiceInfo> {
        let mut results = Vec::new();

        for services in self.services.values() {
            for service in services {
                // 只返回可用的服务
                if service.status != ServiceStatus::Available {
                    continue;
                }

                // 匹配 ActrType (manufacturer + name)
                if service.actor_id.r#type.manufacturer == target_type.manufacturer
                    && service.actor_id.r#type.name == target_type.name
                {
                    results.push(service.clone());
                }
            }
        }

        results
    }

    /// Discover services by ActrType with ACL filtering
    ///
    /// Returns only services that the requester is allowed to discover
    /// based on ACL rules
    ///
    /// # Arguments
    ///
    /// - `requester_id`: Actor requesting discovery
    /// - `target_type`: Target service type
    ///
    /// # Returns
    ///
    /// List of ServiceInfo that match the service type and pass ACL check
    pub async fn discover_with_acl(
        &self,
        requester_id: &ActrId,
        target_type: &ActrType,
    ) -> Result<Vec<ServiceInfo>, RealmError> {
        let all_services = self.find_by_actr_type(target_type);
        let total_count = all_services.len(); // Save count before moving

        let mut allowed_services = Vec::new();

        for service in all_services {
            // Skip self
            if &service.actor_id == requester_id {
                continue;
            }

            // ACL check: can requester discover this service?
            let can_discover = Self::check_discovery_acl(requester_id, &service.actor_id).await?;

            if can_discover {
                allowed_services.push(service);
            } else {
                debug!(
                    requester = %requester_id.serial_number,
                    service = %service.actor_id.serial_number,
                    "ACL denied service discovery"
                );
            }
        }

        info!(
            requester = %requester_id.serial_number,
            target_type = ?target_type,
            total_services = total_count,
            allowed_services = allowed_services.len(),
            "Service discovery completed with ACL filtering"
        );

        Ok(allowed_services)
    }

    /// Check if discovery is allowed between two actors based on ACL rules
    ///
    /// # Arguments
    ///
    /// - `from_actor`: Actor requesting discovery
    /// - `to_actor`: Target actor
    ///
    /// # Returns
    ///
    /// Returns true if discovery is allowed based on ACL rules
    async fn check_discovery_acl(
        from_actor: &ActrId,
        to_actor: &ActrId,
    ) -> Result<bool, RealmError> {
        // Extract realm and actor types
        let from_realm = from_actor.realm.realm_id;
        let to_realm = to_actor.realm.realm_id;

        // Only check ACL if actors are in the same realm
        if from_realm != to_realm {
            debug!(
                from_realm = %from_realm,
                to_realm = %to_realm,
                "Cross-realm discovery denied"
            );
            return Ok(false);
        }

        let from_type = &from_actor.r#type.name;
        let to_type = &to_actor.r#type.name;

        ActorAcl::can_discover(from_realm, from_type, to_type).await
    }
}

/// 获取当前时间戳
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::ActrType;

    fn create_test_actor_id(serial: u64) -> ActrId {
        ActrId {
            serial_number: serial,
            r#type: ActrType {
                manufacturer: "test".to_string(),
                name: "test".to_string(),
            },
            realm: actr_protocol::Realm { realm_id: 0 },
        }
    }

    #[test]
    fn test_service_registration() {
        let mut registry = ServiceRegistry::new();
        let actor_id = create_test_actor_id(1);

        let result = registry.register_service(
            actor_id.clone(),
            "test_service".to_string(),
            vec!["TestMessage".to_string()],
            None,
        );

        assert!(result.is_ok());

        let services = registry.discover_by_message_type("TestMessage");
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].actor_id, actor_id);
    }

    #[test]
    fn test_service_discovery() {
        let mut registry = ServiceRegistry::new();
        let actor_id1 = create_test_actor_id(1);
        let actor_id2 = create_test_actor_id(2);

        registry
            .register_service(
                actor_id1,
                "service1".to_string(),
                vec!["Message1".to_string()],
                None,
            )
            .unwrap();

        registry
            .register_service(
                actor_id2,
                "service2".to_string(),
                vec!["Message1".to_string(), "Message2".to_string()],
                None,
            )
            .unwrap();

        let services = registry.discover_by_message_type("Message1");
        assert_eq!(services.len(), 2);

        let services = registry.discover_by_message_type("Message2");
        assert_eq!(services.len(), 1);

        let services = registry.discover_by_message_type("Message3");
        assert_eq!(services.len(), 0);
    }

    #[test]
    fn test_service_unregister() {
        let mut registry = ServiceRegistry::new();
        let actor_id = create_test_actor_id(1);

        registry
            .register_service(
                actor_id.clone(),
                "test_service".to_string(),
                vec!["TestMessage".to_string()],
                None,
            )
            .unwrap();

        // 验证服务已注册
        let services = registry.discover_by_message_type("TestMessage");
        assert_eq!(services.len(), 1);

        // 注销服务
        let result = registry.unregister_service(&actor_id, "test_service");
        assert!(result.is_ok());

        // 验证服务已移除
        let services = registry.discover_by_message_type("TestMessage");
        assert_eq!(services.len(), 0);
    }

    #[test]
    fn test_unregister_nonexistent_service() {
        let mut registry = ServiceRegistry::new();
        let actor_id = create_test_actor_id(1);

        // 注意：当前实现中，注销不存在的服务名会返回 Ok(())
        // 只有当服务名存在但找不到对应 actor_id 时才返回 Err
        let result = registry.unregister_service(&actor_id, "nonexistent");
        assert!(result.is_ok());

        // 测试服务名存在但 actor_id 不匹配的情况
        let actor_id1 = create_test_actor_id(1);
        let actor_id2 = create_test_actor_id(2);

        registry
            .register_service(
                actor_id1.clone(),
                "test_service".to_string(),
                vec!["Test".to_string()],
                None,
            )
            .unwrap();

        // 尝试用错误的 actor_id 注销
        let result = registry.unregister_service(&actor_id2, "test_service");
        assert!(result.is_err()); // 这种情况才返回错误
    }

    #[test]
    fn test_service_status_update() {
        let mut registry = ServiceRegistry::new();
        let actor_id = create_test_actor_id(1);

        registry
            .register_service(
                actor_id.clone(),
                "test_service".to_string(),
                vec!["TestMessage".to_string()],
                None,
            )
            .unwrap();

        // 更新服务状态为 Busy
        let result =
            registry.update_service_status(&actor_id, "test_service", ServiceStatus::Busy, None);
        assert!(result.is_ok());

        // 验证状态已更新（Busy 服务不应出现在发现结果中）
        let services = registry.discover_by_message_type("TestMessage");
        assert_eq!(services.len(), 0, "Busy 状态的服务不应被发现");

        // 更新回 Available
        registry
            .update_service_status(&actor_id, "test_service", ServiceStatus::Available, None)
            .unwrap();
        let services = registry.discover_by_message_type("TestMessage");
        assert_eq!(services.len(), 1);
    }

    #[test]
    fn test_load_metrics_update() {
        let mut registry = ServiceRegistry::new();
        let actor_id = create_test_actor_id(1);

        registry
            .register_service(
                actor_id.clone(),
                "test_service".to_string(),
                vec!["TestMessage".to_string()],
                None,
            )
            .unwrap();

        // 更新负载指标
        let result = registry.update_load_metrics(&actor_id, 0, 0.8, 0.3);
        assert!(result.is_ok());

        // 验证指标已更新
        let services = registry.discover_by_service_name("test_service");
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].service_availability_state, Some(0));
        assert_eq!(services[0].power_reserve, Some(0.8));
        assert_eq!(services[0].mailbox_backlog, Some(0.3));
    }

    #[test]
    fn test_discover_by_service_name() {
        let mut registry = ServiceRegistry::new();
        let actor_id1 = create_test_actor_id(1);
        let actor_id2 = create_test_actor_id(2);

        // 注册两个相同服务名的实例
        registry
            .register_service(
                actor_id1,
                "api_service".to_string(),
                vec!["ApiMessage".to_string()],
                None,
            )
            .unwrap();

        registry
            .register_service(
                actor_id2,
                "api_service".to_string(),
                vec!["ApiMessage".to_string()],
                None,
            )
            .unwrap();

        let services = registry.discover_by_service_name("api_service");
        assert_eq!(services.len(), 2);

        let services = registry.discover_by_service_name("nonexistent");
        assert_eq!(services.len(), 0);
    }

    #[test]
    fn test_unregister_actor() {
        let mut registry = ServiceRegistry::new();
        let actor_id = create_test_actor_id(1);

        // 注册多个服务
        registry
            .register_service(
                actor_id.clone(),
                "service1".to_string(),
                vec!["Message1".to_string()],
                None,
            )
            .unwrap();

        registry
            .register_service(
                actor_id.clone(),
                "service2".to_string(),
                vec!["Message2".to_string()],
                None,
            )
            .unwrap();

        // 验证服务已注册
        assert_eq!(registry.discover_by_message_type("Message1").len(), 1);
        assert_eq!(registry.discover_by_message_type("Message2").len(), 1);

        // 注销 Actor 的所有服务
        registry.unregister_actor(&actor_id);

        // 验证所有服务已移除
        assert_eq!(registry.discover_by_message_type("Message1").len(), 0);
        assert_eq!(registry.discover_by_message_type("Message2").len(), 0);
    }

    #[test]
    fn test_service_with_capabilities() {
        let mut registry = ServiceRegistry::new();
        let actor_id = create_test_actor_id(1);

        let mut tags = HashMap::new();
        tags.insert("region".to_string(), "us-west".to_string());

        let capabilities = ServiceCapabilities {
            max_concurrent_requests: Some(100),
            version_range: Some("1.0.0".to_string()),
            region: Some("us-west".to_string()),
            tags: Some(tags),
        };

        registry
            .register_service(
                actor_id,
                "test_service".to_string(),
                vec!["TestMessage".to_string()],
                Some(capabilities),
            )
            .unwrap();

        let services = registry.discover_by_message_type("TestMessage");
        assert_eq!(services.len(), 1);
        assert!(services[0].capabilities.is_some());
        assert_eq!(
            services[0].capabilities.as_ref().unwrap().region,
            Some("us-west".to_string())
        );
    }

    #[test]
    fn test_discover_by_requirements() {
        let mut registry = ServiceRegistry::new();
        let actor_id1 = create_test_actor_id(1);
        let actor_id2 = create_test_actor_id(2);

        // 注册带区域标签的服务
        let mut tags_us = HashMap::new();
        tags_us.insert("env".to_string(), "prod".to_string());

        registry
            .register_service(
                actor_id1,
                "service_us".to_string(),
                vec!["Message1".to_string()],
                Some(ServiceCapabilities {
                    max_concurrent_requests: None,
                    version_range: Some("2.0.0".to_string()),
                    region: Some("us-west".to_string()),
                    tags: Some(tags_us.clone()),
                }),
            )
            .unwrap();

        let mut tags_eu = HashMap::new();
        tags_eu.insert("env".to_string(), "dev".to_string());

        registry
            .register_service(
                actor_id2,
                "service_eu".to_string(),
                vec!["Message1".to_string()],
                Some(ServiceCapabilities {
                    max_concurrent_requests: None,
                    version_range: Some("1.0.0".to_string()),
                    region: Some("eu-west".to_string()),
                    tags: Some(tags_eu),
                }),
            )
            .unwrap();

        // 按区域查询
        let requirements = ServiceRequirements {
            min_version: None,
            preferred_regions: Some(vec!["us-west".to_string()]),
            required_tags: None,
        };

        let services = registry.discover_by_requirements(&requirements);
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].service_name, "service_us");

        // 按标签查询
        let requirements = ServiceRequirements {
            min_version: None,
            preferred_regions: None,
            required_tags: Some(tags_us),
        };

        let services = registry.discover_by_requirements(&requirements);
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].service_name, "service_us");
    }

    #[test]
    fn test_service_stats() {
        let mut registry = ServiceRegistry::new();
        let actor_id1 = create_test_actor_id(1);
        let actor_id2 = create_test_actor_id(2);

        // 两个不同的服务名，都支持 Message1
        registry
            .register_service(
                actor_id1,
                "service1".to_string(),
                vec!["Message1".to_string()],
                None,
            )
            .unwrap();

        registry
            .register_service(
                actor_id2,
                "service2".to_string(),
                vec!["Message1".to_string()],
                None,
            )
            .unwrap();

        let stats = registry.get_service_stats();
        assert_eq!(stats.get("service1"), Some(&1)); // service1 有 1 个实例
        assert_eq!(stats.get("service2"), Some(&1)); // service2 有 1 个实例

        // get_message_type_stats() 返回的是每个消息类型对应的服务名数量
        // 注意：当前实现会为每个注册的服务实例重复添加服务名到索引
        let msg_stats = registry.get_message_type_stats();
        assert_eq!(msg_stats.get("Message1"), Some(&2)); // Message1 被 2 个服务名支持
    }

    #[test]
    fn test_register_service_full_with_spec_and_acl() {
        let mut registry = ServiceRegistry::new();
        let actor_id = create_test_actor_id(1);

        let service_spec = actr_protocol::ServiceSpec {
            name: "secure_service".to_string(),
            fingerprint: "sha256:test123".to_string(),
            description: Some("Test service".to_string()),
            protobufs: vec![],
            published_at: None,
            tags: vec![],
        };

        let acl = actr_protocol::Acl { rules: vec![] };

        let result = registry.register_service_full(
            actor_id.clone(),
            "secure_service".to_string(),
            vec!["SecureMessage".to_string()],
            None,
            Some(service_spec.clone()),
            Some(acl),
        );

        assert!(result.is_ok());

        // 验证 ServiceSpec 可以被获取
        let spec = registry.get_service_spec(&actor_id);
        assert!(spec.is_some());
        assert_eq!(spec.unwrap().fingerprint, "sha256:test123");

        // 验证 ACL 可以被获取
        let acl = registry.get_acl(&actor_id);
        assert!(acl.is_some());
        assert_eq!(acl.unwrap().rules.len(), 0);
    }

    #[test]
    fn test_find_by_actr_type() {
        let mut registry = ServiceRegistry::new();

        let actor_id1 = ActrId {
            serial_number: 1,
            r#type: ActrType {
                manufacturer: "acme".to_string(),
                name: "worker".to_string(),
            },
            realm: actr_protocol::Realm { realm_id: 0 },
        };

        let actor_id2 = ActrId {
            serial_number: 2,
            r#type: ActrType {
                manufacturer: "acme".to_string(),
                name: "worker".to_string(),
            },
            realm: actr_protocol::Realm { realm_id: 0 },
        };

        let actor_id3 = ActrId {
            serial_number: 3,
            r#type: ActrType {
                manufacturer: "other".to_string(),
                name: "service".to_string(),
            },
            realm: actr_protocol::Realm { realm_id: 0 },
        };

        registry
            .register_service(
                actor_id1,
                "worker1".to_string(),
                vec!["Work".to_string()],
                None,
            )
            .unwrap();

        registry
            .register_service(
                actor_id2,
                "worker2".to_string(),
                vec!["Work".to_string()],
                None,
            )
            .unwrap();

        registry
            .register_service(
                actor_id3,
                "other_service".to_string(),
                vec!["Other".to_string()],
                None,
            )
            .unwrap();

        let target_type = ActrType {
            manufacturer: "acme".to_string(),
            name: "worker".to_string(),
        };

        let results = registry.find_by_actr_type(&target_type);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(
            |s| s.actor_id.r#type.manufacturer == "acme" && s.actor_id.r#type.name == "worker"
        ));
    }

    #[test]
    fn test_discover_all_with_manufacturer_filter() {
        let mut registry = ServiceRegistry::new();

        let actor_id1 = ActrId {
            serial_number: 1,
            r#type: ActrType {
                manufacturer: "acme".to_string(),
                name: "service1".to_string(),
            },
            realm: actr_protocol::Realm { realm_id: 0 },
        };

        let actor_id2 = ActrId {
            serial_number: 2,
            r#type: ActrType {
                manufacturer: "vendor".to_string(),
                name: "service2".to_string(),
            },
            realm: actr_protocol::Realm { realm_id: 0 },
        };

        registry
            .register_service(actor_id1, "s1".to_string(), vec!["M1".to_string()], None)
            .unwrap();

        registry
            .register_service(actor_id2, "s2".to_string(), vec!["M2".to_string()], None)
            .unwrap();

        // 不过滤
        let all = registry.discover_all(None);
        assert_eq!(all.len(), 2);

        // 按制造商过滤
        let acme_only = registry.discover_all(Some("acme"));
        assert_eq!(acme_only.len(), 1);
        assert_eq!(acme_only[0].actor_id.r#type.manufacturer, "acme");
    }
}
