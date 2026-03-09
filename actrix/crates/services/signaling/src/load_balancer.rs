//! 负载均衡模块
//!
//! 实现基于多种因素的服务实例排序和选择算法
//!
//! # 支持的排序因子
//! - `MAXIMUM_POWER_RESERVE`: 按剩余处理能力降序（优先选择负载轻的）
//! - `MINIMUM_MAILBOX_BACKLOG`: 按消息积压升序（优先选择积压少的）
//! - `BEST_COMPATIBILITY`: 精确匹配 fingerprint 优先（ExactMatchFirst 策略）
//! - `NEAREST`: 按地理距离最近（基于 Haversine 公式）
//! - `CLIENT_AFFINITY`: 按客户端亲和性（会话保持）
//!
//! # 使用示例
//! ```ignore
//! use signaling::load_balancer::LoadBalancer;
//! use signaling::service_registry::ServiceInfo;
//! use actr_protocol::route_candidates_request::node_selection_criteria::NodeRankingFactor;
//!
//! let mut candidates: Vec<ServiceInfo> = vec![]; // 获取候选列表
//! let criteria = Some(&NodeSelectionCriteria {
//!     candidate_count: 3,
//!     ranking_factors: vec![
//!         NodeRankingFactor::MaximumPowerReserve as i32,
//!         NodeRankingFactor::MinimumMailboxBacklog as i32,
//!     ],
//!     minimal_health_requirement: None,
//!     minimal_dependency_requirement: None,
//! });
//!
//! let ranked = LoadBalancer::rank_candidates(candidates, criteria, "", None, None);
//! // 返回排序后的候选 ActrId 列表
//! ```

use crate::service_registry::ServiceInfo;
use actr_protocol::{
    ActrId, ServiceAvailabilityState, ServiceDependencyState,
    route_candidates_request::{NodeSelectionCriteria, node_selection_criteria::NodeRankingFactor},
};

/// 负载均衡器
pub struct LoadBalancer;

impl LoadBalancer {
    /// 根据选择标准对候选服务进行排序
    ///
    /// # 参数
    /// - `candidates`: 候选服务列表
    /// - `criteria`: 节点选择标准（包含排序因子、最小健康要求等）
    /// - `client_id`: 可选的客户端 ID（用于 CLIENT_AFFINITY）
    /// - `client_location`: 可选的客户端地理坐标 (latitude, longitude)（用于 NEAREST）
    /// # 返回
    /// 排序后的 ActrId 列表（最多返回 candidate_count 个）
    ///
    /// # 实现逻辑
    /// 1. 应用健康和依赖过滤
    /// 2. 按排序因子依次排序
    /// 3. 返回前 N 个候选
    pub fn rank_candidates(
        mut candidates: Vec<ServiceInfo>,
        criteria: Option<&NodeSelectionCriteria>,
        client_fingerprint: &str,
        client_id: Option<&str>,
        client_location: Option<(f64, f64)>,
    ) -> Vec<ActrId> {
        if candidates.is_empty() {
            return Vec::new();
        }

        // 如果没有指定标准，返回所有候选
        let criteria = match criteria {
            Some(c) => c,
            None => {
                platform::recording::info!("未指定选择标准，返回所有候选");
                return candidates.into_iter().map(|s| s.actor_id).collect();
            }
        };

        platform::recording::info!(
            "负载均衡排序: 候选数量={}, 排序因子数量={}",
            candidates.len(),
            criteria.ranking_factors.len()
        );

        // 1. 应用健康要求过滤
        if let Some(min_health) = criteria.minimal_health_requirement {
            candidates = Self::filter_by_health(&candidates, min_health);
            platform::recording::debug!("健康过滤后剩余: {} 个", candidates.len());
        }

        // 2. 应用依赖要求过滤
        if let Some(min_dependency) = criteria.minimal_dependency_requirement {
            candidates = Self::filter_by_dependency(&candidates, min_dependency);
            platform::recording::debug!("依赖过滤后剩余: {} 个", candidates.len());
        }

        if candidates.is_empty() {
            platform::recording::warn!("过滤后无可用候选");
            return Vec::new();
        }

        // 标记精确匹配（fingerprint 对比在 LB 内完成，供 ExactMatchFirst 因子使用）
        if !client_fingerprint.is_empty() {
            for c in &mut candidates {
                let fp = c.service_spec.as_ref().map(|s| s.fingerprint.as_str()).unwrap_or("");
                c.is_exact_match = fp == client_fingerprint;
            }
        }

        // 3. 单次多键排序：按 ranking_factors 优先级依次作为主键/次键
        let factors: Vec<NodeRankingFactor> = criteria
            .ranking_factors
            .iter()
            .filter_map(|&f| NodeRankingFactor::try_from(f).ok())
            .collect();

        if !factors.is_empty() {
            candidates.sort_by(|a, b| {
                for factor in &factors {
                    let ord = Self::cmp_by_factor(a, b, factor, client_id, client_location);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // 若存在非精确匹配候选且有 fingerprint，记录日志
        if !client_fingerprint.is_empty() && candidates.iter().any(|c| !c.is_exact_match) {
            platform::recording::warn!(
                "⚠️  非精确 fingerprint 匹配候选被选入结果（client_fp={}）",
                client_fingerprint
            );
        }

        // 4. 返回前 N 个候选
        let limit = criteria.candidate_count as usize;
        candidates
            .into_iter()
            .take(limit)
            .map(|s| s.actor_id)
            .collect()
    }

    /// 按健康要求过滤
    ///
    /// 健康状态优先级排序：FULL > DEGRADED > None(未知) > OVERLOADED > UNAVAILABLE
    /// 过滤掉所有低于 min_health 要求的候选
    fn filter_by_health(candidates: &[ServiceInfo], min_health: i32) -> Vec<ServiceInfo> {
        platform::recording::debug!(
            "应用健康过滤: min_health={}",
            ServiceAvailabilityState::try_from(min_health)
                .map(|s| format!("{s:?}"))
                .unwrap_or_else(|_| "Invalid".to_string())
        );

        let mut filtered: Vec<ServiceInfo> = candidates
            .iter()
            .filter(|s| {
                match s.service_availability_state {
                    Some(service_availability_state) => {
                        // 数值越小越健康（FULL=0, DEGRADED=1, OVERLOADED=2, UNAVAILABLE=3）
                        service_availability_state <= min_health
                    }
                    None => {
                        // None 视为亚健康（介于 DEGRADED 和 OVERLOADED 之间）
                        // 如果要求 FULL 或 DEGRADED，则 None 符合
                        // 如果要求 OVERLOADED 或 UNAVAILABLE，则 None 也符合
                        min_health >= ServiceAvailabilityState::Degraded as i32
                    }
                }
            })
            .cloned()
            .collect();

        platform::recording::debug!(
            "健康过滤后: {} -> {} 个候选",
            candidates.len(),
            filtered.len()
        );

        // 按健康状态排序：FULL(0) > DEGRADED(1) > None(视为1.5) > OVERLOADED(2) > UNAVAILABLE(3)
        filtered.sort_by(|a, b| {
            let a_health = a.service_availability_state.unwrap_or(2); // None 视为介于 DEGRADED 和 OVERLOADED 之间
            let b_health = b.service_availability_state.unwrap_or(2);
            a_health.cmp(&b_health)
        });

        filtered
    }

    /// 按依赖要求过滤
    ///
    /// 依赖状态优先级排序：HEALTHY > WARNING > None(未知) > BROKEN
    /// 过滤掉所有低于 min_dependency 要求的候选
    fn filter_by_dependency(candidates: &[ServiceInfo], min_dependency: i32) -> Vec<ServiceInfo> {
        platform::recording::debug!(
            "应用依赖过滤: min_dependency={}",
            ServiceDependencyState::try_from(min_dependency)
                .map(|s| format!("{s:?}"))
                .unwrap_or_else(|_| "Invalid".to_string())
        );

        let mut filtered: Vec<ServiceInfo> = candidates
            .iter()
            .filter(|s| {
                match s.worst_dependency_health_state {
                    Some(worst_dependency_health_state) => {
                        // 数值越小依赖越健康（HEALTHY=0, WARNING=1, BROKEN=2）
                        worst_dependency_health_state <= min_dependency
                    }
                    None => {
                        // None 视为警告状态（介于 WARNING 和 BROKEN 之间）
                        min_dependency >= ServiceDependencyState::Warning as i32
                    }
                }
            })
            .cloned()
            .collect();

        platform::recording::debug!(
            "依赖过滤后: {} -> {} 个候选",
            candidates.len(),
            filtered.len()
        );

        // 按依赖健康状态排序：HEALTHY(0) > WARNING(1) > None(视为1.5) > BROKEN(2)
        filtered.sort_by(|a, b| {
            let a_dep = a.worst_dependency_health_state.unwrap_or(2); // None 视为介于 WARNING 和 BROKEN 之间
            let b_dep = b.worst_dependency_health_state.unwrap_or(2);
            a_dep.cmp(&b_dep)
        });

        filtered
    }

    /// 按单个因子比较两个候选（用于多键排序）
    fn cmp_by_factor(
        a: &ServiceInfo,
        b: &ServiceInfo,
        factor: &NodeRankingFactor,
        client_id: Option<&str>,
        client_location: Option<(f64, f64)>,
    ) -> std::cmp::Ordering {
        match factor {
            NodeRankingFactor::MaximumPowerReserve => Self::cmp_power_reserve(a, b),
            NodeRankingFactor::MinimumMailboxBacklog => Self::cmp_mailbox_backlog(a, b),
            NodeRankingFactor::ExactMatchFirst => Self::cmp_exact_match(a, b),
            NodeRankingFactor::Nearest => Self::cmp_distance(a, b, client_location),
            NodeRankingFactor::ClientAffinity => Self::cmp_affinity(a, b, client_id),
        }
    }

    fn cmp_power_reserve(a: &ServiceInfo, b: &ServiceInfo) -> std::cmp::Ordering {
        b.power_reserve
            .unwrap_or(0.0)
            .partial_cmp(&a.power_reserve.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    }

    fn cmp_mailbox_backlog(a: &ServiceInfo, b: &ServiceInfo) -> std::cmp::Ordering {
        a.mailbox_backlog
            .unwrap_or(1.0)
            .partial_cmp(&b.mailbox_backlog.unwrap_or(1.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    }

    fn cmp_exact_match(a: &ServiceInfo, b: &ServiceInfo) -> std::cmp::Ordering {
        // 精确匹配者排前面（true > false）
        match (a.is_exact_match, b.is_exact_match) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn cmp_distance(
        a: &ServiceInfo,
        b: &ServiceInfo,
        client_location: Option<(f64, f64)>,
    ) -> std::cmp::Ordering {
        // 距离近者排前面，复用 haversine 逻辑
        if let (Some((clat, clon)), Some(a_loc), Some(b_loc)) = (
            client_location,
            &a.geo_location,
            &b.geo_location,
        ) {
            if let (Some(a_lat), Some(a_lon), Some(b_lat), Some(b_lon)) = (
                a_loc.latitude,
                a_loc.longitude,
                b_loc.latitude,
                b_loc.longitude,
            ) {
                let dist_a = crate::geo::haversine_distance(clat, clon, a_lat, a_lon);
                let dist_b = crate::geo::haversine_distance(clat, clon, b_lat, b_lon);
                return dist_a
                    .partial_cmp(&dist_b)
                    .unwrap_or(std::cmp::Ordering::Equal);
            }
        }
        std::cmp::Ordering::Equal
    }

    fn cmp_affinity(
        a: &ServiceInfo,
        b: &ServiceInfo,
        client_id: Option<&str>,
    ) -> std::cmp::Ordering {
        if let Some(cid) = client_id {
            let a_sticky = a.sticky_client_ids.contains(&cid.to_string());
            let b_sticky = b.sticky_client_ids.contains(&cid.to_string());
            match (a_sticky, b_sticky) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            }
        } else {
            std::cmp::Ordering::Equal
        }
    }

}

#[cfg(test)]
impl LoadBalancer {
    pub fn sort_by_power_reserve(candidates: &mut [ServiceInfo]) {
        candidates.sort_by(Self::cmp_power_reserve);
    }

    pub fn sort_by_mailbox_backlog(candidates: &mut [ServiceInfo]) {
        candidates.sort_by(Self::cmp_mailbox_backlog);
    }

    pub fn sort_by_affinity(candidates: &mut [ServiceInfo], client_id: Option<&str>) {
        candidates.sort_by(|a, b| Self::cmp_affinity(a, b, client_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::{ActrType, Realm};

    fn create_test_service(serial: u64, name: &str) -> ServiceInfo {
        ServiceInfo {
            actor_id: ActrId {
                serial_number: serial,
                r#type: ActrType {
                    manufacturer: "test".to_string(),
                    name: name.to_string(),
                    version: String::new(),
                },
                realm: Realm { realm_id: 0 },
            },
            service_name: name.to_string(),
            message_types: vec![],
            capabilities: None,
            status: crate::service_registry::ServiceStatus::Available,
            last_heartbeat_time_secs: 0,
            service_spec: None,
            acl: None,
            service_availability_state: None,
            power_reserve: None,
            mailbox_backlog: None,
            worst_dependency_health_state: None,
            geo_location: None,
            is_exact_match: false,
            sticky_client_ids: Vec::new(),
            ws_address: None,
        }
    }

    #[test]
    fn test_rank_candidates_without_criteria() {
        let candidates = vec![
            create_test_service(1, "service-1"),
            create_test_service(2, "service-2"),
        ];

        let ranked = LoadBalancer::rank_candidates(candidates, None, "", None, None);
        assert_eq!(ranked.len(), 2);
    }

    #[test]
    fn test_rank_candidates_with_limit() {
        let candidates = vec![
            create_test_service(1, "service-1"),
            create_test_service(2, "service-2"),
            create_test_service(3, "service-3"),
        ];

        let criteria = NodeSelectionCriteria {
            candidate_count: 2,
            ranking_factors: vec![],
            minimal_dependency_requirement: None,
            minimal_health_requirement: None,
        };

        let ranked =
            LoadBalancer::rank_candidates(candidates, Some(&criteria), "", None, None);
        assert_eq!(ranked.len(), 2);
    }

    #[test]
    fn test_empty_candidates() {
        let candidates = vec![];
        let ranked = LoadBalancer::rank_candidates(candidates, None, "", None, None);
        assert_eq!(ranked.len(), 0);
    }

    // ========================================================================
    // 健康和依赖过滤测试
    // ========================================================================

    #[test]
    fn test_health_filter_full_only() {
        let mut s1 = create_test_service(1, "s1");
        s1.service_availability_state = Some(ServiceAvailabilityState::Full as i32);
        let mut s2 = create_test_service(2, "s2");
        s2.service_availability_state = Some(ServiceAvailabilityState::Degraded as i32);
        let mut s3 = create_test_service(3, "s3");
        s3.service_availability_state = Some(ServiceAvailabilityState::Overloaded as i32);

        let candidates = vec![s1.clone(), s2, s3];
        let filtered =
            LoadBalancer::filter_by_health(&candidates, ServiceAvailabilityState::Full as i32);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].actor_id.serial_number, 1);
    }

    #[test]
    fn test_health_filter_with_none() {
        let mut s1 = create_test_service(1, "s1");
        s1.service_availability_state = Some(ServiceAvailabilityState::Full as i32);
        let s2 = create_test_service(2, "s2"); // None
        let mut s3 = create_test_service(3, "s3");
        s3.service_availability_state = Some(ServiceAvailabilityState::Unavailable as i32);

        let candidates = vec![s1.clone(), s2.clone(), s3];

        // 要求 DEGRADED 或更好，None 应该通过
        let filtered =
            LoadBalancer::filter_by_health(&candidates, ServiceAvailabilityState::Degraded as i32);
        assert_eq!(filtered.len(), 2); // s1(FULL) 和 s2(None)

        // 排序应该是 FULL < None
        assert_eq!(filtered[0].actor_id.serial_number, 1); // FULL 排第一
        assert_eq!(filtered[1].actor_id.serial_number, 2); // None 排第二
    }

    #[test]
    fn test_dependency_filter_healthy_only() {
        let mut s1 = create_test_service(1, "s1");
        s1.worst_dependency_health_state = Some(ServiceDependencyState::Healthy as i32);
        let mut s2 = create_test_service(2, "s2");
        s2.worst_dependency_health_state = Some(ServiceDependencyState::Warning as i32);
        let mut s3 = create_test_service(3, "s3");
        s3.worst_dependency_health_state = Some(ServiceDependencyState::Broken as i32);

        let candidates = vec![s1.clone(), s2, s3];
        let filtered =
            LoadBalancer::filter_by_dependency(&candidates, ServiceDependencyState::Healthy as i32);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].actor_id.serial_number, 1);
    }

    // ========================================================================
    // 单因子排序测试
    // ========================================================================

    #[test]
    fn test_sort_by_power_reserve() {
        let mut s1 = create_test_service(1, "s1");
        s1.power_reserve = Some(0.3);
        let mut s2 = create_test_service(2, "s2");
        s2.power_reserve = Some(0.9);
        let mut s3 = create_test_service(3, "s3");
        s3.power_reserve = Some(0.5);
        let s4 = create_test_service(4, "s4"); // None

        let mut candidates = vec![s1, s2, s3, s4];
        LoadBalancer::sort_by_power_reserve(&mut candidates);

        // 应该是降序：0.9 > 0.5 > 0.3，None 在最后
        assert_eq!(candidates[0].actor_id.serial_number, 2); // 0.9
        assert_eq!(candidates[1].actor_id.serial_number, 3); // 0.5
        assert_eq!(candidates[2].actor_id.serial_number, 1); // 0.3
        assert_eq!(candidates[3].actor_id.serial_number, 4); // None
    }

    #[test]
    fn test_sort_by_mailbox_backlog() {
        let mut s1 = create_test_service(1, "s1");
        s1.mailbox_backlog = Some(0.7);
        let mut s2 = create_test_service(2, "s2");
        s2.mailbox_backlog = Some(0.2);
        let mut s3 = create_test_service(3, "s3");
        s3.mailbox_backlog = Some(0.5);
        let s4 = create_test_service(4, "s4"); // None

        let mut candidates = vec![s1, s2, s3, s4];
        LoadBalancer::sort_by_mailbox_backlog(&mut candidates);

        // 应该是升序：0.2 < 0.5 < 0.7，None 在最后
        assert_eq!(candidates[0].actor_id.serial_number, 2); // 0.2
        assert_eq!(candidates[1].actor_id.serial_number, 3); // 0.5
        assert_eq!(candidates[2].actor_id.serial_number, 1); // 0.7
        assert_eq!(candidates[3].actor_id.serial_number, 4); // None
    }

    #[test]
    fn test_sort_by_affinity_sticky_clients() {
        let mut s1 = create_test_service(1, "s1");
        s1.sticky_client_ids = vec!["client-A".to_string(), "client-B".to_string()];
        let mut s2 = create_test_service(2, "s2");
        s2.sticky_client_ids = vec!["client-C".to_string()];
        let s3 = create_test_service(3, "s3"); // 空列表

        let mut candidates = vec![s1.clone(), s2.clone(), s3.clone()];

        // 测试：client-C 应该路由到 s2
        LoadBalancer::sort_by_affinity(&mut candidates, Some("client-C"));
        assert_eq!(candidates[0].actor_id.serial_number, 2); // s2 粘滞匹配

        // 测试：client-A 应该路由到 s1
        let mut candidates = vec![s1.clone(), s2.clone(), s3.clone()];
        LoadBalancer::sort_by_affinity(&mut candidates, Some("client-A"));
        assert_eq!(candidates[0].actor_id.serial_number, 1); // s1 粘滞匹配

        // 测试：client-X（不在任何粘滞列表）所有候选同等优先级
        let mut candidates = vec![s1, s2, s3];
        LoadBalancer::sort_by_affinity(&mut candidates, Some("client-X"));
        // 无粘滞匹配，保持原序
        assert_eq!(candidates[0].actor_id.serial_number, 1);
    }

    // ========================================================================
    // 多因子组合排序测试
    // ========================================================================

    #[test]
    fn test_multi_factor_ranking() {
        let mut s1 = create_test_service(1, "s1");
        s1.power_reserve = Some(0.8);
        s1.mailbox_backlog = Some(0.3);

        let mut s2 = create_test_service(2, "s2");
        s2.power_reserve = Some(0.8); // 相同 power
        s2.mailbox_backlog = Some(0.1); // 但 backlog 更小

        let mut s3 = create_test_service(3, "s3");
        s3.power_reserve = Some(0.5);
        s3.mailbox_backlog = Some(0.05); // backlog 最小，但 power 低

        let candidates = vec![s1, s2, s3];
        let criteria = NodeSelectionCriteria {
            candidate_count: 10,
            ranking_factors: vec![
                NodeRankingFactor::MaximumPowerReserve as i32,
                NodeRankingFactor::MinimumMailboxBacklog as i32,
            ],
            minimal_health_requirement: None,
            minimal_dependency_requirement: None,
        };

        let ranked =
            LoadBalancer::rank_candidates(candidates, Some(&criteria), "", None, None);

        // 多键排序：power_reserve 为主键，mailbox_backlog 为次键
        // s1 和 s2 的 power 相同(0.8)，此时 backlog 决定顺序：s2(0.1) < s1(0.3)
        // s3 的 power(0.5) < s1/s2(0.8)，排最后
        assert_eq!(ranked[0].serial_number, 2); // power=0.8, backlog=0.1
        assert_eq!(ranked[1].serial_number, 1); // power=0.8, backlog=0.3
        assert_eq!(ranked[2].serial_number, 3); // power=0.5
    }

    // ========================================================================
    // 边界情况测试
    // ========================================================================

    #[test]
    fn test_all_none_values() {
        let candidates = vec![
            create_test_service(1, "s1"),
            create_test_service(2, "s2"),
            create_test_service(3, "s3"),
        ];

        let criteria = NodeSelectionCriteria {
            candidate_count: 10,
            ranking_factors: vec![
                NodeRankingFactor::MaximumPowerReserve as i32,
                NodeRankingFactor::MinimumMailboxBacklog as i32,
            ],
            minimal_health_requirement: None,
            minimal_dependency_requirement: None,
        };

        let ranked =
            LoadBalancer::rank_candidates(candidates, Some(&criteria), "", None, None);
        assert_eq!(ranked.len(), 3); // 全部保留，顺序不变
    }

    #[test]
    fn test_all_same_values() {
        let mut s1 = create_test_service(1, "s1");
        s1.power_reserve = Some(0.5);
        let mut s2 = create_test_service(2, "s2");
        s2.power_reserve = Some(0.5);
        let mut s3 = create_test_service(3, "s3");
        s3.power_reserve = Some(0.5);

        let mut candidates = vec![s1, s2, s3];
        LoadBalancer::sort_by_power_reserve(&mut candidates);

        // 所有值相同，应该保持稳定排序
        assert_eq!(candidates[0].actor_id.serial_number, 1);
        assert_eq!(candidates[1].actor_id.serial_number, 2);
        assert_eq!(candidates[2].actor_id.serial_number, 3);
    }

    #[test]
    fn test_filter_removes_all_candidates() {
        let mut s1 = create_test_service(1, "s1");
        s1.service_availability_state = Some(ServiceAvailabilityState::Unavailable as i32);
        let mut s2 = create_test_service(2, "s2");
        s2.service_availability_state = Some(ServiceAvailabilityState::Overloaded as i32);

        let candidates = vec![s1, s2];
        let criteria = NodeSelectionCriteria {
            candidate_count: 10,
            ranking_factors: vec![],
            minimal_health_requirement: Some(ServiceAvailabilityState::Full as i32),
            minimal_dependency_requirement: None,
        };

        let ranked =
            LoadBalancer::rank_candidates(candidates, Some(&criteria), "", None, None);
        assert_eq!(ranked.len(), 0); // 全部被过滤
    }

    #[test]
    fn test_sort_by_distance_with_client_location() {
        use crate::service_registry::ServiceLocation;

        // 客户端位置：北京（39.9042, 116.4074）
        let client_location = Some((39.9042, 116.4074));

        // 候选服务：上海、深圳、北京
        let mut s1 = create_test_service(1, "shanghai");
        s1.geo_location = Some(ServiceLocation {
            region: "cn-east".to_string(),
            latitude: Some(31.2304),
            longitude: Some(121.4737),
        });

        let mut s2 = create_test_service(2, "shenzhen");
        s2.geo_location = Some(ServiceLocation {
            region: "cn-south".to_string(),
            latitude: Some(22.5431),
            longitude: Some(114.0579),
        });

        let mut s3 = create_test_service(3, "beijing");
        s3.geo_location = Some(ServiceLocation {
            region: "cn-north".to_string(),
            latitude: Some(39.9042),
            longitude: Some(116.4074),
        });

        let s4 = create_test_service(4, "unknown"); // 无坐标

        let candidates = vec![s1, s2, s3, s4];
        let criteria = NodeSelectionCriteria {
            candidate_count: 10,
            ranking_factors: vec![NodeRankingFactor::Nearest as i32],
            minimal_health_requirement: None,
            minimal_dependency_requirement: None,
        };

        let ranked = LoadBalancer::rank_candidates(
            candidates,
            Some(&criteria),
            "",
            None,
            client_location,
        );

        // 排序结果应该是：北京(0km) < 上海(~1067km) < 深圳(~1943km)，无坐标的在最后
        assert_eq!(ranked.len(), 4);
        assert_eq!(ranked[0].serial_number, 3); // 北京（最近）
        assert_eq!(ranked[1].serial_number, 1); // 上海
        assert_eq!(ranked[2].serial_number, 2); // 深圳
        assert_eq!(ranked[3].serial_number, 4); // 无坐标
    }

}
