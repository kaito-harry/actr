//! Presence 订阅管理模块
//!
//! 负责管理 Actor 上线事件的订阅与通知机制
//!
//! # 功能
//! - 订阅特定 ActrType 的上线事件
//! - 取消订阅
//! - 当新 Actor 注册时，通知所有订阅者
//!
//! # 使用示例
//! ```ignore
//! use signaling::presence::PresenceManager;
//! use actr_protocol::{ActrId, ActrType, Realm};
//!
//! let mut manager = PresenceManager::new();
//!
//! let actor_a_id = ActrId { /* ... */ };
//! let user_service_type = ActrType {
//!     manufacturer: "acme".to_string(),
//!     name: "user-service".to_string(),
//!     version: "1.0.0".to_string(),
//! };
//!
//! // Actor A 订阅 user-service 类型的上线事件
//! manager.subscribe(actor_a_id, user_service_type.clone());
//!
//! // 当新的 user-service 实例注册时
//! let subscribers = manager.get_subscribers(&user_service_type);
//! // 向 subscribers 推送 ActrUpEvent
//! ```

use actr_protocol::{ActrId, ActrType};
use platform::RealmError;
use platform::realm::acl::ActorAcl;
use std::collections::HashMap;

/// Presence 订阅管理器
#[derive(Debug, Default)]
pub struct PresenceManager {
    /// 订阅映射表：target_type -> Vec<subscriber_actor_id>
    ///
    /// Key: 被订阅的服务类型（ActrType）
    /// Value: 订阅该类型的 Actor 列表
    subscriptions: HashMap<ActrType, Vec<ActrId>>,
}

impl PresenceManager {
    /// 创建新的 PresenceManager
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    /// 订阅特定类型的 Actor 上线事件
    ///
    /// # 参数
    /// - `subscriber`: 订阅者的 ActrId
    /// - `target_type`: 要订阅的服务类型
    ///
    /// # 示例
    /// ```ignore
    /// manager.subscribe(client_actor_id, user_service_type);
    /// ```
    pub fn subscribe(&mut self, subscriber: ActrId, target_type: ActrType) {
        platform::recording::info!(
            "Actor {} 订阅 {}/{} 上线事件",
            subscriber.to_string_repr(),
            target_type.manufacturer,
            target_type.name
        );

        let subscribers = self.subscriptions.entry(target_type).or_default();

        // 避免重复订阅
        if !subscribers.iter().any(|id| id == &subscriber) {
            subscribers.push(subscriber);
            platform::recording::debug!("订阅成功，当前订阅者数量: {}", subscribers.len());
        } else {
            platform::recording::warn!("Actor {} 已经订阅过该类型", subscriber.to_string_repr());
        }
    }

    /// 取消订阅特定类型的 Actor 上线事件
    ///
    /// # 参数
    /// - `subscriber`: 订阅者的 ActrId
    /// - `target_type`: 要取消订阅的服务类型
    ///
    /// # 返回
    /// - `true`: 成功取消订阅
    /// - `false`: 该订阅不存在
    pub fn unsubscribe(&mut self, subscriber: &ActrId, target_type: &ActrType) -> bool {
        platform::recording::info!(
            "Actor {} 取消订阅 {}/{} 上线事件",
            subscriber.to_string_repr(),
            target_type.manufacturer,
            target_type.name
        );

        if let Some(subscribers) = self.subscriptions.get_mut(target_type) {
            let original_len = subscribers.len();
            subscribers.retain(|id| id != subscriber);

            let removed = subscribers.len() < original_len;
            if removed {
                platform::recording::debug!("取消订阅成功，剩余订阅者数量: {}", subscribers.len());

                // 如果没有订阅者了，删除整个条目
                if subscribers.is_empty() {
                    self.subscriptions.remove(target_type);
                    platform::recording::debug!("该类型已无订阅者，移除订阅表条目");
                }
            } else {
                platform::recording::warn!("Actor {} 未订阅该类型", subscriber.to_string_repr());
            }

            removed
        } else {
            platform::recording::warn!(
                "类型 {}/{} 不存在任何订阅",
                target_type.manufacturer,
                target_type.name
            );
            false
        }
    }

    /// 取消指定 Actor 的所有订阅
    ///
    /// 用于 Actor 下线或断开连接时清理
    ///
    /// # 参数
    /// - `subscriber`: 订阅者的 ActrId
    ///
    /// # 返回
    /// 取消的订阅数量
    pub fn unsubscribe_all(&mut self, subscriber: &ActrId) -> usize {
        platform::recording::info!("清理 Actor {} 的所有订阅", subscriber.to_string_repr());

        let mut removed_count = 0;

        // 从所有订阅列表中移除该订阅者
        self.subscriptions.retain(|_target_type, subscribers| {
            let original_len = subscribers.len();
            subscribers.retain(|id| id != subscriber);
            removed_count += original_len - subscribers.len();

            // 如果列表为空，返回 false 以删除该条目
            !subscribers.is_empty()
        });

        if removed_count > 0 {
            platform::recording::info!("清理了 {} 个订阅", removed_count);
        }

        removed_count
    }

    /// 获取订阅了特定类型的所有 Actor
    ///
    /// # 参数
    /// - `target_type`: 服务类型
    ///
    /// # 返回
    /// 订阅者的 ActrId 列表（引用）
    ///
    /// # 用途
    /// 当有新的 target_type 实例注册时，使用此方法获取所有需要通知的订阅者
    pub fn get_subscribers(&self, target_type: &ActrType) -> Vec<&ActrId> {
        self.subscriptions
            .get(target_type)
            .map(|subscribers| subscribers.iter().collect())
            .unwrap_or_default()
    }

    /// 获取订阅统计信息
    ///
    /// # 返回
    /// (订阅的服务类型数量, 总订阅者数量)
    pub fn stats(&self) -> (usize, usize) {
        let type_count = self.subscriptions.len();
        let subscriber_count: usize = self
            .subscriptions
            .values()
            .map(|subscribers| subscribers.len())
            .sum();

        (type_count, subscriber_count)
    }

    /// 检查特定 Actor 是否订阅了某个类型
    pub fn is_subscribed(&self, subscriber: &ActrId, target_type: &ActrType) -> bool {
        self.subscriptions
            .get(target_type)
            .map(|subscribers| subscribers.iter().any(|id| id == subscriber))
            .unwrap_or(false)
    }

    /// Get subscribers with ACL filtering
    ///
    /// Returns only subscribers that are allowed to discover the target actor
    /// based on ACL rules
    ///
    /// # Arguments
    ///
    /// - `target_actor_id`: The actor that came online
    ///
    /// # Returns
    ///
    /// List of subscriber ActorIds that passed ACL checks
    pub async fn get_subscribers_with_acl(&self, target_actor_id: &ActrId) -> Vec<ActrId> {
        let target_type = &target_actor_id.r#type;
        let subscribers = self.get_subscribers(target_type);
        let total_count = subscribers.len(); // Save count before moving

        let mut allowed_subscribers = Vec::new();

        for subscriber_id in subscribers {
            // ACL check: can subscriber discover target?
            match Self::check_discovery_acl(subscriber_id, target_actor_id).await {
                Ok(true) => {
                    allowed_subscribers.push(subscriber_id.clone());
                }
                Ok(false) => {
                    platform::recording::debug!(
                        "ACL denied discovery notification: subscriber={}, target={}",
                        subscriber_id.to_string_repr(),
                        target_actor_id.to_string_repr()
                    );
                }
                Err(e) => {
                    platform::recording::warn!(
                        "ACL check failed, denying notification: subscriber={}, target={}, error={}",
                        subscriber_id.to_string_repr(),
                        target_actor_id.to_string_repr(),
                        e
                    );
                }
            }
        }

        platform::recording::info!(
            "ACL filtering completed for presence notification: target_type={:?}, total_subscribers={}, allowed_subscribers={}",
            target_type,
            total_count,
            allowed_subscribers.len()
        );

        allowed_subscribers
    }

    /// Check if discovery is allowed between two actors based on ACL rules
    ///
    /// # Arguments
    ///
    /// - `from_actor`: Subscriber actor ID
    /// - `to_actor`: Target actor ID
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

        let from_type = format!(
            "{}:{}:{}",
            from_actor.r#type.manufacturer, from_actor.r#type.name, from_actor.r#type.version
        );
        let to_type = format!(
            "{}:{}:{}",
            to_actor.r#type.manufacturer, to_actor.r#type.name, to_actor.r#type.version
        );

        ActorAcl::can_discover(from_realm, to_realm, &from_type, &to_type).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::Realm;

    fn create_test_actor_id(serial: u64) -> ActrId {
        ActrId {
            serial_number: serial,
            r#type: ActrType {
                manufacturer: "test".to_string(),
                name: "test-actor".to_string(),
                version: "1.0.0".to_string(),
            },
            realm: Realm { realm_id: 0 },
        }
    }

    fn create_test_actor_type(name: &str) -> ActrType {
        ActrType {
            manufacturer: "test".to_string(),
            name: name.to_string(),
            version: "1.0.0".to_string(),
        }
    }

    #[test]
    fn test_subscribe_and_get_subscribers() {
        let mut manager = PresenceManager::new();
        let actor1 = create_test_actor_id(1);
        let actor2 = create_test_actor_id(2);
        let target_type = create_test_actor_type("user-service");

        manager.subscribe(actor1.clone(), target_type.clone());
        manager.subscribe(actor2.clone(), target_type.clone());

        let subscribers = manager.get_subscribers(&target_type);
        assert_eq!(subscribers.len(), 2);
    }

    #[test]
    fn test_unsubscribe() {
        let mut manager = PresenceManager::new();
        let actor1 = create_test_actor_id(1);
        let target_type = create_test_actor_type("user-service");

        manager.subscribe(actor1.clone(), target_type.clone());
        assert!(manager.unsubscribe(&actor1, &target_type));

        let subscribers = manager.get_subscribers(&target_type);
        assert_eq!(subscribers.len(), 0);
    }

    #[test]
    fn test_unsubscribe_all() {
        let mut manager = PresenceManager::new();
        let actor1 = create_test_actor_id(1);
        let type1 = create_test_actor_type("user-service");
        let type2 = create_test_actor_type("order-service");

        manager.subscribe(actor1.clone(), type1.clone());
        manager.subscribe(actor1.clone(), type2.clone());

        let removed = manager.unsubscribe_all(&actor1);
        assert_eq!(removed, 2);

        assert_eq!(manager.get_subscribers(&type1).len(), 0);
        assert_eq!(manager.get_subscribers(&type2).len(), 0);
    }

    #[test]
    fn test_duplicate_subscription() {
        let mut manager = PresenceManager::new();
        let actor1 = create_test_actor_id(1);
        let target_type = create_test_actor_type("user-service");

        manager.subscribe(actor1.clone(), target_type.clone());
        manager.subscribe(actor1.clone(), target_type.clone()); // 重复订阅

        let subscribers = manager.get_subscribers(&target_type);
        assert_eq!(subscribers.len(), 1); // 应该只有一个
    }

    #[test]
    fn test_stats() {
        let mut manager = PresenceManager::new();
        let actor1 = create_test_actor_id(1);
        let actor2 = create_test_actor_id(2);
        let type1 = create_test_actor_type("user-service");
        let type2 = create_test_actor_type("order-service");

        manager.subscribe(actor1.clone(), type1.clone());
        manager.subscribe(actor2.clone(), type1.clone());
        manager.subscribe(actor1.clone(), type2.clone());

        let (type_count, subscriber_count) = manager.stats();
        assert_eq!(type_count, 2); // 2 种类型
        assert_eq!(subscriber_count, 3); // 3 个订阅关系
    }
}
