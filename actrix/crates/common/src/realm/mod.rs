//! Realm 管理模块
//!
//! 提供多 Realm 管理功能，包括 Realm 配置、权限控制、数据迁移等
//!
//! 按照概念独立性原则组织，每个概念都有独立的文件：
//! - `model.rs` - 核心 Realm 数据结构
//! - `repository.rs` - 数据库操作
//! - `validation.rs` - 业务规则验证
//! - `compatibility.rs` - 向后兼容性支持

// 子模块
pub mod acl;
pub mod compatibility;
pub mod config;
pub mod error;
pub mod model;
pub mod repository;
pub mod service_type;
pub mod validation;

// 公共API导出
pub use acl::ActorAcl;
pub use config::RealmConfig;
pub use error::RealmError;
pub use model::{Realm, RealmStatus};
pub use service_type::ServiceType;
