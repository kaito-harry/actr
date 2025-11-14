//! 服务管理模块
//!
//! 管理各种辅助服务的生命周期
//! # Service Management Abstraction
//!
//! 提供通用的服务管理抽象，用于细粒度地管理不同类型的服务（STUN、TURN、Signaling、Admin等）
//!
//! ## 核心概念
//!
//! - `HttpRouterService`: HTTP路由服务的核心 trait，提供 axum 路由器
//! - `IceService`: ICE服务的核心 trait，独立的 UDP 服务器
//! - `ServiceInfo`: 服务的基本信息
//! - `ServiceManager`: 服务管理器，负责管理多个服务的生命周期

pub mod container;
pub mod grpc;
pub mod http;
pub mod ice;
pub mod info;
pub mod manager;
pub mod trace;

use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use info::ServiceInfo;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use strum::Display;
// TODO: supervit 暂时禁用，等待重构
// use supervit::ResourceType;
use tracing::info;
use url::Url;

// 重新导出服务实现
pub use grpc::KsGrpcService;
pub use http::{AisService, KsHttpService, SignalingService, SupervisorService};
pub use ice::{StunService, TurnService};

// 重新导出核心组件
pub use container::ServiceContainer;
pub use manager::ServiceManager;

// 重新导出 ServiceStatus 类型供外部使用
pub use actrix_common::status::services::ServiceStatus;

/// 服务类型
#[derive(Debug, Clone, Serialize, Deserialize, Display, PartialEq, Eq)]
pub enum ServiceType {
    Stun,
    Turn,
    Signaling,
    Supervisor,
    Ais,
    Ks,
}

// TODO: supervit 暂时禁用，等待重构
// impl From<ServiceType> for ResourceType {
//     fn from(service_type: ServiceType) -> Self {
//         match service_type {
//             ServiceType::Signaling => ResourceType::Signaling,
//             ServiceType::Stun => ResourceType::Stun,
//             ServiceType::Turn => ResourceType::Turn,
//             ServiceType::Supervisor => ResourceType::Supervisor,
//             ServiceType::Ais => ResourceType::Authority, // AIS 使用 Authority 资源类型
//             ServiceType::Ks => ResourceType::Authority,  // KS 使用 Authority 资源类型
//         }
//     }
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceRegistrationPayload {
    pub resource_id: String,
    pub secret: Vec<u8>,
    pub public_key: Option<Vec<u8>>,
    pub services: Vec<ServiceInfo>,
    pub location: String,
    pub name: String,
    pub location_tag: Option<String>,
    pub service_tag: Option<Vec<String>>,
    pub power_reserve: u8,
}

/// HTTP路由服务的核心 trait - 为 axum 提供路由器
#[async_trait]
pub trait HttpRouterService: Send + Sync + Debug {
    /// 获取服务信息
    fn info(&self) -> &ServiceInfo;

    /// 获取可变的服务信息
    fn info_mut(&mut self) -> &mut ServiceInfo;

    /// 构建axum路由器
    async fn build_router(&mut self) -> Result<Router>;

    /// 服务启动回调（路由器已构建并启动后调用）
    async fn on_start(&mut self, base_url: Url) -> Result<()> {
        self.info_mut().set_running(base_url);
        Ok(())
    }

    /// 服务停止回调
    async fn on_stop(&mut self) -> Result<()> {
        info!("HTTP router service '{}' stopped", self.info().name);
        self.info_mut().status = ServiceStatus::Unknown;
        Ok(())
    }

    /// 获取路由前缀（如 "/admin", "/authority" 等）
    fn route_prefix(&self) -> &str;
}

/// ICE服务的核心 trait - 独立的 UDP 服务器
#[async_trait]
pub trait IceService: Send + Sync + Debug {
    /// 获取服务信息
    fn info(&self) -> &ServiceInfo;

    /// 获取可变的服务信息
    fn info_mut(&mut self) -> &mut ServiceInfo;

    /// 启动ICE服务
    async fn start(
        &mut self,
        shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        oneshot_tx: tokio::sync::oneshot::Sender<ServiceInfo>,
    ) -> Result<()>;

    /// 停止ICE服务
    async fn stop(&mut self) -> Result<()> {
        info!("ICE service '{}' stopped", self.info().name);
        self.info_mut().status = ServiceStatus::Unknown;
        Ok(())
    }

    /// 获取服务健康状态
    #[allow(dead_code)]
    async fn health_check(&self) -> Result<bool> {
        Ok(self.info().is_running())
    }
}
