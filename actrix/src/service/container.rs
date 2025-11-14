//\! 服务容器
//\!
//\! 管理各种服务的容器和生命周期
//! 服务容器模块 - 封装不同类型的服务

use super::{
    AisService, KsHttpService, SignalingService, StunService, SupervisorService, TurnService,
};
use super::{HttpRouterService, IceService};
use crate::service::info::ServiceInfo;
use axum::Router;
use url::Url;

/// 服务容器，用于封装不同类型的服务
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ServiceContainer {
    Supervit(SupervisorService),
    Signaling(SignalingService),
    Ais(AisService),
    Ks(KsHttpService),
    Stun(StunService),
    Turn(TurnService),
}

impl ServiceContainer {
    /// 创建Supervit服务容器 (supervisor client)
    pub fn supervisor(service: SupervisorService) -> Self {
        Self::Supervit(service)
    }

    /// 创建Signaling服务容器
    pub fn signaling(service: SignalingService) -> Self {
        Self::Signaling(service)
    }

    /// 创建AIS服务容器
    pub fn ais(service: AisService) -> Self {
        Self::Ais(service)
    }

    /// 创建KS服务容器
    pub fn ks(service: KsHttpService) -> Self {
        Self::Ks(service)
    }

    /// 创建STUN服务容器
    pub fn stun(service: StunService) -> Self {
        Self::Stun(service)
    }

    /// 创建TURN服务容器
    pub fn turn(service: TurnService) -> Self {
        Self::Turn(service)
    }

    #[allow(dead_code)]
    pub fn service_type(&self) -> &'static str {
        match self {
            ServiceContainer::Supervit(_) => "Supervit",
            ServiceContainer::Signaling(_) => "Signaling",
            ServiceContainer::Ais(_) => "AIS",
            ServiceContainer::Ks(_) => "KS",
            ServiceContainer::Stun(_) => "STUN",
            ServiceContainer::Turn(_) => "TURN",
        }
    }

    pub fn info(&self) -> &ServiceInfo {
        match self {
            ServiceContainer::Supervit(service) => service.info(),
            ServiceContainer::Signaling(service) => service.info(),
            ServiceContainer::Ais(service) => service.info(),
            ServiceContainer::Ks(service) => service.info(),
            ServiceContainer::Stun(service) => service.info(),
            ServiceContainer::Turn(service) => service.info(),
        }
    }

    pub fn is_http_router(&self) -> bool {
        matches!(
            self,
            ServiceContainer::Supervit(_)
                | ServiceContainer::Signaling(_)
                | ServiceContainer::Ais(_)
                | ServiceContainer::Ks(_)
        )
    }

    pub fn is_ice(&self) -> bool {
        matches!(self, ServiceContainer::Stun(_) | ServiceContainer::Turn(_))
    }

    /// 获取路由前缀（仅适用于 HTTP 路由服务）
    pub fn route_prefix(&self) -> Option<&str> {
        match self {
            ServiceContainer::Supervit(service) => Some(service.route_prefix()),
            ServiceContainer::Signaling(service) => Some(service.route_prefix()),
            ServiceContainer::Ais(service) => Some(service.route_prefix()),
            ServiceContainer::Ks(service) => Some(service.route_prefix()),
            _ => None,
        }
    }

    /// 构建路由器（仅适用于 HTTP 路由服务）
    pub async fn build_router(&mut self) -> Option<Result<Router, anyhow::Error>> {
        match self {
            ServiceContainer::Supervit(service) => Some(service.build_router().await),
            ServiceContainer::Signaling(service) => Some(service.build_router().await),
            ServiceContainer::Ais(service) => Some(service.build_router().await),
            ServiceContainer::Ks(service) => Some(service.build_router().await),
            _ => None,
        }
    }

    /// 服务启动回调（仅适用于 HTTP 路由服务）
    pub async fn on_start(&mut self, base_url: Url) -> Option<Result<(), anyhow::Error>> {
        match self {
            ServiceContainer::Supervit(service) => Some(service.on_start(base_url).await),
            ServiceContainer::Signaling(service) => Some(service.on_start(base_url).await),
            ServiceContainer::Ais(service) => Some(service.on_start(base_url).await),
            ServiceContainer::Ks(service) => Some(service.on_start(base_url).await),
            _ => None,
        }
    }
}
