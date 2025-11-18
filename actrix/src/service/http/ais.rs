//! AIS (Actor Identity Service) HTTP 服务实现
//!
//! 提供 ActrId 注册和 Token 签发的 HTTP API 服务

use crate::service::ServiceType;
use crate::service::{HttpRouterService, info::ServiceInfo};
use actrix_common::config::ActrixConfig;
use ais::create_ais_router;
use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use tracing::info;

/// AIS HTTP 服务实现
#[derive(Debug)]
pub struct AisService {
    info: ServiceInfo,
    config: ActrixConfig,
}

impl AisService {
    #[allow(dead_code)]
    pub fn new(config: ActrixConfig) -> Self {
        Self {
            info: ServiceInfo::new(
                "AIS Service",
                ServiceType::Ais,
                Some("Actor Identity Service - ActrId 注册和凭证签发服务".to_string()),
                &config,
            ),
            config,
        }
    }
}

#[async_trait]
impl HttpRouterService for AisService {
    fn info(&self) -> &ServiceInfo {
        &self.info
    }

    fn info_mut(&mut self) -> &mut ServiceInfo {
        &mut self.info
    }

    async fn build_router(&mut self) -> Result<Router> {
        info!("Building AIS router");

        // 获取 AIS 配置
        let ais_config = self
            .config
            .services
            .ais
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("AIS config not found"))?;

        // 创建 AIS 路由器（传递配置）
        let ais_router = create_ais_router(ais_config, &self.config).await?;

        let router = Router::new().merge(ais_router);

        info!("AIS router built successfully");
        Ok(router)
    }

    fn route_prefix(&self) -> &str {
        "/ais"
    }
}
