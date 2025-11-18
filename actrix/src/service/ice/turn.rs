//! TURN服务实现

use crate::service::{IceService, ServiceType, info::ServiceInfo};
use actrix_common::config::ActrixConfig;
use actrix_common::status::services::ServiceStatus;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tracing::{error, info};
use url::Url;

/// TURN服务实现
#[derive(Debug, Clone)]
pub struct TurnService {
    info: ServiceInfo,
    config: ActrixConfig,
    socket: Option<Arc<UdpSocket>>,
}

impl TurnService {
    pub fn new(config: ActrixConfig) -> Self {
        Self {
            info: ServiceInfo::new(
                "TURN Server",
                ServiceType::Turn,
                Some("TURN server with built-in STUN support for WebRTC connectivity".to_string()),
                &config,
            ),
            config,
            socket: None,
        }
    }
}

#[async_trait]
impl IceService for TurnService {
    fn info(&self) -> &ServiceInfo {
        &self.info
    }

    fn info_mut(&mut self) -> &mut ServiceInfo {
        &mut self.info
    }

    async fn start(
        &mut self,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        oneshot_tx: tokio::sync::oneshot::Sender<ServiceInfo>,
    ) -> Result<()> {
        let ice_bind = &self.config.bind.ice;
        let addr = format!("{}:{}", ice_bind.ip, ice_bind.port);

        info!("Starting TURN service on {}", addr);

        // 绑定UDP套接字
        let socket = match UdpSocket::bind(&addr).await {
            Ok(socket) => {
                info!("TURN service listening on: {}", addr);
                Arc::new(socket)
            }
            Err(e) => {
                let error_msg = format!("Failed to bind TURN service to {addr}: {e}");
                self.info.set_error(&error_msg);
                return Err(anyhow::anyhow!(error_msg));
            }
        };

        self.socket = Some(socket.clone());

        // 创建TURN服务器
        let realm = self.config.turn.realm.clone();
        let auth_handler = Arc::new(
            turn::Authenticator::new()
                .map_err(|e| anyhow::anyhow!("Failed to create TURN authenticator: {e}"))?,
        );

        let turn_server = match turn::create_turn_server(
            socket.clone(),
            &self.config.turn.advertised_ip,
            &realm,
            auth_handler,
        )
        .await
        {
            Ok(server) => {
                let url = Url::parse(&format!(
                    "turn:{}:{}?transport=udp",
                    ice_bind.domain_name, ice_bind.port
                ))?;
                self.info.set_running(url);
                oneshot_tx
                    .send(self.info.clone())
                    .map_err(|e| anyhow::anyhow!("Failed to send TURN service info: {e:?}"))?;
                info!("TURN service started successfully");
                server
            }
            Err(e) => {
                let error_msg = format!("Failed to start TURN service: {e}");
                self.info.set_error(&error_msg);
                return Err(anyhow::anyhow!(error_msg));
            }
        };

        // 等待关闭信号
        let _ = shutdown_rx.recv().await;
        info!("TURN service received shutdown signal");

        // 关闭TURN服务器
        if let Err(e) = turn::shutdown_turn_server(&turn_server).await {
            error!("Error shutting down TURN server: {}", e);
        }

        self.stop().await?;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!("Stopping TURN service");

        self.socket = None;
        self.info.status = ServiceStatus::Unknown;

        info!("TURN service stopped");
        Ok(())
    }
}
