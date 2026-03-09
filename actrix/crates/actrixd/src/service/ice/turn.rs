//! TURN服务实现

use crate::service::IceService;
use anyhow::Result;
use async_trait::async_trait;
use platform::config::ActrixConfig;
use platform::monitoring::ServiceCounters;
use platform::status::services::ServiceState;
use platform::{ServiceInfo, ServiceType};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;
use url::Url;

/// TURN服务实现
#[derive(Debug, Clone)]
pub struct TurnService {
    info: ServiceInfo,
    config: ActrixConfig,
    socket: Option<Arc<UdpSocket>>,
    /// Service-level counters for metrics collection.
    pub counters: Option<Arc<ServiceCounters>>,
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
            counters: None,
        }
    }

    /// Attach service-level counters.
    pub fn with_counters(mut self, counters: Arc<ServiceCounters>) -> Self {
        self.counters = Some(counters);
        self
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
        let ip = ice_bind.ip.parse::<IpAddr>().map_err(|e| {
            anyhow::anyhow!(
                "Invalid bind.ice.ip '{}': {} (expected IPv4/IPv6 literal)",
                ice_bind.ip,
                e
            )
        })?;
        let addr = SocketAddr::new(ip, ice_bind.port);

        platform::recording::info!("Starting TURN service on {}", addr);

        // 绑定UDP套接字
        let socket = match UdpSocket::bind(addr).await {
            Ok(socket) => {
                platform::recording::info!("TURN service listening on: {}", addr);
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
            turn::Authenticator::new(self.config.turn.turn_secret.clone())
                .map_err(|e| anyhow::anyhow!("Failed to create TURN authenticator: {e}"))?,
        );

        let turn_server = match turn::create_turn_server(
            socket.clone(),
            &self.config.bind.ice.advertised_ip,
            &realm,
            auth_handler,
        )
        .await
        {
            Ok(server) => {
                let url = Url::parse(&format!(
                    "turn:{}:{}?transport=udp",
                    ice_bind.ip, ice_bind.port
                ))?;
                self.info.set_running(url);
                // Attach counters to ServiceInfo and record TURN allocation
                if let Some(ref ctr) = self.counters {
                    self.info.set_counters(ctr.clone());
                    ctr.inc_conns();
                    ctr.record_request(true, 0.0).await;
                }
                oneshot_tx
                    .send(self.info.clone())
                    .map_err(|e| anyhow::anyhow!("Failed to send TURN service info: {e:?}"))?;
                platform::recording::info!("TURN service started successfully");
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
        platform::recording::info!("TURN service received shutdown signal");

        // 关闭TURN服务器
        if let Err(e) = turn::shutdown_turn_server(&turn_server).await {
            platform::recording::error!("Error shutting down TURN server: {}", e);
        }

        self.stop().await?;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        platform::recording::info!("Stopping TURN service");

        // Decrement active connection on stop
        if let Some(ref ctr) = self.counters {
            ctr.dec_conns();
        }

        self.socket = None;
        self.info.status = ServiceState::Unknown;

        platform::recording::info!("TURN service stopped");
        Ok(())
    }
}
