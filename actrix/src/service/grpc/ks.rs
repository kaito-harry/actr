//! KS (Key Server) gRPC 服务实现
//!
//! 提供椭圆曲线密钥生成和管理的 gRPC API 服务

use crate::service::ServiceType;
use crate::service::info::ServiceInfo;
use actrix_common::config::ActrixConfig;
use actrix_common::storage::nonce::SqliteNonceStorage;
use anyhow::Result;
use ks::{KeyEncryptor, KeyStorage, create_grpc_service};
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::{error, info};

/// KS gRPC 服务实现
#[derive(Debug)]
pub struct KsGrpcService {
    info: ServiceInfo,
    config: ActrixConfig,
}

impl KsGrpcService {
    pub fn new(config: ActrixConfig) -> Self {
        Self {
            info: ServiceInfo::new(
                "KS gRPC Service",
                ServiceType::Ks,
                Some("Key Server gRPC - 椭圆曲线密钥生成和管理服务".to_string()),
                &config,
            ),
            config,
        }
    }

    pub fn info_mut(&mut self) -> &mut ServiceInfo {
        &mut self.info
    }

    /// 启动 gRPC 服务器
    pub async fn start(
        &mut self,
        addr: SocketAddr,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<()> {
        info!("Starting KS gRPC service on {}", addr);

        // 获取 KS 服务配置
        let ks_service_config = self
            .config
            .services
            .ks
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("KS service configuration not found"))?;

        // 创建 nonce storage 实例（用于防重放攻击）
        let nonce_storage = SqliteNonceStorage::new_async(ks_service_config.nonce_db_path.clone())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create nonce storage: {e}"))?;

        // 创建密钥加密器
        let encryptor = match ks_service_config.get_kek_source() {
            Some(kek_source) => {
                info!("KEK configured, enabling private key encryption");
                KeyEncryptor::from_kek_source(&kek_source)
                    .map_err(|e| anyhow::anyhow!("Failed to create key encryptor: {e}"))?
            }
            None => {
                info!("No KEK configured, private keys will be stored in plaintext");
                KeyEncryptor::no_encryption()
            }
        };

        // 创建 KS storage
        let storage = KeyStorage::from_config(&ks_service_config.storage, encryptor)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create KS storage: {e}"))?;

        // 创建 gRPC 服务
        let grpc_service = create_grpc_service(
            storage,
            nonce_storage,
            self.config.actrix_shared_key.clone(),
        );

        info!("KS gRPC service created successfully");

        // 启动 gRPC 服务器
        let server = Server::builder()
            .add_service(grpc_service)
            .serve_with_shutdown(addr, async move {
                let _ = shutdown_rx.recv().await;
                info!("KS gRPC service received shutdown signal");
            });

        // 更新服务状态为运行中
        self.info_mut().status =
            actrix_common::status::services::ServiceStatus::Running(format!("gRPC {addr}"));

        info!("✅ KS gRPC service listening on {}", addr);

        // 运行服务器（阻塞直到关闭）
        if let Err(e) = server.await {
            error!("KS gRPC server error: {}", e);
            self.info_mut().status = actrix_common::status::services::ServiceStatus::Unknown;
            return Err(anyhow::anyhow!("KS gRPC server failed: {e}"));
        }

        info!("KS gRPC service stopped");
        self.info_mut().status = actrix_common::status::services::ServiceStatus::Unknown;
        Ok(())
    }
}
