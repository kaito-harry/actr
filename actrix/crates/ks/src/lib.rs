//! Key Server (KS) - 椭圆曲线密钥生成和管理服务
//!
//! KS 服务提供以下功能：
//! 1. 生成椭圆曲线密钥对（使用 ECIES），返回公钥给 Issue 服务
//! 2. 基于 key_id 查询私钥给验证服务
//! 3. PSK 签名验证和防重放攻击保护
//! 4. 多存储后端支持：SQLite, PostgreSQL

#[cfg(test)]
pub mod client;
pub mod config;
pub mod crypto;
pub mod error;
pub mod grpc_client;
pub mod grpc_handlers;
pub mod handlers;
pub mod storage;
pub mod types;

// Re-export commonly used items
#[cfg(test)]
pub use client::{Client, ClientConfig};
pub use config::KsServiceConfig;
pub use crypto::{KekSource, KeyEncryptor};
pub use error::KsError;
pub use grpc_client::{GrpcClient, GrpcClientConfig};
pub use grpc_handlers::{KsGrpcService, create_grpc_service};
// Re-export proto types from actrix-proto
pub use actrix_proto::ks::v1::key_server_server::{KeyServer, KeyServerServer};
pub use handlers::{KSState, create_ks_state, create_router, get_stats, register_ks_metrics};
pub use storage::{KeyStorage, StorageConfig};
pub use types::{
    GenerateKeyRequest, GenerateKeyResponse, GetSecretKeyRequest, GetSecretKeyResponse, KeyPair,
    KeyRecord,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{SqliteConfig, StorageBackend, StorageConfig};
    use nonce_auth::storage::MemoryStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_ks_service_creation() {
        let temp_dir = tempdir().unwrap();

        let config = KsServiceConfig {
            storage: StorageConfig {
                backend: StorageBackend::Sqlite,
                key_ttl_seconds: 3600,
                sqlite: Some(SqliteConfig {}),
                postgres: None,
            },
            kek: None,
            kek_env: None,
            kek_file: None,
            tolerance_seconds: 3600,
        };

        // 使用内存存储进行测试（避免文件系统依赖）
        let nonce_storage = MemoryStorage::new();
        let state = create_ks_state(
            &config,
            nonce_storage,
            "test-actrix-shared-key",
            temp_dir.path(),
        )
        .await;
        assert!(state.is_ok());
    }
}
