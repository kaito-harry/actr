//! AId Token 签发器
//!
//! # 职责
//!
//! 负责处理 `RegisterRequest` 并生成 `RegisterResponse`，包括：
//! - 序列号分配（Snowflake 算法）
//! - Token 加密（ECIES）
//! - PSK 生成（客户端保管）
//! - 密钥生命周期管理（从 KS 获取、缓存、刷新）
//!
//! # 密钥管理策略
//!
//! ## 初始化阶段
//!
//! 1. 尝试从本地 SQLite 加载缓存的密钥
//! 2. 如果密钥不存在或过期超过容忍时间，从 KS 获取新密钥
//! 3. 启动后台刷新任务
//!
//! ## 运行时刷新
//!
//! - 检查频率：每 10 分钟
//! - 刷新触发：距离过期时间 < 10 分钟
//! - 容忍时间：过期后 24 小时内仍可使用
//!
//! ## 错误处理
//!
//! - 后台刷新失败：记录 warn 日志，下次继续重试
//! - 同步刷新失败：返回 `AidError::GenerationFailed`
//!
//! # 示例
//!
//! ```no_run
//! use ais::issuer::{AIdIssuer, IssuerConfig};
//! use ais::ks_client_wrapper::{KsClientWrapper, create_ks_client};
//! use actrix_common::config::ks::KsClientConfig;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let ks_config = KsClientConfig {
//!     endpoint: "http://localhost:50052".to_string(),
//!     psk: "test-key".to_string(),
//!     timeout_seconds: 30,
//!     enable_tls: false,
//!     tls_domain: None,
//!     ca_cert: None,
//!     client_cert: None,
//!     client_key: None,
//! };
//! let ks_client = create_ks_client(&ks_config, "shared-key").await?;
//! let config = IssuerConfig::default();
//!
//! let issuer = AIdIssuer::new(ks_client, config).await?;
//!
//! // 处理注册请求
//! // let response = issuer.issue_credential(&request).await?;
//! # Ok(())
//! # }
//! ```

use crate::ks_client_wrapper::KsClientWrapper;
use crate::sn::{AIdSerialNumberIssuer, SerialNumber};
use crate::storage::{KeyRecord, KeyStorage};

// ========== 常量配置 ==========

/// 密钥刷新检查间隔（秒）
///
/// 后台任务每隔此时间检查一次密钥是否需要刷新
const KEY_REFRESH_CHECK_INTERVAL_SECS: u64 = 600; // 10 分钟

/// 默认 PSK 长度（字节）
///
/// 生成的预共享密钥长度，用于 Actor 与 Signaling Server 的连接认证
const DEFAULT_PSK_LENGTH: usize = 32; // 256-bit
use actr_protocol::{
    AIdCredential, ActrId, ActrType, ErrorResponse, Realm, RegisterRequest, RegisterResponse,
    register_response,
};
use actrix_common::aid::{AidError, IdentityClaims};
use base64::prelude::*;
use ecies::{PublicKey, encrypt};
use prost::bytes::Bytes;
use prost_types::Timestamp;
use rand::RngCore;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// AId Token 签发器配置
#[derive(Debug, Clone)]
pub struct IssuerConfig {
    /// Token 有效期（秒）
    pub token_ttl_secs: u64,
    /// Signaling Server 心跳间隔（秒）
    pub signaling_heartbeat_interval_secs: u32,
    /// 密钥缓存刷新间隔（秒，默认 1 小时）
    pub key_refresh_interval_secs: u64,
    /// 密钥存储数据库路径
    pub key_storage_path: String,
    /// 是否启用定期密钥轮替
    pub enable_periodic_rotation: bool,
    /// 密钥轮替间隔（秒，默认 24 小时）
    ///
    /// 仅当 enable_periodic_rotation = true 时生效
    /// 到达此间隔后会主动生成新密钥，即使旧密钥未过期
    pub key_rotation_interval_secs: u64,
}

impl Default for IssuerConfig {
    fn default() -> Self {
        Self {
            token_ttl_secs: 3600,                  // 1 小时
            signaling_heartbeat_interval_secs: 30, // 30 秒
            key_refresh_interval_secs: 3600,       // 1 小时
            key_storage_path: "ais_keys.db".to_string(),
            enable_periodic_rotation: false,   // 默认禁用定期轮替
            key_rotation_interval_secs: 86400, // 24 小时
        }
    }
}

/// 密钥缓存
struct KeyCache {
    key_id: u32,
    public_key: PublicKey,
    #[allow(dead_code)]
    expires_at: u64,
}

/// AId Token 签发器 - 专注于签发新的 Actor Identity Token
pub struct AIdIssuer {
    ks_client: KsClientWrapper,
    key_storage: Arc<KeyStorage>,
    key_cache: Arc<RwLock<Option<KeyCache>>>,
    config: IssuerConfig,
}

impl AIdIssuer {
    /// 创建新的 AIdIssuer
    pub async fn new(ks_client: KsClientWrapper, config: IssuerConfig) -> Result<Self, AidError> {
        let key_storage = KeyStorage::new(&config.key_storage_path)
            .await
            .map_err(|e| {
                AidError::GenerationFailed(format!("Failed to create key storage: {e}"))
            })?;

        let issuer = Self {
            ks_client,
            key_storage: Arc::new(key_storage),
            key_cache: Arc::new(RwLock::new(None)),
            config,
        };

        // 初始化时加载或获取密钥
        issuer.ensure_key_loaded().await?;

        // 启动后台密钥刷新任务
        issuer.spawn_key_refresh_task();

        Ok(issuer)
    }

    /// 确保密钥已加载
    async fn ensure_key_loaded(&self) -> Result<(), AidError> {
        // 先尝试从缓存加载
        if self.key_cache.read().await.is_some() {
            debug!("Key already in cache");
            return Ok(());
        }

        // 尝试从存储加载
        if let Some(record) = self.key_storage.get_current_key().await.map_err(|e| {
            AidError::GenerationFailed(format!("Failed to get key from storage: {e}"))
        })? {
            // 检查是否过期超出容忍时间
            if self
                .key_storage
                .is_expired_beyond_tolerance()
                .await
                .map_err(|e| AidError::GenerationFailed(e.to_string()))?
            {
                warn!("Stored key expired beyond tolerance, fetching new key from KS");
                self.refresh_key_from_ks().await?;
            } else {
                debug!("Loaded key from storage: key_id={}", record.key_id);
                self.load_key_from_record(&record)?;
            }
        } else {
            // 没有存储的密钥，从 KS 获取
            info!("No stored key found, fetching from KS");
            self.refresh_key_from_ks().await?;
        }

        Ok(())
    }

    /// 从 KeyRecord 加载密钥到缓存
    fn load_key_from_record(&self, record: &KeyRecord) -> Result<(), AidError> {
        let public_key_bytes = BASE64_STANDARD
            .decode(&record.public_key)
            .map_err(|e| AidError::GenerationFailed(format!("Invalid base64 public key: {e}")))?;

        let public_key = PublicKey::parse_slice(&public_key_bytes, None)
            .map_err(|e| AidError::GenerationFailed(format!("Failed to parse public key: {e}")))?;

        let cache = KeyCache {
            key_id: record.key_id,
            public_key,
            expires_at: record.expires_at,
        };

        // 同步加载，阻塞等待
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                *self.key_cache.write().await = Some(cache);
            });
        });

        Ok(())
    }

    /// 从 KS 刷新密钥
    async fn refresh_key_from_ks(&self) -> Result<(), AidError> {
        info!("Fetching new key from KS");

        Self::refresh_key_internal(
            &self.ks_client,
            &self.key_storage,
            &self.key_cache,
            &self.config,
        )
        .await?;

        info!("Key refreshed successfully");
        Ok(())
    }

    /// 手动触发密钥轮替
    ///
    /// 立即从 KS 生成新密钥并更新缓存
    /// 返回新的 key_id
    pub async fn rotate_key(&self) -> Result<u32, AidError> {
        info!("Manual key rotation triggered");

        Self::refresh_key_internal(
            &self.ks_client,
            &self.key_storage,
            &self.key_cache,
            &self.config,
        )
        .await?;

        // 读取新的 key_id
        let cache = self.key_cache.read().await;
        let key_id = cache.as_ref().map(|c| c.key_id).ok_or_else(|| {
            AidError::GenerationFailed("No key available after rotation".to_string())
        })?;

        info!("Manual key rotation completed, new key_id: {}", key_id);
        Ok(key_id)
    }

    /// 获取当前使用的 key_id
    pub async fn get_current_key_id(&self) -> Result<u32, AidError> {
        let cache = self.key_cache.read().await;
        cache
            .as_ref()
            .map(|c| c.key_id)
            .ok_or_else(|| AidError::GenerationFailed("No key loaded".to_string()))
    }

    /// 启动后台密钥刷新任务
    fn spawn_key_refresh_task(&self) {
        let ks_client = self.ks_client.clone();
        let key_storage = self.key_storage.clone();
        let key_cache = self.key_cache.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(KEY_REFRESH_CHECK_INTERVAL_SECS));

            loop {
                interval.tick().await;

                let mut should_rotate = false;

                // 检查是否需要刷新（密钥即将过期）
                let should_refresh = match key_storage.should_refresh().await {
                    Ok(should) => should,
                    Err(e) => {
                        error!("Failed to check key refresh status: {}", e);
                        continue;
                    }
                };

                if should_refresh {
                    info!("Key expiring soon, rotation triggered");
                    should_rotate = true;
                }

                // 检查是否需要定期轮替
                if config.enable_periodic_rotation && !should_rotate {
                    match Self::should_periodic_rotate(&key_storage, &config).await {
                        Ok(true) => {
                            info!("Periodic rotation interval reached, rotation triggered");
                            should_rotate = true;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            error!("Failed to check periodic rotation status: {}", e);
                            continue;
                        }
                    }
                }

                if !should_rotate {
                    debug!("Key rotation not needed yet");
                    continue;
                }

                debug!("Background key rotation triggered");

                // 轮替密钥
                match Self::refresh_key_internal(&ks_client, &key_storage, &key_cache, &config)
                    .await
                {
                    Ok(()) => info!("Background key rotation successful"),
                    Err(e) => {
                        warn!("Background key rotation failed: {}, will retry later", e);
                    }
                }
            }
        });

        info!("Background key refresh task started");
    }

    /// 检查是否需要定期轮替密钥
    ///
    /// 根据 fetched_at 时间和配置的 key_rotation_interval_secs 判断
    async fn should_periodic_rotate(
        key_storage: &KeyStorage,
        config: &IssuerConfig,
    ) -> Result<bool, AidError> {
        let current_key = key_storage
            .get_current_key()
            .await
            .map_err(|e| AidError::GenerationFailed(format!("Failed to get current key: {e}")))?;

        let Some(key_record) = current_key else {
            // 没有密钥，需要生成
            return Ok(true);
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let time_since_fetched = now.saturating_sub(key_record.fetched_at);

        Ok(time_since_fetched >= config.key_rotation_interval_secs)
    }

    /// 内部密钥刷新方法（供后台任务使用）
    async fn refresh_key_internal(
        ks_client: &KsClientWrapper,
        key_storage: &KeyStorage,
        key_cache: &RwLock<Option<KeyCache>>,
        _config: &IssuerConfig,
    ) -> Result<(), AidError> {
        let (key_id, public_key, expires_at) = ks_client
            .generate_key()
            .await
            .map_err(|e| AidError::GenerationFailed(format!("KS unavailable: {e}")))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // 更新缓存
        let cache = KeyCache {
            key_id,
            public_key,
            expires_at,
        };

        *key_cache.write().await = Some(cache);

        // 保存到存储 - 需要 Base64 编码的公钥字符串
        let public_key_str = BASE64_STANDARD.encode(public_key.serialize_compressed());
        let record = KeyRecord {
            key_id,
            public_key: public_key_str,
            fetched_at: now,
            expires_at,
        };

        key_storage
            .update_current_key(&record)
            .await
            .map_err(|e| AidError::GenerationFailed(format!("Failed to save key: {e}")))?;

        Ok(())
    }

    /// 处理 register 请求并签发 credential
    pub async fn issue_credential(
        &self,
        request: &RegisterRequest,
    ) -> Result<RegisterResponse, AidError> {
        match self.issue_credential_inner(request).await {
            Ok(register_ok) => Ok(RegisterResponse {
                result: Some(register_response::Result::Success(register_ok)),
            }),
            Err(err) => Ok(RegisterResponse {
                result: Some(register_response::Result::Error(ErrorResponse {
                    code: 500,
                    message: err.to_string(),
                })),
            }),
        }
    }

    /// 内部处理逻辑
    async fn issue_credential_inner(
        &self,
        request: &RegisterRequest,
    ) -> Result<register_response::RegisterOk, AidError> {
        // 确保有可用的密钥
        self.ensure_key_loaded().await?;

        // 生成 ActrId
        let actr_id = self.generate_actr_id(&request.actr_type, &request.realm)?;

        // 生成过期时间
        let expr_time = self.calculate_expiry_time();

        // 创建 Claims
        let claims = IdentityClaims::from_actr_id(&actr_id, expr_time);

        // 从缓存获取密钥
        let (key_id, public_key) = {
            let cache = self.key_cache.read().await;
            let cache = cache
                .as_ref()
                .ok_or_else(|| AidError::GenerationFailed("No key available".to_string()))?;
            (cache.key_id, cache.public_key)
        };

        // 生成加密的 credential
        let encrypted_token = self.encrypt_claims(&claims, &public_key)?;

        // 生成 PSK (pre-shared key)
        let psk = self.generate_psk()?;

        // 创建 AIdCredential
        let credential = AIdCredential {
            encrypted_token: Bytes::from(encrypted_token),
            token_key_id: key_id,
        };

        // 创建过期时间的 Timestamp
        let credential_expires_at = Some(Timestamp {
            seconds: expr_time as i64,
            nanos: 0,
        });

        Ok(register_response::RegisterOk {
            actr_id,
            credential,
            psk: Some(Bytes::from(psk)),
            credential_expires_at,
            signaling_heartbeat_interval_secs: self.config.signaling_heartbeat_interval_secs,
        })
    }

    /// 生成 ActrId
    fn generate_actr_id(&self, actr_type: &ActrType, realm: &Realm) -> Result<ActrId, AidError> {
        // 使用 Snowflake 算法生成序列号
        let serial_number = SerialNumber::sn(realm.realm_id);

        Ok(ActrId {
            realm: *realm,
            serial_number: serial_number.value(),
            r#type: actr_type.clone(),
        })
    }

    /// 计算过期时间 (Unix timestamp, seconds)
    fn calculate_expiry_time(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + self.config.token_ttl_secs
    }

    /// 加密 Claims 为 credential
    fn encrypt_claims(
        &self,
        claims: &IdentityClaims,
        public_key: &PublicKey,
    ) -> Result<Vec<u8>, AidError> {
        // 序列化 claims
        let claims_bytes = serde_json::to_vec(claims)
            .map_err(|e| AidError::GenerationFailed(format!("Serialization error: {e}")))?;

        // 将 PublicKey 转换为字节
        let public_key_bytes = public_key.serialize();

        // 加密
        encrypt(&public_key_bytes, &claims_bytes)
            .map_err(|e| AidError::GenerationFailed(format!("Encryption error: {e}")))
    }

    /// 生成 PSK (pre-shared key)
    fn generate_psk(&self) -> Result<Vec<u8>, AidError> {
        let mut psk = vec![0u8; DEFAULT_PSK_LENGTH];
        rand::thread_rng().fill_bytes(&mut psk);
        Ok(psk)
    }

    // ========== 健康检查方法 ==========

    /// 检查数据库健康状态
    pub async fn check_database_health(&self) -> Result<(), AidError> {
        self.key_storage
            .health_check()
            .await
            .map_err(|e| AidError::GenerationFailed(format!("Database unhealthy: {e}")))
    }

    /// 检查 KS 服务健康状态
    ///
    /// 通过尝试获取当前密钥来验证 KS 服务可用性
    pub async fn check_ks_health(&self) -> Result<(), AidError> {
        // 尝试从缓存获取当前 key_id
        let key_id = {
            let cache = self.key_cache.read().await;
            cache.as_ref().map(|c| c.key_id)
        };

        // 如果有缓存的 key_id，尝试获取其私钥来验证 KS 连通性
        if let Some(key_id) = key_id {
            self.ks_client
                .fetch_secret_key(key_id)
                .await
                .map(|_| ())
                .map_err(|e| AidError::GenerationFailed(format!("KS service unhealthy: {e}")))
        } else {
            // 没有缓存密钥，尝试生成新密钥
            self.ks_client
                .generate_key()
                .await
                .map(|_| ())
                .map_err(|e| AidError::GenerationFailed(format!("KS service unhealthy: {e}")))
        }
    }

    /// 检查密钥缓存健康状态
    pub async fn check_key_cache_health(&self) -> Result<KeyCacheInfo, AidError> {
        let cache = self.key_cache.read().await;
        let cache = cache
            .as_ref()
            .ok_or_else(|| AidError::GenerationFailed("No key in cache".to_string()))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let expires_in = cache.expires_at.saturating_sub(now);

        Ok(KeyCacheInfo {
            key_id: cache.key_id,
            expires_in,
        })
    }
}

/// 密钥缓存健康信息
pub struct KeyCacheInfo {
    pub key_id: u32,
    pub expires_in: u64,
}

#[cfg(test)]
mod tests {

    // Note: 完整测试需要 KS 服务运行，这里只做基本单元测试
    // 集成测试在 lib.rs 中
}
