//! AId token issuer.
//!
//! # Responsibilities
//!
//! Handles `RegisterRequest` and builds `RegisterResponse`, including:
//! - Serial number allocation with Snowflake.
//! - Signed access credential issuance.
//! - Time-limited TURN credential generation.
//! - Renewal token issuance and hash persistence.
//! - Signing key lifecycle management through Signer.
//!
//! # 密钥管理策略
//!
//! ## 安全设计
//!
//! - Ed25519 私钥永不离开 KS 服务
//! - AIS 只保存 verifying key（公钥）用于凭证携带
//! - 所有签名操作通过 KS 的 Sign RPC 完成
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
//! use ais::signer_client_wrapper::{SignerClientWrapper, create_signer_client};
//! use platform::config::signer::SignerClientConfig;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let signer_config = SignerClientConfig {
//!     endpoint: "http://localhost:8080".to_string(),
//!     timeout_seconds: 30,
//!     enable_tls: false,
//!     tls_domain: None,
//!     ca_cert: None,
//!     client_cert: None,
//!     client_key: None,
//! };
//! let signer_client = create_signer_client(&signer_config, "shared-key").await?;
//! let config = IssuerConfig::default();
//!
//! let issuer = AIdIssuer::new(signer_client, config, tokio_util::sync::CancellationToken::new()).await?;
//!
//! // 处理注册请求
//! // let response = issuer.issue_credential(&request).await?;
//! # Ok(())
//! # }
//! ```

use crate::signer_client_wrapper::SignerClientWrapper;
use crate::sn::{AIdSerialNumberIssuer, SerialNumber};
use crate::storage::{KeyRecord, KeyStorage};

// ========== 常量配置 ==========

/// 密钥刷新检查间隔（秒）
///
/// 后台任务每隔此时间检查一次密钥是否需要刷新
const KEY_REFRESH_CHECK_INTERVAL_SECS: u64 = 600; // 10 分钟
const MANUFACTURER_SIGNED_AT_MAX_AGE_SECS: u64 = 5 * 60;
const MANUFACTURER_SIGNED_AT_FUTURE_SKEW_SECS: u64 = 60;
const MANUFACTURER_NONCE_TTL_SECS: u64 = 10 * 60;

use actr_protocol::{
    AIdCredential, ActrId, ActrType, ErrorResponse, IdentityClaims, Realm, RegisterRequest,
    RegisterResponse, register_response,
};
use base64::prelude::*;
use ed25519_dalek::VerifyingKey;
use hmac::{Hmac, Mac};
use platform::aid::AidError;
use platform::realm::RealmSecretCheck;
use prost::Message as ProstMessage;
use prost::bytes::Bytes;
use prost_types::Timestamp;
use sha1::Sha1;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

type HmacSha1 = Hmac<Sha1>;

fn aid_error_to_code(err: &AidError) -> u32 {
    match err {
        AidError::InvalidFormat
        | AidError::InvalidPrefix
        | AidError::EmptyId
        | AidError::InvalidTimestamp(_)
        | AidError::Base64DecodeError(_)
        | AidError::HexDecodeError(_) => 400,
        AidError::Expired => 401,
        AidError::RealmError(_) | AidError::ManufacturerNotVerified | AidError::PackageRevoked => {
            403
        }
        AidError::GenerationFailed(msg) => {
            if msg.contains("KS unavailable") || msg.contains("KS service") {
                503
            } else {
                500
            }
        }
        AidError::InvalidSignature(_) | AidError::DecodeFailure(_) => 500,
    }
}

/// AId Token 签发器配置
#[derive(Debug, Clone)]
pub struct IssuerConfig {
    /// Token 有效期（秒）
    pub token_ttl_secs: u64,
    /// Signaling Server 心跳间隔（秒）
    pub signaling_heartbeat_interval_secs: u32,
    /// 密钥缓存刷新间隔（秒，默认 1 小时）
    pub key_refresh_interval_secs: u64,
    /// 密钥存储数据库文件路径
    pub key_storage_file: std::path::PathBuf,
    /// 是否启用定期密钥轮替
    pub enable_periodic_rotation: bool,
    /// 密钥轮替间隔（秒，默认 24 小时）
    ///
    /// 仅当 enable_periodic_rotation = true 时生效
    /// 到达此间隔后会主动生成新密钥，即使旧密钥未过期
    pub key_rotation_interval_secs: u64,
    /// TURN 共享密钥（与 TURN 服务器共享，用于生成时效凭证）
    pub turn_secret: String,
    /// SQLite 数据目录（与 signaling 共享，用于写入 verifying key 供其验证凭证）
    ///
    /// AIS 在加载或刷新 signing key 后，会把 verifying key 写入此目录下的
    /// `signaling_key_cache.db`，让 signaling 的 `AIdCredentialValidator` 能够验证凭证。
    pub sqlite_path: std::path::PathBuf,
    /// Renewal token TTL in seconds.
    pub renewal_token_ttl_secs: u64,
    /// Renewal rotation window in seconds.
    pub renewal_rotation_window_secs: u64,
    /// Base64-decoded renewal token secret (at least 32 bytes).
    pub renewal_token_secret: Vec<u8>,
}

impl Default for IssuerConfig {
    fn default() -> Self {
        Self {
            token_ttl_secs: 3600,                  // 1 小时
            signaling_heartbeat_interval_secs: 30, // 30 秒
            key_refresh_interval_secs: 3600,       // 1 小时
            key_storage_file: std::path::PathBuf::from("ais_keys.db"),
            enable_periodic_rotation: false,   // 默认禁用定期轮替
            key_rotation_interval_secs: 86400, // 24 小时
            turn_secret: "actrix-turn-secret-change-in-production".to_string(),
            sqlite_path: std::path::PathBuf::from("."),
            renewal_token_ttl_secs: 86400,
            renewal_rotation_window_secs: 21600,
            renewal_token_secret: Vec::new(),
        }
    }
}

/// 密钥缓存（只保存 verifying key，私钥由 KS 保管）
struct KeyCache {
    key_id: u32,
    verifying_key: VerifyingKey,
    #[allow(dead_code)]
    expires_at: u64,
    #[allow(dead_code)]
    tolerance_seconds: u64,
}

/// AId Token 签发器 - 专注于签发新的 Actor Identity Token
pub struct AIdIssuer {
    signer_client: SignerClientWrapper,
    key_storage: Arc<KeyStorage>,
    key_cache: Arc<RwLock<Option<KeyCache>>>,
    /// Issuer configuration (public for renewal handler access).
    pub config: IssuerConfig,
}

impl AIdIssuer {
    /// 创建新的 AIdIssuer
    pub async fn new(
        signer_client: SignerClientWrapper,
        config: IssuerConfig,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Self, AidError> {
        let key_storage = KeyStorage::new(&config.key_storage_file)
            .await
            .map_err(|e| {
                AidError::GenerationFailed(format!("Failed to create key storage: {e}"))
            })?;

        let issuer = Self {
            signer_client,
            key_storage: Arc::new(key_storage),
            key_cache: Arc::new(RwLock::new(None)),
            config,
        };

        // 初始化时尝试加载/获取密钥。主端口复用场景下，
        // KS 可能在同进程稍后就绪，因此这里失败不阻塞服务启动。
        if let Err(e) = issuer.ensure_key_loaded().await {
            platform::recording::warn!("Initial KS key load deferred, will retry on demand: {}", e);
        }

        // 启动后台密钥刷新任务
        issuer.spawn_key_refresh_task(cancel);

        Ok(issuer)
    }

    /// 确保密钥已加载
    async fn ensure_key_loaded(&self) -> Result<(), AidError> {
        // 先尝试从缓存加载
        if self.key_cache.read().await.is_some() {
            platform::recording::debug!("Key already in cache");
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
                platform::recording::warn!(
                    "Stored key expired beyond tolerance, fetching new key from KS"
                );
                self.refresh_key_from_ks().await?;
            } else {
                platform::recording::debug!("Loaded key from storage: key_id={}", record.key_id);
                self.load_key_from_record(&record)?;
            }
        } else {
            // 没有存储的密钥，从 KS 获取
            platform::recording::info!("No stored key found, fetching from KS");
            self.refresh_key_from_ks().await?;
        }

        Ok(())
    }

    /// 从 KeyRecord 加载 Ed25519 verifying key 到缓存
    ///
    /// AIS DB 中的 `public_key` 存储 base64 编码的 Ed25519 verifying key（32 字节）
    fn load_key_from_record(&self, record: &KeyRecord) -> Result<(), AidError> {
        let key_bytes = BASE64_STANDARD
            .decode(&record.public_key)
            .map_err(|e| AidError::GenerationFailed(format!("Invalid base64 key: {e}")))?;

        let key_array: [u8; 32] = key_bytes.try_into().map_err(|_| {
            AidError::GenerationFailed("Verifying key must be exactly 32 bytes".to_string())
        })?;

        let verifying_key = VerifyingKey::from_bytes(&key_array).map_err(|e| {
            AidError::GenerationFailed(format!("Invalid Ed25519 verifying key: {e}"))
        })?;

        let cache = KeyCache {
            key_id: record.key_id,
            verifying_key,
            expires_at: record.expires_at,
            tolerance_seconds: record.tolerance_seconds,
        };

        let key_id = record.key_id;
        let expires_at = record.expires_at;

        // 同步加载，阻塞等待
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                *self.key_cache.write().await = Some(cache);
                // 将 verifying key 持久化写入 signaling_key_cache.db，
                // 保证即使 AIdCredentialValidator::init 尚未调用也能落盘，
                // 后续 init 会从 DB 读取到该密钥。
                if let Err(e) =
                    platform::aid::credential::validator::AIdCredentialValidator::persist_key(
                        &self.config.sqlite_path,
                        key_id,
                        &verifying_key,
                        expires_at,
                    )
                    .await
                {
                    platform::recording::warn!(
                        "写入 verifying key 到 key_cache DB 失败（非致命，key_id={}）: {}",
                        key_id,
                        e
                    );
                }
            });
        });

        Ok(())
    }

    /// 从 KS 刷新密钥
    async fn refresh_key_from_ks(&self) -> Result<(), AidError> {
        platform::recording::info!("Fetching new key from KS");

        Self::refresh_key_internal(
            &self.signer_client,
            &self.key_storage,
            &self.key_cache,
            &self.config,
        )
        .await?;

        platform::recording::info!("Key refreshed successfully");
        Ok(())
    }

    /// 手动触发密钥轮替
    ///
    /// 立即从 KS 生成新密钥并更新缓存
    /// 返回新的 key_id
    pub async fn rotate_key(&self) -> Result<u32, AidError> {
        platform::recording::info!("Manual key rotation triggered");

        Self::refresh_key_internal(
            &self.signer_client,
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

        platform::recording::info!("Manual key rotation completed, new key_id: {}", key_id);
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

    /// 返回当前签名公钥（key_id + raw 32-byte verifying key）
    ///
    /// 供 `/ais/signing-pubkey` HTTP 端点使用，
    /// 也可被 signaling 服务在 key_cache miss 时按需拉取。
    pub async fn get_current_signing_pubkey(&self) -> Result<(u32, Vec<u8>), AidError> {
        let cache = self.key_cache.read().await;
        cache
            .as_ref()
            .map(|c| (c.key_id, c.verifying_key.as_bytes().to_vec()))
            .ok_or_else(|| AidError::GenerationFailed("No signing key loaded".to_string()))
    }

    /// 启动后台密钥刷新任务
    fn spawn_key_refresh_task(&self, cancel: tokio_util::sync::CancellationToken) {
        let signer_client = self.signer_client.clone();
        let key_storage = self.key_storage.clone();
        let key_cache = self.key_cache.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(KEY_REFRESH_CHECK_INTERVAL_SECS));

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {}
                }

                let mut should_rotate = false;

                // 检查是否需要刷新（密钥即将过期）
                let should_refresh = match key_storage.should_refresh().await {
                    Ok(should) => should,
                    Err(e) => {
                        platform::recording::error!("Failed to check key refresh status: {}", e);
                        continue;
                    }
                };

                if should_refresh {
                    platform::recording::info!("Key expiring soon, rotation triggered");
                    should_rotate = true;
                }

                // 检查是否需要定期轮替
                if config.enable_periodic_rotation && !should_rotate {
                    match Self::should_periodic_rotate(&key_storage, &config).await {
                        Ok(true) => {
                            platform::recording::info!(
                                "Periodic rotation interval reached, rotation triggered"
                            );
                            should_rotate = true;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            platform::recording::error!(
                                "Failed to check periodic rotation status: {}",
                                e
                            );
                            continue;
                        }
                    }
                }

                if !should_rotate {
                    platform::recording::debug!("Key rotation not needed yet");
                    continue;
                }

                platform::recording::debug!("Background key rotation triggered");

                // 轮替密钥
                match Self::refresh_key_internal(&signer_client, &key_storage, &key_cache, &config)
                    .await
                {
                    Ok(()) => platform::recording::info!("Background key rotation successful"),
                    Err(e) => {
                        platform::recording::warn!(
                            "Background key rotation failed: {}, will retry later",
                            e
                        );
                    }
                }
            }
            platform::recording::debug!("AIS key refresh task cancelled");
        });

        platform::recording::info!("Background key refresh task started");
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
    ///
    /// 从 KS 生成新 Ed25519 签名密钥对，只保存 verifying key；
    /// 私钥由 KS 保管，AIS 仅通过 Sign RPC 进行签名。
    async fn refresh_key_internal(
        signer_client: &SignerClientWrapper,
        key_storage: &KeyStorage,
        key_cache: &RwLock<Option<KeyCache>>,
        config: &IssuerConfig,
    ) -> Result<(), AidError> {
        // 从 KS 申请新的 Ed25519 签名密钥（私钥由 KS 保管）
        let (key_id, verifying_key_bytes, expires_at, tolerance_seconds) = signer_client
            .generate_signing_key()
            .await
            .map_err(|e| AidError::GenerationFailed(format!("KS unavailable: {e}")))?;

        let verifying_key = VerifyingKey::from_bytes(&verifying_key_bytes).map_err(|e| {
            AidError::GenerationFailed(format!("Invalid verifying key from KS: {e}"))
        })?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // 更新缓存（只存 verifying key）
        let cache = KeyCache {
            key_id,
            verifying_key,
            expires_at,
            tolerance_seconds,
        };

        *key_cache.write().await = Some(cache);

        // 保存到存储（以 base64 编码的 verifying key 存储）
        let key_str = BASE64_STANDARD.encode(verifying_key.as_bytes());
        let record = KeyRecord {
            key_id,
            public_key: key_str,
            fetched_at: now,
            expires_at,
            tolerance_seconds,
        };

        key_storage
            .update_current_key(&record)
            .await
            .map_err(|e| AidError::GenerationFailed(format!("Failed to save key: {e}")))?;

        // 将 verifying key 持久化写入 signaling_key_cache.db（同时更新内存缓存）。
        if let Err(e) = platform::aid::credential::validator::AIdCredentialValidator::persist_key(
            &config.sqlite_path,
            key_id,
            &verifying_key,
            expires_at,
        )
        .await
        {
            platform::recording::warn!(
                "写入 verifying key 到 key_cache DB 失败（非致命，key_id={}）: {}",
                key_id,
                e
            );
        } else {
            platform::recording::info!("verifying key 已持久化到 key_cache DB (key_id={})", key_id);
        }

        Ok(())
    }

    /// 处理 register 请求并签发 credential
    pub async fn issue_credential(
        &self,
        request: &RegisterRequest,
    ) -> Result<RegisterResponse, AidError> {
        self.issue_credential_with_realm_secret_check(request, None)
            .await
    }

    /// 处理 register 请求并签发 credential，携带 handler 已完成的 realm secret 校验结果。
    pub async fn issue_credential_with_realm_secret_check(
        &self,
        request: &RegisterRequest,
        realm_secret_check: Option<RealmSecretCheck>,
    ) -> Result<RegisterResponse, AidError> {
        match self
            .issue_credential_inner(request, realm_secret_check.as_ref())
            .await
        {
            Ok(register_ok) => Ok(RegisterResponse {
                result: Some(register_response::Result::Success(register_ok)),
            }),
            Err(err) => Ok(RegisterResponse {
                result: Some(register_response::Result::Error(ErrorResponse {
                    code: aid_error_to_code(&err),
                    message: err.to_string(),
                })),
            }),
        }
    }

    /// 内部处理逻辑（Ed25519 签名，通过 KS Sign RPC）
    async fn issue_credential_inner(
        &self,
        request: &RegisterRequest,
        realm_secret_check: Option<&RealmSecretCheck>,
    ) -> Result<register_response::RegisterOk, AidError> {
        // 确保有可用的密钥
        self.ensure_key_loaded().await?;

        self.verify_registration_identity(request, realm_secret_check)
            .await?;

        // 生成 ActrId
        let actr_id = self.generate_actr_id(&request.actr_type, &request.realm)?;

        // 生成过期时间
        let expr_time = self.calculate_expiry_time();

        // 构建 IdentityClaims（proto 类型，明文）
        let claims_proto = IdentityClaims {
            realm_id: actr_id.realm.realm_id,
            actor_id: actr_id.to_string_repr(),
            expires_at: expr_time,
        };

        // Proto 编码 claims bytes
        let claims_bytes = claims_proto.encode_to_vec();

        // 从缓存读取 key_id 和 verifying_key（释放读锁后进行异步签名）
        let (key_id, verifying_key) = {
            let cache = self.key_cache.read().await;
            let cache = cache
                .as_ref()
                .ok_or_else(|| AidError::GenerationFailed("No key available".to_string()))?;
            (cache.key_id, cache.verifying_key)
        };

        // 通过 KS Sign RPC 签名（私钥不离开 KS）
        let signature_bytes = self
            .signer_client
            .sign(key_id, &claims_bytes)
            .await
            .map_err(|e| AidError::GenerationFailed(format!("KS sign failed: {e}")))?;

        // 创建 AIdCredential（Ed25519 格式）
        let credential = AIdCredential {
            key_id,
            claims: Bytes::from(claims_bytes),
            signature: Bytes::from(signature_bytes),
        };

        // 创建过期时间的 Timestamp
        let credential_expires_at = Some(Timestamp {
            seconds: expr_time as i64,
            nanos: 0,
        });

        // Generate a time-limited TURN credential compatible with coturn --use-auth-secret.
        let turn_credential = self.generate_turn_credential(&actr_id.to_string_repr(), expr_time);

        if self.config.renewal_token_secret.is_empty() {
            return Err(AidError::GenerationFailed(
                "renewal_token_secret is required".to_string(),
            ));
        }

        let token = self.generate_renewal_token();
        let renewal_expires = self.calculate_renewal_expiry_time();

        // Persist token hash; failure here fails the whole registration.
        let token_hash = crate::renewal::hash_token(&token);
        crate::renewal::insert_renewal_token(&actr_id, &token_hash, renewal_expires)
            .await
            .map_err(|e| {
                AidError::GenerationFailed(format!("Failed to persist renewal token: {e}"))
            })?;

        // Run global GC for expired tokens.
        let _ = crate::renewal::gc_expired_tokens().await;

        let renewal_token = Some(Bytes::from(token));
        let renewal_token_expires_at = Some(Timestamp {
            seconds: renewal_expires,
            nanos: 0,
        });

        Ok(register_response::RegisterOk {
            actr_id,
            credential,
            turn_credential,
            credential_expires_at,
            signaling_heartbeat_interval_secs: self.config.signaling_heartbeat_interval_secs,
            signing_pubkey: Bytes::from(verifying_key.as_bytes().to_vec()),
            signing_key_id: key_id,
            renewal_token,
            renewal_token_expires_at,
        })
    }

    /// Verify the registration identity according to the explicit request source.
    async fn verify_registration_identity(
        &self,
        request: &RegisterRequest,
        realm_secret_check: Option<&RealmSecretCheck>,
    ) -> Result<(), AidError> {
        let auth_mode = match request.auth_mode {
            Some(value) if value == actr_protocol::RegisterAuthMode::Linked as i32 => {
                actr_protocol::RegisterAuthMode::Linked
            }
            Some(value) if value == actr_protocol::RegisterAuthMode::Package as i32 => {
                actr_protocol::RegisterAuthMode::Package
            }
            _ => actr_protocol::RegisterAuthMode::Unspecified,
        };

        match auth_mode {
            actr_protocol::RegisterAuthMode::Unspecified
            | actr_protocol::RegisterAuthMode::Package => {
                // Verify MFR identity through a published package, or through
                // signatures for an unpublished package.
                self.verify_mfr_identity(request).await
            }
            actr_protocol::RegisterAuthMode::Linked => {
                self.verify_linked_identity(request, realm_secret_check)
            }
        }
    }

    fn verify_linked_identity(
        &self,
        request: &RegisterRequest,
        realm_secret_check: Option<&RealmSecretCheck>,
    ) -> Result<(), AidError> {
        if request.actr_type.version.is_empty()
            || request.manifest_raw.is_some()
            || request.mfr_signature.is_some()
            || request.manufacturer_auth_signature.is_some()
            || request.manufacturer_auth_signed_at.is_some()
            || request.manufacturer_auth_nonce.is_some()
        {
            return Err(AidError::InvalidFormat);
        }

        match realm_secret_check {
            Some(RealmSecretCheck::ValidCurrent) | Some(RealmSecretCheck::ValidPrevious) => Ok(()),
            other => {
                platform::recording::warn!(
                    "Linked registration rejected: realm secret was not verified, realm={}, type={}, secret_check={:?}",
                    request.realm.realm_id,
                    request.actr_type.to_string_repr(),
                    other
                );
                Err(AidError::ManufacturerNotVerified)
            }
        }
    }

    /// verify MFR identity
    ///
    /// Path 1: an existing package tuple is authoritative. Active packages must
    /// still have an active MFR and a non-revoked signing key; revoked packages
    /// are terminal and cannot fall through.
    /// Path 2: only a never-published tuple may use manifest and request signatures.
    /// Otherwise reject
    async fn verify_mfr_identity(&self, request: &RegisterRequest) -> Result<(), AidError> {
        let actr_type = &request.actr_type;
        if actr_type.version.is_empty() {
            return Err(AidError::InvalidFormat);
        }

        let type_str = actr_type.to_string_repr();
        let mfr_name = &actr_type.manufacturer;

        // SECURITY / COMPATIBILITY: package registration must be bound to the
        // exact manifest bytes the manufacturer saw locally. Without
        // manifest_raw, AIS can only prove that "some active package has this
        // type_str"; without target, a multi-target package lookup degrades to
        // type-only matching. Treat both as malformed package-auth requests
        // instead of falling back to a broad registry lookup.
        // This intentionally tightens the legacy Path 1 request shape.
        let pool = platform::storage::db::get_database().get_pool().clone();
        let target_ref = request
            .target
            .as_deref()
            .filter(|target| !target.is_empty())
            .ok_or_else(|| {
                platform::recording::warn!(
                    "MFR verification failed: package registration requires target; type_str={}",
                    type_str
                );
                AidError::InvalidFormat
            })?;
        let request_manifest = request
            .manifest_raw
            .as_ref()
            .filter(|manifest| !manifest.is_empty())
            .ok_or_else(|| {
                platform::recording::warn!(
                    "MFR verification failed: package registration requires manifest_raw; type_str={}, target={}",
                    type_str,
                    target_ref
                );
                AidError::InvalidFormat
            })?;

        let published_pkg = actrix_mfr::model::ActrPackage::get_by_type_and_target_any_status(
            &pool, &type_str, target_ref,
        )
        .await
        .map_err(|e| AidError::GenerationFailed(format!("MFR lookup failed: {e}")))?;

        if let Some(pkg) = published_pkg {
            use sha2::{Digest, Sha256};

            if pkg.status == actrix_mfr::model::PkgStatus::Revoked {
                platform::recording::warn!(
                    "MFR table lookup rejected: package revoked, type_str={}, target={}",
                    type_str,
                    target_ref
                );
                return Err(AidError::PackageRevoked);
            }

            if pkg.manufacturer != actr_type.manufacturer
                || pkg.name != actr_type.name
                || pkg.version != actr_type.version
                || pkg.target != target_ref
            {
                platform::recording::warn!(
                    "MFR table lookup rejected: package identity mismatch, type_str={}, target={}",
                    type_str,
                    target_ref
                );
                return Err(AidError::ManufacturerNotVerified);
            }

            let request_manifest_hash = Sha256::digest(request_manifest.as_ref());
            let stored_manifest_hash = Sha256::digest(pkg.manifest.as_bytes());
            if request_manifest_hash[..] != stored_manifest_hash[..] {
                platform::recording::warn!(
                    "MFR table lookup rejected: manifest hash mismatch, type_str={}, target={}",
                    type_str,
                    target_ref
                );
                // Do not fall through to Path 2 when the published tuple exists.
                // Rejecting here prevents a mismatched or stale manifest from
                // downgrading into the unpublished-package authentication path.
                return Err(AidError::ManufacturerNotVerified);
            }

            let manager = actrix_mfr::MfrManager::new(pool.clone());
            let signing_key_id = manager
                .verify_published_package_signing_key(&pkg)
                .await
                .map_err(Self::map_mfr_verification_error)?;

            platform::recording::debug!(
                "MFR table lookup passed, type_str={}, target={}, signing_key_id={}",
                type_str,
                target_ref,
                signing_key_id
            );
            return Ok(());
        }

        // Path 2: not in table -> verify package signature + manufacturer authorization
        // (own package, not yet published).
        let (
            manifest_bytes,
            mfr_signature,
            manufacturer_auth_signature,
            manufacturer_auth_signed_at,
            manufacturer_auth_nonce,
        ) = match (
            &request.manifest_raw,
            &request.mfr_signature,
            &request.manufacturer_auth_signature,
            request.manufacturer_auth_signed_at,
            &request.manufacturer_auth_nonce,
        ) {
            (
                Some(manifest_bytes),
                Some(mfr_signature),
                Some(manufacturer_auth_signature),
                Some(manufacturer_auth_signed_at),
                Some(manufacturer_auth_nonce),
            ) => (
                manifest_bytes,
                mfr_signature,
                manufacturer_auth_signature,
                manufacturer_auth_signed_at,
                manufacturer_auth_nonce,
            ),
            _ => {
                platform::recording::warn!(
                    "MFR verification failed: unpublished package requires manifest_raw, mfr_signature, manufacturer_auth_signature, manufacturer_auth_signed_at, and manufacturer_auth_nonce; type_str={}",
                    type_str
                );
                return Err(AidError::ManufacturerNotVerified);
            }
        };

        if !matches!(manufacturer_auth_nonce.len(), 16 | 32) {
            platform::recording::warn!(
                "manufacturer_auth_nonce has invalid length: len={}, type_str={}",
                manufacturer_auth_nonce.len(),
                type_str
            );
            return Err(AidError::InvalidFormat);
        }

        let now = Self::unix_now_secs()?;
        Self::verify_manufacturer_auth_signed_at(manufacturer_auth_signed_at, now)?;

        // Parse manifest using standard TOML. Reject if not valid UTF-8/TOML —
        // unparseable manifests must not bypass the identity binding check below.
        let manifest_str = std::str::from_utf8(manifest_bytes.as_ref()).map_err(|_| {
            platform::recording::warn!("MFR manifest_raw is not valid UTF-8");
            AidError::InvalidFormat
        })?;
        let manifest_toml: toml::Value = manifest_str.parse().map_err(|e| {
            platform::recording::warn!("MFR manifest_raw is not valid TOML: {}", e);
            AidError::InvalidFormat
        })?;

        // Extract signing_key_id — mandatory for all modern packages.
        // Without it, we cannot reliably resolve the correct MFR key after rotation.
        let signing_key_id = manifest_toml
            .get("signing_key_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                platform::recording::warn!(
                    "MFR manifest_raw missing 'signing_key_id', manufacturer={}",
                    mfr_name
                );
                AidError::InvalidFormat
            })?
            .to_string();

        // Look up the manufacturer's public key by key_id (supports current + historical)
        let manager = actrix_mfr::MfrManager::new(pool.clone());
        let mfr_info = manager
            .resolve_key_by_id(mfr_name, &signing_key_id)
            .await
            .map_err(Self::map_mfr_verification_error)?;

        // Verify package manifest signature using the manufacturer's public key.
        let sig_b64 = base64::prelude::BASE64_STANDARD.encode(mfr_signature.as_ref());
        let valid = actrix_mfr::crypto::verify_signature(
            manifest_bytes.as_ref(),
            &sig_b64,
            &mfr_info.public_key,
        )
        .map_err(|e| {
            platform::recording::warn!(
                "MFR signature verification error: manufacturer={}, err={}",
                mfr_name,
                e
            );
            AidError::GenerationFailed(format!("Signature verification error: {e}"))
        })?;

        if !valid {
            platform::recording::warn!(
                "MFR signature verification failed: invalid signature, type_str={}",
                type_str
            );
            return Err(AidError::ManufacturerNotVerified);
        }

        // SECURITY: Verify that manifest identity matches the RegisterRequest.
        // Without this check, an attacker holding any valid signed manifest from
        // the same MFR could reuse it to register a different actr_type/target.
        let m_manufacturer = manifest_toml
            .get("manufacturer")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let m_name = manifest_toml
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let m_version = manifest_toml
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let m_target = manifest_toml
            .get("binary")
            .and_then(|b| b.get("target"))
            .and_then(|v| v.as_str());

        // Check manufacturer/name/version match actr_type.
        if m_manufacturer != actr_type.manufacturer
            || m_name != actr_type.name
            || m_version != actr_type.version
        {
            platform::recording::warn!(
                "MFR manifest identity mismatch: manifest={}:{}:{}, request={}",
                m_manufacturer,
                m_name,
                m_version,
                type_str
            );
            return Err(AidError::ManufacturerNotVerified);
        }

        // The MFR-signed manifest must declare binary.target. Falling back to
        // request.target would leave the manifest signature unbound to target,
        // letting one signed manifest be registered under arbitrary targets.
        // Mirror Path 1, where the registry row's target is authoritative.
        let manifest_target = m_target.ok_or_else(|| {
            platform::recording::warn!("MFR manifest missing binary.target; type_str={}", type_str);
            AidError::InvalidFormat
        })?;

        // request.target is guaranteed non-empty by the entry guard; ensure it
        // matches the target the MFR signed into the manifest.
        if let Some(req_target) = request.target.as_deref() {
            if req_target != manifest_target {
                platform::recording::warn!(
                    "MFR manifest target mismatch: manifest={}, request={}",
                    manifest_target,
                    req_target
                );
                return Err(AidError::ManufacturerNotVerified);
            }
        }
        let manifest_sha256_hex = {
            use sha2::{Digest, Sha256};
            hex::encode(Sha256::digest(manifest_bytes.as_ref()))
        };
        let manufacturer_payload = actr_protocol::build_manufacturer_register_payload(
            actr_protocol::ManufacturerRegisterPayload {
                realm_id: request.realm.realm_id,
                actr_type,
                target: manifest_target,
                manifest_sha256_hex: &manifest_sha256_hex,
                manufacturer_auth_signed_at,
                manufacturer_auth_nonce: manufacturer_auth_nonce.as_ref(),
            },
        );
        let manufacturer_sig_b64 =
            base64::prelude::BASE64_STANDARD.encode(manufacturer_auth_signature.as_ref());
        let manufacturer_valid = actrix_mfr::crypto::verify_signature(
            manufacturer_payload.as_bytes(),
            &manufacturer_sig_b64,
            &mfr_info.public_key,
        )
        .map_err(|e| {
            platform::recording::warn!(
                "manufacturer_auth_signature verification error: manufacturer={}, key_id={}, err={}",
                mfr_name,
                signing_key_id,
                e
            );
            AidError::GenerationFailed(format!("Manufacturer signature verification error: {e}"))
        })?;

        if !manufacturer_valid {
            platform::recording::warn!(
                "manufacturer_auth_signature verification failed: manufacturer={}, key_id={}, type_str={}",
                mfr_name,
                signing_key_id,
                type_str
            );
            return Err(AidError::ManufacturerNotVerified);
        }

        Self::consume_manufacturer_auth_nonce(
            &pool,
            mfr_name,
            &signing_key_id,
            manufacturer_auth_nonce.as_ref(),
            now,
        )
        .await?;

        platform::recording::debug!(
            "MFR manifest and manufacturer signature verification passed, type_str={}",
            type_str
        );
        Ok(())
    }

    fn map_mfr_verification_error(error: actrix_mfr::MfrError) -> AidError {
        let message = error.to_string();
        match error {
            actrix_mfr::MfrError::NotFound
            | actrix_mfr::MfrError::KeyRevoked(_)
            | actrix_mfr::MfrError::InvalidStatus(_)
            | actrix_mfr::MfrError::CertificateExpired
            | actrix_mfr::MfrError::InvalidSignature
            | actrix_mfr::MfrError::Unauthorized
            | actrix_mfr::MfrError::InvalidRequest(_) => {
                platform::recording::warn!("MFR verification rejected: {}", message);
                AidError::ManufacturerNotVerified
            }
            other => {
                platform::recording::warn!("MFR verification failed internally: {}", other);
                AidError::GenerationFailed(format!("MFR verification failed: {other}"))
            }
        }
    }

    fn unix_now_secs() -> Result<u64, AidError> {
        crate::renewal::try_now_secs()
            .map_err(|e| AidError::InvalidTimestamp(format!("system clock before Unix epoch: {e}")))
    }

    fn verify_manufacturer_auth_signed_at(signed_at: u64, now: u64) -> Result<(), AidError> {
        if signed_at > now.saturating_add(MANUFACTURER_SIGNED_AT_FUTURE_SKEW_SECS) {
            platform::recording::warn!(
                "manufacturer_auth_signed_at is too far in the future: signed_at={}, now={}, future_skew_secs={}",
                signed_at,
                now,
                MANUFACTURER_SIGNED_AT_FUTURE_SKEW_SECS
            );
            return Err(AidError::InvalidTimestamp(
                "manufacturer_auth_signed_at is too far in the future".to_string(),
            ));
        }

        if now.saturating_sub(signed_at) > MANUFACTURER_SIGNED_AT_MAX_AGE_SECS {
            platform::recording::warn!(
                "manufacturer_auth_signed_at outside max age: signed_at={}, now={}, max_age_secs={}",
                signed_at,
                now,
                MANUFACTURER_SIGNED_AT_MAX_AGE_SECS
            );
            return Err(AidError::Expired);
        }

        Ok(())
    }

    async fn consume_manufacturer_auth_nonce(
        pool: &sqlx::SqlitePool,
        manufacturer: &str,
        signing_key_id: &str,
        manufacturer_auth_nonce: &[u8],
        now: u64,
    ) -> Result<(), AidError> {
        let now_i64 = i64::try_from(now)
            .map_err(|_| AidError::InvalidTimestamp("current time exceeds i64".to_string()))?;
        let expires_at = now_i64.saturating_add(MANUFACTURER_NONCE_TTL_SECS as i64);

        sqlx::query("DELETE FROM ais_manufacturer_auth_nonce WHERE expires_at < ?")
            .bind(now_i64)
            .execute(pool)
            .await
            .map_err(|e| {
                AidError::GenerationFailed(format!("manufacturer nonce cleanup failed: {e}"))
            })?;

        let result = sqlx::query(
            "INSERT INTO ais_manufacturer_auth_nonce (manufacturer, key_id, nonce, created_at, expires_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(manufacturer)
        .bind(signing_key_id)
        .bind(manufacturer_auth_nonce)
        .bind(now_i64)
        .bind(expires_at)
        .execute(pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(err)) if err.is_unique_violation() => {
                platform::recording::warn!(
                    "manufacturer_auth_nonce replay detected: manufacturer={}, key_id={}",
                    manufacturer,
                    signing_key_id
                );
                Err(AidError::ManufacturerNotVerified)
            }
            Err(err) => Err(AidError::GenerationFailed(format!(
                "manufacturer nonce consume failed: {err}"
            ))),
        }
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

    /// 计算 renewal token 过期时间（Unix timestamp，seconds，i64 for SQLite）
    fn calculate_renewal_expiry_time(&self) -> i64 {
        (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + self.config.renewal_token_ttl_secs) as i64
    }

    /// 使用 CSPRNG 生成 32 字节 renewal token
    fn generate_renewal_token(&self) -> Vec<u8> {
        let mut token = vec![0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut token);
        token
    }

    /// 为已有 ActrId 重新签发 credential（供 /ais/renew 使用）
    ///
    /// 接收已验证的 `ActrId`，不调用 `generate_actr_id()`，也不重新执行注册认证。
    pub async fn issue_credential_for_actor(
        &self,
        actor_id: &ActrId,
    ) -> Result<register_response::RegisterOk, AidError> {
        // 确保有可用的签名密钥
        self.ensure_key_loaded().await?;

        // 生成过期时间
        let expr_time = self.calculate_expiry_time();

        // 构建 IdentityClaims
        let claims_proto = IdentityClaims {
            realm_id: actor_id.realm.realm_id,
            actor_id: actor_id.to_string_repr(),
            expires_at: expr_time,
        };

        let claims_bytes = claims_proto.encode_to_vec();

        // 从缓存读取 key_id 和 verifying_key
        let (key_id, verifying_key) = {
            let cache = self.key_cache.read().await;
            let cache = cache
                .as_ref()
                .ok_or_else(|| AidError::GenerationFailed("No key available".to_string()))?;
            (cache.key_id, cache.verifying_key)
        };

        // 通过 KS Sign RPC 签名
        let signature_bytes = self
            .signer_client
            .sign(key_id, &claims_bytes)
            .await
            .map_err(|e| AidError::GenerationFailed(format!("KS sign failed: {e}")))?;

        let credential = AIdCredential {
            key_id,
            claims: Bytes::from(claims_bytes),
            signature: Bytes::from(signature_bytes),
        };

        let credential_expires_at = Some(Timestamp {
            seconds: expr_time as i64,
            nanos: 0,
        });

        // 生成 TURN 时效凭证
        let turn_credential = self.generate_turn_credential(&actor_id.to_string_repr(), expr_time);

        Ok(register_response::RegisterOk {
            actr_id: actor_id.clone(),
            credential,
            turn_credential,
            credential_expires_at,
            signaling_heartbeat_interval_secs: self.config.signaling_heartbeat_interval_secs,
            signing_pubkey: Bytes::from(verifying_key.as_bytes().to_vec()),
            signing_key_id: key_id,
            renewal_token: None,
            renewal_token_expires_at: None,
        })
    }

    /// 生成 TURN 时效凭证（coturn --use-auth-secret 兼容格式）
    ///
    /// `username = "<expires_at>:<actor_id>"`
    /// `password = base64(HMAC-SHA1(turn_secret, username))`
    fn generate_turn_credential(
        &self,
        actor_id: &str,
        expires_at: u64,
    ) -> actr_protocol::TurnCredential {
        let username = format!("{expires_at}:{actor_id}");
        let mut mac = HmacSha1::new_from_slice(self.config.turn_secret.as_bytes())
            .expect("HMAC-SHA1 accepts any key length");
        mac.update(username.as_bytes());
        let result = mac.finalize();
        let password = BASE64_STANDARD.encode(result.into_bytes());

        actr_protocol::TurnCredential {
            username,
            password,
            expires_at,
        }
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
    /// 通过 health_check RPC 验证 KS 服务可用性
    pub async fn check_ks_health(&self) -> Result<(), AidError> {
        self.signer_client
            .health_check()
            .await
            .map(|_| ())
            .map_err(|e| AidError::GenerationFailed(format!("KS service unhealthy: {e}")))
    }

    /// 检查密钥缓存健康状态
    pub async fn check_key_cache_health(&self) -> Result<KeyCacheInfo, AidError> {
        let has_cache = self.key_cache.read().await.is_some();
        if !has_cache {
            self.ensure_key_loaded().await?;
        }

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
