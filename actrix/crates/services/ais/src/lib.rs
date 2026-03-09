//! Actor Identity Service (AIS) - ActrId 注册和凭证签发服务
//!
//! # 功能概述
//!
//! AIS 是 Actrix 系统的核心身份服务，负责：
//! - ActrId 注册：为新 Actor 分配全局唯一的序列号
//! - 凭证签发：生成加密的 AIdCredential Token
//! - PSK 生成：为 Actor 与 Signaling Server 的连接生成预共享密钥
//!
//! # 架构设计
//!
//! ```text
//! ┌──────────────┐
//! │   Client     │
//! └──────┬───────┘
//!        │ POST /ais/register (protobuf)
//!        ▼
//! ┌──────────────────────────────────────────┐
//! │  AIS Service                             │
//! │  ┌────────────┐      ┌────────────────┐ │
//! │  │  Handlers  │─────▶│  AIdIssuer     │ │
//! │  └────────────┘      └────────┬───────┘ │
//! │                               │         │
//! │  ┌──────────────────┐  ┌─────▼──────┐  │
//! │  │ SN Generator     │  │ KeyStorage │  │
//! │  │ (Snowflake)      │  │ (SQLite)   │  │
//! │  └──────────────────┘  └────────────┘  │
//! └──────────────┬───────────────────────────┘
//!                │ KS Client
//!                ▼
//!         ┌─────────────┐
//!         │ KS Service  │ (密钥生成)
//!         └─────────────┘
//! ```
//!
//! # 核心流程
//!
//! ## 注册流程
//!
//! 1. 接收 `RegisterRequest` (protobuf 格式)
//! 2. 使用 Snowflake 算法生成 54-bit serial_number
//! 3. 从 KS 获取公钥，加密 Claims 生成 AIdCredential
//! 4. 生成 256-bit PSK（客户端负责保管）
//! 5. 返回 `RegisterResponse`（包含 ActrId + Credential + PSK）
//!
//! ## 密钥管理
//!
//! - **获取**：启动时从本地 SQLite 加载缓存密钥，如果过期则从 KS 获取
//! - **刷新**：后台任务每 10 分钟检查，提前 10 分钟刷新即将过期的密钥
//! - **容忍**：密钥过期后 24 小时内仍可使用（避免时钟偏差导致服务中断）
//!
//! # 使用示例
//!
//! ```no_run
//! use ais::create_ais_router;
//! use platform::config::{AisConfig, ActrixConfig};
//! use tokio_util::sync::CancellationToken;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // 创建配置
//! let global_config = ActrixConfig::default();
//! let ais_config = AisConfig::default();
//! let cancel = CancellationToken::new();
//!
//! // 创建 AIS 路由器
//! let router = create_ais_router(&ais_config, &global_config, cancel).await?;
//!
//! // 集成到主 HTTP 服务
//! // let app = Router::new().nest("/ais", router);
//! # Ok(())
//! # }
//! ```
//!
//! # 安全考虑
//!
//! - **Stateless 设计**：PSK 不在服务端存储，由客户端保管
//! - **加密传输**：Token 使用 ECIES 加密，只有持有私钥的服务才能解密
//! - **序列号唯一性**：Snowflake 算法保证分布式环境下的全局唯一性
//! - **密钥轮换**：支持自动密钥刷新，旧密钥在容忍期内仍可验证旧 Token
//!
//! # 配置选项
//!
//! 参见 [`platform::config::AisConfig`] 获取完整配置说明。
#![deny(clippy::disallowed_macros)]

pub mod handlers;
pub mod issuer;
pub mod ks_client_wrapper;
pub mod ratelimit;
mod sn;
mod storage;

pub use issuer::{AIdIssuer, IssuerConfig, KeyCacheInfo};

use crate::handlers::{AISState, create_router};
use crate::ks_client_wrapper::create_ks_client;
use anyhow::{Context, Result};
use axum::Router;
use platform::config::AisConfig;
use platform::monitoring::ServiceCounters;
use std::sync::Arc;

/// 创建 AIS 路由器，遵循项目的 HttpRouterService 架构
pub async fn create_ais_router(
    config: &AisConfig,
    global_config: &platform::config::ActrixConfig,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<Router> {
    create_ais_router_with_counters(config, global_config, cancel, None).await
}

/// Create AIS router with optional service counters for metrics collection.
pub async fn create_ais_router_with_counters(
    config: &AisConfig,
    global_config: &platform::config::ActrixConfig,
    cancel: tokio_util::sync::CancellationToken,
    counters: Option<Arc<ServiceCounters>>,
) -> Result<Router> {
    platform::recording::info!("Creating AIS router with config");

    // 获取 KS 客户端配置
    let ks_client_config = config
        .get_ks_client_config(global_config)
        .context("Failed to get KS client config. Ensure KS is enabled or ais.dependencies.ks is configured.")?;

    // 创建 KS gRPC 客户端
    let ks_client = create_ks_client(&ks_client_config, &global_config.actrix_shared_key)
        .await
        .context("Failed to create KS gRPC client")?;

    platform::recording::info!("KS gRPC client created successfully");

    // 创建 Issuer 配置
    let issuer_config = IssuerConfig {
        token_ttl_secs: config.server.token_ttl_secs,
        signaling_heartbeat_interval_secs: config.server.signaling_heartbeat_interval_secs,
        key_refresh_interval_secs: 3600, // 1 小时
        key_storage_file: global_config.sqlite_path.join("ais_keys.db"),
        enable_periodic_rotation: false, // 默认禁用，可通过配置文件开启
        key_rotation_interval_secs: 86400, // 24 小时
        turn_secret: global_config.turn.turn_secret.clone(),
    };

    // 创建 AId Token 签发器
    let issuer = AIdIssuer::new(ks_client, issuer_config, cancel)
        .await
        .context("Failed to create AIS issuer")?;

    let state = if let Some(ctr) = counters {
        AISState::new(issuer).with_counters(ctr)
    } else {
        AISState::new(issuer)
    };

    // 创建路由器
    let router = create_router(state);

    platform::recording::info!("AIS router created successfully");
    Ok(router)
}

#[cfg(test)]
mod tests {

    // Note: 完整的集成测试需要 KS 服务运行
    // 这里仅做基本的单元测试
    // 实际测试在主程序启动后通过 HTTP 端点进行
}
