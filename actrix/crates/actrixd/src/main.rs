//! Actrix 辅助服务器主程序
//!
//! 启动和管理 WebRTC 相关的辅助服务，包括信令、STUN、TURN 等服务
#![deny(clippy::disallowed_macros)]

mod cli;
// mod config; // 已迁移到独立的 config crate
mod error;
mod process;
mod recording_pipeline;
mod service;

use anyhow::Context;
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use platform::config::ActrixConfig;
use recording_pipeline::init_recording_pipeline;
use service::{
    AisService, ServiceContainer, ServiceManager, SignalingService, StunService, TurnService,
};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

macro_rules! bootstrap_info {
    ($($arg:tt)*) => {
        println!($($arg)*);
    };
}

macro_rules! bootstrap_error {
    ($($arg:tt)*) => {
        eprintln!($($arg)*);
    };
}

use cli::{Cli, Commands};
use error::{Error, Result};

/// Application launcher utilities
struct ApplicationLauncher;

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Test { config_file }) => {
            let config_path =
                ApplicationLauncher::find_config_file(config_file.as_ref().unwrap_or(&cli.config))?;
            ApplicationLauncher::test_config_file(&Some(config_path.clone()), &config_path)
        }
        None => {
            let config_path = ApplicationLauncher::find_config_file(&cli.config)?;

            // Create Tokio runtime（before running the application）
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            // Run the asynchronous application
            runtime.block_on(ApplicationLauncher::run_application(&config_path))
        }
    }
}

impl ApplicationLauncher {
    /// Find config file with fallback locations
    fn find_config_file(provided_path: &PathBuf) -> Result<PathBuf> {
        // If the provided path is not the default "config.toml", check if it exists
        if provided_path != Path::new("config.toml") {
            if provided_path.exists() {
                bootstrap_info!("Using provided config file: {:?}", provided_path);
                return Ok(provided_path.clone());
            } else {
                bootstrap_error!("Provided config file not found: {:?}", provided_path);
                return Err(Error::custom(format!(
                    "Config file not found: {provided_path:?}"
                )));
            }
        }

        // Otherwise, try fallback locations
        let fallback_paths = vec![
            // 1. Current working directory
            PathBuf::from("config.toml"),
            // 2. System config directory
            PathBuf::from("/etc/actrix/config.toml"),
        ];

        bootstrap_info!("Searching for config file in default locations...");

        for path in &fallback_paths {
            if path.exists() {
                bootstrap_info!("Found config file: {:?}", path);
                return Ok(path.clone());
            } else {
                bootstrap_info!("Config not found at: {:?}", path);
            }
        }

        // If no config file found, provide helpful error message
        bootstrap_error!("No configuration file found!");
        bootstrap_error!("Please create a config file in one of these locations:");
        for (i, path) in fallback_paths.iter().enumerate() {
            bootstrap_error!("  {}. {:?}", i + 1, path);
        }
        bootstrap_error!("Or specify a custom path with: actrix --config <path>");

        Err(Error::custom(
            "No configuration file found. Please create one or specify path with --config",
        ))
    }

    /// 测试配置文件是否有效
    fn test_config_file(config_file: &Option<PathBuf>, default_config: &PathBuf) -> Result<()> {
        // Initialize basic logging for test command
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();

        let config_path = config_file.as_ref().unwrap_or(default_config);
        match ActrixConfig::from_file(config_path) {
            Ok(config) => {
                platform::recording::info!("✅ 配置文件解析成功: {:?}", config_path);

                // 验证配置
                match config.validate() {
                    Ok(()) => {
                        platform::recording::info!("✅ 配置验证通过");
                    }
                    Err(errors) => {
                        platform::recording::error!("❌ 配置验证发现问题:");
                        for (i, err) in errors.iter().enumerate() {
                            if err.starts_with("Warning:") {
                                platform::recording::info!("  {}. ⚠️  {}", i + 1, err);
                            } else {
                                platform::recording::error!("  {}. ❌ {}", i + 1, err);
                            }
                        }
                        // 检查是否有非警告错误
                        let has_errors = errors.iter().any(|e| !e.starts_with("Warning:"));
                        if has_errors {
                            return Err(Error::service_validation("配置验证失败".to_string()));
                        }
                    }
                }

                // 不需要再次初始化 recording 管线，因为已经初始化了基本日志
                platform::recording::info!("✅ 完整配置验证通过");
                Ok(())
            }
            Err(e) => {
                platform::recording::error!("❌ 配置文件解析失败: {}", e);
                Err(Error::service_validation(format!("配置解析失败: {e}")))
            }
        }
    }

    /// 运行应用程序的主入口
    async fn run_application(config_path: &Path) -> Result<()> {
        bootstrap_info!("📄 加载配置文件: {:?}", config_path);

        // 加载配置文件
        let config = match ActrixConfig::from_file(config_path) {
            Ok(config) => {
                bootstrap_info!("✅ 配置加载成功");

                // 验证配置
                if let Err(errors) = config.validate() {
                    bootstrap_error!("❌ 配置验证发现问题:");
                    let mut has_critical_errors = false;
                    for (i, err) in errors.iter().enumerate() {
                        if err.starts_with("Warning:") {
                            bootstrap_info!("  {}. ⚠️  {}", i + 1, err);
                        } else {
                            bootstrap_error!("  {}. ❌ {}", i + 1, err);
                            has_critical_errors = true;
                        }
                    }
                    if has_critical_errors {
                        return Err(Error::custom("配置验证失败，请修复上述错误".to_string()));
                    }
                }

                config
            }
            Err(e) => {
                bootstrap_error!("❌ 配置加载失败: {}", e);
                return Err(Error::custom(format!("配置加载失败: {e}")));
            }
        };

        // ensure sqlite_path directory exists
        if !config.sqlite_path.exists() {
            std::fs::create_dir_all(&config.sqlite_path).with_context(|| {
                format!(
                    "Failed to create SQLite data directory: {}",
                    config.sqlite_path.display()
                )
            })?;
        }

        // 初始化记录管线（日志 + 追踪）
        let _recording_pipeline_guard = init_recording_pipeline(&config)?;

        // 写入 PID 文件（在绑定端口之前，需要权限）
        let pid_path = process::ProcessManager::write_pid_file(config.get_pid_path().as_deref())?;
        let _pid_guard = process::PidFileGuard::new(pid_path);

        // 需要在创建服务之前克隆配置，因为服务可能需要 root 权限来绑定端口
        let user = config.user.clone();
        let group = config.group.clone();

        // 运行服务
        Self::run_services_with_privilege_drop(config, user, group, config_path.to_path_buf()).await
    }

    /// 运行服务并在适当时机切换用户权限
    async fn run_services_with_privilege_drop(
        config: ActrixConfig,
        user: Option<String>,
        group: Option<String>,
        config_path: PathBuf,
    ) -> Result<()> {
        platform::recording::info!("🚀 启动 WebRTC 辅助服务器集群");

        // First initialize the database,
        // ensure it is ready before any service that may access it starts
        platform::storage::db::set_db_path(&config.sqlite_path)
            .await
            .map_err(|e| Error::custom(format!("数据库初始化失败: {e}")))?;
        platform::recording::info!("✅ 数据库初始化完成");

        // 初始化全局关闭通道（供所有服务共享）
        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(10);

        // 安装 Ctrl-C 处理器，确保任何阶段都能广播关闭
        setup_ctrl_c_handler(shutdown_tx.clone()).await;

        // 如果启用 KS，构建 gRPC 服务 future
        let mut handle_futs: Vec<JoinHandle<()>> = Vec::new();

        let mut service_manager =
            Self::create_service_manager(config.clone(), shutdown_tx.clone(), config_path).await?;

        let handle_futures = service_manager.start_all().await?;
        handle_futs.extend(handle_futures);
        platform::recording::info!("启动所有服务...");

        // 端口绑定完成后，切换用户和组
        platform::recording::info!("服务启动完成，准备切换用户权限...");
        if let Err(e) = process::ProcessManager::drop_privileges(user.as_deref(), group.as_deref())
        {
            platform::recording::error!("Failed to drop privileges: {}", e);
            // 继续运行，但记录错误
        }

        // 显示服务信息
        Self::display_service_info(&config);

        // Setup SIGHUP handler for hot reload
        let mut sighup_rx = setup_sighup_channel();

        // Main event loop: monitor services, SIGHUP, and shutdown
        let mut shutdown_rx = shutdown_tx.subscribe();
        let mut pending: FuturesUnordered<_> = handle_futs.into_iter().collect();
        loop {
            tokio::select! {
                Some(result) = pending.next() => {
                    if let Err(e) = result {
                        platform::recording::error!("Service task terminated unexpectedly: {}", e);
                        let _ = shutdown_tx.send(());
                        break;
                    }
                    // A task completed normally (e.g. shutdown in progress).
                    // If all tasks are done, next() returns None and the loop
                    // exits via the else branch.
                }
                Some(()) = sighup_rx.recv() => {
                    platform::recording::info!("Received SIGHUP, initiating configuration reload...");
                    if let Err(e) = service_manager.reload().await {
                        platform::recording::error!(
                            "Configuration reload failed: {}. Continuing with previous config.",
                            e
                        );
                    }
                }
                _ = shutdown_rx.recv() => {
                    platform::recording::info!("Shutdown signal received, exiting main loop");
                    break;
                }
                else => break,
            }
        }
        service_manager.stop_all().await?;

        platform::recording::info!("🛑 所有服务已安全关闭");
        Ok(())
    }

    /// 创建服务管理器
    async fn create_service_manager(
        config: ActrixConfig,
        shutdown_tx: tokio::sync::broadcast::Sender<()>,
        config_path: PathBuf,
    ) -> Result<ServiceManager> {
        platform::recording::info!("📊 计划启动的服务:");
        // 数据库已在 run_services_with_privilege_drop 中提前初始化，
        // 以确保 control gRPC 头可以安全处理 RPC 回调

        // 初始化 Prometheus metrics registry
        let registry = &platform::metrics::REGISTRY;
        if let Err(e) = platform::metrics::register_metrics() {
            platform::recording::warn!(
                "Prometheus metrics registration warning (may already be registered): {}",
                e
            );
        }

        // 注册各服务的 metrics
        if config.is_signer_enabled()
            && let Err(e) = signer::register_signer_metrics(registry)
        {
            platform::recording::warn!(
                "KS metrics registration warning (may already be registered): {}",
                e
            );
        }

        platform::recording::info!("✅ Prometheus metrics registry 初始化成功");

        let mut service_manager =
            ServiceManager::new(config.clone(), shutdown_tx.clone(), config_path);
        // 添加ICE服务 - 细粒度控制STUN和TURN
        if config.is_ice_enabled() {
            if config.is_turn_enabled() {
                platform::recording::info!("  - TURN Server (UDP, 包含内置 STUN 支持)");
                let turn_service = TurnService::new(config.clone());
                service_manager.add_service(ServiceContainer::turn(turn_service));
            } else if config.is_stun_enabled() {
                platform::recording::info!("  - STUN Server (UDP)");
                let stun_service = StunService::new(config.clone());
                service_manager.add_service(ServiceContainer::stun(stun_service));
            }
        } else {
            platform::recording::info!("ICE服务(STUN/TURN)已禁用");
        }

        // 添加HTTP路由服务 - 每个服务独立控制
        if config.is_signaling_enabled() {
            platform::recording::info!("  - Signaling WebSocket Service (/signaling)");
            let signaling_service =
                SignalingService::new(config.clone(), tokio_util::sync::CancellationToken::new());
            service_manager.add_service(ServiceContainer::signaling(signaling_service));
        }

        if config.is_ais_enabled() {
            platform::recording::info!("  - AIS Service (/ais)");
            let ais_service =
                AisService::new(config.clone(), tokio_util::sync::CancellationToken::new());
            service_manager.add_service(ServiceContainer::ais(ais_service));
        }

        if config.is_signer_enabled() {
            platform::recording::info!("  - Signer Service (gRPC: /signer.v1.Signer/<Method>)");
        }

        platform::recording::info!(
            "  - Control Service (admin_ui={}, grpc_api={})",
            config.admin_ui_enabled(),
            config.grpc_api_enabled()
        );

        Ok(service_manager)
    }

    /// 显示服务信息
    fn display_service_info(config: &ActrixConfig) {
        // Determine which URLs are available
        let mut urls = Vec::new();

        if let Some(ref http_config) = config.bind.http {
            let scheme = http_config.scheme();
            let ws_scheme = http_config.ws_scheme();
            let protocol = if http_config.is_tls() {
                "HTTPS"
            } else {
                "HTTP"
            };
            let http_url = format!("{}://{}:{}", scheme, http_config.ip, http_config.port);
            let ws_url = format!("{}://{}:{}", ws_scheme, http_config.ip, http_config.port);
            urls.push((protocol, http_url, ws_url));
        }

        platform::recording::info!("✅ 所有服务已启动");

        if !urls.is_empty() {
            for (protocol, http_url, _ws_url) in &urls {
                platform::recording::info!("📡 {} 服务器监听在: {}", protocol, http_url);
                platform::recording::info!("🔧 可用的API端点:");
                if config.is_signaling_enabled() {
                    platform::recording::info!("  - {}/signaling/ws", _ws_url);
                }
                if config.is_signer_enabled() {
                    platform::recording::info!("  - {}/signer.v1.Signer/<Method>", http_url);
                }
                if config.is_ais_enabled() {
                    platform::recording::info!("  - {}/ais/health", http_url);
                    platform::recording::info!("  - {}/ais/register (POST protobuf)", http_url);
                }
                if config.admin_ui_enabled() {
                    platform::recording::info!("  - {}/admin", http_url);
                    platform::recording::info!("  - {}/admin/health", http_url);
                }
                if config.grpc_api_enabled() {
                    platform::recording::info!(
                        "  - {}/admin.v1.NodeAdminService/<Method>",
                        http_url
                    );
                }
            }
        } else {
            platform::recording::info!("📡 没有配置 HTTP/HTTPS 服务器");
        }

        // KS gRPC 已复用主 HTTP 监听端口，无独立 gRPC 端口。
    }
}

/// 设置Ctrl-C信号处理程序
async fn setup_ctrl_c_handler(shutdown_tx: tokio::sync::broadcast::Sender<()>) {
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            platform::recording::error!("无法监听Ctrl-C信号: {}", e);
            return;
        }
        platform::recording::info!("收到Ctrl-C信号，开始优雅关闭...");
        let _ = shutdown_tx.send(());
    });
}

/// Setup SIGHUP signal handler for hot-reload.
///
/// Returns an mpsc receiver that yields `()` each time SIGHUP is received.
#[cfg(unix)]
fn setup_sighup_channel() -> mpsc::Receiver<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let (tx, rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        let mut sighup = signal(SignalKind::hangup()).expect("Failed to install SIGHUP handler");
        loop {
            sighup.recv().await;
            if tx.send(()).await.is_err() {
                break;
            }
        }
    });
    rx
}

/// On non-Unix platforms, SIGHUP is not available. Return a receiver that
/// never yields.
#[cfg(not(unix))]
fn setup_sighup_channel() -> mpsc::Receiver<()> {
    let (_tx, rx) = mpsc::channel::<()>(1);
    rx
}
