//! Actor-RTC 辅助服务器主程序
//!
//! 启动和管理 WebRTC 相关的辅助服务，包括信令、STUN、TURN 等服务

mod cli;
// mod config; // 已迁移到独立的 config crate
mod error;
mod process;
mod service;

use actrix_common::config::ActrixConfig;
use clap::Parser;
use service::{
    AisService, KsGrpcService, KsHttpService, ServiceContainer, ServiceManager, SignalingService,
    StunService, SupervisorService, TurnService,
};
use std::path::{Path, PathBuf};

use tracing::{error, info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

#[cfg(feature = "opentelemetry")]
use opentelemetry::KeyValue;
#[cfg(feature = "opentelemetry")]
use opentelemetry_otlp::WithExportConfig;
#[cfg(feature = "opentelemetry")]
use opentelemetry_sdk::propagation::TraceContextPropagator;
#[cfg(feature = "opentelemetry")]
use opentelemetry_sdk::{
    Resource,
    trace::{self, SdkTracerProvider},
};
#[cfg(feature = "opentelemetry")]
use tracing_opentelemetry::OpenTelemetryLayer;

use cli::{Cli, Commands};
use error::{Error, Result};

/// Observability guard that manages lifecycle of tracing and logging resources
///
/// Ensures proper shutdown of OpenTelemetry tracer provider and log file handles
#[derive(Default)]
struct ObservabilityGuard {
    #[cfg(feature = "opentelemetry")]
    tracer_provider: Option<SdkTracerProvider>,
    log_guard: Option<WorkerGuard>,
}

impl Drop for ObservabilityGuard {
    fn drop(&mut self) {
        #[cfg(feature = "opentelemetry")]
        if let Some(provider) = self.tracer_provider.take() {
            if let Err(e) = provider.shutdown() {
                eprintln!("Failed to shutdown tracer provider: {e:?}");
            }
        }
    }
}

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
            ApplicationLauncher::run_application(&config_path)
        }
    }
}

impl ApplicationLauncher {
    /// Find config file with fallback locations
    fn find_config_file(provided_path: &PathBuf) -> Result<PathBuf> {
        // If the provided path is not the default "config.toml", check if it exists
        if provided_path != Path::new("config.toml") {
            if provided_path.exists() {
                info!("Using provided config file: {:?}", provided_path);
                return Ok(provided_path.clone());
            } else {
                error!("Provided config file not found: {:?}", provided_path);
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
            PathBuf::from("/etc/actor-rtc-actrix/config.toml"),
        ];

        info!("Searching for config file in default locations...");

        for path in &fallback_paths {
            if path.exists() {
                info!("Found config file: {:?}", path);
                return Ok(path.clone());
            } else {
                info!("Config not found at: {:?}", path);
            }
        }

        // If no config file found, provide helpful error message
        error!("No configuration file found!");
        error!("Please create a config file in one of these locations:");
        for (i, path) in fallback_paths.iter().enumerate() {
            error!("  {}. {:?}", i + 1, path);
        }
        error!("Or specify a custom path with: actrix --config <path>");

        Err(Error::custom(
            "No configuration file found. Please create one or specify path with --config",
        ))
    }

    /// 初始化可观测性系统（日志 + 追踪）
    fn init_observability(config: &ActrixConfig) -> Result<ObservabilityGuard> {
        let mut guard = ObservabilityGuard::default();

        // 创建日志目录
        std::fs::create_dir_all(&config.log_path)?;

        let log_filter = EnvFilter::new(config.get_log_level());

        // 控制台输出模式
        if config.is_console_logging() {
            #[cfg(feature = "opentelemetry")]
            {
                if let Some((otel_layer, provider)) = Self::build_tracing_layer(config)? {
                    guard.tracer_provider = Some(provider);

                    tracing_subscriber::registry()
                        .with(otel_layer)
                        .with(
                            fmt::layer()
                                .with_target(true)
                                .with_level(true)
                                .with_line_number(true)
                                .with_file(true)
                                .with_ansi(true)
                                .with_filter(log_filter),
                        )
                        .init();

                    info!("✅ 可观测性系统初始化完成 (控制台 + OpenTelemetry)");
                    Self::log_status(config);
                    return Ok(guard);
                }
            }

            // 没有 OpenTelemetry 或未启用
            tracing_subscriber::registry()
                .with(
                    fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .with_line_number(true)
                        .with_file(true)
                        .with_ansi(true)
                        .with_filter(log_filter),
                )
                .init();

            info!("✅ 日志系统初始化完成 (控制台)");
            info!("📝 日志级别: {}", config.log_level);
            return Ok(guard);
        }

        // 文件输出模式
        let (non_blocking, worker_guard) = if config.should_rotate_logs() {
            // 按天轮转日志文件
            let file_appender = tracing_appender::rolling::daily(&config.log_path, "actrix.log");
            tracing_appender::non_blocking(file_appender)
        } else {
            // 追加到单个文件，不轮转
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("{}/actrix.log", config.log_path))?;
            tracing_appender::non_blocking(file)
        };

        guard.log_guard = Some(worker_guard);

        #[cfg(feature = "opentelemetry")]
        {
            if let Some((otel_layer, provider)) = Self::build_tracing_layer(config)? {
                guard.tracer_provider = Some(provider);

                tracing_subscriber::registry()
                    .with(otel_layer)
                    .with(
                        fmt::layer()
                            .with_target(true)
                            .with_level(true)
                            .with_line_number(true)
                            .with_file(true)
                            .with_ansi(false) // 文件输出禁用颜色
                            .with_writer(non_blocking)
                            .with_filter(log_filter),
                    )
                    .init();

                info!("✅ 可观测性系统初始化完成 (文件 + OpenTelemetry)");
                Self::log_status(config);
                return Ok(guard);
            }
        }

        // 没有 OpenTelemetry 或未启用
        tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_target(true)
                    .with_level(true)
                    .with_line_number(true)
                    .with_file(true)
                    .with_ansi(false) // 文件输出禁用颜色
                    .with_writer(non_blocking)
                    .with_filter(log_filter),
            )
            .init();

        info!("✅ 日志系统初始化完成 (文件)");
        Self::log_status(config);

        Ok(guard)
    }

    /// 构建 OpenTelemetry 追踪层
    #[cfg(feature = "opentelemetry")]
    fn build_tracing_layer(
        config: &ActrixConfig,
    ) -> Result<
        Option<(
            OpenTelemetryLayer<tracing_subscriber::Registry, trace::SdkTracer>,
            SdkTracerProvider,
        )>,
    > {
        let tracing_cfg = config.tracing_config();

        if !tracing_cfg.is_enabled() {
            return Ok(None);
        }

        // 验证配置
        if let Err(e) = tracing_cfg.validate() {
            error!("OpenTelemetry 配置验证失败: {}", e);
            return Ok(None);
        }

        // 构建 OTLP exporter
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(tracing_cfg.endpoint())
            .build()
            .map_err(|e| Error::custom(format!("Failed to build OTLP exporter: {e}")))?;

        // 构建资源标签
        let resource = Resource::builder()
            .with_service_name(tracing_cfg.service_name().to_string())
            .with_attributes([
                KeyValue::new("service.instance.id", config.name.clone()),
                KeyValue::new("service.environment", config.env.clone()),
                KeyValue::new("service.location", config.location_tag.clone()),
            ])
            .build();

        // 构建 tracer provider
        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build();

        // 设置全局 tracer provider
        opentelemetry::global::set_tracer_provider(tracer_provider.clone());
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        // 创建 tracer
        use opentelemetry::trace::TracerProvider as _;
        let tracer = tracer_provider.tracer("actrix");
        let layer = tracing_opentelemetry::layer().with_tracer(tracer);

        Ok(Some((layer, tracer_provider)))
    }

    /// 记录日志和追踪状态
    fn log_status(config: &ActrixConfig) {
        info!("📝 日志配置:");
        info!("  - 级别: {}", config.log_level);
        info!("  - 输出: {}", config.log_output);

        if config.log_output == "file" {
            info!("  - 路径: {}", config.log_path);
            info!(
                "  - 轮转: {}",
                if config.log_rotate {
                    "开启（按天）"
                } else {
                    "关闭"
                }
            );
        }

        #[cfg(feature = "opentelemetry")]
        {
            let tracing_cfg = config.tracing_config();
            if tracing_cfg.is_enabled() {
                info!("📊 OpenTelemetry 追踪:");
                info!("  - 服务名: {}", tracing_cfg.service_name());
                info!("  - OTLP 端点: {}", tracing_cfg.endpoint());
                info!("  - 实例 ID: {}", config.name);
                info!("  - 环境: {}", config.env);
                info!("  - 位置: {}", config.location_tag);
            }
        }
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
                info!("✅ 配置文件解析成功: {:?}", config_path);

                // 验证配置
                match config.validate() {
                    Ok(()) => {
                        info!("✅ 配置验证通过");
                    }
                    Err(errors) => {
                        error!("❌ 配置验证发现问题:");
                        for (i, err) in errors.iter().enumerate() {
                            if err.starts_with("Warning:") {
                                info!("  {}. ⚠️  {}", i + 1, err);
                            } else {
                                error!("  {}. ❌ {}", i + 1, err);
                            }
                        }
                        // 检查是否有非警告错误
                        let has_errors = errors.iter().any(|e| !e.starts_with("Warning:"));
                        if has_errors {
                            return Err(Error::service_validation("配置验证失败".to_string()));
                        }
                    }
                }

                // 不需要再次初始化 observability，因为已经初始化了基本日志
                info!("✅ 完整配置验证通过");
                Ok(())
            }
            Err(e) => {
                error!("❌ 配置文件解析失败: {}", e);
                Err(Error::service_validation(format!("配置解析失败: {e}")))
            }
        }
    }

    /// 运行应用程序的主入口
    fn run_application(config_path: &PathBuf) -> Result<()> {
        info!("📄 加载配置文件: {:?}", config_path);

        // 加载配置文件
        let config = match ActrixConfig::from_file(config_path) {
            Ok(config) => {
                info!("✅ 配置加载成功");

                // 验证配置
                if let Err(errors) = config.validate() {
                    error!("❌ 配置验证发现问题:");
                    let mut has_critical_errors = false;
                    for (i, err) in errors.iter().enumerate() {
                        if err.starts_with("Warning:") {
                            info!("  {}. ⚠️  {}", i + 1, err);
                        } else {
                            error!("  {}. ❌ {}", i + 1, err);
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
                error!("❌ 配置加载失败: {}", e);
                return Err(Error::custom(format!("配置加载失败: {e}")));
            }
        };

        // 初始化可观测性系统（日志 + 追踪）
        let _observability_guard = Self::init_observability(&config)?;

        // 写入 PID 文件（在绑定端口之前，需要权限）
        let pid_path = process::ProcessManager::write_pid_file(config.get_pid_path().as_deref())?;
        let _pid_guard = process::PidFileGuard::new(pid_path);

        // 创建tokio runtime (自动使用默认工作线程数)
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        // 需要在创建服务之前克隆配置，因为服务可能需要 root 权限来绑定端口
        let user = config.user.clone();
        let group = config.group.clone();

        // 运行服务
        runtime.block_on(Self::run_services_with_privilege_drop(config, user, group))
    }

    /// 运行服务并在适当时机切换用户权限
    async fn run_services_with_privilege_drop(
        config: ActrixConfig,
        user: Option<String>,
        group: Option<String>,
    ) -> Result<()> {
        info!("🚀 启动 WebRTC 辅助服务器集群");

        // 先创建并启动所有需要特权端口的服务
        let mut service_manager = Self::create_service_manager(config.clone()).await?;

        // 启动所有服务（这会绑定端口）
        info!("启动所有服务...");
        if let Err(e) = service_manager.start_all().await {
            error!("启动服务失败: {}", e);
            return Err(Error::service_startup(format!("启动服务失败: {e}")));
        }

        // 启动 KS gRPC 服务（如果启用）
        let grpc_handle = if config.is_ks_enabled() {
            info!("启动 KS gRPC 服务器...");
            let mut grpc_service = KsGrpcService::new(config.clone());
            let grpc_addr = "127.0.0.1:50052".parse().map_err(|e| {
                Error::service_startup(format!("Failed to parse gRPC address: {e}"))
            })?;
            let shutdown_rx = service_manager.shutdown_receiver();

            let handle = tokio::spawn(async move {
                if let Err(e) = grpc_service.start(grpc_addr, shutdown_rx).await {
                    error!("KS gRPC service error: {}", e);
                }
            });
            Some(handle)
        } else {
            None
        };

        // 端口绑定完成后，切换用户和组
        info!("服务启动完成，准备切换用户权限...");
        if let Err(e) = process::ProcessManager::drop_privileges(user.as_deref(), group.as_deref())
        {
            error!("Failed to drop privileges: {}", e);
            // 继续运行，但记录错误
        }

        // 显示服务信息
        Self::display_service_info(&config);

        // 等待关闭信号
        if let Err(e) = service_manager.wait_for_shutdown().await {
            error!("Error during shutdown: {}", e);
        }
        info!("收到关闭信号，等待所有服务停止...");

        // 等待 gRPC 服务停止
        if let Some(handle) = grpc_handle {
            info!("等待 KS gRPC 服务停止...");
            let _ = handle.await;
        }

        info!("🛑 所有服务已安全关闭");
        Ok(())
    }

    /// 创建服务管理器
    async fn create_service_manager(config: ActrixConfig) -> Result<ServiceManager> {
        info!("📊 计划启动的服务:");
        actrix_common::storage::db::set_db_path(Path::new(&config.sqlite)).await?;

        // 初始化 Prometheus metrics registry
        let registry = &actrix_common::metrics::REGISTRY;
        if let Err(e) = actrix_common::metrics::register_metrics() {
            warn!(
                "Prometheus metrics registration warning (may already be registered): {}",
                e
            );
        }

        // 注册各服务的 metrics
        if config.is_ks_enabled() {
            if let Err(e) = ks::register_ks_metrics(registry) {
                warn!(
                    "KS metrics registration warning (may already be registered): {}",
                    e
                );
            }
        }

        info!("✅ Prometheus metrics registry 初始化成功");

        let mut service_manager = ServiceManager::new(config.clone());
        // 添加ICE服务 - 细粒度控制STUN和TURN
        if config.is_ice_enabled() {
            if config.is_turn_enabled() {
                info!("  - TURN Server (UDP, 包含内置 STUN 支持)");
                let turn_service = TurnService::new(config.clone());
                service_manager.add_service(ServiceContainer::turn(turn_service));
            } else if config.is_stun_enabled() {
                info!("  - STUN Server (UDP)");
                let stun_service = StunService::new(config.clone());
                service_manager.add_service(ServiceContainer::stun(stun_service));
            }
        } else {
            info!("ICE服务(STUN/TURN)已禁用");
        }

        // 添加HTTP路由服务 - 每个服务独立控制
        if config.is_supervisor_enabled() {
            info!("  - Supervisor Client Service (/supervisor)");
            let supervisor_service = SupervisorService::new(config.clone());
            service_manager.add_service(ServiceContainer::supervisor(supervisor_service));
        }

        if config.is_signaling_enabled() {
            info!("  - Signaling WebSocket Service (/signaling)");
            let signaling_service = SignalingService::new(config.clone());
            service_manager.add_service(ServiceContainer::signaling(signaling_service));
        }

        if config.is_ais_enabled() {
            info!("  - AIS Service (/ais)");
            let ais_service = AisService::new(config.clone());
            service_manager.add_service(ServiceContainer::ais(ais_service));
        }

        if config.is_ks_enabled() {
            info!("  - KS Service (/ks)");
            let ks_service = KsHttpService::new(config.clone());
            service_manager.add_service(ServiceContainer::ks(ks_service));
        }

        // 设置Ctrl-C信号处理程序
        setup_ctrl_c_handler(service_manager.shutdown_sender()).await;

        Ok(service_manager)
    }

    /// 显示服务信息
    fn display_service_info(config: &ActrixConfig) {
        let is_dev = config.env == "dev";

        // Determine which URLs are available
        let mut urls = Vec::new();

        if is_dev {
            if let Some(ref http_config) = config.bind.http {
                let http_url = format!("http://{}:{}", http_config.ip, http_config.port);
                let ws_url = format!("ws://{}:{}", http_config.ip, http_config.port);
                urls.push(("HTTP", http_url, ws_url));
            }
        }

        if let Some(ref https_config) = config.bind.https {
            let https_url = format!("https://{}:{}", https_config.domain_name, https_config.port);
            let wss_url = format!("wss://{}:{}", https_config.domain_name, https_config.port);
            urls.push(("HTTPS", https_url, wss_url));
        }

        info!("✅ 所有服务已启动");

        if !urls.is_empty() {
            for (protocol, http_url, _ws_url) in &urls {
                info!("📡 {} 服务器监听在: {}", protocol, http_url);
                info!("🔧 可用的API端点:");
                if config.is_supervisor_enabled() {
                    info!("  - {}/supervisor/health", http_url);
                }
                if config.is_signaling_enabled() {
                    info!("  - {}/signaling/ws", _ws_url);
                }
                if config.is_ks_enabled() {
                    info!("  - {}/ks/health", http_url);
                }
                if config.is_ais_enabled() {
                    info!("  - {}/ais/health", http_url);
                    info!("  - {}/ais/register (POST protobuf)", http_url);
                }
            }
        } else {
            info!("📡 没有配置 HTTP/HTTPS 服务器");
        }

        // 显示 gRPC 服务信息
        if config.is_ks_enabled() {
            info!("🔌 gRPC 服务:");
            info!("  - KS gRPC Server: 127.0.0.1:50052");
        }
    }
}

/// 设置Ctrl-C信号处理程序
async fn setup_ctrl_c_handler(shutdown_tx: tokio::sync::broadcast::Sender<()>) {
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("无法监听Ctrl-C信号: {}", e);
            return;
        }
        info!("收到Ctrl-C信号，开始优雅关闭...");
        let _ = shutdown_tx.send(());
    });
}
