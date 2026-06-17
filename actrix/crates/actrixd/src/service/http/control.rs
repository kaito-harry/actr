use super::admin_api::{AdminApiState, build_admin_api_router};
use actrix_sdk::control::{AdminApiService, AuthService, NodeAdminServiceServer};
use anyhow::Result;
use axum::{Json, Router, routing::get};
use platform::{
    ServiceCollector, config::ActrixConfig, config::config_store::ConfigOverrideStore,
    monitoring::MetricsStore, storage::nonce::SqliteNonceStorage,
};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Build control router with absolute routes.
///
/// Control is always available and reuses the main HTTP listener.
///
/// `jwt_secret` is an optional pre-existing secret for Admin UI JWT tokens.
/// When `None`, a new random secret is generated.  Pass the same secret on
/// reload to avoid invalidating active admin sessions.
///
/// Returns `(Router, jwt_secret)` — the caller should persist the secret
/// for subsequent reloads.
pub async fn build_control_router(
    config: &ActrixConfig,
    service_collector: ServiceCollector,
    shutdown_tx: broadcast::Sender<()>,
    config_path: PathBuf,
    jwt_secret: Option<Vec<u8>>,
    config_store: Arc<ConfigOverrideStore>,
    metrics_store: MetricsStore,
) -> Result<(Router, Vec<u8>)> {
    let mut router = Router::new();
    let mut jwt = jwt_secret.unwrap_or_default();
    let admin_ui_enabled = config.admin_ui_enabled();
    let grpc_api_enabled = config.grpc_api_enabled();

    if admin_ui_enabled {
        let (admin_router, next_jwt) = build_admin_ui_router(
            config,
            service_collector.clone(),
            shutdown_tx.clone(),
            config_path.clone(),
            Some(jwt),
            config_store.clone(),
            metrics_store,
        )
        .await?;
        router = router.merge(admin_router);
        jwt = next_jwt;
    }

    if grpc_api_enabled {
        let grpc_router = build_grpc_api_router(
            config,
            service_collector,
            shutdown_tx,
            config_store,
            !admin_ui_enabled,
        )
        .await?;
        router = router.merge(grpc_router);
    }

    if !admin_ui_enabled && !grpc_api_enabled {
        anyhow::bail!("No control endpoint enabled");
    }

    Ok((router, jwt))
}

async fn build_admin_ui_router(
    config: &ActrixConfig,
    service_collector: ServiceCollector,
    shutdown_tx: broadcast::Sender<()>,
    config_path: PathBuf,
    jwt_secret: Option<Vec<u8>>,
    config_store: Arc<ConfigOverrideStore>,
    metrics_store: MetricsStore,
) -> Result<(Router, Vec<u8>)> {
    // Read raw TOML content for L1 detection
    let toml_content = tokio::fs::read_to_string(&config_path)
        .await
        .unwrap_or_default();

    let mut service = AdminApiService::new(
        config.name.clone(),
        config.name.clone(),
        config.location_tag.clone(),
        env!("CARGO_PKG_VERSION"),
        service_collector,
    )
    .map_err(|e| anyhow::anyhow!("Failed to create admin API service: {e}"))?;

    service = service
        .with_override_store(config_store)
        .with_running_config(config.clone())
        .with_toml_content(toml_content)
        .with_config_path(config_path);

    let shutdown_tx_for_handler = shutdown_tx.clone();
    service = service.with_shutdown_handler(move |_graceful, _timeout, reason| {
        let shutdown_tx = shutdown_tx_for_handler.clone();
        async move {
            if let Some(reason) = reason {
                platform::recording::warn!("Admin UI shutdown requested: {}", reason);
            } else {
                platform::recording::warn!("Admin UI shutdown requested");
            }
            let _ = shutdown_tx.send(());
            Ok(())
        }
    });

    // Reuse existing JWT secret on reload, generate only on first call
    let jwt_secret: Vec<u8> = jwt_secret.unwrap_or_else(|| {
        use rand::RngCore;
        let mut buf = vec![0u8; 32];
        rand::rng().fill_bytes(&mut buf);
        buf
    });

    let jwt_secret_clone = jwt_secret.clone();
    let advertised_ip = config
        .bind
        .http
        .as_ref()
        .map(|h| h.advertised_ip.clone())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let state = Arc::new(AdminApiState {
        service,
        config: config.control.admin_ui.clone(),
        jwt_secret,
        advertised_ip,
        metrics_store,
        realm_writes_enabled: !config.superv_managed(),
    });

    // Build MFR router and nest under /admin/api/mfr
    let mfr_router = {
        use actrix_mfr::{
            MfrManager,
            handlers::{MfrState, create_admin_router},
        };
        let pool = platform::storage::db::get_database().get_pool().clone();
        let domain = config
            .bind
            .http
            .as_ref()
            .map(|h| h.domain_name.clone())
            .unwrap_or_else(|| "localhost".to_string());
        let manager = MfrManager::new(pool).with_domain(domain);
        let mfr_state = MfrState {
            manager: Arc::new(manager),
        };
        create_admin_router(mfr_state)
    };

    Ok((
        build_admin_api_router(state, Some(mfr_router)),
        jwt_secret_clone,
    ))
}

async fn build_grpc_api_router(
    config: &ActrixConfig,
    service_collector: ServiceCollector,
    shutdown_tx: broadcast::Sender<()>,
    config_store: Arc<ConfigOverrideStore>,
    include_admin_index_and_alias: bool,
) -> Result<Router> {
    let grpc_cfg = &config.control.grpc_api;
    let shared_secret = Arc::new(
        hex::decode(&grpc_cfg.shared_secret)
            .map_err(|e| anyhow::anyhow!("Invalid control.grpc_api.shared_secret hex: {e}"))?,
    );
    let nonce_storage = Arc::new(
        SqliteNonceStorage::new_async(&config.sqlite_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize nonce storage for control: {e}"))?,
    );

    let mut service = AdminApiService::new(
        grpc_cfg.node_id.clone(),
        grpc_cfg.effective_node_name(),
        config.location_tag.clone(),
        env!("CARGO_PKG_VERSION"),
        service_collector,
    )
    .map_err(|e| anyhow::anyhow!("Failed to create control gRPC service: {e}"))?;

    service = service
        .with_override_store(config_store)
        .with_running_config(config.clone());

    let shutdown_tx_for_handler = shutdown_tx.clone();
    service = service.with_shutdown_handler(move |_graceful, _timeout, reason| {
        let shutdown_tx = shutdown_tx_for_handler.clone();
        async move {
            if let Some(reason) = reason {
                platform::recording::warn!("Control gRPC shutdown requested: {}", reason);
            } else {
                platform::recording::warn!("Control gRPC shutdown requested");
            }
            let _ = shutdown_tx.send(());
            Ok(())
        }
    });

    let authed_service = AuthService::new(
        service,
        grpc_cfg.node_id.clone(),
        shared_secret,
        nonce_storage,
        grpc_cfg.max_clock_skew_secs,
    );
    let node_admin_service = NodeAdminServiceServer::new(authed_service);

    // Primary route for tonic clients:
    // `/admin.v1.NodeAdminService/<Method>`
    //
    // Compatibility alias:
    // `/admin/grpc/admin.v1.NodeAdminService/<Method>`
    let mut router = Router::new().route_service(
        "/admin.v1.NodeAdminService/{*grpc_method}",
        node_admin_service.clone(),
    );

    if include_admin_index_and_alias {
        router = router
            .route("/admin", get(control_grpc_head_index))
            .nest_service("/admin/grpc", node_admin_service);
    }

    Ok(router)
}

async fn control_grpc_head_index() -> Json<serde_json::Value> {
    Json(json!({
        "service": "control",
        "head": "grpc_api",
        "grpc_methods": "/admin.v1.NodeAdminService/<Method>",
        "grpc_compat_mount": "/admin/grpc/admin.v1.NodeAdminService/<Method>"
    }))
}
