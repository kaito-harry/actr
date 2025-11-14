//! HTTP服务模块
//!
//! 管理HTTP相关的服务

mod ais;
mod ks;
mod managed;
mod signaling;

pub use ais::AisService;
pub use ks::KsHttpService;
pub use managed::SupervisorService;
pub use signaling::SignalingService;

/// Prometheus metrics endpoint
async fn metrics_endpoint() -> String {
    actrix_common::metrics::export_metrics()
}
