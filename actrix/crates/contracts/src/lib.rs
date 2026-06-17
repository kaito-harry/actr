//! Actrix Protocol Buffer Definitions
//!
//! This crate contains all protocol buffer definitions for Actrix services.
//!
//! # Modules
//!
//! - [`admin::v1`]: Admin service definitions (ControlService and NodeAdminService)
//! - [`signer::v1`]: Signer service definitions
//!
//! # Usage
//!
//! ## Direct module access
//!
//! ```ignore
//! use actrix_proto::admin::v1::{RegisterNodeRequest, ReportRequest};
//! use actrix_proto::signer::v1::{GenerateKeyRequest, SignerClient};
//! ```
//!
//! ## Convenience re-exports
//!
//! Common types are re-exported at the crate root for convenience:
//!
//! ```ignore
//! use actrix_proto::{
//!     NonceCredential, RealmInfo, ResourceType,
//!     ControlServiceClient, NodeAdminServiceServer,
//! };
//! ```
//!
//! # Design Notes
//!
//! ## Cross-Package References
//!
//! The `signer.v1` package imports `NonceCredential` from `admin.v1` for consistent
//! authentication across all services. When using Signer gRPC types directly, reference
//! the credential type via `actrix_proto::admin::v1::NonceCredential`.

/// Admin service protocol definitions.
///
/// Contains both `ControlService` (Node → Admin) and
/// `NodeAdminService` (Admin → Node) definitions.
pub mod admin {
    pub mod v1 {
        tonic::include_proto!("admin.v1");
    }
}

/// Signer protocol definitions.
///
/// Contains `Signer` service for key generation and management.
pub mod signer {
    pub mod v1 {
        tonic::include_proto!("signer.v1");
    }
}

// ============================================================================
// Re-exports: Common Types (from admin.v1)
// ============================================================================

pub use admin::v1::{
    // Enums
    ConfigType,
    // Shared message types
    Directive,
    DirectiveType,
    // Authentication
    NonceCredential,
    RealmInfo,
    ResourceType,
    ServiceAdvertisement,
    ServiceAdvertisementStatus,
    ServiceStatus,
    SystemMetrics,
};

// ============================================================================
// Re-exports: ControlService (Node calls Admin)
// ============================================================================

pub use admin::v1::{
    // Health check (aliased to avoid collision with signer::v1)
    HealthCheckRequest as ControlHealthCheckRequest,
    HealthCheckResponse as ControlHealthCheckResponse,
    // Registration
    RegisterNodeRequest,
    RegisterNodeResponse,
    // Reporting
    ReportRequest,
    ReportResponse,
    // Client and server
    control_service_client::ControlServiceClient,
    control_service_server::{ControlService, ControlServiceServer},
};

// ============================================================================
// Re-exports: NodeAdminService (Admin calls Node)
// ============================================================================

pub use admin::v1::{
    // Config override management
    ConfigOverrideEntry,
    // Realm management
    CreateRealmRequest,
    CreateRealmResponse,
    DeleteConfigOverrideRequest,
    DeleteConfigOverrideResponse,
    DeleteRealmRequest,
    DeleteRealmResponse,
    // Configuration management
    GetConfigRequest,
    GetConfigResponse,
    // Node control
    GetNodeInfoRequest,
    GetNodeInfoResponse,
    GetRealmRequest,
    GetRealmResponse,
    ListConfigOverridesRequest,
    ListConfigOverridesResponse,
    ListRealmsRequest,
    ListRealmsResponse,
    ManagedRealmState,
    SetConfigOverrideRequest,
    SetConfigOverrideResponse,
    ShutdownRequest,
    ShutdownResponse,
    UpdateConfigRequest,
    UpdateConfigResponse,
    UpdateRealmRequest,
    UpdateRealmResponse,
    // Client and server
    node_admin_service_client::NodeAdminServiceClient,
    node_admin_service_server::{NodeAdminService, NodeAdminServiceServer},
};

// ============================================================================
// Re-exports: Signer Service
// ============================================================================

pub use signer::v1::{
    // Health check (aliased to avoid collision with admin::v1)
    HealthCheckRequest as SignerHealthCheckRequest,
    HealthCheckResponse as SignerHealthCheckResponse,
    // Client and server
    signer_client::SignerClient,
    signer_server::{Signer, SignerServer},
};

// Note: Signer proto message types (GenerateKeyRequest, etc.) are NOT re-exported
// here because the signer crate defines its own native Rust types with the same
// names for HTTP/JSON API usage. For gRPC usage, access them via:
//   use actrix_proto::signer::v1::{GenerateKeyRequest, ...};
