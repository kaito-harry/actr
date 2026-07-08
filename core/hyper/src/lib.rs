//! # actr-hyper
//!
//! Hyper — Actor platform layer + runtime infrastructure
//!
//! ## Positioning
//!
//! Hyper is the operating system for Actors: it defines boundaries (Sandbox), provides platform
//! primitives, and carries the full runtime infrastructure (transport, routing, lifecycle management).
//!
//! An Actor cannot open a database on its own, cannot hold its own private key, and cannot claim
//! to be a certain type — everything must go through Hyper's controlled interfaces.
//!
//! ## Responsibilities
//!
//! ### Platform Layer (formerly Hyper)
//!
//! - Package signature verification (binary_hash + MFR signature)
//! - Actor bootstrap (registers with AIS on behalf of the Actor, obtains credential)
//! - Storage namespace isolation (independent SQLite space per Actor)
//! - Cryptographic primitives (Ed25519 sign/verify, Actor does not hold raw private keys)
//! - Runtime lifecycle management (ActrNode lifecycle for Executor execution bodies)
//!
//! ### Runtime Infrastructure (formerly actr-runtime)
//!
//! - **Actor Lifecycle**: system init, node start/stop (ActrNode / ActrRef)
//! - **Message Transport**: layered architecture (Wire -> Transport -> Gate -> Dispatch)
//! - **Communication Modes**: in-process (zero-copy) and cross-process (WebRTC / WebSocket)
//! - **Message Persistence**: SQLite-backed Mailbox (ACID guarantees)
//! - **Observability**: logging, distributed tracing (OpenTelemetry, optional feature)
//! - **WASM Engine**: WASM actor execution (optional feature)
//!
//! ## Architecture Layers
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │  Platform (Hyper)                                   │  AIS Bootstrap
//! │  Sandbox / Verify / Storage / KeyCache              │  Package Verify
//! ├─────────────────────────────────────────────────────┤
//! │  Lifecycle Management (ActrNode → ActrRef)
//! ├─────────────────────────────────────────────────────┤
//! │  Layer 3: Inbound Dispatch                          │  DataStreamRegistry
//! │           (Fast Path Routing)                       │  MediaFrameRegistry
//! ├─────────────────────────────────────────────────────┤
//! │  Layer 2: Outbound Adapters (internal)             │  HostGate
//! │           (Message Sending)                         │  PeerGate
//! ├─────────────────────────────────────────────────────┤
//! │  Layer 1: Transport                                 │  Lane (core abstraction)
//! │           (Channel Management)                      │  HostTransport
//! │                                                     │  PeerTransport
//! ├─────────────────────────────────────────────────────┤
//! │  Layer 0: Wire                                      │  WebRtcGate
//! │           (Physical Connections)                     │  WebRtcCoordinator
//! │                                                     │  SignalingClient
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! ## Non-Goals
//!
//! Hyper does not understand business logic, does not perform business-level message routing,
//! and is unaware of business relationships between Actors.
//! The `hyper_send`/`hyper_recv` provided in WASM mode are network I/O primitives;
//! routing decisions are made by the ActrNode running inside the WASM.

// ═══════════════════════════════════════════════════════════════════════════════
// Platform modules (cross-platform)
// ═══════════════════════════════════════════════════════════════════════════════

pub mod config;
pub mod error;

// Runtime error re-exports (from actr_protocol, distinct from HyperError)
pub mod runtime_error;

// Verify module: TrustProvider trait + built-in verifiers (native-only).
// The verified manifest / package types live in `actr_pack` and are
// re-exported below for downstream consumers.
pub mod verify;

// ═══════════════════════════════════════════════════════════════════════════════
// Native-only modules (excluded on wasm32)
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(not(target_arch = "wasm32"))]
pub mod actr_ref;
#[cfg(not(target_arch = "wasm32"))]
pub mod ais_client;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod key_cache;
#[cfg(not(target_arch = "wasm32"))]
pub mod storage;

// Runtime infrastructure modules (native-only)
#[cfg(all(not(target_arch = "wasm32"), feature = "test-utils"))]
pub mod inbound;
#[cfg(all(not(target_arch = "wasm32"), not(feature = "test-utils")))]
pub(crate) mod inbound;
#[cfg(not(target_arch = "wasm32"))]
pub mod lifecycle;
#[cfg(all(not(target_arch = "wasm32"), feature = "test-utils"))]
pub mod outbound;
#[cfg(all(not(target_arch = "wasm32"), not(feature = "test-utils")))]
pub(crate) mod outbound;
#[cfg(not(target_arch = "wasm32"))]
pub mod transport;
#[cfg(not(target_arch = "wasm32"))]
pub mod wire;

// Shared helpers for integration tests (native-only)
#[cfg(all(not(target_arch = "wasm32"), feature = "test-utils"))]
pub mod test_support;

// Context (native-only, depends on transport/wire)
#[cfg(not(target_arch = "wasm32"))]
pub mod context;

// Runtime workload abstraction (native-only, WASM/dynclib host)
#[cfg(not(target_arch = "wasm32"))]
pub mod workload;

// Per-actor serial command runner: replaces the node-global
// `Arc<Mutex<Workload>>` with a command channel feeding one owning task.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod executor;

// ServiceSpec derivation from a verified package (native-only; pulls
// actr-service-compat/proto-fingerprint).
#[cfg(not(target_arch = "wasm32"))]
mod service_spec;

// WASM actor execution engine (optional, native-only)
#[cfg(all(not(target_arch = "wasm32"), feature = "wasm-engine"))]
pub mod wasm;

// Dynclib actor execution engine (optional, native-only)
#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
pub mod dynclib;

// Observability is public so bindings can bootstrap tracing. Monitoring
// and resource management are reserved scaffolding; they stay crate-private.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod monitoring;
#[cfg(not(target_arch = "wasm32"))]
pub mod observability;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod resource;

// ═══════════════════════════════════════════════════════════════════════════════
// Re-exports: Cross-platform
// ═══════════════════════════════════════════════════════════════════════════════

pub use actr_pack::{PackageManifest, VerifiedPackage};
pub use config::HyperConfig;
pub use error::HyperError;
pub(crate) use error::HyperResult;

// Core protocol types
pub use actr_protocol::{Acl, ActrId, ActrType, ServiceSpec};

// Re-export MediaSample and MediaType from framework (dependency inversion)
pub use actr_framework::{MediaSample, MediaType};

// Runtime error types (distinct from HyperError — these are actor-facing errors)
pub use runtime_error::{ActorResult, ActrError, Classify, ErrorKind};

// Platform traits re-exports
pub use actr_platform_traits::{CryptoProvider, KvStore, PlatformError, PlatformProvider};

// ═══════════════════════════════════════════════════════════════════════════════
// Re-exports: Native-only
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(not(target_arch = "wasm32"))]
pub use ais_client::AisClient;
#[cfg(not(target_arch = "wasm32"))]
pub use storage::ActorStore;
#[cfg(not(target_arch = "wasm32"))]
pub use verify::{ChainTrust, MfrCertCache, RegistryTrust, StaticTrust, TrustProvider};

// Observability
#[cfg(not(target_arch = "wasm32"))]
pub use observability::{ObservabilityGuard, init_observability};

#[cfg(not(target_arch = "wasm32"))]
pub use actr_ref::ActrRef;
// Runtime core structures
#[cfg(not(target_arch = "wasm32"))]
pub use lifecycle::{CredentialState, NetworkEventHandle};

// Layer 1: Transport layer
#[cfg(all(not(target_arch = "wasm32"), feature = "test-utils"))]
pub use transport::{
    ConnType, DataLane, DefaultWireBuilder, DefaultWireBuilderConfig, HostTransport, PeerTransport,
    WireBuilder, WireHandle,
};
#[cfg(not(target_arch = "wasm32"))]
pub use transport::{Dest, ExponentialBackoff, NetworkError, NetworkResult};

// Layer 0: Wire layer
#[cfg(not(target_arch = "wasm32"))]
pub use wire::{
    AuthConfig, AuthType, DisconnectReason, ReconnectConfig, SignalingClient, SignalingConfig,
    SignalingEvent, SignalingStats, WebRtcConfig,
};
#[cfg(all(not(target_arch = "wasm32"), feature = "test-utils"))]
pub use wire::{WebRtcCoordinator, WebSocketSignalingClient};

// Mailbox (from actr-runtime-mailbox crate)
#[cfg(not(target_arch = "wasm32"))]
pub use actr_runtime_mailbox::{
    Mailbox, MailboxStats, MessagePriority, MessageRecord, MessageStatus,
};

// Bootstrap context builder (lifecycle hooks + ActrRef app-side context) is
// crate-internal; consumers go through the Node / ActrRef lifecycle.

// Runtime workload abstraction
#[cfg(not(target_arch = "wasm32"))]
pub use workload::{HostAbiFn, HostOperation, HostOperationResult, InvocationContext};

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

pub(crate) const INITIAL_CONNECTION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);

// ═══════════════════════════════════════════════════════════════════════════════
// Prelude
// ═══════════════════════════════════════════════════════════════════════════════

pub mod prelude {
    //! Convenience prelude module
    //!
    //! Re-exports commonly used types and traits for quick imports:
    //!
    //! ```rust
    //! use actr_hyper::prelude::*;
    //! ```

    // ── Platform types (cross-platform) ─────────────────────────────────────
    pub use crate::verify::{ChainTrust, RegistryTrust, StaticTrust, TrustProvider};
    #[cfg(not(target_arch = "wasm32"))]
    pub use crate::{Attached, Hyper, Init, Node, Registered, storage::ActorStore};
    pub use crate::{HyperConfig, HyperError};
    pub use actr_pack::{PackageManifest, VerifiedPackage};

    // ── Core structures (native-only) ───────────────────────────────────────
    #[cfg(not(target_arch = "wasm32"))]
    pub use crate::actr_ref::ActrRef;

    // Re-export MediaSample and MediaType from framework (dependency inversion)
    pub use actr_framework::{MediaSample, MediaType};

    // ── Layer 0: Wire / WebRTC (native-only) ────────────────────────────────
    #[cfg(not(target_arch = "wasm32"))]
    pub use crate::wire::webrtc::{
        AuthConfig, AuthType, DisconnectReason, ReconnectConfig, SignalingClient, SignalingConfig,
        SignalingEvent, SignalingStats, WebRtcConfig,
    };
    #[cfg(feature = "test-utils")]
    pub use crate::wire::webrtc::{WebRtcCoordinator, WebSocketSignalingClient};

    // ── Mailbox (native-only) ───────────────────────────────────────────────
    #[cfg(not(target_arch = "wasm32"))]
    pub use actr_runtime_mailbox::{
        Mailbox, MailboxStats, MessagePriority, MessageRecord, MessageStatus,
    };

    // ── Layer 1: Transport (native-only) ────────────────────────────────────
    #[cfg(feature = "test-utils")]
    pub use crate::transport::{
        ConnType, DataLane, DefaultWireBuilder, DefaultWireBuilderConfig, HostTransport,
        PeerTransport, WireBuilder, WireHandle,
    };
    #[cfg(not(target_arch = "wasm32"))]
    pub use crate::transport::{Dest, NetworkError, NetworkResult};

    // ── Error types ─────────────────────────────────────────────────────────
    pub use crate::runtime_error::{ActorResult, ActrError};

    // ── Base types ──────────────────────────────────────────────────────────
    pub use actr_protocol::ActrId;

    // ── Framework traits (for implementing Workload) ────────────────────────
    pub use actr_framework::{Context, Workload};

    // ── Async trait support ─────────────────────────────────────────────────
    pub use async_trait::async_trait;

    // ── Common utilities ────────────────────────────────────────────────────
    pub use anyhow::{Context as AnyhowContext, Result as AnyhowResult};
    pub use chrono::{DateTime, Utc};
    pub use uuid::Uuid;

    // ── Tokio runtime primitives ────────────────────────────────────────────
    pub use tokio::sync::{Mutex, RwLock, broadcast, mpsc, oneshot};
    #[cfg(not(target_arch = "wasm32"))]
    pub use tokio::time::{Duration, Instant, sleep, timeout};

    // ── Logging ─────────────────────────────────────────────────────────────
    pub use tracing::{debug, error, info, trace, warn};
}

// ═══════════════════════════════════════════════════════════════════════════════
// Hyper runtime instance (platform singleton) — native-only
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
use std::io::Write;
#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
use std::path::Path;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use std::str::FromStr;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use prost::Message;
#[cfg(not(target_arch = "wasm32"))]
use tracing::{debug, error, info, warn};
#[cfg(not(target_arch = "wasm32"))]
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
use actr_platform_traits::KvOp;
#[cfg(not(target_arch = "wasm32"))]
use actr_protocol::{Realm, RegisterAuthMode, RegisterRequest, register_response};

#[cfg(not(target_arch = "wasm32"))]
/// Compile-time state marker: a [`Node`] has been born from a [`Hyper`]
/// plus a [`actr_config::RuntimeConfig`], but no attachment path has been
/// chosen yet. Transition to [`Attached`] via [`Node::attach`] or
/// [`Node::link`].
pub struct Init;
#[cfg(not(target_arch = "wasm32"))]
/// Compile-time state marker: a package has been verified and attached; AIS credential still pending.
pub struct Attached;
#[cfg(not(target_arch = "wasm32"))]
/// Compile-time state marker: AIS credential has been obtained and injected; ready to start.
pub struct Registered;

#[cfg(not(target_arch = "wasm32"))]
mod node_state_sealed {
    pub trait Sealed {}
    impl Sealed for super::Init {}
    impl Sealed for super::Attached {}
    impl Sealed for super::Registered {}
}

#[cfg(not(target_arch = "wasm32"))]
/// Sealed trait describing valid [`Node`] lifecycle states.
pub trait NodeState: node_state_sealed::Sealed {}
#[cfg(not(target_arch = "wasm32"))]
impl NodeState for Init {}
#[cfg(not(target_arch = "wasm32"))]
impl NodeState for Attached {}
#[cfg(not(target_arch = "wasm32"))]
impl NodeState for Registered {}

#[cfg(not(target_arch = "wasm32"))]
/// Hyper — pre-workload framework infrastructure.
///
/// `Hyper` is the operating system that runs an Actor: it owns configuration,
/// instance identity, trust material, and the package verifier. It is
/// deliberately generic-free and has no knowledge of a specific workload.
///
/// User code constructs Hyper only in the escape-hatch path
/// ([`Node::from_hyper`]); prefer [`Node::from_config_file`] for the common
/// case where config lives in `actr.toml`. The full typestate chain is:
///
/// ```text
/// Node::from_config_file(path)    -> Node<Init>              (framework only)
/// Node::from_hyper(hyper, config) -> Node<Init>              (escape hatch)
///     .attach(package)            -> Node<Attached>          (attach: wasm / dyn lib)
///     .link(workload)             -> Node<Attached>          (link: static lib)
///     .register(ais_endpoint)     -> Node<Registered>        (credential obtained)
///     .start()                    -> ActrRef                 (running node)
/// ```
///
/// Once you call `attach`, you no longer have a `Hyper`: you have a `Node`,
/// which is "Hyper wired to a workload". `register` and `start` live on
/// `Node`, not on `Hyper`.
pub struct Hyper {
    inner: Arc<HyperInner>,
}

#[cfg(not(target_arch = "wasm32"))]
struct HyperInner {
    config: HyperConfig,
    /// Locally unique ID generated and persisted on first startup
    instance_id: String,
    /// Optional platform provider for cross-platform abstraction
    platform: Option<Arc<dyn PlatformProvider>>,
}

#[cfg(not(target_arch = "wasm32"))]
/// Carries state-dependent data for an attached [`Node`].
struct Attachment {
    node: crate::lifecycle::node::Inner,
    /// Verified package retained for AIS bootstrap: the manifest plus the raw
    /// manifest bytes and signature that AIS may need to re-verify upstream.
    ///
    /// `None` for linked attachments, which have no verified package
    /// metadata attached. In that case `Node::register*` falls back to the
    /// runtime config's actor metadata instead of package-derived
    /// registration inputs.
    verified: Option<VerifiedPackage>,
    package_bytes: bytes::Bytes,
}

#[cfg(not(target_arch = "wasm32"))]
/// Node — Hyper wired to a runtime configuration (and optionally a workload).
///
/// A `Node<Init>` is produced by [`Node::from_config_file`] or
/// [`Node::from_hyper`]; it carries `Hyper` + [`actr_config::RuntimeConfig`]
/// but has not yet been attached. Call one of the attach methods to progress
/// into `Node<Attached>`, then `register().start()` into a running
/// [`ActrRef`]:
///
/// ```text
/// Node::from_config_file(path) -> Node<Init>
///     .attach(package)         -> Node<Attached>   (attach: wasm / dyn lib)
///     .link(workload)          -> Node<Attached>   (link: static lib)
///
/// Node<Attached>.register(ais) -> Node<Registered>
/// Node<Registered>.start()     -> ActrRef
/// ```
///
/// The default type parameter `Attached` means writing `Node` unqualified
/// refers to the attached state; `start()` only exists on `Node<Registered>`.
pub struct Node<S: NodeState = Attached> {
    hyper: Arc<HyperInner>,
    /// Present on `Node<Attached>` and `Node<Registered>`; `None` on
    /// `Node<Init>`, which holds `pending_runtime_config` instead.
    attachment: Option<Attachment>,
    /// Pending runtime configuration for `Node<Init>`; consumed by attach
    /// methods. `None` on `Attached` / `Registered`.
    pending_runtime_config: Option<actr_config::RuntimeConfig>,
    _state: std::marker::PhantomData<S>,
}

#[cfg(not(target_arch = "wasm32"))]
/// Execution backend selected from a verified `.actr` package target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryKind {
    /// Execute the package binary with the WASM runtime.
    Wasm,
    /// Execute the package binary as a C-ABI dynamic library (`cdylib`).
    DynClib,
}

#[cfg(not(target_arch = "wasm32"))]
/// Public `.actr` package input object consumed by Hyper.
#[derive(Debug, Clone)]
pub struct WorkloadPackage {
    bytes: bytes::Bytes,
}

#[cfg(not(target_arch = "wasm32"))]
impl WorkloadPackage {
    /// Wrap already-loaded package bytes.
    pub fn new(bytes: impl Into<bytes::Bytes>) -> Self {
        Self {
            bytes: bytes.into(),
        }
    }

    /// Load a `.actr` package from the filesystem in one call.
    pub fn from_path(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Ok(Self {
            bytes: bytes.into(),
        })
    }

    /// Raw `.actr` bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Parse and return the package manifest (unverified).
    ///
    /// This reads the manifest TOML embedded in the `.actr` ZIP without checking
    /// the signature. Use [`Hyper::verify_package`] to obtain a verified manifest.
    /// Re-parses on every call — cache externally if you need it hot.
    pub fn manifest(&self) -> HyperResult<actr_pack::PackageManifest> {
        actr_pack::read_manifest(&self.bytes)
            .map_err(|e| HyperError::InvalidManifest(e.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManufacturerRegistrationAuth {
    pub signature: Vec<u8>,
    pub signed_at: u64,
    pub nonce: Vec<u8>,
}

/// Re-signing capability for the manufacturer proof used in package registration.
///
/// Unpublished package registration (AIS Path 2) requires a manufacturer proof —
/// an Ed25519 signature over the `ACTR-MANUFACTURER-REGISTER-V1` payload that AIS
/// verifies with the MFR key that signed the package manifest. Its nonce is
/// consumed once to prevent replay. Because the nonce is single-use, the proof
/// **cannot be reused**: hard rebind must mint a fresh `signed_at` + `nonce` +
/// signature.
///
/// Rather than caching the signing key in memory, this trait is implemented by
/// the CLI with a type that reloads the MFR private key from the keychain on
/// every [`Self::sign`] call. AIS bootstrap calls it once for the initial
/// registration; the credential manager calls it again on each hard rebind.
pub trait ManufacturerAuthProvider: Send + Sync {
    /// Produce a fresh manufacturer proof for the given registration inputs.
    ///
    /// `manifest_raw` is the exact package manifest bound into the proof
    /// (via its SHA-256). Implementations must generate a new `signed_at` and
    /// random `nonce` each call.
    fn sign(
        &self,
        realm_id: u32,
        actr_type: &actr_protocol::ActrType,
        target: &str,
        manifest_raw: &[u8],
    ) -> Result<ManufacturerRegistrationAuth, HyperError>;
}

#[cfg(not(target_arch = "wasm32"))]
async fn sign_manufacturer_proof(
    provider: Arc<dyn ManufacturerAuthProvider>,
    realm_id: u32,
    actr_type: ActrType,
    target: String,
    manifest_raw: Vec<u8>,
) -> HyperResult<ManufacturerRegistrationAuth> {
    tokio::task::spawn_blocking(move || provider.sign(realm_id, &actr_type, &target, &manifest_raw))
        .await
        .map_err(|err| HyperError::Runtime(format!("manufacturer signing task failed: {err}")))?
}

#[cfg(not(target_arch = "wasm32"))]
fn manufacturer_auth_fields(
    auth: Option<&ManufacturerRegistrationAuth>,
) -> (Option<bytes::Bytes>, Option<u64>, Option<bytes::Bytes>) {
    match auth {
        Some(auth) => (
            Some(bytes::Bytes::from(auth.signature.clone())),
            Some(auth.signed_at),
            Some(bytes::Bytes::from(auth.nonce.clone())),
        ),
        None => (None, None, None),
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ManufacturerRegistrationAuth {
    pub fn sign(
        signing_key: &ed25519_dalek::SigningKey,
        realm_id: u32,
        actr_type: &actr_protocol::ActrType,
        target: &str,
        manifest_raw: &[u8],
    ) -> HyperResult<Self> {
        use ed25519_dalek::Signer as _;
        use rand::RngCore as _;
        use sha2::{Digest as _, Sha256};
        use std::time::{SystemTime, UNIX_EPOCH};

        let signed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| HyperError::Runtime(format!("system clock before Unix epoch: {e}")))?
            .as_secs();

        let mut nonce = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut nonce);

        let manifest_sha256 = Sha256::digest(manifest_raw);
        let manifest_sha256_hex = hex::encode(manifest_sha256);
        let payload = actr_protocol::build_manufacturer_register_payload(
            actr_protocol::ManufacturerRegisterPayload {
                realm_id,
                actr_type,
                target,
                manifest_sha256_hex: &manifest_sha256_hex,
                manufacturer_auth_signed_at: signed_at,
                manufacturer_auth_nonce: &nonce,
            },
        );
        let signature = signing_key.sign(payload.as_bytes()).to_bytes().to_vec();

        Ok(Self {
            signature,
            signed_at,
            nonce,
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Result of verifying a package and preparing a runtime workload from it.
pub(crate) struct LoadedWorkload {
    /// Verified package retained for downstream bootstrap and storage
    /// operations — carries the parsed manifest plus the raw manifest bytes
    /// and signature needed for transparent forwarding to AIS.
    pub verified: VerifiedPackage,
    /// Binary kind detected from `verified.manifest.binary.target`.
    pub binary_kind: BinaryKind,
    /// Ready-to-attach runtime workload.
    pub workload: crate::workload::Workload,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for LoadedWorkload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedWorkload")
            .field("manifest", &self.verified.manifest)
            .field("backend", &self.binary_kind)
            .finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Hyper {
    /// Construct a Hyper with native defaults (uses `tokio::fs` / `ActorStore`).
    ///
    /// - Parse configuration
    /// - Load or generate instance_id (persisted to data_dir)
    /// - Initialize package verifier
    pub async fn new(config: HyperConfig) -> HyperResult<Self> {
        Self::init_inner(config, None).await
    }

    /// Construct a Hyper with an injected platform provider (cross-platform / embedded).
    ///
    /// When a `PlatformProvider` is injected:
    /// - instance UID comes from `platform.instance_uid()` (and its backing store)
    /// - `bootstrap_credential` uses `platform.secret_store()` instead of `ActorStore::open()`
    /// - `TrustProvider` verifies `.actr` package signatures using whatever
    ///   mechanism the injected provider implements
    pub async fn with_platform(
        config: HyperConfig,
        platform: Arc<dyn PlatformProvider>,
    ) -> HyperResult<Self> {
        Self::init_inner(config, Some(platform)).await
    }

    async fn init_inner(
        config: HyperConfig,
        platform: Option<Arc<dyn PlatformProvider>>,
    ) -> HyperResult<Self> {
        info!(
            data_dir = %config.data_dir.display(),
            "Hyper initializing"
        );

        // Resolve an instance UID + ensure data_dir exists. When a platform
        // provider is injected, delegate to it; otherwise fall back to direct
        // tokio::fs calls so this crate stays free of an actr-platform-native
        // dependency (which would be circular).
        let instance_id = if let Some(ref p) = platform {
            p.instance_uid()
                .await
                .map_err(|e| HyperError::Storage(format!("failed to load instance_uid: {e}")))?
        } else {
            tokio::fs::create_dir_all(&config.data_dir)
                .await
                .map_err(|e| {
                    HyperError::Config(format!(
                        "failed to create data_dir `{}`: {e}",
                        config.data_dir.display()
                    ))
                })?;
            load_or_create_instance_uid_local(&config.data_dir).await?
        };
        debug!(instance_id, "Hyper instance_uid ready");

        Ok(Self {
            inner: Arc::new(HyperInner {
                config,
                instance_id,
                platform,
            }),
        })
    }

    /// Verify a [`WorkloadPackage`] and return the verified package bundle
    /// (parsed manifest + raw manifest bytes + signature).
    ///
    /// Delegates entirely to the configured [`crate::verify::TrustProvider`];
    /// the provider decides how to authenticate the package (static key,
    /// registry lookup, keyless transparency log, etc).
    pub async fn verify_package(&self, package: &WorkloadPackage) -> HyperResult<VerifiedPackage> {
        self.inner
            .config
            .trust_provider
            .verify_package(package.bytes())
            .await
    }

    /// Verify a package, select the execution backend from `binary.target`,
    /// and prepare a runtime workload from it.
    ///
    /// Internal helper used by attachment flow and test-support shims.
    #[cfg(feature = "test-utils")]
    pub(crate) async fn load_workload_package(
        &self,
        package: &WorkloadPackage,
    ) -> HyperResult<LoadedWorkload> {
        load_workload_package_inner(&self.inner, package).await
    }
}

// ── Node entry methods (unparameterized) ─────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
impl Node {
    /// Load `actr.toml` from disk, build the underlying [`Hyper`] from the
    /// `[hyper]` section (or an explicit `[[trust]]` / `[hyper.trust]`
    /// anchor set), and return a [`Node<Init>`] ready to attach a workload.
    ///
    /// The caller is expected to drive the typestate chain themselves:
    ///
    /// ```ignore
    /// let actr_ref = Node::from_config_file("actr.toml").await?
    ///     .attach(&package).await?
    ///     .register(&ais_endpoint).await?
    ///     .start().await?;
    /// ```
    ///
    /// For a one-shot sugar covering the entire chain see
    /// [`Node::run_from_config`].
    pub async fn from_config_file(path: impl AsRef<std::path::Path>) -> HyperResult<Node<Init>> {
        config::node_from_config_file(path.as_ref()).await
    }

    /// Like [`Node::from_config_file`] but lets the caller pin the
    /// node's identity by supplying the [`actr_config::PackageInfo`]
    /// derived from a sibling `manifest.toml`.
    ///
    /// This is the path the language bindings use: they already parsed
    /// `manifest.toml` to find the runtime config dir and want their
    /// `[package]` honoured rather than collapsed to the
    /// `local:Client:0.0.0` placeholder that `from_config_file` synthesises.
    pub async fn from_config_with_package(
        path: impl AsRef<std::path::Path>,
        package_info: actr_config::PackageInfo,
    ) -> HyperResult<Node<Init>> {
        config::node_from_config_file_with_package(path.as_ref(), Some(package_info)).await
    }

    /// Escape-hatch constructor: wrap an already-built [`Hyper`] plus a
    /// pre-loaded [`actr_config::RuntimeConfig`] into a [`Node<Init>`].
    ///
    /// Use this when you need direct control over `HyperConfig`
    /// construction (custom trust chain, injected platform provider, etc.)
    /// and cannot drive the whole flow through
    /// [`Node::from_config_file`].
    pub fn from_hyper(hyper: Hyper, runtime_config: actr_config::RuntimeConfig) -> Node<Init> {
        Node {
            hyper: hyper.inner,
            attachment: None,
            pending_runtime_config: Some(runtime_config),
            _state: std::marker::PhantomData,
        }
    }

    /// One-shot sugar: `from_config_file(path).attach(package).register().start()`.
    ///
    /// Loads the runtime configuration from `path`, attaches the given
    /// workload package, registers with AIS at the `[ais_endpoint]` URL
    /// from the config, and starts the node, returning a live
    /// [`ActrRef`]. Use the typestate chain directly when you need to
    /// interleave `create_network_event_handle` or swap in a custom
    /// `service_spec` via `register_with`.
    pub async fn run_from_config(
        path: impl AsRef<std::path::Path>,
        package: &WorkloadPackage,
    ) -> HyperResult<ActrRef> {
        let init = Self::from_config_file(path).await?;
        let ais_endpoint = init
            .pending_runtime_config
            .as_ref()
            .map(|c| c.ais_endpoint.clone())
            .expect("Node<Init> without pending runtime config");
        let attached = init.attach(package).await?;
        let registered = attached.register(&ais_endpoint).await?;
        registered
            .start()
            .await
            .map_err(|e| HyperError::Runtime(format!("failed to start node: {e}")))
    }
}

// ── Node<Init> accessors + state transition: Init → Attached ─────────────────

#[cfg(not(target_arch = "wasm32"))]
impl Node<Init> {
    /// Read-only view of the runtime configuration pending attachment.
    /// Useful for callers that need to configure observability /
    /// tracing subscribers from the config before driving `attach`.
    pub fn runtime_config(&self) -> &actr_config::RuntimeConfig {
        self.pending_runtime_config
            .as_ref()
            .expect("Node<Init> without pending runtime config")
    }

    /// Override the runtime actor type before attaching or linking a workload.
    ///
    /// `Node::from_config_file` can synthesize a placeholder actor type when
    /// the runtime config has no package manifest. Linked/static hosts use this
    /// method to provide the concrete actor identity used for AIS registration.
    pub fn with_actor_type(mut self, actor_type: actr_protocol::ActrType) -> Self {
        let runtime_config = self
            .pending_runtime_config
            .as_mut()
            .expect("Node<Init> without pending runtime config");
        runtime_config.package.name = actor_type.name.clone();
        runtime_config.package.actr_type = actor_type;
        self
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Node<Init> {
    /// Bind a verified [`WorkloadPackage`] to this node.
    ///
    /// Equivalent to the former `Hyper::attach` — verifies the package
    /// signature through the configured `TrustProvider`, loads its guest
    /// binary (WASM or dynclib), and advances the node to
    /// `Node<Attached>`.
    /// Attach a packaged workload (`wasm` / `dyn lib`) to this node.
    pub async fn attach(self, package: &WorkloadPackage) -> HyperResult<Node<Attached>> {
        let runtime_config = self
            .pending_runtime_config
            .expect("Node<Init> without pending runtime config");
        let hyper_inner = self.hyper;
        let loaded = load_workload_package_inner(&hyper_inner, package).await?;
        let packaged_lock = actr_pack::read_lock_file(package.bytes())
            .map_err(|e| HyperError::Runtime(e.to_string()))?
            .map(|bytes| {
                let raw = std::str::from_utf8(&bytes).map_err(|e| {
                    HyperError::Runtime(format!("manifest.lock.toml is not valid UTF-8: {e}"))
                })?;
                actr_config::lock::LockFile::from_str(raw).map_err(|e| {
                    HyperError::Runtime(format!("failed to parse manifest.lock.toml: {e}"))
                })
            })
            .transpose()?;
        let mailbox_backpressure_threshold =
            hyper_inner.config.resolved_mailbox_backpressure_threshold();
        let credential_expiry_warning = hyper_inner.config.credential_expiry_warning;
        let mut node_inner = crate::lifecycle::node::Inner::build(
            runtime_config,
            loaded.workload,
            Some(loaded.verified.manifest.clone()),
            packaged_lock,
            mailbox_backpressure_threshold,
            credential_expiry_warning,
        )
        .await
        .map_err(|e| HyperError::Runtime(e.to_string()))?;
        let observer: Arc<dyn crate::lifecycle::hooks::WorkloadHookObserver> =
            Arc::new(crate::workload::PackageHookObserver {
                workload_dispatch: node_inner.workload_dispatch.clone(),
            });
        node_inner.hook_observer = Some(observer);
        Ok(Node {
            hyper: hyper_inner,
            attachment: Some(Attachment {
                node: node_inner,
                verified: Some(loaded.verified),
                package_bytes: package.bytes.clone(),
            }),
            pending_runtime_config: None,
            _state: std::marker::PhantomData,
        })
    }

    /// Bind an internal workload handle for the `link` path to this node.
    ///
    /// No package is loaded; the host process *is* the workload. The
    /// handle provides both observation hooks and an inbound-dispatch
    /// entry point (see [`workload::LinkedWorkloadHandle::dispatch`]).
    ///
    /// Prefer [`Node::link`] when you already have a generic
    /// [`actr_framework::Workload`] implementation — it wraps the
    /// workload in a [`workload::WorkloadAdapter`] automatically.
    pub(crate) async fn link_handle(
        self,
        handle: Arc<dyn workload::LinkedWorkloadHandle>,
    ) -> HyperResult<Node<Attached>> {
        let runtime_config = self
            .pending_runtime_config
            .expect("Node<Init> without pending runtime config");
        let hyper_inner = self.hyper;
        let mailbox_backpressure_threshold =
            hyper_inner.config.resolved_mailbox_backpressure_threshold();
        let credential_expiry_warning = hyper_inner.config.credential_expiry_warning;
        let mut node_inner = crate::lifecycle::node::Inner::build(
            runtime_config,
            crate::workload::Workload::Linked(handle.clone()),
            None,
            None,
            mailbox_backpressure_threshold,
            credential_expiry_warning,
        )
        .await
        .map_err(|e| HyperError::Runtime(e.to_string()))?;
        let observer: Arc<dyn crate::lifecycle::hooks::WorkloadHookObserver> =
            Arc::new(crate::workload::LinkedHandleObserver { handle });
        node_inner.hook_observer = Some(observer);
        Ok(Node {
            hyper: hyper_inner,
            attachment: Some(Attachment {
                node: node_inner,
                verified: None,
                package_bytes: bytes::Bytes::new(),
            }),
            pending_runtime_config: None,
            _state: std::marker::PhantomData,
        })
    }

    /// Link a generic [`actr_framework::Workload`] implementation
    /// (`static lib`) into this node.
    ///
    /// This is the preferred `link` path for Rust hosts: the
    /// workload is wrapped in a [`workload::WorkloadAdapter`] so that its
    /// associated [`actr_framework::MessageDispatcher`] drives inbound
    /// RPC dispatch and its hook methods are bridged into the node's
    /// observer plumbing.
    pub async fn link<W: actr_framework::Workload>(
        self,
        workload: W,
    ) -> HyperResult<Node<Attached>> {
        let handle: Arc<dyn workload::LinkedWorkloadHandle> =
            workload::WorkloadAdapter::new(workload);
        self.link_handle(handle).await
    }
}

// ── State transition: Attached → Registered ──────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
impl Node<Attached> {
    /// Add a host-side observer to an already attached node.
    ///
    /// Package-backed nodes keep their guest hook observer installed; this
    /// method chains the supplied host observer after it so shells such as
    /// mobile bindings can watch signaling/WebRTC readiness without replacing
    /// package-delivered hooks.
    pub fn with_hook_observer<W: actr_framework::Workload>(mut self, observer: W) -> Self {
        let attachment = self
            .attachment
            .as_mut()
            .expect("Node<Attached> without attachment");
        let handle: Arc<dyn workload::LinkedWorkloadHandle> =
            workload::WorkloadAdapter::new(observer);
        let observer: Arc<dyn crate::lifecycle::hooks::WorkloadHookObserver> =
            Arc::new(crate::workload::LinkedHandleObserver { handle });
        attachment.node.hook_observer = crate::lifecycle::hooks::chain_observers(
            attachment.node.hook_observer.take(),
            Some(observer),
        );
        self
    }

    /// Register with AIS, obtain an AId credential, and inject it into this
    /// attached node. Consumes `Node<Attached>` and returns `Node<Registered>`.
    ///
    /// `realm_id`, `acl`, and `realm_secret` come from the attached
    /// [`RuntimeConfig`]; `service_spec` is derived from the package's proto
    /// exports when a package-backed attach was used. Linked attachments
    /// register from the runtime config's actor metadata instead.
    ///
    /// This convenience method does not supply a manufacturer proof. A
    /// package-backed caller can therefore use it only for a published package
    /// accepted through AIS Path 1. Use
    /// [`Self::register_with_manufacturer_auth`] for an unpublished package.
    pub async fn register(self, ais_endpoint: &str) -> HyperResult<Node<Registered>> {
        self.register_with_manufacturer_auth(ais_endpoint, None)
            .await
    }

    /// Register with AIS and optionally attach manufacturer authorization for
    /// unpublished package registrations.
    ///
    /// When `resign` is `Some`, it is invoked once here to mint the initial
    /// manufacturer proof, and is then retained in the registration context so hard
    /// rebind can re-invoke it to mint a fresh proof (the proof's nonce is
    /// single-use on the AIS side). Linked registration ignores it.
    pub async fn register_with_manufacturer_auth(
        self,
        ais_endpoint: &str,
        resign: Option<Arc<dyn ManufacturerAuthProvider>>,
    ) -> HyperResult<Node<Registered>> {
        let attachment = self
            .attachment
            .as_ref()
            .expect("Node<Attached> without attachment");
        let service_spec = if let Some(verified) = attachment.verified.as_ref() {
            crate::service_spec::calculate_service_spec_from_package(
                &attachment.package_bytes,
                &verified.manifest,
            )?
        } else {
            None
        };
        self.register_with_inner(ais_endpoint, service_spec, resign)
            .await
    }

    /// Register with AIS using an explicit `service_spec`.
    ///
    /// This skips package-based `service_spec` derivation for
    /// package-backed attachments. Linked attachments use the supplied
    /// `service_spec` together with the runtime config's actor metadata.
    /// Like [`Self::register`], this convenience method supplies no
    /// manufacturer proof and package-backed callers must use published Path 1.
    pub async fn register_with(
        self,
        ais_endpoint: &str,
        service_spec: Option<ServiceSpec>,
    ) -> HyperResult<Node<Registered>> {
        self.register_with_inner(ais_endpoint, service_spec, None)
            .await
    }

    async fn register_with_inner(
        mut self,
        ais_endpoint: &str,
        service_spec: Option<ServiceSpec>,
        resign: Option<Arc<dyn ManufacturerAuthProvider>>,
    ) -> HyperResult<Node<Registered>> {
        let attachment = self
            .attachment
            .as_mut()
            .expect("Node<Attached> without attachment");
        let realm_id = attachment.node.config.realm.realm_id;
        let acl = attachment.node.config.acl.clone();
        let realm_secret = attachment.node.config.realm_secret.clone();

        // Mint the initial manufacturer proof (if a re-signing provider was supplied)
        // so the same one-shot auth feeds both the saved registration context
        // and the bootstrap request. The provider itself is retained in the
        // context below so hard rebind can re-invoke it for a fresh proof
        // instead of replaying the single-use nonce.
        let manufacturer_auth = match (&resign, attachment.verified.as_ref()) {
            (Some(provider), Some(verified)) => Some(
                sign_manufacturer_proof(
                    Arc::clone(provider),
                    realm_id,
                    ActrType {
                        manufacturer: verified.manifest.manufacturer.clone(),
                        name: verified.manifest.name.clone(),
                        version: verified.manifest.version.clone(),
                    },
                    verified.manifest.binary.target.clone(),
                    verified.manifest_raw.clone(),
                )
                .await?,
            ),
            _ => None,
        };
        let (manufacturer_auth_signature, manufacturer_auth_signed_at, manufacturer_auth_nonce) =
            manufacturer_auth_fields(manufacturer_auth.as_ref());

        let registration_context = if let Some(verified) = attachment.verified.as_ref() {
            let manifest = &verified.manifest;
            Some(
                crate::lifecycle::credential_manager::RegistrationContext::Package {
                    request: RegisterRequest {
                        actr_type: ActrType {
                            manufacturer: manifest.manufacturer.clone(),
                            name: manifest.name.clone(),
                            version: manifest.version.clone(),
                        },
                        realm: Realm { realm_id },
                        service_spec: service_spec.clone(),
                        acl: acl.clone(),
                        service: None,
                        ws_address: None,
                        manifest_raw: Some(verified.manifest_raw.clone().into()),
                        mfr_signature: Some(verified.sig_raw.clone().into()),
                        target: Some(manifest.binary.target.clone()),
                        auth_mode: Some(RegisterAuthMode::Package as i32),
                        manufacturer_auth_signature,
                        manufacturer_auth_signed_at,
                        manufacturer_auth_nonce,
                    },
                    resign,
                },
            )
        } else {
            None
        };

        let register_ok = if let Some(verified) = attachment.verified.as_ref() {
            let verified = verified.clone();
            bootstrap_credential_inner(
                &self.hyper,
                &verified,
                CredentialBootstrapRequest {
                    ais_endpoint,
                    realm_id,
                    service_spec,
                    acl,
                    realm_secret: realm_secret.as_deref(),
                    manufacturer_auth,
                },
            )
            .await?
        } else {
            bootstrap_linked_credential_inner(&attachment.node.config, ais_endpoint, service_spec)
                .await?
        };

        attachment.node.set_preregistered_credential(register_ok);
        if let Some(ctx) = registration_context {
            attachment.node.set_preregistered_registration_context(ctx);
        }

        Ok(Node {
            hyper: self.hyper,
            attachment: self.attachment,
            pending_runtime_config: None,
            _state: std::marker::PhantomData,
        })
    }

    /// Create a network event handle for platform callbacks. Must be called
    /// before [`Node::start`].
    pub fn create_network_event_handle(
        &mut self,
        debounce_ms: u64,
    ) -> crate::lifecycle::NetworkEventHandle {
        self.attachment
            .as_mut()
            .expect("Node<Attached> without attachment")
            .node
            .create_network_event_handle(debounce_ms)
    }

    /// AIS endpoint URL resolved from the attached [`RuntimeConfig`].
    /// Convenience accessor for callers that just drove `from_config_file`
    /// + `attach` and need the endpoint to pass into `register`.
    pub fn ais_endpoint(&self) -> &str {
        &self
            .attachment
            .as_ref()
            .expect("Node<Attached> without attachment")
            .node
            .config
            .ais_endpoint
    }
}

// ── State transition: Registered → ActrRef ───────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
impl Node<Registered> {
    /// Start the attached, registered node and return the live [`ActrRef`].
    pub async fn start(self) -> actr_protocol::ActorResult<crate::actr_ref::ActrRef> {
        let Attachment { node, .. } = self
            .attachment
            .expect("Node<Registered> without attachment");
        node.start().await
    }

    /// Create a network event handle for platform callbacks. Must be called
    /// before [`Node::start`].
    pub fn create_network_event_handle(
        &mut self,
        debounce_ms: u64,
    ) -> crate::lifecycle::NetworkEventHandle {
        self.attachment
            .as_mut()
            .expect("Node<Registered> without attachment")
            .node
            .create_network_event_handle(debounce_ms)
    }
}

// ── Helpers available in all states ──────────────────────────────────────────

// ── Hyper common helpers (framework operations that don't require attachment) ─

#[cfg(not(target_arch = "wasm32"))]
impl Hyper {
    /// Resolve the storage namespace path for a verified manifest.
    ///
    /// The path is fixed here; all subsequent storage operations are isolated based on this path.
    pub fn resolve_storage_path(&self, manifest: &PackageManifest) -> HyperResult<PathBuf> {
        resolve_storage_path_for(&self.inner, manifest)
    }

    /// Bootstrap credential registration with AIS.
    ///
    /// Hyper completes registration bootstrap on behalf of the Actor and returns the full AIS
    /// registration payload.
    ///
    /// ## Registration Logic
    ///
    /// Package bootstrap always authenticates with the MFR-signed manifest.
    /// Credential renewal uses the renewal token returned by AIS through
    /// `POST /ais/renew`, not a follow-up `/register` call.
    ///
    /// This legacy convenience API cannot supply a fresh manufacturer proof,
    /// so it is suitable only for published packages accepted through AIS
    /// Path 1. Unpublished packages must use
    /// [`Node::<Attached>::register_with_manufacturer_auth`].
    ///
    /// ## Parameters
    ///
    /// - `verified`: verified package bundle (from `verify_package`) — carries
    ///   the parsed manifest plus the raw manifest bytes and signature needed
    ///   for phase-1 registration with AIS.
    /// - `ais_endpoint`: AIS HTTP address, e.g. `"http://ais.example.com:8080"`
    /// - `realm_id`: target Realm ID
    /// - `service_spec`: optional protobuf API metadata published to discovery
    /// - `acl`: optional access-control policy attached to the actor
    pub async fn bootstrap_credential(
        &self,
        verified: &VerifiedPackage,
        ais_endpoint: &str,
        realm_id: u32,
        service_spec: Option<ServiceSpec>,
        acl: Option<Acl>,
    ) -> HyperResult<register_response::RegisterOk> {
        bootstrap_credential_inner(
            &self.inner,
            verified,
            CredentialBootstrapRequest {
                ais_endpoint,
                realm_id,
                service_spec,
                acl,
                realm_secret: None,
                manufacturer_auth: None,
            },
        )
        .await
    }

    /// Current instance_id
    pub fn instance_id(&self) -> &str {
        &self.inner.instance_id
    }

    /// Current configuration
    pub fn config(&self) -> &HyperConfig {
        &self.inner.config
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn resolve_storage_path_for(
    inner: &HyperInner,
    manifest: &PackageManifest,
) -> HyperResult<PathBuf> {
    let resolver = config::NamespaceResolver::new(&inner.config, &inner.instance_id)?
        .with_actor_type(&manifest.manufacturer, &manifest.name, &manifest.version);
    resolver.resolve(&inner.config.storage_path_template)
}

/// Free-function counterpart of [`Hyper::load_workload_package`] —
/// shared by both [`Hyper::attach`] and `Node<Init>::attach` without
/// needing a `Hyper` handle to own the call.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn load_workload_package_inner(
    inner: &HyperInner,
    package: &WorkloadPackage,
) -> HyperResult<LoadedWorkload> {
    let bytes = package.bytes();
    let verified = inner.config.trust_provider.verify_package(bytes).await?;
    let binary_kind = detect_binary_kind(&verified.manifest)?;
    let workload = match binary_kind {
        BinaryKind::Wasm => load_wasm_workload_inner(inner, bytes, &verified.manifest).await?,
        BinaryKind::DynClib => load_dynclib_workload_inner(inner, bytes, &verified.manifest)?,
    };
    Ok(LoadedWorkload {
        verified,
        binary_kind,
        workload,
    })
}

#[cfg(not(target_arch = "wasm32"))]
async fn load_wasm_workload_inner(
    _inner: &HyperInner,
    bytes: &[u8],
    manifest: &PackageManifest,
) -> HyperResult<crate::workload::Workload> {
    #[cfg(feature = "wasm-engine")]
    {
        // Refuse legacy core-module packages before attempting to compile
        // them — `Component::from_binary` already rejects them downstream
        // with an opaque "unknown binary format" error, so catching the
        // case here produces a migration-pointing message instead.
        if matches!(
            manifest.binary.resolved_kind(),
            actr_pack::BinaryKind::CoreModule
        ) {
            return Err(HyperError::InvalidManifest(format!(
                "package `{}` uses the legacy core wasm module format, which was retired in Phase 1. \
                 Rebuild with actr 0.2+ (`actr build`, target wasm32-wasip2 + wasm-component-ld 0.5.22+) \
                 to produce a Component Model binary, and set `binary.kind = \"component\"` in manifest.toml.",
                manifest.actr_type_str()
            )));
        }

        let wasm_bytes = actr_pack::load_binary(bytes).map_err(|e| {
            HyperError::Runtime(format!(
                "failed to extract package binary `{}` for target `{}`: {e}",
                manifest.binary.path, manifest.binary.target
            ))
        })?;
        let host = crate::wasm::WasmHost::compile(&wasm_bytes).map_err(|e| {
            HyperError::Runtime(format!(
                "failed to compile WASM package target `{}`: {e}",
                manifest.binary.target
            ))
        })?;
        let mut instance = host.instantiate().await.map_err(|e| {
            HyperError::Runtime(format!(
                "failed to instantiate WASM package target `{}`: {e}",
                manifest.binary.target
            ))
        })?;
        instance
            .init(&actr_framework::guest::dynclib_abi::InitPayloadV1 {
                version: actr_framework::guest::dynclib_abi::version::V1,
                actr_type: manifest.actr_type_str(),
                credential: Vec::new(),
                actor_id: Vec::new(),
                realm_id: 0,
            })
            .map_err(|e| {
                HyperError::Runtime(format!(
                    "failed to initialize WASM package target `{}`: {e}",
                    manifest.binary.target
                ))
            })?;
        Ok(crate::workload::Workload::Wasm(instance))
    }

    #[cfg(not(feature = "wasm-engine"))]
    {
        let _ = (bytes, manifest);
        Err(HyperError::Runtime(
            "package target requires the `wasm-engine` feature, but it is not enabled".to_string(),
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn load_dynclib_workload_inner(
    _inner: &HyperInner,
    bytes: &[u8],
    manifest: &PackageManifest,
) -> HyperResult<crate::workload::Workload> {
    #[cfg(feature = "dynclib-engine")]
    {
        let cache_path = ensure_dynclib_cache_path(&_inner.config.data_dir, bytes, manifest)?;
        let host = load_dynclib_host_with_rebuild(&cache_path, bytes, manifest)?;
        let instance = host
            .instantiate(&actr_framework::guest::dynclib_abi::InitPayloadV1 {
                version: actr_framework::guest::dynclib_abi::version::V1,
                actr_type: manifest.actr_type_str(),
                credential: Vec::new(),
                actor_id: Vec::new(),
                realm_id: 0,
            })
            .map_err(|e| {
                HyperError::Runtime(format!(
                    "failed to initialize dynclib package target `{}`: {e}",
                    manifest.binary.target
                ))
            })?;

        Ok(crate::workload::Workload::DynClib(
            crate::dynclib::DynClibWorkload::new(host, instance),
        ))
    }

    #[cfg(not(feature = "dynclib-engine"))]
    {
        let _ = (bytes, manifest);
        Err(HyperError::Runtime(
            "package target requires the `dynclib-engine` feature, but it is not enabled"
                .to_string(),
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct CredentialBootstrapRequest<'a> {
    ais_endpoint: &'a str,
    realm_id: u32,
    service_spec: Option<ServiceSpec>,
    acl: Option<Acl>,
    realm_secret: Option<&'a str>,
    manufacturer_auth: Option<ManufacturerRegistrationAuth>,
}

#[cfg(not(target_arch = "wasm32"))]
async fn bootstrap_credential_inner(
    inner: &HyperInner,
    verified: &VerifiedPackage,
    request: CredentialBootstrapRequest<'_>,
) -> HyperResult<register_response::RegisterOk> {
    let CredentialBootstrapRequest {
        ais_endpoint,
        realm_id,
        service_spec,
        acl,
        realm_secret,
        manufacturer_auth,
    } = request;
    let manifest = &verified.manifest;
    info!(
        actr_type = manifest.actr_type_str(),
        ais_endpoint, realm_id, "starting credential bootstrap with AIS"
    );

    // 1. Open the Actor's secret store (via platform provider or direct ActorStore)
    let storage_path = resolve_storage_path_for(inner, manifest)?;
    let store: Arc<dyn KvStore> = if let Some(ref platform) = inner.platform {
        let ns = storage_path.to_string_lossy().to_string();
        platform
            .secret_store(&ns)
            .await
            .map_err(|e| HyperError::Storage(format!("failed to open secret store: {e}")))?
    } else {
        Arc::new(ActorStore::open(&storage_path).await?)
    };

    // 2. Build RegisterRequest and send to AIS.
    // Package bootstrap always authenticates with manifest + MFR signature.
    // (Legacy PSK renewal path removed — credential renewal now goes through
    // the Credential Manager via POST /ais/renew.)
    let mut ais = AisClient::new(ais_endpoint);
    if let Some(secret) = realm_secret {
        ais = ais.with_realm_secret(secret);
    }

    let actr_type = ActrType {
        manufacturer: manifest.manufacturer.clone(),
        name: manifest.name.clone(),
        version: manifest.version.clone(),
    };
    let realm = Realm { realm_id };

    info!(
        actr_type = manifest.actr_type_str(),
        "registering with AIS using MFR manifest"
    );

    let (manufacturer_auth_signature, manufacturer_auth_signed_at, manufacturer_auth_nonce) =
        manufacturer_auth_fields(manufacturer_auth.as_ref());

    let req = RegisterRequest {
        actr_type,
        realm,
        service_spec,
        acl,
        service: None,
        ws_address: None,
        manifest_raw: Some(verified.manifest_raw.clone().into()),
        mfr_signature: Some(verified.sig_raw.clone().into()),
        target: Some(manifest.binary.target.clone()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature,
        manufacturer_auth_signed_at,
        manufacturer_auth_nonce,
    };
    let response = ais.register_with_manifest(req).await?;

    // 3. Process AIS response
    let ok = match response.result {
        Some(register_response::Result::Success(ok)) => ok,
        Some(register_response::Result::Error(e)) => {
            error!(
                actr_type = manifest.actr_type_str(),
                error_code = e.code,
                error_message = %e.message,
                "AIS registration returned error"
            );
            return Err(HyperError::AisBootstrapFailed(format!(
                "AIS rejected registration (code={}): {}",
                e.code, e.message
            )));
        }
        None => {
            error!(
                actr_type = manifest.actr_type_str(),
                "AIS response missing result field"
            );
            return Err(HyperError::AisBootstrapFailed(
                "AIS response missing result field".to_string(),
            ));
        }
    };

    // 4. Store signing_pubkey + signing_key_id (for AisKeyCache use)
    let pubkey_bytes = ok.signing_pubkey.to_vec();
    let key_id_bytes = ok.signing_key_id.to_le_bytes().to_vec();
    store
        .batch(vec![
            KvOp::Set {
                key: "hyper:ais:signing_pubkey".to_string(),
                value: pubkey_bytes,
            },
            KvOp::Set {
                key: "hyper:ais:signing_key_id".to_string(),
                value: key_id_bytes,
            },
        ])
        .await
        .map_err(|e| HyperError::Storage(format!("failed to store signing key: {e}")))?;
    debug!(
        actr_type = manifest.actr_type_str(),
        signing_key_id = ok.signing_key_id,
        "AIS signing public key persisted to ActorStore"
    );

    info!(
        actr_type = manifest.actr_type_str(),
        credential_len = ok.credential.encode_to_vec().len(),
        "AIS credential bootstrap succeeded"
    );

    Ok(ok)
}

#[cfg(not(target_arch = "wasm32"))]
async fn bootstrap_linked_credential_inner(
    config: &actr_config::RuntimeConfig,
    ais_endpoint: &str,
    service_spec: Option<ServiceSpec>,
) -> HyperResult<register_response::RegisterOk> {
    let mut ais = AisClient::new(ais_endpoint);
    if let Some(ref secret) = config.realm_secret {
        ais = ais.with_realm_secret(secret.clone());
    }

    let req = build_linked_register_request(config, service_spec);
    let response = ais.register_linked(req).await?;
    match response.result {
        Some(register_response::Result::Success(ok)) => Ok(ok),
        Some(register_response::Result::Error(e)) => Err(HyperError::AisBootstrapFailed(format!(
            "AIS rejected registration (code={}): {}",
            e.code, e.message
        ))),
        None => Err(HyperError::AisBootstrapFailed(
            "AIS response missing result field".to_string(),
        )),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn build_linked_register_request(
    config: &actr_config::RuntimeConfig,
    service_spec: Option<ServiceSpec>,
) -> RegisterRequest {
    let ws_address = if let Some(port) = config.websocket_listen_port {
        let host = config
            .websocket_advertised_host
            .as_deref()
            .unwrap_or("127.0.0.1");
        Some(format!("ws://{}:{}", host, port))
    } else {
        None
    };

    RegisterRequest {
        actr_type: config.actr_type().clone(),
        realm: config.realm,
        service_spec,
        acl: config.acl.clone(),
        service: None,
        ws_address,
        auth_mode: Some(RegisterAuthMode::Linked as i32),
        ..Default::default()
    }
}

// ─── Helper functions (native-only) ──────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(target_arch = "wasm32"))]
fn detect_binary_kind(manifest: &PackageManifest) -> HyperResult<BinaryKind> {
    if manifest.binary.is_wasm_target() {
        return Ok(BinaryKind::Wasm);
    }

    if is_compatible_native_target(&manifest.binary.target) {
        return Ok(BinaryKind::DynClib);
    }

    Err(HyperError::InvalidManifest(format!(
        "unsupported binary target `{}` for host `{}-{}`; expected `wasm32-*` or a native target matching this host",
        manifest.binary.target,
        std::env::consts::ARCH,
        std::env::consts::OS,
    )))
}

/// Check that `target` is a valid Rust target triple compatible with the current host.
///
/// A target triple has at least 3 segments (arch-vendor-os or arch-vendor-os-env).
/// We verify that the arch and OS components match the running host to reject
/// cross-platform cdylib packages early, rather than failing at `dlopen` time.
#[cfg(not(target_arch = "wasm32"))]
fn is_compatible_native_target(target: &str) -> bool {
    let segments: Vec<&str> = target.split('-').filter(|s| !s.is_empty()).collect();
    if segments.len() < 3 {
        return false;
    }

    let target_arch = segments[0];
    // OS is typically the third segment (arch-vendor-os[-env]).
    let target_os = segments[2];

    // Normalize arch names: Rust target triples use different names than std::env::consts::ARCH.
    let arch_matches = match (target_arch, std::env::consts::ARCH) {
        (a, b) if a == b => true,
        ("x86_64", "x86_64") => true,
        ("aarch64", "aarch64") => true,
        _ => false,
    };

    // Normalize OS names: Rust target triples use e.g. "darwin" while consts::OS is "macos".
    let os_matches = match (target_os, std::env::consts::OS) {
        (a, b) if a == b => true,
        ("darwin", "macos") | ("macos", "darwin") => true,
        _ => false,
    };

    arch_matches && os_matches
}

#[cfg(all(
    not(target_arch = "wasm32"),
    feature = "dynclib-engine",
    target_os = "macos"
))]
fn dynclib_tempfile_suffix() -> &'static str {
    ".dylib"
}

#[cfg(all(
    not(target_arch = "wasm32"),
    feature = "dynclib-engine",
    target_os = "linux"
))]
fn dynclib_tempfile_suffix() -> &'static str {
    ".so"
}

#[cfg(all(
    not(target_arch = "wasm32"),
    feature = "dynclib-engine",
    target_os = "windows"
))]
fn dynclib_tempfile_suffix() -> &'static str {
    ".dll"
}

#[cfg(all(
    not(target_arch = "wasm32"),
    feature = "dynclib-engine",
    not(any(target_os = "macos", target_os = "linux", target_os = "windows"))
))]
fn dynclib_tempfile_suffix() -> &'static str {
    ".dynlib"
}

#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
const DYNCLIB_CACHE_DIR: &str = "dynclib-cache";

#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
fn dynclib_cache_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(DYNCLIB_CACHE_DIR)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
fn dynclib_cache_path(data_dir: &Path, binary_hash: &[u8; 32]) -> PathBuf {
    dynclib_cache_dir(data_dir).join(format!(
        "{}{}",
        hex::encode(binary_hash),
        dynclib_tempfile_suffix()
    ))
}

#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
fn extract_dynclib_binary(bytes: &[u8], manifest: &PackageManifest) -> HyperResult<Vec<u8>> {
    actr_pack::load_binary(bytes).map_err(|e| {
        HyperError::Runtime(format!(
            "failed to extract package binary `{}` for target `{}`: {e}",
            manifest.binary.path, manifest.binary.target
        ))
    })
}

#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
fn write_dynclib_cache_file(cache_path: &Path, binary_bytes: &[u8]) -> HyperResult<()> {
    let cache_dir = cache_path.parent().ok_or_else(|| {
        HyperError::Runtime("dynclib cache path has no parent directory".to_string())
    })?;
    std::fs::create_dir_all(cache_dir).map_err(|e| {
        HyperError::Runtime(format!(
            "failed to create dynclib cache directory `{}`: {e}",
            cache_dir.display()
        ))
    })?;

    let mut temp_file = tempfile::Builder::new()
        .prefix("actr-dynclib-")
        .tempfile_in(cache_dir)
        .map_err(|e| {
            HyperError::Runtime(format!(
                "failed to allocate dynclib cache temp file in `{}`: {e}",
                cache_dir.display()
            ))
        })?;

    temp_file.write_all(binary_bytes).map_err(|e| {
        HyperError::Runtime(format!(
            "failed to write dynclib cache temp file `{}`: {e}",
            temp_file.path().display()
        ))
    })?;
    temp_file.flush().map_err(|e| {
        HyperError::Runtime(format!(
            "failed to flush dynclib cache temp file `{}`: {e}",
            temp_file.path().display()
        ))
    })?;

    match temp_file.persist_noclobber(cache_path) {
        Ok(_) => Ok(()),
        Err(err) if err.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(HyperError::Runtime(format!(
            "failed to persist dynclib cache file `{}`: {}",
            cache_path.display(),
            err.error
        ))),
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
fn ensure_dynclib_cache_path(
    data_dir: &Path,
    bytes: &[u8],
    manifest: &PackageManifest,
) -> HyperResult<PathBuf> {
    let binary_hash = manifest
        .binary
        .hash_bytes()
        .map_err(|e| HyperError::InvalidManifest(e.to_string()))?;
    let cache_path = dynclib_cache_path(data_dir, &binary_hash);
    if cache_path.exists() {
        return Ok(cache_path);
    }

    let binary_bytes = extract_dynclib_binary(bytes, manifest)?;
    write_dynclib_cache_file(&cache_path, &binary_bytes)?;
    Ok(cache_path)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
fn rebuild_dynclib_cache_file(
    cache_path: &Path,
    bytes: &[u8],
    manifest: &PackageManifest,
) -> HyperResult<()> {
    match std::fs::remove_file(cache_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(HyperError::Runtime(format!(
                "failed to remove corrupt dynclib cache file `{}`: {err}",
                cache_path.display()
            )));
        }
    }

    let binary_bytes = extract_dynclib_binary(bytes, manifest)?;
    write_dynclib_cache_file(cache_path, &binary_bytes)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "dynclib-engine"))]
fn load_dynclib_host_with_rebuild(
    cache_path: &Path,
    bytes: &[u8],
    manifest: &PackageManifest,
) -> HyperResult<crate::dynclib::DynclibHost> {
    match crate::dynclib::DynclibHost::load(cache_path) {
        Ok(host) => Ok(host),
        Err(first_err) => {
            warn!(
                path = %cache_path.display(),
                target = %manifest.binary.target,
                error = %first_err,
                "cached dynclib load failed, rebuilding cache once"
            );
            rebuild_dynclib_cache_file(cache_path, bytes, manifest)?;
            crate::dynclib::DynclibHost::load(cache_path).map_err(|second_err| {
                HyperError::Runtime(format!(
                    "failed to load dynclib package target `{}` from cache `{}` after rebuild; first load error: {first_err}; second load error: {second_err}",
                    manifest.binary.target,
                    cache_path.display()
                ))
            })
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Load an existing `instance_uid` or generate and persist a new one.
///
/// Used only when no `PlatformProvider` is injected; otherwise the provider's
/// `instance_uid()` is the source of truth.
async fn load_or_create_instance_uid_local(data_dir: &std::path::Path) -> HyperResult<String> {
    let id_file = data_dir.join(".hyper-instance-uid");

    if id_file.exists() {
        let id = tokio::fs::read_to_string(&id_file)
            .await
            .map_err(|e| HyperError::Storage(format!("failed to read instance_uid file: {e}")))?;
        let id = id.trim().to_string();
        if !id.is_empty() {
            return Ok(id);
        }
        warn!("instance_uid file is empty; generating a new one");
    }

    let new_id = Uuid::new_v4().to_string();
    tokio::fs::write(&id_file, &new_id)
        .await
        .map_err(|e| HyperError::Storage(format!("failed to write instance_uid file: {e}")))?;
    info!(instance_uid = %new_id, "generated a new Hyper instance_uid");
    Ok(new_id)
}

#[cfg(all(not(target_arch = "wasm32"), test))]
#[path = "lib_tests.rs"]
mod tests;
