//! Mock actrix server for Actor-RTC integration tests.
//!
//! Provides a drop-in replacement for a real `actrix` instance, covering the
//! endpoints that `actr`'s `Hyper` runtime and the `actr` CLI talk to:
//!
//! - WebSocket signaling at `/signaling/ws` (also `/` for legacy tests)
//!   handles `PeerToSignaling(RegisterRequest)`,
//!   `ActrToSignaling(Ping|RouteCandidatesRequest|Unregister|...)` and
//!   `ActrRelay` forwarding.
//! - HTTP AIS endpoints at `/ais/register` + `/register` (protobuf body).
//! - HTTP MFR registry endpoints at `/ais/mfr/{name}/verifying_key` +
//!   `/mfr/{name}/verifying_key` for package verification.
//! - HTTP publish endpoints at `/mfr/pkg/nonce` + `/mfr/pkg/publish` used by
//!   `actr pkg publish`.
//! - Admin seeding endpoints under `/admin/*` (replaces the old
//!   `register.sh` that used `sqlite3` directly on actrix.db).
//!
//! All state lives in memory and is shared between the WS and HTTP surfaces so
//! an MFR that was registered via `POST /admin/mfr` is immediately visible to
//! `GET /mfr/{name}/verifying_key`, etc.

pub mod http;
pub mod signaling;
pub mod state;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use axum::Router;
use ed25519_dalek::{SigningKey, VerifyingKey};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub use state::{MfrEntry, MockState, PackageEntry, RealmEntry};

/// Mock actrix server with WebSocket signaling and HTTP AIS/MFR endpoints.
///
/// WebSocket and HTTP share a single TCP listener (axum's WS upgrade handler)
/// so callers only need one port.
pub struct MockActrixServer {
    port: u16,
    state: Arc<MockState>,
    cancel: tokio_util::sync::CancellationToken,
    is_running: Arc<AtomicBool>,
    message_count: Arc<AtomicU32>,
    ice_restart_offer_count: Arc<AtomicU32>,
    ice_restart_request_count: Arc<AtomicU32>,
    pause_forwarding: Arc<AtomicBool>,
    ice_candidate_drop_count: Arc<AtomicU32>,
    connection_count: Arc<AtomicU32>,
    disconnection_count: Arc<AtomicU32>,
}

impl MockActrixServer {
    /// Start the mock server on a random available port.
    pub async fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Self::start_with_listener(listener).await
    }

    /// Start the mock server on the specified port.
    pub async fn start_on_port(port: u16) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
        Self::start_with_listener(listener).await
    }

    pub async fn start_with_listener(listener: TcpListener) -> anyhow::Result<Self> {
        let port = listener.local_addr()?.port();

        // Deterministic keys so tests can reproduce signatures.
        let ais_signing_key = SigningKey::from_bytes(&[42u8; 32]);
        let mfr_signing_key = SigningKey::from_bytes(&[43u8; 32]);

        let cancel = tokio_util::sync::CancellationToken::new();
        let state = Arc::new(MockState::new(
            ais_signing_key,
            1,
            mfr_signing_key,
            cancel.clone(),
        ));

        let is_running = Arc::new(AtomicBool::new(true));

        let message_count = state.message_count.clone();
        let ice_restart_offer_count = state.ice_restart_offer_count.clone();
        let ice_restart_request_count = state.ice_restart_request_count.clone();
        let pause_forwarding = state.pause_forwarding.clone();
        let ice_candidate_drop_count = state.ice_candidate_drop_count.clone();
        let connection_count = state.connection_count.clone();
        let disconnection_count = state.disconnection_count.clone();

        let app = build_router(state.clone());

        let cancel_clone = cancel.clone();
        let is_running_clone = is_running.clone();
        let (ready_tx, ready_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            let _ = ready_tx.send(());
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    cancel_clone.cancelled().await;
                })
                .await;
            if let Err(err) = result {
                tracing::error!(%err, "mock-actrix axum server exited with error");
            }
            is_running_clone.store(false, Ordering::Release);
        });

        tokio::time::timeout(std::time::Duration::from_secs(5), ready_rx)
            .await
            .map_err(|_| anyhow::anyhow!("mock-actrix server failed to start on port {port}"))?
            .map_err(|_| anyhow::anyhow!("mock-actrix startup task exited early"))?;

        tracing::info!(port, "mock-actrix listening on 127.0.0.1:{port}");

        Ok(Self {
            port,
            state,
            cancel,
            is_running,
            message_count,
            ice_restart_offer_count,
            ice_restart_request_count,
            pause_forwarding,
            ice_candidate_drop_count,
            connection_count,
            disconnection_count,
        })
    }

    /// HTTP base URL (e.g. `http://127.0.0.1:PORT`). This is what callers pass
    /// to `AisClient::new()` / `MfrCertCache::new()`; the server accepts both
    /// `/register` and `/ais/register` so it also works when callers append
    /// `/ais` to the endpoint.
    pub fn http_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// WebSocket signaling URL (`ws://127.0.0.1:PORT/signaling/ws`).
    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}/signaling/ws", self.port)
    }

    /// Legacy helper used by existing hyper integration tests: returns the base
    /// WS URL without the `/signaling/ws` path. The mock server accepts WS
    /// upgrades on both `/` and `/signaling/ws` so this keeps working.
    pub fn url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }

    /// Return the bound port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Deterministic AIS signing key (`SigningKey::from_bytes(&[42u8; 32])`).
    ///
    /// Used by tests that need to pre-verify credentials issued by the mock.
    pub fn ais_signing_key(&self) -> &SigningKey {
        self.state.ais_signing_key()
    }

    /// Deterministic built-in MFR signing key
    /// (`SigningKey::from_bytes(&[43u8; 32])`).
    ///
    /// This is the key the mock will hand out as the default manufacturer's
    /// verifying key when no MFR is explicitly registered via
    /// [`add_mfr`](Self::add_mfr).
    pub fn mfr_signing_key(&self) -> &SigningKey {
        self.state.builtin_mfr_signing_key()
    }

    /// Register a realm. `name` is advisory; `id` must match `realm_id` in the
    /// caller's config.
    pub async fn add_realm(&self, id: u32, name: &str) {
        self.state.add_realm(id, name.to_string()).await;
    }

    /// Register a manufacturer. The `verifying_key` will be returned by
    /// `GET /mfr/{name}/verifying_key`.
    pub async fn add_mfr(&self, name: &str, verifying_key: VerifyingKey) {
        self.state.add_mfr(name.to_string(), verifying_key).await;
    }

    /// Register a package (manufacturer + name + version + target). Used by
    /// tests that want to pre-seed the registry without going through
    /// `POST /mfr/pkg/publish`.
    pub async fn add_package(&self, manufacturer: &str, name: &str, version: &str, target: &str) {
        self.state
            .add_package(PackageEntry {
                manufacturer: manufacturer.to_string(),
                name: name.to_string(),
                version: version.to_string(),
                target: target.to_string(),
                manifest_raw: String::new(),
                signature_b64: String::new(),
            })
            .await;
    }

    /// Shutdown the server.
    pub async fn shutdown(&mut self) {
        if !self.cancel.is_cancelled() {
            self.cancel.cancel();
        }
    }

    pub fn pause_forwarding(&self) {
        self.pause_forwarding.store(true, Ordering::Release);
    }

    pub fn resume_forwarding(&self) {
        self.pause_forwarding.store(false, Ordering::Release);
    }

    /// Stop reading/writing frames for WebSocket connections that already
    /// exist, while allowing later reconnects to establish a fresh socket.
    pub fn blackhole_websocket_io(&self) {
        tracing::warn!("🕳️  Blackholing existing WebSocket IO");
        let current_generation = self.state.websocket_generation.load(Ordering::Acquire);
        self.state
            .blackhole_websocket_generation
            .store(current_generation, Ordering::Release);
    }

    /// Resume reading/writing frames for blackholed WebSocket connections.
    pub fn restore_websocket_io(&self) {
        tracing::info!("🟢 Restoring WebSocket IO");
        self.state
            .blackhole_websocket_generation
            .store(0, Ordering::Release);
    }

    /// Drop the next N ICE candidate relay messages.
    ///
    /// This is a test-only hook for reproducing a post-cleanup negotiation
    /// where SDP arrives but trickle ICE is interrupted.
    pub fn drop_next_ice_candidates(&self, count: u32) {
        tracing::warn!(
            "🧪 Dropping the next {} ICE candidate relay message(s)",
            count
        );
        self.ice_candidate_drop_count.store(count, Ordering::SeqCst);
    }

    /// Drop the next N ICE candidate relay messages for a bounded duration.
    pub fn drop_next_ice_candidates_for(&self, count: u32, duration: std::time::Duration) {
        self.drop_next_ice_candidates(count);
        let ice_candidate_drop_count = self.ice_candidate_drop_count.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            ice_candidate_drop_count.store(0, Ordering::SeqCst);
        });
    }

    pub fn message_count(&self) -> u32 {
        self.message_count.load(Ordering::Relaxed)
    }

    pub fn ice_restart_count(&self) -> u32 {
        self.ice_restart_offer_count.load(Ordering::SeqCst)
    }

    pub fn ice_restart_request_count(&self) -> u32 {
        self.ice_restart_request_count.load(Ordering::SeqCst)
    }

    pub fn connection_count(&self) -> u32 {
        self.connection_count.load(Ordering::SeqCst)
    }

    pub fn disconnection_count(&self) -> u32 {
        self.disconnection_count.load(Ordering::SeqCst)
    }

    pub fn reset_counters(&self) {
        self.message_count.store(0, Ordering::Relaxed);
        self.ice_restart_offer_count.store(0, Ordering::SeqCst);
        self.ice_restart_request_count.store(0, Ordering::SeqCst);
        self.ice_candidate_drop_count.store(0, Ordering::SeqCst);
        self.connection_count.store(0, Ordering::SeqCst);
        self.disconnection_count.store(0, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }
}

impl Drop for MockActrixServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Build the axum router that multiplexes WebSocket signaling and HTTP
/// endpoints on the same port.
fn build_router(state: Arc<MockState>) -> Router {
    use axum::routing::{get, post};
    use tower_http::cors::{Any, CorsLayer};

    let ais_routes = Router::new()
        .route("/register", post(http::register_handler))
        .route(
            "/mfr/{name}/verifying_key",
            get(http::verifying_key_handler),
        )
        .route("/mfr/pkg/nonce", post(http::publish_nonce_handler))
        .route("/mfr/pkg/publish", post(http::publish_handler))
        .with_state(state.clone());

    Router::new()
        // WebSocket signaling — real actrix path and legacy root alias.
        .route("/signaling/ws", get(signaling::ws_upgrade_handler))
        .route("/", get(signaling::ws_upgrade_handler))
        .route("/signaling/health", get(http::health_handler))
        .route("/health", get(http::health_handler))
        // AIS endpoints at the root (for callers that use the root URL as
        // their AIS endpoint base).
        .route("/register", post(http::register_handler))
        .route(
            "/mfr/{name}/verifying_key",
            get(http::verifying_key_handler),
        )
        .route("/mfr/pkg/nonce", post(http::publish_nonce_handler))
        .route("/mfr/pkg/publish", post(http::publish_handler))
        // Admin seeding API (replaces register.sh sqlite3 INSERTs).
        .route("/admin/realms", post(http::admin_realm_handler))
        .route("/admin/mfr", post(http::admin_mfr_handler))
        .route("/admin/packages", post(http::admin_package_handler))
        .route("/admin/state", get(http::admin_state_handler))
        .with_state(state)
        // AIS routes mounted under `/ais/*` for callers whose configured AIS
        // endpoint is e.g. `http://localhost:8081/ais`.
        .merge(Router::new().nest("/ais", ais_routes))
        // Permissive CORS so browser-resident Service Workers (served from
        // e.g. `http://localhost:5173`) can POST to AIS / MFR endpoints.
        // Dev-only; a production signaling stack would restrict origins.
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}
