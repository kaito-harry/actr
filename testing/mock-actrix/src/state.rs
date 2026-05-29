//! Shared server state for the mock actrix.
//!
//! State is intentionally in-memory: the mock is rebuilt every test run and
//! persistence would only add brittleness.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};

use actr_protocol::{ActrId, ActrType, ServiceSpec, SignalingEnvelope};
use axum::extract::ws;
use ed25519_dalek::{SigningKey, VerifyingKey};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_util::sync::CancellationToken;

/// One registered actor/session in the signaling registry.
#[derive(Clone, Debug)]
pub struct RegisteredActor {
    pub actr_id: ActrId,
    pub actr_type: ActrType,
    pub client_id: String,
    pub ws_address: Option<String>,
    pub service_spec: Option<ServiceSpec>,
}

/// Realm entry (seeded via `POST /admin/realms` or lazily by the signaling
/// `RegisterRequest`).
#[derive(Clone, Debug)]
pub struct RealmEntry {
    pub id: u32,
    pub name: String,
}

/// Manufacturer entry — exposed via `GET /mfr/{name}/verifying_key`.
#[derive(Clone, Debug)]
pub struct MfrEntry {
    pub name: String,
    pub verifying_key: VerifyingKey,
    pub contact: Option<String>,
}

/// Package registration entry — populated by `POST /mfr/pkg/publish` or the
/// admin seeding endpoint.
#[derive(Clone, Debug)]
pub struct PackageEntry {
    pub manufacturer: String,
    pub name: String,
    pub version: String,
    pub target: String,
    pub manifest_raw: String,
    pub signature_b64: String,
}

/// Publish nonces issued via `POST /mfr/pkg/nonce`; consumed once by
/// `POST /mfr/pkg/publish`.
#[derive(Default)]
pub struct NonceStore {
    inner: RwLock<HashMap<String, String>>, // manufacturer -> nonce (b64)
}

impl NonceStore {
    pub async fn issue(&self, manufacturer: &str, nonce_b64: String) {
        self.inner
            .write()
            .await
            .insert(manufacturer.to_string(), nonce_b64);
    }

    pub async fn take(&self, manufacturer: &str, expected: &str) -> bool {
        let mut map = self.inner.write().await;
        matches!(map.remove(manufacturer), Some(v) if v == expected)
    }
}

/// All shared state owned by the server.
pub struct MockState {
    // --- Signing material -------------------------------------------------
    ais_signing_key: SigningKey,
    ais_signing_key_id: u32,
    builtin_mfr_signing_key: SigningKey,

    /// Cancellation token that signaling connections watch in their recv
    /// loop: when cancelled, each open WS breaks out and drops the stream so
    /// the peer sees a close. Mirrors the old tokio-tungstenite mock's
    /// behavior where shutting down the server forcibly disconnected every
    /// connected actor.
    pub cancel: CancellationToken,

    // --- Signaling registry -----------------------------------------------
    pub clients: RwLock<HashMap<String, mpsc::UnboundedSender<ws::Message>>>,
    pub registry: RwLock<Vec<RegisteredActor>>,
    pub client_to_actr_id: RwLock<HashMap<String, ActrId>>,
    pub next_serial: AtomicU64,

    // --- AIS/MFR registry -------------------------------------------------
    pub realms: RwLock<HashMap<u32, RealmEntry>>,
    pub mfrs: RwLock<HashMap<String, MfrEntry>>,
    pub packages: RwLock<Vec<PackageEntry>>,
    pub nonces: NonceStore,

    // --- Test observability -----------------------------------------------
    pub message_count: Arc<AtomicU32>,
    pub ice_restart_offer_count: Arc<AtomicU32>,
    pub ice_restart_request_count: Arc<AtomicU32>,
    pub pause_forwarding: Arc<AtomicBool>,
    pub ice_candidate_drop_count: Arc<AtomicU32>,
    pub blackhole_websocket_generation: Arc<AtomicU32>,
    pub websocket_generation: Arc<AtomicU32>,
    pub connection_count: Arc<AtomicU32>,
    pub disconnection_count: Arc<AtomicU32>,
    pub received_messages: Mutex<Vec<SignalingEnvelope>>,
}

impl MockState {
    pub fn new(
        ais_signing_key: SigningKey,
        ais_signing_key_id: u32,
        builtin_mfr_signing_key: SigningKey,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            ais_signing_key,
            ais_signing_key_id,
            builtin_mfr_signing_key,
            cancel,
            clients: RwLock::new(HashMap::new()),
            registry: RwLock::new(Vec::new()),
            client_to_actr_id: RwLock::new(HashMap::new()),
            next_serial: AtomicU64::new(1),
            realms: RwLock::new(HashMap::new()),
            mfrs: RwLock::new(HashMap::new()),
            packages: RwLock::new(Vec::new()),
            nonces: NonceStore::default(),
            message_count: Arc::new(AtomicU32::new(0)),
            ice_restart_offer_count: Arc::new(AtomicU32::new(0)),
            ice_restart_request_count: Arc::new(AtomicU32::new(0)),
            pause_forwarding: Arc::new(AtomicBool::new(false)),
            ice_candidate_drop_count: Arc::new(AtomicU32::new(0)),
            blackhole_websocket_generation: Arc::new(AtomicU32::new(0)),
            websocket_generation: Arc::new(AtomicU32::new(0)),
            connection_count: Arc::new(AtomicU32::new(0)),
            disconnection_count: Arc::new(AtomicU32::new(0)),
            received_messages: Mutex::new(Vec::new()),
        }
    }

    pub fn ais_signing_key(&self) -> &SigningKey {
        &self.ais_signing_key
    }

    pub fn ais_signing_key_id(&self) -> u32 {
        self.ais_signing_key_id
    }

    pub fn builtin_mfr_signing_key(&self) -> &SigningKey {
        &self.builtin_mfr_signing_key
    }

    pub async fn add_realm(&self, id: u32, name: String) {
        self.realms
            .write()
            .await
            .insert(id, RealmEntry { id, name });
    }

    pub async fn add_mfr(&self, name: String, verifying_key: VerifyingKey) {
        self.mfrs.write().await.insert(
            name.clone(),
            MfrEntry {
                name,
                verifying_key,
                contact: None,
            },
        );
    }

    pub async fn add_package(&self, pkg: PackageEntry) {
        let mut packages = self.packages.write().await;
        // De-duplicate by (manufacturer, name, version, target).
        packages.retain(|p| {
            !(p.manufacturer == pkg.manufacturer
                && p.name == pkg.name
                && p.version == pkg.version
                && p.target == pkg.target)
        });
        packages.push(pkg);
    }

    /// Look up a manufacturer's verifying key, falling back to the built-in
    /// MFR key when no explicit entry exists. This makes the mock forgiving
    /// for tests that never register an MFR.
    pub async fn mfr_verifying_key(&self, name: &str) -> Option<VerifyingKey> {
        if let Some(mfr) = self.mfrs.read().await.get(name) {
            return Some(mfr.verifying_key);
        }
        None
    }
}
