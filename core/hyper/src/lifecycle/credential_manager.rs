//! Credential Manager — single-flight soft renew / hard rebind orchestration.
//!
//! # Trigger sources
//!
//! - Access credential expiry scheduler (5 min before expiry + 0–30s jitter).
//! - Heartbeat / signaling returns 401.
//! - (Legacy) signaling credential warning.
//!
//! # Behaviour
//!
//! 1. All triggers enter the same single-flight future.
//! 2. Call `POST /ais/renew`.
//! 3. On success: atomically replace credentials (soft renew).
//! 4. On 401 or locally-expired renewal token: hard rebind via `/register`.
//! 5. On 403: transition to `RealmUnavailable`, stop retrying.
//! 6. Temporary errors: exponential backoff with jitter.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use actr_protocol::prost::Message as _;
use actr_protocol::{
    IdentityClaims, RenewCredentialRequest, register_response, renew_credential_response,
};
use tokio::sync::Mutex;

use crate::ais_client::{AisClient, RenewError};
use crate::transport::PeerTransport;
use crate::wire::webrtc::gate::WebRtcGate;
use crate::wire::webrtc::{HookCallback, HookEvent, SignalingClient, WebRtcCoordinator};

use super::node::CredentialState;
use super::session_state::{SessionSnapshot, SessionState};

// ---- Registration Context --------------------------------------------------

/// Saved registration parameters so hard rebind can call `/register` again
/// with the same authentication context.
#[derive(Clone)]
pub(crate) enum RegistrationContext {
    /// Package-backed registration — carries the full original request
    /// including manifest bytes and MFR signature.
    ///
    /// `resign` is the manufacturer re-signing capability. It is `Some` when the
    /// initial registration carried a manufacturer proof (unpublished package). Hard
    /// rebind re-invokes it to mint a fresh proof — the original nonce was
    /// consumed by AIS on first success and cannot be reused.
    Package {
        #[allow(dead_code)]
        request: actr_protocol::RegisterRequest,
        resign: Option<Arc<dyn crate::ManufacturerAuthProvider>>,
    },
    /// Source-linked registration — carries the request and an optional
    /// realm secret (kept in memory only, never logged).
    Linked {
        #[allow(dead_code)]
        request: actr_protocol::RegisterRequest,
        #[allow(dead_code)]
        realm_secret: Option<String>,
    },
}

// ---- Credential Manager ----------------------------------------------------

/// Shared credential manager — clonable, all clones share the same state.
#[derive(Clone)]
pub(crate) struct CredentialManager {
    session: SessionState,
    registration_ctx: RegistrationContext,
    ais_endpoint: String,
    realm_secret: Option<String>,

    /// Single-flight guard: only one renewal attempt at a time.
    renewing: Arc<AtomicBool>,
    /// Pending renewal join handle for cancellation during shutdown.
    inflight: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Runtime handles that must be updated after hard rebind commits.
    hard_rebind_handles: Arc<Mutex<Option<HardRebindHandles>>>,
    /// Lifecycle callback notified after soft renewal commits.
    hook_callback: Arc<Mutex<Option<HookCallback>>>,
}

#[derive(Clone)]
pub(crate) struct HardRebindHandles {
    pub signaling_client: Arc<dyn SignalingClient>,
    pub credential_state: CredentialState,
    pub webrtc_coordinator: Option<Arc<WebRtcCoordinator>>,
    pub webrtc_gate: Option<Arc<WebRtcGate>>,
    pub peer_transport: Option<Arc<PeerTransport>>,
}

impl CredentialManager {
    pub(crate) fn new(
        session: SessionState,
        registration_ctx: RegistrationContext,
        ais_endpoint: impl Into<String>,
        realm_secret: Option<String>,
    ) -> Self {
        Self {
            session,
            registration_ctx,
            ais_endpoint: ais_endpoint.into(),
            realm_secret,
            renewing: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(Mutex::new(None)),
            hard_rebind_handles: Arc::new(Mutex::new(None)),
            hook_callback: Arc::new(Mutex::new(None)),
        }
    }

    /// Return a clone of the managed SessionState.
    pub(crate) fn session_state(&self) -> SessionState {
        self.session.clone()
    }

    pub(crate) async fn install_hard_rebind_handles(&self, handles: HardRebindHandles) {
        *self.hard_rebind_handles.lock().await = Some(handles);
    }

    pub(crate) async fn install_hook_callback(&self, callback: Option<HookCallback>) {
        *self.hook_callback.lock().await = callback;
    }

    /// Entry point for all renewal triggers. Returns immediately if a
    /// renewal is already in flight (single-flight).
    pub(crate) fn trigger_renewal(&self) {
        // Fast-path: if already renewing, skip.
        if self.renewing.swap(true, Ordering::AcqRel) {
            tracing::debug!("CredentialManager: renewal already in flight, skipping trigger");
            return;
        }

        let session = self.session.clone();
        let ais_endpoint = self.ais_endpoint.clone();
        let realm_secret = self.realm_secret.clone();
        let registration_ctx = self.registration_ctx.clone();
        let renewing = self.renewing.clone();
        let hard_rebind_handles = self.hard_rebind_handles.clone();
        let hook_callback = self.hook_callback.clone();

        // Spawn the actual work so the caller isn't blocked.
        let handle = tokio::spawn(async move {
            let handles = hard_rebind_handles.lock().await.clone();
            let hook_callback = hook_callback.lock().await.clone();
            let result = run_renewal_once(
                session,
                ais_endpoint,
                realm_secret,
                registration_ctx,
                handles,
                hook_callback,
            )
            .await;
            if let Err(err) = result {
                tracing::warn!(error = %err, "CredentialManager: renewal attempt ended");
            }
            renewing.store(false, Ordering::Release);
        });

        // Store the handle for potential cancellation during shutdown.
        let inflight = self.inflight.clone();
        tokio::spawn(async move {
            let mut guard = inflight.lock().await;
            *guard = Some(handle);
        });
    }

    /// Cancel any in-flight renewal (called during shutdown).
    #[allow(dead_code)]
    pub(crate) async fn cancel(&self) {
        let mut guard = self.inflight.lock().await;
        if let Some(handle) = guard.take() {
            handle.abort();
        }
        self.renewing.store(false, Ordering::Release);
    }
}

async fn run_renewal_once(
    session: SessionState,
    ais_endpoint: String,
    realm_secret: Option<String>,
    registration_ctx: RegistrationContext,
    hard_rebind_handles: Option<HardRebindHandles>,
    hook_callback: Option<HookCallback>,
) -> Result<(), String> {
    let snapshot = session.snapshot().await;

    if snapshot.renewal_token.is_empty() {
        return run_hard_rebind(
            session,
            ais_endpoint,
            realm_secret,
            registration_ctx,
            hard_rebind_handles,
        )
        .await;
    }

    if is_expired(snapshot.renewal_token_expires_at.seconds) {
        return run_hard_rebind(
            session,
            ais_endpoint,
            realm_secret,
            registration_ctx,
            hard_rebind_handles,
        )
        .await;
    }

    let mut ais = AisClient::new(&ais_endpoint);
    if let Some(secret) = realm_secret.as_deref() {
        ais = ais.with_realm_secret(secret);
    }

    let request = RenewCredentialRequest {
        actr_id: snapshot.actor_id.clone(),
        renewal_token: snapshot.renewal_token.clone(),
    };

    let response = match ais.renew_credential(request).await {
        Ok(response) => response,
        Err(RenewError::RealmUnavailable) => {
            session.set_realm_unavailable().await;
            return Err("realm unavailable during renewal".to_string());
        }
        Err(RenewError::TokenRejected) => {
            return run_hard_rebind(
                session,
                ais_endpoint,
                realm_secret,
                registration_ctx,
                hard_rebind_handles,
            )
            .await;
        }
        Err(RenewError::RateLimited { retry_after }) => {
            if let Some(delay) = retry_after {
                tokio::time::sleep(delay).await;
            }
            return Err("renewal rate limited".to_string());
        }
        Err(RenewError::Retryable(err)) => {
            let mut backoff = Backoff::new();
            tokio::time::sleep(backoff.next()).await;
            return Err(format!("retryable renew error: {err}"));
        }
        Err(err) => return Err(err.to_string()),
    };

    let ok = match response.result {
        Some(renew_credential_response::Result::Success(ok)) => ok,
        Some(renew_credential_response::Result::Error(err)) => {
            return Err(format!(
                "renew response contained error {}: {}",
                err.code, err.message
            ));
        }
        None => return Err("renew response missing result".to_string()),
    };

    if ok.actr_id != snapshot.actor_id {
        return Err("renew response changed ActrId".to_string());
    }

    let claims = IdentityClaims::decode(ok.credential.claims.as_ref())
        .map_err(|e| format!("renew credential claims decode failed: {e}"))?;
    if claims.actor_id != snapshot.actor_id.to_string_repr() {
        return Err("renew credential claims actor_id mismatch".to_string());
    }

    let credential_expires_at = ok
        .credential_expires_at
        .ok_or_else(|| "renew response missing credential expiry".to_string())?;
    let renewal_token = ok
        .renewal_token
        .ok_or_else(|| "renew response missing renewal token".to_string())?;
    let renewal_token_expires_at = ok
        .renewal_token_expires_at
        .ok_or_else(|| "renew response missing renewal token expiry".to_string())?;

    session
        .update_credentials(
            ok.credential.clone(),
            credential_expires_at,
            ok.turn_credential.clone(),
            renewal_token.clone(),
            renewal_token_expires_at,
        )
        .await;

    if let Some(handles) = hard_rebind_handles {
        handles
            .credential_state
            .update(
                ok.credential,
                Some(credential_expires_at),
                Some(ok.turn_credential),
            )
            .await;
    }

    fire_credential_renewed(hook_callback.as_ref(), &credential_expires_at).await;

    tracing::info!(
        actor_id = %snapshot.actor_id.to_string_repr(),
        credential_expires_at = credential_expires_at.seconds,
        renewal_token_expires_at = renewal_token_expires_at.seconds,
        "CredentialManager: soft renewal completed"
    );

    Ok(())
}

async fn run_hard_rebind(
    session: SessionState,
    ais_endpoint: String,
    realm_secret: Option<String>,
    registration_ctx: RegistrationContext,
    hard_rebind_handles: Option<HardRebindHandles>,
) -> Result<(), String> {
    let old_snapshot = session.snapshot().await;
    tracing::warn!(
        actor_id = %old_snapshot.actor_id.to_string_repr(),
        generation = old_snapshot.generation,
        "CredentialManager: starting hard rebind"
    );

    let mut ais = AisClient::new(&ais_endpoint);
    let request = match registration_ctx {
        RegistrationContext::Package { request, resign } => {
            let mut request = request;
            // The original manufacturer proof's nonce was consumed by AIS on the
            // first successful registration. Reusing it would be a replay.
            // Re-invoke the provider to mint a fresh `signed_at` + `nonce` +
            // signature, overwriting only the three manufacturer fields — the
            // manifest bytes and MFR signature are static and stay as-is.
            if let Some(provider) = resign.as_ref() {
                let realm_id = request.realm.realm_id;
                let actr_type = request.actr_type.clone();
                let target = request
                    .target
                    .clone()
                    .filter(|target| !target.is_empty())
                    .ok_or_else(|| {
                        "hard rebind manufacturer re-sign failed: package target is missing"
                            .to_string()
                    })?;
                let manifest_raw = request
                    .manifest_raw
                    .as_ref()
                    .filter(|manifest| !manifest.is_empty())
                    .map(|manifest| manifest.to_vec())
                    .ok_or_else(|| {
                        "hard rebind manufacturer re-sign failed: package manifest is missing"
                            .to_string()
                    })?;
                let fresh = crate::sign_manufacturer_proof(
                    Arc::clone(provider),
                    realm_id,
                    actr_type,
                    target,
                    manifest_raw,
                )
                .await
                .map_err(|e| format!("hard rebind manufacturer re-sign failed: {e}"))?;
                request.manufacturer_auth_signature = Some(bytes::Bytes::from(fresh.signature));
                request.manufacturer_auth_signed_at = Some(fresh.signed_at);
                request.manufacturer_auth_nonce = Some(bytes::Bytes::from(fresh.nonce));
            }
            request
        }
        RegistrationContext::Linked {
            request,
            realm_secret,
        } => {
            if let Some(secret) = realm_secret {
                ais = ais.with_realm_secret(secret);
            }
            request
        }
    };
    if let Some(secret) = realm_secret {
        ais = ais.with_realm_secret(secret);
    }

    let response = ais
        .register_with_manifest(request)
        .await
        .map_err(|err| format!("hard rebind register failed before commit: {err}"))?;

    let ok = match response.result {
        Some(register_response::Result::Success(ok)) => ok,
        Some(register_response::Result::Error(err)) => {
            return Err(format!(
                "hard rebind register rejected before commit {}: {}",
                err.code, err.message
            ));
        }
        None => return Err("hard rebind register response missing result".to_string()),
    };

    let credential_expires_at = ok
        .credential_expires_at
        .ok_or_else(|| "hard rebind response missing credential expiry".to_string())?;
    let renewal_token = ok
        .renewal_token
        .ok_or_else(|| "hard rebind response missing renewal token".to_string())?;
    let renewal_token_expires_at = ok
        .renewal_token_expires_at
        .ok_or_else(|| "hard rebind response missing renewal token expiry".to_string())?;

    let new_snapshot = SessionSnapshot {
        actor_id: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        credential_expires_at,
        turn_credential: ok.turn_credential.clone(),
        renewal_token,
        renewal_token_expires_at,
        generation: old_snapshot.generation.saturating_add(1),
    };

    session.enter_rebinding().await;
    let _old = session.commit_hard_rebind(new_snapshot.clone()).await;

    if let Some(handles) = hard_rebind_handles {
        handles
            .credential_state
            .update(
                new_snapshot.credential.clone(),
                Some(new_snapshot.credential_expires_at),
                Some(new_snapshot.turn_credential.clone()),
            )
            .await;

        handles
            .signaling_client
            .set_actor_id(new_snapshot.actor_id.clone())
            .await;
        handles
            .signaling_client
            .set_credential_state(handles.credential_state.clone())
            .await;

        if let Some(coordinator) = handles.webrtc_coordinator {
            coordinator
                .set_local_id(new_snapshot.actor_id.clone())
                .await;
            if let Err(err) = coordinator.close_all_peers().await {
                tracing::warn!(error = %err, "hard rebind failed to close old WebRTC peers");
            }
        }
        if let Some(gate) = handles.webrtc_gate {
            gate.set_local_id(new_snapshot.actor_id.clone()).await;
        }
        if let Some(peer_transport) = handles.peer_transport
            && let Err(err) = peer_transport.close_all().await
        {
            tracing::warn!(error = %err, "hard rebind failed to close old peer transports");
        }

        if let Err(err) = handles.signaling_client.disconnect().await {
            tracing::warn!(error = %err, "hard rebind signaling disconnect failed");
        }
        match handles.signaling_client.connect_once().await {
            Ok(()) => session.set_active().await,
            Err(err) => {
                handles.signaling_client.schedule_auto_reconnect();
                return Err(format!(
                    "hard rebind committed but signaling reconnect failed: {err}"
                ));
            }
        }
    } else {
        session.set_active().await;
    }

    tracing::info!(
        actor_id = %new_snapshot.actor_id.to_string_repr(),
        generation = new_snapshot.generation,
        credential_expires_at = new_snapshot.credential_expires_at.seconds,
        renewal_token_expires_at = new_snapshot.renewal_token_expires_at.seconds,
        "CredentialManager: hard rebind committed"
    );

    Ok(())
}

fn is_expired(expires_at: i64) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    expires_at <= now
}

async fn fire_credential_renewed(
    hook_callback: Option<&HookCallback>,
    expires_at: &prost_types::Timestamp,
) {
    if let Some(callback) = hook_callback {
        let new_expiry =
            SystemTime::UNIX_EPOCH + Duration::from_secs(expires_at.seconds.max(0) as u64);
        callback(HookEvent::CredentialRenewed { new_expiry }).await;
    }
}

// ---- Exponential backoff with jitter ---------------------------------------

struct Backoff {
    attempt: u32,
}

impl Backoff {
    fn new() -> Self {
        Self { attempt: 0 }
    }

    /// Returns the next delay: 5, 10, 20, 40, 60, 60, ... seconds with
    /// ±25% jitter, capped at 60s.
    #[allow(dead_code)]
    fn next(&mut self) -> Duration {
        let base = match self.attempt {
            0 => 5,
            1 => 10,
            2 => 20,
            3 => 40,
            _ => 60,
        };
        self.attempt += 1;

        // Deterministic jitter: use attempt number as seed.
        let jitter =
            (base as f64 * 0.25 * ((self.attempt.wrapping_mul(7)) as f64 % 2.0 - 1.0)) as i64;
        let ms = ((base * 1000) as i64 + jitter * 1000i64).max(1000);
        Duration::from_millis(ms as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::{
        AIdCredential, ActrId, ActrType, IdentityClaims, Realm, RegisterRequest, RegisterResponse,
        RenewCredentialResponse, TurnCredential, register_response, renew_credential_response,
    };
    use prost::bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    fn actor(serial: u64) -> ActrId {
        ActrId {
            realm: Realm { realm_id: 7 },
            serial_number: serial,
            r#type: ActrType {
                manufacturer: "acme".to_string(),
                name: "node".to_string(),
                version: "1.0.0".to_string(),
            },
        }
    }

    fn credential(key_id: u32) -> AIdCredential {
        AIdCredential {
            key_id,
            claims: Bytes::new(),
            signature: Bytes::from(vec![0; 64]),
        }
    }

    fn credential_for_actor(actor_id: &ActrId, key_id: u32, expires_at: u64) -> AIdCredential {
        let claims = IdentityClaims {
            realm_id: actor_id.realm.realm_id,
            actor_id: actor_id.to_string_repr(),
            expires_at,
        };
        AIdCredential {
            key_id,
            claims: claims.encode_to_vec().into(),
            signature: Bytes::from(vec![0; 64]),
        }
    }

    fn register_ok(serial: u64, key_id: u32) -> register_response::RegisterOk {
        register_response::RegisterOk {
            actr_id: actor(serial),
            credential: credential(key_id),
            turn_credential: TurnCredential {
                username: format!("1000:actor-{serial}"),
                password: "turn-password".to_string(),
                expires_at: 1000,
            },
            credential_expires_at: Some(prost_types::Timestamp {
                seconds: 1000,
                nanos: 0,
            }),
            signaling_heartbeat_interval_secs: 30,
            signing_pubkey: Bytes::from(vec![1; 32]),
            signing_key_id: key_id,
            renewal_token: Some(Bytes::from_static(b"new-renewal-token-32-bytes!!")),
            renewal_token_expires_at: Some(prost_types::Timestamp {
                seconds: 2000,
                nanos: 0,
            }),
        }
    }

    #[tokio::test]
    async fn expired_renewal_token_hard_rebinds_via_register() {
        let mut server = mockito::Server::new_async().await;
        let response = RegisterResponse {
            result: Some(register_response::Result::Success(register_ok(2, 9))),
        };
        let mock = server
            .mock("POST", "/register")
            .with_status(200)
            .with_header("content-type", "application/x-protobuf")
            .with_body(response.encode_to_vec())
            .expect(1)
            .create_async()
            .await;

        let session = SessionState::new(super::super::session_state::SessionSnapshot {
            actor_id: actor(1),
            credential: credential(1),
            credential_expires_at: prost_types::Timestamp {
                seconds: 10,
                nanos: 0,
            },
            turn_credential: TurnCredential {
                username: "10:actor-1".to_string(),
                password: "old".to_string(),
                expires_at: 10,
            },
            renewal_token: Bytes::from_static(b"expired-renewal-token-32bytes"),
            renewal_token_expires_at: prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            },
            generation: 1,
        });

        let request = RegisterRequest {
            actr_type: actor(1).r#type,
            realm: Realm { realm_id: 7 },
            ..Default::default()
        };

        run_renewal_once(
            session.clone(),
            server.url(),
            None,
            RegistrationContext::Linked {
                request,
                realm_secret: None,
            },
            None,
            None,
        )
        .await
        .expect("hard rebind should commit new snapshot");

        mock.assert_async().await;
        let snapshot = session.snapshot().await;
        assert_eq!(snapshot.actor_id, actor(2));
        assert_eq!(snapshot.credential.key_id, 9);
        assert_eq!(snapshot.generation, 2);
        assert_eq!(
            session.phase().await,
            super::super::session_state::SessionPhase::Active
        );
    }

    #[tokio::test]
    async fn soft_renewal_fires_credential_renewed_hook() {
        const OLD_EXPIRY: i64 = 4_000_000_000;
        const NEW_EXPIRY: i64 = 4_000_001_000;
        let actor_id = actor(1);
        let mut server = mockito::Server::new_async().await;
        let response = RenewCredentialResponse {
            result: Some(renew_credential_response::Result::Success(
                register_response::RegisterOk {
                    actr_id: actor_id.clone(),
                    credential: credential_for_actor(&actor_id, 9, NEW_EXPIRY as u64),
                    turn_credential: TurnCredential {
                        username: "4000001000:actor-1".to_string(),
                        password: "turn-password".to_string(),
                        expires_at: NEW_EXPIRY as u64,
                    },
                    credential_expires_at: Some(prost_types::Timestamp {
                        seconds: NEW_EXPIRY,
                        nanos: 0,
                    }),
                    signaling_heartbeat_interval_secs: 30,
                    signing_pubkey: Bytes::from(vec![1; 32]),
                    signing_key_id: 9,
                    renewal_token: Some(Bytes::from(vec![8; 32])),
                    renewal_token_expires_at: Some(prost_types::Timestamp {
                        seconds: NEW_EXPIRY + 1000,
                        nanos: 0,
                    }),
                },
            )),
        };
        let mock = server
            .mock("POST", "/renew")
            .with_status(200)
            .with_header("content-type", "application/x-protobuf")
            .with_body(response.encode_to_vec())
            .expect(1)
            .create_async()
            .await;

        let session = SessionState::new(super::super::session_state::SessionSnapshot {
            actor_id: actor_id.clone(),
            credential: credential_for_actor(&actor_id, 1, OLD_EXPIRY as u64),
            credential_expires_at: prost_types::Timestamp {
                seconds: OLD_EXPIRY,
                nanos: 0,
            },
            turn_credential: TurnCredential {
                username: "4000000000:actor-1".to_string(),
                password: "old".to_string(),
                expires_at: OLD_EXPIRY as u64,
            },
            renewal_token: Bytes::from(vec![7; 32]),
            renewal_token_expires_at: prost_types::Timestamp {
                seconds: OLD_EXPIRY + 1000,
                nanos: 0,
            },
            generation: 1,
        });

        let hook_expiry = Arc::new(AtomicU64::new(0));
        let hook_expiry_for_cb = hook_expiry.clone();
        let hook_callback: crate::wire::webrtc::HookCallback = Arc::new(move |event| {
            let hook_expiry = hook_expiry_for_cb.clone();
            Box::pin(async move {
                if let crate::wire::webrtc::HookEvent::CredentialRenewed { new_expiry } = event {
                    let secs = new_expiry
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("expiry should be after epoch")
                        .as_secs();
                    hook_expiry.store(secs, AtomicOrdering::SeqCst);
                }
            })
        });

        run_renewal_once(
            session,
            server.url(),
            None,
            RegistrationContext::Linked {
                request: RegisterRequest {
                    actr_type: actor_id.r#type.clone(),
                    realm: actor_id.realm,
                    ..Default::default()
                },
                realm_secret: None,
            },
            None,
            Some(hook_callback),
        )
        .await
        .expect("soft renewal should succeed");

        mock.assert_async().await;
        assert_eq!(hook_expiry.load(AtomicOrdering::SeqCst), NEW_EXPIRY as u64);
    }

    #[test]
    fn backoff_sequence() {
        let mut b = Backoff::new();
        let d0 = b.next();
        let d1 = b.next();
        let d2 = b.next();
        let d3 = b.next();
        let d4 = b.next();

        assert!(d0 >= Duration::from_secs(1));
        assert!(d1 >= Duration::from_secs(1));
        assert!(d2 >= Duration::from_secs(1));
        assert!(d3 >= Duration::from_secs(1));
        assert!(d4 <= Duration::from_secs(75)); // 60 + 25% jitter
    }

    /// Hard rebind of a Package registration must re-invoke the manufacturer auth
    /// provider to mint a fresh proof. The initial manufacturer nonce was consumed
    /// by AIS on the first successful registration; replaying it would be
    /// rejected. This locks in the re-sign behaviour added to fix replay on
    /// hard rebind.
    #[tokio::test]
    async fn package_hard_rebind_re_signs_manufacturer_proof() {
        use actr_protocol::RegisterAuthMode;
        use std::sync::atomic::AtomicU64;

        struct CountingManufacturerProvider {
            calls: Arc<AtomicU64>,
        }
        impl crate::ManufacturerAuthProvider for CountingManufacturerProvider {
            fn sign(
                &self,
                _realm_id: u32,
                _actr_type: &actr_protocol::ActrType,
                _target: &str,
                _manifest_raw: &[u8],
            ) -> std::result::Result<crate::ManufacturerRegistrationAuth, crate::HyperError>
            {
                let n = self.calls.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                // Deterministic fresh proof keyed by call count. The mock AIS
                // server does not validate crypto, so the values only need to
                // differ from the stale proof saved on the original request.
                Ok(crate::ManufacturerRegistrationAuth {
                    signature: vec![n as u8; 64],
                    signed_at: 9_999_999_999,
                    nonce: vec![n as u8; 32],
                })
            }
        }

        let calls = Arc::new(AtomicU64::new(0));
        let provider: Arc<CountingManufacturerProvider> = Arc::new(CountingManufacturerProvider {
            calls: calls.clone(),
        });

        let mut server = mockito::Server::new_async().await;
        let response = RegisterResponse {
            result: Some(register_response::Result::Success(register_ok(2, 9))),
        };
        let mock = server
            .mock("POST", "/register")
            .with_status(200)
            .with_header("content-type", "application/x-protobuf")
            .with_body(response.encode_to_vec())
            .expect(1)
            .create_async()
            .await;

        // Stale manufacturer proof (nonce 0xAA) saved on the original request — this
        // is exactly what must not be replayed to AIS.
        let request = RegisterRequest {
            actr_type: actor(1).r#type,
            realm: Realm { realm_id: 7 },
            manifest_raw: Some(Bytes::from_static(b"manifest-bytes")),
            mfr_signature: Some(Bytes::from_static(b"mfr-sig")),
            target: Some("wasm32-wasip1".to_string()),
            auth_mode: Some(RegisterAuthMode::Package as i32),
            manufacturer_auth_signature: Some(Bytes::from_static(b"stale-manufacturer-auth-sig")),
            manufacturer_auth_signed_at: Some(1),
            manufacturer_auth_nonce: Some(Bytes::from_static(&[0xAA; 32])),
            ..Default::default()
        };

        // Expired renewal token -> run_renewal_once takes the hard rebind branch.
        let session = SessionState::new(SessionSnapshot {
            actor_id: actor(1),
            credential: credential(1),
            credential_expires_at: prost_types::Timestamp {
                seconds: 10,
                nanos: 0,
            },
            turn_credential: TurnCredential {
                username: "10:actor-1".to_string(),
                password: "old".to_string(),
                expires_at: 10,
            },
            renewal_token: Bytes::from_static(b"expired-renewal-token-32bytes"),
            renewal_token_expires_at: prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            },
            generation: 1,
        });

        run_renewal_once(
            session,
            server.url(),
            None,
            RegistrationContext::Package {
                request,
                resign: Some(provider),
            },
            None,
            None,
        )
        .await
        .expect("hard rebind should commit new snapshot");

        mock.assert_async().await;
        assert_eq!(
            calls.load(AtomicOrdering::SeqCst),
            1,
            "hard rebind must re-invoke the manufacturer auth provider exactly once"
        );
    }

    #[test]
    fn is_expired_past_and_future() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Far past → expired.
        assert!(is_expired(now - 3600));
        assert!(is_expired(0));

        // Exactly now → expired (<=).
        assert!(is_expired(now));

        // Far future → not expired.
        assert!(!is_expired(now + 3600));
    }

    #[tokio::test]
    async fn fire_credential_renewed_invokes_callback_with_expiry() {
        use std::sync::Mutex;

        let captured: Arc<Mutex<Option<std::time::SystemTime>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();
        let cb: HookCallback = Arc::new(move |event| {
            let cap = cap.clone();
            Box::pin(async move {
                if let HookEvent::CredentialRenewed { new_expiry } = event {
                    *cap.lock().unwrap() = Some(new_expiry);
                }
            })
        });

        let expires_at = prost_types::Timestamp {
            seconds: 1000,
            nanos: 0,
        };
        fire_credential_renewed(Some(&cb), &expires_at).await;

        let got = captured.lock().unwrap().take();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        assert_eq!(got, Some(expected));
    }

    #[tokio::test]
    async fn fire_credential_renewed_none_callback_is_noop() {
        // None callback is the explicit no-op contract for runtimes without observers.
        fire_credential_renewed(
            None,
            &prost_types::Timestamp {
                seconds: 5,
                nanos: 0,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn fire_credential_renewed_clamps_negative_seconds_to_zero() {
        use std::sync::Mutex;

        let captured: Arc<Mutex<Option<std::time::SystemTime>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();
        let cb: HookCallback = Arc::new(move |event| {
            let cap = cap.clone();
            Box::pin(async move {
                if let HookEvent::CredentialRenewed { new_expiry } = event {
                    *cap.lock().unwrap() = Some(new_expiry);
                }
            })
        });

        // Negative seconds (malformed/epoch) must clamp to 0 → UNIX_EPOCH.
        fire_credential_renewed(
            Some(&cb),
            &prost_types::Timestamp {
                seconds: -100,
                nanos: 0,
            },
        )
        .await;
        assert_eq!(*captured.lock().unwrap(), Some(SystemTime::UNIX_EPOCH));
    }
}
