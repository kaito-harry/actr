//! Unified session state — the single source of truth for actor identity,
//! credentials, renewal tokens, and session phase.
//!
//! # Design
//!
//! Identity and all credentials live in one atomically-replaceable snapshot.
//! Consumers read from the snapshot via [`SessionState`] on every outbound send,
//! so a soft renew automatically propagates new credentials without restarting
//! signaling or closing peers.
//!
//! On hard rebind the phase transitions to `Rebinding`, the generation is
//! bumped, and old-generation contexts are invalidated. New outbound waits
//! until the phase returns to `Active`.

use std::sync::Arc;

use actr_protocol::{AIdCredential, ActrId, TurnCredential};
use prost::bytes::Bytes;
use prost_types::Timestamp;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Phase
// ---------------------------------------------------------------------------

/// What the session is currently allowed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPhase {
    /// Normal operation — outbound allowed.
    Active,
    /// Hard rebind in progress — new outbound is gated, old-generation
    /// contexts are invalidated.
    Rebinding,
    /// Realm returned 403 — no hard register allowed; peers stay alive
    /// but the credential manager stops attempting renewal.
    RealmUnavailable,
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// One complete, consistent view of actor identity and credentials.
#[derive(Clone)]
pub struct SessionSnapshot {
    pub actor_id: ActrId,
    pub credential: AIdCredential,
    pub credential_expires_at: Timestamp,
    pub turn_credential: TurnCredential,
    pub renewal_token: Bytes,
    pub renewal_token_expires_at: Timestamp,
    /// Monotonically-increasing generation. Bumped on hard rebind so that
    /// old-generation [`crate::context::RuntimeContext`] instances can
    /// detect staleness and return `ConnectionNotReady`.
    pub generation: u64,
}

// ---------------------------------------------------------------------------
// SessionState
// ---------------------------------------------------------------------------

/// Thread-safe, clonable handle to the shared session.
#[derive(Clone)]
pub struct SessionState {
    inner: Arc<RwLock<SessionStateInner>>,
}

struct SessionStateInner {
    phase: SessionPhase,
    snapshot: SessionSnapshot,
    /// Stale detector snapshot — written immediately after a hard rebind
    /// commit so old-generation contexts can compare.
    current_generation: u64,
}

impl SessionState {
    /// Create a new `SessionState` in the `Active` phase with generation 1.
    pub fn new(snapshot: SessionSnapshot) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SessionStateInner {
                phase: SessionPhase::Active,
                current_generation: snapshot.generation,
                snapshot,
            })),
        }
    }

    // ---- readers -----------------------------------------------------------

    /// Return a clone of the current snapshot (cheap — protobuf types are
    /// `Clone`). Callers that only need a field should use the specific
    /// accessor methods instead.
    pub async fn snapshot(&self) -> SessionSnapshot {
        self.inner.read().await.snapshot.clone()
    }

    /// Current actor identity.
    pub async fn actor_id(&self) -> ActrId {
        self.inner.read().await.snapshot.actor_id.clone()
    }

    /// Synchronous best-effort actor ID read for APIs that cannot be async.
    pub fn actor_id_sync(&self) -> Option<ActrId> {
        self.inner
            .try_read()
            .ok()
            .map(|guard| guard.snapshot.actor_id.clone())
    }

    /// Current access credential.
    pub async fn credential(&self) -> AIdCredential {
        self.inner.read().await.snapshot.credential.clone()
    }

    /// Current access credential expiry.
    pub async fn credential_expires_at(&self) -> Timestamp {
        self.inner.read().await.snapshot.credential_expires_at
    }

    /// Current TURN credential.
    pub async fn turn_credential(&self) -> TurnCredential {
        self.inner.read().await.snapshot.turn_credential.clone()
    }

    /// Current renewal token.
    pub async fn renewal_token(&self) -> Bytes {
        self.inner.read().await.snapshot.renewal_token.clone()
    }

    /// Current renewal token expiry.
    pub async fn renewal_token_expires_at(&self) -> Timestamp {
        self.inner.read().await.snapshot.renewal_token_expires_at
    }

    /// Current session phase.
    pub async fn phase(&self) -> SessionPhase {
        self.inner.read().await.phase
    }

    /// Current generation number.
    pub async fn generation(&self) -> u64 {
        self.inner.read().await.current_generation
    }

    /// Synchronous best-effort generation read for context builders.
    pub fn generation_sync(&self) -> Option<u64> {
        self.inner
            .try_read()
            .ok()
            .map(|guard| guard.current_generation)
    }

    /// Check whether the given generation is still current.
    pub async fn is_current_generation(&self, generation: u64) -> bool {
        self.inner.read().await.current_generation == generation
    }

    // ---- writers -----------------------------------------------------------

    /// Atomically replace credentials (soft renew path).
    ///
    /// Preserves `actor_id`, `generation`, and `phase`. Only credential,
    /// TURN credential, and renewal token are replaced.
    pub(crate) async fn update_credentials(
        &self,
        credential: AIdCredential,
        credential_expires_at: Timestamp,
        turn_credential: TurnCredential,
        renewal_token: Bytes,
        renewal_token_expires_at: Timestamp,
    ) {
        let mut guard = self.inner.write().await;
        guard.snapshot.credential = credential;
        guard.snapshot.credential_expires_at = credential_expires_at;
        guard.snapshot.turn_credential = turn_credential;
        guard.snapshot.renewal_token = renewal_token;
        guard.snapshot.renewal_token_expires_at = renewal_token_expires_at;
    }

    /// Transition to `Rebinding` — new outbound is gated.
    pub(crate) async fn enter_rebinding(&self) {
        self.inner.write().await.phase = SessionPhase::Rebinding;
    }

    /// Atomically commit a hard rebind.
    ///
    /// Replaces the entire snapshot with a new one, bumps the generation,
    /// and returns the *old* snapshot so callers can detach old handles.
    pub(crate) async fn commit_hard_rebind(
        &self,
        new_snapshot: SessionSnapshot,
    ) -> SessionSnapshot {
        let mut guard = self.inner.write().await;
        let old = guard.snapshot.clone();
        guard.snapshot = new_snapshot;
        guard.current_generation = guard.snapshot.generation;
        old
    }

    /// Transition back to `Active` after signaling reconnect succeeds.
    pub(crate) async fn set_active(&self) {
        self.inner.write().await.phase = SessionPhase::Active;
    }

    /// Transition to `RealmUnavailable` — no further renewal attempts.
    pub(crate) async fn set_realm_unavailable(&self) {
        self.inner.write().await.phase = SessionPhase::RealmUnavailable;
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Build a default empty snapshot (used in tests / placeholder contexts).
impl SessionSnapshot {
    pub fn empty_with_id(actor_id: ActrId, generation: u64) -> Self {
        Self {
            actor_id,
            credential: AIdCredential::default(),
            credential_expires_at: Timestamp::default(),
            turn_credential: TurnCredential::default(),
            renewal_token: Bytes::new(),
            renewal_token_expires_at: Timestamp::default(),
            generation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_actor_id() -> ActrId {
        ActrId {
            realm: actr_protocol::Realm { realm_id: 1 },
            serial_number: 42,
            r#type: actr_protocol::ActrType {
                manufacturer: "test".into(),
                name: "actor".into(),
                version: "1.0.0".into(),
            },
        }
    }

    #[tokio::test]
    async fn soft_renew_preserves_identity() {
        let snap = SessionSnapshot::empty_with_id(test_actor_id(), 1);
        let state = SessionState::new(snap);

        assert_eq!(state.actor_id().await.serial_number, 42);
        assert_eq!(state.generation().await, 1);
        assert_eq!(state.phase().await, SessionPhase::Active);

        let new_cred = AIdCredential {
            key_id: 99,
            ..Default::default()
        };
        let new_turn = TurnCredential {
            username: "renewed".into(),
            ..Default::default()
        };
        state
            .update_credentials(
                new_cred.clone(),
                Timestamp {
                    seconds: 200,
                    nanos: 0,
                },
                new_turn.clone(),
                Bytes::from_static(b"new-renewal-token-32-bytes!!!\0"),
                Timestamp {
                    seconds: 300,
                    nanos: 0,
                },
            )
            .await;

        // Identity unchanged.
        assert_eq!(state.actor_id().await.serial_number, 42);
        assert_eq!(state.generation().await, 1);
        // Credentials updated.
        assert_eq!(state.credential().await.key_id, 99);
        assert_eq!(state.turn_credential().await.username, "renewed");
    }

    #[tokio::test]
    async fn hard_rebind_bumps_generation() {
        let old = SessionSnapshot::empty_with_id(test_actor_id(), 1);
        let state = SessionState::new(old);

        let new_id = ActrId {
            serial_number: 99,
            ..test_actor_id()
        };
        let new_snap = SessionSnapshot::empty_with_id(new_id, 2);
        let _old_snap = state.commit_hard_rebind(new_snap).await;

        assert_eq!(state.actor_id().await.serial_number, 99);
        assert_eq!(state.generation().await, 2);
        assert_eq!(state.phase().await, SessionPhase::Active);
        assert!(!state.is_current_generation(1).await);
        assert!(state.is_current_generation(2).await);
    }

    #[tokio::test]
    async fn phase_transitions_cover_all_states() {
        let state = SessionState::new(SessionSnapshot::empty_with_id(test_actor_id(), 1));
        assert_eq!(state.phase().await, SessionPhase::Active);

        state.enter_rebinding().await;
        assert_eq!(state.phase().await, SessionPhase::Rebinding);

        state.set_active().await;
        assert_eq!(state.phase().await, SessionPhase::Active);

        state.set_realm_unavailable().await;
        assert_eq!(state.phase().await, SessionPhase::RealmUnavailable);
    }

    #[tokio::test]
    async fn sync_readers_return_some_when_unlocked() {
        let state = SessionState::new(SessionSnapshot::empty_with_id(test_actor_id(), 7));
        // No contending lock held → try_read succeeds.
        assert_eq!(state.actor_id_sync().map(|i| i.serial_number), Some(42));
        assert_eq!(state.generation_sync(), Some(7));
    }

    #[tokio::test]
    async fn snapshot_clone_and_credential_accessors_reflect_state() {
        let state = SessionState::new(SessionSnapshot::empty_with_id(test_actor_id(), 3));

        // Full snapshot clone carries the generation.
        assert_eq!(state.snapshot().await.generation, 3);

        // Default-constructed accessors on an empty snapshot.
        assert_eq!(state.credential_expires_at().await, Timestamp::default());
        assert!(state.renewal_token().await.is_empty());
        assert_eq!(state.renewal_token_expires_at().await, Timestamp::default());
        assert_eq!(state.credential().await, AIdCredential::default());
        assert_eq!(state.turn_credential().await, TurnCredential::default());
    }

    #[tokio::test]
    async fn commit_hard_rebind_returns_previous_snapshot() {
        let state = SessionState::new(SessionSnapshot::empty_with_id(test_actor_id(), 1));
        let prev = state
            .commit_hard_rebind(SessionSnapshot::empty_with_id(test_actor_id(), 5))
            .await;
        // Returned snapshot is the pre-rebind one (generation 1).
        assert_eq!(prev.generation, 1);
        // New current generation reflects the committed snapshot.
        assert_eq!(state.generation().await, 5);
        assert!(state.is_current_generation(5).await);
    }
}
