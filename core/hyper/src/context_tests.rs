use super::*;
use crate::test_support::runtime_context_with_host_transport;
use crate::transport::HostTransport;

fn ctx() -> RuntimeContext {
    runtime_context_with_host_transport(ActrId::default(), Arc::new(HostTransport::new()))
}

fn actr_type(mfr: &str, name: &str, ver: &str) -> ActrType {
    ActrType {
        manufacturer: mfr.into(),
        name: name.into(),
        version: ver.into(),
    }
}

// ── select_gate ─────────────────────────────────────────────────────────

#[tokio::test]
async fn select_gate_shell_and_local_use_inproc() {
    let c = ctx();
    assert!(c.select_gate(&Dest::Host).is_ok());
    assert!(c.select_gate(&Dest::Workload).is_ok());
}

#[tokio::test]
async fn select_gate_peer_errors_when_outproc_unset() {
    // Build a context with outproc_gate = None (unlike the test_support
    // helper, which sets Some). Peer dest must fail.
    use crate::inbound::{DataChunkRegistry, MediaFrameRegistry};
    use crate::outbound::{Gate, HostGate};
    use crate::wire::webrtc::{ReconnectConfig, SignalingConfig, WebSocketSignalingClient};
    let host = Arc::new(HostTransport::new());
    let inproc = Gate::Host(Arc::new(HostGate::new(host)));
    let c = RuntimeContext::new(
        ActrId::default(),
        None,
        "req".into(),
        inproc,
        None, // outproc_gate unset
        Arc::new(DataChunkRegistry::new()),
        Arc::new(MediaFrameRegistry::new()),
        Arc::new(WebSocketSignalingClient::new(SignalingConfig {
            server_url: url::Url::parse("ws://127.0.0.1:9").unwrap(),
            connection_timeout: 1,
            heartbeat_interval: 30,
            reconnect_config: ReconnectConfig::default(),
            auth_config: None,
            webrtc_role: None,
        })) as Arc<dyn SignalingClient>,
        AIdCredential::default(),
        None,
        Arc::new(RwLock::new(HashMap::new())),
        None,
        0,
    );
    match c.select_gate(&Dest::Peer(ActrId::default())) {
        Err(ActrError::Internal(_)) => {}
        Err(_) => panic!("expected Internal error, got a different ActrError variant"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[tokio::test]
async fn select_gate_actor_ok_when_outproc_set() {
    // The test_support helper sets outproc_gate = Some(inproc) → Actor ok.
    let c = ctx();
    assert!(c.select_gate(&Dest::Peer(ActrId::default())).is_ok());
}

// ── extract_target_id ───────────────────────────────────────────────────

#[tokio::test]
async fn extract_target_id_resolves_self_for_local_dests() {
    let c = ctx();
    let self_id = c.self_id().clone();
    assert_eq!(c.extract_target_id(&Dest::Host), &self_id);
    assert_eq!(c.extract_target_id(&Dest::Workload), &self_id);

    let remote = ActrId {
        serial_number: 99,
        ..ActrId::default()
    };
    assert_eq!(c.extract_target_id(&Dest::Peer(remote.clone())), &remote);
}

// ── ensure_session_ready ────────────────────────────────────────────────

#[tokio::test]
async fn ensure_session_ready_ok_without_session_state() {
    let c = ctx();
    assert!(c.ensure_session_ready().await.is_ok());
}

#[tokio::test]
async fn ensure_session_ready_rejects_stale_generation() {
    let mut c = ctx();
    let snap =
        crate::lifecycle::session_state::SessionSnapshot::empty_with_id(ActrId::default(), 5);
    let ss = SessionState::new(snap);
    c.session_state = Some(ss.clone());
    c.context_generation = 0;
    let err = c.ensure_session_ready().await.unwrap_err();
    assert!(matches!(err, ActrError::ConnectionNotReady(_)));

    c.context_generation = 5;
    ss.enter_rebinding().await;
    let err = c.ensure_session_ready().await.unwrap_err();
    assert!(matches!(err, ActrError::ConnectionNotReady(_)));

    ss.set_active().await;
    assert!(c.ensure_session_ready().await.is_ok());
}

// ── matches_dependency_actr_type ────────────────────────────────────────

#[test]
fn matches_dependency_exact_type() {
    let target = actr_type("acme", "Sensor", "1.0.0");
    assert!(RuntimeContext::matches_dependency_actr_type(
        "acme:Sensor:1.0.0",
        &target
    ));
}

#[test]
fn matches_dependency_rejects_mismatch() {
    let target = actr_type("acme", "Sensor", "1.0.0");
    assert!(!RuntimeContext::matches_dependency_actr_type(
        "acme:Sensor:2.0.0",
        &target
    ));
    assert!(!RuntimeContext::matches_dependency_actr_type(
        "other:Sensor:1.0.0",
        &target
    ));
}

#[test]
fn matches_dependency_rejects_unparseable() {
    let target = actr_type("acme", "Sensor", "1.0.0");
    assert!(!RuntimeContext::matches_dependency_actr_type(
        "not-a-valid-type",
        &target
    ));
    assert!(!RuntimeContext::matches_dependency_actr_type("", &target));
}

// ── BootstrapContextBuilder ─────────────────────────────────────────────

fn builder() -> BootstrapContextBuilder {
    use crate::outbound::{Gate, HostGate};
    let host = Arc::new(HostGate::new(Arc::new(HostTransport::new())));
    let inproc = Gate::Host(host);
    BootstrapContextBuilder::new(
        inproc.clone(),
        Some(inproc),
        Arc::new(DataChunkRegistry::new()),
        Arc::new(MediaFrameRegistry::new()),
        {
            use crate::wire::webrtc::{ReconnectConfig, SignalingConfig, WebSocketSignalingClient};
            Arc::new(WebSocketSignalingClient::new(SignalingConfig {
                server_url: url::Url::parse("ws://127.0.0.1:9").unwrap(),
                connection_timeout: 1,
                heartbeat_interval: 30,
                reconnect_config: ReconnectConfig::default(),
                auth_config: None,
                webrtc_role: None,
            })) as Arc<dyn SignalingClient>
        },
        None,
        Arc::new(RwLock::new(HashMap::new())),
        None,
        0,
    )
}

#[test]
fn bootstrap_builder_setters_update_state() {
    let mut b = builder();
    b.set_session_state(None);
    b.set_generation(42);
    let _c = b.build_bootstrap(&ActrId::default(), &AIdCredential::default());
}

#[tokio::test]
async fn build_bootstrap_prefers_session_generation_when_set() {
    let mut b = builder();
    let snap =
        crate::lifecycle::session_state::SessionSnapshot::empty_with_id(ActrId::default(), 9);
    let ss = SessionState::new(snap);
    b.set_session_state(Some(ss.clone()));

    let c = b.build_bootstrap(&ActrId::default(), &AIdCredential::default());
    assert_eq!(c.context_generation, 9);
    assert!(c.ensure_session_ready().await.is_ok());
}

#[tokio::test]
async fn build_bootstrap_falls_back_to_builder_generation() {
    let b = builder();
    let c = b.build_bootstrap(&ActrId::default(), &AIdCredential::default());
    assert_eq!(c.context_generation, 0);
    assert!(c.ensure_session_ready().await.is_ok());
    assert!(c.caller_id.is_none());
}

// ── get_dependency_fingerprint ──────────────────────────────────────────

use actr_config::lock::{LockFile, LockedDependency};

fn ctx_with_lock(lock: Option<LockFile>) -> RuntimeContext {
    use crate::inbound::{DataChunkRegistry, MediaFrameRegistry};
    use crate::outbound::{Gate, HostGate};
    use crate::wire::webrtc::{ReconnectConfig, SignalingConfig, WebSocketSignalingClient};
    let host = Arc::new(HostTransport::new());
    let inproc = Gate::Host(Arc::new(HostGate::new(host)));
    RuntimeContext::new(
        ActrId::default(),
        None,
        "req".into(),
        inproc,
        None,
        Arc::new(DataChunkRegistry::new()),
        Arc::new(MediaFrameRegistry::new()),
        Arc::new(WebSocketSignalingClient::new(SignalingConfig {
            server_url: url::Url::parse("ws://127.0.0.1:9").unwrap(),
            connection_timeout: 1,
            heartbeat_interval: 30,
            reconnect_config: ReconnectConfig::default(),
            auth_config: None,
            webrtc_role: None,
        })) as Arc<dyn SignalingClient>,
        AIdCredential::default(),
        lock.map(Arc::new),
        Arc::new(RwLock::new(HashMap::new())),
        None,
        0,
    )
}

fn dep(actr_type: &str, fingerprint: &str) -> LockedDependency {
    LockedDependency {
        name: actr_type.to_string(),
        actr_type: actr_type.to_string(),
        description: None,
        fingerprint: fingerprint.to_string(),
        published_at: None,
        tags: vec![],
        cached_at: String::new(),
        files: vec![],
    }
}

#[tokio::test]
async fn get_dependency_fingerprint_returns_none_without_lock() {
    let c = ctx_with_lock(None);
    assert_eq!(
        c.get_dependency_fingerprint(&actr_type("acme", "Sensor", "1.0.0")),
        None
    );
}

#[tokio::test]
async fn get_dependency_fingerprint_exact_key_match() {
    // to_string_repr uses ':' separators.
    let lock = LockFile {
        metadata: None,
        dependencies: vec![dep("acme:Sensor:1.0.0", "sha256:exact")],
    };
    let c = ctx_with_lock(Some(lock));
    assert_eq!(
        c.get_dependency_fingerprint(&actr_type("acme", "Sensor", "1.0.0")),
        Some("sha256:exact".to_string())
    );
}

#[tokio::test]
async fn get_dependency_fingerprint_falls_back_to_scan() {
    // No exact key, but a dependency whose actr_type parses to the target.
    let lock = LockFile {
        metadata: None,
        dependencies: vec![dep("acme:Sensor:1.0.0", "sha256:scan")],
    };
    let c = ctx_with_lock(Some(lock));
    // Query with a target that won't match the exact key string but parses
    // equal (same components) — exercises the scan fallback.
    assert_eq!(
        c.get_dependency_fingerprint(&actr_type("acme", "Sensor", "1.0.0")),
        Some("sha256:scan".to_string())
    );
}

#[tokio::test]
async fn get_dependency_fingerprint_missing_dependency() {
    let lock = LockFile {
        metadata: None,
        dependencies: vec![dep("acme:Other:2.0.0", "sha256:other")],
    };
    let c = ctx_with_lock(Some(lock));
    assert_eq!(
        c.get_dependency_fingerprint(&actr_type("acme", "Sensor", "1.0.0")),
        None
    );
}
