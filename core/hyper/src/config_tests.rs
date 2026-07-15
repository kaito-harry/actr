use super::*;
use crate::verify::StaticTrust;

fn stub_config(data_dir: &str) -> HyperConfig {
    HyperConfig::new(data_dir, Arc::new(StaticTrust::dev_only()))
}

#[test]
fn resolve_basic_template() {
    let config = stub_config("/var/lib/actr");
    let resolver = NamespaceResolver::new(&config, "abc123")
        .unwrap()
        .with_actor_type("acme", "Sensor", "1.0.0");

    let path = resolver.resolve("{data_dir}/{actr_type}").unwrap();
    assert_eq!(path, PathBuf::from("/var/lib/actr/acme/Sensor/1.0.0"));
}

#[test]
fn resolve_missing_var_returns_error() {
    let config = stub_config("/tmp");
    let resolver = NamespaceResolver::new(&config, "id1").unwrap();
    let result = resolver.resolve("{data_dir}/{realm_id}");
    assert!(matches!(result, Err(HyperError::TemplateVariable(_))));
}

#[test]
fn resolve_with_realm() {
    let config = stub_config("/tmp");
    let resolver = NamespaceResolver::new(&config, "id1")
        .unwrap()
        .with_actor_type("acme", "Worker", "2.0")
        .with_realm(42);
    let path = resolver
        .resolve("{data_dir}/{actr_type}/{realm_id}")
        .unwrap();
    assert_eq!(path, PathBuf::from("/tmp/acme/Worker/2.0/42"));
}

/// Minimal actr.toml body shared across tests. Callers append a
/// `[hyper]` section (with an escaped `data_dir` pointing into the
/// test's tempdir) so `node_from_config_file` never writes into the
/// user's real `~/.actr`.
const BASE_CONFIG_TOML: &str = r#"
edition = 1
[signaling]
url = "ws://localhost:8081/signaling/ws"
[ais_endpoint]
url = "http://localhost:8081/ais"
[deployment]
realm_id = 1
"#;

#[tokio::test]
async fn node_from_config_file_dev_only_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("actr.toml");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    std::fs::write(
        &path,
        format!(
            "{BASE_CONFIG_TOML}[hyper]\ndata_dir = \"{data_dir}\"\n\
                 [hyper.trust]\nkind = \"dev_only\"\n"
        ),
    )
    .unwrap();
    let _node = node_from_config_file(&path)
        .await
        .expect("dev_only trust should be accepted");
}

#[tokio::test]
async fn node_from_config_file_missing_trust_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("actr.toml");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    std::fs::write(
        &path,
        format!("{BASE_CONFIG_TOML}[hyper]\ndata_dir = \"{data_dir}\"\n"),
    )
    .unwrap();
    let result = node_from_config_file(&path).await;
    let err = match result {
        Ok(_) => panic!("missing trust must fail"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("no `[hyper.trust]`") && msg.contains("dev_only"),
        "error should direct user to the dev_only opt-in, got: {msg}"
    );
}

#[tokio::test]
async fn node_from_config_file_accepts_top_level_registry_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("actr.toml");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    std::fs::write(
        &path,
        format!(
            "{BASE_CONFIG_TOML}[hyper]\ndata_dir = \"{data_dir}\"\n\
                 [[trust]]\nkind = \"registry\"\nendpoint = \"http://localhost:8081/ais\"\n"
        ),
    )
    .unwrap();
    let _node = node_from_config_file(&path)
        .await
        .expect("top-level [[trust]] registry anchor should be accepted");
}

#[tokio::test]
async fn node_from_config_file_allows_linked_actor_type_override() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("actr.toml");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    std::fs::write(
        &path,
        format!(
            "{BASE_CONFIG_TOML}[hyper]\ndata_dir = \"{data_dir}\"\n\
                 [hyper.trust]\nkind = \"dev_only\"\n"
        ),
    )
    .unwrap();

    let actor_type = actr_protocol::ActrType {
        manufacturer: "acme".to_string(),
        name: "EchoApp".to_string(),
        version: "0.1.0".to_string(),
    };

    let node = node_from_config_file(&path)
        .await
        .expect("dev_only trust should be accepted")
        .with_actor_type(actor_type.clone());

    assert_eq!(node.runtime_config().actr_type(), &actor_type);
}

// ── resolve_path ────────────────────────────────────────────────────────

#[test]
fn resolve_path_keeps_absolute_and_joins_relative() {
    let base = Path::new("/etc/app");
    // Absolute path is returned verbatim, ignoring base.
    let abs = resolve_path(base, "/var/keys/pub.json");
    assert_eq!(abs, PathBuf::from("/var/keys/pub.json"));

    // Relative path is joined under base.
    let rel = resolve_path(base, "keys/pub.json");
    assert_eq!(rel, PathBuf::from("/etc/app/keys/pub.json"));
}

// ── load_static_pubkey_bytes (b64 + file branches) ──────────────────────

fn b64_of(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[test]
fn load_pubkey_from_valid_b64_32_bytes() {
    let raw = [7u8; 32];
    let got = load_static_pubkey_bytes(None, Some(b64_of(&raw))).unwrap();
    assert_eq!(got, raw.to_vec());
}

#[test]
fn load_pubkey_invalid_b64_errors() {
    let err = load_static_pubkey_bytes(None, Some("not!!valid!!b64@@".to_string())).unwrap_err();
    assert!(err.to_string().contains("pubkey_b64"));
}

#[test]
fn load_pubkey_b64_wrong_length_errors() {
    // 16 bytes is not an Ed25519 public key.
    let err = load_static_pubkey_bytes(None, Some(b64_of(&[0u8; 16]))).unwrap_err();
    assert!(err.to_string().contains("32 bytes"));
}

#[test]
fn load_pubkey_neither_file_nor_b64_errors() {
    let err = load_static_pubkey_bytes(None, None).unwrap_err();
    assert!(err.to_string().contains("pubkey_file"));
}

#[test]
fn load_pubkey_from_json_file_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let raw = [9u8; 32];
    let json = format!("{{\"public_key\": \"{}\"}}", b64_of(&raw));
    let path = dir.path().join("key.json");
    std::fs::write(&path, json).unwrap();

    let got = load_static_pubkey_bytes(Some(path.clone()), None).unwrap();
    assert_eq!(got, raw.to_vec());
}

#[test]
fn load_pubkey_file_missing_public_key_field_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("key.json");
    std::fs::write(&path, "{\"other\": \"value\"}").unwrap();

    let err = load_static_pubkey_bytes(Some(path.clone()), None).unwrap_err();
    assert!(err.to_string().contains("public_key"));
}

#[test]
fn load_pubkey_file_wrong_key_length_errors() {
    let dir = tempfile::tempdir().unwrap();
    let json = format!("{{\"public_key\": \"{}\"}}", b64_of(&[0u8; 10]));
    let path = dir.path().join("key.json");
    std::fs::write(&path, json).unwrap();

    let err = load_static_pubkey_bytes(Some(path), None).unwrap_err();
    assert!(err.to_string().contains("32-byte"));
}

#[test]
fn load_pubkey_file_not_json_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("key.json");
    std::fs::write(&path, "this is not json {{{").unwrap();

    let err = load_static_pubkey_bytes(Some(path), None).unwrap_err();
    assert!(err.to_string().contains("not valid JSON"));
}

// ── HyperConfig builders & Debug ────────────────────────────────────────

#[test]
fn hyper_config_builders_apply_overrides() {
    let cfg = stub_config("/data")
        .with_storage_template("{data_dir}/custom")
        .with_credential_expiry_warning(Duration::from_secs(120))
        .with_mailbox_backpressure_threshold(Some(2048));

    assert_eq!(cfg.storage_path_template, "{data_dir}/custom");
    assert_eq!(cfg.credential_expiry_warning, Duration::from_secs(120));
    assert_eq!(cfg.mailbox_backpressure_threshold, Some(2048));
    assert_eq!(cfg.resolved_mailbox_backpressure_threshold(), 2048);
}

#[test]
fn resolved_mailbox_threshold_defaults_when_unset() {
    let cfg = stub_config("/data");
    assert_eq!(
        cfg.resolved_mailbox_backpressure_threshold(),
        DEFAULT_MAILBOX_BACKPRESSURE_THRESHOLD
    );
}

#[test]
fn hyper_config_debug_formats_without_panic() {
    let cfg = stub_config("/data");
    let s = format!("{cfg:?}");
    assert!(s.contains("HyperConfig"));
    assert!(s.contains("data_dir"));
    assert!(s.contains("credential_expiry_warning"));
}

// ── NamespaceResolver env-var substitution ──────────────────────────────

#[test]
fn resolve_env_var_substitution() {
    // Unique var name to avoid cross-test coupling.
    // env (de)mutation is `unsafe` in edition 2024.
    unsafe { std::env::set_var("ACTR_COV_TEST_VAR", "envvalue") };
    let config = stub_config("/tmp");
    let resolver = NamespaceResolver::new(&config, "id").unwrap();
    let path = resolver
        .resolve("{env.ACTR_COV_TEST_VAR}/data")
        .expect("env var should substitute");
    assert_eq!(path, PathBuf::from("envvalue/data"));
    unsafe { std::env::remove_var("ACTR_COV_TEST_VAR") };
}

#[test]
fn resolve_missing_env_var_errors() {
    let config = stub_config("/tmp");
    let resolver = NamespaceResolver::new(&config, "id").unwrap();
    let err = resolver
        .resolve("{env.ACTR_DEFINITELY_MISSING_VAR_XYZ}")
        .unwrap_err();
    assert!(matches!(err, HyperError::TemplateVariable(_)));
    assert!(
        err.to_string()
            .contains("env.ACTR_DEFINITELY_MISSING_VAR_XYZ")
    );
}

#[test]
fn resolve_plain_string_without_placeholders() {
    let config = stub_config("/tmp");
    let resolver = NamespaceResolver::new(&config, "id").unwrap();
    let path = resolver.resolve("/plain/path/no/vars").unwrap();
    assert_eq!(path, PathBuf::from("/plain/path/no/vars"));
}

// ── Dispatch concurrency (B2) ────────────────────────────────────────────────

#[test]
fn dispatch_concurrency_default_is_on() {
    // Strategy A: the gate defaults *on*. This is safe for keyless actors
    // because the node never spawns a scheduler for them (proven in
    // `lifecycle::node_tests::scheduler_engaged_*`); default-on only unlocks
    // concurrency once a method declares a conflict key.
    let cfg = DispatchConcurrency::default();
    assert!(cfg.enabled);
    assert_eq!(cfg.budget, 8);
    assert_eq!(cfg.queue_cap, 256);
    assert!(cfg.dispatch_timeout.is_none());
}

#[test]
fn hyper_config_dispatch_concurrency_none_resolves_to_default_on() {
    let config = stub_config("/tmp");
    assert!(config.dispatch_concurrency.is_none());
    // With no explicit config, the resolved value is the default — now on.
    let resolved = config.resolved_dispatch_concurrency();
    assert!(resolved.enabled);
}

#[test]
fn explicit_disabled_still_opts_out_of_default_on() {
    // The `Option` field is unchanged, so a caller can still force the
    // fully-serial B1 runner by passing an explicit disabled config.
    let config = stub_config("/tmp").with_dispatch_concurrency(Some(DispatchConcurrency {
        enabled: false,
        ..DispatchConcurrency::default()
    }));
    let resolved = config.resolved_dispatch_concurrency();
    assert!(!resolved.enabled);
}

#[test]
fn with_dispatch_concurrency_builder_sets_field() {
    let config = stub_config("/tmp").with_dispatch_concurrency(Some(DispatchConcurrency {
        enabled: true,
        budget: 4,
        queue_cap: 32,
        dispatch_timeout: None,
    }));
    let dc = config.dispatch_concurrency.expect("set");
    assert!(dc.enabled);
    assert_eq!(dc.budget, 4);
    assert_eq!(dc.queue_cap, 32);
}

#[test]
fn env_escape_hatch_parsing() {
    assert!(dispatch_serial_env_override(Some("1")));
    assert!(dispatch_serial_env_override(Some("true")));
    assert!(dispatch_serial_env_override(Some("TRUE")));
    assert!(dispatch_serial_env_override(Some(" 1 ")));
    assert!(dispatch_serial_env_override(Some("yes")));
    assert!(!dispatch_serial_env_override(Some("0")));
    assert!(!dispatch_serial_env_override(Some("false")));
    assert!(!dispatch_serial_env_override(Some("")));
    assert!(!dispatch_serial_env_override(None));
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn wasm_runtime_limits_reject_zero_security_bounds() {
    let limits = WasmRuntimeLimits {
        max_outstanding_invocations: 0,
        ..WasmRuntimeLimits::default()
    };
    let error = limits.validate().expect_err("zero quota must be rejected");
    assert!(error.to_string().contains("max_outstanding_invocations"));
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn wasm_runtime_limits_reject_inconsistent_memory_budget() {
    let limits = WasmRuntimeLimits {
        max_linear_memory: 128,
        max_total_linear_memory: 64,
        ..WasmRuntimeLimits::default()
    };
    let error = limits
        .validate()
        .expect_err("per-Store memory must fit aggregate memory");
    assert!(error.to_string().contains("max_total_linear_memory"));
}
