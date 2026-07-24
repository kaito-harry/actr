//! AIS 集成测试（自举式）
//!
//! 在测试进程内启动临时 Signer gRPC 服务，验证 AIS 的签发与校验链路。

use actr_protocol::{
    ActrType, Realm, RegisterAuthMode, RegisterRequest, RegisterResponse, RenewCredentialRequest,
    RenewCredentialResponse, register_response, renew_credential_response,
};
use ais::signer_client_wrapper::create_signer_client;
use ais::{
    handlers::{AISState, create_router},
    issuer::{AIdIssuer, IssuerConfig},
};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use base64::Engine as _;
use nonce_auth::storage::MemoryStorage;
use platform::aid::credential::validator::AIdCredentialValidator;
use platform::config::signer::SignerClientConfig;
use platform::realm::{REALM_SECRET_HEADER, RealmSecretCheck, hash_realm_secret};
use prost::Message;
use serial_test::serial;
use signer::{GrpcClient, GrpcClientConfig, KeyStorage, SignerServiceConfig, create_grpc_service};
use std::net::TcpListener;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tower::ServiceExt;

struct TestEnv {
    issuer_temp_dir: TempDir,
    _signer_temp_dir: TempDir,
    signer_handle: Option<JoinHandle<()>>,
    signer_shutdown_tx: Option<oneshot::Sender<()>>,
    signer_config: SignerClientConfig,
    shared_key: String,
}

impl TestEnv {
    async fn shutdown_signer(&mut self) {
        if let Some(tx) = self.signer_shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.signer_handle.take() {
            let _ = handle.await;
        }
    }
}

async fn start_embedded_signer(
    shared_key: &str,
    sqlite_path: &Path,
) -> (String, JoinHandle<()>, oneshot::Sender<()>) {
    let service_config = SignerServiceConfig::default();
    let storage = KeyStorage::from_config(
        &service_config.storage,
        signer::KeyEncryptor::no_encryption(),
        sqlite_path,
    )
    .await
    .expect("Failed to create Signer storage");

    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral port");
    let addr = listener.local_addr().expect("Failed to get local addr");
    drop(listener);

    let service = create_grpc_service(
        storage,
        MemoryStorage::new(),
        shared_key.to_string(),
        service_config.tolerance_seconds,
    );

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        if let Err(err) = Server::builder()
            .add_service(service)
            .serve_with_shutdown(addr, async move {
                let _ = shutdown_rx.await;
            })
            .await
        {
            panic!("Embedded Signer server failed: {err}");
        }
    });

    let endpoint = format!("http://{addr}");

    // Wait until gRPC health check is reachable.
    let mut last_error = String::new();
    for _ in 0..40 {
        let cfg = GrpcClientConfig {
            endpoint: endpoint.clone(),
            actrix_shared_key: shared_key.to_string(),
            timeout_seconds: 2,
            enable_tls: false,
            tls_domain: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
        };

        match GrpcClient::new(&cfg).await {
            Ok(mut client) => match client.health_check().await {
                Ok(status) if status == "healthy" => return (endpoint, handle, shutdown_tx),
                Ok(status) => last_error = format!("unexpected Signer health status: {status}"),
                Err(err) => last_error = err.to_string(),
            },
            Err(err) => last_error = err.to_string(),
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("Embedded Signer did not become healthy in time: {last_error}");
}

async fn setup_test_environment() -> TestEnv {
    use std::sync::OnceLock;
    static DB_DIR: OnceLock<TempDir> = OnceLock::new();

    let issuer_temp_dir = TempDir::new().expect("Failed to create issuer temp dir");
    let signer_temp_dir = TempDir::new().expect("Failed to create signer temp dir");
    let shared_key = "test-shared-key".to_string();
    let (endpoint, signer_handle, signer_shutdown_tx) =
        start_embedded_signer(&shared_key, signer_temp_dir.path()).await;

    // Initialize the global database once with a persistent temp dir.
    // The OnceLock ensures the TempDir (and its SQLite file) lives for the
    // entire process, avoiding "unable to open database file" in serial tests.
    let db_dir = DB_DIR.get_or_init(|| TempDir::new().expect("Failed to create DB temp dir"));
    if !platform::storage::db::is_database_initialized() {
        platform::storage::db::set_db_path(db_dir.path())
            .await
            .expect("Failed to initialize test database");
    }

    let signer_config = SignerClientConfig {
        endpoint,
        timeout_seconds: 10,
        enable_tls: false,
        tls_domain: None,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    };

    TestEnv {
        issuer_temp_dir,
        _signer_temp_dir: signer_temp_dir,
        signer_handle: Some(signer_handle),
        signer_shutdown_tx: Some(signer_shutdown_tx),
        signer_config,
        shared_key,
    }
}

fn default_issuer_config(temp_dir: &TempDir) -> IssuerConfig {
    IssuerConfig {
        token_ttl_secs: 3600,
        renewal_token_ttl_secs: 24 * 60 * 60,
        renewal_rotation_window_secs: 60 * 60,
        renewal_token_secret: vec![0; 32],
        signaling_heartbeat_interval_secs: 30,
        key_refresh_interval_secs: 3600,
        key_storage_file: temp_dir.path().join("issuer_keys.db"),
        enable_periodic_rotation: false,
        key_rotation_interval_secs: 86400,
        turn_secret: "test-turn-secret".to_string(),
        sqlite_path: temp_dir.path().to_path_buf(),
    }
}

fn linked_register_request() -> RegisterRequest {
    RegisterRequest {
        actr_type: ActrType {
            manufacturer: "linked-src".to_string(),
            name: "source-workload".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: None,
        mfr_signature: None,
        target: None,
        auth_mode: Some(RegisterAuthMode::Linked as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    }
}

fn package_register_request(
    manufacturer: &str,
    name: &str,
    version: &str,
    realm_id: u32,
    target: &str,
    manifest: Vec<u8>,
    manifest_signature: Option<Vec<u8>>,
) -> RegisterRequest {
    RegisterRequest {
        actr_type: ActrType {
            manufacturer: manufacturer.to_string(),
            name: name.to_string(),
            version: version.to_string(),
        },
        realm: Realm { realm_id },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest)),
        mfr_signature: manifest_signature.map(prost::bytes::Bytes::from),
        target: Some(target.to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    }
}

async fn create_test_router(env: &TestEnv) -> Router {
    create_router(AISState::new(create_test_issuer(env).await))
}

async fn create_test_issuer(env: &TestEnv) -> AIdIssuer {
    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer")
}

async fn seed_realm(realm_id: u32, name: &str, secret: Option<&str>) {
    let pool = platform::storage::db::get_database().get_pool();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let secret_current = secret.map(hash_realm_secret).unwrap_or_default();

    sqlx::query(
        "INSERT OR REPLACE INTO realm (id, name, status, enabled, created_at, secret_current)
         VALUES (?, ?, 'Active', 1, ?, ?)",
    )
    .bind(realm_id as i64)
    .bind(name)
    .bind(now)
    .bind(secret_current)
    .execute(pool)
    .await
    .expect("seed realm");
}

fn test_registry_manifest(manufacturer: &str, name: &str, version: &str, target: &str) -> Vec<u8> {
    format!(
        r#"manufacturer = "{manufacturer}"
name = "{name}"
version = "{version}"

[binary]
path = "bin/actor.wasm"
target = "{target}"
hash = "0000000000000000000000000000000000000000000000000000000000000000"
"#,
    )
    .into_bytes()
}

async fn seed_active_package(
    manufacturer: &str,
    name: &str,
    version: &str,
    target: &str,
) -> Vec<u8> {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let signing_key = SigningKey::generate(&mut OsRng);
    seed_active_package_with_key(manufacturer, name, version, target, &signing_key).await
}

async fn seed_active_package_with_key(
    manufacturer: &str,
    name: &str,
    version: &str,
    target: &str,
    signing_key: &ed25519_dalek::SigningKey,
) -> Vec<u8> {
    let pool = platform::storage::db::get_database().get_pool();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let (mfr_id, signing_key_id) = seed_mfr_with_key(pool, manufacturer, signing_key).await;
    let (manifest, signature) =
        build_signed_manifest(signing_key, manufacturer, name, version, target);
    let manifest_str = std::str::from_utf8(&manifest).expect("test manifest is UTF-8");
    let signature_b64 = base64::prelude::BASE64_STANDARD.encode(signature);

    sqlx::query(
        "INSERT OR REPLACE INTO mfr_package
         (mfr_id, manufacturer, name, version, type_str, target, manifest, signature, signing_key_id, status, published_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)",
    )
    .bind(mfr_id)
    .bind(manufacturer)
    .bind(name)
    .bind(version)
    .bind(format!("{manufacturer}:{name}:{version}"))
    .bind(target)
    .bind(manifest_str)
    .bind(signature_b64)
    .bind(signing_key_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("seed mfr package");

    manifest
}

async fn post_register(
    app: Router,
    request: RegisterRequest,
    realm_secret: Option<&str>,
) -> RegisterResponse {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/register")
        .header("content-type", "application/protobuf")
        .header("x-real-ip", "127.0.0.1");
    if let Some(secret) = realm_secret {
        builder = builder.header(REALM_SECRET_HEADER, secret);
    }

    let response = app
        .oneshot(builder.body(Body::from(request.encode_to_vec())).unwrap())
        .await
        .expect("register route response");
    let status = response.status();

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("register response body");
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected /register status {status}: {}",
        String::from_utf8_lossy(&body)
    );
    RegisterResponse::decode(body).expect("decode register response")
}

#[tokio::test]
#[serial]
async fn test_register_route_linked_with_realm_secret_succeeds() {
    let env = setup_test_environment().await;
    let realm_secret = "linked-http-route-secret";
    let realm_id = 22001;
    seed_realm(realm_id, "linked-http-route", Some(realm_secret)).await;
    let app = create_test_router(&env).await;

    let mut request = linked_register_request();
    request.realm = Realm { realm_id };
    let response = post_register(app, request, Some(realm_secret)).await;

    match response.result.expect("result") {
        register_response::Result::Success(ok) => {
            assert_eq!(ok.actr_id.realm.realm_id, realm_id);
            assert_eq!(ok.actr_id.r#type.name, "source-workload");
        }
        register_response::Result::Error(err) => {
            panic!("linked /register with realm secret should succeed: {err:?}")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_register_route_package_with_mfr_identity_succeeds() {
    let env = setup_test_environment().await;
    let realm_id = 22002;
    seed_realm(realm_id, "package-http-route", None).await;
    let manifest_raw =
        seed_active_package("httppkg", "PackagedService", "1.0.0", "wasm32-wasip1").await;
    let app = create_test_router(&env).await;

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "httppkg".to_string(),
            name: "PackagedService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_raw)),
        mfr_signature: None,
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    let response = post_register(app, request, None).await;

    match response.result.expect("result") {
        register_response::Result::Success(ok) => {
            assert_eq!(ok.actr_id.realm.realm_id, realm_id);
            assert_eq!(ok.actr_id.r#type.manufacturer, "httppkg");
        }
        register_response::Result::Error(err) => {
            panic!("package /register with MFR identity should succeed: {err:?}")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_register_route_unspecified_auth_mode_uses_package_identity() {
    let env = setup_test_environment().await;
    let realm_id = 22004;
    seed_realm(realm_id, "unspecified-http-route", None).await;
    let manifest_raw =
        seed_active_package("legacyhttp", "LegacyService", "1.0.0", "wasm32-wasip1").await;
    let app = create_test_router(&env).await;

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "legacyhttp".to_string(),
            name: "LegacyService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_raw)),
        mfr_signature: None,
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: None,
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    let response = post_register(app, request, None).await;

    match response.result.expect("result") {
        register_response::Result::Success(ok) => {
            assert_eq!(ok.actr_id.realm.realm_id, realm_id);
            assert_eq!(ok.actr_id.r#type.manufacturer, "legacyhttp");
        }
        register_response::Result::Error(err) => {
            panic!("omitted auth_mode should remain package-compatible: {err:?}")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_register_route_published_package_without_manifest_raw_is_rejected() {
    let env = setup_test_environment().await;
    let realm_id = 22005;
    seed_realm(realm_id, "published-package-missing-manifest", None).await;
    seed_active_package(
        "missingmanifest",
        "PublishedService",
        "1.0.0",
        "wasm32-wasip1",
    )
    .await;
    let app = create_test_router(&env).await;

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "missingmanifest".to_string(),
            name: "PublishedService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: None,
        mfr_signature: None,
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    let response = post_register(app, request, None).await;

    match response.result.expect("result") {
        register_response::Result::Error(err) => {
            assert_eq!(err.code, 400);
        }
        register_response::Result::Success(_) => {
            panic!("published package registration without manifest_raw should be rejected")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_register_route_published_package_without_target_is_rejected() {
    let env = setup_test_environment().await;
    let realm_id = 22006;
    seed_realm(realm_id, "published-package-missing-target", None).await;
    let manifest_raw = seed_active_package(
        "missingtarget",
        "PublishedService",
        "1.0.0",
        "wasm32-wasip1",
    )
    .await;
    let app = create_test_router(&env).await;

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "missingtarget".to_string(),
            name: "PublishedService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_raw)),
        mfr_signature: None,
        target: None,
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    let response = post_register(app, request, None).await;

    match response.result.expect("result") {
        register_response::Result::Error(err) => {
            assert_eq!(err.code, 400);
        }
        register_response::Result::Success(_) => {
            panic!("published package registration without target should be rejected")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_published_package_manifest_hash_mismatch_does_not_fall_back_to_path2() {
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    seed_realm(22007, "published-package-manifest-mismatch", None).await;
    let key = SigningKey::generate(&mut OsRng);
    seed_active_package_with_key(
        "publishedmismatch",
        "MismatchService",
        "1.0.0",
        "wasm32-wasip1",
        &key,
    )
    .await;

    // This manifest is validly signed and would pass Path 2, but it is not the
    // same manifest registered in mfr_package. A published package hash mismatch
    // must be rejected immediately instead of falling through to Path 2.
    let (mut manifest_bytes, _) = build_signed_manifest(
        &key,
        "publishedmismatch",
        "MismatchService",
        "1.0.0",
        "wasm32-wasip1",
    );
    manifest_bytes.extend_from_slice(b"\n# different, but still valid, manifest bytes\n");
    let sig_bytes = key.sign(&manifest_bytes).to_bytes().to_vec();
    let mut request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "publishedmismatch".to_string(),
            name: "MismatchService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 22007 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_bytes.clone())),
        mfr_signature: Some(prost::bytes::Bytes::from(sig_bytes)),
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    sign_manufacturer_request_at(
        &mut request,
        &key,
        &manifest_bytes,
        test_unix_now_secs(),
        vec![0x55; 32],
    );

    let response = issuer.issue_credential(&request).await.expect("issue");
    match response.result.expect("result") {
        register_response::Result::Error(err) => {
            assert_eq!(err.code, 403);
            assert!(err.message.contains("manufacturer not verified"));
        }
        register_response::Result::Success(_) => {
            panic!("published package manifest hash mismatch should not fall back to Path 2")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_expired_mfr_key_allows_existing_package_and_path2_registration() {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let issuer = create_test_issuer(&env).await;
    let realm_id = 22008;
    seed_realm(realm_id, "expired-mfr-runtime-verification", None).await;

    let pool = platform::storage::db::get_database().get_pool();
    let key = SigningKey::generate(&mut OsRng);
    let published_manifest = seed_active_package_with_key(
        "expiredruntime",
        "ExistingService",
        "1.0.0",
        "wasm32-wasip1",
        &key,
    )
    .await;
    sqlx::query("UPDATE mfr SET key_expires_at = ? WHERE name = ?")
        .bind(test_unix_now_secs() as i64 - 1)
        .bind("expiredruntime")
        .execute(pool)
        .await
        .expect("expire MFR publish authority");

    let path1_request = package_register_request(
        "expiredruntime",
        "ExistingService",
        "1.0.0",
        realm_id,
        "wasm32-wasip1",
        published_manifest,
        None,
    );
    let path1_response = issuer
        .issue_credential(&path1_request)
        .await
        .expect("issue Path 1 credential");
    assert!(
        matches!(
            &path1_response.result,
            Some(register_response::Result::Success(_))
        ),
        "natural key expiry must not invalidate an existing package: {path1_response:?}"
    );

    let (path2_manifest, path2_signature) = build_signed_manifest(
        &key,
        "expiredruntime",
        "UnpublishedService",
        "1.0.0",
        "wasm32-wasip1",
    );
    let mut path2_request = package_register_request(
        "expiredruntime",
        "UnpublishedService",
        "1.0.0",
        realm_id,
        "wasm32-wasip1",
        path2_manifest.clone(),
        Some(path2_signature),
    );
    sign_manufacturer_request_at(
        &mut path2_request,
        &key,
        &path2_manifest,
        test_unix_now_secs(),
        vec![0x58; 32],
    );
    let path2_response = issuer
        .issue_credential(&path2_request)
        .await
        .expect("issue Path 2 credential");
    assert!(
        matches!(
            &path2_response.result,
            Some(register_response::Result::Success(_))
        ),
        "natural key expiry must not disable Path 2 verification: {path2_response:?}"
    );
}

#[tokio::test]
#[serial]
async fn test_revoked_published_package_does_not_fall_back_to_path2() {
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let issuer = create_test_issuer(&env).await;
    let realm_id = 22009;
    seed_realm(realm_id, "revoked-package-terminal", None).await;

    let pool = platform::storage::db::get_database().get_pool();
    let key = SigningKey::generate(&mut OsRng);
    let manifest = seed_active_package_with_key(
        "revokedterminal",
        "RevokedService",
        "1.0.0",
        "wasm32-wasip1",
        &key,
    )
    .await;
    sqlx::query(
        "UPDATE mfr_package SET status = 'revoked', revoked_at = ? \
         WHERE type_str = ? AND target = ?",
    )
    .bind(test_unix_now_secs() as i64)
    .bind("revokedterminal:RevokedService:1.0.0")
    .bind("wasm32-wasip1")
    .execute(pool)
    .await
    .expect("revoke package");

    let manifest_signature = key.sign(&manifest).to_bytes().to_vec();
    let mut request = package_register_request(
        "revokedterminal",
        "RevokedService",
        "1.0.0",
        realm_id,
        "wasm32-wasip1",
        manifest.clone(),
        Some(manifest_signature),
    );
    sign_manufacturer_request_at(
        &mut request,
        &key,
        &manifest,
        test_unix_now_secs(),
        vec![0x59; 32],
    );

    let response = issuer.issue_credential(&request).await.expect("issue");
    match response.result.expect("result") {
        register_response::Result::Error(err) => {
            assert_eq!(err.code, 403);
            assert!(err.message.contains("revoked"));
        }
        register_response::Result::Success(_) => {
            panic!("a revoked package must be terminal and must not fall back to Path 2")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_suspended_mfr_rejects_path1_and_path2() {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let issuer = create_test_issuer(&env).await;
    let realm_id = 22010;
    seed_realm(realm_id, "suspended-mfr-runtime-verification", None).await;

    let pool = platform::storage::db::get_database().get_pool();
    let key = SigningKey::generate(&mut OsRng);
    let published_manifest = seed_active_package_with_key(
        "suspendedruntime",
        "ExistingService",
        "1.0.0",
        "wasm32-wasip1",
        &key,
    )
    .await;
    sqlx::query("UPDATE mfr SET status = 'suspended' WHERE name = ?")
        .bind("suspendedruntime")
        .execute(pool)
        .await
        .expect("suspend MFR");

    let path1_request = package_register_request(
        "suspendedruntime",
        "ExistingService",
        "1.0.0",
        realm_id,
        "wasm32-wasip1",
        published_manifest,
        None,
    );
    let path1_response = issuer
        .issue_credential(&path1_request)
        .await
        .expect("issue Path 1 credential");
    assert!(
        matches!(
            &path1_response.result,
            Some(register_response::Result::Error(error)) if error.code == 403
        ),
        "suspended MFR must reject Path 1: {path1_response:?}"
    );

    let (path2_manifest, path2_signature) = build_signed_manifest(
        &key,
        "suspendedruntime",
        "UnpublishedService",
        "1.0.0",
        "wasm32-wasip1",
    );
    let mut path2_request = package_register_request(
        "suspendedruntime",
        "UnpublishedService",
        "1.0.0",
        realm_id,
        "wasm32-wasip1",
        path2_manifest.clone(),
        Some(path2_signature),
    );
    sign_manufacturer_request_at(
        &mut path2_request,
        &key,
        &path2_manifest,
        test_unix_now_secs(),
        vec![0x5A; 32],
    );
    let path2_response = issuer
        .issue_credential(&path2_request)
        .await
        .expect("issue Path 2 credential");
    assert!(
        matches!(
            &path2_response.result,
            Some(register_response::Result::Error(error)) if error.code == 403
        ),
        "suspended MFR must reject Path 2: {path2_response:?}"
    );
}

#[tokio::test]
#[serial]
async fn test_published_package_retired_key_survives_until_explicit_revocation() {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let issuer = create_test_issuer(&env).await;
    let realm_id = 22011;
    seed_realm(realm_id, "revoked-history-path1", None).await;

    let pool = platform::storage::db::get_database().get_pool();
    let old_key = SigningKey::generate(&mut OsRng);
    let manifest = seed_active_package_with_key(
        "revokedhistory",
        "ExistingService",
        "1.0.0",
        "wasm32-wasip1",
        &old_key,
    )
    .await;
    let (mfr_id, old_key_id): (i64, String) =
        sqlx::query_as("SELECT id, key_id FROM mfr WHERE name = ?")
            .bind("revokedhistory")
            .fetch_one(pool)
            .await
            .expect("load MFR current key");
    let old_public_key =
        base64::prelude::BASE64_STANDARD.encode(old_key.verifying_key().to_bytes());
    archive_key_to_history(pool, mfr_id, &old_key_id, &old_public_key, "retired").await;

    let new_key = SigningKey::generate(&mut OsRng);
    let new_key_id = actrix_mfr::crypto::compute_key_id(&new_key.verifying_key().to_bytes());
    let new_public_key =
        base64::prelude::BASE64_STANDARD.encode(new_key.verifying_key().to_bytes());
    sqlx::query("UPDATE mfr SET public_key = ?, key_id = ? WHERE id = ?")
        .bind(new_public_key)
        .bind(new_key_id)
        .bind(mfr_id)
        .execute(pool)
        .await
        .expect("rotate current MFR key");

    let request = package_register_request(
        "revokedhistory",
        "ExistingService",
        "1.0.0",
        realm_id,
        "wasm32-wasip1",
        manifest,
        None,
    );
    let retired_response = issuer
        .issue_credential(&request)
        .await
        .expect("issue with retired key");
    assert!(
        matches!(
            &retired_response.result,
            Some(register_response::Result::Success(_))
        ),
        "ordinary key retirement must not invalidate an existing package: {retired_response:?}"
    );

    sqlx::query("UPDATE mfr_key_history SET status = 'revoked' WHERE mfr_id = ? AND key_id = ?")
        .bind(mfr_id)
        .bind(&old_key_id)
        .execute(pool)
        .await
        .expect("revoke historical key");

    let response = issuer
        .issue_credential(&request)
        .await
        .expect("issue with revoked key");
    assert!(
        matches!(
            &response.result,
            Some(register_response::Result::Error(error)) if error.code == 403
        ),
        "key revocation must invalidate packages signed by that key: {response:?}"
    );
}

#[tokio::test]
#[serial]
async fn test_register_route_package_without_mfr_identity_is_still_rejected() {
    let env = setup_test_environment().await;
    let realm_id = 22003;
    seed_realm(realm_id, "package-http-route-missing-mfr", None).await;
    let app = create_test_router(&env).await;

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "missing-http-mfr".to_string(),
            name: "PackagedService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(test_registry_manifest(
            "missing-http-mfr",
            "PackagedService",
            "1.0.0",
            "wasm32-wasip1",
        ))),
        mfr_signature: None,
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    let response = post_register(app, request, None).await;

    match response.result.expect("result") {
        register_response::Result::Error(err) => {
            assert_eq!(err.code, 403);
            assert!(err.message.contains("manufacturer not verified"));
        }
        register_response::Result::Success(_) => {
            panic!("package /register without MFR identity should still be rejected")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_linked_registration_with_verified_realm_secret_succeeds() {
    let env = setup_test_environment().await;

    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let response = issuer
        .issue_credential_with_realm_secret_check(
            &linked_register_request(),
            Some(RealmSecretCheck::ValidCurrent),
        )
        .await
        .expect("issue linked credential");

    match response.result.expect("result") {
        register_response::Result::Success(ok) => {
            assert_eq!(ok.actr_id.r#type.name, "source-workload");
            assert_eq!(ok.actr_id.realm.realm_id, 1001);
        }
        register_response::Result::Error(err) => {
            panic!("linked registration with verified realm secret should succeed: {err:?}")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_linked_registration_without_verified_realm_secret_is_rejected() {
    let env = setup_test_environment().await;

    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let response = issuer
        .issue_credential_with_realm_secret_check(
            &linked_register_request(),
            Some(RealmSecretCheck::NotConfigured),
        )
        .await
        .expect("issue linked credential");

    match response.result.expect("result") {
        register_response::Result::Error(err) => {
            assert_eq!(err.code, 403);
            assert!(err.message.contains("manufacturer not verified"));
        }
        register_response::Result::Success(_) => {
            panic!("linked registration without verified realm secret should be rejected")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_package_registration_without_mfr_identity_is_still_rejected() {
    let env = setup_test_environment().await;

    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "missing-mfr-for-package-auth-test".to_string(),
            name: "package-workload".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(test_registry_manifest(
            "missing-mfr-for-package-auth-test",
            "package-workload",
            "1.0.0",
            "wasm32-wasip1",
        ))),
        mfr_signature: None,
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };

    let response = issuer
        .issue_credential(&request)
        .await
        .expect("issue package credential");

    match response.result.expect("result") {
        register_response::Result::Error(err) => {
            assert_eq!(err.code, 403);
            assert!(err.message.contains("manufacturer not verified"));
        }
        register_response::Result::Success(_) => {
            panic!("package registration without MFR identity should still be rejected")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_end_to_end_credential_flow() {
    let env = setup_test_environment().await;

    AIdCredentialValidator::init(env.issuer_temp_dir.path())
        .await
        .expect("Failed to initialize validator");

    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("Failed to create Signer gRPC client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("Failed to create issuer");

    // Seed database with MFR and package data so verify_mfr_identity path-1 passes
    let manifest_raw = seed_active_package("acme", "test-device", "1.0.0", "wasm32-wasip1").await;

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "acme".to_string(),
            name: "test-device".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_raw.clone())),
        mfr_signature: None,
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };

    let response = issuer
        .issue_credential(&request)
        .await
        .expect("Failed to issue credential");

    let register_ok = match response.result.expect("Response should contain result") {
        register_response::Result::Success(ok) => ok,
        register_response::Result::Error(err) => panic!("Expected success but got error: {err:?}"),
    };

    assert!(
        !register_ok.turn_credential.username.is_empty(),
        "TURN credential should be present"
    );
    assert!(
        register_ok.credential_expires_at.is_some(),
        "Credential expiry should be present"
    );
    assert_eq!(register_ok.actr_id.realm.realm_id, 1001);
    assert!(register_ok.actr_id.serial_number > 0);

    let (claims, _) = AIdCredentialValidator::check(&register_ok.credential, 1001)
        .await
        .expect("Token validation should succeed");
    assert_eq!(claims.realm_id, 1001);
    assert!(
        !claims.actor_id.is_empty(),
        "Actor ID should be present in claims"
    );
    assert!(
        claims.actor_id.contains(':') && claims.actor_id.contains('@'),
        "Actor ID format should include manufacturer/name and serial/realm separators"
    );

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    assert!(claims.expires_at > now);
    assert!(claims.expires_at <= now + 3600);

    let wrong_realm_result = AIdCredentialValidator::check(&register_ok.credential, 9999).await;
    assert!(
        wrong_realm_result.is_err(),
        "Validation should fail with mismatched realm_id"
    );

    // Issue and validate multiple credentials to verify stability.
    for idx in 0..5 {
        let req = RegisterRequest {
            actr_type: ActrType {
                manufacturer: "acme".to_string(),
                name: "test-device".to_string(),
                version: "1.0.0".to_string(),
            },
            realm: Realm { realm_id: 1001 },
            service_spec: None,
            acl: None,
            service: None,
            ws_address: None,
            manifest_raw: Some(prost::bytes::Bytes::from(manifest_raw.clone())),
            mfr_signature: None,
            target: Some("wasm32-wasip1".to_string()),
            auth_mode: Some(RegisterAuthMode::Package as i32),
            manufacturer_auth_signature: None,
            manufacturer_auth_signed_at: None,
            manufacturer_auth_nonce: None,
        };

        let rsp = issuer
            .issue_credential(&req)
            .await
            .unwrap_or_else(|e| panic!("Failed to issue credential {idx}: {e}"));
        let ok = match rsp.result.expect("Response should contain result") {
            register_response::Result::Success(ok) => ok,
            register_response::Result::Error(err) => {
                panic!("Expected success for token {idx}, got error: {err:?}")
            }
        };

        let (claims, _) = AIdCredentialValidator::check(&ok.credential, 1001)
            .await
            .unwrap_or_else(|e| panic!("Failed to validate credential {idx}: {e}"));
        assert_eq!(claims.realm_id, 1001);
    }
}

#[tokio::test]
#[serial]
async fn test_issuer_health_checks() {
    let env = setup_test_environment().await;

    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("Failed to create Signer gRPC client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("Failed to create issuer");

    issuer
        .check_database_health()
        .await
        .expect("Database health check should pass");
    issuer
        .check_key_cache_health()
        .await
        .expect("Key cache health check should pass");
}

#[tokio::test]
#[serial]
async fn test_issuer_rotate_key_updates_current_key() {
    let env = setup_test_environment().await;

    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("Failed to create Signer gRPC client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("Failed to create issuer");

    let key_before = issuer
        .get_current_key_id()
        .await
        .expect("current key before rotate");
    let rotated = issuer.rotate_key().await.expect("rotate key");
    let key_after = issuer
        .get_current_key_id()
        .await
        .expect("current key after rotate");

    assert_ne!(key_before, rotated, "rotate_key should change key id");
    assert_eq!(key_after, rotated, "current key should match rotated key");

    issuer
        .check_ks_health()
        .await
        .expect("Signer health should still pass after rotation");
    let cache = issuer
        .check_key_cache_health()
        .await
        .expect("key cache should remain healthy");
    assert_eq!(cache.key_id, rotated);
}

#[tokio::test]
#[serial]
async fn test_issuer_creation_fails_with_wrong_shared_key() {
    let env = setup_test_environment().await;

    let signer_client = create_signer_client(&env.signer_config, "wrong-shared-key")
        .await
        .expect("gRPC channel creation should succeed even with wrong secret");

    // With lazy Signer connection, issuer creation succeeds — auth failure happens on first use
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer creation succeeds with lazy Signer connection");

    // Credential issuance should return an error response due to wrong shared key
    let request = actr_protocol::RegisterRequest {
        actr_type: actr_protocol::ActrType {
            manufacturer: "acme".to_string(),
            name: "bad-key-test".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: actr_protocol::Realm { realm_id: 1 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: None,
        mfr_signature: None,
        target: None,
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    let resp = issuer
        .issue_credential(&request)
        .await
        .expect("issue_credential returns Ok wrapping the error");
    assert!(
        matches!(
            resp.result,
            Some(actr_protocol::register_response::Result::Error(_))
        ),
        "expected error result in register response with wrong shared key, got {:?}",
        resp.result
    );
}

#[tokio::test]
#[serial]
async fn test_issuer_check_ks_health_fails_after_ks_shutdown() {
    let mut env = setup_test_environment().await;

    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("Failed to create Signer gRPC client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("Failed to create issuer");

    env.shutdown_signer().await;

    for _ in 0..40 {
        match issuer.check_ks_health().await {
            Ok(()) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("Signer service unhealthy")
                        || msg.contains("Failed")
                        || msg.contains("transport")
                        || msg.contains("unavailable")
                        || msg.contains("connection"),
                    "unexpected Signer health error after shutdown: {msg}"
                );
                return;
            }
        }
    }

    panic!("issuer Signer health should fail after embedded Signer shutdown");
}

#[tokio::test]
#[serial]
async fn test_issuer_rotate_key_fails_when_ks_is_unavailable() {
    let mut env = setup_test_environment().await;

    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("Failed to create Signer gRPC client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("Failed to create issuer");

    env.shutdown_signer().await;

    for _ in 0..40 {
        match issuer.rotate_key().await {
            Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("KS unavailable")
                        || msg.contains("Failed")
                        || msg.contains("transport")
                        || msg.contains("connection"),
                    "unexpected rotate_key error after KS shutdown: {msg}"
                );
                return;
            }
        }
    }

    panic!("rotate_key should fail after embedded KS shutdown");
}

// ═══════════════════════════════════════════════════════════════════════════
// Path 2 tests: verify_mfr_identity with manifest_raw + mfr_signature
// ═══════════════════════════════════════════════════════════════════════════

/// Helper: build a signed manifest TOML string and its Ed25519 signature.
fn build_signed_manifest(
    signing_key: &ed25519_dalek::SigningKey,
    manufacturer: &str,
    name: &str,
    version: &str,
    target: &str,
) -> (Vec<u8>, Vec<u8>) {
    use ed25519_dalek::Signer;

    let key_id = actrix_mfr::crypto::compute_key_id(&signing_key.verifying_key().to_bytes());
    let manifest = format!(
        r#"manufacturer = "{manufacturer}"
name = "{name}"
version = "{version}"
signing_key_id = "{key_id}"

[binary]
path = "bin/actor.wasm"
target = "{target}"
hash = "0000000000000000000000000000000000000000000000000000000000000000"
"#,
    );
    let manifest_bytes = manifest.into_bytes();
    let signature = signing_key.sign(&manifest_bytes);
    (manifest_bytes, signature.to_bytes().to_vec())
}

fn test_unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn sign_manufacturer_request_at(
    request: &mut RegisterRequest,
    signing_key: &ed25519_dalek::SigningKey,
    manifest_bytes: &[u8],
    signed_at: u64,
    nonce: Vec<u8>,
) {
    use ed25519_dalek::Signer;
    use sha2::{Digest, Sha256};

    let target = request
        .target
        .as_deref()
        .expect("test request should carry target");
    let digest = Sha256::digest(manifest_bytes);
    let manifest_sha256_hex = hex::encode(digest);
    let payload = actr_protocol::build_manufacturer_register_payload(
        actr_protocol::ManufacturerRegisterPayload {
            realm_id: request.realm.realm_id,
            actr_type: &request.actr_type,
            target,
            manifest_sha256_hex: &manifest_sha256_hex,
            manufacturer_auth_signed_at: signed_at,
            manufacturer_auth_nonce: &nonce,
        },
    );
    let signature = signing_key.sign(payload.as_bytes());

    request.manufacturer_auth_signature =
        Some(prost::bytes::Bytes::from(signature.to_bytes().to_vec()));
    request.manufacturer_auth_signed_at = Some(signed_at);
    request.manufacturer_auth_nonce = Some(prost::bytes::Bytes::from(nonce));
}

fn sign_manufacturer_request(
    request: &mut RegisterRequest,
    signing_key: &ed25519_dalek::SigningKey,
    manifest_bytes: &[u8],
) {
    sign_manufacturer_request_at(
        request,
        signing_key,
        manifest_bytes,
        test_unix_now_secs(),
        vec![0x42; 32],
    );
}

/// Helper: seed an MFR with a specific keypair and return (mfr_id, key_id, public_key_b64).
async fn seed_mfr_with_key(
    pool: &sqlx::SqlitePool,
    mfr_name: &str,
    signing_key: &ed25519_dalek::SigningKey,
) -> (i64, String) {
    let pub_b64 = base64::prelude::BASE64_STANDARD.encode(signing_key.verifying_key().to_bytes());
    let key_id = actrix_mfr::crypto::compute_key_id(&signing_key.verifying_key().to_bytes());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let expires_at = now + 86400 * 365;

    sqlx::query(
        "INSERT OR REPLACE INTO mfr (name, public_key, key_id, contact, status, created_at, verified_at, key_expires_at) \
         VALUES (?, ?, ?, 'test@example.com', 'active', ?, ?, ?)"
    )
    .bind(mfr_name)
    .bind(&pub_b64)
    .bind(&key_id)
    .bind(now)
    .bind(now)
    .bind(expires_at)
    .execute(pool)
    .await
    .expect("seed mfr");

    let mfr_id: i64 = sqlx::query_scalar("SELECT id FROM mfr WHERE name = ?")
        .bind(mfr_name)
        .fetch_one(pool)
        .await
        .expect("get mfr id");

    (mfr_id, key_id)
}

/// Helper: archive a key into mfr_key_history with given status.
async fn archive_key_to_history(
    pool: &sqlx::SqlitePool,
    mfr_id: i64,
    key_id: &str,
    public_key_b64: &str,
    status: &str,
) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    sqlx::query(
        "INSERT INTO mfr_key_history (mfr_id, key_id, public_key, status, created_at, retired_at) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(mfr_id)
    .bind(key_id)
    .bind(public_key_b64)
    .bind(status)
    .bind(now - 1000)
    .bind(now)
    .execute(pool)
    .await
    .expect("archive key to history");
}

/// Path 2: missing manufacturer_auth_signature is rejected for unpublished packages.
#[tokio::test]
#[serial]
async fn test_path2_missing_manufacturer_auth_signature_rejected() {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let pool = platform::storage::db::get_database().get_pool();
    seed_realm(1001, "test", None).await;

    let key = SigningKey::generate(&mut OsRng);
    let (_mfr_id, _key_id) = seed_mfr_with_key(pool, "missrunner", &key).await;
    let (manifest_bytes, sig_bytes) = build_signed_manifest(
        &key,
        "missrunner",
        "MissingRunnerService",
        "1.0.0",
        "wasm32-wasip1",
    );

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "missrunner".to_string(),
            name: "MissingRunnerService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_bytes)),
        mfr_signature: Some(prost::bytes::Bytes::from(sig_bytes)),
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };

    let response = issuer.issue_credential(&request).await.expect("issue");
    match response.result.expect("result") {
        register_response::Result::Error(_) => {}
        register_response::Result::Success(_) => {
            panic!("Path 2 without manufacturer_auth_signature should be rejected")
        }
    }
}

#[tokio::test]
#[serial]
async fn test_path2_missing_mfr_record_returns_forbidden() {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let issuer = create_test_issuer(&env).await;
    seed_realm(22012, "missing-mfr-path2", None).await;

    let key = SigningKey::generate(&mut OsRng);
    let (manifest, signature) = build_signed_manifest(
        &key,
        "missingpath2mfr",
        "UnpublishedService",
        "1.0.0",
        "wasm32-wasip1",
    );
    let mut request = package_register_request(
        "missingpath2mfr",
        "UnpublishedService",
        "1.0.0",
        22012,
        "wasm32-wasip1",
        manifest.clone(),
        Some(signature),
    );
    sign_manufacturer_request_at(
        &mut request,
        &key,
        &manifest,
        test_unix_now_secs(),
        vec![0x5B; 32],
    );

    let response = issuer.issue_credential(&request).await.expect("issue");
    assert!(
        matches!(
            &response.result,
            Some(register_response::Result::Error(error)) if error.code == 403
        ),
        "a missing MFR record is an authorization rejection, not an internal error: {response:?}"
    );
}

/// Path 2: a manifest that does not declare `binary.target` is rejected.
/// The MFR signature must bind target; AIS must not fall back to the
/// request's target, otherwise one signed manifest could register under
/// arbitrary targets.
#[tokio::test]
#[serial]
async fn test_path2_manifest_without_binary_target_is_rejected() {
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let pool = platform::storage::db::get_database().get_pool();
    seed_realm(1001, "test", None).await;

    let key = SigningKey::generate(&mut OsRng);
    let (_mfr_id, _key_id) = seed_mfr_with_key(pool, "notargetmfr", &key).await;

    // Validly signed manifest that omits [binary].target.
    let key_id = actrix_mfr::crypto::compute_key_id(&key.verifying_key().to_bytes());
    let manifest = format!(
        r#"manufacturer = "notargetmfr"
name = "NoTargetService"
version = "1.0.0"
signing_key_id = "{key_id}"

[binary]
path = "bin/actor.wasm"
hash = "0000000000000000000000000000000000000000000000000000000000000000"
"#,
    );
    let manifest_bytes = manifest.into_bytes();
    let sig_bytes = key.sign(&manifest_bytes).to_bytes().to_vec();

    let mut request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "notargetmfr".to_string(),
            name: "NoTargetService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_bytes.clone())),
        mfr_signature: Some(prost::bytes::Bytes::from(sig_bytes)),
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    // Provide a valid manufacturer proof so the request clears the five-field and
    // signature checks and reaches the binary.target guard.
    sign_manufacturer_request(&mut request, &key, &manifest_bytes);

    let response = issuer.issue_credential(&request).await.expect("issue");
    match response.result.expect("result") {
        register_response::Result::Error(err) => {
            assert_eq!(
                err.code, 400,
                "manifest without binary.target should be InvalidFormat (400)"
            );
        }
        register_response::Result::Success(_) => {
            panic!("manifest without binary.target should be rejected")
        }
    }
}

/// Path 2: replaying the same manufacturer_auth_nonce is rejected after the first success.
#[tokio::test]
#[serial]
async fn test_path2_manufacturer_auth_nonce_replay_rejected() {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let pool = platform::storage::db::get_database().get_pool();
    seed_realm(1001, "test", None).await;

    let key = SigningKey::generate(&mut OsRng);
    let (_mfr_id, _key_id) = seed_mfr_with_key(pool, "replaymfr", &key).await;
    let (manifest_bytes, sig_bytes) =
        build_signed_manifest(&key, "replaymfr", "ReplayService", "1.0.0", "wasm32-wasip1");

    let mut request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "replaymfr".to_string(),
            name: "ReplayService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_bytes.clone())),
        mfr_signature: Some(prost::bytes::Bytes::from(sig_bytes)),
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    sign_manufacturer_request_at(
        &mut request,
        &key,
        &manifest_bytes,
        test_unix_now_secs(),
        vec![0x99; 32],
    );

    let first = issuer
        .issue_credential(&request)
        .await
        .expect("first issue");
    match first.result.expect("first result") {
        register_response::Result::Success(_) => {}
        register_response::Result::Error(err) => {
            panic!("first Path 2 registration should succeed, got error: {err:?}")
        }
    }

    let second = issuer
        .issue_credential(&request)
        .await
        .expect("second issue");
    match second.result.expect("second result") {
        register_response::Result::Error(_) => {}
        register_response::Result::Success(_) => {
            panic!("replayed manufacturer_auth_nonce should be rejected")
        }
    }
}

/// Path 2: historical (retired) key passes signature verification.
#[tokio::test]
#[serial]
async fn test_path2_historical_retired_key_passes() {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let pool = platform::storage::db::get_database().get_pool();

    // Key A (old) and Key B (current)
    let key_a = SigningKey::generate(&mut OsRng);
    let key_b = SigningKey::generate(&mut OsRng);

    let key_a_pub_b64 = base64::prelude::BASE64_STANDARD.encode(key_a.verifying_key().to_bytes());
    let key_a_id = actrix_mfr::crypto::compute_key_id(&key_a.verifying_key().to_bytes());

    // Seed MFR with Key B as current
    let (mfr_id, _key_b_id) = seed_mfr_with_key(pool, "histmfr", &key_b).await;

    // Archive Key A as retired
    archive_key_to_history(pool, mfr_id, &key_a_id, &key_a_pub_b64, "retired").await;

    // Seed realm
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    sqlx::query("INSERT OR IGNORE INTO realm (id, name, status, enabled, created_at, secret_current) VALUES (1001, 'test', 'Active', 1, ?, '')")
        .bind(now)
        .execute(pool)
        .await
        .expect("seed realm");

    // Build manifest signed by Key A (the retired key)
    let (manifest_bytes, sig_bytes) =
        build_signed_manifest(&key_a, "histmfr", "HistService", "1.0.0", "wasm32-wasip1");

    let mut request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "histmfr".to_string(),
            name: "HistService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_bytes.clone())),
        mfr_signature: Some(prost::bytes::Bytes::from(sig_bytes)),
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    sign_manufacturer_request(&mut request, &key_a, &manifest_bytes);

    let response = issuer.issue_credential(&request).await.expect("issue");
    match response.result.expect("result") {
        register_response::Result::Success(_) => {} // expected
        register_response::Result::Error(err) => {
            panic!("Path 2 with retired historical key should succeed, got error: {err:?}")
        }
    }
}

/// Path 2: revoked key is rejected.
#[tokio::test]
#[serial]
async fn test_path2_revoked_key_rejected() {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let pool = platform::storage::db::get_database().get_pool();

    // Seed realm
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    sqlx::query("INSERT OR IGNORE INTO realm (id, name, status, enabled, created_at, secret_current) VALUES (1001, 'test', 'Active', 1, ?, '')")
        .bind(now)
        .execute(pool)
        .await
        .expect("seed realm");

    let key_revoked = SigningKey::generate(&mut OsRng);
    let key_current = SigningKey::generate(&mut OsRng);

    let revoked_pub_b64 =
        base64::prelude::BASE64_STANDARD.encode(key_revoked.verifying_key().to_bytes());
    let revoked_key_id =
        actrix_mfr::crypto::compute_key_id(&key_revoked.verifying_key().to_bytes());

    // Seed MFR with current key
    let (mfr_id, _) = seed_mfr_with_key(pool, "revmfr", &key_current).await;

    // Archive revoked key
    archive_key_to_history(pool, mfr_id, &revoked_key_id, &revoked_pub_b64, "revoked").await;

    // Build manifest signed by the revoked key
    let (manifest_bytes, sig_bytes) = build_signed_manifest(
        &key_revoked,
        "revmfr",
        "RevService",
        "1.0.0",
        "wasm32-wasip1",
    );

    let mut request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "revmfr".to_string(),
            name: "RevService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_bytes.clone())),
        mfr_signature: Some(prost::bytes::Bytes::from(sig_bytes)),
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    sign_manufacturer_request(&mut request, &key_revoked, &manifest_bytes);

    let response = issuer.issue_credential(&request).await.expect("issue");
    match response.result.expect("result") {
        register_response::Result::Error(_) => {} // expected: revoked key rejected
        register_response::Result::Success(_) => {
            panic!("Path 2 with revoked key should be rejected, but got success")
        }
    }
}

/// Path 2: manifest identity mismatch (actr_type spoofing) is rejected.
#[tokio::test]
#[serial]
async fn test_path2_identity_mismatch_rejected() {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let pool = platform::storage::db::get_database().get_pool();

    // Seed realm (shared global DB may not have it if run in isolation)
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    sqlx::query("INSERT OR IGNORE INTO realm (id, name, status, enabled, created_at, secret_current) VALUES (1001, 'test', 'Active', 1, ?, '')")
        .bind(now)
        .execute(pool)
        .await
        .expect("seed realm");

    let key = SigningKey::generate(&mut OsRng);
    let (_mfr_id, _key_id) = seed_mfr_with_key(pool, "spoofmfr", &key).await;

    // Build manifest for ServiceA
    let (manifest_bytes, sig_bytes) =
        build_signed_manifest(&key, "spoofmfr", "ServiceA", "1.0.0", "wasm32-wasip1");

    // But register as ServiceB — identity mismatch!
    let mut request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "spoofmfr".to_string(),
            name: "ServiceB".to_string(), // ← does not match manifest
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_bytes.clone())),
        mfr_signature: Some(prost::bytes::Bytes::from(sig_bytes)),
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    sign_manufacturer_request(&mut request, &key, &manifest_bytes);

    let response = issuer.issue_credential(&request).await.expect("issue");
    match response.result.expect("result") {
        register_response::Result::Error(_) => {} // expected: identity mismatch rejected
        register_response::Result::Success(_) => {
            panic!("Path 2 with identity mismatch should be rejected, but got success")
        }
    }
}

// ============================================================================
// Manufacturer signature matrix tests (组 A / B / C + dedicated cases)
//
// These cover the gaps left by the scenario matrix in the design doc:
// missing-field, tamper/signature-invalidity, time-window, concurrent nonce,
// Path 1 triple-ignored, Linked triple-rejected, and renewal bypass.
// ============================================================================

/// Shared Path 2 fixture: realm seeded, MFR + active key registered, and a
/// validly signed manifest. No `mfr_package` row is seeded, so registration
/// always falls through to Path 2 (signature + manufacturer proof).
///
/// `env` is held for the fixture's lifetime: dropping it would drop the
/// embedded signer's shutdown sender, which resolves `serve_with_shutdown`
/// and tears the signer down before the test can call it.
struct Path2Fixture {
    _env: TestEnv,
    issuer: AIdIssuer,
    key: ed25519_dalek::SigningKey,
    wrong_key: ed25519_dalek::SigningKey,
    manifest_bytes: Vec<u8>,
    sig_bytes: Vec<u8>,
    manufacturer: String,
    name: String,
    target: String,
}

async fn path2_fixture(manufacturer: &str, name: &str, target: &str) -> Path2Fixture {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let env = setup_test_environment().await;
    let signer_client = create_signer_client(&env.signer_config, &env.shared_key)
        .await
        .expect("signer client");
    let issuer = AIdIssuer::new(
        signer_client,
        default_issuer_config(&env.issuer_temp_dir),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("issuer");

    let pool = platform::storage::db::get_database().get_pool();
    seed_realm(1001, "test", None).await;
    seed_realm(1002, "test-alt", None).await;

    let key = SigningKey::generate(&mut OsRng);
    let wrong_key = SigningKey::generate(&mut OsRng);
    let (_mfr_id, _key_id) = seed_mfr_with_key(pool, manufacturer, &key).await;
    let (manifest_bytes, sig_bytes) =
        build_signed_manifest(&key, manufacturer, name, "1.0.0", target);

    Path2Fixture {
        _env: env,
        issuer,
        key,
        wrong_key,
        manifest_bytes,
        sig_bytes,
        manufacturer: manufacturer.to_string(),
        name: name.to_string(),
        target: target.to_string(),
    }
}

/// Build a Path 2 `RegisterRequest` matching the fixture's manifest, with all
/// manufacturer fields left empty. The caller signs (and optionally mutates) it.
fn base_path2_request(fx: &Path2Fixture, realm_id: u32) -> RegisterRequest {
    RegisterRequest {
        actr_type: ActrType {
            manufacturer: fx.manufacturer.clone(),
            name: fx.name.clone(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(fx.manifest_bytes.clone())),
        mfr_signature: Some(prost::bytes::Bytes::from(fx.sig_bytes.clone())),
        target: Some(fx.target.clone()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    }
}

async fn post_renew(app: Router, request: RenewCredentialRequest) -> RenewCredentialResponse {
    let builder = Request::builder()
        .method("POST")
        .uri("/renew")
        .header("content-type", "application/protobuf")
        .header("x-real-ip", "127.0.0.1");
    let response = app
        .oneshot(builder.body(Body::from(request.encode_to_vec())).unwrap())
        .await
        .expect("renew route response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("renew response body");
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected /renew status {status}: {}",
        String::from_utf8_lossy(&body)
    );
    RenewCredentialResponse::decode(body).expect("decode renew response")
}

type FieldBlank<'a> = &'a dyn Fn(&mut RegisterRequest);
type TamperMutation<'a> = &'a dyn Fn(&mut RegisterRequest, &Path2Fixture, u64);

/// 组 A: any one of the manufacturer triple missing → rejected (ManufacturerNotVerified).
/// Covers scenario-matrix #2 / #3 / #4 as a single table.
#[tokio::test]
#[serial]
async fn test_path2_missing_manufacturer_field_matrix() {
    let fx = path2_fixture("missfieldmfr", "MissFieldService", "wasm32-wasip1").await;
    let now = test_unix_now_secs();

    let cases: &[(&str, FieldBlank)] = &[
        ("missing_signature", &|r| {
            r.manufacturer_auth_signature = None
        }),
        ("missing_signed_at", &|r| {
            r.manufacturer_auth_signed_at = None
        }),
        ("missing_nonce", &|r| r.manufacturer_auth_nonce = None),
    ];

    for (i, (label, blank)) in cases.iter().enumerate() {
        let mut request = base_path2_request(&fx, 1001);
        sign_manufacturer_request_at(
            &mut request,
            &fx.key,
            &fx.manifest_bytes,
            now,
            vec![i as u8 + 0x40; 32],
        );
        blank(&mut request);

        let response = fx.issuer.issue_credential(&request).await.expect("issue");
        match response.result.expect("result") {
            register_response::Result::Error(err) => assert_eq!(
                err.code, 403,
                "case {label}: expected ManufacturerNotVerified (403), got {}",
                err.code
            ),
            register_response::Result::Success(_) => {
                panic!("case {label}: missing manufacturer field should be rejected")
            }
        }
    }
}

/// 组 B: tamper / wrong-key matrix. A valid proof is signed first, then one
/// field is mutated per row. All tamper rows must be rejected (403); the
/// control row succeeds (covers #5 happy path).
/// Covers scenario-matrix #5 / #6 / #7 / #9 / #10 / #11 / #12.
#[tokio::test]
#[serial]
async fn test_path2_tamper_and_signature_matrix() {
    let fx = path2_fixture("tampermfr", "TamperService", "wasm32-wasip1").await;
    let now = test_unix_now_secs();

    let cases: &[(&str, bool, TamperMutation)] = &[
        ("control", true, &|_r, _f, _n| {}),
        ("wrong_key", false, &|r, f, n| {
            // overwrite the valid proof with one signed by an unregistered key
            sign_manufacturer_request_at(r, &f.wrong_key, &f.manifest_bytes, n, vec![0x21; 32]);
        }),
        ("tamper_realm", false, &|r, _f, _n| r.realm.realm_id = 1002),
        ("tamper_target", false, &|r, _f, _n| {
            r.target = Some("x86_64-unknown-linux-gnu".to_string());
        }),
        ("tamper_manifest_raw", false, &|r, _f, _n| {
            // flip a byte inside the manifest hash field: still valid TOML, but
            // the MFR signature over the original bytes no longer verifies.
            let mut bytes = r.manifest_raw.as_deref().unwrap().to_vec();
            if let Some(pos) = bytes.iter().rposition(|&b| b == b'0') {
                bytes[pos] = b'1';
            }
            r.manifest_raw = Some(prost::bytes::Bytes::from(bytes));
        }),
        ("tamper_signed_at", false, &|r, _f, n| {
            r.manufacturer_auth_signed_at = Some(n + 1); // still in window, so sig mismatch is what fails
        }),
        ("tamper_nonce", false, &|r, _f, _n| {
            r.manufacturer_auth_nonce = Some(prost::bytes::Bytes::from(vec![0xEE; 32]));
        }),
    ];

    for (i, (label, expect_success, mutation)) in cases.iter().enumerate() {
        let mut request = base_path2_request(&fx, 1001);
        let nonce = vec![i as u8 + 0x30; 32];
        sign_manufacturer_request_at(&mut request, &fx.key, &fx.manifest_bytes, now, nonce);
        mutation(&mut request, &fx, now);

        let response = fx.issuer.issue_credential(&request).await.expect("issue");
        match (response.result.expect("result"), expect_success) {
            (register_response::Result::Success(_), true) => {}
            (register_response::Result::Error(err), false) => assert_eq!(
                err.code, 403,
                "case {label}: expected ManufacturerNotVerified (403), got {}",
                err.code
            ),
            (register_response::Result::Success(_), false) => {
                panic!("case {label}: tamper should be rejected, got success")
            }
            (register_response::Result::Error(err), true) => {
                panic!("case {label}: control should succeed, got error {err:?}")
            }
        }
    }
}

/// 组 C: `manufacturer_auth_signed_at` time window. `verify_manufacturer_auth_signed_at` has two
/// branches (Expired / InvalidTimestamp) with zero coverage today.
/// Covers scenario-matrix #13 / #14 plus both boundaries.
#[tokio::test]
#[serial]
async fn test_path2_manufacturer_auth_signed_at_window_matrix() {
    let fx = path2_fixture("winmfr", "WinService", "wasm32-wasip1").await;
    let now = test_unix_now_secs();

    // (label, signed_at, expected: None = success, Some(code) = error)
    let cases: &[(&str, u64, Option<u32>)] = &[
        ("control_now", now, None),
        ("too_old", now - 600, Some(401)), // Expired (MAX_AGE = 300s)
        ("too_future", now + 300, Some(400)), // InvalidTimestamp (FUTURE_SKEW = 60s)
        ("boundary_back", now - 301, Some(401)), // first failing past boundary
        // Keep this comfortably beyond the 60s allowance: using exactly now+61
        // races the wall clock while the matrix executes and can become valid.
        ("future_guard", now + 120, Some(400)),
    ];

    for (i, (label, signed_at, expect)) in cases.iter().enumerate() {
        let mut request = base_path2_request(&fx, 1001);
        sign_manufacturer_request_at(
            &mut request,
            &fx.key,
            &fx.manifest_bytes,
            *signed_at,
            vec![i as u8 + 0x10; 32],
        );
        let response = fx.issuer.issue_credential(&request).await.expect("issue");
        match (response.result.expect("result"), expect) {
            (register_response::Result::Success(_), None) => {}
            (register_response::Result::Error(err), Some(code)) => assert_eq!(
                err.code, *code,
                "case {label}: expected code {code}, got {}",
                err.code
            ),
            (register_response::Result::Success(_), Some(code)) => {
                panic!("case {label}: expected error {code}, got success")
            }
            (register_response::Result::Error(err), None) => {
                panic!("case {label}: expected success, got error {err:?}")
            }
        }
    }
}

/// #16: two concurrent registrations with the same manufacturer nonce — the
/// UNIQUE(manufacturer, key_id, nonce) constraint must let exactly one win.
#[tokio::test]
#[serial]
async fn test_path2_concurrent_nonce_only_one_succeeds() {
    let fx = path2_fixture("concurrencymfr", "ConcurrentService", "wasm32-wasip1").await;
    let now = test_unix_now_secs();

    let build = || {
        let mut r = base_path2_request(&fx, 1001);
        sign_manufacturer_request_at(&mut r, &fx.key, &fx.manifest_bytes, now, vec![0xC0; 32]);
        r
    };
    let req_a = build();
    let req_b = build();

    let (resp_a, resp_b) = tokio::join!(
        fx.issuer.issue_credential(&req_a),
        fx.issuer.issue_credential(&req_b)
    );

    let success_a = matches!(
        resp_a.expect("issue a").result,
        Some(register_response::Result::Success(_))
    );
    let success_b = matches!(
        resp_b.expect("issue b").result,
        Some(register_response::Result::Success(_))
    );
    assert!(
        success_a ^ success_b,
        "exactly one concurrent registration should succeed (a={success_a}, b={success_b})"
    );
}

/// Path 1: a published package registration that additionally carries a manufacturer
/// triple must still succeed — Path 1 ignores the triple (registry active is
/// authoritative). Mirrors `test_register_route_package_with_mfr_identity_succeeds`.
#[tokio::test]
#[serial]
async fn test_path1_published_package_with_manufacturer_triple_succeeds() {
    let env = setup_test_environment().await;
    let realm_id = 22003;
    seed_realm(realm_id, "package-triple-route", None).await;
    let manifest_raw =
        seed_active_package("triplepkg", "TripleService", "1.0.0", "wasm32-wasip1").await;
    let app = create_test_router(&env).await;

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "triplepkg".to_string(),
            name: "TripleService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_raw)),
        mfr_signature: None,
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        // Path 1 must ignore a (bogus) manufacturer triple.
        manufacturer_auth_signature: Some(prost::bytes::Bytes::from_static(
            b"bogus-manufacturer-sig",
        )),
        manufacturer_auth_signed_at: Some(test_unix_now_secs()),
        manufacturer_auth_nonce: Some(prost::bytes::Bytes::from(vec![0x77; 32])),
    };
    let response = post_register(app, request, None).await;

    match response.result.expect("result") {
        register_response::Result::Success(ok) => {
            assert_eq!(ok.actr_id.realm.realm_id, realm_id);
            assert_eq!(ok.actr_id.r#type.manufacturer, "triplepkg");
        }
        register_response::Result::Error(err) => {
            panic!("Path 1 should ignore manufacturer triple, got error: {err:?}")
        }
    }
}

/// #18: Linked auth must reject any manufacturer triple (InvalidFormat). The triple
/// must not leak into the Linked path; Linked enforces its absence.
#[tokio::test]
#[serial]
async fn test_linked_auth_with_manufacturer_triple_rejected() {
    let env = setup_test_environment().await;
    let realm_secret = "linked-triple-secret";
    let realm_id = 23001;
    seed_realm(realm_id, "linked-triple", Some(realm_secret)).await;
    let app = create_test_router(&env).await;

    let mut request = linked_register_request();
    request.realm = Realm { realm_id };
    request.manufacturer_auth_signature =
        Some(prost::bytes::Bytes::from_static(b"should-not-be-present"));
    request.manufacturer_auth_signed_at = Some(test_unix_now_secs());
    request.manufacturer_auth_nonce = Some(prost::bytes::Bytes::from(vec![0x88; 32]));

    let response = post_register(app, request, Some(realm_secret)).await;
    match response.result.expect("result") {
        register_response::Result::Error(err) => assert_eq!(
            err.code, 400,
            "Linked with manufacturer triple should be InvalidFormat (400), got {}",
            err.code
        ),
        register_response::Result::Success(_) => {
            panic!("Linked auth should reject manufacturer triple presence")
        }
    }
}

/// #17: renewal_token renewal does not require (and cannot carry) a manufacturer
/// triple. Register a published package, then renew via `POST /ais/renew`.
#[tokio::test]
#[serial]
async fn test_renewal_token_renew_without_manufacturer_triple() {
    let env = setup_test_environment().await;
    let realm_id = 22004;
    seed_realm(realm_id, "renew-route", None).await;
    let manifest_raw =
        seed_active_package("renewpkg", "RenewService", "1.0.0", "wasm32-wasip1").await;
    let app = create_test_router(&env).await;

    let register = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "renewpkg".to_string(),
            name: "RenewService".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: Some(prost::bytes::Bytes::from(manifest_raw)),
        mfr_signature: None,
        target: Some("wasm32-wasip1".to_string()),
        auth_mode: Some(RegisterAuthMode::Package as i32),
        manufacturer_auth_signature: None,
        manufacturer_auth_signed_at: None,
        manufacturer_auth_nonce: None,
    };
    // `post_register` consumes the Router; clone it so the renew call hits the
    // same issuer (shared via Arc<AIdIssuer> inside AISState).
    let reg_response = post_register(app.clone(), register, None).await;
    let ok = match reg_response.result.expect("register result") {
        register_response::Result::Success(ok) => ok,
        register_response::Result::Error(err) => {
            panic!("register should succeed before renew, got error: {err:?}")
        }
    };
    let actr_id = ok.actr_id;
    let renewal_token = ok
        .renewal_token
        .expect("register should issue a renewal_token");

    let renew_request = RenewCredentialRequest {
        actr_id,
        renewal_token,
    };
    let renew_response = post_renew(app, renew_request).await;
    match renew_response.result.expect("renew result") {
        renew_credential_response::Result::Success(ok) => {
            let new_token = ok
                .renewal_token
                .expect("renew should issue a rotated renewal_token");
            assert!(!new_token.is_empty(), "renew should rotate the token");
        }
        renew_credential_response::Result::Error(err) => {
            panic!("renewal without manufacturer triple should succeed, got error: {err:?}")
        }
    }
}
