//! AIS 集成测试（自举式）
//!
//! 在测试进程内启动临时 KS gRPC 服务，验证 AIS 的签发与校验链路。

use actr_protocol::{ActrType, Realm, RegisterRequest, register_response};
use actrix_common::aid::credential::validator::AIdCredentialValidator;
use actrix_common::config::ks::KsClientConfig;
use ais::issuer::{AIdIssuer, IssuerConfig};
use ais::ks_client_wrapper::create_ks_client;
use ks::{GrpcClient, GrpcClientConfig, KeyStorage, KsServiceConfig, create_grpc_service};
use nonce_auth::storage::MemoryStorage;
use std::net::TcpListener;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;

struct TestEnv {
    issuer_temp_dir: TempDir,
    validator_temp_dir: TempDir,
    _ks_temp_dir: TempDir,
    ks_handle: Option<JoinHandle<()>>,
    ks_shutdown_tx: Option<oneshot::Sender<()>>,
    ks_config: KsClientConfig,
    shared_key: String,
}

impl TestEnv {
    async fn shutdown_ks(&mut self) {
        if let Some(tx) = self.ks_shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.ks_handle.take() {
            let _ = handle.await;
        }
    }
}

async fn start_embedded_ks(
    psk: &str,
    sqlite_path: &Path,
) -> (String, JoinHandle<()>, oneshot::Sender<()>) {
    let service_config = KsServiceConfig::default();
    let storage = KeyStorage::from_config(
        &service_config.storage,
        ks::KeyEncryptor::no_encryption(),
        sqlite_path,
    )
    .await
    .expect("Failed to create KS storage");

    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral port");
    let addr = listener.local_addr().expect("Failed to get local addr");
    drop(listener);

    let service = create_grpc_service(
        storage,
        MemoryStorage::new(),
        psk.to_string(),
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
            panic!("Embedded KS server failed: {err}");
        }
    });

    let endpoint = format!("http://{addr}");

    // Wait until gRPC health check is reachable.
    let mut last_error = String::new();
    for _ in 0..40 {
        let cfg = GrpcClientConfig {
            endpoint: endpoint.clone(),
            actrix_shared_key: psk.to_string(),
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
                Ok(status) => last_error = format!("unexpected KS health status: {status}"),
                Err(err) => last_error = err.to_string(),
            },
            Err(err) => last_error = err.to_string(),
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("Embedded KS did not become healthy in time: {last_error}");
}

async fn setup_test_environment() -> TestEnv {
    let issuer_temp_dir = TempDir::new().expect("Failed to create issuer temp dir");
    let validator_temp_dir = TempDir::new().expect("Failed to create validator temp dir");
    let ks_temp_dir = TempDir::new().expect("Failed to create ks temp dir");
    let shared_key = "test-psk-key".to_string();
    let (endpoint, ks_handle, ks_shutdown_tx) =
        start_embedded_ks(&shared_key, ks_temp_dir.path()).await;

    let ks_config = KsClientConfig {
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
        validator_temp_dir,
        _ks_temp_dir: ks_temp_dir,
        ks_handle: Some(ks_handle),
        ks_shutdown_tx: Some(ks_shutdown_tx),
        ks_config,
        shared_key,
    }
}

fn default_issuer_config(temp_dir: &TempDir) -> IssuerConfig {
    IssuerConfig {
        token_ttl_secs: 3600,
        signaling_heartbeat_interval_secs: 30,
        key_refresh_interval_secs: 3600,
        key_storage_file: temp_dir.path().join("issuer_keys.db"),
        enable_periodic_rotation: false,
        key_rotation_interval_secs: 86400,
    }
}

#[tokio::test]
async fn test_end_to_end_credential_flow() {
    let env = setup_test_environment().await;

    let ks_client = create_ks_client(&env.ks_config, &env.shared_key)
        .await
        .expect("Failed to create KS gRPC client");
    let issuer = AIdIssuer::new(ks_client, default_issuer_config(&env.issuer_temp_dir))
        .await
        .expect("Failed to create issuer");

    AIdCredentialValidator::init(
        &env.ks_config,
        &env.shared_key,
        env.validator_temp_dir.path(),
    )
    .await
    .expect("Failed to initialize validator");

    let request = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "test-manufacturer".to_string(),
            name: "test-device".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
    };

    let response = issuer
        .issue_credential(&request)
        .await
        .expect("Failed to issue credential");

    let register_ok = match response.result.expect("Response should contain result") {
        register_response::Result::Success(ok) => ok,
        register_response::Result::Error(err) => panic!("Expected success but got error: {err:?}"),
    };

    assert!(register_ok.psk.is_some(), "PSK should be present");
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
    assert!(claims.expr_time > now);
    assert!(claims.expr_time <= now + 3600);

    let wrong_realm_result = AIdCredentialValidator::check(&register_ok.credential, 9999).await;
    assert!(
        wrong_realm_result.is_err(),
        "Validation should fail with mismatched realm_id"
    );

    // Issue and validate multiple credentials to verify stability.
    for idx in 0..5 {
        let req = RegisterRequest {
            actr_type: ActrType {
                manufacturer: format!("test-manufacturer-{idx}"),
                name: format!("test-device-{idx}"),
            },
            realm: Realm { realm_id: 1001 },
            service_spec: None,
            acl: None,
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
async fn test_issuer_health_checks() {
    let env = setup_test_environment().await;

    let ks_client = create_ks_client(&env.ks_config, &env.shared_key)
        .await
        .expect("Failed to create KS gRPC client");
    let issuer = AIdIssuer::new(ks_client, default_issuer_config(&env.issuer_temp_dir))
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
async fn test_issuer_rotate_key_updates_current_key() {
    let env = setup_test_environment().await;

    let ks_client = create_ks_client(&env.ks_config, &env.shared_key)
        .await
        .expect("Failed to create KS gRPC client");
    let issuer = AIdIssuer::new(ks_client, default_issuer_config(&env.issuer_temp_dir))
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
        .expect("KS health should still pass after rotation");
    let cache = issuer
        .check_key_cache_health()
        .await
        .expect("key cache should remain healthy");
    assert_eq!(cache.key_id, rotated);
}

#[tokio::test]
async fn test_issuer_creation_fails_with_wrong_shared_key() {
    let env = setup_test_environment().await;

    let ks_client = create_ks_client(&env.ks_config, "wrong-shared-key")
        .await
        .expect("gRPC channel creation should succeed even with wrong secret");

    let err = match AIdIssuer::new(ks_client, default_issuer_config(&env.issuer_temp_dir)).await {
        Ok(_) => panic!("issuer initialization should fail when KS authentication fails"),
        Err(err) => err,
    };

    let msg = err.to_string();
    assert!(
        msg.contains("Failed to fetch new key from KS")
            || msg.contains("Authentication")
            || msg.contains("Invalid signature"),
        "unexpected issuer initialization error: {msg}"
    );
}

#[tokio::test]
async fn test_issuer_check_ks_health_fails_after_ks_shutdown() {
    let mut env = setup_test_environment().await;

    let ks_client = create_ks_client(&env.ks_config, &env.shared_key)
        .await
        .expect("Failed to create KS gRPC client");
    let issuer = AIdIssuer::new(ks_client, default_issuer_config(&env.issuer_temp_dir))
        .await
        .expect("Failed to create issuer");

    env.shutdown_ks().await;

    for _ in 0..40 {
        match issuer.check_ks_health().await {
            Ok(()) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("KS service unhealthy")
                        || msg.contains("Failed")
                        || msg.contains("transport")
                        || msg.contains("unavailable")
                        || msg.contains("connection"),
                    "unexpected KS health error after shutdown: {msg}"
                );
                return;
            }
        }
    }

    panic!("issuer KS health should fail after embedded KS shutdown");
}

#[tokio::test]
async fn test_issuer_rotate_key_fails_when_ks_is_unavailable() {
    let mut env = setup_test_environment().await;

    let ks_client = create_ks_client(&env.ks_config, &env.shared_key)
        .await
        .expect("Failed to create KS gRPC client");
    let issuer = AIdIssuer::new(ks_client, default_issuer_config(&env.issuer_temp_dir))
        .await
        .expect("Failed to create issuer");

    env.shutdown_ks().await;

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
