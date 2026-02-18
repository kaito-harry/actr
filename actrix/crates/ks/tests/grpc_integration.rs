use actrix_proto::{
    ks::v1::{
        GenerateKeyRequest, GetSecretKeyRequest, HealthCheckRequest,
        key_server_client::KeyServerClient,
    },
    supervisor::v1::NonceCredential,
};
use base64::Engine as _;
use ks::{
    GrpcClient, GrpcClientConfig, KeyEncryptor, KeyStorage, KsError, KsServiceConfig,
    create_grpc_service,
};
use nonce_auth::{CredentialBuilder, storage::MemoryStorage};
use tempfile::TempDir;
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{Duration, Instant, sleep},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, transport::Server};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

struct GrpcTestServer {
    endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
    _temp_dir: TempDir,
}

impl Drop for GrpcTestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle.abort();
    }
}

async fn start_grpc_server(
    psk: &str,
    key_ttl_seconds: u64,
    tolerance_seconds: u64,
) -> GrpcTestServer {
    let temp_dir = tempfile::tempdir().expect("create temp dir");

    let mut config = KsServiceConfig::default();
    config.storage.key_ttl_seconds = key_ttl_seconds;

    let storage = KeyStorage::from_config(
        &config.storage,
        KeyEncryptor::no_encryption(),
        temp_dir.path(),
    )
    .await
    .expect("create key storage");

    let grpc_service = create_grpc_service(
        storage,
        MemoryStorage::new(),
        psk.to_string(),
        tolerance_seconds,
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp listener");
    let addr = listener.local_addr().expect("read listener addr");
    let endpoint = format!("http://{addr}");
    let incoming = TcpListenerStream::new(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("grpc test server crashed");
    });

    GrpcTestServer {
        endpoint,
        shutdown_tx: Some(shutdown_tx),
        handle,
        _temp_dir: temp_dir,
    }
}

async fn connect_client(endpoint: &str) -> KeyServerClient<tonic::transport::Channel> {
    let start = Instant::now();
    loop {
        if let Ok(client) = KeyServerClient::connect(endpoint.to_string()).await {
            return client;
        }

        if start.elapsed() > CONNECT_TIMEOUT {
            panic!("grpc server not ready at {endpoint}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

fn sign_credential(psk: &str, payload: &str) -> NonceCredential {
    let credential = CredentialBuilder::new(psk.as_bytes())
        .sign(payload.as_bytes())
        .expect("sign nonce credential");

    NonceCredential {
        timestamp: credential.timestamp,
        nonce: credential.nonce,
        signature: credential.signature,
    }
}

fn sign_credential_with_timestamp(psk: &str, payload: &str, ts: u64) -> NonceCredential {
    let credential = CredentialBuilder::new(psk.as_bytes())
        .with_time_provider(move || Ok(ts))
        .sign(payload.as_bytes())
        .expect("sign nonce credential with timestamp");

    NonceCredential {
        timestamp: credential.timestamp,
        nonce: credential.nonce,
        signature: credential.signature,
    }
}

#[tokio::test]
async fn test_grpc_health_and_key_lifecycle() {
    let psk = "test-ks-grpc-psk";
    let server = start_grpc_server(psk, 3600, 3600).await;
    let mut client = connect_client(&server.endpoint).await;

    let health_before = client
        .health_check(HealthCheckRequest {})
        .await
        .expect("health check before generate")
        .into_inner();
    assert_eq!(health_before.status, "healthy");
    assert_eq!(health_before.service, "ks");
    assert_eq!(health_before.key_count, 0);

    let generated = client
        .generate_key(GenerateKeyRequest {
            credential: sign_credential(psk, "generate_key"),
        })
        .await
        .expect("generate key")
        .into_inner();
    assert!(generated.key_id > 0);
    assert!(generated.expires_at > 0);

    let public_key_bytes = base64::engine::general_purpose::STANDARD
        .decode(generated.public_key.as_bytes())
        .expect("public key should be valid base64");
    assert_eq!(
        public_key_bytes.len(),
        33,
        "Public key must be 33-byte compressed secp256k1 key"
    );

    let fetched = client
        .get_secret_key(GetSecretKeyRequest {
            key_id: generated.key_id,
            credential: sign_credential(psk, &format!("get_secret_key:{}", generated.key_id)),
        })
        .await
        .expect("fetch secret key")
        .into_inner();
    assert_eq!(fetched.key_id, generated.key_id);
    assert!(fetched.expires_at > 0);

    let secret_key_bytes = base64::engine::general_purpose::STANDARD
        .decode(fetched.secret_key.as_bytes())
        .expect("secret key should be valid base64");
    assert_eq!(secret_key_bytes.len(), 32, "Secret key must be 32 bytes");

    let health_after = client
        .health_check(HealthCheckRequest {})
        .await
        .expect("health check after generate")
        .into_inner();
    assert!(health_after.key_count >= 1);
}

#[tokio::test]
async fn test_grpc_generate_rejects_invalid_signature() {
    let psk = "test-ks-grpc-psk";
    let server = start_grpc_server(psk, 3600, 3600).await;
    let mut client = connect_client(&server.endpoint).await;

    let err = client
        .generate_key(GenerateKeyRequest {
            credential: sign_credential(psk, "not-generate-key"),
        })
        .await
        .expect_err("generate should fail for invalid payload signature");
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn test_grpc_generate_rejects_replay_nonce() {
    let psk = "test-ks-grpc-psk";
    let server = start_grpc_server(psk, 3600, 3600).await;
    let mut client = connect_client(&server.endpoint).await;

    let credential = sign_credential(psk, "generate_key");
    client
        .generate_key(GenerateKeyRequest {
            credential: credential.clone(),
        })
        .await
        .expect("first request should succeed");

    let replay_err = client
        .generate_key(GenerateKeyRequest { credential })
        .await
        .expect_err("replayed nonce should be rejected");
    assert_eq!(replay_err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn test_grpc_get_secret_key_not_found() {
    let psk = "test-ks-grpc-psk";
    let server = start_grpc_server(psk, 3600, 3600).await;
    let mut client = connect_client(&server.endpoint).await;

    let missing_key_id = 9_999_999_u32;
    let err = client
        .get_secret_key(GetSecretKeyRequest {
            key_id: missing_key_id,
            credential: sign_credential(psk, &format!("get_secret_key:{missing_key_id}")),
        })
        .await
        .expect_err("missing key should return not found");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn test_grpc_get_secret_key_rejects_stale_timestamp() {
    let psk = "test-ks-grpc-psk";
    let server = start_grpc_server(psk, 3600, 3600).await;
    let mut client = connect_client(&server.endpoint).await;

    let err = client
        .generate_key(GenerateKeyRequest {
            credential: sign_credential_with_timestamp(psk, "generate_key", 0),
        })
        .await
        .expect_err("stale timestamp should be rejected");
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn test_grpc_get_secret_key_expired_beyond_tolerance_returns_not_found() {
    let psk = "test-ks-grpc-psk";
    let server = start_grpc_server(psk, 1, 0).await;
    let mut client = connect_client(&server.endpoint).await;

    let generated = client
        .generate_key(GenerateKeyRequest {
            credential: sign_credential(psk, "generate_key"),
        })
        .await
        .expect("generate key")
        .into_inner();

    sleep(Duration::from_secs(2)).await;

    let err = client
        .get_secret_key(GetSecretKeyRequest {
            key_id: generated.key_id,
            credential: sign_credential(psk, &format!("get_secret_key:{}", generated.key_id)),
        })
        .await
        .expect_err("expired key should be unavailable");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn test_ks_grpc_client_end_to_end() {
    let psk = "test-ks-grpc-psk";
    let server = start_grpc_server(psk, 3600, 90).await;

    let mut client = GrpcClient::new(&GrpcClientConfig {
        endpoint: server.endpoint.clone(),
        actrix_shared_key: psk.to_string(),
        timeout_seconds: 5,
        enable_tls: false,
        tls_domain: None,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    })
    .await
    .expect("create grpc client");

    let status = client.health_check().await.expect("health check");
    assert_eq!(status, "healthy");

    let (key_id, _public_key, expires_at, tolerance_seconds) =
        client.generate_key().await.expect("generate key");
    assert!(key_id > 0);
    assert!(expires_at > 0);
    assert_eq!(tolerance_seconds, 90);

    let (_secret_key, fetched_expires_at, fetched_tolerance_seconds) = client
        .fetch_secret_key(key_id)
        .await
        .expect("fetch secret key");
    assert_eq!(fetched_expires_at, expires_at);
    assert_eq!(fetched_tolerance_seconds, 90);
}

#[tokio::test]
async fn test_ks_grpc_client_rejects_wrong_shared_secret() {
    let server = start_grpc_server("correct-secret", 3600, 60).await;

    let mut client = GrpcClient::new(&GrpcClientConfig {
        endpoint: server.endpoint.clone(),
        actrix_shared_key: "wrong-secret".to_string(),
        timeout_seconds: 5,
        enable_tls: false,
        tls_domain: None,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    })
    .await
    .expect("create grpc client");

    let err = client
        .generate_key()
        .await
        .expect_err("wrong shared secret should fail");
    match err {
        KsError::Internal(msg) => {
            assert!(
                msg.contains("valid authentication credentials")
                    || msg.contains("Invalid signature"),
                "unexpected error message: {msg}"
            );
        }
        other => panic!("expected internal grpc auth error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_ks_grpc_client_rejects_invalid_endpoint() {
    let result = GrpcClient::new(&GrpcClientConfig {
        endpoint: "not-a-valid-endpoint".to_string(),
        actrix_shared_key: "test-ks-grpc-psk".to_string(),
        timeout_seconds: 1,
        enable_tls: false,
        tls_domain: None,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    })
    .await;

    match result {
        Ok(_) => panic!("invalid endpoint should be rejected"),
        Err(KsError::Internal(msg)) => {
            assert!(
                msg.contains("Invalid endpoint") || msg.contains("Failed to connect to KS"),
                "unexpected error message: {msg}"
            );
        }
        Err(other) => panic!("expected invalid endpoint internal error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_ks_grpc_client_fails_when_server_unreachable() {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind temporary local listener");
    let addr = listener.local_addr().expect("read temporary listener addr");
    drop(listener);

    let result = GrpcClient::new(&GrpcClientConfig {
        endpoint: format!("http://{addr}"),
        actrix_shared_key: "test-ks-grpc-psk".to_string(),
        timeout_seconds: 1,
        enable_tls: false,
        tls_domain: None,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    })
    .await;

    match result {
        Ok(_) => panic!("unreachable endpoint should fail"),
        Err(KsError::Internal(msg)) => {
            assert!(
                msg.contains("Failed to connect to KS"),
                "unexpected error message: {msg}"
            );
        }
        Err(other) => panic!("expected connection failure internal error, got {other:?}"),
    }
}
