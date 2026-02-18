use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use actrix_common::{
    ServiceCollector, ServiceInfo, ServiceState, ServiceType, storage::SqliteNonceStorage,
};
use nonce_auth::{CredentialBuilder, CredentialVerifier, NonceError, storage::NonceStorage};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};

use supervit::{
    HealthCheckRequest, HealthCheckResponse, NonceCredential, RegisterNodeRequest,
    RegisterNodeResponse, ReportRequest, ReportResponse, ServiceAdvertisementStatus,
    SupervisorService, SupervisorServiceClient, SupervisorServiceServer, SupervitClient,
    SupervitConfig, SupervitError,
};

const TEST_SHARED_SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[derive(Clone, Default)]
struct NodeState {
    last_register_request: Option<RegisterNodeRequest>,
    last_report_request: Option<ReportRequest>,
    report_count: u32,
    health_check_count: u32,
}

type NodeMap = Arc<RwLock<std::collections::HashMap<String, NodeState>>>;

#[derive(Clone)]
struct TestSupervisorService {
    shared_secret: Arc<Vec<u8>>,
    nonce_storage: Arc<dyn NonceStorage + Send + Sync>,
    max_clock_skew_secs: u64,
    next_report_interval_secs: i32,
    nodes: NodeMap,
}

impl TestSupervisorService {
    fn new<N: NonceStorage + Send + Sync + 'static>(
        shared_secret: Vec<u8>,
        nonce_storage: N,
        max_clock_skew_secs: u64,
        next_report_interval_secs: i32,
    ) -> Self {
        Self {
            shared_secret: Arc::new(shared_secret),
            nonce_storage: Arc::new(nonce_storage),
            max_clock_skew_secs: if max_clock_skew_secs == 0 {
                300
            } else {
                max_clock_skew_secs
            },
            next_report_interval_secs,
            nodes: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    async fn verify_credential(
        &self,
        credential: &NonceCredential,
        payload: String,
    ) -> Result<(), Status> {
        let nonce_credential = nonce_auth::NonceCredential {
            timestamp: credential.timestamp,
            nonce: credential.nonce.clone(),
            signature: credential.signature.clone(),
        };

        CredentialVerifier::new(self.nonce_storage.clone())
            .with_secret(&self.shared_secret)
            .with_time_window(Duration::from_secs(self.max_clock_skew_secs))
            .with_storage_ttl(Duration::from_secs(self.max_clock_skew_secs + 300))
            .verify(&nonce_credential, payload.as_bytes())
            .await
            .map_err(|e| match e {
                NonceError::DuplicateNonce => Status::unauthenticated("duplicate nonce"),
                NonceError::TimestampOutOfWindow => {
                    Status::unauthenticated("timestamp outside allowed window")
                }
                NonceError::InvalidSignature => Status::unauthenticated("invalid signature"),
                _ => Status::internal(format!("credential verification failed: {e}")),
            })
    }
}

fn build_registration_fingerprint(request: &RegisterNodeRequest) -> String {
    use sha2::{Digest, Sha256};

    let mut tags = request.service_tags.clone();
    tags.sort();
    tags.dedup();

    let mut service_entries = request
        .services
        .iter()
        .map(|svc| {
            let mut svc_tags = svc.tags.clone();
            svc_tags.sort();
            format!(
                "{}|{}|{}|{}|{}|{}|{}",
                svc.name,
                svc.r#type,
                svc.domain_name,
                svc.port_info,
                svc.status,
                svc.url.clone().unwrap_or_default(),
                svc_tags.join(",")
            )
        })
        .collect::<Vec<_>>();
    service_entries.sort();

    let location = request.location.clone().unwrap_or_default();
    let power_level = request.power_reserve_level_init.unwrap_or(0);

    let payload = format!(
        "{}|{}|{}|{}|{}|{}|{}",
        request.node_id,
        request.agent_addr,
        request.location_tag,
        location,
        power_level,
        tags.join(","),
        service_entries.join(";"),
    );

    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    hex::encode(hasher.finalize())
}

#[tonic::async_trait]
impl SupervisorService for TestSupervisorService {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        let fingerprint = build_registration_fingerprint(&req);
        let payload = format!("register:{}:{}", req.node_id, fingerprint);

        self.verify_credential(&req.credential, payload).await?;

        let mut nodes = self.nodes.write().await;
        let state = nodes.entry(req.node_id.clone()).or_default();
        state.last_register_request = Some(req);

        let response = RegisterNodeResponse {
            success: true,
            error_message: None,
            server_timestamp: chrono::Utc::now().timestamp(),
            heartbeat_interval_secs: 5,
            resource_version: Some(1),
            registered_at_iso: None,
        };

        Ok(Response::new(response))
    }

    async fn report(
        &self,
        request: Request<ReportRequest>,
    ) -> Result<Response<ReportResponse>, Status> {
        let req = request.into_inner();
        let payload = format!("report:{}:{}", req.node_id, req.timestamp);

        self.verify_credential(&req.credential, payload).await?;

        let mut nodes = self.nodes.write().await;
        let state = nodes.entry(req.node_id.clone()).or_default();
        state.last_report_request = Some(req);
        state.report_count += 1;

        let response = ReportResponse {
            received: true,
            server_timestamp: chrono::Utc::now().timestamp(),
            next_report_interval_secs: self.next_report_interval_secs,
            directive: None,
        };

        Ok(Response::new(response))
    }

    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let req = request.into_inner();
        let payload = format!("health_check:{}", req.node_id);

        self.verify_credential(&req.credential, payload).await?;
        let mut nodes = self.nodes.write().await;
        let state = nodes.entry(req.node_id.clone()).or_default();
        state.health_check_count += 1;

        let response = HealthCheckResponse {
            healthy: true,
            server_timestamp: chrono::Utc::now().timestamp(),
            latency_ms: 1,
        };

        Ok(Response::new(response))
    }
}

async fn spawn_test_supervisor(
    shared_secret: Vec<u8>,
    max_clock_skew_secs: u64,
    next_report_interval_secs: i32,
) -> Result<(SocketAddr, TempDir, JoinHandle<()>, NodeMap), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let storage = SqliteNonceStorage::new_async(temp_dir.path()).await?;

    let service = TestSupervisorService::new(
        shared_secret,
        storage,
        max_clock_skew_secs,
        next_report_interval_secs,
    );
    let nodes = service.nodes.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(SupervisorServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    Ok((addr, temp_dir, handle, nodes))
}

fn build_register_request(node_id: &str, shared_secret: &[u8]) -> RegisterNodeRequest {
    let request = RegisterNodeRequest {
        node_id: node_id.to_string(),
        name: "test-node".to_string(),
        location_tag: "test-location".to_string(),
        version: "0.0.1".to_string(),
        agent_addr: "127.0.0.1:60000".to_string(),
        credential: NonceCredential::default(),
        location: None,
        service_tags: vec![],
        power_reserve_level_init: Some(1),
        services: vec![],
    };

    let fingerprint = build_registration_fingerprint(&request);
    let payload = format!("register:{node_id}:{fingerprint}");
    let credential = CredentialBuilder::new(shared_secret)
        .sign(payload.as_bytes())
        .expect("credential generation should succeed");
    let credential = supervit::nonce_auth::to_proto_credential(credential);

    RegisterNodeRequest {
        credential,
        ..request
    }
}

fn build_register_request_with_timestamp(
    node_id: &str,
    shared_secret: &[u8],
    timestamp: u64,
) -> RegisterNodeRequest {
    let request = RegisterNodeRequest {
        node_id: node_id.to_string(),
        name: "test-node".to_string(),
        location_tag: "test-location".to_string(),
        version: "0.0.1".to_string(),
        agent_addr: "127.0.0.1:60000".to_string(),
        credential: NonceCredential::default(),
        location: None,
        service_tags: vec![],
        power_reserve_level_init: Some(1),
        services: vec![],
    };

    let fingerprint = build_registration_fingerprint(&request);
    let payload = format!("register:{node_id}:{fingerprint}");
    let credential = CredentialBuilder::new(shared_secret)
        .with_time_provider(move || Ok(timestamp))
        .sign(payload.as_bytes())
        .expect("credential generation should succeed");
    let credential = supervit::nonce_auth::to_proto_credential(credential);

    RegisterNodeRequest {
        credential,
        ..request
    }
}

fn build_report_request(node_id: &str, shared_secret: &[u8]) -> ReportRequest {
    let timestamp = chrono::Utc::now().timestamp();
    let payload = format!("report:{node_id}:{timestamp}");
    let credential = CredentialBuilder::new(shared_secret)
        .sign(payload.as_bytes())
        .expect("credential generation should succeed");
    let credential = supervit::nonce_auth::to_proto_credential(credential);

    ReportRequest {
        node_id: node_id.to_string(),
        timestamp,
        location_tag: "test-location".to_string(),
        version: "0.0.1".to_string(),
        name: "test-node".to_string(),
        power_reserve_level: 1,
        metrics: None,
        services: vec![],
        credential,
        realm_sync_version: 1,
    }
}

fn build_health_check_request(node_id: &str, shared_secret: &[u8]) -> HealthCheckRequest {
    let payload = format!("health_check:{node_id}");
    let credential = CredentialBuilder::new(shared_secret)
        .sign(payload.as_bytes())
        .expect("credential generation should succeed");
    let credential = supervit::nonce_auth::to_proto_credential(credential);

    HealthCheckRequest {
        node_id: node_id.to_string(),
        credential,
    }
}

fn build_supervit_config(
    node_id: &str,
    endpoint: String,
    shared_secret_hex: &str,
) -> SupervitConfig {
    SupervitConfig {
        node_id: node_id.to_string(),
        endpoint,
        location_tag: "test-location".to_string(),
        name: Some("test-node".to_string()),
        location: Some("rack-a1".to_string()),
        agent_addr: "127.0.0.1:60000".to_string(),
        shared_secret: Some(shared_secret_hex.to_string()),
        service_tags: vec!["beta".to_string(), "alpha".to_string(), "beta".to_string()],
        status_report_interval_secs: 1,
        ..Default::default()
    }
}

async fn build_service_collector_with_entries() -> ServiceCollector {
    let collector = ServiceCollector::new();
    collector
        .insert(
            "turn".to_string(),
            ServiceInfo {
                name: "turn-service".to_string(),
                service_type: ServiceType::Turn,
                domain_name: "turn:example.com".to_string(),
                port_info: "3478".to_string(),
                status: ServiceState::Running("turn:example.com:3478".to_string()),
                description: None,
            },
        )
        .await;
    collector
        .insert(
            "ks".to_string(),
            ServiceInfo {
                name: "ks-service".to_string(),
                service_type: ServiceType::Ks,
                domain_name: "http://example.com".to_string(),
                port_info: "8080".to_string(),
                status: ServiceState::Error("degraded".to_string()),
                description: None,
            },
        )
        .await;
    collector
}

#[tokio::test]
async fn register_report_health_flow_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    let shared_secret = hex::decode(TEST_SHARED_SECRET)?;
    let (addr, _temp_dir, handle, _nodes) =
        spawn_test_supervisor(shared_secret.clone(), 300, 15).await?;

    // Wait briefly for server to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    let endpoint = format!("http://{addr}");
    let mut client = SupervisorServiceClient::connect(endpoint).await?;

    let node_id = "integration-node";

    let register_request = build_register_request(node_id, &shared_secret);
    let register_response = client.register_node(register_request).await?.into_inner();
    assert!(register_response.success);
    assert_eq!(register_response.heartbeat_interval_secs, 5);

    let report_request = build_report_request(node_id, &shared_secret);
    let report_response = client.report(report_request).await?.into_inner();
    assert!(report_response.received);
    assert_eq!(report_response.next_report_interval_secs, 15);

    let health_request = build_health_check_request(node_id, &shared_secret);
    let health_response = client.health_check(health_request).await?.into_inner();
    assert!(health_response.healthy);

    handle.abort();
    let _ = handle.await;

    Ok(())
}

#[tokio::test]
async fn register_node_rejects_invalid_signature() -> Result<(), Box<dyn std::error::Error>> {
    let shared_secret = hex::decode(TEST_SHARED_SECRET)?;
    let wrong_secret =
        hex::decode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;
    let (addr, _temp_dir, handle, _nodes) =
        spawn_test_supervisor(shared_secret.clone(), 300, 15).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let endpoint = format!("http://{addr}");
    let mut client = SupervisorServiceClient::connect(endpoint).await?;

    let request = build_register_request("invalid-signature-node", &wrong_secret);
    let err = client
        .register_node(request)
        .await
        .expect_err("request should fail");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert!(
        err.message().contains("invalid signature"),
        "unexpected error: {}",
        err.message()
    );

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn register_node_rejects_timestamp_out_of_window() -> Result<(), Box<dyn std::error::Error>> {
    let shared_secret = hex::decode(TEST_SHARED_SECRET)?;
    let skew_secs = 30_u64;
    let (addr, _temp_dir, handle, _nodes) =
        spawn_test_supervisor(shared_secret.clone(), skew_secs, 15).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let endpoint = format!("http://{addr}");
    let mut client = SupervisorServiceClient::connect(endpoint).await?;

    let stale_ts = (chrono::Utc::now().timestamp() as u64).saturating_sub(skew_secs + 120);
    let request =
        build_register_request_with_timestamp("stale-timestamp-node", &shared_secret, stale_ts);

    let err = client
        .register_node(request)
        .await
        .expect_err("request should fail");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert!(
        err.message().contains("timestamp outside allowed window"),
        "unexpected error: {}",
        err.message()
    );

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn register_node_rejects_duplicate_nonce_replay() -> Result<(), Box<dyn std::error::Error>> {
    let shared_secret = hex::decode(TEST_SHARED_SECRET)?;
    let (addr, _temp_dir, handle, _nodes) =
        spawn_test_supervisor(shared_secret.clone(), 300, 15).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let endpoint = format!("http://{addr}");
    let mut client = SupervisorServiceClient::connect(endpoint).await?;

    let request = build_register_request("duplicate-nonce-node", &shared_secret);
    let first = client.register_node(request.clone()).await?.into_inner();
    assert!(first.success);

    let err = client
        .register_node(request)
        .await
        .expect_err("replay should fail");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert!(
        err.message().contains("duplicate nonce"),
        "unexpected error: {}",
        err.message()
    );

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn supervit_client_end_to_end_flow_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    let shared_secret = hex::decode(TEST_SHARED_SECRET)?;
    let (addr, _temp_dir, handle, nodes) = spawn_test_supervisor(shared_secret, 300, 15).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let collector = build_service_collector_with_entries().await;
    let endpoint = format!("http://{addr}");
    let config = build_supervit_config("supervit-client-node", endpoint, TEST_SHARED_SECRET);
    let mut client = SupervitClient::new(config, collector)?;

    client.connect().await?;
    let register_response = client.register_node().await?;
    assert!(register_response.success);

    let report_response = client.report().await?;
    assert!(report_response.received);

    let health_response = client.health_check().await?;
    assert!(health_response.healthy);

    client.disconnect();
    let disconnected = client
        .health_check()
        .await
        .expect_err("health_check should fail after disconnect");
    assert!(matches!(disconnected, SupervitError::ConnectionClosed));

    let nodes_read = nodes.read().await;
    let state = nodes_read
        .get("supervit-client-node")
        .expect("node state should exist");

    let register = state
        .last_register_request
        .as_ref()
        .expect("register request should be captured");
    assert_eq!(register.name, "test-node");
    assert_eq!(register.location_tag, "test-location");
    assert_eq!(register.location.as_deref(), Some("rack-a1"));
    assert_eq!(
        register.service_tags,
        vec!["alpha".to_string(), "beta".to_string()]
    );
    assert_eq!(register.services.len(), 2);
    assert!(
        register
            .services
            .iter()
            .any(|svc| svc.status == ServiceAdvertisementStatus::Running as i32)
    );
    assert!(
        register
            .services
            .iter()
            .any(|svc| svc.status == ServiceAdvertisementStatus::Error as i32)
    );

    let report = state
        .last_report_request
        .as_ref()
        .expect("report request should be captured");
    assert_eq!(report.node_id, "supervit-client-node");
    assert_eq!(report.location_tag, "test-location");
    assert_eq!(report.name, "test-node");
    assert_eq!(report.services.len(), 2);
    assert_eq!(state.health_check_count, 1);

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn supervit_client_methods_require_connection() -> Result<(), Box<dyn std::error::Error>> {
    let collector = build_service_collector_with_entries().await;
    let config = build_supervit_config(
        "supervit-disconnected-node",
        "http://127.0.0.1:50051".to_string(),
        TEST_SHARED_SECRET,
    );
    let mut client = SupervitClient::new(config, collector)?;

    let register_err = client
        .register_node()
        .await
        .expect_err("register_node should fail when disconnected");
    assert!(matches!(register_err, SupervitError::ConnectionClosed));

    let report_err = client
        .report()
        .await
        .expect_err("report should fail when disconnected");
    assert!(matches!(report_err, SupervitError::ConnectionClosed));

    let health_err = client
        .health_check()
        .await
        .expect_err("health_check should fail when disconnected");
    assert!(matches!(health_err, SupervitError::ConnectionClosed));

    Ok(())
}

#[tokio::test]
async fn supervit_client_register_rejects_wrong_secret() -> Result<(), Box<dyn std::error::Error>> {
    let shared_secret = hex::decode(TEST_SHARED_SECRET)?;
    let (addr, _temp_dir, handle, nodes) = spawn_test_supervisor(shared_secret, 300, 15).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let collector = ServiceCollector::new();
    let wrong_secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let endpoint = format!("http://{addr}");
    let config = build_supervit_config("wrong-secret-node", endpoint, wrong_secret);
    let mut client = SupervitClient::new(config, collector)?;

    client.connect().await?;
    let err = client
        .register_node()
        .await
        .expect_err("registration should fail with wrong secret");
    match err {
        SupervitError::Status(status) => {
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
            assert!(
                status.message().contains("invalid signature"),
                "unexpected error: {}",
                status.message()
            );
        }
        other => panic!("unexpected error variant: {other}"),
    }

    assert!(
        !nodes.read().await.contains_key("wrong-secret-node"),
        "node should not be registered after auth failure"
    );

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn supervit_client_status_reporting_task_sends_reports()
-> Result<(), Box<dyn std::error::Error>> {
    let shared_secret = hex::decode(TEST_SHARED_SECRET)?;
    let (addr, _temp_dir, handle, nodes) = spawn_test_supervisor(shared_secret, 300, 1).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let collector = build_service_collector_with_entries().await;
    let endpoint = format!("http://{addr}");
    let config = build_supervit_config("status-report-node", endpoint, TEST_SHARED_SECRET);
    let mut client = SupervitClient::new(config, collector)?;

    client.connect().await?;
    let register_response = client.register_node().await?;
    assert!(register_response.success);

    client.start_status_reporting().await?;

    let mut reported = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let has_report = {
            let nodes_read = nodes.read().await;
            nodes_read
                .get("status-report-node")
                .and_then(|state| state.last_report_request.as_ref())
                .is_some()
        };
        if has_report {
            reported = true;
            break;
        }
    }
    assert!(
        reported,
        "status reporting task should send report within timeout"
    );

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn supervit_client_connect_rejects_invalid_endpoint_url()
-> Result<(), Box<dyn std::error::Error>> {
    let collector = ServiceCollector::new();
    let config = build_supervit_config(
        "invalid-endpoint-node",
        "http://[::1".to_string(),
        TEST_SHARED_SECRET,
    );
    let mut client = SupervitClient::new(config, collector)?;

    let err = client
        .connect()
        .await
        .expect_err("connect should fail for malformed endpoint url");

    match err {
        SupervitError::Config(msg) => {
            assert!(
                msg.contains("Invalid server address"),
                "unexpected config error: {msg}"
            );
        }
        other => panic!("expected config error, got {other}"),
    }

    Ok(())
}

#[tokio::test]
async fn supervit_client_connect_fails_when_supervisor_unreachable()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    drop(listener);

    let collector = ServiceCollector::new();
    let config = build_supervit_config(
        "unreachable-endpoint-node",
        format!("http://{addr}"),
        TEST_SHARED_SECRET,
    );
    let mut client = SupervitClient::new(config, collector)?;

    let err = client
        .connect()
        .await
        .expect_err("connect should fail when endpoint is unreachable");
    assert!(matches!(err, SupervitError::Transport(_)));

    Ok(())
}

#[tokio::test]
async fn supervit_client_status_reporting_adjusts_interval_from_server_response()
-> Result<(), Box<dyn std::error::Error>> {
    let shared_secret = hex::decode(TEST_SHARED_SECRET)?;
    let (addr, _temp_dir, handle, nodes) = spawn_test_supervisor(shared_secret, 300, 5).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let collector = build_service_collector_with_entries().await;
    let endpoint = format!("http://{addr}");
    let config = build_supervit_config("interval-adjust-node", endpoint, TEST_SHARED_SECRET);
    let mut client = SupervitClient::new(config, collector)?;

    client.connect().await?;
    let register_response = client.register_node().await?;
    assert!(register_response.success);

    client.start_status_reporting().await?;

    tokio::time::sleep(Duration::from_millis(3500)).await;
    let report_count = {
        let nodes_read = nodes.read().await;
        nodes_read
            .get("interval-adjust-node")
            .map(|state| state.report_count)
            .unwrap_or(0)
    };

    assert!(
        report_count >= 1,
        "status reporting should emit at least one report"
    );
    assert!(
        report_count <= 2,
        "server-directed interval adjustment should reduce report frequency, got {report_count}"
    );

    handle.abort();
    let _ = handle.await;
    Ok(())
}
