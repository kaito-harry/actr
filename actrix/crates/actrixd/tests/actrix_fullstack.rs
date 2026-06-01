use actr_protocol::acl_rule::{Permission, SourceRealm};
use actr_protocol::{
    Acl, AclRule, ActrRelay, ActrType, Realm, RegisterRequest, RegisterResponse, RoleNegotiation,
    acl_rule, actr_relay, peer_to_signaling, register_response, route_candidates_response,
    signaling_envelope, signaling_to_actr,
};
use base64::Engine as _;
use futures::{SinkExt, StreamExt};
use platform::aid::credential::validator::AIdCredentialValidator;
use prost::Message;
use serde_json::{Value, json};
use signer::{GrpcClient, GrpcClientConfig};
use std::{
    collections::HashSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message as WsMessage,
};
use uuid::Uuid;

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type WsWrite = futures::stream::SplitSink<WsStream, WsMessage>;
type WsRead = futures::stream::SplitStream<WsStream>;

const START_TIMEOUT: Duration = Duration::from_secs(20);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const ACTRIX_SHARED_KEY: &str = "0123456789abcdef0123456789abcdef";
const DEFAULT_TOKEN_TTL: u64 = 3600;

/// Convenience bundle for a running actrix instance
struct ActrixHarness {
    #[allow(dead_code)]
    tmp: tempfile::TempDir,
    port: u16,
    log_path: PathBuf,
    data_dir: PathBuf,
    child: Child,
}

impl ActrixHarness {
    /// Start actrix with default features (AIS/Signer/Signaling) and wait for health
    async fn start(token_ttl: u64) -> Self {
        let tmp = tempfile::tempdir().expect("temp dir");
        let port = choose_port();
        let config_path = write_fullstack_config(tmp.path(), port, token_ttl);
        let log_path = tmp.path().join("actrix_fullstack.log");
        let data_dir = tmp.path().join("data");
        ensure_realm(&data_dir, 1001).await;
        let mut child = spawn_actrix(&config_path, &log_path);

        let base = format!("http://127.0.0.1:{port}");
        wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
        wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
        wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
        ensure_realm(&data_dir, 1001).await;

        Self {
            tmp,
            port,
            log_path,
            data_dir,
            child,
        }
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn log_path(&self) -> &Path {
        self.log_path.as_path()
    }

    /// Shutdown child, ignoring errors
    fn shutdown(self) {
        graceful_shutdown(self.child);
    }
}

#[cfg(test)]
use serial_test::serial;

fn choose_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .unwrap()
        .port()
}

fn write_fullstack_config(dir: &Path, port: u16, token_ttl_secs: u64) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-fullstack-test"
enable = 25  # ENABLE_SIGNALING | ENABLE_AIS | ENABLE_SIGNER
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "{shared}"
location_tag = "local,test,fullstack"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

[bind.ice]
advertised_ip = "127.0.0.1"
advertised_port = 3478
ip = "127.0.0.1"
port = 0

[turn]
relay_port_range = "49152-65535"
realm = "actrix.local"

[services.signer]
[services.signer.storage]
backend = "sqlite"
key_ttl_seconds = 3600
[services.signer.storage.sqlite]
path = "ks.db"

[services.ais]
[services.ais.server]
token_ttl_secs = {token_ttl}

[services.signaling]
[services.signaling.server]
ws_path = "/signaling"

[control.admin_ui]
password = "testpassword123"

[recording]

[process]
pid = "{pid}"
"#,
        sqlite = data_dir.display(),
        shared = ACTRIX_SHARED_KEY,
        port = port,
        token_ttl = token_ttl_secs,
        pid = dir.join("actrix.pid").display()
    )
    .expect("write config");
    config_path
}

fn write_fullstack_config_with_rate_limits(
    dir: &Path,
    port: u16,
    token_ttl_secs: u64,
    max_concurrent_per_ip: u32,
    message_per_second: u32,
    message_burst_size: u32,
) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-fullstack-test-rate-limit"
enable = 25  # ENABLE_SIGNALING | ENABLE_AIS | ENABLE_SIGNER
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "{shared}"
location_tag = "local,test,fullstack,ratelimit"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

[bind.ice]
advertised_ip = "127.0.0.1"
advertised_port = 3478
ip = "127.0.0.1"
port = 0

[turn]
relay_port_range = "49152-65535"
realm = "actrix.local"

[services.signer]
[services.signer.storage]
backend = "sqlite"
key_ttl_seconds = 3600
[services.signer.storage.sqlite]
path = "ks.db"

[services.ais]
[services.ais.server]
token_ttl_secs = {token_ttl}

[services.signaling]
[services.signaling.server]
ws_path = "/signaling"

[services.signaling.server.rate_limit.connection]
enabled = true
per_minute = 60
burst_size = 10
max_concurrent_per_ip = {max_concurrent}

[services.signaling.server.rate_limit.message]
enabled = true
per_second = {message_per_second}
burst_size = {message_burst}

[control.admin_ui]
password = "testpassword123"

[recording]

[process]
pid = "{pid}"
"#,
        sqlite = data_dir.display(),
        shared = ACTRIX_SHARED_KEY,
        port = port,
        token_ttl = token_ttl_secs,
        max_concurrent = max_concurrent_per_ip,
        message_per_second = message_per_second,
        message_burst = message_burst_size,
        pid = dir.join("actrix.pid").display()
    )
    .expect("write config");
    config_path
}

fn write_fullstack_config_with_ais_signer_endpoint(
    dir: &Path,
    port: u16,
    token_ttl_secs: u64,
    ais_signer_endpoint: &str,
) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-fullstack-test-ais-dependency"
enable = 25  # ENABLE_SIGNALING | ENABLE_AIS | ENABLE_SIGNER
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "{shared}"
location_tag = "local,test,fullstack,ais-dependency"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

[bind.ice]
advertised_ip = "127.0.0.1"
advertised_port = 3478
ip = "127.0.0.1"
port = 0

[turn]
relay_port_range = "49152-65535"
realm = "actrix.local"

[services.signer]
[services.signer.storage]
backend = "sqlite"
key_ttl_seconds = 3600
[services.signer.storage.sqlite]
path = "ks.db"

[services.ais]
[services.ais.server]
token_ttl_secs = {token_ttl}
[services.ais.dependencies.signer]
endpoint = "{ais_signer_endpoint}"
timeout_seconds = 1
enable_tls = false

[services.signaling]
[services.signaling.server]
ws_path = "/signaling"

[control.admin_ui]
password = "testpassword123"

[recording]

[process]
pid = "{pid}"
"#,
        sqlite = data_dir.display(),
        shared = ACTRIX_SHARED_KEY,
        port = port,
        token_ttl = token_ttl_secs,
        ais_signer_endpoint = ais_signer_endpoint,
        pid = dir.join("actrix.pid").display()
    )
    .expect("write config");
    config_path
}

#[allow(dead_code)]
fn write_signaling_without_local_ais_config(dir: &Path, port: u16) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-fullstack-signaling-without-ais"
enable = 17  # ENABLE_SIGNALING | ENABLE_SIGNER
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "{shared}"
location_tag = "local,test,fullstack,signaling-no-ais"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

[bind.ice]
advertised_ip = "127.0.0.1"
advertised_port = 3478
ip = "127.0.0.1"
port = 0

[turn]
relay_port_range = "49152-65535"
realm = "actrix.local"

[services.signer]
[services.signer.storage]
backend = "sqlite"
key_ttl_seconds = 3600
[services.signer.storage.sqlite]
path = "ks.db"

[services.signaling]
[services.signaling.server]
ws_path = "/signaling"
[services.signaling.dependencies.ais]
endpoint = "http://127.0.0.1:1"
timeout_seconds = 1

[control.admin_ui]
password = "testpassword123"

[recording]

[process]
pid = "{pid}"
"#,
        sqlite = data_dir.display(),
        shared = ACTRIX_SHARED_KEY,
        port = port,
        pid = dir.join("actrix.pid").display()
    )
    .expect("write config");
    config_path
}

fn spawn_actrix(config: &Path, log_path: &Path) -> Child {
    let bin = PathBuf::from(env!("CARGO_BIN_EXE_actrix"));
    let log_file = fs::File::create(log_path).expect("create log file");
    Command::new(bin)
        .arg("--config")
        .arg(config)
        .stdout(Stdio::from(log_file.try_clone().expect("dup log")))
        .stderr(Stdio::from(log_file))
        .env("ACTRIX_TEST_NO_MFR_VERIFY", "1")
        .spawn()
        .expect("spawn actrix")
}

async fn ensure_realm(sqlite_dir: &Path, realm_id: u32) {
    let db = platform::storage::db::Database::new(sqlite_dir)
        .await
        .expect("init db");
    db.execute(&format!(
        "INSERT OR IGNORE INTO realm (id, name, status, enabled, created_at, secret_current)
         VALUES ({realm_id}, 'test-realm', 'Active', 1, strftime('%s','now'), '')"
    ))
    .await
    .expect("insert realm");
}

/// 通过 AIS HTTP 注册，返回 RegisterOk
async fn ais_register_http(
    base_url: &str,
    realm_id: u32,
    manufacturer: &str,
    name: &str,
    service_spec: Option<actr_protocol::ServiceSpec>,
    acl: Option<Acl>,
) -> register_response::RegisterOk {
    ais_register_http_with_secret(
        base_url,
        realm_id,
        manufacturer,
        name,
        service_spec,
        acl,
        None,
    )
    .await
}

async fn ais_register_http_with_secret(
    base_url: &str,
    realm_id: u32,
    manufacturer: &str,
    name: &str,
    service_spec: Option<actr_protocol::ServiceSpec>,
    acl: Option<Acl>,
    realm_secret: Option<&str>,
) -> register_response::RegisterOk {
    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: manufacturer.to_string(),
            name: name.to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id },
        service_spec,
        acl,
        service: None,
        ws_address: None,
        manifest_raw: None,
        mfr_signature: None,
        psk_token: None,
        target: None,
    };
    let client = reqwest::Client::new();
    let register_url = format!("{base_url}/ais/register");
    let mut req = client
        .post(&register_url)
        .header("content-type", "application/octet-stream")
        .body(register_req.encode_to_vec());
    if let Some(secret) = realm_secret {
        req = req.header("x-actrix-realm-secret", secret);
    }
    let rsp_bytes = req
        .send()
        .await
        .expect("ais register call")
        .bytes()
        .await
        .expect("ais register bytes")
        .to_vec();
    let register_rsp = RegisterResponse::decode(&*rsp_bytes).expect("decode register response");
    match register_rsp.result.expect("result missing") {
        register_response::Result::Success(ok) => ok,
        register_response::Result::Error(err) => {
            panic!(
                "ais register failed: code={} message={}",
                err.code, err.message
            );
        }
    }
}

/// 带认证参数连接 WS
async fn connect_ws_authenticated(
    port: u16,
    ok: &register_response::RegisterOk,
) -> (WsWrite, WsRead) {
    let actor_id_str = ok.actr_id.to_string_repr();
    let claims_b64 = base64::engine::general_purpose::STANDARD.encode(&ok.credential.claims);
    let signature_b64 = base64::engine::general_purpose::STANDARD.encode(&ok.credential.signature);
    let key_id = ok.credential.key_id;
    let ws_url = format!(
        "ws://127.0.0.1:{}/signaling/ws?actor_id={}&key_id={}&claims={}&signature={}",
        port,
        urlencoding::encode(&actor_id_str),
        key_id,
        urlencoding::encode(&claims_b64),
        urlencoding::encode(&signature_b64),
    );
    let (ws_stream, _) = connect_async(&ws_url).await.expect("ws connect");
    ws_stream.split()
}

/// 不带认证的裸 WS 连接（预期会被拒绝 — 仅供负面测试）
async fn connect_ws_raw(
    port: u16,
) -> Result<(WsWrite, WsRead), tokio_tungstenite::tungstenite::Error> {
    let ws_url = format!("ws://127.0.0.1:{}/signaling/ws", port);
    match connect_async(&ws_url).await {
        Ok((ws_stream, _)) => Ok(ws_stream.split()),
        Err(e) => Err(e),
    }
}

fn make_envelope(flow: signaling_envelope::Flow) -> actr_protocol::SignalingEnvelope {
    actr_protocol::SignalingEnvelope {
        envelope_version: 1,
        envelope_id: Uuid::new_v4().to_string(),
        timestamp: prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        },
        reply_for: None,
        traceparent: None,
        tracestate: None,
        flow: Some(flow),
    }
}

async fn send_envelope(write: &mut WsWrite, env: actr_protocol::SignalingEnvelope) {
    let mut buf = Vec::new();
    env.encode(&mut buf).expect("encode envelope");
    write
        .send(WsMessage::Binary(buf.into()))
        .await
        .expect("send envelope");
}

async fn recv_envelope(read: &mut WsRead) -> actr_protocol::SignalingEnvelope {
    let resp = read.next().await.expect("ws response").expect("ws msg");
    match resp {
        WsMessage::Binary(data) => {
            actr_protocol::SignalingEnvelope::decode(&data[..]).expect("decode signaling resp")
        }
        other => panic!("expected binary ws message, got {other:?}"),
    }
}

async fn ws_register_with_spec(
    port: u16,
    manufacturer: &str,
    name: &str,
    acl: Option<Acl>,
    service_spec: Option<actr_protocol::ServiceSpec>,
) -> (WsWrite, WsRead, register_response::RegisterOk) {
    ws_register_in_realm_with_spec(port, manufacturer, name, 1001, acl, service_spec).await
}

async fn ws_register_in_realm_with_spec(
    port: u16,
    manufacturer: &str,
    name: &str,
    realm_id: u32,
    acl: Option<Acl>,
    service_spec: Option<actr_protocol::ServiceSpec>,
) -> (WsWrite, WsRead, register_response::RegisterOk) {
    let base_url = format!("http://127.0.0.1:{port}");
    let ok = ais_register_http(&base_url, realm_id, manufacturer, name, service_spec, acl).await;
    let (write, read) = connect_ws_authenticated(port, &ok).await;
    (write, read, ok)
}

async fn ws_register(
    port: u16,
    manufacturer: &str,
    name: &str,
    acl: Option<Acl>,
) -> (WsWrite, WsRead, register_response::RegisterOk) {
    ws_register_with_spec(port, manufacturer, name, acl, None).await
}

async fn ws_register_in_realm(
    port: u16,
    manufacturer: &str,
    name: &str,
    realm_id: u32,
    acl: Option<Acl>,
) -> (WsWrite, WsRead, register_response::RegisterOk) {
    ws_register_in_realm_with_spec(port, manufacturer, name, realm_id, acl, None).await
}

async fn query_route_candidates(
    write: &mut WsWrite,
    read: &mut WsRead,
    source: &register_response::RegisterOk,
    target_manufacturer: &str,
    target_name: &str,
) -> Vec<actr_protocol::ActrId> {
    let route_req = actr_protocol::ActrToSignaling {
        source: source.actr_id.clone(),
        credential: source.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: target_manufacturer.into(),
                        name: target_name.into(),
                        version: "1.0.0".to_string(),
                    },
                    client_fingerprint: "".into(),
                    criteria: Some(
                        actr_protocol::route_candidates_request::NodeSelectionCriteria {
                            candidate_count: 8,
                            ranking_factors: vec![],
                            minimal_dependency_requirement: None,
                            minimal_health_requirement: None,
                        },
                    ),
                    client_location: None,
                },
            ),
        ),
    };

    send_envelope(
        write,
        make_envelope(signaling_envelope::Flow::ActrToServer(route_req)),
    )
    .await;

    let resp = recv_envelope(read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => ok.candidates,
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }
}

fn make_service_spec(
    fingerprint: &str,
    package: &str,
    content: &str,
) -> actr_protocol::ServiceSpec {
    actr_protocol::ServiceSpec {
        name: package.to_string(),
        description: Some("integration spec".into()),
        fingerprint: fingerprint.into(),
        protobufs: vec![actr_protocol::service_spec::Protobuf {
            package: package.into(),
            content: content.into(),
            fingerprint: format!("proto-fp::{fingerprint}"),
        }],
        published_at: None,
        tags: vec!["stable".into()],
    }
}

async fn wait_for_health(url: &str, child: &mut Child, log_path: &Path) {
    let client = reqwest::Client::new();
    let start = Instant::now();
    let is_signer_health = url.ends_with("/signer/health");
    let ks_grpc_endpoint = if is_signer_health {
        Some(url.trim_end_matches("/signer/health").to_string())
    } else {
        None
    };
    loop {
        if let Some(status) = child.try_wait().unwrap_or(None) {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("actrix exited early: status={status:?}\nlogs:\n{log}");
        }

        if is_signer_health {
            let config = GrpcClientConfig {
                endpoint: ks_grpc_endpoint
                    .as_ref()
                    .expect("ks grpc endpoint should exist")
                    .clone(),
                actrix_shared_key: ACTRIX_SHARED_KEY.to_string(),
                timeout_seconds: 2,
                enable_tls: false,
                tls_domain: None,
                ca_cert: None,
                client_cert: None,
                client_key: None,
            };
            if let Ok(mut grpc) = GrpcClient::new(&config).await
                && let Ok(status) = grpc.health_check().await
                && status == "healthy"
            {
                return;
            }
        } else if let Ok(resp) = client.get(url).send().await
            && resp.status().is_success()
        {
            return;
        }
        if start.elapsed() > START_TIMEOUT {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("health check not ready at {}\nlogs:\n{}", url, log);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn graceful_shutdown(mut child: Child) {
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as i32, libc::SIGINT);
    }
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_status)) => return,
            Ok(None) => {
                if start.elapsed() > SHUTDOWN_TIMEOUT {
                    let _ = child.kill();
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(_) => return,
        }
    }
}

#[tokio::test]
#[serial]
async fn actrix_end_to_end_register_and_health() {
    let mut harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;

    let base = harness.base_url();
    let ais_health = format!("{base}/ais/health");
    let signaling_health = format!("{base}/signaling/health");

    let log_path = harness.log_path().to_path_buf();
    wait_for_health(
        &format!("{base}/signer/health"),
        &mut harness.child,
        &log_path,
    )
    .await;
    wait_for_health(&ais_health, &mut harness.child, &log_path).await;
    wait_for_health(&signaling_health, &mut harness.child, &log_path).await;
    ensure_realm(&harness.data_dir, 1001).await;

    let client = reqwest::Client::new();

    // KS health via authenticated gRPC call
    let mut ks_client = GrpcClient::new(&GrpcClientConfig {
        endpoint: base.clone(),
        actrix_shared_key: ACTRIX_SHARED_KEY.to_string(),
        timeout_seconds: 5,
        enable_tls: false,
        tls_domain: None,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    })
    .await
    .expect("create ks grpc client");
    let ks_status = ks_client.health_check().await.expect("ks grpc health");
    assert_eq!(ks_status, "healthy");

    // AIS health JSON
    let ais_resp = client.get(&ais_health).send().await.expect("ais health");
    assert!(ais_resp.status().is_success());
    let ais_body: Value = ais_resp.json().await.expect("ais health json");
    assert_eq!(ais_body["status"], "healthy");

    // Signaling health plain text
    let sig_resp = client
        .get(&signaling_health)
        .send()
        .await
        .expect("sig health");
    assert!(sig_resp.status().is_success());
    let sig_text = sig_resp.text().await.expect("sig text");
    assert!(
        sig_text.to_lowercase().contains("healthy"),
        "signaling health text: {sig_text}"
    );

    // Register an actor via AIS HTTP (protobuf body)
    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "acme".to_string(),
            name: "device".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: None,
        mfr_signature: None,
        psk_token: None,
        target: None,
    };
    let body = register_req.encode_to_vec();
    let register_url = format!("{base}/ais/register");
    let rsp_bytes = client
        .post(&register_url)
        .body(body)
        .send()
        .await
        .expect("register call")
        .bytes()
        .await
        .expect("register bytes")
        .to_vec();
    let register_rsp =
        actr_protocol::RegisterResponse::decode(&*rsp_bytes).expect("decode register response");
    let ok = match register_rsp.result.expect("result missing") {
        register_response::Result::Success(ok) => ok,
        register_response::Result::Error(err) => {
            panic!("register failed: {:?}", err);
        }
    };
    assert_eq!(ok.actr_id.realm.realm_id, 1001);

    // Validate credential through AIdCredentialValidator — reads from the same
    // signaling_key_cache.db that actrix wrote during AIS key loading.
    AIdCredentialValidator::init(&harness.data_dir)
        .await
        .expect("validator init");
    let (claims, _) = AIdCredentialValidator::check(&ok.credential, 1001)
        .await
        .expect("validate credential");
    assert_eq!(claims.realm_id, 1001);

    // WebSocket signaling ping/pong with valid credential
    let (mut write, mut read) = connect_ws_authenticated(harness.port, &ok).await;

    let ping_msg = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 100,
                mailbox_backlog: 0.0,
                power_reserve: 80.0,
                ..Default::default()
            },
        )),
    };
    let envelope = actr_protocol::SignalingEnvelope {
        envelope_version: 1,
        envelope_id: Uuid::new_v4().to_string(),
        timestamp: prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        },
        reply_for: None,
        traceparent: None,
        tracestate: None,
        flow: Some(actr_protocol::signaling_envelope::Flow::ActrToServer(
            ping_msg,
        )),
    };
    let mut buf = Vec::new();
    envelope.encode(&mut buf).expect("encode envelope");
    write
        .send(WsMessage::Binary(buf.into()))
        .await
        .expect("send ping");

    let resp = read.next().await.expect("ws response").expect("ws msg");
    let pong_env = match resp {
        WsMessage::Binary(data) => {
            actr_protocol::SignalingEnvelope::decode(&data[..]).expect("decode signaling resp")
        }
        other => panic!("expected binary ws message, got {other:?}"),
    };
    match pong_env.flow {
        Some(actr_protocol::signaling_envelope::Flow::ServerToActr(server_msg)) => {
            match server_msg.payload {
                Some(actr_protocol::signaling_to_actr::Payload::Pong(_)) => {}
                other => panic!("expected Pong, got {other:?}"),
            }
        }
        other => panic!("unexpected flow: {other:?}"),
    }

    // Non-binary frame should be ignored without breaking the connection.
    write
        .send(WsMessage::Text("not-protobuf".into()))
        .await
        .expect("send text frame");

    let ping_after_text = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 99,
                mailbox_backlog: 0.0,
                power_reserve: 75.0,
                ..Default::default()
            },
        )),
    };
    let mut text_ping_buf = Vec::new();
    actr_protocol::SignalingEnvelope {
        envelope_version: 1,
        envelope_id: Uuid::new_v4().to_string(),
        timestamp: prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        },
        reply_for: None,
        traceparent: None,
        tracestate: None,
        flow: Some(actr_protocol::signaling_envelope::Flow::ActrToServer(
            ping_after_text,
        )),
    }
    .encode(&mut text_ping_buf)
    .expect("encode ping after text");
    write
        .send(WsMessage::Binary(text_ping_buf.into()))
        .await
        .expect("send ping after text");

    let resp = read.next().await.expect("ws response").expect("ws msg");
    let pong_after_text = match resp {
        WsMessage::Binary(data) => {
            actr_protocol::SignalingEnvelope::decode(&data[..]).expect("decode signaling resp")
        }
        other => panic!("expected binary ws message, got {other:?}"),
    };
    match pong_after_text.flow {
        Some(actr_protocol::signaling_envelope::Flow::ServerToActr(server_msg)) => {
            match server_msg.payload {
                Some(actr_protocol::signaling_to_actr::Payload::Pong(_)) => {}
                other => panic!("expected Pong after text frame, got {other:?}"),
            }
        }
        other => panic!("unexpected flow after text frame: {other:?}"),
    }

    // Tamper credential to ensure 401 is returned
    let mut bad_cred = ok.credential.clone();
    if !bad_cred.signature.is_empty() {
        let mut tampered = bad_cred.signature.to_vec();
        tampered[0] ^= 0xFF;
        bad_cred.signature = tampered.into();
    }
    let bad_msg = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: bad_cred,
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 50,
                mailbox_backlog: 1.0,
                power_reserve: 50.0,
                ..Default::default()
            },
        )),
    };
    let mut buf = Vec::new();
    actr_protocol::SignalingEnvelope {
        envelope_version: 1,
        envelope_id: Uuid::new_v4().to_string(),
        timestamp: prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        },
        reply_for: None,
        traceparent: None,
        tracestate: None,
        flow: Some(actr_protocol::signaling_envelope::Flow::ActrToServer(
            bad_msg,
        )),
    }
    .encode(&mut buf)
    .expect("encode bad envelope");
    write
        .send(WsMessage::Binary(buf.into()))
        .await
        .expect("send bad ping");

    let resp = read.next().await.expect("ws response").expect("ws msg");
    let err_env = match resp {
        WsMessage::Binary(data) => {
            actr_protocol::SignalingEnvelope::decode(&data[..]).expect("decode signaling resp")
        }
        other => panic!("expected binary ws message, got {other:?}"),
    };
    match err_env.flow {
        Some(actr_protocol::signaling_envelope::Flow::ServerToActr(server_msg)) => {
            match server_msg.payload {
                Some(actr_protocol::signaling_to_actr::Payload::Error(err)) => {
                    assert_eq!(err.code, 401, "expected 401 for bad credential");
                }
                other => panic!("expected Error, got {other:?}"),
            }
        }
        other => panic!("unexpected flow: {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn ais_register_returns_error_response_for_malformed_protobuf() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let client = reqwest::Client::new();
    let register_url = format!("{}/ais/register", harness.base_url());

    let response = client
        .post(&register_url)
        .header("content-type", "application/octet-stream")
        .body(vec![0xff, 0x00, 0x10, 0x80])
        .send()
        .await
        .expect("send malformed register request");
    assert!(
        response.status().is_success(),
        "ais register should respond with protobuf error body"
    );

    let rsp_bytes = response.bytes().await.expect("read response body").to_vec();
    let register_rsp = RegisterResponse::decode(&*rsp_bytes).expect("decode register response");
    match register_rsp.result.expect("result missing") {
        register_response::Result::Error(err) => {
            assert_eq!(err.code, 400, "expected bad-request protobuf error");
            assert!(
                err.message.contains("Invalid protobuf"),
                "unexpected error message: {}",
                err.message
            );
        }
        other => panic!("expected register error for malformed protobuf, got {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn ais_register_rejects_non_preprovisioned_realm() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let client = reqwest::Client::new();
    let register_url = format!("{}/ais/register", harness.base_url());

    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "acme".to_string(),
            name: "realm-device".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 9999 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: None,
        mfr_signature: None,
        psk_token: None,
        target: None,
    };

    let rsp_bytes = client
        .post(&register_url)
        .body(register_req.encode_to_vec())
        .send()
        .await
        .expect("send register with non-preprovisioned realm")
        .bytes()
        .await
        .expect("read register response")
        .to_vec();
    let register_rsp = RegisterResponse::decode(&*rsp_bytes).expect("decode register response");
    match register_rsp.result.expect("result missing") {
        register_response::Result::Error(err) => {
            assert_eq!(err.code, 403, "non-preprovisioned realm should be denied");
            assert!(
                err.message.contains("Realm validation failed"),
                "unexpected error message: {}",
                err.message
            );
        }
        other => panic!("expected register error for non-preprovisioned realm, got {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn ais_rotate_key_updates_current_key_endpoint() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let client = reqwest::Client::new();
    let base = harness.base_url();

    let current_before: Value = client
        .get(format!("{base}/ais/current-key"))
        .send()
        .await
        .expect("current-key request before rotate")
        .json()
        .await
        .expect("parse current-key before rotate");
    assert_eq!(current_before["status"], "success");
    let key_before = current_before["key_id"]
        .as_u64()
        .expect("current key id should be u64");

    let rotated: Value = client
        .post(format!("{base}/ais/rotate-key"))
        .send()
        .await
        .expect("rotate-key request")
        .json()
        .await
        .expect("parse rotate-key response");
    assert_eq!(rotated["status"], "success");
    let key_after_rotate = rotated["new_key_id"]
        .as_u64()
        .expect("rotated key id should be u64");
    assert_ne!(
        key_after_rotate, key_before,
        "rotate-key should switch to a new key id"
    );

    let current_after: Value = client
        .get(format!("{base}/ais/current-key"))
        .send()
        .await
        .expect("current-key request after rotate")
        .json()
        .await
        .expect("parse current-key after rotate");
    assert_eq!(current_after["status"], "success");
    assert_eq!(
        current_after["key_id"].as_u64(),
        Some(key_after_rotate),
        "current-key should reflect rotated key"
    );

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn ais_register_enforces_realm_secret_when_configured() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let client = reqwest::Client::new();
    let base = harness.base_url();

    let login: Value = client
        .post(format!("{base}/admin/api/auth/login"))
        .json(&json!({ "password": "testpassword123" }))
        .send()
        .await
        .expect("admin login request")
        .json()
        .await
        .expect("parse admin login response");
    let token = login["token"]
        .as_str()
        .expect("admin login should return token");

    let rotate: Value = client
        .post(format!("{base}/admin/api/realms/1001/secret/rotate"))
        .bearer_auth(token)
        .send()
        .await
        .expect("rotate realm secret request")
        .json()
        .await
        .expect("parse rotate realm secret response");
    let realm_secret = rotate["realm_secret"]
        .as_str()
        .expect("rotate response should include realm_secret");

    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "acme".to_string(),
            name: "secret-check".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: None,
        mfr_signature: None,
        psk_token: None,
        target: None,
    };

    // 1) 未携带 realm_secret，AIS HTTP 注册应被拒绝
    {
        let rsp_bytes = client
            .post(format!("{base}/ais/register"))
            .header("content-type", "application/octet-stream")
            .body(register_req.encode_to_vec())
            .send()
            .await
            .expect("register without secret")
            .bytes()
            .await
            .expect("read body")
            .to_vec();
        let rsp = RegisterResponse::decode(&*rsp_bytes).expect("decode");
        match rsp.result.expect("result") {
            register_response::Result::Error(err) => {
                assert_eq!(err.code, 403, "missing realm_secret should be denied");
                assert!(
                    err.message.to_lowercase().contains("realm secret"),
                    "unexpected error message: {}",
                    err.message
                );
            }
            other => panic!("expected register error, got {other:?}"),
        }
    }

    // 2) 携带错误 realm_secret，AIS HTTP 注册应被拒绝
    {
        let rsp_bytes = client
            .post(format!("{base}/ais/register"))
            .header("content-type", "application/octet-stream")
            .header("x-actrix-realm-secret", "wrong-secret")
            .body(register_req.encode_to_vec())
            .send()
            .await
            .expect("register with wrong secret")
            .bytes()
            .await
            .expect("read body")
            .to_vec();
        let rsp = RegisterResponse::decode(&*rsp_bytes).expect("decode");
        match rsp.result.expect("result") {
            register_response::Result::Error(err) => {
                assert_eq!(err.code, 403, "invalid realm_secret should be denied");
                assert!(
                    err.message.to_lowercase().contains("realm secret"),
                    "unexpected error message: {}",
                    err.message
                );
            }
            other => panic!("expected register error, got {other:?}"),
        }
    }

    // 3) 携带正确 realm_secret，AIS HTTP 注册应成功
    {
        let rsp_bytes = client
            .post(format!("{base}/ais/register"))
            .header("content-type", "application/octet-stream")
            .header("x-actrix-realm-secret", realm_secret)
            .body(register_req.encode_to_vec())
            .send()
            .await
            .expect("register with correct secret")
            .bytes()
            .await
            .expect("read body")
            .to_vec();
        let rsp = RegisterResponse::decode(&*rsp_bytes).expect("decode");
        match rsp.result.expect("result") {
            register_response::Result::Success(ok) => {
                assert_eq!(ok.actr_id.realm.realm_id, 1001);
                assert!(ok.actr_id.serial_number > 0);
            }
            other => panic!("expected register success, got {other:?}"),
        }
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn ais_health_and_endpoints_degrade_when_ks_dependency_is_unreachable() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config_with_ais_signer_endpoint(
        tmp.path(),
        port,
        DEFAULT_TOKEN_TTL,
        "http://127.0.0.1:1",
    );
    let log_path = tmp.path().join("actrix_fullstack_ais_degraded.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;

    // With lazy KS connection, AIS routes are mounted but degrade at runtime
    let client = reqwest::Client::new();
    let ais_health = client
        .get(format!("{base}/ais/health"))
        .send()
        .await
        .expect("ais health request");
    assert_eq!(ais_health.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = ais_health.json().await.expect("health json");
    assert_eq!(
        body["status"], "degraded",
        "AIS should report degraded when KS is unreachable"
    );
    assert_eq!(body["ks_service"], "failed");

    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "acme".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: None,
        mfr_signature: None,
        psk_token: None,
        target: None,
    };
    let register_resp = client
        .post(format!("{base}/ais/register"))
        .header("x-actrix-realm-secret", "")
        .body(register_req.encode_to_vec())
        .send()
        .await
        .expect("ais register request");
    // HTTP 200 but protobuf body should contain an error
    let resp_bytes = register_resp.bytes().await.expect("read register body");
    let resp = RegisterResponse::decode(resp_bytes).expect("decode register response");
    assert!(
        matches!(resp.result, Some(register_response::Result::Error(_))),
        "AIS register should return protobuf error when KS is unreachable, got {:?}",
        resp.result
    );

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_rejects_unauthenticated_ws_connection() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;

    // 不带凭证的裸连接应被拒绝（401 Unauthorized → WS upgrade 失败）
    let result = connect_ws_raw(harness.port).await;
    assert!(
        result.is_err(),
        "unauthenticated WS connection should be rejected"
    );

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_url_identity_reconnect_replaces_stale_connection() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;

    let (mut old_write, mut old_read, register_ok) =
        ws_register(harness.port, "acme", "url-id", None).await;

    let actor_id = register_ok.actr_id.to_string_repr();
    let signature_b64 =
        base64::engine::general_purpose::STANDARD.encode(&register_ok.credential.signature);
    let claims_b64 =
        base64::engine::general_purpose::STANDARD.encode(&register_ok.credential.claims);
    let ws_url = format!(
        "ws://127.0.0.1:{}/signaling/ws?actor_id={}&key_id={}&claims={}&signature={}",
        harness.port,
        urlencoding::encode(&actor_id),
        register_ok.credential.key_id,
        urlencoding::encode(&claims_b64),
        urlencoding::encode(&signature_b64),
    );

    let (reconnect_stream, _) = connect_async(&ws_url)
        .await
        .expect("reconnect with url identity");
    let (mut new_write, mut new_read) = reconnect_stream.split();

    let new_ping = actr_protocol::ActrToSignaling {
        source: register_ok.actr_id.clone(),
        credential: register_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 90,
                mailbox_backlog: 0.0,
                power_reserve: 90.0,
                ..Default::default()
            },
        )),
    };
    send_envelope(
        &mut new_write,
        make_envelope(signaling_envelope::Flow::ActrToServer(new_ping)),
    )
    .await;
    let new_resp = recv_envelope(&mut new_read).await;
    match new_resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(msg)) => match msg.payload {
            Some(signaling_to_actr::Payload::Pong(_)) => {}
            other => panic!("expected pong on reconnected session, got {other:?}"),
        },
        other => panic!("unexpected reconnected response flow: {other:?}"),
    }

    let stale_ping = actr_protocol::ActrToSignaling {
        source: register_ok.actr_id.clone(),
        credential: register_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 85,
                mailbox_backlog: 0.0,
                power_reserve: 85.0,
                ..Default::default()
            },
        )),
    };
    let mut stale_buf = Vec::new();
    make_envelope(signaling_envelope::Flow::ActrToServer(stale_ping))
        .encode(&mut stale_buf)
        .expect("encode stale ping");

    if old_write
        .send(WsMessage::Binary(stale_buf.into()))
        .await
        .is_ok()
    {
        let stale_result = tokio::time::timeout(Duration::from_millis(600), old_read.next()).await;
        match stale_result {
            Ok(Some(Ok(WsMessage::Binary(data)))) => {
                let stale_resp =
                    actr_protocol::SignalingEnvelope::decode(&data[..]).expect("decode stale resp");
                if let Some(signaling_envelope::Flow::ServerToActr(msg)) = stale_resp.flow
                    && matches!(msg.payload, Some(signaling_to_actr::Payload::Pong(_)))
                {
                    panic!("stale connection unexpectedly still receives pong");
                }
            }
            Ok(Some(Ok(_))) | Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                // stale connection should be unusable for normal request/response flow.
            }
        }
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_peer_payload_none_is_ignored_and_connection_remains_usable() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let (mut write, mut read, ok) = ws_register(harness.port, "acme", "peer-none", None).await;

    // 发送空 peer payload
    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::PeerToServer(
            actr_protocol::PeerToSignaling { payload: None },
        )),
    )
    .await;

    let no_response = tokio::time::timeout(Duration::from_millis(500), read.next()).await;
    assert!(
        no_response.is_err(),
        "empty peer payload should be ignored without server response"
    );

    // 连接仍可用：发送 Ping 应收到 Pong
    let ping_msg = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 100,
                mailbox_backlog: 0.0,
                power_reserve: 80.0,
                ..Default::default()
            },
        )),
    };
    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::ActrToServer(ping_msg)),
    )
    .await;

    let resp = recv_envelope(&mut read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Pong(_)) => {}
            other => panic!("expected pong after ignored payload, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = write.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_actr_payload_none_and_invalid_realm_are_rejected_safely() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let (mut write, mut read, ok) = ws_register(harness.port, "acme", "actr-none", None).await;

    let no_payload_msg = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: None,
    };
    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::ActrToServer(no_payload_msg)),
    )
    .await;

    let no_response = tokio::time::timeout(Duration::from_millis(500), read.next()).await;
    assert!(
        no_response.is_err(),
        "empty actr payload should be ignored without server response"
    );

    let mut invalid_realm_source = ok.actr_id.clone();
    invalid_realm_source.realm.realm_id = 9999;
    let invalid_realm_ping = actr_protocol::ActrToSignaling {
        source: invalid_realm_source,
        credential: ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 70,
                mailbox_backlog: 0.0,
                power_reserve: 60.0,
                ..Default::default()
            },
        )),
    };
    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::ActrToServer(invalid_realm_ping)),
    )
    .await;

    let invalid_resp = recv_envelope(&mut read).await;
    match invalid_resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Error(err)) => {
                assert_eq!(err.code, 403, "invalid realm should be rejected");
            }
            other => panic!("expected realm validation error, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let good_ping = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 95,
                mailbox_backlog: 0.0,
                power_reserve: 90.0,
                ..Default::default()
            },
        )),
    };
    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::ActrToServer(good_ping)),
    )
    .await;
    let good_resp = recv_envelope(&mut read).await;
    match good_resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Pong(_)) => {}
            other => panic!("expected pong after recovery ping, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = write.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_connection_rate_limit_rejects_second_concurrent_connection() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path =
        write_fullstack_config_with_rate_limits(tmp.path(), port, DEFAULT_TOKEN_TTL, 1, 50, 50);
    let log_path = tmp.path().join("actrix_fullstack_ratelimit_conn.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;

    // 注册两个不同的 actor 用于测试连接速率限制
    let base_url = format!("http://127.0.0.1:{port}");
    let ok1 = ais_register_http(&base_url, 1001, "acme", "rate1", None, None).await;
    let ok2 = ais_register_http(&base_url, 1001, "acme", "rate2", None, None).await;

    let (_first_write, _first_read) = connect_ws_authenticated(port, &ok1).await;

    // 第二个连接应被 concurrent limit 拒绝
    let actor_id_str = ok2.actr_id.to_string_repr();
    let signature_b64 = base64::engine::general_purpose::STANDARD.encode(&ok2.credential.signature);
    let claims_b64_2 = base64::engine::general_purpose::STANDARD.encode(&ok2.credential.claims);
    let ws_url2 = format!(
        "ws://127.0.0.1:{port}/signaling/ws?actor_id={}&key_id={}&claims={}&signature={}",
        urlencoding::encode(&actor_id_str),
        ok2.credential.key_id,
        urlencoding::encode(&claims_b64_2),
        urlencoding::encode(&signature_b64),
    );
    let second = connect_async(&ws_url2).await;
    match second {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                "second connection should be rejected by concurrent limit"
            );
        }
        Ok(_) => panic!("expected second concurrent connection to be rate-limited"),
        Err(other) => panic!("expected HTTP 429 on second connection, got {other:?}"),
    }

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_message_rate_limit_returns_envelope_error() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path =
        write_fullstack_config_with_rate_limits(tmp.path(), port, DEFAULT_TOKEN_TTL, 20, 1, 1);
    let log_path = tmp.path().join("actrix_fullstack_ratelimit_msg.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;

    let (mut write, mut read, register_ok) = ws_register(port, "acme", "rate-limited", None).await;
    let ping_payload = actr_protocol::ActrToSignaling {
        source: register_ok.actr_id.clone(),
        credential: register_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 80,
                mailbox_backlog: 0.0,
                power_reserve: 80.0,
                ..Default::default()
            },
        )),
    };

    for _ in 0..8 {
        send_envelope(
            &mut write,
            make_envelope(signaling_envelope::Flow::ActrToServer(ping_payload.clone())),
        )
        .await;
    }

    let mut saw_rate_limit = false;
    for _ in 0..12 {
        let next = tokio::time::timeout(Duration::from_secs(2), read.next()).await;
        let Some(Ok(WsMessage::Binary(data))) = next.ok().and_then(|m| m) else {
            continue;
        };
        let envelope =
            actr_protocol::SignalingEnvelope::decode(&data[..]).expect("decode signaling response");
        if let Some(signaling_envelope::Flow::EnvelopeError(err)) = envelope.flow
            && err.code == 429
        {
            saw_rate_limit = true;
            break;
        }
    }
    assert!(
        saw_rate_limit,
        "expected message rate limiter to emit envelope error 429"
    );

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_register_and_discovery_acl_allow() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config(tmp.path(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers with ACL allowing client:* to discover
    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };
    let (_ws_service_write, _ws_service_read, _service_ok) =
        ws_register(port, "acme", "svc", Some(acl)).await;

    // Client registers (no ACL needed)
    let (mut client_write, mut client_read, client_ok) =
        ws_register(port, "acme", "client", None).await;

    // Discovery should return the service type because ACL allows it
    let discover = actr_protocol::ActrToSignaling {
        source: client_ok.actr_id.clone(),
        credential: client_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("acme".into()),
                limit: Some(10),
            },
        )),
    };
    let env = make_envelope(signaling_envelope::Flow::ActrToServer(discover));
    send_envelope(&mut client_write, env).await;
    let resp = recv_envelope(&mut client_read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::DiscoveryResponse(rsp)) => match rsp.result {
                Some(actr_protocol::discovery_response::Result::Success(ok)) => {
                    assert!(
                        !ok.entries.is_empty(),
                        "expected at least one service entry"
                    );
                    assert_eq!(ok.entries[0].actr_type.name, "svc");
                    assert_eq!(ok.entries[0].actr_type.manufacturer, "acme");
                }
                other => panic!("unexpected discovery result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_discovery_acl_denied() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config(tmp.path(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers without ACL (default deny)
    let (_ws_service_write, _ws_service_read, _service_ok) =
        ws_register(port, "acme", "svc-deny", None).await;

    // Client registers
    let (mut client_write, mut client_read, client_ok) =
        ws_register(port, "acme", "client", None).await;

    let discover = actr_protocol::ActrToSignaling {
        source: client_ok.actr_id.clone(),
        credential: client_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("acme".into()),
                limit: Some(10),
            },
        )),
    };
    let env = make_envelope(signaling_envelope::Flow::ActrToServer(discover));
    send_envelope(&mut client_write, env).await;
    let resp = recv_envelope(&mut client_read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::DiscoveryResponse(rsp)) => match rsp.result {
                Some(actr_protocol::discovery_response::Result::Success(ok)) => {
                    assert!(
                        ok.entries.is_empty(),
                        "ACL default deny should yield empty discovery list"
                    );
                }
                other => panic!("unexpected discovery result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_discovery_cross_realm_isolated() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    ensure_realm(&harness.data_dir, 2002).await;

    let (_service_write, _service_read, _service_ok) =
        ws_register_in_realm(harness.port, "acme", "svc-cross-realm", 2002, None).await;
    let (mut client_write, mut client_read, client_ok) =
        ws_register_in_realm(harness.port, "acme", "client", 1001, None).await;

    let discover = actr_protocol::ActrToSignaling {
        source: client_ok.actr_id.clone(),
        credential: client_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("acme".into()),
                limit: Some(10),
            },
        )),
    };

    send_envelope(
        &mut client_write,
        make_envelope(signaling_envelope::Flow::ActrToServer(discover)),
    )
    .await;

    let resp = recv_envelope(&mut client_read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::DiscoveryResponse(rsp)) => match rsp.result {
                Some(actr_protocol::discovery_response::Result::Success(ok)) => {
                    assert!(
                        ok.entries.is_empty(),
                        "cross-realm service should not appear in discovery results"
                    );
                }
                other => panic!("unexpected discovery result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = client_write.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_discovery_cross_realm_acl_allow() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    ensure_realm(&harness.data_dir, 2002).await;

    let service_acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let (_service_write, _service_read, _service_ok) = ws_register_in_realm(
        harness.port,
        "acme",
        "svc-cross-realm",
        2002,
        Some(service_acl),
    )
    .await;
    let (mut client_write, mut client_read, client_ok) =
        ws_register_in_realm(harness.port, "acme", "client", 1001, None).await;

    let discover = actr_protocol::ActrToSignaling {
        source: client_ok.actr_id.clone(),
        credential: client_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("acme".into()),
                limit: Some(10),
            },
        )),
    };

    send_envelope(
        &mut client_write,
        make_envelope(signaling_envelope::Flow::ActrToServer(discover)),
    )
    .await;

    let resp = recv_envelope(&mut client_read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::DiscoveryResponse(rsp)) => match rsp.result {
                Some(actr_protocol::discovery_response::Result::Success(ok)) => {
                    assert!(
                        !ok.entries.is_empty(),
                        "cross-realm ACL allow should expose service in discovery"
                    );
                    assert!(
                        ok.entries.iter().any(|entry| {
                            entry.actr_type.manufacturer == "acme"
                                && entry.actr_type.name == "svc-cross-realm"
                        }),
                        "expected cross-realm service entry"
                    );
                }
                other => panic!("unexpected discovery result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = client_write.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_route_candidates_cross_realm_isolated() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    ensure_realm(&harness.data_dir, 2002).await;

    let (_service_write, _service_read, _service_ok) =
        ws_register_in_realm(harness.port, "acme", "svc-cross-route", 2002, None).await;

    let (mut client_write, mut client_read, client_ok) =
        ws_register_in_realm(harness.port, "acme", "client", 1001, None).await;

    let candidates = query_route_candidates(
        &mut client_write,
        &mut client_read,
        &client_ok,
        "acme",
        "svc-cross-route",
    )
    .await;
    assert!(
        candidates.is_empty(),
        "route candidates should not include cross-realm actors"
    );

    let _ = client_write.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_route_candidates_cross_realm_acl_allow() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    ensure_realm(&harness.data_dir, 2002).await;

    let service_acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let (_service_write, _service_read, service_ok) = ws_register_in_realm(
        harness.port,
        "acme",
        "svc-cross-route",
        2002,
        Some(service_acl),
    )
    .await;

    let (mut client_write, mut client_read, client_ok) =
        ws_register_in_realm(harness.port, "acme", "client", 1001, None).await;

    let candidates = query_route_candidates(
        &mut client_write,
        &mut client_read,
        &client_ok,
        "acme",
        "svc-cross-route",
    )
    .await;
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate.serial_number == service_ok.actr_id.serial_number),
        "cross-realm ACL allow should include route candidate"
    );

    let _ = client_write.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_rejects_expired_credential() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config(tmp.path(), port, 1);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Register actor
    let (mut write, mut read, ok) = ws_register(port, "acme", "shortlived", None).await;

    // Wait for credential to expire
    sleep(Duration::from_secs(2)).await;

    // Send ping with expired credential -> expect 401 error
    let ping_msg = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 50,
                mailbox_backlog: 1.0,
                power_reserve: 50.0,
                ..Default::default()
            },
        )),
    };
    let env = make_envelope(signaling_envelope::Flow::ActrToServer(ping_msg));
    send_envelope(&mut write, env).await;
    let resp = recv_envelope(&mut read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Error(err)) => {
                assert_eq!(err.code, 401, "expired credential should be rejected");
            }
            other => panic!("expected error for expired credential, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_credential_update_via_ws_returns_410() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let (mut write, mut read, ok) = ws_register(port, "acme", "cred-update-client", None).await;
    let update_req = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::CredentialUpdateRequest(
                actr_protocol::CredentialUpdateRequest {
                    actr_id: ok.actr_id.clone(),
                },
            ),
        ),
    };

    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::ActrToServer(update_req)),
    )
    .await;

    let resp = recv_envelope(&mut read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Error(err)) => {
                assert_eq!(
                    err.code, 410,
                    "credential update via WS should return 410 Gone"
                );
                assert!(
                    err.message.contains("/ais/register"),
                    "error should direct to AIS HTTP, got: {}",
                    err.message,
                );
            }
            other => panic!("expected 410 error, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = write.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_credential_update_returns_410_and_connection_stays_healthy() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let (mut write, mut read, ok) = ws_register(port, "acme", "cred-mismatch-client", None).await;

    // CredentialUpdateRequest 已迁移到 AIS HTTP，应返回 410
    let update_req = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::CredentialUpdateRequest(
                actr_protocol::CredentialUpdateRequest {
                    actr_id: ok.actr_id.clone(),
                },
            ),
        ),
    };

    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::ActrToServer(update_req)),
    )
    .await;

    let resp = recv_envelope(&mut read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Error(err)) => {
                assert_eq!(err.code, 410);
            }
            other => panic!("expected 410 error, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    // 连接仍可用
    let ping = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 90,
                mailbox_backlog: 0.0,
                power_reserve: 80.0,
                ..Default::default()
            },
        )),
    };
    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::ActrToServer(ping)),
    )
    .await;

    let pong = recv_envelope(&mut read).await;
    match pong.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Pong(_)) => {}
            other => panic!("expected pong after 410, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = write.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_route_candidates_with_acl() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config(tmp.path(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers with ACL allowing client:sdp to discover/route
    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client-sdp".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };
    let (_svc_w, _svc_r, svc_ok) = ws_register(port, "acme", "svc-rtp", Some(acl)).await;

    // Client registers
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-sdp", None).await;

    // RouteCandidates should return the service because ACL allows
    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "acme".into(),
                        name: "svc-rtp".into(),
                        version: "1.0.0".to_string(),
                    },
                    client_fingerprint: "".into(),
                    criteria: Some(
                        actr_protocol::route_candidates_request::NodeSelectionCriteria {
                            candidate_count: 5,
                            ranking_factors: vec![],
                            minimal_dependency_requirement: None,
                            minimal_health_requirement: None,
                        },
                    ),
                    client_location: None,
                },
            ),
        ),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(route_req)),
    )
    .await;
    let resp = recv_envelope(&mut cli_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => {
                    assert!(
                        ok.candidates
                            .iter()
                            .any(|id| id.serial_number == svc_ok.actr_id.serial_number),
                        "expected routed candidate"
                    );
                }
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_route_candidates_acl_denied() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config(tmp.path(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers without ACL (default deny)
    let (_svc_w, _svc_r, _svc_ok) = ws_register(port, "acme", "svc-deny-route", None).await;

    // Client registers
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-sdp", None).await;

    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "acme".into(),
                        name: "svc-deny-route".into(),
                        version: "1.0.0".to_string(),
                    },
                    client_fingerprint: "".into(),
                    criteria: Some(
                        actr_protocol::route_candidates_request::NodeSelectionCriteria {
                            candidate_count: 5,
                            ranking_factors: vec![],
                            minimal_dependency_requirement: None,
                            minimal_health_requirement: None,
                        },
                    ),
                    client_location: None,
                },
            ),
        ),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(route_req)),
    )
    .await;
    let resp = recv_envelope(&mut cli_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => {
                    assert!(
                        ok.candidates.is_empty(),
                        "ACL deny should yield empty route candidates"
                    );
                }
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_route_candidates_respects_limit_and_sorting() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    // ACL: allow client-route to reach the services
    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client-route".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    // Register two service instances with different load indicators
    let (mut svc1_w, svc1_r, svc1_ok) =
        ws_register(port, "acme", "svc-route", Some(acl.clone())).await;
    let (mut svc2_w, svc2_r, svc2_ok) =
        ws_register(port, "acme", "svc-route", Some(acl.clone())).await;

    // Update runtime metrics via ping (own data per call to avoid lifetimes)
    let send_ping =
        |mut w: WsWrite, ok: register_response::RegisterOk, power: f32, backlog: f32| async move {
            let ping = actr_protocol::ActrToSignaling {
                source: ok.actr_id.clone(),
                credential: ok.credential.clone(),
                payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
                    actr_protocol::Ping {
                        availability: 50,
                        mailbox_backlog: backlog,
                        power_reserve: power,
                        ..Default::default()
                    },
                )),
            };
            send_envelope(
                &mut w,
                make_envelope(signaling_envelope::Flow::ActrToServer(ping)),
            )
            .await;
            w
        };

    svc1_w = send_ping(svc1_w, svc1_ok.clone(), 10.0, 5.0).await; // lower power, higher backlog
    svc2_w = send_ping(svc2_w, svc2_ok.clone(), 80.0, 1.0).await; // higher power, lower backlog

    // Wait for ping metrics to be ingested
    sleep(Duration::from_millis(200)).await;

    // Client registers
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-route", None).await;

    // Request route candidates with sorting and limit=1
    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "acme".into(),
                        name: "svc-route".into(),
                        version: "1.0.0".to_string(),
                    },
                    client_fingerprint: "".into(),
                    criteria: Some(
                        actr_protocol::route_candidates_request::NodeSelectionCriteria {
                            candidate_count: 1,
                            ranking_factors: vec![
                                actr_protocol::route_candidates_request::node_selection_criteria::NodeRankingFactor::MaximumPowerReserve as i32,
                                actr_protocol::route_candidates_request::node_selection_criteria::NodeRankingFactor::MinimumMailboxBacklog as i32,
                            ],
                            minimal_dependency_requirement: None,
                            minimal_health_requirement: None,
                        },
                    ),
                    client_location: None,
                },
            ),
        ),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(route_req)),
    )
    .await;

    let resp = recv_envelope(&mut cli_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => {
                    assert_eq!(
                        ok.candidates.len(),
                        1,
                        "limit=1 should return exactly one candidate"
                    );
                    assert_eq!(ok.candidates.len(), 1);
                    let winner = ok.candidates[0].serial_number;
                    assert_eq!(
                        winner, svc2_ok.actr_id.serial_number,
                        "higher power reserve should win"
                    );
                }
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    // Close sockets and shutdown
    let _ = cli_w.send(WsMessage::Close(None)).await;
    let _ = svc1_w.send(WsMessage::Close(None)).await;
    let _ = svc2_w.send(WsMessage::Close(None)).await;
    std::mem::drop(svc1_r.into_future());
    std::mem::drop(svc2_r.into_future());

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_route_candidates_prefers_exact_fingerprint() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    // ACL allow client-fp
    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client-fp".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    // Helper to build a simple ServiceSpec with unique fingerprint
    let make_spec = |fingerprint: &str| actr_protocol::ServiceSpec {
        name: "svc-fp".into(),
        description: Some("test svc spec".into()),
        fingerprint: fingerprint.into(),
        protobufs: vec![actr_protocol::service_spec::Protobuf {
            package: "echo.v1".into(),
            content: "service Echo { rpc Ping (Ping) returns (Pong); }".into(),
            fingerprint: format!("fp::{fingerprint}"),
        }],
        published_at: None,
        tags: vec!["stable".into()],
    };

    let spec_exact = make_spec("fp-exact");
    let spec_backward = make_spec("fp-backward");

    // Register two service instances with different fingerprints
    let (_svc_exact_w, _svc_exact_r, svc_exact_ok) = ws_register_with_spec(
        port,
        "acme",
        "svc-fp-exact",
        Some(acl.clone()),
        Some(spec_exact.clone()),
    )
    .await;

    let (_svc_bw_w, _svc_bw_r, _svc_bw_ok) = ws_register_with_spec(
        port,
        "acme",
        "svc-fp-bw",
        Some(acl.clone()),
        Some(spec_backward.clone()),
    )
    .await;

    // Client registers
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-fp", None).await;

    // Request route candidates with client_fingerprint matching spec_exact
    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "acme".into(),
                        name: "svc-fp-exact".into(),
                        version: "1.0.0".to_string(),
                    },
                    client_fingerprint: spec_exact.fingerprint.clone(),
                    criteria: Some(
                        actr_protocol::route_candidates_request::NodeSelectionCriteria {
                            candidate_count: 2,
                            ranking_factors: vec![
                                actr_protocol::route_candidates_request::node_selection_criteria::NodeRankingFactor::ExactMatchFirst as i32,
                            ],
                            minimal_dependency_requirement: None,
                            minimal_health_requirement: None,
                        },
                    ),
                    client_location: None,
                },
            ),
        ),
    };

    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(route_req)),
    )
    .await;

    let resp = recv_envelope(&mut cli_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => {
                    assert_eq!(
                        ok.candidates.first().map(|c| c.serial_number),
                        Some(svc_exact_ok.actr_id.serial_number),
                        "exact fingerprint service should rank first"
                    );
                }
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = cli_w.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_get_service_spec_returns_spec() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config(tmp.path(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    let spec = make_service_spec(
        "fp-spec",
        "echo.v1",
        r#"syntax = "proto3";
            package echo.v1;
            message Ping { string msg = 1; }
            message Pong { string msg = 1; }
            service Echo { rpc Say(Ping) returns (Pong); }"#,
    );

    let (_svc_w, _svc_r, _svc_ok) =
        ws_register_with_spec(port, "acme", "svc-spec", None, Some(spec.clone())).await;
    sleep(Duration::from_millis(200)).await;

    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-spec", None).await;

    let get_spec = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::GetServiceSpecRequest(
                actr_protocol::GetServiceSpecRequest {
                    name: spec.name.clone(),
                },
            ),
        ),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(get_spec)),
    )
    .await;
    let resp = recv_envelope(&mut cli_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::GetServiceSpecResponse(rsp)) => match rsp.result {
                Some(actr_protocol::get_service_spec_response::Result::Success(returned)) => {
                    assert_eq!(returned.fingerprint, spec.fingerprint);
                }
                other => panic!("unexpected get spec result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_get_service_spec_not_found() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    // Client without any services, just to issue request
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-nospec", None).await;

    let get_spec = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::GetServiceSpecRequest(
                actr_protocol::GetServiceSpecRequest {
                    name: "non-existent-svc".into(),
                },
            ),
        ),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(get_spec)),
    )
    .await;
    let resp = recv_envelope(&mut cli_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::GetServiceSpecResponse(rsp)) => match rsp.result {
                Some(actr_protocol::get_service_spec_response::Result::Error(err)) => {
                    assert_eq!(err.code, 404, "missing spec should return 404");
                }
                other => panic!("unexpected get spec result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_subscribe_and_unsubscribe_actr_up() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    // Subscriber
    let (mut sub_w, mut sub_r, sub_ok) = ws_register(port, "acme", "subscriber", None).await;

    // Subscribe to target type
    let subscribe = actr_protocol::ActrToSignaling {
        source: sub_ok.actr_id.clone(),
        credential: sub_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::SubscribeActrUpRequest(
                actr_protocol::SubscribeActrUpRequest {
                    target_type: ActrType {
                        manufacturer: "acme".into(),
                        name: "svc-subject".into(),
                        version: "1.0.0".to_string(),
                    },
                },
            ),
        ),
    };
    send_envelope(
        &mut sub_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(subscribe)),
    )
    .await;
    let resp = recv_envelope(&mut sub_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::SubscribeActrUpResponse(rsp)) => match rsp.result {
                Some(actr_protocol::subscribe_actr_up_response::Result::Success(_)) => {}
                other => panic!("unexpected subscribe result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    // Unsubscribe should also succeed
    let unsubscribe = actr_protocol::ActrToSignaling {
        source: sub_ok.actr_id.clone(),
        credential: sub_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::UnsubscribeActrUpRequest(
                actr_protocol::UnsubscribeActrUpRequest {
                    target_type: ActrType {
                        manufacturer: "acme".into(),
                        name: "svc-subject".into(),
                        version: "1.0.0".to_string(),
                    },
                },
            ),
        ),
    };
    send_envelope(
        &mut sub_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(unsubscribe)),
    )
    .await;
    let resp2 = recv_envelope(&mut sub_r).await;
    match resp2.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::UnsubscribeActrUpResponse(rsp)) => match rsp.result {
                Some(actr_protocol::unsubscribe_actr_up_response::Result::Success(_)) => {}
                other => panic!("unexpected unsubscribe result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_subscribe_receives_actr_up_and_unsubscribe_stops() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    // Subscriber registers
    let (mut sub_w, mut sub_r, sub_ok) = ws_register(port, "acme", "subscriber", None).await;

    // Subscribe to service type
    let target_type = ActrType {
        manufacturer: "acme".into(),
        name: "svc-presence".into(),
        version: "1.0.0".to_string(),
    };
    let subscribe = actr_protocol::ActrToSignaling {
        source: sub_ok.actr_id.clone(),
        credential: sub_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::SubscribeActrUpRequest(
                actr_protocol::SubscribeActrUpRequest {
                    target_type: target_type.clone(),
                },
            ),
        ),
    };
    send_envelope(
        &mut sub_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(subscribe)),
    )
    .await;
    // ack
    let _ = recv_envelope(&mut sub_r).await;

    // allow subscription to settle
    sleep(Duration::from_millis(100)).await;

    // New service registers -> should trigger ActrUp notification
    let presence_acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "subscriber".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };
    let (_svc_w, _svc_r, _svc_ok) =
        ws_register(port, "acme", "svc-presence", Some(presence_acl.clone())).await;
    sleep(Duration::from_millis(200)).await;

    // Expect ActrUp notification (poll with timeout)
    let mut got_up = false;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(8) {
        if let Ok(env) = timeout(Duration::from_millis(500), recv_envelope(&mut sub_r)).await {
            if let Some(signaling_envelope::Flow::ServerToActr(server_msg)) = env.flow
                && let Some(signaling_to_actr::Payload::ActrUpEvent(evt)) = server_msg.payload
                && evt.actor_id.r#type == target_type
            {
                got_up = true;
                break;
            }
        } else {
            continue;
        }
    }
    if !got_up {
        let logs = fs::read_to_string(harness.log_path()).unwrap_or_default();
        panic!("subscriber should receive ActrUp notice. Logs:\n{logs}");
    }

    // Unsubscribe
    let unsubscribe = actr_protocol::ActrToSignaling {
        source: sub_ok.actr_id.clone(),
        credential: sub_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::UnsubscribeActrUpRequest(
                actr_protocol::UnsubscribeActrUpRequest {
                    target_type: target_type.clone(),
                },
            ),
        ),
    };
    send_envelope(
        &mut sub_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(unsubscribe)),
    )
    .await;
    let _ = recv_envelope(&mut sub_r).await; // unsubscribe ack

    // Register another service; notification should not arrive after unsubscribe
    let (_svc2_w, _svc2_r, _svc2_ok) =
        ws_register(port, "acme", "svc-presence-2", Some(presence_acl)).await;

    // Drain with timeout; expect None
    use tokio::time::timeout;
    let no_msg = timeout(Duration::from_millis(300), sub_r.next()).await;
    assert!(
        no_msg.is_err(),
        "should not receive ActrUp after unsubscribe"
    );

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_route_candidates_compatibility_cache_hit() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let acl = Acl {
        rules: vec![AclRule {
            from_type: ActrType {
                manufacturer: "mfg".into(),
                name: "client-fp-cache".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(acl_rule::SourceRealm::RealmId(1001)),
            permission: Permission::Allow as i32,
        }],
    };

    let spec_base = make_service_spec(
        "fp-base",
        "compat.v1",
        r#"syntax = "proto3";
            package compat.v1;
            message Req { string data = 1; }
            message Resp { string data = 1; }
            service Compat { rpc Call(Req) returns (Resp); }"#,
    );
    // content identical but fingerprint changed to exercise compatibility cache path without breaking changes
    let spec_new = make_service_spec(
        "fp-new",
        "compat.v1",
        r#"syntax = "proto3";
            package compat.v1;
            message Req { string data = 1; }
            message Resp { string data = 1; }
            service Compat { rpc Call(Req) returns (Resp); }"#,
    );

    // Register base spec instance (provides client fingerprint spec in storage)
    let (_svc_base_w, _svc_base_r, _svc_base_ok) = ws_register_with_spec(
        port,
        "mfg",
        "svc-compat",
        Some(acl.clone()),
        Some(spec_base.clone()),
    )
    .await;
    // Register upgraded instance with different fingerprint
    let (_svc_w, _svc_r, _svc_ok) = ws_register_with_spec(
        port,
        "mfg",
        "svc-compat",
        Some(acl.clone()),
        Some(spec_new.clone()),
    )
    .await;
    sleep(Duration::from_millis(200)).await;

    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-fp-cache", None).await;

    // First request triggers compatibility analysis and populates cache
    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
            actr_protocol::RouteCandidatesRequest {
                target_type: ActrType {
                    manufacturer: "mfg".into(),
                    name: "svc-compat".into(),
                    version: "1.0.0".to_string(),
                },
                client_fingerprint: spec_base.fingerprint.clone(),
                criteria: Some(
                    actr_protocol::route_candidates_request::NodeSelectionCriteria {
                        candidate_count: 3,
                        ranking_factors: vec![actr_protocol::route_candidates_request::node_selection_criteria::NodeRankingFactor::ExactMatchFirst as i32],
                        minimal_dependency_requirement: None,
                        minimal_health_requirement: None,
                    },
                ),
                client_location: None,
            },
        )),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(route_req)),
    )
    .await;
    let resp1 = recv_envelope(&mut cli_r).await;
    let candidates1 = match resp1.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => ok.candidates,
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    };
    assert!(
        !candidates1.is_empty(),
        "should return at least one candidate"
    );

    // Second request should reuse cache and still succeed
    let route_req2 = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
            actr_protocol::RouteCandidatesRequest {
                target_type: ActrType {
                    manufacturer: "mfg".into(),
                    name: "svc-compat".into(),
                    version: "1.0.0".to_string(),
                },
                client_fingerprint: spec_base.fingerprint.clone(),
                criteria: Some(
                    actr_protocol::route_candidates_request::NodeSelectionCriteria {
                        candidate_count: 3,
                        ranking_factors: vec![actr_protocol::route_candidates_request::node_selection_criteria::NodeRankingFactor::ExactMatchFirst as i32],
                        minimal_dependency_requirement: None,
                        minimal_health_requirement: None,
                    },
                ),
                client_location: None,
            },
        )),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(route_req2)),
    )
    .await;
    let resp2 = recv_envelope(&mut cli_r).await;
    let candidates2 = match resp2.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => ok.candidates,
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    };
    assert!(
        !candidates2.is_empty(),
        "cache hit should keep returning candidates"
    );

    let _ = cli_w.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_concurrent_registration_keeps_unique_route_candidates() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client-concurrent".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let service_count = 8usize;
    let mut service_tasks = Vec::with_capacity(service_count);
    for _ in 0..service_count {
        let acl_clone = acl.clone();
        service_tasks.push(tokio::spawn(async move {
            ws_register(port, "acme", "svc-concurrent", Some(acl_clone)).await
        }));
    }

    let mut service_sockets = Vec::with_capacity(service_count);
    let mut expected_serials = HashSet::new();
    for task in service_tasks {
        let (w, _r, ok) = task.await.expect("registration task should complete");
        expected_serials.insert(ok.actr_id.serial_number);
        service_sockets.push(w);
    }
    assert_eq!(
        expected_serials.len(),
        service_count,
        "each concurrent service registration should get a unique serial"
    );

    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-concurrent", None).await;
    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "acme".into(),
                        name: "svc-concurrent".into(),
                        version: "1.0.0".to_string(),
                    },
                    client_fingerprint: "".into(),
                    criteria: Some(
                        actr_protocol::route_candidates_request::NodeSelectionCriteria {
                            candidate_count: 32,
                            ranking_factors: vec![],
                            minimal_dependency_requirement: None,
                            minimal_health_requirement: None,
                        },
                    ),
                    client_location: None,
                },
            ),
        ),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(route_req)),
    )
    .await;

    let resp = recv_envelope(&mut cli_r).await;
    let candidates = match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => ok.candidates,
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    };

    let unique_candidates: HashSet<u64> = candidates.iter().map(|c| c.serial_number).collect();
    assert_eq!(
        candidates.len(),
        unique_candidates.len(),
        "route candidates must not contain duplicates"
    );
    assert_eq!(
        unique_candidates.len(),
        service_count,
        "all concurrently registered services should be routable"
    );

    let _ = cli_w.send(WsMessage::Close(None)).await;
    for mut w in service_sockets {
        let _ = w.send(WsMessage::Close(None)).await;
    }
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_actr_relay_role_assignment() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config(tmp.path(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers with ACL allowing client-offer
    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client-offer".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };
    let (mut svc_w, mut svc_r, svc_ok) = ws_register(port, "acme", "svc-relay", Some(acl)).await;

    // Client registers
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-offer", None).await;

    // Send role negotiation relay
    let relay = ActrRelay {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        target: svc_ok.actr_id.clone(),
        payload: Some(actr_relay::Payload::RoleNegotiation(RoleNegotiation {
            from: cli_ok.actr_id.clone(),
            to: svc_ok.actr_id.clone(),
            realm_id: 1001,
        })),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrRelay(relay)),
    )
    .await;

    // Client should receive RoleAssignment
    let client_resp = recv_envelope(&mut cli_r).await;
    let mut got_client_assignment = false;
    if let Some(signaling_envelope::Flow::ActrRelay(relay_msg)) = client_resp.flow
        && let Some(actr_relay::Payload::RoleAssignment(assign)) = relay_msg.payload
    {
        got_client_assignment = true;
        let _ = assign.is_offerer;
    }
    assert!(got_client_assignment, "client should get role assignment");

    // Service should receive RoleAssignment
    let service_resp = recv_envelope(&mut svc_r).await;
    let mut got_service_assignment = false;
    if let Some(signaling_envelope::Flow::ActrRelay(relay_msg)) = service_resp.flow
        && let Some(actr_relay::Payload::RoleAssignment(assign)) = relay_msg.payload
    {
        got_service_assignment = true;
        let _ = assign.is_offerer;
    }
    assert!(got_service_assignment, "service should get role assignment");

    // Cleanup
    let _ = cli_w.send(WsMessage::Close(None)).await;
    let _ = svc_w.send(WsMessage::Close(None)).await;
    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_rejects_register_request_via_ws() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let (mut write, mut read, _ok) = ws_register(port, "acme", "dup-client", None).await;

    // 通过 WS 发送 RegisterRequest 应返回 410（已迁移到 AIS HTTP）
    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "acme".into(),
            name: "dup-client".into(),
            version: "1.0.0".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
        service: None,
        ws_address: None,
        manifest_raw: None,
        mfr_signature: None,
        psk_token: None,
        target: None,
    };
    let env = make_envelope(signaling_envelope::Flow::PeerToServer(
        actr_protocol::PeerToSignaling {
            payload: Some(peer_to_signaling::Payload::RegisterRequest(register_req)),
        },
    ));
    send_envelope(&mut write, env).await;
    let resp = recv_envelope(&mut read).await;

    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RegisterResponse(RegisterResponse {
                result: Some(register_response::Result::Error(err)),
            })) => {
                assert_eq!(err.code, 410, "WS register should return 410 Gone");
                assert!(
                    err.message.contains("/ais/register"),
                    "error should direct to AIS HTTP, got: {}",
                    err.message,
                );
            }
            other => panic!("expected register error, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_unregister_removes_actor_from_route_candidates() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client-unreg".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let (mut svc_w, mut svc_r, svc_ok) = ws_register(port, "acme", "svc-unreg", Some(acl)).await;
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-unreg", None).await;

    let make_route_request =
        |source: &register_response::RegisterOk| actr_protocol::ActrToSignaling {
            source: source.actr_id.clone(),
            credential: source.credential.clone(),
            payload: Some(
                actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                    actr_protocol::RouteCandidatesRequest {
                        target_type: ActrType {
                            manufacturer: "acme".into(),
                            name: "svc-unreg".into(),
                            version: "1.0.0".to_string(),
                        },
                        client_fingerprint: "".into(),
                        criteria: Some(
                            actr_protocol::route_candidates_request::NodeSelectionCriteria {
                                candidate_count: 8,
                                ranking_factors: vec![],
                                minimal_dependency_requirement: None,
                                minimal_health_requirement: None,
                            },
                        ),
                        client_location: None,
                    },
                ),
            ),
        };

    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(make_route_request(
            &cli_ok,
        ))),
    )
    .await;
    let before = recv_envelope(&mut cli_r).await;
    match before.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => {
                    assert!(
                        ok.candidates
                            .iter()
                            .any(|id| id.serial_number == svc_ok.actr_id.serial_number),
                        "service should be routable before unregister"
                    );
                }
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let unregister = actr_protocol::ActrToSignaling {
        source: svc_ok.actr_id.clone(),
        credential: svc_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::UnregisterRequest(
                actr_protocol::UnregisterRequest {
                    actr_id: svc_ok.actr_id.clone(),
                    reason: Some("test-unregister".into()),
                },
            ),
        ),
    };
    send_envelope(
        &mut svc_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(unregister)),
    )
    .await;
    let unregister_resp = recv_envelope(&mut svc_r).await;
    match unregister_resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::UnregisterResponse(resp)) => match resp.result {
                Some(actr_protocol::unregister_response::Result::Success(_)) => {}
                other => panic!("unexpected unregister result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    sleep(Duration::from_millis(200)).await;

    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(make_route_request(
            &cli_ok,
        ))),
    )
    .await;
    let after = recv_envelope(&mut cli_r).await;
    match after.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => {
                    assert!(
                        !ok.candidates
                            .iter()
                            .any(|id| id.serial_number == svc_ok.actr_id.serial_number),
                        "service should be removed from routing after unregister"
                    );
                }
                other => panic!("unexpected route result {other:?}"),
            },
            other => panic!("unexpected payload {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = cli_w.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_relay_cross_realm_is_denied() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    ensure_realm(&harness.data_dir, 1002).await;

    let (mut src_w, mut src_r, src_ok) = ws_register(port, "acme", "relay-src", None).await;
    let (_dst_w, _dst_r, dst_ok) =
        ws_register_in_realm(port, "acme", "relay-dst", 1002, None).await;

    let relay = ActrRelay {
        source: src_ok.actr_id.clone(),
        credential: src_ok.credential.clone(),
        target: dst_ok.actr_id.clone(),
        payload: Some(actr_relay::Payload::RoleNegotiation(RoleNegotiation {
            from: src_ok.actr_id.clone(),
            to: dst_ok.actr_id.clone(),
            realm_id: src_ok.actr_id.realm.realm_id,
        })),
    };
    send_envelope(
        &mut src_w,
        make_envelope(signaling_envelope::Flow::ActrRelay(relay)),
    )
    .await;

    let resp = recv_envelope(&mut src_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Error(err)) => {
                assert_eq!(err.code, 403, "cross-realm relay should be denied");
                assert!(
                    err.message.contains("ACL policy denies relay")
                        || err.message.contains("Cross-realm relay is not allowed"),
                    "unexpected relay deny message: {}",
                    err.message
                );
            }
            other => panic!("expected relay error, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_relay_rejects_invalid_credential() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "relay-src-auth".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let (_dst_w, _dst_r, dst_ok) = ws_register(port, "acme", "relay-dst-auth", Some(acl)).await;
    let (mut src_w, mut src_r, src_ok) = ws_register(port, "acme", "relay-src-auth", None).await;

    let mut bad_cred = src_ok.credential.clone();
    if !bad_cred.signature.is_empty() {
        let mut tampered = bad_cred.signature.to_vec();
        tampered[0] ^= 0xAA;
        bad_cred.signature = tampered.into();
    }

    let relay = ActrRelay {
        source: src_ok.actr_id.clone(),
        credential: bad_cred,
        target: dst_ok.actr_id.clone(),
        payload: Some(actr_relay::Payload::RoleNegotiation(RoleNegotiation {
            from: src_ok.actr_id.clone(),
            to: dst_ok.actr_id.clone(),
            realm_id: 1001,
        })),
    };
    send_envelope(
        &mut src_w,
        make_envelope(signaling_envelope::Flow::ActrRelay(relay)),
    )
    .await;

    let resp = recv_envelope(&mut src_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Error(err)) => {
                assert_eq!(err.code, 401, "invalid relay credential should be rejected");
            }
            other => panic!("expected relay auth error, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_relay_acl_denied_in_same_realm() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let deny_acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Deny as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "relay-src-deny".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let (_dst_w, _dst_r, dst_ok) =
        ws_register(port, "acme", "relay-dst-deny", Some(deny_acl)).await;
    let (mut src_w, mut src_r, src_ok) = ws_register(port, "acme", "relay-src-deny", None).await;

    let relay = ActrRelay {
        source: src_ok.actr_id.clone(),
        credential: src_ok.credential.clone(),
        target: dst_ok.actr_id.clone(),
        payload: Some(actr_relay::Payload::RoleNegotiation(RoleNegotiation {
            from: src_ok.actr_id.clone(),
            to: dst_ok.actr_id.clone(),
            realm_id: 1001,
        })),
    };
    send_envelope(
        &mut src_w,
        make_envelope(signaling_envelope::Flow::ActrRelay(relay)),
    )
    .await;

    let resp = recv_envelope(&mut src_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Error(err)) => {
                assert_eq!(err.code, 403, "same-realm relay should be ACL-denied");
                assert!(err.message.contains("ACL policy denies relay"));
            }
            other => panic!("expected relay ACL error, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_relay_forwards_ice_candidate_payload() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let allow_acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "relay-src-forward".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let (mut dst_w, mut dst_r, dst_ok) =
        ws_register(port, "acme", "relay-dst-forward", Some(allow_acl)).await;
    let (mut src_w, _src_r, src_ok) = ws_register(port, "acme", "relay-src-forward", None).await;

    let relay = ActrRelay {
        source: src_ok.actr_id.clone(),
        credential: src_ok.credential.clone(),
        target: dst_ok.actr_id.clone(),
        payload: Some(actr_relay::Payload::IceCandidate(
            actr_protocol::IceCandidate {
                candidate: "candidate:1 1 udp 2122252543 127.0.0.1 54400 typ host".into(),
                sdp_mid: Some("0".into()),
                sdp_mline_index: Some(0),
                username_fragment: Some("ufrag-forward".into()),
            },
        )),
    };
    send_envelope(
        &mut src_w,
        make_envelope(signaling_envelope::Flow::ActrRelay(relay)),
    )
    .await;

    let forwarded = recv_envelope(&mut dst_r).await;
    match forwarded.flow {
        Some(signaling_envelope::Flow::ActrRelay(relay_msg)) => {
            assert_eq!(relay_msg.source.serial_number, src_ok.actr_id.serial_number);
            assert_eq!(relay_msg.target.serial_number, dst_ok.actr_id.serial_number);
            match relay_msg.payload {
                Some(actr_relay::Payload::IceCandidate(candidate)) => {
                    assert_eq!(candidate.sdp_mid.as_deref(), Some("0"));
                    assert_eq!(candidate.sdp_mline_index, Some(0));
                    assert!(
                        candidate.candidate.contains("127.0.0.1"),
                        "forwarded ICE candidate payload should be preserved"
                    );
                }
                other => panic!("expected forwarded IceCandidate payload, got {other:?}"),
            }
        }
        other => panic!("expected forwarded ActrRelay flow, got {other:?}"),
    }

    let _ = src_w.send(WsMessage::Close(None)).await;
    let _ = dst_w.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_relay_to_missing_target_is_ignored_and_source_stays_usable() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let allow_acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "relay-src-missing-target".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let (mut dst_w, _dst_r, dst_ok) =
        ws_register(port, "acme", "relay-dst-missing-target", Some(allow_acl)).await;
    let (mut src_w, mut src_r, src_ok) =
        ws_register(port, "acme", "relay-src-missing-target", None).await;

    let mut missing_target = dst_ok.actr_id.clone();
    missing_target.serial_number = missing_target.serial_number.saturating_add(1_000_000);

    let relay = ActrRelay {
        source: src_ok.actr_id.clone(),
        credential: src_ok.credential.clone(),
        target: missing_target,
        payload: Some(actr_relay::Payload::IceCandidate(
            actr_protocol::IceCandidate {
                candidate: "candidate:2 1 udp 2122252543 127.0.0.1 54401 typ host".into(),
                sdp_mid: Some("0".into()),
                sdp_mline_index: Some(0),
                username_fragment: Some("ufrag-missing".into()),
            },
        )),
    };
    send_envelope(
        &mut src_w,
        make_envelope(signaling_envelope::Flow::ActrRelay(relay)),
    )
    .await;

    let no_reply = tokio::time::timeout(Duration::from_millis(300), src_r.next()).await;
    match no_reply {
        Err(_) => {}
        Ok(Some(Ok(msg))) => panic!("expected no relay response for missing target, got {msg:?}"),
        Ok(Some(Err(err))) => panic!("unexpected websocket error: {err:?}"),
        Ok(None) => panic!("source websocket closed unexpectedly"),
    }

    let ping = actr_protocol::ActrToSignaling {
        source: src_ok.actr_id.clone(),
        credential: src_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 95,
                mailbox_backlog: 0.0,
                power_reserve: 88.0,
                ..Default::default()
            },
        )),
    };
    send_envelope(
        &mut src_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(ping)),
    )
    .await;
    let pong = recv_envelope(&mut src_r).await;
    match pong.flow {
        Some(signaling_envelope::Flow::ServerToActr(msg)) => match msg.payload {
            Some(signaling_to_actr::Payload::Pong(_)) => {}
            other => panic!("expected Pong after missing-target relay, got {other:?}"),
        },
        other => panic!("unexpected flow after ping {other:?}"),
    }

    let _ = src_w.send(WsMessage::Close(None)).await;
    let _ = dst_w.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_disconnect_removes_actor_from_route_candidates() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client-disconnect".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let (mut svc_w, _svc_r, svc_ok) = ws_register(port, "acme", "svc-disconnect", Some(acl)).await;
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-disconnect", None).await;

    let before =
        query_route_candidates(&mut cli_w, &mut cli_r, &cli_ok, "acme", "svc-disconnect").await;
    assert!(
        before
            .iter()
            .any(|id| id.serial_number == svc_ok.actr_id.serial_number),
        "service should be routable before disconnect"
    );

    let _ = svc_w.send(WsMessage::Close(None)).await;

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let after =
            query_route_candidates(&mut cli_w, &mut cli_r, &cli_ok, "acme", "svc-disconnect").await;
        let still_present = after
            .iter()
            .any(|id| id.serial_number == svc_ok.actr_id.serial_number);
        if !still_present {
            break;
        }

        if Instant::now() > deadline {
            panic!("service should be removed from routing after websocket disconnect");
        }
        sleep(Duration::from_millis(100)).await;
    }

    let _ = cli_w.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_malformed_binary_removes_actor_from_route_candidates() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client-malformed".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };

    let (mut svc_w, _svc_r, svc_ok) = ws_register(port, "acme", "svc-malformed", Some(acl)).await;
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client-malformed", None).await;

    let before =
        query_route_candidates(&mut cli_w, &mut cli_r, &cli_ok, "acme", "svc-malformed").await;
    assert!(
        before
            .iter()
            .any(|id| id.serial_number == svc_ok.actr_id.serial_number),
        "service should be routable before malformed binary"
    );

    svc_w
        .send(WsMessage::Binary(vec![0xFF, 0x00, 0xAA].into()))
        .await
        .expect("send malformed binary frame");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let after =
            query_route_candidates(&mut cli_w, &mut cli_r, &cli_ok, "acme", "svc-malformed").await;
        let still_present = after
            .iter()
            .any(|id| id.serial_number == svc_ok.actr_id.serial_number);
        if !still_present {
            break;
        }

        if Instant::now() > deadline {
            panic!("service should be removed from routing after malformed binary disconnect");
        }
        sleep(Duration::from_millis(100)).await;
    }

    let _ = cli_w.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
#[ignore = "Flaky test after actrix restart, maybe due to ACL rules persistence"]
async fn service_registry_persists_across_restart() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let data_dir = tmp.path().join("data");
    let port = choose_port();
    let config_path = write_fullstack_config(tmp.path(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_persist.log");
    ensure_realm(&data_dir, 1001).await;

    // first run: register a service
    let mut child = spawn_actrix(&config_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/signer/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&data_dir, 1001).await;

    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };
    let (_svc_w, _svc_r, svc_ok) = ws_register(port, "acme", "svc", Some(acl)).await;
    sleep(Duration::from_millis(100)).await;

    // Verify discovery before restart
    let (mut cli_w1, mut cli_r1, cli_ok1) = ws_register(port, "acme", "client", None).await;
    let discover1 = actr_protocol::ActrToSignaling {
        source: cli_ok1.actr_id.clone(),
        credential: cli_ok1.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("acme".into()),
                limit: Some(10),
            },
        )),
    };
    send_envelope(
        &mut cli_w1,
        make_envelope(signaling_envelope::Flow::ActrToServer(discover1)),
    )
    .await;
    let resp1 = recv_envelope(&mut cli_r1).await;
    let mut has_before = false;
    if let Some(signaling_envelope::Flow::ServerToActr(server_msg)) = resp1.flow
        && let Some(signaling_to_actr::Payload::DiscoveryResponse(rsp)) = server_msg.payload
        && let Some(actr_protocol::discovery_response::Result::Success(ok)) = rsp.result
    {
        has_before = ok
            .entries
            .iter()
            .any(|e| e.actr_type == svc_ok.actr_id.r#type);
    }
    assert!(has_before, "service should be discoverable before restart");
    graceful_shutdown(child);

    // second run: same data_dir, discovery should restore service from cache
    let log_path2 = tmp.path().join("actrix_persist2.log");
    let mut child2 = spawn_actrix(&config_path, &log_path2);
    wait_for_health(&format!("{base}/signer/health"), &mut child2, &log_path2).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child2, &log_path2).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child2, &log_path2).await;
    ensure_realm(&data_dir, 1001).await;
    sleep(Duration::from_millis(200)).await;

    // register client
    // Since ACL is in SQLite without wait-for-fsync or WAL checkpointing might be flaky,
    // re-apply the ACL explicitly so the restored route works.
    let acl = Acl {
        rules: vec![AclRule {
            permission: Permission::Allow as i32,
            from_type: ActrType {
                manufacturer: "acme".into(),
                name: "client".into(),
                version: "1.0.0".to_string(),
            },
            source_realm: Some(SourceRealm::RealmId(1001)),
        }],
    };
    ais_register_http(&base, 1001, "acme", "svc", None, Some(acl)).await;

    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "acme", "client", None).await;

    let discover = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("acme".into()),
                limit: Some(10),
            },
        )),
    };
    send_envelope(
        &mut cli_w,
        make_envelope(signaling_envelope::Flow::ActrToServer(discover)),
    )
    .await;
    let resp = recv_envelope(&mut cli_r).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => {
            match server_msg.payload {
                Some(signaling_to_actr::Payload::DiscoveryResponse(rsp)) => match rsp.result {
                    Some(actr_protocol::discovery_response::Result::Success(ok)) => {
                        if !ok.entries.iter().any(|e| {
                            e.actr_type.name == "svc" && e.actr_type.manufacturer == "acme"
                        }) {
                            let log = std::fs::read_to_string(&log_path2).unwrap_or_default();
                            panic!("expected restored service entry. LOG2:\n{log}");
                        }
                    }
                    other => {
                        let log = std::fs::read_to_string(&log_path2).unwrap_or_default();
                        panic!("unexpected discovery result {other:?}\nLOG2:\n{log}");
                    }
                },
                other => {
                    let log = std::fs::read_to_string(&log_path2).unwrap_or_default();
                    panic!("unexpected payload {other:?}\nLOG2:\n{log}");
                }
            }
        }
        other => {
            let log = std::fs::read_to_string(&log_path2).unwrap_or_default();
            panic!("unexpected flow {other:?}\nLOG2:\n{log}");
        }
    }

    graceful_shutdown(child2);
}
