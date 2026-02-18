use actr_protocol::acl_rule::{Permission, Principal};
use actr_protocol::{
    Acl, AclRule, ActrIdExt, ActrRelay, ActrType, Realm, RegisterRequest, RegisterResponse,
    RoleNegotiation, actr_relay, peer_to_signaling, register_response, route_candidates_response,
    signaling_envelope, signaling_to_actr,
};
use actrix_common::aid::credential::validator::AIdCredentialValidator;
use base64::Engine as _;
use futures::{SinkExt, StreamExt};
use prost::Message;
use serde_json::Value;
use std::{
    collections::HashSet,
    fs,
    io::Write,
    path::PathBuf,
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
    tmp: tempfile::TempDir,
    port: u16,
    log_path: PathBuf,
    data_dir: PathBuf,
    child: Child,
}

impl ActrixHarness {
    /// Start actrix with default features (AIS/KS/Signaling) and wait for health
    async fn start(token_ttl: u64) -> Self {
        let tmp = tempfile::tempdir().expect("temp dir");
        let port = choose_port();
        let config_path = write_fullstack_config(&tmp.path().to_path_buf(), port, token_ttl);
        let log_path = tmp.path().join("actrix_fullstack.log");
        let data_dir = tmp.path().join("data");
        ensure_realm(&data_dir, 1001).await;
        let mut child = spawn_actrix(&config_path, &log_path);

        let base = format!("http://127.0.0.1:{port}");
        wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
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

    fn log_path(&self) -> &PathBuf {
        &self.log_path
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

fn write_fullstack_config(dir: &PathBuf, port: u16, token_ttl_secs: u64) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-fullstack-test"
enable = 25  # ENABLE_SIGNALING | ENABLE_AIS | ENABLE_KS
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
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 0

[turn]
advertised_ip = "127.0.0.1"
advertised_port = 3478
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[services.ks]
[services.ks.storage]
backend = "sqlite"
key_ttl_seconds = 3600
[services.ks.storage.sqlite]
path = "ks.db"

[services.ais]
[services.ais.server]
token_ttl_secs = {token_ttl}

[services.signaling]
[services.signaling.server]
ws_path = "/signaling"

[observability.log]
output = "console"
level = "info"

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
    dir: &PathBuf,
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
enable = 25  # ENABLE_SIGNALING | ENABLE_AIS | ENABLE_KS
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
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 0

[turn]
advertised_ip = "127.0.0.1"
advertised_port = 3478
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[services.ks]
[services.ks.storage]
backend = "sqlite"
key_ttl_seconds = 3600
[services.ks.storage.sqlite]
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

[observability.log]
output = "console"
level = "info"

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

fn write_fullstack_config_with_ais_ks_endpoint(
    dir: &PathBuf,
    port: u16,
    token_ttl_secs: u64,
    ais_ks_endpoint: &str,
) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-fullstack-test-ais-dependency"
enable = 25  # ENABLE_SIGNALING | ENABLE_AIS | ENABLE_KS
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
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 0

[turn]
advertised_ip = "127.0.0.1"
advertised_port = 3478
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[services.ks]
[services.ks.storage]
backend = "sqlite"
key_ttl_seconds = 3600
[services.ks.storage.sqlite]
path = "ks.db"

[services.ais]
[services.ais.server]
token_ttl_secs = {token_ttl}
[services.ais.dependencies.ks]
endpoint = "{ais_ks_endpoint}"
timeout_seconds = 1
enable_tls = false

[services.signaling]
[services.signaling.server]
ws_path = "/signaling"

[observability.log]
output = "console"
level = "info"

[process]
pid = "{pid}"
"#,
        sqlite = data_dir.display(),
        shared = ACTRIX_SHARED_KEY,
        port = port,
        token_ttl = token_ttl_secs,
        ais_ks_endpoint = ais_ks_endpoint,
        pid = dir.join("actrix.pid").display()
    )
    .expect("write config");
    config_path
}

fn write_signaling_without_local_ais_config(dir: &PathBuf, port: u16) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-fullstack-signaling-without-ais"
enable = 17  # ENABLE_SIGNALING | ENABLE_KS
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
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 0

[turn]
advertised_ip = "127.0.0.1"
advertised_port = 3478
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[services.ks]
[services.ks.storage]
backend = "sqlite"
key_ttl_seconds = 3600
[services.ks.storage.sqlite]
path = "ks.db"

[services.signaling]
[services.signaling.server]
ws_path = "/signaling"
[services.signaling.dependencies.ais]
endpoint = "http://127.0.0.1:1"
timeout_seconds = 1

[observability.log]
output = "console"
level = "info"

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

fn spawn_actrix(config: &PathBuf, log_path: &PathBuf) -> Child {
    let bin = PathBuf::from(env!("CARGO_BIN_EXE_actrix"));
    let log_file = fs::File::create(log_path).expect("create log file");
    Command::new(bin)
        .arg("--config")
        .arg(config)
        .stdout(Stdio::from(log_file.try_clone().expect("dup log")))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("spawn actrix")
}

async fn ensure_realm(sqlite_dir: &PathBuf, realm_id: u32) {
    let db = actrix_common::storage::db::Database::new(sqlite_dir)
        .await
        .expect("init db");
    db.execute(&format!(
        "INSERT OR IGNORE INTO realm (realm_id, name, status, expires_at)
         VALUES ({realm_id}, 'test-realm', 0, NULL)"
    ))
    .await
    .expect("insert realm");
}

async fn connect_ws(port: u16) -> (WsWrite, WsRead) {
    let ws_url = format!("ws://127.0.0.1:{}/signaling/ws", port);
    let (ws_stream, _) = connect_async(&ws_url).await.expect("ws connect");
    ws_stream.split()
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
    let (mut write, mut read) = connect_ws(port).await;

    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: manufacturer.to_string(),
            name: name.to_string(),
        },
        realm: Realm { realm_id },
        service_spec,
        acl,
    };

    let env = make_envelope(signaling_envelope::Flow::PeerToServer(
        actr_protocol::PeerToSignaling {
            payload: Some(peer_to_signaling::Payload::RegisterRequest(register_req)),
        },
    ));
    send_envelope(&mut write, env).await;
    let resp = recv_envelope(&mut read).await;
    let register_ok = match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RegisterResponse(RegisterResponse {
                result: Some(register_response::Result::Success(ok)),
            })) => ok,
            other => panic!("expected register success, got {other:?}"),
        },
        other => panic!("unexpected flow: {other:?}"),
    };

    (write, read, register_ok)
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

async fn wait_for_health(url: &str, child: &mut Child, log_path: &PathBuf) {
    let client = reqwest::Client::new();
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap_or(None) {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("actrix exited early: status={status:?}\nlogs:\n{log}");
        }

        if let Ok(resp) = client.get(url).send().await {
            if resp.status().is_success() {
                return;
            }
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
    let ks_health = format!("{base}/ks/health");
    let ais_health = format!("{base}/ais/health");
    let signaling_health = format!("{base}/signaling/health");

    let log_path = harness.log_path().clone();
    wait_for_health(&ks_health, &mut harness.child, &log_path).await;
    wait_for_health(&ais_health, &mut harness.child, &log_path).await;
    wait_for_health(&signaling_health, &mut harness.child, &log_path).await;
    ensure_realm(&harness.data_dir, 1001).await;

    let client = reqwest::Client::new();

    // KS health JSON
    let ks_resp = client.get(&ks_health).send().await.expect("ks health");
    assert!(ks_resp.status().is_success());
    let ks_body: Value = ks_resp.json().await.expect("ks health json");
    assert_eq!(ks_body["status"], "healthy");

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
            manufacturer: "test-mfg".to_string(),
            name: "device".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
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

    // Validate credential through AIdCredentialValidator (fetches key via KS gRPC)
    let ks_client_cfg = actrix_common::config::ks::KsClientConfig {
        endpoint: "http://127.0.0.1:50052".to_string(),
        timeout_seconds: 5,
        enable_tls: false,
        tls_domain: None,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    };
    AIdCredentialValidator::init(&ks_client_cfg, ACTRIX_SHARED_KEY, harness.tmp.path())
        .await
        .expect("validator init");
    let (claims, _) = AIdCredentialValidator::check(&ok.credential, 1001)
        .await
        .expect("validate credential");
    assert_eq!(claims.realm_id, 1001);

    // WebSocket signaling ping/pong with valid credential
    let ws_url = format!("ws://127.0.0.1:{}/signaling/ws", harness.port);
    let (ws_stream, _) = connect_async(&ws_url).await.expect("ws connect");
    let (mut write, mut read) = ws_stream.split();

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
    if !bad_cred.encrypted_token.is_empty() {
        let mut tampered = bad_cred.encrypted_token.to_vec();
        tampered[0] ^= 0xFF;
        bad_cred.encrypted_token = tampered.into();
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
async fn ais_register_accepts_non_preprovisioned_realm() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let client = reqwest::Client::new();
    let register_url = format!("{}/ais/register", harness.base_url());

    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "realm-mfg".to_string(),
            name: "realm-device".to_string(),
        },
        realm: Realm { realm_id: 9999 },
        service_spec: None,
        acl: None,
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
        register_response::Result::Success(ok) => {
            assert_eq!(ok.actr_id.realm.realm_id, 9999);
            assert!(
                ok.actr_id.serial_number > 0,
                "serial number should be generated for non-preprovisioned realm"
            );
        }
        other => panic!("expected register success for non-preprovisioned realm, got {other:?}"),
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
async fn ais_health_and_endpoints_degrade_when_ks_dependency_is_unreachable() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config_with_ais_ks_endpoint(
        &tmp.path().to_path_buf(),
        port,
        DEFAULT_TOKEN_TTL,
        "http://127.0.0.1:1",
    );
    let log_path = tmp.path().join("actrix_fullstack_ais_degraded.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;

    let client = reqwest::Client::new();
    let ais_health = client
        .get(format!("{base}/ais/health"))
        .send()
        .await
        .expect("ais health request");
    assert_eq!(
        ais_health.status(),
        reqwest::StatusCode::NOT_FOUND,
        "AIS route should not be mounted when AIS initialization fails"
    );

    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "mfg".to_string(),
            name: "svc".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
    };
    let register_resp = client
        .post(format!("{base}/ais/register"))
        .body(register_req.encode_to_vec())
        .send()
        .await
        .expect("ais register request");
    assert_eq!(
        register_resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "AIS register should be unavailable when AIS service failed to initialize"
    );

    let rotate = client
        .post(format!("{base}/ais/rotate-key"))
        .send()
        .await
        .expect("rotate-key request with unreachable ks");
    assert_eq!(
        rotate.status(),
        reqwest::StatusCode::NOT_FOUND,
        "AIS rotate-key should be unavailable when AIS service failed to initialize"
    );

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_register_returns_error_when_ais_endpoint_is_unreachable() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_signaling_without_local_ais_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix_fullstack_signaling_no_ais.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;

    let ais_health = reqwest::Client::new()
        .get(format!("{base}/ais/health"))
        .send()
        .await
        .expect("ais health request");
    assert_eq!(
        ais_health.status(),
        reqwest::StatusCode::NOT_FOUND,
        "AIS should not be mounted when AIS service is disabled"
    );

    let (mut write, mut read) = connect_ws(port).await;
    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "mfg".to_string(),
            name: "no-ais".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
    };

    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::PeerToServer(
            actr_protocol::PeerToSignaling {
                payload: Some(peer_to_signaling::Payload::RegisterRequest(register_req)),
            },
        )),
    )
    .await;

    let resp = recv_envelope(&mut read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RegisterResponse(RegisterResponse {
                result: Some(register_response::Result::Error(err)),
            })) => {
                assert_eq!(
                    err.code, 500,
                    "registration should fail when AIS is unreachable"
                );
                assert!(
                    err.message.contains("Failed to call AIS"),
                    "error should report upstream AIS call failure, got: {}",
                    err.message
                );
            }
            other => panic!("expected register error, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = write.send(WsMessage::Close(None)).await;
    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_url_identity_reconnect_replaces_stale_connection() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;

    let (mut old_write, mut old_read, register_ok) =
        ws_register(harness.port, "mfg", "url-id", None).await;

    let actor_id = register_ok.actr_id.to_string_repr();
    let token_b64 =
        base64::engine::general_purpose::STANDARD.encode(&register_ok.credential.encrypted_token);
    let ws_url = format!(
        "ws://127.0.0.1:{}/signaling/ws?actor_id={}&token={}&token_key_id={}",
        harness.port,
        urlencoding::encode(&actor_id),
        urlencoding::encode(&token_b64),
        register_ok.credential.token_key_id
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
    let (mut write, mut read) = connect_ws(harness.port).await;

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

    let register_req = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "mfg".to_string(),
            name: "peer-none".to_string(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
    };
    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::PeerToServer(
            actr_protocol::PeerToSignaling {
                payload: Some(peer_to_signaling::Payload::RegisterRequest(register_req)),
            },
        )),
    )
    .await;

    let resp = recv_envelope(&mut read).await;
    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RegisterResponse(RegisterResponse {
                result: Some(register_response::Result::Success(_)),
            })) => {}
            other => panic!("expected register success after ignored payload, got {other:?}"),
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
    let (mut write, mut read, ok) = ws_register(harness.port, "mfg", "actr-none", None).await;

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
    let config_path = write_fullstack_config_with_rate_limits(
        &tmp.path().to_path_buf(),
        port,
        DEFAULT_TOKEN_TTL,
        1,
        50,
        50,
    );
    let log_path = tmp.path().join("actrix_fullstack_ratelimit_conn.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;

    let ws_url = format!("ws://127.0.0.1:{port}/signaling/ws");
    let (_first_stream, _) = connect_async(&ws_url)
        .await
        .expect("first websocket connect");

    let second = connect_async(&ws_url).await;
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
    let config_path = write_fullstack_config_with_rate_limits(
        &tmp.path().to_path_buf(),
        port,
        DEFAULT_TOKEN_TTL,
        20,
        1,
        1,
    );
    let log_path = tmp.path().join("actrix_fullstack_ratelimit_msg.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;

    let (mut write, mut read, register_ok) = ws_register(port, "mfg", "rate-limited", None).await;
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
    let config_path = write_fullstack_config(&tmp.path().to_path_buf(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers with ACL allowing client:* to discover
    let acl = Acl {
        rules: vec![AclRule {
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };
    let (_ws_service_write, _ws_service_read, _service_ok) =
        ws_register(port, "mfg", "svc", Some(acl)).await;

    // Client registers (no ACL needed)
    let (mut client_write, mut client_read, client_ok) =
        ws_register(port, "mfg", "client", None).await;

    // Discovery should return the service type because ACL allows it
    let discover = actr_protocol::ActrToSignaling {
        source: client_ok.actr_id.clone(),
        credential: client_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("mfg".into()),
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
                    assert_eq!(ok.entries[0].actr_type.manufacturer, "mfg");
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
    let config_path = write_fullstack_config(&tmp.path().to_path_buf(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers without ACL (default deny)
    let (_ws_service_write, _ws_service_read, _service_ok) =
        ws_register(port, "mfg", "svc-deny", None).await;

    // Client registers
    let (mut client_write, mut client_read, client_ok) =
        ws_register(port, "mfg", "client", None).await;

    let discover = actr_protocol::ActrToSignaling {
        source: client_ok.actr_id.clone(),
        credential: client_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("mfg".into()),
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

    let service_acl = Acl {
        rules: vec![AclRule {
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    let (_service_write, _service_read, _service_ok) = ws_register_in_realm(
        harness.port,
        "mfg",
        "svc-cross-realm",
        2002,
        Some(service_acl),
    )
    .await;
    let (mut client_write, mut client_read, client_ok) =
        ws_register_in_realm(harness.port, "mfg", "client", 1001, None).await;

    let discover = actr_protocol::ActrToSignaling {
        source: client_ok.actr_id.clone(),
        credential: client_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("mfg".into()),
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
async fn signaling_route_candidates_cross_realm_isolated() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    ensure_realm(&harness.data_dir, 2002).await;

    let service_acl = Acl {
        rules: vec![AclRule {
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    let (_service_write, _service_read, _service_ok) = ws_register_in_realm(
        harness.port,
        "mfg",
        "svc-cross-route",
        2002,
        Some(service_acl),
    )
    .await;

    let (mut client_write, mut client_read, client_ok) =
        ws_register_in_realm(harness.port, "mfg", "client", 1001, None).await;

    let candidates = query_route_candidates(
        &mut client_write,
        &mut client_read,
        &client_ok,
        "mfg",
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
async fn signaling_rejects_expired_credential() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_fullstack_config(&tmp.path().to_path_buf(), port, 1);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Register actor
    let (mut write, mut read, ok) = ws_register(port, "mfg", "shortlived", None).await;

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
async fn signaling_credential_update_succeeds_and_new_credential_is_usable() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let (mut write, mut read, ok) = ws_register(port, "mfg", "cred-update-client", None).await;
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
    let updated_credential = match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RegisterResponse(RegisterResponse {
                result: Some(register_response::Result::Success(updated)),
            })) => {
                assert_eq!(updated.actr_id.serial_number, ok.actr_id.serial_number);
                assert!(
                    !updated.credential.encrypted_token.is_empty(),
                    "updated credential should not be empty"
                );
                updated.credential
            }
            other => panic!("expected credential update success, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    };

    let ping_with_new_credential = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: updated_credential,
        payload: Some(actr_protocol::actr_to_signaling::Payload::Ping(
            actr_protocol::Ping {
                availability: 80,
                mailbox_backlog: 0.0,
                power_reserve: 70.0,
                ..Default::default()
            },
        )),
    };
    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::ActrToServer(
            ping_with_new_credential,
        )),
    )
    .await;

    let pong = recv_envelope(&mut read).await;
    match pong.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::Pong(_)) => {}
            other => panic!("expected pong after credential update, got {other:?}"),
        },
        other => panic!("unexpected flow {other:?}"),
    }

    let _ = write.send(WsMessage::Close(None)).await;
    harness.shutdown();
}

#[tokio::test]
#[serial]
async fn signaling_credential_update_actr_id_mismatch_is_ignored_and_connection_stays_healthy() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let (mut write, mut read, ok) = ws_register(port, "mfg", "cred-mismatch-client", None).await;
    let mut wrong_actr_id = ok.actr_id.clone();
    wrong_actr_id.serial_number = wrong_actr_id.serial_number.saturating_add(1);

    let mismatch_req = actr_protocol::ActrToSignaling {
        source: ok.actr_id.clone(),
        credential: ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::CredentialUpdateRequest(
                actr_protocol::CredentialUpdateRequest {
                    actr_id: wrong_actr_id,
                },
            ),
        ),
    };

    send_envelope(
        &mut write,
        make_envelope(signaling_envelope::Flow::ActrToServer(mismatch_req)),
    )
    .await;

    let no_response = tokio::time::timeout(Duration::from_millis(500), read.next()).await;
    assert!(
        no_response.is_err(),
        "mismatched credential update should be ignored without server response"
    );

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
            other => panic!("expected pong after mismatch request, got {other:?}"),
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
    let config_path = write_fullstack_config(&tmp.path().to_path_buf(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers with ACL allowing client:sdp to discover/route
    let acl = Acl {
        rules: vec![AclRule {
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client-sdp".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };
    let (_svc_w, _svc_r, svc_ok) = ws_register(port, "mfg", "svc-rtp", Some(acl)).await;

    // Client registers
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-sdp", None).await;

    // RouteCandidates should return the service because ACL allows
    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "mfg".into(),
                        name: "svc-rtp".into(),
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
    let config_path = write_fullstack_config(&tmp.path().to_path_buf(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers without ACL (default deny)
    let (_svc_w, _svc_r, _svc_ok) = ws_register(port, "mfg", "svc-deny-route", None).await;

    // Client registers
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-sdp", None).await;

    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "mfg".into(),
                        name: "svc-deny-route".into(),
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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client-route".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    // Register two service instances with different load indicators
    let (mut svc1_w, svc1_r, svc1_ok) =
        ws_register(port, "mfg", "svc-route", Some(acl.clone())).await;
    let (mut svc2_w, svc2_r, svc2_ok) =
        ws_register(port, "mfg", "svc-route", Some(acl.clone())).await;

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
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-route", None).await;

    // Request route candidates with sorting and limit=1
    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "mfg".into(),
                        name: "svc-route".into(),
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
    let _ = svc1_r.into_future();
    let _ = svc2_r.into_future();

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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client-fp".into(),
                }),
            }],
            permission: Permission::Allow as i32,
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
        "mfg",
        "svc-fp-exact",
        Some(acl.clone()),
        Some(spec_exact.clone()),
    )
    .await;

    let (_svc_bw_w, _svc_bw_r, _svc_bw_ok) = ws_register_with_spec(
        port,
        "mfg",
        "svc-fp-bw",
        Some(acl.clone()),
        Some(spec_backward.clone()),
    )
    .await;

    // Client registers
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-fp", None).await;

    // Request route candidates with client_fingerprint matching spec_exact
    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "mfg".into(),
                        name: "svc-fp-exact".into(),
                    },
                    client_fingerprint: spec_exact.fingerprint.clone(),
                    criteria: Some(
                        actr_protocol::route_candidates_request::NodeSelectionCriteria {
                            candidate_count: 2,
                            ranking_factors: vec![
                                actr_protocol::route_candidates_request::node_selection_criteria::NodeRankingFactor::BestCompatibility as i32,
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
                    assert!(
                        ok.has_exact_match.unwrap_or(false),
                        "should report exact match"
                    );
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
    let config_path = write_fullstack_config(&tmp.path().to_path_buf(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
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
        ws_register_with_spec(port, "mfg", "svc-spec", None, Some(spec.clone())).await;
    sleep(Duration::from_millis(200)).await;

    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-spec", None).await;

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
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-nospec", None).await;

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
    let (mut sub_w, mut sub_r, sub_ok) = ws_register(port, "mfg", "subscriber", None).await;

    // Subscribe to target type
    let subscribe = actr_protocol::ActrToSignaling {
        source: sub_ok.actr_id.clone(),
        credential: sub_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::SubscribeActrUpRequest(
                actr_protocol::SubscribeActrUpRequest {
                    target_type: ActrType {
                        manufacturer: "mfg".into(),
                        name: "svc-subject".into(),
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
                        manufacturer: "mfg".into(),
                        name: "svc-subject".into(),
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
    let (mut sub_w, mut sub_r, sub_ok) = ws_register(port, "mfg", "subscriber", None).await;

    // Subscribe to service type
    let target_type = ActrType {
        manufacturer: "mfg".into(),
        name: "svc-presence".into(),
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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "subscriber".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };
    let (_svc_w, _svc_r, _svc_ok) =
        ws_register(port, "mfg", "svc-presence", Some(presence_acl.clone())).await;
    sleep(Duration::from_millis(200)).await;

    // Expect ActrUp notification (poll with timeout)
    let mut got_up = false;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(8) {
        if let Ok(env) = timeout(Duration::from_millis(500), recv_envelope(&mut sub_r)).await {
            if let Some(signaling_envelope::Flow::ServerToActr(server_msg)) = env.flow {
                if let Some(signaling_to_actr::Payload::ActrUpEvent(evt)) = server_msg.payload {
                    if evt.actor_id.r#type == target_type {
                        got_up = true;
                        break;
                    }
                }
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
        ws_register(port, "mfg", "svc-presence-2", Some(presence_acl)).await;

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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client-fp-cache".into(),
                }),
            }],
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
                },
                client_fingerprint: spec_base.fingerprint.clone(),
                criteria: Some(
                    actr_protocol::route_candidates_request::NodeSelectionCriteria {
                        candidate_count: 3,
                        ranking_factors: vec![actr_protocol::route_candidates_request::node_selection_criteria::NodeRankingFactor::BestCompatibility as i32],
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
    let (candidates1, info1) = match resp1.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => {
                    (ok.candidates, ok.compatibility_info)
                }
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
    assert!(
        !info1.is_empty(),
        "compatibility analysis info should be returned"
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
                },
                client_fingerprint: spec_base.fingerprint.clone(),
                criteria: Some(
                    actr_protocol::route_candidates_request::NodeSelectionCriteria {
                        candidate_count: 3,
                        ranking_factors: vec![actr_protocol::route_candidates_request::node_selection_criteria::NodeRankingFactor::BestCompatibility as i32],
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
    let (candidates2, info2) = match resp2.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RouteCandidatesResponse(rsp)) => match rsp.result {
                Some(route_candidates_response::Result::Success(ok)) => {
                    (ok.candidates, ok.compatibility_info)
                }
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
    assert!(
        !info2.is_empty(),
        "compatibility info should still be present on cache hit"
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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client-concurrent".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    let service_count = 8usize;
    let mut service_tasks = Vec::with_capacity(service_count);
    for _ in 0..service_count {
        let acl_clone = acl.clone();
        service_tasks.push(tokio::spawn(async move {
            ws_register(port, "mfg", "svc-concurrent", Some(acl_clone)).await
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

    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-concurrent", None).await;
    let route_req = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(
            actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                actr_protocol::RouteCandidatesRequest {
                    target_type: ActrType {
                        manufacturer: "mfg".into(),
                        name: "svc-concurrent".into(),
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
    let config_path = write_fullstack_config(&tmp.path().to_path_buf(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_fullstack.log");
    ensure_realm(&tmp.path().join("data"), 1001).await;
    let mut child = spawn_actrix(&config_path, &log_path);

    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&tmp.path().join("data"), 1001).await;

    // Service registers with ACL allowing client-offer
    let acl = Acl {
        rules: vec![AclRule {
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client-offer".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };
    let (mut svc_w, mut svc_r, svc_ok) = ws_register(port, "mfg", "svc-relay", Some(acl)).await;

    // Client registers
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-offer", None).await;

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
    if let Some(signaling_envelope::Flow::ActrRelay(relay_msg)) = client_resp.flow {
        if let Some(actr_relay::Payload::RoleAssignment(assign)) = relay_msg.payload {
            got_client_assignment = true;
            assert!(assign.remote_fixed.is_some());
        }
    }
    assert!(got_client_assignment, "client should get role assignment");

    // Service should receive RoleAssignment
    let service_resp = recv_envelope(&mut svc_r).await;
    let mut got_service_assignment = false;
    if let Some(signaling_envelope::Flow::ActrRelay(relay_msg)) = service_resp.flow {
        if let Some(actr_relay::Payload::RoleAssignment(assign)) = relay_msg.payload {
            got_service_assignment = true;
            assert!(assign.remote_fixed.is_some());
        }
    }
    assert!(got_service_assignment, "service should get role assignment");

    // Cleanup
    let _ = cli_w.send(WsMessage::Close(None)).await;
    let _ = svc_w.send(WsMessage::Close(None)).await;
    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn signaling_rejects_duplicate_register_on_same_connection() {
    let harness = ActrixHarness::start(DEFAULT_TOKEN_TTL).await;
    let port = harness.port;

    let (mut write, mut read, _ok) = ws_register(port, "mfg", "dup-client", None).await;

    let duplicate_register = RegisterRequest {
        actr_type: ActrType {
            manufacturer: "mfg".into(),
            name: "dup-client".into(),
        },
        realm: Realm { realm_id: 1001 },
        service_spec: None,
        acl: None,
    };
    let env = make_envelope(signaling_envelope::Flow::PeerToServer(
        actr_protocol::PeerToSignaling {
            payload: Some(peer_to_signaling::Payload::RegisterRequest(
                duplicate_register,
            )),
        },
    ));
    send_envelope(&mut write, env).await;
    let resp = recv_envelope(&mut read).await;

    match resp.flow {
        Some(signaling_envelope::Flow::ServerToActr(server_msg)) => match server_msg.payload {
            Some(signaling_to_actr::Payload::RegisterResponse(RegisterResponse {
                result: Some(register_response::Result::Error(err)),
            })) => {
                assert_eq!(err.code, 409, "duplicate register should return 409");
                assert!(err.message.contains("Already registered"));
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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client-unreg".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    let (mut svc_w, mut svc_r, svc_ok) = ws_register(port, "mfg", "svc-unreg", Some(acl)).await;
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-unreg", None).await;

    let make_route_request =
        |source: &register_response::RegisterOk| actr_protocol::ActrToSignaling {
            source: source.actr_id.clone(),
            credential: source.credential.clone(),
            payload: Some(
                actr_protocol::actr_to_signaling::Payload::RouteCandidatesRequest(
                    actr_protocol::RouteCandidatesRequest {
                        target_type: ActrType {
                            manufacturer: "mfg".into(),
                            name: "svc-unreg".into(),
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

    let (mut src_w, mut src_r, src_ok) = ws_register(port, "mfg", "relay-src", None).await;
    let (_dst_w, _dst_r, dst_ok) = ws_register_in_realm(port, "mfg", "relay-dst", 1002, None).await;

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
                assert!(err.message.contains("Cross-realm relay is not allowed"));
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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "relay-src-auth".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    let (_dst_w, _dst_r, dst_ok) = ws_register(port, "mfg", "relay-dst-auth", Some(acl)).await;
    let (mut src_w, mut src_r, src_ok) = ws_register(port, "mfg", "relay-src-auth", None).await;

    let mut bad_cred = src_ok.credential.clone();
    if !bad_cred.encrypted_token.is_empty() {
        let mut tampered = bad_cred.encrypted_token.to_vec();
        tampered[0] ^= 0xAA;
        bad_cred.encrypted_token = tampered.into();
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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "relay-src-deny".into(),
                }),
            }],
            permission: Permission::Deny as i32,
        }],
    };

    let (_dst_w, _dst_r, dst_ok) = ws_register(port, "mfg", "relay-dst-deny", Some(deny_acl)).await;
    let (mut src_w, mut src_r, src_ok) = ws_register(port, "mfg", "relay-src-deny", None).await;

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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "relay-src-forward".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    let (mut dst_w, mut dst_r, dst_ok) =
        ws_register(port, "mfg", "relay-dst-forward", Some(allow_acl)).await;
    let (mut src_w, _src_r, src_ok) = ws_register(port, "mfg", "relay-src-forward", None).await;

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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "relay-src-missing-target".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    let (mut dst_w, _dst_r, dst_ok) =
        ws_register(port, "mfg", "relay-dst-missing-target", Some(allow_acl)).await;
    let (mut src_w, mut src_r, src_ok) =
        ws_register(port, "mfg", "relay-src-missing-target", None).await;

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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client-disconnect".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    let (mut svc_w, _svc_r, svc_ok) = ws_register(port, "mfg", "svc-disconnect", Some(acl)).await;
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-disconnect", None).await;

    let before =
        query_route_candidates(&mut cli_w, &mut cli_r, &cli_ok, "mfg", "svc-disconnect").await;
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
            query_route_candidates(&mut cli_w, &mut cli_r, &cli_ok, "mfg", "svc-disconnect").await;
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
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "mfg".into(),
                    name: "client-malformed".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };

    let (mut svc_w, _svc_r, svc_ok) = ws_register(port, "mfg", "svc-malformed", Some(acl)).await;
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "mfg", "client-malformed", None).await;

    let before =
        query_route_candidates(&mut cli_w, &mut cli_r, &cli_ok, "mfg", "svc-malformed").await;
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
            query_route_candidates(&mut cli_w, &mut cli_r, &cli_ok, "mfg", "svc-malformed").await;
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
async fn service_registry_persists_across_restart() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let data_dir = tmp.path().join("data");
    let port = choose_port();
    let config_path = write_fullstack_config(&tmp.path().to_path_buf(), port, DEFAULT_TOKEN_TTL);
    let log_path = tmp.path().join("actrix_persist.log");
    ensure_realm(&data_dir, 1001).await;

    // first run: register a service
    let mut child = spawn_actrix(&config_path, &log_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/ks/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child, &log_path).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child, &log_path).await;
    ensure_realm(&data_dir, 1001).await;

    let acl = Acl {
        rules: vec![AclRule {
            principals: vec![Principal {
                realm: Some(Realm { realm_id: 1001 }),
                actr_type: Some(ActrType {
                    manufacturer: "persist".into(),
                    name: "client".into(),
                }),
            }],
            permission: Permission::Allow as i32,
        }],
    };
    let (_svc_w, _svc_r, svc_ok) = ws_register(port, "persist", "svc", Some(acl)).await;
    sleep(Duration::from_millis(100)).await;

    // Verify discovery before restart
    let (mut cli_w1, mut cli_r1, cli_ok1) = ws_register(port, "persist", "client", None).await;
    let discover1 = actr_protocol::ActrToSignaling {
        source: cli_ok1.actr_id.clone(),
        credential: cli_ok1.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("persist".into()),
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
    if let Some(signaling_envelope::Flow::ServerToActr(server_msg)) = resp1.flow {
        if let Some(signaling_to_actr::Payload::DiscoveryResponse(rsp)) = server_msg.payload {
            if let Some(actr_protocol::discovery_response::Result::Success(ok)) = rsp.result {
                has_before = ok
                    .entries
                    .iter()
                    .any(|e| e.actr_type == svc_ok.actr_id.r#type);
            }
        }
    }
    assert!(has_before, "service should be discoverable before restart");
    graceful_shutdown(child);

    // second run: same data_dir, discovery should restore service from cache
    let log_path2 = tmp.path().join("actrix_persist2.log");
    let mut child2 = spawn_actrix(&config_path, &log_path2);
    wait_for_health(&format!("{base}/ks/health"), &mut child2, &log_path2).await;
    wait_for_health(&format!("{base}/ais/health"), &mut child2, &log_path2).await;
    wait_for_health(&format!("{base}/signaling/health"), &mut child2, &log_path2).await;
    ensure_realm(&data_dir, 1001).await;
    sleep(Duration::from_millis(200)).await;

    // register client
    let (mut cli_w, mut cli_r, cli_ok) = ws_register(port, "persist", "client", None).await;

    let discover = actr_protocol::ActrToSignaling {
        source: cli_ok.actr_id.clone(),
        credential: cli_ok.credential.clone(),
        payload: Some(actr_protocol::actr_to_signaling::Payload::DiscoveryRequest(
            actr_protocol::DiscoveryRequest {
                manufacturer: Some("persist".into()),
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
                        assert!(
                            ok.entries.iter().any(|e| e.actr_type.name == "svc"
                                && e.actr_type.manufacturer == "persist"),
                            "expected restored service entry"
                        );
                    }
                    other => panic!("unexpected discovery result {other:?}"),
                },
                other => panic!("unexpected payload {other:?}"),
            }
        }
        other => panic!("unexpected flow {other:?}"),
    }

    graceful_shutdown(child2);
}
