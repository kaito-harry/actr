use actrix::service::SupervisordGrpcService;
use actrix_common::{
    ServiceCollector,
    config::SupervisorConfig,
    config::supervisor::{SupervisorClientConfig, SupervisordConfig},
    realm::{Realm as RealmEntity, RealmConfig},
    storage::db::set_db_path,
};
use nonce_auth::CredentialBuilder;
use serial_test::serial;
use std::{
    net::SocketAddr,
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use supervit::{
    ConfigType, CreateRealmRequest, DeleteRealmRequest, GetConfigRequest, GetNodeInfoRequest,
    GetRealmRequest, ListRealmsRequest, REALM_ENABLED_KEY, REALM_USE_SERVERS_KEY,
    REALM_VERSION_KEY, ResourceType, ShutdownRequest, SupervisedServiceClient, UpdateConfigRequest,
    UpdateRealmRequest,
};
use tokio::sync::{OnceCell, broadcast};
use tokio::task::JoinHandle;
use tonic::Code;

const START_TIMEOUT: Duration = Duration::from_secs(10);
const STOP_TIMEOUT: Duration = Duration::from_secs(3);
const TEST_SHARED_SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const TEST_NODE_ID: &str = "supervisord-grpc-test-node";
const TEST_LOCATION_TAG: &str = "local,test,supervisord-grpc";

static DB_INIT: OnceCell<()> = OnceCell::const_new();

struct RunningServer {
    client: SupervisedServiceClient<tonic::transport::Channel>,
    shared_secret: Vec<u8>,
    shutdown_tx: broadcast::Sender<()>,
    handle: JoinHandle<()>,
    _temp: tempfile::TempDir,
}

fn choose_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read bound local addr")
        .port()
}

fn unique_realm_id() -> u32 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_micros();
    10_000 + (micros % 900_000) as u32
}

async fn init_global_test_db() {
    DB_INIT
        .get_or_init(|| async {
            let db_dir = std::env::temp_dir().join("actrix_supervisord_grpc_test_db");
            std::fs::create_dir_all(&db_dir).expect("create test db directory");

            for name in ["actrix.db", "actrix.db-shm", "actrix.db-wal"] {
                let path = db_dir.join(name);
                if path.exists() {
                    let _ = std::fs::remove_file(&path);
                }
            }

            match set_db_path(Path::new(
                db_dir
                    .to_str()
                    .expect("convert test db directory to string"),
            ))
            .await
            {
                Ok(()) => {}
                Err(e) => panic!("failed to initialize global sqlite db: {e}"),
            }
        })
        .await;
}

fn build_supervisor_config(port: u16) -> SupervisorConfig {
    SupervisorConfig {
        connect_timeout_secs: 5,
        status_report_interval_secs: 5,
        health_check_interval_secs: 5,
        enable_tls: false,
        tls_domain: None,
        client_cert: None,
        client_key: None,
        ca_cert: None,
        max_clock_skew_secs: 300,
        supervisord: SupervisordConfig {
            node_name: "supervisord-grpc-test".into(),
            ip: "127.0.0.1".into(),
            port,
            advertised_ip: "127.0.0.1".into(),
        },
        client: SupervisorClientConfig {
            node_id: TEST_NODE_ID.into(),
            endpoint: "http://127.0.0.1:1".into(),
            shared_secret: TEST_SHARED_SECRET.into(),
        },
    }
}

fn build_credential_for_payload(shared_secret: &[u8], payload: &str) -> supervit::NonceCredential {
    let credential = CredentialBuilder::new(shared_secret)
        .sign(payload.as_bytes())
        .expect("build credential");
    supervit::nonce_auth::to_proto_credential(credential)
}

fn build_credential_for_payload_with_timestamp(
    shared_secret: &[u8],
    payload: &str,
    timestamp: u64,
) -> supervit::NonceCredential {
    let credential = CredentialBuilder::new(shared_secret)
        .with_time_provider(move || Ok(timestamp))
        .sign(payload.as_bytes())
        .expect("build credential with explicit timestamp");
    supervit::nonce_auth::to_proto_credential(credential)
}

fn build_node_info_credential(shared_secret: &[u8], node_id: &str) -> supervit::NonceCredential {
    build_credential_for_payload(shared_secret, &format!("node_info:{node_id}"))
}

async fn wait_for_supervisord_client(
    endpoint: &str,
) -> SupervisedServiceClient<tonic::transport::Channel> {
    let start = Instant::now();
    loop {
        if let Ok(client) = SupervisedServiceClient::connect(endpoint.to_string()).await {
            return client;
        }

        if start.elapsed() > START_TIMEOUT {
            panic!("supervisord grpc not ready at {endpoint}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn start_supervisord_service() -> RunningServer {
    init_global_test_db().await;

    let temp = tempfile::tempdir().expect("create temp dir");
    let port = choose_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}")
        .parse()
        .expect("parse socket addr");

    let service_collector = ServiceCollector::new();
    let (shutdown_tx, _) = broadcast::channel(8);
    let mut service = SupervisordGrpcService::new(
        build_supervisor_config(port),
        temp.path().to_path_buf(),
        TEST_LOCATION_TAG.to_string(),
        service_collector,
    );

    let handle = service
        .start(addr, shutdown_tx.clone())
        .await
        .expect("start supervisord grpc service");

    let endpoint = format!("http://127.0.0.1:{port}");
    let client = wait_for_supervisord_client(&endpoint).await;

    RunningServer {
        client,
        shared_secret: hex::decode(TEST_SHARED_SECRET).expect("decode shared secret"),
        shutdown_tx,
        handle,
        _temp: temp,
    }
}

async fn stop_supervisord_service(server: RunningServer) {
    let _ = server.shutdown_tx.send(());
    tokio::time::timeout(STOP_TIMEOUT, server.handle)
        .await
        .expect("supervisord grpc handle should stop")
        .expect("supervisord grpc task should not panic");
}

#[tokio::test]
#[serial]
async fn supervisord_grpc_covers_config_realm_nodeinfo_shutdown_and_auth_rejection() {
    let RunningServer {
        mut client,
        shared_secret,
        shutdown_tx,
        handle,
        _temp,
    } = start_supervisord_service().await;

    let config_type = ConfigType::LogLevel as i32;
    let config_key = "log.level".to_string();
    let first_value = "debug".to_string();
    let first_update = client
        .update_config(UpdateConfigRequest {
            config_type,
            config_key: config_key.clone(),
            config_value: first_value.clone(),
            apply_immediately: true,
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("update_config:{TEST_NODE_ID}:{config_type}:{config_key}"),
            ),
        })
        .await
        .expect("first update config should succeed")
        .into_inner();
    assert!(first_update.success);
    assert!(first_update.old_value.is_none());

    let second_value = "info".to_string();
    let second_update = client
        .update_config(UpdateConfigRequest {
            config_type,
            config_key: config_key.clone(),
            config_value: second_value.clone(),
            apply_immediately: true,
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("update_config:{TEST_NODE_ID}:{config_type}:{config_key}"),
            ),
        })
        .await
        .expect("second update config should succeed")
        .into_inner();
    assert!(second_update.success);
    assert_eq!(
        second_update.old_value.as_deref(),
        Some(first_value.as_str())
    );

    let get_config = client
        .get_config(GetConfigRequest {
            config_type,
            config_key: config_key.clone(),
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("get_config:{TEST_NODE_ID}:{config_type}:{config_key}"),
            ),
        })
        .await
        .expect("get config should succeed")
        .into_inner();
    assert!(get_config.success);
    assert_eq!(
        get_config.config_value.as_deref(),
        Some(second_value.as_str())
    );

    let missing_key = "log.missing".to_string();
    let get_missing = client
        .get_config(GetConfigRequest {
            config_type,
            config_key: missing_key.clone(),
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("get_config:{TEST_NODE_ID}:{config_type}:{missing_key}"),
            ),
        })
        .await
        .expect("get missing config should return response")
        .into_inner();
    assert!(!get_missing.success);
    assert!(get_missing.config_value.is_none());

    let realm_id = unique_realm_id();
    let create_realm = client
        .create_realm(CreateRealmRequest {
            realm_id,
            name: "realm-alpha".into(),
            enabled: true,
            use_servers: vec![ResourceType::Signaling as i32, ResourceType::Ks as i32],
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("create_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
            version: 11,
            expires_at: (chrono::Utc::now().timestamp() + 3600) as u64,
        })
        .await
        .expect("create realm should succeed")
        .into_inner();
    assert!(create_realm.success);
    let created = create_realm.realm.expect("realm should be returned");
    assert_eq!(created.realm_id, realm_id);
    assert_eq!(created.name, "realm-alpha");
    assert!(created.enabled);

    let duplicate_create = client
        .create_realm(CreateRealmRequest {
            realm_id,
            name: "realm-alpha-duplicate".into(),
            enabled: true,
            use_servers: vec![],
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("create_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
            version: 12,
            expires_at: (chrono::Utc::now().timestamp() + 3600) as u64,
        })
        .await
        .expect("duplicate create should still return response")
        .into_inner();
    assert!(!duplicate_create.success);

    let get_realm = client
        .get_realm(GetRealmRequest {
            realm_id,
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("get_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
        })
        .await
        .expect("get realm should succeed")
        .into_inner();
    assert!(get_realm.success);
    assert_eq!(
        get_realm.realm.expect("realm should exist").realm_id,
        realm_id
    );

    let update_realm = client
        .update_realm(UpdateRealmRequest {
            realm_id,
            name: Some("realm-beta".into()),
            enabled: Some(false),
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("update_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
        })
        .await
        .expect("update realm should succeed")
        .into_inner();
    assert!(update_realm.success);
    let updated = update_realm
        .realm
        .expect("updated realm should be returned");
    assert_eq!(updated.name, "realm-beta");
    assert!(!updated.enabled);

    let missing_realm_id = realm_id + 1;
    let update_missing = client
        .update_realm(UpdateRealmRequest {
            realm_id: missing_realm_id,
            name: Some("realm-missing".into()),
            enabled: Some(true),
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("update_realm:{TEST_NODE_ID}:{missing_realm_id}"),
            ),
        })
        .await
        .expect("update missing realm should return response")
        .into_inner();
    assert!(!update_missing.success);

    let list_realms = client
        .list_realms(ListRealmsRequest {
            page_size: Some(50),
            page_token: None,
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("list_realms:{TEST_NODE_ID}"),
            ),
        })
        .await
        .expect("list realms should succeed")
        .into_inner();
    assert!(list_realms.success);
    assert!(
        list_realms
            .realms
            .iter()
            .any(|realm| realm.realm_id == realm_id)
    );

    let delete_realm = client
        .delete_realm(DeleteRealmRequest {
            realm_id,
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("delete_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
        })
        .await
        .expect("delete realm should succeed")
        .into_inner();
    assert!(delete_realm.success);

    let get_deleted = client
        .get_realm(GetRealmRequest {
            realm_id,
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("get_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
        })
        .await
        .expect("get deleted realm should return response")
        .into_inner();
    assert!(!get_deleted.success);
    assert!(get_deleted.realm.is_none());

    let delete_deleted = client
        .delete_realm(DeleteRealmRequest {
            realm_id,
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("delete_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
        })
        .await
        .expect("delete deleted realm should return response")
        .into_inner();
    assert!(!delete_deleted.success);

    let valid_node_info_credential = build_node_info_credential(&shared_secret, TEST_NODE_ID);
    let node_info = client
        .get_node_info(GetNodeInfoRequest {
            credential: valid_node_info_credential.clone(),
        })
        .await
        .expect("get node info should succeed")
        .into_inner();
    assert!(node_info.success);
    assert_eq!(node_info.node_id, TEST_NODE_ID);
    assert_eq!(node_info.location_tag, TEST_LOCATION_TAG);

    let mut bad_signature = valid_node_info_credential;
    bad_signature.signature.push('x');
    let bad_signature_err = client
        .get_node_info(GetNodeInfoRequest {
            credential: bad_signature,
        })
        .await
        .expect_err("tampered credential should fail");
    assert_eq!(bad_signature_err.code(), Code::Unauthenticated);

    let shutdown = client
        .shutdown(ShutdownRequest {
            graceful: true,
            timeout_secs: Some(2),
            reason: Some("integration shutdown".into()),
            credential: build_credential_for_payload(
                &shared_secret,
                &format!("shutdown:{TEST_NODE_ID}"),
            ),
        })
        .await
        .expect("shutdown should succeed")
        .into_inner();
    assert!(shutdown.accepted);
    assert!(shutdown.estimated_shutdown_time.is_some());

    let _ = shutdown_tx.send(());
    tokio::time::timeout(STOP_TIMEOUT, handle)
        .await
        .expect("supervisord grpc handle should stop after shutdown")
        .expect("supervisord grpc task should not panic");
}

#[tokio::test]
#[serial]
async fn supervisord_grpc_rejects_duplicate_nonce_and_expired_timestamp() {
    let mut server = start_supervisord_service().await;
    let node_payload = format!("node_info:{TEST_NODE_ID}");

    let replay_credential = build_credential_for_payload(&server.shared_secret, &node_payload);
    let first = server
        .client
        .get_node_info(GetNodeInfoRequest {
            credential: replay_credential.clone(),
        })
        .await
        .expect("first request with nonce should pass")
        .into_inner();
    assert!(first.success);

    let replay_err = server
        .client
        .get_node_info(GetNodeInfoRequest {
            credential: replay_credential,
        })
        .await
        .expect_err("replayed nonce should fail");
    assert_eq!(replay_err.code(), Code::Unauthenticated);
    assert!(
        replay_err.message().contains("nonce already used"),
        "unexpected replay error: {}",
        replay_err.message()
    );

    let stale_ts = (chrono::Utc::now().timestamp() as u64).saturating_sub(301);
    let stale_credential =
        build_credential_for_payload_with_timestamp(&server.shared_secret, &node_payload, stale_ts);
    let stale_err = server
        .client
        .get_node_info(GetNodeInfoRequest {
            credential: stale_credential,
        })
        .await
        .expect_err("stale timestamp should fail");
    assert_eq!(stale_err.code(), Code::Unauthenticated);
    assert!(
        stale_err.message().contains("timestamp out of range"),
        "unexpected stale error: {}",
        stale_err.message()
    );

    stop_supervisord_service(server).await;
}

#[tokio::test]
#[serial]
async fn supervisord_grpc_rejects_bad_signature_for_update_config_and_keeps_previous_value() {
    let mut server = start_supervisord_service().await;

    let config_type = ConfigType::LogLevel as i32;
    let config_key = "log.secure_level".to_string();

    let first_update = server
        .client
        .update_config(UpdateConfigRequest {
            config_type,
            config_key: config_key.clone(),
            config_value: "info".to_string(),
            apply_immediately: true,
            credential: build_credential_for_payload(
                &server.shared_secret,
                &format!("update_config:{TEST_NODE_ID}:{config_type}:{config_key}"),
            ),
        })
        .await
        .expect("first update config should succeed")
        .into_inner();
    assert!(first_update.success);

    let mut bad_credential = build_credential_for_payload(
        &server.shared_secret,
        &format!("update_config:{TEST_NODE_ID}:{config_type}:{config_key}"),
    );
    bad_credential.signature.push('x');
    let bad_update_err = server
        .client
        .update_config(UpdateConfigRequest {
            config_type,
            config_key: config_key.clone(),
            config_value: "debug".to_string(),
            apply_immediately: true,
            credential: bad_credential,
        })
        .await
        .expect_err("update with bad signature should fail");
    assert_eq!(bad_update_err.code(), Code::Unauthenticated);

    let get_after_reject = server
        .client
        .get_config(GetConfigRequest {
            config_type,
            config_key: config_key.clone(),
            credential: build_credential_for_payload(
                &server.shared_secret,
                &format!("get_config:{TEST_NODE_ID}:{config_type}:{config_key}"),
            ),
        })
        .await
        .expect("get config after rejected update should succeed")
        .into_inner();
    assert!(get_after_reject.success);
    assert_eq!(get_after_reject.config_value.as_deref(), Some("info"));

    stop_supervisord_service(server).await;
}

#[tokio::test]
#[serial]
async fn supervisord_grpc_tolerates_corrupted_use_servers_metadata() {
    let mut server = start_supervisord_service().await;
    let realm_id = unique_realm_id();

    let create = server
        .client
        .create_realm(CreateRealmRequest {
            realm_id,
            name: "realm-corrupted-metadata".into(),
            enabled: true,
            use_servers: vec![ResourceType::Signaling as i32, ResourceType::Ks as i32],
            credential: build_credential_for_payload(
                &server.shared_secret,
                &format!("create_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
            version: 42,
            expires_at: (chrono::Utc::now().timestamp() + 3600) as u64,
        })
        .await
        .expect("create realm should succeed")
        .into_inner();
    assert!(create.success);

    let realm = RealmEntity::get_by_realm_id(realm_id)
        .await
        .expect("query realm by id")
        .expect("realm should exist");
    let rowid = realm.rowid.expect("realm rowid should exist");

    let mut use_servers_cfg = RealmConfig::get_by_realm_and_key(rowid, REALM_USE_SERVERS_KEY)
        .await
        .expect("query realm use_servers config")
        .expect("use_servers config should exist");
    use_servers_cfg.set_value("{invalid-json".to_string());
    use_servers_cfg
        .save()
        .await
        .expect("persist corrupted use_servers config");

    let get_realm = server
        .client
        .get_realm(GetRealmRequest {
            realm_id,
            credential: build_credential_for_payload(
                &server.shared_secret,
                &format!("get_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
        })
        .await
        .expect("get realm should still succeed even with corrupted metadata")
        .into_inner();
    assert!(get_realm.success);
    let realm_info = get_realm.realm.expect("realm info should be present");
    assert_eq!(realm_info.realm_id, realm_id);
    assert!(
        realm_info.use_servers.is_empty(),
        "corrupted metadata should degrade to empty use_servers"
    );
    assert_eq!(realm_info.version, 42);

    let delete_realm = server
        .client
        .delete_realm(DeleteRealmRequest {
            realm_id,
            credential: build_credential_for_payload(
                &server.shared_secret,
                &format!("delete_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
        })
        .await
        .expect("delete realm should succeed")
        .into_inner();
    assert!(delete_realm.success);

    stop_supervisord_service(server).await;
}

#[tokio::test]
#[serial]
async fn supervisord_grpc_tolerates_corrupted_enabled_and_version_metadata() {
    let mut server = start_supervisord_service().await;
    let realm_id = unique_realm_id();

    let create = server
        .client
        .create_realm(CreateRealmRequest {
            realm_id,
            name: "realm-corrupted-bool-version".into(),
            enabled: false,
            use_servers: vec![ResourceType::Signaling as i32],
            credential: build_credential_for_payload(
                &server.shared_secret,
                &format!("create_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
            version: 77,
            expires_at: (chrono::Utc::now().timestamp() + 3600) as u64,
        })
        .await
        .expect("create realm should succeed")
        .into_inner();
    assert!(create.success);

    let realm = RealmEntity::get_by_realm_id(realm_id)
        .await
        .expect("query realm by id")
        .expect("realm should exist");
    let rowid = realm.rowid.expect("realm rowid should exist");

    let mut enabled_cfg = RealmConfig::get_by_realm_and_key(rowid, REALM_ENABLED_KEY)
        .await
        .expect("query realm enabled config")
        .expect("enabled config should exist");
    enabled_cfg.set_value("definitely-not-a-bool".to_string());
    enabled_cfg
        .save()
        .await
        .expect("persist corrupted enabled config");

    let mut version_cfg = RealmConfig::get_by_realm_and_key(rowid, REALM_VERSION_KEY)
        .await
        .expect("query realm version config")
        .expect("version config should exist");
    version_cfg.set_value("not-a-number".to_string());
    version_cfg
        .save()
        .await
        .expect("persist corrupted version config");

    let get_realm = server
        .client
        .get_realm(GetRealmRequest {
            realm_id,
            credential: build_credential_for_payload(
                &server.shared_secret,
                &format!("get_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
        })
        .await
        .expect("get realm should still succeed with corrupted metadata")
        .into_inner();
    assert!(get_realm.success);
    let realm_info = get_realm.realm.expect("realm info should be present");
    assert_eq!(realm_info.realm_id, realm_id);
    assert!(
        realm_info.enabled,
        "corrupted enabled flag should fall back to true"
    );
    assert_eq!(
        realm_info.version, 0,
        "corrupted version should fall back to 0"
    );

    let delete_realm = server
        .client
        .delete_realm(DeleteRealmRequest {
            realm_id,
            credential: build_credential_for_payload(
                &server.shared_secret,
                &format!("delete_realm:{TEST_NODE_ID}:{realm_id}"),
            ),
        })
        .await
        .expect("delete realm should succeed")
        .into_inner();
    assert!(delete_realm.success);

    stop_supervisord_service(server).await;
}
