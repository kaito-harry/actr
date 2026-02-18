use actrix_common::{
    ServiceCollector, ServiceInfo, ServiceState, ServiceType, storage::db::set_db_path,
};
use serial_test::serial;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use supervit::{
    ConfigType, CreateRealmRequest, DeleteRealmRequest, GetConfigRequest, GetNodeInfoRequest,
    GetRealmRequest, ListRealmsRequest, NonceCredential, ResourceType, ShutdownRequest,
    SupervisedServiceClient, SupervisedServiceServer, Supervisord, SupervitError, SystemMetrics,
    UpdateConfigRequest, UpdateRealmRequest,
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, OnceCell};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::Server;

const START_TIMEOUT: Duration = Duration::from_secs(8);

static DB_INIT: OnceCell<()> = OnceCell::const_new();

fn test_credential() -> NonceCredential {
    NonceCredential {
        timestamp: 1,
        nonce: "test-nonce".to_string(),
        signature: "test-signature".to_string(),
    }
}

fn unique_realm_id() -> u32 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_micros();
    20_000 + (micros % 900_000) as u32
}

async fn init_global_test_db() {
    DB_INIT
        .get_or_init(|| async {
            let db_dir = std::env::temp_dir().join("actrix_supervit_service_test_db");
            std::fs::create_dir_all(&db_dir).expect("create test db directory");

            for name in ["actrix.db", "actrix.db-shm", "actrix.db-wal"] {
                let path = db_dir.join(name);
                if path.exists() {
                    let _ = std::fs::remove_file(path);
                }
            }

            set_db_path(Path::new(
                db_dir
                    .to_str()
                    .expect("convert test db directory to string"),
            ))
            .await
            .expect("initialize sqlite database");
        })
        .await;
}

async fn connect_client(endpoint: &str) -> SupervisedServiceClient<tonic::transport::Channel> {
    let start = std::time::Instant::now();
    loop {
        if let Ok(client) = SupervisedServiceClient::connect(endpoint.to_string()).await {
            return client;
        }

        if start.elapsed() > START_TIMEOUT {
            panic!("supervised service not ready at {endpoint}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn spawn_supervised_service(service: Supervisord) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr: SocketAddr = listener.local_addr().expect("read local addr");
    let incoming = TcpListenerStream::new(listener);

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(SupervisedServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .expect("run supervised service server");
    });

    (format!("http://{addr}"), handle)
}

async fn build_service_collector() -> ServiceCollector {
    let collector = ServiceCollector::new();

    collector
        .insert(
            "signaling".to_string(),
            ServiceInfo {
                name: "signaling".to_string(),
                service_type: ServiceType::Signaling,
                domain_name: "ws://signaling.example.com".to_string(),
                port_info: "443".to_string(),
                status: ServiceState::Running("ws://signaling.example.com/ws".to_string()),
                description: None,
            },
        )
        .await;

    collector
        .insert(
            "ks".to_string(),
            ServiceInfo {
                name: "ks".to_string(),
                service_type: ServiceType::Ks,
                domain_name: "https://ks.example.com".to_string(),
                port_info: "443".to_string(),
                status: ServiceState::Error("degraded".to_string()),
                description: None,
            },
        )
        .await;

    collector
}

#[tokio::test]
#[serial]
async fn supervised_service_covers_config_realm_node_info_and_shutdown() {
    init_global_test_db().await;

    let shutdown_calls: Arc<Mutex<Vec<(bool, Option<i32>, Option<String>)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let shutdown_calls_for_handler = Arc::clone(&shutdown_calls);

    let service = Supervisord::new(
        "supervit-node",
        "supervit-name",
        "edge-a",
        "1.0.0",
        build_service_collector().await,
    )
    .expect("create supervisord service")
    .with_metrics_provider(|| async {
        Ok(SystemMetrics {
            cpu_usage_percent: 19.5,
            memory_used_bytes: 1_048_576,
            memory_total_bytes: 4_194_304,
            memory_usage_percent: 25.0,
            network_rx_bytes: 120,
            network_tx_bytes: 340,
            disk_used_bytes: 2_000_000,
            disk_total_bytes: 8_000_000,
            load_average_1m: 0.7,
            load_average_5m: Some(0.5),
            load_average_15m: Some(0.3),
        })
    })
    .with_shutdown_handler(move |graceful, timeout_secs, reason| {
        let shutdown_calls_for_handler = Arc::clone(&shutdown_calls_for_handler);
        async move {
            shutdown_calls_for_handler
                .lock()
                .await
                .push((graceful, timeout_secs, reason));
            Ok(())
        }
    });

    let (endpoint, handle) = spawn_supervised_service(service).await;
    let mut client = connect_client(&endpoint).await;

    let first_update = client
        .update_config(UpdateConfigRequest {
            config_type: ConfigType::LogLevel as i32,
            config_key: "log.level".to_string(),
            config_value: "debug".to_string(),
            apply_immediately: true,
            credential: test_credential(),
        })
        .await
        .expect("first update config should succeed")
        .into_inner();
    assert!(first_update.success);
    assert!(first_update.old_value.is_none());

    let second_update = client
        .update_config(UpdateConfigRequest {
            config_type: ConfigType::LogLevel as i32,
            config_key: "log.level".to_string(),
            config_value: "info".to_string(),
            apply_immediately: true,
            credential: test_credential(),
        })
        .await
        .expect("second update config should succeed")
        .into_inner();
    assert!(second_update.success);
    assert_eq!(second_update.old_value.as_deref(), Some("debug"));

    let get_config = client
        .get_config(GetConfigRequest {
            config_type: ConfigType::LogLevel as i32,
            config_key: "log.level".to_string(),
            credential: test_credential(),
        })
        .await
        .expect("get config should succeed")
        .into_inner();
    assert!(get_config.success);
    assert_eq!(get_config.config_value.as_deref(), Some("info"));

    let get_missing_config = client
        .get_config(GetConfigRequest {
            config_type: ConfigType::LogLevel as i32,
            config_key: "log.unknown".to_string(),
            credential: test_credential(),
        })
        .await
        .expect("get missing config should return response")
        .into_inner();
    assert!(!get_missing_config.success);
    assert!(get_missing_config.config_value.is_none());

    let realm_id = unique_realm_id();

    let create_realm = client
        .create_realm(CreateRealmRequest {
            realm_id,
            name: "realm-alpha".to_string(),
            enabled: true,
            use_servers: vec![ResourceType::Signaling as i32, ResourceType::Ks as i32],
            credential: test_credential(),
            version: 4,
            expires_at: (chrono::Utc::now().timestamp() + 1800) as u64,
        })
        .await
        .expect("create realm should succeed")
        .into_inner();
    assert!(create_realm.success);
    let created = create_realm
        .realm
        .expect("created realm should be returned");
    assert_eq!(created.realm_id, realm_id);
    assert_eq!(created.name, "realm-alpha");
    assert!(created.enabled);
    assert_eq!(created.version, 4);

    let get_realm = client
        .get_realm(GetRealmRequest {
            realm_id,
            credential: test_credential(),
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
            name: Some("realm-beta".to_string()),
            enabled: Some(false),
            credential: test_credential(),
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

    let update_missing = client
        .update_realm(UpdateRealmRequest {
            realm_id: realm_id + 9_999,
            name: Some("realm-missing".to_string()),
            enabled: Some(true),
            credential: test_credential(),
        })
        .await
        .expect("updating missing realm should return response")
        .into_inner();
    assert!(!update_missing.success);

    let list_realms = client
        .list_realms(ListRealmsRequest {
            page_size: Some(100),
            page_token: None,
            credential: test_credential(),
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

    let node_info = client
        .get_node_info(GetNodeInfoRequest {
            credential: test_credential(),
        })
        .await
        .expect("get node info should succeed")
        .into_inner();
    assert!(node_info.success);
    assert_eq!(node_info.node_id, "supervit-node");
    assert_eq!(node_info.name, "supervit-name");
    assert_eq!(node_info.location_tag, "edge-a");
    assert_eq!(node_info.version, "1.0.0");
    assert_eq!(node_info.services.len(), 2);
    let metrics = node_info
        .current_metrics
        .expect("metrics should be returned");
    assert_eq!(metrics.network_rx_bytes, 120);
    assert_eq!(metrics.network_tx_bytes, 340);
    assert_eq!(metrics.memory_total_bytes, 4_194_304);

    let shutdown = client
        .shutdown(ShutdownRequest {
            graceful: true,
            timeout_secs: Some(3),
            reason: Some("integration shutdown".to_string()),
            credential: test_credential(),
        })
        .await
        .expect("shutdown should succeed")
        .into_inner();
    assert!(shutdown.accepted);
    assert!(shutdown.error_message.is_none());
    assert!(shutdown.estimated_shutdown_time.is_some());

    let shutdown_calls = shutdown_calls.lock().await;
    assert_eq!(shutdown_calls.len(), 1);
    assert_eq!(shutdown_calls[0].0, true);
    assert_eq!(shutdown_calls[0].1, Some(3));
    assert_eq!(shutdown_calls[0].2.as_deref(), Some("integration shutdown"));
    drop(shutdown_calls);

    let delete_realm = client
        .delete_realm(DeleteRealmRequest {
            realm_id,
            credential: test_credential(),
        })
        .await
        .expect("delete realm should succeed")
        .into_inner();
    assert!(delete_realm.success);

    let delete_missing = client
        .delete_realm(DeleteRealmRequest {
            realm_id,
            credential: test_credential(),
        })
        .await
        .expect("deleting missing realm should return response")
        .into_inner();
    assert!(!delete_missing.success);

    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
#[serial]
async fn supervised_service_get_node_info_returns_internal_on_metrics_failure() {
    let service = Supervisord::new(
        "node-metrics-failure",
        "node-metrics-failure",
        "edge-x",
        "1.0.0",
        ServiceCollector::new(),
    )
    .expect("create supervisord service")
    .with_metrics_provider(|| async {
        Err(SupervitError::Metrics("collector failed".to_string()))
    });

    let (endpoint, handle) = spawn_supervised_service(service).await;
    let mut client = connect_client(&endpoint).await;

    let err = client
        .get_node_info(GetNodeInfoRequest {
            credential: test_credential(),
        })
        .await
        .expect_err("node info should fail when metrics provider fails");
    assert_eq!(err.code(), Code::Internal);
    assert!(
        err.message().contains("Failed to collect metrics"),
        "unexpected error message: {}",
        err.message()
    );

    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
#[serial]
async fn supervised_service_shutdown_handler_failure_and_missing_handler_are_covered() {
    let with_failing_handler = Supervisord::new(
        "node-shutdown-failure",
        "node-shutdown-failure",
        "edge-y",
        "1.0.0",
        ServiceCollector::new(),
    )
    .expect("create supervisord service")
    .with_shutdown_handler(|_, _, _| async {
        Err(SupervitError::Internal(
            "simulated shutdown failure".to_string(),
        ))
    });

    let (endpoint_fail, handle_fail) = spawn_supervised_service(with_failing_handler).await;
    let mut client_fail = connect_client(&endpoint_fail).await;

    let failed_shutdown = client_fail
        .shutdown(ShutdownRequest {
            graceful: true,
            timeout_secs: Some(5),
            reason: Some("failing path".to_string()),
            credential: test_credential(),
        })
        .await
        .expect("shutdown response should be returned")
        .into_inner();
    assert!(!failed_shutdown.accepted);
    assert!(failed_shutdown.estimated_shutdown_time.is_none());
    assert!(
        failed_shutdown
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("Shutdown handler failed")
    );

    handle_fail.abort();
    let _ = handle_fail.await;

    let without_handler = Supervisord::new(
        "node-shutdown-default",
        "node-shutdown-default",
        "edge-z",
        "1.0.0",
        ServiceCollector::new(),
    )
    .expect("create supervisord service");

    let (endpoint_ok, handle_ok) = spawn_supervised_service(without_handler).await;
    let mut client_ok = connect_client(&endpoint_ok).await;

    let immediate_shutdown = client_ok
        .shutdown(ShutdownRequest {
            graceful: false,
            timeout_secs: None,
            reason: Some("no handler".to_string()),
            credential: test_credential(),
        })
        .await
        .expect("shutdown should still be accepted without handler")
        .into_inner();
    assert!(immediate_shutdown.accepted);
    assert!(immediate_shutdown.error_message.is_none());
    assert!(immediate_shutdown.estimated_shutdown_time.is_some());

    handle_ok.abort();
    let _ = handle_ok.await;
}
