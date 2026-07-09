//! Smoke test: start a fully-linked Node against a mock signaling/AIS server.
//!
//! Drives the `Node::from_hyper → link → register → start` typestate chain,
//! which is the only path that exercises `lifecycle::node::Inner::start` (the
//! ~750-line async runtime bring-up). Existing integration tests bypass Node
//! and construct coordinators directly, so `start` was previously uncovered.

use std::collections::HashMap;
use std::sync::Arc;

use actr_framework::{Bytes, Context, Dest, MessageDispatcher, Workload};
use actr_hyper::test_support::TestSignalingServer;
use actr_hyper::{ActrRef, Hyper, HyperConfig, Node, StaticTrust};
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActrError, ActrId, ActrType, PayloadType, Realm, RpcEnvelope, RpcRequest};
use async_trait::async_trait;
use tempfile::TempDir;

/// Minimal typed RPC request used for cross-node calls.
#[derive(Clone, PartialEq, ProstMessage)]
pub struct EchoRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub payload: Vec<u8>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct SmokeEchoResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub payload: Vec<u8>,
}

impl RpcRequest for EchoRequest {
    type Response = SmokeEchoResponse;
    fn route_key() -> &'static str {
        "echo"
    }
}

/// Request whose route_key no workload handles — drives the inbound dispatch
/// error path (workload returns InvalidArgument → propagated to caller).
#[derive(Clone, PartialEq, ProstMessage)]
pub struct UnknownRouteRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub payload: Vec<u8>,
}

impl RpcRequest for UnknownRouteRequest {
    type Response = SmokeEchoResponse;
    fn route_key() -> &'static str {
        "no/such/route"
    }
}

/// Minimal linked workload whose dispatcher echoes the payload back.
struct EchoWorkload;

#[async_trait]
impl Workload for EchoWorkload {
    type Dispatcher = EchoDispatcher;
}

struct EchoDispatcher;

#[async_trait]
impl MessageDispatcher for EchoDispatcher {
    type Workload = EchoWorkload;

    async fn dispatch<C: Context>(
        _workload: &Self::Workload,
        envelope: RpcEnvelope,
        _ctx: &C,
    ) -> actr_protocol::ActorResult<Bytes> {
        match envelope.route_key.as_str() {
            "echo" => Ok(envelope.payload.unwrap_or_default()),
            other => Err(actr_protocol::ActrError::InvalidArgument(format!(
                "unknown route: {other}"
            ))),
        }
    }
}

fn runtime_config(
    dir: &TempDir,
    server: &TestSignalingServer,
    name: &str,
) -> actr_config::RuntimeConfig {
    runtime_config_with_type(
        dir,
        server,
        name,
        ActrType {
            manufacturer: "test-mfr".to_string(),
            name: name.to_string(),
            version: "1.0.0".to_string(),
        },
    )
}

fn runtime_config_with_type(
    dir: &TempDir,
    server: &TestSignalingServer,
    name: &str,
    actr_type: ActrType,
) -> actr_config::RuntimeConfig {
    actr_config::RuntimeConfig {
        package: actr_config::PackageInfo {
            name: name.to_string(),
            actr_type,
            description: None,
            authors: vec![],
            license: None,
        },
        signaling_url: url::Url::parse(&server.url()).unwrap(),
        realm: Realm { realm_id: 7 },
        ais_endpoint: format!("http://127.0.0.1:{}/ais", server.port()),
        realm_secret: None,
        visible_in_discovery: true,
        acl: None,
        mailbox_path: None,
        scripts: HashMap::new(),
        webrtc: actr_config::WebRtcConfig::default(),
        websocket_listen_port: None,
        websocket_advertised_host: None,
        observability: actr_config::ObservabilityConfig {
            filter_level: "warn".to_string(),
            tracing_enabled: false,
            tracing_endpoint: String::new(),
            tracing_service_name: "node-start-smoke".to_string(),
        },
        config_dir: dir.path().to_path_buf(),
        trust: vec![],
        package_path: None,
        web: None,
    }
}

async fn discover_required(caller: &ActrRef, target_type: &ActrType, expected: &ActrId) -> ActrId {
    let candidates = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        caller.discover_route_candidates(target_type, 1),
    )
    .await
    .expect("discover must not hang")
    .expect("discover must not error");

    assert!(
        candidates.iter().any(|candidate| candidate == expected),
        "discovery did not return expected actor {expected:?}; candidates: {candidates:?}"
    );

    candidates
        .into_iter()
        .next()
        .expect("discovery returned no candidates")
}

async fn assert_echo_rpc(caller: &ActrRef, target: ActrId, payload: &[u8]) {
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        caller.call_remote::<EchoRequest>(
            target,
            EchoRequest {
                payload: payload.to_vec(),
            },
        ),
    )
    .await
    .expect("echo RPC must not hang")
    .expect("echo RPC must succeed");

    assert_eq!(response.payload, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linked_node_starts_and_shuts_down_cleanly() {
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let node = Node::from_hyper(hyper, runtime_config(&dir, &server, "SmokeNode"))
        .link(EchoWorkload)
        .await
        .expect("link should attach the workload");

    // register hits the mock AIS (MockActrixServer serves /ais/register).
    let registered = node
        .register(&format!("http://127.0.0.1:{}/ais", server.port()))
        .await
        .expect("register should succeed against mock AIS");

    let actr = registered.start().await.expect("node should start");
    // Node is live: it has an actor id and is not yet shutting down.
    let _id = actr.actor_id();
    assert!(!actr.is_shutting_down());

    // Graceful shutdown.
    actr.shutdown();
    actr.wait_for_shutdown().await;

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linked_node_advertises_websocket_listen_port() {
    // Setting websocket_listen_port makes `start` build a ws:// address and
    // register it with the signaling server, and binds a WebSocketServer.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let mut cfg = runtime_config(&dir, &server, "WsAdvertised");
    // Bind an ephemeral port; the advertised address is constructed from it.
    cfg.websocket_listen_port = Some(0);
    cfg.websocket_advertised_host = Some("127.0.0.1".to_string());

    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let registered = Node::from_hyper(hyper, cfg)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .expect("register should succeed");

    let actr = registered.start().await.expect("node should start");
    assert!(!actr.is_shutting_down());

    actr.shutdown();
    actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_against_unreachable_ais_errors() {
    // A garbage AIS endpoint must surface a registration error rather than
    // hanging or starting the node.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let node = Node::from_hyper(hyper, runtime_config(&dir, &server, "AisFail"))
        .link(EchoWorkload)
        .await
        .unwrap();

    // Point AIS at a closed port → register must fail.
    let result = node.register("http://127.0.0.1:1/ais").await;
    assert!(
        result.is_err(),
        "register against an unreachable AIS must error"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linked_node_runs_background_tasks_before_shutdown() {
    // Start the node, let background tasks (signaling heartbeat, mailbox poll,
    // coordinator idle) run briefly, then shut down. This exercises the
    // long-running spawned tasks inside `start` rather than only the bring-up.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let actr = Node::from_hyper(hyper, runtime_config(&dir, &server, "BgRun"))
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .expect("register")
        .start()
        .await
        .expect("start");

    // Let the runtime settle so spawned background loops make progress.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!actr.is_shutting_down());

    actr.shutdown();
    actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn double_shutdown_is_idempotent() {
    // Calling shutdown twice and then waiting must not panic or hang.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let actr = Node::from_hyper(hyper, runtime_config(&dir, &server, "DoubleStop"))
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    actr.shutdown();
    actr.shutdown(); // idempotent
    actr.wait_for_shutdown().await;
    assert!(actr.is_shutting_down());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discover_unknown_type_returns_no_candidates() {
    // discover_route_candidates against a type no peer registered must return
    // an empty list (or error) without hanging — exercises the signaling
    // route-candidates request path inside the running node.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let actr = Node::from_hyper(hyper, runtime_config(&dir, &server, "Discover"))
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    let unknown_type = ActrType {
        manufacturer: "nobody".to_string(),
        name: "NoSuchActor".to_string(),
        version: "0.0.0".to_string(),
    };
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        actr.discover_route_candidates(&unknown_type, 1),
    )
    .await
    .expect("discover must not hang")
    .expect_err("unknown type should not return route candidates");
    assert!(
        matches!(err, ActrError::NotFound(_)),
        "unknown type should map to NotFound, got {err:?}"
    );

    actr.shutdown();
    actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_connect_and_exchange_rpc() {
    // Two linked nodes on the same signaling server: the caller discovers the
    // echo node by ActrType and issues a typed RPC. This drives coordinator
    // connection establishment, signaling route-candidates, and the inbound
    // handle_incoming dispatch path — the largest remaining uncovered surface.
    let mut server = TestSignalingServer::start().await.expect("server start");

    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let echo_type = ActrType {
        manufacturer: "test-mfr".to_string(),
        name: "EchoService".to_string(),
        version: "1.0.0".to_string(),
    };

    // Echo node (responder).
    let hyper_b = Hyper::new(HyperConfig::new(
        dir_b.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let mut cfg_b = runtime_config_with_type(&dir_b, &server, "EchoNode", echo_type.clone());
    cfg_b.websocket_listen_port = Some(0);
    cfg_b.websocket_advertised_host = Some("127.0.0.1".to_string());
    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let echo_actr = Node::from_hyper(hyper_b, cfg_b)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    let echo_id = echo_actr.actor_id();

    // Caller node.
    let hyper_a = Hyper::new(HyperConfig::new(
        dir_a.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let caller_type = ActrType {
        manufacturer: "test-mfr".to_string(),
        name: "Caller".to_string(),
        version: "1.0.0".to_string(),
    };
    let cfg_a = runtime_config_with_type(&dir_a, &server, "CallerNode", caller_type);
    let caller_actr = Node::from_hyper(hyper_a, cfg_a)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    let target = discover_required(&caller_actr, &echo_type, &echo_id).await;
    assert_echo_rpc(&caller_actr, target, b"ping").await;

    caller_actr.shutdown();
    caller_actr.wait_for_shutdown().await;
    echo_actr.shutdown();
    echo_actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_rpc_unknown_route_returns_error() {
    // A call to a route_key no workload handles must propagate an error back
    // to the caller — exercises the inbound dispatch error path.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let echo_type = ActrType {
        manufacturer: "test-mfr".to_string(),
        name: "EchoService".to_string(),
        version: "1.0.0".to_string(),
    };

    let hyper_b = Hyper::new(HyperConfig::new(
        dir_b.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let mut cfg_b = runtime_config_with_type(&dir_b, &server, "EchoNode", echo_type.clone());
    cfg_b.websocket_listen_port = Some(0);
    cfg_b.websocket_advertised_host = Some("127.0.0.1".to_string());
    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let echo_actr = Node::from_hyper(hyper_b, cfg_b)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    let echo_id = echo_actr.actor_id();

    let hyper_a = Hyper::new(HyperConfig::new(
        dir_a.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let caller_type = ActrType {
        manufacturer: "test-mfr".to_string(),
        name: "Caller2".to_string(),
        version: "1.0.0".to_string(),
    };
    let cfg_a = runtime_config_with_type(&dir_a, &server, "CallerNode2", caller_type);
    let caller_actr = Node::from_hyper(hyper_a, cfg_a)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    // First discover to warm the connection, then issue the unknown-route call.
    let target = discover_required(&caller_actr, &echo_type, &echo_id).await;

    let err = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        caller_actr.call_remote::<UnknownRouteRequest>(target, UnknownRouteRequest::default()),
    )
    .await
    .expect("unknown-route RPC must not hang")
    .expect_err("unknown route should propagate an error");
    assert!(
        err.to_string().contains("unknown route"),
        "unexpected unknown-route error: {err:?}"
    );

    caller_actr.shutdown();
    caller_actr.wait_for_shutdown().await;
    echo_actr.shutdown();
    echo_actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_connect_over_webrtc() {
    // Without a WebSocket listen port advertised by the echo node, the caller
    // must establish the connection over WebRTC — drives the coordinator's
    // WebRTC connection-creation path (offer/answer/ICE) rather than the
    // WebSocket direct-connect fast path.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let echo_type = ActrType {
        manufacturer: "test-mfr".to_string(),
        name: "EchoWebrtc".to_string(),
        version: "1.0.0".to_string(),
    };

    // Echo node with NO websocket_listen_port → forces WebRTC.
    let hyper_b = Hyper::new(HyperConfig::new(
        dir_b.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let cfg_b = runtime_config_with_type(&dir_b, &server, "EchoWebrtcNode", echo_type.clone());
    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let echo_actr = Node::from_hyper(hyper_b, cfg_b)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    let echo_id = echo_actr.actor_id();

    let hyper_a = Hyper::new(HyperConfig::new(
        dir_a.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let caller_type = ActrType {
        manufacturer: "test-mfr".to_string(),
        name: "CallerWebrtc".to_string(),
        version: "1.0.0".to_string(),
    };
    let cfg_a = runtime_config_with_type(&dir_a, &server, "CallerWebrtcNode", caller_type);
    let caller_actr = Node::from_hyper(hyper_a, cfg_a)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    let target = discover_required(&caller_actr, &echo_type, &echo_id).await;
    assert_echo_rpc(&caller_actr, target, b"hi").await;

    caller_actr.shutdown();
    caller_actr.wait_for_shutdown().await;
    echo_actr.shutdown();
    echo_actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_remains_running_during_signaling_pause_and_resume_smoke() {
    // Pause then resume signaling forwarding while the node runs. This is a
    // bounded smoke test for the signaling disruption path; it does not assert
    // detailed reconnect state transitions.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let actr = Node::from_hyper(hyper, runtime_config(&dir, &server, "Reconnect"))
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    // Let the node connect to signaling, then disrupt forwarding and restore.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    server.pause_forwarding();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    server.resume_forwarding();
    // Give background signaling logic time to run.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!actr.is_shutting_down());

    actr.shutdown();
    actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_handles_signaling_blackhole_io_without_panic_smoke() {
    // Blackhole WebSocket I/O (drop packets without closing) to exercise the
    // heartbeat-timeout disruption path as a bounded smoke test.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let actr = Node::from_hyper(hyper, runtime_config(&dir, &server, "Blackhole"))
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    server.blackhole_websocket_io();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    server.restore_websocket_io();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    actr.shutdown();
    actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_drop_ice_candidates_returns_bounded_rpc_result() {
    // Dropping ICE candidates between two connected nodes disrupts connectivity;
    // this test asserts the RPC resolves with either success or a transport-class
    // error instead of hanging.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let echo_type = ActrType {
        manufacturer: "test-mfr".to_string(),
        name: "EchoIce".to_string(),
        version: "1.0.0".to_string(),
    };

    let hyper_b = Hyper::new(HyperConfig::new(
        dir_b.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let cfg_b = runtime_config_with_type(&dir_b, &server, "EchoIceNode", echo_type.clone());
    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let echo_actr = Node::from_hyper(hyper_b, cfg_b)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    let echo_id = echo_actr.actor_id();

    let hyper_a = Hyper::new(HyperConfig::new(
        dir_a.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let cfg_a = runtime_config_with_type(
        &dir_a,
        &server,
        "CallerIceNode",
        ActrType {
            manufacturer: "test-mfr".into(),
            name: "CallerIce".into(),
            version: "1.0.0".into(),
        },
    );
    let caller_actr = Node::from_hyper(hyper_a, cfg_a)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    // Drop ICE candidates to disrupt connectivity, then attempt a bounded call.
    server.drop_next_ice_candidates(3);
    let target = discover_required(&caller_actr, &echo_type, &echo_id).await;

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        caller_actr.call_remote::<EchoRequest>(
            target.clone(),
            EchoRequest {
                payload: b"x".to_vec(),
            },
        ),
    )
    .await
    .expect("ICE-disrupted call must resolve instead of hanging");
    match result {
        Ok(response) => assert_eq!(response.payload, b"x"),
        Err(err) => assert!(
            matches!(
                err,
                ActrError::ConnectionNotReady(_) | ActrError::Unavailable(_) | ActrError::TimedOut
            ),
            "ICE disruption should return a transport-class error, got {err:?}"
        ),
    }

    caller_actr.shutdown();
    caller_actr.wait_for_shutdown().await;
    echo_actr.shutdown();
    echo_actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linked_node_registers_with_realm_secret() {
    // Setting realm_secret exercises the linked-registration auth path that
    // supplies the x-actrix-realm-secret header, a distinct branch from the
    // default linked register flow.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let mut cfg = runtime_config(&dir, &server, "RealmSecretNode");
    cfg.realm_secret = Some("test-realm-secret".to_string());

    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let actr = Node::from_hyper(hyper, cfg)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .expect("register with realm secret should succeed")
        .start()
        .await
        .expect("start");

    // Let the node run briefly so background tasks progress.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    actr.shutdown();
    actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linked_node_run_with_acl_and_scripts_config() {
    // Non-default ACL and scripts config exercises the runtime config plumbing.
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let mut cfg = runtime_config(&dir, &server, "AclNode");
    cfg.acl = Some(actr_protocol::Acl { rules: vec![] });
    cfg.scripts
        .insert("greet".to_string(), "echo hi".to_string());

    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let actr = Node::from_hyper(hyper, cfg)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    actr.shutdown();
    actr.wait_for_shutdown().await;
    server.shutdown().await;
}

/// A second linked workload used purely as a host-side hook observer.
struct NoopObserver;

#[async_trait]
impl Workload for NoopObserver {
    type Dispatcher = NoopDispatcher;
}

struct NoopDispatcher;

#[async_trait]
impl MessageDispatcher for NoopDispatcher {
    type Workload = NoopObserver;
    async fn dispatch<C: Context>(
        _workload: &Self::Workload,
        _envelope: RpcEnvelope,
        _ctx: &C,
    ) -> actr_protocol::ActorResult<Bytes> {
        Err(actr_protocol::ActrError::NotImplemented("observer".into()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linked_node_accepts_extra_hook_observer() {
    // `with_hook_observer` chains a second observer onto an attached node —
    // exercises Node<Attached>::with_hook_observer (lib.rs L842-856).
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir = TempDir::new().unwrap();

    let hyper = Hyper::new(HyperConfig::new(
        dir.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();

    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let actr = Node::from_hyper(hyper, runtime_config(&dir, &server, "ObsNode"))
        .link(EchoWorkload)
        .await
        .unwrap()
        // Chain a second (no-op) host observer alongside the linked observer.
        .with_hook_observer(NoopObserver)
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    actr.shutdown();
    actr.wait_for_shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_disconnect_triggers_coordinator_cleanup() {
    // Two nodes connect, then the echo node shuts down. The caller observes
    // the peer going away, driving the coordinator's peer-disconnect cleanup
    // path (close peer, remove session, emit disconnected hook).
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let echo_type = ActrType {
        manufacturer: "test-mfr".to_string(),
        name: "EchoDisconnect".to_string(),
        version: "1.0.0".to_string(),
    };

    let hyper_b = Hyper::new(HyperConfig::new(
        dir_b.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let mut cfg_b = runtime_config_with_type(&dir_b, &server, "EchoDiscNode", echo_type.clone());
    cfg_b.websocket_listen_port = Some(0);
    cfg_b.websocket_advertised_host = Some("127.0.0.1".to_string());
    let ais = format!("http://127.0.0.1:{}/ais", server.port());
    let echo_actr = Node::from_hyper(hyper_b, cfg_b)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    let echo_id = echo_actr.actor_id();

    let hyper_a = Hyper::new(HyperConfig::new(
        dir_a.path(),
        Arc::new(StaticTrust::dev_only()),
    ))
    .await
    .unwrap();
    let cfg_a = runtime_config_with_type(
        &dir_a,
        &server,
        "CallerDiscNode",
        ActrType {
            manufacturer: "test-mfr".into(),
            name: "CallerDisc".into(),
            version: "1.0.0".into(),
        },
    );
    let caller_actr = Node::from_hyper(hyper_a, cfg_a)
        .link(EchoWorkload)
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    // Establish a connection via discovery + a call.
    let target = discover_required(&caller_actr, &echo_type, &echo_id).await;
    assert_echo_rpc(&caller_actr, target.clone(), b"hi").await;

    // Now shut down the echo peer — caller should detect the disconnect and
    // clean up its side of the connection.
    echo_actr.shutdown();
    echo_actr.wait_for_shutdown().await;

    // Give the caller time to observe the peer loss and run cleanup.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let context = caller_actr.app_context().await;
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        context.call_raw(
            &Dest::Peer(target),
            EchoRequest::route_key().to_string(),
            PayloadType::RpcReliable,
            EchoRequest {
                payload: b"after-disconnect".to_vec(),
            }
            .encode_to_vec()
            .into(),
            1_000,
        ),
    )
    .await
    .expect("post-disconnect call must resolve")
    .expect_err("call to a shut down peer should fail");
    assert!(
        matches!(
            err,
            ActrError::ConnectionNotReady(_) | ActrError::Unavailable(_) | ActrError::TimedOut
        ),
        "post-disconnect call should return a transport-class error, got {err:?}"
    );

    caller_actr.shutdown();
    caller_actr.wait_for_shutdown().await;
    server.shutdown().await;
}
