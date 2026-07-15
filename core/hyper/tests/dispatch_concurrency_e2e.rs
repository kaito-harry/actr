//! End-to-end proof that conflict-key dispatch concurrency (B2) lets a native
//! `Linked` workload run distinct-key invocations concurrently through the real
//! node mailbox path.
//!
//! The echo node enables `dispatch_concurrency` and declares a conflict key on
//! the request payload's field 1, so two requests with distinct payloads project
//! to distinct keys. Its handler blocks on a `tokio::sync::Barrier` sized for two
//! parties: the barrier only releases once *both* dispatches are simultaneously
//! in flight. If dispatch were serial (as it is gate-off), the first handler
//! would block forever waiting for the second, which would never be dispatched —
//! the caller would time out. Success therefore *is* the concurrency proof, with
//! no wall-clock sleeps.
//!
//! A companion test enables the gate but leaves the key undeclared, so both
//! requests project to the global `Serial` key and must NOT run concurrently —
//! the barrier is proven to stay unreleased while the first handler is in flight.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use actr_framework::{Bytes, Context, MessageDispatcher, Workload};
use actr_hyper::test_support::TestSignalingServer;
use actr_hyper::{
    ActrRef, ConflictKeySpec, DispatchConcurrency, Hyper, HyperConfig, KeySource, Node,
    PayloadFieldKind, StaticTrust,
};
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActrId, ActrType, Realm, RpcEnvelope, RpcRequest};
use async_trait::async_trait;
use tempfile::TempDir;

const ROUTE: &str = "echo";

#[derive(Clone, PartialEq, ProstMessage)]
pub struct EchoRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub payload: Vec<u8>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct EchoResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub payload: Vec<u8>,
}

impl RpcRequest for EchoRequest {
    type Response = EchoResponse;
    fn route_key() -> &'static str {
        ROUTE
    }
}

/// Workload whose dispatch waits on a shared barrier before echoing, so a caller
/// can observe whether two handlers are ever in flight at the same time.
struct BarrierWorkload {
    barrier: Arc<tokio::sync::Barrier>,
    entered: Arc<AtomicUsize>,
}

#[async_trait]
impl Workload for BarrierWorkload {
    type Dispatcher = BarrierDispatcher;
}

struct BarrierDispatcher;

#[async_trait]
impl MessageDispatcher for BarrierDispatcher {
    type Workload = BarrierWorkload;

    async fn dispatch<C: Context>(
        workload: &Self::Workload,
        envelope: RpcEnvelope,
        _ctx: &C,
    ) -> actr_protocol::ActorResult<Bytes> {
        workload.entered.fetch_add(1, Ordering::SeqCst);
        // Blocks until the required number of parties are simultaneously here.
        workload.barrier.wait().await;
        Ok(envelope.payload.unwrap_or_default())
    }
}

/// Workload that hands the test a *release handle* on every dispatch and then
/// parks until the test releases it. This lets the test observe — without any
/// wall-clock sleeps and without ever leaving a permanently-stuck handler that
/// would hang shutdown — exactly how many handlers are simultaneously in
/// flight. Under serial dispatch the second handler is never even started while
/// the first is held open.
struct GatedWorkload {
    entry_tx: tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>,
    entered: Arc<AtomicUsize>,
}

#[async_trait]
impl Workload for GatedWorkload {
    type Dispatcher = GatedDispatcher;
}

struct GatedDispatcher;

#[async_trait]
impl MessageDispatcher for GatedDispatcher {
    type Workload = GatedWorkload;

    async fn dispatch<C: Context>(
        workload: &Self::Workload,
        envelope: RpcEnvelope,
        _ctx: &C,
    ) -> actr_protocol::ActorResult<Bytes> {
        workload.entered.fetch_add(1, Ordering::SeqCst);
        // Publish a release handle to the test and park until it fires. If the
        // test dropped the receiver (teardown), we simply complete.
        let (rel_tx, rel_rx) = tokio::sync::oneshot::channel::<()>();
        let _ = workload.entry_tx.send(rel_tx);
        let _ = rel_rx.await;
        Ok(envelope.payload.unwrap_or_default())
    }
}

fn runtime_config(
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
            tracing_service_name: "dispatch-concurrency-e2e".to_string(),
        },
        config_dir: dir.path().to_path_buf(),
        trust: vec![],
        package_path: None,
        web: None,
    }
}

fn echo_type() -> ActrType {
    ActrType {
        manufacturer: "test-mfr".to_string(),
        name: "BarrierEcho".to_string(),
        version: "1.0.0".to_string(),
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
    candidates
        .into_iter()
        .find(|c| c == expected)
        .expect("discovery returned expected actor")
}

/// Build the caller node and a barrier-gated echo node. `gate` toggles dispatch
/// concurrency; `declare_key` toggles whether the route has a conflict key
/// (needed for distinct-key concurrency — undeclared projects to Serial).
async fn setup(
    server: &TestSignalingServer,
    dir_a: &TempDir,
    dir_b: &TempDir,
    barrier: Arc<tokio::sync::Barrier>,
    entered: Arc<AtomicUsize>,
    gate: bool,
    declare_key: bool,
) -> (ActrRef, ActrRef, ActrId) {
    let ais = format!("http://127.0.0.1:{}/ais", server.port());

    // Echo (responder) node with the barrier workload.
    let mut echo_cfg = HyperConfig::new(dir_b.path(), Arc::new(StaticTrust::dev_only()));
    if gate {
        echo_cfg = echo_cfg.with_dispatch_concurrency(Some(DispatchConcurrency {
            enabled: true,
            budget: 8,
            queue_cap: 256,
            dispatch_timeout: None,
        }));
    }
    let hyper_b = Hyper::new(echo_cfg).await.unwrap();
    let mut cfg_b = runtime_config(dir_b, server, "EchoNode", echo_type());
    cfg_b.websocket_listen_port = Some(0);
    cfg_b.websocket_advertised_host = Some("127.0.0.1".to_string());

    let mut node_b = Node::from_hyper(hyper_b, cfg_b);
    if declare_key {
        node_b = node_b.with_conflict_keys(
            ConflictKeySpec::builder()
                .method(
                    ROUTE,
                    KeySource::PayloadField {
                        tag: 1,
                        kind: PayloadFieldKind::Bytes,
                    },
                )
                .build()
                .unwrap(),
        );
    }
    let echo_actr = node_b
        .link(BarrierWorkload { barrier, entered })
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
    let cfg_a = runtime_config(dir_a, server, "CallerNode", caller_type);
    // The caller also links a barrier workload it never uses (it only makes
    // outbound calls); a fresh 1-party barrier keeps the type simple.
    let caller_actr = Node::from_hyper(hyper_a, cfg_a)
        .link(BarrierWorkload {
            barrier: Arc::new(tokio::sync::Barrier::new(1)),
            entered: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    (caller_actr, echo_actr, echo_id)
}

/// Build the caller + a gate-ON, key-UNDECLARED echo node whose handlers are
/// gated on a release channel (see `GatedWorkload`). The `entry_tx` is the echo
/// node's; the test uses the paired receiver to observe/release handlers.
async fn setup_serial(
    server: &TestSignalingServer,
    dir_a: &TempDir,
    dir_b: &TempDir,
    entry_tx: tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>,
    entered: Arc<AtomicUsize>,
) -> (ActrRef, ActrRef, ActrId) {
    let ais = format!("http://127.0.0.1:{}/ais", server.port());

    // Echo node: dispatch concurrency ON but NO conflict key declared, so every
    // request projects to the global `Serial` key.
    let echo_cfg = HyperConfig::new(dir_b.path(), Arc::new(StaticTrust::dev_only()))
        .with_dispatch_concurrency(Some(DispatchConcurrency {
            enabled: true,
            budget: 8,
            queue_cap: 256,
            dispatch_timeout: None,
        }));
    let hyper_b = Hyper::new(echo_cfg).await.unwrap();
    let mut cfg_b = runtime_config(dir_b, server, "EchoNode", echo_type());
    cfg_b.websocket_listen_port = Some(0);
    cfg_b.websocket_advertised_host = Some("127.0.0.1".to_string());
    // NOTE: no `with_conflict_keys` → the route is undeclared → Serial.
    let echo_actr = Node::from_hyper(hyper_b, cfg_b)
        .link(GatedWorkload { entry_tx, entered })
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();
    let echo_id = echo_actr.actor_id();

    // Caller node with a dummy gated workload it never exercises.
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
    let cfg_a = runtime_config(dir_a, server, "CallerNode", caller_type);
    let (dummy_tx, _dummy_rx) = tokio::sync::mpsc::unbounded_channel();
    let caller_actr = Node::from_hyper(hyper_a, cfg_a)
        .link(GatedWorkload {
            entry_tx: dummy_tx,
            entered: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .unwrap()
        .register(&ais)
        .await
        .unwrap()
        .start()
        .await
        .unwrap();

    (caller_actr, echo_actr, echo_id)
}

/// Gate ON + distinct keys declared → the two distinct-payload requests must run
/// concurrently. The 2-party barrier releases only if both handlers are in
/// flight together, so both calls returning is the concurrency proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_keys_dispatch_concurrently_end_to_end() {
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let entered = Arc::new(AtomicUsize::new(0));

    let (caller, echo, echo_id) = setup(
        &server,
        &dir_a,
        &dir_b,
        barrier.clone(),
        entered.clone(),
        true,
        true,
    )
    .await;

    let target = discover_required(&caller, &echo_type(), &echo_id).await;

    // Two concurrent calls with distinct payloads → distinct conflict keys.
    let c1 = {
        let caller = caller.clone();
        let target = target.clone();
        tokio::spawn(async move {
            caller
                .call_remote::<EchoRequest>(
                    target,
                    EchoRequest {
                        payload: b"room-A".to_vec(),
                    },
                )
                .await
        })
    };
    let c2 = {
        let caller = caller.clone();
        let target = target.clone();
        tokio::spawn(async move {
            caller
                .call_remote::<EchoRequest>(
                    target,
                    EchoRequest {
                        payload: b"room-B".to_vec(),
                    },
                )
                .await
        })
    };

    // If dispatch were serial, the first handler would block on the 2-party
    // barrier forever → this timeout would fire. Success = concurrency.
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        (c1.await.unwrap(), c2.await.unwrap())
    })
    .await
    .expect("both concurrent dispatches must complete (proves concurrency)");

    assert_eq!(r1.expect("call A ok").payload, b"room-A");
    assert_eq!(r2.expect("call B ok").payload, b"room-B");
    assert_eq!(
        entered.load(Ordering::SeqCst),
        2,
        "both handlers must have entered"
    );

    caller.shutdown();
    caller.wait_for_shutdown().await;
    echo.shutdown();
    echo.wait_for_shutdown().await;
    server.shutdown().await;
}

/// Gate ON but NO key declared → both requests project to the global `Serial`
/// key and must NOT overlap. This is the *falsifiable* form of that claim: we
/// fire **two** concurrent undeclared requests and prove the second handler is
/// never started while the first is still in flight. A regression that let the
/// undeclared route dispatch concurrently would surface a second handler here
/// and fail the test.
///
/// No wall-clock sleeps: handlers are held open via a per-invocation release
/// channel, so the test drives completion order directly. The one bounded
/// observation window (`OBSERVE`) is the only sound way to assert the *negative*
/// — "no second handler appeared while the first was in flight" — and stands in
/// for (and forbids) sleep-based coordination.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undeclared_route_stays_serial_end_to_end() {
    let mut server = TestSignalingServer::start().await.expect("server start");
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let (entry_tx, mut entry_rx) =
        tokio::sync::mpsc::unbounded_channel::<tokio::sync::oneshot::Sender<()>>();
    let entered = Arc::new(AtomicUsize::new(0));

    let (caller, echo, echo_id) =
        setup_serial(&server, &dir_a, &dir_b, entry_tx, entered.clone()).await;

    let target = discover_required(&caller, &echo_type(), &echo_id).await;

    // Two concurrent undeclared requests with distinct payloads. Distinct
    // payloads would project to distinct keys *if a key were declared* — but it
    // is not, so both must land on the global Serial key.
    let c1 = {
        let caller = caller.clone();
        let target = target.clone();
        tokio::spawn(async move {
            caller
                .call_remote::<EchoRequest>(
                    target,
                    EchoRequest {
                        payload: b"room-A".to_vec(),
                    },
                )
                .await
        })
    };
    let c2 = {
        let caller = caller.clone();
        let target = target.clone();
        tokio::spawn(async move {
            caller
                .call_remote::<EchoRequest>(
                    target,
                    EchoRequest {
                        payload: b"room-B".to_vec(),
                    },
                )
                .await
        })
    };

    // The first handler enters and parks on its release channel.
    let rel1 = entry_rx
        .recv()
        .await
        .expect("first handler must enter and publish its release handle");

    // While the first handler is held in flight, a serial runner must NOT have
    // started the second one. A concurrent dispatch would surface a second entry
    // within this window; its absence is the serial proof.
    const OBSERVE: std::time::Duration = std::time::Duration::from_secs(2);
    match tokio::time::timeout(OBSERVE, entry_rx.recv()).await {
        Ok(Some(rel2)) => {
            // Concurrency leak: release both handlers so teardown is clean, then
            // fail loudly.
            let _ = rel1.send(());
            let _ = rel2.send(());
            panic!(
                "undeclared route dispatched a second handler while the first was \
                 in flight; gate-on + undeclared must stay fully serial"
            );
        }
        Ok(None) => panic!("entry channel closed before the serial proof completed"),
        Err(_) => { /* timed out: no second handler while the first was held → serial */ }
    }
    assert_eq!(
        entered.load(Ordering::SeqCst),
        1,
        "exactly one handler may be in flight under serial dispatch"
    );

    // Release the first handler; the runner may now start the second.
    let _ = rel1.send(());
    let rel2 = tokio::time::timeout(std::time::Duration::from_secs(30), entry_rx.recv())
        .await
        .expect("second handler must start once the first completes")
        .expect("entry channel still open");
    let _ = rel2.send(());

    // Both calls complete correctly, one after the other.
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        (c1.await.unwrap(), c2.await.unwrap())
    })
    .await
    .expect("both calls complete after serial dispatch");
    let mut got = [
        r1.expect("call A ok").payload,
        r2.expect("call B ok").payload,
    ];
    got.sort();
    assert_eq!(
        got,
        [b"room-A".to_vec(), b"room-B".to_vec()],
        "both distinct payloads must be echoed back"
    );
    assert_eq!(
        entered.load(Ordering::SeqCst),
        2,
        "both handlers must eventually have run"
    );

    caller.shutdown();
    caller.wait_for_shutdown().await;
    echo.shutdown();
    echo.wait_for_shutdown().await;
    server.shutdown().await;
}
