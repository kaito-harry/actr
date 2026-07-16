use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering as UsizeOrdering};

/// Simple fake SignalingClient implementation for testing the reconnect helper.
struct FakeSignalingClient {
    event_tx: broadcast::Sender<SignalingEvent>,
    connected: AtomicBool,
    connect_calls: Arc<AtomicUsize>,
    actor_id: tokio::sync::Mutex<Option<ActrId>>,
    credential_state: tokio::sync::Mutex<Option<CredentialState>>,
}

#[async_trait]
impl SignalingClient for FakeSignalingClient {
    async fn connect(&self) -> NetworkResult<()> {
        self.connect_calls.fetch_add(1, UsizeOrdering::SeqCst);
        Ok(())
    }

    async fn disconnect(&self) -> NetworkResult<()> {
        Ok(())
    }

    async fn send_register_request(
        &self,
        _request: RegisterRequest,
    ) -> NetworkResult<RegisterResponse> {
        unimplemented!("not needed in tests");
    }

    async fn send_unregister_request(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _reason: Option<String>,
    ) -> NetworkResult<UnregisterResponse> {
        unimplemented!("not needed in tests");
    }

    async fn send_heartbeat(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _availability: ServiceAvailabilityState,
        _power_reserve: f32,
        _mailbox_backlog: f32,
    ) -> NetworkResult<Pong> {
        unimplemented!("not needed in tests");
    }

    async fn send_route_candidates_request(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _request: RouteCandidatesRequest,
    ) -> NetworkResult<RouteCandidatesResponse> {
        unimplemented!("not needed in tests");
    }

    async fn get_signing_key(
        &self,
        _actor_id: ActrId,
        _credential: AIdCredential,
        _key_id: u32,
    ) -> NetworkResult<(u32, Vec<u8>)> {
        unimplemented!("not needed in tests");
    }

    async fn send_envelope(&self, _envelope: SignalingEnvelope) -> NetworkResult<()> {
        unimplemented!("not needed in tests");
    }

    async fn receive_envelope(&self) -> NetworkResult<Option<SignalingEnvelope>> {
        unimplemented!("not needed in tests");
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    fn get_stats(&self) -> SignalingStats {
        SignalingStats::default()
    }

    fn subscribe_events(&self) -> broadcast::Receiver<SignalingEvent> {
        self.event_tx.subscribe()
    }

    async fn set_actor_id(&self, actor_id: ActrId) {
        *self.actor_id.lock().await = Some(actor_id);
    }

    async fn set_credential_state(&self, credential_state: CredentialState) {
        *self.credential_state.lock().await = Some(credential_state);
    }

    async fn clear_identity(&self) {
        *self.actor_id.lock().await = None;
        *self.credential_state.lock().await = None;
    }
}

fn make_fake_client() -> Arc<FakeSignalingClient> {
    let (event_tx, _erx) = broadcast::channel(64);
    Arc::new(FakeSignalingClient {
        event_tx,
        connected: AtomicBool::new(false),
        connect_calls: Arc::new(AtomicUsize::new(0)),
        actor_id: tokio::sync::Mutex::new(None),
        credential_state: tokio::sync::Mutex::new(None),
    })
}

/// Helper: create a minimal SignalingConfig with an unreachable URL.
fn make_config() -> SignalingConfig {
    SignalingConfig {
        server_url: Url::parse("ws://127.0.0.1:1/signaling/ws").unwrap(),
        connection_timeout: 2,
        heartbeat_interval: 30,
        reconnect_config: ReconnectConfig::default(),
        auth_config: None,
        webrtc_role: None,
    }
}

/// Helper: create a WebSocketSignalingClient wrapped in Arc
fn make_ws_client(config: SignalingConfig) -> Arc<WebSocketSignalingClient> {
    Arc::new(WebSocketSignalingClient::new(config))
}

#[tokio::test]
async fn probe_alive_times_out_when_sink_lock_is_stalled() {
    let client = make_ws_client(make_config());
    client.connected.store(true, Ordering::Release);

    let _sink_guard = client.ws_sink.lock().await;

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        client.probe_alive(Duration::from_millis(20)),
    )
    .await
    .expect("probe should be bounded by its own timeout");

    let err = result.expect_err("stalled sink lock should fail the probe");
    assert!(
        err.to_string()
            .contains("Timed out sending signaling probe ping"),
        "unexpected error: {err}"
    );
    assert!(
        !client.is_connected(),
        "stalled probe send should mark signaling disconnected"
    );
    assert_eq!(client.get_stats().disconnections, 1);
    assert!(
        client.pending_pongs.lock().await.is_empty(),
        "failed probe send should remove its pending pong waiter"
    );
}

#[tokio::test(start_paused = true)]
async fn send_envelope_times_out_while_waiting_for_sink_lock() {
    let client = make_ws_client(make_config());
    client.connected.store(true, Ordering::Release);
    let mut events = client.subscribe_events();
    let _sink_guard = client.ws_sink.lock().await;

    let send_task = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.send_envelope(SignalingEnvelope::default()).await })
    };
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(SIGNALING_SEND_TIMEOUT_SECS + 1)).await;

    let err = send_task
        .await
        .expect("send task should join")
        .expect_err("stalled sink lock should time out");
    assert!(
        err.to_string().contains("sink lock/send timed out"),
        "unexpected error: {err}"
    );
    assert!(
        !client.is_connected(),
        "a timed-out signaling commit should mark the socket disconnected"
    );
    assert_eq!(client.get_stats().disconnections, 1);
    assert_eq!(client.get_stats().errors, 1);
    assert!(matches!(
        events.recv().await,
        Ok(SignalingEvent::Disconnected {
            reason: DisconnectReason::SendTimeout
        })
    ));
    tokio::time::timeout(Duration::from_secs(1), client.reconnect_notify.notified())
        .await
        .expect("send timeout should wake the reconnect manager");
}

#[tokio::test]
async fn send_envelope_failure_publishes_disconnect_and_requests_reconnect() {
    let client = make_ws_client(make_config());
    client.connected.store(true, Ordering::Release);
    let mut events = client.subscribe_events();

    let err = client
        .send_envelope(SignalingEnvelope::default())
        .await
        .expect_err("a connected flag without a WebSocket sink should fail");
    assert!(
        err.to_string().contains("Not connected"),
        "unexpected error: {err}"
    );
    assert!(!client.is_connected());
    assert_eq!(client.get_stats().disconnections, 1);
    assert_eq!(client.get_stats().errors, 1);
    assert!(matches!(
        events.recv().await,
        Ok(SignalingEvent::Disconnected {
            reason: DisconnectReason::SendFailed
        })
    ));
    tokio::time::timeout(Duration::from_secs(1), client.reconnect_notify.notified())
        .await
        .expect("send failure should wake the reconnect manager");
}

#[tokio::test]
async fn send_envelope_failure_does_not_wait_for_reentrant_hook() {
    let client = make_ws_client(make_config());
    client.connected.store(true, Ordering::Release);

    let hook_entered = Arc::new(tokio::sync::Notify::new());
    let hook_release = Arc::new(tokio::sync::Notify::new());
    let hook_entered_for_cb = hook_entered.clone();
    let hook_release_for_cb = hook_release.clone();
    client.set_hook_callback(Arc::new(move |event| {
        let hook_entered = hook_entered_for_cb.clone();
        let hook_release = hook_release_for_cb.clone();
        Box::pin(async move {
            if matches!(event, HookEvent::SignalingDisconnected) {
                hook_entered.notify_one();
                hook_release.notified().await;
            }
        })
    }));

    tokio::time::timeout(
        Duration::from_millis(250),
        client.send_envelope(SignalingEnvelope::default()),
    )
    .await
    .expect("send failure must return without waiting for the hook")
    .expect_err("missing WebSocket sink should fail");

    tokio::time::timeout(Duration::from_millis(250), hook_entered.notified())
        .await
        .expect("disconnected hook should still be invoked");
    hook_release.notify_one();
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn explicit_connect_once_retries_after_concurrent_attempt_fails() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let server_url = format!(
        "ws://{}/signaling/ws",
        listener
            .local_addr()
            .expect("test listener should have local addr")
    );
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("test server should accept tcp connection");
        let ws_stream = tokio_tungstenite::accept_async(stream)
            .await
            .expect("test server should complete websocket handshake");
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(ws_stream);
    });

    let mut config = make_config();
    config.server_url = Url::parse(&server_url).expect("test websocket URL should parse");
    config.connection_timeout = 2;
    config.reconnect_config = ReconnectConfig {
        enabled: false,
        ..ReconnectConfig::default()
    };
    let client = make_ws_client(config);

    client.connecting.store(true, Ordering::Release);
    let connect_task = {
        let client = client.clone();
        tokio::spawn(async move { client.connect_once().await })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;
    client.connecting.store(false, Ordering::Release);
    let _ = client.event_tx.send(SignalingEvent::Disconnected {
        reason: DisconnectReason::ConnectionFailed("simulated auto attempt failed".into()),
    });

    tokio::time::timeout(Duration::from_secs(2), connect_task)
        .await
        .expect("explicit connect_once should not wait for auto backoff")
        .expect("connect_once task should not panic")
        .expect("explicit connect_once should retry after concurrent failure");

    assert!(
        client.is_connected(),
        "explicit recovery connect should establish signaling"
    );

    client.disconnect().await.ok();
    let _ = tokio::time::timeout(Duration::from_secs(1), server_task).await;
}

#[tokio::test]
async fn network_restore_connect_once_preempts_connect_backoff() {
    let reserved_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should reserve a local port");
    let addr = reserved_listener
        .local_addr()
        .expect("reserved listener should have local addr");
    drop(reserved_listener);

    let mut config = make_config();
    config.server_url =
        Url::parse(&format!("ws://{addr}/signaling/ws")).expect("test URL should parse");
    config.connection_timeout = 1;
    config.reconnect_config = ReconnectConfig {
        enabled: true,
        max_attempts: 10,
        initial_delay: 30,
        max_delay: 30,
        backoff_multiplier: 1.0,
    };
    let client = make_ws_client(config);
    let mut rx = client.subscribe_events();

    let long_connect_task = {
        let client = client.clone();
        tokio::spawn(async move { client.connect().await })
    };

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match rx.recv().await {
                Ok(SignalingEvent::Disconnected {
                    reason: DisconnectReason::ConnectionFailed(_),
                }) => break,
                Ok(_) => continue,
                Err(e) => panic!("unexpected signaling event receive error: {e}"),
            }
        }
    })
    .await
    .expect("long connect should fail first attempt and enter backoff");
    assert!(
        !client.connecting.load(Ordering::Acquire),
        "connect() must release connecting while sleeping in backoff"
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("network restore should make the signaling endpoint reachable");
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("restored test server should accept tcp connection");
        let ws_stream = tokio_tungstenite::accept_async(stream)
            .await
            .expect("restored test server should complete websocket handshake");
        tokio::time::sleep(Duration::from_millis(250)).await;
        drop(ws_stream);
    });

    let restore_result = tokio::time::timeout(
        Duration::from_secs(CONCURRENT_CONNECT_WAIT_TIMEOUT_SECS + 2),
        {
            let client = client.clone();
            async move { client.connect_once().await }
        },
    )
    .await
    .expect("restore connect_once should complete within the concurrent wait window");

    long_connect_task.abort();
    server_task.abort();
    client.disconnect().await.ok();

    assert!(
        restore_result.is_ok(),
        "network restore should not be blocked by an older connect() backoff; got {restore_result:?}"
    );
}

#[tokio::test]
async fn explicit_connect_backoff_reset_restarts_attempt_sequence() {
    let reserved_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should reserve a local port");
    let addr = reserved_listener
        .local_addr()
        .expect("reserved listener should have local addr");
    drop(reserved_listener);

    let mut config = make_config();
    config.server_url =
        Url::parse(&format!("ws://{addr}/signaling/ws")).expect("test URL should parse");
    config.connection_timeout = 1;
    config.reconnect_config = ReconnectConfig {
        enabled: true,
        max_attempts: 10,
        initial_delay: 30,
        max_delay: 30,
        backoff_multiplier: 1.0,
    };
    let client = make_ws_client(config);

    let (attempt_tx, mut attempt_rx) = tokio::sync::mpsc::unbounded_channel();
    let hook_callback: HookCallback = Arc::new(move |event| {
        let attempt_tx = attempt_tx.clone();
        Box::pin(async move {
            if let HookEvent::SignalingConnectStart { attempt } = event {
                let _ = attempt_tx.send(attempt);
            }
        })
    });
    client.set_hook_callback(hook_callback);

    let connect_task = {
        let client = client.clone();
        tokio::spawn(async move { client.connect().await })
    };

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), attempt_rx.recv())
            .await
            .expect("connect should publish attempt 1"),
        Some(1)
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), attempt_rx.recv())
            .await
            .expect("connect should enter first backoff as attempt 2"),
        Some(2)
    );

    client.schedule_auto_reconnect_reset_backoff();

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), attempt_rx.recv())
            .await
            .expect("reset should restart explicit connect attempts"),
        Some(1),
        "network recovery reset should restart explicit connect() backoff from attempt 1"
    );

    connect_task.abort();
    client.disconnect().await.ok();
}

#[tokio::test]
async fn test_publish_disconnected_transition_fires_hook_once() {
    let stats = Arc::new(AtomicSignalingStats::default());
    let (event_tx, mut event_rx) = broadcast::channel(4);
    let hook_count = Arc::new(AtomicUsize::new(0));
    let hook_count_for_cb = hook_count.clone();
    let hook_callback: HookCallback = Arc::new(move |event| {
        let hook_count = hook_count_for_cb.clone();
        Box::pin(async move {
            if matches!(event, HookEvent::SignalingDisconnected) {
                hook_count.fetch_add(1, UsizeOrdering::SeqCst);
            }
        }) as Pin<Box<dyn Future<Output = ()> + Send>>
    });

    let first = WebSocketSignalingClient::publish_disconnected_transition(
        true,
        &stats,
        &event_tx,
        Some(hook_callback.clone()),
        DisconnectReason::StreamEnded,
        None,
    )
    .await;
    assert!(
        first,
        "first connected->disconnected transition should publish"
    );
    assert_eq!(hook_count.load(UsizeOrdering::SeqCst), 1);
    assert_eq!(stats.snapshot().disconnections, 1);
    assert!(matches!(
        event_rx.recv().await,
        Ok(SignalingEvent::Disconnected {
            reason: DisconnectReason::StreamEnded
        })
    ));

    let second = WebSocketSignalingClient::publish_disconnected_transition(
        false,
        &stats,
        &event_tx,
        Some(hook_callback),
        DisconnectReason::PongTimeout,
        None,
    )
    .await;
    assert!(
        !second,
        "stale duplicate disconnected transition should be ignored"
    );
    assert_eq!(hook_count.load(UsizeOrdering::SeqCst), 1);
    assert_eq!(stats.snapshot().disconnections, 1);
    assert!(event_rx.try_recv().is_err());
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 1. Configuration defaults
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[test]
fn test_reconnect_config_defaults() {
    let cfg = ReconnectConfig::default();
    assert!(cfg.enabled);
    assert_eq!(cfg.max_attempts, 10);
    assert_eq!(cfg.initial_delay, 1);
    assert_eq!(cfg.max_delay, 60);
    assert!((cfg.backoff_multiplier - 2.0).abs() < f64::EPSILON);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 2. Initial state
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[test]
fn test_websocket_signaling_client_initial_state_disconnected() {
    let client = WebSocketSignalingClient::new(make_config());
    assert!(
        !client.is_connected(),
        "newly created client should be Disconnected"
    );
    assert!(
        !client.connecting.load(Ordering::Acquire),
        "newly created client should not be in connecting state"
    );
    assert!(
        !client.reconnector_started.load(Ordering::Acquire),
        "reconnect manager should not be started automatically"
    );
}

#[test]
fn test_initial_stats_are_zero() {
    let client = WebSocketSignalingClient::new(make_config());
    let stats = client.get_stats();
    assert_eq!(stats.connections, 0);
    assert_eq!(stats.disconnections, 0);
    assert_eq!(stats.messages_sent, 0);
    assert_eq!(stats.messages_received, 0);
    assert_eq!(stats.errors, 0);
}

#[test]
fn test_signaling_url_log_redacts_credential_query_params() {
    let url = Url::parse(
            "wss://example.com/signaling?actor_id=abc&key_id=7&claims=claims-value&signature=signature-value&token=token-value",
        )
        .unwrap();

    let redacted = WebSocketSignalingClient::redact_signaling_url_for_log(&url);

    assert!(redacted.contains("actor_id=abc"));
    assert!(redacted.contains("key_id=7"));
    assert!(redacted.contains("claims=REDACTED"));
    assert!(redacted.contains("signature=REDACTED"));
    assert!(redacted.contains("token=REDACTED"));
    assert!(!redacted.contains("claims-value"));
    assert!(!redacted.contains("signature-value"));
    assert!(!redacted.contains("token-value"));
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 3. Reconnect manager idempotency
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_reconnect_manager_idempotent() {
    let client = make_ws_client(make_config());

    // First start should succeed
    client.start_reconnect_manager();
    assert!(
        client.reconnector_started.load(Ordering::Acquire),
        "reconnector_started should be true after first call"
    );

    // Second call should not start a new manager (CAS fails)
    client.start_reconnect_manager();
    // Multiple managers would cause flaky tests due to duplicate reconnections; mainly verify the flag
    assert!(client.reconnector_started.load(Ordering::Acquire));
}

#[tokio::test]
async fn test_reconnect_manager_disabled_when_config_disabled() {
    let mut config = make_config();
    config.reconnect_config.enabled = false;
    let client = make_ws_client(config);

    client.start_reconnect_manager();
    assert!(
        !client.reconnector_started.load(Ordering::Acquire),
        "reconnect manager should not start when reconnect config is disabled"
    );
}

#[tokio::test]
async fn test_reconnect_manager_does_not_keep_client_alive() {
    let client = make_ws_client(make_config());
    let weak = Arc::downgrade(&client);

    client.start_reconnect_manager();
    drop(client);

    assert!(
        weak.upgrade().is_none(),
        "reconnect manager must not keep signaling client alive after owner drop"
    );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 4. connect() concurrency exclusion
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_connect_fast_path_when_already_connected() {
    let client = make_ws_client(make_config());
    // Manually set as connected
    client.connected.store(true, Ordering::Release);

    // connect() should return Ok immediately without establishing a new connection
    let result = client.connect().await;
    assert!(
        result.is_ok(),
        "connect() should return Ok when already connected"
    );
    // Should not change connecting flag
    assert!(!client.connecting.load(Ordering::Acquire));
}

#[tokio::test]
async fn test_connect_sets_connecting_flag() {
    let mut config = make_config();
    config.reconnect_config.enabled = false; // disable retry, fail fast
    config.connection_timeout = 1;
    let client = make_ws_client(config);

    // Connection will fail (unreachable address), but should properly clean up connecting flag
    let result = client.connect().await;
    assert!(
        result.is_err(),
        "connecting to unreachable address should fail"
    );
    assert!(
        !client.connecting.load(Ordering::Acquire),
        "connecting flag should be cleared after connection failure"
    );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 5. Event broadcast
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_event_subscribe_receives_events() {
    let client = make_ws_client(make_config());
    let mut rx = client.subscribe_events();

    // Manually send event
    let _ = client.event_tx.send(SignalingEvent::Connected);

    match tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await {
        Ok(Ok(SignalingEvent::Connected)) => {} // expect Connected event
        other => panic!("expected Connected event, but got {:?}", other),
    }
}

#[tokio::test]
async fn test_disconnect_event_on_connect_failure() {
    let mut config = make_config();
    config.reconnect_config.enabled = false;
    config.connection_timeout = 1;
    let client = make_ws_client(config);
    let mut rx = client.subscribe_events();

    // Connection fails
    let _ = client.connect().await;

    // Should receive Disconnected(ConnectionFailed) event
    match tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await {
        Ok(Ok(SignalingEvent::Disconnected {
            reason: DisconnectReason::ConnectionFailed(_),
        })) => {} // expected
        other => panic!(
            "expected Disconnected(ConnectionFailed) event, but got {:?}",
            other
        ),
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 6. disconnect() state cleanup
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_disconnect_clears_connected_flag() {
    let client = make_ws_client(make_config());
    // Simulate connected state
    client.connected.store(true, Ordering::Release);
    assert!(client.is_connected());

    let result = client.disconnect().await;
    assert!(result.is_ok());
    assert!(
        !client.is_connected(),
        "should be Disconnected after disconnect()"
    );
}

#[tokio::test]
async fn test_disconnect_increments_disconnection_stat() {
    let client = make_ws_client(make_config());
    client.connected.store(true, Ordering::Release);

    let stats_before = client.get_stats().disconnections;
    let _ = client.disconnect().await;
    let stats_after = client.get_stats().disconnections;
    assert_eq!(
        stats_after,
        stats_before + 1,
        "disconnect() should increment disconnection count"
    );
}

#[tokio::test]
async fn test_disconnect_idempotent() {
    let client = make_ws_client(make_config());

    // Calling disconnect() while not connected should not panic
    let r1 = client.disconnect().await;
    let r2 = client.disconnect().await;
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert!(!client.is_connected());
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 7. Reconnect notify mechanism
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_reconnect_notify_wakes_waiter() {
    let notify = Arc::new(tokio::sync::Notify::new());
    let notify_clone = notify.clone();
    let woken = Arc::new(AtomicBool::new(false));
    let woken_clone = woken.clone();

    let handle = tokio::spawn(async move {
        notify_clone.notified().await;
        woken_clone.store(true, Ordering::Release);
    });

    // Ensure waiter has registered
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !woken.load(Ordering::Acquire),
        "should not be woken before notification"
    );

    // Trigger notification
    notify.notify_one();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        woken.load(Ordering::Acquire),
        "should be woken after notification"
    );

    handle.abort();
}

#[tokio::test]
async fn test_schedule_auto_reconnect_reenables_after_explicit_disconnect() {
    let client = make_ws_client(make_config());

    client
        .disconnect()
        .await
        .expect("explicit disconnect should be idempotent");
    assert!(
        client.auto_reconnect_suppressed.load(Ordering::Acquire),
        "explicit disconnect should suppress stale auto-reconnect cycles"
    );

    client.schedule_auto_reconnect();

    assert!(
        !client.auto_reconnect_suppressed.load(Ordering::Acquire),
        "scheduling a fresh auto-reconnect should clear explicit disconnect suppression"
    );
}

#[test]
fn test_suppress_auto_reconnect_invalidates_generation_until_rescheduled() {
    let client = make_ws_client(make_config());
    let generation = client.reconnect_generation.load(Ordering::Acquire);

    client.suppress_auto_reconnect();

    assert!(
        client.auto_reconnect_suppressed.load(Ordering::Acquire),
        "lifecycle preparation should pause automatic reconnect"
    );
    assert_eq!(
        client.reconnect_generation.load(Ordering::Acquire),
        generation + 1,
        "lifecycle preparation should invalidate the current reconnect generation"
    );

    client.schedule_auto_reconnect_reset_backoff();
    assert!(
        !client.auto_reconnect_suppressed.load(Ordering::Acquire),
        "network recovery should re-enable future automatic reconnects"
    );
}

#[tokio::test]
async fn test_schedule_auto_reconnect_reset_backoff_restarts_attempt_sequence() {
    let mut config = make_config();
    config.connection_timeout = 1;
    config.reconnect_config = ReconnectConfig {
        enabled: true,
        max_attempts: 5,
        initial_delay: 30,
        max_delay: 30,
        backoff_multiplier: 1.0,
    };
    let client = make_ws_client(config);
    let mut rx = client.subscribe_events();

    let reconnect_client = client.clone();
    let reconnect_task = tokio::spawn(async move {
        reconnect_client.run_reconnect_cycle().await;
    });

    match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
        Ok(Ok(SignalingEvent::ConnectStart { attempt: 1 })) => {}
        other => panic!("expected first reconnect attempt, got {other:?}"),
    }

    match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
        Ok(Ok(SignalingEvent::Disconnected {
            reason: DisconnectReason::ConnectionFailed(_),
        })) => {}
        other => panic!("expected first reconnect failure, got {other:?}"),
    }

    client.schedule_auto_reconnect_reset_backoff();

    match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
        Ok(Ok(SignalingEvent::ConnectStart { attempt: 1 })) => {}
        other => panic!("expected reset reconnect attempt to restart at 1, got {other:?}"),
    }

    client
        .disconnect()
        .await
        .expect("explicit disconnect should stop reconnect cycle");
    tokio::time::timeout(Duration::from_secs(2), reconnect_task)
        .await
        .expect("reconnect cycle should stop after explicit disconnect")
        .expect("reconnect task should not panic");
}

#[tokio::test]
async fn test_explicit_disconnect_suppresses_reconnect_cycle_in_backoff() {
    let mut config = make_config();
    config.connection_timeout = 1;
    config.reconnect_config = ReconnectConfig {
        enabled: true,
        max_attempts: 5,
        initial_delay: 1,
        max_delay: 1,
        backoff_multiplier: 1.0,
    };
    let client = make_ws_client(config);
    let mut rx = client.subscribe_events();

    let reconnect_client = client.clone();
    let reconnect_task = tokio::spawn(async move {
        reconnect_client.run_reconnect_cycle().await;
    });

    match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
        Ok(Ok(SignalingEvent::ConnectStart { attempt: 1 })) => {}
        other => panic!("expected first reconnect attempt, got {other:?}"),
    }

    client
        .disconnect()
        .await
        .expect("explicit disconnect should be idempotent");

    tokio::time::timeout(Duration::from_secs(2), reconnect_task)
        .await
        .expect("suppressed reconnect cycle should exit promptly")
        .expect("reconnect task should not panic");

    while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
        if let SignalingEvent::ConnectStart { attempt } = event {
            panic!("suppressed reconnect cycle sent unexpected attempt {attempt}");
        }
    }

    assert!(
        client.auto_reconnect_suppressed.load(Ordering::Acquire),
        "explicit disconnect should suppress stale auto-reconnect cycles"
    );
}

#[tokio::test]
async fn test_explicit_disconnect_suppresses_in_flight_auto_reconnect_connected_event() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let server_url = format!(
        "ws://{}/signaling/ws",
        listener
            .local_addr()
            .expect("test listener should have local addr")
    );
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("test server should accept tcp connection");
        let _ = release_rx.await;
        let ws_stream = tokio_tungstenite::accept_async(stream)
            .await
            .expect("test server should complete websocket handshake");
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(ws_stream);
    });

    let mut config = make_config();
    config.server_url = Url::parse(&server_url).expect("test websocket URL should parse");
    config.connection_timeout = 5;
    config.reconnect_config = ReconnectConfig {
        enabled: true,
        max_attempts: 3,
        initial_delay: 1,
        max_delay: 1,
        backoff_multiplier: 1.0,
    };
    let client = make_ws_client(config);
    let mut rx = client.subscribe_events();

    let reconnect_client = client.clone();
    let reconnect_task = tokio::spawn(async move {
        reconnect_client.run_reconnect_cycle().await;
    });

    match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
        Ok(Ok(SignalingEvent::ConnectStart { attempt: 1 })) => {}
        other => panic!("expected first reconnect attempt, got {other:?}"),
    }

    client
        .disconnect()
        .await
        .expect("explicit disconnect should cancel the in-flight auto-reconnect");
    release_tx
        .send(())
        .expect("test server handshake should still be waiting");

    tokio::time::timeout(Duration::from_secs(2), reconnect_task)
        .await
        .expect("cancelled in-flight reconnect should exit promptly")
        .expect("reconnect task should not panic");

    while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await {
        assert!(
            !matches!(event, SignalingEvent::Connected),
            "cancelled auto-reconnect must not publish Connected"
        );
    }

    assert!(
        !client.is_connected(),
        "cancelled auto-reconnect must not leave signaling connected"
    );
    assert!(
        client.auto_reconnect_suppressed.load(Ordering::Acquire),
        "cancelled auto-reconnect must not clear lifecycle suppression"
    );

    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("test server task should finish")
        .expect("test server task should not panic");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 8. URL construction tests
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_build_url_without_identity() {
    let config = make_config();
    let expected_base = config.server_url.to_string();
    let client = WebSocketSignalingClient::new(config);

    let url = client.build_url_with_identity().await;
    assert_eq!(
        url.to_string(),
        expected_base,
        "URL should not contain identity parameters when actor_id is not set"
    );
}

#[tokio::test]
async fn test_build_url_with_webrtc_role() {
    let mut config = make_config();
    config.webrtc_role = Some("answer".to_string());
    let client = WebSocketSignalingClient::new(config);

    let url = client.build_url_with_identity().await;
    assert!(
        url.query().unwrap_or("").contains("webrtc_role=answer"),
        "URL should contain webrtc_role parameter, actual URL: {}",
        url
    );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 9. Inbound channel reset
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_reset_inbound_channel_creates_fresh_channel() {
    let client = WebSocketSignalingClient::new(make_config());

    // Get old tx and send a message
    {
        let tx = client.inbound_tx.lock().await;
        let _ = tx.send(SignalingEnvelope::default());
    }

    // Reset channel
    client.reset_inbound_channel().await;

    // Old messages should not be visible in the new channel
    let mut rx = client.inbound_rx.lock().await;
    let result = rx.try_recv();
    assert!(
        result.is_err(),
        "old messages should not be visible in the new channel after reset"
    );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 10. Envelope ID incrementing
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_envelope_id_monotonically_increasing() {
    let client = WebSocketSignalingClient::new(make_config());

    let id1 = client.next_envelope_id().await;
    let id2 = client.next_envelope_id().await;
    let id3 = client.next_envelope_id().await;

    assert_eq!(id1, "env-1");
    assert_eq!(id2, "env-2");
    assert_eq!(id3, "env-3");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 11. send_envelope should return error when not connected
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_send_envelope_fails_when_not_connected() {
    let client = WebSocketSignalingClient::new(make_config());
    let envelope = SignalingEnvelope::default();

    let result = client.send_envelope(envelope).await;
    assert!(
        result.is_err(),
        "send_envelope should return error when not connected"
    );
    match result {
        Err(NetworkError::ConnectionError(msg)) => {
            assert!(
                msg.contains("not connected") || msg.contains("Not connected"),
                "error message should contain 'not connected', actual: {}",
                msg
            );
        }
        other => panic!("expected ConnectionError, got {:?}", other),
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 12. FakeSignalingClient trait implementation verification
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::test]
async fn test_fake_client_tracks_connect_calls() {
    let client = make_fake_client();
    assert_eq!(client.connect_calls.load(UsizeOrdering::SeqCst), 0);

    client.connect().await.unwrap();
    client.connect().await.unwrap();
    client.connect().await.unwrap();

    assert_eq!(
        client.connect_calls.load(UsizeOrdering::SeqCst),
        3,
        "FakeSignalingClient should accurately track connect call count"
    );
}
