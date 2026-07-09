use super::*;
use crate::transport::lane::DataLane;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::time::{Duration, timeout};

#[derive(Debug)]
struct FakeLane {
    send_count: Arc<AtomicUsize>,
    send_closed: bool,
}

#[async_trait]
impl DataLane for FakeLane {
    async fn send(&self, _data: bytes::Bytes) -> NetworkResult<()> {
        self.send_count.fetch_add(1, Ordering::Relaxed);
        if self.send_closed {
            return Err(NetworkError::DataChannelClosed(
                "test send closed".to_string(),
            ));
        }
        Ok(())
    }

    fn lane_type(&self) -> &'static str {
        "fake"
    }
}

#[derive(Debug)]
struct FakeWire {
    conn_type: ConnType,
    connect_fails: bool,
    lane_closed: bool,
    lane_send_closed: bool,
    lane_non_established: bool,
    retry_get_lane_closed_after_invalidate: bool,
    connected: AtomicBool,
    invalidated: AtomicBool,
    send_count: Arc<AtomicUsize>,
    identity: Option<WireIdentity>,
}

impl FakeWire {
    fn new(conn_type: ConnType) -> Self {
        Self {
            conn_type,
            connect_fails: false,
            lane_closed: false,
            lane_send_closed: false,
            lane_non_established: false,
            retry_get_lane_closed_after_invalidate: false,
            connected: AtomicBool::new(false),
            invalidated: AtomicBool::new(false),
            send_count: Arc::new(AtomicUsize::new(0)),
            identity: None,
        }
    }

    fn connect_fails(mut self) -> Self {
        self.connect_fails = true;
        self
    }

    fn lane_closed(mut self) -> Self {
        self.lane_closed = true;
        self
    }

    fn lane_send_closed(mut self) -> Self {
        self.lane_send_closed = true;
        self
    }

    fn lane_non_established(mut self) -> Self {
        self.lane_non_established = true;
        self
    }

    fn retry_get_lane_closed_after_invalidate(mut self) -> Self {
        self.retry_get_lane_closed_after_invalidate = true;
        self
    }

    fn with_identity(mut self, identity: WireIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    fn with_send_count(mut self, send_count: Arc<AtomicUsize>) -> Self {
        self.send_count = send_count;
        self
    }
}

#[async_trait]
impl WireHandle for FakeWire {
    fn connection_type(&self) -> ConnType {
        self.conn_type
    }

    fn priority(&self) -> u8 {
        match self.conn_type {
            ConnType::WebSocket => 0,
            ConnType::WebRTC => 1,
        }
    }

    async fn connect(&self) -> NetworkResult<()> {
        if self.connect_fails {
            return Err(NetworkError::ConnectionError("connect failed".into()));
        }
        self.connected.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    async fn close(&self) -> NetworkResult<()> {
        self.connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    async fn get_lane(&self, _payload_type: PayloadType) -> NetworkResult<Arc<dyn DataLane>> {
        if self.retry_get_lane_closed_after_invalidate && self.invalidated.load(Ordering::Relaxed) {
            return Err(NetworkError::PeerConnectionClosed(
                "test peer connection closed".to_string(),
            ));
        }
        if self.lane_closed {
            return Err(NetworkError::DataChannelClosed(
                "test lane closed".to_string(),
            ));
        }
        if self.lane_non_established {
            return Err(NetworkError::DataChannelNotOpen(
                "non-Established".to_string(),
            ));
        }
        Ok(Arc::new(FakeLane {
            send_count: Arc::clone(&self.send_count),
            send_closed: self.lane_send_closed,
        }))
    }

    async fn invalidate_lane(&self, _payload_type: PayloadType) {
        self.invalidated.store(true, Ordering::Relaxed);
    }

    fn identity(&self) -> Option<WireIdentity> {
        self.identity.clone()
    }
}

async fn test_transport(connections: Vec<Arc<dyn WireHandle>>) -> DestTransport {
    let conn_mgr = Arc::new(WirePool::new(RetryConfig {
        max_attempts: 1,
        initial_delay_ms: 0,
        max_delay_ms: 0,
        multiplier: 1.0,
    }));
    for conn in connections {
        conn_mgr.add_connection(conn).await;
    }
    DestTransport { conn_mgr }
}

#[tokio::test]
async fn retry_get_lane_closed_after_send_closed_falls_back_to_websocket() {
    let peer_id = actr_protocol::ActrId::default();
    let websocket_sends = Arc::new(AtomicUsize::new(0));
    let transport = test_transport(vec![
        Arc::new(
            FakeWire::new(ConnType::WebRTC)
                .lane_send_closed()
                .retry_get_lane_closed_after_invalidate()
                .with_identity(WireIdentity::WebRtc {
                    peer_id,
                    session_id: 9,
                }),
        ),
        Arc::new(FakeWire::new(ConnType::WebSocket).with_send_count(Arc::clone(&websocket_sends))),
    ])
    .await;

    let identity = timeout(
        Duration::from_secs(1),
        transport.send_with_identity(PayloadType::RpcReliable, b"payload"),
    )
    .await
    .expect("send should not hang")
    .expect("closed WebRTC retry should be evicted and WebSocket fallback should send");

    assert!(identity.is_none());
    assert_eq!(websocket_sends.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn send_returns_when_only_webrtc_candidate_exhausted() {
    let transport = test_transport(vec![Arc::new(
        FakeWire::new(ConnType::WebRTC).connect_fails(),
    )])
    .await;

    let err = timeout(
        Duration::from_secs(1),
        transport.send_with_identity(PayloadType::StreamReliable, b"payload"),
    )
    .await
    .expect("send should not hang")
    .expect_err("send should fail after WebRTC candidate is exhausted");

    assert!(
        matches!(err, NetworkError::NoRoute(ref msg) if msg.contains("all transport candidates exhausted")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn send_falls_back_to_websocket_when_webrtc_fails() {
    let websocket_sends = Arc::new(AtomicUsize::new(0));
    let transport = test_transport(vec![
        Arc::new(FakeWire::new(ConnType::WebRTC).connect_fails()),
        Arc::new(FakeWire::new(ConnType::WebSocket).with_send_count(Arc::clone(&websocket_sends))),
    ])
    .await;

    timeout(
        Duration::from_secs(1),
        transport.send_with_identity(PayloadType::StreamReliable, b"payload"),
    )
    .await
    .expect("send should not hang")
    .expect("send should use WebSocket fallback");

    assert_eq!(websocket_sends.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn stale_webrtc_ready_candidate_does_not_wait_forever() {
    let peer_id = actr_protocol::ActrId::default();
    let transport = test_transport(vec![Arc::new(
        FakeWire::new(ConnType::WebRTC)
            .lane_closed()
            .with_identity(WireIdentity::WebRtc {
                peer_id,
                session_id: 7,
            }),
    )])
    .await;

    let err = timeout(
        Duration::from_secs(1),
        transport.send_with_identity(PayloadType::StreamReliable, b"payload"),
    )
    .await
    .expect("send should not hang")
    .expect_err("stale WebRTC-only candidate should fail clearly");

    assert!(
        matches!(err, NetworkError::NoRoute(ref msg) if msg.contains("all transport candidates exhausted")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn non_established_webrtc_candidate_is_evicted() {
    let peer_id = actr_protocol::ActrId::default();
    let transport = test_transport(vec![Arc::new(
        FakeWire::new(ConnType::WebRTC)
            .lane_non_established()
            .with_identity(WireIdentity::WebRtc {
                peer_id,
                session_id: 8,
            }),
    )])
    .await;

    let err = timeout(
        Duration::from_secs(1),
        transport.send_with_identity(PayloadType::RpcReliable, b"response"),
    )
    .await
    .expect("send should not hang")
    .expect_err("non-established WebRTC candidate should be evicted");

    assert!(
        matches!(err, NetworkError::NoRoute(ref msg) if msg.contains("all transport candidates exhausted")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn generic_error_with_closed_text_does_not_trigger_stale_self_heal() {
    #[derive(Debug)]
    struct GenericClosedTextWire {
        connected: AtomicBool,
    }

    #[async_trait]
    impl WireHandle for GenericClosedTextWire {
        fn connection_type(&self) -> ConnType {
            ConnType::WebRTC
        }

        fn priority(&self) -> u8 {
            1
        }

        async fn connect(&self) -> NetworkResult<()> {
            self.connected.store(true, Ordering::Relaxed);
            Ok(())
        }

        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::Relaxed)
        }

        async fn close(&self) -> NetworkResult<()> {
            self.connected.store(false, Ordering::Relaxed);
            Ok(())
        }

        async fn get_lane(&self, _payload_type: PayloadType) -> NetworkResult<Arc<dyn DataLane>> {
            Err(NetworkError::WebRtcError(
                "validation failed: closed is only payload text".to_string(),
            ))
        }
    }

    let transport = test_transport(vec![Arc::new(GenericClosedTextWire {
        connected: AtomicBool::new(false),
    })])
    .await;

    let result = timeout(
        Duration::from_millis(200),
        transport.send_with_identity(PayloadType::RpcReliable, b"payload"),
    )
    .await;

    assert!(
        result.is_err(),
        "generic error text containing 'closed' should not mark the candidate failed"
    );
}

#[tokio::test]
async fn has_healthy_connection_false_when_no_connections() {
    // Empty DestTransport has no ready connections → not healthy.
    let t = DestTransport::new(Dest::Host, vec![]).await.unwrap();
    assert!(!t.has_healthy_connection().await);
    // watch_ready returns a live receiver (empty initially).
    let rx = t.watch_ready();
    assert!(rx.borrow().is_empty());
}
