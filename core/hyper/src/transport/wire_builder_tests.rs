use super::*;
use crate::transport::ConnType;
use actr_protocol::{ActrId, Direction, RpcEnvelope};

fn envelope_with_direction(direction: Option<i32>) -> RpcEnvelope {
    RpcEnvelope {
        request_id: "req-direction".to_string(),
        route_key: "pkg.Service.Method".to_string(),
        direction,
        ..Default::default()
    }
}

#[test]
fn client_websocket_response_reader_accepts_only_response_direction() {
    assert!(ClientWebSocketHandle::is_response_envelope(
        &envelope_with_direction(Some(Direction::Response as i32)),
        PayloadType::RpcReliable,
    ));
    assert!(!ClientWebSocketHandle::is_response_envelope(
        &envelope_with_direction(Some(Direction::Request as i32)),
        PayloadType::RpcReliable,
    ));
    assert!(!ClientWebSocketHandle::is_response_envelope(
        &envelope_with_direction(Some(Direction::Unspecified as i32)),
        PayloadType::RpcReliable,
    ));
    assert!(!ClientWebSocketHandle::is_response_envelope(
        &envelope_with_direction(Some(99)),
        PayloadType::RpcReliable,
    ));
    assert!(!ClientWebSocketHandle::is_response_envelope(
        &envelope_with_direction(None),
        PayloadType::RpcReliable,
    ));
}

#[tokio::test]
async fn test_no_ws_connection_without_discovery() {
    // WebSocket URLs come only from service discovery; without a discovery record no WS connection should be created.
    let config = DefaultWireBuilderConfig {
        enable_websocket: true,
        enable_webrtc: false,
        local_id_hex: "deadbeef".to_string(),
        discovered_ws_addresses: Arc::new(RwLock::new(HashMap::new())),
        credential_state: None,
        session_state: None,
        pending_requests: None,
    };
    let factory = DefaultWireBuilder::new(None, config);
    let dest = Dest::actor(ActrId::default());
    let connections = factory.create_connections(&dest).await.unwrap();
    assert!(connections.is_empty());
}

#[tokio::test]
async fn test_ws_connection_from_discovery() {
    // A discovered address should allow a WS connection to be created.
    let map = Arc::new(RwLock::new(HashMap::new()));
    let actor_id = ActrId::default();
    map.write()
        .await
        .insert(actor_id.clone(), "ws://localhost:9001".to_string());

    let config = DefaultWireBuilderConfig {
        enable_websocket: true,
        enable_webrtc: false,
        local_id_hex: "deadbeef".to_string(),
        discovered_ws_addresses: map,
        credential_state: None,
        session_state: None,
        pending_requests: None,
    };
    let factory = DefaultWireBuilder::new(None, config);
    let dest = Dest::actor(actor_id);
    let connections = factory.create_connections(&dest).await.unwrap();
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].connection_type(), ConnType::WebSocket);
}

#[test]
fn default_config_enables_both_transports_with_empty_identity() {
    let cfg = DefaultWireBuilderConfig::default();
    assert!(cfg.enable_webrtc);
    assert!(cfg.enable_websocket);
    assert!(cfg.local_id_hex.is_empty());
    assert!(cfg.credential_state.is_none());
    assert!(cfg.session_state.is_none());
    assert!(cfg.pending_requests.is_none());
}

#[tokio::test]
async fn cancelled_token_aborts_before_any_connection() {
    // Even with a discovered WS URL present, a pre-cancelled token must
    // short-circuit before opening a connection.
    let map = Arc::new(RwLock::new(HashMap::new()));
    let actor_id = ActrId::default();
    map.write()
        .await
        .insert(actor_id.clone(), "ws://localhost:9001".to_string());

    let config = DefaultWireBuilderConfig {
        enable_websocket: true,
        enable_webrtc: true, // also enabled — must still be skipped due to early cancel
        local_id_hex: "deadbeef".to_string(),
        discovered_ws_addresses: map,
        credential_state: None,
        session_state: None,
        pending_requests: None,
    };
    let factory = DefaultWireBuilder::new(None, config);

    let token = CancellationToken::new();
    token.cancel();

    let dest = Dest::actor(actor_id);
    let res = factory
        .create_connections_with_cancel(&dest, Some(token))
        .await;
    assert!(
        matches!(res, Err(NetworkError::ConnectionClosed(_))),
        "cancelled creation should yield ConnectionClosed, got {res:?}"
    );
}

#[tokio::test]
async fn resolve_websocket_url_miss_then_hit_for_actor() {
    let map = Arc::new(RwLock::new(HashMap::new()));
    let id = ActrId::default();
    let factory = DefaultWireBuilder::new(
        None,
        DefaultWireBuilderConfig {
            discovered_ws_addresses: map.clone(),
            ..Default::default()
        },
    );

    // Cache miss → None.
    assert!(
        factory
            .resolve_websocket_url(&Dest::actor(id.clone()))
            .await
            .is_none()
    );

    // Populate discovery map → hit returns the URL.
    map.write()
        .await
        .insert(id.clone(), "ws://host:7".to_string());
    assert_eq!(
        factory
            .resolve_websocket_url(&Dest::actor(id))
            .await
            .as_deref(),
        Some("ws://host:7")
    );
}
