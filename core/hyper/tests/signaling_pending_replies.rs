//! Regression tests for signaling reply waiters during connection rebuilds.

use actr_hyper::test_support::{dummy_credential, make_actor_id};
use actr_hyper::wire::webrtc::{
    ReconnectConfig, SignalingClient, SignalingConfig, WebSocketSignalingClient,
};
use actr_protocol::{ActrType, RouteCandidatesRequest, route_candidates_request};
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

type SilentServer = (String, oneshot::Receiver<()>, tokio::task::JoinHandle<()>);

async fn start_silent_signaling_server() -> SilentServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind silent signaling server");
    let url = format!("ws://{}", listener.local_addr().expect("local addr"));
    let (first_message_tx, first_message_rx) = oneshot::channel();

    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept websocket");
        let mut ws = accept_async(stream)
            .await
            .expect("accept websocket handshake");
        let mut first_message_tx = Some(first_message_tx);

        while let Some(message) = ws.next().await {
            match message {
                Ok(Message::Binary(_)) => {
                    if let Some(tx) = first_message_tx.take() {
                        let _ = tx.send(());
                    }
                    // Intentionally do not reply. The test simulates a request
                    // that is still pending when the client rebuilds signaling.
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    (url, first_message_rx, handle)
}

fn test_signaling_config(url: &str) -> SignalingConfig {
    SignalingConfig {
        server_url: Url::parse(url).expect("parse signaling url"),
        connection_timeout: 5,
        heartbeat_interval: 30,
        reconnect_config: ReconnectConfig {
            enabled: false,
            ..ReconnectConfig::default()
        },
        auth_config: None,
        webrtc_role: None,
    }
}

fn route_candidates_request() -> RouteCandidatesRequest {
    RouteCandidatesRequest {
        target_type: ActrType {
            manufacturer: "acme".to_string(),
            name: "StreamEchoServer".to_string(),
            version: "1.0.0".to_string(),
        },
        criteria: Some(route_candidates_request::NodeSelectionCriteria {
            candidate_count: 1,
            ranking_factors: Vec::new(),
            minimal_dependency_requirement: None,
            minimal_health_requirement: None,
        }),
        client_location: None,
        client_fingerprint: "service_semantic:test".to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_route_candidates_waiter_uses_short_response_timeout() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init()
        .ok();

    let (server_url, first_message_rx, server_task) = start_silent_signaling_server().await;
    let client = Arc::new(WebSocketSignalingClient::new(test_signaling_config(
        &server_url,
    )));
    client
        .connect_once()
        .await
        .expect("connect signaling client");

    let started = Instant::now();
    let request_task = {
        let client = client.clone();
        let actor_id = make_actor_id(43);
        let credential = dummy_credential();
        let request = route_candidates_request();

        tokio::spawn(async move {
            client
                .send_route_candidates_request(actor_id, credential, request)
                .await
        })
    };

    first_message_rx
        .await
        .expect("server should receive route candidates request");

    let result = tokio::time::timeout(Duration::from_secs(7), request_task)
        .await
        .expect("pending route candidates request should use the short response timeout")
        .expect("request task should not panic");
    let elapsed = started.elapsed();

    let err = result.expect_err("request should fail when signaling server stays silent");
    assert!(
        err.to_string()
            .contains("Timed out waiting for signaling response"),
        "unexpected error: {err}"
    );
    assert!(
        elapsed >= Duration::from_secs(4),
        "request should wait for its response budget; elapsed={elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(7),
        "request/response timeout must not use the old 15s window; elapsed={elapsed:?}"
    );

    let _ = client.disconnect().await;
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_route_candidates_waiter_fails_promptly_on_disconnect() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init()
        .ok();

    let (server_url, first_message_rx, server_task) = start_silent_signaling_server().await;
    let client = Arc::new(WebSocketSignalingClient::new(test_signaling_config(
        &server_url,
    )));
    client
        .connect_once()
        .await
        .expect("connect signaling client");

    let request_task = {
        let client = client.clone();
        let actor_id = make_actor_id(42);
        let credential = dummy_credential();
        let request = route_candidates_request();

        tokio::spawn(async move {
            client
                .send_route_candidates_request(actor_id, credential, request)
                .await
        })
    };

    first_message_rx
        .await
        .expect("server should receive route candidates request");

    client
        .disconnect()
        .await
        .expect("disconnect should be best effort");

    let result = tokio::time::timeout(Duration::from_millis(500), request_task)
        .await
        .expect("pending route candidates request should fail promptly after disconnect")
        .expect("request task should not panic");

    let err = result.expect_err("request should fail after disconnect drops pending waiter");
    assert!(
        err.to_string()
            .contains("Receiver dropped while waiting for signaling response"),
        "unexpected error: {err}"
    );

    server_task.abort();
}
