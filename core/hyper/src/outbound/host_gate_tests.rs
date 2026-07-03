use super::*;
use crate::transport::HostTransport;

fn gate() -> HostGate {
    HostGate::new(Arc::new(HostTransport::new()))
}

fn envelope(rid: &str) -> RpcEnvelope {
    RpcEnvelope {
        request_id: rid.to_string(),
        route_key: "echo".into(),
        payload: Some(Bytes::from_static(b"hi")),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_request_with_type_reliable_times_out_without_response() {
    let gate = gate();
    let mut env = envelope("req-1");
    env.timeout_ms = 10;
    let err = gate
        .send_request_with_type(&ActrId::default(), PayloadType::RpcReliable, None, env)
        .await
        .unwrap_err();
    assert!(matches!(err, ActrError::TimedOut), "got {err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_request_with_type_completes_when_responded() {
    let _gate = gate();
    // Share the underlying transport by constructing the gate from a
    // shared Arc, then complete the pending request from another task.
    let transport = Arc::new(HostTransport::new());
    let gate = HostGate::new(transport.clone());

    let t2 = transport.clone();
    let handle = tokio::spawn(async move {
        let mut env = envelope("req-ok");
        env.timeout_ms = 5000;
        gate.send_request_with_type(&ActrId::default(), PayloadType::RpcReliable, None, env)
            .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    t2.complete_response("req-ok", Bytes::from_static(b"resp"))
        .await
        .unwrap();
    let resp = handle.await.unwrap().unwrap();
    assert_eq!(resp, Bytes::from_static(b"resp"));
}

#[tokio::test]
async fn send_message_with_type_reliable_succeeds() {
    let gate = gate();
    gate.send_message_with_type(
        &ActrId::default(),
        PayloadType::RpcReliable,
        None,
        envelope("msg-1"),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn send_message_with_type_stamps_request_direction() {
    let transport = Arc::new(HostTransport::new());
    let gate = HostGate::new(transport.clone());
    let lane = transport
        .get_lane(PayloadType::RpcReliable, None)
        .await
        .unwrap();
    let mut env = envelope("msg-direction");
    env.direction = Some(Direction::Response as i32);

    gate.send_message_with_type(&ActrId::default(), PayloadType::RpcReliable, None, env)
        .await
        .unwrap();

    let received = lane.recv_envelope().await.unwrap();
    assert_eq!(received.direction, Some(Direction::Request as i32));
}

#[tokio::test]
async fn send_message_with_type_unknown_stream_channel_errors() {
    let gate = gate();
    let err = gate
        .send_message_with_type(
            &ActrId::default(),
            PayloadType::StreamLatencyFirst,
            Some("nope".into()),
            envelope("msg-2"),
        )
        .await
        .unwrap_err();
    // HostTransport returns ChannelNotFound → mapped to Unavailable.
    assert!(matches!(err, ActrError::Unavailable(_)));
}

#[tokio::test]
async fn send_data_stream_unknown_channel_errors() {
    let gate = gate();
    let err = gate
        .send_data_stream(
            &ActrId::default(),
            PayloadType::StreamLatencyFirst,
            "missing-stream",
            Bytes::from_static(b"data"),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ActrError::Unavailable(_)));
}

#[tokio::test]
async fn send_data_stream_rejects_non_stream_payload_type() {
    let gate = gate();
    let err = gate
        .send_data_stream(
            &ActrId::default(),
            PayloadType::RpcReliable,
            "any-stream",
            Bytes::from_static(b"data"),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ActrError::InvalidArgument(_)));
}
