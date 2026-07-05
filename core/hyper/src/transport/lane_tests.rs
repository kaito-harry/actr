use super::*;
use bytes::Bytes;

#[test]
fn test_stream_closed_send_error_wins_over_stale_open_state() {
    let error = webrtc::Error::Data(webrtc::data::Error::Sctp(
        webrtc::sctp::Error::ErrStreamClosed,
    ));

    let classified =
        classify_data_channel_send_error(error, RTCDataChannelState::Open, "Send failed");

    assert!(matches!(classified, NetworkError::DataChannelClosed(_)));
    assert!(classified.is_closed_like());
}

#[test]
fn test_non_established_send_error_wins_over_stale_open_state() {
    let error = webrtc::Error::Data(webrtc::data::Error::Sctp(
        webrtc::sctp::Error::ErrPayloadDataStateNotExist,
    ));

    let classified =
        classify_data_channel_send_error(error, RTCDataChannelState::Open, "Send failed");

    assert!(matches!(classified, NetworkError::DataChannelNotOpen(_)));
    assert!(classified.is_closed_like());
}

#[test]
fn test_unrelated_send_error_remains_data_channel_error_while_open() {
    let classified = classify_data_channel_send_error(
        webrtc::Error::ErrUnknownType,
        RTCDataChannelState::Open,
        "Send failed",
    );

    assert!(matches!(classified, NetworkError::DataChannelError(_)));
    assert!(!classified.is_closed_like());
}

// ── classify_peer_connection_error ─────────────────────────────────────────

#[test]
fn test_closed_peer_connection_error_wins_over_stale_state() {
    // The state read races the failure: webrtc can report the connection
    // closed while the sampled state still shows a transitional value.
    for state in [
        RTCPeerConnectionState::Connecting,
        RTCPeerConnectionState::Disconnected,
    ] {
        let classified = classify_peer_connection_error(webrtc::Error::ErrConnectionClosed, state);

        assert!(matches!(classified, NetworkError::PeerConnectionClosed(_)));
        assert!(classified.is_closed_like());
    }
}

#[test]
fn test_closed_peer_state_wins_over_generic_error() {
    let classified = classify_peer_connection_error(
        webrtc::Error::ErrUnknownType,
        RTCPeerConnectionState::Failed,
    );

    assert!(matches!(classified, NetworkError::PeerConnectionClosed(_)));
    assert!(classified.is_closed_like());
}

#[test]
fn test_unrelated_peer_connection_error_remains_webrtc_error() {
    let classified =
        classify_peer_connection_error(webrtc::Error::ErrUnknownType, RTCPeerConnectionState::New);

    assert!(matches!(classified, NetworkError::WebRtcError(_)));
    assert!(!classified.is_closed_like());
}

// ── Fragment header encode / decode ───────────────────────────────────────

#[test]
fn test_encode_decode_fragment_header_single() {
    let mut buf = Vec::new();
    encode_fragment_header(&mut buf, 42, 0, 1);
    assert_eq!(buf.len(), FRAGMENT_HEADER_SIZE);

    let payload = Bytes::from(b"hello".as_slice().to_vec());
    let mut raw = buf;
    raw.extend_from_slice(&payload);

    let (msg_id, frag_index, total_frags, decoded_payload) =
        decode_fragment_header(Bytes::from(raw)).unwrap();
    assert_eq!(msg_id, 42);
    assert_eq!(frag_index, 0);
    assert_eq!(total_frags, 1);
    assert_eq!(decoded_payload, payload);
}

#[test]
fn test_encode_decode_fragment_header_multi() {
    let mut buf = Vec::new();
    encode_fragment_header(&mut buf, 0xDEAD_BEEF, 3, 7);
    let (msg_id, frag_index, total_frags, _) = decode_fragment_header(Bytes::from(buf)).unwrap();
    assert_eq!(msg_id, 0xDEAD_BEEF);
    assert_eq!(frag_index, 3);
    assert_eq!(total_frags, 7);
}

#[test]
fn test_decode_too_short_returns_error() {
    let short = Bytes::from_static(b"short");
    assert!(decode_fragment_header(short).is_err());
}

// ── ReassemblyBuffer ─────────────────────────────────────────────────────

#[test]
fn test_reassembly_single_fragment() {
    let mut buf = ReassemblyBuffer::new();
    let payload = Bytes::from_static(b"single");
    let result = buf.insert(1, 0, 1, payload.clone());
    assert_eq!(result, Some(payload));
}

#[test]
fn test_reassembly_two_fragments_in_order() {
    let mut buf = ReassemblyBuffer::new();
    let part0 = Bytes::from_static(b"hello ");
    let part1 = Bytes::from_static(b"world");

    assert!(buf.insert(5, 0, 2, part0).is_none());
    let result = buf.insert(5, 1, 2, part1).unwrap();
    assert_eq!(result, Bytes::from_static(b"hello world"));
}

#[test]
fn test_reassembly_two_fragments_out_of_order() {
    let mut buf = ReassemblyBuffer::new();
    let part0 = Bytes::from_static(b"hello ");
    let part1 = Bytes::from_static(b"world");

    assert!(buf.insert(7, 1, 2, part1).is_none());
    let result = buf.insert(7, 0, 2, part0).unwrap();
    assert_eq!(result, Bytes::from_static(b"hello world"));
}

#[test]
fn test_reassembly_multiple_messages_interleaved() {
    let mut buf = ReassemblyBuffer::new();

    assert!(buf.insert(1, 0, 2, Bytes::from_static(b"A1")).is_none());
    assert!(buf.insert(2, 0, 2, Bytes::from_static(b"B1")).is_none());
    assert!(buf.insert(1, 1, 2, Bytes::from_static(b"A2")).is_some());
    let msg2 = buf.insert(2, 1, 2, Bytes::from_static(b"B2")).unwrap();
    assert_eq!(msg2, Bytes::from_static(b"B1B2"));
}

// ── Fragment count calculation ────────────────────────────────────────────

#[test]
fn test_fragment_count_small_message() {
    let size = DC_MAX_PAYLOAD_SIZE;
    let count = size.div_ceil(DC_MAX_PAYLOAD_SIZE);
    assert_eq!(
        count, 1,
        "message equal to payload size should be 1 fragment"
    );
}

#[test]
fn test_fragment_count_one_byte_over() {
    let size = DC_MAX_PAYLOAD_SIZE + 1;
    let count = size.div_ceil(DC_MAX_PAYLOAD_SIZE);
    assert_eq!(count, 2, "one byte over should require 2 fragments");
}

#[test]
fn test_fragment_count_200kb() {
    let size: usize = 200 * 1024; // 200 KB
    let count = size.div_ceil(DC_MAX_PAYLOAD_SIZE);
    // 200*1024 / (65535 - 8) = 204800 / 65527 = 3.126 -> 4 fragments
    assert_eq!(count, 4);
}

// ── Round-trip via mpsc (simulated channel, no real DataChannel) ──────────

/// Helper: build a framed bytes buffer as the DataChannel would produce it.
fn make_frame(msg_id: u32, frag_index: u16, total_frags: u16, payload: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(FRAGMENT_HEADER_SIZE + payload.len());
    encode_fragment_header(&mut buf, msg_id, frag_index, total_frags);
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

/// Simulate what the recv() loop does: decode header from a raw frame.
fn recv_one(raw: Bytes, reassembly: &mut ReassemblyBuffer) -> Option<Bytes> {
    let (msg_id, frag_index, total_frags, payload) = decode_fragment_header(raw).unwrap();
    if total_frags == 1 {
        return Some(payload);
    }
    reassembly.insert(msg_id, frag_index, total_frags, payload)
}

#[test]
fn test_roundtrip_small_message() {
    let data = b"small message";
    let frame = make_frame(0, 0, 1, data);
    let mut buf = ReassemblyBuffer::new();
    let result = recv_one(frame, &mut buf).unwrap();
    assert_eq!(result.as_ref(), data);
}

#[test]
fn test_roundtrip_exactly_max_payload() {
    let data = vec![0xABu8; DC_MAX_PAYLOAD_SIZE];
    let frame = make_frame(1, 0, 1, &data);
    let mut buf = ReassemblyBuffer::new();
    let result = recv_one(frame, &mut buf).unwrap();
    assert_eq!(result.as_ref(), data.as_slice());
}

#[test]
fn test_roundtrip_one_byte_over_max_payload() {
    let data = vec![0xCDu8; DC_MAX_PAYLOAD_SIZE + 1];
    let (part0, part1) = data.split_at(DC_MAX_PAYLOAD_SIZE);

    let frame0 = make_frame(2, 0, 2, part0);
    let frame1 = make_frame(2, 1, 2, part1);

    let mut buf = ReassemblyBuffer::new();
    assert!(recv_one(frame0, &mut buf).is_none());
    let result = recv_one(frame1, &mut buf).unwrap();
    assert_eq!(result.as_ref(), data.as_slice());
}

#[test]
fn test_roundtrip_200kb_message() {
    let data: Vec<u8> = (0u8..=255).cycle().take(200 * 1024).collect();
    let total_frags = data.len().div_ceil(DC_MAX_PAYLOAD_SIZE) as u16;

    let mut buf = ReassemblyBuffer::new();
    let mut result = None;
    for (i, chunk) in data.chunks(DC_MAX_PAYLOAD_SIZE).enumerate() {
        let frame = make_frame(99, i as u16, total_frags, chunk);
        result = recv_one(frame, &mut buf);
    }
    let result = result.unwrap();
    assert_eq!(result.as_ref(), data.as_slice());
}

#[tokio::test]
async fn test_mpsc_lane() {
    use actr_protocol::RpcEnvelope;

    let (tx, rx) = mpsc::channel(10);
    let lane = MpscLane::new(PayloadType::RpcReliable, tx.clone(), rx);

    let envelope = RpcEnvelope {
        request_id: "test-1".to_string(),
        route_key: "test.route".to_string(),
        payload: Some(Bytes::from_static(b"hello")),
        traceparent: None,
        tracestate: None,
        metadata: vec![],
        timeout_ms: 30000,
        error: None,
        direction: Some(actr_protocol::Direction::Request as i32),
    };
    lane.send_envelope(envelope.clone()).await.unwrap();

    let received = lane.recv_envelope().await.unwrap();
    assert_eq!(received.request_id, "test-1");
    assert_eq!(received.payload, Some(Bytes::from_static(b"hello")));
}

#[tokio::test]
async fn test_mpsc_lane_clone() {
    use actr_protocol::RpcEnvelope;

    let (tx, rx) = mpsc::channel(10);
    let lane = MpscLane::new(PayloadType::RpcReliable, tx.clone(), rx);

    let lane2 = lane.clone();

    let envelope = RpcEnvelope {
        request_id: "test-2".to_string(),
        route_key: "test.route".to_string(),
        payload: Some(Bytes::from_static(b"test")),
        traceparent: None,
        tracestate: None,
        metadata: vec![],
        timeout_ms: 30000,
        error: None,
        direction: Some(actr_protocol::Direction::Request as i32),
    };
    lane.send_envelope(envelope.clone()).await.unwrap();

    let received = lane2.recv_envelope().await.unwrap();
    assert_eq!(received.request_id, "test-2");
    assert_eq!(received.payload, Some(Bytes::from_static(b"test")));
}

#[tokio::test]
async fn test_mpsc_lane_with_shared_rx() {
    use actr_protocol::RpcEnvelope;

    let (tx, rx) = mpsc::channel(10);
    let rx_shared = Arc::new(Mutex::new(rx));

    let lane = MpscLane::new_shared(PayloadType::RpcReliable, tx.clone(), rx_shared.clone());

    let envelope = RpcEnvelope {
        request_id: "test-3".to_string(),
        route_key: "test.route".to_string(),
        payload: Some(Bytes::from_static(b"shared")),
        traceparent: None,
        tracestate: None,
        metadata: vec![],
        timeout_ms: 30000,
        error: None,
        direction: Some(actr_protocol::Direction::Request as i32),
    };
    lane.send_envelope(envelope.clone()).await.unwrap();

    let received = lane.recv_envelope().await.unwrap();
    assert_eq!(received.request_id, "test-3");
    assert_eq!(received.payload, Some(Bytes::from_static(b"shared")));
}

#[test]
fn test_lane_type_name() {
    let (tx, rx) = mpsc::channel(10);
    let lane = MpscLane::new(PayloadType::RpcReliable, tx, rx);
    assert_eq!(lane.lane_type(), "Mpsc");
}
