use super::*;

// ── NetworkError::kind() classification ──────────────────────────────────

#[test]
fn transient_network_errors() {
    let cases = [
        NetworkError::ConnectionError("x".into()),
        NetworkError::ConnectionClosed("x".into()),
        NetworkError::PeerConnectionClosed("x".into()),
        NetworkError::ChannelClosed("x".into()),
        NetworkError::DataChannelClosed("x".into()),
        NetworkError::DataChannelNotOpen("x".into()),
        NetworkError::SendError("x".into()),
        NetworkError::NetworkUnreachableError("x".into()),
        NetworkError::ResourceExhaustedError("x".into()),
        NetworkError::WebSocketError("x".into()),
        NetworkError::WebSocketClosed("x".into()),
        NetworkError::SignalingError("x".into()),
        NetworkError::WebRtcError("x".into()),
        NetworkError::NatTraversalError("x".into()),
        NetworkError::IceError("x".into()),
        NetworkError::TimeoutError("x".into()),
    ];
    for e in &cases {
        assert_eq!(e.kind(), ErrorKind::Transient, "{e} should be Transient");
        assert!(e.is_retryable(), "{e} should be retryable");
    }
}

#[test]
fn client_network_errors() {
    let cases = [
        NetworkError::ConnectionNotFound("x".into()),
        NetworkError::ChannelNotFound("x".into()),
        NetworkError::NoRoute("x".into()),
        NetworkError::InvalidArgument("x".into()),
        NetworkError::InvalidOperation("x".into()),
        NetworkError::ConfigurationError("x".into()),
        NetworkError::ServiceDiscoveryError("x".into()),
        NetworkError::AuthenticationError("x".into()),
        NetworkError::PermissionError("x".into()),
        NetworkError::CredentialExpired("x".into()),
    ];
    for e in &cases {
        assert_eq!(e.kind(), ErrorKind::Client, "{e} should be Client");
        assert!(!e.is_retryable(), "{e} should not be retryable");
    }
}

#[test]
fn corrupt_network_error() {
    let e = NetworkError::DeserializationError("bad bytes".into());
    assert_eq!(e.kind(), ErrorKind::Corrupt);
    assert!(e.requires_dlq());
    assert!(!e.is_retryable());
}

#[test]
fn internal_network_errors() {
    let cases = [
        NetworkError::ProtocolError("x".into()),
        NetworkError::SerializationError("x".into()),
        NetworkError::DataChannelError("x".into()),
        NetworkError::BroadcastError("x".into()),
        NetworkError::DtlsError("x".into()),
        NetworkError::StunTurnError("x".into()),
        NetworkError::NotImplemented("x".into()),
    ];
    for e in &cases {
        assert_eq!(e.kind(), ErrorKind::Internal, "{e} should be Internal");
        assert!(!e.is_retryable());
        assert!(!e.requires_dlq());
    }
}

#[test]
fn closed_like_network_errors_are_structural() {
    let closed_like = [
        NetworkError::ConnectionClosed("x".into()),
        NetworkError::PeerConnectionClosed("x".into()),
        NetworkError::DataChannelClosed("x".into()),
        NetworkError::DataChannelNotOpen("Closing".into()),
        NetworkError::WebSocketClosed("x".into()),
    ];

    for e in &closed_like {
        assert!(e.is_closed_like(), "{e} should be closed-like");
    }

    let generic_with_closed_text = [
        NetworkError::ConnectionError("not actually closed".into()),
        NetworkError::WebRtcError("not actually closed".into()),
        NetworkError::DataChannelError("not actually closed".into()),
        NetworkError::WebSocketError("not actually closed".into()),
        NetworkError::SendError("not actually closed".into()),
        NetworkError::ChannelClosed("not a stale transport lane".into()),
    ];

    for e in &generic_with_closed_text {
        assert!(
            !e.is_closed_like(),
            "{e} should not be closed-like based on message text"
        );
    }
}

// ── From<NetworkError> for ActrError (single boundary conversion) ─────────

#[test]
fn transient_network_error_becomes_unavailable() {
    let e: ActrError = NetworkError::ConnectionError("lost".into()).into();
    assert!(matches!(e, ActrError::Unavailable(_)));
    assert!(e.is_retryable());
}

#[test]
fn client_network_error_becomes_not_found() {
    let e: ActrError = NetworkError::NoRoute("dst".into()).into();
    assert!(matches!(e, ActrError::NotFound(_)));
    assert!(!e.is_retryable());
}

#[test]
fn corrupt_network_error_becomes_decode_failure() {
    let e: ActrError = NetworkError::DeserializationError("garbled".into()).into();
    assert!(matches!(e, ActrError::DecodeFailure(_)));
    assert!(e.requires_dlq());
}

#[test]
fn internal_network_error_becomes_internal() {
    let e: ActrError = NetworkError::ProtocolError("bug".into()).into();
    assert!(matches!(e, ActrError::Internal(_)));
    assert!(!e.is_retryable());
    assert!(!e.requires_dlq());
}

// ── category() / severity() surface every variant ───────────────────────

#[test]
fn category_covers_all_variants() {
    // Exhaustive: one representative per category arm, including the merged
    // serialization/deserialization bucket.
    let cases: Vec<(NetworkError, &str)> = vec![
        (NetworkError::ConnectionError("x".into()), "connection"),
        (NetworkError::SignalingError("x".into()), "signaling"),
        (NetworkError::WebRtcError("x".into()), "webrtc"),
        (NetworkError::ProtocolError("x".into()), "protocol"),
        (
            NetworkError::SerializationError("x".into()),
            "serialization",
        ),
        (
            NetworkError::DeserializationError("x".into()),
            "serialization",
        ),
        (NetworkError::TimeoutError("x".into()), "timeout"),
        (
            NetworkError::AuthenticationError("x".into()),
            "authentication",
        ),
        (NetworkError::PermissionError("x".into()), "permission"),
        (
            NetworkError::ConfigurationError("x".into()),
            "configuration",
        ),
        (
            NetworkError::ResourceExhaustedError("x".into()),
            "resource_exhausted",
        ),
        (
            NetworkError::NetworkUnreachableError("x".into()),
            "network_unreachable",
        ),
        (
            NetworkError::ServiceDiscoveryError("x".into()),
            "service_discovery",
        ),
        (NetworkError::NatTraversalError("x".into()), "nat_traversal"),
        (NetworkError::DataChannelError("x".into()), "data_channel"),
        (
            NetworkError::DataChannelClosed("x".into()),
            "data_channel_closed",
        ),
        (
            NetworkError::DataChannelNotOpen("x".into()),
            "data_channel_not_open",
        ),
        (NetworkError::IceError("x".into()), "ice"),
        (NetworkError::DtlsError("x".into()), "dtls"),
        (NetworkError::StunTurnError("x".into()), "stun_turn"),
        (NetworkError::WebSocketError("x".into()), "websocket"),
        (
            NetworkError::WebSocketClosed("x".into()),
            "websocket_closed",
        ),
        (
            NetworkError::ConnectionNotFound("x".into()),
            "connection_not_found",
        ),
        (
            NetworkError::ConnectionClosed("x".into()),
            "connection_closed",
        ),
        (
            NetworkError::PeerConnectionClosed("x".into()),
            "peer_connection_closed",
        ),
        (NetworkError::NotImplemented("x".into()), "not_implemented"),
        (NetworkError::ChannelClosed("x".into()), "channel_closed"),
        (NetworkError::SendError("x".into()), "send_error"),
        (NetworkError::NoRoute("x".into()), "no_route"),
        (
            NetworkError::InvalidOperation("x".into()),
            "invalid_operation",
        ),
        (
            NetworkError::InvalidArgument("x".into()),
            "invalid_argument",
        ),
        (
            NetworkError::ChannelNotFound("x".into()),
            "channel_not_found",
        ),
        (NetworkError::BroadcastError("x".into()), "broadcast"),
        (
            NetworkError::CredentialExpired("x".into()),
            "credential_expired",
        ),
    ];
    for (err, expected) in &cases {
        assert_eq!(err.category(), *expected, "category mismatch for {err}");
        // category() must be non-empty for every variant.
        assert!(!err.category().is_empty());
    }
}

#[test]
fn severity_is_within_1_to_10_for_all_variants() {
    // Exercises every severity arm and confirms the documented 1..=10 range.
    let all: Vec<NetworkError> = vec![
        NetworkError::ConnectionError("x".into()),
        NetworkError::SignalingError("x".into()),
        NetworkError::WebRtcError("x".into()),
        NetworkError::ProtocolError("x".into()),
        NetworkError::SerializationError("x".into()),
        NetworkError::DeserializationError("x".into()),
        NetworkError::TimeoutError("x".into()),
        NetworkError::AuthenticationError("x".into()),
        NetworkError::PermissionError("x".into()),
        NetworkError::CredentialExpired("x".into()),
        NetworkError::ConfigurationError("x".into()),
        NetworkError::ResourceExhaustedError("x".into()),
        NetworkError::NetworkUnreachableError("x".into()),
        NetworkError::ServiceDiscoveryError("x".into()),
        NetworkError::NatTraversalError("x".into()),
        NetworkError::DataChannelError("x".into()),
        NetworkError::DataChannelClosed("x".into()),
        NetworkError::DataChannelNotOpen("x".into()),
        NetworkError::IceError("x".into()),
        NetworkError::DtlsError("x".into()),
        NetworkError::StunTurnError("x".into()),
        NetworkError::WebSocketError("x".into()),
        NetworkError::WebSocketClosed("x".into()),
        NetworkError::ConnectionNotFound("x".into()),
        NetworkError::ConnectionClosed("x".into()),
        NetworkError::PeerConnectionClosed("x".into()),
        NetworkError::NotImplemented("x".into()),
        NetworkError::ChannelClosed("x".into()),
        NetworkError::SendError("x".into()),
        NetworkError::NoRoute("x".into()),
        NetworkError::InvalidOperation("x".into()),
        NetworkError::InvalidArgument("x".into()),
        NetworkError::ChannelNotFound("x".into()),
        NetworkError::BroadcastError("x".into()),
    ];
    for e in &all {
        let s = e.severity();
        assert!((1..=10).contains(&s), "severity {s} out of range for {e}");
    }
    // Spot-check a few known tiers.
    assert_eq!(NetworkError::ConfigurationError("x".into()).severity(), 10);
    assert_eq!(NetworkError::Other(anyhow::anyhow!("x")).severity(), 1);
}

// ── From conversions into NetworkError ─────────────────────────────────

#[test]
fn from_io_error_into_network_error() {
    let e: NetworkError = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "boom").into();
    assert!(matches!(e, NetworkError::IoError(_)));
    assert_eq!(e.category(), "io");
    assert_eq!(e.kind(), ErrorKind::Internal);
}

#[test]
fn from_url_parse_error_into_network_error() {
    let bad = "not a url".parse::<url::Url>().unwrap_err();
    let e: NetworkError = bad.into();
    assert!(matches!(e, NetworkError::UrlParseError(_)));
    assert_eq!(e.category(), "url_parse");
}

#[test]
fn from_json_error_into_network_error() {
    let bad: serde_json::Error = serde_json::from_str::<serde_json::Value>("{bad}").unwrap_err();
    let e: NetworkError = bad.into();
    assert!(matches!(e, NetworkError::JsonError(_)));
    assert_eq!(e.category(), "json");
}

#[test]
fn from_anyhow_into_network_error() {
    let e: NetworkError = anyhow::anyhow!("kaboom").into();
    assert!(matches!(e, NetworkError::Other(_)));
    assert_eq!(e.severity(), 1);
    assert_eq!(e.kind(), ErrorKind::Internal);
}

#[test]
fn from_webrtc_closed_error_into_peer_connection_closed() {
    for err in [
        webrtc::Error::ErrConnectionClosed,
        webrtc::Error::ErrClosedPipe,
    ] {
        let e: NetworkError = err.into();
        assert!(matches!(e, NetworkError::PeerConnectionClosed(_)));
        assert!(e.is_closed_like());
    }
}

#[test]
fn from_webrtc_not_open_error_into_data_channel_not_open() {
    for err in [
        webrtc::Error::ErrDataChannelNotOpen,
        webrtc::Error::ErrSCTPNotEstablished,
    ] {
        let e: NetworkError = err.into();
        assert!(matches!(e, NetworkError::DataChannelNotOpen(_)));
        assert!(e.is_closed_like());
    }
}

#[test]
fn from_generic_webrtc_error_stays_webrtc_error() {
    let e: NetworkError = webrtc::Error::ErrUnknownType.into();
    assert!(matches!(e, NetworkError::WebRtcError(_)));
    assert!(!e.is_closed_like());
}

#[test]
fn from_actr_id_error_into_invalid_argument() {
    // An unparseable actr-id string yields ActrIdError, which maps to InvalidArgument.
    let id_err = actr_protocol::ActrId::from_string_repr("").unwrap_err();
    let e: NetworkError = id_err.into();
    assert!(matches!(e, NetworkError::InvalidArgument(_)));
    assert_eq!(e.kind(), ErrorKind::Client);
}

#[test]
fn network_error_display_and_to_actr_error_for_other() {
    // The `Other(anyhow)` arm must round-trip through Display and become ActrError::Internal.
    let e = NetworkError::Other(anyhow::anyhow!("boom"));
    let s = e.to_string();
    assert!(s.contains("boom"));
    let ae: ActrError = e.into();
    assert!(matches!(ae, ActrError::Internal(_)));
}

// ── From<NetworkError> for ActrError: kind() fallback arms ──────────────

#[test]
fn invalid_client_inputs_keep_precise_actr_error_variants() {
    let e: ActrError = NetworkError::InvalidArgument("bad arg".into()).into();
    assert!(matches!(e, ActrError::InvalidArgument(_)));

    let e: ActrError = NetworkError::InvalidOperation("bad op".into()).into();
    assert!(matches!(e, ActrError::InvalidArgument(_)));

    let e: ActrError = NetworkError::ConfigurationError("bad config".into()).into();
    assert!(matches!(e, ActrError::Internal(_)));
}

#[test]
fn transient_kind_error_without_precise_mapping_becomes_unavailable() {
    // ResourceExhaustedError is Transient-kind but not in the precise map.
    let e: ActrError = NetworkError::ResourceExhaustedError("overload".into()).into();
    assert!(matches!(e, ActrError::Unavailable(_)));
}

#[test]
fn io_error_becomes_internal_via_kind_fallback() {
    // IoError is Internal-kind, not in any precise map → Internal.
    let e: ActrError =
        NetworkError::IoError(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "io")).into();
    assert!(matches!(e, ActrError::Internal(_)));
}
