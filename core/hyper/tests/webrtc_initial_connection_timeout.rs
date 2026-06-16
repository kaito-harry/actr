use actr_hyper::test_support::{
    TestHarness, TestSignalingServer, create_peer_with_websocket, make_actor_id,
};
use actr_protocol::{
    ActrError, ActrId, SignalingEnvelope, actr_relay, session_description::Type as SdpType,
    signaling_envelope,
};
use std::time::{Duration, Instant};

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
}

fn count_initial_offers(
    messages: &[SignalingEnvelope],
    source_id: &ActrId,
    target_id: &ActrId,
) -> usize {
    messages
        .iter()
        .filter(|envelope| {
            let Some(signaling_envelope::Flow::ActrRelay(relay)) = envelope.flow.as_ref() else {
                return false;
            };
            if &relay.source != source_id || &relay.target != target_id {
                return false;
            }
            let Some(actr_relay::Payload::SessionDescription(sd)) = relay.payload.as_ref() else {
                return false;
            };
            sd.r#type() == SdpType::Offer
        })
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_connection_waits_for_delayed_ice_candidate_without_factory_retry() {
    init_tracing();

    let mut harness = TestHarness::new().await;
    harness
        .server
        .delay_ice_candidates_for(Duration::from_secs(6));
    harness.add_peer(5100).await;
    harness.add_peer(5200).await;

    let offerer_id = harness.peer(5100).id.clone();
    let answerer_id = harness.peer(5200).id.clone();

    let started = Instant::now();
    harness
        .connect_with_timeout(5100, 5200, Duration::from_secs(15))
        .await;
    let elapsed = started.elapsed();

    assert!(
        harness
            .peer(5100)
            .coordinator
            .has_open_data_channel_for_test(&answerer_id)
            .await
            .expect("peer lookup should succeed"),
        "create_connection must return only after DataChannel is open"
    );
    assert!(
        harness.server.delayed_ice_candidate_count() > 0,
        "test must delay at least one ICE candidate relay"
    );
    assert!(
        elapsed >= Duration::from_secs(5),
        "connection should exercise the old 5s DataChannel-open window; elapsed={elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "connection should complete inside the first full-stack request timeout; elapsed={elapsed:?}"
    );

    let messages = harness.server.received_messages().await;
    let offer_count = count_initial_offers(&messages, &offerer_id, &answerer_id);
    assert_eq!(
        offer_count, 1,
        "delayed ICE candidates should not force a second initial offer/factory retry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn role_negotiation_timeout_is_shorter_than_initial_connection_budget() {
    init_tracing();

    let server = TestSignalingServer::start().await.unwrap();

    let offerer_id = make_actor_id(5300);
    let answerer_id = make_actor_id(5400);
    let (offerer, _offerer_client) = create_peer_with_websocket(offerer_id, &server.url())
        .await
        .unwrap();
    let (_answerer, _answerer_client) =
        create_peer_with_websocket(answerer_id.clone(), &server.url())
            .await
            .unwrap();

    server.pause_forwarding();

    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(7),
        offerer.initiate_connection(&answerer_id),
    )
    .await
    .expect("role negotiation should fail inside the short timeout");
    let elapsed = started.elapsed();

    assert!(
        matches!(result, Err(ActrError::TimedOut)),
        "expected role negotiation timeout, got {result:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(4),
        "role negotiation should wait for its response budget; elapsed={elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(7),
        "role negotiation must not consume the old 15s window; elapsed={elapsed:?}"
    );
}
