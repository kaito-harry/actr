//! Executable coverage for documented Android/iOS network event scenarios.
//!
//! Each documented mobile SDK event sequence is mapped to the events the
//! runtime sees and reconciled into the final recovery action. A separate
//! complex scenario below runs the same reconciled events through the real
//! network event processor with real signaling/WebRTC peers.

use std::sync::Arc;
use std::time::Duration;

use actr_hyper::lifecycle::{
    AppLifecycleState, CleanupReason, NetworkAvailability, NetworkEvent, NetworkEventHandle,
    NetworkEventProcessor, NetworkEventResult, NetworkRecoveryAction, NetworkSnapshot,
    NetworkTransportFlags, ReconnectReason, process_network_event_batch,
    run_network_event_reconciler, select_network_recovery_action,
};
use actr_hyper::outbound::PeerGate;
use actr_hyper::test_support::{TestHarness, install_test_crypto_provider};
use actr_hyper::wire::webrtc::WebRtcCoordinator;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActrId, DataStream, Direction, PayloadType, RpcEnvelope};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
struct RoleCase {
    name: &'static str,
    mobile_serial: u64,
    server_serial: u64,
}

#[derive(Clone, Copy)]
struct DirectionCase {
    name: &'static str,
    from_serial: u64,
    to_serial: u64,
}

impl RoleCase {
    fn directions(self) -> [DirectionCase; 2] {
        [
            DirectionCase {
                name: "mobile_to_server",
                from_serial: self.mobile_serial,
                to_serial: self.server_serial,
            },
            DirectionCase {
                name: "server_to_mobile",
                from_serial: self.server_serial,
                to_serial: self.mobile_serial,
            },
        ]
    }
}

const ROLE_CASES: [RoleCase; 2] = [
    RoleCase {
        name: "mobile_offerer",
        mobile_serial: 100,
        server_serial: 200,
    },
    RoleCase {
        name: "mobile_answerer",
        mobile_serial: 200,
        server_serial: 100,
    },
];

struct BackgroundTasks {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

#[derive(Clone, Copy)]
enum EventSpec {
    Available,
    Lost,
    TypeWifi,
    TypeCellular,
    TypeVpn,
    TypeOther,
    UnknownPath,
    ExpensiveConstrainedCellular,
    Background,
    ForegroundShort,
    ForegroundLong,
    CleanupConnections,
    ForceReconnect,
}

impl EventSpec {
    fn to_event(self, sequence: u64) -> NetworkEvent {
        match self {
            EventSpec::Available => network_event(sequence, true, false, false),
            EventSpec::Lost => network_event(sequence, false, false, false),
            EventSpec::TypeWifi => network_event(sequence, true, true, false),
            EventSpec::TypeCellular => network_event(sequence, true, false, true),
            EventSpec::TypeVpn => NetworkEvent::NetworkPathChanged {
                snapshot: NetworkSnapshot {
                    sequence,
                    availability: NetworkAvailability::Available,
                    transport: NetworkTransportFlags {
                        wifi: false,
                        cellular: false,
                        ethernet: false,
                        vpn: true,
                        other: true,
                    },
                    is_expensive: false,
                    is_constrained: false,
                },
            },
            EventSpec::TypeOther => NetworkEvent::NetworkPathChanged {
                snapshot: NetworkSnapshot {
                    sequence,
                    availability: NetworkAvailability::Available,
                    transport: NetworkTransportFlags {
                        wifi: false,
                        cellular: false,
                        ethernet: false,
                        vpn: false,
                        other: true,
                    },
                    is_expensive: false,
                    is_constrained: false,
                },
            },
            EventSpec::UnknownPath => NetworkEvent::NetworkPathChanged {
                snapshot: NetworkSnapshot {
                    sequence,
                    availability: NetworkAvailability::Unknown,
                    transport: NetworkTransportFlags {
                        wifi: true,
                        cellular: false,
                        ethernet: false,
                        vpn: false,
                        other: false,
                    },
                    is_expensive: false,
                    is_constrained: false,
                },
            },
            EventSpec::ExpensiveConstrainedCellular => NetworkEvent::NetworkPathChanged {
                snapshot: NetworkSnapshot {
                    sequence,
                    availability: NetworkAvailability::Available,
                    transport: NetworkTransportFlags {
                        wifi: false,
                        cellular: true,
                        ethernet: false,
                        vpn: false,
                        other: false,
                    },
                    is_expensive: true,
                    is_constrained: true,
                },
            },
            EventSpec::Background => NetworkEvent::AppLifecycleChanged {
                state: actr_hyper::lifecycle::AppLifecycleState::Background,
            },
            EventSpec::ForegroundShort => NetworkEvent::AppLifecycleChanged {
                state: actr_hyper::lifecycle::AppLifecycleState::Foreground {
                    background_duration_ms: 5_000,
                },
            },
            EventSpec::ForegroundLong => NetworkEvent::AppLifecycleChanged {
                state: actr_hyper::lifecycle::AppLifecycleState::Foreground {
                    background_duration_ms: 45_000,
                },
            },
            EventSpec::CleanupConnections => NetworkEvent::CleanupConnections {
                reason: CleanupReason::ManualReset,
            },
            EventSpec::ForceReconnect => NetworkEvent::ForceReconnect {
                reason: actr_hyper::lifecycle::ReconnectReason::ManualReconnect,
            },
        }
    }
}

fn network_event(sequence: u64, available: bool, wifi: bool, cellular: bool) -> NetworkEvent {
    NetworkEvent::NetworkPathChanged {
        snapshot: NetworkSnapshot {
            sequence,
            availability: if available {
                NetworkAvailability::Available
            } else {
                NetworkAvailability::Unavailable
            },
            transport: NetworkTransportFlags {
                wifi,
                cellular,
                ethernet: false,
                vpn: false,
                other: false,
            },
            is_expensive: false,
            is_constrained: false,
        },
    }
}

fn online_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, true, false, false)
}

fn offline_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, false, false, false)
}

fn wifi_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, true, true, false)
}

fn cellular_event(sequence: u64) -> NetworkEvent {
    network_event(sequence, true, false, true)
}

#[derive(Clone, Copy)]
struct MobileScenario {
    name: &'static str,
    sdk_events: &'static [EventSpec],
    expected_action: NetworkRecoveryAction,
}

const A: EventSpec = EventSpec::Available;
const L: EventSpec = EventSpec::Lost;
const TW: EventSpec = EventSpec::TypeWifi;
const TC: EventSpec = EventSpec::TypeCellular;
const TV: EventSpec = EventSpec::TypeVpn;
const TO: EventSpec = EventSpec::TypeOther;
const TU: EventSpec = EventSpec::UnknownPath;
const TEC: EventSpec = EventSpec::ExpensiveConstrainedCellular;
const BG: EventSpec = EventSpec::Background;
const FS: EventSpec = EventSpec::ForegroundShort;
const FL: EventSpec = EventSpec::ForegroundLong;
const CC: EventSpec = EventSpec::CleanupConnections;
const FR: EventSpec = EventSpec::ForceReconnect;

const ANDROID_SCENARIOS: &[MobileScenario] = &[
    MobileScenario {
        name: "android_cold_start_online",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_cold_start_offline",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "android_wifi_enabled",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_wifi_lost_without_cellular",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "android_wifi_to_cellular_failover",
        sdk_events: &[L, A, TC],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_cellular_to_wifi_with_interleaved_lost",
        sdk_events: &[A, L, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_duplicate_available_type_changed_storm",
        sdk_events: &[A, A, TW, TW, A],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_short_network_flap",
        sdk_events: &[L, A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_airplane_mode_on",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "android_airplane_mode_off",
        sdk_events: &[A, TC],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_vpn_toggle",
        sdk_events: &[TV],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_other_transport_available",
        sdk_events: &[TO],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_captive_portal_or_validated_change",
        sdk_events: &[TU],
        expected_action: NetworkRecoveryAction::Probe,
    },
    MobileScenario {
        name: "android_dns_or_link_properties_change",
        sdk_events: &[TU],
        expected_action: NetworkRecoveryAction::Probe,
    },
    MobileScenario {
        name: "android_metered_change_no_event",
        sdk_events: &[],
        expected_action: NetworkRecoveryAction::Noop,
    },
    MobileScenario {
        name: "android_metered_change_reported_as_type_changed",
        sdk_events: &[TEC],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_blocked_status_change",
        sdk_events: &[],
        expected_action: NetworkRecoveryAction::Noop,
    },
    MobileScenario {
        name: "android_background_default_no_cleanup",
        sdk_events: &[BG],
        expected_action: NetworkRecoveryAction::Noop,
    },
    MobileScenario {
        name: "android_short_foreground_probe_without_path",
        sdk_events: &[FS],
        expected_action: NetworkRecoveryAction::Probe,
    },
    MobileScenario {
        name: "android_short_foreground_with_online_snapshot",
        sdk_events: &[FS, A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_long_foreground_force_reconnect",
        sdk_events: &[FL, A, TW],
        expected_action: NetworkRecoveryAction::ForceReconnect,
    },
    MobileScenario {
        name: "android_long_foreground_offline_wins",
        sdk_events: &[FL, L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "android_foreground_without_cleanup",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_foreground_legacy_cleanup",
        sdk_events: &[CC, A, TW],
        expected_action: NetworkRecoveryAction::CleanupOnly,
    },
    MobileScenario {
        name: "android_cleanup_suppresses_later_network_restore",
        sdk_events: &[CC, A, TW, A],
        expected_action: NetworkRecoveryAction::CleanupOnly,
    },
    MobileScenario {
        name: "android_force_reconnect_over_online_path",
        sdk_events: &[FR, A, TW],
        expected_action: NetworkRecoveryAction::ForceReconnect,
    },
    MobileScenario {
        name: "android_background_network_change_delayed_online",
        sdk_events: &[A, TC],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_background_network_change_delayed_offline",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "android_doze_delayed_callback",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_process_restart_online",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "android_process_restart_offline",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "android_websocket_remote_close_not_a_network_event",
        sdk_events: &[],
        expected_action: NetworkRecoveryAction::Noop,
    },
];

const IOS_SCENARIOS: &[MobileScenario] = &[
    MobileScenario {
        name: "ios_cold_start_online",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_cold_start_offline",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "ios_wifi_to_cellular_with_unsatisfied_gap",
        sdk_events: &[L, A, TC],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_cellular_to_wifi",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_duplicate_path_updates_collapse_to_restore",
        sdk_events: &[A, A, TW, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_wifi_lost_without_cellular",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "ios_airplane_mode_on",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "ios_airplane_mode_off",
        sdk_events: &[A, TC],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_vpn_or_hotspot_change",
        sdk_events: &[TV],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_hotspot_other_transport_change",
        sdk_events: &[TO],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_low_data_mode_change",
        sdk_events: &[],
        expected_action: NetworkRecoveryAction::Noop,
    },
    MobileScenario {
        name: "ios_low_data_mode_reported_as_constrained_path",
        sdk_events: &[TEC],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_expensive_network_change_no_event",
        sdk_events: &[],
        expected_action: NetworkRecoveryAction::Noop,
    },
    MobileScenario {
        name: "ios_expensive_network_change_reported_as_type_changed",
        sdk_events: &[TC],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_route_or_dns_change",
        sdk_events: &[TU],
        expected_action: NetworkRecoveryAction::Probe,
    },
    MobileScenario {
        name: "ios_background_default_no_cleanup",
        sdk_events: &[BG],
        expected_action: NetworkRecoveryAction::Noop,
    },
    MobileScenario {
        name: "ios_short_foreground_probe_without_path",
        sdk_events: &[FS],
        expected_action: NetworkRecoveryAction::Probe,
    },
    MobileScenario {
        name: "ios_short_foreground_with_online_snapshot",
        sdk_events: &[FS, A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_long_foreground_force_reconnect",
        sdk_events: &[FL, A, TW],
        expected_action: NetworkRecoveryAction::ForceReconnect,
    },
    MobileScenario {
        name: "ios_long_foreground_offline_wins",
        sdk_events: &[FL, L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "ios_foreground_without_cleanup",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_foreground_legacy_cleanup",
        sdk_events: &[CC, A, TW],
        expected_action: NetworkRecoveryAction::CleanupOnly,
    },
    MobileScenario {
        name: "ios_cleanup_suppresses_delayed_path_callbacks",
        sdk_events: &[CC, A, TW],
        expected_action: NetworkRecoveryAction::CleanupOnly,
    },
    MobileScenario {
        name: "ios_force_reconnect_over_online_path",
        sdk_events: &[FR, A, TW],
        expected_action: NetworkRecoveryAction::ForceReconnect,
    },
    MobileScenario {
        name: "ios_suspended_restore_online",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_suspended_restore_offline",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "ios_multi_scene_duplicate_foreground_events",
        sdk_events: &[A, A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_app_killed_restart_online",
        sdk_events: &[A, TW],
        expected_action: NetworkRecoveryAction::Restore,
    },
    MobileScenario {
        name: "ios_app_killed_restart_offline",
        sdk_events: &[L],
        expected_action: NetworkRecoveryAction::Offline,
    },
    MobileScenario {
        name: "ios_websocket_remote_close_not_a_network_event",
        sdk_events: &[],
        expected_action: NetworkRecoveryAction::Noop,
    },
];

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .with_test_writer()
        .try_init()
        .ok();
}

fn materialize_events(specs: &[EventSpec]) -> Vec<NetworkEvent> {
    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| spec.to_event(index as u64 + 1))
        .collect()
}

fn parse_cleanup_reason(value: &str) -> CleanupReason {
    match value {
        "AppTerminating" => CleanupReason::AppTerminating,
        "UserLogout" => CleanupReason::UserLogout,
        "StaleConnectionSuspected" => CleanupReason::StaleConnectionSuspected,
        "ManualReset" => CleanupReason::ManualReset,
        other => panic!("unsupported cleanup reason in mobile JSONL: {other}"),
    }
}

fn parse_reconnect_reason(value: &str) -> actr_hyper::lifecycle::ReconnectReason {
    match value {
        "NetworkPathChanged" => actr_hyper::lifecycle::ReconnectReason::NetworkPathChanged,
        "LongBackground" => actr_hyper::lifecycle::ReconnectReason::LongBackground,
        "ProbeFailed" => actr_hyper::lifecycle::ReconnectReason::ProbeFailed,
        "ManualReconnect" => actr_hyper::lifecycle::ReconnectReason::ManualReconnect,
        "StaleConnectionSuspected" => {
            actr_hyper::lifecycle::ReconnectReason::StaleConnectionSuspected
        }
        other => panic!("unsupported reconnect reason in mobile JSONL: {other}"),
    }
}

fn parse_mobile_jsonl_events(jsonl: &str) -> Vec<NetworkEvent> {
    let mut events = Vec::new();

    for line in jsonl.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let value: serde_json::Value =
            serde_json::from_str(line).expect("mobile JSONL line should be valid JSON");

        if let Some(snapshot) = value.get("network_snapshot") {
            let sequence = snapshot
                .get("sequence")
                .and_then(serde_json::Value::as_u64)
                .expect("network_snapshot.sequence is required");
            let availability = match snapshot
                .get("availability")
                .and_then(serde_json::Value::as_str)
                .expect("network_snapshot.availability is required")
            {
                "Available" | "available" => NetworkAvailability::Available,
                "Unavailable" | "unavailable" => NetworkAvailability::Unavailable,
                "Unknown" | "unknown" => NetworkAvailability::Unknown,
                other => panic!("unsupported network availability in mobile JSONL: {other}"),
            };
            let transport = snapshot
                .get("transport")
                .unwrap_or(&serde_json::Value::Null);
            let flag = |name: &str| {
                transport
                    .get(name)
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
            };

            events.push(NetworkEvent::NetworkPathChanged {
                snapshot: NetworkSnapshot {
                    sequence,
                    availability,
                    transport: NetworkTransportFlags {
                        wifi: flag("wifi"),
                        cellular: flag("cellular"),
                        ethernet: flag("ethernet"),
                        vpn: flag("vpn"),
                        other: flag("other"),
                    },
                    is_expensive: snapshot
                        .get("is_expensive")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false),
                    is_constrained: snapshot
                        .get("is_constrained")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false),
                },
            });
        }

        if let Some(lifecycle) = value.get("lifecycle_event") {
            let state = match lifecycle
                .get("state")
                .and_then(serde_json::Value::as_str)
                .expect("lifecycle_event.state is required")
            {
                "Background" | "background" => actr_hyper::lifecycle::AppLifecycleState::Background,
                "Foreground" | "foreground" => {
                    actr_hyper::lifecycle::AppLifecycleState::Foreground {
                        background_duration_ms: lifecycle
                            .get("background_duration_ms")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(0),
                    }
                }
                other => panic!("unsupported lifecycle state in mobile JSONL: {other}"),
            };
            events.push(NetworkEvent::AppLifecycleChanged { state });
        }

        if let Some(command) = value.get("cleanup_command") {
            let reason = command
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(parse_cleanup_reason)
                .unwrap_or(CleanupReason::ManualReset);
            events.push(NetworkEvent::CleanupConnections { reason });
        }

        if let Some(command) = value.get("reconnect_command") {
            let reason = command
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(parse_reconnect_reason)
                .unwrap_or(actr_hyper::lifecycle::ReconnectReason::ManualReconnect);
            events.push(NetworkEvent::ForceReconnect { reason });
        }
    }

    events
}

fn spawn_rpc_router(
    coordinator: Arc<WebRtcCoordinator>,
    gate: Arc<PeerGate>,
    name: &str,
) -> tokio::task::JoinHandle<()> {
    let name = name.to_string();
    tokio::spawn(async move {
        loop {
            match coordinator.receive_message().await {
                Ok(Some((sender_bytes, message_data, _payload_type))) => {
                    let sender_id = match ActrId::decode(sender_bytes.as_slice()) {
                        Ok(sender_id) => sender_id,
                        Err(e) => {
                            tracing::error!("{} failed to decode sender ActrId: {}", name, e);
                            continue;
                        }
                    };

                    let envelope = match RpcEnvelope::decode(message_data.as_ref()) {
                        Ok(envelope) => envelope,
                        Err(e) => {
                            tracing::error!("{} failed to decode RpcEnvelope: {}", name, e);
                            continue;
                        }
                    };

                    if envelope.route_key == "response" {
                        let result = if let Some(error) = envelope.error {
                            Err(actr_protocol::ActrError::Unavailable(format!(
                                "RPC error {}: {}",
                                error.code, error.message
                            )))
                        } else if let Some(payload) = envelope.payload {
                            Ok(payload)
                        } else {
                            Err(actr_protocol::ActrError::DecodeFailure(
                                "Invalid response: no payload or error".to_string(),
                            ))
                        };

                        if let Err(e) = gate.handle_response(&envelope.request_id, result).await {
                            tracing::error!(
                                "{} failed to handle response {}: {}",
                                name,
                                envelope.request_id,
                                e
                            );
                        }
                        continue;
                    }

                    let response = RpcEnvelope {
                        request_id: envelope.request_id.clone(),
                        route_key: "response".to_string(),
                        payload: envelope.payload.clone(),
                        timeout_ms: 0,
                        direction: Some(Direction::Response as i32),
                        ..Default::default()
                    };

                    if let Err(e) = gate.send_message(&sender_id, response).await {
                        tracing::error!(
                            "{} failed to send echo response for {}: {}",
                            name,
                            envelope.request_id,
                            e
                        );
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("{} receive loop failed: {}", name, e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
}

async fn setup_bidirectional_mobile_server_harness(
    case: RoleCase,
) -> (TestHarness, BackgroundTasks) {
    let mut harness = TestHarness::with_vnet().await;
    for serial in [
        case.mobile_serial.min(case.server_serial),
        case.mobile_serial.max(case.server_serial),
    ] {
        harness.add_peer(serial).await;
    }

    let bg_tasks = BackgroundTasks {
        handles: vec![
            spawn_rpc_router(
                harness.peer(case.mobile_serial).coordinator.clone(),
                harness.peer(case.mobile_serial).gate.clone(),
                "mobile_network_event_router",
            ),
            spawn_rpc_router(
                harness.peer(case.server_serial).coordinator.clone(),
                harness.peer(case.server_serial).gate.clone(),
                "server_network_event_router",
            ),
        ],
    };

    expect_request_ok(
        &harness,
        case.mobile_serial,
        case.server_serial,
        &format!("{}_bidirectional_setup", case.name),
        Duration::from_secs(30),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    (harness, bg_tasks)
}

async fn expect_request_ok(
    harness: &TestHarness,
    from_serial: u64,
    to_serial: u64,
    request_id: &str,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempt = 0;

    loop {
        attempt += 1;
        let attempt_id = format!("{request_id}_{attempt}");
        let handle = harness
            .peer(from_serial)
            .spawn_request(to_serial, &attempt_id, 2_000);

        let last_error = match tokio::time::timeout(Duration::from_secs(3), handle).await {
            Ok(Ok(Ok(response))) => {
                assert!(
                    !response.is_empty(),
                    "{} should receive a non-empty response",
                    request_id
                );
                return;
            }
            Ok(Ok(Err(err))) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("connection not ready")
                        || (msg.contains("Request timeout") || msg.contains("timed out"))
                        || msg.contains("timed out")
                        || msg.contains("Connection"),
                    "unexpected retry error while waiting for recovery: {msg}"
                );
                msg
            }
            Ok(Err(err)) => panic!("{} request task panicked: {}", request_id, err),
            Err(_) => format!("{request_id} attempt {attempt} timed out"),
        };

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "{} request failed to recover within {:?}; last error: {}",
                request_id, timeout, last_error
            );
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn expect_request_bounded_failure(
    harness: &TestHarness,
    from_serial: u64,
    to_serial: u64,
    request_id: &str,
    request_timeout_ms: u32,
    timeout: Duration,
) -> String {
    let handle = harness
        .peer(from_serial)
        .spawn_request(to_serial, request_id, request_timeout_ms);

    match tokio::time::timeout(timeout, handle).await {
        Ok(Ok(Err(err))) => {
            let msg = err.to_string();
            assert!(
                is_expected_bounded_send_error(&msg),
                "{} failed with unexpected error: {}",
                request_id,
                msg
            );
            msg
        }
        Ok(Ok(Ok(response))) => panic!(
            "{} unexpectedly succeeded with {} bytes",
            request_id,
            response.len()
        ),
        Ok(Err(err)) => panic!("{} request task panicked: {}", request_id, err),
        Err(_) => panic!("{} did not complete within {:?}", request_id, timeout),
    }
}

async fn expect_direction_requests_ok(
    harness: &TestHarness,
    case: RoleCase,
    label: &str,
    timeout: Duration,
) {
    for direction in case.directions() {
        expect_request_ok(
            harness,
            direction.from_serial,
            direction.to_serial,
            &format!("{label}_{}", direction.name),
            timeout,
        )
        .await;
    }
}

async fn wait_for_transport_dest_count(
    harness: &TestHarness,
    serial: u64,
    expected: usize,
    label: &str,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let actual = harness.peer(serial).transport_manager.dest_count().await;
        if actual == expected {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "{label} expected peer {serial} to have {expected} cached transports, got {actual}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn is_expected_bounded_send_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "connection",
        "request timeout",
        "timed out",
        "closed",
        "recovering",
        "data channel",
        "datachannel",
        "channel error",
        "not opened",
        "timeout",
        "unavailable",
        "not found",
        "no route",
        "all transport candidates exhausted",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

async fn send_data_stream_bounded(
    harness: &TestHarness,
    from_serial: u64,
    to_serial: u64,
    stream_id: &str,
    timeout: Duration,
) {
    let target_id = harness.peer(to_serial).id.clone();
    let stream = DataStream {
        stream_id: stream_id.to_string(),
        sequence: 1,
        payload: bytes::Bytes::from_static(b"mobile-event-storm-stream-payload"),
        metadata: Vec::new(),
        timestamp_ms: Some(0),
    };
    let payload = bytes::Bytes::from(stream.encode_to_vec());
    let send = harness.peer(from_serial).gate.send_data_stream(
        &target_id,
        PayloadType::StreamReliable,
        &stream.stream_id,
        payload,
    );

    let result = tokio::time::timeout(timeout, send)
        .await
        .unwrap_or_else(|_| panic!("{stream_id} DataStream send hung for {:?}", timeout));

    if let Err(err) = result {
        let msg = err.to_string();
        assert!(
            is_expected_bounded_send_error(&msg),
            "{stream_id} DataStream failed with unexpected error: {msg}"
        );
    }
}

fn spawn_network_event_reconciler(
    processor: Arc<dyn NetworkEventProcessor>,
    result_timeout: Duration,
) -> (
    NetworkEventHandle,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(32);
    let handle = NetworkEventHandle::new_with_result_timeout(event_tx, result_timeout);
    let shutdown = CancellationToken::new();
    let reconciler_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        run_network_event_reconciler(event_rx, processor, reconciler_shutdown).await;
    });

    (handle, shutdown, task)
}

async fn submit_mobile_event(
    handle: NetworkEventHandle,
    event: NetworkEvent,
) -> Result<NetworkEventResult, String> {
    match event {
        NetworkEvent::NetworkPathChanged { snapshot } => {
            handle.handle_network_path_changed(snapshot).await
        }
        NetworkEvent::AppLifecycleChanged { state } => {
            handle.handle_app_lifecycle_changed(state).await
        }
        NetworkEvent::CleanupConnections { reason } => handle.cleanup_connections(reason).await,
        NetworkEvent::ForceReconnect { reason } => handle.force_reconnect(reason).await,
    }
}

async fn submit_mobile_event_storm(
    handle: &NetworkEventHandle,
    events: Vec<NetworkEvent>,
) -> Vec<NetworkEventResult> {
    let mut tasks = Vec::new();
    for event in events {
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            submit_mobile_event(handle, event).await
        }));
    }

    let mut results = Vec::new();
    for task in tasks {
        let result = task
            .await
            .expect("mobile network event task should not panic")
            .expect("mobile network event should complete");
        results.push(result);
    }
    results
}

fn assert_scenario_action(platform: &str, scenario: &MobileScenario) {
    let label = format!("{}_{}", platform, scenario.name);
    let events = materialize_events(scenario.sdk_events);
    let action = select_network_recovery_action(&events);
    assert_eq!(
        action, scenario.expected_action,
        "{} selected unexpected action for {:?}",
        label, events
    );
}

fn assert_documented_scenarios(platform: &str, scenarios: &[MobileScenario]) {
    for scenario in scenarios {
        assert_scenario_action(platform, scenario);
    }
}

#[test]
fn test_android_documented_network_scenarios() {
    assert_documented_scenarios("android", ANDROID_SCENARIOS);
}

#[test]
fn test_ios_documented_network_scenarios() {
    assert_documented_scenarios("ios", IOS_SCENARIOS);
}

#[test]
fn test_mobile_replay_cases_select_expected_actions() {
    struct Case {
        name: &'static str,
        events: Vec<NetworkEvent>,
        expected: NetworkRecoveryAction,
    }

    let cases = vec![
        Case {
            name: "android_old_lost_late",
            events: vec![
                network_event(10, true, false, true),
                wifi_event(11),
                offline_event(9),
            ],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            name: "ios_unsatisfied_gap_restored",
            events: vec![
                offline_event(20),
                network_event(21, true, false, true),
                wifi_event(22),
            ],
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            name: "short_foreground_online",
            events: materialize_events(&[FS, A, TW]),
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            name: "long_foreground_online",
            events: materialize_events(&[FL, A, TW]),
            expected: NetworkRecoveryAction::ForceReconnect,
        },
        Case {
            name: "long_foreground_offline",
            events: materialize_events(&[FL, L]),
            expected: NetworkRecoveryAction::Offline,
        },
        Case {
            name: "cleanup_path_force",
            events: materialize_events(&[CC, A, FR]),
            expected: NetworkRecoveryAction::CleanupOnly,
        },
    ];

    for case in cases {
        assert_eq!(
            select_network_recovery_action(&case.events),
            case.expected,
            "{} selected unexpected action for {:?}",
            case.name,
            case.events
        );
    }
}

#[test]
fn test_mobile_jsonl_replay_maps_real_log_shape_to_recovery_actions() {
    struct Case {
        name: &'static str,
        jsonl: &'static str,
        expected: NetworkRecoveryAction,
    }

    let cases = [
        Case {
            name: "android_old_lost_late",
            jsonl: r#"
{"case_id":"L3-A06","platform":"android","t_ms":1,"network_snapshot":{"sequence":10,"availability":"Available","transport":{"cellular":true}}}
{"case_id":"L3-A06","platform":"android","t_ms":2,"network_snapshot":{"sequence":11,"availability":"Available","transport":{"wifi":true}}}
{"case_id":"L3-A06","platform":"android","t_ms":3,"network_snapshot":{"sequence":9,"availability":"Unavailable","transport":{}}}
"#,
            expected: NetworkRecoveryAction::Restore,
        },
        Case {
            name: "ios_long_foreground_online",
            jsonl: r#"
{"case_id":"L3-I14","platform":"ios","t_ms":1,"lifecycle_event":{"state":"Foreground","background_duration_ms":65000}}
{"case_id":"L3-I14","platform":"ios","t_ms":2,"network_snapshot":{"sequence":22,"availability":"Available","transport":{"wifi":true},"is_expensive":false,"is_constrained":false}}
"#,
            expected: NetworkRecoveryAction::ForceReconnect,
        },
        Case {
            name: "cleanup_suppresses_delayed_path",
            jsonl: r#"
{"case_id":"RC-27","platform":"ios","t_ms":1,"cleanup_command":{"reason":"UserLogout"}}
{"case_id":"RC-27","platform":"ios","t_ms":2,"network_snapshot":{"sequence":30,"availability":"Available","transport":{"wifi":true}}}
"#,
            expected: NetworkRecoveryAction::CleanupOnly,
        },
    ];

    for case in cases {
        assert_eq!(
            select_network_recovery_action(&parse_mobile_jsonl_events(case.jsonl)),
            case.expected,
            "{} JSONL selected unexpected action",
            case.name
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mobile_app_kill_cleanup_then_restart_online_recovers_bidirectional_server_send() {
    install_test_crypto_provider();

    init_tracing();

    for case in ROLE_CASES {
        let label = format!("{}_app_kill_restart_online", case.name);
        let (harness, mut bg_tasks) = setup_bidirectional_mobile_server_harness(case).await;
        harness.reset_counters();

        expect_direction_requests_ok(
            &harness,
            case,
            &format!("{label}_baseline"),
            Duration::from_secs(15),
        )
        .await;

        let cleanup_events = vec![NetworkEvent::CleanupConnections {
            reason: CleanupReason::AppTerminating,
        }];
        assert_eq!(
            select_network_recovery_action(&cleanup_events),
            NetworkRecoveryAction::CleanupOnly
        );
        let cleanup_results = process_network_event_batch(
            cleanup_events,
            harness.peer(case.mobile_serial).network_processor(),
        )
        .await;
        assert!(
            cleanup_results.iter().all(|result| result.success),
            "{label} app terminating cleanup should succeed: {cleanup_results:?}"
        );

        let mobile_router = bg_tasks.handles.remove(0);
        mobile_router.abort();
        assert!(
            !harness
                .peer(case.mobile_serial)
                .signaling_client
                .is_connected(),
            "{label} mobile signaling should be disconnected while app is killed"
        );
        wait_for_transport_dest_count(
            &harness,
            case.mobile_serial,
            0,
            &format!("{label}_mobile_cleanup"),
            Duration::from_secs(5),
        )
        .await;

        let killed_error = expect_request_bounded_failure(
            &harness,
            case.server_serial,
            case.mobile_serial,
            &format!("{label}_server_send_while_killed"),
            1_000,
            Duration::from_secs(15),
        )
        .await;
        tracing::info!(
            case = case.name,
            error = %killed_error,
            "server-to-mobile send failed bounded while mobile app was killed"
        );
        assert_eq!(
            harness.peer(case.server_serial).pending_count().await,
            0,
            "{label} server send while killed should not leak pending state"
        );

        bg_tasks.handles.push(spawn_rpc_router(
            harness.peer(case.mobile_serial).coordinator.clone(),
            harness.peer(case.mobile_serial).gate.clone(),
            "mobile_network_event_router_restarted",
        ));

        let restart_events = vec![online_event(20), wifi_event(21)];
        assert_eq!(
            select_network_recovery_action(&restart_events),
            NetworkRecoveryAction::Restore
        );
        let restart_results = process_network_event_batch(
            restart_events,
            harness.peer(case.mobile_serial).network_processor(),
        )
        .await;
        assert!(
            restart_results.iter().all(|result| result.success),
            "{label} online restart restore should succeed: {restart_results:?}"
        );
        assert!(
            harness
                .peer(case.mobile_serial)
                .signaling_client
                .is_connected(),
            "{label} mobile signaling should reconnect after app restart"
        );

        expect_direction_requests_ok(
            &harness,
            case,
            &format!("{label}_after_restart"),
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(
            harness.peer(case.mobile_serial).pending_count().await,
            0,
            "{label} mobile should not leak pending requests after restart"
        );
        assert_eq!(
            harness.peer(case.server_serial).pending_count().await,
            0,
            "{label} server should not leak pending requests after restart"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mobile_app_kill_restart_offline_bounds_server_send_until_online_restore() {
    install_test_crypto_provider();

    init_tracing();

    for case in ROLE_CASES {
        let label = format!("{}_app_kill_restart_offline", case.name);
        let (harness, mut bg_tasks) = setup_bidirectional_mobile_server_harness(case).await;
        harness.reset_counters();

        expect_direction_requests_ok(
            &harness,
            case,
            &format!("{label}_baseline"),
            Duration::from_secs(15),
        )
        .await;

        let cleanup_results = process_network_event_batch(
            vec![NetworkEvent::CleanupConnections {
                reason: CleanupReason::AppTerminating,
            }],
            harness.peer(case.mobile_serial).network_processor(),
        )
        .await;
        assert!(
            cleanup_results.iter().all(|result| result.success),
            "{label} app terminating cleanup should succeed: {cleanup_results:?}"
        );

        let mobile_router = bg_tasks.handles.remove(0);
        mobile_router.abort();
        wait_for_transport_dest_count(
            &harness,
            case.mobile_serial,
            0,
            &format!("{label}_mobile_cleanup"),
            Duration::from_secs(5),
        )
        .await;

        harness.simulate_disconnect();
        bg_tasks.handles.push(spawn_rpc_router(
            harness.peer(case.mobile_serial).coordinator.clone(),
            harness.peer(case.mobile_serial).gate.clone(),
            "mobile_network_event_router_restarted_offline",
        ));

        let offline_restart_events = vec![offline_event(30)];
        assert_eq!(
            select_network_recovery_action(&offline_restart_events),
            NetworkRecoveryAction::Offline
        );
        let offline_results = process_network_event_batch(
            offline_restart_events,
            harness.peer(case.mobile_serial).network_processor(),
        )
        .await;
        assert!(
            offline_results.iter().all(|result| result.success),
            "{label} offline restart processing should succeed: {offline_results:?}"
        );
        assert!(
            !harness
                .peer(case.mobile_serial)
                .signaling_client
                .is_connected(),
            "{label} mobile signaling should remain disconnected while restarted offline"
        );

        let offline_error = expect_request_bounded_failure(
            &harness,
            case.server_serial,
            case.mobile_serial,
            &format!("{label}_server_send_while_restart_offline"),
            1_000,
            Duration::from_secs(15),
        )
        .await;
        tracing::info!(
            case = case.name,
            error = %offline_error,
            "server-to-mobile send failed bounded while mobile app restarted offline"
        );
        assert_eq!(
            harness.peer(case.server_serial).pending_count().await,
            0,
            "{label} server send while offline restart should not leak pending state"
        );

        harness.simulate_reconnect();
        let online_restore_events = vec![online_event(31), wifi_event(32)];
        assert_eq!(
            select_network_recovery_action(&online_restore_events),
            NetworkRecoveryAction::Restore
        );
        let restore_results = process_network_event_batch(
            online_restore_events,
            harness.peer(case.mobile_serial).network_processor(),
        )
        .await;
        assert!(
            restore_results.iter().all(|result| result.success),
            "{label} online restore should succeed after offline restart: {restore_results:?}"
        );

        expect_direction_requests_ok(
            &harness,
            case,
            &format!("{label}_after_online_restore"),
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(
            harness.peer(case.mobile_serial).pending_count().await,
            0,
            "{label} mobile should not leak pending requests after online restore"
        );
        assert_eq!(
            harness.peer(case.server_serial).pending_count().await,
            0,
            "{label} server should not leak pending requests after online restore"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_complex_mobile_event_storms_with_real_network_outage() {
    install_test_crypto_provider();

    init_tracing();

    for case in ROLE_CASES {
        let (harness, _bg_tasks) = setup_bidirectional_mobile_server_harness(case).await;
        harness.reset_counters();

        harness.simulate_disconnect();
        tokio::time::sleep(Duration::from_secs(8)).await;
        harness.simulate_reconnect();

        let recovered_after_outage = vec![offline_event(1), online_event(2), wifi_event(3)];
        assert_eq!(
            select_network_recovery_action(&recovered_after_outage),
            NetworkRecoveryAction::Restore
        );
        let results = process_network_event_batch(
            recovered_after_outage,
            harness.peer(case.mobile_serial).network_processor(),
        )
        .await;
        assert!(
            results.iter().all(|result| result.success),
            "{} full outage restore should succeed: {results:?}",
            case.name
        );
        harness
            .wait_for_ice_restart_count(1, Duration::from_secs(10))
            .await;
        expect_direction_requests_ok(
            &harness,
            case,
            &format!("{}_complex_full_outage_recovered", case.name),
            Duration::from_secs(15),
        )
        .await;

        let restore_last = vec![online_event(4), offline_event(5), online_event(6)];
        assert_eq!(
            select_network_recovery_action(&restore_last),
            NetworkRecoveryAction::Restore
        );
        let results = process_network_event_batch(
            restore_last,
            harness.peer(case.mobile_serial).network_processor(),
        )
        .await;
        assert!(
            results.iter().all(|result| result.success),
            "{} available-lost-available restore should succeed: {results:?}",
            case.name
        );
        expect_direction_requests_ok(
            &harness,
            case,
            &format!("{}_complex_available_lost_available", case.name),
            Duration::from_secs(15),
        )
        .await;

        let offline_last = vec![offline_event(7), online_event(8), offline_event(9)];
        assert_eq!(
            select_network_recovery_action(&offline_last),
            NetworkRecoveryAction::Offline
        );
        let results = process_network_event_batch(
            offline_last,
            harness.peer(case.mobile_serial).network_processor(),
        )
        .await;
        assert!(
            results.iter().all(|result| result.success),
            "{} offline-last batch should succeed: {results:?}",
            case.name
        );

        let restore_results = process_network_event_batch(
            vec![online_event(10)],
            harness.peer(case.mobile_serial).network_processor(),
        )
        .await;
        assert!(
            restore_results.iter().all(|result| result.success),
            "{} final online restore should succeed: {restore_results:?}",
            case.name
        );
        expect_direction_requests_ok(
            &harness,
            case,
            &format!("{}_complex_offline_then_restore", case.name),
            Duration::from_secs(15),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mobile_network_event_handle_storm_then_call_and_data_stream_are_bounded() {
    install_test_crypto_provider();

    init_tracing();

    for case in ROLE_CASES {
        let (harness, _bg_tasks) = setup_bidirectional_mobile_server_harness(case).await;
        harness.reset_counters();

        let processor: Arc<dyn NetworkEventProcessor> =
            harness.peer(case.mobile_serial).network_processor();
        let (handle, shutdown, reconciler) =
            spawn_network_event_reconciler(processor, Duration::from_secs(10));

        harness.simulate_disconnect();
        tokio::time::sleep(Duration::from_secs(8)).await;
        harness.simulate_reconnect();

        let storm_events = vec![
            offline_event(1),
            online_event(2),
            wifi_event(3),
            cellular_event(4),
            wifi_event(5),
            NetworkEvent::AppLifecycleChanged {
                state: AppLifecycleState::Foreground {
                    background_duration_ms: 5_000,
                },
            },
            NetworkEvent::ForceReconnect {
                reason: ReconnectReason::StaleConnectionSuspected,
            },
        ];
        assert_eq!(
            select_network_recovery_action(&storm_events),
            NetworkRecoveryAction::ForceReconnect
        );

        let results = submit_mobile_event_storm(&handle, storm_events).await;
        assert!(
            results.iter().all(|result| result.success),
            "{} network events should all succeed",
            case.name
        );

        expect_direction_requests_ok(
            &harness,
            case,
            &format!("{}_handle_storm_then_call", case.name),
            Duration::from_secs(25),
        )
        .await;
        for direction in case.directions() {
            send_data_stream_bounded(
                &harness,
                direction.from_serial,
                direction.to_serial,
                &format!("{}-{}-handle-storm-then-stream", case.name, direction.name),
                Duration::from_secs(20),
            )
            .await;
        }
        expect_direction_requests_ok(
            &harness,
            case,
            &format!("{}_handle_storm_after_stream_call", case.name),
            Duration::from_secs(25),
        )
        .await;
        assert_eq!(
            harness.peer(case.mobile_serial).pending_count().await,
            0,
            "{} mobile event storm call/DataStream path should not leak pending requests",
            case.name
        );
        assert_eq!(
            harness.peer(case.server_serial).pending_count().await,
            0,
            "{} server event storm call/DataStream path should not leak pending requests",
            case.name
        );

        shutdown.cancel();
        reconciler
            .await
            .expect("network event reconciler should not panic");
    }
}
