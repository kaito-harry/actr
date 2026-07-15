//! M4 compatibility guard: a frozen `actr:workload@0.1.0` sync-lift package
//! must keep loading AND dispatching on the wasmtime 46 dual-world host,
//! routed to the serial (V1) execution path.
//!
//! Background: M4 introduces the `actr:workload@0.2.0` async world alongside
//! the existing 0.1.0 world. `WasmHost::instantiate` probes which world a
//! component exports and picks the matching kernel. This test is the V1
//! positive control: the frozen 0.1.0 package must (a) compile as a
//! Component (not hit the async-lift rejection that
//! `legacy_asynclift_guest.wasm` triggers) and (b) round-trip an inbound
//! `test/echo` dispatch through the serial path.
//!
//! `fixtures/v1_synclift_guest.wasm` is a frozen artifact of the
//! `wasm_actor_fixture` guest built by the 0.1.0 (sync-lift) framework SDK,
//! captured BEFORE the guest SDK was switched to the 0.2.0 async world.

#![cfg(feature = "wasm-engine")]

use actr_hyper::wasm::WasmHost;
use actr_protocol::{ActrId, ActrType, Realm, RpcEnvelope, prost::Message as ProstMessage};

/// A genuine 0.1.0 sync-lift Component built by the (pre-M4) SDK.
const V1_SYNCLIFT_GUEST: &[u8] = include_bytes!("fixtures/v1_synclift_guest.wasm");

#[test]
fn v1_synclift_package_loads_on_wasmtime_46_host() {
    // The 0.1.0 sync-lift package must compile as a Component without the
    // async-lift rejection that `legacy_asynclift_guest.wasm` triggers.
    WasmHost::compile(V1_SYNCLIFT_GUEST)
        .expect("frozen actr:workload@0.1.0 sync-lift package must load on the wasmtime 46 host");
}

#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v1_synclift_package_dispatches_through_serial_path() {
    use actr_hyper::test_support::instantiate_wasm_workload;
    use actr_hyper::workload::{HostAbiFn, HostOperationResult, InvocationContext};
    use std::sync::Arc;

    let host = WasmHost::compile(V1_SYNCLIFT_GUEST).expect("compile frozen v1 package");
    // instantiate probes the world; a 0.1.0 export routes to WasmKernel::V1.
    let mut wl = instantiate_wasm_workload(&host)
        .await
        .expect("instantiate frozen v1 package");

    let ctx = InvocationContext {
        self_id: ActrId {
            realm: Realm { realm_id: 1 },
            serial_number: 1,
            r#type: ActrType {
                manufacturer: "test".to_string(),
                name: "fixture".to_string(),
                version: "0.1.0".to_string(),
            },
        },
        caller_id: None,
        request_id: "v1-compat".to_string(),
    };
    // `test/echo` returns the payload as-is without any outbound host call.
    let bridge: HostAbiFn = Arc::new(|_| Box::pin(async move { HostOperationResult::Error(-1) }));
    let payload = b"hello-v1-serial".to_vec();
    let req = RpcEnvelope {
        route_key: "test/echo".to_string(),
        payload: Some(payload.clone().into()),
        request_id: "v1-compat".to_string(),
        direction: Some(actr_protocol::Direction::Request as i32),
        ..Default::default()
    }
    .encode_to_vec();

    let reply = wl
        .handle(&req, ctx, &bridge)
        .await
        .expect("frozen v1 package must round-trip test/echo on the serial path");
    assert_eq!(
        reply, payload,
        "V1 serial dispatch must echo the payload unchanged"
    );
}
