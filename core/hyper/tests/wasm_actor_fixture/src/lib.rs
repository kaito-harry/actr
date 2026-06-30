//! WASM actor end-to-end test fixture
//!
//! Verification target: a real wasm32 actor that implements the `Context`
//! trait via `actr-framework` guest module, plus a panic route used by
//! Phase 1 Commit 6's regression suite to validate guest-trap behaviour
//! after a host suspension point.
//!
//! # Test protocol
//!
//! - route `test/echo`: returns the inbound payload as-is without
//!   outbound IO.
//! - route `test/double`: decodes the payload as a 4-byte little-endian
//!   i32 value `x`, calls `ctx.call_raw(self_id, "test/double_impl", payload)`,
//!   and returns whatever the host responds with. Used by the async
//!   round-trip / tick-probe / dispatch tests.
//! - route `test/boom-after-await`: same control flow as `test/double` up
//!   to the `ctx.call_raw` await, then panics. Used to verify that a
//!   guest panic after a real suspension surfaces as a wasmtime trap
//!   rather than corrupting the dispatch result.
//!
//! All other routes surface `ActrError::UnknownRoute(route_key)`,
//! exercising guest→host structured error propagation.

use actr_framework::{entry, Context, MessageDispatcher, Workload};
use actr_protocol::{ActorResult, ActrError, RpcEnvelope};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::UNIX_EPOCH;

async fn record_hook<C: Context>(ctx: &C, name: &'static str) {
    record_hook_value(ctx, name.to_string()).await;
}

async fn record_hook_value<C: Context>(ctx: &C, value: String) {
    let _ = ctx
        .call_raw(ctx.self_id(), "test/record_hook", Bytes::from(value))
        .await;
}

fn relayed_label(event: &actr_framework::PeerEvent) -> &'static str {
    match event.relayed {
        Some(true) => "true",
        Some(false) => "false",
        None => "none",
    }
}

fn status_label(event: &actr_framework::PeerEvent) -> &'static str {
    match event.status {
        Some(actr_framework::WebRtcPeerStatus::Idle) => "idle",
        Some(actr_framework::WebRtcPeerStatus::Connecting) => "connecting",
        Some(actr_framework::WebRtcPeerStatus::Connected) => "connected",
        Some(actr_framework::WebRtcPeerStatus::Recovering) => "recovering",
        None => "none",
    }
}

async fn record_peer_hook<C: Context>(
    ctx: &C,
    name: &'static str,
    event: &actr_framework::PeerEvent,
) {
    record_hook_value(
        ctx,
        format!(
            "{name}:peer={}:relayed={}:status={}",
            event.peer.serial_number,
            relayed_label(event),
            status_label(event),
        ),
    )
    .await;
}

// ── Workload ──────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct DoubleActor;

pub struct DoubleDispatcher;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MessageDispatcher for DoubleDispatcher {
    type Workload = DoubleActor;

    async fn dispatch<C: Context>(
        _workload: &Self::Workload,
        envelope: RpcEnvelope,
        ctx: &C,
    ) -> ActorResult<Bytes> {
        match envelope.route_key.as_str() {
            "test/echo" => Ok(Bytes::from(envelope.payload.unwrap_or_default().to_vec())),
            "test/record_hook" => Ok(Bytes::from(envelope.payload.unwrap_or_default().to_vec())),
            "test/double" => {
                // payload: 4-byte little-endian i32 (RpcEnvelope.payload is optional)
                let payload = envelope.payload.unwrap_or_default();
                if payload.len() < 4 {
                    return Err(ActrError::InvalidArgument("payload too short".into()));
                }
                let x = i32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);

                // Call ctx.call_raw() -> triggers the host-import await path.
                let target = ctx.self_id().clone();
                let resp = ctx
                    .call_raw(
                        &target,
                        "test/double_impl",
                        Bytes::from(x.to_le_bytes().to_vec()),
                    )
                    .await?;

                Ok(resp)
            }
            "test/boom-after-await" => {
                // Exercise Phase 0.5 spike Test 8: perform a host-side
                // await, then panic once control returns to the guest.
                // The test host routes "test/double_impl" back to a stub
                // reply; the actual content is discarded.
                let target = ctx.self_id().clone();
                let payload = envelope.payload.unwrap_or_default();
                let _ = ctx.call_raw(&target, "test/double_impl", payload).await?;
                panic!("fixture: intentional post-await panic for trap test");
            }
            _ => Err(ActrError::UnknownRoute(envelope.route_key)),
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl Workload for DoubleActor {
    type Dispatcher = DoubleDispatcher;

    async fn on_start<C: Context>(&self, ctx: &C) -> ActorResult<()> {
        if ctx.request_id() == "lifecycle:on_start" {
            return Err(ActrError::Internal(
                "fixture lifecycle on_start failed".to_string(),
            ));
        }
        Ok(())
    }

    async fn on_ready<C: Context>(&self, ctx: &C) -> ActorResult<()> {
        record_hook(ctx, "on_ready").await;
        Ok(())
    }

    async fn on_stop<C: Context>(&self, ctx: &C) -> ActorResult<()> {
        record_hook(ctx, "on_stop").await;
        Ok(())
    }

    async fn on_signaling_connecting<C: Context>(&self, ctx: Option<&C>) {
        if let Some(ctx) = ctx {
            record_hook(ctx, "on_signaling_connecting").await;
        }
    }

    async fn on_signaling_connected<C: Context>(&self, ctx: Option<&C>) {
        if let Some(ctx) = ctx {
            record_hook(ctx, "on_signaling_connected").await;
        }
    }

    async fn on_signaling_disconnected<C: Context>(&self, ctx: &C) {
        record_hook(ctx, "on_signaling_disconnected").await;
    }

    async fn on_websocket_connecting<C: Context>(
        &self,
        ctx: &C,
        event: &actr_framework::PeerEvent,
    ) {
        record_peer_hook(ctx, "on_websocket_connecting", event).await;
    }

    async fn on_websocket_connected<C: Context>(
        &self,
        ctx: &C,
        event: &actr_framework::PeerEvent,
    ) {
        record_peer_hook(ctx, "on_websocket_connected", event).await;
    }

    async fn on_websocket_disconnected<C: Context>(
        &self,
        ctx: &C,
        event: &actr_framework::PeerEvent,
    ) {
        record_peer_hook(ctx, "on_websocket_disconnected", event).await;
    }

    async fn on_webrtc_connecting<C: Context>(
        &self,
        ctx: &C,
        event: &actr_framework::PeerEvent,
    ) {
        record_peer_hook(ctx, "on_webrtc_connecting", event).await;
    }

    async fn on_webrtc_connected<C: Context>(&self, ctx: &C, event: &actr_framework::PeerEvent) {
        record_peer_hook(ctx, "on_webrtc_connected", event).await;
    }

    async fn on_webrtc_disconnected<C: Context>(
        &self,
        ctx: &C,
        event: &actr_framework::PeerEvent,
    ) {
        record_peer_hook(ctx, "on_webrtc_disconnected", event).await;
    }

    async fn on_credential_renewed<C: Context>(
        &self,
        ctx: &C,
        event: &actr_framework::CredentialEvent,
    ) {
        let secs = event
            .new_expiry
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        record_hook_value(ctx, format!("on_credential_renewed:expiry={secs}")).await;
    }

    async fn on_credential_expiring<C: Context>(
        &self,
        ctx: &C,
        event: &actr_framework::CredentialEvent,
    ) {
        let secs = event
            .new_expiry
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        record_hook_value(ctx, format!("on_credential_expiring:expiry={secs}")).await;
    }

    async fn on_mailbox_backpressure<C: Context>(
        &self,
        ctx: &C,
        event: &actr_framework::BackpressureEvent,
    ) {
        record_hook_value(
            ctx,
            format!(
                "on_mailbox_backpressure:queue_len={}:threshold={}",
                event.queue_len, event.threshold
            ),
        )
        .await;
    }
}

// ── ABI exports (generated by entry! macro) ──────────────────────────────────

entry!(DoubleActor);
