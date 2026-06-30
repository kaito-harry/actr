//! Dynclib actor fixture for e2e tests
//!
//! A simple cdylib actor implementing:
//! - "test/double": reads i32 from payload, calls ctx.call_raw() to get x*2
//! - "test/echo": returns payload as-is (no outbound calls)
//! - unknown route: returns error

use actr_framework::{Context, MessageDispatcher, Workload, entry};
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

#[derive(Default)]
pub struct DoubleActor;

pub struct DoubleDispatcher;

#[async_trait]
impl MessageDispatcher for DoubleDispatcher {
    type Workload = DoubleActor;

    async fn dispatch<C: Context>(
        _workload: &Self::Workload,
        envelope: RpcEnvelope,
        ctx: &C,
    ) -> ActorResult<Bytes> {
        match envelope.route_key.as_str() {
            "test/double" => {
                let payload = envelope.payload.unwrap_or_default();
                if payload.len() < 4 {
                    return Err(ActrError::InvalidArgument("payload too short".into()));
                }
                let x = i32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);

                // Call ctx.call_raw() -> triggers vtable.call trampoline
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
            "test/echo" => {
                let payload = envelope.payload.unwrap_or_default();
                Ok(Bytes::from(payload))
            }
            "test/record_hook" => {
                let payload = envelope.payload.unwrap_or_default();
                Ok(Bytes::from(payload))
            }
            _ => Err(ActrError::UnknownRoute(envelope.route_key)),
        }
    }
}

#[async_trait]
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

entry!(DoubleActor);
