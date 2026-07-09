//! Echo Client Guest WASM for Web — unified source (Option U γ-unified Phase 6c).
//!
//! Implements the transparent proxy for `echo.EchoService.Echo`:
//! 1. Discover `acme:EchoService:0.1.0` (cached after first success)
//! 2. Call `echo.EchoService.Echo` on the remote server
//! 3. Return the reply to the host
//! 4. On failure with a cached target, clear cache and retry once
//!
//! One source tree, two wasm32 ABIs — selected by Cargo feature:
//!
//!   * **default**  → `wasm32-wasip2` + wasm-component-ld (Component Model).
//!   * **`--features web`** → `wasm32-unknown-unknown` + wasm-pack
//!     (wasm-bindgen + `actr-web-abi`).
//!
//! `actr_framework::entry!` inspects `cfg(feature = "web")` at expansion
//! time and emits the matching bootstrap (`Guest` impl vs
//! `#[wasm_bindgen(start)]`). The handler / dispatcher business logic
//! below is identical across both branches — every out-of-process call
//! funnels through the `Context` trait, whose web and wasip2 impls both
//! thread `request_id` through the host bridge.
//!
//! `cargo build --target wasm32-wasip2` produces a Component Model binary
//! consumed by the native wasmtime host (see core/hyper). Browser-side
//! consumption goes through the `--features web` build instead — the
//! Component Model + jco transpile path was deleted in Option U Phase 8.

pub mod echo {
    include!(concat!(env!("OUT_DIR"), "/echo.rs"));
}

use std::cell::RefCell;

use actr_framework::{Context, Dest, MessageDispatcher, Workload, entry};
use actr_protocol::{ActorResult, ActrError, ActrId, ActrType, RpcEnvelope, RpcRequest};
use async_trait::async_trait;
use bytes::Bytes;
use prost::Message as ProstMessage;

use echo::{EchoRequest, EchoResponse};

const ECHO_SERVICE_MANUFACTURER: &str = "acme";
const ECHO_SERVICE_NAME: &str = "EchoService";
const ECHO_SERVICE_VERSION: &str = "0.1.0";

impl RpcRequest for EchoRequest {
    type Response = EchoResponse;

    fn route_key() -> &'static str {
        "echo.EchoService.Echo"
    }

    fn payload_type() -> actr_protocol::PayloadType {
        actr_protocol::PayloadType::RpcReliable
    }
}

/// Client guest workload — holds cached server `ActrId`.
///
/// `Clone` is required by `actr_framework::web::WebWorkloadAdapter`'s
/// `W: Clone` bound under the `web` feature. `RefCell<Option<ActrId>>`
/// clones by cloning the currently-held value (a single `prost`-derived
/// `ActrId`); the cache is per-module anyway, so clone semantics
/// ("snapshot then independent") are fine.
#[derive(Clone, Default)]
pub struct ClientGuestWorkload {
    cached_server_id: RefCell<Option<ActrId>>,
}

// Safety: cdylib guest is single-threaded (host serializes actr_handle calls).
unsafe impl Send for ClientGuestWorkload {}
unsafe impl Sync for ClientGuestWorkload {}

pub struct ClientGuestDispatcher;

// See server-guest note: `?Send` on wasm32 so the impl's async futures
// match the `MessageDispatcher` trait's `async_trait(?Send)` form.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl MessageDispatcher for ClientGuestDispatcher {
    type Workload = ClientGuestWorkload;

    async fn dispatch<C: Context>(
        workload: &Self::Workload,
        envelope: RpcEnvelope,
        ctx: &C,
    ) -> ActorResult<Bytes> {
        match envelope.route_key.as_str() {
            "echo.EchoService.Echo" => {
                let payload = envelope.payload.unwrap_or_default();
                let req = EchoRequest::decode(payload.as_ref())
                    .map_err(|e| ActrError::DecodeFailure(format!("decode EchoRequest: {e}")))?;

                let resp = proxy_echo(workload, ctx, req).await?;
                Ok(Bytes::from(resp.encode_to_vec()))
            }
            _ => Err(ActrError::UnknownRoute(envelope.route_key)),
        }
    }
}

async fn proxy_echo<C: Context>(
    workload: &ClientGuestWorkload,
    ctx: &C,
    req: EchoRequest,
) -> ActorResult<EchoResponse> {
    let server_id = resolve_server(workload, ctx).await?;

    let echo_req = EchoRequest {
        message: req.message.clone(),
    };

    match ctx.call(&Dest::Peer(server_id.clone()), echo_req).await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            // Cache miss: clear cached id and retry once with fresh discovery.
            *workload.cached_server_id.borrow_mut() = None;
            let fresh_id = discover_server(ctx).await?;
            *workload.cached_server_id.borrow_mut() = Some(fresh_id.clone());

            let echo_req2 = EchoRequest {
                message: req.message.clone(),
            };
            ctx.call(&Dest::Peer(fresh_id), echo_req2)
                .await
                .map_err(|e2| {
                    ActrError::Internal(format!(
                        "retry after cache clear failed: {e2} (original: {e})"
                    ))
                })
        }
    }
}

async fn resolve_server<C: Context>(
    workload: &ClientGuestWorkload,
    ctx: &C,
) -> ActorResult<ActrId> {
    if let Some(id) = workload.cached_server_id.borrow().clone() {
        return Ok(id);
    }
    let id = discover_server(ctx).await?;
    *workload.cached_server_id.borrow_mut() = Some(id.clone());
    Ok(id)
}

async fn discover_server<C: Context>(ctx: &C) -> ActorResult<ActrId> {
    let target_type = ActrType {
        manufacturer: ECHO_SERVICE_MANUFACTURER.to_string(),
        name: ECHO_SERVICE_NAME.to_string(),
        version: ECHO_SERVICE_VERSION.to_string(),
    };
    ctx.discover_route_candidate(&target_type).await
}

impl Workload for ClientGuestWorkload {
    type Dispatcher = ClientGuestDispatcher;
}

entry!(ClientGuestWorkload);
