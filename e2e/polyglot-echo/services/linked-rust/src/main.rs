//! polyglot-echo Echo server — linked Rust workload binary.
//!
//! Same wire shape as the cdylib path (registers
//! `polyglot:EchoService:1.0.0`, serves `echo.EchoService.Echo`), but
//! built as a normal binary that links actr-hyper directly instead of
//! being dlopened by `actr run`.  This validates the linked-workload
//! transport path against the same client drivers, with no client
//! changes.
//!
//! The actual echo business logic lives in `echo_service.rs` (5 lines
//! of derive-y code).  Everything else here is dispatcher boilerplate
//! that will eventually be replaced by `actr gen -l rust` output once
//! the linked path supports actr-cli codegen end-to-end.

mod proto {
    include!(concat!(env!("OUT_DIR"), "/echo.rs"));
}
mod echo_service;

use std::env;
use std::path::PathBuf;

use actr_config::ConfigParser;
use actr_framework::{Context as RtContext, MessageDispatcher, Workload as RtWorkload};
use actr_hyper::Node;
use actr_protocol::{ActorResult, ActrError, PayloadType, RpcEnvelope, RpcRequest};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use prost::Message as ProstMessage;
use tracing::info;

use proto::{EchoRequest, EchoResponse};

// ── RpcRequest impl ──────────────────────────────────────────────────────────

impl RpcRequest for EchoRequest {
    type Response = EchoResponse;

    fn route_key() -> &'static str {
        "echo.EchoService.Echo"
    }

    fn payload_type() -> PayloadType {
        PayloadType::RpcReliable
    }
}

// ── Workload + Dispatcher (template-derivable) ───────────────────────────────

struct EchoWorkload;

#[async_trait]
impl RtWorkload for EchoWorkload {
    type Dispatcher = EchoDispatcher;
}

struct EchoDispatcher;

#[async_trait]
impl MessageDispatcher for EchoDispatcher {
    type Workload = EchoWorkload;

    async fn dispatch<C: RtContext>(
        _workload: &Self::Workload,
        envelope: RpcEnvelope,
        _ctx: &C,
    ) -> ActorResult<Bytes> {
        match envelope.route_key.as_str() {
            "echo.EchoService.Echo" => {
                let payload = envelope.payload.as_ref().ok_or_else(|| {
                    ActrError::DecodeFailure("Missing payload in RpcEnvelope".to_string())
                })?;
                let req = EchoRequest::decode(&**payload).map_err(|e| {
                    ActrError::DecodeFailure(format!("Failed to decode EchoRequest: {e}"))
                })?;
                let resp = echo_service::handle_echo(req);
                Ok(Bytes::from(resp.encode_to_vec()))
            }
            route => Err(ActrError::UnknownRoute(route.to_string())),
        }
    }
}

// ── Arg parsing + entry ──────────────────────────────────────────────────────

fn parse_actr_toml() -> Result<PathBuf> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--actr-toml" {
            return args
                .next()
                .map(PathBuf::from)
                .ok_or_else(|| anyhow!("--actr-toml needs value"));
        }
        if let Some(path) = arg.strip_prefix("--actr-toml=") {
            return Ok(PathBuf::from(path));
        }
    }
    bail!("missing --actr-toml argument")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let runtime_toml = parse_actr_toml()?;
    let manifest_toml = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("manifest.toml");

    info!(config = %runtime_toml.display(), "linked-rust echo server starting");

    let manifest = ConfigParser::from_manifest_file(&manifest_toml)
        .with_context(|| format!("failed to load manifest {}", manifest_toml.display()))?;
    let init = Node::from_config_file(&runtime_toml)
        .await
        .with_context(|| format!("failed to load runtime config {}", runtime_toml.display()))?
        .with_actor_type(manifest.package.actr_type.clone());
    let ais_endpoint = init.runtime_config().ais_endpoint.to_string();

    let attached = init
        .link(EchoWorkload)
        .await
        .context("failed to link echo workload")?;
    let registered = attached
        .register(&ais_endpoint)
        .await
        .context("failed to register with AIS")?;
    let actr_ref = registered.start().await.context("failed to start node")?;

    info!(actor_id = ?actr_ref.actor_id(), "linked-rust echo server registered");

    actr_ref
        .wait_for_ctrl_c_and_shutdown()
        .await
        .context("server shutdown error")?;
    Ok(())
}
