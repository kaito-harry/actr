//! polyglot-echo streaming server — linked workload binary.
//!
//! Registers as `polyglot:EchoStreamService:1.0.0` under mock-actrix and
//! serves two control RPCs:
//!
//! - `EchoStreamService.StartServerStream`: push N DataChunks to the
//!   caller's registered `stream_id`.
//! - `EchoStreamService.SetupBidi`: register a server-side receive stream;
//!   echo each incoming chunk back to the caller's receive stream.
//!
//! Because this server runs as a **linked workload** (not a cdylib loaded by
//! `actr run`), `ctx` is a full `RuntimeContext` with working DataChunk
//! send / receive / register APIs.
//!
//! Inputs:
//!   --actr-toml <path>   runtime config (rendered from the shared template)

mod proto {
    include!(concat!(env!("OUT_DIR"), "/echo.rs"));
}

use std::env;
use std::path::PathBuf;

use actr_config::ConfigParser;
use actr_framework::{Context as RtContext, MessageDispatcher, Workload as RtWorkload};
use actr_hyper::Node;
use actr_protocol::{
    ActorResult, ActrError, ActrId, DataChunk, PayloadType, RpcEnvelope, RpcRequest,
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use prost::Message as ProstMessage;
use tracing::{info, warn};

use proto::{
    SetupBidiRequest, SetupBidiResponse, StartServerStreamRequest, StartServerStreamResponse,
};

// ── RpcRequest impl ──────────────────────────────────────────────────────────

impl RpcRequest for StartServerStreamRequest {
    type Response = StartServerStreamResponse;

    fn route_key() -> &'static str {
        "echo.EchoStreamService.StartServerStream"
    }

    fn payload_type() -> actr_protocol::PayloadType {
        PayloadType::RpcReliable
    }
}

impl RpcRequest for SetupBidiRequest {
    type Response = SetupBidiResponse;

    fn route_key() -> &'static str {
        "echo.EchoStreamService.SetupBidi"
    }

    fn payload_type() -> actr_protocol::PayloadType {
        PayloadType::RpcReliable
    }
}

// ── Workload ─────────────────────────────────────────────────────────────────

struct StreamServerWorkload;

#[async_trait]
impl RtWorkload for StreamServerWorkload {
    type Dispatcher = StreamServerDispatcher;
}

struct StreamServerDispatcher;

#[async_trait]
impl MessageDispatcher for StreamServerDispatcher {
    type Workload = StreamServerWorkload;

    async fn dispatch<C: RtContext>(
        _workload: &Self::Workload,
        envelope: RpcEnvelope,
        ctx: &C,
    ) -> ActorResult<Bytes> {
        match envelope.route_key.as_str() {
            "echo.EchoStreamService.StartServerStream" => {
                let payload = envelope.payload.as_ref().ok_or_else(|| {
                    ActrError::DecodeFailure("Missing payload in RpcEnvelope".to_string())
                })?;
                let req = StartServerStreamRequest::decode(&**payload).map_err(|e| {
                    ActrError::DecodeFailure(format!(
                        "Failed to decode StartServerStreamRequest: {e}"
                    ))
                })?;
                let resp = handle_start_server_stream(req, ctx).await?;
                Ok(Bytes::from(resp.encode_to_vec()))
            }
            "echo.EchoStreamService.SetupBidi" => {
                let payload = envelope.payload.as_ref().ok_or_else(|| {
                    ActrError::DecodeFailure("Missing payload in RpcEnvelope".to_string())
                })?;
                let req = SetupBidiRequest::decode(&**payload).map_err(|e| {
                    ActrError::DecodeFailure(format!("Failed to decode SetupBidiRequest: {e}"))
                })?;
                let resp = handle_setup_bidi(req, ctx).await?;
                Ok(Bytes::from(resp.encode_to_vec()))
            }
            route => Err(ActrError::UnknownRoute(route.to_string())),
        }
    }
}

// ── Handlers ─────────────────────────────────────────────────────────────────

/// Server-streaming: push `chunk_count` DataChunks to the caller's
/// `client_stream_id`.
async fn handle_start_server_stream<C: RtContext>(
    req: StartServerStreamRequest,
    ctx: &C,
) -> ActorResult<StartServerStreamResponse> {
    let caller_id = ctx.caller_id().cloned().ok_or_else(|| {
        ActrError::Internal("StartServerStream: caller_id is None".to_string())
    })?;

    info!(
        caller = ?caller_id,
        stream_id = %req.client_stream_id,
        chunk_count = req.chunk_count,
        text = %req.text,
        "server-stream: request received",
    );

    let dest = actr_framework::Dest::Peer(caller_id);
    let stream_id = req.client_stream_id.clone();
    let text = req.text.clone();
    let count = req.chunk_count;
    let ctx_clone = ctx.clone();

    // Spawn delivery so the control RPC returns immediately.
    tokio::spawn(async move {
        for seq in 0..count {
            let payload = format!("{text}:{seq}").into_bytes();
            let chunk = DataChunk {
                stream_id: stream_id.clone(),
                sequence: seq as u64,
                payload: Bytes::from(payload),
                metadata: vec![],
                timestamp_ms: None,
            };
            if let Err(e) = ctx_clone
                .send_data_chunk(&dest, chunk, PayloadType::StreamReliable)
                .await
            {
                warn!(seq, error = ?e, "server-stream: send_data_chunk failed");
                break;
            }
            info!(seq, "server-stream: chunk sent");
        }
        info!(count, "server-stream: all chunks delivered");
    });

    Ok(StartServerStreamResponse { ok: true })
}

/// Bidi: register a receiver on `server_rx_stream_id`; echo each incoming
/// DataChunk back to `client_rx_stream_id`.
async fn handle_setup_bidi<C: RtContext>(
    req: SetupBidiRequest,
    ctx: &C,
) -> ActorResult<SetupBidiResponse> {
    let caller_id = ctx.caller_id().cloned().ok_or_else(|| {
        ActrError::Internal("SetupBidi: caller_id is None".to_string())
    })?;

    info!(
        caller = ?caller_id,
        client_rx = %req.client_rx_stream_id,
        server_rx = %req.server_rx_stream_id,
        expected = req.expected_chunks,
        "bidi: setup request received",
    );

    let client_rx_id = req.client_rx_stream_id.clone();
    let ctx_clone = ctx.clone();
    let dest = actr_framework::Dest::Peer(caller_id);

    ctx.register_stream(
        req.server_rx_stream_id.clone(),
        move |chunk: DataChunk, sender_id: ActrId| {
            let ctx_inner = ctx_clone.clone();
            let dest_inner = dest.clone();
            let reply_stream_id = client_rx_id.clone();
            Box::pin(async move {
                let seq = chunk.sequence;
                info!(seq, from = ?sender_id, "bidi: chunk received, echoing");
                let echo_chunk = DataChunk {
                    stream_id: reply_stream_id,
                    sequence: seq,
                    payload: chunk.payload,
                    metadata: vec![],
                    timestamp_ms: None,
                };
                if let Err(e) = ctx_inner
                    .send_data_chunk(&dest_inner, echo_chunk, PayloadType::StreamReliable)
                    .await
                {
                    warn!(seq, error = ?e, "bidi: echo failed");
                }
                Ok(())
            })
        },
    )
    .await?;

    info!(server_rx = %req.server_rx_stream_id, "bidi: stream registered");
    Ok(SetupBidiResponse { ok: true })
}

// ── Arg parsing ──────────────────────────────────────────────────────────────

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

// ── Entry point ──────────────────────────────────────────────────────────────

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

    info!(
        config = %runtime_toml.display(),
        "polyglot-echo stream server starting",
    );

    let manifest = ConfigParser::from_manifest_file(&manifest_toml).with_context(|| {
        format!("failed to load manifest {}", manifest_toml.display())
    })?;
    let init = Node::from_config_file(&runtime_toml)
        .await
        .with_context(|| {
            format!("failed to load runtime config {}", runtime_toml.display())
        })?
        .with_actor_type(manifest.package.actr_type.clone());
    let ais_endpoint = init.runtime_config().ais_endpoint.to_string();

    let attached = init
        .link(StreamServerWorkload)
        .await
        .context("failed to link stream server workload")?;
    let registered = attached
        .register(&ais_endpoint)
        .await
        .context("failed to register with AIS")?;
    let actr_ref = registered.start().await.context("failed to start node")?;

    info!(actor_id = ?actr_ref.actor_id(), "stream server registered and ready");

    actr_ref
        .wait_for_ctrl_c_and_shutdown()
        .await
        .context("stream server shutdown error")?;
    Ok(())
}
