//! Polyglot Echo — Rust client driver.
//!
//! Mirrors the bindings/* link-only registration shape (no client-guest
//! package): construct a `Node<Init>` from a runtime config, attach a no-op
//! workload, register with mock-actrix's AIS, then drive a single typed
//! `EchoService.Echo` round-trip.
//!
//! Read by `run.sh --client rust`. Inputs:
//!   --actr-toml <path>   runtime config (rendered from the shared template)
//!   --service-type "<mfr>:<name>:<ver>"  triple to discover
//!   --scenario <echo|server-stream|bidi>  test scenario (default: echo)
//!   [message]            payload to echo (defaults to "polyglot-rust")

pub mod echo {
    include!(concat!(env!("OUT_DIR"), "/echo.rs"));
}

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actr_config::ConfigParser;
use actr_framework::{Context as RtContext, MessageDispatcher, Workload as RtWorkload};
use actr_hyper::Node;
use actr_protocol::{
    ActorResult, ActrError, ActrId, ActrType, DataChunk, PayloadType, RpcEnvelope, RpcRequest,
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use tracing::info;

use crate::echo::{
    EchoRequest, EchoResponse, SetupBidiRequest, SetupBidiResponse, StartServerStreamRequest,
    StartServerStreamResponse,
};

const STREAM_CHUNK_COUNT: u32 = 5;

impl RpcRequest for EchoRequest {
    type Response = EchoResponse;

    fn route_key() -> &'static str {
        "echo.EchoService.Echo"
    }

    fn payload_type() -> actr_protocol::PayloadType {
        actr_protocol::PayloadType::RpcReliable
    }
}

impl RpcRequest for StartServerStreamRequest {
    type Response = StartServerStreamResponse;

    fn route_key() -> &'static str {
        "echo.EchoStreamService.StartServerStream"
    }

    fn payload_type() -> actr_protocol::PayloadType {
        actr_protocol::PayloadType::RpcReliable
    }
}

impl RpcRequest for SetupBidiRequest {
    type Response = SetupBidiResponse;

    fn route_key() -> &'static str {
        "echo.EchoStreamService.SetupBidi"
    }

    fn payload_type() -> actr_protocol::PayloadType {
        actr_protocol::PayloadType::RpcReliable
    }
}

// Drivers issue outbound RPCs only; the dispatcher is a stub that rejects
// any inbound dispatch the framework might attempt.
struct LinkOnlyWorkload;

#[async_trait]
impl RtWorkload for LinkOnlyWorkload {
    type Dispatcher = LinkOnlyDispatcher;
}

struct LinkOnlyDispatcher;

#[async_trait]
impl MessageDispatcher for LinkOnlyDispatcher {
    type Workload = LinkOnlyWorkload;

    async fn dispatch<C: RtContext>(
        _workload: &Self::Workload,
        _envelope: RpcEnvelope,
        _ctx: &C,
    ) -> ActorResult<Bytes> {
        Err(ActrError::NotImplemented(
            "polyglot-echo Rust driver does not host inbound RPC".to_string(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Echo,
    ServerStream,
    Bidi,
}

struct DriverArgs {
    runtime_toml: PathBuf,
    manifest_toml: PathBuf,
    service_type: ActrType,
    message: String,
    scenario: Scenario,
}

fn parse_args() -> Result<DriverArgs> {
    let mut args = env::args().skip(1);
    let mut runtime_toml: Option<PathBuf> = None;
    let mut manifest_toml: Option<PathBuf> = None;
    let mut service_type: Option<String> = None;
    let mut message: Option<String> = None;
    let mut scenario = Scenario::Echo;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--actr-toml" => {
                runtime_toml = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--actr-toml needs value"))?,
                ));
            }
            "--manifest-toml" => {
                manifest_toml = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--manifest-toml needs value"))?,
                ));
            }
            "--service-type" => {
                service_type = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--service-type needs value"))?,
                );
            }
            "--message" => {
                message = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--message needs value"))?,
                );
            }
            "--scenario" => {
                let s = args
                    .next()
                    .ok_or_else(|| anyhow!("--scenario needs value"))?;
                scenario = match s.as_str() {
                    "echo" => Scenario::Echo,
                    "server-stream" => Scenario::ServerStream,
                    "bidi" => Scenario::Bidi,
                    other => bail!("unknown --scenario value: {other} (expected echo|server-stream|bidi)"),
                };
            }
            other if !other.starts_with("--") && message.is_none() => {
                message = Some(other.to_string());
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    let runtime_toml = runtime_toml.ok_or_else(|| anyhow!("missing --actr-toml"))?;
    let manifest_toml = manifest_toml.unwrap_or_else(|| {
        // Default to the manifest.toml shipped next to the driver crate
        // so callers don't have to repeat themselves; --manifest-toml
        // overrides for tests that want to swap identity.
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("manifest.toml")
    });
    let service_type_str = service_type.ok_or_else(|| anyhow!("missing --service-type"))?;
    let parts: Vec<&str> = service_type_str.splitn(3, ':').collect();
    if parts.len() != 3 {
        bail!("--service-type must be 'manufacturer:name:version', got {service_type_str}");
    }
    let service_type = ActrType {
        manufacturer: parts[0].to_string(),
        name: parts[1].to_string(),
        version: parts[2].to_string(),
    };
    let message = message.unwrap_or_else(|| "polyglot-rust".to_string());

    Ok(DriverArgs {
        runtime_toml,
        manifest_toml,
        service_type,
        message,
        scenario,
    })
}

async fn scenario_echo(
    actr_ref: &actr_hyper::ActrRef,
    target: ActrId,
    message: &str,
) -> Result<()> {
    let request = EchoRequest {
        message: message.to_string(),
    };
    let response: EchoResponse = actr_ref
        .call_remote(target, request)
        .await
        .map_err(|e| anyhow!("Echo RPC failed: {e}"))?;

    println!("[Received reply] {}", response.reply);
    info!(reply = %response.reply, "echo reply received");
    Ok(())
}

async fn scenario_server_stream(
    actr_ref: &actr_hyper::ActrRef,
    target: ActrId,
    message: &str,
) -> Result<()> {
    let stream_id = format!("server-stream-{}", uuid::Uuid::new_v4());
    let chunk_count = STREAM_CHUNK_COUNT;

    // Register a callback BEFORE the control RPC so no chunk is missed.
    let received = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let received_clone = received.clone();
    let done_tx = Arc::new(tokio::sync::Notify::new());
    let done_tx_clone = done_tx.clone();
    let expected = chunk_count as usize;

    let ctx = actr_ref.app_context().await;
    ctx.register_stream(
        stream_id.clone(),
        move |chunk: DataChunk, sender_id: ActrId| {
            let received = received_clone.clone();
            let done_tx = done_tx_clone.clone();
            Box::pin(async move {
                let text = String::from_utf8_lossy(&chunk.payload).to_string();
                info!(
                    seq = chunk.sequence,
                    from = ?sender_id,
                    text = %text,
                    "server-stream chunk received",
                );
                let mut guard = received.lock().await;
                guard.push(text);
                if guard.len() >= expected {
                    done_tx.notify_one();
                }
                Ok(())
            })
        },
    )
    .await
    .context("register_stream failed")?;

    info!(
        stream_id = %stream_id,
        chunk_count,
        "sending StartServerStream RPC",
    );

    let resp: StartServerStreamResponse = actr_ref
        .call_remote(
            target,
            StartServerStreamRequest {
                client_stream_id: stream_id.clone(),
                chunk_count,
                text: message.to_string(),
            },
        )
        .await
        .map_err(|e| anyhow!("StartServerStream RPC failed: {e}"))?;

    if !resp.ok {
        bail!("server returned ok=false for StartServerStream");
    }

    // Wait for all chunks with a timeout.
    let timeout = Duration::from_secs(15);
    tokio::time::timeout(timeout, done_tx.notified())
        .await
        .map_err(|_| {
            let count = {
                // best-effort sync read — we're in error path
                received.try_lock().map(|g| g.len()).unwrap_or(0)
            };
            anyhow!(
                "server-stream: timed out waiting for chunks; received {count}/{chunk_count}"
            )
        })?;

    let chunks = received.lock().await;
    let n = chunks.len();
    if n < chunk_count as usize {
        bail!("server-stream: expected {chunk_count} chunks, received {n}");
    }
    info!(n, "all server-stream chunks received");
    println!("[server-stream] received {n}/{chunk_count} chunks");

    // Unregister the callback.
    ctx.unregister_stream(&stream_id)
        .await
        .context("unregister_stream failed")?;

    Ok(())
}

async fn scenario_bidi(
    actr_ref: &actr_hyper::ActrRef,
    target: ActrId,
    message: &str,
) -> Result<()> {
    let chunk_count = STREAM_CHUNK_COUNT;
    let client_rx_id = format!("bidi-client-rx-{}", uuid::Uuid::new_v4());
    let server_rx_id = format!("bidi-server-rx-{}", uuid::Uuid::new_v4());

    // Register receiver for echoed chunks.
    let received = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let received_clone = received.clone();
    let done_tx = Arc::new(tokio::sync::Notify::new());
    let done_tx_clone = done_tx.clone();
    let expected = chunk_count as usize;

    let ctx = actr_ref.app_context().await;
    ctx.register_stream(
        client_rx_id.clone(),
        move |chunk: DataChunk, sender_id: ActrId| {
            let received = received_clone.clone();
            let done_tx = done_tx_clone.clone();
            Box::pin(async move {
                let text = String::from_utf8_lossy(&chunk.payload).to_string();
                info!(
                    seq = chunk.sequence,
                    from = ?sender_id,
                    text = %text,
                    "bidi echo chunk received",
                );
                let mut guard = received.lock().await;
                guard.push(text);
                if guard.len() >= expected {
                    done_tx.notify_one();
                }
                Ok(())
            })
        },
    )
    .await
    .context("register_stream for client_rx failed")?;

    // Tell server about the bidi setup.
    let resp: SetupBidiResponse = actr_ref
        .call_remote(
            target.clone(),
            SetupBidiRequest {
                client_rx_stream_id: client_rx_id.clone(),
                server_rx_stream_id: server_rx_id.clone(),
                expected_chunks: chunk_count,
            },
        )
        .await
        .map_err(|e| anyhow!("SetupBidi RPC failed: {e}"))?;

    if !resp.ok {
        bail!("server returned ok=false for SetupBidi");
    }

    // Send N chunks to the server.
    let dest = actr_framework::Dest::Peer(target.clone());
    for seq in 0..chunk_count {
        let payload = format!("{message}:{seq}").into_bytes();
        let chunk = DataChunk {
            stream_id: server_rx_id.clone(),
            sequence: seq as u64,
            payload: Bytes::from(payload),
            metadata: vec![],
            timestamp_ms: None,
        };
        ctx.send_data_chunk(&dest, chunk, PayloadType::StreamReliable)
            .await
            .with_context(|| format!("send_data_chunk seq={seq} failed"))?;
        info!(seq, "bidi: sent chunk to server");
    }

    // Wait for all echoed chunks.
    let timeout = Duration::from_secs(15);
    tokio::time::timeout(timeout, done_tx.notified())
        .await
        .map_err(|_| {
            let count = received.try_lock().map(|g| g.len()).unwrap_or(0);
            anyhow!("bidi: timed out waiting for echoes; received {count}/{chunk_count}")
        })?;

    let chunks = received.lock().await;
    let n = chunks.len();
    if n < chunk_count as usize {
        bail!("bidi: expected {chunk_count} echo chunks, received {n}");
    }
    info!(n, "all bidi echo chunks received");
    println!("[bidi] received {n}/{chunk_count} echo chunks");

    // Clean up.
    ctx.unregister_stream(&client_rx_id)
        .await
        .context("unregister_stream client_rx failed")?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args()?;
    info!(
        config = %args.runtime_toml.display(),
        service_type = ?args.service_type,
        message = %args.message,
        scenario = ?args.scenario,
        "polyglot-echo rust driver starting",
    );

    let manifest = ConfigParser::from_manifest_file(&args.manifest_toml).with_context(|| {
        format!(
            "failed to load driver manifest {}",
            args.manifest_toml.display()
        )
    })?;
    let init = Node::from_config_with_package(&args.runtime_toml, manifest.package.clone())
        .await
        .with_context(|| {
            format!(
                "failed to load runtime config {}",
                args.runtime_toml.display()
            )
        })?;
    let ais_endpoint = init.runtime_config().ais_endpoint.to_string();

    let attached = init
        .link(LinkOnlyWorkload)
        .await
        .context("failed to link no-op workload")?;
    let registered = attached
        .register(&ais_endpoint)
        .await
        .context("failed to register with AIS")?;
    let actr_ref = registered.start().await.context("failed to start node")?;

    info!(actor_id = ?actr_ref.actor_id(), "rust driver registered");

    // Determine which service to discover (echo or stream).
    let target_type = match args.scenario {
        Scenario::Echo => args.service_type.clone(),
        Scenario::ServerStream | Scenario::Bidi => {
            // The stream service has the same manufacturer/version but
            // a different name — EchoStreamService vs EchoService.
            ActrType {
                manufacturer: args.service_type.manufacturer.clone(),
                name: "EchoStreamService".to_string(),
                version: args.service_type.version.clone(),
            }
        }
    };

    let mut targets = actr_ref
        .discover_route_candidates(&target_type, 1)
        .await
        .context("discover_route_candidates failed")?;
    let target = targets
        .pop()
        .ok_or_else(|| anyhow!("no candidates discovered for {:?}", target_type))?;
    info!(?target, "discovered service instance");

    match args.scenario {
        Scenario::Echo => scenario_echo(&actr_ref, target, &args.message).await?,
        Scenario::ServerStream => {
            scenario_server_stream(&actr_ref, target, &args.message).await?
        }
        Scenario::Bidi => scenario_bidi(&actr_ref, target, &args.message).await?,
    }

    actr_ref.shutdown();
    tokio::time::timeout(Duration::from_secs(5), actr_ref.wait_for_shutdown())
        .await
        .ok();
    Ok(())
}
