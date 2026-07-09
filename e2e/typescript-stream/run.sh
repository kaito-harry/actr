#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUN_ID="${ACTR_E2E_RUN_ID:-$(date +%Y%m%d-%H%M%S)-$$}"
RUN_ROOT="${ACTR_E2E_RUN_ROOT:-$REPO_ROOT/target/e2e-cache/typescript-stream/$RUN_ID}"
LOG_DIR="$RUN_ROOT/logs"
REALM_ID="${ACTR_E2E_REALM_ID:-2368266035}"
REALM_SECRET="${ACTR_E2E_REALM_SECRET:-typescript-stream-e2e-secret}"
MANUFACTURER="demo1"

ACTR_BIN="${ACTR_BIN:-$REPO_ROOT/target/release/actr}"
MOCK_ACTRIX_BIN="${MOCK_ACTRIX_BIN:-$REPO_ROOT/target/release/mock-actrix}"

MOCK_PID=""
ECHO_PID=""
RELAY_PID=""

dump_log_tail() {
  local file="$1"
  local lines="${2:-300}"
  if [ ! -f "$file" ]; then
    printf '\n--- %s missing ---\n' "$file" >&2
    return
  fi

  printf '\n--- %s (last %s lines) ---\n' "$file" "$lines" >&2
  tail -n "$lines" "$file" | REALM_SECRET="$REALM_SECRET" python3 -c '
import os
import sys

secret = os.environ.get("REALM_SECRET", "")
data = sys.stdin.read()
if secret:
    data = data.replace(secret, "[redacted]")
sys.stderr.write(data)
' || true
}

dump_failure_logs() {
  echo "E2E failed. Logs are in: $LOG_DIR" >&2
  dump_log_tail "$LOG_DIR/mock-actrix.log" 300
  dump_log_tail "$LOG_DIR/echo-run.log" 400
  dump_log_tail "$LOG_DIR/relay-run.log" 400
  dump_log_tail "$LOG_DIR/client-run.log" 400
}

cleanup() {
  local status=$?
  for pid in "$ECHO_PID" "$RELAY_PID" "$MOCK_PID"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait "$ECHO_PID" "$RELAY_PID" "$MOCK_PID" 2>/dev/null || true
  if [ "$status" -ne 0 ]; then
    dump_failure_logs
  fi
}
trap cleanup EXIT

log() {
  printf '\n==> %s\n' "$*"
}

run_logged() {
  local name="$1"
  shift
  log "$name"
  "$@" 2>&1 | tee "$LOG_DIR/${name//[^A-Za-z0-9_.-]/_}.log"
}

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required tool: $1" >&2
    exit 1
  fi
}

wait_http() {
  local url="$1"
  for _ in $(seq 1 100); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "Timed out waiting for $url" >&2
  return 1
}

wait_log() {
  local file="$1"
  local needle="$2"
  for _ in $(seq 1 120); do
    if [ -f "$file" ] && sed 's/\x1b\[[0-9;]*m//g' "$file" | grep -q "$needle"; then
      return 0
    fi
    sleep 0.25
  done
  echo "Timed out waiting for '$needle' in $file" >&2
  return 1
}

wait_actor_registered() {
  local name="$1"
  wait_log "$LOG_DIR/mock-actrix.log" "name=\"$name\""
}

write_cli_config() {
  local project="$1"
  mkdir -p "$project/.actr"
  cat >"$project/.actr/config.toml" <<EOF_CFG
[mfr]
manufacturer = "$MANUFACTURER"
keychain = "$RUN_ROOT/dev-key.json"

[network]
signaling_url = "$SIGNALING_WS"
ais_endpoint = "$AIS_ENDPOINT"
realm_id = $REALM_ID
realm_secret = "$REALM_SECRET"

[storage]
hyper_data_dir = "$RUN_ROOT/hyper"
EOF_CFG
}

write_runtime_config() {
  local project="$1"
  local package_path="$2"
  local visible="$3"
  local service_name="$4"
  cat >"$project/actr.toml" <<EOF_RUNTIME
edition = 1

[signaling]
url = "$SIGNALING_WS"

[ais_endpoint]
url = "$AIS_ENDPOINT"

[deployment]
realm_id = $REALM_ID
realm_secret = "$REALM_SECRET"

[[trust]]
kind = "static"
pubkey_file = "public-key.json"

[discovery]
visible = $visible

[observability]
filter_level = "info"
tracing_enabled = false
tracing_endpoint = "http://127.0.0.1:4317"
tracing_service_name = "$service_name"

[webrtc]
force_relay = false
stun_urls = []
turn_urls = []

[package]
path = "$package_path"
EOF_RUNTIME
}

seed_cargo_lock() {
  local project="$1"
  # Seed workspace resolution, then add the generated package without resolving new versions.
  cp "$REPO_ROOT/Cargo.lock" "$project/Cargo.lock"
  (cd "$project" && cargo generate-lockfile --offline)
}

write_public_key() {
  local project="$1"
  jq '{public_key: .public_key}' "$RUN_ROOT/dev-key.json" >"$project/public-key.json"
  mkdir -p "$project/dist"
  cp "$project/public-key.json" "$project/dist/public-key.json"
}

write_ts_echo_impl() {
  cat >"$TS_ECHO/src/actr_service.ts" <<'EOF_TS'
// ActrService is Implemented: This file contains a complete implementation.
import { create } from '@bufbuild/protobuf';
import {
  PayloadType,
  defineWorkload,
  registerStream,
  sendDataChunk,
  unregisterStream,
} from '@actrium/actr-workload';

import type {
  EchoRequest,
  EchoResponse,
  StreamPrepareRequest,
  StreamPrepareResponse,
  StreamReleaseRequest,
  StreamReleaseResponse,
} from './generated/echo_pb.js';
import {
  EchoResponseSchema,
  StreamPrepareResponseSchema,
  StreamReleaseResponseSchema,
} from './generated/echo_pb.js';
import type { EchoServiceHandler } from './generated/echo_workload.js';
import { EchoServiceDispatcher } from './generated/echo_workload.js';

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();

function toUint8Array(payload: ArrayBuffer | ArrayLike<number>): Uint8Array {
  if (payload instanceof Uint8Array) {
    return payload;
  }
  if (payload instanceof ArrayBuffer) {
    return new Uint8Array(payload);
  }
  return Uint8Array.from(payload);
}

class EchoServiceHandlerImpl implements EchoServiceHandler {
  echo(request: EchoRequest): EchoResponse {
    console.log(`typescript echo: echo ${request.message}`);
    return create(EchoResponseSchema, {
      reply: `echo: ${request.message}`,
      timestamp: BigInt(Date.now()),
    });
  }

  async prepareStream(
    request: StreamPrepareRequest,
  ): Promise<StreamPrepareResponse> {
    const inboundStreamId = request.inboundStreamId;
    const replyStreamId = request.replyStreamId;
    const replyMessage = request.replyMessage;
    console.log(
      `typescript echo: prepare ${inboundStreamId} -> ${replyStreamId}`,
    );

    await registerStream(inboundStreamId, async (chunk, sender) => {
      const incoming = textDecoder.decode(toUint8Array(chunk.payload));
      console.log(`typescript echo: stream ${inboundStreamId} ${incoming}`);
      await sendDataChunk(
        { peer: sender },
        {
          streamId: replyStreamId,
          sequence: BigInt(chunk.sequence) + 1n,
          payload: textEncoder.encode(`${replyMessage}: ${incoming}`),
          metadata: [{ key: 'echo-runtime', value: 'typescript-wasm' }],
          timestampMs: BigInt(Date.now()),
        },
        PayloadType.StreamReliable,
      );
      await unregisterStream(inboundStreamId);
    });

    return create(StreamPrepareResponseSchema, {
      status: `registered:${inboundStreamId}`,
    });
  }

  async releaseStream(
    request: StreamReleaseRequest,
  ): Promise<StreamReleaseResponse> {
    console.log(`typescript echo: release ${request.streamId}`);
    await unregisterStream(request.streamId);
    return create(StreamReleaseResponseSchema, {
      status: `unregistered:${request.streamId}`,
    });
  }
}

const dispatcher = new EchoServiceDispatcher(new EchoServiceHandlerImpl());

export default defineWorkload({
  async onStart(): Promise<void> {
    console.log('TypeScript EchoService workload started');
  },

  async dispatch(envelope): Promise<Uint8Array> {
    return dispatcher.dispatch(envelope);
  },
});
EOF_TS
}

write_ts_echo_project_overrides() {
  cat >"$TS_ECHO/package.json" <<EOF_PKG
{
  "name": "echo-service-ts",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "tsc",
    "componentize": "actr-workload-ts componentize dist/actr_service.js -o dist/echo-service-ts.wasm --wit $REPO_ROOT/core/framework/wit/actr-workload.wit"
  },
  "dependencies": {
    "@actrium/actr-workload": "file:$REPO_ROOT/bindings/typescript/actr-workload",
    "@bufbuild/protobuf": "2.11.0"
  },
  "devDependencies": {
    "@bufbuild/protoc-gen-es": "2.11.0",
    "@types/node": "^20.14.0",
    "typescript": "^5.6.3"
  }
}
EOF_PKG

  cat >"$TS_ECHO/tsconfig.json" <<'EOF_TSCONFIG'
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "outDir": "dist"
  },
  "include": ["src/**/*.ts"]
}
EOF_TSCONFIG

  cat >"$TS_ECHO/manifest.toml" <<'EOF_MANIFEST'
edition = 1
exports = ["protos/local/echo.proto"]

[package]
name = "EchoService"
manufacturer = "demo1"
version = "1.0.0"
description = "Actor-RTC TypeScript EchoService provider"
authors = []
license = "Apache-2.0"
tags = ["dev", "service"]

[binary]
path = "dist/echo-service-ts.wasm"
target = "wasm32-wasip2"
EOF_MANIFEST

  cat >"$TS_ECHO/protos/local/echo.proto" <<'EOF_PROTO'
syntax = "proto3";

package echo;

message EchoRequest {
  string message = 1;
}

message EchoResponse {
  string reply = 1;
  uint64 timestamp = 2;
}

message StreamPrepareRequest {
  string inbound_stream_id = 1;
  string reply_stream_id = 2;
  string reply_message = 3;
}

message StreamPrepareResponse {
  string status = 1;
}

message StreamReleaseRequest {
  string stream_id = 1;
}

message StreamReleaseResponse {
  string status = 1;
}

service EchoService {
  rpc Echo(EchoRequest) returns (EchoResponse);
  rpc PrepareStream(StreamPrepareRequest) returns (StreamPrepareResponse);
  rpc ReleaseStream(StreamReleaseRequest) returns (StreamReleaseResponse);
}
EOF_PROTO
}

write_relay_project() {
  mkdir -p "$RELAY/protos/local" "$RELAY/src"
  cat >"$RELAY/Cargo.toml" <<EOF_CARGO
[package]
name = "relay-service"
version = "1.0.0"
edition = "2024"
build = "build.rs"

[workspace]

[lib]
name = "relay_service"
crate-type = ["rlib", "cdylib"]

[features]
default = ["cdylib"]
cdylib = ["actr-framework/cdylib"]

[dependencies]
actr-framework = "=$ACTR_CRATE_VERSION"
actr-protocol = "=$ACTR_CRATE_VERSION"
async-trait = "0.1"
prost = "0.14"
prost-types = "0.14"
bytes = "1"
tokio = { version = "1", features = ["sync"] }

[lints.rust]
unexpected_cfgs = { level = "allow", check-cfg = ['cfg(actr_has_generated)'] }

[patch.crates-io]
actr = { path = "$REPO_ROOT" }
actr-config = { path = "$REPO_ROOT/core/config" }
actr-framework = { path = "$REPO_ROOT/core/framework" }
actr-hyper = { path = "$REPO_ROOT/core/hyper" }
actr-protocol = { path = "$REPO_ROOT/core/protocol" }
actr-runtime = { path = "$REPO_ROOT/core/runtime" }
actr-runtime-mailbox = { path = "$REPO_ROOT/core/runtime-mailbox" }
actr-service-compat = { path = "$REPO_ROOT/core/service-compat" }
EOF_CARGO

  cat >"$RELAY/build.rs" <<'EOF_BUILD'
fn main() {
    println!("cargo:rustc-check-cfg=cfg(actr_has_generated)");
    println!("cargo:rerun-if-changed=src/generated");

    if std::path::Path::new("src/generated/mod.rs").exists() {
        println!("cargo:rustc-cfg=actr_has_generated");
    }
}
EOF_BUILD

  cat >"$RELAY/manifest.toml" <<'EOF_MANIFEST'
edition = 1
exports = ["protos/local/relay.proto"]

[package]
name = "RelayService"
manufacturer = "demo1"
version = "1.0.0"
description = "Actor-RTC RelayService workload package"
authors = []
license = "Apache-2.0"
tags = ["dev", "service"]

[dependencies]
EchoService = { actr_type = "demo1:EchoService:1.0.0" }

[binary]
path = "dist/relay_service.cdylib"

[build]
tool = "cargo"
manifest_path = "Cargo.toml"
artifact = "lib"
profile = "release"
features = ["cdylib"]
post_build = [
  'mkdir -p "$(dirname "$ACTR_BUILD_BINARY_PATH")" && TARGET_ROOT="${CARGO_TARGET_DIR:-target}" && case "$(uname)" in Darwin) LIB_NAME="librelay_service.dylib" ;; Linux) LIB_NAME="librelay_service.so" ;; *) LIB_NAME="relay_service.dll" ;; esac && for SRC in "$TARGET_ROOT/$ACTR_BUILD_TARGET/$ACTR_BUILD_PROFILE/$LIB_NAME" "$TARGET_ROOT/$ACTR_BUILD_PROFILE/$LIB_NAME"; do if [ -f "$SRC" ]; then cp "$SRC" "$ACTR_BUILD_BINARY_PATH"; exit 0; fi; done && echo "Unable to locate built workload library under $TARGET_ROOT" >&2 && exit 1',
]
EOF_MANIFEST

  cat >"$RELAY/protos/local/relay.proto" <<'EOF_PROTO'
syntax = "proto3";

package relay;

message RelayRequest {
  string message = 1;
}

message RelayResponse {
  string reply = 1;
}

service RelayService {
  rpc Process(RelayRequest) returns (RelayResponse);
}
EOF_PROTO

  cat >"$RELAY/src/lib.rs" <<'EOF_LIB'
//! relay-service -- package-backed Actor-RTC RelayService workload.

pub mod generated;
pub mod echo_service;

use actr_framework::entry;
use generated::relay_actor::RelayServiceWorkload;

pub use crate::echo_service::RelayServiceImpl;

entry!(
    RelayServiceWorkload<RelayServiceImpl>,
    RelayServiceWorkload::new(RelayServiceImpl::new())
);
EOF_LIB

  cat >"$RELAY/src/echo_service.rs" <<'EOF_RELAY'
use crate::generated::echo::{
    EchoRequest, EchoResponse, StreamPrepareRequest, StreamPrepareResponse, StreamReleaseRequest,
    StreamReleaseResponse,
};
use crate::generated::relay::{RelayRequest, RelayResponse};
use crate::generated::relay_actor::RelayServiceHandler;
use actr_framework::{Context, DataChunk, Dest};
use actr_protocol::{ActrError, ActrId, ActrType, ActorResult, MetadataEntry, PayloadType};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

pub struct RelayServiceImpl;

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl RelayServiceHandler for RelayServiceImpl {
    async fn process<C: Context>(
        &self,
        req: RelayRequest,
        ctx: &C,
    ) -> ActorResult<RelayResponse> {
        eprintln!("relay: process {}", req.message);
        let echo_type = echo_type();
        let echo_actor = ctx.discover_route_candidate(&echo_type).await?;
        eprintln!("relay: discovered echo actor {echo_actor:?}");

        if req.message == "check-stream" {
            eprintln!("relay: checking stream results");
            let reliable = check_stream_result("reliable", "hello-stream-reliable")?;
            let latency_first = check_stream_result("latency", "hello-stream-latency")?;
            release_echo_stream(ctx, echo_actor.clone(), "reliable").await?;
            release_echo_stream(ctx, echo_actor, "latency").await?;
            eprintln!("relay: stream check complete");
            return Ok(RelayResponse {
                reply: format!("relay: echo: hello; stream: {reliable}, {latency_first}"),
            });
        }

        clear_stream_results()?;
        eprintln!("relay: calling echo");
        let echo_response: EchoResponse = ctx
            .call(
                &Dest::Peer(echo_actor.clone()),
                EchoRequest {
                    message: req.message,
                },
            )
            .await?;
        eprintln!("relay: echo replied {}", echo_response.reply);
        start_stream_round_trip(
            ctx,
            echo_actor.clone(),
            PayloadType::StreamReliable,
            "reliable",
            "hello-stream-reliable",
        )
        .await?;
        eprintln!("relay: reliable stream started");
        start_stream_round_trip(
            ctx,
            echo_actor,
            PayloadType::StreamLatencyFirst,
            "latency",
            "hello-stream-latency",
        )
        .await?;
        eprintln!("relay: latency stream started");

        Ok(RelayResponse {
            reply: format!("relay: {}; stream: started", echo_response.reply),
        })
    }
}

impl RelayServiceImpl {
    pub fn new() -> Self {
        Self
    }
}

static STREAM_RESULTS: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();

fn echo_type() -> ActrType {
    ActrType {
        manufacturer: "demo1".to_string(),
        name: "EchoService".to_string(),
        version: "1.0.0".to_string(),
    }
}

fn stream_results() -> &'static Mutex<HashMap<String, String>> {
    STREAM_RESULTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn clear_stream_results() -> ActorResult<()> {
    stream_results()
        .lock()
        .map_err(|_| ActrError::Internal("stream result mutex poisoned".to_string()))?
        .clear();
    Ok(())
}

fn check_stream_result(label: &str, outbound_message: &str) -> ActorResult<String> {
    let expected = format!("echo-stream-{label}: {outbound_message}");
    let actual = stream_results()
        .lock()
        .map_err(|_| ActrError::Internal("stream result mutex poisoned".to_string()))?
        .get(label)
        .cloned()
        .ok_or_else(|| ActrError::NotFound(format!("stream result missing for {label}")))?;
    if actual != expected {
        return Err(ActrError::Internal(format!(
            "unexpected stream reply for {label}: {actual}"
        )));
    }
    Ok(format!("{label}-ok"))
}

async fn release_echo_stream<C: Context>(
    ctx: &C,
    echo_actor: ActrId,
    label: &str,
) -> ActorResult<()> {
    let inbound_stream_id = format!("demo1-{label}-to-echo");
    let release: StreamReleaseResponse = ctx
        .call(
            &Dest::Peer(echo_actor),
            StreamReleaseRequest {
                stream_id: inbound_stream_id.clone(),
            },
        )
        .await?;
    if release.status != format!("unregistered:{inbound_stream_id}") {
        return Err(ActrError::Internal(format!(
            "unexpected release_stream status: {}",
            release.status
        )));
    }
    Ok(())
}

async fn start_stream_round_trip<C: Context>(
    ctx: &C,
    echo_actor: ActrId,
    payload_type: PayloadType,
    label: &str,
    outbound_message: &str,
) -> ActorResult<()> {
    let inbound_stream_id = format!("demo1-{label}-to-echo");
    let reply_stream_id = format!("demo1-{label}-to-relay");
    let label = label.to_string();
    let callback_ctx = ctx.clone();
    let callback_label = label.clone();
    let callback_reply_stream_id = reply_stream_id.clone();

    ctx.register_stream(reply_stream_id.clone(), move |chunk, _sender| {
        let label = callback_label.clone();
        let reply_stream_id = callback_reply_stream_id.clone();
        let ctx = callback_ctx.clone();
        Box::pin(async move {
            let payload = String::from_utf8(chunk.payload.to_vec()).map_err(|err| {
                ActrError::DecodeFailure(format!("stream payload is not utf-8: {err}"))
            })?;
            stream_results()
                .lock()
                .map_err(|_| ActrError::Internal("stream result mutex poisoned".to_string()))?
                .insert(label, payload);
            ctx.unregister_stream(&reply_stream_id).await
        })
    })
    .await?;

    let prepare: StreamPrepareResponse = ctx
        .call(
            &Dest::Peer(echo_actor.clone()),
            StreamPrepareRequest {
                inbound_stream_id: inbound_stream_id.clone(),
                reply_stream_id: reply_stream_id.clone(),
                reply_message: format!("echo-stream-{label}"),
            },
        )
        .await?;
    if prepare.status != format!("registered:{inbound_stream_id}") {
        return Err(ActrError::Internal(format!(
            "unexpected prepare_stream status: {}",
            prepare.status
        )));
    }

    ctx.send_data_chunk(
        &Dest::Peer(echo_actor),
        DataChunk {
            stream_id: inbound_stream_id,
            sequence: 1,
            payload: Bytes::from(outbound_message.to_string()),
            metadata: vec![MetadataEntry {
                key: "relay-lane".to_string(),
                value: label,
            }],
            timestamp_ms: Some(now_millis()),
        },
        payload_type,
    )
    .await?;
    Ok(())
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
EOF_RELAY
}

write_client_project() {
  mkdir -p "$CLIENT/protos/local" "$CLIENT/src"
  cat >"$CLIENT/Cargo.toml" <<EOF_CARGO
[package]
name = "client-app"
version = "0.1.0"
edition = "2024"

[workspace]

[lib]
name = "client_app_guest"
crate-type = ["cdylib", "rlib"]

[[bin]]
name = "client-app"
path = "src/main.rs"
required-features = ["host"]

[features]
default = ["cdylib"]
cdylib = ["actr-framework/cdylib"]
host = [
  "dep:actr-config",
  "dep:actr-hyper",
  "dep:anyhow",
  "dep:base64",
  "dep:serde_json",
  "dep:tokio",
  "dep:tracing",
]

[dependencies]
actr-framework = "=$ACTR_CRATE_VERSION"
actr-protocol = "=$ACTR_CRATE_VERSION"
async-trait = "0.1"
bytes = "1"
prost = "0.14"
prost-types = "0.14"

[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
actr-config = { version = "=$ACTR_CRATE_VERSION", optional = true }
actr-hyper = { version = "=$ACTR_CRATE_VERSION", features = ["dynclib-engine"], optional = true }
anyhow = { version = "1", optional = true }
base64 = { version = "0.22", optional = true }
serde_json = { version = "1", optional = true }
tokio = { version = "1", features = ["full"], optional = true }
tracing = { version = "0.1", optional = true }

[patch.crates-io]
actr = { path = "$REPO_ROOT" }
actr-config = { path = "$REPO_ROOT/core/config" }
actr-framework = { path = "$REPO_ROOT/core/framework" }
actr-hyper = { path = "$REPO_ROOT/core/hyper" }
actr-protocol = { path = "$REPO_ROOT/core/protocol" }
actr-runtime = { path = "$REPO_ROOT/core/runtime" }
actr-runtime-mailbox = { path = "$REPO_ROOT/core/runtime-mailbox" }
actr-service-compat = { path = "$REPO_ROOT/core/service-compat" }
EOF_CARGO

  cat >"$CLIENT/manifest.toml" <<'EOF_MANIFEST'
edition = 1
exports = []

[package]
name = "ClientApp"
manufacturer = "demo1"
version = "1.0.0"
description = "Package-backed Rust relay client app"
authors = []
license = "Apache-2.0"
tags = ["dev", "app"]

[dependencies]
RelayService = { actr_type = "demo1:RelayService:1.0.0" }

[binary]
path = "dist/app.cdylib"

[build]
tool = "cargo"
manifest_path = "Cargo.toml"
artifact = "lib"
profile = "release"
features = ["cdylib"]
post_build = [
  'mkdir -p "$(dirname "$ACTR_BUILD_BINARY_PATH")" && TARGET_ROOT="${CARGO_TARGET_DIR:-target}" && case "$(uname)" in Darwin) LIB_NAME="libclient_app_guest.dylib" ;; Linux) LIB_NAME="libclient_app_guest.so" ;; *) LIB_NAME="client_app_guest.dll" ;; esac && for SRC in "$TARGET_ROOT/$ACTR_BUILD_TARGET/$ACTR_BUILD_PROFILE/$LIB_NAME" "$TARGET_ROOT/$ACTR_BUILD_PROFILE/$LIB_NAME"; do if [ -f "$SRC" ]; then cp "$SRC" "$ACTR_BUILD_BINARY_PATH"; exit 0; fi; done && echo "Unable to locate built workload library under $TARGET_ROOT" >&2 && exit 1',
]
EOF_MANIFEST

  cat >"$CLIENT/protos/local/local.proto" <<'EOF_PROTO'
syntax = "proto3";

package client_app;

service ClientAppClientApp {}
EOF_PROTO

  cat >"$CLIENT/src/lib.rs" <<'EOF_LIB'
//! client-app -- local guest bridge for the Rust relay app.

pub mod generated;

use actr_framework::entry;
use async_trait::async_trait;

use generated::local_actor::ClientAppClientAppHandler;
use generated::local_actor::ClientAppClientAppWorkload;

pub struct ClientAppBridge;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ClientAppClientAppHandler for ClientAppBridge {}

entry!(
    ClientAppClientAppWorkload<ClientAppBridge>,
    ClientAppClientAppWorkload::new(ClientAppBridge)
);
EOF_LIB

  cat >"$CLIENT/src/main.rs" <<'EOF_MAIN'
//! client-app -- package-backed Rust relay app.

mod generated;

use anyhow::{Context, Result, anyhow};
use base64::Engine;
use generated::relay::RelayRequest;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use actr_config::{ConfigParser, PackageInfo};
use actr_hyper::{Hyper, HyperConfig, Node, StaticTrust, WorkloadPackage, init_observability};
use actr_protocol::ActrType;

const PACKAGE_PATH: &str = "dist/app.actr";
const OP_TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::main]
async fn main() -> Result<()> {
    let manifest = ConfigParser::from_manifest_file("manifest.toml")
        .context("failed to parse manifest.toml")?;
    ensure_package_built(Path::new(PACKAGE_PATH))?;

    let runtime = ConfigParser::from_runtime_file("actr.toml", package_info(&manifest.package))
        .context("failed to parse actr.toml")?;

    let _observability = init_observability(&runtime.observability)?;

    let package_path = runtime
        .package_path
        .clone()
        .ok_or_else(|| anyhow!("actr.toml is missing [package].path"))?;
    let package_dir = package_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let pubkey = load_public_key(&package_dir)?;
    let trust = Arc::new(StaticTrust::new(pubkey).context("invalid public key")?);

    let hyper_data_dir = actr_config::user_config::resolve_hyper_data_dir()?;
    let hyper = Hyper::new(HyperConfig::new(&hyper_data_dir, trust))
        .await
        .context("failed to initialize Hyper")?;

    let package = WorkloadPackage::from_path(&package_path)
        .with_context(|| format!("failed to read {}", package_path.display()))?;

    let ais_endpoint = runtime.ais_endpoint.clone();
    eprintln!("client phase: attach/register/start");
    let actr_ref = tokio::time::timeout(OP_TIMEOUT, async {
        Node::from_hyper(hyper, runtime.clone())
            .attach(&package)
            .await
            .context("failed to attach local guest package")?
            .register(&ais_endpoint)
            .await
            .context("failed to register with AIS")?
            .start()
            .await
            .context("failed to start app node")
    })
    .await
    .context("timed out during attach/register/start")??;

    let relay_type = ActrType {
        manufacturer: "demo1".to_string(),
        name: "RelayService".to_string(),
        version: "1.0.0".to_string(),
    };
    info!("Discovering demo1:RelayService:1.0.0");
    eprintln!("client phase: discover RelayService");
    let relay_actor = tokio::time::timeout(OP_TIMEOUT, async {
        actr_ref
            .discover_route_candidates(&relay_type, 1)
            .await
            .context("failed to discover RelayService")?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("RelayService discovery returned no candidates"))
    })
    .await
    .context("timed out discovering RelayService")??;

    eprintln!("client phase: call RelayService");
    let start_response = tokio::time::timeout(OP_TIMEOUT, async {
        actr_ref
            .call_remote(
                relay_actor.clone(),
                RelayRequest {
                    message: "hello".to_string(),
                },
            )
            .await
            .context("Relay RPC failed")
    })
    .await
    .context("timed out calling RelayService")??;

    if start_response.reply != "relay: echo: hello; stream: started" {
        return Err(anyhow!("unexpected relay start reply: {}", start_response.reply));
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    let response = tokio::time::timeout(OP_TIMEOUT, async {
        actr_ref
            .call_remote(
                relay_actor,
                RelayRequest {
                    message: "check-stream".to_string(),
                },
            )
            .await
            .context("Relay stream check RPC failed")
    })
    .await
    .context("timed out checking RelayService stream result")??;
    if !response.reply.starts_with("relay: echo: hello; stream: ")
        || !response.reply.contains("reliable-ok")
        || !response.reply.contains("latency-ok")
    {
        return Err(anyhow!("unexpected relay reply: {}", response.reply));
    }

    println!("Relay reply: {}", response.reply);

    actr_ref.shutdown();
    eprintln!("client phase: shutdown");
    let _ = tokio::time::timeout(Duration::from_secs(5), actr_ref.wait_for_shutdown()).await;
    Ok(())
}

fn package_info(package: &PackageInfo) -> PackageInfo {
    package.clone()
}

fn ensure_package_built(package_path: &Path) -> Result<()> {
    let public_key_path = package_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("public-key.json");

    if package_path.exists() && public_key_path.exists() {
        return Ok(());
    }

    let actr = std::env::var("ACTR_BIN").unwrap_or_else(|_| "actr".to_string());
    let status = Command::new(&actr)
        .args(["build", "-o", PACKAGE_PATH])
        .status()
        .with_context(|| format!("failed to run `{actr} build` for the local guest package"))?;

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!(
            "`{} build -o {}` failed with status {}",
            actr,
            PACKAGE_PATH,
            status
        ))
    }
}

fn load_public_key(package_dir: &Path) -> Result<Vec<u8>> {
    let key_path = package_dir.join("public-key.json");
    let key_content = std::fs::read_to_string(&key_path)
        .with_context(|| format!("failed to read {}", key_path.display()))?;
    let key_json: serde_json::Value =
        serde_json::from_str(&key_content).context("invalid public-key.json")?;
    let public_key = key_json["public_key"]
        .as_str()
        .ok_or_else(|| anyhow!("public-key.json is missing `public_key`"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(public_key)
        .context("invalid base64 public key")?;

    if bytes.len() != 32 {
        return Err(anyhow!("public-key.json must contain a 32-byte key"));
    }

    Ok(bytes)
}
EOF_MAIN
}

require_tool jq
require_tool curl
require_tool npm
require_tool node
require_tool protoc
require_tool python3

ACTR_CRATE_VERSION="${ACTR_CRATE_VERSION:-$(awk -F'"' '
  /^\[package\]$/ { in_package = 1; next }
  /^\[/ && in_package { exit }
  in_package && /^version = "/ { print $2; exit }
' "$REPO_ROOT/Cargo.toml")}"
if [ -z "$ACTR_CRATE_VERSION" ]; then
  echo "Unable to determine actr crate version from $REPO_ROOT/Cargo.toml" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

if [ ! -x "$ACTR_BIN" ]; then
  log "Build actr CLI"
  cargo build --locked --release -p actr-cli --bin actr --features wasm-engine
fi

if [ ! -x "$MOCK_ACTRIX_BIN" ]; then
  log "Build mock-actrix"
  cargo build --locked --release -p actr-mock-actrix --bin mock-actrix
fi

log "Build local Rust protoc plugin"
cargo build --locked -p actr-framework-protoc-codegen --bin protoc-gen-actrframework
RUST_PLUGIN_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
if [[ "$RUST_PLUGIN_TARGET_DIR" != /* ]]; then
  RUST_PLUGIN_TARGET_DIR="$REPO_ROOT/$RUST_PLUGIN_TARGET_DIR"
fi
RUST_PLUGIN_BIN="$RUST_PLUGIN_TARGET_DIR/debug/protoc-gen-actrframework"
if [ ! -x "$RUST_PLUGIN_BIN" ]; then
  echo "Expected local Rust protoc plugin at $RUST_PLUGIN_BIN" >&2
  exit 1
fi
mkdir -p "$RUN_ROOT/bin"
ln -sf "$RUST_PLUGIN_BIN" "$RUN_ROOT/bin/protoc-gen-actrframework"
export PATH="$RUN_ROOT/bin:$PATH"
run_logged "rust protoc plugin" protoc-gen-actrframework --version

log "Build local TypeScript workload package"
(cd "$REPO_ROOT/bindings/typescript/actr-workload" && npm ci && npm run build)

MOCK_PORT="${ACTR_E2E_MOCK_PORT:-$(
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)}"
MOCK_HTTP="http://127.0.0.1:$MOCK_PORT"
SIGNALING_WS="ws://127.0.0.1:$MOCK_PORT/signaling/ws"
AIS_ENDPOINT="$MOCK_HTTP/ais"

log "Start mock-actrix on $MOCK_PORT"
NO_COLOR=1 "$MOCK_ACTRIX_BIN" --port "$MOCK_PORT" >"$LOG_DIR/mock-actrix.log" 2>&1 &
MOCK_PID=$!
wait_http "$MOCK_HTTP/signaling/health"

log "Prepare e2e workspace at $RUN_ROOT"
TS_ECHO="$RUN_ROOT/echo-service-ts"
RELAY="$RUN_ROOT/relay-service"
CLIENT="$RUN_ROOT/client-app"
mkdir -p "$RUN_ROOT"

mkdir -p "$RUN_ROOT/home"
run_logged "keygen" env HOME="$RUN_ROOT/home" "$ACTR_BIN" pkg keygen --output "$RUN_ROOT/dev-key.json" --force
PUBLIC_KEY="$(jq -r '.public_key' "$RUN_ROOT/dev-key.json")"

curl -fsS -X POST "$MOCK_HTTP/admin/realms" \
  -H 'content-type: application/json' \
  -d "{\"id\":$REALM_ID,\"name\":\"typescript-stream-e2e\"}" >/dev/null
curl -fsS -X POST "$MOCK_HTTP/admin/mfr" \
  -H 'content-type: application/json' \
  -d "{\"name\":\"$MANUFACTURER\",\"pubkey_b64\":\"$PUBLIC_KEY\",\"contact\":\"e2e@local.actr\"}" >/dev/null

log "Generate TypeScript EchoService template"
(cd "$RUN_ROOT" && "$ACTR_BIN" init echo-service-ts -l typescript --role service --manufacturer "$MANUFACTURER" --signaling "$MOCK_HTTP")
write_cli_config "$TS_ECHO"
write_ts_echo_project_overrides
(cd "$TS_ECHO" && run_logged "ts echo deps install" "$ACTR_BIN" deps install)
(cd "$TS_ECHO" && run_logged "ts echo protoc-gen-es" ./node_modules/.bin/protoc-gen-es --version)
(cd "$TS_ECHO" && run_logged "ts echo gen" "$ACTR_BIN" gen -l typescript --clean --overwrite-user-code)
write_ts_echo_impl
(cd "$TS_ECHO" && run_logged "ts echo npm build" npm run build)
(cd "$TS_ECHO" && run_logged "ts echo componentize" npm run componentize)
(cd "$TS_ECHO" && run_logged "ts echo actr build" "$ACTR_BIN" build --no-compile -k "$RUN_ROOT/dev-key.json" -o dist/echo-service-ts.actr)
write_public_key "$TS_ECHO"
write_runtime_config "$TS_ECHO" "dist/echo-service-ts.actr" "true" "typescript-stream-echo"
(cd "$TS_ECHO" && run_logged "publish echo" "$ACTR_BIN" registry publish -p dist/echo-service-ts.actr -k "$RUN_ROOT/dev-key.json" -e "$MOCK_HTTP")

log "Start EchoService for dependency discovery"
(cd "$TS_ECHO" && NO_COLOR=1 "$ACTR_BIN" run --config actr.toml --hyper-dir "$RUN_ROOT/hyper/echo" >"$LOG_DIR/echo-run.log" 2>&1) &
ECHO_PID=$!
wait_actor_registered "EchoService"

log "Generate Rust RelayService from fixture source"
write_relay_project
seed_cargo_lock "$RELAY"
write_cli_config "$RELAY"
write_public_key "$RELAY"
write_runtime_config "$RELAY" "dist/relay-service.actr" "true" "typescript-stream-relay"
(cd "$RELAY" && run_logged "relay deps install" "$ACTR_BIN" deps install)
(cd "$RELAY" && run_logged "relay protoc-gen-actrframework" protoc-gen-actrframework --version)
(cd "$RELAY" && run_logged "relay gen" "$ACTR_BIN" gen -l rust --clean)
(cd "$RELAY" && run_logged "relay build" "$ACTR_BIN" build -k "$RUN_ROOT/dev-key.json" -o dist/relay-service.actr)
(cd "$RELAY" && run_logged "publish relay" "$ACTR_BIN" registry publish -p dist/relay-service.actr -k "$RUN_ROOT/dev-key.json" -e "$MOCK_HTTP")

log "Start RelayService for dependency discovery"
(cd "$RELAY" && NO_COLOR=1 "$ACTR_BIN" run --config actr.toml --hyper-dir "$RUN_ROOT/hyper/relay" >"$LOG_DIR/relay-run.log" 2>&1) &
RELAY_PID=$!
wait_actor_registered "RelayService"

log "Generate Rust ClientApp from fixture source"
write_client_project
seed_cargo_lock "$CLIENT"
write_cli_config "$CLIENT"
write_public_key "$CLIENT"
write_runtime_config "$CLIENT" "dist/app.actr" "false" "typescript-stream-client"
(cd "$CLIENT" && run_logged "client deps install" "$ACTR_BIN" deps install)
(cd "$CLIENT" && run_logged "client protoc-gen-actrframework" protoc-gen-actrframework --version)
(cd "$CLIENT" && run_logged "client gen" "$ACTR_BIN" gen -l rust --clean)
(cd "$CLIENT" && run_logged "client build" "$ACTR_BIN" build -k "$RUN_ROOT/dev-key.json" -o dist/app.actr)

log "Run ClientApp against mock-actrix"
CLIENT_OUTPUT="$LOG_DIR/client-run.log"
(cd "$CLIENT" && ACTR_BIN="$ACTR_BIN" cargo run --locked --features host --bin client-app) 2>&1 | tee "$CLIENT_OUTPUT"

grep -q "Relay reply: relay: echo: hello; stream:" "$CLIENT_OUTPUT"
grep -q "reliable-ok" "$CLIENT_OUTPUT"
grep -q "latency-ok" "$CLIENT_OUTPUT"

log "TypeScript stream E2E passed"
