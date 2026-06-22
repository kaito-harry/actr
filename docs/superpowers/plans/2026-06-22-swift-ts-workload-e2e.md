# Swift → TypeScript Workload E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增一个 macOS-only e2e 测试，验证 iOS 上的 Swift linked actor 能与一个 TypeScript workload 服务做 `call`（Echo RPC）和 `stream`（duplex DataStream echo）通讯。

**Architecture:** Fork 现有 `e2e/swift-datastream-app` 的编排骨架（真实 actrix + iOS 模拟器），把服务端从 Rust cdylib 换成 TS workload（行为移植 `demo2_duplex_stream_service`），并新增一个 Echo RPC 验证 call。所有框架/绑定/生成代码每次运行都用本地源码的 actr CLI 现场生成，确保能测出 actr 源码改动。

**Tech Stack:** bash 编排、actr CLI（本地源码 debug 构建）、真实私有 actrix、TypeScript workload（`@actrium/actr-workload` + `actr-workload-ts componentize` → wasm32-wasip2）、Swift/iOS（XcodeGen + xcodebuild + simctl）、SwiftProtobuf、protobuf。

## Global Constraints

- 目录：`e2e/swift-ts-workload/`（不修改任何现有 e2e 用例）。
- 服务身份：`actrium:DuplexEchoService:1.0.0`；app 身份：`actrium:SwiftTsWorkloadApp:0.1.0`；manufacturer 默认 `actrium`（env `MANUFACTURER` 可覆盖）。
- bundle id：`io.actrium.SwiftTsWorkloadApp`。
- 成功标记（app 打印，双写 stdout+stderr）：`ACTR_E2E_RESULT:call=ok stream=3/3`。run.sh 判定需同时匹配 `call=ok` 与 `stream=3/3`。
- 生成代码绝不 checked-in：仓库只放手写源（proto、TS handler、Swift 探针、模板）；TS/Swift 绑定、wasm、XCFramework 全部 run.sh 现场生成。
- TS workload 本地依赖：`@actrium/actr-workload` 必须指向 `file:$REPO_ROOT/bindings/typescript/actr-workload`；componentize 的 `--wit` 指向 `$REPO_ROOT/core/framework/wit/actr-workload.wit`。
- actr CLI 用本地源码 debug 构建（`build_local_actr_cli`，复用 `package-runtime-echo/lib/common.sh`）。
- CI：仅 `schedule` + `workflow_dispatch`，`runs-on: macos-latest`，需 `GH_TOKEN: ACTRIX_READ_TOKEN` 下载 `actrix-macos-arm64`。
- 参照 spec：`docs/superpowers/specs/2026-06-22-swift-ts-workload-e2e-design.md`。

---

## File Structure

```
e2e/swift-ts-workload/
├── run.sh                         # 编排（fork 自 swift-datastream-app/run.sh，差异见 Task 2/5）
├── README.md                      # 架构/流程/验证说明
├── lib/
│   └── readiness.sh               # 与 swift-datastream-app/lib/readiness.sh 完全相同（直接复制）
├── service-src/                   # 手写 TS 业务逻辑（覆盖进 CLI echo 模板）
│   ├── duplex_echo.proto          # Echo + StartDuplexStream/FinishDuplexStream
│   ├── actr_service.ts            # Echo handler + registerStream duplex echo（移植 demo2）
│   ├── package.json               # 指向本地 binding + componentize/package 脚本
│   └── tsconfig.json
├── SwiftTsApp/                    # 手写 Swift 探针源（覆盖进 CLI empty 模板）
│   ├── App/SwiftTsApp.swift
│   ├── Views/ContentView.swift
│   ├── Probes/StreamEchoCollector.swift
│   ├── Probes/DuplexEchoProbeRunner.swift
│   ├── Probes/ProbeResult.swift
│   ├── Services/ActrService.swift
│   └── Info.plist
├── protos/
│   └── local/probe.proto          # app 自身 ProbeService（触发 ctx）
├── actr.toml.tpl
└── manifest.toml                  # app 包身份 + 远端 DuplexEchoService 依赖
```

复用（不复制，运行时 `source`）：`e2e/package-runtime-echo/lib/common.sh`、`e2e/package-runtime-echo/config/actrix.toml`。

CI：修改 `.github/workflows/ci-e2e.yml`（新增 job）。

---

## Task 1: 目录骨架 + TS 服务端源（proto + handler）

建立目录、直接可复制的文件、以及服务端 TS 手写源。本任务交付物：一个能被 `actr` 工具链构建成 `.actr` 的 TS 服务源（构建验证放在 Task 2，本任务只验证 TS 能 `tsc` 通过 + proto 语法正确）。

**Files:**
- Create: `e2e/swift-ts-workload/service-src/duplex_echo.proto`
- Create: `e2e/swift-ts-workload/service-src/actr_service.ts`
- Create: `e2e/swift-ts-workload/service-src/package.json`
- Create: `e2e/swift-ts-workload/service-src/tsconfig.json`
- Create: `e2e/swift-ts-workload/lib/readiness.sh` (copy)
- Create: `e2e/swift-ts-workload/protos/local/probe.proto`

**Interfaces:**
- Produces:
  - proto service `local.DuplexEchoService`，RPC：`Echo(EchoRequest{string message})→EchoResponse{string message}`、`StartDuplexStream(StartDuplexStreamRequest)→StartDuplexStreamResponse`、`FinishDuplexStream(FinishDuplexStreamRequest)→FinishDuplexStreamResponse`（字段同 spec §7）。
  - TS handler 行为：`echo` 返回 `"echo:" + req.message`；duplex echo 回 `sequence+1000`、payload `echo:<原文>`、metadata `session_id`/`direction=service-to-client`/`ack_for_sequence`/`source_stream_id`。
  - `service-src/package.json` 含 `componentize` 脚本（`run.sh` 会用占位 `__REPO_ROOT__` 替换或在 patch 步骤改写——见下方说明）。

- [ ] **Step 1: 建目录并复制 readiness.sh**

```bash
mkdir -p e2e/swift-ts-workload/{lib,service-src,SwiftTsApp/{App,Views,Probes,Services},protos/local}
cp e2e/swift-datastream-app/lib/readiness.sh e2e/swift-ts-workload/lib/readiness.sh
```

- [ ] **Step 2: 写 proto**（`e2e/swift-ts-workload/service-src/duplex_echo.proto`）

```proto
syntax = "proto3";
package local;

enum StreamPayloadMode {
  STREAM_RELIABLE = 0;
  STREAM_LATENCY_FIRST = 1;
}

service DuplexEchoService {
  rpc Echo(EchoRequest) returns (EchoResponse);
  rpc StartDuplexStream(StartDuplexStreamRequest) returns (StartDuplexStreamResponse);
  rpc FinishDuplexStream(FinishDuplexStreamRequest) returns (FinishDuplexStreamResponse);
}

message EchoRequest  { string message = 1; }
message EchoResponse { string message = 1; }

message StartDuplexStreamRequest {
  string session_id = 1;
  string client_to_service_stream_id = 2;
  uint32 client_chunk_count = 3;
  StreamPayloadMode payload_mode = 4;
  string note = 5;
}
message StartDuplexStreamResponse {
  string session_id = 1;
  string accepted_client_to_service_stream_id = 2;
  string service_to_client_stream_id = 3;
  string status = 4;
}
message FinishDuplexStreamRequest {
  string session_id = 1;
  string client_to_service_stream_id = 2;
  string service_to_client_stream_id = 3;
}
message FinishDuplexStreamResponse {
  string session_id = 1;
  uint32 client_chunks_received = 2;
  uint32 service_chunks_sent = 3;
  string status = 4;
}
```

- [ ] **Step 3: 写 probe.proto**（`e2e/swift-ts-workload/protos/local/probe.proto`，app 自身用于触发 ctx，照搬 swift-datastream-app）

```proto
syntax = "proto3";
package local;

service ProbeService {
  rpc StartProbe(StartProbeRequest) returns (StartProbeResponse);
}

message StartProbeRequest {
  string probe_name = 1;
  string target_type = 2;
}
message StartProbeResponse {
  bool started = 1;
  string message = 2;
}
```

- [ ] **Step 4: 写 TS handler**（`e2e/swift-ts-workload/service-src/actr_service.ts`，移植 demo2，新增 echo）

```typescript
import { create } from '@bufbuild/protobuf';
import {
  PayloadType,
  defineWorkload,
  registerStream,
  sendDataStream,
  unregisterStream,
  type ActrId,
  type DataStream,
  type MetadataEntry,
} from '@actrium/actr-workload';

import type {
  EchoRequest,
  EchoResponse,
  FinishDuplexStreamRequest,
  FinishDuplexStreamResponse,
  StartDuplexStreamRequest,
  StartDuplexStreamResponse,
} from './generated/duplex_echo_pb.js';
import {
  EchoResponseSchema,
  FinishDuplexStreamResponseSchema,
  StartDuplexStreamResponseSchema,
  StreamPayloadMode,
} from './generated/duplex_echo_pb.js';
import type { DuplexEchoServiceHandler } from './generated/duplex_echo_workload.js';
import { DuplexEchoServiceDispatcher } from './generated/duplex_echo_workload.js';

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();

type SessionState = {
  sessionId: string;
  clientToServiceStreamId: string;
  serviceToClientStreamId: string;
  payloadType: PayloadType;
  clientChunksReceived: number;
  serviceChunksSent: number;
};

const sessionsById = new Map<string, SessionState>();
const sessionIdByClientStreamId = new Map<string, string>();

function payloadTypeFor(mode: StreamPayloadMode): PayloadType {
  if (mode === StreamPayloadMode.STREAM_LATENCY_FIRST) {
    return PayloadType.StreamLatencyFirst;
  }
  return PayloadType.StreamReliable;
}

function payloadText(payload: Uint8Array | ArrayBuffer | ArrayLike<number>): string {
  const bytes =
    payload instanceof Uint8Array
      ? payload
      : payload instanceof ArrayBuffer
        ? new Uint8Array(payload)
        : Uint8Array.from(payload);
  return textDecoder.decode(bytes);
}

class DuplexEchoServiceHandlerImpl implements DuplexEchoServiceHandler {
  async echo(request: EchoRequest): Promise<EchoResponse> {
    console.log(`[DuplexEchoService] recv Echo message="${request.message}"`);
    return create(EchoResponseSchema, { message: `echo:${request.message}` });
  }

  async startDuplexStream(
    request: StartDuplexStreamRequest,
  ): Promise<StartDuplexStreamResponse> {
    const payloadType = payloadTypeFor(request.payloadMode);
    const serviceToClientStreamId = `s2c-${request.sessionId}`;

    const state: SessionState = {
      sessionId: request.sessionId,
      clientToServiceStreamId: request.clientToServiceStreamId,
      serviceToClientStreamId,
      payloadType,
      clientChunksReceived: 0,
      serviceChunksSent: 0,
    };

    await registerStream(request.clientToServiceStreamId, async (chunk, sender) => {
      await this.onClientChunk(chunk, sender);
    });

    sessionsById.set(request.sessionId, state);
    sessionIdByClientStreamId.set(request.clientToServiceStreamId, request.sessionId);

    return create(StartDuplexStreamResponseSchema, {
      sessionId: request.sessionId,
      acceptedClientToServiceStreamId: request.clientToServiceStreamId,
      serviceToClientStreamId,
      status: `registered:${request.clientToServiceStreamId}`,
    });
  }

  async finishDuplexStream(
    request: FinishDuplexStreamRequest,
  ): Promise<FinishDuplexStreamResponse> {
    const state = sessionsById.get(request.sessionId);
    await unregisterStream(request.clientToServiceStreamId);
    sessionIdByClientStreamId.delete(request.clientToServiceStreamId);
    if (state) {
      sessionsById.delete(request.sessionId);
    }
    return create(FinishDuplexStreamResponseSchema, {
      sessionId: request.sessionId,
      clientChunksReceived: state?.clientChunksReceived ?? 0,
      serviceChunksSent: state?.serviceChunksSent ?? 0,
      status: `unregistered:${request.clientToServiceStreamId}`,
    });
  }

  private async onClientChunk(chunk: DataStream, sender: ActrId): Promise<void> {
    const sessionId = sessionIdByClientStreamId.get(chunk.streamId);
    const state = sessionId ? sessionsById.get(sessionId) : undefined;
    if (!state) {
      console.log(`[DuplexEchoService] drop chunk, no session stream=${chunk.streamId}`);
      return;
    }

    const text = payloadText(chunk.payload);
    state.clientChunksReceived += 1;
    state.serviceChunksSent += 1;

    const ackSequence = BigInt(chunk.sequence) + 1000n;
    const ackPayload = `echo:${text}`;
    const ackMetadata: MetadataEntry[] = [
      { key: 'session_id', value: state.sessionId },
      { key: 'direction', value: 'service-to-client' },
      { key: 'ack_for_sequence', value: String(chunk.sequence) },
      { key: 'source_stream_id', value: chunk.streamId },
    ];

    await sendDataStream(
      { actor: sender },
      {
        streamId: state.serviceToClientStreamId,
        sequence: ackSequence,
        payload: textEncoder.encode(ackPayload),
        metadata: ackMetadata,
        timestampMs: BigInt(Date.now()),
      },
      state.payloadType,
    );
  }
}

const dispatcher = new DuplexEchoServiceDispatcher(new DuplexEchoServiceHandlerImpl());

export default defineWorkload({
  async onStart(): Promise<void> {
    console.log('[DuplexEchoService] workload started');
  },
  async onStop(): Promise<void> {
    console.log('[DuplexEchoService] workload stopped');
  },
  async dispatch(envelope): Promise<Uint8Array> {
    return dispatcher.dispatch(envelope);
  },
});
```

> 注：`./generated/duplex_echo_pb.js` 与 `duplex_echo_workload.js` 由 `actr gen -l typescript` 从 `duplex_echo.proto` 现场生成（proto 文件名 `duplex_echo.proto` → 生成 `duplex_echo_pb`/`duplex_echo_workload`）。若实际生成名不同，Task 2 首次运行会暴露，按生成名修正 import。

- [ ] **Step 5: 写 package.json**（`e2e/swift-ts-workload/service-src/package.json`，`__REPO_ROOT__` 由 run.sh 替换为绝对路径）

```json
{
  "name": "duplex-echo-service",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "build": "tsc",
    "componentize": "actr-workload-ts componentize dist/actr_service.js -o dist/duplex-echo-service.wasm --wit __REPO_ROOT__/core/framework/wit/actr-workload.wit",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "@actrium/actr-workload": "file:__REPO_ROOT__/bindings/typescript/actr-workload",
    "@bufbuild/protobuf": "2.11.0"
  },
  "devDependencies": {
    "@types/node": "^20.14.0",
    "typescript": "^5.6.3"
  }
}
```

- [ ] **Step 6: 写 tsconfig.json**（`e2e/swift-ts-workload/service-src/tsconfig.json`，照搬 demo2）

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "Bundler",
    "outDir": "dist",
    "rootDir": "src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "declaration": false
  },
  "include": ["src/**/*"]
}
```

- [ ] **Step 7: 校验 proto 语法**

Run: `cd /Users/luzicheng/project/actr-all/actr && protoc --proto_path=e2e/swift-ts-workload/service-src --descriptor_set_out=/dev/null e2e/swift-ts-workload/service-src/duplex_echo.proto`
Expected: 无输出、退出码 0（proto 语法正确）。

- [ ] **Step 8: Commit**

```bash
cd /Users/luzicheng/project/actr-all/actr
git add e2e/swift-ts-workload/
git commit -m "test(e2e): add swift-ts-workload service-side TS source and proto"
```

---

## Task 2: run.sh — actrix 基础设施 + TS 服务脚手架/构建/发布

把 sibling run.sh fork 过来，替换「服务端构建」相关函数为 TS 工具链，并把所有 DataStream/DataStreamApp 命名改为本用例。本任务交付物：`run.sh` 跑到「TS 服务在 signaling 注册成功」即可（先在 `check_service_ready` 后 `exit 0` 验证，验证完移除该临时早退）。

**Files:**
- Create: `e2e/swift-ts-workload/run.sh`（fork 自 `e2e/swift-datastream-app/run.sh`）
- Create: `e2e/swift-ts-workload/actr.toml.tpl`（见 Task 3）
- Create: `e2e/swift-ts-workload/manifest.toml`（见 Task 3）

**Interfaces:**
- Consumes: Task 1 的 `service-src/*`、`lib/readiness.sh`、`protos/local/probe.proto`；`package-runtime-echo/lib/common.sh`、`config/actrix.toml`。
- Produces（供 Task 4/5）:
  - 变量/函数命名：`MANUFACTURER=actrium`、服务身份 `DuplexEchoService:1.0.0`、app 身份 `SwiftTsWorkloadApp:0.1.0`。
  - `SERVICE_PACKAGE`（发布的 `.actr`）、`run_server_host`（`actr run` 托管 TS 服务）、`check_service_ready`（等 `DuplexEchoService` 注册）。
  - 路径：`E2E_TARGET_ROOT="$REPO_ROOT/target/e2e-cache/swift-ts-workload"`。

- [ ] **Step 1: fork sibling run.sh 并全局改名**

```bash
cd /Users/luzicheng/project/actr-all/actr
cp e2e/swift-datastream-app/run.sh e2e/swift-ts-workload/run.sh
# 全局重命名（保持 actrix/realm/keychain/simulator 逻辑不变）
sed -i '' \
  -e 's/swift-datastream-app/swift-ts-workload/g' \
  -e 's/DuplexStreamService/DuplexEchoService/g' \
  -e 's/DataStreamApp/SwiftTsWorkloadApp/g' \
  -e 's/datastream-app/swift-ts-workload-app/g' \
  -e 's/datastream/swift-ts-workload/g' \
  -e 's/DATASTREAMAPP/SWIFTTSAPP/g' \
  e2e/swift-ts-workload/run.sh
```

Run: `grep -c 'DataStream\|datastream' e2e/swift-ts-workload/run.sh`
Expected: `0`（无残留旧名）。若非 0，逐处人工核对修正。

- [ ] **Step 2: 替换 Rust 服务构建函数为 TS 工具链**

在 `e2e/swift-ts-workload/run.sh` 中，删除 Rust 服务相关函数（`write_service_cargo_toml`、`write_service_build_rs`、`write_service_lib_rs`、`write_service_handler_rs`、`write_service_manifest`、`write_service_sources`、`append_workspace_patch`、`write_duplex_stream_proto`、`scaffold_service_guest`、`build_service_package`），用以下 TS 版本替换（保留 `provision_mfr_keychain`/`write_project_keychain_config`/`write_probe_proto` 不变）：

```bash
# ──── TS service scaffold/build/publish ────

scaffold_service_guest() {
    section "🧱 Scaffolding temporary TS DuplexEchoService"
    require_cmd npm
    mkdir -p "$TMP_SERVICE_DIR"
    (
        cd "$TMP_SERVICE_DIR"
        run_actr init \
            -l typescript \
            --template echo \
            --role service \
            --project-name "DuplexEchoService" \
            --signaling "ws://127.0.0.1:${HTTP_PORT}/signaling/ws" \
            --manufacturer "$MANUFACTURER" \
            "."
    )

    # Overlay handwritten sources over the echo template stub.
    mkdir -p "$TMP_SERVICE_DIR/src" "$TMP_SERVICE_DIR/protos/local"
    cp "$SCRIPT_DIR/service-src/actr_service.ts" "$TMP_SERVICE_DIR/src/actr_service.ts"
    cp "$SCRIPT_DIR/service-src/duplex_echo.proto" "$TMP_SERVICE_DIR/protos/local/duplex_echo.proto"
    cp "$SCRIPT_DIR/service-src/tsconfig.json" "$TMP_SERVICE_DIR/tsconfig.json"
    rm -f "$TMP_SERVICE_DIR/protos/local/echo.proto"

    # package.json: substitute __REPO_ROOT__ with the absolute repo path.
    perl -pe "s{__REPO_ROOT__}{$REPO_ROOT}g" \
        "$SCRIPT_DIR/service-src/package.json" >"$TMP_SERVICE_DIR/package.json"

    # manifest.toml: export our proto (template defaults to echo.proto) and keep
    # the published service identity DuplexEchoService:1.0.0.
    perl -i -pe 's{exports = \["protos/local/echo\.proto"\]}{exports = ["protos/local/duplex_echo.proto"]}' \
        "$TMP_SERVICE_DIR/manifest.toml"

    mkdir -p "$(dirname "$SERVICE_KEYCHAIN")"
    cp "$PROVISIONED_KEYCHAIN" "$SERVICE_KEYCHAIN"
    cp "$PROVISIONED_PUBLIC_KEY" "$SERVICE_PUBLIC_KEY"
    write_project_keychain_config "$TMP_SERVICE_DIR" "$SERVICE_KEYCHAIN"

    section "📦 Building local TypeScript workload binding"
    (cd "$REPO_ROOT/bindings/typescript/actr-workload" && npm ci && npm run build)

    (
        cd "$TMP_SERVICE_DIR"
        run_actr deps install
        run_actr gen -l typescript
        npm install
        npm run build
        npm run componentize
    )

    SERVICE_VERSION="1.0.0"
    [ -f "$TMP_SERVICE_DIR/dist/duplex-echo-service.wasm" ] \
        || fail "TS service wasm component missing"
    success "Temporary TS DuplexEchoService ready: version ${SERVICE_VERSION}"
}

build_service_package() {
    section "📦 Building and publishing the TS server package"
    SERVICE_PACKAGE="$DIST_DIR/${MANUFACTURER}-DuplexEchoService-${SERVICE_VERSION}-wasm32-wasip2.actr"

    (
        cd "$TMP_SERVICE_DIR"
        run_actr build \
            --no-compile \
            --manifest-path manifest.toml \
            --key "$SERVICE_KEYCHAIN" \
            --output "$SERVICE_PACKAGE"
    )

    [ -f "$SERVICE_PACKAGE" ] || fail "Server package missing: $SERVICE_PACKAGE"

    run_actr pkg verify --pubkey "$SERVICE_PUBLIC_KEY" --package "$SERVICE_PACKAGE" >/dev/null
    run_actr registry publish \
        --package "$SERVICE_PACKAGE" \
        --keychain "$SERVICE_KEYCHAIN" \
        --endpoint "http://127.0.0.1:${HTTP_PORT}"

    success "TS server package published"
}
```

> 说明：TS workload 是 `wasm32-wasip2` 组件，`manifest.toml` 的 `[binary] path` 指向 `dist/duplex-echo-service.wasm`、`target = "wasm32-wasip2"`（echo 模板 service manifest 已含 `[binary]`，若无则在 manifest patch 步骤补写）。`actr build --no-compile` 直接打包已 componentize 的 wasm（参照 demo2 的 `npm run package`）。

- [ ] **Step 3: 临时早退验证服务注册**

在 `run.sh` 的 `check_service_ready`（Phase 4 之后、Phase 5 之前）下一行临时插入：

```bash
echo "TASK2-CHECKPOINT: service registered, early exit"; exit 0
```

- [ ] **Step 4: 运行到服务注册**

Run: `cd /Users/luzicheng/project/actr-all/actr && bash e2e/swift-ts-workload/run.sh`
Expected: 日志出现 `✓ TS server package published`、`✓ DuplexEchoService readiness check complete`、`TASK2-CHECKPOINT: service registered, early exit`，退出码 0。
（失败排查：`actr gen -l typescript` 生成名是否为 `duplex_echo_pb`/`duplex_echo_workload`，不符则改 Task 1 Step 4 的 import 并重跑。）

- [ ] **Step 5: 移除临时早退**

删除 Step 3 插入的那行。

- [ ] **Step 6: Commit**

```bash
cd /Users/luzicheng/project/actr-all/actr
git add e2e/swift-ts-workload/run.sh
git commit -m "test(e2e): swift-ts-workload run.sh actrix + TS service build/publish"
```

---

## Task 3: Swift app 源（probe + Echo call + stream + 合并标记）

提供 Swift app 手写源、模板文件。从 sibling 的 Swift 源 fork 后改名，并在 `startProbe` handler 中**先做 Echo call、再做 stream**，标记改为 `call=ok stream=N/N`。

**Files:**
- Create: `e2e/swift-ts-workload/SwiftTsApp/**`（fork 自 `swift-datastream-app/DataStreamApp/**`）
- Create: `e2e/swift-ts-workload/actr.toml.tpl`
- Create: `e2e/swift-ts-workload/manifest.toml`

**Interfaces:**
- Consumes: `actr gen -l swift` 从 `probe.proto` + 远端 `duplex_echo.proto` 生成的 `Local_*` 类型（`Local_EchoRequest`/`Local_EchoResponse`/`Local_StartDuplexStreamRequest` 等）、`DuplexEchoServiceClient` 或 `callRaw` + `routeKey`。
- Produces: app 打印 `ACTR_E2E_RESULT:call=ok stream=3/3`（供 Task 4 `wait_for_result` 匹配）。

- [ ] **Step 1: fork Swift 源并改名**

```bash
cd /Users/luzicheng/project/actr-all/actr
cp -R e2e/swift-datastream-app/DataStreamApp e2e/swift-ts-workload/SwiftTsApp
cp e2e/swift-datastream-app/actr.toml.tpl e2e/swift-ts-workload/actr.toml.tpl
# 重命名探针 runner 文件
git -C e2e/swift-ts-workload mv SwiftTsApp/Probes/DataStreamProbeRunner.swift SwiftTsApp/Probes/DuplexEchoProbeRunner.swift 2>/dev/null || \
  mv e2e/swift-ts-workload/SwiftTsApp/Probes/DataStreamProbeRunner.swift e2e/swift-ts-workload/SwiftTsApp/Probes/DuplexEchoProbeRunner.swift
# 全局改名
grep -rl 'DataStreamApp\|DataStreamProbeRunner\|DuplexStreamService\|datastream' e2e/swift-ts-workload/SwiftTsApp e2e/swift-ts-workload/actr.toml.tpl \
  | xargs sed -i '' \
    -e 's/DataStreamProbeRunner/DuplexEchoProbeRunner/g' \
    -e 's/DuplexStreamService/DuplexEchoService/g' \
    -e 's/DataStreamApp/SwiftTsWorkloadApp/g' \
    -e 's/datastream-app-ios/swift-ts-workload-ios/g' \
    -e 's/datastream/swift-ts-workload/g'
```

Run: `grep -rc 'DataStream\|datastream' e2e/swift-ts-workload/SwiftTsApp | grep -v ':0' || echo CLEAN`
Expected: `CLEAN`。

- [ ] **Step 2: 在 actr.toml.tpl 修正 ACL 身份**

确认 `e2e/swift-ts-workload/actr.toml.tpl` 末尾 ACL 规则为（sed 后应已是 `DuplexEchoService`/`SwiftTsWorkloadApp`，核对）：

```toml
[acl]

[[acl.rules]]
permission = "allow"
type = "__MANUFACTURER__:DuplexEchoService:1.0.0"

[[acl.rules]]
permission = "allow"
type = "__MANUFACTURER__:SwiftTsWorkloadApp:0.1.0"
```

- [ ] **Step 3: 写 app 的 manifest.toml**（`e2e/swift-ts-workload/manifest.toml`）

```toml
edition = 1

[package]
name = "SwiftTsWorkloadApp"
manufacturer = "actrium"
version = "0.1.0"
description = "Actrium SwiftTsWorkloadApp — iOS linked runtime"

[dependencies]
duplex_echo = { actr_type = "actrium:DuplexEchoService:1.0.0" }
```

- [ ] **Step 4: 在 startProbe handler 中加入 Echo call，并改标记格式**

编辑 `e2e/swift-ts-workload/SwiftTsApp/Services/ActrService.swift`，在 `startProbe` 的 stream-echo 分支（discover 成功后、`runner.runHelloStream` 之前）插入 Echo call，并把 `emitE2EResult` 改为合并格式。把原 stream 分支（对应 sibling 第 343-369 行）替换为：

```swift
        if let count = streamEchoCount(from: req.probeName) {
            // ── call 验证：Echo RPC ──
            var callOk = false
            do {
                let token = "ping-\(UUID().uuidString)"
                var echoReq = Local_EchoRequest()
                echoReq.message = token
                let rd = try await ctx.callRaw(
                    target: target,
                    routeKey: Local_EchoRequest.routeKey,
                    payloadType: .rpcReliable,
                    payload: try echoReq.serializedData(),
                    timeoutMs: 30_000
                )
                let echoResp = try Local_EchoResponse(serializedBytes: rd)
                callOk = (echoResp.message == "echo:\(token)")
                fileLog("[SwiftTsWorkloadApp] Echo call ok=\(callOk) resp=\(echoResp.message)")
            } catch {
                fileLog("[SwiftTsWorkloadApp] ❌ Echo call failed: \(error)")
            }

            // ── stream 验证：duplex echo ──
            let result = await runner.runHelloStream(count: count) { line, receivedLine in
                await MainActor.run {
                    svc?.appendStreamLog(line, receivedLine: receivedLine)
                }
            }
            for line in result.logLines {
                fileLog("[SwiftTsWorkloadApp] \(line)")
            }
            await MainActor.run {
                svc?.receivedEchoLines = result.receivedLines
            }

            let expectedLines = (1...count).map { "received: echo: hello \($0)" }
            let passCount = zip(result.receivedLines, expectedLines).filter { $0.0 == $0.1 }.count
            let streamOk = result.succeeded && passCount == count && result.receivedLines == expectedLines
            await MainActor.run {
                emitE2EResult("call=\(callOk ? "ok" : "fail") stream=\(passCount)/\(count)")
            }

            var resp = Local_StartProbeResponse()
            resp.started = callOk && streamOk
            resp.message = "call=\(callOk) stream=\(passCount)/\(count)"
            return resp
        }
```

> 说明：`runHelloStream` 发送 `hello N`、`StreamEchoCollector.displayLine` 产生 `received: echo: hello N`（与 TS handler 回的 `echo:hello N` 对应；displayLine 在 `echo:` 与内容间保留空格，故期望串是 `received: echo: hello N`——与 sibling 完全一致，因 sibling Rust 服务也回 `echo: hello N`）。若 TS 回串无空格（`echo:hello N`），在 `StreamEchoCollector.displayLine` 或 expectedLines 对齐；以 Task 4 实跑为准。

- [ ] **Step 5: 编译期静态核对（无 iOS 构建，仅语法核对 import 名）**

Run: `grep -n 'Local_EchoRequest\|Local_EchoResponse\|emitE2EResult' e2e/swift-ts-workload/SwiftTsApp/Services/ActrService.swift`
Expected: 出现 Echo 调用与新 `emitE2EResult("call=...")`。真正编译在 Task 4 的 xcodebuild。

- [ ] **Step 6: Commit**

```bash
cd /Users/luzicheng/project/actr-all/actr
git add e2e/swift-ts-workload/SwiftTsApp e2e/swift-ts-workload/actr.toml.tpl e2e/swift-ts-workload/manifest.toml
git commit -m "test(e2e): swift-ts-workload Swift app probes (Echo call + duplex stream)"
```

---

## Task 4: run.sh — Swift app 脚手架/构建 + 模拟器 + 全链路绿

补齐 run.sh 的 app 侧函数（fork 自 sibling，已被 Task 2 的 sed 改名），对齐 app 源拷贝路径与标记匹配逻辑，跑通整条链路。

**Files:**
- Modify: `e2e/swift-ts-workload/run.sh`

**Interfaces:**
- Consumes: Task 3 的 `SwiftTsApp/`、`actr.toml.tpl`、`manifest.toml`；Task 2 的服务发布与 `run_server_host`。
- Produces: 全链路成功 + `ACTR_E2E_RESULT:call=ok stream=3/3`。

- [ ] **Step 1: 对齐 app 源拷贝路径**

在 `run.sh` 的 `scaffold_swift_ts_workload_app`（原 `scaffold_datastream_app`，已改名）中，确认/修正：模板拷贝源是 `$SCRIPT_DIR/SwiftTsApp`、目标目录名 `SwiftTsApp`、删除 `SwiftTsApp/Generated`、`write_*_manifest` 改为 `cp "$SCRIPT_DIR/manifest.toml"`、`write_*_project_yml` 内 `name`/`sources` 路径/`PRODUCT_BUNDLE_IDENTIFIER: io.actrium.SwiftTsWorkloadApp`、proto 拷贝 `protos/local/probe.proto`。逐项核对 sed 后结果，残留不一致处人工改。

Run: `grep -n 'SwiftTsApp\|io.actrium.SwiftTsWorkloadApp\|probe.proto' e2e/swift-ts-workload/run.sh`
Expected: 拷贝路径、bundle id、probe.proto 均为新名。

- [ ] **Step 2: 修正成功标记匹配**

把 `run.sh` 中 `wait_for_*_result`（原 `wait_for_datastream_result`）的匹配逻辑改为同时要求 call 与 stream：

```bash
            if echo "$result" | grep -qE "ACTR_E2E_RESULT:call=ok stream=3/3"; then
                success "call ok and all 3 stream echo messages passed"
                return 0
            fi
            warn "Incomplete: got $result, expected ACTR_E2E_RESULT:call=ok stream=3/3"
```

并确认 `install_and_launch_app` 注入的 env 前缀已是 `SIMCTL_CHILD_ACTR_SWIFTTSAPP_AUTO_STREAM_COUNT=3`、`SIMCTL_CHILD_ACTR_SWIFTTSAPP_TARGET_TYPE="${MANUFACTURER}:DuplexEchoService:1.0.0"`，且 app 端读取的 env 名一致（核对 `SwiftTsApp/App/*.swift` 中 `AUTO_STREAM_COUNT`/`TARGET_TYPE` 的 env key 与此处前缀拼接后相同；不一致则统一）。

- [ ] **Step 3: 全链路本地运行**

Run: `cd /Users/luzicheng/project/actr-all/actr && bash e2e/swift-ts-workload/run.sh`
Expected: 末尾出现 `Datastream result: ACTR_E2E_RESULT:call=ok stream=3/3` 与 `✓ ... e2e completed successfully`，退出码 0。
（失败保留产物：`KEEP_TMP=1 bash e2e/swift-ts-workload/run.sh`，查 `.tmp/run-*/logs/app.std*.log`。）

- [ ] **Step 4: 源码改动可被测到（防回归证明）**

临时把 TS handler 的 echo 返回改坏：`e2e/swift-ts-workload/service-src/actr_service.ts` 里 `echo:${request.message}` 改成 `WRONG:${request.message}`，重跑 run.sh。
Expected: 测试失败（`call=fail`），证明它真在测服务端逻辑。随后**还原**该改动并重跑确认恢复绿。

- [ ] **Step 5: Commit**

```bash
cd /Users/luzicheng/project/actr-all/actr
git add e2e/swift-ts-workload/run.sh
git commit -m "test(e2e): swift-ts-workload full run.sh (app build + simulator + verify)"
```

---

## Task 5: CI job + README

新增 CI job 与文档。

**Files:**
- Modify: `.github/workflows/ci-e2e.yml`
- Create: `e2e/swift-ts-workload/README.md`

**Interfaces:**
- Consumes: `e2e/swift-ts-workload/run.sh`。

- [ ] **Step 1: 在 ci-e2e.yml 新增 job**

在 `.github/workflows/ci-e2e.yml` 的 `swift-datastream-app-e2e` job 之后，追加（基于 `swift-datastream-app-e2e` 步骤，额外加 Node 24；脱敏日志路径改名）：

```yaml
  swift-ts-workload-e2e:
    name: Swift TS Workload E2E
    runs-on: macos-latest
    timeout-minutes: 240
    permissions:
      contents: read
      actions: read
    steps:
      - uses: actions/checkout@v5
      - name: Cache SPM packages
        uses: actions/cache@v4
        with:
          path: ~/Library/Caches/org.swift.swiftpm
          key: spm-${{ runner.os }}-${{ hashFiles('e2e/swift-ts-workload/manifest.toml') }}
          restore-keys: |
            spm-${{ runner.os }}-
      - name: Setup Rust
        uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-wasip2,wasm32-unknown-unknown,aarch64-apple-ios,aarch64-apple-ios-sim
      - name: Setup Xcode
        uses: maxim-lobanov/setup-xcode@v1
        with:
          xcode-version: latest-stable
      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: "24"
      - name: Install macOS e2e dependencies
        run: |
          brew install xcodegen jq sqlite libssh2 pkg-config
          brew link libssh2
      - name: Install protoc
        uses: arduino/setup-protoc@v3
        with:
          repo-token: ${{ secrets.GITHUB_TOKEN }}
      - name: Install wasm-pack
        run: cargo install wasm-pack --version 0.13.1 --locked
      - name: Install wasm Component Model toolchain
        run: |
          cargo install wasm-component-ld --version 0.5.22 --locked
          cargo install wasm-tools --version 1.247.0 --locked
      - uses: Swatinem/rust-cache@v2
      - name: Generate CLI web runtime assets
        run: bash bindings/web/scripts/sync-cli-assets.sh --build
      - name: Download actrix artifact
        run: bash .github/scripts/download-actrix-artifact.sh actrix-macos-arm64
      - name: Cache uniffi-bindgen binary
        id: cache-uniffi
        uses: actions/cache@v4
        with:
          path: ~/.cargo/bin/uniffi-bindgen*
          key: uniffi-bindgen-0.31.1-${{ runner.os }}-${{ runner.arch }}
      - name: Install UniFFI bindgen
        if: steps.cache-uniffi.outputs.cache-hit != 'true'
        run: cargo install --locked uniffi --version 0.31.1 --features cli --bin uniffi-bindgen
      - name: Build local Swift XCFramework (iOS + macOS)
        working-directory: bindings/swift
        env:
          ACTR_BUILD_PROFILE: debug
          ACTR_XCFRAMEWORK_TARGETS: all
          CMAKE_POLICY_VERSION_MINIMUM: "3.5"
        run: |
          ACTR_BINDINGS_PATH=dist/ActrBindings \
          ACTR_BINARY_PATH=dist/ActrFFI.xcframework \
          ./build-xcframework.sh
          echo "ACTR_BINARY_PATH=dist/ActrFFI.xcframework" >> "$GITHUB_ENV"
          echo "ACTR_BINDINGS_PATH=dist/ActrBindings" >> "$GITHUB_ENV"
      - name: Run Swift TS Workload e2e
        env:
          CAPTURE_DIAGNOSTICS_ON_SUCCESS: "1"
        run: bash e2e/swift-ts-workload/run.sh
      - name: Upload sanitized diagnostic logs
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: swift-ts-workload-e2e-logs-${{ github.run_id }}-${{ github.run_attempt }}
          path: e2e/swift-ts-workload/.tmp/sanitized-logs/
          retention-days: 7
          if-no-files-found: warn
```

> 注：`env` 顶部 `GH_TOKEN: ${{ secrets.ACTRIX_READ_TOKEN }}` 已在 workflow 顶层定义（`download-actrix-artifact.sh` 用），无需 job 内重复。

- [ ] **Step 2: 校验 workflow YAML 合法**

Run: `cd /Users/luzicheng/project/actr-all/actr && python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci-e2e.yml')); print('YAML OK')"`
Expected: `YAML OK`。

- [ ] **Step 3: 写 README**（`e2e/swift-ts-workload/README.md`）

```markdown
# Swift TS Workload E2E

验证 iOS Swift linked app 与 TypeScript workload 服务的 `call`（Echo RPC）和
`stream`（duplex DataStream echo）通讯。服务端行为移植自
`demo2_duplex_stream_service`，编排骨架 fork 自 `swift-datastream-app`。

## 架构

- 客户端：`actrium:SwiftTsWorkloadApp:0.1.0`（iOS 模拟器，ActrNode.linked）
- 服务端：`actrium:DuplexEchoService:1.0.0`（TS workload，wasm32-wasip2，`actr run` 托管）
- 信令：真实私有 actrix（本地 `ensure_actrix_available`，CI 下载 `actrix-macos-arm64`）

## 验证

- call：`Echo("ping-X")` → 期望返回 `echo:ping-X`
- stream：发 `hello 1..3`，期望逐条收到 `echo:hello N`（sequence+1000、c2s≠s2c、Finish 计数对账）
- 成功标记：`ACTR_E2E_RESULT:call=ok stream=3/3`

## 运行

\`\`\`bash
bash e2e/swift-ts-workload/run.sh          # 本地（仅 macOS）
KEEP_TMP=1 bash e2e/swift-ts-workload/run.sh   # 失败保留产物
\`\`\`

## 关键不变量

所有生成代码（TS/Swift 绑定、wasm 组件、XCFramework）每次运行都用本地源码现场生成，
因此改动 actr 源码可被本测试端到端检出。

## CI

`.github/workflows/ci-e2e.yml` 的 `swift-ts-workload-e2e` job（macOS，定时/手动）。
```

- [ ] **Step 4: Commit**

```bash
cd /Users/luzicheng/project/actr-all/actr
git add .github/workflows/ci-e2e.yml e2e/swift-ts-workload/README.md
git commit -m "ci(e2e): add swift-ts-workload-e2e job and README"
```

---

## Self-Review

**Spec coverage（逐节核对 spec → task）：**
- §4 架构 / §6 目录 → Task 1（目录/源）+ Task 2/4（run.sh）✅
- §7 proto → Task 1 Step 2 ✅
- §8 TS handler（echo + duplex + 本地 binding + componentize）→ Task 1 Step 4/5 + Task 2 Step 2 ✅
- §9 Swift 探针（call + stream + 合并标记）→ Task 3 Step 4 ✅
- §10 run.sh 六 phase（含「现场生成」不变量）→ Task 2 + Task 4 ✅
- §11 CI job（含 Node 24、actrix 产物）→ Task 5 Step 1 ✅
- §12 诊断/脱敏 → fork 自 sibling，随 Task 2 复制保留 ✅
- §13 验收标准（标记绿 / 现场生成 / 改坏源码能失败 / CI 绿）→ Task 4 Step 3-4 + Task 5 ✅

**Placeholder scan:** 无 TBD/TODO；`__REPO_ROOT__` 是 run.sh 运行时替换的占位（Task 2 Step 2 的 perl 替换），非计划占位。Task 3 Step 4/Task 4 Step 2 标注了「以实跑为准」的对齐点（生成名、echo 串空格、env key），均给了核对命令与修正方向。

**Type consistency:** 服务身份 `DuplexEchoService:1.0.0`、app 身份 `SwiftTsWorkloadApp:0.1.0`、标记 `call=ok stream=3/3`、proto 名 `duplex_echo.proto`、wasm `dist/duplex-echo-service.wasm` —— 全计划一致。Swift 侧 `Local_EchoRequest`/`Local_EchoResponse`/`routeKey`/`callRaw` 与 sibling 既有用法一致。

**已知需在实跑中确认的对齐点（非阻塞，已在对应 step 标注）：**
1. `actr gen -l typescript` 对 `duplex_echo.proto` 的生成文件名（影响 actr_service.ts import）。
2. TS echo service 模板 manifest 是否含 `[binary]`（无则 manifest patch 补 `path=dist/duplex-echo-service.wasm` / `target=wasm32-wasip2`）。
3. `StreamEchoCollector.displayLine` 产生的期望串与 TS 回包 `echo:hello N` 的空格对齐。
4. app 读取 `AUTO_STREAM_COUNT`/`TARGET_TYPE` 的 env key 与 `SIMCTL_CHILD_ACTR_SWIFTTSAPP_*` 前缀拼接一致。
