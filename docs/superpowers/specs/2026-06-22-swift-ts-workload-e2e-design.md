# Swift → TypeScript Workload E2E 设计

- 日期：2026-06-22
- 状态：已批准（待实现）
- 目录：`e2e/swift-ts-workload/`

## 1. 目标与动机

新增一个端到端测试，验证 **iOS 上的 Swift 客户端（linked actor）能与一个 TypeScript workload 服务通讯**，覆盖两类能力：

1. **call** —— 普通 RPC 收发：Swift 发送一个 `Echo` 请求，TS 服务返回响应。只验证「发出去、收回来、内容正确」。
2. **stream** —— 双工数据流（duplex DataStream）：Swift 开流发 N 条消息，TS 服务逐条 echo 回来，参照 `demo2_duplex_stream_service` 的服务端行为。

**核心动机（决定整体设计的关键约束）**：本测试必须能验证「改动 actr 源码后端到端是否生效」。因此所有**框架/绑定/生成代码每次运行都用 actr CLI 从本地源码现场生成**（`actr deps install` / `actr gen` / `componentize` / XCFramework 构建均指向 `$REPO_ROOT`），仓库里**只 checked-in 手写的业务逻辑**，绝不 checked-in 生成代码。否则改了 actr 源码、测试却用旧的生成代码，测不出回归。

现状缺口：现有 Swift e2e（`swift-echo-app`、`swift-datastream-app`）的服务端都是 **Rust**；现有 TS workload 服务（`typescript-stream`、`demo2`）的客户端都是 **Rust** 且跑 mock-actrix。没有任何用例覆盖「Swift 客户端 ↔ TS 服务端」这条链路。

## 2. 范围

**做：**
- 一个新的隔离 e2e 目录 `e2e/swift-ts-workload/`（方案 1：fork `swift-datastream-app` 的编排骨架，服务端换成 TS）。
- 服务端 TS workload：一个 proto + 一个 handler，提供 `Echo`（call）和 `StartDuplexStream`/`FinishDuplexStream`（stream）。
- 客户端 Swift iOS app：探针先做 call 验证、再做 stream 验证，打印合并标记。
- 一个新的 CI job `swift-ts-workload-e2e`（`macos-latest`，定时/手动）。

**不做（YAGNI）：**
- 不改动 `swift-datastream-app` 等任何现有通过的测试。
- 不做 8-probe 全矩阵（并发/ACL/latency-first/注销边界等）；stream 验证限定为「单路 duplex echo + 核心断言」。
- 不引入 mock-actrix 路径（Swift iOS WebRTC 全链路用 mock-actrix 未验证，风险高）。

## 3. 关键决策（已确认）

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 目录与结构 | 新建 `e2e/swift-ts-workload/`（方案 1） | 隔离，不危及现有测试，职责单一 |
| actrix | 真实私有 actrix | 与所有 Swift e2e 一致、已验证；mock-actrix 对 Swift WebRTC 未验证 |
| call 协议 | 新增独立 `Echo` RPC | call 与 stream 职责清晰分离，最易验证 |
| 源码组织 | CLI 生成模板 + 填充手写逻辑；生成代码每次现场产出 | 保证改 actr 源码能被端到端测到 |
| stream 验证层次 | ② 单路 + 核心断言 | 比纯数量可靠（能抓服务端真出错），又不像全矩阵那样代码膨胀 |
| Echo 返回值 | 加 `echo:` 前缀 | 断言更强，能区分是否真经过服务端 |

## 4. 架构

```
iOS 模拟器                 actrix(真实)              Host 进程
┌──────────────┐        ┌─────────────┐        ┌────────────────────────┐
│ SwiftTsApp   │─discover→│ WS Signaling│←register─│ DuplexEchoService      │
│ ActrNode.    │─register→│ AIS         │        │ (TS workload,          │
│ linked       │        │ MFR Registry│←publish─│ wasm32-wasip2,actr run)│
│              │                                                          │
│  call:  Echo("ping-X") ─────WebRTC RPC─────→ 返回 "echo:ping-X"         │
│  stream:hello 1..3 ─────────WebRTC DataStream→ 逐条回 echo:hello N      │
└──────────────┘                                └────────────────────────┘
```

身份命名（可在实现时微调）：
- 服务：`actrium:DuplexEchoService:1.0.0`
- app：`actrium:SwiftTsWorkloadApp:0.1.0`
- manufacturer 默认 `actrium`

## 5. 参照来源映射

| 部分 | 参照来源 |
|------|---------|
| TS 服务端 stream 行为（duplex echo 逻辑、ack metadata：`sequence+1000`、`session_id`/`direction`/`ack_for_sequence`/`source_stream_id`） | `demo2_duplex_stream_service/src/actr_service.ts` |
| TS 服务脚手架（package.json/manifest/stub handler） | `actr init --template echo -l typescript --role service` + `typescript-stream` 的本地 binding/componentize 改法 |
| Swift 客户端 stream 驱动 + 验收标记（`StreamEchoCollector`、`runHelloStream`、`ACTR_E2E_RESULT`） | `e2e/swift-datastream-app/DataStreamApp/**` |
| actrix/realm/keychain/AIS/模拟器/诊断 编排 | `e2e/swift-datastream-app/run.sh` + `e2e/package-runtime-echo/lib/common.sh` + `config/actrix.toml` |
| Echo（call）RPC | 新写，简单收发 |

注：`demo2` 是 service-only（无 client/driver），故只覆盖服务端一半；客户端与整套验收来自 `swift-datastream-app`。

## 6. 目录结构

```
e2e/swift-ts-workload/
├── run.sh                    # 编排：actrix→realm→keychain→建TS服务→建Swift app→模拟器→抓标记
├── README.md                 # 架构/流程/验证说明（仿 swift-datastream-app）
├── lib/
│   └── readiness.sh          # wait_for_service_registration（复用/仿 swift-datastream-app）
├── service-src/              # 手写 TS 业务逻辑（覆盖进 CLI 模板）
│   ├── duplex_echo.proto     # Echo + StartDuplexStream/FinishDuplexStream
│   └── actr_service.ts       # Echo handler + registerStream duplex echo（移植 demo2）
├── app-src/                  # 手写 Swift 探针逻辑（覆盖进 CLI 模板）
│   ├── Probes/               # StreamEchoCollector / DuplexEchoProbeRunner
│   └── Services/             # ActrService（call + stream 驱动 + 标记）
├── manifest/                 # service & app 的 manifest 片段模板
├── actr.toml.tpl             # app 运行期配置模板
└── project.yml               # XcodeGen 覆盖模板
```

`service-src/` 与 `app-src/` 仅含手写逻辑。所有生成物（proto→TS/Swift 绑定、workload dispatcher、wasm 组件、XCFramework）由 run.sh 现场生成，不入库。

## 7. proto 设计

`duplex_echo.proto`，`package local;`：

```proto
service DuplexEchoService {
  // call：发送即返回
  rpc Echo(EchoRequest) returns (EchoResponse);
  // stream：参照 demo2 的 duplex 生命周期
  rpc StartDuplexStream(StartDuplexStreamRequest) returns (StartDuplexStreamResponse);
  rpc FinishDuplexStream(FinishDuplexStreamRequest) returns (FinishDuplexStreamResponse);
}

message EchoRequest  { string message = 1; }
message EchoResponse { string message = 1; }

enum StreamPayloadMode { STREAM_RELIABLE = 0; STREAM_LATENCY_FIRST = 1; }

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

## 8. TS 服务端 handler

从 `actr init --template echo -l typescript --role service` 起步（模板是薄壳：仅 package.json/tsconfig/manifest + 抛异常的 stub handler），覆盖为：

- `echo(req)` → 返回 `EchoResponse{ message: "echo:" + req.message }`（验证 call）。
- `startDuplexStream` / `finishDuplexStream` + `registerStream`：直接移植 demo2 的 duplex 逻辑——收到 client chunk → 在 s2c 上回 `echo:`/`ack:` chunk（`sequence + 1000`，带 `session_id`/`direction=service-to-client`/`ack_for_sequence`/`source_stream_id` metadata）给 sender。
- `package.json`：把 `@actrium/actr-workload` 改为 `file:$REPO_ROOT/bindings/typescript/actr-workload`（用本地 binding 源码），加 `componentize`（`actr-workload-ts componentize ... --wit $REPO_ROOT/core/framework/wit/actr-workload.wit`）与 `package`（`actr build`）脚本。

## 9. Swift 客户端探针与验收标记

app 从 `actr init --template empty -l swift --role app` 起步，填入探针。linked app 需在 workload RPC handler 内才有 `ContextBridge`，故 app 先自调一次自身 ProbeService 拿到 `ctx`，再 `ctx.discover("actrium:DuplexEchoService:1.0.0")`，然后**先 call 后 stream**：

**call 验证：**
1. `Echo(EchoRequest{message:"ping-<uuid>"})` 经 `ctx.callRaw`（`.rpcReliable`）发送。
2. 断言 `EchoResponse.message == "echo:ping-<uuid>"`。通过 → `call=ok`，否则 `call=fail`。

**stream 验证（② 单路 + 核心断言）：**
1. 生成 `session_id` + `c2s` 流 id → `StartDuplexStream` RPC → 服务端 `registerStream(c2s)`，返回 `s2c` 流 id。
2. `ctx.registerStream(s2c, StreamEchoCollector(expected=N))`。
3. c2s 上 `sendDataStream` 发 `hello 1..N`（`.streamReliable`，带 metadata）。
4. 服务端逐条 echo 回 s2c。
5. `collector.waitForCompletion(timeout)` 收齐 N 条。
6. **核心断言**：
   - 顺序：回包 sequence == `[1001, 1002, 1003]`
   - 内容：每条回包内容是对应 `hello N` 的 echo（`echo:hello N` / `ack:...`）
   - 真双工：所有回包 `streamId == s2c` 且 `c2s != s2c`
   - 对账：`FinishDuplexStream` 返回 `client_chunks_received == service_chunks_sent == N`
7. 全部通过 → `stream=N/N`。

**合并标记**：app 打印 `ACTR_E2E_RESULT:call=ok stream=3/3`（双写 stdout/stderr，供 run.sh grep）。

## 10. run.sh 编排流程

```
Phase 0  建 actr CLI(本地源码,debug) + 建 TS binding(本地源码) + ensure_actrix_available
Phase 1  起 actrix → login_admin → ensure_realm → provision_mfr_keychain → warmup_ais_key
Phase 2  脚手架 TS 服务: actr init(echo/ts/service) → 覆盖 service-src(proto+handler) →
         patch package.json(本地binding+componentize) → 写 .actr/config.toml + keychain →
         deps install → gen -l typescript → npm build → componentize(wasm32-wasip2) →
         actr build → 发布 DuplexEchoService:1.0.0（pkg verify）
Phase 3  发布 app linked 身份标记 SwiftTsWorkloadApp:0.1.0
Phase 4  actr run 托管 TS 服务 → wait_for_service_registration
Phase 5  脚手架 Swift app: actr init(empty/swift/app) → 覆盖 app-src →
         渲染 actr.toml/manifest/project.yml → deps install(拉远端 proto) →
         gen -l swift → 建 XCFramework(本地源码,iOS+macOS) → xcodegen + xcodebuild
Phase 6  建/启模拟器 → 安装 → simctl launch(+AUTO_..._COUNT=3 等 env) →
         wait_for_result(匹配 call=ok stream=3/3) → 失败 dump 诊断
```

**不变量**：所有生成代码（TS/Swift 绑定、wasm、XCFramework）每次跑都从 `$REPO_ROOT` 现场产出 —— 改 actr 源码可被测到。

run.sh 复用 `package-runtime-echo/lib/common.sh`（actrix 生命周期、`render_template`、`wait_for_http_ok`、`kill_listener`、`require_cmd`）与 `config/actrix.toml`，并仿 `swift-datastream-app/run.sh` 的 Admin API、keychain、模拟器、诊断函数。

## 11. CI 集成

`.github/workflows/ci-e2e.yml` 新增 job `swift-ts-workload-e2e`，照搬 `swift-datastream-app-e2e`：
- `runs-on: macos-latest`，仅 `schedule`（每日）+ `workflow_dispatch` 触发。
- 步骤：checkout → Rust toolchain（`wasm32-unknown-unknown,aarch64-apple-ios,aarch64-apple-ios-sim`）→ Xcode → `brew install xcodegen jq sqlite libssh2 pkg-config` → protoc → wasm-pack → rust-cache → 生成 CLI web 资产 → `download-actrix-artifact.sh actrix-macos-arm64` → uniffi-bindgen → 建 XCFramework → **`actions/setup-node@v4` node 24（TS 构建所需）** → `bash e2e/swift-ts-workload/run.sh` → 失败 upload 脱敏日志。
- 需要 `GH_TOKEN: ACTRIX_READ_TOKEN`（下载私有 actrix 产物）。

## 12. 错误处理与诊断

复用 `swift-datastream-app` 的：
- `capture_diagnostics`：进程状态、`/signaling/health`、`signaling_cache.db` 注册行、过滤后的 actrix/server 日志、app stdout/stderr。
- `sanitize_logs_for_upload`：脱敏 `REALM_SECRET`/`ADMIN_PASSWORD`/`ADMIN_TOKEN` 后移到 `.tmp/sanitized-logs`。
- `cleanup` trap：终止/清理模拟器、server、actrix；`KEEP_TMP=1` 保留产物；`CAPTURE_DIAGNOSTICS_ON_SUCCESS=1` 成功也抓诊断。
- 失败模式：actrix 不健康、服务未注册、`actr deps install` 拉不到远端 proto、XcodeGen/xcodebuild 失败、模拟器启动失败、超时未见 `ACTR_E2E_RESULT` 或不匹配 `call=ok stream=3/3` —— 均 dump 诊断并以非零退出。

## 13. 验收标准

1. 本地 `bash e2e/swift-ts-workload/run.sh`（macOS）成功，日志出现 `ACTR_E2E_RESULT:call=ok stream=3/3`。
2. 服务端、客户端、绑定、wasm、XCFramework 全部由本地源码现场生成（无 checked-in 生成代码）。
3. 故意改坏 actr 相关源码能让本测试失败（证明它真在测源码）。
4. CI job `swift-ts-workload-e2e` 在 `workflow_dispatch` 下绿。
5. 不影响任何现有 e2e job。

## 14. 待实现时确认的小问题

- 身份名 `DuplexEchoService` / `SwiftTsWorkloadApp` 是否符合命名习惯（可微调）。
- TS echo 模板的 `manifest.toml.service.hbs` 默认 `exports = ["protos/local/echo.proto"]`，需对齐到我们的 `duplex_echo.proto`。
- Swift app 起步模板用 `empty` 还是 `data-stream`（empty 更接近 swift-datastream-app 现状；data-stream 模板能多测一条模板路径）——倾向 `empty` + 手写探针。
