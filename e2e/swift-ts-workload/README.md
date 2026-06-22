# Swift TS Workload E2E

验证 iOS Swift linked app 与 TypeScript workload 服务的 `call`（Echo RPC）和
`stream`（duplex DataStream echo）通讯。服务端行为移植自
`demo2_duplex_stream_service`，编排骨架 fork 自 `swift-datastream-app`。

## 架构

- 客户端：`actrium:SwiftTsWorkloadApp:0.1.0`（iOS 模拟器，ActrNode.linked）
- 服务端：`actrium:DuplexEchoService:1.0.0`（TS workload，wasm32-wasip2，`actr run` 托管）
- 信令：真实私有 actrix（本地 `ensure_actrix_available`，CI 下载 `actrix-macos-arm64`）

## 验证

- call：`Echo("ping-X")` -> 期望返回 `echo:ping-X`
- stream：发 `hello 1..3`，期望逐条收到 `echo:hello N`（sequence+1000、c2s≠s2c、Finish 计数对账）
- 成功标记：`ACTR_E2E_RESULT:call=ok stream=3/3`

## 运行

```bash
bash e2e/swift-ts-workload/run.sh
KEEP_TMP=1 bash e2e/swift-ts-workload/run.sh
```

## 关键不变量

所有生成代码（TS/Swift 绑定、wasm 组件、XCFramework）每次运行都用本地源码现场生成，
因此改动 actr 源码可被本测试端到端检出。

## CI

`.github/workflows/ci-e2e.yml` 的 `swift-ts-workload-e2e` job（macOS，定时/手动）。
