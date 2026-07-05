# actr 应用接口面评估（内部审查）

- 日期：2026-07-05
- 范围：actr 框架暴露给上层应用的全部接口面——Rust 核心（`core/framework` 的 `Context` / `Workload` / `Dest`、codegen 生成面、node bootstrap、配置、错误模型、测试支持）与多语言绑定（Kotlin / Swift / TypeScript / Python / Go / C / Web）
- 方法：逐行精读核心 trait 与接收循环源码；对 `examples/`、`cli` codegen 模板、`bindings/`、WIT ABI 做全面证据采集；所有结论均附 `file:line` 级证据
- 评估问题：接口有品味吗？架构合理吗？舒适且完备吗？

## 总结论

**核心类型系统的品味是真实的、上乘的；但应用面整体处于"迁移到一半"的状态。** 架构上存在一个尚未拍板的深层矛盾（并发模型），舒适度与完备度距离"可以放心交给外部开发者"有明确差距。地基是一台优秀引擎，外壳是半拆的车架。

---

## 一、品味：内核是，外围不是

### 品味好的部分（都在核心链路上）

1. **静态分发生成链**：`RpcRequest → {Service}Handler → 零大小 Dispatcher → Workload` 四层全程静态分发、零虚表；`ctx.call(&target, req)` 的响应类型从 `R::Response` 关联类型自动推导，无需标注。
2. **极简 guest 路径是真实的**：`examples/rust/echo-actr/src/lib.rs` 全部 70 行，业务代码 6 行——实现一个生成的 trait 方法 + `entry!` 宏即可运行。
3. **渐进披露**：`Workload` 的 16 个 lifecycle hook（生命周期 4 + 信令 3 + WebSocket 3 + WebRTC 3 + 凭证 2 + 邮箱背压 1）全部带默认实现且默认打 tracing 日志，不关心的一个都不用写（`core/framework/src/workload.rs:220-373`）。
4. **双平台统一 trait**：`MaybeSendSync` 标记让同一份 `Context` / `Workload` 定义在 native（`Send` futures，tokio 多线程）与 wasm32（`?Send` 单线程）同时成立（`core/framework/src/context.rs:7-31`），换来"native 与浏览器写同一份 handler"。
5. **`Dest::{Shell, Local, Actor}`** 三态显式建模 shell⇄workload 分离，比隐式路由诚实。

### 品味破绽（几乎都能在 `Context` trait 一个文件里看到）

1. **god-trait**：RPC、流、媒体轨、发现、日志 15+ 个方法挤在一个 trait 上（`core/framework/src/context.rs:67-352`）。
2. **媒体 API 自我矛盾**：`send_media_sample` 用 `MediaType` 枚举，同一文件里 `add_media_track` 却是 `media_type: &str, codec: &str` 字符串参数（`context.rs:318-324`）。
3. **wire 细节渗漏**：应用要从 `PayloadType`（一个同时含 `RpcReliable` / `MediaRtp` 等对流 API 非法值的 wire 枚举）里挑 lane；`Dest` 文档在讲 HostGate / PeerGate 与序列化策略（`dest.rs:52-65`）；`MediaSample.timestamp` 是裸 RTP u32。
4. **命名债躺在应用面上**：`send_data_stream(chunk: DataStream)`——类型叫"流"、参数叫"块"（issue #267）。
5. `call` 无超时参数（写死 30s，issue #257）；`discover_route_candidate` 名字暴露内部概念，只返回单个候选，无缓存 / 失效 / 订阅语义。

---

## 二、架构：分层合理，但有一个没拍板的根本矛盾

分层本身（protocol / framework / hyper / bindings，接口与实现严格分离，framework 零实现）是合理的。

### 并发模型两头不靠（P0）

`node.rs` 的两个接收循环（inproc guest 循环与远程 mailbox 循环）都是 inline `await handle_incoming`（`core/hyper/src/lifecycle/node.rs:1911`、`:2345`）——**请求严格串行处理**；而 handler 签名却是 `&self`。这同时拿到两边的坏处：

- **付出串行的代价**：一个慢 handler 阻塞整个节点的队头（一次 30s 上游调用期间节点不处理任何其他请求；一个 in-flight 重复 tell 可卡邮箱循环最多 30s——dedup `duplicate_wait_timeout(0) → DEDUP_TTL`）；两个 actor 互相 `ctx.call` 会形成分布式死锁，双方等到超时。该风险目前无任何文档警告。
- **没换来串行的收益**：既然保证一次只处理一条消息，handler 本可以 `&mut self` 直接改状态（经典 actor 模型的核心舒适点），现在用户仍要在 `&self` 后面自己塞 `Mutex`。

要么是真 actor（串行 + 独占状态 + 文档化重入规则），要么是并发 service（每请求 spawn + 明示 `&self` 并发语义）；当前是"串行执行的 service 接口"。**这是接口面上唯一需要拍板的架构决策，一旦有外部用户就再也改不动。**

### 多语言是两级制，而非一等公民阵列

实际上存在三种 guest 机制：uniffi FFI（Kotlin / Swift）、WIT Component Model（TS guest / Python / Go / C）、wasm-bindgen（Web，即 "Option U"）。形成明确两级：

- **Kotlin / Swift（近全功能）**：媒体、流、超时、16 hook 齐备，但 `ContextBridge` 悄悄丢了 `self_id` / `caller_id` / `request_id` / `log` 四个访问器——**FFI handler 无法问"谁在调用我"**（`bindings/ffi/src/context.rs`）。
- **整个 WASM 层（被刻意收窄）**：WIT ABI 无媒体轨、`call` 无超时也无 lane 选择（`core/framework/wit/actr-workload.wit`）；context 是环境式（`get-self-id` 等 host import，dispatch 不携带 ctx）；Web guest 另加：全部流 / 媒体方法 `NotImplemented`，`call` 仅支持 `Dest::Actor`（`core/framework/src/web/context.rs:235,302-355`）；Go / C 无 codegen 支持，纯手写 wit-bindgen + 手动内存管理。

两级制可以是合理取舍，但目前没有文档承认这个分级，各语言用户会以为拿到的是同一个框架。

### 能力 parity 矩阵

| 能力 | Rust | Kotlin/Swift (FFI) | TS napi (host) | WIT guest (TS/Py/Go/C) | Web guest |
|---|---|---|---|---|---|
| 类型化 call | ✅ | ⚠️ codegen 包装，原语是 callRaw | ⚠️ 裸 Buffer | ⚠️ 裸 bytes | ✅ |
| call 超时参数 | ❌（写死 30s） | ✅ callRaw 带 timeout | ✅ | ❌ | ❌ |
| 数据流 | ✅ | ✅ | ❌ | ⚠️（单一 on-data-stream 汇入） | ❌ |
| 媒体轨 | ✅ | ✅ | ❌ | ❌（WIT 无此面） | ❌ |
| 16 lifecycle hook | ✅ | ✅ | n/a | ✅ | ✅ |
| self_id/caller_id/request_id/log | ✅ | ❌ | n/a | ✅ | ✅ |
| codegen 脚手架 | ✅ | ✅ | ✅ | Py/TS ✅，Go/C ❌ | ⚠️ |

---

## 三、舒适与完备：最弱的一环

按严重程度排序：

1. **可测试性接近零**。`test_support::DummyContext` 对 `call` / `tell` / `send_data_stream` / `discover` / `call_raw` / 全部媒体方法一律返回 `NotImplemented`（`core/framework/src/test_support.rs:55-133`）——任何调用过 `ctx.call` 的 handler 都无法单测，没有可编程的 mock 响应机制。框架自身示例也只有起真节点的集成测试。
2. **业务错误无类型通道**。`ActrError` 全部变体为 `String` payload（`core/protocol/src/error.rs:68-143`），应用只能把领域错误塞进 `Internal(format!(...))` 过 wire；无错误码、无结构化 domain payload 扩展点。FFI 边界上 `ErrorEvent.source` 被进一步压平成 Display 字符串（`bindings/ffi/src/workload.rs:128-154`）。
3. **示例场是废墟**。约 10 个示例的 main 是 `unimplemented!("source-defined workload examples were removed; migrate ...")`（mutil-actr / shell-actr-echo / ws-actr-echo / data-stream / acl / media-relay），仅 echo-actr 与 package-echo 可运行；`mutil-actr` 留有机翻痕迹注释（"loadconfig"、"createfailed"）。
4. **host 侧上手成本失控**。旗舰示例 `examples/rust/package-echo/client/src/main.rs` 手工编排约 240 行（读包 → 拼 `PackageInfo` → 配置解析 → 观测 → 信任锚 → `Hyper::new` → attach → register → start），而现成的 `Node::run_from_config` 糖（`core/hyper/src/lib.rs:771`）没有任何示例使用。新人入门需吞下约 24 个概念。guest 侧每个作者背一份 187 行的 `build.rs`（含硬编码 repo URL、构建期网络安装 codegen 插件、手工字符串解析 Cargo.toml 取 rev）。
5. **常用能力缺失，人人重造**：发现无缓存 / failover（package-echo 的 client-guest 手写了 cache + 失效清除 + 重试三件套，`client-guest/src/lib.rs:83-139`）；typed call 无超时选项；流 API 是回调风格（`Fn → BoxFuture`）而非 `Stream` / 背压一体的现代 async 形态。
6. **umbrella crate 只服务 guest**：`actr::prelude` 不含 `Node` / `Hyper` / `HyperConfig` / `Dest` / 信任类型，真实 host 必须直接依赖 `actr_hyper` + `actr_config`（`src/lib.rs:64-84`）。
7. **配置偏重、无开发友好默认**：`signaling_url` / `realm` / `ais_endpoint` 必填，信任锚缺失是硬错误，开发环境要显式启用 `dev_only` 全零公钥哨兵。安全导向正确，但缺一条"零配置本地起两个节点互 echo"的路径。

### 跨语言一致性瑕疵清单

- TS 内部 `ActrId.serialNumber` 一处 `number`（napi）一处 `bigint`（WIT guest）——u64 精度在 napi 侧静默丢失（`bindings/typescript/index.d.ts:79`）。
- 同一概念异名：Swift `Context` vs Kotlin `ActrContext`；`WebRtcPeerStatus` vs Swift `WebRTCPeerStatus`；Kotlin 示例里 Workload / LifecycleAdapter / UnifiedWorkload 三名并存。
- `discover` 有四种签名形状（Rust 单候选 / FFI 单候选 / napi `(type, count) → Array` / WIT 单候选）。
- `*Bridge` 底层命名仍在 Swift / Kotlin / napi 公开导出（#258 只加了别名未收口）。
- 超时魔数散落：Kotlin 生成代码 30000L×2、Swift `Context+Call.swift` 30000、TS 脚手架 15000。
- TS guest 的 `host-imports.d.ts` 缺 `call` / `tell` / `discover` / `log-message` 等一半 ABI 的类型声明；生成的脚手架示例用的是 napi `ActrRef.call` 签名，与 WIT guest ABI 不符（`cli/src/commands/codegen/typescript.rs` ~990）。
- napi 发布依赖 `strip-napi-version-check.js` 正则删改生成物的版本检查，属脆弱的后处理 hack。
- 生成脚手架保护采用与 pristine 模板的字节级全等比较（`cli/src/commands/codegen/rust.rs:466-476`），任何格式化漂移都会翻转覆盖判定。

---

## 四、改进优先级

### P0（需要拍板的架构决策）

1. **并发模型二选一并文档化**：串行 + `&mut self` 独占状态（真 actor），或每请求 spawn + 明示 `&self` 并发语义（service）。附带决定：互调死锁的规避规则、慢 handler 的隔离策略。
2. **`ActrError` 增加结构化业务错误通道**：wire 上 `ErrorResponse` 已有位置，可加 domain code + payload bytes；同步修复 FFI 边界的字符串压平。

### P1（舒适度）

3. 可编程 `MockContext`（可注册 route → 响应映射，覆盖 call / tell / stream）。
4. host bootstrap 收敛到 `Node::run_from_config`，旗舰示例改用之；提供开发模式默认配置。
5. 修复或删除 10 个 `unimplemented!()` 示例；清除机翻注释。
6. 发现内建缓存 / 失效 / failover；`CallOptions` 超时选项（已列入 0.5 计划，连同 #254/#256/#257）。
7. guest `build.rs` 收敛为 `actr-build` crate 的一个函数调用。

### P2（一致性）

8. 媒体 API 全枚举化（`add_media_track` 的 codec / media_type）。
9. FFI `ContextBridge` 补 `self_id` / `caller_id` / `request_id` / `log`。
10. TS `serialNumber` 统一 `bigint`；补全 `host-imports.d.ts`；修正 TS guest 脚手架的 call 示例。
11. 跨语言命名对齐（Context 别名、discover 签名、Bridge 名收口）；超时魔数统一为配置常量。
12. `DataStream → DataChunk`（#267，0.5 破坏窗口，与 Direction+TELL=3 协议手术同批）。

---

## 附：证据索引

- 核心 trait：`core/framework/src/context.rs`、`workload.rs`、`dest.rs`、`dispatcher.rs`、`test_support.rs`
- 串行接收循环：`core/hyper/src/lifecycle/node.rs:1889-1935`（inproc）、`:2330-2360`（mailbox）
- 错误模型：`core/protocol/src/error.rs:68-210`
- 极简示例：`examples/rust/echo-actr/src/lib.rs`；重量级示例：`examples/rust/package-echo/client/src/main.rs:82-179`
- FFI 面：`bindings/ffi/src/context.rs`、`workload.rs`；WIT ABI：`core/framework/wit/actr-workload.wit`；Web guest：`core/framework/src/web/context.rs`
- codegen：`cli/src/commands/codegen/{rust,kotlin,swift,typescript,python}.rs`
