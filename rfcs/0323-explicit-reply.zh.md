# RFC-0323: 显式回复（Explicit Reply）——受理与回复解耦

- 状态（Status）：已接受（Accepted）
- 日期（Date）：2026-07-08
- 修订（Revised）：2026-07-24（按 #289 review 重写，对齐 #337 之后的 main）
- RFC PR：[Actrium/actr#289](https://github.com/Actrium/actr/pull/289)
- 跟踪议题（Tracking issue）：[Actrium/actr#323](https://github.com/Actrium/actr/issues/323)
- 替代 RFC（Superseded by）：无
- 关联（Related）：[Actrium/actr#257](https://github.com/Actrium/actr/issues/257)、[Actrium/actr#263](https://github.com/Actrium/actr/pull/263)、[Actrium/actr#268](https://github.com/Actrium/actr/pull/268)、[Actrium/actr#280](https://github.com/Actrium/actr/pull/280)、[Actrium/actr#336](https://github.com/Actrium/actr/pull/336)、[Actrium/actr#337](https://github.com/Actrium/actr/pull/337)

## 摘要（Summary）

把被调方（callee）的“回复时刻”从“dispatch 完成时刻（handler 返回）”上解开：新增一种按方法选择的回复模式，handler 通过一个可移动的 `Reply<T>` 句柄显式发送回复，dispatch 在 handler 返回后立即完成、释放调度槽，而不等待回复产生。**wire 层零改动**——请求与响应本来就是两条以 `request_id` 关联的单向 envelope，本 RFC 只改变被调方 dispatch 层的 API 形态。

设计哲学（已确认）：**准入严格有序（按到达序），业务并发由应用按场景自选**。#337 已把 admission 与 dispatch completion 拆开，并经 conflict-key 调度器给出 distinct-key 并发（受 budget `C` 约束）；但单条 call 的 response 仍焊死在 handler 返回。本 RFC 补上最后一步：让 reply 脱离 handler 返回。它相比“声明一个冲突键”独特地增加三点——**(1) 回复生命周期可超出 handler**（parked waiter 不占 budget C / key slot）；**(2) CPU-bound 工作经 spawn 卸载**（串行校验前缀 + 并发执行体）；**(3) keyless 默认即可获得并发**，不必为每个方法设计冲突键。且不向简单场景征税。

## 动机（Motivation）

#337 之后的接收循环（`core/hyper/src/lifecycle/node.rs`，inproc 与 mailbox 两条循环共享 `admit_incoming`）：

```
admit_incoming(envelope) ──同步、按到达序──→ ACL / dedup / 键提取 / 调度器提交
        │
        └→ IncomingContinuation（执行 handler body）─┬→ 返回值即 response bytes（RETURN）
                                                     ├→ 构造 RESPONSE envelope 发回
                                                     └→ mailbox ack → 释放键槽 / budget C
```

gate-on 且声明了 conflict-key 时，多个 continuation 跨 distinct key 并发（budget `C`）；gate-off 或 keyless 时退化为串行。但**单条 call 的 response 仍焊死在 handler 返回**——continuation 须 await handler 返回后才发 RESPONSE、才 ack。后果：

1. keyless / gate-off 节点入站 call 并发恒为 1，队头阻塞：10 个互不相关的调用方同时 call 一个均耗时 100ms 的 handler，第 10 个白等 900ms；
2. 分布式死锁：A 的 handler 内同步 call B，B 的 handler 内同步 call A，双方 dispatch 槽都被占住，互等到超时；
3. 无法表达“准入即返回、稍后回复”（长轮询、等外部事件、批量聚合）。

而 wire 层从不要求这种焊接：响应按 `request_id` 关联、乱序合法、迟到的孤儿响应被方向路由丢弃（#268/#263）。焊接纯粹是 server 侧 dispatch 层的选择。

## 详细设计（Detailed design）

### 1. 回复模式（proto method option）

在 `core/protocol/proto/actr/options.proto` 增加 method 级选项，extension tag 取 **50002**（`payload_type` 已占 50001，紧随其后分配）：

```proto
enum ReplyMode {
    REPLY_MODE_RETURN = 0;    // Reply through the handler return value (default)
    REPLY_MODE_EXPLICIT = 1;  // Reply explicitly through a Reply<T> handle
}

extend google.protobuf.MethodOptions {
    ReplyMode reply_mode = 50002;
}
```

用法：

```proto
service MediaService {
    rpc Probe(ProbeRequest) returns (ProbeResponse);   // Defaults to RETURN
    rpc Transcode(TranscodeRequest) returns (TranscodeResponse) {
        option (actr.reply_mode) = REPLY_MODE_EXPLICIT;
    }
}
```

命名说明：两个值命名于**机制**而非时机——`RETURN`（回复取自返回值）/ `EXPLICIT`（回复经句柄显式发送）。不叫 `DEFERRED`，因为显式模式下也允许在 handler 内立即回复；不叫 `CONCURRENT`，因为并发与否是应用的选择而非该选项的承诺。

### 2. 生成的 handler 签名

```rust
// RETURN (unchanged)
async fn probe<C: Context>(&self, req: ProbeRequest, ctx: &C)
    -> ActorResult<ProbeResponse>;

// EXPLICIT (new)
async fn transcode<C: Context>(&self, req: TranscodeRequest, ctx: &C,
    reply: Reply<TranscodeResponse>) -> ActorResult<()>;
```

EXPLICIT 的返回值 `ActorResult<()>` 表示**受理结果**。handler 返回 `Err` 时，框架依 §3 的幂等 completer 决定行为：若 reply 尚未完成，框架用该错误发送错误回复并 complete dedup（`?` 运算符在校验前缀里可直接使用）；若已被 `send` 完成，仅记日志。**注意 §2 自身的 spawn 模式会制造双回复竞态**，由 §3/§5 的 first-write-wins completer 消解，而非仅靠“句柄已消费”的编译期检查。

### 3. `Reply<T>` 句柄

```rust
pub struct Reply<T> { /* request_id, return route, trace context, deadline, dedup completer (idempotent) */ }

impl<T: prost::Message> Reply<T> {
    /// Enqueues a reply synchronously and consumes the handle.
    pub fn send(self, result: ActorResult<T>);

    /// Returns the caller's deadline: arrival time plus timeout_ms.
    /// Sending after the deadline remains valid; the caller discards the
    /// resulting orphan response.
    pub fn deadline(&self) -> Option<Instant>;

    pub fn request_id(&self) -> &str;
}
```

**类型系统承担契约**：

- `send(self)` 消费句柄 → 同一 Reply 实例**二次回复在编译期不可能**；
- `Reply<T>: Send + 'static` → 可 move 进 `tokio::spawn`、可存入状态 map（长轮询/订阅式应答的自然表达）；
- **`Drop` 兜底**：句柄未经 `send` 被析构（包括 spawn 任务 panic 展开）时，自动发送 `ActrError::Internal("reply dropped without response")` 并记 warn——调用方永远不会因为被调方忘记回复而白等到超时。`send` 必须是同步入队正是为此：`Drop` 不能 await。TELL 注入的空回程变体例外（见 §6）：其 Drop 以空字节完成 dedup 而非发送 Err。

**幂等 completer（first-write-wins，消解双回复竞态）**：`Reply` 内部持有一个共享完成状态（`Arc` 包裹的 `Once`/`watch`），框架在将 `Reply` 交给 handler 前保留该 completer 的 `Arc` 引用（含回程路由），故 Reply 已 move 进 spawned task 后，handler 返回 `Err` 的 error 路径仍可经同一 completer 完成并发送错误回复。handler 返回 `Err` 的框架 error 路径、`send`、`Drop` 三者都经同一 completer：**首条写入者胜出**——它发出 RESPONSE 并 complete dedup；迟到写入者降级为 logged no-op；**dedup 的 Done 值锁定为实际发出的首条 RESPONSE**（TELL 变体不发 RESPONSE，完成值固定为空字节，见 §6）。这覆盖了 §2 的竞态：reply 已 move 进 spawned task（尚未 send）、handler 又因别的原因返回 `Err` 时，框架 error reply 与 task 的 `send` 只有一个真正落地，另一个成 no-op。wire 层 #268/#263 的孤儿丢弃仍作为第二道防线。

**句柄携带的上下文**：`request_id`、回程路由（inproc lane 或 peer gate）、`traceparent/tracestate`（延迟回复仍接续原调用链）、deadline、dedup 完成器。

**deadline 语义**：`deadline()` = arrival + timeout_ms，是**调用方真实 deadline 的上界**（不含请求在途传输时间 / transit time，真实 deadline 更早）；TELL 请求返回 None。回复保留请求的 payload-type lane（RpcReliable vs RpcSignal），即延迟回复的路由与可靠性语义与原请求一致。与 #257（typed `Context::call` 的可配置 timeout）对齐：#257 落地后，此 timeout_ms 应取其配置值。

### 4. 消息泵契约（随本 RFC 一并成文，独立于新特性生效）

词汇统一：**准入（admission）** 指前置的 ACL/dedup/键提取/调度器提交；**dispatch 完成** 指 continuation 跑完 handler body；**reply** 指 RESPONSE envelope 发出。不再用 “accept” 指 handler 返回。

| 时刻 | 语义 | 承诺 |
|---|---|---|
| **准入（admission）** | `admit_incoming`：ACL / 去重 / 键提取 / 调度器提交 | 严格按到达顺序同步；RETURN / EXPLICIT 相同 |
| **dispatch 完成** | continuation 跑完 handler body | gate-on + conflict-key：跨 distinct key 并发（budget `C`）；gate-off / keyless：串行 |
| **reply** | RESPONSE envelope 发出 | RETURN = dispatch 完成；EXPLICIT ≥ handler 返回，跨请求无序 |

- **串行范围**：handler body 的串行性仅存在于 gate-off / keyless 路径；gate-on 时由 conflict-key 调度器管理（同 key 串行、distinct key 并发）。EXPLICIT 不改变 dispatch 层——handler 仍被 await，框架**不隐式 spawn**；并发由应用在 handler 内显式 spawn。由此得到一个有用模式：**串行校验前缀 + 并发执行体**——在 handler 内串行地检查 / 预占状态，然后 spawn 慢活并立即返回。
- **EXPLICIT 的价值**：handler 返回即 dispatch 完成，调度器立即释放该请求的 conflict key slot 与 budget C（`on_complete(key)`），reply 之后再发。若不释放，特性便无意义。
- **ack 顺序 trade**：RETURN 模式下 mailbox ack 在 dispatch 完成（= reply）时发出，保持 `reply → ack` 顺序（`node.rs` mailbox tail）；EXPLICIT 模式下 ack 提前到 handler 返回、reply 在之后，即 **ack 先于 reply**，削弱了原 per-message 的 reply→ack 契约。本 RFC 接受 ack-at-return 这个 trade——EXPLICIT 的目的正是让泵在 reply 之前继续受理；ack-at-reply 与特性矛盾。（若未来某场景需 ack-at-reply，应在方法粒度另开选项，不在本 RFC。）
- **崩溃窗口**：ack 前崩溃 → 持久邮箱重投（at-least-once）；ack 后、reply 前崩溃 → 调用方超时。**dedup 是进程内内存，不抗崩溃**：崩溃后重试会重跑 handler（见 Drawbacks），§4 不再声称 dedup 兜住崩溃重试；仅同进程内 ack 后、reply 前到达的重复请求才由 dedup 兜住。
- **死锁规则**（文档义务）：RETURN 模式 handler 内不得同步 call 可能回调自己的对端；需要该拓扑时使用 EXPLICIT 模式（A 准入后 spawn 中 call B，A 的泵得以继续服务 B 的入站请求——死锁解除）。

### 5. dedup 与 conflict-key 交互（关键细节）

dedup 条目的完成时刻从“handler 返回”移到“**reply 发出**”（经 §3 的幂等 completer，RETURN 模式两个时刻重合，行为不变）：

- 回复未发出期间到达的重复 call → 命中 InFlight，等待真正的回复（受调用方超时上界约束）；
- 回复已发出后的重复 → 命中 Done 缓存，立即返回缓存回复；
- spawn 任务 panic → `Reply` 在展开中 Drop → 错误回复 + dedup 以 Err 完成，重复请求得到一致的错误而非悬挂。

**conflict-key 交互**：EXPLICIT handler 返回时 dispatch 完成，调度器释放该请求的 conflict key slot 与 budget C，reply 之后的 spawned task 工作落在 conflict-key 串行化之外。后果，须文档化并以测试钉住：

- 同键 reply 相对准入可能乱序（reply 跨 handler 实例无序）；
- return 之后搬走的工作不受“同 key = 状态保护”契约保护——这是相对 #337 的新 footgun。应用若需状态安全，须在 handler body 的串行前缀内完成预占，或为 spawned task 显式声明自己的冲突键。

**未决回复边界（Phase 1 强制）**：EXPLICIT 在 dispatch 完成即释放 budget C，允许 reply 在 spawned task / waiter map 中累积，突破了 #337 的所有边界（scheduler queue M、budget C、mailbox 批量准入）。更棘手的是 `DedupState::evict_expired` 从不驱逐 InFlight 条目（`dedup.rs`：`InFlight => true`，注释明说 retained until completion）——一个永未 send 的 `Reply`（如 §8.3 waiter map 中 session 永不返回）会永久钉住一条 InFlight dedup 条目，使该 request_id 的每次重试挂死。故 Phase 1 必须包含（详见 §7）：

- `outstanding_replies` gauge；
- 可执行的 `max_pending_replies` 上限（超出暂停泵 pop，背压）；
- parked handle 的 TTL/sweep：为 InFlight dedup 条目与 parked Reply 增加独立于完成缓存 TTL 的清扫，超时未完成者以 `ActrError::Timeout` 完成，释放 dedup 条目与 waiter 槽位。

**双回复竞态（已在 §3 定义，此处重申）**：§2 的 spawn 模式——reply 被 move 进 spawned task（尚未 send），handler 因别的原因返回 Err，框架发 error reply，随后 task 的 send 落地。由 §3 的幂等 first-write-wins completer 消解：首条写入者发 RESPONSE 并 complete dedup，迟到者降级为 logged no-op，dedup Done 值锁定为首条实际发出的 RESPONSE。

### 6. tell 与 EXPLICIT 的交叉

`tell` 到达 EXPLICIT 方法时，dispatcher 注入一个**空回程**的 `Reply`：`send` 与 `Drop` 都不发送任何 RESPONSE，但**必须以空字节 complete dedup 条目**，匹配 main 上 TELL 的缓存约定（`node.rs`：TELL 保留 dedup 条目、成功 TELL 缓存空 response）。否则 InFlight 条目永久保留，回到 §5 的挂死问题。契约：fire-and-forget 的语义由调用方决定，被调方代码无需感知。

### 7. 配套护栏（Phase 1 强制，非可选）

- 未决回复计数 gauge（`outstanding_replies`）与受理→回复延迟 histogram；
- `max_pending_replies: usize`（默认值宽松，benchmark 后定）：未决显式回复超过阈值时暂停泵的 pop，形成天然背压，与 `on_mailbox_backpressure` hook 联动；
- parked handle TTL/sweep（见 §5）：清扫超时未完成的 InFlight dedup 条目与 parked Reply，以 `ActrError::Timeout` 完成。

### 8. 典型场景

```rust
// 1. Simple method: unchanged RETURN mode with no migration cost
async fn probe<C: Context>(&self, req: ProbeRequest, _ctx: &C) -> ActorResult<ProbeResponse> {
    Ok(ProbeResponse { alive: true })
}

// 2. Slow method: serial validation followed by concurrent execution
async fn transcode<C: Context>(&self, req: TranscodeRequest, ctx: &C,
    reply: Reply<TranscodeResponse>) -> ActorResult<()> {
    let job = self.validate_and_reserve(&req)?;   // Serial prefix: validate and reserve state
    let ctx = ctx.clone();
    tokio::spawn(async move {                     // Concurrent body: budget C / key slot released at Ok(())
        let result = run_transcode(job, &ctx).await;
        reply.send(result);                        // Late replies are harmless; Drop handles panics
    });
    Ok(())
}

// 3. Long polling or event-driven response: retain Reply until the event arrives
async fn wait_for_update<C: Context>(&self, req: WaitRequest, _ctx: &C,
    reply: Reply<UpdateEvent>) -> ActorResult<()> {
    self.waiters.lock().insert(req.session_id, reply);   // Reply: Send + 'static; subject to TTL/sweep
    Ok(())
}
// Elsewhere: if let Some(reply) = waiters.remove(&id) { reply.send(Ok(event)); }
```

## 缺点（Drawbacks）

- 双 handler 签名增加了 codegen、dispatch 与 FFI 的实现和维护成本，使用者也需要理解 RETURN 与 EXPLICIT 两套契约。
- `Reply<T>` 跨任务存活后，trace 上下文、deadline、dedup 完成状态与回程路由必须随句柄一起保存，运行时资源占用高于同步返回。
- EXPLICIT 在 dispatch 完成即释放 budget C，未决回复受 `max_pending_replies` 与 TTL/sweep 约束；配置不当仍可能造成内存与邮箱压力，但有界可观测。
- `Drop` 与幂等 completer 只能覆盖正常析构与 panic 展开。进程 abort 或崩溃仍可能发生 ack 后、回复前的窗口，且 dedup 不抗崩溃——只能由调用方超时与可靠调用重试处理。
- ack-at-return 削弱了 RETURN 模式下 per-message 的 reply→ack 顺序契约（见 §4 trade）。

## 替代方案（Alternatives）

| 方案 | 否决理由 |
|---|---|
| A. 维持现状 | keyless / gate-off 入站 call 并发恒为 1、同步回调死锁、无法延迟回复——与“应用自选并发”哲学矛盾 |
| B. 全量显式（所有方法都拿 Reply） | 向多数简单方法征税；echo 从 6 行变 10 行且引入无谓概念 |
| D. 泵对每条消息隐式 spawn | 并发选择权回到框架手里；状态竞争成为默认；破坏“准入有序”承诺与顺序保证 |
| E. handler 返回 future、泵不 await（隐式分离） | 隐藏并发，签名看不出语义差异，品味差于显式句柄 |

C（本案：按方法双签名 + 显式句柄）在“简单场景零成本”与“复杂场景全表达力”之间不设中间税。

## 兼容性与分期（Compatibility and phasing）

- **wire 零改动**；与 Direction+TELL=3 正交（迟到回复作为孤儿响应被方向路由丢弃的行为，两者共同保证）。
- **additive**：默认签名与行为完全不变，proto option 是新增项——不占 0.5 破坏窗口，随时可落。
- 分期：
  - Phase 1（本 RFC 主体）：options.proto 扩展（tag 50002）、`actr-framework::Reply`（含幂等 completer）、hyper 泵 / dedup 调整（conflict-key 释放、`outstanding_replies` 上限、TTL/sweep）、Rust codegen 双签名、泵契约文档与测试（含 conflict-key 边界、双回复竞态、TELL 完成约定）；
  - Phase 2：FFI（uniffi 暴露 Reply 对象，Kotlin/Swift 得到同等能力）+ **dynclib 引擎**：`Workload::DynClib` 的 `actr_handle` C ABI 为 sync-world 串行 dispatch，Phase 1 期间 codegen 对 dynclib 目标遇 EXPLICIT 选项**报错而非降级**（与 wasm 同策），Phase 2 落地同等能力；
  - Phase 3：WIT guest ABI 扩展。main 现有两个 world——wit-v1（sync）与 wit-v2 0.2.0（`dispatch` 已是 `async func`、可多调用并发 in-flight，但**仍返回 bytes**）。需新增 `send-reply` host import，让 `dispatch` 返回 `replied(bytes) | deferred` 变体。在此之前 WASM guest 仅支持 RETURN 模式，codegen 对 wasm 目标遇 EXPLICIT 选项报错而非静默降级（reject-don't-degrade 策略不变）。

## 未决问题（Unresolved questions）

1. ~~`reply_mode` 的 proto extension tag 号~~ → 已定 50002（`payload_type` 持有 50001）；
2. `max_pending_replies` 默认值与 parked handle TTL 的具体取值（benchmark 后定）；
3. FFI 侧 `Reply` 对象的生命周期约定（uniffi 对象跨语言 Drop 语义需验证）。

## 未来可能性（Future possibilities）

- 在 Phase 1 的指标与背压机制稳定后，可基于未决回复数量、deadline 与取消信号提供更细粒度的资源治理策略。
- FFI 与 WIT 获得同等的显式回复能力后，可评估将 `Reply<T>` 扩展为进度通知或流式回复抽象；这些语义需要独立 RFC，不属于本提案。
- 显式回复建立的“准入有序、应用控制并发”契约可作为后续 actor 调度与公平性设计的共同基础。
