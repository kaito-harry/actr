# Actor-RTC Web API Layer 设计

**版本**：2025-11-11
**状态**：已实现，已按当前 Option U 路径校准

> 当前源码事实：API 层位于 Service Worker runtime；DOM 侧 `@actrium/actr-dom` 不承载用户 Fast Path callback，只通过 `FastPathForwarder` 把 `fast_path_data` 送入 SW。浏览器 guest 通过 `actor.sw.js` 加载 `.actr` + `.wbg` bundle，并由 `actr-web-abi` / wasm-bindgen host imports 接入 `RuntimeContext`。

## 1. 总览

本文档描述 `actr-web` 的 API 层实现，包括 Gate、Context 和 ActrRef。
这些组件对标 `actr` 的核心 API，但针对 Web 环境（Service Worker + DOM）进行了适配。

## 2. 架构对比

### 2.1 actr (Native) vs actr-web (Web)

| 组件 | actr (Native) | actr-web (Web) | 一致性 |
|------|---------------|----------------|--------|
| **Gate** | Host + Peer | Host + Peer | 100% |
| **Context** | trait + RuntimeContext | trait + RuntimeContext | 95% |
| **ActrRef** | ActrRef<W> | ActrRef<W> | 95% |

**差异说明**：
- actr 使用 tokio 异步原语（mpsc channel, CancellationToken）
- actr-web 使用 Web 异步原语（futures::channel, parking_lot::Mutex）
- 功能和接口完全等价，只是底层实现不同

## 3. Gate 层

### 3.1 设计

Gate 是出站消息门的统一接口，使用 enum dispatch 实现零虚函数调用。

```rust
pub enum Gate {
    /// SW 内部通信（零序列化）
    Host(Arc<HostGate>),

    /// 跨节点传输（WebSocket/WebRTC）
    Peer(Arc<PeerGate>),
}
```

### 3.2 HostGate (SW 内部通信)

**用途**：同一个 Service Worker 中的 Actor 之间的通信

**特点**：
- 零序列化：直接传递 RpcEnvelope 对象
- 通过 request_id 映射实现请求-响应模式
- 单线程环境，使用 futures::channel::oneshot

**实现位置**：`actr-web/crates/sw-host/src/outbound/host_gate.rs`

```rust
pub struct HostGate {
    /// Pending requests: request_id → oneshot sender
    pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<Bytes>>>>,

    /// 消息处理回调（由 System 设置）
    message_handler: Arc<Mutex<Option<MessageHandler>>>,
}
```

**关键方法**：
- `send_request()`: 发送请求并等待响应
- `send_message()`: 发送单向消息
- `handle_response()`: 处理接收到的响应

### 3.3 PeerGate (跨节点传输)

**用途**：跨 Service Worker 或跨节点的 Actor 通信

**特点**：
- 封装 PeerTransport
- 提供 ActrId → Dest 映射
- 实现请求-响应模式

**实现位置**：`actr-web/crates/sw-host/src/outbound/peer_gate.rs`

```rust
pub struct PeerGate {
    /// Transport manager
    transport: Arc<PeerTransport>,

    /// ActrId → Dest 映射
    actor_dest_map: Arc<Mutex<HashMap<ActrId, Dest>>>,

    /// Pending requests: request_id → oneshot sender
    pending_requests: Arc<Mutex<HashMap<String, futures::channel::oneshot::Sender<Bytes>>>>,
}
```

**关键方法**：
- `register_actor()`: 注册 ActrId → Dest 映射
- `send_request()`: 发送请求并等待响应
- `send_message()`: 发送单向消息
- `send_data_chunk()`: 发送 DataChunk（Fast Path）

## 4. Context 层

### 4.1 设计

Context 是 Actor 内部的执行上下文，实现 `actr_framework::Context` trait。

**实现位置**：`actr-web/crates/sw-host/src/context.rs`

```rust
pub struct RuntimeContext {
    /// 当前 Actor ID
    self_id: ActrId,

    /// 调用方 Actor ID
    caller_id: Option<ActrId>,

    /// 追踪 ID
    trace_id: String,

    /// 请求 ID
    request_id: String,

    /// 出站 gate
    outproc_gate: Gate,
}
```

### 4.2 实现的方法

**数据访问**：
- `self_id()`: 获取当前 Actor ID
- `caller_id()`: 获取调用方 Actor ID
- `trace_id()`: 获取分布式追踪 ID
- `request_id()`: 获取请求唯一 ID

**通信能力**：
- `call<R>(&self, target: &Dest, request: R)`: 类型安全的 RPC 调用
- `tell<R>(&self, target: &Dest, message: R)`: 单向消息发送

**Fast Path（当前路径）**：
- `send_data_chunk()` - ✅ 已实现，出站经 PeerGate / WebRTC MessagePort
- `handle_dom_fast_path()` - ✅ 已实现，DOM `fast_path_data` 入站后进入 SW runtime
- stream handler 注册/注销 - ✅ SW runtime 内有 handler map 和 register/unregister 能力
- MediaTrack 高级 API - ⚠️ 仍需按当前媒体路径继续补齐和验证

### 4.3 与 actr 的差异

| 特性 | actr | actr-web | 说明 |
|------|------|----------|------|
| **RPC call/tell** | ✅ | ✅ | 完全等价 |
| **Fast Path** | ✅ | ⚠️ DataChunk baseline 已进 SW handlers，MediaTrack 需继续补齐 | 不再是 DOM 本地 callback |
| **Dest 支持** | Host/Workload/Peer | Peer only | Web 版本只支持 Dest::Peer |
| **ID 生成** | uuid | js_sys::Math::random() | Web 环境简化 |

## 5. ActrRef 层

### 5.1 设计

ActrRef 是对运行中 Actor 的轻量级引用，提供从 DOM 侧调用 SW 侧 Actor 的能力。

**实现位置**：`actr-web/crates/sw-host/src/actr_ref.rs`

```rust
pub struct ActrRef<W: Workload> {
    pub(crate) shared: Arc<ActrRefShared>,
    _phantom: PhantomData<W>,
}

pub(crate) struct ActrRefShared {
    /// Actor ID
    pub(crate) actor_id: ActrId,

    /// Inproc gate for DOM → SW RPC
    pub(crate) inproc_gate: Arc<HostGate>,

    /// Shutdown flag
    pub(crate) shutdown: Arc<parking_lot::Mutex<bool>>,
}
```

### 5.2 实现的方法

**RPC 调用**：
- `call<R>(&self, request: R)`: 类型安全的 RPC 调用
- `tell<R>(&self, message: R)`: 单向消息发送

**生命周期**：
- `shutdown()`: 触发 Actor 关闭
- `wait_for_shutdown()`: 等待 Actor 完全关闭
- `is_shutting_down()`: 检查是否正在关闭

**查询**：
- `actor_id()`: 获取 Actor ID

### 5.3 与 actr 的差异

| 特性 | actr | actr-web | 说明 |
|------|------|----------|------|
| **RPC call/tell** | ✅ | ✅ | 完全等价 |
| **Shutdown** | CancellationToken | parking_lot::Mutex<bool> | Web 简化实现 |
| **wait_for_shutdown** | tokio::select! | 轮询 + setTimeout | Web 环境限制 |
| **Background tasks** | JoinHandle::abort() | (无后台任务) | SW 是事件驱动 |

### 5.4 Code Generation 支持

`actr-cli` 将为 `ActrRef` 生成类型安全的 RPC 方法：

```protobuf
service EchoService {
  rpc Echo(EchoRequest) returns (EchoResponse);
}
```

生成代码：

```rust
impl ActrRef<EchoServiceWorkload> {
    pub async fn echo(&self, request: EchoRequest) -> ActorResult<EchoResponse> {
        self.call(request).await
    }
}
```

## 6. 消息流

### 6.1 DOM → SW (通过 ActrRef)

```
DOM 侧
  actr_ref.call(request)
    ↓
  ActrRef::call()
    ├─ 编码请求 (protobuf)
    ├─ 创建 RpcEnvelope
    └─ HostGate.send_request()
        ↓
      MessageHandler (由 System 设置)
        ↓
      InboundPacketDispatcher
        ↓
      Mailbox.enqueue()
        ↓
      MailboxProcessor
        ↓
      Scheduler
        ↓
      Actor.handle_xxx(request, ctx)
        ↓
      响应返回 (通过 request_id 匹配)
        ↓
      HostGate.handle_response()
        ↓
      oneshot::Sender.send(response)
        ↓
SW 侧   ActrRef::call() 返回响应
```

### 6.2 SW Actor → Remote Actor (通过 Context)

```
SW 侧
  ctx.call(&Dest::Peer(remote_id), request)
    ↓
  RuntimeContext::call()
    ├─ 编码请求 (protobuf)
    ├─ 创建 RpcEnvelope
    └─ Gate.send_request()
        ↓
      PeerGate.send_request()
        ├─ 查找 remote_id → Dest 映射
        └─ PeerTransport.send()
            ↓
          DestTransport
            ↓
          WireHandle (WebSocket/WebRTC)
            ↓
          网络传输
            ↓
          远程节点接收
            ↓
          响应返回 (通过 request_id 匹配)
            ↓
          PeerGate.handle_response()
            ↓
          oneshot::Sender.send(response)
            ↓
远程      ctx.call() 返回响应
```

## 7. 性能特性

| 路径 | 延迟 | 特点 |
|------|------|------|
| **DOM → SW (ActrRef)** | 需当前 benchmark 确认 | 包含 Mailbox 持久化 + Scheduler |
| **SW → SW (Host)** | 需当前 benchmark 确认 | 零序列化，直接传递 |
| **SW → Remote (Peer)** | 取决于网络，需实测 | 包含网络传输 |

## 8. 后续工作

- [x] 实现 HostGate 的 MessageHandler 自动注册（通过 System.init_message_handler）
- [x] 实现 DataChunk Fast Path baseline（`send_data_chunk`、`handle_dom_fast_path`、SW stream handlers）
- [ ] 补齐 MediaTrack 高级 API 和更多端到端覆盖
- [ ] 添加 ActrRef 的事件订阅功能（events()）
- [ ] 完善错误处理和超时机制
- [ ] 添加更多单元测试

---

**实现时间**：2025-11-11
**代码位置**：`bindings/web/crates/sw-host/src/`、`bindings/web/packages/web-sdk/src/actor.sw.js`
**编译状态**：✅ 通过
