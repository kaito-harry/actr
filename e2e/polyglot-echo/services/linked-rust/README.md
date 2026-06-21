# linked-rust — in-process Rust workload binary

## 形态

EchoService 实现编译为普通 Rust 可执行二进制（不是 cdylib）。可执行启动后**直接 link `actr-hyper`**，构造 `Node`，注入 `EchoWorkload`，向 mock-actrix 注册 `polyglot:EchoService:1.0.0`。同样的客户端 driver（rust/ts/python/swift）通过 `discover` + `call` 调到这里。

与 cdylib 路径的对比：
- **不需要** `actr build` 打包
- **不需要** `actr run` 加载
- 用 `cargo build --bin polyglot-echo-linked-rust` 直接产出 binary
- ctx 是完整 `RuntimeContext`（含 DataStream / MediaTrack 等高级 API）—— 这是 stream-server 使用此形态的原因

## 文件分工

| 文件 | 角色 | 来源 |
|---|---|---|
| `src/echo_service.rs` | echo 业务逻辑（5 行） | 手写 |
| `src/main.rs` | actr Node bootstrap + dispatcher 模板代码 | 手写（boilerplate，模板可生成） |
| `manifest.toml` | actr 包元信息（actr_type、version） | 手写 |
| `Cargo.toml`, `build.rs` | Rust 项目元信息 | 手写 |
| `OUT_DIR/echo.rs` | prost 编译 echo.proto 输出（`EchoRequest`/`EchoResponse`） | 生成（cargo 自动，输出在 target/ 中天然 gitignored） |

**TODO**：dispatcher 模板部分（~25 行）目前手写，在 `actr gen -l rust` 支持 linked binary 工作流后可自动生成到 `src/generated/`，进一步缩减手写量到只剩 `echo_service.rs`。

## 重生成生成代码

`cargo build`/`cargo check` 自动通过 `build.rs` 重生成 prost 输出 —— 没有独立的 `regenerate.sh`，因为这里所有生成都是 cargo 集成的。

## 限制

- 只跑 echo 场景（不含 streaming）。需要 streaming 用 `e2e/polyglot-echo/server/` (linked stream-server)。
