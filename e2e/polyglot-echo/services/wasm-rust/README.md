# wasm-rust — Rust Wasm Component `.actr` package

## 形态

EchoService 实现编译为 **Rust wasm32-wasip2 Component Model** 二进制（`.wasm`），打包成签名 `.actr`，由 `actr run` 加载执行。Hyper 的 `wasm-engine` feature 用 wasmtime 实例化组件并 dispatch 进 RPC 请求。

跟 `cdylib-rust` 几乎完全对称（同 `actr build` + `actr run` pipeline，同 `entry!` 宏 + `EchoServiceHandler` 实现），唯一区别：

| 维度 | cdylib-rust | wasm-rust |
|---|---|---|
| `Cargo.toml` `crate-type` | `["rlib", "cdylib"]` | `["cdylib"]` |
| `Cargo.toml` `actr-framework` features | `cdylib` 启用 | `cdylib` 不启用（`entry!` 在 `cfg(target_arch = "wasm32")` 下走 Component Model 路径） |
| `manifest.toml` `[binary]` | `path = "dist/x.cdylib"` | `path = "dist/x.wasm"` + `target = "wasm32-wasip2"` + `kind = "component"` |
| 加载侧 hyper feature | `dynclib-engine` | `wasm-engine` |

## 文件分工

| 文件 | 角色 | 来源 |
|---|---|---|
| `src/echo_service.rs` | echo 业务逻辑（5 行） | 手写 |
| `src/lib.rs` | `entry!` + `generated_stub` fallback | 手写（mirror of `cli/fixtures/rust/lib.rs.service.hbs`） |
| `manifest.toml`, `Cargo.toml` | 包/项目元信息 | 手写 |
| `protos/local/echo.proto` | 服务契约 | 手写（共享自 `e2e/polyglot-echo/proto/echo.proto`） |
| `src/generated/` | `actr gen -l rust` 输出（dispatcher + prost types） | 生成（gitignored） |

跑 `regenerate.sh` 重生成 `src/generated/`。`src/lib.rs` 用 `#[cfg(actr_has_generated)]` 在缺失时降级 `generated_stub`，这样无需 gen 也能编译。

## 工具链要求

- `wasm32-wasip2` rustup target
- `wasm-component-ld >= 0.5.22`（`cargo install wasm-component-ld --version 0.5.22 --locked`）
- `wasm-tools >= 1.247`（diagnostic 用，可选）
