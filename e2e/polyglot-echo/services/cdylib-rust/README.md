# cdylib-rust — Rust cdylib `.actr` package

## 形态

EchoService 实现编译为 Rust cdylib（`.so`/`.dylib`/`.dll`），打包成签名 `.actr`，由 `actr run` 加载执行。这是**默认**且**唯一支持 cdylib 部署的语言**（cdylib 需要 C ABI + `actr_framework::entry!` 宏 → 当前只有 Rust）。

## 实现来源

不在本目录持久化 service src，而是每次 e2e 跑的时候 `actr init -l rust --template echo --role service` **从 fixture 临时脚手架**。fixture 模板的源在：

```
cli/fixtures/rust/echo/                   ← .hbs 模板（manifest, build, ...）
cli/fixtures/rust/echo_service.rs.hbs     ← 业务 handler 模板（5-15 行 echo 实现）
cli/fixtures/rust/lib.rs.service.hbs      ← 入口 + dispatcher stub fallback
cli/fixtures/rust/Cargo.service.toml.hbs  ← Cargo.toml 模板
```

要改 echo 业务逻辑，改 fixture `.hbs` 文件，**不在这里改**。

## 生成关系

`actr gen -l rust` 把 `protos/local/echo.proto` 编译成 `src/generated/`（含 prost types 和 dispatcher）。fixture 的 `src/lib.rs` 用 `#[cfg(actr_has_generated)]` 在 generated 缺失时降级到 `generated_stub`，所以即便没跑 gen 也能编译（虽然不能运行）。

## 一致性检查清单（form 级别）

- [x] 业务代码与生成代码物理分离（`src/echo_service.rs` vs `src/generated/`）
- [x] 生成物 .gitignored（`cli/fixtures/rust/gitignore.hbs` 含 `/src/generated/`）
- [x] 生成过程脚本化（`actr gen -l rust`，由 `setup.sh::scaffold_service` 调用）
- [x] 不需要在本目录手写代码（fixture 是 single source of truth）

## 限制 & TODO

- 每次 e2e 都重新 init/build，慢（~10-20 s service build）。改进路径：把 scaffold 缓存到 `target/e2e-cache/polyglot-echo/scaffolded-cdylib-rust/`，仅当 fixture 或 proto 变更时重建。
