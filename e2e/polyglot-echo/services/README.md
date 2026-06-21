# polyglot-echo `services/` — server forms 矩阵

`run.sh --server <form>` 选择的 server 形态实现都放在这里。每种形态独立子目录。

## 设计约定

每个 server 形态子目录都遵循同样的目录约定：

```
services/<form>/
├── README.md          ← 形态说明、构建要求、限制
├── regenerate.sh      ← 一键重生成 generated/ 内容（脚本化）
├── .gitignore         ← 排除 generated/ 等产物
├── src/               ← 手写代码（业务 handler、入口胶水）— 入 git
│   └── generated/     ← protoc/actr gen 输出 — gitignored
├── manifest.toml(.tpl) ← actr 包元信息 — 入 git（模板带占位符的入 git 为 .tpl）
└── ...
```

**核心原则**：
- 生成代码与手写代码物理分离（不同文件/目录）
- 生成物 .gitignored；只保留生成脚本 + 说明
- e2e 跑的时候 setup.sh 自动 invoke 各 form 的 regenerate.sh
- 业务代码（实际的 echo handler）尽量短（5-15 行），其余靠生成

## 已实现的 server 形态

| `--server` 值 | Workload 载体 | 实现语言 | 部署方式 |
|---|---|---|---|
| `cdylib-rust` (默认) | Rust cdylib `.actr` package | Rust | `actr build` → `actr run` |
| `linked-rust` | 进程内 linked workload | Rust | 直接二进制启动，UniFFI-free |
| `wasm-rust` | Wasm Component | Rust | `actr-hyper` `wasm-engine` feature 加载 |

## 显式不实现的形态

- linked-typescript / linked-python / linked-swift / linked-kotlin server form —— 依赖尚未落地的 FFI 入口工厂（`ActrNode.fromConfigWithWorkload` / `from_toml_with_workload` / `fromConfig(workload:)`），该路线已被取代，故此处不收录
- TypeScript / Python source-defined service workload —— 项目设计已排除（参见 `cli/fixtures/typescript/echo/index.service.ts.hbs`：声明 source-defined workload 已移除）
- Web binding 作为 server —— wasm-pack 当前只做 client

## 客户端语言（与 server form 解耦）

目前仅 Rust client driver（`--client rust`）。Client 通过 `discover(actr_type)` 找 server actor id 后做 RPC，不感知 server 部署形态，因此任意 `--server` 形态都可被同一 client 驱动。TypeScript / Python / Swift client 的绑定入口在当前主干上不可用或有缺陷（详见根 `README.md`），待绑定侧修复后可一并接回。
