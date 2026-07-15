# Actrix 开发指南

## 快速开始

### 环境要求

- Rust 1.95+ (Edition 2024)
- Cargo
- SQLite 3.x
- OpenSSL (用于 TLS 证书)

### 克隆和构建

```bash
# 克隆仓库(包含子项目)
git clone https://github.com/Actrium/actrix.git
cd actrix

# 开发构建
cargo build

# 发布构建
cargo build --release

# 带 OpenTelemetry 支持
cargo build --release --features opentelemetry
```

### 运行测试

```bash
# 所有测试
cargo test

# 特定 crate
cargo test -p ks
cargo test -p turn

# 带日志
RUST_LOG=debug cargo test

# 质量检查
make all  # fmt, clippy, test, build, coverage
```

## 项目结构

```
actrix/
├── crates/
│   ├── actrixd/                # 主程序 crate
│   │   ├── src/
│   │   │   ├── main.rs         # 入口点、可观测性初始化
│   │   │   ├── cli.rs          # 命令行参数
│   │   │   ├── recording_pipeline.rs  # recording 出口初始化
│   │   │   ├── process.rs      # pid/权限切换
│   │   │   ├── service/        # 服务管理
│   │   │   │   ├── manager.rs  # 生命周期管理
│   │   │   │   └── container.rs
│   │   │   └── error.rs        # 错误类型
│   │   └── tests/
│   ├── contracts/              # gRPC 协议与消息定义（package: actrix-proto）
│   ├── platform/               # 共享基础库（lifecycle/cfg/state/auth/events）
│   │   ├── config/             # 配置系统
│   │   ├── storage/            # SQLite 抽象
│   │   └── aid/                # Actor ID 相关
│   ├── control/                # 控制面协议实现（package: admin）
│   ├── sdk/                    # 统一导出门面（package: actrix-sdk）
│   └── services/               # 业务服务集合
│       ├── ais/                # 身份服务
│       ├── ks/                 # 密钥服务
│       ├── signaling/          # 信令服务
│       ├── stun/               # STUN 服务器
│       └── turn/               # TURN 服务器（含 LRU 认证缓存）
├── deploy/                     # 最小部署引导工具
├── docs/                       # 文档
└── AGENTS.md                   # AI 开发助手指南
```

## 开发工作流

### 1. 创建功能分支

```bash
git checkout -b feature/my-feature
```

### 2. 开发和测试

```bash
# 格式化代码
cargo fmt

# Lint 检查
cargo clippy -- -D warnings

# 运行测试
cargo test

# 本地运行
cargo run -p actrix -- --config config.example.toml
```

### 3. 提交代码

遵循语义化提交规范:

```
feat: 添加新功能
fix: 修复 bug
perf: 性能改进
refactor: 代码重构
docs: 文档更新
test: 测试相关
chore: 构建/工具变更
```

**重要**: 不要在提交消息中提及 AI 工具。

### 4. 提交前检查

```bash
# 运行所有检查
make all

# 或手动执行
cargo fmt --check
cargo clippy -- -D warnings
cargo test
cargo build
```

## 添加新服务

### 1. 创建 Crate

```bash
cargo new --lib crates/myservice
```

### 2. 更新 `Cargo.toml`

```toml
[workspace]
members = [
    "crates/actrixd",
    "crates/myservice",
    # ...
]

[workspace.dependencies]
myservice = { path = "crates/myservice" }
```

### 3. 实现服务

```rust
// crates/myservice/src/lib.rs
pub struct MyService {
    config: MyServiceConfig,
}

impl MyService {
    pub fn new(config: MyServiceConfig) -> anyhow::Result<Self> {
        Ok(Self { config })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        // 服务逻辑
        Ok(())
    }
}
```

### 4. 添加到 ServiceContainer

```rust
// crates/actrixd/src/service/container.rs
pub enum ServiceContainer {
    // 现有服务...
    MyService(MyService),
}

impl ServiceContainer {
    pub async fn start(self) -> Result<()> {
        match self {
            // ...
            ServiceContainer::MyService(svc) => svc.start().await?,
        }
        Ok(())
    }
}
```

### 5. 添加到 ServiceManager

```rust
// crates/actrixd/src/service/manager.rs
impl ServiceManager {
    pub async fn start_myservice(&mut self) -> Result<()> {
        let service = MyService::new(/* config */)?;
        self.services.push(ServiceContainer::MyService(service));
        Ok(())
    }
}
```

### 6. 更新配置

```rust
// crates/platform/src/config/mod.rs
pub const ENABLE_MYSERVICE: u8 = 0b100000;  // 新位

impl ActrixConfig {
    pub fn is_myservice_enabled(&self) -> bool {
        self.enable & ENABLE_MYSERVICE != 0
    }
}
```

## 调试技巧

### 日志级别

```bash
# 全局 debug
RUST_LOG=debug cargo run -p actrix

# 特定模块
RUST_LOG=actrix::service=debug,ks=trace cargo run -p actrix

# 仅错误
RUST_LOG=error cargo run -p actrix
```

### 使用 GDB/LLDB

```bash
# 构建 debug 版本
cargo build

# GDB
rust-gdb target/debug/actrix

# LLDB
rust-lldb target/debug/actrix
```

### Flamegraph 性能分析

```bash
cargo install flamegraph
sudo cargo flamegraph --bin actrix
```

## 常见问题

### 1. SQLite 锁定错误

**问题**: `database is locked`

**解决**:
- 确保只有一个实例运行
- 检查文件权限
- 使用 WAL 模式 (未来版本)

### 2. 端口已占用

**问题**: `Address already in use`

**解决**:
```bash
# 查找占用进程
sudo netstat -tlnp | grep 3478
sudo lsof -i :8443

# 杀死进程
kill -9 <PID>
```

### 3. TLS 证书错误

**问题**: `invalid certificate`

**解决**:
```bash
# 生成自签名证书
openssl req -x509 -newkey rsa:4096 \
  -keyout certificates/server.key \
  -out certificates/server.crt \
  -days 365 -nodes
```

### 4. 编译错误

**问题**: workspace 依赖冲突

**解决**:
```bash
# 清理构建缓存
cargo clean

# 更新依赖
cargo update

# 检查 Cargo.lock
git diff Cargo.lock
```

## 性能优化

### Profiling

```bash
# CPU 性能分析
cargo flamegraph --bin actrix

# 内存分析
valgrind --tool=massif target/release/actrix
```

### 基准测试

```bash
# 添加 benches/
cargo bench

# 或使用 criterion
cargo bench --bench turn_auth
```

## 发布流程

### 1. 更新版本号

```toml
# Cargo.toml
[workspace.package]
version = "0.2.0"
```

### 2. 更新 CHANGELOG

```markdown
## [0.2.0] - 2025-01-15

### Added
- OpenTelemetry 追踪支持
- 日志轮转

### Changed
- 使用 LRU 缓存提升 TURN 性能

### Fixed
- 配置验证 bug
```

### 3. 创建标签

```bash
git tag -a v0.2.0 -m "Release v0.2.0"
git push origin v0.2.0
```

### 4. 构建发布版

```bash
cargo build --release
strip target/release/actrix  # 减小二进制大小
```

## 贡献指南

1. Fork 项目
2. 创建功能分支
3. 遵循代码规范 (参考 AGENTS.md)
4. 添加测试
5. 提交 Pull Request

### 代码审查要点

- ✅ 所有测试通过
- ✅ 代码格式化 (`cargo fmt`)
- ✅ 无 Clippy 警告
- ✅ 文档完整
- ✅ 性能无退化
- ✅ 安全性考虑

## 工具推荐

### IDE / 编辑器

- **VS Code**: rust-analyzer 插件
- **IntelliJ IDEA**: Rust 插件
- **Vim/Neovim**: coc-rust-analyzer

### 开发工具

```bash
# Rust 工具链
rustup component add rustfmt clippy

# 代码覆盖率
cargo install cargo-tarpaulin

# 依赖审计
cargo install cargo-audit

# 许可证检查
cargo install cargo-license
```

## 参考资源

- [AGENTS.md](../AGENTS.md) - AI 助手开发指南
- [CLAUDE.md](../CLAUDE.md) - 项目上下文
- [deploy/README.md](../deploy/README.md) - 部署指南
- [Rust Book](https://doc.rust-lang.org/book/)
- [Tokio 文档](https://tokio.rs)
- [Axum 文档](https://docs.rs/axum/)
