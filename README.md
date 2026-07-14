# Actor-RTC Framework Demo

![Rust](https://img.shields.io/badge/rust-1.88+-orange.svg)
![Node.js](https://img.shields.io/badge/node.js-16+-green.svg)
![WebRTC](https://img.shields.io/badge/webrtc-enabled-blue.svg)
[![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Factrium.github.io%2Factr%2Fcoverage%2Fcoverage-badge.json)](https://actrium.github.io/actr/coverage/)
![License](https://img.shields.io/badge/license-MIT-green.svg)

A distributed real-time communication framework demo built on WebRTC and the Actor model.

## 📖 Overview

This project demonstrates a distributed system architecture that combines the classic Actor model with modern WebRTC transport. Each process is treated as a coarse-grained actor, communicates peer-to-peer over WebRTC, and uses a dual-path processing model to optimize different kinds of traffic.

### 🎯 Key Features

- **Coarse-grained Actor model**: Process-level actor abstraction for simpler distributed design
- **Native WebRTC support**: Built-in NAT traversal and peer-to-peer connectivity
- **Dual-path processing**:
  - **State Path**: Reliable ordered control-message handling
  - **Fast Path**: Low-latency streaming data handling
- **Type safety**: Contract-driven development based on Protobuf
- **ACL-aware discovery**: Secure discovery with access control list support

---

## 📚 Documentation

The repository includes a complete set of design and development documents. A practical reading order is:

#### **Part 1: Concepts & Architecture**
*Start here to build a shared mental model of the framework.*

1.  **[Ecosystem Overview](./docs/0-Ecosystem-Overview.zh.md)** (recommended first)
2.  **[Concepts and Architecture](./docs/1-Concepts-and-Architecture.zh.md)**
3.  **[ActorSystem and Actor](./docs/1.1-ActorSystem-and-Actor.zh.md)**
4.  **[Interacting with the Outside World](./docs/1.3-Interacting-with-the-Outside-World.zh.md)**
5.  **[Inter-Actor Communication Patterns](./docs/1.4-Inter-Actor-Communication-Patterns.zh.md)**
6.  **[Framework Internal Protocols](./docs/1.2-Framework-Internal-Protocols-zh.md)**

#### **Part 2: Guides & Practices**
*Hands-on material for building with the framework.*

1.  **[Developer Guide](./docs/2-Developer-Guide.zh.md)** (quick start)
2.  **[Project Manifest and CLI](./docs/2.4-Project-Manifest-and-CLI.zh.md)** (CLI reference)
3.  **[Actor Cookbook](./docs/2.2-Actor-Cookbook.zh.md)** (advanced patterns)
4.  **[Testing Your Actors](./docs/2.3-Testing-Your-Actors.zh.md)**
5.  **[Media Sources and Tracks](./docs/2.1-Media-Sources-and-Tracks.zh.md)** (scenario-specific guidance)

#### **Part 3: How It Works**
*For contributors and readers who want implementation-level details.*

1.  **[How It Works](./docs/3-How-it-works.zh.md)** (implementation overview)
2.  **[Deep-dive Topics](./docs/)** (includes the detailed `3.1` to `3.13` documents)

#### **Appendix**

*   **[Glossary](./docs/appendix-a-glossary.zh.md)**

---

## 🚀 Quick Start

### Prerequisites

- **Rust**: 1.88+ ([install guide](https://rustup.rs/))
- **Node.js**: 16+ ([download](https://nodejs.org/))
- **protoc**: Protocol Buffer compiler
  ```bash
  # Ubuntu/Debian
  sudo apt install protobuf-compiler
  
  # macOS
  brew install protobuf
  ```

### One-Command Demo

```bash
# 1. Set up the project (install dependencies and build)
./run_demo.sh setup

# 2. Run the full demo
./run_demo.sh demo
```

## 📁 Project Structure

```
actor-rtc/
├── docs/                          # framework design documents
├── proto/                         # Protobuf definitions
├── actor-rtc-framework/          # core framework crate
├── signaling-server/             # Node.js signaling server
├── examples/                     # example applications
└── run_demo.sh                   # automation script
```

## 🤝 Contributing

Contributions of all kinds are welcome.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push the branch (`git push origin feature/AmazingFeature`)
5. Open a pull request

## Release Train (Maintainers)

Use the manual workflow `Release Train (Basic)` for the monorepo-managed
foundation crates, protoc tools, supported SDK crates, and `actr-cli` with one
shared stable version.

- Workflow file: `.github/workflows/release-train.yml`
- Local/CI entrypoint: `scripts/release-train.sh`
- Required secrets:
  - `CARGO_REGISTRY_TOKEN` — crates.io publishing
  - `PYPI_API_TOKEN` — PyPI publishing (optional; omit to skip Python)
  - `PACKAGE_SYNC_GITHUB_TOKEN` — classic PAT with `repo` scope (or
    `public_repo` when every involved repository is public) for the
    Swift/Kotlin package-sync repositories plus `read:packages` and
    `write:packages`; used to publish synchronized tags, release assets, and
    Kotlin Maven packages
- npm publishing uses Trusted Publishing (OIDC) via `id-token: write`;
  no `NPM_TOKEN` secret is needed.
- Reports are generated under `release/reports/` and uploaded as workflow
  artifacts.

The release train publishes all components in a single run:
  1. Foundation crates → protoc-gen crates → Python (optional) → SDK → CLI
  2. Final Git tag created (`vX.Y.Z`)
  3. Swift package-sync dispatched (`Actrium/actr-swift-package-sync`)
  4. Kotlin package-sync dispatched (`Actrium/actr-kotlin-package-sync`)
  5. TypeScript npm package published (`@actrium/actr` from `bindings/typescript/`)
  6. Web npm packages published (`@actrium/actr-dom`, `@actrium/actr-web`,
     `@actrium/actr-web-react`) via `bindings/web/scripts/publish.sh`

Pre-release support is available via the `pre_release` workflow input,
which accepts semver `X.Y.Z-<id>` and publishes npm packages with
`--tag pre` (does not affect the `latest` dist-tag).

## 📄 License

This project is released under the MIT License.
