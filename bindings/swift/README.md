# Actr

Swift Package for distributing the ACTR (Actrium) framework via a prebuilt XCFramework.

## Overview

- **ActrFFI.xcframework**: Precompiled iOS/macOS XCFramework published through GitHub Releases (remote `binaryTarget`).
- **Actr**: Swift API that includes UniFFI-generated bindings and high-level helpers.

## Relationship to the Rust Node Typestate

The native host exposes a typestate chain
`Node<Init> → Node<Attached> → Node<Registered> → ActrRef`
(`from_config_file` → `attach_*` → `register` → `start`) so Rust-side
system code can hook into each transition. The Swift API collapses the
pipeline into a one-shot `ActrNode.from(packageConfig:packagePath:)`
followed by `start()`: iOS/macOS app developers only see the node and
the live `ActrRef`. The `Node<S>` typestate is intentionally Rust-layer
power-user territory — bindings do not re-export it. If fine-grained
control is required (custom `TrustProvider`, pre-built `Hyper`,
attaching a Rust `Workload`, etc.), use the `actr_hyper::{Hyper, Node}`
API directly from native Rust.

Package-backed Swift hosts can pass `RuntimeObservers` to
`ActrNode.from(packageConfig:packagePath:observers:)` to receive signaling and
target-scoped WebRTC readiness callbacks. Treat signaling connection as
service availability only; retry saved user intent with a fresh send after the
matching `WebRTCObserver.onConnected(ctx:event:)` target callback.

Application code importing `Actr` uses Swift-facing names such as `Context`,
`Workload`, `RpcEnvelope`, `PeerEvent`, `WebRTCObserver`, and
`WebRTCPeerStatus`. The corresponding transport-oriented types remain available
through the low-level `ActrBindings` module.

## Workspace Layout

The Swift build scripts build `libactr` from the monorepo workspace root.

```text
actr/
├── Cargo.toml                # Rust workspace root
├── bindings/
│   ├── ffi/                  # libactr crate
│   └── swift/                # Swift package sources and build scripts
└── core/                     # Rust crates required by libactr
```

The package-sync repository owns its own workflow definitions.
The monorepo release train only dispatches that external release workflow with `version`, `source_sha`, and `source_tag`.

## Consume via SwiftPM

```swift
dependencies: [
    .package(url: "https://github.com/Actrium/actr-swift-package-sync.git", from: "0.1.0")
]
```

Targets that need the SDK should depend on `Actr`.

### Local development without a published binary

For source-tree validation, use a locally built XCFramework that matches the
checked-out Swift bindings. A bare `swift build` or `swift test` in the monorepo
will fail fast unless a local binary exists. External SwiftPM consumers should
depend on the published `actr-swift` package instead of this monorepo package.

Set an environment override to point the package at a locally built XCFramework:

```bash
ACTR_BINARY_PATH=ActrFFI.xcframework swift build
```

To build fresh bindings/binaries without dirtying the git worktree, point outputs to an ignored directory:

```bash
ACTR_BINDINGS_PATH=dist/ActrBindings ACTR_BINARY_PATH=dist/ActrFFI.xcframework ./build-xcframework.sh
```

Then consume the package with the same environment variables:

```bash
ACTR_BINDINGS_PATH=dist/ActrBindings ACTR_BINARY_PATH=dist/ActrFFI.xcframework swift build
ACTR_BINDINGS_PATH=dist/ActrBindings ACTR_BINARY_PATH=dist/ActrFFI.xcframework swift test
```

`Package.swift` also auto-detects `dist/ActrFFI.xcframework` when it exists, but
passing both environment variables keeps generated bindings and the binary
artifact paired during local validation.

## Build (maintainers)

Prerequisites:
- Rust 1.95+
- Xcode Command Line Tools
- UniFFI CLI: `cargo install uniffi --features "cli"`

Steps:

```bash
./build-xcframework.sh
```

This generates Swift bindings and the multi-platform XCFramework at `ActrFFI.xcframework/`.

## Package for release

1. Build the xcframework: `./build-xcframework.sh`
2. Package and compute checksum: `./scripts/package-binary.sh v0.1.0`
   - Outputs `dist/ActrFFI.xcframework.zip` and `dist/release.txt` with the checksum and URL.
3. Update `Package.swift` defaults:
   - Set `ACTR_BINARY_TAG` (default tag) and `ACTR_BINARY_CHECKSUM` (64-hex checksum) to match `dist/release.txt`.
4. Trigger the `Actrium/actr-swift-package-sync` release workflow from the monorepo release train.
   - The external package-sync repository clones the tagged `actr` source, rebuilds the XCFramework, updates `Package.swift`, and publishes `ActrFFI.xcframework.zip`.
5. Consumers can then resolve the package without building Rust locally.

## Project Structure

- `../ffi/`: Rust FFI crate used to build the XCFramework and generate UniFFI bindings
- `ActrBindings/`: UniFFI-generated Swift bindings (Swift + headers/modulemap)
- `build-xcframework.sh`: Build script
- `scripts/package-binary.sh`: Zip + checksum helper for Release assets
- `dist/`: Local release artifacts (ignored)
- `Package.swift`: Swift Package manifest

## Configuration

UniFFI configuration lives in `../ffi/uniffi.toml`.

## Documentation

- **[API Reference](docs/api.md)** - Comprehensive API documentation covering both Low Level and High Level APIs

## License

Apache-2.0
