# libactr - Rust FFI Layer for ACTR Bindings

This is the Rust FFI layer for the actr project, providing bindings for the Actor-RTC (actr) framework using Mozilla UniFFI.

## Overview

This crate provides the native implementation that gets compiled to a dynamic library (`libactr.dylib` on macOS, `libactr.so` on Linux, etc.) and exposes a C-compatible API that UniFFI uses to generate type-safe bindings.

## Relationship to the Rust Node Typestate

The native host exposes a typestate chain
`Node<Init> → Node<Attached> → Node<Registered> → ActrRef`
(`from_config_file` → `attach_*` → `register` → `start`), letting
system-level Rust code observe and customize each transition. The UniFFI
surface in this crate deliberately collapses that pipeline into a single
`ActrNode.newFromPackageFile(...).start()` shape so downstream Swift /
Kotlin SDKs only see the two states app developers actually care about.
The `Node<S>` typestate is Rust-layer power-user territory; bindings
should not try to re-export it. When deeper control is required
(custom `TrustProvider`, pre-built `Hyper`, attaching a generic Rust
`Workload`, etc.), use the `actr_hyper::{Hyper, Node}` API directly
from native Rust.

## Architecture

- **Core Types**: `types.rs` - Core ACTR types (ActrId, ActrType, ActrConfig, etc.)
- **Runtime**: `runtime.rs` - ACTR client wrapper and system management
- **Workload**: `workload.rs` - Workload callback interfaces
- **Error Handling**: `error.rs` - Error types and conversions
- **Library**: `lib.rs` - UniFFI scaffolding and exports

### Prerequisites

- Rust 1.95+ with `rustup`
- The actr workspace dependencies (protocol, framework, runtime, config)

### Installing UniFFI CLI

To generate bindings from the compiled library, you need to install the UniFFI CLI tool:

```bash
cargo install uniffi --features "cli"
```

This will install the `uniffi-bindgen` command globally, which can be used to generate bindings for various target languages.

### Build Commands

```bash
# Build the release library
cargo build --release

# The output will be target/release/libactr.dylib (macOS)
# or target/release/libactr.so (Linux)
```

## Development

### Code Organization

- **Rust Implementation**: Core logic in `src/` files
- **UniFFI Interface**: Defined via proc-macros in Rust code with `#[uniffi::*]` attributes

### Adding New Types/Functions

1. Implement the Rust side in the appropriate `src/*.rs` file
2. Mark functions with `#[uniffi::export]` for exposure via FFI
3. Rebuild the library after changes

### Testing

```bash
# Run Rust tests
cargo test
```

## Dependencies

This crate depends on the actr workspace crates:
- `actr-protocol`: Core protocol definitions
- `actr-framework`: Actor framework
- `actr-runtime`: Runtime implementation
- `actr-config`: Configuration management

## Features

- `default`: Empty (no default features)

## Troubleshooting

### Common Warnings

- `associated function 'new' is never used`: This is expected for some internal constructors that are only used through FFI

### Build Issues

- Ensure all workspace dependencies are available
- Check that `protoc` is installed for protobuf compilation
- Verify Rust version meets minimum requirements (1.95+)
