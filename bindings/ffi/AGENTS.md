# Repository Guidelines

## Project Structure & Module Organization

- `src/`: Rust implementation and UniFFI exports
  - `lib.rs`: module wiring and `uniffi::setup_scaffolding!()`
  - `runtime.rs`: system/node/ref wrappers (main entry points)
  - `types.rs`: FFI-safe core types (IDs, configs, enums)
  - `workload.rs`: callback interfaces and dynamic workload adapter
  - `error.rs`: `ActrError`/`ActrResult` and error conversions
- `Cargo.toml`: crate metadata (Rust 2024, `rust-version = 1.95`)
- `target/`: build artifacts (ignored; do not commit)

## Build, Test, and Development Commands

- `cargo build`: debug build for local development.
- `cargo build --release`: produces `target/release/libactr.{dylib,so,dll}` plus static libs.
- `cargo fmt --all`: format the codebase (run before opening a PR).
- `cargo clippy --all-targets --all-features -- -D warnings`: lint and fail on warnings.
- `cargo test`: compile + run tests (currently acts as a smoke/compile check).

Optional binding generation (requires UniFFI CLI):

- `cargo install uniffi --features cli`
- Example: `uniffi-bindgen generate --library target/release/libactr.dylib --language kotlin`

## Coding Style & Naming Conventions

- Follow `rustfmt` defaults (4-space indentation) and keep modules small and focused.
- Exported FFI API should stay UniFFI-friendly: avoid generics/borrowed lifetimes, prefer owned types (`String`, `Vec<u8>`, `Arc<T>`), and return `ActrResult<T>` from `#[uniffi::export]` functions.
- Naming: modules/files `snake_case`, types `CamelCase`, exported wrappers end with `Wrapper`.
- When logging or printing `ActrType` or `ActrId`, use `to_string_repr()` for stable, readable output.
- When mapping `ActrError` to `actr_protocol::ProtocolError` in workload/runtime bridges, use `ProtocolError::from` to preserve error categories.
- When bridging `Context` into `ContextBridge`, use `ContextBridge::try_from_context` and treat non-`RuntimeContext` inputs as errors.
- ABI/API stability is not guaranteed yet; coordinate breaking FFI changes via PR description.

## Testing Guidelines

- There are no dedicated test suites today; add unit tests under `src/*` with `#[cfg(test)]` or integration tests under `tests/` when you add behavior that can be exercised without networking.
- Keep tests deterministic and fast; prefer mocking boundaries over real signaling connections.

## Commit & Pull Request Guidelines

- Git history is minimal; use clear, imperative commit summaries (e.g., `feat: export discovery callback`, `fix: handle consumed system`).
- PRs should explain: what changed, why it changed, and any UniFFI/FFI surface impact. Ensure `cargo fmt`, `cargo clippy`, and a release build pass.
- Do not commit secrets or local config: `.env`, `config*.toml`, `*.key`, `*.pem`, `*.p12` (see `.gitignore`).
