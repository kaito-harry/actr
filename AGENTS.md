# Repository Guidelines

## Project Structure & Module Organization
- `src/` contains the primary Rust library re-exporting `actr_protocol`, `actr_framework`, and feature-gated crates. Treat it as the public API hub.
- `crates/` holds the core components (`protocol`, `framework`, `runtime`, etc.). Each subcrate is self-contained with its own `Cargo.toml`; edit inside these directories for implementation-level changes.
- `examples/` provides runnable reference apps such as `shell-actr-echo`. Mirror their layouts when wiring new services or clients.
- Docs and helper notes live in the repo root (`usage.md`, `explain.md`). Update them whenever behavior changes.

## Build, Test, and Development Commands
- `cargo build` — standard local build; run from the workspace root to compile every crate.
- `cargo check` — fast type/lint verification; run after editing any crate to ensure shared interfaces still compile.
- `cargo test` — executes the full test suite, including per-crate tests in `crates/*`.
- `actr gen --input=../echo-service/proto --output=../echo-service/src/generated --clean` — regenerates protobuf + actor scaffolding for the `echo-service` sample (run from workspace root).

### WASM Component Model toolchain (Phase 1+)

The native `wasm-engine` feature loads guests as WASM Component Model
components. Rebuilding the test fixture and shipping `.actr` packages
targeted at `wasm32-wasip2` requires pinned versions of two out-of-tree
tools:

```
rustup target add wasm32-wasip2
cargo install wasm-component-ld --version 0.5.26 --locked
cargo install wasm-tools        --version 1.247.0 --locked
```

`wasm-component-ld 0.5.22` is the first release that recognises the
async-ABI custom sections wit-bindgen 0.57 emits. `wasm-tools`
is pinned separately for component validation and diagnostics. CI
installs both via `cargo install --version` so local and CI behaviour
match exactly.

## Coding Style & Naming Conventions
- Follow Rust 2024 idioms: four-space indentation, snake_case for modules/functions, CamelCase for types.
- Run `rustfmt` (same options used by `actr gen`) before committing: `cargo fmt --all`.
- Keep comments concise and purposeful; prefer English for inline docs even when user-facing docs are localized.
- When logging/returning/tracing `ActrId`, always use `ActrId::to_string_repr()` (applies to logs, errors, and tracing spans, including `#[instrument]` fields).
- When logging/returning/tracing `ActrType`, always use `ActrType::to_string_repr()` instead of manual manufacturer/name formatting.

## Testing Guidelines
- Unit tests live beside implementation files; integration tests belong in `tests/` when present.
- Use `#[cfg(test)] mod tests` patterns and meaningful test names (`test_actor_registration_flow`).
- Execute targeted tests with `cargo test -p crate_name` when iterating on a specific component; finish with a workspace-wide `cargo test` before merging.

## Commit & Pull Request Guidelines
- All commits and PR titles MUST follow [Conventional Commits](https://www.conventionalcommits.org/): `<type>[optional scope]: <description>`.
- Valid types: `feat`, `fix`, `chore`, `docs`, `style`, `refactor`, `test`, `ci`, `perf`, `build`, `revert`.
- Breaking changes: append `!` after type/scope (e.g. `feat!:`) or include `BREAKING CHANGE` in the footer.
- The release train auto-detects semver bump from PR titles: `feat` → MINOR, `fix` → PATCH, `feat!` / `BREAKING CHANGE` → MAJOR.

### Commit type selection for 0.x development

During 0.x early development, prefer conservative bumping to avoid unnecessary version inflation:

- **`fix:` (PATCH)** — The default for most changes. Use for bug fixes, small enhancements, internal helpers, FFI bindings, incremental refinements, and non-user-facing improvements.
- **`feat:` (MINOR)** — Use ONLY when exposing a clearly new end-user-facing capability or API surface. Do NOT use for internal helpers, small refinements, or incremental polish.
- **`feat!:` / `BREAKING CHANGE` (MAJOR)** — Public API removal, signature changes that break callers, or incompatible protocol changes.
- **`refactor:` (no bump)** — Pure internal restructuring with zero behavioral change.
- **`chore:`, `ci:`, `build:`, `docs:`, `test:` (no bump)** — Maintenance, CI, dependencies, documentation, tests.

- Each PR should describe the change scope, mention affected crates or directories, and link to any relevant issues or design docs.
- Include reproduction or validation steps (commands, screenshots, or log excerpts) so reviewers can verify behavior quickly.
- Ensure generated files are up to date (`actr gen …`) and that formatters/tests have been run prior to opening a PR.

## Additional Tips
- Regenerating code may fail if `src/generated` files are read-only; run `chmod -R u+w src/generated` beforehand.
- Signaling-related examples expect the signaling server at `ws://localhost:8081/signaling/ws`; document deviations if you change endpoints.
- **Tracing subscriber initialization**: Errors from `tracing_subscriber::registry().try_init()` are intentionally ignored (using `let _ = ...`). This is by design because `try_init()` only reports errors when the subscriber has already been initialized elsewhere (e.g., in tests or multiple initialization attempts). Silently discarding these errors is the correct behavior and prevents false alarms.
- Credential refresh is intentionally on-demand (e.g., triggered by signaling warnings) with no background scheduling; do not reintroduce periodic refresh loops or automatic retries unless explicitly requested.
