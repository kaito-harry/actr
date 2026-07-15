//! Build script for `actr-hyper`.
//!
//! When the `wasm-engine` + `test-utils` features are both enabled (the
//! integration-test configuration, never a publish build), compiles the
//! `wasm_actor_fixture` guest crate (`tests/wasm_actor_fixture/`) to a
//! wasm32-wasip2 Component Model component and exposes its bytes via the
//! `ACTR_WASM_FIXTURE` env var + `actr_wasm_fixture_available` cfg. Tests
//! then `include_bytes!` the built artifact instead of carrying a 15k-line
//! hex blob in source.
//!
//! Requires the `wasm32-wasip2` target and `wasm-component-ld` (>= 0.5.22).
//! A local build without them skips with a `cargo:warning` (the fixture
//! tests are compiled out); CI sets `ACTR_REQUIRE_WASM_FIXTURE=1` to make a
//! missing toolchain a hard failure so the tests can't silently go green.

use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Declare the custom cfg unconditionally so `unexpected_cfgs` doesn't fire
    // on the test files gating on it, even when this script skips.
    println!("cargo::rustc-check-cfg=cfg(actr_wasm_fixture_available)");

    // Re-run when the gating env vars change.
    println!("cargo::rerun-if-env-changed=WASM_COMPONENT_LD");
    println!("cargo::rerun-if-env-changed=ACTR_REQUIRE_WASM_FIXTURE");

    // Only compile the fixture for the integration-test configuration: both
    // wasm-engine and test-utils must be on. `test-utils` is a dev/test
    // feature (not enabled by `cargo package`), so publish builds skip this.
    if env::var_os("CARGO_FEATURE_WASM_ENGINE").is_none()
        || env::var_os("CARGO_FEATURE_TEST_UTILS").is_none()
    {
        return;
    }

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo"));
    let guest_dir = manifest_dir.join("tests/wasm_actor_fixture");
    let wit = manifest_dir.join("../framework/wit/actr-workload.wit");

    // Publish builds (`cargo package`) strip `tests/`, so the guest source is
    // absent — skip silently rather than fail. This is expected, not an error.
    if !guest_dir.join("Cargo.toml").exists() {
        return;
    }

    let require = env::var_os("ACTR_REQUIRE_WASM_FIXTURE").is_some();

    // Rebuild when inputs change.
    println!(
        "cargo:rerun-if-changed={}",
        guest_dir.join("src/lib.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        guest_dir.join("Cargo.toml").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        guest_dir.join("Cargo.lock").display()
    );
    println!("cargo:rerun-if-changed={}", wit.display());

    let ld = match find_wasm_component_ld() {
        Some(path) => path,
        None => {
            missing_toolchain(
                require,
                "wasm-component-ld was not found on PATH or in ~/.cargo/bin",
                "cargo install wasm-component-ld --version 0.5.22",
            );
            return;
        }
    };

    if !target_installed("wasm32-wasip2") {
        missing_toolchain(
            require,
            "the wasm32-wasip2 target is not installed",
            "rustup target add wasm32-wasip2",
        );
        return;
    }

    let cargo = env::var_os("CARGO").expect("CARGO set by cargo");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR set by cargo"));
    // Isolated target-dir so the nested `cargo build` never contends for the
    // host workspace's target-dir locks (the guest is its own workspace).
    let guest_target_dir = out_dir.join("wasm-guest-target");

    // The guest is a separate workspace that path-depends on the framework
    // crates, so its Cargo.lock pins their versions. A routine workspace version
    // bump (driven by the release train, which does not know about this nested
    // lock) would make a `--locked` build hard-fail here and break CI on every
    // release. So we do NOT pass `--locked`: cargo resolves against the committed
    // lock and only rewrites it when a bump left it stale. The committed lock is
    // a resolution hint, not an enforced pin. To keep the build from dirtying the
    // source tree (cargo writes Cargo.lock next to the manifest), snapshot the
    // committed lock and restore it if the build rewrote it.
    let guest_lock = guest_dir.join("Cargo.lock");
    let lock_snapshot = std::fs::read(&guest_lock).ok();

    // Pin the Component Model linker via the target-specific env (highest
    // precedence) and strip any inherited RUSTFLAGS so they can't override it
    // (see Cargo's build.rustflags precedence). Also drop the coverage
    // instrumentation wrappers (RUSTC_WRAPPER / RUSTC_WORKSPACE_WRAPPER, set by
    // the Coverage CI job via cargo-llvm-cov): the wasm fixture is a test-time
    // build artifact, not a host coverage instrumentation target, and the
    // wrapper is invalid for the wasm32-wasip2 target.
    let status = Command::new(&cargo)
        .args(["build", "--release", "--target", "wasm32-wasip2"])
        .current_dir(&guest_dir)
        .env("CARGO_TARGET_WASM32_WASIP2_LINKER", &ld)
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env("CARGO_TARGET_DIR", &guest_target_dir)
        .status()
        .expect("failed to spawn `cargo build` for wasm_actor_fixture");

    // Restore the committed lock if cargo updated it, so the build leaves the
    // working tree untouched.
    if let Some(snapshot) = &lock_snapshot {
        if std::fs::read(&guest_lock).ok().as_deref() != Some(snapshot.as_slice()) {
            let _ = std::fs::write(&guest_lock, snapshot);
        }
    }

    if !status.success() {
        panic!(
            "`cargo build` of wasm_actor_fixture (wasm32-wasip2) failed; \
             set ACTR_REQUIRE_WASM_FIXTURE=1 locally to reproduce"
        );
    }

    let wasm = guest_target_dir.join("wasm32-wasip2/release/wasm_actor_fixture.wasm");
    if !wasm.exists() {
        panic!("expected guest artifact not found: {}", wasm.display());
    }

    let dest = out_dir.join("wasm_actor_fixture.wasm");
    std::fs::copy(&wasm, &dest).expect("failed to copy built wasm fixture into OUT_DIR");

    println!("cargo::rustc-env=ACTR_WASM_FIXTURE={}", dest.display());
    println!("cargo::rustc-cfg=actr_wasm_fixture_available");
}

/// Handle a missing wasm toolchain: hard-fail when `require` (CI), otherwise
/// emit a `cargo:warning` and skip so a missing local toolchain never fails.
fn missing_toolchain(require: bool, reason: &str, install_hint: &str) {
    if require {
        panic!(
            "ACTR_REQUIRE_WASM_FIXTURE is set but {reason}; install with \
             `{install_hint}` (fixture tests cannot be compiled out in CI)"
        );
    }
    println!("cargo:warning=wasm fixture toolchain missing: {reason};");
    println!("cargo:warning=  install with: `{install_hint}`");
    println!("cargo:warning=  wasm_actor_fixture integration tests will be compiled out.");
    println!("cargo:warning=  (set ACTR_REQUIRE_WASM_FIXTURE=1 to make this a hard error)");
}

/// Locate `wasm-component-ld`. An explicit `WASM_COMPONENT_LD` override is
/// honoured as-is: if it points nowhere the toolchain is treated as
/// unavailable (no silent fallback), so `WASM_COMPONENT_LD=/nonexistent`
/// deterministically forces the skip / require path. Without an override,
/// `PATH` then `~/.cargo/bin` are consulted.
fn find_wasm_component_ld() -> Option<PathBuf> {
    if let Some(path) = env::var_os("WASM_COMPONENT_LD") {
        let path = PathBuf::from(path);
        return if path.is_file() { Some(path) } else { None };
    }
    find_in_path("wasm-component-ld").or_else(|| {
        let home = env::var_os("HOME")?;
        let candidate = PathBuf::from(home).join(".cargo/bin/wasm-component-ld");
        if candidate.is_file() {
            Some(candidate)
        } else {
            None
        }
    })
}

fn find_in_path(cmd: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .map(|dir| dir.join(cmd))
        .find(|p| p.is_file())
}

/// Check whether the `wasm32-wasip2` target std libs are installed by looking
/// in the rustc sysroot (works with non-rustup toolchains, unlike
/// `rustup target list`).
fn target_installed(target: &str) -> bool {
    let rustc = env::var("RUSTC").unwrap_or_else(|_| String::from("rustc"));
    let output = match Command::new(&rustc).args(["--print", "sysroot"]).output() {
        Ok(output) => output,
        Err(_) => return false,
    };
    if !output.status.success() {
        return false;
    }
    let sysroot = String::from_utf8_lossy(&output.stdout);
    PathBuf::from(sysroot.trim())
        .join("lib/rustlib")
        .join(target)
        .exists()
}
