#!/usr/bin/env bash
# Phase 0.5 async spike: build guest Component + host, run all 8 tests.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
GUEST_TARGET_DIR="$REPO_ROOT/target/experiments-component-spike-async-guest"
cd "$HERE"

# Use wasm-component-ld (>=0.5.22) from ~/.cargo/bin for async component
# custom sections; 0.5.22 is the first release that parses the
# wit-bindgen 0.57 async component-type sections.
NEW_LD="${HOME}/.cargo/bin/wasm-component-ld"
if [[ ! -x "$NEW_LD" ]]; then
    echo "ERROR: wasm-component-ld not found at $NEW_LD" >&2
    echo "install with: cargo install wasm-component-ld --version 0.5.22" >&2
    exit 1
fi
LD_VER=$("$NEW_LD" --version | awk '{print $2}')
echo "using wasm-component-ld $LD_VER"

echo "=== [1/3] building async guest (wasm32-wasip2) ==="
pushd guest >/dev/null
RUSTFLAGS="-Clinker=$NEW_LD" cargo build --release --target wasm32-wasip2
GUEST_WASM="$GUEST_TARGET_DIR/wasm32-wasip2/release/spike_guest_async.wasm"
popd >/dev/null

echo
echo "=== [2/3] inspecting guest Component metadata ==="
wasm-tools component wit "$GUEST_WASM" | head -70 || true
SIZE_UNSTRIPPED=$(stat -c %s "$GUEST_WASM")
echo "unstripped size: ${SIZE_UNSTRIPPED} bytes"

STRIPPED="$GUEST_TARGET_DIR/wasm32-wasip2/release/spike_guest_async.stripped.wasm"
wasm-tools strip "$GUEST_WASM" -o "$STRIPPED" 2>/dev/null || cp "$GUEST_WASM" "$STRIPPED"
SIZE_STRIPPED=$(stat -c %s "$STRIPPED")
echo "stripped   size: ${SIZE_STRIPPED} bytes"

# Async components embed `context.get` which requires --features=all (or the
# equivalent async feature flag). Sanity-validate explicitly.
echo
if wasm-tools validate --features=all "$GUEST_WASM" 2>&1; then
    echo "validate (--features=all) OK"
else
    echo "WARNING: validate failed — see above"
fi

echo
echo "=== [3/3] building + running async host ==="
pushd host >/dev/null
cargo run --release --quiet -- "$GUEST_WASM"
popd >/dev/null

echo
echo "=== async spike build.sh OK ==="
