#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Build the echo Python workload as a Component Model wasm via the
# actr-workload authoring package, and optionally pack it into a signed `.actr`.
#
# Required tools (versions known to work against this WIT contract):
#
#   - python3           >= 3.11    (componentize-py host interpreter;
#                                    distinct from the CPython *guest*
#                                    interpreter that componentize-py
#                                    bundles into the Component)
#   - pip               >= 23
#   - actr-workload[build]         (pins componentize-py==0.23.0)
#   - wasm-tools        >= 1.219   (component metadata verification)
#
# Optional:
#
#   - actr CLI                     (workspace root: cargo run -p actr-cli -- build ...)
#   - ACTR_SIGNING_KEY             (optional key path for `./build.sh package`;
#                                    defaults to dist/dev-key.json)
#   - wasm-pack                    (only needed when packaging must regenerate
#                                    the CLI web runtime assets first)
#
# Toolchain reality: componentize-py downloads a prebuilt CPython WASM
# interpreter on first use (cached under ~/.cache/componentize-py or the
# pip wheels directory). Internet access is required the first time.
#
# Size warning: the resulting Component is large because it embeds a
# CPython WASM interpreter plus the standard library
# subset that componentize-py's bundler selects. For size-sensitive
# deployments, prefer the Go / C / Rust examples.
#
# Usage:
#   ./build.sh              # install deps + bindings + componentize + verify
#   ./build.sh package      # additionally run `actr build --no-compile`

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WIT_FILE="${HERE}/../../../core/framework/wit/actr-workload.wit"
WORLD="actr-workload-guest"
WORLD_MODULE="actr_workload_bindings"
ACTR_WORKLOAD_SRC="${HERE}/../../../bindings/python/actr-workload/src"

BINDINGS_DIR="${HERE}/bindings"
DIST_DIR="${HERE}/dist"
OUT_WASM="${DIST_DIR}/echo-python-0.1.0-wasm32-wasip2.wasm"

VENV_DIR="${HERE}/.venv"

if [[ ! -f "${WIT_FILE}" ]]; then
    echo "error: WIT contract not found at ${WIT_FILE}" >&2
    exit 1
fi

# ── 0. Prepare an isolated Python venv ───────────────────────────────────────
#
# componentize-py and its bundled CPython-WASM wheels are heavy; keep
# them out of the user's global site-packages.
if [[ ! -d "${VENV_DIR}" ]]; then
    echo "[0/4] creating venv at ${VENV_DIR} ..."
    python3 -m venv "${VENV_DIR}"
fi
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

echo "[0/4] installing actr-workload build helpers ..."
pip install --upgrade pip >/dev/null
(
    cd "${HERE}"
    pip install -e "../../../bindings/python/actr-workload[build]"
)

# ── 1. Generate Python bindings from WIT ─────────────────────────────────────
#
# The `bindings` subcommand emits a Python package tree under
# `actr_workload_bindings`, keeping generated code separate from the
# authoring package imported by workload.py:
#
#   actr_workload_bindings/__init__.py
#   actr_workload_bindings/exports/__init__.py
#   actr_workload_bindings/imports/host.py
#   actr_workload_bindings/imports/types.py

echo "[1/4] componentize-py bindings ..."
rm -rf "${BINDINGS_DIR}"
actr-workload bindings "${BINDINGS_DIR}" \
    --world "${WORLD}" \
    --world-module "${WORLD_MODULE}"

# ── 2. Bundle workload.py + bindings + CPython into a Component ──────────────
#
# The `componentize` subcommand takes the module name (here `workload`,
# which resolves to workload.py in the working directory) and produces
# a wasm32-wasip2 Component that exports the world's workload interface.
# componentize-py embeds a CPython WASM interpreter plus the subset of
# the stdlib that its bundler decides is reachable.

echo "[2/4] componentize-py componentize ..."
mkdir -p "${DIST_DIR}"
(
    cd "${HERE}"
    actr-workload componentize workload \
        -o "${OUT_WASM}" \
        --project-dir "${HERE}" \
        --bindings-dir "${BINDINGS_DIR}" \
        --world "${WORLD}" \
        --world-module "${WORLD_MODULE}" \
        --python-path "${ACTR_WORKLOAD_SRC}"
)

# ── 3. Verify world / interfaces via wasm-tools ──────────────────────────────
echo "[3/4] wasm-tools component wit (verify world) ..."
wasm-tools component wit "${OUT_WASM}" | tee "${DIST_DIR}/echo-python.wit.txt"

if grep -q "actr:workload" "${DIST_DIR}/echo-python.wit.txt"; then
    echo
    echo "OK: emitted Component references actr:workload interfaces"
else
    echo "FAIL: actr:workload interfaces not found in component metadata" >&2
    exit 1
fi

# ── 4. Report size (the 10 MB warning) ───────────────────────────────────────
echo "[4/4] size report ..."
ls -lh "${OUT_WASM}" | awk '{ print "    component size: " $5 "  (" $NF ")" }'

# ── Optional: pack into .actr ────────────────────────────────────────────────
if [[ "${1:-}" == "package" ]]; then
    echo
    echo "[+] actr build --no-compile ..."
    ACTR_ROOT="${HERE}/../../.."
    CLI_SW_HOST_WASM="${ACTR_ROOT}/cli/assets/web-runtime/actr_sw_host_bg.wasm"
    CLI_SW_HOST_JS="${ACTR_ROOT}/cli/assets/web-runtime/actr_sw_host.js"
    if [[ ! -f "${CLI_SW_HOST_WASM}" || ! -f "${CLI_SW_HOST_JS}" ]]; then
        echo "[+] generating CLI web runtime assets ..."
        (
            cd "${ACTR_ROOT}"
            bash bindings/web/scripts/sync-cli-assets.sh --build
        )
    fi
    SIGNING_KEY="${ACTR_SIGNING_KEY:-${DIST_DIR}/dev-key.json}"
    if [[ ! -f "${SIGNING_KEY}" ]]; then
        echo "[+] generating development signing key at ${SIGNING_KEY} ..."
        KEYGEN_HOME="${DIST_DIR}/.actr-keygen-home"
        KEYGEN_CARGO_HOME="${CARGO_HOME:-}"
        KEYGEN_RUSTUP_HOME="${RUSTUP_HOME:-}"
        if [[ -z "${KEYGEN_CARGO_HOME}" && -n "${HOME:-}" ]]; then
            KEYGEN_CARGO_HOME="${HOME}/.cargo"
        fi
        if [[ -z "${KEYGEN_RUSTUP_HOME}" && -n "${HOME:-}" ]]; then
            KEYGEN_RUSTUP_HOME="${HOME}/.rustup"
        fi
        KEYGEN_ENV=(HOME="${KEYGEN_HOME}")
        if [[ -n "${KEYGEN_CARGO_HOME}" ]]; then
            KEYGEN_ENV+=(CARGO_HOME="${KEYGEN_CARGO_HOME}")
        fi
        if [[ -n "${KEYGEN_RUSTUP_HOME}" ]]; then
            KEYGEN_ENV+=(RUSTUP_HOME="${KEYGEN_RUSTUP_HOME}")
        fi
        mkdir -p "${KEYGEN_HOME}"
        (
            cd "${HERE}"
            env "${KEYGEN_ENV[@]}" cargo run --manifest-path "${ACTR_ROOT}/Cargo.toml" -p actr-cli -- \
                pkg keygen --output "${SIGNING_KEY}" --force >/dev/null
        )
    fi
    (
        cd "${HERE}"
        cargo run --manifest-path "${ACTR_ROOT}/Cargo.toml" -p actr-cli -- \
            build --no-compile -m manifest.toml --key "${SIGNING_KEY}"
    )
fi

echo
echo "Done. Component at: ${OUT_WASM}"
