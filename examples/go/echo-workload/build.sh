#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Build the Go echo workload for the async actr:workload@0.2.0 world.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WIT_FILE="${HERE}/../../../core/framework/wit-v2/actr-workload.wit"
WORLD="actr-workload-guest-v2"
GEN_DIR="${HERE}/gen"
BUILD_DIR="${HERE}/build"
DIST_DIR="${HERE}/dist"
CORE_WASM="${BUILD_DIR}/echo-go.core.wasm"
EMBEDDED_WASM="${BUILD_DIR}/echo-go.embedded.wasm"
OUT_WASM="${DIST_DIR}/echo-go-0.1.0-wasm32-wasip2.wasm"
WIT_DUMP="${DIST_DIR}/echo-go.wit.txt"

WIT_BINDGEN="${WIT_BINDGEN:-wit-bindgen}"
ACTR_GO="${ACTR_GO:-go}"
WASM_TOOLS="${WASM_TOOLS:-wasm-tools}"

WIT_BINDGEN_VERSION="wit-bindgen-cli 0.59.0"
GO_VERSION="go1.25.5"
WASM_TOOLS_VERSION="wasm-tools 1.253.0"
ADAPTER_URL="https://github.com/bytecodealliance/wasmtime/releases/download/v46.0.1/wasi_snapshot_preview1.reactor.wasm"
ADAPTER_SHA256="0acb10959bd3c1d2e7903ef82212910bfc156ab5698ef3f2ff669474ba59fb0a"
WASI_ADAPTER="${WASI_ADAPTER:-${HERE}/.cache/wasi_snapshot_preview1.reactor-v46.0.1.wasm}"

require_version() {
    local actual="$1"
    local expected="$2"
    local tool="$3"
    if [[ "${actual}" != *"${expected}"* ]]; then
        echo "error: ${tool} must report ${expected}; got: ${actual}" >&2
        exit 1
    fi
}

require_version "$("${WIT_BINDGEN}" --version)" "${WIT_BINDGEN_VERSION}" "wit-bindgen"
require_version "$("${ACTR_GO}" version)" "${GO_VERSION}" "patched Go"
require_version "$("${WASM_TOOLS}" --version)" "${WASM_TOOLS_VERSION}" "wasm-tools"

if [[ ! -f "${WIT_FILE}" ]]; then
    echo "error: V2 WIT contract not found at ${WIT_FILE}" >&2
    exit 1
fi

if [[ ! -f "${WASI_ADAPTER}" ]]; then
    mkdir -p "$(dirname "${WASI_ADAPTER}")"
    curl --fail --location --retry 3 --retry-all-errors \
        --output "${WASI_ADAPTER}" \
        "${ADAPTER_URL}"
fi
echo "${ADAPTER_SHA256}  ${WASI_ADAPTER}" | sha256sum --check --status

rm -rf "${GEN_DIR}" "${BUILD_DIR}" "${DIST_DIR}"
mkdir -p \
    "${GEN_DIR}/export_actr_workload_workload" \
    "${BUILD_DIR}" \
    "${DIST_DIR}"

echo "[1/5] Generate async Go bindings ..."
"${WIT_BINDGEN}" go \
    --world "${WORLD}" \
    --out-dir "${GEN_DIR}" \
    "${WIT_FILE}"
cp \
    "${HERE}/src/export_actr_workload_workload/workload.go" \
    "${GEN_DIR}/export_actr_workload_workload/workload.go"

echo "[2/5] Resolve the generated Go module ..."
(
    cd "${GEN_DIR}"
    "${ACTR_GO}" mod tidy
)

echo "[3/5] Build the wasm32-wasip1 reactor core module ..."
(
    cd "${GEN_DIR}"
    GOOS=wasip1 GOARCH=wasm "${ACTR_GO}" build \
        -buildmode=c-shared \
        -ldflags=-checklinkname=0 \
        -o "${CORE_WASM}"
)

echo "[4/5] Embed V2 WIT and create the Component ..."
"${WASM_TOOLS}" component embed \
    --world "${WORLD}" \
    "${WIT_FILE}" \
    "${CORE_WASM}" \
    -o "${EMBEDDED_WASM}"
"${WASM_TOOLS}" component new \
    --adapt "${WASI_ADAPTER}" \
    "${EMBEDDED_WASM}" \
    -o "${OUT_WASM}"

echo "[5/5] Validate the V2 component ..."
"${WASM_TOOLS}" validate "${OUT_WASM}"
"${WASM_TOOLS}" component wit "${OUT_WASM}" > "${WIT_DUMP}"
grep -q "actr:workload/host@0.2.0" "${WIT_DUMP}"
grep -q "actr:workload/workload@0.2.0" "${WIT_DUMP}"

if [[ "${1:-}" == "package" ]]; then
    echo "[+] Package with actr ..."
    (
        cd "${HERE}"
        cargo run --manifest-path "${HERE}/../../../Cargo.toml" -p actr-cli -- \
            build --no-compile -m manifest.toml
    )
fi

echo "Done. Component at: ${OUT_WASM}"
