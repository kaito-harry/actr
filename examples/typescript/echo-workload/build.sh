#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Build the TypeScript EchoService workload as a Component Model wasm.
#
# Usage:
#   ./build.sh          # npm install + build + componentize
#   ./build.sh package  # additionally run actr build --no-compile

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTR_ROOT="${HERE}/../../.."
OUT_WASM="${HERE}/dist/echo-typescript-0.1.0-wasm32-wasip2.wasm"

echo "[0/3] building local @actrium/actr-workload package ..."
(
    cd "${ACTR_ROOT}/bindings/typescript/actr-workload"
    npm install
    npm run build
)

echo "[1/3] installing example dependencies ..."
(
    cd "${HERE}"
    npm install
)

echo "[2/3] TypeScript build ..."
(
    cd "${HERE}"
    npm run build
)

echo "[3/3] componentize ..."
(
    cd "${HERE}"
    npm run componentize
)

if [[ -f "${OUT_WASM}" ]]; then
    ls -lh "${OUT_WASM}" | awk '{ print "    component size: " $5 "  (" $NF ")" }'
else
    echo "error: expected component missing at ${OUT_WASM}" >&2
    exit 1
fi

if [[ "${1:-}" == "package" ]]; then
    echo
    echo "[+] actr build --no-compile ..."
    SIGNING_KEY="${ACTR_SIGNING_KEY:-${HERE}/dist/dev-key.json}"
    if [[ ! -f "${SIGNING_KEY}" ]]; then
        cargo run --manifest-path "${ACTR_ROOT}/Cargo.toml" -p actr-cli -- \
            pkg keygen --output "${SIGNING_KEY}" --force >/dev/null
    fi
    cargo run --manifest-path "${ACTR_ROOT}/Cargo.toml" -p actr-cli -- \
        build --no-compile -m "${HERE}/manifest.toml" --key "${SIGNING_KEY}"
fi

echo
echo "Done. Component at: ${OUT_WASM}"
