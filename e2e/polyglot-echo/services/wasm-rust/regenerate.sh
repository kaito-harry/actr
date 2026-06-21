#!/usr/bin/env bash
# Regenerate src/generated/ from protos/local/echo.proto via actr CLI.
#
# Use the locally-built actr binary at $ACTR_CLI_BIN if exported, or fall
# back to PATH.  setup.sh::build_actr_cli exports ACTR_CLI_BIN; running
# this manually outside an e2e run requires `actr` on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTR="${ACTR_CLI_BIN:-actr}"

cd "$SCRIPT_DIR"
"$ACTR" gen -l rust
