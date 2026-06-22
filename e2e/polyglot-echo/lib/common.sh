#!/usr/bin/env bash
# Shared shell helpers for the polyglot-echo e2e scenario.
#
# This file is a slimmer cousin of `e2e/package-runtime-echo/lib/common.sh`:
# the polyglot scenario runs against the in-tree `mock-actrix` binary, so the
# `ensure_actrix_available` / admin-login / AIS warmup machinery is dropped.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

section() {
    echo ""
    echo -e "${BLUE}$1${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

warn() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

fail() {
    echo -e "${RED}❌ $1${NC}" >&2
    exit 1
}

require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        fail "Required command not found: $cmd"
    fi
}

absolute_path() {
    local input="$1"
    if [ -d "$input" ]; then
        (cd "$input" && pwd)
        return 0
    fi
    local dir base
    dir="$(dirname "$input")"
    base="$(basename "$input")"
    (cd "$dir" && printf '%s/%s\n' "$(pwd)" "$base")
}

kill_listener() {
    local protocol="$1"
    local port="$2"
    local pids=""
    case "$protocol" in
        tcp) pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)" ;;
        udp) pids="$(lsof -tiUDP:"$port" 2>/dev/null || true)" ;;
        *)   fail "Unsupported protocol for kill_listener: $protocol" ;;
    esac
    if [ -n "$pids" ]; then
        echo "Releasing ${protocol^^} port $port..."
        kill $pids 2>/dev/null || true
        sleep 1
    fi
}

# Reap `actr run` server processes left behind by a previous polyglot-echo
# run. They connect OUTBOUND to the signaling port (they are clients, not
# listeners), so `kill_listener` never sees them; orphaned by a hard-killed
# run (e.g. the EXIT trap could not fire), they keep reconnecting to the
# fixed HTTP_PORT and re-register with the fresh mock-actrix under a stale
# ACL — surfacing later as a baffling "ACL denied" on the new run. The match
# is pinned to this scenario's per-run config path (`polyglot-echo/.tmp/run-`),
# so unrelated `actr` processes elsewhere on the host are never touched.
reap_stale_polyglot_servers() {
    local pids
    pids="$(pgrep -f 'actr run -c .*/polyglot-echo/\.tmp/run-' 2>/dev/null || true)"
    if [ -n "$pids" ]; then
        warn "Reaping stale polyglot-echo server(s) from a previous run: $pids"
        # SIGKILL: these are orphans that already ignored normal teardown.
        kill -9 $pids 2>/dev/null || true
    fi
}

wait_for_http_ok() {
    local url="$1"
    local timeout="$2"
    local started_at
    started_at="$(date +%s)"
    while true; do
        if curl -fsS "$url" >/dev/null 2>&1; then
            return 0
        fi
        local now
        now="$(date +%s)"
        if [ $((now - started_at)) -ge "$timeout" ]; then
            return 1
        fi
        sleep 1
    done
}

# Render a template by literal-key substitution. Each remaining argument is of
# the form `KEY=VALUE`; every occurrence of `KEY` in the template is replaced
# with `VALUE` (special characters are escaped for sed).
render_template() {
    local src="$1"
    local dst="$2"
    shift 2
    cp "$src" "$dst"
    while [ $# -gt 0 ]; do
        local key="${1%%=*}"
        local value="${1#*=}"
        local escaped
        escaped="$(printf '%s' "$value" | sed -e 's/[\\/&]/\\&/g')"
        sed -i.bak "s|$key|$escaped|g" "$dst"
        rm -f "$dst.bak"
        shift
    done
}

json_field() {
    local file="$1"
    local query="$2"
    jq -er "$query" "$file"
}

# Append a `[patch.crates-io]` block routing the actr workspace crates at the
# given repo root. Idempotent: skipped if the file already has such a block.
append_workspace_patch() {
    local cargo_toml="$1"
    local repo_path="$2"

    if ! grep -q '^\[workspace\]' "$cargo_toml"; then
        cat >>"$cargo_toml" <<'EOF'

[workspace]
EOF
    fi

    if grep -q '^\[patch\.crates-io\]' "$cargo_toml"; then
        return 0
    fi

    cat >>"$cargo_toml" <<EOF

[patch.crates-io]
actr = { path = "$repo_path" }
actr-protocol = { path = "$repo_path/core/protocol" }
actr-framework = { path = "$repo_path/core/framework" }
actr-hyper = { path = "$repo_path/core/hyper" }
actr-runtime = { path = "$repo_path/core/runtime" }
actr-config = { path = "$repo_path/core/config" }
actr-service-compat = { path = "$repo_path/core/service-compat" }
actr-runtime-mailbox = { path = "$repo_path/core/runtime-mailbox" }
EOF
}

# Write a project-local `.actr/config.toml` that pins the manufacturer keychain
# path. Used by `actr build` / `actr registry publish`.
write_project_keychain_config() {
    local project_dir="$1"
    local keychain_path="$2"
    mkdir -p "$project_dir/.actr"
    cat >"$project_dir/.actr/config.toml" <<EOF
[mfr]
keychain = "$keychain_path"
EOF
}
