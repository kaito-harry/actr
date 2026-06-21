#!/usr/bin/env bash
# polyglot-echo end-to-end runner.
#
# Spins up a fresh mock-actrix + EchoService instance (chosen server form),
# then drives a single Echo round-trip (or streaming scenario) with the
# Rust client driver.
#
# Usage:
#   bash e2e/polyglot-echo/run.sh                            # defaults: --server cdylib-rust --client rust --scenario echo
#   bash e2e/polyglot-echo/run.sh --server cdylib-rust       # default Rust cdylib package server
#   bash e2e/polyglot-echo/run.sh --server linked-rust       # in-process Rust linked workload
#   bash e2e/polyglot-echo/run.sh --server wasm-rust         # Rust Wasm Component (hyper wasm-engine)
#   bash e2e/polyglot-echo/run.sh --client rust              # Rust driver, echo scenario
#   bash e2e/polyglot-echo/run.sh --client rust --scenario server-stream
#   bash e2e/polyglot-echo/run.sh --client rust --scenario bidi
#   bash e2e/polyglot-echo/run.sh --client rust hello        # custom message
#
# Honoured environment variables:
#   HTTP_PORT    Override mock-actrix port (default 18181).
#   ICE_PORT     Override STUN/TURN port advertised in configs (default 13478).
#   REALM_ID     Override the seeded realm id (default 4242).
#   KEEP_TMP=1   Preserve $RUN_DIR (mock + server logs, runtime configs).
#   RUST_LOG     Forwards to the server + Rust driver.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
export REPO_ROOT SCRIPT_DIR

source "$SCRIPT_DIR/lib/common.sh"

SERVER="cdylib-rust"
CLIENT="rust"
SCENARIO="echo"
TEST_MESSAGE="polyglot-default"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --server)
            [[ $# -lt 2 ]] && fail "Missing value for --server"
            SERVER="$2"
            shift 2
            ;;
        --server=*)
            SERVER="${1#--server=}"
            shift
            ;;
        --client)
            [[ $# -lt 2 ]] && fail "Missing value for --client"
            CLIENT="$2"
            shift 2
            ;;
        --client=*)
            CLIENT="${1#--client=}"
            shift
            ;;
        --scenario)
            [[ $# -lt 2 ]] && fail "Missing value for --scenario"
            SCENARIO="$2"
            shift 2
            ;;
        --scenario=*)
            SCENARIO="${1#--scenario=}"
            shift
            ;;
        --help|-h)
            sed -n '2,16p' "$0"
            exit 0
            ;;
        -*)
            fail "Unknown option: $1"
            ;;
        *)
            TEST_MESSAGE="$1"
            shift
            ;;
    esac
done

case "$SERVER" in
    cdylib-rust|linked-rust|wasm-rust) ;;
    *)
        fail "Unknown --server value: $SERVER (expected cdylib-rust|linked-rust|wasm-rust)"
        ;;
esac

case "$CLIENT" in
    rust | ts) ;;
    *)
        fail "Unknown --client value: $CLIENT (expected rust|ts)"
        ;;
esac

case "$SCENARIO" in
    echo|server-stream|bidi) ;;
    *)
        fail "Unknown --scenario value: $SCENARIO (expected echo|server-stream|bidi)"
        ;;
esac

# Streaming scenarios are independent of the chosen --server form: they always
# spin up an additional linked stream-server (e2e/polyglot-echo/server/) and
# the client driver picks STREAM_SERVICE_TYPE for those scenarios.  The
# --server form only affects which echo workload is reachable under
# SERVICE_TYPE.

RUN_ID="$(date +%Y%m%d-%H%M%S)-$RANDOM"
RUN_DIR="$SCRIPT_DIR/.tmp/run-$RUN_ID"
STATE_DIR="$RUN_DIR/state"
LOG_DIR="$RUN_DIR/logs"
DIST_DIR="$RUN_DIR/dist"
mkdir -p "$STATE_DIR" "$LOG_DIR" "$DIST_DIR"
export RUN_DIR STATE_DIR LOG_DIR DIST_DIR

cleanup() {
    local status=$?
    if [ -n "${STREAM_SERVER_PID:-}" ] && kill -0 "$STREAM_SERVER_PID" 2>/dev/null; then
        kill "$STREAM_SERVER_PID" 2>/dev/null || true
    fi
    if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    if [ -n "${MOCK_PID:-}" ] && kill -0 "$MOCK_PID" 2>/dev/null; then
        kill "$MOCK_PID" 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    if [ $status -eq 0 ] && [ "${KEEP_TMP:-0}" != "1" ]; then
        rm -rf "$RUN_DIR"
    else
        echo ""
        echo "Artifacts preserved at: $RUN_DIR"
    fi
}
trap cleanup EXIT INT TERM

source "$SCRIPT_DIR/lib/setup.sh"

section "🧪 polyglot-echo (--server $SERVER --client $CLIENT --scenario $SCENARIO)"
echo "Run dir:     $RUN_DIR"
echo "HTTP port:   $HTTP_PORT"
echo "ICE port:    $ICE_PORT"
echo "Realm:       $REALM_ID"
echo "Message:     $TEST_MESSAGE"
echo "Server:      $SERVER"
echo "Scenario:    $SCENARIO"

# Dispatch to the chosen server form.  Each setup_server_<form> function is
# expected to bring up mock-actrix + an EchoService instance reachable under
# SERVICE_TYPE, exporting MOCK_PID, SERVER_PID (or its form-specific kin),
# CLIENT_RUNTIME, SERVICE_TYPE for the client driver to pick up.
case "$SERVER" in
    cdylib-rust)        setup_polyglot_echo ;;
    linked-rust)        setup_server_linked_rust ;;
    wasm-rust)          setup_server_wasm_rust ;;
esac

# Stream scenarios require a second server (EchoStreamService).
if [[ "$SCENARIO" == "server-stream" || "$SCENARIO" == "bidi" ]]; then
    setup_stream_server
fi

# ── client: rust ─────────────────────────────────────────────────────────────

run_rust_driver() {
    section "🔨 Building Rust client driver"
    CARGO_TARGET_DIR="$E2E_CACHE_ROOT/driver-rust" \
        cargo build --manifest-path "$SCRIPT_DIR/clients/rust/Cargo.toml" \
        >"$LOG_DIR/driver-rust-build.log" 2>&1 || {
            cat "$LOG_DIR/driver-rust-build.log" >&2
            fail "Rust driver build failed"
        }
    success "Rust driver built"

    section "🚀 Running Rust client driver (--scenario $SCENARIO)"
    local driver_log="$LOG_DIR/driver-rust.log"
    local driver_bin="$E2E_CACHE_ROOT/driver-rust/debug/polyglot-echo-rust-driver"
    [ -x "$driver_bin" ] || fail "driver binary missing at $driver_bin"

    # For streaming scenarios, use the stream service type for discovery.
    local active_service_type="$SERVICE_TYPE"
    if [[ "$SCENARIO" == "server-stream" || "$SCENARIO" == "bidi" ]]; then
        active_service_type="$STREAM_SERVICE_TYPE"
    fi

    RUST_LOG="${RUST_LOG:-info}" \
        "$driver_bin" \
        --actr-toml "$CLIENT_RUNTIME" \
        --service-type "$active_service_type" \
        --scenario "$SCENARIO" \
        --message "$TEST_MESSAGE" \
        >"$driver_log" 2>&1 &
    DRIVER_PID=$!

    local timeout="${CLIENT_TIMEOUT_SECONDS:-30}"
    local attempt=0
    while kill -0 "$DRIVER_PID" 2>/dev/null && [ $attempt -lt "$timeout" ]; do
        sleep 1
        attempt=$((attempt + 1))
    done

    if kill -0 "$DRIVER_PID" 2>/dev/null; then
        kill "$DRIVER_PID" 2>/dev/null || true
        echo "" >&2
        cat "$driver_log" >&2
        fail "Rust driver timed out after ${timeout}s"
    fi

    # Assertion: verify the expected output appeared in the driver log.
    case "$SCENARIO" in
        echo)
            if grep -q "\[Received reply\].*Echo: $TEST_MESSAGE" "$driver_log"; then
                success "Rust driver echoed back: 'Echo: $TEST_MESSAGE'"
                return 0
            fi
            ;;
        server-stream)
            if grep -q "\[server-stream\] received" "$driver_log"; then
                success "Rust driver server-stream completed"
                return 0
            fi
            ;;
        bidi)
            if grep -q "\[bidi\] received" "$driver_log"; then
                success "Rust driver bidi completed"
                return 0
            fi
            ;;
    esac

    echo "" >&2
    echo "Rust driver log:" >&2
    cat "$driver_log" >&2
    echo "" >&2
    echo "Server log (last 80):" >&2
    tail -80 "$LOG_DIR/server.log" >&2 || true
    if [[ "$SCENARIO" == "server-stream" || "$SCENARIO" == "bidi" ]]; then
        echo "" >&2
        echo "Stream server log (last 80):" >&2
        tail -80 "$LOG_DIR/stream-server.log" >&2 || true
    fi
    fail "Expected output not found in driver log for scenario '$SCENARIO'"
}

run_ts_driver() {
    [[ "$SCENARIO" == "echo" ]] || fail "TS client only supports --scenario echo (streaming not implemented in the TypeScript binding)"
    require_cmd node
    require_cmd npm

    local bindings_dir="$REPO_ROOT/bindings/typescript"
    local dist_entry="$bindings_dir/dist/index.js"
    local build_log="$LOG_DIR/driver-ts-build.log"

    section "🔨 Building bindings/typescript"
    if [ ! -f "$dist_entry" ]; then
        (
            cd "$bindings_dir"
            npm install --no-audit --no-fund
            npm run build:debug
            npm run compile:ts
        ) >"$build_log" 2>&1 || { cat "$build_log" >&2; fail "bindings/typescript build failed"; }
        [ -f "$dist_entry" ] || fail "bindings/typescript dist missing after build"
        # `napi build` regenerates index.js/index.d.ts and strips the
        # hand-maintained ActrError shim. The driver only consumes dist/, so
        # restore the checked-in root bindings to keep this build side-effect
        # out of the working tree.
        git -C "$REPO_ROOT" checkout -- bindings/typescript/index.js bindings/typescript/index.d.ts 2>/dev/null || true
    fi
    success "bindings/typescript built"

    section "🚀 Running TypeScript client driver"
    local driver_log="$LOG_DIR/driver-ts.log"
    local timeout_s="${CLIENT_TIMEOUT_SECONDS:-60}"

    local rc=0
    timeout "$timeout_s" node "$SCRIPT_DIR/clients/typescript/index.cjs" \
        --actr-toml "$CLIENT_RUNTIME" \
        --service-type "$SERVICE_TYPE" \
        --message "$TEST_MESSAGE" \
        >"$driver_log" 2>&1 || rc=$?

    if [ "$rc" -ne 0 ]; then
        cat "$driver_log" >&2
        [ "$rc" -eq 124 ] && fail "TS driver timed out after ${timeout_s}s"
        fail "TS driver failed (exit $rc)"
    fi

    if grep -q "\[Received reply\].*Echo: $TEST_MESSAGE" "$driver_log"; then
        success "TS driver echoed back: 'Echo: $TEST_MESSAGE'"
        return 0
    fi
    echo "" >&2
    echo "TS driver log:" >&2
    cat "$driver_log" >&2
    echo "" >&2
    echo "Server log (last 80):" >&2
    tail -80 "$LOG_DIR/server.log" >&2 || true
    fail "Expected output not found in TS driver log for scenario '$SCENARIO'"
}

# ── dispatch ─────────────────────────────────────────────────────────────────

case "$CLIENT" in
    rust)            run_rust_driver ;;
    ts)              run_ts_driver ;;
esac

echo ""
success "polyglot-echo --client $CLIENT --scenario $SCENARIO completed"
