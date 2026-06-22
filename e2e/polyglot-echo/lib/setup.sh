#!/usr/bin/env bash
# setup.sh — shared bootstrap for the polyglot-echo scenario.
#
# Sourced by run.sh after argument parsing. Drives:
#   1. Build local actr CLI + mock-actrix.
#   2. Start mock-actrix on $HTTP_PORT.
#   3. Generate a fresh MFR keypair and seed it via /admin/mfr.
#   4. Seed the realm via /admin/realms.
#   5. Scaffold a Rust echo service (cdylib) via `actr init`, build, publish.
#   6. Render server- and client-runtime configs from templates.
#   7. Launch the server via `actr run`; export readiness state for the caller.
#
# Caller is responsible for:
#   - Defining: SCRIPT_DIR (run.sh's dir), REPO_ROOT, MANUFACTURER, RUN_DIR,
#     STATE_DIR, LOG_DIR, DIST_DIR, HTTP_PORT, ICE_PORT, REALM_ID.
#   - Sourcing lib/common.sh first.
#
# After setup.sh completes the following globals are exported:
#   ACTR_CLI_BIN     — path to the locally built actr binary
#   MOCK_ACTRIX_BIN  — path to the locally built mock-actrix binary
#   MFR_PUBKEY       — base64 ed25519 verifying key (seeded into mock-actrix)
#   SERVICE_VERSION  — version field from the scaffolded service manifest
#   SERVICE_PACKAGE  — path to the signed `.actr` server package
#   SERVER_RUNTIME   — path to the rendered server-runtime.toml
#   CLIENT_RUNTIME   — path to the rendered client-runtime.toml (per-client
#                       subdirs reuse this template)
#   SERVICE_TYPE     — `polyglot:EchoService:<version>` (for client ACL pin)
#   MOCK_PID         — pid of the running mock-actrix process
#   SERVER_PID       — pid of the running `actr run` server process

set -euo pipefail

# Tunables — caller may override before sourcing.
: "${HTTP_PORT:=18181}"
: "${ICE_PORT:=13478}"
: "${REALM_ID:=4242}"
: "${MANUFACTURER:=polyglot}"

# Cache directories live under target/ so cargo-cache picks them up.
: "${E2E_CACHE_ROOT:=$REPO_ROOT/target/e2e-cache/polyglot-echo}"
: "${ACTR_CLI_TARGET_DIR:=$E2E_CACHE_ROOT/actr-cli}"
: "${MOCK_TARGET_DIR:=$E2E_CACHE_ROOT/mock-actrix}"
: "${SERVICE_TARGET_DIR:=$E2E_CACHE_ROOT/service-build}"

mkdir -p "$LOG_DIR" "$DIST_DIR" "$STATE_DIR" "$E2E_CACHE_ROOT"

ACTR_CLI_BIN=""
MOCK_ACTRIX_BIN=""
MFR_PUBKEY=""
MFR_KEY_FILE="$RUN_DIR/dev-key.json"
SERVICE_VERSION=""
SERVICE_PACKAGE=""
SERVER_RUNTIME="$RUN_DIR/server-runtime.toml"
CLIENT_RUNTIME="$RUN_DIR/client-runtime.toml"
SERVICE_TYPE=""
TMP_SERVICE_DIR="$RUN_DIR/service"
SERVICE_KEYCHAIN="$RUN_DIR/service-keychain.json"
MOCK_PID=""
SERVER_PID=""

run_actr() {
    CARGO_TARGET_DIR="$ACTR_CLI_TARGET_DIR" "$ACTR_CLI_BIN" "$@"
}

build_actr_cli() {
    section "🔧 Building local actr CLI"
    # Build with `wasm-engine` on top of the default `dynclib-engine`: the
    # wasm-rust server form needs `actr run` to host a wasm32-wasip2 component
    # (otherwise it aborts with "package target requires the `wasm-engine`
    # feature, but it is not enabled").  The flag is additive, so the cdylib
    # and linked forms keep working; building it unconditionally also keeps a
    # single feature set across every form that shares this actr-cli target
    # dir, avoiding cargo feature-flip rebuilds when forms run back-to-back.
    CARGO_TARGET_DIR="$ACTR_CLI_TARGET_DIR" cargo build \
        --manifest-path "$REPO_ROOT/cli/Cargo.toml" --bin actr \
        --features wasm-engine >/dev/null
    ACTR_CLI_BIN="$ACTR_CLI_TARGET_DIR/debug/actr"
    [ -x "$ACTR_CLI_BIN" ] || fail "actr CLI binary missing at $ACTR_CLI_BIN"
    success "actr CLI ready: $ACTR_CLI_BIN"
}

build_mock_actrix() {
    section "🔧 Building local mock-actrix"
    CARGO_TARGET_DIR="$MOCK_TARGET_DIR" cargo build \
        --manifest-path "$REPO_ROOT/Cargo.toml" -p actr-mock-actrix --bin mock-actrix >/dev/null
    MOCK_ACTRIX_BIN="$MOCK_TARGET_DIR/debug/mock-actrix"
    [ -x "$MOCK_ACTRIX_BIN" ] || fail "mock-actrix binary missing at $MOCK_ACTRIX_BIN"
    success "mock-actrix ready: $MOCK_ACTRIX_BIN"
}

start_mock_actrix() {
    section "🚀 Starting mock-actrix"
    # Reap orphaned servers from a prior run BEFORE the fresh mock-actrix
    # comes up, so a stale `actr run` cannot re-register under an old ACL.
    reap_stale_polyglot_servers
    kill_listener tcp "$HTTP_PORT"

    "$MOCK_ACTRIX_BIN" --port "$HTTP_PORT" \
        >"$LOG_DIR/mock-actrix.log" 2>&1 &
    MOCK_PID=$!

    if ! wait_for_http_ok "http://127.0.0.1:${HTTP_PORT}/signaling/health" 30; then
        cat "$LOG_DIR/mock-actrix.log" >&2 || true
        fail "mock-actrix did not become healthy on port $HTTP_PORT"
    fi
    success "mock-actrix is healthy on http://127.0.0.1:${HTTP_PORT}"
}

generate_mfr_key() {
    section "🔑 Generating MFR keychain"
    run_actr pkg keygen --output "$MFR_KEY_FILE" --force >/dev/null
    MFR_PUBKEY="$(json_field "$MFR_KEY_FILE" '.public_key')"
    [ -n "$MFR_PUBKEY" ] || fail "Failed to read MFR pubkey from $MFR_KEY_FILE"
    success "MFR pubkey: $MFR_PUBKEY"
}

seed_realm_and_mfr() {
    section "🪪 Seeding realm + MFR via mock-actrix admin"
    curl -fsS -X POST "http://127.0.0.1:${HTTP_PORT}/admin/realms" \
        -H 'content-type: application/json' \
        --data "{\"id\": ${REALM_ID}, \"name\": \"polyglot-echo\"}" >/dev/null
    success "realm ${REALM_ID} seeded"

    curl -fsS -X POST "http://127.0.0.1:${HTTP_PORT}/admin/mfr" \
        -H 'content-type: application/json' \
        --data "{\"name\": \"${MANUFACTURER}\", \"pubkey_b64\": \"${MFR_PUBKEY}\", \"contact\": \"polyglot-echo@local\"}" >/dev/null
    success "MFR ${MANUFACTURER} seeded"
}

scaffold_service() {
    section "🧱 Scaffolding Rust echo service via 'actr init'"
    rm -rf "$TMP_SERVICE_DIR"
    run_actr init \
        -l rust \
        --template echo \
        --role service \
        --signaling "ws://127.0.0.1:${HTTP_PORT}/signaling/ws" \
        --manufacturer "$MANUFACTURER" \
        "$TMP_SERVICE_DIR"

    append_workspace_patch "$TMP_SERVICE_DIR/Cargo.toml" "$REPO_ROOT"
    cp "$MFR_KEY_FILE" "$SERVICE_KEYCHAIN"
    write_project_keychain_config "$TMP_SERVICE_DIR" "$SERVICE_KEYCHAIN"

    (
        cd "$TMP_SERVICE_DIR"
        CARGO_TARGET_DIR="$SERVICE_TARGET_DIR" run_actr deps install >/dev/null
        CARGO_TARGET_DIR="$SERVICE_TARGET_DIR" run_actr gen -l rust >/dev/null
    )

    SERVICE_VERSION="$(
        awk '
            /^\[package\]/ { in_package = 1; next }
            /^\[/ && in_package { exit }
            in_package && $1 == "version" {
                gsub(/"/, "", $3); print $3; exit
            }
        ' "$TMP_SERVICE_DIR/manifest.toml"
    )"
    [ -n "$SERVICE_VERSION" ] || fail "Unable to detect scaffolded service version"
    SERVICE_TYPE="${MANUFACTURER}:EchoService:${SERVICE_VERSION}"
    success "scaffolded service: ${SERVICE_TYPE}"
}

build_service_package() {
    section "📦 Building + publishing service package"
    local host_target
    host_target="$(rustc -vV | awk '/host:/ {print $2}')"
    SERVICE_PACKAGE="$DIST_DIR/${MANUFACTURER}-EchoService-${SERVICE_VERSION}-${host_target}.actr"

    (
        cd "$TMP_SERVICE_DIR"
        CARGO_TARGET_DIR="$SERVICE_TARGET_DIR" run_actr build \
            --manifest-path manifest.toml \
            --key "$SERVICE_KEYCHAIN" \
            --output "$SERVICE_PACKAGE" >/dev/null
    )
    [ -f "$SERVICE_PACKAGE" ] || fail "Service package missing: $SERVICE_PACKAGE"

    run_actr registry publish \
        --package "$SERVICE_PACKAGE" \
        --keychain "$SERVICE_KEYCHAIN" \
        --endpoint "http://127.0.0.1:${HTTP_PORT}" >/dev/null
    success "service package published: $SERVICE_PACKAGE"
}

render_client_runtime_config() {
    render_template \
        "$SCRIPT_DIR/config/client-runtime.toml.tpl" \
        "$CLIENT_RUNTIME" \
        "__HTTP_PORT__=$HTTP_PORT" \
        "__ICE_PORT__=$ICE_PORT" \
        "__REALM_ID__=$REALM_ID" \
        "__SERVICE_TYPE__=$SERVICE_TYPE" \
        "__MFR_PUBKEY__=$MFR_PUBKEY"
    success "client runtime: $CLIENT_RUNTIME"
}

render_runtime_configs() {
    # cdylib-rust path needs both: server-runtime.toml (for `actr run`)
    # plus client-runtime.toml (for all clients).  Other --server forms
    # only need the client config; they bring their own server runtime.
    section "📝 Rendering runtime configs"
    render_template \
        "$SCRIPT_DIR/config/server-runtime.toml.tpl" \
        "$SERVER_RUNTIME" \
        "__PACKAGE_PATH__=$SERVICE_PACKAGE" \
        "__HTTP_PORT__=$HTTP_PORT" \
        "__ICE_PORT__=$ICE_PORT" \
        "__REALM_ID__=$REALM_ID" \
        "__MFR_PUBKEY__=$MFR_PUBKEY"
    success "server runtime: $SERVER_RUNTIME"

    render_client_runtime_config
}

start_server() {
    section "🚀 Starting EchoService host (actr run)"
    RUST_LOG="${RUST_LOG:-info}" \
        run_actr run -c "$SERVER_RUNTIME" \
        >"$LOG_DIR/server.log" 2>&1 &
    SERVER_PID=$!

    local attempt=0
    while [ $attempt -lt 30 ]; do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            cat "$LOG_DIR/server.log" >&2 || true
            fail "Server host exited early"
        fi
        if grep -qE "fully started|ActrNode started|Echo Host" \
            "$LOG_DIR/server.log" 2>/dev/null; then
            success "EchoService host is up"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    warn "Did not observe server readiness log; continuing"
}

setup_polyglot_echo() {
    require_cmd cargo
    require_cmd curl
    require_cmd jq
    require_cmd lsof
    require_cmd rustc

    build_actr_cli
    build_mock_actrix
    start_mock_actrix
    generate_mfr_key
    seed_realm_and_mfr
    scaffold_service
    build_service_package
    render_runtime_configs
    start_server

    export ACTR_CLI_BIN MOCK_ACTRIX_BIN MFR_PUBKEY SERVICE_VERSION
    export SERVICE_PACKAGE SERVER_RUNTIME CLIENT_RUNTIME SERVICE_TYPE
    export MOCK_PID SERVER_PID
}

# ── Common pre-server bootstrap (shared by all --server forms) ──────────────
#
# All non-cdylib server forms (linked-*, wasm-*) need the same prelude:
#   mock-actrix up + MFR seeded + client-runtime config rendered.
# They differ only in how the EchoService workload itself is brought up
# (linked binary vs cdylib package vs wasm component).
#
# Sets SERVICE_TYPE / SERVICE_VERSION to the constants linked/wasm servers
# advertise when self-registering (no cdylib version-from-manifest dance).

setup_common_prelude() {
    require_cmd cargo
    require_cmd curl
    require_cmd jq
    require_cmd lsof
    require_cmd rustc

    build_actr_cli
    build_mock_actrix
    start_mock_actrix
    generate_mfr_key
    seed_realm_and_mfr
    SERVICE_VERSION="1.0.0"
    SERVICE_TYPE="${MANUFACTURER}:EchoService:${SERVICE_VERSION}"

    # Set up the service keychain (signing key for `actr build` + publish).
    # Only the cdylib path creates this in scaffold_service today; other
    # forms that drive `actr build` directly (wasm-rust) need it too.
    cp "$MFR_KEY_FILE" "$SERVICE_KEYCHAIN"

    render_client_runtime_config

    export ACTR_CLI_BIN MOCK_ACTRIX_BIN MFR_PUBKEY SERVICE_VERSION
    export CLIENT_RUNTIME SERVICE_TYPE MOCK_PID SERVICE_KEYCHAIN
}

# ── Server form: linked-rust (in-process Rust workload) ────────────────────
#
# Brings up an in-tree Rust binary at e2e/polyglot-echo/services/linked-rust/
# that builds an actr Node, registers EchoService, and serves Echo RPCs.
# Same wire protocol as the cdylib path; client driver does not care.
setup_server_linked_rust() {
    setup_common_prelude
    build_linked_rust_server
    render_linked_rust_runtime
    start_linked_rust_server

    export SERVER_PID
}

# ── Server form: wasm-rust (Rust Wasm Component) ──────────────────────────
#
# Builds a Rust Wasm Component at e2e/polyglot-echo/services/wasm-rust/ and
# serves it via actr-hyper's wasm-engine feature.  Workload contract is the
# same as cdylib (entry! macro), but the binary is a wasm32-wasip2 component
# loaded by the Rust hyper host instead of dlopened.
setup_server_wasm_rust() {
    setup_common_prelude
    build_wasm_rust_server
    start_wasm_rust_server

    export SERVER_PID
}

# ── Server form: linked-rust implementation ──────────────────────────────────

: "${LINKED_RUST_TARGET_DIR:=$E2E_CACHE_ROOT/linked-rust}"
LINKED_RUST_RUNTIME=""

build_linked_rust_server() {
    section "🔨 Building linked-rust EchoService server"
    local server_dir="$SCRIPT_DIR/services/linked-rust"
    [ -d "$server_dir" ] || fail "services/linked-rust/ not present yet (Phase B2)"

    CARGO_TARGET_DIR="$LINKED_RUST_TARGET_DIR" \
        cargo build \
        --manifest-path "$server_dir/Cargo.toml" \
        --bin polyglot-echo-linked-rust \
        >"$LOG_DIR/linked-rust-build.log" 2>&1 || {
            cat "$LOG_DIR/linked-rust-build.log" >&2
            fail "linked-rust server build failed"
        }
    success "linked-rust server built"
}

render_linked_rust_runtime() {
    LINKED_RUST_RUNTIME="$RUN_DIR/linked-rust-runtime.toml"
    render_template \
        "$SCRIPT_DIR/config/linked-rust-runtime.toml.tpl" \
        "$LINKED_RUST_RUNTIME" \
        "__HTTP_PORT__=$HTTP_PORT" \
        "__ICE_PORT__=$ICE_PORT" \
        "__REALM_ID__=$REALM_ID" \
        "__MFR_PUBKEY__=$MFR_PUBKEY"
    success "linked-rust server runtime: $LINKED_RUST_RUNTIME"
}

start_linked_rust_server() {
    section "🚀 Starting linked-rust EchoService"
    local server_bin="$LINKED_RUST_TARGET_DIR/debug/polyglot-echo-linked-rust"
    [ -x "$server_bin" ] || fail "linked-rust server binary missing at $server_bin"

    RUST_LOG="${RUST_LOG:-info}" \
        "$server_bin" \
        --actr-toml "$LINKED_RUST_RUNTIME" \
        >"$LOG_DIR/server.log" 2>&1 &
    SERVER_PID=$!

    local attempt=0
    while [ $attempt -lt 30 ]; do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            cat "$LOG_DIR/server.log" >&2 || true
            fail "linked-rust server exited early"
        fi
        if grep -qE "linked-rust echo server registered|ActrNode started|fully started" \
            "$LOG_DIR/server.log" 2>/dev/null; then
            success "linked-rust EchoService is up"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    warn "Did not observe linked-rust readiness log; continuing"
}

# ── Server form: wasm-rust implementation ────────────────────────────────────
#
# Builds a wasm32-wasip2 Component Model package from services/wasm-rust/
# via the same `actr build` pipeline as cdylib-rust (just with a different
# manifest target/kind), then loads it through `actr run` whose hyper has
# the wasm-engine feature enabled (build_actr_cli builds the CLI with
# `--features wasm-engine`; it is not a default feature of cli/Cargo.toml).
#
# Reuses server-runtime.toml.tpl (same one cdylib-rust uses) since the
# only difference at runtime config level is the package path, which the
# template already accepts via __PACKAGE_PATH__.

: "${WASM_RUST_TARGET_DIR:=$E2E_CACHE_ROOT/wasm-rust}"
WASM_RUST_PACKAGE=""

build_wasm_rust_server() {
    section "🔨 Building wasm-rust EchoService package"
    local server_dir="$SCRIPT_DIR/services/wasm-rust"
    [ -d "$server_dir" ] || fail "services/wasm-rust/ not present"

    # Project-local .actr/config.toml pins the keychain path so `actr build`
    # picks up the per-run signing key.  Cleaned up at end of run.
    write_project_keychain_config "$server_dir" "$SERVICE_KEYCHAIN"

    # Regenerate src/generated/ from protos/local/echo.proto.  Done in-tree
    # rather than via a copy because actr gen needs manifest.toml + the proto
    # at the conventional layout; this is exactly what services/wasm-rust/
    # provides.  `actr deps install` writes manifest.lock.toml which actr gen
    # then consumes.
    (
        cd "$server_dir"
        run_actr deps install
        ACTR_CLI_BIN="$ACTR_CLI_BIN" bash regenerate.sh
    ) >"$LOG_DIR/wasm-rust-gen.log" 2>&1 || {
        cat "$LOG_DIR/wasm-rust-gen.log" >&2
        fail "actr deps/gen for wasm-rust failed"
    }

    # Pre-add wasm32-wasip2 target if missing.  CI installs it explicitly
    # (ci-rust.yml's setup-rust-toolchain step); local devs may not have it.
    if ! rustup target list --installed | grep -q wasm32-wasip2; then
        section "⬇️  Adding rustup target wasm32-wasip2"
        rustup target add wasm32-wasip2 >>"$LOG_DIR/wasm-rust-build.log" 2>&1
    fi

    local host_target
    host_target="$(rustc -vV | awk '/host:/ {print $2}')"
    WASM_RUST_PACKAGE="$DIST_DIR/polyglot-EchoService-1.0.0-wasm32-wasip2.actr"

    (
        cd "$server_dir"
        # A globally-configured host linker (e.g. `~/.cargo/config.toml` with
        # `[build] rustflags = ["-C", "link-arg=-fuse-ld=mold"]`) otherwise
        # leaks into the wasm32-wasip2 link step.  That target links via
        # wasm-component-ld, which rejects `-fuse-ld` ("unexpected argument
        # '-f'") and aborts the build.  Override the per-target rustflags so
        # the wasm component links with its own toolchain, matching CI (which
        # has no global linker config).  An empty value does NOT work: cargo
        # treats an empty/whitespace CARGO_TARGET_<triple>_RUSTFLAGS as unset
        # and falls back to `[build] rustflags`, so a harmless no-op codegen
        # flag is used to actively displace the global flags.
        export CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS="-Cdebuginfo=0"
        CARGO_TARGET_DIR="$WASM_RUST_TARGET_DIR" run_actr build \
            --manifest-path manifest.toml \
            --key "$MFR_KEY_FILE" \
            --output "$WASM_RUST_PACKAGE"
    ) >"$LOG_DIR/wasm-rust-build.log" 2>&1 || {
        cat "$LOG_DIR/wasm-rust-build.log" >&2
        fail "wasm-rust actr build failed"
    }
    [ -f "$WASM_RUST_PACKAGE" ] || fail "wasm-rust package missing: $WASM_RUST_PACKAGE"

    run_actr registry publish \
        --package "$WASM_RUST_PACKAGE" \
        --keychain "$SERVICE_KEYCHAIN" \
        --endpoint "http://127.0.0.1:${HTTP_PORT}" \
        >>"$LOG_DIR/wasm-rust-build.log" 2>&1 || {
            cat "$LOG_DIR/wasm-rust-build.log" >&2
            fail "wasm-rust publish failed"
        }
    success "wasm-rust package built + published: $WASM_RUST_PACKAGE"
}

start_wasm_rust_server() {
    section "🚀 Starting wasm-rust EchoService host (actr run)"

    # Reuse the cdylib server-runtime template (same shape: package path +
    # signaling/AIS endpoints).  Set SERVICE_PACKAGE so the existing
    # render_runtime_configs helper picks the wasm package.
    SERVICE_PACKAGE="$WASM_RUST_PACKAGE"
    SERVER_RUNTIME="$RUN_DIR/wasm-rust-runtime.toml"
    render_template \
        "$SCRIPT_DIR/config/server-runtime.toml.tpl" \
        "$SERVER_RUNTIME" \
        "__PACKAGE_PATH__=$WASM_RUST_PACKAGE" \
        "__HTTP_PORT__=$HTTP_PORT" \
        "__ICE_PORT__=$ICE_PORT" \
        "__REALM_ID__=$REALM_ID" \
        "__MFR_PUBKEY__=$MFR_PUBKEY"
    success "wasm-rust server runtime: $SERVER_RUNTIME"

    SERVICE_KEYCHAIN_FOR_SERVER="$SERVICE_KEYCHAIN"
    RUST_LOG="${RUST_LOG:-info}" \
        run_actr run -c "$SERVER_RUNTIME" \
        >"$LOG_DIR/server.log" 2>&1 &
    SERVER_PID=$!

    local attempt=0
    while [ $attempt -lt 30 ]; do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            cat "$LOG_DIR/server.log" >&2 || true
            fail "wasm-rust host exited early"
        fi
        if grep -qE "wasm-rust echo server registered|ActrNode started|fully started|Echo Host" \
            "$LOG_DIR/server.log" 2>/dev/null; then
            success "wasm-rust EchoService is up"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    warn "Did not observe wasm-rust readiness log; continuing"
}

# ── Streaming server setup ────────────────────────────────────────────────────
#
# Builds and starts the in-tree streaming server binary
# (e2e/polyglot-echo/server/).  The streaming server runs as a *linked
# workload* (not a cdylib package) so that RuntimeContext's DataStream APIs
# are available.  It registers directly with mock-actrix under
# polyglot:EchoStreamService:1.0.0 without going through `actr build` /
# `actr run`.
#
# Prerequisites: setup_polyglot_echo must have run first (mock-actrix up,
# MFR seeded, ais_endpoint available in CLIENT_RUNTIME template).
#
# Exports after completion:
#   STREAM_SERVER_PID     — pid of the running stream server binary
#   STREAM_SERVER_RUNTIME — path to the rendered stream-server runtime config
#   STREAM_SERVICE_TYPE   — "polyglot:EchoStreamService:1.0.0"

STREAM_SERVER_PID=""
STREAM_SERVER_RUNTIME=""
STREAM_SERVICE_TYPE="polyglot:EchoStreamService:1.0.0"
: "${STREAM_SERVER_TARGET_DIR:=$E2E_CACHE_ROOT/stream-server}"

build_stream_server_binary() {
    section "🔨 Building stream server binary"
    local server_dir="$SCRIPT_DIR/server"

    CARGO_TARGET_DIR="$STREAM_SERVER_TARGET_DIR" \
        cargo build \
        --manifest-path "$server_dir/Cargo.toml" \
        --bin polyglot-echo-stream-server \
        >"$LOG_DIR/stream-server-build.log" 2>&1 || {
            cat "$LOG_DIR/stream-server-build.log" >&2
            fail "Stream server build failed"
        }
    success "stream server binary built"
}

render_stream_server_runtime() {
    STREAM_SERVER_RUNTIME="$RUN_DIR/stream-server-runtime.toml"
    render_template \
        "$SCRIPT_DIR/config/stream-server-runtime.toml.tpl" \
        "$STREAM_SERVER_RUNTIME" \
        "__HTTP_PORT__=$HTTP_PORT" \
        "__ICE_PORT__=$ICE_PORT" \
        "__REALM_ID__=$REALM_ID" \
        "__MFR_PUBKEY__=$MFR_PUBKEY"
    success "stream server runtime: $STREAM_SERVER_RUNTIME"
}

start_stream_server() {
    section "🚀 Starting EchoStreamService binary"
    local server_bin="$STREAM_SERVER_TARGET_DIR/debug/polyglot-echo-stream-server"
    [ -x "$server_bin" ] || fail "stream server binary missing at $server_bin"

    RUST_LOG="${RUST_LOG:-info}" \
        "$server_bin" \
        --actr-toml "$STREAM_SERVER_RUNTIME" \
        >"$LOG_DIR/stream-server.log" 2>&1 &
    STREAM_SERVER_PID=$!

    local attempt=0
    while [ $attempt -lt 30 ]; do
        if ! kill -0 "$STREAM_SERVER_PID" 2>/dev/null; then
            cat "$LOG_DIR/stream-server.log" >&2 || true
            fail "Stream server exited early"
        fi
        if grep -qE "stream server registered and ready|ActrNode started|fully started" \
            "$LOG_DIR/stream-server.log" 2>/dev/null; then
            success "EchoStreamService is up"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    warn "Did not observe stream-server readiness log; continuing"
}

setup_stream_server() {
    build_stream_server_binary
    render_stream_server_runtime
    start_stream_server

    export STREAM_SERVER_PID STREAM_SERVER_RUNTIME STREAM_SERVICE_TYPE
}
