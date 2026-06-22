#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/../package-runtime-echo/lib/common.sh"
source "$SCRIPT_DIR/lib/readiness.sh"

HTTP_PORT=8081
ICE_PORT=3478
REALM_ID=""
ADMIN_PASSWORD="e2e-test-password"
MANUFACTURER="${MANUFACTURER:-actrium}"
ACTRIX_BIN="${ACTRIX_BIN:-}"
ACTR_CLI_MANIFEST="$REPO_ROOT/cli/Cargo.toml"
E2E_TARGET_ROOT="$REPO_ROOT/target/e2e-cache/swift-ts-workload"
ACTR_TARGET_DIR="$E2E_TARGET_ROOT/actr-cli"

for cmd in cargo curl jq sqlite3 python3 perl rustc lsof; do
    require_cmd "$cmd"
done
ensure_actrix_available "$REPO_ROOT"

RUN_ID="$(date +%Y%m%d-%H%M%S)-$RANDOM"
RUN_DIR="$SCRIPT_DIR/.tmp/run-$RUN_ID"
STATE_DIR="$RUN_DIR/state"
SQLITE_DIR="$STATE_DIR/sqlite"
LOG_DIR="$RUN_DIR/logs"
DIST_DIR="$RUN_DIR/dist"
TMP_SERVICE_ROOT="$RUN_DIR/workspace"
TMP_SERVICE_DIR="$TMP_SERVICE_ROOT/swift-ts-workload-actr-$RANDOM"
TMP_APP_DIR="$TMP_SERVICE_ROOT/swift-ts-workload-app-$RANDOM"
ACTRIX_CONFIG_PATH="$RUN_DIR/actrix.toml"
SERVER_RUNTIME_PATH="$RUN_DIR/server-runtime.toml"
SERVICE_KEYCHAIN="$TMP_SERVICE_DIR/packaging/keys/mfr.keychain.json"
SERVICE_PUBLIC_KEY="$TMP_SERVICE_DIR/public-key.json"
PROVISIONED_KEYCHAIN="$RUN_DIR/mfr.keychain.json"
PROVISIONED_PUBLIC_KEY="$RUN_DIR/mfr-public-key.json"
SWIFTTSAPP_ACTRIX_CONFIG="$TMP_APP_DIR/actr.toml"
HOST_TARGET="$(rustc -vV | awk '/host:/ {print $2}')"
SWIFTTSAPP_PACKAGE_MANIFEST="$RUN_DIR/swift_ts_workload_app-package-manifest.toml"
SWIFTTSAPP_MARKER_BINARY="$RUN_DIR/swift_ts_workload_app-linked-identity.bin"
SWIFTTSAPP_PACKAGE="$DIST_DIR/${MANUFACTURER}-SwiftTsWorkloadApp-0.1.0-${HOST_TARGET}.actr"
APP_STDOUT_LOG="$LOG_DIR/app.stdout.log"
APP_STDERR_LOG="$LOG_DIR/app.stderr.log"
DIAGNOSTIC_DIR="$RUN_DIR/diagnostics"
SANITIZED_LOG_DIR="$RUN_DIR/sanitized-logs"

mkdir -p "$SQLITE_DIR" "$LOG_DIR" "$DIST_DIR" "$TMP_SERVICE_ROOT" "$E2E_TARGET_ROOT" "$DIAGNOSTIC_DIR" "$SANITIZED_LOG_DIR"

ACTRIX_PID=""
SERVER_PID=""
ACTR_CLI_BIN=""
ADMIN_TOKEN=""
SERVICE_PACKAGE=""
SERVICE_VERSION=""
REALM_SECRET=""
DEVICE_UDID=""
DEVICE_CREATED="0"

# ──── Diagnostics ────

capture_diagnostics() {
    local diag_dir="$DIAGNOSTIC_DIR"
    mkdir -p "$diag_dir"

    # Process status
    {
        echo "=== Process Status ==="
        echo "ACTRIX_PID=${ACTRIX_PID:-none}"
        echo "SERVER_PID=${SERVER_PID:-none}"
        if [ -n "${ACTRIX_PID:-}" ] && kill -0 "$ACTRIX_PID" 2>/dev/null; then
            echo "actrix: RUNNING"
        else
            echo "actrix: NOT RUNNING"
        fi
        if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "server: RUNNING"
        else
            echo "server: NOT RUNNING"
        fi
    } >"$diag_dir/process-status.txt" 2>/dev/null || true

    # Signaling health
    if curl -fsS "http://127.0.0.1:${HTTP_PORT}/signaling/health" >"$diag_dir/signaling-health.json" 2>/dev/null; then
        echo "signaling health: OK"
    else
        echo "signaling health: FAILED" >"$diag_dir/signaling-health.txt"
    fi

    # signaling_cache.db inspection
    local db_path="$SQLITE_DIR/signaling_cache.db"
    if [ -f "$db_path" ]; then
        {
            echo "=== signaling_cache.db ==="
            echo "--- Tables ---"
            sqlite3 "$db_path" ".tables" 2>/dev/null || true
            echo "--- Service registry ---"
            sqlite3 "$db_path" \
                "SELECT actor_realm_id, actor_manufacturer, actor_device_name, service_name, status, last_heartbeat_at FROM service_registry;" \
                2>/dev/null || true
        } >"$diag_dir/signaling-cache.txt" 2>/dev/null || true
    fi

    # Ghost candidates and ACL filtering from actrix log
    if [ -f "$LOG_DIR/actrix.log" ]; then
        grep -iE "heartbeat|disconnect|cleanup|ghost|candidate|acl|filter" "$LOG_DIR/actrix.log" >"$diag_dir/actrix-filtered.log" 2>/dev/null || true
    fi

    # Server log heartbeat/disconnect/registry events
    if [ -f "$LOG_DIR/server.log" ]; then
        grep -iE "heartbeat|disconnect|registry|cleanup|ghost|acl|filter|error|warn" "$LOG_DIR/server.log" >"$diag_dir/server-filtered.log" 2>/dev/null || true
    fi

    # App logs
    if [ -f "$APP_STDOUT_LOG" ]; then
        cp "$APP_STDOUT_LOG" "$diag_dir/app-stdout.log" 2>/dev/null || true
    fi
    if [ -f "$APP_STDERR_LOG" ]; then
        cp "$APP_STDERR_LOG" "$diag_dir/app-stderr.log" 2>/dev/null || true
    fi

    echo "Diagnostics captured at: $diag_dir"
}

sanitize_logs_for_upload() {
    local src_dir="$1"
    local dst_dir="$2"
    mkdir -p "$dst_dir"

    local secrets=(
        "$REALM_SECRET"
        "$ADMIN_PASSWORD"
        "$ADMIN_TOKEN"
    )

    for file in "$src_dir"/*; do
        [ -f "$file" ] || continue
        local basename
        basename="$(basename "$file")"
        local content
        content="$(cat "$file" 2>/dev/null || true)"

        for secret in "${secrets[@]}"; do
            if [ -n "$secret" ]; then
                content="${content//$secret/REDACTED}"
            fi
        done

        echo "$content" >"$dst_dir/$basename"
    done

    # Copy logs but NOT keychain, runtime config, or SQLite state
    for log in "$LOG_DIR"/*.log; do
        [ -f "$log" ] || continue
        local basename
        basename="$(basename "$log")"
        local content
        content="$(cat "$log" 2>/dev/null || true)"
        for secret in "${secrets[@]}"; do
            if [ -n "$secret" ]; then
                content="${content//$secret/REDACTED}"
            fi
        done
        echo "$content" >"$dst_dir/$basename"
    done

    echo "Sanitized logs at: $dst_dir"
}

cleanup() {
    local status=$?

    # Collect diagnostics BEFORE killing processes
    if [ $status -ne 0 ] || [ "${CAPTURE_DIAGNOSTICS_ON_SUCCESS:-0}" = "1" ]; then
        capture_diagnostics || true
        sanitize_logs_for_upload "$DIAGNOSTIC_DIR" "$SANITIZED_LOG_DIR" || true
    fi

    if [ -n "$DEVICE_UDID" ]; then
        xcrun simctl terminate "$DEVICE_UDID" io.actrium.SwiftTsWorkloadApp 2>/dev/null || true
        if [ "$DEVICE_CREATED" = "1" ]; then
            xcrun simctl shutdown "$DEVICE_UDID" 2>/dev/null || true
            xcrun simctl delete "$DEVICE_UDID" 2>/dev/null || true
        fi
    fi
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    if [ -n "$ACTRIX_PID" ] && kill -0 "$ACTRIX_PID" 2>/dev/null; then
        kill "$ACTRIX_PID" 2>/dev/null || true
    fi
    wait 2>/dev/null || true

    # Move sanitized logs out of RUN_DIR to a fixed location so the
    # upload-artifact step can find them regardless of success or failure.
    local upload_dir="$SCRIPT_DIR/.tmp/sanitized-logs"
    if [ -d "$SANITIZED_LOG_DIR" ] && [ -n "$(ls -A "$SANITIZED_LOG_DIR" 2>/dev/null)" ]; then
        rm -rf "$upload_dir"
        mv "$SANITIZED_LOG_DIR" "$upload_dir"
        echo "Sanitized logs moved to: $upload_dir"
    fi

    if [ $status -eq 0 ] && [ "${KEEP_TMP:-0}" != "1" ]; then
        rm -rf "$RUN_DIR"
    else
        echo ""
        echo "Artifacts preserved at: $RUN_DIR"
        if [ -d "$upload_dir" ] && [ -n "$(ls -A "$upload_dir" 2>/dev/null)" ]; then
            echo "Sanitized logs for upload at: $upload_dir"
        fi
    fi

    exit $status
}
trap cleanup EXIT INT TERM

run_actr() {
    CARGO_TARGET_DIR="$ACTR_TARGET_DIR" "$ACTR_CLI_BIN" "$@"
}

# ──── Rust / actrix lifecycle (reused from package-runtime-echo) ────

build_local_actr_cli() {
    section "🔧 Building local actr CLI"
    local cargo_env=()
    local libssh2_configured=0

    if command -v brew >/dev/null 2>&1; then
        local libssh2_prefix
        libssh2_prefix="$(brew --prefix libssh2 2>/dev/null || true)"
        if [ -n "$libssh2_prefix" ]; then
            cargo_env+=(
                "LIBSSH2_SYS_USE_PKG_CONFIG=1"
                "PKG_CONFIG_PATH=${libssh2_prefix}/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
                "CFLAGS=-I${libssh2_prefix}/include${CFLAGS:+ $CFLAGS}"
                "LDFLAGS=-L${libssh2_prefix}/lib${LDFLAGS:+ $LDFLAGS}"
            )
            libssh2_configured=1
        fi
    fi

    if [ "$libssh2_configured" -eq 0 ] && command -v pkg-config >/dev/null 2>&1 && pkg-config --exists libssh2; then
        cargo_env+=(LIBSSH2_SYS_USE_PKG_CONFIG=1)
    fi

    # The TS DuplexEchoService is a wasm32-wasip2 component, so the `actr run`
    # host needs the wasm runtime engine. Enable wasm-engine in addition to the
    # default dynclib-engine.
    env "${cargo_env[@]}" CARGO_TARGET_DIR="$ACTR_TARGET_DIR" cargo build --manifest-path "$ACTR_CLI_MANIFEST" --bin actr --features wasm-engine >/dev/null
    ACTR_CLI_BIN="$ACTR_TARGET_DIR/debug/actr"
    [ -x "$ACTR_CLI_BIN" ] || fail "actr CLI binary missing at $ACTR_CLI_BIN"
    success "actr CLI ready: $ACTR_CLI_BIN"
}

render_runtime_configs() {
    render_template \
        "$SCRIPT_DIR/../package-runtime-echo/config/actrix.toml" \
        "$ACTRIX_CONFIG_PATH" \
        "__SQLITE_DIR__=$SQLITE_DIR" \
        "__HTTP_PORT__=$HTTP_PORT" \
        "__ICE_PORT__=$ICE_PORT"
}

start_actrix() {
    section "🚀 Starting local actrix"
    kill_listener tcp "$HTTP_PORT"
    kill_listener udp "$ICE_PORT"

    "$ACTRIX_BIN" --config "$ACTRIX_CONFIG_PATH" >"$LOG_DIR/actrix.log" 2>&1 &
    ACTRIX_PID=$!

    if ! wait_for_http_ok "http://127.0.0.1:${HTTP_PORT}/signaling/health" 120; then
        cat "$LOG_DIR/actrix.log" >&2 || true
        fail "actrix did not become healthy on port $HTTP_PORT"
    fi
    success "actrix is healthy on http://127.0.0.1:${HTTP_PORT}"
}

login_admin() {
    section "🔐 Logging into Admin API"
    local response_file="$RUN_DIR/admin-login.json"
    curl -fsS \
        -X POST \
        "http://127.0.0.1:${HTTP_PORT}/admin/api/auth/login" \
        -H 'Content-Type: application/json' \
        -d "{\"password\":\"${ADMIN_PASSWORD}\"}" \
        >"$response_file"
    ADMIN_TOKEN="$(json_field "$response_file" '.token')"
    success "Admin API login succeeded"
}

warmup_ais_key() {
    section "🔑 Warming up AIS signing key"
    local current_key_file="$RUN_DIR/ais-current-key.json"
    local rotate_file="$RUN_DIR/ais-rotate-key.json"
    local attempt=0

    while [ $attempt -lt 60 ]; do
        if curl -fsS "http://127.0.0.1:${HTTP_PORT}/ais/current-key" >"$current_key_file" 2>/dev/null \
            && [ "$(jq -r '.status // "missing"' "$current_key_file" 2>/dev/null)" = "success" ]; then
            success "AIS signing key is ready"
            return 0
        fi

        curl -fsS -X POST "http://127.0.0.1:${HTTP_PORT}/ais/rotate-key" >"$rotate_file" 2>/dev/null || true
        sleep 1
        attempt=$((attempt + 1))
    done

    fail "AIS signing key warmup timed out"
}

ensure_realm() {
    section "🪪 Creating realm via Admin API"
    local create_file="$RUN_DIR/realm-create.json"
    local realm_name="swift-ts-workload-${RUN_ID}"
    curl -fsS \
        -X POST \
        "http://127.0.0.1:${HTTP_PORT}/admin/api/realms" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H 'Content-Type: application/json' \
        -d "{\"name\":\"${realm_name}\",\"enabled\":true,\"expires_at\":0}" \
        >"$create_file"

    REALM_ID="$(json_field "$create_file" '.realm.realm_id')"
    REALM_SECRET="$(json_field "$create_file" '.realm_secret')"

    [ -n "$REALM_ID" ] || fail "Realm creation returned an empty realm id"
    [ -n "$REALM_SECRET" ] || fail "Realm creation returned an empty realm secret"
    success "Realm ${REALM_ID} created"
}

write_project_keychain_config() {
    local project_dir="$1"
    local keychain_path="$2"
    mkdir -p "$project_dir/.actr"
    cat >"$project_dir/.actr/config.toml" <<EOF
[mfr]
manufacturer = "$MANUFACTURER"
keychain = "$keychain_path"

[network]
signaling_url = "ws://127.0.0.1:${HTTP_PORT}/signaling/ws"
ais_endpoint = "http://127.0.0.1:${HTTP_PORT}/ais"
realm_id = $REALM_ID
realm_secret = "$REALM_SECRET"
EOF
}

write_probe_proto() {
    local proto_path="$1"
    mkdir -p "$(dirname "$proto_path")"
    cat >"$proto_path" <<'EOF'
syntax = "proto3";
package local;

service ProbeService {
  rpc StartProbe(StartProbeRequest) returns (StartProbeResponse);
}

message StartProbeRequest {
  string probe_name = 1;
  string target_type = 2;
}

message StartProbeResponse {
  bool started = 1;
  string message = 2;
}
EOF
}

provision_mfr_keychain() {
    section "🏷️  Provisioning MFR keychain via Admin API"
    local apply_file="$RUN_DIR/mfr-apply.json"
    local approve_file="$RUN_DIR/mfr-approve.json"
    local now
    now="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

    curl -fsS \
        -X POST \
        "http://127.0.0.1:${HTTP_PORT}/admin/api/mfr/apply" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H 'Content-Type: application/json' \
        -d "{\"github_login\":\"${MANUFACTURER}\",\"contact\":\"e2e@local.actr\"}" \
        >"$apply_file"

    local mfr_id
    mfr_id="$(json_field "$apply_file" '.mfr_id')"

    curl -fsS \
        -X POST \
        "http://127.0.0.1:${HTTP_PORT}/admin/api/mfr/admin/${mfr_id}/approve" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H 'Content-Type: application/json' \
        -d '{}' \
        >"$approve_file"

    mkdir -p "$(dirname "$PROVISIONED_KEYCHAIN")"
    jq -n \
        --arg private_key "$(json_field "$approve_file" '.private_key')" \
        --arg public_key "$(json_field "$approve_file" '.certificate.mfr_pubkey')" \
        --arg created_at "$now" \
        '{
          created_at: $created_at,
          note: "E2E manufacturer signing key issued by local actrix admin API",
          private_key: $private_key,
          public_key: $public_key
        }' \
        >"$PROVISIONED_KEYCHAIN"
    chmod 600 "$PROVISIONED_KEYCHAIN" 2>/dev/null || true

    jq -n \
        --arg public_key "$(json_field "$approve_file" '.certificate.mfr_pubkey')" \
        '{ public_key: $public_key }' \
        >"$PROVISIONED_PUBLIC_KEY"

    success "Generated manufacturer keychain for ${MANUFACTURER}"
}

# ──── TS service scaffold/build/publish ────

scaffold_service_guest() {
    section "🧱 Scaffolding temporary TS DuplexEchoService"
    require_cmd npm
    mkdir -p "$TMP_SERVICE_DIR"
    (
        cd "$TMP_SERVICE_DIR"
        run_actr init \
            -l typescript \
            --template echo \
            --role service \
            --project-name "DuplexEchoService" \
            --signaling "ws://127.0.0.1:${HTTP_PORT}/signaling/ws" \
            --manufacturer "$MANUFACTURER" \
            "."
    )

    # Overlay handwritten sources over the echo template stub.
    mkdir -p "$TMP_SERVICE_DIR/src" "$TMP_SERVICE_DIR/protos/local"
    cp "$SCRIPT_DIR/service-src/actr_service.ts" "$TMP_SERVICE_DIR/src/actr_service.ts"
    cp "$SCRIPT_DIR/service-src/duplex_echo.proto" "$TMP_SERVICE_DIR/protos/local/duplex_echo.proto"
    cp "$SCRIPT_DIR/service-src/tsconfig.json" "$TMP_SERVICE_DIR/tsconfig.json"
    rm -f "$TMP_SERVICE_DIR/protos/local/echo.proto"

    # package.json: substitute __REPO_ROOT__ with the absolute repo path.
    perl -pe "s{__REPO_ROOT__}{$REPO_ROOT}g" \
        "$SCRIPT_DIR/service-src/package.json" >"$TMP_SERVICE_DIR/package.json"

    # manifest.toml: export our proto (template defaults to echo.proto) and keep
    # the published service identity DuplexEchoService:1.0.0.
    perl -i -pe 's{exports = \["protos/local/echo\.proto"\]}{exports = ["protos/local/duplex_echo.proto"]}' \
        "$TMP_SERVICE_DIR/manifest.toml"

    # The echo template hardcodes name = "EchoService"; publish as DuplexEchoService.
    perl -i -pe 's{^name = "EchoService"$}{name = "DuplexEchoService"}' \
        "$TMP_SERVICE_DIR/manifest.toml"

    # The echo template ACL rule "${MANUFACTURER}:echo-client-app" lacks a version,
    # which the actr CLI rejects (expects manufacturer:name:version). Point the ACL
    # at the SwiftTsWorkloadApp client identity instead.
    perl -i -pe "s{^type = \"${MANUFACTURER}:echo-client-app\"\$}{type = \"${MANUFACTURER}:SwiftTsWorkloadApp:0.1.0\"}" \
        "$TMP_SERVICE_DIR/manifest.toml"

    # The echo service template manifest has no [binary] section. The TS workload
    # is a componentized wasm32-wasip2 component; ensure the manifest points at it.
    if ! grep -q '^\[binary\]' "$TMP_SERVICE_DIR/manifest.toml"; then
        # Ensure the file ends with a newline before appending a new table.
        printf '\n' >>"$TMP_SERVICE_DIR/manifest.toml"
        cat >>"$TMP_SERVICE_DIR/manifest.toml" <<'EOF'
[binary]
path = "dist/duplex-echo-service.wasm"
target = "wasm32-wasip2"
EOF
    fi

    mkdir -p "$(dirname "$SERVICE_KEYCHAIN")"
    cp "$PROVISIONED_KEYCHAIN" "$SERVICE_KEYCHAIN"
    cp "$PROVISIONED_PUBLIC_KEY" "$SERVICE_PUBLIC_KEY"
    write_project_keychain_config "$TMP_SERVICE_DIR" "$SERVICE_KEYCHAIN"

    section "📦 Building local TypeScript workload binding"
    (cd "$REPO_ROOT/bindings/typescript/actr-workload" && npm ci && npm run build)

    (
        cd "$TMP_SERVICE_DIR"
        run_actr deps install
        run_actr gen -l typescript
        npm install
        npm run build
        npm run componentize
    )

    SERVICE_VERSION="1.0.0"
    [ -f "$TMP_SERVICE_DIR/dist/duplex-echo-service.wasm" ] \
        || fail "TS service wasm component missing"
    success "Temporary TS DuplexEchoService ready: version ${SERVICE_VERSION}"
}

build_service_package() {
    section "📦 Building and publishing the TS server package"
    SERVICE_PACKAGE="$DIST_DIR/${MANUFACTURER}-DuplexEchoService-${SERVICE_VERSION}-wasm32-wasip2.actr"

    (
        cd "$TMP_SERVICE_DIR"
        run_actr build \
            --no-compile \
            --manifest-path manifest.toml \
            --key "$SERVICE_KEYCHAIN" \
            --output "$SERVICE_PACKAGE"
    )

    [ -f "$SERVICE_PACKAGE" ] || fail "Server package missing: $SERVICE_PACKAGE"

    run_actr pkg verify --pubkey "$SERVICE_PUBLIC_KEY" --package "$SERVICE_PACKAGE" >/dev/null
    run_actr registry publish \
        --package "$SERVICE_PACKAGE" \
        --keychain "$SERVICE_KEYCHAIN" \
        --endpoint "http://127.0.0.1:${HTTP_PORT}"

    success "TS server package published"
}

publish_swift_ts_workload_app_package_identity() {
    section "📦 Publishing SwiftTsWorkloadApp package identity"

    # Linked SwiftTsWorkloadApp does not load this package. It is a registry marker for
    # actrix versions that still require the actor type to be package-registered.
    printf 'linked SwiftTsWorkloadApp identity marker\n' >"$SWIFTTSAPP_MARKER_BINARY"
    cat >"$SWIFTTSAPP_PACKAGE_MANIFEST" <<EOF
edition = 1

[package]
name = "SwiftTsWorkloadApp"
manufacturer = "${MANUFACTURER}"
version = "0.1.0"
description = "Actrium SwiftTsWorkloadApp linked runtime identity marker"

[binary]
path = "${SWIFTTSAPP_MARKER_BINARY}"
target = "${HOST_TARGET}"
EOF

    run_actr build \
        --no-compile \
        --manifest-path "$SWIFTTSAPP_PACKAGE_MANIFEST" \
        --key "$PROVISIONED_KEYCHAIN" \
        --output "$SWIFTTSAPP_PACKAGE"

    run_actr pkg verify --pubkey "$PROVISIONED_PUBLIC_KEY" --package "$SWIFTTSAPP_PACKAGE" >/dev/null
    run_actr registry publish \
        --package "$SWIFTTSAPP_PACKAGE" \
        --keychain "$PROVISIONED_KEYCHAIN" \
        --endpoint "http://127.0.0.1:${HTTP_PORT}"

    success "SwiftTsWorkloadApp package identity published"
}

# ──── SwiftTsWorkloadApp config ────

render_swift_ts_workload_app_config() {
    section "📝 Rendering SwiftTsWorkloadApp runtime config"
    render_template \
        "$SCRIPT_DIR/actr.toml.tpl" \
        "$SWIFTTSAPP_ACTRIX_CONFIG" \
        "__HOST__=127.0.0.1" \
        "__HTTP_PORT__=$HTTP_PORT" \
        "__ICE_PORT__=$ICE_PORT" \
        "__MANUFACTURER__=$MANUFACTURER" \
        "__REALM_ID__=$REALM_ID" \
        "__REALM_SECRET__=$REALM_SECRET"
    success "SwiftTsWorkloadApp actr.toml rendered"
}

write_swift_ts_workload_app_project_yml() {
    cat >"$TMP_APP_DIR/project.yml" <<EOF
name: SwiftTsWorkloadApp
options:
  bundleIdPrefix: io.actrium
  deploymentTarget:
    iOS: "26.2"
settings:
  base:
    SUPPORTED_PLATFORMS: "iphoneos iphonesimulator"
packages:
  actr-swift:
    path: $REPO_ROOT/bindings/swift
  swift-protobuf:
    url: https://github.com/apple/swift-protobuf.git
    from: 1.32.0
schemes:
  SwiftTsWorkloadApp:
    build:
      targets:
        SwiftTsWorkloadApp: all
    run:
      config: Debug

targets:
  SwiftTsWorkloadApp:
    type: application
    platform: iOS
    sources:
      - path: SwiftTsWorkloadApp
      - path: actr.toml
        type: file
        buildPhase: resources
      - path: manifest.lock.toml
        type: file
        buildPhase: resources
      - path: manifest.toml
        type: file
        buildPhase: resources
    dependencies:
      - package: actr-swift
        product: Actr
      - package: swift-protobuf
        product: SwiftProtobuf
    info:
      path: SwiftTsWorkloadApp/Info.plist
      properties:
        CFBundleDisplayName: SwiftTsWorkloadApp
        UILaunchScreen: {}
        NSLocalNetworkUsageDescription: SwiftTsWorkloadApp connects to the local actrix development server.
        NSAppTransportSecurity:
          NSAllowsArbitraryLoads: true
    settings:
      base:
        PRODUCT_BUNDLE_IDENTIFIER: io.actrium.SwiftTsWorkloadApp
        SWIFT_VERSION: "6.0"
        TARGETED_DEVICE_FAMILY: "1,2"
        SUPPORTED_PLATFORMS: "iphoneos iphonesimulator"
        CODE_SIGN_STYLE: Automatic
EOF
}

scaffold_swift_ts_workload_app() {
    section "🧱 Scaffolding temporary SwiftTsWorkloadApp"
    mkdir -p "$TMP_APP_DIR"
    (
        cd "$TMP_APP_DIR"
        run_actr init \
            -l swift \
            --template empty \
            --project-name "SwiftTsWorkloadApp" \
            --signaling "ws://127.0.0.1:${HTTP_PORT}/signaling/ws" \
            --manufacturer "$MANUFACTURER" \
            "."
    )

    rm -rf "$TMP_APP_DIR/SwiftTsWorkloadApp"
    cp -R "$SCRIPT_DIR/SwiftTsWorkloadApp" "$TMP_APP_DIR/SwiftTsWorkloadApp"
    rm -rf "$TMP_APP_DIR/SwiftTsWorkloadApp/Generated"

    cp "$SCRIPT_DIR/manifest.toml" "$TMP_APP_DIR/manifest.toml"
    write_swift_ts_workload_app_project_yml
    rm -f "$TMP_APP_DIR/protos/local/local.proto"
    write_probe_proto "$TMP_APP_DIR/protos/local/probe.proto"
    write_project_keychain_config "$TMP_APP_DIR" "$PROVISIONED_KEYCHAIN"

    success "Temporary SwiftTsWorkloadApp scaffolded from empty template"
}

# ──── iOS Simulator ────

setup_ios_simulator() {
    section "📱 Setting up iOS Simulator"

    # Find available iOS runtime
    RUNTIME_ID="$(xcrun simctl list runtimes -j | jq -r '.runtimes[] | select(.name | test("iOS")) | .identifier' | tail -1)"
    [ -n "$RUNTIME_ID" ] || fail "No iOS Simulator runtime found"
    success "iOS runtime: $RUNTIME_ID"

    # Find template device for the runtime
    DEVICE_TYPE_ID="$(xcrun simctl list devicetypes -j | jq -r '.devicetypes[] | select(.name | test("iPhone 16$")) | .identifier' | head -1)"
    if [ -z "$DEVICE_TYPE_ID" ]; then
        DEVICE_TYPE_ID="$(xcrun simctl list devicetypes -j | jq -r '.devicetypes[] | select(.name | test("iPhone")) | .identifier' | tail -1)"
    fi
    [ -n "$DEVICE_TYPE_ID" ] || fail "No iPhone device type found"
    success "Device type: $DEVICE_TYPE_ID"

    DEVICE_NAME="swift-swift-ts-workload-e2e-${RUN_ID}"
    DEVICE_UDID="$(xcrun simctl create "$DEVICE_NAME" "$DEVICE_TYPE_ID" "$RUNTIME_ID")"
    DEVICE_CREATED="1"
    success "Created simulator: $DEVICE_NAME ($DEVICE_UDID)"

    xcrun simctl boot "$DEVICE_UDID" 2>/dev/null || true
    if xcrun simctl bootstatus "$DEVICE_UDID" -b >/dev/null 2>&1; then
        success "Simulator booted"
        export DEVICE_UDID
        return 0
    fi

    # Fall back to polling the device state when bootstatus is unavailable or flaky.
    local attempt=0
    local boot_status=""
    while [ $attempt -lt 60 ]; do
        boot_status="$(xcrun simctl list devices -j | jq -r --arg udid "$DEVICE_UDID" '
            .devices | to_entries | .[] | .value | .[] | select(.udid == $udid) | .state')"
        if [ "$boot_status" = "Booted" ]; then
            success "Simulator booted"
            break
        fi
        sleep 1
        attempt=$((attempt + 1))
    done

    if [ "$boot_status" = "Booted" ]; then
        export DEVICE_UDID
        return 0
    fi

    fail "Simulator did not boot: $DEVICE_UDID"
}

# ──── SwiftTsWorkloadApp build (no launch) ────

build_swift_ts_workload_app() {
    section "🔨 Building SwiftTsWorkloadApp with XcodeGen"

    require_cmd xcodegen
    local prev_dir="$PWD"
    cd "$TMP_APP_DIR"

    section "📦 Installing SwiftTsWorkloadApp deps and generating Swift code"
    run_actr deps install
    run_actr gen -l swift
    rm -f SwiftTsWorkloadApp/ActrService.swift

    # Generate Xcode project from project.yml
    rm -rf SwiftTsWorkloadApp.xcodeproj
    xcodegen generate --spec project.yml --project "$TMP_APP_DIR" >"$LOG_DIR/xcodegen.log" 2>&1
    success "XcodeGen project generated"

    section "🏗️  Building SwiftTsWorkloadApp for iOS Simulator"

    local derived_data="$RUN_DIR/DerivedData"

    # Resolve SPM dependencies first (visible progress)
    echo "Resolving SPM packages..."
    xcodebuild \
        -project SwiftTsWorkloadApp.xcodeproj \
        -scheme SwiftTsWorkloadApp \
        -destination "id=$DEVICE_UDID" \
        -derivedDataPath "$derived_data" \
        -resolvePackageDependencies \
        2>&1 | tee -a "$LOG_DIR/xcodebuild.log"
    echo "SPM resolve complete, building..."

    xcodebuild \
        -project SwiftTsWorkloadApp.xcodeproj \
        -scheme SwiftTsWorkloadApp \
        -destination "id=$DEVICE_UDID" \
        -derivedDataPath "$derived_data" \
        -configuration Debug \
        build \
        2>&1 | tee -a "$LOG_DIR/xcodebuild.log"

    # Find built .app
    APP_PATH="$(find "$derived_data/Build/Products" -name "SwiftTsWorkloadApp.app" -type d | head -1)"
    [ -n "$APP_PATH" ] || {
        tail -100 "$LOG_DIR/xcodebuild.log" >&2
        fail "SwiftTsWorkloadApp.app not found in build products"
    }
    success "App built: $APP_PATH"

    cd "$prev_dir"
}

# ──── DuplexEchoService lifecycle ────

run_server_host() {
    section "🚀 Starting package-backed server host"

    cat >"$SERVER_RUNTIME_PATH" <<EOF
edition = 1

[package]
path = "${SERVICE_PACKAGE}"

[signaling]
url = "ws://127.0.0.1:${HTTP_PORT}/signaling/ws"

[ais_endpoint]
url = "http://127.0.0.1:${HTTP_PORT}/ais"

[deployment]
realm_id = ${REALM_ID}
realm_secret = "${REALM_SECRET}"

[[trust]]
kind = "registry"
endpoint = "http://127.0.0.1:${HTTP_PORT}/ais"

[discovery]
visible = true

[observability]
filter_level = "info"
tracing_enabled = false
tracing_endpoint = "http://localhost:4317"
tracing_service_name = "swift-ts-workload-server"

[webrtc]
force_relay = false
stun_urls = ["stun:127.0.0.1:${ICE_PORT}"]
turn_urls = ["turn:127.0.0.1:${ICE_PORT}"]

[acl]

[[acl.rules]]
permission = "allow"
type = "${MANUFACTURER}:SwiftTsWorkloadApp:0.1.0"
EOF

    # Isolate hyper runtime state per-run so stale global state in
    # ~/.actr/hyper cannot stall package attach / registration.
    local server_hyper_dir="$RUN_DIR/hyper/service"
    mkdir -p "$server_hyper_dir"

    RUST_LOG="${RUST_LOG:-info}" \
        run_actr run -c "$SERVER_RUNTIME_PATH" --hyper-dir "$server_hyper_dir" >"$LOG_DIR/server.log" 2>&1 &
    SERVER_PID=$!

    local attempt=0
    while [ $attempt -lt 30 ]; do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            cat "$LOG_DIR/server.log" >&2 || true
            fail "Server host exited early"
        fi

        if grep -q "DuplexEchoService Host fully started\|ActrNode started" "$LOG_DIR/server.log" 2>/dev/null; then
            success "Server host is running"
            return 0
        fi

        sleep 1
        attempt=$((attempt + 1))
    done

    warn "Server host readiness log not observed, continuing"
}

check_service_ready() {
    section "🔍 Verifying DuplexEchoService readiness"

    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        cat "$LOG_DIR/server.log" >&2 || true
        fail "DuplexEchoService process died before app launch"
    fi
    success "DuplexEchoService process alive (PID: $SERVER_PID)"

    if ! curl -fsS "http://127.0.0.1:${HTTP_PORT}/signaling/health" >/dev/null 2>&1; then
        fail "Signaling health check failed before app launch"
    fi
    success "Signaling health OK"

    local db_path="$SQLITE_DIR/signaling_cache.db"
    # The TS workload is a JS-on-wasm component; its first wasmtime compile +
    # instantiate during `actr run` attach can take well over a minute, so allow
    # a generous default registration window.
    local timeout="${SERVICE_READY_TIMEOUT_SECONDS:-240}"
    if ! wait_for_service_registration \
        "$db_path" \
        "$REALM_ID" \
        "$MANUFACTURER" \
        "DuplexEchoService" \
        "$timeout"; then
        echo "Service registrations observed before timeout:" >&2
        if [ -f "$db_path" ]; then
            sqlite3 "$db_path" \
                "SELECT actor_realm_id, actor_manufacturer, actor_device_name, service_name, status, last_heartbeat_at FROM service_registry;" \
                >&2 2>/dev/null || true
        else
            echo "  signaling_cache.db not found at $db_path" >&2
        fi
        tail -n 120 "$LOG_DIR/server.log" >&2 2>/dev/null || true
        fail "DuplexEchoService did not register with signaling within ${timeout}s"
    fi

    sqlite3 "$db_path" "
        SELECT actor_realm_id, actor_manufacturer, actor_device_name, service_name, status
        FROM service_registry
        WHERE actor_realm_id = ${REALM_ID}
          AND actor_manufacturer = '${MANUFACTURER}'
          AND actor_device_name = 'DuplexEchoService'
          AND service_name = '${MANUFACTURER}:DuplexEchoService'
          AND status = 'Available';
    " 2>/dev/null | while read -r line; do
        echo "  $line"
    done
    success "DuplexEchoService readiness check complete"
}

# ──── App install & launch ────

install_and_launch_app() {
    section "📲 Installing and launching SwiftTsWorkloadApp"
    xcrun simctl install "$DEVICE_UDID" "$APP_PATH"

    # Launch with direct stdout/stderr redirection. `simctl launch --console`
    # may return before the app exits when detached from the terminal, so do not
    # treat the wrapper process as the app lifetime.
    SIMCTL_CHILD_ACTR_SWIFTTSAPP_AUTO_STREAM_COUNT=3 \
    SIMCTL_CHILD_ACTR_MANUFACTURER="${MANUFACTURER}" \
    SIMCTL_CHILD_ACTR_SWIFTTSAPP_TARGET_TYPE="${MANUFACTURER}:DuplexEchoService:1.0.0" \
    xcrun simctl launch \
        --terminate-running-process \
        --stdout="$APP_STDOUT_LOG" \
        --stderr="$APP_STDERR_LOG" \
        "$DEVICE_UDID" \
        "io.actrium.SwiftTsWorkloadApp" \
        >"$LOG_DIR/app.launch.log" 2>&1

    success "App launched, waiting for swift-ts-workload echo result"
}

# ──── Result verification ────

grep_app_logs() {
    grep -h "$@" "$APP_STDOUT_LOG" "$APP_STDERR_LOG" 2>/dev/null
}

tail_app_logs() {
    local lines="$1"
    echo "App stdout log tail:"
    tail -n "$lines" "$APP_STDOUT_LOG" >&2 2>/dev/null || true
    echo "App stderr log tail:"
    tail -n "$lines" "$APP_STDERR_LOG" >&2 2>/dev/null || true
}

wait_for_swift_ts_workload_result() {
    section "⏳ Waiting for swift-ts-workload echo result"
    local timeout="${CLIENT_TIMEOUT_SECONDS:-180}"
    local elapsed=0

    while [ $elapsed -lt "$timeout" ]; do
        if grep_app_logs -q "ACTR_E2E_RESULT:"; then
            local result
            result="$(grep_app_logs "ACTR_E2E_RESULT:" | tail -1)"
            echo "swift-ts-workload echo result: $result"
            if echo "$result" | grep -qE "ACTR_E2E_RESULT:call=ok stream=3/3"; then
                success "call ok and all 3 stream echo messages passed"
                return 0
            fi
            warn "Incomplete: got $result, expected ACTR_E2E_RESULT:call=ok stream=3/3"
            return 1
        fi

        sleep 2
        elapsed=$((elapsed + 2))
    done

    echo ""
    tail_app_logs 80
    fail "Timed out waiting for swift-ts-workload echo result after ${timeout}s"
}

# ──── Main ────

section "🧪 Swift SwiftTsWorkloadApp E2E"
echo "Run directory: $RUN_DIR"
echo "Actrix binary: $ACTRIX_BIN"
echo "Host target:   $HOST_TARGET"

# Phase 1: Prepare actrix infrastructure
render_runtime_configs
build_local_actr_cli
start_actrix
login_admin
warmup_ais_key
ensure_realm
provision_mfr_keychain

# Phase 2: Build service package and publish identities
scaffold_service_guest
build_service_package
publish_swift_ts_workload_app_package_identity

# Phase 3: Render SwiftTsWorkloadApp config and prepare the simulator
scaffold_swift_ts_workload_app
render_swift_ts_workload_app_config
setup_ios_simulator

# Phase 4: Start DuplexEchoService before SwiftTsWorkloadApp dependency install.
# `actr deps install` validates the remote dependency through discovery, so the
# service must be registered before Swift codegen resolves manifest.lock.toml.
run_server_host
check_service_ready

# Phase 5: Install SwiftTsWorkloadApp dependencies, generate Swift code, and build
build_swift_ts_workload_app
check_service_ready

# Phase 6: Install app, launch, and verify
install_and_launch_app
wait_for_swift_ts_workload_result

echo ""
success "Swift SwiftTsWorkloadApp e2e completed successfully"
