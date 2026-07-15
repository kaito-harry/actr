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
MANUFACTURER="actrium"
ACTRIX_BIN="${ACTRIX_BIN:-}"
ACTR_CLI_MANIFEST="$REPO_ROOT/cli/Cargo.toml"
ACTRIX_CONFIG_TEMPLATE="$SCRIPT_DIR/../package-runtime-echo/config/actrix.toml"
E2E_TARGET_ROOT="$REPO_ROOT/target/e2e-cache/swift-echo-app"
ACTR_TARGET_DIR="$E2E_TARGET_ROOT/actr-cli"
TEMP_SERVICE_TARGET_DIR="$E2E_TARGET_ROOT/temp-service"
DEFAULT_MESSAGE="e2e-test-message"
REALM_NAME_PREFIX="swift-echo-app"

TEST_INPUT="$DEFAULT_MESSAGE"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -*)
            fail "Unknown option: $1"
            ;;
        *)
            TEST_INPUT="$1"
            shift
            ;;
    esac
done

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
TMP_SERVICE_DIR="$TMP_SERVICE_ROOT/echo-actr-$RANDOM"
TMP_APP_DIR="$TMP_SERVICE_ROOT/echo-app-$RANDOM"
ACTRIX_CONFIG_PATH="$RUN_DIR/actrix.toml"
SERVER_RUNTIME_PATH="$RUN_DIR/server-runtime.toml"
SERVICE_KEYCHAIN="$TMP_SERVICE_DIR/packaging/keys/mfr.keychain.json"
SERVICE_PUBLIC_KEY="$TMP_SERVICE_DIR/public-key.json"
PROVISIONED_KEYCHAIN="$RUN_DIR/mfr.keychain.json"
PROVISIONED_PUBLIC_KEY="$RUN_DIR/mfr-public-key.json"
ECHOAPP_ACTRIX_CONFIG="$TMP_APP_DIR/actr.toml"
HOST_TARGET="$(rustc -vV | awk '/host:/ {print $2}')"
ECHOAPP_PACKAGE_MANIFEST="$RUN_DIR/echoapp-package-manifest.toml"
ECHOAPP_MARKER_BINARY="$RUN_DIR/echoapp-linked-identity.bin"
ECHOAPP_PACKAGE="$DIST_DIR/${MANUFACTURER}-EchoApp-0.1.0-${HOST_TARGET}.actr"
APP_STDOUT_LOG="$LOG_DIR/app.stdout.log"
APP_STDERR_LOG="$LOG_DIR/app.stderr.log"
APP_BUNDLE_ID="io.actrium.EchoApp"
APP_PROCESS_NAME="EchoApp"
DIAGNOSTIC_DIR="$RUN_DIR/diagnostics"
SANITIZED_LOG_DIR="$RUN_DIR/sanitized-logs"
SYMBOL_DIR="$RUN_DIR/symbols"

mkdir -p "$SQLITE_DIR" "$LOG_DIR" "$DIST_DIR" "$TMP_SERVICE_ROOT" "$E2E_TARGET_ROOT" "$DIAGNOSTIC_DIR" "$SANITIZED_LOG_DIR"
rm -rf "$SCRIPT_DIR/.tmp/symbols"

ACTRIX_PID=""
SERVER_PID=""
APP_PID=""
APP_DSYM=""
APP_BINARY=""
LLDB_PID=""
ACTR_CLI_BIN=""
ADMIN_TOKEN=""
SERVICE_PACKAGE=""
SERVICE_VERSION=""
REALM_SECRET=""
DEVICE_UDID=""

# ──── Diagnostics ────

app_process_is_running() {
    [ -n "${APP_PID:-}" ] && kill -0 "$APP_PID" 2>/dev/null
}

record_app_pid_from_launch_log() {
    APP_PID="$(
        awk -F': ' -v bundle="$APP_BUNDLE_ID" \
            '$1 == bundle && $2 ~ /^[0-9]+$/ { print $2; exit }' \
            "$LOG_DIR/app.launch.log" 2>/dev/null || true
    )"

    if [ -n "$APP_PID" ]; then
        echo "APP_PID=$APP_PID" >>"$LOG_DIR/app.launch.log"
    else
        warn "Unable to parse app PID from $LOG_DIR/app.launch.log"
    fi
}

collect_app_symbols() {
    local products_dir="$1"
    local dsym
    local app_binary

    [ -d "$APP_DSYM" ] || fail "App dSYM not found: $APP_DSYM"
    [ -f "$APP_BINARY" ] || fail "App executable not found: $APP_BINARY"

    rm -rf "$SYMBOL_DIR"
    mkdir -p "$SYMBOL_DIR/${APP_PROCESS_NAME}.app"
    cp -R "$APP_DSYM" "$SYMBOL_DIR/"
    xcrun dwarfdump --uuid "$APP_DSYM" >"$SYMBOL_DIR/uuids.txt"

    while IFS= read -r -d '' dsym; do
        [ "$dsym" = "$APP_DSYM" ] && continue
        cp -R "$dsym" "$SYMBOL_DIR/"
        xcrun dwarfdump --uuid "$dsym" >>"$SYMBOL_DIR/uuids.txt"
    done < <(find "$products_dir" -type d -name "*.dSYM" -prune -print0)

    for app_binary in "$APP_BINARY" "$APP_PATH"/*.debug.dylib; do
        [ -f "$app_binary" ] || continue
        cp "$app_binary" "$SYMBOL_DIR/${APP_PROCESS_NAME}.app/"
        xcrun dwarfdump --uuid "$app_binary" >>"$SYMBOL_DIR/uuids.txt"
    done

    success "Debug symbols collected: $SYMBOL_DIR"
}

start_lldb_crash_capture() {
    [ -n "${APP_PID:-}" ] || fail "Cannot attach LLDB without an app PID"

    local lldb_args=(
        --batch
        --no-lldbinit
        --attach-pid "$APP_PID"
        -o "settings set target.debug-file-search-paths \"$SYMBOL_DIR\""
        -o "process continue"
        -k "process status"
        -k "thread backtrace all"
        -k "register read"
        -k "image list -o -f"
    )

    xcrun lldb "${lldb_args[@]}" >"$LOG_DIR/app.lldb.log" 2>&1 &
    LLDB_PID=$!
    success "LLDB crash capture started (PID: $LLDB_PID)"
}

wait_for_lldb_capture() {
    [ -n "${LLDB_PID:-}" ] || return 0

    local elapsed=0
    while kill -0 "$LLDB_PID" 2>/dev/null && [ "$elapsed" -lt 15 ]; do
        sleep 1
        elapsed=$((elapsed + 1))
    done
    if kill -0 "$LLDB_PID" 2>/dev/null; then
        kill "$LLDB_PID" 2>/dev/null || true
    fi
    wait "$LLDB_PID" 2>/dev/null || true
}

capture_app_crash_reports() {
    local diag_dir="$1"
    local reports_dir="$HOME/Library/Logs/DiagnosticReports"
    local dst_dir="$diag_dir/crash-reports"

    [ -d "$reports_dir" ] || return 0
    mkdir -p "$dst_dir"
    find "$reports_dir" -maxdepth 1 -type f \
        \( -name "*${APP_PROCESS_NAME}*.ips" -o -name "*${APP_PROCESS_NAME}*.crash" \) \
        -exec cp {} "$dst_dir/" \; 2>/dev/null || true
    rmdir "$dst_dir" 2>/dev/null || true
}

capture_core_simulator_logs() {
    local diag_dir="$1"
    local sim_logs="$HOME/Library/Developer/CoreSimulator/Devices/$DEVICE_UDID/data/Library/Logs"

    [ -n "${DEVICE_UDID:-}" ] || return 0
    [ -d "$sim_logs" ] || return 0
    mkdir -p "$diag_dir/core-simulator-logs"
    cp -R "$sim_logs/." "$diag_dir/core-simulator-logs/" 2>/dev/null || true
}

capture_simulator_diagnostics() {
    local diag_dir="$1"
    local predicate
    local diagnose_dir="$diag_dir/simctl-diagnose"

    [ -n "${DEVICE_UDID:-}" ] || return 0

    predicate="process CONTAINS \"$APP_PROCESS_NAME\" OR eventMessage CONTAINS[c] \"actr\""
    xcrun simctl spawn "$DEVICE_UDID" log show --last 10m --style compact \
        --predicate "$predicate" \
        >"$diag_dir/simulator-app.log" 2>"$diag_dir/simulator-app.err" || true

    mkdir -p "$diagnose_dir"
    printf '\n' | xcrun simctl diagnose -b --timeout=60 --output="$diagnose_dir" --no-archive \
        --udid="$DEVICE_UDID" >"$diag_dir/simctl-diagnose.log" 2>&1 || true

    capture_app_crash_reports "$diag_dir"
    capture_core_simulator_logs "$diag_dir"
}

capture_app_container_logs() {
    local diag_dir="$1"
    local app_container

    [ -n "${DEVICE_UDID:-}" ] || return 0

    app_container="$(xcrun simctl get_app_container "$DEVICE_UDID" "$APP_BUNDLE_ID" data 2>/dev/null || true)"
    if [ -n "$app_container" ] && [ -d "$app_container/Documents" ]; then
        mkdir -p "$diag_dir/app-container"
        find "$app_container/Documents" -maxdepth 1 -type f -name "*.log" \
            -exec cp {} "$diag_dir/app-container/" \; 2>/dev/null || true
        rmdir "$diag_dir/app-container" 2>/dev/null || true
    fi
}

fail_if_app_exited_before_result() {
    local marker_description="$1"

    if [ -n "${APP_PID:-}" ] && ! app_process_is_running; then
        echo ""
        tail_app_logs 80
        fail "$APP_PROCESS_NAME exited before $marker_description (APP_PID=$APP_PID)"
    fi
}

capture_diagnostics() {
    local diag_dir="$DIAGNOSTIC_DIR"
    mkdir -p "$diag_dir"

    # Process status
    {
        echo "=== Process Status ==="
        echo "ACTRIX_PID=${ACTRIX_PID:-none}"
        echo "SERVER_PID=${SERVER_PID:-none}"
        echo "APP_PID=${APP_PID:-none}"
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
        if app_process_is_running; then
            echo "app: RUNNING"
            ps -p "$APP_PID" -o pid,ppid,stat,etime,command 2>/dev/null || true
        else
            echo "app: NOT RUNNING"
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

    if app_process_is_running && command -v sample >/dev/null 2>&1; then
        sample "$APP_PID" 5 1 -file "$diag_dir/app.sample.txt" >/dev/null 2>&1 || true
    fi

    capture_simulator_diagnostics "$diag_dir"
    capture_app_container_logs "$diag_dir"

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

    sanitize_one_file() {
        local src_file="$1"
        local dst_file="$2"
        mkdir -p "$(dirname "$dst_file")"
        cp "$src_file" "$dst_file" 2>/dev/null || return 0

        case "$src_file" in
            *.log|*.txt|*.json|*.ips|*.crash|*.plist|*.stdout|*.stderr|*.sample)
                for secret in "${secrets[@]}"; do
                    if [ -n "$secret" ]; then
                        SECRET="$secret" perl -0pi -e 's/\Q$ENV{SECRET}\E/REDACTED/g' "$dst_file" 2>/dev/null || true
                    fi
                done
                ;;
        esac
    }

    if [ -d "$src_dir" ]; then
        while IFS= read -r file; do
            local rel_path
            rel_path="${file#$src_dir/}"
            sanitize_one_file "$file" "$dst_dir/$rel_path"
        done < <(find "$src_dir" -type f)
    fi

    # Copy logs but NOT keychain, runtime config, or SQLite state
    for log in "$LOG_DIR"/*.log; do
        [ -f "$log" ] || continue
        sanitize_one_file "$log" "$dst_dir/$(basename "$log")"
    done

    echo "Sanitized logs at: $dst_dir"
}

cleanup() {
    local status=$?

    wait_for_lldb_capture

    # Collect diagnostics BEFORE killing processes
    if [ $status -ne 0 ] || [ "${CAPTURE_DIAGNOSTICS_ON_SUCCESS:-0}" = "1" ]; then
        capture_diagnostics || true
        sanitize_logs_for_upload "$DIAGNOSTIC_DIR" "$SANITIZED_LOG_DIR" || true
    fi

    if [ -n "$DEVICE_UDID" ]; then
        xcrun simctl terminate "$DEVICE_UDID" "$APP_BUNDLE_ID" 2>/dev/null || true
    fi
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    if [ -n "$ACTRIX_PID" ] && kill -0 "$ACTRIX_PID" 2>/dev/null; then
        kill "$ACTRIX_PID" 2>/dev/null || true
    fi
    wait 2>/dev/null || true

    # Remove generated project configs that contain the per-run realm secret.
    rm -f "$ECHOAPP_ACTRIX_CONFIG" "$TMP_APP_DIR/.actr/config.toml"
    rmdir "$TMP_APP_DIR/.actr" 2>/dev/null || true

    # Shut down booted iOS Simulators
    xcrun simctl shutdown all 2>/dev/null || true

    # Move sanitized logs out of RUN_DIR to a fixed location so the
    # upload-artifact step can find them regardless of success or failure.
    local upload_dir="$SCRIPT_DIR/.tmp/sanitized-logs"
    if [ -d "$SANITIZED_LOG_DIR" ] && [ -n "$(ls -A "$SANITIZED_LOG_DIR" 2>/dev/null)" ]; then
        rm -rf "$upload_dir"
        mv "$SANITIZED_LOG_DIR" "$upload_dir"
        echo "Sanitized logs moved to: $upload_dir"
    fi

    local symbol_upload_dir="$SCRIPT_DIR/.tmp/symbols"
    if [ -d "$SYMBOL_DIR" ] && [ -n "$(ls -A "$SYMBOL_DIR" 2>/dev/null)" ]; then
        rm -rf "$symbol_upload_dir"
        mv "$SYMBOL_DIR" "$symbol_upload_dir"
        echo "Debug symbols moved to: $symbol_upload_dir"
    fi

    if [ $status -eq 0 ] && [ "${KEEP_TMP:-0}" != "1" ]; then
        rm -rf "$RUN_DIR"
    else
        echo ""
        echo "Artifacts preserved at: $RUN_DIR"
        if [ -d "$upload_dir" ] && [ -n "$(ls -A "$upload_dir" 2>/dev/null)" ]; then
            echo "Sanitized logs for upload at: $upload_dir"
        fi
        if [ -d "$symbol_upload_dir" ] && [ -n "$(ls -A "$symbol_upload_dir" 2>/dev/null)" ]; then
            echo "Debug symbols for upload at: $symbol_upload_dir"
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

    env "${cargo_env[@]}" CARGO_TARGET_DIR="$ACTR_TARGET_DIR" cargo build --manifest-path "$ACTR_CLI_MANIFEST" --bin actr >/dev/null
    ACTR_CLI_BIN="$ACTR_TARGET_DIR/debug/actr"
    [ -x "$ACTR_CLI_BIN" ] || fail "actr CLI binary missing at $ACTR_CLI_BIN"
    success "actr CLI ready: $ACTR_CLI_BIN"
}

append_workspace_patch() {
    local cargo_toml="$1"
    local repo_path="$REPO_ROOT"

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
actr-config = { path = "$repo_path/core/config" }
actr-protocol = { path = "$repo_path/core/protocol" }
actr-framework = { path = "$repo_path/core/framework" }
actr-hyper = { path = "$repo_path/core/hyper" }
actr-pack = { path = "$repo_path/core/pack" }
actr-platform-native = { path = "$repo_path/core/platform-native" }
actr-platform-traits = { path = "$repo_path/core/platform-traits" }
actr-runtime = { path = "$repo_path/core/runtime" }
actr-runtime-mailbox = { path = "$repo_path/core/runtime-mailbox" }
actr-service-compat = { path = "$repo_path/core/service-compat" }
EOF
}

pin_workspace_actr_dependencies() {
    local cargo_toml="$1"

    ACTR_FRAMEWORK_PATH="$REPO_ROOT/core/framework" \
    ACTR_PROTOCOL_PATH="$REPO_ROOT/core/protocol" \
        perl -i -pe '
            if (/^actr-framework = /) {
                $_ = qq{actr-framework = { path = "$ENV{ACTR_FRAMEWORK_PATH}" }\n};
            } elsif (/^actr-protocol = /) {
                $_ = qq{actr-protocol = { path = "$ENV{ACTR_PROTOCOL_PATH}" }\n};
            }
        ' "$cargo_toml"

    grep -Fq "actr-framework = { path = \"$REPO_ROOT/core/framework\" }" "$cargo_toml" ||
        fail "Failed to pin actr-framework to the workspace"
    grep -Fq "actr-protocol = { path = \"$REPO_ROOT/core/protocol\" }" "$cargo_toml" ||
        fail "Failed to pin actr-protocol to the workspace"
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

scaffold_service_guest() {
    section "🧱 Scaffolding temporary echo service"
    run_actr init \
        -l rust \
        --template echo \
        --role service \
        --signaling "ws://127.0.0.1:${HTTP_PORT}/signaling/ws" \
        --manufacturer "$MANUFACTURER" \
        "$TMP_SERVICE_DIR"

    pin_workspace_actr_dependencies "$TMP_SERVICE_DIR/Cargo.toml"
    append_workspace_patch "$TMP_SERVICE_DIR/Cargo.toml"
    mkdir -p "$(dirname "$SERVICE_KEYCHAIN")"
    cp "$PROVISIONED_KEYCHAIN" "$SERVICE_KEYCHAIN"
    cp "$PROVISIONED_PUBLIC_KEY" "$SERVICE_PUBLIC_KEY"
    write_project_keychain_config "$TMP_SERVICE_DIR" "$SERVICE_KEYCHAIN"

    (
        cd "$TMP_SERVICE_DIR"
        CARGO_TARGET_DIR="$TEMP_SERVICE_TARGET_DIR" run_actr deps install
        CARGO_TARGET_DIR="$TEMP_SERVICE_TARGET_DIR" run_actr gen -l rust
    )

    SERVICE_VERSION="$(
        awk '
            /^\[package\]/ { in_package = 1; next }
            /^\[/ && in_package { exit }
            in_package && $1 == "version" {
                gsub(/"/, "", $3)
                print $3
                exit
            }
        ' "$TMP_SERVICE_DIR/manifest.toml"
    )"

    [ -n "$SERVICE_VERSION" ] || fail "Unable to detect temporary service version"
    success "Temporary echo service ready: version ${SERVICE_VERSION}"
}

build_service_package() {
    section "📦 Building and publishing the server package"
    SERVICE_PACKAGE="$DIST_DIR/${MANUFACTURER}-EchoService-${SERVICE_VERSION}-${HOST_TARGET}.actr"

    (
        cd "$TMP_SERVICE_DIR"
        CARGO_TARGET_DIR="$TEMP_SERVICE_TARGET_DIR" run_actr build \
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

    success "Server package published"
}

publish_echoapp_package_identity() {
    section "📦 Publishing EchoApp package identity"

    # Linked EchoApp does not load this package. It is a registry marker for
    # actrix versions that still require the actor type to be package-registered.
    printf 'linked EchoApp identity marker\n' >"$ECHOAPP_MARKER_BINARY"
    cat >"$ECHOAPP_PACKAGE_MANIFEST" <<EOF
edition = 1

[package]
name = "EchoApp"
manufacturer = "${MANUFACTURER}"
version = "0.1.0"
description = "Actrium EchoApp linked runtime identity marker"

[binary]
path = "${ECHOAPP_MARKER_BINARY}"
target = "${HOST_TARGET}"
EOF

    run_actr build \
        --no-compile \
        --manifest-path "$ECHOAPP_PACKAGE_MANIFEST" \
        --key "$PROVISIONED_KEYCHAIN" \
        --output "$ECHOAPP_PACKAGE"

    run_actr pkg verify --pubkey "$PROVISIONED_PUBLIC_KEY" --package "$ECHOAPP_PACKAGE" >/dev/null
    run_actr registry publish \
        --package "$ECHOAPP_PACKAGE" \
        --keychain "$PROVISIONED_KEYCHAIN" \
        --endpoint "http://127.0.0.1:${HTTP_PORT}"

    success "EchoApp package identity published"
}

# ──── EchoApp config ────

prepare_echo_app_workspace() {
    section "🧱 Preparing temporary EchoApp workspace"

    rm -rf "$TMP_APP_DIR"
    mkdir -p "$TMP_APP_DIR"
    cp -R "$SCRIPT_DIR/EchoApp" "$TMP_APP_DIR/EchoApp"
    cp -R "$SCRIPT_DIR/protos" "$TMP_APP_DIR/protos"
    cp "$SCRIPT_DIR/manifest.toml" "$TMP_APP_DIR/manifest.toml"
    cp "$SCRIPT_DIR/actr.lock.toml" "$TMP_APP_DIR/actr.lock.toml"
    cp "$SCRIPT_DIR/project.yml" "$TMP_APP_DIR/project.yml"

    # Always validate freshly generated sources and dependency snapshots while
    # leaving the checked-out fixture and any developer-local CLI config intact.
    rm -rf "$TMP_APP_DIR/EchoApp/Generated" "$TMP_APP_DIR/protos/remote"

    ACTR_SWIFT_PATH="$REPO_ROOT/bindings/swift" \
        perl -i -pe '
            if (/^\s*path: \.\.\/\.\.\/bindings\/swift\s*$/) {
                $_ = qq{    path: $ENV{ACTR_SWIFT_PATH}\n};
            }
        ' "$TMP_APP_DIR/project.yml"
    grep -Fq "path: $REPO_ROOT/bindings/swift" "$TMP_APP_DIR/project.yml" ||
        fail "Failed to point EchoApp at the workspace Swift binding"

    write_project_keychain_config "$TMP_APP_DIR" "$PROVISIONED_KEYCHAIN"
    success "Temporary EchoApp workspace ready: $TMP_APP_DIR"
}

render_echoapp_config() {
    section "📝 Rendering EchoApp runtime config"
    render_template \
        "$SCRIPT_DIR/actr.toml.tpl" \
        "$ECHOAPP_ACTRIX_CONFIG" \
        "__HOST__=127.0.0.1" \
        "__HTTP_PORT__=$HTTP_PORT" \
        "__ICE_PORT__=$ICE_PORT" \
        "__REALM_ID__=$REALM_ID" \
        "__REALM_SECRET__=$REALM_SECRET"
    success "EchoApp actr.toml rendered"
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

    # Look for an existing device with this runtime + device type
    DEVICE_UDID="$(xcrun simctl list devices -j | jq -r --arg runtime "$RUNTIME_ID" --arg dt "$DEVICE_TYPE_ID" '
        .devices[$runtime] // [] | .[] | select(.deviceTypeIdentifier == $dt) | .udid' | head -1)"

    if [ -z "$DEVICE_UDID" ]; then
        DEVICE_NAME="swift-echo-e2e-${RUN_ID}"
        DEVICE_UDID="$(xcrun simctl create "$DEVICE_NAME" "$DEVICE_TYPE_ID" "$RUNTIME_ID")"
        success "Created simulator: $DEVICE_NAME ($DEVICE_UDID)"
    else
        success "Reusing simulator: $DEVICE_UDID"
    fi

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

# ──── EchoApp build (no launch) ────

build_echo_app() {
    section "🔨 Building EchoApp with XcodeGen"

    require_cmd xcodegen
    local prev_dir="$PWD"
    cd "$TMP_APP_DIR"

    section "📦 Installing EchoApp deps and generating Swift code"
    run_actr deps install
    run_actr gen -l swift
    rm -f EchoApp/LocalEchoServiceHandlerImpl.swift
    rm -f EchoApp/LocalEchoServiceLifecycleAdapter.swift

    # Generate Xcode project from project.yml
    rm -rf EchoApp.xcodeproj
    xcodegen generate --spec project.yml --project "$TMP_APP_DIR" >"$LOG_DIR/xcodegen.log" 2>&1
    success "XcodeGen project generated"

    section "🏗️  Building EchoApp for iOS Simulator"

    local derived_data="$RUN_DIR/DerivedData"

    # Resolve SPM dependencies first (visible progress)
    echo "Resolving SPM packages..."
    xcodebuild \
        -project EchoApp.xcodeproj \
        -scheme EchoApp \
        -destination "id=$DEVICE_UDID" \
        -derivedDataPath "$derived_data" \
        -resolvePackageDependencies \
        2>&1 | tee -a "$LOG_DIR/xcodebuild.log"
    echo "SPM resolve complete, building..."

    xcodebuild \
        -project EchoApp.xcodeproj \
        -scheme EchoApp \
        -destination "id=$DEVICE_UDID" \
        -derivedDataPath "$derived_data" \
        -configuration Debug \
        DEBUG_INFORMATION_FORMAT=dwarf-with-dsym \
        GCC_GENERATE_DEBUGGING_SYMBOLS=YES \
        COPY_PHASE_STRIP=NO \
        STRIP_INSTALLED_PRODUCT=NO \
        build \
        2>&1 | tee -a "$LOG_DIR/xcodebuild.log"

    # Find built .app
    APP_PATH="$(find "$derived_data/Build/Products" -name "EchoApp.app" -type d | head -1)"
    [ -n "$APP_PATH" ] || {
        tail -100 "$LOG_DIR/xcodebuild.log" >&2
        fail "EchoApp.app not found in build products"
    }
    APP_DSYM="${APP_PATH}.dSYM"
    APP_BINARY="$APP_PATH/$APP_PROCESS_NAME"
    collect_app_symbols "$derived_data/Build/Products"
    success "App built: $APP_PATH"

    cd "$prev_dir"
}

# ──── EchoService lifecycle ────

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
tracing_service_name = "swift-echo-app-server"

[webrtc]
force_relay = false
stun_urls = ["stun:127.0.0.1:${ICE_PORT}"]
turn_urls = ["turn:127.0.0.1:${ICE_PORT}"]

[acl]

[[acl.rules]]
permission = "allow"
type = "${MANUFACTURER}:EchoApp:0.1.0"
EOF

    RUST_LOG="${RUST_LOG:-info}" \
        run_actr run -c "$SERVER_RUNTIME_PATH" >"$LOG_DIR/server.log" 2>&1 &
    SERVER_PID=$!

    local attempt=0
    while [ $attempt -lt 30 ]; do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            cat "$LOG_DIR/server.log" >&2 || true
            fail "Server host exited early"
        fi

        if grep -q "Echo Host fully started\|ActrNode started" "$LOG_DIR/server.log" 2>/dev/null; then
            success "Server host is running"
            return 0
        fi

        sleep 1
        attempt=$((attempt + 1))
    done

    warn "Server host readiness log not observed, continuing"
}

check_service_ready() {
    section "🔍 Verifying EchoService readiness"

    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        cat "$LOG_DIR/server.log" >&2 || true
        fail "EchoService process died before app launch"
    fi
    success "EchoService process alive (PID: $SERVER_PID)"

    if ! curl -fsS "http://127.0.0.1:${HTTP_PORT}/signaling/health" >/dev/null 2>&1; then
        fail "Signaling health check failed before app launch"
    fi
    success "Signaling health OK"

    local db_path="$SQLITE_DIR/signaling_cache.db"
    local timeout="${SERVICE_READY_TIMEOUT_SECONDS:-60}"
    if ! wait_for_service_registration \
        "$db_path" \
        "$REALM_ID" \
        "$MANUFACTURER" \
        "EchoService" \
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
        fail "EchoService did not register with signaling within ${timeout}s"
    fi

    sqlite3 "$db_path" "
        SELECT actor_realm_id, actor_manufacturer, actor_device_name, service_name, status
        FROM service_registry
        WHERE actor_realm_id = ${REALM_ID}
          AND actor_manufacturer = '${MANUFACTURER}'
          AND actor_device_name = 'EchoService'
          AND service_name = '${MANUFACTURER}:EchoService'
          AND status = 'Available';
    " 2>/dev/null | while read -r line; do
        echo "  $line"
    done
    success "EchoService readiness check complete"
}

# ──── App install & launch ────

install_and_launch_app() {
    section "📲 Installing and launching EchoApp"
    xcrun simctl install "$DEVICE_UDID" "$APP_PATH"

    local launch_args=(
        --terminate-running-process
        "--stdout=$APP_STDOUT_LOG"
        "--stderr=$APP_STDERR_LOG"
    )
    if [ "${CAPTURE_CRASH_BACKTRACE:-0}" = "1" ]; then
        launch_args+=(--wait-for-debugger)
    fi

    # Launch with direct stdout/stderr redirection. `simctl launch --console`
    # may return before the app exits when detached from the terminal, so do not
    # treat the wrapper process as the app lifetime.
    SIMCTL_CHILD_ACTR_ECHOAPP_AUTO_SEND=1 \
    SIMCTL_CHILD_ACTR_ECHOAPP_TEST_INPUT="$TEST_INPUT" \
    xcrun simctl launch \
        "${launch_args[@]}" \
        "$DEVICE_UDID" \
        "$APP_BUNDLE_ID" \
        >"$LOG_DIR/app.launch.log" 2>&1

    record_app_pid_from_launch_log
    if [ "${CAPTURE_CRASH_BACKTRACE:-0}" = "1" ]; then
        start_lldb_crash_capture
    fi
    success "App launched (APP_PID=${APP_PID:-unknown}), waiting for echo result"
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

wait_for_echo_result() {
    section "⏳ Waiting for echo result"
    local timeout="${CLIENT_TIMEOUT_SECONDS:-120}"
    local elapsed=0

    while [ $elapsed -lt "$timeout" ]; do
        if grep_app_logs -q "ACTR_E2E_RESULT:"; then
            local result
            result="$(grep_app_logs "ACTR_E2E_RESULT:" | tail -1)"
            echo "Echo result: $result"
            if echo "$result" | grep -q "$TEST_INPUT"; then
                success "End-to-end echo succeeded"
                return 0
            fi
            warn "Echo result received but does not contain expected message: $TEST_INPUT"
            return 1
        fi

        fail_if_app_exited_before_result "echo result"
        sleep 2
        elapsed=$((elapsed + 2))
    done

    echo ""
    tail_app_logs 80
    fail "Timed out waiting for echo result after ${timeout}s"
}

# ──── Main ────

section "🧪 Swift EchoApp E2E"
echo "Run directory: $RUN_DIR"
echo "Message:       $TEST_INPUT"
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
publish_echoapp_package_identity

# Phase 3: Prepare the isolated EchoApp workspace, render config, and setup simulator
# (Service starts in Phase 4 before the app build, because `actr deps install`
#  inside build_echo_app validates the remote EchoService dependency through
#  discovery — the service must be registered first, mirroring the Swift
#  DataStreamApp / SwiftTsWorkloadApp e2e ordering.)
prepare_echo_app_workspace
render_echoapp_config
setup_ios_simulator

# Phase 4: Start EchoService, then build the app (deps install needs the service)
run_server_host
check_service_ready
build_echo_app
check_service_ready

# Phase 5: Install app, launch, and verify
install_and_launch_app
wait_for_echo_result

echo ""
success "Swift EchoApp e2e completed successfully"
